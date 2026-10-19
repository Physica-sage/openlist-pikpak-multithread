from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "apply_patch", PROJECT_ROOT / "scripts" / "apply_patch.py"
)
assert SPEC is not None and SPEC.loader is not None
PATCHER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PATCHER
SPEC.loader.exec_module(PATCHER)


class ApplyPatchTest(unittest.TestCase):
    def make_source(self) -> Path:
        root = Path(self.temp_dir.name)
        (root / "go.mod").write_text(
            "module github.com/OpenListTeam/OpenList/v4\n", encoding="utf-8"
        )
        file_parts: dict[str, list[str]] = {}
        for replacement in PATCHER.REPLACEMENTS:
            file_parts.setdefault(replacement.path, []).append(replacement.before)
        for relative, anchors in file_parts.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "before\n" + "\nbetween\n".join(anchors) + "\nafter\n",
                encoding="utf-8",
            )
        return root

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_applies_all_replacements_and_overlay_files(self) -> None:
        root = self.make_source()
        changed = PATCHER.apply_patch(root)

        overlay_count = sum(1 for path in PATCHER.OVERLAY_ROOT.rglob("*") if path.is_file())
        replacement_file_count = len({item.path for item in PATCHER.REPLACEMENTS})
        self.assertEqual(len(changed), replacement_file_count + overlay_count)
        for replacement in PATCHER.REPLACEMENTS:
            content = (root / replacement.path).read_text(encoding="utf-8")
            self.assertNotIn(replacement.before, content)
            self.assertIn(replacement.after, content)
        self.assertTrue((root / "drivers/pikpak/transfer_config.go").is_file())
        self.assertTrue((root / "drivers/pikpak/transfer_config_test.go").is_file())
        self.assertTrue((root / "internal/op/link_cache_type.go").is_file())

    def test_anchor_failure_does_not_partially_modify_source(self) -> None:
        root = self.make_source()
        first = PATCHER.REPLACEMENTS[0]
        first_path = root / first.path
        first_before = first_path.read_text(encoding="utf-8")
        broken_path = root / PATCHER.REPLACEMENTS[-1].path
        broken_path.write_text("upstream changed\n", encoding="utf-8")

        with self.assertRaises(PATCHER.PatchError):
            PATCHER.apply_patch(root)

        self.assertEqual(first_path.read_text(encoding="utf-8"), first_before)
        self.assertFalse((root / "drivers/pikpak/transfer_config.go").exists())

    def test_refuses_to_patch_twice(self) -> None:
        root = self.make_source()
        PATCHER.apply_patch(root)

        with self.assertRaises(PATCHER.PatchError):
            PATCHER.apply_patch(root)

    def test_patch_is_limited_to_pikpak_transfer(self) -> None:
        self.assertEqual(
            {replacement.path for replacement in PATCHER.REPLACEMENTS},
            {
                "drivers/pikpak/driver.go",
                "internal/model/args.go",
                "internal/op/fs.go",
                "internal/fs/copy_move.go",
                "internal/offline_download/tool/transfer.go",
            },
        )
        self.assertEqual(
            {
                path.relative_to(PATCHER.OVERLAY_ROOT).as_posix()
                for path in PATCHER.OVERLAY_ROOT.rglob("*")
                if path.is_file()
            },
            {
                "drivers/pikpak/transfer_config.go",
                "drivers/pikpak/transfer_config_test.go",
                "internal/op/link_cache_type.go",
                "internal/op/link_cache_type_test.go",
            },
        )
        for replacement in PATCHER.REPLACEMENTS:
            self.assertNotIn("Hook", replacement.after)

    def test_preserves_strm_and_batch_handlers(self) -> None:
        root = self.make_source()
        untouched_paths = [
            "drivers/strm/hook.go",
            "server/handles/fsmanage.go",
            "internal/task_group/transfer.go",
        ]
        for relative in untouched_paths:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"unchanged upstream contents\n")
        PATCHER.apply_patch(root)
        for relative in untouched_paths:
            self.assertEqual(
                (root / relative).read_bytes(), b"unchanged upstream contents\n"
            )

    def test_cache_patch_preserves_ip_and_user_agent_partitioning(self) -> None:
        root = self.make_source()
        path = root / "internal/op/fs.go"
        suffix = (
            '\nif mode&driver.LinkCacheIP != 0 { typeKey += "/" + args.IP }\n'
            'if mode&driver.LinkCacheUA != 0 { typeKey += "/" + args.Header.Get("User-Agent") }\n'
        )
        path.write_text(path.read_text(encoding="utf-8") + suffix, encoding="utf-8")
        PATCHER.apply_patch(root)
        self.assertTrue(path.read_text(encoding="utf-8").endswith(suffix))

    def test_prepare_replacements_combines_edits_to_the_same_file(self) -> None:
        root = Path(self.temp_dir.name)
        path = root / "shared.go"
        path.write_text("one\ntwo\n", encoding="utf-8")
        original = PATCHER.REPLACEMENTS
        PATCHER.REPLACEMENTS = (
            PATCHER.Replacement("shared.go", "one", "ONE"),
            PATCHER.Replacement("shared.go", "two", "TWO"),
        )
        try:
            updates = PATCHER.prepare_replacements(root)
        finally:
            PATCHER.REPLACEMENTS = original

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0][1], "ONE\nTWO\n")


if __name__ == "__main__":
    unittest.main()
