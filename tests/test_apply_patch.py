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

    def test_batches_write_hooks_in_fs_handlers(self) -> None:
        replacements = [
            replacement
            for replacement in PATCHER.REPLACEMENTS
            if replacement.path == "server/handles/fsmanage.go"
        ]
        self.assertEqual(len(replacements), 2)
        for replacement in replacements:
            self.assertIn("op.WithObjsUpdateHookBatch", replacement.after)
            self.assertIn("hookBatch.Dispatch", replacement.after)
            self.assertNotIn("len(req.Names) > i+1", replacement.after)

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
