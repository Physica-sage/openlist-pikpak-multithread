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
        for replacement in PATCHER.REPLACEMENTS:
            path = root / replacement.path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"before\n{replacement.before}\nafter\n", encoding="utf-8")
        return root

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_applies_all_replacements_and_overlay_files(self) -> None:
        root = self.make_source()
        changed = PATCHER.apply_patch(root)

        overlay_count = sum(1 for path in PATCHER.OVERLAY_ROOT.rglob("*") if path.is_file())
        self.assertEqual(len(changed), len(PATCHER.REPLACEMENTS) + overlay_count)
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


if __name__ == "__main__":
    unittest.main()
