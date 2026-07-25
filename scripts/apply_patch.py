#!/usr/bin/env python3
"""Apply the PikPak transfer multirange overlay to an OpenList source tree."""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OVERLAY_ROOT = PROJECT_ROOT / "overlay"


class PatchError(RuntimeError):
    """Raised when an upstream source tree does not match the expected shape."""


@dataclass(frozen=True)
class Replacement:
    path: str
    before: str
    after: str


REPLACEMENTS = (
    Replacement(
        "drivers/pikpak/driver.go",
        """\treturn &model.Link{
\t\tURL: url,
\t}, nil""",
        """\tlink := &model.Link{URL: url}
\tif args.InternalTransfer {
\t\tconfig := getPikPakTransferConfig()
\t\tlink.Concurrency = config.concurrency
\t\tlink.PartSize = config.partSize
\t}
\treturn link, nil""",
    ),
    Replacement(
        "internal/model/args.go",
        """type LinkArgs struct {
\tIP       string
\tHeader   http.Header
\tType     string
\tRedirect bool
}""",
        """type LinkArgs struct {
\tIP       string
\tHeader   http.Header
\tType     string
\tRedirect bool

\t// InternalTransfer is set only by trusted copy and offline-transfer paths.
\tInternalTransfer bool `json:"-" form:"-"`
}""",
    ),
    Replacement(
        "internal/op/fs.go",
        """\ttypeKey := args.Type""",
        """\ttypeKey := linkCacheTypeKey(args)""",
    ),
    Replacement(
        "internal/fs/copy_move.go",
        """\tlink, srcObj, err := op.Link(t.Ctx(), t.SrcStorage, t.SrcActualPath, model.LinkArgs{})""",
        """\tlink, srcObj, err := op.Link(t.Ctx(), t.SrcStorage, t.SrcActualPath, model.LinkArgs{InternalTransfer: true})""",
    ),
    Replacement(
        "internal/offline_download/tool/transfer.go",
        """\tlink, srcFile, err := op.Link(t.Ctx(), t.SrcStorage, t.SrcActualPath, model.LinkArgs{})""",
        """\tlink, srcFile, err := op.Link(t.Ctx(), t.SrcStorage, t.SrcActualPath, model.LinkArgs{InternalTransfer: true})""",
    ),
)


def prepare_replacements(source_root: Path) -> list[tuple[Path, str]]:
    updates: list[tuple[Path, str]] = []
    for replacement in REPLACEMENTS:
        path = source_root / replacement.path
        if not path.is_file():
            raise PatchError(f"required upstream file is missing: {replacement.path}")

        content = path.read_text(encoding="utf-8")
        matches = content.count(replacement.before)
        if matches != 1:
            state = "already patched" if replacement.after in content else "upstream changed"
            raise PatchError(
                f"cannot patch {replacement.path}: expected one source anchor, "
                f"found {matches} ({state})"
            )
        updates.append((path, content.replace(replacement.before, replacement.after, 1)))
    return updates


def prepare_overlay(source_root: Path) -> list[tuple[Path, Path]]:
    copies: list[tuple[Path, Path]] = []
    for source in sorted(path for path in OVERLAY_ROOT.rglob("*") if path.is_file()):
        relative = source.relative_to(OVERLAY_ROOT)
        target = source_root / relative
        if target.exists():
            raise PatchError(f"overlay target already exists: {relative.as_posix()}")
        copies.append((source, target))
    if not copies:
        raise PatchError("overlay is empty")
    return copies


def apply_patch(source_root: Path) -> list[Path]:
    source_root = source_root.resolve()
    if not (source_root / "go.mod").is_file():
        raise PatchError(f"not an OpenList source tree: {source_root}")

    updates = prepare_replacements(source_root)
    copies = prepare_overlay(source_root)

    changed: list[Path] = []
    for path, content in updates:
        with path.open("w", encoding="utf-8", newline="\n") as output:
            output.write(content)
        changed.append(path)
    for source, target in copies:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        changed.append(target)
    return changed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="path to a clean OpenList source tree")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        changed = apply_patch(args.source)
    except (OSError, PatchError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    source_root = args.source.resolve()
    print("PikPak transfer patch applied:")
    for path in changed:
        print(f"  {path.relative_to(source_root).as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
