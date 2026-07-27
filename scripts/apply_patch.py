#!/usr/bin/env python3
"""Apply the PikPak transfer and STRM batch-hook patches to OpenList."""

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
        "internal/op/fs.go",
        """\tsrcKey := Key(storage, srcDirPath)
\tdstKey := Key(storage, dstDirPath)
\tif !srcRawObj.IsDir() {
\t\tCache.linkCache.DeleteKey(stdpath.Join(srcKey, srcRawObj.GetName()))
\t\tCache.linkCache.DeleteKey(stdpath.Join(dstKey, srcRawObj.GetName()))
\t}
\tif !storage.Config().NoCache {
\t\tif cache, exist := Cache.dirCache.Get(srcKey); exist {
\t\t\tif srcRawObj.IsDir() {
\t\t\t\tCache.deleteDirectoryTree(stdpath.Join(srcKey, srcRawObj.GetName()))
\t\t\t}
\t\t\tcache.RemoveObject(srcRawObj.GetName())
\t\t}
\t\tif cache, exist := Cache.dirCache.Get(dstKey); exist {
\t\t\tif newObj == nil {
\t\t\t\tnewObj = &model.ObjWrapMask{Obj: srcRawObj, Mask: model.Temp}
\t\t\t} else {
\t\t\t\tnewObj = wrapObjName(storage, newObj)
\t\t\t}
\t\t\tcache.UpdateObject(srcRawObj.GetName(), newObj)
\t\t}
\t}

\tif ctx.Value(conf.SkipHookKey) != nil || !needHandleObjsUpdateHook() {
\t\treturn nil
\t}
\tif !srcObj.IsDir() {
\t\tgo objsUpdateHook(context.WithoutCancel(ctx), storage, dstDirPath, false)
\t} else {
\t\tgo objsUpdateHook(context.WithoutCancel(ctx), storage, stdpath.Join(dstDirPath, srcObj.GetName()), true)
\t}""",
        """\tsrcKey := Key(storage, srcDirPath)
\tdstKey := Key(storage, dstDirPath)
\tif !srcRawObj.IsDir() {
\t\tCache.linkCache.DeleteKey(stdpath.Join(srcKey, srcRawObj.GetName()))
\t\tCache.linkCache.DeleteKey(stdpath.Join(dstKey, srcRawObj.GetName()))
\t}
\tif !storage.Config().NoCache {
\t\tif cache, exist := Cache.dirCache.Get(srcKey); exist {
\t\t\tif srcRawObj.IsDir() {
\t\t\t\tCache.deleteDirectoryTree(stdpath.Join(srcKey, srcRawObj.GetName()))
\t\t\t}
\t\t\tcache.RemoveObject(srcRawObj.GetName())
\t\t}
\t\tif cache, exist := Cache.dirCache.Get(dstKey); exist {
\t\t\tif newObj == nil {
\t\t\t\tnewObj = &model.ObjWrapMask{Obj: srcRawObj, Mask: model.Temp}
\t\t\t} else {
\t\t\t\tnewObj = wrapObjName(storage, newObj)
\t\t\t}
\t\t\tcache.UpdateObject(srcRawObj.GetName(), newObj)
\t\t}
\t}

\thookPath := dstDirPath
\trecursiveHook := srcObj.IsDir()
\tif recursiveHook {
\t\thookPath = stdpath.Join(dstDirPath, srcObj.GetName())
\t}
\tif enqueueObjsUpdateHook(ctx, storage, hookPath, recursiveHook) {
\t\treturn nil
\t}
\tif ctx.Value(conf.SkipHookKey) != nil || !needHandleObjsUpdateHook() {
\t\treturn nil
\t}
\tgo objsUpdateHook(context.WithoutCancel(ctx), storage, hookPath, recursiveHook)""",
    ),
    Replacement(
        "internal/op/fs.go",
        """\tdstKey := Key(storage, dstDirPath)
\tif !srcRawObj.IsDir() {
\t\tCache.linkCache.DeleteKey(stdpath.Join(dstKey, srcRawObj.GetName()))
\t}
\tif !storage.Config().NoCache {
\t\tif cache, exist := Cache.dirCache.Get(dstKey); exist {
\t\t\tif newObj == nil {
\t\t\t\tnewObj = &model.ObjWrapMask{Obj: srcRawObj, Mask: model.Temp}
\t\t\t} else {
\t\t\t\tnewObj = wrapObjName(storage, newObj)
\t\t\t}
\t\t\tcache.UpdateObject(srcRawObj.GetName(), newObj)
\t\t}
\t}

\tif ctx.Value(conf.SkipHookKey) != nil || !needHandleObjsUpdateHook() {
\t\treturn nil
\t}
\tif !srcObj.IsDir() {
\t\tgo objsUpdateHook(context.WithoutCancel(ctx), storage, dstDirPath, false)
\t} else {
\t\tgo objsUpdateHook(context.WithoutCancel(ctx), storage, stdpath.Join(dstDirPath, srcObj.GetName()), true)
\t}""",
        """\tdstKey := Key(storage, dstDirPath)
\tif !srcRawObj.IsDir() {
\t\tCache.linkCache.DeleteKey(stdpath.Join(dstKey, srcRawObj.GetName()))
\t}
\tif !storage.Config().NoCache {
\t\tif cache, exist := Cache.dirCache.Get(dstKey); exist {
\t\t\tif newObj == nil {
\t\t\t\tnewObj = &model.ObjWrapMask{Obj: srcRawObj, Mask: model.Temp}
\t\t\t} else {
\t\t\t\tnewObj = wrapObjName(storage, newObj)
\t\t\t}
\t\t\tcache.UpdateObject(srcRawObj.GetName(), newObj)
\t\t}
\t}

\thookPath := dstDirPath
\trecursiveHook := srcObj.IsDir()
\tif recursiveHook {
\t\thookPath = stdpath.Join(dstDirPath, srcObj.GetName())
\t}
\tif enqueueObjsUpdateHook(ctx, storage, hookPath, recursiveHook) {
\t\treturn nil
\t}
\tif ctx.Value(conf.SkipHookKey) != nil || !needHandleObjsUpdateHook() {
\t\treturn nil
\t}
\tgo objsUpdateHook(context.WithoutCancel(ctx), storage, hookPath, recursiveHook)""",
    ),
    Replacement(
        "server/handles/fsmanage.go",
        """\t// Create all tasks immediately without any synchronous validation
\t// All validation will be done asynchronously in the background
\tvar addedTasks []task.TaskExtensionInfo
\tfor i, p := range req.Names {
\t\tif p == "" {
\t\t\tcontinue
\t\t}
\t\tt, err := fs.Move(c.Request.Context(), p, dstDir, len(req.Names) > i+1)
\t\tif t != nil {
\t\t\taddedTasks = append(addedTasks, t)
\t\t}
\t\tif err != nil {
\t\t\tcommon.ErrorResp(c, err, 500)
\t\t\treturn
\t\t}
\t}""",
        """\t// Create all tasks immediately without any synchronous validation.
\t// Direct operations collect exact hook targets; async transfer tasks keep
\t// using TransferCoordinator and dispatch their hooks after completion.
\tvar addedTasks []task.TaskExtensionInfo
\tbatchCtx, hookBatch := op.WithObjsUpdateHookBatch(c.Request.Context())
\tdefer hookBatch.Dispatch(c.Request.Context())
\tfor _, p := range req.Names {
\t\tif p == "" {
\t\t\tcontinue
\t\t}
\t\tt, err := fs.Move(batchCtx, p, dstDir)
\t\tif t != nil {
\t\t\taddedTasks = append(addedTasks, t)
\t\t}
\t\tif err != nil {
\t\t\tcommon.ErrorResp(c, err, 500)
\t\t\treturn
\t\t}
\t}""",
    ),
    Replacement(
        "server/handles/fsmanage.go",
        """\t// Create all tasks immediately without any synchronous validation
\t// All validation will be done asynchronously in the background
\tvar addedTasks []task.TaskExtensionInfo
\tfor i, p := range req.Names {
\t\tif p == "" {
\t\t\tcontinue
\t\t}
\t\tvar t task.TaskExtensionInfo
\t\tif req.Merge {
\t\t\tt, err = fs.Merge(c.Request.Context(), p, dstDir, len(req.Names) > i+1)
\t\t} else {
\t\t\tt, err = fs.Copy(c.Request.Context(), p, dstDir, len(req.Names) > i+1)
\t\t}
\t\tif t != nil {
\t\t\taddedTasks = append(addedTasks, t)
\t\t}
\t\tif err != nil {
\t\t\tcommon.ErrorResp(c, err, 500)
\t\t\treturn
\t\t}
\t}""",
        """\t// Create all tasks immediately without any synchronous validation.
\t// Direct operations collect exact hook targets; async transfer tasks keep
\t// using TransferCoordinator and dispatch their hooks after completion.
\tvar addedTasks []task.TaskExtensionInfo
\tbatchCtx, hookBatch := op.WithObjsUpdateHookBatch(c.Request.Context())
\tdefer hookBatch.Dispatch(c.Request.Context())
\tfor _, p := range req.Names {
\t\tif p == "" {
\t\t\tcontinue
\t\t}
\t\tvar t task.TaskExtensionInfo
\t\tif req.Merge {
\t\t\tt, err = fs.Merge(batchCtx, p, dstDir)
\t\t} else {
\t\t\tt, err = fs.Copy(batchCtx, p, dstDir)
\t\t}
\t\tif t != nil {
\t\t\taddedTasks = append(addedTasks, t)
\t\t}
\t\tif err != nil {
\t\t\tcommon.ErrorResp(c, err, 500)
\t\t\treturn
\t\t}
\t}""",
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
    updates: dict[Path, str] = {}
    for replacement in REPLACEMENTS:
        path = source_root / replacement.path
        if not path.is_file():
            raise PatchError(f"required upstream file is missing: {replacement.path}")

        content = updates.get(path)
        if content is None:
            content = path.read_text(encoding="utf-8")
        matches = content.count(replacement.before)
        if matches != 1:
            state = "already patched" if replacement.after in content else "upstream changed"
            raise PatchError(
                f"cannot patch {replacement.path}: expected one source anchor, "
                f"found {matches} ({state})"
            )
        updates[path] = content.replace(replacement.before, replacement.after, 1)
    return list(updates.items())


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
    print("OpenList patch bundle applied:")
    for path in changed:
        print(f"  {path.relative_to(source_root).as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
