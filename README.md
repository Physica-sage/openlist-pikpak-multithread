# OpenList PikPak 转存加速版

本仓库仅保留 **PikPak 内部转存多线程加速**。不再包含自定义 STRM 修复、批量移动钩子、目录并行扫描或 STRM 诊断日志；这些功能恢复为所构建 OpenList 版本的上游行为。需要继续研究 STRM 时，应使用单独分支或项目，不能混入这个镜像。

当前适配基线为 **OpenList v4.2.7**，上游提交 `3b28270a4066ea52943a69ecddd850d83f7e8bdc`。自动构建仍每 6 小时检查最新稳定 Release，不追随未发布的 `main` 提交；不兼容的新版本会停止构建，不覆盖已发布镜像。

## 保留的加速范围

- 仅对 `PikPak` 驱动作为来源的 OpenList 内部跨存储复制、移动和离线下载后转存配置多 Range 下载。
- 普通下载、直链访问和媒体播放不会被这组参数启用多线程。
- 不修改 `PikPak Share`，不修改目标网盘的上传实现。
- 使用不可从 JSON/Form 输入设置的 `LinkArgs.InternalTransfer` 标记，普通访问与内部转存使用不同链接缓存。
- 兼容 v4.2.7 的代理/重定向、IP 和 User-Agent 缓存隔离，不移除上游新增的缓存维度。
- `Alias` 会传递参数；上游 `Crypt` 重建 `LinkArgs`，因此通过 Crypt 包装 PikPak 的来源仍不在这个补丁的加速范围内。

## 参数

| 环境变量 | 默认值 | 范围 | 说明 |
| --- | ---: | --- | --- |
| `PIKPAK_TRANSFER_CONCURRENCY` | `10` | `0..64` | 每个内部转存读取器的下载并发；`0` 关闭加速 |
| `PIKPAK_TRANSFER_PART_SIZE_MB` | `32` | `4..256` | 每片大小，单位 MiB |

参数首次用于 PikPak 转存时读取，修改后需重新创建容器。无效值回退到默认值。单读取器的 `Concurrency × PartSize` 上限为 2048 MiB，超出时自动缩小分片并记录警告。

默认缓冲预算约为 `10 × 32 MiB = 320 MiB`，多个转存任务的内存占用会叠加。全局 `MAX_CONCURRENCY` 和 `MAX_BLOCK_LIMIT` 仍可能限制实际并发及分片大小，它们不是本补丁新增的变量。

不再提供 `OPENLIST_BATCH_HOOK_CONCURRENCY`、`OPENLIST_BATCH_HOOK_NOT_FOUND_TIMEOUT`、`OPENLIST_BATCH_HOOK_DEBUG`；旧 Compose 中可删除这些无效变量。上游自身的 STRM 功能和“写入操作后触发目录更新钩子”设置不受本补丁修改。

## 自动构建和发布

[build.yml](.github/workflows/build.yml) 获取最新稳定版，解析并核对上游提交，验证补丁后发布 `linux/amd64` 与 `linux/arm64` 镜像和二进制。PikPak 补丁与 overlay 内容参与哈希，纯 PikPak 版与之前混合 STRM 的版本会产生不同精确标签。

```text
ghcr.io/physica-sage/openlist-pikpak-multithread:latest
ghcr.io/physica-sage/openlist-pikpak-multithread:v4.2.7
ghcr.io/physica-sage/openlist-pikpak-multithread:v4.2.7-u<上游提交前12位>-p<补丁哈希>
```

这些是构建成功后的标签格式，不表示所有标签当前均已发布。生产环境建议使用构建摘要中的精确标签或 digest。手动指定旧稳定版不会更新 `latest`，旧版本不一定与当前补丁兼容。

GitHub Release 使用 `patched-<精确镜像标签>`，包含 AMD64/ARM64 压缩包及 SHA-256 校验。工作流使用 GitHub 的仓库令牌发布，无需额外 PAT。

## 部署

参考 [compose.example.yml](compose.example.yml)，保留自己的数据卷、端口及运行用户。更新前备份数据和旧镜像 digest；新版本可能迁移数据库，回滚时必要的话同时恢复旧数据。

```bash
docker compose pull openlist
docker compose up -d --force-recreate --no-deps openlist
docker exec openlist ./openlist version
```

只执行 `restart` 不会切换镜像。首次 PikPak 内部转存应有类似日志：

```text
[pikpak] transfer multirange configured: concurrency=10, part_size=32 MiB
```

出现持续的 `200/416` Range 异常、限流或校验失败时，先设置 `PIKPAK_TRANSFER_CONCURRENCY=0` 并重新创建容器排除加速因素。升级后先用少量文件核对源端和目标端大小及哈希。

## 验证补丁

```bash
python3 -m unittest discover -s tests -v
git clone --depth 1 --branch v4.2.7 https://github.com/OpenListTeam/OpenList.git upstream
python3 scripts/apply_patch.py upstream
git -C upstream diff --check
cd upstream
GOMAXPROCS=1 go test -p 1 ./drivers/pikpak ./internal/op ./internal/fs ./internal/offline_download/tool
```

补丁器先验证所有锚点与 overlay 目标，再开始写入，拒绝重复应用。测试另行检查不改写 STRM、任务组和批量 HTTP 接口。

Coder 环境执行编译时需对整个编译进程树限速，不能只使用 `nice`。可用单核亲和性、`GOMAXPROCS=1`、`GOFLAGS=-p=1` 与 `cpulimit --monitor-forks --limit=40 --foreground` 组合限制负载，且不要并行运行其他编译任务。

## 上游与许可

上游：[OpenListTeam/OpenList](https://github.com/OpenListTeam/OpenList)。修改后构建沿用 GNU Affero General Public License v3.0，精确镜像标签同时记录上游提交和补丁哈希以定位对应源码。
