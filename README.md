# OpenList PikPak 转存与 STRM 增强版

这是一个非官方的 OpenList Docker 构建仓库，当前方向是增强 PikPak 内部转存以及 STRM 批量操作后的本地生成体验。仓库不复制或长期维护 OpenList 源码；GitHub Actions 会获取上游最新稳定 Release，应用一组受检查的补丁，通过测试后发布 `linux/amd64` 和 `linux/arm64` 镜像到当前仓库的 GHCR。

当前补丁包含三部分：

1. **PikPak 多线程转存**：只加速以 PikPak 为源的内部转存。
2. **STRM 批量写入钩子完整性修复**：同存储批量移动、复制或合并多个目录时，聚合每个成功项目的精确更新路径，避免只扫描最后一个目录而导致本地 `.strm` 落盘缺失。
3. **STRM 批量目录有界并行**：对同批操作中路径互不重叠的顶层目录并行执行写入后钩子，减少多个独立媒体目录逐个扫描的等待时间。

PikPak 多线程参数只作用于以下路径：

- OpenList 网盘间复制或移动中的下载阶段。
- 离线下载完成后的目标存储转存阶段。
- 普通代理下载、直链访问和媒体播放不启用这组参数。
- 只作用于 `PikPak` 驱动，不作用于 `PikPak Share`。

STRM 增强位于 OpenList 通用文件操作和写入后钩子层，不依赖 PikPak 驱动；关闭全局 `Handle hook after writing` 时不会增加扫描。跨存储异步任务仍由上游的 `TransferCoordinator` 在任务完成后触发钩子。

批量目录并行只覆盖本补丁接管的**同存储批量移动、复制和合并**。它不会改变单个目录内部的上游深度优先递归方式，也不会改变手动 STRM 扫描。调度前会清洗并去重路径；同一存储中只要出现相同路径、父子路径或根目录重叠，整批会保守地退回串行。OpenList 的 `Handle hook rate limit` 大于 `0` 时也会自动串行，避免每个 worker 建立独立 limiter 后放大总请求速率。

这里的 STRM 本地保存不是 OpenList 的网盘“复制”任务：视频和音频通常只在服务器本地生成包含播放 URL 的小型 `.strm` 文本；`ass`、`srt`、`vtt`、`sub` 等下载类型才会读取源文件并保存到本地。

## 默认参数

| 环境变量 | 默认值 | 有效范围 | 说明 |
| --- | ---: | ---: | --- |
| `PIKPAK_TRANSFER_CONCURRENCY` | `10` | `0..64` | 每个活跃 RangeReader 的请求并发；`0` 关闭 PikPak 多线程 |
| `PIKPAK_TRANSFER_PART_SIZE_MB` | `32` | `4..256` | 每个下载分片的大小，单位为 MiB |
| `OPENLIST_BATCH_HOOK_CONCURRENCY` | `2` | `1..4` | 同批不重叠顶层目录的写入后钩子 worker 数；`1` 关闭目录并行 |

PikPak 两个参数只在第一次 PikPak 转存时读取；批量钩子并发参数在每次批量调度时读取。空值使用默认值；非法、负数或越界值会回退默认值。修改变量后应重新创建容器。

两个参数的组合还受每个活跃 RangeReader `2048 MiB` 的名义缓冲上限保护；超过时会保留并发数、自动降低分片大小并记录 warning。例如 `64 × 256 MiB` 会调整为 `64 × 32 MiB`。

这里故意使用 `PIKPAK_TRANSFER_PART_SIZE_MB=32`，而不是无单位的 `PIKPAK_TRANSFER_PART_SIZE=33554432`。OpenList 的 `Link.Concurrency` 和 `Link.PartSize` 都是 `int`，补丁在赋值前会完成范围检查，避免负数或溢出值进入 Downloader。

实际并发不一定等于配置值，大致受以下三者中的最小值限制：

```text
配置并发数
ceil(当前读取范围 / 分片大小)
当前可用的全局 MAX_CONCURRENCY 令牌
```

OpenList Downloader 的内存量级约为每个活跃 RangeReader 的 `Concurrency * PartSize`。默认值约为 `10 * 32 MiB = 320 MiB`。多个转存任务和目标驱动的并行 RangeReader 可能继续叠加，所以建议先保留 `MAX_CONCURRENCY=64`。

当内存缓存启用时，实际分片大小还可能被 `MAX_BLOCK_LIMIT` 限制。24 GB 内存机器的自动值通常为 64 MiB，默认的 32 MiB 不会被截断；示例 Compose 显式设置为 64。

## 自动发布

[build.yml](.github/workflows/build.yml) 每 6 小时查询一次 `OpenListTeam/OpenList` 的最新稳定 Release。流水线会：

1. 将 tag 解析到确定的上游 commit。
2. 对补丁脚本和 overlay 计算内容哈希。
3. 比较精确 tag、版本 tag 和 `latest` 的 manifest digest；全部一致时跳过，别名缺失或过期时自动重建修复。
4. 精确检出上游源码并运行带断言的补丁器；任何锚点不匹配都会失败。
5. 运行补丁器单测、Go 格式检查和受影响包的测试。
6. 使用 Buildx 构建并发布 AMD64 与 ARM64 镜像。

镜像会获得三类 tag：

```text
ghcr.io/<owner>/<repo>:latest
ghcr.io/<owner>/<repo>:v4.2.4
ghcr.io/<owner>/<repo>:v4.2.4-u84ecda35aae2-p<补丁哈希>
```

最后一种 tag 同时标识上游提交和补丁内容，适合固定部署版本。上游构建仍会获取当时的前端和基础镜像；需要严格冻结二进制时，应在部署中固定 Actions 构建摘要里的 manifest digest。手动勾选 `force_build` 会重新构建并覆盖同名精确 tag。新上游版本若与补丁冲突，Actions 会失败，已有的 `latest` 不会被新镜像覆盖。

Actions 页面也可以手动运行工作流并填写某个稳定 tag，例如 `v4.2.3`。手动构建旧版本不会更新 `latest`，除非 tag 输入留空。

## 建立 GitHub 仓库

在 GitHub 创建一个公开的空仓库，例如 `openlist-pikpak-multithread`，然后在本目录执行。公开分发修改镜像时，补丁源码也应保持可访问。

```bash
git init -b main
git add .
git commit -m "feat: build OpenList PikPak multithread images"
git remote add origin https://github.com/<你的用户名>/openlist-pikpak-multithread.git
git push -u origin main
```

首次 push 会触发构建。工作流只使用当前仓库的 `GITHUB_TOKEN`，不需要额外 PAT。仓库必须允许 Actions 写入 Packages；若镜像要免登录拉取，在第一次构建后进入 GitHub Package 设置，将可见性改为 Public，并确认 Package 已关联这个源码仓库。

GitHub 的 schedule 使用 UTC，且可能延迟。公共仓库若连续 60 天没有活动，GitHub 也可能自动停用定时工作流，需要在 Actions 页面重新启用。

## 部署

把 [compose.example.yml](compose.example.yml) 中的镜像所有者和仓库名改成实际值，并全部使用小写；工作流生成的 GHCR 路径会强制转成小写。与你当前配置对应的核心部分如下：

```yaml
services:
  openlist:
    image: ghcr.io/<你的用户名>/openlist-pikpak-multithread:latest
    container_name: openlist
    user: "0:0"
    volumes:
      - ./data:/opt/openlist/data
      - ./temp/qBittorrent:/opt/openlist/data/temp/qBittorrent
      - /mnt/alist:/mnt/alist
      - /mnt/media:/mnt/media
    ports:
      - 172.17.0.1:11524:5244
    environment:
      - UMASK=022
      - PIKPAK_TRANSFER_CONCURRENCY=10
      - PIKPAK_TRANSFER_PART_SIZE_MB=32
      - OPENLIST_BATCH_HOOK_CONCURRENCY=2
      - MAX_CONCURRENCY=64
      - MAX_BLOCK_LIMIT=64
    restart: unless-stopped
```

示例统一使用你已实际验证可正常启动的 Compose 环境变量列表语法，并保留 `- UMASK=022`。Compose 规范上列表与映射写法通常等价；这里不把先前启动失败归因于 YAML 语法本身，而是采用已验证可用的部署形式。修改这些值后建议使用 `--force-recreate` 重新创建容器，而不只是 restart。

更新容器：

```bash
cd /opt/openlist
docker compose pull openlist
docker compose up -d --force-recreate --no-deps openlist
docker compose ps openlist
docker exec openlist ./openlist version
curl -fsS http://172.17.0.1:11524/ping
```

`openlist version` 的 `Go Version` 应包含 `linux/arm64`。若 GHCR Package 保持私有，服务器必须先登录；使用具有 `read:packages` 权限的 token：

```bash
read -rsp 'GitHub package read token: ' CR_PAT
echo
printf '%s' "$CR_PAT" | docker login ghcr.io -u '<GitHub 用户名>' --password-stdin
unset CR_PAT
```

第一次从 PikPak 发起复制后，日志应出现：

```text
[pikpak] transfer multirange configured: concurrency=10, part_size=32 MiB
```

这表示补丁参数已经进入 PikPak 的内部转存路径。实际连接数仍应结合 `iftop -nP` 和 OpenList debug 日志观察。

生产环境建议固定精确 tag 或 manifest digest，而不是长期直接跟随 `latest`。升级前先记录当前镜像并一致性备份 OpenList 数据；备份 SQLite 数据时应先停止容器：

```bash
cd /opt/openlist
stamp="$(date +%Y%m%d-%H%M%S)"
image_id="$(docker inspect openlist --format '{{.Image}}')"
docker inspect openlist --format 'image_ref={{.Config.Image}} image_id={{.Image}}' > "image-$stamp.txt"
docker image inspect "$image_id" --format 'repo_digests={{json .RepoDigests}}' >> "image-$stamp.txt"
docker compose stop openlist
tar -czf "/opt/openlist-backup-$stamp.tgz" data docker-compose.yml
docker compose start openlist
```

回滚时，把 Compose 中的 `image` 改为上一次成功构建的精确 tag，或者恢复官方镜像，然后重新创建容器：

```yaml
image: ghcr.io/<你的用户名>/openlist-pikpak-multithread:v4.2.3-u<上游提交>-p<补丁哈希>
```

```bash
docker compose pull openlist
docker compose up -d --force-recreate --no-deps openlist
```

OpenList 启动时可能升级持久化数据结构。跨上游版本降级若出现兼容问题，应同时恢复该镜像升级前的配套 `data` 备份，不能只切换镜像。

## 安全边界

旧补丁复用了 `LinkArgs.Type == "transfer"`，但 `Type` 同时可以来自外部下载请求的 `?type=` 查询参数。公开服务的访问者因此可能主动触发多线程请求。

本仓库改用带 `json:"-" form:"-"` 的 `InternalTransfer` 字段，并为普通链接和内部转存使用不同的 link-cache 命名空间。只有 OpenList 的复制和离线转存调用点能设置该标记，外部 `?type=transfer` 不会开启多线程，也不会命中内部转存缓存。

PikPak 偶尔可能对 Range 请求返回 `200` 而不是 `206`。启用新镜像后，建议先对几次大文件转存抽样核对源和目标文件大小及哈希，再长期无人值守运行。出现持续的 `200/416`、`429/503`、速度下降或校验失败时，先设置 `PIKPAK_TRANSFER_CONCURRENCY=0` 并重新创建容器；必要时回滚镜像。本补丁只优化源端读取，目标网盘的上传实现仍可能是端到端瓶颈。

`Alias` 驱动会继续传递 `LinkArgs`；当前上游的 `Crypt` 驱动会重新创建空的 `LinkArgs`，因此用 Crypt 包裹 PikPak 时这个补丁不会生效。

## 本地验证

补丁器测试：

```bash
python3 -m unittest discover -s tests -v
```

对一份干净的 OpenList 源码应用补丁：

```bash
git clone --depth 1 --branch v4.2.4 https://github.com/OpenListTeam/OpenList.git upstream
python3 scripts/apply_patch.py upstream
git -C upstream diff --check
```

补丁器先验证所有上游锚点和 overlay 目标，再开始写文件，不会在结构不匹配时只应用一半。它也会拒绝对同一源码重复应用。

## 上游与许可证

OpenList 上游项目：[OpenListTeam/OpenList](https://github.com/OpenListTeam/OpenList)。本仓库发布的是修改后的 OpenList 构建，沿用 GNU Affero General Public License v3.0。镜像标签中的上游 commit 和补丁哈希可定位到对应的完整源码与修改。
