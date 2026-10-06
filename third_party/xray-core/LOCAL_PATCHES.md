# 本地 Xray-core 补丁（third_party/xray-core）

基线：上游 wyx2685/xray-core 提交 `63db1dc9e9e2`（2025-12-02，即 Xray 25.12.2
fork，原 go.mod replace 所指版本）。

## 为什么 vendor

该 fork 基线包含 2025-12-02 当天合入的 XTLS Vision 改动（PR #5179
"Check TLS record isComplete"），其后上游在 2025-12 ~ 2026-09 陆续修复了这批
改动引入/暴露的三个 Vision 直拷贝/splice 缺陷，而 fork 长期未跟进：

1. **#5391（2025-12-08）"Fix enabled uplink splice flag by mistake"**——
   错误地启用了 uplink 方向的 splice 标志；
2. **#5737（2026-03-22）"Defer Splice handoff until write completes"**——
   padding→direct 切换时立即置 `CanSpliceCopy=1` 并进行 splice 接管，
   Vision writer 中尚未写出的缓冲帧字节丢失，表现为大文件下载
   HTTP 200 后在 ~5.5KB 处截断（curl exit 18）；
3. **#6834（2026-09-27）"Suppress outer CloseNotify after switching to
   direct copy"**——direct copy 切换后关闭本地 TLS conn 会发出错误的
   close_notify，导致客户端 TLS BAD_RECORD_MAC 连接中断。

三个缺陷共同构成 Linux 实机上 VLESS+REALITY+Vision 大文件传输间歇性截断。
本目录在上列 fork 基线之上 cherry-pick 了这三个上游修复（见下）。

## 应用补丁

| 上游 commit | 内容 | 备注 |
|---|---|---|
| 903214a (#5391) | 注释掉 uplink splice 标志误启用 | 原样套用 |
| f926ee4 (#5737) | 将 splice 接管延迟到 Vision writer 写完缓冲帧 | 原样套用 |
| e5e85ca (#6834) | direct copy 切换后抑制外层 close_notify | 适配：本 fork 无 stats 连接包装，`SuppressOuterCloseNotify` 直接对 conn 断言 `CloseNotifySuppressor`；reality/tls 的 Conn/UConn 增加 `suppressCloseNotify` |

`main` 分支（含全部修复）无法直接升级使用：其已将 quic-go 切换为
apernet fork v0.61.x 并删除旧 transport headers 包，与 V2bX 依赖的
apernet/hysteria v2.6.4 与 sing-box_mod（sagernet/quic-go v0.55）不兼容。

## 与上游的已知差异

- `SuppressOuterCloseNotify` 不经过 `stat.TryUnwrapStatsConn`（本 fork 无该包装）。
