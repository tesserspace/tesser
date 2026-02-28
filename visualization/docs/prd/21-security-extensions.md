# 21. 安全、凭据与扩展模型（Security & Extensions）

> 原则：local-first ≠ 无安全。默认“最小权限 + 明确披露 + 可审计 + 可撤销”，并与 `18` 的 loopback_ws/pull-based 流式传输一致。

## 1) 目标与非目标

### 1.1 目标
- 下载凭据安全（不明文落盘；可撤销/轮换）。
- logs/artifacts/bundle 脱敏与隐私控制（默认最小化）。
- 扩展（RPC/WASM）有明确威胁模型、隔离边界与可复现标识。

### 1.2 非目标
- 不提供“对抗本机管理员/恶意内核”的强安全（本地桌面应用的常识边界）。
- 不承诺插件/脚本的强沙箱隔离（除非明确选用 WASM/WASI 并限制能力集）。

## 2) 凭据与敏感信息（硬要求）

### 2.1 凭据存储
- 凭据（API key/secret/token/cookie/session）禁止明文落盘。
- 存储位置：
  - macOS：Keychain
  - Windows：Credential Vault
  - Linux：Secret Service（或用户明确选择“仅内存会话”）

Linux caveat（必须披露）：
- Linux keyring 依赖 D-Bus session + 已解锁的 Secret Service 实现（如 GNOME Keyring/KWallet/libsecret）。
- 在 headless/WSL/容器/无 session bus 时可能不可用；此时必须降级为“仅内存会话”，或要求用户显式选择“加密落盘”（用户口令/`aes-gcm`）。
- keyring 可能触发解锁弹窗/阻塞；非交互场景必须无弹窗安全降级（memory-only），并在 UI 明确提示当前凭据持久化状态。
- Key 的命名空间必须包含：`app_id + venue + account_label`，并支持“撤销/轮换”。

### 2.2 凭据使用（最小化暴露）
- 凭据只能在 host（Rust）侧使用；前端不得接触明文凭据。
- 下载类 Job（见 `16` 的 `dataset_download`）：前端只传 `credential_ref`（不传 secret）。
- 任何错误/日志/崩溃报告都不得包含 secret（见 §4）。

### 2.3 导出 bundle
- 默认不包含凭据与任何可复用 session。
- 若用户显式选择“包含凭据”（一般不推荐）：
  - 必须二次确认（明确风险文案）
  - 必须加密（AEAD；v1 允许：`aes-gcm`、`age`；密钥来自用户输入或 OS 安全存储）
  - 必须在 bundle manifest 写入审计字段（见 `24`）：
    - `redaction.included_secrets=true`
    - `encryption.enabled=true`
    - `encryption.encryption_scheme`
    - `created_at_ms`

## 3) 扩展模型（策略/RPC/WASM）

### 3.1 Strategy 扩展（阶段性）
- v1：内置策略（`tesser-strategy`）+ params（RunSpec 记录）
- vNext：RPC（只读/超时/限流/结构化 schema 校验）

### 3.2 WASM/WASI 插件（未来）
- 插件必须有明确能力边界（文件/网络/时间/随机数）与资源限制（CPU/内存/超时）。
- 插件必须有可复现标识（hash/version），并写入 RunSpec/Artifacts 的披露信息（见 `13/14`）。

### 3.3 RPC 扩展的最小安全契约（vNext 预留）
- 默认只读：RPC 仅能接收 market data / candles / portfolio snapshot，输出 signals（不得直接触发文件/网络）。
- 必须限制：
  - 超时：每次调用 `timeout_ms`（写入 Execution Profile/RunSpec）
  - 并发：`max_concurrency`
  - 速率：`rate_limit`
- 必须记录可复现标识：
  - `rpc_endpoint_ref`（禁止包含 token；如需 token 走凭据系统）
  - `rpc_schema_hash`
  - `rpc_impl_version`（commit/hash）

## 4) IPC/流式传输安全（与 `18` 对齐）

### 4.1 loopback_ws（硬要求）
- 仅绑定 `127.0.0.1`/`[::1]`，禁止对外暴露。
- 使用随机 ephemeral port；无 active streams 时必须关闭 listener。
- `auth_token` 必须短 TTL、>=128-bit 随机；禁止出现在 URL（query/fragment）。
- 握手（与 `18` 对齐）：`auth_token` 必须通过 `Sec-WebSocket-Protocol`（`token_in="sec-websocket-protocol"`）携带；服务端只接受预期 subprotocol。
- 防浏览器/恶意网页连本机 WS：必须校验 `Origin` + `Host`（防 DNS rebinding）；Host 仅允许 localhost/127.0.0.1/[::1]。
- WS 层仅允许 `stream_id` 范围内的 pull/cancel；禁止暴露任意 command。
- 资源限制：最大连接数（默认 1～2）、最大并发 streams、速率限制；失败计数后断开。
- 日志脱敏：token 必须脱敏（见 §5）。
  - 必须限制 WS message 大小/频率；未知 message type 直接断开。

### 4.2 资源滥用防护（DoS）
- host 必须对以下维度设置上限并可配置：
  - 最大并发 streams
  - 单 stream 最大 `replay_window_chunks`
  - 单次 pull 最大 bytes（见 `18` 的 `stream_pull.max_bytes_per_pull`）
  - 单 chunk 最大 bytes（见 `18` 的 `stream_pull.max_chunk_bytes`）
  - 重拉窗口（见 `18` 的 `stream_pull.replay_window_chunks`）
  - idle 回收阈值（`stream_idle_timeout_ms`；见 `18` §8.3）
  - 最大连接数（默认 1～2）
- 超限必须返回结构化错误码（D-024），并在 UI 告知降级建议（例如减少叠加 run、降低 target_points）。

## 5) 日志、脱敏与隐私（硬要求）

### 5.1 默认脱敏规则
- 必须脱敏：`api_key`, `secret`, `token`, `auth`, `authorization`, `cookie`, `session`, `signature`。
- 必须脱敏：URL query 中疑似凭据字段（如 `key=`, `token=`），以及 header 值。
- 脱敏策略：保留前后少量字符用于排障（例如 `abcd…wxyz`），并标注 `redacted=true`（见 `13` 的 log schema）。

### 5.2 数据最小化
- 默认不记录：完整响应 body、完整下载 URL、完整用户路径（仅记录 root-relative + hash）。
- 用户显式开启 debug 才允许记录更多，但仍必须脱敏并标注 `debug_mode=true`。

## 6) 文件系统与导入安全（硬要求）
- 导入必须走显式用户选择（文件对话框）；不得静默扫描用户目录。
- 所有落盘必须限定在 roots（workspace_root/dataset_root/cache_root）下（见 `12`/`16`）。
- 任何来自 UI 的路径输入都必须被 host 校验并规范化（防止路径穿越）。
- Archive 导入（`.tesserbundle`，见 `24`）必须额外满足：
  - Zip-slip 防护：规范化分隔符（`\\`→`/`）、拒绝 `..`/绝对路径/UNC/drive-letter/NUL、拒绝重复路径、限制最大路径深度与长度
  - Symlink/reparse-point 防护：拒绝 symlink 类型 entry；解压到全新 temp 目录校验完再原子移动进 workspace
  - Zip-bomb 防护：限制最大 entry 数、单文件最大解压后大小、总解压后大小、最大压缩比；解压必须流式并可中止
  - 完整性校验：stream 解压→计数字节→sha256→比对 `manifest.json`→通过后 commit（中途失败不得落半成品）
  - Allowlist（硬要求）：仅允许导入 `24` manifest 声明的 `entries[].format` 在以下集合内：
    - `json`、`jsonl`、`parquet`
    - 允许未来追加，但必须 bump 版本并更新本 allowlist

## 7) 威胁模型（最小集）
- 数据外泄（策略/日志/导出包）
- 任意代码执行（脚本/插件）
- 凭据泄露（下载/配置）
- 供应链风险（第三方依赖）

## 8) 依赖决策（D-IDs）
- [D-012b](03-decisions-open-questions.md)（扩展机制）
- [D-007](03-decisions-open-questions.md)（下载/凭据）
- [D-013](03-decisions-open-questions.md)（IPC/大数据流式通道）
- [D-024](03-decisions-open-questions.md)（错误码体系）

## 9) 安全验收（草案）
- logs/artifacts 默认不包含任何 secret（提供测试样例与自动扫描）。
- 导出 bundle 默认不包含凭据；若用户选择包含，则必须加密并二次确认。
- 若 bundle 包含敏感信息（`redaction.included_secrets=true`）：必须加密（v1：`age` | `aes-gcm`），并写入审计字段（见 `24` 的 bundle manifest）。
- loopback_ws 不对外监听；token 不出现在 URL；任意 stream 可取消且不会导致内存无界增长（与 `18`/`10` 对齐）。
- RPC（若启用）必须启用超时/限流/schema 校验，并在 artifacts 中记录其配置与版本信息。
- `protocol.get_info().stream_pull` 必须完整返回（含 `seq_start/max_bytes_per_pull/max_chunk_bytes/replay_window_chunks`）。
- `StreamRef.transport.loopback_ws` 必须包含 `token_in + expires_at_ms`，且 `transport.url` 不能含 token。
