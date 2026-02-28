# 18. IPC 与数据传输（Control + Binary Streaming）

> 本文定义 UI ↔ host（Rust）之间的“协议合约”：可演进、可诊断，并能支撑 `17` 的大数据流式查询与 pixel-bound 可视化。

## 1) 目标与非目标

### 1.1 目标
- 控制面：小 payload、强类型、易调试、schema 可演进（兼容读取）。
- 大序列：二进制、可流式、可背压、可取消、可观测（必须贯穿 `correlation_id`）。
- 与 Job 体系一致：事件至少一次投递、可去重、支持断线重连（见 `15-jobs-lifecycle.md`）。

### 1.2 非目标
- 不做远端/分布式 RPC（local-first）。
- 不承诺“二进制流按字节断点续传”；断线后允许通过重新发起 query 恢复（依赖 determinism + cache，见 `17`）。

## 2) 通道设计（3 通道）
- commands（invoke）：启动 job、查询元数据、分页查询、发起数据查询（返回 `stream_ref`，类型 `StreamRef`）
- events：job.progress/job.log/job.completed/job.failed/job.canceled（至少一次）
- binary stream：series/tiles/分页表格的 Arrow IPC bytes（分片传输）

> 底层实现允许多种（Tauri channel / loopback WS / file），但对 UI 暴露统一的抽象：`StreamRef`（见 §8）。

### 2.1 Phase 1 默认方案（范围控制，避免膨胀）
- Phase 1（二进制流）默认实现：`loopback_ws`（原因：可稳定承载 binary + 可做协议级背压）。
- `tauri_channel`：作为可选优化（若 Tauri 能稳定传输 binary + 背压语义可测，再启用）。
- `file`：仅用于“小结果兜底”（不承诺边收边画；见 §8.1），避免阻塞主路径。

### 2.2 `loopback_ws` 安全硬要求（P0）
- 仅绑定 `127.0.0.1` / `[::1]`，禁止 `0.0.0.0`。
- 使用随机 ephemeral port；无 active streams 时必须关闭 listener。
  - 每个 `stream_id` 必须有 `auth_token`（>=128-bit 随机；短 TTL；可一次性）。
  - WS 握手必须携带 token（推荐 `Sec-WebSocket-Protocol`，避免落在 URL/日志里）。
    - token 编码必须满足 header token 字符集（建议 `hex` 或 `base64url-no-pad`；禁止包含 `=`/空格等）。
    - token 必须“一次性”：握手成功后立即失效，避免重放。
- WS 层只允许“读指定 `stream_id` + pull/cancel”类消息；**禁止**把 commands 全量暴露到 WS。
- 资源限制：最大连接数必须 `<= protocol.get_info().limits.max_active_streams`（默认 4），最大并发 streams、速率限制；失败计数后断开。
- 日志脱敏：token 必须脱敏（对齐 `21-security-extensions.md`），并且任何日志/遥测不得记录 `Sec-WebSocket-Protocol` 原文。
- `transport.url` 禁止携带 token（query/fragment 等），避免落入日志/崩溃报告。
  - Phase 1 选择：**每个 `stream_id` 一个 WS 连接**（`StreamRef.transport` 携带 per-stream `auth_token`）；最大连接数应与 `protocol.get_info().limits.max_active_streams` 一致（默认 4）。

## 3) 背压与取消（硬要求）

### 3.1 背压（定义）
- UI 停止消费（不读取 stream）时，host 必须在有限缓冲后阻塞/降速，而不是无限堆积。
- Phase 1 必须提供**协议级背压机制**（P0，二选一，推荐 pull-based）：
  - **Pull-based（推荐）**：UI 主动拉取：`streams.pull({stream_id, next_seq, max_bytes, correlation_id, request_id})`，host 在 pull 时返回下一段数据（chunk）或 pull-level 错误；stream 的终止必须由 `streams.closed(...)` 明确宣告（见 §8.2.1）。
  - Credit/Ack-based：host push chunk，但 UI 必须 ack/credit，host 只能在 credit 内发送（更复杂，非 Phase 1 默认）。
- host 必须对每个 stream 设置 `max_bytes_per_pull` 与 `max_chunk_bytes` 上限（默认值在 D-023 benchmark 后固化）。

### 3.2 取消（定义）
- UI 取消后，host 必须：
  - 尽快停止生产 bytes，并释放该 stream 的内存/文件句柄/任务；
  - 给出确定终止信号：`streams.closed(reason_code=canceled)`（见 §8.2.1）。

## 4) 数据格式策略
- JSON：控制面、少量指标/元数据
- Arrow IPC：大向量 series、tiles、分页表格（见 D-004）

### 4.1 Arrow 约束（硬要求）
- 大序列必须用 **Arrow IPC streaming**（非 file），以支持边收边画与取消。
- Arrow schema 演进：
  - 允许新增 nullable 列；
  - 禁止随意变更 dtype/语义（需通过 version/`schema_id` 管理，见 D-015/D-018）。

## 5) 协议版本化与兼容策略

### 5.1 `protocol_version`（硬要求）
- UI 启动后必须调用：`protocol.get_info()`，返回：
  - `protocol_version`
  - `app_version`
  - `capabilities`（如：支持的 `stream_transport.kinds[]`、`default_stream_transport_kind`、是否支持 Arrow streaming）
  - `limits`（Phase 1 必填；用于 UI 做并发控制与提示）：
    - `max_active_streams`: number
  - `stream_pull`（当支持 pull-based 时必填）：
    - `seq_start`: number（一期固定为 `1`）
    - `max_bytes_per_pull`: number
    - `max_chunk_bytes`: number
    - `replay_window_chunks`: number（允许 UI 在丢包重试时重拉最近 N 个 chunk；超出则报错）
    - `stream_idle_timeout_ms`: number（UI 不消费/断联时的回收超时；用于 UI 侧展示与调试）
- 若 `protocol_version` 不兼容：UI 必须阻止关键功能并给出可理解错误（含 `correlation_id` 与升级建议）。

### 5.2 Schema 演进规则（硬要求）
- 允许：新增字段（可选/nullable）、新增 enum variant（旧端需容忍 unknown）。
- 禁止：删除必填字段；更改字段类型导致旧端崩溃；“语义变化但不 bump 版本”。

## 6) Commands：通用 envelope 与错误

### 6.1 请求建议字段（最小 envelope）
- `protocol_version`
- `correlation_id`
- `request_id`（UUID；用于调试与日志关联）

### 6.2 错误 shape（最小）
- `{ code: string, message: string, details?: any, hint?: string, correlation_id: string }`
- `code` 必须来自错误码注册表（D-024）。

## 7) Events：Job 事件投递与重连

### 7.1 Job events schema（最小字段集合）
- 所有 job events 必须至少包含：
  - `job_id`, `job_type`, `status`
  - `attempt`, `seq`, `at_ms`
  - `correlation_id`
- `job.progress` 额外字段：
  - `units_done`, `units_total`（可为 `null`）, `phase`（当 `units_total=null` 时必填）
  - `rate?`, `eta_ms?`
- 投递语义：至少一次（at-least-once）；UI 必须用 `(job_id, attempt, seq)` 去重。

### 7.2 断线重连（必须可实现）
- UI 可携带 `{job_id, last_seq}` 重新订阅。
- host 返回：
  - `snapshot`: 当前 job 状态（含 attempt 与最新 seq）
  - `events[]?`: 可选补齐事件（若 host 有持久化事件日志；否则返回空并让 UI 以 snapshot 为准）

## 8) Binary streaming：`StreamRef` 与 framing

### 8.1 `StreamRef`（统一抽象）
查询类 commands（例如 `candles.query/series.query/tiles.query`，见 `17`）必须返回：
- `stream_ref`: `{ stream_id, format, transport, schema_id? }`
  - `stream_id`: UUID
  - `format`: `"arrow_ipc_stream"`（一期固定）
  - `transport.kind`（占位，D-013 决策后固化一种或多种）：
    - `tauri_channel`
    - `loopback_ws`
    - `file`（仅适用于小结果兜底；不承诺边收边画）

`transport` 字段建议（占位）：
- `loopback_ws`: `{ url, auth_token, token_in: "sec-websocket-protocol", expires_at_ms }`
- `file`: `{ uri, size_bytes, delete_on_close: true }`（uri 必须在 app 管控的 temp/cache root 下；不得由 UI 传入任意路径）

### 8.2 framing（最小要求）
Phase 1 采用 pull-based 时，最小协议为：
- UI → host：`streams.pull({ stream_id, next_seq, max_bytes, correlation_id, request_id })`
- host → UI：
  - `streams.chunk({ stream_id, seq, bytes })` 或 `streams.eof({ stream_id, seq })`
  - `streams.error({ stream_id, seq, code, message, correlation_id, request_id, details?, terminal? })`（控制消息；`loopback_ws` 下为 text JSON）
  - `streams.closed({ stream_id, seq, reason_code, correlation_id, request_id?, error_code? })`（控制消息；stream 终止时必发）

说明（P0）：
- **一次 pull 只对应一次响应**：host 必须在 `chunk` / `eof` / `error` 三者中返回其一（恰好一个），以便 UI 明确该次 pull 已完成。
- `streams.eof` 是 data-plane 的“完成标记”（`loopback_ws` binary frame `kind=eof`）：表示该 stream 不再产生新的 bytes；它不是生命周期终止信号。
- `streams.chunk` 的 `payload` 拼接后必须构成**完整可解码**的 Arrow IPC stream；`streams.eof`/`kind=eof` 不属于 Arrow bytes。
- **stream 的终止以 `streams.closed(...)` 为准**；`streams.closed(reason_code=eof)` 必须在最后一个 `streams.eof` 之后发送（Phase 1：随后关闭该 stream 的 WS）。
- WS close / TCP FIN 不是有效终止信号；若 UI 观察到连接关闭但未收到 `streams.closed`，必须按 `reason_code=error` 处理（建议错误码：`STREAM.TRANSPORT_CLOSED`）。

硬约束（P0）：
- 对每个 stream：chunk 的“逻辑序列”为 `seq_start, seq_start+1, ...`；正常推进（`next_seq == expected_next_seq`）时返回的 `seq` 必须递增 1。
- 当 UI 主动请求 replay（`next_seq < expected_next_seq`）时，host 允许重发旧 `seq`（因此“投递的 seq”可能重复）；UI 必须按 `(stream_id, seq)` 去重。
- 若发生重试/重传：同一 `(stream_id, seq)` 的 `bytes` 必须字节级一致（不得同 seq 不同 payload）。
- 默认要求“有序投递”（UI 不需要乱序重排缓冲）；若未来要支持乱序，必须先补充重排缓冲上限与错误语义。
- `bytes` 分片大小上限：`max_chunk_bytes` 由 `protocol.get_info()` 暴露。
- Phase 1 推荐：`max_bytes_per_pull == max_chunk_bytes`（每次 pull 至多返回 1 个 chunk），避免 UI 误解“一次 pull 会返回多个 chunk”。
- chunking 建议：尽量按 Arrow message/record-batch 边界切分，减少前端拼接与拷贝。

#### 8.2.1 `streams.error` / `streams.closed` 语义（一期必须固化）

终止时序（P0，Phase 1 / pull-based）：
- 正常完成：`pull` → `chunk`* → `eof` → `closed(reason_code=eof)` → close WS
- pull 参数错误（可恢复）：`pull` → `error(terminal=false)`（不发送 `closed`；UI 修正参数后重试）
- pull 触发的不可恢复错误：`pull` → `error(terminal=true)` → `closed(reason_code=error, error_code=<streams.error.code>)` → close WS
- 取消/超时/producer 异常：`cancel` 或 host timeout/producer error → `closed(reason_code=canceled|idle_timeout|error)` → close WS（若有 in-flight pull，UI 以收到 `closed` 视为该 pull 已结束）

- `streams.error` 是 `streams.pull` 的响应（text frame JSON）：
  - `seq`：回显该次 `streams.pull.next_seq`
  - `terminal=false`（默认）：表示“本次 pull 的请求不合法/不可满足”，stream 仍保持可用，UI 可修正参数后重试 pull
  - `terminal=true`：表示 stream 进入终止状态；host **不得再发送**任何 `chunk/eof`，并且必须紧随其后发送 `streams.closed(reason_code=error, error_code=code)` 后关闭该 WS 连接
- `streams.closed`（text frame JSON）是 stream 的**唯一终止信号**（必须恰好发送一次）：
  - `reason_code ∈ eof|canceled|idle_timeout|error`
  - `seq`：终止时的 `expected_next_seq`（即“最后一个 chunk 的 seq + 1”；若无 chunk 则 `seq=seq_start`）
  - 注意：`streams.error.seq` 始终回显本次 pull 的 `next_seq`；`streams.closed.seq` 始终是终止时的 `expected_next_seq`，两者在“terminal error”场景下可能不同
  - 发送后：host 必须关闭该 stream 对应的 WS（Phase 1 每 stream 一连接），并把该 stream 标记为 inactive（用于关闭 listener 与释放资源）

#### 8.2.2 `loopback_ws` 的 on-wire 约定（一期推荐，避免 base64）

> 目标：让 “bytes 是 binary” 在不同实现中没有歧义，同时保持调试与性能可控。

- WS 连接上存在两类消息：
  - text frame：JSON（控制面：`streams.pull/streams.cancel` + 错误/调试小消息）
  - binary frame：数据面（`streams.chunk/streams.eof`）
- binary frame 采用固定 32 bytes header + payload：
  - `magic[4]`：ASCII `TSR1`
  - `kind[1]`：`1=chunk`，`2=eof`
  - `reserved[1]`：`0`
  - `header_len[2]`：big-endian，固定 `32`
  - `seq[8]`：u64 big-endian
  - `stream_id[16]`：UUID bytes（与 Rust `uuid::Uuid::as_bytes()`一致）
  - `payload[...]`：chunk bytes（Arrow IPC stream 的连续片段；不得跨 Arrow IPC message 边界切割）
    - `kind=eof` 时 `payload_len=0`，且 `seq = last_chunk_seq + 1`（若从未发送过 chunk，则 `seq=seq_start`）。
- Phase 1（每 stream 一连接）：
  - `stream_id` 用于冗余校验：必须与该连接对应的 `StreamRef.stream_id` 一致；不一致视为协议错误并断开连接。
    - 说明：Phase 1 下 `stream_id` 在 header 中是冗余字段（用于自检/调试），为未来 multiplex 预留。
  - `streams.pull` / `streams.cancel` 在 `loopback_ws` 下通过 WS text frame 发送 JSON（仍遵守 commands envelope：`correlation_id/request_id`），避免把全量 commands 暴露到 WS（见 §2.2）。
  - 每个 stream 同时只允许 1 个 in-flight 的 `streams.pull`：UI 在收到 `chunk/error(terminal=false)`（或连接异常关闭）前不得再次 pull。
  - 一旦收到 `eof` 或 `error(terminal=true)`：stream 进入 closing 状态；UI 不得再发送 `pull`，必须等待紧随其后的 `streams.closed(...)`（仍允许发送 `streams.cancel`，但应视为 no-op）。
  - `streams.error` / `streams.closed` 一律通过 WS text frame JSON 发送（binary frame 仅用于 `chunk/eof`）。

`next_seq` 语义（P1，但实现前必须固化）：
- `seq_start=1`；正常情况下 UI 必须按 `1,2,3,...` 连续拉取。
- UI 若请求 `next_seq < expected_next_seq`：
  - 若 `next_seq ∈ [max(seq_start, expected_next_seq - replay_window_chunks), expected_next_seq - 1]`：host 允许重发（bytes 必须一致；`expected_next_seq` 不前进）。
  - 否则：返回 `streams.error(code=STREAM.SEQ_TOO_OLD, terminal=true, ...)` → `streams.closed(reason_code=error)`（Arrow 连续字节流不可恢复）。
- UI 若请求 `next_seq > expected_next_seq`：返回结构化错误（建议错误码：`STREAM.SEQ_INVALID`）。

`max_bytes` 语义（P0）：
- UI 传入的 `max_bytes`：
  - 若 `max_bytes <= 0`：返回结构化错误（建议错误码：`STREAM.MAX_BYTES_INVALID`）
  - 若 `max_bytes > max_bytes_per_pull`：host 必须 clamp 到 `max_bytes_per_pull`（并可在日志记录一次）或返回 `STREAM.MAX_BYTES_EXCEEDED`（二选一，默认 clamp）
- host 不得把 Arrow message 切碎到无法解码：
  - `max_bytes` 仅约束 binary frame 的 `payload`（不含 32 bytes header）。
  - host 必须保证其生成的 Arrow IPC stream 中任一 message（schema/record batch/dictionary batch）在 IPC wire-format 上的单条消息大小 `<= max_chunk_bytes`（也 `<= max_bytes_per_pull`）：
    - 计入：continuation marker + length prefix + message + body + padding 的完整字节数
    - 做法：通过控制 record batch 大小进行 re-batch/slice；若确实无法满足（例如单行数据过大），则终止 stream（结构化错误码建议：`STREAM.MESSAGE_TOO_LARGE`）。
  - “Arrow IPC message 边界”指 Arrow IPC stream 中按 length-prefix（含 continuation marker）划分的完整消息；chunk payload 必须包含一个或多个完整消息，不能只发半个。
  - 若（UI 请求或 clamp 后的）effective `max_bytes` 过小，导致无法发送一个完整 Arrow message：返回结构化错误 `STREAM.MAX_BYTES_TOO_SMALL`，提示 UI 采用默认 `max_bytes_per_pull`。

### 8.3 取消与资源回收
- UI 取消：
  - command：`streams.cancel({ stream_id, correlation_id, request_id })`
  - host：停止生产、释放资源、发送 `streams.closed(reason_code=canceled)` 并关闭该 stream 的 WS；cancel 必须幂等（重复 cancel 返回 ok）
    - 若 stream 已处于 `closed`：重复 cancel 仅返回 ok，不得再次发送 `streams.closed`。
- host 必须设置超时回收：
  - UI 断联/不消费导致的悬挂 stream，超过 `stream_idle_timeout_ms` 必须回收并记录（脱敏）日志。
  - idle timeout 的终止信号：`streams.closed(reason_code=idle_timeout)`。
 - 避免 UI 卡死（硬要求）：
   - 若存在等待中的 `streams.pull`（long-poll/阻塞等待数据），当 cancel/idle-timeout/producer error 发生时，host 必须通过发送 `streams.closed(...)` 并关闭连接，显式解阻该 pull。

建议补充（与 `15` 对齐）：
- `jobs.cancel` 必须级联关闭关联 streams（若该 stream 由 job 产生/依赖）。

### 8.4 断线后的恢复策略（一期建议）
- 不承诺 byte-level resume；断线后 UI 通过重发 query 恢复：
  - tiles/LOD 查询：重发相同 query（因 determinism + cache，常为 cache hit；见 `17`）
  - 分页表格：使用 cursor 重拉

## 9) 依赖决策（D-IDs）
- [D-013](03-decisions-open-questions.md)（IPC/streaming）
- [D-004](03-decisions-open-questions.md)（传输格式）
- [D-014](03-decisions-open-questions.md)（Job）
- [D-024](03-decisions-open-questions.md)（错误码体系）
- [D-023](03-decisions-open-questions.md)（SLA 测量口径）

## 10) 验收（草案）
- 1e6 点序列端到端传输+渲染满足 `10` 的 SLA，且中途取消不泄露内存。
- UI 停止消费时：host 内存不无界增长（背压有效），并能在 idle 超时后回收。
- 协议版本不匹配时：有可理解的错误提示（含 `correlation_id`）。
