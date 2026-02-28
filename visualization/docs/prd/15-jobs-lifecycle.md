# 15. Job 体系（Lifecycle, Persistence, Progress, Cancel/Retry/Resume）

## 1) 目标
- 所有长任务（run、下载、索引、LOD 构建、导出）统一走 Job 系统。
- Job 可取消、可重试、可恢复（重启不丢）。
- Job 的进度与日志可观测，错误可诊断。

### 1.1 非目标（避免膨胀）
- 不做分布式队列/远端计算调度（local-first）。
- 不承诺任意 Job 都能“断点续算到每一步”（优先保证一致性与可恢复性）。

## 2) Job 状态机（草案）
- `queued` → `running` → (`completed` | `failed` | `canceled`)
- 可选：`paused`（仅当确有需求）

### 2.1 状态不变量（必须满足）
- `completed`：所有输出产物落盘完成，索引事务提交；UI 可 render replay。
- `failed`：输出可部分存在，但必须被标记为“不可当作成功产物”；索引必须可解释（可定位失败原因）。
- `canceled`：必须停止计算/下载/写入；不得留下会被当作成功结果的半成品。

### 2.2 术语与标识（必须明确，否则无法保证幂等/恢复/索引一致性）
- `job_id`：Job 的稳定标识（UUID）。重试/恢复不变。
- `attempt`：同一 `job_id` 的执行尝试编号（从 1 开始递增）。每次重试/崩溃恢复都会产生新 attempt。
- `idempotency_key`：用于“去重同一意图”的键。它**不是**输出路径；它用于：
  - 防止重复入队（enqueue dedupe）
  - 防止重复写入同一意图的最终产物（commit dedupe）
- `run_id`：一次 backtest 结果的产物标识（用于 artifacts 目录与 runs index）。默认是新 UUID，但**可被** `idempotency_key`（例如 `run_spec_hash`）映射与复用。
- `dataset_id`：数据集标识（manifest 的主键）。
- `bundle_id`：导出包标识（一次 export 的产物标识）。
- `correlation_id`：贯穿 command→job→events→logs 的关联 ID（见 `10-principles-slas.md` 的可观测性要求）。

## 3) Job 类型
- `backtest_run`（Quick/Full）
- `dataset_download`
- `dataset_normalize`
- `dataset_index_build`（manifest/stats）
- `tiles_build`（LOD 金字塔）
- `export_bundle`

### 3.1 每类 Job 的输出与幂等 key（草案）

| job_type | 输出位置（示意） | 幂等 key（示意） |
| --- | --- | --- |
| `backtest_run` | `workspaces/<workspace_id>/runs/<run_id>/...` | `run_spec_hash`（默认） |
| `dataset_download` | `datasets/<dataset_id>/raw/...` | `exchange+symbol+range+interval` |
| `dataset_normalize` | `datasets/<dataset_id>/data/partitions/...` | `raw_fingerprint+normalize_config_hash` |
| `dataset_index_build` | `datasets/<dataset_id>/manifests/<manifest_hash>.json` + `datasets/<dataset_id>/manifest.json` + `datasets/<dataset_id>/index/...` | `fingerprint.fast+normalize_config_hash`（建议） |
| `tiles_build` | `cache/tiles/<dataset_id>/<manifest_hash>/<lod_profile>/<chunk>.parquet` | `manifest_hash+lod_profile`（建议） |
| `export_bundle` | `exports/<bundle_id>.zip` | `run_id+bundle_profile` |

## 4) 进度语义（必须一致）
- 进度事件必须至少包含：`job_id`、`job_type`、`status`、`attempt`、`seq`、`at_ms`、`correlation_id`、`units_done`、`units_total`（可为 `null`）、`phase`（当 `units_total=null` 时必填）。
- 不允许只发“百分比”而没有单位；单位必须能让用户理解（bars/events/bytes/chunks）。
- 进度必须可解释：`units_done/units_total` + `rate` + `eta`（若可估）
- 下载：字节/文件/分区维度
- 回测：bars/events 维度
- tiles：chunk/level 维度

### 4.1 进度不变量（硬要求）
- `units_done` 必须单调不减。
- `units_total` 允许为 `null`（表示 indeterminate）；若为 `null`，必须提供 `phase`（例如 download/normalize/index/tiles/backtest）。
- 终态事件（completed/failed/canceled）发出后，不得再发 progress/log。
- event 必须带 `seq`（每 job 单调递增）用于 UI 断线重连补齐与去重（与 `18-ipc-transport.md` 对齐）。

## 5) 持久化与一致性（必须先于并行与性能）

### 5.1 持久化范围
- Job 元数据与状态必须持久化（重启不丢）。
- 每次状态转换必须可重放（至少能解释“为什么这个 job 变成 failed/canceled”）。

### 5.2 事务边界（与 `12/13` 对齐）
- 大对象写入：临时文件 → fsync → rename 原子替换。
- 索引更新：SQLite 事务（同一事务内完成“输出存在性 + 状态更新”）。

### 5.3 崩溃恢复与避免双执行（占位，但必须实现）
- host 启动时扫描处于 `running` 且 lease 过期的 jobs，必须进入确定状态：
  - 进入 `queued`（可恢复）或 `failed`（不可恢复），并写入 `reason_code`（例如 `worker_lost`）。
- 对于支持分片提交的 job_type（例如 `tiles_build`），允许从已提交分片继续；否则恢复等价于创建新 attempt 从头执行，但必须保持幂等（见 `6.1`）。

## 6) 取消/重试/恢复
- 取消必须保证：索引一致性、不留下“半成品被当成成功产物”。
- 重试策略：幂等 key（避免重复写入 artifacts/tiles）。
- 崩溃恢复：重启后任务可恢复或安全失败，并提示用户下一步。

### 6.1 取消语义（硬要求）
- cancel 必须是“尽快停止 + 一致性优先”：
  - 允许丢弃未完成的 tiles/chunks；
  - 不允许把 partial 输出当作 completed；
  - UI 必须收到 `job.canceled` 事件或明确失败原因。

### 6.2 重试语义（硬要求）
- “重试”必须显式区分：
  - `retry`：同一 `job_id`，新 `attempt`，同 `idempotency_key`，目标是“把这次意图做成功”（尽可能复用已完成分片）
  - `rerun`：新 `job_id`（通常也会产生新 `run_id`），用于“意图发生变化”（RunSpec/参数变化导致的 hash 变化，或用户强制重跑）

## 7) IPC 集成（与 `18` 对齐，必须可实现）

### 7.1 Commands（invoke）最小集合（占位）
- `jobs.start({job_type, inputs, idempotency_key?, correlation_id, reuse_output?}) -> {job_id}`
- `jobs.cancel({job_id, correlation_id})`
- `jobs.retry({job_id, correlation_id})`
- `jobs.get({job_id})` / `jobs.list({filters...})`

> 幂等约定（占位，但必须明确实现并写入 API 文档）：
> - 若 `idempotency_key` 相同且存在 active job（queued/running），`jobs.start` 必须返回该 job 的 `job_id`（不创建新 job）。
> - 若存在 terminal job：
>   - `reuse_output=true` 时：允许直接返回已有 job_id（或返回“完成状态 + 产物引用”）；用于复用既有产物。
>   - 否则：创建新 job（新 job_id、新 attempt=1），并按 job_type 决定是否复用已提交分片（若可校验）。

### 7.2 Events（至少一次投递，需可去重）（占位）
- `job.progress` / `job.log`
- `job.completed` / `job.failed` / `job.canceled`
- 每条 event 必须包含：`job_id, job_type, status, attempt, seq, at_ms, correlation_id`
- 断线重连：UI 重新订阅时可携带 `{job_id,last_seq}`，host 返回缺失事件或“状态快照 + 最新 seq”。

## 8) 依赖决策（D-IDs）
- [D-014](03-decisions-open-questions.md)（持久化队列）
- [D-003](03-decisions-open-questions.md)（索引/事务）
- [D-013](03-decisions-open-questions.md)（streaming + 背压）

## 9) 验收（占位）
- `kill -9` 任意 Job 关键阶段后重启：要么可恢复继续，要么安全失败且不污染索引。
- cancel 在 stream 中途也必须正确（不会让 UI 持续增长内存；不会落盘半成品为成功）。
- 并发运行多个 jobs 时：进度语义一致，且不会出现索引竞争导致的“丢 run/重复 run”。
