# 13. RunSpec/Artifacts 数据模型、版本化与迁移

## 1) 目标
- 定义 `RunSpec`：一次 run 的完整输入（可 compute replay）。
- 定义 `Artifacts`：运行产物（可 render replay）。
- 定义 schema 版本化与迁移：历史 runs 不“报废”。

## 1.1 范围与原则
- 本文输出的是“数据契约”：字段、文件、目录、版本化、兼容规则。
- 本文不强行决定具体序列二进制格式（Parquet vs Arrow IPC），但必须定义：**小对象 JSON，大对象列式/二进制**（见 [D-004](03-decisions-open-questions.md)）。
- 任意一次 run 至少要落盘“最小审计包”（见 [D-002](03-decisions-open-questions.md)），使 Reviewer/Engineer 能复盘与对比。

## 2) RunSpec（草案字段）

> RunSpec 是 compute replay 的输入；其 schema 必须可版本化且可迁移（见 [D-015](03-decisions-open-questions.md)）。

### 2.0 schema_version 边界（硬规则）
- 本 PRD 采用“单一版本”策略：`schema_version` 作为 **run bundle schema version**，覆盖：
  - `run.json` / `metrics.json` / `logs/log.jsonl` / `diff/diff.json` / 以及本 PRD 定义的所有 artifacts
  - `run_spec.schema_version` 必须与 `run.json.schema_version` 一致
- 若未来需要拆分版本（RunSpec/Artifacts 独立演进），必须先在 [D-015](03-decisions-open-questions.md) 决策并补迁移策略。

### 2.1 RunSpec 顶层字段（规范化草案）
- `schema_version`：RunSpec schema 版本（与 Artifacts schema 可一致也可独立，但必须显式）
- `run_id`：UUID（产物目录主键）
- `engine`
  - `kind`: `quick` | `full`
  - `engine_version`: `{ git_commit, cargo_lock_hash }`
  - `options`: 引擎特定选项（必须可序列化；不得携带绝对路径）
- `execution_profile`：完整对象（字段口径见 `14-execution-semantics-disclosure.md`）
- `execution_profile_hash`：用于对比/归因的稳定 hash（算法固定：JCS canonical JSON + sha256；与 `run_spec_hash` 相同）
- `dataset_ref`
  - `dataset_id`
  - `fingerprint`: `{ level: fast|strict, value: string }`
  - `dataset_manifest_ref`：指向某个不可变 manifest 修订（用于复现定位；fingerprint 单独存放）（见 D-018）
    - `manifest_hash`: string
    - `uri?`: string（例如 `dataset://...`；禁止绝对路径）
  - `symbols`: string[]
  - `timeframe`: interval / resolution
  - `partitions`: 可选（用于严格复现；否则以 fingerprint 绑定的数据集为准）
- `strategy`（见 [D-012a](03-decisions-open-questions.md)）
  - `kind`: `builtin` | `rpc` | `wasm` | `script`（一期通常只用 builtin）
  - `name`
  - `params`（TOML/JSON 表；必须可序列化）
  - `source`：可选（例如策略仓库版本/文件 hash；不得默认包含敏感信息）
- `created_at`, `created_by`（可选）
- `notes`, `tags`（可选）

### 2.1.1 稳定意图标识（必须产出）
- `run_spec_hash`：RunSpec 的稳定 hash（用于幂等与审计）
  - canonicalization：采用 JCS（JSON Canonicalization Scheme）输出 canonical JSON
  - 算法：`sha256(canonical_json_bytes)`
  - 约束：任何会影响计算结果的字段必须纳入 canonicalization 输入；不得在 hash 外“暗含默认值”

> 同理，`execution_profile_hash` 必须采用同一 canonicalization+hash 方案。

### 2.1.2 determinism（必须记录任何非确定性来源）
- `determinism`
  - `seed`：可选但推荐（若策略/优化器使用随机性则必填）
  - `rng`：可选（PRNG 名称/版本；若使用随机性则必填）
  - `parallelism`：可选（影响并行归约顺序的配置，如线程数）
  - `clock`：可选（如“使用哪种时间戳口径/截断规则”）

### 2.2 RunSpec 禁止项（硬约束）
- 禁止包含：任何凭据/secret/token。
- 禁止把绝对路径作为“复现依据”（用 root-relative + fingerprint）。
- 禁止引入非确定性源（随机数）而不显式记录 seed。

## 3) Artifacts 分层（默认审计包 + 可选明细）

### 3.1 最小审计包（必须保存）
- `run.json`（RunSpec 快照 + 版本信息 + 产物清单摘要）
- `metrics.json`
- `equity_series`（下采样版本 + 原始可选）
- `trades/fills`（分页/索引可选）
- `logs`（脱敏）

### 3.1.1 落盘布局（占位，需与 `12` 对齐）
- `workspaces/<workspace_id>/runs/<run_id>/run.json`
- `workspaces/<workspace_id>/runs/<run_id>/metrics.json`
- `workspaces/<workspace_id>/runs/<run_id>/series/equity.lod.parquet`（或 Arrow IPC）
- `workspaces/<workspace_id>/runs/<run_id>/trades/trades.parquet`（分页查询）
- `workspaces/<workspace_id>/runs/<run_id>/logs/log.jsonl`

### 3.1.2 `run.json`（建议字段）
- `run_id`, `schema_version`
- `run_spec`（完整 RunSpec）
- `job`（追溯生命周期；与 `15` 对齐）
  - `job_id`, `attempt`, `correlation_id`, `idempotency_key`（若有）
- `artifact_manifest`（**必须**存在，用于稳定 UI/搬运/校验）
  - `entries[]`: `{ kind, logical_name, path, format, schema_id?, content_hash, size_bytes, row_count?, time_range? }`
  - `total_size_bytes`
- `environment_fingerprint`（最小集；口径见 `10-principles-slas.md`）
  - `os`, `arch`, `webview_version?`, `gpu_class?`
  - `app_version?`, `protocol_version?`, `build_profile?`
- `replayability`
  - `render`: bool（通常为 true）
  - `compute`: bool
  - `reason_code?`, `hint?`（compute=false 时必填）
- `commit`
  - `started_at_ms?`, `finished_at_ms?`, `committed_at_ms?`
  - `commit_seq?`（可选，用于索引一致性/事件对齐）
- `status`: `queued|running|completed|failed|canceled`（与 jobs 对齐；非终态不得进入“completed runs index”）
- `error`：结构化错误（若失败；见 [D-024](03-decisions-open-questions.md)）

> 原子一致性规范（硬要求）：
> - `status=completed` 的 `run.json` 必须最后写入（temp+fsync+rename），并与 runs index 更新同一事务提交（见 `12-storage-portability.md` 与 `15-jobs-lifecycle.md`）。

#### 3.1.2.1 `artifact_manifest.entries[].path` 规则（可移植性硬要求）
- 禁止绝对路径。
- 推荐两种之一（择一并在实现里统一）：
  - run-root 相对路径（例如 `series/equity.lod.parquet`）
  - storage URI（例如 `workspace://<workspace_id>/runs/<run_id>/series/equity.lod.parquet`）

### 3.2 可选明细（用户显式启用）
- 全量事件（tick/LOB、订单生命周期、撮合细节）
- 更高分辨率的 series（或按需生成）

> 可选明细必须受配额/保留策略控制（见 [D-020](03-decisions-open-questions.md)），并在 UI 中明确空间预估。

## 4) 版本化与迁移策略
- `schema_version`：显式数字/semver（D-015）
- 兼容读取：新版本应尽量读取旧版本（forward-compatible reader）
- 无法 compute replay 的降级：仍可 render replay，并明确提示原因（缺数据/语义变化/版本不兼容）

### 4.1 Render replay vs Compute replay
- render replay：仅依赖 artifacts（series/trades/metrics/logs），不重新跑引擎。
- compute replay：依赖 RunSpec + dataset fingerprint + 引擎语义与版本；会生成新的 artifacts 或覆盖策略由 Job/Storage 规则决定。

### 4.2 Diff 报告（占位，但必须标准化）
- 当 compute replay 超出容差（见 [D-016](03-decisions-open-questions.md)）必须生成 diff：
  - `execution_profile_diff`
  - `dataset_fingerprint_diff`
  - `version_diff`
  - 指标差/交易差摘要

**最小落盘要求（占位）：**
- `workspaces/<workspace_id>/runs/<run_id>/diff/diff.json`：包含上述归因字段与摘要（字段名可迭代，但必须可被 UI 稳定读取）。

### 4.3 最小 schema（占位，但必须落到字段级）

#### `metrics.json`（最小字段集）
- `schema_version`
- `metrics_def_version`（来自 Execution Profile）
- `summary`（一期最小建议；字段含义见 `23-metrics-definitions-tolerances.md`）：
  - `{ starting_equity, ending_equity, total_return_pct, cagr_pct?, max_drawdown_pct, sharpe_ratio?, sortino_ratio?, calmar_ratio?, annualized_volatility_pct?, avg_daily_return_pct?, return_samples_count, fills_count, fees_paid?, realized_pnl?, start_time_ms, end_time_ms }`
- `metrics_context?`（建议；用于审计与 UI 展示）：`{ metrics_def_version, return_sampling, periods_per_year, days_per_year, risk_free_rate_annual, pnl_mode, trade_model_version?, pnl_attribution_version? }`
- `metrics_debug?`（建议；用于诊断）：`{ carried_forward_days_count, missing_days_count, skipped_returns_count, null_reasons? }`
- `notes?`

#### `logs/log.jsonl`（最小字段集，且必须脱敏）
- 每行一个 JSON 对象：`{ at_ms, level, target?, message, fields?, correlation_id?, redacted: true, redaction_rules_version }`

#### `diff/diff.json`（最小字段集）
- `schema_version`
- `tolerance_ref`: `{ metrics_tol, series_tol }`（或引用 D-016 的版本化容差配置）
- `execution_profile_diff`, `dataset_fingerprint_diff`, `version_diff`
- `metrics_delta_summary`
- `reason_attribution`（string/enum）

## 5) 依赖决策（D-IDs）
- [D-015](03-decisions-open-questions.md)（schema 版本化）
- [D-002](03-decisions-open-questions.md)（最小审计包）
- [D-016](03-decisions-open-questions.md)（determinism）
- [D-012a](03-decisions-open-questions.md)（策略标识）
- [D-004](03-decisions-open-questions.md)（大序列传输格式）
- [D-024](03-decisions-open-questions.md)（错误码体系）
- [D-018](03-decisions-open-questions.md)（Dataset manifest）
- [D-020](03-decisions-open-questions.md)（配额/保留策略）

## 6) 验收（占位）
- 至少两次 schema bump 后仍可加载旧 runs（render replay 必可用）。
- compute replay 不可用时，必须有标准化提示字段（原因分类 + 修复建议）。
- 最小审计包可支持 Runs Compare 的核心对比（指标表 + equity + trades 摘要）。
