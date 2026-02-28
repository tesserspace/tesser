# 24. Bundle 导出/导入规范（Export / Import Bundle Spec）

> 目的：把 `19` 的“分享与复盘”旅程落到**可实现**的 bundle 格式与验证规则：可跨平台搬运、可校验、可脱敏、可恢复、可进度展示，并与 `13/21` 的安全与可复现约束一致。

## 1) 范围与目标

### 1.1 目标
- 支持把一个（或多个）run 的 **render replay** 能力在另一台机器上打开并复盘（不依赖原机器路径）。
- 可选支持 compute replay（只在许可与条件满足时），否则必须清晰降级。
- 导出/导入全程走 Job：可取消、可重试、显示进度与体积预估（见 `15/19`）。
- 默认安全：不包含凭据、token、用户绝对路径；日志/错误必须脱敏（见 `21`）。

### 1.2 非目标（一期不做）
- 不做“云端分享链接/协作”；仅本地文件 bundle。
- 不承诺自动打包超大数据集全量（数据切片只作为可选项，并受配额/许可约束）。

## 2) Bundle 类型（产品层）

- `render_replay_only`（默认）：仅包含 UI 复盘所需 artifacts（`run.json/metrics.json/series/trades/logs` 的安全子集）。
- `compute_replay_ready`（可选）：在 `render_replay_only` 基础上，补齐 compute replay 所需（RunSpec + dataset manifest ref + 可选数据切片或下载计划）。

> UI 必须明确展示 bundle 类型与“能否 compute replay”的判定依据（见 `13` 的 `replayability`）。

## 3) 文件容器与目录布局（硬要求）

### 3.1 容器格式
- 文件扩展名：`.tesserbundle`
- 容器：`zip`（跨平台/生态友好；实现简单；支持流式写入以便进度）
- 压缩：默认 `deflate`；允许未来版本引入 `zstd`（需 bump `bundle_format_version`）

### 3.2 Archive 内部布局（规范化路径；禁止绝对路径）

```
manifest.json
runs/<run_id>/run.json
runs/<run_id>/metrics.json
runs/<run_id>/series/...
runs/<run_id>/trades/...
runs/<run_id>/logs/...
datasets/<dataset_id>/manifest.json                (可选；仅指针)
datasets/<dataset_id>/manifests/<manifest_hash>.json (可选)
datasets/<dataset_id>/data/...                     (可选：数据切片)
reports/bench/...                                  (可选)
```

硬约束：
- archive 内所有路径必须是 **相对路径**，且必须拒绝 `..`、驱动器前缀、UNC、以及 symlink 逃逸（见 `21`）。
- `manifest.json` 必须位于根目录，且为导入校验的唯一入口。

## 4) `manifest.json`（Bundle Manifest，schema_version=1）

### 4.1 最小字段（硬要求）
- `schema_version`: number（固定 `1`；manifest 自身的 schema 版本）
- `bundle_format_version`: number（固定 `1`）
- `bundle_id`: string（UUID；用于去重/溯源/错误关联）
- `created_at_ms`: number
- `producer`: `{ app_version, protocol_version, os?, arch?, webview_version? }`
- `bundle_kind`: `render_replay_only|compute_replay_ready`
- `entries[]`: array（至少 1 个）
  - `entries[].kind`: `run_bundle|dataset_slice|dataset_plan|bench_report|redaction_rules`
  - `entries[].logical_name`: string（稳定标识；用于 UI 展示）
  - `entries[].path`: string（archive 内相对路径；必须规范化）
  - `entries[].format`: string（如 `json|jsonl|parquet|arrow_ipc_stream|zip`；用于 allowlist）
  - `entries[].schema_id?`: string（可选；对齐 `13` 的 `artifact_manifest.entries[].schema_id`）
  - `entries[].content_hash`: `{ algo: "sha256", hex }`
  - `entries[].uncompressed_size_bytes`: number
  - `entries[].compressed_size_bytes?`: number（若缺失，导入端必须从 zip central directory 读取）
- `total_uncompressed_size_bytes`: number（用于 quota gating；必须等于 entries uncompressed size 之和）
- `total_compressed_size_bytes?`: number（可选；用于 UX 展示与 zip-bomb 预检）
- `redaction`: `{ redaction_rules_version, redaction_rules_hash?, mode: default|debug, included_secrets: boolean }`
  - 语义：`redaction.included_secrets=true` 表示 bundle 内包含任何可复用的凭据/会话材料（API key/secret/token/cookie/session）；默认应为 false
  - 若 `redaction_rules_hash` 存在：必须在 `entries[]` 中包含对应 `redaction_rules` blob（用于导入端展示“实际脱敏规则”）
- `encryption?`（可选；当 `redaction.included_secrets=true` 时必须存在；否则仅在用户显式开启加密导出时存在）：
  - `{ enabled: true, encryption_scheme: "age"|"aes-gcm", recipients?: string[] }`
  - 约束：`redaction.included_secrets=true` 时 `encryption.enabled` 必须为 true

### 4.2 run entry（建议字段）
- `run_ref`: `{ run_id, run_spec_hash?, execution_profile_hash?, metrics_def_version?, dataset_manifest_hash?, dataset_fingerprint? }`

### 4.3 dataset entry（可选）

两种互斥方式（必须二选一，或都不提供）：

1) `dataset_slice`（可携带数据切片）
- `dataset_ref`: `{ dataset_id, manifest_hash }`
- `slice`: `{ time_range, symbols?, resolution?, partitions_included[] }`

2) `dataset_plan`（只携带“如何获取数据”的计划，不携带数据）
- `dataset_ref`: `{ dataset_id, manifest_hash }`
- `plan`: `{ vendor, venue, market, symbols[], resolution, time_range, license_hint?, normalize_profile? }`
- 约束：不得包含任何凭据；导入端可引导用户“下载同一数据”并展示进度（对齐 `16` 下载 Job）

## 5) 导出流程（Job：export_bundle）

### 5.1 UI 输入
- 选择：单 run / 多 run（Compare 页）
- 选择 bundle_kind：`render_replay_only`（默认）或 `compute_replay_ready`
- 选择日志级别：默认脱敏；debug 需显式勾选（仍必须脱敏但可保留更多上下文，见 `21`）
- 可选：是否包含 dataset slice（若许可）；若不包含则生成 `dataset_plan`

### 5.2 Host 行为（硬要求）
- 预估体积：基于 artifact_manifest + 可选 slice 体积，先给出 `units_total`（bytes），再开始写 zip。
- 原子写入：写入临时文件 → fsync → rename（见 `12`）。
- 内容哈希：边写入边计算 sha256，写回 `manifest.json`（manifest 最后写入）。
- 失败恢复：若中断，临时文件必须可清理；不得留下半成品占用配额。

## 6) 导入流程（Job：import_bundle）

### 6.1 校验（硬要求）
- 解压前校验：
  - `manifest.json` 可解析且 `bundle_format_version` 支持
  - `total_uncompressed_size_bytes` 不超过 workspace quota（见 `12/10`）
- 资源 DoS 防护（zip bombs；硬要求，默认值可配置）：
  - `manifest.json` 必须小于 `5 MiB` 且建议不压缩存储
  - 最大 entry 数：`<= 20_000`
  - 单 entry 最大解压后大小：`<= 1 GiB`
  - 总解压后大小上限：`<= 5 GiB`
  - 最大压缩比：`uncompressed_size_bytes / max(1, compressed_size_bytes) <= 200`
- 解压时校验：
  - 路径规范化（禁止路径穿越）
    - 规范化规则：`\\`→`/`；拒绝 `..`；拒绝 NUL；拒绝 drive-letter/UNC；拒绝尾随点/空格歧义（Windows）
    - 拒绝重复路径（zip 可包含同名 entry 多次）
  - `content_hash/size_bytes` 校验（不匹配即失败）
  - 拒绝未知大文件类型（按 allowlist；见 `21`）
  - 拒绝 symlink/reparse-point 类型 entry；并必须在全新 temp 目录解压校验后再原子移动进 workspace（避免目的目录已有 symlink 逃逸）
  - 哈希校验顺序（硬要求）：stream 解压 → 计数 `uncompressed_size_bytes` → sha256 → 比对 manifest → 通过后再 commit 到 workspace

### 6.2 落盘（硬要求）
- 导入到指定 workspace（或新建 workspace）。
- run 去重：
  - storage-level：以 `run_id` 为主键；并建议计算 `run_bundle_hash`（对该 run 目录下 entries 的 `{path,content_hash,uncompressed_size_bytes}` 做规范化后 sha256）用于内容去重
  - compute-level（避免重复 rerun）：`run_spec_hash + dataset_fingerprint(level,value) + engine_version`（用于 jobs 调度，不用于丢弃导入 artifacts）
- dataset 处理：
  - `dataset_slice`：导入为新 dataset 修订或 cache（需明确策略；一期可只用于 replay，不注册为全局 dataset）
  - `dataset_plan`：在 Datasets 页生成“可下载项”，用户确认后启动下载 Job（见 `16`）

## 7) 安全与隐私（对齐 `21`）

- 默认不包含：任何凭据、token、cookie、API key。
- 默认不包含：用户绝对路径；如需用于调试，仅以 hash/脱敏字段形式出现（见 `22` 的 `bench_extras.roots`）。
- 日志脱敏规则必须随 bundle 携带 `redaction_rules_version`，导入端必须展示“脱敏声明”。
- 导入端必须提示：bundle 可能包含策略参数与交易行为数据；用户确认后才落盘。

完整性 vs 真实性（必须提示）：
- `sha256` 校验仅保证“传输/存储完整性”，不保证来源真实性；若需要防篡改来源，后续版本可引入可选签名（不在一期范围）。

## 8) 错误码（与 D-024 对齐；占位）

建议最小命名空间：
- `BUNDLE.MANIFEST_INVALID`
- `BUNDLE.UNSUPPORTED_VERSION`
- `BUNDLE.HASH_MISMATCH`
- `BUNDLE.PATH_TRAVERSAL`
- `BUNDLE.QUOTA_EXCEEDED`
- `BUNDLE.DUPLICATE_RUN`

## 9) 与其它 PRD 的连接点

- `13`：run bundle 落盘布局、artifact_manifest、replayability
- `15`：导入/导出作为 Job（取消/重试/进度）
- `16`：dataset_plan → 下载/normalize/health/tiles 的复用
- `21`：脱敏、安全、路径规范化、凭据隔离
- `22`：bench 报告作为可选 entry（用于性能复现）
