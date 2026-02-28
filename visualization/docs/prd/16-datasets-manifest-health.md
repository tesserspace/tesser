# 16. Dataset 管理（Manifest/Fingerprint/Preview/Health + Download/Normalize）

> 关键词：**数据集一等公民**、**可复现引用**、**不全量扫描也能预览/质量判断**、**下载/规范化/索引/LOD 全链路可观测**。

## 1) 目标与非目标

### 1.1 目标（必须达成）
- Dataset 是一等公民：可预览、可校验、可版本化、可复现引用（RunSpec 必须能引用到“某一版 manifest/fingerprint”）。
- **不依赖全量扫描**即可给出“可用性判断”：时间范围、分区覆盖、基础质量告警、规模估计（行数/体积）。
- 支持 “Download → Normalize → Index/Manifest → Health → Tiles/LOD（后台）” 闭环；所有长任务走 Job（见 `15`）。
- 具备强可诊断性：进度准确、错误码稳定、可导出 health 报告与（脱敏）日志（见 [D-024](03-decisions-open-questions.md)）。
- **Manifest 不可变**：strict fingerprint、health report 等“后置生成/可再生成”的派生物必须写 sidecar（不得回写既有 manifest 文件与 `manifest_hash`）。

### 1.2 非目标（一期不做或不承诺）
- 不承诺任意数据源都可下载（一期只做 crypto 的少数 venue；见 [D-007](03-decisions-open-questions.md)）。
- 不承诺“任何情况下都能 100% 无损 normalize”；若源数据缺字段/口径不一致，必须在 manifest 的 `semantics_disclosure` 里显式披露（与 `14` 对齐）。
- 不做分布式数据湖/远端计算（local-first）。

## 2) 概念模型：Dataset / Manifest / Revision / Ref

### 2.1 Dataset 是“逻辑集合”，Manifest 是“可复现快照”
- `dataset_id`：逻辑数据集主键（用于 UI 列表、缓存键、下载/ETL 目标）。
- `manifest`：描述“这一版数据集内容与语义”的不可变快照（schema/分区列表/stats/语义披露等）。
- `manifest_hash`：该 manifest 的不可变“修订版本 ID”（hash），用于定位与搬运（算法：JCS canonical JSON + sha256；与 `13` 一致）。
- `dataset_manifest_ref`：RunSpec 里引用数据集的最小可复现指针（见 `13`），**只负责指向某个 manifest 修订**，至少包含：
  - `manifest_hash`
  - `uri?`：例如 `dataset://<dataset_id>/manifests/<manifest_hash>.json`（可移植定位提示；禁止绝对路径）

> 核心约束：**dataset_id 不足以复现**；RunSpec 必须绑定某个 manifest_hash（或等价不可变版本）。

### 2.2 面向未来市场的扩展点（crypto → A股/美股）
manifest 与 dataset_id 需要能容纳不同市场的最小共同字段：
- `asset_class`：`crypto` / `cn_stock` / `us_stock` / `futures` / ...
- `venue`：交易所/券商/数据供应商标识（如 `binance`, `bybit`, `sse`, `nasdaq`）
- `instrument`：规范化后的品种标识（展示可保留原样，但 `dataset_id` 必须 canonicalize；见 §3）
- `data_kind`：`candles` / `trades` / `orderbook_l2` / ...
- `resolution`：`1m`/`1d`/`tick`/`l2` 等（短期可只实现一种，但 schema 与路径要可扩展）

### 2.3 标识与 hash 的关系（必须明确，否则会破坏复现/缓存/对比）

| 名称 | 用途 | 是否必须写入 RunSpec | 是否可后置生成 | 典型落盘位置 |
| --- | --- | --- | --- | --- |
| `manifest_hash` | 定位某次 manifest 修订（复现“当时看到的那版描述”） | 是（通过 `dataset_manifest_ref`） | 否（manifest 一旦生成就固定） | `datasets/<dataset_id>/manifests/<manifest_hash>.json` |
| `fingerprint.fast` | 快速内容校验/比对（避免全量 hash） | 是（见 `13` 的 `dataset_ref.fingerprint`） | 否（随 manifest 生成） | manifest 内字段 |
| `fingerprint.strict` | 强内容证明（审计/benchmark/分享） | 视需求（建议在分享/benchmark 时必填） | 是（Job，可取消） | `datasets/<dataset_id>/fingerprints/<manifest_hash>.json`（sidecar） |

## 3) dataset_id 规范（建议草案，需最终固化）

> 本节在实现前需要在 [D-026](03-decisions-open-questions.md) 下固化“最终规范”，否则会导致目录结构与缓存键重构成本极高。

### 3.1 组成（推荐）
- `dataset_id = <asset_class>.<venue>.<market?>.<instrument>.<data_kind>.<resolution>.<schema_id?>`
  - 示例（crypto spot 1m candle）：`crypto.binance.spot.btcusdt.candles.1m.v1`
  - 示例（美股 1d candle）：`us_stock.nasdaq.aapl.candles.1d.v1`
- 字符集：`[a-z0-9._-]`（全部小写）。
- `instrument` 规范化：upper→lower、去空格；如包含 `/`、`:` 等字符，必须按规则转义/映射（见 D-026）。
- 版本段：末尾 `.v1` 表示 **dataset_id 的 schema/口径版本**（不是时间范围）；字段或口径变化必须 bump。

### 3.2 不变量
- **同一个 dataset_id 的 schema_id 不可隐式变更**；若字段/口径变化必须 bump `vN` 或生成新 dataset_id，并在 UI 里提示“兼容/迁移”。
- `dataset_id` 不包含时间范围（range 属于 manifest/partitions）。

## 4) 存储布局（与 `12` 对齐）

### 4.1 目录结构（建议）
- `datasets/<dataset_id>/`
  - `manifest.json`（可变指针；指向当前 **active** manifest 修订；用于默认预览/新 runs；原子替换）
  - `manifests/`
    - `<manifest_hash>.json`（不可变）
  - `fingerprints/`
    - `<manifest_hash>.json`（strict/派生 fingerprint 的 sidecar；可再生成）
  - `data/`（规范化后的主数据；优先列式/可 mmap）
    - `partitions/<partition_id>.parquet`（或 Arrow/其他列式）
  - `raw/`（可选：下载的原始文件/压缩包/逐日 CSV；受配额控制）
  - `index/`（SQLite 或等价索引；见 [D-003](03-decisions-open-questions.md)）
  - `health/`
    - `<manifest_hash>.health.json`（深度扫描输出，可缓存）
  - `locks/`（可选：跨进程互斥/lease 元信息）

### 4.2 原子性与一致性（硬要求）
- `manifest.json`（指针）与 `index/` 的“可见版本”必须一致：
  - 写入新 manifest（不可变文件）
  - 更新 SQLite（事务）
  - 最后原子替换 `manifest.json`（temp+fsync+rename）
- `run_spec.dataset_manifest_ref` 一旦写入 run bundle，不得被 `manifest.json` 指针变化影响。

#### 4.2.1 `manifest.json` 指针文件最小 schema（建议）
- `schema_version`: number（必填）
- `active_manifest_hash`: string（必填）
- `updated_at_ms?`
- `note?`

指针文件格式（硬要求）：
- 必须是 JSON 文件（不使用 symlink，避免跨平台差异）。
- 文件内容**只**作为指针+少量元信息，不得内嵌完整 manifest。

示例：
```json
{ "schema_version": 1, "active_manifest_hash": "..." }
```

读取/修复语义（建议）：
- `manifest.json` 缺失/损坏/指向不存在的 manifest：`datasets.preview` 必须返回 `active_manifest_ref=null` 并提示“需要 index_build 修复”；不得猜测“latest”。

> UI 若需要浏览历史修订：直接枚举 `manifests/<manifest_hash>.json`（或通过 index 查询）；不得通过修改既有 manifest 文件实现“切换”。

## 5) 规范化数据（Normalize）与最小 schema（一期范围）

> 目标：让引擎与可视化/查询管线消费**统一的列式 schema**，避免每个数据源写一套解析/修补逻辑。

### 5.1 Candle（OHLCV）最小字段集（建议）
- `ts_ms`：int64（bar 起始时间；UTC；normalize 后必须统一为 bar start）
- `open`, `high`, `low`, `close`：float64（或 decimal 编码；一期用 float64 但需披露）
- `volume`：float64（需披露 base/quote 口径）
- `symbol`：string（当 dataset 包含多 symbol 时必填；否则可省略并在 manifest 里固定）
- `source_flags?`：uint32（可选：缺失补齐、异常修复、聚合来源等位标记）

### 5.2 Trade（逐笔）与 L2（占位）
- Trade：`ts_ms`, `price`, `qty`, `side`, `symbol?`, `trade_id?`
- L2：建议以 tiles/heatmap 为主（见 `17/20`），原始事件只做按需分页（见 `11` 的 `trades.list` 占位）。

## 6) Dataset Manifest：字段级契约（建议 v1）

> Manifest 是“可复现引用”的核心；任何会影响回测/对比/渲染语义的点都必须显式落在 manifest 或 execution profile（见 `14`）。

### 6.1 顶层字段（建议）
- `schema_version`: number（与 Dataset Manifest schema 版本绑定，非 Run bundle schema）
- `dataset_id`: string
- `provenance`
  - `asset_class`, `venue`, `market?`
  - `instrument?` / `symbols?`
  - `data_kind`, `resolution`
  - `timezone`: `"UTC"`（硬要求；非 UTC 必须 normalize）
  - `source`: `{ kind: exchange_api|vendor|file_import, name, endpoint?, request_params_redacted? }`
  - `license?`: `{ name?, url?, notes? }`
- `time_semantics`（用于 health/gap 的基准；不同市场可不同）
  - `expected_cadence_ms?`: number（例如 1m candle = 60000；未知则省略）
  - `calendar?`: string（例如 `24x7`、或未来股票市场交易日历 ID）
- `semantics_disclosure`（最小集；与 `14` 对齐）
  - `bar_time_alignment`: `start` | `end`（normalize 后 candle 应为 `start`；否则必须报错或强告警）
  - `volume_semantics`: `base` | `quote` | `unknown`
  - `price_type`: `trade` | `mark` | `mid` | `unknown`
  - `fill_policy_hint?`：用于 UI 提示（不是执行语义本身）
- `schema`
  - `schema_id`: string（例如 `candles.v1`）
  - `columns[]`: `{ name, dtype, nullable, semantics? }`
- `partitions[]`
  - `partition_id`: string（稳定、可排序）
  - `uri`: string（root-relative 或 dataset URI；禁止绝对路径）
  - `time_range`: `{ start_ms, end_ms }`（闭开区间约定需固定）
  - `row_count`: number
  - `stats?`: `{ min_price?, max_price?, volume_sum?, missing_ts_count? ... }`（可扩展）
- `stats`（全局汇总）
  - `time_range`: `{ start_ms, end_ms }`
  - `row_count_total`
  - `size_bytes_total?`
  - `quality_summary`: `{ gaps?: number, overlaps?: number, duplicates?: number, outliers?: number }`
- `fingerprints`
  - `fast`: `{ algo: "jcs_sha256", value }`
- `build`
  - `normalize_config_hash`: string（ETL 配置的稳定 hash）
  - `toolchain?`: `{ app_version?, git_commit?, rustc?, parquet_writer? }`

### 6.2 示例（片段，非最终）
```json
{
  "schema_version": 1,
  "dataset_id": "crypto.binance.spot.btcusdt.candles.1m.v1",
  "provenance": {
    "asset_class": "crypto",
    "venue": "binance",
    "market": "spot",
    "data_kind": "candles",
    "resolution": "1m",
    "timezone": "UTC",
    "source": { "kind": "exchange_api", "name": "binance" }
  },
  "time_semantics": { "expected_cadence_ms": 60000, "calendar": "24x7" },
  "schema": { "schema_id": "candles.v1", "columns": [{ "name": "ts_ms", "dtype": "i64", "nullable": false }] },
  "partitions": [
    { "partition_id": "2024-01", "uri": "data/partitions/2024-01.parquet", "time_range": { "start_ms": 1704067200000, "end_ms": 1706745600000 }, "row_count": 44640 }
  ],
  "fingerprints": { "fast": { "algo": "jcs_sha256", "value": "..." } }
}
```

## 7) Fingerprint（fast/strict）与生成规则

> Fingerprint 的目的：在“用户机器资源有限”的前提下，用可解释的成本换取可复现性；UI 必须展示 level 与风险提示（见 [D-005](03-decisions-open-questions.md)）。

### 7.1 fast fingerprint（默认，用于日常）
- 输入：manifest 的 canonical JSON（JCS），但**必须剔除**以下字段以避免自指/非内容漂移：
  - `fingerprints`（否则会把 fingerprint 自己算进去）
  - `build.toolchain`（运行环境/版本信息不应导致“同内容不同指纹”）
- 算法：`sha256(jcs(manifest_without_fingerprint_and_toolchain))`
- 语义：快速、稳定（对路径修复/根目录变化友好），但无法证明每个分区文件未被篡改。

### 7.2 strict fingerprint（可选，用于审计/分享/benchmark）
- 输入：对所有 `data/partitions/*` 计算内容 hash（每个分区文件 sha256）+ 规范化排序后的 `(partition_id, content_hash, row_count, time_range)` 列表。
- 算法建议：构建有序 hash tree（或直接 hash 拼接后的 bytes；需固定规则）。
- 成本：需要对所有分区文件计算 hash（可增量缓存；中途可取消）。
- 语义：更强的内容证明，适合作为 benchmark/golden fixtures 的依赖（见 D-021/D-022）。
- strict 结果必须写入 sidecar：`datasets/<dataset_id>/fingerprints/<manifest_hash>.json`；不得回写既有 manifest 文件。

### 7.3 指纹与 RunSpec 的绑定规则（硬要求）
- RunSpec 必须写入：
  - `fingerprint.level` + `fingerprint.value`
  - `dataset_manifest_ref.manifest_hash`
- 当用户用 fast 指纹运行后再切换 strict 复核：系统应提示“同 dataset_id 的 strict 指纹可能变化”，并提供“一键生成 strict fingerprint”的 job。

## 8) Preview（不全量扫描）

### 8.1 `datasets.preview` 返回内容（建议）
- `dataset_id`
- `active_manifest_ref`: `{ manifest_hash, fingerprint(level,value) }`
- `time_range`, `row_count_total`, `size_bytes_total?`
- `symbols?`, `resolution`, `data_kind`
- `quality_summary`（来自 manifest；不得触发深度扫描）
- `storage_status`: `{ roots_ok: bool, missing_partitions?: number, last_seen_at_ms? }`
- `hints[]`：例如“缺口较多/建议 deep health scan/建议生成 tiles”

### 8.2 Preview 的“禁止动作”
- 不允许为了 preview 扫描所有分区文件内容。
- 允许做的 IO：读取 SQLite 索引/manifest/少量文件头（例如 Parquet footer 的 row_group 元信息），但必须有严格上限与可取消。
- 资源预算（硬要求，占位阈值可在 D-023 的 benchmark 后回填）：
  - 最多读取 `N` 个 partition footer / 最多 `X MB` 元信息 / 最长 `T ms`；超过预算必须降级返回（字段为 `null` 并给出 `hints[]`）。

## 9) Health Report（质量校验）与分级扫描

### 9.1 扫描级别（建议）
- `health.quick`（默认）：基于 manifest 的 `partitions[].time_range/row_count` + 少量抽样；目标是“发现明显问题”。
- `health.deep`（显式触发，Job）：逐分区扫描必要列（至少 `ts_ms`），输出缺口/重复/乱序等明细与可定位证据。

### 9.2 Health Report（JSON）最小字段集（建议）
- `schema_version`
- `dataset_id`, `manifest_hash`
- `level`: `quick|deep`
- `generated_at_ms`
- `summary`: `{ status: ok|warn|error, issues_total, rows_scanned?, partitions_scanned? }`
- `issues[]`（可分页/截断；必须可导出）
  - `code`（来自 [D-024](03-decisions-open-questions.md) 的错误码注册表命名空间，例如 `DATASET.GAP_DETECTED`）
  - `severity`: `info|warn|error`
  - `message`
  - `evidence`: `{ partition_id?, ts_ms?, range?, sample_rows? }`（脱敏/截断）
  - `hint?`

### 9.3 质量检查项（一期至少覆盖）
- 时间戳单调性：分区内 `ts_ms` 是否严格递增（或允许相等？必须固定）。
- Gap/Overlap：分区之间 `time_range` 是否连续/重叠；以及分区内是否存在缺口（deep）；gap 判定基准来自 `time_semantics.expected_cadence_ms` / `time_semantics.calendar`（见 D-030）。
- Duplicate：同 `ts_ms` 重复（deep；必要时按 symbol 维度）。
- Outlier：价格/成交量显著异常（规则需在报告里给出阈值与口径；默认保守）。
- Schema drift：列缺失/类型变化/nullable 变化（与 `schema_id` 冲突必须报错）。
- Semantics mismatch：例如 `timezone!=UTC`、`bar_time_alignment` 未披露（必须报错或强告警）。

## 10) Download/Import（与 Job 系统绑定）

### 10.1 范围
- 一期：crypto 的 candles（单一 resolution 优先，例如 `1m`），venue 优先 `Bybit/Binance`（可切换顺序，但必须可扩展）。
- 入口：
  - “选择数据源→选择 symbol(s)→选择时间范围→下载”
  - “导入本地文件→检测 schema→normalize”

### 10.2 `dataset_download` job（建议 phases）
- phases：`resolve_plan` → `quota_check` → `download` → `verify` → `commit`
- `quota_check` 必须给出磁盘占用预估（区间）并在配额不足时提前失败（与 `12`/D-020 对齐）。
- 进度单位（建议）：
  - `units_total = bytes_total_estimate`（若可估算）；`units_done = bytes_downloaded`
  - 若不可估算，`units_total=null` 且 `phase` 必填（见 `15/18`）
- 可靠性：
  - 断网/限流：指数退避 + 错误码分类（rate_limit/network/auth）
  - 续传：基于分片文件与校验（ETag/Content-Range 或分区级幂等）
  - 重试：同 `job_id` 新 `attempt`（见 `15`），尽可能复用已完成分片

### 10.3 凭据与许可（硬要求）
- API Key 等凭据禁止明文落盘；使用 OS Keychain/Credential Vault（见 `21`）。
- 必须记录数据来源与许可摘要到 manifest 的 `provenance.license`，并在 UI 中可见（尤其是用于分享/导出时）。

## 11) Normalize / Index Build（与 manifest 生成绑定）

### 11.1 `dataset_normalize` job（建议）
- 输入：raw 文件集合（或下载结果）+ `normalize_config`（含字段映射、去重规则、时间戳口径、异常修补策略）。
- 输出：
  - `data/partitions/*.parquet`
  - `index/`（写入或更新）
  - 生成新的不可变 `manifest`：`manifests/<manifest_hash>.json`（含 `normalize_config_hash` 与 stats）
  - 原子更新 `manifest.json` 指针（见 §4.2）
- 幂等 key（建议）：`raw_fingerprint + normalize_config_hash`（见 `15` 表格）

### 11.2 `dataset_index_build` job（可选拆分）
- 当用户导入了“已规范化数据”（例如外部 parquet）：可单独跑 index/manifest 构建。
- 深度健康扫描也可以复用 index_build 的扫描结果（避免重复 IO）。

## 12) 依赖决策（D-IDs）
- [D-018](03-decisions-open-questions.md)（Dataset manifest）
- [D-005](03-decisions-open-questions.md)（fingerprint）
- [D-007](03-decisions-open-questions.md)（内置下载）
- [D-003](03-decisions-open-questions.md)（索引存储/事务一致性）
- [D-019](03-decisions-open-questions.md)（多根目录/修复）
- [D-020](03-decisions-open-questions.md)（缓存/配额/保留）
- [D-024](03-decisions-open-questions.md)（错误码体系）
- [D-025](03-decisions-open-questions.md)（dataset layout canonical）
- [D-026](03-decisions-open-questions.md)（dataset_id 语法与规范化）
- [D-027](03-decisions-open-questions.md)（manifest 不可变 + sidecar 规则）
- [D-030](03-decisions-open-questions.md)（时间口径与 gap/health 基准）
- [D-032](03-decisions-open-questions.md)（下载幂等/增量语义）

## 13) 验收（草案，可在实现前细化）
- `datasets.preview`：在“仅读取 index+manifest（不全扫）”前提下，返回 time_range/row_count/quality_summary，并能明确提示“是否需要 deep scan”。
- `health.deep`：显式触发 Job；可取消；失败有结构化错误码；成功生成可导出的 `health.json`，且包含 `manifest_hash` 与证据定位字段。
- 下载进度：断网/限流下可恢复；进度在 UI 中单调递增（attempt 内），且 attempt 切换可感知（见 `15`）。
- 复现引用：任意 run 的 `dataset_manifest_ref` 指向的 manifest 文件可被找到（或可修复 root 后找到），且 `fingerprint` 校验可复核（fast/strict）。
