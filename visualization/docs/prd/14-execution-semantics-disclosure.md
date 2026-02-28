# 14. 执行语义与披露规范（Execution Semantics & Disclosure）

> 目标：让“结果”可解释、可对比、可复现。任何 Quick/Full 的差异都必须能归因到：`Execution Profile` 或 `Dataset Ref`（见 `13/16`）。

## 1) 目标
- 统一 Quick/Full 的“语义词汇表”，用 `Execution Profile` 作为可比较对象。
- 强制 UI 披露与对比差异字段，避免无意 lookahead/语义作弊。

## 2) 范围与非目标

### 2.1 范围
- 本文定义 `Execution Profile` 的字段级数据契约（用于：RunSpec 落盘、UI 披露、Runs Compare diff）。
- 本文定义“披露 checklist”与“风险提示规范”（尤其是 lookahead 风险）。

### 2.2 非目标
- 不在本文决定引擎实现细节（撮合器/队列模型的代码实现属于 engine）。
- 不在本文决定具体指标数学定义（只要求 `metrics_def_version` 可追溯）。

## 3) Execution Profile：字段级契约（v1 草案）

> `Execution Profile` 必须是可序列化对象，并写入 RunSpec（见 `13-runspec-artifacts-schema.md`）。

### 3.1 顶层字段
- `profile_version`: number（Execution Profile schema 版本；与 run bundle schema_version 不同）
- `engine_mode`: `quick` | `full`（用于披露；不是执行开关本身）
- `time_semantics`
  - `bar_time_alignment`: `start` | `end`（执行假设；必须与 dataset manifest 的披露一致，否则报错；见 `16`）
  - `clock`: `utc`（一期固定；展示可切换见 `10`）
- `fill_model`
  - `kind`: `next_open` | `close_fill` | `tick_sim`
  - `risk`: `{ lookahead_risk: `none`|`low`|`high`, reason_code?: string }`
- `event_timeline`（信号/下单/成交的事件顺序；用于判定 lookahead 风险）
  - `signal_eval`: `{ point: `bar_open`|`bar_close`|`tick`, bar_offset?: number }`
  - `order_submit`: `{ point: `bar_open`|`bar_close`|`tick`, bar_offset?: number }`
  - `fill`: `{ point: `bar_open`|`bar_close`|`tick`, bar_offset?: number }`

> `bar_offset` 约定（v1）：以“当前 decision bar”为 0；例如 `fill.point=bar_open` 且 `bar_offset=1` 表示“下一根 bar 的 open 成交”。当 `point=tick` 时 `bar_offset` 必须省略。
- `fees`
  - `model`: `fixed_bps` | `schedule`
  - `bps?`: number
  - `maker_taker?`: `{ maker_bps, taker_bps }`
  - `schedule_ref?`: `{ id: string, content_hash: string }`（引用版本化费率表）
- `slippage`
  - `model`: `none` | `fixed_bps` | `impact_model`
  - `bps?`: number
  - `impact?`: `{ k?, model_ref?: { id: string, content_hash: string }, volume_field?: string }`

> `impact_model` 必须明确用量字段与单位：`volume_field` 与 dataset manifest 的 `semantics_disclosure.volume_semantics` 必须一致，否则必须报错或强告警（见 `16` 与 D-024）。
- `latency`
  - `model`: `none` | `fixed_ms` | `queue_model`
  - `ms?`: number
  - `queue_model_ref?`: `{ id: string, content_hash: string }`
- `shorting`
  - `allow_short`: boolean
  - `borrow_cost_apr?`: number
  - `locate_model?`: `always_available` | `constrained`（占位）
- `matching`
  - `kind`: `bar_ohlc` | `tick_matching`
  - `partial_fill`: boolean
  - `pessimism`: `none` | `conservative` | `custom`
- `pricing`（估值/滑点/成交参考价的口径）
  - `trade_price_source`: `last` | `mid` | `mark` | `close` | `open`
  - `mark_to_market_price_source`: `last` | `mid` | `mark` | `close`
  - `index_price_source?`:
    - `{ kind: dataset_column, column: string }` | `{ kind: vendor_ref, ref: { id: string, content_hash: string } }`
  - `mark_price_source?`:
    - `{ kind: dataset_column, column: string }` | `{ kind: vendor_ref, ref: { id: string, content_hash: string } }`
  - `fx_conversion?`: `none` | `spot_rate` | `vendor_rate`（为多币种预留）

> 兼容性硬规则（与 `16` 对齐）：
> - 若 `pricing.trade_price_source`/`mark_to_market_price_source` 指向的数据来源与 dataset manifest 的 `semantics_disclosure.price_type` 不兼容，必须报错或强告警（按 D-024）。
> - 若 `*_price_source.kind=dataset_column`：`column` 必须存在于 `manifest.schema.columns[].name`，否则报错。

兼容映射（v1，最小可实现）：
- dataset `price_type=trade`：允许 `open/close/last`（来自 OHLC/成交价）。
- dataset `price_type=mark`：允许 `mark`（若使用 `mark` 且数据不含 mark，则必须提供 `mark_price_source`）。
- dataset `price_type=mid`：允许 `mid`（若使用 `mid` 且数据不含 mid，则必须提供 vendor_ref）。
- `account_model`
  - `kind`: `cash` | `margin`（一期 crypto spot 可为 cash；衍生品为 margin）
  - `base_currency?`: string（如 `USDT`；用于披露与指标口径）
  - `leverage?`: number
  - `liquidation?`: `{ enabled: boolean, model_ref?: { id: string, content_hash: string } }`（占位）
- `derivatives?`（crypto 衍生品占位：一期可不启用，但必须可扩展）
  - `contract_kind`: `spot` | `perp` | `future`
  - `contract_type?`: `linear` | `inverse` | `quanto`
  - `contract_multiplier?`: number（单位必须在实现中固定，并在 UI 披露）
  - `settlement_currency?`: string
  - `margin?`:
    - `{ mode: cross|isolated, collateral_currency, initial_margin_ratio?, maintenance_margin_ratio?, liquidation_enabled, liquidation_model_ref?: { id: string, content_hash: string } }`
  - `funding?`:
    - `{ enabled: boolean, interval_ms?, payment_time?: period_end|period_start, apply_price?: mark|index, cap_bps?, floor_bps?, rate_source: { kind: dataset_column|vendor_ref|fixed, column?: string, ref?: { id: string, content_hash: string }, bps?: number } }`
- `corporate_actions`（为 A股/美股预留，一期 crypto 可默认 `none`）
  - `dividend`: `none` | `adjusted`
  - `split`: `none` | `adjusted`
- `metrics_def_version`: string（指标定义版本；必须指向一份可执行契约，见 `23`）
  - v1：`crypto.v1`（约束年化/return 采样/无风险利率等；不得由实现端默认值暗含）
- `determinism`
  - `seed?`: number
  - `rng?`: string
  - `parallelism?`: `{ threads, reduce_order: `fixed` }`

### 3.2 禁止项（硬要求）
- 禁止包含任何 secret/token（同 RunSpec 规则；见 `13`）。
- 禁止“默认值暗含”：任何影响结果的默认值必须显式写入 profile（用于 hash 与对比）。
- 禁止自由文本进入 hash：Execution Profile 必须是“稳定、可对比”的结构化字段集合；任何展示用注释应放入 run `notes/tags`（见 `13`）。

### 3.3 materialization（hash 前必须做的标准化）
- 必须先将 profile “物化”（materialize）后再计算 `execution_profile_hash`：
  - 所有默认值必须显式展开为字段值（不得依赖实现端默认值）。
  - 禁止用 “字段缺失 vs null” 表达同一语义（必须在 schema 中固定一种写法）。
  - 任何会影响结果的字段不得取“实现端默认/不确定”（必须是明确 enum/value）。

#### 3.3.1 默认值（v1，物化后必须显式存在）
- `fees`: 默认 `{ model: fixed_bps, bps: 0 }`
- `slippage`: 默认 `{ model: none }`
- `latency`: 默认 `{ model: none }`
- `shorting`: 默认 `{ allow_short: false }`
- `matching`: 默认 `{ kind: bar_ohlc, partial_fill: false, pessimism: none }`
- `pricing` 默认由 `fill_model.kind` 推导（推导结果必须写回物化 profile）：
  - `next_open` → `trade_price_source=open`
  - `close_fill` → `trade_price_source=close`
  - `tick_sim` → `trade_price_source=last`
  - `mark_to_market_price_source`：bar 模式默认 `close`；tick 模式默认 `last`
- `event_timeline` 默认由 `fill_model.kind` 推导（推导结果必须写回物化 profile）：
  - `next_open` → `signal_eval={bar_close,0}, order_submit={bar_close,0}, fill={bar_open,1}`
  - `close_fill` → `signal_eval={bar_close,0}, order_submit={bar_close,0}, fill={bar_close,0}`（lookahead_risk 默认为 `high`）
  - `tick_sim` → `signal_eval={tick}, order_submit={tick}, fill={tick}`
- `determinism.parallelism`：若未显式配置并行，视为 `{ threads: 1, reduce_order: fixed }`（必须写回）

#### 3.3.2 一致性约束（v1，必须校验）
- `engine_mode` 必须与 RunSpec 的 `engine.kind` 一致；实现中建议：`engine_mode` 由 `engine.kind` 派生，不允许用户单独设置（见 `13`）。
- `fill_model.kind` ↔ `matching.kind` ↔ `event_timeline` 必须一致：
  - `tick_sim` ⇒ `matching.kind=tick_matching` 且 `event_timeline.*.point=tick`
  - bar 模式（`next_open/close_fill`）⇒ `matching.kind=bar_ohlc`
- `fill_model.risk.lookahead_risk` 的默认/派生规则：
  - `close_fill` 默认为 `high`；`next_open` 默认为 `none`；`tick_sim` 默认为 `low`（实现可提升为 `none`，但不得降级为更低风险而不改字段）。

#### 3.3.3 与 RunSpec 的 determinism 一致性（避免双真源）
- 若 RunSpec 顶层存在 `determinism`（见 `13`），其值必须与 `execution_profile.determinism` 完全一致；不一致必须报错（D-024）。

### 3.4 `execution_profile_hash`（必须）
- hash 算法与 canonicalization 必须与 `13` 一致（JCS + sha256）。
- UI 必须展示 hash，并用于 Compare 的差异归因与去重。

## 4) 披露 checklist（对 UI 的硬要求）
- Run Launcher：在“开始运行”前展示 profile 摘要，用户可展开查看详情。
- Run Detail：固定区域展示 profile + 版本信息 + 数据 fingerprint。
- Runs Compare：任何对比必须显示 profile diff（字段级），并允许“对齐语义”快捷操作（若可对齐）。

### 4.1 披露内容（最小必显）
- 引擎：`engine_mode` + engine 版本（git_commit/cargo_lock_hash；见 `13`）
- 数据：`dataset_ref.dataset_manifest_ref.manifest_hash` + fingerprint level/value（见 `13/16`）
- 关键语义：fill/fees/slippage/latency/shorting/matching
- 估值口径：pricing（成交参考价、M2M 价源）
- 账户口径：cash/margin、基准币种、杠杆/清算（若启用）
- 指标口径：`metrics_def_version`
- 确定性：seed/parallelism（若影响结果则必须显式）

### 4.2 风险提示（必须统一文案与触发条件）
- `fill_model.kind=close_fill`：必须显示 lookahead 风险提示（默认 high），并要求用户显式确认。
- 任何“非默认语义”必须在 Compare 中高亮（例如 slippage 从 none→fixed_bps）。

> `bar_open/bar_close` 的定义必须结合 dataset 的 `bar_time_alignment`：
> - 当 dataset `bar_time_alignment=start`：`bar_open=ts_ms`，`bar_close=ts_ms+bucket_ms`。
> - 当 dataset `bar_time_alignment=end`：`bar_close=ts_ms`，`bar_open=ts_ms-bucket_ms`（不推荐；normalize 后应为 start，见 `16`）。

## 5) Quick vs Full 的差异呈现
- 明确“差异不是 bug”：当差异来自引擎语义/模型不同，必须归因到 profile 字段。
- 当差异来自数据 fingerprint 不同：必须提示“不是同一数据集/分区”。

## 6) Runs Compare：差异归因规则（最小）
- Compare 必须按以下优先级归因（1→5）：
  1) `dataset_ref.dataset_manifest_ref.manifest_hash` 不同 → 数据修订不同（优先提示）
  2) `dataset_ref` 不同（symbols/timeframe/partitions）→ 数据切片/窗口不同（提示“对齐数据切片”）
  3) `dataset_ref.fingerprint` 不同 → 数据内容不同或引用损坏（提示“重新校验数据集/修复 roots”；见 `16`）
  4) `execution_profile_hash` 不同 → 语义不同（展示字段级 diff）
  5) 上述均相同但结果不同 → 引擎版本/非确定性/bug（提示“需生成 diff 报告”，见 `13`）

## 7) 依赖决策（D-IDs）
- [D-001](03-decisions-open-questions.md)（双引擎）
- [D-008](03-decisions-open-questions.md)（强制披露）
- [D-017](03-decisions-open-questions.md)（Execution Profile）
- [D-016](03-decisions-open-questions.md)（determinism/tolerance）
- [D-024](03-decisions-open-questions.md)（错误码体系）

## 8) 验收（草案）
- Run Launcher/Detail/Compare 三处的披露 checklist 在任何引擎模式下都必须出现。
- Runs Compare 的 diff 必须是字段级（profile 字段完整覆盖），并能把差异归因到 profile 字段或数据 fingerprint。
- 任意 run 的 `execution_profile_hash` 与 `run_spec_hash` 可稳定复算（JCS+sha256），且跨平台一致。
