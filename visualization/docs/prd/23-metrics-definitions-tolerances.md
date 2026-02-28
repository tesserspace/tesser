# 23. 指标定义与容差契约（Metrics Definitions & Tolerances）

> 目的：把 `metrics_def_version` 从“可追溯字段”升级为可执行契约：**指标列表固定、单位固定、年化/采样固定、对比与容差固定**，避免 Quick vs Full / 不同平台 / 不同实现产生“不可解释的差异”。

## 1) 范围与非目标

### 1.1 范围
- 阶段 1（crypto/candles 优先）必须锁定的：核心收益/风险/交易统计指标定义、单位、显示与比较规则。
- 兼容：未来接入 A 股/美股等（交易日历、无风险利率、交易时段）与 tick/L2（只影响输入序列，不改变本契约的指标定义）。

### 1.2 非目标（一期不在此文解决）
- 不在此文定义“撮合/队列模型”的微观执行语义（见 `14`）。
- 不在此文给出“多标的/多腿/期权”的完整 trade decomposition（一期先要求可披露/可降级）。

## 2) 契约总览（硬要求）

- `Execution Profile.metrics_def_version` 必须存在且参与 `execution_profile_hash`（见 `14`）。
- `run.json` 与 `metrics.json` 必须落盘记录 `metrics_def_version`（见 `13`）。
- 任何对比（Runs Compare / Diff）必须以 **同一 `metrics_def_version`** 为前提；不一致时必须显示“口径不同”并禁止自动结论（见 `19`）。

## 3) `metrics_def_version` 命名与版本化

### 3.1 命名
- v1 规范名：`crypto.v1`
- 未来扩展：`equities.cn.v1` / `equities.us.v1`（体现交易日历与年化口径差异）

### 3.2 演进规则（硬要求）
- 新增指标：只能追加字段（旧指标含义不得改动）。
- 口径变更：必须 bump `metrics_def_version`（例如 Sharpe 年化从 252→365 视为不兼容变更）。

## 4) 共同输入与时间口径

### 4.0 时间字段（硬要求）
- 所有时间戳统一使用 `ms since epoch`（UTC）。
- `start_time_ms`：用于本次回测统计的第一个 equity 样本时间戳。
- `end_time_ms`：用于本次回测统计的最后一个 equity 样本时间戳。
- `duration_seconds = (end_time_ms - start_time_ms) / 1000.0`（若 `end_time_ms <= start_time_ms` 则视为 0）。

### 4.1 Equity 系列（定义）
- `equity[t]`：按回测引擎语义定义的 **mark-to-market** 净值（现金 + 持仓按 `mark_price` 估值 - 应计费用/资金费等）。
- 阶段 1：默认以 candle close 作为 `mark_price`，并要求引擎披露（见 `14` 的 `pricing/marking`）。

`crypto.v1` 的指标计算必须基于 **metrics sampling cadence**：
- candles backtest：`equity` 必须在每个输入 bar（每根 candle）的统计点上可得（通常为 bar close）；指标不得基于 UI 下采样序列计算。

### 4.2 时间与分组
- 时间戳：UTC；日频分组以 **UTC day** 为界（`00:00:00Z` 到 `23:59:59Z`）。
- Return 采样：`daily_close_utc`（每日取最后一个 `equity` 样本作为 close；缺失日按 carry-forward 补齐）。

`daily_close_utc`（硬定义）：
- 令 `D` 为从 `start_time_ms` 所在 UTC 日期到 `end_time_ms` 所在 UTC 日期的日期序列（按天递增）。
- 对每个 UTC day `d ∈ D`：
  - 若该日存在 `equity` 样本：`close[d] = last_equity_value_in_that_day`
  - 若该日没有 `equity` 样本：`close[d] = close[d-1]`（carry-forward；若 `d` 为首日且无样本，则该日从序列中剔除）
- `daily_return[d] = close[d] / close[d-1] - 1`（算术收益；第一个有效日无 return）
- 若 `close[d-1] <= 0`：该日 return 记为缺失并从统计中剔除（同时必须在 `metrics.debug` 里记录 `skipped_returns_count`）

### 4.3 年化因子（crypto.v1）
- `days_per_year = 365.0`
- `periods_per_year = 365.0`（当 return 采样为 `daily_close_utc`）
- 无风险利率：`risk_free_rate_annual = 0.0`（后续可在 `Execution Profile` 配置；口径变更需 bump 版本）

### 4.4 数值阈值（epsilon，硬要求）
- `eps_std = 1e-12`：用于判断 `stdev/downside_dev` 是否视为 0
- `eps_dd_pct = 1e-9`：用于判断 `max_drawdown_pct` 是否视为 0（Calmar 分母）

## 5) 指标列表（crypto.v1）

> 约定：字段名后缀含义：
> - `_pct`：以“百分比点”存储（例如 `1.23` 表示 `1.23%`）
> - `_ratio`：无单位比值（例如 Sharpe）
> - `_count`：整数计数

### 5.1 Core（必须提供）

1) `starting_equity`（number）
- 定义：回测起点净值（与 RunSpec/Execution Profile 对齐）

2) `ending_equity`（number）
- 定义：回测终点净值（最后一个 equity 样本）

3) `total_return_pct`（number）
- 定义：`(ending_equity / starting_equity - 1) * 100`
- 约束：`starting_equity<=0` 时必须返回结构化错误（D-024），不得返回 NaN

4) `cagr_pct`（number|null）
- 定义：
  - `years = duration_seconds / (365.0 * 86400.0)`
  - 若 `years<=0`：`null`
  - 否则：`((ending_equity / starting_equity)^(1/years) - 1) * 100`

5) `max_drawdown_pct`（number）
- 定义（基于 **完整 equity 序列** 的逐点 peak-to-trough；不得用 daily closes 替代）：
  - `peak = max_{i<=t} equity[i]`
  - `dd[t] = (peak - equity[t]) / peak`（`peak>0` 才计算）
  - `max_drawdown_pct = max_t dd[t] * 100`

6) `sharpe_ratio`（number|null）
- 输入：`daily_returns[]`（见 §4.2）
- 定义：
  - `r_f_daily = risk_free_rate_annual / periods_per_year`
  - `excess = mean(daily_returns) - r_f_daily`
  - `sharpe = sqrt(periods_per_year) * excess / stdev(daily_returns)`（样本标准差；`n<2` 或 `stdev<=eps_std` → `null`）
  - `stdev()`：样本标准差（分母 `n-1`）

7) `sortino_ratio`（number|null）
- 定义：与 Sharpe 相同，但分母为 downside deviation：
  - `downside = sqrt(mean( min(0, daily_returns - r_f_daily)^2 ))`
  - `sortino = sqrt(periods_per_year) * excess / downside`（downside<=eps_std → `null`）

8) `calmar_ratio`（number|null）
- 定义：`annualized_return_pct / abs(max_drawdown_pct)`（`abs(max_drawdown_pct)<=eps_dd_pct` → `null`）
- 其中 `annualized_return_pct` 与 `cagr_pct` 数值相同（一期只保留 `cagr_pct`，需要 Calmar 时用 `cagr_pct`）

9) `return_samples_count`（number）
- 定义：参与 Sharpe/Sortino 的 `daily_returns` 样本数（剔除缺失后的长度）

10) `avg_daily_return_pct`（number|null）
- 定义：`mean(daily_returns) * 100`（`return_samples_count==0` → `null`）

11) `annualized_volatility_pct`（number|null）
- 定义：`stdev(daily_returns) * sqrt(periods_per_year) * 100`（`return_samples_count<2` → `null`）

### 5.2 Trading（一期必须提供可比最小集；可扩展）

> 一期 trade decomposition 尚未锁死，因此只要求“不会因为定义不同而误导”的最小集；更丰富的交易统计必须通过新版本扩展并可审计。

12) `fills_count`（number）
- 定义：执行层产生的 fills 数（一个订单可能产生多个 fills；具体语义见 `14`）

13) `fees_paid`（number|null）
- 定义：全程累计费用（基准货币；**以对 equity 的现金影响计**；正数=成本，负数=返佣/返利）

14) `realized_pnl`（number|null）
- 定义：全程累计 realized PnL（基准货币；一期强制 `pnl_mode=net_of_fees`）
- 合约安全约束：仅当同时提供 `pnl_attribution_version`（见 §6.1）时允许非空；否则必须为 `null`

15) `win_rate_pct`（number|null，可选）
- 若引擎提供 trade-level 归因：必须同时提供 `trade_model_version`（见 §6），否则应返回 `null` 并给出原因

## 6) 显示与对比规则（UI 合约）

- UI 展示使用四舍五入（默认：`_pct` 保留 2 位；`_ratio` 保留 2 位；货币保留 2–4 位），但对比与导出必须使用未舍入原值。
- Compare 视图必须展示：
  - `metrics_def_version`
  - `days_per_year/periods_per_year`（若未来版本可配置）
  - `return_sampling`（`daily_close_utc`）
  - `pnl_mode`（一期固定 `net_of_fees`）
  - 指标缺失原因（`null` 的原因：样本不足/除零/禁用）

### 6.1 `metrics_context`（建议落盘，避免口径漂移）

`metrics.json` 建议携带（并在 UI 可查看）：
- `metrics_context`：`{ metrics_def_version, return_sampling, periods_per_year, days_per_year, risk_free_rate_annual, pnl_mode, trade_model_version?, pnl_attribution_version? }`

`pnl_mode`（一期枚举；crypto.v1 固定为 `net_of_fees`）：
- `net_of_fees`：PnL 已包含交易费用对现金/equity 的影响（`fees_paid` 与 `realized_pnl` 不得“双计”）
- `gross_of_fees`：PnL 不含费用（仅允许未来版本；crypto.v1 禁止）

`trade_model_version` / `pnl_attribution_version`（可选但一旦提供必须可比较）：
- `trade_model_version`：trade 如何从 fills/orders 归因与聚合（例如 `fills.v1` / `round_trip.v1`）
- `pnl_attribution_version`：realized PnL 的归因模型（例如 `avg_cost.v1` / `fifo.v1`）

`metrics_debug?`（建议；用于诊断 determinism/数据缺口）：
- `{ carried_forward_days_count, missing_days_count, skipped_returns_count, null_reasons?: { [metric_name]: string } }`

## 7) 容差（Determinism tolerances，建议默认）

> 容差用于跨平台/不同实现的“可接受差异”裁决；最终以 [D-016](03-decisions-open-questions.md) spike 出数固化。

### 7.1 指标容差（建议）
- `total_return_pct`：`abs ≤ 1e-4`
- `cagr_pct`：`abs ≤ 1e-4`
- `max_drawdown_pct`：`abs ≤ 1e-4`
- `sharpe_ratio`：`abs ≤ 1e-3`
- `sortino_ratio`：`abs ≤ 1e-3`
- `calmar_ratio`：`abs ≤ 1e-3`
- `win_rate_pct`：`abs ≤ 1e-2`（若提供；trade decomposition 差异更敏感）
- `fees_paid/realized_pnl`：`abs+rel`（默认：`abs ≤ 1e-6` 且 `rel ≤ 1e-6`）

`abs+rel` 判定公式（硬要求）：
- 令 `a`、`b` 为两次计算结果，`Δ = |a-b|`
- 通过当且仅当：`Δ <= abs_tol + rel_tol * max(|a|, |b|)`

计数类指标（`*_count`）默认要求 `exact`（差异必须归因，不适用容差）。

### 7.2 超容差处理（硬要求）
- 必须生成差异报告（最小字段：RunSpec hash、Execution Profile hash、dataset manifest hash、metrics_def_version、引擎版本、指标差值）。
- UI 必须把差异归因候选排序显示（见 `14`/`10`）。

## 8) 与现有实现的对齐要求（一期明确事项）

- `crypto.v1` 要求：
  - `daily_close_utc` + `periods_per_year=365`（而非 `252`）
  - `days_per_year=365.0`（不使用 `365.25`）
  - `pnl_mode=net_of_fees`
- 任何不满足上述约束的实现必须：
  - 使用不同的 `metrics_def_version`（例如 `crypto.legacy.v0`），并禁止与 `crypto.v1` 自动对比结论（见 `19`），或
  - 迁移到 `crypto.v1` 并 bump 引擎版本号以便可追溯

## 9) 依赖决策（D-IDs）
- [D-016](03-decisions-open-questions.md)（determinism/容差）
- [D-022](03-decisions-open-questions.md)（golden fixtures）
- [D-024](03-decisions-open-questions.md)（错误码体系）
