# 02. 竞品矩阵（Competitors Matrix）

> 目的：用外部参照系校准我们“必须做什么/不必做什么/怎么做才合理”，并把关键取舍（性能、可复现、语义严谨、门槛）提前暴露。

> 说明：本矩阵以 **量化研究/回测平台** 为主，同时选取少量“桌面端工具”作为“降门槛/体验设计”参照。资产侧以 crypto 为优先，但关注其是否具备扩展到多资产的能力。

## 1) 维度（我们关心什么）

- 数据闭环：下载/归一化/校验/增量更新/缺口提示
- 可视化性能：LOD/tiles、下采样、虚拟化、渲染栈（Canvas/WebGL）、大窗口体验
- 回测语义：no-lookahead、成交/撮合、fee/slippage、延迟模型、tick/LOB 支持
- 可复现与审计：run artifacts、版本锁定、结果缓存策略、导出/分享
- 扩展性：策略语言/插件、远端执行、参数扫描/优化
- 跨平台与部署：local-first、跨平台、对用户环境要求

## 2) 竞品快照（选取理由）

> 注：本节的“可借鉴点”以**产品形态与工作流设计**为主；具体功能是否存在、边界如何，以官方文档为准（见文末 References）。

### 2.1 回测/研究引擎与框架（Engine / Framework）

#### A) QuantConnect LEAN
- 开源事件驱动引擎（research/backtest/live），具备多资产扩展思路。
- 可借鉴：**引擎版本锁定**（commit/tag/image digest）作为可复现基石；引擎与数据/策略解耦。

#### B) NautilusTrader
- 强调高保真事件驱动仿真（执行/延迟/场所等语义维度丰富）。
- 可借鉴：Full 引擎的“语义可配置项”要体系化呈现（并在 UI 里解释差异与成本）。

#### C) Freqtrade + FreqUI
- crypto 优先，提供数据下载与回测工作流，并有 UI（webserver/FreqUI）呈现与对比回测结果。
- 可借鉴：**结果包结构**（配置/策略快照 + 报告 + 可视化数据）、以及对“数据变化→缓存不可靠”的明确告知。

#### D) Jesse
- crypto 策略研究的“开发-回测-复盘”闭环参照（偏研究者工作流）。
- 可借鉴：研究者的迭代回路、run 组织方式、结果展示默认值。

#### E) VectorBT
- 向量化/批量分析与参数扫描参照。
- 可借鉴：Quick 引擎的核心价值：**高吞吐探索**（参数空间、区间切片、多组合对比），与 Full 引擎互补。

#### F) Backtrader / Zipline
- 经典研究型 backtester（生态/抽象成熟）。
- 可借鉴：研究者熟悉的输出形态（analyzer/report）、与 Pandas/表格导出习惯。

#### G) Hummingbot（及其 Dashboard/可视化组件）
- crypto 交易框架，具备策略/回测/可视化的综合产品形态参照。
- 可借鉴：将 backtest/optimize 设计成明确的任务接口（Job 生命周期）与结果拆分（metrics vs series vs logs）。

### 2.2 终端/可视化体验参照（Terminal / UX Reference）

#### H) TradingView Strategy Tester（Web）
- 门槛极低，分享与可视化体验强；支持导出 Strategy Tester 数据（CSV）。
- 可借鉴：**导出粒度**、对比展示形态、默认交互体验。

#### I) NinjaTrader Strategy Analyzer（桌面端参照）
- 桌面端 run library、backtest logs、以及“内存优化 vs 明细保留”的显式取舍参照。
- 可借鉴：实验库（runs/logs）体验、Pin/Notes、以及资源开关（例如是否包含交易明细）。

#### J) MetaTrader 5 Strategy Tester（桌面端 + 优化器参照）
- 桌面端策略测试与参数优化体验参照。
- 可借鉴：optimizer 的任务化（并行/预算/取消）、结果对比与回溯。

#### K) Bookmap / TradingLite（订单簿/热力图参照）
- L2/订单流可视化与高密度渲染交互参照。
- 可借鉴：heatmap 的 LOD/tiles、十字光标/缩放手感、以及“大窗口快速定位→小窗口细看”的交互范式。

### 2.3 数据供应与数据 QA（非直接竞品，但定义“数据闭环上限”）

#### L) Coin Metrics / Kaiko（数据版本化与质量参照）
- 强项：数据产品的版本化、质量指标、缺口与异常解释。
- 可借鉴：Dataset 健康报告（gap/dup/outlier）、数据 provenance、可追溯与可审计的默认姿势。

## 3) 矩阵（定性对比）

> 记号：✓=成熟支持，△=部分/依赖插件或需要额外工程，—=不主打或不支持。

### 3.1 引擎/框架矩阵（Engine / Framework）

| 产品 | local-first | crypto 优先 | 多资产扩展 | 数据闭环（下载/校验） | tick | L2/LOB | artifacts/可复现包 | 参数扫描/优化 | UI/终端 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| LEAN | ✓ | △ | ✓ | △ | △ | △ | △ | △ | △ |
| NautilusTrader | ✓ | △ | ✓ | △ | ✓ | △ | △ | △ | △ |
| Freqtrade+FreqUI | ✓ | ✓ | △ | ✓ | — | — | ✓ | △ | ✓ |
| Jesse | ✓ | ✓ | △ | △ | — | — | △ | △ | △ |
| VectorBT | ✓ | △ | △ | — | — | — | △ | ✓ | △ |
| Backtrader/Zipline | ✓ | △ | △ | — | △ | — | △ | △ | △ |
| Hummingbot | △ | ✓ | △ | △ | △ | △ | △ | △ | ✓ |

### 3.2 终端/体验矩阵（Terminal / UX）

| 产品 | local-first | run library / 历史对比 | 导出/互操作 | 高频/大窗口可视化 | L2/订单流 | 参数优化体验 |
| --- | --- | --- | --- | --- | --- | --- |
| TradingView | — | △ | ✓ | ✓ | — | — |
| NinjaTrader | ✓ | ✓ | △ | ✓ | △ | △ |
| MetaTrader 5 | ✓ | ✓ | △ | △ | — | ✓ |
| Bookmap / TradingLite | △ | △ | △ | ✓ | ✓ | — |

## 4) 对我们 PRD 的启示（结论先行）

1) **实验库（runs/logs）是核心体验**
- 桌面端与 crypto-first 工具普遍强调“保存历史 backtest 并对比”，并支持 notes/pin/restore。
- 我们应把 runs 管理当 P0：可搜索、可标注、可对比、可恢复参数（生成可复现 RunSpec）。

2) **可复现不是一句口号，而是“结果包结构 + 数据一致性策略”**
- 一些 crypto-first 工具会在产物中包含策略/config 快照，并明确数据变化会影响缓存复用与复现。
- 我们需要：RunSpec + 数据 fingerprint + 版本信息 + 缓存策略（何时复用，何时强制重算）。

3) **高性能可视化依赖“按需 + 多分辨率”**
- 桌面端工具常提供“是否保留明细”的显式选项：明细越多越吃资源，需要分层存储/按需加载。
- 我们的 PRD 必须把 LOD/tiles、下采样、虚拟化、二进制传输写成系统性方案（而不是优化项）。

4) **双引擎（Quick vs Full）是合理架构，不是妥协**
- 向量化/批量分析工具代表高吞吐探索；事件驱动引擎代表高保真仿真。
- 我们需要在产品层面把两类引擎的差异讲清楚，并允许用户对比与选择。

5) **参数扫描/优化必须 Job 化（可取消/可预算/可复现）**
- 桌面端优化器体验说明：参数扫描不应是一次性脚本，而是“有预算、有并行、有可中断与可追溯产物”的任务系统。

6) **数据健康报告是 P0（尤其是 crypto）**
- 数据供应与 QA 产品把缺口/异常/版本化当核心卖点；回测平台至少要做到：gap/dup/outlier 报告 + fingerprint + schema 版本提示。

## 5) 待补充（下一轮迭代矩阵）
- 更细的“数据规模能力”对比（单机 1 年 1m candle、1 周 tick、1 天 L2 等的可用性）
- 更细的“指标定义与一致性”对比（Sharpe/Drawdown/成交价/手续费定义）
- 竞品对“结果导出”的格式与接口（JSON/CSV/Parquet/Arrow）

## References（2026-02-27 访问；用于事实核查）

- TradingView Strategy Tester export: https://www.tradingview.com/support/solutions/43000582044-how-to-export-strategy-tester-data/
- QuantConnect / LEAN: https://www.lean.io/ , https://github.com/QuantConnect/Lean
- NautilusTrader: https://nautilustrader.io/ , https://github.com/nautechsystems/nautilus_trader
- Freqtrade: https://www.freqtrade.io/ , https://github.com/freqtrade/freqtrade
- Jesse: https://jesse.trade/
- vectorbt: https://vectorbt.dev/
- Backtrader: https://www.backtrader.com/
- Zipline: https://zipline.ml4trading.io/
- Hummingbot: https://hummingbot.org/ , https://github.com/hummingbot/hummingbot
- NinjaTrader Strategy Analyzer docs (示例入口)：https://support.ninjatrader.com/s/article/Strategies-Tab-Using-the-Strategy-Analyzer?language=en_US
- MetaTrader 5 Strategy Tester: https://www.metatrader5.com/en/terminal/help/testing
- Bookmap: https://bookmap.com/
- TradingLite: https://tradinglite.com/
- Coin Metrics: https://coinmetrics.io/
- Kaiko: https://www.kaiko.com/
