# Visualization (Tauri) 可视化回测平台：构思草案

> 目标：基于现有 Tesser 工作区能力（数据→回测→报告→策略），做一个 **local-first** 的可视化回测平台，既能“快速迭代”（秒级跑完、可交互），也能“高保真”（事件驱动、可复现实盘语义），并把结果沉淀成可对比、可追溯的 artifacts。

## 1. 现状盘点（来自本仓库的“可复用资产”）

### 1.1 两条回测引擎路径（语义不同、互补）

1) **高保真事件驱动引擎：`tesser-backtester`**
- 角色：离线模拟器，把 `Strategy` + `ExecutionEngine` + `Portfolio` + `tesser-paper` 串成一条确定性的回放链路。
- 数据输入：通过 `MarketStream` 回放 candles（以及 tick 模式下的 LOB 事件流）。
- 模式：`BacktestMode::{Candle, Tick}`（tick 模式使用 `UnifiedEventStream` 读取 flight-recorder / JSONL）。
- 当前产出：返回 `PerformanceReport`（Sharpe/Drawdown/Ending equity 等），内部其实已经计算了 `equity_curve` 与 `fills`，但默认不对外暴露完整序列。

2) **轻量确定性“数学内核”：`tesser-backtest-core` + `tesser-backtest-wasm`**
- 角色：浏览器/轻量工具使用的同步纯计算 backtest core（no tokio、no IO、WASM 友好）。
- 语义：策略在 bar close `i` 决策，交易在 next bar open `i+1` 执行（no-lookahead）；target weight rebalancing；fee/slippage。
- 产出：`BacktestResult { equity[], trades[], metrics }`，天然适合可视化（图表 + 交易标记 + 指标面板）。
- `tesser-backtest-wasm`：提供最小 JSON-in/JSON-out ABI，适合跑在 WebWorker 或嵌入式 WebView 中。

结论：可视化平台应把两者都纳入“引擎配置项”：
- **Quick（可交互、秒出图）**：基于 `tesser-backtest-core`（native 或 WASM）。
- **Full（语义更贴近 runtime）**：基于 `tesser-backtester`（Candle/Tick、含 execution/portfolio 细节）。

### 1.2 数据工程与格式（GUI 需要“数据选择器”和“预览器”）
- `tesser-cli data ...`：下载/校验/重采样/normalize 等工作流入口。
- `tesser-data`：Parquet 回放流 `ParquetMarketStream`（ticks/candles/books/depth），以及 `UnifiedEventStream`（tick 模式聚合事件）。
- 现状：CLI 强、可视化弱；缺少“数据集索引/预览/元信息”层，GUI 需要补齐。

### 1.3 策略与扩展能力（GUI 需要“策略选择 + 参数编辑 + 插件管理”）
- `tesser-strategy`：内置策略注册表（`builtin_strategy_names()` + `load_strategy(name, params)`），策略参数来自 TOML `params` 表。
- `tesser-rpc`：`RpcStrategy` 可把策略逻辑外包给外部进程（gRPC），适合 GUI 做“远端策略/脚本策略”扩展。
- `tesser-wasm` + `tesser-execution::wasm`：WASI execution plugins（执行算法插件）可通过 `ExecutionHint::Plugin` 绑定。

## 2. 产品定位与核心原则

### 2.1 目标用户
- 本仓库的策略研究者/开发者：需要比 CLI 更快的“配置→运行→对比→复盘”闭环。
- 偏工程的量化团队：需要可复现、可追溯的 artifacts（数据版本、参数、语义、代码版本）。

### 2.2 核心原则（必须明确写进产品）
1) **可复现优先**：任何一次 run 都要能“凭 artifacts 重放”，至少包含：
   - 引擎类型（Quick/Full）与关键语义参数（fill model、fee/slippage、latency、shorting 等）
   - 数据集 fingerprint（路径 + 元信息 + hash/统计）
   - 策略标识（内置策略名 / RPC endpoint / 代码版本）与 params
   - 仓库版本（git commit）与依赖版本（Cargo.lock 摘要）
2) **语义显式化**：UI 必须把“无意的 lookahead / 作弊”变成困难的事：
   - 默认 next-open fill（与 `tesser-backtest-core` 一致）
   - tick 模式下的撮合/队列模型等，必须显示为配置项并写入 run metadata
3) **local-first**：默认不需要云服务；所有数据、结果、索引都在本机目录中管理。
4) **性能可控**：大数据回放时：
   - 后端流式处理 + 分段落盘
   - 前端只加载 decimated/downsampled 曲线与分页表格

## 3. 体验设计：把“回测”变成一个可管理的 Workspace

### 3.1 关键对象（心智模型）
- **Workspace**：一个“研究工作区”（包含数据集引用、策略配置集、run 记录、对比面板布局）。
- **Dataset**：一个可选择的数据集（通常是一组 canonical Parquet partitions）。
- **Strategy Config**：TOML（与 CLI 一致）或 GUI 表单（生成 TOML）。
- **Run**：一次可复现执行（engine + dataset + strategy + params + execution model）。
- **Artifact**：run 的产物（metrics、equity curve、fills/trades、logs、charts snapshot）。

### 3.2 MVP 页面（第一版要能闭环）
1) **Run Launcher（启动页）**
   - 选择引擎：Quick / Full（Candle）/ Full（Tick）
   - 选择数据：Parquet 目录/文件（支持拖拽 + 近期使用）
   - 选择策略：内置策略下拉（来自 `builtin_strategy_names()`）
   - 参数编辑：TOML editor（最小实现），或表单（后续）
   - 执行模型：fee/slippage/latency/allow_short 等

2) **Run Detail（结果页）**
   - 总览：Performance 指标（与 `PerformanceReport` / `BacktestMetrics` 对齐）
   - 图表：
     - Candle + 交易标记（trades/fills）
     - Equity curve + Drawdown
   - 表格：交易列表（可筛选/排序）、关键事件日志
   - 导出：JSON/CSV（指标 + trades）与“生成 CLI 命令/配置”

3) **Runs（历史与对比）**
   - run 列表（可按策略/数据/时间过滤）
   - 对比视图：多条 equity 叠加 + 指标表对比

## 4. 技术方案：Tauri 作为“本机研究终端”

### 4.1 总体架构
- 前端：Web UI（React/Vite 或 Next.js 均可；建议 Vite 以简化 Tauri 打包）
- 后端：Tauri Rust commands + 后台任务系统
- 计算：
  - Quick：`tesser-backtest-core`（优先 native，必要时 WASM + Worker）
  - Full：`tesser-backtester`（tokio async、流式回放、可扩展到 tick）
- 存储：
  - artifacts 根目录：`~/.tesser/visualization/` 或 workspace 内 `.tesser/`
  - 元数据：SQLite（便于查询 run 列表/对比）或 JSON 索引（MVP 可先 JSON）

### 4.2 “长任务”模型（避免 UI 卡死）
后端提供 Job API：
- `start_backtest(run_spec) -> job_id`
- `cancel_job(job_id)`
- 通过 Tauri events 推送：
  - `job.progress`（当前时间戳、处理 bars/events 数、估计 ETA）
  - `job.log`（结构化日志片段）
  - `job.completed`（artifact 路径 / run_id）

### 4.3 需要补齐/抽象的 Rust API（建议在实现时补）
为 GUI 友好，建议把 backtester 的“中间结果”也作为一等公民输出：
- 新增（建议）`BacktestArtifacts`：
  - `equity_curve: Vec<(ts, equity, ...)>`
  - `fills: Vec<Fill>` 或 `trades: Vec<...>`
  - `signals/orders`（若可获得）
  - `metrics: PerformanceReport`
- 对 `tesser-backtester`：提供一个可选的 “sink/collector” 或返回扩展结果类型。
- 对 `tesser-cli`：GUI 不建议通过 shell 调 CLI（解析输出脆弱），而是直接依赖 crate API。

## 5. 引擎策略：Quick vs Full 的分层与一致性

### 5.1 为什么必须双引擎
- `tesser-backtest-core` 的优势：确定性强、产物天然适合 UI、运行轻、WASM 友好。
- `tesser-backtester` 的优势：复用 runtime 组件（execution/portfolio/risk/connector），能承载 tick/LOB 与更真实的执行语义。

### 5.2 一致性风险（需要在产品里“讲清楚”）
- 两条引擎在“订单生成/撮合/费用模型/仓位语义/多标的”上天然可能不一致。
- UI 必须把引擎类型标出来，并允许用户理解差异：
  - Quick：target-weight rebalancing、单标的（当前 core）
  - Full：订单/撮合/队列模型、可扩展多标的

## 6. 里程碑（从最小闭环到可扩展平台）

### M0：能跑、能看（1–2 周量级）
- Tauri 壳 + 基础 UI（选择数据、选择策略、运行、展示指标 + equity 图）
- 引擎先用 Quick（`tesser-backtest-core` native/WASM 任一）
- artifacts 落盘（run metadata + result JSON）

### M1：接入 Full Candle（2–4 周量级）
- 后端接入 `tesser-backtester`（Candle mode）
- 增加 fills/trades 序列输出（需要扩展 backtester API 或新增 collector）
- Runs 列表 + 对比

### M2：数据集管理与批量实验（4–6 周量级）
- Dataset 索引/预览（时间范围、bar 数、缺口统计）
- Batch 回测（对齐 `tesser-cli backtest batch` 的核心能力）
- 参数扫描（grid / random）+ 结果对比

### M3：Tick / LOB 可视化（长期）
- Tick mode：`UnifiedEventStream` + `MatchingEngine`
- orderbook 深度图、盘口回放、成交与订单生命周期可视化
- execution plugins（WASM）管理与调试面板

## 7. 开放问题（建议在动工前定下来）
1) artifacts 的“标准格式”选什么？
   - JSON（易调试）vs Parquet（大规模更合适）vs 混合（元数据 JSON + 大表 Parquet）
2) Full engine 要不要保证与 CLI 输出严格一致（指标定义/四舍五入/字段命名）？
3) Quick engine 何时扩展到多标的/更多执行模型？还是明确定位为“研究预估器”？
4) 策略编辑体验：
   - MVP 先只支持内置策略 + params TOML
   - 后续再接 `RpcStrategy`（Python/JS 等外部语言）或引入 sandboxed scripting

## 8. 大数据 + 弱机器：如何高性能可视化（必须纳入设计）

现实约束：tick/LOB/长周期 candle 数据可以轻松达到 **GB~TB**；多数用户机器的瓶颈不是算力，而是 **内存、磁盘吞吐、以及前端渲染**。平台要做到“看得动”，关键不是把全量数据塞进 UI，而是把可视化当成 **按需查询 + 多分辨率渲染** 的系统。

### 8.1 核心原则：永远只传“屏幕需要的量”
- 图表的上限由像素宽度决定：例如 1800px 宽的 K 线图，最终最多需要 ~1800 根 bar（再乘 2–4 的冗余用于 antialias/滑动），而不是 5,000,000 根。
- 任何曲线（equity、drawdown、PnL）都必须走 **decimation/downsample**：
  - 后端按目标点数执行 LTTB（Largest-Triangle-Three-Buckets）或 min/max 保形采样；
  - 前端只画下采样后的点，并在用户 zoom-in 时请求更高分辨率。

### 8.2 多分辨率“金字塔”（LOD / Tiles）缓存
为每个 dataset 构建一个 visualization cache（后台任务离线生成，增量更新）：
- **Dataset Index（元信息索引）**
  - 时间范围、bar/tick 数、缺口统计、每分区行数、文件大小、schema 版本等。
  - 让 UI 在不扫描全量 parquet 的前提下完成“选择/预览/校验提示”。
- **Candle LOD（金字塔）**
  - L0：原始粒度（1m/1s/原始 candle）
  - L1/L2/...：按时间桶聚合（例如 5m/15m/1h/4h/1d），每级都按固定 chunk（比如每 10k bars）分片落盘。
  - 聚合字段用 OHLCV 的天然聚合：open=first、high=max、low=min、close=last、volume=sum。
- **Tick/LOB 可视化 LOD（长期）**
  - 不把全量事件直接推给 UI；为 heatmap/深度回放生成 time-bucketed 的稀疏矩阵或图片 tiles（例如 time×price bins），并可按需加载。

效果：首次打开大数据集时，UI 先用粗粒度 LOD 秒级出图；用户放大到某段区间时再切换更细级别 tiles。

### 8.3 查询接口：以“时间窗 + 目标分辨率”为一等参数
后端对前端暴露的查询不应是“给我全部”，而应是：
- `get_candles(symbol, range, target_points)` → 返回不超过 `target_points` 的 OHLCV（自动选择 LOD + 边界补齐）
- `get_series(run_id, metric, range, target_points)` → equity/drawdown 等曲线下采样
- `get_trades(run_id, range, page)` → 交易表分页（前端虚拟列表）
- `get_orderbook_heatmap(symbol, range, resolution)` → 返回 tile/块（可选）

### 8.4 IO 与内存：流式读取、避免全量解压
Rust 后端应遵循：
- **流式扫描 Parquet**：以 RecordBatch 方式顺序读取，避免一次性 materialize 到 Vec。
- **按列读取**：可视化聚合常只需要少数列（timestamp、OHLCV），尽量避免读取无关列。
- **增量聚合**：构建 LOD 时逐批聚合，写出 chunk 后释放内存。
- **背压与取消**：所有长任务都要支持取消（Job 模型），并在 UI 层显式展示“扫描进度/剩余时间估计”。

### 8.5 前端渲染：Canvas/WebGL + 虚拟化
前端要把“渲染性能”当成第一类需求：
- K 线与曲线：优先 Canvas/WebGL（大量 DOM/SVG 会成为瓶颈）。
- 表格：必须用虚拟列表（只渲染可见行）。
- 交互：hover/crosshair 不做全量命中计算；用索引结构（binary search 时间戳）定位当前点。

### 8.6 传输格式：JSON 只用于小数据，向量数据用二进制
MVP 可以用 JSON（开发快），但一旦进入“大数据可视化”阶段建议：
- 后端返回 **Arrow IPC / MessagePack / CBOR** 这类二进制向量格式（减少序列化开销与内存峰值）。
- 前端用 TypedArray 直接喂给渲染层（避免大量对象分配导致 GC 抖动）。

### 8.7 运行结果（run artifacts）也要分层存储
Full backtester 的输出可能很大（fills/events）：
- 默认只保存：metrics + downsampled equity + trades/fills 的索引（或摘要）
- 可选保存全量：用户显式勾选“保存全量事件/用于复盘”，并展示空间占用预估
- 对“复盘需要”的大对象（fills、order lifecycle）用列式存储（Parquet）并分区

