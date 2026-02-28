# 00. 背景与范围（Context & Scope）

## 1) 背景

Tesser 是一个模块化、事件驱动的量化交易框架（Rust workspace）。当前已有完整的 **数据工程 → 回测 → 策略 → 执行** 链路，但主要以 CLI 为入口。我们计划基于 **Tauri** 构建一个跨平台桌面应用：在不牺牲可复现与语义严谨的前提下，把“回测实验”的门槛降低，并把结果沉淀为可管理的 artifacts。

前端技术栈固定为：
- React + Vite
- TailwindCSS + shadcn/ui
- 高性能渲染优先（Canvas/WebGL + 虚拟列表）

## 2) 产品定位

这是一个 **量化研究工具**（research terminal），桌面端只是为了：
- local-first（无需云端服务）
- 更好的 IO/计算资源利用（直接调用 Rust 能力）
- 更顺滑的可视化与交互（不受浏览器沙箱/内存限制掣肘）

## 3) 目标（Goals）

### G1：可复现的实验闭环
用户可以在 GUI 中完成：
- 选择/管理数据集（含下载进度与校验提示）
- 选择策略与参数
- 运行回测（支持取消/进度）
- 查看结果（指标、曲线、交易/订单等明细）
- 对比多个 runs，导出 artifacts，并能用 artifacts 重放

### G2：高性能可视化（面向“大数据 + 弱机器”）
在数据量极大（GB~TB）且用户机器资源有限时：
- 首屏可在粗粒度 LOD 下快速出图
- 缩放/拖动/hover 不触发全量加载
- 后端按需查询与下采样，前端只渲染屏幕所需点数

### G3：保持扩展性（频率/资产/引擎）
短期可以只实现一种频率/一种输入形态，但必须在设计上保留：
- Candle → Tick/LOB 的扩展路径
- Crypto → A 股/美股/期货等多资产的扩展路径
- Quick（轻量）与 Full（高保真）引擎并存

## 4) 非目标（Non-goals）

以下不在第一轮 PRD 范围内（写清楚以免需求膨胀）：
- 实盘交易的全量操作台（下单、风控、账户管理等“交易终端”形态）
- 云端协作/多人共享/在线运行（可作为长期方向，但不是近期目标）
- 对任意第三方格式数据“全能导入器”（先围绕 Tesser canonical parquet 与既有 ETL）
- 一次性支持所有策略开发语言（先围绕内置策略 + 参数化；脚本化属于后续扩展）

## 5) 范围拆解（Scope Breakdown）

### 5.1 对象模型（心智模型）
- Workspace：用户的研究工作区（数据引用、策略配置集、runs 列表、对比布局）
- Dataset：可选择的数据集（通常为 Parquet partitions 的集合）
- Strategy Config：策略名 + 参数（与 CLI TOML 对齐）
- Run：一次可复现执行（引擎 + 数据 + 策略 + 参数 + 执行语义）
- Artifact：run 的落盘产物（元数据、指标、曲线、交易/订单明细、日志、缓存）

### 5.2 引擎层（必须在 UI 中“显式化语义”）
Tesser 仓库现有两类引擎能力（PRD 需决定如何对外暴露）：
- Quick：确定性、纯计算、易可视化（例如 `tesser-backtest-core` 输出 trades/equity/metrics）
- Full：复用 runtime 组件，语义更接近实盘（例如 `tesser-backtester` 的 candle/tick 回放与撮合）

关键要求：
- UI 必须清楚标注引擎类型与执行语义（no-lookahead、fill model、fee/slippage、latency 等）
- 同一策略在不同引擎的结果可能不同：必须允许对比，并说明差异来源

### 5.3 数据工程（GUI 需要覆盖的最小闭环）
- 数据下载/归一化/校验（能显示进度、失败原因、重试）
- 数据预览（时间范围、bar 数、缺口统计、schema 版本）
- 数据 LOD/tiles 缓存生成与管理（后台任务、可取消）

## 6) 质量指标（初版就要定义的“硬指标”）

> 这些指标在 `03-decisions-open-questions.md` 中会进一步收敛为可测试的验收项。

- 可复现：任意 run 都能导出 `RunSpec` + fingerprint，使其可重放并得到一致结果（在同一引擎/同一版本语义下）
- 性能：图表渲染点数与 UI 像素绑定（target_points）；任何“全量加载”都必须是显式操作并可预估资源
- 稳定性：长任务可取消；任务失败有可诊断错误（错误码 + 简明提示 + 详情日志）
- 跨平台：macOS/Windows/Linux 都能运行（Tauri + Rust），路径/权限/文件对话框差异需在设计上覆盖

### 6.1 资源预算（定义“弱机器”以便可测试）
- 目标基线（可调整）：4 核 CPU、16GB 内存、普通 NVMe/SSD、单显示器 2K 分辨率。
- 在该基线上：
  - 大窗口浏览（年级别 candle LOD）应保持交互流畅（pan/zoom/hover 不显著卡顿）。
  - 任何可能导致内存爆炸的操作（加载全量 tick/LOB、保存全量事件）必须显式提醒并给出空间/时间预估。

### 6.2 可视化交互 SLA（先定义类别，再在后续 PRD 落到数值）
- 首屏：选择 dataset/run 后，能在粗粒度 LOD 下“快速出图”（无需等待全量索引/全量回测结果加载）。
- 交互：缩放/拖动/十字光标与 tooltip 不触发全量数据拉取；按需请求更高分辨率 tiles。

## 7) 关键约束与假设（Assumptions）

- 资产范围：当前只做 crypto，但对象模型与数据 schema 不应把“crypto”写死。
- 时间窗口：用户可能查看从分钟到年的窗口；必须用 LOD/tiles 解决“跨度越大数据越多”的矛盾。
- 单机资源：默认假设用户机器可能只有有限内存/磁盘吞吐；应以流式 IO 与二进制传输为主。
- 数据来源：优先复用现有 Tesser ETL/Parquet schema；后续再扩展导入器。

## 8) 可复现的定义（“重放”到底是什么意思）

- 重放（Replay）分两类：
  1) **结果重放（Render replay）**：不重新跑引擎，仅基于已落盘的 artifacts（曲线/交易/日志）进行复盘与可视化。
  2) **引擎重放（Compute replay）**：用保存的 `RunSpec` + dataset fingerprint，在同一引擎语义与同一版本下重新运行，得到一致结果。
- 需要被指纹化（fingerprint）的至少包括：
  - 引擎类型与关键语义参数（fill model、fee/slippage、latency、shorting、撮合/队列模型等）
  - 数据集 fingerprint（路径 + 元信息 + schema 版本 + hash/统计摘要）
  - 策略标识与参数（内置策略名 / RPC endpoint / plugin 名称等）以及 params
  - 版本信息（git commit、Cargo.lock 摘要、可选：OS/CPU 信息用于排查差异）

## 8) 术语（术语定义只写一次）

- LOD / Tiles：多分辨率分片缓存，用于大窗口快速可视化与按需细化
- Fingerprint：数据集与配置的可复现指纹（路径 + 元信息 + hash/统计 + 版本）
- RunSpec：描述一次 run 所需的全部输入（引擎/语义/数据/策略/参数/版本）
- Artifact：RunSpec 执行后的落盘产物集合

## 9) 依赖与集成边界（Dependencies）

- Rust：复用 workspace crates 作为核心能力来源（不通过 shell 调 CLI 解析输出）。
- UI：React/Vite/Tailwind/shadcn；大规模绘制走 Canvas/WebGL，表格走虚拟列表。
- 存储：本地文件系统 +（待定）SQLite/JSON 索引；需要跨平台路径与权限策略。

## 10) 与现有仓库能力的对齐（仅列出“必须复用”的入口）

- 数据：`tesser-data`（Parquet 回放、UnifiedEventStream、ETL/下载工具），`tesser-cli data ...`（可作为功能参照）
- 策略：`tesser-strategy`（内置策略注册表与参数化装配）
- 回测：
  - `tesser-backtest-core`（纯计算、天然适合可视化）
  - `tesser-backtester`（事件驱动、可扩展 tick/LOB）
- 扩展（后续）：`tesser-rpc`（远端策略），`tesser-wasm`（执行插件）
