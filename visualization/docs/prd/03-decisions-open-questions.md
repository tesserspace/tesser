# 03. 关键决策与未决问题（Decisions & Open Questions）

> 目标：在进入“完整 PRD（阶段 1）”前，把最容易推倒重写的点集中管理：哪些必须先决策？哪些先做 spike 验证？哪些可以延后？

## 0) 用法与规则

- **所有“会影响架构/数据格式/可复现”的问题必须在这里登记**，并标注状态（Open / Proposed / Decided / Revisit）。
- 每条 Decision 都应包含：选项、推荐、理由、风险、验证方式（spike/benchmark/用户访谈）。
- 任何 PRD 章节出现 “TBD/以后再说” 的地方，都要回填到本文件，避免散落。

## 1) Decisions Log（决策日志）

> 状态说明：Open=未开始；Proposed=已有推荐但未验证；Decided=已确认；Revisit=已做但需要复盘。

| ID | 优先级 | 决策点 | 选项（简） | 推荐（当前） | 状态 | 风险 | 验证方式（Check） |
| --- | --- | --- | --- | --- | --- | --- | --- |
| D-001 | P0 | 引擎对外形态（Quick vs Full） | 仅 Quick / 仅 Full / 双引擎 | 双引擎（产品层显式差异） | Proposed | 结果不一致引发困惑 | Quick+Full 同策略同数据对照；差异必须归因到“语义字段”，并能提示“如何对齐” |
| D-002 | P0 | Full 引擎的“最小审计包”与可选明细 | 仅 metrics / metrics+curve / 全量事件 | 默认摘要（审计包）+ 可选全量 | Proposed | 资源爆炸与复盘不足两难 | 采样：1D tick、1Y candle 的 artifacts 体积、加载时间、分页体验 |
| D-003 | P0 | Run/Artifacts 索引存储 | 纯文件夹+JSON / SQLite / SQLite+文件 | SQLite（索引）+ 文件（大对象） | Proposed | 迁移复杂；跨平台路径差异；并发写入 | spike：1 万 runs 的查询/筛选；并发写入与崩溃恢复；迁移与备份方案 |
| D-004 | P1 | 大序列传输格式（Rust↔前端） | JSON / MessagePack/CBOR / Arrow IPC | 控制面 JSON；大序列二进制（优先 Arrow IPC） | Proposed | 前端解码与渲染链路复杂 | benchmark：1e6 点序列传输+解码+渲染耗时/内存；中途取消与背压 |
| D-005 | P0 | Dataset fingerprint | 文件 hash 全量 / 元信息+抽样 hash / “schema+统计+路径” | 分级 fingerprint：fast/strict | Proposed | 指纹不稳导致复现失败 | 设计 fast/strict 两级；对比误报率与耗时；跨盘移动/重命名稳定性 |
| D-006 | P1 | LOD/tiles 的落盘格式 | JSON tiles / Parquet tiles / Arrow IPC tiles | Parquet（列式）+ 小索引（可调整） | Open | 生成成本与读取复杂 | spike：生成 1Y 1m LOD 金字塔；评估磁盘/生成时长/范围查询延迟 |
| D-007 | P1 | 数据下载是否内置 | 仅消费现有 parquet / 内置下载+normalize | 内置（复用 tesser-data/cli 语义） | Proposed | 限流、失败恢复、凭据安全、数据许可 | 先支持 Bybit/Binance；断网/限流/续传；凭据存储（Keychain 等）与错误分类 |
| D-008 | P0 | “语义显式化”的 UI 规范 | 简单提示 / 强制披露面板 / 逐项解释 | 强制披露（引擎/成交/费用/延迟） | Proposed | 新手觉得啰嗦；老手才需要 | 定义披露 checklist；评审者能从 1 屏复原关键假设；对比时强制显示差异字段 |
| D-009 | P2 | 参数扫描/优化（Optimizer） | 不做 / 第三方脚本 / 内置 Job | 内置 Job（可取消、预算） | Open | 容易变成无底洞 | 最小优化器：grid/random + 预算 + top-k；每个候选都产出 RunSpec |
| D-010 | P1 | 图表渲染栈与交互模型 | SVG/DOM / Canvas / WebGL | Canvas 优先，必要时 WebGL | Proposed | 实现成本；跨平台 GPU 差异 | spike：1e5–1e6 点；多 run 叠加；hover/crosshair 延迟与 FPS；内存峰值 |
| D-011 | P2 | tick/LOB 的可视化形态 | 全量事件回放 / heatmap tiles / 混合 | heatmap tiles + 按需事件 | Proposed | 预生成成本；解释难；数据规模 | spike：1 天 L2 生成 heatmap tiles；测加载与交互；定义分辨率/预算 |
| D-012a | P0 | RunSpec 中的“策略标识” | 内置名 / RPC / plugin / 脚本 | 先内置名 + params；预留扩展字段 | Proposed | 后续扩展导致破坏兼容 | RunSpec schema 预留：strategy.kind/name/params/source；两次 schema bump 仍可读取 |
| D-012b | P2 | 策略扩展机制 | RPC / WASM / 脚本 | RPC 作为下一步（先只读） | Proposed | 安全与调试复杂 | 先做只读 RPC（只接收 market data 输出 signals）；定义错误码与超时策略 |
| D-013 | P0 | Tauri IPC + 大数据流式通道 | 仅 invoke / invoke+events / loopback WS / file | 控制 invoke + `stream_ref` + pull-based 二进制流（Phase 1：暂定 per-stream `loopback_ws` + binary framing；建议以 `streams.closed` 作为终止信号，对齐 `18`） | Proposed | UI 卡死/无背压/本机进程嗅探 | 验证：背压（pull）、取消、终止语义一致性、loopback 安全（token/127.0.0.1）、1e6 点端到端不卡顿（见 `18`/`22`） |
| D-014 | P0 | Job 系统语义（run/下载/LOD） | 内存队列 / 持久化队列 | 持久化队列（重启可恢复） | Open | 崩溃后丢任务/重复执行 | 崩溃恢复；取消语义一致；进度准确性；并发限制 |
| D-015 | P0 | RunSpec/Artifacts schema 版本化与迁移 | 无版本/semver+迁移 | 显式 schema 版本 + 兼容读取 | Open | 升级后历史 runs 不可用 | 两次 schema bump 后仍可加载旧 runs；无法 compute replay 时有明确提示 |
| D-016 | P0 | 可复现（Determinism）契约 | bitwise / tolerance / “语义一致” | tolerance（数值）+ 语义严格一致 | Open | 跨平台差异引发信任问题 | 同机重跑一致；跨 OS 在容差内一致；差异报告模板 |
| D-017 | P0 | Execution Profile（默认语义束） | 分散配置 / 统一 Profile | 统一 Profile（Quick/Full 共享） | Open | 配置散落导致漏披露 | UI 披露 checklist 与 diff 必须基于 Profile 字段 |
| D-018 | P0 | Dataset manifest（显式数据集描述） | 路径隐式 / manifest 文件 | manifest（schema、分区、stats、provenance） | Open | 预览需全量扫描；迁移困难 | dataset 预览不全扫；fingerprint 稳定；外置盘迁移可修复 |
| D-019 | P1 | 存储位置与可移植性策略 | 仅默认目录 / 可选多根目录 | 多根目录（workspace/dataset/cache） | Open | 权限/拔盘/路径变化 | 拔盘/重命名/迁移的修复 UX；跨平台路径规范化 |
| D-020 | P1 | 磁盘配额与保留策略 | 不限 / 手动 / 配额+LRU | 配额+LRU + Pin | Open | 缓存无限增长拖垮机器 | 压测：优化器+LOD 生成下空间有界；Pin 不被清理；空间预估准确 |
| D-021 | P1 | Benchmark 数据集来源与分发 | in-repo 小样本 / 脚本生成 / 脚本下载 | 小样本入库 + 可复现脚本生成（优先） | Open | 无法复测/无法分发/许可证风险 | 制定 dataset_id 规范；提供生成/下载脚本；校验 fingerprint |
| D-022 | P0 | Golden fixtures 与回归流程 | 无流程 / 手动比对 / 自动 diff+review | 自动 diff+review（含容差） | Open | 指标漂移不可控 | 定义对比面（metrics/trades/series）与更新流程；diff 报告 schema |
| D-023 | P0 | SLA 测量口径与环境指纹标准 | 各测各的 / 统一口径 | 统一口径（cold/warm、N 下限、计时边界）+ `bench_report.json` schema | Proposed | 指标不可对比/不可复现 | 对齐 `22` 的场景库与 `bench_report.json` schema；跨 OS/WebView/GPU 指纹记录；输出可对比的 P50/P95/P99 |
| D-024 | P0 | 错误码体系与注册表 | 字符串报错 / 分层错误码 | 分层错误码 + UI 映射 | Open | 失败不可诊断/无法自动化 | 设计 error code 命名空间；与 UI 展示/过滤映射；脱敏规则测试 |
| D-025 | P0 | Dataset 存储布局 canonical | 继续沿用旧路径 / 切换新布局 / 兼容双布局 | 单一 canonical（manifest 指针 + manifest revisions + sidecars） | Proposed | 实现分叉、迁移代价高、缓存键不稳 | 统一 `12/15/16` 文档；实现前做目录迁移/兼容策略评审 |
| D-026 | P0 | `dataset_id` 语法与规范化 | 自由字符串 / 固定 grammar / 分层命名+转义 | 固定 grammar + canonicalize（小写/转义/版本段） | Proposed | 后期重构缓存键与路径；跨市场不兼容 | 给出 ABNF/正则与示例；用真实 symbol（含特殊字符）做映射测试 |
| D-027 | P0 | Manifest 不可变边界与 sidecar 规则 | 允许回写 manifest / 新 manifest 修订 / sidecar 派生物 | manifest 不可变；派生物写 sidecar | Proposed | 复现引用被破坏；hash 自相矛盾 | 以 strict fingerprint/health 为例走通“后置生成不改 manifest”的流程 |
| D-028 | P1 | Partitioning 与 `partition_id` 规范 | 按日 / 按月 / 自适应 | 先按月（1m candles）+ 可配置 | Open | 影响查询/并行/下载增量/磁盘 | 对比：不同分区粒度下 LOD 生成与 range query 延迟/体积 |
| D-029 | P1 | Fingerprint 算法固化 | 自由实现 / 规范化伪代码 | 规范化（排序/编码/剔除字段） | Open | 不同版本/平台指纹不一致 | 写出可实现伪代码；做跨 OS/版本一致性测试 |
| D-030 | P1 | 时间口径与 health 基准 | 仅连续时间 / cadence+日历 | crypto=24x7 cadence；股票=日历 | Open | gap/duplicate/outlier 定义混乱 | 定义 `time_range` 闭开、`ts_ms` 单调性、gap 判定基准并验证样例 |
| D-031 | P1 | 数值类型与精度策略 | float64 / fixed-point / decimal | 一期 float64 + 披露；预留迁移 | Open | 指标漂移与对比误差不可解释 | 用基准数据测误差；定义披露字段与迁移方案（schema bump） |
| D-032 | P1 | 下载幂等与增量语义 | 覆盖写 / 追加分区 / 版本化修订 | 追加分区 + 新 manifest 修订 | Open | 断点续传/重复下载导致数据腐化 | 定义 idempotency_key、commit 规则与“修复/覆盖”的显式操作 |
| D-033 | P1 | Bundle 导出/导入容器与校验 | tar.zst / zip / 自定义 | `.tesserbundle`=zip + `manifest.json` + sha256 校验 + allowlist + zip-slip/zip-bomb 防护；可选加密（若包含敏感信息则必须加密） | Proposed | 导入漏洞/数据腐化/分享不可复盘 | 以 `24/21/19` 的规范实现一次导出+导入回放；覆盖 hash mismatch/path traversal/quota exceeded |
| D-034 | P1 | 指标口径版本（`metrics_def_version`） | 不锁 / 仅字符串 / 可执行契约 | `crypto.v1`（daily_close_utc + 365 年化 + 容差） | Proposed | Quick/Full/跨平台对比不可信 | 以 `23` 的指标契约与 `22` golden fixtures 做回归；禁止跨版本自动结论（见 `19`） |

## 2) Open Questions（未决问题清单）

### 2.1 数据与规模
- 我们要定义的“最大可用规模”是什么？
  - Candle：1m×1y×多 symbol 的典型上限？
  - Tick：1w/1d 的典型上限？
  - L2：1d 的典型上限（time×price bins 分辨率）？
- 是否需要支持“外置存储”（NAS/移动硬盘）作为 dataset 与 cache 位置？跨平台权限与路径策略怎么做？

### 2.2 指标与语义一致性
- Quick 与 Full 的指标（Sharpe、DD、交易统计）要不要严格同名同定义？
- “成交语义”允许哪些模式：
  - next-open（默认）
  - close-fill（研究者常要，但有 lookahead 风险）
  - tick 模式：撮合/队列模型的默认与可选项

### 2.3 artifacts 与可复现
- artifacts 的“默认保留策略”：
  - 保留多久/保留多少个/按项目隔离？
  - 清理时如何避免破坏复现（删 cache vs 删 run 原始产物）？
- RunSpec 的 schema 版本如何管理（升级/迁移/兼容）？

### 2.4 UX 与“降低门槛”
- 我们要不要提供：
  - 模板策略与示例数据（开箱即跑）
  - “一键复现”分享包（压缩 + metadata + 最小数据切片）？
- 默认界面是“研究者模式”还是“新手模式”？是否需要 onboarding tour？

## 3) Spike / Benchmark 计划（Do → Check → Act 的落地点）

> 这些 spike 的产物应回填到对应 Decision 的“验证方式”里，并把结论更新为 Decided。

1) **序列传输与渲染基准（D-004, D-010）**
- 输入：1e5/5e5/1e6 点的 equity 曲线 + 多 run 叠加
- 输出：端到端耗时（Rust 生成→传输→前端解码→绘制），峰值内存，缩放/hover 延迟

2) **LOD 金字塔生成基准（D-006）**
- 输入：1 年 1m candle（单 symbol 与多 symbol）
- 输出：生成时长、磁盘占用、查询 latency（range+target_points）

3) **下载任务可靠性（D-007）**
- 输入：Bybit/Binance 下载 1m candles（带限流/断网模拟）
- 输出：重试/续传语义、进度准确性、失败分类（错误码）

4) **Quick vs Full 一致性对照（D-001, D-002）**
- 输入：同一策略/同一数据/同一费用假设
- 输出：差异报告模板（指标差、交易差、原因定位）

## 4) 下一步（进入阶段 1 PRD 的闸门）

当以下条件满足时，开始写阶段 1 的详细 PRD（8–12 篇）：
- **语义与披露锁定**：D-001 + D-008 + D-017 至少达到 Proposed（并有 checklist 与对比 diff 规范）。
- **可复现骨架锁定**：D-015 + D-016 至少达到 Proposed（RunSpec/Artifacts schema 版本化 + determinism 契约）。
- **任务系统锁定**：D-014 至少达到 Proposed（Job 生命周期、取消/重试/恢复语义）。
- **至少 1 个性能 spike 出数**：完成“序列传输与渲染基准”或“LOD 金字塔生成基准”，形成默认阈值与降级策略。

> 说明：D-003/D-004/D-006/D-020 很关键，但允许以 “Proposed + fallback + spike 计划” 的方式边写 PRD 边推进；不强制全部 Decided 才能开始写阶段 1。
