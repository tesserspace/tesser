# 10. 产品原则与验收 SLA（Principles & Acceptance SLAs）

> 这是阶段 1 PRD 的“总约束文档”。任何功能 PRD 都必须引用本文件中的 SLA/非协商原则。

## 0) 范围与读法

- 本文定义：**口径（怎么测）**、**指标（测什么）**、**SLA（目标）**、**验收（怎么验）**。
- 本文不定义：具体页面布局、具体 API 字段（详见 `11/18/19`），也不替代 D-IDs 的决策过程（详见 `03`）。

## 1) 非协商原则（Non-negotiables）

### P1 可复现优先（Reproducibility-first）
**要求文本（可复制到其他 PRD）：**
> 任何一次 run 必须生成可持久化的 `RunSpec` 与 dataset fingerprint；在相同引擎语义与版本下，必须支持 compute replay，并且在定义的 determinism 容差内得到一致结果；否则必须生成差异报告并明确原因归因字段。

### P2 语义显式化（Semantics must be explicit）
> UI 必须强制披露回测语义（Execution Profile）：fill model、fee/slippage、latency、shorting、matching/queue（若适用）、指标定义版本；对比视图必须显示语义差异字段。

### P3 大数据友好（Pixel-bound visualization）
> 前端永远只渲染屏幕需要的点数；任何全量加载必须是显式操作，并提供资源预估（时间/内存/磁盘）。

### P4 local-first + 跨平台
> 默认无需云服务；macOS/Windows/Linux 功能一致（允许 UI 微差），路径/权限差异必须在 PRD 中显式处理。

### P5 失败可诊断（Debuggability）
> 所有失败必须有结构化错误码（可被 UI 显示与过滤），并有最小复现包（RunSpec + 日志片段 + 版本信息）。

### P6 无障碍与可用性（A11y）
> 关键路径（Run Launcher / Run Detail / Runs Compare / Jobs / Datasets）必须支持键盘可达与清晰的焦点状态；颜色传达信息必须提供非颜色冗余（图例/标注）。

### P7 国际化底座（Time/Number）
> 时间戳展示与查询必须明确时区口径（默认 UTC + 可切换显示时区）；数值展示必须明确单位与小数精度规则（尤其是收益率/费率/数量）。

### P8 可观测性（Observability）
> 必须能把一次用户操作/Job/run 贯穿到 logs/metrics/trace（至少具备 correlation id），用于定位性能与正确性问题。

## 2) 术语与测量口径（必须一致）

### 2.1 百分位与统计口径
- `P95`：同一测试场景下的 95 分位延迟（不取平均值做 SLA）。
- 每个指标至少记录：`P50/P95/P99` + 样本量 `N` + 测试环境指纹（OS/CPU/内存/磁盘）。

### 2.1.1 cold/warm 与样本量（硬规则）
- 所有 SLA 至少给出两套口径：
  - `cold`：首次打开/首次加载（缓存为空或被清理）；
  - `warm`：再次打开（缓存命中，tiles/索引可复用）。
- `N` 下限：每个场景至少 `N ≥ 30`；如涉及交互（pan/zoom/hover）建议 `N ≥ 100`。
- 不允许把“首次编译/首次安装依赖”计入 SLA；但必须单独记录为工程指标。

### 2.2 时间与时区
- 数据与查询口径：UTC。
- 展示口径：可切换（默认 UTC；可选“本地时区显示”）。
- 所有导出（CSV/JSON/Parquet）必须包含明确时区或约定（例如 RFC3339 + `Z`）。

### 2.3 单位与精度
- 金额/数量：必须显示基础计价货币（例如 USDT/USD）与小数位策略（截断/四舍五入）。
- 收益率/费率：必须显示单位（%/bps），并明确 bps 定义（1bp=0.01%）。

## 3) 资源基线（“弱机器”定义）
- 基线（可调整）：4 核 CPU、16GB 内存、普通 NVMe/SSD、2K 单屏。
- PRD 中任何性能指标若未注明，默认以该基线为验收环境。

### 3.1 WebView/OS/GPU 环境指纹（必须记录）
- OS：macOS/Windows/Linux 的具体版本范围（例如最低支持版本）。
- WebView：macOS=WKWebView，Windows=WebView2，Linux=WebKitGTK（以 Tauri 选择为准），记录其版本号。
- GPU：至少记录“集显/独显/禁用硬件加速”三档，并要求有降级策略（见 `20`）。
- 电源：插电/省电模式（如可获取）要记录到环境指纹中。

## 4) 基准数据集（用于可重复的性能/正确性验收）

> 目的：避免“每个人测的都不一样”。阶段 1 PRD 必须指定至少一个基准数据集。

### 4.1 Candle 基准
- `CANDLE_1Y_1M_SINGLE`：单 symbol，1 年 1m candles（用于 LOD/查询/图表）。
- `CANDLE_1Y_1M_MULTI`：多 symbol（例如 5–20），1 年 1m candles（用于多标的/并发与索引）。

**交付要求（必须可执行）：**
- 对每个 dataset_id 必须明确：
  - 文件格式（Parquet/Arrow/CSV）与 schema 版本；
  - 获取方式：`in-repo`（小样本）/ `generate`（脚本生成，固定 seed）/ `download`（脚本下载，需凭据/限流处理）；
  - fingerprint 校验步骤（fast/strict）。

> 注：大规模基准数据不要求入库；优先用“可复现脚本生成”满足性能验证（见 [D-021](03-decisions-open-questions.md)）。

### 4.2 Tick/L2 基准（可选，作为扩展性/压力测试）
- `TICK_1D`：单 symbol，1 天 tick（用于 Full/tick 与事件明细分页）。
- `L2_1D`：单 symbol，1 天 L2/LOB（用于 heatmap tiles 的 spike；不要求一期功能完整）。

### 4.3 结果基准（golden fixtures）
- `GOLDEN_RUN_QUICK`：固定 candles + 固定策略参数 + 固定 Execution Profile → 固定 metrics/trades（容差内）。
- `GOLDEN_RUN_FULL_CANDLE`：同上，但 Full/candle（用于 Quick vs Full 差异归因模板）。

## 5) SLA 与资源预算（阶段 1 先用“建议阈值”，出数后固化）

> 说明：以下阈值是“建议初版目标”（用于约束设计与实现），应在完成 D-023 的基准测试后固化为项目默认值。

### 5.1 交互与端到端 SLA（建议阈值）

| 类别 | 指标 | 口径 | v1 目标（建议） | v2 目标（建议） |
| --- | --- | --- | --- | --- |
| 启动 | 冷启动 → Home 可交互 | P95 | `≤ 3.0s` | `≤ 2.0s` |
| 启动 | 热启动 → Home 可交互 | P95 | `≤ 1.5s` | `≤ 1.0s` |
| 首屏 | 选 dataset/run → 首个图层可交互（可 pan/zoom/hover） | P95 | cold `≤ 2.5s` / warm `≤ 0.8s` | cold `≤ 1.5s` / warm `≤ 0.4s` |
| 交互 | pan/zoom 输入 → 新帧绘制完成 | P95 | `≤ 33ms`（≈30 FPS） | `≤ 16ms`（≈60 FPS） |
| hover | 十字光标/tooltip 更新完成 | P95 | `≤ 50ms` | `≤ 20ms` |
| 查询 | Rust 收到 query → ready-to-stream | P95 | cold `≤ 250ms` / warm `≤ 80ms` | cold `≤ 150ms` / warm `≤ 40ms` |
| 传输 | 1e6 点压力测试（显式全量/导出路径）端到端时长 | P95 | cold `≤ 2.0s` / warm `≤ 1.2s` | cold `≤ 1.2s` / warm `≤ 0.6s` |
| 传输 | 1e6 点压力测试峰值 RSS 增量（相对 baseline） | P95(peak) | `≤ 250MB` | `≤ 150MB` |
| 取消 | cancel → 停止 + stream EOF/closed | P95 | `≤ 200ms` | `≤ 100ms` |
| 取消 | cancel 后内存“无积累增长”（重复 30 次压力场景） | bounded | `ΔRSS_P95 ≤ +100MB` | `ΔRSS_P95 ≤ +50MB` |
| Library | runs list（10k runs 过滤/排序） | P95 | `≤ 200ms` | `≤ 100ms` |
| LOD | tiles build（`CANDLE_1Y_1M_SINGLE`） | P95 | `≤ 60s` | `≤ 30s` |
| LOD | tiles build（`CANDLE_1Y_1M_MULTI`，5–20 symbols） | P95 | `≤ 6min` | `≤ 3min` |

### 5.2 规模边界（必须写在产品上，不允许隐含）

> 目标：把“能用到什么程度”写清楚，避免无限承诺。数值可在 benchmark 后调整，但必须有默认值与提示策略。

- Runs library：默认支持 `10k` runs 的列表/筛选/排序在 SLA 内完成（见 `5.1`）。
- Compare 叠加：默认支持 `≤ 8` 条曲线同屏比较（超过则提示“合并/采样/分组”）。
- 并发 streams：默认 `≤ 4` 个活跃 stream（超过则排队或复用；见 `18` 的 stream 生命周期）。
- 单次交互查询返回点数：必须遵守 pixel-bound 合约（见 `17`），且 `points_returned <= target_points*(1+ε)`。
- 全量加载：必须是显式操作（例如“导出/离线分析”），并在 UI 展示时间/内存/磁盘预估与取消入口。

### 5.3 Transport（pull-based streaming）预算（建议阈值）

> 这些预算用于避免“弱机器 OOM/卡死”。最终默认值以 D-023 测得为准，并由 `protocol.get_info()` 暴露（见 `18`）。

- `max_chunk_bytes`：建议默认 `1 MiB`。
- `max_bytes_per_pull`：建议默认 `1 MiB`（Phase 1：每次 pull 至多返回 1 个 chunk）。
- `replay_window_chunks`：建议默认 `128`（允许 UI 在短暂断线/重试时重拉）。
- `stream_idle_timeout_ms`：建议默认 `30_000`（UI 不消费时回收；由 `protocol.get_info()` 暴露）。

### 5.4 指标起止点定义（必须写清楚）

> 目的：让任何人都能按相同口径测出同一组数字。

- 首屏（First plot）
  - start：用户确认打开某 dataset/run（点击或快捷键确认）
  - end：首个非空图层完成绘制且可交互（支持 pan/zoom/hover，不阻塞主线程）
  - 允许：后台继续流式补全更高分辨率 tiles（只要不影响交互 SLA）
- 启动（Launch）
  - cold start：进程启动/窗口创建 → Home 可交互
  - warm start：二次启动/二次打开窗口 → Home 可交互（缓存命中）
- 交互（Pan/Zoom）
  - start：输入事件触发（wheel/drag）
  - end：下一帧绘制完成
  - FPS：在连续交互窗口内以 rAF 统计（定义窗口长度与采样方法）
- Hover
  - start：指针移动事件
  - end：十字光标与 tooltip 内容更新完成
- 查询（Backend query）
  - start：Rust 侧收到查询请求
  - end：Rust 侧完成数据准备（不含 UI 解码/绘制）
- 传输（Transport）
  - baseline：发起压力测试前，进入 idle（无活跃 stream/job）≥2s 的 RSS
  - start：用户确认触发“显式全量/导出”操作（该操作必须可取消）
  - end：UI 收到最后一个 chunk 并完成解码；绘制允许按 pixel-bound/LOD 下采样完成（压力测试不要求“每点一像素绘制”）
  - peak RSS：以 host 进程 RSS（P95 of peak over N runs）作为主口径；同时记录全进程 RSS 作为辅助口径；增量=peak-baseline
- 取消（Cancel）
  - start：用户发起 cancel
  - end：producer task 停止且状态一致（若该 producer 为 job，则 job 状态/索引必须一致）；UI 不再收到新数据（stream 关闭/停止）
- runs list（Library query）
  - start：用户触发过滤/排序（输入确认或点击）
  - end：列表首屏稳定渲染完成（虚拟列表允许后续补绘，但不可阻塞滚动）
- tiles build（LOD build job）
  - start：job 进入 `running`（见 `15`）
  - end：job `completed` 且 tileset 可被 `17` 查询命中
- 取消后内存“无积累增长”
  - 方法：执行压力场景（例如 1e6 点 stream）→ cancel → GC/idle（允许 2s 安静窗口），重复 30 次
  - 目标：第 30 次后的 RSS（P95）相对第 1 次不持续抬升（见 `5.1`）

### 5.5 determinism 容差（必须定义策略）
- 数值容差分两层：
  - `metrics_tol`：指标级容差（例如 total_return、sharpe、max_dd）
  - `series_tol`：序列点级容差（equity curve 等）
- 当超出容差：
  - 必须输出差异报告（归因字段：Execution Profile diff、dataset fingerprint diff、版本 diff）。

**容差策略（占位）：**
- 误差形式：每个指标明确采用 `abs`/`rel`/`abs+rel` 的哪一种。
- 指标分桶：某些指标（如 Sharpe）对微小差异敏感，应单独定义更宽容差或改用同一计算路径。

## 6) 统一验收矩阵（后续 PRD 必须引用）

> 详细项见 `03-decisions-open-questions.md` 的 spike 计划；此处仅规定“必须测什么”。

- 渲染：1e5/5e5/1e6 点多 run 叠加的 FPS/延迟/内存
- LOD：1Y 1m candle 金字塔生成时长/磁盘占用/查询延迟
- IPC：背压、取消、中断恢复（stream 中途取消）
- Jobs：崩溃恢复（kill -9）后任务一致性与可恢复性
- 下载：限流/断网/续传/重试的正确性与错误分类

## 7) 错误码与失败分类（跨 PRD 的硬要求）

> 详细错误码列表在后续 PRD 补齐；此处规定“必须可分类”。

- 数据类：schema mismatch、分区缺失、fingerprint 不一致、健康检查失败（gaps/dups/outliers）
- 网络类：DNS、TLS、超时、限流（429）、认证失败
- 资源类：磁盘配额不足、内存压力、GPU/渲染失败（需降级）
- 语义类：Execution Profile 不可用组合、引擎不支持某字段、版本不兼容导致 compute replay 禁用

## 8) 最小复现包（最小可行，且必须安全）

> 复现包用于“让别人能定位问题”，不是“把所有东西都打包走”。

- 必须包含：RunSpec、dataset_ref + fingerprint、版本信息、结构化错误（若失败）、脱敏日志片段、差异报告（若超容差）。
- 禁止默认包含：任何凭据、任何 secret、任何可直接复用的 access token。
- 如需包含敏感信息：必须显式 opt-in 且加密（见 `21` 的安全验收）。

## 9) 依赖决策（D-IDs）
- [D-014](03-decisions-open-questions.md)（Job）
- [D-015](03-decisions-open-questions.md)（schema 版本化）
- [D-016](03-decisions-open-questions.md)（determinism）
- [D-020](03-decisions-open-questions.md)（quota/保留）
- [D-010](03-decisions-open-questions.md)（渲染栈）
- [D-008](03-decisions-open-questions.md)（强制披露）
- [D-005](03-decisions-open-questions.md)（dataset fingerprint）
- [D-017](03-decisions-open-questions.md)（Execution Profile）
- [D-004](03-decisions-open-questions.md)（大序列传输格式）
- [D-013](03-decisions-open-questions.md)（IPC 流式通道）
- [D-021](03-decisions-open-questions.md)（Benchmark 数据集）
- [D-022](03-decisions-open-questions.md)（Golden fixtures 回归流程）
- [D-023](03-decisions-open-questions.md)（SLA 测量口径）
- [D-024](03-decisions-open-questions.md)（错误码体系）

## 10) 验收（阶段性）
- 在基线机器上完成 `1e6` 点序列端到端展示：满足 SLA（P95/峰值内存/取消正确性）。
- 完成至少 2 个关键 spike 并把阈值写回本文件（见 `03` 的 spike 列表）。
