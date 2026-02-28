# 22. 基准测试与测量协议（Benchmarks & Measurement Protocol）

> 目的：把 [D-023](03-decisions-open-questions.md) 变成可执行的“怎么测”，并为 `10` 的 SLA 数值固化提供统一口径。

## 1) 适用范围

- 覆盖：`10/17/18/20` 中涉及的交互性能、传输背压、取消、内存/磁盘预算与正确性回归。
- 不覆盖：策略/引擎本身的收益优化；也不替代 `03` 中的 spike 结论。

## 2) 基准环境指纹（必须记录）

每次 bench 必须输出 `environment_fingerprint`（JSON；**仅使用 `13` 的最小集字段**，同名同义）：
- 最小集（必须与 `13-runspec-artifacts-schema.md` 保持一致）：
  - `os`, `arch`, `webview_version?`, `gpu_class?`
  - `app_version?`, `protocol_version?`, `build_profile?`
- bench 扩展（必须放入 `bench_extras`，避免与 run.json 的“最小集”口径冲突）：
  - CPU：model / cores / threads
  - Memory：total_gb
  - Disk：root_kind（NVMe/SSD/HDD/External/NAS）/ fs / free_gb
  - Display：width/height、`devicePixelRatio`
  - GPU：renderer / driver（可为空）；`gpu_mode ∈ hw_accel|sw_fallback|disabled`
  - Power：plugged / battery_saver（若可得）
  - Roots：**默认不落绝对路径**；记录介质/挂载类型即可（必要时用 hash/脱敏字段）

`bench_extras`（建议结构；`schema_version=1`）：
- `bench_extras.schema_version`: number（固定 `1`）
- `bench_extras.cpu`: `{ model, cores, threads }`
- `bench_extras.memory`: `{ total_gb }`
- `bench_extras.disk`: `{ root_kind, fs, free_gb }`
- `bench_extras.display`: `{ width_px, height_px, device_pixel_ratio }`
- `bench_extras.gpu`: `{ renderer?, driver?, gpu_mode }`
- `bench_extras.power`: `{ plugged?, battery_saver? }`
- `bench_extras.roots[]`: `{ kind: workspace|datasets|cache|downloads|logs, media_kind: nvme|ssd|hdd|external|nas, path_hash?: string }`
  - `path_hash`：仅用于本机调试对照（例如 `sha256(normalized_path)`）；不得用于分享包/复现包默认输出（见 `21` 的脱敏要求）

> 约束：任何不带 `environment_fingerprint` 的数字都不得写回 `10-principles-slas.md` 作为默认 SLA。

## 3) 基准数据集与 fixtures（必须可复现）

### 3.1 Dataset fixtures（引用 `10`）
- `CANDLE_1Y_1M_SINGLE`
- `CANDLE_1Y_1M_MULTI`

### 3.2 Run fixtures（golden）
- `GOLDEN_RUN_QUICK`
- `GOLDEN_RUN_FULL_CANDLE`

每个 fixture 必须声明：
- 获取方式：`in-repo|generate(seed)|download(script)`
- schema 版本、dataset manifest hash（或生成脚本的版本 hash）
- fast/strict fingerprint 的校验步骤（见 `16`）
- `metrics_def_version`（必须记录并在回归中断言；见 `23`）

## 4) 场景库（Scenarios）

> 场景是“可重复执行的操作脚本”，每个场景都必须定义：输入参数、cold/warm 条件、起止点、采样次数、输出指标。

### S-001 启动（Launch）
- 输入：启动 app 到 Home
- 口径：见 `10-principles-slas.md` 的 Launch 定义
- 指标：cold/warm 的 `P50/P95/P99`、失败率

### S-002 首屏（First Plot Interactive）
- 输入：选定 dataset/run → 首个图层可交互（含 pan/zoom/hover）
- 口径：见 `10-principles-slas.md` 的 First plot 定义
- 指标：cold/warm `P50/P95/P99`；并记录 `17` 定义的 query `meta`：
  - `data_source: tiles|raw_fallback`
  - `cache: hit|partial_hit|miss`
- 记录规则（避免歧义）：
  - 若首屏涉及多个 query：必须记录全部 query 的 `meta`（按时间顺序），并标记 gating 交互完成点的 `critical_query_meta`
  - 若启用 build-on-demand：必须在 `scenario_params` 标注 `build_on_demand_enabled=true|false`（不能仅凭 `meta` 推断）

### S-003 Pan/Zoom 帧时间
- 输入：固定窗口内连续 pan/zoom（脚本化）
- 口径：见 `10-principles-slas.md` 的 Pan/Zoom 定义
- 指标：帧时间 `P50/P95/P99` + FPS 分布；掉帧次数

### S-004 Hover 延迟
- 输入：固定速度移动指针穿越图表区域
- 口径：见 `10-principles-slas.md` 的 Hover 定义
- 指标：hover 更新延迟 `P50/P95/P99`

### S-005 Backend query（ready-to-stream）
- 输入：`17` 中标准 query（range + target_points；含 tiles hit 与 raw_fallback 两条路径）
- 口径：见 `10-principles-slas.md` 的 Backend query 定义
- 指标：cold/warm `P50/P95/P99`；必须按 `17` 的 `meta.data_source/meta.cache` 分桶统计，并将 build-on-demand（若启用）单独分桶（不得污染 tiles hit SLA）
  - 同上：build-on-demand 必须由 `scenario_params.build_on_demand_enabled` 显式标注

### S-006 Transport 压力测试（1e6 points）
- 输入：显式全量/导出路径，产生 1e6 点 Arrow IPC stream（见 `10`）
- 口径：见 `10-principles-slas.md` 的 Transport baseline/start/end/peak RSS
- 指标：端到端时长 `P50/P95/P99`；RSS 增量 `P50/P95/P99`；取消正确性

### S-007 Cancel / Backpressure
- 输入：
  - 传输中 cancel
  - tiles build job 中 cancel（校验 job 状态/索引一致性，见 `15` 与 `10` 的 cancel 口径）
  - UI 停止 pull（模拟背压）持续 T 秒
- 口径：cancel 定义见 `10-principles-slas.md`；背压有效性与 stream 终止信号见 `18-ipc-transport.md`
- 指标：cancel 延迟 `P50/P95/P99`；内存不无界增长；idle timeout 回收

### S-008 Runs Library Query（10k runs）
- 输入：构造/导入 10k runs 的索引库；执行过滤/排序/分页
- 口径：见 `10-principles-slas.md` 的 runs list 定义
- 指标：查询与首屏渲染 `P50/P95/P99`；SQLite/索引命中情况；UI 虚拟列表滚动是否阻塞

### S-009 Tiles Build Job（single/multi）
- 输入：对 `CANDLE_1Y_1M_SINGLE` 与 `CANDLE_1Y_1M_MULTI` 触发 tiles build job
- 口径：见 `10-principles-slas.md` 的 tiles build 定义；job 状态机见 `15-jobs-lifecycle.md`
- 指标：完成时长 `P50/P95/P99`；产物磁盘占用；后续 query 的命中率（hit/partial/miss）

### S-010 Cancel Leak Check（30 次循环）
- 输入：重复执行压力场景（例如 S-006）→ cancel → idle（允许 2s 安静窗口）共 30 次
- 口径：见 `10-principles-slas.md` 的“取消后内存无积累增长”
- 指标：`ΔRSS_P95` bounded；以及 streams/job 是否存在残留句柄（打开文件数/活跃任务数）

### S-011 Compare Overlay（≤8 curves）
- 输入：在 Compare 视图叠加 ≤8 条曲线并做 pan/zoom/hover
- 口径：交互口径见 `10-principles-slas.md`；曲线数上限见 `10-principles-slas.md`
- 指标：交互延迟与 FPS 是否仍达标；UI 掉帧/卡顿次数

### S-012 Table Virtualization（1e6 rows）
- 输入：载入/生成 1e6 行表格（trades/events/preview），进行滚动/过滤/跳转
- 口径：表格交互必须不阻塞滚动（`20-visualization-performance.md`）
- 指标：滚动帧时间分布；内存峰值；首屏与过滤延迟

## 5) 测量埋点与数据采集（实现约束）

> 一期允许“日志 + JSON 汇总”实现；但必须保证口径一致、可复盘。

- UI 侧：用 `performance.mark/measure`（或等价方案）记录用户动作起点与首屏/交互完成点；记录渲染循环帧时间。
- Host 侧：记录 query 接收/准备完成、stream 生命周期（open/pull/chunk/eof/closed/cancel/idle_timeout）。
- 关联键：全链路必须贯穿 `correlation_id`（见 `18`）。

## 6) 输出格式（必须机器可读）

输出规则（硬要求）：
- **每个 scenario 执行输出 1 份** `bench_report.json`（目录由执行器决定；建议 `bench/<ts>/<scenario_id>/bench_report.json`）。
- 若一次执行包含多个 scenarios：必须额外输出 `bench_suite.json` 索引文件，列出本次包含的 `bench_report.json` 列表与整体 failures（可推导失败率）。

`bench_report.json`（`schema_version=1`，最小契约）：
- `schema_version`: number（固定 `1`）
- `environment_fingerprint`: object（见 §2）
- `bench_extras?`: object（见 §2）
- `scenario_id`: string
- `scenario_params`: object（必须包含 `cold_warm_bucket`；若相关则包含 `build_on_demand_enabled`）
  - `cold_warm_bucket ∈ cold|process_warm|disk_warm`
- `samples[]`: array
  - `samples[].correlation_id`: string（必须）
  - `samples[].ok`: bool
  - `samples[].elapsed_ms?`: number（ok=true 时必填）
  - `samples[].error_code?`: string（ok=false 时必填）
  - `samples[].query_meta[]?`: array（当场景包含 query 时：记录 `17` 的完整 `meta`）
  - `samples[].critical_query_meta?`: object（当存在多个 query：标记关键路径）
  - `samples[].build_on_demand_triggered?`: bool（当 `build_on_demand_enabled=true` 时必须记录“本次是否实际触发 build”）
- `summary`: object
  - `N`: number
  - `failures`: number
  - `metrics`: map（例如 `{ elapsed_ms: {p50,p95,p99} }`；允许多指标）
- `summary_by_bucket?`: array（建议；用于分桶统计）
  - `summary_by_bucket[].bucket`：`{ cold_warm_bucket, data_source?, cache?, build_on_demand_enabled?, build_on_demand_triggered? }`
  - `summary_by_bucket[].summary`：`{ N, failures, metrics }`
- `artifacts?`: object（可选：flamegraph/trace/screenshot 等）

`bench_suite.json`（`schema_version=1`，最小契约；当一次执行包含多个 scenarios 时必须输出）：
- `schema_version`: number（固定 `1`）
- `started_at_ms?`, `finished_at_ms?`
- `environment_fingerprint`: object
- `reports[]`: `{ scenario_id, path }`（`path` 为相对 suite 文件的相对路径）
- `summary`: `{ N, failures }`

> 建议：同时输出一份 `bench_summary.md` 方便人读，但最终验收以 JSON 为准。

## 7) 执行规则（避免“测不准”）

- 每个场景：`N ≥ 30`；交互类建议 `N ≥ 100`（见 `10`）。
- cold/warm 必须分离：
  - cold：切换到空 workspace 或清理 app 管理的 roots（见 `12-storage-portability.md`）；建议重启进程与 WebView；不得把“首次安装/首次编译”计入
  - warm（process-warm）：同一进程内重复执行并命中内存+磁盘缓存
  - warm（disk-warm）：允许重启进程，但保留磁盘缓存/索引（必须在 `scenario_params.cold_warm_bucket` 标注）
- 运行顺序随机化：避免热身效应污染单一场景
- 失败样本必须保留错误码与 `correlation_id`，不得静默丢弃

## 8) 用这些数字回填 `10`（Act）

当满足：
- 覆盖 `S-001..S-012`
- 每个场景 N 达标 + 有 `environment_fingerprint`
- 关键平台（macOS/Windows/Linux）至少各一台基线机出数

则：
- 把测得的默认阈值写回 `10`（替换“建议阈值”中的数值或加注释“以数据为准”）
- 把相关 D-ID 状态推进（例如 D-023 从 Proposed → Decided）

## 9) 依赖决策（D-IDs）
- [D-023](03-decisions-open-questions.md)（SLA 测量口径）
- [D-021](03-decisions-open-questions.md)（Benchmark 数据集）
- [D-004](03-decisions-open-questions.md)（传输格式）
- [D-010](03-decisions-open-questions.md)（渲染栈）
- [D-006](03-decisions-open-questions.md)（LOD/tiles 落盘）
