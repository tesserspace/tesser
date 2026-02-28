# 19. 信息架构与关键用户旅程（UX / IA / Flows）

## 1) 核心观点
> “Runs library” 是产品核心：用户不是在跑一次 backtest，而是在经营一个可复现的实验库。

## 2) 产品形态与导航（IA）

### 2.1 顶层导航
- Home（最近项目/最近 runs/正在运行 jobs）
- Workspaces（项目与 runs library）
- Datasets（数据集管理：导入/下载/预览/health/tiles）
- Runs（列表/筛选/对比/导出）
- Jobs（后台任务：队列、并发、进度、日志）
- Settings（roots/quota/凭据/性能/协议/隐私）

### 2.2 全局 UI 组件（跨页面）
- Global search（runs/datasets/策略名/tags）
- Toast + Error drawer（结构化错误码 + correlation_id + hint；见 D-024/`11`）
- Progress center（统一展示 job 与 stream 的取消入口；stream 的“重试”仅指 replay window 内的 pull 重拉：当 stream 仍为 active（未进入 closing/closed）且回退的 `next_seq` 仍在 `replay_window_chunks` 内时允许重新 `streams.pull`；若超出 window，UI 不得发起该次 pull（否则会收到 `streams.error(code=STREAM.SEQ_TOO_OLD, terminal=true)` → `streams.closed(reason_code=error)` 并关闭连接），改为重发 query 获取新的 `stream_ref`；见 `15`/`18`）
- “语义披露条”（Execution Profile 摘要 + 差异高亮；见 `14`）

## 3) 关键对象与状态机（用户心智模型）

### 3.1 Dataset（数据集）
- **Active 修订**：`datasets/<dataset_id>/manifest.json` 指向的 `active_manifest_hash`（见 `16`）。
- 历史修订：`manifests/<manifest_hash>.json`（只读）。
- 健康报告：`health/<manifest_hash>.health.json`（可再生成）。
- Tiles：按 tileset key 缓存（至少包含 `dataset_id, manifest_hash, lod_profile, tile_schema_version`；见 `17`）。

### 3.2 Run（实验结果）
- 输入：RunSpec（含 `dataset_manifest_ref.manifest_hash` + `dataset_ref.fingerprint`；见 `13`/`16`）。
- 输出：Artifacts（render replay 必可用；compute replay 视语义与数据而定；见 `10`/`13`）。

### 3.3 Job（长任务）
- 统一状态机：queued/running/completed/failed/canceled（见 `15`）。
- UI 去重：events 至少一次投递，按 `(job_id, attempt, seq)` 去重（见 `18`）。

### 3.4 Stream（大序列数据流）
- Phase 1 默认采用 pull-based：UI 从 `stream_ref` 拉取 bytes：`streams.pull({stream_id,next_seq,max_bytes,correlation_id,request_id})`（见 `18`）；UI “停止拉取”即背压。
- 任意 stream 必须可取消：`streams.cancel`（见 `18`）。
- stream 生命周期终止以 `streams.closed(...)` 为准：
  - `reason_code=eof`：视为正常完成
  - 注意：`streams.eof` 不是终止信号；必须等 `streams.closed(reason_code=eof)`
  - `streams.error(terminal=true)` 后同理：UI 不再 pull，等待紧随其后的 `streams.closed(reason_code=error, error_code=<code>)`；若仍需数据则重发 query 获取新的 `stream_ref`（不做 byte-level resume）
  - `reason_code=error|idle_timeout`：若仍需数据则重发 query 获取新的 `stream_ref`（不做 byte-level resume）
  - 连接异常关闭且**未收到** `streams.closed`：UI 视为 `reason_code=error` + `error_code=STREAM.TRANSPORT_CLOSED`，提示用户后允许重试（重发 query）

## 4) 关键旅程（闭环流程与验收点）

### 4.1 旅程 A：下载数据 → 预览 → health → tiles（Dataset 侧闭环）

#### A0 入口：创建/选择 dataset
- 用户选择：数据源（venue）、market、symbol(s)、resolution（一期只实现一种，但 UI 预留扩展）。
- 系统生成 `dataset_id`（按 D-026 grammar；UI 显示可编辑别名，但 dataset_id 必须 canonical）。

#### A1 下载（Job：dataset_download）
- UI 提交 `jobs.start(job_type=dataset_download, inputs...)`。
- Jobs 页与 Dataset 页都能看到：
  - phase：resolve_plan/quota_check/download/verify/commit（见 `16`）
  - 进度：`units_done/units_total`（单位=bytes；或 indeterminate + phase；见 `15/18`）
  - 操作：cancel/retry（见 `15`）
- 失败时 UI 必须展示：`code + message + hint + correlation_id`（见 D-024/`11`）。

#### A2 Normalize + Index（Job：dataset_normalize / dataset_index_build）
- 若下载产出 raw：必须进入 normalize（生成 `data/partitions/*.parquet` + manifests 修订；见 `16`）。
- 若用户导入已规范化 parquet：允许直接 index_build（生成 manifests 修订）。
- 完成后 Dataset 页显示：
  - `active_manifest_ref`（manifest_hash + fingerprint）
  - `time_range/row_count/quality_summary`

#### A3 Preview（不全扫）
- Dataset 页点击“Preview”：
  - 调用 `datasets.preview` 获取元信息（必须在 IO 预算内返回；见 `16`）
  - 图表数据通过 `candles.query` 获取，必须是 pixel-bound（见 `17`）
  - 通过 `stream_ref` 拉流（见 `18`）

#### A4 Health（分级扫描）
- Quick health：默认展示（基于 manifest/index；不全扫）。
- Deep health：显式触发（Job），完成后生成 `health/<manifest_hash>.health.json` 并可导出（见 `16`）。

#### A5 Tiles（LOD 金字塔）
- 若 tiles 缺失且用户进行 zoom/pan：
  - 默认允许 raw_fallback（meta 标记），但必须与 tiles_build 使用同一聚合实现/参数（同 `lod_profile`；见 `17`），并给出“一键生成 tiles”提示。
- 用户点击“Build tiles”：触发 `tiles_build` Job；完成后可显著降低后续交互延迟。

**验收点（A）**
- 下载/normalize/index/tiles 全程可取消、可重试、进度可解释（`15/18`）。
- Preview 永远不触发全量扫描；全量/深度扫描必须显式（`10/16`）。

### 4.2 旅程 B：选策略 + 参数 → 运行 → 查看 → 标注 → 对比（Run 侧闭环）

#### B0 入口：从 Workspace 创建 run
- 用户选择：
  - dataset（必须绑定 `manifest_hash`；UI 明示 active 修订）
  - strategy（一期内置名 + params）
  - execution profile（默认 profile + 可编辑；披露 checklist）
- UI 在“Run”按钮旁必须展示“语义摘要”（关键字段；见 `14`），并提供“将差异写入 notes/tags”的入口。

#### B1 运行（Job：backtest_run）
- UI 提交 `jobs.start(job_type=backtest_run, idempotency_key=run_spec_hash, inputs...)`（见 `15/13`）。
- Run 创建后立即出现一个 run stub（状态 queued/running），并实时展示 job progress/log（可取消）。

#### B2 结果页（Run Detail）
- 固定区域：
  - 语义披露面板（Execution Profile 完整对象 + hash + 版本）
  - 数据引用（dataset_manifest_ref.manifest_hash + fingerprint level/value）
  - Artifacts 清单（artifact_manifest；见 `13`）
- 图表区：
  - equity/drawdown 等通过 `series.query`（pixel-bound）获取并渲染（见 `17/20`）
  - trades 仅分页（`trades.list`），默认展示摘要

#### B3 标注与收藏
- 支持 tags/notes（写入 run bundle 的 metadata；见 `13`）。
- 支持 pin（避免被 LRU 清理；见 `12`/D-020）。

#### B4 对比（Runs Compare）
- 用户选择两个（或多）runs：
  - UI 必须高亮语义差异字段（Execution Profile diff；见 `14`）
  - 若 `metrics_def_version` 不一致：必须显示“指标口径不同”，并禁止自动结论（仍可分别展示各自指标；见 `23`）
  - 指标表与 equity overlay 必须使用同一 `target_points` 预算与同一 LOD profile（见 `17/20`）
- 一期允许只实现“2-run 对比 + 指标表 + equity overlay”，但必须保留扩展（多 run、更多维度）。

**验收点（B）**
- 任意 run 都能 render replay（不重新计算）并可导出（`13`）。
- 对比视图必须把“语义差异”作为第一解释路径，而不是仅显示数值差异（`10/14`）。

### 4.3 旅程 C：导出 bundle → 另一台机器 replay（分享与复盘）

#### C0 导出（Job：export_bundle）
- 用户在 Run Detail 或 Compare 页触发导出：
  - 选择：仅 render replay / 包含 compute replay 所需（若许可）
  - UI 显示：预计体积、是否包含数据切片、脱敏声明（见 `13/21`）
- 导出进度通过 Job 展示；完成后给出文件位置与“导入”入口。

#### C1 导入与 replay
- 另一台机器导入 bundle：
  - 导入前必须 preview：bundle_kind、包含 runs 数量、预计落盘体积、脱敏声明、是否加密/是否包含敏感信息（如有）、compute replay 可用性（reason_code/hint）
  - 必须能打开（render replay）并看见语义披露 + 数据引用
  - compute replay 若不可用，必须有 reason_code + hint（见 `13`）

**验收点（C）**
- render replay 跨平台可用（macOS/Windows/Linux）。
- bundle 不包含明文凭据；日志脱敏（见 `21`）。

## 4) 语义披露在 UX 中的位置
- 运行前披露、结果页固定披露、对比时差异披露（引用 `14`）

## 5) 依赖决策（D-IDs）
- [D-008](03-decisions-open-questions.md)（披露规范）
- [D-019](03-decisions-open-questions.md)（roots/可移植）
- [D-020](03-decisions-open-questions.md)（配额/清理）
- [D-014](03-decisions-open-questions.md)（Job 语义）
- [D-013](03-decisions-open-questions.md)（IPC/streaming）
- [D-024](03-decisions-open-questions.md)（错误码体系）

## 6) 无障碍（占位）
- 关键页面支持键盘可达、焦点可见、对比度达标；图表提供非颜色信息冗余。

## 7) 验收（占位）
- 三条关键旅程均可在无外部服务前提下闭环完成，并能导出可复现 artifacts（render replay）。
