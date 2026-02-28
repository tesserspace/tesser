# PRD Docs（可视化回测平台 / Visualization）

本目录用于沉淀“需求收敛 → PRD 定稿 → 任务拆分”的文档集合。

## 目录

### 阶段 0：需求收敛包（先评审，再进入 PRD）
- `00-context-scope.md`：背景、范围、原则、非目标、约束、术语
- `01-personas-jtbd.md`：用户画像、JTBD、成功标准、失败模式
- `02-competitors-matrix.md`：竞品矩阵与启示（量化平台为主）
- `03-decisions-open-questions.md`：关键决策日志与未决问题（写 PRD 前的闸门）

### 阶段 1：完整 PRD（阶段 0 通过后补齐）
- `10-principles-slas.md`：产品原则与验收 SLA（跨文档约束）
- `11-architecture-boundaries.md`：系统架构与模块边界（Rust↔Tauri↔前端）
- `12-storage-portability.md`：Workspace/存储布局与可移植性（多根目录/外置盘/备份）
- `13-runspec-artifacts-schema.md`：RunSpec/Artifacts 数据模型、版本化与迁移
- `14-execution-semantics-disclosure.md`：执行语义与披露规范（Quick vs Full、Execution Profile）
- `15-jobs-lifecycle.md`：Job 体系（持久化、进度、取消/重试/恢复、并发限制）
- `16-datasets-manifest-health.md`：Dataset 管理（manifest、fingerprint、预览、健康报告、下载/normalize）
- `17-series-query-lod-tiles.md`：序列查询 API 与 LOD/Tiles 合约（Candle 优先，可扩展 Tick/LOB）
- `18-ipc-transport.md`：IPC 与数据传输（控制面 + 大序列二进制流、背压与取消）
- `19-ux-ia-flows.md`：信息架构与关键用户旅程（runs library 为核心）
- `20-visualization-performance.md`：可视化组件与性能预算（Canvas/WebGL、虚拟化、交互指标）
- `21-security-extensions.md`：安全、凭据、扩展模型（RPC/WASM、威胁模型、脱敏）
- `22-benchmarks-measurement-protocol.md`：基准测试与测量协议（SLA 口径固化、bench_report schema）
- `23-metrics-definitions-tolerances.md`：指标定义与容差契约（metrics_def_version、年化/采样/对比规则）
- `24-bundle-export-import.md`：Bundle 导出/导入规范（zip 容器、manifest 校验、脱敏与可复盘）

## 评审节奏（PDCA）
- Plan：先把“要解决的问题/边界/关键语义”写清楚
- Do：写出可执行的需求与约束（含验收指标）
- Check：用 `codex exec` 审查每篇文档（缺漏/歧义/风险）
- Act：根据审查意见修订，并把新问题写入 `03-decisions-open-questions.md`
