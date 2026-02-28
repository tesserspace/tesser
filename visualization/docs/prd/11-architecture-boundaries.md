# 11. 系统架构与模块边界（Architecture & Boundaries）

## 1) 目标
- 定义 Rust 侧“服务层”的边界，避免 UI 直接耦合具体 crate 细节。
- 明确 Job、存储、查询、可视化数据管线的责任划分与失败模式。

## 2) 逻辑组件

### 2.1 UI（React/Vite）
- 负责：页面状态机、视图渲染、用户交互、展示语义披露、发起 commands。
- 不负责：扫描全量数据、实现回测语义、持久化索引/事务性更新。

### 2.2 Tauri Host（Rust）
- 负责：命令 API、事件推送、权限与路径抽象、Job 调度、存储管理。
- 作为“应用后端”，对 UI 提供稳定 API（见 `18-ipc-transport.md`）。

### 2.3 Core Services（Rust 子模块/crate 组合）
- `job_runner`：持久化队列、并发限制、取消/恢复
- `storage`：roots/workspace、quota/LRU/pin、原子写与校验
- `dataset`：manifest、fingerprint、预览、健康报告、LOD 构建
- `engine_quick` / `engine_full`：对接 Tesser backtest 能力，输出 artifacts
- `query`：按时间窗 + target_points 提供 series/tiles

## 3) 数据流（关键路径）
- Dataset 导入/下载 → manifest/index → LOD 构建（后台 Job）
- Run 启动 → engine 计算 → artifacts 落盘 → runs index 更新
- UI 打开 Run Detail → query(series/tiles) → renderer 绘制（pixel-bound）

## 4) 错误与可诊断性
- 所有 service 返回结构化错误：`code`、`message`、`details`、`hint`。
- 日志需脱敏（见 `21-security-extensions.md`）。

## 5) API Surface（占位，需与 `18` 对齐）

### Commands（invoke）
- `protocol.get_info`
- `jobs.start` / `jobs.cancel` / `jobs.retry` / `jobs.get` / `jobs.list`
- `runs.list` / `runs.get` / `runs.compare`
- `datasets.list` / `datasets.preview` / `datasets.health`
- `candles.query` / `series.query` / `tiles.query` / `trades.list`
- `streams.pull` / `streams.cancel`

### Events
- `job.progress` / `job.log` / `job.completed` / `job.failed`
- `job.canceled`

### Error shape（草案）
- `{ code: string, message: string, details?: any, hint?: string, correlation_id?: string }`

## 6) 依赖决策（D-IDs）
- [D-013](03-decisions-open-questions.md)（IPC/streaming）
- [D-014](03-decisions-open-questions.md)（Job）
- [D-003](03-decisions-open-questions.md)（索引存储）
- [D-004](03-decisions-open-questions.md)（传输格式）

## 7) 本文输出物
- 组件图（后续补图）
- API surface 列表（commands/events 概览，细节见 `18`/`15`）

## 8) 验收（占位）
- UI 不可直接依赖“回测实现细节 crate”；只能通过 host services API 访问（可通过依赖图/模块边界审查保证）。
- 协议版本升级时（UI 与 Rust 版本不一致）能给出明确的兼容提示与降级策略（与 `18` 对齐）。
