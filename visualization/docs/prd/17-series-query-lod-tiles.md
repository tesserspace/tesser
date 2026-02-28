# 17. 序列查询 API 与 LOD/Tiles 合约（Series Query + LOD/Tiles）

> 本文定义的是“数据合约”，用于保证：在**数据量极大**且**机器资源有限**时，UI 仍能做到 pixel-bound 的流畅交互（见 `10`）。

## 1) 目标与范围
- 定义查询合约：`(range, target_points)` → 返回“像素级”数据量（不做全量拉取）。
- 定义 LOD 金字塔与 tiles 的生成/存储/查询策略（一期 candle 优先；为 tick/L2 预留扩展点）。
- 定义确定性与可复现要求：同一输入必须得到同一输出（允许容差的场景必须显式声明并可审计）。

## 2) 核心术语与不变量

### 2.1 时间与 range
- 时间单位：`ms since epoch`。
- `range`：采用闭开区间 `[start_ms, end_ms)`（`end_ms` 不包含）。
- 时区：存储与查询口径一律 UTC；展示可切换（见 `10-principles-slas.md`）。

### 2.2 `target_points` 与 pixel-bound
- `target_points`：UI 依据可视区域宽度计算的目标点数。
  - 建议口径：以 device pixel 为准，`target_points ~= ceil(css_px_width * devicePixelRatio)`（避免高 DPI 下“1px 多点”造成 aliasing）。
- 合约：返回点数必须满足 `points_returned <= target_points * (1 + ε)`。
  - `ε`：当采用 min/max 等“每 bucket 多点”算法时允许的松弛因子（默认建议 `ε<=1.0`；需在 D-006/D-010 的 spike 后固化）。
- 任何超过 pixel-bound 的“全量拉取”必须是显式操作，并提供资源预估（见 `10` 与 `12`）。

### 2.3 LOD / Tile / Profile
- LOD level：对时间轴做分桶聚合后的分辨率层级（L0=最细；L1+=更粗）。
- Tile：某个 LOD level 下，覆盖一个时间窗口的分片（chunk），用于缓存与快速随机访问。
- `lod_profile`：聚合算法/口径的版本化标识（例如 `candles_ohlcv_v1`）；变更必须可迁移/可并存。

## 3) Query API（commands 合约）

> 控制面返回 JSON 元数据；大序列通过二进制流返回（见 `18-ipc-transport.md`）。

### 3.0 通用响应与错误（最小合约）
- 所有 commands 必须回显：`correlation_id`（便于端到端追踪；见 `10`）。
- Arrow 行排序（硬要求）：
  - 单 symbol：按 `ts_ms` 升序。
  - 多 symbol：必须显式约定排序（建议 `(symbol, ts_ms)` 升序），或拆分为多 stream（需在实现前固化）。
- 错误 shape（最小）：`{ code, message, details?, hint?, correlation_id }`（见 `11` 与 D-024）。

### 3.1 Dataset → Candle 查询（高层）
`candles.query`（建议形态）：
- 输入（JSON）：
  - `dataset_id`
  - `dataset_manifest_ref?`
    - 形态：`{ manifest_hash, uri? }`（与 `13` 对齐；`uri?` 禁止绝对路径）
    - 若为空：使用 `datasets/<dataset_id>/manifest.json` 的 `active_manifest_hash`（见 `16`）；响应必须回填最终使用的 `manifest_hash`
    - 约束：未固定 `manifest_hash` 的请求只表示 interactive 浏览；客户端不得对其做跨会话的持久缓存键
  - `symbols?`（一期可只支持单 symbol；但字段需预留）
  - `range: { start_ms, end_ms }`
  - `target_points`
  - `prefer_tiles?`: bool（默认 true）
  - `allow_raw_fallback?`: bool（默认 true；当 tiles 不存在时允许即席聚合）
  - `correlation_id`
- 输出：
  - `meta`（JSON）：`{ correlation_id, manifest_hash, lod_profile, lod_level, bucket_ms, points_returned, data_source: tiles|raw_fallback, cache: hit|partial_hit|miss, tiles_missing? }`
  - `stream_ref`：`StreamRef`（见 `18-ipc-transport.md`；format 为 Arrow IPC streaming）

### 3.2 Run → Series 查询（结果序列）
`series.query`（建议形态）：
- 输入：`{ run_id, kind, range, target_points, correlation_id }`
- 输出：
  - `meta`：`{ correlation_id, lod_profile, lod_level, bucket_ms, points_returned }`
  - `stream_ref`：`StreamRef`（见 `18-ipc-transport.md`）
    - 最小列：`ts_ms, value`
    - 允许扩展列：`min_value?, max_value?`（用于 zoom-out 时保峰值；需在 D-006 固化）

> Run 的 series 建议落盘为可查询的 LOD 格式（见 `13-runspec-artifacts-schema.md` 的 `equity.lod.parquet` 占位），避免 UI 打开历史 run 仍要全量解码。

### 3.3 Tiles 查询（低层，给缓存命中路径用）
`tiles.query`（占位，可在实现时决定是否暴露给 UI）：
- 输入：`{ dataset_id, manifest_hash, lod_profile, level, tile_ids[], correlation_id }`
- 输出：`stream_ref`（见 `18-ipc-transport.md`）+ `meta`（tile 覆盖范围、缺失 tile 列表）

### 3.4 Trades / 明细查询
`trades.list`（占位，细节见 `11`）：必须分页/可取消，不得一次拉全量。

## 4) LOD 选择规则（必须确定性）

### 4.0 LOD 选择函数（规范，占位但必须在实现前固化）
- `allowed_bucket_ms`：允许的 bucket 步长集合（建议固定集合，如 `60s, 120s, 300s, 900s, 3600s, ...`；未来可由 manifest 的 `time_semantics` 扩展）。
- `points_per_bucket`：由 `lod_profile` 决定（例如 candle=1；series `last`=1；series `minmax`=1 或 2，需固化）。
- 选择规则（建议）：取最小 `bucket_ms ∈ allowed_bucket_ms`，使
  - `num_buckets(range, bucket_ms) <= floor(target_points / points_per_bucket)`
  - 若多档满足：选更细（更小的 `bucket_ms`）。

### 4.1 Candle 聚合口径（L1+）
- 对每个 bucket（时间桶）输出 1 根聚合 candle：
  - `open=first`, `high=max`, `low=min`, `close=last`, `volume=sum`
- bucket 边界必须锚定固定原点（建议 epoch 0），range 只做裁剪不得“漂移桶边界”：
  - `bucket_start_ms = floor(ts_ms / bucket_ms) * bucket_ms`
- `bucket_ms` 必须是 base resolution 的整数倍（一期仅 candle 时可直接约束为 60s 的倍数）。

### 4.2 Series（折线/面积）聚合口径（占位但需留接口）
- zoom-out 时必须保留极值，避免“下采样抹平峰谷”。
- 建议支持两种输出模式（由 `lod_profile` 固化）：
  - `last`：每 bucket 输出 `{ts_ms, value=last}`
  - `minmax`：每 bucket 输出 `{ts_ms, min_value, max_value, last_value?}`

## 5) Tiles：生成、落盘、失效与查询

### 5.1 缓存键与失效（硬要求）
- Tiles 必须按 tileset key 分桶，确保：
  - 数据集内容变更 → `manifest_hash` 变化 → tiles 自动失效（不复用旧 tiles）。
  - 不同聚合算法（`lod_profile`）可并存。
  - 不同 tile schema 版本可并存。

建议键（规范化）：
- `tileset_key = (dataset_id, manifest_hash, lod_profile, tile_schema_version)`
- `tile_key = (tileset_key, level, tile_start_ms [, symbol?])`
  - `tile_start_ms` 必须按 `tile_span_ms(level)` 对齐到固定原点（与 §4 的 bucket 对齐一致）。

### 5.2 目录布局（与 `12/16` 对齐）
- 建议落盘在 `cache_root`：
  - `cache/tiles/<dataset_id>/<manifest_hash>/<lod_profile>/<chunk>.parquet`
  - 其中 `<chunk>` 必须可解码出 `level + tile_window`（例如子目录 `L3/<tile_start_ms>.parquet` 或编码到文件名里）。

### 5.3 生成方式（与 Job 对齐）
- `tiles_build` 必须是 Job（见 `15-jobs-lifecycle.md`），并满足：
  - 可取消、可恢复（至少 attempt 级别可重试）
  - 写入采用临时文件 + 原子 rename（见 `12`）
  - 完成后更新 tiles 索引（SQLite 或等价结构；见 D-003）

### 5.4 缺失 tiles 的行为（必须可解释）
- 当 `prefer_tiles=true` 且 tiles 缺失：
  - 若 `allow_raw_fallback=true`：允许对 raw partitions 做即席聚合并返回，但**必须复用与 tiles_build 完全一致的聚合实现/参数**（同 `lod_profile`、同取整/舍入/缺失处理），否则会破坏确定性；同时可在后台触发 `tiles_build`（产品策略，非硬要求）。
  - 若 `allow_raw_fallback=false`：返回结构化错误（见 D-024），UI 提示用户先生成 tiles。

## 6) Tick/L2 扩展点（不要求一期实现）
- Tick：保留 `trades.list` 分页接口，并允许未来引入 tick LOD（例如分桶的成交量/方向直方图）。
- L2：优先采用 heatmap tiles（time×price bins），并明确分辨率预算（见 D-011）。

## 7) 确定性、可复现与回归（与 `10/13/22` 对齐）
- LOD/tiles 生成与查询必须是确定性的：
  - 同 `manifest_hash + range + target_points + lod_profile` → 同输出（顺序、边界、聚合结果一致）。
- 数值稳定性（占位但必须固化）：若使用浮点聚合，必须在 `lod_profile` 固化舍入策略或固定归约顺序；跨平台一致性容差口径见 D-016/D-031。
- 必须纳入 golden fixtures（见 D-022）：
  - 对固定基准数据集（D-021）生成固定查询样例，作为回归与跨平台一致性检查。

## 8) 依赖决策（D-IDs）
- [D-006](03-decisions-open-questions.md)（tiles 格式与 LOD profile 固化）
- [D-010](03-decisions-open-questions.md)（渲染栈与前端下采样/解码链路）
- [D-011](03-decisions-open-questions.md)（tick/L2 的可视化形态）
- [D-018](03-decisions-open-questions.md)（manifest schema）
- [D-025](03-decisions-open-questions.md)（dataset layout canonical）
- [D-030](03-decisions-open-questions.md)（时间口径与 health/gap 基准）
- [D-004](03-decisions-open-questions.md)（大序列传输格式）
- [D-013](03-decisions-open-questions.md)（IPC/streaming + 背压/取消）
- [D-022](03-decisions-open-questions.md)（golden fixtures）
- [D-003](03-decisions-open-questions.md)（索引/SQLite）
- [D-014](03-decisions-open-questions.md)（Job 语义）
- [D-021](03-decisions-open-questions.md)（benchmark 数据集）
- [D-024](03-decisions-open-questions.md)（错误码体系）
- [D-016](03-decisions-open-questions.md)（determinism/tolerance）
- [D-031](03-decisions-open-questions.md)（数值精度策略）

## 9) 验收（草案）
- `candles.query/series.query`：返回点数满足 pixel-bound 上限；并支持取消（见 `18`）。
- tiles 缓存：按 `manifest_hash` 正确失效；跨 root 修复后仍可命中或可重建（见 `12/16`）。
- 确定性：同输入在同机与跨 OS（在容差范围内）一致；至少 1 组 golden fixture 覆盖 LOD 边界与 bucket 对齐。
