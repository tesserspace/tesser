# 20. 可视化组件与性能预算（Visualization & Performance Budgets）

## 1) 目标
- 定义图表组件（K 线、equity、drawdown、trades markers、heatmap 的未来形态）。
- 定义渲染与交互预算（对齐 `10` 的 SLA）。
- 定义前端虚拟化与数据结构（TypedArray、索引定位、分页）。

## 2) 渲染策略
- Candle/series：Canvas 优先；在高密度叠加时可选 WebGL（可配置）
- 表格：虚拟列表（只渲染可见行）
- hover：binary search 时间戳索引；避免 O(n) 命中
- 数据预算：所有图表数据必须通过 `17` 的 pixel-bound 合约获取（`target_points ~= ceil(css_px_width * devicePixelRatio)`），不得在前端全量解码后再丢弃。

## 3) 多 run 对比
- 叠加规则：颜色/透明度、归一化基线、单位一致性
- 数据量控制：每条曲线都必须下采样到 target_points
- 峰值保真：zoom-out 需要保峰值时，优先消费后端 `min_value/max_value`（见 `17` 的 `lod_profile` 约定），避免前端二次重采样导致不一致。

## 4) 大数据降级策略
- 先 coarse LOD 出图 → zoom-in 请求更细 tiles
- 明细（trades/fills/events）默认分页/按需

## 5) 依赖决策（D-IDs）
- [D-010](03-decisions-open-questions.md)（渲染栈）
- [D-006](03-decisions-open-questions.md)（tiles）
- [D-004](03-decisions-open-questions.md)（传输）

## 6) 验收（占位）
- 在基线机器上：多 run 叠加（N 条曲线）满足 `10` 的 pan/zoom/hover SLA；表格虚拟化在 1e6 行级别不崩溃（分页/按需加载）。
- 关闭硬件加速或低端 GPU 时：有明确降级（减少叠加、降低 target_points、禁用某些效果），且 UI 提示清晰。
