# 12. Workspace/存储布局与可移植性（Storage & Portability）

## 1) 目标
- 支持多根目录（workspace/datasets/cache/artifacts），跨平台一致。
- 支持外置盘/路径变更后的“可修复”体验。
- 控制磁盘增长（配额/LRU/pin）。

## 2) Roots 与目录布局（草案）

### 2.1 Roots
- `workspace_root`：用户项目与 runs（可选多个）
- `dataset_root`：大数据集存放处（可选多个，允许外置盘）
- `cache_root`：LOD/tiles、下载缓存、临时文件（可选多个）

### 2.2 目录结构（示意）
- `workspaces/<workspace_id>/runs/<run_id>/...`
- `datasets/<dataset_id>/manifest.json`（可变指针：仅保存 active manifest hash）
- `datasets/<dataset_id>/manifests/<manifest_hash>.json`（不可变修订）
- `datasets/<dataset_id>/data/partitions/<partition_id>.parquet`
- `datasets/<dataset_id>/raw/...`（可选：下载原始文件）
- `datasets/<dataset_id>/index/...`（索引/统计）
- `datasets/<dataset_id>/fingerprints/<manifest_hash>.json`（strict/派生）
- `datasets/<dataset_id>/health/<manifest_hash>.health.json`（可缓存）
- `cache/tiles/<dataset_id>/<manifest_hash>/<lod_profile>/<chunk>.parquet`

## 3) 可移植性策略
- 路径规范化：记录 “root-relative path” + “content fingerprint”，避免绝对路径绑死。
- 修复流程：当 root 不可达/拔盘时，UI 提示并允许重新绑定 root。

## 4) 配额、LRU、Pin（强约束）
- 配额：用户可配置总上限与各类上限（runs/tiles/download cache）。
- LRU：按最后访问时间淘汰；Pin 的 run/dataset 不可被自动清理。
- 预估：任何会生成大对象的 Job 必须先给出磁盘占用预估（区间）。

## 5) 原子性与一致性
- 大对象落盘：写临时文件 → fsync → rename 原子替换。
- 索引更新：SQLite 事务（与 `15-jobs-lifecycle.md` 对齐）。

## 6) 依赖决策（D-IDs）
- [D-019](03-decisions-open-questions.md)（多根目录）
- [D-020](03-decisions-open-questions.md)（配额/保留）
- [D-003](03-decisions-open-questions.md)（索引存储）
- [D-025](03-decisions-open-questions.md)（dataset layout canonical）

## 7) 验收（占位）
- 外置盘拔出/重命名/迁移后：应用能提示并允许重新绑定 root；绑定后 runs/datasets 可继续访问或给出明确修复步骤。
- 配额触发时：清理不破坏 pinned runs；不会留下索引指向不存在文件的状态。
