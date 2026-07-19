# Task List —— 每语句表加载归属者 · 移植到其它连接器

> **唯一进度清单**。每完成一项随 commit 勾 `[x]` + 填状态/commit。ID 一旦分配永不复用。
> 立项流程、约束铁律、模板见 [`README.md`](./README.md)。地基（fe-core/api SPI）已就位，移植=纯连接器侧。
> 状态图例：⏳ 待启动 · 🔍 recon 中 · 🚧 进行中 · ✅ 完成 · 🔬 复核判不必做 · ❌ 阻塞

## 总览

- **蓝本 = iceberg（PERF-07 已完成）**：`ConnectorStatementScope`(fe-core/api，中性) + `IcebergStatementScope`(连接器) + 四处 `resolveTable*` 共享 + 拆胖句柄 + 删除暂存下沉 + v3 fail-loud。commits `97bdcd6bdbe`(fe-core) + `ea7fd1f6e7a`(iceberg)。
- **本清单只跟踪"其它连接器"的移植**；每项**第一步是 recon**（可能复核判"不必做"）。

| ID | 连接器 | 候选度 | 状态 | 备注 | commit |
|---|---|---|---|---|---|
| PORT-01 | paimon | 高 | ⏳ 待 recon | loadTable 触点最多(4)；写路径结构待确认 | |
| PORT-02 | hive / hms | 中-高 | ⏳ 待 recon | plain-hive 读写活；hms 网关委派 sibling(休眠)——网关按 handle 选 provider 的特殊性要单独设计 | |
| PORT-03 | hudi | 中 | ⏳ 待 recon | 读为主(MTMV)；写臂/多载程度待确认 | |
| PORT-04 | maxcompute / es / jdbc / trino | 低 | ⏳ 待判定 | 0 个 metastore loadTable fan-out；大概率**排除**，下 session 确认后标 🔬 | |

---

## 说明

- iceberg 本身**不在本清单**（已由 PERF-07 完成，见 `plan-doc/perf-hotpath-iceberg/`）。
- 各连接器完成后建各自 `designs/PORT-<connector>-{design,summary}.md`（不预建）。
- **不立项**的连接器（复核判不必做）标 🔬 + 一句原因，保留占位。
