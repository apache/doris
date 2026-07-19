# Task List —— 每语句表加载归属者 · 移植到其它连接器

> **唯一进度清单**。每完成一项随 commit 勾 `[x]` + 填状态/commit。ID 一旦分配永不复用。
> 立项流程、约束铁律、模板见 [`README.md`](./README.md)。地基（fe-core/api SPI）已就位，移植=纯连接器侧。
> 状态图例：⏳ 待启动 · 🔍 recon 中 · 🚧 进行中 · ✅ 完成 · 🔬 复核判不必做 · ❌ 阻塞

## ⭐ 现行主线 = Trino 式重构分期（2026-07-19 session 3 定稿）

> 逐连接器移植（PORT-01~04）经复核**全部 🔬 判暂不做**；用户改为**直接重构成 Trino 式"引擎自持每语句 metadata 实例"架构**（豁免铁律 A）。定稿分期见 `designs/expanded-scope-phasing-and-security-decisions.md`。不砍任何一块，只排序 + 独立提交/验证。

| ID | 步 | 状态 | 内容 | 依赖 | commit |
|---|---|---|---|---|---|
| RD-1 | STEP 1 读取键石 | 🚧 进行中（C1+C2+C3 已落地，仅剩防漂移门禁） | C1 地基 ✅ + C2 关闭接线 ✅ + C3 读/扫描/DDL/MVCC 改道（49 处）✅ + 扫描节点存字段 ✅ + 后台 loader 显式 NONE 读穿（9 处，含名字映射两缝）✅ + 修 fetchRowCount ANALYZE 隐患 ✅ + `buildCrossStatementSession` 助手 ✅ + 守门测试 ✅（247+18 绿、checkstyle 0）。剩：防漂移门禁（bash grep，形态=统一入口零例外）⏳ | — | C1 `5b7312f9d1f` · C2 `12f3e95239b` · C3 待提交 |
| RD-2 | STEP 2 HMS 兄弟扇出 | ⏳ 待启动 | 键=(catalogId, ownerLabel)；兄弟 getMetadata **及 beginTransaction** 进同一 funnel；异构网关 e2e | RD-1 | |
| RD-3 | STEP 3 写入共用 | ⏳ 待启动 | 3a 无状态写点改道进 funnel；3b `ConnectorTransaction` 归属上移 `CatalogStatementTransaction` + commit/rollback vs closeAll 顺序。闸门=读写身份等价 + 保留 hive 起写刷新 + 保留 tx↔session 绑定 | RD-1,RD-2 | |
| RD-4 | STEP 4 缓存隔离（安全 track） | ⏳ 待启动（**list≠load 已确认=真实越权**） | `getIdentityShardKey()` SPI → iceberg 三投影缓存 Key 分片 + fe-core 表结构缓存 bypass(先)/分片(后) + 防漂移门禁；随 STEP 2 属主键覆盖异构网关；越权 e2e | 威胁模型签字 + RD-2 | |

> **纠缠点（RD-3b 时定）**：iceberg 起写复用表读 `IcebergStatementScope.sharedTable`，该 side-car 在 P2 计划要删 → 先接旧 vs 先做 iceberg 最小 P2 前置。

---

## 总览（历史 · 逐连接器移植框架，已被重构主线取代）

- **蓝本 = iceberg（PERF-07 已完成）**：`ConnectorStatementScope`(fe-core/api，中性) + `IcebergStatementScope`(连接器) + 四处 `resolveTable*` 共享 + 拆胖句柄 + 删除暂存下沉 + v3 fail-loud。commits `97bdcd6bdbe`(fe-core) + `ea7fd1f6e7a`(iceberg)。
- **本清单只跟踪"其它连接器"的移植**；每项**第一步是 recon**（可能复核判"不必做"）。

| ID | 连接器 | 候选度 | 状态 | 备注 | commit |
|---|---|---|---|---|---|
| PORT-01 | paimon | 高 | 🔬 复核判暂不做（**将来候选**） | recon 双签：写未迁移→只读，DML=0；加载已被 transient 胖句柄+SDK CachingCatalog 压到≈1。**触发点=paimon 加行级 UPDATE/DELETE**（届时复现风暴，且是抽共享 helper 的正确时机）。详见 designs 复核结论文档 §2/§5 | |
| PORT-02 | hive / hms | 中-高 | 🔬 复核判不必做 | recon 双签：仅 INSERT/OVERWRITE 无行级写；跨查询 `CachingHmsClient.tableCache` 已把加载压到≈0~1（作用域的超集）；无胖句柄/暂存；网关只做 1 次探测加载后委派兄弟，兄弟自带作用域 | |
| PORT-03 | hudi | 中 | 🔬 复核判不必做 | recon 双签：Doris 侧只读（写全拒）；5 步 4 步空操作；唯一读侧成本=未缓存 metaClient 重建，形状不对+可变+鉴权上下文错配，非本模式 | |
| PORT-04 | maxcompute / es / jdbc / trino | 低 | 🔬 排除 | recon 双签：均无行级 DML（es/trino 只读、jdbc/maxcompute 仅 INSERT-append）；无暂存/胖句柄。**trino 读侧重解析最贵**=另立"读侧去重"议题的占位，不属本模板 | |

---

## 说明

- iceberg 本身**不在本清单**（已由 PERF-07 完成，见 `plan-doc/perf-hotpath-iceberg/`）。
- 各连接器完成后建各自 `designs/PORT-<connector>-{design,summary}.md`（不预建）。
- **不立项**的连接器（复核判不必做）标 🔬 + 一句原因，保留占位。

## 本轮结论（2026-07-19 · recon 全部完成）

- **四项全部 🔬**：无连接器现在值得移植——iceberg 独特在它是**唯一迁移了行级写（DELETE/MERGE）**的连接器，别的连接器只读/仅追加写，没有多臂重复加载风暴，现有跨查询缓存又已把加载压到≈1。
- **统一接口标准已存在**：中性 SPI `getStatementScope()` + `ConnectorStatementScope`（新连接器天生继承）。缺的只是"更强的强制/便利外壳"，现在做=单用户过早抽象。
- **推荐高度 L0**（写下约定 + 登记 paimon-加写触发点，生产逻辑零改动）；L1（抽共享 helper）留到 paimon 加写；**L2/L3（Trino 式重构）下个 session 专题讨论**。
- 全部依据：`designs/recon-findings-and-trino-refactor-groundwork.md`。
