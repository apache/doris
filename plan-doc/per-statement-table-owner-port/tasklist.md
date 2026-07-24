# Task List —— 每语句表加载归属者 · 移植到其它连接器

> **唯一进度清单**。每完成一项随 commit 勾 `[x]` + 填状态/commit。ID 一旦分配永不复用。
> 立项流程、约束铁律、模板见 [`README.md`](./README.md)。地基（fe-core/api SPI）已就位，移植=纯连接器侧。
> 状态图例：⏳ 待启动 · 🔍 recon 中 · 🚧 进行中 · ✅ 完成 · 🔬 复核判不必做 · ❌ 阻塞

## ⭐ 现行主线 = Trino 式重构分期（2026-07-19 session 3 定稿）

> 逐连接器移植（PORT-01~04）经复核**全部 🔬 判暂不做**；用户改为**直接重构成 Trino 式"引擎自持每语句 metadata 实例"架构**（豁免铁律 A）。定稿分期见 `designs/expanded-scope-phasing-and-security-decisions.md`。不砍任何一块，只排序 + 独立提交/验证。

| ID | 步 | 状态 | 内容 | 依赖 | commit |
|---|---|---|---|---|---|
| RD-1 | STEP 1 读取键石 | ✅ 完成 | C1 地基 ✅ + C2 关闭接线 ✅ + C3 读/扫描/DDL/MVCC 改道（49 处）✅ + 扫描节点存字段 ✅ + 后台 loader 显式 NONE 读穿（9 处，含名字映射两缝）✅ + 修 fetchRowCount ANALYZE 隐患 ✅ + `buildCrossStatementSession` 助手 ✅ + 守门测试 ✅（247+18 绿、checkstyle 0）+ **防漂移门禁**（bash grep，读取侧零例外、写入 8 处 `getMetadata-funnel-exempt` 标记豁免；自测+validate exec）✅ | — | C1 `5b7312f9d1f` · C2 `12f3e95239b` · C3 `eecb390c4ac` · 门禁 `b2d147998d1` |
| RD-2 | STEP 2 HMS 兄弟扇出 | ✅ 完成（e2e 随后统一补） | 4 处取元数据点(3 helper by-TYPE/by-HANDLE + `getTableSchema` 旁路)收口 `memoizedSiblingMetadata`，key=(catalogId, ownerLabel)；兄弟 getMetadata **及 `beginTransaction(session,handle)`** 同入口(后者免费覆盖)；`SiblingOwner` 命中臂带标签(拒 force-build supplier 比对)；5 新去重单测(1建/NONE每调/by-TYPE==by-HANDLE/跨catalog/iceberg-hudi标签隔离)；348 全绿+门禁 exit0+对抗复审零 finding。异构网关 e2e 择机统一补 | RD-1 | `5fd55d0a32a` |
| RD-3 | STEP 3 写入共用 | ✅ 完成 | 3a 8 处无状态写点改道进 funnel + 删 8 豁免标记(门禁 100% 无例外) + 收口身份一致 fail-loud 断言(getUser)；3b 新增 `CatalogStatementTransaction` 共持体(begin 从共享 metadata 铸事务+全局注册) + 执行器经它开事务 + 两趟 closeAll(先 finalize 事务再关 metadata) + 管理器 `isActive` 幂等兜底(绝不撤已提交)。闸门1(hive 起写自取表)天然满足、闸门2(身份)断言守、tx↔session 绑定/全局注册/提交时机全保留。103 单测绿(7 新)+ checkstyle 0 + 双门禁 exit 0。异构网关写/开事务上一步已免费覆盖，e2e 随后统一补 | RD-1,RD-2 | 3a `f208036f3c5` · 3b `a03b88b0d80` |
| RD-4 | STEP 4 缓存隔离（安全 track） | ✅ 完成（e2e 随后统一补） | **路线纠偏（grounding+对抗验证）**：真活跃漏只 2 个（iceberg `latestSnapshotCache` + fe-core schema cache），partition/format 靠上游 per-user loadTable 已护（脆弱），异构网关是**潜在非活跃**洞（兄弟恒 hms flavor 永不 session=user）。**用户签字=禁用路线（非分片，无新 SPI、无撤权陈旧窗口）+ 三投影缓存统一禁用 + 网关 fail-loud 守卫**。4a iceberg 三缓存 session=user 置空(+beginQuerySnapshot null 回退+失效 null 守卫)；4b fe-core `shouldBypassSchemaCache`(镜像名字缓存 bypass)；4c hive 网关兄弟 session=user 即 fail-loud；4d 防漂移门禁 `check-authz-cache-sharding.sh`(+自测+maven validate)。116 单测绿(含新)+checkstyle 0+三门禁 exit 0+clean-room 对抗复审 0 blocker/major。越权+异构网关 e2e 随统一补 | 威胁模型签字 + RD-2 | 设计 `7cbc515` · 4a `9f88aa4` · 4b `92289e3` · 4c `c40af11` · 4d `5cb5fb9` |

> **纠缠点（RD-3b 已定=保持现状）**：grounding 核实 iceberg 表 side-car `IcebergStatementScope.sharedTable` 与本步**正交**——改道只动 fe-core `getMetadata`、事务上移只动 fe-core 事务生命周期，均不碰起写读表路径，起写照旧从 side-car 读表零改动。side-car 删除属更远期连接器内部重构(表→实例字段)，本步不做。

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
