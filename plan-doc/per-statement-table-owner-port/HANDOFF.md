# 🤝 Session Handoff —— 每语句表加载归属者 · 移植/重构

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`progress.md`](./progress.md)；状态见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本轮（2026-07-19 session 6）已完成：**HMS 异构网关兄弟元数据每语句去重（读取键石之后的第二大步）完全落地**

## 一句话结果
- **背景**：一个 HMS 目录同时管 Hive/Iceberg/Hudi 三类表，Hive 连接器充当网关、把 Iceberg/Hudi 表的操作转发给内嵌"兄弟连接器"。此前网关**每转发一次就新建一个兄弟元数据外壳**（一条 SELECT 扫一张 HMS-Iceberg 表要重建十几次）。本轮把兄弟元数据也纳入"每语句一实例"。
- **grounding 纠偏**：整个 hive 连接器"取兄弟元数据"仅 **4 处**（3 个 helper + `getTableSchema` 旁路）；文档"~43 处 per-handle 改道"实为误导——40+ 处转发 + `beginTransaction(session,handle)` 全穿第三个 helper、改动零行。
- **已落地（全绿，commit `5fd55d0a32a`）**：
  - 新增 `SiblingOwner{connector,label}`（`ICEBERG_LABEL`/`HUDI_LABEL` 常量单一真源）；`HiveConnector.resolveSiblingOwnerLabeled` 命中臂带标签、`resolveSiblingOwner` 委派它（3 个 provider seam 字节不变）。
  - `HiveConnectorMetadata.memoizedSiblingMetadata` key=`metadata:<catalogId>:<label>`（三连接器共享 catalogId，靠标签区分，否则塌成一个派错）；3 helper + 旁路收口；`beginTransaction` 免费覆盖。NONE 下工厂每调 = 字节等价；仅用 fe-connector-api 类型（不 import fe-core）；兄弟只作 `Connector`/`ConnectorMetadata` 持有、绝不 cast。
  - **e2e 时机**：用户拍板"随后续统一补"——本步只做连接器单测锁死机制，异构网关 e2e 留切换阶段统一补。
- **验证**：全模块 348 单测全过（含 5 新去重断言）；checkstyle 0 违规；fe-core 漏斗门禁 + 连接器 import 门禁 exit 0；对抗复审（2 视角 + 逐条核验）**零 finding**。
- **carry-forward（非本轮引入）**：`listFileSizes` 已正确走漏斗、但未列入 `HiveConnectorMetadataSiblingDelegationTest.EXPECTED_METHODS` 转发面锁——留作后续测试硬化。

---

# ➡️ 下一个 session = **写入共用一个元数据实例（RD-3 / STEP 3）**

## 第一件事（先读）
1. 读 `designs/expanded-scope-phasing-and-security-decisions.md` **§1**（读写共用忠实 Trino：`CatalogTransaction` 单实例读写共用；两条正确性闸门）+ **§4 表 STEP 3**（拆 3a/3b + 闸门 + 纠缠点）。
2. 读 `designs/P1-implementation-design.md` §6（read-vs-write 复用决策，写侧当时留后续）。
3. 读架构记忆 `iceberg-table-resolution-cache-scoping`（写侧去重收益薄、真价值=架构连贯 + stash 下单例；碰铁律 A 的高度问题待用户定）、`plugindriven-mvcc-table-is-live-not-dormant`、`catalog-spi-plugin-tccl-classloader-gotcha`。

## A. 动码前先 grounding + 出中文方案待确认（本任务铁律：设计先行）
- **写臂 8 处 `getMetadata` 现在仍裸调**（带 `getMetadata-funnel-exempt` 标记豁免于 fe-core 漏斗门禁）。STEP 3a 把它们改道进 `PluginDrivenMetadata.get`（fe-core 统一入口）并**删对应标记 → 门禁自动收紧**。这 8 处（行号会漂移，须逐处对当前代码核实）：
  - `PhysicalPlanTranslator.visitPhysicalConnectorTableSink`（INSERT）/ `buildPluginRowLevelDmlSink`（DELETE/MERGE）
  - `PluginDrivenInsertExecutor.ensureConnectorSetup`
  - `PluginDrivenExternalTable.resolveWriteTargetHandle`（藏在"读"文件里的写专用点）
  - `BindSink.checkConnectorStaticPartitions` / `checkConnectorWritePartitionNames`
  - `IcebergRowLevelDmlTransform.checkPluginMode`
  - `PhysicalIcebergMergeSink.buildInsertPartitionFieldsFromConnector`
- **注意与 STEP 2 的差别**：STEP 3 主要改**fe-core**（不像 STEP 2 全在连接器内）——用户 2026-07-19 已豁免铁律 A，但 3b 若要新增 fe-core SPI（`CatalogStatementTransaction`）须先向用户确认高度（见记忆 `iceberg-table-resolution-cache-scoping` 末：全高度重写留远期）。
- 出中文方案（不引任务代号）：读写为何可共用一个实例（对齐 Trino `CatalogTransaction`）、两条闸门怎么守、3a/3b 怎么拆、纠缠点怎么定。**待用户确认后再改代码。**

## B. 实现要点（§1 + §4 STEP 3）
1. **3a 无状态写点改道**：8 处改走统一入口（读写在同一语句共用那**一个** memoized 元数据实例）；删对应 funnel-exempt 标记，门禁自动把这些点纳入。
2. **两条必守闸门（正确性，非性能）**：
   - **按连接器保留"起写刷新"**：hive 起写必须保留 `HiveConnectorTransaction.beginWrite` 当场 `hmsClient.getTable`（ACID 事务表拒绝 + 权威起始快照）——**别换成扫描缓存表**；iceberg 靠 `newTransaction()` refresh。
   - **读写身份一致性**：iceberg 接 REST、session=user 时 metadata 烤进 per-user 委派操作；读/写同语句本是同一用户，但须**显式断言身份指纹相等**再共用（`IcebergConnector.getMetadata` 的 `newCatalogBackedOps`）。
3. **3b 事务归属上移**：把首次写懒建的 `ConnectorTransaction` 并入语句级共持体（范本 `ConnectorRewriteDriver` co-hold metadata/session + beginTransaction + commit/rollback）；定 **commit/rollback vs `closeAll` 顺序**（语句末确定性收尾未提交事务 + 幂等）；保留 tx↔session 绑定（`setCurrentTransaction`，不统一两个 session）。
4. **纠缠点（3b 时请用户定）**：iceberg 起写复用读的 `IcebergStatementScope.sharedTable` side-car，而该 side-car 在 P2 计划里要删 → 先接旧 side-car、P2 再搬 **vs** 先做个 iceberg 最小 P2 前置。

## 后续步（详见分期定稿 §4）
- **STEP 4 缓存隔离（安全 track，可与 3 并行启动）**：`getIdentityShardKey()` SPI → iceberg 三投影缓存 Key 分片 + fe-core 表结构缓存 bypass(先)/分片(后) + 防漂移门禁；**随本轮 STEP 2 的属主键覆盖异构网关**（异构网关是最尖的越权洞）；越权 e2e + 威胁模型签字。

## 铁律 / 闸门提醒
- 用户 2026-07-19 已豁免铁律 A（fe-core 只减不增）。仍守：连接器 connector-agnostic（作用域值 Object）；作用域跨用户即泄漏；**fe-connector-* 不得 import fe-core**（门禁）。
- **动码前探测并发活动**（git log/status + maven 进程 + 近 90s mtime），发现活跃即停手（`concurrent-sessions-shared-worktree-hazard`）。
- 本任务设计先行：调研设计阶段结束、正式改代码前，先把方案用中文详述待用户确认；e2e 沿用"择机统一补"（异构网关 e2e 随 STEP 3/4 统一补）。

---

# 🗂 遗留 / 关联
- 分期定稿（现行主线）：`designs/expanded-scope-phasing-and-security-decisions.md`（§1 读写共用、§4 分期 STEP 3、§6 残留风险）。
- STEP 1 蓝图 + read-vs-write：`designs/P1-implementation-design.md`（§5 HMS sibling 已落地；§6 写侧留后续=现在做）。
- STEP 2 grounding/复审 workflow：`wf_62fa5a7f-07a`（grounding）、`wf_e55f3a51-561`（对抗复审，零 finding）。
- 门禁：`tools/check-fecore-metadata-funnel.sh`（STEP 3a 删 8 处 `getMetadata-funnel-exempt` 标记后自动收紧）、`tools/check-connector-imports.sh`。
- 架构记忆：`iceberg-table-resolution-cache-scoping`、`catalog-spi-plugin-tccl-classloader-gotcha`、`hms-iceberg-delegation-needs-e2e`、`hudi-mtmv-freshness-real-instant`。
- e2e：异构网关 e2e（INSERT/DELETE/MERGE/ALTER/EXECUTE 对比独立 iceberg 目录）落 `regression-test/suites/external_table_p2/refactor_catalog_param`，随后续统一补。
