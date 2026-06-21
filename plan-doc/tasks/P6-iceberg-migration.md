# P6 — iceberg 迁移（最大连接器；先在 fe-connector 实现完整能力 → 最后一次性翻闸）

> **阶段拆分 spec（phase-level plan，非全量 task TODO）**。本 doc 只定**阶段边界 / 顺序 / 验收门 / 开放决策**；每个阶段的**逐 task 拆解 + code-grounded recon** 在该阶段**启动时**单独做（AGENT-PLAYBOOK §7.1/§7.2），产出各阶段自己的 task 行。
> 样板 = `P5-paimon-migration.md`（paimon B0–B9 full-adopter + 翻闸）。Master plan = [`00-connector-migration-master-plan.md` §3.7](../00-connector-migration-master-plan.md)。
> 协作规范：[AGENT-PLAYBOOK.md](../AGENT-PLAYBOOK.md)。**每阶段按 §四 handoff + §五 文档同步纪律推进。**

---

## 元信息

- **状态**：🟢 规划完成（本 session 2026-06-21 brainstorm，用户签 **方案 A / 8 阶段 / 单一翻闸**）；**下一步 = P6.1 启动**（其 code-grounded recon + 逐 task 拆解）。
- **启动日期**：2026-06-21（阶段拆分设计）
- **目标策略**：**先在 `fe-connector-iceberg` 实现完整 iceberg 能力（P6.1–P6.5，全程不翻闸）→ P6.6 一次性翻闸 → P6.7 删 legacy → P6.8 回归**。用户定调（2026-06-21）：翻闸是 per-catalog-type 全有或全无（`CatalogFactory:104-113`），故不做中途/混合翻闸。
- **工作分支**：`catalog-spi-10-iceberg`（off `branch-catalog-spi` @ `e5959e1b53d`）。PR base = `branch-catalog-spi`，squash 合并（mirror P5-T29 #64653 / P3b #64655）。
- **阻塞**：P6.1 起步**无硬前置**（P3b kerberos 收口 + docker e2e 已清）。子阶段内前置见下 §阶段内前置。
- **阻塞下游**：P7 hive(+HMS) 复用 P6 的写路径 SPI / procedure SPI / DLA 模型；P8 收尾（删 `SPI_READY_TYPES`、删全部反向 instanceof）。
- **主 owner**：@morningman / TBD

---

## 阶段目标

把 fe-core `datasource/iceberg/`（**74 文件 / ~14.4K 行**）+ `transaction/IcebergTransactionManager` + `planner/Iceberg{Delete,Merge,Table}Sink` + nereids `Iceberg{Update,Delete,Merge}Command` + 反向引用，迁入 `fe-connector-iceberg`，按 paimon full-adopter 样板**最后一次性翻闸**（iceberg 进 `SPI_READY_TYPES`）并删 legacy。覆盖 iceberg 完整能力：**普通读 / 系统表 + 元数据列 / 写路径（INSERT/DELETE/UPDATE/MERGE）/ 10 个 procedure action / MVCC 时间旅行 / vended credentials / 7 catalog flavor**。

---

## 关键事实（本 session code-grounded 核读，2026-06-21）

- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory.java:51` = {jdbc, es, trino-connector, max_compute, paimon}），仍走 built-in `switch-case`。firsthand 核实。
- **翻闸 = 全有或全无**：`CatalogFactory:104-113` 一旦 `iceberg ∈ SPI_READY_TYPES`，catalog 变 `PluginDrivenExternalCatalog`，**所有**操作（metadata/scan/write/MVCC/sys-table）走连接器；seam 内**无** legacy-scan 回退。⇒ P6.1「metadata-only」**不能单独翻闸**。firsthand 核实。
- 连接器现状 = **6 个 skeleton 文件**（`IcebergConnector`/`Provider`/`ConnectorMetadata`[只读]/`ConnectorProperties`/`TableHandle`/`TypeMapping`）；`IcebergConnectorMetadata` 只有只读元数据方法；**0 测试**。
- **flavor dispatch 有 2 个真实缺口**：`IcebergConnector:120` 引用 `connector.iceberg.dlf.DLFCatalog` —— **该类不存在**（legacy 有 4-file `dlf/` 子树待 port）；`s3tables` 引用外部 SDK 类 `software.amazon.s3tables.iceberg.S3TablesCatalog`（需 maven dep）。其余 5 flavor（rest/hms/glue/hadoop/jdbc）走 SDK 内置 `catalog-impl`。
- **iceberg metastore-props 已在 fe-core**：`AbstractIcebergProperties` + 7 flavor（Rest/HMS/Glue/AliyunDLF/Jdbc/FileSystem/S3Tables）+ `IcebergPropertiesFactory`，已 wired 入 `MetastoreProperties.Type.ICEBERG`（`:50/:89`）。与 paimon still-consumed 同构 → **沿用 paimon 决策：留 fe-core 直到翻闸后**（删除属 backlog #2，与 hive/P7 共同设计，**不在 P6 scope**）。
- **3 个缺失 SPI**（firsthand 确认 `fe-connector-api` 里无）：`ConnectorProcedureOps`（卡 P6.4 actions）、扫描期 `ConnectorCredentials`（卡 P6.2 vended；今仅有 `ConnectorCapability` flag）、写路径 RFC（卡 P6.3，需 PMC 评审）。
- **写路径分布**：planner `Iceberg{Merge,Delete,Table}Sink.java` + nereids `commands/Iceberg{Update,Delete,Merge}Command.java` + `transaction/IcebergTransactionManager`。
- **反向 `instanceof IcebergExternal*` ≈ 49 处**，最密集在**写命令层**（`nereids/.../commands/*` ~11 文件）+ `datasource/iceberg`(6) + `catalog`(2) + `statistics`(2) + 散落 nereids rules/analyzer/glue/translator。⇒ P6.7 清理与 P6.3 写路径强耦合。
- legacy 重戏（行数）：`IcebergUtils` 1826 / `IcebergMetadataOps` 1362（读+写两半）/ `IcebergScanNode` 1228 / `IcebergTransaction` 981 / `IcebergNereidsUtils` 608 / `IcebergExternalTable` 535 / `IcebergConflictDetectionFilterUtils` 336。

---

## 阶段拆分（方案 A / 8 阶段，用户 2026-06-21 签）

> **与 master plan 的编号映射**：P6.1–P6.5 = master plan §3.7 同名（不变）；master plan 旧 P6.6（删 legacy）在此**拆成** P6.6 翻闸（新增显式）/ P6.7 删 legacy（= 旧 P6.6）/ P6.8 回归（新增显式），以兑现「单一翻闸在末」。

| 阶段 | scope | 关键 legacy → 归宿 | 新 SPI | 翻闸 |
|---|---|---|---|---|
| **P6.1** | 连接器地基 + **普通读元数据** + **7 flavor** | 测试基建 + 注入 seam（paimon `PaimonCatalogOps` 范式）；`IcebergMetadataOps`(读半) + `IcebergExternalTable`(读) + `IcebergUtils`(schema/type 部分) + `DorisTypeToIcebergType` → `IcebergConnectorMetadata`/`IcebergTypeMapping`；7 flavor（**port DLF 4-file 子树**进连接器 + **wire S3Tables SDK dep**） | — | ❌ |
| **P6.2** | **scan 路径 + MVCC + cache + vended** | `source/`(7, `IcebergScanNode` 1228) + `IcebergExternalMetaCache`(289) + `cache/`(2) + `IcebergSnapshot*` → `IcebergScanPlanProvider` + 连接器内 cache；`IcebergMvccSnapshot` → `ConnectorMvccSnapshot`；`IcebergVendedCredentialsProvider` → 新 vended SPI | **`ConnectorCredentials`**（扫描期 vended，E6） | ❌ |
| **P6.3** | **写路径**（INSERT/DELETE/UPDATE/MERGE + 事务） | **先写 `06-iceberg-write-path-rfc.md` → PMC 评审**；`IcebergTransaction`(981) + `IcebergMetadataOps`(写半) + `helper/`(3) + `IcebergConflictDetectionFilterUtils`(336) + `IcebergNereidsUtils`(608) + `transaction/IcebergTransactionManager`；planner 改用 `PhysicalConnectorTableSink`，删 `Iceberg{Delete,Merge,Table}Sink`；nereids `Iceberg{Update,Delete,Merge}Command` 走 `ConnectorWriteOps` SPI | 写路径 RFC + `ConnectorWriteOps` 扩展（`beginMerge`/`beginDelete`/`getDeleteConfig`/`getMergeConfig`） | ❌ |
| **P6.4** | **10 个 procedure action** | `action/`(11: `BaseIcebergAction` + 9 action + `IcebergExecuteActionFactory`) + `rewrite/`(6) → 连接器；`ExecuteActionCommand` 走通用 dispatch | **`ConnectorProcedureOps`**（`listProcedures`/`callProcedure`，E2；先看 Trino Iceberg connector 定形态，R3） | ❌ |
| **P6.5** | **系统表 + 元数据列** | `IcebergSysExternalTable`(177) → 复用 `PluginDrivenSysExternalTable`（E7 已就绪）；`IcebergMetadataColumn`(122) + `IcebergRowId`(68) → 元数据列 SPI | 复用 E7 sys-table hook（按需扩元数据列承载） | ❌ |
| **P6.6** | **🔻 唯一翻闸** | iceberg 入 `SPI_READY_TYPES` + 删 built-in case + `pluginCatalogTypeToEngine` 加 `iceberg→ENGINE_ICEBERG` + `PhysicalPlanTranslator` 分支收口；**GSON compat**（7 catalog flavor + db + table 全转 `registerCompatibleSubtype`→PluginDriven*）；restore SHOW PARTITIONS / SHOW CREATE TABLE parity；翻闸-scope 文档外编辑点（参 P5-T27 经验：UserAuthentication unwrap / engine 名 / 硬编码消息） | — | ✅ |
| **P6.7** | **删 legacy + 清反向 instanceof** | 删 fe-core `datasource/iceberg/`(74) + `transaction/IcebergTransactionManager` + planner sinks + 清 ~49 处反向 `instanceof IcebergExternal*`（写命令层最密）+ 删可删的 iceberg maven dep（保留 metastore-props + 共享 iceberg-aws，见开放决策 O3） | — | — |
| **P6.8** | **翻闸后 live 回归** | e2e parity（7 flavor 读 / native·JNI / position+equality delete / time-travel / INSERT·DELETE·UPDATE·MERGE / 10 action / sys-table / vended REST·DLF / Kerberos HMS），用户跑 docker（硬门） | — | — |

---

## old → new 映射（按功能区，高层；逐文件映射在各阶段 recon 时定）

| 功能区 | fe-core 旧（代表） | 新归宿 | SPI 点 | 阶段 |
|---|---|---|---|---|
| flavor 装配 | `IcebergExternalCatalogFactory` + 7 flavor catalog + `HiveCompatibleCatalog` + `IcebergDLFExternalCatalog` | `IcebergConnector.createCatalog` flavor switch（已起骨架）+ 连接器内 `dlf/` | 连接器内 | P6.1 |
| 普通读元数据 | `IcebergMetadataOps`(读) + `IcebergExternalDatabase` + `IcebergExternalTable`(读) + `IcebergUtils`(schema) | `IcebergConnectorMetadata` + `IcebergTypeMapping` + `IcebergSchemaBuilder` | E1/ConnectorMetadata | P6.1 |
| 普通读 scan | `source/IcebergScanNode`/`IcebergSource`/`IcebergSplit`/`IcebergApiSource`/`IcebergHMSSource`/`IcebergDeleteFileFilter`/`IcebergTableQueryInfo` | `IcebergScanPlanProvider` + `IcebergScanRange` + 通用 `PluginDrivenScanNode` | E3 | P6.2 |
| cache | `IcebergExternalMetaCache` + `cache/IcebergManifestCacheLoader` + `IcebergSnapshotCacheValue` | 连接器内 cache（决策点 D6，无 SPI） | 无 | P6.2 |
| MVCC | `IcebergMvccSnapshot` + `IcebergSnapshot` | `ConnectorMvccSnapshot` + 通用 `PluginDrivenMvccExternalTable`（D-042 源无关，已就绪） | E5 | P6.2 |
| vended | `IcebergVendedCredentialsProvider` | 新 `ConnectorCredentials getCredentialsForScan(session, scanRange)` | **E6（新）** | P6.2 |
| 写路径 | `IcebergTransaction` + `IcebergMetadataOps`(写) + `helper/*` + `IcebergConflictDetectionFilterUtils` + `transaction/IcebergTransactionManager` + planner `Iceberg{Delete,Merge,Table}Sink` + nereids `Iceberg{Update,Delete,Merge}Command` | `ConnectorWriteOps` 扩展 + `ConnectorTransactionFactory` + 通用 `PhysicalConnectorTableSink` | E1+写 SPI | P6.3 |
| actions | `action/*`(11) + `rewrite/*`(6) | 连接器 procedure impl；fe-core `ExecuteActionCommand` 通用 dispatch | **E2（新 `ConnectorProcedureOps`）** | P6.4 |
| sys-table | `IcebergSysExternalTable` | 通用 `PluginDrivenSysExternalTable`（已就绪）+ 连接器 E7 impl | E7 | P6.5 |
| 元数据列 | `IcebergMetadataColumn` + `IcebergRowId` | 元数据列 SPI（按需新增承载） | E7 扩 | P6.5 |
| GSON / 翻闸 | 7 catalog + db + table（GSON 壳） | `PluginDrivenExternalCatalog/Database/MvccExternalTable`（compat 注册） | GSON compat | P6.6 |
| IO plumbing | `fileio/*`(4 Delegate) + `broker/*`(3) + `profile/IcebergMetricsReporter` | 连接器内（引擎相关）；profile 参 paimon **drop**（连接器禁 import profile，登记回归） | 无 | P6.2/P6.3 随用 |

---

## 阶段内前置（不挡 P6.1，卡各自阶段）

1. **`ConnectorCredentials` SPI（E6）** —— P6.2 起前在 `fe-connector-api` 新建（扫描期 vended）。paimon 当前走另一套 `isVendedCredentialsEnabled` 网关，**非此 SPI**；P6.2 设计时核对 iceberg REST/DLF vended 与 paimon 网关能否共面。
2. **写路径 RFC `plan-doc/06-iceberg-write-path-rfc.md`** —— P6.3 **第一件事**，请 PMC 评审（master plan §7-#4 + R5）。写路径与 nereids 优化器深度耦合（`IcebergConflictDetectionFilterUtils`），RFC 须先于实现。
3. **`ConnectorProcedureOps` SPI（E2）** —— P6.4 起前新建，**先看 Trino Iceberg connector 形态再定**（R3，10 个 action 行为不齐风险）。
- 已就绪（P6.1–P6.2 够用）：`ConnectorMetadata` / `ConnectorMvccSnapshot` / `ConnectorWriteOps`(基础) / `ConnectorTransactionHandle` / `PluginDrivenSysExternalTable` / `PluginDrivenMvccExternalTable`（D-042 源无关）/ E7 sys-table hook。

---

## 验收门（per 阶段；逐项细化在各阶段 recon 时定）

- **每阶段通用门**（mirror P4/P5）：连接器 UT（无 mockito / 无 fe-core import）+ checkstyle 0 + import-gate 净（`tools/check-connector-imports.sh`）+ `dependency:tree` iceberg-core 恰一份（R-004/R-007）。**P6.1–P6.5 不改 `SPI_READY_TYPES`，零行为变更**（纯增量连接器代码 + 测试）。
- **P6.1**：7 flavor catalog 实例化正确（含 DLF/S3Tables）；`ConnectorMetadata` 读（list db/table、schema、type 映射）vs legacy `IcebergMetadataOps` 读半 parity（离线 UT）。
- **P6.2**：scan parity（谓词下推 / 分区裁剪行数 / native·JNI / position+equality delete / SELECT* 无谓词）vs `IcebergScanNode`；MVCC time-travel（AS OF / VERSION）；vended REST/DLF 凭据下发 round-trip。
- **P6.3**：RFC 经 PMC 评审通过；INSERT/DELETE/UPDATE/MERGE 写 parity + 事务提交/冲突检测；planner 改 `PhysicalConnectorTableSink` 后 EXPLAIN/执行不回归。
- **P6.4**：10 个 action（`RewriteDataFiles`/`ExpireSnapshots`/`RollbackToSnapshot`/`CherrypickSnapshot`/`FastForward`/`PublishChanges`/`RewriteManifests`/`RollbackToTimestamp`/`SetCurrentSnapshot`）经 `ConnectorProcedureOps` dispatch 行为 parity。
- **P6.5**：sys-table（`$snapshots/$files/$manifests/$history/$partitions/...`）SELECT+DESC parity；元数据列（`IcebergMetadataColumn`/`IcebergRowId`）正确。
- **P6.6**：iceberg ∈ `SPI_READY_TYPES`；built-in case 删；GSON 7 flavor + db + table 重启 replay 绿；SHOW PARTITIONS/CREATE parity（golden）；翻闸-scope 文档外编辑点全核（参 P5-T27 9-agent 分类经验）。
- **P6.7**：`grep org.apache.iceberg fe-core/src/main` 仅剩 metastore-props（O3 决定的保留集）；反向 instanceof 清零（除 backlog 保留项）；fe-core 编译 + checkstyle 0。
- **P6.8**：live e2e（用户 docker，硬门）—— 7 flavor 全能力不回归。

---

## 开放决策（待各阶段确认 / 用户签字）

- **O1（P6.1）**：DLF flavor — port legacy `dlf/`（`DLFCatalog`/`DLFTableOperations`/`DLFCachedClientPool`/`DLFClientPool` 4 文件）进 `connector.iceberg.dlf`（连接器禁 import fe-core，须自包含）。确认 vs 是否有上游 iceberg-aliyun SDK 可直接 `catalog-impl`。
- **O2（P6.2）**：vended-credentials SPI 形态 —— 新 `ConnectorCredentials` 是否与 paimon 现有 `isVendedCredentialsEnabled` 网关合面（iceberg REST/DLF 共用），还是各连接器独立。影响 metastore 子线。
- **O3（P6.7）**：iceberg maven 依赖删除集 —— `iceberg-core`/`-aws`/`-aliyun`/`s3tables` 等哪些随 legacy 删、哪些因 fe-core metastore-props 仍 import 而保留（与 paimon P5-T29 D 项同型冲突，`dependency:tree | grep iceberg` 实测敲定）。`iceberg-aws` 与既有 s3-transfer-manager 共享须留意。
- **O4（全程）**：fe-core iceberg metastore-props（`AbstractIcebergProperties`+7+factory）—— **本 P6 不删**（沿用 paimon 决策，留 fe-core 驱动翻闸后 auth/validation/`@ConnectorProperty`/type）。删除迁移属 backlog #2，与 hive/P7 共用 `MetastoreProperties` 通用 seam 一并设计。
- **O5（P6.3）**：写路径 nereids 耦合（`IcebergConflictDetectionFilterUtils` 等优化器特殊规则）能否通用 SPI 表达，或需给 `ConnectorMetadata` 暴露 hint API（R5）—— RFC 阶段定。

---

## 阶段依赖 + 节奏

```
P6.1 ──▶ P6.2 ──▶ P6.3 ──▶ P6.4 ──▶ P6.5 ──▶ P6.6 (翻闸) ──▶ P6.7 (删legacy) ──▶ P6.8 (回归)
 读元数据  scan+MVCC  写(RFC先)  actions   sys+元数据列   一次性翻闸       清理            live硬门
 +7flavor  +vended    +写SPI    +procSPI                GSON+SHOW
 +DLF/S3T  (E6)       (RFC)     (E2)                    restore
```

- **P6.1–P6.5 可严格串行**（每阶段建连接器一块 + UT，零行为变更，独立 PR、独立 commit、独立 review）。P6.2 依赖 P6.1 的 seam；P6.3 依赖 P6.2 的 scan/MVCC（写需读快照）；P6.6 翻闸**必须**等 P6.1–P6.5 全完成（全有或全无）。
- **每阶段 = 一个 AGENT-PLAYBOOK 单元**：开场读 PROGRESS/HANDOFF/本 doc 对应阶段块 → code-grounded recon + 逐 task 拆解 → 实现 + UT → 文档同步（§5.1 五步）→ commit + handoff。**大文件（`IcebergUtils` 1826 等）用 subagent 总结（§3.1），勿主线整读。**
- **PR 标题**：`[refactor](catalog) P6.x iceberg: <subj>`（mirror #64653/#64655 已合入风格）；或内部 task `[P6-Tnn]`（§5.4）。Task ID 在各阶段启动时分配，永不复用（§5.3）。

---

## 给下一个 agent 的 meta

- **P6.1 起步无硬前置**，直接进 P6.1 recon。**先读** master plan §3.7 全文 + 本 doc P6.1 块 + 对照 legacy `datasource/iceberg/` flavor + metadata 读路径真实代码。
- **翻闸全有或全无** —— 切忌在 P6.1–P6.5 任何阶段把 iceberg 加进 `SPI_READY_TYPES`（会立刻让未实现的 scan/write 全断）。翻闸只在 P6.6。
- **删除前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（参 P5-T29 scope ledger 教训：naive `rm -rf` 断编译；metastore-props 是 STILL-CONSUMED）。
- **写路径 RFC 须先于 P6.3 实现**（PMC 评审有外部阻塞，P6.3 第一件事即启动 RFC 让评审并行）。
- **连接器禁 import fe-core**：DLF 子树、profile、IO plumbing 均须自包含或 drop（参 paimon profile drop 先例）。
- **元存储子线**细节见 [`metastore-storage-refactor/HANDOFF.md`](../metastore-storage-refactor/HANDOFF.md)。

---

## P6.1 逐 task 拆解（P6-Tnn）—— 2026-06-21 code-grounded recon 产出

> recon 详情 + 已验断言 + 跨切面风险见 [`research/p6.1-iceberg-metadata-recon.md`](../research/p6.1-iceberg-metadata-recon.md)。
> **关键认知**：连接器 6-file 骨架是「编译通过但误导性的近-no-op」——真正的 per-flavor catalog 装配在 metastore-props（`AbstractIcebergProperties.initializeCatalog:133`，非 `datasource/iceberg/*`），骨架把它坍缩成裸类名 switch，且含 4 个 silent 读路径 bug（mapping-flag key 下划线-vs-点分恒 false / `TIMESTAMPTZV2` 名 vs converter 只认 `TIMESTAMPTZ` / nullable·isKey·小写·WITH_TZ marker / format-version 算错）。模板 = paimon（`PaimonCatalogFactory` 纯静态 + `PaimonCatalogOps` 可注入 seam + `Recording*`/`Fake*` 测试基建）。
> **顺序原则**：seam + 测试基建先（让下游 offline 可测）→ pom 依赖闭包 → 5 个 CatalogUtil flavor → s3tables（bespoke）→ DLF port → 读元数据 rewire + type-mapping parity 修。**全程不碰 `SPI_READY_TYPES`，零行为变更**（对齐 legacy 行为，非对齐骨架）。
> **新建连接器类清单**（mirror paimon）：`IcebergCatalogFactory`（纯静态）/ `IcebergCatalogOps`（interface + nested `CatalogBackedIcebergCatalogOps`）/ ported `HiveCompatibleCatalog` + `dlf/{DLFCatalog,DLFTableOperations,client/DLFClientPool,client/DLFCachedClientPool}` + vendored `ProxyMetaStoreClient`；test：`RecordingIcebergCatalogOps` / `FakeIcebergTable` / `RecordingConnectorContext`(copy) / `IcebergCatalogFactoryTest` / `IcebergConnectorMetadataTest` / `IcebergTypeMappingReadTest` / `IcebergConnectorValidatePropertiesTest` / `IcebergLiveConnectivityTest`(env-gated)。

| ID | 标题 | 依赖 | 估 | 状态 |
|---|---|---|---|---|
| **P6-T01** | 抽纯 `IcebergCatalogFactory` + 引入 `IcebergCatalogOps` seam（纯结构倒置，无行为变更、不加新 flavor）| — | M | ✅ 2026-06-21（commit `ae54a2174ff`）|
| **P6-T02** | 建 offline 测试基建（`RecordingIcebergCatalogOps`/`FakeIcebergTable`/`RecordingConnectorContext`/`IcebergCatalogFactoryTest`）| T01 | M | ✅ 2026-06-21（commit `ae54a2174ff`）|
| **P6-T03** | `IcebergConnectorMetadata` rewire 到 seam + `IcebergConnectorMetadataTest`（behavior frozen）| T02 | M | ✅ 2026-06-21（commit `ae54a2174ff`）|
| **P6-T04** | 补 7-flavor SDK 依赖闭包到连接器 pom（HMS/glue/s3tables/sts/aliyun-DLF）+ `fe/pom.xml` dM 条目 | T01 | M | ⬜ |
| **P6-T05** | port 5 个 CatalogUtil flavor（REST/HMS/GLUE/HADOOP/JDBC）完整 per-flavor 属性装配（含 manifest-cache + warehouse + JDBC DriverShim + `iceberg.jdbc.catalog_name`）| T02,T04 | L | ⬜ |
| **P6-T06** | port S3TABLES flavor 的 bespoke（非 CatalogUtil）`new S3TablesCatalog().initialize(name,props,client)` 路径 | T04,T05 | M | ⬜ |
| **P6-T07** | port DLF 子树（`DLFCatalog`+`HiveCompatibleCatalog`+`DLFTableOperations`+2 client pool + vendored `ProxyMetaStoreClient`）+ 断 3 fe-core import + wire dlf flavor（复用 `DlfMetaStorePropertiesImpl.toDlfCatalogConf`）| T04,T05 | L | ⬜ |
| **P6-T08** | 修 Iceberg→Doris type-mapping 读 parity（`TIMESTAMPTZ` 名 + 点分 mapping-flag key）+ `IcebergTypeMappingReadTest` | T02 | M | ⬜ |
| **P6-T09** | 修 `parseSchema` column 构造 parity（小写 / nullable=true / isKey=true / WITH_TIMEZONE marker / format-version）+ nested-namespace + view 过滤 | T03,T08 | L | ⬜ |
| **P6-T10** | `IcebergConnector` 创建走 `executeAuthenticated` + thread-context-CL pin；`IcebergConnectorProvider.validateProperties`；env-gated `IcebergLiveConnectivityTest` | T05,T06,T07,T09 | M | ⬜ |

**通用验收门（每 task）**：连接器 UT 绿（无 Mockito，fail-loud fake）+ checkstyle 0 + `tools/check-connector-imports.sh` 净 + **断言 assembled prop map / Hadoop conf / column flag vs legacy 期望值**（不能只断类名——parity-by-omission 风险）+ `grep` 确认 iceberg **不在** `SPI_READY_TYPES`。

**待用户签字（实现 T04–T07 前）**：
- **Q1 DLF scope**：P6.1 是否 port-now read-only（write 方法 dormant、`IcebergDLFExternalCatalog` DDL-policy 留 fe-core）vs 整 flavor 推迟。
- **Q2 metastore-binding**：metastore-spi 仅复用 HMS HiveConf + DLF conf，REST/glue/jdbc/s3tables 在连接器内 re-derive（vs 扩 metastore-spi）。影响 metastore 子线。
- （已采推荐默认，未单列问）s3tables/glue dep → 加 `fe/pom.xml` dM；结构 seam（executeAuthenticated+CL-pin）P6.1 即纳入（虽 P6.6 docker 前不可 live-test）。

> **T01/T02/T03/T08 与上述决策无关**（纯结构倒置 + 测试基建 + type-mapping 修），可在签字前先行实现。T04–T07/T09/T10 待 Q1/Q2。

### P6-T01..T03 实现记录（2026-06-21，✅ 已实现 + 验证，commit `ae54a2174ff`）

- **新建**：`IcebergCatalogFactory`（纯静态 `resolveFlavor`/`resolveCatalogImpl`，verbatim lift 自 `IcebergConnector`）+ `IcebergCatalogOps`（interface + nested `CatalogBackedIcebergCatalogOps`，read 子集 `listDatabaseNames/databaseExists/listTableNames/tableExists/loadTable/close`，`SupportsNamespaces` 分支内化）。
- **rewire**：`IcebergConnector.getMetadata` 注入 `new CatalogBackedIcebergCatalogOps(catalog)`；`createCatalog` 用 `IcebergCatalogFactory.resolveCatalogImpl`（删私有副本）；`IcebergConnectorMetadata` ctor 改吃 `IcebergCatalogOps`，5 read 方法走 seam（behavior frozen，parseSchema 原样保留待 T08/T09）。
- **测试基建**（连接器首批测试，src/test 从无到有）：`RecordingIcebergCatalogOps`（手写 no-Mockito fake + call log）/ `FakeIcebergTable`（fail-loud，仅 schema/spec/location/properties，余 30 法 throw）/ `RecordingConnectorContext`（copy paimon）/ `IcebergCatalogFactoryTest`（13）/ `IcebergConnectorMetadataTest`（14）。
- **验证**：`mvn -pl :fe-connector-iceberg -am test`（cache off）BUILD SUCCESS，surefire 实证 **27 run / 0 fail / 0 error / 0 skip**；checkstyle 0；import-gate 净。
- **🔴 测试独立确证 2 个 T08/T09 parity bug（已 pin frozen，NOTE 标注，待修）**：① `format-version` = `spec().specId()>=0?2:1` **恒 stamp "2"**（含 unpartitioned，specId==0），应读 table `format-version` 元数据（T09）；② mapping-flag 仍读**下划线** key `enable_mapping_varbinary`/`enable_mapping_timestamp_tz`（`IcebergConnectorMetadataTest` 喂下划线 key 故测绿；生产 catalog map 携**点分** key→恒 false，T08 改常量为点分后须同步改该测）；另 `TIMESTAMPTZV2` 名（converter 只认 `TIMESTAMPTZ`，T08）。
