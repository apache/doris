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
| **P6.2** | **scan 路径 + MVCC + cache + vended** | `source/`(7, `IcebergScanNode` 1228) + `IcebergExternalMetaCache`(289) + `cache/`(2) + `IcebergSnapshot*` → `IcebergScanPlanProvider` + 连接器内 cache（D6）；`IcebergMvccSnapshot` → `ConnectorMvccSnapshot`；`IcebergVendedCredentialsProvider` → 连接器 `extractVendedToken` + 复用既有 `ConnectorContext` 接缝 | **0 新 SPI**（recon 更正：E6 取消，复用 `ConnectorContext.vendStorageCredentials`） | ❌ |
| **P6.3** | **写路径**（INSERT/DELETE/UPDATE/MERGE + 事务） | **先写 `06-iceberg-write-path-rfc.md` → PMC 评审**；`IcebergTransaction`(981) + `IcebergMetadataOps`(写半) + `helper/`(3) + `IcebergConflictDetectionFilterUtils`(336) + `IcebergNereidsUtils`(608) + `transaction/IcebergTransactionManager`；planner 改用 `PhysicalConnectorTableSink`，删 `Iceberg{Delete,Merge,Table}Sink`；nereids `Iceberg{Update,Delete,Merge}Command` 走通用 `RowLevelDmlCommand` 壳 + capability 派发（iceberg plan 合成留 fe-core，DV-04x） | **✅ RFC 评审通过**（`06-iceberg-write-path-rfc.md`，commit `a49720820f9`）：框架**全面统一**（删 `usesConnectorTransaction`/insert-handle/dead delete-merge handle/config-bag 三件套）+ 新 `ConnectorTransaction.applyWriteConstraint`（O5-2 default-no-op）+ `ConnectorWriteHandle.writeOperation`；逐 task 见 §P6.3 拆解 | ❌ |
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
| vended | `IcebergVendedCredentialsProvider` | 连接器 `extractVendedToken(table)`（REST，自包含移植）+ 复用既有 `ConnectorContext.vendStorageCredentials`/`normalizeStorageUri(uri,token)`（发 `location.*`） | **无新 SPI**（recon 更正 E6） | P6.2 |
| 写路径 | `IcebergTransaction` + `IcebergMetadataOps`(写) + `helper/*` + `IcebergConflictDetectionFilterUtils` + `transaction/IcebergTransactionManager` + planner `Iceberg{Delete,Merge,Table}Sink` + nereids `Iceberg{Update,Delete,Merge}Command` | `ConnectorWriteOps` 扩展 + `ConnectorTransactionFactory` + 通用 `PhysicalConnectorTableSink` | E1+写 SPI | P6.3 |
| actions | `action/*`(11) + `rewrite/*`(6) | 连接器 procedure impl；fe-core `ExecuteActionCommand` 通用 dispatch | **E2（新 `ConnectorProcedureOps`）** | P6.4 |
| sys-table | `IcebergSysExternalTable` | 通用 `PluginDrivenSysExternalTable`（已就绪）+ 连接器 E7 impl | E7 | P6.5 |
| 元数据列 | `IcebergMetadataColumn` + `IcebergRowId` | 元数据列 SPI（按需新增承载） | E7 扩 | P6.5 |
| GSON / 翻闸 | 7 catalog + db + table（GSON 壳） | `PluginDrivenExternalCatalog/Database/MvccExternalTable`（compat 注册） | GSON compat | P6.6 |
| IO plumbing | `fileio/*`(4 Delegate) + `broker/*`(3) + `profile/IcebergMetricsReporter` | 连接器内（引擎相关）；profile 参 paimon **drop**（连接器禁 import profile，登记回归） | 无 | P6.2/P6.3 随用 |

---

## 阶段内前置（不挡 P6.1，卡各自阶段）

1. ~~**`ConnectorCredentials` SPI（E6）**~~ **【2026-06-22 recon 更正：取消】** —— code-grounded 复核证 paimon vended **未用独立 SPI**，而是复用既有 `ConnectorContext.vendStorageCredentials(rawToken)` + `normalizeStorageUri(uri,token)`（`DefaultConnectorContext` 实现，引擎中立）。iceberg token 形态同类（raw cloud props）→ **直接复用，零新 SPI**。连接器只写 iceberg SDK 的 `extractVendedToken(table)`。详见 [`../research/p6.2-iceberg-scan-recon.md`](../research/p6.2-iceberg-scan-recon.md) §5。
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
- **O2（P6.2）✅ 已决（2026-06-22，用户签字）**：vended-credentials **复用既有 `ConnectorContext.vendStorageCredentials`/`normalizeStorageUri(uri,token)` 接缝，不新建 SPI**（原 E6 取消）。paimon 实证无独立 `ConnectorCredentials` SPI；iceberg token 同类直接复用。仅 REST flavor；DLF 凭据走 HiveConf（catalog-bind，T07/T10 已接）。详见 recon §5。同样确认：**D6**=cache 全连接器内部（镜像 paimon）；**field-id**=字符串属性（不改 `ConnectorColumn`）；**P6.2 净 0 个新 SPI 接口、0 处 SPI 破坏**（delete equality 元数据编码进既有 `ConnectorDeleteFile.properties`；cache 失效用既有 `ConnectorMetaInvalidator`/`Connector.invalidate*`）。
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
| **P6-T04** | 补 7-flavor 依赖闭包到连接器 pom（HMS/DLF=`hive-catalog-shade` · glue/sts/s3tables=AWS SDK v2 child-first）+ `fe-connector-metastore-spi` + `fe/pom.xml` dM + plugin-zip thrift 排除 | T01 | M | ✅ 2026-06-22（[D-060]）|
| **P6-T05** | port 5 个 CatalogUtil flavor（REST/HMS/GLUE/HADOOP/JDBC）完整 per-flavor 属性装配（含 manifest-cache + warehouse + JDBC DriverShim + `iceberg.jdbc.catalog_name`）| T02,T04 | L | ⬜ |
| **P6-T06** | port S3TABLES flavor 的 bespoke（非 CatalogUtil）`new S3TablesCatalog().initialize(name,props,client)` 路径 | T04,T05 | M | ⬜ |
| **P6-T07** | port DLF 子树（`DLFCatalog`+`HiveCompatibleCatalog`+`DLFTableOperations`+2 client pool + vendored `ProxyMetaStoreClient`）+ 断 3 fe-core import + wire dlf flavor（复用 `DlfMetaStorePropertiesImpl.toDlfCatalogConf`）| T04,T05 | L | ⬜ |
| **P6-T08** | 修 Iceberg→Doris type-mapping 读 parity（`TIMESTAMPTZ` 名 + 点分 mapping-flag key + **BINARY 无界长度**）+ `IcebergTypeMappingReadTest` | T02 | M | ✅ 2026-06-21 |
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

### P6-T08 实现记录（2026-06-21，✅ 已实现 + 验证 + TDD RED→GREEN）

- **scope**：HANDOFF 列 2 个 bug（TIMESTAMPTZ 名 + 点分 key）；实现中**断长度而非只断类名**（通用验收门「不能只断类名——parity-by-omission 风险」）→ **额外发现第 3 个 type-mapping parity 偏差**并一并修（见下 BINARY）。
- **修 1（TIMESTAMPTZ 名）**：`IcebergTypeMapping:116` `TIMESTAMPTZV2`→`TIMESTAMPTZ`（保 scale 6）。`ConnectorColumnConverter:215` 只有 `TIMESTAMPTZ` case（→`createTimeStampTzType(precision)`），无 `TIMESTAMPTZV2`→旧名静默 UNSUPPORTED。legacy `IcebergUtils:605` = `createTimeStampTzType(ICEBERG_DATETIME_SCALE_MS=6)`，parity ✓。
- **修 2（点分 mapping-flag key）**：`IcebergConnectorProperties:46-47` `enable_mapping_varbinary`/`enable_mapping_timestamp_tz`（下划线）→ `enable.mapping.varbinary`/`enable.mapping.timestamp_tz`（点分，= `CatalogProperty:50/52` + paimon `PaimonConnectorProperties:47/55`）。真实 catalog map 携点分 key→旧下划线常量恒读 default-false→静默丢 BINARY→VARBINARY / TS_TZ→TIMESTAMPTZ 映射。
- **修 3（BINARY 无界长度，本 task 新发现）**：`IcebergTypeMapping` BINARY+flag `ConnectorType.of("VARBINARY", 65535, 0)`→`ConnectorType.of("VARBINARY")`（无显式长度，precision=-1）。legacy `IcebergUtils:592` = `createVarbinaryType(VarBinaryType.MAX_VARBINARY_LENGTH == ScalarType.MAX_VARBINARY_LENGTH == 0x7fffffff)`。connector emit precision=-1→converter `case "VARBINARY"` 落 else 分支 `createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH)`=**与 legacy 同一常量、byte-identical**；旧 65535 会令 DESCRIBE/SHOW CREATE 渲染异于 legacy。**关键耦合**：修 2（点分 key）令此 BINARY 路径在生产**首次真激活**（旧下划线 key 下 enableMappingVarbinary 恒 false，该支从不触发）→ 不一并修＝翻闸后 flip-on 回归。UUID(16)/FIXED(len) 长度本就 parity，仅 BINARY（无界）偏。
- **测试**：新 `IcebergTypeMappingReadTest`（9，镜像 `PaimonTypeMappingReadTest` 但覆盖全 primitive + 双 flag on/off + nested array/map/struct 递归 + flag 透传到叶；断 typeName **及** precision/scale vs legacy）。同步改 `IcebergConnectorMetadataTest` 2 个 mapping-flag 测：`getTableSchemaHonorsVarbinaryAndTimestampTzMappingFlags` 喂**字面点分 key**（非常量，钉死 wire 拼写）+ 断 `TIMESTAMPTZ`；`getTableSchemaDefaultsMappingFlagsOff` 注释同步。**format-version 测（`getTableSchemaStampsFormatVersionTwoForAnyValidSpec`）留 T09 不动**。
- **TDD**：3 修各 RED（`expected TIMESTAMPTZ but TIMESTAMPTZV2` / `VARBINARY but STRING`〔点分 key vs 下划线常量〕/ `expected -1 but 65535`）→ GREEN。
- **验证**：`mvn -pl :fe-connector-iceberg -am test`（cache off）BUILD SUCCESS，**iceberg 模块 36 run / 0F / 0E / 0skip**（Factory 13 + Metadata 14 + TypeMappingRead 9）；checkstyle 0；import-gate exit 0；`SPI_READY_TYPES` 仍 = {jdbc,es,trino-connector,max_compute,paimon}（iceberg 缺席，零行为变更）。**已 commit `d41fa4faf3e`**（2026-06-22；T08 4 改 1 新 + HANDOFF/PROGRESS/tasks doc）。docker e2e 未跑（翻闸在 P6.6）。

### P6-T04 实现记录（2026-06-22，✅ 已实现 + 验证 + plugin-zip 实查）

- **关键更正（修 D-059/HANDOFF 误述）**：2-agent code-grounded recon（unzip 实证）证 ① repo **无** `iceberg-hive-metastore` artifact——`org.apache.iceberg.hive.HiveCatalog`（hms）+ 反射加载的 `com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient`（dlf）均**捆在** `org.apache.doris:hive-catalog-shade`（127MB fat jar，thrift relocate `shade.doris.hive.org.apache.thrift`，内含 iceberg 1.10.1 + aliyun DLF SDK 2266 类）；② 无独立 aliyun DLF SDK 可加。HANDOFF/D-059 的「加 iceberg-hive-metastore + aliyun DLF SDK」均不成立。
- **决策 [D-060]（用户签字 2026-06-22）**：HMS/DLF 闭包在「复用 `hive-catalog-shade`（合 convention，fe-connector-hive/hms 同款）」vs「专建精简 iceberg-hive-shade（仿 paimon-hive-shade）」vs「推迟」中 → **复用 hive-catalog-shade**。
- **实改**：① `fe-connector-iceberg/pom.xml` + `fe-connector-metastore-spi`（Q2=B：T05/T07 复用 `HmsMetaStorePropertiesImpl.toHiveConfOverrides` / `DlfMetaStorePropertiesImpl.toDlfCatalogConf`）+ `hive-catalog-shade`（hms/dlf 的 HiveCatalog + ProxyMetaStoreClient，managed `${doris.hive.catalog.shade.version}`=3.1.1）+ AWS SDK v2 child-first 集（s3 · glue〔排 apache-client〕· sts〔排 apache-client〕· s3tables · s3-transfer-manager · sdk-core · aws-json-protocol · protocol-core · url-connection-client，BOM `${awssdk.version}`=2.29.52）+ `software.amazon.s3tables:s3-tables-catalog-for-iceberg`（s3tables flavor 的 `S3TablesCatalog`）。② `fe/pom.xml` dM + `s3-tables-catalog-for-iceberg`（`${s3tables.catalog.version}`=0.1.4，原仅 fe-core 内联）。③ `plugin-zip.xml` + `org.apache.doris:fe-thrift` / `org.apache.thrift:libthrift` 排除（仿 fe-connector-hive；hive-catalog-shade 自带 relocated thrift，原包 thrift 须挡在 plugin 外）。**无 Java 改**（flavor catalog-impl 由 `CatalogUtil` 按类名反射加载，故 T04 纯运行时闭包；连接器现码不直引这些类）。
- **验证（pom-only，编译+打包级；live 真闸在 P6.6）**：`mvn -pl :fe-connector-iceberg -am test` 36/0/0/0 + checkstyle 0 + import-gate 0 + `dependency:tree` iceberg-core **恰 1**（1.10.1）+ metastore-spi→fe-kerberos 链在 + AWS SDK 全 2.29.52 + s3tables-catalog 0.1.4；**`mvn -am package` 装出 plugin-zip 实查**（143 jar）：全 AWS SDK module 在（glue/sts/s3/s3tables/s3-transfer-manager/sdk-core/...）、全 `iceberg-*.jar` 皆 **1.10.1 无 skew**、`libthrift` **缺席**（排除生效）、hadoop **仅 3.4.2**（无 skew jar）；`SPI_READY_TYPES` iceberg 仍缺席（零行为变更）。
- **残留风险（UT/打包不可见，→P6.6 docker plugin-zip e2e 真闸）**：① hive-catalog-shade **内含** iceberg 1.10.1 与直接 iceberg-core 在 child-first loader 共存——版本相同→预期 byte-identical benign（fe-connector-hive 同款已上线），但未 live 验；② `apache-client` 经 awssdk 传递 runtime 入闭包（paimon 同款故意 ship，无害）；③ **glue 显式-AK 凭据 provider 类 `com.amazonaws.glue.catalog.credentials.*` 来源未定**（不在 hive-catalog-shade / iceberg-aws；fe-core `aws-java-sdk-glue` v1 疑源但未证）→ **T05 glue flavor wiring 时核**（不挡 T04 闭包）。
- **遗留待清（非 T04）**：worktree 有 `phase3-module-split` 分支遗留的 stale 生成物（`fe-connector-iceberg-backend-*` / `-api` 目录仅含 gitignored `.flattened-pom.xml`，2026-04，不在 reactor、0 tracked 文件，无害）。

---

## P6.2 逐 task 拆解（P6.2-Tnn）—— 2026-06-22 code-grounded recon 产出

> recon 详情 + 风险 + old→new 映射见 [`../research/p6.2-iceberg-scan-recon.md`](../research/p6.2-iceberg-scan-recon.md)（workflow `wf_a74302c7-194`，7 路并行 + 主线直读 paimon vended 链/`ConnectorContext`/`ConnectorDeleteFile`/`ConnectorMetaInvalidator`）。
> **用户裁定（2026-06-22 全签字）**：D6=cache 全连接器内部（镜像 paimon）；field-id=字符串属性（不改 `ConnectorColumn`）；O2=vended 复用既有 `ConnectorContext` 接缝（**E6 取消**）。
> **关键结论**：**P6.2 净 0 个新 SPI 接口、0 处 SPI 破坏**——scan/MVCC 接缝就绪，delete equality 元数据编码进既有 `ConnectorDeleteFile.properties`，field-id/vended 走既有接缝，cache 失效用既有 `ConnectorMetaInvalidator`/`Connector.invalidate*`。
> **顺序原则**（镜像 paimon proven sequence）：scan provider 骨架 + 测试基建 → 谓词/split/params → delete → COUNT/batch → **field-id 字典** → MVCC → cache（连接器内部）→ vended（复用接缝）→ parity UT → 设计文档/handoff。**全程不碰 `SPI_READY_TYPES`，零行为变更**（对齐 legacy，离线 UT 验；翻闸前连接器 scan 代码运行时不触发）。
> **新建连接器类**（mirror paimon）：`IcebergScanPlanProvider` / `IcebergScanRange` / `IcebergLatestSnapshotCache` / `IcebergSchemaAtMemo` / `IcebergManifestCache`（+ loader/value 移植）/ 连接器版 `extractVendedToken` / `IcebergTableHandle` scan-option 扩展 / `IcebergConnectorMetadata` MVCC 方法；测试 `FakeIcebergTable` 扩 scan 能力 + `IcebergScanPlanProviderTest` 等。

| ID | 标题 | 依赖 | 估 | 状态 |
|---|---|---|---|---|
| **P6.2-T01** | `IcebergScanPlanProvider` 骨架（implements `ConnectorScanPlanProvider`）+ `IcebergScanRange`（implements `ConnectorScanRange`）+ `IcebergConnector.getScanPlanProvider` 接线 + `ignorePartitionPruneShortCircuit()=true` + 测试基建扩（`FakeIcebergTable` scan 能力 / `IcebergScanPlanProviderTest`）。镜像 `PaimonScanPlanProvider`/`PaimonScanRange` | — | M | ✅ |
| **P6.2-T02** | 谓词下推（自包含移植 `convertToIcebergExpr`，不 import fe-core）+ `createTableScan`（filter add 顺序保真）+ `planFileScanTask` split 枚举（targetSplitSize/batch 阈值）；manifest-cache 集成留 T08 | P6.2-T01 | L | ✅ |
| **P6.2-T03** | `FileScanTask`→`IcebergScanRange` + `populateRangeParams`→`TTableFormatFileDesc.icebergParams`（format-version / partition-data-json / first-row-id / last-updated-seq-num v3 / identity 分区列→columns-from-path）+ native vs JNI 文件格式判定 + **`path_partition_keys` 必发**（CI #968880 双填 guard） | P6.2-T02 | L | ✅ |
| **P6.2-T04** | delete files（position bounds / equality field-ids / PUFFIN deletion-vector offset+length）：`task.deletes()` 关联 + 类型/field-ids/bounds 编码（**实际走类型化 `IcebergScanRange.DeleteFile` carrier + `populateRangeParams`，非 `ConnectorDeleteFile.properties`**——设计 §2 超越 recon §2）→ `TIcebergDeleteFileDesc`；`getDeleteFiles` EXPLAIN read-back。**+ 跟修数据路径归一化**（对抗 review 抓的 T02/T03 gap，独立 commit） | P6.2-T03 | L | ✅ |
| **P6.2-T05** | COUNT 下推（`getCountFromSnapshot`：equality-delete 不可下推 / position 处理 / `ignoreIcebergDanglingDelete`）→ 塌缩单 count range（镜像 paimon，丢 >10000 并行切片 trim）+ `pushDownRowCount`→`table_level_row_count`。**batch mode 延后**（用户签字；manifest-计数 vs 通用节点分区-计数轴不匹配，需新 SPI 接缝违反「0 新 SPI」） | P6.2-T03 | M | ✅ |
| **P6.2-T06** | **field-id 字典（最高危）**：`getScanNodeProperties` 用 iceberg `Schema` 构建 `history_schema_info`（`-1`/current 按请求列名 + 历史枚举全 schema-id + name-mapping），`populateScanLevelParams` 落 `TFileScanRangeParams`（镜像 paimon `FIX-SCHEMA-EVOLUTION`，不改 `ConnectorColumn`）；UT 喂多 schema-id/重命名表断字典完整 | P6.2-T03 | L | ✅ |
| **P6.2-T07** | MVCC：`IcebergConnectorMetadata.{resolveTimeTravel(5 kinds: SNAPSHOT_ID/TIMESTAMP/TAG/BRANCH/INCREMENTAL),applySnapshot,getTableSchema(@snapshot),beginQuerySnapshot}` + `IcebergTableHandle` scan-option 键 + timestamp TZ aliases（自包含）+ 接线 `PluginDrivenMvccExternalTable`/`applyMvccSnapshotPin`（通用已就绪） | P6.2-T02 | L | ✅ |
| **P6.2-T08** | cache（D6 连接器内部）：`IcebergLatestSnapshotCache`（TTL，镜像 `PaimonLatestSnapshotCache`，值=`(snapshotId,schemaId)` 二元组）+ `IcebergManifestCache`（path-keyed，移植 loader/value）+ **manifest 级 planning**（移植 legacy `planFileScanTaskWithManifestCache` + vendoring `org.apache.iceberg.DeleteFileIndex`，gate `meta.cache.iceberg.manifest.enable` 默认 off）+ `IcebergConnector` override `invalidateTable`/`invalidateAll`（**不清 manifest**，靠连接器重建清）。**`IcebergSchemaAtMemo` 跳过**（用户裁定：iceberg 历史 schema 内存即得，memo 零 I/O 收益）。**用户裁定扩 scope 走 manifest 级让 cache 真消费**（默认 SDK splitFiles 不变） | P6.2-T06,P6.2-T07 | L | ✅ |
| **P6.2-T09** | vended（O2 复用接缝，仅 REST）：连接器 `extractVendedToken(table)`（`table.io().properties()`+`SupportsStorageCredentials`，自包含移植 `IcebergVendedCredentialsProvider`）→ 复用 `context.vendStorageCredentials`/`normalizeStorageUri(uri,token)` → 发 `location.*`；gate `iceberg.rest.vended-credentials-enabled`/FileIO 能力 | P6.2-T03 | M | ✅ |
| **P6.2-T10** | parity UT 套件（vs legacy 期望值，非只断类名）：谓词下推全形 / 分区裁剪 / delete（position+equality+DV）/ COUNT 下推 / batch / format-version 边界 / field-id 字典 / `path_partition_keys` / native·JNI / MVCC time-travel / vended REST round-trip。**审计 workflow `wf_9d88fe61-5c7`（10 维，10 确认+2 驳回）+ 8 缺口补测；UT 270→278；详见 `designs/P6-T10-iceberg-scan-parity-audit-design.md`** | P6.2-T04..T09 | L | ✅ |
| **P6.2-T11** | 汇总设计文档 `designs/P6-T11-iceberg-scan-summary-design.md` + HANDOFF + 连接器 validation gate 核对（已接线，UT 7/0）；UT-不可见 deviation 中央注册（[deviations-log](../deviations-log.md) **DV-038** 翻闸阻塞 BLOCKER〔GLOBAL_ROWID + getColumnHandles 2 面〕/ **DV-039** parity-忠实 HIGH-MEDIUM / **DV-040** perf-cosmetic 批）待 P6.6 docker。审计 workflow `wf_edde7eac-a5b`（9 reader + critic）。**T11 完成 = P6.2 DONE** | P6.2-T10 | S | ✅ |

**通用验收门（每 task）**：连接器 UT 绿（无 Mockito，fail-loud fake）+ checkstyle 0 + `tools/check-connector-imports.sh` 净 + **断 assembled 属性/Thrift 参数/字典 vs legacy 期望值**（parity-by-omission 风险）+ `grep` 确认 iceberg **不在** `SPI_READY_TYPES`。**P6.2 验收门（line 90）**：scan parity（谓词下推 / 分区裁剪行数 / native·JNI / position+equality delete / SELECT* 无谓词）vs `IcebergScanNode`；MVCC time-travel（AS OF / VERSION）；vended REST round-trip。

> **系统表 scan 不在 P6.2**（migration 表把 sys-table 放 **P6.5** `PluginDrivenSysExternalTable` E7）；P6.2 scan provider 只做普通表。`fileio/*`(4 Delegate) 留 catalog 层；`broker/*`=dead；`profile/IcebergMetricsReporter`=**drop**（参 paimon）。

---

## P6.3 逐 task 拆解（P6.3-Tnn）—— 2026-06-23 RFC §11 产出（**RFC ✅ 评审通过**）

> RFC = [`../06-iceberg-write-path-rfc.md`](../06-iceberg-write-path-rfc.md)；recon = [`../research/p6.3-iceberg-write-recon.md`](../research/p6.3-iceberg-write-recon.md)；Trino 北极星 + 原始 workflow 产出 = `.audit-scratch/p6.3-research/`。commit `a49720820f9`（未 push）。
> **用户/PMC 裁定（RFC §4/§6）**：**Q2** 全面统一写框架（单 `ConnectorTransaction` 模型；删 `usesConnectorTransaction()` fork + `ConnectorInsertHandle`/insert-handle 方法 + dead delete/merge handle 面 + config-bag 三件套；jdbc 退化 no-op txn；plan-provider-only sink；capability 派发无 `instanceof`；改 jdbc/maxcompute 配字节 parity）；**Q1** Route B / option (i)（通用 `RowLevelDmlCommand` 壳 + iceberg plan 合成暂留 fe-core，**有界 deviation DV-04x**，保 EXPLAIN parity；拒 (ii) 新 nereids-spi 模块）；**Q3** O5-2（`applyWriteConstraint` default-no-op，复用 P6.2-T02 `IcebergPredicateConverter`）。**OQ-1** jdbc thrift 移入连接器；**OQ-2** 删 config-bag 三件套；**OQ-3** EXPLAIN sink-标签 diff 非回归。**北极星 = Trino 式 (iii) 通用化**（连接器 0 优化器 import、引擎核心全 DML 合成）→ 后续专门 RFC，演进触发 = hive P7/paimon 第二行级-DML 消费者。
> **顺序原则**：框架统一收口（T01）→ jdbc/maxcompute adopter parity（T02）→ `IcebergConnectorTransaction` 骨架→op→commit+O5（T03–T05）→ sink 统一（T06）→ 通用命令壳（T07）→ parity 审计+deviation（T08）→ 收口（T09）。**全程不碰 `SPI_READY_TYPES`，behind gate 零行为变更直到 P6.6**。
> **新建/改动类**：`ConnectorTransaction.applyWriteConstraint`(+`profileLabel`) / `ConnectorWriteHandle.writeOperation`（SPI 新增）；删 `ConnectorInsertHandle`/`usesConnectorTransaction`/config-bag 三件套 + planner `Iceberg{Table,Delete,Merge}Sink`（删）；`IcebergConnectorTransaction` + `IcebergWriterHelper` 等价物 + 连接器 `planWrite` 建 3 thrift sink（连接器新建）；通用 `RowLevelDmlCommand` + 连接器-键控 plan-变换注册表（fe-core 新建）。
> **大文件 subagent 总结（§3.1）**：`IcebergTransaction`(981) / `IcebergMetadataOps`(写半,1362) / `IcebergMergeCommand` / `IcebergNereidsUtils`(608) / `IcebergConflictDetectionFilterUtils`(336)。

| ID | 标题 | 依赖 | 估 | 状态 |
|---|---|---|---|---|
| **P6.3-T01** | **框架统一·SPI 收口**：删 `usesConnectorTransaction()` + `ConnectorInsertHandle`/`beginInsert·finishInsert·abortInsert` + dead `begin/finish/abortDelete·Merge` handle 面 + `ConnectorDeleteHandle`/`ConnectorMergeHandle`；`beginTransaction` mandatory + 退化 no-op 默认；新增 `ConnectorWriteHandle.writeOperation`(INSERT/OVERWRITE/DELETE/UPDATE/MERGE 枚举) + `ConnectorTransaction.profileLabel()`(default)；`PluginDrivenInsertExecutor` 删 `beforeExec`/`doBeforeCommit` 双臂 fork + `transactionType()` 硬编 enum（→ SPI profileLabel）。**改 maxcompute**（去 `usesConnectorTransaction` override） | — | M | ✅ 2026-06-23（option B；见下实现记录） |
| **P6.3-T02** | **jdbc 退化 adopter**：jdbc→`JdbcNoOpTransaction`(commit/rollback no-op, `getUpdateCnt` 读 BE 行数) + jdbc thrift 装配移入连接器 `planWrite`（OQ-1）+ 删 config-bag 三件套 `ConnectorWriteType`/`ConnectorWriteConfig`/`getWriteConfig` + `PluginDrivenTableSink` config-bag 分支 + `PhysicalPlanTranslator` getWriteConfig 调用（OQ-2）。**jdbc 写字节 parity 测**（`PROP_JDBC_*`/`connection_pool_*` 键） | P6.3-T01 | L | ✅ 2026-06-23（含 `appendExplainInfo` EXPLAIN-保留增补；见下实现记录）|
| **P6.3-T03** | **`IcebergConnectorTransaction` 骨架 + `addCommitData`**：implements `ConnectorTransaction`；单 SDK txn/表/语句（`table.newTransaction()` 经 P6.2 `IcebergCatalogOps` seam + auth 包裹）+ 14 字段 `TIcebergCommitData` `TBinaryProtocol` 反序列化 synchronized 累积（C4）+ `getUpdateCnt`（affectedRows/rowCount, data/delete 拆, dataRows 优先）+ txn-id 双注册表（per-mgr + `GlobalExternalTransactionInfoMgr`）桥接 `PluginDrivenTransaction`；`commit`=`commitTransaction()`、`rollback`=丢未提交 manifest | P6.3-T01 | M | ✅ 2026-06-23（见下实现记录）|
| **P6.3-T04** | **op 选择 + `IcebergWriterHelper` 等价**：INSERT/OVERWRITE 4 子 case（Append / ReplacePartitions 动态 / OverwriteFiles 空表清空 / `overwriteByRowFilter` 静态）+ DELETE（RowDelta deletes）+ MERGE（RowDelta rows+deletes）的 SDK op；begin* guards（fmt≥2 delete/merge、insert branch 校验须 branch 非 tag、baseSnapshotId 捕获）；BE 人类可读分区串→`PartitionData`/`"null"`→null、`TIcebergColumnStats`→`Metrics`、DV→PUFFIN+position-deletes、equality-delete 拒绝、分区表必须有分区数据 | P6.3-T03 | L | ⬜ |
| **P6.3-T05** | **commit 校验套件 + O5-2**：新 `ConnectorTransaction.applyWriteConstraint(ConnectorPredicate)` default-no-op + fe-core 通用抽 analyzed-plan target-only 合取→中性 `ConnectorPredicate`（slot-origin 过滤）+ 连接器复用 P6.2-T02 `IcebergPredicateConverter` 转 iceberg expr 暂存；commit 套件顺序（`validateFromSnapshot`/合并 `conflictDetectionFilter`/serializable `validateNoConflictingDataFiles`/`validateDeletedFiles`/`validateNoConflictingDeleteFiles`/`validateDataFilesExist`，`delete_isolation_level` 默认 serializable）+ V3 DV `removeDeletes`（`rewrittenDeleteFilesByReferencedDataFile`） | P6.3-T04 | L | ⬜ |
| **P6.3-T06** | **sink 统一**：连接器 `planWrite` 据 `writeOperation` 自建 `TIceberg{Table,Delete,Merge}Sink`（**同款 thrift 零 BE 改 C2**；schema-json/sort/spec/row-lineage/`rewritableDeleteFileSets`/`setMaterializedColumnName` 连接器内构建；vended-creds 经 P6.2-T09 接缝）；删 planner `Iceberg{Table,Delete,Merge}Sink` + translator `visitPhysicalIceberg*Sink`；走既有 `visitPhysicalConnectorTableSink`（DV-009 路径）；写分布/sort 经 `ConnectorCapability`（C8）；EXPLAIN sink-标签 diff 登记（OQ-3） | P6.3-T04 | M | ⬜ |
| **P6.3-T07** | **通用 `RowLevelDmlCommand` 壳 + capability 派发**：抽三命令 ~50% 通用脚手架（run/explain、copy-on-write 检查、`icebergRowIdTargetTableId` save/restore、`executeWithExternalTableBatchModeDisabled`、planner-drive loop、`getPhysicalSink`/`childIsEmptyRelation`、conflict-filter plumbing）；`Update/DeleteFrom/MergeInto` 路由 `instanceof IcebergExternalTable`→capability（`supportsDelete`/`supportsMerge`）；iceberg `$row_id` 注入/branch-label 投影代数/nereids→iceberg expr **暂留 fe-core** 经连接器-键控变换注册表调用（**有界 DV-04x**，含 `ConnectContext.icebergRowIdTargetTableId` scan-schema hook） | P6.3-T05,P6.3-T06 | L | ⬜ |
| **P6.3-T08** | **parity-UT 审计 + deviation 注册**：补 gap-fill UT（op 选择矩阵 / commit 校验套件 / V3 DV / getUpdateCnt / addCommitData 14 字段往返 / `applyWriteConstraint`→`IcebergPredicateConverter` 复用 / capability 派发 / jdbc no-op parity；真 InMemoryCatalog 无 Mockito）；对抗 parity workflow（每发现独立 skeptic verify）；登记 [deviations-log](../deviations-log.md) **DV-04x**（iceberg DML plan 合成 fe-resident，北极星 iii 关闭）+ EXPLAIN-diff（OQ-3）+ jdbc-thrift-移位（OQ-1） | P6.3-T07 | L | ⬜ |
| **P6.3-T09** | **收口**：汇总设计文档 `designs/P6-T?-iceberg-write-summary-design.md` + HANDOFF + PROGRESS + connectors/iceberg 同步 + gate 核对（iceberg 仍**不在** `SPI_READY_TYPES`）。**T09 完成 = P6.3 DONE** | P6.3-T08 | S | ⬜ |

**通用验收门（每 task）**：连接器 UT 绿（无 Mockito，fail-loud fake）+ checkstyle 0 + `tools/check-connector-imports.sh` 净 + **断 assembled Thrift / 校验套件 vs legacy 期望值**（parity-by-omission 风险）+ iceberg **不在** `SPI_READY_TYPES` + **jdbc/maxcompute 写字节 parity**（框架统一不得改其 thrift 输出，除 OQ-1 jdbc 移位时显式 parity）。**P6.3 验收门（line 91）**：INSERT/DELETE/UPDATE/MERGE 写 parity + 事务提交/冲突检测 + planner 改 `PhysicalConnectorTableSink` 后 EXPLAIN/执行不回归（EXPLAIN sink-标签 diff 经 OQ-3 接受为非回归）。

> **范围外**（不在 P6.3）：iceberg PROCEDURES（`rewrite_data_files` 等 10 action，含 legacy `RewriteFiles`/`updateRewriteFiles` 写半）→ **P6.4** `ConnectorProcedureOps`；hive 行级 ACID → P7；Trino 式 (iii) 通用化基座 → 后续专门 RFC。**反向 `instanceof IcebergExternal*`**：写层路由 6 处 + planner sink/translator cast 本期清；fe-resident plan 合成内 cast 属 DV-04x（北极星 iii 关闭）；其余（catalog/statistics/glue 读侧）属 P6.7。

### P6.3-T01 实现记录（2026-06-23，✅ 已实现 + 验证，**未 push**）

> 设计文档 = `designs/P6.3-T01-write-framework-unification-design.md`。实现 recon workflow `wf_3d74e33d-7c8`（5 reader + critic）；对抗 parity workflow `wf_0c8b7356-dae`（5 维 + 每发现 skeptic verify）= **7 findings / 0 confirmed real**。

- **⚠️ option B 切分裁定（用户签字）**：按字面 T01/T02 切分**无法各自保持绿**——jdbc 是 insert-handle SPI 的唯一消费者（`beginInsert/finishInsert` 实测 no-op，真写经 config-bag `TJdbcTableSink`），删 insert-handle（T01）会 strand jdbc，而 jdbc 迁移属 T02。⟹ **把「jdbc → no-op txn」提到 T01**（jdbc **暂留 config-bag sink**），各 commit 绿 + parity 测。**T02 调整为** = jdbc thrift 入 `planWrite`（OQ-1）+ 删 config-bag 三件套（OQ-2）+ jdbc 字节 parity（jdbc no-op 迁移已在 T01 完成）。
- **实改**：① SPI（`fe-connector-api`）：删 `usesConnectorTransaction` + `beginInsert/finishInsert/abortInsert` + dead `begin/finish/abortDelete·Merge` + 3 handle marker iface（`ConnectorInsertHandle/Delete/Merge`）；**保留** `getWriteConfig`/`ConnectorWriteType`/`ConnectorWriteConfig`（T02 删）+ `supportsInsert/Overwrite/Delete/Merge`（capability 派发面）；新增 `ConnectorTransaction.profileLabel()` default + **新类 `NoOpConnectorTransaction(id, label)`**（`getUpdateCnt=-1` 哨兵）。② jdbc：删 `beginInsert/finishInsert/JdbcInsertHandle`，加 `beginTransaction → NoOpConnectorTransaction(allocateTransactionId, "JDBC")`。③ maxcompute：删 `usesConnectorTransaction` override + `MaxComputeConnectorTransaction.profileLabel="MAXCOMPUTE"`。④ `PluginDrivenInsertExecutor`：单路（恒 `begin(connectorTx)`；finalizeSink **null-session guard** 防 config-bag NPE；doBeforeCommit `if(cnt>=0)` 哨兵守 jdbc affected-rows；transactionType 从 profileLabel 映射；删 onFail/abortInsert override + insert-handle helper）。
- **2 处对 RFC T01 清单的有意偏移（DV-T01-c，设计 §7）**：(1) `ConnectorWriteHandle.writeOperation` **移到 T03**（T01 0 消费者，Rule 2）；(2) `beginTransaction` **默认保持 throwing（fail-loud, Rule 12）** 非 RFC 字面 "no-op 默认"——默认 0 消费者 + `FakeConnectorPluginTest.beginTransactionDefaultThrows` 既有守 + 退化 no-op 由显式 `NoOpConnectorTransaction` 提供。
- **验证全绿**：connector-api 5 + jdbc 8 + maxcompute 9 + fe-core 目标 50 = 全 0F0E；其余连接器（iceberg/paimon/es/trino/hudi/hive）test-compile SUCCESS；iceberg 278 + paimon 318 无回归（须 `package -Dassembly.skipAssembly=true` 跑,`HiveConf` test-classpath 仅 package 相）；checkstyle 0；import-gate exit 0；iceberg 仍**不在** `SPI_READY_TYPES`；0 BE 改。
- **下一步 = P6.3-T02**（jdbc thrift 入 planWrite + 删 config-bag 三件套 + 字节 parity）。

### P6.3-T02 实现记录（2026-06-23，✅ 已实现 + 验证，**未 push**）

> 设计文档 = `designs/P6.3-T02-jdbc-planwrite-configbag-removal-design.md`。TDD（byte-parity 测先 RED→GREEN）；对抗 parity workflow `wf_86a9e683-6b5`（4 维 byte-parity/translator-dispatch/deletion-closure/regression-explain + 每发现 skeptic verify）= **0 confirmed real / 6 positive 确认**。

- **OQ-1（jdbc thrift 入连接器）**：新 `JdbcWritePlanProvider implements ConnectorWritePlanProvider`（镜像 `MaxComputeWritePlanProvider`）；`planWrite` 直建 `TJdbcTableSink`（熔合 legacy `getWriteConfig` 属性袋 + `bindJdbcWriteSink`）。`JdbcDorisConnector.getWritePlanProvider()=new JdbcWritePlanProvider(getOrCreateClient(), properties)` → 翻译器自动路由 jdbc 入 plan-provider。删 `JdbcConnectorMetadata.getWriteConfig`。**byte-parity 关键**：连接池值用 `getInt(...,DEFAULT_POOL_*)`（非 bind 硬编 fallback——legacy 真值来源）；catalogId/resourceName/tableType/useTransaction/insertSql 逐字段对齐（设计 §4.1）。
- **OQ-2（删 config-bag 三件套）**：删 `ConnectorWriteType` enum + `ConnectorWriteConfig` 类 + `ConnectorWriteOps.getWriteConfig` 方法 + `PluginDrivenTableSink` 整个 config-bag 半边（`writeConfig` 字段/config-bag ctor/`bindFileWriteSink`〔FILE_WRITE 死路〕/`bindJdbcWriteSink`/`PROP_*`/`getExplainString` config-bag 分支）+ `PhysicalPlanTranslator` config-bag 分支（→ `writePlanProvider==null` fail-loud 同款错串）。
- **OQ-3 收窄（用户增补：`appendExplainInfo` 写侧 EXPLAIN 接缝）**：新 source-agnostic SPI `ConnectorWritePlanProvider.appendExplainInfo(output,prefix,session,handle)` default-no-op（镜像扫描侧 `ConnectorScanPlanProvider.appendExplainInfo`）；jdbc 实现回吐 `TABLE TYPE`/`INSERT SQL`/`USE TRANSACTION`（共享 `buildInsertSql` helper → EXPLAIN SQL 与 BE 收到的一致）；`PluginDrivenTableSink.getExplainString` 委派（在 `bindDataSink` 之前跑故从 handle 派生）。**OQ-3 diff 收窄到仅 `WRITE TYPE: JDBC_WRITE`→`WRITE: plan-provider` 标签变**；regression 断言**恢复** `INSERT SQL: ...`（非退化）。对 T06 有利（iceberg sink-detail 可同 hook 保留）。
- **deviation（UT 不可见，P6.6/external-table docker 验）**：DV-T02-a jdbc sink thrift 移位（§4.1 + parity 测守）；DV-T02-b EXPLAIN 标签变（appendExplainInfo 已收窄）；DV-T02-c appendExplainInfo 在 EXPLAIN 字符串生成时读连接器元数据（net 改善 vs legacy translation 期每查必读）。
- **验证全绿**：jdbc 模块 **190/0/0**（`JdbcWritePlanProviderTest` 3 含 byte-parity+appendExplainInfo / `JdbcDorisConnectorTest` 8 +after-close wiring / `JdbcConnectorMetadataTest` 4）；connector-api 25；maxcompute 102(1 skip)；fe-core `PluginDrivenTableSinkTest` 2〔含 getExplainString 委派+BRIEF 短路〕+ `PluginDrivenInsertExecutorTest` 8 + `PluginDrivenTableSinkBindingTest` 2；es/trino/hudi/hive/hms test-compile SUCCESS；iceberg 278 + paimon 318 无回归（不实现 `ConnectorWritePlanProvider`，additive default 不影响）；checkstyle 0；import-gate exit 0；iceberg 仍**不在** `SPI_READY_TYPES`；**0 BE 改**。
- **下一步 = P6.3-T03**（`IcebergConnectorTransaction` 骨架 + `addCommitData` + 14 字段反序列化 + getUpdateCnt + txn-id 双注册表桥接；**此处加 `ConnectorWriteHandle.writeOperation`**，T01 因 0 消费者延后）。

### P6.3-T03 实现记录（2026-06-23，✅ 已实现 + 验证，**未 push**）

> 设计文档 = `designs/P6.3-T03-iceberg-connector-transaction-skeleton-design.md`。TDD（RED=cannot-find-symbol→GREEN）；对抗 parity workflow `wf_1598e4b9-87c`（6 维 + 每发现独立 skeptic verify）= **2 findings / 1 confirmed real（已修）/ 1 refuted**。

- **新类 `IcebergConnectorTransaction implements ConnectorTransaction`**（连接器内自包含，仅 import iceberg SDK + `org.apache.thrift.*`/`org.apache.doris.thrift.*` + connector.api/spi）：持单 SDK `org.apache.iceberg.Transaction`/`Table`；`beginWrite(db,table)` 经 `IcebergCatalogOps` seam loadTable + `table.newTransaction()` **两者都在 `context.executeAuthenticated` 内**（legacy `beginInsert:162` parity，见下 confirmed 修）；`addCommitData(byte[])` = `TDeserializer(TBinaryProtocol)` 反序列化 14 字段 `TIcebergCommitData` + `synchronized` 累积（镜像 maxcompute + legacy:104-115）；`getUpdateCnt` 忠实移植 legacy:577-596（affectedRows||rowCount、POSITION_DELETES/DELETION_VECTOR→deleteRows、`dataRows>0?dataRows:deleteRows`）；`commit`=`commitTransaction()`（null-guard fail-loud + try/catch→`DorisConnectorException`，镜像 maxcompute）；`rollback`=insert-mode no-op；`profileLabel`="ICEBERG"。
- **SPI 增补（T01 因 0 消费者延后）**：新枚举 `WriteOperation{INSERT,OVERWRITE,DELETE,UPDATE,MERGE}` + `ConnectorWriteHandle.getWriteOperation()` default INSERT（向后兼容，jdbc/maxcompute/es/trino/paimon 不 override→零行为变；消费者 = T04 op 选择 / T06 sink 方言）。
- **接线**：`IcebergConnectorMetadata.beginTransaction(session)` = `new IcebergConnectorTransaction(session.allocateTransactionId(), catalogOps, context)`（gate-closed/dormant）。**txn-id 双注册表 = 既有通用 `PluginDrivenTransactionManager.begin(ConnectorTransaction)` 完成**（per-mgr map + `GlobalExternalTransactionInfoMgr.putTxnById`，BE→FE report 路按 id 找 txn；连接器 0 注册码，同 maxcompute）——recon §3.2「双注册」描述的是 legacy `IcebergTransactionManager`，新路走通用 manager。
- **🔴 对抗复核 1 confirmed（已修）= auth-wrap `newTransaction()`**：reviewer 初判 nit「behaviorally inert」，**skeptic 用反编译 iceberg-core 1.10.1 字节码证伪**——`BaseTable.newTransaction()` → `Transactions.newTransaction(name,ops,reporter)` **无条件** `TableOperations.refresh()`（非 `current()`）→ `HiveTableOperations.doRefresh()` 远程 HMS Thrift；原实现仅 loadTable 在 auth 内，`newTransaction()` 在 auth 外 → Kerberized HMS 写丢 UGI 可失败，legacy:162 把两者放同一 auth 块。**修=`newTransaction()` 移进 `executeAuthenticated` lambda**（精确 parity，pass-through 对非-Kerberos 零成本）。**refuted 1**：commit null-guard（happy-path 同 legacy + maxcompute 模板 try/catch + 纯 fail-loud，非缺陷）。
- **deviation（设计 §7，T08 登记 deviations-log）**：DV-T03-a 失败抛 `DorisConnectorException`（连接器禁 import fe-core，消息同义）；DV-T03-b `writeOperation` 消费者在 T04/T06（T03 仅默认-值契约测，同 T01 延后理由）；DV-T03-c txn-id 双注册走通用 manager；DV-T03-d auth-wrap `newTransaction` UT-不可见（离线 InMemoryCatalog 无 auth，P6.6 docker/Kerberized HMS 验）。
- **范围外（后续 task）**：op 选择 + `IcebergWriterHelper`（PartitionData/Metrics/DV/equality 拒绝/分区数据）+ begin* guards（fmt≥2 delete/merge、branch 校验、baseSnapshotId 捕获）= T04；commit 校验套件 + `applyWriteConstraint`(O5-2) = T05；`planWrite` 3 thrift sink 方言 + capability(`supportsInsert/Delete/Merge`) = T06/T07。
- **验证全绿**：fe-connector-iceberg UT **295/0/1**（278→295=+17：`IcebergConnectorTransactionTest` 16 + `IcebergConnectorMetadataTest` 20→21；1 skip=env-gated live）；connector-api **27/0/0**（含新 `ConnectorWriteHandleTest` 2）；jdbc 190 / maxcompute 102(1skip) / paimon 318(1skip) 无回归（additive default SPI）；checkstyle 0（api+iceberg）；import-gate exit 0；iceberg 仍**不在** `SPI_READY_TYPES`；**0 BE / 0 fe-core / 0 pom 改**（fe-thrift 已 provided，P6.2-T03 加）。
- **下一步 = P6.3-T04**（op 选择 + `IcebergWriterHelper` 等价 + begin* guards + baseSnapshotId 捕获）。
