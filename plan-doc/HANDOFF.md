# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🚨 架构裁定升级（2026-07-05 续² 末，用户拍板）= **fe-core 不再持有任何属性解析/组装；已迁移连接器（paimon+iceberg）完全走"插件解析 → BE thrift → 回传 fe-core"**

> **触发**：本轮做属性/鉴权迁移到连接器时，vended 判定撞到"完整删除 vs 铁律 vs 风险"三者不可兼得；查 Trino（连接器独占存储/凭据解析，引擎从不建静态凭据表、也不必知 vended）后，用户升级为更根本的架构原则：
> - **fe-core 不应再持有任何属性解析能力**，全部由插件完成。
> - **storage 属性解析** → **fe-filesystem 模块**（插件侧）；**meta 属性解析** → **fe-connector**（插件侧）。
> - **数据流**：插件组装 → 传给 BE 的 thrift → 再回传给 fe-core（fe-core 只接收成品、不自己解析）。
> - **范围**：未迁移连接器暂留残留代码；但 **paimon 与 iceberg 必须完全按新架构走**（=paimon 也纳入范围，不再是 iceberg-only）。
>
> **影响（重估原 P5 7 刀计划）**：① 原"属性簇删除 + rewire"是这个大原则的**子集/前奏**，需按新架构重写设计；② **CUT 4（在 fe-core `CatalogProperty` 加 warehouse→fs.defaultFS helper）方向反了**（那仍是 fe-core 解析 storage）——应搬到插件/fe-filesystem 侧，待新设计定夺后可能 revert/改向；③ CUT 1（连接器自建 HMS 鉴权器）方向对（meta→连接器）；④ CUT 2（SHOW CREATE 脱敏在 fe-core）多半保留（展示层），但敏感键理想应由插件供。
>
> **✅ 完整新设计已出 = `plan-doc/tasks/designs/plugin-owns-property-parsing-arch-design.md`**（依据侦察工作流 `wf_61c70f0d-bce`，替代/升级 iceberg-metastore-auth-connector-rewire-design.md）。**关键实证（大幅利好）：目标架构大部分已就位** —— `fe-filesystem-api/spi` 已是插件可用共享模块（parent-first、排除打包，无须抽取）；连接器读/扫描路径**已** fe-filesystem-native；fe-core `StorageProperties.createAll` 只是**冗余的第二份解析**。故本迁移 = **退掉 fe-core 第二份解析**（10 步 S1-S10），非新建模块，比预想小。**vended 难题也顺带解掉**：fe-core 对所有插件 catalog **无条件停建静态存储表**（插件 100% 拥有 static+vended），不判别、不 gate、守铁律。
> **✅ 中央决策已裁定（用户 2026-07-05）= A 家族：共享宿主 classpath + 连接器发起 bind**（fe-filesystem 解析器留宿主 parent-first、不打包；连接器直接调 fe-filesystem-api 发起 bind，不经 fe-core context；fe-core 零解析。B 每插件打包不采纳）。
> **✅✅ S1–S10 全部落地（2026-07-05 续⁵，见下方最新一轮）= 已迁连接器（iceberg+paimon）属性解析全归插件、fe-core 零解析目标达成。** S8 用「连接器发起派生」变体落地：新增中立 SPI `Connector.deriveStorageProperties`，iceberg 连接器自持 warehouse→fs.defaultFS（仅 hadoop flavor），fe-core 插件路径经 `CatalogProperty.setPluginDerivedStorageDefaultsSupplier` 折叠、不再调 getMetastoreProperties；S7 删 fe-core Iceberg/Paimon 簇（-4914 行，含 recon 新发现的 5 个 fe-core iceberg 连通性探测器 + 协调器 iceberg 分支）。⚠️ legacy 静态 map 方法（`CatalogProperty` getStoragePropertiesMap/getBackendStorageProperties/getHadoopProperties/getMetastoreProperties + `StorageProperties.createAll` + 共享基类 HMSBaseProperties/AWSGlueMetaStoreBaseProperties/AliyunDLFBaseProperties/AbstractMetastorePropertiesFactory）已按 S10 铁律**保活**，供 Hive/Hudi/LakeSoul/HMS-iceberg 继续用，直到它们全离 SPI（阶段四）。
> **⏭ 下个 session 起步 = 回翻闸主线**（连通性 F2/F3/F15/F16 → ENG-3 flip-gated e2e 全跑 → 用户二签翻闸最后原子提交）；属性簇迁移子线已 CLOSED。
>
> **已 commit 未 push（前序轮，[DEC-FLIP-1] 铁律）**：CUT 1 `cf8dda9f058` / CUT 2 `eb9201dc0a6` / CUT 4 `0de34db83fb`。**CUT 4 已由 S8 `3e7a687e6e2` 接管——`deriveHdfsDefaultFsFromWarehouse` helper 已从 fe-core 删除、逻辑搬入 iceberg 连接器。**

---

# 🎯 最新（2026-07-15）= **rebase 到 upstream `30a44d61a8a`（23/23 重放干净）+ 新增翻闸 BLOCKER：upstream #65135 的 iceberg `$position_deletes` 未在连接器实现**

> ⚠️ 本段最新，**取代下方所有 🎯 历史**（下方 2026-07-10 续² 的 P6.6-C6 OIDC 段仍是有效的实现现状，只是不再是"最新一轮"）。备份 tag=`backup/pre-rebase-2026-07-15`（旧 HEAD `e0c392b9be0`；新 HEAD `c215c4c77c5`，**未 push**）。超越 `catalog-spi-rebase-2026-07-14`。

## rebase 事实
- `git pull --rebase upstream-apache master` → base=`30a44d61a8a`（upstream 23 新 commit / 我方 23 重放 → behind0/ahead23 干净线性）。
- **冲突 5 处，全部来自同一个 upstream commit `0814e49bea7` = #65135「Support Iceberg position deletes system table」**（2026-07-14 合入，比本次 rebase 早一天）。停在 commit 11/23 = P6 iceberg（`ec3a0cd55f4`）。
- 其余 22 个 upstream commit **零语义碰撞**（已逐条核）：#65525 LoadProcessor（改 checkHealthy，我方 P4 改 updateFragmentExecStatus，方法不相交）、#64133 BindRelation（`BINLOG_TIMESTAMP_COL`→`BINLOG_TSO_COL` 单行 rename，与我方 view arm 不相交）、#65570 Ranger / #65325 streaming-job / #65517 pom java17 均在我方未动文件。
- **哨兵已验**：`BINLOG_TSO_COL`(1) / `getBackendHealthStatus`(1) / Ranger `SingletonHolder`(2) / fe-authentication-api pom java17(1) 全存活。

## 冲突逐条裁决（why + 正确性）
| 文件 | 裁决 | 理由 |
|---|---|---|
| `iceberg/IcebergExternalTable.java` | `git rm`（保 P6 删除） | modify/delete。upstream 的改动**只是删掉 findSysTable override**；整类已被 P6 删。 |
| `iceberg/source/IcebergScanNode.java` | `git checkout --theirs`（整取 P6 版） | #65135 是**唯一**碰该文件的 upstream commit（已验），故整取不丢任何其它 upstream 修复；其 280 行全挂 `isSystemTable` 分支（P6 已删 → 部分不可编译，其余是死码+无用 import 会踩 checkstyle）。该文件在我方仅服务 **HMS-iceberg**，而 HMS-iceberg 系统表**两侧都抛**（`IcebergSysTable.createSysExternalTable` 对非 `IcebergExternalTable` 源恒抛），故**零活行为损失**。 |
| `systable/IcebergSysTable.java` | **手写**（不可整取任一侧） | `SUPPORTED_SYS_TABLES` 那段已自动合并成 upstream 版（去 filter、**去 `supported` 字段**）→ P6 侧的 `if (!supported)` 引用已不存在的字段。裁决=保 upstream 的 map（HMS-iceberg 因此 advertise position_deletes 后抛 IllegalArgumentException，**与 upstream 对 HMS 源的行为一致**）+ 保 P6 的恒抛 body（去 `!supported` 守卫）+ 三个 import 全删（两侧的都指向已删类/已无用者）。 |
| `systable/IcebergSysTableResolverTest.java` | **手写** | 保 upstream 的 `testSupportedSysTablesIncludePositionDeletes`（在合并后的 map 上**成立**）；删两侧 hunk（upstream 侧引用已删类，P6 侧为空）。 |
| `iceberg/test_iceberg_sys_table.groovy` | `git checkout --ours`（整取 upstream） | 我方 P6 对它的改动**只是改报错文案期望**（前提="legacy 也不支持"，已被 #65135 推翻）→ 前提失效即丢弃我方改动。**fail loud**：保留 #65135 的正向断言。 |

## ⚠️ git 不标记但会炸的 2 个隐性坑（对抗 review 抓出，已修）
1. **`iceberg/source/IcebergScanNodeTest.java` 不在冲突列表**（我方 23 commit 从未碰它 → 静默停在 upstream 的 329 行版本），却在 `:313/:323` **直接静态调用** `IcebergScanNode.checkPositionDeletesBackendCompatibility` + 4 处反射 `isSystemTable` 等 → 取 P6 侧必炸 **fe-core test 编译**。修=`git checkout f052d9da44e -- <file>` 还原到 pre-#65135 的 192 行（BASE==我方版，我方从未碰过）。**教训第 3 次复现：git 说没冲突 ≠ 语义没坏**（见 [[doris-build-verify-gotchas]]）。
2. **groovy 冲突区外**的 `expectedPositionDeleteColumns.addAll(...)`/`assertEquals(...)`/权限段两条 `$position_deletes` 断言**已被自动合并进来** → 盲取 P6 侧会得到引用未定义变量的坏 Groovy。整取 upstream 后此坑消解。

## ✅ `$position_deletes` 连接器移植 **DONE**（commit `2e49827ecdd`，未 push）= 该 blocker **解除，仅差 e2e**

> 权威 = `plan-doc/tasks/designs/P6.6-position-deletes-connector-port-design.md`（Status=IMPLEMENTED，§9 复审结论 / §10 follow-up）。
- **fe-core + SPI 零改动**（用户裁定不移植 smooth-upgrade 守卫）；全部落在 `fe-connector-iceberg` 内：`IcebergConnectorMetadata` 暴露 + `IcebergScanRange` 第三形状 + `IcebergPartitionUtils.getPartitionDataObjectJson` + `IcebergScanPlanProvider` 规划分支 + [D-065] 收窄。
- **验证**：连接器 **977/977**（+11 新测）、checkstyle 0、`build.sh --fe` **BUILD SUCCESS 61 模块**（禁 cache 真编译）。
- **对抗复审（`wf_08359fe4-611`，4 lens）抓到 2 个真缺陷，已修**：① 我引入的 **auth bug**（`resolveSysTable` 无 auth wrap，被第二个调用方在无 scope 处调用 → kerberos plan 期 GSS 失败）；② 既有**假注释**导致 split-size 对 puffin DV 失真（`fileSizeInBytes` → `ScanTaskUtil.contentSizeInBytes`）。第 3 条被另一 lens 推翻（权重 1.0 与 upstream 等价，非缺陷）。
- **⛔ 唯一剩余 = e2e**（本地无 iceberg REST+MinIO+**Spark** compose，两套件数据由 Spark 写）：`test_iceberg_position_deletes_sys_table.groovy`(659 行) + `test_iceberg_sys_table.groovy`。**这是翻闸门。**
- **follow-up（非阻塞，upstream 同源缺陷勿在此分叉修）**：STRING 分区值含 `"`/`\`/换行 → Jackson 转义但 BE STRUCT 文本 serde 从不反转义（`escape_char=0` 恒真）→ 字面量落库、**静默**。upstream #65135 行为完全相同，应向 upstream 报。

---

## （历史）原计划 = 实施 `$position_deletes` 连接器移植（研究+设计，T0 实证）

> **权威文档 = [`plan-doc/tasks/designs/P6.6-position-deletes-connector-port-design.md`](./tasks/designs/P6.6-position-deletes-connector-port-design.md)（commit `af0f0582bdf`）+ 同名 `-research-notes.md`。起步只读这两份 + 本段，勿重跑侦察（已烧两个工作流）。**

- **用户裁定（2026-07-15）**：① rebase 阶段 = 方案 A（机械对齐 + 登记 blocker）；② 移植阶段 = **不考虑 smooth-upgrade 兼容，按最终形态实现** ⇒ **fe-core 零改动、SPI 零改动，全部落在 iceberg 连接器内部**（原 T1/T2 取消）。
- **T0 实证 DONE，证伪了 2 处设计假设**（细节见设计文档 §2/D3/D6，**勿重新发明**）：
  1. **`partition_data_json` 不是 JSON 解析**——喂给 Doris STRUCT **文本** serde；且 `DataTypeNullableSerDe::from_string` **吞掉一切解析错误 → 静默 NULL + Status::OK()**。连接器现有 `getPartitionDataJson` 发 `["42"]` 数组会正中此坑 ⇒ **必须写第二个渲染器**。
  2. **BigDecimal**：jackson stock `valueToTree` 与 gson **两向皆异**（`1.50`→`1.5`；`10`→**`1E+1`** 语法变了）⇒ 用 `JsonUtil.mapper().copy().setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))` **hoist static final**；🚨 **绝不可改 `JsonUtil.mapper()` 单例**（iceberg 进程级共享）。Float/Double **无风险**（已实证，勿加投机代码）。
  3. **[D-065] 是 blocker**：跳 `iceberg.schema_evolution` 字典的理由"schema 随 serialized FileScanTask 走"**只对 JNI 路径成立**；position_deletes 是**第一个走原生 BE reader 的系统表**，需 `history_schema_info` 解析 `row` 列（**v1 硬报错 / v2 静默按名匹配读错**）⇒ 须把 `IcebergScanPlanProvider:1090` 的 `if (!systemTable)` **收窄为"仅 JNI sys 表跳过"**，字典由**元数据表 schema** 构建；连带 `IcebergScanPlanProviderTest:627/651/685` 断言须**收窄范围而非整删**。
- **剩余 TODO = 设计文档 §8 的 T3→T4→T5→T6→T7→T8**（单层连接器 commit stack）。
- **两个易踩死的实现点**：① `PositionDeletesTable.newScan()` **直接抛**，必须 `newBatchScan()`，且分叉要在 `buildScan` **之前**；② 连接器 `populateRangeParams:328-332` 默认发 **FORMAT_JNI**，而 position-delete range 必须 FORMAT_PARQUET/ORC（**PUFFIN 也发 FORMAT_PARQUET**）。
- **另记（超范围、登记翻闸清单）**：连接器**完全没有** initial-defaults 传输（`fe-connector` 全库 0 处 `setInitialDefaultValue`），是相对 upstream #65502 在**普通 iceberg 数据读路径**的既有更大缺口。勿把"position_deletes 不需要"读成"连接器不需要"。

---

## 🚨 翻闸 BLOCKER = `$position_deletes` 未在 iceberg 连接器实现（rebase 阶段登记；**移植已进行中**，见上段）
- **缺口**：我方 `SPI_READY_TYPES` 已含 `iceberg`（翻闸就在 `ec3a0cd55f4` 里），iceberg catalog 一律走连接器；而 `IcebergConnectorMetadata.listSupportedSysTables` / `isSupportedSysTable`（P6.5 的 **Q2 决策**）**刻意排除 `POSITION_DELETES`**，理由是"legacy fe-core 当时也不支持（抛 not supported yet）"——**该前提已被 #65135 推翻**。
- **后果**（agent 独立复现 high confidence，无任何 fallback：`findSysTable` 只查 `PluginDrivenExternalTable.getSupportedSysTables()`→连接器 list，无第二注册表、无 TVF、无 legacy catalog 分支）：`select * from t$position_deletes` 在我方报 `Unknown sys table`，在 upstream master **端到端可用** = **相对 upstream 的能力缺失**（非"rebase 弄坏原本能用的"，BASE 时两边都不支持；是**未能继承新功能**）。
- **已知红**（有意 fail loud，勿改测试掩盖）：① `test_iceberg_sys_table.groovy`（upstream 正向断言 + 权限段）；② **`test_iceberg_position_deletes_sys_table.groovy`（#65135 新增 659 行，不冲突、干净落地、整体红）**。
- **BE 侧已就位、成孤儿**：`be/src/format/table/` + `be/src/format_v2/table/iceberg_position_delete_sys_table_reader.{cpp,h}`（~1200 行）+ thrift 契约 + BE 单测全在，**只差 FE 没人产生这种 range**。
- **移植清单（下一轮做）**：① `IcebergConnectorMetadata` 去掉 POSITION_DELETES 排除（2 处：`listSupportedSysTables` + `isSupportedSysTable`，并同步改 `IcebergConnectorMetadataSysTableTest` 里 pin 住"连接器==legacy 公式"的断言）；② `IcebergScanRange` 加 position-delete 字段（对齐 fe-core `IcebergSplit` 的 7 个：`positionDeleteSystemTableSplit/FileFormat/Content/OriginalPath/ReferencedDataFilePath/ContentOffset/ContentSizeInBytes`）+ `populateRangeParams` 加 native range 形状分支（现有 sys 分支只发 FORMAT_JNI+serialized_split）；③ `IcebergScanPlanProvider` 移植 #65135 的 ~200 行规划（`BatchScan`+`PositionDeletesScanTask`+`splitPositionDeleteScanTask`+`determinePositionDeleteTargetSplitSize`+partition JSON 按 field-id 映射+binary/fixed/UUID 非空即抛）。**已知障碍**：smooth-upgrade 守卫 `checkPositionDeletesBackendCompatibility(backendPolicy.getBackends())` 需要连接器拿到 backend 列表，**SPI 目前不暴露** → 需扩 SPI 或把该守卫留 fe-core 侧。参考实现 = `git show 0814e49bea7 -- fe/fe-core/.../IcebergScanNode.java`。

---

# 🎯 上一轮（2026-07-10 续²）= **P6.6-C6 #63068 OIDC 会话凭证迁移：Tasks 0–7 全部 DONE + 三层 commit stack + clean-room 复审通过（未 push）**

> **本段是有效实现现状**（不再是"最新一轮"，见顶部 2026-07-15 段）。**三层 commit stack（未 push，[DEC-FLIP-1] 铁律）**：SPI `55c991d0e87`（fe-connector-api：中立 DTO + 能力位）→ FE `930e4778064`（fe-core：能力位门控注入 + 缓存旁路 + 2 测）→ 连接器 `a07216ef102`（iceberg：per-user 路由 + 适配器 + 属性校验 + 4 测）。设计权威 = `plan-doc/P6.6-C6-oidc-session-migration-design.md` 第 12 节 + `...-research-notes.md`。

## ✅ P6.6-C6 实现完成（起步先读本块 + 设计文档；行号可能过时、信控制流不信注释）

**Tasks 0–7 全部 DONE。验证：连接器 full test（禁 cache）GREEN（966 用例 0 fail）；fe-core 注入 + 旁路门 test GREEN；三模块 checkstyle 0。下方 Task 1–5 描述仍准（历史细节）。**

- **Task 1 SPI（fe-connector-api）**：新增中立 DTO `ConnectorDelegatedCredential`（Type{ACCESS_TOKEN,ID_TOKEN,JWT,SAML}+token+OptionalLong expiresAtMillis+isExpired；toString 脱敏）；`ConnectorSession` 加 `getDelegatedCredential():Optional`（default empty）+ `getSessionId():String`（default=getQueryId 兜底）；`ConnectorCapability` 加 `SUPPORTS_USER_SESSION`。
- **Task 2 FE 注入（fe-core）**：`ConnectorSessionImpl` 加 sessionId+delegatedCredential 字段+构造参+getSessionId/getDelegatedCredential 覆写；`ConnectorSessionBuilder.from(ctx)` 存 ctx，加 `withUserSessionCapability(bool)`/`withSessionId`/`withDelegatedCredential`，`build()` 仅当 capable 时从 `ctx.getSessionContext()` 拉凭证+sessionId 映射进中立 DTO（`toConnectorCredential` 按 enum name 桥接，不匹配即 fail-loud）；`PluginDrivenExternalCatalog.buildConnectorSession()` **from(ctx) 分支**加 `.withUserSessionCapability(supportsUserSession())`（create() 后台分支不带凭证=O3 后台跳过雏形），加私有 `supportsUserSession()`。
- **Task 3 缓存旁路（fe-core）**：**大发现——表级旁路机制在 rebase 后【保留】**（`ExternalCatalog.shouldBypassTableNameCache` 默认 false + `ExternalDatabase.loadTableNamePairs/getTableNullableWithoutCache/matchesLocalTableName` 全在，接入 isTableExist/getTableNamesWithLock/getTableNullable），此前无人 override→休眠。本轮只加 `PluginDrivenExternalCatalog.shouldBypassTableNameCache` override（能力位+凭证在场）。**O2 库级旁路是真新增**（旧在已删 IcebergRestExternalCatalog）：`ExternalCatalog` 新增 `shouldBypassDbNameCache` 默认 false + `getFilteredDatabaseNames(boolean updateDbNameLookup)` 重载（bypass 传 false 不动共享 lowerCaseToDatabaseName）+ getDbNames/getDbNullable 加 bypass 分支 + `getDbNullableWithoutCache`（buildDbForInit checkExists=false 防 getDbNames 递归）+ `matchesLocalDbName`；`PluginDrivenExternalCatalog.shouldBypassDbNameCache` override 同门控。
- **Task 4 连接器属性**：`IcebergConnectorProperties` 加 REST_SESSION/SESSION_NONE/SESSION_USER/REST_DELEGATED_TOKEN_MODE/DELEGATED_TOKEN_MODE_*/REST_SESSION_TIMEOUT 常量；`IcebergRestMetaStoreProperties`（**验证在此跑**，经 IcebergConnectorProvider.validateProperties→bindForType(rest)→validate()）加 @ConnectorProperty session+delegatedTokenMode 字段 + `validateUserSession()`（session/token-mode 枚举 + session=user⇒oauth2）+ **放宽** oauth2-requires-token（session=user 不要求静态 cred，加 `!isUserSession()`）；本模块不依赖 fe-connector-iceberg 故内联本地常量；`IcebergConnector.getCapabilities()` 条件加 SUPPORTS_USER_SESSION（新 `isUserSessionEnabled()`=session=user && rest）。
- **Task 5 连接器会话目录+适配器**：新增 `IcebergDelegatedCredentialUtils`（中立 Type→OAuth2Properties key）；新增 `IcebergSessionCatalogAdapter`（持 Catalog+Optional<RESTSessionCatalog>+DelegatedTokenMode；catalog/delegatedCatalog/viewCatalog/delegatedViewCatalog(ConnectorSession)+static toIcebergSessionContext；**fail-closed** delegated* 无凭证抛 DorisConnectorException；含 nested enum DelegatedTokenMode.fromString）；`IcebergConnector` 加 restSessionCatalog+sessionCatalogAdapter 字段 + `buildRestSessionCatalogDefault`（session=user REST 走 `new RESTSessionCatalog()`+configureHadoopConf+initialize+asCatalog(empty)，删 catalog-impl 键，session-timeout→AUTH_SESSION_TIMEOUT_MS）+ `newCatalogBackedOps(session)` 重载（session=user 经 adapter.delegatedCatalog/delegatedViewCatalog 拿 per-user）+ getMetadata 改用它 + close() 关 restSessionCatalog。
  - **关键坑（已解，验过 iceberg 1.6/1.10 javap）**：`RESTSessionCatalog.asCatalog(ctx)` 返回 `AsCatalog implements Catalog,SupportsNamespaces` 但 **NOT ViewCatalog**（视图要单独 `asViewCatalog(ctx)`）。故给 `CatalogBackedIcebergCatalogOps` 加**可选显式 viewCatalog 参**（新 6 参构造；旧 5 参 delegate viewCatalog=null；`resolveViewCatalog()`=显式优先否则 catalog instanceof ViewCatalog；isViewCatalogEnabled+5 处 `((ViewCatalog)catalog)`→resolveViewCatalog()）。session=none 行为逐字不变。
  - **为何元数据路由这么省**：`PluginDrivenExternalCatalog` **每 op 都新建** `connector.getMetadata(session)` 且同 session 传给后续方法（20+ 站点核实），故只在 `IcebergConnector.newCatalogBackedOps(session)` 一处 resolve per-user catalog 即覆盖所有元数据 op，**不动 1765 行 IcebergConnectorMetadata、不动 ops seam**。

### ✅ Task 6 DONE（扫描/写/过程 session 路由，commit `a07216ef102`）
- 3 provider 各加 `Function<ConnectorSession,IcebergCatalogOps> catalogOpsResolver`；connector 传 `this::newCatalogBackedOps`；**旧构造器 = 常量 `s->catalogOps`**（保测试/普通目录 byte 不变）。`resolveTable`/`resolveSysTable`/`runInAuthScope`/`planInAuthScope` → `resolver.apply(session).loadTable(...)`，**先解析 ops 出 auth 作用域**（fail-closed DorisConnectorException 原样抛、不被 executeAuthenticated 的 catch 包裹）。**写入 COMMIT 本就 per-user**（`IcebergConnectorTransaction` 经 `beginTransaction(session)` 继承 session-aware metadata ops），无需改。**测试适配**：`IcebergConnectorTest.catalogOpsOf` 反射 `catalogOps`→`catalogOpsResolver`（`apply(null)`，非 user-session 忽略 session）；`IcebergProcedureOpsTest` 裸 `null` 参加 `(IcebergCatalogOps)` cast 消歧两构造器（bare null 同时匹配 ops/Function 构造器）。

### ✅ Task 7 DONE（测试 + clean-room 复审 + commit）
- **新增/改测**：`IcebergProviderSessionRoutingTest`(4：每 provider resolver 用 call session + fail-closed) / `IcebergSessionCatalogAdapterTest`(7：凭证 key 每模式 access_token=verbatim bearer、token_exchange=typed key + fail-closed + fromString) / `IcebergConnectorValidatePropertiesTest`+5(session=user⇒oauth2 / 放宽静态 token / 非 user 仍需 token / 非法 session/token-mode) / `ConnectorSessionImplTest`+3(capable→注入 / 非 capable→跳过 / capable 无 cred) / `PluginDrivenExternalCatalogSessionBypassTest`(3：旁路判定门=能力位&凭证在场) / **`ExternalDatabaseSessionContextTest`(1，commit `e83f2b77bc4`：#63068 缓存泄漏**数据流**重迁——mockStatic(SessionContext.current) 驱动每令牌身份 + TestableCatalog 重写 listDatabaseNames 每令牌返回各自库并记录令牌；两用户结果不相交 + 每次读都 live 重列(含重复令牌)=证无跨用户泄漏+无共享缓存;#63068 的"bootstrap 无令牌读"在本分支 fail-close，故改用"每读 live 记录令牌"作等价可观测)。
- **Task 0 保留基座已核实 GREEN**（本会话 `mvn test -pl fe-core -am`）：DelegatedCredentialTest 3 / ConnectProcessorDelegatedCredentialTest 5 / FEOpExecutorDelegatedCredentialTest 1 / MysqlAuthPacketCredentialExtractorTest 4 = 13/13。通用 auth 抓凭证+SessionContext+thrift 1004-1007 转发全在。
- **clean-room 对抗复审（自审 + 独立 agent 交叉核对；subagent 首轮环境故障 0 tool-use、重派次轮 25 tool-use 成功且独立复现结论）**：
  - ① **路由完整性 = 零缺口**：每个 query-time 表/视图/命名空间 load 都经 per-request session；shared `asCatalog(empty)` 仅被 **0-caller 的 graceful `catalog(session)/viewCatalog(session)`** 和 `!isUserSessionEnabled()` 门控的 no-arg `newCatalogBackedOps()` 用；`new IcebergConnectorTransaction` **唯一站点**=`IcebergConnectorMetadata.beginTransaction`(session-aware)；`testConnection` 是 CREATE-CATALOG-time（非 query 路径）；method-ref `this::newCatalogBackedOps` 正确解析到 1-参 session-aware 重载。
  - ② **缓存泄漏 = 无**：旁路走 live `loadTableNamePairs(ctx,false)`/`getFilteredDatabaseNames(false)`，`updateLookup=false` 时**不动**共享 `lowerCaseTo{Table,Database}Name`/`metaCache`（ExternalDatabase:186/192/201/230、ExternalCatalog:607/628）；库级旁路仍 re-append `information_schema`+`mysql`（ExternalCatalog:599-602）；无凭证→`SessionContext.empty()`/`current()`→连接器 fail-closed 抛→共享缓存**永不被 per-user 数据污染**。

### ⚠️ 遗留 / follow-up（非阻塞）
- **O3 后台日志降噪 = 未做（登记独立小 follow-up）**：用户裁定"保留无令牌即拒硬 parity（**DONE**）+ 后台无令牌日志降噪"。**降噪经查非小改动**（当初 option 框定失准）——噪音出在后台统计守护 `StatisticsAutoCollector`(:109 通用 catch + :187) 对 session=user 表 `getRowCount`→`getMetadata` fail-closed 抛的 WARN，且拒绝异常穿 `PluginDrivenExternalTable.fetchRowCount`→行数缓存边界（要精确只静音这一种、不掩盖真失败、不误伤手动 ANALYZE，须 marker 异常 + 遍历 cause 链 + 动后台守护 catch）。故**不塞进本安全提交**，作独立 follow-up（识别拒绝异常→后台 catch 降 DEBUG）。触发＝仅"session=user 且对其开自动统计"边缘配置。
- **复审 2 小 nit（非泄漏非阻塞）**：① `ExternalDatabase.matchesLocalTableName` 纯大小写敏感 exact 模式用 `equals`，而缓存路走小写 map（近似大小写不敏感）——罕见 case-sensitive 外表下有分歧、旁路更严格，非泄漏；② `isTableExist/getTableNullable` 非旁路分支用 `SessionContext.current()` 填缓存 vs 旁路判定用参 `sessionContext`——实践同源、仅理论不一致。③ 适配器 graceful `catalog(session)/viewCatalog(session)` **零调用方**（连接器只用 fail-closed 变体；保留对称、非 bug，可后续清）。
- **flip-gated e2e 全未跑**（本地无 REST 会话集群 Polaris/Lakekeeper）：per-user 真绑定/token_exchange/跨 FE sessionId 稳定性待翻闸后 e2e。

---


- **tier3（#65094 读路径列名大小写保留）DONE — commit `ea8b1c1e0ff` (2026-07-10)**：iceberg+paimon 外表**顶层**列名保留远端大小写（纯 FE、**BE 不动**；字节匹配不变式 slot名==TSchema顶层名 两侧一起从小写翻成保留大小写；嵌套 struct 子列保持不变）。both-connector `mvn package -pl :fe-connector-iceberg,:fe-connector-paimon -am`(禁 cache) **GREEN，0 checkstyle，0 fail**。含审计漏项 **IcebergPartitionUtils:627**（listPartitions 值map key，是 :164 的孪生；漏了会让混合大小写 iceberg 分区列在 MTMV/裁剪丢分区值）+ 回归测试。

- **#63068 OIDC 会话凭证迁移 — 设计文档完成，待实现（用户 2026-07-10 拍板全采纳推荐）**：
  - 文档：**`plan-doc/P6.6-C6-oidc-session-migration-design.md`**（第 12 节 = 7 步 TODO）+ **`...-research-notes.md`**（Trino 源码 + #63068 `e545f1ad08a` as-built + 当前 SPI，全 source-verified via 2 subagents）。
  - **关键发现**：通用 session 基座（MySQL鉴权捕获 / `SessionContext` / `DelegatedCredential` / thrift 1004-1007 跨FE转发 / `ConnectProcessor` / `FEOpExecutor`）本分支**全保留且可用** → `ConnectContext.get().getSessionContext().getDelegatedCredential()` 已在查询线程可取。迁移只剩 iceberg 消费侧（3 删类 `IcebergUserSessionCatalog`/`SessionCatalogAdapter`/`DelegatedCredentialUtils` + 路由 + `iceberg.rest.session` 配置 + 缓存旁路），**从 fe-core 重定向到 `fe-connector-iceberg` SPI**。
  - **决策（已定，全采纳推荐）**：**O1**=加 `ConnectorCapability.SUPPORTS_USER_SESSION`（能力位门控 FE 注入；最小权限）；**O2**=库级缓存旁路放通用 `PluginDrivenExternalCatalog`、能力位+凭证在场门控；**O3**=后台/异步任务(preload/auto-analyze/refresh)在 `session=user` 下**跳过**，交互查询**fail-closed**（无凭证即抛，同 #63068）；**O4**=SPI `getSessionId()` **复用转发的 `SessionContext.sessionId`**（稳定 AuthSession key），不用 queryId；**subject-JWT=不做**（按 #63068 verbatim 传 token：access_token / token_exchange 两模式；Trino 式 `sub=<user>` 签名列 follow-up）。
  - **架构对齐**：凭证走**中立 SPI DTO** `ConnectorDelegatedCredential`（连接器不 import fe-core），**一次性注入在 `PluginDrivenExternalCatalog.buildConnectorSession()`**（Trino 式 `session.getIdentity().getExtraCredentials()`，非 #63068 逐方法 SessionContext 穿参）。iceberg 用**单个共享** `RESTSessionCatalog` + per-request `SessionContext`（勿建 per-user catalog）。
  - **硬约束**：纯 FE、不改 BE；`session=user` 的 fail-closed 与缓存旁路是**安全语义**（防跨用户缓存泄漏，参 Trino CVE-2026-34214）需保 parity；凭证勿 editlog/持久化/SHOW/profile/information_schema。

---

# 🎯 最新一轮（2026-07-05 续⁵）= **属性簇迁移收官 S7–S10：连接器自持存储派生 + 删除 fe-core Iceberg/Paimon 元存储簇 = 2 代码 commit（未 push）**

> **本轮范围** = 承接 S6，一次把 **S7、S8、S9、S10** 全部处理掉（用户本 session 明确要求）。先跑 6-agent 侦察工作流（`wf_225ef82c-53c`：删除安全闭包 / 插件 getMetastoreProperties 闭包 / S8 派生 parity / S9-S10-auth 死码 + 2 路对抗核验），**对抗核验揪出设计一句话漏掉的 compile 阻塞**（`datasource/connectivity/` 子系统硬引用 iceberg 簇），据此扩大 S7 删除面。需用户拍板处（派生逻辑放哪）已用中文讲清背景+例子后拿到裁定 = **搬进连接器**。
>
> **✅ S8 `3e7a687e6e2`（连接器自持 warehouse→fs.defaultFS，插件路径退掉 fe-core 元存储解析）**：新增中立 SPI `Connector.deriveStorageProperties(rawProps)`（默认空）；`IcebergConnector` 实现 hadoop-flavor `warehouse=hdfs://ns/path`→`fs.defaultFS=hdfs://ns`（**仅 hadoop 类型派生**，逐字节镜像原 `IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties`，铁律：连接器 inline hdfs 常量、无 fe-core import）；`PluginDrivenExternalCatalog.initLocalObjectsImpl` 经 `CatalogProperty.setPluginDerivedStorageDefaultsSupplier` 把连接器派生结果懒折叠进存储表，**FE 绑定（raw supplier）与 BE 扫描（typed supplier）两路同源**；`CatalogProperty` 的 `initStorageProperties`/`getEffectiveRawStorageProperties` 改走 `mergeDerivedStorageDefaults()`→`resolveDerivedStorageDefaults()`：插件 catalog 用 supplier（不调 getMetastoreProperties），legacy Hive/Hudi 仍走 msp。删 fe-core 中立 `deriveHdfsDefaultFsFromWarehouse` helper（搬入连接器）。**为何这是 S7 前置**：un-register 后任何对插件 catalog 的 getMetastoreProperties 都会抛「Unsupported metastore type」，本刀切断插件存储路径对它的**唯一**依赖（recon 实证：typed+raw 两 supplier 是仅有的插件消费点，连通性协调器被 PluginDriven.checkWhenCreating override 绕开）。验收=fe-core 10 测 + iceberg 7 测 0 失败、三模块 checkstyle 0、连接器无 fe-core import。
>
> **✅ S7 `3c69bfa8265`（删 fe-core Iceberg/Paimon MetastoreProperties 簇 + un-register，−4914 行）**：`MetastoreProperties` 摘 register(Type.ICEBERG/PAIMON)（枚举值留、工厂不注册→fail-loud）+ 删死方法 `initExecutionAuthenticator`；删 **16 簇类**（9 iceberg + 7 paimon）+ **2 property/common 凭据类**（IcebergAwsAssumeRole/ClientCredentials，仅簇内消费）+ **5 fe-core iceberg 连通性探测器** + `CatalogConnectivityTestCoordinator` 摘 4 iceberg 分支/import（Hive 逐字保留）；删 14 簇单测、迁 `CatalogPropertyEffectiveRawStoragePropsTest` 到 supplier 路径。验收=fe-core main+test BUILD SUCCESS（删 37 文件零悬挂引用=闭包完整）+ checkstyle 0 + 28 存储/脱敏/派生用例 + 7 存活 Hive 元存储用例 0 失败。
>
> **✅ S9（决策落地，无代码）= BE-thrift 方向保持现状（INDIRECT 默认）**：实证插件 iceberg/paimon 的 BE thrift **已全在连接器侧**建（`IcebergScanRange/IcebergScanPlanProvider.populate*`、`PaimonScanRange/PaimonScanPlanProvider.populate*`；fe-core 无 paimon 包；fe-core `IcebergScanNode.setIcebergParams` 只服务 legacy HMS-iceberg）。HDFS/kerberos 保持 INDIRECT（唯一 typed build = `HdfsResource.generateHdfsParam` 单转换器留 fe-core），S3/对象存储已 `params.setProperties(map)`。「插件→BE thrift」原则由 by-reference populate* 接缝结构性满足，无须动码。
>
> **✅ S10（铁律验证，无代码）= legacy 残留全保活**：`CatalogProperty` getStoragePropertiesMap/getBackendStorageProperties/getHadoopProperties/getMetastoreProperties（:260/:308/:333/:284）+ `StorageProperties.createAll`（:137，main 3 处消费）+ 共享基类（HMSBaseProperties/AWSGlueMetaStoreBaseProperties/AliyunDLFBaseProperties/AbstractMetastorePropertiesFactory）实测均在，供 Hive/Hudi/LakeSoul/HMS-iceberg 直读解析出 BE thrift（清单见 recon `wf_225ef82c-53c` s10KeepAliveConsumers）。**只 repoint 了插件路，未删任何共享方法。**
>
> **⚠️ flip-gated 未验（本地无集群）**：iceberg hadoop-catalog 只带 warehouse 的 HDFS-HA 绑定 e2e（S8 派生逐字节镜像，单测证 FE+BE 折叠一致 + 逻辑，但真绑定 flip 后才能 e2e）；重部署类加载冒烟。**全部未 push（[DEC-FLIP-1] 铁律）。** 侦察结论持久化在 `wf_225ef82c-53c` journal + 设计文档 §4。

---

# 🎯 上一轮（2026-07-05 续⁴）= **属性簇迁移 S3–S6 一口气落地（写入凭据改绑 / vended 机制退休 / URI 接缝保留决策 / Kerberos 鉴权全归连接器）= 4 代码 commit + 1 doc**

> **本轮范围** = 承接 S2，按设计 §4 刀序把 **S3、S4、S5、S6** 全部处理掉（用户本 session 明确要求）。逐刀独立 commit + build + test + checkstyle + import-gate；每刀凭据/行为差异都做了核对，需用户拍板处已用中文讲清背景+例子后拿到裁定。**全部未 push（[DEC-FLIP-1] 铁律）。**
>
> **✅ S3 `72680f03995`（iceberg 写入路径 BE 静态凭据改绑 fe-filesystem）**：`IcebergWritePlanProvider.buildHadoopConfig` 由 `context.getBackendStorageProperties()`（fe-core CredentialUtils 解析）改为遍历 `context.getStorageProperties()` 取 `toBackendProperties().toMap()`——与扫描路径同源。**parity 核对**（专派 agent 逐 backend）：带凭据的对象存储/HDFS 写认证键（AWS_*/use_path_style/超时）逐字节一致、HDFS 派生默认值一致；唯一非逐字节差异是老路经 createAll 默认注入的 HDFS 兜底多带两个 BE 对象存储写不读的惰性键（ipc.client.fallback-to-simple-auth-allowed、hdfs.security.authentication=simple），新路不带——**扫描早已在用新形且线上运行=存在性证明够用**。验收=iceberg 939 测 0 失败 + 写测 41 mutation KILLED + checkstyle 0 + import 净。
>
> **✅ S4 `f2976900852`（vended 凭据机制彻底退休，用户裁定「彻底退休」，−1083 行）**：删 3 类（VendedCredentialsFactory/AbstractVendedCredentialsProvider/IcebergVendedCredentialsProvider + 3 测）；删 `CatalogProperty.shouldBuildStaticStorage` gate，`initStorageProperties`+`getEffectiveRawStorageProperties` 无条件走 `mergeDerivedStorageDefaults`；`IcebergScanNode`(legacy HMS) 直读 `getStoragePropertiesMap()`（HMS-typed 恒 null，逐字等价）；删已死的 `MetastoreProperties.isVendedCredentialsEnabled()`(+PaimonRest 覆写)。**行为变化（用户已接受，flip-gated）**：vended 目录若同时配静态 key，语义由「vended 取代静态」变「vended 叠加静态」（同名仍 vended 优先、最终认证不变，仅额外仅静态键也发 BE）；纯 REST 无变化。验收=fe-core 113 测 0 失败 + checkstyle 0 + import 净。
>
> **✅ S5（决策落地无代码，用户裁定「保留现状」）= 保留 fe-core 薄 URI 规范化/文件类型接缝**：`normalizeStorageUri`/`getBackendFileType` 已引擎中立（String 进出、无引擎名分支），仅复用已解析存储做 `LocationPath` 规范化。fe-filesystem 无等价 API（`LocationPath`/`TFileType` 均 fe-core-only、fe-filesystem 刻意无 Thrift），迁移代价大且对合并读删有正确性风险，无硬性要求故不做（低优先，将来硬性要求再评估）。
>
> **✅ S6 前置 `498d3230916` + 落地 `12599958bce`（Kerberos 鉴权全归连接器，用户裁定「本 session 完整做掉、接受 flip-gated 风险」）**：**前置**=paimon 连接器补 HMS 元存储 Kerberos 鉴权器（抽 `PaimonConnector.buildPluginAuthenticator` 静态、镜像 iceberg CUT1、新增 HMS-Kerberos-简单存储分支 + 5 单测），补齐 recon 发现的「paimon 缺 HMS 鉴权器」blocker。**落地**=删 `PluginDrivenExternalCatalog.initPreExecutionAuthenticator` 重写→继承基类 no-op（插件 catalog fe-core handle 非空但不做 doAs，真 doAs 在连接器）；删无调用方的 `getOrderedStoragePropertiesList` getter + 内联字段。验收=fe-core 375 测 + paimon 5 测 0 失败 + 双 checkstyle 0 + import 净。**⚠️ flip-gated**：端到端 Kerberos（对真 KDC doAs）本地无集群不可验，单测只证「鉴权器已构建 + 句柄非空 + 插入不崩」。
>
> **本轮 recon（4 路后台 agent 侦察，结论已固化进设计 §4）**：S3 parity 逐 backend 核对、S4 vended 控制流全图（gate 双路 + IcebergScanNode 空转 + 无 legacy 回归）、S5 消费者图 + fe-filesystem 无等价、S6 doAs 全图（iceberg 冗余 / paimon HMS blocker）。**下一步 = S7**（退 paimon/iceberg MetastoreProperties 簇 + S8 CUT4 搬回连接器）。

---

# 🎯 上一轮（2026-07-05 续²）= **属性/鉴权迁移到连接器启动：recon 纠正计划前提 + 用户裁定完整删除 + 设计文档 + CUT 1 done**

> **本轮范围** = 承接 P4 DONE，启动"属性/鉴权迁移到连接器 + fe-core iceberg 属性簇删除"（removal-execution-plan **§P5**，死码删除收官最大一块）。**做了 recon → 暴露 Rule 7 前提错误 → 用户拍板 → 写设计 → 落 CUT 1**。
>
> **⚠️⚠️ Rule 7 关键更正（recon `wf_73999dcf-412` 实证，纠正 §P5 / removal-plan v2 / 旧 HANDOFF 的核心前提）**：
> - 旧说"给连接器 metastore provider 补 authenticator 构建（**镜像 paimon 已有做法**）"——**paimon 无此样板**。实证：paimon 的 HMS 鉴权器**也在 fe-core 建**（`PaimonHMSMetaStoreProperties:60`，与 iceberg `IcebergHMSMetaStoreProperties` 同型），paimon 连接器**不**自建 HMS 鉴权器（只在 storage kerberos 时自建，与 iceberg 连接器同）；fe-core paimon 属性簇**完好在用**（非死码）。→ 真删 fe-core iceberg 簇须做 paimon 都没做的新东西（连接器自建 HMS 鉴权器）= CUT 1，**非"照抄 paimon"**。
> - **方向仍对（pro-Trino）**：Trino 连接器自持元数据鉴权 + 存储凭据解析，引擎核心只给中立 session/identity + doAs 执行面；本仓 metastore-spi 边界正是照此设计。P5 删掉 fe-core 凭据层最后一处引擎专属耦合（iceberg 是 `VendedCredentialsFactory` 唯一残留 case）。
> - **by-design 保留（P5 后仍在 fe-core，非泄漏）**：`executeAuthenticated` doAs 面、存储凭据 NORMALIZATION 到 BE 规范 AWS_*/hadoop 键、fe-filesystem provider registry（paimon 同赖）。**勿把 normalization 挪进连接器。**
>
> **✅ 用户裁定（2026-07-05）= 完整删除、一次到位**（接受鉴权刀 flip-gated 本地无法验证；已用中文讲清背景/风险/三选项后选定）。
>
> **✅ 权威设计 = `plan-doc/tasks/designs/iceberg-metastore-auth-connector-rewire-design.md`**（含纠正后前提 + 7 刀序 + 已裁定子决策 D1-D4 + 风险 + 验收口径）。**下个 session/续跑先读它。** 7 刀：**✅CUT 1（`cf8dda9f058`）** → **✅CUT 2（`eb9201dc0a6`，SHOW CREATE 脱敏改绑）** → CUT 3 vended gate 走 raw prop（⚠️见下，待用户拍板）→ **✅CUT 4（`0de34db83fb`，warehouse→fs.defaultFS 重新安置）** → CUT 5 REWIRE 停造 fe-core iceberg 簇（flip-gated 全 flavor e2e）→ CUT 6 删死连通性探测 → CUT 7 删属性簇。
>
> **✅ CUT 1 `cf8dda9f058`（连接器自建 HMS-metastore Kerberos 鉴权器，PREP，最高风险/flip-gated）**：抽包内 static `IcebergConnector.buildPluginAuthenticator(props, storageHadoopConfig)`；storage-kerberos 分支逐字不变；新增 HMS 分支——flavor==hms 且 `HmsMetaStoreProperties.kerberos()` present-with-creds 时用 HMS client principal/keytab 建 `KerberosAuthenticationConfig`（逐字镜像 fe-core `HMSBaseProperties.initHadoopAuthenticator:176-180`）。补 `IcebergConnectorPluginAuthenticatorTest` 5 例。**验收**：fe-connector-iceberg BUILD SUCCESS + checkstyle 0 + 5/5 绿 + mutation 击杀。**⚠️ flip-gated 未跑**：Kerberized-HMS-on-simple-storage e2e（本地无集群，登记 ENG-3）；CUT 1 使该窄场景从 fe-core-delegate doAs 立即改走 plugin-UGI doAs（同 principal/keytab、且 plugin-UGI 才是 plugin FileSystem 正确副本 → 更正确，但须 e2e 证）。
>
> **✅ CUT 2 `eb9201dc0a6`（SHOW CREATE 脱敏改绑，PREP，纯本地可验）**：`DatasourcePrintableMap.SENSITIVE_KEY` 删 `getSensitiveKeys(IcebergRestProperties.class)` 反射 + import → 显式 add 4 键（byte-identical）。**recon 深挖纠错**：与 `S3Properties` 重叠不均——`iceberg.rest.secret-access-key` 冗余于 S3 敏感 secret-key，但 `iceberg.rest.session-token` 是 S3 **非 sensitive** 字段别名 → 只由本处保护，省略即静默泄漏 → 四键全显式。**验收**：fe-core BUILD SUCCESS + checkstyle 0 + `DatasourcePrintableMapTest` 18/18 + mutation 击杀（oauth2.token/session-token 丢即转红）。
>
> **✅ CUT 4 `0de34db83fb`（warehouse→fs.defaultFS 桥重新安置，PREP，纯本地可验）**：`CatalogProperty` 加包内 static `deriveHdfsDefaultFsFromWarehouse`（中立 warehouse 键 + hdfs scheme，守铁律），`initStorageProperties` 统一 derived 源（msp!=null 用 msp、msp==null 用中立 helper）。**recon 后弃"通用化对所有 engine 生效"**（会误改 paimon fs + core-site.xml fs.defaultFS 冲突场景）→ 用 msp==null 分支天然只覆盖 iceberg，paimon（有 msp）不受影响、行为保持。补 `CatalogPropertyDeriveHdfsWarehouseTest` 5 例；BUILD SUCCESS + checkstyle 0 + 5/5 + mutation 2/2 击杀。
>
> **⏭ 续跑起步任务 = CUT 3（vended gate，但须先找用户拍板一个设计难点）→ 然后 CUT 5 REWIRE（翻掉 iceberg msp）→ CUT 6/7 删除**。**⚠️ CUT 3 设计难点（CUT 4 的 msp==null 中立 helper 模式不能直接套用）**：paimon 靠 `msp.isVendedCredentialsEnabled()` 判 vended **是因保留 msp**；iceberg CUT 5 后 msp==null → 须让 vended 判定在 msp==null 下工作，但 vended 要读 iceberg 专属键 `iceberg.rest.vended-credentials-enabled`（引擎名串=铁律，不同于 CUT 4 的中立 warehouse 键）。**动 CUT 3/CUT 5 前按 `ask-user-explain-in-chinese-first` 找用户拍板**（候选见设计文档 §3 CUT 3）。
>
> **⚠️ 全部未 push**（[DEC-FLIP-1] 铁律）。recon 结论持久化在 `wf_73999dcf-412` journal + 已固化进设计文档。

---

# 🎯 上一轮（2026-07-05）= **iceberg 4 死实体类删除全完成（第 4、5、6 刀 done）= P4 DONE**

> **本轮范围** = 承接上一轮（cut 1-3 done），完成删原生 iceberg 4 死实体类（`IcebergExternalTable`/`IcebergExternalDatabase`/`IcebergSysExternalTable`/`IcebergExternalCatalog` base）的**第 4、5 刀前置 + 用户 sign-off 后第 6 刀原子删**。**至此整条 P4（删 4 实体类簇）= DONE**。**下一 = P5 属性/鉴权迁移到连接器（用户 2026-07-05 已裁定为下个 session 起步任务）**，权威刀序见执行计划 §P5 + 文末 🚀 段。
>
> **✅ 本轮完成 2 刀（均独立 commit + BUILD SUCCESS + checkstyle 0 + 测试全绿，未 push）**：
> - **第 4 刀 `ac0cef5b9a4`（修 buildDbForInit 回放）**：`ExternalCatalog.buildDbForInit case ICEBERG` 由构造 `IcebergExternalDatabase` 改为 `PluginDrivenExternalDatabase`（对齐翻闸后运行时类型 + GSON remap；JDBC/TRINO/PLUGIN 三同胞 case 早已同签名构造），删去现已无用的 IcebergExternalDatabase import。**保留 case 标签**（删则 fall-through `default→return null` 在旧 InitCatalogLog(Type.ICEBERG) 回放崩 db init）；Type.ICEBERG 枚举保留供老镜像反序列化。验收：IcebergGsonCompatReplayTest 3/3。
> - **第 5 刀 `96020c70e99`（迁测试脱死实体到活类型）**：跑 10-agent 对抗核验分类工作流（每 verdict 逐一驳斥核验、防悄悄丢活覆盖 Rule 9/12），把仍测**活逻辑**、当前经死实体类作载体的单测改挂活类型：**6 mock 改挂**（DbsProcDirTest/UserAuthenticationTest/StatementContextTest/StatisticsUtilTest/IcebergMetadataOpTest/IcebergUtilsTest → HMS*/PluginDriven*/mock(ExternalCatalog)）+ **IcebergSysTableResolverTest trim** 到活断言（保留测 SUPPORTED_SYS_TABLES 排除 position_deletes、删死路径 position_deletes 报错用例）+ **迁 IcebergUtils 分区助手两测**（testGetPartitionRange/testSortRange，fe-core 唯一覆盖 getPartitionRange/sortPartitionMap/mergeOverlapPartitions）到新文件 `IcebergUtilsPartitionRangeTest`。**顺带修好一处此前空跑的测试**（UserAuthenticationTest：旧 iceberg mock 不满足生产已收敛的 `instanceof PluginDrivenSysExternalTable`→换活类型后 getSourceTable 委派 + checkTblPriv 断言真正生效）。验收：9 测试类 69 测 0 失败。
>
> **⚠️ Rule 7 更正（cut 5 实证，纠正旧 HANDOFF/执行计划）**：旧 HANDOFF 说 `IcebergExternalTableBranchAndTagTest` 应"改挂 IcebergMetadataOps 活路径 + port getPartitionRange 覆盖"是**双重误判**——① 该测**零** getPartitionRange 引用（getPartitionRange 覆盖实在 `IcebergExternalTableTest`，已迁）；② 其测的 fe-core `IcebergMetadataOps` branch/tag 车道翻闸后**孤儿**（native 走连接器 `IcebergCatalogOps` 独立重实现、HMS 走 `HiveMetadataOps` 抛错，仅死的 `IcebergExternalCatalog:123` 接 dispatching metadataOps），连接器 `CatalogBackedIcebergCatalogOpsDdlTest`/`IcebergConnectorMetadataDdlTest` 已等价覆盖 → 该测随 cut 6 删、**不迁移**、零活覆盖损失。
>
> **✅ cut-6 前置实证 done（grep 全仓 code-vs-comment 分类）= cut 6 确认为干净原子删**：删 4 类后**零 ALIVE 代码引用会断编译**——main-src 仅剩 `GsonUtils.java` 3 处**字符串标签**（`registerCompatibleSubtype(PluginDriven*.class, "IcebergExternal*")` 老镜像升级 remap，字符串非类引用，删后照编）+ 各 ALIVE 文件过时**注释**（cosmetic）。`IcebergUtils`/`IcebergMetadataOps`/`source/`/cache/ **零**死类引用（HANDOFF 担心的"以死类为参/字段类型的活方法"早被 cut 1 搬常量 + cut 3 删 showCreateView 重载清掉）→**无须改任何 ALIVE 签名**。test-src 真实代码引用仅剩 3 文件随删。
>
> **✅ 第 6 刀 `1ca3617a51a`（原子删 7 文件，-1822 行，用户 2026-07-05 sign-off 后执行）**：
> - 4 实体类 `datasource/iceberg/{IcebergExternalTable,IcebergExternalDatabase,IcebergSysExternalTable,IcebergExternalCatalog}.java`
> - 测试夹具 `test/.../iceberg/TestIcebergExternalCatalog.java`
> - 2 纯死路径测试 `test/.../iceberg/{IcebergExternalTableTest,IcebergExternalTableBranchAndTagTest}.java`
> - **留**：`GsonUtils` 3 字符串标签（老镜像升级 remap）、`IcebergGsonCompatReplayTest`（纯字符串标签，证升级路径）。
> - **验收（Rule 12 实测）**：fe-core main+test BUILD SUCCESS；12 smoke 测试类 89 测 0 失败（含 **IcebergGsonCompatReplayTest 3/3** = 证删老类后老 iceberg 镜像仍正确反序列化为 PluginDriven，升级兼容成立 + IcebergUtilsPartitionRangeTest 2/0 = 迁移覆盖存活）；checkstyle 0；连接器零 import 死类（连接器对旧类名引用全为 parity 注释）。
>
> **📌 遗留（登记，非本轮 blocker）**：
> - **cosmetic**：ALIVE 文件（MaterializeProbeVisitor/LogicalFileScan/ShowCreateTableCommand/… + 连接器 parity 注释）里仍有提及已删旧类名的**过时注释**，不影响编译，留后续清理。
> - **ENG-1**：cut 3 从 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES` 删 `IcebergExternalTable.class` 后未加 `PluginDrivenExternalTable.class`（潜在 MTMV 物化孪生缺口）+ `RefreshManager` 同型 → 值得 ENG-1 核（删死码不改运行时行为）。
>
> **⏭ 下个 session 起步任务（用户 2026-07-05 已裁定）= P5 = 属性/鉴权迁移到连接器 + 属性簇删除**（权威 = 执行计划 **§P5**）。**删死码工作的最后一大块、也是最重的——功能增项 + 改接线，非纯删除**。起步要点（起步先读 §P5 + 对照真实代码 recon，行号可能过时、信控制流不信注释）：
> - **为何属性簇还活**：翻闸后 plugin iceberg catalog 仍经 fe-core 属性簇建鉴权/凭据——`PluginDrivenExternalCatalog.initPreExecutionAuthenticator → catalogProperty.getMetastoreProperties()(Type.ICEBERG → IcebergPropertiesFactory)` 建 kerberos 鉴权器；`CatalogProperty.initStorageProperties → VendedCredentialsFactory case ICEBERG + msp.getDerivedStorageProperties()` 建凭据/derived-storage。
> - **为何可自足删（不受 HMS-iceberg 阻塞）**：HMS-iceberg 走 `type=hms → HivePropertiesFactory`，**从不解析 Type.ICEBERG** → 属性簇只被 plugin 路径钉 → rewire 掉 plugin 路径即可删。
> - **rewire 是真活且非平凡**：连接器 `fe-connector-metastore-iceberg` 承接类已存在（IcebergHms/Glue/Dlf/Rest/Jdbc/NoOp MetaStoreProperties+Provider），**但它们尚不自建 authenticator（现依赖 fe-core context authenticator）** → **须先给连接器 metastore provider 补 authenticator 构建（镜像 paimon 已有做法），再把 PluginDrivenExternalCatalog 的鉴权/凭据/derived-storage 接线改走连接器 metastore-spi**，rewire 完才能删属性簇（清单见 §P5：`property/metastore/{AbstractIcebergProperties,IcebergPropertiesFactory,Iceberg*MetaStoreProperties,IcebergRestProperties}` + `property/common/IcebergAws*Properties` + `MetastoreProperties` enum/register + `VendedCredentialsFactory case ICEBERG` + 连通性 testers F2/F3/F15/F16 死臂）。
> - **正交备选（非本次）**：翻闸主线（连通性 F2/F3/F15/F16 → ENG-3 flip-gated e2e → 二签）——P5 与翻闸正交，用户已定先做 P5。
>
> **⚠️ 全部未 push**（[DEC-FLIP-1] 铁律）。cut-5 分类/执行工作流结论持久化在 `wf_4e9d5818-e50`（classify）/`wf_6186b19e-815`（apply）journal + 已固化进执行计划 §P4。

---

# 🎯 上一轮（2026-07-04 续）= **fe-core iceberg 死码删除 P3（catalog flavor 簇 + factory，2 独立 commit）**

> **做了什么**（承接「删死码优先」裁定，按执行计划 `designs/fe-core-iceberg-removal-execution-plan.md` §P3 刀序；先 5-agent 逐 test-file 核验 fixture 处置 + 末尾 3-lens 对抗核验删除安全）：一刀删掉翻闸后不再实例化的 iceberg catalog flavor 全簇（原生 iceberg catalog 现建成 `PluginDrivenExternalCatalog`）。
> - **前置 `c051ff2c3d2`**：削 `IcebergMetadataOps` 两处死 `instanceof IcebergRestExternalCatalog` 臂（`listNestedNamespaces` REST 嵌套 namespace 递归臂 + `isViewCatalogEnabled` REST view-enabled 臂）+ 净化无用 import。**死判定实证**：`IcebergMetadataOps` 仅两处活构造边——`IcebergExternalCatalog:127`（翻闸后 base 不实例化=死）与 `HMSExternalCatalog:246`（活；`dorisCatalog` 恒 `HMSExternalCatalog`，从不是 Rest flavor）→两臂恒 false，删臂对活 HMS 路径行为逐字不变。REST 嵌套/view-enabled 的**活路径已由连接器 `IcebergCatalogOps` 承接并测**（`CatalogBackedIcebergCatalogOpsTest` 注释明写镜像 legacy gate）→无能力孪生缺口。
> - **删除 `a91e6b0a641`**（−344 行）：删 `IcebergExternalCatalogFactory`（零 caller，`CatalogFactory` 内置 case 已在 GSON 切换删、仅留注释）+ **7 flavor**（`Iceberg{Rest,HMS,Glue,Hadoop,Jdbc,S3Tables,DLF}ExternalCatalog`）。GSON 旧类名标签走 `registerCompatibleSubtype` 字符串 remap（非类引用）。base `IcebergExternalCatalog` **留**（阶段四；HMS-iceberg 仍活）。测试 fixture 迁移：新增 test-only 具体子类 `TestIcebergExternalCatalog`（镜像已删 flavor 空构造器）供需真实 `catalogProperty` 的用例（`IcebergUtilsTest`×3、`IcebergExternalTableBranchAndTagTest` spy）；仅占位/mock 的 3 用例改 `Mockito.mock(IcebergExternalCatalog.class)`；`IcebergUnityCatalogRestCatalogTest` 删 @Disabled 的 `testCreateRestCatalog`（测翻闸后已死的原生 REST getAllDbs 端到端路径）。
> **验收（Rule 12 实测）**：全仓零活引用（仅剩历史/parity 注释）；fe-core BUILD SUCCESS + checkstyle 0 + import-gate 净；8 测试类全绿（IcebergUtilsTest 16 / StatisticsUtilTest 9 / IcebergMetadataOpTest 7 / ExternalMetaCacheRouteResolverTest 7 / DbsProcDirTest 6 / IcebergExternalTableBranchAndTagTest 3 / **IcebergGsonCompatReplayTest 3=证删 flavor 后 GSON 旧类名标签 remap 完整、老集群升级不受影响** / IcebergMetadataOpsValidationTest 13）。
> **实证纠正执行计划（Rule 7）**：计划原写「6 flavor」实为 **7**（含 Rest）；`IcebergDLFExternalCatalog` 不引用 P2 已删 DLF 子树，不受 P2→P3 顺序阻塞（已回填执行计划 §P3）。
> **⚠️ 全部未 push**（[DEC-FLIP-1] 铁律：删死码/翻闸做最后原子提交再 push）。
> **⏭ 下一 = P4 实体类 + base catalog**（见下文 🚀 段 + 执行计划 §P4）：删 `IcebergExternalTable`/`IcebergExternalDatabase`/`IcebergSysExternalTable`/`IcebergExternalCatalog`(base)。**前置重**：① 搬 `IcebergExternalCatalog` 常量到 IcebergUtils/新常量类（被活文件读）；② 修 `ExternalCatalog.buildDbForInit case ICEBERG` 回放为 `PluginDrivenExternalDatabase`；③ 削 ~15 处翻闸后恒 false 的 `instanceof IcebergExternalTable/IcebergSysExternalTable` 死臂（含 F4 `redirectSysTableToSource` helper）。**⚠️ P3 迁移保命但 P4-doomed 的测试**（`IcebergExternalTableBranchAndTagTest` 随 IcebergExternalTable 死）：到时若仍要验 IcebergMetadataOps branch/tag 语义，须改挂 HMS/table 活路径而非丢失。

---

# 🎯 上一轮（2026-07-04）= **ENG-1 除连通性外剩余 5 条缺口批量修完（F4/F13·F9/F10/F12·F11·F6/F7·F14；直接动码不 recon + 末尾统一对抗 review；5 独立 commit）**

> **做了什么**（用户 2026-07-04 裁定协议：照审计结论直接动码、不逐条 recon/写单独 design、末尾统一 review）：ENG-1 审计 16 条确认缺口里，除已修 F1、已接受 F8、留后续的连通性 F2/F3/F15/F16 外的 **5 条 low** 一次性修完。信源=报告 §三 + 任务清单 §5b（逐条已回填 DONE + 修法 + commit）+ 完成记录 `designs/ENG1-batch2-remaining-gaps-summary.md`。
> **5 条 commit**：`cd7618ef53e`(F4/F13 SHOW CREATE sys 表 redirectSysTableToSource) · `c8b39f871e3`(F9/F10/F12 iceberg getTableComment) · `50e4a6bcb5d`(F6/F7 EXPLAIN nested columns 通用重发) · `50ad635d9b0`(F14 AWS 非 DEFAULT PROVIDER_CHAIN carrier，新 `AwsCredentialsProviderModes`) · `bc5c39157aa`(F11 元数据预热改 `SUPPORTS_METADATA_PRELOAD` 能力位)。**铁律全守**（无 fe-core if(iceberg)/instanceof/引擎名新 seam；连接器禁 import fe-core；F11/F14 走中立能力位/连接器自包含 twin）。
> **验收（Rule 12 实测）**：8 测试类全绿 + 广义 fe-core 回归 19 类 0 fail；**mutation 7/7 KILLED**（每缺口各对应测试转红）；checkstyle 0（api/fe-core/iceberg/jdbc）；import-gate 净。**统一对抗 review**（5 维度×驳斥，`.claude/wf-eng1-batch2-review.js`）抓出 **F14 真 bug**（`S3_MODE_KEYS` 漏 `iceberg.rest.credentials_provider_type` 别名 + providerFor 4 模式漏测）已修+re-verify 绿；1 nit（F4 死臂无测）驳回。
> **两个有意偏离（Rule 7 记录，非缺陷）**：**F10** 保留共享 twin 单引号转义（消费者单引号包裹→合法 SQL；legacy `SqlUtils.escapeQuota` 双引号含 `'` 会坏 SQL；无引号 comment 字节相同→选正确性+外科不动共享码）；**F6/F7** iceberg field-id 编号注解 `col(3).sub(5)` 不复刻（cosmetic、FU-h10-deadcode、SlotDescriptor 不能越界给连接器；BE 仍收编号形，查询无影响）。**F14 STS base 凭证**仍走默认链（assume-role 已孪生，非 F14 焦点）。
> **e2e flip-gated 全未跑**（本轮无集群）：F14 credential、F6/F7 EXPLAIN、F9 SHOW CREATE/information_schema、F4 SHOW CREATE、F11 preload 均翻闸后 e2e → 登记 **ENG-3**。
---

# 🚀 下个 session 的任务（用户 2026-07-05 裁定）= **P5 属性/鉴权迁移到连接器 metastore-spi**（死码删除 P0–P4 均已 DONE，见顶部 🎯 段；本段属性/鉴权 sub-task 是删死码工作的收官大块）

> **用户裁定**：翻闸主线（连通性 F2/F3/F15/F16 → ENG-3 e2e → 二签，见文末）**往后放**；先做**删除代码**——正交于翻闸，可独立推进。权威设计 = `plan-doc/fe-core-iceberg-removal-plan.md` v2（§阶段一死码 + §4 属性簇 + §6c 鉴权 + §8 用户已裁定 Q1+Q2=A/Q3=B）。**两件事**：
> 1. **删除所有已可删的死码**（阶段一余量）；
> 2. **属性/鉴权迁移**——fe-core iceberg 属性簇改走连接器已有的承接模块。
>
> **⚠️ 起步先读 recon 结论（2026-07-04 本轮实证，纠正 v2 部分过时判断）**：
> - **去 SDK 化七刀已删掉一大批阶段一死码**：`rewrite/`、`action/`、四个 DML 执行器（Delete/Merge/Insert/Rewrite Executor）、`LogicalIcebergTableSink`、`PhysicalIcebergTableSink`、`IcebergTransaction`、`IcebergTransactionManager`、`IcebergApiSource`、`IcebergDmlCommandUtils`、helper 的两个 RewritableDeletePlan* 等**均已 GONE**（见 `af7e244c3fe`/`64b03892b20`/`bf326c04741`/`4e7220d81c7`）。**残留死执行器仅 `LogicalIcebergMergeSink` 一个还在**（LogicalIcebergTableSink 已删但 MergeSink 漏删=待清）。
> - **逐文件三态分类 + 可执行刀序 = `plan-doc/tasks/designs/fe-core-iceberg-removal-execution-plan.md`**（本轮 5-agent 分类工作流 `wf_7f1358fa-35d` 逐调用方核验产出，20 DELETE_NOW / 18 NEEDS_PREP / 23 ALIVE_HMS）。**下个 session 起步先读它的刀序（P0-P5 + 阶段四）再动手。** 刀序：**✅P0=broker/+helper（`b52703dc1b5`）→ ✅P1=fileio/（`6a169f1dd98`）→ ✅P2=DLF 子树+HiveCompatibleCatalog（`b29e9ffcbde`）→ ✅P3=catalog flavor 簇+factory（前置臂 `c051ff2c3d2` + 删除 `a91e6b0a641`）已删** → **✅P4 实体类 + base catalog（6 刀全完成，搬常量 `e6024ea632d` + 清 catalog-type 臂 `816585ef2ab` + 清 table-type 臂 `4b6381b6964` + 修 buildDbForInit 回放 `ac0cef5b9a4` + 迁测试 `96020c70e99` + 原子删 4 类 `1ca3617a51a`）** → **P5 属性/鉴权迁移到连接器 ◀ 下一（用户 2026-07-05 裁定起步）**。**ALIVE_HMS 23 文件禁删**（`IcebergUtils`/`IcebergMetadataOps`/`source/`/cache/ 等，HMS-iceberg 经 `PhysicalPlanTranslator:825` DlaType.ICEBERG 活）——挂 hive 迁 SPI（阶段四）。
> - **属性/鉴权（sub-task 2 = P5，最大一块）**：连接器 `fe-connector-metastore-iceberg` 承接类已存在（IcebergHms/Glue/Dlf/Rest/Jdbc/NoOp MetaStoreProperties+Provider），**但 rewire 是真活且非平凡**：核验确认翻闸后 plugin iceberg 仍经 `PluginDrivenExternalCatalog.initPreExecutionAuthenticator:147 → getMetastoreProperties()(Type.ICEBERG)` 建 kerberos 鉴权器 + `CatalogProperty.initStorageProperties:181 → VendedCredentialsFactory case ICEBERG + getDerivedStorageProperties`。**关键：HMS-iceberg 走 type=hms 从不吃 Type.ICEBERG → 属性簇只被 plugin 路径钉 → rewire 掉 plugin 路径即可删（自足，不受 HMS-iceberg 阻塞）**。但**连接器 metastore provider 尚不自建 authenticator（现依赖 fe-core context authenticator）→ 须先给它补 authenticator 构建（镜像 paimon），再改接线**——这是个功能增项+rewire，非纯删除，详见执行计划 §P5。
>
> **执行纪律（复用本仓铁律）**：删除/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED（信控制流不信注释）；每刀独立 commit + build + test + checkstyle 0 + import-gate 净；**NEEDS_PREP 文件先做前置（搬常量 / 修 buildDbForInit 回放 / 削死臂 / 改 p2 测试）再删**；用户已裁定 Q1+Q2=A（fileio/ 并入删、接受 io-impl 极端配置失效、同步改 p2 回归）、Q3=B（HMS-iceberg 随 hive 迁移，不建重定向接缝）。**iceberg-core 依赖 + HMS-iceberg 活代码是阶段四、挂 hive 迁移，短期删不掉。**
---

# 🎯 翻闸主线（往后放，但仍需完成才能翻闸二签）= 连通性 4 条 → ENG-3 e2e → 二签

> ENG-1 除连通性外已全清（F1 + 本批 5 条 low）。剩 **连通性 F2/F3/F15/F16（medium，opt-in `test_connection`，默认 false）**：hms/glue-flavor iceberg 的 `test_connection` 元存储探测静默 no-op（`IcebergConnector.testConnection` `TYPE_REST`-only）→ 修法=连接器侧为 hms/glue flavor 补 HMS thrift/Glue GetDatabases 探测（信源报告 §三 F2/F3）。之后 **ENG-3 flip-gated e2e 全跑 → 用户二签 → 翻闸最后原子提交（含 GSON 迁移 `e68eb5c00c9`）**。**全部翻闸/review/删死码工作均未 push**（[DEC-FLIP-1]：翻闸做成最后原子提交再 push）。
---

# 🎯 上一轮（2026-07-05）= **行级 DML 去 SDK 化七步全部完成（设计 Status=DONE，7 个独立 commit）**

> **七步 commit 全谱**：`af7e244c3fe`(1/7 rewrite/action 死车道) + `64b03892b20`(2/7 DML 死臂闭包) + `bf326c04741`(3/7 INSERT 死车道并入) + `4e7220d81c7`(4/7 四小类搬中立包) + `255bcaf52a2`(5/7 checkstyle 门禁) + `e5972dfc8a2`(6a/7 rewrite re-derive 补 doAs) + `890b8698e6f`(6b/7 DML 预执行窗口补回滚) + 本文档轮 commit（7/7）。前三刀 -11,000+ 行；设计=`plan-doc/tasks/designs/iceberg-rowlevel-dml-desdk-design.md`（Status=DONE，完成记录在其头部）；removal-plan §6b 已补落地记录。
> **本轮（4-7 步）要点**：
> - **4/7 搬包**：IcebergMergeOperation→`nereids.trees.plans.commands.merge.MergeOperation`（改名）、IcebergNereidsUtils 存活半→`commands.RowLevelDmlRowIdUtils`（改名）、IcebergRowId/IcebergMetadataColumn 保名进 `commands`；实际涟漪比设计多 3 个测试文件 + IcebergExternalTable 补 import；IcebergHiddenColumnTest/IcebergMetadataColumnTest 随类搬包时被 nereids 测试包 ImportControl（禁 JUnit4）强制 **JUnit4→5 机械转换**（断言逐条不变）。
> - **5/7 门禁**：nereids（含 fe-sql-parser）/planner 两 subpackage 禁 `org.apache.iceberg`；**mutation 击杀验证**（注入 SDK import → checkstyle FAILURE → 回滚双绿）。**豁免清单 = 19 处 datasource.iceberg import/14 文件 + 3 测试**（明细在 commit message）：legacy 豁免臂 + 翻闸后死 instanceof 臂 + **活 IcebergUtils 引用**（isIcebergRowLineageColumn v3 行谱系 ×3 + 常量读 ×2——设计"活 import 归零"的说法过于乐观，此面登记待后续中立化，非本刀回归）。
> - **6a/7**（连接器侧）：registerRewriteSourceFiles 的 pinned-snapshot planFiles re-derive 包 `context.executeAuthenticated`（镜像 commit():438）；新增 2 接线 UT（authCount 递增 + failAuth 证 seam 在 authenticator 内）+ mutation 击杀。
> - **6b/7**（fe-core）：begin→finalize 窗口抽 `beginTransactionAndFinalizeSink` 包 catch(Throwable)→onFail（镜像 InsertIntoTableCommand:372-388）；`BaseExternalTableInsertExecutor.onFail` protected→public（调用方在父包）；新 RowLevelDmlCommandTest 3 case + mutation 击杀。
> **整刀验收（Rule 12 口径）**：nereids/planner 零 `org.apache.iceberg` import 且 checkstyle 上锁；13 个 gate 套件 137 测 0 失败（PhysicalPlanTranslatorIcebergRowLevelDml/PhysicalIcebergMergeSink/AdmissionGate/PluginDrivenTableSink/PluginDrivenExternalTable/WriteConstraintExtractor/两 Converter/IcebergDeletePlan/GsonCompatReplay/RowLevelDmlCommand/RowLevelDmlRowIdUtils/IcebergRowLevelDmlTransform）；**docker e2e（dml 4 套件 + action/ 8 套件）flip-gated 未跑**（死码删除理论零行为差；6a kerberos e2e 也 flip-gated——rewrite 车道翻闸后才活）。
> **⏭ 下一 session（按用户 07-01 既定顺序回翻闸主线）**：**ENG-1 能力孪生审计**（任务清单 §5 + review 报告 §七；见下文 🎯 段）。备选并行项：① 有集群时补跑 flip-gated 回归（meta-cache 两回归 + 类加载冒烟 + `iceberg_branch_complex_queries`/`partition_operations`）；② 盯 PR #64689 CI 的 `test_iceberg_hadoop_catalog_kerberos`（第四刀实证）；③ removal-plan 阶段一余量（fileio/、broker/ 等孤岛死码刀，正交可独立做）。

---

# 🎯 上一轮（2026-07-04 深夜）= **行级 DML 去 SDK 化设计完成并四项裁定（APPROVED）：实证改判为死车道删除，设计 = `plan-doc/tasks/designs/iceberg-rowlevel-dml-desdk-design.md`**

> **做了什么**：11-agent 研究工作流（4 路清点：fe-core DML 车道 82+ 引用点 / 连接器 SPI 面 / Trino master merge 模型（@280b81bbc4e）/ BE 契约 → 完备性批评家 → 6 路补盲：删除闭包双证明、HMS-DLA 共享面、测试面全图、鉴权包装逐 crossing、失败/KILL 生命周期、e2e pin 面）。产出设计文档并经用户四项裁定为 **APPROVED**。
> **核心改判（已同步进移除计划 §6b/§8-Q6）**：原定四项中,操作码与行标识**已中立**（IcebergMergeOperation 纯常量、rowid 列经 getSyntheticWriteColumns 声明+中立注入,与 Trino 逐点同构,无需新 SPI）;表达式下沉与暂存句柄化的**目标是翻闸后死码**（转换函数仅剩死调用方,连接器自带 IcebergPredicateConverter 三模式;SDK 暂存两端皆死,中立替身 rewriteSourceFilePaths 已上线）→ 改判**死车道删除**。死码双证明：实例源 3 处全不可达 + GSON 读侧重映射/写侧只写 PluginDriven（保留死码零回滚价值）。
> **用户四项裁定**：①接受改判为删除;②原生 INSERT 死车道并入同刀（IcebergTransaction 被两组死执行器共同钉住）;③4 个 SDK-free 小类现在搬 nereids 中立包（MergeOperation 改中立名,IcebergRowId/IcebergMetadataColumn 保名搬包）;④两个顺带活问题随本轮独立 commit 修：**rewrite 提交前 registerRewriteSourceFiles 的 planFiles 在 kerberos 裸奔**（镜像 commit():438 包 executeAuthenticated）+ **RowLevelDmlCommand.run 预执行窗口无回滚**（镜像 InsertIntoTableCommand:372-388 catch→onFail）。
> **动码安全边界（设计 §4-§6 已固化,此处仅提醒）**：IcebergScanNode 是 **HMS-DLA 活类禁整删**（仅成员级手术）;测试面 12 直删+3 case 手术+**2 个 KEEP 测试须先搬出待删目录**（IcebergDeletePlanTest + isRowIdInjectionTarget 半）;错误消息/结果形状/隐藏列名 pin 面全在连接器侧,删 fe-core 死副本不影响;勿动 StatementContext 的 rewriteSourceFilePaths/rewriteSharedTransaction（活替身）。
> **⏭ 该轮的"按设计 §8 TODO 动码"前三步已于 2026-07-04/05 完成（见顶部最新一轮），剩 4-7 步。**研究工作流归档在 session scratchpad `ice-dml/*.json`（易失）,关键结论已全部固化进设计文档。

---

# 🎯 上一轮（2026-07-04 夜）= **fe-core iceberg 移除全量分析完成（只分析未动码）：`plan-doc/fe-core-iceberg-removal-plan.md` v2 实证重写，用户已裁定 Q1-Q5**

> **做了什么**：39-agent 工作流（7 路清点 → 每个"可删"结论双镜头对抗反驳 + 12 条高风险死臂孪生抽查（12/12 COVERED）+ 8 存活集群移除路径设计（含 Trino 参照）+ 完备性复扫）。**v1 草稿记载失实已确认**（broker/fileio 并未删除）→ 计划文档全量重写为 v2。
> **关键更正/新发现**：① 属性簇翻闸后仍活（`PluginDrivenExternalCatalog:147` initPreExecutionAuthenticator → MetastoreProperties Type.ICEBERG，每个插件 iceberg 目录都跑）；② `IcebergAws*Properties` 非死码（`IcebergRestProperties.addGlueRestCatalogProperties:345-361` 在 initNormalizeAndCheckProps 链上活，REST signing-name=glue|s3tables）；③ fileio/ 有配置注入反射活路（HMS-iceberg `io-impl` 透传 + p2 测试在用）；④ `ExternalCatalog.buildDbForInit:972` case ICEBERG 回放边缘（翻闸前 InitDatabaseLog 回放会构造原生 db）——删实体类前须改；⑤ IcebergExternalCatalog 常量被 IcebergUtils:1876/IcebergMetadataOps/IcebergScanNode:197 活读，删 flavor 前须搬常量；⑥ 原生 rewrite/ + action/ 是死孪生（插件走中立 ConnectorRewriteDriver）可删；⑦ v1 漏了整条目录外 sink/executor 死车道（LogicalIcebergTableSink/MergeSink、planner 三 sink、四 executor、IcebergTransactionManager+factory 方法）。
> **依赖裁决**：iceberg-core 暂留（HMS-iceberg+DML 合成+vendored DeleteFileIndex 钉）；iceberg-aws/s3tables/s3-tables-catalog-for-iceberg 阶段二外科剥离后可摘；avro/s3-transfer-manager 保留（hudi/hadoop-aws 非 iceberg 消费者）。
> **用户已裁定（2026-07-04，详见计划文档 §8）**：Q1+Q2=A（接受 io-impl 极端配置失效：fileio/ 并入阶段一删、iceberg-aws 阶段二照常摘）；Q3=B（HMS-iceberg 随 hive 整体迁移，不建重定向接缝）；Q4=A（行级 DML 去 SDK 化现在做，设计先行）；Q5=继续只分析。
> **⏭ 该轮遗留的"下一步 = 行级 DML 去 SDK 化详细设计"已于 2026-07-04 深夜完成并改判（见顶部最新一轮）**。分析工作流归档（82 臂全清单等）在 session scratchpad `ice/*.json`（易失），关键结论已固化进计划文档。

---

# 🎯 上一轮（2026-07-04 续）= **第四刀已落地并推送：iceberg `d5541bbb384` + paimon parity `58dff8c2790`（已推 origin，待 CI 实证 kerberos case 转绿）**

> **第四刀实现**（对应下文分析）：
> - **iceberg `d5541bbb384`**：数据路径 = 对象级 FileIO seam（`wrapTableForScan`：resolveTable 后把 loaded 表 ops 包成
>   `IcebergAuthenticatedTableOperations`+`IcebergAuthenticatedFileIO`，factory 时刻 doAs 捕获 secured FS → 任意线程
>   newStream 复用，worker 池/streaming lazy 消费全覆盖）；sys 路径 = 线程级 `executeAuthenticated` 包
>   `planSystemTableScan` 全程**单一 scope**（`resolveSysTable` 内层 wrap 拆除；刻意不用对象级包装——FileScanTask 要
>   Java 序列化给 BE JNI，wrapper 带 authenticator 不可序列化）。新 UT `IcebergScanPlanProviderKerberosScanIoTest`
>   5 用例；两处修复点 mutation 逐一击杀；全模块 923 测 0 失败、checkstyle 0。
> - **paimon `58dff8c2790`**（parity，无 kerberos e2e 闸）：`planSplits` helper 包 `scan.plan().splits()`（sys 表同咽喉点）；
>   真表接线 UT + mutation 击杀；全模块 329 测 0 失败。
> - **3-lens 对抗审查（bundled SDK 字节码级）= 0 confirmed**：唯一 high（$partitions 经 worker 池逃逸 doAs）被驳回——
>   iceberg 1.10.1 `ParallelIterable` 任务推进只在消费线程，`newInputFile` factory 全在规划线程 doAs 内 eager 捕获
>   kerberos DFSClient，worker 只用已捕获 FS 开流（与写路径 CI 实证机制一致）。
> - **[FU-kerberos-scan-residual]（low/info，未修）**：① paimon `resolveScanTable` 的 `table.copy(scanOptions)`
>   time-travel 快照/schema 读落在两 wrap 之间；② paimon `buildSchemaEvolutionParam` 的 SchemaManager 读在 scope 外
>   （两者靠 paimon 非 UGI 键控 FS cache 的 wrapped-load 先行温热缓解；反面=冷首触会污染 cache）；③ iceberg
>   kerberos×REST-vended 互斥是配置性非结构性（畸形同配会丢 vended 凭据）；④ kerberos 下 MetricsReporter 退化为
>   default Logging（观测性）。
> - **推送状态**：`25cd9d9f242..58dff8c2790` 已推 `origin/catalog-spi-10-iceberg`（PR #64689）。**⚠️ 下一步 = 盯 CI**：
>   ① `test_iceberg_hadoop_catalog_kerberos` 转绿（第四刀唯一 e2e 实证）；② paimon 批量 case 应随 CI 环境恢复回绿
>   （若再挂，先对失败签名：rpc 超时/not alive = 环境延续，非代码）。

---

# 🎯 上一轮（2026-07-04）= **CI build 985672 失败分析：三刀实证生效；第四刀根因定位（已修，见上）**

> **① kerberos case（真代码问题，本 PR 相关）**：INSERT 已过（三刀 `8d352049394` **CI 实证生效**）；失败点前移到 INSERT 后
> `select count(1)` 的 **plan 期 manifest-list 读**——`PluginDrivenScanNode.getSplits → onPluginClassLoader(只 pin TCCL、无 doAs)
> → IcebergScanPlanProvider.planScan:299 → planScanInternal:498 → planCountPushdown:712 → SnapshotScan.planFiles
> → ManifestLists.read(snap-*.avro) → DFSClient.open → SASL "Client cannot authenticate via:[TOKEN, KERBEROS]"`
> （985672 归档 fe.log 01:22:54）。scan 路径从未包 executeAuthenticated——之前该 case 死在 INSERT、读路径从未被走到，
> 是**新暴露**非新引入。sys-table 路径（同文件 :1572/:1594 context.executeAuthenticated）已是既有范式；
> **master parity**：legacy `IcebergScanNode:477` `preExecutionAuthenticator.execute(() -> doGetSplits)`，
> streaming 臂 :510/:524 亦包。**修法（第四刀）**：`planScan` 两 overload + `streamingSplitEstimate` + `streamSplits`
> 包 `context.executeAuthenticated`；⚠️ `streamSplits` 返回的 `ConnectorSplitSource` 是 **lazy** 的——engine pump 线程
> 消费 planFiles iterator，须在 split source 的逐批拉取处也包 doAs（镜像 master :510/:524），只包 streamSplits 入口不够。
> UT 走 TcclPinningConnectorContextTest 同款接线断言（recording fake context）。paimon `PaimonScanPlanProvider.planScan`
> 同型缺口（无 kerberos e2e 闸）= parity 候选，随四刀一起或单独 commit。
>
> **② paimon 8 case + hive_text_write 大面积失败 = CI 环境宿主资源饥饿，非本 PR（勿逐 case 修）**：
> BE 内嵌 JVM GC Real≫User+Sys（Young GC 2s→30s 递增；GC(276) Real=218s/Sys=48s = 重度内存 reclaim/CPU 争抢）、
> BE JVM 连 KDC 都 SocketTimeout、brpc 8062 从 01:53 瘫（send fragments rpc timeout）、FE 02:03:26 心跳判 dead
> （BE 02:01:44 还在报 tablet=进程活着服务瘫）、02:26 流水线优雅重启 BE（be.out LSAN 泄漏报告=exit() 实证非 crash；
> dmesg 无 OOM-kill）。**跨 PR 同签名实锤**：64923/65175/64891/64924/64854/65031 六个别人的 PR 在**各自不同 agent**
> 上同窗口（07-03 晚 ~ 07-04 凌晨）挂同批 case（hive_text_write_insert + test_paimon_catalog/jdbc_catalog/... +
> mv external_table + load_p0 连带）；本 PR 984925（07-03 10:57）paimon 全绿。**与 `2a5a6aff2d3`（paimon parity doAs）、
> `c2d9631511c`（paimon reader-type）无关**（纯时间巧合；后者的 FORMAT_ORC file-scanner-v2 warning 只是 v1 回落噪音，
> legacy 同行为）。处置：等环境恢复 retrigger / 向流水线维护者（onemorechance）反馈证据。
>
> 分析证据/日志：985672 归档 tarball（GC log、be.out、fe.log、regression log 时间线）+ TeamCity REST 跨 build 对比。

---

# 🎯 上一轮（2026-07-03 深夜）= **kerberos INSERT 三刀收口：temp() 补包装 `8d352049394`（CI 已实证 INSERT 过）**

> **背景**：`test_iceberg_hadoop_catalog_kerberos` INSERT 连挂多轮。一刀 DDL doAs（`a46e420b871`，已生效——CI 里 DDL 全过）；
> 二刀 FileIO 包装（`ba7d04fc8d8`）**上车后 CI（build 985573）原样再挂**——本轮根因分析（字节码级）：
> `BaseTransaction.TransactionTableOperations.io()` 从不读传入 ops 的 io()，而是 `tempOps.io()`
> （`tempOps = ops.temp(current)`，每次中间 commit 重建）；而 `IcebergAuthenticatedTableOperations.temp()`
> 原样转发 delegate.temp() → hadoop catalog 下 `HadoopTableOperations$1.io()` 直通裸 HadoopFileIO →
> worker 池 manifest 写从未穿过 doAs（985452 归档 fe.log 实证：`iceberg-worker-pool-8` 上
> `DistributedFileSystem.create` SASL 拒）→ 二刀是 no-op。
> **本轮修复** `8d352049394`：`temp()` 改为 `new IcebergAuthenticatedTableOperations(delegate.temp(m), io)`。
> TDD 先红后绿（新 UT `IcebergAuthenticatedTableOperationsTest` 用 `Transactions.newTransaction(...).table().io()`
> 纯公开 API 复现绕过机制）；全套 918 测 0 失败、checkstyle 0。
> **⚠️ 待办**：推分支后**盯 External Regression 的该 case 转绿**（本地无 kerberos docker，e2e 只能 CI 实证）。
> **模式教训（第 2 次栽同型坑）**：iceberg 内部有自己的路由/线程池（第一次 worker-pool TCCL，这次 temp() 路由）——
> "边界包一层"式修复必须沿 iceberg 内部调用链（bytecode/源码）走到真正的消费点验证，别只验证包装层自身行为。

---

# 🎯 下一个 session 的任务 = **回主线（iceberg 翻闸：ENG-1 能力孪生审计）或补跑 meta-cache 翻闸回归**

> **本轮（2026-07-01）已完成 = 三个手写连接器缓存全部上共享缓存框架（独立复制策略收官）**。
> 设计+完成记录：**`plan-doc/tasks/designs/metacache-connector-port-design.md`**（Status: DONE）+
> 任务清单 `plan-doc/tasks/metacache-connector-port-tasklist.md`。
>
> **做了什么**：把 `IcebergLatestSnapshotCache` / `IcebergManifestCache` / `PaimonLatestSnapshotCache` 三个手写
> `ConcurrentHashMap` 缓存改成 `connector.cache.MetaCacheEntry` 的**薄适配器**（对外方法签名/值类型不变→调用方零改动，
> 手写 CHM 机制退役）。5 个独立 commit：
> - `24e4c830aeb` 框架副本 Caffeine `3.2.3→2.9.3`（**关键侦察修正**：独立复制下框架随各插件**子加载器**打包、链接
>   **各插件自带**的 Caffeine；iceberg 带 2.9.3、paimon **原本无 Caffeine**→按 2.9.3 编译 + 给 paimon pom 补 2.9.3 依赖）；
> - `0be2679a7ac` iceberg 最新快照适配器；`bc27505eace` iceberg manifest 适配器；`47c4bcc6fd9` paimon 适配器(+Caffeine 依赖)；
> - `808c0cb0f0c` 对抗审查发现的过时"共享单一 Class 身份"注释更正（纯文档）。
> - **适配器统一 flags**：`contextualOnly=true`、`manualMissLoadEnabled=true`（loader 在 Caffeine 锁外单飞，且使
>   `ttl<=0` 禁用路径为**确定性 bypass**，不靠 `maximumSize(0)` 异步淘汰）、`autoRefresh=false`、
>   `executor=ForkJoinPool.commonPool()`。**易错点已守**：`CacheSpec` 里 `-1`=永不过期，故适配器把连接器契约的
>   `ttl<=0`→翻译成 `ttl==0`(禁用)，专门加 `-1` 单测。
> - **验证**：iceberg+paimon **整模块测试套全绿**（0 fail）；三模块 checkstyle 0；import gate 我的文件净；
>   paimon 插件 zip 实测只含唯一可用 `caffeine-2.9.3.jar`（无版本冲突）。**3 视角对抗审查**（行为/框架 API/打包类
>   加载）+ 对抗核验：仅 1 条确认（纯文档、已修），其余全驳回。
>
> **⚠️ 仍 flip-gated 未跑（本轮无集群）**：`test_iceberg_table_meta_cache` / `test_paimon_table_meta_cache` 回归 +
> **重新部署后的类加载冒烟检查**（唯一能端到端证明"插件内 `MetaCacheEntry` 正确链接插件那份 Caffeine 子加载器"的
> 手段；单测只证逻辑）。**下个有集群的 session 起步补跑**。
>
> **⚠️ 大框架统一尚未收官**：本轮只迁完 iceberg/paimon 连接器；fe-core 老框架 `datasource.metacache` **仍原封不动**
> 服务 hive/hudi/doris/iceberg-on-HMS，**等所有连接器都迁完才能删 fe-core 那份**（远期）。
>
> **下一步（用户定优先级）** = ①（有集群时）补跑上面两个 meta-cache 翻闸回归 + 类加载冒烟；或 ②**回 iceberg 翻闸主线：
> ENG-1 能力孪生审计**（见下文 🎯 段）。meta-cache 迁移与翻闸主线正交,可独立推进。
>
> **⚠️ HMS import-gate 命中 = 误报,非违规（用户 2026-07-01 确认,非本任务）**：`fe-connector-hms/.../HiveMetaStoreClient.java` import `datasource.hive.HiveVersionUtil`（补丁版 HMS client）解析到的是 **fe-connector-hms 内 vendored 的同名自包含副本**（非 fe-core,该模块零 fe-core 依赖）→ **未破规则**；`check-connector-imports` 只是按包前缀 grep 误伤。**勿改连接器代码/重新暴露**。仅是 cache-clean reactor 构建/CI 的门禁噪音:`-Dexec.skip=true` 跳过 gate exec（`-pl <m>` 不带 -am 对叶子连接器不行——撞 `${revision}`）。详见 memory `catalog-spi-hms-hiveversionutil-gate-false-positive`。

---

# 🎯 （之后）下一个任务 = **flip-gated e2e 重跑（确认两 branch fix 绿）→ ENG-1 能力孪生审计**

> **本 session（2026-07-01）已完成并各自独立 commit 两个产品 bug**（设计 + 完成记录见 `plan-doc/tasks/designs/iceberg-branch-mvcc-and-static-partition-overwrite-fixes.md` 末尾 Status）：
> - **① complex_queries = 通用 MVCC 快照塌缩 → `de1af7a594e`**：`StatementContext.snapshots` 改按 (ctl,db,table,**版本**) 键化（`MvccTableInfo` 加 version；`loadSnapshots` versionKeyOf 键化；新增版本感知 `getSnapshot(TableIf,ts,sp)` + 版本盲智能回退 default→lone→empty；`MvccUtil` 重载；`PluginDrivenScanNode.pinMvccSnapshot` 改版本感知）。UT 5/5 + mutation 2/2 KILLED + checkstyle。**共享核心，已过 clean-room 3-agent 对抗审（key 稳定/无读者回归/未能 break，无 blocker）**。
> - **② partition_operations = 写路径丢静态分区字面量 → `98e00a14c37`**：新增中立能力位 `ConnectorCapability.SINK_MATERIALIZE_STATIC_PARTITION_VALUES`（iceberg 声明、MaxCompute 不声明）；`PluginDrivenExternalTable.materializeStaticPartitionValues()`；`BindSink.bindConnectorTableSink` full-schema 分支门控投影静态分区字面量（逐行镜像 legacy `bindIcebergTableSink:783-795`）。IcebergConnectorTest 断言 + mutation KILLED + checkstyle。镜像臂 = 焦点验证（非多 agent）。
> - **⚠️ e2e flip-gated 未跑**（本 session 无 live 集群/iceberg-docker）：**下个 session 起步 redeploy 后重跑 `iceberg_branch_complex_queries` + `iceberg_branch_partition_operations` 确认绿**（`tag_retention` 仍是 spark 容器环境，非代码）。
> - **follow-up 已登记**（设计文档末）：[FU-mvcc-mixed-schema]（同语句同表 schema 分歧→版本盲 base schema 取 main，pre-existing 单 schema 限制；SPI 分区裁剪恒列 latest）、[FU-connector-staticpart-validate]（通用 sink 缺 legacy 静态分区校验，应落连接器侧 fail-loud）。
>
> **⚠️ 仍未 commit 的前序工作（勿丢、勿与新工作混提交）**：工作区 `IcebergConnector.java` 仍含**前序 session 的 worker 池 TCCL 修复**（`pinIcebergWorkerPoolToPluginClassLoader`+barrier，已 redeploy 实证）+ 新文件 `IcebergConnectorWorkerPoolPinTest.java`/`TcclPinningConnectorContext.java`+其测试 + `iceberg_branch_tag_edge_cases.groovy` 文案对齐。本 session commit 两 fix 时用 `git apply --cached` 单 hunk 隔离 `IcebergConnector.java`、**未触碰这些前序改动（仍 uncommitted）**。worker 池经验在 memory `catalog-spi-plugin-tccl-classloader-gotcha`（第三 locus）。
>
> **⚠️ 本 session（2026-07-01 后半）另做了一个 meta-cache 属性校验修复（未 commit，待用户裁量）**：`test_iceberg_table_meta_cache` 失败——SPI 切换丢了 `ttl-second=-2` 校验（`CacheSpec.checkLongProperty`），新连接器只保留 best-effort 解析。修复=把 `CacheSpec` 表达模式落一份到 **fe-connector-api**（`org.apache.doris.connector.api.cache.CacheSpec`，校验改抛 `IllegalArgumentException`→fe-core `checkProperties` 原样包成 DdlException），iceberg（6 knob）+ paimon（3 knob）的 `validateProperties` 接回校验（字节对齐 legacy `IcebergExternalCatalog`/已删 `PaimonExternalCatalog` 的 checkProperties）；Phase 2 把 iceberg manifest 的手写 `isCacheEnabled`/`propLong`/`getLong` 改用共享 CacheSpec。设计+完成记录 = `plan-doc/tasks/designs/metacache-connector-cachespec-design.md`。**已 unit 全绿（api CacheSpecTest 9/9、Iceberg/Paimon ValidatePropertiesTest 11/15、iceberg 全模块 892/0-fail），checkstyle 净，import gate 我的文件净；docker 两个 meta_cache 回归未跑（无集群）**。**⚠️ staging 坑**：`IcebergScanPlanProvider.java` 有前序未提交 hunk（~L991/L1003，非本任务），我的 Phase 2 改动同文件——独立提交需 `git apply --cached` 单 hunk 隔离。paimon 单测 `deadTableCacheKeyIsAcceptedNotRejected`（断言 dead knob 不校验）已按恢复指令翻成 `rejectsMalformedMetaCacheKnob`。
>
> **⚠️ 上述校验修复之后，用户把范围扩大为「整套 metacache 框架统一」**（设计文档 `plan-doc/tasks/designs/metacache-framework-unification-design.md`）：三个连接器手抄 cache（`IcebergManifestCache`/`IcebergLatestSnapshotCache`/`PaimonLatestSnapshotCache`）都是 fe-core 框架 entry 的移植；native iceberg/paimon 的 fe-core `IcebergExternalMetaCache`（含 manifestEntry）**已死**（只 HMS-iceberg 还活），paimon 连 fe-core cache 都没有。**用户已定：Option A**（框架搬到新模块 `fe-connector-cache`，连接器自持 cache）+ 新建 `fe-connector-cache` 模块。关键：`org.apache.doris.connector.*` 是 **parent-first**（`ConnectorPluginManager:64`），框架搬那儿=单 app-loader 身份；`MetaCacheEntry` 对外 API 无 Caffeine 类型 → split-brain 可规避（安全红线：`CacheFactory`/Caffeine 类型不得越界给连接器）。**P1 已完成 skeleton**：建 `fe-connector-cache`（pom caffeine **provided 3.2.3**、注册进 aggregator、fe-core 加依赖、package-info），`-pl fe-connector/fe-connector-cache install` 直接构建 **SUCCESS**。**P1 剩余搬类步骤见设计文档 §8**（CacheSpec 三份合一→其余 leaf→CacheFactory+MetaCacheEntry 改 Config 为 ctor 注入→8 个 plugin-zip 加排除）。
> **⚠️⚠️ 新暴露的预存在 gate blocker（非本任务引入，但挡住 P1 reactor 构建 + 会挂 CI）**：加新模块使 aggregator 的 build-cache 失效→`check-connector-imports` gate 重跑并 **FAIL**（Phase1/2 只是命中了缓存的 pass）。唯一违规是 commit `4acb5f91e1a` 的 `fe-connector-hms/.../HiveMetaStoreClient.java:21-22` import `datasource.hive.HiveVersionUtil`（补丁版 HMS client）。**需决定**：给该补丁 client 加 gate allowlist，或把 HiveVersionUtil 换个连接器可见的暴露方式。临时绕过：`-pl <module>` 不带 `-am` 单模块构建。
>
> **之后 = ENG-1 能力孪生审计**（全部 Medium M-1..M-11 已 ✅），详见下文：

# 🎯 （之后）= **ENG-1 能力孪生审计（全部 Medium M-* 已 ✅；翻闸 BLOCKED，先修后翻）**

> **进度**：P0（B-1/B-2）+ 全部关键 P1（H-1..H-10）+ **全部 Medium（M-1..M-11）已全 ✅**——本轮收尾 **M-9 `0d8c5669f9b`**（dropDb 改用 REMOTE 名，镜像 dropTable）/ **M-11 `177f84a7ac9`**（FORCE 删恢复容忍远端已删 namespace，方案 B 含 HMS loadNamespaceLocation 步）/ **M-8 决定=接受偏离不改码**（保留省略空 LOCATION 的 cleaner 输出，用户 2026-06-30 裁定）。逐条状态/commit 见**任务清单 §1–§3** + `git log`（HANDOFF 不再累积「修完成」条目）。
>
> **⏭ 之后（两个 branch fix 完成后）= ENG-1 能力孪生审计**（全部 Medium 已 ✅）：
> - **入口**：任务清单 **§5 ENG-1** + review 报告 **§七**（残留旧逻辑 / 能力门控）。**全部 Medium M-1..M-11 ☑**（本轮收尾 M-9 `0d8c5669f9b` / M-11 `177f84a7ac9` / M-8 决定=接受偏离不改码；M-10+H-11 ☑ 已并入 B-2 `ba80cfb0439`）。
> - **ENG-1 = 全量审计 legacy iceberg `instanceof Iceberg*` 臂的能力孪生覆盖**：翻闸后运行时类型 `PluginDriven*`，所有 `instanceof IcebergExternalTable/Catalog/Sys` 求值 false，正确性逐点依赖人工写的「能力孪生臂」；**H-10（嵌套裁剪）是已实证一次漏写=静默回归**。需逐个 legacy iceberg 臂核对是否有等价 PluginDriven 臂/能力门控——**防「逐点静默回归」的唯一保证**。
> - **处理顺序**：**iceberg branch_tag 两个 fix（complex_queries + partition_operations）◀ 下一（用户 07-01 指定）** → ENG-1 → P3(L-BATCH) → ENG-3 flip-gated e2e 全跑 → 用户二签翻闸。（⚠️ 任务清单 §8 顺序已过时，以此为准。）
> - **每条走 step-by-step-fix**（recon→design→impl→test→clean-room→**独立 commit**→回填任务清单）。**⚠️ 认领前先 recon+`git show master:` 重裁，HANDOFF/review 行号/不变式可能过时（信控制流不信注释）**；冲突项回代码重裁（Rule 7）。
> - **⚠️ M-3 引入新中立 SPI（`ConnectorSplitSource` + `streamingSplitEstimate`/`streamSplits`）= 流式 split 通道**：将来 Hive/Hudi 迁插件路径可复用（file-count 流式是它们共用老套路）。**v3 iceberg 暂闸出流式**（commit-bridge delete stash 写规划点读，流式懒填太晚→复活已删行）；放开 v3 需先设计 plan-time stash barrier（登记 follow-up）。

> **⚠️ 为何 BLOCKED（2026-06-28）**：一轮 clean-room 对抗 review 推翻了「翻闸代码基本完成」的旧结论——发现 **2 blocker + 11 high + 11 medium + 25 low + 18 info**，blocker/high 密集覆盖写入、MTMV、统计、time-travel、缓存一致性等核心路径。**翻闸代码侧写完了但不正确**：P0+关键 P1 现已逐条修完，但仍需关 Medium、跑 ENG-1 审计与 flip-gated e2e 才能二签翻闸。

> **📋 任务跟踪入口（下个 session 必先读）**：
> 1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`** ← **master checkbox 任务清单**，逐条 ID 对齐 review 报告（B-1/B-2/H-1..H-11/M-1..M-11/L-BATCH/ENG-1..4）。**每条任务的状态、位置、修法、验收、依赖、⚠️RECONCILE 标记都在这里。逐步处理 = 按此表逐条 ☐→◐→☑。**
> 2. **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`** ← 完整证据源（每条发现的 file:line、vs master 差异、真回归 vs 内生缺陷、验证者保留意见；**Medium 见 §四**）。

---

# 🔑 翻闸现状 = **代码侧写完、P0+关键 P1 已修；翻闸 BLOCKED（待 Medium + ENG-1 + e2e + 二签）**

- **路由翻闸已在分支**（`18e1b297d7e`）：`SPI_READY_TYPES` 含 `"iceberg"`，建/重放 iceberg catalog 走 `PluginDrivenExternalCatalog`；连接器 ServiceLoader 注册 + plugin-zip 打包齐备。**⚠️ 这意味着 review 所有"this path is live"成立，in-code 的 "dormant / not yet in SPI_READY_TYPES" 注释普遍已过时（false claims）——动码时勿信注释，信控制流。**
- **GSON 兼容迁移已在分支**（`e68eb5c00c9`）：旧 8 catalog 变体 + db + table 标签 `registerCompatibleSubtype`→PluginDriven（table→Mvcc 变体）+ 删 CatalogFactory legacy case。保升级老集群（全新/docker 零影响）。**review §六确认完整且写安全（正面）。**
- **未 push、未二签**：路由翻闸 + GSON 迁移**必须一起 push**（[DEC-FLIP-1] 铁律），但**当前不应 push**——先修完 review 发现。

## ⛔ 翻闸 gate（全绿才能二签翻闸最后原子提交）
1. **P0 全清** ✅：B-1（云存储写 fs.s3a.* vs AWS_*）+ B-2（MTMV listPartitions 缺）。
2. **关键 P1 关** ✅：H-1..H-10（破坏主力部署的回归）逐条修完，详见任务清单 §2。
3. **ENG-1**：能力孪生全量审计 **✅ 已完成**（`fadf844f44c`，16 条确认缺口）；**F1 已修**（`6e14fecc21b`）；**本批 5 条 low 已修完**（`cd7618ef53e`/`c8b39f871e3`/`50e4a6bcb5d`/`50ad635d9b0`/`bc5c39157aa`，统一对抗 review 已过，见顶部 🎯 段 + 任务清单 §5b）；**仅剩连通性 F2/F3/F15/F16（medium，opt-in）= 下个 session 首任务**。
4. **ENG-3**：flip-gated e2e 全套实跑（DV/V3/MTMV/time-travel branch/vended 写/Kerberized HDFS/rewrite）。
5. **用户二签**。
> Medium `M-*`（P2，「翻闸窗口或紧随其后」）= 用户重排的**下一步工作**（见顶部 ⏭），非严格 flip-gate；逐条见任务清单 §3。

---

# ⚖️ 关键决策（沿用，用户已签）

## [DEC-FLIP-1] 持久化 GSON 迁移 = 方向 A（已落地 `e68eb5c00c9`）
> **⚠️ 推送顺序铁律不变**：路由翻闸（`18e1b297d7e`）与 GSON 迁移（`e68eb5c00c9`）**必须一起 push/上线**。单 push 路由翻闸而漏 GSON 迁移到会被升级的老集群 → 老 iceberg 镜像反序列化崩。**但当前两者都不应 push——先修完 review 发现，翻闸做成最后一个原子提交（路由+GSON 已在前序 commit，最后补齐 fix + e2e + 二签）。**

## [视图范围] = parity only（B0/B1/B2/B3 全 DONE）
查询 B1 / DROP+删库级联 B2 / SHOW CREATE B3 / 中立地基 B0 全完。CREATE/RENAME VIEW 出范围（fail-loud）。翻闸后视图 schema 回归 = H-8（已修，见任务清单 §2）；视图面残留 low（L-17/L-18/L-19/L-20 文案/缓存）随 P3。

## [REVIEW 纪律] clean-room，不注入先验（本轮已执行）
本轮 review 刻意不注入开发先验（忽略 plan-doc/注释/commit message）。**后果：部分发现与历史记忆冲突**（最突出=M-10 SHOW PARTITIONS：本轮判真回归 vs 旧记忆判"误报死码翻闸反改善"，已裁定 M-10 正确并随 B-2 修）。**认领冲突项时回代码 + `git show master:` 重裁，不盲信任一方（Rule 7）。**

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI / ConnectorCapability。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog` + `ShowCreateDatabaseCommand`/`Env.getDdlStmt` legacy iceberg 臂 + `BindRelation case ICEBERG_EXTERNAL_TABLE` + `ShowCreateTableCommand` legacy ICEBERG 视图臂 + `InsertUtils` 既有 `UnboundIcebergTableSink` 分支）。
> **修 Medium 发现时尤其注意**：若需新增门控走 `ConnectorCapability` / 中立 SPI 而非 instanceof / 引擎名（例：M-7 DLF 守护；M-4 字段编号链路可参照已完成的 H-10）。

---

# 🟡 已登记 follow-up（部分已并入任务清单）
- **[FU-forcedrop-nosuchns]** = 任务清单 **M-11**（pre-existing）：**namespace 级已修** `177f84a7ac9`（FORCE 删恢复 catch NoSuchNamespaceException，含 HMS loadNamespaceLocation 步=方案 B）；**per-table 级残留 partial**——连接器 `dropTable` seam 缺 master 的 `tableExist`+ifExists 守护，per-table NoSuchTableException 仍不容忍（但 master 亦不经 catch 容忍它→出范围）。
- **[FU-flip-e2e]** = 任务清单 **ENG-3**（真翻闸端到端未跑）。
- **[FU-rewrite-output-sizing]（R6/R8）** 中立 driver 未线程 target-file-size + 自适应并行度（与已完成 H-9 同文件族）。
- **[FU-paimon-topn-dict]（low，M-4 clean-room 两 reader 独立发现，非 M-4 回归，出范围）** = 迁移后 paimon `PaimonScanPlanProvider.buildSchemaEvolutionParam` 的 `-1` 当前 schema 条目按**裁剪列**建（legacy paimon 恒全列），与 iceberg M-4 同型潜在 Top-N 懒物化缺口；但 paimon 另发**每 committed schema-id 的全列 history 条目**（iceberg 只发单 `-1`），其 topn 安全性（若有）或赖于此 → **需独立验证**（确认 paimon BE 按 row-id 回表补取是否经那些全列 history 条目解析、是否真有 wrong-rows）。若确认有缺口，可复用 M-4 的 `applyTopnLazyMaterialization` SPI（paimon 覆写即可）。
- **[FU-h10-deadcode]（LOW，cosmetic/非正确性）** 两条翻闸后死码，留 ENG-1/cleanup：① `PlanNode.mergeIcebergAccessPathsWithId`（`instanceof IcebergScanNode` EXPLAIN 访问路径合并臂，翻闸后只显示 `name` 非 `name(id)`，BE 仍收编号形路径）；② `LogicalFileScan.supportPruneNestedColumn` 的 legacy `IcebergExternalTable||IcebergSysExternalTable→return true` 臂（与 L2 现已不一致、仅反翻闸成隐患）。
- **[FU-view-gson-roundtrip] / [FU-view-exception-arms] / [FU-getsqldialect-deadcode] / [FU-showcreatedb-render-ut] / [FU-createtablelike-plugin]**（低）见 git log 历史 + 任务清单 L-BATCH。
- 其余（nested-nullability / where-literal-coercion / broker-write〔=M-5〕/ doris-version-prop〔=L-13〕等）多已被 review 重新发现并归入任务清单。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**（HiveConf 来自 optional shade，`test-compile` 假错）。**iceberg/api** 正常 `-am test`。
- **⚠️ checkstyle 别加 `-am`**：`-am` 把 `fe-common`（2381 既存 error）拖进假红 → `mvn -pl :<art> checkstyle:check`（不带 -am）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` ~590000ms 或后台跑（全模块 ~2min）。
- **⚠️ maven 经管道 `$?` 是管道尾的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`；`-q` 抑制 console → 读 surefire **XML** 的 `tests=`/`failures=`。
- **⚠️ stale .class 假红坑**：mutation 后 `os.utime`；**commit 前最终验证务必 fresh recompile**。
- **⚠️ fe-connector-iceberg 全模块测试套有预存在 flaky 污染（M-2 期间实证，非任一 fix 引入）**：跑全 849 测时偶发 3 个 field-id/能力测试红（`IcebergConnectorTest.declaresNestedColumnPruneCapability`、`IcebergTypeMappingReadTest.nestedFieldIdsCarriedForBeFieldIdScan`、`IcebergConnectorMetadataTest.getTableSchemaParsesColumnsFromLoadedTable`——field-id 读 -1 / 能力读 false），**取决于 surefire 类执行顺序**（顺序相关共享静态态污染）；三类**单独跑全绿**、stash 改动后**clean tree 同样偶发**——即非确定性、非改动引入。另 `fe-connector-metastore-iceberg` 的 `IcebergMetaStoreProvidersDispatchTest` 亦预存在 flaky（clean tree 也红，且它是 iceberg 的 -am 上游→其红会 skip 整个 iceberg 测试致 XML stale；隔离验证可加 `-Dtest='!IcebergMetaStoreProvidersDispatchTest'`）。**判 iceberg fix 是否破测勿信单次全量红**：① 单独跑相关类；② stash 后对比 clean tree。建议归 ENG（测试隔离修复），非单点 fix 范围。
- **⚠️ 后台 task 通知的 "exit code" 是末尾 echo/df 的、非 maven 的**（M-2 又踩一次）：读 LOG 里 `MAVEN_EXIT=`/`BUILD` 行或 surefire XML，别信通知的 exit 0。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**连接器测试无 Mockito**（真 InMemoryCatalog/Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField` + stub `getConnector`/`getMetadata`/`buildConnectorSession`）。**⚠️ Mockito `anyString()` 不匹配 null**。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_*.py`（单行 exact-string 锚点 count==1 守；KILLED=maven rc!=0）。**⚠️ Python 3.6**：`subprocess.run(stdout=PIPE,stderr=STDOUT,universal_newlines=True)`（无 `capture_output`）。**⚠️ review（读源）与 mutation（改源）务必串行**。
- **cwd 会被 harness 重置** → 一律绝对路径。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# ⚠️ Commit 须知（任何 `git add` 前必读）
- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`)。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。
- **每条 fix = 独立 commit**（沿用 P4-T06e-FIX-* 范式）；HANDOFF + 任务清单 + 设计文档单独 commit（memory 在 `.claude/`、非仓内）。

# 📦 阶段状态
- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **进度**：P6.1–P6.5 ✅ / P6.6 C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness 只读退化 ✅ / 视图 B0–B3 ✅ / 路由翻闸 `18e1b297d7e` ✅ / GSON 迁移 `e68eb5c00c9` ✅ → **⛔ 现卡在 clean-room review 发现修复**：**P0（B-1/B-2）+ 关键 P1（H-1..H-10）全 ✅**（逐条 commit 见任务清单 §1–§2 + `git log`）→ **全部 Medium（M-1..M-11）✅**（收尾 M-9 `0d8c5669f9b` / M-11 `177f84a7ac9` / M-8 决定接受偏离不改码） → **iceberg branch_tag 两个 fix（complex_queries MVCC + partition_operations 静态分区 overwrite）◀ 下一（用户 07-01 指定，见顶部 🎯 + 设计文档）** → ENG-1 能力孪生审计 → P3(L-BATCH) → ENG-3 flip-gated e2e → 二签翻闸。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部未 push**（含路由翻闸 + GSON 迁移 + 视图 + C4/C5 + 全部 review fix）。**先修 review 发现，勿 push 半成品翻闸。** 留用户裁量。
- **⚠️ 分支 2026-06-28 被 rebase**：commit 哈希全重写，本文档/旧 commit message 旧哈希以 `git log` 为准。

# 🧠 给下一个 agent 的 meta
- **逐步处理 = 按任务清单逐条**：每条 Medium (M-*) 走 step-by-step-fix（recon→design 文档 `designs/P6.6-FIX-M<n>-<slug>-design.md`→impl→test+mutation→clean-room review→独立 commit→回填任务清单状态）。
- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/review/设计的依赖名/行号/不变式可能过时** —— 动码前先 recon（grep+实证）再信文档。**翻闸已生效 → in-code "dormant" 注释普遍过时，信控制流不信注释。**
- **⚠️ 冲突优先暴露（Rule 7）**：review 与历史记忆冲突项（M-10 等）回代码重裁，不盲信任一方。`git show master:` 是 legacy 原逻辑的权威来源（工作区 `datasource/iceberg/**` 是迁移后残壳，不可信）。
- **clean-room 对抗 review 偏好**：moderate+ 改动 = 多 reader 对抗 + critic（review 读源与 mutation 改源不可并发）。verbatim 镜像臂则焦点验证即可。
- **flip-gated 诚实**：真 post-flip 写/MTMV/time-travel e2e 翻闸后才能跑——**每条 fix 验收的 e2e 项标注 flip-gated 未跑，勿谎称已验**（Rule 12）。
- **上下文超 30% 即交接**。

## 📖 起步必读
0. **顶部 🚀 段（下个 session 任务）** + **`plan-doc/reviews/P6.6-ENG1-capability-twin-audit-2026-07-04.md`** §三（本批修法信源，逐条 master 行为/缺口/failureScenario/建议修法方向）+ 任务清单 **§5b**（16 条缺口状态表）。**用户裁定：直接照审计结论动码、不再 recon，末尾统一 review。**
1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`**（master 任务清单）+ **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`**（证据源，Medium 见 §四）。
2. memory（仅列现存相关项）：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`memory-keep-only-general-or-requested`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-plugindriven-no-source-specific-code`、`catalog-spi-connector-session-tz-gotcha`、`catalog-spi-be-java-ext-shared-classpath`、`catalog-spi-h9-rewrite-where-rewrite-mode-done`。
3. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（C 类 docker 清单 + 翻闸开关/持久化全景）。
