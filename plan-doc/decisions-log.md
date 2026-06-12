# 决策日志（ADR）

> **Append-only**：新决策置顶；旧决策永不删除（即使被推翻，也只标"已废止"而不删除）。
> 编号规则：`D-NNN` 三位数字，从 001 起单调递增，永不复用。
> 历史决策 D1-D12（master plan §5）+ U1-U6（RFC §16.2）已迁入并映射到 D-001..D-018。
> 与"偏差"的区别见 [README §3.1](./README.md)。
>
> 每条决策模板见文末 §附录。

---

## 📋 索引

> 时间倒序；带 ✅ 表示生效中，❌ 表示已废止，🟡 表示待评审

| 编号 | 别名 | 简述 | 日期 | 状态 |
|---|---|---|---|---|
| D-054 | P5-fix#8 | **FIX-COUNT-PUSHDOWN（M-2，round-2 MAJOR/round-1 MINOR，perf-parity）= fix-now + 新增 default `planScan` 7-arg overload 携 `boolean countPushdown` + 连接器 collapse-to-one（用户签字，2026-06-12）**：翻闸后 plugin-driven paimon `COUNT(*)` **结果正确但慢**——COUNT 枚举已达 BE（`FileScanNode.toThrift:90` 发 `pushDownAggNoGroupingOp`、`PhysicalPlanTranslator:873` 在 plugin 节点设 COUNT、未排除）且 per-range emit 缝**已建全**（`PaimonScanRange.Builder.rowCount`→`paimon.row_count`→`setTableLevelRowCount`，与 legacy `PaimonScanNode:303-308` byte-一致），唯独**信号+计算**缺：merged count `DataSplit.mergedRowCount()` 是 paimon-SDK-only 须连接器算，而 COUNT 信号 `getPushDownAggNoGroupingOp()==COUNT` 只在 fe-core 节点、`PluginDrivenScanNode.getSplits` 从不读（grep 0）也不在任何 `planScan`/`ConnectorSession`/`ConnectorContext`/handle → 连接器每 split 发 `table_level_row_count=-1` → BE 物化全 post-merge 行去 count（`file_scanner.cpp:1298-1326`），PK/MOR merge 表尤贵。**故非纯连接器（更正动手前 framing）**：信号须过 SPI 边界。**否决经 `ConnectorSession` 穿**（FIX-FORCE-JNI 先例）——agg-op 是 per-query planner 输出非 SET-var，会成静默无类型通道（本项目反复踩的 bug 类）。**用户定（vs defer）= fix-now**，且 **count-split 形状 = 连接器 collapse-to-one**（vs full-parity fe-core trim / vs per-split）。**修=3 文件**：① SPI `ConnectorScanPlanProvider` +1 **default** 7-arg `planScan(...,boolean countPushdown)` 委托 6-arg（镜像 limit/requiredPartitions 扩展链，其余连接器零改 no-op）[E15]；② fe-core `PluginDrivenScanNode.getSplits` 读 `getPushDownAggNoGroupingOp()==TPushAggOp.COUNT` 传入（**无 post-loop 数学**）；③ 连接器抽 `planScanInternal(...,countPushdown)`（4-arg 委托 false、7-arg 委托 flag）+ count 短路（**第一 routing 臂**，count-eligible split 不再发数据 range，否则 BE 双计 vs DV/PK-merge）：累加全 eligible split 的 `mergedRowCount` 入 `countSum`、留首个为代表、循环后发**一** JNI count range 携 `countSum`（=legacy `<=10000` singletonList+assignCountToSplits 收一 split case）；无 merged count 的 split 走常规 native/JNI 路 BE 自计（footer/物化）。两新成员=纯静态 `isCountPushdownSplit(boolean,DataSplit)`（mutation-test 路由闸）+ `buildCountRange`。**参数形状 `boolean`**（BE 只需 COUNT-vs-not、`TPushAggOp` 过度泛化）+ **paimon-only**=工程判断（未被否）。legacy `>10000` 并行 split trim **有意丢**（连接器无 numBackends，fe-core-only）= perf-only 偏差 [DV-032]。守门：连接器 252/0/0(1 CI-gated skip)、fe-core compile+checkstyle 0、import-gate 净、**fail-before 恰 2 新测红**（neuter `isCountPushdownSplit`→false）其余 33 绿、end-to-end 真 local PK 表测断言 collapse-to-one 携 merged total(2)。真值闸=live-e2e BE CountReader 选择/EXPLAIN（既有 legacy paimon count regression 覆盖 BE 契约、本 fix 无 BE 改）。设计 [`P5-fix-COUNT-PUSHDOWN-design.md`](./tasks/designs/P5-fix-COUNT-PUSHDOWN-design.md) | 2026-06-12 | ✅ |
| D-053 | P5-fix#6 | **FIX-KERBEROS-DOAS / M-8（MAJOR，Kerberos-only，fe-core，filesystem+jdbc）= fix-now（用户签字，2026-06-11）**：翻闸后 filesystem/jdbc flavor 在 Kerberized HDFS 上丢 UGI `doAs`——连接器 `PaimonConnector.createCatalog` 已把建 catalog 包进 `context.executeAuthenticated`(:194)，但其背后 authenticator 对这两 flavor 是**基类 no-op**：HDFS `HadoopExecutionAuthenticator` 仅在 `initializeCatalog()` 内构建（`PaimonFileSystemMetaStoreProperties:46`/`PaimonJdbcMetaStoreProperties:120`），而 `initializeCatalog` 在翻闸路径**死代码**（唯一 live 调用方=legacy `PaimonExternalCatalog:147`；plugin 路径经 `PaimonCatalogFactory` 自建 catalog）→ `PluginDrivenExternalCatalog.initPreExecutionAuthenticator:130` 读到 `AbstractPaimonProperties:45` 的 no-op → `executeAuthenticated` 不 doAs。HMS 不受影响（authenticator 在 `initNormalizeAndCheckProps:70` 即建、必跑）。**作用域=filesystem+jdbc only**（用户签）：DLF/REST 排除——`PaimonAliyunDLFMetaStoreProperties` 从不设 authenticator、用 Aliyun AK/SK/STS 入 HiveConf 非 Kerberos UGI（无 doAs 可丢），故 review「DLF」从句 **overstated**；HMS 已对。**修=fe-core，零连接器改/零连接器-SPI**：新 fe-core hook `MetastoreProperties.initExecutionAuthenticator(List<StorageProperties>)`（default no-op）由 `PluginDrivenExternalCatalog.initPreExecutionAuthenticator` 用**已安全建好**的 `catalogProperty.getOrderedStoragePropertiesList()` 调（catalog-init 时机、与 legacy 同、避免每次 `MetastoreProperties.create` eager 重复 kerberos login）；filesystem/jdbc override 之经 `AbstractPaimonProperties.initHdfsExecutionAuthenticator` 共享 helper 从 HDFS `StorageProperties` 建 authenticator（镜像 HMS）。**FE-unit 可测 wiring**（断言 `getExecutionAuthenticator()` 返 `HadoopExecutionAuthenticator`、不调 initializeCatalog），**真 doAs 端到端=live-Kerberos-e2e only**（无 paimon-kerberos regression 套件，[DV-031](./deviations-log.md)）。守门：fe-core `Paimon{FileSystem,Jdbc}MetaStorePropertiesTest` 14/0/0、fail-before 双 red（no-op `AbstractPaimonProperties$1`）、checkstyle 0。设计 [`P5-fix-KERBEROS-DOAS-design.md`](./tasks/designs/P5-fix-KERBEROS-DOAS-design.md) | 2026-06-11 | ✅ |
| D-052 | P5-fix#6 | **FIX-KERBEROS-DOAS / M-11（MAJOR，Kerberos-HMS）= full legacy parity 包全部 read RPC（用户签字，2026-06-11，取代 D7=B 的 read 从句）**：翻闸后连接器 metadata **读** RPC（listDatabases/getDatabase/listTables/getTable[getTableHandle+getSysTableHandle+resolveTable]/listPartitions）**不**包 `executeAuthenticated`，仅 4 个 DDL op 包（B3 **D7=B** 故意 read-vs-DDL 不对称、把 read-path doAs 推给 live-e2e 门）→ Kerberos HMS 上 SHOW PARTITIONS/MTMV/partitions-TVF + 任何 getTable 读 RPC 跑在 catalog principal 之外。legacy `PaimonMetadataOps`/`PaimonExternalCatalog` 包**每个** read（`getPaimonPartitions:99`、`getPaimonTable:137`、listDatabases/listTables/getDatabase）。**用户定 = full legacy parity（vs 仅包 listPartitions / vs defer）**：仅包 listPartitions 是半吊子（连分区路径自身先行的 getTable reload 都漏）；defer 则须登 accepted-deviation。本签字**取代 D7=B 的 read-path 从句**（4 DDL op 仍包）。**修=连接器 only、零 SPI**：7 处 read site 包 `context.executeAuthenticated`，其中 `resolveTable`（metadata + scan 双 site）一处包覆盖所有 resolveTable 调用方（DRY）。**异常流关键**：Kerberos `UGI.doAs` 把抛出的 checked `Catalog.{Table,Database}NotExistException` 包成 `UndeclaredThrowableException`（仅 IOException/RuntimeException/Error 透传）→ 故 domain 异常**必须在 lambda 内**捕获（镜像 legacy `getPaimonPartitions:104`），listDatabases/resolveTable 的既有 catch-all 在外吸收。scan `resolveTable` 对 `context==null`（2-arg ctor 离线测）走直连，与同文件 `getScanNodeProperties` 既有 null-context 约定一致。守门：连接器模块 248/0/0(1 CI-gated skip)、新 `PaimonConnectorMetadataReadAuthTest` 12/0/0 + scan 2、fail-before 3 red（authCount/log-empty）、import-gate 净、checkstyle 0。真值闸=live Kerberos HMS e2e（CI-gated、无套件，[DV-031](./deviations-log.md)）。设计 [`P5-fix-KERBEROS-DOAS-design.md`](./tasks/designs/P5-fix-KERBEROS-DOAS-design.md) | 2026-06-11 | ✅ |
| D-051 | P5-fix#5 | **FIX-MAPPING-FLAG-KEYS（M-crit MAJOR，纯连接器/FE-wiring，无 BE/无 SPI）作用域 = paimon-only 修 + hive/iceberg 跨连接器 follow-up（用户签字，2026-06-11）**：翻闸后 paimon 连接器类型映射两开关**静默失效**——连接器读**下划线**键 `enable_mapping_binary_as_varbinary`/`enable_mapping_timestamp_tz`（`PaimonConnectorProperties:39,42`→`PaimonConnectorMetadata.buildTypeMappingOptions`），但 fe-core 只写**点分**键 `enable.mapping.varbinary`/`enable.mapping.timestamp_tz`（`CatalogProperty:50,52`；`ExternalCatalog.setDefaultPropsIfMissing:302-306` 仅写点分键；`HIDDEN_PROPERTIES` 仅藏点分键）→ `PluginDrivenExternalCatalog.createConnectorFromProperties` 把**原始** catalog map 原样喂连接器 → `getOrDefault(下划线,"false")` 恒 false → 即便用户在 CREATE CATALOG 开启，BINARY 仍→STRING、LTZ 仍→DATETIMEV2（legacy `PaimonExternalTable:350` 读点分键并 honor → cutover 回归，flag 启用前 latent）。binary 键**双重漂移**（分隔符 `.`→`_` 且 token `varbinary`→`binary_as_varbinary`）→ 通用归一化器修不了。**M-crit 是 critic-surfaced 未过 3-lens → 先独立复核**（5-agent scout + 对抗 synthesizer workflow `wf_a3626c54-0db` → REAL_BUG high-conf，false-positive steelman 被否：原始 feature PR #57821/#59720、全 regression CREATE CATALOG（paimon/iceberg/hive/jdbc 皆点分）、legacy parity、同 SPI PR 迁移的 JDBC 连接器正确保点分 `JdbcConnectorProperties:66-67` 均证点分为 canonical）。**修（纯连接器、零 SPI/BE）**：`PaimonConnectorProperties` 两常量重指 canonical 点分键（binary 常量并改名 `ENABLE_MAPPING_VARBINARY` 对齐 CatalogProperty/JDBC/iceberg 约定，同修分隔符+token）+ 更新 `PaimonConnectorMetadata` 一处引用；`Options(mapBinaryToVarbinary,mapTimestampTz)` 顺序本就对、无逻辑改。**BE 一致性已核**：`PluginDrivenScanNode extends FileQueryScanNode` 不 override mapping getter → BE scan param 经继承的 `getEnableMappingVarbinary/Tz` 本就读点分键（`FileQueryScanNode:192-193,635-678`），故修连接器 FE 侧读后 FE 列型与 BE scan param 一致（修前两侧分歧）。**用户定 = paimon-only**（vs 一次修全 3 连接器）→ hive/iceberg 同根因登 [DV-030](./deviations-log.md) 跨连接器 follow-up（hive `enable_mapping_binary_as_string` 是误名非语义反转）。否决 fe-core 归一化器（blast 大/破 JDBC 已正确读点分/对 paimon 双重漂移不足）。守门：模块 234/0/0(1 CI-gated skip)、checkstyle 0、import-gate 净、fail-before bug-catcher 向红(期望 VARBINARY 实得 STRING)+guard 两态绿。真值闸=`test_paimon_catalog_{varbinary,timestamp_tz}.groovy`（CI-gated，enablePaimonTest=false+外部 fixture）。设计 [`P5-fix-MAPPING-FLAG-KEYS-design.md`](./tasks/designs/P5-fix-MAPPING-FLAG-KEYS-design.md) | 2026-06-11 | ✅ |
| D-050 | P5-fix#4 | **FIX-JDBC-DRIVER-URL（B-8a BLOCKER + B-8b 安全）作用域 = CREATE-time 校验 parity（用户签字，2026-06-11）**：JDBC flavor 连接器（a）`PaimonScanPlanProvider.getBackendPaimonOptions` 把 `driver_url` **裸**转发给 BE 且 `startsWith("jdbc.")` filter 丢 `paimon.jdbc.*` 别名 → BE `JdbcDriverUtils.registerDriver` 的 `new URL("mysql.jar")` 抛 `MalformedURLException`（B-8a 功能 BLOCKER）；（b）driver_url 无 format/allow-list/secure-path 校验 + stale「paimon 不在 SPI_READY_TYPES」注释（B7 后已假，`CatalogFactory:51` 含 paimon）（B-8b 安全）。**修 = 纯连接器、零新 SPI**（复用既有 `Connector.preCreateValidation` + `ConnectorValidationContext.validateAndResolveDriverPath`）：B-8a 在 `getBackendPaimonOptions` 用 `firstNonBlank(JDBC_DRIVER_URL)` 认两别名 + 抽出共享 `PaimonCatalogFactory.resolveDriverUrl`（FE 注册与 BE 选项同解析）发 canonical `jdbc.driver_url`(resolved)+`jdbc.driver_class`（镜像 legacy `PaimonJdbcMetaStoreProperties.getBackendPaimonOptions`）；B-8b override `PaimonConnector.preCreateValidation` 对 jdbc flavor 调 `validateAndResolveDriverPath`（镜像 `JdbcDorisConnector`）+ 删 stale 注释。**4-lens clean-room review 确认 B-8a + CREATE-time B-8b 正确**，揪出**校验仅 CREATE-time**（FE-restart reload/ALTER CATALOG/scan-time 不复校）= **pre-existing fe-core 缝、所有 plugin 连接器共有（含 JDBC 参考连接器）、默认配置 permissive 无可绕**，legacy 更强（每 scan 经 getFullDriverUrl 复校）。**用户定 = 接受 CREATE-time parity**（vs 扩到 fe-core ALTER hook + scan-time 校验 SPI——触 fe-core+全连接器+ALTER 可能触发 BE 连通测，非 surgical）→ 登 [DV-028](./deviations-log.md)（CREATE-time-only 校验 gap + 跨连接器 follow-up）+ [DV-029](./deviations-log.md)（简化 resolver + BE-side user/password/uri 别名 out-of-scope）。守门：模块 232/0/0、checkstyle 0、import-gate 净、fail-before 5/9 向红。真值闸=`test_paimon_jdbc_catalog`(CI-gated)。设计 [`P5-fix-JDBC-DRIVER-URL-design.md`](./tasks/designs/P5-fix-JDBC-DRIVER-URL-design.md) | 2026-06-11 | ✅ |
| D-049 | P5-fix#3 | **FIX-SCHEMA-EVOLUTION（B-1a BLOCKER + M-10 MAJOR）= Design C「连接器直建 thrift schema 字典」（用户签字，2026-06-11；M-10 deferred）**：翻闸后 native(ORC/Parquet) 读丢 paimon schema-evolution——连接器只发 per-file `TPaimonFileDesc.schema_id`、从不设 scan 级 `TFileScanRangeParams.current_schema_id`/`history_schema_info` → BE `table_schema_change_helper.h:219-237` 走 `!__isset` 分支退化**名匹配** → schema-evolved（改名/重排）表旧 schema 文件**静默错行/读 NULL**（JNI 路不受影响、native 是默认）。**关键事实**（5 层 trace + BE `table_schema_change_helper.cpp:312-430`）：field-id 匹配路 BE 只读 `TField.{id,name,type.type 当 nested-vs-scalar tag}`、**从不读 Doris Type 也不读 tuple descriptor** → 连接器可**直建** `TSchema`（`org.apache.doris.thrift.*` import-legal）、**无需 Doris Type/无新 SPI**；`Column.uniqueId`(M-10) 仅当 FE 从 Doris 列建 history 才有关、直接从 paimon `DataField.id()` 建则 B-1a 独立于 M-10。**用户定 = Design C（vs Design B「穿 ConnectorColumn/ConnectorType field-id + fe-core ExternalUtil 建」）**：连接器在 `getScanNodeProperties` 从 live(snapshot-pinned)表建 `current_schema_id=-1`+`history_schema_info`（-1 entry=pinned schema、外加每个 `SchemaManager.listAllIds()` 提交 schema）→ base64 thrift carrier prop 经既有 `populateScanLevelParams` SPI hook（复用 DV-006 同缝）落 params。**零新 SPI surface**（task-list 原标「SPI?=yes」修正为 no）、连接器局部、最小 blast radius；M-10（`Column.uniqueId=-1`）**deferred**（rereview2 §4 已证伪 standalone repro、无消费者，[DV-026](./deviations-log.md)）。**3-lens clean-room review 揪出 2 真 BLOCKER（均在 -1 entry，已修+复验 clean）**：① 列名 casing（BE verbatim key vs lowercase slot name + `current_schema_id=-1` 永不走 ConstNode 快路 → 大小写混合列即崩、连未演化表都回归）→ 修 = -1 entry **只** lowercase 顶层名（default-locale，byte-match slot 产出方+legacy `parseSchema:507`；嵌套 struct 名保 paimon-case=legacy `PaimonUtil:302` 非对称）；② 时间旅行（-1 entry 取 `schemaManager.latest()` 绝对最新、但 tuple 用 pinned schema → 改名列崩/错行）→ 修 = -1 entry 取 `((FileStoreTable)table).schema()`(pinned)、guard `DataTable`→`FileStoreTable`。MINOR（eager 读全 schema 无 cache）= 接受的 fail-loud 偏差 [DV-027](./deviations-log.md)。守门：模块 222/0/0(+5 schema-evo UT)、checkstyle 0、import-gate 净。真值闸=`test_paimon_full_schema_change.groovy`(CI-gated)。设计 [`P5-fix-SCHEMA-EVOLUTION-design.md`](./tasks/designs/P5-fix-SCHEMA-EVOLUTION-design.md) | 2026-06-11 | ✅ |
| D-048 | P5-fix#2 | **FIX-STATIC-CREDS-BE（B-9 BLOCKER）作用域 = full legacy-parity 替换（用户签字，2026-06-11）**：翻闸后 paimon 连接器 `PaimonScanPlanProvider.getScanNodeProperties:372-381` 把静态 catalog 凭据/配置（`s3.`/`oss.`/`cos.`/`obs.`/`hadoop.`/`fs.`/`dfs.`/`hive.` 前缀）裸拷进 `location.<rawkey>`，fe-core bridge `PluginDrivenScanNode.getLocationProperties` 只剥前缀不归一化 → BE native(FILE_S3) reader 只认 `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/`AWS_ENDPOINT`/`AWS_REGION`/`AWS_TOKEN`（`s3_util.cpp`）→ 私有桶 native 读拿不到凭据 403。是 review §9.3 凭据**第三道缝**（static→BE-scan，FIX-STORAGE-CREDS 修 catalog FileIO 缝、FIX-REST-VENDED 修 vended 缝，本缝两轮均漏）。legacy `PaimonScanNode.getLocationProperties:650-652` 仅返回 `CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap)`（canonical map）。**用户定 = 方案 A（full legacy-parity，非窄 object-store-only）**：新 SPI `ConnectorContext.getBackendStorageProperties()`（default 空，仅 paimon 调）= 引擎用 #1 已接线的 `storagePropertiesSupplier`（`catalogProperty.getStoragePropertiesMap()`）跑同一 `getBackendPropertiesFromStorageMap` → 连接器**整段**替换裸拷循环为该 overlay（vended overlay 仍后置、collision 胜，legacy 优先序）。object-store→`AWS_*`；HDFS→canonical（保留用户 `hadoop.`/`dfs.`/`fs.`/`juicefs.` override + 补 legacy 默认 `ipc.client.fallback-to-simple-auth-allowed` 等，**顺修 review §211 MINOR**）；丢非-parity `hive.*` 裸键（legacy 本不发 scan location）。一处 SPI 调用替掉前缀循环、单一真相源、无漂移。**ANONYMOUS-leak 边角经 BE 证伪无回归**（`s3_util.cpp` v1:383/v2:448 显式 ak/sk 优先于 `cred_provider_type`，vended keys 在则永不走 Anonymous 支）。无 ctor 改、无连接器新 import（import-gate 净）。SPI RFC §22(E14)。测：fe-core `DefaultConnectorContextBackendStoragePropsTest`(2)+连接器 `PaimonScanPlanProviderTest`(3 改/增，red-check 2/3 向红);模块 217/0/0。设计 [`P5-fix-STATIC-CREDS-BE-design.md`](./tasks/designs/P5-fix-STATIC-CREDS-BE-design.md) | 2026-06-11 | ✅ |
| D-039 | P5-D8 | **P5 paimon B4 E7 sys-table SPI 形状 = 复用 live fe-core SysTable 机制（用户签字，2026-06-10）**：RFC §10 的「sys-table 当 `$`-后缀普通表、连接器在 `getTableHandle` 内解析后缀 + `listSysTableSuffixes`」设计**从未落地**——live fe-core 实为 `SysTableResolver`+`NativeSysTable`+`TableIf.getSupportedSysTables/findSysTable`（`BindRelation`/`DescribeCommand`/`ShowCreateTableCommand` 调用；iceberg + legacy-paimon 共用），RFC §10 已 stale。**用户定 = 复用 live 机制（非 RFC §10）**：① 连接器 SPI 加 `ConnectorTableOps.listSupportedSysTables` + `getSysTableHandle`（default no-op，MC/jdbc/es/trino 不受影响）；② fe-core `PluginDrivenExternalTable.getSupportedSysTables` 委托连接器（`listSupportedSysTables`），通用 `PluginDrivenSysTable extends NativeSysTable` + `PluginDrivenSysExternalTable`（**报 `PLUGIN_EXTERNAL_TABLE` 非连接器类型**，经现有 `SysTableResolver` 路由到 `PluginDrivenScanNode`）。否决 RFC §10 的 `getTableHandle("$suffix")`-路由（须改 `BindRelation`/`RelationUtil`、大 surface、偏离 iceberg）。RFC §10 标 superseded（[DV-023](./deviations-log.md)）。**T20（E5 MVCC）置于 B4** = 连接器侧 groundwork（inert until B5 wires fe-core MvccTable 消费者；翻闸 gated on B5 故 inert capability 不达用户，安全）。设计 `tasks/P5-paimon-migration.md` §批次 B4 | 2026-06-10 | ✅ |
| D-038 | P5-D2 | **P5 paimon MTMV + MVCC(时间旅行) scope = P5 内实现桥，翻闸 gated on 它（用户签字，design-only）**：SPI 当前 **MTMV 完全无面（E10 缺）**（`PluginDrivenExternalTable:62` 不 implements MTMVRelatedTableIf/MTMVBaseTableIf/MvccTable，框架靠 `instanceof MTMVRelatedTableIf` 分发——`MTMVPartitionUtil:265/497/588`、`StatementContext:987/1003`），E5(MVCC) `defined-no-consumer`（`ConnectorMvccSnapshotAdapter` 仅自身文件引用、`ConnectorScanRange` 无 snapshot 字段）。legacy `PaimonExternalTable:74` 实现全套。翻闸不机械阻断（plain SELECT 经 `getPaimonTable(empty)` 取 latest）但按 MC 样板直接翻闸=**静默回归** paimon-as-MTMV-base + 时间旅行。**用户定 = 方案 A**：P5 内落 fe-core `PaimonPluginDrivenExternalTable extends PluginDrivenExternalTable` 实现三接口 + 首个真 E5 消费者 override `beginQuerySnapshot` 三方法 + 新增 GAP-LISTPART-AT-SNAPSHOT 的 at-snapshot listPartitions；表级 staleness=`ConnectorMvccSnapshot.getSnapshotId()`(-1 空表)、分区级=`ConnectorPartitionInfo.getLastModifiedMillis()`(已存在)；MTMV 类型/PartitionItem 留 fe-core、连接器仅供 SPI-neutral 数据。**翻闸(B7) gated on MTMV 桥(B5)**；**禁**静默读 latest。否决 B（翻闸先行 + MTMV fail-loud 延后）。最高 correctness 风险=单-pin 不变式 + `lastFileCreationTime()` 跨 flavor 可靠性（SDK 行为，须 live 验）。设计 `tasks/P5-paimon-migration.md` §开放决策 D2 + recon §3.5/§4 | 2026-06-09 | ✅ |
| D-037 | P5-D1 | **P5 paimon flavor(hms/filesystem/dlf/rest/jdbc) 装配 = 单 Catalog + `createCatalog` flavor switch（MC 一致，用户签字，design-only）**：连接器现走单 Catalog stub（`PaimonConnector.createCatalog:75-83` 把 `Options.fromMap` 直喂 paimon SDK CatalogFactory，无 Doris 侧 warehouse/HiveConf/StorageProperties/authenticator 装配）；5 个 `fe-connector-paimon-backend-*` 模块**是空壳**（仅 gitignore `.flattened-pom.xml`、零 src/未注册 Maven 模块）。legacy 装配在 fe-core `AbstractPaimonProperties`+5 子类+`PaimonPropertiesFactory`，全 import 禁用的 fe-core `StorageProperties`/`HMSBaseProperties`/`HadoopExecutionAuthenticator`。**用户定 = 方案 A**：`PaimonConnector.createCatalog` 内 switch on `paimon.catalog.type`，**拷** warehouse/conf/S3-normalize + 重建 Hadoop/HiveConf + **每-flavor ExecutionAuthenticator** 入模块（镜像 MC 拷 MCProperties→MCConnectorProperties；filesystem→hms→rest/jdbc/dlf 渐进）。**不**建 backend 模块 + ServiceLoader（否决 B：无 MC 先例、大 surface、空壳从零建）。约束：StorageProperties 从属性 map 重建（禁 import）；**每-flavor authenticator 必须保**（否则 Kerberized HMS/HDFS DDL 运行时炸、无离线测覆盖）。设计 `tasks/P5-paimon-migration.md` §开放决策 D1 + recon §3.4 | 2026-06-09 | ✅ |
| D-036 | — | **P4-T06e FIX-CAST-PUSHDOWN MaxCompute 关 CAST 谓词下推 + 剥壳时抑制 source LIMIT（F9 静默丢行回归，review 原误判 known-degr 已推翻）**：共享 converter 无条件剥 CAST（`ExprToConnectorExpressionConverter:108`）、MaxCompute 不 override `supportsCastPredicatePushdown`（继承默认 true）→ `buildRemainingFilter` 不剔除含 CAST 的 conjunct → 剥壳谓词推入 ODPS read session（`CAST(str AS INT)=5`→源过滤 `str="5"` 按列 STRING quote）→ 源端 under-match 丢 `'05'/' 5'`、BE 复算只能过滤超集向下无法找回 → **静默丢行**。legacy `convertSlotRefToColumnName` 对 CAST 操作数抛异常→caught→丢弃该谓词（BE-only）→正确 ⇒ cutover 比 legacy 严格更紧 = **回归**（区别于 [DV-016] 仅 limit-opt 资格 CAST-unwrap、非丢行）。**对抗核验 `wzoa6dkvw` 0/3 refuted、verdict=real-unregistered-regression**。**用户定 Fix**。修 = ① 连接器 `MaxComputeConnectorMetadata.supportsCastPredicatePushdown→false`（激活既有 strip 路径、CAST conjunct 保留 BE-only、恢复 legacy parity；镜像 JDBC + `ConnectorPushdownOps` doc 处方；无 SPI 变更、无新路径）；② fe-core `getSplits` 在 CAST conjunct 被剥（`filteredToOriginalIndex!=null`）时抑制 source LIMIT 下推（抽纯静态 `effectiveSourceLimit`）——否则连接器收空 filter→limit-opt(ON 时) row-offset 读首 N 行无谓词→BE under-return（impl-review `wj2h0120n` F9-LIMITOPT-1 折入；`startSplit` 批路径已恒 -1[DEC-1] 故只改 getSplits）。守门：连接器 UT 2/2+mutation(false→true 红)、fe-core LimitStrip 2/2+BatchMode 9/9+mutation 2/2 向红、checkstyle 0、import-gate 净。真值闸=live ODPS CAST(str)=5 返回全集（DV-020，CI 跳）。out-of-scope surface：JDBC `applyLimit`+cast-off 理论同类（MC 不 override applyLimit、本修对 MC 完整）。commit `cc32521ed99` | 2026-06-08 | ✅ |
| D-035 | — | **P4-T06e FIX-BATCH-MODE-SPLIT 通用 batch SPI 路径恢复异步分批 split（Shape A，NG-7/F6=F13 minor）**：翻闸后 `PluginDrivenScanNode` 不 override `isBatchMode/numApproximateSplits/startSplit` → 继承 `SplitGenerator` 默认（false/-1/no-op）→ plugin-driven(含 MC) 读永走同步 `getSplits` 一次性枚举全(已裁剪)分区 split；legacy `MaxComputeScanNode:214-298` 分批异步建 read session 流式喂 split。P1-4 后降级收窄到「裁剪后仍 ≥`num_partitions_in_batch_mode` 分区」（规划慢 + 大 session 潜在 OOM、行正确）。**用户定「实现 batch SPI 路径」（非 DV）**。修 = **Shape A（薄 SPI + fe-core 编排、逐字镜像 legacy）**：① SPI `ConnectorScanPlanProvider` +2 additive default（`supportsBatchScan` 默认 false / `planScanForPartitionBatch` 默认委托 6 参 `planScan` over 子集）零破坏其余 6 连接器；② 连接器 `MaxComputeScanPlanProvider.supportsBatchScan`=`odpsTable.getFileNum()>0`（`planScanForPartitionBatch` 不 override，继承默认即批语义）；③ fe-core `PluginDrivenScanNode`（extends `FileQueryScanNode` 已继承 batch dispatch+stop；`PluginDrivenSplit extends FileSplit` 故 `:381` 转型安全）override `isBatchMode`（4 闸 isPruned+slots+supportsBatchScan+size≥阈值，含 SF-1 `getScanPlanProvider()` null-guard）/`numApproximateSplits`=size/`startSplit`（`getScheduleExecutor` outer/inner CompletableFuture 分批，`needMoreSplit/addToQueue/finishSchedule/setException/isStop` 契约，DEC-1 不下推 limit 传 -1 与 P3-9 limit-opt 互斥）+ 抽纯静态 `shouldUseBatchMode` 供单测。clean-room 设计验证 `wcpg9lblj` GO-WITH-EDITS（0 mustFix + 2 shouldFix：SF-1 null-guard NPE 修 + 预核文案，已折入）+ impl-review `wve7y1jst` GO-WITH-EDITS（0 mustFix + 1 shouldFix TQ-1 测试覆盖文案诚实降级 + 2 nit，已折入）。守门：编译 BUILD SUCCESS、fe-core UT 9/9、fe-connector-api UT 2/2、checkstyle 0、import-gate 净、mutation 5/5 向红。真值闸=大分区 live e2e（DV-019，CI 跳）。**Batch-D 红线**：legacy `MaxComputeScanNode` batch 逻辑须待本 fix 落才可删（读裁剪那半 P1-4 已清，本项为最后前置闸）。commit `ac8f0fc15eb` | 2026-06-08 | ✅ |
| D-034 | — | **P4-T06e FIX-POSTCOMMIT-REFRESH 接受更安全的 post-commit 刷新 swallow、不回退 legacy 传播失败（无产线逻辑改动，NG-8/F15=F21 minor）**：翻闸后 `PluginDrivenInsertExecutor.doAfterCommit()` 用 try/catch 吞 `super.doAfterCommit()`（=`handleRefreshTable`）刷新失败、INSERT 仍报 OK；legacy `MCInsertExecutor` 不 override → 异常传播 → 报 FAILED。按生命周期序 `doBeforeCommit→commit（远端持久）→doAfterCommit`，`handleRefreshTable` 跑时数据已落 ODPS/远端、FE 无法回滚，且只刷 FE 缓存 + 写 external-table refresh editlog（follower 缓存失效提示、非数据真相源）、不碰已提交数据 → 报 FAILED 会诱发重试→**重复写**。**用户定（2026-06-08）：接受 swallow（更安全）+ Javadoc 泛化 + DV 登记，不回退**。改 = **无产线逻辑**：仅 Javadoc(`:164-176`) 从「只讲 JDBC_WRITE」泛化到覆盖 MC connector-transaction 路径（两路径数据均已持久；swallow 最坏只瞬时缓存 stale 自愈；显式注明有意分歧 legacy、引用 [DV-018]）。对抗性安全核查：master 先本地刷新(`RefreshManager:152`)后写 editlog(`:155`)，丢 editlog 仅 follower 缓存暂 stale 自愈、无正确性损失/无主从分裂。守门：checkstyle 0、import-gate 净（注释 only、字节码不变）。真值闸=CI-skip live e2e（MC INSERT 后人为令 refresh 失败→断言报 OK）。commit `1f2e00d3696` | 2026-06-08 | ✅ |
| D-033 | — | **P4-T06e FIX-ISKEY-METADATA 连接器局部恢复 isKey=true（无 SPI 变更，NG-6/F3/F10 minor）**：翻闸后 `MaxComputeConnectorMetadata.getTableSchema` 用 5 参 `ConnectorColumn` ctor（isKey 默认 false）→ `DESCRIBE` 显示 Key=NO；legacy `MaxComputeExternalTable.initSchema` 全列 isKey=true。**用户定 Fix（isKey=true 恢复 parity）**。修 = 连接器局部、不动 SPI：抽 `buildColumn(...)` 静态助手用 6 参 ctor 置 isKey=true，data+partition 两 loop 经之；converter 已透传 isKey。**作用域更正**（设计验证 `wa9t0emta`）：`information_schema.columns.COLUMN_KEY` 受 `FrontendServiceImpl:962-965` OlapTable 门控、MC 前后皆空（已 parity，out-of-scope）→ 本修**仅影响 DESCRIBE**。**非纯展示**：isKey 亦喂 `UnequalPredicateInfer:278` + BE slot/column descriptor（非 OLAP 门控），但 legacy 即喂 true → 恢复 production 既有值、零新行为。clean-room 设计验证 `wa9t0emta` 0 mustFix + impl review `wrx0n11ol` 0 mustFix。UT 3/3(+37 collateral)、checkstyle 0、import-gate 净、mutation killed（isKey true→false→Failures 2）。commit `1b44cd4f065` | 2026-06-08 | ✅ |
| D-032 | — | **P4-T06e FIX-LIMIT-SPLIT-DEFAULT 连接器局部恢复 limit-split 默认 OFF 三重闸（无 SPI 变更，NG-5/F11；并闭 minors F2/F12）**：翻闸后 `MaxComputeScanPlanProvider.planScan` 丢 legacy 三重闸——`checkOnlyPartitionEquality` 恒 false stub + 从不读 `enable_mc_limit_split_optimization`（默认 false）→ `useLimitOpt = limit>0 && !filter.isPresent()`：无过滤 LIMIT 默认即压成单 row-offset split（语义反转 + 静默无视 session var），分区等值 LIMIT 路径永不触发。**用户定 Fix（恢复三重闸）**。修 = 连接器局部、**不动 SPI**：① 加 hardcode 常量 `ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION`（禁依赖 fe-core `SessionVariable`，同 JDBC 约定）经 `ConnectorSession.getSessionProperties()`（live 由 `from(ctx)`→`VariableMgr.toMap` 填）读 gate(1)；② 实 `checkOnlyPartitionEquality` 遍历 `ConnectorExpression` 树（`ConnectorAnd` 全 conjunct / `ConnectorComparison` EQ 且 col 左 lit 右 / `ConnectorIn` 非 NOT-IN 且 value 为分区列、全 literal），镜像 legacy `checkOnlyPartitionEqualityPredicate`；③ 纯静态 `shouldUseLimitOptimization` 合成 gate(1)&&gate(3)&&gate(2)。默认 OFF=保守回退 legacy。clean-room 设计验证 `w17wzd0el` 0 mustFix + impl review `walkff1vf` 1 mustFix（IN-value 守卫缺杀手测，已补 test+mutation G）收敛。UT 26/26、checkstyle 0、import-gate 净、mutation 8 向红。commit `952b08e0cc8` | 2026-06-08 | ✅ |
| D-031 | — | **P4-T06e FIX-PRUNE-PUSHDOWN 新增 additive 6 参 `planScan` SPI overload 透传裁剪分区（DG-1）**：翻闸后 plugin-driven MaxCompute 读路径 Nereids `SelectedPartitions` 在 translator 被丢、`MaxComputeScanPlanProvider` 恒传 `requiredPartitions=emptyList` → ODPS read session 跨全分区（纯性能/内存回归，行正确）。FE 元数据半边 FIX-PART-GATES 已落（[D-028]），缺 translator→SPI→connector 透传（原 READ-C2「②」半）。修 = `ConnectorScanPlanProvider` 加 6 参 `planScan(...,List<String> requiredPartitions)` **default**（委托 5 参，零破坏其余 6 连接器，仅 MaxCompute override）+ `PluginDrivenScanNode` 加 `selectedPartitions` 字段/setter/三态 `resolveRequiredPartitions`（NOT_PRUNED→null 全扫 / pruned-非空→names / pruned-空→fe-core 短路无 split，镜像 legacy `MaxComputeScanNode:718-731`）+ translator plugin 分支注入 + MaxCompute `toPartitionSpecs` 喂两 read-session 路径。**契约**：null/空=全部、非空=子集、零分区 fe-core 短路不下达 SPI。clean-room `w31i0vfo5` 1 轮收敛 0 mustFix。commit `072cd545c54` | 2026-06-08 | ✅ |
| D-030 | — | **P4-T06e FIX-BIND-STATIC-PARTITION 新增 SPI capability `SINK_REQUIRE_FULL_SCHEMA_ORDER` + 回退 D-029 的 cols 位置索引为 full-schema 索引（用户批准扩 scope）**：翻闸后 MaxCompute 写走通用 `bindConnectorTableSink`,该路径克隆自 JDBC（按名 cols 序投影）,而 MaxCompute BE/JNI writer **按位置**映射数据到完整表 schema → 静态分区无列名 INSERT bind 抛、重排/部分显式列名静默错列。修正 = 镜像 legacy `bindMaxComputeTableSink`：对**按位置写**的连接器（声明新 capability `SINK_REQUIRE_FULL_SCHEMA_ORDER`,MaxCompute 声明、JDBC/ES 不声明）恒投影到 full-schema 序(填 NULL/默认);JDBC 维持 cols 序。**并回退 D-029**：分布索引 cols→full-schema（否则 partial-static/重排错列）。判别键三轮收敛 static→partitioned→capability。clean-room 3 轮收敛 0 mustFix（`wi3mnjymb`/`wy299gtsh`/`wlwpw0b2s`）。commit `7cc86c66440` | 2026-06-07 | ✅ |
| D-029 | — | **P4-T06e FIX-WRITE-DISTRIBUTION 新增 SPI capability `SINK_REQUIRE_PARTITION_LOCAL_SORT`（Option A）**〔⚠️其「分区列按 **cols** 位置索引」已被 **D-030** 回退为 full-schema 索引——partial-static/重排显式列名下 cols 索引会错列〕：翻闸后 MaxCompute 写走通用 `PhysicalConnectorTableSink`,丢 legacy 动态分区 hash+local-sort（ODPS Storage API "writer has been closed"）。新增 `ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT`（default 不声明）+ MaxCompute `getCapabilities()` 声明它 + `SUPPORTS_PARALLEL_WRITE`;sink 重写 legacy 3 分支（分区列按 **cols** 位置索引非 legacy full-schema）。替代（隐式 derive / `ConnectorWriteOps` 方法）见详录。clean-room `ww1g95bba` 1 轮收敛 0 must-fix。commit `f0adedba20c` | 2026-06-07 | ✅ |
| D-028 | — | **翻闸功能未完整,补 P4-T06c 接线（用户签字）**：live 验证 recon 代码核实——翻闸（Batch C）只接通 读(SELECT)/CREATE TABLE/写(INSERT);**DROP TABLE / CREATE DB / DROP DB / SHOW PARTITIONS / partitions() TVF 的 FE 分发从未接到 SPI**（连接器侧 P4-T01/T02 已实现,FE 零调用方）→ live 会红 5 项。根因 `PluginDrivenExternalCatalog` 仅 override `createTable`、`metadataOps==null`,且 SHOW PARTITIONS/TVF 仍 legacy `instanceof MaxComputeExternalCatalog` 分发。**决策 = 翻闸前全补接线**：Batch D 前插 **P4-T06c**（通用 PluginDriven 分发,非 MC 专有）把 DDL(create/drop db、drop table)+ SHOW PARTITIONS + partitions TVF 接到已有 SPI,目标 **live 全绿**,再 Batch D。同解 Batch D §2 删-vs-rewire 冲突（先 rewire,Batch D 只删残留 legacy） | 2026-06-07 | ✅ |
| D-027 | — | P4-T06b 翻闸落地 + Batch D 移除范围（2 决策，用户签字）：**翻闸** `CatalogFactory.SPI_READY_TYPES += "max_compute"` + 删 legacy `case "max_compute"`（gate 全绿：compile/checkstyle 0/import-gate 0）；**D-1 时序** = flip 先行、legacy 子系统删除 + fe-core odps 依赖 drop **待用户 live ODPS 验证后**做（保 flip 独立可回退）；**D-2 依赖范围** = fe-core 仅删直接 `odps-sdk-*` 声明，transitive-via-fe-common 留（fe-common 供连接器/be-extensions）。Batch D 完整闭包（21 删 / ~30 清 / keep / pom）见 `designs/P4-batchD-maxcompute-removal-design.md`（OQ-3 穷举 re-grep 满足）。**2 SPI 新增登记 §20 E11**（D-026 预授）：`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction`；T06a 复核修 `PluginDrivenTableSink.getExplainString` `writeConfig==null` NPE 守卫记一笔 | 2026-06-07 | ✅ |
| D-026 | — | P4 Batch C 翻闸设计（用户签字，design-only）：**D-1** capability signal = 新增 `ConnectorWriteOps.usesConnectorTransaction()` default false（MC=true；executor 据此在调任何 throwing-default 写法前分流 txn-model vs JDBC insert-handle）；**D-2** 两 commit（`[P4-T06a]` 写接线/绑定/R-004 隔离测 dormant + `[P4-T06b]` flip 末提）；**D-3** 静态分区/overwrite 绑定**入 cutover**（避 INSERT OVERWRITE PARTITION 翻闸回归）。**两新 SPI**（均 default-preserving）：`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction`（impl 时 E11 登记）。设计 `designs/P4-T05-T06-cutover-design.md` | 2026-06-06 | ✅ |
| D-025 | — | P4-T04 写计划 5 决策（D-1/D-2a 用户签字、D-3/D-4/D-5 主线定）：D-1 **OQ-2=Approach A**（`planWrite` 在 finalizeSink 一处建 ODPS 写 session + `setWriteSession` 绑 txn + 盖 `txn_id`/`write_session_id`，无运行期注入 hook）；D-2a 含 **fe-core seam fill**（`PluginDrivenTableSink.bindViaWritePlanProvider(insertCtx)` 读 overwrite+静态分区；`staticPartitionSpec` 加 `PluginDrivenInsertCommandContext` 非基类——避 `MCInsertCommandContext` override/shadow）；D-3 抽 `MaxComputeDorisConnector.getSettings()`（legacy 单 `settings` 同供 scan+write，抽出=忠实港）；D-4 `supportsInsert()`=true 余最小化（`beginInsert`/`finishInsert`/`getWriteConfig` 留 throwing-default，实际 executor 调用面待 Batch C）；D-5 静态分区作 `getWriteContext()` col→val map | 2026-06-06 | ✅ |
| D-024 | — | P4-T03 两 fork（用户签字）：(1) txn id 经新增 `ConnectorSession.allocateTransactionId()`（fe-core `Env.getNextId` 背书）由连接器分配——尊重 [D-015]/U3，补 id-less 连接器（MC 无外部 id）的分配器机制；(2) ODPS 写 session 创建挪 T04 planWrite（T03 = 纯事务容器，over W4 委派、gate 关 dormant）| 2026-06-06 | ✅ |
| D-023 | — | P4 maxcompute 启 full adopter（recon §9 option A）：W-phase 后按 5 批（A 读/DDL parity → B 写/事务 → C 翻闸 → D 清引用+删 legacy → E 测）落地 + cutover；批次计划 tasks/P4 | 2026-06-06 | ✅ |
| D-022 | — | 写/事务 SPI 设计：A 连接器事务为源·桥接 / B1 commit 载荷 opaque bytes / C1 block-id 窄 callback seam / D INSERT·DELETE·MERGE（defer procedures）/ E 写-plan-provider 仿 scan | 2026-06-06 | ✅ |
| D-021 | — | P4 maxcompute 采 scope=C（写-SPI RFC 先行）：先做共享写/事务 SPI + 通用层解耦（W-phase），再逐连接器 adopter | 2026-06-06 | ✅ |
| D-020 | — | 单 `hms` catalog 多格式 scan 路由 = 方案 B（`ConnectorMetadata.getScanPlanProvider(handle)` per-table default）；细化 D-005（design-only，实现批 E/P7）| 2026-06-05 | ✅ |
| D-019 | — | P3 hudi 采用 hybrid：现做 model-agnostic 连接器硬化+测试（behind gate），推迟 catalog 模型落地+cutover 到 hive/HMS migration | 2026-06-04 | ✅ |
| D-018 | U6 | `ConnectorColumnStatistics` 用 javadoc 类型映射表 + IAE 保证类型安全 | 2026-05-24 | ✅ |
| D-017 | U5 | sys-table 命名统一 `$suffix`，别名机制留待未来 | 2026-05-24 | ✅ |
| D-016 | U4 | `getCredentialsForScans` 批量化，返回 `Map<Range, Credentials>` | 2026-05-24 | ✅ |
| D-015 | U3 | `ConnectorTransaction.getTransactionId` 由连接器分配 | 2026-05-24 | ✅ |
| D-014 | U2 | 不新增 `invalidateColumnStatistics`，挂在 `invalidateTable` | 2026-05-24 | ✅ |
| D-013 | U1 | `ConnectorProcedureOps.listProcedures` 一次性返回，生命周期稳定 | 2026-05-24 | ✅ |
| D-012 | D12 | 用户安装 connector 后初版强制重启 FE | 2026-05-24 | ✅ |
| D-011 | D11 | `RemoteDorisExternalCatalog` 长期做 connector，不在本计划主线 | 2026-05-24 | ✅ |
| D-010 | D10 | `LakeSoulExternalCatalog` 在 P8 删除剩余类 | 2026-05-24 | ✅ |
| D-009 | D9 | API 版本号本计划范围内永不 +1，只新增 default 方法 | 2026-05-24 | ✅ |
| D-008 | D8 | 生产环境不允许 built-in connector，强制目录式插件 | 2026-05-24 | ✅ |
| D-007 | D7 | kafka/kinesis/odbc/doris 子目录不在本计划范围 | 2026-05-24 | ✅ |
| D-006 | D6 | Iceberg snapshot/manifest cache 放连接器内，fe-core 不感知 | 2026-05-24 | ✅ |
| D-005 | D5 | hudi/iceberg-on-HMS 用 `ConnectorTableSchema.tableFormatType` 区分 | 2026-05-24 | ✅ |
| D-004 | D4 | HMS event pipeline 放 `fe-connector-hms`，通过 `ConnectorMetaInvalidator` 回调 | 2026-05-24 | ✅ |
| D-003 | D3 | 旧 `*ExternalCatalog` 子类**全部删除**，不保留中间形态 | 2026-05-24 | ✅ |
| D-002 | D2 | `PluginDrivenScanNode` 长期保持 `extends FileQueryScanNode` | 2026-05-24 | ✅ |
| D-001 | D1 | 沿用已有 `SUPPORTS_PASSTHROUGH_QUERY`，不新增 query SPI | 2026-05-24 | ✅ |

---

## 详细记录（时间倒序）

### D-054 — `FIX-COUNT-PUSHDOWN`（#8 M-2）= 新增 default `planScan(countPushdown)` overload + 连接器 collapse-to-one

- **日期**：2026-06-12
- **状态**：✅ 生效
- **关联**：[task-list #8](./task-list-P5-rereview2-fixes.md)、[设计](./tasks/designs/P5-fix-COUNT-PUSHDOWN-design.md)、[第二轮 review report](./reviews/P5-paimon-rereview2-2026-06-11.md)、[DV-032]、[01-spi-extensions-rfc.md §23 E15](./01-spi-extensions-rfc.md)
- **背景**：翻闸后 plugin-driven paimon `COUNT(*)` 结果正确但慢。recon（5-scout + 对抗 synthesizer `wf_1ce48c93-325`）逐链核实三半中只缺一半：① **emit 半已建全**——`PaimonScanRange.Builder.rowCount`→prop `paimon.row_count`→`populateRangeParams.setTableLevelRowCount`（else -1），与 legacy `PaimonScanNode:303-308` byte-一致，**无新 thrift / 无 BE 改**；② **COUNT 枚举已达 BE**——`PhysicalPlanTranslator:873` 在 `PluginDrivenScanNode` 设 `pushDownAggNoGroupingOp=COUNT`（Nereids 不排除 plugin），`FileScanNode.toThrift:90` 发出，BE 已在 count 模式；③ **信号+计算缺**（bug）——`DataSplit.mergedRowCount()` 是 paimon-SDK-only 须连接器算；COUNT 信号 `getPushDownAggNoGroupingOp()==COUNT` 只在 fe-core 节点、`PluginDrivenScanNode.getSplits` 从不读（grep 0）、不在任何 `planScan`/`ConnectorSession`/`ConnectorContext`/handle → 每 split 发 `table_level_row_count=-1` → BE 物化全 post-merge 行去 count（`file_scanner.cpp:1298-1326`）。
- **决策**：(1) **fix-now**（vs defer）。(2) **count-split 形状 = 连接器 collapse-to-one**：连接器累加全 count-eligible split 的 `mergedRowCount` 入 `countSum`、留首个 split 为代表、循环后发**一** JNI count range 携 `countSum`；= legacy `<=10000` 路径（`singletonList(first)` + `assignCountToSplits([one], sum)` → 一 split 携全 total）普遍化。(3) **SPI 参数 = `boolean countPushdown`**（BE 只需 COUNT-vs-not；`TPushAggOp` 过度泛化、把 thrift 枚举拉进 SPI 签名）。(4) **作用域 = paimon-only**（default no-op overload）。修=3 文件：SPI `ConnectorScanPlanProvider` +1 default 7-arg `planScan(...,boolean countPushdown)` 委托 6-arg [E15]；fe-core `PluginDrivenScanNode.getSplits` 读 agg-op 传入（无 post-loop 数学）；连接器抽 `planScanInternal(...,countPushdown)`（4-arg 委托 false、7-arg 委托 flag）+ count 短路第一臂 + 纯静态 `isCountPushdownSplit` + `buildCountRange`。
- **替代方案**：① **defer**（登 deviations）——用户选 fix-now。② **经 `ConnectorSession` 穿信号**（FIX-FORCE-JNI 先例，零 SPI 签名改）——**否决**：agg-op 是 per-query planner 输出非 SET-var，会成静默无类型通道（本项目反复踩的 handle-bypass/signal-not-threaded bug 类）。③ **full-parity fe-core trim**（连接器发 per-split、fe-core 按 numBackends trim+redistribute）——更多 fe-core 代码、把 count 语义耦进通用 `ConnectorScanRange`，否决。④ **per-split（不 collapse）**——最简但比 legacy 多 fragment，否决。⑤ **`TPushAggOp` / typed `ScanContext` 参数**——过度泛化，选 boolean。
- **影响**：3 产线文件（SPI +1 default 方法、fe-core getSplits、连接器 planScan）+ 1 测文件。**API 不 +1**（仅新增 default，[D-009]）。SPI 新面记 [E15]（RFC §23）。perf 偏差 [DV-032]（collapse-to-one 丢 legacy `>10000` 并行 split trim）。**跨连接器**：新 default overload 利好 hive/iceberg/maxcompute，但 paimon-only 实现（default no-op）→ 将来 full-adopter 各自 override 即可。守门见索引行。


- **日期**：2026-06-08
- **状态**：✅ 生效
- **关联**：[FIX-PRUNE-PUSHDOWN 设计](./tasks/designs/P4-T06e-FIX-PRUNE-PUSHDOWN-design.md)、[review-rounds](./reviews/P4-T06e-FIX-PRUNE-PUSHDOWN-review-rounds.md)、[复审 §B DG-1](./reviews/P4-maxcompute-full-rereview-2026-06-07.md)、[D-028]（FIX-PART-GATES 只落元数据半边）、[DV-015]
- **背景**：翻闸后 plugin-driven MaxCompute 读走通用 `PluginDrivenScanNode`。Nereids `PruneFileScanPartition` 借 FIX-PART-GATES 加的分区元数据 API **算出** `SelectedPartitions`，但 `PhysicalPlanTranslator` plugin 分支（`:753-758`）**从不**调 `setSelectedPartitions`（对比 Hive `:773`/legacy-MC `:797`/Hudi `:882`），`PluginDrivenScanNode` 无承接字段，`MaxComputeScanPlanProvider` 恒传 `requiredPartitions=Collections.emptyList()`（`:201`/`:320`）→ ODPS read session 跨**全分区**。3 lens 对抗复审无法证伪。**纯性能/内存回归**（MaxCompute 未 override `applyFilter`→conjunct 不清→BE 重算→行正确）。这正是原 cutover-review READ-C2 修复建议的「②透传 selectedPartitions→planScan 接 requiredPartitions」半——FIX-PART-GATES 只落「①元数据 API」半（[D-028]）。
- **决策**：(a) `ConnectorScanPlanProvider` 加 6 参 `planScan(session,handle,columns,filter,limit,List<String> requiredPartitions)` **default** 方法，委托回 5 参（镜像既有 5 参 limit overload 模式）→ **零破坏** es/jdbc/hive/paimon/hudi/trino（继承 default），唯一 override=MaxCompute。**契约**：`null`/空=不裁剪 scan all；非空=仅扫这些分区名（`SelectedPartitions.selectedPartitions` keySet）；「裁剪为零」由 fe-core 短路、永不到 SPI。(b) `PluginDrivenScanNode` 加 `selectedPartitions` 字段（默认 `NOT_PRUNED`）+ setter + 纯函数 `resolveRequiredPartitions`（三态：`!isPruned`→null / pruned-非空→names / pruned-空→空 list）+ `getSplits` 短路（空 list→无 split，镜像 legacy `MaxComputeScanNode:724-727`）+ 6 参调用。(c) `PhysicalPlanTranslator` plugin 分支注入 `setSelectedPartitions(fileScan.getSelectedPartitions())`。(d) MaxCompute override 6 参，`toPartitionSpecs(List<String>)`→`List<PartitionSpec>`（镜像 legacy `new PartitionSpec(key)`）喂**两** read-session 路径（标准 + limit-opt）。
- **替代方案**：① 改 `planScan` 签名（破坏全 7 连接器）——否决，default overload 零破坏；② 编码进 `ConnectorTableHandle`（如 Hive/Hudi 经 `applyFilter` 存 pruned partitions）——MaxCompute 未 override `applyFilter` 且会重导出 Nereids 已算的裁剪、less faithful；③ `ConnectorSession` 携带——session 非 scan 级、hacky。capability/overload-additive 与 P0-1/P0-2/P0-3 模式一致。
- **影响**：4 产线文件（`ConnectorScanPlanProvider` SPI +default / `MaxComputeScanPlanProvider` override+`toPartitionSpecs`+两路径 threading / `PluginDrivenScanNode` 字段+setter+helper+短路 / `PhysicalPlanTranslator` 注入）+ 2 UT。**scope 边界**：Hudi-SPI plugin 分支（`visitPhysicalHudiScan`）本次不接——生产不可达（`SPI_READY_TYPES` 不含 hudi）+ Hudi provider 走 default 忽略 requiredPartitions，deferred DV-006。**与 NG-7（batch-mode）解耦**但为其前置。**Batch-D 红线**：删 legacy `MaxComputeScanNode` 须待本 fix 落（读裁剪下推逻辑副本）。**follow-up**：wiring 无 fe-core 端到端 UT → [DV-015]；真值闸 live e2e（p2 `test_max_compute_partition_prune.groovy` + EXPLAIN/profile 证仅扫目标分区）。

### D-030 — P4-T06e FIX-BIND-STATIC-PARTITION 新增 SPI capability SINK_REQUIRE_FULL_SCHEMA_ORDER + 回退 D-029 索引（用户批准扩 scope）

- **日期**：2026-06-07
- **状态**：✅ 生效
- **关联**：[FIX-BIND-STATIC-PARTITION 设计](./tasks/designs/P4-T06e-FIX-BIND-STATIC-PARTITION-design.md)、[review-rounds](./reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md)、[D-029]（被部分回退）、[D-026 DECISION-3]
- **背景**：翻闸后真实 MaxCompute catalog = `PluginDrivenExternalCatalog`，所有 MC 写走通用 `bindConnectorTableSink`。该方法克隆自 `bindJdbcTableSink`（JDBC 按列名生成 INSERT SQL、数据 cols/用户序即可），但 **MaxCompute BE/JNI writer 按位置映射** Arrow 列到 `writeSession.requiredSchema()`（完整表 schema 序）。后果：① 静态分区无列名 `INSERT INTO mc PARTITION(pt='x') SELECT <非分区列>` 列数校验抛（F19/F48 blocker）；② 静态分区列未在 full-schema 末尾 → BE 末尾擦除契约错位；③ **非分区** MC 重排/部分显式列名静默错列/丢列。legacy `bindMaxComputeTableSink` **无条件** full-schema 投影（不论分区与否）——通用路径漏了这层。
- **决策**：(a) 新增 `ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER`（"连接器按位置写 full-schema"，default 不声明）；MaxCompute `getCapabilities()` 声明之、JDBC/ES 不声明；`PluginDrivenExternalTable.requiresFullSchemaWriteOrder()` 读之。(b) `bindConnectorTableSink` 分支键 = `table.requiresFullSchemaWriteOrder()`：true→full-schema 投影（`getColumnToOutput`+`getOutputProjectByCoercion(getFullSchema())`,镜像 legacy,对**全**MC 写形）；false→cols 序（JDBC/ES）。(c) **回退 D-029**：`PhysicalConnectorTableSink.getRequirePhysicalProperties` 分区列索引 cols→full-schema（因 child 现恒 full-schema 序；cols 索引在 partial-static/重排下错列）。(d) `selectConnectorSinkBindColumns` 无列名时剔除静态分区列（镜像 legacy）；`InsertUtils` VALUES 路径加 `UnboundConnectorTableSink` 分支。
- **替代方案**：判别键 = `!staticPartitionColNames.isEmpty()`（round-1 证伪：纯动态重排错列）→ `!getPartitionColumns().isEmpty()`（round-2 证伪：非分区 MC 重排/部分错列）→ **capability**（终态 = legacy 全 parity）。亦考虑 bind 期查 `connector.getWritePlanProvider()!=null`（更重、less explicit）；capability 与 P0-2 模式一致且可扩展（未来按位置写连接器自声明）。
- **影响**：4 产线文件（`ConnectorCapability` SPI / `MaxComputeDorisConnector` / `PluginDrivenExternalTable` reader / `BindSink` bind + `PhysicalConnectorTableSink` 索引）+ `InsertUtils`。两写 capability 正交但有硬依赖（`SINK_REQUIRE_PARTITION_LOCAL_SORT` ⟹ `SINK_REQUIRE_FULL_SCHEMA_ORDER`，已 javadoc 登记，nit P03-V3-1）。**Batch-D 红线**：删 legacy `bindMaxComputeTableSink`/`PhysicalMaxComputeTableSink` 须待本 fix 落（已落）。**follow-up**：bind 投影无 fe-core 单测 harness → DV-014；真值闸 live e2e（p2 `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`）。

---

### D-029 — P4-T06e FIX-WRITE-DISTRIBUTION 新增 SPI capability SINK_REQUIRE_PARTITION_LOCAL_SORT

- **日期**：2026-06-07
- **状态**：✅（已落 commit `f0adedba20c`；live e2e 真值闸待真实 ODPS）
- **关联**：[FIX-WRITE-DISTRIBUTION 设计](./tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md)、[review-rounds](./reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md)、[复审报告 §A.NG-2/NG-4](./reviews/P4-maxcompute-full-rereview-2026-06-07.md)、[D-001]（capability 沿用先例）、[DV-013]、Batch-D 红线
- **背景**：翻闸后 MaxCompute 写走通用 `PhysicalConnectorTableSink`,其 `getRequirePhysicalProperties()` 只有 `supportsParallelWrite?RANDOM:GATHER`,且 `MaxComputeDorisConnector` 无 `getCapabilities` override（空集）→ 每写落 GATHER。丢 legacy `PhysicalMaxComputeTableSink` 的动态分区 hash-by-partition + 强制 local-sort（ODPS Storage API 流式分区 writer,见新分区即关上一 writer,未分组行触发 "writer has been closed"）+ 非分区/全静态并行写。通用 sink 从 JDBC/ES 克隆,无通道让连接器声明该需求。
- **决策（Option A）**：新增 `ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT`（连接器声明动态分区写需 hash-by-partition + 强制 local-sort）;MaxCompute `getCapabilities()` 声明它 + `SUPPORTS_PARALLEL_WRITE`;`PluginDrivenExternalTable.requirePartitionLocalSortOnWrite()` 读之（镜像 `supportsParallelWrite()`,经 `connector.getCapabilities().contains(...)`）;`PhysicalConnectorTableSink.getRequirePhysicalProperties()` 重写 legacy 3 分支。**关键修正 vs legacy**：分区列 → child output 索引按 **cols 位置**（通用 sink 的 child 投影到 cols 序,`BindSink` 强制 `cols.size()==child output size`）,非 legacy 的 full-schema 位置。default 不声明 → 其他连接器零行为变更。
- **替代方案**：(B) 隐式 derive（`supportsParallelWrite && hasPartition && dynamic → 强制 hash+local-sort`）—— 拒：把 MC Storage-API 的 local-sort 政策强加到所有并行写分区连接器（含 per-partition 缓冲、本不需 sort 的）;(C) `ConnectorWriteOps` 方法（仿 `supportsInsertOverwrite`）—— 拒：sink 读它需在 property-derivation 热路建 `ConnectorSession` + `getMetadata`,而 sibling `supportsParallelWrite()`（同方法内读）用更廉价的 `getCapabilities()` 集,不一致。
- **影响**：fe-connector-api（1 枚举值）+ fe-connector-maxcompute（`getCapabilities`）+ fe-core（1 table 方法 + sink 3 分支重写）。blast radius：`SUPPORTS_PARALLEL_WRITE`/新能力仅 sink 分发路径读（grep 实证 2+1 reader;唯一另一 `getCapabilities` consumer `QueryTableValueFunction` 查 `SUPPORTS_PASSTHROUGH_QUERY`,MC 不声明 → 不受影响）。**Batch-D 红线**：删 `PhysicalMaxComputeTableSink`（写分发唯一逻辑副本）须待本 fix + P0-3 双落。`ShuffleKeyPruner` non-strict 少剪 + `enable_strict_consistency_dml=false` 丢 local-sort = [DV-013]。

### D-028 — 翻闸功能未完整,补 P4-T06c FE 分发接线（用户签字）

- **日期**：2026-06-07
- **状态**：✅（翻闸前置工作;实现 = P4-T06c,下一 session）
- **关联**：[tasks/P4](./tasks/P4-maxcompute-migration.md)（新增 P4-T06c）、[HANDOFF](./HANDOFF.md)「⚠️ 关键发现」、[D-027]（翻闸落地）、[Batch D 设计](./tasks/designs/P4-batchD-maxcompute-removal-design.md)（前置门 + §2 处置随之改）、DV-007（`listPartition*` 零 live caller）
- **背景**：用户问「如何做 live 验证 / 验证哪些内容」。并行 recon（catalog 建法 / smoke SQL / SPI 路径映射 / build-deploy）+ **代码逐条核实** 暴出：T05/T06 翻闸**只接通**了 读(SELECT,`PluginDrivenScanNode`)/CREATE TABLE(`PluginDrivenExternalCatalog.createTable:257` override)/写(INSERT 全家,G1–G5)。**未接通**（live 会 FAIL,均 file:line 核实）：
  - **DROP TABLE / CREATE DB / DROP DB**：`PluginDrivenExternalCatalog` **不** override 这些、`metadataOps` **永远 null** → `ExternalCatalog.dropTable:1105`/`createDb:1004`/`dropDb:1029` 抛 `... is not supported for catalog`。（RENAME TABLE 同,且连接器侧未 port。）
  - **SHOW PARTITIONS**：`ShowPartitionsCommand:202-207` allow-list 仍按 `instanceof MaxComputeExternalCatalog`,翻闸后 catalog 是 `PluginDrivenExternalCatalog` → `not allowed`。
  - **partitions() TVF**：`MetadataGenerator.partitionMetadataResult:1308-1319` `instanceof MaxComputeExternalCatalog` 落空 → `not support catalog`。
  - 连接器侧 `createDatabase/dropDatabase/dropTable`（P4-T01）+ `listPartitionNames/listPartitions/listPartitionValues`（P4-T02）**已实现但 FE 零调用方**（DV-007 已记）。tasks/P4 §批次依赖原写「翻闸即 读/写/DDL/分区/show 全切 SPI」**与代码不符**,已纠正。
- **决策（用户 AskUserQuestion 签字,选「翻闸前全补接线」）**：视翻闸为**未完成**;Batch D 之前插 **P4-T06c**,把 DDL（createDb/dropDb/dropTable）+ SHOW PARTITIONS + partitions() TVF 的 **FE 分发接到已有连接器 SPI**。要点：
  - **通用实现**（keyed on `PluginDrivenExternalCatalog` / `PLUGIN_EXTERNAL_TABLE`,**非 MC 专有**）→ ① 同时修 jdbc/es/trino 同类缺口;② 让 Batch D §2 对 `ShowPartitionsCommand`/`MetadataGenerator`/`PartitionsTableValuedFunction` 的处置从 **delete-branch** 退化为**删残留 legacy MC 引用**（先 rewire 后删,解 Batch D 设计 §2 与 RFC `:1065`/master-plan `:126` 的删-vs-rewire 冲突）。
  - DDL override 镜像现有 `createTable:257`（路由 `connector.getMetadata().{createDatabase/dropDatabase/dropTable}` + editlog）。SHOW PARTITIONS / partitions TVF 加 `PluginDrivenExternalCatalog` 分支路由 `listPartitionNames`。
  - **本任务只补 FE 接线**（连接器方法已存在）= "接线"非"重写"。
- **scope 边界**：`partition_values()` TVF（`MetadataGenerator:2080` HMS-only）**不入 T06c**（OQ-5：legacy MC 很可能本就不支持 = 既有限制非回归,待确认）。RENAME TABLE 需连接器先 port,次要/可推迟（不在 live smoke 列表）。
- **完成门**：T06c 落（fe-core gate + UT）→ **用户报 live 验证全绿**（[D-027] D-1 的 `OdpsLiveConnectivityTest` + 手测 smoke 11 项全绿）= 翻闸真正完成 → 才解锁 Batch D。**flip 在 live 绿前保持独立可 revert**（沿 [D-027] D-1）。

> ⚠️ **2026-06-08 补注（DG-1 / D-031）**：本决策的「分区」接线指**元数据可见性**（SHOW PARTITIONS / partitions TVF），由 T06c + FIX-PART-GATES 落地。**read-session 分区裁剪下推**（把 Nereids 算出的 `SelectedPartitions` 真正喂到 ODPS）**不在 T06c/D-028 范围**，且后续复审 DG-1 证伪了 FIX-PART-GATES「pruning 不变式 clean」的过度声明——由 **FIX-PRUNE-PUSHDOWN（D-031）** 补齐。即：D-028/T06c 恢复元数据可见性 ✅、read-session 裁剪下推 = D-031 ✅。

- **日期**：2026-06-07
- **状态**：✅（翻闸已落、gate 全绿；Batch D 移除 = 待 live 验证后做）
- **背景**：用户要求「开始下一步（T06b 翻闸）」+ 追加「fe-core 不再依赖任何 maxcompute jar」。recon（并行 re-grep + 对抗验证，OQ-3 入口门满足）证：fe-core `odps-sdk-core`/`odps-sdk-table-api` 仅经 legacy MaxCompute 子系统（7 文件 `import com.aliyun.odps`，全在删除集）可达 → 去依赖 = 删整套 legacy（21 文件）+ 清 ~30 反向引用（即整个 Batch D）。
- **决策**：
  - **翻闸（T06b）**：`CatalogFactory.SPI_READY_TYPES += "max_compute"`(:52) + 删 `case "max_compute"`(原 :146-149) + 删 unused import + 注释去 max_compute。gate 全绿（compile BUILD SUCCESS/MVN_EXIT=0 + checkstyle 0/CS_EXIT=0 + import-gate 0，真实 EXIT 核）。
  - **D-1（时序）= flip 先行、移除待 live 验证**：本任务只落 flip（独立可回退）；legacy 子系统删除 + pom odps drop（Batch D）挪到**用户跑 `OdpsLiveConnectivityTest`（4 个 `MC_*` 环境变量）+ 手测 smoke 绿之后**的紧邻 follow-up。理由：删 legacy 即去掉易回退的 fallback，故 flip 在 live 验证前保持独立可 revert（trino 翻闸亦 flip 先于删除）。
  - **D-2（依赖范围）= 仅删直接声明**：fe-core/pom.xml 删两 `odps-sdk-*` 块即可；fe-core 删后**零** odps 源引用，但仍经 fe-common transitive 见 `odps-sdk-core`（fe-common 留 odps 供 `MCUtils` → 连接器 + be-java-extensions），可接受（用户选 "Direct declarations only"）。镜像 trino `c4ac2c5911d`（只删 fe-core 直接声明）。
- **2 SPI 新增登记**（D-026 预授，default-preserving）：`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction` 录入 `01-spi-extensions-rfc.md` §20 E11。T06a 对抗复核已修 `PluginDrivenTableSink.getExplainString` 加 `writeConfig==null` 守卫（防 plan-provider 模式 EXPLAIN NPE，翻闸后可达）——记一笔。
- **设计文档（Batch D 执行源，turnkey）**：[tasks/designs/P4-batchD-maxcompute-removal-design.md](./tasks/designs/P4-batchD-maxcompute-removal-design.md)（21 删除集 + 84 反向引用闭包 + keep 集 + pom drop + ordered TODO；执行前置门 = live 验证绿）。

### D-026 — P4 Batch C 翻闸设计（3 子决策 + 2 SPI 新增，用户签字）

- **日期**：2026-06-06
- **状态**：✅（design-only；实现 = T05 → T06，下一 fresh session）
- **背景**：Batch A+B 全完成（gate 关 dormant），下一 = Batch C（唯一 live 切点）。本场 design-first：4 路 Explore re-verify recon 锚点 + 主线核读 executor/txn 生命周期，定 dormant→live 写接线（坑3 三点）+ flip + R-004。recon 校正：GsonUtils 真锚 `:397`/`:472`（非 ~405/~478）；`legacyLogTypeToCatalogType` 默认分支已出 `"max_compute"`（**无需加 case**）；live executor = `PluginDrivenInsertExecutor`（非裸 `beginTransaction`）；`PluginDrivenTransactionManager.begin(connectorTx)` **未** `putTxnById`（G3）；`UnboundConnectorTableSink` 不携静态分区（G4）。
- **决策**：
  - **D-1（capability signal）= (A)** 新增 `ConnectorWriteOps.usesConnectorTransaction()` default false，`MaxComputeConnectorMetadata` override true。executor 据此在调任何 throwing-default 写法（`getWriteConfig`/`beginInsert`/`beginTransaction` 全 default 抛、MC 留抛=D-4）前分流 txn-model（MC）vs JDBC insert-handle。否决 (B) `getWritePlanProvider()!=null` 代理（耦合松）/(C) 复用 `ConnectorWriteType`（逆 D-4 + enum churn + getWriteConfig 调用前移）。
  - **D-2（commit 粒度）= 两 commit、flip 末**：`[P4-T06a]` = 写接线（W-a..d）+ 静态分区/overwrite 绑定（G4/G5）+ R-004 隔离 UT（全 additive/dormant-safe）；`[P4-T06b]` = `CatalogFactory.SPI_READY_TYPES += "max_compute"`(:52) + 删 :146 case（唯一 live-switch 单点，易 review/revert）。
  - **D-3（静态分区/overwrite 绑定 scope）= 入 cutover（T06）**：扩 `UnboundConnectorTableSink` 携静态分区 + `InsertIntoTableCommand`/`InsertOverwriteTableCommand` 填 `PluginDrivenInsertCommandContext`（overwrite + staticPartitionSpec）。避免翻闸瞬间 INSERT OVERWRITE / 静态分区 INSERT 回归。
- **SPI 新增（2，均 default-preserving，零 jdbc/es/trino 影响）**：`ConnectorSession.setCurrentTransaction(ConnectorTransaction)`（+ `ConnectorSessionImpl` 字段/`getCurrentTransaction` override；把 connectorTx 绑入 sink session 供 T04 `planWrite` 读，解 G1）；`ConnectorWriteOps.usesConnectorTransaction()`（D-1）。impl 时登记 `01-spi-extensions-rfc.md` §20 E11。
- **不重开 T03/T04**：Approach A locked（`planWrite` 读 `getCurrentTransaction`）；本设计接线 *到* 它。R-004 拆两分：① classloader 隔离（无 creds，CI 可跑）+ ② live 连通（creds，用户跑）。
- **设计文档**：[tasks/designs/P4-T05-T06-cutover-design.md](./tasks/designs/P4-T05-T06-cutover-design.md)（verified file:line 锚点 + 5 gap G1–G5 + lifecycle order + R-004 两分测 + ordered TODO）。
- **T05 实现校正（2026-06-06，gate-green、待 commit）**：实现期 4-agent 对抗复核发现 §3.1/§8 ordered TODO **漏 GSON DB `:452`**（`MaxComputeExternalDatabase`，仅列了 catalog `:397`+table `:472`）；折入 T05（三注册齐迁 `registerCompatibleSubtype` + 删 3 unused import），否则翻闸后 `MaxComputeExternalDatabase.buildTableInternal:44` cast `PluginDrivenExternalCatalog`→`MaxComputeExternalCatalog` 抛 `ClassCastException`。另 2 告警判非问题（`getMetaCacheEngine` 假阳性=plugin 路径经连接器取 schema、走 "default" 桶同 es/jdbc/trino；`getMysqlType`→"BASE TABLE" 同 ES 既定行为）；dormancy 告警 = 既载中间态 caveat（其"保留 registerSubtype"修法错，会撞 duplicate-label IAE）。详见设计 §3.4。

### D-025 — P4-T04 写计划 5 决策（OQ-2 解法 + seam fill + 三主线定）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[tasks/P4 P4-T04](./tasks/P4-maxcompute-migration.md)、[P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)、[D-024]（T03/T04 边界、`setWriteSession` 槽）、[DV-009]（W5 planWrite layer）、[DV-012]（partition_columns 源）、OQ-2
- **背景**：T04 把 legacy 写计划（`MCTransaction.beginInsert` 建写 session + `MaxComputeTableSink.bindDataSink`/`setWriteContext` 产 `TMaxComputeTableSink`）港入连接器 over W5 opaque-sink seam。核心难点 OQ-2 = legacy 经 `MCInsertExecutor.beforeExec` **运行期注入**的 `txn_id`/`write_session_id`、overwrite/静态分区 context 需在 plugin-driven 侧重建。
- **决策**：
  - **D-1（OQ-2 架构，用户签字）= Approach A**：executor 生命周期序 `beginTransaction`(txn_id 译前生)→translate→`finalizeSink`/`bindDataSink(insertCtx)`→`beforeExec`→coordinator ⇒ `planWrite` 跑在 finalizeSink、txn_id 已在 + ODPS 写 session 可就地建 → **planWrite 一处做完**（建 session + `session.getCurrentTransaction()`→`MaxComputeConnectorTransaction.setWriteSession` + 盖 `txn_id`/`write_session_id`）。**无运行期注入 hook**（否决 Approach B = 泛化 legacy `setWriteContext` dance）。
  - **D-2a（fe-core seam 填充，用户签字）= 含 seam fill**：`PluginDrivenTableSink.bindViaWritePlanProvider` 改收 `Optional<InsertCommandContext>`、读 `isOverwrite()`+`getStaticPartitionSpec()` 填 handle；**实现期细化**：`staticPartitionSpec` 加在 `PluginDrivenInsertCommandContext`（非设计「Why」倾向的基类 `BaseExternalTableInsertCommandContext`）——因 `MCInsertCommandContext` 已自带 `staticPartitionSpec`+getter 且 shadow 基类 `overwrite`，加基类会成 override/shadow 缠结（Rule 3 surgical）；plugin-driven seam 只见 `PluginDrivenInsertCommandContext`，post-migration hive/iceberg 复用同类，复用目标仍满足。在设计「`PluginDrivenInsertCommandContext`（或基类）」envelope 内。
  - **D-3（EnvironmentSettings 复用，主线定）= 抽 `MaxComputeDorisConnector.getSettings()`**：决定性证据——legacy `MaxComputeExternalCatalog` 持**单** `settings` 字段同供 scan（`MaxComputeScanNode`）+ write（`MCTransaction.beginInsert`），故抽出共用是**忠实港 legacy 设计**（非投机重构，化解 Rule 3 张力）；scan provider :146-162 构造上移、scan/write 共用。连接器 gate 关 dormant，动 scan 零 live 风险。
  - **D-4（insert 机制面，主线定）= `supportsInsert()`=true 余最小化**：MC sink 经 `planWrite`、commit 经 `ConnectorTransaction.commit()`，故 `beginInsert`/`finishInsert`/`getWriteConfig` 留 throwing-default（无 MC 实质活）；实际 executor 调用面以 Batch C 为准（不投机加 no-op，Rule 2；显式 doc 不静默，Rule 12）。
  - **D-5（writeContext 编码，主线定）= 静态分区作 `getWriteContext()` 的 col→val map**；overwrite 经 `isOverwrite()`。planWrite 据 ODPS 分区列序拼 `"col=val,..."` 喂 `PartitionSpec`、原样 set 入 `static_partition_spec`(field 10)。
- **影响**：T04 dormant（gate 关，plan-provider 分支无 live caller）；binding 期填充 `PluginDrivenInsertCommandContext.staticPartitionSpec`/overwrite 归 Batch C/D（坑3，`InsertIntoTableCommand:598` 现传空 ctx）；planWrite `getCurrentTransaction()` 要返 MC txn ⇒ Batch C `beginTransaction`→置 `ConnectorSessionImpl`。T04 不新增 SPI 面（W1 全建）。立 paimon/iceberg/hive 写-plan adopter 样板。

---

### D-024 — P4-T03 写/事务 SPI 两 fork（txn id 机制 + T03/T04 边界）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[tasks/P4 P4-T03](./tasks/P4-maxcompute-migration.md)、[P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md)、[D-015]/U3（getTransactionId 连接器分配）、[D-022]（写 SPI）、[01-spi-extensions-rfc E11](./01-spi-extensions-rfc.md)
- **背景**：handoff 标注 T03/T04 未逐行定稿；recon 暴两处需拍板的 fork（[D-015]「连接器分配 id」对 MC 不成立——MC 无外部 id 且连接器够不到 `Env.getNextId`；写 session 创建需 overwrite/静态分区 context = OQ-2）。
- **决策**（用户 AskUserQuestion 签字 2026-06-06）：
  - **Fork 1（txn id）**：给 `ConnectorSession` 加 `default long allocateTransactionId()`（default 抛；fe-core `ConnectorSessionImpl` override 回 `Env.getCurrentEnv().getNextId()`），MC `beginTransaction` 经它分配。**仍属「连接器分配」语义**（经注入的引擎分配器），尊重 [D-015]；id 即 Doris 全局 txn_id，与 sink `txn_id` / `GlobalExternalTransactionInfoMgr` 一致。SPI 加面记 E11。
  - **Fork 2（T03/T04 边界）**：ODPS 写 session 创建挪 **T04 planWrite**（`ConnectorWriteHandle` 带 overwrite+writeContext，顺解 OQ-2）；**T03 = 纯事务容器**（commitDataList/nextBlockId/writeSessionId 槽 + addCommitData[TBinaryProtocol]/block-alloc/commit[港 finishInsert]/rollback/getUpdateCnt）+ `beginTransaction`。
- **影响**：executor 接线（`beginTransaction`→`begin(connectorTx)`）+ `GlobalExternalTransactionInfoMgr` 注册推迟翻闸期（Batch C），保 T03 dormant、不破 JDBC/ES。立 paimon/iceberg/hive 后续事务 adopter 的 id-source 样板。

---

### D-023 — P4 maxcompute 启 full adopter（option A，5 批 cutover）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[tasks/P4-maxcompute-migration.md](./tasks/P4-maxcompute-migration.md)、[research/p4-maxcompute-migration-recon.md §9](./research/p4-maxcompute-migration-recon.md)、[D-021]（scope=C→本决策接 option A）、[D-022]（写 SPI）、[写 RFC §12](./tasks/designs/connector-write-spi-rfc.md)、[R-004]
- **背景**：W-phase（W1–W7）已落地共享写/事务 SPI + 通用层解耦（[D-021]/[D-022]），recon §9 scope fork（B hybrid / A full / C 写-SPI 先行）中 C 已完成、写路径 keystone 已解耦。现决 P4 余下走 **option A（full adopter + 翻闸）**，非 P3 式 hybrid。
- **决策**（用户批准 2026-06-06）：按 [tasks/P4](./tasks/P4-maxcompute-migration.md) 的 **5 批 / 11 task** 落地：A 连接器读/DDL/分区 parity（gate 关）→ B 写/事务 SPI（gate 关）→ **C 翻闸（唯一 live 切点，含 R-004 防御测）** → D 清 ~19 反向引用 + 删 `datasource/maxcompute/`（收口 P1-T02 McStructureHelper 去重）→ E 连接器测试基线 + PR。A、B 并行、均 dormant；两者全绿 + R-004 过方进 C。
- **影响**：P4 成首个 full adopter，为 P5 paimon / P6 iceberg / P7 hive 立样板。recon §3「~36 反向引用」经 post-W-phase re-grep 校正为 **~19**（W-phase 灭 `Coordinator`/`LoadProcessor`/`FrontendServiceImpl` 3 热点 txn 站，grep 证）。每批独立 commit。

---

### D-022 — 写/事务 SPI 设计（A / B1 / C1 / D / E）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[写/事务 SPI RFC](./tasks/designs/connector-write-spi-rfc.md)、[research/connector-write-spi-recon.md](./research/connector-write-spi-recon.md)、[D-021]（scope=C）、[D-009]（default-only）、[01-spi-extensions-rfc.md E11](./01-spi-extensions-rfc.md)、W-phase commits（W1+W2 `be945476ba7`、W3+W6 `9ad2bbe40ec`、W4 `759cc0874c8`、W5 `9ebe5e27fa4`）
- **背景**：P4 maxcompute recon 证它在热路径会写（`MCTransaction` 在 `Coordinator`/`FrontendServiceImpl`/`LoadProcessor` concrete cast）；写路径 = 翻闸 keystone。三现存写者 maxcompute/hive/iceberg 同写生命周期 ⊥ 三处分歧（commit 载荷型 / mc block-id / iceberg procedures+delete/merge），paimon 今读后写需前瞻。须定写/事务 SPI 形状。
- **决策**（用户签字 2026-06-06）：
  - **A 事务模型统一·桥接**：连接器 `ConnectorTransaction` 为单一事实源；fe-core 通用写编排经 `PluginDrivenTransaction`（`PluginDrivenTransactionManager` 产）桥接，只调多态 fe-core `Transaction`；现存 `MC/HMS/IcebergTransaction` 过渡期 override 适配，逐连接器迁入 plugin。
  - **B1 commit 载荷 opaque bytes**：BE→FE commit 载荷（`TMCCommitData`/`THivePartitionUpdate`/`TIcebergCommitData`）`TBinaryProtocol` 序列化为 `byte[]`，经 `Transaction.addCommitData(byte[])` / `ConnectorTransaction.addCommitData` 交连接器反序列化。零 BE 改、保全富信息、消除 3 处 concrete cast。留一处序列化 shim（fail-loud，Open-1）。
  - **C1 block-id 窄 callback seam**：`Transaction.supportsWriteBlockAllocation()` + `allocateWriteBlockRange()` 默认方法，仅 maxcompute override，消 `FrontendServiceImpl` `instanceof MCTransaction`。拒 C2 过度泛化 / C3 留特例。
  - **D INSERT/DELETE/MERGE**：SPI 形状定全；实现 mc/hive=insert、iceberg=+delete/merge（P6）。**defer**：iceberg procedures（E2/P6）、hive 行级 ACID、各连接器代码搬迁（adopter 阶段）。
  - **E 写-plan-provider 仿 scan**：连接器经 `ConnectorWritePlanProvider.planWrite()` 产 opaque `TDataSink`（仿 `ConnectorScanPlanProvider`）；`Connector.getWritePlanProvider()` default null。
- **替代方案**：B2 中立 envelope（丢富信息，否决）/ B3 thrift union 漏进 SPI（否决）；C2/C3（否决）。见 RFC §11。
- **影响**：W-phase（W1–W7）落地共享 SPI 面 + 通用层解耦，**behind gate、零行为变更、golden 等价**；逐连接器 adopter（P4 mc / P6 iceberg / P7 hive）后续。新方法均 default（满足 [D-009]），BE 契约不变。W5 落地暴露 [DV-009]（写 sink 收口位置修正）。

---

### D-021 — P4 maxcompute 采 scope=C（写-SPI RFC 先行）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[research/p4-maxcompute-migration-recon.md](./research/p4-maxcompute-migration-recon.md)、[写/事务 SPI RFC](./tasks/designs/connector-write-spi-rfc.md)、[D-022]（写 SPI 设计）、[connectors/maxcompute.md](./connectors/maxcompute.md)
- **背景**：P4 启动 recon 发现 maxcompute 在热路径**会写**（非只读骨架），写路径是翻闸前提。可选 scope：A 仅迁读+推迟写；B 连写一起但不先定 SPI；**C 写-SPI RFC 先行**（先设计共享写/事务 SPI + 通用层解耦，再迁连接器）。
- **决策**（用户签字 2026-06-06）：采 **scope=C**——先出写/事务 SPI RFC（[D-022]）并落 **W-phase**（共享解耦 + SPI 面，gate 不动、零行为变更），再做 maxcompute full adopter（搬类 + impl 写 SPI + 翻闸）。理由：写面是 mc/hive/iceberg 共享 keystone，先收口避免每连接器重造、降低反向 instanceof 清理风险。
- **影响**：P4 在 adopter 前插入 W-phase（写 RFC 直接后续）；hive(P7)/iceberg(P6) 复用同一 SPI。W-phase 不翻闸、不搬类、不删 legacy。

---

### D-020 — 单 `hms` catalog 多格式 scan 路由 = 方案 B（per-table SPI provider）

- **日期**：2026-06-05
- **状态**：✅ 生效
- **关联**：[D-005](#d-005)（被细化）、[D-009](#d-009)（default-only 约束）、[D-019](#d-019)（hybrid）、[tasks/P3 T08](./tasks/P3-hudi-migration.md)、[designs/P3-T08-tableformat-dispatch-design.md](./tasks/designs/P3-T08-tableformat-dispatch-design.md)、[research/spi-multi-format-hms-catalog-analysis.md](./research/spi-multi-format-hms-catalog-analysis.md)
- **背景**：legacy 单 `hms` catalog 靠 `HMSExternalTable.dlaType` per-table tag + 处处 `switch(dlaType)` 同时暴露 Hive/Hudi/Iceberg。SPI 侧 `ConnectorTableSchema.tableFormatType` **产而不用**——`PluginDrivenExternalTable.initSchema:79-109` 只读 columns、`Connector.getScanPlanProvider:40-42` per-catalog 单点、`HiveScanPlanProvider` 硬编码 `tableFormatType="hive"`（research §6①②③ + 本场 firsthand 核读）。T08（批 D，design-only）须定 per-table 路由 seam；研究浮现三互斥方案（A 连接器内 router / B per-table SPI provider / C fe-core 发现期分派）。
- **决策**：M2 scan 路由采 **方案 B**——在 `ConnectorMetadata` 新增**向后兼容 default** `getScanPlanProvider(ConnectorTableHandle handle)`（默认返 null → fe-core 回落 per-catalog `Connector.getScanPlanProvider()`）；fe-core `PluginDrivenScanNode.getSplits` 优先 per-table provider、回落 per-catalog；注册 `"hms"` 的连接器 override 之、按 `handle.getTableType()` 委派 Hudi/Iceberg provider。把"per-table 选 provider"升为一等 SPI 契约。配套 **M1**（fe-core 按缓存的 `tableFormatType` 做 per-table 引擎名/身份，作 opaque 串逐字上报、热路径不读）三方案通用。**design-only，实现 = 批 E/P7**。
- **替代方案**：**A 连接器内 router**（`Connector.getScanPlanProvider()` 返回一个 `planScan` 按 `handle.getTableType()` 委派的 router）——零 SPI churn（`planScan` 已带 handle，本场核实），但路由藏进连接器、per-table 语义非一等契约；列为备选，批 E 实现期可据 iceberg 接入复杂度复核。**C fe-core 发现期分派**（fe-core 读 `tableFormatType` 建 format-specific 表对象，≈legacy DLAType→多态 DlaTable）——**否决**：fe-core 回退到 per-format 分派，违背瘦 fe-core 北极星（import-gate / D-003 / D-006）。
- **影响**：**细化 [D-005]**——D-005 的"`tableFormatType` 区分符"结论沿用；但其"fe-core dispatch 到对应 `PhysicalXxxScan`"措辞（2026-05-24，**早于 P1 scan-node 统一**为单 `PluginDrivenScanNode` + per-range format）由 per-table provider seam 取代（SPI 路径已无 per-format `PhysicalXxxScan`）。批 E/P7 据此实现 M1+M2；新 default 方法满足 [D-009]（不破签名）。Iceberg-on-hms 经 SPI 依赖 **P6** 先补 `IcebergScanPlanProvider`（M3）；hms 网关引入对 `-hudi`/`-iceberg` 模块依赖边（A/B 同担）。**本场无代码改动**。

---

### D-019 — P3 hudi 采用 hybrid 推进策略

- **日期**：2026-06-04
- **状态**：✅ 生效
- **关联**：[DV-005](./deviations-log.md)、[D-005](#d-005)、[tasks/P3](./tasks/P3-hudi-migration.md)、master plan §3.4/§3.8
- **背景**：两轮 code-grounded recon（+ 对抗验证）揭示：HMS-over-SPI 读码已存在但 dormant（gate 关、零 live caller）；scan/split plumbing 正确（单 `PluginDrivenScanNode` 混合 COW-native+MOR-JNI 非问题，与 legacy 结构等价）；真正阻塞是 catalog 模型错配（独立 `"hudi"` type vs 寄生 `"hms"` 的 `DLAType.HUDI`，fe-core 不消费 `tableFormatType`）+ 关闭的 gate；另有一批**与模型无关**的 SPI-surface 正确性缺口（`schema_id`/`history_schema_info` 缺、`column_types` 双 bug、time-travel 静默返最新、增量读无表示、partition 裁剪缺、三模块零测试）。
- **决策**：P3 走 **hybrid**。**现在做 (b)**（批 A–D，全部 behind 关闭的 gate，零 live-path 风险）：hudi 连接器 model-agnostic 正确性修复 + metadata 补全 + 测试基线 + 模型 dispatch 设计（design-only）。**推迟 (a)**（批 E，登记不编码）：fe-core 消费 `tableFormatType` 的 per-table 分流、gate flip（`SPI_READY_TYPES` 加 hms/hudi）、live cutover、删 legacy `datasource/hudi/`、完整增量/time-travel、集群/runtime 验证 —— 并入一个 properly-scoped hive/HMS migration（P7 或专门子阶段）。
- **替代方案**：(a) **hms-first 一次到位** —— 否决为 P3 首交付（把 P7 范围拉进 P3、re-route live 重度使用的 HMS 路径、零测试网，回归风险大）；(c) **直接 flip gate** —— 早已否决（模型错配下 `"hudi"` provider 不可达 + 高回归）。
- **影响**：P3（hybrid）**不交付用户可见行为变化**（hudi 仍走 legacy，gate 不翻）；产出是连接器硬化 + 测试网 + 设计。批 A–C 验证为单测/设计级，端到端/集群验证随批 E cutover。tasks/P3 据此划批。

---

### D-018 — `ConnectorColumnStatistics` 类型安全契约（原 U6）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §11.2](./01-spi-extensions-rfc.md)
- **背景**：`ConnectorColumnStatistics.minValue / maxValue` 用 `Object` 装载，缺少静态类型检查可能导致 connector 间不一致。
- **决策**：在 `ConnectorColumnStatistics` javadoc 中列出 `ConnectorType` ↔ Java 装箱类型完整映射表（如 INT→Integer、TIMESTAMP→Instant、BINARY→byte[]）；连接器读取不匹配类型时**抛 `IllegalArgumentException`**，由 fe-core 转成 `UserException`。
- **替代方案**：（a）引入泛型 `ConnectorColumnStatistics<T>`——过于复杂、跨方法签名传染；（b）引入 union 类型——Java 不原生支持。
- **影响**：仅 javadoc 与运行时检查，无签名变化。

---

### D-017 — sys-table 命名统一 `$suffix`（原 U5）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §10](./01-spi-extensions-rfc.md)
- **背景**：Iceberg / Paimon 各自有 sys-table（`tbl$snapshots`、`tbl$history` 等）。命名风格 `$xxx` vs `xxx@` vs `[xxx]` 跨方言不一致。
- **决策**：SPI 层固定 `$suffix` 约定。如未来出现冲突（如某 SQL dialect 把 `$` 视为变量前缀），通过 catalog property `sys_table_separator` 提供别名机制，但**不在本计划范围**。
- **影响**：所有 sys-table 实现统一遵循。

---

### D-016 — `getCredentialsForScans` 批量化（原 U4）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §9](./01-spi-extensions-rfc.md)
- **背景**：原设计单 range 调一次 `getCredentialsForScan`，N 个 range 触发 N 次 STS 调用，可能撞限流。
- **决策**：签名定为 `Map<ConnectorScanRange, ConnectorCredentials> getCredentialsForScans(session, handle, List<ConnectorScanRange>)`。连接器自由决定 STS 调用粒度（1 次共享 / 按 prefix 分组 / 1:1）。fe-core 一个 scan node 一次调用。
- **替代方案**：保持单个 + 加内部缓存——把缓存策略推给每个 connector，不一致风险更高。
- **影响**：替换原 `getCredentialsForScan` 单个签名。调用位置从 `setScanParams` 移到 `createScanRangeLocations`。

---

### D-015 — `ConnectorTransaction.getTransactionId` 由连接器分配（原 U3）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §7.2](./01-spi-extensions-rfc.md)
- **背景**：transaction ID 是连接器自己分配还是 fe-core 统一分配？
- **决策**：连接器分配。连接器最清楚事务 ID 与外部系统（如 HMS transaction id、Iceberg snapshot id）的对应关系。fe-core 在 `PluginDrivenTransactionManager` 用 `Map<Long, ConnectorTransaction>` 索引即可。
- **影响**：`ConnectorTransaction.getTransactionId()` 是 connector-side 字段。

---

### D-014 — 不新增 `invalidateColumnStatistics`（原 U2）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §6](./01-spi-extensions-rfc.md)
- **背景**：是否给 `ConnectorMetaInvalidator` 加 `invalidateColumnStatistics(...)`？
- **决策**：暂不加。column stats 失效一并挂在 `invalidateTable` 上，避免接口表面膨胀。如后续发现频繁需要单独失效列统计，再加方法（向后兼容 default 即可）。
- **影响**：`ConnectorMetaInvalidator` 接口保持 5 个方法（catalog / database / table / partition / statistics 整张表）。

---

### D-013 — `ConnectorProcedureOps.listProcedures` 一次性返回（原 U1）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §5.2](./01-spi-extensions-rfc.md)
- **背景**：connector 暴露的 procedure 列表是初始化时固定还是允许运行时变化？
- **决策**：一次性。Connector 生命周期内稳定；如外部系统的可用 procedure 集合变化，必须重新创建 catalog。
- **理由**：fe-core 可缓存该列表用于 `SHOW PROCEDURES`、autocompletion；动态变化模型复杂度不值得。
- **影响**：在 `listProcedures()` 的 javadoc 中明确写出"Lifecycle contract"。

---

### D-012 — Connector 安装初版强制重启 FE（原 D12）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：装新 connector 后是否要求重启 FE？
- **决策**：初版强制重启。原因：跨连接器共享类型可能有 classloader 缓存问题，强制重启避免难复现的 corner case。后续版本可考虑热加载。
- **影响**：文档明确 + 装包流程明确。

---

### D-011 — `RemoteDorisExternalCatalog` 不在本计划主线（原 D11）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：Doris-to-Doris federation 是否做成 connector？
- **决策**：长期目标做 connector，但**单独立项**，不在本计划主线（25 周计划中）。
- **影响**：`RemoteDorisExternalCatalog` 在 P8 不删除；保留独立路径。

---

### D-010 — `LakeSoulExternalCatalog` 在 P8 删除（原 D10）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`CatalogFactory` 已抛 "Lakesoul catalog is no longer supported"，但类文件仍在。
- **决策**：在 P8 收尾时删除剩余 `datasource/lakesoul/` 全部类。
- **影响**：P8 task 增加 lakesoul 清理项。

---

### D-009 — API 版本号本计划永不 +1（原 D9）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)、[01-spi-extensions-rfc.md §2.1](./01-spi-extensions-rfc.md)
- **背景**：`ConnectorProvider.apiVersion()` 何时 +1？
- **决策**：本计划范围内（25 周）保持 `apiVersion=1`，只新增 default 方法，不破坏现有签名。
- **影响**：所有 SPI 扩展必须用 default 方法。如真有不可避免的 breaking change，需走 deviation 流程并升级到 v2。

---

### D-008 — 生产强制目录式插件（原 D8）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：是否允许 built-in connector（classpath 中直接打进 FE jar）？
- **决策**：否。built-in 模式只用于测试（ServiceLoader 扫 classpath）；生产部署必须从 `connector_plugin_root` 目录加载 plugin zip。
- **影响**：FE 发行包不含 connector jar；运维流程文档要明确插件部署步骤。

---

### D-007 — kafka/kinesis/odbc/doris 不在本计划范围（原 D7）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`datasource/` 下还有 kafka / kinesis / odbc / doris 子目录，是否一并迁移？
- **决策**：否。流式数据源（kafka/kinesis）与外部 catalog 模型不同；odbc 是 BE-driven；doris 是内部联邦。单独立项。
- **影响**：P8 不删除这 4 个子目录。

---

### D-006 — Iceberg snapshot/manifest cache 放连接器内（原 D6）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)、[01-spi-extensions-rfc.md §8](./01-spi-extensions-rfc.md)
- **背景**：Iceberg 的 snapshot cache 和 manifest cache 是 fe-core 通用基础设施还是连接器内部细节？
- **决策**：连接器内部细节。fe-core 不感知。连接器自己管理生命周期、淘汰策略。
- **替代方案**：放 `fe-core/datasource/metacache/` 通用框架——会增加 fe-core 对 Iceberg 概念的耦合。
- **影响**：P6 迁移时把 `cache/IcebergManifestCacheLoader` 等整体搬到 `fe-connector-iceberg`。

---

### D-005 — Hudi / Iceberg-on-HMS DLA 模型方案 A（原 D5）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §3.4](./00-master-plan.md)
- **背景**：HMS 表可能"实际是" Hudi 或 Iceberg。如何在 SPI 层建模？
- **决策**：方案 A — 用 `ConnectorTableSchema.tableFormatType` 字段（值如 `"HIVE"` / `"HUDI"` / `"ICEBERG"`），由 HMS connector 探测后填充；fe-core 据此 dispatch 到对应 `PhysicalXxxScan`。
- **替代方案**：方案 B — Hudi 作为独立 catalog type，内部委托 HMS——增加 catalog 实例数，用户混淆度高。
- **影响**：P3 hudi 和 P7 hive 迁移都依赖此模型。

---

### D-004 — HMS event pipeline 放 fe-connector-hms（原 D4）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §3.8](./00-master-plan.md)、[01-spi-extensions-rfc.md §6](./01-spi-extensions-rfc.md)
- **背景**：21 个 HMS event 类放 fe-core 还是 fe-connector-hms？
- **决策**：fe-connector-hms。通过新 SPI 接口 `ConnectorMetaInvalidator`（在 `ConnectorContext` 暴露）回调 fe-core 的 `ExternalMetaCacheMgr`。
- **替代方案**：只把"轮询 HMS 拿事件流"放 connector，"解析事件 + 分发失效"留 fe-core——分散，不利于演化。
- **影响**：P7.2 完整迁移 21 个类 + `MetastoreEventsProcessor`。`HiveConnector.create(...)` 启动 listener 线程；`close()` 停止。

---

### D-003 — 旧 `*ExternalCatalog` 子类全部删除（原 D3）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：迁移过程中是保留旧 `IcebergExternalCatalog` 等类作为"中间形态"还是彻底删除？
- **决策**：全部删除。中间形态会让代码长期处于"两套并存"状态，维护负担、bug 风险都更大。
- **替代方案**：保留一段"deprecated 但可用"期——拒绝，因为旧实现实质上不会被维护。
- **影响**：P8 强制删除所有 `*ExternalCatalog` / `*ExternalDatabase` / `*ExternalTable` 类；前置工作是 P2-P7 把所有反向 `instanceof` 改为通用接口调用。

---

### D-002 — `PluginDrivenScanNode` 长期保持 extends `FileQueryScanNode`（原 D2）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`PluginDrivenScanNode` 当前继承 `FileQueryScanNode`，但 JDBC / ES 本质不是文件扫描，用 `FORMAT_JNI` 兜底。是否要重构为更彻底的多态？
- **决策**：长期保持当前继承结构。JDBC / ES 的 `FORMAT_JNI` 兜底已被 ES/JDBC 验证可行。重构成本高、收益不明确。
- **影响**：所有 plugin-driven connector 走同一 scan-node 子类，简化 dispatch 逻辑。

---

### D-001 — 沿用 `SUPPORTS_PASSTHROUGH_QUERY`（原 D1）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：是否要为 SQL 透传以外的远程 query 类型（如 `query()` TVF）新增 SPI？
- **决策**：不新增。已有 `ConnectorCapability.SUPPORTS_PASSTHROUGH_QUERY` + `ConnectorTableOps.getColumnsFromQuery` 覆盖了主要场景，沿用。
- **影响**：无新增 API。

---

## 附录：决策模板

新增决策时复制以下模板到顶部（在 §详细记录 下方），并更新 §📋 索引表。

```markdown
### D-NNN — <一句话主题>

- **日期**：YYYY-MM-DD
- **状态**：✅ 生效 / 🟡 待评审 / ❌ 已废止（被 D-MMM 取代）
- **关联**：[文档章节链接]、[相关 task ID]
- **背景**：为什么需要做这个决策？触发场景是什么？
- **决策**：具体决定是什么？
- **替代方案**：考虑过哪些其他方案？为什么没选？
- **影响**：哪些代码 / 文档 / 流程会受影响？是否需要后续 follow-up？
```
