# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里，见下「起步必读」）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸）。

---

# 🆕 下一个 session 起步 = FIX-HIVEFS 续（下一步 = 去 jar HIVEFS-7 + 全量 build/e2e HIVEFS-8，收尾）

> 本地 hive 回归 `test_string_dict_filter` q01 `No FileSystem for scheme "hdfs"` 引出的**架构性改造**（非环境/文案）。**起步必读：设计 `plan-doc/tasks/designs/FIX-HIVEFS-design.md` + 任务清单 `plan-doc/tasks/task-list-HIVEFS.md`（行号信 HEAD 不信文档）。连接器读+ACID+写三条裸 Hadoop 路径已全部转引擎注入 FileSystem（HIVEFS-4/5/6 DONE），只剩去依赖 jar + 全量验证。**

**根因**：连接器扫描/写全程用**裸 `org.apache.hadoop.fs.FileSystem`**（`HiveFileListingCache:162`/`HiveScanPlanProvider:272`/`HiveAcidUtil`/`HiveConnectorTransaction`），但 hive 插件类加载器隔离、lib/ 无 `DistributedFileSystem`（仅 hadoop-common，注册 Local/View/Har/Http、无 hdfs）。老 fe-core 经 `org.apache.doris.filesystem.FileSystem`+`DirectoryLister` 列文件（`HiveExternalMetaCache:392-396`），**新代码裸 Hadoop 是迁移走样**。非环境问题（hostname 可解析、metastore 通、已取到 location）。

**已签字决策（2026-07-11）**：① 走**正解 B**——引擎经 `ConnectorContext.getFileSystem(ConnectorSession)` 下发 Doris `FileSystem`（实为 `SpiSwitchingFileSystem`），连接器改调 `listFiles/exists/rename/delete/forLocation`、**删 `hadoop-hdfs-client`**；**对齐 Trino**（`TrinoFileSystemFactory.create(session)`→`TrinoFileSystem`，连接器不 bundle Hadoop、Hadoop 可选仅 HDFS）。② **一步到位**（读+ACID+写全转）。③ SPI 照 Trino 形状留 `session`（identity 经 `getUser()` **预留 per-user**，当前 catalog 级）。④ scope=**hive-only**（paimon/iceberg 经各自 `FileIO`，不动）。

**✅ 本轮完成 = 基础 SPI 缝 + 引擎实现 HIVEFS-0/1/2/3（均构建+靶向 UT 验证、0 checkstyle、已 commit）**：
- **HIVEFS-0**：撤销上会话未提交的 `hadoop-hdfs-client` 创可贴（pom 回 HEAD，无 commit）。
- **HIVEFS-1** `0c4e0595f8f`：`FileSystem` 接口加 `default FileSystem forLocation(Location){return this}`（非切换 FS 即自身，`SpiSwitchingFileSystem` override 返回 per-scheme 委派）。UT `FileSystemDefaultMethodsTest` 9/9。用途：写路径 MPU 按 location 取具体 `ObjFileSystem`（HIVEFS-6）。
- **HIVEFS-2** `3b4f7477d34`：`ConnectorContext` 加 `default FileSystem getFileSystem(ConnectorSession){return null}`+javadoc（**引擎所有/连接器借用/连接器不得 close**；session 对齐 Trino、identity 经 `getUser()` 预留 per-user；默认 null 对齐 `getBackendStorageProperties`）。
- **HIVEFS-3** `a8ed72f2650`：`DefaultConnectorContext.getFileSystem` 懒建+字段缓存一个 per-catalog `SpiSwitchingFileSystem`（空 storage→null），`implements Closeable`；**close 挂点已定**——context 由 catalog **单一持有**（sibling 经 `createSiblingConnector(this)` 共享同一 context），故 catalog 在 `onClose`/换连接器两处关（**连接器只借不关**）。UT 4/4 + 既有 context/catalog 24/24。**对现有行为惰性**（非 hive catalog 不调 getFileSystem→close no-op；须 HIVEFS-4 接线才活）。

**✅ HIVEFS-4 DONE（code `7e06c2aa2a9`）= 连接器·读扫描列文件已转引擎 FileSystem**：`HiveFileListingCache` seam `Configuration`→`org.apache.doris.filesystem.FileSystem`；`listFromFileSystem` 用 `fs.forLocation(loc)`(SYSTEMIC 边界·loud) + `resolved.list(loc)`(LOCAL 边界·skippable，**字面量 `list()` 非 glob 的 `listFiles()`**) 替代裸 `FileSystem.get`+`listStatus`；3 调用点（scan `listAndSplitFiles`、metadata `listFileSizes`/`estimate`）经 `context.getFileSystem(session)` 下发；`HiveScanPlanProvider` +ctx ctor 参；metadata 删 `buildHadoopConf`+`Configuration` import。设计红队 `wf_f25cc498-2de`(GO_WITH_FIXES 7 条全折入：null-FS→loud、`forLocation` catch 拓宽、`isSystemicResolutionFailure` 让惰性"No FileSystem for scheme"仍 loud、literal `list()`、estimate 保护区内下发、4 连带测试文件、去 orphan import)。**fe-connector-hive 289/289、0 checkstyle**。设计 `designs/FIX-HIVEFS-4-design.md`(v2)。**此步 + HIVEFS-3 + 重部署 = `test_string_dict_filter` 应转绿（e2e 待用户自跑）。ACID `planAcidScan:272` 的裸 Hadoop 未动（HIVEFS-5）。**

**✅ HIVEFS-5 DONE（code `7a2d8714951`）= 连接器·ACID 目录下降已转引擎 FileSystem**：`HiveAcidUtil.getAcidState(FileSystem,…)` 换 Doris `FileSystem`——`fs.exists(new Path)`→`exists(Location.of)`；分区列 `fs.listStatus`→`listEntries`(迭代 `fs.list` 收全部含目录)；私有 `listFiles` 助手→`fs.list`+过滤目录（**字面量 `list()` 非 glob 的 `listFiles()`**，镜像 HIVEFS-4）；`FileStatus`→`FileEntry`(`name()`/`location().uri()`/`length()`/`modificationTime()`)；`AcidState.dataFiles`→`List<FileEntry>`；删 hadoop.fs import（留 hive-common `Valid*`）。`HiveScanPlanProvider.planAcidScan`：签名 `Configuration`→`FileSystem`、传 `context.getFileSystem(session)`、删 `Path`+`FileSystem.get`、删孤儿 `buildHadoopConf`+3 import。**红队 `wf_792d1900-cc7`（5 lens+逐条对抗）：迁移码 4 lens SOUND，唯一 REAL(major)=`planAcidScan` 非休眠而是 live-broken**（翻闸后 `hms`∈`SPI_READY_TYPES`→`PluginDrivenScanNode`→`planScan`→`planAcidScan` 无门，hdfs 事务读今天即炸；已实测确认+订正 `:137`/`:248-253` 陈旧"Dormant/never reached"注释）。测试注入=`fe-filesystem-local` 真 `LocalFileSystem`+抛-`listFiles` 子类守卫（非 fake）；`commons-lang` test 依赖实测证实仍需（hive-common `Valid*`）。**fe-connector-hive 9/9、0 checkstyle、BUILD SUCCESS**。设计 `designs/FIX-HIVEFS-5-design.md`。

**✅ HIVEFS-6 DONE（code `d31ceb0364e`）= 连接器·写路径已转引擎 FileSystem（最险，已过红队）**：`HiveConnectorTransaction` `getFileSystem()`（:755）本已全程用 Doris `FileSystem` API（19 非-MPU I/O + 2 MPU），**缺陷只在 FS 来源**——`resolveObjectStoreFileSystem` 本地 `ServiceLoader`（跨插件拿不到 provider）+ `.filter(OBJECT_STORAGE)`（HDFS 后端直接抛）。翻闸后 **live**（class-javadoc "dormant" 陈旧），hive INSERT 今天在 HDFS/对象存储上都必炸。改：`getFileSystem()`→`context.getFileSystem(session)`（引擎 per-catalog `SpiSwitchingFileSystem`，全 scheme）；删 `resolveObjectStoreFileSystem`+`fs` 字段+OBJECT_STORAGE 过滤+孤儿 import；19 非-MPU 点不动（facade 逐操作 `forLocation` 委派）；MPU 两处 `forLocation` narrow 具体 `ObjFileSystem`（`objCommit`=native 写目标 strict、`abortMultiUploads`=逐 upload native `u.path` lenient）；`close()` 删 `fs.close()`（借用不关）；`session` 于 `beginWrite` 捕获（null 安全）。**红队 `wf_8fd372d6-10d`（5 lens+裁决，GO_WITH_FIXES）**：classloader/instanceof（provided-scope+parent-first→共享 `ObjFileSystem`）、session/null、close 去除、19 非-MPU 字节等价 均 SOUND；折入 1 major（MPU abort 用 native-scheme `u.path` 非合成 `s3://`，Azure 才可解析；两处 catch `IOException`→`Exception` 兜 `StoragePropertiesException` RuntimeException）+ 4 minor（test import / non-`ObjFileSystem` facade 强制 `forLocation` + `close()` 抛 AssertionError 钉借用契约 / 类 javadoc）。**fe-connector-hive BUILD SUCCESS、`HiveConnectorTransactionTest` 14/14、0 checkstyle**。设计 `designs/FIX-HIVEFS-6-design.md`。

**⭐ 下一步 = HIVEFS-7（去 jar，达成终点）→ HIVEFS-8（全量 build+e2e）**：① 删 `fe-connector-hive/pom.xml` 的 `hadoop-hdfs-client`（**保留** `hadoop-common`——`Configuration`/`HiveConf`/HMS client 仍需）；全局 grep 校验连接器 main 源 `org.apache.hadoop.fs.FileSystem`/`FileStatus`/`FileSystem.get`/`listStatus` **零残留**（`org.apache.hadoop.fs.Path` 若仅路径拼接可留，确认不再 FS I/O）；打包校验 `output/fe/plugins/connector/hive/lib/` 无 `hadoop-hdfs-client`、插件 zip 完整。② 全量 build（fe-filesystem-api+spi + fe-core + fe-connector-hive）+ 全 UT + 0 checkstyle + import 门净。③ e2e（用户自跑）：重部署后 `test_string_dict_filter`（读 hdfs）+ hive INSERT 写套件（rename/delete/MPU complete）+ INSERT 回滚（MPU abort）+ 若有对象存储环境抽查 s3/oss。

**已探明的去风险事实（勿重查）**：
- **引擎侧已落地（HIVEFS-3）**——`DefaultConnectorContext.getFileSystem` 用已持的 `storagePropertiesSupplier` + `SpiSwitchingFileSystem`（范式镜像 `cleanupEmptyManagedLocation:348`）；close 由 catalog 在 `onClose`/换连接器处关（context 单一持有、sibling 共享）。
- **TCCL 无需连接器侧处理**——`DFSFileSystem.getHadoopFs:131-144` 对 hdfs/viewfs **自钉**到自身插件 loader 再 `FileSystem.get`、finally 还原（注释即描述 “No FileSystem for scheme hdfs” 场景）。连接器去 jar 后任意 TCCL 调用皆安全。
- 写路径 MPU 需**具体** `ObjFileSystem`（`HiveConnectorTransaction` objCommit/abort）→ `FileSystem.forLocation`（HIVEFS-1 加、HIVEFS-6 已用：非切换返回 this、切换返回具体 FS）。**跨 连接器↔fs-plugin `instanceof ObjFileSystem` 安全**（`fe-filesystem-spi` 双 provided-scope + `FileSystemPluginManager:72` parent-first `"org.apache.doris.filesystem."` → 共享同一 `ObjFileSystem` Class；红队 HIVEFS-6 逐点取证）。
- 连接器裸 Hadoop **FileSystem I/O 面已全清**（读扫描 HIVEFS-4 + ACID HIVEFS-5 + 写路径 HIVEFS-6）：三条路径均经 `context.getFileSystem(session)` 借引擎 `SpiSwitchingFileSystem`；仅剩 **pom 的 `hadoop-hdfs-client` jar 依赖**待 HIVEFS-7 删。`FileStatus` 字段全映射 `FileEntry`（`name()`/`location().uri()`/`length()`/`modificationTime()`/`isDirectory()`）。
- fe-filesystem 全 9 插件（hdfs/s3/oss/cos/obs/azure/http/local/broker）已部署 → hive 表任意后端天然支持（免 bundle hadoop-aws/huaweicloud，架构红利）。

**⚠ 待下个 session 核定**（task-list Open §）：无阻塞项待决——HIVEFS-7 是纯 pom 删依赖 + grep 校验，HIVEFS-8 是全量 build + e2e。（写路径 FS/identity 捕获=HIVEFS-6 已定 `beginWrite`；`buildHadoopConf` 去留=HIVEFS-4/5 已删；`HiveAcidUtilTest` 注入=HIVEFS-5 定 `fe-filesystem-local`。）**HIVEFS-7 删 `hadoop-hdfs-client` 后须打包实测**（避免误删 `hadoop-common` 传递依赖破 HMS client/`HiveConf`）。

**⚠ 重要事实（红队 HIVEFS-5 确认）**：翻闸后 hive **事务表读已 live 走 plugin**（`planAcidScan` 非休眠）；HIVEFS-5 前它在 hdfs 上就是坏的（`No FileSystem for scheme hdfs`），HIVEFS-5 修的是 live 生产 bug（e2e 因需 docker HMS+hdfs harness 延后，非"不可测"）。

**⚠ 状态**：HIVEFS-0/1/2/3/4/5/6 已入库（code `0c4e0595f8f`/`3b4f7477d34`/`a8ed72f2650`/`7e06c2aa2a9`/`7a2d8714951`/`d31ceb0364e` + doc commit）。落地纪律：每子步 = 设计→红队→折入→实现→UT→独立 commit（HIVEFS-4/5/6 均已按此走完一轮）。剩 HIVEFS-7（去 jar）/HIVEFS-8（全量 build+e2e）为收尾，无需红队（纯依赖删除 + 验证）。

---

# 🎯 当前状态（2026-07-10）

**⭐ 本轮 = Phase 2 原子翻闸完成：catalog 类型 `hms` 已接入插件 SPI，所有生产流程改走新连接器代码（plain-hive + iceberg-on-HMS + hudi-on-HMS）。未删任何旧代码（Phase 3 才删）。** 4 个独立 commit（均构建+靶向单测验证；净室对抗复核见下）：

- **✅ A** `1af7f063c2d`：事件同步·跟随者游标改接新驱动。`ExternalMetaIdMgr.replayMetaIdMappingsLog` 原无条件喂旧 `MetastoreEventsProcessor`；改双臂——`PluginDrivenExternalCatalog`→新 `MetastoreEventSyncDriver`，否则旧 processor（都按 catalogId keying，不 cast HMS）。翻闸前休眠。**不改则翻闸后从库增量元数据同步静默永久停摆。**
- **✅ B** `eef6fe7b8c2`：事件同步·主/从强制初始化钩子。`MetastoreEventSyncDriver.realRun` 对未初始化目录按 `getType()=="hms"`（读 catalogProperty，不触发 init）强制 `makeSureInitialized()`，每 FE（无 isMaster 门，从库也要）、`!isInitialized()` 一次性、异常吞掉。翻闸前休眠。**不改则从没被查过的翻闸 hms 目录永不同步。**
- **✅ C（原子翻闸本体）** `fb1624be757`：`CatalogFactory` SPI_READY_TYPES += `"hms"` + 删死 `case "hms"`+import；`GsonUtils` 三处 registerSubtype→registerCompatibleSubtype（catalog→PluginDrivenExternalCatalog、db→PluginDrivenExternalDatabase、**table→PluginDrivenMvccExternalTable**，hive 连接器声明 SUPPORTS_MVCC_SNAPSHOT，对齐 paimon/iceberg）+ 删 3 个 orphaned import；`ExternalCatalog.buildDbForInit` HMS 臂→PluginDrivenExternalDatabase（镜像 ICEBERG 臂）；新增 `HmsGsonCompatReplayTest`（3 绿）；**禁用 3 个遗留测试**（`HmsCatalogTest`/`HmsQueryCacheTest`/`HiveDDLAndDMLPlanTest`——它们经路由 create 建 type=hms 目录、fe-core 测试无 hms 插件→抛错，且断言遗留 HMSExternal* 行为；`@Disabled` 指向 Phase 3）。
- **✅ D（D5 视图收尾）** `320702b8166`：翻闸后 hive 视图=PLUGIN_EXTERNAL_TABLE 走共享插件视图臂。删 `enable_query_iceberg_views` 门（视图无条件服务）+ 中立化 “iceberg view not supported…”→“view not supported with snapshot time/version travel” + 两个 @ConfField 标记 `@Deprecated` no-op + 改 iceberg 视图回归 16 处断言。**对已上线 iceberg 是可见行为变更，随翻闸一起发。**

**已签字决策（用户 2026-07-10）**：① 3 个遗留测试=禁用+延后 Phase 3 删除（非改造）；② D5 视图收尾=本轮一起做。

**验证**：`mvn -pl fe-core -am test-compile` BUILD SUCCESS + 0 checkstyle；靶向 `mvn test` 17 跑 0 败 0 错 1 skip（HmsGson 3/3、Iceberg/PaimonGson 各 3/3、ExternalMetaCacheRouteResolver 7/7、HmsCatalog skip）。净室对抗复核（`wf_728cad25-62a`，4 独立审查 + 逐条对抗验证）= **CLEAN**（1 发现 0 确认缺陷；唯一 minor=翻闸 hive 表失去 Nereids SQL 结果缓存资格 @ `BindRelation:729`，经验证 by-design：fail-safe、`enable_hive_sql_cache` 默认关、与 paimon/iceberg-native 一致、被禁用的 `HmsQueryCacheTest` 已明记该遗留缓存 dead-for-hms）。

---

# 🚑 插队任务 DONE（2026-07-11）——TeamCity #991951 hive connector CI classloader split-brain

PR #65474（hive connector SPI 迁移）build **991951** 50 外表 case 失败。根因 = FE 侧 classloader split-brain：`ThriftHmsClient.doAs` 把 metastore-client 创建的 TCCL 钉成 `getSystemClassLoader()`，`SecurityUtil.<clinit>` 的 `new Configuration()` 捕获它 → `DNSDomainNameResolver` 从 fe-core hadoop 副本加载、其父接口 `DomainNameResolver` 从 hive 插件 child-first 副本加载 → `isAssignableFrom` false → `SecurityUtil` 全 JVM 永久毒化 → 49 hive/iceberg-on-HMS/hudi/mtmv/kerberos/tvf 用例全挂（+outlier `test_hms_partitions_tvf` 同因，表象 comms-failure）。集群健康、非 case 问题。**已修**（设计+摘要 `plan-doc/tasks/designs/FIX-CLR-classloader-splitbrain-{design,summary}.md`，跟踪 `plan-doc/tasks/task-list-CLR-991951.md`，对抗验证 `wf_bf3a50e5-046` high-confidence）：
- `92004ef1d0d` **CLR1** `ThriftHmsClient.doAs`→`getClass().getClassLoader()`（plugin loader）+ 隔离 child-first loader 探针单测（RED/GREEN 双验）。fe-connector-hms 40/40。
- `15d3df1dfd6` **CLR2** `HiveConnector.buildPluginAuthenticator` 方法体钉 plugin loader（挡 `HadoopSimpleAuthenticator` eager UGI latent 毒化）。fe-connector-hive 186/186。
- **memory 新增第 4 个 TCCL-pin locus**：plain-hive HMS metastore-client 创建（见 `catalog-spi-plugin-tccl-classloader-gotcha`）。
- **⚠ e2e 欠账（用户自跑）**：真集群重跑 991951 的 49 个 SecurityUtil 用例断言全绿（系统 vs 插件双 loader 只在真 child-first 环境复现，单测已用隔离 loader 复刻精确根因）。`test_hdfs_parquet_group0`（BE `MEM_LIMIT_EXCEEDED`/ASAN flake）与本 PR 无关。
- **残余另开 ticket**：TVF analyze 阶段抛 `java.lang.Error` 未转 SQL 错误、拆连接（outlier1 表象来源；毒化去除后触发点消失）。

---

# 🔧 复核修复系列（#65185 reverify，2026-07-11 起）——跟踪表 `plan-doc/task-list-65185-reverify-fixes.md`

翻闸后第三方复核（`reviews/catalog-spi-review-65185-reverify-2026-07-11.md`）判定的真实/活跃需修条目，按批次做（H1–H4 高 / M1–M8 中 / L1–L20 低 / D-系列设计债）。**处理纪律**：每条 = 设计(`tasks/designs/FIX-<id>-design.md`) → 设计红队 → 实现 → build+靶向 UT → 独立 commit → 勾表 → 更新 HANDOFF。path-whitelist `git add`。

**⭐ 批次 0（H1–H4 hudi 高危）全部 DONE（2026-07-11）**：
- **关键发现+范围决策**：H1（分区名不 unescape 丢行）、H2（datetime 谓词 ISO 化剪到 0 行）**不是 hudi 独有**——`HiveConnectorMetadata` 逐字节相同剪枝块同样静默丢行（对抗 agent 证实）。**用户签字「两份就地各修」**（不抽共享 helper；D-PRUNE 延后）。H3（HMS 名当存储路径）、H4（JNI 列名原样大小写）hudi-only。
- commits：`03f4c12dffa`(H4 lowercase JNI 列名) · `39a279e7c26`(H1 hive+hudi unescape) · `cf540eebc3c`(H2 hive+hudi datetime 渲染) · `9c6fc584eb9`(H3 hudi use_hive_sync 感知源) · `f0ee2ab06d2`(test-hardening) · 各配 doc commit。
- **全量对抗复审(3 skeptic)= CLEAN**：四修正确/复合/无回归、范围完整（其它连接器免疫）、10 新测均可 RED。**登记残余（非本批修，pre-existing legacy parity）**：`use_hive_sync_partition=true`+非-hive-style 表 → hive-sync 臂 0 split；D-PRUNE/相对化时评估（见 `designs/FIX-H3-design.md`）。

**⭐ 复核修复续（backlog；下个 session 首要任务是顶部 FIX-HIVEFS，本系列在其后/穿插）：**
1. **复核修复续批**（跟踪表）：**⭐ 批次 1（M5/M7/M6/M4/M2 连接器局部）全部 DONE**——5 条全绿，各配 RED-able 单测 + 独立 code/doc commit，设计存 `plan-doc/tasks/designs/FIX-M*-design.md`（一轮 recon+对抗红队 `wf_40498e52-19f` 打底，机制全 HEAD 确认、无 UNSOUND）。commits：`84f580c9075`(M5 表级行数恢复 equality gate，**推翻先前签字「不 gate」决定，用户 2026-07-11 已签字**；3 处 P6.6-FIX-H4 已批注 SUPERSEDED) · `f6de950e5bd`(M7 region 别名 4→10) · `03bd4f58187`(M6 s3tables 无存储回退默认凭证链 + data-plane region companion) · `c553c3c7696`+`fca288424fc`(M4 mc `MaxComputePartitionCache`，插件 zip 已验单 caffeine-2.9.3；**最终复核纠正 TTL 86400→600s 对齐旧版**) · `702153885ab`(M2 hive `supportsBatchScan`+`planScanForPartitionBatch`，**登记 BATCH-ACID-SYNC 永久 + BATCH-UNPRUNED-SYNC 由 M3 解**)。**最终对抗复核（`wf_542c60b9-001`，5 per-fix + 1 cross-cut）= M5/M6/M7/M2/cross-cut CLEAN、M4 命中 1 medium(TTL)已修 → 全 CLEAN。**
   **⭐ 批次 2（M3→M1，fe-core 通用节点）全部 DONE。M3 `6963de4124f`**——`shouldUseBatchMode` 的 `!isPruned`→`== NOT_PRUNED`，无谓词大分区表（MaxCompute + 翻闸 hive）恢复异步 batch split，**顺带解 M2 的 BATCH-UNPRUNED-SYNC 残余**（legacy `MaxComputeScanNode:227` 的 `!= NOT_PRUNED` + sibling `displayPartitionCounts` 双证；git `1da88365e85^` 取证 + 全 producer 枚举证闭合）。设计红队 `wf_811e6242-d8b`（3 lens）命中 1 blocker + 1 major 均已解：**反转**被前次评审特意锁定的 pinning 测试（`testUnprocessedPruningNeverBatches`→`testNoPredicatePartitionedTableBatches`）、**登记 supersession**（`decisions-log` D-035 / `deviations-log` DV-019 的 LP-1「`!isPruned` 等价」判定已批注 SUPERSEDED）、**docker-hive golden** `test_hive_partitions:200` `(approximate)inputSplitNum` `60→6`（**用户 2026-07-11 签字：采用 SPI 统一分区数口径**，不给 hive 补 split-count 估算；对齐 MaxCompute/Trino）。`PluginDrivenScanNodeBatchModeTest` 12/12 绿。设计 `designs/FIX-M3-design.md`。**⚠ 本轮踩并行 session 构建污染坑**（`be-java-extensions package -am -T 1C` 污染共享 target 报「cannot access 生成类」假失败，非本码；待其结束干净重跑 12/12）。**M1 DONE `17b432dc1e1`**——翻闸 hive 上 `TABLESAMPLE` 被静默丢弃(全表扫)。设计红队 `wf_32decfa0-349` **推翻原"通用采样"方案(UNSOUND)**:`Split.getLength()` 语义因连接器而异(MaxCompute 默认 byte_size/Paimon JNI range 报 -1、MaxCompute row_offset 报行数)→ 盲目按字节采样出乱结果;**scope 更正为 hive-only 回归**(只有 hive 曾采样)。→ 改**连接器 opt-in**(`ConnectorScanPlanProvider.supportsTableSample()` 默认 false、仅 `HiveScanPlanProvider` true;**用户 2026-07-11 签字 scope=hive-only**,对齐 Trino `applySample` + `supportsBatchScan` 先例)。translator 通用转发 + `PluginDrivenScanNode.sampleSplits`(仅 `applySample` 时,legacy `selectFiles` 端口)+ 非支持连接器 no-op+WARN + 两 gate 门(COUNT/batch 抑制,挂 `applySample`)。`PluginDrivenScanNodeTableSampleTest` 6/6 + BatchMode 12/12 + hive 285/285 绿、0 checkstyle、import 门净。设计 `designs/FIX-M1-design.md`。**⭐ 批次 2(M3+M1)全部 DONE。下一步 = 批次 3(M8 发布工具/文档 + L1 import 门禁)**;之后批次 4(低危连接器 L3–L20)… ⏸ 决策类(L2/L10/L12/L20)先问用户。
2. **e2e 回归（用户自跑，勿丢，非静默）**：**批次 0 全部 e2e 均 live-gated**（含转义值/DATETIME/非-hive-style 带 filter/MOR-JNI 混大小写读），须真集群回归（memory `hms-iceberg-delegation-needs-e2e`）；连同翻闸原有欠账：异构 `type=hms` 目录读/写/DDL/procedure/MTMV/time-travel/@incr；从库事件同步陈旧；Kerberos-HMS 冒烟；升级 GSON replay；耦合缝行；hive 视图。完整矩阵：`hms-cutover-execution-plan-2026-07-10.md` §4/§5 + `hms-spi-cutover-flip-2026-07-10.md` §5。
3. **Phase 3 删除旧代码（最后做）**：~90 类循环单元（`datasource/hive|hudi|iceberg`）+ 死 Nereids 臂 + 删除解锁抽取（HiveUtil/HiveSplit/IcebergUtils）+ 那 3 个 @Disabled 测试。拓扑顺序+清单：execution plan §2.4/§3/§4。

**⚠ 关键纠正（execution plan §3 已过时，本轮已核实纠正，见 `hms-spi-cutover-flip-2026-07-10.md` §2）**：§3.7「rewire 4 个 gate」**错**——两个 instanceof gate（MetastoreEventsProcessor:116、ExternalMetaCacheRouteResolver:66）须**保留**（自动排除翻闸目录、对未翻闸旧目录仍正确；删则破坏旧目录同步/失效）；缓存路自动接管（连接器 CachingHmsClient），事件路靠上面 A/B 两个 ADD-feed（非删 gate）。死 Nereids 臂**翻闸不删**（对齐 iceberg 翻闸留死臂的先例，Phase 3 统删）。

**⚠ 并行 session 风险**：起步先查 `git log`/`git status`/运行中 maven/近 90s mtime 再动手（memory `concurrent-sessions-shared-worktree-hazard`）。

---

# 🧠 起步必读（读文档，别炒 git log 历史）

> **路径**：设计/计划在 `plan-doc/tasks/`，复审报告在 `plan-doc/reviews/`。

1. **本轮翻闸设计（下个 session 起步必读）** = `plan-doc/tasks/hms-spi-cutover-flip-2026-07-10.md`（做了什么 + 对 execution plan 的纠正 + instanceof 全分类 + 验证 + e2e 欠账 + Phase 3 清单）。**行号信 HEAD 不信文档。**
2. **权威翻闸计划（历史，§3 清单本轮已纠正）** = `plan-doc/tasks/hms-cutover-execution-plan-2026-07-10.md`（4 阶段 + DONE 账本 + 硬门；其 §2「Phase 1 未建」已过时，Phase 1 早已 DONE）。
3. **样板**：`plan-doc/tasks/P5-paimon-migration.md`、`P6-iceberg-migration.md`（净室复审 + 能力孪生 + GSON replay 范式；iceberg 翻闸=加 SPI_READY + GSON compat + 留死臂到 Phase 3）。
4. 完成工作明细 = `git log`（commit message 详尽）；勿在 HANDOFF 里重述。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi`）。PR base = `branch-catalog-spi`，**squash 合并**。
- **公开 tracking issue = apache/doris#65185**（进度按已合入 `branch-catalog-spi` PR 口径）。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**：工作树大量历史遗留 scratch（`*.bak` / `regression-conf.groovy` 明文 key / `.audit-scratch/` / `conf.cmy/` / `META-INF/` / `docker/...` / `plan-doc/reviews/P5-*` / `.claude/` / `failed-cases.out`——**非本线程产物，勿混入任何 commit**）。
- commit message：`[feat|fix|doc](catalog) …` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每子步/每条 fix = 独立 commit**；HANDOFF + 设计文档单独 commit（与 code 分开）。上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

# ⚙️ 操作须知（构建/测试，复用）

- maven：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`（**漏 `-am`→假错 `${revision}`**）。连接器：`-pl :fe-connector-<mod> -am`。**靶向单测**加 `-Dtest=<Class> -DfailIfNoTests=false`（`-am` + `-Dtest` 会因上游模块无匹配测试报 “No tests were executed!” 假失败）。
- **⚠️ paimon 模块必须用 `install`（不是 `test`）验证**（shade jar 绑 `package` 阶段）；hms/hive 无此坑。
- **验证信 LOG 不信 exit**：后台 task 通知的 exit code 是 wrapper 的（本轮见过 rc=1 但通知报 exit 0）；重定向到文件跑（不加 `-q`），grep `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Tests run:`/`You have N Checkstyle`（memory `doris-build-verify-gotchas`）。
- **⚠️ bash 默认 timeout 120s**：全量编译 ~6min → 后台跑 + 读 LOG。**⚠️ `/mnt/disk1` 盘紧；勿用 worktree 隔离编译 agent**。cwd 会被重置 → 绝对路径。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**。checkstyle 禁 static import、扫 test 源、`UnusedImports` 会 fail build。

# 🔒 铁律（fe-core 约束）

- fe-core **不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名判别；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。本轮 B 的 `getType()=="hms"` 门是**事件源类型探测（对齐旧 poller 的 instanceof HMSExternalCatalog）**，非源判别式违规。
- fe-core **不解析属性**（storage→fe-filesystem、meta→fe-connector；memory `catalog-spi-no-property-parsing-in-fecore`）。
- 跨插件/跨边界**须 pin TCCL**（memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
- `history_schema_info` 嵌套字段名逐层 lowercase（memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- `PluginDrivenMvccExternalTable`/`PluginDrivenExternalTable` 是 paimon/iceberg/**翻闸后 hms** 实时基类（memory `plugindriven-mvcc-table-is-live-not-dormant`）。

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-plugindriven-no-source-specific-code` · `plugindriven-mvcc-table-is-live-not-dormant` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `concurrent-sessions-shared-worktree-hazard` · `memory-keep-only-general-or-requested`。
