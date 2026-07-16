# Task List — FIX-HIVEFS：hive 连接器改经引擎下发 `FileSystem`（fe-filesystem），去 `hadoop-hdfs-client`（对齐 Trino）

设计 + RCA：`plan-doc/tasks/designs/FIX-HIVEFS-design.md`
触发：本地 hive 回归 `test_string_dict_filter` q01 `No FileSystem for scheme "hdfs"`（fe.log:7657/7698）。
用户签字（2026-07-11）：走正解 B（引擎下发 FileSystem，对齐 Trino）· **一步到位**（读+ACID+写全转）· SPI 照 Trino 形状 `getFileSystem(ConnectorSession)`（identity 预留）· scope = **hive-only**（paimon/iceberg 不动）。

> **性质**：单一逻辑改动，但按"每子步 = 独立 commit + 靶向 UT"推进（`AGENT-PLAYBOOK` 纪律）。编译/依赖序：fe-filesystem-api → fe-connector-spi → fe-core + fe-connector-hive。
> **失败用例何时转绿**：HIVEFS-3（引擎）+ HIVEFS-4（读路径）落地 + 重部署即绿（不必等 HIVEFS-7 删 jar）；HIVEFS-7 是达成"去依赖"终点。

---

## ⚠️ 起步第 0 步（必做）

- [x] **HIVEFS-0 撤销过渡创可贴**（✅ DONE：pom 已回 HEAD，无 commit）：撤销本会话在 `fe/fe-connector/fe-connector-hive/pom.xml` 加的、**未提交**的 `hadoop-hdfs-client` 依赖（Option A 过渡；B 用引擎下发替代）。`git diff fe/fe-connector/fe-connector-hive/pom.xml` 应回到 HEAD。**先查并行 session**（`git log`/`git status`/运行中 maven/近 90s mtime，memory `concurrent-sessions-shared-worktree-hazard`）。

---

## 实现子步（各独立 commit）

- [x] **HIVEFS-1（fe-filesystem-api，基础）** ✅ DONE `0c4e0595f8f`（UT `FileSystemDefaultMethodsTest` 9/9、fe-core test-compile SUCCESS、0 checkstyle）：`org.apache.doris.filesystem.FileSystem` 加 `default FileSystem forLocation(Location loc) throws IOException { return this; }`；`SpiSwitchingFileSystem.forLocation:107`（已 public，返回 `FileSystem`、throws IOException）加 `@Override`。
  - 为何：写路径 MPU 需按 location 取具体 `ObjFileSystem`（HIVEFS-6）。加 default 方法 = 向后兼容（DFSFileSystem/S3 等既有 impl 不破）。
  - UT：`forLocation` default 返回 this；`SpiSwitchingFileSystem.forLocation` 经 fake 返回 per-location 委派。
  - 校验：`mvn -o -pl :fe-filesystem-api -am test-compile`；fe-core 连带编译过。

- [x] **HIVEFS-2（fe-connector-spi，基础）** ✅ DONE `3b4f7477d34`（fe-connector-spi test-compile SUCCESS、0 checkstyle）：`ConnectorContext` 加 `default FileSystem getFileSystem(ConnectorSession session) { return null; }` + javadoc。
  - javadoc 写明：**引擎所有 / 连接器借用 / 连接器不得 close**；`session` 对齐 Trino `create(session)`，identity 经 `session.getUser()` **预留 per-user**，当前 catalog 级（session 暂忽略）。默认 null（对齐 `getBackendStorageProperties()` 良性默认）。
  - 校验：`mvn -o -pl :fe-connector-spi -am test-compile`。

- [x] **HIVEFS-3（fe-core，引擎实现）** ✅ DONE `a8ed72f2650`（fe-core BUILD SUCCESS、UT 4/4 + 既有 context/catalog 24/24、0 checkstyle）：`DefaultConnectorContext.getFileSystem(session)` 懒建 + 字段缓存 `new SpiSwitchingFileSystem(storagePropertiesSupplier.get())`，空 storage→null（对齐 `getBackendStorageProperties`/`cleanupEmptyManagedLocation`）；随 context 拆除 close。
  - 复用现件（已 import `SpiSwitchingFileSystem`/`FileSystemFactory`、已持 `storagePropertiesSupplier`；范式见 `cleanupEmptyManagedLocation:348`）。
  - **close 挂点已定（原「待核」已解）**：context 由**引擎/catalog 单一持有**（sibling 经 `createSiblingConnector(this)` 共享同一 context → 每 catalog 一个缓存 FS），故由 **catalog 关**非连接器关（连接器只借）。`DefaultConnectorContext implements Closeable`（close 幂等、转发缓存 FS、置 null 使 teardown 后 `getFileSystem` 返 null）；`PluginDrivenExternalCatalog` 存 context 引用，在既有两处连接器拆除点关（`onClose` + `initLocalObjectsImpl` 换连接器），**在连接器释放借用引用之后**。非 hive 插件 catalog 从不调 `getFileSystem` → close 为 no-op（本步对现有行为**惰性**，须 HIVEFS-4 接线才活）。
  - UT（fe-core 有 Mockito；实际用 recording-fake seam `buildCatalogFileSystem`）：`getFileSystem` 懒建、二次返回同一实例（缓存）、空 storage→null、close 幂等转发缓存 FS。
  - 校验：`mvn -o -pl fe-core -am test-compile`（已过）。

- [x] **HIVEFS-4（连接器·读扫描列文件）** ✅ DONE（fe-connector-hive test-compile SUCCESS、全量 UT 通过、0 checkstyle；设计红队 `wf_f25cc498-2de` GO_WITH_FIXES 7 条全部折入；设计 `designs/FIX-HIVEFS-4-design.md` v2）：
  - `HiveFileListingCache`：`DirectoryLister` seam `(String, Configuration)` → `(String, FileSystem)`；`listDataFiles` 同改；`listFromFileSystem` 用 `fs.forLocation(loc)`(SYSTEMIC 边界) + `resolved.list(loc)`(LOCAL 边界，**用 `list()` 非 `listFiles()` 走字面量、不 glob-展开**) 替代 `FileSystem.get`+`listStatus`；保留目录/`_`/`.` 过滤（`FileEntry.name()`）+ 零长保留；`FileEntry`→`HiveFileStatus`（location().uri()/length/modificationTime，路径逐字节等价 `Path.toString()`）。
  - **红队折入的加固**：① 空 FS 守卫→loud（D4）；② `forLocation` catch 拓宽 `IOException | RuntimeException`（Minor-1）；③ list catch 内 `isSystemicResolutionFailure`（cause-chain 走 `UnsupportedFileSystemException`/"No FileSystem for scheme"）→ 惰性"scheme 缺实现"仍 loud（Major-2，迁移自身失败类）。
  - `HiveScanPlanProvider`：+`ConnectorContext` ctor 参（第3位）；非-ACID 路径 `context.getFileSystem(session)` 下发；`buildHadoopConf()` 保留（仅 ACID `planAcidScan:272` 用，该处 hadoop `FileSystem` 全限定，属 HIVEFS-5）；`planScanForPartitionBatch` 去掉无用 `hadoopConf`。
  - `HiveConnectorMetadata`：`listFileSizes`(ANALYZE，loud) 钉内下发 FS；`estimateDataSizeByListingFiles` 在 `estimateDataSize` 保护区(catch→-1)**内**（size lambda）下发 FS（Minor-3，统计不炸查询）；删 `buildHadoopConf()` + `Configuration` import（Minor-2）；`sumCachedFileSizes` 参 `Configuration`→`FileSystem`。
  - `HiveConnector.getScanPlanProvider()` 传 `context`。
  - UT：新增共享 `FakeFileSystem`（recording fake，`listFiles` 抛 AssertionError 钉"必须走 list()"）；`HiveFileListingCacheTest` 19 项（含新增 scheme-missing→loud、glob-字面量、null-FS→loud、path/len/mtime 逐字节）；连带修 `HiveScanBatchModeTest`/`HiveConnectorMetadataFileListStatsTest`/`HiveReadTransactionTest`/`HiveConnectorInvalidateTest`（seam/ctor 签名）。
  - **此步 + HIVEFS-3 + 重部署 = 失败用例 `test_string_dict_filter` 转绿**（e2e 待用户自跑）。
  - 已核：`buildHadoopConf()` fe-core-metadata 侧删除（仅两法用）；`HiveScanPlanProvider` 侧保留（ACID）。

- [x] **HIVEFS-5（连接器·ACID）** ✅ DONE（设计 `designs/FIX-HIVEFS-5-design.md`；设计红队 `wf_792d1900-cc7`：5 lens 中 byte-parity/failure-semantics/routing-literal-list/test-fidelity 全判**迁移码 SOUND**，唯一 REAL(major) 是"planAcidScan 非休眠而是 live-broken"——已实测确认并折入注释订正；build+9/9 UT+0 checkstyle）：
  - `HiveAcidUtil`：`getAcidState(FileSystem, …)` 换 Doris `FileSystem`；`fs.exists(new Path)`→`fs.exists(Location.of)`；分区列 `fs.listStatus`→`listEntries`(迭代 `fs.list(Location.of)` 收全部)；私有 `listFiles` 助手→`fs.list`+过滤目录（**字面量 `list()` 非 glob 的 `listFiles()`**，镜像 HIVEFS-4）；`FileStatus`→`FileEntry`（`getPath().getName()`→`name()`、`getPath().toString()`→`location().uri()`、`getLen()`→`length()`、`getModificationTime()`→`modificationTime()`）；`AcidState.dataFiles` 类型换 `List<FileEntry>`；删 hadoop.fs import（保留 hive-common `Valid*`）。
  - `HiveScanPlanProvider.planAcidScan`：签名 `Configuration`→`FileSystem`；调用点传 `context.getFileSystem(session)`；删 `Path`+`FileSystem.get`；数据文件循环 `FileEntry`；删孤儿 `buildHadoopConf()`+`Configuration/FileStatus/Path` import；**订正 `:137`/`:248-253` 陈旧"Dormant/never reached on a live query"注释→ live 事实**（翻闸后 `type=hms` 事务表读经 `PluginDrivenScanNode` 直达）。
  - **测试注入方式已决=`fe-filesystem-local` 真 `LocalFileSystem`**（+ 抛-`listFiles` 子类 `LiteralListingLocalFileSystem` 钉字面量列），非 in-memory fake（保真、改动最小）。`HiveAcidUtilTest` 9 用例全迁、全绿。pom 加 `fe-filesystem-local` test 依赖；`commons-lang` test 依赖**实测证实仍需**（hive-common `Valid*.writeToString` 引用，非 Hadoop），保留+订正注释。
  - 校验：`-pl :fe-connector-hive -am -Dtest=HiveAcidUtilTest -DfailIfNoTests=false test` = BUILD SUCCESS + 9/9 + 0 checkstyle。

- [x] **HIVEFS-6（连接器·写路径，最险）** ✅ DONE（code `d31ceb0364e`；设计 `designs/FIX-HIVEFS-6-design.md`；设计红队 `wf_8fd372d6-10d` GO_WITH_FIXES 全折入；fe-connector-hive BUILD SUCCESS、`HiveConnectorTransactionTest` 14/14、0 checkstyle）：`HiveConnectorTransaction`：
  - **关键发现**：`getFileSystem()`（:755）本已全程用 Doris `FileSystem` API（19 个非-MPU I/O 点 + 2 MPU），缺陷**只在 FS 来源**——`resolveObjectStoreFileSystem`（:784）本地 `ServiceLoader.load(FileSystemProvider)`（跨插件拿不到 provider）+ `.filter(OBJECT_STORAGE)`（HDFS 后端 `objSp==null` 直接抛）。**翻闸后 live**（class-javadoc "dormant" 陈旧），hive INSERT 今天在 HDFS/对象存储上都必炸。
  - `getFileSystem()` 换来源 → `context.getFileSystem(session)`（引擎 per-catalog `SpiSwitchingFileSystem`，全 scheme）；删 `resolveObjectStoreFileSystem` + `fs` 字段 + OBJECT_STORAGE 过滤 + 孤儿 import（ServiceLoader/StorageKind/StorageProperties/FileSystemProvider）。19 非-MPU 点**不动**（facade 逐操作 `forLocation` 委派）。
  - MPU 两处 narrow 具体 `ObjFileSystem`：`objCommit`（complete，strict）`forLocation(Location.of(path))`（native 写目标）；`abortMultiUploads`（abort，lenient）循环内逐 upload `forLocation(Location.of(u.path))`（**红队 major**：native-scheme `u.path`=`pu.getLocation().getWritePath()`，非合成 `s3://`——否则 Azure catalog 不可解析）。
  - **两处 catch 拓宽 `IOException`→`Exception`**（红队 major）：`SpiSwitchingFileSystem.forLocation` props 解析失败抛 `StoragePropertiesException`（RuntimeException）；窄 catch 会逃逸破坏 rollback/泄漏 MPU。
  - `close()`：删 `fs.close()`（借用引擎 FS 不关，仅关自有 executor）。session 于 `beginWrite`（:207）捕获（null 安全：引擎忽略 session）。class-javadoc dormant→live 订正。
  - UT：注入缝迁 `resolveObjectStoreFileSystem`-override → `FakeConnectorContext.getFileSystem`-override + **non-`ObjFileSystem` 路由 facade**（红队 minor：强制走 `forLocation`，漏调即 instanceof RED；facade `close()` 抛 AssertionError 钉借用契约）。import bookkeeping（+`ConnectorSession`/`java.io.IOException`、−`StorageProperties`）。
  - 校验：`-pl :fe-connector-hive -am -Dtest=HiveConnectorTransactionTest -DfailIfNoTests=false test` = BUILD SUCCESS + 14/14 + 0 checkstyle。

- [x] **HIVEFS-7（去 jar，达成终点）** ✅ DONE（**无需改 pom/代码**——已核实当前依赖图本就不含 `hadoop-hdfs-client`；打包实测确认）：
  - **关键纠正**：task-list 原设想"删 `fe-connector-hive/pom.xml` 的 `hadoop-hdfs-client`"是 Option A（bundle hdfs）时代的假设。实际走 Option B（借引擎 FS）+ HIVEFS-0 撤销了未提交的 hdfs-client 创可贴，故**连接器 pom 从无该直接依赖**，`fe-connector-hms→hadoop-common` 也**不传递** hadoop-hdfs-client（`mvn dependency:tree -pl :fe-connector-hive` 全树零 hdfs-client 命中）。→ **pom 无一行可删、无需加 `<exclusion>`**（对不存在的传递依赖加排除是死配置）。
  - **grep 校验**：连接器 main 源 `org.apache.hadoop.fs.FileSystem`/`FileStatus`/`FileSystem.get`/`listStatus` **零残留**（HIVEFS-4/5/6 已清；仅 `org.apache.hadoop.fs.Path` 纯路径拼接留存，非 FS I/O）。
  - **打包实测**（`-pl :fe-connector-hive -am package -DskipTests` BUILD SUCCESS，17:45 新 zip）：`target/doris-fe-connector-hive.zip` 的 `lib/` **无 `hadoop-hdfs-client`**、**保留 `hadoop-common`**（+shaded protobuf/guava/annotations/auth，供 `Configuration`/`HiveConf`/`Path`/HMS client）、root 插件 jar 在、82 个 lib jar，zip 完整。
  - **⚠ 陈旧产物**：`output/fe/plugins/connector/hive/lib/` 与旧 `target` zip（今日 12:44/12:48 打包，创可贴 pom 状态）**仍含 `hadoop-hdfs-client`**——是撤销前的旧构建残留，非当前图。**HIVEFS-8 全量重构建 output/ 后即消失**；e2e 前须重打包重部署（否则测的是带 hdfs-client 的旧插件）。
  - 依赖：HIVEFS-4/5/6 全完成后做（已满足）。

- [ ] **HIVEFS-8（全量构建 + UT + e2e 交接）**：fe-filesystem-api + fe-connector-spi + fe-core + fe-connector-hive 全量 build（后台跑读 LOG，`BUILD SUCCESS`/`Tests run`/checkstyle），全 UT 绿、0 checkstyle、import 门净。

---

## 设计红队（落地前）

- [x] 按 `clean-room-adversarial-review-pref`：实现前对设计做多 agent 对抗红队（重点：写路径 MPU/rename/delete 语义等价、生命周期误 close、`forLocation` 与 SpiSwitchingFileSystem 缓存交互、session-ignore 的 catalog 级正确性）。
  - [x] HIVEFS-5 已做（`wf_792d1900-cc7`）。
  - [x] **HIVEFS-6 写路径红队已做（`wf_8fd372d6-10d`，GO_WITH_FIXES）**：5 lens（classloader-cast / semantic-equivalence / lifecycle-session-concurrency / forlocation-mpu-fidelity / test-fidelity）+ 1 独立裁决者。classloader/instanceof、session/null、close 去除、19 非-MPU 字节等价 均 SOUND；无 blocker。折入 1 major（MPU abort native-path + 两处 catch 拓宽 Exception）+ 4 minor（test import / non-ObjFileSystem facade / close 守卫必做 / 类 javadoc）。

## e2e（用户自跑，勿丢——新能力必配 e2e，memory `hms-iceberg-delegation-needs-e2e`）

- [ ] 重打包 + 重部署后跑：
  - `external_table_p0/hive/test_string_dict_filter`（读 hdfs，本失败用例）全绿；
  - `external_table_p0/hive` 中 37 个含 INSERT 的写套件（验证 rename/delete/MPU 转换）；
  - 若有对象存储环境：抽查 s3/oss 后端 hive 表读（验证 scheme 路由红利、免 bundle hadoop-aws/huaweicloud）；
  - 断言与老实现逐位一致（读结果、写落盘、事务提交/回滚）。

## Open / 待下个 session 核定（勿丢）

1. ~~`DefaultConnectorContext` 缓存 FS 的 close 挂点~~ ✅ **已解（HIVEFS-3）**：catalog 单一持有 + `onClose`/换连接器两处关，`DefaultConnectorContext implements Closeable`（见上 HIVEFS-3）。
2. `HiveScanPlanProvider.buildHadoopConf()`/`Configuration` 在去 FS.get 后的去留（格式/split/传 BE 是否仍需）——HIVEFS-4 定。
3. ~~`HiveAcidUtilTest` 从真 LocalFileSystem 迁到 fake/`fe-filesystem-local` 的注入方式~~ ✅ **已解（HIVEFS-5）**：用 `fe-filesystem-local` 真 `LocalFileSystem`（+ 抛-`listFiles` 子类守卫），非 in-memory fake。`commons-lang` test 依赖实测证实仍需（hive-common `Valid*`）。
4. ~~写路径 commit/abort 时 FS/identity 的捕获时机~~ ✅ **已解（HIVEFS-6）**：`session` 于 `beginWrite` 捕获为字段；FS 解析惰性（引擎 per-catalog 缓存，等价 begin 时建）；null-session（rollback-before-begin/测试）安全（引擎忽略 session）。注入缝迁 `FakeConnectorContext.getFileSystem`-override + non-`ObjFileSystem` facade（非保留死 `resolveObjectStoreFileSystem`）。
5. ~~`forLocation` 加到接口后与现有非切换 impl 的兼容~~ ✅ **已解（HIVEFS-1）**：default `return this`；`SpiSwitchingFileSystem` override 返回 per-scheme 委派；`FileSystemDefaultMethodsTest` 已断言。

## Future（不属本次）

- per-user identity（`session.getUser()`）真正落地 + FS/listing 缓存按 identity keying。
- paimon/iceberg 维持自 bundle `hadoop-hdfs-client`（经各自 `FileIO`，非 Doris `FileSystem`）。
- 其它连接器（maxcompute 等）`getFileSystem` 默认 null，不受影响。
