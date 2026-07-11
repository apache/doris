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

- [ ] **HIVEFS-5（连接器·ACID）**：`HiveAcidUtil:170/255/298` `fs.exists`/`fs.listStatus` → 注入 `FileSystem` 的 `exists(Location)`/`list`/`listFiles`。
  - **⚠ 测试基建待解**：`HiveAcidUtilTest` 现对**真 Hadoop LocalFileSystem** 跑（见 `fe-connector-hive/pom.xml:128-137` commons-lang test 注释）。转 Doris `FileSystem` 后须换成 fake Doris `FileSystem` 或 `fe-filesystem-local` 的 local 实现——下个 session 定夺注入方式。
  - 校验：`-Dtest=HiveAcidUtil* -DfailIfNoTests=false`。

- [ ] **HIVEFS-6（连接器·写路径，最险）**：`HiveConnectorTransaction`：
  - `resolveObjectStoreFileSystem`（坏 ServiceLoader stub，:784-793）→ `context.getFileSystem(session).forLocation(loc)`；MPU `instanceof ObjFileSystem`（:809/1336）落到 `forLocation` 返回的具体 FS；`objFs.completeMultipartUpload`/`getObjStorage().abortMultipartUpload` 不变。
  - 事务内 delete/rename/exists 等裸 Hadoop 调用一并换注入 FS。
  - `close()`：**不再 close 借用的引擎 FS**（仅关自有资源）。
  - **待核**：commit/abort 时机 session 是否还在 → 在 `beginWrite`（:207，已捕获 user）时捕获所需 FS/identity 供后续复用。
  - UT：`resolveObjectStoreFileSystem` 为 `protected` 可 override 注入 fake ObjFileSystem；断言 MPU complete/abort 路径 + close 不关借用 FS。
  - 校验：`-Dtest=HiveConnectorTransaction*,HiveWrite* -DfailIfNoTests=false`。

- [ ] **HIVEFS-7（pom + 去 jar，达成终点）**：删 `fe-connector-hive/pom.xml` 的 `hadoop-hdfs-client`；**保留** `hadoop-common`（`Configuration`/`HiveConf`/HMS client 仍需）。
  - 全局 grep 校验连接器 main 源 `org.apache.hadoop.fs.FileSystem`/`FileStatus`/`FileSystem.get` **零残留**（`new Path` 若仅路径拼接可留，但确认不再 FS I/O）。
  - 打包校验：`output/fe/plugins/connector/hive/lib/` 无 `hadoop-hdfs-client`；插件 zip 仍完整。
  - 依赖：HIVEFS-4/5/6 全完成后做。

- [ ] **HIVEFS-8（全量构建 + UT + e2e 交接）**：fe-filesystem-api + fe-connector-spi + fe-core + fe-connector-hive 全量 build（后台跑读 LOG，`BUILD SUCCESS`/`Tests run`/checkstyle），全 UT 绿、0 checkstyle、import 门净。

---

## 设计红队（落地前）

- [ ] 按 `clean-room-adversarial-review-pref`：实现前对设计做多 agent 对抗红队（重点：写路径 MPU/rename/delete 语义等价、生命周期误 close、`forLocation` 与 SpiSwitchingFileSystem 缓存交互、session-ignore 的 catalog 级正确性）。

## e2e（用户自跑，勿丢——新能力必配 e2e，memory `hms-iceberg-delegation-needs-e2e`）

- [ ] 重打包 + 重部署后跑：
  - `external_table_p0/hive/test_string_dict_filter`（读 hdfs，本失败用例）全绿；
  - `external_table_p0/hive` 中 37 个含 INSERT 的写套件（验证 rename/delete/MPU 转换）；
  - 若有对象存储环境：抽查 s3/oss 后端 hive 表读（验证 scheme 路由红利、免 bundle hadoop-aws/huaweicloud）；
  - 断言与老实现逐位一致（读结果、写落盘、事务提交/回滚）。

## Open / 待下个 session 核定（勿丢）

1. ~~`DefaultConnectorContext` 缓存 FS 的 close 挂点~~ ✅ **已解（HIVEFS-3）**：catalog 单一持有 + `onClose`/换连接器两处关，`DefaultConnectorContext implements Closeable`（见上 HIVEFS-3）。
2. `HiveScanPlanProvider.buildHadoopConf()`/`Configuration` 在去 FS.get 后的去留（格式/split/传 BE 是否仍需）——HIVEFS-4 定。
3. `HiveAcidUtilTest` 从真 LocalFileSystem 迁到 fake/`fe-filesystem-local` 的注入方式——HIVEFS-5 定。
4. 写路径 commit/abort 时 FS/identity 的捕获时机（begin 时捕获）——HIVEFS-6 定。
5. ~~`forLocation` 加到接口后与现有非切换 impl 的兼容~~ ✅ **已解（HIVEFS-1）**：default `return this`；`SpiSwitchingFileSystem` override 返回 per-scheme 委派；`FileSystemDefaultMethodsTest` 已断言。

## Future（不属本次）

- per-user identity（`session.getUser()`）真正落地 + FS/listing 缓存按 identity keying。
- paimon/iceberg 维持自 bundle `hadoop-hdfs-client`（经各自 `FileIO`，非 Doris `FileSystem`）。
- 其它连接器（maxcompute 等）`getFileSystem` 默认 null，不受影响。
