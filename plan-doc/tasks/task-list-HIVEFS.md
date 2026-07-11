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

- [ ] **HIVEFS-3（fe-core，引擎实现）**：`DefaultConnectorContext.getFileSystem(session)` 懒建 + 字段缓存 `new SpiSwitchingFileSystem(storagePropertiesSupplier.get())`，随 context 拆除 close。
  - 复用现件（已 import `SpiSwitchingFileSystem`/`FileSystemFactory`、已持 `storagePropertiesSupplier`；范式见 `HMSExternalCatalog:146`/`DefaultConnectorContext:348`）。
  - **待核**：`DefaultConnectorContext` 的 close/teardown 挂点（沿 `Connector.close()` 链，`ConnectorContext.java:133` 提到 caller 转发 close）——把缓存 FS 的 close 挂上。
  - UT（fe-core 有 Mockito）：`getFileSystem` 非空、二次调用返回同一实例（缓存）、close 释放。
  - 校验：`mvn -o -pl fe-core -am test-compile`。

- [ ] **HIVEFS-4（连接器·读扫描列文件）**：
  - `HiveFileListingCache`：`DirectoryLister` seam 签名 `(String location, Configuration)` → `(String location, FileSystem)`；`listFromFileSystem` 用 `fs.listFiles(Location.of(location))` 替代 `FileSystem.get`+`listStatus`；保留目录/`_`/`.` 前缀过滤 + 两异常语义（systemic `DorisConnectorException` vs per-partition `HiveDirectoryListingException`）。`FileStatus`→`FileEntry`（getPath→location、getLen→length、isDirectory→isDirectory、getModificationTime→mtime）；`HiveFileStatus` DTO 不变。
  - `HiveScanPlanProvider`：`:272` `FileSystem.get(partPath...)` 换；列文件调用点传 `context.getFileSystem(session)`（不再 `FileSystem.get`）。**待核**：`buildHadoopConf()`/`Configuration` 若仍被格式/split 参数或传 BE 所需则保留（本步只摘"建 FileSystem"一职）。
  - UT（连接器无 Mockito，用 recording fake）：注入 fake `FileSystem`/`DirectoryLister`，断言过滤、两异常、缓存/失效（REFRESH）不变，RED-able。
  - **此步 + HIVEFS-3 + 重部署 = 失败用例转绿**。
  - 校验：`mvn -o -pl :fe-connector-hive -am test-compile -Dtest=HiveFileListingCache* -DfailIfNoTests=false`。

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

1. `DefaultConnectorContext` 缓存 FS 的 close 挂点（Connector.close 链）。
2. `HiveScanPlanProvider.buildHadoopConf()`/`Configuration` 在去 FS.get 后的去留（格式/split/传 BE 是否仍需）。
3. `HiveAcidUtilTest` 从真 LocalFileSystem 迁到 fake/`fe-filesystem-local` 的注入方式。
4. 写路径 commit/abort 时 FS/identity 的捕获时机（begin 时捕获）。
5. `forLocation` 加到接口后与现有非切换 impl 的兼容（default `return this` 覆盖）。

## Future（不属本次）

- per-user identity（`session.getUser()`）真正落地 + FS/listing 缓存按 identity keying。
- paimon/iceberg 维持自 bundle `hadoop-hdfs-client`（经各自 `FileIO`，非 Doris `FileSystem`）。
- 其它连接器（maxcompute 等）`getFileSystem` 默认 null，不受影响。
