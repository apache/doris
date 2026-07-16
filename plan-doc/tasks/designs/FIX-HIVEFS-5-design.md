# FIX-HIVEFS-5 — 连接器 ACID 目录下降改经引擎注入 `FileSystem`（去 `HiveAcidUtil` 裸 Hadoop）

> 承接 FIX-HIVEFS（设计 `FIX-HIVEFS-design.md`、清单 `task-list-HIVEFS.md`）。HIVEFS-3（引擎实现）+ HIVEFS-4（读扫描列文件）已入库；本步是**连接器 ACID 路径**——事务表 base/delta/delete-delta 目录下降仍走裸 `org.apache.hadoop.fs.FileSystem`，是同一"hive 插件无 HDFS 实现→`No FileSystem for scheme hdfs`"缺口的最后一条读路径分支。
> 范围 = 仅 hive 连接器 ACID 读路径（`HiveAcidUtil` + `HiveScanPlanProvider.planAcidScan`）。写路径（HIVEFS-6）、去 jar（HIVEFS-7）不属本步。

## Problem

`HiveAcidUtil`（连接器侧 ACID 目录名解析，fe-core `AcidUtil.getAcidState` 的插件移植）仍用裸 Hadoop：
- 签名 `getAcidState(org.apache.hadoop.fs.FileSystem fs, String partitionPath, …)`；
- `HiveAcidUtil:170` `fileSystem.exists(new Path(fileLocation))`（`isValidMetaDataFile`——base 无 `_v` 后缀时探 `_metadata_acid`）；
- `HiveAcidUtil:255` `fs.listStatus(new Path(dir))`（私有 `listFiles` 助手，列 base/delta 目录内 bucket 文件、过滤子目录）；
- `HiveAcidUtil:298` `fs.listStatus(new Path(partitionPath))`（列分区目录**全部**条目——需同时看到 base_/delta_ 子目录与"原始文件"）；
- 返回 `AcidState.dataFiles : List<org.apache.hadoop.fs.FileStatus>`。

调用方 `HiveScanPlanProvider.planAcidScan:281-285` 用 `org.apache.hadoop.fs.FileSystem.get(partPath.toUri(), buildHadoopConf())` 造 FS 喂进去；`planAcidScan:295-298` 迭代 `state.getDataFiles()`（`FileStatus`）取 `getPath()/getLen()/getModificationTime()` 造 split。

根因同 HIVEFS-4：hive 插件类加载器隔离、`lib/` 无 `DistributedFileSystem` → 裸 `FileSystem.get` 对 hdfs 抛 `No FileSystem for scheme "hdfs"`。老 fe-core 经 `org.apache.doris.filesystem.FileSystem` 列文件，从不裸 Hadoop。

**动态状态（红队 `wf_792d1900-cc7` 纠正——原"休眠"判定为假）**：`planAcidScan` **已 live**、且当前在 hdfs 上就是坏的。Phase 2 原子翻闸（commit `b25b15c357f`）已把 `hms` 加入 `SPI_READY_TYPES`（`CatalogFactory:55`），故 `type=hms` 目录的每张表都是 `PluginDrivenExternalTable`（hive 非 MVCC 走基类），`PhysicalPlanTranslator.visitPhysicalFileScan:812` **先**匹配它 → `PluginDrivenScanNode` → `planScan` → 事务表 `isTransactional()` **无门**直入 `planAcidScan`（唯一读侧守卫是 full-ACID 的 ORC 格式检查 `:262`，无"事务读不支持"早拒）。故一张 hdfs 事务表**今天 live 查询即命中** `planAcidScan:284-285` 的裸 `FileSystem.get` → `No FileSystem for scheme "hdfs"`——正是本步要修的 bug。**含义**：① `HiveScanPlanProvider:137`（`(Dormant — see planAcidScan.)`）与 `:248-253` 的 javadoc（`hive is not yet in SPI_READY_TYPES, so this path is never reached on a live query`）是翻闸前的**陈旧假注释**，本步须一并订正（否则 shipping 自相矛盾的文件）；② 本步是修**live 生产 bug**（非休眠路径整洁），ACID 读字节等价须真回归（e2e 见下，延后理由是 harness 可用性、非休眠）。

## Root Cause

连接器 ACID 分支在 catalog-SPI 迁移中把老 fe-core 的 `org.apache.doris.filesystem.FileSystem` 列文件走样成裸 `org.apache.hadoop.fs.FileSystem`，与 HIVEFS-4 读扫描路径同源。引擎已经 HIVEFS-3 提供 `ConnectorContext.getFileSystem(session)`（per-catalog `SpiSwitchingFileSystem`，引擎所有、连接器借用不 close），HIVEFS-4 已把读扫描列文件接上；ACID 分支是最后一条未接的读路径。

## Design

镜像 HIVEFS-4：把 `HiveAcidUtil` 与 `planAcidScan` 全部裸 Hadoop I/O 换成引擎注入的 Doris `FileSystem`（`SpiSwitchingFileSystem`）。

### 1. `HiveAcidUtil`（连接器）
- 签名 `getAcidState(org.apache.doris.filesystem.FileSystem fs, String partitionPath, Map, boolean)` —— 类型换 Doris `FileSystem`，其余不变。
- `exists`：`fs.exists(Location.of(fileLocation))`（`isValidMetaDataFile`）。捕获 `IOException`→`false` 保留（原语义）。
- **分区目录列（:298）**：迭代 `fs.list(Location.of(partitionPath))` 收集**全部** `FileEntry`（含目录），非 `listFiles()`。
- **私有 `listFiles` 助手（:255）**：迭代 `fs.list(Location.of(dir))`，过滤 `!e.isDirectory()` → 文件 `FileEntry`（**字面量 `list()`，非 glob 的 `listFiles()`**——见下"字面量列"）。
- `FileStatus` 字段映射：`entry.isDirectory()`→`FileEntry.isDirectory()`；`entry.getPath().getName()`→`FileEntry.name()`；`AcidState.dataFiles`/`getDataFiles()` 类型 `List<FileStatus>`→`List<FileEntry>`。
- 删 import `org.apache.hadoop.fs.{FileStatus,FileSystem,Path}`；加 `org.apache.doris.filesystem.{FileEntry,FileIterator,FileSystem,Location}`。**保留** `org.apache.hadoop.hive.common.Valid*`（ACID 快照算法，非 FS I/O，不属去 Hadoop 目标）。
- 更新 class-javadoc（"raw Hadoop FileSystem"）+ `getAcidState` 的 `@param fs` javadoc。

### 2. `HiveScanPlanProvider.planAcidScan`（连接器）
- 调用点（:138）：`buildHadoopConf()` → `context.getFileSystem(session)`（与非-ACID 分支 :142 同源，借引擎 per-catalog FS）。
- 签名 `Configuration hadoopConf` → `org.apache.doris.filesystem.FileSystem fs`。
- 删 `Path partPath = new Path(...)` + `org.apache.hadoop.fs.FileSystem.get(...)`（:281-285），直接把注入的 `fs` 喂 `getAcidState`。
- 数据文件循环（:295-298）：`FileStatus dataFile`→`FileEntry dataFile`；`.getPath().toString()`→`.location().uri()`；`.getLen()`→`.length()`；`.getModificationTime()`→`.modificationTime()`。
- 删已孤儿的 `buildHadoopConf()`（:539，唯一调用者是 ACID）。**保留** `catalogProperties`/`isLocationProperty`（`getScanNodeProperties:365/367` 仍用）。
- 删孤儿 import `org.apache.hadoop.conf.Configuration`（:33）、`org.apache.hadoop.fs.FileStatus`（:34）、`org.apache.hadoop.fs.Path`（:35）；加 `org.apache.doris.filesystem.FileEntry`（`FileSystem` :31 已 import）。
- 更新 :282-283 陈旧注释（"ACID path still uses bare Hadoop"）。
- **订正陈旧"休眠"注释（红队 folded）**：`:137` 的 `(Dormant — see planAcidScan.)` 与 `:248-253` javadoc 的"hive is not yet in SPI_READY_TYPES / never reached on a live query"——翻闸后已假（见上"动态状态"）。改为如实：此路径 live（`type=hms` 事务表读经 `PluginDrivenScanNode` 直达）；保留仍然正确的事务生命周期说明（开元数据读锁 → 查询结束经 `releaseReadTransaction`/`deregister` 释放）。

### 3. 失败语义（与 HIVEFS-4 的关键区别）
ACID 路径**任何列文件失败即 loud**——现有 `planAcidScan` 的 `catch(IOException)` 把整批包成 `DorisConnectorException("Failed to list ACID files for partition: …")`、不跳分区。注入 `SpiSwitchingFileSystem` 后，scheme 解析失败（`No StorageProperties`/`No FileSystem for scheme`/factory 异常）经内部 `forLocation` 以 `IOException` 从 `fs.list`/`fs.exists` 冒出 → 同一 catch → loud。**与老 `FileSystem.get`（scheme 失败也抛 IOException→loud）逐位一致**。故本步**不需要** HIVEFS-4 读扫描的 systemic-vs-local 分级 / `isSystemicResolutionFailure` 重分类（那是为"跳单坏分区"服务，ACID 从不跳）。

### 4. 为何直接传 switching FS（不先 `forLocation` 解具体 FS）
`getAcidState` 内所有路径（分区目录、base_/delta_ 子目录、`_metadata_acid` 文件）同属一个分区 location → 同 scheme/authority/`StorageProperties`。`SpiSwitchingFileSystem.list/exists` 内部各自 `forLocation`（按 `StorageProperties` 缓存、首次后命中）→ 路由到具体 FS 的**字面量 `list()`**。传 facade 让 util 签名最小（仅 Doris `FileSystem`，无 forLocation 步），且正确。（备选：在 planAcidScan 里 `forLocation` 解一次传具体 FS——亦可，但为无收益的额外步；ACID 不需要 forLocation 边界带来的失败分级。）

### 5. 字面量列（list() 非 listFiles()）
per-scheme FS（`DFSFileSystem`/`S3CompatibleFileSystem`）override `listFiles` 为 **glob 感知**分支，会把含 `[`/`*`/`?` 的 location 当模式；老 `listStatus` 从不 glob 展开，而 hive location 可合法含这些字符（分区值）。故 `HiveAcidUtil` 一律 `fs.list(Location.of(x))`（`SpiSwitchingFileSystem.list`→具体 FS 字面量 `list`），过滤目录用 `FileEntry.isDirectory()`。**镜像 HIVEFS-4 `listFromFileSystem` 的既定规则**。

## Implementation Plan
1. `HiveAcidUtil.java`：签名/内部 I/O/DTO 类型/import/javadoc 全换（§1）。
2. `HiveScanPlanProvider.java`：调用点/签名/删 `FileSystem.get`/循环/删 `buildHadoopConf`/import/注释（§2）。
3. `HiveAcidUtilTest.java`：真 Hadoop LocalFileSystem → Doris `LocalFileSystem`（见 Test Plan）。
4. `fe-connector-hive/pom.xml`：加 `fe-filesystem-local` **test scope** 依赖。
5. build + 靶向 UT + 0 checkstyle + import 门净。

## Risk Analysis
- **`FileStatus`→`FileEntry` 逐位等价**：`getPath().toString()`↔`location().uri()`（URI 字符串）、`getLen()`↔`length()`、`getModificationTime()`↔`modificationTime()`、`isDirectory()`↔`isDirectory()`、`getPath().getName()`↔`name()`（`FileEntry.name()` 取最后一段、剥尾 `/`）。分区名/文件名解析逻辑（parseBase/parseDelta/filter）只吃 `name()` 字符串，不变。
- **`list()` 非递归、含目录**：`fs.list` = 单层列（LocalFileSystem `Files.newDirectoryStream`、DFSFileSystem `listStatus`），与老 `listStatus` 语义一致（immediate children，dirs+files）。分区列不过滤（需看子目录）、私有 listFiles 过滤目录。
- **live 路径（非休眠）**：`planAcidScan` 已 live（翻闸后 `type=hms` 事务表读经 `PluginDrivenScanNode` 直达，见"动态状态"）；本步修 live 生产 bug。字节等价 e2e 因 harness 可用性延后（非休眠），一旦 docker HMS+hdfs 环境可用即须回归。
- **误 close**：`getFileSystem(session)` 返回引擎所有的借用 FS；`planAcidScan`/`getAcidState` 只 `list`/`exists`、**不 close**（契约：连接器只借）。无生命周期变更。
- **`buildHadoopConf` 删除**：全局仅 ACID 一处调用；`catalogProperties`/`isLocationProperty` 另有 `getScanNodeProperties` 用户，保留。`Configuration`/`Path`/`FileStatus` import 去 FS.get 后无残留用户，删。
- **铁律核对**：`HiveAcidUtil` 不解析属性（用注入 FS）；不新增 fe-core 分支；TCCL 由 `DFSFileSystem.getHadoopFs` 自钉（连接器去 jar 后任意 TCCL 安全）；无源判别式。

## Test Plan

### Unit（连接器模块无 Mockito）
**迁移 `HiveAcidUtilTest`（9 个既有）到 Doris `LocalFileSystem`（`fe-filesystem-local`，真 FS 真临时树）**——最小改动、最高保真（真目录遍历，杜绝"fake 自带 bug 掩盖真 bug"）：
- `@TempDir` + `Files.createDirectories/write` 建 base/delta/delete-delta 树**不变**（已证正确的 fixture）。
- `localFs()`：`new org.apache.doris.filesystem.local.LocalFileSystem(Collections.emptyMap())`（Doris），非 Hadoop `FileSystem.getLocal`。`tempDir.toString()` 是裸 `/tmp/…` 绝对路径 → `Location.of` 被 `LocalFileSystem.toPath` 接受（其显式支持裸绝对路径）。
- import：删 hadoop `Configuration/FileStatus/FileSystem`；加 Doris `FileEntry/FileSystem/LocalFileSystem`；**保留** hive-common `Valid*`（快照仍需）。
- 断言：`FileStatus`→`FileEntry`；`f.getPath().toString()`→`f.location().uri()`（`file:///tmp/…/base_x/bucket` 仍 `contains("/base_x/…")`，逐位断言不破）。
- 9 个用例语义全保留（best-base/delta 选择、insert-only 拒 delete-delta、缺 Valid* 列表、原始文件无 base、可见性 txn 跳过、无效 base 回退、not-enough-history）——RED-able 不变（选错 base/delta 即挂）。

**字面量列守卫（list() 非 listFiles()）**：`localFs()` 用 `LocalFileSystem` 子类，override `listFiles(Location)`/`listFilesRecursive(Location)` 抛 `AssertionError`（其余委派 super 的真 `list()`/`exists()`）。**如此每个用例都顺带钉住"必须走字面量 `list()`"**——若 `HiveAcidUtil` 退回 `fs.listFiles()`，9 个测试全 RED（AssertionError）。~10 行子类，复用真 FS 保真 + 统一守卫，无需另建 tree fake（LocalFileSystem 默认 `listFiles` 亦字面量，单靠它区分不出 list/listFiles，故须此守卫；镜像 HIVEFS-4 `FakeFileSystem.listFiles` 抛错的同一目的）。

### E2E（用户自跑，勿丢——`hms-iceberg-delegation-needs-e2e`）
`external_table_p0/hive` 事务表（full-ACID ORC + insert-only）读，断言与老 fe-core `AcidUtil` 逐位一致（幸存 base/delta、delete-delta 减除）。**此路径已 live（翻闸后事务读经 plugin，见"动态状态"）且当前 hdfs 上就是坏的——本 e2e 验的是 live 生产 bug 的修复**，延后仅因需 docker HMS+hdfs harness，一旦可用即须回归（非"休眠不可测"）。

## Open / 已决
- **测试注入方式**（HIVEFS 清单 Open #3）**已决**：用 `fe-filesystem-local` 真 `LocalFileSystem`（+ 抛-listFiles 子类守卫），非另建 in-memory tree fake。理由：ACID 选择算法安全攸关（选错→静默数据错），真 FS 遍历保真度最高、改动最小、无 fixture-bug 风险；`LocalFileSystem` 本就"for unit testing only"，是老 Hadoop LocalFileSystem 测试的直接 Doris 对等。
- **`commons-lang` test 依赖去留**（红队 test-fidelity lens minor：迁移可能孤儿化它）**已实测定夺=保留（改注释）**：删掉后 `HiveAcidUtilTest` 报 `NoClassDefFoundError: org.apache.commons.lang.StringUtils`——`snapshot()` 建快照用的 hive-common `Valid*`（`ValidReaderWriteIdList.writeToString()`）引用 commons-lang 2.x，与 Hadoop LocalFileSystem 无关（旧注释归因 Hadoop 是错的）。故保留该 test 依赖、订正注释指向真正的消费者。
- 写路径 commit/abort 时 FS/identity 捕获时机 → HIVEFS-6。
