# FIX-HIVEFS — hive 连接器改经引擎下发 `FileSystem`（fe-filesystem）替代裸 Hadoop，去 `hadoop-hdfs-client`（对齐 Trino）

> 触发（2026-07-11）：本地 hive 回归 `external_table_p0/hive/test_string_dict_filter` q01 失败 `Failed to resolve filesystem for hdfs://...`。经取回 fe.log/fe.warn.log + fe-filesystem 拓扑排查 + 类加载器/TCCL 验证 + **Trino 方案对标**，定为架构性缺口而非环境/文案问题。用户拍板走"正解 B、一步到位、SPI 照 Trino 形状留 identity 参"。
> 范围 = **仅 hive 连接器**（读扫描 + ACID + 写路径全部）。paimon/iceberg 不动。

## Problem

- 失败栈（`fe.log:7657/7677/7698`、`fe.warn.log`）：
  ```
  RuntimeException: Failed to resolve filesystem for hdfs://hadoop-master-doris-shared:8320/user/doris/preinstalled_data/parquet_table/test_string_dict_filter_parquet
   → DorisConnectorException (HiveFileListingCache.listFromFileSystem:168)
   → org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "hdfs"
       at FileSystem.getFileSystemClass(:3581) ← HiveFileListingCache.listFromFileSystem:162 (FileSystem.get)
  ```
- **非环境问题**：`hadoop-master-doris-shared` 在 `/etc/hosts` 可解析（`172.20.32.136`）；metastore（thrift）连通、已取到表 location；走的是新 `fe-connector-hive`（`HiveFileListingCache:168`）。
- **直接原因（打包）**：`output/fe/plugins/connector/hive/lib/` 无任何 jar 含 `org.apache.hadoop.hdfs.DistributedFileSystem`；两处 `META-INF/services/org.apache.hadoop.fs.FileSystem`（`hadoop-common`、`hive-catalog-shade`）只注册 Local/View/Har/Http/NullScan/ProxyLocal，**无 hdfs**。paimon/iceberg 插件都自带 `hadoop-hdfs-client-3.4.2.jar`，唯 hive 缺。
- **深层（架构）**：连接器扫描/写全程用裸 `org.apache.hadoop.fs.FileSystem`（`HiveFileListingCache:162/171`、`HiveScanPlanProvider:272`、`HiveAcidUtil:170/255/298`、`HiveConnectorTransaction` 写/删/MPU）。插件类加载器隔离 → 缺 hdfs 实现即 `No FileSystem for scheme "hdfs"`。而**老 fe-core 经 `org.apache.doris.filesystem.FileSystem` + `DirectoryLister` 列文件**（`HiveExternalMetaCache.loadFiles():392-396` → `FileSystemCache.getFileSystem` → `FileSystemDirectoryLister`），从不裸 `FileSystem.get`。**新代码裸 Hadoop 是迁移走样**。

## 备选与否决

- **A（bundle `hadoop-hdfs-client`）**：能修 hdfs，对齐 paimon/iceberg 形态。但复制 `fe-filesystem-hdfs` 插件**已拥有**的 hdfs 依赖；且对象存储 hive 表会再撞 `No FileSystem for scheme s3a/obs`，要继续 bundle `hadoop-aws`/`hadoop-huaweicloud`（重且版本敏感），连接器越抱越多。**否**（仅作过渡）。
- **连接器自建 FileSystem（自跑 `ServiceLoader<FileSystemProvider>`）**：跨插件不可行——FS provider 在**兄弟**插件 loader（`output/fe/plugins/filesystem/*`），共享类路径（`output/fe/lib`）**无任何 provider**，且 `DirectoryPluginRuntimeManager.ServiceResourceFilteringParentClassLoader:443` 刻意屏蔽父级 service 描述符。写路径 `HiveConnectorTransaction.resolveObjectStoreFileSystem:788` 正是此坏 stub（注释自认 cutover-time concern）。**否**。

## Trino 对标（本方案的镜像来源）

Trino 已走完同一条路（[Decouple Trino from Hadoop #15921](https://github.com/trinodb/trino/issues/15921)、[Out with the old file system, 2025](https://trino.io/blog/2025/02/10/old-file-system.html)）：

| Trino | 本方案（Doris） |
|---|---|
| 引擎注入 `TrinoFileSystemFactory`；`HiveMetadata.fileSystemFactory` | `ConnectorContext.getFileSystem(session)`（引擎提供，连接器已持 `context`） |
| `factory.create(ConnectorSession)` → `TrinoFileSystem` | `context.getFileSystem(ConnectorSession)` → `org.apache.doris.filesystem.FileSystem` |
| `TrinoFileSystem`：`listFiles/listDirectories/newInputFile/deleteFile/renameFile`（全 `Location`） | `FileSystem`：`list/listFiles/listDirectories/exists/newInputFile/delete/rename`（全 `Location`，近一一对应） |
| scheme 路由 + native 实现在引擎侧；Hadoop 可选仅 HDFS | `SpiSwitchingFileSystem` + `fe-filesystem-{hdfs,s3,oss,cos,obs,azure,http,local,broker}` 插件 |

因此本方案让连接器对 hdfs/s3/oss/cos/obs/azure **全部免 bundle**（Q1：其它 fs 类型天然支持，为架构红利）。

## Design

### 1. SPI 契约
- **`ConnectorContext.getFileSystem(ConnectorSession session)`** → `org.apache.doris.filesystem.FileSystem`，`default` 返回 `null`（对齐 `getBackendStorageProperties()` 良性默认；maxcompute 等不用存储的连接器不受影响）。契约：**引擎所有、连接器借用、连接器不得 `close()`**。`session` 形状对齐 Trino `create(session)`，identity 经 `session.getUser()` **预留 per-user**；当前实现按 catalog 级建（session 暂忽略）。
- **`org.apache.doris.filesystem.FileSystem` 接口加 `default FileSystem forLocation(Location loc) throws IOException { return this; }`**；`SpiSwitchingFileSystem.forLocation:107`（已 public）改 `@Override`。用途：写路径 MPU 需按 location 取**具体** `ObjFileSystem`（`instanceof ObjFileSystem`，`HiveConnectorTransaction:809/1336`）——非切换 FS 返回自己，切换 FS 返回 per-scheme 委派。capability() 无 location 参、不能按位置路由，故用 forLocation。

### 2. 引擎实现（fe-core）
- `DefaultConnectorContext.getFileSystem(session)`：**懒建 + 按 context 缓存**一个 `SpiSwitchingFileSystem(storagePropertiesSupplier.get())`（字段），随 context/catalog 拆除时 `close`（沿 `Connector.close()` 链）。近零新件——`DefaultConnectorContext` 已 import `SpiSwitchingFileSystem`/`FileSystemFactory`、已持 `storagePropertiesSupplier`，且 `HMSExternalCatalog:146`/`IcebergMetadataOps:472`/`DefaultConnectorContext:348` 已这样用。缓存 = 对齐老 `FileSystemCache` 的 per-catalog 复用（避免每次重建/重认证）。
- TCCL：**无需引擎/连接器侧处理**——`DFSFileSystem.getHadoopFs:131-144` 对 hdfs/viewfs 自钉到**自身**插件 loader（`DFSFileSystem.class.getClassLoader()`）再调 `FileSystem.get`、finally 还原（注释即描述"No FileSystem for scheme hdfs"场景）。故连接器（去 jar 后）任意 TCCL 调用皆安全。

### 3. 连接器改造（fe-connector-hive，一次性转全部裸 Hadoop I/O）

| 现状（裸 Hadoop） | 改为（注入 `FileSystem`） | 位点 |
|---|---|---|
| `FileSystem.get(uri,conf)` + `fs.listStatus` | `injectedFs.listFiles(Location.of(loc))` | `HiveFileListingCache:162/171`、`HiveScanPlanProvider:272` |
| `FileStatus.getPath/getLen/isDirectory/getModificationTime` | `FileEntry.location/length/isDirectory/mtime` | 列文件后处理（`HiveFileStatus` DTO 不变，仅换来源） |
| `fs.exists`、`fs.listStatus` | `injectedFs.exists/list` | `HiveAcidUtil:170/255/298` |
| `resolveObjectStoreFileSystem`（坏 ServiceLoader stub） | `context.getFileSystem(session).forLocation(loc)` | `HiveConnectorTransaction:784` |

- `HiveFileListingCache` 的 `DirectoryLister` seam 签名 `(location, Configuration)` → `(location, FileSystem)`；两种异常语义保留（systemic `DorisConnectorException` vs per-partition `HiveDirectoryListingException`）。
- 写路径 `close()` **不再 close** 借用的引擎 FS（仅关自有资源）。
- **pom**：删 `hadoop-hdfs-client`（撤销本会话工作区加的、**未提交**的那条）；`hadoop-common` **保留**（`Configuration`/`HiveConf`/HMS client 仍需）。改完全局 grep 确认 `org.apache.hadoop.fs.FileSystem` 在连接器 main 源零残留。

## Implementation Plan（单次改动，按可独立验证子步推进）

1. **fe-filesystem-api**：`FileSystem` 加 `forLocation` default；`SpiSwitchingFileSystem` 加 `@Override`（方法体已存在）。
2. **fe-connector-spi**：`ConnectorContext.getFileSystem(ConnectorSession)` default null + javadoc（所有权/借用/identity 预留）。
3. **fe-core**：`DefaultConnectorContext.getFileSystem` 实现 + 字段缓存 + close 释放。
4. **连接器·读**：`HiveFileListingCache` DirectoryLister 换 `FileSystem`；`HiveScanPlanProvider:272` 换；`FileStatus`→`FileEntry` 映射；列文件调用点改用 `context.getFileSystem(session)`（不再 `FileSystem.get`）。**注**：`buildHadoopConf()`/`Configuration` 若仍被格式/split 参数或传 BE 所需则保留（本步只摘除其"建 FileSystem"一职），实际去留由 writing-plans 逐点核定。
5. **连接器·ACID**：`HiveAcidUtil` `exists`/`listStatus` 换。
6. **连接器·写**：`HiveConnectorTransaction` `resolveObjectStoreFileSystem`→`getFileSystem(session).forLocation(loc)`；MPU `instanceof ObjFileSystem` 落到具体 FS；`close` 不关借用 FS；begin 时捕获所需 FS/identity。
7. **pom**：删 `hadoop-hdfs-client`；全局 grep 校验零残留裸 `org.apache.hadoop.fs.FileSystem`。
8. **构建 + 单测 + e2e**。

## Risk Analysis

- **写路径最险**（事务/MPU/rename/delete 语义）：`forLocation` 精确取老代码期望的具体 FS；`ObjFileSystem` 接口不变；靠写 e2e 兜底。
- **生命周期误 close**：连接器若 close 借用的引擎 FS → 引擎 FS 提前失效。对策：契约明确（引擎所有）+ `close()` 只关自有 + 审查。
- **`DirectoryLister` 签名变** → `HiveFileListingCache` 单测适配（seam 本为可注入设计）。
- **性能**：引擎按 catalog 缓存 `SpiSwitchingFileSystem`（其内再按 `StorageProperties` 身份缓存 per-scheme FS）；对齐老 `FileSystemCache`，非每次重建。
- **session 暂忽略**：`getFileSystem(session)` 当前不按 user 建 FS（catalog 级）；javadoc 标注；per-user 落地时缓存须按 identity keying（届时 listing 缓存策略需复审，避免跨用户串读）。
- **铁律核对**：`getFileSystem` 为**通用** SPI（非 source-specific，不在 fe-core 加 hive 分支）；连接器不解析属性（用 `session`/`context`）；对齐 Trino；TCCL 由 `DFSFileSystem` 自钉（memory `catalog-spi-plugin-tccl-classloader-gotcha` 既有 locus，本方案不新增连接器钉点）。

## Test Plan

### Unit（连接器模块无 Mockito；用可注入 fake FileSystem）
- `HiveFileListingCache`：注入 fake `DirectoryLister`/`FileSystem`（seam 已支持），断言列文件过滤（目录、`_`/`.` 前缀）、两种异常语义、缓存/失效（REFRESH）不变。
- 写路径：`resolveObjectStoreFileSystem` 现为 `protected` 可 override 注入 fake；补 `forLocation` 语义单测（非切换返回 this、切换返回具体 FS）。
- 引擎：`DefaultConnectorContext.getFileSystem` 返回非空、缓存复用、close 释放。

### E2E（用户自跑，勿丢——新能力必配 e2e）
- 重打包 `fe-connector-hive` + fe-core + fe-filesystem-api/spi → 重部署 → 跑：
  - `external_table_p0/hive/test_string_dict_filter`（读 hdfs，本失败用例）；
  - hive insert/写套件（`external_table_p0/hive` 中 37 个含 INSERT）验证写路径转换（rename/delete/MPU）；
  - 若有对象存储环境，抽查 s3/oss 后端 hive 表读（验证 scheme 路由红利）。
- 断言与老实现逐位一致（读结果、写落盘、事务提交/回滚）。

## Open / Future（不属本次）
- per-user identity（`session.getUser()`）真正落地 + FS/listing 缓存按 identity keying。
- paimon/iceberg 保持自 bundle `hadoop-hdfs-client`（它们经各自 `FileIO` 做 I/O，非 Doris `FileSystem`；本方案不动）。
- 其它连接器（maxcompute 等）`getFileSystem` 默认 null，不受影响。
