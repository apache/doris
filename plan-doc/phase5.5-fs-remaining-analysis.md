# `org.apache.doris.fs` 包剩余文件分析

> 本文档记录 Phase 5.4 完成后，`fe/fe-core/src/main/java/org/apache/doris/fs/` 下
> 剩余 19 个文件的详细分析，作为后续阶段决策的依据。

---

## 总览

Phase 5.1~5.4 清理后，`fs/` 包仍剩余以下 19 个文件：

```
DirectoryLister.java
FileSystemCache.java
FileSystemDescriptor.java
FileSystemDirectoryLister.java
FileSystemFactory.java
FileSystemIOException.java
FileSystemPluginManager.java
FileSystemType.java
FileSystemUtil.java
FsStorageTypeAdapter.java
MemoryFileSystem.java
RemoteIterator.java
SchemaTypeMapper.java
SimpleRemoteIterator.java
SpiSwitchingFileSystem.java
StoragePropertiesConverter.java
TransactionDirectoryListingCacheKey.java
TransactionScopeCachingDirectoryListerFactory.java
TransactionScopeCachingDirectoryLister.java
```

---

## 逐文件分析

### 1. `DirectoryLister.java`
- **类型：** Interface
- **作用：** 目录列举操作的核心抽象。定义以 `Location` + 递归选项列出文件的契约，
  返回 `RemoteIterator<FileEntry>`。
- **关键依赖：**
  - `org.apache.doris.catalog.TableIf`（fe-core 专属）
  - `org.apache.doris.filesystem.spi.{FileEntry, FileSystem}`（SPI）
- **调用方：** `HiveScanNode`、`HiveExternalMetaCache`、`HMSExternalTable`、
  `PhysicalPlanTranslator`、`TransactionScopeCachingDirectoryLister`、
  `TransactionScopeCachingDirectoryListerFactory`
- **迁移评估：** ❌ **不可迁移** — 接口方法签名含 `TableIf`（fe-core catalog 类型），
  必须留在 fe-core。

---

### 2. `FileSystemCache.java`
- **类型：** Concrete class
- **作用：** 基于 Caffeine 的 SPI FileSystem 实例缓存，按存储标识+属性缓存，支持 TTL 过期。
  提供集中化的 FileSystem 生命周期管理。
- **关键依赖：**
  - `org.apache.doris.common.{CacheFactory, Config}`（fe-core）
  - `org.apache.doris.datasource.property.storage.StorageProperties`（fe-core）
  - `org.apache.doris.filesystem.spi.FileSystem`（SPI）
  - `com.github.benmanes.caffeine.cache.LoadingCache`（外部库）
- **调用方：** `ExternalMetaCacheMgr`（单例持有者）、`HiveExternalMetaCache`
- **迁移评估：** ❌ **不可迁移** — 使用 fe-core 的 `CacheFactory`、`Config`，
  以及 fe-core 专属的存储属性类型。

---

### 3. `FileSystemDescriptor.java`
- **类型：** Concrete class（POJO + 工厂方法）
- **作用：** 可 JSON 序列化的轻量 FileSystem 配置描述符。用于 backup/restore 场景，
  在 `FsStorageType`（foundation）和 Thrift 类型之间桥接。
- **关键依赖：**
  - `org.apache.doris.analysis.StorageBackend`（fe-core，Thrift 生成）
  - `org.apache.doris.datasource.property.storage.StorageProperties`（fe-core）
  - `org.apache.doris.foundation.fs.FsStorageType`（foundation 模块）
- **调用方：** `Repository`、`BackupJob`、`RestoreJob`、`BackupHandler`、
  `CloudRestoreJob`、`RepositoryMgr`
- **迁移评估：** ❌ **不可迁移** — 桥接 Thrift RPC 类型和 fe-core backup/restore 层，
  Thrift 生成代码仅存在于 fe-core。

---

### 4. `FileSystemDirectoryLister.java`
- **类型：** Concrete class（实现 `DirectoryLister`）
- **作用：** 默认目录列举实现，基于 SPI `FileSystemTransferUtil.globList()`。
  将 `IOException` 包装为 `FileSystemIOException`。
- **关键依赖：**
  - `org.apache.doris.catalog.TableIf`（fe-core，透传参数）
  - `org.apache.doris.filesystem.spi.{FileEntry, FileSystem, FileSystemTransferUtil}`（SPI）
- **调用方：** `HiveExternalMetaCache`、`HMSExternalTable`、`PhysicalPlanTranslator`
- **迁移评估：** ⚠️ **仅 `TableIf` 阻碍** — 若能抽象掉 `TableIf` 参数，此文件可迁移；
  目前作为 fe-core adapter 留在 fe-core。

---

### 5. `FileSystemFactory.java`
- **类型：** Concrete utility class（静态工厂）
- **作用：** 创建 SPI FileSystem 实例的核心工厂。管理 `FileSystemPluginManager` 生命周期，
  提供 ServiceLoader 降级发现（测试用），处理 broker 专属 FileSystem 创建。
- **关键依赖：**
  - `org.apache.doris.datasource.property.storage.StorageProperties`（fe-core）
  - `org.apache.doris.filesystem.spi.{FileSystem, FileSystemProvider}`（SPI）
  - `StoragePropertiesConverter`（本包）
- **调用方（17 处）：** `FileSystemCache`、`DelegateFileIO`、`S3SourceOffsetProvider`、
  `SpiSwitchingFileSystem`、`BrokerUtil`、`Repository`、`S3Resource`、`AzureResource`、
  `HdfsStorageVault`、`CloudPluginDownloader`、`CopyIntoAction`、`CopyLoadPendingTask`、
  `CreateStageCommand`、`InsertIntoTVFCommand`、`ObjectInfoAdapter`、`StageUtil`、
  `CleanCopyJobTask`、`Env`
- **迁移评估：** ❌ **核心桥接层，必须留** — 将 SPI 插件发现与 fe-core 存储属性层耦合，
  是整个 FileSystem 插件架构的枢纽。

---

### 6. `FileSystemIOException.java`
- **类型：** Concrete exception class（继承 `IOException`）
- **作用：** fe-core 层的文件系统 I/O 异常封装，可选携带 `ErrCode`（来自 backup 模块）。
- **关键依赖：**
  - `org.apache.doris.backup.Status.ErrCode`（fe-core，可选字段）
- **调用方：** `DirectoryLister`、`RemoteIterator`、`FileSystemDirectoryLister`、
  `TransactionScopeCachingDirectoryLister`、`HiveExternalMetaCache`
- **迁移评估：** ✅ **可考虑迁移** — 通用 IOException 包装，`ErrCode` 是 fe-core 特定但
  可作为可选字段或通过泛型抽象处理。若 `DirectoryLister`/`RemoteIterator` 也迁移则一并迁。

---

### 7. `FileSystemPluginManager.java`
- **类型：** Concrete class
- **作用：** 管理 FileSystem Provider 插件的完整生命周期：内置 ServiceLoader 加载 +
  目录式插件 `DirectoryPluginRuntimeManager` 加载，提供 provider 选择逻辑。
- **关键依赖：**
  - `org.apache.doris.extension.loader.{ClassLoadingPolicy, DirectoryPluginRuntimeManager, LoadFailure, LoadReport, PluginHandle}`（fe-core 插件框架）
  - `org.apache.doris.filesystem.spi.{FileSystem, FileSystemProvider}`（SPI）
- **调用方：** `FileSystemFactory.initPluginManager()`、`Env.initFileSystemPluginManager()`
- **迁移评估：** ❌ **不可迁移** — 完全依赖 fe-core 的 extension/plugin 加载框架，
  是 fe-core 插件加载器与 SPI FileSystem Provider 之间的桥接层。

---

### 8. `FileSystemType.java`
- **类型：** Enum
- **作用：** 支持的文件系统类型枚举（S3、HDFS、OFS、JFS、BROKER、FILE、AZURE、HTTP）。
  注：代码中有 TODO 指出多处存在类似枚举需要统一。
- **关键依赖：** 无（纯枚举，无 import）
- **调用方：** `SchemaTypeMapper`（枚举映射）、`LocationPath`（文件系统类型解析）
- **迁移评估：** ✅ **可迁移** — 完全独立的枚举，零 fe-core 依赖，
  但需评估 `LocationPath` 和 `SchemaTypeMapper` 中的使用是否影响迁移。

---

### 9. `FileSystemUtil.java`
- **类型：** Concrete utility class（静态方法）
- **作用：** 提供目录 rename 的异步辅助方法，使用 `CompletableFuture` + `Executor`。
- **关键依赖：**
  - `org.apache.doris.filesystem.spi.{FileSystem, Location}`（SPI）
  - `org.apache.hadoop.fs.Path`（Hadoop 依赖）
  - 标准 Java：`CompletableFuture`、`Executor`、`AtomicBoolean`
- **调用方：** `HMSTransaction`（事务提交时异步 rename）
- **迁移评估：** ✅ **可迁移** — 纯 SPI 操作工具类，无 fe-core 依赖。
  但 Hadoop `Path` 依赖需确认 SPI 模块是否引入了 Hadoop 依赖。

---

### 10. `FsStorageTypeAdapter.java`
- **类型：** Concrete utility class（final，静态方法）
- **作用：** `FsStorageType`（foundation 模块）与 Thrift 生成的 `StorageBackend.StorageType`
  之间的双向类型转换器。
- **关键依赖：**
  - `org.apache.doris.analysis.StorageBackend`（fe-core，Thrift 生成）
  - `org.apache.doris.foundation.fs.FsStorageType`（foundation 模块）
- **调用方：** `FileSystemDescriptor.getThriftStorageType()`
- **迁移评估：** ❌ **不可迁移** — 唯一同时能访问 Thrift 类型和 foundation 类型的位置，
  Thrift 生成代码仅在 fe-core。

---

### 11. `MemoryFileSystem.java`
- **类型：** Concrete class（实现 SPI `FileSystem`）
- **作用：** 用于单元测试的内存文件系统实现，以 `ConcurrentHashMap` 存储文件数据，
  采用隐式目录模型（路径末尾 `/` 表示目录）。
- **关键依赖：**
  - `org.apache.doris.filesystem.spi.{DorisInputFile, DorisInputStream, DorisOutputFile, FileEntry, FileIterator, Location}`（SPI，仅 SPI）
- **调用方：** 仅 `MemoryFileSystemTest`（测试）
- **迁移评估：** ✅ **强烈建议迁移** — 纯 SPI 实现，零 fe-core 依赖，
  仅供测试使用，放在生产代码位置不合理，应迁移至 `fe-filesystem-spi` 测试资源或独立测试模块。

---

### 12. `RemoteIterator.java`
- **类型：** Interface（泛型）
- **作用：** 远程/懒加载文件系统列举的通用迭代器抽象，与 `java.util.Iterator` 类似但
  声明 `FileSystemIOException` 替代通用异常。
- **关键依赖：**
  - `FileSystemIOException`（本包）
- **调用方：** `DirectoryLister`（返回类型）、`FileSystemDirectoryLister`、
  `TransactionScopeCachingDirectoryLister`、`SimpleRemoteIterator`（实现）、
  `HiveExternalMetaCache`（消费）
- **迁移评估：** ✅ **可与 `FileSystemIOException` 一起迁移** — 通用迭代器接口，
  仅依赖同包的 `FileSystemIOException`，如果两者一起迁移则无循环依赖。

---

### 13. `SchemaTypeMapper.java`
- **类型：** Enum
- **作用：** 将 URI scheme（协议）映射到对应存储类型、文件系统类型和内部文件类型的中心注册表，
  支持向后兼容（null schema → HDFS）。
- **关键依赖：**
  - `org.apache.doris.datasource.property.storage.StorageProperties`（fe-core）
  - `org.apache.doris.thrift.TFileType`（Thrift 生成，fe-core）
  - `FileSystemType`（本包）
- **调用方：** `LocationPath`（schema 解析）、`SchemaTypeMapperTest`、`LocationPathTest`
- **迁移评估：** ❌ **不可迁移** — 依赖 `StorageProperties.Type`（fe-core）和 Thrift 的
  `TFileType`，schema 映射逻辑与 fe-core 存储属性体系强耦合。

---

### 14. `SimpleRemoteIterator.java`
- **类型：** Concrete class（实现 `RemoteIterator`）
- **作用：** 将 `java.util.Iterator<FileEntry>` 适配为 `RemoteIterator<FileEntry>`
  的简单包装（仅异常类型不同）。注释说明来源于 Trino。
- **关键依赖：**
  - `RemoteIterator<FileEntry>`（本包接口）
  - `org.apache.doris.filesystem.spi.FileEntry`（SPI）
- **调用方：** `FileSystemDirectoryLister`、`TransactionScopeCachingDirectoryLister`
- **迁移评估：** ✅ **可与 `RemoteIterator` 一起迁移** — 纯适配器，无 fe-core 依赖，
  随 `RemoteIterator` 迁移即可。

---

### 15. `SpiSwitchingFileSystem.java`
- **类型：** Concrete class（实现 SPI `FileSystem`）
- **作用：** 路由/分发器，实现 SPI FileSystem 接口。根据 URI scheme/authority 将每个操作
  路由到对应后端 FileSystem，以 StorageProperties 标识缓存已解析的 FileSystem 实例。
- **关键依赖：**
  - `org.apache.doris.common.util.LocationPath`（fe-core）
  - `org.apache.doris.datasource.property.storage.StorageProperties`（fe-core）
  - `org.apache.doris.filesystem.spi.*`（SPI）
- **调用方：** `HMSExternalCatalog`、`HMSTransaction`、`HiveTransactionManager`、
  `TransactionManagerFactory`
- **迁移评估：** ❌ **不可迁移** — 核心 catalog-storage 集成点，基于 `LocationPath`
  和 `StorageProperties`（均为 fe-core）路由，深度集成于 fe-core catalog 架构。

---

### 16. `StoragePropertiesConverter.java`
- **类型：** Concrete utility class（静态方法）
- **作用：** 将 fe-core 的 `StorageProperties` 对象转换为 SPI `FileSystemProvider` 接口
  所需的 `Map<String, String>` 格式，集中管理各 StorageProperties 子类型的属性键映射。
- **关键依赖：**
  - `org.apache.doris.datasource.property.storage.{AbstractS3CompatibleProperties, AzureProperties, BrokerProperties, HdfsCompatibleProperties, StorageProperties}`（fe-core）
- **调用方：** `FileSystemFactory.getFileSystem(StorageProperties)`
- **迁移评估：** ❌ **不可迁移** — 唯一掌握 StorageProperties 子类型→属性键映射知识的地方，
  是 SPI 泛型 Map 接口与 fe-core 具体属性类型之间的唯一桥梁。

---

### 17. `TransactionDirectoryListingCacheKey.java`
- **类型：** Concrete class（不可变缓存键 POJO）
- **作用：** 组合 transactionId + location 路径的不可变缓存键，用于事务范围内的目录列举缓存。
  含正确的 `equals`/`hashCode`/`toString` 实现。
- **关键依赖：** 无（纯 POJO，无 import）
- **调用方：** `TransactionScopeCachingDirectoryLister`、`TransactionScopeCachingDirectoryListerFactory`
- **迁移评估：** ✅ **技术上可迁移** — 纯 POJO，无任何依赖。但调用方均在 fe-core，
  独立迁移收益有限，建议与 `RemoteIterator` 等一起决策。

---

### 18. `TransactionScopeCachingDirectoryListerFactory.java`
- **类型：** Concrete factory class
- **作用：** 可选地将 DirectoryLister 包装为事务范围内缓存版本。使用 fe-core 的
  `EvictableCacheBuilder` 创建基于权重驱逐的目录列举缓存。
- **关键依赖：**
  - `org.apache.doris.common.EvictableCacheBuilder`（fe-core）
  - `TransactionScopeCachingDirectoryLister`、`TransactionDirectoryListingCacheKey`（本包）
  - `com.google.common.cache.Cache`（Guava）
- **调用方：** `PhysicalPlanTranslator`（查询规划时创建包装 lister）
- **迁移评估：** ❌ **不可迁移** — 依赖 fe-core 的 `EvictableCacheBuilder`，
  缓存策略（权重驱逐、事务范围）是 fe-core 专属优化逻辑。

---

### 19. `TransactionScopeCachingDirectoryLister.java`
- **类型：** Concrete class（实现 `DirectoryLister`，装饰器模式）
- **作用：** 事务范围内的目录列举缓存装饰器，避免同一事务内对相同路径的重复远程调用。
  使用 `FetchingValueHolder` 实现并发安全的懒加载与共享错误处理。
- **关键依赖：**
  - `org.apache.doris.catalog.TableIf`（fe-core，透传给委托者）
  - 本包：`DirectoryLister`、`FileSystemIOException`、`RemoteIterator`、
    `SimpleRemoteIterator`、`TransactionDirectoryListingCacheKey`
  - SPI：`FileEntry`、`FileSystem`
  - `com.google.common.cache.Cache`、`com.google.common.annotations.VisibleForTesting`（Guava）
- **调用方：** `TransactionScopeCachingDirectoryListerFactory.get()`
- **迁移评估：** ❌ **不可迁移** — 实现 `DirectoryLister`（含 `TableIf` 参数），
  事务范围模式为 fe-core 专属。

---

## 汇总表

| 文件 | 类型 | 可迁移？ | 原因 |
|------|------|---------|------|
| `DirectoryLister` | Interface | ❌ 否 | 含 `TableIf` 参数（fe-core） |
| `FileSystemCache` | Class | ❌ 否 | 依赖 fe-core `CacheFactory`、`Config` |
| `FileSystemDescriptor` | Class | ❌ 否 | Thrift RPC 桥接（fe-core 专属） |
| `FileSystemDirectoryLister` | Class | ⚠️ 条件可迁移 | 仅 `TableIf` 参数阻碍 |
| `FileSystemFactory` | Class | ❌ 否 | 核心插件加载集成点 |
| `FileSystemIOException` | Class | ✅ 可迁移 | 通用 IOException 包装 |
| `FileSystemPluginManager` | Class | ❌ 否 | 依赖 fe-core 插件框架 |
| `FileSystemType` | Enum | ✅ 可迁移 | 零依赖纯枚举 |
| `FileSystemUtil` | Class | ✅ 可迁移 | 纯 SPI 操作，无 fe-core 依赖 |
| `FsStorageTypeAdapter` | Class | ❌ 否 | 唯一访问 Thrift 类型的桥接点 |
| `MemoryFileSystem` | Class | ✅ **强烈建议** | 纯 SPI 实现，仅测试用途 |
| `RemoteIterator` | Interface | ✅ 可迁移 | 通用迭代器，可与 IOExc 一起迁移 |
| `SchemaTypeMapper` | Enum | ❌ 否 | 依赖 `StorageProperties.Type` + Thrift |
| `SimpleRemoteIterator` | Class | ✅ 可迁移 | 随 `RemoteIterator` 迁移 |
| `SpiSwitchingFileSystem` | Class | ❌ 否 | 核心 catalog-storage 集成点 |
| `StoragePropertiesConverter` | Class | ❌ 否 | 唯一持有属性映射知识的桥接层 |
| `TransactionDirectoryListingCacheKey` | Class | ✅ 技术可迁 | 纯 POJO，但调用方均在 fe-core |
| `TransactionScopeCachingDirectoryListerFactory` | Class | ❌ 否 | 依赖 fe-core `EvictableCacheBuilder` |
| `TransactionScopeCachingDirectoryLister` | Class | ❌ 否 | 实现含 `TableIf` 的 fe-core 接口 |

---

## 结论与建议

### 必须留在 fe-core 的文件（11 个）
这些文件与 fe-core 的 catalog、Thrift、插件框架或缓存基础设施强耦合，无法迁移：

- `DirectoryLister`、`FileSystemCache`、`FileSystemDescriptor`、`FileSystemFactory`、
  `FileSystemPluginManager`、`FsStorageTypeAdapter`、`SchemaTypeMapper`、
  `SpiSwitchingFileSystem`、`StoragePropertiesConverter`、
  `TransactionScopeCachingDirectoryListerFactory`、`TransactionScopeCachingDirectoryLister`

### 有迁移价值的文件（6 个）
若决定进一步瘦身 fe-core 的 `fs/` 包，以下文件可考虑迁移到 SPI 或独立工具模块：

| 文件 | 建议目标 |
|------|---------|
| `MemoryFileSystem` | `fe-filesystem-spi` 测试资源（`src/test/java`） |
| `FileSystemUtil` | `fe-filesystem-spi`（需确认 Hadoop 依赖） |
| `RemoteIterator` + `FileSystemIOException` + `SimpleRemoteIterator` | `fe-filesystem-spi`（三者一起迁移） |
| `FileSystemType` | `fe-filesystem-spi`（零依赖枚举） |

### 条件可迁文件（1 个）
- `FileSystemDirectoryLister`：若重构 `DirectoryLister` 接口去除 `TableIf` 参数，
  则可迁移。但这属于较大的接口变更，需评估影响。

### `TransactionDirectoryListingCacheKey`（1 个）
技术上可迁移（纯 POJO），但调用方均在 fe-core，独立迁移收益不大，建议维持现状。

---

*记录时间：Phase 5.4 完成后*
*分支：`branch-fs-spi-phase5-clean`*
