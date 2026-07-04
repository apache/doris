# fe-core iceberg 删除 — 执行计划（分类 + 增量刀序）

> 配套分析 = `plan-doc/fe-core-iceberg-removal-plan.md` v2（本文件是**可执行刀序**，基于 2026-07-04 的 5-agent 分类工作流 `wf_7f1358fa-35d` 逐文件调用方核验产出，纠正 v2 若干过时判断）。
> **三态**：DELETE_NOW（无活消费者，零行为差）/ NEEDS_PREP（死码但删前须做前置）/ ALIVE_HMS（HMS-iceberg 车道 DlaType.ICEBERG 仍活，**禁删**至 hive 迁 SPI）。
> **铁律**：删前 grep 全调用方；每刀独立 commit + build + test + checkstyle 0 + import-gate 净；NEEDS_PREP 先做前置再删。

## 决定性路由事实（核验过）
- `CatalogFactory.SPI_READY_TYPES` 含 iceberg → 原生 iceberg catalog = `PluginDrivenExternalCatalog`（fe-connector-iceberg 承接）；`CatalogFactory:136-138` 的 built-in `case iceberg` 已删；GSON 旧类名标签 remap 到 PluginDriven（无反射回到旧类）。
- **hive/hms 不在 SPI_READY_TYPES** → `HMSExternalTable.getDlaType()==ICEBERG` 仍在 `PhysicalPlanTranslator:824-826` 构造 fe-core `source.IcebergScanNode` → 整条 HMS-iceberg 元数据/scan/cache 栈**活**。这是 ALIVE_HMS 簇的根，也是 `iceberg-core` 依赖的最终阻塞（阶段四，挂 hive 迁移）。
- **属性簇（Type.ICEBERG）只被翻闸后的 PLUGIN 路径吃**，HMS-iceberg 走 `type=hms`→HivePropertiesFactory，**从不解析 Type.ICEBERG** → 属性簇不是 ALIVE_HMS，rewire 掉 plugin 路径即可删（sub-task 2 自足）。

## 刀序（增量，每刀独立 commit）

### ✅ P0（DONE 本轮）= broker/ + helper 孤岛
`iceberg/broker/{IcebergBrokerIO,BrokerInputFile,BrokerInputStream}` + `iceberg/helper/IcebergWriterHelper`(+其测试)。零外部引用（broker 无 io-impl 反射；fe-core helper 仅自测引用，连接器有独立同名类）。零前置。

### P1 = fileio/（4 文件，需连带改 p2 回归）
`iceberg/fileio/{DelegateFileIO,DelegateInputFile,DelegateOutputFile,DelegateSeekableInputStream}`。**前置（用户 Q1=A 已裁定接受失效）**：改 `regression-test/suites/.../iceberg_on_hms_and_filesystem_and_dlf.groovy:481,488`（去掉 `io-impl=...DelegateFileIO` 用法）。反射透传路 `AbstractIcebergProperties:96 FILE_IO_IMPL → IcebergUtils.createIcebergHiveCatalog:1311-1321` 仍存在但接受其对内部 FQCN 失效。

### P2 = DLF 子树（5 文件）
`iceberg/dlf/{DLFCatalog,DLFTableOperations,DLFCachedClientPool,dlf/client/DLFClientPool}` + `iceberg/HiveCompatibleCatalog`(fe-core 副本)。**前置**：`DLFCatalog` 是 NEEDS_PREP——唯一活外部编译边 = `IcebergAliyunDLFMetaStoreProperties.initCatalog():48-66` 的 `new DLFCatalog()`（该 override 死码，仅经死的 `IcebergExternalCatalog.initializeCatalog` 可达）→ 先把它中性化（throw UnsupportedOperationException / 去掉 DLFCatalog 引用），再一刀删 5 文件。**`IcebergAliyunDLFMetaStoreProperties` 留**（plugin 路径经 IcebergPropertiesFactory "dlf" 注册仍活）。连带删测试 `IcebergDLFExternalCatalogTest` + `IcebergAliyunDLFMetaStorePropertiesTest` 里的 DLFCatalog 断言。连接器 twin 已有（`IcebergConnector.createDlfCatalog` + fe-connector .../dlf/*）。

### P3 = catalog flavor 簇（`IcebergExternalCatalogFactory` + 6 flavor）
`IcebergExternalCatalogFactory`（唯一实例化者，零 caller）+ `Iceberg{HMS,Glue,Hadoop,Jdbc,S3Tables}ExternalCatalog` + `IcebergDLFExternalCatalog`（+ `IcebergRestExternalCatalog`）。GSON 只用字符串标签 remap（非类引用）。**前置**：`IcebergRestExternalCatalog` 有 ALIVE_HMS `IcebergMetadataOps:174-175,1320` 的死 `instanceof` 臂 → 先削臂；`IcebergDLFExternalCatalog` 依赖 P2 已删的 DLFCatalog（顺序 P2→P3）。一刀删（flavor 互相 + factory 编译耦合）。base `IcebergExternalCatalog` **留**（P4）。

### P4 = 实体类 + base catalog（死臂清剿 + 前置改造）
`IcebergExternalTable` / `IcebergExternalDatabase` / `IcebergSysExternalTable` / `IcebergExternalCatalog`(base)。**前置（重）**：
- **搬常量**：`IcebergExternalCatalog` 的常量被活文件读 → 搬到 IcebergUtils / 新常量类，repoint `IcebergUtils:1876-1881`、`IcebergMetadataOps:122/124/255/492`、`AbstractIcebergProperties:177-182`、各 `Iceberg*MetaStoreProperties`。
- **修回放**：`ExternalCatalog.buildDbForInit case ICEBERG:972-973` 改 `return new PluginDrivenExternalDatabase(...)`（对齐 GSON remap；不可只删 case → 否则 fall through null 崩 db init）。
- **削死臂**（翻闸后恒 false，逐个删）：`IcebergExternalTable` → Env:4489-4500/4887-4898、RefreshManager:243-244、BindRelation:621-622、LogicalFileScan:213-221/263、StatisticsAutoCollector:152、StatisticsUtil:1001、InsertOverwriteTableCommand:323、MaterializeProbeVisitor:62（去 `IcebergExternalTable.class` 成员）、`IcebergUtils.showCreateView(IcebergExternalTable)`；`IcebergSysExternalTable` → Env:4492-4493/4890-4891、UserAuthentication:57-58、ShowCreateTableCommand:119-120/216-217、LogicalFileScan:263、`systable/IcebergSysTable.createSysExternalTable:80`。**⚠️ ShowCreateTableCommand 的 IcebergSysExternalTable 臂 = 本轮 F4 helper `redirectSysTableToSource` 里那条**（删臂时同步简化 helper）。

### P5 = 属性/鉴权 rewire + 属性簇删除（sub-task 2，最大一块）
**核心**：翻闸后 plugin iceberg catalog 仍经 fe-core 属性簇建鉴权/凭据：
- `PluginDrivenExternalCatalog.initPreExecutionAuthenticator:142-161` → `catalogProperty.getMetastoreProperties()`(:147, Type.ICEBERG→IcebergPropertiesFactory) → `msp.initExecutionAuthenticator`/`getExecutionAuthenticator`（Kerberos HadoopExecutionAuthenticator 由 `IcebergHMSMetaStoreProperties.initNormalizeAndCheckProps:64` / `IcebergFileSystemMetaStoreProperties` 建）→ 经 DefaultConnectorContext 暴露给 `IcebergConnector:763 executeAuthenticated`。
- `CatalogProperty.initStorageProperties:175-220` → getMetastoreProperties()(:181) → (a) `VendedCredentialsFactory.getProviderType case ICEBERG:62`→IcebergVendedCredentialsProvider；(b) `msp.getDerivedStorageProperties():197`→IcebergFileSystemMetaStoreProperties warehouse→fs.defaultFS 桥。
- **rewire 目标**：`fe-connector-metastore-iceberg` 已有 `IcebergHms/Glue/Dlf/Rest/Jdbc/NoOpMetaStoreProperties`+Provider，**但它们尚不自建鉴权器**（现依赖 fe-core context authenticator）→ **须先给连接器 metastore provider 补 authenticator 构建（镜像 paimon）**，再把 PluginDrivenExternalCatalog 的鉴权/凭据/derived-storage 接线改走连接器 metastore-spi。
- **rewire 完才能删**：`property/metastore/{AbstractIcebergProperties,IcebergPropertiesFactory,IcebergHMS/Glue/AliyunDLF/FileSystem/Jdbc/S3Tables MetaStoreProperties,IcebergRestProperties}` + `property/common/{IcebergAwsAssumeRoleProperties,IcebergAwsClientCredentialsProperties}` + `MetastoreProperties.java:51(enum ICEBERG)/:90(register)` + `VendedCredentialsFactory:62 case ICEBERG` + `DatasourcePrintableMap:63`（IcebergRestProperties 敏感键）+ 连通性 testers（F2/F3/F15/F16 死臂一并清）。⚠️ `IcebergAliyunDLFMetaStoreProperties` 到 P5 才随簇删（P2 只中性化其 initCatalog）。

### 阶段四（远期，禁删至 hive 迁 SPI）= ALIVE_HMS 23 文件 + iceberg-core
`IcebergUtils`/`IcebergMetadataOps`/`IcebergExternalMetaCache`/`source/`(6)/cache/(2)/`Iceberg{Snapshot,MvccSnapshot,Partition,PartitionInfo,ManifestEntryKey,SchemaCacheKey/Value,SnapshotCacheValue,TableCacheValue}`/`DorisTypeToIcebergType`/`IcebergVendedCredentialsProvider`(注:plugin 路径活,P5 后可能降级)/`IcebergMetricsReporter`。挂 hive→SPI（Q3=B）。

## 残留死执行器（P3/P4 顺带）
`LogicalIcebergMergeSink`（TableSink 已删、MergeSink 漏删）——去 SDK 化遗漏，随 sink 死臂清。
