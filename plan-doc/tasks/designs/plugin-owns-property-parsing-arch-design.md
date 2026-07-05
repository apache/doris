# Design — 已迁连接器属性解析全归插件（fe-core 零解析）架构

> **权威架构设计**，替代/升级 `iceberg-metastore-auth-connector-rewire-design.md`（后者是本设计的 iceberg-属性簇子集）。
> **用户原则（2026-07-05 拍板）**：fe-core 不持有任何属性解析/组装；storage 解析归 **fe-filesystem**、meta 解析归 **fe-connector**（均插件侧）；插件组装 → BE thrift → 回传 fe-core；paimon+iceberg 都要走，未迁连接器暂留残留。见 memory `catalog-spi-no-property-parsing-in-fecore`。
> 依据 = recon 工作流 `wf_61c70f0d-bce`（7 路，逐文件核验）。

## 0. 关键实证：目标架构**大部分已就位**（本迁移=退掉 fe-core 的第二份解析，非新建模块）

1. **`fe-filesystem-api/spi` 已是插件可用的共享存储契约**（Trino-SPI 式）：`org.apache.doris.filesystem.*` 对连接器插件**强制 parent-first**（`ConnectorPluginManager:64-65`），已是 fe-connector-spi/paimon 的 compile-dep，且 plugin-zip **排除打包**（`plugin-zip.xml:48`）。**接口无须从 fe-core 抽取**，解析器（fe-filesystem-{s3,oss,cos,obs,azure,hdfs,broker} 的 `FileSystemProvider.bind`）**本就在 fe-core 之外**。
2. **连接器读/扫描路径已是 fe-filesystem-native**：iceberg/paimon `buildStorageHadoopConfig` 迭代 `context.getStorageProperties() → sp.toHadoopProperties()`；scan provider 由 `sp.toBackendProperties().toMap()` 出 `location.*` BE 凭据。
3. **fe-core `datasource.property.storage.StorageProperties.createAll` 是冗余的第二份解析**（同一 raw props 被解析两遍）：(a) fe-core createAll（`CatalogProperty.initStorageProperties:211`）；(b) fe-filesystem `FileSystemFactory.bindAllStorageProperties`（连接器经 `context.getStorageProperties()` 触发）。
4. **持久化/展示只用 raw map**：GSON 只存 `CatalogProperty.properties`（raw）；SHOW CREATE / information_schema 渲染 **raw masked map**（`DatasourcePrintableMap` over `getProperties()`），从不用解析值。
5. **BE thrift 已可插件直建**：连接器已 by-reference 就地改 per-split（`populateRangeParams`）/scan-level（`populateScanLevelParams`）thrift 再转发 BE；fe-thrift 对 fe-connector-api 是 provided-scope，插件本可直建 thrift。

## 1. 目标架构

- **单一真相源** = fe-filesystem bind（storage）+ fe-connector-metastore（meta）。为**插件 catalog**，fe-core 退掉自己的 `StorageProperties.createAll` + `Paimon*/Iceberg* MetastoreProperties` 簇。
- **fe-core 仅保留**：raw catalog map（展示 + 回放）、一个 `bindStorage` **调用**接缝（调 fe-filesystem，不自己解析）、非 null 的 no-op ExecutionAuthenticator handle、以及不可约的引擎接缝（broker 地址注入 `Env.getBrokerMgr`、`String-map→THdfsParams` 的 `HdfsResource.generateHdfsParam`、fe-core 自己的 FS 操作 `SpiSwitchingFileSystem`）。
- **插件→fe-core 回传契约**：(1) 展示 = raw props + **敏感键集合**（供脱敏）；(2) 回放 = raw props（已满足）；(3) 扫描 = 已回传的成品（`ScanNodePropertiesResult` + by-reference thrift）；(4) 存储成品 = 插件**返回** BE 凭据/配置 map（真正的 gap，取代 fe-core 从 `getStoragePropertiesMap()` 派生）；(5) 鉴权 = 插件独占 Kerberos，fe-core 只留非 null no-op handle（因 `BaseExternalTableInsertExecutor:113/185` 无条件调 `getExecutionAuthenticator().execute()`，且 `ExternalCatalog:1391` null 会抛）。

## 2. 中央决策（**✅ 用户 2026-07-05 裁定 = A 家族：共享宿主 classpath + 连接器发起 bind**）= bind-location fork

> **裁定落地**：fe-filesystem 解析器留在宿主 classpath（parent-first、不打包）；**连接器直接调 fe-filesystem-api 发起 bind**（它已 compile-dep fe-filesystem-api），不经 fe-core context round-trip；fe-core 零解析。即下方 A，且取"连接器发起"变体（非 fe-core context 发起）。**B 不采纳。**

### （备查）原 fork 权衡

**fe-filesystem 解析器放哪？**
- **A（引擎宿主单 `bindStorage(raw)` 接缝，recon 推荐）**：fe-filesystem provider 留在 fe-core 的宿主 classpath（`DirectoryPluginRuntimeManager`），连接器经 context 调 bind。**满足"fe-core 不解析"**（解析器是独立 fe-filesystem 插件），近乎 drop-in（paimon 已 `toBackendProperties().toMap()`），无 AWS-SDK/hadoop 重复、无 TCCL split-brain。**代价**：fe-core 物理上仍在 bind **调用**路径上（但不解析）。
- **B（每插件自打包 fe-filesystem impl，fe-connector-cache 式 child-first）**：字面"在连接器 classloader 内解析"，但**每个连接器 + 独立 FS 插件都重复 AWS-SDK v2 + hadoop-common/aws**，重新引入隔离架构专门规避的 TCCL/split-brain 冲突。

> **待用户定**：A 满足"fe-core 零解析"的**实质**（解析在 FS 插件里），但 fe-core 仍是 bind 的**调用方**；B 是字面"连接器内解析"但代价大。recon + 我推荐 **A**。若你的"全部由插件完成"要求连接器**自己发起** bind（而非经 fe-core 的 context），A 也可微调为"连接器直接调 fe-filesystem"（连接器已 compile-dep fe-filesystem-api）——这仍是 A 家族，不落 B 的重复。

## 3. 其余决策（recon 推荐 default，本设计采纳，除非上面 fork 改变）
- **URI-normalize + TFileType**：先保留 fe-core 薄接缝（String-in/out、不含 catalog-specific 解析）；只有硬要求"fe-core 零存储码"才 port 进 fe-filesystem-api（低优先）。
- **BE-thrift 方向**：HDFS/kerberos 保持 INDIRECT（仅 `THdfsParams` 需 typed build，留 `HdfsResource.generateHdfsParam` 单一 fe-core 转换器）；S3/对象存储已是 `params.setProperties(map)`。原则"插件→BE thrift"已由 by-reference populate* 接缝结构性满足。
- **vended "跳过静态表"信号**：fe-core 对**所有插件 catalog 无条件不建静态存储表**（插件 100% 拥有 static+vended，precedence 已在扫描路径连接器侧）。**这直接解掉此前卡住的 vended 难题**——不判别、不 gate、不读 iceberg 键，彻底删净且守铁律。
- **fe-core auth handle** = no-op pass-through（getExecutionAuthenticator 只须非 null）。

## 4. 迁移刀序（每刀独立 commit + build + test + checkstyle + import-gate；**先 parity 验证再删 fe-core 路**）
- **S1**：无（fe-filesystem-api/spi 已插件可用，**别抽取**）。
- **S2**：`getStorageProperties()` supplier 改绑 raw props 直传（`DefaultConnectorContext:239` 去掉 `getStoragePropertiesMap()→getOrigProps()` round-trip）。**须保住** derived defaults（warehouse→defaultFS）否则 HDFS-HA/warehouse-only 回归。
- **S3**：`getBackendStorageProperties()` 从 fe-core `CredentialUtils` 改到 fe-filesystem `getStorageProperties().toBackendProperties().toMap()`；iceberg WRITE 路（`IcebergWritePlanProvider:639`）迁到 paimon 形。**删 fe-core 路前做 BE `location.*` map 逐字 parity diff**。
- **S4**：vended 全连接器侧（插件 bind vended token 走 fe-filesystem）；退 fe-core `VendedCredentialsFactory`/`AbstractVendedCredentialsProvider` + `CatalogProperty:184-196` gate；fe-core 对插件 catalog 停建静态表。
- **S5**：URI-normalize/TFileType 决策（默认留薄接缝）。
- **S6**：auth build 移连接器：删 `initPreExecutionAuthenticator` 的 parse（`:153`），fe-core handle 降 no-op；删 `getOrderedStoragePropertiesList` 唯一消费者（paimon HDFS-Kerberos）后删该字段。
- **S7**：退 fe-core `Paimon*/Iceberg* MetastoreProperties` 簇 + un-register `Type.PAIMON/ICEBERG`（`MetastoreProperties:90-91`），待三消费者（auth/vended/derived）全连接器化后。
- **S8**：CUT4 的 `deriveHdfsDefaultFsFromWarehouse`（`CatalogProperty:227-250`）**搬回 fe-connector-metastore-iceberg**（其来源），与 S6-S7 同步、**不早于**（对只带 warehouse 的 HA native-iceberg 仍 load-bearing）。
- **S9**：BE-thrift 方向落地（默认 INDIRECT；S3 已 trivial）。
- **S10（铁律：legacy 残留保命）**：`CatalogProperty` 四个静态 map 方法 + `initStorageProperties` + fe-core `StorageProperties.createAll` **保活**，直到 Hive/Hudi/LakeSoul/HMS-iceberg 全离 SPI（它们直接读 fe-core 解析 map 出 BE thrift：`HiveScanNode:559`/`HudiScanNode:252,425`/`HiveTableSink:133,164`/`IcebergUtils:1309`）。**只 repoint 插件路，别删共享方法。**

## 5. 三刀对账
- **CUT1（`cf8dda9f058` 连接器自建 HMS 鉴权）**：方向对；当前 additive（fe-core 仍并行建）。S6 收尾：退 fe-core parse + handle 降 no-op（不能删，插入-提交路 `BaseExternalTableInsertExecutor:113/185` 用它）。⚠️ 需确认 fe-core authenticator 对 insert-commit 是死重还是 load-bearing（无 live Kerberos e2e）。
- **CUT2（`eb9201dc0a6` SHOW CREATE 脱敏）**：**层对、保留**（展示是 fe-core-owned raw map）。仅改敏感键**来源**：现 fe-core 硬编 4 键（"keep in sync"注释脆），目标=插件**返回**敏感键集经**既有** `DatasourcePrintableMap.registerSensitiveKeys` 通道注册（FS provider 启动时已这么做）。CUT2 = 正确层 + 临时硬编 + 待换连接器供源。
- **CUT4（`0de34db83fb` warehouse→fs.defaultFS helper 在 fe-core）**：**方向反**（fe-core 解析 storage）。S8 搬回 fe-connector-metastore-iceberg；**与 S6-S8 一起搬、不早于**（现仍对 HA native-iceberg load-bearing）。

## 6. 风险 / 验证门（每刀）
- **classloader/bundling**：方案 A 全靠 `org.apache.doris.filesystem.*` parent-first 保 StorageProperties 单类身份。⚠️ 未验：`FileSystemFactory.bindAll` bind() 时是否 pin TCCL 到 FS 插件 loader 使 provider 内部反射解析插件自带 AWS/hadoop——依赖 A 前须确认。
- **double-parse drift**：write（fe-core parse）vs scan（fe-filesystem bind）两路到 BE-canonical map，repoint write 前**须逐字 parity diff**；derived defaults 仅现于 fe-core parse，fe-filesystem bind 须复现否则 HDFS-HA/warehouse-only 回归。
- **paimon 同范围**：非 iceberg-only；且共享码仍服务 legacy Hive/Hudi/LakeSoul/HMS-iceberg（S10 保命）。
- **auth 冗余/正确性**：降 no-op 前须证连接器 pluginAuthenticator 覆盖 commit/abort，否则静默丢 Kerberos（flip-gated）。
- **flip-gated e2e**：全在插件路、无 green Kerberos-HDFS e2e；每刀门 = 重部署 classloader 冒烟 + BE `location.*` map parity diff（fe-core 路 vs fe-filesystem 路）后再删 fe-core 路。
- **SHOW CREATE 脱敏漂移**（CUT2 临时硬编期）：新连接器密钥会静默 unmask 直到连接器供源通道落地——优先做该通道。
