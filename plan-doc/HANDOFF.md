# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取子线**已彻底 CLOSED**（2026-06-22 收官，全部合入主线 #64446/#64653/#64655）——
> [`metastore-storage-refactor/`](./metastore-storage-refactor/) 文档仅作历史留存、**后续勿读**；需了解 metastore-spi 现状请直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 — **P6.2 实现起步（P6.2-T01：`IcebergScanPlanProvider` 骨架）**（**P6.2 recon + 逐 task 拆解已完成、4 决策签字，见下「✅ P6.2 recon = DONE」；P6.1〔T01–T10〕全绿**）

> **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**翻闸全有或全无，P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（翻闸只在 P6.6）。
>
> **✅ P6.1 = DONE（本 session 2026-06-22 收口 T10）**：P6.1 task 表 T01–T10 全绿（见 `P6-iceberg-migration.md:143-154`）。T10 经 redefine 吸收了「metastore 模块拆分（Phase A，行为不变）+ iceberg per-flavor 校验（Phase B，§4 逐字）」——validateProperties 接线只是 Phase B 的尾巴。**A-gate + B-gate 全绿；对抗 parity 复核 4 MATCH + 1 nit。**
> **下一步 = P6.2 实现（从 P6.2-T01 起）——不是 P6.6！** P6.6 翻闸是「全有或全无」（`CatalogFactory:104-113`），**须等 P6.1–P6.5 全部实现完**（scan/write/procedure/sys-table 都还没做）；现在翻闸会让所有 iceberg 查询走只有读元数据+校验的连接器→scan/write 全断。

---

# ✅ P6.2 recon = DONE（2026-06-22 本 session；recon-only，**未改一行产品代码**，仅文档）

> 本 session = recon session（playbook §7.3）：7 路并行结构化 recon（workflow `wf_a74302c7-194`）+ 主线直读 paimon vended 链/`ConnectorContext`/`ConnectorDeleteFile`/`ConnectorMetaInvalidator`。产出 = recon 笔记 + P6.2 逐 task 拆解 + 4 决策签字。**下一轮起实现（P6.2-T01）。**

- **recon 笔记**：[`research/p6.2-iceberg-scan-recon.md`](./research/p6.2-iceberg-scan-recon.md)（scan/MVCC/cache/vended/field-id old→new 映射 + 风险登记 + paimon 模板锚点）。
- **逐 task 拆解**：`tasks/P6-iceberg-migration.md` 末「P6.2 逐 task 拆解」（P6.2-T01..T11）。
- **🔑 用户裁定（4 项，全签字 2026-06-22）**：
  1. **D6 cache = 全连接器内部**（镜像 `PaimonLatestSnapshotCache` + schema-at-snapshot memo + manifest cache loader/value 也搬进连接器，path-keyed）。
  2. **field-id = 字符串属性**（`getScanNodeProperties` 用 iceberg Schema 构 `history_schema_info`，**不改 `ConnectorColumn`**；镜像 paimon `FIX-SCHEMA-EVOLUTION`）。
  3. **O2 vended = 复用既有 `ConnectorContext` 接缝，E6 取消**——code-grounded 证 paimon **无独立 `ConnectorCredentials` SPI**，用 `context.vendStorageCredentials(rawToken)` + `normalizeStorageUri(uri,token)`（`DefaultConnectorContext` 实现，引擎中立）。iceberg token 同类（raw cloud props）直接复用；连接器只写 iceberg SDK `extractVendedToken(table)`；仅 REST，DLF 凭据走 HiveConf（T07/T10 已接）。详见 recon §5。
  4. **节奏 = 本轮收尾 recon，下轮起实现**。
- **🔑 关键结论：P6.2 净 0 个新 SPI 接口、0 处 SPI 破坏**——scan（`ConnectorScanPlanProvider`）/MVCC（`ConnectorMvccSnapshot`/`PluginDrivenMvccExternalTable`）接缝就绪；delete equality 元数据编码进既有 `ConnectorDeleteFile.properties`；field-id/vended 走既有接缝；cache 失效用既有 `ConnectorMetaInvalidator`/`Connector.invalidate*`（默认方法已存在，paimon override 过）。
- **🔴 最高危**（UT 不可见，P6.6 docker 验）：**field-id 丢失**（schema 演化 BE SIGSEGV/DCHECK，CI #969249 类）+ **分区列双填**（必发 `path_partition_keys`，CI #968880 类）。
- **下一步（实现起点）**：P6.2-T01 = `IcebergScanPlanProvider` 骨架 + `IcebergScanRange` + 接线 + `ignorePartitionPruneShortCircuit()=true` + 测试基建扩。镜像 `PaimonScanPlanProvider`(1589)/`PaimonScanRange`(383)。**起步先**读 recon 笔记 + migration P6.2 块 + paimon 真实代码（大文件 subagent 总结）。

## 🧭 用户裁定（T10 session，按序）
1. **iceberg CREATE-CATALOG 校验做全量 per-flavor**（非最小/非延迟）。无任何回归测试强制（115 套件无一断言建目录属性校验报错；paimon 同），纯取忠实度。
2. **校验逻辑下沉到共享 metastore 层**（不在连接器写 bespoke）。storage 半边**已被 fe-filesystem bind 时校验**（`S3FileSystemProperties.validate()` 等），连接器拿到的 `StorageProperties` 已校验过 → 本任务只做 **metastore 半边**。
3. **把 metastore 重构成 per-engine 模块、镜像 fe-filesystem**（`-spi` 只留扩展点+共享基类，impl 进 per-engine 模块）。理由：impl 塞在 `-spi` 既非严格 SPI 惯例、也与本仓库 fe-filesystem（`-spi` 纯接口、impl 在 `fe-filesystem-s3/-oss/...`）不一致。

## ⛓️ 关键架构洞察（写下来免重推）
- **classpath 隔离消解派发碰撞**：`bindForType("hms"/"rest"/...)` 是 ServiceLoader 首命中、token-only。paimon/iceberg 同 token 会撞。**但**各连接器 plugin-zip 只装本引擎的 metastore 模块 → 运行时 ServiceLoader 只见本引擎 provider → `bindForType(flavor)` 引擎内唯一解。**所以拆模块后无需改派发签名、无需 engine-qualified 参数**。（单测同 classpath 含两套时按引擎 scope 断言。）
- **HMS/DLF 是双引擎共享的**（iceberg `IcebergConnector:175/248` 用 `bindForType("hms"/"dlf")` 拿 `toHiveConfOverrides`/`toDlfCatalogConf` 装 conf）。拆模块后 iceberg classpath 不含 paimon impl → iceberg 须有**自己的** Hms/Dlf。共享的 conf + 连接规则抽进 `-spi` 的**引擎中立基类** `AbstractHmsMetaStoreProperties`/`AbstractDlfMetaStoreProperties`（`toHiveConfOverrides`/`toDlfCatalogConf` + `validateConnection`），paimon/iceberg 各自 extend → **零规则重复，paimon 报错/顺序字节不变**。
- **校验全是 prop-map 驱动**（REST 读 `iceberg.rest.*`、Glue `glue.*`、JDBC、HMS、DLF 全从 props）→ `validateProperties(Map)` 够用，不需 storage/context。
- **JDBC driver_class/url 规则是惰性**（initCatalog）→ 已被连接器现有 `preCreateValidation`+`maybeRegisterJdbcDriver` 覆盖，**不进 validateProperties**（只做 uri/catalog_name/warehouse 三条解析期必填）。
- **逐字 legacy 规则已对抗验证 `complete-and-exact`**（5-agent workflow `wf_8ae4353f-9a8`）→ 全部落在设计文档 **§4**（REST 10 条含急切/延迟触发序、Glue 4 条、JDBC 3 条、HMS 3 条、DLF 3 条 AK/SK/endpoint〔**iceberg 不要求 warehouse、不做 OSS 检查**，有别于共享 DLF impl 的 `requireWarehouse`+带"Paimon"字样的 `requireOssStorage`〕）。**实现时逐字照搬 §4 报错串。**

## ✅ T05 = DONE（2026-06-22，part3 `b0dbe91e1a5` 已 commit，未 push）

> 设计文档 = `plan-doc/tasks/designs/P6-T05-iceberg-catalog-assembly-design.md`。前序：part1 `562201deb9f`（metastore-spi `bindForType`），part2 `6f7b292b5c6`（factory common base + S3FileIO）。
> **T05 全部完成**：5-flavor 装配 + connector LIVE 接线 + DriverShim + 52 parity 测试。**验收全绿**：fe-connector-iceberg UT 75/0/0、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`。

1. **5-agent code-grounded 复核**（legacy `AbstractIcebergProperties`+5 flavor 实体逐行）→ 精确 key 派生契约。**纠正 2 处过时 HANDOFF**：
   - **GLUE 是自包含的**（非旧 HANDOFF 说的「走 base chooseS3」）：legacy `appendS3Props` **无条件 plain put** 5 个 `s3.*`（空串也发，从 `S3Properties.of(origProps)` 专属 store，**忽略** storagePropertiesList），不走 base `toS3FileIOProperties`（那条才 blank-guard + assume-role）。连接器版从 `chosenS3`（fe-filesystem 单一真源 D-061）无条件发，`glue.*` 凭据从 raw props 读。
   - **HADOOP warehouse FE 不强制**（旧 HANDOFF 说「必填」错）：仅 JDBC 强制 warehouse；HADOOP 靠 HadoopCatalog 下游自己抛——**不要加 warehouse 检查**。
   - REST 凭据类型优先级（核 `IcebergAwsClientCredentialsProperties.getCredentialType`）：**EXPLICIT(AK&SK) > ASSUME_ROLE(roleArn) > PROVIDER_CHAIN**（连接器用 `hasStaticCredentials()`/`hasAssumeRole()` 对齐）。
2. **实现（纯静态，镜像 paimon）**：`IcebergCatalogFactory.{appendRestProperties,appendGlueProperties,appendJdbcProperties,buildCatalogProperties(编排),resolveCatalogName,buildHadoopConfiguration,assembleHiveConf}`；`IcebergConnector.createCatalog` flavor switch（HMS `bindForType`→HiveConf；GLUE conf=null；JDBC driver+catalog_name 位置参数；REST/HADOOP storage conf）+ TCCL-pin + `executeAuthenticated` + DriverShim + `preCreateValidation`。pom 加 `hadoop-mapreduce-client-core`+`commons-lang` **test-scope**（`new HiveConf()` 静态初始化需,运行时闭包已含,不入 plugin-zip,镜像 paimon）。
3. **对抗 parity review**（3-flavor 独立比对 legacy）= **0 confirmed bug**（2 个 minor 经独立 verify 均非真 bug：REST oauth2 token else 分支 `""` vs `null` 仅在校验拒绝的不可达态;JDBC driver dedup 是有意的 paimon-parity 重构,可观察结果一致）。

## 🟢 T05 已交付的可复用件（T06/T07 直接接）

- **`buildCatalogProperties(props,flavor,chosenS3)`**：编排器,handles base+impl+per-flavor+type 删除+jdbc catalog_name 删除。**s3tables/dlf 当前走 default 分支 = 仅 base+impl（占位）**——T06/T07 在此扩。
- **`IcebergConnector.createCatalog` switch 的 `default` 分支** = rest/hadoop/s3tables/dlf 共用 `buildHadoopConfiguration`；s3tables/dlf 的 bespoke 实例化（`new S3TablesCatalog().initialize` / `new DLFCatalog().setConf().initialize`）尚未接,现仍走 `CatalogUtil.buildIcebergCatalog`(impl-name)——**dlf 会 ClassNotFound（`...dlf.DLFCatalog` 未建），s3tables 可能能起但缺 region/s3 派生**。因 iceberg 不在 SPI_READY 故未被触发。
- **`MetaStoreProviders.bindForType("dlf",props,storageConf)`** 已就绪（part1）：T07 DLF 直接调 → `DlfMetaStoreProperties.toDlfCatalogConf()`,勿再做 metastore-spi recon。
- **`assembleHiveConf` / `buildHadoopConfiguration` / `firstNonBlank`**（public static）= T06/T07 复用。

## 🟡 T05 已记录的 deviation（UT 不可见,仅 P6.6 docker plugin-zip e2e 真验）

- **REST PROVIDER_CHAIN 非-DEFAULT** provider-class 跳过（连接器不能 import fe-core `AwsCredentialsProviderFactory.getV2ClassName`；DEFAULT=no-op=常见情形无缺口）。
- **GLUE/REST 的 `s3.*`** 取自 fe-filesystem 类型化 `S3CompatibleFileSystemProperties`（D-061）而非 legacy `S3Properties.of(origProps)`——若 glue 仅用 `glue.*` 别名供凭据（fe-filesystem 不读这些进 S3 store），s3.* FileIO 凭据可能缺（glue-client 凭据仍从 `glue.*` 读,不受影响）。
- **base 的 no-S3-store fallback region**（`getRegionFromProperties`）未移植（niche；连接器读类型化 storage）。

## ✅ T06 = DONE（2026-06-22，`386e183c091` 已 commit，未 push）

> **纠正 HANDOFF 过时计划**：HANDOFF 原写「2-arg `initialize(name,props)`」是错的——反编译 s3tables SDK 证实 2-arg 走 `DefaultS3TablesAwsClientFactory`，其 `applyClientCredentialConfigurations` **只认 `client.credentials-provider` 类名**，**静默丢 `s3.access-key-id`/`s3.secret-access-key`** → 静态凭据 s3tables 控制面会回退默认链鉴权失败。legacy 实走 **3-arg** `initialize(name,opts,client)` 手搓 client。**用户签字「全阶梯」凭据**（静态+assume-role STS+默认链）。

- **`IcebergCatalogFactory.buildS3TablesCatalogProperties`（纯）**：base（copy-all + warehouse=table-bucket ARN + manifest-cache）+ s3tables S3FileIO dialect。**不加 catalog-impl、不删 type**（legacy bespoke `initCatalog` 都不做；只有 CatalogUtil 路做；3-arg 仅校验 warehouse 非空）。
- **EXPLICIT-wins 凭据阶梯**（对抗 parity review 抓出的真 bug）：s3tables 用 `appendS3TablesFileIOProperties`（镜像 legacy `putS3FileIOCredentialProperties`）——**静态 AK/SK 在场则压制 assume-role 块**；这**有别于** rest/hadoop/jdbc 用的 `appendS3FileIOProperties`（=`toS3FileIOProperties`，恒发 assume-role-if-role）。两 helper 共享新私有 `putS3FileIODialect`。**rest/hadoop/jdbc 的 always-emit 不变**（与 legacy 一致，勿改）。
- **`IcebergConnector`**：s3tables 在 CatalogUtil flavor switch **之前**早分支到 `createS3TablesCatalog`；`buildS3TablesClient`（region + `buildAwsCredentialsProvider` + `s3tables.endpoint` override + `HttpClientProperties`）+ `new S3TablesCatalog().initialize(name,opts,client)`，复用 TCCL-pin + `executeAuthenticated`。`buildCatalogAuthenticated` 泛化成 `Callable<Catalog>`。**无 chosenS3 / region 空 → fail-loud**。**不调 setConf**（legacy 不调；HANDOFF「setConf」也是过时的）。
- **deviation（UT 不可见，仅 P6.6 docker plugin-zip e2e 验）**：非-DEFAULT `PROVIDER_CHAIN` provider mode 退化成 `DefaultCredentialsProvider`（连接器不能 import fe-core `AwsCredentialsProviderFactory.createV2`）；assume-role 的 STS base creds 同此退化。常见静态/实例角色不受影响。**与 T05 REST/glue PROVIDER_CHAIN gap 同族**。
- **验收全绿**：fe-connector-iceberg UT 81/0/0（IcebergCatalogFactoryTest +4 prop-map 含 EXPLICIT-wins 压制；新 IcebergConnectorTest 2 = 无 storage / region 空 fail-loud）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`。SDK 闭包 T04 已就位（s3tables/sts/auth），无 pom 改。

## ✅ T07 = DONE（2026-06-22，`16926486e8c` 已 commit，未 push）

> **纠正 HANDOFF 过时计划**：HANDOFF 列 `ProxyMetaStoreClient` 为「vendored」要 port——recon 实证它（198KB）+ `DataLakeConfig` 在 **hive-catalog-shade 未 relocate**（`com/aliyun/datalake/...`），且仅 `RetryingMetaStoreClient.getProxy(..., ProxyMetaStoreClient.class.getName())` **反射按名加载** → **不 port 2193 行源码**，`DLFClientPool` 直接 import（从 shade 解析，plugin-zip 已 ship `hive-catalog-shade-3.1.1.jar`）。

- **5 文件 port 进连接器** `org.apache.doris.connector.iceberg.dlf[.client]`：`HiveCompatibleCatalog`(DLF-only base)、`DLFTableOperations`、`DLFClientPool`、`DLFCachedClientPool` **逐字**（仅改 package；零 fe-core import；iceberg-hive 1.10.1 + relocated `shade.doris.hive...thrift` 均来自 hive-catalog-shade，连接器直连 iceberg 也 1.10.1 **无版本错位**）。
- **`DLFCatalog` 断 3 fe-core import**（`OSSProperties`/`CloudCredential`/`S3Util`，全在 `initializeFileIO`）：OSS 配置改读类型化 `S3CompatibleFileSystemProperties`（D-061，ctor 注入）；S3 client 内联 `buildOssS3Client` **逐行复刻** `S3Util.buildS3Client`（UrlConnection 30s + AwsS3V4 signer + 3-retry equal-jitter + **chunkedEncoding=false**〔OSS 必需〕+ path-style）；oss→s3.oss endpoint rewrite 抽成纯函数 `toS3CompatibleEndpoint`（单测）。
- **`IcebergCatalogFactory.buildDlfConfiguration`（纯）**：`toDlfCatalogConf()` 的 8 个 `dlf.catalog.*` 键（= `DataLakeConfig.CATALOG_*` 常量值，反编译核对逐一对上）+ legacy 的 `hive.metastore.type=dlf` + `type=hms`。
- **`IcebergConnector`**：dlf 在 CatalogUtil flavor switch **之前**早分支 → `createDlfCatalog`（conf 经 `bindForType("dlf",...).toDlfCatalogConf()`；base catalogProps；`new DLFCatalog(oss).setConf(conf).initialize(...)` 包在 TCCL-pin + executeAuthenticated）；无 OSS storage **fail-loud**。
- **deviation（UT 不可见，P6.6 docker 验）**：①ProxyMetaStoreClient 按名从 shade 加载；②非静态凭据回退 `DefaultCredentialsProvider`（legacy 是 5-provider 链；DLF 必有 OSS 凭据故不可达）；③`toDlfCatalogConf` 把 OSS storage config 叠进 metastore conf（benign 多键）。
- **对抗 parity review = 0 confirmed bug**（4 文件逐字、sever/conf/endpoint/creds/wiring 全对 legacy）。**未删 fe-core legacy DLF**（STILL-CONSUMED 至翻闸）。
- **验收全绿**：fe-connector-iceberg UT 85/0/0（+`DLFCatalogTest` 2 endpoint、+`buildDlfConfiguration` 1、+dlf fail-loud 1）、checkstyle 0、import-gate 净、plugin-zip 含 hive-catalog-shade、iceberg 仍**不在** `SPI_READY_TYPES`、**无 pom 改**。

## ✅ T09 = DONE（2026-06-22，未 push；4 读路径 gap + 字节级命名空间 split + auth 包裹）

> 设计文档 = `plan-doc/tasks/designs/P6-T09-iceberg-read-parity-design.md`。**5-reviewer 对抗 parity 复核 + 每发现二次 adversarial verify**（workflow）跑过：4 个原 gap 全 MATCH；额外查到 2 真差异（已修），1 误报（已证伪）。

- **G1 format-version**：`IcebergConnectorMetadata.getFormatVersion(table)` 逐字 port legacy `IcebergUtils.getFormatVersion`（`BaseTable→operations().current().formatVersion()`，else `format-version` prop parseInt，default 2）。删除 skeleton 的 `spec().specId()>=0?2:1`（恒 2）。
- **G2 column 构造**：name `toLowerCase(Locale.ROOT)`、isKey=true、nullable 恒 true（不读 `isOptional`）、`WITH_TIMEZONE` marker（源 TIMESTAMP+`shouldAdjustToUTC`，**独立于** mapping flag，顶层列）。`ConnectorColumnConverter` 已确认会把这些 flag 落到 `Column`。field-id/uniqueId 仍丢（`ConnectorColumn` 无载体，DESCRIBE 不需，scan 路 P6.2+）。
- **G3/G4 listing**：`CatalogBackedIcebergCatalogOps` 内部加嵌套命名空间递归（REST+`iceberg.rest.nested-namespace-enabled` flag，dotted）、view 过滤（减 `listViews`，gate `ViewCatalog && (!rest||iceberg.rest.view-enabled)`）、dotted-namespace split + `external_catalog.name` 追加。配置由 `IcebergConnector.getMetadata` 从 props 派生后线程进 seam。**seam 接口不变**（递归/过滤/split 全 INTERNAL）。
- **G2.1 命名空间 split 字节级**：改用 legacy 同款 Guava `Splitter.on('.').omitEmptyStrings().trimResults()`（plain Java `String.trim()` 漏 U+0020 以上的 Unicode 空白如 U+3000；NBSP U+00A0 不算差异——Guava whitespace() 不含它）。guava 33.2.1 compile-scope（hadoop 传递，已 bundle）。
- **G5 auth 包裹（用户裁定纳入 T09）**：`IcebergConnectorMetadata` ctor 增 `ConnectorContext`（由 `getMetadata` 线程），5 个读全包 `context.executeAuthenticated(...)`，逐方法镜像 legacy 异常语义（listDatabaseNames warn+rethrow；db/tableExists+loadTable rethrow；listTableNames `catch RuntimeException→rethrow` 再 wrap——iceberg `NoSuch*` 是 unchecked，`UGI.doAs` 不包，故无需 lambda 内 catch，**有别于** paimon 的 checked 异常处理）。seam 保持 auth-agnostic。**仅 Kerberized HMS/REST 翻闸后可见**（simple-auth `executeAuthenticated` 直通）。
- **误报已证伪**：`iceberg.format-version`/`iceberg.partition-spec` 合成键无 fe-core 读者且**不会**泄漏进 SHOW CREATE——`Env.java:4936-4939` 把 PluginDriven LOCATION+PROPERTIES 渲染 gate 在 `PAIMON_EXTERNAL_TABLE`，iceberg `getEngineTableTypeName()` 返回 `PLUGIN_EXTERNAL_TABLE` → 整块跳过。未来 iceberg SHOW-CREATE 渲染分支（P6.6）才需处理这些键。
- **测试**（无 Mockito，fail-loud fake）：新 `FakeIcebergCatalog`/`FakeIcebergViewCatalog`/`PlainIcebergCatalog`（iceberg `Catalog`/`SupportsNamespaces`/`ViewCatalog` 1.10.1 抽象集，注意 `Catalog`+`ViewCatalog` 的 `initialize` diamond 须 override）+ `CatalogBackedIcebergCatalogOpsTest`（13）；`IcebergConnectorMetadataTest`（19，含 column flag/format-version/lowercase/withTimeZone/auth `authCount`+`failAuth`）；`RecordingConnectorContext`（harness 早就备好 authCount/failAuth）。**TDD：每改 RED→GREEN，auth 包裹 post-hoc 拆一个 wrap 验非空 RED 再恢复。**
- **验收全绿**：fe-connector-iceberg UT **103/0/0**、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、**无 pom 改**（guava 已是 compile 依赖）。

## ✅ T10 Phase A = DONE（2026-06-22，本 HANDOFF commit，未 push）— metastore 模块拆分（filesystem 式），行为不变

> 设计文档 = `P6-T10-iceberg-validation-design.md` v2 §5/§10。**A-gate 全绿**：metastore-spi UT 4/0/0、**metastore-paimon UT 43/0/0**、**paimon 连接器 UT 318/0/0**（1 skip=env-gated `PaimonLiveConnectivityTest`）、iceberg 连接器 UT 103/0/0（reactor 健康，未被破）、checkstyle 0、import-gate 净。

- **新模块 `fe-connector-metastore-paimon`**（reactor 入 `fe-connector/pom.xml`，紧跟 metastore-spi）：5 个 `Paimon{Hms,Dlf,Rest,Jdbc,FileSystem}MetaStoreProperties` + 5 个 `Paimon…MetaStoreProvider` + `META-INF/services`（paimon FQN），包 `org.apache.doris.connector.metastore.paimon.{hms,dlf,rest,jdbc,fs}`。镜像 fe-filesystem per-backend。
- **`-spi` 瘦身**：删 5 impl + 5 provider + META-INF/services + 5 flavor 测试 + dispatch 测试；**新增 2 个引擎中立基类** `Abstract{Hms,Dlf}MetaStoreProperties`（字段 + `toHiveConfOverrides`/`toDlfCatalogConf` conf + 新 `validateConnection()`〔§4 共享连接规则〕）；保留框架（`MetaStoreProvider(s)`/`AbstractMetaStoreProperties`/`MetaStoreParseUtils`/`JdbcDriverSupport`）+ `MetaStoreParseUtilsTest`。
- **`validate()` 拆分（字节不变）**：`PaimonHms.validate()` = `requireWarehouse()` + `validateConnection()`；`PaimonDlf.validate()` = `requireWarehouse()` + `validateConnection()` + `requireOssStorage()`（**paimon-only，留连接器子类**，非基类）；Rest/Jdbc/Fs 整体搬（validate 不变）。触发序逐字保留（warehouse→…）。
- **paimon 连接器 pom**：`fe-connector-metastore-spi` 依赖 → `fe-connector-metastore-paimon`（-spi/-api 传递）；**plugin-zip.xml 无需改**（blanket dependencySet 自动收）。`unzip -l` 实证：lib/ 含 `fe-connector-metastore-{paimon,spi,api}-*.jar`；paimon jar 内 META-INF/services=5 paimon FQN + 10 类；spi jar **无** META-INF/services、带 2 基类、**无** 旧 impl 子包。
- **对抗字节复核**：3 wholesale impl（Rest/Jdbc/Fs）+ 5 provider normalized-diff vs 原始 = **仅 javadoc/注释措辞差异**（"Paimon X" vs "X"），可执行代码零变更；Hms/Dlf split 的行为 parity 由 16+7 测试（精确报错/序/conf）守。
- **关键架构事实（纠正过时 pom 注释）**：fe-core **不依赖** metastore 模块（旧 `-spi` pom 注释「compiled into fe-core」是**错的**，从 fe-filesystem-spi 误抄）；metastore jar 是 **child-first bundle 进连接器 plugin-zip**（blanket dependencySet）。ServiceLoader 用 `MetaStoreProvider.class.getClassLoader()`→plugin 内所有 metastore jar 同一 child loader，拆模块后发现照常。
- **Phase A↔B 的过渡态**：iceberg 连接器仍依赖 `-spi`（含基类+框架，**无 paimon provider**）。iceberg 的 `bindForType("hms"/"dlf")` 仅 live createCatalog 触发（非 UT、iceberg 未翻闸），故 Phase A 不破 iceberg UT；**Phase B 建 `-iceberg` 模块补 iceberg provider 后恢复**。
- **本地构建坑（非缺陷）**：非-clean 构建会留 `-spi/target/classes/META-INF/services` 旧 FQN → ServiceLoader 报 `Provider …spi.hms.HmsMetaStoreProvider not found`；**必 `clean`**（CI 本就 clean，无碍）。

## ✅ T10 Phase B = DONE（2026-06-22，本 HANDOFF commit，未 push）— iceberg per-flavor 校验特性

> 设计文档 §10 TODO 7-13。**B-gate 全绿**：metastore-iceberg UT **36/0/0**、iceberg 连接器 UT **111/0/0**（1 skip=env-gated `IcebergLiveConnectivityTest`）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`。**对抗 parity 复核（5-agent workflow `wf_a3aaa55a-79e`，逐 flavor vs legacy fe-core + §4）= 4 MATCH + 1 nit-deviation（REST locale，见下）。**

- **新模块 `fe-connector-metastore-iceberg`**（reactor 入 `fe-connector/pom.xml`，紧跟 `-paimon`）：包 `org.apache.doris.connector.metastore.iceberg.{hms,dlf,rest,jdbc,glue,noop}`。7 flavor + 7 provider + `META-INF/services`。镜像 `-paimon`。
- **flavor 实现**：`IcebergHms/Dlf` extend 共享基类 `Abstract{Hms,Dlf}MetaStoreProperties`，`validate()`=`validateConnection()`（**仅连接，无 warehouse/OSS**——与 paimon 的关键区别）；`IcebergRest/Jdbc/Glue` 逐字 §4（REST 10 条用 `ParamRules`〔fe-foundation,连接器可用〕near-verbatim port `IcebergRestProperties.buildRules` + 内联 `Security`/`AwsCredentialsProviderMode` 枚举检查；Glue 4 条 port `AWSGlueMetaStoreBaseProperties`；JDBC 3 条 uri/catalog_name/warehouse）；`IcebergNoOp`（hadoop/s3tables 共享，`validate()`=no-op，storage 上游校验）。**flavor token：rest/hms/glue/dlf/jdbc/hadoop/s3tables**（无 filesystem，hadoop=文件系统式；`IcebergExternalCatalog` 常量实证）。
- **连接器接线**：iceberg 连接器 pom 依赖 `-spi`→`-iceberg`（透传 `-spi`+`-api`）；`IcebergConnectorProvider.validateProperties` → `bindForType(resolveFlavor(props), props, {}).validate()`（`resolveFlavor` 缺失返回 null → `bindForType(null)` 抛，iceberg classpath 无 provider 认 null）。plugin-zip **无需改**（blanket dependencySet），`unzip -l` 实证含 metastore-iceberg、**不含** metastore-paimon。`bindForType("hms"/"dlf")` conf 经共享基类 == T05/T07（57 `IcebergCatalogFactoryTest` 全绿守）。
- **测试**：metastore-iceberg per-flavor（REST 12〔10 规则+2 fire-order〕、Glue 6、JDBC 4、HMS 5、DLF 3、dispatch 7）+ iceberg 连接器 `IcebergConnectorValidatePropertiesTest` 7（provider 入口 accept/reject）+ env-gated `IcebergLiveConnectivityTest`（镜像 Paimon，gate `ICEBERG_REST_URI`）。无 Mockito，WHY+MUTATION 注释。
- **对抗复核结论**：REST/Glue/JDBC/HMS/DLF 规则集/报错串/fire-order/guard 全核对 legacy。唯一 deviation=**REST `validateCredentialsProviderMode` 用 `Locale.ROOT` vs legacy 默认 locale `toUpperCase()`**（nit；ASCII 模式名字节相同；仅 Turkish-i 等非 ASCII locale 下 ROOT 更正确〔legacy 会误拒 `web-identity`〕；真实 ASCII 输入不可达）——**有意保留 ROOT**（更正确+checkstyle 安全），已在代码注释 + 此处记录。其余 4 flavor MATCH（nit 均 out-of-scope/inert：Glue 的 `S3Properties.of`〔s3.external_id〕=storage 上游；JDBC `isNullOrEmpty` vs `isBlank`=绑定已 trim 故 inert；HMS authType 默认 `""` vs `"none"`=对 3 规则零影响；DLF endpoint 抛 `IllegalArgumentException` vs legacy `StoragePropertiesException`=消息字节同、均 RuntimeException 同样 wrap、无代码分支 subtype；DLF proxyMode 字面量 vs 常量=值相同且 conf-side）。
- **deviation（UT 不可见，P6.6 docker 验）**：与 paimon 同族——HMS/DLF 的 conf/auth 包裹在连接器侧 T05/T07 已接（本 Phase 只加 validate）；iceberg 校验仅 simple-auth 直通可在 UT 见，Kerberized 须翻闸后 e2e。

## 🔴 关键认知（写下来免下次重踩）

- **T04 残留风险（UT/打包不可见，仅 P6.6 docker plugin-zip e2e 真闸可验）**：
  1. `hive-catalog-shade` **内含** iceberg 1.10.1 与连接器直接 `iceberg-core` 在 child-first loader 共存——版本相同→预期 byte-identical benign（fe-connector-hive 同款已上线），但**首次** direct-iceberg + shade 组合，未 live 验。
  2. **glue 显式-AK 凭据 provider 类 `com.amazonaws.glue.catalog.credentials.*` 来源未定**（不在 hive-catalog-shade / iceberg-aws；fe-core `aws-java-sdk-glue` v1 疑源未证）→ **T05 glue flavor wiring 时核**（不挡 T04 闭包）。
  3. `apache-client` 经 awssdk 传递 runtime 入闭包（paimon 同款故意 ship，无害）。
- **silent 读路径 bug（骨架，翻闸后才在 regression 暴露）—— T08 修 #1#1b#2；T09 修 #3#4 + 命名空间 split + auth 包裹**：
  3. ✅ **format-version（T09 DONE）**：已改读 table 真元数据（`getFormatVersion`），删 `spec().specId()>=0?2:1`。pin 测试已翻成 read-from-property。
  4. ✅ **column 构造 + listing（T09 DONE）**：nullable 恒 true、isKey 恒 true、小写化（`Locale.ROOT`）、`WITH_TIMEZONE` marker、nested-namespace 递归 + view 过滤、命名空间 dotted-split + external_catalog.name 全部落地。详见上「✅ T09 = DONE」。
- **Q2=B metastore-spi mini-recon + 改造 = ✅ DONE（part1 `562201deb9f`）**：新增 `MetaStoreProvider.supportsType(String)` + `MetaStoreProviders.bindForType(flavor,props,storageConf)`（5 provider 全转，paimon `supports(Map)` 字节不变）。HMS/DLF conf 复用 `HmsMetaStorePropertiesImpl.toHiveConfOverrides` + `DlfMetaStorePropertiesImpl.toDlfCatalogConf`（SDK-free）。**iceberg 只用 `toHiveConfOverrides()`/`toDlfCatalogConf()`，不调 paimon `validate()`**（paimon-ism: requireWarehouse for all flavors；iceberg HMS 不要求 warehouse）。无 glue/s3tables provider（glue/s3tables 在连接器 `IcebergCatalogFactory` 内直接装配，不走 metastore-spi）。
- **跨切面风险（带入 T05–T07 + P6.6 翻闸门）**：R-004 AWS-SDK `ExecutionAttribute` static 撞（已用 child-first 自包含 awssdk 闭包缓解，待 docker 验）；DLF `ProxyMetaStoreClient` 按类名反射加载须入 plugin-zip 闭包（hive-catalog-shade 已带，T07 决定 vendored-source vs shade-bundled）；hive-catalog-shade relocated thrift vs host（已 exclude fe-thrift/libthrift）；**field-id 丢失**（`ConnectorColumn` 无载体，P6.2+ scan 前须重引，否则同 paimon BE SIGSEGV/DCHECK 类 bug）。**这些 UT 不可见，仅 P6.6 docker plugin-zip e2e 可验**。

## 🟢 下一步（精确）—— **P6.1 ✅ DONE（T01–T10 全绿），当前 = P6.2（scan + MVCC + cache + vended）**

> **⚠️ 不是 P6.6！** P6 是 8 阶段串行（P6.1→P6.2→P6.3→P6.4→P6.5→P6.6 翻闸→P6.7 删 legacy→P6.8 回归，见 `P6-iceberg-migration.md:113`）。刚完成的 T10 是 **P6.1 的最后一个 task**。翻闸（P6.6）「全有或全无」，**须等 P6.2–P6.5（scan/write/procedure/sys-table）全部实现完**——现在翻闸会让 iceberg 全查询走只有读元数据+校验的连接器，scan/write 立刻全断。
> **起步先**：按 AGENT-PLAYBOOK §7.1/§7.2 做 P6.2 的 code-grounded recon + 逐 task 拆解（产出 P6.2 自己的 task 行）；re-read `P6-iceberg-migration.md` 的 P6.2 块（§阶段拆分行 + §old→new 映射 scan/cache/MVCC/vended 行 + §阶段内前置 #1 + §验收门 P6.2 + 开放决策 O2）。

- ✅ **P6.1 = DONE**：T01–T03（结构倒置 + 测试基建，`ae54a2174ff`）、T04（7-flavor pom 闭包，`1fd4d42c297`）、T05（5-flavor 装配 + DriverShim，`b0dbe91e1a5`）、T06（s3tables bespoke，`386e183c091`）、T07（DLF 子树 port，`16926486e8c`）、T08（type-mapping 读 parity，`d41fa4faf3e`）、T09（读路径列/format-version/listing/auth parity，`2caf9edc983`）、**T10（= metastore 模块拆分 Phase A `f67195fee64` + iceberg per-flavor 校验 Phase B `6cc4de3078f`）**。验收门（line 89）：7 flavor 装配 + ConnectorMetadata 读 parity + per-flavor CREATE 校验 全绿。
- **P6.2（当前任务）scope**（`P6-iceberg-migration.md` P6.2 块）：
  1. **scan 路径**：`source/`（7 文件，`IcebergScanNode` 1228）→ `IcebergScanPlanProvider` + `IcebergScanRange` + 通用 `PluginDrivenScanNode`（SPI 点 E3，已就绪）。
  2. **MVCC**：`IcebergMvccSnapshot` + `IcebergSnapshot` → `ConnectorMvccSnapshot` + 通用 `PluginDrivenMvccExternalTable`（E5/D-042 源无关，已就绪）。
  3. **cache**：`IcebergExternalMetaCache`(289) + `cache/`(2) + `IcebergSnapshotCacheValue` → 连接器内 cache（决策 D6，无 SPI）。
  4. **vended**：`IcebergVendedCredentialsProvider` → **新 `ConnectorCredentials` SPI（E6）**——**P6.2 起前在 `fe-connector-api` 新建**（§阶段内前置 #1）；核对 iceberg REST/DLF vended 与 paimon `isVendedCredentialsEnabled` 网关能否合面（开放决策 O2，影响 metastore 子线）。
  - **最高危**：**field-id 丢失**——`ConnectorColumn` 无 field-id 载体（T09 记录），scan 前必须重引，否则同 paimon BE SIGSEGV/DCHECK 类崩溃（见「🔴 关键认知」末条）。
  - **验收门（line 90）**：scan parity（谓词下推 / 分区裁剪行数 / native·JNI / position+equality delete / SELECT* 无谓词）vs `IcebergScanNode`；MVCC time-travel（AS OF / VERSION）；vended REST/DLF round-trip。**仍不碰 `SPI_READY_TYPES`，零行为变更。**
- **此后**：P6.3 写路径（先写 `06-iceberg-write-path-rfc.md` 过 PMC）→ P6.4 procedures（新 `ConnectorProcedureOps` E2）→ P6.5 sys-table + 元数据列 → **P6.6 才翻闸**（加 `SPI_READY_TYPES` + GSON compat + SHOW-CREATE 渲染：合成键 `iceberg.format-version`/`iceberg.partition-spec` 现被 `Env.java:4936` paimon-gate 挡住，翻闸渲染时才决定保留/剥离）→ P6.7 删 legacy → P6.8 docker 回归（届时才首验 T04–T10 全部 UT-不可见 deviation）。

> **历史风险（已缓解，docker 待 P6.6 验）**：R-A1 paimon 打包（Phase A 已搬完，A-gate plugin-zip 实证绿）；R-A2/R-A3 conf 基类抽取/iceberg 重连（既有 paimon/iceberg conf 测试守，全绿）。

---

# 📦 仓库 / 进度状态

- **工作分支 = `catalog-spi-10-iceberg`**（P6.1 起步前 HEAD = `e5959e1b53d` #64655 P3b）。branch off `branch-catalog-spi`，PR 时 squash 合并。**所有 commit 均未 push。**
  - **已 commit（旧 session）**：T01-T03 `ae54a2174ff`；T08 `d41fa4faf3e`（type-mapping read parity）；T04 `1fd4d42c297`（pom 7-flavor 闭包 + plugin-zip + fe/pom dM）。
  - **已 commit（T05 全套）**：`562201deb9f`（part1：metastore-spi `bindForType`/`supportsType` + 设计文档）；`6f7b292b5c6`（part2：factory `buildBaseCatalogProperties`/`appendS3FileIOProperties`/`chooseS3Compatible`）；`b0dbe91e1a5`（**part3：5-flavor appender + connector LIVE wiring + DriverShim + 52 parity 测试；UT 75/0/0、checkstyle 0、import-gate 净**）。
  - **已 commit（T06）**：`386e183c091`（**s3tables bespoke 3-arg：`buildS3TablesCatalogProperties` + EXPLICIT-wins `appendS3TablesFileIOProperties` + connector `createS3TablesCatalog`/`buildS3TablesClient`/`buildAwsCredentialsProvider` + `Callable<Catalog>` 泛化；UT 81/0/0、checkstyle 0、import-gate 净**）。
  - **已 commit（T07）**：`16926486e8c`（**DLF 子树 port 进连接器 `dlf/`〔5 文件，4 逐字 + DLFCatalog 断 3 fe-core import〕 + `IcebergCatalogFactory.buildDlfConfiguration` + connector `createDlfCatalog` 早分支；UT 85/0/0**）。
  - **已 commit（T09）**：`2caf9edc983`——读路径 parity：`IcebergConnectorMetadata`（列构造 + format-version + 5 读 auth 包裹）、`IcebergCatalogOps`（nested-namespace 递归 + view 过滤 + Guava 字节级 split）、`IcebergConnector.getMetadata`（线程 config + context）、`IcebergConnectorProperties`（+`REST_VIEW_ENABLED`/`EXTERNAL_CATALOG_NAME`）+ 5 新测试件 + 设计文档。UT 103/0/0。
  - **已 commit（T10 设计）**：`2043d1f07c2`——`P6-T10-iceberg-validation-design.md` **v2**（metastore 模块拆分 filesystem 式 + iceberg per-flavor 校验下沉；含 §4 逐字规则、§5 目标结构、§8 风险、§10 时序 TODO）。recon workflow `wf_8ae4353f-9a8`。
  - **已 commit（T10 Phase A）**：`f67195fee64`——metastore 模块拆分（行为不变）：新模块 `fe-connector-metastore-paimon`（5 impl + 5 provider + META-INF）、`-spi` 抽 `Abstract{Hms,Dlf}MetaStoreProperties` + 瘦身、reactor + paimon 连接器 pom 重连。A-gate 全绿（metastore UT 4+43、paimon 连接器 318、iceberg 103、plugin-zip 实证、checkstyle 0、import-gate 净）。详见上「✅ T10 Phase A = DONE」。
  - **已 commit（T10 Phase B = 本 HANDOFF commit）**：iceberg per-flavor 校验：新模块 `fe-connector-metastore-iceberg`（7 flavor + 7 provider + META-INF；hms/dlf extend 共享基类、rest/glue/jdbc §4 逐字、hadoop/s3tables no-op）、iceberg 连接器 pom 重连 `-spi`→`-iceberg` + `validateProperties` 接线 + env-gated live test。B-gate 全绿（metastore-iceberg UT 36、iceberg 连接器 UT 111〔1 skip〕、plugin-zip 隔离实证、checkstyle 0、import-gate 净、iceberg 仍不在 SPI_READY_TYPES）；对抗 parity 复核 4 MATCH + 1 nit。详见上「✅ T10 Phase B = DONE」。
  - **metastore 子线 = 已彻底 CLOSED**（8 文档加 CLOSED banner；后续勿读，见顶部范围注）。
- **stale cruft = 本 session 已清理**：删除 `fe-connector-{iceberg,paimon}-{api,backend-*}` 共 12 个目录（仅含 gitignored 生成物 `.flattened-pom.xml`，0 tracked、不在 reactor = 本地 `phase3-module-split` 旧实验遗留；untracked 故 git 无变更）。当前线用单 `fe-connector-iceberg` + flavor switch。
- **P0–P5 + P3 hybrid + P4 + P3b 全部已合入**（#63582/#63641/#64096/#64143/#64253/#64300/#64446/#64653/#64655）。iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- ⚠️ `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ `*.bak` + scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）**严禁 `git add -A`**，commit 前 path-whitelist。

## 🗺️ 代码脚手架（iceberg）

- **连接器（终态归宿）**：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`（现：`IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + **`dlf/`子树〔T07：`DLFCatalog`/`HiveCompatibleCatalog`/`DLFTableOperations`/`client/{DLFClientPool,DLFCachedClientPool}`〕**）。**pom 闭包（T04）已就绪**：7-flavor SDK + hive-catalog-shade + metastore-spi。
- **paimon 模板**（P6.1 镜像）：`fe/fe-connector/fe-connector-paimon/`（`PaimonCatalogFactory`/`PaimonCatalogOps` seam/`createCatalogFromContext` CL-pin+executeAuthenticated/测试 `RecordingPaimonCatalogOps`/`FakePaimonTable`）+ `fe-connector-paimon-hive-shade`（paimon 专建 thrift shade，**iceberg 不复用，改用 hive-catalog-shade**）。
- **legacy 对照（P6.1 读路径）**：fe-core `datasource/iceberg/`（`IcebergMetadataOps`1362 读半 / `IcebergExternalTable`535 读 / `IcebergUtils`1826 schema-type / `DorisTypeToIcebergType`134 / 7 flavor catalog + `HiveCompatibleCatalog`181 + `dlf/`4）+ `datasource/property/metastore/`（`AbstractIcebergProperties`285 + 7 flavor + factory，**STILL-CONSUMED 留 fe-core 至翻闸后**；T05/T07 移植 per-flavor 装配的源）。
- **metastore-spi（Q2=B 已扩 part1）**：`fe/fe-connector/fe-connector-metastore-spi/`（`MetaStoreProviders.bind`(paimon)+**新 `bindForType(flavor,…)`(iceberg)** + `MetaStoreProvider.supportsType` + `HmsMetaStorePropertiesImpl.toHiveConfOverrides`/`DlfMetaStorePropertiesImpl.toDlfCatalogConf`）。无 glue/s3tables provider（这两者在连接器内直接装配）。**T04 已加为 iceberg 连接器依赖。**

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`。**漏 `-am`→`${revision}` 假错 / dependency:tree 找不到 reactor sibling**。**checkstyle 在 `validate` phase（编译前）跑**。连接器模块 art = `fe-connector-iceberg`。
- **plugin-zip 实查**（T04 起验闭包真相）：`mvn -pl :fe-connector-iceberg -am package -DskipTests` → `unzip -l target/doris-fe-connector-iceberg.zip | grep lib/`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- 测试无 Mockito（fail-loud fake）；live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。

## ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`）。
- message `[refactor](catalog) P6.1 iceberg: <subj>`（mirror #64653/#64655）+ 根因/解法/测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` +（本工作分支约定）`Claude-Session: …`（最终 squash 入上游时会被剥离）。
- PR base = `branch-catalog-spi`，squash 合并。历史 `catalog-spi-07-paimon` force-push 流程**已作废**。

## 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（参 P5-T29 教训；metastore-props 是 STILL-CONSUMED）。
- **HANDOFF 的依赖名可能过时**——T04 实证「iceberg-hive-metastore」「aliyun DLF SDK」均不存在（在 hive-catalog-shade 内）。下次动 pom/依赖前**先 recon（grep repo + unzip 实证）再信 HANDOFF**。
- **Q2=B（用户主动选）已落地 part1** —— `bindForType`/`supportsType` 已加（`562201deb9f`）；后续 flavor（T07 DLF）直接调 `bindForType("dlf",…)`，勿再重做 metastore-spi recon。
- **REST signing-cred 的 PROVIDER_CHAIN 非-DEFAULT gap**（见「🟡 T05 剩余」REST 条）：实现 REST 时若要补该 niche 路，需新增一个 `ConnectorContext` seam 让 fe-core 解析 `AwsCredentialsProviderFactory.getV2ClassName`（连接器不能 import）；DEFAULT mode 无缺口，可先 NOTE 跳过。
- **大文件（`IcebergUtils`1826 等）用 subagent 总结**（playbook §3.1），勿主线整读。
