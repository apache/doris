# fe-core 数据源依赖与残留代码清理分析

日期：2026-07-21 · 分支：`catalog-spi-review-17` · 范围：`fe/fe-core`

> 目标（用户提出的两个问题）：
> 1. `fe/fe-core/pom.xml` 里还挂着的 iceberg / hudi 相关依赖能否剔除？（所有数据源依赖理应已迁到 `fe/fe-connector`。）
> 2. `fe/fe-core` 代码里是否还残留 iceberg / paimon / hive / hudi / maxcompute / trino / es 这些数据源特有的类或逻辑？
>
> 方法：多 agent 对抗式审计（28 个 agent：逐依赖分析→独立复核找漏网消费者；逐数据源源码扫描→复核违规项；完备性 critic 兜底），关键结论已由本人用直接 grep 二次核验。所有结论均带 `file:line` 证据。

---

## 0. 结论速览（TL;DR）

**依赖层面**：`fe-core` 主源码对数据源库的编译期依赖**几乎清零**——唯一残留是死代码 `StatisticsUtil.getIcebergColumnStats`。但 **不能立即删** iceberg 依赖，因为还有 **5 个 iceberg 相关测试类**（连接器 metastore/property 解析测试）留在 fe-core 里，把 `iceberg-core`/`iceberg-aws` 钉在编译类路径上。

**可直接删（低风险）**：`lakesoul-io-java`、`scala-library`（均 provided、废弃 lakesoul、零引用）。

**删除需先动代码**：整个 iceberg-AWS 依赖簇（`iceberg-core` / `iceberg-aws` / `glue` / `s3tables` / `s3-tables-catalog-for-iceberg` / `aws-json-protocol`）应作为**一个批次**移除，前置条件是把 5 个 iceberg 测试类迁到连接器模块 + 删死方法。

**注释纠错（保留但注释是错的）**：`kryo-shaded` 注释写"for hudi catalog"，实际消费者是 `WorkloadSchedPolicy`；`avro`/`parquet-avro` 注释写"For Iceberg"，实际是 fe-core 的 parquet reader 在用。

**必须保留（S3 文件系统/凭证/UDF 需要，非数据源用途）**：`s3-transfer-manager`、`sts`、`url-connection-client`、`protocol-core`、`sdk-core`、`kryo-shaded`、`hive-exec(runtime)`、`commons-lang(runtime)`。

**代码层面**：fe-core 里仍有相当量的数据源特有代码，分两类——
- **真·死代码**（可直接删）：iceberg 的 `StatisticsUtil.getIcebergColumnStats`、`UnboundIcebergTableSink` 及其分支、`IcebergInsertCommandContext`；hive 的 `HiveInsertCommandContext`。
- **仍 LIVE 的数据源特有逻辑**（需迁移设计，不能简单删）：**iceberg 行级 DML 一整块（~15 个文件）**、hudi 的 `hudi_meta` TVF（3 文件）、legacy `engine=hive` broker-load 簇、`ranger-hive` 授权包、es 废弃内置引擎兼容桩、以及若干 `CreateTableInfo`/`Env`/`Coordinator` 里的按源硬编码分支。
- **trino**：已完全迁移干净，fe-core 里 0 个 io.trino 引用、0 个 trino-connector 目录类。

---

## Part A — pom.xml 依赖可移除性分析

### A.1 判定口径

- **REMOVE**：fe-core 无编译引用、无运行时/反射/ServiceLoader 需要，且没有任何"保留项"（S3 文件系统 / 凭证签发 / parquet / UDF 引擎）传递依赖它。可直接删 `<dependency>`。
- **KEEP**：fe-core 因**非数据源**原因真需要它，或某个保留项需要它。
- **REMOVE_AFTER_CODE_CHANGE**：只有一小块死代码 / 放错位置的代码把它吊着，先动那块代码即可转 REMOVE。

一条铁律：某依赖注释写"给 iceberg 用"**并不**意味着可删——必须确认 `hadoop-aws` / S3 文件系统 / 凭证代码（都保留）没有一并拉它（双用途）。

### A.2 iceberg / AWS-iceberg 依赖簇 —— 作为**一个批次**移除

| artifact | pom 行 | scope | 判定 | 依据 |
|---|---|---|---|---|
| `org.apache.iceberg:iceberg-core` | 576–580 | compile | **REMOVE_AFTER_CODE_CHANGE** | 主源码唯一消费者 `StatisticsUtil.getIcebergColumnStats`（死代码，全 `fe/` 零调用）用的是 iceberg-**api** 类；真正把 iceberg-**core** 钉住的是 **5 个测试类**（见下）。删死方法 + 迁走测试后可删；在 iceberg-aws 未删前它仍作为其 runtime 传递依赖存在。 |
| `org.apache.iceberg:iceberg-aws` | 581–585 | compile | **REMOVE_AFTER_CODE_CHANGE** | 唯一消费者是 2 个 `@Disabled` 测试 `AWSTest.java`、`IcebergGlueRestCatalogTest.java`（直接 `new GlueCatalog()` / `CatalogUtil.buildIcebergCatalog`）；主源码 0 引用。连接器 `fe-connector-iceberg/pom.xml:149` 自带 iceberg-aws，不依赖 fe-core 供给。 |
| `software.amazon.awssdk:glue` | 587–596 | compile | **REMOVE** | fe-core 主+测源码 0 个 `services.glue.*` 引用；在 JDK17 上做过**实证编译**：移除 glue jar 后 mirror `AWSTest.testGlueCatalog()` 仍编译通过（arity 不匹配的重载 + private 字段不触发 javac 符号解析）。生产 Glue 走插件（`GsonUtils.java:393` 的 `IcebergGlueExternalCatalog` 别名）。`hadoop-aws`/`s3-transfer-manager`/`iceberg-aws` 均不拉 glue。 |
| `software.amazon.awssdk:s3tables` | 765–768 | compile | **REMOVE** | 冗余：BOM（2.29.52）+ `s3-tables-catalog-for-iceberg` 已在 runtime scope 传递供给；fe-core 0 编译引用（`S3TablesTest` import 的是 `software.amazon.s3tables.*` 而非 `awssdk.services.s3tables`）。S3 文件系统用 `awssdk:s3`，不碰 s3tables。可**立即**删。 |
| `software.amazon.s3tables:s3-tables-catalog-for-iceberg` | 769–773 | compile | **REMOVE_AFTER_CODE_CHANGE** | 唯一编译消费者是 `datasource/s3tables/S3TablesTest.java:27`（`import software.amazon.s3tables.iceberg.S3TablesCatalog`）。构建 `-DskipTests` 仍会**编译**测试→不先删该测试则 `cannot find symbol`。下游 `fe-connector-iceberg`、`be-java-extensions` 各自声明，不依赖 fe-core。 |
| `software.amazon.awssdk:aws-json-protocol` | 619–622 | compile | **REMOVE（随本批一起）** | fe-core 0 引用。json 协议的唯一消费者是数据源客户端 glue / s3tables / iceberg-aws（各自 pom 已 compile 声明同版本 2.29.52）。保留项里 `sts`=query 协议、`s3`=xml 协议、`hadoop-aws`=0；故它应与 iceberg 簇**一起**离开，不要单独删。 |

> **关键纠错（完备性 critic 发现，已本人二次核验）**：把 iceberg 依赖钉在编译类路径上的**不是主源码**，而是 5 个仍留在 fe-core 的测试类——它们本质是连接器 metastore/property 解析测试，应随迁移搬到 `fe-connector-iceberg`：
> - `src/test/java/org/apache/doris/datasource/property/metastore/IcebergUnityCatalogRestCatalogTest.java`（iceberg-core + `rest.*`）
> - `.../metastore/IcebergGlueRestCatalogTest.java`（iceberg-core + **iceberg-aws** `AwsClientProperties`/`S3FileIOProperties`）
> - `.../metastore/AWSTest.java`（**iceberg-aws** `glue.GlueCatalog`）
> - `.../metastore/IcebergDlfRestCatalogTest.java`（iceberg-core）
> - `.../datasource/s3tables/S3TablesTest.java`（iceberg-core + s3-tables-catalog）
>
> 因此 iceberg 依赖不是"主源码删干净了就能删"，而是**门槛在这 5 个测试类的归属**。

### A.3 AWS SDK 依赖簇 —— 双用途，**保留**

这些是 AWS SDK v2 的共享底座，被**保留**的 S3 对象存储文件系统 + STS AssumeRole 凭证签发所需，与 iceberg 无关：

| artifact | 判定 | 保留原因（非数据源消费者） |
|---|---|---|
| `software.amazon.awssdk:s3-transfer-manager` | **KEEP** | `hadoop-aws 3.4.2` 的 `S3AFileSystem` 硬引用 `transfer.s3.S3TransferManager`（jar 内 78 处 `transfer/*`）；父 pom 排除了 hadoop-aws 的 `awssdk:bundle`，此独立 jar 是唯一供给源。 |
| `software.amazon.awssdk:sts` | **KEEP** | `S3Properties.java:44-45`、`S3Util.java:51-52` 直接 import `StsClient` + `StsAssumeRoleCredentialsProvider`（对象存储凭证路径）。 |
| `software.amazon.awssdk:url-connection-client` | **KEEP** | `S3Util.java:47` 编译引用 + `DorisFE.java:241-242` 启动时 `System.setProperty("...http.service.impl", UrlConnectionSdkHttpService)` 解决"Multiple HTTP implementations"，对**所有** AWS SDK v2 客户端（含保留的 S3/STS）生效。 |
| `software.amazon.awssdk:protocol-core` | **KEEP** | `sts` / `s3` / `s3-transfer-manager` 的 2.29.52 pom 都直接依赖它（共享 marshaller 基座）。 |
| `software.amazon.awssdk:sdk-core` | **KEEP** | `S3Util.java:43-46` 引用 `ClientOverrideConfiguration`/`RetryPolicy` 等；且 fe-core **vendored** 了 `software/amazon/awssdk/core/client/builder/SdkDefaultClientBuilder.java`（引 ~30 个 sdk-core 类型）。 |

### A.4 avro / parquet-avro —— 注释误导，实为 parquet reader 所用

| artifact | pom 行 | 判定 | 依据 |
|---|---|---|---|
| `org.apache.parquet:parquet-avro` | 633–636 | **REMOVE_AFTER_CODE_CHANGE** | fe-core 主+测 0 个 `org.apache.parquet.avro.*` 引用。真正的 parquet 消费者是 LIVE 非数据源代码 `common/parquet/ParquetReader.java`（`BrokerInputFile`/`LocalInputFile`），被 `httpv2/restv2/ImportAction.java:160`（HTTP 导入抽样）调用，只用 `org.apache.parquet.{io,column,hadoop,schema}`。**改法**：把 `parquet-avro` 换成 `parquet-hadoop`（+`parquet-column` 兜底）——`parquet-hadoop` 不依赖 avro，保留 ParquetReader 所需全部类，丢掉 avro 桥。 |
| `org.apache.avro:avro` | 627–631 | **REMOVE_AFTER_CODE_CHANGE（且与 iceberg 批耦合）** | fe-core 0 个 `org.apache.avro.*` 引用（`AvroFileFormatProperties*` 是纯属性解析，不 import avro）。但删除受两处传递依赖约束：① `iceberg-core`（compile）的 pom 声明 avro，故须**排在 iceberg 移除之后**才能真正离开编译类路径；② 保留的 UDF 引擎 `hive-exec:core`（runtime）也声明 avro→avro 会**永远**留在 runtime 类路径。净效果：删掉这条"给 iceberg"的显式声明是安全的（版本由 dependencyManagement 兜底），但要与 parquet-avro 的替换 + iceberg 移除**一起排期**，且并不会把 avro 真正逐出 `fe/lib`。 |

### A.5 kryo-shaded —— 保留，但注释过期需纠正

| artifact | pom 行 | 判定 | 依据 |
|---|---|---|---|
| `com.esotericsoftware:kryo-shaded` | 675–679 | **KEEP（改注释）** | 注释 `<!-- for hudi catalog -->` **已过期**。fe-core 唯一消费者是 `resource/workloadschedpolicy/WorkloadSchedPolicy.java:32` 的 `import com.esotericsoftware.minlog.Log`（`gsonPostProcess()` 里 `Log.error(...)`，live 非数据源）。`minlog` 由 `kryo-shaded-4.0.2` compile 传递、且是 `Log.class` 的唯一持有者；无其它编译路径供给。删它会破 `WorkloadSchedPolicy` 编译。**建议**：注释改为指向真实消费者（workload scheduling），别当"hudi 清理"删掉。 |

### A.6 完备性 critic 发现的**额外**依赖（不在原 iceberg/hudi 清单内）

| artifact | pom 行 | scope | 判定 | 依据（已本人核验） |
|---|---|---|---|---|
| `com.dmetasoul:lakesoul-io-java` | 498 | provided | **REMOVE** | lakesoul 已废弃（`CatalogFactory.java:143` 抛"Lakesoul catalog is no longer supported"）。fe-core 0 个 `com.dmetasoul`/lakesoul 类引用，全是 Gson 兼容重映射字符串。 |
| `org.scala-lang:scala-library` | 570 | provided | **REMOVE** | fe-core 0 个 `import scala.`（lakesoul 陪跑品）。 |
| `org.postgresql:postgresql` | 564 | provided | **REMOVE（先查测试）** | lakesoul 陪跑品，但 `src/test/.../catalog/JdbcResourceTest.java` import 了 `org.postgresql`，provided 在测试类路径上→删前需处理该测试。 |
| `com.baidubce:bce-java-sdk` | 556 | compile | **REMOVE（Batch 4 已删）** | 全 `fe/**/src` 零引用（主+测）、全仓库零 `com.baidubce`/反射/config 加载。BOS 走 S3 兼容路径（`cloud/storage/ObjectInfoAdapter.java` `case BOS`→`S3Properties`；`S3Properties.PROVIDERS` 含 BOS；`SchemaTypeMapper` BOS 已注释掉），原生 SDK 不在 BOS 路径上；fe-filesystem 亦不声明→非"迁移"而是"删"。**删除坑**：bce 是 fe-core 唯一传递带来 `javax.validation:validation-api` 的源，`ExternalMetaIdMgr` 一处装饰性 `@NotNull` 靠它编译（下一行 `Preconditions.checkNotNull(log)` 才是真校验，全 fe 唯一一处 javax.validation 用法）→删该注解而非加依赖（守 fe-core 只减不增）。连带清理孤立的 mqtt（仅 bce 传递）+ validation-api 依赖管理块/属性。 |
| `com.amazonaws:aws-java-sdk-dynamodb` | 371–374 | compile | **REMOVE（Batch 4 已删）** | `dependency:tree` 证其为 fe-core **直接叶子**（零传递消费者）；hadoop-aws 3.4.2 已随 Hadoop 3.4.0 移除 S3Guard（jar 内零 dynamodb 类，仅剩 `S3Guard.class` 一句"不再需要"的警告字符串）；ranger 审计无 DynamoDB destination；全仓库零 `dynamodbv2` 引用/反射/config。（be-java-ext 的 dynamodb 是 v2 `software.amazon.awssdk`，无关。） |
| `com.amazonaws:aws-java-sdk-logs` | 375–378 | compile | **REMOVE（Batch 4 已删）** | 同 dynamodb：直接叶子、零传递消费者。父 pom "only for apache ranger audit" 注释**已过时**——唯一引用 CloudWatch 的 `AmazonCloudWatchAuditDestination` 在 `ranger-plugins-audit`，而 fe-core 只有 `ranger-plugins-common:2.8.0→ranger-audit-core:2.8.0`（仅 File/base destination），`ranger-plugins-audit` 全不在 fe-core 546-jar 类路径上；全仓库零 `amazonaws.services.logs` 引用/config。 |
| `org.apache.kafka:kafka-clients` | 647 | compile | **超范围** | fe main 0 个 `import org.apache.kafka`（routine-load 经 BE thrift，`load/routineload/kafka/*` 是 Doris 类）。非湖仓 catalog，可能独立清理但不在本次数据源计划内。 |

### A.7 看着像数据源、实为内部用途，**保留**

| artifact | 判定 | 真实消费者 |
|---|---|---|
| `org.mariadb.jdbc:mariadb-java-client` | **KEEP（内部）** | `httpv2/util/StatementSubmitter.java:68`（`jdbc:mariadb://127.0.0.1` 打 FE 自己的 MySQL 端口）；JDBC 外表 catalog 是动态加载驱动，不靠这条编译依赖。 |
| `org.apache.ranger:ranger-plugins-common` | **KEEP（双用途）** | 既有 hive ranger（数据源），也有 Doris 内表 ranger 授权 `catalog/authorizer/ranger/doris/RangerDorisPlugin.java`（非数据源）→内表侧吊着它。 |
| `com.zaxxer:HikariCP` | **KEEP（暂）** | JDBC 外表 catalog `datasource/jdbc/client/JdbcClient.java`。JDBC catalog 是数据源，但**不在**本次 7 个迁移目标内，仍住 fe-core。 |
| `com.squareup.okhttp3:okhttp-jvm` | **KEEP** | Doris 联邦 catalog `datasource/doris/RemoteDorisRestClient.java`（同样超范围）+ ~10 个测试 HTTP 基建。 |

---

## Part B — fe-core 残留数据源特有代码扫描

扫描口径：**良性**（注释 / 迁移备注 / 通用工厂/注册表里的字符串类型名 / thrift 枚举名）不计违规；**违规** = 数据源特有类、import 数据源库、按具体源 `instanceof`/`if`/`switch` 跑源特有**逻辑**、按源硬编码行为、死的源特有 helper。

### B.1 iceberg —— 129 文件提及，**22 项违规**（fe-core 残留最多）

分三簇：

**(a) 真·死代码（可直接删）**
- `statistics/util/StatisticsUtil.java:524` `getIcebergColumnStats(...)` + private `getColId` —— 全 `fe/` 零调用；fe-core 主源码**唯一** import `org.apache.iceberg` 的地方。删掉即斩断主源码与 iceberg 库的最后一根编译期联系。
- `nereids/analyzer/UnboundIcebergTableSink.java:39` —— 唯一 `new` 点在自身 `withChildren` 自拷贝里；`UnboundTableSinkCreator` 从不构造它（连接器写路径走 `UnboundConnectorTableSink`）。
- `.../insert/InsertUtils.java:376` 与 `:597` —— 两处 `instanceof UnboundIcebergTableSink` 分支，因上者从不构造而不可达。
- `.../insert/InsertOverwriteTableCommand.java:393` —— `IcebergInsertCommandContext` 的唯一构造点，同样不可达（live 路径是紧随的 `UnboundConnectorTableSink` 臂）。
- `.../insert/IcebergInsertCommandContext.java:28` —— 因上者死亡而传递死亡。

**(b) 仍 LIVE 的 iceberg 行级 DML 一整块（需迁移设计，勿简单删）**
经 capability 门控但**方法体是 iceberg 特有**，由 `RowLevelDmlRegistry.TRANSFORMS` 单条目 `IcebergRowLevelDmlTransform` 经 `find(table)` 从 `DeleteFromCommand`/`UpdateCommand`/`MergeIntoCommand` live 调用：
- `.../commands/IcebergRowLevelDmlTransform.java:62`（`ICEBERG_EXCLUSION` 谓词、硬编码标签 `iceberg_delete`/`iceberg_merge_into`、`assert instanceof PhysicalIcebergDeleteSink/MergeSink`）
- `.../commands/IcebergDeleteCommand.java`、`IcebergMergeCommand.java:398`、`IcebergUpdateCommand.java:132`（position-delete / MERGE / UPDATE-as-merge 计划合成器）
- `.../commands/IcebergMetadataColumn.java`、`IcebergRowId.java`（`$file_path`/`$row_position`/`$partition_spec_id`/`$partition_data` 隐藏列 + `Column.ICEBERG_ROWID_COL`）
- `nereids/trees/plans/logical/LogicalIcebergDeleteSink.java`、`LogicalIcebergMergeSink.java`
- `nereids/trees/plans/physical/PhysicalIcebergDeleteSink.java`、`PhysicalIcebergMergeSink.java:179`（按 `enable_iceberg_merge_partitioning` session var 门控、`DistributionSpecMerge.IcebergPartitionField`）
- `nereids/rules/implementation/LogicalIcebergDeleteSinkToPhysicalIcebergDeleteSink.java`（+ MergeSink 兄弟；注册进 `RuleSet`/`RuleType`）
- `nereids/glue/translator/PhysicalPlanTranslator.java:548`（`visitPhysicalIcebergDeleteSink`/`MergeSink` + `DataPartition.IcebergPartitionField` for `TPartitionType.MERGE_PARTITIONED`）
- `planner/DataPartition.java:158`、`nereids/properties/DistributionSpecMerge.java:35`（iceberg 分区变换描述符，嵌在核心 planner 里）
- `.../commands/ExplainCommand.java:102`（对 `LogicalIcebergDeleteSink`/`MergeSink` 的 `instanceof` 特判）

> ⚠️ 这一簇是 **live 未迁移特性**，不是清理对象。把它请出 fe-core 是一项独立的迁移设计任务（对齐 SPI 行级 DML 委派），不能当死代码删。

**(c) 其它 iceberg 硬编码分支**
- `datasource/property/storage/AzureProperties.java:311` `isIcebergRestCatalog()`（源码自带 TODO 说是临时检查）——通用存储属性类里塞 iceberg-rest 特判。
- `qe/Coordinator.java:2634` `if (isSetHivePartitionUpdates() || isSetIcebergCommitDatas() || isSetMcCommitDatas())` 的按源 if-链（序列化目标 `CommitDataSerializer.feed` 是通用的，但枚举本身按源硬编码；borderline dispatch）。

### B.2 hive —— **6 项违规**（legacy engine=hive 簇 + ranger-hive）

- `.../insert/HiveInsertCommandContext.java:25` —— **死代码**，全仓 0 个 `new`（仅连接器一句 javadoc 提及）；已被通用 `PluginDrivenInsertCommandContext` 取代。可删。
- `catalog/HiveTable.java:41` —— 源特有 `Table` 子类（`TableType.HIVE`，`toThrift()` 发 `THiveTable`）**且** `validate()` 在 fe-core 里解析 hive 属性（`hive.metastore.uris`/`hive.version`/kerberos）——**双违规**（源特有类 + fe-core 属性解析）。仍 live（`BrokerFileGroup`/`NereidsBrokerFileGroup`/`Env` show-create/GsonUtils 子类），是 legacy `engine=hive` broker-load 源的锚点。
- `load/BrokerFileGroup.java:198` + `nereids/load/NereidsBrokerFileGroup.java:215` —— `if (!(srcTable instanceof HiveTable)) throw ...` 按源 instanceof。
- `catalog/Env.java:4480`（与 `:4847` 重复）—— SHOW CREATE TABLE 的 hive 特有 cast + 渲染臂。
- `catalog/HMSResource.java:41` —— 源特有 `Resource` 子类 + 强制 `hive.metastore.uris` 属性解析（legacy `CREATE RESOURCE type=hms`）。
- `catalog/authorizer/ranger/hive/RangerHiveAccessControllerFactory.java:25` —— ServiceLoader 注册（`ranger-hive`），领着 fe-core 里一个 9 文件的 hive 授权包（`HiveObjectType`/`HiveAccessType`/`RangerHiveResource`）。只 import `org.apache.ranger.*`（非 hive 库），属"源特有行为"违规。这是**授权迁移轴**，独立于连接器 scan 轴。

> 注：`case HMS:`/`HMS_EXTERNAL_TABLE` 那些属于**外部 HMS catalog/连接器**轨（多 catalog SPI），与 legacy `engine=hive` Resource/Table 簇正交，不算本项违规。

### B.3 hudi —— **3 项违规**（`hudi_meta` TVF 残面）

- `tablefunction/HudiTableValuedFunction.java:47` —— 源特有类：硬编码 timeline schema、解析 `query_type`→`THudiQueryType`、`getMetadataType()` 返回 `TMetadataType.HUDI`、构建 `THudiMetadataParams`。
- `tablefunction/MetadataGenerator.java:433` `hudiMetadataResult(...)` + `case HUDI:` —— hudi 特有 thrift 解码 + TIMELINE-only 门控 + hudi 报错文案（行数据本身已迁到通用 SPI `pluginTable.getMetadataTableRows("timeline")`，但外壳还在）。
- `nereids/trees/expressions/functions/table/HudiMeta.java:31` —— 源特有 Nereids TVF 表达式类。

> 4 项被复核**降级为良性**（通用 TVF 工厂/注册表/visitor 的按名 dispatch 臂：`TableValuedFunctionIf.java:60`、`BuiltinTableValuedFunctions.java:63`、`TableValuedFunctionVisitor.java:112`、`MetadataTableValuedFunction.java:45`）。对照组：iceberg/paimon 的 TVF 等价物（`IcebergTableValuedFunction`/`IcebergMeta`/`PaimonMeta`）**已从 fe-core 删除**，证明 hudi 这块是残面。

### B.4 paimon —— **1 项违规**

- `nereids/trees/plans/commands/info/CreateTableInfo.java:788` —— live `validate()` 里 `else if (engineName==ENGINE_PAIMON && distribution != null) throw "Paimon doesn't support 'DISTRIBUTE BY'..."`——按源跑 DDL 校验 + paimon 措辞报错。应下沉到连接器的 create-table 校验。（同文件 `:784` 是 iceberg 孪生分支，同反模式；`:968`/`:1151` 的多引擎并列名单是良性通用 dispatch。）

### B.5 maxcompute —— **1 项违规**

- `common/util/DatasourcePrintableMap.java:20` —— fe-core 主源码**唯一** import 的 maxcompute 类（`MCProperties`），在 `:55-56` 用 `MCProperties.SECRET_KEY` 硬编码遮蔽 `mc.secret_key`。违背同文件的隔离范式（DLF/iceberg-REST 均用纯字符串字面量避免依赖连接器类）。**改法**：`SENSITIVE_KEY.add("mc.secret_key")` 用字符串字面量。⚠️ 不能直接删——`registerSensitiveKeys()` 只被**文件系统** provider 喂（`FileSystemProvider.sensitivePropertyKeys()`），数据源连接器不喂，直接删会让老 MaxCompute catalog 的 `SHOW CREATE CATALOG` 泄露密钥。

### B.6 es —— **6 项违规**（全 low，废弃内置引擎兼容）

ES 连接器**已迁走**（fe-core 无 `datasource/es/`、pom 无 elasticsearch 依赖）。残留全是废弃内置 ES 引擎的向后兼容：
- `catalog/EsTable.java:39` / `catalog/EsResource.java:41` —— 源特有类，但只是 Gson 持久化桩（不 import `org.elasticsearch`，供老镜像反序列化），改动会碰持久化兼容。
- `CreateTableInfo.java:818`（唯一豁免 must-have-columns）、`:1140`（拒绝 distribution 子句）—— 按源硬编码分支。
- `catalog/Env.java:4476`（与 `:4844` 重复）—— SHOW CREATE 的 ELASTICSEARCH 弃用提示分支。
- `datasource/InternalCatalog.java:1281` —— `createTable()` 里硬编码 `elasticsearch`/`es` 抛弃用错误。

### B.7 trino —— **0 项违规**（已完全迁移干净）

- fe-core 主+测源码 **0 个 `io.trino` import**。
- 唯一 trino 命名类 `datasource/property/metastore/TrinoConnectorPropertiesFactory.java` 自述"Just a placeholder"，只把原始属性包成通用 `MetastoreProperties(Type.TRINO_CONNECTOR)`，不做解析。
- 35 处提及全良性：Trino OSS 移植工具类的**署名 URL 注释**（`EvictableCache`/`FederationBackendPolicy`/`SplitWeight` 等）、设计对齐注释、通用 dispatch 里的字符串/枚举名 `trino-connector`。
- **trino-parser 方言**（`Dialect.TRINO`、`sql_dialect`、`TSerdeDialect.PRESTO`）是**输出 serde 方言**，明确超范围。
- `JdbcTrinoClient` 属未迁移的 fe-core **JDBC catalog** 子系统（mysql/oracle/pg/... 方言客户端之一），只用标准 JDBC，不碰 io.trino。

---

## Part C — 建议的清理批次（分阶段）

> 原则（对齐既定架构铁律）：fe-core 源相关代码**只减不增**；禁为"删 A 能编译过"就近把逻辑挪进 fe-core util；遇到真 live 的源特有逻辑，走连接器 SPI 委派而非删除。

### 批次 1 —— 零风险删除（provided，废弃 lakesoul）
- 删 `lakesoul-io-java`、`scala-library`（provided、零引用）。
- `postgresql`：先确认/迁移 `JdbcResourceTest`，再删。

### 批次 2 —— iceberg 死代码 + 注释纠错（不动依赖也能立即做）
- 删 `StatisticsUtil.getIcebergColumnStats` + `getColId`。
- 删 `UnboundIcebergTableSink` + `InsertUtils` 两处分支 + `InsertOverwriteTableCommand:393` 分支 + `IcebergInsertCommandContext`。
- 删 `HiveInsertCommandContext`。
- 改 `kryo-shaded` 注释（"for hudi catalog"→指向 `WorkloadSchedPolicy`）；顺带修正 `avro`/`parquet-avro` 注释误导。

### 批次 3 —— iceberg-AWS 依赖簇整体移除（前置：迁走 5 个测试类）
1. 把 `AWSTest` / `IcebergGlueRestCatalogTest` / `IcebergUnityCatalogRestCatalogTest` / `IcebergDlfRestCatalogTest` / `S3TablesTest` 迁到 `fe-connector-iceberg`（或删除）。
2. 一起删：`iceberg-core`、`iceberg-aws`、`glue`、`s3tables`、`s3-tables-catalog-for-iceberg`、`aws-json-protocol`。
3. `parquet-avro`→`parquet-hadoop`(+`parquet-column`) 替换；删 `avro` 显式声明（此时 iceberg-core 已走，编译类路径干净；runtime avro 仍由 hive-exec 供给，可接受）。
4. 全量构建（**含测试编译**，因 `-DskipTests` 仍编译测试）验证绿。

### 批次 4 —— 需先跑 `mvn dependency:tree` 定性的依赖 ✅ 已完成（三项全 REMOVE，均已删）
- `aws-java-sdk-dynamodb`、`aws-java-sdk-logs`（v1）：dependency:tree 证为 fe-core 直接叶子、零传递消费者；hadoop-aws 3.4.2 无 S3Guard（jar 零 dynamodb 类）；ranger CloudWatch destination（`AmazonCloudWatchAuditDestination`）不在 fe-core 类路径 → **删**。
- `bce-java-sdk`：BOS 走 S3 兼容、全仓库零引用、fe-filesystem 不声明 → **删**（不是迁移）。连带清理孤立 mqtt/validation-api 管理块 + `ExternalMetaIdMgr` 装饰性 `@NotNull`（其真校验 `Preconditions.checkNotNull` 保留）。
- 定性经 4 个对抗 agent 独立反证（反射/config、ranger 审计、BOS 原生、跨模块+BE），全部 `refuted=false`（high）。

### 批次 5 —— live 源特有逻辑的迁移（独立设计，非本次"删依赖"）
按数据源分别设计 SPI 委派，把以下请出 fe-core（顺序与 catalog-SPI 迁移主线对齐）：
- **iceberg 行级 DML 簇**（B.1(b)，~15 文件）——工作量最大。
- **legacy engine=hive 簇**（`HiveTable`/`HMSResource`/`BrokerFileGroup` 分支/`Env` show-create/`ranger-hive` 授权包）。
- **hudi `hudi_meta` TVF**（3 文件）。
- **paimon / es 的 `CreateTableInfo` 分支**、`Coordinator` 按源 if-链、`AzureProperties.isIcebergRestCatalog`（有 TODO）、`DatasourcePrintableMap` 的 maxcompute 遮蔽（改字符串字面量）。
- **es 兼容桩**（`EsTable`/`EsResource`）——碰持久化兼容，最后处理或长期保留。

---

## 附：核验记录

- iceberg 主源码引用：`grep -rl "import org.apache.iceberg" src/main/java` → 仅 `StatisticsUtil.java`；hudi/paimon/hive/odps/trino/es 主源码 import 均为 **0**。
- `getIcebergColumnStats` 调用者：全 `fe/` **0**（连接器写路径 `TIcebergColumnStats` 是 thrift，另一回事）。
- 5 个 iceberg 测试类：`grep -rln "import org.apache.iceberg" src/test/java`（其中 `AWSTest`、`IcebergGlueRestCatalogTest` 含 `iceberg.aws`）——已核验。
- `scala`/`lakesoul` 引用：`src/` **0**；`postgresql` 仅 `JdbcResourceTest`；`bce-java-sdk` 全 `fe/**/src` **0**；v1 dynamodb/logs 直接引用 **0**——均已核验。
- 依赖判定经"分析 agent → 独立对抗复核 agent"两道（`glue` 做过 JDK17 实证编译；`s3-transfer-manager`/`hadoop-aws` 做过 jar `strings` 核验）。
