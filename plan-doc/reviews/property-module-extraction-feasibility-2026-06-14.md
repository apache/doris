# `datasource/property` 独立成 FE Module —— 可行性分析报告

> 日期：2026-06-14 ｜ 分支：catalog-spi-07-paimon
> 范围：`fe/fe-core/src/main/java/org/apache/doris/datasource/property/`（69 个 java 文件，约 10,389 行）
> 方法：7 个分析 agent 并行通读 + 2 个对抗式 verifier 复核 + 主线独立交叉验证（关键结论均有文件级证据）

---

## 0. 结论速览（TL;DR）

| 问题 | 结论 |
|---|---|
| **整个 `property/` 包整体搬出成一个 module？** | **不可行 / 代价过大**。`metastore` 子包（28 文件）直接构造 iceberg/paimon/hive/glue/dlf 的 catalog、直接 import 这些重型 SDK，并且 import 了 fe-core 的 `IcebergExternalCatalog/PaimonExternalCatalog/DLFCatalog`——而这三个类又**反向 import 了 property 包**，构成 Maven 无法接受的循环依赖；此外 Glue 客户端是**以源码形式 vendored 在 fe-core 里**（38 个 .java），根本没有可引用的 maven 坐标。 |
| **拆分：只把 `storage` + `common`(纯AWS部分) + `constants` + `ConnectionProperties` 搬出？** | **可行，中等工作量**。这部分**零 iceberg/paimon 依赖**，对 fe-core 的反向依赖只有 1 个（`common.util.S3URI`，自包含工具类，下沉即可），重型依赖只剩 hadoop + aws-sdk，可用 `provided` scope 排除出最终 jar。 |
| **重依赖如何不进最终 jar？** | 用 `<scope>provided</scope>`（编译期可见、不打包）。这正是用户的硬约束所要求的，且 fe-core 与各 SPI 插件运行时本就自带 hadoop/aws，late-binding 天然成立。 |
| **该并入现有 `fe-foundation` 吗？** | **不该**。`fe-foundation` 的 pom 只依赖 `fastutil-core`，零重依赖是它的立身之本（被 fe-catalog、fe-filesystem、SPI 插件当"轻基座"依赖）。塞入 hadoop/aws 会污染所有下游。应新建 `fe-property` 模块，依赖 `fe-foundation`。 |

**一句话**：用户"把属性解析做成可复用 module"的诉求，对**最有复用价值的对象存储（storage）解析**是成立且推荐的——这恰好就是 minio 那个修复（`PaimonCatalogFactory` 重写 minio.* 解析）本应复用的部分；但 `metastore`（catalog 连接/构造）本质是 fe-core 领域逻辑、不是"通用属性解析"，应留在 fe-core。

---

## 1. 背景与动机

`47bfe201c7c`（FIX-PAIMON-MINIO-STORAGE）在 SPI 连接器 `PaimonCatalogFactory` 里**重新实现**了一套 `minio.*` 属性识别逻辑，而同样的逻辑在 `datasource/property/storage/MinioProperties` 中早已存在。根因是：这套属性解析代码被关在 `fe-core` 内部，**module 外（如 fe-connector 的 SPI 插件）无法依赖复用**（fe-connector 还有 import-gate 禁止反向依赖 fe-core 内部）。因此用户希望把 `property/` 抽成独立 module 供各方复用，并要求**重型依赖（hadoop/hdfs/aws/iceberg/paimon…）不得出现在该 module 的最终 jar 里**。

本报告回答三件事：(1) 代码结构与职责；(2) 全部使用方清单；(3) 独立成 module 的可行性与落地方案。

---

## 2. 代码结构与职责理解

`property/` 下共 6 个区域，69 文件：

| 子包 | 文件数 | 职责 | 重型第三方依赖 | 对 fe-core 反向依赖 |
|---|---|---|---|---|
| **`storage`** (+`storage/exception`) | 20+1 | 把原始属性 map 解析为**按后端分类的 `StorageProperties`**（S3/OSS/COS/OBS/MinIO/GCS/Ozone/HDFS/OSS-HDFS/Azure/Broker/Local/Http），产出 Hadoop `Configuration`、BE 配置 map、AWS `AwsCredentialsProvider`、规范化 URI | hadoop `Configuration`（类型）、aws-sdk-v2（凭据/STS/Region）、hadoop-hdfs-client（仅常量） | **仅 `common.util.S3URI`（1 处）** |
| **`metastore`** | 28 | 解析 catalog 连接属性，并**直接构造 iceberg/paimon/hive/glue/dlf 的 metastore 客户端/`Catalog` 对象**（两级注册工厂：`MetastoreProperties.create` → 家族工厂 → 具体 `*Properties`） | **iceberg、paimon、hive `HiveConf`、aws-sdk-v2、glue、aliyun DLF**——全模块最重 | `IcebergExternalCatalog`(8)、`PaimonExternalCatalog`(5)、`DLFCatalog`(1,new)、`CacheSpec`、`JdbcResource` |
| **`fileformat`** | 12 | 解析文件格式属性（CSV/Text/JSON/Parquet/ORC），产出 `TFileAttributes`/`TResultFileSinkOptions` 等 thrift 结构 | **零重型第三方依赖** | thrift(fe-thrift,安全)、`Separator`、`Util.getFileCompressType`、`ConnectContext`(会话变量) |
| **`common`** | 4 | AWS 凭据 provider 构造 + iceberg-aws 凭据辅助 | aws-sdk-v2；**其中 2 个文件 import iceberg.aws** | —— |
| **`constants`** | 3 | 常量定义 | 仅 guava `Strings`（轻） | 无 |
| `ConnectionProperties.java` | 1 | 反射式属性绑定基类（被 `StorageProperties`/`MetastoreProperties` 继承） | hadoop `Configuration` | `common.CatalogConfigFileUtils`(fe-common,安全) |

关键洞察：

- **`storage` 与 `metastore` 的依赖重量天差地别**。iceberg/paimon 依赖**完全集中在 `metastore`**（`metastore` 有 50 处 iceberg/paimon import，`storage` 有 **0** 处）。
- **`fileformat` 没有重型第三方依赖**，但对 fe-core 的耦合最深（thrift×12、nereids×11、analysis、qe）——搬它没有"排除重依赖"的收益，只有成本。
- **`common` 内部可干净拆分**：`AwsCredentialsProviderFactory`/`AwsCredentialsProviderMode`（零 iceberg，被 storage 用）vs `IcebergAwsClientCredentialsProperties`/`IcebergAwsAssumeRoleProperties`（import iceberg，**只被 metastore 用**）。

---

## 3. 使用方清单（谁在用 `property`）

- `datasource.property.*` 目前**只被 fe-core 使用**：fe-core 内 **100 个文件、128 处 import**；其它 module（fe-connector / fe-filesystem 等）**零引用**（正因为它被锁在 fe-core 里无法复用——这正是要解决的痛点）。
- 外部使用按子包分布：`storage`（28 文件引用）、`common`（8）、`ConnectionProperties`（2）、`metastore`（1）。
- 主要入口（对外 API 契约）：
  - `StorageProperties.createAll(Map)` / `createPrimary(Map)` —— 最常被调用，存储属性解析总入口
  - `MetastoreProperties.create(Map)` —— catalog 连接属性总入口
  - `FileFormatProperties.createFileFormatProperties(...)` —— 文件格式属性入口
- 这意味着抽出后，fe-core 反过来 `compile` 依赖新 module 即可；**爆炸半径完全在 fe-core 内**，无第三方/下游 module 受影响。

---

## 4. 依赖分析（可行性核心）

把 `property/` 对 `org.apache.doris.*` 的**全部 35 个出向符号**逐一定位到 owning module，分类如下：

### 4.1 安全依赖（位于 fe-core 之下的模块，不构成循环）— 26 个

| 类别 | 数量 | owning module | 说明 |
|---|---|---|---|
| 生成的 thrift（`T*`） | 9 | **fe-thrift** | `TResultFileSinkOptions/TFileFormatType/TFileAttributes/TFileTextScanRangeParams/TFileCompressType/TS3StorageParam/TParquetVersion/TParquetCompressionType/TCredProviderType` |
| 生成的 proto | 3 | **fe-grpc** | `cloud.proto.Cloud` 及其嵌套枚举（S3Properties 云上模式用） |
| fe-common / fe-sql-parser | 14 | **fe-common**（13）/ **fe-sql-parser**（1） | `UserException/Config/DdlException/AnalysisException(fe-common版)/CatalogConfigFileUtils/credentials.CloudCredential` + **整个 `common.security.authentication.*` 家族**（`HadoopAuthenticator/ExecutionAuthenticator/HadoopExecutionAuthenticator/HadoopSimpleAuthenticator/SimpleAuthenticationConfig/KerberosAuthenticationConfig/AuthenticationConfig`，均在 fe-common 而非 fe-authentication）；`nereids.exceptions.AnalysisException` 在 **fe-sql-parser** |

> ⚠️ 纠正一个常见误判：`nereids.exceptions.AnalysisException` 与 `common.security.authentication.*` 都**不在 fe-core**（分别在 fe-sql-parser、fe-common），因此**不是**循环依赖障碍。

### 4.2 真正的 fe-core 反向依赖（循环风险）— 9 个

| 符号 | 用法 | 难度 | 归属子包 |
|---|---|---|---|
| `IcebergExternalCatalog` | **仅读字符串常量**（`ICEBERG_HMS/REST/GLUE/HADOOP/DLF/JDBC/S3_TABLES` 及 manifest-cache 常量） | 易（常量上提到 `constants` 子包，反转引用方向） | metastore |
| `PaimonExternalCatalog` | **仅读字符串常量**（`PAIMON_HMS/JDBC/DLF/REST/FILESYSTEM`） | 易（同上） | metastore |
| `analysis.Separator` | 仅 `convertSeparator(String)` 静态方法 | 易（下沉/复制纯字符串转换） | fileformat |
| `common.util.Util` | 仅 `getFileCompressType(String)` 静态方法 | 易（抽出该单方法） | fileformat |
| `common.util.S3URI` | `S3URI.create(...)` + 常量（403 行，自包含 URI 解析器，自身只依赖 UserException） | 易（整类下沉 fe-common 或新 module） | **storage** |
| `catalog.JdbcResource` | 仅常量 `DRIVER_URL/CLASS` + `getFullDriverUrl()` | 易（抽出常量+该方法，勿搬整类——它继承重型 Resource） | metastore |
| `datasource.metacache.CacheSpec` | `fromProperties/isCacheEnabled/...`（轻量 value+parser，只依赖 fe-common+commons+guava） | 易（下沉 fe-common/新 module） | metastore |
| `qe.ConnectContext` | `ConnectContext.get().getSessionVariable().enableTextValidateUtf8`（读 1 个会话布尔，带默认值） | 中（反转：用一个小 SessionFlags 接口/入参传入） | fileformat |
| `datasource.iceberg.dlf.DLFCatalog` | **`new DLFCatalog()` + initialize()**，返回 iceberg `Catalog`（真实行为依赖，类本身 extends iceberg HiveCatalog） | 难（须用 SPI/工厂反转 catalog 构造） | metastore |

**分布很关键**：
- `storage` 的 9 个反向依赖里**只占 1 个**（`S3URI`，且最易处理）。
- `fileformat` 占 3 个（`Separator/Util/ConnectContext`）。
- `metastore` 占 5 个（`Iceberg/PaimonExternalCatalog/DLFCatalog/JdbcResource/CacheSpec`），其中 **`DLFCatalog` 是唯一真正的行为级耦合**。

### 4.3 循环依赖的硬证据（决定"整包搬出不可行"）

三个 fe-core 类**反向 import 了 property 包**（各 1 处，已逐一核实）：

```
fe-core/.../datasource/iceberg/IcebergExternalCatalog.java   → import datasource.property.*
fe-core/.../datasource/paimon/PaimonExternalCatalog.java     → import datasource.property.*
fe-core/.../datasource/iceberg/dlf/DLFCatalog.java           → import datasource.property.*
```

只要 `metastore` 仍 import 这三个类、而它们又 import property 包，把 metastore 搬到 fe-core 之前就会形成 `fe-property → fe-core → fe-property` 循环，Maven 直接拒绝。

### 4.4 重型第三方依赖与"不进 jar"策略

| 库 | maven 坐标（版本由 fe/pom.xml dependencyManagement 统一管理） | 用法 | 出现在 | 排除策略 |
|---|---|---|---|---|
| hadoop-common | `org.apache.hadoop:hadoop-common`（hadoop 3.4.2） | `Configuration` 作字段/构造/参数类型（17 文件） | storage, metastore, ConnectionProperties | **provided** |
| hadoop-hdfs-client | `org.apache.hadoop:hadoop-hdfs-client` | 仅 `HdfsClientConfigKeys` 常量（1 文件，非编译期常量，类仍须在编译路径） | storage | **provided** |
| hive-common | `org.apache.hive:hive-common`（2.3.9）；**运行时实际由 `hive-catalog-shade` 提供（已 relocate）** | `HiveConf` 作字段/构造/返回类型（5 文件） | metastore | provided（注意与 shade 版本耦合） |
| aws-sdk-v2 | `software.amazon.awssdk:{auth,sts,regions,s3tables}`（BOM 2.29.52） | 凭据 provider 返回类型、STS、Region、S3Tables 客户端（~40 处） | storage, metastore, common | **provided** |
| iceberg | `org.apache.iceberg:{iceberg-core,iceberg-aws}`（1.10.1）+ **iceberg-hive-metastore（仅传递依赖）** | `Catalog` 返回类型 + 常量 | **metastore** | provided（**留在 fe-core** 则无需） |
| paimon | `org.apache.paimon:{paimon-core,paimon-common}` + paimon-hive（经 shade） | `Catalog` 返回类型、`Options` 字段 | **metastore** | provided（**留在 fe-core** 则无需） |
| **glue** | **❌ 无 maven 坐标——`com.amazonaws.glue.catalog.*` 是 vendored 在 fe-core 的源码（38 个 .java）** | `AWSGlueConfig` 常量 + 客户端类型 | metastore | **无法 provided**，须连源码一起处理 |
| aliyun DLF | `com.aliyun.datalake:metastore-client-*`（仓库内**经 hive-catalog-shade 打包**，`ProxyMetaStoreClient` 干净坐标本地不存在） | `DataLakeConfig` 常量、`ProxyMetaStoreClient` | metastore（4 文件 import DataLakeConfig） | provided（坐标需补，注意 shade 来源） |
| guava / commons-lang3 / commons-collections4 | 33.2.1-jre / 3.19.0 / —— | 轻量工具 | 全部 | **compile**（轻，随 jar 一起，与 fe-catalog 约定一致） |

**核心结论**：`storage`（+纯 AWS 的 common）只触及 **hadoop + aws-sdk** 两类重依赖，二者都是"编译期类型、运行时由消费方提供"，**`provided` scope 完全满足**用户"不进最终 jar"的约束。而 iceberg/paimon/glue/dlf/hive 这些最棘手的（含 vendored 源码、shade 版本耦合）**全部集中在 metastore**——把 metastore 留在 fe-core，新 module 就彻底不碰它们。

---

## 5. 对抗式复核要点（surface conflicts）

两个 verifier 对"原始综述"提出关键修正，已采纳并在本报告中纠正：

1. **"整包搬出后 module 不再需要 iceberg/paimon/aws"——证伪。** metastore 的 `*Properties` 本身就是 catalog 构造层（`AbstractIcebergProperties.initCatalog():Catalog`、`AbstractPaimonProperties.initializeCatalog():Catalog`），**直接 import 并 new iceberg/paimon catalog**，不止 `DLFCatalog` 一处。所以"只上提常量+反转 DLFCatalog"并不能让整包摆脱重型 SDK。→ **正确做法是不搬 metastore。**
2. **Glue 是 vendored 源码（38 文件）而非依赖——证实。** `HiveGlueMetaStoreProperties` import 的 `com.amazonaws.glue.catalog.util.AWSGlueConfig` 在整个仓库只存在于 fe-core.jar；无 `provided` 坐标可加。→ **又一条 metastore 必须留在 fe-core 的硬理由。**
3. **循环依赖（catalog 类反向 import property 包）——证实**（见 4.3）。

这些修正不削弱"storage 子集可抽出"的结论，反而把"metastore 不可抽出"钉死，使最终方案更清晰。

---

## 6. 推荐方案

### 6.1 新建 `fe-property` 模块，抽出"干净子集"

**搬入 `fe-property` 的内容（约 25 文件）：**
- `storage/*`（20）+ `storage/exception/*`（1）
- `common/AwsCredentialsProviderFactory.java`、`common/AwsCredentialsProviderMode.java`（2，零 iceberg）
- `constants/*`（3）
- `ConnectionProperties.java`（1）

**留在 fe-core 的内容：**
- `metastore/*`（28）—— 循环依赖 + vendored glue + 直接构造 iceberg/paimon catalog
- `fileformat/*`（12）—— 无重依赖、却深耦合 thrift/nereids/analysis/qe，搬迁无收益
- `common/IcebergAwsClientCredentialsProperties.java`、`common/IcebergAwsAssumeRoleProperties.java`（2，import iceberg，仅被 metastore 用）

### 6.2 搬迁前置改造（precondition fixups）

1. **下沉 `S3URI`**：把 `common.util.S3URI`（403 行，自身仅依赖 UserException）移到 `fe-property`（或 fe-common）。这是 storage 唯一的 fe-core 反向依赖，移走后 storage 对 fe-core 零依赖。
2. **避免 split-package**：`property.common` 若一半在 fe-property、一半在 fe-core，会造成同一个包跨两 jar（JVM/Maven 不允许）。解法：把 2 个 iceberg-aws 辅助类从 `property.common` **挪进 `property.metastore`**（它们只被 metastore 用），使 `property.common` 整体进入 fe-property。
3. **修正 `HttpProperties` 的误引**：它 import 了 `org.apache.hudi.common.util.MapUtils`（同级 `LocalProperties` 用的是 commons-collections4）。这会把 Hudi 拖进新 module，应改回 commons-collections4。

### 6.3 `fe-property` 的 pom 模板（仿 fe-catalog，重依赖翻成 provided）

- parent=`fe`（version=`${revision}`），packaging=jar，finalName=`doris-fe-property`，test-jar、javadoc skip、release source профиль——照抄 fe-catalog。
- **compile 依赖**：`fe-foundation`、`fe-common`、`fe-thrift`、`fe-grpc`（兄弟，`${project.version}`）；guava、commons-lang3、commons-collections4、log4j-api。
- **provided 依赖（不进 jar）**：`hadoop-common`、`hadoop-hdfs-client`、`hadoop-aws`、`software.amazon.awssdk:{auth,sts,regions,s3,s3tables}`。**无需 iceberg/paimon/hive/glue/dlf**（都随 metastore 留在 fe-core）。
- **建议加 fe-connector 式的 import-gate**（exec-maven-plugin，validate 阶段）禁止 `fe-property` import `org.apache.doris.datasource.{iceberg,paimon}` 及 fe-core 内部包，防止循环依赖复发。

### 6.4 构建顺序与消费

- 在 `fe/pom.xml` 的 `<modules>` 中把 `fe-property` 放在 `fe-foundation/fe-common/fe-thrift/fe-grpc/fe-catalog` 之后、`fe-core` 之前（Maven 实际按依赖图排序，列表顺序对齐即可）；在 parent dependencyManagement 加 `fe-property` 条目。
- `fe-core/pom.xml` 增加一条对 `fe-property` 的 compile 依赖；删除已搬走的源码。fe-core 运行时本就带 hadoop/aws，`provided` 依赖透明解析。
- **保持 FQN 不变**（`org.apache.doris.datasource.property.storage/common/constants`）：由于每个叶子包整体只落在一个 jar（storage/common/constants→fe-property，fileformat/metastore→fe-core，无包被劈开），fe-core 里 metastore/fileformat 对已搬类的 import **无需改动**，仅物理移动文件。

---

## 7. 风险与坑

1. **hive-catalog-shade / paimon-hive-shade 版本耦合**：`HiveConf`、DLF、iceberg-hive、paimon-hive 在 fe 里运行时来自**relocate/shade** 的制品（HMS 钉 2.3.7、thrift relocate、内嵌远古 fastutil）。本方案把这些**全留在 fe-core**，规避了该坑；若将来要搬 metastore 必须正面处理。
2. **vendored Glue 源码（38 文件）**：`metastore` 留在 fe-core 即可继续编译；不可低估其搬迁成本。
3. **`S3URI` 下沉**：需确认其无其它 fe-core 专属依赖（初查仅 UserException，干净）。下沉后 fe-core 内其它调用方自动从下层 module 解析（fe-core 仍依赖 fe-property，无碍）。
4. **`ConnectContext` 反转**：`fileformat` 留在 fe-core，本次不涉及；仅当未来要抽 fileformat 时才需做 SessionFlags 接口反转。
5. **fe-thrift / fe-grpc 是真实编译依赖**（S3Properties 用 `TS3StorageParam/TCredProviderType` + `cloud.proto.Cloud`），不是可选项，pom 必须显式声明。
6. **`provided` 的运行时契约**：消费方（fe-core、以及将来若依赖它的 SPI 插件）**必须**自带 hadoop/aws。fe-core 满足；SPI 插件本就 child-first 自带 hadoop/aws（见历史 RC-3 self-contained bundling），也满足。

---

## 8. 工作量与分阶段路径

| 阶段 | 内容 | 规模 |
|---|---|---|
| P1 | 前置改造：下沉 `S3URI`；2 个 iceberg-aws 类从 common 挪进 metastore；修 `HttpProperties` 的 hudi 误引 | 小（~5 文件） |
| P2 | 新建 `fe-property` module + pom（provided=hadoop+aws）+ 加入 reactor/dependencyManagement | 小 |
| P3 | 物理搬迁 storage/common(纯AWS)/constants/ConnectionProperties；fe-core 加 `fe-property` 依赖；编译打通 | 中（~25 文件移动，FQN 不变故 import 基本不动） |
| P4 | 加 import-gate 守卫；跑 fe-core 全量编译 + 相关 UT；核对最终 `doris-fe-property.jar` 内**不含** hadoop/aws/iceberg/paimon class | 中（验证为主） |
| （可选，后续）| 若要进一步抽 `fileformat`：先做 `Separator/Util.getFileCompressType` 下沉 + `ConnectContext` 会话标志反转 | 中 |
| （不建议）| 抽 `metastore`：须反转所有 `initCatalog/initializeCatalog/factory.create` 至 SPI + 搬 vendored glue + 解 shade 版本耦合 | 大，收益低 |

成功判据（强约束，可独立 loop 验证）：
- `mvn -pl fe/fe-property -am package` 成功；
- `unzip -l fe/fe-property/target/doris-fe-property.jar` **不含** `org/apache/hadoop/**`、`software/amazon/**`、`org/apache/iceberg/**`、`org/apache/paimon/**`、`com/amazonaws/**`；
- fe-core 全量编译通过、storage 相关 UT 全绿；
- fe-connector（或任一 fe-core 之外的 module）能成功 `compile` 依赖 `fe-property` 并调用 `StorageProperties.createAll(...)`（验证"可复用"目标达成）。

---

## 9. 总结

- 用户的设想**部分成立且值得做**：最具复用价值的**对象存储/HDFS/Azure 属性解析（storage）** 是**可干净抽出**的——它零 iceberg/paimon 依赖、对 fe-core 仅 1 处易解的反向依赖、重依赖仅 hadoop+aws 且可 `provided` 排除出 jar。抽出后，fe-connector 等 module 即可复用，**正好消除 minio 那类"在连接器里重写属性解析"的重复**。
- 但**"把整个 `property/` 搬出"不可行**：`metastore` 子包是 catalog 构造层，直接 new iceberg/paimon/hive/glue/dlf 客户端、import 重型 SDK、依赖 vendored Glue 源码，并与 fe-core 的 `*ExternalCatalog` 构成**循环依赖**。它本质是 fe-core/连接器领域逻辑，不是"通用属性解析"，应留在 fe-core。`fileformat` 无重依赖但深耦合 thrift/nereids/qe，搬迁无收益，亦留在 fe-core。
- **推荐**：新建 `fe-property`（依赖 fe-foundation，**不并入** fe-foundation 以免污染其零依赖基座），抽 `storage + common(纯AWS) + constants + ConnectionProperties`，重依赖 `provided`，加 import-gate 防循环复发。中等工作量，风险可控。
