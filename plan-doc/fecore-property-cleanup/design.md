# 设计文档 — 清理 fe-core `datasource/property/{common,metastore}`

> **稳定参考文档**，不是状态。状态看 [`tasklist.md`](./tasklist.md) / [`HANDOFF.md`](./HANDOFF.md)。
> **基线**：2026-07-28 / `3468d905eb3` / 分支 `catalog-spi-review-21`。
> **⚠️ 行号信 HEAD 不信文档。**
>
> **调研方法**：8 路并行侦察（172 条 finding）→ 3 路独立设计（minimal / architectural / risk 三种视角）
> → 6 项对抗验证（**2 项被推翻**，被推翻的用修正版）→ 综合。承重结论由本文作者逐条复核代码。

---

## 1. 现状盘点

`fe/fe-core/src/main/java/org/apache/doris/datasource/property/` 共 21 个文件 2105 行，四个子域：

| 子域 | 文件数 / 行数 | 是不是「外部数据源」 | 本任务处置 |
|---|---|---|---|
| `metastore/` | 4 / 333 | ✅ 是 | **删** |
| `ConnectionProperties.java` | 1 / 140 | ✅ 是（`metastore/` 的基类） | **删**（孤儿） |
| `common/` | 2 / 237 | ❌ **不是**（内部存储） | **留**，砍死代码 |
| `constants/` + `fileformat/` | 14 / 1395 | ❌ 不是 | 不在范围（§6） |

---

## 2. `metastore/` — 判死证据链

### 2.1 结构：已经是空壳

```
metastore/MetastoreProperties.java              191 行  ← 注册表 + Type 枚举 + 基类
metastore/MetastorePropertiesFactory.java        36 行  ← 接口
metastore/AbstractMetastorePropertiesFactory.java 74 行  ← 子类型分发骨架
metastore/TrinoConnectorPropertiesFactory.java   32 行  ← 唯一注册的工厂
```

- `MetastoreProperties.java:86-93` 的静态注册表**只剩 `Type.TRINO_CONNECTOR`**。
  hms / iceberg / paimon 三家的工厂在 "Design S7" 时被**主动摘掉**，注释明说
  「Type 枚举值保留（好让走岔的 `create()` 响亮报错），但工厂**故意不注册**」。
- 而这唯一的 `TrinoConnectorPropertiesFactory.java:28-31` override 了 `create()`，直接
  `return new MetastoreProperties(Type.TRINO_CONNECTOR, props)` —— **连
  `initNormalizeAndCheckProps()` 都不调**。⇒ 零解析。
- 连带后果：`AbstractMetastorePropertiesFactory.createInternal`（`:58`）**零调用者**，
  `ConnectionProperties.initNormalizeAndCheckProps()` 的 `@ConnectorProperty` 反射绑定
  **永不执行**，`getDerivedStorageProperties()`（`:157-159`）是写死的 `emptyMap()` 且**零子类**，
  `getExecutionAuthenticator` / `asLegacyAuthenticator` / `StorageAuthenticatorBridge`（`:168-190`）
  **零调用者**。

**⚠️ 别被 `Type.TRINO_CONNECTOR` 骗了**：`type=trino-connector` 的目录是走
**fe-connector-trino 插件**的，根本不经这个工厂。

### 2.2 可达性：两道门都关死

全仓唯一 import 者 = `CatalogProperty.java:21`。它开了两道门：

**门一（`checkMetaStoreAndStorageProperties`，`CatalogProperty.java:307-321`）—— 死。**
全仓仅有它自己的声明，**零调用者**（含 `regression-test/` groovy）。⇒ `:310` 的
`MetastoreProperties.create()` 不可达。

**门二（`resolveDerivedStorageDefaults`，`CatalogProperty.java:264-272`）—— 被门挡死。**
```java
Supplier<Map<String,String>> pluginSupplier = pluginDerivedStorageDefaultsSupplier;
if (pluginSupplier != null) { return pluginSupplier.get(); }   // ← 插件目录永远走这条
MetastoreProperties msp = getMetastoreProperties();            // ← 只有 supplier==null 才走
```
- `PluginDrivenExternalCatalog.java:177` **无条件**安装这个 supplier（Design S8）。
- fe-core main 源里 `ExternalCatalog` 的具体子类**只剩 3 个**：`PluginDrivenExternalCatalog`、
  `RemoteDorisExternalCatalog`、`TestExternalCatalog`；后两个**完全不碰 storage/hadoop/metastore**。

**门二的时序窗口（曾被判 HIGH 风险，复核后判为不可达）**：
`PluginDrivenExternalCatalog.java:150` 构造连接器时，`DefaultConnectorContext` 已经接上了
`catalogProperty` 的 storage supplier（`:206-208`），而派生 supplier 要到 `:177` 才安装。
⇒ 理论上存在「supplier 还是 null 就访问 storage」的窗口。
**但实测不可达**：
- `PaimonConnector` 构造函数（`:151-166`）只传方法引用 `this::pluginAuthenticator`（**惰性 memoize**，
  见 `:174-186`），不碰 storage；
- `IcebergConnector` 构造函数（`:215+`）同样只传 `this::pluginAuthenticator`；
- `HiveConnector` / `HudiConnector` 构造函数中**无任何** `storage()` / `getStorageProperties` 调用；
- `IcebergConnectorProvider.validateProperties`（`:78-82`）/ `PaimonConnectorProvider`（`:94-98`）
  显式传 `Collections.emptyMap()`，javadoc 明写「验证不需要 storage」。

⇒ **窗口存在于代码形状上，但今天没有任何路径走进去。** 它只影响「删除后是 fail-loud 还是
fail-silent」这个选择，见 [`open-decisions.md`](./open-decisions.md) OD-1。

### 2.3 持久化：无坑

这条必须查，因为本仓库有前科（`EsTable`/`EsResource`：删掉 Gson 注册的持久化类 →
`RuntimeTypeAdapterFactory` 对未注册 clazz 硬抛 → **老镜像 FE 起不来**）。

- `property/` 树下**无任何** `@SerializedName` / Gson / `Serializable` / `Writable`；
- `CatalogProperty` 只持久化 `resource`（`:56`）和 `properties`（`:59`）；
  `metastoreProperties`（`:94`）**无注解**，被 `GsonUtilsBase.HiddenAnnotationExclusionStrategy` 跳过；
- 不是任何 `RuntimeTypeAdapterFactory` 的基类或子类型；
- `gensrc/thrift`、`gensrc/proto`、所有 `META-INF/services`、所有 `.groovy`/`.out`/`.sh`、
  两个 build gate 里**全无踪迹**。

⚠️ 注意区分：`InitCatalogLog.Type` / `InitDatabaseLog.Type` 里的 `TRINO_CONNECTOR` 是**另一个**
枚举（那个是持久化的，**别碰**）；`MetastoreProperties.Type` 只在运行期计算，不持久化。

### 2.4 接班人已上线 ⇒ 没有东西需要搬

| fe-core（要删的） | 连接器侧接班人（已在跑） |
|---|---|
| `MetastoreProperties` | `org.apache.doris.connector.metastore.MetaStoreProperties`（fe-connector-metastore-api） |
| `MetastorePropertiesFactory` + `AbstractMetastorePropertiesFactory` 注册表 | `MetaStoreProvider` / `MetaStoreProviders.bind(…)` / `bindForType(…)`（fe-connector-metastore-spi） |
| `getDerivedStorageProperties()` | `Connector.deriveStorageProperties(Map)`（fe-connector-api），实现见 `IcebergConnector.java:1161-1198` |
| `ConnectionProperties` 的 `@ConnectorProperty` 反射绑定 | `AbstractMetaStoreProperties` + `MetaStoreParseUtils`（fe-connector-metastore-spi） |

消费点：`PaimonConnectorProvider.java:96`、`IcebergConnectorProvider.java:80`、
`HiveConnector.java:720`、`HudiConnector.java:296`。

### 2.5 `ConnectionProperties` 顺带成孤儿

- 全仓唯一子类就是 `MetastoreProperties`（`:46`）。
- `StorageAdapter.java:856` 那处引用 —— **是 javadoc 散文，不是代码**：
  `* {@code ConnectionProperties.equals}: logically identical configurations must share one`，
  位于 `StorageAdapter` 自己 `equals()` 上方的注释块里。
  ⇒ **`StorageAdapter` 不需要改就能编过**；改它只是为了不留悬空引用（cosmetic）。

---

## 3. `common/` — 判「留」证据链

### 3.1 它不是外部数据源代码

| 消费者 | 服务的功能 |
|---|---|
| `StorageAdapter.java:21-22` | 内部存储适配 |
| `S3ThriftAdapter.java:20` | 发给 BE 的 S3 thrift 参数 |
| `CloudObjectStoreAdapter.java:23` | 云上 `StorageVault`（`StorageVaultMgr.java:150`） |
| `AzureGuessRoutingParityTest.java:21`（测试） | 上述的对齐测试 |

再往上：冷存 `StoragePolicy`（`PushStoragePolicyTask.java:91`）、TVF / backup / export。

**零个 `fe-connector-*` 模块、零个 `fe-filesystem-*` 模块 import 它。** 它不在这次迁移的射程内。

### 3.2 三个候选目的地全部堵死

| 目的地 | 为什么不行 |
|---|---|
| `fe-filesystem-s3-base` | ① fe-core 对它**任何 scope 的依赖都没有**（`grep s3-base fe/fe-core/pom.xml` 退出码 1）；② 它是 IMPL 层，`fe/fe-filesystem/README.md:47-63` 禁止 fe-core 依赖；③ **最要命**：`FileSystemPluginManager.java:88-90` 把 `org.apache.doris.filesystem.` 设为 **parent-first** → 把 s3-base jar 放进 `fe/lib` 会**静默遮蔽** s3/gcs/minio/ozone 各插件自带的同名类（本仓库已知的 split-brain 类坑） |
| `fe-filesystem-api` | 按约定只放 JDK 类型；而工厂 import 了 `software.amazon.awssdk.auth.credentials.*`（`AwsCredentialsProviderFactory.java:25-32`） |
| `fe-foundation` | 不带 AWS SDK；且 `org.apache.doris.foundation.` **不在** `ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES` 里（`:73-74`），而各连接器 zip 都打包 `fe-foundation.jar` → 潜在 duplicate-Class LinkageError |

### 3.3 ⚠️ 「复用 fe-filesystem 现成那份」是行为变更，不是重构

**这是本次调研初判出错、被对抗验证两轮推翻的地方，务必记住。**

仓库里现在有**四份** AWS 凭证模式实现：

| | 位置 |
|---|---|
| A | fe-core `property/common/{AwsCredentialsProviderMode,AwsCredentialsProviderFactory}` |
| B | `fe-filesystem-s3-base` `{S3CredentialsProviderType,S3CredentialsProviderFactory}` |
| C | `fe-connector-iceberg` `AwsCredentialsProviderModes`（注释自称 "self-contained twin"，因连接器不能 import fe-core） |
| D | `fe-connector-metastore-iceberg` `IcebergRestMetaStoreProperties` 内联的模式检查 |

**A → B 替换会带来两条活的行为差异**：

1. **发给 hadoop 的凭证 provider 串会多一项**
   `,software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider`
   （B 的 `S3CredentialsProviderFactory.java:115` 有，A 的 `:116-129` 没有）。
   活链路：`StorageAdapter.java:101-105`（`S3_CREDENTIAL_KEYS`）→ `:678-694` 跳过 SPI 的值并重新推导
   → `:730` / `:739` 写 `fs.s3a.assumed.role.credentials.provider` / `fs.s3a.aws.credentials.provider`
   → `CatalogProperty.java:383` 消费。

2. **模式串接受面放宽**
   B（`S3CredentialsProviderType.java:45,:51,:56`）接受空串、`ENVIRONMENT`、`WEB_IDENTITY_TOKEN_FILE`；
   A（`AwsCredentialsProviderMode.java:48,:69-72`）对这些**抛异常**。
   而 `StorageAdapter.java:169-170` 的注释明说这个严格是**故意的**。报错文案也不同。

**而 `fe/` 和 `regression-test/` 里没有任何测试钉住发出的那个串** ⇒ 这个回归会**绿着上线**。

**已确认是死代码的差异**（对抗第一轮夸大、第二轮修正）：
- A 与 B 的 `DEFAULT` 链差异，在 fe-core 侧**只能**经 `StorageAdapter.getAwsCredentialsProvider()`
  （`:391`）到达，而该方法**零调用者**（全仓只有它自己的声明和 javadoc）。
  ⇒ 「匿名访问突然开始签名」这个吓人的场景**今天不可能发生**。
- `AwsCredentialsProviderFactory.getV2ClassName(mode)`（单参，`:141-162`）同样零调用者。

**真正对齐的部分**：只有发给 BE 的 `AWS_CREDENTIALS_PROVIDER_TYPE` 值（`StorageAdapter.java:614,:623`）
—— 两个枚举声明了相同的 7 个常量且 `getMode() == name()`。

### 3.4 能砍的：约 146 行纯死代码

- `StorageAdapter.getAwsCredentialsProvider()`（`:383-416` 含 javadoc）+
  `staticAwsCredentialsProvider(...)`（`:418-429`）+ `s3AwsCredentialsProvider(...)`（`:431-458`）
- `AwsCredentialsProviderFactory.createV2(mode,boolean)`（`:46-68`）+
  `createDefaultV2(boolean)`（`:80-99`）+ 单参 `getV2ClassName(mode)`（`:136-162`）

**必须保留**：`StorageAdapter.getAwsCredentialsProviderMode()`（`:379-381`）和 `s3CredentialsMode` 字段
（被 `AzureGuessRoutingParityTest` 钉住，且喂 `:614` 那个**活的** BE 值）；
`AwsCredentialsProviderFactory.getV2ClassName(mode, boolean)`（`:101-134`）和两个 env 探针（`:70-78`）；
`StorageAdapter` 的 `InstanceProfileCredentialsProvider` import（`:42`，`:731` 在用）。

---

## 4. 🔴 checkstyle 是硬门禁（两份候选设计栽在这）

`fe/pom.xml:114` 在**父 `<build><plugins>`** 里声明 `maven-checkstyle-plugin`，
`fe/pom.xml:177-183` 把 `check` 绑到**每个模块的 `validate` 阶段**；
`fe/check/checkstyle/checkstyle.xml:27` 设 `severity=error`、`:167` 开 `UnusedImports`；
`suppressions.xml` 对这两个路径都没有豁免。

⇒ **漏删一个 import，不带 flag 的 `mvn test` 会在跑任何测试之前就中止。**

三份候选设计里**有两份的 import 清单是错的**。修正后的精确清单见 `tasklist.md` 各任务。

---

## 5. 风险登记

| 级别 | 风险 | 缓解 |
|---|---|---|
| 🟠 中 | 删除后，理论上的 null-supplier 窗口从 **fail-loud** 变 **fail-silent**（返回 `emptyMap` → 丢掉 iceberg `warehouse→fs.defaultFS` 桥接 → 而且因为 setter 故意不重置缓存，错误的 `StorageBindings` 会被**永久缓存**） | 窗口今天不可达（§2.2）。处置方式见 [`open-decisions.md`](./open-decisions.md) **OD-1**（推荐：null 分支 `throw`，精确保留今天行为） |
| 🟠 中 | 删除类改动**不能只信增量编译**——`fe-core/target/classes` 里确实存在无源文件的陈旧 `.class` | 每步 `rm -rf fe-core/target/{classes,test-classes}` + 全反应堆 `clean test-compile`（**含测试源，禁 `-Dmaven.test.skip=true`**）+ 全仓 `grep -rIn`（不是符号 grep） |
| 🟠 中 | checkstyle `UnusedImports` 门禁（§4） | 每步把 `checkstyle:check` 当**阻塞项**跑 |
| 🟠 中 | `fe-connector-api` 的录制基线（`ConnectorMetadataSurfaceTest` ↔ `connector-metadata-methods.txt`）是本分支已知盲区——全反应堆 test-compile **不跑 surefire** | FPC-03 的验收**显式**跑 `-pl fe-connector/fe-connector-api test`。**预期不需要刷基线**（该模块不依赖 fe-core，72 行基线只用 `connector.api.*`/`java.*` 类型）——**一旦红了就停手** |
| 🟢 低 | FPC-02 删的 `StorageAdapter.getAwsCredentialsProvider()` 是上游 `f499c78c67c`（#66004）整体带进来的；若 apache/doris master 有或将有调用者，下次 rebase 会 modify/delete 冲突 | FPC-02 可整项丢弃，不影响其它任务。落地前 grep 一次上游 master |

---

## 6. 兄弟目录：明确不在范围

### `fileformat/`（11 文件 ~1318 行）— 命名不当，不是架构违规
解析的是 LOAD（broker/routine/mysql）、`SELECT … INTO OUTFILE`、`COPY INTO`、文件 TVF 的读写选项，
直接吐 fe-thrift 类型（`FileFormatProperties.java:21-25` import `TFileAttributes`/`TFileCompressType`/
`TFileFormatType`/`TResultFileSinkOptions`）。24 个消费者**全在 fe-core**，零连接器 / 零 fe-filesystem 引用。
**它挂在 `datasource.property` 下只是名字取错了**，改名是 24 文件的纯 churn，无架构收益。

### `constants/`（3 文件）— 不是数据源属性
`AIProperties` 是 AI/LLM 模型资源；`RemoteDorisProperties` 是 Doris-to-Doris 目录的**纯键名常量、零解析**
（import 者只有 `catalog/AIResource.java:22`、`AIResourceTest.java:24`、
`datasource/doris/RemoteDorisExternalCatalog.java:26`）。
🔎 **顺带发现一个可独立清理项**（另开 ticket，不并入本任务）：
`constants/BaseProperties.getCloudCredential(...)` **零调用者**，其唯一作用是当 `AIProperties.java:28` 的空父类。

### `StorageAdapter.checkAzureOauth2OnlyForIcebergRest()`（`:822-843`）— 真违规，但要单独一刀
它在 **storage 路径**上读 **metastore 命名空间**的键（`type` / `iceberg.catalog.type`），
是货真价实的 ARCH-GOAL 违规。但它是上游 #66004 的代码，带着**刻意的大小写敏感怪癖**，
需要自己的一刀 + e2e。**在此记下，免得丢。**

---

## 7. 被否方案（存档，免得下次重走）

1. **把 `common/` 搬去 `fe-filesystem-s3-base` / `fe-filesystem-api` / `fe-foundation`** — §3.2 三条独立理由。
2. **把 fe-core 三个 adapter 改指向 `fe-filesystem-s3-base` 的现成实现，然后删 `common/`**
   （architectural 视角的完整 S3–S5 轨道：把 `S3CredentialsProviderType` 上提到 `fe-filesystem-api`、
   调和 `hadoopClassName`、再删 `common/`）。
   —— 它**在架构上是对的终局，本文不称其为错**，但：① 需要用户对
   `Config.aws_credentials_provider_version` 的 v1 分支（`Config.java:3740`）和「最终发哪条 DEFAULT 链」
   两个问题拍板；② 会改 FE→hadoop 的线上串和持久化的 `AWS_CREDENTIALS_PROVIDER_TYPE` 别名族；
   ③ **对 catalog-SPI 迁移零收益**（消费者是内部存储代码）。**另开 ticket 跟踪。**
3. **给 fe-core 加新 SPI**（如在 `S3CompatibleFileSystemProperties` 上加
   `hadoopCredentialsProviderClassName(boolean)`）—— 为了让 fe-core **继续留在**凭证生意里而发明的
   additive SPI，违背方向；且六个 S3 方言无从作答（`AbstractDelegatingS3Properties.java:216-224`
   把 role/external-id 写死为空）。
4. **让 fe-core 依赖 `fe-connector-metastore-api`** —— FPC-03 之后 fe-core 不再持有任何 metastore 代码，
   这个依赖零用户，还会把 `fe-kerberos` 拖上 fe-core 的 classpath。
   🔎 顺带记一笔：该模块 `pom.xml:64` 的注释「This module is compiled into fe-core」**是过时的假话**
   —— 没有任何 pom 声明它，它是打进 paimon/iceberg/hms/hudi 各插件 zip 的。
5. **收敛第三份 AWS twin**（`fe-connector-iceberg/AwsCredentialsProviderModes`）—— 它确实是**第三个变体**
   （未知模式回退 DEFAULT 而非抛异常；DEFAULT 不发类名），有自己的测试钉着，
   收敛需要新增「连接器 → fe-filesystem-api」依赖。不做。

---

## 8. 诚实声明：尚未验证的部分

- **没跑任何 maven 构建**：`tasklist.md` 里的验证命令是开好的方子，**不是已跑的结果**。
- **没查 apache/doris master** 是否有 `StorageAdapter.getAwsCredentialsProvider()` 的调用者
  ⇒ FPC-02 的 rebase 冲突风险是**陈述**不是实测。
- **没跑 e2e**（需要集群）。相关套件（`regression-test/suites/aws_iam_role_p0/*`）只做了 grep：
  用的是 `INSTANCE_PROFILE` / `CONTAINER` / `ANONYMOUS` 这类规范名，A/B 两个枚举都同样接受；
  全仓唯一的 `v1` 行是注释掉的（`test_tvf_anonymous.groovy:30`）。
- **`ExternalCatalog.buildHadoopConfiguration(Map)` 的调用者没有枚举** ⇒ FPC-04 明确排除它。
- **FE 侧的 hadoop s3a client 是否真的会去实例化 `fs.s3a.aws.credentials.provider` 里点名的类**，
  没有端到端追到底：发出与传播链路追到了（`StorageAdapter.java:730,:739` → `CatalogProperty.java:383`
  → `ExternalCatalog.java:209,:252`），**终端消费者没追**。这决定了 §3.3 差异 (1) 的严重程度上限。
