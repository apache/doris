# fe-core / fe-connector / fe-filesystem 属性体系重构设计方案（paimon 优先）

> 目标：把 fe-core 的 **Storage Property** 全部收口到 `fe-filesystem`、把 **MetaStore Property** 收口到 `fe-connector` 的新 SPI/API，最终从 fe-core 彻底删除两者，并淘汰临时模块 `fe-property`。
> 设计聚焦 **paimon** 连接器（唯一已实质迁移属性的连接器），但 SPI 形状要让 hive/hudi/iceberg 后续直接套用、不再重抄。
> 日期：2026-06-17 ｜ 方法：8-agent 现状取证 + 关键事实 `grep` 复核（见文末附录）。配套背景报告：`plan-doc/reviews/fe-filesystem-storage-spi-review-2026-06-17.md`。

---

## 0. 决策摘要（已与架构师确认）

| # | 决策点 | 选定方案 |
|---|---|---|
| ① | MetaStore Property SPI/API 模块归属 | **新建 `fe-connector-metastore-api` + `fe-connector-metastore-spi` 模块对**，镜像 fe-filesystem 的 api/spi 拆分 |
| ② | 跨引擎连接逻辑去重策略 | **混合**：HMS/DLF/Glue/REST/JDBC 的「连接事实解析器」在 metastore-spi 实现一次；每个连接器只写薄的 catalog adapter 消费这些 facts |
| ③ | 连接器如何获取 Storage Property | **fe-core 在 CREATE CATALOG 入口绑定 `StorageProperties`（用 fe-filesystem 全量+providers），把已绑定对象经 `ConnectorContext` 传给连接器**；连接器只见 `fe-filesystem-api` 接口 |
| ④ | typed MetaStore 属性的绑定机制 | **复用 fe-foundation 的 `@ConnectorProperty` + `ConnectorPropertiesUtils`**（别名优先级 / required / sensitive / matchedProperties 全免费） |
| ⑤ | MetaStore 后端「类型」如何表达（**D-006**） | **api 层不放 per-backend `MetaStoreType` 枚举**；用 `String providerName()` + 能力方法 + `MetaStoreProvider.supports(Map)` 自识别 + ServiceLoader 发现，镜像 `FileSystemProvider`。新增后端零 api/spi 改动 |
| ⑥ | Kerberos 归属（**D-007**） | **新建叶子模块 `fe-kerberos`**（仅 hadoop-auth/common），搬入 fe-common `security.authentication.*` 作唯一真相源，fe-filesystem-hdfs 删自有副本改依赖它。⚠️ 全量去重超出本次 paimon-only 范围，分两步（见 D-007） |
| ⑦ | Vended creds 边界（**D-008**） | **连接器只「抽取」SDK token，fe-core 单点「归一」**（`ctx.vendStorageCredentials` 用 providers 重绑 → BE map）；连接器保持 api-only |

**目标依赖图（终态）**

```
fe-foundation  (叶子: @ConnectorProperty / ConnectorPropertiesUtils / ParamRules)
fe-extension-spi (叶子: Plugin / PluginFactory)
fe-kerberos (叶子 D-007: security.authentication.* / HadoopAuthenticator / Kerberos; 仅 hadoop-auth/common)
        ▲                         ▲                        ▲
        │                         │                        │
fe-filesystem-api (纯 JDK 契约)   │              （fe-kerberos 被 fe-filesystem-hdfs / fe-connector-* / fe-common / fe-core 共用）
   ▲        ▲                     │
   │        │                     │
fe-filesystem-spi             fe-connector-api  ──► fe-thrift(provided)
   ▲ (providers s3/oss/...)       ▲       ▲
   │ (fe-filesystem-hdfs ──► fe-kerberos)     │
   │                    fe-connector-spi  fe-connector-metastore-api ──► fe-foundation, fe-filesystem-api
   │                              ▲              ▲
   │                              │              │  (无 per-backend 枚举 D-006)
   │                    fe-connector-metastore-spi (共享后端 fact 解析器 + MetaStoreProvider SPI/ServiceLoader)
   │                              ▲
   │                    fe-connector-paimon / -iceberg / -hive / ...  (薄 adapter, 各注册 MetaStoreProvider)
   │
fe-core ──► fe-filesystem(全量, 含 providers) + fe-connector(api/spi/metastore-spi 经由连接器) + fe-kerberos

约束:  fe-connector-* 任何模块 ──╳──► fe-core      (CI gate 强制)
       fe-filesystem-* 任何模块 ──╳──► fe-core / fe-connector
       fe-kerberos ──╳──► fe-core / fe-connector / fe-filesystem  (纯叶子, 仅 hadoop)
```

终态边核对（与用户目标逐条对齐）：
- `fe-core → fe-connector + fe-filesystem` ✔（fe-core 已依赖 `fe-connector-api/spi`，且依赖 `fe-filesystem-api/spi/local`）
- `fe-connector → 仅 fe-filesystem-api` ✔（通过 `fe-connector-spi` 的 `ConnectorContext.getStorageProperties(): List<StorageProperties>` 引入 `fe-filesystem-api` 接口类型；连接器不依赖 fe-filesystem-spi/providers）
- `fe-filesystem → 不依赖 fe-connector/fe-core` ✔（现状已满足，api 纯 JDK）

### 0.1 本次任务范围（重要 — 已与架构师约定）

**只做迁移 / 新增，不做破坏性删除；只动 paimon，不动其它连接器。**

| 范围 | 内容 |
|---|---|
| ✅ 本次做 | 新建 `fe-connector-metastore-api/spi`（仅实现 paimon 用到的后端，后端用 `MetaStoreProvider` 自识别、**无枚举** D-006）；新增 `ConnectorContext.getStorageProperties()` 让 fe-core 下发已绑定的 fe-filesystem `StorageProperties`；改造 **paimon** 连接器：storage 改走 fe-filesystem-api、metastore 改走新 SPI；移除 paimon 对 `fe-property` 的依赖边；**新建顶层叶子 `fe-kerberos`（additive）+ 让 paimon 的 HMS kerberos facts 走它（P3a，D-007 步骤 a，paimon-local 不碰 fe-common/fe-filesystem-hdfs）** |
| 🚫 本次**不**做 | **不删除** fe-core 的 `datasource.property.storage` / `datasource.property.metastore` 任何类（hive/hudi/iceberg 仍在用，保持不动）；**不修改** hive / hudi / iceberg / es / jdbc / mc / trino 任何连接器；fe-property 物理删除留待后续（本次只断开 paimon 的依赖，使其变为孤儿）；**不动 fe-common / fe-filesystem-hdfs 的既有 kerberos 路径**（其收口到 fe-kerberos = P3b follow-up） |
| 🔭 范围外（后续任务） | hive/hudi/iceberg 迁移到新 SPI；**P3b**：fe-common + fe-filesystem-hdfs 收口到 `fe-kerberos`（全量去重、统一 `HadoopAuthenticator` 接口）；待所有连接器迁完后从 fe-core 彻底删除两个 property 包、删除 `StoragePropertiesConverter`、物理删除 fe-property 模块 |

> 即本次的「收口」= **让 paimon 不再经由 fe-core 风格的旧 storage-property 模型（fe-property 是其逐字拷贝）获取存储配置，改为消费 fe-core 经 fe-filesystem-api 下发的 typed `StorageProperties`**；fe-core 旧 property 包整体**原样保留**，其删除是后续全连接器迁完后的独立任务。

---

## 1. 现状（精炼）

### 1.1 三套 StorageProperties 并存
| 树 | 形态 | 现状角色 |
|---|---|---|
| `fe-core` `datasource.property.storage.StorageProperties` | 胖抽象类 | **线上引擎路径**（`CatalogProperty.createAll` + `DefaultConnectorContext` + `StoragePropertiesConverter`）；hive/hudi/iceberg + paimon 的 BE 侧都走它 |
| `fe-property` `property.storage.StorageProperties` | 胖抽象类（fe-core 的逐字 re-root 拷贝） | **临时**；唯一消费者是 paimon 连接器的 `PaimonCatalogFactory.buildObjectStorageHadoopConfig` |
| `fe-filesystem-api` `filesystem.properties.StorageProperties` | 瘦接口 + `FileSystemProvider<P>` 绑定 SPI | **目标**，但当前**休眠**（0 个 fe-core 消费者，详见背景报告） |

### 1.2 MetaStore Property = fe-core 的 (引擎 × 后端) 矩阵
`org.apache.doris.datasource.property.metastore`：28 文件 ~3624 LOC。

- 已存在**共享后端连接契约**：`HMSBaseProperties.of()`、`AliyunDLFBaseProperties.of()`、`AWSGlueMetaStoreBaseProperties.of()`——被 Hive/Iceberg/Paimon 的同后端 leaf 复用。
- 每个 leaf 很薄，~70–80% 是相同的连接装配，仅 ~20–30% 是引擎特定（建各自 SDK catalog）。
- **重复实测**：HMS 后端被复制约 **4 次**（`HMSBaseProperties` + `Iceberg/Paimon/HiveHMS*` + 连接器侧 `PaimonCatalogFactory.buildHmsHiveConf`）；DLF 的 8 行 `DataLakeConfig.CATALOG_*` 块逐字出现 **3 次**；JDBC 的 `registerJdbcDriver + DriverShim`（~50 行）重复 **2 次**。

### 1.3 连接器现状
- 已迁移 es/jdbc/maxcompute/trino：各自 `XxxConnectorProperties`（纯常量 + `Map.get`），**放弃了 typed 模型**，彼此零复用。
- paimon（迁移中、唯一实质迁移属性者）：`PaimonConnectorProperties`（常量 + `String[]` 别名）+ `PaimonCatalogFactory`（627 LOC 纯函数，**手抄** fe-core 的 `AbstractPaimonProperties` + 每个 `Paimon*MetaStoreProperties` + `HMSBaseProperties.getHiveConf` + `PaimonAliyunDLFMetaStoreProperties.buildHiveConf`）。
- paimon 的唯一非 connector-api/thrift 的 doris import 是 `org.apache.doris.property.storage.StorageProperties`（fe-property，1 处调用 `buildObjectStorageHadoopConfig`）。**metastore 已与 fe-core 解耦，只剩 storage 这一条边要换。**
- `fe-connector-paimon-{api,backend-hms,backend-rest,backend-aliyun-dlf,backend-filesystem}` 与 iceberg 同名目录：**当前分支上是空的 stale 残留**（仅 `.flattened-pom.xml`，无 src、未被父 pom 引用）。per-backend 拆分只在 `catalog-spi-v20260422` 分支以「**catalog-builder SPI（buildCatalog）**」形态存在，**不是** metastore-property SPI。本方案要新建的是 metastore-property SPI（与 buildCatalog SPI 互补）。

### 1.4 组合模型（必须保留的不变量）
`CatalogProperty` 持有**一份** raw map，**独立**惰性派生两者：`MetastoreProperties.create(props)` 与 `StorageProperties.createAll(props)`。二者**正交**：storage 作为参数**传入** metastore 初始化（`initializeCatalog(name, List<StorageProperties>)`），metastore **从不**把 storage 当字段持有；storage 对 metastore 一无所知。HMS 是自洽的（不吃 storage list）；只有 FileSystem/HDFS（Kerberos authenticator）与 DLF（OSS）需要 storage。

### 1.5 BE 侧转换（必须留在 fe-core）
- `StorageProperties.getBackendConfigProperties()` → BE 规范 map，`CredentialUtils.getBackendPropertiesFromStorageMap` 汇总。
- `S3Properties.getS3TStorageParam()` → `TS3StorageParam`、`getObjStoreInfoPB()` → `Cloud.ObjectStoreInfoPB`：**唯一** import thrift/cloud-proto 的存储类。
- 连接器路径已通过 `ConnectorContext`（`getBackendStorageProperties`/`normalizeStorageUri`/`vendStorageCredentials`/`loadHiveConfResources`/`executeAuthenticated`）把这些委托回 fe-core 的 `DefaultConnectorContext`。

---

## 2. 目标架构与数据流

### 2.1 CREATE CATALOG（静态配置）数据流 —— 决策③
```
用户 CREATE CATALOG (raw Map<String,String>)
        │  入口在 fe-core
        ▼
fe-core: List<StorageProperties> storageList = FileSystemPluginManager.bindAll(rawMap)   // D-009: provider 全量 bind
        │   （fe-core 依赖 fe-filesystem 全量，可发现 providers）
        ▼
fe-core: 路由到目标连接器, 经 ConnectorContext 传入:
        - List<StorageProperties> (fe-filesystem-api 接口类型, 已绑定)
        - 原始 rawMap
        ▼
fe-connector-paimon (PaimonConnector):
        - 用 fe-connector-metastore-api 解析 metastore 属性:
              MetaStoreProperties ms = HmsMetastoreBackend.parse(rawMap, storageList)   // 共享 fact 解析器
        - 用 storageList 的 toHadoopProperties().toHadoopConfigurationMap() 拿 fs.s3a.* 叠到 HiveConf
        - 用 ms.toHiveConfOverrides()/facts 拿 hive.* / dlf.catalog.* 叠到 HiveConf
        - 建 paimon Catalog (在 ctx.executeAuthenticated 内, Kerberos doAs 仍由 fe-core)
```
连接器只 import `fe-filesystem-api`（StorageProperties 接口）+ `fe-connector-metastore-api/spi`，**零 fe-core / 零 fe-property / 零 fe-filesystem-spi**。

### 2.2 BE scan（静态凭据）数据流
```
连接器 (PaimonScanPlanProvider):
   for sp in ctx.getStorageProperties():
       awsMap = sp.toBackendProperties().orElseThrow().toMap()   // AWS_* —— fe-filesystem-api 已有
   把 awsMap 写进 scan range 的 location 属性 (String map, 交给 BE)
fe-core/BE: 由 AWS_* map 组装 TS3StorageParam（thrift 仍在 fe-core S3-RPC 适配层, api 不见 thrift）
```
→ 取代现有的 `ConnectorContext.getBackendStorageProperties()` 回调（连接器现在自己用 typed 对象算 BE map）。

### 2.3 Vended creds（REST/DLF 动态、读时）与 URI 归一化
- **Vended creds 边界（D-008）**：明确两段——
  - **「抽取」= 连接器职责（SDK 特定）**：token 在读时从活的引擎 SDK 表对象提取，是**任意形状**（`s3.*`/`oss.*`…）。paimon **已落地**于 `PaimonScanPlanProvider.extractVendedToken(table)`（port 自 legacy `PaimonVendedCredentialsProvider.extractRawVendedCredentials`）。后续各连接器各写各的抽取，fe-core 旧 `Paimon/IcebergVendedCredentialsProvider` 随迁移正式下沉（本次不删，D-005）。
  - **「归一」= fe-core 单点（通用）**：raw-token → 统一 BE map 仍走 `ConnectorContext.vendStorageCredentials(rawToken)`：`filterCloudStorageProperties` + `StorageProperties.createAll`（provider **重新绑定**、派生 region/endpoint/后端调优默认）+ `getBackendPropertiesFromStorageMap` → `AWS_*`。这是 api 接口做不到的（需 ServiceLoader 发现 providers）；连接器按 D-003 只见 fe-filesystem-api、无 providers，故归一**必须**留 fe-core 单点（无漂移）。**备选**（连接器依赖 fe-filesystem-spi 自做端到端）被否：加重连接器 + 破红线。
- **URI 归一化**（`oss://`/`cos://` → BE 规范 `s3://`）：保持 `ConnectorContext.normalizeStorageUri(...)`（依赖 fe-core `LocationPath`）；**可选后续**下沉到 fe-filesystem。
- **thrift `TS3StorageParam` / `ObjectStoreInfoPB`**：永久留 fe-core（api 是 RPC-neutral）。

> 即：**静态、CREATE-CATALOG 时即可定的 → 走 typed `StorageProperties`（连接器自算）；动态/RPC/需 provider 发现的 → 留 `ConnectorContext` 委托 fe-core。** 这是决策③「混合」的精确边界。

---

## 3. 新 SPI/API 设计

### 3.1 `fe-connector-metastore-api`（纯契约，依赖 fe-foundation + fe-filesystem-api）

> **（D-013 修订）** P2-T01 落地时 api 仅依赖 **fe-kerberos**（为 `HmsMetaStoreProperties` 的 `AuthType`/`KerberosAuthSpec` 中立 facts）；`fe-foundation`/`fe-filesystem-api` 当前 api 纯接口未直接引用（`@ConnectorProperty` 绑定、`StorageProperties` 入参均在 spi 用），故留待 spi（P2-T02）按需引入，避免 api 声明未用依赖。`AuthType`/`KerberosAuthSpec` 归 fe-kerberos（先于 P2-T01 建，D-013）。

镜像 `fe-filesystem-api` 的瘦接口风格，**只暴露中立的 Map / 标量 facts，不暴露 `HiveConf`/Hadoop/SDK 类型**（HiveConf 的实体装配在连接器侧，连接器有 hive-shade）。

```java
package org.apache.doris.connector.metastore;

/** 各连接器自持的、已绑定校验的 metastore 连接属性的公共契约（对标 fe-filesystem 的 StorageProperties）。 */
public interface MetaStoreProperties {
    String providerName();                  // 字符串标识 "HMS"/"DLF"/"GLUE"/"REST"/"JDBC"/"FILESYSTEM"（D-006，非枚举）
    // ── 横切行为用「能力方法」表达，取代 per-backend 枚举上的 switch（D-006）──
    default boolean needsStorage() { return false; }            // FileSystem/DLF 需要 storageList；HMS/REST/JDBC 不需要（§1.4）
    default boolean needsVendedCredentials() { return false; }  // 取代 VendedCredentialsFactory:61 的 getType() switch
    default void validate() {}
    Map<String,String> rawProperties();
    Map<String,String> matchedProperties(); // @ConnectorProperty 实际命中的别名子集
}

/** HMS 后端的中立连接事实（HiveConf 实体由连接器组装）。 */
public interface HmsMetaStoreProperties extends MetaStoreProperties {
    String getUri();
    AuthType getAuthType();                 // SIMPLE / KERBEROS
    /** hive.* / hadoop.security.* / sasl 等中立键，连接器叠到自己的 HiveConf 上。 */
    Map<String,String> toHiveConfOverrides();
    /** Kerberos 事实(principal/keytab)，真正的 UGI.doAs 仍由 ConnectorContext.executeAuthenticated 执行。 */
    Optional<KerberosAuthSpec> kerberos();
}
public interface DlfMetaStoreProperties extends MetaStoreProperties { Map<String,String> toDlfCatalogConf(); /* 8×dlf.catalog.* */ }
public interface RestMetaStoreProperties extends MetaStoreProperties { String getUri(); Map<String,String> toRestOptions(); }
public interface JdbcMetaStoreProperties extends MetaStoreProperties { String getUri(); String getUser(); String getPassword(); String getDriverUrl(); String getDriverClass(); }
public interface GlueMetaStoreProperties extends MetaStoreProperties { Map<String,String> toGlueConf(); }
public interface FileSystemMetaStoreProperties extends MetaStoreProperties { String getWarehouse(); }
```

> 设计要点：与 fe-filesystem-api 完全一致的「瘦接口 + 中立 Map 转换」原则——**不把 hive-conf/hadoop/各引擎 SDK 类型泄进 api**，从而 REST/JDBC-only 的连接器不会被迫拖 hive 依赖。

### 3.2 `fe-connector-metastore-spi`（共享 fact 解析器，依赖 metastore-api + fe-foundation + fe-filesystem-api）—— 决策②

每个后端**一个**解析器，吃 `(rawMap, List<StorageProperties>)`，产出对应的 `*MetaStoreProperties` facts。`@ConnectorProperty` 绑定（决策④）使别名优先级/required/sensitive/matched 全部免费——**消灭 paimon 的 `String[]` 手抄别名数组**。

```java
package org.apache.doris.connector.metastore.spi;

/** HMS 连接事实解析器(共享)。Hive/Iceberg/Paimon 的 HMS adapter 都调它一次。 */
public final class HmsMetastoreBackend {
    // 内部用 @ConnectorProperty 注解的 typed holder + ConnectorPropertiesUtils.bindConnectorProperties
    public static HmsMetaStoreProperties parse(Map<String,String> raw, List<StorageProperties> storage);
}
public final class DlfMetastoreBackend  { public static DlfMetaStoreProperties  parse(Map<String,String> raw, List<StorageProperties> storage); } // 含 endpoint-from-region 推导 + 8 key
public final class GlueMetastoreBackend { public static GlueMetaStoreProperties parse(Map<String,String> raw); }                                     // 含 AssumeRole provider 链
public final class RestMetastoreBackend { public static RestMetaStoreProperties parse(Map<String,String> raw); }
public final class JdbcMetastoreBackend { public static JdbcMetaStoreProperties parse(Map<String,String> raw, Map<String,String> env); }              // 含 resolveDriverUrl + DriverShim
public final class JdbcDriverSupport    { /* registerJdbcDriver + DRIVER_CLASS_LOADER_CACHE + DriverShim —— 现在只存一份 */ }
```

**后端发现/派发 = Provider 自识别 + ServiceLoader（D-006，镜像 `FileSystemProvider`）**——取代旧 `MetastoreProperties.Type` 枚举 + 中心 switch。每个后端一个 provider（薄壳，包住上面对应的 `*MetastoreBackend.parse`），经 `META-INF/services` 注册；连接器调一次注册表即可，**不再 `switch (flavor)`**：

```java
package org.apache.doris.connector.metastore.spi;

/** 后端发现 SPI。新增后端 = 新 provider + 一行 META-INF/services，api/spi 零改动、无中心 switch。 */
public interface MetaStoreProvider<P extends MetaStoreProperties> extends PluginFactory {
    boolean supports(Map<String,String> props);                          // 自识别（读 metastore.type/特征键），cheap & 确定性
    P bind(Map<String,String> props, List<StorageProperties> storage);   // 命中后绑定（内部调对应 *MetastoreBackend.parse）
    @Override default String name() { return getClass().getSimpleName().replace("MetaStoreProvider", ""); }
}
// 内置 provider（各自 META-INF/services/...MetaStoreProvider 注册一行）：
//   HmsMetaStoreProvider / DlfMetaStoreProvider / RestMetaStoreProvider / JdbcMetaStoreProvider / FileSystemMetaStoreProvider
// 后续 Glue/S3Tables：新建 GlueMetaStoreProvider + 一行 services —— 不动 api/spi 既有代码。

/** 连接器/fe-core 调它派发，循环 providers 找首个 supports() 命中（对标 FileSystemPluginManager.createFileSystem）。 */
public final class MetaStoreProviders {
    public static MetaStoreProperties bind(Map<String,String> raw, List<StorageProperties> storage);
}
```

`@ConnectorProperty` typed holder 示例（消灭手抄别名）：
```java
final class HmsRawProps {
    @ConnectorProperty(names = {"hive.metastore.uris", "uri"}, required = true) String uri;
    @ConnectorProperty(names = {"hive.metastore.authentication.type"}, required = false) String authType = "none";
    @ConnectorProperty(names = {"hive.metastore.client.principal"}, required = false) String principal = "";
    @ConnectorProperty(names = {"hive.metastore.client.keytab"}, required = false, sensitive = true) String keytab = "";
    // ConnectorPropertiesUtils.bindConnectorProperties(this, raw) 完成绑定 + matchedProperties
}
```

> **shared vs format 切割线**：spi 解析器只产出「**连接事实**」（uri/auth/8-key/driver/warehouse/中立 hive.* map）；「**建哪个 SDK 的 catalog**」是引擎特定，留在各连接器的 adapter。`hive.conf.resources` 文件加载、Kerberos `doAs` 仍经 `ConnectorContext` 由 fe-core 执行（连接器不能 import fe-core）。

### 3.3 连接器侧 adapter（以 paimon 为例，薄）

`PaimonCatalogFactory` 从「627 行手抄」瘦身为「provider 派发拿 facts + 组装 paimon Options/HiveConf」。metastore 后端由 `MetaStoreProviders.bind` 经 `supports()` 自动选中（D-006，**无 per-backend 枚举 switch**）；剩下的 `instanceof`/`providerName` 分支是**连接器本地**的「建哪个 paimon SDK catalog」（引擎特定、允许）：
```java
MetaStoreProperties ms = MetaStoreProviders.bind(raw, storageList);   // 共享 + ServiceLoader 自识别派发
if (ms instanceof HmsMetaStoreProperties hms) {                       // 连接器本地分支(非 api 枚举)
    HiveConf hc = new HiveConf();
    ctx.loadHiveConfResources(raw.get("hive.conf.resources")).forEach(hc::set); // fe-core 加载文件
    hms.toHiveConfOverrides().forEach(hc::set);                                 // 共享 facts
    for (StorageProperties sp : storageList)                                    // fe-filesystem-api
        sp.toHadoopProperties().ifPresent(h -> h.toHadoopConfigurationMap().forEach(hc::set));
    return createPaimonHiveCatalog(buildPaimonOptions(raw, hms), hc);           // paimon 特定(薄)
}
// else if (ms instanceof RestMetaStoreProperties ...) / DlfMetaStoreProperties / ...
```
hive/iceberg 后续迁移时复用同一批 provider/`*MetastoreBackend.parse`，只写各自 `createXxxCatalog` —— **HMS/DLF/JDBC 连接逻辑不再重抄第 3、4 遍**。

### 3.4 fe-core 侧改动
- **新增**：CREATE CATALOG 时绑定 `List<StorageProperties>`（fe-filesystem 全量）并经 `ConnectorContext.getStorageProperties()` 下发。
- **保留**：`DefaultConnectorContext` 的 `vendStorageCredentials` / `normalizeStorageUri` / `loadHiveConfResources` / `executeAuthenticated`（动态/RPC/特权步骤）。
- **保留/迁移**：`S3Properties.getS3TStorageParam`/`getObjStoreInfoPB` 这类 thrift/proto 适配，迁到 fe-core 的一个 BE-RPC adapter（吃 `BackendStorageProperties.toMap()` 的中立 map）；**api 永不见 thrift**。

### 3.5 Kerberos 收口到独立叶子模块 `fe-kerberos`（D-007）

**现状（三处实现，须去重）**
| 位置 | 内容 | 谁用 |
|---|---|---|
| `fe-common` `org.apache.doris.common.security.authentication.*` | 完整套件：`AuthenticationConfig`/`KerberosAuthenticationConfig`/`HadoopAuthenticator`/`HadoopKerberosAuthenticator`/`HadoopSimpleAuthenticator`/`ExecutionAuthenticator`/`PreExecutionAuthenticator(Cache)`/`ImpersonatingHadoopAuthenticator` | fe-core（HMS `HMSBaseProperties`、`HdfsProperties`、注入 `ConnectorContext.executeAuthenticated`） |
| `fe-filesystem-hdfs` `org.apache.doris.filesystem.hdfs.KerberosHadoopAuthenticator` | **自抄一份**（实现 fe-filesystem-spi **另一个** `HadoopAuthenticator` 接口，用 `IOCallable` 而非 `PrivilegedExceptionAction`），为避免依赖 fe-common | fe-filesystem-hdfs（`DFSFileSystem`/`HdfsInputFile`） |
| `fe-connector-paimon` `PaimonCatalogFactory.buildHmsHiveConf` | **手抄** HMS 的 kerberos 条件 HiveConf 键（`sasl.enabled`、service principal、`auth_to_local`）+ doAs 回调 `ctx.executeAuthenticated` | paimon |

→ 同一段 UGI 登录/刷新/JVM-全局 `UGI.setConfiguration` 锁逻辑散在三处，改一处要改三处（fe-filesystem-hdfs 那份是约一年前拷贝，TGT 刷新可能已漂移）。

**目标：新建叶子模块 `fe-kerberos`**
- 依赖**仅** `hadoop-auth` / `hadoop-common`（把唯一外部依赖 trino `KerberosTicketUtils` 用 JDK `javax.security.auth.kerberos` 等价替换，做到零外部依赖）。auth 类现有 import 已很干净（JDK/hadoop/log4j/commons/guava + 1 trino），fe-common 不依赖 fe-core → 抽取无阻力。
- 把 fe-common `security.authentication.*` 整套**搬入 `fe-kerberos`** 作唯一真相源；fe-common 重新 export（或转依赖 fe-kerberos），fe-core 无感。
- `fe-filesystem-hdfs` **删自有 `KerberosHadoopAuthenticator`**，改依赖 `fe-kerberos`；**统一**两个打架的 `HadoopAuthenticator` 接口（`PrivilegedExceptionAction` vs `IOCallable`）为单接口 + 消费侧 adapter。
- 连接器（paimon HMS）的 kerberos facts（principal/keytab/auth_to_local）由 `fe-kerberos` 的 `KerberosAuthSpec` 承载；真正的 `UGI.doAs` 仍经 `ConnectorContext.executeAuthenticated` 由 fe-core 执行（连接器不能 import fe-core；§5 不变量 4）。

**依赖图位置**：`fe-kerberos` 与 `fe-foundation` 平级做**纯叶子**（仅 hadoop），被 `fe-common`/`fe-core`/`fe-filesystem-hdfs`/`fe-connector-*` 共用，无环（见 §0 依赖图）。**不**折进 `fe-foundation`（它是零-hadoop 的 `@ConnectorProperty` 纯叶子，不应被 hadoop 污染）。

**范围（与 §0.1 / D-005）：分两步（见 §4 Phase 3 与 tasks P3）**
- **(a) P3a，本次做（用户 2026-06-17 确认纳入）**：建顶层叶子 `fe-kerberos`（additive）+ 让 paimon 的 HMS kerberos facts 走它（**不碰** fe-common/fe-filesystem-hdfs 既有路径）→ 仍符合 D-005「只动 paimon + 纯新增」。过渡期 fe-common/fe-filesystem-hdfs 各自副本暂留（计数不增：paimon 手抄被 fe-kerberos 取代），由 (b) 收口。
- **(b) P3b，follow-up（本次不做）**：全量去重（删 fe-filesystem-hdfs 副本、fe-common 重指向 fe-kerberos、统一两个 `HadoopAuthenticator` 接口），与 hive/iceberg 迁移同批——此步会改 fe-common + fe-filesystem-hdfs，超出 D-005，故独立。

---

## 4. 实施步骤（有序 TODO，paimon 优先、分阶段）

> 原则：每步独立可编译可测、可单独提交；先建能力、再切 paimon、最后删 fe-core（待 hive/hudi/iceberg 也迁完）。

### Phase 0 — 准备
- [ ] **P0-1（DV-001 修订）** 在 `fe-filesystem-api` 确认连接器所需的**消费**侧 api：`StorageProperties.toHadoopProperties().toHadoopConfigurationMap()`（已存）、`toBackendProperties().toMap()`（已存）。**结论**：消费侧 api 已够（覆盖 paimon 现 fe-property 路的常见静态凭据键，fe-filesystem 为新事实源、较 fe-property 略**超集**：S3 assume-role/anon 额外键 + OSS/COS/OBS endpoint/region 无条件 vs 懒发；T1 钉常见路径全等 + 记超集）。**但绑定侧缺口**：仓内无 raw map → `List<StorageProperties>` 聚合入口（`FileSystemProvider.bind` 在，但 registry 私有、仅首个命中 `createFileSystem`）→ 需在 fe-core 加 `bindAll`（见 P0-2 / D-009）。~~无需新增静态门面~~（消费侧确无需；绑定侧需 bindAll）。
- [ ] **P0-2（DV-001/D-009 修订）** fe-core `FileSystemPluginManager` 新增 additive `public List<StorageProperties> bindAll(Map)`（镜像 `createFileSystem` 的 provider 循环，但 `provider.bind(props)` 全量收集所有 `supports()` 命中者，而非首个命中 `create`）；`DefaultConnectorContext.getStorageProperties()` 调它（raw map 经现有 `storagePropertiesSupplier` 值的 `getOrigProps()` 取，**不改构造点** `PluginDrivenExternalCatalog`）。**fe-filesystem 模块零改动、fe-core 旧 `datasource.property.storage` 包零改动。**
- [ ] **P0-3** `tools/check-connector-imports.sh`：当前 FORBIDDEN 不含 `property`/`foundation`，**本次不收紧**（避免破坏性改动；fe-property 物理删除与 gate 收紧均属后续任务）。Phase 1 完成后 paimon 已零 `org.apache.doris.property` import，可作为后续收紧的前置条件。

### Phase 1 — paimon 的 Storage 改走 fe-filesystem-api（决策③，纯新增/迁移，不删 fe-core）
- [ ] **P1-1** `fe-connector-spi`：`ConnectorContext` 新增 `default List<StorageProperties> getStorageProperties() { return List.of(); }`（返回 **fe-filesystem-api** 类型）→ 引入 `fe-connector-spi → fe-filesystem-api` 边（**这条边即「fe-connector 依赖 fe-filesystem-api」的落地**）。**纯新增**，默认空实现，其它连接器不受影响。
- [ ] **P1-2** fe-core `DefaultConnectorContext.getStorageProperties()`：用 fe-filesystem（全量 + providers）绑定 `StorageProperties` 并返回。**作用域限定到 plugin-driven（paimon）catalog 路径**，不改 hive/iceberg 现有引擎绑定；fe-core 旧 `datasource.property.storage` 类**原样保留**（仍服务 hive/hudi/iceberg）。
- [ ] **P1-3** paimon `PaimonCatalogFactory.applyStorageConfig`：把 `fe-property StorageProperties.buildObjectStorageHadoopConfig(props)` 替换为「遍历 `ctx.getStorageProperties()` 调 `toHadoopProperties().toHadoopConfigurationMap()`」；保留其后的 `paimon.*/fs./dfs./hadoop.` 覆盖块（**last-write-wins 顺序不变**，否则会 clobber 用户 fs.s3a./kerberos 键——有历史 bug 注释为证）。
- [ ] **P1-4** paimon `PaimonScanPlanProvider`：BE 静态凭据从 `ctx.getBackendStorageProperties()` 切到「遍历 `getStorageProperties()` 调 `toBackendProperties().toMap()`」（vended 动态路径不动，仍走 `ctx.vendStorageCredentials`）。
- [ ] **P1-5** 移除 paimon pom 的 `fe-property` 依赖与 `PaimonCatalogFactory:20` 的 import；paimon 模块 `grep` 应零 `org.apache.doris.property`。**至此「fe-connector 不再依赖旧 storage-property 模型」达成。** fe-property 模块本身**不在本次删除**（其唯一消费者是 paimon，断开后变为孤儿 0 消费者，物理删除留待后续任务）。
- [ ] **P1-6** 验证：paimon UT 全绿 + docker `enablePaimonTest=true`（5 flavor）+ 新旧 Hadoop/BE map 等价性测试（见 §5 T1）。

### Phase 2 — MetaStore Property SPI 建模 + paimon adapter 改造（决策①②④，纯新增/迁移，不删 fe-core）
- [ ] **P2-1** 新建 `fe-connector-metastore-api`（依赖 fe-foundation + fe-filesystem-api）：`MetaStoreProperties`（`String providerName()` + 能力方法 `needsStorage()`/`needsVendedCredentials()`，**无 per-backend `MetaStoreType` 枚举**，D-006）+ 后端子接口（§3.1）。**本次只定义 paimon 用到的后端**：HMS / DLF / REST / JDBC / FileSystem；Glue / S3Tables（iceberg/hive 专用）**不在本次实现**，留接口可扩展即可。
- [ ] **P2-2** 新建 `fe-connector-metastore-spi`（依赖 metastore-api + fe-foundation + fe-filesystem-api）：`Hms/Dlf/Rest/Jdbc/FileSystem MetastoreBackend.parse(...)` + `JdbcDriverSupport` + **`MetaStoreProvider<P>` SPI（`supports()` 自识别）+ 5 内置 provider + `META-INF/services` + `MetaStoreProviders.bind` 派发**（§3.2，D-006），用 `@ConnectorProperty` typed holder 绑定。**来源 = 上移 paimon 现有 `PaimonCatalogFactory` 里已经手抄的连接逻辑**（它本就是 fe-core `HMSBaseProperties`/`AliyunDLFBaseProperties` 等的 port），做去 fe-core 化整理（HiveConf→中立 map、authenticator→`KerberosAuthSpec` facts）。**fe-core 的 `HMSBaseProperties` 等对应类一律保持不动**（仍服务 hive/hudi/iceberg）。
- [ ] **P2-3** paimon adapter 改造：`PaimonCatalogFactory` 的 `buildHmsHiveConf`/`buildDlfHiveConf`/`validate`/别名常量 → 改为调用共享 `*MetastoreBackend.parse` + 薄 paimon Options/HiveConf 组装（§3.3）。删（连接器内部的）`PaimonConnectorProperties` 别名数组，由 spi typed holder 取代——**这是连接器自身代码，不属于 fe-core**。
- [ ] **P2-4** paimon pom 增 `fe-connector-metastore-api/spi` 依赖；`grep` 确认 paimon 无 fe-core import；CI gate 通过。
- [ ] **P2-5** 验证：paimon UT + docker 5 flavor（filesystem/hms/rest/jdbc/dlf）+ vended(REST/DLF) + Kerberos HMS；与 fe-core 旧 `Paimon*MetaStoreProperties` 行为对照（HiveConf key 集、ParamRules 报错文案一致，见 §5 T2）。

> **fe-core 旧 `datasource.property.metastore` 包在本次全程保持不动。** paimon 切换后这些类对 paimon 路径成为 dead code（`PaimonExternalCatalog` 旧路径），但仍被 hive/hudi/iceberg 使用，故**不删**。

### 范围外（后续独立任务，本次不做）
- hive / hudi / iceberg 连接器迁移到本 SPI：各写薄 adapter 复用 `*MetastoreBackend.parse` + `getStorageProperties()`，并补齐 Glue / S3Tables / REST-oauth2-sigv4 等后端。
- 全部连接器迁完后：从 fe-core **彻底删除** `datasource.property.storage` 与 `datasource.property.metastore` 两个包、删 `StoragePropertiesConverter` 等桥；物理删除 `fe-property` 模块（`fe/pom.xml` module/version + 目录）并收紧 import gate 禁 `org.apache.doris.property`。

---

## 5. 关键不变量 / 风险 / 测试

**必须保留的不变量**
1. **正交组合**：metastore 不持有 storage 字段；storage 作入参传入（§1.4）。新 `parse(raw, storageList)` 维持此形态。
2. **storage 叠加顺序**：canonical 翻译在前、`paimon.*/fs./dfs./hadoop.` 覆盖在后（last-write-wins）。P1-3 必须保序。
3. **HMS Kerberos 条件键**：`hive.metastore.sasl.enabled` + `hadoop.security.authentication=kerberos` 的分支、service principal、`auth_to_local` 必须在 storage 叠加**之后**施加（否则被 raw `hadoop.*` passthrough clobber——已知 bug）。
4. **特权/RPC 留 fe-core**：Kerberos `doAs`、`hive.conf.resources` 文件加载、vended 绑定、`TS3StorageParam`/`ObjectStoreInfoPB` 全部经 `ConnectorContext`/fe-core，连接器零 fe-core import（CI gate 强制）。

**风险**
- **R1 等价性漂移**：新 `toHadoopConfigurationMap()`/`toBackendProperties().toMap()` 与旧 `getHadoopStorageConfig()`/`getBackendConfigProperties()` 的 key/value 必须逐一对齐（注意默认调优值已分叉：S3=50/3000/1000 vs OSS/COS/OBS=100/10000/10000）。
- **R2 双路径并存窗口**：Phase 1/2 期间 fe-core 旧 storage（hive/hudi/iceberg 用）与 fe-filesystem 新 storage（paimon 用）并存；同一 catalog 不能两路推出不同配置——paimon 已完全切到新路即可隔离。
- **R3 打包/类加载**：HMS/DLF 活连接需 relocated thrift（`fe-connector-paimon-hive-shade`）build-order 在前 + child-first hadoop/aws bundling，重构模块时不可破坏（有历史 S3A/thrift 跨 loader cast bug）。

**测试（决策驱动，强制）**
- **T1 新旧等价性（DV-002 修订）**：对 S3/OSS/COS/OBS/HDFS 代表输入，断言新 `toHadoopConfigurationMap()` / `toBackendProperties().toMap()` 与 paimon 现走 fe-property 旧产物在**常见静态凭据路径**（配齐 endpoint/region/AK/SK，无 role、无 vended）下 key/value **全等**（含默认调优值分叉）；fe-filesystem 的**超集差异**（S3 role/anon、OSS/COS/OBS endpoint 无条件、BE map 多 AWS_BUCKET/ROOT_PATH/CREDENTIALS_PROVIDER_TYPE）作**有意、更完整**记录，不视为漂移（用户 2026-06-17 定 A，认 fe-filesystem 为新事实源）。这是切换的回归闸（背景报告指出当前**缺**此测试）。
- **T2 metastore facts 等价性**：对 HMS(simple/kerberos)、DLF(endpoint-from-region)、REST、JDBC、filesystem，断言共享 `*MetastoreBackend.parse` 产出的中立 map 与 fe-core 旧 `Paimon*MetaStoreProperties` 一致（含 ParamRules 报错文案）。
- **T3 依赖图守门**：ArchUnit/CI gate 断言 `fe-connector-*` 不 import `org.apache.doris.{catalog,common,datasource,qe,...}`，且 Phase 1 后追加禁 `org.apache.doris.property`；`fe-filesystem-*` 不 import fe-core/fe-connector。
- **T4 端到端**：docker `enablePaimonTest=true` 跑 paimon 5 flavor（filesystem/hms/rest/jdbc/dlf）读 + vended(REST/DLF) + Kerberos HMS。

---

## 6. 验收标准（本次任务）
1. paimon 连接器零 `org.apache.doris.property`、零 `org.apache.doris.datasource`、零 fe-core import；仅依赖 `fe-connector-{api,spi,metastore-api,metastore-spi}` + `fe-filesystem-api` + `fe-thrift(provided)` + SDK。
2. `fe-property` 变为 **0 消费者**（孤儿模块，**本次不物理删除**）；import gate **未收紧**（保持现状）。
3. paimon 用到的 HMS/DLF/REST/JDBC/FileSystem 连接逻辑在 `fe-connector-metastore-spi` 各存**一份**；paimon adapter 不再含手抄连接逻辑。
4. T1–T4 全绿；docker paimon 5 flavor 通过。
5. 依赖边落地：`fe-connector → 仅 fe-filesystem-api`，`fe-filesystem ↛ fe-connector/fe-core`。
6. **零改动核对**：fe-core 的 `datasource.property.storage` / `datasource.property.metastore` 两个包，以及 hive/hudi/iceberg/es/jdbc/mc/trino 连接器，本次**未被修改**（`git diff` 应不含这些路径，除 P1-2 的 `DefaultConnectorContext` 新增方法外不动 fe-core property 包）。
7. （范围外、后续）全连接器迁完后再删 fe-core 两包 + 物理删 fe-property + 收紧 gate。

---

## 附录 A — 关键事实独立核验（grep）
| 论断 | 结果 |
|---|---|
| paimon 连接器对 fe-core 的 import 数 | **0**（唯一存储 import 是 fe-property `property.storage.StorageProperties`） |
| BE thrift/proto 适配器位置 | **仅** `fe-core/.../storage/S3Properties.java`（`getS3TStorageParam`/`getObjStoreInfoPB`） |
| fe-core 是否已依赖 fe-connector | **是**（`fe-connector-api` + `fe-connector-spi`） |
| fe-core 是否依赖 fe-property | **否** |
| import gate 禁止/允许 | 禁 `catalog|common|datasource|qe|analysis|nereids|planner`；允许 `thrift`/`filesystem`；**未禁** `property`/`foundation` |
| paimon/iceberg per-backend 模块 | 当前分支为 **stale 空目录**；真实拆分在 `catalog-spi-v20260422`，且是 **buildCatalog SPI** 而非 metastore-property SPI |
| fe-core metastore 包规模 | 28 文件 ~3624 LOC；共享后端基类 `HMSBaseProperties`/`AliyunDLFBaseProperties`/`AWSGlueMetaStoreBaseProperties` 已存在 |

*本方案基于 commit `70e934d` 工作区；docker/e2e 未运行；属设计与可实施步骤层面。实施前请按本工作流（research-design-workflow）批准 TODO 列表。*
