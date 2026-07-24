# fe-filesystem 存储属性 SPI/API 设计调研与评审报告

> 对象：commit `2a113a6` `[feat](fs) Add native filesystem SPI for object storage (#63400)`
> 范围：新引入的 `fe-filesystem` 多模块存储属性模型（`org.apache.doris.filesystem.properties.*`）与旧的
> `fe-core` 胖抽象类模型（`org.apache.doris.datasource.property.storage.*`）的异同、SPI/API 设计评审、以及后续使用指南。
> 日期：2026-06-17
> 方法：6 路并行只读取证 + 3 路对抗式设计评审（解耦 / 接口工效 / 清晰度与迁移完整性），关键结论已用 `grep` 独立交叉核验。

---

## 0. 一句话结论（TL;DR）

**新模型在“架构解耦”上是教科书级别的（纯 JDK 的 api 层、零 fe-core 回边、按 provider 拆模块、运行期插件加载），但在“是否真正被使用”上目前是休眠（dormant）状态——`fe-core` 对新 `filesystem.properties.*` 的引用为 0，线上链路仍然走旧的胖类模型经 `StoragePropertiesConverter` 拍平成 `Map` 的老路。** 因此：

- 解耦/分层质量：高（9/10 级别）。
- 接口命名与一致性：偏低（4/10）——三处同名 `StorageProperties`、双接口冗余、能力模型与类型枚举尚无消费者。
- 迁移完整性：很低（3/10）——“删除 fe-core 的 StorageProperties”短期不现实，被 83 个调用方 + 转换桥 + 40 处 BE/Hadoop 配置生成点阻塞。

> **重要澄清（与提问表述的偏差，按事实修正）**
> 1. 这并不是一次简单的“搬家”。新的 `fe-filesystem` 版本是**重新设计**：旧 `StorageProperties` 是 `abstract class`，新 `StorageProperties` 是 `interface`，形状与职责都不同。
> 2. 仓库里**同时存在三套**“StorageProperties”：旧的 `fe-core`（在用）、本次新增的 `fe-filesystem`（休眠）、以及给 paimon 用的 `fe-property` 模块里的近似拷贝（commit `70e934d`，与 fe-core 版本逐字不同）。删除 fe-core 版本前，必须先把这三套理清楚。详见 §7。

---

## 1. 背景与动机

PR #63400 的目标是：**不再假设所有对象存储都能永久地通过 AWS S3 兼容协议访问**。动机（摘自 commit message）：

- AWS S3 SDK v2 2.30+ 行为变更，国产云厂商适配滞后；
- 旧版 AWS SDK 有 CVE，不可长期停留；
- Catalog FileIO 依赖 Hadoop，而 Hadoop 3.4 停维、3.5 又抬高了 AWS SDK 的最低版本要求，会反向逼迫 Doris 升级 SDK；
- OBS 私有化部署 / OBS 并行文件系统在 S3 兼容语义下出现签名错误，必须使用厂商原生 SDK。

为此该 PR 在 FE 侧引入了一套“原生 SDK 对象存储” SPI：`S3FileSystem` 保留对象存储的通用文件语义，具体 I/O 下沉到各厂商的 `ObjStorage` 实现；同时**顺带引入了一套全新的、provider 自持的、强类型存储属性模型**——这正是本报告关注的 `StorageProperties` / `FileSystemProperties` 体系。

本次 commit 共改动 **89 个文件**，但对 `fe-core` 的侵入很小（见 §6）：核心的新增内容都落在 `fe/fe-filesystem/` 的多模块树里。

---

## 2. 旧模型：`fe-core` 的胖抽象类体系

位置：`fe/fe-core/src/main/java/org/apache/doris/datasource/property/storage/`

### 2.1 结构

```
ConnectionProperties (abstract, 持有原始 Map + 反射绑定)
 └─ StorageProperties (abstract class)         ← 旧的 "StorageProperties"
     ├─ AbstractS3CompatibleProperties (implements ObjectStorageProperties)
     │   ├─ S3Properties / OSSProperties / OBSProperties / COSProperties / MinioProperties / GCSProperties / OSSHdfsProperties
     │   └─ ...
     ├─ HdfsCompatibleProperties → HdfsProperties
     ├─ AzureProperties / BrokerProperties / LocalProperties / HttpProperties / OzoneProperties
```

这是一个**继承（而非组合）**的“胖基类”：工厂、校验、BE/Hadoop 转换、类型探测全部熔进抽象基类与 `AbstractS3CompatibleProperties`，子类只重写若干钩子。

### 2.2 职责（全部塞在基类里）

| 职责 | 实现位置 |
|---|---|
| 原始参数绑定 | `@ConnectorProperty(names=…)` 注解 + `ConnectorPropertiesUtils.bindConnectorProperties`（反射）|
| 别名匹配 | 每个逻辑属性声明多个别名 key，首个命中者胜（`matchedProperties`）|
| 校验 | `checkRequiredProperties()`（反射 required 字段）+ 子类规则 |
| **类型探测 + 多实例工厂** | 静态 `createAll` / `createPrimary` 遍历硬编码的 `PROVIDERS` lambda 列表，按 `fs.xx.support` 标志或 `XxxProperties.guessIsMe(props)` 启发式匹配，并在首位兜底注入 `HdfsProperties` |
| toBE | 抽象 `getBackendConfigProperties()`（AWS_* map）+ `AbstractS3CompatibleProperties.generateBackendS3Configuration()` |
| getHadoopStorageConfig | 公有字段 `org.apache.hadoop.conf.Configuration hadoopStorageConfig`，由 `buildHadoopStorageConfig()` 懒构建 |
| 脱敏 | `ConnectorPropertiesUtils.toMaskedString`（`sensitive=true` 字段）|
| URI 规范化 | 抽象 `validateAndNormalizeUri` / `validateAndGetUri` |

工厂是“类型分发”的心脏（节选）：

```java
public static StorageProperties createPrimary(Map<String, String> origProps) {
    boolean useGuess = !hasAnyExplicitFsSupport(origProps);
    for (BiFunction<Map<String,String>, Boolean, StorageProperties> func : PROVIDERS) {
        StorageProperties p = func.apply(origProps, useGuess);
        if (p != null) { p.initNormalizeAndCheckProps(); p.buildHadoopStorageConfig(); return p; }
    }
    throw new StoragePropertiesException("No supported storage type found...");
}
// PROVIDERS = 硬编码的 HDFS/OSS/S3/OBS/COS/GCS/AZURE/MINIO/OZONE/BROKER/LOCAL/HTTP 探测 lambda 列表
```

### 2.3 旧模型的耦合问题（“为什么不能原样搬走”）

依赖方向是**反的**：旧模型住在 `fe-core` 里，却又**依赖 `fe-core` 自己的类型**：

- `common.UserException` / `DdlException` / `common.Config`（`S3Properties` 读 `Config.aws_credentials_provider_version`）；
- `cloud.proto.Cloud`（`S3Properties` 构造 `ObjectStoreInfoPB`/`CredProviderTypePB`，Cloud 模式专用）；
- `thrift.TS3StorageParam` / `TCredProviderType`（`S3Properties.getS3TStorageParam` 的 toBE thrift 结构）；
- `common.security.authentication.HadoopAuthenticator`（`HdfsProperties`）；
- `common.CatalogConfigFileUtils`（`ConnectionProperties.loadConfigFromFile`）。

最重的耦合集中在 `S3Properties`（Config 标志 + Cloud proto + thrift）和 `HdfsProperties`（Kerberos 认证栈）。其余 S3 兼容子类只碰到 `UserException` 和 Hadoop `Configuration`，相对好搬。

> 值得一提的是：**这套模型并不走 GSON 持久化**。`GsonUtils` 没有为 `ConnectionProperties`/`StorageProperties` 注册任何适配器；`CatalogProperty` 只持久化原始的 `@SerializedName("properties")` map，typed 列表是 `volatile`/transient 的，按需通过 `createAll` 重建。**所以删除旧类没有元数据格式迁移成本**——阻塞点纯粹在编译期调用方，不在持久化。

---

## 3. 新模型：`fe-filesystem` 多模块体系

### 3.1 模块与依赖方向（已 `grep` 核验，无环、单向）

```
fe-foundation        (叶子模块: @ConnectorProperty + ConnectorPropertiesUtils + ParamRules, 零 doris 依赖)
fe-extension-spi     (叶子模块: Plugin / PluginFactory)
        ▲
        │
fe-filesystem-api    (纯 JDK, "零三方依赖": FileSystem + StorageProperties/FileSystemProperties/
        ▲             BackendStorageProperties/HadoopStorageProperties + StorageKind/BackendStorageKind + capability/*)
        │
fe-filesystem-spi    (+ fe-extension-spi: FileSystemProvider<P>, ObjStorage, ObjFileSystem, S3CompatibleFileSystem)
        ▲
        │
fe-filesystem-{s3,oss,cos,obs,azure,hdfs,local,broker}   (各 provider 实现 + fe-foundation 绑定工具)

fe-core ──(compile)──► fe-filesystem-api + fe-filesystem-spi
fe-core ──(test)─────► fe-filesystem-local
```

关键事实（已核验）：

- `fe-filesystem-api` 的 pom 只在 test scope 引入 JUnit/Mockito，描述里写明 “Zero third-party external dependencies — pure JDK only”；对 `api`/`spi` 主源码做 `grep`，**没有任何 `org.apache.hadoop` / `software.amazon` / `org.apache.thrift` / `fe-core` 的 import**（api 里唯一的 `org.apache.hadoop` 字样是 `HadoopStorageProperties` 的一句 Javadoc，说明“故意不依赖它”）。
- 8 个 provider 实现模块**已从 `fe-core` 的编译 classpath 移除**（Phase 4 P4.1），改为运行期从 `plugins/filesystem/` 目录由 `FileSystemPluginManager` + `DirectoryPluginRuntimeManager` 加载（child-first + parent-first 白名单 `org.apache.doris.filesystem.`/`software.amazon.awssdk.`/`org.apache.hadoop.`）。即 **fe-core 在编译期根本无法引用任何具体 provider 类**——这是最强形态的解耦。

### 3.2 属性契约（api 层，全部是“瘦接口”）

`StorageProperties` 从“胖抽象类”变成了“瘦接口”，且只用 JDK 类型：

```java
public interface StorageProperties {
    String providerName();
    StorageKind kind();
    FileSystemType type();
    default void validate() {}
    Map<String, String> rawProperties();
    Map<String, String> matchedProperties();
    default Optional<BackendStorageProperties> toBackendProperties() { return Optional.empty(); }
    default Optional<HadoopStorageProperties> toHadoopProperties() { return Optional.empty(); }
}
```

三层接口阶梯：

- `StorageProperties`（公共契约）
- `FileSystemProperties extends StorageProperties`（“provider 自持的强类型模型”，是 `FileSystemProvider<P>` 的泛型上界）
- `S3CompatibleFileSystemProperties extends FileSystemProperties`（S3 家族共享访问器：endpoint/region/ak/sk/token/roleArn/bucket/… 全部返回原始 `String`，并把易错的 `use_path_style` 解析收敛到唯一一处 `isUsePathStyle()`，非 `true/false` 直接抛异常而不是静默当 false）。

两个“转换目标接口”刻意保持中立（只暴露 `Map<String,String>`，把 Thrift/Hadoop 依赖挡在 api 之外）：

```java
public interface BackendStorageProperties {     // 给 BE 的中立 KV；fe-core adapter 负责拼 TS3StorageParam
    BackendStorageKind backendKind();
    Map<String, String> toMap();
}
public interface HadoopStorageProperties {       // 返回 Map 而非 org.apache.hadoop.conf.Configuration
    Map<String, String> toHadoopConfigurationMap();
}
```

三个枚举处于**三个不同的抽象高度**，刻意不混用：

| 枚举 | 用途 | 取值 |
|---|---|---|
| `StorageKind` | 框架选路/分类 | `OBJECT_STORAGE / HDFS_COMPATIBLE / BROKER / LOCAL / HTTP` |
| `BackendStorageKind` | 选择 FE→BE adapter（比 StorageKind 更细）| `S3_COMPATIBLE / NATIVE / HDFS / BROKER / LOCAL` |
| `FileSystemType` | Doris fs 类型（带 TODO：承认存在 3+ 套竞品定义待统一）| `S3 / HDFS / OFS / JFS / BROKER / FILE / AZURE / HTTP` |

### 3.3 SPI 层：强类型 provider + 能力模型

```java
public interface FileSystemProvider<P extends FileSystemProperties> extends PluginFactory {
    boolean supports(Map<String,String> properties);                  // 唯一“便宜、确定”的匹配判断（abstract）
    default P bind(Map<String,String> properties) { throw new UnsupportedOperationException(...); }
    default FileSystem create(P properties) throws IOException { throw new UnsupportedOperationException(...); }
    default FileSystem createUntyped(FileSystemProperties properties) throws IOException { return create((P) properties); }
    FileSystem create(Map<String,String> properties) throws IOException;   // 兼容路径（abstract，当前唯一被 fe-core 调用的）
    default Set<String> sensitivePropertyKeys() { return Collections.emptySet(); }
    @Override default String name() { return getClass().getSimpleName().replace("FileSystemProvider",""); }
    @Override default Plugin create() { throw new UnsupportedOperationException(...); } // 来自 PluginFactory，被迫覆盖抛异常
}
```

设计意图：迁移期 `bind`/`create(P)`/`createUntyped` 是**默认抛异常的 default 方法**，未迁移的 provider 只需实现 `supports(Map)` + `create(Map)` 即可；已迁移的 provider 把 `create(Map)` 写成 `create(bind(props))`。

能力模型（`FileSystem` 接口）用 `Optional` 取代 `instanceof` 向下转型：

```java
default <T extends Capability> Optional<T> capability(Class<T> capabilityType) { return Optional.empty(); }
default <T extends Capability> T requireCapability(Class<T> capabilityType) {
    return capability(capabilityType).orElseThrow(() -> new UnsupportedOperationException(...));
}
// 可选能力接口：BatchDeleteCapability / MultipartUploadCapability / PresignedUrlCapability
```

### 3.4 具体 provider（以 `S3FileSystemProperties` 为例）

一个对象同时实现 4 个接口，`toBackend/toHadoop` 直接返回 `this`：

```java
public final class S3FileSystemProperties implements
        FileSystemProperties, BackendStorageProperties, HadoopStorageProperties, S3CompatibleFileSystemProperties {

    @Getter @ConnectorProperty(names = {ENDPOINT,"AWS_ENDPOINT","endpoint","glue.endpoint",...}, required=false)
    private String endpoint = "";
    @Getter @ConnectorProperty(names = {SECRET_KEY,"AWS_SECRET_KEY",...}, required=false, sensitive=true)
    private String secretKey = "";
    // ... region/accessKey/sessionToken/roleArn/bucket/maxConnections/usePathStyle ...

    public static S3FileSystemProperties of(Map<String,String> p) { S3FileSystemProperties x = new S3FileSystemProperties(p); x.validate(); return x; }

    @Override public void validate() {       // ParamRules 流式校验
        new ParamRules()
            .requireTogether(new String[]{accessKey, secretKey}, "s3.access_key and s3.secret_key must be set together")
            .requireAllIfPresent(externalId, new String[]{roleArn}, "s3.external_id must be used together with s3.role_arn")
            .check(() -> StringUtils.isBlank(endpoint) && StringUtils.isBlank(region), "Either s3.endpoint or s3.region must be set")
            .check(this::hasInvalidUsePathStyle, "use_path_style must be true or false...")
            .validate("Invalid S3 filesystem properties");
    }
    @Override public Optional<BackendStorageProperties> toBackendProperties() { return Optional.of(this); }
    @Override public Map<String,String> toMap() { return toFileSystemKv(); }                       // AWS_* BE map
    @Override public Map<String,String> toHadoopConfigurationMap() { /* fs.s3a.* */ }
    @Override public String toString() { return ConnectorPropertiesUtils.toMaskedString(this); }   // 脱敏
}
```

每个 S3 兼容厂商的差异点：默认调优值（S3 = 50/3000/1000，OSS/COS/OBS = 100/10000/10000）、endpoint/region 互推规则、Hadoop impl key（`fs.s3a.*` vs `fs.cosn.*` vs `fs.obs.*`）、原生 SDK 选择等。

---

## 4. 新旧对比

| 维度 | 旧（fe-core `datasource.property.storage`）| 新（fe-filesystem `filesystem.properties`）|
|---|---|---|
| `StorageProperties` 形态 | **抽象类**（`extends ConnectionProperties`）| **接口**（纯 JDK）|
| 扩展方式 | 继承胖基类 + 重写钩子 | 实现窄接口 + 组合（一个类实现 4 个接口）|
| 类型分发 | 硬编码 `PROVIDERS` lambda 列表 + `guessIsMe` 启发式（封闭，新增厂商要改中心列表）| `FileSystemProvider.supports(Map)` + ServiceLoader/插件目录发现（开放，无中心注册表）|
| Hadoop 依赖 | 基类**直接持有** `org.apache.hadoop.conf.Configuration` 字段 | api 只返回 `Map<String,String>`，由调用方/ provider 物化 Configuration |
| Thrift/Cloud 依赖 | `S3Properties` 内含 `TS3StorageParam`/`ObjectStoreInfoPB` 转换 | api 把 RPC 结构挡在外面，留给 fe-core adapter（adapter 尚未实现）|
| 模块位置 | 全在 `fe-core`，反向依赖 fe-core | 独立模块树，零 fe-core 回边 |
| 可选能力 | （无统一机制）| `FileSystem.capability(Class<T>)` / `requireCapability`（取代 instanceof）|
| 绑定/脱敏工具 | `@ConnectorProperty` + `ConnectorPropertiesUtils`（已搬到 `fe-foundation`）| **同一套**（`fe-foundation`，新旧共用）|
| 是否被线上消费 | **是**（83 个 fe-core 引用方）| **否**（0 个 fe-core 引用方，休眠）|

**注意：绑定/脱敏的反射工具（`@ConnectorProperty` / `ConnectorPropertiesUtils`）已先一步抽到叶子模块 `fe-foundation`，新旧模型都 import 它。** 这是两套模型能并存、且新模型不必依赖 fe-core 的关键基础设施。

---

## 5. SPI/API 设计评审（解耦 / 接口合理性 / 清晰度）

三路对抗式评审打分（关键论断均经 `grep` 验证）：

| 评审视角 | 维度 | 分(满10) |
|---|---|---|
| 解耦与模块边界 | fe-core 解耦 / 模块分层 / 依赖方向 / provider 独立性 | 7 / 9 / 9 / 8 |
| 接口与 SPI 工效 | 命名清晰度 / SPI 流程一致性 / 能力模型 / 新 provider 扩展性 | 4 / 4 / 5 / 6 |
| 清晰度与迁移 | 过渡期清晰度 / 新旧功能对等 / 迁移完整性 / 测试覆盖 | 4 / 5 / 3 / 7 |

### 5.1 优点（值得肯定）

1. **真正的纯净 api。** `fe-filesystem-api` 零三方依赖、零 fe-core import；`BackendStorageProperties.toMap()` / `HadoopStorageProperties.toHadoopConfigurationMap()` 都只返回 `Map<String,String>`，把 Thrift / Hadoop `Configuration` / AWS SDK 全部挡在外面。这是相对旧模型（基类内嵌 Hadoop `Configuration`）的一次干净的**依赖反转**。
2. **无环、单向的依赖图**，pom 与源码两级核验：`api ← spi(+extension-spi) ← provider(+foundation)`，`fe-core → api+spi`。没有任何 provider 模块声明 `fe-core` 依赖。
3. **fe-core 编译 classpath 已剥离到只剩 api+spi**，provider 运行期插件加载——物理上杜绝了 fe-core 在编译期引用具体 provider。
4. **脱敏解耦得很漂亮**：`sensitivePropertyKeys()`（= `ConnectorPropertiesUtils.getSensitiveKeys(XxxProperties.class)`，以 `@ConnectorProperty(sensitive=true)` 为唯一真相源）在 provider 注册时被 `FileSystemPluginManager` 聚合进 `DatasourcePrintableMap`，fe-core 无需编译期依赖任何具体 provider 属性类。
5. **`use_path_style` 解析收敛到唯一一处**且对非法值 fail-fast，是相对旧“静默当 false”的一处实打实的正确性改进。
6. **迁移友好的 default 方法策略**：未迁移 provider 只实现 `supports`+`create(Map)`，与已迁移 provider 共存，不强制 big-bang。

### 5.2 缺陷与风险（按严重度）

**[MAJOR] 整套强类型 SPI 是“到货即死代码”（dead-on-arrival）。** 已核验：fe-core 对 `filesystem.properties.*` 的 import 数 = **0**；`bind` / `createUntyped` / `toBackendProperties` / `toHadoopProperties` 在 fe-filesystem 树之外**零调用方**。线上桥 `FileSystemFactory.getFileSystem(StorageProperties)` 接收的是**旧类型**，经 `StoragePropertiesConverter.toMap()` 拍平后走 `create(Map)`。连已迁移的 typed provider 也把 `create(Map)` 内部塌缩成 `create(bind(props))`——**强类型对象从不跨越 fe-core/SPI 边界**。也就是说，SPI 一半以上的“卖点表面积”当前是脚手架。

**[MAJOR] “后续删除 fe-core StorageProperties”短期不现实。** 阻塞项（全部核验）：
- 83 个 fe-core 文件仍 import `datasource.property.storage.*`；
- 桥 `FileSystemFactory.getFileSystem(StorageProperties)` + `StoragePropertiesConverter` 仍消费旧类型；
- BE/Hadoop 配置生成仍走旧 `getBackendConfigProperties()` / `getHadoopStorageConfig()`（约 40 处，含 `CatalogProperty`、`CredentialUtils`、Paimon/Iceberg metastore 属性）；
- 旧 `S3Properties` 的 Cloud-proto / thrift 转换器在“无依赖的新 api”里**无处安放**（被刻意留给“fe-core adapter”，而该 adapter 尚不存在）。

**[MAJOR] 三处同名 `StorageProperties`。** `datasource.property.storage.StorageProperties`（旧胖类，在用）/ `property.storage.StorageProperties`（fe-property，paimon 用的近似拷贝）/ `filesystem.properties.StorageProperties`（新瘦接口）。同名不同形（两个 class + 一个 interface）在迁移边界上同时存在，IDE 自动 import、Javadoc、stack trace 都得靠包名消歧。**建议给新接口换个不同的名字**（如 `FsStorageContract` / 只保留 `FileSystemProperties`）。

**[MAJOR] 能力模型定义了却没接线。** 已核验：没有任何生产 `FileSystem`（S3/OSS/Azure…）覆盖 `capability()` 或实现 `*Capability`，唯一实现者/调用者是单测 `FileSystemCapabilityTest`。而真正在用的“可选能力”机制仍是底层 `ObjStorage` 的 `UnsupportedOperationException` 默认方法（`getStsToken`/`getPresignedUrl`/`deleteObjectsByKeys`）。于是仓库里**并存两套可选能力惯用法**，更好的那套（`capability()`）无人采用、无样例可抄。

**[MINOR] `FileSystemProperties` 相对 `StorageProperties` 零增量**——逐字重声明了全部 7 个方法，仅 Javadoc 更详细，无新方法、无行为差异。泛型上界完全可以直接写成 `<P extends StorageProperties>`。建议合并为一个接口，或给 `FileSystemProperties` 一个真正的额外方法。

**[MINOR] 分类枚举大多是“纸面值”。** `BackendStorageKind.NATIVE/HDFS/BROKER/LOCAL` 零使用；只有 `S3_COMPATIBLE` 被返回；更糟的是 `NATIVE` 的 Javadoc 用 AZURE 举例，而 `AzureFileSystemProperties.backendKind()` 返回的却是 `S3_COMPATIBLE`，**provider 自我打脸**。

**[MINOR] 别名数组手抄漂移风险。** `S3FileSystemProvider.supports()` 把 `ENDPOINT_NAMES/ACCESS_KEY_NAMES/...` 当字面量重抄了一遍，必须与 `S3FileSystemProperties` 上的 `@ConnectorProperty(names=…)` 手工保持同步。应让 `supports()` 反射读取注解别名（单一真相源）。

**[MINOR] typed 迁移在新树内部也不齐。** HDFS/Local/Broker 只实现 `create(Map)`，`bind`/`create(P)` 继承默认抛异常——任何想统一按 typed 契约编程的代码，对这三个 provider 会 runtime 抛 `UnsupportedOperationException`。

**[MINOR] fe-core 与 provider 之间仍是“魔法字符串”契约。** `StoragePropertiesConverter` 注入 `_STORAGE_TYPE_`/`AWS_*`/`AZURE_*` 等 marker key，provider 的 `supports()` 再去识别它们；这套字符串契约半在 fe-core、半在 provider，正是 typed `bind()` 想消灭、却尚未启用的东西。

**[NIT]** `PluginFactory.create()`（无参）被迫覆盖抛异常，是复用 `PluginFactory` 作发现基类带来的契约泄漏；`name()` 默认实现形同虚设（8 个 provider 全部自行覆盖）；`FileSystemType` 自带 TODO 承认 3+ 套 fs 类型定义待统一；**缺少新旧输出等价性测试**（默认调优值已分叉，正是该被 pin 的）。

---

## 6. 本次 commit 对 fe-core 的真实改动面（很小）

新模型本身**没有改动任何 fe-core 调用方**。fe-core 的全部改动是非行为性的：

- `DatasourcePrintableMap` 新增 `registerSensitiveKeys(Collection<String>)` 作为脱敏聚合 sink：
  ```java
  public static void registerSensitiveKeys(Collection<String> keys) {
      if (keys == null) return;
      synchronized (SENSITIVE_KEY) { SENSITIVE_KEY.addAll(keys); }
  }
  ```
- `FileSystemPluginManager` 在三处注册点调用上面的 sink；
- `S3Resource` / `AzureResource` 仅把 `UploadPartResult` 的 import 从 `filesystem.spi` 改到 `filesystem`（且**仍在 import 旧 `S3Properties`**，证明旧模型仍是承重的）。

线上桥本体：

```java
// fe-core: FileSystemFactory.java:113
public static org.apache.doris.filesystem.FileSystem getFileSystem(StorageProperties storageProperties) // ← 旧类型
        throws IOException {
    return getFileSystem(StoragePropertiesConverter.toMap(storageProperties));   // ← 拍平成 Map 走老路
}
```

---

## 7. 后续如何使用新模块（迁移指南）

### 7.1 写一个新的 provider（推荐姿势）

1. 新建模块 `fe-filesystem-xxx`，对 `fe-filesystem-spi`/`api` 用 `provided` scope，对 `fe-foundation` 用 `compile`。
2. 写 `XxxFileSystemProperties implements FileSystemProperties[, BackendStorageProperties, HadoopStorageProperties, S3CompatibleFileSystemProperties]`，字段用 `@ConnectorProperty(names=…, sensitive=…)` 标注，提供 `static of(Map)`（内部 `bind` + `validate`）。
3. 写 `XxxFileSystemProvider implements FileSystemProvider<XxxFileSystemProperties>`：
   ```java
   @Override public XxxFileSystemProperties bind(Map<String,String> p) { return XxxFileSystemProperties.of(p); }
   @Override public FileSystem create(XxxFileSystemProperties p) { return new XxxFileSystem(p); }
   @Override public FileSystem create(Map<String,String> p) { return create(bind(p)); }
   @Override public Set<String> sensitivePropertyKeys() {
       return ConnectorPropertiesUtils.getSensitiveKeys(XxxFileSystemProperties.class);
   }
   ```
4. 在 `META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider` 注册，按 `plugin-zip.xml` 打包（jar 在 zip 根供 ServiceLoader 扫描，依赖放 `lib/`，api/spi/extension-spi 用 provided 不打进 lib/）。

### 7.2 接口调用示例（均取自单测，可直接对照）

**(a) 强类型主流程 `bind → create(P)`：**
```java
OssFileSystemProvider provider = new OssFileSystemProvider();
OssFileSystemProperties props = provider.bind(Map.of("oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com"));
FileSystem fs = provider.create(props);
assertEquals("OSS", props.providerName());
assertEquals(StorageKind.OBJECT_STORAGE, props.kind());
assertInstanceOf(OssFileSystem.class, fs);          // OssFileSystem extends S3CompatibleFileSystem
```

**(b) 类型擦除的逃生口 `createUntyped`（静态类型未知时）：**
```java
@SuppressWarnings("unchecked")
default FileSystem createUntyped(FileSystemProperties properties) throws IOException {
    return create((P) properties);   // 依赖注册表已用 supports() 预选到匹配 provider，否则运行期 ClassCastException
}
```

**(c) 兼容/遗留路径 `create(Map)`（当前 fe-core 唯一实际走的）：**
```java
@Override public FileSystem create(Map<String,String> properties) throws IOException {
    return create(bind(properties));
}
```

**(d) 可选能力 `capability` / `requireCapability`：**
```java
// 消费者
PresignedUrlCapability cap = fs.requireCapability(PresignedUrlCapability.class);   // 不存在则抛 UnsupportedOperationException(含类型名)
Optional<PresignedUrlCapability> maybe = fs.capability(PresignedUrlCapability.class);
// provider 侧实现
@Override public <T extends Capability> Optional<T> capability(Class<T> t) {
    if (t == PresignedUrlCapability.class) return Optional.of(t.cast(presignedUrl));
    return Optional.empty();
}
```

**(e) 转换视图 `toBackendProperties` / `toHadoopProperties`：**
```java
BackendStorageProperties be = S3FileSystemProperties.of(raw).toBackendProperties().orElseThrow();
assertEquals(BackendStorageKind.S3_COMPATIBLE, be.backendKind());
assertEquals("https://minio.local", be.toMap().get("AWS_ENDPOINT"));   // BE 侧 AWS_* map

Map<String,String> hadoop = S3FileSystemProperties.of(raw).toHadoopProperties().orElseThrow().toHadoopConfigurationMap();
assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", hadoop.get("fs.s3a.impl"));   // fs.s3a.* map
```

**(f) 原始/匹配视图 + 脱敏：**
```java
S3FileSystemProperties p = S3FileSystemProperties.of(raw);
p.rawProperties();                                   // 原始输入
p.matchedProperties().get("s3.endpoint");            // 实际命中的别名子集
p.toString();                                        // secretKey=*** / sessionToken=*** ，accessKey/endpoint 明文
new S3FileSystemProvider().sensitivePropertyKeys();  // 含 s3.secret_key/s3.session_token，不含 access_key
```

**fe-core 侧脱敏闭环（无编译期 provider 依赖）：**
```java
manager.registerProvider(provider);   // 内部把 provider.sensitivePropertyKeys() 折叠进 DatasourcePrintableMap.SENSITIVE_KEY
```

### 7.3 要真正“替换旧模型”，必须做的事（按优先级）

1. **先翻桥，再谈解耦**：改写 `FileSystemFactory` / `StoragePropertiesConverter`，让它用 `provider.bind()` + `createUntyped()` 传递强类型 `FileSystemProperties`，从而让 `toBackendProperties()`/`toHadoopProperties()` 真正“活”起来。在此之前，整套 typed api/spi 只能算脚手架，不是已交付的抽象。
2. **公布 83 个调用方的迁移序列**（建议 TVF → catalog → backup/resource），并把 40 处 BE/Hadoop 配置生成点从旧 `getBackendConfigProperties`/`getHadoopStorageConfig` 切到新转换视图。
3. **理清三套树**：明确 `fe-property`（paimon）与 `fe-filesystem` 的关系——是被新 api 收编，还是作为独立产物保留，需白纸黑字写下来，否则“单一真相源”无从谈起。
4. **补齐 HDFS/Local/Broker 的 typed `bind()`/`create(P)`**，或显式声明它们永久 map-only。
5. **加新旧等价性测试**：对代表性的 S3/OSS/COS/OBS/Azure/HDFS 输入，断言新 `toMap()`/`toHadoopConfigurationMap()` 与旧 `getBackendConfigProperties()`/`getHadoopStorageConfig()` 的 key/value 一致（含默认 region/timeout 调优值），守住未来切换的回归。
6. **接线一个能力做样板**（如 `S3FileSystem` 暴露 `PresignedUrlCapability`），否则能力模型一直是“有定义无样例”。
7. **加架构守门测试**（ArchUnit 或 CI grep gate）：断言 api/spi 不 import `org.apache.hadoop`/`software.amazon`/`org.apache.thrift`/`org.apache.doris.{catalog,common,qe}`，把当前已核验的干净边界锁死，防回归。
8. **改名消除三同名冲突**（成本极低、收益高，应在更多调用方引用新类型之前落地）。

---

## 8. 附：本报告关键事实的独立核验（grep）

| 论断 | 核验结果 |
|---|---|
| fe-core import 新 `filesystem.properties` 的文件数 | **0** |
| fe-core import 旧 `datasource.property.storage` 的文件数 | **83** |
| `StoragePropertiesConverter.java` 是否存在 | 是 |
| 第三套 `fe-property/.../property/storage/StorageProperties.java` 是否存在 | 是 |
| 生产 `FileSystem` 覆盖 `capability()` | 无（仅 api 默认 + 单测）|
| fe-core 调用 `toBackendProperties`/`createUntyped` 次数 | **0** |
| 线上桥 `getFileSystem(StorageProperties)` 入参类型 | 旧类型，经 `StoragePropertiesConverter.toMap()` |

---

*报告依据 commit `2a113a6` 的工作区状态生成。docker/e2e 未运行；本报告为静态代码与设计层面的分析。*
