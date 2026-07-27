# 06. 补齐两个连接器对引擎上下文的转发缺口，并根治「加一个方法就漏一次类加载器钉桩」的机理

> **优先级**：第一优先级（潜伏事故，非当前可观测的线上错误） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-spi`（新增一个转发基类 + 一个单测）、`fe-connector-iceberg`、`fe-connector-paimon`。**不改 `fe-core`**。
> **预计改动规模**：新增 2 个文件（约 200 行，含注释），两个包装类各净减约 80～90 行；合计 4～5 个文件。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

iceberg 与 paimon 各有一个「把线程上下文类加载器钉到插件加载器上」的包装类，它们逐方法手抄转发引擎上下文 `ConnectorContext`，今天各漏抄了 1～2 个方法；漏抄不报编译错、只会静默返回接口默认值（取文件系统的默认值是 `null`）。本任务补齐这两处缺口，并用一个公共转发基类 + 一个反射驱动的单测，把「以后每加一个方法就再漏一次」的机理堵掉。

## 二、背景：现在的代码是怎么写的

### 2.1 引擎上下文的 19 个方法里只有 2 个是抽象的

`fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorContext.java` 是「引擎实现、连接器消费」的接口，共 19 个方法，其中只有 `getCatalogName()`（`:39`）和 `getCatalogId()`（`:42`）是抽象的，其余 17 个都带默认实现，而这些默认值的语义一律是**静默降级**：

| 方法 | 位置 | 默认行为 |
|---|---|---|
| `sanitizeJdbcUrl` | `:79-81` | 原样返回，不做任何地址安全检查 |
| `executeAuthenticated` | `:98-100` | 直接跑任务，不套任何认证上下文 |
| `getMetaInvalidator` | `:109-111` | 返回空操作 |
| `newStorageUriNormalizer` | `:230-232` | 退化成逐次调用 `normalizeStorageUri`，丢掉「每次扫描只推导一次存储配置」的优化 |
| `getBackendStorageProperties` / `getStorageProperties` / `getBrokerAddresses` | `:314` / `:362` / `:291` | 返回空 |
| **`getFileSystem`** | **`:390-392`** | **返回 `null`** |
| `cleanupEmptyManagedLocation` | `:412-414` | 什么都不做 |

真正实现它的引擎类只有一个：`fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:78`。

### 2.2 两个包装类为什么存在

iceberg 与 paimon 的插件把 `hadoop-common`、`fe-kerberos`（iceberg 还有 `iceberg-aws`）按 child-first 打进插件包，所以插件内部任何**按类名反射**的加载（默认用线程上下文类加载器）如果跑在引擎线程的默认加载器下，就会拿到 fe-core 那一份副本，与插件自己那一份互相 `ClassCastException`。为此两个连接器各有一个装饰器，把 `executeAuthenticated` 包起来，在任务执行期间把线程上下文类加载器钉到插件加载器，Kerberos 目录还额外在插件侧的 `doAs` 里跑：

- `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/TcclPinningConnectorContext.java:74`（`executeAuthenticated` 在 `:98-114`，其余「纯转发」段落在 `:116-210`）
- `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/TcclPinningConnectorContext.java:63`（`executeAuthenticated` 在 `:76-92`，纯转发段落在 `:94-179`）

装饰器在连接器构造时套一次，之后整个连接器只看得见包装后的上下文：`IcebergConnector.java:225`、`PaimonConnector.java:153`。

### 2.3 实测的转发缺口

按符号逐个比对（已在 HEAD 上核实）：

| 包装类 | 覆写的 `ConnectorContext` 方法数 | 漏掉的方法 |
|---|---|---|
| iceberg | 18 / 19 | `getFileSystem(ConnectorSession)` |
| paimon | 17 / 19 | `getFileSystem(ConnectorSession)`、`newStorageUriNormalizer(Map)` |

漏掉的方法会落到接口默认实现上：连接器调 `context.getFileSystem(session)` 拿到的是 `null`，而不是引擎那个按 catalog 缓存的文件系统。

### 2.4 hive 已经在真的用引擎文件系统

hive 连接器（它**没有**这种包装类，直接持有引擎注入的上下文）已经有 6 个直接调用点：`HiveScanPlanProvider.java:153`、`:157`、`:258`，`HiveConnectorMetadata.java:910`、`:942`，`HiveConnectorTransaction.java:756`；其中 transaction 那一处是私有 helper，内部再扩散到约 19 个读写目录的位置。这个 helper 对 `null` 是失败退出的：

```java
// HiveConnectorTransaction.java:755-761
private FileSystem getFileSystem() {
    FileSystem engineFs = context.getFileSystem(session);
    if (engineFs == null) {
        throw new DorisConnectorException("No engine FileSystem available for hive write transaction "
                + transactionId + " (catalog has no storage properties)");
    }
```

注意这句报错文案把原因归给了「目录没有存储属性」。

### 2.5 同一类接口存在三套政策

- `ConnectorContext`（引擎实现）：2 抽象 + 17 默认；
- `ConnectorValidationContext`（同样是引擎实现，`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorValidationContext.java`）：`:34`、`:37`、`:40`、`:50`、`:59`、`:73` 共 6 个方法，**全抽象**；
- `ConnectorProvider`（连接器实现，`fe-connector-spi/.../ConnectorProvider.java`）：`:46`、`:64` 抽象，`:52`、`:74`、`:79`、`:84`、`:93` 默认（方向正确）。

公共模块里没有任何一处成文规则说明这三套差异从何而来。（把规则写下来是任务 07 的事，本任务只做机理修复。）

## 三、为什么这是个问题

**先把严重性说准**：今天 iceberg 与 paimon 都还没有调用 `context.getFileSystem(...)`（全仓库检索，只有 hive 在用），paimon 也还在用逐次的 `normalizeStorageUri(rawUri, token)`（`PaimonScanPlanProvider.java:735`）而没有用批量归一器。所以这**不是一个当前用户能观测到的错误**，是潜伏缺陷。这一点必须写清楚，避免下一位实施者按「线上故障」去找复现。

它真正的问题有三层：

1. **一旦被使用就是难查的故障。** iceberg/paimon 哪天开始用引擎文件系统（这是本项目明确的方向：连接器不再自带 Hadoop `FileSystem`，由引擎按 scheme 路由），拿到的是 `null`。如果照 hive 的写法做 `null` 检查，用户看到的报错会是「catalog has no storage properties」——**指向一个完全无关的原因**，而目录的存储属性其实是好的；如果没做检查，就是一个空指针。
2. **paimon 少的那个批量归一器是静默性能回退。** 行为仍然正确，但每个数据文件都会重新推导一遍存储配置（`StorageProperties.createAll` + 一次 hadoop config 构建），把「每次扫描一次」变回「每文件一次」，没有任何日志。
3. **机理本身是事故源，而且会复发。** 这两个类存在的唯一理由就是钉类加载器。而它们采用「实现接口 + 手抄每个方法」的写法，于是**每往 `ConnectorContext` 加一个带默认实现的方法，这两个类都会默认漏掉那一次转发**，编译器一句话都不会说。本项目已经反复踩过类加载器分裂事故（扫描线程、写/DDL 引擎线程、iceberg 内部 manifest 写线程池、HMS 客户端创建点，四个位置各修过一次），这是同一类事故的下一个入口。

## 四、用一个最小例子说明

### 例子一：连接器写了一行取文件系统的代码

假设 paimon 连接器要清理一个写失败留下的临时目录，于是照 hive 的写法写：

```java
FileSystem fs = context.getFileSystem(session);   // 连接器代码，编译通过
fs.delete(Location.of(tmpPath), true);
```

| 连接器作者写了什么 | 今天实际发生什么 | 应该发生什么 |
|---|---|---|
| `context.getFileSystem(session)` | 调用落到包装类上；包装类没有覆写这个方法 → 走接口默认实现 → **返回 `null`** → 下一行空指针 | 转发到引擎上下文 → 拿到该 catalog 的引擎文件系统 |
| 编译期 | 无任何提示 | 无提示（这也是问题：所以要靠单测兜住） |
| 排错时看到的线索 | 空指针，或者（若模仿 hive 加了检查）「catalog has no storage properties」——把作者引向去查目录属性 | 不该发生 |

### 例子二：明天给引擎上下文加一个新方法

假设某个新需求要在引擎上下文上加一个 `getTableLocationResolver()`，它内部会进插件代码：

```java
// 有人在 ConnectorContext 里加：
default TableLocationResolver getTableLocationResolver() { return TableLocationResolver.NOOP; }
```

作者改了 `DefaultConnectorContext`，跑通了 hive 的端到端用例（hive 不走包装类），提交。**iceberg 与 paimon 从此静默拿到 `NOOP`，且这次调用不再有类加载器钉桩。** 这就是本任务要堵的那条缝：不是这一次谁忘了，而是这个写法保证了以后每次都会忘。

## 五、解决方案

### 5.1 目标状态

在公共模块 `fe-connector-spi` 提供一个转发基类，两个钉桩包装类改为**继承它、只覆写真正需要特殊处理的方法**；再用一个反射驱动的单测钉住「基类必须覆写接口的每一个方法，且每次调用都必须原样到达被包装的上下文」。

签名草案（`org.apache.doris.connector.spi.ForwardingConnectorContext`）：

```java
/**
 * 装饰 ConnectorContext 的基类：逐方法转发给被包装的上下文。
 * 需要在某个方法上做额外处理（例如把线程上下文类加载器钉到插件加载器）的装饰器，
 * 只覆写那个方法，其余方法由本类保证转发。
 *
 * 新增 ConnectorContext 方法时必须同时在本类补一个转发，
 * ForwardingConnectorContextTest 会强制这件事。
 * 如果新方法会进入插件代码，钉桩子类还必须覆写它并加钉桩。
 */
public abstract class ForwardingConnectorContext implements ConnectorContext {

    private final ConnectorContext delegate;

    protected ForwardingConnectorContext(ConnectorContext delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /** 被包装的原始引擎上下文（子类需要绕过自身装饰时用它）。 */
    protected final ConnectorContext delegate() {
        return delegate;
    }

    @Override public String getCatalogName() { return delegate.getCatalogName(); }
    // …… 其余 18 个方法逐一转发，一个不漏 ……
}
```

改完之后，两个包装类的正文只剩：构造函数（多传一个插件加载器与认证器）、`executeAuthenticated` 的钉桩实现、iceberg 那个包私有的 `getPluginAuthenticator()`（`:94-96`，写路径要拿它把认证器带进 FileIO，必须保留）。

**为什么选转发基类，而不是把方法改成抽象**：

| 方案 | 代价 | 是否根治 |
|---|---|---|
| 转发基类（推荐） | 新增 1 个公共类；两个包装类各减约 85 行 | 是。补一处即两个包装类同时受益；配合单测，新增方法时会被强制处理 |
| 把「引擎必须履约」的方法（地址消毒、认证包装、取文件系统）改成抽象 | 全仓库有 25 个 `ConnectorContext` 实现（8 个具名 + 17 个匿名），其中 22 个是测试替身（17 个匿名全在测试源，具名测试替身 5 个），只有 3 个是生产实现（`DefaultConnectorContext` 与两个钉桩包装类）；改抽象要逐个补空实现，`getFileSystem` 变抽象还会逼每个离线测试替身去编造一个文件系统 | **否**。包装类仍然要为每个新方法手抄一次转发，钉桩照旧会漏；而且以后每加一个抽象方法就一次性打断 25 个实现 |

所以推荐第一条路；同时**不**顺手改抽象/默认的划分（那属于任务 07 的规则梳理，本任务只在基类的注释里写清「新增方法必须补转发」）。

残留风险要写在基类注释里：基类只保证「不丢转发」，不保证「不丢钉桩」。如果新方法会进入插件代码，钉桩子类必须自己覆写并加钉桩——单测失败时的提示语要把这句话直接写出来，让改接口的人当场做这个判断。

### 5.2 改动清单

| 文件 | 要做什么 |
|---|---|
| `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ForwardingConnectorContext.java` | 新增。逐方法转发全部 19 个方法；`protected final ConnectorContext delegate()` 暴露原始上下文；类注释写明「新增接口方法必须在此补转发」与「进插件代码的方法还需子类钉桩」 |
| `fe/fe-connector/fe-connector-spi/src/test/java/org/apache/doris/connector/spi/ForwardingConnectorContextTest.java` | 新增。见第六节 |
| `.../fe-connector-iceberg/.../TcclPinningConnectorContext.java` | 改为 `extends ForwardingConnectorContext`；删除 `:116-210` 全部纯转发方法；`executeAuthenticated` 内部把 `delegate.executeAuthenticated(task)` 换成 `delegate().executeAuthenticated(task)`；保留类注释与 `getPluginAuthenticator()`；`createSiblingConnector` 的「必须转发给原始上下文而非本装饰器」语义由基类天然满足，把原有那段解释性注释移到基类或就地保留一句说明 |
| `.../fe-connector-paimon/.../TcclPinningConnectorContext.java` | 同上（`:94-179` 纯转发方法删除，`executeAuthenticated` 同样改用 `delegate()`） |
| 两个连接器已有的 `TcclPinningConnectorContextTest` | 保留现有断言（钉桩、异常时恢复调用方加载器、Kerberos 单一认证方、sibling 转发给原始上下文）。补一条断言：`getFileSystem(session)` 与（paimon）`newStorageUriNormalizer(...)` 的返回值来自被包装的上下文，而不是接口默认值 |

顺序建议：先加基类与基类单测（此时两个包装类不动，编译通过），再逐个迁移包装类；每一步都能独立编译验证。

### 5.3 明确不要顺手做的事

- **不要把 `ConnectorContext` 的任何默认方法改成抽象。** 理由见 5.1 的对照表（25 个实现、其中 22 个测试替身）。抽象/默认政策的统一是任务 07 的范围。
- **不要给 hive 连接器加这种包装类。** hive 直接用引擎注入的上下文，本来就没有这个缺口；plain-hive 的类加载器钉桩在别的位置（HMS 客户端创建点与 `HiveConf` 构造点）已经解决，不要在这里重做一遍。
- **不要顺手给基类的转发方法加钉桩。** 现有两个包装类明确写了哪些方法不需要钉桩（存储地址归一化、后端连通性探测完全跑在引擎侧）。无差别加钉桩会改变现有行为并带来无谓开销。
- **不要动 `ConnectorSession` 的默认值问题**（`getStatementScope` 默认不记忆等）。那是同类问题的另一个接口，属于另一条任务，本任务不扩。
- **不要写 shell / 正则门禁去校验「包装类是否覆写齐全」。** 本仓库已有结论：这类门禁只适合存在性与前缀类不变量，要理解 Java 语义就等于在 shell 里写解析器，误报比漏报更毒。这里用运行时单测。
- **不要顺手删 `getMetaInvalidator`**。删除推模型失效接口是任务 14，它同样要改这两个包装类；本任务先做完，任务 14 届时只需在基类删一个转发方法。
- 不要改 `fe-core`：本任务全部改动落在公共模块与两个插件里，符合「fe-core 只出不进」。

## 六、怎么验证

### 6.1 基类单测：反射 + 动态代理，逐方法断言「原样到达」

`ForwardingConnectorContextTest` 的做法（放在 `fe-connector-spi`，与既有的 `ConnectorContextTest` 同目录；该模块已有 `junit-jupiter` 依赖，动态代理用 JDK 自带的 `java.lang.reflect.Proxy`，无需新依赖）：

1. 用 `Proxy.newProxyInstance` 造一个记录型 `ConnectorContext`，记录每次被调用的 `Method`（名字 + 参数类型）与实参，并返回该返回类型的一个可区分的取值（例如文件系统返回一个 `Proxy` 出来的非 `null` 实例、字符串返回带标记的串）。
2. 造一个空的匿名子类 `new ForwardingConnectorContext(recording) {}`。
3. 反射枚举 `ConnectorContext.class.getMethods()`，对每个方法用按类型编造的实参调用一次（`ConnectorSession` 传 `null`，接口文档允许；`Callable` 传一个返回标记值的任务）。
4. 断言：**每次调用都在记录器上留下了同一个方法**（按名字 + 参数类型精确比对，不能只比名字），且实参与返回值原样穿过。

这个测法为什么能真正抓住缺陷（逐一对应今天的两个缺口）：

- 少覆写 `getFileSystem` → 走接口默认 → 记录器上没有任何记录 → **失败**；
- 少覆写 `newStorageUriNormalizer` → 接口默认返回一个 lambda、当场不碰被包装对象 → 记录器上没有记录 → **失败**；
- 少覆写 `normalizeStorageUri(String, Map)` → 接口默认转调单参版本 → 记录器上记录的是**单参**方法 → 因为按参数类型精确比对，**失败**（这正是「只比方法名会漏」的那种情况）；
- 少覆写 `getBackendFileType` / `testBackendStorageConnectivity` / `cleanupEmptyManagedLocation` → 默认实现自己算或什么都不做 → 无记录 → **失败**。

**变异验证（必须做）**：从基类里手工删掉任意一个转发方法，跑该测试，确认它报错并且报错信息指出了是哪个方法；恢复。至少对 `getFileSystem` 和 `normalizeStorageUri(String, Map)` 各做一次。

### 6.2 两个连接器的包装类单测

保留现有断言不变（这是行为不变的证据）：钉桩生效、任务抛异常时恢复调用方加载器、非 Kerberos 走被包装上下文的认证、Kerberos 走插件侧 `doAs` 且不再调被包装上下文的认证、`createSiblingConnector` 转发给原始上下文。各补一条：

- iceberg：`ctx.getFileSystem(null)` 返回的对象与记录型上下文给出的同一个（今天返回 `null`，改前应先确认这条新断言在旧代码上是失败的）。
- paimon：同上，并额外断言 `ctx.newStorageUriNormalizer(token)` 返回的是被包装上下文给出的那个归一器实例（不是接口默认的 lambda）。

### 6.3 编译与测试命令

```bash
# 最强单一信号：全反应堆含测试源编译（禁止跳过测试编译）
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T1C test-compile

# 跑本任务相关单测（必须禁用 build cache，否则 surefire 会被静默跳过、BUILD SUCCESS 是空的）
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
    -pl fe-connector/fe-connector-spi,fe-connector/fe-connector-iceberg,fe-connector/fe-connector-paimon \
    -Dmaven.build.cache.enabled=false test
```

注意 checkstyle 会扫测试源，新文件要按仓库现有风格写（许可头、import 顺序）。

### 6.4 端到端回归

本任务不改变任何现有运行时行为（补的两个方法今天无调用点，其余方法转发路径逐字等价），**不需要新增端到端用例**。既有的端到端把关点是 iceberg 的 Kerberos 回归套件——它验证 `executeAuthenticated` 的钉桩与单一认证方语义没被这次继承结构调整改坏；paimon 没有活的 Kerberos 套件，依靠上面的单测与 iceberg 套件同机理覆盖（这一点两个包装类的现有类注释已经写明）。

## 七、风险与回退

- **风险：继承结构调整改坏认证语义。** `executeAuthenticated` 是唯一带逻辑的方法，迁移时只把 `delegate` 字段访问换成 `delegate()`，其余一字不改；两个连接器现有的四条认证/钉桩断言全部保留，任何行为偏移都会被它们抓到。
- **风险：类加载器层面的问题。** 基类放在 `org.apache.doris.connector.spi`，落在 `ConnectorPluginManager.java:65` 声明的 parent-first 前缀内（`org.apache.doris.connector.`），与 `ConnectorContext` 本身同一份副本，插件里的子类继承它不产生第二份类。这与两个包装类今天实现 `ConnectorContext` 的加载路径完全一致。
- **风险：给公共模块增加了一个公共类。** 这个类是「引擎实现、连接器消费」这一侧的官方装饰基类，与该模块的定位一致，且它减少的是「以后每次改接口要动的地方」，方向与本条工作线的目标（新增连接器不必改公共模块）一致。
- **回退**：改动自包含在 1 个新增类 + 1 个新增测试 + 2 个插件文件里，直接 revert 这一个提交即可，无数据格式、无持久化、无有线格式牵连。

## 八、相关背景

- 调研报告 `../audit-report.md`：
  - **附录 D.2** —— 引擎侧接口默认值全是静默降级、两个包装类漏转发：本任务的直接来源，含三套政策的对比；
  - 第 3.2 节「五个结构性问题」总表里编号为六的那一行 —— 同一条问题的一句话版；
  - **附录 D.3** —— 会话接口同病、有默认值会静默关掉性能优化：同一类问题在 `ConnectorSession` 上的表现（`getStatementScope` 默认不记忆会静默关掉按语句去重）。本任务**不**处理它，但下一位实施者应知道它是同一机理的另一处。
- 同一任务空间：任务 07（把两个公共模块的设计规则写下来，包括抽象/默认的政策）、任务 14（删除推模型失效接口，同样要改这两个包装类，**排在本任务之后**）。
- 项目记忆：`catalog-spi-plugin-tccl-classloader-gotcha`（四个已修的类加载器分裂位置，解释了为什么漏一次钉桩是真事故）、`static-gate-only-for-existence-not-language-semantics`（为什么这里用单测而不是 shell 门禁）、`doris-build-verify-gotchas`（maven 绝对路径 `-f`、后台任务退出码的读法）。
