# 23. 把引擎上下文里的存储服务收成独立服务对象（高危）

> **优先级**：第五优先级（高危，排在整条工作线最后） ｜ **风险**：高 ｜ **前置依赖**：任务 06（必须先合入）
> **影响模块**：`fe-connector-spi`（接口拆分 + 单测）、`fe-connector-hive`、`fe-connector-iceberg`、`fe-connector-paimon`、`fe-connector-hudi`、`fe-connector-jdbc`（只涉及改名那一小步）、`fe-core`（只加两处接线，不搬逻辑）
> **预计改动规模**：约 22～25 个文件；新增 1 个接口文件（约 270 行，javadoc 原样搬过去）、`ConnectorContext` 减约 265 行、连接器侧 35 处生产调用点 + 9 个测试替身的机械替换。净增长接近于零。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

`ConnectorContext` 是引擎交给连接器的唯一服务入口，今天它把 19 个方法压在一个接口上，其中 11 个全是「存储与 BE 侧」的事（凭证、地址归一、文件系统、broker、BE 探测……）。本任务把这 11 个方法收进一个独立的服务接口 `ConnectorStorageContext`，`ConnectorContext` 只留一个取得器；顺带把长在这个中立接口上的 `sanitizeJdbcUrl` 改成中立命名并把它的安全契约写准。

## 二、背景：现在的代码是怎么写的

### 2.1 一个接口装了两类完全不相干的东西

`fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorContext.java:36` 共 19 个方法（文件 415 行）。按职责分成两堆，界线非常干净：

**引擎装配与运行时服务（8 个，留在 `ConnectorContext`）**

| 方法 | 位置 | 做什么 |
|---|---|---|
| `getCatalogName()` / `getCatalogId()` | `:39` / `:42` | 目录身份，唯一两个抽象方法 |
| `getEnvironment()` | `:54` | FE 进程级配置项表 |
| `getHttpSecurityHook()` | `:63` | 对外 HTTP 前后的安全钩子 |
| `sanitizeJdbcUrl(String)` | `:79` | 出站地址消毒（见 2.5） |
| `executeAuthenticated(Callable)` | `:98` | 把操作包进目录的认证上下文 |
| `getMetaInvalidator()` | `:109` | 元数据失效通知（任务 14 要删） |
| `createSiblingConnector(String, Map)` | `:147` | 异构网关的兄弟连接器工厂 |

**存储与 BE 侧服务（11 个，本任务要搬走）**

| 方法 | 位置 | 做什么 |
|---|---|---|
| `vendStorageCredentials(Map)` | `:165` | 把 REST 目录发的临时凭证归一成 BE 认的键 |
| `normalizeStorageUri(String)` | `:188` | 把连接器的原生路径归一成 BE 认的规范 scheme |
| `normalizeStorageUri(String, Map)` | `:208` | 上一条的「带临时凭证」重载 |
| `newStorageUriNormalizer(Map)` | `:230` | 上一条的批量形式（每次扫描只推导一次存储配置） |
| `getBackendFileType(String, Map)` | `:255` | 告诉 BE 用哪一族文件系统打开输出路径 |
| `getBrokerAddresses()` | `:291` | broker 写入时的 broker 地址 |
| `getBackendStorageProperties()` | `:314` | 目录静态凭证归一成 BE 认的键 |
| `testBackendStorageConnectivity(int, Map)` | `:337` | 建目录时让 BE 探一次存储可达性 |
| `getStorageProperties()` | `:362` | 目录的类型化存储配置（`fe-filesystem` 的契约对象） |
| `getFileSystem(ConnectorSession)` | `:390` | 引擎持有的按 scheme 路由的文件系统 |
| `cleanupEmptyManagedLocation(String, List)` | `:412` | 删表后清理空目录壳 |

存储那 11 个方法的 javadoc（`:151`～`:415`，约 265 行）占了整个文件的三分之二。

### 2.2 谁在用这 11 个方法

全仓库检索连接器生产代码（`fe/fe-connector/*/src/main`），共 **35 处**调用点，集中在 4 个连接器：

| 连接器 | 调用点数 | 具体位置 |
|---|---|---|
| hive | 14 | `HiveScanPlanProvider.java:153/157/258/405/619`、`HiveWritePlanProvider.java:153/155/269/327/328/339`、`HiveConnectorTransaction.java:756`、`HiveConnectorMetadata.java:910/942` |
| iceberg | 14 | `IcebergWritePlanProvider.java:614/622/635/674/679`、`IcebergScanPlanProvider.java:1464/1585/1598`、`IcebergConnector.java:443/450/899/1205`、`IcebergConnectorMetadata.java:938/1087` |
| paimon | 4 | `PaimonScanPlanProvider.java:735/823/837`、`PaimonConnector.java:437` |
| hudi | 3 | `HudiScanPlanProvider.java:331/424/939` |

其余连接器（es、jdbc、maxcompute、trino）**一处都不用**——它们没有存储概念，却同样看着这 11 个方法。

引擎侧的实现只有一个：`fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:78`（598 行）。这 11 个方法的实现体占 `:200`～`:507`，加上私有辅助方法一直到 `:566`，以及 5 个字段（`:91` 存储配置供给器、`:96` 原始存储属性供给器、`:103` 文件系统锁、`:104` 缓存的文件系统、`:105` 关闭标记）。文件系统还与生命周期绑定：`close()`（`:371`）关掉缓存的文件系统，由 `PluginDrivenExternalCatalog` 的私有方法 `closeConnectorContextQuietly`（声明在 `:1374`）关掉，调用点在 `:1401`（目录销毁）与 `:158`（重建上下文时关掉旧的）。

### 2.3 为什么这次改动被定为高危：两个逐方法转发的钉桩包装类

iceberg 与 paimon 各有一个装饰器，把线程上下文类加载器钉到插件加载器上（这是本项目已经踩过四次的类加载器分裂事故的防护）：

- `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/TcclPinningConnectorContext.java:74`——`executeAuthenticated` 带钉桩逻辑在 `:98-114`，其后 `:116-210` 全是「纯转发」，其中存储那一段是 `:156-209`；
- `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/TcclPinningConnectorContext.java:63`——钉桩在 `:76-92`，存储转发在 `:134-178`。

已核实的转发缺口（也是任务 06 的内容）：iceberg 覆写 18/19，漏 `getFileSystem`；paimon 覆写 17/19，漏 `getFileSystem` 与 `newStorageUriNormalizer`。这两个类正文里明确写了哪些方法**不需要**钉桩（地址归一、BE 连通性探测完全跑在引擎侧，见 iceberg `:173-177` 与 `:203`）。

**风险就在这里**：这个接口一动，两个类都要跟着动；而它们承载的是「远程提交时 iceberg-aws 按类名反射加载 S3 客户端」这类只有真跑起来才暴露的机制，单测覆盖不到全部。所以必须做插件包重部署冒烟。

### 2.4 测试替身的规模

`ConnectorContext` 全仓有 25 个实现：8 个具名 + 17 个匿名。其中生产实现只有 3 个（`DefaultConnectorContext` 与 iceberg / paimon 两个钉桩包装类），另 **22 个是测试替身**。真正覆写了存储方法的替身共 9 处：

`fe-connector-iceberg/src/test/.../RecordingConnectorContext.java:45`（8 个存储覆写：98 / 104 / 109 / 116 / 127 / 137 / 142 / 183 行）、`fe-connector-hive/src/test/.../RecordingConnectorContext.java:37`（6 个）、`fe-connector-paimon/src/test/.../RecordingConnectorContext.java:39`（3 个），以及 `IcebergConnectorTestConnectionTest`、`HiveConnectorTransactionTest`、`HiveScanBatchModeTest`、`PaimonScanPlanProviderTest`、`HudiBackendDescriptorTest`（2 处匿名）里的内联替身。它们全部标了 `@Override`（已核实）。

`fe-core` 的 7 个 `DefaultConnectorContext*Test`（BackendStorageProps / Cleanup / FileSystem / NormalizeUri / Sibling / StorageProps / Vend）全部用**具体类型**声明变量（已逐个核实），所以只要引擎实现类同时实现新接口，这 7 个测试**一行都不用改**。

### 2.5 顺带要处理的 `sanitizeJdbcUrl`

`ConnectorContext:79` 上挂着 `sanitizeJdbcUrl(String)`，javadoc 写的契约是：

```
Connectors MUST call this method before using any JDBC URL to establish a database connection.
```

已核实的事实：

- 引擎实现走 `SecurityChecker.getInstance().getSafeJdbcUrl(...)`（`DefaultConnectorContext.java:186-192`），失败抛异常；
- 全仓**唯一**的调用者是 `fe-connector-jdbc`：`JdbcDorisConnector.java:241` 把 `context::sanitizeJdbcUrl` 作为**方法引用**传进 `JdbcConnectorClient.create(...)`，客户端在 `JdbcConnectorClient.java:182` 建连前应用它；
- iceberg 的 JDBC 元存储（`IcebergCatalogFactory.java:601-602`）与 paimon 的 JDBC 目录（`PaimonCatalogFactory.java:196` 把 `uri` 直接塞进 paimon 的 `JdbcCatalogFactory`）都把用户地址原样交给第三方 SDK 建连，从不经过这个钩子。

**这不是迁移引入的回退**：上游 master 的老代码同样没有在这两条路上做检查。所以这里要修的是「契约写得比实现宽」，不是补一个安全漏洞。

## 三、为什么这是个问题

1. **接口没有告诉读者哪些方法与他有关。** 一个新连接器作者（比如接一个纯 JDBC 源）拿到的服务入口有 19 个方法，其中 11 个是他这辈子都用不到的存储与 BE 概念，而接口本身没有任何分组或说明。这条工作线的目标是「照着接口定义就能清晰实现」，19 个方法一锅端是这个目标的直接障碍。
2. **每加一个存储服务，要改的地方是 4 处而不是 2 处。** 加一个存储方法今天必须动：接口、引擎实现、iceberg 包装类、paimon 包装类。任务 06 把后两处收成一处转发基类之后是 3 处。收成独立服务对象之后是 **2 处**（新接口 + 引擎实现），包装类与转发基类完全不必再动。
3. **钉桩包装类的表面积越大，漏钉桩的概率越高。** 今天两个包装类逐方法手抄，存储那 11 个占了它们正文的一半以上。把这 11 个搬进一个子对象后，包装类对存储的转发从 11 个方法塌成 1 个取得器——**结构上不可能再漏**，也顺带把任务 06 里那个 `getFileSystem` 缺口在存储侧永久消灭。
4. **`sanitizeJdbcUrl` 的问题是双重的**：名字把一个通用的出站地址检查钩子写成了 JDBC 专有（中立接口不该出现协议名），契约又用 MUST 承诺了一件没有强制点、且实测只有 1 个连接器遵守的事。读者按字面理解会以为 FE 侧所有外部连接都过了这道检查，实际不是。

**注意这不是正确性缺陷**：改完之后没有任何一条查询的结果会变化，收益是接口可读性与「以后改动只需碰 2 处」。这也是它排在最后的原因——**收益不紧急，代价（重部署冒烟）不小**。

## 四、用一个最小例子说明

假设明天要给引擎加一个存储服务：`getStorageStats(String location)`（返回某个目录的大小，用于统计）。

| 时间点 | 我必须改哪些文件 | 漏改的后果 |
|---|---|---|
| 今天 | ① `ConnectorContext`（加带默认实现的方法）② `DefaultConnectorContext`（真实现）③ iceberg `TcclPinningConnectorContext`（加一行转发）④ paimon `TcclPinningConnectorContext`（加一行转发） | 漏了 ③ 或 ④ **不报编译错**：这两个连接器静默拿到接口默认值，且这次调用没有类加载器钉桩 |
| 任务 06 合入后 | ① 接口 ② 引擎实现 ③ `ForwardingConnectorContext`（转发基类，一处） | 漏了 ③ 会被基类单测抓住 |
| **本任务合入后** | ① `ConnectorStorageContext` ② `DefaultConnectorContext` | 包装类与转发基类**不需要动**：它们只转发一个 `getStorageContext()`，存储服务的增删与它们无关 |

`sanitizeJdbcUrl` 那一半用一段 SQL 就能说清。两条语句里的地址形态完全一样：

```sql
-- 甲：走 fe-connector-jdbc。地址在建连前经过引擎的出站地址检查，内网地址会被拒。
CREATE CATALOG c1 PROPERTIES (
  "type" = "jdbc",
  "jdbc_url" = "jdbc:mysql://10.0.0.5:3306/db", ...);

-- 乙：走 fe-connector-paimon 的 jdbc 目录。地址被原样交给 paimon 的 JdbcCatalogFactory 建连。
CREATE CATALOG c2 PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "jdbc",
  "uri" = "jdbc:mysql://10.0.0.5:3306/db", ...);
```

| 接口文档说了什么 | 实际发生什么 | 应该怎么写 |
|---|---|---|
| 「使用任何 JDBC 地址建连之前**必须**调用」 | 甲调用了；乙没有（第三方 SDK 内部建连，连接器手里没有建连时机） | 「连接器**自行**建立连接时必须调用；第三方 SDK 内部建连不在本钩子覆盖范围内」 |
| 方法名叫 `sanitizeJdbcUrl` | 引擎实现做的是通用出站地址安全检查，与 JDBC 协议无关 | 改成中立名 `sanitizeOutboundUrl` |

## 五、解决方案

### 5.1 目标状态

**第一步（主体）**：在 `fe-connector-spi` 新增 `ConnectorStorageContext`，把上面表格里那 11 个方法**连 javadoc 一起原样搬过去**（一个字不改，只改 `{@link}` 的目标），保留它们现有的默认实现；`ConnectorContext` 只留一个取得器：

```java
public interface ConnectorStorageContext {

    /** 什么都不管的默认实现：目录没有存储机制时用它，语义与今天各方法的默认值逐字一致。 */
    ConnectorStorageContext NOOP = new ConnectorStorageContext() { };

    default Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) { … }
    default String normalizeStorageUri(String rawUri) { … }
    default String normalizeStorageUri(String rawUri, Map<String, String> rawVendedCredentials) { … }
    default UnaryOperator<String> newStorageUriNormalizer(Map<String, String> rawVendedCredentials) { … }
    default String getBackendFileType(String rawUri, Map<String, String> rawVendedCredentials) { … }
    default List<ConnectorBrokerAddress> getBrokerAddresses() { … }
    default Map<String, String> getBackendStorageProperties() { … }
    default void testBackendStorageConnectivity(int storageBackendTypeValue,
            Map<String, String> backendProperties) throws Exception { … }
    default List<StorageProperties> getStorageProperties() { … }
    default FileSystem getFileSystem(ConnectorSession session) { … }
    default void cleanupEmptyManagedLocation(String location, List<String> tableChildDirs) { … }
}

public interface ConnectorContext {
    // …目录身份 / 环境变量 / HTTP 钩子 / 出站地址消毒 / 认证 / 失效通知 / 兄弟连接器工厂…

    /**
     * 本目录的存储与 BE 侧服务。目录不由引擎管理存储时返回 {@link ConnectorStorageContext#NOOP}
     * （不返回 null）。返回值在目录生命周期内是稳定的，连接器可以在构造时取一次存下来。
     */
    default ConnectorStorageContext getStorageContext() {
        return ConnectorStorageContext.NOOP;
    }
}
```

`NOOP` 常量沿用既有先例：本模块的 `ConnectorMetaInvalidator.java:34`，以及 `fe-connector-api` 的 `ConnectorHttpSecurityHook.java:56`（后者归属哪个模块本身未定，见 5.3，这里只借它的写法）。

**引擎侧不搬代码**：`DefaultConnectorContext` 改成 `implements ConnectorContext, ConnectorStorageContext, Closeable`，并加一个 `getStorageContext() { return this; }`。这样：

- `fe-core` 里 11 个方法的实现体、5 个字段、`close()` 与文件系统的生命周期绑定**一行都不动**（避免把 `:330-384` 那段有锁有关闭标记的代码搬来搬去）；
- `fe-core` 的 7 个 `DefaultConnectorContext*Test` 一行都不用改（它们用具体类型）；
- 完全符合「`fe-core` 只出不进」：新增的只有两处签名接线，约 6 行。

**钉桩包装类怎么处理**：任务 06 合入后，两个 `TcclPinningConnectorContext` 已经继承 `ForwardingConnectorContext`、正文里没有任何存储转发。本任务只需在**转发基类**里把 11 个转发换成 1 个 `getStorageContext()` 转发。语义与今天逐字等价——今天这 11 个转发全是不带钉桩的纯直通（两个类的注释明确说明了原因），改完之后连接器直接拿到引擎的存储上下文，中间没有装饰层，行为一致。

基类 javadoc 里要补一句残留风险：**如果将来某个存储方法需要钉桩，钉桩子类必须自己包一层存储上下文**（把 `getStorageContext()` 覆写成返回一个自己的装饰器）。今天没有这种方法，所以不预先造这层包装。

**第二步（顺带）**：`sanitizeJdbcUrl` → `sanitizeOutboundUrl`，并按第四节表格重写 javadoc。它**留在 `ConnectorContext` 上**，不进 `ConnectorStorageContext`（跟存储无关），也**不能挪进 `ConnectorValidationContext`**——那个接口（`fe-connector-api/.../ConnectorValidationContext.java:31`，6 个方法全抽象）只在建目录校验期存在，而这个钩子是运行时创建客户端时以方法引用形式传给长生命周期客户端的（`JdbcDorisConnector.java:241` → `JdbcConnectorClient.java:182`），校验期上下文早已消失。

### 5.2 改动清单

| 文件 | 要做什么 |
|---|---|
| `fe-connector-spi/.../spi/ConnectorStorageContext.java` | **新增**。11 个方法 + javadoc 从 `ConnectorContext:151-415` 原样搬入；加 `NOOP` 常量；类注释说明「这是引擎实现、连接器消费的存储侧服务，新增存储服务加在这里，不要加回 `ConnectorContext`」 |
| `fe-connector-spi/.../spi/ConnectorContext.java` | 删掉 `:151-415` 那 11 个方法；加 `getStorageContext()` 默认返回 `NOOP`；`sanitizeJdbcUrl` 改名 `sanitizeOutboundUrl` 并重写契约段 |
| `fe-connector-spi/.../spi/ForwardingConnectorContext.java`（任务 06 产出） | 11 个存储转发换成 1 个 `getStorageContext()` 转发；`sanitizeJdbcUrl` 转发跟着改名；类注释补「存储方法若将来需要钉桩，子类须包装存储上下文」 |
| `fe-connector-spi` 的 `ConnectorContextTest.java` | `:47`（存储配置默认空）与 `:58`（BE 文件类型默认按 scheme 推导）两组断言移到新增的 `ConnectorStorageContextTest`；`createSiblingConnector` 那组留在原处 |
| `fe-core/.../connector/DefaultConnectorContext.java` | 类声明加 `ConnectorStorageContext`；加 `getStorageContext(){ return this; }`；`sanitizeJdbcUrl` 改名。**不搬任何逻辑** |
| `fe-connector-hive`（4 个文件 14 处）、`fe-connector-iceberg`（4 个文件 14 处）、`fe-connector-paimon`（2 个文件 4 处）、`fe-connector-hudi`（1 个文件 3 处） | 调用点改成经存储上下文调用。调用点 ≥3 处的文件加一个私有取得器（如 `private ConnectorStorageContext storage() { return context.getStorageContext(); }`），把 `context.getFileSystem(session)` 写成 `storage().getFileSystem(session)`。**现有的 `context != null ? … : …` 判空保持不动**（`HiveScanPlanProvider:619`、`HudiScanPlanProvider:424`、`PaimonScanPlanProvider:735`、`IcebergScanPlanProvider:1464` 这四处是离线单测走的分支） |
| `fe-connector-jdbc/.../JdbcDorisConnector.java:241` | 方法引用改成 `context::sanitizeOutboundUrl`（`JdbcConnectorClient` 侧的参数名/注释顺带改成中立措辞） |
| 3 个 `RecordingConnectorContext`（hive/iceberg/paimon）+ 6 处内联匿名替身 | 让替身同时实现 `ConnectorStorageContext` 并 `getStorageContext(){ return this; }`；覆写的存储方法原地不动 |

**顺序建议**（每一步都能独立 `test-compile` 通过）：先加新接口并让 `ConnectorContext` 的 11 个方法暂时保留为「转调 `getStorageContext()`」的过渡默认实现 → 逐个连接器迁调用点与测试替身 → 最后从 `ConnectorContext` 删掉这 11 个方法并收拾转发基类 → 独立一个 commit 做改名。

**这次迁移是编译期强制的**（与任务 06 那个静默缺口相反）：接口方法一删，所有测试替身上的 `@Override` 立刻编译失败，编译器会把每一处需要复查的替身点出来。已核实 9 处替身全部标了 `@Override`。

### 5.3 明确不要顺手做的事

- **不要把 `DefaultConnectorContext` 真的拆成两个类。** 那 300 多行搬家会把带锁的文件系统缓存与 `close()` 生命周期一起搬走，风险与本任务的收益（SPI 表面可读性）不成比例；`fe-core` 的内部结构也不是这条工作线的目标。让它同时实现两个接口、返回 `this` 就够了。
- **不要顺手给存储方法加类加载器钉桩。** 现有两个包装类明确写了这些方法完全跑在引擎侧、不需要钉桩。无差别加钉桩会改变现有行为并带来无谓开销。
- **不要顺手把 `getHttpSecurityHook` 和改名后的出站地址消毒再收成第三个服务对象。** 只有 2 个方法，收益不足；`getHttpSecurityHook` 的归属（在 `api` 还是 `spi`）是另一条任务的事。
- **不要顺手给 iceberg / paimon 的 REST / JDBC 目录补上出站地址检查。** 那是一项独立的安全增强（要动第三方 SDK 的建连路径），与上游 master 行为一致、不是本次迁移的回退；混进本任务会让「行为不变」这个验收前提失效。
- **不要顺手删 `getMetaInvalidator`**（任务 14）**或改动本次范围外的默认值政策**（任务 07）。本任务只搬位置 + 一次改名。
- **不要为「存储方法是否搬齐」写 shell / 正则门禁。** 本仓库已有结论：这类门禁只适合存在性与前缀类不变量。这里编译器本身就是门禁。
- **不要变动 `ConnectorPluginManager.java:60` 的 `CURRENT_API_VERSION`。** 它至今是 1，从未随任何一次 SPI 改动递增；本工作线里 10 / 11 / 13 / 14 号任务同样在改 SPI 表面。真正的保障是「FE 与插件包一起重新构建、一起部署」，见第六节。

## 六、怎么验证

### 6.1 编译（最强的单一符号级信号）

```bash
# 全反应堆含测试源编译，禁止跳过测试编译
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T1C test-compile
```

这一步同时承担「有没有漏改测试替身」的检查：接口方法删掉后任何遗留的 `@Override` 都会失败。

### 6.2 单元测试

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
    -pl fe-connector/fe-connector-spi,fe-connector/fe-connector-hive,fe-connector/fe-connector-iceberg,\
fe-connector/fe-connector-paimon,fe-connector/fe-connector-hudi,fe-connector/fe-connector-jdbc,fe-core \
    -Dmaven.build.cache.enabled=false test
```

（必须禁用 build cache，否则 surefire 会被静默跳过、`BUILD SUCCESS` 是空的。）

要断言到的东西：

1. **新接口的默认值逐字不变**：`ConnectorStorageContextTest` 断言 `NOOP` 的 11 个方法返回值与今天 `ConnectorContext` 的默认值一致——尤其 `getBackendFileType` 按 scheme 推导的四条分支（`hdfs`/`viewfs` → `FILE_HDFS`、`file` 与无 scheme → `FILE_LOCAL`、其余 → `FILE_S3`），这些断言直接从 `ConnectorContextTest:58` 搬过来。
2. **`getStorageContext()` 不返回 null**：断言未覆写它的匿名上下文返回 `NOOP`，让连接器侧不需要判空。
3. **转发基类**：任务 06 建立的反射驱动单测继续跑通，并新增一条——空子类的 `getStorageContext()` 返回的是被包装上下文给出的那个实例（不是 `NOOP`）。这条要做**变异验证**：手工删掉基类里的 `getStorageContext()` 转发，确认测试失败并指出方法名，然后恢复。
4. **两个钉桩包装类的现有断言全部保留不改**：钉桩生效、任务抛异常时恢复调用方加载器、非 Kerberos 走被包装上下文的认证、Kerberos 走插件侧 `doAs`、`createSiblingConnector` 转发给原始上下文。它们是「认证与钉桩语义没被继承结构调整改坏」的证据。
5. **测试替身迁移不能把断言弱化**：替身改成实现两个接口后，逐个确认原断言仍然**能失败**——挑 3 处代表（hive 的 `getFileSystem`、iceberg 的 `getBackendFileType`、paimon 的 `normalizeStorageUri`），临时把替身的返回值改错，确认对应测试红。这一步是必须的：如果某个替身漏了 `getStorageContext()` 却仍编译通过（它没覆写过存储方法的情况），连接器会拿到 `NOOP`，某些断言可能从「验证归一化结果」退化成「验证原样返回」而依旧变绿。
6. **改名**：全仓复扫 `sanitizeJdbcUrl` 命中数必须为 0（今天有 7 处：接口、引擎实现、两个包装类各 2 行、jdbc 调用点各 1 行）。

### 6.3 插件包重部署冒烟（本任务的核心把关，不可省）

原因：改的是 parent-first 前缀（`ConnectorPluginManager.java:64-65` 声明 `org.apache.doris.connector.` 与 `org.apache.doris.filesystem.` 走 parent-first）内的接口。FE 与插件包必须**一起重建、一起部署**；混用旧插件 zip 会在运行时报 `AbstractMethodError` / `NoSuchMethodError` 而不是启动期拒绝。

步骤：

1. `mvn -f …/fe/pom.xml package`，取各插件模块 `target/doris-fe-connector-<type>.zip`（由 `src/main/assembly/plugin-zip.xml` 生成）；
2. 清空并重新解包到 `connector_plugin_root`（默认 `${DORIS_HOME}/plugins/connector`），确认目录里没有上一版残留的 jar；
3. 重启 FE，日志里确认 `ConnectorPluginManager initialized … registered types: [...]` 列出全部类型；
4. 至少跑通下面这几条（每条都覆盖一类被改到的存储服务）：

| 冒烟项 | 覆盖什么 |
|---|---|
| iceberg 目录 `INSERT`（对象存储 warehouse） | 写路径的 BE 文件类型 + 地址归一 + 静态凭证；**同时是 iceberg-aws 按类名反射建 S3 客户端的那条路**，钉桩失效会在这里 `ClassCastException` |
| iceberg `DROP TABLE`（HMS 托管位置） | 空目录清理 |
| iceberg Kerberos 目录的一次读 + 一次写 | 钉桩与「连接器单一认证方」语义，这是唯一活的端到端认证把关点 |
| paimon 目录一次带临时凭证的扫描（REST 目录） | 临时凭证归一 + 批量地址归一器 |
| hive 目录一次分区表扫描 + 一次 `INSERT` | 引擎文件系统（今天唯一真在用它的连接器，14 处调用点里 6 处是它） |
| hudi 目录一次扫描 | BE 存储属性 + 地址归一 |
| `CREATE CATALOG … "test_connection" = "true"`（iceberg，S3 warehouse） | BE 连通性探测 |

5. 观察 FE 日志无 `ClassCastException` / `NoClassDefFoundError` / `AbstractMethodError`。

### 6.4 端到端回归

本任务不改变任何运行时行为，端到端只作兜底。跑受影响的四个连接器的既有 `external_table_p0` / `external_table_p2` 子集（hive、iceberg、paimon、hudi）与 jdbc 的目录用例（改名影响它的建连路径）。**不需要新增端到端用例。**

## 七、风险与回退

- **最大风险：钉桩失效但单测全绿。** 缓解只有一条——6.3 的重部署冒烟必须真跑，尤其 iceberg 的写入与 Kerberos 两项。单测能证明「转发到位」，不能证明「反射加载落在插件侧」。
- **风险：测试替身静默弱化断言。** 见 6.2 第 5 条，用挑样变异验证兜住。这是本任务最容易出现的隐性质量损失。
- **风险：混合部署（新 FE + 旧插件 zip）。** 表现为运行时 `AbstractMethodError`。缓解：冒烟前清空 `connector_plugin_root`；在 commit 信息里写明「插件包必须与 FE 同版本部署」。
- **风险：与任务 06 的顺序倒置。** 若在 06 之前做，本任务要在两个包装类里各手改 11 处转发，风险显著上升且要重复两遍相同的判断。**06 未合入就不要开工。**
- **风险：与任务 14 撞车。** 14 号删 `getMetaInvalidator`，同样改转发基类。两者不冲突（一个删非存储方法，一个搬存储方法），但**不要合在一个 commit 里**，否则冒烟出问题时无法二分。
- **回退**：改动全部是结构性的（搬位置 + 改名 + 调用点替换），无数据格式、无持久化、无 thrift 有线格式牵连（新接口保持零 thrift：BE 文件类型仍返回枚举名字符串，broker 地址仍用中立的 `ConnectorBrokerAddress`）。直接 revert 相应 commit 即可，但**必须连同插件包一起回退重部署**。

## 八、相关背景

- 调研报告 `../audit-report.md`：
  - 第 6.1 节的接口规模表中 `ConnectorContext` 那一行——415 行 / 19 个方法 / 9 类能力；以及第 6.3 节建议里「`ConnectorContext` 把存储相关的方法收成一个服务对象，**这一批高危**，必须做插件包重部署冒烟」那一条；
  - 第十五节整治路线表的第 11 批「装配上下文拆分」——风险标「高」，原因写明必须做插件包重部署冒烟以验证线程上下文类加载器的钉法；
  - 附录 A 第 106 条（大杂烩接口，19 方法 9 类能力）、第 117 条（`sanitizeJdbcUrl` 契约只有一个连接器遵守，判定「成立」）、第 32 条（协议命名的中立性问题，判定「部分成立」，建议改名）、第 125 条（HTTP 安全钩子有同类的契约过宽问题，但明确不是迁移引入的回退）；
  - 附录 D.2（两个钉桩包装类的转发缺口机理）——那是任务 06 的来源，也是本任务风险评级的依据。
- 同一任务空间：**任务 06**（转发基类，本任务的前置）、任务 07（把公共模块的设计规则写下来，包括「新增存储服务加在哪里」这条应写进新接口所在模块的包级说明）、任务 14（删推模型失效接口，同改转发基类，排在本任务前后皆可但不要同 commit）。
- 项目记忆：`catalog-spi-plugin-tccl-classloader-gotcha`（四个已修的类加载器分裂位置，解释为什么钉桩相关改动必须重部署验证）、`fe-core-source-isolation-iron-rules`（`fe-core` 只出不进，本任务据此选择「引擎实现类同时实现两个接口」而不是搬代码）、`static-gate-only-for-existence-not-language-semantics`（为什么不写静态门禁）、`doris-build-verify-gotchas`（maven 绝对路径 `-f`、后台任务退出码的读法）。
