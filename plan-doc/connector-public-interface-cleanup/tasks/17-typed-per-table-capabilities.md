# 17. 把按表能力从字符串升级为类型化集合，并删掉写特性的镜像方法

> **优先级**：第四优先级（兑现承诺） ｜ **风险**：中 ｜ **前置依赖**：07 号（能力声明的三层规则由它写下来；本任务是按那份规则去改代码。07 号只动注释，不做也能编译，但先做能省一次返工）
> **影响模块**：`fe-connector-api`、`fe-connector-hive`、`fe-core`（含 `fe-core` 与各连接器的测试源）
> **预计改动规模**：约 20 个文件；第一、二部分合计净删约 60 行、新增约 90 行；第三部分净删约 40 行、新增约 60 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

---

## 一、一句话说明这个任务要解决什么

「一个连接器怎么告诉引擎它支持某项可选能力」这件事上，公共接口里有两处形状不对：**按表细化的能力靠一串逗号分隔的字符串塞在属性表里**（拼错不报错、而且只有一小部分能力真的生效），**写路径的 7 项特性在入口接口上又被镜像了一遍**（同一个事实有两个可覆写的入口，一旦分叉没有任何测试能发现）。本任务把前者换成类型化的能力集合、把后者换成公共模块里的静态派生函数，并把「哪些能力可以按表细化、哪些天生只能按目录声明」这件事在枚举上逐条写清楚。

---

## 二、背景：现在的代码是怎么写的

### 2.1 连接器级能力：类型化集合

`Connector.getCapabilities()`（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/Connector.java:211`）返回 `Set<ConnectorCapability>`，默认空集。`ConnectorCapability`（同目录 `ConnectorCapability.java:30`）今天有 **13 个常量**：`SUPPORTS_MVCC_SNAPSHOT`、`SUPPORTS_PASSTHROUGH_QUERY`、`SUPPORTS_PARTITION_STATS`、`SUPPORTS_COLUMN_AUTO_ANALYZE`、`SUPPORTS_TOPN_LAZY_MATERIALIZE`、`SUPPORTS_SHOW_CREATE_DDL`、`SUPPORTS_VIEW`、`SUPPORTS_NESTED_COLUMN_PRUNE`、`SUPPORTS_METADATA_PRELOAD`、`SUPPORTS_USER_SESSION`、`SUPPORTS_SAMPLE_ANALYZE`、`SUPPORTS_SORT_ORDER`、`SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE`。这一条通道本身没问题。

### 2.2 按表能力：一串逗号分隔的字符串

`ConnectorTableSchema.java:98` 定义了一个保留属性键：

```java
public static final String PER_TABLE_CAPABILITIES_KEY = INTERNAL_KEY_PREFIX + "connector.per-table-capabilities";
```

键名展开是 `__internal.connector.per-table-capabilities`，它和另外 6 个保留控制键一起登记在 `RESERVED_CONTROL_KEYS`（`ConnectorTableSchema.java:118-121`），值是**能力枚举常量名拼成的逗号分隔串**，塞进 `ConnectorTableSchema` 的 `Map<String, String> properties` 里（构造器在 `:128`）。

**唯一的生产写入方是 hive 连接器**，两处：

- `HiveConnectorMetadata.java:522-541`：对普通 hive 表，按文件格式逐项判断后写入 `SUPPORTS_COLUMN_AUTO_ANALYZE` / `SUPPORTS_SAMPLE_ANALYZE` / `SUPPORTS_TOPN_LAZY_MATERIALIZE`（判定谓词在 `:2209` / `:2228` / `:2239`）。
- `HiveConnectorMetadata.java:566-588` 的 `reflectSiblingScanCapabilities`（调用点 `:487`）：对 iceberg-on-HMS / hudi-on-HMS 这类由兄弟连接器代管的表，把**兄弟连接器的整个 `getCapabilities()` 集合**逐个 `name()` 拼进这个串。

**唯一的生产读取方是引擎的一个私有方法** `PluginDrivenExternalTable.hasScanCapability`（`fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalTable.java:302`）：

```java
if (connector.getCapabilities().contains(capability)) {
    return true;
}
String csv = rawTableProperties().get(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY);
...
for (String name : csv.split(",")) {
    if (name.trim().equals(capability.name())) { return true; }
}
return false;
```

它的调用方只有 5 处，对应 5 个能力：`supportsColumnAutoAnalyze`（`:239`）、`supportsTopNLazyMaterialize`（`:251`）、`supportsNestedColumnPrune`（`:264`）、`supportsNestedColumnSchemaChange`（`:276`）、`supportsSampleAnalyze`（`:289`）。

字符串从连接器传到引擎的载体是进程内的 schema 缓存值 `PluginDrivenSchemaCacheValue.tableProperties`（`fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenSchemaCacheValue.java:57`），由 `PluginDrivenExternalTable.toSchemaCacheValue`（`:491`，末尾把整个 `tableSchema.getProperties()` 原样交给缓存值）填入，读取入口是私有的 `rawTableProperties()`（`:752`）。已核实 `SchemaCacheValue` 及其子类**没有任何 Gson 注解、不参与元数据持久化**，所以改字段没有镜像兼容负担。

### 2.3 其余 8 个能力：只读连接器级集合，完全绕过字符串通道

除 `hasScanCapability` 之外，引擎侧还有 **9 处**直接 `connector.getCapabilities().contains(...)`：

| 位置 | 读的能力 | 作用域 |
|---|---|---|
| `PluginDrivenExternalTable.java:337` | `SUPPORTS_SHOW_CREATE_DDL` | 表 |
| `PluginDrivenExternalTable.java:353` | `SUPPORTS_VIEW` | 表 |
| `PluginDrivenExternalTable.java:438` | `SUPPORTS_METADATA_PRELOAD` | 表 |
| `ShowPartitionsCommand.java:350`（私有方法 `hasPartitionStatsCapability`，被 `:293` 与 `:390` 两处调用） | `SUPPORTS_PARTITION_STATS` | 表（但 `:390` 的 `getMetaData()` 路径上没有解析出表对象） |
| `PluginDrivenExternalCatalog.java:316` | `SUPPORTS_VIEW` | 目录（在列表名字，此时没有表） |
| `PluginDrivenExternalCatalog.java:1289` | `SUPPORTS_USER_SESSION` | 目录 |
| `PluginDrivenExternalDatabase.java:60` | `SUPPORTS_MVCC_SNAPSHOT` | 库（决定新建哪个表子类，此时表还没建出来） |
| `QueryTableValueFunction.java:78` | `SUPPORTS_PASSTHROUGH_QUERY` | 目录（表值函数，没有表） |
| `CreateTableInfo.java:794` | `SUPPORTS_SORT_ORDER` | 目录（建表语句分析期，表还不存在） |

上表是 `fe-core` 侧的全部。连接器内部另有一处同形状的读取：`HiveConnector.java:543` 读兄弟连接器的 `SUPPORTS_USER_SESSION` 做越权守卫。它不属于引擎侧统一入口的范围，本任务不动它。

### 2.4 写路径的 7 项特性在两个接口上各有一份

真源是 `ConnectorWritePlanProvider`（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/write/ConnectorWritePlanProvider.java`）上的 7 个默认方法：`supportedOperations()`（`:159`）、`supportsWriteBranch()`（`:164`）、`requiresParallelWrite()`（`:174`）、`requiresFullSchemaWriteOrder()`（`:184`）、`requiresPartitionLocalSort()`（`:194`）、`requiresPartitionHashWrite()`（`:210`）、`requiresMaterializeStaticPartitionValues()`（`:220`）。三个连接器覆写它们（`IcebergWritePlanProvider:321-345`、`HiveWritePlanProvider:124-141`、`MaxComputeWritePlanProvider:145-162`）。

而 `Connector` 上又镜像了 **11 个**同名方法：`supportedWriteOperations()`（`:115`）、`supportedWriteOperations(handle)`（`:126`）、`supportsWriteBranch()`（`:132`）、`supportsWriteBranch(handle)`（`:138`）、`requiresParallelWrite()`（`:144`）、`requiresFullSchemaWriteOrder()`（`:150`）、`requiresPartitionLocalSort()`（`:156`）、`requiresPartitionHashWrite()`（`:162`）、`requiresPartitionHashWrite(handle)`（`:168`）、`requiresMaterializeStaticPartitionValues()`（`:177`）、`requiresMaterializeStaticPartitionValues(handle)`（`:183`）。方法体一律是同一个形状：

```java
default boolean requiresParallelWrite() {
    ConnectorWritePlanProvider p = getWritePlanProvider();
    return p != null && p.requiresParallelWrite();
}
```

已实测：**没有任何连接器覆写这 11 个方法中的任何一个**（`fe-core` 的 `ConnectorWriteDelegationTest:67` 那处覆写是覆写在 provider 上的，不是覆写在 `Connector` 上）。调用方全部在引擎侧与契约校验器里：`PluginDrivenExternalTable:178/206/223/367/388/403/423`、`PhysicalPlanTranslator:630/682`、`ConnectorContractValidator:42-70`。

7 项特性里只有 4 项有「按表」重载（`supportedWriteOperations` / `supportsWriteBranch` / `requiresPartitionHashWrite` / `requiresMaterializeStaticPartitionValues`），另外 3 项（`requiresParallelWrite` / `requiresFullSchemaWriteOrder` / `requiresPartitionLocalSort`）没有——这是历史上按需一个个加出来的，不是设计。

---

## 三、为什么这是个问题

**（1）字符串通道没有编译期约束，写错就静默失效。** 能力名写成 `"SUPPORTS_TOPN_LAZY_MATERIALIZ"`（少一个字母），编译通过、启动通过、`SHOW CREATE TABLE` 也看不出来，只是这张表永远拿不到 Top-N 延迟物化——退化成一次性能损失，没有任何报错。

**（2）接口文档承诺的范围和实现范围不一致。** `ConnectorTableSchema.java:82-97` 的注释写的是「本表支持的能力名」，语气上任何能力都可以按表细化；实际只有 5 个能力的读取路径会看这个串（第 2.2 节的 5 个调用方），另外 8 个只读连接器级集合。**而且哪 5 个生效这件事没有写在任何地方**，只能靠反查引擎里那个私有方法的调用方才能知道。

**（3）hive 网关往里塞的东西大部分被静默丢弃。** `reflectSiblingScanCapabilities` 把 iceberg 兄弟的整个能力集合都拼进串里。iceberg 的集合里含 `SUPPORTS_SHOW_CREATE_DDL` / `SUPPORTS_VIEW` / `SUPPORTS_METADATA_PRELOAD` / `SUPPORTS_SORT_ORDER` / `SUPPORTS_MVCC_SNAPSHOT`（`IcebergConnector.java:849-857`），这些在引擎侧一个都不会被读到。「多发的会被丢掉」这件事现在是**引擎读取范围窄**这个实现细节在兜着，而不是连接器明确表达了意图——它同时也是下面这个改动的陷阱：一旦引擎把读取范围扩大，这些被丢弃的能力会**突然生效**，iceberg-on-HMS 表的 `SHOW CREATE TABLE`、视图判定、元数据预载行为会在没人评审过的情况下改变。

**（4）名字是错的。** `hasScanCapability` 里的 "scan" 早就不成立：它服务的 5 个调用方里有 `ALTER TABLE` 嵌套列演进（`supportsNestedColumnSchemaChange`）和统计信息采集（`supportsColumnAutoAnalyze` / `supportsSampleAnalyze`），跟扫描无关。

**（5）写特性的镜像方法给同一个事实开了第二个可覆写入口。** 今天 0 个连接器覆写它们，所以没出事——但那是运气，不是设计保证。假设某个连接器作者在 `Connector` 上覆写 `requiresParallelWrite()` 返回 `true`，而没有同步改自己的写计划提供者：`PhysicalConnectorTableSink` 走 `Connector` 这条路会拿到 `true`，任何直接问 provider 的地方拿到 `false`，两个答案分叉。**这种分叉没有任何测试能捕获**，因为两条路各自都「符合自己的契约」。这也直接违背 07 号要写下的第二层规则：子系统内部的开关只挂在拥有它的那个 provider 上，禁止在入口接口上做镜像转发。

---

## 四、用一个最小例子说明

**例子一：连接器写了什么 vs 引擎实际怎么理解。**

| 连接器写下的东西 | 今天实际发生什么 | 应该发生什么 |
|---|---|---|
| `props.put(键, "SUPPORTS_SAMPLE_ANALYZE")` | 生效 | 生效（类型化后写成 `EnumSet.of(SUPPORTS_SAMPLE_ANALYZE)`） |
| `props.put(键, "SUPPORTS_SAMPLE_ANALYZ")`（拼错） | **静默失效**：这张表永远不能 `ANALYZE ... WITH SAMPLE`，无任何日志 | **编译不过**（枚举常量不存在） |
| hive 把 iceberg 兄弟的 `SUPPORTS_SHOW_CREATE_DDL` 也拼进串里 | 引擎不读这一项，**静默丢弃**；iceberg-on-HMS 表的 `SHOW CREATE TABLE` 不走连接器渲染 | hive 只反射它确实想让表继承的那几项，不发多余的；行为不变但意图写在代码里 |

**例子二：镜像方法怎么分叉。** 假设新连接器作者这样写（完全合法、编译通过、评审也很难看出）：

```java
class MyConnector implements Connector {
    // 作者以为这是"声明我的写能力"的地方
    @Override public boolean requiresParallelWrite() { return true; }
    @Override public ConnectorWritePlanProvider getWritePlanProvider() { return new MyWriteProvider(); }
}
class MyWriteProvider implements ConnectorWritePlanProvider {
    // 而这里没写，默认 false
}
```

于是：`connector.requiresParallelWrite()` 得到 `true`，`connector.getWritePlanProvider().requiresParallelWrite()` 得到 `false`。改成静态派生函数之后，上面那个 `@Override` **根本写不出来**——`Connector` 上没有这个方法可覆写，唯一的真源由语言保证。

---

## 五、解决方案

### 5.1 目标状态

**（一）按表能力改成类型化集合。** `ConnectorTableSchema` 新增一个能力字段，字符串键删除：

```java
// fe-connector-api：ConnectorTableSchema
private final Set<ConnectorCapability> tableCapabilities;

/** 不做按表细化的连接器沿用这个构造器（能力集合为空）。 */
public ConnectorTableSchema(String tableName, List<ConnectorColumn> columns,
        String tableFormatType, Map<String, String> properties);

/** 需要按表细化的连接器（今天只有 hive）用这个。 */
public ConnectorTableSchema(String tableName, List<ConnectorColumn> columns,
        String tableFormatType, Map<String, String> properties,
        Set<ConnectorCapability> tableCapabilities);

/** 本表在连接器级集合之外额外支持的能力；默认空集，绝不为 null。 */
public Set<ConnectorCapability> getTableCapabilities();
```

调研报告建议加建造器，这里**故意不加**：只多一个字段，两个构造器就够了；加建造器会与现有 23 个 `new ConnectorTableSchema(...)` 构造点长期并存，正好制造出这轮清理要消除的那种「同一件事两条通道」。

`PER_TABLE_CAPABILITIES_KEY` 连同它在 `RESERVED_CONTROL_KEYS` 里的登记一起删掉。

**（二）能力集合沿 schema 缓存往下走。** `PluginDrivenSchemaCacheValue` 增一个 `Set<ConnectorCapability> tableCapabilities` 字段（与已有的 `tableProperties` 并列，同样只活在进程内缓存里），`toSchemaCacheValue` 从 `tableSchema.getTableCapabilities()` 取。

**（三）引擎侧表作用域能力统一走一个入口。** `hasScanCapability` 改名 `hasCapability`（仍是私有），语义仍是「连接器级 ∪ 本表级」，只是本表级从解析字符串变成读集合；`PluginDrivenExternalTable` 里 3 处直读（`:337` / `:353` / `:438`）改走它。

**（四）目录作用域能力也统一走一个入口。** 在 `PluginDrivenExternalCatalog` 上加一个通用访问器，替掉 4 个类里重复的 `catalog instanceof ... && getConnector() != null && getConnector().getCapabilities().contains(...)` 样板：

```java
// PluginDrivenExternalCatalog
public boolean hasConnectorCapability(ConnectorCapability capability);
```

这条不违反「fe-core 只出不进」：它不是数据源相关代码，是把已存在的重复判断收成一个通用访问器，整体是净删行。

**（五）在枚举上逐条写清作用域。** `ConnectorCapability` 每个常量的注释里补一句它是**按目录解析**还是**按目录 ∪ 按表解析**。核实后的分布是：

- 按目录 ∪ 按表（8 个）：`SUPPORTS_COLUMN_AUTO_ANALYZE`、`SUPPORTS_SAMPLE_ANALYZE`、`SUPPORTS_TOPN_LAZY_MATERIALIZE`、`SUPPORTS_NESTED_COLUMN_PRUNE`、`SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE`、`SUPPORTS_SHOW_CREATE_DDL`、`SUPPORTS_VIEW`、`SUPPORTS_METADATA_PRELOAD`。
- 只能按目录（5 个）：`SUPPORTS_MVCC_SNAPSHOT`（在表对象**建出来之前**就要用它选表子类，读表级集合会形成循环依赖）、`SUPPORTS_USER_SESSION`（目录级凭证投射）、`SUPPORTS_PASSTHROUGH_QUERY`（表值函数，没有表）、`SUPPORTS_SORT_ORDER`（建表语句分析期，表还不存在）、`SUPPORTS_PARTITION_STATS`（两个调用点必须给出一致答案，其中 `getMetaData()` 那条路径上没有表对象，见 5.3）。

也就是说，**「5/13」会变成「8/13，剩下 5 个天生只能按目录，并逐条写明原因」**，而不是调研报告里那句 13/13——13/13 在结构上做不到。

**（六）hive 收窄反射范围。** `reflectSiblingScanCapabilities` 不再反射兄弟的整个集合，改为反射一个显式列出的「打算让表继承的能力」子集：`SUPPORTS_COLUMN_AUTO_ANALYZE`、`SUPPORTS_SAMPLE_ANALYZE`、`SUPPORTS_TOPN_LAZY_MATERIALIZE`、`SUPPORTS_NESTED_COLUMN_PRUNE`、`SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE` —— 也就是今天引擎实际会读的那 5 项。取这 5 项而不是「iceberg 兄弟现在恰好声明的那 4 项」，是为了让行为不变这件事**与兄弟连接器声明了什么无关**（iceberg 今天不声明 `SUPPORTS_SAMPLE_ANALYZE`，反射它是空操作；将来若声明，行为与今天一致）。这一步**行为完全不变**（今天多发的部分本来就被丢弃），但它是第（三）步安全的前提：只有先收窄，扩大引擎读取范围才不会顺带改变 iceberg-on-HMS 表的行为。方法名里的 "Scan" 一并去掉。

**（七）写特性改成静态派生函数。** `fe-connector-api` 的 write 包下新增：

```java
public final class ConnectorWriteTraits {
    private ConnectorWriteTraits() {}

    /** 连接器级写计划提供者，连接器为 null 时返回 null。 */
    public static ConnectorWritePlanProvider providerOf(Connector connector);
    /** 按表的写计划提供者（异构网关据 handle 选子提供者），连接器为 null 时返回 null。 */
    public static ConnectorWritePlanProvider providerOf(Connector connector, ConnectorTableHandle handle);

    // 以下 7 个是空安全解包：provider 为 null 时布尔返 false、集合返空集
    public static Set<WriteOperation> supportedOperations(ConnectorWritePlanProvider provider);
    public static boolean supportsWriteBranch(ConnectorWritePlanProvider provider);
    public static boolean requiresParallelWrite(ConnectorWritePlanProvider provider);
    public static boolean requiresFullSchemaWriteOrder(ConnectorWritePlanProvider provider);
    public static boolean requiresPartitionLocalSort(ConnectorWritePlanProvider provider);
    public static boolean requiresPartitionHashWrite(ConnectorWritePlanProvider provider);
    public static boolean requiresMaterializeStaticPartitionValues(ConnectorWritePlanProvider provider);
}
```

`Connector` 上那 11 个默认方法全部删除。调用方改成两段式，比如 `PluginDrivenExternalTable:388`：

```java
return resolveWriteCapabilityHandle(connector)
        .map(handle -> ConnectorWriteTraits.requiresPartitionHashWrite(
                ConnectorWriteTraits.providerOf(connector, handle)))
        .orElse(false);
```

**这个形状顺手把「7 项特性只有 4 项有按表形态」的缺口取消掉了**，而不是去补那 3 个缺失的重载：作用域由调用方选哪个 `providerOf` 决定，7 项特性各只有一个解包函数，任何一项都能按连接器级或按表级取。这比补 3 个重载更好——那 3 个重载**今天没有任何调用方**，而本轮清理的其它任务正在删的就是这种零使用接口面，一边删一边加新的零使用方法说不通。

### 5.2 改动清单

| 文件 | 要做什么 |
|---|---|
| `fe-connector-api/.../ConnectorTableSchema.java` | 加 `tableCapabilities` 字段、5 参构造器、`getTableCapabilities()`；第二批删 `PER_TABLE_CAPABILITIES_KEY`（`:98`）及其在 `RESERVED_CONTROL_KEYS`（`:121`）中的登记与相关注释 |
| `fe-connector-api/.../ConnectorCapability.java` | 每个常量的注释补一句作用域（按目录 / 按目录 ∪ 按表），并说明只能按目录的原因 |
| `fe-connector-api/.../Connector.java` | 删 `:115`–`:186` 的 11 个写特性镜像方法；清理随之无用的 `EnumSet` 导入 |
| `fe-connector-api/.../write/ConnectorWriteTraits.java` | **新增**：9 个静态方法（见 5.1 第七条） |
| `fe-connector-api/.../ConnectorContractValidator.java` | `:42`–`:70` 五处校验改用静态派生（先取一次 provider，再逐项解包） |
| `fe-connector-hive/.../HiveConnectorMetadata.java` | `:522-541` 改为收集 `EnumSet<ConnectorCapability>` 并走 5 参构造器；`:566-588` 的反射改名并收窄到显式子集；第二批删掉 CSV 写入 |
| `fe-core/.../plugin/PluginDrivenSchemaCacheValue.java` | 加 `tableCapabilities` 字段 + 取值方法（与 `tableProperties` 同款，非持久化） |
| `fe-core/.../plugin/PluginDrivenExternalTable.java` | `hasScanCapability`（`:302`）改名 `hasCapability` 并改读集合；`:337`/`:353`/`:438` 三处直读改走它；`toSchemaCacheValue`（`:491`）透传能力集合；写特性 7 处调用（`:178`/`:206`/`:223`/`:367`/`:388`/`:403`/`:423`）改用静态派生 |
| `fe-core/.../plugin/PluginDrivenExternalCatalog.java` | 加 `hasConnectorCapability`；`:316` 与 `:1289` 改用它 |
| `fe-core/.../plugin/PluginDrivenExternalDatabase.java` | `:60` 改用 `hasConnectorCapability` |
| `fe-core/.../commands/info/CreateTableInfo.java` | `:794` 改用 `hasConnectorCapability` |
| `fe-core/.../tablefunction/QueryTableValueFunction.java` | `:78` 改用 `hasConnectorCapability` |
| `fe-core/.../commands/ShowPartitionsCommand.java` | `:344-351` 改用 `hasConnectorCapability`，并把「为什么这一项保持目录级」写进注释 |
| `fe-core/.../translator/PhysicalPlanTranslator.java` | `:630`/`:682` 改用静态派生 |
| 测试源：`PluginDrivenExternalTableTest`（`:475`/`:494`/`:553`/`:562`）、`HiveConnectorMetadataSchemaTest:117`、`HiveConnectorMetadataSiblingDelegationTest:373/393`、`ConnectorWriteDelegationTest`、`ConnectorContractValidatorTest`、`PhysicalConnectorTableSinkTest`、`InsertIntoTableCommandTest`、`InsertOverwriteTableCommandTest` | 断言从「查属性表里的字符串」改成「查类型化集合」；写特性断言改走静态派生 |

**建议分三批合入**（每批单独能编译、单独能跑测试）：

1. **第一批（双写，行为不变）**：加类型化字段与缓存字段；hive 同时发类型化集合与旧字符串；引擎按「类型化集合 ∪ 旧字符串 ∪ 连接器级」解析；hive 反射收窄到显式子集。
2. **第二批（拆旧）**：删 `PER_TABLE_CAPABILITIES_KEY`、hive 停发字符串、引擎删字符串分支；`hasScanCapability` 改名 `hasCapability`；3 处表作用域直读改走它；`hasConnectorCapability` 落地；枚举注释补作用域。
3. **第三批（写特性，与前两批无耦合，可并行）**：新增 `ConnectorWriteTraits`，删 `Connector` 上 11 个镜像方法，改所有调用方与测试。

### 5.3 明确不要顺手做的事

- **不要把 `SUPPORTS_PARTITION_STATS` 改成按表解析。** 它的两个调用点必须给出一致答案（`ShowPartitionsCommand:293` 决定每行有几列，`:390` 决定表头有几列），而 `getMetaData()` 那条路径上没有解析出表对象、也不适合在那里抛表不存在的异常。表头和行宽一旦不一致是可见的错误结果。保持目录级并把原因写进注释。
- **不要顺手让 iceberg-on-HMS 表继承 `SUPPORTS_SHOW_CREATE_DDL` / `SUPPORTS_VIEW` / `SUPPORTS_METADATA_PRELOAD`。** 机制上第二批之后它就是一行改动的事（把这几项加进 hive 的反射子集），但那是真实的行为变化（`SHOW CREATE TABLE` 输出、视图判定、元数据预载），需要独立评审加异构目录端到端回归。本任务只把机制做齐，行为保持不变。
- **不要把 `requiresParallelWrite` / `requiresFullSchemaWriteOrder` / `requiresPartitionLocalSort` 这三处引擎调用改成按表解析。** 按表解析要多做一次表句柄解析（`resolveWriteCapabilityHandle`），而对今天唯一的异构网关来说这三项在 hive 与 iceberg 上取值相同，结果必然一致——纯粹多花 CPU 换同一个答案。API 上具备按表能力，引擎按需使用。
- **不要顺手改 `ConnectorTableSchema` 的其它 6 个保留控制键。** 分区列、主键、分布列仍是列名字符串 CSV，它们是名字而不是枚举，不在本任务范围。
- **不要为「不许在 `Connector` 上镜像 provider 方法」写 shell 或正则门禁。** 删掉方法之后这条约束由语言保证（没有方法可覆写），门禁是多余的；本仓库已有结论：这类门禁只适合存在性与前缀类不变量。
- **不要引入建造器。** 理由见 5.1 第一条。

---

## 六、怎么验证

**编译门禁（最强单一信号）**：全反应堆**含测试源**编译通过。

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T1C test-compile
```

不得使用任何跳过测试源编译的参数。删掉 `Connector` 上 11 个方法之后，任何漏改的调用方都是编译错误——这是本任务第三部分的主要保障。删掉 `PER_TABLE_CAPABILITIES_KEY` 之后，任何还在引用它的生产代码或测试同样编译不过。

**单元测试要断言的东西**（跑测试必须禁用 maven build cache，否则 surefire 会被静默跳过而构建仍报成功）：

1. `PluginDrivenExternalTableTest`：改造已有 4 处断言（`:475`/`:494`/`:553`/`:562`），把「属性表里放字符串」换成「schema 里放类型化集合」，保留它们原有的变异说明。新增两条：
   - **加法语义**：连接器级集合为空 + 本表集合含某能力 ⇒ 该能力为真；反之亦然；本表集合含 A 不得让 B 为真。
   - **新纳入的三项**：`supportsShowCreateDdl` / `supportsView` / `supportsExternalMetadataPreload` 在「连接器级没有、本表集合有」时为真（这三条是第二批新增能力的行为证据）。
2. `HiveConnectorMetadataSchemaTest` / `HiveConnectorMetadataSiblingDelegationTest`：断言点从读属性串改为读 `getTableCapabilities()`；**新增一条断言反射子集**：给一个声明了 `SUPPORTS_SHOW_CREATE_DDL` + `SUPPORTS_SORT_ORDER` + `SUPPORTS_COLUMN_AUTO_ANALYZE` 的假兄弟连接器，断言反射结果**只含** `SUPPORTS_COLUMN_AUTO_ANALYZE`。变异说明：如果有人把反射改回「整个集合」，这条断言变红——它正是防止 iceberg-on-HMS 行为被意外改变的那道闸。
3. `ConnectorWriteDelegationTest` / `ConnectorContractValidatorTest`：改走静态派生后，断言「provider 为 null 时 7 项解包全部退化为 false / 空集」和「provider 覆写为 true 时解包为 true」。这两条编码的意图是：真源只有 provider 一处。
4. **不需要写「没有连接器覆写镜像方法」这类守卫测试**：方法删掉之后这件事由语言保证。

**是否需要端到端回归**：本任务的三批改动都以行为不变为目标，不新增用户可见能力，因此不需要新增 e2e 用例。但第二批合入后建议在异构 HMS 目录上跑一遍现有的 hive/iceberg-on-HMS 统计与查询回归（`ANALYZE`、带 `LIMIT ... ORDER BY` 的 Top-N、嵌套列裁剪、`SHOW CREATE TABLE`），确认按表能力的三项仍生效、`SHOW CREATE TABLE` 输出**没有**变化。

---

## 七、风险与回退

| 风险 | 说明与对策 |
|---|---|
| 扩大引擎读取范围时顺带改变 iceberg-on-HMS 行为 | 这是本任务最大的风险点。对策是顺序：先收窄 hive 反射（行为不变），再扩大引擎读取范围。收窄那一步配一条断言子集内容的单测（见六.2） |
| 按表能力在某条路径上丢失 | 载体从属性 `Map` 换成独立字段，任何忘记透传的地方都会让对应表退化成「只有连接器级能力」——不报错、只是性能或功能静默退化。对策是六.1 的加法语义断言 + 逐项断言三个新纳入能力 |
| 删镜像方法漏改调用方 | 编译期即失败，无运行时风险 |
| 静态派生函数写反极性（该 `false` 写成 `true`） | 由六.3 的空 provider 退化断言覆盖 |
| 缓存值改字段影响元数据兼容 | 已核实 `SchemaCacheValue` 及 `PluginDrivenSchemaCacheValue` 无任何 Gson 注解、不参与持久化，只是进程内 schema 缓存；无兼容负担 |

**回退**：三批各自是独立提交，任一批可单独 `revert`。第三批（写特性）与前两批无代码耦合，回退互不影响。第二批回退后第一批的双写状态仍是自洽的可运行状态。

---

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`
  - 第五章「主题二：『声明一项能力』有多条并行通道」（5.1 现象 / 5.2 为什么是问题 / 5.3 建议三步）——本任务对应其中的第二步与第三步，第一步是 07 号。
  - 第 6.1 节的接口规模表中 `Connector` 那一行——把混在一起的职责逐项列出，其中写着「9 个写特性镜像」；报告当时只数了 9 个返回布尔的，实测是 11 个（另有 2 个返回写操作集合），本文正文的口径更准。
  - 附录 A 第 76 / 77 / 87 / 108 / 133 条——能力声明多通道、按表能力 CSV、写侧三层并存表达、入口接口职责过载的原始记录。
  - 第 17.2 节「直译后没接线的真缺陷」第 9 条——「在入口接口上做 provider 方法的镜像转发」，Trino 里不存在这种东西，能力永远只挂在归属它的那个 provider 上。
- `tasks/07-write-down-the-design-rules.md`：能力声明的三层规则由它写下来，本任务是按规则改代码。其中第二层「禁止在 `Connector` 上做镜像转发」正是本任务第三部分要落实的内容。
- `tasks/11-delete-dead-surface-batch-one.md` / `tasks/12-delete-dead-surface-batch-two.md` / `tasks/13-delete-scan-range-type-enum.md`：同一轮里删零使用接口面的任务。本任务 5.1 第七条决定「不补 3 个零调用方重载」正是为了不和它们互相打架。
