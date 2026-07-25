# 16. 把引擎里三处按数据源名判定的软阻塞分支改成中立声明

> **优先级**：第四优先级（兑现承诺） ｜ **风险**：中 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-spi`、`fe-connector-api`、`fe-core`、`fe-connector-hive`、`fe-connector-iceberg`、`fe-connector-paimon`、`fe-connector-hudi`、`fe-connector-es`
> **预计改动规模**：改约 13 个文件；新增约 60 行（3 个默认方法 + 1 个查表工具 + 各连接器一行声明），删约 20 行（源名白名单、两个源专有剖析常量及其三处引用），新增 3～4 个单测约 150 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

引擎里还有三处用「数据源类型名字符串」当判据的分支（事件同步的强制预热、BE 文件缓存准入治理的适用范围、切换目录时自动进入的默认库），名单外的连接器静默拿不到这份行为；把这三处换成连接器自己声明的中立开关，再顺手删掉查询剖析里两个源专有、且从来没人赋值的常量。改完之后，第 9 个、第 10 个连接器都不必再碰这四处公共代码。

## 二、背景：现在的代码是怎么写的

### 2.1 事件同步的一次性强制预热按 `"hms"` 筛选

`fe/fe-core/src/main/java/org/apache/doris/datasource/MetastoreEventSyncDriver.java` 是元数据变更事件同步的引擎侧驱动。它每个周期遍历所有目录（`realRun`，99 行起），对每个插件目录做两件事：

- 若目录**已初始化**：直接用中立能力探针取事件源（132 行 `pluginCatalog.getConnector().getEventSource()`，137 行判 `null` 跳过）。这一段完全中立，没有任何类型判断。
- 若目录**尚未初始化**（107 行 `!pluginCatalog.isInitialized()`）：为了对齐迁移前的行为——旧的事件轮询器每周期强制初始化每一个 HMS 目录，好让「从未被查询过」的目录也能播下事件游标——这里补了一次性的强制预热，而这次预热被一个硬编码类型串挡住：

```java
// MetastoreEventSyncDriver.java:119
if (!"hms".equalsIgnoreCase(pluginCatalog.getType())) {
    continue;
}
try {
    pluginCatalog.makeSureInitialized();
```

`getType()` 读的是目录属性、不会触发初始化，所以这里刻意用类型串而不是碰连接器实例（108～118 行的注释把这一点写清楚了：不能让空闲的 paimon/iceberg/jdbc 目录被这段代码强制初始化）。

同时 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/Connector.java` 的 `getEventSource()`（347 行）在文档里承诺（340～346 行）：

> the engine's single, connector-agnostic, role-aware event driver iterates catalogs and calls `pollOnce` only on connectors that expose a source, **never via `instanceof`**

### 2.2 BE 文件缓存准入治理按目录类型白名单

`fe/fe-core/src/main/java/org/apache/doris/datasource/scan/FileQueryScanNode.java`：

```java
// :117-121
// The data cache function only works for queries on Hive, Iceberg, Hudi(via HMS), and Paimon tables.
private static final Set<String> CACHEABLE_CATALOGS = new HashSet<>(
        Arrays.asList("hms", "iceberg", "paimon")
);
```

唯一消费点在 `fileCacheAdmissionCheck()`（747 行起）：

```java
// :757
if (CACHEABLE_CATALOGS.contains(externalTableIf.getCatalog().getType())) {
    ... FileCacheAdmissionManager.getInstance().isAdmittedAtTableLevel(...)
} else {
    // LOG.debug("Skip file cache admission control for non-cacheable table: ...")
}
```

该方法在 `FileQueryScanNode` 的扫描范围构建里被调用（398 行，前置条件是会话开了 `enableFileCache` 且 `Config.enable_file_cache_admission_control` 打开），基类 `fe/fe-core/src/main/java/org/apache/doris/planner/ScanNode.java:773` 的默认实现恒返回 `true`。

已核实 `FileQueryScanNode` 在主代码里只有三个子类：`PluginDrivenScanNode`、`TVFScanNode`、`RemoteDorisScanNode`。也就是说白名单里的 hms/iceberg/paimon 三种目录今天**全部**由 `PluginDrivenScanNode` 服务，fe-core 里已经没有各数据源自己的文件扫描节点了。

### 2.3 切换目录时的默认库硬编码 `"es"`

`fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java` 的 `changeCatalog`（6495 行起）在恢复「上次待过的库」之后，补了一段：

```java
// :6509-6512
if ("es".equalsIgnoreCase(
                (String) catalogIf.getProperties().get(CatalogMgr.CATALOG_TYPE_PROP))) {
    ctx.setDatabase("default_db");
}
```

而 `"default_db"` 这个名字在连接器侧已经有权威定义：`fe/fe-connector/fe-connector-es/src/main/java/org/apache/doris/connector/es/EsConnectorMetadata.java:43` 的 `public static final String DEFAULT_DB = "default_db"`。同一个事实在引擎和连接器各写了一份。

（补充核实：ES 目录今天已经是插件目录——`fe/fe-core/src/main/java/org/apache/doris/persist/gson/GsonUtils.java:366` 把持久化别名 `"EsExternalCatalog"` 映射到 `PluginDrivenExternalCatalog`，fe-core 里已无 `EsExternalCatalog` 类，所以「按类型查 provider」这条路对它是通的。）

### 2.4 查询剖析里两个源专有常量

`fe/fe-core/src/main/java/org/apache/doris/common/profile/SummaryProfile.java` 有两个常量：

```java
// :158-159
public static final String ICEBERG_SCAN_METRICS = "Iceberg Scan Metrics";
public static final String PAIMON_SCAN_METRICS = "Paimon Scan Metrics";
```

它们出现在两张表里：显示顺序表 `EXECUTION_SUMMARY_KEYS`（218～219 行）和缩进表 `EXECUTION_SUMMARY_KEYS_INDENTATION`（278～279 行）。连接器侧各有一份逐字相同的字符串字面量，并在注释里声称必须与 fe-core 常量一致：`fe/fe-connector/fe-connector-iceberg/.../IcebergScanProfileReporter.java:52-53`、`fe/fe-connector/fe-connector-paimon/.../PaimonScanMetrics.java:47-48`。

真实机制已核实为：连接器交出的 `ConnectorScanProfile` 由 `PluginDrivenScanNode.writeScanProfilesInto`（420 行起）转写，分组名被用来 **get-or-create 一个子剖析节点**（427～428 行 `new RuntimeProfile(profile.getGroupName())` + `executionSummary.addChild(...)`），子节点的顺序就是插入顺序。而上面那两张表只作用于**信息字符串**：`SummaryProfile.init()`（520 行起）给 `EXECUTION_SUMMARY_KEYS` 里每个键无条件塞一条 `"N/A"`；缩进表只在 `RuntimeProfile.prettyPrint` 打印本节点自己的信息字符串时被查（409～410 行）。

## 三、为什么这是个问题

**第一处（事件同步预热）——文档承诺与代码不符，且有真实后果。** 调研报告把后果写成「事件源永远不会被激活」，实测要收窄：已初始化的目录走的是完全中立的能力探针，一旦目录被查询过一次就正常同步。准确的后果是：**在某个 FE 上从未被初始化过的目录，其事件游标不会被自动播种**。这在多 FE 部署里是真实的——每个 FE 各自跑一份驱动、各自维护游标，而 follower 上通常没人发查询，于是一个实现了 `getEventSource()` 的新连接器在 follower 上（以及 FE 重启后到首次查询之间）拿不到增量同步，只能等有人查它。这正是 108～118 行注释为 HMS 特意保留强制预热的原因，新连接器却拿不到同一份照顾。同时 `getEventSource()` 文档里那句「从不用 `instanceof`」在这条分支上是假的——按类型名筛选和 `instanceof` 是同一件事的两种写法。

**第二处（文件缓存准入）——目前不是 bug，是扩展点。** 今天的覆盖是正确的：白名单外的 jdbc / trino / max_compute 走 JNI 读取，本来就没有 BE 文件缓存，跳过路径还有 `LOG.debug` 兜底。问题在判据错位：治理的真实前提是「这个连接器的数据由 BE 原生文件读取器读取，因此 BE 文件缓存对它有效」，代码却写成「目录类型叫这三个名字之一」。将来新增一个 BE 原生读文件的湖格式连接器，它的表会静默绕过缓存准入治理（用户设置了库/表级的缓存准入规则，对这个新目录不生效，且没有任何报错），必须改 fe-core 才能纳入。

**第三处（默认库）——SPI 缺一个声明位。** 「切到本目录时自动进入某个默认库」是数据源自己的事实（ES 没有库的概念，Doris 给它造了一个 `default_db`），却只能由引擎硬编码。新连接器要这个行为必须改 `Env`。

**第四处（剖析常量）——中立性瑕疵 + 每个查询两行垃圾。** `ICEBERG_SCAN_METRICS` / `PAIMON_SCAN_METRICS` 已核实**没有任何地方给它们赋值**（全仓 grep：只有常量声明、两张表、以及镜像断言的测试）。于是每个查询的执行摘要里都无条件多出两行 `- Iceberg Scan Metrics: N/A` 和 `- Paimon Scan Metrics: N/A`；而真正的 iceberg/paimon 扫描指标是以**同名子节点**的形式挂上去的，于是同一份剖析里会出现「一行 N/A 的条目」和「一个同名的子树」并存，反而更容易看错。缩进表里那两条对子节点完全无效（子节点的信息字符串键是各自的指标名，不在缩进表里）。连接器注释里「MUST equal fe-core 常量（display ordering）」的说法与实际机制不符：分组名是连接器自选的子节点名字，fe-core 不需要预先知道它。

## 四、用一个最小例子说明

假设我要新增一个连接器 `X`（一个 BE 原生读 Parquet 的新湖格式，带元数据变更事件源，并且它的元数据模型没有「库」这一层，希望切进去就落到一个固定库）。我今天必须动的公共模块文件：

| 我想要的行为 | 今天实际发生什么 | 我今天必须改哪里 | 改完本任务后 |
|---|---|---|---|
| 目录的元数据变更能自动同步 | 我实现了 `getEventSource()`，master 上查过一次的目录能同步；follower 上（没人查）游标从不播种，一直不同步 | `MetastoreEventSyncDriver.java:119`，把 `"x"` 加进类型判断 | 在 `XConnectorProvider` 里 `providesEventSource()` 返回 `true` |
| 我的表纳入 BE 文件缓存准入治理 | 用户配的缓存准入规则对我的目录静默不生效，只有一行 `LOG.debug` | `FileQueryScanNode.java:119`，把 `"x"` 加进 `CACHEABLE_CATALOGS` | 在 `XScanPlanProvider` 里 `supportsFileCache()` 返回 `true` |
| `SWITCH x;` 之后自动进入 `default_db` | 停在没有库的状态，用户必须显式 `USE` | `Env.java:6509`，在 `"es"` 旁边加 `"x"` | 在 `XConnectorProvider` 里 `defaultDatabaseOnUse()` 返回库名 |
| 在查询剖析里输出我的扫描指标 | 实际上**已经可以**（分组名自选，无需 fe-core 登记）；但 fe-core 的注释与常量表让人以为必须先去加常量 | 误以为要改 `SummaryProfile.java:158` | 什么都不用改（那两个常量已删） |

用户视角的一个具体现象（第四处）：今天任意一条查询

```sql
SET enable_profile = true;
SELECT count(*) FROM internal.some_db.some_olap_table;
```

的执行摘要里也会出现

```
- Iceberg Scan Metrics: N/A
- Paimon Scan Metrics: N/A
```

——这条查询跟 iceberg / paimon 毫无关系。删掉这两个常量是修正，不是回归。

## 五、解决方案

### 5.1 目标状态

**`ConnectorProvider` 新增两个默认方法**（`fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorProvider.java`）。挂在 provider 而不是 `Connector` 上是这条任务的关键判断：这两处判定发生在目录**可能尚未初始化**的时刻（事件驱动的预热分支、`SWITCH` 时可能还没碰过连接器），只能按类型查 provider；碰 `Connector` 实例会强制初始化，引入现状没有的副作用（正好是 `MetastoreEventSyncDriver` 108～118 行注释要避免的事）。

```java
/**
 * 本类型的连接器是否会通过 Connector#getEventSource() 暴露增量元数据变更源。
 * 引擎用它决定：一个尚未初始化的目录是否值得为播种事件游标做一次性强制预热。
 * 必须与 Connector#getEventSource() 是否返回非 null 保持一致（同一份能力的两个高度）。
 * 默认 false —— 无事件源的连接器不会被强制初始化。
 */
default boolean providesEventSource() {
    return false;
}

/**
 * 切换到本类型目录时应自动进入的库名；空表示不自动进入任何库（默认）。
 * 用于元数据模型没有「库」这一层、由 Doris 造一个固定库名的数据源（如 ES 的 default_db）。
 */
default Optional<String> defaultDatabaseOnUse() {
    return Optional.empty();
}
```

**`ConnectorScanPlanProvider` 新增一个能力位**（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanPlanProvider.java`，紧邻已有的 `supportsBatchScan`（250 行）/ `supportsTableSample`（268 行）放，照抄它们的 opt-in 形状）：

```java
/**
 * 本连接器规划出的扫描范围是否由 BE 的原生文件读取器读取（因此 BE 文件缓存对它有效，
 * 引擎应对它的表施加文件缓存准入治理）。JNI 读取的连接器必须保持 false ——
 * 它们不经过 BE 文件缓存，做准入判定只是白花一次远程/本地开销。默认 false。
 */
default boolean supportsFileCache() {
    return false;
}
```

**fe-core 侧多一个中立的 provider 查表入口**：`ConnectorFactory`（`fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorFactory.java`）加静态 `findProvider(String catalogType, Map<String, String> properties)` 返回 `Optional<ConnectorProvider>`，委派给 `ConnectorPluginManager` 里一个新的同名方法——该方法复用 `createConnector`（127 行起）里现成的选择逻辑（第一个 `supports(...)` 为真且 `apiVersion` 匹配的 provider），只是不 `create`。plugin manager 未初始化时返回空。

关于「fe-core 只出不进」：这个查表入口是**中立**的（不含任何数据源名），它的存在是为了删掉三处数据源名判定；本任务在 fe-core 里的数据源相关代码是净减的。这不属于「为了让删除能编译过就把逻辑挪进 fe-core」。

**三处判定改写后的样子**：

- `MetastoreEventSyncDriver.java:119` → `if (!providesEventSource(pluginCatalog.getType(), pluginCatalog.getProperties())) { continue; }`，其余（`makeSureInitialized()` 的 try/catch、`!isInitialized()` 的一次性守卫）一字不动。
- `FileQueryScanNode.java:757` → `if (isFileCacheAdmissionApplicable()) { ... }`；`FileQueryScanNode` 里加 `protected boolean isFileCacheAdmissionApplicable() { return false; }`；`PluginDrivenScanNode` 覆写为「取 `resolveScanProvider()`（245 行），非 null 则返回 `supportsFileCache()`」。删掉 `CACHEABLE_CATALOGS` 常量与随之失效的 `java.util.Arrays` import（81 行；`HashSet` 在 455 行仍有用，别删）。
- `Env.java:6509-6512` → 查 provider 的 `defaultDatabaseOnUse()`，非空则 `ctx.setDatabase(...)`。**保持原有位置与覆盖语义**：仍在「恢复上次待过的库」之后执行，即声明了默认库的目录会覆盖 `lastDb`。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe/fe-connector/fe-connector-spi/.../ConnectorProvider.java` | 新增 `providesEventSource()`、`defaultDatabaseOnUse()` 两个默认方法（见 5.1 签名草案） |
| `fe/fe-connector/fe-connector-api/.../scan/ConnectorScanPlanProvider.java` | 新增 `supportsFileCache()` 默认方法，紧邻 `supportsTableSample` |
| `fe/fe-core/.../connector/ConnectorPluginManager.java` | 新增 `findProvider(String, Map)`，复用 `createConnector` 的 provider 选择逻辑（含 `apiVersion` 校验），不创建连接器 |
| `fe/fe-core/.../connector/ConnectorFactory.java` | 新增静态 `findProvider`，manager 未初始化时返回空 |
| `fe/fe-core/.../datasource/MetastoreEventSyncDriver.java` | 119 行的 `"hms"` 判定换成 provider 声明；把 108～118 行注释里「Mirror that ONLY for the event-source type ("hms", ...)」改成按声明筛选的表述，保留「按类型查 provider、不碰连接器实例，避免强制初始化」的理由 |
| `fe/fe-core/.../datasource/scan/FileQueryScanNode.java` | 删 `CACHEABLE_CATALOGS`（117～121 行）与 `Arrays` import（81 行）；757 行改为调用新的 `protected boolean isFileCacheAdmissionApplicable()`（默认 false，带注释说明默认 false 保持 TVF / 远程 Doris 扫描节点的现状） |
| `fe/fe-core/.../datasource/scan/PluginDrivenScanNode.java` | 覆写 `isFileCacheAdmissionApplicable()`，委派给 `resolveScanProvider().supportsFileCache()`（provider 为 null 时 false） |
| `fe/fe-core/.../catalog/Env.java` | `changeCatalog` 里 6509～6512 行的 `"es"` 判定换成 provider 的 `defaultDatabaseOnUse()`；类型串为空时直接跳过查表（内部目录没有 `type` 属性） |
| `fe/fe-core/.../common/profile/SummaryProfile.java` | 删 `ICEBERG_SCAN_METRICS` / `PAIMON_SCAN_METRICS`（158～159 行）及其在 `EXECUTION_SUMMARY_KEYS`（218～219 行）、`EXECUTION_SUMMARY_KEYS_INDENTATION`（278～279 行）里的条目 |
| `fe/fe-connector/fe-connector-hive/.../HiveConnectorProvider.java` | `providesEventSource()` 返回 `true`（`getType()` 已核实为 `"hms"`，37～39 行） |
| `fe/fe-connector/fe-connector-hive/.../HiveScanPlanProvider.java` | `supportsFileCache()` 返回 `true` |
| `fe/fe-connector/fe-connector-iceberg/.../IcebergScanPlanProvider.java` | `supportsFileCache()` 返回 `true` |
| `fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProvider.java` | `supportsFileCache()` 返回 `true`（保持现状：白名单里有 `paimon`） |
| `fe/fe-connector/fe-connector-hudi/.../HudiScanPlanProvider.java` | `supportsFileCache()` 返回 `true`。**这一条容易漏**：hudi 表寄生在 hms 目录上，今天靠白名单里的 `"hms"` 拿到治理；改成按服务方 provider 声明之后，如果 hudi 表被转交给 hudi 兄弟连接器（`HiveConnector.getScanPlanProvider(handle)`，248～253 行按 handle 三路分派），不声明就会静默丢掉治理 |
| `fe/fe-connector/fe-connector-es/.../EsConnectorProvider.java` | `defaultDatabaseOnUse()` 返回 `Optional.of(EsConnectorMetadata.DEFAULT_DB)`（43 行已有常量，别再写一遍字面量） |
| `fe/fe-connector/fe-connector-iceberg/.../IcebergScanProfileReporter.java` | 只改 52 行注释：分组名是连接器自选的剖析子节点名，与 fe-core 常量无耦合；`GROUP_NAME` 字面量与测试断言保留（它是用户可见名，值得钉住） |
| `fe/fe-connector/fe-connector-paimon/.../PaimonScanMetrics.java` | 同上，只改 47 行注释 |
| `fe/fe-core/src/test/.../scan/PluginDrivenScanNodeScanProfileTest.java` | 删掉 `groupNameConstantsMatchConnectorLiterals`（92～99 行，断言两个即将删除的常量）；其余用例（分组合并、两个扫描子节点）不动 |

**保持 `false` 不动的连接器**（今天不在白名单里，改后必须仍然不做准入判定）：`MaxComputeScanPlanProvider`、`TrinoScanPlanProvider`、`JdbcScanPlanProvider`、`EsScanPlanProvider`。不要「顺手都开上」。

### 5.3 明确不要顺手做的事

- **不要给 `Connector` 也加一份 `providesEventSource()`。** 同一能力两个高度会立刻产生「哪个是真的」的问题；`Connector.getEventSource()` 仍是唯一事实来源，provider 上那个只是「未初始化时的先行声明」，靠 javadoc 约束一致，并由 5.1 说明的原因决定它必须在 provider 上。
- **不要顺手改事件驱动的其它行为**：`!isInitialized()` 的一次性守卫、`makeSureInitialized()` 的吞异常重试、self-heal 的游标重置（149 行）都是刻意对齐迁移前行为的，本任务只换判据。
- **不要把 `defaultDatabaseOnUse()` 扩成「默认目录/默认会话属性」框架**，也不要顺手改 `changeDb`。只做一个可选库名。
- **不要顺手删 `SummaryProfile` 里其它看起来源专有的常量**（`HMS_ADD_PARTITION_TIME`、`GET_PARTITIONS_TIME` 等）：那些有真实赋值点（`GET_PARTITIONS_TIME` 在 635 行、`HMS_ADD_PARTITION_TIME` 在 713～714 行，等等），删了就是功能回归。本任务只删已核实无赋值点的那两个。
- **不要把 `CACHEABLE_CATALOGS` 的语义换成「是否 JNI 格式」之类的现场推断**（例如照 `fileFormatType == FORMAT_JNI` 判断）：扫描级的格式判定另有一套坑，且那仍是引擎在猜连接器的事实。就用连接器声明。
- **不要为这三处写 shell / 正则构建门禁**（例如「grep 不允许出现 `"hms".equalsIgnoreCase`」）：本仓库已有结论，这类门禁只适合存在性与前缀类不变量，判断语言语义的门禁误报比漏报更毒。用单测 + 评审。
- **不要顺手动 `CatalogFactory.SPI_READY_TYPES`**（`fe/fe-core/.../CatalogFactory.java:56-57`）。那也是一张类型名白名单，但它是另一件事（决定一个类型能不能建目录），风险与验证面完全不同，属于另一条任务。

## 六、怎么验证

**编译门禁（最强单一信号）**：全反应堆**含测试源**的编译

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T 1C test-compile
```

不要加任何跳过测试编译的参数。理由：新增的是 SPI 默认方法 + 各连接器覆写，编不过的地方（比如某个连接器模块看不到 `Optional` import、或测试仍引用被删的常量）只有把测试源一起编译才会暴露。

**单元测试**（跑测试时必须禁用 maven build cache，否则 surefire 会被静默跳过而 `BUILD SUCCESS` 是空的）：

1. `MetastoreEventSyncDriver` 的预热筛选：现在没有任何直测这个类行为的测试（已核实唯一提到它的是 `fe/fe-core/src/test/java/org/apache/doris/datasource/ExternalMetaIdMgrTest.java`，那里只把它当协作者 `Mockito.mock` 掉、不验证它自己的筛选逻辑），需要新建一个。断言的是**意图**而不是形状：注册两个假 provider（一个声明有事件源、一个不声明）+ 两个未初始化的假插件目录，跑一次 `runAfterCatalogReady()`，断言只有前者的目录被 `makeSureInitialized()` 触碰过（用计数器记录），后者**一次都没有被触碰**（这条就是「空闲目录不得被强制初始化」的守卫，去掉声明位判定后这条会失败——即它能在业务逻辑变化时失败）。
2. 文件缓存准入的适用性：给 `PluginDrivenScanNode` 加用例，`supportsFileCache()` 为 `true` / `false` / provider 为 `null` 三种情形下 `isFileCacheAdmissionApplicable()` 的返回值；再补一条断言 `FileQueryScanNode` 的默认实现为 `false`（现有 `FileQueryScanNodeTest` 里已有可复用的 `TestFileQueryScanNode`，63 行）。
3. `changeCatalog` 的默认库：假 provider 声明 `defaultDatabaseOnUse()` 返回某个库名，断言切换后 `ctx.getDatabase()` 落到该库、且**覆盖了**先前记住的 `lastDb`（这条钉住的是现状语义，不是新语义）；再断言未声明的目录不改动 `lastDb` 恢复结果。
4. 剖析常量删除：`PluginDrivenScanNodeScanProfileTest` 剩余用例（分组 get-or-create、两个扫描子节点各自的指标）必须继续通过——它们证明扫描剖析的分组不依赖被删的常量。连接器侧 `IcebergScanProfileReporterTest`（104 行）、`PaimonScanMetricsTest`（80 行）对自身 `GROUP_NAME` 字面量的断言保留不动。

**变异验证**（推荐做，成本很低）：把新加的三个默认方法的默认值分别从 `false`/空翻成 `true`/非空，确认至少有一条测试变红；再把某个连接器的覆写删掉，确认对应测试变红。若翻转后全绿，说明测试没有真的在验证行为。

**端到端回归**：本地无集群时不跑，需要集群的择机补。要点是三条：

- 一个 hms 目录在 FE 重启后不查询、直接等事件同步（验证预热路径仍然生效，行为与改动前一致）；
- hms 目录下的 hive / iceberg / hudi 表 + 独立 iceberg / paimon 目录，在打开 `enable_file_cache_admission_control` 并配了库级准入规则时，规则仍然生效（验证白名单→能力位迁移零行为差）；
- `SWITCH <es 目录>;` 之后 `SELECT DATABASE()` 仍是 `default_db`。

**人工核对**：打开一条普通内表查询的剖析，确认 `Iceberg Scan Metrics: N/A` / `Paimon Scan Metrics: N/A` 两行消失，而 iceberg / paimon 查询的扫描指标子树照旧出现。

## 七、风险与回退

| 风险 | 说明与缓解 |
|---|---|
| 文件缓存准入治理漏声明 → 静默丢治理 | 最需要小心的一处，尤其是 hudi（今天靠 hms 白名单覆盖，改后要靠 hudi 兄弟的 provider 声明）。缓解：改动清单里逐个连接器列明 true/false；单测覆盖三态；上线前用 hms 异构目录（hive + iceberg + hudi 同库）跑一次准入规则生效断言 |
| provider 声明与 `Connector.getEventSource()` 不一致 | 若 provider 声明了 `true` 但连接器实际不返回事件源，后果只是白做一次强制初始化（132 行仍会判 `null` 跳过），不影响正确性；反向（声明 false 但有事件源）等于保持今天的缺陷。靠 javadoc 明确「两者必须一致」+ 连接器侧单测断言 |
| 从 fe-core 调用插件代码的类加载器 | 三个新方法都是返回常量的纯 getter（没有远程调用、没有按名反射），与已在用的 `provider.getType()` / `supports()` 同性质，不需要固定线程上下文类加载器。评审时确认没有人在实现里做重活 |
| 删剖析常量导致外部工具解析失败 | 已核实这两行恒为 `N/A`、无赋值点，且 `regression-test/` 与 `docs/` 里零引用（grep `"Scan Metrics"` 无命中）。若某个外部脚本硬解析执行摘要行数，会受影响——这属于删掉一条恒为空的行的正常代价 |
| `changeCatalog` 每次切目录多一次 provider 遍历 | provider 列表是个位数的 `CopyOnWriteArrayList`，`supports()` 契约要求廉价且无网络调用；切目录不是热点路径 |

**回退**：四处彼此独立，建议拆成 4 个提交（事件同步预热 / 文件缓存能力位 / 默认库 / 剖析常量），任一处出问题单独回退即可。SPI 上新增的都是**默认方法**，回退 fe-core 侧调用点后接口留着也不影响任何连接器编译。本任务不涉及 Gson 持久化格式与 thrift 有线格式，无兼容性尾巴。

## 八、相关背景

- 调研报告 `plan-doc/connector-public-interface-cleanup/audit-report.md`：
  - 第 4.2 节「软阻塞：连接器能跑，但拿不到与既有数据源同等的行为」——按类型名字符串判定的四处清单（含表格），以及第四处剖析常量的提及。表格里第四行（配置项逐键手工转发）**不属于**本任务，见报告 4.5 节。
  - 附录 A 第 5 条（事件驱动里硬编码 `"hms"`）与第 30 条的复核收窄（`getEventSource` 的中立承诺在未初始化目录上是假的）——两处合起来给出事件同步预热的准确影响面：「本 FE 上从未初始化过的目录不会被强制预热」，不是「永远不会被激活」。
  - 附录 A 第 13 条——剖析分组名必须匹配 fe-core 的源专有常量表，结论是那两个常量应删。
  - 附录 A 第 15 条——文件缓存目录类型白名单当前不是 bug、只是扩展点。
  - 第 4.3 节「不阻塞但仍是按类型分支的地方」——`PluginDrivenExternalTable.getEngine()` 那两张同样按类型名的展示名表，报告结论是**不下沉**、只合并去重，不要顺手卷进本任务。
- 已有的能力位 opt-in 先例：`ConnectorScanPlanProvider.supportsBatchScan`（250 行）、`supportsTableSample`（268 行）、`scannedPartitionCount`（291 行）——新加的 `supportsFileCache()` 照抄它们的形状与文档风格。
- 同任务集：编号 07《把设计规则写下来》——本任务确立的「按类型查 provider 用于未初始化时刻的判定，碰连接器实例会引入强制初始化副作用」这条经验，值得写进规则文档。
