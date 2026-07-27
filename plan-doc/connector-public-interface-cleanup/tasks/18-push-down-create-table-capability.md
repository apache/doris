# 18. 把建表能力从引擎白名单下沉为连接器声明（高风险，需端到端兜底）

> **优先级**：第四优先级（高风险） ｜ **风险**：高 ｜ **前置依赖**：无（与「删除目录类型白名单」那个任务改的是同一批引擎侧硬编码但不同文件，先后顺序不影响编译）
> **影响模块**：`fe-connector-api`（新增一个连接器声明方法 + 两个能力位）、`fe-connector-hive`、`fe-connector-iceberg`、`fe-connector-paimon`、`fe-connector-maxcompute`（各声明一次）、`fe-core`（`CreateTableInfo` 三处硬编码退化为读声明，只删不加数据源相关代码）
> **预计改动规模**：约 9～11 个文件；`fe-core` 净删约 25 行、新增约 35 行（含注释），连接器侧每个 5～15 行，测试与端到端用例新增约 150 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

今天一个新连接器想支持 `CREATE TABLE`，必须去 `fe-core` 的 `CreateTableInfo` 里改三处按引擎名写死的清单（目录类型到引擎名的映射、可接受的引擎名、可接受 `PARTITION BY` / `DISTRIBUTED BY` 的引擎名），本任务把这三处的判据换成「问连接器自己」：连接器声明它建表时用哪个引擎名（不声明就等于不支持建表），再用两个能力位声明它接不接分区子句和分桶子句。

## 二、背景：现在的代码是怎么写的

所有外部目录现在都是同一个类：`CatalogFactory.java:56-57` 的 `SPI_READY_TYPES` 已经包含 `jdbc`、`es`、`trino-connector`、`max_compute`、`paimon`、`iceberg`、`hms` 七种类型，它们全部被造成 `PluginDrivenExternalCatalog`（`CatalogFactory.java:110-117`）。也就是说建表分析期拿到的目录对象上，随时可以问到背后的连接器。但 `CreateTableInfo` 至今还是按字符串判断的。

相关代码集中在一个文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfo.java`。

**（1）引擎名常量表**，`:115-124`，十个常量：`olap` / `jdbc` / `elasticsearch` / `odbc` / `mysql` / `broker` / `hive` / `iceberg` / `paimon` / `maxcompute`。后四个是外部目录建表用的，前六个是内部表与已下线的老外部表语法。

**（2）目录类型到引擎名的映射**，`:916-932`：

```java
private static String pluginCatalogTypeToEngine(PluginDrivenExternalCatalog catalog) {
    switch (catalog.getType()) {
        case "max_compute": return ENGINE_MAXCOMPUTE;
        case "paimon":      return ENGINE_PAIMON;
        case "iceberg":     return ENGINE_ICEBERG;
        case "hms":         return ENGINE_HIVE;
        default:            return null;
    }
}
```

它的注释 `:907-915` 自己写着这段与 `PluginDrivenExternalTable.getEngine()/getEngineTableTypeName()` 是一对镜像，「**两个 switch 必须保持同步**」（原文 `the two switches must stay in sync`），并说明返回 `null` 表示这个类型不支持建表。

它有两个读者：

- `paddingEngineName`（`:886-905`）：用户没写 `ENGINE=` 时补一个。内部目录补 `olap`；插件目录若映射非空就补映射值；否则抛 `Current catalog does not support create table: <目录名>`（`:902`）。
- `checkEngineWithCatalog`（`:375-394`，在 `validate` 的 `:441` 被调用）：用户显式写了 `ENGINE=` 时校验一致性，不一致抛 `This catalog can only use \`<映射值>\` engine.`（`:391`）；`ENGINE=olap` 写在外部目录里另有一条专门文案（`:378-379`）。映射返回 `null` 的类型（`jdbc` / `es` / `trino-connector`）在这里**一律放过**，不抛。

**（3）可接受的引擎名清单**，`checkEngineName`（`:954-986`），核心是 `:955-958` 一串 `equals` 的或：

```java
if (engineName.equals(ENGINE_MYSQL) || engineName.equals(ENGINE_ODBC) || engineName.equals(ENGINE_BROKER)
        || engineName.equals(ENGINE_ELASTICSEARCH) || engineName.equals(ENGINE_HIVE)
        || engineName.equals(ENGINE_ICEBERG) || engineName.equals(ENGINE_JDBC)
        || engineName.equals(ENGINE_PAIMON) || engineName.equals(ENGINE_MAXCOMPUTE)) {
    if (!isExternal) { isExternal = true; }          // 兼容：外部引擎名隐含 external
} else {
    if (isExternal) { throw ... "Do not support external table with engine name = olap"; }
    else if (!engineName.equals(ENGINE_OLAP)) { throw ... "Do not support table with engine name = " + engineName; }
}
```

不在这张清单里的名字，在 `:968-969` 被拒。同方法后半段还有临时表拒绝（`:973-975`）和 `odbc` / `mysql` / `broker` 的硬下线文案（`:980-985`）。

**（4）分区与分桶子句的允许列表**，`analyzeEngine`（`:1125-1147`）：

```java
if (engineName.equals(ENGINE_ELASTICSEARCH)) {
    if (distributionDesc != null) { throw ... "could not support distribution clause"; }
} else if (!engineName.equals(ENGINE_OLAP)) {
    if (!engineName.equals(ENGINE_HIVE) && !engineName.equals(ENGINE_MAXCOMPUTE) && distributionDesc != null) {
        throw ... "Create " + engineName + " table should not contain distribution desc";
    }
    if (!engineName.equals(ENGINE_HIVE) && !engineName.equals(ENGINE_ICEBERG)
            && !engineName.equals(ENGINE_PAIMON) && !engineName.equals(ENGINE_MAXCOMPUTE) && partitionDesc != null) {
        throw ... "Create " + engineName + " table should not contain partition desc";
    }
}
```

即：分桶子句只有 `hive` 与 `maxcompute` 接受；分区子句 `hive` / `iceberg` / `paimon` / `maxcompute` 接受。注意 `elasticsearch` 走的是**独立的前置分支并且直接结束**，它既有自己的分桶文案，也从来不检查分区子句——这是既有怪癖，本任务不碰。

**（5）同一个文件里已经有一个正确范式**：写入排序子句 `ORDER BY` 的门（`:791-800`）已经从「引擎名等于 iceberg」改成了读能力位：

```java
boolean supportsSortOrder = catalog instanceof PluginDrivenExternalCatalog
        && ((PluginDrivenExternalCatalog) catalog).getConnector().getCapabilities()
                .contains(ConnectorCapability.SUPPORTS_SORT_ORDER);
```

本任务要做的就是把上面（2）（3）（4）改成同一形状。能力枚举在 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorCapability.java`，`SUPPORTS_SORT_ORDER` 在 `:165`（文档 `:153-164`），枚举末项是 `:182` 的 `SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE`。连接器侧的取得器是 `Connector.getCapabilities()`（`Connector.java:210-213`，默认返回空集）。

**校验发生的先后顺序**（这是本任务最重要的事实，`validate(ConnectContext)` 内）：

| 顺序 | 位置 | 做什么 |
|---|---|---|
| 1 | `:413` | `paddingEngineName` —— 补引擎名，或抛「本目录不支持建表」 |
| 2 | `:414` | `checkEngineName` —— 引擎名是否在可接受清单里 |
| 3 | `:441` | `checkEngineWithCatalog` —— 引擎名与目录是否匹配 |
| 4 | `:871` | `analyzeEngine` —— 分区 / 分桶子句是否允许 |

## 三、为什么这是个问题

**违反的原则**：连接器是独立打包的插件，应该「装上就能用」；但建表这一项能力的开关握在 `fe-core` 手里。这正是本轮整治要收掉的那类卡点——树外连接器根本没法通过改 `fe-core` 来放行自己。

**真实后果有三层**：

1. **新连接器必须改公共模块**。哪怕连接器已经把 `createTable` 实现得完整无缺，只要没在 `CreateTableInfo` 里登记，`CREATE TABLE` 在分析期就被拒，永远到不了连接器。
2. **一处已被写进注释的重复**。`:911-912` 的「两个 switch 必须保持同步」是开发者自己留下的告警：建表引擎名和展示引擎名是两张各自维护的表，`hms` 目录在建表侧要映射成 `hive`、在展示侧却必须显示 `hms`（`PluginDrivenExternalTable.java:1267-1298`）。这种「必须靠人记住同步」的结构迟早漏改。
3. **能力与实现分处两地，容易出现半开状态**。分区/分桶允许列表在 `fe-core`，真正的分区语义校验在连接器的 `createTable` 里。今天四个连接器恰好对齐，但对齐关系只存在于口头。

这不是正确性缺陷：用户今天观察不到错误结果，只会观察到「这个连接器不支持建表」。所以本任务的验收标准是**行为逐字不变**，而不是修出什么新行为。

## 四、用一个最小例子说明

假设我要新写一个连接器 `mytable`，它的 `createTable` 已经实现好了，也支持分区。今天我必须动 `fe-core`：

| 我想做的事 | 今天必须在 `CreateTableInfo` 里加什么 | 不加会怎样 |
|---|---|---|
| `CREATE TABLE t (id int)`（省略 ENGINE） | `pluginCatalogTypeToEngine` 加一个 `case "mytable": return ENGINE_MYTABLE;` | 抛 `Current catalog does not support create table: mycat` |
| `CREATE TABLE t (id int) ENGINE=mytable` | 引擎名常量 `ENGINE_MYTABLE` + `checkEngineName` 的或链加一项 | 抛 `Do not support table with engine name = mytable` |
| `CREATE TABLE t (id int) PARTITION BY LIST (id) ()` | `analyzeEngine` 分区允许列表加一项 | 抛 `Create mytable table should not contain partition desc` |

改完之后，我在自己的连接器里写：

```java
@Override
public Optional<String> getCreateTableEngineName() {
    return Optional.of("mytable");       // 不覆写 = 不支持建表，行为与今天的 jdbc/es 一致
}

@Override
public Set<ConnectorCapability> getCapabilities() {
    return ImmutableSet.of(ConnectorCapability.SUPPORTS_CREATE_TABLE_PARTITION_BY);
}
```

`fe-core` 一行不改，上面三条 SQL 全部按预期工作。

## 五、解决方案

### 5.1 目标状态

**新增一个连接器声明**，放在 `fe-connector-api` 的 `Connector` 接口上（紧邻 `getCapabilities()`）：

```java
/**
 * 本目录建表时使用的引擎名（CREATE TABLE ... ENGINE=<名字>）。返回空表示本连接器不支持建表：
 * 省略 ENGINE 的建表被拒（"Current catalog does not support create table"），显式 ENGINE 不做
 * 目录一致性校验（与今天的 jdbc / es / trino-connector 完全一致）。
 * 声明的名字同时成为可接受的引擎名，无需在引擎侧登记。
 * 注意：这与 SHOW TABLE STATUS 等处的「展示引擎名」是两件事（hms 目录建表用 hive、展示用 hms）。
 */
default Optional<String> getCreateTableEngineName() {
    return Optional.empty();
}
```

**新增两个能力位**，加在 `ConnectorCapability` 末尾（命名与既有的 `SUPPORTS_SORT_ORDER` 同族，都是建表子句门）：

- `SUPPORTS_CREATE_TABLE_PARTITION_BY` —— 接受 `PARTITION BY` 子句；声明者：hive、iceberg、paimon、maxcompute。
- `SUPPORTS_CREATE_TABLE_DISTRIBUTED_BY` —— 接受 `DISTRIBUTED BY` 子句；声明者：hive、maxcompute。

**引擎侧读法**：`CreateTableInfo` 加一个私有辅助方法，按目录名取声明。这里有一个必须显式处理的点：`PluginDrivenExternalCatalog.getConnector()`（`:347-350`）内部会调 `makeSureInitialized()`，即**强制初始化目录**。同类的能力读取在同一个类里已有两个不强制初始化的先例（`:377-389` 的 `overlayMetaCacheConfig`、`:1287-1290` 的 `supportsUserSession`，都直接读字段并对 `null` 早退）。建议在 `PluginDrivenExternalCatalog` 上加一个不强制初始化的读法：连接器字段非空时直接读声明；字段为空（元数据重放时插件缺失的降级目录）才调 `makeSureInitialized()`，让「插件没装」这条清晰错误如常抛出——这与今天的最终结果一致（今天是靠 `getType()` 先补上引擎名、随后在解析库时才抛插件缺失），只是提前了。

**三处硬编码的目标形状**：

1. `pluginCatalogTypeToEngine` 整个删除，两个调用点改成读声明。文案与抛出条件一字不动。
2. `checkEngineName` 的或链**保留原样**，末尾追加一个析取项：或者引擎名等于目标目录声明的建表引擎名。今天四个声明值本来就在或链里，所以行为零变化；新连接器则无需登记。
3. `analyzeEngine` 的两条允许列表改成读能力位，两条 `throw` 的文案与拼接方式（`"Create " + engineName + " table should not contain ..."`）一字不动，且**两条检查的先后顺序不变**（先分桶后分区）。`elasticsearch` 前置分支整块不动。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe-connector-api/.../api/Connector.java`（`:210` 附近） | 新增 `getCreateTableEngineName()` 默认方法，返回 `Optional.empty()`；文档写清「返回空=不支持建表」与「区别于展示引擎名」 |
| `fe-connector-api/.../api/ConnectorCapability.java`（`:182` 之后） | 新增两个枚举值，各带完整文档：谁声明、谁不声明、对应哪条 SQL 子句、与写路径的分区/排序特性有何区别（照 `:153-164` 的写法） |
| `fe-connector-hive/.../HiveConnector.java`（`getCapabilities` 在 `:297`） | 声明建表引擎名 `hive`（目录类型是 `hms`，这里名字不同是**刻意的**，要写注释）；能力集加分区位与分桶位 |
| `fe-connector-iceberg/.../IcebergConnector.java`（`getCapabilities` 在 `:815`） | 声明 `iceberg`；能力集加分区位（**不加**分桶位） |
| `fe-connector-paimon/.../PaimonConnector.java`（`getCapabilities` 在 `:322`） | 声明 `paimon`；能力集加分区位 |
| `fe-connector-maxcompute/.../MaxComputeDorisConnector.java`（`:50`，目前没有 `getCapabilities` 覆写） | 声明 `maxcompute`；新增 `getCapabilities` 覆写，加分区位与分桶位 |
| `fe-core/.../plugin/PluginDrivenExternalCatalog.java` | 新增一个不强制初始化的建表声明读法（连接器字段为空时才 `makeSureInitialized()`），并补一个读能力位的同款方法供 `analyzeEngine` 用 |
| `fe-core/.../info/CreateTableInfo.java` | 删 `pluginCatalogTypeToEngine`（`:907-932`，含那段「两个 switch 必须同步」的注释）；`paddingEngineName`（`:896-900`）与 `checkEngineWithCatalog`（`:389-392`）改读声明；`checkEngineName`（`:955-958`）追加一个析取项；`analyzeEngine`（`:1134-1145`）两条允许列表改读能力位。`ENGINE_*` 常量与或链里的既有名字**全部保留** |
| `fe-core` 单元测试 `CreateTableInfoEngineCatalogTest.java` | 现有 9 个用例的桩从「mock `getType()` 返回类型串」改成「mock 建表声明」，并**新增**分区/分桶两条子句门的用例（现在完全没有覆盖） |
| 端到端用例（见第六节） | 先固化文案断言，再改代码 |

`analyzeEngine` 有第二个调用者 `CreateMTMVInfo.java:291`，但它在 `:283` 把引擎名固定设为 `ENGINE_OLAP`，会走 `!olap` 判断而跳过整段，因此不受影响——改完要复核这一点仍然成立。

### 5.3 明确不要顺手做的事

- **不要删或收窄 `checkEngineName` 的或链**。`mysql` / `odbc` / `broker` / `elasticsearch` / `jdbc` 是用户可见的历史 SQL 语法，`InternalCatalog`（`fe/fe-core/.../datasource/InternalCatalog.java:1268-1295`）为每一个都准备了专门的下线文案。更要紧的是：`iceberg` / `paimon` / `maxcompute` 也**必须留在或链里**——`CREATE TABLE ... ENGINE=jdbc` 写在一个 iceberg 目录里，今天先过或链、再被目录一致性检查拒掉，文案是 `This catalog can only use \`iceberg\` engine`，而这条断言在 `regression-test/suites/external_table_p0/iceberg/write/test_iceberg_create_table.groovy:70-72` 与 `.../hive/ddl/test_hive_ddl.groovy:730-733` 都是活的。把或链改成「只接受已声明的名字」会让文案变成 `Do not support table with engine name = jdbc`，直接挂两个端到端用例。
- **不要合并「建表引擎名」与「展示引擎名」**。`PluginDrivenExternalTable.getEngine()`（`:1267-1298`）是另一件事，`hms` 目录建表用 `hive`、展示必须是 `hms`，`max_compute` / `trino-connector` 的展示名甚至是 `null`。那段 switch 属于另一个任务，本任务只删建表侧这一张表，并把注释里那句「两个 switch 必须同步」的约束**如实改写**成「展示侧仍是独立的一张表」。
- **不要动 `elasticsearch` 的前置分支**。它的分桶文案（`could not support distribution clause`）与「不检查分区」的怪癖都要原样保留。把它并进通用分支会同时改掉一条文案、新增一条拒绝。
- **不要让 hive 连接器声明它的 iceberg 兄弟的引擎名**。hms 目录上 `ENGINE=iceberg` 今天是被拒的（`test_hive_ddl.groovy:725-728` 有断言）。建表引擎名保持单值。
- **不要顺手把建表时的分区列校验从连接器搬回引擎**，也不要在 `fe-core` 里新增任何属性解析或分区推导 helper。本阶段 `fe-core` 只出不进。
- **不要为这两个能力位写 shell 或正则的构建门禁**。这类不变量改完之后由类型系统保证，语义级校验交给单测与评审。

## 六、怎么验证

**必须先固化、再改代码。** 第一步只写测试、不动一行生产代码，跑通并确认全部通过（说明它们抓的是当前行为）；第二步再改代码，要求同一套测试仍然全绿。

**第一步：把五种形态的错误文案固化成断言。**

单元测试放在既有的 `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfoEngineCatalogTest.java`（已有 264 行，覆盖了前四种形态中的三种），补齐到六条：

| 形态 | 断言 | 现状 |
|---|---|---|
| 省略 `ENGINE` | maxcompute 目录补出 `maxcompute`、hms 目录补出 `hive`（含 CTAS 入口） | 已有（`:119-131`、`:203-235`） |
| 显式正确 `ENGINE` | `checkEngineWithCatalog` 不抛 | 已有（`:166-173`、`:249-256`） |
| 显式错误 `ENGINE` | 抛 `AnalysisException`，消息**逐字**为 `This catalog can only use \`<名字>\` engine.` | 已有但只断言了抛异常，**要补上文案逐字断言** |
| jdbc / es 目录建表 | 省略 `ENGINE` 抛出且消息含 `does not support create table`；显式 `ENGINE=jdbc` 不在一致性检查处抛 | 已有（`:175-193`） |
| 带 `PARTITION BY` | 声明分区位的目标放过；未声明的抛 `Create <engine> table should not contain partition desc` | **完全没有覆盖，必须新增** |
| 带 `DISTRIBUTED BY` | 声明分桶位的目标放过；未声明的抛 `Create <engine> table should not contain distribution desc`；`elasticsearch` 仍抛 `could not support distribution clause` | **完全没有覆盖，必须新增** |

新增的两条要覆盖「两条检查的相对顺序」：构造一个同时带 `PARTITION BY` 和 `DISTRIBUTED BY` 的建表信息，打到一个两位都不声明的目标上，断言**先**报分桶文案。本仓库出过把连接器内多阶段校验合并后丢掉优先级、导致建表用例失败的事故，这条顺序断言就是防它复发的。

**变异验证**（每条新增用例都要做一次，确认它真的会失败）：把连接器的分区位声明去掉 → 分区放过用例必须失败；把 `analyzeEngine` 里两条检查换个顺序 → 顺序用例必须失败；把建表声明改成返回空 → 补引擎名用例必须失败。

**第二步：端到端回归（本任务的必需项，不是兜底项）。**

改动会经过分析期的四道门，任何文案漂移都会被现成用例抓到，必须实跑：

- `regression-test/suites/external_table_p0/iceberg/write/test_iceberg_create_table.groovy` —— `:60-62` 外部目录用 `ENGINE=olap`、`:64-72` 两条错误引擎名、`:74-76` 省略 ENGINE 与正确 ENGINE 的成功建表、以及 `ORDER BY` 一组（顺带回归第 5.1 节提到的既有能力位范式）。
- `regression-test/suites/external_table_p0/hive/ddl/test_hive_ddl.groovy` —— `:720-735` 的错误引擎名一组；`:342` / `:552` / `:599` 等 `PARTITION BY LIST` 成功建表；`:437` 与 `:460` 两处带 `DISTRIBUTED BY HASH(...) BUCKETS 16` 的建表——**注意这两处都是期望抛异常的否定用例，不是成功建表**：`:437` 那条写了 `ENGINE=olap`，期望 `Cannot create olap table out of internal catalog...`（`:442`）；`:460` 那条期望 `Create hive bucket table need set enable_create_hive_bucket_table to true`（`:465`），拒绝发生在 `analyzeEngine` 之后的 hive 建表阶段，所以它只能**间接**证明 `analyzeEngine` 放行了分桶子句。

  **分桶子句今天没有正向端到端护栏，本任务必须自己补一条。** 已实测：全仓 `regression-test` 里 `enable_create_hive_bucket_table` 只有上面那一处否定断言，插件目录上「带分桶子句成功建表」的用例一个都不存在（其余 `DISTRIBUTED BY` 命中全部是内部目录的 olap 表）。所以搬迁 `analyzeEngine` 的分桶允许列表时，**不能指望现成用例兜底**：必须新增一条 hive 目录上打开 `enable_create_hive_bucket_table` 后带 `DISTRIBUTED BY HASH(...) BUCKETS N` 成功建表并能写入读回的用例。它是本任务唯一能证明「分桶位声明真的放行了这条子句」的端到端证据；本地无集群时不能因此跳过——跑不了就必须在提交说明里写明该用例已写好但未跑，不要当它通过。作为兜底，第一步单测里那条「声明分桶位的目标放过」的用例必须同批加上（见上表最后一行）。
- `regression-test/suites/external_table_p0/hive/ddl/test_hive_ctas.groovy` —— CTAS 入口的补引擎名路径。
- `regression-test/suites/external_table_p0/iceberg/write/test_iceberg_write_partitions.groovy:35` 起 —— iceberg 分区建表。
- paimon 侧带建表的用例（`regression-test/suites/external_table_p0/paimon/` 下 `test_paimon_table.groovy`、`test_paimon_schema_metadata_atomicity_matrix.groovy` 等）。
- `regression-test/suites/external_table_p2/maxcompute/test_max_compute_create_table.groovy` —— maxcompute 是唯一同时声明两个能力位的连接器；这套用例在第二档环境，跑不了就必须在提交说明里写明未跑，不要默认它通过。

**第三步：显式列出并逐条确认「本来就会失败、只是文案变了」的边角组合。** 把判据从引擎名换成目标目录之后，下面这些组合的报错文案会变（它们改前改后都是拒绝，只是拒绝的位置提前了），动手前要逐条实测确认没有用例依赖旧文案：

- 内部目录里写 `ENGINE=hive` 且带 `PARTITION BY`：原本先过 `analyzeEngine`、由 `InternalCatalog` 抛「不能在内部目录建 hive 表」，改后在 `analyzeEngine` 就抛分区文案。
- jdbc 目录里写 `ENGINE=hive` 且带 `PARTITION BY`：原本按引擎名放过分区、到连接器才失败，改后在 `analyzeEngine` 抛分区文案。
- 已下线的 `doris` / `test` 类型外部目录（`CatalogFactory.java:141-153`，非插件目录）：不带 `ENGINE` 时仍应抛 `Current catalog does not support create table`。

若某条组合被现成用例断言了旧文案，就地把该条改成「保留原判据」而不是硬改文案——本任务的目标是搬迁判据，不是改用户可见行为。

**第四步：编译门禁。** 全反应堆**含测试源**的 `test-compile` 通过（绝对路径 `-f`，禁用任何跳过测试编译的参数），这是最强的单一符号级信号。跑单测时必须禁用 maven build cache，否则 surefire 会被静默跳过而 `BUILD SUCCESS` 是空的。删除 `pluginCatalogTypeToEngine` 后对该符号名全仓复扫应为零命中（注意 `PluginDrivenExternalTable` 与测试注释里都提到过它，要一并改掉说法）。

## 七、风险与回退

**风险一：文案漂移。** 四道门共十余条用户可见文案，端到端用例只覆盖了其中一部分。缓解手段就是第六节的「先固化再改」，以及第三步那张边角组合清单——不要靠「跑一遍绿了」当结论，要能说出每条文案在改后由谁抛出。

**风险二：校验顺序被打乱。** 四道门的顺序（补名 → 名字合法 → 与目录匹配 → 子句允许）和 `analyzeEngine` 内部「先分桶后分区」的顺序都承载了具体文案。本仓库已有先例：把多阶段校验合并后丢掉优先级，导致建表用例失败。要求：不合并任何两道门，`analyzeEngine` 内两条 `throw` 位置不互换，并用一条同时触发两条检查的单测把顺序钉死。

**风险三：把目录初始化时机提前。** `getConnector()` 会强制初始化目录。若在 `paddingEngineName`（`:413`，四道门里最早的一道）直接用它，一个远端不可达的目录上 `CREATE TABLE` 的报错会从引擎名相关文案变成连接失败。缓解：按 5.1 用不强制初始化的读法，只在连接器字段为空时才初始化。要专门测一条：连接器为空的降级目录上建表，报错仍指向「插件未加载」。

**风险四：插件与引擎版本错配。** 新增的默认方法与两个枚举值都是向后兼容的（旧连接器不覆写=不支持建表，与今天的 jdbc/es 行为一致），但反过来不成立：**新连接器包配旧 FE** 会因为枚举值不存在而在读取能力集时炸类加载。这与本仓库既有的插件版本纪律一致，要在提交说明里写明「连接器插件包与 FE 必须同批部署」，并做一次插件包重部署冒烟。

**回退**：改动集中在一个 `fe-core` 文件加四个连接器的少量声明，没有持久化格式、没有 thrift 有线格式、没有新增用户可见语法，直接整体 revert 即可，无需数据迁移。分两个提交做更稳：第一个提交只加测试（此时全绿），第二个提交搬迁判据；出问题只回退第二个。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`：第 4.1 节第（2）小节「`CREATE TABLE` 的四处协同改动」——本任务的来源，列出 engine 常量、type→engine switch、engine 白名单、分区/分桶允许列表这四处；同节第（1）小节是目录类型白名单的删除，与本任务同源但不同文件。附录 A 第 4 条的复核——把「四处」收窄为「三个协同编辑点」，与本文第二节一致。
- 同一份报告第十五节的整治路线表第 10 批「建表能力下沉」——把本任务标为风险 **高**，明确要求端到端兜底且错误文案与校验优先级逐字保持；第 17.1 节「合理的本地化（不要改回去）」表格中「引擎名白名单」那一行——说明白名单本身合理、不可整体移除，只该把外部目录那一段变成连接器声明，这是本任务范围的边界依据。
- 同目录 `tasks/07-write-down-the-design-rules.md`：能力位该放枚举还是放 provider 的分层判据；本任务新增的两个能力位属于「建表子句门」，与 `SUPPORTS_SORT_ORDER` 同族，落在枚举里。
- `plan-doc/HANDOFF.md`：动手前先读，再对照真实代码复核本文行号。
