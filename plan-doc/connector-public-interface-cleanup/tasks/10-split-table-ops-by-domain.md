# 10. 把 ConnectorTableOps 按域拆成父接口，并为每域写清最少实现集

> **优先级**：第二优先级（零破坏重构） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-api`（主源 + 测试源）。**不改任何连接器模块，也不改 `fe-core`。**
> **预计改动规模**：新增 7 个源文件（6 个域接口 + 1 个标记注解）、1 个测试类、1 个基线资源文件；改写 `ConnectorTableOps.java`（504 行 → 约 60 行的聚合）；修 9 处 javadoc 引用（分布在 5 个文件里）。约 15 个文件；净新增代码量很小，主体是方法搬移与每个接口的类文档。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

`ConnectorTableOps` 是一个 504 行、46 个方法、全部带默认实现的巨接口，八类互不相干的职责挤在一起；因为所有方法都有默认实现，一个新连接器**被编译器强制实现的方法数是 0**——编译能过，但一行也不工作，作者只能靠抄别的连接器猜该覆写哪些。本任务把它按域拆成 6 个父接口，`ConnectorTableOps` 保留为它们的聚合，并**在每个域的类文档里写清「最少实现集」**，同时给最少实现集一个机器可读的标记。这是零破坏重构：连接器一行不改、编译期完全兼容。

## 二、背景：现在的代码是怎么写的

`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorTableOps.java`，共 504 行，接口声明在第 40 行，里面 46 个方法**全部**是 `default`（已逐行核实：文件内不存在任何抽象方法）。按声明顺序，它混着这些职责：

| 职责 | 方法（行号） | 方法数 |
|---|---|---|
| 表句柄解析与表名列举 | `getTableHandle`(43)、`listTableNames`(177) | 2 |
| 系统表发现 | `listSupportedSysTables`(57)、`getSysTableHandle`(70)、`isPartitionValuesSysTable`(88) | 3 |
| 读 schema / 列句柄（各含一个带快照的重载） | `getTableSchema`(94, 108)、`getColumnHandles`(133, 154)、`supportsColumnHandleSnapshotPin`(172) | 5 |
| 渲染建表语句 / 表注释 / 主键 | `renderShowCreateTableDdl`(127)、`getTableComment`(423)、`getPrimaryKeys`(417) | 3 |
| 视图 | `viewExists`(187)、`listViewNames`(196)、`getViewDefinition`(207)、`dropView`(218) | 4 |
| 表级 DDL | `createTable`(223 旧窄重载, 241 全量重载)、`dropTable`(252)、`renameTable`(259)、`truncateTable`(274) | 5 |
| 列演进（含嵌套列） | `addColumn`(286)、`addColumns`(292)、`dropColumn`(298)、`renameColumn`(304)、`modifyColumn`(314)、`reorderColumns`(320)、`addNestedColumn`(340)、`dropNestedColumn`(346)、`renameNestedColumn`(352)、`modifyNestedColumn`(363)、`modifyColumnComment`(369) | 11 |
| 分支与标签 | `createOrReplaceBranch`(375)、`createOrReplaceTag`(381)、`dropBranch`(387)、`dropTag`(393) | 4 |
| 分区规格演进 | `addPartitionField`(399)、`dropPartitionField`(405)、`replacePartitionField`(411) | 3 |
| 执行裸 SQL / 透传查询取列 | `executeStmt`(432)、`getColumnsFromQuery`(440) | 2 |
| 构造 thrift 表描述符 | `buildTableDescriptor`(464) | 1 |
| 分区列举 | `listPartitionNames`(476)、`listPartitions`(487)、`listPartitionValues`(499) | 3 |

合计 46。它被 `ConnectorMetadata` 继承（`ConnectorMetadata.java:44-51` 的 `extends` 列表，`ConnectorTableOps` 在第 46 行）。

**本任务成立的关键事实（已在 `HEAD` 上重新核实）**：全仓库没有任何一处把 `ConnectorTableOps` 当成静态类型使用——没有变量、参数、返回值、字段、泛型实参用它。全部命中只有三类：

- `ConnectorMetadata.java:46` 的 `extends` 一行；
- javadoc 与注释里的引用：`fe-core` 侧 8 处（`PluginDrivenExternalCatalog.java:393/572/651/692/797/903/1003/1084`，全是 `{@code}` 文本）、`fe-connector-api` 侧 11 处非声明引用；
- 各连接器里的分节注释（如 `HiveConnectorMetadata.java:386` 的 `// ========== ConnectorTableOps ==========`），以及 `fe-core` 测试里的一处分节注释（`FakeConnectorPluginTest.java:114`）。

其中真正需要在本任务里动的是 7 处 **成员级** javadoc 链接：`ConnectorCapability.java:36`（指向 `getColumnsFromQuery`）、`:42`（`listPartitions`）、`:89`（`viewExists`）、`:90`（`listViewNames`），以及 `ConnectorColumnPosition.java:24-25`（`addColumn` / `modifyColumn`）、`ConnectorMvccPartitionView.java:29`（`listPartitions`），另有两处成员级的 `{@code}` 文本引用同样会因拆分变成陈旧描述，要一并改准：`ConnectorViewDefinition.java:27`（`ConnectorTableOps.getViewDefinition`）与 `ConnectorCreateTableRequest.java:30`（`ConnectorTableOps.createTable(session, request)`）。指向类型本身的链接（`ConnectorCapability.java:175`、`ConnectorColumnPath.java:28`）不用动，因为聚合接口名保留。

## 三、为什么这是个问题

**第一，「新连接器该覆写什么」这个信息今天在代码里根本不存在。** `ConnectorMetadata` 加它继承的 6 个 `Ops` 子接口一共 81 个方法，`default` 计数分别是：`ConnectorMetadata` 自身 11、`ConnectorSchemaOps` 7、`ConnectorTableOps` 46、`ConnectorPushdownOps` 4、`ConnectorStatisticsOps` 5、`ConnectorWriteOps` 5、`ConnectorIdentifierOps` 3——**抽象方法 0 个**。也就是说 `class XConnectorMetadata implements ConnectorMetadata { }` 是一个合法的、能编译过的空实现。

**第二，「必须实现」的方法里有一半的默认值是静默的空值，不是报错。** 这才是「编译能过但一行不工作」的具体机制：`getTableHandle` 默认返回 `Optional.empty()`（46 行）、`listTableNames` 默认返回空列表（179 行），都不报错；只有 `getTableSchema`(97) 与 `getColumnHandles`(136) 是 fail-loud 的。所以一个漏实现的连接器不会在启动时炸，而是在用户面前表现成「目录是空的」。

**第三，连维护者自己都记不清有没有强制方法。** `fe/fe-connector/fe-connector-api/src/test/java/org/apache/doris/connector/api/ConnectorSchemaOpsDefaultsTest.java:38` 的注释写的是「A bare metadata implementing only the one abstract SPI method」——这句话在 `HEAD` 上已经是错的（没有抽象方法了）。文档与代码脱节到这个程度，说明「靠口头传承最少实现集」不可行。

**第四，四组只有一个数据源用得上的方法对其余连接器是纯噪音**：分支与标签（4 个）、分区规格演进（3 个）、执行裸 SQL、透传查询取列。一个只读连接器的作者要在 46 个方法里逐个判断「这个跟我有关吗」。

需要说明的是：**目标不是把接口拆到多小**。Trino 的 `ConnectorMetadata` 是上百个全默认方法的巨接口，同样靠文档告诉实现者最少实现集——所以 46 个全默认方法本身不异常。真问题是**没有分域、也没有写最少实现集**。

调研实测的覆写分布可以直接当作最少实现集的证据（统计口径：各连接器元数据实现类里 `@Override` 且方法名属于 `ConnectorTableOps`）：

| 连接器 | 覆写的 `ConnectorTableOps` 方法名个数 |
|---|---|
| iceberg | 35 |
| hive | 31 |
| paimon | 13 |
| maxcompute | 10 |
| jdbc | 9 |
| hudi | 8 |
| es | 5 |
| trino | 4 |

8 个连接器**无一例外**都覆写的是 4 个：`getTableHandle`、`listTableNames`、`getTableSchema`、`getColumnHandles`。再加上 8 个里有 7 个覆写的 `buildTableDescriptor`（唯一的例外是 trino，它吃引擎的通用兜底描述符），构成「表基础」域的无条件最少实现集，不是拍脑袋定的。

## 四、用一个最小例子说明

假设我要新增一个连接器 X。我写下这一个类，它能编译过：

```java
public class XConnectorMetadata implements ConnectorMetadata {
}
```

然后用户在 `x` 目录上做这几件事：

| 用户写了什么 | 今天实际发生什么 | 应该发生什么 |
|---|---|---|
| `SHOW TABLES FROM x.db` | 返回**空列表**，不报错（`listTableNames` 默认返回空列表） | 作者一开始就知道 `listTableNames` 属于「表基础」域的无条件最少实现集 |
| `SELECT * FROM x.db.t` | 报「表不存在」（`getTableHandle` 默认返回 `Optional.empty()`），像是用户打错了表名 | 同上，`getTableHandle` 在最少实现集里 |
| `DESC x.db.t` | 抛 `getTableSchema not implemented` | 唯一一条今天就能告诉作者「你漏了」的路径 |
| `ALTER TABLE x.db.t ADD BRANCH b1` | 抛「CREATE/REPLACE BRANCH not supported」 | 这一族本就与 X 无关；拆分后它在独立的「快照引用」域里，作者一眼就知道整域可以不看 |

第一行的「返回空列表不报错」有测试固化：`fe/fe-core/src/test/java/org/apache/doris/connector/fake/FakeConnectorPluginTest.java:117-118`（`// SHOW TABLES against an unimplemented connector returns empty rather than throwing.`）。这是有意的设计，不是缺陷——但正因为默认值是静默的，「哪些必须实现」就必须写在文档里，否则无处可寻。

## 五、解决方案

### 5.1 目标状态

`ConnectorTableOps` 变成一个不声明任何方法的聚合接口（外加两个暂未归域的残留方法），46 个方法按域分到 6 个新接口。**签名一个字都不改，包名不变（都在 `org.apache.doris.connector.api`）。**

```java
public interface ConnectorTableOps extends
        ConnectorTableMetadataOps,      // 表基础：14
        ConnectorViewOps,               // 视图：4
        ConnectorTableDdlOps,           // 表级 DDL：5
        ConnectorColumnEvolutionOps,    // 列演进：11
        ConnectorSnapshotRefOps,        // 快照引用与分区规格演进：7
        ConnectorPartitionListingOps {  // 分区列举：3

    // 暂未归域（2 个）：jdbc 直通。等「把 jdbc 直通摘成可选接口」那一批处理，
    // 现在放在聚合上而不是硬塞进某个域，避免给它们一个错误的归属。
    default void executeStmt(ConnectorSession session, String stmt) { ... }
    default ConnectorTableSchema getColumnsFromQuery(ConnectorSession session, String query) { ... }
}
```

6 个域接口与各自的最少实现集（这一节的内容就是要写进各接口类文档的正文）：

**1. `ConnectorTableMetadataOps`（14 个）**：`getTableHandle`、`listTableNames`、`getTableSchema`×2、`getColumnHandles`×2、`supportsColumnHandleSnapshotPin`、`getTableComment`、`getPrimaryKeys`、`renderShowCreateTableDdl`、`listSupportedSysTables`、`getSysTableHandle`、`isPartitionValuesSysTable`、`buildTableDescriptor`。
- 无条件必须：`getTableHandle`、`listTableNames`、`getTableSchema(session, handle)`、`getColumnHandles(session, handle)`（8/8 连接器全覆写）、`buildTableDescriptor`（8 个里 7 个覆写，只有 trino 未实现；它的唯一消费方是 `PluginDrivenExternalTable.java:1343`，返回 `null` 会退到引擎的通用兜底描述符，这也是 trino 今天能不实现的原因）。
- 支持时间旅行或模式演进才必须：`getTableSchema(..., snapshot)`、`getColumnHandles(..., snapshot)`、`supportsColumnHandleSnapshotPin`（三者要么全实现要么全不实现；只实现前两个而不声明第三个，会让引擎跳过「绑定列必须有句柄」的 fail-loud 检查）。
- 暴露系统表才必须：`listSupportedSysTables` + `getSysTableHandle`；`isPartitionValuesSysTable` 只在该系统表走通用分区值表函数时覆写。
- 其余按需：`getTableComment`、`getPrimaryKeys`、`renderShowCreateTableDdl`。

**2. `ConnectorViewOps`（4 个）**：`viewExists`、`listViewNames`、`getViewDefinition`、`dropView`。
- 最少实现集：整域可空。声明 `ConnectorCapability.SUPPORTS_VIEW` 后，`viewExists` + `getViewDefinition` 必须（否则 `getViewDefinition` 的默认会抛）；`listViewNames` 只在 `listTableNames` **不**含视图时必须（iceberg 属于这种）；`dropView` 只在支持 `DROP VIEW` 时。

**3. `ConnectorTableDdlOps`（5 个）**：`createTable`×2、`dropTable`、`renameTable`、`truncateTable`。
- 最少实现集：支持建表就必须实现 `createTable(session, request)` 这个**全量**重载，**不要**只实现旧的窄重载——全量重载的默认实现（241-249 行）会把 `PARTITION BY` / 分桶 / `EXTERNAL` / `IF NOT EXISTS` 静默丢掉，只实现窄签名的后果是「建表成功但分区丢了」。这一条必须在类文档里写成警告。
- 其余按需：`dropTable`、`renameTable`、`truncateTable`。

**4. `ConnectorColumnEvolutionOps`（11 个）**：顶层 6 个 + 嵌套 4 个 + `modifyColumnComment`。
- 最少实现集：整域可空。支持列变更时顶层 6 个（`addColumn`、`addColumns`、`dropColumn`、`renameColumn`、`modifyColumn`、`reorderColumns`）成组实现（hive 与 iceberg 都是这 6 个全覆写）；嵌套 4 个 + `modifyColumnComment` 只在支持嵌套列演进时（目前只有 iceberg）。原文件 325-333 行那段说明嵌套路径约定的注释要整段搬到这个接口的类文档里。

**5. `ConnectorSnapshotRefOps`（7 个）**：分支标签 4 个 + 分区规格演进 3 个。
- 最少实现集：整域可空，这是给「有快照引用概念」的数据源的。要点：分支/标签 4 个方法要么全实现要么全不实现——只实现一半会让 `CREATE BRANCH` 成功而 `DROP BRANCH` 报「不支持」。

**6. `ConnectorPartitionListingOps`（3 个）**：`listPartitionNames`、`listPartitions`、`listPartitionValues`。
- 最少实现集：分区表连接器必须 `listPartitionNames` + `listPartitions`。`listPartitionValues` **不要**实现：它零生产调用方（详见任务清单里删死接口那一批），文档必须点明，否则新作者会照着现有的三个实现继续抄。

**机器可读的标记**：在 `fe-connector-api` 新增一个纯文档用途的注解，作为最少实现集的**唯一真源**（类文档负责解释「为什么」，注解负责「是哪些」）：

```java
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConnectorMustImplement {
    /** 空串 = 无条件必须；否则写触发前提（能力名或一句话条件）。 */
    String when() default "";
}
```

`RUNTIME` 保留期只为让单元测试能反射读到它。**生产代码不读它，不做任何运行时校验，也不加 shell/正则门禁**——本仓库已有结论：那类门禁只适合存在性与前缀类不变量，要理解语言语义就会误报，而误报比漏报更毒。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `.../connector/api/ConnectorTableMetadataOps.java` | 新建。搬入 14 个方法（含两组带快照的重载与三个系统表方法）+ 类文档写最少实现集 |
| `.../connector/api/ConnectorViewOps.java` | 新建。搬入 4 个视图方法 + 类文档 |
| `.../connector/api/ConnectorTableDdlOps.java` | 新建。搬入 5 个表级 DDL 方法 + 类文档（含「别只实现窄重载」警告） |
| `.../connector/api/ConnectorColumnEvolutionOps.java` | 新建。搬入 11 个列演进方法 + 原 325-333 行嵌套路径说明整段搬入类文档 |
| `.../connector/api/ConnectorSnapshotRefOps.java` | 新建。搬入分支标签 4 个 + 分区规格演进 3 个 + 类文档 |
| `.../connector/api/ConnectorPartitionListingOps.java` | 新建。搬入 3 个分区列举方法 + 类文档（含 `listPartitionValues` 无调用方的提示） |
| `.../connector/api/ConnectorMustImplement.java` | 新建注解 |
| `.../connector/api/ConnectorTableOps.java` | 改写成 `extends` 6 个域接口的聚合，只保留 `executeStmt` / `getColumnsFromQuery` 两个残留方法；`import` 相应收缩 |
| `.../connector/api/ConnectorCapability.java` | 改 4 处成员级 javadoc 链接（36 / 42 / 89 / 90 行）指向新接口 |
| `.../connector/api/ddl/ConnectorColumnPosition.java` | 改 2 处链接（24-25 行）指向 `ConnectorColumnEvolutionOps` |
| `.../connector/api/mvcc/ConnectorMvccPartitionView.java` | 改 1 处链接（29 行）指向 `ConnectorPartitionListingOps` |
| `.../connector/api/ConnectorViewDefinition.java` | 改 1 处 `{@code}` 文本引用（27 行）指向 `ConnectorViewOps.getViewDefinition` |
| `.../connector/api/ddl/ConnectorCreateTableRequest.java` | 改 1 处 `{@code}` 文本引用（30 行）指向 `ConnectorTableDdlOps.createTable(session, request)` |
| `.../api/src/test/.../ConnectorMetadataSurfaceTest.java` | 新建（见第六节） |
| `.../api/src/test/resources/connector-metadata-methods.txt` | 新建基线：拆分**前**生成的 `ConnectorMetadata` 方法签名清单 |

搬移时的三个机械要点：

1. **默认实现体里的方法调用不跨域**，已逐条核实：`getTableSchema(..., snapshot)` 调 `getTableSchema(...)`（同域）、`getColumnHandles(..., snapshot)` 调 `getColumnHandles(...)`（同域）、`createTable(request)` 调 `createTable(schema, props)`（同域）。所以 6 个域接口**互不继承**，也不会出现同一方法在两个域里声明的钻石问题。
2. `import` 要按域重新分配，`UnusedImports` 与 `CustomImportOrder`（`fe/check/checkstyle/checkstyle.xml:160-167`）会卡住遗漏。
3. 域接口内的 `{@link #xxx}` 若目标方法落在别的域里，要改成 `{@link ConnectorXxxOps#xxx}`。已知一处：`listViewNames` 的文档引用了 `listTableNames`。

### 5.3 明确不要顺手做的事

- **不要改任何方法签名，不要合并重载，不要删任何方法。** 删 `listPartitionValues`、收 `createTable` 旧窄重载、删 `getPrimaryKeys`，都在「删死接口」那两批里，各自有独立的连带改动（要动连接器与单测）。本任务混进去就不再是零破坏，也会让第六节那条「方法集合完全一致」的断言失效。
- **不要动连接器**，包括那些 `// ========== ConnectorTableOps ==========` 分节注释：聚合接口名保留，注释仍然准确；改它会把一个纯公共模块的改动扩散成 8 个模块。
- **不要把 `buildTableDescriptor` 的 7 个散列标量参数改成传句柄。** 那是 thrift 边界那一批的事，本任务只给它安个域。
- **不要给注解加运行时校验**，也不要在 `Connector` 注册路径上做任何检查——那会把每个连接器的元数据对象构造提前，本仓库已有先例说明代价（见 `ConnectorContractValidator` 类文档解释为什么校验放在契约测试而不是注册路径）。
- **不要顺手把另外 5 个 `Ops` 子接口也拆了。** 它们分别只有 3～7 个方法，不构成可发现性问题；本任务只需要在各自类文档里补最少实现集就够，但那属于「文档据实」那一批。
- **不要写 shell 门禁**去校验「新连接器是否实现了标记方法」。

## 六、怎么验证

**第一，全反应堆含测试源编译（最强的单一符号级信号）**：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -Dmaven.build.cache.enabled=false
```

必须 `BUILD SUCCESS`，且**禁用**任何跳过测试编译的参数。这一条通过就直接证明了零破坏：8 个连接器与 `fe-core` 的全部 `@Override` 仍然绑得上。（注意 `fe/.mvn/maven.config` 已带 `-Dmaven.build.cache.cacheCompile=false`，但跑测试时仍要显式关掉整个构建缓存，否则 surefire 会被静默跳过而 `BUILD SUCCESS` 是空的。）

**第二，方法集合冻结测试**（新建 `ConnectorMetadataSurfaceTest`，junit5，与 `fe-connector-api` 现有测试同风格）：

1. **拆分前**在 `HEAD` 上生成基线：反射 `ConnectorMetadata.class.getMethods()`，过滤掉 `isSynthetic()`，把「方法名 + 参数类型全限定名列表」渲染成每行一条、排序后写入 `src/test/resources/connector-metadata-methods.txt`。按上面的计数，基线应为 **81 条**（11+7+46+4+5+5+3；`close()` 已被 `ConnectorMetadata.java:232` 的默认实现覆盖，计在 11 里）——但要**机械生成**，不要照抄这个数字。
2. 测试断言：运行时算出的集合与基线文件**完全相等**（不只是数量相等；不相等时把差集打印出来，方便判断是漏搬还是签名手误）。
   - 这条能失败的场景：搬移时手抖改了参数类型、漏搬一个方法、把某个重载写成了同一签名。
   - 这条测试在后续「删死接口」批次里会红——那是**故意的**，基线文件必须跟着那一批一起有意识地更新，这正是给公共接口加的减速带。类文档要写明这一点。
3. 断言每个 `@ConnectorMustImplement` 标记都落在 6 个域接口之一**自己声明**的方法上（用 `getDeclaredMethods()` 判定），且 6 个域接口各至少有一个标记或在类文档里显式说明「整域可空」——防止标记随手打在聚合接口上，让「域 → 最少实现集」这个映射保持成立。
4. 断言无条件标记（`when()` 为空串）的方法集合恰好是那 5 个（`getTableHandle`、`listTableNames`、`getTableSchema(session, handle)`、`getColumnHandles(session, handle)`、`buildTableDescriptor`）。这条把「前 4 个 8/8 连接器实测都覆写、`buildTableDescriptor` 8 个里 7 个覆写」这个证据钉在测试里；将来要把第 6 个方法升为无条件必须，必须先改这条断言，从而被迫说明理由。

**变异验证**：第 2 条断言的变异是「从任一域接口里删掉一个方法」→ 必须红；第 4 条的变异是「把 `renderShowCreateTableDdl` 也标成无条件必须」→ 必须红（它只有 hive 一个实现）。两个变异都要实际跑一遍确认会红，而不是只在脑子里推。

**端到端回归**：不需要。本任务不改任何签名、不改任何默认实现体、不改任何连接器，运行时字节行为逐条不变。全反应堆含测试源编译 + `fe-connector-api` 与 `fe-core` 的既有单测（尤其 `FakeConnectorPluginTest`、`ConnectorSchemaOpsDefaultsTest`）通过即可。

## 七、风险与回退

风险低，来源只有两个，都是机械性的：

- **搬移时漏搬或改错签名。** 由第六节第 2 条断言 + 全反应堆含测试源编译双重兜底。任一环节红就是漏搬。
- **javadoc 链接指向搬走的方法。** 影响仅限文档渲染，不影响编译。`{@link ConnectorTableOps#addColumn}` 这种写法在方法搬到父接口后仍能解析（javadoc 会查继承来的成员），所以即使漏改也不会断链；但 5.2 列的那 9 处仍应改准，避免读者被指到聚合接口上找不到方法体。

回退成本近似于零：改动集中在一个模块的公共接口文件，`git revert` 单个提交即可，不涉及任何持久化格式、thrift 有线格式或连接器插件包。也**不涉及** Gson 持久化的类型标签——本任务不新增也不删除任何被持久化的类型。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`：
  - **第六节「主题三：大接口把互不相干的职责捆在一起」**（6.1 现状规模的接口尺寸表、6.2 哪些不是问题、6.3 建议的形状与「无一处当静态类型用」这条有利事实）；
  - **附录结论 113**（`ConnectorTableOps` 把 8 类职责与两种寻址风格捆在一个 46 方法接口里，判定「部分成立」，收窄理由值得一读）；
  - **第十五节「建议的整治路线」第 3 批**（本任务在整套路线里的位置：仅公共模块、无风险、为后续每一批划定域边界）；
  - **第七节 7.2 / 7.3**（后续要删的死接口面，含 `listPartitionValues`、`getPrimaryKeys`、`createTable` 旧窄重载——本任务只给它们安域、不动它们）。
- 与本任务紧邻的两个后续任务：把 jdbc 直通（`executeStmt` / `getColumnsFromQuery`）摘成可选接口；thrift 边界整治（`buildTableDescriptor` 的参数形状）。本任务刻意为这两个留了钩子（前者放在聚合上不归域，后者归入表基础但只安域不改形状）。

---

## 九、施工后订正（2026-07-25 落地）

**已完成。** 6 个域接口 + 1 个注解 + 冻结测试 + 基线资源，`ConnectorTableOps` 从 504 行缩到 69 行的聚合；连接器与 fe-core 零改动。

复核订正（**§三、§5.1 里下列说法以本节为准**）：

1. **§114 的「带快照的三个方法要么全实现要么全不实现」被推翻。** 实测只有 paimon 3/3；hive、hudi、iceberg 各只实现 `getTableSchema(..., snapshot)`，而且 `supportsColumnHandleSnapshotPin` 的注释里明确祝福了 iceberg 走 false 那条路。类文档因此写成：只实现 schema 那个是常态，列句柄的快照重载是**更强的**一步，只有当句柄按钉住的名字建键时才实现，并同时声明 pin。
2. **§5.1-5 的「分支/标签实现一半」在今天是假想风险**（iceberg 与 hive 都 4/4，分区规格演进也都 3/3）。但**同一失效模式在列演进域是真的**：网关委派了 6 个顶层列操作、漏了 5 个路径列操作。已作为独立缺陷修复（见 README「调研期发现、已修复的真实缺口」）。类文档按「假想风险 + 真实前例」写。
3. **`listViewNames` 的必要条件要写准**：只有当连接器的 `listTableNames` **减掉**视图时才必须实现——今天只有 iceberg，且只在启用视图目录时才减；非视图目录的 iceberg 目录 `listTableNames` 仍含视图。
4. **窄重载 `createTable(session, schema, properties)` 今天零实现**（4 个支持建表的连接器全部实现 request 重载），这条比文档原来的说法更有力，已写进类文档。
5. **`ConnectorTableOps` 原有 15 个 import（不是 16）**；拆完后聚合接口零 import。
6. **需要改的成员级引用是 9 处**，另有 2 处（iceberg 连接器里提到 `ConnectorTableOps.getTableComment` 的注释）只提聚合接口名、无需改动。跨域的 `{@link}` 只有一处（`listViewNames` → `listTableNames`），已改成指向新接口。
7. **`@ConnectorMustImplement` 的无条件集合**最终是 5 个：取表句柄、列表名、取 schema、取列句柄、构造表描述符。冻结测试第 4 条断言把它钉住。
8. **验证口径修正**：全反应堆 `test-compile` 必须**排除两个 shade 模块**（`fe-connector-hms-hive-shade`、`fe-connector-paimon-hive-shade`），否则 hive 相关模块必然报 `package org.apache.hadoop.hive.conf does not exist`——那是反应堆用未 shade 的 `target/classes` 解析依赖导致的，与改动无关。§六 的命令要按这个改。
9. **两个变异都实际跑过并确认变红**：删掉某个域接口里的一个方法 → 方法集合断言红；把 `renderShowCreateTableDdl` 标成无条件必须 → 无条件集合断言红。
