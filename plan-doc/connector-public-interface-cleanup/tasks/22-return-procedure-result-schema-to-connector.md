# 22. 把分布式过程的结果列定义交还连接器

> **优先级**：第五优先级（中立化） ｜ **风险**：中 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-api`、`fe-connector-iceberg`、`fe-core`（**净减少**：删掉硬编码的四列定义与 3 个随之失效的 import，只加一处 SPI 调用）
> **预计改动规模**：约 10 个文件；生产代码新增约 110 行、删除约 25 行，测试改动约 120 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

`ALTER TABLE ... EXECUTE <过程名>` 有两种执行方式：单次同步调用（`SINGLE_CALL`）的结果列由连接器自己返回，而分布式编排（`DISTRIBUTED`）的结果列却被硬编码在引擎里 —— `ConnectorRewriteDriver.buildResult` 直接写死了 iceberg `rewrite_data_files` 那一个过程的四个列名和四个列类型。本任务把这四列的定义搬回 iceberg 连接器，引擎改为只负责编排每组的 `INSERT-SELECT`、把每组统计原样汇总成一个中立的统计对象交给连接器去渲染成结果行。

## 二、背景：现在的代码是怎么写的

### 2.1 两种执行方式的分派

`ConnectorProcedureOps.getExecutionMode(String)`（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/procedure/ConnectorProcedureOps.java:64-66`）默认返回 `SINGLE_CALL`，连接器可以覆写成 `DISTRIBUTED`。引擎在 `ConnectorExecuteAction` 里按这个声明分派（`fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/execute/ConnectorExecuteAction.java:142`、`:158`）：

- `SINGLE_CALL`：调 `procedureOps.execute(...)`（`:176`），连接器返回一个 `ConnectorProcedureResult`（schema + 行），引擎只是 `wrapResult` 包成结果集（`:213-225`）。**列名列类型完全由连接器决定。**
- `DISTRIBUTED`：构造 `ConnectorRewriteDriver` 并调 `driver.run()`（`:162-166`），驱动器返回 `ConnectorProcedureResult`，同样交给 `wrapResult`。**但这个 `ConnectorProcedureResult` 是引擎自己拼的。**

全仓只有 iceberg 覆写了执行方式：`IcebergProcedureOps.getExecutionMode`（`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergProcedureOps.java:108-113`）在名字等于 `rewrite_data_files` 时返回 `DISTRIBUTED`，其余全是 `SINGLE_CALL`。

### 2.2 引擎里硬编码的四列

`ConnectorRewriteDriver.java:245-264`：

```java
private ConnectorProcedureResult buildResult(int rewrittenDataFilesCount, int addedDataFilesCount,
        long rewrittenBytesCount, int removedDeleteFilesCount) {
    // Four-column schema in the exact legacy order/types (IcebergRewriteDataFilesAction.getResultSchema);
    // rewritten_bytes_count is INT for byte-parity with legacy (a latent quirk kept on purpose).
    List<ConnectorColumn> schema = ImmutableList.of(
            new ConnectorColumn("rewritten_data_files_count", ConnectorType.of("INT"), ...),
            new ConnectorColumn("added_data_files_count", ConnectorType.of("INT"), ...),
            new ConnectorColumn("rewritten_bytes_count", ConnectorType.of("INT"), ...),
            new ConnectorColumn("removed_delete_files_count", ConnectorType.of("BIGINT"), ...));
    ...
}
```

四个列名、四个列类型、四段列注释文本，全部是 iceberg 这一个过程的历史行为，全部住在 `fe-core` 的一个通用引擎类里。

### 2.3 四个统计数字从哪来

`ConnectorRewriteDriver.java:177-183`：三个数字由引擎对连接器给出的分组对象求和，一个由事务在提交后回报。

```java
int addedDataFilesCount = rewriteTx.getRewriteAddedDataFilesCount();
int rewrittenDataFilesCount = groups.stream().mapToInt(ConnectorRewriteGroup::getDataFileCount).sum();
long rewrittenBytesCount = groups.stream().mapToLong(ConnectorRewriteGroup::getTotalSizeBytes).sum();
int removedDeleteFilesCount = groups.stream().mapToInt(ConnectorRewriteGroup::getDeleteFileCount).sum();
```

其中 `getRewriteAddedDataFilesCount()` 来自窄能力接口 `RewriteCapableTransaction`（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/handle/RewriteCapableTransaction.java:43-51`），只有提交后才有效。

还有一条**空计划早退路径**：`ConnectorRewriteDriver.java:122-125`，当连接器规划出零个分组时，引擎**不开事务**直接返回 `buildResult(0, 0, 0, 0)`。这条路径同样需要列定义，而它发生在任何远程调用之前。

### 2.4 连接器一侧本来就有现成的落点

`BaseIcebergAction` 已经有 `protected List<ConnectorColumn> getResultSchema()`（`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/action/BaseIcebergAction.java:140-142`，默认空表），构造时捕获一次（`:81`），八个单次调用过程都是通过覆写它来声明自己的结果列的。唯独 `IcebergRewriteDataFilesAction`（`.../action/IcebergRewriteDataFilesAction.java:47`）没有覆写它 —— 因为它的 `executeAction` 是个永不可达的守卫抛异常（`:168-176`），结果列被搬到引擎去了。上面那段引擎注释里的 `IcebergRewriteDataFilesAction.getResultSchema` 指的是**迁移前 fe-core 里的旧同名类**，现在的连接器类里并没有这个方法。

### 2.5 中立模块文档里的源专有列名

`ConnectorRewriteGroup`（`.../api/procedure/ConnectorRewriteGroup.java`）是中立分组对象，但它的文档用 iceberg 的结果列名来解释每个字段的用途：`:47`（`rewritten_data_files_count`）、`:49`（`rewritten_bytes_count`）、`:51`（`removed_delete_files_count`），三个 getter 注释 `:67`/`:72`/`:77` 又各重复一遍。`RewriteCapableTransaction.java:48` 也提到 `added_data_files_count`。

## 三、为什么这是个问题

1. **不对称**：同一个 SPI 的两种执行方式，一种把结果列的所有权交给连接器，另一种留在引擎。读代码的人无法从 `ConnectorProcedureOps` 的接口面看出「分布式过程的结果列谁定」。
2. **新连接器必须改公共模块**：任何连接器想加第二个分布式过程（例如 paimon 的 compact），它的结果列只能通过改 `fe-core` 的 `ConnectorRewriteDriver` 来表达。而 `fe-core` 当前阶段是「只出不进」，往里加第二个数据源的列定义方向就是错的。更糟的是：`buildResult` 只有一套列，两个连接器的两个分布式过程会在这里正面冲突，只能在通用引擎类里按过程名分支。
3. **一处历史遗留的类型 quirk 归属错了**：`rewritten_bytes_count` 声明成 `INT`，而它承载的值是 `long` 求和出来的字节数（`:179`）；对称地，`removed_delete_files_count` 声明成 `BIGINT`，而它承载的值是 `int`。这两处「类型与直觉不符」是那个 iceberg 过程的历史行为（为与迁移前逐字一致而**刻意保留**），不是引擎的行为规范。放在引擎里，它看起来像是「分布式过程的通用结果契约」，会被下一个连接器照抄。
4. 用户目前观察不到任何错误：iceberg 是唯一的分布式过程连接器，行为完全正确。**这是一个归属问题，不是正确性缺陷。** 但它是「新增连接器必须动公共模块」清单上的一项。

## 四、用一个最小例子说明

用户今天执行：

```sql
ALTER TABLE ice_ctl.db1.t EXECUTE rewrite_data_files("min-input-files" = "2");
```

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
|---|---|---|
| 上面这条 SQL | iceberg 连接器规划分组 → 引擎跑 N 个 `INSERT-SELECT` → 引擎**自己**造出 `rewritten_data_files_count / added_data_files_count / rewritten_bytes_count / removed_delete_files_count` 四列并填值 | iceberg 连接器规划分组 → 引擎跑 N 个 `INSERT-SELECT` → 引擎把四个统计数字打成中立统计对象交回连接器 → **连接器**造出同样的四列同样的值 |
| 结果集 | `+---+---+------+---+`（四列，值不变） | **逐字相同**（列名、列顺序、列类型、值全不变） |

再看「新增一个分布式过程」的成本。假设我要给 paimon 加一个分布式的 `compact` 过程，返回两列 `compacted_file_count` / `compacted_bytes`：

| 今天必须动的文件 | 改完之后必须动的文件 |
|---|---|
| `fe-connector-paimon`：覆写 `getExecutionMode` + 实现 `planRewrite` | 同左 |
| **`fe-core/ConnectorRewriteDriver.java`**：给 `buildResult` 加一个「如果是 paimon 的 compact 就换另一套列」的分支 | **不用动** |

## 五、解决方案

### 5.1 目标状态

引擎只做三件事：编排、把每组统计原样求和、把结果原样透传。列定义与行渲染全在连接器。

**新增一个中立的统计对象**（`fe-connector-api`，`org.apache.doris.connector.api.procedure` 包）：

```java
public final class ConnectorRewriteStatistics {
    // 前三项是引擎对连接器自己给出的每组 ConnectorRewriteGroup 数字做的直和（不做任何换算、不改单位）；
    // 最后一项来自 RewriteCapableTransaction#getRewriteAddedDataFilesCount()，仅提交后有效。
    public ConnectorRewriteStatistics(int dataFileCount, long totalSizeBytes,
            int deleteFileCount, int addedDataFileCount);
    public int getDataFileCount();
    public long getTotalSizeBytes();
    public int getDeleteFileCount();
    public int getAddedDataFileCount();
}
```

字段名与 `ConnectorRewriteGroup` 的三个 getter 同名，让「谁汇总成谁」一眼可见；四个字段都是中立词，不含任何数据源名与列名。

**`ConnectorProcedureOps` 新增一个默认抛异常的方法**（与 `planRewrite`（`:87-97`）的既有写法完全对称）：

```java
/**
 * 把引擎编排出的统计渲染成该分布式过程的结果（列 schema + 行）。只对 getExecutionMode 返回
 * DISTRIBUTED 的过程有意义；默认失败到底，避免误路由静默返回空结果集。
 *
 * 实现约束：必须是纯本地渲染 —— 不得加载表、不得发起远程调用、不得要求鉴权作用域。
 * 引擎在「零分组早退」路径上也会调它（此时还没有开事务）。
 */
default ConnectorProcedureResult buildRewriteResult(String procedureName,
        ConnectorRewriteStatistics statistics) {
    throw new UnsupportedOperationException(
            "buildRewriteResult is only implemented for DISTRIBUTED procedures; '"
                    + procedureName + "' is not one");
}
```

**iceberg 一侧**：把四列定义放进 `IcebergRewriteDataFilesAction`（它就是这个过程的连接器落点），并让它同时覆写 `getResultSchema()` 返回同一个常量，与八个单次调用兄弟保持一致（今天不可达，但让这个动作自描述）。`IcebergProcedureOps.buildRewriteResult` 只做过程名校验 + 委派，不进 `runInAuthScope` / `planInAuthScope`。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe-connector-api/.../api/procedure/ConnectorRewriteStatistics.java` | **新增**。四个 final 字段 + 四个 getter + `toString`。不做 null 检查（全是基本类型） |
| `fe-connector-api/.../api/procedure/ConnectorProcedureOps.java` | 在 `planRewrite`（`:87-97`）之后新增默认方法 `buildRewriteResult`，默认抛 `UnsupportedOperationException`；文档写清「纯本地渲染、不得远程调用」这条约束 |
| `fe-connector-api/.../api/procedure/ConnectorRewriteGroup.java` | **仅文档**。把 `:47` / `:49` / `:51` 与 `:67` / `:72` / `:77` 里的 iceberg 列名改述为中立说法：这是「按文件路径原子替换的合并模型」，字段分别是本组被替换的数据文件数、字节总量、附带的删除文件数；`:31` 那句列举 iceberg 判据的话改成「由连接器自行定义选文件与分组判据」 |
| `fe-connector-api/.../api/handle/RewriteCapableTransaction.java` | **仅文档**。`:48` 的 `added_data_files_count` 改述为「新增数据文件数这项统计，是引擎无法从规划分组算出、只能由连接器在提交后回报的那一项」 |
| `fe-core/.../execute/ConnectorRewriteDriver.java` | 删掉 `buildResult`（`:245-264`）整个方法；`:122-125` 的早退改为 `procedureOps.buildRewriteResult(procedureName, new ConnectorRewriteStatistics(0, 0L, 0, 0))`；`:182-183` 改为把 `:177-180` 求出的四个数字装进 `ConnectorRewriteStatistics` 后调 `buildRewriteResult` 并原样返回；删掉随之失效的 3 个 import（`ConnectorColumn`:22、`ConnectorType`:25、`ImmutableList`:41 —— 已核实这三个只在 `buildResult` 里用到）；类注释 `:64` 那句 “emit the four-column result row” 改成「让连接器渲染结果行」 |
| `fe-connector-iceberg/.../action/IcebergRewriteDataFilesAction.java` | 新增 `private static final List<ConnectorColumn> RESULT_SCHEMA`，**四个列名、四个列类型、四段列注释文本从引擎逐字搬过来**（含 `rewritten_bytes_count` = `INT`、`removed_delete_files_count` = `BIGINT` 两处刻意保留的历史 quirk，注释里写明这是历史行为、故意不改）；覆写 `getResultSchema()` 返回它；新增 `public static ConnectorProcedureResult buildResult(ConnectorRewriteStatistics stats)`，按「数据文件数、新增数据文件数、字节总量、删除文件数」的**原顺序**填行 |
| `fe-connector-iceberg/.../IcebergProcedureOps.java` | 覆写 `buildRewriteResult`：过程名不是 `rewrite_data_files` 时抛 `DorisConnectorException`（照 `planRewrite`:139-142 的写法），否则返回 `IcebergRewriteDataFilesAction.buildResult(statistics)` |
| `fe-connector-api/src/test/.../ConnectorProcedureOpsDefaultsTest.java` | 照 `planRewriteDefaultsToUnsupported`（`:158-168`）加一条 `buildRewriteResultDefaultsToUnsupported`；再加一条断言统计对象四个 getter 各自取到正确字段 |
| `fe-connector-iceberg/src/test/.../IcebergProcedureOpsTest.java` | **本任务的主断言落点**，见第六节 |
| `fe-core/src/test/.../ConnectorRewriteDriverTest.java` | `:84-93` 的四列名/四类型/全零行断言**移走**（改由 iceberg 单测承担）；改为断言引擎的编排职责：用 `ArgumentCaptor` 抓 `buildRewriteResult` 收到的 `ConnectorRewriteStatistics`，并断言驱动器把连接器返回的 `ConnectorProcedureResult` **原样**返回。注意：mock 的 `buildRewriteResult` 默认返回 `null`，不 stub 会让 `run()` 返回 null |
| `fe-core/src/test/.../ConnectorExecuteActionTest.java` | `:230-258` 与 `:397-415` 两条分布式用例必须补 stub —— 不 stub 会在 `ConnectorExecuteAction.wrapResult`（`:214`）对 null 解引用而 NPE；`:257` 那条「全零四列行」断言改成断言引擎透传了 stub 返回的结果 |

### 5.3 明确不要顺手做的事

- **不要修 `rewritten_bytes_count` 的 `INT` 类型，也不要修 `removed_delete_files_count` 的 `BIGINT`。** 这是本任务的红线：搬家必须逐字，列名、列顺序、列类型、列注释文本一个字都不能变。修 quirk 是另一件事，要单独提、单独评审、单独跑端到端（改类型会改变客户端看到的列元数据）。
- **不要把 `ConnectorRewriteGroup` 也一起搬进连接器。** 它是引擎编排真正要读的对象（按 `getDataFilePaths()` 给每组扫描定范围，见 `ConnectorRewriteDriver.java:192-198`），必须留在中立模块。本任务只改它的文档措辞。
- **不要重命名 `ConnectorRewriteDriver` / `planRewrite` / `RewriteCapableTransaction`。** 「rewrite」在这里是合并重写这个操作模型的中立词，不是数据源名。
- **不要把求和逻辑挪进连接器。** 引擎跑了 N 组、引擎知道跑了几组，汇总是编排的一部分；连接器给的每组数字被原样直和，不做换算、不改单位。
- **不要顺手给 `getExecutionMode` 加第三种模式，也不要顺手给 paimon/hudi 加分布式过程。** 第四节里的 paimon compact 只是用来说明成本的假想例子。
- **不要为「结果列不得出现在公共模块」写 shell / 正则门禁。** 判断一个字符串字面量是不是结果列名需要理解 Java 语义，本仓库已有结论：这类门禁只适合存在性与前缀类不变量。靠单测 + 评审。
- **不要动 `ProcedureExecutionMode` 枚举本身。** 它的两个值和文档都是中立的，只是文档里拿 iceberg 举例，属另一批文档措辞任务。

## 六、怎么验证

### 6.1 连接器单测（本任务的主断言）

在 `fe-connector-iceberg` 的 `IcebergProcedureOpsTest` 里新增：

1. `buildRewriteResultDeclaresFourLegacyColumns`：调 `procOps.buildRewriteResult("rewrite_data_files", new ConnectorRewriteStatistics(3, 4096L, 1, 2))`，断言
   - 列名有序等于 `["rewritten_data_files_count", "added_data_files_count", "rewritten_bytes_count", "removed_delete_files_count"]`；
   - 列类型有序等于 `["INT", "INT", "INT", "BIGINT"]`（取 `getType().getTypeName()`，与被删掉的 `ConnectorRewriteDriverTest.java:90-91` 同一断言形状）；测试注释里必须写明第三列的 `INT` 与第四列的 `BIGINT` 是**刻意保留的历史行为**，看到就改是错的；
   - 单行等于 `["3", "2", "4096", "1"]`。**四个数字必须互不相同**：这样把「新增数」和「重写数」填反、或把字节数与删除数填反，都会让断言变红（变异验证的着眼点就在这里 —— 用全零或用重复值的断言杀不掉填错顺序）。
2. `buildRewriteResultRejectsNonDistributedProcedure`：传 `"rollback_to_snapshot"` 应抛 `DorisConnectorException`（照 `planRewriteRejectsNonRewriteProcedure`（`IcebergProcedureOpsTest.java:208-215`）写）。
3. `buildRewriteResultDoesNotTouchTheCatalog`：用现有 fixture 断言这次调用没有触发任何 `loadTable`（对应 5.1 里「纯本地渲染」那条契约；这条契约支撑引擎的零分组早退路径 —— 那时还没有事务、也没有鉴权作用域）。

在 `fe-connector-api` 的 `ConnectorProcedureOpsDefaultsTest` 里新增默认实现抛 `UnsupportedOperationException` 的断言（变异点：默认改成返回空结果 → 误路由变成静默空结果集 → 该断言变红）。

### 6.2 引擎单测（职责改成编排）

改写后的 `ConnectorRewriteDriverTest`：

- 空计划早退：仍断言 `metadata.beginTransaction` 一次都没被调（保留 `:94-98` 那条变异守卫），并断言驱动器调了 `buildRewriteResult` 且传进去的统计四项全为 0；
- 透传：stub `buildRewriteResult` 返回一个自造的单列结果，断言 `run()` 返回的就是**同一个对象**（`assertSame`），即引擎不再对结果做任何加工。

### 6.3 端到端回归

**先纠正最初那轮调研里的一条说法**：现有端到端用例并**不**断言列名。已核实 `regression-test/suites/external_table_p0/iceberg/action/test_iceberg_rewrite_data_files.groovy:160-170` 只断言结果非空并按**下标** `[0][0]` / `[0][1]` / `[0][2]` 取值；`test_iceberg_rewrite_data_files_where_conditions.groovy:87-90`、`:117-119`、`:139-145` 同样按下标断言四个值的正负与零。所以：

- **列名与列类型的逐字一致只能靠 6.1 的连接器单测保证**，端到端兜不住；
- 端到端兜的是**列顺序与取值**：把上述两个用例（外加 `test_iceberg_rewrite_data_files_parallelism.groovy`、`test_iceberg_v3_row_lineage_rewrite_data_files.groovy`）在本地集群跑一遍，下标语义不变即通过。这四个用例已覆盖三条关键路径：正常重写、`WHERE` 收窄重写、`WHERE` 不命中（零分组早退路径，`[0][0..3]` 全 0）。
- 可选加强（不强制）：在 `test_iceberg_rewrite_data_files.groovy` 里补一句把结果集列名打进日志的断言，让列名从此有端到端护栏。若加，必须与连接器单测的名字一致，不要新造措辞。

### 6.4 编译门禁

含测试源的全反应堆编译（禁用跳过测试编译的参数）：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile
```

这是最强的单一验收信号 —— 新增的 SPI 默认方法不会破坏任何连接器，但 `fe-core` 两个测试类若忘了补 stub 会在这里就暴露成编译或运行失败。跑具体单测时必须禁用 maven build cache，否则 surefire 会被静默跳过而仍然 `BUILD SUCCESS`。

## 七、风险与回退

- **主要风险：搬家时把列写错。** 列名拼错、顺序颠倒、类型写反，用户可见的结果集就变了。控制手段是 6.1 里那条「四个互不相同的数字」断言 + 端到端下标断言。
- **次要风险：漏了空计划早退路径。** 如果只改了 `:182-183` 而漏了 `:122-125`，零分组时会走进老的 `buildResult`（已删）导致编译失败 —— 这个漏项由编译器兜住，不会静默。
- **第三个风险：把 `buildRewriteResult` 实现成需要加载表。** 那会让「没有分组时不开事务、不做远程调用」这条既有性质退化，鉴权作用域也无处安放。由 6.1 第 3 条断言兜住。
- **回退**：本任务不涉及持久化格式、不涉及 thrift 有线格式、不涉及 Gson 类型标签，结果集只在 FE 内部经 `CommonResultSet` 直接返回客户端。单个提交 revert 即可完全回到现状。
- 类加载器方面无新风险：新增的统计对象是纯数据的中立类，编在 `fe-connector-api`（公共模块，编进 `fe-core`），与 `ConnectorRewriteGroup` 走同一条路径。

## 八、相关背景

- `audit-report.md` 第八主题 8.2 节（「名字中立，但语义只对一个数据源成立」）里关于 `ProcedureExecutionMode.DISTRIBUTED` 的那一条，是本任务的出处。
- `audit-report.md` 附录 A 第 27 条给出了收窄后的准确表述：问题是**结果 schema 的所有权**错放在 `fe-core`，与 `SINGLE_CALL` 由连接器返回 `ConnectorProcedureResult` 不对称。
- `audit-report.md` 第十五节整治路线表的第 9 批「中立化」把本任务与 19、20、21 号排在一起；同批任务都改 `fe-connector-api`，但本任务只碰 `procedure` 与 `handle` 两个包，与它们无文件重叠。
- 对称参照：`RewriteCapableTransaction` 是「窄能力接口 opt-in，而不是往共享契约上加源专有方法」的既有先例（`RewriteCapableTransaction.java:22-30` 的类注释写明了这个理由）；本任务新增的默认抛异常方法沿用 `planRewrite` 的同一范式。
