# 11. 删除第一批死接口面（公共模块内部，不动连接器生产代码）

> ## ✅ 已落地（2026-07-25），分五个提交
>
> | 提交 | 内容 |
> |---|---|
> | `delete six pieces of dead connector surface` | 两个空句柄接口、MVCC 快照两字段、两个统计 `UNKNOWN`、`ConnectorTestResult` 子组件、`ConnectorType` 四个整表取得器 |
> | `remove the LIMIT pushdown entry point` | `applyLimit` + `LimitApplicationResult` + `tryPushDownLimit` + **冻结基线重新生成** |
> | `drop the create-table request's external flag` | `isExternal`（用户拍板：删） |
> | `drop the partition-value list nothing carries` | `ConnectorPartitionValueDef` + `initialValues` + 11 处连接器测试源实参 |
> | `delete the unwired property-descriptor mechanism` | `ConnectorPropertyMetadata` + 两个 `Connector` 取得器 + 包级说明新增规则七（用户拍板：删，等同 24 号选项二） |
>
> **动手前 22 个 agent 的复核推翻/订正了本文以下说法，正文未逐条重写，以这里为准：**
>
> 1. **本文完全没提冻结基线**（它比本文的基线提交新）。`connector-metadata-methods.txt` 第 6 行就是 `applyLimit`，必须同一提交重新生成；该文件**没有 ASF 头**，重新生成时别加。已双向变异验证。
> 2. **第 5.2 节第 4 项漏了引擎侧第四处引用**：`PluginDrivenScanNode.pinMvccSnapshot()` 的 javadoc 里有 `{@link #tryPushDownLimit}`。fe-core 的 javadoc 插件是 `<skip>true</skip>`，**编译抓不到**。
> 3. **第 6 节第 1 项「全反应堆 test-compile 是唯一能一次证明引用全清的动作」不成立**——它对 javadoc 引用是结构性失明的。必须配人工 grep，且 grep 清单要包含 `tryPushDownLimit` 这类只在注释里出现的名字。
> 4. **第四节的最小例子举错了连接器**：hive **没有**关掉带类型转换的谓词下推（继承默认 `true`）；关掉的是 paimon 与 maxcompute，jdbc 按会话。风险是「实现 `applyLimit`」+「关掉 cast 下推」的双重条件，不是单条件。该隐患早已登记为 `plan-doc/deviations-log.md` 的 DV-020。
> 5. **第三节第 2 条关于两个 `UNKNOWN` 的危害论证不成立**：`Optional.of(UNKNOWN)` 在 fe-core 三个消费点与 `Optional.empty()` 行为完全相同。删除理由改为「类文档与方法签名互相矛盾」。第八节「把未知收成一种表达」也不成立——同模块还有第三个**活的** `ConnectorPartitionInfo.UNKNOWN`（-1L），hive 主源与 fe-core 都在用。
> 6. **第 6.3 节的变异验证配方无效**：`HiveConnectorMetadataDdlTest` 直接构造 spec，根本不经过 fe-core 转换器；且该测试类在本分支上本来就红（19 用例 / 5 failures + 7 errors，改动前后逐数字一致）。
> 7. **第 6.3 节「必须仍然断言 `hasExplicitPartitionValues()`」无法执行**：fe-core 主源与测试对该方法**零命中**。实际做法是**新增**一条断言（喂非空分区定义列表、要求置位），并做变异验证：把转换器那个布尔位写死 `false` 时只有这条变红。
> 8. **`isExternal` 的删除理由比本文强得多**：它在任何能到达连接器的路径上都是编译期常量 `true`（`CreateTableInfo.checkEngineName` 强制置真），且 `EXTERNAL_TABLE`/`MANAGED_TABLE` 这个决策在 Doris 里不存在（`HmsWriteConverter` 硬编码 `MANAGED_TABLE`，与迁移前逐字相同）。**选项 B 若按字面做会改变行为**（DROP TABLE 不再删数据）。
> 9. **`isExternal` 的测试改动被低估**：夹具 `stubInfo` 有 **9 个调用点**传那个尾参，只改本文列的 3 行会编译失败。
> 10. **`ConnectorTestResult` 还有一个消费者**：引擎把整个结果对象丢进 `LOG.info`，所以 `toString()` 是活的（输出不变，因为那个 map 恒空）。删字段会孤立 `Collections`/`Map` 两个 import，checkstyle `UnusedImports` 会报错。
> 11. **`ConnectorMvccSnapshot` 的测试有个方法叫 `equalsAndHashCodeCoverAllSixFields`**、javadoc 写着「6 个字段」，删两字段后必须改名改文案。另外「20 多个生产构造点」实为 15 个（测试侧 37 个）。
> 12. **第 5.2 节第 1 项的 import 提示是错的**：`java.util.List` 与 `java.util.Collections` 在 `Connector.java` 里都仍被使用，import 净变化为零。
> 13. **名字撞车清单（5.3 第二类）要补两条**：`ConnectorCapability.java` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个活方法，且是**安全相关**的（哪些连接器不能声明 SHOW CREATE TABLE，否则泄露连接密码）；以及上面第 5 条的 `ConnectorPartitionInfo.UNKNOWN`。
> 14. **第 6.1 节的构建命令要排除两个 shade 模块**，否则失败原因与本批无关（见交接文档的构建坑）。
>
> **顺带发现、留给下一批**：`fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` 全仓库零引用，是一个可删的死类。

> **优先级**：第三优先级（删死面） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-api`（主源 + 自带测试）、`fe-core`（引擎主源 + 测试）；另有三个连接器的**测试源**各去掉一个恒为空的构造参数（`fe-connector-hive`、`fe-connector-iceberg`、`fe-connector-paimon`，生产代码零改动）
> **预计改动规模**：约 20 个文件，净减 400～550 行（其中 5 个整类删除）
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

连接器公共接口上有一批方法、字段和整个类，既没有任何连接器实现，也没有任何引擎代码消费；这一批当中有 10 项可以在**完全不碰连接器生产代码**的前提下删掉，本任务把它们删干净，让公共接口第一次真正变小而不是变大。

## 二、背景：现在的代码是怎么写的

调研报告列了约 20 项「可以直接删」的死面。逐条在 `7ff51a106f0` 上重扫之后，**这 20 项里只有 10 项真的不碰连接器生产代码**，另外 8 项各有 1 到 8 个连接器在实现或构造它们（详见 5.3）。本任务只做前 10 项。下面把这 10 项的现状讲清楚。

**1）连接器属性描述符机制（`ConnectorPropertyMetadata`）**
`fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorPropertyMetadata.java` 是一个 120 行的泛型类，提供 `stringProperty` / `intProperty` / `booleanProperty` 等工厂，用来描述「连接器暴露哪些配置项」。`Connector.java:234-242` 上挂着两个默认方法把它返回出来：

```java
    /** Returns the table-level property descriptors. */
    default List<ConnectorPropertyMetadata<?>> getTableProperties() { ... }
    /** Returns the session-level property descriptors. */
    default List<ConnectorPropertyMetadata<?>> getSessionProperties() { ... }
```

全仓库对 `ConnectorPropertyMetadata` 只有 15 处命中，全部在这个类自身和上面两个默认方法里。八个连接器一个都没有覆写这两个方法。
**特别注意同名不同义**：真正给 `SHOW CREATE TABLE` 渲染 `PROPERTIES (...)` 的是 `fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalTable.java:768` 的 `getTableProperties()`，它返回 `Map<String, String>`，由 `Env.java:4881` 消费，**是活的，不许动**。同样，`ConnectorSession.java:89` 的 `getSessionProperties()` 返回 `Map<String, String>`，被 hive / iceberg / paimon / jdbc / hudi / es / maxcompute 大量读取，也**是活的，不许动**。要删的只是 `Connector` 这个接口上返回描述符列表的两个方法。

**2）两个空句柄接口**
`handle/ConnectorPartitionHandle.java:25` 是一个 `extends Serializable` 的空接口，全仓库只有它自己这一处命中。
`handle/ConnectorTransactionHandle.java:23` 也是空接口，唯一引用来自同目录的 `ConnectorTransaction.java:35`（`extends ConnectorTransactionHandle`），而它的存在理由写在 `ConnectorTransaction.java:32` 的注释里：「Extends the marker ConnectorTransactionHandle so that existing APIs that traffic in opaque handles continue to work without change」。核实结果：全仓库没有任何方法以 `ConnectorTransactionHandle` 作参数或返回值，这句注释在代码里为假。

**3）`applyLimit` 与 `LimitApplicationResult`**
`ConnectorPushdownOps.java:53-59` 声明了默认返回 `Optional.empty()` 的 `applyLimit`，八个连接器零覆写。`pushdown/LimitApplicationResult.java` 是配套的 70 行结果类，零构造点。
但它不是「零调用」——引擎真的在调：`fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java:911-921` 的 `tryPushDownLimit()` 调用 `metadata.applyLimit(...)`，并在 `getSplits()` 里的 `1261` 行执行。这一点很关键，见第三节。

**4）`ConnectorMvccSnapshot` 的描述与时间戳字段**
`mvcc/ConnectorMvccSnapshot.java:37-38` 有 `timestampMillis` 与 `description` 两个字段，配 builder setter（`151-162`）、getter（`59-66`）以及 `equals`/`hashCode`/`toString` 中的项。全仓库对这两个 setter/getter 的调用只出现在公共模块自带的 `ConnectorMvccSnapshotTest.java`。生产侧的 `ConnectorMvccSnapshot.builder()` 调用点有 20 多个（hive、iceberg、paimon、hudi 和 fe-core），全部只用 `snapshotId` / `schemaId` / `lastModifiedFreshness` / `properties`，没有一处调 `.description(...)` 或 `.timestampMillis(...)`。这个类没有 Gson 注解，也不过 thrift。

**5）两个统计类的 `UNKNOWN` 常量**
`ConnectorTableStatistics.java:29` 与 `ConnectorColumnStatistics.java:36` 各有一个 `public static final ... UNKNOWN` 哨兵（分别是 `(-1, -1)` 和 `(-1, -1, -1, -1)`），类文档写着「statistics 不可用时用 UNKNOWN」。全仓库对这两个常量零引用。而 `ConnectorStatisticsOps.java:33/52/67` 的真实签名是 `Optional<ConnectorTableStatistics>` / `Optional<ConnectorColumnStatistics>`——「不可用」的约定其实是 `Optional.empty()`。

**6）`ConnectorPartitionValueDef` 与 `ConnectorPartitionSpec.getInitialValues`**
`ddl/ConnectorPartitionValueDef.java` 是 77 行的分区值定义类，唯一引用者是 `ddl/ConnectorPartitionSpec.java`（字段 `:48`、两个构造参数 `:52`/`:59`、getter `:79`）。而 `ConnectorPartitionSpec.java:86-88` 自己的注释就写明了它恒为空：

> The neutral converter does not lower those value expressions into `getInitialValues()` (it stays empty), so this flag preserves the information a connector needs to reject them

真正被消费的是布尔位 `hasExplicitPartitionValues()`（hive 用它拒绝显式分区值）。唯一生产构造点 `fe-core/src/main/java/org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverter.java:149` 传的就是 `Collections.emptyList()`。

**7）`ConnectorCreateTableRequest.isExternal`**
`ddl/ConnectorCreateTableRequest.java:115-117` 的 `isExternal()`，由 `CreateTableInfoToConnectorRequestConverter.java:75` 的 `.external(info.isExternal())` 填入。核实结果：有生产者，零消费者——八个连接器没有一个读过它，类文档也没说清「external」在连接器语境下意味着什么。（调研报告写的「零消费者、零文档」需要修正为「有生产者、零消费者、零语义文档」。）

**8）`ConnectorTestResult` 的子组件机制**
`ConnectorTestResult.java:36`（`componentResults` 字段）、`:62-70`（`withComponents` 工厂）、`:80-83`（`getComponentResults`）以及 `:89-96`（`toString` 里的拼接）。全仓库零调用。另外 `:100-110` 的 `equals` 只比 `success` 与 `message`，**故意或无意地忽略了 `componentResults`**——留着它就是留一个「两个不等的对象相等」的坑。引擎侧只消费 `isSuccess()` 与 `getMessage()`。

**9）`ConnectorType` 的四个整表子列表取得器**
`ConnectorType.java:249-268` 的 `getChildrenNullable` / `getChildrenComments` / `getChildrenFieldIds` / `getChildrenCommentSpecified`，全仓库（含全部测试）零调用；实际使用的都是同类里的按索引访问器。

## 三、为什么这是个问题

三条真实伤害，逐条对应上面的现状：

1. **公共接口在向每个新连接器收税，而收来的东西没人用。** 一个新连接器作者读 `Connector` 接口会看到两个属性描述符方法、读 `ConnectorPartitionSpec` 会看到一个分区值列表、读 `ConnectorCreateTableRequest` 会看到 `isExternal()`——他要么白花时间实现，要么白花时间确认「不实现行不行」。这批面越留越长，每一次「新增连接器要读多少接口」的评估都被它抬高。

2. **文档说的和代码做的不一致，会制造排查浪费。** `ConnectorTransaction` 说自己是为了兼容「以不透明句柄传递事务的既有接口」，但那种接口不存在；两个统计类说「不可用时用 `UNKNOWN`」，但签名要求的是 `Optional.empty()`——照文档写的连接器会返回 `Optional.of(UNKNOWN)`，让「没有统计」变成「行数 -1」，引擎侧对 -1 的处理和对 `empty` 的处理不是一回事。这类文档不是无害的装饰，它会把人引到错的实现上。

3. **`applyLimit` 留着比删掉危险，这是本批唯一有正确性含义的一项。** 引擎在 `PluginDrivenScanNode.getSplits()` 里的调用顺序是：
   - `1261`：`tryPushDownLimit()` → 调 `metadata.applyLimit(...)`；
   - `1318`：`buildRemainingFilter()` → 若连接器不支持带隐式类型转换的谓词下推，这里会把含类型转换的谓词**剥掉**，并把 `filteredToOriginalIndex` 置为非 null；
   - `1341`：`long sourceLimit = effectiveSourceLimit(limit, filteredToOriginalIndex != null);` → 一旦剥过谓词，就把传给 `planScan` 的 LIMIT 抑制掉。第 `1326-1333` 行的注释把理由写得很清楚：连接器看到的过滤条件已经不反映被剥掉的谓词，此时若在数据源侧应用 LIMIT，取回的行会被后续在 BE 上重新求值的谓词再砍一刀，**结果少返回行**。

   问题在于这条安全抑制只作用于 `sourceLimit`，而 `applyLimit` 在它之前 80 行就已经调过了。也就是说：今天没人实现 `applyLimit`，所以没事；哪天有连接器实现了它（接口摆在那儿、名字又直白，这是完全可能的），它就会在「谓词已被剥掉」的情况下拿到完整 LIMIT 并把它下推下去，用户看到的是**查询少返回行**，且只在带隐式类型转换的谓词 + LIMIT 的组合下出现——极难定位。删掉这个方法与它的结果类，等于把这个陷阱拆掉；真要做 LIMIT 下推，也应该在正确的位置（谓词剥离之后）重新设计入口。

**不建议「先加过时标注、下个版本再删」。** 这些都是内部接口，仓库外没有实现者；打上过时标注只会让公共接口再长一岁而不缩小。分批删 + 每批全反应堆含测试源编译，已经足够安全。

## 四、用一个最小例子说明

用 `applyLimit` 这一项举例，因为它是本批唯一「留着有正确性风险」的。假设 hive 表 `t` 有 100 万行，`a` 是字符串列，用户写：

```sql
SELECT * FROM hive_catalog.db.t WHERE a = 1 LIMIT 10;
```

`a = 1` 会被分析成「把字符串列隐式转成数字再比较」，也就是含隐式类型转换的谓词。

| 用户写了什么 | 今天实际发生什么 | 如果哪天有人实现了 `applyLimit` |
|---|---|---|
| `WHERE a = 1 LIMIT 10` | 引擎发现 hive 不支持带类型转换的谓词下推，把这个谓词剥掉；因为剥过，`sourceLimit` 被抑制成「不下推 LIMIT」；数据源扫全表，BE 端重新算 `a = 1` 并取前 10 行 → **结果正确** | `applyLimit` 在谓词剥离之前就被调用，把 `LIMIT 10` 交给了数据源；数据源在没有 `a = 1` 的情况下取 10 行还给 BE；BE 再用 `a = 1` 过滤这 10 行 → **可能只返回 1 行甚至 0 行，用户看到结果少了** |

同一段 SQL，接口面留着与删掉的区别不在性能而在正确性。删掉之后，任何人想做 LIMIT 下推都必须重新加入口，而那时他会看到 `effectiveSourceLimit` 这条抑制并绕不过去。

## 五、解决方案

### 5.1 目标状态

改完之后：

- `fe-connector-api` 少掉 5 个类文件：`ConnectorPropertyMetadata`、`handle/ConnectorPartitionHandle`、`handle/ConnectorTransactionHandle`、`pushdown/LimitApplicationResult`、`ddl/ConnectorPartitionValueDef`（其中 `ConnectorPartitionValueDef` 与 `LimitApplicationResult` 是配套删）。
- `Connector` 接口不再有属性描述符方法；`ConnectorPushdownOps` 只剩 `applyFilter` / `applyProjection` / `supportsCastPredicatePushdown`。
- `ConnectorTransaction` 的声明变成：

```java
public interface ConnectorTransaction extends Closeable {
```

（连带删掉 `ConnectorTransaction.java:32-33` 那段说明「为兼容不透明句柄接口」的注释。）

- `ConnectorPartitionSpec` 的两个构造收成一个，签名草案：

```java
public ConnectorPartitionSpec(Style style, List<ConnectorPartitionField> fields);
public ConnectorPartitionSpec(Style style, List<ConnectorPartitionField> fields,
        boolean hasExplicitPartitionValues);
```

- `ConnectorTestResult` 只剩 `success()` / `success(String)` / `failure(String)` / `isSuccess()` / `getMessage()`，`equals` 与实际字段重新一致。
- `ConnectorMvccSnapshot` 只剩 `snapshotId` / `schemaId` / `lastModifiedFreshness` / `properties`。
- 两个统计类不再有 `UNKNOWN`，类文档改成指向 `Optional.empty()` 这一个约定。
- `fe-core` 的 `PluginDrivenScanNode` 不再有 `tryPushDownLimit()`；`getSplits()` 里 `1261` 那行连同上面「Attempt limit and projection pushdown via SPI protocol」的注释一起去掉（投影下推的调用在别处，不受影响）。

### 5.2 改动清单

`api` 指 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/`。

| 序号 | 文件 | 做什么 |
|---|---|---|
| 1 | `api/ConnectorPropertyMetadata.java` | 整文件删除 |
| 1 | `api/Connector.java`（234-242） | 删两个默认方法 `getTableProperties()` / `getSessionProperties()`，清理随之不用的 import（`List` 视其它用法保留） |
| 1 | `fe-core/src/test/java/org/apache/doris/connector/fake/FakeConnectorPluginTest.java`（177-178） | 删两行断言；该测试方法其余断言保留 |
| 2 | `api/handle/ConnectorPartitionHandle.java` | 整文件删除 |
| 3 | `api/handle/ConnectorTransactionHandle.java` | 整文件删除 |
| 3 | `api/handle/ConnectorTransaction.java`（32-35） | 去掉 `extends ConnectorTransactionHandle` 与对应注释段 |
| 4 | `api/ConnectorPushdownOps.java`（53-59 及 `LimitApplicationResult` import） | 删 `applyLimit` 默认方法 |
| 4 | `api/pushdown/LimitApplicationResult.java` | 整文件删除 |
| 4 | `fe-core/.../datasource/scan/PluginDrivenScanNode.java`（44、903-921、1260-1261） | 删 import、删整个 `tryPushDownLimit()` 方法及其 javadoc、删 `getSplits()` 里的调用行与其上方注释 |
| 5 | `api/mvcc/ConnectorMvccSnapshot.java` | 删 `timestampMillis` / `description` 字段、构造赋值、getter、builder setter，以及 `equals`/`hashCode`/`toString` 中对应项；类 javadoc 相应收窄 |
| 5 | `api/src/test/.../mvcc/ConnectorMvccSnapshotTest.java` | 删对这两个字段的构造与断言；**保留**对 `snapshotId`/`schemaId`/`lastModifiedFreshness`/`properties` 的 equals/hashCode 覆盖 |
| 6 | `api/ConnectorTableStatistics.java`（23、29-30） | 删 `UNKNOWN` 常量，类 javadoc 改为「不可用时返回 `Optional.empty()`」 |
| 6 | `api/ConnectorColumnStatistics.java`（30、36-37） | 同上 |
| 7 | `api/ddl/ConnectorPartitionValueDef.java` | 整文件删除 |
| 7 | `api/ddl/ConnectorPartitionSpec.java`（30-35 javadoc、48、52-69、79、equals/hashCode/toString） | 删 `initialValues` 字段与构造参数、getter；把 `86-88` 那段解释「转换器不下降值表达式」的注释改成不再引用已删除的 getter |
| 7 | `fe-core/.../connector/ddl/CreateTableInfoToConnectorRequestConverter.java`（149） | 去掉 `Collections.emptyList()` 实参 |
| 7 | `fe-core/src/test/.../connector/ddl/CreateTableInfoToConnectorRequestConverterTest.java`（243、291、308） | 删三处 `getInitialValues().isEmpty()` 断言；**保留**同处对 `hasExplicitPartitionValues()` 的断言 |
| 7 | 连接器**测试源**去实参：`fe-connector-iceberg` 的 `IcebergSchemaBuilderTest.java`（215、239）、`IcebergConnectorMetadataDdlTest.java`（232 附近）；`fe-connector-hive` 的 `HiveConnectorMetadataDdlTest.java`（187、203、223）；`fe-connector-paimon` 的 `PaimonSchemaBuilderTest.java`（106、145、164）、`PaimonConnectorMetadataDdlTest.java`（59、81 附近） | 每处删掉一个 `Collections.emptyList()` 实参，其余不动。这些连接器的**生产代码零改动** |
| 8 | `api/ddl/ConnectorCreateTableRequest.java`（32 javadoc、51、69、115-117、129、143、193-196） | 删 `external` 字段、构造赋值、getter、`toString` 项、builder 字段与 setter（**见下方需拍板项**） |
| 8 | `fe-core/.../connector/ddl/CreateTableInfoToConnectorRequestConverter.java`（75） | 删 `.external(info.isExternal())` |
| 8 | `fe-core/src/test/.../CreateTableInfoToConnectorRequestConverterTest.java`（86、379、388） | 删 `isExternal()` 断言与测试夹具里的 `external` 形参/打桩 |
| 9 | `api/ConnectorTestResult.java`（28-30、36、38-45、62-70、80-83、89-96） | 删子组件字段、`withComponents`、`getComponentResults`、`toString` 里的拼接段；三个工厂改为不再传 `null`；`equals` 与剩余字段自然一致 |
| 10 | `api/ConnectorType.java`（249-268） | 删四个整表子列表取得器；**保留**同类的按索引访问器与四个底层字段（它们由按索引访问器使用） |

**需要用户拍板的一项（第 8 项 `isExternal`）**
这是判断题不是事实题：元存储确实区分「托管表」与「外部表」，未来某个连接器可能真需要知道建的是哪种。三个选项，请选一个：

- **选项 A（建议）**：按上表删掉。理由是「一个零消费者、零语义说明的布尔位挂在公共建表请求上」本身就在误导——连接器作者会以为自己该读它，读了又不知道该怎么用。真需要时按当时的语义重新加，成本只有几行。
- **选项 B**：保留，但**必须同时**补两件事：一是在类文档里写明「external 在连接器语境下的确切含义」（是 `CREATE EXTERNAL TABLE` 语法位，还是元存储的表类型？），二是让至少一个连接器真正读它并据此改变行为（例如 hive 建表时决定写 `EXTERNAL_TABLE` 还是 `MANAGED_TABLE`）。
- **选项 C（不允许）**：维持现状——零消费 + 零文档地留着。

调研报告里还有一项同性质的判断题（`ConnectorTableOps.getPrimaryKeys` 与 `ConnectorTableSchema.PRIMARY_KEYS_KEY`），但它要改 paimon 的生产代码，不在本批范围，留给需要连带改连接器的那一批一并拍板。

### 5.3 明确不要顺手做的事

**第一类：调研报告列在同一张表里、但核实后发现会碰连接器生产代码的 8 项——本批一律不做，留给下一批。** 之所以要写出来，是为了避免动手的人以为漏了：

| 项 | 为什么不在本批 |
|---|---|
| `ConnectorEventSource.getCurrentEventId` | `fe-connector-hms` 的 `HmsEventSource.java:58` 有真实实现（且 `pollForMaster` 内部又自己读了一次同样的 id）；删接口方法必须同时删这个覆写 |
| `ConnectorScanPlanProvider.estimateScanRangeCount` | `fe-connector-jdbc` 的 `JdbcScanPlanProvider.java:152` 有一个恒返回 1 的实现 |
| 两个 `ApplicationResult` 上的 `precalculateStatistics` | `LimitApplicationResult` 随本批整类删除，但 `FilterApplicationResult` 的这个必填参数有三个生产构造点：hive `HiveConnectorMetadata.java:1115`、hudi `HudiConnectorMetadata.java:312`、trino `TrinoConnectorDorisMetadata.java:296`（都传 `false`） |
| `ProjectionApplicationResult` 的投影列与赋值 + `ConnectorColumnAssignment` 整类 | `fe-connector-trino` 的 `TrinoConnectorDorisMetadata.java:359/369/371` 真的在构造它们 |
| `ConnectorViewDefinition.dialect` | 两个生产者：hive `HiveConnectorMetadata.java:693`（编造的占位符）与 iceberg `IcebergConnectorMetadata.java:350`（视图表示里的真方言） |
| `ConnectorProcedureOps.execute` 的 WHERE 参数 | 引擎侧确实恒传 `null`（`ConnectorExecuteAction.java:176-177`，带 WHERE 的分布式重写走 `planRewrite`），但删参数要改 `fe-connector-iceberg` 的 `IcebergProcedureOps` 实现签名 |
| `MetastoreChangeDescriptor.forTable` 的「改名后表名」参数 | 4 个生产调用点全在 `fe-connector-hms` 的 `HmsEventParser.java`（103、108、125、192），都传 `null`；真改名走 `forTableRename` |
| `ConnectorTableSchema.tableFormatType` | 构造参数，八个连接器都在传值；删它是最大的一次机械改动，必须单独一批 |

**第二类：名字撞车、绝对不能顺手删的活代码。**

- `ConnectorScanRange.getTableFormatType()`（`api/scan/ConnectorScanRange.java:121`）与 `ConnectorTableSchema.getTableFormatType()`（`ConnectorTableSchema.java:150`）**同名不同义**。前者是活的有线协议字段，被 `PluginDrivenScanNode.java:1793` 写进 thrift 的 `tableFormatFileDesc`。本批不碰 `tableFormatType`，但仍在这里点名，以防后续批次误删。
- `PluginDrivenExternalTable.getTableProperties()`（返回 `Map<String,String>`）与 `ConnectorSession.getSessionProperties()`（返回 `Map<String,String>`）都是活的，与本批要删的 `Connector` 上两个同名方法毫无关系。
- `fe-connector-trino` 与 `be-java-extensions/trino-connector-scanner` 里出现的 `ConnectorTransactionHandle` / `LimitApplicationResult` / `ProjectionApplicationResult` 都是 `io.trino.spi.connector.*`，是 Trino 自己 SPI 的同名类，一行都不许动。

**第三类：范围纪律。**
- 不要顺手把 `ConnectorTestResult` 的 `equals` 忽略字段这类问题「顺便修好」再保留字段——本批的处置就是删字段，删完 `equals` 自然一致。
- 不要为「让删除后能编译」往 `fe-core` 新增任何数据源相关代码；本批只删不加。
- 不要写 shell 或正则的构建门禁去防止这些符号复活。它们已经不存在了，编译就是最强的约束；这类门禁只适合存在性与前缀类不变量。

## 六、怎么验证

**1）编译（最强的单一信号）。** 全反应堆、**含测试源**：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T 1C test-compile
```

禁止任何跳过测试编译的参数。本批的核心风险就是「某处还引用着已删符号」，而这条命令覆盖 `fe-core`、`fe-connector-api` 和八个连接器的主源与测试源，是唯一能一次证明「引用全清」的动作。`BUILD SUCCESS` 之外任何 symbol not found 都必须当场处理，不许注释掉测试绕过。

**2）单元测试（必须关掉构建缓存，否则测试会被静默跳过而仍报 BUILD SUCCESS）。** 至少跑这四组：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -Dmaven.build.cache.enabled=false \
    -pl fe-connector/fe-connector-api,fe-core test \
    -Dtest=ConnectorMvccSnapshotTest,FakeConnectorPluginTest,CreateTableInfoToConnectorRequestConverterTest
```

外加三个连接器的分区相关测试（改过测试源实参的那些）：`HiveConnectorMetadataDdlTest`、`IcebergSchemaBuilderTest`、`PaimonSchemaBuilderTest`、`PaimonConnectorMetadataDdlTest`、`IcebergConnectorMetadataDdlTest`。

**3）测试要断言的是什么（不是「还能跑」）。**
- `ConnectorMvccSnapshotTest` 删掉描述与时间戳的断言后，**必须仍然覆盖** `equals`/`hashCode` 对 `snapshotId`、`schemaId`、`lastModifiedFreshness`、`properties` 的敏感性——这四个是活的，快照身份靠它们区分。如果删完之后这个测试只剩「构造不抛异常」，那就是把测试的意图删掉了，要补回差异断言。
- `CreateTableInfoToConnectorRequestConverterTest` 删掉 `getInitialValues()` 断言后，**必须仍然断言 `hasExplicitPartitionValues()`**。做一次变异验证：手工把 `CreateTableInfoToConnectorRequestConverter` 里传给 `ConnectorPartitionSpec` 的这个布尔位改成恒 `false`，`HiveConnectorMetadataDdlTest` 里「hive 拒绝显式分区值」那条用例必须变红。变红说明我们删掉的是死的那半（值列表），保住的是活的那半（布尔位）；不变红说明保护不足，要补断言。

**4）删除彻底性自查（人工一次，不做成门禁）。** 对下列符号在全仓库 grep，期望零命中（`io.trino.spi.connector.*` 的同名类除外）：`ConnectorPropertyMetadata`、`ConnectorPartitionHandle`、`ConnectorPartitionValueDef`、`getInitialValues`、`applyLimit(`（`ShowCommand.applyLimit` 是完全无关的同名方法，需排除）、`withComponents`、`getComponentResults`、`getChildrenNullable`、`ConnectorTableStatistics.UNKNOWN`、`ConnectorColumnStatistics.UNKNOWN`。

**5）端到端回归：本批不需要。** 删掉的路径在生产上全是「默认值 / 恒空 / 恒 `Optional.empty()`」，唯一涉及引擎行为的是移除 `tryPushDownLimit()`，而它调用的接口八个连接器都没实现，返回值恒为空、恒不改 handle，删掉与保留在运行时完全等价。如果手上正好有集群，跑一遍外部表带 LIMIT 的既有回归用例作为额外确认即可（端到端用例需要本地集群，不构成本批的完成条件）。

## 七、风险与回退

- **主要风险是漏删引用导致编译失败**，而这在合并前一定会被第五条第 1 项的全反应堆含测试源编译抓住，不会漏到运行期。
- **误删活代码的风险集中在两组同名符号上**（`getTableFormatType`、`getTableProperties`/`getSessionProperties`），5.3 第二类已逐个点名；动手时按**完整类名 + 方法签名**定位，不要按方法名 grep 后批量替换。
- **`ConnectorMvccSnapshot` 的 `equals`/`hashCode` 语义会变**（少比两个字段）。因为生产侧从不设置这两个字段（恒为 `0` 与 `""`），任何两个生产对象在这两个字段上必然相等，去掉它们不改变任何一次比较的结果。它没有 Gson 持久化也不过 thrift，无兼容包袱。
- **回退成本极低**：建议按上表的 10 个序号拆成 10 个提交（或至少把「`applyLimit` + `LimitApplicationResult`」和「`isExternal`」各自独立成一个提交）。任何一项出问题单独 revert，不牵连其它。

## 八、相关背景

- 调研报告 `plan-doc/connector-public-interface-cleanup/audit-report.md`：
  - 「主题四：没有调用方或没有实现方的接口面」第 7.1 节（为什么死面比看起来严重）、第 7.2 节（本批的原始清单，注意其中 8 项经核实需要连带改连接器，已在 5.3 移出）、第 7.4 节（不做过时标注的理由与两条判断题）；
  - 「主题四」第 7.3 节：需要连带改连接器的删除，下一批的范围；
  - 附录 A.3「没有调用方或没有实现方的接口面」第 43–74 条：每一项的原始判定与复核收窄记录；
  - 附录 B.2：几条关键结论的可复核重跑方式。
- 本批与「主题七：语义与契约不清」第 10.2 节（数值的单位与「未知值」没有统一约定）有交集：删掉两个 `UNKNOWN` 常量，正是把「未知」的三种表达收成一种（`Optional.empty()`）。
