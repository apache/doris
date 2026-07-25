> ⚠ **2026-07-25 实测订正，动手前必读**：`getRangeType()` **不是**零调用方。fe-core 确实从不读它，
> 但 `fe-connector-api` 自己的 `ConnectorScanRange.populateRangeParams` 默认实现读它，并把
> `connector_scan_range_type=<NAME>` 写进 `TTableFormatFileDesc` 的 jdbc 参数——这是 **BE 可见**的字符串；
> jdbc 是唯一不覆写 `populateRangeParams` 的连接器，所以这条默认路径今天是活的。
> **本任务因此不是「删死代码」，而是一次会改变 jdbc 发给 BE 的内容的行为改动**，必须按行为改动配回归验证。

# 13. 删除分片类型枚举族（本轮最有价值的一条删除）

> **优先级**：第三优先级（删除死接口面） ｜ **风险**：中 ｜ **前置依赖**：11 号（同样改动 `fe-connector-api` 的 scan 包，先做 11 号可避免同文件反复冲突；两者之间没有逻辑依赖，单独做本任务也能编译通过）
> **影响模块**：`fe-connector-api`、`fe-connector-es`、`fe-connector-hive`、`fe-connector-hudi`、`fe-connector-iceberg`、`fe-connector-jdbc`、`fe-connector-maxcompute`、`fe-connector-paimon`、`fe-connector-trino`、`fe-core`（**仅测试源**，只删不加）
> **预计改动规模**：约 22 个文件，净删约 130～150 行，新增约 25 行（一条新单测）
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

`ConnectorScanRange.getRangeType()` 是公共接口里**唯一一个「必须实现、却没有任何出口」的抽象方法**：8 个连接器全都实现了它、4 个测试匿名类也被迫实现一遍，而它的返回值在整个仓库里没有任何生产代码读取；本任务把这个方法、它的枚举 `ConnectorScanRangeType`、以及提供方一侧同义的 `ConnectorScanPlanProvider.getScanRangeType()` 一起删掉，让 `ConnectorScanRange` 的必须实现方法从 2 个降到 1 个。

## 二、背景：现在的代码是怎么写的

**枚举本体**：`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanRangeType.java:34`，4 个值 `FILE_SCAN` / `JDBC_SCAN` / `REMOTE_OLAP_SCAN` / `CUSTOM`。类注释（:20-32）声称：

```
Identifies the type of a ConnectorScanRange, which determines how BE processes the scan range.
Each type maps to a specific Thrift scan range variant in the execution layer.
```

**分片一侧的抽象方法**：`ConnectorScanRange.java:42-43`

```java
/** Returns the scan range type, which determines BE processing. */
ConnectorScanRangeType getRangeType();
```

这是该接口仅有的两个抽象方法之一（另一个是 `getProperties()`，:112），其余十几个方法全部带默认实现。

**8 个连接器的实现，返回值完全一致**，无一例返回 `FILE_SCAN` 之外的值：

| 连接器 | 位置 | 返回值 |
|---|---|---|
| es | `EsScanRange.java:75` | `FILE_SCAN` |
| hive | `HiveScanRange.java:82` | `FILE_SCAN` |
| hudi | `HudiScanRange.java:123` | `FILE_SCAN` |
| iceberg | `IcebergScanRange.java:154` | `FILE_SCAN` |
| jdbc | `JdbcScanRange.java:48` | `FILE_SCAN` |
| maxcompute | `MaxComputeScanRange.java:65` | `FILE_SCAN` |
| paimon | `PaimonScanRange.java:119` | `FILE_SCAN` |
| trino | `TrinoScanRange.java:79` | `FILE_SCAN` |

也就是说 `JDBC_SCAN`、`REMOTE_OLAP_SCAN`、`CUSTOM` 三个值零生产者——**连 jdbc 连接器自己都返回 `FILE_SCAN`**。

**提供方一侧还有一个同义方法**：`ConnectorScanPlanProvider.java:52` 的 `getScanRangeType()`，带默认值 `FILE_SCAN`，javadoc 说「引擎用它决定生成哪种 Thrift 分片结构」。它有 3 个覆写（`HiveScanPlanProvider.java:117`、`EsScanPlanProvider.java:95`、`JdbcScanPlanProvider.java:61`），三处都是逐字返回默认值 `FILE_SCAN`；引擎侧没有任何调用点。

**唯一的运行时痕迹**在公共模块自己的默认方法里，`ConnectorScanRange.java:182-194`：

```java
default void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
    Map<String, String> props = new HashMap<>(getProperties());
    props.put("connector_scan_range_type", getRangeType().name());   // :185
    props.put("connector_file_format", getFileFormat());             // :186
    ...
    formatDesc.setJdbcParams(props);
}
```

引擎的调用点只有一处：`fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java:1796`，它先按 `getTableFormatType()` 设好 `table_format_type`，然后把 Thrift 结构的构造整体委派给分片自己的 `populateRangeParams`。

**谁真的走这个默认实现**：8 个分片类里有 7 个覆写了 `populateRangeParams`（es/hive/hudi/iceberg/maxcompute/paimon/trino），且没有一个调用 `super.populateRangeParams`；只有 `JdbcScanRange` 吃默认实现。所以 `connector_scan_range_type` 这个键在生产中只会出现在 jdbc 分片的 `jdbc_params` 里。

**这个键在 BE 侧零命中**：`be/` 全树 grep `connector_scan_range_type` 无任何结果；jdbc 的 JNI 侧读取器 `fe/be-java-extensions/jdbc-scanner/.../JdbcJniScanner.java:109-147` 是逐键 `params.getOrDefault("jdbc_url", …)` 这样按名取值的，多余的键被直接忽略。而 `jdbc_params` 这张表本身是活的（`be/src/exec/scan/file_scanner.cpp:1160`、`be/src/format_v2/jni/jdbc_reader.cpp:65` 都在消费它），所以**只能删这一个键，不能删整个 `populateRangeParams` 默认方法**。

**4 个测试匿名类被迫实现它**（这是「交税」最直观的证据）：`fe/fe-core/src/test/java/org/apache/doris/datasource/scan/PluginDrivenScanNodeExplainStatsTest.java:58`、`fe/fe-core/src/test/java/org/apache/doris/datasource/split/PluginDrivenSplitPartitionValuesTest.java:46`、`fe/fe-core/src/test/java/org/apache/doris/datasource/split/PluginDrivenSplitWeightTest.java:47`、`fe/fe-connector/fe-connector-api/src/test/java/org/apache/doris/connector/api/scan/ConnectorScanRangeWeightDefaultsTest.java:38`。三个 fe-core 测试的注释直接写着「the two required methods」「the only two getters under test」——它们要测的是分片权重、分区值、EXPLAIN 统计，跟分片类型毫无关系。

## 三、为什么这是个问题

1. **必须实现，却没有出口。** 公共接口的抽象方法是对所有实现者的强制要求，代价必须由「引擎真的会读它」来偿付。这一处没有。新增一个连接器时，作者必须为它写一个 `return FILE_SCAN;`，而这个返回值走完全程后落在一个 BE 不认识的字符串键上。
2. **文档是错的，会制造真实的排查浪费。** 枚举注释说「每个值对应一种 Thrift 分片结构」、`getScanRangeType()` 的注释说「引擎据此决定生成哪种结构」。实际决定 Thrift 形状的是两件别的事：分片自己覆写的 `populateRangeParams`（构造 iceberg/hudi/paimon/es 各自的 typed 结构），以及扫描节点级的格式类型（`PluginDrivenScanNode.getFileFormatType()`，:576-582，取自扫描节点属性表的 `file_format_type` 键）。按注释行事的人会去改枚举，然后发现改了不生效。
3. **枚举值里带数据源名。** `JDBC_SCAN` 这种命名把具体数据源写进了本应中立的公共枚举，与「公共模块保持连接器中立」的方向相反；而它连一个生产者都没有。
4. **它还在污染测试的表达。** 现有测试为了满足编译，被迫写出 `FILE_SCAN` 相关断言与注释，其中 `IcebergScanRangeTest.java:56-58`、`IcebergScanPlanProviderTest.java:174-175` 的 WHY 注释把错误的因果（「返回 JDBC_SCAN 会导致错误的 thrift 分片结构」）当作既定事实固化进了测试，反过来给后来人背书。

**用户可见后果**：没有。这不是正确性缺陷，唯一的运行时变化是 jdbc 分片的 `jdbc_params` 少一个没人读的键。

## 四、用一个最小例子说明

假设我要新增一个连接器 X，它从远端 OLAP 系统读数据，我看到枚举里正好有 `REMOTE_OLAP_SCAN`：

| 我写了什么 | 现在实际发生什么 | 应该发生什么 |
|---|---|---|
| `getRangeType()` 返回 `REMOTE_OLAP_SCAN`，期待引擎生成 OLAP 形状的 Thrift 分片 | 没有任何事发生：引擎从不读这个方法。分片形状仍由我是否覆写 `populateRangeParams` 决定；这个值最终只会（在我不覆写 `populateRangeParams` 时）以 `connector_scan_range_type=REMOTE_OLAP_SCAN` 落进 `jdbc_params`，BE 不认识这个键 | 根本不该有这个方法可写。我要控制 Thrift 形状，就覆写 `populateRangeParams`；要控制 BE 读取器，就用 `getTableFormatType()` 和扫描节点属性里的格式类型 |
| `getRangeType()` 返回 `FILE_SCAN`（照抄别人） | 同样什么都不发生 | 同上 |
| 我写测试想覆盖「分片权重默认值」这件事，必须先给匿名类补一个 `getRangeType()` | 每个测试匿名类都得写一遍这段与被测行为无关的代码 | 匿名类只需实现 `getProperties()` |

三行的差别只有一处：删掉之后，**这个选择项不存在了**，没人会再花时间去选它、也没人会再因为「我改了枚举为什么不生效」去 debug。

## 五、解决方案

### 5.1 目标状态

`ConnectorScanRange` 只剩一个抽象方法：

```java
public interface ConnectorScanRange extends Serializable {
    /** Returns additional connector-specific properties. */
    Map<String, String> getProperties();
    // …其余全部带默认实现，包括 populateRangeParams
}
```

`populateRangeParams` 的默认实现去掉分片类型键（其余不动）：

```java
default void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
    Map<String, String> props = new HashMap<>(getProperties());
    props.put("connector_file_format", getFileFormat());
    // partition.* 键照旧
    formatDesc.setJdbcParams(props);
}
```

`ConnectorScanPlanProvider` 不再有 `getScanRangeType()`；`ConnectorScanRangeType.java` 整个文件删除。

类注释同步据实改写：`ConnectorScanRange` 的类 javadoc（:33-35）现在说「range type 决定引擎如何转换成 Thrift 结构」，改成「连接器通过覆写 `populateRangeParams` 决定自己的 Thrift 形状，`getTableFormatType()` 决定 BE 侧读取器」。

### 5.2 改动清单

| 文件 | 位置 | 做什么 |
|---|---|---|
| `fe-connector-api/.../scan/ConnectorScanRangeType.java` | 整个文件 | 删除 |
| `fe-connector-api/.../scan/ConnectorScanRange.java` | :42-43 | 删除抽象方法与其注释 |
| 同上 | :33-35 | 类 javadoc 改写（去掉「range type 决定 Thrift 转换」的错误因果，改述为 `populateRangeParams` + `getTableFormatType()`） |
| 同上 | :63-65 | `getFileFormat()` 的 javadoc 引用了 `ConnectorScanRangeType#FILE_SCAN`，改成不依赖枚举的措辞（**只改这句引用，不动方法本身**） |
| 同上 | :185 | 删除 `props.put("connector_scan_range_type", …)` 一行；:186 起的其余内容保持原样 |
| `fe-connector-api/.../scan/ConnectorScanPlanProvider.java` | :43-55 | 删除 `getScanRangeType()` 及其 javadoc（javadoc 从 :43 起，方法体到 :55） |
| `fe-connector-es/.../EsScanRange.java` | :74-77 | 删除覆写 + `import` |
| `fe-connector-hive/.../HiveScanRange.java` | :81-84 | 同上 |
| `fe-connector-hudi/.../HudiScanRange.java` | :122-125 | 同上 |
| `fe-connector-iceberg/.../IcebergScanRange.java` | :153-156 | 同上 |
| `fe-connector-jdbc/.../JdbcScanRange.java` | :47-50 | 同上 |
| `fe-connector-maxcompute/.../MaxComputeScanRange.java` | :64-67 | 同上 |
| `fe-connector-paimon/.../PaimonScanRange.java` | :118-121 | 同上 |
| `fe-connector-trino/.../TrinoScanRange.java` | :78-81 | 同上 |
| `fe-connector-hive/.../HiveScanPlanProvider.java` | :116-119 | 删除 `getScanRangeType()` 覆写 + `import` |
| `fe-connector-es/.../EsScanPlanProvider.java` | :94-97 | 同上 |
| `fe-connector-jdbc/.../JdbcScanPlanProvider.java` | :60-63 | 同上 |
| `fe-connector-api/src/test/.../ConnectorScanRangeWeightDefaultsTest.java` | :37-40 | 删除匿名类里的 `getRangeType()` 覆写 + `import` |
| `fe-core/src/test/.../scan/PluginDrivenScanNodeExplainStatsTest.java` | :57-60 | 同上（fe-core 只删不加） |
| `fe-core/src/test/.../split/PluginDrivenSplitPartitionValuesTest.java` | :45-48 | 同上 |
| `fe-core/src/test/.../split/PluginDrivenSplitWeightTest.java` | :46-49 | 同上 |
| `fe-connector-iceberg/src/test/.../IcebergScanRangeTest.java` | :56-58 | 删掉该断言与它上面两行 WHY 注释；测试方法其余断言（path/start/length/fileSize/fileFormat/tableFormatType）全部保留 |
| `fe-connector-iceberg/src/test/.../IcebergScanPlanProviderTest.java` | :170-176 | 删除整个 `getScanRangeTypeIsFileScan` 测试方法 |
| `fe-connector-es/src/test/.../EsNodeInfoAndScanRangeTest.java` | :134-138 | 删除整个 `testScanRangeType` 测试方法 |
| `fe-connector-jdbc/src/test/.../JdbcScanRangeAndPropertiesTest.java` | :76-80 | 删除整个 `testScanRangeType` 测试方法；**同一个文件里新增六（1）要求的默认 `populateRangeParams` 测试** |

### 5.3 明确不要顺手做的事

- **不要把 `getProperties()` 降成带默认实现（返回空表）。** 删掉分片类型方法后，`getProperties()` 成了唯一的抽象方法，看上去很想顺手一起默认化。实测收益很小：8 个连接器里只有 iceberg 返回空表（`IcebergScanRange.java:316-320`，它的载荷是 typed 字段），另外 7 个都有真实内容；只有 iceberg 加 4 个测试匿名类能因此少写几行。代价是明确的：`JdbcScanRange` 是唯一走默认 `populateRangeParams` 的生产实现，它的整张属性表就是 BE jdbc 读取器的入参，一旦 `getProperties()` 可以不实现，将来某个连接器忘了实现就会静默地把空表发给 BE（表现为运行期缺 `jdbc_url` 之类，而不是编译期报错）。这一项如果要做，应当作为独立决策，不要塞进本任务。
- **不要动 `getFileFormat()` 本身**，也不要删 `connector_file_format` 键。本任务只改它 javadoc 里对被删枚举的引用。这个方法的出口是否充足是另一条独立结论（见调研报告附录 C.3 第 1 条 —— 格式与读取机制混在一个字段），牵动扫描级格式类型这条已知风险线，不适合在一次删除里带过。
- **不要删 `populateRangeParams` 默认方法**，也不要删 `formatDesc.setJdbcParams(props)`。`jdbc_params` 在 BE 侧是活链路。
- **不要顺手改 `getTableFormatType()` 的默认值 `"plugin_driven"`**，那是 BE 读取器路由的活值。
- **不要去改 `plan-doc/` 下的历史文档**里对这个枚举的描述（`plan-doc/tasks/designs/` 里的两份设计稿、以及同日的另一份评审文档）。历史文档的勘误由 25 号任务统一处理。
- **不要为「不许再出现分片类型枚举」加 shell 或正则构建门禁。** 删除后类不存在，编译本身就是最强门禁。

## 六、怎么验证

1. **新增一条单测钉住唯一的运行时变化**（放 `fe-connector-jdbc` 的 `JdbcScanRangeAndPropertiesTest`，因为 jdbc 是默认 `populateRangeParams` 的唯一生产消费者，而目前**整个仓库没有任何测试覆盖这个默认实现**）：
   - 构造一个带 `querySql/jdbcUrl/jdbcUser` 的 `JdbcScanRange`，调用 `populateRangeParams(new TTableFormatFileDesc(), new TFileRangeDesc())`；
   - 断言 `formatDesc.getJdbcParams()` 仍然包含 `query_sql`、`jdbc_url`、`jdbc_user` 这几个 BE 侧真实消费的键（WHY：这张表就是 BE jdbc 读取器的入参，删键的改动绝不能碰到它）；
   - 断言这张表**不含** `connector_scan_range_type` 键（WHY：这个键 BE 与 JNI 侧都不读，本次删除的意图就是让它消失；变异验证：把那行 `props.put` 加回去 → 该断言变红）。
2. **零残留 grep**（存在性检查，可直接跑）：
   `grep -rn "ConnectorScanRangeType\|getRangeType\|getScanRangeType\|connector_scan_range_type" --include=*.java fe/` 应为 0 命中。
3. **编译门禁（最强单一信号）**：全反应堆**含测试源**编译，禁用任何跳过测试编译的参数——
   `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile`
   这一步同时覆盖了「删接口方法后所有实现类与匿名类都清理干净」和「`import` 未残留（checkstyle 扫测试源）」两件事。
4. **跑受影响模块的单测**，必须显式关掉 maven build cache（否则 surefire 会被静默跳过、`BUILD SUCCESS` 是空的）：
   `mvn -f .../fe/pom.xml -Dmaven.build.cache.enabled=false -pl fe-connector/fe-connector-api,fe-connector/fe-connector-jdbc,fe-connector/fe-connector-es,fe-connector/fe-connector-iceberg test`
   另外单独跑 fe-core 的三个受影响测试类（`PluginDrivenSplitWeightTest`、`PluginDrivenSplitPartitionValuesTest`、`PluginDrivenScanNodeExplainStatsTest`）。
5. **端到端回归**：不需要新增用例。这个改动唯一的有线格式变化是 jdbc 分片的 `jdbc_params` 少一个键，已被上面的单测钉住；如果手上正好有环境，跑一遍任意 jdbc 目录的查询回归即可（jdbc 是唯一走默认路径的连接器），其余连接器的分片路径字节不变。

## 七、风险与回退

- **风险点只有一处**：`populateRangeParams` 默认实现里的键删除。若误删同一段里的 `connector_file_format` 或 `setJdbcParams` 调用，会打断 jdbc 的活链路，表现为 jdbc 目录查询在 BE 侧拿不到连接参数而失败。六（1）的断言正是为此设置。
- **不涉及 Gson 持久化**：`ConnectorScanRangeType` 没有注册进任何 `RuntimeTypeAdapterFactory`，也不在任何元数据镜像里；`ConnectorScanRange` 虽然声明 `extends Serializable`，但仓库里没有任何地方对它做 Java 序列化（`fe-core` 的 datasource 包下无 `ObjectOutputStream` / `SerializationUtils` 命中），分片对象只在单次查询的规划期内存活。因此删方法不存在兼容性负担。
- **不涉及 Thrift 有线结构**：删除的只是一个字符串键，没有改动任何 `.thrift` 定义。
- **插件是独立打包的**：连接器与公共模块必须同批构建、同批部署。混用（老连接器包 + 新公共模块）会在类加载期报 `NoSuchMethodError` / `NoClassDefFoundError`。本仓库的连接器与 `fe-core` 一起构建发布，正常流程下不会出现混用；但如果有人手工替换单个插件 zip，需要重新打包全部连接器。
- **回退**：本任务是纯删除 + 一条新测试，`git revert` 单个提交即可完整回到原状，无数据面残留。

## 八、相关背景

- 调研报告 `plan-doc/connector-public-interface-cleanup/audit-report.md`：附录 A.3 第 49、50、51 条 —— 分片类型枚举与强制方法零消费者，其中第 50、51 条的「复核收窄」记录了严重度为中而非高的理由，以及「BE 侧零命中」的证据；附录 C.3 第 1 条 —— `getFileFormat()` 默认值把格式与读取机制混在一起，是独立结论（本任务明确不动）。
- 同目录 `README.md` 第三优先级小节说明了这一批删除的判据：「死接口的成本不是占空间，是逼着每个新连接器为不存在的出口交税」。
- 11 号任务（第一批死接口面删除）同样改动 `fe-connector-api` 的 scan 包，建议排在其后。
- 关于扫描级格式类型为什么危险（本任务刻意不碰 `getFileFormat()` 的原因）：`PluginDrivenScanNode.getFileFormatType()`（`:576-582`）在分片之前就决定了 BE 走哪一代文件读取器，发错值会把整个连接器钉在旧读取器上。
