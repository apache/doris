# 21. 把扫描节点属性表的键契约集中到公共模块

> **优先级**：第五优先级（结构） ｜ **风险**：中 ｜ **前置依赖**：无（与「把通用扫描节点里的 ES 专属分支搬进连接器」那一项任务同改 `PluginDrivenScanNode`，建议先做本任务，那一项就能直接把它要新增的三个中立合成键声明进本任务建的常量类）
> **影响模块**：`fe-connector-api`、`fe-core`、`fe-connector-hive`、`fe-connector-hudi`、`fe-connector-iceberg`、`fe-connector-paimon`、`fe-connector-jdbc`、`fe-connector-es`
> **预计改动规模**：约 14～16 个文件；新增一个约 150 行的常量类，其余文件净变化在 ±80 行量级（大部分是把字面量换成常量引用）
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

连接器通过 `ConnectorScanPlanProvider.getScanNodeProperties` 返回一张 `Map<String,String>` 把扫描节点级信息交给引擎，但**这张表的键是什么、谁读、值怎么写，在公共模块里一个字都没有**——键一半藏在引擎的 `private` 常量里、一半散成各连接器里的裸字面量，新连接器只能去读引擎源码抄字符串；本任务把这份键契约集中成公共模块里一个常量类，同时顺手修掉同一片区域里两个已核实的小问题（两个返回面缺一句「只覆写一个」的说明、包装对象用构造器重载隐式编码布尔位）以及 `getSerializedTable(Map)` 这条绕私有键的弯路。

## 二、背景：现在的代码是怎么写的

### 2.1 属性表的两端

连接器一侧的入口在 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanPlanProvider.java:417`：

```java
default Map<String, String> getScanNodeProperties(
        ConnectorSession session, ConnectorTableHandle handle,
        List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
    return Collections.emptyMap();
}
```

它的 javadoc（:403-416）只举了一个 ES 的例子，**没有列出任何一个键名**。

引擎一侧的消费者是 `fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java`，键定义在 :127-130，全部是 `private`：

```java
/** Scan node property keys (shared with connector plugins). */
private static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
private static final String PROP_PATH_PARTITION_KEYS = "path_partition_keys";
private static final String PROP_LOCATION_PREFIX = "location.";
private static final String PROP_HIVE_TEXT_PREFIX = "hive.text.";
```

注释自称「shared with connector plugins」，但修饰符是 `private`——连接器根本引用不到，只能抄字面量。

### 2.2 引擎实际会读的键，逐个核实

| 键 | 引擎读取处 | 引擎拿它做什么 |
|---|---|---|
| `file_format_type` | `PluginDrivenScanNode.java:578`（另 :561 把它和 `"es_http"` 比） | 经 `mapFileFormatType`（:1954-1975）映射成 `TFileFormatType`；识别的值只有 `parquet`/`orc`/`text`/`csv`/`json`/`avro`/`es_http`，其余一律落到 `FORMAT_JNI` |
| `path_partition_keys` | `:588` | 逗号切分后作为 `getPathPartitionKeys()`，决定哪些列不从文件里解码 |
| `location.` 前缀 | `:788-789` | 剥掉前缀后作为 `getLocationProperties()`，即 BE 访问存储用的配置 |
| `hive.text.` 前缀 | `:799-863` | 组装 `TFileAttributes`；实际读 12 个后缀：`serde_lib`、`skip_lines`、`column_separator`、`line_delimiter`、`mapkv_delimiter`、`collection_delimiter`、`escape`、`null_format`、`enclose`、`trim_double_quotes`、`is_json`、`openx_ignore_malformed` |
| `query` | `:447` | 打进 EXPLAIN 的 `QUERY:` 行。**这一个连 `private` 常量都没有，是裸字面量** |

反方向（引擎写、连接器读）还有三个合成键，`PluginDrivenScanNode.java:139-146` 定义、`PaimonScanPlanProvider.java:202-208` **按字节复制了一份**，两边注释都写着「keys are byte-identical … so the inject/consume sides stay in lockstep」：`__native_read_splits`、`__total_read_splits`、`__explain_verbose`。这是靠注释维持的字符串对齐，编译器完全不参与。

### 2.3 连接器一侧的现状

- hive：`HiveScanPlanProvider.java:82-84` 自己又定义了一份 `PROP_FILE_FORMAT_TYPE` / `PROP_PATH_PARTITION_KEYS` / `PROP_LOCATION_PREFIX`（值与引擎逐字相同）；`hive.text.` 前缀在 `HiveTextProperties.java:87`。
- hudi：`HudiScanPlanProvider.java:316`、`:317`、`:322`、`:331`、`:341`、`:352` 全是裸字面量。
- iceberg：`IcebergScanPlanProvider.java:1554`、`:1572`、`:1588`、`:1599` 全是裸字面量。
- paimon：`PaimonScanPlanProvider.java:751`、`:752`、`:765`、`:827`、`:839` 全是裸字面量。
- jdbc：`JdbcScanPlanProvider.java:185` 写 `props.put("query", querySql)`。
- es：`EsScanPlanProvider.java:184` 写 `"file_format_type"`；它自己那批键（`query_dsl` 等）有常量，在 :66-74。

顺带核实到两个**只写不读**的键：`table_format_type`（`PaimonScanPlanProvider.java:752`、`HudiScanPlanProvider.java:317` 写入，全仓库没有任何 `get` 端）与 `_table_name`（`EsScanPlanProvider.java:187` 写入，无读端）。它们不属于本任务范围（见 5.3）。

### 2.4 两个返回面

同一份属性还有第二个返回面，`ConnectorScanPlanProvider.java:455`：

```java
default ScanNodePropertiesResult getScanNodePropertiesResult(...) {
    return new ScanNodePropertiesResult(getScanNodeProperties(session, handle, columns, filter));
}
```

核实结论（比调研报告更精确）：**引擎只调这个包装面**（`PluginDrivenScanNode.java:1928`），**6 个**连接器覆写了 `Map` 面（es :156、hive :377、hudi :306、iceberg :1535、jdbc :161、paimon :739），其中 es 同时覆写了包装面（:165），所以真正靠默认委派生效的是**另外 5 个**。这**不是**两套竞争机制——包装面的默认实现显式调用了 `Map` 面。但由此产生一个静默陷阱：一个连接器如果两个面都覆写而实现不一致，`Map` 面的返回值会被彻底丢弃且不报错。接口 javadoc 现在没有任何一句话提醒这件事。

包装对象本身 `ScanNodePropertiesResult.java:46` 与 `:60` 是两个只差一个参数的构造器，一参版把 `hasConjunctTracking` 置 `false`、两参版置 `true`——**「有没有下推追踪」这个布尔位是靠「调用方选了哪个构造器」隐式编码的**，读代码的人看 `new ScanNodePropertiesResult(props)` 完全看不出自己顺带声明了「不做谓词裁剪」。全仓库只有 3 个构造点：`ConnectorScanPlanProvider.java:460`、`PluginDrivenScanNode.java:1932`、`EsScanPlanProvider.java:220`。

### 2.5 `getSerializedTable(Map)` 这条弯路

`ConnectorScanPlanProvider.java:520`：

```java
default String getSerializedTable(Map<String, String> nodeProperties) {
    return null;
}
```

唯一实现是 `PaimonScanPlanProvider.java:1763-1765`：`return properties.get("paimon.serialized_table")`——而这个键正是它自己在 `:770` 写进属性表的。引擎侧 `PluginDrivenScanNode.java:1803-1813` 覆写了 `FileQueryScanNode.getSerializedTable()`（基类定义在 `FileQueryScanNode.java:339`，基类在 `:472` 把结果塞进 `params.setSerializedTable`，对应 thrift `TFileScanRangeParams.serialized_table`，`gensrc/thrift/PlanNodes.thrift:540`）。

所以调研报告说「承接的是引擎侧一个既有通用钩子」是对的。真正的多余是：paimon **已经**覆写了扫描级参数填充 `populateScanLevelParams`（`PaimonScanPlanProvider.java:1355`），拿到的正是同一个 `TFileScanRangeParams` 对象（`PluginDrivenScanNode.java:1823` 传的就是节点的 `params` 字段），它完全可以直接 `params.setSerializedTable(...)`，不必绕「写进属性表 → 引擎回调一个通用名方法 → 从属性表里把自己写的键取回来」这一圈。时序上也没问题：`PluginDrivenScanNode.createScanRangeLocations`（:1816-1826）先调 `super`（基类在 :472 设值），再调 `populateScanLevelParams`，后者在后面执行。

## 三、为什么这是个问题

1. **公共接口的契约不在公共模块**。这是本轮整治反复出现的同一条毛病：接口签名中立（`Map<String,String>`），真正的语义写在引擎的 `private` 常量和各连接器的字面量里。结果是「新增一个连接器」这件事的成本被抬高到必须先读引擎源码——而读源码这件事本身不产生任何编译期保障。
2. **字面量对齐靠注释维持**。三个合成键在引擎和 paimon 里各存一份，靠「byte-identical」的注释约束；`hive.text.` 的 12 个后缀在引擎侧是 `PROP_HIVE_TEXT_PREFIX + "column_separator"` 这类拼接、在 hive 侧是 `PROP_PREFIX + "column_separator"` 的另一次拼接。任一侧改一个字母，编译期毫无反应，运行期表现为「该属性静默失效」：比如 `column_separator` 拼错，文本表读出来整行挤在第一列，而不是报错。
3. **中立接口上挂着源专属命名**。引擎的通用节点里有一个叫 `hive.text.` 的前缀，而它服务的是 TEXT/CSV/JSON 三个格式族、hudi 和 iceberg 走同一条 `getFileAttributes`。这与本项目「通用层不出现源专属符号」的既定纪律直接冲突。
4. **两个返回面缺一句文档，就是一个静默丢结果的坑**。同时覆写两者不报错、不告警，`Map` 面被无声丢弃。这不是当前的活跃缺陷（现役连接器里只有 es 两个都覆写，且两者指向同一个私有实现 `buildScanNodeProperties`，行为一致），但它是给下一个连接器作者留的陷阱。
5. **`getSerializedTable(Map)` 是公共接口上一个语义未定义的方法**。名字通用（「返回序列化后的表」），签名不说明什么算「表」、什么格式、给谁用，唯一实现是把自己写的私有键原样取回。新连接器看到这个方法无法判断自己该不该实现。

用户能观察到什么？这几条**都不会**表现为当前的错误结果——现役连接器的字符串是对齐的。真实后果是可维护性与新增连接器成本，以及一类「改错一个字母，编译通过、查询静默返回错数据」的高危改动窗口。

## 四、用一个最小例子说明

假设我要新增一个连接器（叫它 `foo`），表是分区的 Parquet 文件存在 S3 上。我今天要做的事：

| 我作为连接器作者想做的事 | 今天实际必须怎么做 | 应该怎么做 |
|---|---|---|
| 告诉引擎「用原生 Parquet 读取器，别走 JNI」 | 去翻 `PluginDrivenScanNode.java:1954` 的 `switch`，才知道键叫 `file_format_type`、值必须正好是小写 `"parquet"`，然后在自己代码里写死这两个字面量 | `props.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "parquet")`，识别的取值在常量类的 javadoc 里列着 |
| 告诉引擎「`dt` 列是目录分区列，不要从文件里解码」 | 翻到 `:588`，抄 `path_partition_keys`，还得自己发现值是逗号分隔 | `props.put(ScanNodePropertyKeys.PATH_PARTITION_KEYS, String.join(",", keys))` |
| 把 S3 凭证交给 BE | 翻到 `:788`，抄前缀 `location.`；抄错成 `locations.` 则编译通过、查询时 BE 对私有桶报 403 | `props.put(ScanNodePropertyKeys.LOCATION_PREFIX + k, v)` |
| 表是文本格式，要设分隔符 | 翻到 `:799-863` 一行行数出 12 个后缀，还要接受自己的通用连接器代码里出现 `hive.text.` 这个名字 | `props.put(ScanNodePropertyKeys.TEXT_COLUMN_SEPARATOR, "")` |
| 想知道属性表里 `__` 开头的键是什么 | 无处可查（引擎侧 `private`，只有 paimon 复制了一份） | 常量类里明确标注：这三个是引擎注入给 EXPLAIN 用的，永不发往 BE |

一句话：今天新增连接器需要「读引擎源码 + 抄 5 处字面量」，改完后是「引用 1 个常量类」。

## 五、解决方案

### 5.1 目标状态

**（1）公共模块新增一个键常量类**，路径 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ScanNodePropertyKeys.java`，签名草案：

```java
/** 扫描节点属性表（ConnectorScanPlanProvider#getScanNodeProperties 的返回值）的键契约。 */
public final class ScanNodePropertyKeys {

    // ---- 连接器 -> 引擎：引擎会读的键 ----
    /** 取值：parquet / orc / text / csv / json / avro / es_http；其余（含缺省）= JNI 读取器。 */
    public static final String FILE_FORMAT_TYPE = "file_format_type";
    /** 逗号分隔的目录分区列名，大小写按 Doris 列名原样。 */
    public static final String PATH_PARTITION_KEYS = "path_partition_keys";
    /** 前缀；剥掉前缀后的键值对整体作为 BE 访问存储的配置。 */
    public static final String LOCATION_PREFIX = "location.";
    /** 连接器渲染的远端查询文本，仅用于 EXPLAIN 的 QUERY: 行。 */
    public static final String REMOTE_QUERY = "query";

    /** 文本类格式（TEXT/CSV/JSON）属性前缀。前缀值沿用历史字面量，与 hive 无关。 */
    public static final String TEXT_PROPERTY_PREFIX = "hive.text.";
    public static final String TEXT_SERDE_LIB = TEXT_PROPERTY_PREFIX + "serde_lib";
    public static final String TEXT_SKIP_LINES = TEXT_PROPERTY_PREFIX + "skip_lines";
    // …其余 10 个后缀同上，一个不多一个不少（见 2.2 表格）

    // ---- 引擎 -> 连接器：合成键，永不发往 BE，只供 appendExplainInfo 读 ----
    public static final String SYNTHETIC_NATIVE_READ_SPLITS = "__native_read_splits";
    public static final String SYNTHETIC_TOTAL_READ_SPLITS = "__total_read_splits";
    public static final String SYNTHETIC_EXPLAIN_VERBOSE = "__explain_verbose";

    private ScanNodePropertyKeys() {}
}
```

原则三条：**只收录引擎读的键与引擎注入的合成键**；**所有字面量的值一个字节都不改**；连接器私有键（`paimon.*`、`transactional_hive`、es 那批）留在各自连接器里不动。`TEXT_PROPERTY_PREFIX` 是「符号名中立、字面量保持历史值」的处理——改值需要同时动引擎与 hive 两侧，属于纯改名收益、不在本任务范围（见 5.3）。

**（2）包装对象改成具名工厂**，`ScanNodePropertiesResult`：两个构造器降为 `private`，新增

```java
public static ScanNodePropertiesResult of(Map<String, String> properties);                 // 不做谓词裁剪
public static ScanNodePropertiesResult withPushdownTracking(Map<String, String> properties,
        Set<Integer> notPushedConjunctIndices);                                            // 做谓词裁剪
```

3 个构造点全部改成对应工厂。

**（3）两个返回面补文档**：在 `getScanNodeProperties` 与 `getScanNodePropertiesResult` 的 javadoc 上各加一段，明确「引擎只调用包装面；包装面的默认实现委派 `Map` 面；**连接器只应覆写其中一个**——若覆写包装面，`Map` 面不再被引擎调用，两者不一致时 `Map` 面的结果会被静默丢弃」。

**（4）`getSerializedTable(Map)` 走直接设置的路径**：从 `ConnectorScanPlanProvider` 删掉这个方法；paimon 在 `populateScanLevelParams` 里直接 `params.setSerializedTable(properties.get(PROP_SERIALIZED_TABLE))`（`paimon.serialized_table` 顺便提成 paimon 自己的私有常量，因为它同一个类里出现两次）；引擎侧删掉 `PluginDrivenScanNode.getSerializedTable()` 覆写（:1803-1813）。删完后 `FileQueryScanNode.getSerializedTable()`（:339）与 `:472` 的调用在本仓库再无任何覆写者，一并删除（`fe-core` 纯减，符合只出不进）。

### 5.2 改动清单

| 文件 | 要做什么 |
|---|---|
| `fe-connector-api/.../scan/ScanNodePropertyKeys.java` | **新增**。按 5.1（1）建类，javadoc 写清每个键的取值约定、读取方是引擎还是连接器 |
| `fe-connector-api/.../scan/ConnectorScanPlanProvider.java` | `getScanNodeProperties`（:403-423）javadoc 指向常量类并加「只覆写一个面」的说明；`getScanNodePropertiesResult`（:437-462）同样加说明，默认实现改用 `ScanNodePropertiesResult.of(...)`；**删除** `getSerializedTable(Map)`（:510-522） |
| `fe-connector-api/.../scan/ScanNodePropertiesResult.java` | 两个构造器改 `private`，新增 `of` / `withPushdownTracking` 两个具名工厂，javadoc 说明两者语义差别 |
| `fe-core/.../datasource/scan/PluginDrivenScanNode.java` | 删 :127-130 与 :139-146 共 7 个 `private` 常量，全部改引用常量类；:447 的裸 `"query"` 改成 `ScanNodePropertyKeys.REMOTE_QUERY`；:1932 改用 `of(...)`；**删除** `getSerializedTable()` 覆写（:1803-1813）；把 :943-952 与 :1905-1908 注释里提到的「serialized-table 路径」改成「扫描级参数填充路径」（事实变了，注释必须跟着变） |
| `fe-core/.../datasource/scan/FileQueryScanNode.java` | 删除 `getSerializedTable()`（:339-341）与 :472 的调用 |
| `fe-connector-hive/.../HiveScanPlanProvider.java` | 删自有的三个重复常量（:82-84），改引用公共常量类；`PROP_TRANSACTIONAL_HIVE`（:89）**保留**（连接器私有信号，自己的 `populateScanLevelParams` 读） |
| `fe-connector-hive/.../HiveTextProperties.java` | `PROP_PREFIX`（:87）改为引用 `ScanNodePropertyKeys.TEXT_PROPERTY_PREFIX`，各处拼接改用对应的具名后缀常量。注意 `hive.text.json_serde_lib`（:177）引擎不读，是 hive 私有键，留在本类里 |
| `fe-connector-hudi/.../HudiScanPlanProvider.java` | :316、:322、:331、:341、:352 的字面量改常量引用（`table_format_type` :317 不动，见 5.3） |
| `fe-connector-iceberg/.../IcebergScanPlanProvider.java` | :1554、:1572、:1588、:1599 的字面量改常量引用 |
| `fe-connector-paimon/.../PaimonScanPlanProvider.java` | :751、:765、:827、:839 改常量引用；删 :202-208 那三个复制的合成键常量，改引用公共常量类（`appendExplainInfo` 里的读取点随之改）；**删** `getSerializedTable`（:1763-1765）；`populateScanLevelParams`（:1355）里增加 `params.setSerializedTable(...)`；`paimon.serialized_table` 提为私有常量 |
| `fe-connector-jdbc/.../JdbcScanPlanProvider.java` | :185 的 `"query"` 改 `ScanNodePropertyKeys.REMOTE_QUERY` |
| `fe-connector-es/.../EsScanPlanProvider.java` | :184 改常量引用；**删除** `Map` 面覆写（:155-162）——引擎只调包装面（:165 已覆写），这个覆写从引擎侧不可达 |
| `fe-connector-es/.../EsScanPlanProviderTest.java` | :166 与 :341 两处调用 `getScanNodeProperties` 改为 `getScanNodePropertiesResult(...).getProperties()` |
| 相关单测 | paimon 加断言（见第六节）；其余测试若引用了被删的构造器/方法，同步改到工厂方法 |

### 5.3 明确不要顺手做的事

- **不要改任何键的字面量值**，包括不要把 `hive.text.` 改成 `text.`。改值必须引擎与 hive 同步改，收益纯粹是命名，风险是「漏改一处 → 文本表静默读错」。本任务只把符号名中立化。
- **不要删只写不读的键**（paimon/hudi 的 `table_format_type`、es 的 `_table_name`、hive 的 `hive.text.json_serde_lib`）。它们是独立的死键清理，判活需要单独核对 BE 与 JNI 侧，混进本任务会把「纯结构调整」变成「有行为风险的删除」。
- **不要碰 `PluginDrivenScanNode:561` 与 :1829-1835 那两处 ES 专属分支**。那是「把通用扫描节点里的源专属分支搬进连接器」那一项任务的正题；本任务只把它读的键换成常量，分支本身原样保留。
- **不要试图和 `fe-core` 已有的 `FileFormatConstants.PROP_PATH_PARTITION_KEYS`（`FileFormatConstants.java:49`）合并**。那是表函数（TVF）的属性命名空间，字面量相同纯属巧合，且合并会往 `fe-core` 增加连接器相关代码，撞「只出不进」。
- **不要把 `Map` 面删掉只留包装面**。5 个连接器靠默认委派生效，删掉等于强迫每个连接器去处理谓词追踪参数，是无谓的扩大改动。本任务只补文档、不改机制。
- **不要为「键必须来自常量类」加 shell/正则构建门禁**。判断一个 `props.put` 的第一个实参是不是常量引用需要理解 Java 语义，本仓库已有明确结论：这类门禁误报比漏报更毒。改动本身由编译期常量引用保障。

## 六、怎么验证

1. **零残留 grep（存在性检查，可直接跑）**——这是本任务能机器验证的部分：
   - `grep -rn '"file_format_type"\|"path_partition_keys"\|"location\."\|"hive\.text\.\|"__native_read_splits"\|"__total_read_splits"\|"__explain_verbose"' --include=*.java fe/fe-core/src/main fe/fe-connector/*/src/main` 期望只在 `ScanNodePropertyKeys.java` 里命中。
   - `grep -rn "getSerializedTable" --include=*.java fe/ | grep -v /target/` 期望 0 命中（`be-java-extensions` 里 BE 侧读 `serialized_table` 的那两处不在 `fe/fe-core` 与 `fe/fe-connector` 下，属另一侧，不受影响）。
2. **paimon 的序列化表必须仍然到位**（唯一有运行期行为的改动，必须有断言）。在 `PaimonScanPlanProviderTest` 加一条：先 `getScanNodeProperties(...)` 拿到属性表，再 `populateScanLevelParams(new TFileScanRangeParams(), props)`，断言 `params.isSetSerializedTable()` 且值与属性表里 `paimon.serialized_table` 相等。WHY 要写进断言：BE 的 paimon JNI 读取器在 `be/src/format_v2/jni/paimon_jni_reader.cpp:68-71` 对缺失的 `serialized_table` 直接抛错（"missing serialized_table … possibly caused by FE/BE version mismatch"），所以这个字段丢了就是 paimon 全表查询失败。**变异验证**：把新加的 `params.setSerializedTable(...)` 那一行注释掉 → 该断言必须变红。
3. **两个返回面的委派关系钉一条单测**（放 `fe-connector-api` 的测试源）：写一个只覆写 `Map` 面的匿名 `ConnectorScanPlanProvider`，断言 `getScanNodePropertiesResult(...).getProperties()` 拿到的是那张表、且 `hasConjunctTracking()` 为 `false`；再写一个覆写包装面并用 `withPushdownTracking` 的，断言 `hasConjunctTracking()` 为 `true`。WHY：这两条正是接口新增那段文档的行为承诺，文档不能只是注释。
4. **es 删掉 `Map` 面覆写后行为不变**：`EsScanPlanProviderTest` 改到包装面后原有断言应全部照旧通过（两个覆写本来指向同一个私有实现 `buildScanNodeProperties`，:173）。这条不需要新增断言，通过即证。
5. **编译门禁（最强单一信号）**：全反应堆**含测试源**编译，禁用任何跳过测试编译的参数——
   `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile`
   它同时覆盖「删掉接口方法后所有实现都清理干净」「构造器改 `private` 后调用点全部改到工厂」「`import` 无残留（checkstyle 扫测试源）」三件事。
6. **跑受影响模块的单测，必须显式关掉 maven build cache**（否则 surefire 会被静默跳过、`BUILD SUCCESS` 是空的）：
   `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -Dmaven.build.cache.enabled=false -pl fe-connector/fe-connector-api,fe-connector/fe-connector-paimon,fe-connector/fe-connector-es,fe-connector/fe-connector-hive,fe-connector/fe-connector-hudi,fe-connector/fe-connector-iceberg,fe-connector/fe-connector-jdbc test`
   `fe-core` 侧至少跑 `PluginDrivenScanNode*` 那一批（尤其 `PluginDrivenScanNodeVerboseExplainTest`、`PluginDrivenScanNodeExplainStatsTest`，它们覆盖合成键与 EXPLAIN 行）。
7. **端到端回归**：不需要新增用例，但**必须实跑 paimon 目录的查询回归**（`serialized_table` 是它读数据的必要条件，第 2 条单测只覆盖 FE 侧装配）。hive 文本/CSV/JSON 表的读回归也建议跑一遍，因为 `hive.text.*` 的 12 个后缀经过了一次符号替换。其余连接器只有字面量换常量，属性表内容按字节不变。

## 七、风险与回退

- **最大风险是「换常量时打错一个后缀」**，尤其 `hive.text.*` 那 12 个。表现不是报错而是该属性静默失效（例如分隔符回退默认值 → 文本表列错位）。防线是第六节第 1 条的 grep（旧字面量必须全部消失）+ 第 7 条的 hive 文本表回归。建议实现时用「先把常量类写全、再逐文件替换、每替换一个文件立刻 grep 该文件是否还有旧字面量」的节奏，不要跨文件批量正则替换。
- **paimon 的序列化表是硬失败点**：BE 侧对缺失直接抛错，不会静默降级。这既是风险也是好事——一旦漏设，回归立刻红，不会带着错数据上线。
- **不涉及 thrift 有线格式**：`serialized_table` 字段（`PlanNodes.thrift:540`）本身不动，只是改由谁来 set。
- **不涉及 Gson 持久化**：属性表只在单次查询规划期内存活，不进元数据镜像。
- **接口有删除方法（`getSerializedTable`）与构造器降级为 `private`**：插件与公共模块必须同批构建、同批部署；混用老插件包 + 新公共模块会在类加载期报 `NoSuchMethodError`。本仓库连接器与 `fe-core` 一起构建发布，正常流程不会混用；手工替换单个插件 zip 需重新打包全部连接器。
- **回退**：本任务是「新增一个常量类 + 引用替换 + 一处调用路径改写」，无数据面残留，`git revert` 单个提交即可完整回到原状。建议拆成两个提交（一是常量类 + 引用替换；二是两个返回面文档 + 具名工厂 + `getSerializedTable` 改写），这样第二个提交若有问题可单独回退。

## 八、相关背景

- 调研报告 `../audit-report.md`：
  - 第 10.1 节（b）小节「扫描节点属性表」——本任务的主问题来源：键契约不在公共模块，一半散在引擎私有常量里、一半散在各连接器字面量里，建议在公共模块建一个键常量类；
  - 第 10.5 节「方法名与行为不符 / 重载堆叠」最后一条——两个返回面（`Map` 面与包装对象面）同时覆写会静默丢掉一个，且包装对象用「调了哪个构造器」隐式编码一个布尔位；
  - 附录 A 第 23 条——`getSerializedTable(Map)`，注意其中的「复核收窄」已确认它承接的是引擎既有通用钩子，本文按修正后的事实叙述；
  - 附录 A 第 93 / 94 条与第 139 / 141 条——两个返回面与键契约散落的原始判定。
  - 第 8.3 节「通用引擎代码里残留的数据源分支」——通用扫描节点里的 ES 专属分支，是另一项任务，与本任务在同一文件相邻位置，见 5.3。
- 设计纪律：见同目录 `07-write-down-the-design-rules.md`（通用层不出现源专属符号、`fe-core` 只出不进）。
- 同批的接口删除类任务 `11-delete-dead-surface-batch-one.md`、`12-delete-dead-surface-batch-two.md`、`13-delete-scan-range-type-enum.md` 也改 `fe-connector-api` 的 `scan` 包，若同期进行请注意同文件冲突（与本任务无逻辑依赖）。
