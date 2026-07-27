# 20. 分区空值哨兵的中立化命名与归一方法下沉

> **优先级**：第五优先级（中立化） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-api`、`fe-connector-hudi`、`fe-connector-hive`、`fe-connector-paimon`、`fe-core`（主源只做「一个重复常量定义换成引用」的净删，测试源改名引用）
> **预计改动规模**：约 10～12 个文件；公共模块净删约 35 行、hudi 净增约 12 行、fe-core 净删约 1 行；新增 2 个单测文件约 90 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

面向所有连接器的中立公共模块 `fe-connector-api` 里，唯一一个带数据源品牌的字符串常量叫 `HIVE_DEFAULT_PARTITION`，旁边还挂着三个名字通用、语义却只对 hive 与 hudi 的目录式分区成立的归一方法（`normalize` / `isNullPartitionValue` / `normalizePartitionValue`）——hive 和 paimon 都在注释里明确写了「不要走这些方法」。本任务把常量改成中立名字（**字符串值一个字节都不动**），把三个只有 hudi 一个生产调用方的方法下沉进 hudi 连接器，并把引擎里那份重复的同串定义改成引用公共常量。

## 二、背景：现在的代码是怎么写的

**公共模块本体**：`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorPartitionValues.java`，整个文件只有 73 行，**没有类级 javadoc**：

```java
public final class ConnectorPartitionValues {                                  // :24
    public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";  // :26
    public static final String NULL_PARTITION_VALUE = "\\N";                   // :27

    public static Normalized normalize(List<String> partitionValues) { ... }   // :32
    public static boolean isNullPartitionValue(String value) {                 // :46
        return value == null || HIVE_DEFAULT_PARTITION.equals(value)
                || NULL_PARTITION_VALUE.equals(value);
    }
    public static String normalizePartitionValue(String value) { ... }         // :51
    public static final class Normalized { ... }                               // :56
}
```

**谁在用这个常量（只用常量、不用方法）**：

| 位置 | 干什么 |
|---|---|
| `fe-core` `FilePartitionUtils.java:143`（import 在 :21） | 引擎自己按 hive 风格目录名解析分区列：`boolean isNull = ConnectorPartitionValues.HIVE_DEFAULT_PARTITION.equals(pair[1]);` |
| `fe-connector-hive` `HiveScanRange.java:173` | 只做窄比较（`value == null || 常量.equals(value)`），注释 :159-161 明确写「用窄比较，**不要**用 `ConnectorPartitionValues.normalize`，它会把字面量 `\N` 也判成空」 |
| `fe-connector-hive` `HiveConnectorMetadata.java:1206` | 生成分区空值标志，注释 :1198-1200 同样点名「不用更宽的 `isNullPartitionValue`」 |
| `fe-connector-paimon` `PaimonConnectorMetadata.java:1263` | **反过来**：paimon 把自己的空分区名（`partition.default-name`，默认 `__DEFAULT_PARTITION__`）**主动归一到这个常量**，注释 :1260-1261 自陈「名字归一到 Doris 规范哨兵」 |
| `fe-connector-paimon` `PaimonScanRange.java:281-283` | 注释里第三次明确绕开 `normalize` |

**谁在用那三个方法**：整仓库只有一处生产调用点，`fe-connector-hudi` `HudiScanRange.java:247-248`：

```java
ConnectorPartitionValues.Normalized normalized = ConnectorPartitionValues.normalize(pathValues);
rangeDesc.setColumnsFromPathKeys(pathKeys);
rangeDesc.setColumnsFromPath(normalized.getValues());
rangeDesc.setColumnsFromPathIsNull(normalized.getIsNull());
```

`NULL_PARTITION_VALUE`（`\N`）在类外零引用；`isNullPartitionValue` / `normalizePartitionValue` 也只有类内调用方；`fe-connector-api` 的测试目录里没有任何一个用例覆盖这个类。

**引擎侧还有第二份同串定义**：`fe-core` `TablePartitionValues.java:47` 又写了一遍 `public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";`。它是活的：本类 :162 用它决定 `PartitionValue` 的空值位（经 `PluginDrivenExternalTable.java:858` 的 `addPartitions` 走活路径），`MetadataGenerator.java:2067` 用它把 `partition_values()` 表函数的这一格渲染成 SQL NULL。这个类没有任何 Gson 注解，不涉及持久化格式。

**为什么这个字符串值不能改**：`test_hive_partition_values_tvf.groovy:66` 与 `:73` 直接在 SQL 里写 `where t_int != "__HIVE_DEFAULT_PARTITION__"`，且 :71 建了一个持久化的 internal 视图；`test_paimon_mtmv.groovy:272` 的注释记录了物化视图曾按 `region IN ('__HIVE_DEFAULT_PARTITION__')` 刷新。分区名一旦进了视图定义、物化视图分区、以及 BE 的 `columns_from_path` 字节，就是对外可见的持久化标识。

**另外一件容易搞混的事**：`ConnectorPartitionInfo` 的分区空值标志（结构化布尔位）与这个字符串哨兵**不是替代关系**。`PluginDrivenMvccExternalTable.java:308-323` 的 javadoc 已经写清了分工：标志服务于 FE 侧构造带类型的 `NullLiteral`（否则 INT / DATE 分区列会在解析哨兵字符串时抛异常、整个分区被静默丢弃，表被误报成未分区），哨兵服务于分区名身份与 BE 列路径解析的字节兼容。同一段 javadoc 还点明「hive 与 paimon 渲染出**同一个**哨兵字符串但空值语义不同，所以 fe-core 不能靠字符串比较判空」。

## 三、为什么这是个问题

三件事，都属于「命名与归属」层面，不是正确性缺陷：

1. **中立模块里挂着数据源品牌名**。`fe-connector-api` 是给所有连接器（包括第三方自研连接器）看的公共契约面，`HIVE_DEFAULT_PARTITION` 是里面唯一一个带数据源品牌的字符串常量。一个写 paimon 连接器的人被迫引用一个叫「hive 默认分区」的常量来表达「Doris 规范的空分区名」，读代码的人会以为自己走错了模块。

2. **名字通用、语义专有的方法会把人骗进坑里**。`isNullPartitionValue(String)` 这个签名看不出任何限定，实际语义是「hive/hudi 的目录式分区里，`null`、`__HIVE_DEFAULT_PARTITION__`、字面量 `\N` 三者都算空」。对类型化分区值的连接器（paimon 的分区值本来就是 Java 类型，`\N` 是合法的字符串数据）用它就会**把真实数据判成 NULL**。现状是靠三处注释（`HiveScanRange.java:159-161`、`HiveConnectorMetadata.java:1198-1200`、`PaimonScanRange.java:281-283`）挡住的——需要三条注释反复警告「别用公共方法」，说明这个方法放错了地方。下一个连接器作者不会先读别人的注释。

3. **同一个字面量三处可写、两处已写**。公共模块和 `fe-core` 各有一份活定义，谁改一处都不会让另一处编译失败。这也是为什么**不能**把常量下沉到 hive 连接器：引擎自己的 `FilePartitionUtils` 在用它（引擎不能 import 插件），非 hive 的 paimon 也在主动往它归一，下沉的结果是从两份变三份。

## 四、用一个最小例子说明

假设我要新增一个连接器 X，它的分区值是类型化的（跟 paimon 一样），空分区的目录名是 `<NULL>`，而且它有一列的真实数据里就存着字符串 `\N`。

| 我今天读到什么 | 我大概率会怎么写 | 实际发生什么 |
|---|---|---|
| 中立公共模块里有 `ConnectorPartitionValues.normalize(values)`，名字通用，附近没有任何限定说明 | 直接调它，一次拿到值列表和空值标志列表 | 那一行真实数据 `\N` 被判成 SQL NULL，查询静默少行；要发现这点，得先读到 hive 和 paimon 源码里的三条「别用它」注释 |
| 我想表达「这个分区是空值」的规范分区名 | 找不到中立名字，只能 `import ...HIVE_DEFAULT_PARTITION` | 我的连接器里出现了一个 hive 品牌常量，评审要花时间解释「这不是 hive 专用」 |

改完之后：公共模块里只剩一个中立命名的常量（值不变），旁边有一句 javadoc 说明它是「Doris 规范的空分区名、字节冻结」；那个会误伤的 `normalize` 不再出现在公共面上，它连同 `\N` 语义一起留在唯一真正需要它的 hudi 连接器里。连接器 X 的作者拿不到那把误伤的刀，只能按自己的类型化语义自己填空值标志——这正是 hive、paimon、iceberg 现在各自的做法。

## 五、解决方案

### 5.1 目标状态

公共模块只保留常量，签名草案：

```java
/**
 * Doris 规范的分区名相关常量。
 *
 * <p>NULL_PARTITION_NAME 是「这个分区列的值是真正的 NULL」在**分区名**里的规范写法。它的字面量
 * 沿用 hive 的历史取值，且**必须逐字冻结**：分区名会进入视图 / 物化视图定义、partition_values()
 * 表函数结果，以及 BE 的 columns_from_path 字节。连接器若有自己的空分区名（如 paimon 的
 * partition.default-name），应在渲染分区名时归一到本常量。
 *
 * <p>注意：本常量不能替代 ConnectorPartitionInfo 的分区空值标志。标志服务于 FE 侧构造带类型的
 * NullLiteral，哨兵服务于分区名身份；「值是否为空」必须由连接器用标志声明，fe-core 不做字符串比较。
 */
public final class ConnectorPartitionValues {

    public static final String NULL_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__";

    /** @deprecated 改用 {@link #NULL_PARTITION_NAME}；本别名仅为外部连接器保留一轮。 */
    @Deprecated
    public static final String HIVE_DEFAULT_PARTITION = NULL_PARTITION_NAME;

    private ConnectorPartitionValues() {
    }
}
```

hudi 一侧：`normalize` / `Normalized` / `\N` 常量全部消失，改成 `HudiScanRange.populateRangeParams` 里的一段内联循环（与 hive、paimon、iceberg 三家现在的写法一致，不新增类、不新增抽象）：

```java
private static final String HUDI_NULL_PARTITION_VALUE = "\\N";   // hudi 目录式分区的空值渲染，保持发给 BE 的字节不变
...
String value = entry.getValue();
// hudi 的分区值来自路径目录名：Java null、规范空分区名、以及历史上的 "\N" 都算空。
// 这三条只对目录式分区成立，所以留在 hudi 里，不放在中立公共模块（hive 与 paimon 都刻意绕开它）。
boolean nullValue = value == null
        || ConnectorPartitionValues.NULL_PARTITION_NAME.equals(value)
        || HUDI_NULL_PARTITION_VALUE.equals(value);
pathKeys.add(entry.getKey());
pathValues.add(nullValue ? HUDI_NULL_PARTITION_VALUE : value);
pathIsNull.add(nullValue);
```

这段与原 `normalize` 逐字等价：原实现对 `null` 与规范空分区名都渲染成 `\N`，对字面量 `\N` 原样保留（也是 `\N`）并置空值位为 true——新代码三种情况都渲染 `\N`、空值位 true，发给 BE 的字节完全一致。注意 hudi 的空值渲染是 `\N` 而 hive / paimon / iceberg 是空串（`HiveScanRange.java:175`、`PaimonScanRange.java:287`），**本任务不统一这个差异**（见 5.3）。

`fe-core` 一侧：删掉 `TablePartitionValues.java:47` 那份重复定义，两个使用点改为引用公共常量。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe-connector-api/.../scan/ConnectorPartitionValues.java` | 新增中立常量 `NULL_PARTITION_NAME`（值不变）；旧名保留为 `@Deprecated` 别名并指向新常量；补类级 javadoc（写清「值冻结」「与空值标志的分工」）；删除 `NULL_PARTITION_VALUE`、`normalize`、`isNullPartitionValue`、`normalizePartitionValue`、嵌套类 `Normalized`（共约 35 行） |
| `fe-connector-hudi/.../HudiScanRange.java:247-251` | 用 5.1 的内联循环替换 `normalize` 调用；加 `HUDI_NULL_PARTITION_VALUE` 私有常量与 WHY 注释；`import` 保留（仍要引用中立常量） |
| `fe-connector-hive/.../HiveScanRange.java:173` | 常量引用改新名；顺手把 :160 那句「不要用 `ConnectorPartitionValues.normalize`」的注释改成指向 hudi 内部实现（否则注释指向一个已不存在的符号） |
| `fe-connector-hive/.../HiveConnectorMetadata.java:1206` | 常量引用改新名；:1200 提到 `isNullPartitionValue` 的注释同上改写 |
| `fe-connector-paimon/.../PaimonConnectorMetadata.java:1263`（及 :1245 注释） | 常量引用与注释里的符号名改新名 |
| `fe-connector-paimon/.../PaimonScanRange.java:281-283` | 注释里提到 `normalize` 的部分改写（无代码引用） |
| `fe-core/.../datasource/TablePartitionValues.java:47,162` | 删掉重复的常量定义，:162 改为引用 `ConnectorPartitionValues.NULL_PARTITION_NAME`（fe-core 主源净删一行，不新增数据源相关代码） |
| `fe-core/.../tablefunction/MetadataGenerator.java:2067` | 引用改为公共常量；去掉不再需要的 `TablePartitionValues` import（若该 import 还有别的用途则保留） |
| `fe-core` 测试：`PluginDrivenMvccExternalTableTest.java`、`ListPartitionItemTest.java` | 9 处 `TablePartitionValues.HIVE_DEFAULT_PARTITION` 引用改为公共常量（`PluginDrivenMvccExternalTableTest` 8 处：287 / 291 / 297 / 310 / 319 / 329 / 335 / 346 行；`ListPartitionItemTest` 1 处：64 行）。纯改名，断言语义不动，但处数比看上去多，别漏改 |
| `fe-connector-paimon` / `fe-connector-hive` 测试中引用旧常量名的用例 | 改新名（`PaimonConnectorMetadataPartitionTest` 5 处等；写死字面量的用例不必改） |
| **新增** `fe-connector-hudi/src/test/java/.../HudiScanRangePartitionValuesTest.java` | 见第六节 |
| **新增** `fe-connector-api/src/test/java/.../scan/ConnectorPartitionValuesTest.java` | 见第六节 |

### 5.3 明确不要顺手做的事

- **不要改字符串值，也不要「顺手」把 `__HIVE_DEFAULT_PARTITION__` 改成看起来更中立的字面量。** 它是持久化标识：视图 / 物化视图定义、`partition_values()` 表函数输出、BE 列路径解析都按它对齐（证据见第二节的两个回归套件）。本任务改的只是 Java 侧的符号名。
- **不要把常量下沉到 hive 连接器。** 引擎的 `FilePartitionUtils.java:143` 在用它，而引擎不能 import 插件；paimon 也在主动往它归一。下沉只会让同一个字面量变成三份。
- **不要试图用结构化空值标志「取代」哨兵、也不要反向取代。** 两者服务的对象不同（第二节最后一段）。任何「统一成一套」的提议都会打破 FE 侧类型化空值或 BE 侧字节兼容中的一个。
- **不要顺手统一 hudi 的 `\N` 与 hive / paimon 的空串。** 这两个渲染都是发给 BE 的 `columns_from_path` 值，理论上空值位为 true 时 BE 会忽略字符串，但这属于行为变更、需要端到端验证，与本次的命名与归属整治无关。要做就单独立项。
- **不要重命名 `ConnectorPartitionValues` 这个类**（哪怕它瘦到只剩一个常量）。类改名会牵动全部连接器的 import，收益为零。
- **不要顺手清理 `TablePartitionValues` 这个类**（它在 `PluginDrivenExternalTable.getNameToPartitionItems` 的活路径上）。本任务只删它那一行重复的常量定义。
- **不要为「公共模块里不许出现数据源品牌字符串」写 shell 或正则门禁。** 本仓库已有结论：这类门禁只适合存在性与前缀类不变量，去校验「哪个字符串算品牌名」必然误报，而误报会挡住合法构建。

## 六、怎么验证

**新增单测一：hudi 分区值渲染（这是本任务唯一有行为风险的地方）**
`fe-connector-hudi/src/test/java/org/apache/doris/connector/hudi/HudiScanRangePartitionValuesTest.java`，用 `new HudiScanRange.Builder().partitionValues(...)` 构造后调 `populateRangeParams(new TTableFormatFileDesc(), new TFileRangeDesc())`（现有 `HudiScanRangeTest` 就是这个套路），断言四种输入的 `columns_from_path` / `columns_from_path_is_null`：

| 输入分区值 | 断言的值 | 断言的空值位 |
|---|---|---|
| `"2024-01-01"`（普通值） | `"2024-01-01"` | `false` |
| `"__HIVE_DEFAULT_PARTITION__"` | `"\N"` | `true` |
| Java `null` | `"\N"` | `true` |
| 字面量 `"\N"` | `"\N"` | `true` |

再加一条：分区值为空 map 时，三个 `columns_from_path*` 字段一个都不设置（保持现状：现有代码在 `partValues` 空时整段跳过）。测试注释必须写清 WHY——**这四行断言就是「下沉不改字节」的证据**，hudi 的分区值来自路径目录名，所以这三条空值判定成立；hive / paimon 用的是别的判定，所以这段逻辑不能回到公共模块。

**变异验证（必须做，写进测试注释）**：把 `pathValues.add(nullValue ? HUDI_NULL_PARTITION_VALUE : value)` 改成 `pathValues.add(value)` → 第 3 行（Java `null`）必须变红；把 `nullValue` 的第三个条件（字面量 `\N`）删掉 → 第 4 行必须变红。两条各自能被对应的错误写法打红，才算钉住了「与原 `normalize` 逐字等价」。

**新增单测二：常量值冻结**
`fe-connector-api/src/test/java/org/apache/doris/connector/api/scan/ConnectorPartitionValuesTest.java`，两条断言：`NULL_PARTITION_NAME` 逐字等于 `"__HIVE_DEFAULT_PARTITION__"`；过时别名与新常量是同一个值。注释写清 WHY：改这个字面量会让已持久化的视图 / 物化视图分区与 BE 列路径解析对不上。这是一条「不许有人顺手美化字面量」的护栏测试，不是行为快照。

**回归既有用例**：`fe-core` 的 `PluginDrivenMvccExternalTableTest`、`ListPartitionItemTest`、`BrokerUtilTest`（后者覆盖 `FilePartitionUtils.parseColumnsFromPath`），以及 `fe-connector-paimon` 的 `PaimonConnectorMetadataPartitionTest`、`fe-connector-hive` 的 `HiveScanRangePartitionValuesTest` / `HiveConnectorMetadataPartitionListTest`。这些用例在改名后必须**不改断言**地继续通过。

**编译门禁（最强单一信号）**：全反应堆含测试源编译，**不许**加跳过测试编译的参数：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -DskipTests
```

这一条同时验证了「过时别名不影响任何现有编译单元」和「删掉的方法确实无人引用」。

**跑测试**必须显式关掉 maven build cache，否则 surefire 会被静默跳过、`BUILD SUCCESS` 是空的：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
    -pl fe-connector/fe-connector-hudi,fe-connector/fe-connector-api,fe-connector/fe-connector-hive,fe-connector/fe-connector-paimon \
    -Dmaven.build.cache.enabled=false \
    -Dtest=HudiScanRangePartitionValuesTest+ConnectorPartitionValuesTest+HiveScanRangePartitionValuesTest+PaimonConnectorMetadataPartitionTest test
```

要读输出里的 `Tests run:` 行确认用例真的跑了，不要只看 `BUILD SUCCESS`。

**端到端回归**：本任务不改任何发给 BE 的字节，理论上不需要。若要保险，跑 `test_hive_partition_values_tvf`（表函数与视图里的哨兵字符串）与 `test_paimon_mtmv`（物化视图空分区刷新）两个既有套件即可，需要 docker 环境，本地不跑，不阻塞合并。

## 七、风险与回退

风险低，改动分成三块，每块的失败模式都很好识别：

1. **改名**：编译期强制，改漏就编不过；过时别名保证了本仓库之外按旧名编译的连接器仍能通过。
2. **hudi 下沉**：唯一有运行时语义的部分，但新代码与原 `normalize` 逐字等价，由第六节的四行断言加两次变异验证钉住。真出问题的表现是 hudi 分区表某一列的空值行读成字符串 `\N` 或反之——回退只需把那段循环换回 `ConnectorPartitionValues.normalize`（方法从 git 历史取回即可）。
3. **fe-core 去重**：把一份重复的字面量换成引用，无行为变化。

需要留意的一点：`@Deprecated` 别名会让编译器对仍然引用旧名的代码报 deprecation 警告。本仓库内所有引用都在本任务里一次改完，因此不会新增警告；如果 CI 打开了「警告即错误」，那就必须确保改全（`grep -rn "HIVE_DEFAULT_PARTITION"` 应只剩公共模块的别名声明、以及测试与注释里写死字面量的地方）。

回退：三块互不依赖，可以单独 revert。

## 八、相关背景

- 调研报告 `plan-doc/connector-public-interface-cleanup/audit-report.md`：
  - 第 8.1 节的清单表格中 `ConnectorPartitionValues.HIVE_DEFAULT_PARTITION` 那一行——本任务对应的那条建议：常量不删不改值、只做中立化命名 + 保留旧名别名一轮，三个归一方法下沉到唯一的生产调用方；
  - 附录 A 第 22 条（公共 API 唯一的数据源品牌字符串常量，判定成立）、第 35 / 36 条（命名中立性与跨模块重复定义）、第 65 条（两个 public static 助手只有类内调用方）、第 89 条（结构化空值标志与哨兵**不是**两套冗余机制的复核收窄）；
  - 第十六节「明确不建议动的部分」第 10 条——哨兵的字符串值不能改（物化视图与表函数已持久化这些名字，BE 列路径解析也依赖它）；附录 C.2「三处需要改结论」第 2 条——不能下沉到 hive 连接器（引擎自己的路径解析在用它，paimon 也主动往它归一，下沉只会变成三份定义）。
- 代码里已有的两段权威说明，动手前建议先读：`PluginDrivenMvccExternalTable.java:308-323`（空值标志与哨兵的分工，以及 fe-core 不做字符串比较的理由）、`PaimonConnectorMetadata.java:1245-1262`（paimon 为什么主动往这个哨兵归一）。
