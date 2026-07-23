# 类别 C — 分区粒度

**范围说明**：本类别聚焦 catalog SPI 迁移中「分区」相关的抽象错配，含四条发现：#4（Hive 分区值 unescape 缺失，🔴/休眠 P7）、#13（DDL `initialValues` 槽位对 LIST/RANGE 显式值恒空未接线，🟠/现行·已知 P5-7）、#14（`listPartitionValues` 无类型、paimon 返回原始 spec，🟠/被误标潜伏）与 #20（分区 transform 参数只建模为 `List<Integer>`，🟡/潜伏）。

**基线声明**：本核实基于**当前工作树** branch `catalog-spi-2-lvl-cache`。每条发现均由**独立逐条读取当前代码**（Read/Grep 当前工作树文件，而非 `git show` 旧分支或轻信原 survey）并叠加对抗复核（verify）二次校验得出。所有结论均标注当前工作树的 `file:line`。

---

## 总表

| # | 严重 | 原报告结论 | 核实结论 | 一句话 |
|---|------|-----------|---------|--------|
| 4 | 🔴 | Hive `parsePartitionName` 取 `substring(eq+1)` 无 unescape，escaped 分区值永不匹配 unescaped 谓词→剪光丢行 | **STALE_FIXED**（高置信） | 主张对几天前快照属实、缺陷真实且严重，但当前代码已由 #65473 完整修复（KEY+VALUE 双双 unescape），并加了单测 |
| 13 | 🟠 | DDL 有 `initialValues` 槽，但 LIST/RANGE 分区的显式值/边界被丢弃 | **PARTIAL**（高置信） | 机械事实成立（槽位在、LIST/RANGE 显式值/边界恒空未 lower），但「活跃 bug/中等严重」被高估——注释显式记录的有意分层、零消费者、需要的信息经 `hasExplicitPartitionValues` 无损保留并 fail-loud，属 vestigial 未接线槽位 |
| 14 | 🟠 | `listPartitionValues` 无类型：paimon 返回原始 spec（DATE=epoch-day、NULL 未归一），与 `getPartitionName()` 不一致，iceberg 无 override，休眠 | **PARTIAL**（高置信） | 技术事实全部成立，但「休眠」定性错误——同一 raw spec 缺陷经 `getNameToPartitionValues` 在 paimon `partition_values()` TVF 上活跃产错（DATE epoch-day 报错/错值、null 显 `__DEFAULT_PARTITION__` 而非 SQL NULL） |
| 20 | 🟡 | 分区 transform 参数在 SPI 只建模 `List<Integer>`，无法表达非整型参数 | **CONFIRMED**（高置信） | 类型上限真实存在，但纯潜伏——当前 Iceberg/Paimon/Hive 全部分区 transform 均无非整型参数，今天无可复现错误 |

---

## #4 Hive 分区值不 unescape

**核实结论**：**STALE_FIXED**（高置信度）。stage-1 与 verify 两阶段完全一致，均判定「曾真实存在、现已修复」。以此为准。

**原报告主张**：Hive `parsePartitionName` 对分区值只做 `part.substring(eq+1)`，缺 `FileUtils.unescapePathName`（legacy `HiveUtil` 有）；含 `/ 空格 : % 非ASCII` 的分区值经 HMS 返回时是 escaped 的，而谓词字面量是 unescaped 的，两者字符串比较永不相等→该分区被剪光→丢行。报告将其归为「与 hudi P3 同族」。

**核实过程**：
读当前工作树 `fe/fe-connector/fe-connector-hive/src/main/java/org/apache/doris/connector/hive/HiveConnectorMetadata.java:2311-2331`（`parsePartitionName` 完整方法）。当前代码按 `/` 切分、按第一个 `=` 分割后，第 2326-2327 行为：

```java
values.put(HiveWriteUtils.unescapePathName(part.substring(0, eq)),
        HiveWriteUtils.unescapePathName(part.substring(eq + 1)));
```

即对 **KEY 与 VALUE 双双** 调用 `HiveWriteUtils.unescapePathName`——与主张所指「裸 substring 无 unescape」正相反，主张描述的缺陷代码在当前工作树已不存在。第 2318-2325 行有详尽注释，明确说明 HMS `get_partition_names` 返回 Hive-escaped 名（如 `:`→`%3A`）、谓词侧（`extractLiteralValue`）是 unescaped，故 value 与 key 都必须 unescape，并点名镜像 legacy `FileUtils.unescapePathName`。

- 消费链路自洽：`matchesPredicates`（`HiveConnectorMetadata.java:2333-2344`）以 `actualValue = partValues.get(colName)` 与 `allowedValues.contains(actualValue)` 比较，消费的正是上面已 unescape 的 map。
- `unescapePathName` 实现在 `HiveWriteUtils.java:207-227`：逐字符扫，遇 `%` 且 `i+2 < len` 时 `Integer.parseInt(substr(i+1, i+3), 16)` 十六进制解码追加 char，`i += 2`；是 hive-common `FileUtils.unescapePathName` 的真实内联移植（边界 `i+2 < length` 对应 `substring i+3 <= length`，无 off-by-one）。兄弟路径 `toPartitionValues`（`HiveWriteUtils.java:176-196`）用同一 unescape。
- 回归保护：`HiveConnectorMetadataPartitionPruningTest.java:142` 的 `parsePartitionNameUnescapesValues`，第 145/154 行注释写明 `RED before the fix: US%3ACA`，即修复前正是本主张的失败场景，并覆盖了特殊字符分区**列名**的情形。

git 历史佐证（stage-1 查证）：历史 commit `5c325655b8b` 的该方法确为 `values.put(part.substring(0, eq), part.substring(eq + 1))`（纯 substring、无 unescape），与主张逐字吻合；缺陷已在 commit `3593684715f`（#65473 "Catalog spi 11 hive"）修复。报告标注「休眠(P7)」且快照早于该修复。

**为何已修（曾属实、现非问题）**：

修复前（历史版本），分区值 `US:CA` 在 HMS 中存为 `US%3ACA`，`parsePartitionName` 直接把 `US%3ACA` 放入 map；而谓词 `WHERE region = 'US:CA'` 经 `extractLiteralValue` 得到 unescaped 的 `US:CA`；`matchesPredicates` 里 `"US%3ACA".equals("US:CA")` 恒为 false→该分区被判定不匹配→**该分区全部行被静默丢弃**。主张的机理、严重度、「与 hudi P3 同族」判断均准确。

当前修复更比 legacy 完整：连**列名（KEY）**也 unescape，因此像 `pt2=x!!!! **1+1/&^%3` 这类特殊字符分区列名（`makePartName` 会把列名一起 escape）也能正确按真实列名查回，而 legacy 仅处理值。

**结论**：主张针对的是几天前快照，对应缺陷真实存在但已被 #65473 完整修复并加单测，当前工作树无此问题，**无需再动**。

---

## #13 initialValues LIST/RANGE 丢弃

**核实结论**：**PARTIAL**（高置信度）。stage-1 与 verify 两阶段完全一致，均判 PARTIAL——机械事实成立，但把它定性为「活跃 bug / 中等严重」被高估。以此为准。verify 亲读后仅纠正一处行号漂移（见下），不改结论。

**原报告主张**：DDL 有 `initialValues` 槽位，但 LIST/RANGE 分区的显式值（`VALUES IN`）/边界（`VALUES LESS THAN`）被丢弃。

**核实过程**（均读当前工作树）：

1. SPI 数据模型确有槽位：`ConnectorPartitionSpec.java:47-49` 有 `initialValues`（`List<ConnectorPartitionValueDef>`）+ `hasExplicitPartitionValues` 布尔位；getter/构造/`toString` 齐全（`:79-93`），但 `toString` 只输出 `initialValues.size()`。
2. 生产者（fe-core → SPI 转换）`CreateTableInfoToConnectorRequestConverter.java:135-149`：对 LIST/RANGE，`fields` 仍由分区列 `getPartitionList()` 生成（identity 字段），但 `initialValues` **恒传 `Collections.emptyList()`**（`:147-149`）；仅把「是否有显式分区定义」经 `info.getPartitionDefs()` 非空计入 `hasExplicitPartitionValues`。注释 `:138-146` 显式声明「不 lower 显式值，留 follow-up」——即恒空是被代码注释记录的有意分层决策，非疏漏。
3. **零消费者**：全仓 grep `getInitialValues()`，生产代码零调用，唯一调用者是 3 处 test（`CreateTableInfoToConnectorRequestConverterTest.java:243/291/308`）。所有连接器建表分区都从 `getFields()`（列名+transform）构造，不依赖 `initialValues`，故「丢弃」不产生任何下游错误结果。
4. 唯一生产消费者是 Hive，且用的是标志位而非 `initialValues`：`HiveConnectorMetadata.java:1614`（注释）/`1620`（RANGE 直接抛 `Only support 'LIST' partition type in hive catalog.`）/`1626`（`hasExplicitPartitionValues()` 时抛 `Partition values expressions is not supported`）。**行号纠正**：stage-1 写 Hive 块为 1620-1629，verify 亲读实际起于 1614（注释）/1620（RANGE reject）/1626（explicit reject），不影响结论。
5. iceberg/paimon 建表分区只读 `getFields()`：`IcebergSchemaBuilder.buildPartitionSpec:159-198`、`PaimonSchemaBuilder.partitionKeys:133-150`，走隐藏/identity 分区，不看 `initialValues`。

**背景**：SPI 层 `ConnectorPartitionSpec` 为 LIST/RANGE 分区预留了 `initialValues` 槽（语义上应承载 `VALUES IN` / `VALUES LESS THAN` 的显式值/边界）与 `hasExplicitPartitionValues` 标志位。converter 只填了标志位、`initialValues` 恒空，且代码注释显式声明把「lower 显式值」留到后续。这是一个「已接线一半」的 vestigial 槽位：字段在、语义应承载 LIST/RANGE 值、但从未被 populate 也从未被读。

**影响（机械事实属实、但当前无害）**：

`initialValues` 的显式值/边界确实恒空未 lower（机械事实成立），但这不导致任何错误结果，原因是需要的信息已被无损保留并 fail-loud：

- 真正需要感知「用户写了显式分区定义」的是 Hive 外表（它从数据布局发现分区，须拒绝显式值），这一位经 `hasExplicitPartitionValues` 无损传下去。具体行为：
  - `PARTITION BY RANGE(dt)(PARTITION p1 VALUES LESS THAN('2024'))` 在 Hive catalog：不会「静默丢边界建错表」，而是抛 `Only support 'LIST' partition type in hive catalog.`——正确拒绝（`HiveConnectorMetadata.java:1620`）。
  - LIST + 显式 `VALUES IN` 在 Hive：抛 `Partition values expressions is not supported in hive catalog.`——正确拒绝（`:1626`）。
- iceberg/paimon 用隐藏/identity 分区，Doris 原生 LIST/RANGE 的预建分区边界在这些引擎里本无对应语义，不 lower 反而正确；分区列名本身经 `fields` 传递也没丢。

故本条属报告标注的「现行·已知（P5-7）」范畴，代码注释已自认待后续，**严重度（🟠 中等）被高估**——它是 vestigial 未接线槽位，而非导致错误结果的活跃缺陷。

**修复方案**：

当前无需紧急修复，不影响正确性。若要收敛抽象错配，可选：
1. 后续真需要外表预建 LIST/RANGE 分区时，在 `CreateTableInfoToConnectorRequestConverter.convertPartition`（`:147-149`）补上 `initialValues` 的 lowering，并让对应连接器消费 `getInitialValues()`；
2. 否则直接删除 vestigial 的 `initialValues` / `ConnectorPartitionValueDef` 槽位以消除 SPI 抽象错配（`hasExplicitPartitionValues` 标志位保留即可满足 Hive 的 fail-loud 需求）。

---

## #14 listPartitionValues 无类型

**核实结论**：**PARTIAL**（高置信度）。stage-1 与 verify 两阶段一致：技术事实全部成立，但「休眠」定性错误——同一 raw spec 缺陷经**另一条方法**在 `partition_values()` TVF 上是活的、且产错结果。以此为准。

**原报告主张**：`listPartitionValues` 返回 `List<List<String>>` 无类型：paimon 返回原始 `partition.spec()` map（DATE=epoch-day `19723`、NULL=未归一 `__DEFAULT_PARTITION__`），与 `getPartitionName()`（格式化日期、归一 null）不一致；iceberg 无 override。休眠（TVF 还走 legacy HMS，零 fe-core 消费者）。

**核实过程**（均读当前工作树）：

1. SPI 默认退化：`ConnectorTableOps.listPartitionValues` 默认返回 `emptyList`（`ConnectorTableOps.java:412`）。iceberg **无** override（退化空表，确认）。
2. paimon override：`PaimonConnectorMetadata.java:1038-1053`，逐列读 `partition.getPartitionValues()`。而 `ConnectorPartitionInfo.getPartitionValues()`（`ConnectorPartitionInfo.java:148-149`）返回的是 **RAW** paimon spec map——构造点 `PaimonConnectorMetadata.java:1157-1159`，第 2 构造参数即 raw spec（注释写 RAW spec un-rendered），rendered 的 `orderedValues` 是另传的第 3 参数。
3. 两套值的分叉：`collectPartitions` 把每个分区拆成 rendered 的 `orderedValues`（NULL 归一为 `__HIVE_DEFAULT_PARTITION__`、DATE 经 SDK `formatDate` 格式化，`:1138/1144`）与 raw 的 `partition.spec()`（DATE 为 epoch-day 字符串 `19723`、NULL 为 paimon 原生 `__DEFAULT_PARTITION__`，`defaultPartitionName` 定义于 `:1101-1102`）。`listPartitionValues`（`:1043`）读的正是 raw map，故与 `getPartitionName()` / `getOrderedPartitionValues()` 不一致——技术事实全部成立。
4. **「休眠」定性错误**：SPI `listPartitionValues` 确无 fe-core 生产调用者（仅 test：`FakeConnectorPluginTest`，verify 另补 `HudiConnectorPartitionListingTest.java:180`），但**同一个 raw `getPartitionValues()`** 被一条活跃路径读取：`partition_values()` TVF → `PluginDrivenExternalTable.getNameToPartitionValues`（`:870-897`，`:892` 读同一 raw map）→ `MetadataGenerator.java:2099`。paimon 表走 `PLUGIN_EXTERNAL_TABLE` 分支（`:2076`），**不走 legacy HMS**。载体是 `listPartitions`（`PluginDrivenExternalTable.java:887/892`），非 `listPartitionValues`。

**背景**：paimon 的 `collectPartitions` 对每个分区同时产出 rendered `orderedValues`（已格式化日期 + 归一 null）与 raw `partition.spec()`（epoch-day + paimon 原生 default 常量）。`ConnectorPartitionInfo.getPartitionValues()` 暴露的是 raw 那一套。凡按名索引下游读了 raw map，就与 `getPartitionName()` 的渲染口径分叉。

**影响（报告低估处：活跃 TVF 产错结果）**：

raw `getPartitionValues()` 并非只被休眠的 SPI `listPartitionValues` 用，`partition_values()` TVF 经 `getNameToPartitionValues` 命中同一 raw map，产错：

- **DATE 分区列**：raw `19723` 进 DATE 分支 `convertStringToDateV2`（`MetadataGenerator.java:2158-2161`），把 epoch-day 当日期字符串解析→报错或错值（正确应为 `2024-01-01`）。
- **NULL 分区**：raw `__DEFAULT_PARTITION__` 与归一常量 `TablePartitionValues.HIVE_DEFAULT_PARTITION`（`__HIVE_DEFAULT_PARTITION__`，`:2131`）不相等→不被判为 NULL→渲染成字面量而非 SQL NULL。

具体示例：paimon 分区表 `dt` 为 DATE 且 legacy-name 默认 true，`SELECT * FROM partition_values(...)` 时 `dt` 列报错或错值；含 null 分区则显示 `__DEFAULT_PARTITION__` 而非 SQL NULL。故「raw 无类型不一致」判断正确，但「潜伏零消费者」定性错误——活跃 TVF 命中同一缺陷，严重度被低估，判 PARTIAL。

（verify 两点补记，不改结论：(1) SPI `listPartitionValues` 除 `FakeConnectorPluginTest` 外还有 `HudiConnectorPartitionListingTest.java:180` 一个 test 调用者，零 fe-core 生产调用者的结论不变。(2) iceberg 也 override 了 `listPartitions`（`IcebergConnectorMetadata.java:1625`），故 iceberg 同样喂 `getNameToPartitionValues`——但其 `ConnectorPartitionInfo` 值渲染另论，超出 #14（paimon raw spec）范围，仅记备。）

**修复方案**：

真正该修的是活跃的 `getNameToPartitionValues`，而不仅是休眠的 `listPartitionValues`。可选：
1. 按名索引的下游改用 `getOrderedPartitionValues()`（已 rendered + 归一），即 `getNameToPartitionValues` 与 `listPartitionValues` 都不再直接读 raw `partition.spec()`；
2. 若保留 raw map 键名索引，则须在渲染点补 DATE 格式化与 null 归一，使 raw 值与 TVF 下游（`convertStringToDateV2`、`HIVE_DEFAULT_PARTITION` 判定）口径对齐。

---

## #20 transform 参数只 List<Integer>

**核实结论**：**CONFIRMED**（高置信度）。stage-1 与 verify 一致确认「类型上限真实、纯潜伏、今天无可复现错误」。verify 在同意的前提下补充了两处细化（见下），不改结论。

**原报告主张**：分区 transform 参数在 SPI 里只建模为 `List<Integer>`，无法表达任意 transform 参数（如非整型参数）。

**核实过程**（均读当前工作树）：

1. SPI 数据模型固定为整型：`ConnectorPartitionField.java:38` `private final List<Integer> transformArgs;`；构造器（`:40-47`）、getter（`:57-59`）、`equals/hashCode/toString` 全部围绕 `List<Integer>`；类 javadoc（`:31-32`）明写 "carries numeric parameters"。
2. ALTER 路径更严：`PartitionFieldChange.java:44` `private final Integer transformArg;`（**单个整数**，非 list），旧值字段同样是 `Integer`；javadoc（`:36`）说明只承载 bucket/truncate 宽度。
3. 生产者（fe-core → SPI 转换）`CreateTableInfoToConnectorRequestConverter.java:178-199`（实际路径为 `fe/fe-core/src/main/java/org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverter.java`，非 JSON 提示的 `datasource/connector/converter/`）：`List<Integer> args`（`:182`）；收集分支——`IntegerLikeLiteral` 走 `getIntValue()`（`:186-187`）；其它 `Literal` **仅当** `getValue() instanceof Number` 才 `intValue()` 入列（`:190-191`）。非 Number 字面量（如 `StringLiteral`，本身是 Literal 但 value 是 String）进入 `:188` 的 else-if 后，内层 `:190` 判 false→无 else→**什么都不做→静默丢弃**。`convertFields`（`:161-176`）对 `UnboundSlot` 走 identity 空 args，未知表达式静默丢。
4. 消费者：`IcebergSchemaBuilder` 中只有 `bucket`（`:176`）与 `truncate`（`:197`）用到 args，经 `intArg`（`:206-212`）取 `args.get(0)` 当 int；`identity/year/month/day/hour` 均无参；未知 transform 直接抛异常（`:200`）。全连接器 grep `getTransformArgs/getTransformArg`：唯一消费者是 Iceberg——`IcebergSchemaBuilder`（CREATE）与 `IcebergCatalogOps.java:585/603/621`（ALTER，经 `getTransform(name, col, Integer arg)`）；Paimon/Hive 零消费，佐证其分区列 identity-only。

**背景**：SPI 用 `ConnectorPartitionField.transformArgs`（`List<Integer>`）承载分区 transform 参数，ALTER 场景（`PartitionFieldChange.transformArg`）甚至只是单个 `Integer`。converter 从 Nereids `UnboundFunction` 抽参时只把整型字面量塞进 args，凡非 Number 的字面量被静默丢弃。

**影响（真实但纯潜伏，当前无可复现错误案例）**：

遍历当前支持连接器的全部分区 transform，没有任何一个接受非整型参数：
- Iceberg 分区 transform 全集 = `identity / bucket[N] / truncate[W] / year / month / day / hour / void`，唯二带参的 `bucket`、`truncate` 都是整型宽度，`IcebergSchemaBuilder.java:176/197` 正是按 int 消费，`intArg`（`:206`）取 `args.get(0)`。
- Paimon/Hive 的分区列均为 identity，无参。

因此对今天所有真实 DDL（如 `PARTITIONED BY (bucket(16, id), truncate(4, name), day(ts))`），`List<Integer>` 完全够用，不会产生错误结果。真正暴露面只在未来引入「带非整型参数的分区 transform」时才出现，届时具体错误路径有两类：

- **非整型（字符串）参数被静默丢弃**：假设未来支持 `truncate('prefix', col)` 这类字符串参数 transform，输入 `PARTITIONED BY (truncate('prefix', name))`→converter 在 `:190` 判 `'prefix'` 非 Number→丢弃该参数→连接器侧拿到空 args→`IcebergSchemaBuilder.java:207-210` 抛 "requires an integer argument"，或建出参数丢失的错误分区规格。
- **小数参数被无声截断为 int**（verify 比 stage-1 更进一步发现的盲区）：即便是数值型但非整数的参数（`DecimalLiteral/DoubleLiteral`）也不会被丢弃，而是经 `:191` `((Number) v).intValue()` 被静默截断——例如 `truncate(4.9, col)`→args 变成 `4`。故上限不止「非整型丢失」，还含「小数参数被无声截断」。当前 Iceberg/Paimon/Hive 无小数参数 transform，同样不触发，仍属潜伏。

（verify 另一处措辞细化：非整型字面量的丢弃点精确说是 `:190` 内层 `if (v instanceof Number)` 判 false 后无 else，它确实进入了 `:188` 的 else-if 分支、只是分支内不处理；效果等同静默丢弃，不影响 stage-1 结论。）

**修复方案**：

现状无需改动（无活跃缺陷，遵循 YAGNI，不提前抽象）。若将来确有非整型参数 transform 需求，最小改动清单：
1. 把 `transformArgs` 从 `List<Integer>` 放宽为 `List<ConnectorLiteral>`（或 `List<Object>/List<String>`）。
2. 放宽 `CreateTableInfoToConnectorRequestConverter.convertTransformField` 的字面量收集（`:188-192`），不再静默丢弃非 Number、不再无声截断小数。
3. `PartitionFieldChange.transformArg`（`:44`）一并放宽。
4. 各连接器 SchemaBuilder 的 `intArg` 保持向后兼容（整型 transform 仍按 int 解析）。
5. **（verify 补，stage-1 遗漏）** Iceberg ALTER 三个消费入口 `IcebergCatalogOps.java:585/603/621`（经 `getTransform` 消费 `Integer transformArg`）也要同步放宽——stage-1 修复方案仅提了 SchemaBuilder 的 CREATE 侧。

---

## 本类别小结

- **真伪分布**：本类别四条发现，无一是凭空伪报——但性质各异。#4 是「曾真、现已修」（STALE_FIXED）：报告基于几天前快照，缺陷真实且严重（🔴 丢行），但当前工作树已由 #65473 完整修复并加单测，无需再动。#13 是「机械事实真、定性高估」（PARTIAL）：`initialValues` 对 LIST/RANGE 显式值恒空未 lower 属实，但这是注释显式记录的有意分层、零消费者、需要的信息经 `hasExplicitPartitionValues` 无损保留并 fail-loud，属 vestigial 未接线槽位而非活跃缺陷，🟠 被高估。#14 是「技术事实真、休眠定性错」（PARTIAL）：paimon 的 raw spec 无类型不一致属实，但同一缺陷经 `getNameToPartitionValues` 在 `partition_values()` TVF 上活跃产错（DATE epoch-day 报错/错值、null 显 `__DEFAULT_PARTITION__` 而非 SQL NULL），并非休眠，严重度被低估。#20 是「真、但纯潜伏」（CONFIRMED）：类型层面的表达力上限确实存在，但当前三个连接器（Iceberg/Paimon/Hive）的所有分区 transform 均无非整型/小数参数，今天没有任何可复现的错误结果。
- **共性根因**：四条都源于「SPI 数据模型对分区语义做了窄化/不完整假设」。#4 假设 HMS 返回的分区名与谓词字面量在**同一 escape 层**（忽略 HMS 的 Hive-escape）；#13 抽象槽位只接线一半（标志位接了、显式值 lowering 留待后续）；#14 用 raw spec 而非 rendered 值做按名索引（忽略 DATE/null 的渲染口径）；#20 假设 transform 参数恒为整型（忽略未来非整型/小数参数）。#4 已随迁移成熟被修正；#14 有活跃产错点需真修（改 `getNameToPartitionValues` 用 rendered 值）；#13、#20 则因当前需求边界内够用而保留为已知（vestigial / 潜伏上限）。
- **与其他类别关联**：#4 报告自身点名「与 hudi P3 同族」——同属「分区值/名的 escape/unescape 一致性」问题族，应与 hudi 侧 P3 发现交叉核对是否同样已修；#14 的 raw-vs-rendered 分歧（DATE epoch-day、`__DEFAULT_PARTITION__` 未归一）与该族同源，是「分区值渲染口径一致性」的姊妹问题。#13、#20 属 DDL/schema 建模的表达力边界（「全部DDL」类），与其他 S 级潜伏项同属「先落地够用、留待需求驱动放宽」的一类，不建议提前抽象；其中 #13 的 vestigial 槽位可择机删除以消除 SPI 抽象错配。
