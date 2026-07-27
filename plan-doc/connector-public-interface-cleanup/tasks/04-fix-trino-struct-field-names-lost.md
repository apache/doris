# 04. 修复 trino 连接器丢弃复杂类型字段名导致引擎编造 col0 / col1

> **优先级**：第一优先级（正确性缺陷，用户拿不到真实字段名、按名访问子字段直接报错） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe/fe-connector/fe-connector-trino`（主修 + 单元测试）、`fe/fe-connector/fe-connector-api`（`ConnectorType` 加构造期校验与契约文档 + 新增单元测试）。**不动** `fe-core`。
> **预计改动规模**：生产代码 2 个文件，净增约 45 行；测试 2 个文件（1 个新建），约 130 行。可选的端到端用例 1 个 groovy 文件，约 40 行。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

trino 连接器把 Trino 的 ROW 类型翻译成 Doris 类型时只带了每个字段的**类型**、丢掉了每个字段的**名字**，引擎侧拿到一个「有子类型、没字段名」的 STRUCT 后不报错，而是按下标编造 `col0` / `col1` 顶上去；用户于是在 `DESCRIBE` 里看到假名字，并且**没有任何办法按真实字段名访问子字段**。本任务一方面把 trino 侧的字段名带上，另一方面在公共接口 `ConnectorType` 的构造期把「字段名列表必须与子类型列表等长同序」这个到今天为止既无文档、也无校验的不变量变成硬约束，让下一个连接器无法再犯同样的错。

## 二、背景：现在的代码是怎么写的

### 2.1 类型是怎么从连接器流到引擎的

连接器不认识 Doris 的 `Type`，它只用公共接口里的 `ConnectorType`（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorType.java`）描述一个列的类型；引擎侧再由 `fe/fe-core/src/main/java/org/apache/doris/datasource/connector/converter/ConnectorColumnConverter.java` 把它翻成 Doris 的 `Type`。

`ConnectorType` 描述复杂类型的方式是**多条平行列表**（`:54-84`）：`children` 存子类型，`fieldNames` 存 STRUCT 的字段名，另外还有三组可选的按子元素元数据（`childrenNullable` / `childrenComments` / `childrenFieldIds` / `childrenCommentSpecified`）。类里一共 7 个公开构造器（`:86-148`）——6 个便捷构造器（`:86`、`:91`、`:96`、`:102`、`:108`、`:115`）加 1 个规范构造器（`:123`），便捷构造器全部层层委派到最长的那个规范构造器（`:123-148`），而**这个规范构造器只做了 `typeName` 的非空检查和不可变包装，对任何一条列表的长度都不做校验**。

工厂方法是给这些平行列表兜底的（`:162-215`）：`arrayOf` 保证 1 个子类型、`mapOf` 保证 2 个、`structOf(names, fieldTypes, ...)` 强迫调用方同时给出名字和类型。除 trino 与 es 之外的连接器都走工厂：`HmsTypeMapping.java:150`、`HudiTypeMapping.java:220`、`IcebergTypeMapping.java:89`、`MCTypeMapping.java:135`、`PaimonTypeMapping.java:200`，全部是 `structOf(names, types, ...)`，且 names 与 types 在同一个循环里成对填充。es 只用裸构造器包 ARRAY，且传的是 `Collections.singletonList(type)`（`EsTypeMapping.java:182-183`），个数恰好正确。

### 2.2 出问题的地方：trino 侧丢名

`fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoTypeMapping.java:107-112`：

```java
} else if (type instanceof RowType) {
    List<ConnectorType> children = new ArrayList<>();
    for (RowType.Field field : ((RowType) type).getFields()) {
        children.add(toConnectorType(field.getType()));
    }
    return new ConnectorType("STRUCT", -1, -1, children);
}
```

循环里只 `add(field.getType())`，`field.getName()` 从头到尾没被读过（Trino 435 的 `RowType.Field` 提供 `Optional<String> getName()`，trino 版本见 `fe/pom.xml:416`）；返回时用的是 4 参数裸构造器，`fieldNames` 槽位空缺。同一方法里相邻的 ARRAY（`:92-97`）与 MAP（`:98-106`）分支也用裸构造器，只是子类型个数恰好对，没有暴露出问题。

这个映射有两个消费点，都在 `TrinoConnectorDorisMetadata.java`：`:213` 是表结构（`DESCRIBE` / 查询分析看到的列类型），`:366` 是投影下推时回报给引擎的表达式类型。

### 2.3 引擎侧的宽容：缺名就编造

`ConnectorColumnConverter.java:258-273`：

```java
private static Type convertStructType(ConnectorType ct) {
    List<ConnectorType> children = ct.getChildren();
    List<String> fieldNames = ct.getFieldNames();
    ArrayList<StructField> fields = new ArrayList<>();
    for (int i = 0; i < children.size(); i++) {
        String fieldName = i < fieldNames.size() ? fieldNames.get(i) : "col" + i;
        ...
```

`:263` 这一行就是「编造」：名字缺了就用 `col` + 下标，**不告警、不抛错**。同一个文件里 ARRAY 与 MAP 也是同样的宽容风格：子类型列表为空时 `convertArrayType`（`:242-248`）返回 `ARRAY<NULL>`、`convertMapType`（`:250-256`）在子类型不足 2 个时返回 `MAP<NULL,NULL>`，一样没有任何提示。

同类的「缺名就编造」兜底在反方向（`ConnectorType` → 数据源类型）的连接器代码里还有四处：`HmsTypeMapping.java:239`、`IcebergSchemaBuilder.java:137`、`PaimonTypeMapping.java:289`、`MCTypeMapping.java:212`。也就是说这套「平行列表可以对不齐、对不齐就猜」的风格已经在五个地方复制过。

### 2.4 两条补充事实

- **这是迁移引入的回退，不是历史一直如此。** 迁移前 fe-core 里的老实现 `TrinoConnectorExternalTable.trinoConnectorTypeToDorisType()`（`git show master:fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/TrinoConnectorExternalTable.java`，ROW 分支）明确判断 `field.getName().isPresent()`，有名字就 `new StructField(name, childType)`，没名字才退化。所以真实字段名在迁移前是能被用户看到的。顺带一个细节：老实现对匿名字段用的是 `new StructField(childType)`，而 `fe/fe-type/src/main/java/org/apache/doris/catalog/StructField.java:46,69-70` 把这种字段一律命名为 `"col"`——多个匿名字段会重名，这本身是老实现的缺陷。
- **现有单元测试断不出这个缺陷。** `fe/fe-connector/fe-connector-trino/src/test/java/org/apache/doris/connector/trino/TrinoTypeMappingTest.java:123-133` 的 `testStructCarriesFieldTypes` 用 `RowType.field("a", INTEGER)` / `RowType.field("b", VARCHAR)` 造了带名字的 ROW，却只断言 `getChildren()` 的两个子类型名，从不看 `getFieldNames()`——名字丢没丢它都是绿的。

## 三、为什么这是个问题

**用户能观察到的后果（正确性）**：Doris 的 STRUCT 子字段访问是**按名字**解析的。`fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar/ElementAt.java:142-147`（struct 字段访问的类型推导）在按名字找不到字段时抛 `AnalysisException: the specified field name <名字> was not found: ...`。字段名一旦被换成 `col0` / `col1`，用户对着自己在 Trino / Hive 里定义的字段名写查询，一律在分析期就被拒；`DESCRIBE` 也只会显示编造出来的名字，用户连"真名是什么"都查不到。唯一还能用的访问方式是按序号（`s[1]`），而这恰好掩盖了问题——冒烟测试如果只用序号访问，看起来一切正常。

**公共接口层违反的设计原则**：

1. **平行列表的对应关系是不变量，却既没有契约也没有校验。** `ConnectorType` 的类文档（`:25-53`）花了大段篇幅说明 `childrenNullable` / `childrenComments` / `childrenFieldIds` 三组可选元数据「parallel to children」以及为什么它们不参与 `equals`，但**对 `fieldNames` 与 `children` 的对应关系一个字都没写**，`getFieldNames()`（`:245-247`）甚至没有 javadoc；7 个构造器（6 个便捷 + 1 个规范，`:86-148`）也没有一处校验长度。于是"名字和类型必须等长同序"完全靠调用方自觉。
2. **违约被静默吸收，而不是 fail loud。** 一个连接器写错了，代码能编译、表能加载、`DESCRIBE` 有输出，错误一路潜行到用户写查询的那一刻才以「字段不存在」的形式冒出来，而那时报错现场已经离真正的错误点（连接器的类型映射）很远了。
3. **同一个坑对 ARRAY / MAP 一样敞开。** ARRAY 需要 1 个子类型、MAP 需要 2 个，公共接口同样不校验，引擎同样静默产出 `ARRAY<NULL>` / `MAP<NULL,NULL>`（`ConnectorColumnConverter.java:242-256`）。今天没人踩只是因为工厂方法恰好被大多数连接器用了。

另外值得注意的一点：`fieldNames` 是**参与** `equals` / `hashCode` 的（`ConnectorType.java:313-333`），也就是说字段名在这个类的设计里属于「类型的结构身份」，不是可有可无的附加元数据。丢名不是"少带了点信息"，是构造出了一个身份就不对的类型。

## 四、用一个最小例子说明

假设 Trino 侧（比如 trino-connector 挂 hive）有这么一张表：

```sql
-- 数据源侧的表定义
CREATE TABLE t (id int, s row(a int, b varchar));
```

在 Doris 里通过 trino 连接器查它：

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
| --- | --- | --- |
| `DESC t;` | `s` 显示为 `struct<col0:int,col1:text>` | `s` 显示为 `struct<a:int,b:text>` |
| `SELECT s.a FROM t;` | 分析期报错 `the specified field name a was not found` | 正常返回 `a` 列的值 |
| `SELECT s['b'] FROM t;` | 同上，按名访问全军覆没 | 正常返回 |
| `SELECT s[1] FROM t;` | 恰好能跑（按序号访问） | 恰好能跑（行为不变） |

公共接口这一侧的问题，用两行就能说明白：

```java
// 今天：编译通过、构造成功、什么都不报，直到用户按名字查子字段才炸
new ConnectorType("STRUCT", -1, -1, Arrays.asList(intType, strType));           // 名字全丢
new ConnectorType("STRUCT", -1, -1, Arrays.asList(intType, strType), List.of("a"));  // 名字只给一半

// 期望：上面两行都在构造点立刻抛 IllegalArgumentException
```

## 五、解决方案

### 5.1 目标状态

**(a) trino 侧把名字带上，并改用工厂方法。** `TrinoTypeMapping.toConnectorType` 的 ROW 分支改成：

```java
} else if (type instanceof RowType) {
    List<RowType.Field> rowFields = ((RowType) type).getFields();
    List<String> names = new ArrayList<>(rowFields.size());
    List<ConnectorType> types = new ArrayList<>(rowFields.size());
    for (int i = 0; i < rowFields.size(); i++) {
        RowType.Field field = rowFields.get(i);
        // Trino ROW fields may be anonymous (RowType.anonymousRow); name them by position so that
        // every field still gets a distinct, resolvable name.
        names.add(field.getName().orElse("col" + i));
        types.add(toConnectorType(field.getType()));
    }
    return ConnectorType.structOf(names, types);
}
```

匿名字段的处理要写清楚：**用 `col` + 下标，而不是复刻老实现给所有匿名字段同名 `"col"` 的做法**。理由是重名字段在 Doris 侧一样无法按名访问，属于老实现的缺陷；`col` + 下标与引擎兜底（`ConnectorColumnConverter.java:263`）以及其它四个连接器的反向兜底命名完全一致，是本仓库既有约定。这是本任务唯一一处有意偏离迁移前行为的地方。

ARRAY / MAP 两个分支顺带改成 `ConnectorType.arrayOf(...)` / `ConnectorType.mapOf(...)`（等价改写，只为让「复杂类型一律走工厂」在这个文件里成为一眼可见的事实）。

**(b) 公共接口加构造期校验。** 在 `ConnectorType` 的规范构造器（`:123-148`）末尾调用一个新的私有静态方法，签名草案：

```java
/**
 * Fail loud on a malformed complex type: the parallel lists carried alongside {@link #getChildren()}
 * must line up with it, and the three complex type tags have a fixed arity.
 */
private static void validateShape(String typeName, List<ConnectorType> children, List<String> fieldNames,
        List<Boolean> childrenNullable, List<String> childrenComments,
        List<Integer> childrenFieldIds, List<Boolean> childrenCommentSpecified)
```

校验规则（`typeName` 用 `toUpperCase(Locale.ROOT)` 比对，与 `ConnectorColumnConverter.convertType` 的 `:229` 一致，避免 `"Struct"` 这种拼写绕过校验）：

| 类型标签 | 规则 |
| --- | --- |
| `ARRAY` | `children.size() == 1` |
| `MAP` | `children.size() == 2` |
| `STRUCT` | `children` 非空；`fieldNames.size() == children.size()`；`fieldNames` 中不含 `null` |
| 以上三者 | 四组可选元数据列表（nullable / comments / fieldIds / commentSpecified）每一条要么为空（表示未携带），要么长度恰好等于 `children.size()`；比 `children` 长一定是调用方错了 |
| 其它标签 | **不校验**。`typeName` 是无词表的裸字符串，不能反过来断言「非复杂类型一定没有子类型」 |

异常一律用 `IllegalArgumentException`（与同一构造器里 `Objects.requireNonNull` 的失败风格一致），消息里带上 `typeName` 和实际长度，例如 `STRUCT field name count (1) must match child type count (2)`。因为所有构造器与工厂方法都汇聚到这个规范构造器，一处落地即全覆盖。

**(c) 把契约写进类文档。** 在 `ConnectorType` 类 javadoc（`:25-53`）里补一段说明：`fieldNames` 与 `children` 是等长同序的平行列表、STRUCT 必须携带全部字段名、三个复杂类型标签的子类型个数固定、四组可选元数据要么不带要么带全，并注明这些在构造期强制。同时给 `getFieldNames()`（`:245-247`）补一行 javadoc 指向该契约。

**实施顺序**：(a) 必须与 (b) 在同一次改动里落地，且先改 trino——否则先加校验会让 trino 的表加载与现有 `TrinoTypeMappingTest` 立刻抛异常。

### 5.2 改动清单

| 文件 | 做什么 |
| --- | --- |
| `fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoTypeMapping.java`（ROW 分支 `:107-112`，ARRAY `:92-97`，MAP `:98-106`） | ROW 分支收集字段名并改用 `ConnectorType.structOf(names, types)`，匿名字段用 `col` + 下标；ARRAY / MAP 改用 `arrayOf` / `mapOf`。加注释说明匿名字段命名的来由 |
| `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorType.java`（规范构造器 `:123-148`、类 javadoc `:25-53`、`getFieldNames()` `:245-247`） | 新增私有 `validateShape(...)` 并在规范构造器末尾调用；补齐平行列表契约文档 |
| `fe/fe-connector/fe-connector-trino/src/test/java/org/apache/doris/connector/trino/TrinoTypeMappingTest.java`（`:123-133`） | 扩写 `testStructCarriesFieldTypes` 断言字段名；新增匿名 ROW、嵌套 ROW 两个用例 |
| `fe/fe-connector/fe-connector-api/src/test/java/org/apache/doris/connector/api/ConnectorTypeTest.java`（新建） | 覆盖 5.1(b) 的每条校验规则，正反两向 |
| `regression-test/suites/external_table_p0/trino_connector/hive/`（可选） | 补一个 STRUCT 列的端到端用例，见第六节 |

### 5.3 明确不要顺手做的事

- **不要动 `equals` / `hashCode`**（`:313-333`）。三组按子元素元数据被有意排除在类型身份之外，类文档 `:39-46` 已写明理由（类型身份等于结构形状，可空性与注释由消费方逐字段单独比较），改它会波及所有基于类型相等的模式变更检测。
- **不要动 fe-core 的兜底分支**（`ConnectorColumnConverter.java:263` 的 `col` + 下标、`:242-256` 的 `ARRAY<NULL>` / `MAP<NULL,NULL>`）。三个理由：本阶段 fe-core 只出不进；构造期校验落地后这些分支已经不可能被合法构造的 `ConnectorType` 触发，留着当防御；把它们改成抛异常会把「某一列显示退化」升级成「整张表加载失败」，风险大于收益。
- **不要顺手清理另外四处反方向的缺名兜底**（`HmsTypeMapping.java:239`、`IcebergSchemaBuilder.java:137`、`PaimonTypeMapping.java:289`、`MCTypeMapping.java:212`）。它们在 `ConnectorType` → 数据源类型的方向上，输入来自 fe-core 的转换器，与本缺陷不是同一条路径；逐一改属于扩大范围。
- **不要给 ARRAY / MAP 发明 `fieldNames` 语义**。今天没有任何生产者给它们传名字，校验里也只要求「STRUCT 必须带全名字」，不去规定 ARRAY / MAP 必须为空——留出余地，但不主动定义新语义。
- **不要把这 7 个构造器重构成 builder**，也不要给 `ConnectorType` 加新的工厂重载。校验落在唯一的规范构造器上就够了，重构会波及全部连接器。
- **不要写 shell / 正则静态门禁**去检查「有没有人用裸构造器造 STRUCT」。本仓库已有结论：这类门禁只适合存在性与前缀类不变量，语言语义交给构造期校验加单元测试。

## 六、怎么验证

### 单元测试要断言什么

`TrinoTypeMappingTest`（扩写与新增）：

1. **带名 ROW**：`RowType.rowType(RowType.field("a", INTEGER), RowType.field("b", VARCHAR))` 转换后 `getFieldNames()` 必须等于 `["a", "b"]`，且与 `getChildren()` 等长同序。**改代码前先跑这条，它必须是红的**——这是本任务的变异验证要求：如果它在旧代码上就绿，说明用例没打到缺陷。
2. **匿名 ROW**：`RowType.anonymousRow(INTEGER, VARCHAR)`（Trino 435 提供）转换后名字为 `["col0", "col1"]`，锁住匿名字段各自拿到互不相同的名字这一决定。
3. **嵌套**：`row(a int, b row(c int))` 转换后内层 STRUCT 的字段名也必须是 `["c"]`，证明递归路径同样带名。
4. 现有的 ARRAY / MAP 用例（`:105-121`）保持不改，作为改用工厂方法后行为未变的基线。

新建 `ConnectorTypeTest`（`fe-connector-api`）：

1. STRUCT 名字数少于子类型数、多于子类型数，两向都抛 `IllegalArgumentException`；断言消息里同时出现两个长度（否则报错对定位没帮助）。
2. STRUCT 子类型列表为空、`fieldNames` 含 `null` 元素，抛异常。
3. ARRAY 传 0 个或 2 个子类型、MAP 传 1 个或 3 个，抛异常。
4. 任一组可选元数据列表比 `children` 长时抛异常；**为空时必须仍然合法**（`HudiTypeMapping` / `MCTypeMapping` 的 `structOf(names, types)` 就走这条路，`ConnectorColumnConverterTest.java:446-453` 也依赖它）。
5. 大小写不敏感：`new ConnectorType("struct", -1, -1, children)`（无名字）同样抛异常，证明校验不能被拼写绕过。
6. 合法路径全部不抛：`structOf` 的三个重载、`arrayOf` 两个重载、`mapOf` 两个重载，以及 `withChildrenFieldIds` 在 ARRAY（1 个 id）/ MAP（2 个 id）/ STRUCT（N 个 id）上的调用——后者对应 `IcebergTypeMapping.java:66-67,75-76,89` 的真实用法。
7. 非复杂类型标签不受影响：`ConnectorType.of("JSONB")`、`of("DECIMALV3", 10, 2)` 正常。

### 现成的回归网

新校验最有价值的副作用是：所有构造复杂类型的既有测试都会变成它的回归网。这几个必须仍然全绿——`fe-core` 的 `ConnectorColumnConverterTest`（大量 `structOf` / `arrayOf` / `mapOf` + `withChildrenFieldIds`），以及 `HmsTypeMappingTest`、`HudiTypeMappingTest`、`HudiSchemaParityTest`、`IcebergTypeMappingReadTest`、`IcebergSchemaBuilderTest`、`IcebergNestedColumnEvolutionTest`、`MCTypeMappingTest`、`PaimonTypeMappingReadTest`、`PaimonTypeMappingToPaimonTest`、`HiveConnectorMetadataSchemaTest`。

### 命令

单模块测试（**必须禁用 maven build cache**，否则 surefire 会被静默跳过而报 BUILD SUCCESS）：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
    -pl fe-connector/fe-connector-api,fe-connector/fe-connector-trino -am \
    -Dmaven.build.cache.enabled=false \
    -Dtest=TrinoTypeMappingTest,ConnectorTypeTest -DfailIfNoTests=false test
```

编译门禁（最强的单一信号，验收必跑；`ConnectorType` 加了硬校验，要靠全反应堆确认没有别处的生产或测试源在构造不合法的复杂类型）：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -Dmaven.build.cache.enabled=false
```

不要加任何跳过测试编译的参数。checkstyle 会扫测试源，新建文件要过。

### 端到端回归（可选，需要外部环境）

`regression-test/suites/external_table_p0/trino_connector/hive/` 与 `external_table_p2/trino_connector/` 下**目前没有任何 STRUCT 列的用例**（已 grep 确认），所以这个缺陷此前不可能被端到端拦住。补一条最有价值：建一张带 `struct<a:int,b:string>` 列的 hive 表（`test_trino_prepare_hive_data_in_case.groovy` 是"用例内建表"的现成模板），断言 `DESC` 输出的字段名以及 `SELECT s.a` 能跑通。这一步需要 docker 外部环境，本 session 通常跑不了——补了用例后如实标注「未在本地执行」，不要声称已通过。单元测试已经覆盖转换逻辑本身。

## 七、风险与回退

- **总体风险低。** trino 侧是一个 `else if` 分支内部的改写，方向是「补上本该带的信息」，不改变任何类型的形状与个数；`ConnectorType` 侧是纯前置校验，合法调用路径的行为完全不变。
- **主要风险是新校验硬抛异常。** 已逐一核对全部生产侧构造点：只有 `TrinoTypeMapping`（本次修）与 `EsTypeMapping.java:182-183`（ARRAY 单子类型，合法）用裸构造器，其余连接器一律走工厂且名字与类型在同一循环成对填充；四组可选元数据的现有传参也都是等长或为空。测试源里的构造点由全反应堆 `test-compile` 加上第六节列出的既有测试兜住。若后续有人构造出对不齐的复杂类型，会在构造点立刻抛错而不是静默编造名字——这是刻意选择。
- **用户可见变化：trino 目录下 STRUCT 列的字段名会从 `col0` / `col1` 变成真实名字。** 外部表结构不落盘持久化，升级后重新加载即生效，无需元数据迁移。如果有人此前把 `col0` 写进了查询或视图，那些写法会失效——但它们本来就是在依赖一个缺陷，并且真实名字恰好叫 `col0` 的情况仍然正常工作。
- **回退**：两处改动互相独立，各自都是单文件局部改写，直接 revert 即可。注意若只 revert trino 侧而保留校验，trino 目录的 STRUCT 列会在表加载时抛异常——要回退就一起回退。

## 八、相关背景

- 调研报告 `plan-doc/connector-public-interface-cleanup/audit-report.md`：
  - `### 11.1 四个有实际用户可见后果的缺陷` 第（2）条 —— 复杂类型字段名被丢弃，本缺陷的摘要条目；
  - `### A.6 实现与接口定义不符（一致性）（24 条）` 第 116 条 —— 平行列表无契约无校验，原始判定与位置。
  - 同一章的第 124 条记录了 `ConnectorType.typeName` 的另一个问题（javadoc 说是「连接器自己的类型系统」，实际必须用 Doris 内部拼写，未知名字静默退化为 `UNSUPPORTED`）。它与本任务同在 `ConnectorType`，但**不在本任务范围内**：那是词表与文档问题，本任务只处理复杂类型的形状校验，不要混在一起改。
- 相邻任务：`tasks/01-fix-trino-or-predicate-row-loss.md` 同样同时改 `fe-connector-trino` 与 `fe-connector-api`，但改的是 `TrinoPredicateConverter` 与 `ConnectorOr`，与本任务无文件重叠，两者可独立进行；两者采用同一种「在公共接口构造期 fail loud」的修法，实施时可互相参照措辞风格。
