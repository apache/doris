# 02. 修复 paimon 连接器把「空值安全等于」下推成 IS NULL 导致的丢行

> **优先级**：第一优先级（正确性缺陷） ｜ **风险**：低 ｜ **前置依赖**：无（与「下推表达式契约补全」任务同源，但两者可各自独立完成，本任务不必等契约文字先落地）
> **影响模块**：`fe-connector-paimon`（主代码 1 个文件 + 测试 1 个文件），不触碰 `fe-core`，不触碰 `fe-connector-api`
> **预计改动规模**：2 个文件；主代码约 10 行，测试约 60 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

paimon 连接器把 `列 <=> 非空字面量`（空值安全等于）翻译成了「该列 IS NULL」，方向完全相反；要改成「右边是非空字面量就翻成等值比较，右边是空值字面量才翻成 IS NULL」，与 iceberg / trino / es 三家已经写对的做法一致。

## 二、背景：现在的代码是怎么写的

谓词下推的入口是 `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonPredicateConverter.java`。它把引擎交下来的中立表达式树（`ConnectorExpression`）翻译成 paimon 自己的 `Predicate`，翻不出来就返回 `null`，让 BE 端自己再过滤一遍——这是安全的放宽方向。

出问题的是比较表达式的翻译函数 `convertComparison`（`PaimonPredicateConverter.java:144-178`）：

```java
Object value = convertLiteralValue(literal, fieldTypes.get(idx));   // :156
if (value == null) {                                                // :157
    return null;                                                    // :158
}
switch (cmp.getOperator()) {
    case EQ:
        return builder.equal(idx, value);                           // :162
    ...
    case EQ_FOR_NULL:
        return builder.isNull(idx);                                 // :173-174
    default:
        return null;
}
```

关键在于 `convertLiteralValue`（`PaimonPredicateConverter.java:244-247`）开头就写着：

```java
if (literal.isNull()) {
    return null;
}
```

也就是说：**空值字面量在 :156 就已经被变成 `null`，在 :157 被提前 return 掉了**。等执行流走到 :173 的 `case EQ_FOR_NULL` 时，右操作数一定是个非空字面量——却返回了 `builder.isNull(idx)`。

注意 `value == null` 在这里有两种完全不同的含义，代码把它们混在一起了：一是「字面量本身是空值」，二是「这个 paimon 类型故意不下推」（同文件里 FLOAT、CHAR、TIMESTAMP WITH LOCAL TIME ZONE 都会返回 `null`，见测试 `floatNotPushed` / `charNotPushed` / `ltzNotPushed`）。修复时必须靠 `literal.isNull()` 把两者区分开。

翻出来的谓词有两个下游消费点，都在 `PaimonScanPlanProvider.java`：

- `:506` 把它交给 paimon 的 `ReadBuilder.withFilter`，在 FE 规划阶段做分区级 / 数据文件级裁剪；
- `:783` 把它序列化进 `paimon.predicate` 属性发给 BE 的 paimon JNI reader，在读取时再过滤一次。

顺带说明：paimon 连接器不声明「已消费某个 conjunct」，所以原始条件仍会作为残余过滤在 BE 上重算（`PluginDrivenScanNode.buildRemainingFilter`）。残余过滤只能再减行，不可能把 FE 阶段已经裁掉的文件找回来。

**同一个算子，其他四家都是对的：**

| 连接器 | 位置 | 做法 |
| --- | --- | --- |
| iceberg | `IcebergPredicateConverter.java:245-251` 与 `:252-254` | 只有「值转换失败且 `literal.isNull()` 且算子是空值安全等于」才转 `Expressions.isNull`；非空时和普通等值走同一分支 |
| trino | `TrinoPredicateConverter.java:132-139` | 显式按 `((ConnectorLiteral) cmp.getRight()).isNull()` 分两支：`Domain.onlyNull` 还是 `Range.equal` |
| es | `EsQueryDslBuilder.java:396-405` | `value == null` 走 `must_not exists`，否则走 term 查询；并有两个专测 `EsQueryDslBuilderTest.java:176` 与 `:201` |
| maxcompute | `MaxComputePredicateConverter.java:172-173` | 注释说明 ODPS 无对应算子，落到 default 直接放弃下推 |

**归责（必须写清）**：这不是本次连接器迁移引入的回退。`git show master:fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonPredicateConverter.java` 里的老实现，在 `binaryExprDesc` 中同样是先 `if (value == null) return null;`，再 `case EQ_FOR_NULL: return builder.isNull(idx);`——写法逐字相同。这是上游既有缺陷的忠实移植，本任务是顺手把它修掉。

## 三、为什么这是个问题

1. **翻译方向相反，不是收窄而是错。** 下推的容许方向只有「放宽」（多读点、让 BE 再过滤）。把「等于 5」翻成「为空」既不是原条件的超集也不是子集，两者的结果集是互斥的。
2. **后果是静默少行。** FE 阶段按「IS NULL」裁剪，含 `c=5` 的数据文件根本不会进入扫描列表；BE 的残余过滤只能删行，补不回来。用户观察到的现象是：`WHERE c <=> 5` 返回 0 行（或只剩恰好也满足 IS NULL 的行），**没有任何报错、没有 warning**，`EXPLAIN` 里看到的裁剪后分区数也偏小。
3. **默认会话下这条缺陷目前是潜伏的，但仍必须修。** 这一点是对早期调研结论的修正：Nereids 有一条改写规则 `NullSafeEqualToEqual`（`fe/fe-core/src/main/java/org/apache/doris/nereids/rules/expression/rules/NullSafeEqualToEqual.java:66-87`），在过滤条件位置只要有一边不可为空（字面量恒不可为空），就会把 `c <=> 5` 提前改写成 `c = 5`，所以走普通 `WHERE` 时坏分支通常到不了连接器。但是：
   - 这条规则带 `ExpressionRuleType.NULL_SAFE_EQUAL_TO_EQUAL` 标签，可以被会话变量 `disable_nereids_rules` 关掉（`ExpressionPatternRules.java:78/95/111` 按该标签查 `disableRules` 位图）。关掉之后就是一条实打实的错误结果查询。
   - 连接器的翻译正确性不能建立在「某条可关闭的优化规则一定先跑过」之上。等值下推是连接器自己的契约义务。
   - 一旦坏分支出现在 OR 里（`convertOr`，`PaimonPredicateConverter.java:132-142`），错误的那一支会污染整个 OR 谓词，影响范围比单个 conjunct 更大。
4. **根因在公共接口一侧。** 比较算子的公共契约总共只有一行文字：`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/pushdown/ConnectorComparison.java:27` 写着 `Supported operators: EQ, NE, LT, LE, GT, GE, EQ_FOR_NULL.`，然后 `:41` 给了个 `EQ_FOR_NULL("<=>")` 的符号。没有任何地方写明「右操作数可能是空值字面量」、「空值安全等于的语义是什么」、「不能精确翻译时必须放弃下推、只准放宽不准收窄」。于是五个消费者各写一遍，四家写对一家写错。**补这段契约文字是另一个任务（下推表达式契约补全）的事，本任务不改 `ConnectorComparison`。**

## 四、用一个最小例子说明

建一张 paimon 表，`c` 列可为空，写入三行：`c = 5`、`c = 7`、`c = NULL`。

```sql
-- 让优化器不要抢先把 <=> 改写掉，直接压到连接器的翻译逻辑上
set disable_nereids_rules = 'NULL_SAFE_EQUAL_TO_EQUAL';

SELECT * FROM paimon_ctl.db.t WHERE c <=> 5;
```

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
| --- | --- | --- |
| `WHERE c <=> 5` | 下推给 paimon 的是 `isNull(c)`；只有 `c IS NULL` 的文件被读进来，BE 再用原条件 `c <=> 5` 过滤 → **返回 0 行** | 下推 `equal(c, 5)` → 返回 `c = 5` 那一行 |
| `WHERE c <=> NULL` | 空值字面量在 `convertLiteralValue` 就被丢掉 → 整个条件不下推，BE 全量过滤 → 结果正确，只是白读数据 | 下推 `isNull(c)` → 结果同样正确，且能做文件级裁剪 |
| `WHERE c = 5` | 下推 `equal(c, 5)` → 正确（对照组，说明问题只在空值安全等于这一支） | 不变 |

一句话：第一行现在是**错的**（少行），第二行现在是**对但浪费**的，本任务把两行都摆正。

## 五、解决方案

### 5.1 目标状态

`convertComparison` 里把「值转换失败」这个统一出口拆成两种情形，形状与 iceberg 对齐（先判空值安全等于的空值特例，再走原有 switch）：

```java
Object value = convertLiteralValue(literal, fieldTypes.get(idx));
if (value == null) {
    // 只有 `col <=> NULL` 能在没有值的情况下翻译：它等价于 IS NULL。
    // 其他情况（普通比较遇空值字面量、或该 paimon 类型故意不下推）一律放弃下推。
    if (cmp.getOperator() == ConnectorComparison.Operator.EQ_FOR_NULL && literal.isNull()) {
        return builder.isNull(idx);
    }
    return null;
}
switch (cmp.getOperator()) {
    case EQ:
    case EQ_FOR_NULL:            // 右边是非空字面量时，`<=>` 与 `=` 的结果集完全一致
        return builder.equal(idx, value);
    ...
    // 原 `case EQ_FOR_NULL: return builder.isNull(idx);` 删除
}
```

为什么非空时用 `equal` 是精确等价而不是放宽：`c <=> 5` 为真当且仅当 `c = 5`（`c` 为空时结果是 false 而非 unknown）。paimon 的 `Equal` 继承 `NullFalseLeafBinaryFunction`，空值本身不会匹配 `equal`，语义正好吻合。

**注意判定顺序**：必须先判 `literal.isNull()` 再判类型，不能只判「算子是 `EQ_FOR_NULL`」就返回 `isNull`——否则一个 FLOAT 列上的 `c <=> 1.5`（值转换因类型故意不下推而失败）又会被翻成 IS NULL，等于换个地方复发同一个 bug。

### 5.2 改动清单

| 文件 | 改什么 |
| --- | --- |
| `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonPredicateConverter.java` | `convertComparison`：`:157-159` 的 `value == null` 出口内加空值安全等于 + `literal.isNull()` 的 IS NULL 分支；`:173-174` 的 `case EQ_FOR_NULL` 从返回 `isNull` 改为与 `case EQ` 合并（fall-through 到 `builder.equal`）。两处都补注释说明为什么。 |
| `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonPredicateConverter.java` 类注释 | 类头注释里补一句「空值安全等于按右操作数是否为空值字面量分流」，与 iceberg 同类注释口径一致。可选但推荐。 |
| `fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/PaimonPredicateConverterTest.java` | 新增测试，见第六节。现有的 `convertEq` 辅助方法只造 `EQ`，需要再加一个能指定算子和空值字面量的辅助方法（`ConnectorLiteral.ofNull(type)` 已存在，见 `ConnectorLiteral.java:45`）。 |

### 5.3 明确不要顺手做的事

- **不要改 `ConnectorComparison.java`**（不改那行 `Supported operators`、不改枚举、不加 javadoc 契约段落）。契约补全是另一个任务的范围，两个任务同时改同一行会互相打架。
- **不要顺手修同一个文件里 LIKE 的收窄缺陷**（`convertLike`，`PaimonPredicateConverter.java:218-239`，含单字符通配符 `_` 的模式被当成前缀匹配）。那是独立的一条，单独一个任务、单独一次提交，便于回退定位。
- **不要动 iceberg / trino / es / maxcompute 的转换器**。四家已核实为正确实现，改它们只会引入风险。
- **不要在 `fe-core` 里加一个「统一改写空值安全等于」的公共 helper**。当前阶段 `fe-core` 数据源相关代码只出不进；而且在引擎侧统一改写会把连接器各自的翻译义务藏起来，下一个连接器照样会写错。
- **不要去改 Nereids 的 `NullSafeEqualToEqual` 规则**，也不要为了「反正优化器会改写」就不修连接器。优化器改写是加分项，不是正确性依据。
- **不要新增 `supportsXxx()` 能力位**。这不是「paimon 不支持某个能力」，而是「翻译写错了」，能力位在这里没有意义。
- **不要写 shell / 正则的构建门禁**去校验「`case EQ_FOR_NULL` 后面不许接 `isNull`」。本仓库已有明确结论：静态门禁只适合存在性与前缀类不变量，要理解 switch 分支极性就等于在 shell 里写 Java 解析器，误报比漏报更毒。用单元测试 + 注释即可。

## 六、怎么验证

**单元测试（必须）**，加在 `PaimonPredicateConverterTest.java`，全部离线（转换器只需要一个 `RowType`，不需要 catalog）：

1. `eqForNullWithNonNullLiteralPushesEqual`：INT 列上构造 `ConnectorComparison(EQ_FOR_NULL, col("id"), literal 5)`，断言产出 1 个谓词，且 `((LeafPredicate) p).function()` 是 `org.apache.paimon.predicate.Equal`（可用 `Assertions.assertSame(Equal.INSTANCE, leaf.function())`），`leaf.literals().get(0)` 等于 `5`。
   注释要写清 WHY：**如果这里翻成 IS NULL，含 `id=5` 的数据文件会在 FE 规划阶段被裁掉，查询静默少行**。
2. `eqForNullWithNullLiteralPushesIsNull`：同一列上用 `ConnectorLiteral.ofNull(...)`，断言 `function()` 是 `org.apache.paimon.predicate.IsNull`，且 `literals()` 为空。
3. `plainEqWithNullLiteralNotPushed`：`ConnectorComparison(EQ, col, ofNull(...))` 必须一个谓词都不产生（普通等值遇空值字面量结果恒为 unknown，不能翻成 IS NULL 也不该翻成 equal）。
4. `eqForNullOnNonPushableTypeNotPushed`：FLOAT 列上 `EQ_FOR_NULL` 配非空字面量 `1.5`——值转换会失败（FLOAT 故意不下推），断言产出为空，**不能**变成 IS NULL。这条专门守住 5.1 里提到的判定顺序。

**变异验证（必须做，写进测试注释）**：把改好的 `case EQ_FOR_NULL` 改回 `return builder.isNull(idx)` → 第 1 条测试必须变红；把第 4 条对应的守卫条件从 `literal.isNull()` 放宽成只判算子 → 第 4 条必须变红。两条测试各自能被对应的错误写法打红，才算测到了意图而不是行为快照。

**编译门禁（最强单一信号）**：全反应堆含测试源编译，**不许**加跳过测试编译的参数。

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -DskipTests
```

**跑测试**必须显式关掉 maven build cache，否则 surefire 会被静默跳过、`BUILD SUCCESS` 是空的：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-paimon \
    -Dmaven.build.cache.enabled=false \
    -Dtest=PaimonPredicateConverterTest test
```

要读输出里的 `Tests run:` 行确认用例真的跑了，不要只看 `BUILD SUCCESS`。

**端到端回归（可选，需要 docker 环境，本地不跑）**：现有 paimon 回归套件里检索不到任何 `<=>` 用例。如果要补，注意两点：一是必须在用例里 `set disable_nereids_rules='NULL_SAFE_EQUAL_TO_EQUAL'`，否则优化器会先把条件改写掉，用例根本压不到连接器路径，改坏了也照样绿；二是断言要同时覆盖 `c <=> 非空值`（有行）、`c <=> NULL`（只出空值行）和 `c = 非空值`（对照）。这一项不阻塞本任务合并。

## 七、风险与回退

风险低。行为变化被严格限制在「算子是空值安全等于」这一条分支上，其余算子的代码路径逐字不变，第 3、4 条测试就是用来钉住这一点的。

唯一的新增行为是「`c <=> NULL` 现在会下推成 IS NULL」——这是从「不下推」变成「下推一个语义正确的 IS NULL」，属于新增裁剪能力。paimon 的 `isNull` 叶子谓词只依赖空值计数统计，与列类型无关，因此即使在 FLOAT / CHAR / 带本地时区时间戳这些故意不下推值比较的列上也是安全的（iceberg 就是这么做的）。如果评审希望把改动面压到最小，可以只做「非空字面量 → equal」这一半，把空值字面量继续留给 BE 过滤；两种方案都修掉了正确性缺陷，前者额外多一点裁剪收益。

回退：单文件 `git revert`，无跨模块耦合，无持久化格式、无 thrift 有线格式、无公共接口签名变化。

## 八、相关背景

- 调研报告 `plan-doc/connector-public-interface-cleanup/audit-report.md`：
  - 「主题八：实现与接口定义不符」→「11.1 四个有实际用户可见后果的缺陷」的第（3）条 —— `<=>` 被译成 IS NULL，就是本任务的来源；
  - 「十五、建议的整治路线」里的分组表第 6 项 —— 修四个真实缺陷的排期；
  - 附录 D.7 末尾一条 —— 四家兄弟连接器写法均正确：确认只有 paimon 错，并已用 `git show master` 确认属上游既有缺陷的移植。
- 相关任务：**下推表达式契约补全**（把「右操作数可能为空值字面量」「只准放宽不准收窄」写进 `ConnectorComparison` 的公共契约），它解决的是根因；本任务解决的是已经发生的后果。
- 同一文件另一条独立缺陷：paimon 的 LIKE 把含单字符通配符的模式收窄成前缀匹配，单列一个任务处理。
