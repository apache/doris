# 09. 补全下推表达式的契约（逐算子语义 + 不可精确翻译必须放弃下推）

> **优先级**：第二优先级（零风险） ｜ **风险**：低 ｜ **前置依赖**：无。本任务是任务 01、02、03 三条丢行缺陷的共同根因；那三条修复不必等本任务，但本任务落地后应回头把它们的守卫写法对齐到这里写下的契约。
> **影响模块**：`fe-connector-api`（全部改动集中在这里）。可选：`fe-connector-paimon`（仅当决定让它改用本任务提供的公共工具方法）。
> **预计改动规模**：1 个新增 `package-info.java`（约 120 行注释）＋ 6 个已有类的 javadoc 补写（每处 10～40 行注释）＋ 可选 1 个新增工具类（约 40 行）＋ 1 个新增单元测试。零行为改动（除可选工具类外不新增任何可执行语句）。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

下推表达式（`org.apache.doris.connector.api.pushdown` 这一包）是引擎交给连接器的唯一谓词语言，但它今天几乎没有契约：算子的语义、字面量里能装什么 Java 对象、通配符怎么写、翻译不出来时该怎么办，全都没写。结果是七个连接器各自猜一遍规则，猜错的那几个会让查询**静默少行**。这个任务把规则写进公共模块，让下一个连接器作者不必靠读别人的实现来反推。

## 二、背景：现在的代码是怎么写的

**这个包提供什么。** `fe-connector-api` 的 `pushdown` 包有 18 个类，构成一棵中立表达式树：根接口 `ConnectorExpression` 与谓词标记 `ConnectorPredicate`，节点 `ConnectorAnd` / `ConnectorOr` / `ConnectorNot` / `ConnectorComparison` / `ConnectorIn` / `ConnectorIsNull` / `ConnectorBetween` / `ConnectorLike` / `ConnectorFunctionCall` / `ConnectorColumnRef` / `ConnectorLiteral`，入参载体 `ConnectorFilterConstraint` 与 `ConnectorColumnAssignment`，加上三个"应答"载体 `FilterApplicationResult` / `ProjectionApplicationResult` / `LimitApplicationResult`。

**契约总量。** 每个节点类只有一句话的 javadoc。两个最关键的：

- `ConnectorComparison.java:24-28`：
  `Binary comparison: left op right.` ＋ `Supported operators: EQ, NE, LT, LE, GT, GE, EQ_FOR_NULL.`
  枚举本体（`:34-52`）只给了七个符号串（`EQ("=")` … `EQ_FOR_NULL("<=>")`）。没有一处说明右操作数可能是空值字面量、`EQ_FOR_NULL` 在字面量非空与为空两种情形下分别是什么意思。
- `ConnectorLike.java:24-26`：
  `A LIKE/REGEXP predicate: {@code value LIKE pattern}.`
  没有转义符、没有 `%` 与 `_` 的方言说明、没有说 `REGEXP` 是部分匹配还是整串锚定、没有说大小写敏感性。

**表达式是谁生产的，两条到达路径不一样。** 生产者只有 fe-core 的 `ExprToConnectorExpressionConverter`（另外两个入口 `NereidsToConnectorExpressionConverter:232-234` 与 `UnboundExpressionToConnectorPredicateConverter` 都转手复用它，所以字面量编码全仓一致）。它到达连接器有两条路，**只有第二条会剥掉带 CAST 的谓词**：

| 路径 | 引擎侧构造点 | 是否按 `supportsCastPredicatePushdown` 剥 CAST |
|---|---|---|
| `ConnectorPushdownOps.applyFilter`（`:38-44`）收到的 `ConnectorFilterConstraint` | `PluginDrivenScanNode.java:2043-2046` `buildFilterConstraint` | **不剥**，原样给 |
| `planScan` / `getScanNodePropertiesResult` 收到的 `Optional<ConnectorExpression> filter` | `PluginDrivenScanNode.java:2053-2079` `buildRemainingFilter` | 剥（`supportsCastPredicatePushdown` 为 false 时） |

且 `convertConjuncts`（`ExprToConnectorExpressionConverter.java:135-147`）在只有一个 conjunct 时**直接返回那一个节点，不包 `ConnectorAnd`**；无 conjunct 时返回布尔真字面量。

**字面量里实际会装什么。** `ConnectorLiteral.java:29-32` 的 javadoc 声称是 `(null, Boolean, Integer, Long, Double, String, BigDecimal, LocalDate, LocalDateTime)`。对照真实生产代码 `ExprToConnectorExpressionConverter.java:288-322`：`IntLiteral.getValue()` 返回 `long`，`FloatLiteral.getValue()` 返回 `double`，`LargeIntLiteral` 三个分支都不匹配、落到兜底 `literal.getStringValue()`。所以引擎实际产出的集合是：`null` / `Boolean` / `Long` / `Double` / `BigDecimal` / `String` / `LocalDate` / `LocalDateTime`——**`Integer` 永远不会出现**（`ConnectorLiteral.ofInt` 只被测试用到），而 `LARGEINT` 字面量会以 `String` 到达。

**两套"连接器吃掉了哪些谓词"的协议，只有一套真的生效。**

| 协议 | 连接器怎么表达 | 引擎实际怎么处理 |
|---|---|---|
| `FilterApplicationResult.getRemainingFilter()`（`:45-47`） | 返回剩余表达式，或 `null` 表示全吃掉 | `PluginDrivenScanNode.java:883-892`：`null` → `conjuncts.clear()`；**非 `null` → 一个 conjunct 都不摘**（注释写明细粒度反查 deferred to a future enhancement） |
| `ScanNodePropertiesResult` 的 `notPushedConjunctIndices`（`:52-65`） | 报出**没被**吃掉的下标 | `PluginDrivenScanNode.java:1847-1892`：按下标反查并真的摘除；`hasConjunctTracking()` 为 false 时不摘 |

现实是：全仓三个 `applyFilter` 实现（`HiveConnectorMetadata.java:1115-1116`、`HudiConnectorMetadata.java:312`、`TrinoConnectorDorisMetadata.java:292-296`）都把原表达式原样当残差返回，没人返回 `null`；而下标协议全仓只有一个实现方（`EsScanPlanProvider.java:165,220`）。也就是说今天所有连接器的谓词都会在 BE 侧被复算一遍。

**顺带一个文档 bug**：`ConnectorScanPlanProvider.java:446-447` 说默认实现"wraps getScanNodeProperties with an empty not-pushed set, meaning all conjuncts are assumed to have been pushed"。但 `:459-461` 走的是单参构造器 → `hasConjunctTracking = false` → 引擎**一个也不摘**。文档说的语义和真实语义正好相反。

## 三、为什么这是个问题

**第一，它是三条已知丢行缺陷的同一个根因。** 五个连接器各自实现了同一段翻译，四家蒙对一家蒙错：

| 连接器 | `EQ_FOR_NULL` 怎么处理 | 结果 |
|---|---|---|
| iceberg（`IcebergPredicateConverter.java:245-251`） | 字面量为空 → `isNull`；非空 → `equal` | 对 |
| trino（`TrinoPredicateConverter.java:132-140`） | 同上，显式判 `isNull()` | 对 |
| maxcompute（`MaxComputePredicateConverter.java:168-174`） | 远端没有对应算子 → 落到 default → 放弃下推 | 对（且注释写明了理由） |
| es | 有专门测试覆盖 | 对 |
| paimon（`PaimonPredicateConverter.java:144-176`） | `:157-159` 先把空字面量挡掉 → `:173-174` 无条件 `builder.isNull(idx)` | **错**：`WHERE c <=> 5` 变成 `c IS NULL` |

`ConnectorLike` 那侧同理：paimon 只要模式"不以 `%` 开头且以 `%` 结尾"就转成前缀匹配（`PaimonPredicateConverter.java:234-237`），对 `_` 和被转义的 `%` 毫无守卫；es 是唯一实现了转义处理的（`EsQueryDslBuilder.java:547-567`）；jdbc 把模式原样拼进远端 SQL（`JdbcQueryBuilder.java:424-432`）；`REGEXP` 在 es 直接交给 ES 的 `regexp` 查询（`:661-666`，Lucene 语义是整串锚定），在 jdbc 交给远端数据库的 `REGEXP`（多数是部分匹配）——同一棵表达式树，两种锚定语义，接口没说哪种是对的。

**第二，"过宽安全、过窄致命"这条最重要的规则从来没写下来，而且它有前提条件。** 用户观察到的后果是：查询不报错，只是少了行；`EXPLAIN` 看不出异常；BE 也补不回来——因为连接器已经在文件/分区级别把数据跳过了，根本没送到 BE。反过来"过宽"之所以安全，恰恰是因为引擎保留了 conjuncts 让 BE 复算（见上一节的两套协议）；一旦连接器通过残差协议声明"这些谓词我全吃了"，过宽就会**多返回行**。这两件事必须写在同一处，否则连接器作者只会读到半句话。

**第三，接口把"安全方向"当成了普适的，但它按用途反转。** 同一棵表达式树有三个用途，三种正确做法：

| 用途 | 丢一个 conjunct 意味着 | 正确做法 |
|---|---|---|
| 扫描下推（`applyFilter` / `planScan`） | 过滤变宽 → BE 复算兜住 | 允许放弃，禁止收窄 |
| 写时冲突检测（`ConnectorTransaction.applyWriteConstraint`） | 冲突检测范围变宽 → 更保守 | 允许放弃 |
| `ALTER TABLE … EXECUTE … WHERE` 的重写范围 | 重写更多文件，极端情况整表重写 | **必须报错**，不许放弃（`UnboundExpressionToConnectorPredicateConverter.java:71-79` 已这样实现并写了注释） |

这条只在一个 fe-core 内部类的注释里，公共模块里读不到。

## 四、用一个最小例子说明

一张 paimon 表，`c` 列有两行：`c = 5` 和 `c = NULL`。

| 用户写的 SQL | 今天实际发生什么 | 应该发生什么 |
|---|---|---|
| `SELECT * FROM t WHERE c <=> 5` | 连接器把它翻成 `c IS NULL`，paimon 按此裁掉含 `c=5` 的数据文件 → **返回 0 行** | 返回 1 行（`c=5`）。翻不出来就整条别下推，让 BE 自己过滤 |
| `SELECT * FROM t WHERE s LIKE 'a_c%'` | 翻成 `startsWith("a_c")` → `'abc'` 这行被数据源跳过 → **少行** | `'abc'` 应当命中。模式里有 `_` 就不能当前缀用 |

两条的共同点：**连接器写出了一个比用户谓词更窄的过滤条件**，而更窄的下推条件在架构上是不可恢复的。契约要写的就是这一句：

```text
翻译规则（连接器实现方必读）：
  能精确表达  → 下推
  不能精确表达 → 整条放弃（返回 null / 不吃这个 conjunct），交给 BE
  不允许      → 下推一个"差不多"的、范围更小的近似
并且：只有在你没有通过残差协议声明"已全部吃掉"时，过宽才是安全的。
```

## 五、解决方案

### 5.1 目标状态

**（1）包级说明。** 新增 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/pushdown/package-info.java`，把下面六件事一次写清（英文 javadoc，与仓库其余注释一致）：

1. 这棵树是谁生产的、两条到达路径的差别（`applyFilter` 拿到的表达式**未剥** CAST；`planScan` / `getScanNodePropertiesResult` 拿到的已按 `supportsCastPredicatePushdown` 剥过）；单 conjunct 时根节点不是 `ConnectorAnd`；空 conjunct 时是布尔真字面量。
2. **总则**：不能精确表达就整条放弃，禁止返回更窄的近似；并说明"过宽为何安全"的前提是引擎仍保留 conjuncts。
3. 安全方向按用途反转的三行表（扫描 / 写冲突 / EXECUTE 重写），指向 `applyWriteConstraint` 与 `UnboundExpressionToConnectorPredicateConverter` 的既有注释。
4. 字面量取值域：引擎实际产出的 8 种 Java 类型，逐 Doris 类型对照（含 `TINYINT`/`SMALLINT`/`INT`/`BIGINT` 一律 `Long`、`FLOAT`/`DOUBLE` 一律 `Double`、`LARGEINT` 是 `String`、`DATE` 是 `LocalDate`、`DATETIME` 是 `LocalDateTime`、其余类型兜底为 `Expr.getStringValue()` 的 `String`），并写明 `Integer` 不会出现。
5. 两套残差协议的**实际效力**（照抄第二节那张表的结论），明确"返回 remainingFilter 不等于引擎会替你摘 conjunct"。
6. 零参数 `ConnectorFunctionCall` 的陷阱：`ExprToConnectorExpressionConverter.java:342-354` 对无法识别的表达式会构造一个**函数名是原始 Doris SQL 文本、参数列表为空**的 `ConnectorFunctionCall`。它不是函数调用，连接器不得按函数名匹配语义（jdbc 在 `JdbcQueryBuilder.java:492-495` 刻意把它当预渲染 SQL 片段处理）。

**（2）逐算子语义写到类上。** `ConnectorComparison` 的 javadoc 补出七个算子的语义，其中 `EQ_FOR_NULL` 必须写成两种情形：

```java
/**
 * ...
 * <p>EQ_FOR_NULL ({@code <=>}) is Doris' null-safe equality and has TWO cases that MUST be
 * distinguished; collapsing them loses rows:</p>
 * <ul>
 *   <li>right operand is a NULL literal ({@code ConnectorLiteral#isNull()}):
 *       equivalent to {@code IS NULL}.</li>
 *   <li>right operand is a NON-NULL literal: equivalent to plain {@code EQ} —
 *       it is NOT {@code IS NULL}. Translating {@code c <=> 5} to {@code c IS NULL}
 *       silently drops every matching row.</li>
 * </ul>
 * <p>A connector whose dialect has no null-safe form must drop the whole conjunct
 * (see the package javadoc); it must never substitute a narrower predicate.</p>
 */
```

**（3）`ConnectorLike` 的方言写清**：`%` 匹配任意长度、`_` 匹配单字符、转义符是反斜杠（因此模式里 `\%` `\_` 是字面量）、`REGEXP` 是**部分匹配**（Doris 语义，未锚定）、大小写敏感性随列的排序规则、以及一条已核实的表示能力边界——Doris 的 `LIKE … ESCAPE <ch>` 三参形态（`nereids/trees/expressions/Like.java:46,106-116`）**无法**用 `ConnectorLike` 表示（它只有 value/pattern 两个孩子，且 `ExprToConnectorExpressionConverter.java:117-122` 只在 `size() == 2` 时才构造它），因此连接器只需按固定的反斜杠转义符处理。

**（4）修正三处与实现矛盾的文档**（都在下推与扫描应答面上，见 5.3 的分工约定）：`ConnectorLiteral` 的 Java 类型清单、`FilterApplicationResult.getRemainingFilter` 的实际效力、`ConnectorScanPlanProvider.java:446-447` 那句说反了的默认语义。

**（5）可选的中立工具方法（一个类、一个方法）。** 只做真正能消灭一类缺陷的那一个：

```java
public final class ConnectorLikePatterns {
    private ConnectorLikePatterns() {}

    /**
     * Returns the literal prefix iff {@code pattern} is exactly "literal text + one trailing %"
     * (no other %, no _, no backslash escape). Empty otherwise — the caller must then NOT
     * narrow the predicate to a prefix match.
     */
    public static Optional<String> exactPrefix(String pattern);
}
```

`EQ_FOR_NULL` 那侧**不加**工具方法：判断本身只有一行（算子 ＋ `isNull()`），而 iceberg / trino 已各自写对，为了复用去改它们等于给零收益的改动加上非零风险。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe-connector-api/…/api/pushdown/package-info.java`（新增） | 写入 5.1(1) 的六节包级契约 |
| `fe-connector-api/…/api/pushdown/ConnectorComparison.java` | 类 javadoc 补逐算子语义，`EQ_FOR_NULL` 两种情形；不动枚举与字段 |
| `fe-connector-api/…/api/pushdown/ConnectorLike.java` | 类 javadoc 补通配符/转义/锚定/大小写，写明三参 ESCAPE 不可表示 |
| `fe-connector-api/…/api/pushdown/ConnectorLiteral.java` | 修正类 javadoc 的 Java 类型清单（去掉 `Integer`、补 `LARGEINT` 走 `String`），`getValue()` 上补一句"空值字面量合法，比较算子必须先判 `isNull()`" |
| `fe-connector-api/…/api/pushdown/FilterApplicationResult.java` | `getRemainingFilter()` 上写明：非 `null` → 引擎不摘任何 conjunct；`null` → 引擎清空 conjuncts，因此声明 `null` 前必须确保下推是**精确**的 |
| `fe-connector-api/…/api/scan/ConnectorScanPlanProvider.java` | 改正 `:446-447` 说反的默认语义；在 `getScanNodePropertiesResult` 上写明下标是**剥 CAST 之后**那份 conjunct 列表的下标，单 conjunct 时下标 0 指整棵表达式 |
| `fe-connector-api/…/api/scan/ScanNodePropertiesResult.java` | 类 javadoc 补一句：这是目前唯一真正生效的残差协议，全仓仅 es 实现 |
| `fe-connector-api/…/api/pushdown/ConnectorLikePatterns.java`（新增，可选） | 5.1(5) 的单个静态方法 |
| `fe-connector-api/src/test/…/api/pushdown/ConnectorLikePatternsTest.java`（新增，随可选项） | 见第六节 |

### 5.3 明确不要顺手做的事

- **不要改任何连接器的翻译逻辑。** paimon 的两个丢行缺陷分别归任务 02 与 03，trino 的 OR 归任务 01。本任务只提供契约；如果它们已经先落地，本任务不再回改它们的代码，只在契约里引用其守卫作为示例。
- **不要在本任务里合并那两套残差协议。** 合并的前置是先实现细粒度反查（把残差子表达式对回原始 conjunct），那是行为改动，与"零风险"矛盾。本任务只把现状写清楚。
- **不要给 `ConnectorOr` / `ConnectorAnd` 加构造期校验。** "至少两个分支"的校验属于任务 01 的范围（它需要连带确认没有单分支构造点）。
- **不要统一列名大小写规则。** 今天 paimon 在查找时 `toLowerCase()`（`PaimonPredicateConverter.java:152`）、jdbc 走自己的列名映射表。要不要统一是行为决策，不属于本任务；本任务最多在包级说明里如实记一句"列名大小写规则目前由各连接器自行决定"。
- **不要写 shell 或正则的构建门禁去校验"是否收窄了谓词"。** 这需要理解 Java 布尔语义，本仓库已有结论：那类门禁只适合存在性与前缀类不变量，误报比漏报更毒。这里正确的机器化手段是单元测试。
- **不要顺手改 `pushdown` 包外的其它陈旧文档。** 那是任务 08 的范围。为避免两个任务改同一行，约定：`pushdown` 包内的文档 ＋ `ConnectorScanPlanProvider` / `ScanNodePropertiesResult` 上与残差协议相关的那几行归本任务，其余归任务 08。动手前请先看任务 08 的清单是否已包含这几处并划掉。

## 六、怎么验证

**编译门禁（最强的单一信号）。** 全反应堆含测试源编译：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T 1C test-compile
```

不得使用任何跳过测试编译的参数。javadoc 改动会被 checkstyle 扫到（本仓库 checkstyle 也扫测试源），因此这一步同时验证注释格式合法。

**单元测试（仅当采纳可选工具方法时）。** 新增 `ConnectorLikePatternsTest`，断言必须编码"为什么"而不只是"是什么"——每个用例的名字直接说明它防的是哪种丢行：

| 模式 | 期望 | 这条测试在防什么 |
|---|---|---|
| `abc%` | `Optional.of("abc")` | 正常前缀仍然可以下推，不能因为守卫过严丢掉优化 |
| `a_c%` | `Optional.empty()` | `_` 被当字面量 → 前缀匹配会漏掉 `abc` |
| `a\%c%` | `Optional.empty()` | 被转义的 `%` 是字面百分号 |
| `%abc%` | `Optional.empty()` | 前置 `%` 不是前缀匹配 |
| `a%b%` | `Optional.empty()` | 中间还有通配符 |
| `abc`（无 `%`） | `Optional.empty()` | 这是等值而非前缀，交给调用方按等值处理 |

**变异验证。** 把 `exactPrefix` 的守卫逐条删掉（先删 `_` 检查，再删转义检查），确认对应用例各自失败。若删掉某个守卫后测试仍全绿，说明该用例没有真的覆盖它。

**端到端回归：本任务不需要。** 改的是注释与一个纯函数，运行时行为不变。真正需要端到端回归的是任务 01 / 02 / 03 的连接器修复（`WHERE c <=> 5` 与 `LIKE 'a_c%'` 在 paimon 表上的行数断言），本任务不要替它们跑。

**验收口径。** ① 上面的 `test-compile` 为 `BUILD SUCCESS`；② 5.2 表格逐行完成；③ 一个没读过这段代码的人只读 `package-info.java`，能独立回答三个问题：`c <=> 5` 该翻成什么、`LIKE 'a_c%'` 能不能翻成前缀匹配、返回 `remainingFilter` 之后引擎会不会替我摘 conjunct。

## 七、风险与回退

风险来自两处，都可控：

- **写错契约比不写更糟。** 契约一旦写下就会被后来的连接器当权威照做。因此本文所有事实性陈述都必须在动手时复核一遍（尤其"引擎实际产出哪些 Java 类型"和"两套残差协议的实际效力"这两段）；凡是没能在代码里坐实的推断，宁可写成"目前由各连接器自行决定"，不要写成规则。
- **可选工具方法有溢出风险。** 它落在公共模块，一旦有连接器调用就成了公共 API。控制手段：只提供一个方法、语义收紧到"要么给出确定的前缀、要么明确说不行"，且不强制任何连接器改用它。

回退：注释改动 `git revert` 即可，无数据、无持久化、无有线格式影响。可选工具类若被判定不需要，单独摘掉它与它的测试即可，其余文档改动不受影响。

## 八、相关背景

- `audit-report.md`「11.1 四个有实际用户可见后果的缺陷」—— 四条会被用户看到的丢行/丢名缺陷：第（3）（4）条是本任务的直接依据，并写明这两个 paimon 缺陷与上游既有实现相同、不是本次迁移引入的回退；第（4）条的行为后果是代码路径推断，未跑端到端验证。
- `audit-report.md` 附录 A.4 第 84 条 —— 两套残差协议只有一套生效：完整证据链。
- `audit-report.md` 第十六节「明确不建议动的部分」第 9 项 —— 两套残差协议不合并、只补文档：这条决定就是本任务的定位。
- 同目录任务 07（写下两个公共模块的设计规则）：本任务是它在 `pushdown` 这一包上的细化；如果 07 先落地，本任务的包级说明应该引用它而不是重述。
- 同目录任务 08（修正陈旧接口文档）：文档改动的分工见 5.3 最后一条。
- 同目录任务 01 / 02 / 03：本任务是这三条的共同根因，它们是本任务契约的第一批"使用者"。
