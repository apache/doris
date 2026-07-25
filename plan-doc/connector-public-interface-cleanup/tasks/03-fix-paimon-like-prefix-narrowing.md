# 03. 修复 paimon 连接器把含单字符通配符或转义的 LIKE 收窄成前缀匹配

> **优先级**：第一优先级（正确性缺陷，会静默少行） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe/fe-connector/fe-connector-paimon`（主改动 + 单元测试）；`regression-test`（新增一个端到端用例）。**不动** `fe-connector-api`，**不动** `fe-core`。
> **预计改动规模**：生产代码 1 个文件、约 20 行；单元测试 1 个文件、约 80 行；端到端用例 1 个 groovy 文件、约 70 行。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

paimon 连接器在把 Doris 的 `LIKE` 谓词下推给 Paimon 时，只看「模式串是不是以 `%` 结尾」就当成前缀匹配，完全没有检查模式串里有没有单字符通配符 `_`、有没有反斜杠转义、有没有夹在中间的 `%`；这三种情况下下推出去的谓词都比原谓词更严格，Paimon 会据此跳过本该被读到的数据文件，查询**静默少行**。本任务把这个下推收紧成「只有能证明等价时才下推，否则放弃下推」。

## 二、背景：现在的代码是怎么写的

Doris 把查询过滤条件翻成连接器可消费的表达式树（`ConnectorExpression`），`LIKE` 会变成 `ConnectorLike`。paimon 连接器用 `PaimonPredicateConverter` 把这棵树翻成 Paimon SDK 的 `Predicate`。

出问题的就是这个转换方法，`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonPredicateConverter.java:233-238`：

```java
String pattern = ((ConnectorLiteral) patternExpr).getValue().toString();
if (!pattern.startsWith("%") && pattern.endsWith("%")) {
    String prefix = pattern.substring(0, pattern.length() - 1);
    return builder.startsWith(idx, BinaryString.fromString(prefix));
}
return null;
```

即：**只要模式串不以 `%` 开头、且以 `%` 结尾，就把「去掉最后一个字符」的结果当作字面前缀**交给 Paimon 的 `startsWith`。除此之外的模式串（同一方法的 `:238`）返回 `null`，表示放弃下推——这部分是对的。

转换出来的谓词有两个消费点，都在 `PaimonScanPlanProvider.java`：

- FE 规划期：`:488` 调转换器，`:506` `readBuilder.withFilter(predicates)`。Paimon SDK 在 `newScan().plan()` 里用这些谓词做分区裁剪与数据文件裁剪（该类的类注释 `:120-125` 明确说明 paimon 是「纯谓词驱动」的裁剪，连引擎给的分区集都不消费）。
- BE 读取期：`:780-783` 再转换一遍，序列化进 `paimon.predicate` 交给 BE 的 Paimon JNI scanner，做行级过滤。

两条路都意味着：**下推的谓词一旦比原谓词严格，被裁掉的文件/被过滤掉的行 BE 侧再也补不回来**（BE 上仍挂着原始 `LIKE` 过滤，但它只能过滤已经读到的行，不能把跳过的文件读回来）。

两条相关事实也已核对：

- `ConnectorLike` 的公共契约总共只有一句话——「A LIKE/REGEXP predicate: `value LIKE pattern`」（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/pushdown/ConnectorLike.java:24-26`），枚举只有 `LIKE` / `REGEXP` 两个值（`:32-35`）。**没有任何地方约定转义字符是什么、`%` 与 `_` 的方言、正则是部分匹配还是整串锚定、大小写敏感性，也没有约定「翻不精确就必须放弃下推」。** 契约缺失是这类实现错误能活下来的土壤。
- Doris 的 `LIKE` 默认转义字符是反斜杠：BE 侧实现 `be/src/exprs/function/like.cpp` 的快速路径正则 `:58` 明确把 `\%` 与 `\_` 当成转义后的字面量处理，并在 `:815` 的 `remove_escape_character` 里去掉转义再做前缀/后缀/子串匹配；自定义 `ESCAPE` 走 `:980` 之后的 `has_custom_escape` 分支。也就是说 BE 自己**是**做了 paimon 这里缺的那层守卫。

**归责**：这不是本次迁移引入的回退。`git show master` 对比确认，老的 fe-core 实现 `master:fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonPredicateConverter.java:164-165` 是完全相同的写法（`name.equals("like") && !s.startsWith("%") && s.endsWith("%")` → `startsWith(s.substring(0, s.length() - 1))`）。属于上游既有缺陷的忠实移植。

## 三、为什么这是个问题

违反的原则只有一条，但很硬：**谓词下推只允许放宽，不允许收窄。** 引擎把过滤条件交给数据源，是为了让数据源少读数据；数据源如果把条件理解得比原意更严格，就会跳过本该返回的数据，而引擎无法察觉——因为返回的行数看起来「合理」，只是少了。

用户能观察到的现象：**同一张 paimon 表、同一条带 `LIKE` 的 SQL，结果比正确答案少行**，且没有任何报错、日志或 `EXPLAIN` 提示。切到别的引擎（Spark 读同一张 paimon 表）结果不一致。这类问题在生产上极难定位，因为它不崩、不慢、只是答案不对。

具体有三种模式串会踩中（第三种是本次核实时新发现的，最初那轮调研里没有）：

1. **含单字符通配符 `_`**：`LIKE 'a_c%'` 里 `_` 应匹配任意一个字符，被当成字面下划线 → 下推 `startsWith("a_c")` → 真正含 `abc…` 的文件被裁掉。
2. **含反斜杠转义**：`LIKE 'a\%%'` 的原意是「以字面量 `a%` 开头」，前缀应为 `a%`，但代码原样取到 `a\%`（带反斜杠）→ 下推 `startsWith("a\\%")` → 一行都匹配不上，结果可能直接空集。
3. **`%` 夹在中间**：`LIKE 'a%b%'` 不以 `%` 开头、以 `%` 结尾 → 代码取前缀 `a%b` 并当成字面串 → 下推 `startsWith("a%b")`。这是同一个漏洞的第三种表现，修法相同。

一个附带确认（不用改）：`NOT LIKE` 不受影响。fe-core 把 `NOT` 翻成 `ConnectorNot`（`ExprToConnectorExpressionConverter.java:223-224`），而 paimon 的 `convertSingle`（`PaimonPredicateConverter.java:105-118`）没有 `ConnectorNot` 分支，直接返回 `null` 放弃下推。带 `ESCAPE` 子句的三参数 `LIKE` 也进不来（`ExprToConnectorExpressionConverter.java:117` 要求恰好两个子表达式）。

**诚实标注**：上述「少行」是从代码路径推断出来的，**尚未跑端到端验证**。所以本任务的第一步就是先写出一个能复现的端到端用例，先看到红，再动生产代码。

## 四、用一个最小例子说明

准备一张 paimon 表，两行数据，**分两次 insert**（这样两行落在两个不同的数据文件里，文件级统计就能触发裁剪）：

```sql
-- 数据：第一个文件只有 'abc1'，第二个文件只有 'a_c1'
-- 查询（_ 是通配符，应该同时命中两行）
SELECT s FROM paimon_tbl WHERE s LIKE 'a_c%' ORDER BY s;
```

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
| --- | --- | --- |
| `s LIKE 'a_c%'` | 下推 `startsWith("a_c")` → 只有 `'a_c1'` 所在文件被读，返回 1 行 | `_` 是通配符，两行都该返回：`a_c1`、`abc1` |
| `s LIKE 'a\%%'`（找以字面 `a%` 开头的值） | 下推 `startsWith("a\%")`（多了个反斜杠）→ 可能返回 0 行 | 返回所有以 `a%` 开头的行 |
| `s LIKE 'a%b%'` | 下推 `startsWith("a%b")`（`%` 被当字面量）→ 少行 | 返回以 `a` 开头、中间有 `b` 的行 |
| `s LIKE 'abc%'`（唯一安全的形态） | 下推 `startsWith("abc")` | 不变，继续下推 |

修完之后的判断逻辑，用伪代码表达就是这么几行：

```
若 pattern 含 '_' 或含 '\'        -> 不下推（返回 null）
去掉 pattern 末尾连续的 '%'，得到 body
若 body 为空，或 body 里还有 '%'  -> 不下推
否则                              -> startsWith(body)
```

「含反斜杠就整体放弃」这条看起来粗，但它同时保证了「去掉末尾 `%`」不会误剥一个被转义的 `%`——因为到那一步已经确定串里没有反斜杠了。宁可少下推几个模式串，也不能下推错。

## 五、解决方案

### 5.1 目标状态

`PaimonPredicateConverter.convertLike` 只在**能证明等价**时才产出 `startsWith`，其余一切模式串返回 `null`（走不下推、由 BE 全量过滤，慢但正确）。不新增 SPI、不新增能力位、不改公共接口签名，只在连接器内部收紧一个判断。

建议把判断抽成同类里的一个私有静态方法，便于单测直接打：

```java
/**
 * Returns the literal prefix a Doris LIKE pattern is equivalent to, or null when the
 * pattern cannot be proven equivalent to a prefix match. Declining is always safe;
 * narrowing is not (Paimon prunes files from this predicate and BE cannot recover them).
 */
private static String literalPrefixOrNull(String pattern)
```

`convertLike` 里的调用形态：

```java
String prefix = literalPrefixOrNull(pattern);
if (prefix == null) {
    return null;
}
return builder.startsWith(idx, BinaryString.fromString(prefix));
```

### 5.2 改动清单

| 文件 | 做什么 |
| --- | --- |
| `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonPredicateConverter.java` | 新增私有静态 `literalPrefixOrNull(String)`；`convertLike`（`:218-239`）里把 `:234-237` 那段替换为调用它。方法注释写清「Doris LIKE 默认转义符是反斜杠；不可精确翻译必须放弃下推，不得收窄」，并说明为什么收窄在这里是致命的（谓词同时用于 Paimon 文件裁剪与 BE JNI 行过滤，见 `PaimonScanPlanProvider:506` 与 `:783`）。 |
| `fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/PaimonPredicateConverterTest.java` | 新增一组 LIKE 用例（该文件目前**一个 LIKE 用例都没有**，已核实 grep 无命中）。见第六节的断言清单。 |
| `regression-test/suites/external_table_p0/paimon/`（新增一个 groovy 文件，例如 `test_paimon_like_pushdown.groovy`） | 端到端复现 + 回归。用 `spark_paimon_multi`（框架方法在 `regression-test/framework/.../Suite.groovy:1692`）建表并**分多次 insert** 制造多个数据文件，然后断言 `LIKE` 查询的结果集。可参考同目录 `test_paimon_partition_schema_filter_refs.groovy:130-160` 的建表/写入写法，catalog 属性抄 `test_paimon_predict.groovy:30-38`。 |

### 5.3 明确不要顺手做的事

- **不要去补 `ConnectorLike` 的公共契约文档/校验。** 「把逐算子语义与『不可精确翻译必须放弃下推』写进公共契约」是另一份任务的范围（见 audit-report.md 11.1 节末尾那段结论）。两边同时动 `fe-connector-api` 会互相冲突，本任务只修连接器实现。
- **不要顺手修 es 那处同根因的问题。** 详见第八节：`EsQueryDslBuilder.java:512-522` 把 Doris 的 `REGEXP` 模式原样交给 ES 的 `regexp` 查询，锚定语义不同。是同一个根因的第二处，但涉及 ES 侧语义与另一套端到端环境，**是否同批修由排期决定**，不要塞进本任务。
- **不要顺手扩大下推能力**（比如给 `%abc` 加后缀匹配、给 `%abc%` 加子串匹配）。那是性能增强，不是本任务的正确性修复，且要先核实所用 Paimon SDK 版本是否真有对应的 `endsWith` / `contains` 构造器。混在一起会让「这次到底修了什么」说不清。
- **不要试图在连接器里建模排序规则与大小写敏感性。** 现状是 Doris `LIKE` 与 Paimon `startsWith` 都按字节比较，本任务不引入这个维度。
- **不要顺手处理不含任何通配符的 `LIKE 'abc'`。** 现状是不下推（等价于 `=`，是个可下推机会），但那是增强不是修 bug。
- **不要往 fe-core 加任何东西。** 当前阶段 fe-core 只出不进，这个修复完全在插件内部可解。

## 六、怎么验证

**第一步（先看到红）**：先写端到端用例并跑通「现在是错的」。用例形态：建一张带字符串列的 paimon 表，分三次 insert 分别写入 `abc1`、`a_c1`、`a%b1`，然后断言

- `WHERE s LIKE 'a_c%'` 返回 `a_c1` 与 `abc1` 两行（修复前预计只返回 1 行）；
- `WHERE s LIKE 'a\\%b%'` 返回 `a%b1`（修复前预计 0 行）；
- `WHERE s LIKE 'a%b%'` 返回 `a%b1` 与 `abc1`（`abc1` 里 `a` 后有 `b`）；
- `WHERE s LIKE 'abc%'` 仍返回 `abc1`（证明安全形态的下推没被误伤）。

如果某条在修复前就是绿的，**必须在实施记录里写清哪条没能复现**，不要默认「三条都复现」。文件级裁剪是否触发依赖 Paimon 的统计与读取路径（native raw-file 读 vs JNI 读），分文件写入是为了让裁剪确定触发；若仍不复现，改用 JNI 读路径的表（例如带 deletion vector 的表）再试一次。

**第二步：单元测试**（`PaimonPredicateConverterTest`）。断言的是「下推出来的 Paimon 谓词是什么」，不需要集群：

| 输入模式串 | 期望 |
| --- | --- |
| `abc%` | 产出前缀 `abc` 的 `startsWith` |
| `abc%%` | 产出前缀 `abc` 的 `startsWith` |
| `a_c%` | **不下推**（转换结果为空列表） |
| `a\%%` | **不下推** |
| `a\_c%` | **不下推** |
| `a%b%` | **不下推** |
| `%abc%` / `%abc` / `abc` | 不下推（现状行为，作为回归护栏钉住） |
| `%` | 不下推 |

测试要按 Rule 9 的要求把「为什么」写进注释：断言的不是「返回 null」这个实现细节，而是「翻不精确时必须放弃下推，因为收窄会让 Paimon 跳过文件而 BE 补不回来」。

**变异验证**（推荐做，成本很低）：把新方法里的 `_` 检查单独注释掉，确认 `a_c%` 那条单测转红；再恢复。证明这些测试不是「永远绿」的空壳。

**编译门禁**：全反应堆含测试源的 test-compile 是最强单一信号。

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -Dmaven.build.cache.enabled=false
```

不得使用任何跳过测试编译的参数。跑单测：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test -pl fe-connector/fe-connector-paimon -am \
    -Dtest=PaimonPredicateConverterTest -DfailIfNoTests=false -Dmaven.build.cache.enabled=false
```

`-Dmaven.build.cache.enabled=false` 不是可选项：不加它 surefire 会被 build cache 静默跳过，日志出现 `Skipping plugin execution (cached): surefire:test`，此时 `BUILD SUCCESS` 是空的（见 `plan-doc/HANDOFF.md` 构建坑第 1 条）。另外注意 `mvn ... | tail` 之后的 `$?` 是 `tail` 的退出码，要读日志里的 `BUILD SUCCESS` / `BUILD FAILURE` 行。

## 七、风险与回退

- **功能风险：低。** 改动只把「下推」变成「不下推」，不下推的语义永远是安全的（BE 侧仍有原始 `LIKE` 过滤），不会产生错误结果。
- **性能风险：小而真实。** 原本被错误下推的那几种模式串（`_`、转义、中间 `%`）今后不再裁剪文件，这类查询会多读数据。这是把「快但错」换成「慢但对」，方向上没有争议；受影响的只是这三类模式串，最常见的 `'前缀%'` 形态完全不受影响。
- **兼容性风险：无。** 不涉及 Gson 持久化类型标签、不涉及 thrift 有线格式、不改公共接口签名，插件独立打包也不影响 fe-core。
- **回退**：单文件单方法改动，`git revert` 即可。端到端用例可独立保留（它断言的是正确行为，回退生产代码后会转红，这正是它该做的）。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md` 11.1 节第（4）条：本任务的出处，含「行为后果是代码路径推断、未跑端到端验证」的原始标注，以及「（3）（4）是 paimon 连接器的问题，且与上游既有实现完全相同」的归责结论。
- 同一节第（3）条：paimon 把空值安全比较 `列 <=> 5` 下推成「该列 IS NULL」，同样是「不可精确翻译却收窄」的错误，同样在 `PaimonPredicateConverter` 里。**是另一份任务**，但如果两个任务由同一人连着做，可以合并成一次端到端回归跑。
- 同一根因的第二处（本任务范围之外）：`fe/fe-connector/fe-connector-es/src/main/java/org/apache/doris/connector/es/EsQueryDslBuilder.java:512-522` 把 Doris 的 `REGEXP` 模式串原样交给 ES 的 `regexp` 查询（`:522` 调 `regexpQuery`，`:661` 是其实现）。Doris 的 `regexp` 是部分匹配、Lucene 的 `regexp` 是整串锚定，语义不同。值得一提的是 ES 那侧本来就有干净的「拒绝下推」机制（`notPushDownList`），修起来有落点。
- audit-report.md 第十五节整治路线表第 6 项 —— 修四个真实缺陷的排期，把这批缺陷归为一组，理由是「有用户可见后果（其中三条会静默少行），不应排在设计整治后面」。
