# 01. 修复 trino 连接器对三路以上 OR 谓词只取前两支导致的丢行

> **优先级**：第一优先级（正确性缺陷，会静默少行） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-trino`（主修）、`fe-connector-api`（构造器加校验与防御性拷贝）
> **预计改动规模**：2 个生产文件 + 2 个测试文件，生产代码净增约 15 行，测试约 60 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

`fe-connector-trino` 把 Doris 的 OR 谓词翻译成 Trino 的 `TupleDomain` 时只读取前两个分支，第三个及以后的分支被静默丢弃，导致 `WHERE a=1 OR a=2 OR a=3` 在数据源侧被收窄成 `WHERE a=1 OR a=2`，查询结果**少行**。

## 二、背景：现在的代码是怎么写的

### 谓词在公共接口里是 N 元的

`fe-connector-api` 的 `ConnectorOr` 明确文档化为「两个或更多分支」，`getDisjuncts()` 返回一个列表：

`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/pushdown/ConnectorOr.java:25-37`

```java
/**
 * Logical OR of two or more disjuncts.
 */
public final class ConnectorOr implements ConnectorExpression {
    private final List<ConnectorExpression> disjuncts;

    public ConnectorOr(List<ConnectorExpression> disjuncts) {
        Objects.requireNonNull(disjuncts, "disjuncts");
        this.disjuncts = Collections.unmodifiableList(disjuncts);
    }
```

fe-core 侧三个生产者都会先把嵌套的 OR **拉平**成一个多元列表再构造 `ConnectorOr`，所以三路 OR 到达连接器时确实是一个持有 3 个元素的 `ConnectorOr`，不是嵌套的两层二元树：

- `fe/fe-core/src/main/java/org/apache/doris/datasource/connector/converter/ExprToConnectorExpressionConverter.java:218-221`（`flattenOr` 后 `new ConnectorOr(disjuncts)`）
- `fe/fe-core/src/main/java/org/apache/doris/datasource/connector/converter/NereidsToConnectorExpressionConverter.java:137-142`
- `fe/fe-core/src/main/java/org/apache/doris/datasource/connector/converter/UnboundExpressionToConnectorPredicateConverter.java:176`

### trino 连接器只读了前两支

`fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoPredicateConverter.java:114-118`

```java
private TupleDomain<ColumnHandle> convertOr(ConnectorOr or) {
    TupleDomain<ColumnHandle> left = doConvert(or.getDisjuncts().get(0));
    TupleDomain<ColumnHandle> right = doConvert(or.getDisjuncts().get(1));
    return TupleDomain.columnWiseUnion(left, right);
}
```

同一个类里紧邻的 `convertAnd`（`:102-112`）是**遍历全部** `getConjuncts()` 的，只有 OR 这一支是取下标。

### 转换结果会真的落到数据源上

转换出的 `TupleDomain` 有两个消费点，两处都会把它交给 Trino 自己的连接器做实际过滤：

| 位置 | 用途 |
| --- | --- |
| `TrinoConnectorDorisMetadata.java:255-297`（`applyFilter`） | 交给 Trino 的 `ConnectorMetadata.applyFilter`，换回一个**已下推谓词的表句柄**，这个句柄随后被序列化进扫描分片发给 BE 的 JNI scanner |
| `TrinoScanPlanProvider.java:262-272`（`buildConstraint`） | 包成 `Constraint` 传给 Trino 的 `applyFilter` 与 `ConnectorSplitManager.getSplits`，直接参与分片裁剪 |

### 兜底重算救不回来

`TrinoConnectorDorisMetadata.java:292-296` 有一段注释说明它会把原始表达式作为「剩余谓词」回传，让 BE 再算一遍：

```java
// Trino tracks the remaining filter as a TupleDomain, not as a Doris ConnectorExpression.
// Returning the original expression keeps BE-side re-evaluation, matching the legacy
// fe-core scan-node behavior.
```

但 BE 的重算只能**再筛掉**行，不可能把源端根本没返回的行补回来。所以这层兜底对本缺陷完全无效。

## 三、为什么这是个问题

1. **这是正确性缺陷，而且是静默的。** 用户看不到任何报错或告警，只是结果行数变少。这类问题往往要等到与其它系统对数才被发现。
2. **实现与公共接口的契约不符。** 接口说自己是 N 元，实现只处理 2 元。八个连接器里只有 trino 这一家是取下标的——`getDisjuncts()` 的全部其它消费者（paimon `PaimonPredicateConverter.java:134`、es `EsQueryDslBuilder.java:267`、maxcompute `MaxComputePredicateConverter.java:152`、iceberg `IcebergPredicateConverter.java:215` 与 `:633`）都在遍历整个列表。
3. **公共类没有把这个约束变成可执行的校验。** `ConnectorOr` 的文档写了「两个或更多」，构造器却只做非空判断；而且它只用 `Collections.unmodifiableList` 包了调用方传进来的列表**视图**（调用方之后改自己的列表，节点内容就会跟着变），而同一个包里的 `ConnectorIn`（`ConnectorIn.java:40-41`）是先 `new ArrayList<>(...)` 真拷贝再包不可变的。同包两种写法不一致。

### 归责：是本次迁移引入的，不是上游既有行为

必须写清，因为处理方式不同：

- **旧实现是对的。** 迁移前 fe-core 的 `TrinoConnectorPredicateConverter`（`git show master:fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/source/TrinoConnectorPredicateConverter.java`，OR 分支在 `:112-115`）也是读 `getChild(0)` / `getChild(1)` 两个孩子，但它的输入是老 Expr 抽象树里的 `CompoundPredicate`——那个类的构造器只接受两个操作数（`git show master:fe/fe-catalog/src/main/java/org/apache/doris/analysis/CompoundPredicate.java:42-50`），三路 OR 在那里是嵌套的二元树，靠递归天然覆盖全部分支。**读两个孩子在旧模型下是完备的。**
- **迁移换了输入模型但没换读法。** 新的 `ConnectorOr` 是拉平后的 N 元列表，照抄「读两个」就漏了。所以这是**移植时引入的回退**，不是忠实搬运上游缺陷。
- **注意**：`master` 上已经有这个带缺陷的文件（`fe/fe-connector/fe-connector-trino/...` 在 `master` 上存在，由本次迁移的早期提交 apache/doris#62183 带入）。但 `master` 上 trino 目录仍走 fe-core 老路径（`fe/fe-core/.../trinoconnector/` 在 `master` 上还在，在本分支已删除），所以**只有本分支上这段代码是线上生效路径**。修复应视为修自己引入的回退，不要在提交信息里描述成上游缺陷。

## 四、用一个最小例子说明

假设一张 trino 目录下的表 `t`，`c_int` 列有 1、2、3 三种值各一行：

```sql
SELECT * FROM trino_catalog.db.t WHERE c_int = 1 OR c_int = 2 OR c_int = 3;
```

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
| --- | --- | --- |
| `c_int = 1 OR c_int = 2 OR c_int = 3` | 下推给数据源的域是 `c_int IN (1, 2)`，源端只返回 2 行；BE 再用原谓词过一遍，仍是 2 行 | 下推的域是 `c_int IN (1, 2, 3)`，返回 3 行 |
| `c_int = 1 OR c_int = 2` | 正确（恰好只有两支） | 同左 |
| `c_int = 1 OR c_int = 2 OR c_int = 3 OR c_int = 4` | 只剩 `IN (1, 2)`，丢 2 行 | `IN (1, 2, 3, 4)` |

结果就是「查得越复杂，丢得越多」，且没有任何提示。

## 五、解决方案

### 5.1 目标状态

**`TrinoPredicateConverter.convertOr` 折叠全部分支。** 逐个转换所有 disjunct，收集成列表后交给 Trino 的 `TupleDomain.columnWiseUnion(List)` 一次性做列级并集：

```java
private TupleDomain<ColumnHandle> convertOr(ConnectorOr or) {
    List<TupleDomain<ColumnHandle>> parts = new ArrayList<>();
    for (ConnectorExpression child : or.getDisjuncts()) {
        // Not caught per-disjunct on purpose: dropping an OR arm narrows the pushed
        // predicate and loses rows. Let the failure propagate so the caller degrades
        // to TupleDomain.all() (no pushdown) instead.
        parts.add(doConvert(child));
    }
    if (parts.isEmpty()) {
        return TupleDomain.all();
    }
    return TupleDomain.columnWiseUnion(parts);
}
```

三个要点：

1. **不要在循环里 catch 单个分支的异常。** `convertAnd`（`:102-112`）catch 并跳过失败的孩子是安全的——少一个 AND 条件只会让下推**变宽**，BE 会补算回来。OR 相反：少一支就是收窄，正是本缺陷的成因。让异常向上抛，由 `convert`（`:74-84`）的 catch 兜到 `TupleDomain.all()`，即「放弃下推、全量扫描」，语义安全。这与 iceberg 的处理方式一致（`IcebergPredicateConverter.java:212` 有一行注释明写 OR 是 all-or-nothing）。
2. **`columnWiseUnion(List)` 用不了空列表。** 实测 trino-spi 435 的 `columnWiseUnion(List)` 在列表为空时抛 `IllegalArgumentException("tupleDomains must have at least one element")`，所以必须留空集合守卫返回 `TupleDomain.all()`。加上 5.1 的构造器校验后这条守卫在实践上不可达，但它是廉价的失败安全兜底，保留。
3. **逐对折叠与一次性并集等价，但仍用 List 重载。** 列级并集里「一个列只有在所有分支都出现时才保留，其域取并集」，逐对折叠与整体计算结果相同；用 `List` 重载只是更直白，也少一次中间对象。

**`ConnectorOr` 构造器补齐契约。** 签名不变，只补校验与真拷贝，与同包 `ConnectorIn` 对齐：

```java
public ConnectorOr(List<ConnectorExpression> disjuncts) {
    Objects.requireNonNull(disjuncts, "disjuncts");
    if (disjuncts.size() < 2) {
        throw new IllegalArgumentException(
                "ConnectorOr requires at least two disjuncts, got " + disjuncts.size());
    }
    this.disjuncts = Collections.unmodifiableList(new ArrayList<>(disjuncts));
}
```

已核实**全部现存生产者都已满足这个前置条件**，加校验不会打破任何调用点：

| 生产者 | 是否可能少于 2 个 |
| --- | --- |
| `ExprToConnectorExpressionConverter.java:218-221` | 不会。入参已判定是 OR 型 `CompoundPredicate`，`flattenOr`（`:240-248`）对两个孩子各产出至少一个节点，`convert` 走不通时会 `fallback`（`:342`）返回 SQL 片段节点而非 null |
| `NereidsToConnectorExpressionConverter.java:142` | 不会，已有 `disjuncts.size() == 1 ? disjuncts.get(0) : ...` 的三元判断 |
| `UnboundExpressionToConnectorPredicateConverter.java:176` | 不会，同上 |
| 各连接器与 fe-core 的测试用例 | 已核实全部传 2 个以上 |

### 5.2 改动清单

| 文件 | 做什么 |
| --- | --- |
| `fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoPredicateConverter.java` | 重写 `convertOr`（`:114-118`）为遍历全部 disjunct + `columnWiseUnion(List)` + 空集合守卫；不要加 per-disjunct 的 try/catch，并留一行注释说明「OR 少一支等于收窄」这个理由 |
| `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/pushdown/ConnectorOr.java` | 构造器（`:34-37`）加「至少两个分支」校验 + `new ArrayList<>(disjuncts)` 防御性拷贝；`import java.util.ArrayList` |
| `fe/fe-connector/fe-connector-trino/src/test/java/org/apache/doris/connector/trino/TrinoPredicateConverterTest.java` | 在现有 `testOrUnionsSameColumn`（`:215-224`）旁新增三路与四路 OR 用例，并补一个跨列 OR 用例与一个含不可转换分支的 OR 用例（见第六节） |
| `fe/fe-connector/fe-connector-api/src/test/java/org/apache/doris/connector/api/pushdown/ConnectorOrTest.java`（新建） | 校验构造器拒绝 0/1 个分支、接受 2 个以上、以及「构造后修改调用方原列表不影响节点内容」 |

### 5.3 明确不要顺手做的事

- **不要给 `ConnectorAnd` 一起加校验和拷贝。** 它是同样的浅包裹形状（`ConnectorAnd.java:34-37`），但已核实它的所有生产者（`WriteConstraintExtractor.java:86`、两个 Nereids 转换器、`ExprToConnectorExpressionConverter`）都已经守住了「只剩一个就不包 AND」，今天没有实际风险。把它一起改会让这次提交从「修一个会丢行的缺陷」变成「顺带整理公共类」，稀释了变更意图，也需要另做一遍全生产者核对。留给后续的一致性清理。
- **不要顺手实现「剩余谓词」的精细化。** `TrinoConnectorDorisMetadata.java:292-296` 的注释里提到「未来可以把剩余 TupleDomain 映射回 ConnectorExpression 并清掉已完全下推的条件」。那是性能优化（少一次 BE 重算），与本缺陷无关，而且清错了就是另一次丢行。
- **不要动 `convertAnd` 的「catch 并跳过」写法。** 它对 AND 是安全的（下推变宽），改成 all-or-nothing 只会降低下推率。
- **不要在 fe-core 侧做任何改动。** fe-core 现阶段只出不进，这个缺陷完全在连接器与公共接口类里，无需碰引擎。
- **不要写脚本或正则门禁去检查「是否遍历了 getDisjuncts()」。** 那是语言语义判断，本仓库已有结论：这类静态门禁误报比漏报更毒。用单元测试锁住行为即可。

## 六、怎么验证

### 单元测试要断言什么

在 `TrinoPredicateConverterTest` 里新增（现有 `CONVERTER` / `col()` / `expect()` / `singleValue()` 辅助方法直接可用，见 `:89-107`）：

1. **三路同列 OR** —— `c_int=1 OR c_int=2 OR c_int=3` 必须得到 `c_int` 上含三个等值 range 的域。**这条在修复前必须失败**（改代码前先跑一次确认它红，这是本任务唯一的变异验证要求：如果它在旧代码上就绿，说明用例没打到缺陷）。
2. **四路同列 OR** —— 证明修的是「全部分支」而不是把 2 改成 3。
3. **三路跨列 OR** —— 例如 `c_int=1 OR c_bigint=2 OR c_str='x'`，列级并集下没有任何列在所有分支中都出现，结果必须是 `TupleDomain.all()`（放弃下推、不收窄）。这条锁住「不要为了下推而编造约束」。
4. **含不可转换分支的 OR** —— 例如 `c_int=1 OR c_int=2 OR <一个裸列引用>`，结果必须是 `TupleDomain.all()`，**不能**是 `c_int IN (1,2)`。这条正面锁住 5.1 里「不要 per-disjunct catch」的决定，是防止本缺陷以另一种形态复活的关键用例。
5. **保留现有的两路 OR 用例不改**，作为不回退的基线。

在新建的 `ConnectorOrTest` 里断言：0 个和 1 个分支抛 `IllegalArgumentException`；2 个及以上正常；构造后往调用方原 `ArrayList` 里再 `add` 一个分支，`getDisjuncts()` 的大小不变（证明是真拷贝）。

### 命令

单模块测试（必须禁用 maven build cache，否则 surefire 会被静默跳过而报 BUILD SUCCESS）：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-trino,fe-connector/fe-connector-api -am \
    -Dmaven.build.cache.enabled=false \
    -Dtest=TrinoPredicateConverterTest,ConnectorOrTest -DfailIfNoTests=false test
```

编译门禁（最强的单一信号，验收必跑；`ConnectorOr` 加了校验，要靠它确认没有测试源在别处构造单分支 OR）：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -Dmaven.build.cache.enabled=false test-compile
```

不要加任何跳过测试编译的参数。checkstyle 会扫测试源，新增文件要过。

### 端到端回归

已有 `regression-test/suites/external_table_p0/trino_connector/jdbc/` 下的用例（如 `test_trino_pg.groovy`）跑的是真实 trino 目录，可以在其中一个里补一条三路 OR 的查询与结果断言。**这一步需要 docker 外部环境，本 session 通常跑不了**——补了用例后如实标注「未在本地执行」，不要声称已通过。单元测试已经能覆盖转换逻辑本身，端到端只是额外保险。

## 七、风险与回退

- **风险很低。** 生产改动是一个私有方法的循环化，加上一个公共构造器的前置校验。改动方向是「让下推更完整」，不会产生新的收窄。
- **唯一需要留意的是新加的构造器校验会硬抛异常。** 已逐一核对全部生产者与测试用例都传 2 个以上（见 5.1 的表），并且 `mvn test-compile` + 全量单测能把遗漏暴露出来。若后续有人在别处构造单分支 OR，会立刻在构造点抛错而不是静默降级——这是刻意选择：单分支 OR 是调用方的逻辑错误，应当被发现。
- **下推变完整后，某些查询扫到的数据会比修复前多。** 这不是回退，是本来就该读的数据。但如果有依赖旧（错误）行数的回归用例基线，需要更新基线而不是回滚修复。
- **回退方式**：两个文件各自独立可回滚。若只想回退构造器校验、保留转换器修复，也是可行的——转换器的修复不依赖校验。

## 八、相关背景

- 调研报告 `plan-doc/connector-public-interface-cleanup/audit-report.md`：
  - 第十一节 11.1 第（1）条 —— 三路以上 OR 在源端丢行，本任务对应条目；同一小节末尾的归责段 —— 四条缺陷分别归谁（那里把本条归为「trino 连接器的实现问题」，本文进一步核实为「迁移引入的回退」）。
  - 附录 A.6 第 114 条 —— 原始判定与符号定位。
  - 第十五节整治路线表第 6 项 —— 修四个真实缺陷的排期；附录 C.4 第 1 条 —— 全部材料里唯一可能静默少数据的问题。
- 同一批缺陷里的相邻项（各自独立任务，不要合并进本次提交）：复杂类型字段名被丢弃（审计 11.1 第（2）条 / 附录 A.6 第 116 条 —— 引擎编造替代字段名）、paimon 的空值安全比较被译成 `IS NULL`（审计 11.1 第（3）条 —— `<=>` 被译成 IS NULL）。
- 正确处理 N 元 OR 的可参考实现：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergPredicateConverter.java:212-223`（含「OR 是 all-or-nothing」的注释）。
