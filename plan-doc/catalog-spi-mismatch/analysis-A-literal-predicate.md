# 类别 A — Literal / 谓词粒度

**范围说明**：本类别聚焦 SPI 谓词/字面量表示层的忠实度问题——即 fe-core 边界把 Nereids 谓词转成连接器可消费的 `ConnectorLiteral` / `ConnectorDomain` 时，是否丢失了 canonical 文本或类型语义，进而导致各连接器分区剪枝失配。本类别共 2 条发现：#1（ConnectorLiteral 无 typed getStringValue）、#23（ConnectorDomain 无 CHAR padding）。

**基线声明**：本核实基于**当前工作树** branch `catalog-spi-2-lvl-cache`，逐条独立读当前代码（Read/Grep，非 `git show` 旧分支），并经初查（investigate）+ 对抗复核（verify）两阶段。最终结论以 verify 的 `final_verdict` 与 `corrections` 为准修正初查草稿。凡结论均引 file:line。

---

## 总表

| # | 严重 | 原报告结论 | 核实结论 | 一句话 |
|---|------|-----------|---------|--------|
| 1 | 🔴 | 现行 hive bug 根因：ConnectorLiteral 无 typed getStringValue，hive 用 String.valueOf 推错 canonical string | **PARTIAL**（high） | 架构缺口（无 typed 访问器→N 份分叉重建）属实且仍在；但把 hive 标为「现行错、根因」已过时（hive 早已修）；真正未对齐的是 hudi 的 decimal 分支 |
| 23 | 🟡 | ConnectorDomain 用 Comparable 建模、无 CHAR(n) 定宽 padding 语义，无法忠实表达 CHAR 比较 | **CONFIRMED**（medium） | 字面成立，但该抽象是彻底死代码（columnDomains 恒空、零消费者），当前对查询零影响，实际严重度低于「潜伏」 |

---

## #1 ConnectorLiteral 无 typed getStringValue()

**核实结论**：**PARTIAL**（置信度 high）。两阶段一致判 PARTIAL，verify 完全同意 stage-1 的核心事实并补充了 paimon / Boolean / DATE 三处细节，未推翻任何结论。最终以 verify 为准（对 stage-1 的补强而非分歧）。

**原报告主张**：SPI 的 `ConnectorLiteral` 只揣裸 `Object` + `toString()`，没有 typed `getStringValue()`，逼每个连接器自己从 Java 值重推 canonical string；hive 用 `String.valueOf(val)` 推错（datetime 出 ISO `T`、decimal 出保 scale/科学计数），iceberg 手写 `dorisDateTimeString()` + bool `1/0` 推对；这是 hudi/paimon/hive 一系列分区匹配 bug 的共同 SPI 根因。

**核实过程**（融合 investigate.code_reality + verify.recheck_notes，均按当前工作树亲验）：

1. `ConnectorLiteral.java`（fe-connector-api/.../pushdown/ConnectorLiteral.java:33-119）确实只存 `ConnectorType type` + 裸 `Object value`；访问器只有 `getType()`(81)、`getValue()`(86)、`isNull()`(90)；`toString()` 即 `value.toString()`(100-102)。工厂 `ofDatetime/ofDate/ofDecimal`(69-79) 只塞 `LocalDateTime/LocalDate/BigDecimal`，**不带 canonical 文本**。全类无任何 typed `getStringValue()`/canonical 访问器。**结构主张成立**。
2. `ExprToConnectorExpressionConverter.convertDateLiteral`（fe-core/.../converter/ExprToConnectorExpressionConverter.java:302-322）：DATE/DATEV2 分支存裸 `LocalDate`(314)，否则 DATETIME 存裸 `LocalDateTime`(316-320)。边界处 `DateLiteral` 本可给 canonical 文本，但被丢弃。**「边界丢 canonical」主张成立**。
3. `IcebergPredicateConverter`（fe-connector-iceberg/.../IcebergPredicateConverter.java:310-360, 507）：Boolean→`"1"/"0"`(322)、LocalDate 分支(326)、LocalDateTime→`dorisDateTimeString`(342)、BigDecimal 分支(348)。**iceberg 手写推对，成立**。
4. 【关键反证】`HiveConnectorMetadata.extractLiteralValue`（fe-connector-hive/.../HiveConnectorMetadata.java:2246-2270）：LocalDateTime→`hiveDateTimeString()`(2259)、BigDecimal→`stripTrailingZeros().toPlainString()`(2267)、`String.valueOf(val)`(2269) 仅作标量兜底。注释 2255-2266 逐字记录了旧 `String.valueOf` 曾把整表剪到 0 行的 bug、并说明已改为 Hive-canonical 渲染。`git log -S 'stripTrailingZeros().toPlainString()'` 仅命中 commit `3593684715f`（hive 连接器引入）。**「hive 现行错」确已过时**。
5. `HudiConnectorMetadata.extractLiteralValue`（fe-connector-hudi/.../HudiConnectorMetadata.java:1092-1108）：仅 LocalDateTime→`hiveDateTimeString`(1105)，**无 BigDecimal 分支**，decimal 走 `String.valueOf`(1107)。`git log -S stripTrailingZeros -- fe-connector-hudi/` 零命中，证明 hive 的 decimal 修复**从未同步到 hudi**。残留成立。
6. （verify 补查，stage-1 未做）`PaimonPredicateConverter.convertLiteralValue`（fe-connector-paimon/.../PaimonPredicateConverter.java:244-290）：把 value 转成 Paimon **原生 typed 对象**（BigDecimal→`Decimal.fromBigDecimal`:266-268、LocalDateTime→Paimon Timestamp:286-287），再构造原生 Predicate，**不走 canonical 字符串比较路径**。

**背景**：SPI 的 `ConnectorLiteral` 是「裸 Object + toString」，没有 typed 访问器。canonical 文本在 fe-core 边界（`convertDateLiteral`）本可从 `DateLiteral` 拿到却被丢弃，只存 `LocalDateTime/LocalDate`。后果是每个连接器必须各自从 Java 值重建 Hive/Doris canonical 字符串，形成 divergent fill：iceberg 一套（推对）、hive 一套、hudi 一套、paimon 又用原生 typed 对象一套。「缺 typed 访问器 → N 份重复重建 → 易分叉」的设计缺口**客观存在**，报告对它的方向性观察准确且有价值。

**为何降为 PARTIAL（主张过时之处）**：报告把 hive 明确标为「现行错、根因」，称 `extractLiteralValue` 用 `String.valueOf` 推错。但当前工作树中该方法已修（见核实过程 4）——LocalDateTime 走 `hiveDateTimeString()` 输出 `2024-01-01 10:00:00`（空格分隔、补秒）而非 ISO `2024-01-01T10:00`；BigDecimal 走 `stripTrailingZeros().toPlainString()` 输出 `1`（去尾零、避科学计数）而非保 scale 的 `1.0000`；`String.valueOf` 仅对 Boolean/Int/Long 兜底，而这些类型 `String.valueOf` 结果恰是 Hive canonical（`true/false`、纯整数），不产生剪 0 行。因此报告基于「几天前代码」所述的 hive 活跃 bug 在当前代码已不存在。故非 CONFIRMED，也非 STALE_FIXED（架构缺口与 hudi 残留仍在）。

**影响**（基于当前代码的真实可复现面）：

- **hive：无**。datetime/decimal 分区列做 WHERE 等值/IN 剪枝已正确。例：`SELECT * FROM hive_tbl WHERE dt_part = '2024-01-01 10:00:00'` 或 `dec_part = 1.0`，当前会命中而非剪到 0 行。
- **hudi：decimal 分区列仍有潜在剪枝失配**。具体示例：hudi 表以 `decimal(8,4)` 列 `d` 分区、存储分区值字符串为 `"1"`，查询 `WHERE d = 1` —— Nereids 传入 BigDecimal `1.0000`，`extractLiteralValue` 走 `String.valueOf`(1107) 得 `"1.0000"`，与存储 `"1"` 字符串不等 → `matchesPredicates` 失配、该分区被**误剪**（本应命中的行返回空）。datetime 分区 hudi 已修（1105），不受影响。此残留的运行时可复现性依赖「hudi 用 decimal 列分区 + matchesPredicates 走字符串等值比较」前置，故措辞为「潜在失配」而非确证运行时 bug。
- **DATE(LocalDate) 路径**：hive/hudi 均走 `String.valueOf`→`2024-01-01`（ISO 即 Hive canonical date），**无 bug**。
- **Boolean 路径**：hive/hudi 的 `String.valueOf` 对 Boolean 产出 `true`/`false`，恰是 Hive 布尔分区 canonical，**无 bug**。iceberg 需 `1`/`0` 是其对接 `BoolLiteral.getStringValue()` 语义（IcebergPredicateConverter:321），两者要求不同、各自都对。故原报告笼统称「Boolean 出 true/false 不等于 canonical」在 hive 语境下不成立。
- **paimon 校准**：paimon 不走 canonical-string 路径（原生 typed 对象），不存在 string-compare 失配。原报告把 paimon 归入「同一 canonical-string 分区 bug」对 paimon **不准确**；但 paimon 恰是第 4 份各自为政的字面量重实现，反而更强化「缺 typed SPI 访问器 → N 份分叉」的架构论点。建议把 paimon 从「受害连接器」改列为「另一种分叉规避方式」。

**修复方案**（超出本条主张范围，二选一）：

1. 若只消除残留：把 hive 的 BigDecimal(`stripTrailingZeros().toPlainString()`) 分支镜像进 `HudiConnectorMetadata.extractLiteralValue`（HudiConnectorMetadata.java:1107 之前），与其已镜像的 LocalDateTime 分支一致。小步、低风险。
2. 若根治架构分叉（工作量大、涉铁律 A「fe-core 只出不进」）：**不**在 fe-core 加 helper，而是在 `ConnectorLiteral` 上增设各连接器可复用的 canonical 渲染入口，或在 `ExprToConnectorExpressionConverter` 边界透传 `DateLiteral` 的 canonical 文本，消除三处（iceberg/hive/hudi）重复重建。此项应交 review 定夺，不宜在核实中擅动。

---

## #23 ConnectorDomain 无 CHAR padding

**核实结论**：**CONFIRMED**（置信度 medium）。两阶段一致；verify 完整复读 `ConnectorRange`/`ConnectorFilterConstraint` 并确认死代码结论，`corrections` 为空。字面主张成立，但因目标类是死代码，**实际严重度低于报告标注的「潜伏」**。

**原报告主张**：`ConnectorDomain` 用 `Comparable` 建模谓词值，但没有 CHAR(n) 定宽 padding 语义；CHAR(n) 定宽比较（尾部空格填充）无法忠实表达。

**核实过程**（融合 investigate.code_reality + verify.recheck_notes）：

1. `ConnectorDomain`（fe-connector-api/.../pushdown/ConnectorDomain.java:33-116）持有 `ConnectorType type` + `List<ConnectorRange> ranges` + `boolean nullsAllowed`；值在 `ConnectorRange`（ConnectorRange.java:34,37）中以 `Comparable<?> low/high` 存储。`equal()`(54-57) 裸塞值不归一化，`isSingleValue()`(109-112) 用 `low.equals(high)` 精确比较，`equals()`(125-138) 精确比较。**全类无任何按类型/长度做尾空格 padding 或截断的逻辑**——`type` 仅参与 equals/hashCode/toString，不驱动 CHAR(n) 定宽比较。**字面主张成立**。
2. 整条 `ConnectorDomain/ConnectorRange` 抽象在当前工作树是**死代码**：grep `new ConnectorDomain|ConnectorDomain.(all|none|singleValue|onlyNull)|getColumnDomains|columnDomains`，排除 `ConnectorDomain.java`/`ConnectorFilterConstraint.java` 后**零命中**——从不被构造、从不被读取。
3. 唯一生产侧构造 `ConnectorFilterConstraint` 的点 `PluginDrivenScanNode.java:1941`（经 `buildFilterConstraint`:1939）传 `Collections.emptyMap()` 给 columnDomains，故 domain map 恒空。
4. 三个消费者 `HiveConnectorMetadata.applyFilter`（HiveConnectorMetadata.java:1103,1132）、`HudiConnectorMetadata`（:264,313）、`TrinoConnectorDorisMetadata`（:260）**全部只读 `constraint.getExpression()`**（表达式树，走 ConnectorLiteral 路径），无一调用 `getColumnDomains()`。
5. `getColumnDomains|.columnDomains` 全仓仅命中 `ConnectorFilterConstraint.java` 自身（:32 javadoc、:47 构造赋值、:67 getter），无外部消费者；单参构造器亦默认 emptyMap。`ConnectorDomain` 自 `5c325655b8b`（初始 SPI 框架引入）后未再改动，自始未接线。

**背景**：`ConnectorDomain`+`ConnectorRange` 是 SPI 里一套「按列的取值域摘要」，javadoc 自称「Used for fast partition pruning」。它把谓词边界值以 `Comparable<?>` 存储，只附带 `ConnectorType`，不携带 CHAR(n) 定宽长度，也无尾部空格填充逻辑。就类的形状而言，主张全部属实。

**影响**（当前对任何查询**零影响**；下例仅在假设未来有人接线 domain 剪枝时才成立）：设 hive 外表 `t(c CHAR(5))`，某行 `c` 逻辑值为 `'AB'`（CHAR 语义下等价于 `'AB   '` 三个尾空格）。查询 `WHERE c = 'AB'`。若将来有人把这条谓词摘要成 `ConnectorDomain.singleValue(CHAR(5), "AB")`，`ConnectorRange.equal` 会用裸串 `"AB"` 做 `.equals` 比较（ConnectorRange.java:111,136），既不会把探针 padding 到 `'AB   '`，也无从判断存储侧是 padding 还是 trim 的表示，可能与 CHAR 定宽语义下应命中的行错配。**但这条链路今天不存在**，且同样的 CHAR 填充问题在真正生效的表达式路径（ConnectorLiteral，`ExprToConnectorExpressionConverter.java:300-301` 把 `StringLiteral.getValue()` 原样塞入）里本就同样存在，属独立议题，与 `ConnectorDomain` 无关。此外该缺口对全部类型一视同仁地不生效，并非 CHAR 特有。

**修复方案**（优先级低，二选一）：

1. **最省**：既然 `ConnectorDomain/ConnectorRange` 至今零使用，直接删除这套死抽象（连同 `ConnectorFilterConstraint.columnDomains` 字段与 `getColumnDomains()`），消除误导性 javadoc，避免后人误接线时踩 CHAR/DECIMAL 归一化坑。
2. 若保留以备将来 domain 剪枝：在真正被填充的构造侧引入类型感知归一化（CHAR(n) 边界值统一 padding 到定宽再比较，或 `ConnectorType` 携带长度并在比较处 padding），但应等该路径真被启用时再做，现在做属投机（违反「不写投机代码」）。

综上：主张字面成立（CONFIRMED），但目标类是死代码，实际严重度低于「潜伏」，当前无需修，建议记为「删除死抽象」候选而非缺陷修复。

---

## 本类别小结

- **真问题（残留）**：#1 揭示的架构缺口——`ConnectorLiteral` 无 typed canonical 访问器、边界丢弃 `DateLiteral` canonical 文本——真实存在，且导致 hudi 的 **decimal 分区谓词** 仍走 `String.valueOf` 而与 hive 已修分支不对齐（HudiConnectorMetadata.java:1107）。这是本类别唯一有运行时可复现面的残留（潜在剪枝失配）。
- **伪/已修**：#1 报告主张的核心——「hive 现行错、String.valueOf 推错」——已在 commit `3593684715f` 修复，与当前工作树不符（过时）；paimon 被误归为同类 canonical-string 受害者（实为原生 typed 路径，另一种规避方式）。#23 字面成立但目标是彻底死代码（columnDomains 恒空、零消费者），当前零影响。
- **共性根因**：SPI 谓词/字面量层**缺少 typed、canonical-aware 的统一表示**。`ConnectorLiteral` 只携裸 Java 值 + `ConnectorType`，不带 canonical 文本；`ConnectorDomain` 只携 `Comparable` + 类型，不带 CHAR 长度/padding。结果是「每个连接器各自从 Java 值重建 canonical string」（#1，4 份分叉）或「无法忠实表达定宽语义」（#23）。修复难以在各连接器间对齐（hive 修了、hudi 没跟）正是这一缺口的直接症状。
- **与其他类别关联**：#1 的 canonical-string 分区匹配问题，与「分区剪枝 / matchesPredicates」类别的连接器侧失配直接相连（谓词字面量是剪枝的输入）；#23 的 CHAR padding 缺口在真正生效的 `ConnectorLiteral`/表达式路径上同样存在，应并入「字符串/CHAR 语义忠实度」议题统一考量，而非孤立看 `ConnectorDomain` 死抽象。
