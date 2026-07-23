# Problem

Paimon 建表（Doris CREATE TABLE on a paimon catalog）时，嵌套在 STRUCT 列内的字段 **COMMENT 被丢弃**；对称地，读取 paimon 表结构时嵌套 STRUCT 字段的 **COMMENT 与 NULLABILITY 也被丢弃**。结果：

- 顶层列注释正常保留（`PaimonSchemaBuilder` 写、`PaimonConnectorMetadata` 读均带 comment），但**嵌套结构体字段注释在建表时不落盘**、读回时也不呈现。
- `DESC` / `SHOW CREATE TABLE` 对嵌套字段的注释与非空约束展示不一致。

用户签字：**写侧 + 读侧一起改，让 DESC 可见**。

# Root Cause

`PaimonTypeMapping`（`fe-connector-paimon`）两处用的是"窄"构造，只传了类型、丢了注释（读侧还丢了嵌套可空性）：

- 写侧 `toPaimonRowType`（:284-285）：`new DataField(id, name, type.copy(nullable))` —— 三参构造，comment 恒为 null。
- 读侧 `toStructType`（:191）：`ConnectorType.structOf(names, types)` —— 二参工厂，`childrenNullable` / `childrenComments` 恒空。

paimon `DataField` 有 4 参构造 `(int id, String name, DataType type, String description)`；`ConnectorType` 有 4 参工厂 `structOf(names, types, fieldNullable, fieldComments)` 及取值器 `getChildComment(i)` / `isChildNullable(i)`。数据在两侧都拿得到，只是没接。

- 写侧的注释来源：建表路径经 `ConnectorColumnConverter.toConnectorType`（:193 `comments.add(f.getComment())`）已把每个 StructField 注释装进 `ConnectorType.childrenComments`。
- 读侧的注释/可空来源：paimon `DataField.description()` / `DataField.type().isNullable()`。

# Design

对称地把注释（及读侧的可空性）接上，与顶层列已有行为保持一致。

> **实现中发现的第三处网关（用户已签字改）**：读路径的最终落点是 fe-core 通用转换器
> `ConnectorColumnConverter.convertStructType`，它用 2 参 `new StructField(name, type)` 建 Doris
> StructField，把嵌套字段的 comment/nullability **统一丢掉**（所有连接器读路径共用，iceberg/hms/hudi/
> maxcompute 此前也在此被丢）。要让 `DESC` 真正显示，必须把它改成 4 参
> `StructField(name, type, getChildComment(i), isChildNullable(i))`。该改动**通用、向后兼容**：未带
> 数据的连接器（childrenComments/childrenNullable 为空）→ comment=null / nullable=true，与原 2 参逐字节
> 一致，仅 paimon（已带数据）开始显示。用户 2026-07-22 签字同意此 fe-core 改动（属 SPI 边界通用转换器
> 本职，非把连接器逻辑塞进 fe-core）。

## 写侧 `toPaimonRowType`

```java
fields.add(new DataField(fieldId.incrementAndGet(), fieldName,
        toPaimonType(children.get(i)).copy(type.isChildNullable(i)),
        type.getChildComment(i)));
```

`getChildComment(i)` 无注释时返回 null；paimon 三参构造本就委托给四参补 null，故**无注释时逐字节等价于现状**，仅在有注释时把它带上。

## 读侧 `toStructType`

补齐 `nullable` 与 `comments` 两条并列列表，改用 4 参工厂：

```java
List<Boolean> nullable = fields.stream().map(f -> f.type().isNullable())
        .collect(Collectors.toCollection(ArrayList::new));
List<String> comments = fields.stream().map(DataField::description)
        .collect(Collectors.toCollection(ArrayList::new));
return ConnectorType.structOf(names, types, nullable, comments);
```

# Implementation Plan

1. `PaimonTypeMapping.toPaimonRowType`：改 4 参 DataField；更新 :281-283 注释（去掉"comment 有意丢弃 DV-035"措辞）。
2. `PaimonTypeMapping.toStructType`：补 nullable/comments，改 4 参 structOf。
3. 单测：
   - `PaimonTypeMappingToPaimonTest`：更新 `nestedNullabilityPreservedForStructField` 的过时注释；新增 `nestedStructFieldCommentPreserved`（写侧注释保留）。
   - `PaimonTypeMappingReadTest`：新增 `nestedStructFieldCommentAndNullabilityCarried`（读侧注释 + 可空性带回）。
4. e2e：新增 `test_paimon_nested_struct_comment.groovy`（可写 paimon catalog：CREATE TABLE 带嵌套字段注释 → `DESC` 可见 + `SELECT fields FROM t$schemas` 佐证落盘）。

# Risk Analysis

- 写侧：无注释时逐字节等价现状，纯增量，零风险。
- 读侧：嵌套字段 nullability 从"恒 nullable"变为真实值，属**展示层**（`DESCRIBE`）。查询正确性影响为零（与 iceberg 读路径已有的 4 参行为一致，是消除不对称而非引入新行为）。用全量 paimon 单测 + 类型映射测试守卫。
- 不碰 fe-core（改动全在 `fe-connector-paimon` + 测试 + e2e），不碰"fe-core 只出不进"铁律。

# Test Plan

## Unit Tests

- 写侧：`nestedStructFieldCommentPreserved` —— `structOf(names, types, nullable, comments)` 构 STRUCT，断言 `RowType.getFields().get(0).description()` == 注释；MUTATION：退回 3 参 → 红。
- 读侧：`nestedStructFieldCommentAndNullabilityCarried` —— 由 paimon `RowType`（含带 description、NOT NULL 的 DataField）走 `toConnectorType`，断言 `getChildComment(0)` / `isChildNullable(0)` 正确；MUTATION：退回 2 参 structOf → 红。
- 回归保持绿：`structBuildsSequentialFieldIdsAndNames`（2 参 structOf → null comment → DataField 相等不变）、`nestedNullabilityPreservedForStructField`。

## E2E Tests

`regression-test/suites/external_table_p0/paimon/test_paimon_nested_struct_comment.groovy`：
1. 建可写 paimon catalog（filesystem/minio）。
2. `CREATE TABLE t (id INT, s STRUCT<a:INT COMMENT 'note_a', b:STRING>) ...`。
3. `DESC t` / `SHOW CREATE TABLE t` 断言嵌套字段 `a` 带 `note_a`。
4. `SELECT fields FROM t$schemas` 佐证注释已落进 paimon 磁盘元数据。
