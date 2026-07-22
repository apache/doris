# Problem

iceberg 列的**写入默认值**（`writeDefault`，用于 INSERT 省略该列时填什么、DESC 显示什么）未接线：`parseSchema` 把每个 `ConnectorColumn` 的 `defaultValue` 参数硬编码为 `null`，`Types.NestedField.writeDefault()` 全树无人读。结果：iceberg（v3）表上 `ALTER TABLE ADD COLUMN c INT DEFAULT 42` 把默认写进了 iceberg schema，但 Doris 刷新后 `DESC` 显示无默认、`INSERT` 省列落 NULL（能力缺失）。

用户签字升级此项。**读默认值 `initialDefault`（#65502，历史行补列）走独立的 BE 字典路径，绝不能碰。**

# Root Cause

`IcebergConnectorMetadata.parseSchema`（HEAD `:1987-1994`）：
```java
ConnectorColumn column = new ConnectorColumn(
        field.name(), <type>, field.doc()||"", true,
        null,   // <- 第 5 参 defaultValue 硬编码
        true);
```
`field.writeDefault()` 从不参与。`ConnectorColumn.defaultValue` → fe-core `ConnectorColumnConverter` → `Column.defaultValue`（DESC 展示 + `InsertUtils` 省列填充），期望格式=**裸字面量**（数字裸、其余由 `Column.getDefaultValueSql` 自动加引号）。

# Design

`parseSchema` 用 `field.writeDefault()` 转 Doris 默认串填第 5 参。转换新起一个 helper（放 `IcebergSchemaUtils`，复用已有 `serializeInitialDefault` + `isBinaryLike`），**不改** `serializeInitialDefault`/`buildField`/字典路径（读默认）。

用户签字的边界处理：
- **仅顶层标量**：`!type.isPrimitiveType()` → 返回 null（复杂类型 STRUCT/LIST/MAP 的默认装不进扁平字符串）。
- **二进制类跳过**：`isBinaryLike`（UUID/BINARY/FIXED）→ 返回 null（无法作裸字面量供 DESC/INSERT 重解析；读路径的 base64 载体是另一套契约）。
- **timestamptz 对齐读路径**：非二进制标量复用 `serializeInitialDefault`（同一 `Transforms.identity(type).toHumanString` + TIMESTAMP `T`→空格 + timestamptz offset 处理），使写默认展示与读默认一致。
- **不回退读默认**：`writeDefault()` 为 null 时不看 `initialDefault()`，`defaultValue` 保持 null（现状），保留"只有读默认的列 DESC 显 \N、省列 INSERT 到 NOT NULL 仍报错"。

新 helper：
```java
static String writeDefaultToDorisString(Type type, Object writeDefault, boolean enableTimestampTz) {
    if (writeDefault == null || !type.isPrimitiveType() || isBinaryLike(type)) {
        return null;
    }
    return serializeInitialDefault(type, writeDefault, enableTimestampTz);
}
```

# Implementation Plan

1. `IcebergSchemaUtils`：加 `writeDefaultToDorisString`（package-static，复用 `serializeInitialDefault`/`isBinaryLike`）。
2. `IcebergConnectorMetadata.parseSchema`：第 5 参 `null` → `IcebergSchemaUtils.writeDefaultToDorisString(field.type(), field.writeDefault(), enableTimestampTz)`；更新注释。
3. 单测：`IcebergSchemaUtilsTest` 加 helper 断言（int/string/boolean/date/timestamp/timestamptz/binary-skip/complex-skip/null）。
4. e2e：新增 `test_iceberg_write_default.groovy`（v3 REST catalog：CREATE v3 表 → `ALTER ADD COLUMN c INT DEFAULT 42` → refresh → DESC 显 42 + `INSERT(id)` 省列 → c=42）。

# Risk Analysis

- 只填 FE `Column.defaultValue`，不碰 `initialDefault`/字典/BE 读路径（#65502 正交，object graph 不同）。
- 只影响**有 iceberg writeDefault 的列**（v3 特性，少见）。核实现有 iceberg e2e：schema-change DDL 全 v2（DEFAULT 被 iceberg 拒）、complex-types 的 DEFAULT 被拒且 DESC 只断言类型列，均不含 writeDefault + DESC-default 断言 → **无 .out 回归**。
- 已提交的嵌套注释修复对 iceberg 无影响（iceberg 读路径 `structOf` 未填 childrenComments，`convertStructType` 拿到 null → 逐字节不变）。
- 改动隔离在 `fe-connector-iceberg`，不碰 fe-core、不碰铁律。

# Test Plan

## Unit Tests

`IcebergSchemaUtilsTest.writeDefaultCarriedAsDorisString`（或分条）：
- INT `42`→"42"、STRING→原串、BOOLEAN→"true"、DATE→"YYYY-MM-DD"、TIMESTAMP→空格分隔、timestamptz(off)→去 offset。
- UUID/BINARY/FIXED→null（跳过）、STRUCT/LIST→null（非标量）、writeDefault=null→null。
- MUTATION：helper 恒返回 `serializeInitialDefault` 不判 binary → binary 断言红；parseSchema 退回硬编码 null → 各断言红。

## E2E Tests

`regression-test/suites/external_table_p0/iceberg/test_iceberg_write_default.groovy`：v3 表 `ALTER ADD COLUMN c INT DEFAULT 42` → `refresh catalog` → 找 DESC 中 c 行断言默认 42（读 writeDefault）→ `INSERT INTO t(id) VALUES(1)` 省列 → `SELECT id,c` 得 c=42（省列套用写默认）。
