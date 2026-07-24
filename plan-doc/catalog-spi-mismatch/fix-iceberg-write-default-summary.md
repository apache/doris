# Summary

## Problem

iceberg 列的**写入默认值**（`writeDefault`）未接线：`parseSchema` 把每个 `ConnectorColumn` 的 `defaultValue` 硬编码为 null，`Types.NestedField.writeDefault()` 全树无人读。iceberg（v3）表 `ALTER ADD COLUMN c INT DEFAULT 42` 把默认写进 iceberg schema，但 Doris 刷新后 `DESC` 无默认、`INSERT` 省列落 NULL。

## Root Cause

`IcebergConnectorMetadata.parseSchema`：`new ConnectorColumn(name, type, doc, true, null /*defaultValue*/, true)` —— 第 5 参恒 null。

## Fix

- `IcebergSchemaUtils` 新增 `writeDefaultToDorisString(type, writeDefault, enableTimestampTz)`：非空 + 顶层标量 + 非二进制 → 复用 `serializeInitialDefault`（同 `Transforms.identity(type).toHumanString` + TIMESTAMP `T`→空格 + timestamptz offset 处理）转 Doris 裸字面量；否则返回 null。
- `parseSchema` 第 5 参改调该 helper（`field.writeDefault()`）。

用户签字的边界（均已实现）：仅顶层标量、二进制类（UUID/BINARY/FIXED）跳过、timestamptz 对齐读路径、写默认为空不回退读默认。**读默认 initialDefault 的 BE 字典路径（#65502）未碰**——不同 object graph（TField 字典 vs FE Column），正交。

## Tests

- 单测 `IcebergSchemaUtilsTest.writeDefaultCarriedAsDorisString`：INT→"42"、STRING→原串、BOOLEAN→"true"、DATE(19723)→"2024-01-01"、TIMESTAMP→"2024-01-01 00:00:00.123456"、timestamptz(off)→去 offset；UUID/BINARY→null、LIST→null、无写默认→null。
- e2e `test_iceberg_write_default.groovy`：v3 REST catalog 建表 → `ALTER ADD COLUMN c INT DEFAULT 42` → `refresh catalog` → DESC 中 c 行含 42（读 writeDefault）→ `INSERT(id)` 省列 → `SELECT id,c` 得 c=42（省列套用写默认）。

## Result

- `IcebergSchemaUtilsTest` 24 全绿（含新测），iceberg 模块编译 + checkstyle 通过（`BUILD SUCCESS`）。
- e2e 需在 iceberg docker 回归环境跑（`enableIcebergTest=true`）。
- 核实无副作用：iceberg ALTER/MODIFY 的 `getDefaultValue` 消费者读的都是 DDL 侧目标列（`modifyColumn`/`toAddColumnChange`/`IcebergCatalogOps`），不读 parseSchema 加载的现有列；改动只影响 DESC + 省列 INSERT。现有 iceberg e2e 无"写默认值 + DESC-default 断言"组合（schema-change DDL 全 v2 拒默认、complex-types 只断言类型列），无 .out 回归。
- 改动隔离在 `fe-connector-iceberg`，不碰 fe-core、不碰铁律。
