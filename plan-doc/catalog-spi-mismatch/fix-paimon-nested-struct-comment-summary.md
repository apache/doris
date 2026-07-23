# Summary

## Problem

Paimon 建表（Doris CREATE TABLE on a paimon catalog）丢弃嵌套 STRUCT 字段的 COMMENT；读回时嵌套字段的 COMMENT 与 NULLABILITY 也被丢弃。顶层列注释正常、唯独嵌套字段不一致，`DESC` / `SHOW CREATE TABLE` 展示缺失。

## Root Cause

三处"窄构造"依次丢弃嵌套字段元数据：

1. 写侧 `PaimonTypeMapping.toPaimonRowType`：3 参 `new DataField(id, name, type)`，comment 恒 null。
2. 读侧 `PaimonTypeMapping.toStructType`：2 参 `ConnectorType.structOf(names, types)`，childrenNullable/childrenComments 恒空。
3. fe-core 通用转换器 `ConnectorColumnConverter.convertStructType`：2 参 `new StructField(name, type)`，即使 ConnectorType 带了数据也在此丢弃（所有连接器读路径共用，此前 iceberg/hms/hudi/maxcompute 同样在此被丢——这才是"任何连接器 DESC 都不显示嵌套注释"的真正原因）。

## Fix

对称补齐三处，走各自的 4 参构造：

1. 写侧 `toPaimonRowType`：`new DataField(id, name, type.copy(isChildNullable(i)), getChildComment(i))`。无注释时 `getChildComment` 返回 null，与 3 参逐字节一致。
2. 读侧 `toStructType`：`structOf(names, types, nullable, comments)`，`nullable=f.type().isNullable()`、`comments=f.description()`。
3. fe-core `convertStructType`：`new StructField(name, convertType(child), getChildComment(i), isChildNullable(i))`。**向后兼容**：未带数据的连接器 → comment=null / nullable=true，与原 2 参逐字节一致，仅 paimon 开始显示。

改动隔离在 `fe-connector-paimon` + fe-core 通用 SPI 边界转换器（用户 2026-07-22 签字同意后者）。不新增类/util，不把连接器逻辑塞进 fe-core。

## Tests

- 单测（写侧）`PaimonTypeMappingToPaimonTest.nestedStructFieldCommentPreserved`：STRUCT 字段注释进 paimon DataField.description；退回 3 参 → 红。
- 单测（读侧）`PaimonTypeMappingReadTest.nestedStructFieldCommentAndNullabilityCarried`：paimon RowType（带 description + NOT NULL 字段）→ ConnectorType 的 getChildComment/isChildNullable 正确；退回 2 参 structOf → 红。
- 单测（fe-core 读向）`ConnectorColumnConverterTest.convertStructTypeCarriesFieldNullabilityAndComment`：ConnectorType → Doris StructField 带回 comment/containsNull；退回 2 参 StructField → 红。
- e2e `test_paimon_nested_struct_comment.groovy`：可写 paimon(hms) catalog 建带嵌套注释表 → `SHOW CREATE` / `DESC` 显示 `note_a`（读侧），`SELECT fields FROM t$schemas` 佐证注释落进 paimon 磁盘元数据（写侧）。

## Result

- paimon 类型映射单测 16 全绿（含 2 条新测）；fe-core `ConnectorColumnConverterTest` 28 全绿（含 1 条新测）。两模块编译 + checkstyle 通过（`BUILD SUCCESS`）。
- e2e 需在 paimon docker 回归环境跑（`enablePaimonTest=true`）。
- 净效果：嵌套 STRUCT 字段注释在 Doris 端全链路打通（写落盘 + 读回显示），并顺带修正了通用转换器一直丢弃嵌套注释/可空性的共性问题（对其它连接器向后兼容、零行为变化）。
