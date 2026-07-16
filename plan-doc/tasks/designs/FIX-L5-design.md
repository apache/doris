# FIX-L5 — trino listTableNames 丢 LinkedHashSet 去重

> 来源：reverify §1 表 L5（原 P2-4）。🟡 低（连接器局部，防御性 parity）。范围：`TrinoConnectorDorisMetadata.listTableNames` 一行。
> HEAD 复核基线：`4cd63c6911a`（L3 之后；同文件）。

## Problem / Root Cause

`listTableNames` 把 `metadata.listTables(connSession, Optional.of(dbName))` 的结果 `.map(getTableName)` 后
直接 `.collect(toList())`，**丢了 legacy 的 LinkedHashSet 去重**。个别 Trino 连接器 `listTables` 会对同一 schema 返回
重复 `SchemaTableName`（表+视图双列、多源合并等），legacy Doris trino 用 `LinkedHashSet` 去重且保序；SPI 版漏了。

## Design

`.map(SchemaTableName::getTableName)` 之后加 `.distinct()`（保序去重，等价 LinkedHashSet）再 `.collect(toList())`。
**不复原 legacy 的 prefix 过滤**（`listTables` 已按 schema 过滤，prefix 冗余；task-list 明确不复原）。

## Risk

单 schema 内表名本唯一 → `.distinct()` 只折叠意外重复，不会误并两个真不同的表。保序不变。零其它行为变更。连接器局部，
不碰 fe-core，不新增 import。

## Test Plan

- build-compile（`.distinct()` 编译 + 0 checkstyle）。
- 行为 UT 受同一 `io.trino.Session` 构造墙阻（见 FIX-L3-design）——不加，登记。
- e2e live-gated：trino 目录 `SHOW TABLES` 无重复项（需真 trino 连接器返回重复的场景，live）。

## 备注

无 red-team（一行防御性 parity，非「大改动」；memory `clean-room-adversarial-review-pref`）。
