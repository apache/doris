# FIX-M5 — iceberg 表级行数统计恢复 equality-delete 护栏（对齐当前旧版）

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §3 M5。
> 跟踪表：`plan-doc/task-list-65185-reverify-fixes.md` 行 9。
> recon+对抗复审：workflow `wf_40498e52-19f`（recon SOUND_WITH_CHANGES，机制 HEAD 确认）。

## Problem

`IcebergConnectorMetadata.computeRowCount`（fe-connector-iceberg）把表级行数算作
`total-records - total-position-deletes`，**没有 equality-delete 护栏**。对带 equality-delete 的
Iceberg MOR/CDC 表，这个数**偏大**（equality-delete 删掉的行无法从快照 summary 里扣除），被喂给
CBO 会误导 join 顺序 / 广播判断。而同一连接器里 COUNT(\*) 下推的 `IcebergScanPlanProvider
.getCountFromSummary` **已带**这道护栏 → 两条路自相矛盾。

## Root Cause（重点：不是读错代码，是旧版在脚下变了）

- 表级统计覆写（`getTableStatistics` + `computeRowCount`）是先前 iceberg 翻闸阻断项的一环，**recon 于
  2026-06-29、HEAD `6252ecc248b`**。当时旧版 `IcebergUtils.getIcebergRowCount` 就是
  `total-records - total-position-deletes`、**无** equality 护栏，故连接器忠实镜像为「不 gate」，并经
  对抗评审 + 变异测试**锁死**（设计见 `P6.6-FIX-H4-rowcount-table-statistics-design.md` §关键 parity 决策 1）。
- **2026-07-01**，上游 `32a2651f66b`（#64648，*Fix NPE in COUNT(\*) pushdown when snapshot summary omits
  total-\* counters*）把行数函数重构成公共 helper `getCountFromSummary(summary, ignoreDanglingDelete)`，
  并**新增** equality 护栏（`total-equality-deletes` 为 null 或 `!= "0"` → `UNKNOWN_ROW_COUNT`），同时把
  `getIcebergRowCount` 改为 `getCountFromSummary(summary, true)`。这个上游 commit 也进了本分支。
- 于是旧版路径现在遇到 equality-delete 表**报 UNKNOWN**，而连接器覆写仍报偏大的数。**根因 = 迁移镜像的
  parity 目标在 recon 之后被上游改动了**（非当初读错）。翻闸让 iceberg-on-HMS 也走此覆写
  （旧 `HMSExternalTable:547` 走带护栏的 `getIcebergRowCount`）→ 对 iceberg-on-HMS 是**新激活**回归。

## 决策 / 签字

- 恢复护栏 = **推翻**先前签字并变异锁死的「不 gate」决定。因其推翻的是用户签过字的决定，已在 session 中用
  中文讲清背景+两个方向，**用户 2026-07-11 签字选「恢复护栏、对齐当前旧版」**（另一路 = 维持不 gate + 登记
  验收偏差，被否）。
- 既定迁移原则 = 与旧版行为对齐；当前旧版已 gate，故 gate 是 parity-correct 目标。

## Design

连接器内忠实复刻当前旧版 `getCountFromSummary(summary, /*ignoreDanglingDelete=*/true)`，**不动 fe-core**：

1. 加第三个 summary 键常量 `TOTAL_EQUALITY_DELETES = "total-equality-deletes"`——与连接器自身
   `IcebergScanPlanProvider`（:136）及 fe-core `IcebergUtils` 同串字节一致；沿用本文件「本地复制、不抽公共类、
   不动无关 scan provider」的既有惯例。
2. `computeRowCount` 读三个计数并三者一起 null-guard；减法前加护栏 `if (!equalityDeletes.equals("0"))
   return -1;`。`-1` 已被 `getTableStatistics` 的 `rowCount > 0` 门映射为 UNKNOWN。位置删除仍照减（等价旧版
   `ignoreDanglingDelete = true` 分支：`deleteCount==0` 时 `totalRecords - 0 == totalRecords`，故简单减法即可）。
3. 更正三处失效注释（常量块 WHY、方法 javadoc、方法内 NOTE），改述护栏**已**施加、且与 COUNT(\*) 下推仅在
   dangling-delete 处理上不同。
4. 重写变异锁死的单测断言为 UNKNOWN，改名 `equalityDeletesGateTableStatisticsToUnknown`。

## Implementation Plan（均在 fe-connector-iceberg，无 fe-core/BE/thrift/pom 改）

- `IcebergConnectorMetadata.java` 常量块：改 WHY 注释 + 加 `TOTAL_EQUALITY_DELETES` 常量。
- `IcebergConnectorMetadata.java` `getTableStatistics` javadoc：FORMULA 子句补「gated on equality deletes」。
- `IcebergConnectorMetadata.java` `computeRowCount` javadoc + body：护栏 + null-guard 三者 + 减法保留。
- `IcebergConnectorMetadataStatisticsTest.java`：重写/改名 `equalityDeletesDoNotGateTableStatistics` →
  断言 empty；更新类 javadoc FORMULA 描述。
- **无新 import。**

## Risk Analysis

- 连接器局部，无 fe-core 依赖；`check-connector-imports.sh` 不受影响（仅字符串字面量）。
- 行为收窄 = 旧版 parity：summary 缺 `total-equality-deletes` → UNKNOWN，与当前旧版一致。
- append / 仅位置删除表不受影响：iceberg `SnapshotSummary` 总发 `total-equality-deletes`（无则 ="0"），
  新 null-guard 不误伤（由两个正数用例保持 GREEN 佐证——它们现也读新计数）。
- `computeRowCount` 私有、唯一调用方 `getTableStatistics`，无其它 caller。
- checkstyle：无 static / 无 unused import。

## 文档收口（红队 required change #2）

先前的「不 gate」结论散落在三处签字文档，须更正为「上游 #64648 已改 gate，故对齐」，否则 stale 文档与
已发代码 + 倒置后的锁死测试长期相反：
- `P6.6-FIX-H4-rowcount-table-statistics-design.md`（§关键 parity 决策 1 / Test #7 / Mutation 末条）
- `P6.6-FIX-H4-rowcount-table-statistics-summary.md`
- `P6.6-iceberg-flip-blockers-tasklist.md`（H-4 行）
以顶部「SUPERSEDED」批注 + 指向本文件的方式收口（不删历史）。

## Test Plan

### Unit Tests（fe-connector-iceberg；真 InMemoryCatalog v2；无 Mockito）

- **重写** `equalityDeletesGateTableStatisticsToUnknown`（原 `equalityDeletesDoNotGateTableStatistics`）：
  100 数据行 + 1 个 equality-delete 文件（5 行）→ `getTableStatistics` 返回 `Optional.empty()`（UNKNOWN）。
  **HEAD RED**：修前返回 100-0=100 → present(100)，`assertFalse(isPresent())` 挂；护栏返回 -1 后 GREEN。
  WHY 注释编码意图 + 变异（去 gate → present(100)）。
- **非回归全类重跑**：`rowCountFromTotalRecords` present(100)、`rowCountNetsOutPositionDeletes` present(70)、
  `emptyTableReportsUnknown` empty、`zeroNetRowsReportUnknown` empty、`systemTableReportsUnknownWithoutLoading`
  empty+无 seam load、`loadFailureDegradesToUnknown` empty+不抛。这些现也读新 `total-equality-deletes`，
  绿跑额外佐证 iceberg 对 append/位置删除快照发 "0"。

### E2E（live-gated，随 iceberg-on-HMS 批量 e2e 补，见 memory `hms-iceberg-delegation-needs-e2e`）

- docker iceberg/HMS + equality-delete writer（Spark MERGE/DELETE）；断言 `SHOW TABLE STATS` / explain 估计
  行数 = UNKNOWN(-1)，**独立 iceberg 目录 + iceberg-on-HMS 目录同表同结果**。
