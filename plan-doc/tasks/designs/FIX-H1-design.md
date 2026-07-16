# FIX-H1 — 分区名不 unescape → 静默丢行（hive + hudi 两份就地各修）

> 来源：`task-list-65185-reverify-fixes.md` §H1；证据 reverify §2 H1 + §4 DUPLICATION:partition-prune。
> 批次 0 第 2 条。**范围决策**：对抗 agent 证实 H1 **不是 hudi 独有**——`HiveConnectorMetadata` 逐字节相同的剪枝块同样丢行。用户签字 **「两份就地各修」**（不抽共享 helper）。

## Problem

翻闸后，对 hive-on-HMS 或 hudi-on-HMS 分区表，若**分区值含 Hive 转义字符**（`:`→`%3A`、`/`→`%2F`、`%`→`%25`、以及 `"#'*/=?\{[]^` 等；注：空格 **不**被 Hive 转义）且带该列的 `=`/`IN` 谓词，**静默丢行**（对真实存在的值返回 0 行、无报错）。

例：分区列 `code`（STRING），HMS 名 `code=US%3ACA`，谓词 `code='US:CA'` → 剪掉该分区 → 0 行。

## Root Cause（对 HEAD 核实）

两连接器各有一份**逐字节相同**的剪枝块（reverify §4 DUPLICATION:partition-prune）：
- 候选名来自 `hmsClient.listPartitionNames`（`ThriftHmsClient` 原样转发 HMS `get_partition_names`，返回 **Hive 转义**名）。
- `parsePartitionName`（hudi `HudiConnectorMetadata:1054-1065` / hive `HiveConnectorMetadata:2069-2080`）存 `part.substring(eq+1)` —— **原样转义值，无 unescape**。
- `matchesPredicates`（hudi `:1067` / hive `:2082`）裸 `allowedValues.contains(actualValue)` 字符串比；谓词侧 `extractLiteralValue` 返回 **未转义** 字面量。
- 转义值 `US%3ACA` ≠ 未转义 `US:CA` → 该分区落选 → `matched < all`（非全命中，`applyFilter` 的 bail 不触发）→ prunedPartitions 为错误子集 → 丢行。

对比：两连接器的**同名兄弟路径已 unescape** —— hudi 扫描侧 `HudiScanPlanProvider.parsePartitionValues:723`（`unescapePathName`）、hive fe-core 分区项路 `HiveWriteUtils.toPartitionValues`（`unescapePathName`）/ `CachingHmsClient.toPartitionValues`（`FileUtils.unescapePathName`）。唯**剪枝决策**这条 `parsePartitionName` 漏。legacy 经 Nereids typed 剪枝 + 只发 Hive-canonical 原文，从不转义比对。故 **回归**。

fe-core 其实算出正确 typed `requiredPartitions`，但连接器 `planScan` 只认 applyFilter 的 `prunedPartitions`（丢弃 requiredPartitions）→ 连接器剪枝是权威，其 bug 直接丢行（对抗 agent 已 walk 全链确认 hive+hudi 皆真）。

## Design

**两份就地各修**：在**每个**连接器的 `parsePartitionName` 存值前 unescape，复用**本连接器同包**已有的 `unescapePathName`（与其兄弟 parse 路径一致；连接器不跨 import fe-core、亦不跨连接器互 import）。

- 键（分区列名）不 unescape（列名非转义；与 `parsePartitionValues` 一致——只 unescape 值）。
- 为可离线单测（对齐已有 `HudiPartitionValuesTest` 直接测静态 parse helper 的范式，Rule 11）：把 `parsePartitionName` 从 `private` 提为**包私有 `static`**（纯函数、不用实例态），`unescapePathName` 从 `private static` 提为**包私有 static**。**每份只动 2 个方法**（parsePartitionName + unescapePathName）。

> H1/H3 纠缠说明：H3 稍后会重构 **hudi** 的 `applyFilter`（改分区源 + 相对化），届时非-hive-sync 臂改走 `parsePartitionValues`（本就 unescape），hive-sync 臂仍用 `parsePartitionName`（H1 修的）。故 H1 的 hudi 修在 hive-sync 臂长期有效；**用直接单测 `parsePartitionName`**（不测 applyFilter 全链）使 H1 测试**跨 H3 稳定**（applyFilter 级测试会被 H3 改语义）。hive 的 `applyFilter` H3 不动，稳定。

## Implementation Plan

### hudi（`HudiConnectorMetadata.java` + `HudiScanPlanProvider.java`）
1. `HudiScanPlanProvider.unescapePathName`：`private static` → `static`。
2. `HudiConnectorMetadata.parsePartitionName`：`private` → `static`；值改
   `HudiScanPlanProvider.unescapePathName(part.substring(eq + 1))`。

### hive（`HiveConnectorMetadata.java` + `HiveWriteUtils.java`）
3. `HiveWriteUtils.unescapePathName`：`private static` → `static`。
4. `HiveConnectorMetadata.parsePartitionName`：`private` → `static`；值改
   `HiveWriteUtils.unescapePathName(part.substring(eq + 1))`。

（`prunePartitionNames` 内 `parsePartitionName(...)` 调用点：静态方法从实例上下文调用，编译不变，无需改。）

## Risk Analysis

| Risk | 处置 |
|---|---|
| unescape 误伤非转义值 | `unescapePathName` 只解码合法 `%XX`；`year=2024` 等无 `%` 值不变；含真实 `%`+2 hex 的数据值会被解码——但**与 legacy/兄弟路径 parity**（都 unescape），非本 fix 引入。 |
| 列名被 unescape | 只 unescape 值（`substring(eq+1)`），键不动。 |
| static 化破坏调用点 | 同类静态调用编译不变；checkstyle 无 static-import（是方法调用非 import）。 |
| 与 H2 交互 | 正交：H1 修 HMS-名侧转义，H2 修谓词侧 datetime 渲染；datetime 分区两者**都需**且**复合**（H1 unescape 后 `10:00:00`，H2 渲染 `2024-01-01 10:00:00` → 命中）。 |
| fe-core 铁律 | 仅连接器内改；无 fe-core 变更、无源名判别、无属性解析。import-gate 须 exit 0。 |

## Test Plan

### Unit Tests（直接测 `parsePartitionName`，跨 H3 稳定）
- **hudi** `HudiPartitionValuesTest` 加 `parsePartitionNameUnescapesValues`：
  `HudiConnectorMetadata.parsePartitionName("code=US%3ACA/kind=a%2Fb", Arrays.asList("code","kind"))`
  == `{code:"US:CA", kind:"a/b"}`。RED（改前）：`{code:"US%3ACA", kind:"a%2Fb"}`。
- **hive** `HiveConnectorMetadataPartitionPruningTest` 加 `parsePartitionNameUnescapesValues`：
  `HiveConnectorMetadata.parsePartitionName("code=US%3ACA", Collections.singletonList("code"))`
  == `{code:"US:CA"}`。RED：`{code:"US%3ACA"}`。
- **hive** 额外 applyFilter 级集成测试（hive applyFilter 稳定）：2 分区 `code=US%3ACA`/`code=EU%3ADE` + 谓词 `code='US:CA'` → 命中 `code=US%3ACA` 一条（pruned 非空且正确）。RED：命中集为空（两分区都因转义不等被剪）。

### E2E Tests
含转义字符分区值的 hive/hudi 分区表带谓词读回归 = **live-gated**（真集群）；显式登记 gated（Rule 12）。

## 决策类型
明确修复（用户签字「两份就地各修」）。连接器局部、无 SPI 变更、与 legacy + 兄弟 unescape 路径 parity。D-PRUNE 抽取延后（reverify §4）。

## 守门结果（DONE，commit `39a279e7c26`）

- hudi：`mvn -o -pl :fe-connector-hudi -am test -Dtest=HudiPartitionValuesTest,HudiPartitionPruningTest` → **BUILD SUCCESS**；17 run / 0 fail（`HudiPartitionValuesTest` 9 含新 `parsePartitionNameUnescapesValuesForPruning`、`HudiPartitionPruningTest` 8）；checkstyle 全 0。
- hive：`mvn -o -pl :fe-connector-hive -am test -Dtest=HiveConnectorMetadataPartitionPruningTest` → **BUILD SUCCESS**；10 run / 0 fail（+2 新：`parsePartitionNameUnescapesValues`、`testEscapedPartitionValuePrunesInsteadOfDropping`）；checkstyle 全 0。
- import-gate：`GATE_RC=0`（仅已知 `HiveVersionUtil` vendored 误报被 `is_vendored()` 跳过）。
- 测试可 RED：断言 decoded `US:CA`/`a/b`，改前 parsePartitionName 出 `US%3ACA`/`a%2Fb` → 变红。含转义值分区表读端到端 = live-gated。
