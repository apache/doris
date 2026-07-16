# FIX-H2 — datetime 分区谓词 ISO 化 → 整表剪到 0 行（hive + hudi 两份就地各修）

> 来源：`task-list-65185-reverify-fixes.md` §H2；证据 reverify §2 H2。**范围决策**：H2 与 H1 同源、同属两连接器逐字节相同的剪枝块——对抗 agent 已证实 **hive 亦丢行**。用户签字「两份就地各修」。
> 关联勘验（复用）：`P4-T06e-FIX-DATETIME-PUSHDOWN-FORMAT-design.md`（MaxCompute 同根因：`ExprToConnectorExpressionConverter.convertDateLiteral` 产 `LocalDateTime`；修法=直接 `format` 空格分隔文本而非 `String.valueOf`/`toString`）。

## Problem

翻闸后，对 **DATETIME/DATETIMEV2/TIMESTAMP 分区列** 的 `=`/`IN` 谓词，剪枝把整表剪到 0 行（静默丢全部行）。DATE 列安全、STRING 列安全。

例：`dt` 为 `DATETIMEV2(0)` 分区列，谓词 `dt = '2024-01-01 10:00:00'` → 0 行。

## Root Cause（对 HEAD 核实 + 勘验 agent 实测）

剪枝谓词侧 `extractLiteralValue`（hudi `HudiConnectorMetadata:1030-1036` / hive `HiveConnectorMetadata:2045-2051`）对字面量做 `String.valueOf(getValue())`。`convertDateLiteral` 对非 DATE 的时间字面量存入 **`LocalDateTime`**（DATE/DATEV2 存 `LocalDate`）。`String.valueOf(LocalDateTime)` = `LocalDateTime.toString()` = **ISO-8601**：`'T'` 分隔、且**省略末尾零秒/零分数**（`2024-01-01 10:00:00` → `"2024-01-01T10:00"`）。而 HMS 分区值文本（经 H1 unescape 后）是 Hive-canonical **空格分隔、全 `HH:mm:ss`**（`"2024-01-01 10:00:00"`）。`matchesPredicates` 裸字符串比 → **永不相等** → 每分区被剪 → 0 行。

- `LocalDate.toString()` = `"2024-01-01"` 恰与 Hive 文本一致 → **DATE 安全**（不改）。
- STRING/VARCHAR 值本就 verbatim → 安全。
- legacy 经 Nereids typed 剪枝（`PruneFileScanPartition`）比 typed 值、只发 Hive-canonical 原文，从不 ISO 化 → **回归**。
- **与 MaxCompute 的关键差异**：MC 谓词下推涉及 TZ 转 UTC（故有 `ZoneId.of("CST")` 抛坑）；**分区剪枝是拿字面量与「存储的分区值文本」比，无任何 TZ 转换** → H2 无 TZ、无 `ZoneId.of` 崩坑，比 MC 简单。

## Design

**两份就地各修**：在**每个**连接器的 `extractLiteralValue`，对 `LocalDateTime` 值渲染 **Hive-canonical 分区文本**（空格分隔、全 `yyyy-MM-dd HH:mm:ss`、有微秒才带 `.ffffff`），而非 `String.valueOf`。DATE(`LocalDate`)/STRING/其它分支不变（`String.valueOf`）。

渲染放**包私有 static** helper `hiveDateTimeString(LocalDateTime)`（可离线单测，对齐 H1 的 static 化范式）：

```java
private static final DateTimeFormatter HIVE_DATETIME_SECONDS_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

static String hiveDateTimeString(LocalDateTime ldt) {
    String base = ldt.format(HIVE_DATETIME_SECONDS_FORMAT);   // "yyyy-MM-dd HH:mm:ss"（全秒、空格）
    int nano = ldt.getNano();
    if (nano == 0) {
        return base;                                          // 现实唯一场景（scale-0 datetime 分区）
    }
    // DATETIMEV2 是微秒精度（convertDateLiteral: nano = micro*1000）；追加 6 位微秒、去尾零（Hive 分数写法）。
    String micros = String.format("%06d", nano / 1000);
    int end = micros.length();
    while (end > 1 && micros.charAt(end - 1) == '0') {
        end--;
    }
    return base + "." + micros.substring(0, end);
}

// extractLiteralValue:
Object val = ((ConnectorLiteral) expr).getValue();
if (val == null) {
    return null;
}
if (val instanceof LocalDateTime) {
    return hiveDateTimeString((LocalDateTime) val);
}
return String.valueOf(val);
```

### 为何现实场景 bulletproof（关键论证）
唯一现实的 datetime 分区是 **scale-0**（按整秒；按微秒分区会分区爆炸，无人这么建表）：谓词字面量 scale-0 → `LocalDateTime` nano=0 → 渲 `2024-01-01 10:00:00`（无分数）；存储的整秒值 canonical 亦 `2024-01-01 10:00:00`（经 H1 unescape）→ **精确相等**。H1(unescape)+H2(渲染) **复合**：HMS 名 `dt=2024-01-01 10%3A00%3A00` → H1 unescape → `2024-01-01 10:00:00` == H2 渲染 → 命中。
残余风险仅 scale>0 datetime 分区（实际不存在）且写入方分数写法与本渲染不一致时——**登记为 e2e 验证点**（见 Test Plan），非现实缺陷。

### 为何不做 type-aware / 退回 fe-core（对齐用户「最小」选择）
- type-aware（matchesPredicates 按列类型 typed 比）需把列类型穿进剪枝块、改两份数据结构 → 超出「每份就地小改」；
- 「退回 fe-core typed 剪枝」实际未接线（`planScan` 丢弃 fe-core `requiredPartitions`，勘验已证），退回=对 datetime 列不剪=全表扫（perf 回归）——比 (a) 渲染差。
- 故取 task-list **primary** = (a) 渲染 canonical 文本，保留剪枝且现实场景正确。

## Implementation Plan

对 `HudiConnectorMetadata.java` 与 `HiveConnectorMetadata.java` **各**：
1. 加 import `java.time.LocalDateTime`、`java.time.format.DateTimeFormatter`（java 组内按序）。
2. 加常量 `HIVE_DATETIME_SECONDS_FORMAT` + 包私有 static `hiveDateTimeString(LocalDateTime)`。
3. `extractLiteralValue`：`LocalDateTime` 分支渲染，其它 `String.valueOf`（null→null 不变）。

## Risk Analysis

| Risk | 处置 |
|---|---|
| `instanceof LocalDateTime` 误分类 | `convertDateLiteral` 仅非 DATE 时间类型产 `LocalDateTime`，DATE 产 `LocalDate`；精确区分。 |
| scale>0 datetime 分区分数写法不匹配 | 现实不存在（微秒分区爆炸）；登记 e2e 点；非本 fix 引入 silent-loss（本 fix 只修 scale-0 现实场景，改前该场景 100% 丢）。 |
| 与 H1 交互 | 正交且复合（H1 unescape 名侧、H2 渲染谓词侧）；datetime 分区两者都需。UT 覆盖复合。 |
| TZ | 无——分区剪枝不转 TZ（与 MC 不同），无 `ZoneId.of` 崩坑。 |
| fe-core 铁律 | 仅连接器内；无 fe-core 变更、无源名判别、无属性解析。import-gate exit 0。 |
| checkstyle | 无 static import；新 import 按序；`%06d` 无需 Locale（整数无分组）。 |

## Test Plan

### Unit Tests（直接测 `hiveDateTimeString`，两连接器各一套；Rule 9 钉 WHY）
钉 WHY：datetime 分区谓词必须渲成 Hive-canonical 空格文本，否则整表剪到 0 行（静默丢全部行）。
1. **整秒**：`hiveDateTimeString(LocalDateTime.of(2024,1,1,10,0,0))` == `"2024-01-01 10:00:00"`（**核心**，现实场景；改前 `String.valueOf`→`"2024-01-01T10:00"`）。
2. **午夜/零分零秒**：`...of(2024,1,1,0,0,0)` == `"2024-01-01 00:00:00"`（ISO 会缩成 `"2024-01-01T00:00"`）。
3. **带微秒**：`...of(2024,1,1,10,0,0, 123456*1000)` == `"2024-01-01 10:00:00.123456"`；去尾零 `...100000*1000` → `.1`。
4. **秒非零**：`...of(2024,1,1,10,0,30)` == `"2024-01-01 10:00:30"`（ISO 出 `"2024-01-01T10:00:30"`，仍 `T`）。
5. **mutation**（守门）：helper 改回 `ldt.toString()` → 断言 1/2/4 变红。
6. **DATE 不回归**：`extractLiteralValue` 对 `LocalDate` 仍出 `"2024-01-01"`（经既有 pruning 测或补一条 `String.valueOf(LocalDate)` 断言；LocalDate 不入 LocalDateTime 分支）。

### E2E Tests（live-gated）
- DATETIME(scale-0) 分区列 + `= '2024-01-01 10:00:00'` 返回**正确行集**（改前 0 行）；含微秒 scale>0 分区（若真存在）作 e2e 验证点（真值闸）。真集群，显式登记 gated（Rule 12）。

## 设计红队（对抗 agent，实现前）

**结论：DESIGN SOUND — 无 must-fix gap。** 逐条 CONFIRM：
1. `String.valueOf(LocalDateTime)`==`toString()`==ISO `T`+省零秒（`convertDateLiteral:309-322` DATE→`LocalDate`、非 DATE→`LocalDateTime`；TIMEV2 是 `TimeV2Literal` 非 `DateLiteral`→走 String，且 TIME 非合法 Hive 分区类型→无 gap；TIMESTAMP_NTZ→DATETIME→`LocalDateTime` 已覆盖）。
2. 渲染逻辑逐例正确（含去尾零 `.1`/`.123456`）；`convertDateLiteral` 保证 nano=micro×1000（1000 倍数）→ 无越界；`%06d` 无需 Locale。
3. **scale-0 存储形吻合有实证 fixture**：`docker/.../hive/scripts/create_preinstalled_scripts/run17.hql:7` `time_par timestamp` 分区，真实目录 `time_par=2023-01-01 01%3A30%3A00` → H1 unescape → `2023-01-01 01:30:00` == H2 渲染。Hive 写 + Doris 写（`be/.../vhive_utils.cpp`）均整秒无 `.0`。
4. 完整性：`extractLiteralValue` 是唯一 literal→string 点，EQ + IN-list 均经它；其它连接器（MC 已空格化、Trino 转 epoch typed、ES 有意 ISO 自有 DSL）无需修 → 范围 hive+hudi 完整。
5. 无回归：if-chain 对 null/LocalDate/Boolean/数值/String 行为不变；static 化无碍。
- 非阻断 nit（已折入）：javadoc 注明 helper 亚微秒行为 out-of-contract（剪枝路不可达）。

## 守门结果（DONE，commit `cf540eebc3c`）

- hudi：`mvn -o -pl :fe-connector-hudi -am test -Dtest=HudiPartitionValuesTest` → **BUILD SUCCESS**；10 run / 0 fail（+`hiveDateTimeStringRendersHiveCanonicalText`）；checkstyle 全 0。
- hive：`mvn -o -pl :fe-connector-hive -am test -Dtest=HiveConnectorMetadataPartitionPruningTest` → **BUILD SUCCESS**；12 run / 0 fail（+`hiveDateTimeStringRendersHiveCanonicalText` 直接测 + `testDatetimePartitionPredicatePrunesWithHiveCanonicalText` H1+H2 复合 e2e applyFilter）；checkstyle 全 0。
- import-gate `GATE_RC=0`。测试可 RED：断言空格文本 `2024-01-01 10:00:00`/`.123456`/`.1`，改前 `String.valueOf`→`2024-01-01T10:00` → 变红。DATETIME 分区读端到端 = live-gated。

## 决策类型
明确修复（用户签字「两份就地各修」，task-list primary=(a) 渲染）。连接器局部、无 SPI 变更、与 legacy 语义 parity（现实场景）。D-PRUNE 抽取延后。
