# [P4-T06e] FIX-DATETIME-PUSHDOWN-FORMAT (GAP0/1) — design

> 来源：Batch-D 红线扩充对抗复审 workflow `wbw4xszrg`。用户定 **Fix（Tier 1，major correctness/perf）**。
> 关联：legacy 对照 `MaxComputeScanNode.convertLiteralToOdpsValues:529-613`；fe-core 字面量来源 `ExprToConnectorExpressionConverter.convertDateLiteral:309-321`。

## Problem

翻闸后，对 max_compute 表的 **DATETIME / TIMESTAMP / TIMESTAMP_NTZ** 列下推谓词坏。当 catalog 开启 `datetime_predicate_push_down`（默认开，见 `MCConnectorProperties.DEFAULT_DATETIME_PREDICATE_PUSH_DOWN`）时：

```sql
-- 例：dt 为 DATETIME 列，session time_zone = 'Asia/Shanghai'（非 UTC）
SELECT * FROM mc.db.t WHERE dt = '2023-02-02 00:00:00';
```

两条独立 delta（互不掩盖，须同修）：

- **delta-1（format，perf + 可能错）**：谓词字面量被错误地序列化为 `LocalDateTime.toString()` 形态（`'T'` 分隔、变精度，如 `"2023-02-02T00:00"`），再喂给空格分隔、定长的 `DATETIME_3/6_FORMATTER` →
  - **非 UTC session**：`LocalDateTime.parse` 抛 `DateTimeParseException` → 被顶层 `convert()` catch → **整棵 conjunct 树降为 `NO_PREDICATE`**（谓词永不下推 = 性能回归，全表扫 + BE 兜底过滤）。
  - **UTC session**：`convertDateTimezone` 短路返回未转换的 `"2023-02-02T00:00"` → 把 **malformed 字面量**推给 ODPS（`dt == "2023-02-02T00:00"`，结果未定：可能 ODPS 报错、可能匹配错/丢行）。
- **delta-2（TZ source，丢行）**：source timezone 取 **project-region TZ**（由 endpoint URL 推），而 legacy 取 **session TZ**。当 session TZ ≠ project-region TZ 且 ≠ UTC 时，转换基准错位 → 推给 ODPS 的 UTC 字面量整体偏移 → **静默丢行 / 匹配错行**。仅 delta-1 修好后 delta-2 才会显形（否则谓词早已被丢）。

行正确性 + 性能双回归。仅 MaxCompute 暴露（唯一翻闸的 SPI 文件扫描连接器；`MaxComputePredicateConverter` 为 MC 专有类）。

## Root Cause（已核码确认）

### delta-1：过早 stringify LocalDateTime

| # | 位置 | 行为 |
|---|---|---|
| 1 | `ExprToConnectorExpressionConverter.convertDateLiteral:316-320` | DATE/DATEV2 → `LocalDate`；其余（DATETIME/DATETIMEV2/TIMESTAMP…）→ **`LocalDateTime`**（nanos = `microsecond*1000`，故始终微秒精度、末 3 nano 位恒 0）。存入 `ConnectorLiteral` 的 `Object value`。 |
| 2 | `MaxComputePredicateConverter.formatLiteralValue:201` | `String rawValue = String.valueOf(literal.getValue())` → 对 `LocalDateTime` 调 `toString()` = ISO-8601 `'T'` 分隔、**变精度**（省略尾零：`"2023-02-02T00:00"`、`"...T00:00:00.123"`）。 |
| 3 | `formatLiteralValue` DATETIME:227-232 / TIMESTAMP:234-239 | 把该 `rawValue` 喂 `convertDateTimezone(rawValue, DATETIME_3/6_FORMATTER, UTC)`。formatter = `"yyyy-MM-dd HH:mm:ss.SSS[SSS]"`（空格分隔、定长）。 |
| 4 | `convertDateTimezone:256-262` | 非 UTC → `LocalDateTime.parse(rawValue, formatter)` ↯ `'T'` vs 空格 + 缺秒/分数 → **抛**；UTC → 短路返回 raw（malformed）。 |
| 5 | `TIMESTAMP_NTZ:241-245` | 直接 `" \"" + rawValue + "\" "`（无 formatter、无 TZ）→ 推 `'T'` 分隔 malformed 字面量。 |

**legacy 正确做法**（`MaxComputeScanNode:558-593`）：`dateLiteral.getStringValue(ScalarType.createDatetimeV2Type(3|6))` → 直接产空格分隔、定长 fraction 的串（`"2023-02-02 00:00:00.000"`），与同名 formatter 完全对齐；从不经 `toString()`。

**字节级对齐已验**（delta-1 修法依据）：`DateLiteral.getStringValue(Type)` :508-520 用 `scaledMicroseconds = microsecond / SCALE_FACTORS[scale]`（**截断**）+ 定长 `scale` 位 fraction + 空格分隔。`LocalDateTime.format(ofPattern("...ss.SSS"))` 的 `SSS` 同为**截断**取前 N 位 fraction；因 step-1 保证 nanos = micro×1000（末 3 nano 位恒 0），`SSS`→3 位、`SSSSSS`→6 位与 legacy `getStringValue(scale=3|6)` **逐字符相等**（micro=123456: 截断→`.123`/`.123456`；micro=0→`.000`/`.000000`）。故「直接 format LocalDateTime」= legacy `getStringValue` 输出，无精度分歧。

### delta-2：source TZ 用 project-region 而非 session

| 位置 | cutover | legacy |
|---|---|---|
| source TZ | `MaxComputeScanPlanProvider.convertFilter:287` → `resolveProjectTimeZone()` → `MCConnectorEndpoint.resolveProjectTimeZone(endpoint)` :111-125（由 endpoint URL region 查 `REGION_ZONE_MAP`） | `MaxComputeScanNode.convertDateTimezone:603/609` → `DateUtils.getTimeZone()` :403-408 = `ZoneId.of(ConnectContext.get().getSessionVariable().getTimeZone())`（**session TZ**） |

Doris 把 datetime 字面量按 **session time_zone** 解释；要正确推给 ODPS（其 DATETIME 按 UTC 比较）必须以 session TZ 为转换基准。用 project-region TZ 会以错误基准解释用户字面量 → 偏移丢行。

**连接器可拿 session TZ（关键调研结论）**：`ConnectorSession` 有一等方法 `getTimeZone()`（`ConnectorSession.java:36-37`，「session time zone identifier, e.g. 'Asia/Shanghai'」）。其实现 `ConnectorSessionImpl.getTimeZone()` :75-77 返回构造期注入值；`ConnectorSessionBuilder.from(ctx):58` 注入 `ctx.getSessionVariable().getTimeZone()` —— **与 legacy `DateUtils.getTimeZone()` 同源**。scan 路径的 session 经 `PluginDrivenExternalCatalog.buildConnectorSession():465-472` 走 `from(ctx)`（query 线程有 ctx），并由 `PluginDrivenScanNode.create():143` 在构造期捕获、`getSplits():426-427` 传入 `planScan`。故 `ZoneId.of(session.getTimeZone())` ≡ legacy（且因 session 在 plan 期捕获，比 legacy 运行时读 `ConnectContext.get()` 对异步 batch-split 路径**更稳**）。

**为何 CI 没抓**：连接器侧 `MaxComputePredicateConverter` **零 UT 覆盖**（仅 `MaxComputeScanPlanProviderTest` 测 partition/limit helper，不构造 converter）；live e2e 仅 `test_max_compute_partition_prune.groovy`（分区裁剪，无 datetime 谓词、无跨 TZ）。

## Blast radius

- `MaxComputePredicateConverter` 为 MaxCompute 专有类，仅被 `MaxComputeScanPlanProvider.convertFilter` 构造 → 修改只影响 MC 读谓词下推。
- 仅 DATETIME/TIMESTAMP/TIMESTAMP_NTZ 三分支改动；BOOLEAN/数值/STRING/CHAR/VARCHAR/**DATE** 分支不动（DATE 用 `LocalDate.toString()`=`"yyyy-MM-dd"` 恰与 legacy `getStringValue(DateV2)` 一致，本就正确，不在本 fix 范围）。
- delta-2 改 source TZ：当 session TZ == project-region TZ（同区部署、最常见）时行为不变；仅在两者不一致时纠偏（恢复 legacy parity）。
- `dateTimePushDown=false` 时三分支 fall-through 抛 `UnsupportedOperationException` → `convert()` catch → `NO_PREDICATE`（不下推，BE 过滤）——与现状一致，不动。

## Design

**Shape A（连接器局部，无 SPI 变更）** —— 直接对 `LocalDateTime` 值格式化 + 用 session TZ 转换。

### delta-1：`MaxComputePredicateConverter` 直接 format LocalDateTime

把 DATETIME/TIMESTAMP/TIMESTAMP_NTZ 三分支从「`String.valueOf(value)` → 喂 formatter」改为「取 `LocalDateTime value` → 直接 `format(formatter)`（+ 可选 TZ 转换）」。新私有助手取代字符串版 `convertDateTimezone`：

```java
// DATETIME (scale 3, 转 TZ):
return " \"" + formatDateTimeLiteral(literal.getValue(), DATETIME_3_FORMATTER, true) + "\" ";
// TIMESTAMP (scale 6, 转 TZ):
return " \"" + formatDateTimeLiteral(literal.getValue(), DATETIME_6_FORMATTER, true) + "\" ";
// TIMESTAMP_NTZ (scale 6, 不转 TZ —— 镜像 legacy :585-592 无 convertDateTimezone):
return " \"" + formatDateTimeLiteral(literal.getValue(), DATETIME_6_FORMATTER, false) + "\" ";

private String formatDateTimeLiteral(Object value, DateTimeFormatter formatter, boolean convertTimeZone) {
    if (!(value instanceof LocalDateTime)) {                 // 防御：非 LocalDateTime → 抛 → convert() catch → NO_PREDICATE（镜像 legacy 对非 DateLiteral 抛）
        throw new UnsupportedOperationException("Expected LocalDateTime for datetime predicate, got: "
                + (value == null ? "null" : value.getClass().getSimpleName()));
    }
    LocalDateTime ldt = (LocalDateTime) value;
    if (convertTimeZone && !sourceTimeZone.equals(UTC)) {   // 镜像 legacy convertDateTimezone 短路：source==UTC 不转
        ldt = ldt.atZone(sourceTimeZone).withZoneSameInstant(UTC).toLocalDateTime();
    }
    return ldt.format(formatter);
}
```

- 为何正确：value 即 fe-core 存入的 `LocalDateTime`（已是 bound 谓词 scale），`format(DATETIME_3/6_FORMATTER)` 逐字符等于 legacy `getStringValue(DatetimeV2Type(3|6))`（见 Root Cause 字节级对齐）。彻底根除 `toString()`→reparse 链：不再抛、不再推 malformed。
- TZ 转换语义 = legacy `convertDateTimezone`（source TZ → UTC，source==UTC 短路）。NTZ 不转，对齐 legacy。
- 删除字符串版 `convertDateTimezone:254-263`（被新助手取代）。

### delta-2：source TZ 改用 session TZ（**TZ 字符串惰性解析** —— impl-review F1 折入）

`MaxComputeScanPlanProvider`：planScan 已持 `session`，把 session TZ **字符串**下传 `convertFilter`，由 converter 在格式化 datetime 字面量时（`convert()` 的 catch 内）惰性 `ZoneId.of`：

```java
// planScan 内：
Predicate filterPredicate = convertFilter(filter, odpsTable, session);
...
private Predicate convertFilter(..., ConnectorSession session) {
    ...
    // 传 raw id 字符串（非预解析 ZoneId）；converter 惰性解析。≡ legacy DateUtils.getTimeZone() 来源。
    MaxComputePredicateConverter converter = new MaxComputePredicateConverter(
            columnTypeMap, dateTimePushDown, session.getTimeZone());
    return converter.convert(filter.get());
}
```

**⚠️ impl-review F1（real regression，已修）**：初版用 `ZoneId sourceZone = ZoneId.of(session.getTimeZone())` 在 `convertFilter` **eager 解析**。但 Doris `SET time_zone='CST'`（华区常见、本 Alibaba 连接器尤甚）被 `TimeUtils.checkTimeZoneValidAndStandardize:334` **逐字存储**（不标准化），而 `java.time.ZoneId.of("CST")` 抛 `ZoneRulesException`（PST/EST/MST 同；UTC/GMT/+08:00/Asia\* OK——已实测）。eager 解析 → 抛出 `planScan/getSplits`（**无 enclosing catch**）→ **整查询失败**，且对**任何** WHERE（含非 datetime 如 `id=5`）都炸——比 legacy（per-conjunct catch 降级、且仅 datetime 才解析 TZ）**更糟**，亦比翻闸前（`resolveProjectTimeZone` 永不抛）回归。
**修法**：构造签名改 `(Map, boolean, String sourceTimeZoneId)`；`ZoneId.of(sourceTimeZoneId)` 移入 `formatDateTimeLiteral`（仅 `convertTimeZone=true`=DATETIME/TIMESTAMP 分支、在 `convert()` 的 try 内）。效果（**legacy parity**）：
- 非 datetime 谓词 + CST → 不解析 TZ → 正常下推 ✅
- DATETIME/TIMESTAMP + CST → `ZoneId.of` 抛 → `convert()` catch → 该谓词 `NO_PREDICATE` 降级（BE 兜底过滤，结果仍正确）✅
- TIMESTAMP_NTZ + CST → 不转 TZ → 不解析 → 正常下推 ✅
**不纳入「CST→+08:00 正确解析」**（需 fe-core `TimeUtils.timeZoneAliasMap`，连接器 import-gate 禁；legacy 亦降级 ⇒ parity=降级，正确改进越界）。

### 死代码处置（决策点）

delta-2 后 `MaxComputeScanPlanProvider.resolveProjectTimeZone()`（私有 wrapper :293-295）**唯一调用点消失** → 删之（同文件、确定死、留之即死代码）。其委托的 `MCConnectorEndpoint.resolveProjectTimeZone(String)` :111-125 + 仅供它用的 `REGION_ZONE_MAP` :34-60（共 ~60 行）随之变为**零调用方**（grep 全 repo 0 test 引用）。

- **本设计取：删私有 wrapper（provider 内，确定死）；`MCConnectorEndpoint.resolveProjectTimeZone`+`REGION_ZONE_MAP` 暂留**，登记为 **Batch-D 死代码清理项**。理由（Rule 3 surgical）：correctness fix 不跨文件做大段删除，跨文件死代码归 Batch-D 清理阶段统一处理；该 public 方法语义（「项目区域 TZ」）非内在错误，仅「用错于谓词转换」，留之不破坏编译、不误导（已在 tracker 标注）。
- 备选（设计验证可推翻）：本 fix 一并删 `MCConnectorEndpoint.resolveProjectTimeZone`+`REGION_ZONE_MAP`，彻底无死代码。若设计验证/用户倾向「不留 bug 残骸」则采此。

## Implementation Plan

1. `MaxComputePredicateConverter`：三 datetime 分支改直接 format `LocalDateTime`；新增私有 `formatDateTimeLiteral`；删字符串版 `convertDateTimezone`；保留 `DATETIME_3/6_FORMATTER` 常量与构造签名。`UTC` 抽常量 `private static final ZoneId UTC = ZoneId.of("UTC")`（避免重复 `ZoneId.of`）。
2. `MaxComputeScanPlanProvider`：`convertFilter` 加 `ConnectorSession session` 形参、用 `ZoneId.of(session.getTimeZone())`；planScan 调用点传 `session`；删私有 `resolveProjectTimeZone()`。
3. **新增 UT** `MaxComputePredicateConverterTest`（连接器模块，无 fe-core/Mockito，纯构造 ConnectorExpression）——见 Test Plan。
4. tracker 登记 `MCConnectorEndpoint.resolveProjectTimeZone`+`REGION_ZONE_MAP` 为 Batch-D 死代码清理项。

## Risk Analysis

| Risk | Mitigation |
|---|---|
| `LocalDateTime.format` 与 legacy `getStringValue` 精度/格式分歧 | 已字节级核对（截断 + 定长 + 空格 + nanos 末3位恒0）；UT 钉确切串 `"2023-02-02 00:00:00.000"` / `.000000`。 |
| value 非 LocalDateTime（异常输入） | 防御 instanceof → 抛 → `convert()` catch → `NO_PREDICATE`（镜像 legacy 对非 DateLiteral 抛 AnalysisException 丢谓词）。UT 覆盖。 |
| session TZ 为 null / ctx 缺失 | scan 在 query 线程必有 ctx → `from(ctx)` 注入真 TZ；`ConnectorSessionImpl` 默认 "UTC"（仅 no-ctx 边角，legacy 此时 `systemDefault()`，scan 路径不可达）。设计中已说明、UT 注释标注。 |
| 改 source TZ 误伤同区部署 | session==project-region 时零变化；仅跨 TZ 纠偏，恢复 legacy parity。 |
| 删 wrapper 误删在用代码 | grep 确认 `resolveProjectTimeZone()` 唯一调用点即 line 287；删后编译验证。 |

## Test Plan

### Unit Tests（新增 `MaxComputePredicateConverterTest`，连接器模块）

钉 **WHY**（Rule 9）：谓词必须以正确格式 + 正确 TZ 基准下推，否则静默丢行 / 性能回归。

1. **delta-1 format（核心）**：DATETIME 列 `dt == LocalDateTime(2023,2,2,0,0,0)`，`dateTimePushDown=true`，sourceTZ=UTC → `convert(cmp).toString()` 含 `dt == "2023-02-02 00:00:00.000"`（空格分隔、3 位 fraction）。带 fraction 例（micro=123456 → `.123`）。
2. **TIMESTAMP**：→ `.000000` / `.123456`（6 位）。**TIMESTAMP_NTZ**：6 位且**不**做 TZ 转换（sourceTZ≠UTC 时仍按本地值 format）。
3. **delta-1 非降级（perf 回归 repro）**：sourceTZ=非 UTC（Asia/Shanghai）+ DATETIME 谓词 → 结果**非** `Predicate.NO_PREDICATE`（修前此处抛→NO_PREDICATE）。`assertNotSame(Predicate.NO_PREDICATE, result)` + 串非空。
4. **delta-2 TZ 转换**：sourceTZ=Asia/Shanghai（+08:00）、DATETIME `2023-02-02 08:00:00` → UTC `2023-02-02 00:00:00.000`。钉转换确切串（证基准为传入 sourceZone）。
5. **混合树不降级**：`AND(part_eq, datetime_cmp)` 整树正常转换（修前 datetime leaf 抛 → 整树 NO_PREDICATE）。
6. **mutation**（守门）：还原任一 delta（format 改回 `String.valueOf` / TZ 改回固定 UTC 常量）→ 对应断言变红。

### E2E Tests（CI 跳，真实 ODPS = 真值闸 DV-022）

- DATETIME/TIMESTAMP 列谓词在 **UTC 与非-UTC（如 Asia/Shanghai）session time_zone** 下均返回**正确行集**（修前：非 UTC 全表扫不下推 / 跨 TZ 丢行）。
- EXPLAIN/profile 证谓词确下推 ODPS（非 BE-only 兜底）。
- 需 ODPS 含 datetime 列表 + 跨 region/TZ 验证 → 归 DV-022，需用户 live 跑。

## 守门结果（DONE）

编译 BUILD SUCCESS；UT `MaxComputePredicateConverterTest` 13/13 + 连接器模块 55 run/0 fail/1 skip（live 测跳）；既有 `MaxComputeScanPlanProviderTest` 26/26 不受影响；checkstyle 0；import-gate exit 0。
mutation（in-place，因构造 API 改、revert-to-HEAD 不可编译）：M1 `format(formatter)`→`toString()` → 8/13 红（format 断言）；M2 `ZoneId.of(sourceTimeZoneId)`→`UTC` → 3/13 红（TZ 转换 + CST 降级断言）；还原 → 13/13 绿。

## impl-review（单 Agent 对抗，Ultracode off）

CHANGES-REQUIRED → 折入：
- **F1（real regression，已修）**：见上 delta-2「⚠️ impl-review F1」。已实测 `ZoneId.of("CST/PST/EST/MST")` 抛、`UTC/GMT/+08:00/Asia\*/Z/PRC` OK；`checkTimeZoneValidAndStandardize` 逐字存 CST（line 334）；legacy `convertPredicate:307-314` per-conjunct catch 降级。
- **F2（test gap，已补）**：F1 修后解析移入 converter → 由 `testUnparseableSessionZoneDegradesDatetimePredicate`（CST+datetime→NO_PREDICATE）+ `testUnparseableSessionZoneStillPushesNonDatetimePredicate`（CST+`id=5`→下推）+ `testTimestampNtzPushesUnderUnparseableZone` 覆盖。
- **F3（test breadth，部分补）**：加 `testDatetimeInListFormatsEachValue`（IN-list datetime 走 `convertIn`→`formatLiteralValue` 路径）。非-EQ 算子 / zero-offset-非-UTC（`+00:00`）经核**非缺陷**（复用同 format 路径），未补、接受。
- **F4（nit，接受不改）**：`formatLiteralValue:201` 仍对所有字面量算 `rawValue=String.valueOf(value)`，datetime 分支不再用之 → 对 datetime 字面量多跑一次 `LocalDateTime.toString()` 丢弃。**pre-existing**（翻闸前 datetime 分支即先算 rawValue），非本 fix 引入；rawValue 仍被 BOOLEAN/数值/STRING/CHAR/VARCHAR/DATE/null-type 分支用。Rule 2/3：纯 cosmetic/微 perf，不改。
- **确认 SAFE**（reviewer 证据）：format 字节级 parity（全 10^6 microsecond × scale 3/6，0 mismatch）；TZ source parity（同 `from(ctx)` 源、null-ctx 分歧 scan 路不可达、plan 期捕获比 legacy 运行时读更稳）；instanceof guard = legacy（非 DateLiteral 亦丢谓词）；NTZ scale-6 不转 TZ = legacy；死代码零调用方（grep 证）。
- **死代码登记**：`MCConnectorEndpoint.resolveProjectTimeZone` + `REGION_ZONE_MAP`（~60 行）翻闸后零调用方 → 登记 Batch-D 死代码清理（本 fix 仅删 provider 内死的私有 wrapper）。

## 决策类型

明确修复（用户定 Fix，Tier 1）。连接器局部、无 SPI 变更、与 legacy `MaxComputeScanNode` 谓词转换达成 parity。

**用户定夺（2026-06-08）**：
- **design-verify = Skip → 直接 implement**（设计已深度核码：format 字节级对齐 + TZ source 经 `from(ctx)` 确认）。仍走守门（编译+UT+checkstyle+import-gate+mutation）+ 末端 impl-review。
- **死代码 = Keep + defer Batch-D**：本 fix 仅删 provider 内死的私有 wrapper `resolveProjectTimeZone()`；`MCConnectorEndpoint.resolveProjectTimeZone`+`REGION_ZONE_MAP` 暂留、登记 Batch-D 死代码清理项。
