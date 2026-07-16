# FIX-L15 — `PAIMON_SCAN_METRICS` 悬空常量(死引用)

> reverify §1 L15 (P5-?)。严重度 🟡低。模块 = fe-core (`SummaryProfile`)。

## Problem

`SummaryProfile` 里保留了 `PAIMON_SCAN_METRICS = "Paimon Scan Metrics"` 这个 profile 分组键的三处引用,
但 **P5 迁移(commit `dbc38a265e5`,把 legacy paimon 子系统从 fe-core 移除、使 fe-core paimon-SDK-free)
之后再没有任何代码往这个分组写入数据**。它是一个纯粹的死引用:一个永远不会被填充的 profile 列。

对照活的 `ICEBERG_SCAN_METRICS`:iceberg 保留了 `IcebergMetricsReporter`,它在
`IcebergMetricsReporter.java:72,74` 主动 `getChildMap().get(ICEBERG_SCAN_METRICS)` / `new RuntimeProfile(ICEBERG_SCAN_METRICS)`
创建并填充该分组。paimon **没有**对应的 reporter(P5 删除时一并弃用了 paimon FE scan metrics),故其键悬空。

## HEAD recon(对 HEAD 复核 + grep 全量取证)

`PAIMON_SCAN_METRICS` 全代码库仅 3 处引用,**全部在 `SummaryProfile.java` 自身**:
- `:158` 常量声明 `public static final String PAIMON_SCAN_METRICS = "Paimon Scan Metrics";`
- `:218` `EXECUTION_SUMMARY_KEYS` 列表条目(display 顺序表)
- `:277` `EXECUTION_SUMMARY_KEYS_INDENTATION` 缩进 map 条目 `.put(PAIMON_SCAN_METRICS, 3)`

取证:
- `grep -rn "PAIMON_SCAN_METRICS" fe/` → 只上述 3 行。
- `grep -rn "Paimon Scan Metrics" fe/ be/` → 只 `:158` 一行(BE 侧亦无写入方)。
- `grep -rln "PaimonMetricsReporter\|PaimonScanMetrics"` → 无(确认无 paimon metrics 机制)。
- `fe/fe-core/src/test/` 无任何引用 → 删除不破测试。
- `git log -S "PAIMON_SCAN_METRICS"` → 写入方在 `dbc38a265e5`(P5)被移除。

对照:`ICEBERG_SCAN_METRICS` 有活的 `IcebergMetricsReporter` → **保留不动**。

## Design(surgical:删三行死引用)

P5 已验收弃用 paimon FE scan metrics(task list L15:「删三处死引用」为**推荐**路;
「加 connector-neutral scan-metrics SPI」标注为 feature、**非必需**,不做——本 SPI 迁移铁律是不在 fe-core
加 source-specific 结构,而一个真正通用的 scan-metrics SPI 是独立的 feature 债,超出「清死引用」的最小 scope)。

删除:
- `:158` 常量声明整行。
- `:218` `EXECUTION_SUMMARY_KEYS` 里的 `PAIMON_SCAN_METRICS,` 整行。
- `:277` 缩进 map 里的 `.put(PAIMON_SCAN_METRICS, 3)` 整行。

`ICEBERG_SCAN_METRICS` 三处(`:157`/`:217`/`:276`)全部**保留原样**(活引用)。

## Risk Analysis

- **零行为变更**:该键从未被任何 reporter 填充,故其在 `EXECUTION_SUMMARY_KEYS`(display 顺序)
  与缩进 map 中都是无效条目——删除不改变任何实际 profile 输出。既有含 iceberg scan metrics 的 profile 不受影响
  (独立键)。
- **无序列化耦合**:`EXECUTION_SUMMARY_KEYS` 只驱动 FE web-ui / profile 文本的展示顺序与集合;
  移除一个从不出现的键不影响其它键的展示。
- **无跨模块引用**:全库仅 `SummaryProfile` 自引用,连接器/BE/测试均不引用。

## Test Plan

### Unit Tests
- 无需新增单测:这是死引用删除,无行为可断言(强行造测=测「不存在的东西不存在」,无意义)。
- 回归护栏:`SummaryProfile` 及现有 profile 相关单测须继续编译通过(fe-core test-compile)。

### E2E Tests
- 不适用(无运行时行为变更;paimon profile 从不含此列)。

## 验证判据
- `mvn -o -f fe/pom.xml -pl fe-core -am test-compile` BUILD SUCCESS、0 checkstyle。
- 删后 `grep -rn "PAIMON_SCAN_METRICS" fe/` 返回空。
- `ICEBERG_SCAN_METRICS` 三处保持不变(diff 只删 paimon 三行)。
