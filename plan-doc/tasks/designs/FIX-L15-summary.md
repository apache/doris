# FIX-L15 Summary — `PAIMON_SCAN_METRICS` 悬空常量清除

## Problem
`SummaryProfile` 保留了 `PAIMON_SCAN_METRICS`(profile 分组键 `"Paimon Scan Metrics"`)三处引用,
但自 P5(`dbc38a265e5`,paimon 子系统移出 fe-core、fe-core paimon-SDK-free)后再无任何代码填充该分组
——纯死引用(一个永不出现的 profile 列)。

## Root Cause
P5 移除 paimon FE 子系统时弃用了 paimon FE scan metrics,却漏删了 `SummaryProfile` 里对应的常量与两处列表/缩进
条目。对照活的 `ICEBERG_SCAN_METRICS`:iceberg 保留 `IcebergMetricsReporter`(`IcebergMetricsReporter.java:72,74`
主动创建/填充该分组),故其键活;paimon 无对应 reporter,键悬空。

## Fix
删除 `SummaryProfile.java` 三处死引用:
- 常量声明 `public static final String PAIMON_SCAN_METRICS = "Paimon Scan Metrics";`
- `EXECUTION_SUMMARY_KEYS` 列表中的 `PAIMON_SCAN_METRICS,`
- `EXECUTION_SUMMARY_KEYS_INDENTATION` map 中的 `.put(PAIMON_SCAN_METRICS, 3)`

`ICEBERG_SCAN_METRICS` 三处(常量 + 列表 + 缩进)全部保留(活引用)。

**未做**「加 connector-neutral scan-metrics SPI」:task list 标注为 feature、非必需;一个真正通用的 scan-metrics
SPI 是独立 feature 债,超出「清死引用」的最小 scope,且与本系列铁律(fe-core 不加 source-specific 结构)正交。

## Tests
- 无新增单测:死引用删除,无运行时行为可断言(强行造测无意义)。
- 回归护栏:fe-core `test-compile` 须继续通过。

## Result
- `mvn -o -f fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false` → **BUILD SUCCESS**,
  **0 Checkstyle violations**。
- `grep -rn "PAIMON_SCAN_METRICS\|Paimon Scan Metrics" fe/` → 空(确认已清)。
- `ICEBERG_SCAN_METRICS` 5 处引用不变(diff 仅删 paimon 三行,`1 file changed, 3 deletions(-)`)。
- 零行为变更;不 live(无 e2e 欠账——该 profile 列本就从不出现)。

## Commit
- code:`SummaryProfile.java`(-3 行)。
