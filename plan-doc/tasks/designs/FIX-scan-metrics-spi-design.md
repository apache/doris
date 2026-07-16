# FIX-SCAN-METRICS — 连接器中立的扫描指标 SPI(恢复 paimon + iceberg profile scan metrics)

> 用户 2026-07-13 指令:老版本 Paimon/Iceberg 的 profile scan metrics 在插件迁移中被丢(已登记偏差
> `deviations-log.md:189` T02/T29);要求在 connector SPI 框架上**统一设计**恢复。**本设计取代 L15 删除**
> (复活 `PAIMON_SCAN_METRICS` 常量,不再当死引用删)。

## Problem

老 `PaimonScanNode`/`IcebergScanNode` 在规划扫描时把连接器 SDK 的扫描指标写进查询 profile:
- **paimon**(`PaimonScanMetricsReporter`+`PaimonMetricRegistry`):SDK `ScanMetrics` → `last_scan_duration`、
  `scan_duration`(直方图)、`last_scanned_manifests`、`last_scan_skipped/resulted_table_files`、
  **`manifest_hit_cache`/`manifest_missed_cache`**。
- **iceberg**(`IcebergMetricsReporter`):SDK `ScanReport.scanMetrics()` → `planning` timer、`data/delete_files`、
  `skipped_*`、`total_size`、`scanned/skipped_manifests`、`equality/positional_delete_files` 等。

插件迁移后**两者都丢**:新 `PaimonScanPlanProvider` 不挂 metric registry;新 `IcebergScanPlanProvider:719-720`
**明确"intentionally drop"** metricsReporter。fe-core 的两个 reporter 类只剩死 legacy `IcebergScanNode` 引用
(该节点已不在活路径,前轮证实)。纯诊断能力(manifest 缓存命中率等排障关键),不影响正确性。

## 架构约束(为何不能照搬老代码)

老代码在 fe-core 的 ScanNode 里直接写 `SummaryProfile`。插件连接器**不得 import fe-core**(铁律
`catalog-spi-no-property-parsing-in-fecore`/import 门禁)。故需一条**连接器中立通道**:连接器采集 SDK 指标 →
渲染成中立结构 → fe-core 拿回写 profile。

## Design(统一 SPI)

### 1. fe-connector-api:中立值类型 + SPI 方法

新 `ConnectorScanProfile`(不可变值类型,连接器产、fe-core 消费):
```java
public final class ConnectorScanProfile {
    private final String groupName;              // "Paimon Scan Metrics" / "Iceberg Scan Metrics"
    private final String scanLabel;              // "Table Scan (db.tbl)"
    private final Map<String, String> metrics;   // 有序 name -> 已格式化 value
    // ctor(拷贝为不可变 LinkedHashMap) + 三 getter
}
```
`ConnectorScanPlanProvider` 新增(默认空,opt-in,仿 `scannedPartitionCount`/`supportsTableSample` 先例):
```java
/**
 * Connector SDK scan diagnostics harvested during the just-finished planScan (manifest cache hit/miss,
 * scan/planning durations, files/manifests scanned vs skipped), as connector-neutral profile groups the
 * engine writes into the query's SummaryProfile execution summary. Default empty (connector reports none).
 * The connector stashes these during planScan keyed by session.getQueryId() and drains them here — mirrors
 * the existing per-query queryId stashes (rewritable-deletes, manifest-cache tally).
 */
default List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
    return Collections.emptyList();
}
```

### 2. fe-core:通用写 profile(connector-agnostic)

`PluginDrivenScanNode.getSplits` 中 planScan 之后(TCCL pin,同 planScan):
```java
List<ConnectorScanProfile> scanProfiles = onPluginClassLoader(scanProvider,
        () -> scanProvider.collectScanProfiles(connectorSession));
appendConnectorScanProfiles(scanProfiles);
```
拆两层(红队 major:纯静态可测):
- 调用方 `appendConnectorScanProfiles(profiles)`:查 `SummaryProfile.getSummaryProfile(ConnectContext.get())`
  → `getExecutionSummary()`(null-guard),再委派纯静态 helper。
- **纯静态** `writeScanProfilesInto(RuntimeProfile executionSummary, List<ConnectorScanProfile> profiles)`
  (connector-agnostic——只搬运连接器给的 group/label/metrics,无 source 分支;取裸 `RuntimeProfile` 故可脱离
  `ConnectContext` 直接单测,**RED-able**):
```java
static void writeScanProfilesInto(RuntimeProfile executionSummary, List<ConnectorScanProfile> profiles) {
    if (executionSummary == null || profiles == null || profiles.isEmpty()) {
        return;
    }
    for (ConnectorScanProfile profile : profiles) {
        RuntimeProfile group = executionSummary.getChildMap().get(profile.getGroupName());
        if (group == null) {
            group = new RuntimeProfile(profile.getGroupName());
            executionSummary.addChild(group, true);
        }
        RuntimeProfile scan = new RuntimeProfile(profile.getScanLabel());
        for (Map.Entry<String, String> e : profile.getMetrics().entrySet()) {
            scan.addInfoString(e.getKey(), e.getValue());
        }
        group.addChild(scan, true);
    }
}
```
- 复活 `SummaryProfile.PAIMON_SCAN_METRICS`(撤回 L15 删除)+ 保留 `ICEBERG_SCAN_METRICS`:仅供
  `EXECUTION_SUMMARY_KEYS`(显示顺序)+ 缩进 map。**连接器 groupName 字符串须与之逐字一致**(stringly-typed
  coupling,文档登记;两侧各持字面量,连接器不 import fe-core)。
- 位置:非 batch/streaming 的同步 `getSplits` 路(paimon 恒走此路;iceberg <1024 文件 / batch-off 走此路)。
  streaming iceberg(≥1024 文件 + 显式开 batch)不采集——登记残余(与 L12 同边界;`streamSplits` 惰性,无
  planScan)。

> **⚠ 格式化器自搬(红队 blocker)**:legacy 渲染用 fe-core `DebugUtil.getPrettyStringMs`/`printByteWithUnit`,
> 连接器**不得 import fe-core**。故两连接器各自**内联自搬**这两个纯函数(`getPrettyStringMs`:HOUR/MINUTE/SECOND
> 拆分;`printByteWithUnit`:KB..TB + `DecimalFormat("0.000")`)——同 `IcebergScanPlanProvider.rootCauseMessage`
> 已有的自搬先例。`ConnectorScanProfile.metrics` 存**已格式化**字符串。

### 3. fe-connector-paimon:pull 采集

- 新 `PaimonScanMetrics`(连接器内 util,搬 legacy `PaimonScanMetricsReporter` 的抽取逻辑:counter/duration/
  histogram 渲染 + **自搬 `getPrettyStringMs`**)。
- 新 `PaimonMetricRegistry`(逐字搬 legacy 72 行,`implements org.apache.paimon.metrics.MetricRegistry`)。
- `planScanInternal`:`TableScan scan = readBuilder.newScan()` 后 `if (scan instanceof InnerTableScan) scan =
  ((InnerTableScan) scan).withMetricRegistry(registry)`;`scan.plan()` 后 `PaimonScanMetrics.harvest(registry,
  paimonTableName, tableDisplayName)` → `ConnectorScanProfile` → 存 `ConcurrentHashMap<queryId,
  List<ConnectorScanProfile>>`。
- override `collectScanProfiles(session)`:`remove(queryId)`。

### 4. fe-connector-iceberg:push 采集

- 新 `IcebergScanProfileReporter implements org.apache.iceberg.metrics.MetricsReporter`:构造绑 `queryId` +
  共享 stash;`report(ScanReport)` → 抽取(搬 legacy `IcebergMetricsReporter` 的 ScanReport 抽取 + **自搬
  `getPrettyStringMs`/`printByteWithUnit`**)→ `ConnectorScanProfile` → append 到 `stash[queryId]`。
- **附着点 = `planScanInternal`(数据+count 路),NOT 共享 `buildScan`(红队 blocker)**:在 `planScanInternal`
  `TableScan scan = buildScan(...)`(:527)后 `scan = scan.metricsReporter(new IcebergScanProfileReporter(
  session.getQueryId(), stash, tableName))`。**故意避开 `buildScan` 本身**——它还被 `streamSplits`(:416)、
  `planSystemTableScan`(:697)、`streamingSplitEstimate`(:374)复用;若挂 buildScan,streaming 路 `planFiles()`
  在后台线程 close 时**也会 fire report → stash 积累但从不 drain(getSplits 被 batch 旁路)→ 泄漏**。
  callback 在 planFiles 的 `CloseableIterable` **close** 时同步触发(planScanInternal try-with-resources 于
  planScan 线程 close,`report` 内联跑,非 worker 线程)。sys-table(planSystemTableScan 早返、走独立 buildScan)
  与 streaming 均**不挂** reporter → 不采集(登记残余)。
- override `collectScanProfiles(session)`:`remove(queryId)`。

### 5. 泄漏/关联

- **关联**:fe-core 在每次 planScan 后**紧接**同线程 `collectScanProfiles(queryId)` 排空 → 多扫描节点各取各的
  (node1 planScan→collect drains node1;node2 planScan→collect drains node2)。多节点同 queryId 即使并发,
  drain-all 也把该连接器该查询已积累的全写进共享 group(profile 去重靠 get-or-create group),净输出正确。
- **泄漏**:正常路 collect 排空;planScan 采集后抛异常的窄泄漏由连接器在 query-finish 清理——**复用 fe-core 已
  为每 planScan 注册的 `releaseReadTransaction(queryId)` 回调**(`PluginDrivenScanNode:1016`):paimon/iceberg
  override 该方法**顺带 `stash.remove(queryId)`**(当前二者继承 no-op 默认;新 override 只做 stash 清理,读事务
  仍 no-op)。

## Risk Analysis
- **connector-agnostic**:fe-core 只搬运连接器给的中立结构,无 source 分支;SPI 加法向后兼容,其它连接器继承默认
  空。iron rule 守。
- **零正确性影响**:纯 profile 诊断;不改扫描结果/split/分区数。
- **paimon 构建**:paimon SDK(`paimon-core`)已含 `metrics.*`/`InnerTableScan`(连接器已 bundle,验证过)。
- **登记残余(红队补全,均与 legacy parity 或纯诊断缺口,不影响正确性)**:
  - **streaming iceberg**(≥1024 文件 + 显式开 batch):reporter 不挂 streaming 路 → 不采集(=legacy batch 亦不
    在 profile 报);已避开 buildScan 故**无泄漏**。
  - **iceberg manifest-cache 子路**(`meta.cache.iceberg.manifest.enable=true`,**默认 off**):
    `planFileScanTaskWithManifestCache` 手工重建 task、**不调 `scan.planFiles()`** → report 不 fire → 该查询无
    iceberg scan metrics(**=legacy parity**,legacy 同不调 planFiles)。
  - **iceberg sys-table**:reporter 只挂数据+count 路,sys-table 走 `planSystemTableScan` 独立 buildScan → 不采集
    (P6.6 后仍不采集;out of scope)。
  - **paimon 非 `AbstractDataTableScan`**(sys/incremental scan,`withMetricRegistry` 默认 no-op)与 **iceberg/
    paimon 空表**(无 snapshot,`planFiles` 返空无 whenComplete)→ 无指标(=legacy)。
  - **paimon scanLabel 弱限定**(display-only 偏差):legacy 用 fe-core `TableIf.getNameWithFullQualifiers()`
    (catalog.db.tbl);连接器不得 import fe-core → 用连接器侧 `db.tbl`(无 catalog 前缀)。iceberg 不受影响
    (legacy 本用 `report.tableName()`)。
  - EXPLAIN VERBOSE 的 iceberg `manifest cache:` 行(现存,别的机制)不动。
- **stringly-typed group 名耦合**:连接器字面量须匹配 fe-core 常量。**镜像双测**(不能跨模块 assert):fe-core 断言
  `PAIMON_SCAN_METRICS.equals("Paimon Scan Metrics")`,连接器断言其字面量 == 同串。
- **stash 线程安全**:reporter confined 到同步 planScan 路(paimon 恒同步、iceberg 已避开 streaming)→ 单线程
  append/drain,race-free;`ConcurrentHashMap<queryId,List>` + `remove(queryId)` 原子排空即足。
- **泄漏兜底**:paimon/iceberg override `releaseReadTransaction(queryId)` **顺带 `stash.remove(queryId)`**(二者
  现继承 no-op 默认,新 override 只清 stash、读事务仍 no-op);fe-core 每 planScan 前注册该回调
  (`PluginDrivenScanNode:1016`)故查询结束必清。

## Test Plan
### Unit Tests
- **fe-core**:`ConnectorScanProfile` 值类型不可变性;`appendConnectorScanProfiles` 把 group/label/metrics 正确
  建树(用假 `ConnectorScanProfile` + 真 `RuntimeProfile`/`SummaryProfile`,fe-core 有 Mockito;RED:不写=无子)。
- **paimon**:`PaimonScanMetrics.harvest` 对含 ScanMetrics 组的假 registry 渲染出预期 map(counter/duration/
  histogram);`collectScanProfiles` 排空语义(stash→drain→空)。RED:默认空。
- **iceberg**:`IcebergScanProfileReporter.report(fakeScanReport)` append 出预期 `ConnectorScanProfile`;
  `collectScanProfiles` 排空。RED:默认空。
- 一致性断言:连接器 groupName 字面量 == fe-core 常量值(防 stringly-typed 漂移)。
### E2E(live-gated)
- 真集群 paimon/iceberg 表查询后 `SHOW QUERY PROFILE`/profile 含 "Paimon/Iceberg Scan Metrics" 组 + manifest
  缓存命中/未命中等条目。

## 验证判据
- fe-core test-compile + paimon(package/shade)+ iceberg 三模块 BUILD SUCCESS + 0 checkstyle;import 门 exit 0。
- 复活 `PAIMON_SCAN_METRICS` 后 `EXECUTION_SUMMARY_KEYS`/缩进含之;连接器 group 名一致性单测绿。
