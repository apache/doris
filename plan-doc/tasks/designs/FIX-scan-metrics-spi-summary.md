# FIX-SCAN-METRICS Summary — 连接器中立的扫描指标 SPI(恢复 paimon + iceberg profile scan metrics)

> 用户 2026-07-13 指令(取代 L15 删除:复活常量而非删死引用)。设计经 3-lens 对抗红队 `wf_0f803c49-7bb` 全
> SOUND_WITH_CHANGES,2 blocker + majors 全折入。

## Problem
老 `PaimonScanNode`/`IcebergScanNode` 把连接器 SDK 扫描指标(paimon:manifest 缓存命中/未命中、扫描耗时、
表文件跳过/保留;iceberg:planning 耗时、data/delete 文件、扫过/跳过 manifest 等)写进查询 profile。插件迁移
**两者都丢**(已登记偏差 `deviations-log.md:189` T02/T29):新 `PaimonScanPlanProvider` 不挂 metric registry;
新 `IcebergScanPlanProvider` 明确 "intentionally drop" metricsReporter。fe-core 两个 reporter 类只剩**死 legacy
`IcebergScanNode`** 引用(该节点已不在活路径)——iceberg 的 `ICEBERG_SCAN_METRICS` 只是靠死代码续命。

## Root Cause
插件连接器**不得 import fe-core**(铁律),而老 reporter 直接写 fe-core `SummaryProfile`。迁移时未建替代通道 →
功能整体丢失(纯诊断,不影响正确性)。

## Fix(统一 opt-in SPI,连接器采集 → 中立结构 → 内核写 profile)
- **fe-connector-api**:新中立值类型 `ConnectorScanProfile{groupName, scanLabel, metrics(不可变有序 map)}` +
  `default List<ConnectorScanProfile> collectScanProfiles(session)`(默认空,仿 `scannedPartitionCount`)。
- **fe-core**(`PluginDrivenScanNode`):`getSplits` planScan 后调 `collectScanProfiles` → 纯静态
  `writeScanProfilesInto(RuntimeProfile, profiles)`(get-or-create group + 逐 metric addInfoString,**无源分支**、
  可脱 ConnectContext 单测)写进 `SummaryProfile` 执行摘要;复活 `SummaryProfile.PAIMON_SCAN_METRICS` 常量(撤回
  L15 删除,供显示顺序)。
- **fe-connector-paimon**:port `PaimonMetricRegistry`(72 行,paimon-SDK-only)+ 新 `PaimonScanMetrics`
  (harvest + **自搬** `getPrettyStringMs`);`planScanInternal` `newScan()` 后挂 `withMetricRegistry`(仅
  `InnerTableScan`)、`plan()` 后 harvest → 按 queryId stash;override `collectScanProfiles`(排空)+
  `releaseReadTransaction`(清 stash 兜底,读事务仍 no-op)。
- **fe-connector-iceberg**:新 `IcebergScanProfileReporter implements MetricsReporter`(capture ScanReport +
  **自搬** `getPrettyStringMs`/`printByteWithUnit`,无 Guava/无 fe-core);**挂在 `planScanInternal` 数据+count 路**
  (NOT 共享 `buildScan`——避开 streaming/sys-table 的**泄漏 blocker**);同样 stash + collect + cleanup override。

## 红队(`wf_0f803c49-7bb`)折入
- **blocker1**(iceberg streaming 泄漏):reporter 从共享 `buildScan` 移到 `planScanInternal` 数据/count 路 →
  streaming/sys-table 不挂 → 不泄漏。
- **blocker2**(DebugUtil fe-core-only):两连接器各自**内联自搬** `getPrettyStringMs`/`printByteWithUnit`。
- **major**(可测性):fe-core 拆纯静态 `writeScanProfilesInto(裸 RuntimeProfile,…)`。
- 登记残余:streaming iceberg、iceberg manifest-cache 子路(默认 off,不调 planFiles)、iceberg sys-table、
  paimon 非 `AbstractDataTableScan`/空表 均不采集(=legacy parity);paimon scanLabel 弱限定(无 catalog 前缀,
  display-only);COUNT(*) 共享同一 plan/scan 故采集正常。

## Tests(全 RED-able,连接器无 Mockito 用真 SDK fixture)
- **fe-core** `PluginDrivenScanNodeScanProfileTest` 4:`writeScanProfilesInto` 建 group/child/infoString(RED)、
  空 no-op、共享 group、group 名常量镜像。
- **paimon** `PaimonScanMetricsTest` 4:真 `ScanMetrics.reportScan(ScanStats)` → harvest 出 5/3/7 计数(RED)、
  空 registry→empty、`prettyMs` 格式、GROUP_NAME 镜像。
- **iceberg** `IcebergScanProfileReporterTest` 4:真 `ImmutableScanReport` → report 入 stash(RED,含 "2.000 KB"
  字节格式)、空 queryId 不 stash、formatters、GROUP_NAME 镜像。

## Result
- api+fe-core+paimon(package/shade)+iceberg **BUILD SUCCESS + 0 Checkstyle**;import 门 exit 0。
- 靶向 UT:fe-core 4/4(+PartitionCount 8/8)、iceberg 4/4(+PartitionCount 4/4)、paimon 4/4(+Capability 5/5)。
- 全模块回归:见下(paimon/iceberg 全绿)。
- paimon-core 1.3.1(fe/pom.xml)含 `MANIFEST_HIT/MISSED_CACHE`——headline 缓存诊断可采。
- **e2e live-gated**:真集群 paimon/iceberg 查询后 profile 含 "Paimon/Iceberg Scan Metrics" 组 + manifest 缓存
  命中/未命中等条目。
- 死 legacy fe-core `IcebergMetricsReporter` 保持不动(随 P8 清死路径)。

## Commits
- code:api(2)+fe-core(2)+paimon(3)+iceberg(2)+ 3 测试。
