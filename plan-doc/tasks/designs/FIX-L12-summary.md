# FIX-L12 Summary — `selectedPartitionNum` 恢复为连接器真实扫描分区数(能力 SPI)

> 用户 2026-07-13 签字**选项 B**。memory `catalog-spi-selected-partition-num-sdk-distinct`。

## Problem
外部表扫描节点的 `selectedPartitionNum`(喂 EXPLAIN `partition=N/M` + `sql_block_rule partition_num` 治理)
迁移后从**连接器 SDK 规划后的真实 distinct 分区数**变成 **Nereids 按声明分区列剪枝数**(恒 ≥ 真实)。隐藏/transform
分区(iceberg `days(ts)`)、非分区列 manifest 剪枝(paimon)下**高报**→ 喂治理规则可**误拦**本只扫 1 分区的查询。

## Root Cause
`PluginDrivenScanNode.displayPartitionCounts` 用 `selectedPartitions.size()`(Nereids `PruneFileScanPartition`,
只认声明分区列、只 `LogicalFilter` 下跑,见不到 SDK manifest/residual/transform/bucket 剪枝)。legacy 连接器
(paimon `partitionInfoMaps.size()` keyed `dataSplit.partition()`、iceberg `partitionMapInfos.size()` keyed
`file().partition()`)按 SDK 规划后的 distinct 原生分区数报——迁移丢了这层。

## Fix(能力 SPI opt-in,仿 `supportsTableSample`;身份逻辑落连接器,通用节点 connector-agnostic)
- **SPI**(`ConnectorScanPlanProvider`):新 `default OptionalLong scannedPartitionCount(List<ConnectorScanRange>)`
  = `empty`(默认保留 Nereids 数)。
- **fe-core**(`PluginDrivenScanNode`):新纯静态 `resolveSelectedPartitionNum(nereids, countPushdown, connectorCount)`
  ——`!countPushdown && present` 时用连接器数,否则 Nereids;`getSplits` 中 planScan 后统一调用覆写 `selectedPartitionNum`。
  无 `instanceof`/源名分支。
- **paimon**(`PaimonScanPlanProvider`):override 收集 distinct 非空 `getPartitionValues()`(= 由 `dataSplit.partition()`
  渲染,单调注入),空则 `empty`。
- **iceberg**(`IcebergScanRange` 加 `getScannedPartitionKey()`=`specId|partitionDataJson`;`IcebergScanPlanProvider`
  override 收集 distinct 非 null key,空则 `empty`)。含 specId 以区分跨 spec 值撞(对齐 legacy PartitionData 对象去重)。
- **不动**:`totalPartitionNum`(M,legacy 本留 0)、batch/streaming 路(paimon 从不 stream;iceberg 仅在显式开
  `enable_external_table_batch_mode`(默认关)+ ≥1024 文件时 stream,那路留 0=legacy batch parity)。

## Tests(全 RED-able 载重断言)
- fe-core `PluginDrivenScanNodePartitionCountTest` +3:connector 报数覆写 Nereids(RED)/countPushdown 保留 Nereids/
  empty 保留 Nereids。**8/8**。
- paimon `PaimonScanPlanProviderCapabilityTest` +3:3 range/2 分区计 2(RED)/未分区 empty/默认 empty。**5/5**。
- iceberg 新 `IcebergScanPlanProviderPartitionCountTest`:key=specId|json、跨 spec 不撞、3 range/2 分区计 2(RED)、
  未分区 empty。**4/4**。

## 登记残余(Rule 12)
- `COUNT(*)` pushdown 保留 Nereids(collapsed range,保守 over-report,零覆盖)。
- iceberg BINARY/FIXED identity 分区列(渲 `[null]` 塌成 1,⚠ undercount)、null-partition bucket(⚠ ≤1 undercount);
  paimon binary 分区列(整表空 map → 回退 Nereids over-report,安全)、NaN-float/零长 native file(academic)。
- streaming-iceberg(显式开 batch + ≥1024 文件)本修 inert(=legacy batch parity,留 0,非回归)。
- 第三消费者 query-cache(`CacheAnalyzer` partitionNum)随之变 SDK-distinct 数,核实 benign(有效性键在 data-version
  token 非 partitionNum;零相关回归测)。

## Result
- fe-core / paimon / iceberg 三模块 **BUILD SUCCESS + 0 Checkstyle**;靶向 UT 8/8 + 5/5 + 4/4;import 门 exit 0。
- 3 sql_block_rule 治理测(iceberg/paimon/hive)均 identity 分区 + 无谓词 + 小表(<1024 文件同步路)→ 新旧同值不破测;
  paimon/iceberg 零 `partition=N/M` 黄金断言。
- 设计经 3-lens 对抗红队(`wf_f1524868-4b8`)全 SOUND_WITH_CHANGES,所有 major/minor 已折入设计文档 + 本 summary。
- **e2e live-gated**:隐藏/transform 分区 iceberg(<1024 文件,batch off)`WHERE ts` 单日 → `partition=1/M`;paimon
  非分区列剪枝 → 真实数;真集群回归。

## Commits
- code:SPI + fe-core + paimon + iceberg(4 文件)+ 3 测试。
