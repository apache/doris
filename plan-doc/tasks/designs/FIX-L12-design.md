# FIX-L12 — `selectedPartitionNum` 恢复为「连接器真实扫描分区数」(能力 SPI opt-in)

> reverify §1 L12 / 原始 review P5-3(CONFIRMED medium)。严重度 🟡低(治理相关)。模块 = fe-core 通用节点 + fe-connector-paimon + fe-connector-iceberg。
> **用户 2026-07-13 签字 = 选项 B**(加能力 SPI 回报 SDK-distinct,非登记偏差)。memory `catalog-spi-selected-partition-num-sdk-distinct`。

## Problem

外部表扫描节点的 `selectedPartitionNum`(喂 EXPLAIN `partition=N/M` 的 N + `sql_block_rule` 的 `partition_num`
治理)在迁移到通用插件节点后,含义从**旧 = 连接器 SDK 规划完 split 后的真实 distinct 原生分区数**变成
**新 = Nereids 按声明分区列剪枝后的分区项数**。新数**恒 ≥** 旧数;隐藏/transform 分区(iceberg `days(ts)`)与
非分区列 manifest 剪枝(paimon `WHERE 数据列`)下**高报**实际扫描分区数 → 喂 `sql_block_rule` 可**误拦**本只扫
1 个分区的查询(治理 false-positive)。

## HEAD recon(侦察 workflow `wf_6c516483-c34` + 直读确认)

**通用节点(现状)**:`PluginDrivenScanNode`
- `displayPartitionCounts(selectedPartitions)`(:302-308)= `{selectedPartitions.size(), totalPartitionNum}`,
  `selectedPartitions` 来自 Nereids `PruneFileScanPartition`(仅按声明分区列、仅 `LogicalFilter` 下跑,见不到
  SDK manifest/residual/transform/bucket 剪枝);默认 `NOT_PRUNED`。
- `this.selectedPartitionNum = partitionCounts[0]` 于 `getSplits:1007-1009`(planScan 之前)、`startSplit:1309-1311`
  (batch 路);渲染 `partition=N/M` 于 :357-358;`getSelectedPartitionNum()`(基类)喂 sql_block_rule。
- **iceberg 也走此节点**(legacy `IcebergScanNode.java` 已死;`PhysicalPlanTranslator:812-829` instanceof
  `PluginDrivenExternalTable` 臂);故 paimon+iceberg 两者均受影响。

**旧语义(SDK-distinct)**:
- paimon(`dbc38a265e5^` `PaimonScanNode.getSplits`):`Map<BinaryRow,…> partitionInfoMaps` keyed
  `dataSplit.partition()`(:412,:421-429),遍历**全部** DataSplit(含 count-pushdown 前),末
  `selectedPartitionNum = partitionInfoMaps.size()`(:514);**`totalPartitionNum` 从不赋值**(基类默认 0)。
- iceberg(`7ffeca95abf^` `IcebergScanNode`):`partitionMapInfos` keyed `(PartitionData) file().partition()`
  (:881-884),`selectedPartitionNum = partitionMapInfos.size()`(:934/:969;metadata 路 :987=0)。

**分区身份可从 range 复算吗?**(决定机制)
- paimon `PaimonScanRange.getPartitionValues()`(:154)= `getPartitionInfoMap(table, dataSplit.partition(), tz)`
  (planScan :515-516)——**直接由 `dataSplit.partition()` 渲染**,identity 分区(paimon 无隐藏 transform)→
  distinct map == distinct BinaryRow → **可用作 paimon 身份**。
- iceberg `IcebergScanRange.getPartitionValues()` **只含 identity 分区列**(:69-71)→ transform 分区**不忠实**;
  但同类另带 `partitionDataJson`(:66,planScan :787 `getPartitionDataJson(partitionData, spec, zone)`,= 序列化
  的整个 `PartitionData`)→ **iceberg 身份 = `partitionDataJson`**(null=未分区),忠实于 legacy 的 `file().partition()`。
- ⇒ **身份逻辑因连接器而异,必须落在连接器侧**,通用节点不得渲染/理解分区(铁律
  `catalog-spi-plugindriven-no-source-specific-code`)。

**两处 landmine**:
1. **provider 共享**:`resolveScanProvider()`→`connector.getScanPlanProvider(handle)`(:212-213),疑共享 →
   **禁在 provider 上 stash 可变计数**。
2. **COUNT(*) 折叠**:paimon planScan 在 countPushdown 下发**单个折叠 count range**(`countRepresentative`,
   :561-566)→ 返回 ranges 丢失 per-partition 信息 → 基于 ranges 复算会 undercount。iceberg COUNT 走 summary
   元数据(无 per-file range)同理。⇒ **countPushdown 下不 override,保留 Nereids 数**(保守:Nereids≥真实,治理
   只会更严不会漏拦;且 COUNT+partition_num 是边缘、零黄金覆盖)。

## Design

### SPI(fe-connector-api,additive,仿 `supportsTableSample`/`supportsBatchScan` opt-in)

`ConnectorScanPlanProvider` 加**默认返回空**的能力方法:
```java
/**
 * Distinct native partitions among the just-planned scan ranges — the count the connector's SDK
 * actually resolved after ITS full manifest/residual/transform/bucket pruning. Feeds the scan node's
 * selectedPartitionNum (EXPLAIN partition=N/M + sql_block_rule partition_num), so it reflects what is
 * really scanned, not the engine's declared-partition-column (Nereids) prune count.
 *
 * <p>The default returns empty, so the generic node keeps its Nereids-pruned count — correct for
 * directory-partitioned / requiredPartition-driven connectors (hive, MaxCompute) where the two
 * coincide. A predicate-driven connector whose SDK prunes beyond the engine (paimon manifest pruning,
 * iceberg hidden/transform partitioning) overrides this. Mirrors the supportsTableSample opt-in shape;
 * the connector downcasts its own range type (it produced these ranges) to read partition identity.</p>
 */
default OptionalLong scannedPartitionCount(List<ConnectorScanRange> scanRanges) {
    return OptionalLong.empty();
}
```

### fe-core 通用节点(`PluginDrivenScanNode`)

`getSplits` 中 planScan 之后(拿到 `ranges`、已设 Nereids 数于 :1009),加:
```java
// L12: prefer the connector's real scanned-partition count over the Nereids declared-column prune
// count, so partition=N/M and sql_block_rule reflect what is actually scanned. Opt-in (default empty)
// keeps the Nereids count for hive/MaxCompute (where they coincide). Suppressed under COUNT(*)
// pushdown, whose collapsed ranges have lost per-partition info (keep the conservative Nereids count).
OptionalLong connectorScannedPartitions = onPluginClassLoader(scanProvider,
        () -> scanProvider.scannedPartitionCount(ranges));
this.selectedPartitionNum = resolveSelectedPartitionNum(
        this.selectedPartitionNum, countPushdown, connectorScannedPartitions);
```
纯静态 helper(**RED-able 单测**,仿 M1/M3 的 `shouldUseBatchMode`/`displayPartitionCounts` 可测模式):
```java
/**
 * selectedPartitionNum to surface: the connector's real scanned-partition count when it reports one
 * (non-count scans of predicate-driven connectors), else the engine's Nereids-pruned count. Suppressed
 * under COUNT(*) pushdown (collapsed ranges lost per-partition info → keep the conservative, >= real
 * Nereids count). Pure so the gate is unit-testable.
 */
static long resolveSelectedPartitionNum(long nereidsSelectedPartitionNum, boolean countPushdown,
        OptionalLong connectorScannedPartitionCount) {
    if (!countPushdown && connectorScannedPartitionCount.isPresent()) {
        return connectorScannedPartitionCount.getAsLong();
    }
    return nereidsSelectedPartitionNum;
}
```
- **不动** `totalPartitionNum`(M):legacy 本就留 0;override 只在连接器回报 partition-bearing ranges 时触发
  (⇒ 声明了分区列 ⇒ `displayPartitionCounts` 已设 total,非 NOT_PRUNED),故不产生 `partition=N/0` 异常;
  sql_block_rule 只读 N;零黄金覆盖。
- **override 只在同步 `getSplits` 路**(红队订正):`selectedPartitionNum` 有**三**处写点——`getSplits:1009`、
  `startSplit:1311`(MC 分区切片 batch)、以及**`startStreamingSplit`(:1396,从不设)**。**iceberg 经 streaming
  flavor 进 batch**(override `streamingSplitEstimate`,**非** `supportsBatchScan`;`computeBatchMode:1208-1217`):
  当 `enable_external_table_batch_mode=true`(**默认 false**,`SessionVariable:3037`)且匹配文件 ≥ `num_files_in_batch_mode`
  (默认 1024)→ `startStreamingSplit` 不设 selectedPartitionNum(=0,**与 legacy batch parity**——legacy 也只在
  `doGetSplits` 设、batch 路留 0),本 override(仅 getSplits)对其 **inert**。**默认路(batch off)paimon+iceberg
  全尺寸均修复**;仅「显式开 batch mode + ≥1024 文件 iceberg」inert,**登记 streaming-iceberg 残余**(非回归:与
  legacy 同为 0)。paimon **无** streaming override → 永远 getSplits → 全修。
- **不按源分支**:通用节点只调统一方法 + 纯 helper,无 `instanceof`/源名。

### 连接器(身份逻辑落连接器)

**paimon** `PaimonScanPlanProvider` override:遍历 ranges,对 partition-bearing(`getPartitionValues()` 非空)的
`PaimonScanRange` 收集 distinct `getPartitionValues()` 到 `Set`;有则返回 `OptionalLong.of(size)`,全无(未分区)
→ `OptionalLong.empty()`(通用节点保留现状)。

**iceberg** `IcebergScanRange` 加访问器 `getScannedPartitionKey()`(= `partitionSpecId + "|" + partitionDataJson`,
null=未分区/无 PartitionData);**含 specId**(红队订正:`partitionDataJson` 是 value-only JSON,跨 spec 会撞——如
identity(id)=2 与 bucket(id)=2 都渲 `["2"]`;legacy 按 `PartitionData` 对象(含分区类型)本就区分 spec,故加 specId
= 更贴 legacy)。`IcebergScanPlanProvider` override:遍历 ranges 收集 distinct 非 null key;有则 `of(size)`,无则
`empty()`。访问器 package-private 即可(测试同包)。

## Risk Analysis / 忠实度

- **hive/MaxCompute 不变**:不 override → `empty` → 保留 Nereids 数(本就 == SDK-distinct)。零回归。
- **paimon/iceberg 常态忠实,且从不 over-count**(红队核实):paimon `getPartitionValues()`=`getPartitionInfoMap(
  table,dataSplit.partition(),tz)` 对 BinaryRow 确定性、单调注入(同分区→同 map、异分区→异 map);iceberg
  `specId|partitionDataJson` 对 `(PartitionData,spec,zone)` 确定性,单 spec 的 transform/hidden(day/bucket/truncate)
  渲染各异→distinct(**正是本修目标场景**)。二者均**不可能 over-count**(同 PartitionData/BinaryRow 在一次扫描内
  不会映到两个 key)→ **不引入任何超过 Nereids 的新 sql_block_rule false-positive**。
- **登记残余偏差**(Rule 12 不静默;红队补全):
  - `COUNT(*)` pushdown:保留 Nereids 数——保守 over-report(Nereids≥真实,治理只更严不漏拦),零覆盖。
  - `ignore_split_type`(L14 调试阀)丢光某分区全部 split → 该分区无 range 不计;legacy 在 `continue` 前已计 →
    可 undercount。调试态、极边缘。
  - **iceberg BINARY/FIXED identity 分区列**(⚠ undercount,governance-unsafe 方向):`serializePartitionValue`
    对 binary/fixed 抛异常→`getPartitionValues` 吞并 append null→每个 distinct binary 分区渲 `[null]`→塌成 1;
    legacy 按 PartitionData 分别计。**极罕见**(binary 作分区列本不常见);加 specId 不解此例(同 spec 内仍塌)。
  - **iceberg null-partition bucket**(⚠ undercount ≤1):partitioned 表某文件 `partition()` 非 `PartitionData`
    实例→key=null→本修不计;legacy 把 null 计为一个 distinct 桶(+1)。窄触发、至多差 1。
  - **paimon BINARY/VARBINARY 分区列**(over-report,安全方向):`getPartitionInfoMap` 遇不可序列化列**整表**返空
    map→override 见空→`empty`→回退 Nereids(≥真实,安全)。非 legacy 的 distinct-BinaryRow 数,但保守。
  - paimon 极端 academic:NaN-bit float 分区值皆渲 `"NaN"` 撞;native raw file 长度 ≤0 产 0 range→该分区不计。
  - `totalPartitionNum`(M)保持现状(见上,不产生 N/0 异常)。
  - **注**:iceberg「当前 unpartitioned spec + 旧分区文件」**非**偏差(红队订正):legacy 亦按当前默认 spec
    `spec().isPartitioned()` 门,current-unpartitioned 时 legacy 同样留 0 → 本修与 legacy 一致,不登记。
- **第三消费者:query-cache**(红队补,Rule 12):`getSelectedPartitionNum()` 亦被 `CacheAnalyzer:486`
  (`cacheTable.partitionNum`)→`sumOfPartitionNum:237`→SQL-cache proto `RowBatchBuilder:107 setPartitionNum` 读。
  paimon/iceberg 该存值将从 Nereids 数变 SDK-distinct 数。**核实 benign**:外表 SQL-cache 有效性键在 data-version
  token `getNewestUpdateVersionOrTime`(`CacheAnalyzer:489`,非 partitionNum);partitionNum 不 gate cacheMode;
  零 paimon/iceberg query-cache 回归测。登记不阻断。关联 L2(已恢复 query-cache SPI 能力)。
- **streaming-iceberg 残余**(见 Design §override 只在 getSplits):`enable_external_table_batch_mode=true`(默认关)
  + ≥1024 文件 iceberg 走 `startStreamingSplit` → selectedPartitionNum=0(与 legacy batch parity,非回归);本修 inert。
- **铁律**:身份逻辑全在连接器;通用节点 connector-agnostic(统一方法 + 纯 helper)。SPI 加法向后兼容,其它连接器
  (hive/hudi/mc/trino/es/jdbc)继承默认 `empty` 不受影响。
- **blast radius**:paimon/iceberg **零** `partition=N/M` 黄金断言;3 个 `sql_block_rule partition_num` 测均
  identity 分区(4 桶)+ 无谓词、小表(<1024 文件走同步路)→ 新旧同值(4)>阈值→仍拦→不破测(红队逐条核实,
  strict-`<` 语义 `SqlBlockRuleMgr:322,326`)。**新增 import**:`java.util.OptionalLong`(两文件)。

## Test Plan

### Unit Tests
- **fe-core**(有 Mockito):新 `resolveSelectedPartitionNum` 纯 helper 单测——**载重 RED 断言**=connector 报数
  且非 count→用 connector 数(RED:旧逻辑忽略,恒返 Nereids);另 guard:报数但 countPushdown→Nereids;`empty`→Nereids。
- **paimon**(无 Mockito,真 range 直构):**载重 RED 断言**=`scannedPartitionCount` 对含 N 个 distinct
  partitionValues(含同分区多 range 去重)的列表返回 `of(N)`(RED:默认返 empty);guard:全空 partitionValues→empty
  (**非 RED-able**,与默认同值,仅文档)。
- **iceberg**(无 Mockito):`getScannedPartitionKey`=`specId|partitionDataJson`;**载重 RED 断言**=
  `scannedPartitionCount` 对 distinct key 计数返回 `of(N)`(RED:默认 empty);guard:未分区(null key)→empty。

### E2E Tests(live-gated)
- ⚠ **前置(红队订正)**:测试须 `set enable_external_table_batch_mode=false`(默认)**或**小表(<1024 文件),
  否则 iceberg 走 streaming batch → `partition=0/0`、本修 inert → 假绿。
- 隐藏/transform 分区 iceberg 表(<1024 文件)`WHERE ts` 单日谓词 → EXPLAIN `partition=1/M`(迁移后错为 M/M);
  paimon 非分区列 manifest 剪枝 → `partition=<真实>/M`;各配 sql_block_rule partition_num 门验治理。
  真集群(memory `hms-iceberg-delegation-needs-e2e`),本地登记 gated。

## 验证判据
- fe-core `test-compile` BUILD SUCCESS + 0 checkstyle;paimon `package`(shade)靶向 UT 绿;iceberg 靶向 UT 绿。
- `bash tools/check-connector-imports.sh` exit 0(连接器不 import fe-core)。
- 三处新 UT 均 RED-able(去掉 override/helper 即挂)。
