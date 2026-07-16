# FIX-M1 — 翻闸 hive 上 TABLESAMPLE 被静默丢弃 → 连接器 opt-in 采样

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §3 M1（scope = **翻闸 hive**，"新激活"）。
> 批次 2（fe-core 通用节点）。设计红队 `wf_32decfa0-349`（3 lens）**推翻原"对所有连接器通用采样"方案**（UNSOUND）→ 改
> **连接器 opt-in**（用户 2026-07-11 签字"只修 hive"）。HEAD 复核（primary source）：translator 两臂、`getSplits`、
> legacy `HiveScanNode.selectFiles`、各连接器 range length。

## Problem（scope 更正：**仅 hive 回归**，非四连接器）

**采样(`TABLESAMPLE`)迁移前只有 hive 真正支持**——`HiveScanNode.selectFiles:435-467` 按文件字节大小随机挑一部分文件。
其它连接器（paimon/iceberg/maxcompute）的 legacy scan node **从不** sample tableSample（grep 全 fe-core scan node 仅
`HiveScanNode` 引用 tableSample）→ 对它们 TABLESAMPLE 一直是**静默 no-op（返回全表）**，非回归。

翻闸后 hive 走通用 `PluginDrivenScanNode`，`visitPhysicalFileScan` 插件臂 `:812-821` 只 `setSelectedPartitions`、
**不转发** `fileScan.getTableSample()`（唯一转发在 legacy hive 臂 `:837-840`，翻闸后死）→ hive 也退化成静默全表扫。
**这是 hive-only 回归**（reverify "翻闸 hive 新激活"）。

## Root Cause + 红队推翻的原方案

- **转发缺失**（真 bug）：插件臂无 `setTableSample`。
- **原方案（红队 UNSOUND，已弃）**：在通用节点对**所有** `PluginDrivenSplit` 按 `getLength()` 统一采样。红队证实
  `Split.getLength()` 语义**因连接器而异**——`ConnectorScanRange.getLength()` 默认 -1；**MaxCompute 默认 byte_size 策略
  每 range `.length(-1L)`**（`MaxComputeScanPlanProvider:345`），**Paimon JNI-read range 不设 length**（默认 -1，
  `PaimonScanRange:300`），MaxCompute row_offset 策略 length 是**行数非字节**。把 -1/行数喂进按字节采样 → totalSize 负 →
  ROWS 模式返**全表**、PERCENT 模式返全表或 1 个 split（乱）。故对无字节大小的连接器统一采样=**给它们造新错结果**。
- **结论**：分片能否按大小采样**只有连接器自己知道**；通用节点不能靠 split 本身判断，也不得按源名分支。

## Design（连接器 opt-in；connector-agnostic，无源判别、无属性解析）

### 1. SPI 能力开关（`ConnectorScanPlanProvider`，镜像 `supportsBatchScan` opt-in）
```java
/** 该连接器的 range 是否携带有意义的字节长度(ConnectorScanRange#getLength())，使引擎可按大小采样 TABLESAMPLE。
 *  返回 false(默认)→ TABLESAMPLE no-op(全表 + 一条 WARN)，即这些连接器 SPI 迁移前的行为。连接器**必须**确保它
 *  planScan 的每个 range 都有正字节长度才可返 true(MaxCompute 默认 byte_size/Paimon JNI range 报 -1，须保持 false)。
 *  镜像 supportsBatchScan 的 opt-in 形状 + Trino ConnectorMetadata.applySample。 */
default boolean supportsTableSample() { return false; }
```

### 2. hive 连接器声明支持（`HiveScanPlanProvider`）
```java
@Override public boolean supportsTableSample() { return true; }   // hive base/insert 文件 range 有正字节长度
```
+ 订正 class-javadoc `:64` 的 "No table sampling"。**仅 hive override**；iceberg/paimon/maxcompute 保持 false（不变）。

### 3. translator 转发（`PhysicalPlanTranslator.visitPhysicalFileScan` 插件臂，`:820` 后）
镜像 legacy hive 臂的 Nereids→analysis `TableSample` 转换（**通用转发**，不按源；实际是否采样由节点按连接器能力定）：
```java
if (fileScan.getTableSample().isPresent()) {
    pluginScanNode.setTableSample(new TableSample(fileScan.getTableSample().get().isPercent,
            fileScan.getTableSample().get().sampleValue, fileScan.getTableSample().get().seek));
}
```
**Hudi 不动**：`visitPhysicalHudiScan` legacy 臂 `:932-940` 本就不转发 tableSample（legacy parity；reverify 只点 file scan 臂）。

### 4. `PluginDrivenScanNode` 采样（`getSplits`）+ 能力门
```java
boolean applySample = tableSample != null
        && onPluginClassLoader(scanProvider, scanProvider::supportsTableSample);
if (tableSample != null && !applySample) {
    LOG.warn("TABLESAMPLE is not supported by connector {}; scanning the full table", <catalog type>);
}
...
boolean countPushdown = getPushDownAggNoGroupingOp() == TPushAggOp.COUNT && !applySample;   // (a)
...
// 建完 splits，return 前：
if (applySample) {
    long estimatedRowSize = 0;
    for (Column column : desc.getTable().getFullSchema()) {
        estimatedRowSize += column.getDataType().getSlotSize();
    }
    splits = sampleSplits(splits, tableSample, estimatedRowSize);
}
return splits;
```
新纯静态 helper（对齐 `HiveScanNode.selectFiles:435-467`，操作通用 `Split#getLength()`——**仅在连接器声明 range 有正
字节长度时才调**，故对 hive 安全）：
```java
static List<Split> sampleSplits(List<Split> splits, TableSample tableSample, long estimatedRowSize) {
    long totalSize = 0;
    for (Split split : splits) { totalSize += split.getLength(); }
    long sampleSize = tableSample.isPercent()
            ? totalSize * tableSample.getSampleValue() / 100
            : estimatedRowSize * tableSample.getSampleValue();
    Collections.shuffle(splits, new Random(tableSample.getSeek()));   // REPEATABLE 种子 → 可复现
    long selectedSize = 0; int index = 0;
    for (Split split : splits) {
        selectedSize += split.getLength(); index += 1;
        if (selectedSize >= sampleSize) { break; }
    }
    return splits.subList(0, index);
}
```
imports 加 `java.util.Random`、`analysis.TableSample`（`Column`/`Collections`/`Split`/`List`/`ArrayList` 已在）。

### 5. 完整性两门（红队确认 NECESSARY，均 gate 在 `applySample`）
- **(a) COUNT 下推抑制**（`getSplits`）：`countPushdown = COUNT && !applySample`。红队证实 paimon(`:471,515-521`)、
  **iceberg**(`IcebergScanPlanProvider:518-524`) 会把 count-eligible split **塌成一个** carrying 全表 precomputed count
  的 range，BE `CountReader` 直接服务、不读数据 → FE 侧采样裁不掉这个塌缩 range → `count(*) TABLESAMPLE` 返全表计数。
  gate 在 `applySample`：当前仅 hive 采样、hive 不塌缩 count（无此风险），但**将来 opt-in 且塌缩 count 的连接器**须此门；
  且 `!applySample` 使 **plain `count(*)`（无 sample）不受影响**（非回归）。
- **(b) batch 抑制**（`computeBatchMode` scanProvider null-guard 后）：`if (applySample) return false`。采样仅在同步
  `getSplits`；batch `startSplit`(`:1181-1247`) 不采样。M3 已把 batch 拓到无谓词大分区表 → 不抑制则 hive 大分区表
  `TABLESAMPLE` 走 batch 无采样=静默全扫。采样本须全量枚举 split（shuffle 全集），batch 对采样无收益 → 强制同步无损。

## Risk

- **connector-agnostic**：能力开关是连接器声明（非源名分支）；节点只按通用 `tableSample` 字段 + 通用能力布尔 + 通用
  `Split#getLength()`。无 `if(hive)`/instanceof/属性解析。符合铁律 + 用户 2026-07-11「只修 hive」签字。
- **非采样路径 = 严格 no-op**（红队 SOUND 核心）：`tableSample` 默认 null，仅新 translator 转发在 `isPresent()` 下 set；
  `sampleSplits`/countPushdown 额外子句/computeBatchMode 门全 gate 在 `tableSample!=null`(且 `applySample`)→ 普通查询零变。
- **非 hive 连接器**：转发了 tableSample 但 `supportsTableSample=false` → `applySample=false` → 不采样 + WARN（显式非静默，
  同迁移前全表结果）。iceberg/paimon/maxcompute 行为不变。
- **hive 采样正确性**：`HiveScanRange.getLength()` 正字节长度；percent/rows/seed 逐字节对齐 legacy `selectFiles`。
  注：粒度是 split（连接器已切）非 legacy 的 file-then-split，结果**近似** legacy 非逐行等同——采样本即近似语义，可接受。
- **subList 视图**：`splits.subList(0,index)`（同 legacy），getSplits 结果只读消费(`FileQueryScanNode:415/419/424`)，安全。
- **Hudi/其它**：不触碰。

## Test Plan

### Unit（`PluginDrivenScanNodeTableSampleTest`，fe-core 有 Mockito；RED-able）
纯静态 `sampleSplits` 直测（mock `Split.getLength()` 返正长度）：
- percent 100%→全选；percent 50%(均匀)→断言 `selectedSize>=sampleSize` 且去末元 `<sampleSize`(边界)。
- rows 模式：`estimatedRowSize×rows` 已知 + split 大小已知 → 断言选中数。
- **种子可复现**：同 `seek` 两次调用返回相同子集(元素身份)；不同 seek 可不同。
- 边界：空→空；sampleSize>totalSize→全选。
- **RED-able**：删采样调用则「采样后 < 全集」断言红。
（能力门 `applySample`/countPushdown/computeBatchMode 的 wiring：CALLS_REAL_METHODS mock 补测或 helper 直测 + live e2e，
登记同 DV-019。SPI 默认 false + hive override true 由连接器模块测/编译覆盖。）

### E2E（live-gated，docker-hive；须真集群）
- 强化 `test_hive_tablesample_p0.groovy`：现仅断言 EXPLAIN `count(*)[#7]`（抓不到 bug）。加**结果不变式**
  `count(*) TABLESAMPLE(...) <= count(*)`(全表)；**强基数缩减**(sample<full)须多文件 fixture 方稳定(单文件表采样最小粒度=1
  文件=全表)，本地不可跑 + fixture 未知 → 登记 live-gated，用户真集群验(memory `hms-iceberg-delegation-needs-e2e`)。
  矩阵：ROWS + PERCENT + REPEATABLE 复现 + `count(*) TABLESAMPLE`(验 COUNT 门) + 大分区表 TABLESAMPLE(验 batch 门)。
- （可选）非 hive 连接器 `TABLESAMPLE` 断言仍返全表 + FE log 有 WARN。
