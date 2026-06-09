# [P4-T06e] FIX-NONPART-PRUNE-DATALOSS (GAP8) — review rounds

> issue 来源：Batch-D 红线扩充对抗复审 `wbw4xszrg`（schema-table unit，GAP8）。用户定 **Fix now，repro-test 先行**。
> 设计：`plan-doc/tasks/designs/P4-T06e-FIX-NONPART-PRUNE-DATALOSS-design.md`。

## 根因（5 处核码确认，见 design）
非分区 plugin 表 + WHERE → 静默 0 行。`supportInternalPartitionPruned()`=`!partCols.isEmpty()`(非分区=false) → `PruneFileScanPartition` else 支覆写 `SelectedPartitions(0,{},isPruned=true)` → `PluginDrivenScanNode.getSplits` 短路 0 split。坏 override=`35cfa50f988`(FIX-PART-GATES，dormant) + `072cd545c54`(P1-4，加短路激活)。

## 修法
Option A：`PluginDrivenExternalTable.supportInternalPartitionPruned()` → 无条件 `true`（镜像 legacy `MaxComputeExternalTable`/`IcebergExternalTable`）。非分区 → `pruneExternalPartitions:78` 返 NOT_PRUNED → 扫全表。**通用插件层修复**（非 MC 专有：CatalogFactory SPI_READY_TYPES={jdbc,es,trino,max_compute} 全经 PluginDrivenExternalTable→LogicalFileScan→PluginDrivenScanNode；当前仅 MC 翻闸暴露）。

## 改动（4 文件）
1. `PluginDrivenExternalTable.java`：`return true` + cautionary 注释（编码 data-loss WHY，防回退）。
2. `PluginDrivenExternalTablePartitionTest.java`：翻转钉错不变式的断言（`assertFalse`→`assertTrue`，方法名 `...ReportsNoPartitionsButStillOptsIntoPruning`）+ 重写 WHY + 类 Javadoc 更正。**此翻转即 repro**。
3. `PluginDrivenScanNodePartitionPruningTest.java`：helper 契约测保留 + 澄清注释（isPruned+空 只对真分区表裁剪正确）。
4. `test_max_compute_partition_prune.groovy`：加 `no_partition_tb` live-DV（直接 assertEquals 行数、无 .out 依赖、CI 跳）。

## 守门
编译 BUILD SUCCESS；UT `PluginDrivenExternalTablePartitionTest` 6/6 + `PluginDrivenScanNodePartitionPruningTest` 5/5 全绿；**mutation**：还原 fix(`true`→`!isEmpty()`) → repro 断言红（`expected:<true> but was:<false>`，FIX-NONPART-PRUNE-DATALOSS 文案）→ 还原后绿（RAM 备份 /dev/shm，diff 确认 identical）；checkstyle 0 violations；import-gate exit 0。

## Round 1 — 设计验证 workflow `wijd3qgk0`（4 lens clean-room 对抗）
**4 lens 全 design-sound，0 refuted。** 1 mustFix + 3 shouldFix → 全折入：
- **mustFix（Lens-2）**：fix 会令 `PluginDrivenExternalTablePartitionTest:98` 现 `assertFalse` 变红——该断言钉住 buggy 值（WHY 注释明文为 false 辩护）。**已翻转**为 assertTrue + 重写 WHY（= repro 本身）。
- **shouldFix（Lens-2）更正 blast-radius**：原稿「仅 MC / 注释 aspirational」错。jdbc/es/trino 经 CatalogFactory SPI_READY_TYPES 同为 PluginDrivenExternalTable → 本 bug 通用插件层。**已更正 design**。Option A 对全部 4 类中性或有益（非分区 pruneExternalPartitions 返 NOT_PRUNED），绝不有害。
- **shouldFix（Lens-2）MV-path**：QueryPartitionCollector:75/PartitionCompensator:246 对非分区改 true 后转分支但 benign=恢复 legacy parity（MaxComputeExternalTable/Iceberg 即无条件 true）。**已注 design**。
- **shouldFix（Lens-3）test 基建**：全 rule-transform 需真 CascadesContext、fe-core 无 pattern 可抄 → **轻量翻转断言作主 repro**（复用 tableWithCacheValue harness，真生产代码跑空分区列）。**已采纳**。
- Lens-1 root-cause skeptic 独立重推 5 步链每环成立、无逃逸路；Lens-4 确认 Option A 正确且优于冗余 guard（guard 对正确性冗余、不纳入，Rule 2/3），且不破坏「分区表裁剪到 0→0 行」合法语义。

## Round 2 — impl-review workflow `wza2khdb2`（2 lens 对抗）
**2 lens 全 approve，0 mustFix / 0 shouldFix。**
- **Lens A（correctness/completeness）**：prod diff 即 `return true`，注释每条 claim 对源码核实无误；grep 全树无其它 site 依赖旧行为（`PartitionCompensatorTest:371` 是 HMS-mock stub false、不涉本类；无残留旧不变式断言）；分区表 + 合法 pruned-to-zero 无回归。
- **Lens B（test-quality, Rule 9）**：独立**重跑 mutation**（还原→红→恢复→绿）确认 repro 非真空 + WHY 链对源码精确；helper 注释准确；groovy 行数断言对 DDL 正确、自验无 .out。
- **nits（2，已修）**：① PartitionPruningTest 注释截断方法名 `...OptsIntoPruning` → 拼全；② groovy seed-doc 补 `select * from no_partition_tb;`。
- **观察（非缺陷）**：实现扩既有 groovy 而非 design step-5 提的新文件——更优（复用 enable_profile×num_partitions×cross_partition 矩阵全模式覆盖非分区）。design 已注此分歧。

## 结论
设计验证 + impl-review 双 workflow 收敛 0 mustFix。**确诊 live 静默丢行回归已修复**（通用插件层，恢复 legacy parity）。真值闸 = live ODPS 非分区表 + WHERE 返正确行集（DV，CI 跳，已加 groovy）。auto-memory [[catalog-spi-nonpartitioned-prune-dataloss]] 已记。
