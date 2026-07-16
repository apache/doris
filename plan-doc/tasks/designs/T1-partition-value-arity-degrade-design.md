# Problem

CI 996541 中 **6 个 iceberg 用例**在 bind 期硬失败：

```
IllegalStateException: connector supplied 2 partition values for 'year=2024/month=1'
but table has 3 partition columns [`year` int NULL, `month` int NULL, `day` int NULL]
  at PluginDrivenMvccExternalTable.listLatestPartitions(PluginDrivenMvccExternalTable.java:277)
  at PluginDrivenMvccExternalTable.materializeLatest(:178)
  at PluginDrivenMvccExternalTable.loadSnapshot(:341)
  at StatementContext.loadSnapshots(:995)
  at BindRelation.getLogicalPlan(:733)
```

受影响用例：`test_iceberg_partition_evolution`、`test_iceberg_partition_evolution_ddl`、
`test_iceberg_partition_evolution_query_write`、`test_iceberg_table_cache`、
`test_iceberg_rewrite_manifests`、`test_iceberg_position_deletes_sys_table`。

两种形态：**部分值**（`'year=2024/month=1'` 2 值 vs 3 列）与**零值**（`''` 0 值 vs 1 列）。

> ⚠️ 每个 suite 都在**第一个** query 就 abort ⇒ 下游断言在本分支**从未执行过**。本修复只保证「不再 crash」。

# Root Cause

**异构 arity 是 iceberg partition spec evolution 的合法形态，不是「连接器接错线」。**

- **列**取自**当前 spec**：`IcebergConnectorMetadata.buildTableSchema:439-456` 遍历 `table.spec().fields()`。
- **值**取自**每行数据文件的历史 spec**：`IcebergPartitionUtils.generateRawPartition:730-737` 读 `spec_id` → `table.specs().get(specId)` → 按**那一行的 spec** 的 field 数循环。

⇒ 老 spec 写的文件天然少给值；表从 UNPARTITIONED 演进而来时 spec-0 循环 0 次 → name `''`、0 值。

## 是本分支引入的（两个 commit 叠加）

1. `442a1081e6d`（含 `1a7e071a65f`）新增非-RANGE→LIST 路径（原意修 `selectedPartitionNum=0` 致 sql_block_rule 失效），使这些表**首次**走进 `materializeLatest:175` 的 LIST 分支。
2. **`cfb0958e607`（2026-07-13）把 size checkState 移出 per-partition try/catch** —— 本 bug 的直接触发者。commit message 自陈：

   > *"The size check is moved OUT of the per-partition try/catch in listLatestPartitions so a mis-wired connector surfaces immediately instead of being swallowed: the old in-catch checkState would log-and-skip the partition, leaving the built-item set short of the listed-name set and the table mis-reported as UNPARTITIONED (partition=0/0)."*

   **该 commit 的载重前提 = 「短值 ⇒ 连接器接错线 ⇒ bug」。本次 CI 用 6/6 全是合法 spec 演进证伪了它——误报率 100%。**

**master 无此问题**：`IcebergUtils.loadSnapshotCacheValue`（`442a1081e6d^`）对 `!isValidRelatedTable()` 直接 `IcebergPartitionInfo.empty()`，从不枚举 PARTITIONS 元数据表。

# Design

**回退 `cfb0958e607` 的 hoist**：把 arity 检查交还给 per-partition try/catch，恢复 master 的 log-and-skip → UNPARTITIONED 降级。

关键观察：`toListPartitionItem:321` **本来就有**一个等价的 `Preconditions.checkState(partitionValues.size() == types.size(), ...)`（`cfb0958e607` 把它降级为注释所称的 "defensive invariant"）。它在 try 内被调用 ⇒ **删掉 hoist 的那一块即可**，剩下 `:321` 自然抛 → `catch (Exception e)` → `LOG.warn` + skip → built-item set 短于 listed-name set → `PluginDrivenMvccSnapshot.isPartitionInvalid` → **UNPARTITIONED**。

**净效果 = 逐字节恢复 master 语义**（唯一差异：不再有 `HiveUtil.toPartitionValues` 名字解析回退——`HiveUtil` 已在删除阶段移除，且四个连接器现均供值，故不需要）。

## 用户 2026-07-15 拍板：本方案 vs 连接器侧降级

| | **回退 hoist（选中）** | 连接器侧名字列表比较降级 |
|---|---|---|
| 改动量 | 删 9 行 + 改注释 | +逻辑 + 3 单测，仅 iceberg |
| 覆盖 | hive/paimon/hudi/iceberg **全覆盖** | 仅 iceberg |
| 与 master | **逐字节恢复** | 近似 |
| `partition_values()` TVF | **不受影响** | 演进表 N 行→**0 行**（`PluginDrivenExternalTable:838` `getNameToPartitionValues` 未被 override，是被漏掉的第二消费者） |
| 残留 nested-source 洞 | **不存在**（根本不抛） | 混合 spec（≥1 顶层源 + ≥1 嵌套源）仍抛 |
| 代价 | 丢掉 `cfb0958e607` 的 fail-loud | 保住 fail-loud |

## 已否决

- **连接器侧降级**：见上表，且其设计的核心前提（"v1 保留 void field 使 arity 变 2、v2 不会"）经评审核实在本仓**零证据**——所引 `IcebergPartitionUtilsTest:753` 实为 v1（未传 format-version）且其断言对 arity 无鉴别力。
- **用 `partitionValueNullFlags` 补 NULL**：**正确性陷阱**。编码「老文件 day=NULL」这个假事实；`WHERE day=5` 会剪掉真正命中的分区 ⇒ 静默错结果。

# Implementation Plan

## 1. `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenMvccExternalTable.java`

**删除 `:271-279`**（hoist 的 `connectorValues` 提取 + 注释 + checkState），并把 `:281-283` 的 try 内注释改为同时说明 arity 不匹配也走降级：

```java
        for (ConnectorPartitionInfo part : parts) {
            String partitionName = part.getPartitionName();
            nameToLastModifiedMillis.put(partitionName, part.getLastModifiedMillis());
            try {
                // The connector supplies the parsed values in name-segment order; building from them is
                // byte-parity with legacy. Two shapes are tolerated by skipping the partition rather than
                // failing the whole query (parity PaimonUtil.generatePartitionInfo):
                //   - a value that is un-representable in its column type;
                //   - a value/column count mismatch, which is LEGITIMATE under iceberg partition spec
                //     evolution: the column list comes from the CURRENT spec while each row's values come
                //     from the spec its data file was written under, so rows written before an
                //     ADD/DROP PARTITION FIELD carry fewer values (an unpartitioned-origin table carries
                //     none). Skipping leaves the built-item set short of the listed-name set, which
                //     isPartitionInvalid turns into UNPARTITIONED -- the query still returns correct rows,
                //     it just loses partition pruning. Do NOT hoist this check out of the catch to
                //     "fail loud": that was tried (cfb0958e607) and every real-world hit was a legitimate
                //     spec evolution, not a mis-wired connector (CI 996541, 6 suites).
                nameToPartitionItem.put(partitionName,
                        toListPartitionItem(partitionName, types,
                                part.getOrderedPartitionValues(), part.getPartitionValueNullFlags()));
            } catch (Exception e) {
                LOG.warn("toListPartitionItem failed, partitionColumns: {}, partitionName: {}",
                        partitionColumns, partitionName, e);
            }
        }
```

**改 `toListPartitionItem:317-321` 的注释**——现文案称 `:321` 的 checkState 是 "defensive invariant"（因为 listLatestPartitions 已 fail-loud 检查过），回退后该句**变假**，而 `:321` 成为**载重**检查：

```java
        // The connector supplies the already-parsed values in name-segment order (hive/paimon/iceberg/hudi).
        // There is no name-parsing fallback anymore. This size check is LOAD-BEARING, not defensive: it is
        // the one that turns a heterogeneous-arity partition (legitimate under iceberg spec evolution) into
        // the skip -> UNPARTITIONED degrade. Its caller relies on it throwing inside the try/catch.
        List<String> partitionValues = connectorValues;
        Preconditions.checkState(partitionValues.size() == types.size(), partitionName + " vs. " + types);
```

**`Preconditions` import 保留**（`:321`/`:324` 仍在用）。`partitionColumns` 局部变量保留（`:266` 与 `LOG.warn` 仍在用）。

## 2. `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenMvccExternalTableTest.java`

**改写 `testValueCountMismatchFailsLoud:450-464`** —— 它字面编码了被本设计推翻的意图，且自带 `// MUTATION: moving the checkState back inside the try/catch (or dropping it) makes this red.`。改写为编码**新**意图：

```java
    @Test
    public void testValueCountMismatchDegradesToUnpartitioned() {
        // A value/column count mismatch is LEGITIMATE under iceberg partition spec evolution: the column
        // list comes from the CURRENT spec while a row's values come from the spec its data file was
        // written under. It must degrade to UNPARTITIONED (parity master / PaimonUtil.generatePartitionInfo),
        // NOT fail the query: cfb0958e607 hoisted this check out of the try/catch to "fail loud" and every
        // real-world hit was a legitimate evolution, taking down 6 suites (CI 996541).
        Fixture f = Fixture.with(Arrays.asList(
                cpi("dt=2024-01-01/region=cn", TS_2024_01_01)));
        // MUTATION: hoisting the checkState back out of the per-partition try/catch makes this red.
        Assertions.assertEquals(PartitionType.UNPARTITIONED, f.table.getPartitionType(Optional.empty()),
                "a value/column count mismatch must degrade to UNPARTITIONED, not fail the query");
    }
```

**新增 `testPartialValuesFromEvolvedSpecDegradeToUnpartitioned`** —— 锁住「零值」形态（表由 UNPARTITIONED 演进而来，spec-0 渲染出空名/零值），这是 6 个失败用例里 3 个的形态，上面那个 2-vs-1 用例覆盖不到：

```java
    @Test
    public void testZeroValuesFromUnpartitionedOriginDegradeToUnpartitioned() {
        // The spec-0 shape: rows written before the first ADD PARTITION KEY render to an empty partition
        // name and carry ZERO values while the table now has 1 partition column (test_iceberg_table_cache,
        // test_iceberg_partition_evolution_ddl, test_iceberg_partition_evolution_query_write).
        Fixture f = Fixture.with(Arrays.asList(cpiRaw("", TS_2024_01_01, Collections.emptyList())));
        Assertions.assertEquals(PartitionType.UNPARTITIONED, f.table.getPartitionType(Optional.empty()),
                "a zero-value partition from an unpartitioned-origin spec must degrade, not fail");
    }
```

> `cpi(name, ts)` 经 `orderedValuesOf(name)` 从名字派生值，空名会派生出空列表还是 1 个空串**需在施工时核实**；若 `orderedValuesOf("")` 不产出空列表，则需新增一个直接给定 `orderedValues` 的 helper（`cpiRaw`）。**施工时以真实代码为准。**

**保留不动**：`testValidPartitionSetIsList`、`testDuplicateRenderedNamesCollapseAndStayValid`、`testSuppliedPinIsNotReQueried` 等（均不涉 arity mismatch）。

# Risk Analysis

## 会打挂的现有单测

- **`PluginDrivenMvccExternalTableTest.testValueCountMismatchFailsLoud:450`** — **必红，已计划改写**（见上）。它是**唯一**引用 `"connector supplied"` 文案的测试（`grep -rn "connector supplied" --include=*.java` 全仓仅 2 命中：产品码 `:278` + 该测试 `:462`）。
- 其余：`PluginDrivenExternalTablePartitionTest` 不涉 arity mismatch 路径。

## 对 550 个通过用例的影响

**行为变化面 = 今天必然抛 `IllegalStateException` 的表**，即今天**必挂**的表。一个今天能跑通的查询，其所有分区都满足 `connectorValues.size() == types.size()` ⇒ `:321` 不抛 ⇒ 走原路径 ⇒ **逐字节不变**。故不可能命中那 550 个。

> ⚠️ 与「连接器侧降级」方案不同，本方案**不**改变 `metadata.listPartitions` 的返回值，故 `PluginDrivenExternalTable:838 getNameToPartitionValues`（`partition_values()` TVF）、`ShowPartitionsCommand:299` 等**其它 listPartitions 消费者一律不受影响**。这是本方案相对连接器侧降级的实质优势。

## 语义代价（明确接受）

丢掉 `cfb0958e607` 对**真正接错线的连接器**的 fail-loud：一个新连接器若忘了供 ordered values，将静默降级为 UNPARTITIONED（partition=0/0）而非报错。缓解：
1. `LOG.warn("toListPartitionItem failed, ...")` 仍逐分区打印，非完全静默。
2. 新连接器接入本就要过 `PluginDrivenMvccExternalTableTest` 那批 fixture 断言。
3. **这正是 master 今天的行为**——不是新造的坑，是恢复既有的（同样宽松的）契约。

## 副产品

`selectedPartitionNum` 不受影响：`IcebergScanPlanProvider.scannedPartitionCount:296-308` 从 scan range 的 `getScannedPartitionKey()` 独立构造，`PluginDrivenScanNode:330-333` 优先采用它，与 `listPartitions` 无关。

# Test Plan

## Unit Tests

`fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenMvccExternalTableTest.java`：

| 方法 | 断言意图 | 变异 |
|---|---|---|
| `testValueCountMismatchDegradesToUnpartitioned`（改写自 `...FailsLoud`） | 2 值 vs 1 列 ⇒ `PartitionType.UNPARTITIONED`，不抛 | 把 checkState 再 hoist 出 try ⇒ 红 |
| `testZeroValuesFromUnpartitionedOriginDegradeToUnpartitioned`（新增） | 0 值 vs 1 列（spec-0 形态）⇒ UNPARTITIONED | 同上 |
| `testValidPartitionSetIsList`（既有，不动） | **negative guard**：arity 匹配的表**仍**报 LIST，降级没有被扩大到正常表 | 若有人把降级写成无条件 ⇒ 红 |

`testValidPartitionSetIsList` 是关键的反向护栏：它保证本次修改**只**放宽 mismatch 路径，没有把正常表也降级。

## E2E Tests

**不新增。** 本轮修的就是 6 个既有 e2e（`test_iceberg_partition_evolution` 等），它们本身即回归闸门。

> ⚠️ 但它们**只闸住「不再 crash」**：每个 suite 都在第一个 query 就 abort，下游约 22 个 qt_ 断言（含 `$partitions` 系统表断言）在本分支**从未跑过**，状态未知，以真实 CI 为准。

# 仍未证实

1. **「修完就绿」未证**。见上：下游断言从未执行过。本修复只保证不再 crash。
2. **`orderedValuesOf("")` 的行为未核实** —— 决定 `testZeroValuesFromUnpartitioned...` 是否需要新 helper。施工时以真实代码为准。
3. **降级后 6 个 suite 的 `$partitions` 系统表断言**走的是另一条路径（`IcebergScanPlanProvider` 的 sys-table 规划），本修复不触碰，其结果未知。
4. **未审计** paimon/hudi/hive 是否存在同形态的异构 arity。本方案对它们同样放宽（这是优点），但「它们是否需要」未证明。
5. **丢掉 fail-loud 之后**，未来真正接错线的连接器只会 `LOG.warn` + 静默 partition=0/0。是否需要另立一个「连接器接入自检」机制（在 SPI 注册期而非查询期校验）—— 建议单开 issue，本轮不做。
