# UT 覆盖补充方案

## 已有测试

### parquet_column_reader_test.cpp

| 类别 | 测试 | 覆盖 |
|---|---|---|
| 标量类型 | `ReadAllSupportedPhysicalAndLogicalTypes` | 所有物理/逻辑类型的 flat read |
| 复杂类型 | `ReadSupportedComplexTypes` | struct/list/map 基本 read |
| Skip/Select | `SkipThenRead`、`SelectReadsOnlySelectedRanges` | 标量 skip/select |
| Struct 投影 | `ReadProjectedStructChildren`、`ReadProjectedNullableStructChildren` | 标量子字段投影 |
| List 投影 | `ReadProjectedListStructElementChildren` | list<struct> 元素字段投影 |
| Map 投影 | `ReadProjectedMapStructValueChildren` | map<k, struct> value 字段投影 |
| 未投影复杂 child | `ReadProjectedStructListChildOnly`、`SkipProjectedStructListChildOnlyThenRead`、`SelectProjectedStructListChildOnly` | struct 中未投影的 list child |
| 未投影 MAP child | `ReadProjectedStructMapChildOnly`、`SkipProjectedStructMapChildOnlyThenRead`、`SelectProjectedStructMapChildOnly` | struct 中未投影的 map child |
| Overflow | `ReadListWithOverflowAcrossChunks`、`SkipListWithOverflowThenRead`、`SelectListWithOverflow` 等 | list<scalar/struct/map>、list<list>、map<scalar/struct> 的溢出 |
| 嵌套 | `ReadListListWithOverflowAcrossChunks`、`ReadListStructWithOverflowAcrossChunks`、`ReadStructMapWithOverflowAcrossChunks` | 多层嵌套的 read/skip/select |

### parquet_reader_test.cpp

| 类别 | 测试 |
|---|---|
| Reader 生命周期 | `OpenStoresRequestAndCloseClearsState`、`CloseReleasesSharedIOContext` |
| 读 | `ReadSingleRowGroupThenEof`、`ReadMultipleRowGroups` |
| 过滤 | `ReadPredicateAndNonPredicateColumnsWithSelection`、`ColumnPredicateOnlyPrunesAndDoesNotFilterRowsInsideRowGroup`、`ReadMultiPredicateColumnsBeforeExpressionFilter`、`PredicateColumnFiltersBeforeNonPredicateRead`、`NonPredicateColumnKeepsSelectionFromPredicateColumn` |
| Pruning | `PredicateFiltersRowGroupsByStatistics`、`PredicateFiltersRowGroupsByDictionary`、`PlannerNarrowsRowRangesByPageIndex`、`InPredicateFiltersRowGroupsByDictionary`、`StatisticsPruningSkipsPrefixRowGroupsAndReadsLaterGroups` |
| Row position | `RowPositionReaderReturnsFileLocalPositions`、`RowPositionReaderKeepsPositionsAfterSelection`、`RowPositionReaderUsesFileLocalPositionsForScanRange` |
| Delete | `DeletePredicateFiltersRowPositions`、`QueryPredicateAndDeletePredicateFilterRowPositions` |
| Bloom filter | `EqPredicateUsesArrowHashAndPrunesAbsentIntValue`、`InPredicatePrunesOnlyWhenAllValuesAreAbsent`、`BooleanPredicateHashesAsParquetInt32`、`StringPredicateUsesArrowByteArrayHash`、`NullableAcceptingAndUnsupportedPredicatesKeepRowGroup` |
| Column predicate | `ColumnPredicatesDoNotForcePredicateMaterialization` |
| ColumnMapper | `CreatesComplexProjectionForStructChildren`、`CreatesComplexProjectionForMapValueStructChildren` |

### table_reader_test.cpp

| 类别 | 测试 |
|---|---|
| 聚合下推 | `PushDownCount/MinMax*` 系列（含 struct/list/map 子字段） |
| Schema 变化 | `ProjectedColumnsFillDefaultForParquetSchemaMismatch`、`ProjectedStructFillsMissingChildWithDefault` |
| Filter | `OpenReaderBuildsTableFiltersFromConjuncts`、`OpenReaderPushesMultiColumnConjunctToParquetReader` |
| Delete | `IcebergTableReaderAppliesDeletionVectorFile`、`IcebergTableReaderDoesNotPushDownAggregateWithDeletes` 等 |
| Virtual column | `IcebergVirtualColumnsUseRowLineageMetadata` |

## 需要补充

### P0：基本正确性

| 测试 | 说明 |
|---|---|
| `NullableListElement` | LIST 元素 nullable：`[null, 1]`、`[1, null]`、empty list、null list 各场景的 read/skip/select |
| `NullableMapValue` | MAP value nullable：`{k: null}`、`{k: v}`、empty map、null map 各场景的 read/skip/select |
| `ListStructNullableChild` | `List<Struct<nullable_child>>` 的 read/skip/select，验证 null child 的 def level 处理 |
| `MapStructNullableChild` | `Map<K, Struct<nullable_child>>` 同上 |
| `MapListNullableValue` | `Map<K, List<nullable V>>` 小 batch read + overflow，验证两层 cursor 与 overflow 一致 |

### P1：过滤交互

| 测试 | 说明 |
|---|---|
| `ConjunctFilterOnStructField` | `SELECT * FROM t WHERE s.id > 5`，验证 struct 子字段 conjunct 过滤正确 |
| `ConjunctFilterOnMapValue` | `SELECT * FROM t WHERE m['k'] > 5`，验证 map value 过滤 |
| `ComplexColumnSelectPath` | 非谓词复杂列（struct/list/map）在过滤后通过 `select()` 读取，验证 SelectionVector → column 的行数一致 |
| `ProjectionAndFilterInteraction` | `SELECT s.b FROM t WHERE s.a > 0`（同一 struct 内，a 是谓词列，b 是非谓词投影列），验证列 reader 数量、类型、行数正确 |
| `FilterThenNullStruct` | filter 后 struct 列为 null 时，子字段不会被错误物化 |

### P2：边界

| 测试 | 说明 |
|---|---|
| `EmptyStruct` | struct 无子字段的 read/skip |
| `AllChildrenProjectedOut` | struct 所有子字段都不投影（project_all_children=false，children 为空）时的行为——应返回 error 或 skip |
| `DeepNestedPath` | 三层以上嵌套（`a.b.c.d`）的 read/skip/select |
| `SkipLongRepeatedList` | 跳过跨越多个 page 的 long repeated list，验证 overflow + cursor 状态一致 |
| `SelectLongRepeatedList` | select 跨越 overflow 边界的 repeated list |

### P3：P4 完成后

| 测试 | 说明 |
|---|---|
| `PageLevelSkipByStatistics` | 构造一个 row group 包含多个 page，其中部分 page 的 min/max 完全落在 filter 范围外。验证 page-level skip 调用次数正确 |
| `PageLevelSkipByDictionary` | dictionary filter 触发 page skip |

## 现有测试未覆盖的复杂类型组合

以下组合在当前代码中有路径但无测试：

- `Map<K, Map<K2, V2>>`（nested map）
- `Array(Map<K, V>)`
- `Array(Struct<list_child, map_child>)`
- struct 内同时有投影和未投影的 non-scalar child（已覆盖 list-only 和 map-only，未覆盖同时存在）

建议在 P0 中优先覆盖前三项（属于已实现路径的回归保护），第四项在 P2 中补充。
