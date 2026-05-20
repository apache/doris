# Doris New Parquet Reader 谓词下推实现方案

## 背景

本文档分析 DuckDB Parquet reader 的谓词下推路径，并给出 Doris
`be/src/format/new_parquet/` 新 reader 的实现方案。

本方案只讨论新 Parquet reader 的 file-local 谓词下推，不改变既有
`vparquet` 路径，也不把 Iceberg/global schema 语义下沉到 `ParquetReader`。

## DuckDB 代码路径

DuckDB 的谓词下推分成两层：

- `MultiFileColumnMapper`：负责 global schema 到 local file schema 的 filter
  localization。
- `ParquetReader` / `ParquetStatisticsUtils`：只消费 local filter，负责 Parquet
  row group stats、bloom filter 和读时过滤。

关键代码位置：

- `src/common/multi_file/multi_file_column_mapper.cpp`
- `extension/parquet/parquet_reader.cpp`
- `extension/parquet/parquet_statistics.cpp`
- `extension/parquet/include/parquet_statistics.hpp`

### Filter Localization

DuckDB 的 `MultiFileColumnMapper::CreateMapping()` 会先构造 global column 到 local
column 的映射，然后处理 table filter：

1. 对文件中不存在、但可以由 default/constant 表达的列，先在 mapper 层直接对常量求值。
   如果 filter 不成立，则跳过整个文件。
2. 对存在本地列的 filter，进入 `CreateFilters()`。
3. 如果 local type 和 global type 一致，直接复制 filter。
4. 如果类型不同，`TryCastTableFilter()` 尝试把 filter 常量 cast 到 local type。
5. 如果 filter 无法安全 cast，则把 global filter 保留，同时把 global conversion
   expression 写入 reader 的 `expression_map`，让 reader 内部先算表达式，再执行 filter。

这个设计的核心是：Parquet reader 不理解 global schema。它只看到已经 localize
之后的 filter；无法 localize 的部分通过 expression reader 变成 reader 内的局部表达式。

### Row Group Pruning

DuckDB 在 `ParquetReader::PrepareRowGroupBuffer()` 中做 row group 级裁剪。

主要流程：

1. 找到当前输出列对应的 `ColumnReader`。
2. 如果该列存在 local filter，则读取该列 row group statistics。
3. 对普通类型调用 `filter.CheckStatistics(stats)`。
4. 对字符串列有特殊处理：Parquet 文件可能提供完整 min/max，而 DuckDB 的
   `StringStats` 只保留部分信息，因此会用 Parquet 原始 min/max 做更精确判断。
5. 对 float/double 有特殊处理：因为 NaN 不能简单由 `[min, max]` 覆盖，所以同时检查
   min/max 范围和 NaN 可能性。
6. 如果 stats 不能裁剪，再尝试 bloom filter。
7. 如果确定 filter 永远 false，则把当前 row group 的 offset 直接移动到末尾，相当于跳过
   row group。

DuckDB 的 bloom filter 逻辑在 `ParquetStatisticsUtils::BloomFilterExcludes()`：

- 只处理有常量的等值类 filter；
- 只处理支持的非 nested 类型；
- 读取 Parquet bloom filter header，要求 `BLOCK + UNCOMPRESSED + XXHASH`；
- 将 filter 常量编码成 Parquet bloom filter 使用的 hash；
- 如果 bloom filter 一定不包含该值，则裁剪 row group。

### Batch Filter And Lazy Materialization

DuckDB 在 `ParquetReader::ScanInternal()` 中处理 batch 级过滤。

当存在 filters 或 deletion filter 时：

1. 初始化 selection vector。
2. 优先读取 filter columns。
3. 每个 filter column 通过 `child_reader.Filter(...)` 直接产生/收窄 selection。
4. 如果 selection 已经为空，后续列只需要 `Skip(...)`。
5. 对非 filter columns，通过 `child_reader.Select(...)` 按 selection 读取结果列。
6. 如果 filter column 同时也是 projection column，它第一次读取出的结果可以直接复用。

这就是 DuckDB Parquet 延时物化的基础：谓词列先读，输出列按 selection 读。

## 对 Doris 新架构的结论

Doris 新架构应保留同样的分层边界：

- `TableColumnMapper` 是 table/global filter localization 的唯一入口。
- `ParquetReader` 只消费 `FileLocalFilter`，不理解 Iceberg schema evolution。
- `ParquetReader` 的 row group/page/bloom pruning 只基于 file-local column id 和
  file-local type。
- 无法变成 file-local predicate 的 filter 不能塞进 Parquet metadata pruning，只能走
  `reader_expression_map` 或 table-level finalize 后过滤。
- Doris 不引入 DuckDB `TableFilter` 风格的独立 filter AST。新架构统一复用 Doris
  已有的 `ColumnPredicate + VExprContext`。

## Doris 当前代码落点

新 reader 相关代码：

- `be/src/format/reader/file_reader.h`
- `be/src/format/reader/table_reader.h`
- `be/src/format/new_parquet/parquet_reader.h`
- `be/src/format/new_parquet/parquet_reader.cpp`
- `be/src/format/new_parquet/parquet_statistics.h`
- `be/src/format/new_parquet/parquet_statistics.cpp`
- `be/src/format/new_parquet/column_reader.h`
- `be/src/format/new_parquet/column_reader.cpp`

可复用的 Doris 谓词能力：

- `be/src/storage/predicate/column_predicate.h`
- `be/src/storage/predicate/comparison_predicate.h`
- `be/src/storage/predicate/in_list_predicate.h`
- `be/src/storage/predicate/null_predicate.h`
- `be/src/format/parquet/parquet_predicate.h`
- `be/src/format/parquet/parquet_block_split_bloom_filter.h`

旧 `vparquet` 中已经有 `ColumnPredicate::evaluate_and(ParquetPredicate::ColumnStat*)`
和 Parquet bloom filter 支持。新 reader 可以复用这些 predicate 语义，但不应复用旧
reader 的 table mapping、tuple descriptor、slot id、`TableInfoNode` 等表层路径。

`ColumnValueRange` 主要服务 OLAP scan key/range 归一化，不作为新 Parquet reader 的
核心 filter API。它可以继续存在于上游 scan normalize 阶段，但传入 file reader 的结构
应是 `ColumnPredicate + VExprContext`。

## 目标 API

### Filter 表达决策

Doris 新 reader 的 filter 表达采用两类已有结构：

- `ColumnPredicate`
  结构化单列谓词，表示 `EQ`、`IN`、`LT`、`IS NULL` 等稳定语义。它用于
  row group min/max、page index、dictionary、bloom filter 和解码后列过滤。

- `VExprContext`
  表达式过滤，表示复杂表达式、多列表达式、reader expression fallback 或无法结构化
  下推的 residual filter。它只能用于读时表达式执行，不能直接驱动 Parquet metadata
  pruning。

因此本文中的 `TableFilter` / `FileLocalFilter` 不是 DuckDB 式 filter AST，只是
Doris 分层 API 中的 filter carrier。

### FileLocalFilter

`FileLocalFilter` 继续作为 `ParquetReader` 的唯一 filter 输入：

- `file_column_id`
  file-local top-level field id。对于 primitive 列，它可以直接定位到 leaf column；
  对 struct，后续需要携带 child path。

- `conjunct`
  `VExprContext` 表达式过滤。它可以用于解码后过滤或 reader expression fallback，
  但不能直接驱动 row group stats、page index、dictionary、bloom filter。

- `predicates`
  `ColumnPredicate` 结构化单列谓词。它是 row group stats、page index、dictionary、
  bloom filter 的主要输入。

第一阶段只处理 `predicates`，不处理 `conjunct`。

### ParquetStatisticsUtils

`parquet_statistics.*` 参考 DuckDB 的 `ParquetStatisticsUtils` 组织方式：

```cpp
struct ParquetStatisticsUtils {
    static std::vector<ParquetColumnPredicate> BuildColumnPredicates(...);
    static ParquetColumnStatistics TransformColumnStatistics(...);
    static bool CheckStatistics(...);
    static bool RowGroupExcludes(...);
    static Status SelectRowGroups(...);
    static bool BloomFilterSupported(...);
};
```

和 DuckDB 的差异是：DuckDB 把 Parquet stats 转成 `BaseStatistics`，再调用
`TableFilter::CheckStatistics()`；Doris 新 reader 把 Parquet stats 转成
`ParquetColumnStatistics`，再调用 Doris `ColumnPredicate` 判断。

该工具类不包含 table schema、Iceberg field id、slot descriptor 或 tuple descriptor。

### ParquetColumnPredicate

建议把每个 `FileLocalFilter` 编译成 file-local 计划对象：

```cpp
struct ParquetColumnPredicate {
    reader::ColumnId file_column_id = -1;
    int leaf_column_id = -1;
    const ParquetColumnSchema* column_schema = nullptr;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};
```

同时定义 `ParquetColumnStatistics` 作为统计转换结果：

```cpp
struct ParquetColumnStatistics {
    Field min_value;
    Field max_value;
    bool has_null = false;
    bool has_not_null = false;
    bool has_null_count = false;
    bool has_min_max = false;
};
```

第一阶段只支持 primitive leaf column。struct/list/map 谓词先保守不下推。

## 第一阶段：Row Group Min/Max Pruning

第一阶段只实现 row group 级 min/max pruning。

输入：

- `FileScanRequest::local_filters[*].predicates`
- `ParquetColumnSchema`
- Arrow Parquet `FileMetaData`

输出：

- `selected_row_groups`

实现步骤：

1. 在 `ParquetReader::init()` 调用 `select_row_groups_by_statistics(...)` 时传入
   file schema。
2. `ParquetStatisticsUtils::BuildColumnPredicates()` 将 `local_filters` 编译成
   `ParquetColumnPredicate`。
3. 对每个 row group，遍历可下推谓词。
4. 根据 `leaf_column_id` 找到 `RowGroupMetaData::ColumnChunk(leaf_column_id)`。
5. `ParquetStatisticsUtils::TransformColumnStatistics()` 从 Arrow Parquet
   `ColumnChunkMetaData::statistics()` 读取 null_count、min、max，并转换成
   `ParquetColumnStatistics`。
6. `ParquetStatisticsUtils::CheckStatistics()` 调用
   `ColumnPredicate` 判断该 row group 是否可能匹配。
7. 任意必需谓词确定不可能匹配，则裁剪该 row group。
8. 无 stats、stats 不可信、类型不支持、谓词不支持时，保守保留 row group。

第一阶段当前实现支持类型：

- boolean
- int32 / int64
- float / double
- string / binary，按 Doris `StringRef` 的 byte-by-byte 比较语义处理

第一阶段暂不支持的类型：

- decimal
- date / datetime / timestamp
- int8 / int16 等需要额外逻辑类型解释的窄整数
- nested column

正确性规则：

- 不支持的类型必须保留 row group。
- nullable 列必须正确处理 all-null、has-null、is-null、is-not-null。当前实现会在
  null count 可用时支持 `IS NULL` / `IS NOT NULL` pruning。
- float/double 后续需要补充 NaN 语义处理；如果 Parquet stats 无法完整表达，必须保守
  保留 row group。
- string stats 需要注意 Parquet 新旧 min/max 字段和排序语义。
- schema change 的 cast 不在这里处理；必须已经由 `TableColumnMapper` 转成 file-local
  predicate。

## 第二阶段：Bloom Filter Pruning

Bloom filter pruning 放在 min/max 之后执行。

触发条件：

- min/max 不能裁剪；
- predicate 是 EQ 或 IN；
- column 是 primitive leaf；
- Parquet metadata 存在 bloom filter offset；
- Doris 能把谓词常量编码成 Parquet bloom filter 的物理 bytes/hash；
- bloom filter header 是 `BLOCK + UNCOMPRESSED + XXHASH`。

实现建议：

1. 在 `parquet_statistics.cpp` 中新增 `try_read_bloom_filter(...)`。
2. 可以复用旧 `ParquetBlockSplitBloomFilter` 的 block split bloom filter 实现。
3. 对同一个 row group 内同一列的多个 predicate，缓存 bloom filter，避免重复 IO。
4. 只对物理表示与 Doris predicate value 可安全互转的类型开启。

第一批支持类型建议：

- boolean
- int32
- int64
- float
- double
- string / binary

暂不支持：

- date/datetime 物理值和 Doris 内部值不一致的场景；
- decimal fixed byte array；
- tinyint/smallint 等需要反向物理编码的类型；
- nested column；
- 非等值谓词。

## 第三阶段：Page Index Pruning

Page index pruning 的输出不是 selected row groups，而是 row group 内部的 row ranges。

建议新增：

```cpp
struct ParquetRowGroupScanPlan {
    int row_group_id = -1;
    int64_t num_rows = 0;
    std::vector<RowRange> row_ranges;
};
```

`ParquetReaderScanState::selected_row_groups` 后续可以替换为
`std::vector<ParquetRowGroupScanPlan>`。

第一阶段不要把 page index 和 row group pruning 混在一个 bool 里，否则后续会很难接
延时物化和 selective read。

## 第四阶段：Decoded Value Filtering

当 filter 不能通过 metadata 裁剪时，需要在读取 batch 后执行。

建议按 DuckDB 的方式实现：

1. 将 projection columns 和 filter columns 合并为 required columns。
2. batch 开始时先读取 filter columns。
3. 对 `FileLocalFilter::predicates` 调用 `ColumnPredicate::evaluate(...)` 产生 selection。
4. 对 `FileLocalFilter::conjunct` 调用表达式执行，继续收窄 selection。
5. selection 为空时，跳过 output-only columns。
6. 读取 output-only columns 时按 selection 输出。
7. filter column 同时是 projection column 时，复用第一次读取结果，不重复物化。

当前已经落地第一版 decoded filtering / 延时物化骨架：

- `ParquetReaderScanState` 将当前 row group 的 reader 分成 filter columns 和 output
  columns；
- batch 开始先读取 filter columns，并用 `ColumnPredicate::evaluate(...)` 生成 selection；
- 如果 selection 为空，output-only columns 通过 `ParquetColumnReader::skip(...)` 推进；
- 如果 filter column 同时也是 projection column，输出阶段复用第一次解码出的列；
- output-only columns 当前仍按整批读取，再用 selection 过滤成输出 block。

这个版本只处理 `FileLocalFilter::predicates`。`FileLocalFilter::conjunct` 和
`reader_expression_map` fallback 后续需要在 filter columns 之后、output columns 之前执行，
继续收窄 selection。

当前 `ParquetColumnReader::skip(...)` 的策略：

- 无逻辑注解的基础物理 primitive 列优先调用 Arrow Parquet
  `RecordReader::SkipRecords(...)`，保证 nullable primitive 也按 row/record 语义跳过；
- 无逻辑注解的基础物理 primitive 列在 selection 非空时会通过
  `ParquetColumnReader::read_selected(...)` 执行 row-range 级 selective materialization；
- 尚未迁移到 `RecordReader` 的 string、decimal、timestamp 和 struct 暂时退化为
  read-and-discard；
- nested repetition 场景后续需要基于 record boundary 的 skip/selective read，不能直接用
  Arrow value-level `Skip(...)` 表示行级跳过。

后续如果要进一步接近 DuckDB 的 selective read，需要补充：

- `read_filter_batch(...)`
- `read_selective_batch(...)`
- `skip_rows(...)`
- 或者引入 `ParquetColumnReaderState` 保存 def/rep level 与 decoded value 缓存。

由于 Arrow Parquet `TypedColumnReader::Skip` 面向 physical values，不等价于 semantic
rows，所以 nullable 和 nested 类型不能直接靠 Arrow Skip 实现按行跳过。复杂类型需要
独立的 def/rep level row boundary 设计。

## Required Columns 规则

谓词下推会改变当前列裁剪的 required column 计算方式。

现在：

- `projected_file_columns` 展开成 `required_leaf_columns`。

加入谓词后：

- output columns 来自 `projected_file_columns`；
- filter columns 来自 `local_filters[*].file_column_id`；
- reader expression columns 来自 `reader_expression_map`；
- physical required leaves 是三者并集。

但是输出 block 仍然只包含 projection columns。filter-only columns 不能泄漏到输出
block。

## 和 TableColumnMapper 的边界

`ParquetReader` 不做以下事情：

- 不判断 Iceberg field id；
- 不处理 schema evolution；
- 不补 default/generated/partition columns；
- 不把 global filter 常量 cast 成 local type；
- 不把 finalize expression 塞进 metadata pruning；
- 不对 table-level cast 后的值做 bloom filter probe。

这些都属于 `TableColumnMapper` 或 `IcebergTableReader`。

`TableColumnMapper` 必须在进入 `ParquetReader` 前完成：

- trivial filter 复制；
- 类型不同但安全的 filter 常量 cast；
- 无法安全 cast 时生成 `reader_expression_map`；
- constant/default/partition column filter 的文件级裁剪判断。

## 实施顺序

### Step 1：扩展 Statistics API

- 修改 `select_row_groups_by_statistics` 参数，增加 file schema。
- 增加 `ParquetStatisticsUtils`，将 row group filter plan 编译、statistics 转换、
  predicate 检查和 row group 选择拆开。
- 只支持 primitive top-level leaf min/max / null count。
- 不改变 `ParquetReader::next()`，因此第一阶段只做 metadata pruning，不做 decoded
  batch filtering。

### Step 2：接入 Required Filter Columns

- `ParquetReader::init()` 计算 required leaves 时纳入 filter columns。
- 输出 block 仍然按 projection columns。
- filter-only columns 第一阶段仅用于 metadata pruning，不参与 batch filter。

### Step 3：Bloom Filter

- 复用 `ParquetBlockSplitBloomFilter`。
- 在 `parquet_statistics.cpp` 内实现 bloom filter 读取和 per-row-group cache。
- 只对 EQ/IN 和安全物理类型开启。

### Step 4：Batch Filter

- 为 `ParquetColumnReader` 增加 filter/read-selective 能力。
- filter columns 先读并生成 selection。
- output-only columns 按 selection 读。
- 谓词列和 projection 重叠时复用结果列。

### Step 5：Page Index

- 将 `selected_row_groups` 升级为 row group scan plan。
- row group plan 中携带 row ranges。
- column reader 根据 row ranges 做 selective read。

## 验收标准

第一阶段完成后：

- 对 `where int_col = 10`，能通过 row group min/max 跳过不可能命中的 row group。
- 对 `where int_col > 10 and int_col < 20`，多个 predicate 能共同裁剪 row group。
- 对无 stats 或不支持类型，结果正确且保守不裁剪。
- projection 只读输出列和必要 filter 列。
- `ParquetReader` 代码中不出现 table/global schema、Iceberg field id、slot descriptor
  依赖。

第二阶段完成后：

- 对 `where string_col = 'abc'`，存在 Parquet bloom filter 时能跳过不包含该值的 row group。
- bloom filter 缺失、格式不支持或类型不支持时保守保留。

第四阶段完成后：

- decoded value filter 能正确过滤 batch。
- filter column 同时是 projection column 时不会重复读取。
- filter-only column 不出现在输出 block。
