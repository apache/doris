# Doris New Parquet Reader Dictionary Predicate Pushdown 方案

## 背景

当前 new parquet reader 位于 `be/src/format/new_parquet/`，读取路径基于 Arrow
Parquet core API，并输出 Doris `Block` / `Column`。

当前已经实现的谓词相关能力主要有两类：

- row group 级 min/max/null statistics 裁剪；
- 读取谓词列后，用 Doris `ColumnPredicate` 生成 `SelectionVector`，再对非谓词列做延时物化。

但当前还没有实现 dictionary predicate pushdown。主要原因是
`ParquetColumnReaderFactory` 创建 Arrow `RecordReader` 时使用：

```cpp
_row_group->RecordReader(leaf_column_id, /*read_dictionary=*/false);
```

因此底层会把字典编码列直接解码成普通值。等 `ParquetReader` 执行
`ColumnPredicate::evaluate()` 时，已经看不到 dictionary page，也看不到 dictionary id。

本文档描述后续在 new parquet reader 中实现字典列谓词下推的设计方案。

## 目标

字典谓词下推的目标不是替代现有 statistics pruning，而是补充一类更强的过滤能力：

```sql
where c = 'abc'
where c in ('a', 'b', 'c')
where c != 'x'
```

如果 Parquet column chunk 是全字典编码，可以只检查 dictionary values 或 dictionary
ids，而不必先把整列解码成字符串列。

预期收益：

- 在 row group 级提前跳过不可能命中的 row group；
- 在 batch 级避免谓词列 string materialization；
- 和现有 `SelectionVector` / 延时物化路径结合，减少非谓词列读取量。

## 当前实现状态

### 已具备

- `ParquetStatisticsUtils` 已经有 file-local `ParquetColumnPredicate` 计划结构。
- `ParquetReader` 已经有谓词列优先读取流程。
- `SelectionVector` 已经能表示 batch 内选中 row offset。
- `ParquetColumnReader::select()` 已经能按 selection 对非谓词列做 selected read。

### 不具备

- 没有判断 column chunk 是否全字典编码。
- 没有读取 dictionary page 并转换成 Doris Column 的接口。
- 没有 dictionary id reader。
- 没有 dictionary value 到 dict id 的谓词重写。
- 没有把 dictionary id selection 接入当前 `SelectionVector`。

因此当前实现不能利用字典列谓词下推。

## 分层原则

字典谓词下推必须保持 file-local 语义：

- `TableColumnMapper` 负责把 table filter 转换成 file-local `ColumnPredicate`。
- `ParquetReader` 只消费 file-local `FileScanRequest`。
- 字典页、encoding、dictionary id 都属于 Parquet 文件格式层，不能泄露到
  Iceberg/table schema 层。

建议放置位置：

```text
be/src/format/new_parquet/parquet_statistics.*
    row group 级 dictionary pruning

be/src/format/new_parquet/column_reader.*
    dictionary values / dictionary ids 读取能力

be/src/format/new_parquet/parquet_reader.cpp
    将 dictionary selection 接入现有 predicate-first scan loop
```

## 方案一：Row Group 级字典裁剪

### 思路

对于全字典编码的 column chunk，dictionary page 包含该 row group 中所有可能出现的非
NULL 值。如果所有 dictionary values 都不能满足谓词，则整个 row group 可以跳过。

例子：

```text
predicate: name = 'Bob'
dictionary values: ['Alice', 'Cindy']

=> dictionary 中没有任何值满足 name = 'Bob'
=> row group 可以跳过
```

### 流程

```text
FileScanRequest.local_filters
    -> Build ParquetColumnPredicate
    -> 对每个 row group / column chunk：
       1. 判断 column chunk 是否全字典编码
       2. 读取 dictionary page
       3. 将 dictionary values materialize 成 Doris Column
       4. 对 dictionary values 执行 ColumnPredicate
       5. 如果没有任何 dictionary value 命中，则跳过 row group
```

### 全字典编码判断

Parquet 允许同一个 column chunk 先使用字典编码，后续 fallback 到 plain encoding。
这种 mixed encoding 不能用于 row group 级字典裁剪，否则会漏读 plain page 中的值。

判断方式可以参考旧 `vparquet`：

- 优先使用 `encoding_stats`：
  - 所有 `DATA_PAGE` 必须是 `PLAIN_DICTIONARY` 或 `RLE_DICTIONARY`；
  - 不能存在 count > 0 的非字典 data page。
- 如果没有 `encoding_stats`，退化检查 `encodings`：
  - 必须包含 dictionary encoding；
  - 除 dictionary encoding、`RLE`、`BIT_PACKED` 外，不能包含其它 data encoding。

需要注意：`RLE` / `BIT_PACKED` 可能用于 definition/repetition levels，不代表 value
不是字典编码。

### 支持的谓词

第一阶段建议只支持结构化 `ColumnPredicate`：

- `EQ`
- `IN`
- `NE`
- `NOT IN`
- `IS NULL`
- `IS NOT NULL`

其中 null 语义需要谨慎：

- dictionary page 不包含 NULL；
- `IS NULL` / `IS NOT NULL` 仍需要结合 column chunk null count；
- 不能仅靠 dictionary values 判断 NULL 谓词。

更复杂的表达式型 filter，例如 `lower(name) = 'abc'`，不在第一阶段支持。

### 正确性规则

row group 级裁剪必须保守：

- 不能确认全字典编码时，保留 row group；
- 不能读取 dictionary page 时，保留 row group；
- 谓词类型不支持时，保留 row group；
- 类型转换不安全时，保留 row group；
- NULL 语义不能确认时，保留 row group。

## 方案二：Batch 级 Dict Id Selection

### 思路

row group 不能整体跳过时，仍可以避免把谓词列完整解码成字符串列。

例子：

```text
dictionary values:
  id 0 -> 'Alice'
  id 1 -> 'Bob'
  id 2 -> 'Cindy'

predicate:
  name = 'Bob'

matched dict ids:
  {1}

data page ids:
  [0, 1, 1, 2, 0]

selection:
  [1, 2]
```

这时谓词列只需要扫描 dictionary ids，不需要 materialize 成 `ColumnString`。
非谓词列继续复用当前 `SelectionVector` 做延时物化。

### 流程

```text
打开 row group
    -> 对字典谓词列读取 dictionary values
    -> 对 dictionary values 执行 ColumnPredicate
    -> 得到 matched dict id set

读取 batch
    -> 读取该 batch 的 dictionary ids
    -> 用 matched dict id set 生成 SelectionVector
    -> 非谓词列按 SelectionVector selected read
    -> 如果字典谓词列也在 projection 中，再按需转换成真实值列
```

### Reader 抽象

建议在 `column_reader.*` 增加独立 reader 分支，而不是把逻辑塞进
`PrimitiveColumnReader::read()`：

```text
ParquetColumnReader
    PrimitiveColumnReader
    DictionaryColumnReader
```

或者先不新增类，通过内部 strategy 表达：

```text
PrimitiveColumnReader
    decoded reader path
    dictionary reader path
```

需要暴露的能力：

```text
read_dictionary_values(MutableColumnPtr* values)
read_dictionary_ids(int64_t rows, MutableColumnPtr* ids, int64_t* rows_read)
select_by_dictionary_ids(...)
materialize_dictionary_ids(...)
```

具体命名可以在实现时收敛，但边界应保持：

- dictionary values / ids 读取属于 `column_reader.*`；
- 用谓词生成 matched dict ids 属于 `parquet_statistics.*` 或新的 filter helper；
- 将 selection 接入 scan loop 属于 `parquet_reader.cpp`。

### Arrow RecordReader 的限制

Arrow Parquet `RecordReader` 有 `read_dictionary` 参数和 `ReadDictionary()` API。
但当前代码用的是 `read_dictionary=false`。

后续可以尝试：

```cpp
_row_group->RecordReader(leaf_column_id, /*read_dictionary=*/true)
```

需要验证：

- 只有全字典编码 column chunk 是否才会暴露 dictionary ids；
- mixed encoding 是否自动 fallback 为 decoded values；
- `RecordReader::read_dictionary()` 是否能可靠表示当前 reader 是否真的在读 ids；
- `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` 之外的类型支持情况；
- nullable column 下 ids 和 def levels 的行对齐方式。

从 Arrow 头文件注释看，dictionary expose 主要是 experimental API，且对 fully
dictionary encoded byte array column chunk 更可靠。因此第一版实现应该只针对 string-like
列，并且必须有 fallback。

## 和旧 vparquet 的关系

旧 `vparquet` 已经实现了一套字典过滤思路：

1. 判断 column chunk 是否全字典编码；
2. 读取 dictionary values 到临时 string column；
3. 执行原始谓词；
4. 将命中的 dictionary value 下标重写成 int dict code 谓词；
5. 读取 data page 时输出 dict id column；
6. 最终需要输出该列时再把 dict id 转回 string。

new parquet reader 可以复用这个设计思想，但不建议直接复用旧实现代码：

- 旧实现基于 Doris 自研 page decoder；
- new parquet reader 当前基于 Arrow Parquet core API；
- new reader 已有 `SelectionVector`，可以直接用 dict ids 生成 selection，而不一定要重写成
  `VExprContext`。

更适合 new reader 的方式是：

```text
dictionary values -> ColumnPredicate -> matched dict id set -> SelectionVector
```

而不是：

```text
dictionary values -> VExprContext -> rewrite predicate expression
```

## 推荐实施顺序

### 阶段一：Metadata 判断和 Row Group 级 Dictionary Pruning

新增能力：

- 判断 column chunk 是否全字典编码；
- 为 string-like primitive column 读取 dictionary values；
- 对 dictionary values 执行 `ColumnPredicate`；
- 在 `ParquetStatisticsUtils::SelectRowGroups()` 中额外执行 dictionary pruning。

约束：

- 只支持 `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` string-like 列；
- 只支持结构化 `ColumnPredicate`；
- 不处理 expression fallback；
- 不处理 mixed encoding；
- 不能确认时保守保留 row group。

### 阶段二：Batch 级 Dict Id Selection

新增能力：

- 构造 dictionary-aware predicate column reader；
- 读取 batch dictionary ids；
- 用 matched dict id set 生成 `SelectionVector`；
- 和现有延时物化路径合并。

约束：

- 谓词列如果也在 projection 中，需要按 selection materialize 成真实 Doris column；
- dict id column 不应泄露到 `ParquetReader` 输出 block；
- fallback 到 decoded value path 必须保持正确。

### 阶段三：扩展类型和复杂谓词

后续再考虑：

- numeric dictionary；
- decimal dictionary；
- timestamp/date dictionary；
- `LIKE` / prefix filter；
- expression fallback；
- page index + dictionary 组合裁剪。

## 当前实现是否可以直接做到

不能。

当前实现缺少以下关键点：

- `RecordReader` 使用 `read_dictionary=false`；
- 没有 dictionary metadata 判断；
- 没有 dictionary page 读取接口；
- 没有 dict id column 或 dict id selection；
- 谓词过滤发生在已经 materialize 的 Doris Column 上。

因此，当前最多只能做 decoded value filter，不能做 dictionary predicate pushdown。

## 关键设计结论

- 字典优化应该放在 Parquet file-local 层，不进入 table schema / Iceberg 层。
- 第一阶段优先做 row group 级 dictionary pruning，收益明确且风险低。
- 第二阶段再做 batch 级 dict id selection，与现有 `SelectionVector` 和延时物化结合。
- 基于 Arrow Parquet API 时，必须明确 fallback 策略，不能假设所有字典编码列都能暴露
  dictionary ids。
- 输出 block 必须始终是正常 Doris Column，不能把 dict id column 暴露给上层。
