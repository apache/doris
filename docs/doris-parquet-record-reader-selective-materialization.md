# Doris Parquet RecordReader 延时物化方案

## 背景

新 `ParquetReader` 当前依赖 Arrow C++ Parquet core API，并已经实现了第一版延时物化骨架：

- 先读取谓词列；
- 用 `ColumnPredicate::evaluate(...)` 生成 selection；
- 谓词列同时是 projection 时复用已解码列；
- selection 为空时跳过 output-only 列；
- selection 非空时 output-only 列仍整批读取，再按 selection 过滤输出。

这个版本可以验证 `ParquetReader` / `TableColumnMapper` / `IcebergTableReader` 的分层是可行的，
但还不是 DuckDB 风格的高质量 selective materialization。核心原因是当前底层主要使用
Arrow Parquet `TypedColumnReader`，它的 `Skip(...)` 是 value-level API，不是 row/record-level API。

本文档说明：继续依赖 Arrow Parquet 三方库是否可以实现真正的 `Select(selection)`，以及下一步
`ParquetColumnReader` 应如何重构。

## 结论

继续依赖 Arrow Parquet 三方库是可行的，但不能只依赖当前使用的 `TypedColumnReader`。

下一阶段应把 Doris 新 `ParquetColumnReader` 的底层 decode engine 从
`TypedColumnReader` 调整为 Arrow Parquet 的 record-level reader：

```cpp
parquet::internal::RecordReader
```

然后 Doris 自己在 `ParquetColumnReader` 层实现：

- `read_records(num_records)`
- `skip_records(num_records)`
- `read_selected(selection)`
- Doris column assembly

也就是说：

- Parquet page、encoding、dictionary、definition/repetition level decode 继续交给 Arrow Parquet；
- selection 调度、Doris `Column` 构造、复杂类型组装由 Doris 自己负责；
- `RecordReader` 只作为 `be/src/format/new_parquet/column_reader.*` 内部依赖，不能泄漏到
  `FileReader`、`TableReader`、`IcebergTableReader` API。

经过当前分支上的部分重构，这条路径已经验证了最关键的假设：

- primitive nullable skip 可以从 value-level `TypedColumnReader::Skip(...)` 切换到
  row-level `RecordReader::SkipRecords(...)`；
- `Select(selection)` 不需要 Arrow C++ 直接提供，可以在 Doris `ParquetColumnReader` 层把
  selection 压缩成 row ranges 后用 `SkipRecords(...)` / `ReadRecords(...)` 实现；
- `ParquetReader` 的 filter-first / output-second 调度可以保持不变，只需要把 output column 的
  读取入口从整批 `read_batch(...)` 切换成 `read_selected(...)`；
- Arrow 继续负责 Parquet page、encoding、dictionary、definition/repetition level decode，
  Doris 负责 scan 语义和 Doris column assembly。

因此后续不应回到围绕 `TypedColumnReader::Skip(...)` 打补丁的方向，而应继续推进
RecordReader-backed `ParquetColumnReader`。

## TypedColumnReader 的限制

Arrow Parquet `TypedColumnReader::Skip(...)` 的语义是跳过 physical values：

```cpp
virtual int64_t Skip(int64_t num_values_to_skip) = 0;
```

它不等价于 SQL row / Parquet record。

### Required flat primitive

对于 required flat primitive 列：

```sql
a int not null
```

每一行正好对应一个 physical value：

```text
rows:   r0 r1 r2 r3
values: 10 20 30 40
```

此时 `Skip(2)` 可以安全地表示跳过 2 行。

### Nullable primitive

对于 nullable 列：

```sql
a int null
```

数据可能是：

```text
rows:   r0   r1    r2   r3
a:      10   NULL  30   40
values: 10         30   40
defs:   1    0     1    1
```

如果要跳过前 2 行，正确位置应该来到 `r2 = 30`。

但 `TypedColumnReader::Skip(2)` 会跳过两个 physical values，即 `10` 和 `30`，下一次读取会来到
`r3 = 40`。因此 nullable 列不能用 value-level skip 表示 row-level skip。

### Repeated / nested

对于 repeated 列：

```sql
a array<int>
```

数据可能是：

```text
r0 = [1, 2, 3]
r1 = [4, 5]
```

跳过 1 行应该跳过整个 `[1, 2, 3]`。

但 `TypedColumnReader::Skip(1)` 只跳过一个 physical value `1`，reader 仍然停留在 `r0` 内部。

所以 `TypedColumnReader` 只适合作为基础顺序读取 API，不适合作为完整延时物化的 row-level
reader API。

## RecordReader 的层级

Arrow Parquet 提供了 record-level API：

```cpp
namespace parquet::internal {
class RecordReader {
public:
    virtual int64_t ReadRecords(int64_t num_records) = 0;
    virtual int64_t SkipRecords(int64_t num_records) = 0;
    int16_t* def_levels() const;
    int16_t* rep_levels() const;
    uint8_t* values() const;
    int64_t values_written() const;
    int64_t levels_position() const;
    int64_t levels_written() const;
};
}
```

它位于 Arrow Parquet 的 row group / column reader 层：

```text
parquet::ParquetFileReader
  -> parquet::RowGroupReader
      -> parquet::ColumnReader / TypedColumnReader
          value-level API
      -> parquet::internal::RecordReader
          record-level API
```

它比 `TypedColumnReader` 高一层，因为它理解 definition/repetition levels 和 record boundary；
但它仍然比 `parquet::arrow::FileReader` 低一层，因为它不直接输出 Arrow Array / RecordBatch。

对 Doris 来说，这正好是合适的边界：

- 不使用 `parquet::arrow::FileReader` 输出 Arrow Array；
- 不重写 Parquet page/encoding/level decode；
- 在 Doris `ParquetColumnReader` 中消费 values + def/rep levels，组装 Doris column。

## API 稳定性风险

`RecordReader` 位于 `parquet::internal` namespace，并且头文件标注了 experimental。

这意味着它不是 Arrow Parquet 对外最稳定的 public API。Doris 如果采用它，需要遵守以下原则：

- 只在 `be/src/format/new_parquet/column_reader.*` 内部引用；
- 不在 Doris 新 reader 公共头文件中暴露 `parquet::internal::RecordReader`；
- 用 Doris 自己的 `ParquetColumnReader` API 包一层，隔离 Arrow API 变动；
- 升级 Arrow 时重点验证 `RecordReader` 的行为和 ABI/API 变化；
- 如果未来 Arrow 提供稳定的 record-level reader API，再替换内部实现。

由于 Doris 使用 vendored thirdparty Arrow，这个风险是可控的，但必须收敛在内部 wrapper 中。

## Select(selection) 的实现方式

Arrow `RecordReader` 提供 row-level `ReadRecords` 和 `SkipRecords`，但不直接提供
`Select(selection)`。

因此 Doris 需要在 `ParquetColumnReader` 层把 selection 转换成连续 row ranges：

```text
selection: [1, 2, 3, 10, 11, 20]

skip 1
read 3
skip 6
read 2
skip 8
read 1
```

建议 API 形状：

```cpp
struct RowRange {
    int64_t start = 0;
    int64_t length = 0;
};

class ParquetColumnReader {
public:
    Status read_records(int64_t rows, MutableColumnPtr* column, int64_t* rows_read);
    Status skip_records(int64_t rows);
    Status read_selected(const std::vector<uint16_t>& selection, uint16_t selected_rows,
                         int64_t batch_rows, MutableColumnPtr* column);
};
```

`read_selected(...)` 内部流程：

1. 将 selection 压缩为连续 row ranges。
2. 从当前 batch 起点开始维护 `cursor`。
3. 对每个 range：
   - `skip_records(range.start - cursor)`；
   - `read_records(range.length)`；
   - 将读取结果 append 到目标 Doris column；
   - 更新 `cursor`。
4. 末尾 `skip_records(batch_rows - cursor)`，保证该列 reader 与 batch 物理进度对齐。

## 与 DuckDB Select 的关系

DuckDB 的 Parquet reader 中，谓词列先通过 filter 路径产生 selection，projection 列再通过
`Select(...)` 按 selection 物化。

Doris 新方案应对齐这个模型，但实现方式不同：

- DuckDB 自己实现了 Parquet column reader、page reader 和 Dremel 组装；
- Doris 新方案复用 Arrow Parquet `RecordReader` 处理底层 decode；
- Doris 在 `ParquetColumnReader` 层实现 DuckDB-style `Select(selection)` 调度和 Doris column
  assembly。

因此 Doris 不需要重写完整 Parquet 解码栈，但仍需要实现自己的 column reader 状态机。

## 复杂类型处理

`RecordReader` 可以提供 leaf column 的 values、definition levels 和 repetition levels，但不会直接生成
Doris `ColumnArray` / `ColumnStruct` / `ColumnMap`。

复杂类型仍需要 Doris 自己实现 assembler：

- primitive nullable：根据 def level 填充 null map；
- struct：递归读取 children，并根据 struct definition level 构造 null map；
- list：根据 repetition level 切分 offsets，根据 definition level 区分 null list、empty list、
  null element 和 non-null element；
- map：在 list/struct 基础上组装 key/value；
- selected read：所有 leaf child 必须以相同 record ranges 推进，保证复杂列 row boundary 一致。

第一阶段可以先支持 primitive 的 `RecordReader` 重构；复杂类型可以分阶段接入。

## 推荐实施路径

### 阶段一：Primitive RecordReader

- 在 `PrimitiveColumnReader` 内部改用 `parquet::internal::RecordReader`。
- 实现 required / nullable primitive 的 `read_records` 和 `skip_records`。
- `read_selected` 支持 primitive 列。
- 保持 `ParquetReader` 当前 filter-first / output-second 调度不变。
- 移除对 nullable primitive 的 read-and-discard skip。

### 阶段二：String / Decimal / Timestamp

- 支持 BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY 的 `RecordReader` 输出。
- 保留当前 Doris 类型转换逻辑。
- 验证 dictionary 编码、plain 编码和 mixed encoding。
- 支持 decimal 和 timestamp 的 selected materialization。

### 阶段三：Struct

- 将 struct reader 改成 record-range 驱动。
- children 使用同一批 selection / row ranges 推进。
- required struct 先支持，nullable struct 后支持。

### 阶段四：List / Map

- 实现 Dremel assembler。
- 支持 list offsets、map offsets、null map 构造。
- 支持 selected read 下的 offsets 重建。

### 阶段五：与 page index / bloom filter 联动

- row group pruning 继续在 `parquet_statistics.*`。
- page index 输出 row ranges。
- batch-level selection 和 page-index row ranges 在 `ParquetReaderScanState` 中合并。
- `ParquetColumnReader::read_selected` 同时消费 predicate selection 和 page ranges。

## 后续规划

### 近期目标

近期目标是把 primitive selected materialization 打稳，而不是立刻扩到所有 Parquet 类型：

- 为 required / nullable boolean、int32、int64、float、double 增加单元测试。
- 覆盖顺序读取、整批 skip、稀疏 selection、连续 selection、全选和全过滤场景。
- 验证 `RecordReader::Reset()` 的使用方式：每轮 read/skip 前清理上一次输出 buffer，但不能重置
  row group 物理位置。
- 增加选择率策略：当 selection 选择率很高或 row ranges 很碎时，可以退化为整批读取后过滤；
  当 selection 稀疏时使用 `read_selected(...)`。
- 将 `RowRange` / selection 压缩逻辑从 `column_reader.cpp` 内部工具逐步抽成可测试的小模块。

### 中期目标

中期目标是扩大 RecordReader-backed reader 的类型覆盖：

- BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY：
  接入 Arrow `BinaryRecordReader` 输出，保留 Doris string/binary column assembly。
- Decimal：
  复用当前 int32/int64/byte-array/fixed-len byte-array decimal decode 逻辑，但读取入口改成
  record-level。
- Timestamp / Time / Date：
  复用当前 logical type / converted type 转换逻辑，保持 file-local 类型语义不进入 table schema。
- Struct：
  让 children 共享同一批 row ranges，先支持 required struct，再支持 nullable struct。

### 长期目标

长期目标是把 DuckDB-style selective read 和 Arrow Rust-style row selection 思路完整落到 Doris：

- List / Map：
  实现 Dremel assembler，支持 offsets、null map 和 selected read 下的 offset 重建。
- Page index：
  将 page index pruning 的 row ranges 与 predicate selection 合并，减少 page 内 decode。
- Bloom filter：
  继续放在 `parquet_statistics.*`，只消费 file-local predicate，不引入 table/global schema。
- Reader expression fallback：
  对无法结构化下推的 filter，在 reader 内部生成临时列并继续收窄 selection。
- 运行时策略：
  根据选择率、range 碎片度、列宽和类型决定 selected read 或整批读后过滤。
- 构建与测试：
  将 `new_parquet` 接入构建目标和 compile database，补齐 primitive / string / decimal /
  timestamp / struct / list / map 的回归测试。

## 设计边界

`ParquetReader` 仍然只理解 file-local schema：

- 不理解 Iceberg global schema；
- 不处理 schema evolution；
- 不处理 default/generated/partition columns；
- 不执行 table-level finalize cast；
- 不把 table-level expression 直接塞进 Parquet metadata pruning。

`RecordReader` 重构只改变 Parquet 文件物理读取层，不改变 `TableColumnMapper` 和
`IcebergTableReader` 的职责边界。

## 当前代码状态

当前代码已经开始落地 Primitive RecordReader 重构：

- 无逻辑注解的基础物理 primitive 列，包括 boolean、int32、int64、float、double，会优先使用
  Arrow Parquet `RecordReader`；
- 这些列的顺序读取通过 `RecordReader::ReadRecords(...)` 完成；
- 这些列的整批跳过通过 `RecordReader::SkipRecords(...)` 完成；
- 这些列的 `read_selected(selection)` 会把 selection 压缩成连续 row ranges，并通过
  `SkipRecords(...)` / `ReadRecords(...)` 交替推进；
- string、decimal、timestamp 仍保留原有 `TypedColumnReader` 路径；
- struct/list/map 的 record-level selected materialization 还未实现；
- 对尚未迁移到 `RecordReader` 的列，`read_selected(selection)` 仍退化为整批读取再过滤。

这仍不是最终性能形态，但它验证了上层调度：

- filter columns 和 output columns 分离；
- projection/filter overlap 复用；
- selection 为空时 output-only columns 不物化；
- row group 进度按物理 batch 推进；
- primitive nullable skip 已经从 value-level 语义切换到 record-level 语义。

下一步如果要继续提升到完整 selective materialization，应继续把 string、decimal、timestamp 和
复杂类型迁移到 record-level reader，并补充 page-index row ranges 与 predicate selection 的合并。
