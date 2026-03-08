# Doris Parquet Reader 纯读取层性能优化方向分析

> 对比 DuckDB 与 StarRocks 的 Parquet Reader 实现，从纯读取层角度分析 Doris 的优化方向。

---

## 一、三者架构总览

| 维度 | Doris | DuckDB | StarRocks |
|------|-------|--------|-----------|
| 入口类 | `ParquetReader` → `RowGroupReader` → `ScalarColumnReader<IN_COL,OFF_IDX>` | `ParquetScanFunction` → `ParquetReader` → `ColumnReader` | `FileReader` → `GroupReader` → `ScalarColumnReader` |
| 解码器 | 自研 Decoder 体系 (Plain/Dict/Delta/BSS/RLE) | 自研模板化 Decoder (高度类型特化) | 自研 Decoder 体系 + SIMD intrinsics |
| IO 层 | `BufferedFileStreamReader` + `MergeRangeFileReader` | `BufferedFileReader` + 自适应 prefetch | `SharedBufferedInputStream` (全局 IO coalescing) |
| 向量化 | `ColumnSelectVector` run-length 批处理 | DuckDB Vector (2048 batch) 原生向量化 | 模板特化 + AVX2 SIMD + branchless |
| 延迟物化 | 2 级 (谓词列 vs lazy 列) | 依赖执行引擎的 filter pushdown | 4 级 (列分组 + lazy dict + lazy convert + filter→decoder) |

### 关键源码位置

**Doris:**
- `be/src/vec/exec/format/parquet/vparquet_reader.h` — `ParquetReader` 主入口
- `be/src/vec/exec/format/parquet/vparquet_group_reader.h` — `RowGroupReader`
- `be/src/vec/exec/format/parquet/vparquet_column_reader.h` — `ScalarColumnReader<IN_COL, OFF_IDX>`
- `be/src/vec/exec/format/parquet/vparquet_column_chunk_reader.h` — `ColumnChunkReader`
- `be/src/vec/exec/format/parquet/decoder.h` — Decoder 基类
- `be/src/vec/exec/format/parquet/parquet_common.h` — `ColumnSelectVector` / `FilterMap`

**DuckDB:**
- `extension/parquet/include/parquet_reader.hpp` — `ParquetReader`
- `extension/parquet/include/column_reader.hpp` — `ColumnReader` 基类 + `PlainTemplatedInternal`
- `extension/parquet/include/reader/templated_column_reader.hpp` — 模板化列读取器
- `extension/parquet/include/decoder/dictionary_decoder.hpp` — 字典解码器
- `extension/parquet/include/decode_utils.hpp` — bitpack/zigzag/varint 工具

**StarRocks:**
- `be/src/formats/parquet/file_reader.h` — `FileReader`
- `be/src/formats/parquet/group_reader.h` — `GroupReader`
- `be/src/formats/parquet/scalar_column_reader.h` — `ScalarColumnReader`
- `be/src/formats/parquet/stored_column_reader.h` — `StoredColumnReaderImpl`
- `be/src/formats/parquet/encoding_dict.h` — `CacheAwareDictDecoder` + AVX2
- `be/src/formats/parquet/encoding_plain.h` — Plain 解码 + SIMD
- `be/src/formats/parquet/column_read_order_ctx.h` — 列读取顺序优化

---

## 二、逐层对比分析

### 1. 谓词下推 & Row Group 过滤

#### Doris 现状

三级漏斗，在 `ParquetReader::_next_row_group_reader()` (`vparquet_reader.cpp:743`) 中编排：

1. **Range 对齐检查**：`_is_misaligned_range_group()` (line 900) — 检查 row group 中点是否在分配的 scan range 内
2. **Row Group 级 Min/Max + Bloom Filter**：`_process_column_stat_filter()` (line 1171) — 逐列评估 min/max 统计值，同列多谓词共享 bloom filter 缓存
3. **Page Index**：`_process_page_index_filter()` (line 914) — 读取 Column Index 做页级 min/max 过滤，产出 `RowRanges`

#### DuckDB 优势

- **Zone Map 与 filter 框架统一**：`ParquetStatisticsUtils` 做类型感知的统计比较，与 DuckDB filter pushdown 框架紧密集成
- **自适应 prefetch 策略**：`disable_parquet_prefetching` / `prefetch_all_parquet_files` 两个开关，根据文件类型（本地 vs 远程）自动选择预取策略
- **Metadata cache**：`parquet_metadata_cache` 选项，支持跨查询缓存 metadata，避免重复解析同一文件 footer

#### StarRocks 优势

- **Runtime Filter 动态 Row Group 剪裁**：`RuntimeScanRangePruner` (`file_reader.cpp:358-373`) 在扫描过程中，当新的 runtime filter 到达时，通过 `_update_rf_and_filter_group()` 动态跳过尚未读取的 row group。Doris 的 runtime filter 在 scan 开始前就已确定，缺乏这种动态能力
- **Bloom Filter 自适应 IO 决策**：`adaptive_judge_if_apply_bloom_filter(span_size)` (`column_reader.h:202`) 根据数据量判断 bloom filter IO 是否值得
- **统一的 `PredicateFilterEvaluator`**：visitor 模式遍历 `PredicateTree`，同时派发 zone map / page index / bloom filter 三种过滤，架构更清晰

> **→ 优化方向 1：Runtime Filter 动态 Row Group 剪裁**
>
> Join 查询中 build 端完成后，probe 端扫描过程中动态跳过不满足条件的 row groups，避免无用 IO 和解码。

---

### 2. 解码层优化

#### Doris 现状

`decoder.h:50-92`，`vparquet_column_reader.cpp:321`：

- `ColumnSelectVector` 将 null map + filter map 编码为 RLE 流 (CONTENT / NULL_DATA / FILTERED_CONTENT / FILTERED_NULL)，decoder 按 run 批量处理
- `BaseDictDecoder::_decode_dict_values<has_filter>()` 模板化 filter 分支
- `ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>` 四重模板特化消除嵌套/索引分支
- **无任何 SIMD intrinsics**

#### DuckDB 优势

- **四重模板特化的 Plain 解码**：`PlainTemplatedInternal<VALUE_TYPE, CONVERSION, HAS_DEFINES, CHECKED>` 生成 4 条编译时路径，无 NULL + 类型大小匹配时退化为单次 `memcpy`：

```cpp
// column_reader.hpp:218-224
if (!HAS_DEFINES && !CHECKED && CONVERSION::PlainConstantSize() == sizeof(VALUE_TYPE)) {
    idx_t copy_count = num_values * CONVERSION::PlainConstantSize();
    memcpy(result_ptr + result_offset, plain_data.ptr, copy_count);
    plain_data.unsafe_inc(copy_count);
    return;
}
```

- **String dictionary zero-copy**：直接引用 dict buffer 中的数据，通过 `StringHeap` 管理生命周期，避免 memcpy
- **直接写入 DuckDB Vector（2048 行）**：无中间格式转换

#### StarRocks 优势 — 多处 AVX2 SIMD 加速

**(a) FLBA 向量化 Slice 构造** (`encoding_plain.h:586-605`)：

```cpp
#ifdef __AVX2__
// 每次迭代处理 4 个 Slice，用 256-bit 寄存器批量构造
__m256i fixed_length = _mm256_set1_epi64x(_type_length);
__m256i inc = _mm256_set1_epi64x(_type_length * 4);
// shuffle + store 4 Slices at once
#endif
```

**(b) Dictionary Decoder 的 AVX2 Null 处理** (`encoding_dict.h:146-172`)：

```cpp
#ifdef __AVX2__
// 稀疏 null 列（非空率 < 10%）用 AVX2 扫描 null bitmap
__m256i loaded = _mm256_loadu_si256((__m256i*)&nulls[i]);
int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(loaded, _mm256_setzero_si256()));
// 用 phmap BitMask 迭代 set bits，scatter 非空值到正确位置
#endif
// 稠密路径用 SIMD::Expand::expand_load()
```

**(c) Branchless Null 处理** (`encoding_dict.h:469-473`)：

```cpp
uint32_t mask = ~(static_cast<uint32_t>(-null_data_ptr[i]));
int32_t code = mask & dict_codes[i]; // 无分支选择
```

**(d) `append_strings_overflow()` SIMD 安全读取**：允许读取超出字符串边界最多 `APPEND_OVERFLOW_MAX_SIZE` 字节，避免 SIMD 边界检查开销。

> **→ 优化方向 2：SIMD 加速解码**
>
> Doris 的解码器完全没有 SIMD 优化。可参考 StarRocks 实现：
> - Dict 解码中的 null bitmap AVX2 扫描 + scatter
> - FLBA 向量化 Slice 构造
> - Branchless null 处理模式

---

### 3. Filter 下推到 Decoder 内部

#### Doris 现状

- `ColumnSelectVector` 在 decoder 外层将 filter map 编码为 FILTERED_CONTENT run，decoder 内部逐 run 调用 `skip_values()` 跳过被过滤的值
- 问题：即使是 skip，也需要 RLE 解码 dict codes 来推进位置，开销不小

#### StarRocks 实现

`stored_column_reader.h:155-161`：

```cpp
const FilterData* _convert_filter_row_to_value(const Filter* filter, size_t row_readed) {
    if (!filter || !config::parquet_push_down_filter_to_decoder_enable) return nullptr;
    // 选择率 < 20% 时，直接传 filter bitmap 给 decoder
    return SIMD::count_nonzero(*filter) * 1.0 / filter->size() < 0.2
        ? filter->data() + row_readed : nullptr;
}
```

当选择率 < 20% 时，filter bitmap 直接传入 `Decoder::next_batch(count, content_type, dst, filter)`，decoder 内部跳过被过滤值的物化（不执行 dict lookup、不执行 string copy），比 Doris 的外层 skip 更高效。

#### DuckDB 实现

DuckDB 在 dictionary 初始化时一次性评估 filter，标记每个 dict entry 是否满足条件。后续如果一个页面的所有 dict entries 都被过滤（`HasFilteredOutAllValues()`），则整页直接跳过，连 RLE 解码都不做。

> **→ 优化方向 3：Filter 下推到 Decoder 层**
>
> 在低选择率场景（< 20%），直接将 filter bitmap 传给 decoder，decoder 内部跳过被过滤值的物化。与 Doris 现有的 `FILTERED_CONTENT` run 机制相比，省去了 "先解码 → 再 skip" 的开销。

---

### 4. Cache-Aware 字典解码

#### Doris 现状

无任何 cache 感知的解码策略。

#### StarRocks 实现

`encoding_dict.h:91-127` `CacheAwareDictDecoder`：

```cpp
CacheAwareDictDecoder() { _dict_size_threshold = CpuInfo::get_l2_cache_size(); }

Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                  const FilterData* filter) {
    // ...
    if (_get_dict_size() > _dict_size_threshold &&
        config::parquet_cache_aware_dict_decoder_enable) {
        return _next_batch_value(count, dst, filter);  // 传入 filter，跳过无用 lookup
    } else {
        return _next_batch_value(count, dst, nullptr);  // 不传 filter，直接 lookup
    }
}
```

核心逻辑：
- 字典 > L2 cache → 随机 dict lookup 产生大量 cache miss → 传入 filter bitmap 跳过无用 lookup
- 字典 < L2 cache → lookup 基本都是 cache hit → 传 filter 反而增加判断开销

> **→ 优化方向 4：Cache-Aware 字典解码**
>
> 大字典（> L2 cache 大小，通常 256KB-1MB）的 dict lookup 是 cache-unfriendly 的热点操作。结合 filter bitmap 跳过无用 lookup 可以显著减少 L2 cache miss。

---

### 5. 延迟物化（Late Materialization）

#### Doris 现状

`vparquet_group_reader.cpp:518` `_do_lazy_read()`：

- **2 级**：predicate columns（先读） + lazy columns（后读，带 filter map）
- `_cached_filtered_rows` 跨 batch 累积，允许跳过整页 lazy 列
- `filter_ratio > 0.6` 时触发整页跳过优化

#### DuckDB

- 不在 reader 内部做 late materialization，依赖执行引擎的 filter + projection pushdown
- `AdaptiveFilter` 动态重排 filter 执行顺序（filter 级别而非列级别）

#### StarRocks — 4 级延迟物化

| 层级 | 机制 | 位置 | 说明 |
|------|------|------|------|
| L1 | Active vs Lazy 列分离 | `GroupReader._active_column_indices` / `_lazy_column_indices` | 与 Doris 类似，谓词列先读，非谓词列后读 |
| L2 | **Lazy Dictionary Decode** | `ScalarColumnReader._can_lazy_dict_decode` (`scalar_column_reader.h:162`) | string 类型 + 全页字典编码时，先只读 dict codes (int32)，filter 后仅对存活行做 dict lookup → string 物化 |
| L3 | **Lazy Type Conversion** | `ScalarColumnReader._can_lazy_convert` | 先以 Parquet 原生类型读取（如 INT96），filter 后仅对存活行做类型转换（如 INT96→DateTime） |
| L4 | Filter 下推到 Decoder | `_convert_filter_row_to_value()` | 选择率 < 20% 时直接跳过值物化 |

**关键细节：自适应阈值** — Lazy Dict Decode 仅在 `FILTER_RATIO < 0.2` 时启用 (`scalar_column_reader.h:215`)，避免低选择率时增加无用的中间步骤。

> **→ 优化方向 5：引入 Lazy Dict Decode + Lazy Type Conversion**
>
> - **Lazy Dict Decode**：对 string 类型字典编码列，先只读 dict codes (int32)，filter 后仅对存活行做字典 lookup。在高过滤率场景，省去大量 string copy
> - **Lazy Type Conversion**：先以 Parquet 物理类型读取，filter 后仅对存活行做类型转换（如 INT96→DateTime、FLBA→Decimal）

---

### 6. 列读取顺序优化

#### Doris 现状

lazy read 只区分 "谓词列" 和 "非谓词列" 两组，两组内部无排序。

#### StarRocks 实现

`column_read_order_ctx.h:24-54`：

```cpp
class ColumnReadOrderCtx {
    std::vector<int> _column_indices;       // 最优顺序
    std::vector<int> _trying_column_indices; // 当前尝试的顺序
    size_t _min_round_cost = 0;             // 最小 round cost
    size_t _rand_round_order_index = 10;    // 从 10 个随机顺序中选最优
    std::unordered_map<int, size_t> _column_cost_map; // 列 → cost
};
```

- `_read_range_round_by_round()` (`group_reader.h:173`) 按轮次读列，每轮之间可应用 filter
- `update_ctx(round_cost, first_selectivity)` 动态更新列读取顺序
- 从 10 个随机顺序中选择 cost 最低的排列，实现自适应优化
- 高选择率谓词列先读 → 产生 filter → 后续列在更少行上物化

#### DuckDB 实现

`AdaptiveFilter` (`parquet_reader.cpp:1432-1452`) 运行时动态重排 filter 执行顺序，粒度是 filter 级别：

```cpp
auto filter_state = state.adaptive_filter->BeginFilter();
for (idx_t i = 0; i < state.scan_filters.size(); i++) {
    auto &scan_filter = state.scan_filters[state.adaptive_filter->permutation[i]];
    // ... evaluate filter ...
}
state.adaptive_filter->EndFilter(filter_state);
```

> **→ 优化方向 6：列读取顺序优化**
>
> 在谓词列内部按选择率排序：先读选择率高（过滤效果好）的列，产生 filter 后再读其他列。最大化 filter 效果，减少后续列的解码量。

---

### 7. IO 模式

#### Doris 现状

- `BufferedFileStreamReader`：每列独立的顺序预读缓冲
- `MergeRangeFileReader`：平均 IO < `SMALL_IO` 阈值时，合并邻近小 IO
- 两者互斥（有 MergeRange 时禁用 Buffered prefetch，避免双缓冲）
- `StoragePageCache`：LRU 页面缓存，支持压缩/解压两种缓存策略
- `FileMetaCache`：footer 缓存

#### DuckDB 优势

- **自适应 prefetch**：根据文件存储类型（本地 vs 远程 S3/HTTP）自动调整 prefetch 策略
- **整 Row Group prefetch**：当扫描 > 95% 列且无 filter 时，一次性预取整个 row group 数据范围
- **列级 prefetch 与 lazy fetch 协作**：有 filter 时，filter 列立即预取，非 filter 列延迟预取（`allow_merge=false`）
- **Metadata cache 独立配置**：`parquet_metadata_cache` 允许跨查询缓存 metadata

#### StarRocks 优势

- **`SharedBufferedInputStream` 全局 IO Coalescing**：row group 内所有列共享同一个缓冲输入流，统一收集所有列的 IO ranges 后全局合并
- **分类型 IO 收集**：区分 `PAGES` / `PAGE_INDEX` / `BLOOM_FILTER` 三种 IO 类型，分别收集和调度
- **Lazy Column IO 延迟合并**：`lazy_column_coalesce_counter` (`group_reader.h:98`) 追踪是否需要将 lazy 列 IO 与 active 列合并，避免预读永远不会被使用的 lazy 列数据
- **DataCache 集成**：与 StarRocks 的分布式缓存系统集成

> **→ 优化方向 7：统一的 IO Coalescing**
>
> Doris 的 `MergeRangeFileReader` 只做简单的邻近 IO 合并。StarRocks 的全局 IO coalescing 跨列统一优化，对远程存储（S3/HDFS）场景可以显著减少 IO 次数。且区分 active/lazy 列的 IO 策略更精细。

---

### 8. Page Index 利用

#### Doris 现状

- 支持 Offset Index（页级定位）和 Column Index（页级 min/max）
- `OFFSET_INDEX=true` 模板参数启用直接页面寻址，消除运行时开销
- `_process_page_index_filter()` 利用 Column Index 做页级行范围过滤

#### StarRocks

- `StoredColumnReaderWithIndex`：专门的带索引读取器
- `_next_selected_page()` 直接跳到下一个选中的页面
- 与 Zone Map Filter 统一流程：`page_index_zone_map_filter()` 返回 `SparseRange<uint64_t>`，与 row group 级过滤结果直接交集

#### DuckDB

- **不支持 Page Index**（ColumnIndex / OffsetIndex）。无法做页级行范围过滤。在这一点上 Doris 和 StarRocks 均领先。

> **该方面 Doris 已有较好实现**，通过模板参数消除了运行时开销。

---

### 9. 编码支持完整度

| 编码 | Doris | DuckDB | StarRocks |
|------|-------|--------|-----------|
| PLAIN | ✅ | ✅ | ✅ |
| RLE_DICTIONARY | ✅ | ✅ | ✅ |
| RLE (Boolean) | ✅ | ✅ | ✅ |
| DELTA_BINARY_PACKED | ✅ | ✅ | ✅ |
| DELTA_BYTE_ARRAY | ✅ | ✅ | ✅ |
| DELTA_LENGTH_BYTE_ARRAY | ✅ | ✅ | ✅ |
| BYTE_STREAM_SPLIT | ✅ | ✅ | ✅ |

三者编码支持基本对齐，差异不大。

---

### 10. 字典过滤优化

#### Doris 现状

`RowGroupReader::_rewrite_dict_predicates()` (`vparquet_group_reader.cpp:1042`)：

1. 读取字典值到 string column
2. 构建临时 Block，执行 conjuncts 过滤
3. 全部命中则跳过整个 row group（`_is_row_group_filtered = true`）
4. 部分命中则改写为 dict code 上的 `EQ` / `IN` 谓词，避免后续 string 比较
5. 有上限：`MAX_DICT_CODE_PREDICATE_TO_REWRITE`，超过则退回原始谓词
6. 读取后需 `_convert_dict_cols_to_string_cols()` 将 dict codes 转回字符串

#### DuckDB

- 字典解码更轻量：直接在 dictionary buffer 上做 lookup，string 结果引用 dict buffer（zero-copy）
- `DictionaryDecoder::InitializeDictionary()` 接受 filter，一次性评估所有 dict entries

#### StarRocks

- 四级 Lazy 机制配合字典过滤（见上文第 5 节）
- 自适应 Lazy Decode 阈值：`FILTER_RATIO = 0.2`
- L2 Cache 感知：`CacheAwareDictDecoder`（见上文第 4 节）
- Struct 子字段级字典过滤下推：`StructColumnReader` 通过 `sub_field_path` 路由字典过滤到子 reader

---

## 三、Doris 现有优势

对比之下，Doris 也有自身的亮点：

1. **模板四重特化**：`ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>` × `ColumnChunkReader` × `PageReader` 各 4 个实例化（共 12 个），消除了嵌套列处理和 offset index 的运行时分支

2. **ColumnSelectVector run-length 批处理**：将 null map + filter map 编码为 run-length 流，decoder 按 run 批量处理，比逐行判断高效

3. **Page Index 完整支持**：支持 Offset Index + Column Index，通过模板参数消除运行时开销（DuckDB 不支持 Page Index）

4. **Page Cache 两级策略**：根据压缩比选择缓存压缩数据还是解压数据，平衡内存占用和 CPU 开销

5. **MergeRangeFileReader 与 BufferedFileStreamReader 互斥**：避免双缓冲浪费

---

## 四、总结：优化方向优先级排序

### P0 — 高收益，改动可控

| # | 优化方向 | 参考实现 | 核心收益 |
|---|---------|---------|---------|
| 1 | **Filter bitmap 下推到 Decoder** | StarRocks `stored_column_reader.h:155-161` | 低选择率查询（< 20% 存活）减少 60-80% 无用值物化 |
| 2 | **谓词列读取顺序优化** | StarRocks `ColumnReadOrderCtx` / DuckDB `AdaptiveFilter` | 多谓词列查询，最大化 filter 裁剪效果，减少后续列解码量 |
| 3 | **Lazy Dictionary Decode** | StarRocks `ScalarColumnReader._can_lazy_dict_decode` | 字典编码 string 列 + 高过滤率时省去大量 string copy |

### P1 — 中等收益

| # | 优化方向 | 参考实现 | 核心收益 |
|---|---------|---------|---------|
| 4 | **AVX2 SIMD 解码热路径** | StarRocks `encoding_dict.h` null scatter/expand | CPU-bound 场景整体解码加速 |
| 5 | **Cache-Aware 字典解码** | StarRocks `CacheAwareDictDecoder` (L2 cache check) | 大字典（> L2 cache）场景减少 cache miss |
| 6 | **Plain 编码 memcpy 快速路径** | DuckDB `PlainTemplatedInternal` 四重模板 | 无 NULL 定长列整批 memcpy，消除逐值处理 |
| 7 | **全局 IO Coalescing** | StarRocks `SharedBufferedInputStream` | 远程存储（S3/HDFS）多列查询减少 IO 次数 |

### P2 — 长期优化

| # | 优化方向 | 参考实现 | 核心收益 |
|---|---------|---------|---------|
| 8 | **Runtime Filter 动态 Row Group 剪裁** | StarRocks `RuntimeScanRangePruner` | Join 查询中 build 端完成后动态跳过 probe 端 row groups |
| 9 | **Lazy Type Conversion** | StarRocks `_can_lazy_convert` | INT96→DateTime 等需类型转换列 + filter 场景 |
| 10 | **String Zero-Copy Dict Lookup** | DuckDB `StringHeap` 引用 dict buffer | 字典编码 string 列减少 memcpy 开销 |

---

## 五、核心结论

Doris 的 Parquet Reader 架构设计合理，模板四重特化和 `ColumnSelectVector` run-length 批处理是其亮点。但与 StarRocks 对比，在三个关键维度存在明显差距：

1. **Decoder 层精细度**：StarRocks 的 filter→decoder 下推 + cache-aware dict + SIMD intrinsics，使得解码热路径效率显著更高。Doris 的 decoder 没有任何 SIMD，也不接收 filter bitmap。

2. **延迟物化深度**：Doris 2 级 vs StarRocks 4 级。差距主要在 dict decode 和 type convert 两个环节的延迟物化 — StarRocks 可以先读 dict codes (int32)，filter 后仅对存活行做字典 lookup 和类型转换。

3. **列间协作**：StarRocks 的 `ColumnReadOrderCtx` 在谓词列之间做顺序优化（高选择率列先读），DuckDB 也有 `AdaptiveFilter` 动态重排。Doris 缺乏谓词列间的排序优化。

与 DuckDB 对比，Doris 在 Page Index 支持上领先（DuckDB 不支持），但 DuckDB 在 Plain 解码的 memcpy 快速路径和 String 零拷贝字典引用上有优势。

**最大的性能杠杆在 P0 三项** — 不需要大规模重构架构，但能在典型分析查询（低选择率 + 多谓词列 + 字典编码 string）中带来显著提升。
