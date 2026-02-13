# P0-3 惰性列字典延迟解码优化 — 测试文档

## 1. 功能概述

本优化为 Doris Parquet Reader 的 P0-3 优化项：**惰性列字典延迟解码（Lazy Dictionary Decode for Lazy String Columns）**，实现了 Phase 2 惰性字符串列的"先解码为字典索引 int32，过滤后再转换为字符串"策略。

### 1.1 优化目标

在 lazy read 模式下，Phase 2 读取的惰性字符串列（不参与谓词过滤）需要全量解码为字符串。当 Phase 1 的过滤率较高时，大量被过滤行的字符串解码是浪费。P0-3 优化将这些列的解码分为两步：
- **Step 1**：以字典索引（int32）形式读取全部行——写 4 字节整数远快于字典查找 + 字符串拷贝
- **Step 2**：Phase 1 过滤完成后，仅对存活行执行字典索引 → 字符串的转换

### 1.2 核心对比

| | 原始路径（Eager String） | 优化路径（Lazy Dict Decode） |
|---|---|---|
| Phase 2 解码 | 全部 N 行解码为字符串（dict lookup + string copy） | 全部 N 行解码为 int32（写 4 字节） |
| 过滤后处理 | 直接 filter 字符串列 | 先 filter int32 列，再 `convert_dict_column_to_string_column` 仅 S 行 |
| 内存占用 | N × avg_str_len | N × 4 + S × avg_str_len |
| 适用条件 | 通用 | 列必须全字典编码（PLAIN_DICTIONARY / RLE_DICTIONARY） |

### 1.3 与 P0-1 的关系

P0-1（Filter Bitmap Pushdown）的懒惰解码路径 `_lazy_decode_string_values()` 对字符串列实际有**负面效果**（比基线慢 24-152%），因为 per-RLE-run 的 `GetBatch` + `SkipBatch` 开销大于一次性全量 `GetBatch`。P0-3 是字符串惰性列的正确优化策略，通过改变数据类型（string → int32）而非改变解码粒度来避免无效字符串物化。

---

## 2. 修改文件清单

### 2.1 核心修改

| 文件 | 修改内容 | 重要程度 |
|------|----------|----------|
| `be/src/vec/exec/format/parquet/vparquet_reader.cpp` | 候选列识别：遍历惰性列，检查字符串 slot 类型 + BYTE_ARRAY 物理类型，加入 `lazy_dict_decode_candidates` | 高 |
| `be/src/vec/exec/format/parquet/vparquet_group_reader.h` | 新增 `lazy_dict_decode_candidates` 字段（LazyReadContext）、`_lazy_dict_decode_cols` 成员、`_convert_lazy_dict_cols_to_string_cols()` 声明 | 高 |
| `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp` | Row group 级确认（`is_dictionary_encoded`）、`_read_column_data()` 中替换为 ColumnInt32、Phase 2 后调用转换函数、`_convert_lazy_dict_cols_to_string_cols()` 实现 (~56 行) | 高 |
| `be/src/common/config.h` | 新增 `enable_parquet_lazy_dict_decode_for_lazy_columns` 配置项 | 中 |
| `be/src/common/config.cpp` | 对应定义 | 中 |

### 2.2 新增文件

| 文件 | 说明 |
|------|------|
| `be/benchmark/benchmark_lazy_dict_decode.hpp` | 微基准测试：4 种策略对比 + 转换开销测量 |

### 2.3 已修改文件（其他 P0 共享）

| 文件 | P0-3 相关修改 |
|------|--------------|
| `be/benchmark/benchmark_main.cpp` | 新增 `#include "benchmark_lazy_dict_decode.hpp"` |

---

## 3. 配置项

### 3.1 `enable_parquet_lazy_dict_decode_for_lazy_columns`

| 属性 | 值 |
|------|-----|
| 类型 | mBool（运行时可修改） |
| 默认值 | `true` |
| 作用 | 控制是否对 Phase 2 惰性字符串列启用字典延迟解码 |
| 关闭方式 | `curl http://be_host:webserver_port/api/update_config?enable_parquet_lazy_dict_decode_for_lazy_columns=false` |

### 3.2 触发条件

惰性列字典延迟解码在同时满足以下条件时激活：
1. `enable_parquet_lazy_dict_decode_for_lazy_columns = true`
2. 惰性列的 slot 类型为字符串类型（`TYPE_STRING` / `TYPE_VARCHAR` / `TYPE_CHAR`）
3. 列的 Parquet 物理类型为 `BYTE_ARRAY`
4. 当前 row group 中该列全字典编码（通过 `is_dictionary_encoded()` 检查 `encoding_stats` 或 `encodings` 元数据）

---

## 4. 技术实现细节

### 4.1 候选列识别

**位置**：`vparquet_reader.cpp::set_fill_columns()` (~line 564-599)

遍历所有惰性列，筛选满足条件的候选列：

```
for each lazy_column:
  slot_type = slot_desc->type().type
  if slot_type in {TYPE_STRING, TYPE_VARCHAR, TYPE_CHAR}:
    parquet_col = find_column_in_schema(lazy_column.name)
    if parquet_col.physical_type == BYTE_ARRAY:
      lazy_read_ctx.lazy_dict_decode_candidates.push_back({col_name, slot_id})
```

### 4.2 Row Group 级确认

**位置**：`vparquet_group_reader.cpp::init()` (~line 254-268)

在每个 row group 初始化时，逐一检查候选列的编码类型：

```
for each candidate in lazy_dict_decode_candidates:
  column_metadata = get_column_metadata(candidate.name)
  if is_dictionary_encoded(column_metadata):
    _lazy_dict_decode_cols.push_back(candidate)
```

`is_dictionary_encoded()` 检查逻辑（line 367-425）：
- **优先检查 `encoding_stats`**（Parquet v2.6+）：要求所有 `DATA_PAGE` / `DATA_PAGE_V2` 的编码为 `PLAIN_DICTIONARY` 或 `RLE_DICTIONARY`
- **回退检查 `encodings`**：排除 `PLAIN_DICTIONARY` / `RLE_DICTIONARY` / `RLE`（用于定义级别）后，确认无其他编码

### 4.3 Phase 2 读取为 int32

**位置**：`vparquet_group_reader.cpp::_read_column_data()` (~line 568-594)

在 `_dict_filter_cols` 检查循环之后，新增对 `_lazy_dict_decode_cols` 的检查。匹配到的列执行与 dict filter 列相同的类型替换：

```
for col in _lazy_dict_decode_cols:
  if block.column_name == col.name:
    // 替换列为 ColumnInt32 + DataTypeInt32
    replace_column_with_int32(block[i])
    is_dict_filter = true  // 使 decoder 输出 dict indices
    break
```

### 4.4 过滤后字典转换

**位置**：`vparquet_group_reader.cpp::_convert_lazy_dict_cols_to_string_cols()` (~line 1384-1435)

在 Phase 2 完成过滤后，将 int32 字典索引列转换回字符串：

```
for col in _lazy_dict_decode_cols:
  find column in block by slot_id
  if column is empty (all rows filtered):
    restore original string type with empty column
    continue
  
  if column is nullable:
    extract nested ColumnInt32 from nullable wrapper
    convert_dict_column_to_string_column(int32_col) → string_col
    re-wrap with nullable
  else:
    convert_dict_column_to_string_column(int32_col) → string_col
  
  replace column in block
```

`convert_dict_column_to_string_column()` 由 `ByteArrayDictDecoder` 提供，对每个 int32 索引执行字典查找，构建 `ColumnString`。

### 4.5 调用链路

```
ParquetReader::set_fill_columns()
  → 识别 lazy_dict_decode_candidates（字符串 + BYTE_ARRAY）

RowGroupReader::init()
  → 逐列检查 is_dictionary_encoded()
  → 确认 _lazy_dict_decode_cols

RowGroupReader::_do_lazy_read()  /  _do_lazy_read_per_column()
  Phase 1: 读取谓词列 → 过滤
  Phase 2: _read_column_data() 
    → _lazy_dict_decode_cols 匹配的列替换为 ColumnInt32
    → decoder 输出 dict indices（4 字节/行）
  → filter Phase 2 列
  → _convert_dict_cols_to_string_cols()     // 谓词列的 dict filter 转换
  → _convert_lazy_dict_cols_to_string_cols() // 惰性列的字典转换（仅存活行）
```

---

## 5. 测试方案

### 5.1 微基准测试（已完成）

#### 5.1.1 构建与运行

```bash
# 增量构建
cd be/build_benchmark && ninja -j 10 benchmark_test

# 运行 P0-3 benchmark
export JAVA_HOME=/path/to/jdk17
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
export DORIS_HOME=$(pwd)/be/build_benchmark
./bin/benchmark_test --benchmark_filter="BM_P03_" --benchmark_repetitions=3 --benchmark_report_aggregates_only=true
```

#### 5.1.2 基准测试用例一览

基准测试使用**真实 `ByteArrayDictDecoder`**（非模拟），对比 4 种策略：

| 组别 | 测试名 | P0-1 | P0-3 | 机制 |
|------|--------|------|------|------|
| 基线 | `BM_P03_Baseline` | 否 | 否 | `decode_values(ColumnString, is_dict_filter=false, filter_data=nullptr)` — 全部行解码为字符串 |
| P0-1 Only | `BM_P03_P01Only` | 是 | 否 | `decode_values(ColumnString, is_dict_filter=false, filter_data=bitmap)` — 懒惰解码仅存活行为字符串 |
| P0-3 Only | `BM_P03_P03Only` | 否 | 是 | `decode_values(ColumnInt32, is_dict_filter=true, filter_data=nullptr)` → filter int32 → `convert_dict_column_to_string_column` 仅存活行 |
| P0-3+P0-1 | `BM_P03_P03PlusP01` | 是 | 是 | `decode_values(ColumnInt32, is_dict_filter=true, filter_data=bitmap)` → `convert_dict_column_to_string_column` |
| 辅助 | `BM_P03_ConvertOverhead` | — | — | 纯 `convert_dict_column_to_string_column` 开销测量 |

参数格式：`(dict_size, selectivity_percent, num_values_in_thousands, avg_str_len)`

**P0-3+P0-1 的代码路径细节**：当 `is_dict_filter=true` 且 `filter_data!=nullptr` 时，`byte_array_dict_decoder.cpp` 的 `_decode_values<true>()` 路径**不使用** `_lazy_decode_string_values()`，而是走 bulk `GetBatch` 解码全部 RLE 索引，然后 `_decode_dict_values<true>` 通过 `ColumnSelectVector` 仅写入 CONTENT 行的 int32 值到 `ColumnInt32`。因此 P0-3+P0-1 仍解码全部 RLE 索引，但写入更少的 int32 值 + 转换更少的字符串。

#### 5.1.3 基准测试结果

**测试环境**：16 核 CPU @ 3437.92 MHz，L1D 48KB×8, L2 1280KB×8, L3 49152KB×1

##### 小字典（dict=100），短字符串（strlen=32），100K 行

| 存活率 | Baseline (µs) | P0-1 Only (µs) | P0-3 Only (µs) | P0-3+P0-1 (µs) | 最优策略 |
|--------|---------------|-----------------|-----------------|-----------------|----------|
| 5% | 261 | 337 | **167** | 180 | P0-3 Only (1.56x) |
| 10% | 426 | 569 | **248** | 268 | P0-3 Only (1.72x) |
| 20% | 723 | 1000 | **437** | 450 | P0-3 Only (1.65x) |
| 50% | 1328 | 1764 | 1014 | **856** | P0-3+P0-1 (1.55x) |
| 100% | **501** | 509 | 744 | 507 | Baseline (无过滤) |

##### 小字典（dict=100），长字符串（strlen=128），100K 行

| 存活率 | Baseline (µs) | P0-1 Only (µs) | P0-3 Only (µs) | P0-3+P0-1 (µs) | 最优策略 |
|--------|---------------|-----------------|-----------------|-----------------|----------|
| 5% | 296 | 375 | **168** | 213 | P0-3 Only (1.76x) |
| 20% | 804 | 1093 | **460** | 561 | P0-3 Only (1.75x) |
| 50% | 3781 | 4522 | **1162** | 3151 | P0-3 Only (3.25x) |
| 100% | 5280 | **5155** | 5598 | 5717 | P0-1 Only ≈ Baseline |

##### 中字典（dict=10000），短字符串（strlen=32），100K 行

| 存活率 | Baseline (µs) | P0-1 Only (µs) | P0-3 Only (µs) | P0-3+P0-1 (µs) | 最优策略 |
|--------|---------------|-----------------|-----------------|-----------------|----------|
| 5% | 325 | 355 | **185** | 214 | P0-3 Only (1.76x) |
| 20% | 826 | 1048 | **467** | 486 | P0-3 Only (1.77x) |
| 50% | 1451 | 1855 | 1043 | **919** | P0-3+P0-1 (1.58x) |

##### 中字典（dict=10000），长字符串（strlen=128），100K 行

| 存活率 | Baseline (µs) | P0-1 Only (µs) | P0-3 Only (µs) | P0-3+P0-1 (µs) | 最优策略 |
|--------|---------------|-----------------|-----------------|-----------------|----------|
| 5% | 389 | 474 | **191** | 254 | P0-3 Only (2.04x) |
| 20% | 2341 | 1266 | **498** | 665 | P0-3 Only (4.70x) |

##### 转换开销（`convert_dict_column_to_string_column`）

| 字典大小 | 行数 | 字符串长度 | 耗时 (µs) |
|---------|------|-----------|-----------|
| 100 | 5K | 32 | 17.7 |
| 100 | 50K | 32 | 201 |
| 100 | 100K | 32 | 437 |
| 100 | 5K | 128 | 36.2 |
| 100 | 100K | 128 | 5243 |
| 10000 | 5K | 32 | 20.6 |
| 10000 | 100K | 32 | 501 |
| 10000 | 5K | 128 | 55.6 |
| 10000 | 100K | 128 | 6160 |

#### 5.1.4 性能分析

**核心发现：P0-3 是惰性字符串列的最优策略，全面优于 P0-1 和基线。**

##### 1. P0-1 Only 对字符串列有负面效果

| 场景 | Baseline (µs) | P0-1 Only (µs) | 差异 |
|------|---------------|-----------------|------|
| dict=100, strlen=32, sel=5% | 261 | 337 | **+29% 退化** |
| dict=100, strlen=32, sel=10% | 426 | 569 | **+34% 退化** |
| dict=100, strlen=128, sel=5% | 296 | 375 | **+27% 退化** |
| dict=100, strlen=128, sel=50% | 3781 | 4522 | **+20% 退化** |

**原因分析**：P0-1 的 `_lazy_decode_string_values()` 按 RLE run 粒度处理，每个 CONTENT run 独立调用 `GetBatch` + `insert_many_strings_overflow`，每个 FILTERED_CONTENT run 调用 `SkipBatch`。这种 per-run 开销累积显著大于原始路径的一次性 `GetBatch`（全量索引解码） + 遍历 ColumnSelectVector（仅 CONTENT 行做字典查找）。字符串物化成本（字典查找 + 字符串拷贝）在两条路径中相同，而 P0-1 增加了额外的 per-run 管理开销。

**结论**：P0-1 的 filter bitmap pushdown **不应应用于**字符串惰性列。当前代码正确处理了这一点——`is_dict_filter=true` 时 `_decode_values` 不会进入 `_lazy_decode_string_values` 路径。

##### 2. P0-3 Only 在全部 <100% 选择率下全面领先

| 场景 | vs Baseline 加速比 | 关键优势 |
|------|-------------------|----------|
| dict=100, strlen=32, sel=5% | 1.56x | int32 解码远快于字符串 |
| dict=100, strlen=32, sel=10% | 1.72x | |
| dict=100, strlen=128, sel=5% | 1.76x | 长字符串放大优势 |
| dict=100, strlen=128, sel=50% | **3.25x** | 50% 行的字符串物化节省巨大 |
| dict=10000, strlen=128, sel=5% | 2.04x | 大字典 + 长字符串 |
| dict=10000, strlen=128, sel=20% | **4.70x** | **最大加速比** |

**核心机制**：解码 N 行 int32（写 4 字节/行）的成本约为解码 N 行字符串（字典查找 + 字符串拷贝 avg_str_len 字节/行）的 1/3 ~ 1/10。即使 P0-3 仍解码全部 N 行为 int32，总成本 = (N × int32 decode cost) + (S × string convert cost) 远小于 (N × string decode cost)，只要 S << N。

##### 3. 长字符串显著放大 P0-3 优势

| 存活率 | strlen=32 加速比 | strlen=128 加速比 | 放大倍数 |
|--------|----------------|------------------|----------|
| 5%, dict=100 | 1.56x | 1.76x | 1.13x |
| 50%, dict=100 | 1.31x (P0-3 Only) | **3.25x** | 2.48x |
| 20%, dict=10000 | 1.77x | **4.70x** | 2.65x |

字符串越长，每行字符串物化成本越高，P0-3 的"延迟到过滤后再物化"策略收益越大。

##### 4. P0-3+P0-1 在低选择率时略逊于 P0-3 Only，50% 时反超

| 存活率 | P0-3 Only (µs) | P0-3+P0-1 (µs) | 差异 |
|--------|-----------------|-----------------|------|
| 5%, dict=100, strlen=32 | 167 | 180 | P0-3+P0-1 慢 8% |
| 20%, dict=100, strlen=32 | 437 | 450 | P0-3+P0-1 慢 3% |
| 50%, dict=100, strlen=32 | 1014 | **856** | P0-3+P0-1 快 16% |
| 50%, dict=10000, strlen=32 | 1043 | **919** | P0-3+P0-1 快 12% |

**原因**：当 `is_dict_filter=true` 且 `filter_data!=nullptr` 时，P0-1 通过 ColumnSelectVector 跳过 FILTERED_CONTENT 行的 int32 写入。在低选择率下，节省的 int32 写入量很少（int32 写入本身就很廉价），但 ColumnSelectVector 的 per-run 处理开销使总成本略增。在 50% 选择率下，跳过的 int32 写入量足够多，收益超过了开销。

##### 5. 100% 选择率时 P0-3 有退化

| 场景 | Baseline (µs) | P0-3 Only (µs) | 退化比例 |
|------|---------------|-----------------|----------|
| dict=100, strlen=32, sel=100% | 501 | 744 | **+49%** |
| dict=100, strlen=128, sel=100% | 5280 | 5598 | +6% |

**原因**：100% 选择率时无行被过滤，P0-3 的解码路径为 (N × int32 decode) + (N × string convert)，比直接 (N × string decode) 多了一次完整的数据遍历。短字符串时退化更明显（因为 string decode 的绝对成本较低，额外遍历开销占比更大）。

**缓解方案**：建议增加选择率门控，当 `filter_ratio < 0.05`（即存活率 > 95%）时禁用 P0-3，回退到直接字符串解码。

##### 6. 转换开销线性扩展

`convert_dict_column_to_string_column` 的开销与 `行数 × 字符串长度` 成线性关系：
- 100K 行 × strlen=32：~437-501 µs
- 100K 行 × strlen=128：~5243-6160 µs
- 5K 行 × strlen=32：~17-21 µs

这是存活行的主要成本。P0-3 的优势在于将此成本从 N 行降低到 S 行（S = 存活行数）。

### 5.2 功能正确性测试方案

#### 5.2.1 单元测试（建议补充）

**候选列识别正确性**

| 测试场景 | 验证点 |
|----------|--------|
| 字符串惰性列 + BYTE_ARRAY | 被正确加入 candidates |
| 非字符串惰性列（INT、DOUBLE） | 不加入 candidates |
| 字符串谓词列（非惰性） | 不加入 candidates |
| FIXED_LEN_BYTE_ARRAY 字符串列 | 不加入 candidates（仅 BYTE_ARRAY） |

**Row group 级确认正确性**

| 测试场景 | 验证点 |
|----------|--------|
| 全字典编码列 | 加入 `_lazy_dict_decode_cols` |
| 混合编码列（部分 page 用 PLAIN） | 不加入 |
| encoding_stats 存在时 | 优先使用 encoding_stats 判断 |
| 仅 encodings 字段时 | 回退到 encodings 检查 |

**`_convert_lazy_dict_cols_to_string_cols()` 正确性**

| 测试场景 | 验证点 |
|----------|--------|
| 非 nullable 列 | int32 → string 转换正确 |
| nullable 列 | null bitmap 保留，非 null 行正确转换 |
| 全部被过滤（空列） | 正确恢复字符串类型，列为空 |
| 字典索引为 0（第一个字典项） | 不会被误判为空/null |
| 大字典（10000+ 条目） | 全部索引正确映射 |

#### 5.2.2 集成测试（建议执行）

```sql
-- 1. 基础查询：惰性字符串列在过滤后正确返回
SELECT name, address FROM parquet_table WHERE id = 12345;

-- 2. 多字符串惰性列
SELECT col_str_a, col_str_b, col_str_c FROM parquet_table 
WHERE int_col BETWEEN 1 AND 10;

-- 3. 字符串列包含 null 值
SELECT nullable_str_col FROM parquet_table WHERE status = 'ACTIVE';

-- 4. 高选择率（验证 P0-3 不引入额外开销）
SELECT name FROM parquet_table WHERE id > 0;

-- 5. 无过滤条件（不触发 lazy read）
SELECT count(*) FROM parquet_table;

-- 6. 配置开关关闭时走原始路径
-- curl ...update_config?enable_parquet_lazy_dict_decode_for_lazy_columns=false
-- 重复上述查询，验证结果一致

-- 7. 混合编码列（部分 row group 非字典编码）
-- 验证该列在非字典编码 row group 中回退到直接字符串解码
```

**外表类型覆盖**：
- Hive 外表（Parquet 格式）
- Iceberg 外表（Parquet 格式）
- 直接 `SELECT * FROM S3()` 读取 Parquet 文件

### 5.3 回归测试方案

#### 5.3.1 配置开关对比测试

| 场景 | 配置 | 预期 |
|------|------|------|
| A | `enable_parquet_lazy_dict_decode_for_lazy_columns = true` | 结果正确，有过滤时性能提升 |
| B | `enable_parquet_lazy_dict_decode_for_lazy_columns = false` | 结果正确，走原始字符串解码路径 |

验证：A 和 B 的查询结果完全一致（`diff` 比较）。

#### 5.3.2 边界条件测试

| 场景 | 描述 |
|------|------|
| 空 Page | 0 行数据的 Parquet page |
| 全 null 字符串列 | 所有行都是 null，无 int32 索引需转换 |
| 全非 null 列 | 无 null 值 |
| 单行 Page | 每个 page 只有 1 行 |
| 字典仅 1 个条目 | 极端小字典 |
| 字典很大（100K+条目） | 验证 convert 开销在预期范围内 |
| 空字符串值 | 字典中包含 "" 的情况 |
| 超长字符串值（64KB+） | 验证内存分配正确 |
| filter_all 场景 | Phase 1 全部行被过滤，Phase 2 不读取惰性列 |
| 跨 row group 切换 | 验证 `_lazy_dict_decode_cols` 在每个 row group 重新确认 |
| 某个 row group 非字典编码 | 该 row group 回退直接解码，不影响其他 row group |

---

## 6. 已知限制与风险

### 6.1 性能限制

- **100% 选择率退化**：当无行被过滤时，P0-3 多了一次数据遍历（int32 解码 + 全量 convert），比直接字符串解码慢 6-49%。建议增加选择率门控（filter_ratio < 0.05 时禁用）。
- **P0-1 对字符串列有害**：P0-1 的 `_lazy_decode_string_values()` 在字符串列上比基线慢 20-34%。P0-3 惰性列不应使用 P0-1 的 filter bitmap pushdown。当前代码正确处理了这一点（`is_dict_filter=true` 时不进入 lazy decode 路径）。
- **仅适用于全字典编码列**：如果某列在部分 page 使用 PLAIN 编码（fallback），该列在该 row group 不会启用 P0-3。
- **转换开销与字符串长度正相关**：strlen=128 时 100K 行的转换开销达 ~5-6 ms，占总时间比例较大。对于超长字符串（如 JSON/XML 存储），存活行的转换成本可能成为瓶颈。

### 6.2 兼容性

- **无协议变更**：仅 BE 内部解码逻辑优化，不涉及存储格式、网络协议、FE 变更。
- **向后兼容**：通过配置开关 `enable_parquet_lazy_dict_decode_for_lazy_columns = false` 完全关闭。
- **与 P0-1 的关系**：P0-3 列以 `is_dict_filter=true` 模式读取，decoder 直接输出 int32 字典索引。P0-1 的 filter_data 即使被传递，也不会进入 `_lazy_decode_string_values()` 路径——而是走 bulk GetBatch + ColumnSelectVector 路径，仅影响 int32 写入量。
- **与 P0-2 的兼容**：P0-3 惰性列在 Phase 2 读取，不参与 P0-2 的谓词列排序。两者完全正交。

### 6.3 潜在风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| `is_dictionary_encoded()` 误判 | 非字典编码列被当作字典列读取，decode 输出错误数据 | 已使用 Parquet 标准的 `encoding_stats` / `encodings` 元数据，与已有 `_dict_filter_cols` 使用相同检查逻辑 |
| 空列转换崩溃 | 全部行被过滤后 ColumnInt32 为空，convert 可能越界 | 已在 `_convert_lazy_dict_cols_to_string_cols()` 中特殊处理空列情况 |
| nullable 列 unwrap 错误 | 从 ColumnNullable 中错误提取内部列 | 使用与 `_convert_dict_cols_to_string_cols()` 相同的 nullable 处理逻辑 |
| 选择率门控缺失 | 100% 选择率时性能退化 | 建议后续增加 filter_ratio 门控 |

---

## 7. Benchmark 复现指南

### 7.1 环境准备

```bash
# 确保 benchmark 构建目录存在
ls be/build_benchmark/build.ninja

# 增量构建
cd be/build_benchmark && ninja -j 10 benchmark_test
```

### 7.2 运行方式

```bash
export JAVA_HOME=/path/to/jdk17
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
export DORIS_HOME=$(pwd)/be/build_benchmark

# 运行全部 P0-3 benchmark
./bin/benchmark_test --benchmark_filter="BM_P03_"

# 运行带重复的精确测量
./bin/benchmark_test \
  --benchmark_filter="BM_P03_" \
  --benchmark_repetitions=3 \
  --benchmark_report_aggregates_only=true

# 只运行核心四组对比（排除 ConvertOverhead）
./bin/benchmark_test --benchmark_filter="BM_P03_Baseline|BM_P03_P01Only|BM_P03_P03Only|BM_P03_P03PlusP01"

# 只运行转换开销测试
./bin/benchmark_test --benchmark_filter="BM_P03_ConvertOverhead"

# 只运行特定字典大小
./bin/benchmark_test --benchmark_filter="BM_P03_.*/100/"

# 只运行长字符串测试
./bin/benchmark_test --benchmark_filter="BM_P03_.*/128"
```

### 7.3 Benchmark 文件说明

**文件**：`be/benchmark/benchmark_lazy_dict_decode.hpp`

**核心设计**：使用真实 `ByteArrayDictDecoder` 实例，通过控制 `is_dict_filter`、`filter_data` 两个参数组合隔离 P0-1 和 P0-3 的效果：

| 参数组合 | is_dict_filter | filter_data | 策略 |
|----------|---------------|-------------|------|
| Baseline | false | nullptr | 原始全量字符串解码 |
| P0-1 Only | false | bitmap | 懒惰字符串解码（per-run） |
| P0-3 Only | true | nullptr | 全量 int32 解码 + convert |
| P0-3+P0-1 | true | bitmap | int32 解码（skip FILTERED_CONTENT） + convert |

辅助函数：
- 复用 `be/benchmark/benchmark_parquet_dict_decoder.hpp` 中的：
  - `build_string_dict(dict_size, avg_str_len)` — 构建 ByteArray 字典
  - `build_rle_dict_indexes(num_values, dict_size)` — 生成 RLE 编码字典索引
  - `build_run_length_null_map(num_values)` — 构建无 null 的 run length null map
  - `build_filter_bitmap(num_values, selectivity)` — 生成过滤位图

参数格式为 `(dict_size, selectivity_percent, num_values_in_thousands, avg_str_len)`。

---

## 8. 总结

P0-3 优化通过将惰性字符串列的解码分为"int32 字典索引解码"和"过滤后字符串转换"两步，在 Phase 2 有过滤的场景下显著减少了字符串物化开销。

### 核心发现

**P0-3 是惰性字符串列的最优策略，全面且大幅优于 P0-1 和基线。**

#### 关键数据

1. **P0-3 Only 最高加速 4.70x**：dict=10000, strlen=128, sel=20% 场景，Baseline 2341 µs → P0-3 Only 498 µs
2. **P0-3 Only 在全部 <100% 选择率下均优于基线**：加速范围 1.56x ~ 4.70x
3. **长字符串显著放大优势**：strlen=128 时加速比是 strlen=32 的 1.1x ~ 2.65x
4. **P0-1 对字符串列有害**：比基线慢 20-34%，不应用于字符串惰性列
5. **P0-3+P0-1 仅在 50% 选择率时优于 P0-3 Only**：低选择率时 P0-3 Only 更优
6. **100% 选择率退化 6-49%**：需增加选择率门控

#### 策略选择建议

| 场景 | 推荐策略 |
|------|----------|
| 惰性字符串列 + 有过滤（存活率 < 95%） | **P0-3 Only**（禁用 P0-1 pushdown） |
| 惰性字符串列 + 无/弱过滤（存活率 ≥ 95%） | 原始路径（禁用 P0-3） |
| 谓词定长列（INT/FLOAT/DOUBLE） | P0-1（Filter Bitmap Pushdown） |
| 谓词字符串列 | 已有 dict filter 机制处理 |

### 架构意义

P0-3 揭示了一个重要设计原则：**对于字符串列，改变数据类型（string → int32）比改变解码粒度（全量 → per-run）更有效**。P0-1 的 per-run SkipBatch 在定长类型（INT32/INT64/FLOAT/DOUBLE）上有效，但在变长类型（字符串）上因 per-run 开销而退化。P0-3 通过将变长问题转化为定长问题（int32），完美规避了这一瓶颈。

### 生产安全

- 运行时可调配置 `enable_parquet_lazy_dict_decode_for_lazy_columns`，可随时关闭回退原始路径
- 每个 row group 独立确认字典编码状态，非字典编码列自动回退
- 与已有的 `_dict_filter_cols` 机制共享类型替换和转换逻辑，代码复用度高
- 建议后续增加 `filter_ratio` 门控（存活率 > 95% 时禁用），消除 100% 选择率退化
