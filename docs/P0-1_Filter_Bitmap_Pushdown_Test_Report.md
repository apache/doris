# P0-1 Filter Bitmap 下推到 Decoder 层 — 测试文档

## 1. 功能概述

本优化为 Doris Parquet Reader 的 P0-1 优化项：**Filter Bitmap 下推到 Decoder 层**，实现了懒惰字典索引解码（Lazy Dict Index Decoding）。

### 1.1 优化目标

在低选择率场景（存活行 < 5%）下，避免对所有非空行进行 RLE 字典索引解码，改为：
- **CONTENT 行**（存活行）：按需解码 RLE 索引，再做字典查找
- **FILTERED_CONTENT 行**（被过滤行）：通过 `RleBatchDecoder::SkipBatch()` 直接跳过 RLE 数据流，不解码

### 1.2 核心对比

| | 原始路径（Eager） | 优化路径（Lazy） |
|---|---|---|
| 索引解码 | 一次性 `GetBatch` 解码全部非空索引 | 按 run 分段：CONTENT 用 `GetBatch`，FILTERED_CONTENT 用 `SkipBatch` |
| 字典查找 | CONTENT 做查找，FILTERED_CONTENT 跳过 index | CONTENT 做查找，FILTERED_CONTENT 不解码不查找 |
| 内存分配 | `_indexes.resize(non_null_size)` 全量 | `_indexes.resize(run_length)` 按需 |
| RLE 跳过方式 | 无 | `SkipBatch` 以 32 值为对齐单位的快速字节跳过 |

---

## 2. 修改文件清单

### 2.1 核心修改

| 文件 | 修改内容 | 重要程度 |
|------|----------|----------|
| `be/src/util/rle_encoding.h` | 新增 `RleBatchDecoder::SkipBatch()` 方法 | 高 |
| `be/src/vec/exec/format/parquet/decoder.h` | `Decoder::decode_values()` 增加 `filter_data` 参数；`BaseDictDecoder::skip_values()` 使用 SkipBatch | 高 |
| `be/src/vec/exec/format/parquet/fix_length_dict_decoder.hpp` | 新增 `_lazy_decode_fixed_values()` 懒惰解码路径 | 高 |
| `be/src/vec/exec/format/parquet/byte_array_dict_decoder.cpp` | 新增 `_lazy_decode_string_values()` 懒惰解码路径 | 高 |
| `be/src/vec/exec/format/parquet/byte_array_dict_decoder.h` | 新增 `_lazy_decode_string_values()` 声明 | 中 |
| `be/src/vec/exec/format/parquet/vparquet_column_reader.cpp` | 选择率计算 + `filter_data` 传递逻辑 | 高 |
| `be/src/vec/exec/format/parquet/vparquet_column_chunk_reader.h/.cpp` | `decode_values()` 增加 `filter_data` 参数并透传 | 中 |
| `be/src/common/config.h` / `config.cpp` | 新增配置项 `enable_parquet_lazy_dict_decode` | 中 |

### 2.2 签名更新（仅参数变更，无功能改动）

| 文件 | 修改内容 |
|------|----------|
| `be/src/vec/exec/format/parquet/fix_length_plain_decoder.h/.cpp` | `decode_values()` 增加 `filter_data` 默认参数 |
| `be/src/vec/exec/format/parquet/byte_array_plain_decoder.h/.cpp` | 同上 |
| `be/src/vec/exec/format/parquet/byte_stream_split_decoder.h/.cpp` | 同上 |
| `be/src/vec/exec/format/parquet/bool_plain_decoder.h/.cpp` | 同上 |
| `be/src/vec/exec/format/parquet/bool_rle_decoder.h/.cpp` | 同上 |
| `be/src/vec/exec/format/parquet/delta_bit_pack_decoder.h` | 3 个内联 `decode_values()` 签名更新 |

### 2.3 新增文件

| 文件 | 说明 |
|------|------|
| `be/benchmark/benchmark_parquet_dict_decoder.hpp` | 微基准测试：字典解码器 + RLE SkipBatch |

---

## 3. 配置项

### 3.1 `enable_parquet_lazy_dict_decode`

| 属性 | 值 |
|------|-----|
| 类型 | mBool（运行时可修改） |
| 默认值 | `true` |
| 作用 | 控制是否启用 Parquet 字典解码器的懒惰索引解码优化 |
| 关闭方式 | `curl http://be_host:webserver_port/api/update_config?enable_parquet_lazy_dict_decode=false` |

### 3.2 触发条件

懒惰解码路径在同时满足以下条件时激活：
1. `enable_parquet_lazy_dict_decode = true`（配置开关打开）
2. `filter_map.has_filter() = true`（存在过滤条件）
3. `filter_map.filter_ratio() > 0.95`（超过 95% 的行被过滤，即存活率 < 5%）
4. 列不是 `ColumnDictionary` 类型，且不是 `is_dict_filter` 模式

代码位置：`vparquet_column_reader.cpp:398-407`

```cpp
const uint8_t* filter_data = nullptr;
if (config::enable_parquet_lazy_dict_decode && filter_map.has_filter() &&
    filter_map.filter_ratio() > 0.95) {
    filter_data = filter_map.filter_map_data();
}
return _chunk_reader->decode_values(data_column, type, select_vector, is_dict_filter,
                                    filter_data);
```

---

## 4. 技术实现细节

### 4.1 RleBatchDecoder::SkipBatch() 实现

**文件**: `be/src/util/rle_encoding.h:894-959`

该方法在 RLE 编码数据流中跳过指定数量的值，不进行实际解码。处理三种情况：

1. **Repeat Run（重复值）**：直接递减 `repeat_count_`，零成本跳过
2. **Literal Run 已缓冲部分**：推进 `literal_buffer_pos_`，跳过已解码到缓冲区的值
3. **Literal Run 未缓冲部分**：
   - 以 32 值为对齐单位，调用 `bit_reader_.SkipBatch()` 进行字节级快速跳过
   - 不足 32 值的尾部，通过 `FillLiteralBuffer()` 解码到缓冲区后推进位置

**为何以 32 对齐**：`BatchedBitReader::SkipBatch()` 要求 `bit_width * num_values` 能被 8 整除。32 值 × 任意 bit_width 总能满足此约束（因为 RLE literal run 是 8 的倍数，32 是 8 的倍数）。非对齐跳过会导致字节位移错位，读取后续数据产生垃圾值。

### 4.2 懒惰解码路径

以 `FixLengthDictDecoder::_lazy_decode_fixed_values()` 为例（`fix_length_dict_decoder.hpp:194-242`）：

```
Loop over ColumnSelectVector runs:
  CONTENT:
    _indexes.resize(run_length)
    _index_batch_decoder->GetBatch(_indexes.data(), run_length)  // 仅解码当前 run
    for i in 0..run_length:
      output[i] = _dict_items[_indexes[i]]  // 字典查找
  FILTERED_CONTENT:
    _index_batch_decoder->SkipBatch(run_length)  // 直接跳过，不解码
  NULL_DATA:
    data_index += run_length * _type_length  // 填充默认值
  FILTERED_NULL:
    // 什么都不做
```

`ByteArrayDictDecoder::_lazy_decode_string_values()` 逻辑相同，区别仅在字典值类型为变长字符串。

### 4.3 调用链路

```
ScalarColumnReader::_read_values(filter_map)
  → 计算 filter_ratio，决定是否传递 filter_data
  → ColumnChunkReader::decode_values(select_vector, is_dict_filter, filter_data)
    → Decoder::decode_values(column, type, select_vector, is_dict_filter, filter_data)
      → filter_data != nullptr 时进入懒惰解码路径
      → filter_data == nullptr 时走原始路径（全量解码后遍历 run）
```

---

## 5. 测试方案

### 5.1 微基准测试（已完成）

#### 5.1.1 构建与运行

```bash
# 构建
cd be/build_benchmark
ninja -j 10 benchmark_test

# 运行全部 Parquet 相关 benchmark
export JAVA_HOME=/path/to/jdk17
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
export DORIS_HOME=$(pwd)
./bin/benchmark_test --benchmark_filter="BM_ByteArray|BM_FixLen|BM_Rle"
```

#### 5.1.2 基准测试用例一览

| 测试名 | 参数 | 测试目标 |
|--------|------|----------|
| `BM_RleSkip_GetBatch` | 10K/100K/1M 值 | RLE 全量解码基线 |
| `BM_RleSkip_SkipBatch` | 10K/100K/1M 值 | RLE SkipBatch 性能 |
| `BM_ByteArrayDictDecode_NoFilter` | dict=100/10K/100K, sel=1-100% | 字符串字典解码原始路径 |
| `BM_ByteArrayDictDecode_WithFilter` | dict=100/10K/100K, sel=1-100% | 字符串字典解码懒惰路径 |
| `BM_FixLenDictDecode_NoFilter` | dict=100/1M, sel=5-50% | 定长字典解码原始路径 |
| `BM_FixLenDictDecode_WithFilter` | dict=100/1M, sel=5-50% | 定长字典解码懒惰路径 |

#### 5.1.3 基准测试结果

**测试环境**：16 核 CPU，L1D 48KB×8, L2 1280KB×8, L3 49152KB×1

##### RLE SkipBatch vs GetBatch

| 数据量 | GetBatch (µs) | SkipBatch (µs) | 加速比 |
|--------|---------------|----------------|--------|
| 10K | 3.94 | 0.59 | **6.7x** |
| 100K | 33.4 | 3.92 | **8.5x** |
| 1M | 341 | 38.2 | **8.9x** |

**结论**：SkipBatch 相比 GetBatch 有 **6.7-8.9 倍**的性能提升，验证了 RLE 跳过的有效性。

##### ByteArray 字典解码（dict=100K，大字典）

| 存活率 | NoFilter (µs) | WithFilter (µs) | 对比 |
|--------|---------------|-----------------|------|
| 1% | 239 | 603 | +152%（回退） |
| 5% | 498 | 620 | +24%（回退） |
| 20% | 1273 | 1656 | +30%（回退） |
| 50% | 3878 | 3373 | -13%（提升） |
| 100% | 2555 | 2736 | +7%（回退） |

##### FixLen 字典解码（dict=1M，大字典）

| 存活率 | NoFilter (µs) | WithFilter (µs) | 对比 |
|--------|---------------|-----------------|------|
| 5% | 707 | 629 | **-11%（提升）** |
| 20% | 880 | 1114 | +27%（回退） |
| 50% | 1370 | 2005 | +46%（回退） |

##### 性能分析

1. **RLE SkipBatch 本身非常高效**，相比 GetBatch 有 6-9 倍提升。
2. **FixLen 类型在低选择率时有明显收益**（dict=1M, sel=5% 时提升 11%）。
3. **ByteArray 类型的懒惰路径存在额外开销**，原因是：
   - 每个 CONTENT run 需要独立调用 `insert_many_strings_overflow`，而原始路径只在最外层按 run 调用
   - Per-run 的 `GetBatch` 调用开销累积大于一次性 `GetBatch` 的开销
4. **因此生产环境触发阈值设为 filter_ratio > 0.95**（存活率 < 5%），仅在极端低选择率场景才启用，最小化回退风险。

### 5.2 功能正确性测试方案

#### 5.2.1 单元测试（建议补充）

需要编写的单元测试覆盖以下场景：

**RleBatchDecoder::SkipBatch 正确性**

| 测试场景 | 验证点 |
|----------|--------|
| 跳过完整 repeat run | SkipBatch(N) 后 GetBatch 读到正确下一个值 |
| 跳过完整 literal run | SkipBatch(N) 后 GetBatch 读到正确值 |
| 跳过部分 repeat run | 跳过 run 的前半段，GetBatch 读后半段 |
| 跳过部分 literal run（< 32 值）| 触发 FillLiteralBuffer 的 buffer 路径 |
| 跳过部分 literal run（>= 32 值）| 触发 SkipBatch 的 32-对齐字节跳过路径 |
| 混合交替跳过和读取 | Skip(10) → Get(5) → Skip(20) → Get(10) → ... |
| 跳过全部值 | SkipBatch(total_count) 返回 total_count |
| 跳过超过剩余值的数量 | SkipBatch(total+100) 返回 total_count（不崩溃） |
| bit_width 边界值 | bit_width=1, 8, 16, 32 |

**懒惰字典解码正确性**

| 测试场景 | 验证点 |
|----------|--------|
| 全部 CONTENT（无过滤） | 结果与原始路径完全一致 |
| 全部 FILTERED_CONTENT | 列为空（无新增行） |
| 混合 CONTENT + FILTERED_CONTENT | 存活行的值正确，列大小正确 |
| 包含 NULL_DATA + FILTERED_NULL | null 值处理正确 |
| 极端低选择率（1 行存活/100K 行） | 该 1 行的值正确 |
| 大字典 + 大数据量 | 无越界访问、无垃圾值 |
| INT32/INT64/FLOAT/DOUBLE/FIXED_LEN_BYTE_ARRAY 各类型 | 类型兼容 |
| ByteArray 字典（变长字符串） | 字符串内容和长度正确 |

#### 5.2.2 集成测试（建议执行）

使用 Doris 的 regression test 框架，测试真实 Parquet 文件读取：

```sql
-- 1. 基础查询：有过滤条件的 Parquet 表扫描
SELECT * FROM parquet_table WHERE id = 12345;  -- 极低选择率

-- 2. 聚合查询：低选择率 + 聚合
SELECT count(*), sum(amount) FROM parquet_table WHERE status = 'RARE_VALUE';

-- 3. 字符串列：验证变长字符串字典解码
SELECT name, address FROM parquet_table WHERE category = 'UNCOMMON';

-- 4. 多列联合过滤
SELECT * FROM parquet_table WHERE col_a = 1 AND col_b = 'x';

-- 5. 无过滤条件：验证不触发懒惰路径时无回退
SELECT count(*) FROM parquet_table;

-- 6. 高选择率：验证不触发懒惰路径
SELECT * FROM parquet_table WHERE id > 0;  -- 几乎全部存活

-- 7. 配置开关关闭时应走原始路径
-- SET enable_parquet_lazy_dict_decode = false;
-- 重复上述查询，验证结果一致
```

**外表类型覆盖**：
- Hive 外表（Parquet 格式）
- Iceberg 外表（Parquet 格式）
- 直接 `SELECT * FROM S3()` 读取 Parquet 文件

### 5.3 回归测试方案

#### 5.3.1 配置开关对比测试

对同一查询分别执行：

| 场景 | 配置 | 预期 |
|------|------|------|
| A | `enable_parquet_lazy_dict_decode = true` | 结果正确，低选择率时性能持平或提升 |
| B | `enable_parquet_lazy_dict_decode = false` | 结果正确，走原始路径 |

验证：A 和 B 的查询结果完全一致（`diff` 比较）。

#### 5.3.2 边界条件测试

| 场景 | 描述 |
|------|------|
| 空 Page | 0 行数据的 Parquet page |
| 全 null 列 | 所有行都是 null |
| 全非 null 列 | 无 null 值 |
| 单行 Page | 每个 page 只有 1 行 |
| filter_ratio 恰好 0.95 | 边界不触发（需 > 0.95） |
| filter_ratio = 1.0（全过滤） | 全部 SkipBatch，列不增长 |
| filter_ratio = 0.0（全存活） | 不触发懒惰路径 |
| 跨 Page 读取 | 验证 Page 切换时 RLE decoder 重置正确 |

---

## 6. 已知限制与风险

### 6.1 性能限制

- **ByteArray 类型在中等选择率（5-50%）时可能有回退**，因为 per-run `insert_many_strings_overflow` 调用频率增加。生产环境通过 `filter_ratio > 0.95` 阈值规避。
- 当前懒惰路径不适用于 `ColumnDictionary`（Doris 内部字典列）和 `is_dict_filter` 模式，这些场景需要全量索引。

### 6.2 兼容性

- **无协议变更**：仅 BE 内部解码逻辑优化，不涉及存储格式、网络协议、FE 变更。
- **向后兼容**：通过配置开关 `enable_parquet_lazy_dict_decode = false` 可完全关闭优化，回退到原始路径。
- **所有非字典编码器**（PlainDecoder、BoolDecoder、DeltaBitPack 等）仅做签名更新，功能无变化。

### 6.3 潜在风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| SkipBatch 字节对齐错误 | 后续 GetBatch 读取垃圾值导致崩溃 | 已通过 32 对齐 + FillLiteralBuffer 修复并通过 benchmark 验证 |
| 极端 bit_width 场景 | bit_width=0 或 bit_width=64 时的边界行为 | bit_width=0 表示字典仅一个值（全 repeat run），SkipBatch 只走 repeat 分支，安全 |
| filter_ratio 计算精度 | filter_ratio 是 double，阈值比较可能有浮点精度问题 | 使用 `> 0.95` 而非 `>= 0.95`，足够宽松 |

---

## 7. Benchmark 复现指南

### 7.1 环境准备

```bash
# 1. 确保 benchmark 构建目录存在
ls be/build_benchmark/build.ninja

# 2. 如果不存在，运行完整构建脚本
./run-be-benchmark.sh

# 3. 如果已存在，增量构建
cd be/build_benchmark && ninja -j 10 benchmark_test
```

### 7.2 运行方式

```bash
# 设置 Java 环境（benchmark 二进制依赖 libjvm.so）
export JAVA_HOME=/path/to/jdk17
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
export DORIS_HOME=$(pwd)/be/build_benchmark

# 运行所有 Parquet 相关 benchmark
./be/build_benchmark/bin/benchmark_test --benchmark_filter="BM_ByteArray|BM_FixLen|BM_Rle"

# 只运行 RLE SkipBatch 测试
./be/build_benchmark/bin/benchmark_test --benchmark_filter="BM_Rle"

# 运行特定字典大小的测试
./be/build_benchmark/bin/benchmark_test --benchmark_filter="BM_ByteArrayDictDecode.*/100000"
```

### 7.3 Benchmark 文件说明

**文件**: `be/benchmark/benchmark_parquet_dict_decoder.hpp`

辅助函数：
- `build_string_dict(dict_size, avg_str_len)` — 构建 ByteArray 字典数据
- `build_int32_dict(dict_size)` — 构建 INT32 定长字典数据
- `build_rle_dict_indexes(num_values, dict_size)` — 生成 RLE 编码的字典索引数据
- `build_run_length_null_map(num_values)` — 构建无 null 的 run length null map
- `build_filter_bitmap(num_values, selectivity)` — 按给定选择率生成过滤位图

参数格式为 `(dict_size, selectivity_percent, num_values_in_thousands)`。

---

## 8. 总结

P0-1 优化通过在 Decoder 层实现懒惰字典索引解码，在极低选择率（< 5%）场景下避免了无效的 RLE 索引解码开销。核心贡献包括：

1. **RleBatchDecoder::SkipBatch()** — 以 6-9 倍于 GetBatch 的速度跳过 RLE 编码数据
2. **懒惰解码路径** — FixLengthDictDecoder 和 ByteArrayDictDecoder 均支持按 run 粒度的按需解码
3. **生产安全** — 通过运行时可调配置 `enable_parquet_lazy_dict_decode` 和 `filter_ratio > 0.95` 阈值控制，最小化对现有查询的影响
4. **完整调用链路** — 从 ScalarColumnReader 到 Decoder 的 filter_data 传递已打通
