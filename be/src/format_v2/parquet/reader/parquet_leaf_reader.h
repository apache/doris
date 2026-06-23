// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_type.h"

namespace parquet {
class ColumnDescriptor;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace arrow {
class Array;
} // namespace arrow

namespace doris::format::parquet {

struct ParquetLeafReaderTestAccess;

// 嵌套标量叶子的读取结果，将 Dremel 编码的 shape 和实际 value 分离。
//
// 设计意图：复杂 reader（LIST/MAP/STRUCT）先消费 shape（def_levels + rep_levels + value_indices）
// 重建容器结构（offsets + null_map），再按 value_indices 将 values_column 写入容器。
//
// 例子：MAP<STRING, INT> 的 value 列在 rep_level=[0,1,0] 表示 2 个 entry 的 3 个 slot。
//       复杂 reader 先根据 rep_levels 确定 offsets=[0,2]，再根据 value_indices 把 value 写入对应 slot。
//
// 字段说明：
//   records_read         - 本批读到的顶层记录数（从 ReadRecords 返回）
//   levels_written       - 本批实际产生的 level 数（= consumed_level_count）
//   value_slot_definition_level - 有资格包含 value 的 slot 的最小 def level（由父 reader 设定）
//   value_slot_repetition_level - 有资格包含 value 的 slot 的最大 rep level（由父 reader 设定）
//   def_levels[]         - definition levels 的拷贝
//   rep_levels[]         - repetition levels 的拷贝
//   value_indices[]      - level_idx → value buffer 中的下标（-1 表示该 slot 无 value，如 NULL key）
//   values_column        - 物化后的 value 列（非 nullable 的类型）
struct ParquetNestedScalarBatch {
    int64_t records_read = 0;
    int64_t levels_written = 0;
    int16_t value_slot_definition_level = 0;
    int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max();
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    std::vector<int64_t> value_indices;
    MutableColumnPtr values_column;

    bool empty() const { return levels_written == 0; }
};

// Arrow RecordReader 一次 ReadRecords() 后的批次结果视图。
//
// 该类将 Arrow RecordReader 的内部状态"快照化"为一个不可变的视图，解决两个问题：
// 1. BinaryRecordReader 通过 GetBuilderChunks() 返回值的所有权，而固定宽度类型通过 values()。
//    统一为 ParquetLeafBatch 后，外部只需判断 is_binary_value() 选择数据源。
// 2. Arrow RecordReader 的数据是 one-shot transfer 语义（GetBuilderChunks 会 reset builder），
//    ParquetLeafBatch 将数据捕获后，允许多次读取（如先 build_null_map 再 append_values）。
//
// 字段说明：
//   _consumed_level_count  - 本次读取前 RecordReader 已经消费的 level 数（= levels_position）
//   _decoded_level_count   - 本次读取后 RecordReader 解码出的 level 总数（= levels_written）
//   _values_written        - 本批写出的 value 个数
//   _def_levels / _rep_levels - 指向 RecordReader 内部 level 数组的指针（非拥有）
//   _fixed_values          - 固定宽度类型的 value buffer 指针（非拥有）
//   _binary_chunks         - Binary 类型的 Arrow Array chunks（拥有所有权）
//   _read_dense_for_nullable - RecordReader 是否为 nullable 列启用了 dense 模式
class ParquetLeafBatch {
public:
    int64_t consumed_level_count() const { return _consumed_level_count; }
    int64_t decoded_level_count() const { return _decoded_level_count; }
    int64_t values_written() const { return _values_written; }
    bool read_dense_for_nullable() const { return _read_dense_for_nullable; }
    const int16_t* def_levels() const { return _def_levels; }
    const int16_t* rep_levels() const { return _rep_levels; }

private:
    friend class ParquetLeafReader;

    bool is_binary_value() const;

    DecodedValueKind _value_kind = DecodedValueKind::INT32;
    int64_t _consumed_level_count = 0;
    int64_t _decoded_level_count = 0;
    int64_t _values_written = 0;
    const int16_t* _def_levels = nullptr;
    const int16_t* _rep_levels = nullptr;
    const uint8_t* _fixed_values = nullptr;
    bool _read_dense_for_nullable = false;
    std::vector<std::shared_ptr<::arrow::Array>> _binary_chunks;
};

// Parquet 原始类型叶子的值读取器。每个 ScalarColumnReader 在读取时创建一个临时 ParquetLeafReader。
//
// 职责：包装 Arrow 的 RecordReader，将其解码出的 type-erased level/value buffer 转换为 Doris
// Column 可以消费的形式。
//
// 该类不持有任何可变状态（除 _record_reader 的 shared_ptr），因此是 const 可调用的。
// RecordReader 本身由 ParquetColumnReaderFactory 按 leaf_column_id 缓存和共享。
//
// 对外提供两组接口：
//
// ① 平铺列（top-level primitive column）的读取路径：
//      read_batch() → build_null_map() + append_values()
//      适用于非嵌套的基本类型列，如 SELECT id, name FROM t。
//
// ② 嵌套叶子（nested LIST/MAP/STRUCT 内的 primitive leaf）的读取路径：
//      read_nested_batch()
//      一步完成"读取 level/value → 解析 value slot 映射 → 物化 values_column"，
//      返回 ParquetNestedScalarBatch 供父 reader 组装容器结构。
class ParquetLeafReader {
public:
    ParquetLeafReader(const ::parquet::ColumnDescriptor* descriptor,
                      ParquetTypeDescriptor type_descriptor, DataTypePtr type, std::string name,
                      std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                      ParquetColumnReaderProfile profile = {},
                      const cctz::time_zone* timezone = nullptr, bool enable_strict_mode = false,
                      std::function<Status(MutableColumnPtr&, const DecodedColumnView&)>
                              decoded_value_appender = nullptr);

    // ①a. 从 Arrow RecordReader 读取 batch_rows 行，将结果捕获到 ParquetLeafBatch 中。
    // 调用方拿到 batch 后可以多次访问 level 和 value 信息。
    Status read_batch(int64_t batch_rows, ParquetLeafBatch* batch, int64_t* rows_read) const;

    // ①b. 根据 batch 中的 definition levels 构建 Doris NullMap。
    // def_level == max_definition_level → 非 NULL，否则为 NULL。
    // 如果该列没有 optional/repeated 祖先（max_definition_level == 0），直接返回 OK。
    Status build_null_map(const ParquetLeafBatch& batch, int64_t records_read,
                          NullMap* null_map) const;

    // ①c. 将 batch 中的值写入目标 Doris Column。
    // - 固定宽度类型：直接从 _fixed_values 指针读取
    // - Binary 类型：从 _binary_chunks 构造 StringRef 向量
    // - FLOAT16 类型：从 binary 解码后转为 float
    // - dense nullable 模式：先按 null_map 展开为间隔排列的 values 再写入
    // 值转换（如 INT64 timestamp → DateTime）通过 DataTypeSerde::read_column_from_decoded_values 完成。
    Status append_values(const ParquetLeafBatch& batch, int64_t row_count, const NullMap* null_map,
                         MutableColumnPtr& column) const;

    // ② 嵌套叶子的一步式读取。内部调用 read_batch() 获取 level/value，
    // 然后解析 value layout（Arrow 的 RecordReader 在不同场景下按不同粒度写入 value：
    // LEVELS / VALUE_SLOTS / LEAF_VALUES / PAYLOAD_VALUE_SLOTS），
    // 构建 value_indices 映射和 value_nulls，最后调用 append_values() 物化 values_column。
    //
    // value_slot_definition_level: 有资格容纳 value 的 slot 的最小 def level。
    //   例如 MAP key 的 value_slot_definition_level = key 的 max_dl（只有 def>=max_dl 的 slot 才有 key 值）。
    // value_slot_repetition_level: 有资格容纳 value 的 slot 的最大 rep level。
    //   用于过滤属于其他 repeated 层级（如嵌套 LIST inside MAP）的 slot。
    Status read_nested_batch(
            int64_t batch_rows, int16_t value_slot_definition_level,
            ParquetNestedScalarBatch* batch,
            int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max()) const;

private:
    friend struct ParquetLeafReaderTestAccess;

    // 将 RecordReader 的内部状态捕获为不可变的 ParquetLeafBatch。
    // 分别处理固定宽度类型（values()）和 binary 类型（GetBuilderChunks()）。
    Status collect_batch(::parquet::internal::RecordReader& record_reader,
                         ParquetLeafBatch* batch) const;

    // 为 dense nullable 模式构建间隔排列的固定宽度值数组。
    // Arrow RecordReader 在 read_dense_for_nullable 模式下只写非 NULL 值（紧凑排列），
    // 本函数按 null_map 将它们展开为与行一一对应的间隔排列格式。
    Status build_spaced_fixed_values(const ParquetLeafBatch& batch, int64_t row_count,
                                     const NullMap* null_map,
                                     std::vector<uint8_t>* spaced_values) const;

    Status build_nested_batch_from_leaf_batch(const ParquetLeafBatch& leaf_batch,
                                              int64_t records_read,
                                              int16_t value_slot_definition_level,
                                              ParquetNestedScalarBatch* batch,
                                              int16_t value_slot_repetition_level) const;

    const ::parquet::ColumnDescriptor* _descriptor =
            nullptr;                        // Arrow 列描述符（physical_type, max_dl, max_rl）
    ParquetTypeDescriptor _type_descriptor; // 类型编码信息（decimal 精度、timestamp 单位等）
    DataTypePtr _type;                      // Doris 目标类型
    std::string _name;                      // 列名（用于报错信息）
    std::shared_ptr<::parquet::internal::RecordReader>
            _record_reader;                     // Arrow 物理列读取器（共享所有权）
    ParquetColumnReaderProfile _profile;        // Profile 计数器
    const cctz::time_zone* _timezone = nullptr; // 时区（timestamp 转换用）
    bool _enable_strict_mode = false;           // 严格模式（类型不匹配时是否报错）
    std::function<Status(MutableColumnPtr&, const DecodedColumnView&)> _decoded_value_appender;
};

} // namespace doris::format::parquet
