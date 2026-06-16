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

#include "format_v2/parquet/reader/parquet_leaf_reader.h"

#include <arrow/array/array_binary.h>
#include <parquet/api/schema.h>
#include <parquet/column_reader.h>
#include <parquet/exception.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/string_ref.h"
#include "runtime/runtime_profile.h"
#include "util/simd/bits.h"

namespace doris::format::parquet {
namespace {

// 将 ParquetTimeUnit 转换为 DataTypeSerde 层的 DecodedTimeUnit。
DecodedTimeUnit decoded_time_unit(ParquetTimeUnit time_unit) {
    switch (time_unit) {
    case ParquetTimeUnit::MILLIS:
        return DecodedTimeUnit::MILLIS;
    case ParquetTimeUnit::MICROS:
        return DecodedTimeUnit::MICROS;
    case ParquetTimeUnit::NANOS:
        return DecodedTimeUnit::NANOS;
    case ParquetTimeUnit::UNKNOWN:
    default:
        return DecodedTimeUnit::UNKNOWN;
    }
}

// 返回指定 DecodedValueKind 的单个值的字节大小。
// Binary/FIXED_BINARY 返回 error（它们不是固定宽度类型）。
Status decoded_fixed_value_size(const std::string& column_name, DecodedValueKind value_kind,
                                size_t* value_size) {
    switch (value_kind) {
    case DecodedValueKind::BOOL:
        *value_size = sizeof(bool);
        return Status::OK();
    case DecodedValueKind::INT32:
        *value_size = sizeof(int32_t);
        return Status::OK();
    case DecodedValueKind::UINT32:
        *value_size = sizeof(uint32_t);
        return Status::OK();
    case DecodedValueKind::INT64:
        *value_size = sizeof(int64_t);
        return Status::OK();
    case DecodedValueKind::UINT64:
        *value_size = sizeof(uint64_t);
        return Status::OK();
    case DecodedValueKind::INT96:
        *value_size = 12;
        return Status::OK();
    case DecodedValueKind::FLOAT:
        *value_size = sizeof(float);
        return Status::OK();
    case DecodedValueKind::DOUBLE:
        *value_size = sizeof(double);
        return Status::OK();
    case DecodedValueKind::BINARY:
    case DecodedValueKind::FIXED_BINARY:
        return Status::InvalidArgument("Parquet binary value kind has no fixed value size for {}",
                                       column_name);
    }
    return Status::InternalError("Unknown decoded value kind for column {}", column_name);
}

// 从 BinaryRecordReader 获取 Arrow Array chunks。
// GetBuilderChunks() 会将 Arrow 内部 builder 的所有权转移出来并 reset，
// 因此每个 batch 只能调用一次。
Status get_binary_chunks(const std::string& column_name,
                         ::parquet::internal::RecordReader& record_reader,
                         std::vector<std::shared_ptr<::arrow::Array>>* chunks) {
    auto* binary_reader = dynamic_cast<::parquet::internal::BinaryRecordReader*>(&record_reader);
    if (binary_reader == nullptr) {
        return Status::InternalError("Parquet binary record reader is not available for column {}",
                                     column_name);
    }
    *chunks = binary_reader->GetBuilderChunks();
    return Status::OK();
}

// 将 Arrow BinaryArray / FixedSizeBinaryArray 的 chunks 转换为 Doris StringRef 向量。
//
// read_dense_for_nullable 模式：Arrow 只输出了非 NULL 的紧凑值，本函数按 null_map
// 将它们展开为与 records_read 对齐的稀疏数组（NULL 行填 nullptr + 0）。
//
// 非 dense 模式：直接按行一一对应转换。
Status build_binary_values(const std::string& column_name,
                           const std::vector<std::shared_ptr<::arrow::Array>>& chunks,
                           int64_t records_read, const NullMap* null_map,
                           bool read_dense_for_nullable, std::vector<StringRef>* binary_values) {
    std::vector<StringRef> compact_values;
    auto* values = read_dense_for_nullable ? &compact_values : binary_values;
    values->reserve(records_read);
    for (const auto& chunk : chunks) {
        if (chunk == nullptr) {
            return Status::Corruption(
                    "Parquet binary record reader returned null chunk for column {}", column_name);
        }
        if (auto* binary_array = dynamic_cast<::arrow::BinaryArray*>(chunk.get())) {
            for (int64_t row_idx = 0; row_idx < binary_array->length(); ++row_idx) {
                if (binary_array->IsNull(row_idx)) {
                    values->emplace_back(static_cast<const char*>(nullptr), 0);
                    continue;
                }
                int32_t length = 0;
                const uint8_t* value = binary_array->GetValue(row_idx, &length);
                values->emplace_back(reinterpret_cast<const char*>(value), length);
            }
        } else if (auto* fixed_array = dynamic_cast<::arrow::FixedSizeBinaryArray*>(chunk.get())) {
            for (int64_t row_idx = 0; row_idx < fixed_array->length(); ++row_idx) {
                if (fixed_array->IsNull(row_idx)) {
                    values->emplace_back(static_cast<const char*>(nullptr), 0);
                    continue;
                }
                values->emplace_back(reinterpret_cast<const char*>(fixed_array->GetValue(row_idx)),
                                     fixed_array->byte_width());
            }
        } else {
            return Status::InternalError("Unexpected Arrow binary array type for column {}",
                                         column_name);
        }
    }
    if (read_dense_for_nullable) {
        if (null_map == nullptr || null_map->size() != static_cast<size_t>(records_read)) {
            return Status::Corruption(
                    "Invalid dense nullable parquet null map for column {}: rows={}, null_map={}",
                    column_name, records_read, null_map == nullptr ? 0 : null_map->size());
        }
        const int64_t non_null_count = static_cast<int64_t>(simd::count_zero_num(
                reinterpret_cast<const int8_t*>(null_map->data()), null_map->size()));
        if (compact_values.size() != static_cast<size_t>(non_null_count)) {
            return Status::Corruption(
                    "Invalid dense nullable parquet binary values for column {}: values={}, "
                    "records={}, nulls={}",
                    column_name, compact_values.size(), records_read,
                    records_read - non_null_count);
        }
        binary_values->reserve(records_read);
        size_t value_idx = 0;
        for (int64_t record_idx = 0; record_idx < records_read; ++record_idx) {
            if ((*null_map)[record_idx] != 0) {
                binary_values->emplace_back(static_cast<const char*>(nullptr), 0);
                continue;
            }
            binary_values->emplace_back(compact_values[value_idx++]);
        }
        return Status::OK();
    }
    if (binary_values->size() != static_cast<size_t>(records_read)) {
        return Status::Corruption(
                "Invalid parquet binary record read result for column {}: rows={}, records={}",
                column_name, binary_values->size(), records_read);
    }
    return Status::OK();
}

// IEEE 754 half-precision (16-bit) → single-precision (32-bit) 转换。
// Parquet FLOAT16 用 FIXED_LEN_BYTE_ARRAY(2) 存储，Doris 没有原生 Float16 类型，
// 需要提升为 Float32。
float half_to_float(uint16_t value) {
    const uint32_t sign = (value & 0x8000U) << 16;
    const uint32_t exponent = (value & 0x7C00U) >> 10;
    const uint32_t mantissa = value & 0x03FFU;

    if (exponent == 0) {
        if (mantissa == 0) {
            return std::bit_cast<float>(sign);
        }
        const float subnormal = std::ldexp(static_cast<float>(mantissa), -24);
        return sign == 0 ? subnormal : -subnormal;
    }
    if (exponent == 0x1FU) {
        return std::bit_cast<float>(sign | 0x7F800000U | (mantissa << 13));
    }
    return std::bit_cast<float>(sign | ((exponent + 112U) << 23) | (mantissa << 13));
}

// 将 Parquet FLOAT16 的 binary values 批量解码为 float 向量。
// 每个 FLOAT16 值占 2 bytes，调用 half_to_float 逐值转换。
Status build_float16_values(const std::string& column_name,
                            const ParquetTypeDescriptor& type_descriptor,
                            const std::vector<StringRef>& binary_values, int64_t row_count,
                            std::vector<float>* float_values) {
    if (type_descriptor.fixed_length != 2) {
        return Status::Corruption("Invalid parquet Float16 length for column {}: {}", column_name,
                                  type_descriptor.fixed_length);
    }
    if (binary_values.size() != static_cast<size_t>(row_count)) {
        return Status::Corruption(
                "Invalid parquet Float16 value count for column {}: values={}, rows={}",
                column_name, binary_values.size(), row_count);
    }
    float_values->resize(static_cast<size_t>(row_count));
    for (int64_t row = 0; row < row_count; ++row) {
        const auto& binary_value = binary_values[static_cast<size_t>(row)];
        if (binary_value.data == nullptr && binary_value.size == 0) {
            (*float_values)[static_cast<size_t>(row)] = 0;
            continue;
        }
        if (binary_value.data == nullptr || binary_value.size != 2) {
            return Status::Corruption(
                    "Invalid parquet Float16 value for column {} at row {}: data={}, size={}",
                    column_name, row, binary_value.data == nullptr ? "null" : "non-null",
                    binary_value.size);
        }
        uint16_t raw_value = 0;
        std::memcpy(&raw_value, binary_value.data, sizeof(raw_value));
        (*float_values)[static_cast<size_t>(row)] = half_to_float(raw_value);
    }
    return Status::OK();
}

} // namespace

// 将 RecordReader 的内部状态捕获为不可变的 ParquetLeafBatch。
//
// 该函数在 RecordReader::ReadRecords() 之后立即调用，将 Arrow 返回的
// level/value buffer 指针（或 binary chunks 的所有权）快照到 batch 中。
// 之后 batch 可以被多次读取（如先 build_null_map 再 append_values），
// 不受 RecordReader 后续操作的干扰。
Status ParquetLeafReader::collect_batch(::parquet::internal::RecordReader& record_reader,
                                        ParquetLeafBatch* batch) const {
    DORIS_CHECK(batch != nullptr);
    batch->_def_levels = nullptr;
    batch->_rep_levels = nullptr;
    batch->_fixed_values = nullptr;
    batch->_binary_chunks.clear();
    // 根据 type_descriptor 确定 value_kind，控制后续 value 读取路径
    batch->_value_kind = decoded_value_kind(_type_descriptor);
    batch->_consumed_level_count = record_reader.levels_position();
    batch->_decoded_level_count = record_reader.levels_written();
    if (_descriptor->max_definition_level() > 0) {
        batch->_def_levels = record_reader.def_levels();
    }
    if (_descriptor->max_repetition_level() > 0) {
        batch->_rep_levels = record_reader.rep_levels();
    }
    batch->_read_dense_for_nullable = record_reader.read_dense_for_nullable();
    batch->_values_written = record_reader.values_written();

    // 固定宽度类型：values buffer 指针直接可用
    if (!batch->is_binary_value()) {
        batch->_fixed_values = record_reader.values();
        return Status::OK();
    }

    // Binary 类型：必须通过 GetBuilderChunks() 获取所有权。
    // GetBuilderChunks() 会转移 Arrow builder 所有权并 reset builder，
    // 所以只能调用一次——这里就是那一次。
    RETURN_IF_ERROR(get_binary_chunks(_name, record_reader, &batch->_binary_chunks));
    // 从 chunks 重新计算 values_written（因为二进制值的计数方式不同）
    batch->_values_written = 0;
    for (const auto& chunk : batch->_binary_chunks) {
        if (chunk == nullptr) {
            return Status::Corruption(
                    "Parquet binary record reader returned null chunk for column {}", _name);
        }
        batch->_values_written += chunk->length();
    }
    return Status::OK();
}

// 将 batch 中的值写入目标 Doris Column。
//
// 数据准备阶段（DecodedColumnView 填充前）：
//   根据物理存储格式将值准备为 DataTypeSerde 可消费的形式：
//   - FLOAT16: binary → half_to_float → float_values
//   - Binary 类型: Arrow chunks → StringRef[]
//   - 固定宽度 dense nullable: Arrow 紧凑值 → 间隔排列的 spaced_values
//   - 固定宽度非 dense: 直接使用 batch._fixed_values 指针
//
// 物化阶段（DataTypeSerde::read_column_from_decoded_values）：
//   根据 type_descriptor 的信息（decimal precision/scale、timestamp unit、timezone 等）
//   将原始 bytes 转换为 Doris 的最终类型表示。
Status ParquetLeafReader::append_values(const ParquetLeafBatch& batch, int64_t row_count,
                                        const NullMap* null_map, MutableColumnPtr& column) const {
    std::vector<StringRef> binary_values;
    std::vector<uint8_t> spaced_values;
    std::vector<float> float_values;
    DecodedColumnView view;
    view.value_kind = batch._value_kind;
    view.time_unit = decoded_time_unit(_type_descriptor.time_unit);
    view.row_count = row_count;
    view.decimal_precision = _type_descriptor.decimal_precision;
    view.decimal_scale = _type_descriptor.decimal_scale;
    view.fixed_length = _type_descriptor.fixed_length;
    view.timestamp_is_adjusted_to_utc = _type_descriptor.timestamp_is_adjusted_to_utc;
    view.timezone = _timezone;
    view.enable_strict_mode = _enable_strict_mode;
    view.null_map = null_map == nullptr || null_map->empty() ? nullptr : null_map->data();
    const bool read_dense_for_nullable = batch._read_dense_for_nullable && view.null_map != nullptr;

    // 数据准备：根据物理存储格式填充 view
    if (_type_descriptor.extra_type_info == ParquetExtraTypeInfo::FLOAT16) {
        // FLOAT16: FIXED_LEN_BYTE_ARRAY(2) → half_to_float → float 向量
        RETURN_IF_ERROR(build_binary_values(_name, batch._binary_chunks, row_count, null_map,
                                            read_dense_for_nullable, &binary_values));
        RETURN_IF_ERROR(build_float16_values(_name, _type_descriptor, binary_values, row_count,
                                             &float_values));
        view.value_kind = DecodedValueKind::FLOAT;
        view.values = reinterpret_cast<const uint8_t*>(float_values.data());
    } else if (batch.is_binary_value()) {
        // STRING / DECIMAL_BYTE_ARRAY / ENUM / JSON 等
        RETURN_IF_ERROR(build_binary_values(_name, batch._binary_chunks, row_count, null_map,
                                            read_dense_for_nullable, &binary_values));
        view.binary_values = &binary_values;
    } else if (read_dense_for_nullable) {
        // 固定宽度 + dense nullable: 需要展开为间隔排列
        RETURN_IF_ERROR(build_spaced_fixed_values(batch, row_count, null_map, &spaced_values));
        view.values = spaced_values.data();
    } else {
        // 固定宽度 + 非 nullable 或 非 dense: values 指针直接可用
        view.values = batch._fixed_values;
    }

    {
        SCOPED_TIMER(_profile.materialization_time);
        // 通过 DataTypeSerde 完成类型感知的值写入。
        // 对于 nullable 类型，serde 会直接写入 null_map + nested_column。
        // 对于非 nullable 类型（嵌套场景），当前实现临时走 ColumnNullable 兼容路径。
        if (!_type->is_nullable()) {
            if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
                nullable_column != nullptr) {
                auto& nested_column = nullable_column->get_nested_column();
                auto& tmp_null_map = nullable_column->get_null_map_data();
                const auto old_nested_size = nested_column.size();
                const auto old_null_map_size = tmp_null_map.size();
                auto st = _type->get_serde()->read_column_from_decoded_values(nested_column, view);
                if (!st.ok()) {
                    nested_column.resize(old_nested_size);
                    return st;
                }
                tmp_null_map.resize(old_null_map_size + nested_column.size() - old_nested_size);
                memset(tmp_null_map.data() + old_null_map_size, 0,
                       tmp_null_map.size() - old_null_map_size);
            } else {
                RETURN_IF_ERROR(_type->get_serde()->read_column_from_decoded_values(*column, view));
            }
        } else {
            RETURN_IF_ERROR(_type->get_serde()->read_column_from_decoded_values(*column, view));
        }
    }
    return Status::OK();
}

// 判断当前 value_kind 是否为 binary 类型（需要走 Arrow chunks 路径而非 fixed_values 指针）。
bool ParquetLeafBatch::is_binary_value() const {
    return _value_kind == DecodedValueKind::BINARY || _value_kind == DecodedValueKind::FIXED_BINARY;
}

// 为 dense nullable 模式构建间隔排列的固定宽度值数组。
//
// Arrow RecordReader 在 read_dense_for_nullable 模式下只写非 NULL 值（紧凑排列，
// 不包含 NULL 行的占位），本函数按 null_map 将紧凑值展开：
//   - NULL 行：对应位置保留为 0（不会被 read_column_from_decoded_values 读取）
//   - 非 NULL 行：从 compact buffer 中取下一个值写入对应位置
//
// 例如：null_map = [0,1,0,1,0]，values = [v0,v1,v2]
//         展开后（逻辑上）= [v0, -, v1, -, v2]
Status ParquetLeafReader::build_spaced_fixed_values(const ParquetLeafBatch& batch,
                                                    int64_t row_count, const NullMap* null_map,
                                                    std::vector<uint8_t>* spaced_values) const {
    DORIS_CHECK(null_map != nullptr);
    DORIS_CHECK(spaced_values != nullptr);
    size_t value_size = 0;
    RETURN_IF_ERROR(decoded_fixed_value_size(_name, batch._value_kind, &value_size));
    spaced_values->resize(static_cast<size_t>(row_count) * value_size);
    const auto non_null_count = static_cast<int64_t>(simd::count_zero_num(
            reinterpret_cast<const int8_t*>(null_map->data()), null_map->size()));
    // 完整性校验：紧凑值数量必须等于非 NULL 行数
    if (batch._values_written != non_null_count) {
        return Status::Corruption(
                "Invalid dense nullable parquet values for column {}: values={}, records={}, "
                "nulls={}",
                _name, batch._values_written, row_count, row_count - non_null_count);
    }
    auto* dst = spaced_values->data();
    int64_t value_idx = 0;
    for (int64_t record_idx = 0; record_idx < row_count; ++record_idx) {
        if ((*null_map)[record_idx] != 0) {
            continue; // NULL 行：跳过，对应位置保持为 0
        }
        // 非 NULL 行：从紧凑 buffer 中取出下一个值，按 value_size 拷贝到对应行偏移
        std::memcpy(dst + static_cast<size_t>(record_idx) * value_size,
                    batch._fixed_values + static_cast<size_t>(value_idx) * value_size, value_size);
        ++value_idx;
    }
    return Status::OK();
}

ParquetLeafReader::ParquetLeafReader(
        const ::parquet::ColumnDescriptor* descriptor, ParquetTypeDescriptor type_descriptor,
        DataTypePtr type, std::string name,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader,
        ParquetColumnReaderProfile profile, const cctz::time_zone* timezone,
        bool enable_strict_mode)
        : _descriptor(descriptor),
          _type_descriptor(type_descriptor),
          _type(std::move(type)),
          _name(std::move(name)),
          _record_reader(std::move(record_reader)),
          _profile(profile),
          _timezone(timezone),
          _enable_strict_mode(enable_strict_mode) {}

// 从 Arrow RecordReader 读取 batch_rows 行，并将结果捕获到 ParquetLeafBatch 中。
//
// 步骤：
// 1. Reset + Reserve: 准备 RecordReader 的内部缓冲区
// 2. ReadRecords(batch_rows): 触发一次 data page 读取和解码（Dremel levels + values）
// 3. collect_batch(): 将解码结果快照到 ParquetLeafBatch
Status ParquetLeafReader::read_batch(int64_t batch_rows, ParquetLeafBatch* batch,
                                     int64_t* rows_read) const {
    if (batch == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet leaf batch result pointer for column {}",
                                       _name);
    }
    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }

    try {
        _record_reader->Reset();
        _record_reader->Reserve(batch_rows);
        {
            SCOPED_TIMER(_profile.arrow_read_records_time);
            // ReadRecords 返回实际读到的记录数，可能小于 batch_rows（到达 column chunk 末尾）
            *rows_read = _record_reader->ReadRecords(batch_rows);
        }
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to read parquet records for column {}: {}", _name,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to read parquet records for column {}: {}", _name,
                                     e.what());
    }
    if (*rows_read < 0 || *rows_read > batch_rows) {
        return Status::Corruption("Invalid parquet record read result for column {}: {}", _name,
                                  *rows_read);
    }
    return collect_batch(*_record_reader, batch);
}

// 根据 batch 中的 definition levels 构建 Doris NullMap。
//
// 规则：def_level == max_definition_level → 非 NULL（null_map=0），否则为 NULL（null_map=1）。
// 如果该列没有 optional/repeated 祖先（max_definition_level == 0），则所有值都非 NULL，
// 直接返回 OK（不设置 null_map），由调用方按无 NULL 处理。
Status ParquetLeafReader::build_null_map(const ParquetLeafBatch& batch, int64_t records_read,
                                         NullMap* null_map) const {
    // 无 optional 祖先 → 所有值都非 NULL，不需要 null_map
    if (_descriptor->max_definition_level() == 0) {
        return Status::OK();
    }
    auto* def_levels = batch.def_levels();
    if (def_levels == nullptr && records_read > 0) {
        return Status::Corruption(
                "Parquet record reader returned null definition levels for nullable column {}",
                _name);
    }
    const int16_t max_definition_level = _descriptor->max_definition_level();
    null_map->resize(records_read);
    auto* __restrict dst = null_map->data();
    const auto* __restrict src = def_levels;
    for (int64_t record_idx = 0; record_idx < records_read; ++record_idx) {
        dst[record_idx] = src[record_idx] != max_definition_level;
    }
    return Status::OK();
}

// 嵌套叶子的一步式读取：read levels + values → 解析 value layout → 物化 values_column。
//
// 这是 ParquetLeafReader 中最复杂的函数。它处理 Arrow RecordReader 在不同场景下
// 按不同方式写入 level/value 的复杂情况。
//
// 整体流程：
//
// 1. 调用 read_batch() 获取原始的 level/value 数据。
//
// 2. 将 def/rep levels 拷贝到 batch 中（因为 RecordReader 的 level 数组可能被后续读取覆盖）。
//
// 3. 解析 value layout — Arrow RecordReader 在不同场景下按不同粒度和方式写入 value：
//    - LEVELS:            value 与 level 一一对应（每个 level slot 恰好有一个 value）
//    - VALUE_SLOTS:       value 数量 == 满足 value_slot_definition_level 的 slot 数
//    - LEAF_VALUES:       value 数量 == def_level == max_definition_level 的 slot 数（真正的叶值）
//    - PAYLOAD_VALUE_SLOTS: value 数量 == 降级后的 payload_slot_definition_level slot 数
//      这种降级发生在 Arrow 为 NULL 祖先写入 value placeholder 时（见下文）。
//
// 4. 构建 value_indices[] 映射：level_idx → value buffer 中的位置（-1 表示该 slot 无 value）。
//
// 5. 构建 value_nulls[]：标记每个 value 是否为 NULL。
//
// 6. 调用 append_values() 物化 values_column（非 nullable 的基本类型）。
//
// 关于 PAYLOAD_VALUE_SLOTS 降级（count_value_slots 的 while 循环）：
//   Arrow 的 RecordReader 有时会为不满足 Doris 物化阈值的 NULL 祖先写入 value placeholder。
//   例如 MAP value 在 def_level 不足时，Arrow 仍可能分配一个 value slot 但写入占位值。
//   此时 values_written > value_slot_count（按标准 threshold 计算的 slot 数）。
//   代码尝试逐步降低 payload_slot_definition_level，直到找到匹配 value 数目的 threshold，
//   确保 value_indices 映射和 values_written 对齐，不会把占位值错配给真实 slot。
Status ParquetLeafReader::read_nested_batch(int64_t batch_rows, int16_t value_slot_definition_level,
                                            ParquetNestedScalarBatch* batch,
                                            int16_t value_slot_repetition_level) const {
    if (batch == nullptr) {
        return Status::InvalidArgument("Nested scalar batch is null for column {}", _name);
    }
    *batch = ParquetNestedScalarBatch();
    batch->value_slot_definition_level = value_slot_definition_level;
    batch->value_slot_repetition_level = value_slot_repetition_level;

    ParquetLeafBatch leaf_batch;
    RETURN_IF_ERROR(read_batch(batch_rows, &leaf_batch, &batch->records_read));
    if (_type->is_nullable() && leaf_batch.read_dense_for_nullable()) {
        return Status::NotSupported(
                "Dense nullable parquet nested reader is not supported for column {}", _name);
    }
    batch->levels_written = leaf_batch.consumed_level_count();
    const int64_t values_written = leaf_batch.values_written();
    if (batch->levels_written > leaf_batch.decoded_level_count()) {
        return Status::Corruption(
                "Invalid nested parquet level position for column {}: position={}, levels={}",
                _name, batch->levels_written, leaf_batch.decoded_level_count());
    }
    if (batch->levels_written == 0 && batch->records_read > 0 &&
        values_written == batch->records_read && _descriptor->max_definition_level() == 0 &&
        _descriptor->max_repetition_level() == 0) {
        batch->levels_written = batch->records_read;
    }
    if (batch->levels_written < batch->records_read || values_written < 0 ||
        values_written > batch->levels_written) {
        return Status::Corruption(
                "Invalid nested parquet read result for column {}: rows={}, levels={}, values={}",
                _name, batch->records_read, batch->levels_written, values_written);
    }
    if (batch->levels_written == 0) {
        return Status::OK();
    }

    auto* def_levels = leaf_batch.def_levels();
    if (def_levels == nullptr && _descriptor->max_definition_level() > 0) {
        return Status::Corruption(
                "Nested parquet reader returned null definition levels for column {}", _name);
    }
    batch->def_levels.resize(static_cast<size_t>(batch->levels_written));
    if (_descriptor->max_definition_level() == 0 || def_levels == nullptr) {
        std::fill(batch->def_levels.begin(), batch->def_levels.end(),
                  _descriptor->max_definition_level());
    } else {
        std::copy(def_levels, def_levels + batch->levels_written, batch->def_levels.begin());
    }

    auto* rep_levels = leaf_batch.rep_levels();
    if (rep_levels == nullptr && _descriptor->max_repetition_level() > 0) {
        return Status::Corruption(
                "Nested parquet reader returned null repetition levels for column {}", _name);
    }
    batch->rep_levels.resize(static_cast<size_t>(batch->levels_written));
    if (_descriptor->max_repetition_level() == 0 || rep_levels == nullptr) {
        std::fill(batch->rep_levels.begin(), batch->rep_levels.end(), 0);
    } else {
        std::copy(rep_levels, rep_levels + batch->levels_written, batch->rep_levels.begin());
    }

    const int16_t leaf_definition_level = _descriptor->max_definition_level();
    // Arrow's RecordReader may emit value placeholders for null ancestors that are below the
    // Doris materialization threshold. Those slots must still advance the payload value index;
    // otherwise the next defined child level points at the placeholder instead of its real value.
    auto count_value_slots = [&](int16_t slot_definition_level) {
        int64_t slot_count = 0;
        for (int64_t level_idx = 0; level_idx < batch->levels_written; ++level_idx) {
            if (batch->def_levels[level_idx] >= slot_definition_level &&
                batch->rep_levels[level_idx] <= value_slot_repetition_level) {
                ++slot_count;
            }
        }
        return slot_count;
    };

    const int64_t value_slot_count = count_value_slots(value_slot_definition_level);
    int16_t payload_slot_definition_level = value_slot_definition_level;
    int64_t payload_value_slot_count = value_slot_count;
    while (payload_slot_definition_level > 0 && payload_value_slot_count < values_written) {
        --payload_slot_definition_level;
        payload_value_slot_count = count_value_slots(payload_slot_definition_level);
    }

    int64_t leaf_value_count = 0;
    for (int64_t level_idx = 0; level_idx < batch->levels_written; ++level_idx) {
        if (batch->def_levels[level_idx] < value_slot_definition_level ||
            batch->rep_levels[level_idx] > value_slot_repetition_level) {
            continue;
        }
        if (batch->def_levels[level_idx] == leaf_definition_level) {
            ++leaf_value_count;
        }
    }

    enum class ValueLayout { LEVELS, VALUE_SLOTS, LEAF_VALUES, PAYLOAD_VALUE_SLOTS };
    ValueLayout value_layout = ValueLayout::LEAF_VALUES;
    if (values_written == batch->levels_written) {
        value_layout = ValueLayout::LEVELS;
    } else if (values_written == value_slot_count) {
        value_layout = ValueLayout::VALUE_SLOTS;
    } else if (values_written == leaf_value_count) {
        value_layout = ValueLayout::LEAF_VALUES;
    } else if (values_written == payload_value_slot_count) {
        value_layout = ValueLayout::PAYLOAD_VALUE_SLOTS;
    } else {
        return Status::Corruption(
                "Nested parquet reader returned inconsistent value count for column {}: values={}, "
                "levels={}, slots={}, leaf_values={}, payload_slots={}, "
                "payload_slot_definition_level={}",
                _name, values_written, batch->levels_written, value_slot_count, leaf_value_count,
                payload_value_slot_count, payload_slot_definition_level);
    }

    batch->value_indices.resize(static_cast<size_t>(batch->levels_written), -1);
    NullMap value_nulls(static_cast<size_t>(values_written), 1);
    int64_t value_idx = 0;
    const int16_t decoded_slot_definition_level = value_layout == ValueLayout::PAYLOAD_VALUE_SLOTS
                                                          ? payload_slot_definition_level
                                                          : value_slot_definition_level;
    for (int64_t level_idx = 0; level_idx < batch->levels_written; ++level_idx) {
        if (batch->def_levels[level_idx] < decoded_slot_definition_level ||
            batch->rep_levels[level_idx] > value_slot_repetition_level) {
            continue;
        }
        const bool has_leaf_value = batch->def_levels[level_idx] == leaf_definition_level;
        int64_t decoded_value_idx = -1;
        if (value_layout == ValueLayout::LEVELS) {
            decoded_value_idx = level_idx;
        } else if (value_layout == ValueLayout::VALUE_SLOTS) {
            decoded_value_idx = value_idx++;
        } else if (value_layout == ValueLayout::PAYLOAD_VALUE_SLOTS) {
            decoded_value_idx = value_idx++;
        } else {
            if (!has_leaf_value) {
                continue;
            }
            decoded_value_idx = value_idx++;
        }
        DORIS_CHECK(decoded_value_idx >= 0);
        DORIS_CHECK(decoded_value_idx < values_written);
        if (has_leaf_value) {
            batch->value_indices[static_cast<size_t>(level_idx)] = decoded_value_idx;
            value_nulls[static_cast<size_t>(decoded_value_idx)] = 0;
        }
    }
    if (value_layout != ValueLayout::LEVELS && value_idx != values_written) {
        return Status::Corruption(
                "Nested parquet reader value cursor stopped early for column {}: values={}, "
                "visited={}",
                _name, values_written, value_idx);
    }

    const auto value_type = remove_nullable(_type);
    batch->values_column = value_type->create_column();
    if (values_written > 0) {
        ParquetLeafReader value_reader(_descriptor, _type_descriptor, value_type, _name,
                                       _record_reader, _profile, _timezone, _enable_strict_mode);
        RETURN_IF_ERROR(value_reader.append_values(leaf_batch, values_written, &value_nulls,
                                                   batch->values_column));
    }
    return Status::OK();
}

} // namespace doris::format::parquet
