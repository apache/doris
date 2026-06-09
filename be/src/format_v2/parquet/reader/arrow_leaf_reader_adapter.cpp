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

#include "format_v2/parquet/reader/arrow_leaf_reader_adapter.h"

#include <arrow/array/array_binary.h>
#include <parquet/api/schema.h>
#include <parquet/column_reader.h>
#include <parquet/exception.h>

#include <algorithm>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/string_ref.h"
#include "format_v2/parquet/reader/nested_column_reader.h"
#include "util/simd/bits.h"

namespace doris::parquet {
namespace {

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

DecodedValueKind decoded_value_kind(const ParquetTypeDescriptor& type_descriptor) {
    switch (type_descriptor.physical_type) {
    case ::parquet::Type::BOOLEAN:
        return DecodedValueKind::BOOL;
    case ::parquet::Type::INT32:
        return DecodedValueKind::INT32;
    case ::parquet::Type::INT64:
        return DecodedValueKind::INT64;
    case ::parquet::Type::FLOAT:
        return DecodedValueKind::FLOAT;
    case ::parquet::Type::DOUBLE:
        return DecodedValueKind::DOUBLE;
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return DecodedValueKind::FIXED_BINARY;
    case ::parquet::Type::BYTE_ARRAY:
    default:
        return DecodedValueKind::BINARY;
    }
}

Status decoded_fixed_value_size(const ArrowLeafReaderContext& context, DecodedValueKind value_kind,
                                size_t* value_size) {
    switch (value_kind) {
    case DecodedValueKind::BOOL:
        *value_size = sizeof(bool);
        return Status::OK();
    case DecodedValueKind::INT32:
        *value_size = sizeof(int32_t);
        return Status::OK();
    case DecodedValueKind::INT64:
        *value_size = sizeof(int64_t);
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
                                       context.column_name());
    }
    return Status::InternalError("Unknown decoded value kind for column {}", context.column_name());
}

Status get_binary_chunks(const ArrowLeafReaderContext& context,
                         ::parquet::internal::RecordReader& record_reader,
                         std::vector<std::shared_ptr<::arrow::Array>>* chunks) {
    auto* binary_reader = dynamic_cast<::parquet::internal::BinaryRecordReader*>(&record_reader);
    if (binary_reader == nullptr) {
        return Status::InternalError("Parquet binary record reader is not available for column {}",
                                     context.column_name());
    }
    *chunks = binary_reader->GetBuilderChunks();
    return Status::OK();
}

Status build_binary_values(const ArrowLeafReaderContext& context,
                           const std::vector<std::shared_ptr<::arrow::Array>>& chunks,
                           int64_t records_read, const NullMap* null_map,
                           bool read_dense_for_nullable, std::vector<StringRef>* binary_values) {
    std::vector<StringRef> compact_values;
    auto* values = read_dense_for_nullable ? &compact_values : binary_values;
    values->reserve(records_read);
    for (const auto& chunk : chunks) {
        if (chunk == nullptr) {
            return Status::Corruption(
                    "Parquet binary record reader returned null chunk for column {}",
                    context.column_name());
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
                                         context.column_name());
        }
    }
    if (read_dense_for_nullable) {
        if (null_map == nullptr || null_map->size() != static_cast<size_t>(records_read)) {
            return Status::Corruption(
                    "Invalid dense nullable parquet null map for column {}: rows={}, null_map={}",
                    context.column_name(), records_read,
                    null_map == nullptr ? 0 : null_map->size());
        }
        const int64_t non_null_count = static_cast<int64_t>(simd::count_zero_num(
                reinterpret_cast<const int8_t*>(null_map->data()), null_map->size()));
        if (compact_values.size() != static_cast<size_t>(non_null_count)) {
            return Status::Corruption(
                    "Invalid dense nullable parquet binary values for column {}: values={}, "
                    "records={}, nulls={}",
                    context.column_name(), compact_values.size(), records_read,
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
                context.column_name(), binary_values->size(), records_read);
    }
    return Status::OK();
}

} // namespace

Status read_leaf_records(const ArrowLeafReaderContext& context, int64_t batch_rows,
                         ::parquet::internal::RecordReader** record_reader, int64_t* rows_read) {
    if (context.record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     context.column_name());
    }

    try {
        context.record_reader->Reset();
        context.record_reader->Reserve(batch_rows);
        *rows_read = context.record_reader->ReadRecords(batch_rows);
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to read parquet records for column {}: {}",
                                  context.column_name(), e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to read parquet records for column {}: {}",
                                     context.column_name(), e.what());
    }
    if (*rows_read < 0 || *rows_read > batch_rows) {
        return Status::Corruption("Invalid parquet record read result for column {}: {}",
                                  context.column_name(), *rows_read);
    }
    *record_reader = context.record_reader.get();
    return Status::OK();
}

Status build_leaf_null_map(const ArrowLeafReaderContext& context,
                           ::parquet::internal::RecordReader& record_reader, int64_t records_read,
                           NullMap* null_map) {
    if (context.descriptor->max_definition_level() == 0) {
        return Status::OK();
    }
    auto* def_levels = record_reader.def_levels();
    if (def_levels == nullptr && records_read > 0) {
        return Status::Corruption(
                "Parquet record reader returned null definition levels for nullable column {}",
                context.column_name());
    }
    const int16_t max_definition_level = context.descriptor->max_definition_level();
    null_map->resize(records_read);
    auto* __restrict dst = null_map->data();
    const auto* __restrict src = def_levels;
    for (int64_t record_idx = 0; record_idx < records_read; ++record_idx) {
        dst[record_idx] = src[record_idx] != max_definition_level;
    }
    return Status::OK();
}

Status append_leaf_values(const ArrowLeafReaderContext& context,
                          ::parquet::internal::RecordReader& record_reader, int64_t row_count,
                          const NullMap* null_map, MutableColumnPtr& column) {
    std::vector<StringRef> binary_values;
    std::vector<std::shared_ptr<::arrow::Array>> binary_chunks;
    std::vector<uint8_t> spaced_values;
    DecodedColumnView view;
    view.value_kind = decoded_value_kind(context.type_descriptor);
    view.time_unit = decoded_time_unit(context.type_descriptor.time_unit);
    view.row_count = row_count;
    view.decimal_precision = context.type_descriptor.decimal_precision;
    view.decimal_scale = context.type_descriptor.decimal_scale;
    view.fixed_length = context.type_descriptor.fixed_length;
    view.null_map = null_map == nullptr || null_map->empty() ? nullptr : null_map->data();
    const bool read_dense_for_nullable =
            record_reader.read_dense_for_nullable() && view.null_map != nullptr;
    if (view.value_kind == DecodedValueKind::BINARY ||
        view.value_kind == DecodedValueKind::FIXED_BINARY) {
        RETURN_IF_ERROR(get_binary_chunks(context, record_reader, &binary_chunks));
        RETURN_IF_ERROR(build_binary_values(context, binary_chunks, row_count, null_map,
                                            read_dense_for_nullable, &binary_values));
        view.binary_values = &binary_values;
    } else if (read_dense_for_nullable) {
        size_t value_size = 0;
        RETURN_IF_ERROR(decoded_fixed_value_size(context, view.value_kind, &value_size));
        spaced_values.resize(static_cast<size_t>(row_count) * value_size);
        const auto values_written = record_reader.values_written();
        const auto non_null_count = static_cast<int64_t>(simd::count_zero_num(
                reinterpret_cast<const int8_t*>(null_map->data()), null_map->size()));
        if (values_written != non_null_count) {
            return Status::Corruption(
                    "Invalid dense nullable parquet values for column {}: values={}, records={}, "
                    "nulls={}",
                    context.column_name(), values_written, row_count, row_count - non_null_count);
        }
        const auto* src = record_reader.values();
        auto* dst = spaced_values.data();
        int64_t value_idx = 0;
        for (int64_t record_idx = 0; record_idx < row_count; ++record_idx) {
            if ((*null_map)[record_idx] != 0) {
                continue;
            }
            std::memcpy(dst + static_cast<size_t>(record_idx) * value_size,
                        src + static_cast<size_t>(value_idx) * value_size, value_size);
            ++value_idx;
        }
        view.values = spaced_values.data();
    } else {
        view.values = record_reader.values();
    }

    RETURN_IF_ERROR(
            context.data_type()->get_serde()->read_column_from_decoded_values(*column, view));
    return Status::OK();
}

Status read_nested_leaf_batch(const ArrowLeafReaderContext& context, int64_t batch_rows,
                              int16_t value_slot_definition_level, NestedScalarBatch* batch,
                              int16_t value_slot_repetition_level) {
    if (batch == nullptr) {
        return Status::InvalidArgument("Nested scalar batch is null for column {}",
                                       context.column_name());
    }
    *batch = NestedScalarBatch();
    batch->value_slot_definition_level = value_slot_definition_level;
    batch->value_slot_repetition_level = value_slot_repetition_level;

    ::parquet::internal::RecordReader* record_reader = nullptr;
    RETURN_IF_ERROR(read_leaf_records(context, batch_rows, &record_reader, &batch->records_read));
    if (context.data_type()->is_nullable() && record_reader->read_dense_for_nullable()) {
        return Status::NotSupported(
                "Dense nullable parquet nested reader is not supported for column {}",
                context.column_name());
    }
    batch->levels_written = record_reader->levels_position();
    const int64_t values_written = record_reader->values_written();
    if (batch->levels_written > record_reader->levels_written()) {
        return Status::Corruption(
                "Invalid nested parquet level position for column {}: position={}, levels={}",
                context.column_name(), batch->levels_written, record_reader->levels_written());
    }
    if (batch->levels_written == 0 && batch->records_read > 0 &&
        values_written == batch->records_read && context.descriptor->max_definition_level() == 0 &&
        context.descriptor->max_repetition_level() == 0) {
        batch->levels_written = batch->records_read;
    }
    if (batch->levels_written < batch->records_read || values_written < 0 ||
        values_written > batch->levels_written) {
        return Status::Corruption(
                "Invalid nested parquet read result for column {}: rows={}, levels={}, values={}",
                context.column_name(), batch->records_read, batch->levels_written, values_written);
    }
    if (batch->levels_written == 0) {
        return Status::OK();
    }

    auto* def_levels = record_reader->def_levels();
    if (def_levels == nullptr && context.descriptor->max_definition_level() > 0) {
        return Status::Corruption(
                "Nested parquet reader returned null definition levels for column {}",
                context.column_name());
    }
    batch->def_levels.resize(static_cast<size_t>(batch->levels_written));
    if (context.descriptor->max_definition_level() == 0 || def_levels == nullptr) {
        std::fill(batch->def_levels.begin(), batch->def_levels.end(),
                  context.descriptor->max_definition_level());
    } else {
        std::copy(def_levels, def_levels + batch->levels_written, batch->def_levels.begin());
    }

    auto* rep_levels = record_reader->rep_levels();
    if (rep_levels == nullptr && context.descriptor->max_repetition_level() > 0) {
        return Status::Corruption(
                "Nested parquet reader returned null repetition levels for column {}",
                context.column_name());
    }
    batch->rep_levels.resize(static_cast<size_t>(batch->levels_written));
    if (context.descriptor->max_repetition_level() == 0 || rep_levels == nullptr) {
        std::fill(batch->rep_levels.begin(), batch->rep_levels.end(), 0);
    } else {
        std::copy(rep_levels, rep_levels + batch->levels_written, batch->rep_levels.begin());
    }

    batch->value_indices.resize(static_cast<size_t>(batch->levels_written), -1);
    int64_t value_idx = 0;
    const bool dense_value_slots = values_written == batch->levels_written;
    for (int64_t level_idx = 0; level_idx < batch->levels_written; ++level_idx) {
        if (batch->def_levels[level_idx] < value_slot_definition_level ||
            batch->rep_levels[level_idx] > value_slot_repetition_level) {
            continue;
        }
        if (dense_value_slots) {
            batch->value_indices[static_cast<size_t>(level_idx)] = level_idx;
        } else {
            if (value_idx >= values_written) {
                return Status::Corruption(
                        "Nested parquet reader returned fewer values than definition levels for "
                        "column {}",
                        context.column_name());
            }
            batch->value_indices[static_cast<size_t>(level_idx)] = value_idx++;
        }
    }

    const auto value_type = remove_nullable(context.data_type());
    batch->values_column = value_type->create_column();
    if (values_written > 0) {
        ArrowLeafReaderContext value_context = context;
        value_context.type = value_type;
        RETURN_IF_ERROR(append_leaf_values(value_context, *record_reader, values_written, nullptr,
                                           batch->values_column));
    }
    return Status::OK();
}

} // namespace doris::parquet
