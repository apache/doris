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

#include "format/new_parquet/column_reader.h"

#include <arrow/array/array_binary.h>
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/reader/file_reader.h"

namespace doris::parquet {
namespace {

constexpr int64_t NESTED_READ_BATCH_ROWS = 4096;

struct NestedScalarBatch {
    int64_t records_read = 0;
    int64_t levels_written = 0;
    int64_t values_written = 0;
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    std::vector<int64_t> value_indices;
    MutableColumnPtr values_column;

    bool empty() const { return levels_written == 0; }
};

struct NestedScalarOverflow {
    NestedScalarBatch batch;

    bool empty() const { return batch.empty(); }
    void clear() { batch = NestedScalarBatch(); }
};

class ScalarColumnReader final : public ParquetColumnReader {
public:
    ScalarColumnReader(int parquet_leaf_column_id, const ::parquet::ColumnDescriptor* descriptor,
                       ParquetTypeDescriptor type_descriptor, DataTypePtr type, std::string name,
                       std::shared_ptr<::parquet::internal::RecordReader> record_reader)
            : _file_column_id(parquet_leaf_column_id),
              _parquet_leaf_column_id(parquet_leaf_column_id),
              _descriptor(descriptor),
              _type_descriptor(std::move(type_descriptor)),
              _type(std::move(type)),
              _name(std::move(name)),
              _record_reader(std::move(record_reader)) {}

    int file_column_id() const override { return _file_column_id; }
    int parquet_leaf_column_id() const override { return _parquet_leaf_column_id; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    const ::parquet::ColumnDescriptor* descriptor() const { return _descriptor; }
    const std::shared_ptr<::parquet::internal::RecordReader>& record_reader() const {
        return _record_reader;
    }
    const ParquetTypeDescriptor& type_descriptor() const { return _type_descriptor; }

private:
    int _file_column_id = -1;
    int _parquet_leaf_column_id = -1;
    const ::parquet::ColumnDescriptor* _descriptor = nullptr;
    ParquetTypeDescriptor _type_descriptor;
    DataTypePtr _type;
    std::string _name;
    std::shared_ptr<::parquet::internal::RecordReader> _record_reader;
};

class StructColumnReader final : public ParquetColumnReader {
public:
    StructColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                       std::vector<std::unique_ptr<ParquetColumnReader>> children)
            : _field_id(schema.top_level_field_id),
              _nullable_definition_level(schema.nullable_definition_level),
              _type(std::move(type)),
              _name(schema.name),
              _children(std::move(children)) {}

    int file_column_id() const override { return _field_id; }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    int _field_id = -1;
    int16_t _nullable_definition_level = 0;
    DataTypePtr _type;
    std::string _name;
    std::vector<std::unique_ptr<ParquetColumnReader>> _children;
};

class ListColumnReader final : public ParquetColumnReader {
public:
    ListColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                     std::unique_ptr<ParquetColumnReader> element_reader)
            : _field_id(schema.top_level_field_id),
              _nullable_definition_level(schema.nullable_definition_level),
              _repeated_repetition_level(schema.repeated_repetition_level),
              _type(std::move(type)),
              _name(schema.name),
              _element_reader(std::move(element_reader)) {}

    int file_column_id() const override { return _field_id; }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    int _field_id = -1;
    int16_t _nullable_definition_level = 0;
    int16_t _repeated_repetition_level = 0;
    DataTypePtr _type;
    std::string _name;
    std::unique_ptr<ParquetColumnReader> _element_reader;
    NestedScalarOverflow _element_overflow;
};

class RowPositionColumnReader final : public ParquetColumnReader {
public:
    explicit RowPositionColumnReader(int64_t row_group_first_row)
            : _row_group_first_row(row_group_first_row) {}

    int file_column_id() const override {
        return ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
    }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override {
        if (column.get() == nullptr || rows_read == nullptr) {
            return Status::InvalidArgument("Invalid parquet row position read result pointer");
        }
        if (rows < 0) {
            return Status::InvalidArgument("Invalid parquet row position read rows {}", rows);
        }
        auto* vector_column = assert_cast<ColumnInt64*>(column.get());
        auto& data = vector_column->get_data();
        const auto old_size = data.size();
        data.resize(old_size + rows);
        for (int64_t row = 0; row < rows; ++row) {
            data[old_size + row] = _row_group_first_row + _next_row_position + row;
        }
        _next_row_position += rows;
        *rows_read = rows;
        return Status::OK();
    }

    Status skip(int64_t rows) override {
        if (rows <= 0) {
            return Status::OK();
        }
        _next_row_position += rows;
        return Status::OK();
    }

private:
    int64_t _row_group_first_row = 0;
    int64_t _next_row_position = 0;
    DataTypePtr _type = std::make_shared<DataTypeInt64>();
    std::string _name = ParquetColumnReaderFactory::ROW_POSITION_COLUMN_NAME;
};

class MapColumnReader final : public ParquetColumnReader {
public:
    MapColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                    std::unique_ptr<ParquetColumnReader> key_reader,
                    std::unique_ptr<ParquetColumnReader> value_reader)
            : _field_id(schema.top_level_field_id),
              _nullable_definition_level(schema.nullable_definition_level),
              _repeated_repetition_level(schema.repeated_repetition_level),
              _type(std::move(type)),
              _name(schema.name),
              _key_reader(std::move(key_reader)),
              _value_reader(std::move(value_reader)) {}

    int file_column_id() const override { return _field_id; }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    int _field_id = -1;
    int16_t _nullable_definition_level = 0;
    int16_t _repeated_repetition_level = 0;
    DataTypePtr _type;
    std::string _name;
    std::unique_ptr<ParquetColumnReader> _key_reader;
    std::unique_ptr<ParquetColumnReader> _value_reader;
    NestedScalarOverflow _key_overflow;
    NestedScalarOverflow _value_overflow;
};

Status read_records(ScalarColumnReader& column_reader, int64_t batch_rows,
                    ::parquet::internal::RecordReader** record_reader, int64_t* rows_read) {
    auto reader = column_reader.record_reader();
    if (reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     column_reader.name());
    }

    int64_t records_read = 0;
    try {
        reader->Reset();
        reader->Reserve(batch_rows);
        records_read = reader->ReadRecords(batch_rows);
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to read parquet records for column {}: {}",
                                  column_reader.name(), e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to read parquet records for column {}: {}",
                                     column_reader.name(), e.what());
    }
    if (records_read < 0 || records_read > batch_rows) {
        return Status::Corruption("Invalid parquet record read result for column {}: {}",
                                  column_reader.name(), records_read);
    }
    *record_reader = reader.get();
    *rows_read = records_read;
    return Status::OK();
}

struct RowRange {
    int64_t start = 0;
    int64_t length = 0;
};

std::vector<RowRange> selection_to_ranges(const SelectionVector& selection,
                                          uint16_t selected_rows) {
    std::vector<RowRange> ranges;
    if (selected_rows == 0) {
        return ranges;
    }

    int64_t range_start = selection.get_index(0);
    int64_t previous = selection.get_index(0);
    for (uint16_t selection_idx = 1; selection_idx < selected_rows; ++selection_idx) {
        const int64_t current = selection.get_index(selection_idx);
        DCHECK_GT(current, previous);
        if (current == previous + 1) {
            previous = current;
            continue;
        }
        ranges.push_back(RowRange {range_start, previous - range_start + 1});
        range_start = current;
        previous = current;
    }
    ranges.push_back(RowRange {range_start, previous - range_start + 1});
    return ranges;
}

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

Status build_null_map(const ScalarColumnReader& column_reader,
                      ::parquet::internal::RecordReader& record_reader, int64_t records_read,
                      NullMap* null_map) {
    if (column_reader.descriptor()->max_definition_level() == 0) {
        return Status::OK();
    }
    if (record_reader.read_dense_for_nullable()) {
        return Status::NotSupported(
                "Dense nullable parquet record reader is not supported for column {}",
                column_reader.name());
    }
    auto* def_levels = record_reader.def_levels();
    if (def_levels == nullptr && records_read > 0) {
        return Status::Corruption(
                "Parquet record reader returned null definition levels for nullable column {}",
                column_reader.name());
    }
    const int16_t max_definition_level = column_reader.descriptor()->max_definition_level();
    null_map->resize(records_read);
    auto* __restrict dst = null_map->data();
    const auto* __restrict src = def_levels;
    for (int64_t record_idx = 0; record_idx < records_read; ++record_idx) {
        dst[record_idx] = src[record_idx] != max_definition_level;
    }
    return Status::OK();
}

Status get_binary_chunks(const ScalarColumnReader& column_reader,
                         ::parquet::internal::RecordReader& record_reader,
                         std::vector<std::shared_ptr<::arrow::Array>>* chunks) {
    auto* binary_reader = dynamic_cast<::parquet::internal::BinaryRecordReader*>(&record_reader);
    if (binary_reader == nullptr) {
        return Status::InternalError("Parquet binary record reader is not available for column {}",
                                     column_reader.name());
    }
    *chunks = binary_reader->GetBuilderChunks();
    return Status::OK();
}

Status build_binary_values(const ScalarColumnReader& column_reader,
                           const std::vector<std::shared_ptr<::arrow::Array>>& chunks,
                           int64_t records_read, std::vector<StringRef>* binary_values) {
    binary_values->reserve(records_read);
    for (const auto& chunk : chunks) {
        if (chunk == nullptr) {
            return Status::Corruption(
                    "Parquet binary record reader returned null chunk for column {}",
                    column_reader.name());
        }
        if (auto* binary_array = dynamic_cast<::arrow::BinaryArray*>(chunk.get())) {
            for (int64_t row_idx = 0; row_idx < binary_array->length(); ++row_idx) {
                if (binary_array->IsNull(row_idx)) {
                    binary_values->emplace_back(static_cast<const char*>(nullptr), 0);
                    continue;
                }
                int32_t length = 0;
                const uint8_t* value = binary_array->GetValue(row_idx, &length);
                binary_values->emplace_back(reinterpret_cast<const char*>(value), length);
            }
        } else if (auto* fixed_array = dynamic_cast<::arrow::FixedSizeBinaryArray*>(chunk.get())) {
            for (int64_t row_idx = 0; row_idx < fixed_array->length(); ++row_idx) {
                if (fixed_array->IsNull(row_idx)) {
                    binary_values->emplace_back(static_cast<const char*>(nullptr), 0);
                    continue;
                }
                binary_values->emplace_back(
                        reinterpret_cast<const char*>(fixed_array->GetValue(row_idx)),
                        fixed_array->byte_width());
            }
        } else {
            return Status::InternalError("Unexpected Arrow binary array type for column {}",
                                         column_reader.name());
        }
    }
    if (binary_values->size() != static_cast<size_t>(records_read)) {
        return Status::Corruption(
                "Invalid parquet binary record read result for column {}: rows={}, records={}",
                column_reader.name(), binary_values->size(), records_read);
    }
    return Status::OK();
}

Status append_scalar_values(const ScalarColumnReader& column_reader,
                            ::parquet::internal::RecordReader& record_reader, int64_t row_count,
                            const NullMap* null_map, MutableColumnPtr& column) {
    std::vector<StringRef> binary_values;
    std::vector<std::shared_ptr<::arrow::Array>> binary_chunks;
    DecodedColumnView view;
    view.value_kind = decoded_value_kind(column_reader.type_descriptor());
    view.time_unit = decoded_time_unit(column_reader.type_descriptor().time_unit);
    view.row_count = row_count;
    view.decimal_precision = column_reader.type_descriptor().decimal_precision;
    view.decimal_scale = column_reader.type_descriptor().decimal_scale;
    view.fixed_length = column_reader.type_descriptor().fixed_length;
    view.null_map = null_map == nullptr || null_map->empty() ? nullptr : null_map->data();
    if (view.value_kind == DecodedValueKind::BINARY ||
        view.value_kind == DecodedValueKind::FIXED_BINARY) {
        RETURN_IF_ERROR(get_binary_chunks(column_reader, record_reader, &binary_chunks));
        RETURN_IF_ERROR(
                build_binary_values(column_reader, binary_chunks, row_count, &binary_values));
        view.binary_values = &binary_values;
    } else {
        view.values = record_reader.values();
    }

    RETURN_IF_ERROR(
            column_reader.type()->get_serde()->read_column_from_decoded_values(*column, view));
    return Status::OK();
}

Status read_nested_scalar_batch(ScalarColumnReader& column_reader, int64_t batch_rows,
                                int16_t value_slot_definition_level, NestedScalarBatch* batch) {
    if (batch == nullptr) {
        return Status::InvalidArgument("Nested scalar batch is null for column {}",
                                       column_reader.name());
    }
    *batch = NestedScalarBatch();

    ::parquet::internal::RecordReader* record_reader = nullptr;
    RETURN_IF_ERROR(read_records(column_reader, batch_rows, &record_reader, &batch->records_read));
    if (column_reader.type()->is_nullable() && record_reader->read_dense_for_nullable()) {
        return Status::NotSupported(
                "Dense nullable parquet nested reader is not supported for column {}",
                column_reader.name());
    }
    batch->levels_written = record_reader->levels_written();
    batch->values_written = record_reader->values_written();
    if (batch->levels_written < batch->records_read || batch->values_written < 0 ||
        batch->values_written > batch->levels_written) {
        return Status::Corruption(
                "Invalid nested parquet read result for column {}: rows={}, levels={}, values={}",
                column_reader.name(), batch->records_read, batch->levels_written,
                batch->values_written);
    }
    if (batch->levels_written == 0) {
        return Status::OK();
    }

    auto* def_levels = record_reader->def_levels();
    if (def_levels == nullptr && column_reader.descriptor()->max_definition_level() > 0) {
        return Status::Corruption(
                "Nested parquet reader returned null definition levels for column {}",
                column_reader.name());
    }
    batch->def_levels.resize(static_cast<size_t>(batch->levels_written));
    if (def_levels == nullptr) {
        std::fill(batch->def_levels.begin(), batch->def_levels.end(),
                  column_reader.descriptor()->max_definition_level());
    } else {
        std::copy(def_levels, def_levels + batch->levels_written, batch->def_levels.begin());
    }

    auto* rep_levels = record_reader->rep_levels();
    if (rep_levels == nullptr && column_reader.descriptor()->max_repetition_level() > 0) {
        return Status::Corruption(
                "Nested parquet reader returned null repetition levels for column {}",
                column_reader.name());
    }
    batch->rep_levels.resize(static_cast<size_t>(batch->levels_written));
    if (rep_levels == nullptr) {
        std::fill(batch->rep_levels.begin(), batch->rep_levels.end(), 0);
    } else {
        std::copy(rep_levels, rep_levels + batch->levels_written, batch->rep_levels.begin());
    }

    batch->value_indices.resize(static_cast<size_t>(batch->levels_written), -1);
    int64_t value_idx = 0;
    const int16_t max_definition_level = column_reader.descriptor()->max_definition_level();
    NullMap value_null_map;
    for (int64_t level_idx = 0; level_idx < batch->levels_written; ++level_idx) {
        if (batch->def_levels[level_idx] >= value_slot_definition_level) {
            if (value_idx >= batch->values_written) {
                return Status::Corruption(
                        "Nested parquet reader returned fewer values than definition levels for "
                        "column {}",
                        column_reader.name());
            }
            batch->value_indices[level_idx] = value_idx++;
            if (column_reader.type()->is_nullable()) {
                value_null_map.push_back(batch->def_levels[level_idx] != max_definition_level);
            }
        }
    }
    if (value_idx != batch->values_written) {
        return Status::Corruption(
                "Nested parquet reader returned extra values for column {}: consumed={}, values={}",
                column_reader.name(), value_idx, batch->values_written);
    }
    if (column_reader.type()->is_nullable() &&
        value_null_map.size() != static_cast<size_t>(batch->values_written)) {
        return Status::Corruption("Invalid nested parquet null map for column {}",
                                  column_reader.name());
    }

    batch->values_column = column_reader.type()->create_column();
    if (batch->values_written > 0) {
        const NullMap* null_map = value_null_map.empty() ? nullptr : &value_null_map;
        RETURN_IF_ERROR(append_scalar_values(column_reader, *record_reader, batch->values_written,
                                             null_map, batch->values_column));
    }
    return Status::OK();
}

void move_nested_scalar_tail(const NestedScalarBatch& src, int64_t start_level,
                             NestedScalarOverflow* overflow) {
    DORIS_CHECK(overflow != nullptr);
    if (start_level >= src.levels_written) {
        overflow->clear();
        return;
    }

    NestedScalarBatch dst;
    dst.records_read = 0;
    dst.levels_written = src.levels_written - start_level;
    dst.def_levels.assign(src.def_levels.begin() + start_level, src.def_levels.end());
    dst.rep_levels.assign(src.rep_levels.begin() + start_level, src.rep_levels.end());
    dst.value_indices.resize(static_cast<size_t>(dst.levels_written), -1);
    dst.values_column = src.values_column->clone_empty();

    for (int64_t level_idx = start_level; level_idx < src.levels_written; ++level_idx) {
        const int64_t value_idx = src.value_indices[level_idx];
        if (value_idx < 0) {
            continue;
        }
        dst.value_indices[static_cast<size_t>(level_idx - start_level)] = dst.values_written;
        dst.values_column->insert_from(*src.values_column, static_cast<size_t>(value_idx));
        dst.values_written++;
    }
    overflow->batch = std::move(dst);
}

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 MutableColumnPtr& column) {
    const int64_t value_idx = batch.value_indices[level_idx];
    if (value_idx < 0) {
        return Status::Corruption("Nested parquet value is absent for column {}",
                                  column_reader.name());
    }
    column->insert_from(*batch.values_column, static_cast<size_t>(value_idx));
    return Status::OK();
}

ColumnArray* array_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnArray*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnArray*>(column.get());
}

ColumnMap* map_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnMap*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnMap*>(column.get());
}

ColumnStruct* struct_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnStruct*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnStruct*>(column.get());
}

NullMap* null_map_from_nullable_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return &nullable_column->get_null_map_data();
    }
    return nullptr;
}

void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts) {
    offsets.reserve(offsets.size() + entry_counts.size());
    uint64_t current_offset = offsets.empty() ? 0 : offsets.back();
    for (const auto entry_count : entry_counts) {
        current_offset += entry_count;
        offsets.push_back(current_offset);
    }
}

void append_parent_nulls(NullMap* dst, const NullMap& src) {
    if (dst == nullptr) {
        return;
    }
    dst->insert(src.begin(), src.end());
}

template <typename Sink>
Status assemble_repeated_levels(ScalarColumnReader& driver_reader, int16_t repeated_level,
                                int16_t value_slot_definition_level, int64_t rows,
                                NestedScalarOverflow* overflow, Sink& sink, int64_t* rows_read) {
    if (overflow == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid repeated level assembler arguments for column {}",
                                       driver_reader.name());
    }
    *rows_read = 0;
    while (*rows_read < rows) {
        NestedScalarBatch read_batch;
        NestedScalarBatch* batch = nullptr;
        bool from_overflow = false;
        if (!overflow->empty()) {
            batch = &overflow->batch;
            from_overflow = true;
        } else {
            const int64_t batch_rows = std::max<int64_t>(rows - *rows_read, NESTED_READ_BATCH_ROWS);
            RETURN_IF_ERROR(read_nested_scalar_batch(driver_reader, batch_rows,
                                                     value_slot_definition_level, &read_batch));
            if (read_batch.empty()) {
                break;
            }
            batch = &read_batch;
        }
        RETURN_IF_ERROR(sink.start_batch(*batch));

        int64_t level_idx = 0;
        while (level_idx < batch->levels_written) {
            const bool starts_parent = batch->rep_levels[level_idx] < repeated_level;
            if (starts_parent && *rows_read >= rows) {
                move_nested_scalar_tail(*batch, level_idx, overflow);
                return Status::OK();
            }
            if (starts_parent) {
                RETURN_IF_ERROR(sink.start_parent(*batch, level_idx));
                ++*rows_read;
            } else {
                if (*rows_read == 0) {
                    return Status::Corruption(
                            "Repeated parquet stream starts with repeated level for column {}",
                            driver_reader.name());
                }
                RETURN_IF_ERROR(sink.append_repeated(*batch, level_idx));
            }
            ++level_idx;
        }

        if (from_overflow) {
            overflow->clear();
        }
    }
    return Status::OK();
}

} // namespace

Status ScalarColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet column read result pointer for column {}",
                                       _name);
    }
    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }
    ::parquet::internal::RecordReader* record_reader = nullptr;
    RETURN_IF_ERROR(read_records(*this, rows, &record_reader, rows_read));
    if (record_reader->values_written() != *rows_read) {
        return Status::Corruption(
                "Invalid parquet record read result for column {}: values={}, records={}", _name,
                record_reader->values_written(), *rows_read);
    }

    NullMap null_map;
    RETURN_IF_ERROR(build_null_map(*this, *record_reader, *rows_read, &null_map));

    RETURN_IF_ERROR(append_scalar_values(*this, *record_reader, *rows_read, &null_map, column));
    return Status::OK();
}

Status ScalarColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }

    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }
    int64_t skipped_rows = 0;
    try {
        _record_reader->Reset();
        while (skipped_rows < rows) {
            const int64_t skipped = _record_reader->SkipRecords(rows - skipped_rows);
            if (skipped <= 0) {
                return Status::Corruption(
                        "Failed to skip parquet records for column {}: skipped {} of {} rows",
                        _name, skipped_rows, rows);
            }
            skipped_rows += skipped;
        }
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to skip parquet records for column {}: {}", _name,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to skip parquet records for column {}: {}", _name,
                                     e.what());
    }
    return Status::OK();
}

Status StructColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet struct read result pointer for column {}",
                                       _name);
    }
    if (_children.empty()) {
        column->resize(static_cast<size_t>(rows));
        *rows_read = rows;
        return Status::OK();
    }

    auto* struct_column = struct_column_from_output(column);
    DORIS_CHECK(struct_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    DCHECK_EQ(struct_column->get_columns().size(), _children.size());

    std::vector<ScalarColumnReader*> scalar_children;
    scalar_children.reserve(_children.size());
    bool all_scalar_children = true;
    for (const auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        auto* scalar_child = dynamic_cast<ScalarColumnReader*>(child_reader.get());
        if (scalar_child == nullptr) {
            all_scalar_children = false;
            break;
        }
        scalar_children.push_back(scalar_child);
    }
    if (all_scalar_children) {
        std::vector<NestedScalarBatch> child_batches(scalar_children.size());
        int64_t expected_rows = -1;
        for (size_t child_idx = 0; child_idx < scalar_children.size(); ++child_idx) {
            RETURN_IF_ERROR(read_nested_scalar_batch(*scalar_children[child_idx], rows,
                                                     _nullable_definition_level,
                                                     &child_batches[child_idx]));
            if (expected_rows < 0) {
                expected_rows = child_batches[child_idx].records_read;
            } else if (child_batches[child_idx].records_read != expected_rows) {
                return Status::Corruption(
                        "Parquet struct children returned different row counts in column {}: {} "
                        "vs {}",
                        _name, expected_rows, child_batches[child_idx].records_read);
            }
            if (child_batches[child_idx].levels_written != child_batches[child_idx].records_read) {
                return Status::Corruption(
                        "Parquet struct child {} returned repeated levels in column {}",
                        scalar_children[child_idx]->name(), _name);
            }
        }

        if (expected_rows <= 0) {
            *rows_read = 0;
            return Status::OK();
        }

        std::vector<MutableColumnPtr> child_columns;
        child_columns.reserve(scalar_children.size());
        for (size_t child_idx = 0; child_idx < scalar_children.size(); ++child_idx) {
            child_columns.push_back(struct_column->get_column_ptr(child_idx)->assume_mutable());
        }

        NullMap parent_nulls;
        parent_nulls.reserve(static_cast<size_t>(expected_rows));
        for (int64_t row_idx = 0; row_idx < expected_rows; ++row_idx) {
            const bool parent_is_null =
                    child_batches[0].def_levels[row_idx] < _nullable_definition_level;
            parent_nulls.push_back(parent_is_null);
            for (size_t child_idx = 1; child_idx < child_batches.size(); ++child_idx) {
                const bool child_parent_is_null =
                        child_batches[child_idx].def_levels[row_idx] < _nullable_definition_level;
                if (child_parent_is_null != parent_is_null) {
                    return Status::Corruption(
                            "Parquet struct children returned different null parent shape in "
                            "column {}",
                            _name);
                }
            }
            for (size_t child_idx = 0; child_idx < scalar_children.size(); ++child_idx) {
                if (parent_is_null) {
                    child_columns[child_idx]->insert_default();
                } else {
                    if (!scalar_children[child_idx]->type()->is_nullable() &&
                        child_batches[child_idx].def_levels[row_idx] !=
                                scalar_children[child_idx]->descriptor()->max_definition_level()) {
                        return Status::Corruption(
                                "Parquet STRUCT column {} contains null for non-nullable child {}",
                                _name, scalar_children[child_idx]->name());
                    }
                    RETURN_IF_ERROR(append_scalar_batch_value(*scalar_children[child_idx],
                                                              child_batches[child_idx], row_idx,
                                                              child_columns[child_idx]));
                }
            }
        }
        for (size_t child_idx = 0; child_idx < child_columns.size(); ++child_idx) {
            struct_column->get_column_ptr(child_idx) = std::move(child_columns[child_idx]);
        }
        if (parent_null_map == nullptr) {
            for (const auto parent_is_null : parent_nulls) {
                if (parent_is_null) {
                    return Status::Corruption(
                            "Parquet STRUCT column {} contains null for non-nullable struct",
                            _name);
                }
            }
        } else {
            append_parent_nulls(parent_null_map, parent_nulls);
        }
        *rows_read = expected_rows;
        return Status::OK();
    }

    if (parent_null_map != nullptr) {
        return Status::NotSupported(
                "Current parquet nullable STRUCT reader only supports scalar children for column "
                "{}",
                _name);
    }

    int64_t expected_rows = -1;
    size_t child_idx = 0;
    for (auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        int64_t child_rows = 0;
        auto child_column = struct_column->get_column_ptr(child_idx)->assume_mutable();
        RETURN_IF_ERROR(child_reader->read(rows, child_column, &child_rows));
        if (expected_rows < 0) {
            expected_rows = child_rows;
        } else if (child_rows != expected_rows) {
            return Status::Corruption(
                    "Parquet struct children returned different row counts in column {}: {} vs {}",
                    _name, expected_rows, child_rows);
        }
        struct_column->get_column_ptr(child_idx) = std::move(child_column);
        child_idx++;
    }

    *rows_read = std::max<int64_t>(expected_rows, 0);
    return Status::OK();
}

Status StructColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    for (auto& child_reader : _children) {
        RETURN_IF_ERROR(child_reader->skip(rows));
    }
    return Status::OK();
}

Status ListColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet list read result pointer for column {}",
                                       _name);
    }
    if (_element_reader == nullptr) {
        return Status::InternalError("Parquet list element reader is not initialized for column {}",
                                     _name);
    }
    auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get());
    if (element_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar elements for column {}", _name);
    }
    auto* array_column = array_column_from_output(column);
    DORIS_CHECK(array_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto nested_column = array_column->get_data_ptr()->assume_mutable();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    const int16_t element_slot_definition_level = _nullable_definition_level + 1;
    const int16_t element_max_definition_level =
            element_reader->descriptor()->max_definition_level();

    struct ListSink {
        ListColumnReader* self = nullptr;
        ScalarColumnReader* element_reader = nullptr;
        MutableColumnPtr* nested_column = nullptr;
        std::vector<uint64_t>* entry_counts = nullptr;
        NullMap* parent_nulls = nullptr;
        int16_t element_max_definition_level = 0;

        Status start_batch(const NestedScalarBatch&) { return Status::OK(); }

        Status start_parent(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level < self->_nullable_definition_level) {
                if (!self->_type->is_nullable()) {
                    return Status::Corruption(
                            "Parquet LIST column {} contains null for non-nullable list",
                            self->_name);
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(1);
                return Status::OK();
            }
            entry_counts->push_back(0);
            parent_nulls->push_back(0);
            if (def_level == self->_nullable_definition_level) {
                return Status::OK();
            }
            return append_element(batch, level_idx);
        }

        Status append_repeated(const NestedScalarBatch& batch, int64_t level_idx) {
            if (entry_counts->empty()) {
                return Status::Corruption("Invalid repeated LIST level for column {}", self->_name);
            }
            return append_element(batch, level_idx);
        }

        Status append_element(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level == element_max_definition_level) {
                RETURN_IF_ERROR(append_scalar_batch_value(*element_reader, batch, level_idx,
                                                          *nested_column));
            } else {
                if (!element_reader->type()->is_nullable()) {
                    return Status::Corruption(
                            "Parquet LIST column {} contains null for non-nullable element",
                            self->_name);
                }
                (*nested_column)->insert_default();
            }
            ++entry_counts->back();
            return Status::OK();
        }
    };

    ListSink sink {this,          element_reader, &nested_column,
                   &entry_counts, &parent_nulls,  element_max_definition_level};
    RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                             element_slot_definition_level, rows,
                                             &_element_overflow, sink, rows_read));

    array_column->get_data_ptr() = std::move(nested_column);
    append_offsets(array_column->get_offsets(), entry_counts);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

Status ListColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get());
    if (element_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar elements for column {}", _name);
    }
    struct SkipSink {
        Status start_batch(const NestedScalarBatch&) { return Status::OK(); }
        Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }
        Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }
    };
    SkipSink sink;
    int64_t rows_read = 0;
    RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                             _nullable_definition_level + 1, rows,
                                             &_element_overflow, sink, &rows_read));
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet LIST column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

Status MapColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet map read result pointer for column {}",
                                       _name);
    }
    if (_key_reader == nullptr || _value_reader == nullptr) {
        return Status::InternalError("Parquet map child reader is not initialized for column {}",
                                     _name);
    }
    auto* key_reader = dynamic_cast<ScalarColumnReader*>(_key_reader.get());
    auto* value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get());
    if (key_reader == nullptr || value_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet MAP reader only supports scalar key/value for column {}", _name);
    }

    auto* map_column = map_column_from_output(column);
    DORIS_CHECK(map_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto key_column = map_column->get_keys_ptr()->assume_mutable();
    auto value_column = map_column->get_values_ptr()->assume_mutable();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    const int16_t entry_definition_level = _nullable_definition_level + 1;
    const int16_t key_max_definition_level = key_reader->descriptor()->max_definition_level();
    const int16_t value_max_definition_level = value_reader->descriptor()->max_definition_level();

    struct MapSink {
        MapColumnReader* self = nullptr;
        ScalarColumnReader* key_reader = nullptr;
        ScalarColumnReader* value_reader = nullptr;
        MutableColumnPtr* key_column = nullptr;
        MutableColumnPtr* value_column = nullptr;
        std::vector<uint64_t>* entry_counts = nullptr;
        NullMap* parent_nulls = nullptr;
        int16_t key_max_definition_level = 0;
        int16_t value_max_definition_level = 0;

        Status read_value_batch(int64_t batch_rows, NestedScalarBatch* out_value_batch) {
            if (!self->_value_overflow.empty()) {
                *out_value_batch = std::move(self->_value_overflow.batch);
                self->_value_overflow.clear();
                return Status::OK();
            }
            return read_nested_scalar_batch(*value_reader, batch_rows,
                                            self->_nullable_definition_level + 1, out_value_batch);
        }

        Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                        const NestedScalarBatch& candidate_value_batch) {
            if (candidate_value_batch.records_read != key_batch.records_read ||
                candidate_value_batch.levels_written != key_batch.levels_written) {
                return Status::Corruption(
                        "Parquet MAP key/value levels are not aligned for column {}: key rows={}, "
                        "key levels={}, value rows={}, value levels={}",
                        self->_name, key_batch.records_read, key_batch.levels_written,
                        candidate_value_batch.records_read, candidate_value_batch.levels_written);
            }
            for (int64_t level_idx = 0; level_idx < key_batch.levels_written; ++level_idx) {
                if (candidate_value_batch.rep_levels[level_idx] !=
                    key_batch.rep_levels[level_idx]) {
                    return Status::Corruption(
                            "Parquet MAP key/value repetition levels are not aligned for column {}",
                            self->_name);
                }
            }
            return Status::OK();
        }

        Status start_batch(const NestedScalarBatch& key_batch) {
            RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
            RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
            return Status::OK();
        }

        Status start_parent(const NestedScalarBatch& key_batch, int64_t level_idx) {
            const int16_t def_level = key_batch.def_levels[level_idx];
            if (def_level < self->_nullable_definition_level) {
                if (!self->_type->is_nullable()) {
                    return Status::Corruption(
                            "Parquet MAP column {} contains null for non-nullable map",
                            self->_name);
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(1);
                return Status::OK();
            }
            entry_counts->push_back(0);
            parent_nulls->push_back(0);
            if (def_level == self->_nullable_definition_level) {
                return Status::OK();
            }
            return append_entry(key_batch, level_idx);
        }

        Status append_repeated(const NestedScalarBatch& key_batch, int64_t level_idx) {
            if (entry_counts->empty()) {
                return Status::Corruption("Invalid repeated MAP level for column {}", self->_name);
            }
            return append_entry(key_batch, level_idx);
        }

        Status append_entry(const NestedScalarBatch& key_batch, int64_t level_idx) {
            if (key_batch.def_levels[level_idx] != key_max_definition_level) {
                return Status::Corruption("Parquet MAP column {} contains null map key",
                                          self->_name);
            }
            RETURN_IF_ERROR(
                    append_scalar_batch_value(*key_reader, key_batch, level_idx, *key_column));
            if (value_batch.def_levels[level_idx] == value_max_definition_level) {
                RETURN_IF_ERROR(append_scalar_batch_value(*value_reader, value_batch, level_idx,
                                                          *value_column));
            } else {
                if (!value_reader->type()->is_nullable()) {
                    return Status::Corruption(
                            "Parquet MAP column {} contains null for non-nullable value",
                            self->_name);
                }
                (*value_column)->insert_default();
            }
            ++entry_counts->back();
            return Status::OK();
        }

        NestedScalarBatch value_batch;
    };

    MapSink sink;
    sink.self = this;
    sink.key_reader = key_reader;
    sink.value_reader = value_reader;
    sink.key_column = &key_column;
    sink.value_column = &value_column;
    sink.entry_counts = &entry_counts;
    sink.parent_nulls = &parent_nulls;
    sink.key_max_definition_level = key_max_definition_level;
    sink.value_max_definition_level = value_max_definition_level;
    RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                             entry_definition_level, rows, &_key_overflow, sink,
                                             rows_read));
    if (!_key_overflow.empty()) {
        move_nested_scalar_tail(
                sink.value_batch,
                sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                &_value_overflow);
    }

    map_column->get_keys_ptr() = std::move(key_column);
    map_column->get_values_ptr() = std::move(value_column);
    append_offsets(map_column->get_offsets(), entry_counts);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

Status MapColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    auto* key_reader = dynamic_cast<ScalarColumnReader*>(_key_reader.get());
    auto* value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get());
    if (key_reader == nullptr || value_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet MAP reader only supports scalar key/value for column {}", _name);
    }
    struct SkipSink {
        MapColumnReader* self = nullptr;
        ScalarColumnReader* value_reader = nullptr;

        Status read_value_batch(int64_t batch_rows, NestedScalarBatch* out_value_batch) {
            if (!self->_value_overflow.empty()) {
                *out_value_batch = std::move(self->_value_overflow.batch);
                self->_value_overflow.clear();
                return Status::OK();
            }
            return read_nested_scalar_batch(*value_reader, batch_rows,
                                            self->_nullable_definition_level + 1, out_value_batch);
        }

        Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                        const NestedScalarBatch& candidate_value_batch) {
            if (candidate_value_batch.records_read != key_batch.records_read ||
                candidate_value_batch.levels_written != key_batch.levels_written) {
                return Status::Corruption(
                        "Parquet MAP key/value levels are not aligned for column {} while "
                        "skipping",
                        self->_name);
            }
            for (int64_t level_idx = 0; level_idx < key_batch.levels_written; ++level_idx) {
                if (candidate_value_batch.rep_levels[level_idx] !=
                    key_batch.rep_levels[level_idx]) {
                    return Status::Corruption(
                            "Parquet MAP key/value repetition levels are not aligned for column {}",
                            self->_name);
                }
            }
            return Status::OK();
        }

        Status start_batch(const NestedScalarBatch& key_batch) {
            RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
            RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
            return Status::OK();
        }

        Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }

        Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }

        NestedScalarBatch value_batch;
    };
    SkipSink sink;
    sink.self = this;
    sink.value_reader = value_reader;
    int64_t rows_read = 0;
    RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                             _nullable_definition_level + 1, rows, &_key_overflow,
                                             sink, &rows_read));
    if (!_key_overflow.empty()) {
        move_nested_scalar_tail(
                sink.value_batch,
                sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                &_value_overflow);
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet MAP column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

Status ParquetColumnReader::skip(int64_t rows) {
    return Status::NotSupported("Parquet column skip is not implemented, rows={}", rows);
}

Status ParquetColumnReader::select(const SelectionVector& sel, uint16_t selected_rows,
                                   int64_t batch_rows, MutableColumnPtr& column) {
    if (column.get() == nullptr) {
        return Status::InvalidArgument("Parquet selected read result is null for column {}",
                                       name());
    }
    RETURN_IF_ERROR(sel.verify(selected_rows, batch_rows));

    const auto ranges = selection_to_ranges(sel, selected_rows);
    int64_t cursor = 0;
    for (const auto& range : ranges) {
        if (range.start < cursor || range.start + range.length > batch_rows) {
            return Status::InvalidArgument("Invalid parquet selection range [{}, {}) for column {}",
                                           range.start, range.start + range.length, name());
        }
        RETURN_IF_ERROR(skip(range.start - cursor));

        int64_t range_rows_read = 0;
        RETURN_IF_ERROR(read(range.length, column, &range_rows_read));
        if (range_rows_read != range.length) {
            return Status::Corruption(
                    "Parquet selected read returned {} rows, expected {} rows for column {}",
                    range_rows_read, range.length, name());
        }
        cursor = range.start + range.length;
    }
    RETURN_IF_ERROR(skip(batch_rows - cursor));
    return Status::OK();
}

ParquetColumnReaderFactory::ParquetColumnReaderFactory(
        std::shared_ptr<::parquet::RowGroupReader> row_group, int num_leaf_columns)
        : _row_group(std::move(row_group)),
          _record_readers(static_cast<size_t>(num_leaf_columns)) {}

reader::SchemaField ParquetColumnReaderFactory::row_position_schema_field() {
    reader::SchemaField field;
    field.id = ROW_POSITION_COLUMN_ID;
    field.name = ROW_POSITION_COLUMN_NAME;
    field.type = std::make_shared<DataTypeInt64>();
    field.column_type = reader::ColumnType::ROW_NUMBER;
    return field;
}

std::unique_ptr<ParquetColumnReader> ParquetColumnReaderFactory::create_row_position_column_reader(
        int64_t row_group_first_row) const {
    return std::make_unique<RowPositionColumnReader>(row_group_first_row);
}

Status ParquetColumnReaderFactory::create_scalar_reader(
        int parquet_leaf_column_id, const ParquetTypeDescriptor& type_descriptor,
        const ::parquet::ColumnDescriptor* descriptor, DataTypePtr type, std::string name,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (descriptor == nullptr || type == nullptr || record_reader == nullptr) {
        return Status::InvalidArgument("Invalid parquet column reader arguments for column {}",
                                       name);
    }
    *reader = std::make_unique<ScalarColumnReader>(parquet_leaf_column_id, descriptor,
                                                   type_descriptor, std::move(type),
                                                   std::move(name), std::move(record_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_scalar_column_reader(
        const ParquetColumnSchema& column_schema,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.leaf_column_id < 0 ||
        column_schema.leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       column_schema.leaf_column_id, column_schema.name);
    }
    if (!supports_record_reader(column_schema.type_descriptor)) {
        return Status::NotSupported(
                "Current parquet reader only supports primitive columns without repetition; "
                "column {} is not supported",
                column_schema.name);
    }
    if (column_schema.descriptor == nullptr ||
        column_schema.descriptor->max_repetition_level() != 0 ||
        column_schema.descriptor->max_definition_level() > 1) {
        return Status::NotSupported(
                "Current parquet scalar reader only supports flat primitive columns; column {} is "
                "not supported",
                column_schema.name);
    }
    std::shared_ptr<::parquet::internal::RecordReader> record_reader;
    RETURN_IF_ERROR(get_record_reader(column_schema.leaf_column_id, column_schema.descriptor,
                                      column_schema.name, &record_reader));
    return create_scalar_reader(column_schema.leaf_column_id, column_schema.type_descriptor,
                                column_schema.descriptor, column_schema.type, column_schema.name,
                                std::move(record_reader), reader);
}

Status ParquetColumnReaderFactory::create_nested_scalar_column_reader(
        const ParquetColumnSchema& column_schema,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE) {
        return Status::InvalidArgument("Parquet nested scalar reader requires primitive column {}",
                                       column_schema.name);
    }
    if (column_schema.leaf_column_id < 0 ||
        column_schema.leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       column_schema.leaf_column_id, column_schema.name);
    }
    if (!supports_record_reader(column_schema.type_descriptor)) {
        return Status::NotSupported(
                "Current parquet nested scalar reader does not support column {}",
                column_schema.name);
    }
    std::shared_ptr<::parquet::internal::RecordReader> record_reader;
    RETURN_IF_ERROR(get_record_reader(column_schema.leaf_column_id, column_schema.descriptor,
                                      column_schema.name, &record_reader));
    return create_scalar_reader(column_schema.leaf_column_id, column_schema.type_descriptor,
                                column_schema.descriptor, column_schema.type, column_schema.name,
                                std::move(record_reader), reader);
}

Status ParquetColumnReaderFactory::get_record_reader(
        int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor, const std::string& name,
        std::shared_ptr<::parquet::internal::RecordReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (_row_group == nullptr) {
        return Status::InternalError("Parquet row group reader is not initialized for column {}",
                                     name);
    }
    if (leaf_column_id < 0 || leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       leaf_column_id, name);
    }
    if (descriptor == nullptr) {
        return Status::InvalidArgument("Parquet column descriptor is null for column {}", name);
    }
    if (_record_readers[leaf_column_id] == nullptr) {
        try {
            _record_readers[leaf_column_id] =
                    _row_group->RecordReader(leaf_column_id, /*read_dictionary=*/false);
        } catch (const ::parquet::ParquetException& e) {
            return Status::Corruption("Failed to create parquet record reader for column {}: {}",
                                      name, e.what());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to create parquet record reader for column {}: {}",
                                         name, e.what());
        }
    }
    if (_record_readers[leaf_column_id] == nullptr) {
        return Status::Corruption("Failed to create parquet record reader for column {}", name);
    }
    *reader = _record_readers[leaf_column_id];
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_struct_column_reader(
        const ParquetColumnSchema& column_schema, const reader::FieldProjection* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    std::vector<std::unique_ptr<ParquetColumnReader>> child_readers;
    child_readers.reserve(column_schema.children.size());
    DataTypes projected_child_types;
    Strings projected_child_names;
    for (size_t child_idx = 0; child_idx < column_schema.children.size(); ++child_idx) {
        const auto& child_schema = column_schema.children[child_idx];
        const reader::FieldProjection* child_projection = nullptr;
        if (projection != nullptr && !projection->project_all_children) {
            auto it = std::find_if(projection->children.begin(), projection->children.end(),
                                   [&](const reader::FieldProjection& child) {
                                       return child.file_path == child_schema->file_path;
                                   });
            if (it == projection->children.end()) {
                continue;
            }
            child_projection = &*it;
        }
        std::unique_ptr<ParquetColumnReader> child_reader;
        if (child_schema->kind == ParquetColumnSchemaKind::PRIMITIVE) {
            RETURN_IF_ERROR(create_nested_scalar_column_reader(*child_schema, &child_reader));
        } else {
            RETURN_IF_ERROR(create(*child_schema, child_projection, &child_reader));
        }
        projected_child_types.push_back(child_reader->type());
        projected_child_names.push_back(child_reader->name());
        child_readers.push_back(std::move(child_reader));
    }
    if (child_readers.empty() && !column_schema.children.empty()) {
        return Status::NotSupported("Parquet STRUCT projection for column {} contains no children",
                                    column_schema.name);
    }
    DataTypePtr type = column_schema.type;
    if (projection != nullptr && !projection->project_all_children) {
        type = std::make_shared<DataTypeStruct>(projected_child_types, projected_child_names);
        if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
            type = make_nullable(type);
        }
    }
    *reader = std::make_unique<StructColumnReader>(column_schema, std::move(type),
                                                   std::move(child_readers));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_list_column_reader(
        const ParquetColumnSchema& column_schema, const reader::FieldProjection* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (projection != nullptr && !projection->project_all_children) {
        return Status::NotSupported("Parquet LIST projection is not implemented for column {}",
                                    column_schema.name);
    }
    if (column_schema.children.size() != 1) {
        return Status::NotSupported("Unsupported parquet LIST layout for column {}",
                                    column_schema.name);
    }
    std::unique_ptr<ParquetColumnReader> element_reader;
    RETURN_IF_ERROR(
            create_nested_scalar_column_reader(*column_schema.children[0], &element_reader));
    *reader = std::make_unique<ListColumnReader>(column_schema, column_schema.type,
                                                 std::move(element_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_map_column_reader(
        const ParquetColumnSchema& column_schema, const reader::FieldProjection* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (projection != nullptr && !projection->project_all_children) {
        return Status::NotSupported("Parquet MAP projection is not implemented for column {}",
                                    column_schema.name);
    }
    if (column_schema.children.size() != 1 || column_schema.children[0]->children.size() != 2) {
        return Status::NotSupported("Unsupported parquet MAP layout for column {}",
                                    column_schema.name);
    }
    const auto& key_value_schema = *column_schema.children[0];
    std::unique_ptr<ParquetColumnReader> key_reader;
    RETURN_IF_ERROR(create_nested_scalar_column_reader(*key_value_schema.children[0], &key_reader));
    std::unique_ptr<ParquetColumnReader> value_reader;
    RETURN_IF_ERROR(
            create_nested_scalar_column_reader(*key_value_schema.children[1], &value_reader));
    *reader = std::make_unique<MapColumnReader>(column_schema, column_schema.type,
                                                std::move(key_reader), std::move(value_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create(const ParquetColumnSchema& column_schema,
                                          const reader::FieldProjection* projection,
                                          std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    switch (column_schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return create_scalar_column_reader(column_schema, reader);
    case ParquetColumnSchemaKind::STRUCT:
        return create_struct_column_reader(column_schema, projection, reader);
    case ParquetColumnSchemaKind::LIST:
        return create_list_column_reader(column_schema, projection, reader);
    case ParquetColumnSchemaKind::MAP:
        return create_map_column_reader(column_schema, projection, reader);
    }
    return Status::NotSupported("Unsupported parquet column schema kind for column {}",
                                column_schema.name);
}

} // namespace doris::parquet
