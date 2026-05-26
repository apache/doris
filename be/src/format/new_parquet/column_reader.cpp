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
#include "core/column/column_struct.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "format/new_parquet/parquet_column_schema.h"

namespace doris::parquet {
namespace {

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
    StructColumnReader(const ParquetColumnSchema& schema,
                       std::vector<std::unique_ptr<ParquetColumnReader>> children)
            : _field_id(schema.field_id),
              _type(schema.type),
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
    DataTypePtr _type;
    std::string _name;
    std::vector<std::unique_ptr<ParquetColumnReader>> _children;
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

    std::vector<StringRef> binary_values;
    std::vector<std::shared_ptr<::arrow::Array>> binary_chunks;
    DecodedColumnView view;
    view.value_kind = decoded_value_kind(_type_descriptor);
    view.time_unit = decoded_time_unit(_type_descriptor.time_unit);
    view.row_count = *rows_read;
    view.decimal_precision = _type_descriptor.decimal_precision;
    view.decimal_scale = _type_descriptor.decimal_scale;
    view.fixed_length = _type_descriptor.fixed_length;
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    if (view.value_kind == DecodedValueKind::BINARY ||
        view.value_kind == DecodedValueKind::FIXED_BINARY) {
        RETURN_IF_ERROR(get_binary_chunks(*this, *record_reader, &binary_chunks));
        RETURN_IF_ERROR(build_binary_values(*this, binary_chunks, *rows_read, &binary_values));
        view.binary_values = &binary_values;
    } else {
        view.values = record_reader->values();
    }

    RETURN_IF_ERROR(_type->get_serde()->read_column_from_decoded_values(*column, view));
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

    int64_t expected_rows = -1;
    size_t child_idx = 0;
    DCHECK_EQ(assert_cast<ColumnStruct&>(*column).get_columns().size(), _children.size());
    for (auto& child_reader : _children) {
        int64_t child_rows = 0;
        auto child_column =
                assert_cast<ColumnStruct&>(*column).get_column_ptr(child_idx)->assume_mutable();
        RETURN_IF_ERROR(child_reader->read(rows, child_column, &child_rows));
        if (expected_rows < 0) {
            expected_rows = child_rows;
        } else if (child_rows != expected_rows) {
            return Status::Corruption(
                    "Parquet struct children returned different row counts in column {}: {} vs {}",
                    _name, expected_rows, child_rows);
        }
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
    if (descriptor->max_repetition_level() != 0 || descriptor->max_definition_level() > 1) {
        return Status::NotSupported(
                "Current parquet reader only supports RecordReader-backed columns; column {} is "
                "not supported",
                name);
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
        const ParquetColumnSchema& column_schema,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
        return Status::NotSupported(
                "Nullable parquet STRUCT reader is not implemented for column {}",
                column_schema.name);
    }
    std::vector<std::unique_ptr<ParquetColumnReader>> child_readers;
    child_readers.reserve(column_schema.children.size());
    for (const auto& child_schema : column_schema.children) {
        std::unique_ptr<ParquetColumnReader> child_reader;
        RETURN_IF_ERROR(create(*child_schema, &child_reader));
        child_readers.push_back(std::move(child_reader));
    }
    *reader = std::make_unique<StructColumnReader>(column_schema, std::move(child_readers));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create(const ParquetColumnSchema& column_schema,
                                          std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    switch (column_schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return create_scalar_column_reader(column_schema, reader);
    case ParquetColumnSchemaKind::STRUCT:
        return create_struct_column_reader(column_schema, reader);
    case ParquetColumnSchemaKind::LIST:
        return Status::NotSupported("Parquet LIST reader is not implemented for column {}",
                                    column_schema.name);
    case ParquetColumnSchemaKind::MAP:
        return Status::NotSupported("Parquet MAP reader is not implemented for column {}",
                                    column_schema.name);
    }
    return Status::NotSupported("Unsupported parquet column schema kind for column {}",
                                column_schema.name);
}

} // namespace doris::parquet
