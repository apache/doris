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
#include <type_traits>
#include <utility>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_decimal.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/value/vdatetime_value.h"
#include "format/new_parquet/parquet_column_schema.h"

namespace doris::parquet {
namespace {

class PrimitiveColumnReader final : public ParquetColumnReader {
public:
    PrimitiveColumnReader(int file_column_id, const ::parquet::ColumnDescriptor* descriptor,
                          ParquetTypeDescriptor type_descriptor, DataTypePtr type, std::string name,
                          std::shared_ptr<::parquet::internal::RecordReader> record_reader)
            : _file_column_id(file_column_id),
              _parquet_column_ordinal(file_column_id),
              _descriptor(descriptor),
              _type_descriptor(std::move(type_descriptor)),
              _type(std::move(type)),
              _name(std::move(name)),
              _record_reader(std::move(record_reader)) {}

    int file_column_id() const override { return _file_column_id; }
    int parquet_column_ordinal() const override { return _parquet_column_ordinal; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr* column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    const ::parquet::ColumnDescriptor* descriptor() const { return _descriptor; }
    const std::shared_ptr<::parquet::internal::RecordReader>& record_reader() const {
        return _record_reader;
    }

private:
    int _file_column_id = -1;
    int _parquet_column_ordinal = -1;
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
    int parquet_column_ordinal() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr* column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    int _field_id = -1;
    DataTypePtr _type;
    std::string _name;
    std::vector<std::unique_ptr<ParquetColumnReader>> _children;
};

template <typename InsertValue>
Status append_record_reader_values(const PrimitiveColumnReader& column_reader,
                                   ::parquet::internal::RecordReader& record_reader,
                                   int64_t records_read, MutableColumnPtr* result_column,
                                   InsertValue&& insert_value) {
    auto column = column_reader.type()->create_column();
    const auto* values = record_reader.values();
    if (values == nullptr && record_reader.values_written() > 0) {
        return Status::Corruption("Parquet record reader returned null values buffer for column {}",
                                  column_reader.name());
    }

    if (column_reader.descriptor()->max_definition_level() == 0) {
        if (record_reader.values_written() != records_read) {
            return Status::Corruption(
                    "Invalid required parquet record read result for column {}: values={}, "
                    "records={}",
                    column_reader.name(), record_reader.values_written(), records_read);
        }
        for (int64_t value_idx = 0; value_idx < records_read; ++value_idx) {
            RETURN_IF_ERROR(insert_value(*column, values, value_idx));
        }
    } else {
        const int16_t max_definition_level = column_reader.descriptor()->max_definition_level();
        auto* def_levels = record_reader.def_levels();
        if (def_levels == nullptr && records_read > 0) {
            return Status::Corruption(
                    "Parquet record reader returned null definition levels for nullable column {}",
                    column_reader.name());
        }
        if (record_reader.read_dense_for_nullable()) {
            return Status::NotSupported(
                    "Dense nullable parquet record reader is not supported for column {}",
                    column_reader.name());
        }
        if (record_reader.values_written() != records_read) {
            return Status::Corruption(
                    "Invalid nullable parquet record read result for column {}: values={}, "
                    "records={}",
                    column_reader.name(), record_reader.values_written(), records_read);
        }
        for (int64_t record_idx = 0; record_idx < records_read; ++record_idx) {
            if (def_levels[record_idx] == max_definition_level) {
                RETURN_IF_ERROR(insert_value(*column, values, record_idx));
            } else {
                column->insert_data(nullptr, 0);
            }
        }
    }

    *result_column = std::move(column);
    return Status::OK();
}

Status read_records(PrimitiveColumnReader& column_reader, int64_t batch_rows,
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

template <PrimitiveType DorisType, typename ParquetValueType>
Status read_record_physical_column(PrimitiveColumnReader& column_reader, int64_t batch_rows,
                                   MutableColumnPtr* result_column, int64_t* rows_read) {
    ::parquet::internal::RecordReader* record_reader = nullptr;
    RETURN_IF_ERROR(read_records(column_reader, batch_rows, &record_reader, rows_read));
    using DorisCppType = typename PrimitiveTypeTraits<DorisType>::CppType;
    return append_record_reader_values(
            column_reader, *record_reader, *rows_read, result_column,
            [](IColumn& column, const uint8_t* values, int64_t value_idx) {
                const auto* typed_values = reinterpret_cast<const ParquetValueType*>(values);
                DorisCppType doris_value = static_cast<DorisCppType>(typed_values[value_idx]);
                column.insert_data(reinterpret_cast<const char*>(&doris_value),
                                   sizeof(DorisCppType));
                return Status::OK();
            });
}

template <typename ParquetValueType, typename InsertValue>
Status read_record_value_column(PrimitiveColumnReader& column_reader, int64_t batch_rows,
                                MutableColumnPtr* result_column, int64_t* rows_read,
                                InsertValue&& insert_value) {
    ::parquet::internal::RecordReader* record_reader = nullptr;
    RETURN_IF_ERROR(read_records(column_reader, batch_rows, &record_reader, rows_read));
    return append_record_reader_values(
            column_reader, *record_reader, *rows_read, result_column,
            [&](IColumn& column, const uint8_t* values, int64_t value_idx) {
                const auto* typed_values = reinterpret_cast<const ParquetValueType*>(values);
                return insert_value(column, typed_values[value_idx]);
            });
}

Status get_binary_chunks(const PrimitiveColumnReader& column_reader,
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

template <typename InsertValue>
Status append_binary_record_reader_values(const PrimitiveColumnReader& column_reader,
                                          ::parquet::internal::RecordReader& record_reader,
                                          int64_t records_read, MutableColumnPtr* result_column,
                                          InsertValue&& insert_value) {
    std::vector<std::shared_ptr<::arrow::Array>> chunks;
    RETURN_IF_ERROR(get_binary_chunks(column_reader, record_reader, &chunks));
    auto column = column_reader.type()->create_column();
    int64_t appended_rows = 0;
    for (const auto& chunk : chunks) {
        if (chunk == nullptr) {
            return Status::Corruption(
                    "Parquet binary record reader returned null chunk for column {}",
                    column_reader.name());
        }
        for (int64_t row_idx = 0; row_idx < chunk->length(); ++row_idx) {
            if (chunk->IsNull(row_idx)) {
                column->insert_data(nullptr, 0);
            } else {
                RETURN_IF_ERROR(insert_value(*column, *chunk, row_idx));
            }
            ++appended_rows;
        }
    }
    if (appended_rows != records_read) {
        return Status::Corruption(
                "Invalid parquet binary record read result for column {}: rows={}, records={}",
                column_reader.name(), appended_rows, records_read);
    }
    *result_column = std::move(column);
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

Status append_rows(MutableColumnPtr* dst, MutableColumnPtr src) {
    if (!*dst) {
        *dst = std::move(src);
        return Status::OK();
    }
    const size_t rows = src->size();
    for (size_t row_idx = 0; row_idx < rows; ++row_idx) {
        (*dst)->insert_from(*src, row_idx);
    }
    return Status::OK();
}

template <typename InsertValue>
Status read_record_binary_column(PrimitiveColumnReader& column_reader, int64_t batch_rows,
                                 MutableColumnPtr* result_column, int64_t* rows_read,
                                 InsertValue&& insert_value) {
    ::parquet::internal::RecordReader* record_reader = nullptr;
    RETURN_IF_ERROR(read_records(column_reader, batch_rows, &record_reader, rows_read));
    return append_binary_record_reader_values(column_reader, *record_reader, *rows_read,
                                              result_column,
                                              std::forward<InsertValue>(insert_value));
}

template <typename NativeType>
NativeType decode_big_endian_signed_integer(const uint8_t* data, int length) {
    using UnsignedNativeType =
            std::conditional_t<std::is_same_v<NativeType, Int128>, unsigned __int128,
                               std::make_unsigned_t<NativeType>>;
    UnsignedNativeType value = data != nullptr && length > 0 && (data[0] & 0x80) != 0
                                       ? static_cast<UnsignedNativeType>(-1)
                                       : 0;
    for (int i = 0; i < length; ++i) {
        value = static_cast<UnsignedNativeType>((value << 8) | data[i]);
    }
    return static_cast<NativeType>(value);
}

template <PrimitiveType DecimalType, typename NativeType>
Status insert_decimal_value(IColumn& column, NativeType value) {
    using DecimalCppType = typename PrimitiveTypeTraits<DecimalType>::CppType;
    DecimalCppType decimal_value {value};
    column.insert_data(reinterpret_cast<const char*>(&decimal_value), sizeof(DecimalCppType));
    return Status::OK();
}

Status insert_int32_decimal(IColumn& column, int32_t value) {
    return insert_decimal_value<TYPE_DECIMAL128I>(column, static_cast<Int128>(value));
}

Status insert_int64_decimal(IColumn& column, int64_t value) {
    return insert_decimal_value<TYPE_DECIMAL128I>(column, static_cast<Int128>(value));
}

int64_t timestamp_second_mask(const ParquetTypeDescriptor& type_descriptor) {
    if (type_descriptor.time_unit == ParquetTimeUnit::MILLIS) {
        return 1000;
    }
    return 1000000;
}

Status insert_int64_timestamp(IColumn& column, int64_t value,
                              const ParquetTypeDescriptor& type_descriptor) {
    static const cctz::time_zone utc_time_zone = cctz::utc_time_zone();
    const int64_t second_mask = timestamp_second_mask(type_descriptor);
    int64_t epoch_seconds = value / second_mask;
    int64_t sub_second = value % second_mask;
    if (sub_second < 0) {
        sub_second += second_mask;
        --epoch_seconds;
    }
    const int32_t microsecond = static_cast<int32_t>(sub_second * (1000000 / second_mask));
    DateV2Value<DateTimeV2ValueType> datetime_value;
    datetime_value.from_unixtime(epoch_seconds, utc_time_zone);
    datetime_value.set_microsecond(static_cast<uint64_t>(microsecond));
    const auto raw_value = datetime_value.to_date_int_val();
    column.insert_data(reinterpret_cast<const char*>(&raw_value), sizeof(raw_value));
    return Status::OK();
}

Status insert_binary_array_value(IColumn& column, const ::arrow::Array& array, int64_t row_idx) {
    const auto* binary_array = dynamic_cast<const ::arrow::BinaryArray*>(&array);
    if (binary_array == nullptr) {
        return Status::InternalError("Expected Arrow BinaryArray from parquet record reader");
    }
    int32_t length = 0;
    const uint8_t* value = binary_array->GetValue(row_idx, &length);
    column.insert_data(reinterpret_cast<const char*>(value), static_cast<size_t>(length));
    return Status::OK();
}

Status insert_fixed_size_binary_array_value(IColumn& column, const ::arrow::Array& array,
                                            int64_t row_idx, int type_length) {
    const auto* fixed_array = dynamic_cast<const ::arrow::FixedSizeBinaryArray*>(&array);
    if (fixed_array == nullptr) {
        return Status::InternalError(
                "Expected Arrow FixedSizeBinaryArray from parquet record reader");
    }
    column.insert_data(reinterpret_cast<const char*>(fixed_array->GetValue(row_idx)),
                       static_cast<size_t>(type_length));
    return Status::OK();
}

Status insert_decimal_from_binary_array(IColumn& column, const ::arrow::Array& array,
                                        int64_t row_idx) {
    const auto* binary_array = dynamic_cast<const ::arrow::BinaryArray*>(&array);
    if (binary_array == nullptr) {
        return Status::InternalError("Expected Arrow BinaryArray for parquet decimal column");
    }
    int32_t length = 0;
    const uint8_t* value = binary_array->GetValue(row_idx, &length);
    if (length > static_cast<int32_t>(sizeof(Int128))) {
        return Status::NotSupported("Decimal byte array longer than 16 bytes is not supported");
    }
    Int128 decimal_value = decode_big_endian_signed_integer<Int128>(value, length);
    return insert_decimal_value<TYPE_DECIMAL128I>(column, decimal_value);
}

Status insert_decimal_from_fixed_size_binary_array(IColumn& column, const ::arrow::Array& array,
                                                   int64_t row_idx, int type_length) {
    const auto* fixed_array = dynamic_cast<const ::arrow::FixedSizeBinaryArray*>(&array);
    if (fixed_array == nullptr) {
        return Status::InternalError(
                "Expected Arrow FixedSizeBinaryArray for parquet decimal column");
    }
    if (type_length > static_cast<int>(sizeof(Int128))) {
        return Status::NotSupported("Fixed length decimal longer than 16 bytes is not supported");
    }
    Int128 decimal_value =
            decode_big_endian_signed_integer<Int128>(fixed_array->GetValue(row_idx), type_length);
    return insert_decimal_value<TYPE_DECIMAL128I>(column, decimal_value);
}

} // namespace

Status PrimitiveColumnReader::read(int64_t rows, MutableColumnPtr* column, int64_t* rows_read) {
    if (column == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet column read result pointer for column {}",
                                       _name);
    }
    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }
    if (_type_descriptor.is_decimal) {
        switch (_type_descriptor.physical_type) {
        case ::parquet::Type::INT32:
            return read_record_value_column<int32_t>(*this, rows, column, rows_read,
                                                     [](IColumn& column, int32_t value) {
                                                         return insert_int32_decimal(column, value);
                                                     });
        case ::parquet::Type::INT64:
            return read_record_value_column<int64_t>(*this, rows, column, rows_read,
                                                     [](IColumn& column, int64_t value) {
                                                         return insert_int64_decimal(column, value);
                                                     });
        case ::parquet::Type::BYTE_ARRAY:
            return read_record_binary_column(
                    *this, rows, column, rows_read,
                    [](IColumn& column, const ::arrow::Array& array, int64_t row_idx) {
                        return insert_decimal_from_binary_array(column, array, row_idx);
                    });
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return read_record_binary_column(
                    *this, rows, column, rows_read,
                    [this](IColumn& column, const ::arrow::Array& array, int64_t row_idx) {
                        return insert_decimal_from_fixed_size_binary_array(
                                column, array, row_idx, _type_descriptor.fixed_length);
                    });
        default:
            return Status::NotSupported("Unsupported parquet decimal physical type for column {}",
                                        _name);
        }
    }
    if (_type_descriptor.is_timestamp && _type_descriptor.physical_type == ::parquet::Type::INT64) {
        return read_record_value_column<int64_t>(
                *this, rows, column, rows_read, [this](IColumn& column, int64_t value) {
                    return insert_int64_timestamp(column, value, _type_descriptor);
                });
    }
    if (_type_descriptor.is_string_like) {
        if (_type_descriptor.physical_type == ::parquet::Type::BYTE_ARRAY) {
            return read_record_binary_column(
                    *this, rows, column, rows_read,
                    [](IColumn& column, const ::arrow::Array& array, int64_t row_idx) {
                        return insert_binary_array_value(column, array, row_idx);
                    });
        }
        return read_record_binary_column(
                *this, rows, column, rows_read,
                [this](IColumn& column, const ::arrow::Array& array, int64_t row_idx) {
                    return insert_fixed_size_binary_array_value(column, array, row_idx,
                                                                _type_descriptor.fixed_length);
                });
    }

    switch (_type_descriptor.physical_type) {
    case ::parquet::Type::BOOLEAN:
        return read_record_physical_column<TYPE_BOOLEAN, bool>(*this, rows, column, rows_read);
    case ::parquet::Type::INT32:
        return read_record_physical_column<TYPE_INT, int32_t>(*this, rows, column, rows_read);
    case ::parquet::Type::INT64:
        return read_record_physical_column<TYPE_BIGINT, int64_t>(*this, rows, column, rows_read);
    case ::parquet::Type::FLOAT:
        return read_record_physical_column<TYPE_FLOAT, float>(*this, rows, column, rows_read);
    case ::parquet::Type::DOUBLE:
        return read_record_physical_column<TYPE_DOUBLE, double>(*this, rows, column, rows_read);
    default:
        return Status::NotSupported("Unsupported parquet physical type for column {}", _name);
    }
}

Status PrimitiveColumnReader::skip(int64_t rows) {
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

Status StructColumnReader::read(int64_t rows, MutableColumnPtr* column, int64_t* rows_read) {
    if (column == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet struct read result pointer for column {}",
                                       _name);
    }
    if (_children.empty()) {
        auto result_column = _type->create_column();
        result_column->resize(static_cast<size_t>(rows));
        *column = std::move(result_column);
        *rows_read = rows;
        return Status::OK();
    }

    MutableColumns child_columns;
    child_columns.reserve(_children.size());
    int64_t expected_rows = -1;
    for (auto& child_reader : _children) {
        MutableColumnPtr child_column;
        int64_t child_rows = 0;
        RETURN_IF_ERROR(child_reader->read(rows, &child_column, &child_rows));
        if (expected_rows < 0) {
            expected_rows = child_rows;
        } else if (child_rows != expected_rows) {
            return Status::Corruption(
                    "Parquet struct children returned different row counts in column {}: {} vs {}",
                    _name, expected_rows, child_rows);
        }
        child_columns.push_back(std::move(child_column));
    }

    *rows_read = std::max<int64_t>(expected_rows, 0);
    *column = ColumnStruct::create(std::move(child_columns));
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
                                   int64_t batch_rows, MutableColumnPtr* column) {
    if (column == nullptr) {
        return Status::InvalidArgument("Parquet selected read result is null for column {}",
                                       name());
    }
    RETURN_IF_ERROR(sel.verify(selected_rows, batch_rows));

    *column = nullptr;
    const auto ranges = selection_to_ranges(sel, selected_rows);
    int64_t cursor = 0;
    for (const auto& range : ranges) {
        if (range.start < cursor || range.start + range.length > batch_rows) {
            return Status::InvalidArgument("Invalid parquet selection range [{}, {}) for column {}",
                                           range.start, range.start + range.length, name());
        }
        RETURN_IF_ERROR(skip(range.start - cursor));

        MutableColumnPtr range_column;
        int64_t range_rows_read = 0;
        RETURN_IF_ERROR(read(range.length, &range_column, &range_rows_read));
        if (range_rows_read != range.length) {
            return Status::Corruption(
                    "Parquet selected read returned {} rows, expected {} rows for column {}",
                    range_rows_read, range.length, name());
        }
        RETURN_IF_ERROR(append_rows(column, std::move(range_column)));
        cursor = range.start + range.length;
    }
    RETURN_IF_ERROR(skip(batch_rows - cursor));

    if (!*column) {
        *column = type()->create_column();
    }
    return Status::OK();
}

ParquetColumnReaderFactory::ParquetColumnReaderFactory(
        std::shared_ptr<::parquet::RowGroupReader> row_group, int num_leaf_columns)
        : _row_group(std::move(row_group)),
          _record_readers(static_cast<size_t>(num_leaf_columns)) {}

Status ParquetColumnReaderFactory::create_primitive_reader(
        int file_column_id, const ParquetTypeDescriptor& type_descriptor,
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
    *reader = std::make_unique<PrimitiveColumnReader>(file_column_id, descriptor, type_descriptor,
                                                      std::move(type), std::move(name),
                                                      std::move(record_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_primitive(
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
    return create_primitive_reader(column_schema.leaf_column_id, column_schema.type_descriptor,
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

Status ParquetColumnReaderFactory::create_struct(
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
        return create_primitive(column_schema, reader);
    case ParquetColumnSchemaKind::STRUCT:
        return create_struct(column_schema, reader);
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
