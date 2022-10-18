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

#include "vorc_reader.h"

#include "gutil/strings/substitute.h"
#include "io/file_factory.h"
#include "vec/columns/column_array.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

#define FOR_FLAT_ORC_COLUMNS(M)                            \
    M(TypeIndex::Int8, Int8, orc::LongVectorBatch)         \
    M(TypeIndex::UInt8, UInt8, orc::LongVectorBatch)       \
    M(TypeIndex::Int16, Int16, orc::LongVectorBatch)       \
    M(TypeIndex::UInt16, UInt16, orc::LongVectorBatch)     \
    M(TypeIndex::Int32, Int32, orc::LongVectorBatch)       \
    M(TypeIndex::UInt32, UInt32, orc::LongVectorBatch)     \
    M(TypeIndex::Int64, Int64, orc::LongVectorBatch)       \
    M(TypeIndex::UInt64, UInt64, orc::LongVectorBatch)     \
    M(TypeIndex::Float32, Float32, orc::DoubleVectorBatch) \
    M(TypeIndex::Float64, Float64, orc::DoubleVectorBatch)

void ORCFileInputStream::read(void* buf, uint64_t length, uint64_t offset) {
    _statistics.read_calls++;
    _statistics.read_bytes += length;
    SCOPED_RAW_TIMER(&_statistics.read_time);
    uint64_t has_read = 0;
    char* out = reinterpret_cast<char*>(buf);
    while (has_read < length) {
        int64_t loop_read;
        Status st = _file_reader->readat(offset + has_read, length - has_read, &loop_read,
                                         out + has_read);
        if (!st.ok()) {
            throw orc::ParseError(
                    strings::Substitute("Failed to read $0: $1", _file_name, st.to_string()));
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != length) {
        throw orc::ParseError(strings::Substitute("Try to read $0 bytes from $1, actually read $2",
                                                  length, has_read, _file_name));
    }
}

OrcReader::OrcReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                     const TFileRangeDesc& range, const std::vector<std::string>& column_names,
                     size_t batch_size, const std::string& ctz)
        : _profile(profile),
          _scan_params(params),
          _scan_range(range),
          _batch_size(batch_size),
          _range_start_offset(range.start_offset),
          _range_size(range.size),
          _ctz(ctz),
          _column_names(column_names) {
    TimezoneUtils::find_cctz_time_zone(ctz, _time_zone);
    _init_profile();
}

OrcReader::~OrcReader() {
    close();
}

void OrcReader::close() {
    if (!_closed) {
        if (_profile != nullptr) {
            auto& fst = _file_reader->statistics();
            COUNTER_UPDATE(_orc_profile.read_time, fst.read_time);
            COUNTER_UPDATE(_orc_profile.read_calls, fst.read_calls);
            COUNTER_UPDATE(_orc_profile.read_bytes, fst.read_bytes);
            COUNTER_UPDATE(_orc_profile.column_read_time, _statistics.column_read_time);
            COUNTER_UPDATE(_orc_profile.get_batch_time, _statistics.get_batch_time);
            COUNTER_UPDATE(_orc_profile.parse_meta_time, _statistics.parse_meta_time);
            COUNTER_UPDATE(_orc_profile.decode_value_time, _statistics.decode_value_time);
            COUNTER_UPDATE(_orc_profile.decode_null_map_time, _statistics.decode_null_map_time);
        }
        _closed = true;
    }
}

void OrcReader::_init_profile() {
    if (_profile != nullptr) {
        static const char* orc_profile = "OrcReader";
        ADD_TIMER(_profile, orc_profile);
        _orc_profile.read_time = ADD_TIMER(_profile, "FileReadTime");
        _orc_profile.read_calls = ADD_COUNTER(_profile, "FileReadCalls", TUnit::UNIT);
        _orc_profile.read_bytes = ADD_COUNTER(_profile, "FileReadBytes", TUnit::BYTES);
        _orc_profile.column_read_time = ADD_CHILD_TIMER(_profile, "ColumnReadTime", orc_profile);
        _orc_profile.get_batch_time = ADD_CHILD_TIMER(_profile, "GetBatchTime", orc_profile);
        _orc_profile.parse_meta_time = ADD_CHILD_TIMER(_profile, "ParseMetaTime", orc_profile);
        _orc_profile.decode_value_time = ADD_CHILD_TIMER(_profile, "DecodeValueTime", orc_profile);
        _orc_profile.decode_null_map_time =
                ADD_CHILD_TIMER(_profile, "DecodeNullMapTime", orc_profile);
    }
}

Status OrcReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    SCOPED_RAW_TIMER(&_statistics.parse_meta_time);
    if (_file_reader == nullptr) {
        std::unique_ptr<FileReader> inner_reader;
        RETURN_IF_ERROR(FileFactory::create_file_reader(_profile, _scan_params, _scan_range.path,
                                                        _scan_range.start_offset,
                                                        _scan_range.file_size, 0, inner_reader));
        RETURN_IF_ERROR(inner_reader->open());
        _file_reader = new ORCFileInputStream(_scan_range.path, inner_reader.release());
    }
    if (_file_reader->getLength() == 0) {
        return Status::EndOfFile("Empty orc file");
    }

    // create orc reader
    try {
        orc::ReaderOptions options;
        _reader = orc::createReader(std::unique_ptr<ORCFileInputStream>(_file_reader), options);
    } catch (std::exception& e) {
        return Status::InternalError("Init OrcReader failed. reason = {}", e.what());
    }
    if (_reader->getNumberOfRows() == 0) {
        return Status::EndOfFile("Empty orc file");
    }
    // _init_bloom_filter(colname_to_value_range);

    // create orc row reader
    _row_reader_options.range(_range_start_offset, _range_size);
    _row_reader_options.setTimezoneName(_ctz);
    RETURN_IF_ERROR(_init_read_columns());
    // _init_search_argument(colname_to_value_range);
    _row_reader_options.include(_read_cols);
    try {
        _row_reader = _reader->createRowReader(_row_reader_options);
        _batch = _row_reader->createRowBatch(_batch_size);
    } catch (std::exception& e) {
        return Status::InternalError("Failed to create orc row reader. reason = {}", e.what());
    }
    auto& selected_type = _row_reader->getSelectedType();
    _col_orc_type.resize(selected_type.getSubtypeCount());
    for (int i = 0; i < selected_type.getSubtypeCount(); ++i) {
        _colname_to_idx[selected_type.getFieldName(i)] = i;
        _col_orc_type[i] = selected_type.getSubtype(i);
    }
    return Status::OK();
}

Status OrcReader::_init_read_columns() {
    auto& root_type = _reader->getType();
    std::unordered_set<std::string> orc_cols;
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        orc_cols.emplace(root_type.getFieldName(i));
    }
    for (auto& col_name : _column_names) {
        if (orc_cols.find(col_name) == orc_cols.end()) {
            _missing_cols.emplace_back(col_name);
        } else {
            _read_cols.emplace_back(col_name);
        }
    }
    if (_read_cols.empty()) {
        return Status::InternalError("No columns found in orc file");
    }
    return Status::OK();
}

void OrcReader::_init_search_argument(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
    // generate min-max search argument
    _row_reader_options.searchArgument(builder->build());
}

void OrcReader::_init_bloom_filter(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    // generate bloom filter
    // _reader->getBloomFilters()
}

TypeDescriptor OrcReader::_convert_to_doris_type(const orc::Type* orc_type) {
    switch (orc_type->getKind()) {
    case orc::TypeKind::BOOLEAN:
        return TypeDescriptor(PrimitiveType::TYPE_BOOLEAN);
    case orc::TypeKind::BYTE:
        return TypeDescriptor(PrimitiveType::TYPE_TINYINT);
    case orc::TypeKind::SHORT:
        return TypeDescriptor(PrimitiveType::TYPE_SMALLINT);
    case orc::TypeKind::INT:
        return TypeDescriptor(PrimitiveType::TYPE_INT);
    case orc::TypeKind::LONG:
        return TypeDescriptor(PrimitiveType::TYPE_BIGINT);
    case orc::TypeKind::FLOAT:
        return TypeDescriptor(PrimitiveType::TYPE_FLOAT);
    case orc::TypeKind::DOUBLE:
        return TypeDescriptor(PrimitiveType::TYPE_DOUBLE);
    case orc::TypeKind::STRING:
        return TypeDescriptor(PrimitiveType::TYPE_STRING);
    case orc::TypeKind::BINARY:
        return TypeDescriptor(PrimitiveType::TYPE_BINARY);
    case orc::TypeKind::TIMESTAMP:
        return TypeDescriptor(PrimitiveType::TYPE_DATETIMEV2);
    case orc::TypeKind::DECIMAL:
        return TypeDescriptor(PrimitiveType::TYPE_DECIMALV2);
    case orc::TypeKind::DATE:
        return TypeDescriptor(PrimitiveType::TYPE_DATEV2);
    case orc::TypeKind::VARCHAR:
        return TypeDescriptor(PrimitiveType::TYPE_VARCHAR);
    case orc::TypeKind::CHAR:
        return TypeDescriptor(PrimitiveType::TYPE_CHAR);
    case orc::TypeKind::TIMESTAMP_INSTANT:
        return TypeDescriptor(PrimitiveType::TYPE_DATETIMEV2);
    case orc::TypeKind::LIST: {
        TypeDescriptor list_type(PrimitiveType::TYPE_ARRAY);
        list_type.children.emplace_back(_convert_to_doris_type(orc_type->getSubtype(0)));
        return list_type;
    }
    case orc::TypeKind::MAP: {
        TypeDescriptor map_type(PrimitiveType::TYPE_MAP);
        map_type.children.emplace_back(_convert_to_doris_type(orc_type->getSubtype(0)));
        map_type.children.emplace_back(_convert_to_doris_type(orc_type->getSubtype(1)));
        return map_type;
    }
    case orc::TypeKind::STRUCT: {
        TypeDescriptor struct_type(PrimitiveType::TYPE_STRUCT);
        for (int i = 0; i < orc_type->getSubtypeCount(); ++i) {
            struct_type.children.emplace_back(_convert_to_doris_type(orc_type->getSubtype(i)));
            struct_type.field_names.emplace_back(orc_type->getFieldName(i));
        }
        return struct_type;
    }
    default:
        return TypeDescriptor(PrimitiveType::INVALID_TYPE);
    }
}

std::unordered_map<std::string, TypeDescriptor> OrcReader::get_name_to_type() {
    std::unordered_map<std::string, TypeDescriptor> map;
    auto& root_type = _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        map.emplace(root_type.getFieldName(i), _convert_to_doris_type(root_type.getSubtype(i)));
    }
    return map;
}

Status OrcReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    auto& root_type = _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        name_to_type->emplace(root_type.getFieldName(i),
                              _convert_to_doris_type(root_type.getSubtype(i)));
    }
    for (auto& col : _missing_cols) {
        missing_cols->insert(col);
    }
    return Status::OK();
}

// Hive ORC char type will pad trailing spaces.
// https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_char.html
static inline size_t trim_right(const char* s, size_t size) {
    while (size > 0 && s[size - 1] == ' ') {
        size--;
    }
    return size;
}

Status OrcReader::_decode_string_column(const std::string& col_name,
                                        const MutableColumnPtr& data_column,
                                        const orc::TypeKind& type_kind, orc::ColumnVectorBatch* cvb,
                                        size_t num_values) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    auto* data = down_cast<orc::StringVectorBatch*>(cvb);
    if (data == nullptr) {
        return Status::InternalError("Wrong data type for colum '{}'", col_name);
    }
    std::vector<StringRef> string_values;
    string_values.reserve(num_values);
    if (type_kind == orc::TypeKind::CHAR) {
        // Possibly there are some zero padding characters in CHAR type, we have to strip them off.
        for (int i = 0; i < num_values; ++i) {
            string_values.emplace_back(data->data[i], trim_right(data->data[i], data->length[i]));
        }
    } else {
        for (int i = 0; i < num_values; ++i) {
            string_values.emplace_back(data->data[i], data->length[i]);
        }
    }
    data_column->insert_many_strings(&string_values[0], num_values);
    return Status::OK();
}

Status OrcReader::_fill_doris_array_offsets(const std::string& col_name,
                                            const MutableColumnPtr& data_column,
                                            orc::ListVectorBatch* lvb, size_t num_values,
                                            size_t* element_size) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    if (num_values > 0) {
        auto& offsets_data = static_cast<ColumnArray&>(*data_column).get_offsets();
        auto& orc_offsets = lvb->offsets;
        if (orc_offsets.size() < num_values + 1) {
            return Status::InternalError("Wrong array offsets in orc file for column '{}'",
                                         col_name);
        }
        auto prev_offset = offsets_data.back();
        auto base_offset = orc_offsets[0];
        for (int i = 1; i < num_values + 1; ++i) {
            offsets_data.emplace_back(prev_offset + orc_offsets[i] - base_offset);
        }
        *element_size = orc_offsets[num_values] - base_offset;
    }
    return Status::OK();
}

Status OrcReader::_orc_column_to_doris_column(const std::string& col_name,
                                              const ColumnPtr& doris_column,
                                              const DataTypePtr& data_type,
                                              const orc::Type* orc_column_type,
                                              orc::ColumnVectorBatch* cvb, size_t num_values) {
    MutableColumnPtr data_column;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_statistics.decode_null_map_time);
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(doris_column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
        NullMap& map_data_column = nullable_column->get_null_map_data();
        auto origin_size = map_data_column.size();
        map_data_column.resize(origin_size + num_values);
        if (cvb->hasNulls) {
            auto* cvb_nulls = reinterpret_cast<uint8_t*>(cvb->notNull.data());
            for (int i = 0; i < num_values; ++i) {
                map_data_column[origin_size + i] = !cvb_nulls[i];
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                map_data_column[origin_size + i] = false;
            }
        }
    } else {
        if (cvb->hasNulls) {
            return Status::InternalError("Not nullable column {} has null values in orc file",
                                         col_name);
        }
        data_column = doris_column->assume_mutable();
    }

    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
#define DISPATCH(FlatType, CppType, OrcColumnType) \
    case FlatType:                                 \
        return _decode_flat_column<CppType, OrcColumnType>(col_name, data_column, cvb, num_values);
        FOR_FLAT_ORC_COLUMNS(DISPATCH)
#undef DISPATCH
    case TypeIndex::Decimal32:
        return _decode_decimal_column<Int32>(col_name, data_column, data_type,
                                             _decimal_scale_params, cvb, num_values);
    case TypeIndex::Decimal64:
        return _decode_decimal_column<Int64>(col_name, data_column, data_type,
                                             _decimal_scale_params, cvb, num_values);
    case TypeIndex::Decimal128:
        return _decode_decimal_column<Int128>(col_name, data_column, data_type,
                                              _decimal_scale_params, cvb, num_values);
    case TypeIndex::Date:
        return _decode_time_column<VecDateTimeValue, Int64, orc::LongVectorBatch>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateV2:
        return _decode_time_column<DateV2Value<DateV2ValueType>, UInt32, orc::LongVectorBatch>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateTime:
        return _decode_time_column<VecDateTimeValue, Int64, orc::TimestampVectorBatch>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateTimeV2:
        return _decode_time_column<DateV2Value<DateTimeV2ValueType>, UInt64,
                                   orc::TimestampVectorBatch>(col_name, data_column, cvb,
                                                              num_values);
    case TypeIndex::String:
    case TypeIndex::FixedString:
        return _decode_string_column(col_name, data_column, orc_column_type->getKind(), cvb,
                                     num_values);
    case TypeIndex::Array: {
        if (orc_column_type->getKind() != orc::TypeKind::LIST) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
        size_t element_size;
        RETURN_IF_ERROR(_fill_doris_array_offsets(col_name, data_column, orc_list, num_values,
                                                  &element_size));
        DataTypePtr& nested_type = const_cast<DataTypePtr&>(
                (reinterpret_cast<const DataTypeArray*>(remove_nullable(data_type).get()))
                        ->get_nested_type());
        const orc::Type* nested_orc_type = orc_column_type->getSubtype(0);
        return _orc_column_to_doris_column(
                col_name, static_cast<ColumnArray&>(*data_column).get_data_ptr(), nested_type,
                nested_orc_type, orc_list->elements.get(), element_size);
    }
    default:
        break;
    }
    return Status::InternalError("Unsupported type for column '{}'", col_name);
}

Status OrcReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    SCOPED_RAW_TIMER(&_statistics.column_read_time);
    {
        SCOPED_RAW_TIMER(&_statistics.get_batch_time);
        if (!_row_reader->next(*_batch)) {
            *eof = true;
            *read_rows = 0;
            return Status::OK();
        }
    }
    const auto& batch_vec = down_cast<orc::StructVectorBatch*>(_batch.get())->fields;
    for (auto& col : _read_cols) {
        auto& column_with_type_and_name = block->get_by_name(col);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        auto orc_col_idx = _colname_to_idx.find(col);
        if (orc_col_idx == _colname_to_idx.end()) {
            return Status::InternalError("Wrong read column '{}' in orc file", col);
        }
        RETURN_IF_ERROR(_orc_column_to_doris_column(
                col, column_ptr, column_type, _col_orc_type[orc_col_idx->second],
                batch_vec[orc_col_idx->second], _batch->numElements));
    }
    *read_rows = _batch->numElements;
    return Status::OK();
}

} // namespace doris::vectorized
