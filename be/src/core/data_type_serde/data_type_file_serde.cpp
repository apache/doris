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

#include "core/data_type_serde/data_type_file_serde.h"

#include <gen_cpp/types.pb.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <array>
#include <utility>

#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_file.h"
#include "core/string_buffer.hpp"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/url_coding.h"

namespace doris {
namespace {

constexpr size_t FILE_FIELDS = DataTypeFile::FIELD_COUNT;
constexpr size_t INLINE_FIELD_INDEX = DataTypeFile::INLINE_FIELD_INDEX;

// Multiples of three keep padding confined to the final chunk. Scratch space is bounded
// independently of the inline value's size.
void write_base64(StringRef bytes, BufferWritable& bw) {
    constexpr size_t INPUT_CHUNK = 3 * 1024;
    std::array<unsigned char, INPUT_CHUNK / 3 * 4> output;
    ColumnString::check_chars_length((bytes.size / 3 + (bytes.size % 3 != 0)) * 4 + 2, 1);
    bw.write('"');
    for (size_t offset = 0; offset < bytes.size; offset += INPUT_CHUNK) {
        const auto length = std::min(INPUT_CHUNK, bytes.size - offset);
        const auto written = base64_encode(
                reinterpret_cast<const unsigned char*>(bytes.data + offset), length, output.data());
        bw.write(reinterpret_cast<const char*>(output.data()), written);
    }
    bw.write('"');
}

Status check_pb_null_map(const PValues& value, int rows) {
    if ((value.has_null() && value.null_map_size() != rows) ||
        (!value.has_null() && value.null_map_size() != 0)) {
        return Status::InvalidArgument("FILE protobuf null map does not match row count {}", rows);
    }
    return Status::OK();
}

} // namespace

DataTypeFileSerDe::DataTypeFileSerDe(int nesting_level) : DataTypeSerDe(nesting_level) {
    const DataTypeFile type;
    _names = type.get_element_names();
    for (const auto& element : type.get_elements()) {
        _element_serdes.push_back(element->get_serde(nesting_level + 1));
    }
}

void DataTypeFileSerDe::set_return_object_as_string(bool value) {
    DataTypeSerDe::set_return_object_as_string(value);
    for (auto& serde : _element_serdes) {
        serde->set_return_object_as_string(value);
    }
}

void DataTypeFileSerDe::_write_json(const IColumn& column, int64_t row, BufferWritable& bw) const {
    const auto [data, index] = check_column_const_set_readability(column, row);
    const auto& file = assert_cast<const ColumnFile&>(*data);
    bw.write('{');
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        if (i != 0) {
            bw.write(',');
        }
        bw.write_json_string(_names[i].data(), _names[i].size());
        bw.write(':');
        const auto& child = assert_cast<const ColumnNullable&>(file.get_column(i));
        if (child.is_null_at(index)) {
            bw.write("null", 4);
        } else if (i == 1 || i == 2) {
            bw.write_number(child.get_nested_column().get_int(index));
        } else {
            const auto bytes = child.get_nested_column().get_data_at(index);
            if (i == INLINE_FIELD_INDEX) {
                write_base64(bytes, bw);
            } else {
                bw.write_json_string(bytes.data, bytes.size);
            }
        }
    }
    bw.write('}');
}

void DataTypeFileSerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                  const FormatOptions& options) const {
    _write_json(column, row_num, bw);
}

bool DataTypeFileSerDe::write_column_to_mysql_text(const IColumn& column, BufferWritable& bw,
                                                   int64_t row_idx,
                                                   const FormatOptions& options) const {
    to_string(column, row_idx, bw, options);
    return true;
}

Status DataTypeFileSerDe::write_column_to_mysql_binary(const IColumn& column,
                                                       MysqlRowBinaryBuffer& row_buffer,
                                                       int64_t row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    auto output = ColumnString::create();
    BufferWritable bw(*output);
    to_string(column, index_check_const(row_idx, col_const), bw, options);
    bw.commit();
    const auto json = output->get_data_at(0);
    if (row_buffer.push_string(json.data, json.size) != 0) {
        return Status::InternalError("Packing FILE into MySQL binary buffer failed");
    }
    return Status::OK();
}

Status DataTypeFileSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                     BufferWritable& bw,
                                                     FormatOptions& options) const {
    _write_json(column, row_num, bw);
    return Status::OK();
}

Status DataTypeFileSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                   int64_t end_idx, BufferWritable& bw,
                                                   FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeFileSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                         const FormatOptions&) const {
    rapidjson::Document document;
    document.Parse(slice.data, slice.size);
    if (document.HasParseError() || !document.IsObject()) {
        return Status::InvalidArgument("FILE aggregate state requires a JSON object");
    }
    File value(FILE_FIELDS);
    DorisVector<char> inline_bytes;
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        DCHECK(document.HasMember(_names[i].c_str()));
        const auto& field = document[_names[i].c_str()];
        if (field.IsNull()) {
            continue;
        }
        if (i == 1 || i == 2) {
            value[i] = Field::create_field<TYPE_BIGINT>(field.GetInt64());
        } else if (i == INLINE_FIELD_INDEX) {
            const auto encoded_size = field.GetStringLength();
            inline_bytes.resize(static_cast<size_t>(encoded_size) / 4 * 3);
            const auto size = encoded_size == 0 ? 0
                                                : base64_decode(field.GetString(), encoded_size,
                                                                inline_bytes.data());
            if (size < 0) {
                return Status::InvalidArgument("Invalid FILE aggregate state Base64");
            }
            value[i] = Field::create_field<TYPE_VARBINARY>(
                    StringView(inline_bytes.data(), cast_set<uint32_t>(size)));
        } else {
            value[i] = Field::create_field<TYPE_STRING>(
                    std::string(field.GetString(), field.GetStringLength()));
        }
    }
    column.insert(Field::create_field<TYPE_FILE>(std::move(value)));
    return Status::OK();
}

Status DataTypeFileSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                              std::vector<Slice>& slices,
                                                              uint64_t* num_deserialized,
                                                              const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeFileSerDe::serialize_column_to_jsonb(const IColumn& column, int64_t row_num,
                                                    JsonbWriter& writer,
                                                    const FormatOptions& options) const {
    const auto [data, index] = check_column_const_set_readability(column, row_num);
    const auto& file = assert_cast<const ColumnFile&>(*data);
    if (!writer.writeStartObject()) {
        return Status::InternalError("Cannot start FILE JSON object");
    }
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        if (!writer.writeKey(_names[i].data(), cast_set<uint8_t>(_names[i].size()))) {
            return Status::InternalError("Cannot write FILE JSON key");
        }
        if (i == INLINE_FIELD_INDEX) {
            const auto& child = assert_cast<const ColumnNullable&>(file.get_column(i));
            if (child.is_null_at(index)) {
                if (!writer.writeNull()) {
                    return Status::InternalError("Cannot write FILE inline NULL");
                }
            } else {
                auto encoded = ColumnString::create();
                BufferWritable buffer(*encoded);
                write_base64(child.get_nested_column().get_data_at(index), buffer);
                buffer.commit();
                const auto text = encoded->get_data_at(0);
                if (!writer.writeStartString() ||
                    !writer.writeString(text.data + 1, text.size - 2) || !writer.writeEndString()) {
                    return Status::InternalError("Cannot write FILE inline Base64");
                }
            }
        } else {
            RETURN_IF_ERROR(_element_serdes[i]->serialize_column_to_jsonb(file.get_column(i), index,
                                                                          writer, options));
        }
    }
    if (!writer.writeEndObject()) {
        return Status::InternalError("Cannot end FILE JSON object");
    }
    return Status::OK();
}

Status DataTypeFileSerDe::write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                                             int64_t end) const {
    const auto& file = assert_cast<const ColumnFile&>(column);
    result.mutable_type()->set_id(PGenericType::FILE);
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        RETURN_IF_ERROR(_element_serdes[i]->write_column_to_pb(
                file.get_column(i), *result.add_child_element(), start, end));
    }
    return Status::OK();
}

Status DataTypeFileSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    if (arg.type().id() != PGenericType::FILE ||
        std::cmp_not_equal(arg.child_element_size(), FILE_FIELDS)) {
        return Status::InvalidArgument("Expected FILE protobuf with six children");
    }
    const int rows = arg.child_element(0).string_value_size();
    RETURN_IF_ERROR(check_pb_null_map(arg, rows));
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        const auto& child = arg.child_element(cast_set<int>(i));
        const bool integer = i == 1 || i == 2;
        auto expected_type = integer ? PGenericType::INT64 : PGenericType::STRING;
        auto count = integer ? child.int64_value_size() : child.string_value_size();
        if (i == INLINE_FIELD_INDEX) {
            expected_type = PGenericType::VARBINARY;
            count = child.bytes_value_size();
        }
        if (child.type().id() != expected_type || count != rows) {
            return Status::InvalidArgument("Invalid FILE protobuf child {} type or row count",
                                           _names[i]);
        }
        RETURN_IF_ERROR(check_pb_null_map(child, rows));
    }
    auto& file = assert_cast<ColumnFile&>(column);
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        RETURN_IF_ERROR(_element_serdes[i]->read_column_from_pb(
                file.get_column(i), arg.child_element(cast_set<int>(i))));
    }
    return Status::OK();
}

void DataTypeFileSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& writer,
                                                Arena& arena, int32_t col_id, int64_t row_num,
                                                const FormatOptions& options) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "FILE row-store JSONB is not supported");
}

void DataTypeFileSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "FILE row-store JSONB is not supported");
}

Status DataTypeFileSerDe::write_column_to_arrow(const IColumn&, const NullMap*,
                                                arrow::ArrayBuilder*, int64_t, int64_t,
                                                const cctz::time_zone&) const {
    return Status::NotSupported("FILE Arrow output is not supported");
}

Status DataTypeFileSerDe::read_column_from_arrow(IColumn&, const arrow::Array*, int64_t, int64_t,
                                                 const cctz::time_zone&) const {
    return Status::NotSupported("FILE Arrow input is not supported");
}

Status DataTypeFileSerDe::write_column_to_orc(const std::string&, const IColumn&, const NullMap*,
                                              orc::ColumnVectorBatch*, int64_t, int64_t, Arena&,
                                              const FormatOptions&) const {
    return Status::NotSupported("FILE ORC output is not supported");
}

} // namespace doris
