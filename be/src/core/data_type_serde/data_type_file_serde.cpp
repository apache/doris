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

#include <arrow/array/array_nested.h>
#include <arrow/array/builder_nested.h>
#include <gen_cpp/types.pb.h>
#include <rapidjson/document.h>
#include <rapidjson/memorystream.h>

#include <algorithm>
#include <array>
#include <limits>
#include <orc/Type.hh>
#include <string_view>
#include <utility>

#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type_serde/arrow_validation.h"
#include "core/data_type_serde/orc_serde_utils.h"
#include "core/string_buffer.hpp"
#include "core/value/file_value.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/url_coding.h"

namespace doris {
namespace {

constexpr size_t FILE_FIELDS = DataTypeFile::FIELD_COUNT;
constexpr size_t INLINE_FIELD_INDEX = DataTypeFile::INLINE_FIELD_INDEX;

int base64_digit(char ch) {
    if (ch >= 'A' && ch <= 'Z') return ch - 'A';
    if (ch >= 'a' && ch <= 'z') return ch - 'a' + 26;
    if (ch >= '0' && ch <= '9') return ch - '0' + 52;
    if (ch == '+') return 62;
    if (ch == '/') return 63;
    return -1;
}

Status check_inline_base64(StringRef encoded, size_t padding) {
    const auto digits = encoded.size - padding;
    for (size_t i = 0; i < digits; ++i) {
        if (base64_digit(encoded.data[i]) < 0) {
            return Status::InvalidArgument("FILE inline requires standard padded Base64");
        }
    }
    // RFC 4648's unused low bits must be zero, so each byte sequence has one encoding.
    if (padding != 0 && (base64_digit(encoded.data[digits - 1]) & ((1 << (2 * padding)) - 1))) {
        return Status::InvalidArgument("FILE inline Base64 has nonzero pad bits");
    }
    return Status::OK();
}

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

Status append_file(IColumn& column, File value) {
    RETURN_IF_ERROR(validate_file(value));
    column.insert(Field::create_field<TYPE_FILE>(std::move(value)));
    return Status::OK();
}

Status check_pb_null_map(const PValues& value, int rows) {
    if ((value.has_null() && value.null_map_size() != rows) ||
        (!value.has_null() && value.null_map_size() != 0)) {
        return Status::InvalidArgument("FILE protobuf null map does not match row count {}", rows);
    }
    return Status::OK();
}

template <typename Array>
Status read_arrow_inline(IColumn& column, const Array& array, int64_t row) {
    check_arrow_array_range(array, row, row + 1);
    check_arrow_validity_bitmap(array);
    check_arrow_no_offset(array);
    if (array.IsNull(row)) {
        column.insert_default();
        return Status::OK();
    }
    check_arrow_binary_offsets_buffer(array);
    using Offset = typename Array::offset_type;
    const auto* offsets = array.value_offsets()->data() + row * sizeof(Offset);
    const auto offset = unaligned_load<Offset>(offsets);
    const auto next_offset = unaligned_load<Offset>(offsets + sizeof(Offset));
    if (offset < 0 || next_offset < offset) {
        return Status::InvalidArgument("Invalid FILE inline Arrow offsets");
    }
    const auto length = next_offset - offset;
    if (length > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("FILE inline exceeds VARBINARY value length");
    }
    const auto& buffer = array.value_data();
    check_arrow_value_range(array, offset, length, buffer ? buffer->size() : 0);
    const auto* bytes = length == 0 ? "" : reinterpret_cast<const char*>(buffer->data() + offset);
    column.insert(
            Field::create_field<TYPE_VARBINARY>(StringView(bytes, static_cast<uint32_t>(length))));
    return Status::OK();
}

Status read_orc_inline(IColumn& column, const orc::StructVectorBatch& structure, int position,
                       size_t rows, const std::vector<size_t>* selected_rows) {
    if (position < 0) {
        column.insert_many_defaults(rows);
        return Status::OK();
    }
    const auto* binary = dynamic_cast<const orc::StringVectorBatch*>(structure.fields[position]);
    if (!binary) {
        return Status::InvalidArgument("FILE inline ORC child must be binary");
    }
    for (size_t row = 0; row < rows; ++row) {
        const auto source = orc_serde_utils::orc_source_row_at(row, selected_rows);
        if (orc_serde_utils::orc_row_is_null(*binary, source)) {
            column.insert_default();
        } else {
            if (binary->length[source] < 0 ||
                std::cmp_greater(binary->length[source], std::numeric_limits<uint32_t>::max())) {
                return Status::InvalidArgument("FILE inline exceeds VARBINARY value length");
            }
            column.insert(Field::create_field<TYPE_VARBINARY>(StringView(
                    binary->data[source], static_cast<uint32_t>(binary->length[source]))));
        }
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
        if (i != 0) bw.write(',');
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

Status DataTypeFileSerDe::from_string(StringRef& str, IColumn& column,
                                      const FormatOptions& options) const {
    return _read_json(str, column);
}

Status DataTypeFileSerDe::_checked_inline_size(size_t encoded_size, size_t padding,
                                               uint32_t* decoded_size) {
    if (encoded_size % 4 != 0 || padding > 2 || (encoded_size == 0 && padding != 0)) {
        return Status::InvalidArgument("FILE inline requires standard padded Base64");
    }
    constexpr uint64_t MAX_INLINE_SIZE = std::numeric_limits<uint32_t>::max();
    const auto groups = encoded_size / 4;
    // Check before multiplication/allocation; the limit is VARBINARY's uint32_t length.
    if (groups > (MAX_INLINE_SIZE + padding) / 3) {
        return Status::InvalidArgument("FILE inline exceeds VARBINARY value length");
    }
    *decoded_size = static_cast<uint32_t>(groups * 3 - padding);
    return Status::OK();
}

Status DataTypeFileSerDe::_decode_inline(StringRef encoded, DorisVector<char>& scratch,
                                             Field& value) {
    size_t padding = 0;
    if (encoded.size != 0 && encoded.data[encoded.size - 1] == '=') {
        padding = encoded.size > 1 && encoded.data[encoded.size - 2] == '=' ? 2 : 1;
    }
    uint32_t decoded_size = 0;
    RETURN_IF_ERROR(_checked_inline_size(encoded.size, padding, &decoded_size));
    RETURN_IF_ERROR(check_inline_base64(encoded, padding));
    // libbase64 needs room for 3/4 of the encoded input, including padding.
    // The checked group count bounds this scratch allocation before it is made.
    scratch.resize(static_cast<size_t>(decoded_size) + padding);
    if (decoded_size != 0 &&
        base64_decode(encoded.data, encoded.size, scratch.data()) != decoded_size) {
        return Status::InvalidArgument("Invalid FILE inline Base64");
    }
    value = Field::create_field<TYPE_VARBINARY>(StringView(scratch.data(), decoded_size));
    return Status::OK();
}

Status DataTypeFileSerDe::_read_json(StringRef str, IColumn& column) const {
    if (str.size == 0) return Status::InvalidArgument("FILE requires a JSON object");
    rapidjson::Document document;
    rapidjson::MemoryStream stream(str.data, str.size);
    document.ParseStream<rapidjson::kParseValidateEncodingFlag>(stream);
    if (document.HasParseError() || !document.IsObject() || stream.Tell() != str.size) {
        return Status::InvalidArgument("FILE requires a JSON object");
    }
    File value(FILE_FIELDS);
    DorisVector<char> inline_bytes;
    std::array<bool, FILE_FIELDS> seen {};
    for (const auto& member : document.GetObject()) {
        const std::string_view name(member.name.GetString(), member.name.GetStringLength());
        const auto it = std::ranges::find(_names, name);
        if (it == _names.end()) {
            return Status::InvalidArgument("Unknown FILE JSON field {}", name);
        }
        const auto index = cast_set<size_t>(it - _names.begin());
        if (seen[index]) {
            return Status::InvalidArgument("Duplicate FILE JSON field {}", name);
        }
        seen[index] = true;
        const auto& field = member.value;
        if (field.IsNull()) continue;
        if (index == 1 || index == 2) {
            if (!field.IsInt64()) {
                return Status::InvalidArgument("FILE {} must be a BIGINT JSON integer", name);
            }
            value[index] = Field::create_field<TYPE_BIGINT>(field.GetInt64());
        } else {
            if (!field.IsString()) {
                return Status::InvalidArgument("FILE {} must be a JSON string", name);
            }
            if (index == INLINE_FIELD_INDEX) {
                RETURN_IF_ERROR(_decode_inline(
                        StringRef(field.GetString(), field.GetStringLength()), inline_bytes, value[index]));
            } else {
                value[index] = Field::create_field<TYPE_STRING>(
                        std::string(field.GetString(), field.GetStringLength()));
            }
        }
    }
    return append_file(column, std::move(value));
}

Status DataTypeFileSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                         const FormatOptions& options) const {
    return _read_json(StringRef(slice.data, slice.size), column);
}

Status DataTypeFileSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                              std::vector<Slice>& slices,
                                                              uint64_t* num_deserialized,
                                                              const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeFileSerDe::serialize_column_to_jsonb(const IColumn& column, int64_t row_num,
                                                    JsonbWriter& writer) const {
    const auto [data, index] = check_column_const_set_readability(column, row_num);
    const auto& file = assert_cast<const ColumnFile&>(*data);
    if (!writer.writeStartObject()) return Status::InternalError("Cannot start FILE JSON object");
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
                if (!writer.writeStartString() || !writer.writeString(text.data + 1, text.size - 2) ||
                    !writer.writeEndString()) {
                    return Status::InternalError("Cannot write FILE inline Base64");
                }
            }
        } else {
            RETURN_IF_ERROR(_element_serdes[i]->serialize_column_to_jsonb(file.get_column(i), index, writer));
        }
    }
    if (!writer.writeEndObject()) return Status::InternalError("Cannot end FILE JSON object");
    return Status::OK();
}

Status DataTypeFileSerDe::deserialize_column_from_jsonb(IColumn& column, const JsonbValue* json,
                                                        CastParameters& cast_parameters) const {
    if (!json->isObject()) return Status::InvalidArgument("FILE requires a JSON object");
    File value(FILE_FIELDS);
    std::array<bool, FILE_FIELDS> seen {};
    DorisVector<char> inline_bytes;
    for (const auto& member : *json->unpack<ObjectVal>()) {
        const std::string_view name(member.getKeyStr(), member.klen());
        const auto it = std::ranges::find(_names, name);
        if (it == _names.end()) {
            return Status::InvalidArgument("Unknown FILE JSON field {}", name);
        }
        const auto index = cast_set<size_t>(it - _names.begin());
        if (seen[index]) return Status::InvalidArgument("Duplicate FILE JSON field {}", name);
        seen[index] = true;
        const auto* field = member.value();
        if (field->isNull()) continue;
        if (index == 1 || index == 2) {
            if (!field->isInt() || field->int_val() < std::numeric_limits<int64_t>::min() ||
                field->int_val() > std::numeric_limits<int64_t>::max()) {
                return Status::InvalidArgument("FILE {} must be a BIGINT JSON integer", name);
            }
            value[index] = Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(field->int_val()));
        } else {
            if (!field->isString())
                return Status::InvalidArgument("FILE {} must be a string", name);
            const auto* bytes = field->unpack<JsonbStringVal>();
            if (index == INLINE_FIELD_INDEX) {
                RETURN_IF_ERROR(_decode_inline(StringRef(bytes->getBlob(), bytes->getBlobLen()),
                                               inline_bytes, value[index]));
            } else {
                value[index] = Field::create_field<TYPE_STRING>(
                        std::string(bytes->getBlob(), bytes->getBlobLen()));
            }
        }
    }
    if (std::ranges::any_of(seen, [](bool present) { return !present; })) {
        return Status::InvalidArgument("FILE CAST requires exactly six fields");
    }
    return append_file(column, std::move(value));
}

Status DataTypeFileSerDe::write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                                             int64_t end) const {
    const auto& file = assert_cast<const ColumnFile&>(column);
    result.mutable_type()->set_id(PGenericType::FILE);
    for (size_t i = 0; i < INLINE_FIELD_INDEX; ++i) {
        RETURN_IF_ERROR(_element_serdes[i]->write_column_to_pb(
                file.get_column(i), *result.add_child_element(), start, end));
    }
    // VARBINARY's standalone serde has no PB representation; FILE carries raw bytes explicitly.
    auto* binary = result.add_child_element();
    binary->mutable_type()->set_id(PGenericType::VARBINARY);
    const auto& child = assert_cast<const ColumnNullable&>(file.get_column(INLINE_FIELD_INDEX));
    const bool has_null = child.has_null(start, end);
    binary->set_has_null(has_null);
    for (int64_t row = start; row < end; ++row) {
        if (has_null) binary->add_null_map(child.is_null_at(row));
        const auto bytes = child.get_nested_column().get_data_at(row);
        binary->add_bytes_value(bytes.data, bytes.size);
    }
    return Status::OK();
}

Status DataTypeFileSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    if (arg.type().id() != PGenericType::FILE || arg.child_element_size() != FILE_FIELDS) {
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
    auto decoded = column.clone_empty();
    auto& file = assert_cast<ColumnFile&>(*decoded);
    for (size_t i = 0; i < INLINE_FIELD_INDEX; ++i) {
        RETURN_IF_ERROR(
                _element_serdes[i]->read_column_from_pb(file.get_column(i), arg.child_element(cast_set<int>(i))));
    }
    const auto& binary = arg.child_element(INLINE_FIELD_INDEX);
    for (int row = 0; row < rows; ++row) {
        if (binary.has_null() && binary.null_map(row)) {
            file.get_column(INLINE_FIELD_INDEX).insert_default();
        } else {
            const auto& bytes = binary.bytes_value(row);
            if (bytes.size() > std::numeric_limits<uint32_t>::max()) {
                return Status::InvalidArgument("FILE inline exceeds VARBINARY value length");
            }
            file.get_column(INLINE_FIELD_INDEX).insert(Field::create_field<TYPE_VARBINARY>(StringView(bytes)));
        }
    }
    // Ancestor STRUCT/ARRAY/MAP null masks are unavailable here. The complete-value
    // boundary must call validate_file_column after decoding all enclosing columns.
    column.insert_range_from(file, 0, rows);
    return Status::OK();
}

void DataTypeFileSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& writer,
                                                Arena& arena, int32_t col_id, int64_t row_num,
                                                const FormatOptions& options) const {
    // An opaque, length-delimited protobuf retains both FILE identity and all six children.
    const auto [data, index] = check_column_const_set_readability(column, row_num);
    PValues value;
    THROW_IF_ERROR(write_column_to_pb(*data, value, index, index + 1));
    if (value.ByteSizeLong() > std::numeric_limits<int32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "FILE row-store payload exceeds JSONB length");
    }
    std::string bytes;
    if (!value.SerializeToString(&bytes)) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Cannot serialize FILE row-store value");
    }
    // The nullable wrapper may already have written this key, as for other row-store serdes.
    writer.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    if (!writer.writeStartBinary() || !writer.writeBinary(bytes.data(), bytes.size()) ||
        !writer.writeEndBinary()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Cannot write FILE row-store value");
    }
}

void DataTypeFileSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    if (!arg->isBinary()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "FILE row-store value must be binary");
    }
    const auto* blob = arg->unpack<JsonbBinaryVal>();
    PValues value;
    if (blob->getBlobLen() > std::numeric_limits<int>::max() ||
        !value.ParseFromArray(blob->getBlob(), static_cast<int>(blob->getBlobLen())) ||
        value.has_null() || value.child_element_size() != FILE_FIELDS ||
        value.child_element(0).string_value_size() != 1) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid FILE row-store payload");
    }
    auto decoded = column.clone_empty();
    THROW_IF_ERROR(read_column_from_pb(*decoded, value));
    THROW_IF_ERROR(validate_file_row(assert_cast<const ColumnFile&>(*decoded), 0));
    column.insert_from(*decoded, 0);
}

Status DataTypeFileSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int64_t start,
                                                int64_t end, const cctz::time_zone& ctz) const {
    auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
    const auto& file = assert_cast<const ColumnFile&>(column);
    // FILE markers belong to Arrow Fields and are emitted by the schema factory.
    for (auto row = start; row < end; ++row) {
        if (null_map && (*null_map)[row]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(), column, builder));
        for (size_t i = 0; i < FILE_FIELDS; ++i) {
            RETURN_IF_ERROR(_element_serdes[i]->write_column_to_arrow(
                    file.get_column(i), nullptr, builder.field_builder(cast_set<int>(i)), row, row + 1, ctz));
        }
    }
    return Status::OK();
}

Status DataTypeFileSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* array,
                                                 int64_t start, int64_t end,
                                                 const cctz::time_zone& ctz) const {
    const auto* structure = dynamic_cast<const arrow::StructArray*>(array);
    if (!structure || structure->num_fields() != FILE_FIELDS) {
        return Status::InvalidArgument("FILE Arrow input requires six children");
    }
    check_arrow_array_range(*array, start, end);
    check_arrow_validity_bitmap(*array);
    check_arrow_no_offset(*array);
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        const auto& field = structure->type()->field(cast_set<int>(i));
        if (field->name() != _names[i]) {
            return Status::NotSupported(
                    "FILE Arrow input currently requires canonical child names and order");
        }
        const auto id = field->type()->id();
        bool valid_type;
        if (i == 1 || i == 2) {
            valid_type = id == arrow::Type::INT64;
        } else if (i == INLINE_FIELD_INDEX) {
            valid_type = id == arrow::Type::BINARY || id == arrow::Type::LARGE_BINARY;
        } else {
            valid_type = id == arrow::Type::STRING || id == arrow::Type::LARGE_STRING;
        }
        if (!valid_type) {
            return Status::InvalidArgument("Invalid FILE Arrow child {} type {}", _names[i],
                                           field->type()->ToString());
        }
    }
    auto decoded = column.clone_empty();
    auto& file = assert_cast<ColumnFile&>(*decoded);
    for (auto row = start; row < end; ++row) {
        if (array->IsNull(row)) {
            file.insert_default();
            continue;
        }
        for (size_t i = 0; i < INLINE_FIELD_INDEX; ++i) {
            RETURN_IF_ERROR(_element_serdes[i]->read_column_from_arrow(
                    file.get_column(i), structure->field(cast_set<int>(i)).get(), row, row + 1, ctz));
        }
        const auto& binary = *structure->field(INLINE_FIELD_INDEX);
        if (binary.type_id() == arrow::Type::BINARY) {
            RETURN_IF_ERROR(read_arrow_inline(file.get_column(INLINE_FIELD_INDEX),
                                              assert_cast<const arrow::BinaryArray&>(binary), row));
        } else {
            RETURN_IF_ERROR(read_arrow_inline(
                    file.get_column(INLINE_FIELD_INDEX), assert_cast<const arrow::LargeBinaryArray&>(binary), row));
        }
    }
    // Validate the whole enclosing column once its ancestor null maps are available.
    column.insert_range_from(file, 0, file.size());
    return Status::OK();
}

Status DataTypeFileSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                              const NullMap* null_map,
                                              orc::ColumnVectorBatch* batch, int64_t start,
                                              int64_t end, Arena& arena,
                                              const FormatOptions& options) const {
    auto& structure = assert_cast<orc::StructVectorBatch&>(*batch);
    const auto& file = assert_cast<const ColumnFile&>(column);
    structure.hasNulls = null_map != nullptr;
    for (auto row = start; row < end; ++row) {
        structure.notNull[row] = !null_map || !(*null_map)[row];
    }
    for (size_t i = 0; i < FILE_FIELDS; ++i) {
        RETURN_IF_ERROR(_element_serdes[i]->write_column_to_orc(timezone, file.get_column(i),
                                                                null_map, structure.fields[i],
                                                                start, end, arena, options));
    }
    structure.numElements = end - start;
    return Status::OK();
}

Status DataTypeFileSerDe::read_column_from_orc(IColumn& column,
                                               const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.selected_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    const auto* structure = dynamic_cast<const orc::StructVectorBatch*>(view.batch);
    if (!structure || structure->fields.size() != view.selected_type->getSubtypeCount()) {
        return Status::InvalidArgument("FILE ORC batch does not match its selected schema");
    }
    if (view.selected_type->getSubtypeCount() != view.file_type->getSubtypeCount()) {
        return Status::NotSupported("Reading a pruned ORC FILE value is not implemented");
    }
    std::array<int, FILE_FIELDS> positions;
    RETURN_IF_ERROR(orc_serde_utils::validate_orc_file_type(*view.file_type, &positions));
    for (size_t i = 0; i < view.file_type->getSubtypeCount(); ++i) {
        if (view.selected_type->getFieldName(i) != view.file_type->getFieldName(i)) {
            return Status::NotSupported("FILE ORC input requires canonical child names and order");
        }
    }
    auto decoded = column.clone_empty();
    auto& file = assert_cast<ColumnFile&>(*decoded);
    const auto decode_children = [&](const std::vector<size_t>* selected_rows) -> Status {
        const auto rows = orc_serde_utils::orc_decode_row_count(view.rows, selected_rows);
        for (size_t i = 0; i < INLINE_FIELD_INDEX; ++i) {
            const auto position = positions[i];
            if (position < 0) {
                file.get_column(i).insert_many_defaults(rows);
                continue;
            }
            auto child = file.get_column_ptr(i)->assert_mutable();
            const auto child_view = orc_serde_utils::make_child_orc_view(
                    view, view.file_type->getSubtype(position),
                    view.selected_type->getSubtype(position), structure->fields[position],
                    view.rows, selected_rows);
            RETURN_IF_ERROR(
                    orc_serde_utils::read_orc_child_column(_element_serdes[i], child, child_view));
            file.get_column_ptr(i) = std::move(child);
        }
        return read_orc_inline(file.get_column(INLINE_FIELD_INDEX), *structure, positions[INLINE_FIELD_INDEX], rows, selected_rows);
    };
    if (!structure->hasNulls && view.selected_rows == nullptr) {
        RETURN_IF_ERROR(decode_children(nullptr));
    } else {
        // Decode live rows separately so parent NULLs never expose child payloads.
        const auto rows = orc_serde_utils::orc_decode_row_count(view.rows, view.selected_rows);
        std::vector<size_t> selection(1);
        for (size_t row = 0; row < rows; ++row) {
            const auto source = orc_serde_utils::orc_source_row_at(row, view.selected_rows);
            if (orc_serde_utils::orc_row_is_null(*structure, source)) {
                file.insert_default();
            } else {
                selection[0] = source;
                RETURN_IF_ERROR(decode_children(&selection));
            }
        }
    }
    // Validate the whole enclosing column once its ancestor null maps are available.
    column.insert_range_from(file, 0, file.size());
    return Status::OK();
}

} // namespace doris
