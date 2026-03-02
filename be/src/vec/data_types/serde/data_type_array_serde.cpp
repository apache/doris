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

#include "data_type_array_serde.h"

#include <arrow/array/builder_nested.h>

#include "common/status.h"
#include "complex_type_deserialize_util.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {
class Arena;
#include "common/compile_check_begin.h"

Status DataTypeArraySerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                    int64_t end_idx, BufferWritable& bw,
                                                    FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeArraySerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                      BufferWritable& bw,
                                                      FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& data_column = assert_cast<const ColumnArray&>(*ptr);
    const auto& offsets = data_column.get_offsets();
    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& nested_column = data_column.get_data();
    //    bool is_nested_string = remove_nullable(nested_column.get_ptr())->is_column_string();

    bw.write("[", 1);
    // nested column field delim should be replaced as collection delim because this field is in array.
    //  add ' ' to keep same with origin format with array
    options.field_delim = options.collection_delim;
    options.field_delim += " ";
    RETURN_IF_ERROR(nested_serde->serialize_column_to_json(nested_column, offset, next_offset, bw,
                                                           options));
    bw.write("]", 1);
    return Status::OK();
}

Status DataTypeArraySerDe::deserialize_column_from_json_vector(IColumn& column,
                                                               std::vector<Slice>& slices,
                                                               uint64_t* num_deserialized,
                                                               const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeArraySerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                          const FormatOptions& options) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }

    auto& array_column = assert_cast<ColumnArray&>(column);
    auto& offsets = array_column.get_offsets();
    IColumn& nested_column = array_column.get_data();
    DCHECK(nested_column.is_nullable());
    if (slice[0] != '[') {
        return Status::InvalidArgument("Array does not start with '[' character, found '{}'",
                                       slice[0]);
    }
    if (slice[slice.size - 1] != ']') {
        return Status::InvalidArgument("Array does not end with ']' character, found '{}'",
                                       slice[slice.size - 1]);
    }
    // empty array []
    if (slice.size == 2) {
        auto last_off = offsets.back();
        offsets.push_back(last_off);
        return Status::OK();
    }
    slice.remove_prefix(1);
    slice.remove_suffix(1);

    // deserialize array column from text we have to know how to split from text and support nested
    //  complex type.
    //   1. get item according to collection_delimiter, but if meet collection_delimiter in string, we should ignore it.
    //   2. keep a nested level to support nested complex type.
    int nested_level = 0;
    bool has_quote = false;
    std::vector<Slice> slices;
    slices.reserve(10);
    slice.trim_prefix();
    slices.emplace_back(slice);
    size_t slice_size = slice.size;
    // pre add total slice can reduce lasted element check.
    char quote_char = 0;
    for (int idx = 0; idx < slice_size; ++idx) {
        char c = slice[idx];
        if (c == '"' || c == '\'') {
            if (!has_quote) {
                quote_char = c;
                has_quote = !has_quote;
            } else if (has_quote && quote_char == c) {
                quote_char = 0;
                has_quote = !has_quote;
            }
        } else if (!has_quote && (c == '[' || c == '{')) {
            ++nested_level;
        } else if (!has_quote && (c == ']' || c == '}')) {
            --nested_level;
        } else if (!has_quote && nested_level == 0 && c == options.collection_delim) {
            // if meet collection_delimiter and not in quote, we can make it as an item.
            auto& last_slice = slices.back();
            last_slice.remove_suffix(slice_size - idx);
            // we do not handle item in array is empty,just return error
            if (last_slice.empty()) {
                return Status::InvalidArgument("here has item in Array({}) is empty!",
                                               slice.to_string());
            }
            // add next total slice.(slice data will not change, so we can use slice directly)
            // skip delimiter
            Slice next(slice.data + idx + 1, slice_size - idx - 1);
            next.trim_prefix();
            slices.emplace_back(next);
        }
    }

    uint64_t elem_deserialized = 0;
    Status st = nested_serde->deserialize_column_from_json_vector(nested_column, slices,
                                                                  &elem_deserialized, options);
    offsets.emplace_back(offsets.back() + elem_deserialized);
    return st;
}

Status DataTypeArraySerDe::deserialize_one_cell_from_hive_text(
        IColumn& column, Slice& slice, const FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    auto& array_column = assert_cast<ColumnArray&>(column);
    auto& offsets = array_column.get_offsets();
    IColumn& nested_column = array_column.get_data();
    DCHECK(nested_column.is_nullable());

    char collection_delimiter =
            options.get_collection_delimiter(hive_text_complex_type_delimiter_level);

    std::vector<Slice> slices;
    for (int idx = 0, start = 0; idx <= slice.size; idx++) {
        char c = (idx == slice.size) ? collection_delimiter : slice[idx];
        if (c == collection_delimiter) {
            if (options.escape_char != 0 && idx > 0 && slice[idx - 1] == options.escape_char) {
                continue;
            }
            slices.emplace_back(slice.data + start, idx - start);
            start = idx + 1;
        }
    }

    uint64_t elem_deserialized = 0;
    Status status = nested_serde->deserialize_column_from_hive_text_vector(
            nested_column, slices, &elem_deserialized, options,
            hive_text_complex_type_delimiter_level + 1);
    offsets.emplace_back(offsets.back() + elem_deserialized);
    return status;
}

Status DataTypeArraySerDe::deserialize_column_from_hive_text_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options, int hive_text_complex_type_delimiter_level) const {
    DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR();
    return Status::OK();
}

Status DataTypeArraySerDe::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& data_column = assert_cast<const ColumnArray&>(*ptr);
    const auto& offsets = data_column.get_offsets();

    size_t start = offsets[row_num - 1];
    size_t end = offsets[row_num];

    const IColumn& nested_column = data_column.get_data();

    char delimiter = options.get_collection_delimiter(hive_text_complex_type_delimiter_level);
    for (size_t i = start; i < end; ++i) {
        if (i != start) {
            bw.write(delimiter);
        }
        RETURN_IF_ERROR(nested_serde->serialize_one_cell_to_hive_text(
                nested_column, i, bw, options, hive_text_complex_type_delimiter_level + 1));
    }
    return Status::OK();
}

Status DataTypeArraySerDe::serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                                     JsonbWriter& writer) const {
    const auto& data_column = assert_cast<const ColumnArray&>(from_column);
    const auto& offsets = data_column.get_offsets();

    size_t start = offsets[row_num - 1];
    size_t end = offsets[row_num];
    const IColumn& nested_column = data_column.get_data();

    if (!writer.writeStartArray()) {
        return Status::InternalError("writeStartArray failed");
    }

    for (size_t i = start; i < end; ++i) {
        RETURN_IF_ERROR(nested_serde->serialize_column_to_jsonb(nested_column, i, writer));
    }
    if (!writer.writeEndArray()) {
        return Status::InternalError("writeEndArray failed");
    }

    return Status::OK();
}

Status DataTypeArraySerDe::deserialize_column_from_jsonb(IColumn& column,
                                                         const JsonbValue* jsonb_value,
                                                         CastParameters& castParms) const {
    if (jsonb_value->isString()) {
        RETURN_IF_ERROR(parse_column_from_jsonb_string(column, jsonb_value, castParms));
        return Status::OK();
    }
    if (!jsonb_value->isArray()) {
        return Status::InvalidArgument("JsonbValue type is not array, type: {}",
                                       jsonb_value->typeName());
    }

    auto& array_column = assert_cast<ColumnArray&>(column);

    auto& offsets = array_column.get_offsets();
    IColumn& nested_column = array_column.get_data();
    DCHECK(nested_column.is_nullable());

    const auto* jsonb_array = jsonb_value->unpack<ArrayVal>();
    const auto array_size = jsonb_array->numElem();

    for (int i = 0; i < array_size; ++i) {
        const JsonbValue* elem = jsonb_array->get(i);
        RETURN_IF_ERROR(
                nested_serde->deserialize_column_from_jsonb(nested_column, elem, castParms));
    }

    offsets.push_back(offsets.back() + array_size);

    return Status::OK();
}

void DataTypeArraySerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                 Arena& arena, int32_t col_id, int64_t row_num,
                                                 const FormatOptions& options) const {
    // JsonbKeyValue::keyid_type is uint16_t and col_id is int32_t, need a cast
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    const char* begin = nullptr;
    // maybe serialize_value_into_arena should move to here later.
    StringRef value = column.serialize_value_into_arena(row_num, arena, begin);
    result.writeStartBinary();
    result.writeBinary(value.data, value.size);
    result.writeEndBinary();
}

void DataTypeArraySerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    const auto* blob = arg->unpack<JsonbBinaryVal>();
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

Status DataTypeArraySerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                 arrow::ArrayBuilder* array_builder, int64_t start,
                                                 int64_t end, const cctz::time_zone& ctz) const {
    const auto& array_column = static_cast<const ColumnArray&>(column);
    const auto& offsets = array_column.get_offsets();
    const auto& nested_data = array_column.get_data();
    auto& builder = assert_cast<arrow::ListBuilder&>(*array_builder);
    auto* nested_builder = builder.value_builder();
    for (size_t array_idx = start; array_idx < end; ++array_idx) {
        if (null_map && (*null_map)[array_idx]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
            continue;
        }
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(), column.get_name(),
                                         array_builder->type()->name()));
        RETURN_IF_ERROR(nested_serde->write_column_to_arrow(nested_data, nullptr, nested_builder,
                                                            offsets[array_idx - 1],
                                                            offsets[array_idx], ctz));
    }
    return Status::OK();
}

Status DataTypeArraySerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                  int64_t start, int64_t end,
                                                  const cctz::time_zone& ctz) const {
    auto& column_array = static_cast<ColumnArray&>(column);
    auto& offsets_data = column_array.get_offsets();
    const auto* concrete_array = dynamic_cast<const arrow::ListArray*>(arrow_array);
    auto arrow_offsets_array = concrete_array->offsets();
    auto* arrow_offsets = dynamic_cast<arrow::Int32Array*>(arrow_offsets_array.get());
    auto prev_size = offsets_data.back();
    const auto* base_offsets_ptr = reinterpret_cast<const uint8_t*>(arrow_offsets->raw_values());
    const size_t offset_element_size = sizeof(int32_t);
    int32_t arrow_nested_start_offset = 0;
    int32_t arrow_nested_end_offset = 0;
    const uint8_t* start_offset_ptr = base_offsets_ptr + start * offset_element_size;
    const uint8_t* end_offset_ptr = base_offsets_ptr + end * offset_element_size;
    memcpy(&arrow_nested_start_offset, start_offset_ptr, offset_element_size);
    memcpy(&arrow_nested_end_offset, end_offset_ptr, offset_element_size);

    for (auto i = start + 1; i < end + 1; ++i) {
        int32_t current_offset = 0;
        const uint8_t* current_offset_ptr = base_offsets_ptr + i * offset_element_size;
        memcpy(&current_offset, current_offset_ptr, offset_element_size);
        // convert to doris offset, start from offsets.back()
        offsets_data.emplace_back(prev_size + current_offset - arrow_nested_start_offset);
    }
    return nested_serde->read_column_from_arrow(
            column_array.get_data(), concrete_array->values().get(), arrow_nested_start_offset,
            arrow_nested_end_offset, ctz);
}

Status DataTypeArraySerDe::write_column_to_mysql_binary(const IColumn& column,
                                                        MysqlRowBinaryBuffer& result,
                                                        int64_t row_idx_of_mysql, bool col_const,
                                                        const FormatOptions& options) const {
    return Status::NotSupported("Array type does not support write to mysql binary format");
}

Status DataTypeArraySerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                               const NullMap* null_map,
                                               orc::ColumnVectorBatch* orc_col_batch, int64_t start,
                                               int64_t end, vectorized::Arena& arena,
                                               const FormatOptions& options) const {
    auto* cur_batch = dynamic_cast<orc::ListVectorBatch*>(orc_col_batch);
    cur_batch->offsets[0] = 0;

    const auto& array_col = assert_cast<const ColumnArray&>(column);
    const IColumn& nested_column = array_col.get_data();
    const auto& offsets = array_col.get_offsets();
    for (size_t row_id = start; row_id < end; row_id++) {
        size_t offset = offsets[row_id - 1];
        size_t next_offset = offsets[row_id];
        RETURN_IF_ERROR(nested_serde->write_column_to_orc(timezone, nested_column, nullptr,
                                                          cur_batch->elements.get(), offset,
                                                          next_offset, arena, options));
        cur_batch->offsets[row_id + 1] = next_offset;
    }
    cur_batch->elements->numElements = nested_column.size();

    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeArraySerDe::write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                                              int64_t end) const {
    const auto& array_col = assert_cast<const ColumnArray&>(column);
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::LIST);
    const IColumn& nested_column = array_col.get_data();
    const auto& offsets = array_col.get_offsets();
    auto* child_element = result.add_child_element();
    for (size_t row_id = start; row_id < end; row_id++) {
        size_t offset = offsets[row_id - 1];
        size_t next_offset = offsets[row_id];
        result.add_child_offset(next_offset);
        RETURN_IF_ERROR(nested_serde->write_column_to_pb(nested_column, *child_element, offset,
                                                         next_offset));
    }
    return Status::OK();
}

Status DataTypeArraySerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& array_column = assert_cast<ColumnArray&>(column);
    auto& offsets = array_column.get_offsets();
    IColumn& nested_column = array_column.get_data();
    for (int i = 0; i < arg.child_offset_size(); ++i) {
        offsets.emplace_back(arg.child_offset(i));
    }
    for (int i = 0; i < arg.child_element_size(); ++i) {
        RETURN_IF_ERROR(nested_serde->read_column_from_pb(nested_column, arg.child_element(i)));
    }
    return Status::OK();
}

template <bool is_strict_mode>
Status DataTypeArraySerDe::_from_string(StringRef& str, IColumn& column,
                                        const FormatOptions& options) const {
    if (str.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }

    auto& array_column = assert_cast<ColumnArray&>(column);
    auto& offsets = array_column.get_offsets();
    IColumn& nested_column = array_column.get_data();
    DCHECK(nested_column.is_nullable());
    if (str.front() != '[') {
        return Status::InvalidArgument("Array does not start with '[' character, found '{}'",
                                       str.to_string());
    }
    if (str.back() != ']') {
        return Status::InvalidArgument("Array does not end with ']' character, found '{}'",
                                       str.to_string());
    }
    // empty array []
    if (str.size == 2) {
        auto last_off = offsets.back();
        offsets.push_back(last_off);
        return Status::OK();
    }
    str = str.substring(1, str.size - 2); // remove '[' and ']'

    auto split_result = ComplexTypeDeserializeUtil::split_by_delimiter(
            str, [&](char c) { return c == options.collection_delim; });

    for (auto& e : split_result) {
        RETURN_IF_ERROR(ComplexTypeDeserializeUtil::process_column<is_strict_mode>(
                nested_serde, nested_column, e.element, options));
    }

    offsets.emplace_back(offsets.back() + split_result.size());
    return Status::OK();
}

Status DataTypeArraySerDe::from_string(StringRef& str, IColumn& column,
                                       const FormatOptions& options) const {
    return _from_string<false>(str, column, options);
}

Status DataTypeArraySerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                   const FormatOptions& options) const {
    return _from_string<true>(str, column, options);
}

void DataTypeArraySerDe::write_one_cell_to_binary(const IColumn& src_column,
                                                  ColumnString::Chars& chars,
                                                  int64_t row_num) const {
    const uint8_t type = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_ARRAY);
    const size_t old_size = chars.size();
    const size_t new_size = old_size + sizeof(uint8_t) + sizeof(size_t);
    chars.resize(new_size);
    memcpy(chars.data() + old_size, reinterpret_cast<const char*>(&type), sizeof(uint8_t));

    const auto& array_col = assert_cast<const ColumnArray&>(src_column);
    const IColumn& nested_column = array_col.get_data();
    const auto& offsets = array_col.get_offsets();
    size_t start = offsets[row_num - 1];
    size_t end = offsets[row_num];
    size_t size = end - start;
    memcpy(chars.data() + old_size + sizeof(uint8_t), reinterpret_cast<const char*>(&size),
           sizeof(size_t));
    for (size_t offset = start; offset != end; ++offset) {
        nested_serde->write_one_cell_to_binary(nested_column, chars, offset);
    }
}

const uint8_t* DataTypeArraySerDe::deserialize_binary_to_column(const uint8_t* data,
                                                                IColumn& column) {
    auto& array_col = assert_cast<ColumnArray&, TypeCheckOnRelease::DISABLE>(column);
    auto& offsets = array_col.get_offsets();
    auto& nested_column = array_col.get_data();
    const size_t nested_size = unaligned_load<size_t>(data);
    data += sizeof(size_t);
    if (nested_size == 0) [[unlikely]] {
        offsets.push_back(offsets.back());
        return data;
    }

    for (size_t i = 0; i < nested_size; ++i) {
        const uint8_t* new_data = DataTypeSerDe::deserialize_binary_to_column(data, nested_column);
        data = new_data;
    }
    offsets.push_back(offsets.back() + nested_size);
    return data;
}

const uint8_t* DataTypeArraySerDe::deserialize_binary_to_field(const uint8_t* data, Field& field,
                                                               FieldInfo& info) {
    const size_t nested_size = unaligned_load<size_t>(data);
    data += sizeof(size_t);
    field = Field::create_field<TYPE_ARRAY>(Array(nested_size));
    info.num_dimensions++;
    auto& array = field.get<TYPE_ARRAY>();
    PrimitiveType nested_type = PrimitiveType::TYPE_NULL;
    for (size_t i = 0; i < nested_size; ++i) {
        Field nested_field;
        data = DataTypeSerDe::deserialize_binary_to_field(data, nested_field, info);
        array[i] = std::move(nested_field);
        if (info.scalar_type_id != PrimitiveType::TYPE_NULL) {
            nested_type = info.scalar_type_id;
        }
    }
    info.scalar_type_id = nested_type;
    return data;
}

void DataTypeArraySerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                   const FormatOptions& options) const {
    const auto& data_column = assert_cast<const ColumnArray&>(column);
    const auto& offsets = data_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& nested_column = data_column.get_data();
    bw.write("[", 1);
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            bw.write(", ", 2);
        }
        nested_serde->to_string(nested_column, i, bw, options);
    }
    bw.write("]", 1);
}

bool DataTypeArraySerDe::write_column_to_presto_text(const IColumn& column, BufferWritable& bw,
                                                     int64_t row_idx,
                                                     const FormatOptions& options) const {
    const auto& data_column = assert_cast<const ColumnArray&>(column);
    const auto& offsets = data_column.get_offsets();

    size_t offset = offsets[row_idx - 1];
    size_t next_offset = offsets[row_idx];

    const IColumn& nested_column = data_column.get_data();
    bw.write("[", 1);
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            bw.write(", ", 2);
        }
        nested_serde->write_column_to_presto_text(nested_column, bw, i, options);
    }
    bw.write("]", 1);
    return true;
}

bool DataTypeArraySerDe::write_column_to_hive_text(const IColumn& column, BufferWritable& bw,
                                                   int64_t row_idx,
                                                   const FormatOptions& options) const {
    const auto& data_column = assert_cast<const ColumnArray&>(column);
    const auto& offsets = data_column.get_offsets();

    size_t offset = offsets[row_idx - 1];
    size_t next_offset = offsets[row_idx];

    const IColumn& nested_column = data_column.get_data();
    bw.write("[", 1);
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            bw.write(",", 1);
        }
        nested_serde->write_column_to_hive_text(nested_column, bw, i, options);
    }
    bw.write("]", 1);
    return true;
}

} // namespace doris::vectorized
