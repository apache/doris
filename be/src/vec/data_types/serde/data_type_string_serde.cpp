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

#include "data_type_string_serde.h"

#include "runtime/define_primitive_type.h"
#include "util/jsonb_document_cast.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column_string.h"

namespace doris::vectorized {

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_column_to_json(const IColumn& column,
                                                                     int64_t start_idx,
                                                                     int64_t end_idx,
                                                                     BufferWritable& bw,
                                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_one_cell_to_json(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    const auto& value = assert_cast<const ColumnType&>(*ptr).get_data_at(row_num);

    if (_nesting_level > 1) {
        bw.write('"');
    }
    if constexpr (std::is_same_v<ColumnType, ColumnString>) {
        if (options.escape_char != 0) {
            // we should make deal with some special characters in json str if we have escape_char
            StringRef str_ref = value;
            write_with_escaped_char_to_json(str_ref, bw);
        } else {
            bw.write(value.data, value.size);
        }
    } else {
        bw.write(value.data, value.size);
    }
    if (_nesting_level > 1) {
        bw.write('"');
    }

    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    const auto& value = assert_cast<const ColumnType&>(*ptr).get_data_at(row_num);
    if constexpr (std::is_same_v<ColumnType, ColumnString>) {
        if (options.escape_char != 0) {
            StringRef str_ref = value;
            write_with_escaped_char_to_hive_text(str_ref, bw, options.escape_char,
                                                 options.need_escape);
        } else {
            bw.write(value.data, value.size);
        }
    } else {
        bw.write(value.data, value.size);
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice, const FormatOptions& options) const {
    /*
         * For strings in the json complex type, we remove double quotes by default.
         *
         * Because when querying complex types, such as selecting complexColumn from table,
         * we will add double quotes to the strings in the complex type.
         *
         * For the map<string,int> column, insert { "abc" : 1, "hello",2 }.
         * If you do not remove the double quotes, it will display {""abc"":1,""hello"": 2 },
         * remove the double quotes to display { "abc" : 1, "hello",2 }.
         *
         */
    if (_nesting_level >= 2) {
        slice.trim_quote();
    }
    if (options.escape_char != 0) {
        escape_string(slice.data, &slice.size, options.escape_char);
    }
    assert_cast<ColumnType&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_one_cell_from_csv(
        IColumn& column, Slice& slice, const FormatOptions& options) const {
    if (options.escape_char != 0) {
        escape_string_for_csv(slice.data, &slice.size, options.escape_char, options.quote_char);
    }
    assert_cast<ColumnType&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_one_cell_from_hive_text(
        IColumn& column, Slice& slice, const FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    if (options.escape_char != 0) {
        escape_string(slice.data, &slice.size, options.escape_char);
    }
    assert_cast<ColumnType&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_pb(const IColumn& column,
                                                               PValues& result, int64_t start,
                                                               int64_t end) const {
    result.mutable_string_value()->Reserve(cast_set<int>(end - start));
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::STRING);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef data = column.get_data_at(row_num);
        result.add_string_value(data.to_string());
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, uint64_t rows, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeStringSerDeBase::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

template <typename ColumnType>
void DataTypeStringSerDeBase<ColumnType>::insert_column_last_value_multiple_times(
        IColumn& column, uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = static_cast<ColumnString&>(column);
    auto sz = col.size();

    StringRef ref = col.get_data_at(sz - 1);
    String str(ref.data, ref.size);
    std::vector<StringRef> refs(times, {str.data(), str.size()});

    col.insert_many_strings(refs.data(), refs.size());
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_column_from_pb(IColumn& column,
                                                                const PValues& arg) const {
    auto& column_dest = assert_cast<ColumnType&>(column);
    column_dest.reserve(column_dest.size() + arg.string_value_size());
    for (int i = 0; i < arg.string_value_size(); ++i) {
        column_dest.insert_data(arg.string_value(i).c_str(), arg.string_value(i).size());
    }
    return Status::OK();
}

template <typename ColumnType>
void DataTypeStringSerDeBase<ColumnType>::write_one_cell_to_jsonb(const IColumn& column,
                                                                  JsonbWriter& result,
                                                                  Arena& mem_pool, int32_t col_id,
                                                                  int64_t row_num) const {
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    const auto& data_ref = column.get_data_at(row_num);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(data_ref.data), data_ref.size);
    result.writeEndBinary();
}

template <typename ColumnType>
void DataTypeStringSerDeBase<ColumnType>::read_one_cell_from_jsonb(IColumn& column,
                                                                   const JsonbValue* arg) const {
    assert(arg->isBinary());
    const auto* blob = arg->unpack<JsonbBinaryVal>();
    assert_cast<ColumnType&>(column).insert_data(blob->getBlob(), blob->getBlobLen());
}

template <typename ColumnType>
template <typename BuilderType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_arrow_impl(const IColumn& column,
                                                                       const NullMap* null_map,
                                                                       BuilderType& builder,
                                                                       int64_t start,
                                                                       int64_t end) const {
    const auto& string_column = assert_cast<const ColumnType&>(column);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && (*null_map)[string_i]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                             builder.type()->name()));
            continue;
        }
        auto string_ref = string_column.get_data_at(string_i);
        RETURN_IF_ERROR(checkArrowStatus(
                builder.Append(string_ref.data, cast_set<int, size_t, false>(string_ref.size)),
                column.get_name(), builder.type()->name()));
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_arrow(
        const IColumn& column, const NullMap* null_map, arrow::ArrayBuilder* array_builder,
        int64_t start, int64_t end, const cctz::time_zone& ctz) const {
    if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
        auto& builder = assert_cast<arrow::LargeStringBuilder&>(*array_builder);
        return write_column_to_arrow_impl(column, null_map, builder, start, end);
    } else if (array_builder->type()->id() == arrow::Type::STRING) {
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        return write_column_to_arrow_impl(column, null_map, builder, start, end);
    } else {
        return Status::InvalidArgument("Unsupported arrow type for string column: {}",
                                       array_builder->type()->name());
    }
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_column_from_arrow(
        IColumn& column, const arrow::Array* arrow_array, int64_t start, int64_t end,
        const cctz::time_zone& ctz) const {
    if (arrow_array->type_id() == arrow::Type::STRING ||
        arrow_array->type_id() == arrow::Type::BINARY) {
        const auto* concrete_array = dynamic_cast<const arrow::BinaryArray*>(arrow_array);
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

        for (auto offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
                assert_cast<ColumnType&>(column).insert_data(
                        (char*)raw_data, concrete_array->value_length(offset_i));
            } else {
                assert_cast<ColumnType&>(column).insert_default();
            }
        }
    } else if (arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
        const auto* concrete_array = dynamic_cast<const arrow::FixedSizeBinaryArray*>(arrow_array);
        uint32_t width = concrete_array->byte_width();
        const auto* array_data = concrete_array->GetValue(start);

        for (size_t offset_i = 0; offset_i < end - start; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = array_data + (offset_i * width);
                assert_cast<ColumnType&>(column).insert_data((char*)raw_data, width);
            } else {
                assert_cast<ColumnType&>(column).insert_default();
            }
        }
    } else if (arrow_array->type_id() == arrow::Type::LARGE_STRING ||
               arrow_array->type_id() == arrow::Type::LARGE_BINARY) {
        const auto* concrete_array = dynamic_cast<const arrow::LargeBinaryArray*>(arrow_array);
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

        for (auto offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
                assert_cast<ColumnType&>(column).insert_data(
                        (char*)raw_data, concrete_array->value_length(offset_i));
            } else {
                assert_cast<ColumnType&>(column).insert_default();
            }
        }
    } else {
        return Status::InvalidArgument("Unsupported arrow type for string column: {}",
                                       arrow_array->type_id());
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<true>& row_buffer, int64_t row_idx, bool col_const,
        const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<false>& row_buffer, int64_t row_idx, bool col_const,
        const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_orc(
        const std::string& timezone, const IColumn& column, const NullMap* null_map,
        orc::ColumnVectorBatch* orc_col_batch, int64_t start, int64_t end,
        vectorized::Arena& arena) const {
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    for (auto row_id = start; row_id < end; row_id++) {
        const auto& ele = assert_cast<const ColumnType&>(column).get_data_at(row_id);
        cur_batch->data[row_id] = const_cast<char*>(ele.data);
        cur_batch->length[row_id] = ele.size;
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_column_to_jsonb(const IColumn& from_column,
                                                                      int64_t row_num,
                                                                      JsonbWriter& writer) const {
    if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
        return Status::NotSupported(
                "DataTypeStringSerDeBase only supports ColumnString for serialize_column_to_jsonb");
    }
    const auto& data = assert_cast<const ColumnString&>(from_column).get_data_at(row_num);

    // start writing string
    if (!writer.writeStartString()) {
        return Status::InternalError("writeStartString failed");
    }

    // write string
    if (data.size > 0) {
        if (writer.writeString(data.data, data.size) == 0) {
            return Status::InternalError("writeString failed");
        }
    }

    // end writing string
    if (!writer.writeEndString()) {
        return Status::InternalError("writeEndString failed");
    }

    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_jsonb(
        IColumn& column, const JsonbValue* jsonb_value, CastParameters& castParms) const {
    if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
        return Status::NotSupported(
                "DataTypeStringSerDeBase only supports ColumnString for "
                "deserialize_column_from_jsonb");
    }
    auto& col_str = assert_cast<ColumnString&>(column);
    std::string str;
    if (jsonb_value->isString()) {
        const auto* blob = jsonb_value->unpack<JsonbBinaryVal>();
        str.assign(blob->getBlob(), blob->getBlobLen());
    } else {
        str = JsonbToJson {}.to_json_string(jsonb_value);
    }
    col_str.insert_value(str);

    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_jsonb_vector(
        ColumnNullable& column_to, const ColumnString& col_from_json,
        CastParameters& castParms) const {
    if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
        return Status::NotSupported(
                "DataTypeStringSerDeBase only supports ColumnString for "
                "deserialize_column_from_jsonb_vector");
    }

    const size_t size = col_from_json.size();

    auto& null_map = column_to.get_null_map_data();
    auto& col_str = assert_cast<ColumnString&>(column_to.get_nested_column());

    null_map.resize_fill(size, false);
    col_str.get_offsets().reserve(size);
    col_str.get_chars().reserve(col_from_json.get_chars().size());

    for (size_t i = 0; i < size; ++i) {
        const auto& val = col_from_json.get_data_at(i);
        auto* jsonb_value = handle_jsonb_value(val);
        if (!jsonb_value) {
            null_map[i] = true;
            col_str.insert_default();
            continue;
        }
        if (jsonb_value->isString()) {
            const auto* blob = jsonb_value->unpack<JsonbBinaryVal>();
            col_str.insert_data(blob->getBlob(), blob->getBlobLen());
        } else {
            col_str.insert_value(JsonbToJson {}.to_json_string(jsonb_value));
        }
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::from_string(StringRef& str, IColumn& column,
                                                        const FormatOptions& options) const {
    auto slice = str.to_slice();
    return deserialize_one_cell_from_json(column, slice, options);
}

template class DataTypeStringSerDeBase<ColumnString>;
template class DataTypeStringSerDeBase<ColumnString64>;
template class DataTypeStringSerDeBase<ColumnFixedLengthObject>;

} // namespace doris::vectorized