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

#include "data_type_map_serde.h"

#include "arrow/array/builder_nested.h"
#include "common/exception.h"
#include "common/status.h"
#include "complex_type_deserialize_util.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Arena;
#include "common/compile_check_begin.h"
Status DataTypeMapSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                  int64_t end_idx, BufferWritable& bw,
                                                  FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeMapSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                    BufferWritable& bw,
                                                    FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& map_column = assert_cast<const ColumnMap&>(*ptr);
    const ColumnArray::Offsets64& offsets = map_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    bw.write("{", 1);
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            bw.write(&options.collection_delim, 1);
            bw.write(" ", 1);
        }
        RETURN_IF_ERROR(key_serde->serialize_one_cell_to_json(nested_keys_column, i, bw, options));
        bw.write(&options.map_key_delim, 1);
        RETURN_IF_ERROR(
                value_serde->serialize_one_cell_to_json(nested_values_column, i, bw, options));
    }
    bw.write("}", 1);
    return Status::OK();
}

Status DataTypeMapSerDe::deserialize_one_cell_from_hive_text(
        IColumn& column, Slice& slice, const FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    auto& array_column = assert_cast<ColumnMap&>(column);
    auto& offsets = array_column.get_offsets();
    IColumn& nested_key_column = array_column.get_keys();
    IColumn& nested_val_column = array_column.get_values();
    DCHECK(nested_key_column.is_nullable());
    DCHECK(nested_val_column.is_nullable());

    char collection_delimiter =
            options.get_collection_delimiter(hive_text_complex_type_delimiter_level);
    char map_kv_delimiter =
            options.get_collection_delimiter(hive_text_complex_type_delimiter_level + 1);

    std::vector<Slice> key_slices;
    std::vector<Slice> value_slices;

    for (size_t i = 0, from = 0, kv = 0; i <= slice.size; i++) {
        /*
         *  In hive , when you special map key and value delimiter as ':'
         *  for map<int,timestamp> column , the query result is correct , but
         *  for map<timestamp, int> column and map<timestamp,timestamp> column , the query result is incorrect,
         *  because this field have many '_map_kv_delimiter'.
         *
         *  So i use 'kv <= from' in order to get _map_kv_delimiter that appears first.
         * */
        if (i < slice.size && slice[i] == map_kv_delimiter && kv <= from &&
            (options.escape_char == 0 || i == 0 || slice[i - 1] != options.escape_char)) {
            kv = i;
            continue;
        }
        if ((i == slice.size || slice[i] == collection_delimiter) && i >= kv + 1) {
            if (options.escape_char != 0 && i > 0 && slice[i - 1] == options.escape_char) {
                continue;
            }
            key_slices.emplace_back(slice.data + from, kv - from);
            value_slices.emplace_back(slice.data + kv + 1, i - 1 - kv);
            from = i + 1;
            kv = from;
        }
    }

    uint64_t num_keys = 0, num_values = 0;
    Status st;
    st = key_serde->deserialize_column_from_hive_text_vector(
            nested_key_column, key_slices, &num_keys, options,
            hive_text_complex_type_delimiter_level + 2);
    if (st != Status::OK()) {
        return st;
    }

    st = value_serde->deserialize_column_from_hive_text_vector(
            nested_val_column, value_slices, &num_values, options,
            hive_text_complex_type_delimiter_level + 2);
    if (st != Status::OK()) {
        return st;
    }

    CHECK(num_keys == num_values);

    offsets.push_back(offsets.back() + num_values);
    return Status::OK();
}

Status DataTypeMapSerDe::deserialize_column_from_hive_text_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options, int hive_text_complex_type_delimiter_level) const {
    DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR();
    return Status::OK();
}

Status DataTypeMapSerDe::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& map_column = assert_cast<const ColumnMap&>(*ptr);
    const ColumnArray::Offsets64& offsets = map_column.get_offsets();

    size_t start = offsets[row_num - 1];
    size_t end = offsets[row_num];

    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();

    char collection_delimiter =
            options.get_collection_delimiter(hive_text_complex_type_delimiter_level);
    char map_kv_delimiter =
            options.get_collection_delimiter(hive_text_complex_type_delimiter_level + 1);

    for (size_t i = start; i < end; ++i) {
        if (i != start) {
            bw.write(collection_delimiter);
        }
        RETURN_IF_ERROR(key_serde->serialize_one_cell_to_hive_text(
                nested_keys_column, i, bw, options, hive_text_complex_type_delimiter_level + 2));
        bw.write(map_kv_delimiter);
        RETURN_IF_ERROR(value_serde->serialize_one_cell_to_hive_text(
                nested_values_column, i, bw, options, hive_text_complex_type_delimiter_level + 2));
    }
    return Status::OK();
}

Status DataTypeMapSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                             std::vector<Slice>& slices,
                                                             uint64_t* num_deserialized,
                                                             const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

Status DataTypeMapSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                        const FormatOptions& options) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    auto& array_column = assert_cast<ColumnMap&>(column);
    auto& offsets = array_column.get_offsets();
    IColumn& nested_key_column = array_column.get_keys();
    IColumn& nested_val_column = array_column.get_values();
    DCHECK(nested_key_column.is_nullable());
    DCHECK(nested_val_column.is_nullable());
    if (slice[0] != '{') {
        std::stringstream ss;
        ss << slice[0] << '\'';
        return Status::InvalidArgument("Map does not start with '{' character, found '" + ss.str());
    }
    if (slice[slice.size - 1] != '}') {
        std::stringstream ss;
        ss << slice[slice.size - 1] << '\'';
        return Status::InvalidArgument("Map does not end with '}' character, found '" + ss.str());
    }
    // empty map
    if (slice.size == 2) {
        auto last_off = offsets.back();
        offsets.push_back(last_off);
        return Status::OK();
    }

    // remove '{' '}'
    slice.remove_prefix(1);
    slice.remove_suffix(1);
    slice.trim_prefix();

    // deserialize map column from text we have to know how to split from text and support nested
    //  complex type.
    //   1. get item according to collection_delimiter, but if meet collection_delimiter in string, we should ignore it.
    //   2. get kv according map_key_delimiter, but if meet map_key_delimiter in string, we should ignore it.
    //   3. keep a nested level to support nested complex type.
    int nested_level = 0;
    bool has_quote = false;
    int start_pos = 0;
    size_t slice_size = slice.size;
    bool key_added = false;
    int idx = 0;
    int elem_deserialized = 0;
    char quote_char = 0;
    for (; idx < slice_size; ++idx) {
        char c = slice[idx];
        if (c == '"' || c == '\'') {
            if (!has_quote) {
                quote_char = c;
                has_quote = !has_quote;
            } else if (has_quote && quote_char == c) {
                quote_char = 0;
                has_quote = !has_quote;
            }
        } else if (c == '\\' && idx + 1 < slice_size) { //escaped
            ++idx;
        } else if (!has_quote && (c == '[' || c == '{')) {
            ++nested_level;
        } else if (!has_quote && (c == ']' || c == '}')) {
            --nested_level;
        } else if (!has_quote && nested_level == 0 && c == options.map_key_delim && !key_added) {
            // if meet map_key_delimiter and not in quote, we can make it as key elem.
            if (idx == start_pos) {
                continue;
            }
            Slice next(slice.data + start_pos, idx - start_pos);
            next.trim_prefix();
            if (Status st =
                        key_serde->deserialize_one_cell_from_json(nested_key_column, next, options);
                !st.ok()) {
                nested_key_column.pop_back(elem_deserialized);
                nested_val_column.pop_back(elem_deserialized);
                return st;
            }
            // skip delimiter
            start_pos = idx + 1;
            key_added = true;
        } else if (!has_quote && nested_level == 0 && c == options.collection_delim && key_added) {
            // if meet collection_delimiter and not in quote, we can make it as value elem
            if (idx == start_pos) {
                continue;
            }
            Slice next(slice.data + start_pos, idx - start_pos);
            next.trim_prefix();

            if (Status st = value_serde->deserialize_one_cell_from_json(nested_val_column, next,
                                                                        options);
                !st.ok()) {
                nested_key_column.pop_back(elem_deserialized + 1);
                nested_val_column.pop_back(elem_deserialized);
                return st;
            }
            // skip delimiter
            start_pos = idx + 1;
            // reset key_added
            key_added = false;
            ++elem_deserialized;
        }
    }
    // for last value elem
    if (!has_quote && nested_level == 0 && idx == slice_size && idx != start_pos && key_added) {
        Slice next(slice.data + start_pos, idx - start_pos);
        next.trim_prefix();

        if (Status st =
                    value_serde->deserialize_one_cell_from_json(nested_val_column, next, options);
            !st.ok()) {
            nested_key_column.pop_back(elem_deserialized + 1);
            nested_val_column.pop_back(elem_deserialized);
            return st;
        }
        ++elem_deserialized;
    }

    if (nested_key_column.size() != nested_val_column.size()) {
        // nested key and value should always same size otherwise we should popback wrong data
        nested_key_column.pop_back(nested_key_column.size() - offsets.back());
        nested_val_column.pop_back(nested_val_column.size() - offsets.back());
        DCHECK(nested_key_column.size() == nested_val_column.size());
        return Status::InvalidArgument(
                "deserialize map error key_size({}) not equal to value_size{}",
                nested_key_column.size(), nested_val_column.size());
    }
    offsets.emplace_back(offsets.back() + elem_deserialized);
    return Status::OK();
}

void DataTypeMapSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    const auto* blob = arg->unpack<JsonbBinaryVal>();
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

void DataTypeMapSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                               Arena& arena, int32_t col_id,
                                               int64_t row_num) const {
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    const char* begin = nullptr;
    // maybe serialize_value_into_arena should move to here later.
    StringRef value = column.serialize_value_into_arena(row_num, arena, begin);
    result.writeStartBinary();
    result.writeBinary(value.data, value.size);
    result.writeEndBinary();
}

Status DataTypeMapSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                               arrow::ArrayBuilder* array_builder, int64_t start,
                                               int64_t end, const cctz::time_zone& ctz) const {
    auto& builder = assert_cast<arrow::MapBuilder&>(*array_builder);
    const auto& map_column = assert_cast<const ColumnMap&>(column);
    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    // now we default set key value in map is nullable
    DCHECK(nested_keys_column.is_nullable());
    DCHECK(nested_values_column.is_nullable());
    const auto* keys_nullmap_data =
            check_and_get_column<ColumnNullable>(nested_keys_column)->get_null_map_data().data();
    const auto& offsets = map_column.get_offsets();
    auto* key_builder = builder.key_builder();
    auto* value_builder = builder.item_builder();

    for (size_t r = start; r < end; ++r) {
        if ((null_map && (*null_map)[r])) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else if (simd::contain_byte(keys_nullmap_data + offsets[r - 1],
                                      offsets[r] - offsets[r - 1], 1)) {
            // arrow do not support key is null, so we ignore the null key-value
            MutableColumnPtr key_mutable_data = nested_keys_column.clone_empty();
            MutableColumnPtr value_mutable_data = nested_values_column.clone_empty();
            for (size_t i = offsets[r - 1]; i < offsets[r]; ++i) {
                if (keys_nullmap_data[i] == 1) {
                    return Status::Error(ErrorCode::INVALID_ARGUMENT,
                                         "Can not write null value of map key to arrow.");
                }
                key_mutable_data->insert_from(nested_keys_column, i);
                value_mutable_data->insert_from(nested_values_column, i);
            }
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(), column.get_name(),
                                             array_builder->type()->name()));

            RETURN_IF_ERROR(key_serde->write_column_to_arrow(
                    *key_mutable_data, nullptr, key_builder, 0, key_mutable_data->size(), ctz));
            RETURN_IF_ERROR(value_serde->write_column_to_arrow(*value_mutable_data, nullptr,
                                                               value_builder, 0,
                                                               value_mutable_data->size(), ctz));
        } else {
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(), column.get_name(),
                                             array_builder->type()->name()));
            RETURN_IF_ERROR(key_serde->write_column_to_arrow(
                    nested_keys_column, nullptr, key_builder, offsets[r - 1], offsets[r], ctz));
            RETURN_IF_ERROR(value_serde->write_column_to_arrow(
                    nested_values_column, nullptr, value_builder, offsets[r - 1], offsets[r], ctz));
        }
    }
    return Status::OK();
}

Status DataTypeMapSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                int64_t start, int64_t end,
                                                const cctz::time_zone& ctz) const {
    auto& column_map = static_cast<ColumnMap&>(column);
    auto& offsets_data = column_map.get_offsets();
    const auto* concrete_map = dynamic_cast<const arrow::MapArray*>(arrow_array);
    auto arrow_offsets_array = concrete_map->offsets();
    auto* arrow_offsets = dynamic_cast<arrow::Int32Array*>(arrow_offsets_array.get());
    auto prev_size = offsets_data.back();
    auto arrow_nested_start_offset = arrow_offsets->Value(start);
    auto arrow_nested_end_offset = arrow_offsets->Value(end);
    for (int64_t i = start + 1; i < end + 1; ++i) {
        // convert to doris offset, start from offsets.back()
        offsets_data.emplace_back(prev_size + arrow_offsets->Value(i) - arrow_nested_start_offset);
    }
    RETURN_IF_ERROR(key_serde->read_column_from_arrow(
            column_map.get_keys(), concrete_map->keys().get(), arrow_nested_start_offset,
            arrow_nested_end_offset, ctz));
    RETURN_IF_ERROR(value_serde->read_column_from_arrow(
            column_map.get_values(), concrete_map->items().get(), arrow_nested_start_offset,
            arrow_nested_end_offset, ctz));
    return Status::OK();
}

template <bool is_binary_format>
Status DataTypeMapSerDe::_write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<is_binary_format>& result,
                                                int64_t row_idx, bool col_const,
                                                const FormatOptions& options) const {
    const auto& map_column = assert_cast<const ColumnMap&>(column);
    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    bool is_key_string = remove_nullable(nested_keys_column.get_ptr())->is_column_string();
    bool is_val_string = remove_nullable(nested_values_column.get_ptr())->is_column_string();

    const auto col_index = index_check_const(row_idx, col_const);
    result.open_dynamic_mode();
    if (0 != result.push_string("{", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    const auto& offsets = map_column.get_offsets();
    for (auto j = offsets[col_index - 1]; j < offsets[col_index]; ++j) {
        if (j != offsets[col_index - 1]) {
            if (0 != result.push_string(options.mysql_collection_delim.c_str(),
                                        options.mysql_collection_delim.size())) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }
        if (nested_keys_column.is_null_at(j)) {
            if (0 != result.push_string(options.null_format, options.null_len)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (is_key_string && options.wrapper_len > 0) {
                if (0 != result.push_string(options.nested_string_wrapper, options.wrapper_len)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(key_serde->write_column_to_mysql(nested_keys_column, result, j,
                                                                 false, options));
                if (0 != result.push_string(options.nested_string_wrapper, options.wrapper_len)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(key_serde->write_column_to_mysql(nested_keys_column, result, j,
                                                                 false, options));
            }
        }
        if (0 != result.push_string(&options.map_key_delim, 1)) {
            return Status::InternalError("pack mysql buffer failed.");
        }
        if (nested_values_column.is_null_at(j)) {
            if (0 != result.push_string(options.null_format, options.null_len)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (is_val_string && options.wrapper_len > 0) {
                if (0 != result.push_string(options.nested_string_wrapper, options.wrapper_len)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(value_serde->write_column_to_mysql(nested_values_column, result, j,
                                                                   false, options));
                if (0 != result.push_string(options.nested_string_wrapper, options.wrapper_len)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(value_serde->write_column_to_mysql(nested_values_column, result, j,
                                                                   false, options));
            }
        }
    }
    if (0 != result.push_string("}", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    result.close_dynamic_mode();
    return Status::OK();
}

Status DataTypeMapSerDe::write_column_to_mysql(const IColumn& column,
                                               MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
                                               bool col_const, const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeMapSerDe::write_column_to_mysql(const IColumn& column,
                                               MysqlRowBuffer<false>& row_buffer, int64_t row_idx,
                                               bool col_const, const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeMapSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                             const NullMap* null_map,
                                             orc::ColumnVectorBatch* orc_col_batch, int64_t start,
                                             int64_t end, vectorized::Arena& arena) const {
    auto* cur_batch = dynamic_cast<orc::MapVectorBatch*>(orc_col_batch);
    cur_batch->offsets[0] = 0;

    const auto& map_column = assert_cast<const ColumnMap&>(column);
    const ColumnArray::Offsets64& offsets = map_column.get_offsets();
    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    for (size_t row_id = start; row_id < end; row_id++) {
        size_t offset = offsets[row_id - 1];
        size_t next_offset = offsets[row_id];

        RETURN_IF_ERROR(key_serde->write_column_to_orc(timezone, nested_keys_column, nullptr,
                                                       cur_batch->keys.get(), offset, next_offset,
                                                       arena));
        RETURN_IF_ERROR(value_serde->write_column_to_orc(timezone, nested_values_column, nullptr,
                                                         cur_batch->elements.get(), offset,
                                                         next_offset, arena));

        cur_batch->offsets[row_id + 1] = next_offset;
    }
    cur_batch->keys->numElements = nested_keys_column.size();
    cur_batch->elements->numElements = nested_values_column.size();

    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeMapSerDe::write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                                            int64_t end) const {
    const auto& map_column = assert_cast<const ColumnMap&>(column);
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::MAP);
    const ColumnArray::Offsets64& offsets = map_column.get_offsets();
    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    auto* key_child_element = result.add_child_element();
    auto* value_child_element = result.add_child_element();
    for (size_t row_id = start; row_id < end; row_id++) {
        size_t offset = offsets[row_id - 1];
        size_t next_offset = offsets[row_id];
        result.add_child_offset(next_offset);
        RETURN_IF_ERROR(key_serde->write_column_to_pb(nested_keys_column, *key_child_element,
                                                      offset, next_offset));
        RETURN_IF_ERROR(value_serde->write_column_to_pb(nested_values_column, *value_child_element,
                                                        offset, next_offset));
    }
    return Status::OK();
}

Status DataTypeMapSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& map_column = assert_cast<ColumnMap&>(column);
    auto& offsets = map_column.get_offsets();
    auto& key_column = map_column.get_keys();
    auto& value_column = map_column.get_values();
    for (int i = 0; i < arg.child_offset_size(); ++i) {
        offsets.emplace_back(arg.child_offset(i));
    }
    for (int i = 0; i < arg.child_element_size(); i = i + 2) {
        RETURN_IF_ERROR(key_serde->read_column_from_pb(key_column, arg.child_element(i)));
        RETURN_IF_ERROR(value_serde->read_column_from_pb(value_column, arg.child_element(i + 1)));
    }
    return Status::OK();
}

template <bool is_strict_mode>
Status DataTypeMapSerDe::_from_string(StringRef& str, IColumn& column,
                                      const FormatOptions& options) const {
    if (str.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    auto& map_column = assert_cast<ColumnMap&>(column);
    auto& offsets = map_column.get_offsets();
    IColumn& nested_key_column = map_column.get_keys();
    IColumn& nested_val_column = map_column.get_values();
    DCHECK(nested_key_column.is_nullable());
    DCHECK(nested_val_column.is_nullable());
    if (str.front() != '{') {
        std::stringstream ss;
        ss << str.front() << '\'';
        return Status::InvalidArgument("Map does not start with '{' character, found '" + ss.str());
    }
    if (str.back() != '}') {
        std::stringstream ss;
        ss << str.back() << '\'';
        return Status::InvalidArgument("Map does not end with '}' character, found '" + ss.str());
    }
    // empty map
    if (str.size == 2) {
        auto last_off = offsets.back();
        offsets.push_back(last_off);
        return Status::OK();
    }
    str = str.substring(1, str.size - 2); // remove '{' '}'

    auto split_result = ComplexTypeDeserializeUtil::split_by_delimiter(str, [&](char c) {
        return c == options.map_key_delim || c == options.collection_delim;
    });

    // check syntax error
    if (split_result.size() % 2 != 0) {
        return Status::InvalidArgument("Map does not have even number of key-value pairs");
    }

    std::vector<StringRef> key_slices;
    std::vector<StringRef> value_slices;

    for (int i = 0; i < split_result.size(); i += 2) {
        // : , : , : , :
        if (split_result[i].delimiter != options.map_key_delim) {
            return Status::InvalidArgument("Map key-value pair does not have map key delimiter");
        }
        if (i != 0 && split_result[i - 1].delimiter != options.collection_delim) {
            return Status::InvalidArgument("Map key-value pair does not have collection delimiter");
        }
        key_slices.push_back(split_result[i].element);
        value_slices.push_back(split_result[i + 1].element);
    }

    DCHECK(nested_key_column.is_nullable());
    DCHECK(nested_val_column.is_nullable());
    DCHECK_EQ(key_slices.size(), value_slices.size());
    uint64_t key_value_size = key_slices.size();
    for (int i = 0; i < key_value_size; i++) {
        RETURN_IF_ERROR(ComplexTypeDeserializeUtil::process_column<is_strict_mode>(
                key_serde, nested_key_column, key_slices[i], options));
        RETURN_IF_ERROR(ComplexTypeDeserializeUtil::process_column<is_strict_mode>(
                value_serde, nested_val_column, value_slices[i], options));
    }
    offsets.push_back(offsets.back() + key_value_size);
    return Status::OK();
}

Status DataTypeMapSerDe::from_string(StringRef& str, IColumn& column,
                                     const FormatOptions& options) const {
    return _from_string<false>(str, column, options);
}
Status DataTypeMapSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                 const FormatOptions& options) const {
    return _from_string<true>(str, column, options);
}

Status DataTypeMapSerDe::serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                                   JsonbWriter& writer) const {
    const auto& map_column = assert_cast<const ColumnMap&>(from_column);
    const ColumnArray::Offsets64& offsets = map_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& keys_column = map_column.get_keys();
    const IColumn& values_column = map_column.get_values();

    DCHECK(keys_column.is_nullable());
    DCHECK(values_column.is_nullable());

    const auto* key_string_column = check_and_get_column<ColumnString>(
            assert_cast<const ColumnNullable&>(keys_column).get_nested_column());

    if (key_string_column == nullptr) {
        return Status::InternalError("Cast to Jsonb failed, map key is not string type");
    }

    if (!writer.writeStartObject()) {
        return Status::InternalError("writeStartObject failed");
    }
    for (size_t i = offset; i < next_offset; ++i) {
        auto key_str = key_string_column->get_data_at(i);
        // check key size
        if (key_str.size > std::numeric_limits<uint8_t>::max()) {
            return Status::InternalError("key size exceeds max limit {} ", key_str.to_string());
        }
        // write key
        if (!writer.writeKey(key_str.data, (uint8_t)key_str.size)) {
            return Status::InternalError("writeKey failed : {}", key_str.to_string());
        }
        // write value
        RETURN_IF_ERROR(value_serde->serialize_column_to_jsonb(values_column, i, writer));
    }

    if (!writer.writeEndObject()) {
        return Status::InternalError("writeEndObject failed");
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
