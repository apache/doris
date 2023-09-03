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

#include "util/jsonb_document.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"

namespace doris {

namespace vectorized {
class Arena;

void DataTypeArraySerDe::serialize_column_to_text(const IColumn& column, int start_idx, int end_idx,
                                                  BufferWritable& bw,
                                                  FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_TEXT()
}

void DataTypeArraySerDe::serialize_one_cell_to_text(const IColumn& column, int row_num,
                                                    BufferWritable& bw,
                                                    FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& data_column = assert_cast<const ColumnArray&>(*ptr);
    auto& offsets = data_column.get_offsets();
    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& nested_column = data_column.get_data();
    //    bool is_nested_string = remove_nullable(nested_column.get_ptr())->is_column_string();

    bw.write("[", 1);
    // nested column field delim should be replaced as collection delim because this field is in array.
    //  add ' ' to keep same with origin format with array
    options.field_delim = options.collection_delim;
    options.field_delim += " ";
    nested_serde->serialize_column_to_text(nested_column, offset, next_offset, bw, options);
    bw.write("]", 1);
}

Status DataTypeArraySerDe::deserialize_column_from_text_vector(IColumn& column,
                                                               std::vector<Slice>& slices,
                                                               int* num_deserialized,
                                                               const FormatOptions& options) const {
    DCHECK(!slices.empty());
    int end = num_deserialized && *num_deserialized > 0 ? *num_deserialized : slices.size();

    for (int i = 0; i < end; ++i) {
        if (Status st = deserialize_one_cell_from_text(column, slices[i], options);
            st != Status::OK()) {
            *num_deserialized = i + 1;
            return st;
        }
    }
    return Status::OK();
}

Status DataTypeArraySerDe::deserialize_one_cell_from_text(IColumn& column, Slice& slice,
                                                          const FormatOptions& options) const {
    DCHECK(!slice.empty());
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
        offsets.push_back(offsets.back());
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
    slice.trim_prefix();
    slices.emplace_back(slice);
    size_t slice_size = slice.size;
    // pre add total slice can reduce lasted element check.
    for (int idx = 0; idx < slice_size; ++idx) {
        char c = slice[idx];
        if (c == '"' || c == '\'') {
            has_quote = !has_quote;
        } else if (!has_quote && (c == '[' || c == '{')) {
            ++nested_level;
        } else if (!has_quote && (c == ']' || c == '}')) {
            --nested_level;
        } else if (!has_quote && nested_level == 0 && c == options.collection_delim) {
            // if meet collection_delimiter and not in quote, we can make it as an item.
            slices.back().remove_suffix(slice_size - idx);
            // add next total slice.(slice data will not change, so we can use slice directly)
            // skip delimiter
            Slice next(slice.data + idx + 1, slice_size - idx - 1);
            next.trim_prefix();
            if (options.converted_from_string) slices.back().trim_quote();
            slices.emplace_back(next);
        }
    }

    if (options.converted_from_string) slices.back().trim_quote();

    int elem_deserialized = 0;
    Status st = nested_serde->deserialize_column_from_text_vector(nested_column, slices,
                                                                  &elem_deserialized, options);
    offsets.emplace_back(offsets.back() + elem_deserialized);
    return st;
}

void DataTypeArraySerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                 Arena* mem_pool, int32_t col_id,
                                                 int row_num) const {
    result.writeKey(col_id);
    const char* begin = nullptr;
    // maybe serialize_value_into_arena should move to here later.
    StringRef value = column.serialize_value_into_arena(row_num, *mem_pool, begin);
    result.writeStartBinary();
    result.writeBinary(value.data, value.size);
    result.writeEndBinary();
}

void DataTypeArraySerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

void DataTypeArraySerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                               arrow::ArrayBuilder* array_builder, int start,
                                               int end) const {
    auto& array_column = static_cast<const ColumnArray&>(column);
    auto& offsets = array_column.get_offsets();
    auto& nested_data = array_column.get_data();
    auto& builder = assert_cast<arrow::ListBuilder&>(*array_builder);
    auto nested_builder = builder.value_builder();
    for (size_t array_idx = start; array_idx < end; ++array_idx) {
        if (null_map && (*null_map)[array_idx]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
            continue;
        }
        checkArrowStatus(builder.Append(), column.get_name(), array_builder->type()->name());
        nested_serde->write_column_to_arrow(nested_data, nullptr, nested_builder,
                                            offsets[array_idx - 1], offsets[array_idx]);
    }
}

void DataTypeArraySerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                int start, int end,
                                                const cctz::time_zone& ctz) const {
    auto& column_array = static_cast<ColumnArray&>(column);
    auto& offsets_data = column_array.get_offsets();
    auto concrete_array = dynamic_cast<const arrow::ListArray*>(arrow_array);
    auto arrow_offsets_array = concrete_array->offsets();
    auto arrow_offsets = dynamic_cast<arrow::Int32Array*>(arrow_offsets_array.get());
    auto prev_size = offsets_data.back();
    auto arrow_nested_start_offset = arrow_offsets->Value(start);
    auto arrow_nested_end_offset = arrow_offsets->Value(end);
    for (int64_t i = start + 1; i < end + 1; ++i) {
        // convert to doris offset, start from offsets.back()
        offsets_data.emplace_back(prev_size + arrow_offsets->Value(i) - arrow_nested_start_offset);
    }
    return nested_serde->read_column_from_arrow(
            column_array.get_data(), concrete_array->values().get(), arrow_nested_start_offset,
            arrow_nested_end_offset, ctz);
}

template <bool is_binary_format>
Status DataTypeArraySerDe::_write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<is_binary_format>& result,
                                                  int row_idx, bool col_const) const {
    auto& column_array = assert_cast<const ColumnArray&>(column);
    auto& offsets = column_array.get_offsets();
    auto& data = column_array.get_data();
    bool is_nested_string = data.is_column_string();
    const auto col_index = index_check_const(row_idx, col_const);
    result.open_dynamic_mode();
    if (0 != result.push_string("[", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    for (int j = offsets[col_index - 1]; j < offsets[col_index]; ++j) {
        if (j != offsets[col_index - 1]) {
            if (0 != result.push_string(", ", 2)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }
        if (data.is_null_at(j)) {
            if (0 != result.push_string("NULL", strlen("NULL"))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (is_nested_string) {
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(nested_serde->write_column_to_mysql(data, result, j, false));
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(nested_serde->write_column_to_mysql(data, result, j, false));
            }
        }
    }
    if (0 != result.push_string("]", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    result.close_dynamic_mode();
    return Status::OK();
}

Status DataTypeArraySerDe::write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                 bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeArraySerDe::write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                 bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris