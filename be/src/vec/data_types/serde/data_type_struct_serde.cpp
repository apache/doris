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

#include "data_type_struct_serde.h"

#include "arrow/array/builder_nested.h"
#include "common/status.h"
#include "util/jsonb_document.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_struct.h"
#include "vec/common/string_ref.h"

namespace doris {

namespace vectorized {
class Arena;

std::optional<size_t> DataTypeStructSerDe::try_get_position_by_name(const String& name) const {
    size_t size = elem_serdes_ptrs.size();
    for (size_t i = 0; i < size; ++i) {
        if (elem_names[i] == name) {
            return std::optional<size_t>(i);
        }
    }
    return std::nullopt;
}

Status DataTypeStructSerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                     int end_idx, BufferWritable& bw,
                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeStructSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                       BufferWritable& bw,
                                                       FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const ColumnStruct& struct_column =
            assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(*ptr);
    bw.write('{');
    for (int i = 0; i < struct_column.get_columns().size(); i++) {
        if (i != 0) {
            bw.write(',');
            bw.write(' ');
        }
        std::string col_name = "\"" + elem_names[i] + "\": ";
        bw.write(col_name.c_str(), col_name.length());
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->serialize_one_cell_to_json(struct_column.get_column(i),
                                                                        row_num, bw, options));
    }
    bw.write('}');
    return Status::OK();
}
Status DataTypeStructSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                           const FormatOptions& options) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    auto& struct_column = assert_cast<ColumnStruct&, TypeCheckOnRelease::DISABLE>(column);

    if (slice[0] != '{') {
        std::stringstream ss;
        ss << slice[0] << '\'';
        return Status::InvalidArgument("Struct does not start with '{' character, found '" +
                                       ss.str());
    }
    if (slice[slice.size - 1] != '}') {
        std::stringstream ss;
        ss << slice[slice.size - 1] << '\'';
        return Status::InvalidArgument("Struct does not end with '}' character, found '" +
                                       ss.str());
    }

    // here need handle the empty struct '{}'
    if (slice.size == 2) {
        for (size_t i = 0; i < struct_column.tuple_size(); ++i) {
            struct_column.get_column(i).insert_default();
        }
        return Status::OK();
    }
    // remove '{' '}'
    slice.remove_prefix(1);
    slice.remove_suffix(1);
    slice.trim_prefix();

    bool is_explicit_names = false;
    int nested_level = 0;
    bool has_quote = false;
    int start_pos = 0;
    size_t slice_size = slice.size;
    bool key_added = false;
    int idx = 0;
    char quote_char = 0;

    auto elem_size = elem_serdes_ptrs.size();
    int field_pos = 0;

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
            next.trim_quote();
            // check field_name
            if (elem_names[field_pos] != next) {
                // we should do column revert if error
                for (size_t j = 0; j < field_pos; j++) {
                    struct_column.get_column(j).pop_back(1);
                }
                return Status::InvalidArgument("Cannot find struct field name {} in schema.",
                                               next.to_string());
            }
            // skip delimiter
            start_pos = idx + 1;
            is_explicit_names = true;
            key_added = true;
        } else if (!has_quote && nested_level == 0 && c == options.collection_delim &&
                   (key_added || !is_explicit_names)) {
            // if meet collection_delimiter and not in quote, we can make it as value elem
            if (idx == start_pos) {
                continue;
            }
            Slice next(slice.data + start_pos, idx - start_pos);
            next.trim_prefix();
            if (field_pos > elem_size) {
                // we should do column revert if error
                for (size_t j = 0; j < field_pos; j++) {
                    struct_column.get_column(j).pop_back(1);
                }
                return Status::InvalidArgument(
                        "Actual struct field number is more than schema field number {}.",
                        field_pos, elem_size);
            }
            if (Status st = elem_serdes_ptrs[field_pos]->deserialize_one_cell_from_json(
                        struct_column.get_column(field_pos), next, options);
                st != Status::OK()) {
                // we should do column revert if error
                for (size_t j = 0; j < field_pos; j++) {
                    struct_column.get_column(j).pop_back(1);
                }
                return st;
            }
            // skip delimiter
            start_pos = idx + 1;
            // reset key_added
            key_added = false;
            ++field_pos;
        }
    }
    // for last value elem
    if (!has_quote && nested_level == 0 && idx == slice_size && idx != start_pos &&
        (key_added || !is_explicit_names)) {
        Slice next(slice.data + start_pos, idx - start_pos);
        next.trim_prefix();
        if (field_pos > elem_size) {
            // we should do column revert if error
            for (size_t j = 0; j < field_pos; j++) {
                struct_column.get_column(j).pop_back(1);
            }
            return Status::InvalidArgument(
                    "Actual struct field number is more than schema field number {}.", field_pos,
                    elem_size);
        }
        if (Status st = elem_serdes_ptrs[field_pos]->deserialize_one_cell_from_json(
                    struct_column.get_column(field_pos), next, options);
            st != Status::OK()) {
            // we should do column revert if error
            for (size_t j = 0; j < field_pos; j++) {
                struct_column.get_column(j).pop_back(1);
            }
            return st;
        }
        ++field_pos;
    }

    // check stuff:
    if (field_pos < elem_size) {
        return Status::InvalidArgument(
                "Actual struct field number {} is less than schema field number {}.", field_pos,
                elem_size);
    }
    return Status::OK();
}

Status DataTypeStructSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

void DataTypeStructSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
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

Status DataTypeStructSerDe::deserialize_one_cell_from_hive_text(
        IColumn& column, Slice& slice, const FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    char struct_delimiter =
            options.get_collection_delimiter(hive_text_complex_type_delimiter_level);

    std::vector<Slice> slices;
    char* data = slice.data;
    for (size_t i = 0, from = 0; i <= slice.size; i++) {
        if (i == slice.size || data[i] == struct_delimiter) {
            slices.push_back({data + from, i - from});
            from = i + 1;
        }
    }
    auto& struct_column = static_cast<ColumnStruct&>(column);
    for (size_t loc = 0; loc < struct_column.get_columns().size(); loc++) {
        Status st = elem_serdes_ptrs[loc]->deserialize_one_cell_from_hive_text(
                struct_column.get_column(loc), slices[loc], options,
                hive_text_complex_type_delimiter_level + 1);
        if (st != Status::OK()) {
            return st;
        }
    }
    return Status::OK();
}

Status DataTypeStructSerDe::deserialize_column_from_hive_text_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options, int hive_text_complex_type_delimiter_level) const {
    DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR();
    return Status::OK();
}

Status DataTypeStructSerDe::serialize_one_cell_to_hive_text(
        const IColumn& column, int row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const ColumnStruct& struct_column =
            assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(*ptr);

    char collection_delimiter =
            options.get_collection_delimiter(hive_text_complex_type_delimiter_level);
    for (int i = 0; i < struct_column.get_columns().size(); i++) {
        if (i != 0) {
            bw.write(collection_delimiter);
        }
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->serialize_one_cell_to_hive_text(
                struct_column.get_column(i), row_num, bw, options,
                hive_text_complex_type_delimiter_level + 1));
    }
    return Status::OK();
}

void DataTypeStructSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

void DataTypeStructSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end, const cctz::time_zone& ctz) const {
    auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
    auto& struct_column = assert_cast<const ColumnStruct&>(column);
    for (int r = start; r < end; ++r) {
        if (null_map != nullptr && (*null_map)[r]) {
            checkArrowStatus(builder.AppendNull(), struct_column.get_name(),
                             builder.type()->name());
            continue;
        }
        checkArrowStatus(builder.Append(), struct_column.get_name(), builder.type()->name());
        for (size_t ei = 0; ei < struct_column.tuple_size(); ++ei) {
            auto elem_builder = builder.field_builder(ei);
            elem_serdes_ptrs[ei]->write_column_to_arrow(struct_column.get_column(ei), nullptr,
                                                        elem_builder, r, r + 1, ctz);
        }
    }
}

void DataTypeStructSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    auto& struct_column = static_cast<ColumnStruct&>(column);
    auto concrete_struct = dynamic_cast<const arrow::StructArray*>(arrow_array);
    DCHECK_EQ(struct_column.tuple_size(), concrete_struct->num_fields());
    for (size_t i = 0; i < struct_column.tuple_size(); ++i) {
        elem_serdes_ptrs[i]->read_column_from_arrow(
                struct_column.get_column(i), concrete_struct->field(i).get(), start, end, ctz);
    }
}

template <bool is_binary_format>
Status DataTypeStructSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    auto& col = assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    result.open_dynamic_mode();
    if (0 != result.push_string("{", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    bool begin = true;
    for (size_t j = 0; j < elem_serdes_ptrs.size(); ++j) {
        if (!begin) {
            if (0 != result.push_string(", ", 2)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }

        // eg: `"col_name": `
        // eg: `col_name=`
        std::string col_name;
        if (options.wrapper_len > 0) {
            col_name =
                    options.nested_string_wrapper + elem_names[j] + options.nested_string_wrapper;
        } else {
            col_name = elem_names[j];
        }
        col_name += options.map_key_delim;
        if (0 != result.push_string(col_name.c_str(), col_name.length())) {
            return Status::InternalError("pack mysql buffer failed.");
        }

        if (col.get_column_ptr(j)->is_null_at(col_index)) {
            if (0 != result.push_string(options.null_format, options.null_len)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (remove_nullable(col.get_column_ptr(j))->is_column_string() &&
                options.wrapper_len > 0) {
                if (0 != result.push_string(options.nested_string_wrapper, options.wrapper_len)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(elem_serdes_ptrs[j]->write_column_to_mysql(
                        col.get_column(j), result, col_index, false, options));
                if (0 != result.push_string(options.nested_string_wrapper, options.wrapper_len)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(elem_serdes_ptrs[j]->write_column_to_mysql(
                        col.get_column(j), result, col_index, false, options));
            }
        }
        begin = false;
    }
    if (UNLIKELY(0 != result.push_string("}", 1))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    result.close_dynamic_mode();
    return Status::OK();
}

Status DataTypeStructSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeStructSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeStructSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch, int start,
                                                int end,
                                                std::vector<StringRef>& buffer_list) const {
    auto* cur_batch = dynamic_cast<orc::StructVectorBatch*>(orc_col_batch);
    const auto& struct_col = assert_cast<const ColumnStruct&>(column);
    for (size_t row_id = start; row_id < end; row_id++) {
        for (int i = 0; i < struct_col.tuple_size(); ++i) {
            RETURN_IF_ERROR(elem_serdes_ptrs[i]->write_column_to_orc(
                    timezone, struct_col.get_column(i), nullptr, cur_batch->fields[i], row_id,
                    row_id + 1, buffer_list));
        }
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeStructSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                               int end) const {
    const auto& struct_col = assert_cast<const ColumnStruct&>(column);
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::STRUCT);
    auto tuple_size = struct_col.tuple_size();
    std::vector<PValues*> child_elements(tuple_size);
    for (int i = 0; i < tuple_size; ++i) {
        child_elements[i] = result.add_child_element();
    }
    for (int i = 0; i < tuple_size; ++i) {
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->write_column_to_pb(struct_col.get_column(i),
                                                                *child_elements[i], start, end));
    }
    return Status::OK();
}

Status DataTypeStructSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& struct_column = assert_cast<ColumnStruct&>(column);
    DCHECK_EQ(struct_column.tuple_size(), arg.child_element_size());
    for (size_t i = 0; i < struct_column.tuple_size(); ++i) {
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->read_column_from_pb(struct_column.get_column(i),
                                                                 arg.child_element(i)));
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
