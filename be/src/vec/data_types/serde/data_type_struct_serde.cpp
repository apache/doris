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
#include "complex_type_deserialize_util.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_struct.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {

namespace vectorized {
class Arena;
#include "common/compile_check_begin.h"

std::string DataTypeStructSerDe::get_name() const {
    size_t size = elem_names.size();
    std::stringstream s;

    s << "Struct(";
    for (size_t i = 0; i < size; ++i) {
        if (i != 0) {
            s << ", ";
        }
        s << elem_names[i] << ":";
        s << elem_serdes_ptrs[i]->get_name();
    }
    s << ")";

    return s.str();
}

Status DataTypeStructSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                     int64_t end_idx, BufferWritable& bw,
                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeStructSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                       BufferWritable& bw,
                                                       FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& struct_column = assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(*ptr);
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

    // only test

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
    DCHECK_EQ(elem_size, elem_names.size());
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
            if (field_pos >= elem_size) {
                // we should do column revert if error
                for (size_t j = 0; j < field_pos; j++) {
                    struct_column.get_column(j).pop_back(1);
                }
                return Status::InvalidArgument(
                        "Actual struct field number is more than schema field number {}.",
                        field_pos, elem_size);
            }
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
            // field_pos should always less than elem_size, if not, we should return error
            if (field_pos >= elem_size) {
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
        /// field_pos should always less than elem_size, if not, we should return error
        if (field_pos >= elem_size) {
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
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

void DataTypeStructSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
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
            if (options.escape_char != 0 && i > 0 && data[i - 1] == options.escape_char) {
                continue;
            }
            slices.emplace_back(data + from, i - from);
            from = i + 1;
        }
    }
    auto& struct_column = static_cast<ColumnStruct&>(column);

    for (auto i = slices.size(); i < struct_column.get_columns().size(); ++i) {
        // Hive schema change will cause the number of sub-columns in the file to
        // be inconsistent with the number of sub-columns of the column in the table.
        slices.emplace_back(options.null_format, options.null_len);
    }
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
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options, int hive_text_complex_type_delimiter_level) const {
    DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR();
    return Status::OK();
}

Status DataTypeStructSerDe::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& struct_column = assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(*ptr);

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

Status DataTypeStructSerDe::serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                                      JsonbWriter& writer) const {
    const auto& struct_column = assert_cast<const ColumnStruct&>(from_column);

    if (!writer.writeStartObject()) {
        return Status::InternalError("writeStartObject failed");
    }

    for (size_t i = 0; i < elem_serdes_ptrs.size(); ++i) {
        // check key
        if (elem_names[i].size() > std::numeric_limits<uint8_t>::max()) {
            return Status::InternalError("key size exceeds max limit {} ", elem_names[i]);
        }
        // write key
        if (!writer.writeKey(elem_names[i].data(), (uint8_t)elem_names[i].size())) {
            return Status::InternalError("writeKey failed : {}", elem_names[i]);
        }
        // write value
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->serialize_column_to_jsonb(struct_column.get_column(i),
                                                                       row_num, writer));
    }

    if (!writer.writeEndObject()) {
        return Status::InternalError("writeEndObject failed");
    }

    return Status::OK();
}

Status DataTypeStructSerDe::deserialize_column_from_jsonb(IColumn& column,
                                                          const JsonbValue* jsonb_value,
                                                          CastParameters& castParms) const {
    if (jsonb_value->isString()) {
        RETURN_IF_ERROR(parse_column_from_jsonb_string(column, jsonb_value, castParms));
        return Status::OK();
    }
    auto& struct_column = assert_cast<ColumnStruct&>(column);
    if (!jsonb_value->isObject()) {
        return Status::InvalidArgument("jsonb_value is not an object");
    }
    const auto* jsonb_object = jsonb_value->unpack<ObjectVal>();

    if (jsonb_object->numElem() != elem_names.size()) {
        return Status::InvalidArgument("jsonb_value field size {} is not equal to struct size {}",
                                       jsonb_object->numElem(), struct_column.tuple_size());
    }

    for (const auto& field_name : elem_names) {
        if (!jsonb_object->find(field_name.data(), (int)field_name.size())) {
            return Status::InvalidArgument("jsonb_value does not have key {}", field_name);
        }
    }

    for (size_t i = 0; i < elem_names.size(); ++i) {
        const auto& field_name = elem_names[i];
        JsonbValue* value = jsonb_object->find(field_name.data(), (int)field_name.size());
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->deserialize_column_from_jsonb(
                struct_column.get_column(i), value, castParms));
    }

    return Status::OK();
}

void DataTypeStructSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    const auto* blob = arg->unpack<JsonbBinaryVal>();
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

Status DataTypeStructSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                  arrow::ArrayBuilder* array_builder, int64_t start,
                                                  int64_t end, const cctz::time_zone& ctz) const {
    auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
    const auto& struct_column = assert_cast<const ColumnStruct&>(column);
    for (auto r = start; r < end; ++r) {
        if (null_map != nullptr && (*null_map)[r]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), struct_column.get_name(),
                                             builder.type()->name()));
            continue;
        }
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(), struct_column.get_name(),
                                         builder.type()->name()));
        for (auto ei = 0; ei < struct_column.tuple_size(); ++ei) {
            auto* elem_builder = builder.field_builder(ei);
            RETURN_IF_ERROR(elem_serdes_ptrs[ei]->write_column_to_arrow(
                    struct_column.get_column(ei), nullptr, elem_builder, r, r + 1, ctz));
        }
    }
    return Status::OK();
}

Status DataTypeStructSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                   int64_t start, int64_t end,
                                                   const cctz::time_zone& ctz) const {
    auto& struct_column = static_cast<ColumnStruct&>(column);
    const auto* concrete_struct = dynamic_cast<const arrow::StructArray*>(arrow_array);
    DCHECK_EQ(struct_column.tuple_size(), concrete_struct->num_fields());
    for (auto i = 0; i < struct_column.tuple_size(); ++i) {
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->read_column_from_arrow(
                struct_column.get_column(i), concrete_struct->field(i).get(), start, end, ctz));
    }
    return Status::OK();
}

template <bool is_binary_format>
Status DataTypeStructSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    const auto& col = assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    result.open_dynamic_mode();
    if (0 != result.push_string("{", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    bool begin = true;
    for (size_t j = 0; j < elem_serdes_ptrs.size(); ++j) {
        if (!begin) {
            if (0 != result.push_string(options.mysql_collection_delim.c_str(),
                                        options.mysql_collection_delim.size())) {
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
                                                  MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeStructSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer,
                                                  int64_t row_idx, bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeStructSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch,
                                                int64_t start, int64_t end,
                                                vectorized::Arena& arena) const {
    auto* cur_batch = dynamic_cast<orc::StructVectorBatch*>(orc_col_batch);
    const auto& struct_col = assert_cast<const ColumnStruct&>(column);
    for (auto row_id = start; row_id < end; row_id++) {
        for (int i = 0; i < struct_col.tuple_size(); ++i) {
            RETURN_IF_ERROR(elem_serdes_ptrs[i]->write_column_to_orc(
                    timezone, struct_col.get_column(i), nullptr, cur_batch->fields[i], row_id,
                    row_id + 1, arena));
        }
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeStructSerDe::write_column_to_pb(const IColumn& column, PValues& result,
                                               int64_t start, int64_t end) const {
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
    for (auto i = 0; i < struct_column.tuple_size(); ++i) {
        RETURN_IF_ERROR(elem_serdes_ptrs[i]->read_column_from_pb(struct_column.get_column(i),
                                                                 arg.child_element(i)));
    }
    return Status::OK();
}

template <bool is_strict_mode>
Status DataTypeStructSerDe::_from_string(StringRef& str, IColumn& column,
                                         const FormatOptions& options) const {
    if (str.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    auto& struct_column = assert_cast<ColumnStruct&, TypeCheckOnRelease::DISABLE>(column);

    if (str.front() != '{') {
        std::stringstream ss;
        ss << str.front() << '\'';
        return Status::InvalidArgument("Struct does not start with '{' character, found '" +
                                       ss.str());
    }
    if (str.back() != '}') {
        std::stringstream ss;
        ss << str.back() << '\'';
        return Status::InvalidArgument("Struct does not end with '}' character, found '" +
                                       ss.str());
    }

    // here need handle the empty struct '{}'
    if (str.size == 2) {
        for (size_t i = 0; i < struct_column.tuple_size(); ++i) {
            struct_column.get_column(i).insert_default();
        }
        return Status::OK();
    }
    str = str.substring(1, str.size - 2); // remove '{' '}'

    auto split_result = ComplexTypeDeserializeUtil::split_by_delimiter(str, [&](char c) {
        return c == options.map_key_delim || c == options.collection_delim;
    });

    const auto elem_size = elem_serdes_ptrs.size();

    std::vector<StringRef> field_value;
    // check syntax error
    if (split_result.size() == elem_size) {
        // no field name
        for (int i = 0; i < split_result.size(); i++) {
            if (i != split_result.size() - 1 &&
                split_result[i].delimiter != options.collection_delim) {
                return Status::InvalidArgument(
                        "Struct field value {} is not separated by collection_delim.", i);
            }
            field_value.push_back(split_result[i].element);
        }
    } else if (split_result.size() == 2 * elem_size) {
        // field name : field value
        int field_pos = 0;
        for (int i = 0; i < split_result.size(); i += 2) {
            if (split_result[i].delimiter != options.map_key_delim) {
                return Status::InvalidArgument(
                        "Struct name-value pair does not have map key delimiter");
            }
            if (i != 0 && split_result[i - 1].delimiter != options.collection_delim) {
                return Status::InvalidArgument(
                        "Struct name-value pair does not have collection delimiter");
            }
            if (field_pos >= elem_size) {
                return Status::InvalidArgument(
                        "Struct field number is more than schema field number");
            }
            auto field_name = split_result[i].element.trim_quote();

            if (!field_name.eq(StringRef(elem_names[field_pos]))) {
                return Status::InvalidArgument("Cannot find struct field name {} in schema.",
                                               split_result[i].element.to_string());
            }
            field_value.push_back(split_result[i + 1].element);
            field_pos++;
        }
    } else {
        return Status::InvalidArgument(
                "Struct field number {} is not equal to schema field number {}.",
                split_result.size(), elem_size);
    }

    for (int field_pos = 0; field_pos < elem_size; ++field_pos) {
        if (Status st = ComplexTypeDeserializeUtil::process_column<is_strict_mode>(
                    elem_serdes_ptrs[field_pos], struct_column.get_column(field_pos),
                    field_value[field_pos], options);
            st != Status::OK()) {
            // we should do column revert if error
            for (size_t j = 0; j < field_pos; j++) {
                struct_column.get_column(j).pop_back(1);
            }
            return st;
        }
    }
    return Status::OK();
}

Status DataTypeStructSerDe::from_string(StringRef& str, IColumn& column,
                                        const FormatOptions& options) const {
    return _from_string<false>(str, column, options);
}
Status DataTypeStructSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                    const FormatOptions& options) const {
    return _from_string<true>(str, column, options);
}

} // namespace vectorized
} // namespace doris
