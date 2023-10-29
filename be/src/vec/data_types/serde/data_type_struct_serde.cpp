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
    size_t size = elemSerDeSPtrs.size();
    for (size_t i = 0; i < size; ++i) {
        if (elemNames[i] == name) {
            return std::optional<size_t>(i);
        }
    }
    return std::nullopt;
}

Status DataTypeStructSerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                     int end_idx, BufferWritable& bw,
                                                     FormatOptions& options,
                                                     int nesting_level) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeStructSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                       BufferWritable& bw, FormatOptions& options,
                                                       int nesting_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const ColumnStruct& struct_column = assert_cast<const ColumnStruct&>(*ptr);
    bw.write('{');
    for (int i = 0; i < struct_column.get_columns().size(); i++) {
        if (i != 0) {
            bw.write(',');
            bw.write(' ');
        }
        RETURN_IF_ERROR(elemSerDeSPtrs[i]->serialize_one_cell_to_json(
                struct_column.get_column(i), row_num, bw, options, nesting_level + 1));
    }
    bw.write('}');
    return Status::OK();
}

Status DataTypeStructSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                           const FormatOptions& options,
                                                           int nesting_level) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    auto& struct_column = assert_cast<ColumnStruct&>(column);

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

    ReadBuffer rb(slice.data, slice.size);
    ++rb.position();

    bool is_explicit_names = false;
    std::vector<std::string> field_names;
    std::vector<ReadBuffer> field_rbs;
    std::vector<size_t> field_pos;

    while (!rb.eof()) {
        StringRef slot(rb.position(), rb.count());
        bool has_quota = false;
        bool is_name = false;
        if (!next_slot_from_string(rb, slot, is_name, has_quota)) {
            return Status::InvalidArgument("Cannot read struct field from text '{}'",
                                           slot.to_string());
        }
        if (is_name) {
            std::string name = slot.to_string();
            if (!next_slot_from_string(rb, slot, is_name, has_quota)) {
                return Status::InvalidArgument("Cannot read struct field from text '{}'",
                                               slot.to_string());
            }
            ReadBuffer field_rb(const_cast<char*>(slot.data), slot.size);
            field_names.push_back(name);
            field_rbs.push_back(field_rb);

            if (!is_explicit_names) {
                is_explicit_names = true;
            }
        } else {
            ReadBuffer field_rb(const_cast<char*>(slot.data), slot.size);
            field_rbs.push_back(field_rb);
        }
    }

    // TODO: should we support insert default field value when actual field number is less than
    // schema field number?
    if (field_rbs.size() != elemSerDeSPtrs.size()) {
        std::string cmp_str = field_rbs.size() > elemSerDeSPtrs.size() ? "more" : "less";
        return Status::InvalidArgument(
                "Actual struct field number {} is {} than schema field number {}.",
                field_rbs.size(), cmp_str, elemSerDeSPtrs.size());
    }

    if (is_explicit_names) {
        if (field_names.size() != field_rbs.size()) {
            return Status::InvalidArgument(
                    "Struct field name number {} is not equal to field number {}.",
                    field_names.size(), field_rbs.size());
        }
        std::unordered_set<std::string> name_set;
        for (size_t i = 0; i < field_names.size(); i++) {
            // check duplicate fields
            auto ret = name_set.insert(field_names[i]);
            if (!ret.second) {
                return Status::InvalidArgument("Struct field name {} is duplicate with others.",
                                               field_names[i]);
            }
            // check name valid
            auto idx = try_get_position_by_name(field_names[i]);
            if (idx == std::nullopt) {
                return Status::InvalidArgument("Cannot find struct field name {} in schema.",
                                               field_names[i]);
            }
            field_pos.push_back(idx.value());
        }
    } else {
        for (size_t i = 0; i < field_rbs.size(); i++) {
            field_pos.push_back(i);
        }
    }

    for (size_t idx = 0; idx < elemSerDeSPtrs.size(); idx++) {
        auto field_rb = field_rbs[field_pos[idx]];
        // handle empty element
        if (field_rb.count() == 0) {
            struct_column.get_column(idx).insert_default();
            continue;
        }
        // handle null element
        if (field_rb.count() == 4 && strncmp(field_rb.position(), "null", 4) == 0) {
            auto& nested_null_col =
                    reinterpret_cast<ColumnNullable&>(struct_column.get_column(idx));
            nested_null_col.insert_null_elements(1);
            continue;
        }
        Slice element_slice(field_rb.position(), field_rb.count());
        auto st = elemSerDeSPtrs[idx]->deserialize_one_cell_from_json(struct_column.get_column(idx),
                                                                      element_slice, options);
        if (!st.ok()) {
            // we should do column revert if error
            for (size_t j = 0; j < idx; j++) {
                struct_column.get_column(j).pop_back(1);
            }
            return st;
        }
    }

    return Status::OK();
}

Status DataTypeStructSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                                std::vector<Slice>& slices,
                                                                int* num_deserialized,
                                                                const FormatOptions& options,
                                                                int nesting_level) const {
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

Status DataTypeStructSerDe::deserialize_one_cell_from_hive_text(IColumn& column, Slice& slice,
                                                                const FormatOptions& options,
                                                                int nesting_level) const {
    if (slice.empty()) {
        return Status::InvalidArgument("slice is empty!");
    }
    char struct_delimiter = options.get_collection_delimiter(nesting_level);

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
        Status st = elemSerDeSPtrs[loc]->deserialize_one_cell_from_hive_text(
                struct_column.get_column(loc), slices[loc], options, nesting_level + 1);
        if (st != Status::OK()) {
            return st;
        }
    }
    return Status::OK();
}

Status DataTypeStructSerDe::deserialize_column_from_hive_text_vector(IColumn& column,
                                                                     std::vector<Slice>& slices,
                                                                     int* num_deserialized,
                                                                     const FormatOptions& options,
                                                                     int nesting_level) const {
    DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR();
    return Status::OK();
}

void DataTypeStructSerDe::serialize_one_cell_to_hive_text(const IColumn& column, int row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options,
                                                          int nesting_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const ColumnStruct& struct_column = assert_cast<const ColumnStruct&>(*ptr);

    char collection_delimiter = options.get_collection_delimiter(nesting_level);
    for (int i = 0; i < struct_column.get_columns().size(); i++) {
        if (i != 0) {
            bw.write(collection_delimiter);
        }
        elemSerDeSPtrs[i]->serialize_one_cell_to_hive_text(struct_column.get_column(i), row_num, bw,
                                                           options, nesting_level + 1);
    }
}

void DataTypeStructSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

void DataTypeStructSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
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
            elemSerDeSPtrs[ei]->write_column_to_arrow(struct_column.get_column(ei), nullptr,
                                                      elem_builder, r, r + 1);
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
        elemSerDeSPtrs[i]->read_column_from_arrow(struct_column.get_column(i),
                                                  concrete_struct->field(i).get(), start, end, ctz);
    }
}

template <bool is_binary_format>
Status DataTypeStructSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& col = assert_cast<const ColumnStruct&>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    result.open_dynamic_mode();
    if (0 != result.push_string("{", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    bool begin = true;
    for (size_t j = 0; j < elemSerDeSPtrs.size(); ++j) {
        if (!begin) {
            if (0 != result.push_string(", ", 2)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }

        if (col.get_column_ptr(j)->is_null_at(col_index)) {
            if (0 != result.push_string("NULL", strlen("NULL"))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (remove_nullable(col.get_column_ptr(j))->is_column_string()) {
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(elemSerDeSPtrs[j]->write_column_to_mysql(col.get_column(j), result,
                                                                         col_index, false));
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(elemSerDeSPtrs[j]->write_column_to_mysql(col.get_column(j), result,
                                                                         col_index, false));
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
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeStructSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeStructSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch, int start,
                                                int end,
                                                std::vector<StringRef>& buffer_list) const {
    orc::StructVectorBatch* cur_batch = dynamic_cast<orc::StructVectorBatch*>(orc_col_batch);

    const ColumnStruct& struct_col = assert_cast<const ColumnStruct&>(column);
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            for (int i = 0; i < struct_col.tuple_size(); ++i) {
                static_cast<void>(elemSerDeSPtrs[i]->write_column_to_orc(
                        timezone, struct_col.get_column(i), nullptr, cur_batch->fields[i], row_id,
                        row_id + 1, buffer_list));
            }
        } else {
            // This else is necessary
            // because we must set notNull when cur_batch->notNull[row_id] == 0
            for (int j = 0; j < struct_col.tuple_size(); ++j) {
                cur_batch->fields[j]->hasNulls = true;
                cur_batch->fields[j]->notNull[row_id] = 0;
            }
        }
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
