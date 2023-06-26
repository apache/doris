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

#include "gutil/casts.h"
#include "util/jsonb_document.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"

namespace doris {

namespace vectorized {
class Arena;

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

void DataTypeArraySerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                               arrow::ArrayBuilder* array_builder, int start,
                                               int end) const {
    auto& array_column = static_cast<const ColumnArray&>(column);
    auto& offsets = array_column.get_offsets();
    auto& nested_data = array_column.get_data();
    auto& builder = assert_cast<arrow::ListBuilder&>(*array_builder);
    auto nested_builder = builder.value_builder();
    for (size_t array_idx = start; array_idx < end; ++array_idx) {
        checkArrowStatus(builder.Append(), column.get_name(), array_builder->type()->name());
        nested_serde->write_column_to_arrow(nested_data, null_map, nested_builder,
                                            offsets[array_idx - 1], offsets[array_idx]);
    }
}

void DataTypeArraySerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                int start, int end,
                                                const cctz::time_zone& ctz) const {
    auto& column_array = static_cast<ColumnArray&>(column);
    auto& offsets_data = column_array.get_offsets();
    auto concrete_array = down_cast<const arrow::ListArray*>(arrow_array);
    auto arrow_offsets_array = concrete_array->offsets();
    auto arrow_offsets = down_cast<arrow::Int32Array*>(arrow_offsets_array.get());
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
                RETURN_IF_ERROR(nested_serde->write_column_to_mysql(data, result, j, col_const));
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(nested_serde->write_column_to_mysql(data, result, j, col_const));
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