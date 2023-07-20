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
#include "util/jsonb_document.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Arena;

void DataTypeMapSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

void DataTypeMapSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                               Arena* mem_pool, int32_t col_id, int row_num) const {
    result.writeKey(col_id);
    const char* begin = nullptr;
    // maybe serialize_value_into_arena should move to here later.
    StringRef value = column.serialize_value_into_arena(row_num, *mem_pool, begin);
    result.writeStartBinary();
    result.writeBinary(value.data, value.size);
    result.writeEndBinary();
}

void DataTypeMapSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                             arrow::ArrayBuilder* array_builder, int start,
                                             int end) const {
    auto& builder = assert_cast<arrow::MapBuilder&>(*array_builder);
    auto& map_column = assert_cast<const ColumnMap&>(column);
    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    // now we default set key value in map is nullable
    DCHECK(nested_keys_column.is_nullable());
    DCHECK(nested_values_column.is_nullable());
    auto keys_nullmap_data =
            check_and_get_column<ColumnNullable>(nested_keys_column)->get_null_map_data().data();
    auto& offsets = map_column.get_offsets();
    auto key_builder = builder.key_builder();
    auto value_builder = builder.item_builder();

    for (size_t r = start; r < end; ++r) {
        if ((null_map && (*null_map)[r])) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else if (simd::contain_byte(keys_nullmap_data + offsets[r - 1],
                                      offsets[r] - offsets[r - 1], 1)) {
            // arrow do not support key is null, so we ignore the null key-value
            MutableColumnPtr key_mutable_data = nested_keys_column.clone_empty();
            MutableColumnPtr value_mutable_data = nested_values_column.clone_empty();
            for (size_t i = offsets[r - 1]; i < offsets[r]; ++i) {
                if (keys_nullmap_data[i] == 1) {
                    continue;
                }
                key_mutable_data->insert_from(nested_keys_column, i);
                value_mutable_data->insert_from(nested_values_column, i);
            }
            checkArrowStatus(builder.Append(), column.get_name(), array_builder->type()->name());

            key_serde->write_column_to_arrow(*key_mutable_data, nullptr, key_builder, 0,
                                             key_mutable_data->size());
            value_serde->write_column_to_arrow(*value_mutable_data, nullptr, value_builder, 0,
                                               value_mutable_data->size());
        } else {
            checkArrowStatus(builder.Append(), column.get_name(), array_builder->type()->name());
            key_serde->write_column_to_arrow(nested_keys_column, nullptr, key_builder,
                                             offsets[r - 1], offsets[r]);
            value_serde->write_column_to_arrow(nested_values_column, nullptr, value_builder,
                                               offsets[r - 1], offsets[r]);
        }
    }
}

void DataTypeMapSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                              int start, int end,
                                              const cctz::time_zone& ctz) const {
    auto& column_map = static_cast<ColumnMap&>(column);
    auto& offsets_data = column_map.get_offsets();
    auto concrete_map = down_cast<const arrow::MapArray*>(arrow_array);
    auto arrow_offsets_array = concrete_map->offsets();
    auto arrow_offsets = down_cast<arrow::Int32Array*>(arrow_offsets_array.get());
    auto prev_size = offsets_data.back();
    auto arrow_nested_start_offset = arrow_offsets->Value(start);
    auto arrow_nested_end_offset = arrow_offsets->Value(end);
    for (int64_t i = start + 1; i < end + 1; ++i) {
        // convert to doris offset, start from offsets.back()
        offsets_data.emplace_back(prev_size + arrow_offsets->Value(i) - arrow_nested_start_offset);
    }
    key_serde->read_column_from_arrow(column_map.get_keys(), concrete_map->keys().get(),
                                      arrow_nested_start_offset, arrow_nested_end_offset, ctz);
    value_serde->read_column_from_arrow(column_map.get_values(), concrete_map->items().get(),
                                        arrow_nested_start_offset, arrow_nested_end_offset, ctz);
}

template <bool is_binary_format>
Status DataTypeMapSerDe::_write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<is_binary_format>& result,
                                                int row_idx, bool col_const) const {
    auto& map_column = assert_cast<const ColumnMap&>(column);
    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    bool is_key_string = remove_nullable(nested_keys_column.get_ptr())->is_column_string();
    bool is_val_string = remove_nullable(nested_values_column.get_ptr())->is_column_string();

    const auto col_index = index_check_const(row_idx, col_const);
    result.open_dynamic_mode();
    if (0 != result.push_string("{", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    auto& offsets = map_column.get_offsets();
    for (auto j = offsets[col_index - 1]; j < offsets[col_index]; ++j) {
        if (j != offsets[col_index - 1]) {
            if (0 != result.push_string(", ", 2)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }
        if (nested_keys_column.is_null_at(j)) {
            if (0 != result.push_string("NULL", strlen("NULL"))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (is_key_string) {
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(
                        key_serde->write_column_to_mysql(nested_keys_column, result, j, col_const));
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(
                        key_serde->write_column_to_mysql(nested_keys_column, result, j, col_const));
            }
        }
        if (0 != result.push_string(":", 1)) {
            return Status::InternalError("pack mysql buffer failed.");
        }
        if (nested_values_column.is_null_at(j)) {
            if (0 != result.push_string("NULL", strlen("NULL"))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (is_val_string) {
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(value_serde->write_column_to_mysql(nested_values_column, result, j,
                                                                   col_const));
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(value_serde->write_column_to_mysql(nested_values_column, result, j,
                                                                   col_const));
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
                                               MysqlRowBuffer<true>& row_buffer, int row_idx,
                                               bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeMapSerDe::write_column_to_mysql(const IColumn& column,
                                               MysqlRowBuffer<false>& row_buffer, int row_idx,
                                               bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris
