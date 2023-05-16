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

#include "util/jsonb_document.h"
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

void DataTypeMapSerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                             arrow::ArrayBuilder* array_builder, int start,
                                             int end) const {
    LOG(FATAL) << "Not support write " << column.get_name() << " to arrow";
}

void DataTypeMapSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                              int start, int end,
                                              const cctz::time_zone& ctz) const {
    LOG(FATAL) << "Not support read " << column.get_name() << " from arrow";
}
template <bool is_binary_format>
Status DataTypeMapSerDe::_write_column_to_mysql(
        const IColumn& column, std::vector<MysqlRowBuffer<is_binary_format>>& result, int row_idx,
        int start, int end, int scale, bool col_const) const {
    int buf_ret = 0;
    auto& map_column = assert_cast<const ColumnMap&>(column);
    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();
    auto& offsets = map_column.get_offsets();
    for (ssize_t i = start; i < end; ++i) {
        if (0 != buf_ret) {
            return Status::InternalError("pack mysql buffer failed.");
        }
        const auto col_index = index_check_const(i, col_const);
        result[row_idx].open_dynamic_mode();
        buf_ret = result[row_idx].push_string("{", 1);
        for (auto j = offsets[col_index - 1]; j < offsets[col_index]; ++j) {
            if (j != offsets[col_index - 1]) {
                buf_ret = result[row_idx].push_string(", ", 2);
            }
            RETURN_IF_ERROR(key_serde->write_column_to_mysql(nested_keys_column, result, row_idx, j,
                                                             j + 1, scale, col_const));
            buf_ret = result[row_idx].push_string(":", 1);
            RETURN_IF_ERROR(value_serde->write_column_to_mysql(
                    nested_values_column, result, row_idx, j, j + 1, scale, col_const));
        }
        buf_ret = result[row_idx].push_string("}", 1);
        result[row_idx].close_dynamic_mode();
        ++row_idx;
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris
