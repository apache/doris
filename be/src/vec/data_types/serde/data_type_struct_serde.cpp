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

#include "util/jsonb_document.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_struct.h"
#include "vec/common/string_ref.h"

namespace doris {

namespace vectorized {
class Arena;

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

void DataTypeStructSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

void DataTypeStructSerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    LOG(FATAL) << "Not support write " << column.get_name() << " to arrow";
}

void DataTypeStructSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    LOG(FATAL) << "Not support read " << column.get_name() << " from arrow";
}
template <bool is_binary_format>
Status DataTypeStructSerDe::_write_column_to_mysql(
        const IColumn& column, bool return_object_data_as_binary,
        std::vector<MysqlRowBuffer<is_binary_format>>& result, int row_idx, int start, int end,
        bool col_const) const {
    int buf_ret = 0;
    auto& col = assert_cast<const ColumnStruct&>(column);
    for (ssize_t i = start; i < end; ++i) {
        if (0 != buf_ret) {
            return Status::InternalError("pack mysql buffer failed.");
        }
        const auto col_index = index_check_const(i, col_const);
        result[row_idx].open_dynamic_mode();
        buf_ret = result[row_idx].push_string("{", 1);
        bool begin = true;
        for (size_t j = 0; j < elemSerDeSPtrs.size(); ++j) {
            if (!begin) {
                buf_ret = result[row_idx].push_string(", ", 2);
            }

            if (col.get_column_ptr(j)->is_null_at(col_index)) {
                buf_ret = result[row_idx].push_string("NULL", strlen("NULL"));
            } else {
                if (remove_nullable(col.get_column_ptr(j))->is_column_string()) {
                    buf_ret = result[row_idx].push_string("\"", 1);
                    RETURN_IF_ERROR(elemSerDeSPtrs[j]->write_column_to_mysql(
                            col.get_column(j), return_object_data_as_binary, result, row_idx,
                            col_index, col_index + 1, col_const));
                    buf_ret = result[row_idx].push_string("\"", 1);
                } else {
                    RETURN_IF_ERROR(elemSerDeSPtrs[j]->write_column_to_mysql(
                            col.get_column(j), return_object_data_as_binary, result, row_idx,
                            col_index, col_index + 1, col_const));
                }
            }
            begin = false;
        }
        buf_ret = result[row_idx].push_string("}", 1);
        result[row_idx].close_dynamic_mode();
        ++row_idx;
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris