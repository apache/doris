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

#include "data_type_varbinary_serde.h"

#include "vec/columns/column_varbinary.h"

namespace doris::vectorized {

void DataTypeVarbinarySerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                     Arena& arena, int32_t col_id,
                                                     int64_t row_num) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} read_one_cell_from_jsonb ostr not implement.",
                           column.get_name());
}

void DataTypeVarbinarySerDe::read_one_cell_from_jsonb(IColumn& column,
                                                      const JsonbValue* arg) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} read_one_cell_from_jsonb ostr not implement.",
                           column.get_name());
}

Status DataTypeVarbinarySerDe::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<true>& row_buffer,
                                                     int64_t row_idx, bool col_const,
                                                     const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeVarbinarySerDe::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<false>& row_buffer,
                                                     int64_t row_idx, bool col_const,
                                                     const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <bool is_binary_format>
Status DataTypeVarbinarySerDe::_write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<is_binary_format>& result,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    auto col_index = index_check_const(row_idx, col_const);
    auto data = assert_cast<const ColumnVarbinary&>(column).get_data()[col_index];

    if (0 != result.push_string(data.data(), data.size())) {
        return Status::InternalError("pack mysql buffer failed.");
    }

    return Status::OK();
}

} // namespace doris::vectorized
