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

#include "data_type_ipv4_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "gutil/casts.h"
#include "vec/columns/column_const.h"

namespace doris {
namespace vectorized {

void DataTypeIPv4SerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    auto& col_data = static_cast<const ColumnVector<UInt32>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        IPv4Value ipv4_val(col_data[i]);
        auto ipv4_str = ipv4_val.to_string();
        if (null_map && null_map[i]) {
            checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            checkArrowStatus(string_builder.Append(ipv4_str.c_str(), ipv4_str.length()), column.get_name(),
                             array_builder->type()->name());
        }
    }
}

void DataTypeIPv4SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    std::cout << "column : " << column.get_name() << " data" << getTypeName(column.get_data_type())
              << " array " << arrow_array->type_id() << std::endl;
}

template <bool is_binary_format>
Status DataTypeIPv4SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& data = assert_cast<const ColumnVector<UInt32>&>(column).get_data();
    auto col_index = index_check_const(row_idx, col_const);
    IPv4Value ipv4_val(data[col_index]);
    if (UNLIKELY(0 != result.push_ipv4(ipv4_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

Status DataTypeIPv4SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeIPv4SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris