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

#include "data_type_ipv6_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "gutil/casts.h"
#include "vec/columns/column_const.h"

namespace doris {
namespace vectorized {

void DataTypeIPv6SerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    auto& col_data = static_cast<const ColumnVector<UInt128>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        IPv6Value ipv6_val(col_data[i]);
        auto ipv6_str = ipv6_val.to_string();
        if (null_map && null_map[i]) {
            checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            checkArrowStatus(string_builder.Append(ipv6_str.c_str(), ipv6_str.length()), column.get_name(),
                             array_builder->type()->name());
        }
    }
}

void DataTypeIPv6SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    std::cout << "column : " << column.get_name() << " data" << getTypeName(column.get_data_type())
              << " array " << arrow_array->type_id() << std::endl;
    auto& col_data = static_cast<ColumnVector<UInt32>&>(column).get_data();
    auto concrete_array = down_cast<const arrow::Date64Array*>(arrow_array);
    int64_t divisor = 1;
    int64_t multiplier = 1;

    multiplier = 24 * 60 * 60; // day => secs
    for (size_t value_i = start; value_i < end; ++value_i) {
        DateV2Value<DateV2ValueType> v;
        v.from_unixtime(static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
        col_data.emplace_back(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(v));
    }
}

template <bool is_binary_format>
Status DataTypeIPv6SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& data = assert_cast<const ColumnVector<UInt128>&>(column).get_data();
    auto col_index = index_check_const(row_idx, col_const);
    IPv6Value ipv6_val(data[col_index]);
    if (UNLIKELY(0 != result.push_ipv6(ipv6_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

Status DataTypeIPv6SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeIPv6SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris