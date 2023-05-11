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

#include "data_type_datetimev2_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "gutil/casts.h"
#include "vec/columns/column_const.h"

namespace doris {
namespace vectorized {

void DataTypeDateTimeV2SerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                    arrow::ArrayBuilder* array_builder, int start,
                                                    int end) const {
    auto& col_data = static_cast<const ColumnVector<UInt64>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        char buf[64];
        const vectorized::DateV2Value<vectorized::DateTimeV2ValueType>* time_val =
                (const vectorized::DateV2Value<vectorized::DateTimeV2ValueType>*)(col_data[i]);
        int len = time_val->to_buffer(buf);
        if (null_map && null_map[i]) {
            checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            checkArrowStatus(string_builder.Append(buf, len), column.get_name(),
                             array_builder->type()->name());
        }
    }
}
template <bool is_binary_format>
Status DataTypeDateTimeV2SerDe::_write_column_to_mysql(
        const IColumn& column, std::vector<MysqlRowBuffer<is_binary_format>>& result, int start,
        int end, int scale, bool col_const) const {
    auto& data = assert_cast<const ColumnVector<UInt64>&>(column).get_data();
    int buf_ret = 0;
    for (ssize_t i = start; i < end; ++i) {
        if (0 != buf_ret) {
            return Status::InternalError("pack mysql buffer failed.");
        }
        const auto col_index = index_check_const(i, col_const);
        auto time_num = data[col_index];
        char buf[64];
        DateV2Value<DateTimeV2ValueType> date_val =
                binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(time_num);
        char* pos = date_val.to_string(buf, scale);
        buf_ret = result[i].push_string(buf, pos - buf - 1);
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris