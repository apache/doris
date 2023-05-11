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

#include "data_type_time_serde.h"

#include "vec/columns/column_const.h"

namespace doris {
namespace vectorized {
template <bool is_binary_format>
Status DataTypeTimeSerDe::_write_column_to_mysql(
        const IColumn& column, std::vector<MysqlRowBuffer<is_binary_format>>& result, int start,
        int end, int scale, bool col_const) const {
    int buf_ret = 0;
    auto& data = static_cast<const ColumnVector<Float64>&>(column).get_data();
    for (int i = start; i < end; ++i) {
        if (0 != buf_ret) {
            return Status::InternalError("pack mysql buffer failed.");
        }
        const auto col_index = index_check_const(i, col_const);
        buf_ret = result[i].push_time(data[col_index]);
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris