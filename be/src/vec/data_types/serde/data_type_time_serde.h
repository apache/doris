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

#pragma once
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include "data_type_number_serde.h"
#include "vec/core/types.h"

namespace doris {
class JsonbOutStream;

namespace vectorized {
class Arena;

class DataTypeTimeSerDe : public DataTypeNumberSerDe<Float64> {
    Status write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                 std::vector<MysqlRowBuffer<false>>& result, int row_idx, int start,
                                 int end, bool col_const) const override {
        return _write_date_time_column_to_mysql(column, return_object_data_as_binary, result,
                                                row_idx, start, end, col_const);
    }
    Status write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                 std::vector<MysqlRowBuffer<true>>& result, int row_idx, int start,
                                 int end, bool col_const) const override {
        return _write_date_time_column_to_mysql(column, return_object_data_as_binary, result,
                                                row_idx, start, end, col_const);
    }

private:
    template <bool is_binary_format>
    Status _write_date_time_column_to_mysql(const IColumn& column,
                                            bool return_object_data_as_binary,
                                            std::vector<MysqlRowBuffer<is_binary_format>>& result,
                                            int row_idx, int start, int end, bool col_const) const {
        int buf_ret = 0;
        auto& data = assert_cast<const ColumnVector<Float64>&>(column).get_data();
        for (int i = start; i < end; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }
            const auto col_index = index_check_const(i, col_const);
            buf_ret = result[row_idx].push_time(data[col_index]);
            ++row_idx;
        }
        return Status::OK();
    }
};
} // namespace vectorized
} // namespace doris