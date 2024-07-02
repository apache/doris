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
namespace doris {
namespace vectorized {

template <bool is_binary_format>
Status DataTypeTimeSerDe::_write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<is_binary_format>& result,
                                                 int row_idx, bool col_const,
                                                 const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnFloat64&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    if (UNLIKELY(0 != result.push_time(data[col_index]))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

Status DataTypeTimeSerDe::write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                bool col_const,
                                                const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeTimeSerDe::write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                bool col_const,
                                                const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
template <bool is_binary_format>
Status DataTypeTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnFloat64&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    // _nesting_level >= 2 means this time is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_timev2(data[col_index], scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris
