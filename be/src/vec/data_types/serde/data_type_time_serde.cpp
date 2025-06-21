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

#include "vec/runtime/time_value.h"
namespace doris {
namespace vectorized {
#include "common/compile_check_begin.h"

void write_time(const typename PrimitiveTypeTraits<TYPE_TIMEV2>::ColumnItemType& value,
                BufferWritable& bw, int scale) {
    auto time_str = TimeValue::to_string(value, scale);
    bw.write(time_str.data(), time_str.size());
}

Status DataTypeTimeV2SerDe::serialize_column_to_text(const IColumn& column, int64_t row_num,
                                                     BufferWritable& bw) const {
    const auto& time_column = assert_cast<const ColumnTimeV2&>(column);
    DataTypeSerDe::write_left_quotation(bw);
    write_time(time_column.get_element(row_num), bw, scale);
    DataTypeSerDe::write_right_quotation(bw);
    return Status::OK();
}

Result<ColumnString::Ptr> DataTypeTimeV2SerDe::serialize_column_to_column_string(
        const IColumn& column) const {
    const auto size = column.size();
    auto column_to = ColumnString::create();
    const size_t output_length = sizeof("HH:MM:SS") + scale;
    column_to->reserve(size * output_length);
    BufferWritable write_buffer(*column_to);
    const auto& col = assert_cast<const ColumnTimeV2&>(column);
    for (size_t i = 0; i < size; ++i) {
        write_time(col.get_element(i), write_buffer, scale);
        write_buffer.commit();
    }
    return column_to;
}

Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer,
                                                  int64_t row_idx, bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
template <bool is_binary_format>
Status DataTypeTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnTimeV2&>(column).get_data();
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
