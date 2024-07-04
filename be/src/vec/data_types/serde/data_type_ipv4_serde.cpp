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

#include "vec/columns/column_const.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {

template <bool is_binary_format>
Status DataTypeIPv4SerDe::_write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<is_binary_format>& result,
                                                 int row_idx, bool col_const,
                                                 const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnVector<IPv4>&>(column).get_data();
    auto col_index = index_check_const(row_idx, col_const);
    IPv4Value ipv4_val(data[col_index]);
    // _nesting_level >= 2 means this ipv4 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_ipv4(ipv4_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeIPv4SerDe::write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                bool col_const,
                                                const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeIPv4SerDe::write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                bool col_const,
                                                const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeIPv4SerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                     BufferWritable& bw,
                                                     FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    IPv4 data = assert_cast<const ColumnIPv4&>(*ptr).get_element(row_num);
    IPv4Value ipv4_value(data);
    std::string ipv4_str = ipv4_value.to_string();
    bw.write(ipv4_str.c_str(), ipv4_str.length());
    return Status::OK();
}

Status DataTypeIPv4SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                         const FormatOptions& options) const {
    auto& column_data = reinterpret_cast<ColumnIPv4&>(column);
    ReadBuffer rb(slice.data, slice.size);
    IPv4 val = 0;
    if (!read_ipv4_text_impl(val, rb)) {
        return Status::InvalidArgument("parse ipv4 fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeIPv4SerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                             int end) const {
    const auto& column_data = assert_cast<const ColumnIPv4&>(column).get_data();
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::IPV4);
    auto* values = result.mutable_uint32_value();
    values->Reserve(end - start);
    values->Add(column_data.begin() + start, column_data.begin() + end);
    return Status::OK();
}

Status DataTypeIPv4SerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col_data = assert_cast<ColumnIPv4&>(column).get_data();
    auto old_column_size = column.size();
    column.resize(old_column_size + arg.uint32_value_size());
    for (int i = 0; i < arg.uint32_value_size(); ++i) {
        col_data[old_column_size + i] = arg.uint32_value(i);
    }
    return Status::OK();
}

void DataTypeIPv4SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                              arrow::ArrayBuilder* array_builder, int start,
                                              int end, const cctz::time_zone& ctz) const {
    const auto& col_data = assert_cast<const ColumnIPv4&>(column).get_data();
    auto& int32_builder = assert_cast<arrow::Int32Builder&>(*array_builder);
    auto arrow_null_map = revert_null_map(null_map, start, end);
    auto* arrow_null_map_data = arrow_null_map.empty() ? nullptr : arrow_null_map.data();
    checkArrowStatus(int32_builder.AppendValues(
                             reinterpret_cast<const Int32*>(col_data.data()) + start, end - start,
                             reinterpret_cast<const uint8_t*>(arrow_null_map_data)),
                     column.get_name(), array_builder->type()->name());
}

void DataTypeIPv4SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                               int start, int end,
                                               const cctz::time_zone& ctz) const {
    auto& col_data = assert_cast<ColumnIPv4&>(column).get_data();
    int row_count = end - start;
    /// buffers[0] is a null bitmap and buffers[1] are actual values
    std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
    const auto* raw_data = reinterpret_cast<const UInt32*>(buffer->data()) + start;
    col_data.insert(raw_data, raw_data + row_count);
}
} // namespace vectorized
} // namespace doris
