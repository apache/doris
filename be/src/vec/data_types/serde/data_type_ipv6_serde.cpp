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

#include <string>

#include "vec/columns/column_const.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {

template <bool is_binary_format>
Status DataTypeIPv6SerDe::_write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<is_binary_format>& result,
                                                 int row_idx, bool col_const,
                                                 const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnVector<IPv6>&>(column).get_data();
    auto col_index = index_check_const(row_idx, col_const);
    IPv6Value ipv6_val(data[col_index]);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_ipv6(ipv6_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeIPv6SerDe::write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                bool col_const,
                                                const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeIPv6SerDe::write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                bool col_const,
                                                const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeIPv6SerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                     BufferWritable& bw,
                                                     FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    IPv6 data = assert_cast<const ColumnIPv6&>(*ptr).get_element(row_num);
    IPv6Value ipv6_value(data);
    std::string ipv6_str = ipv6_value.to_string();
    bw.write(ipv6_str.c_str(), ipv6_str.length());
    return Status::OK();
}

Status DataTypeIPv6SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                         const FormatOptions& options) const {
    auto& column_data = reinterpret_cast<ColumnIPv6&>(column);
    ReadBuffer rb(slice.data, slice.size);
    IPv6 val = 0;
    if (!read_ipv6_text_impl(val, rb)) {
        return Status::InvalidArgument("parse ipv6 fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeIPv6SerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                             int end) const {
    const auto& column_data = assert_cast<const ColumnIPv6&>(column);
    result.mutable_bytes_value()->Reserve(end - start);
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::IPV6);
    for (int i = start; i < end; ++i) {
        const auto& val = column_data.get_data_at(i);
        result.add_bytes_value(val.data, val.size);
    }
    return Status::OK();
}

Status DataTypeIPv6SerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col_data = assert_cast<ColumnIPv6&>(column).get_data();
    auto old_column_size = column.size();
    col_data.resize(old_column_size + arg.bytes_value_size());
    for (int i = 0; i < arg.bytes_value_size(); ++i) {
        col_data[old_column_size + i] = *(IPv6*)(arg.bytes_value(i).c_str());
    }
    return Status::OK();
}

void DataTypeIPv6SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                              arrow::ArrayBuilder* array_builder, int start,
                                              int end, const cctz::time_zone& ctz) const {
    const auto& col_data = assert_cast<const ColumnIPv6&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            std::string ipv6_str = IPv6Value::to_string(col_data[i]);
            checkArrowStatus(string_builder.Append(ipv6_str.c_str(), ipv6_str.size()),
                             column.get_name(), array_builder->type()->name());
        }
    }
}

void DataTypeIPv6SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                               int start, int end,
                                               const cctz::time_zone& ctz) const {
    auto& col_data = assert_cast<ColumnIPv6&>(column).get_data();
    const auto* concrete_array = assert_cast<const arrow::StringArray*>(arrow_array);
    std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

    for (size_t offset_i = start; offset_i < end; ++offset_i) {
        if (!concrete_array->IsNull(offset_i)) {
            const char* raw_data = reinterpret_cast<const char*>(
                    buffer->data() + concrete_array->value_offset(offset_i));
            const auto raw_data_len = concrete_array->value_length(offset_i);

            IPv6 ipv6_val;
            if (!IPv6Value::from_string(ipv6_val, raw_data, raw_data_len)) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "parse number fail, string: '{}'",
                                       std::string(raw_data, raw_data_len).c_str());
            }
            col_data.emplace_back(ipv6_val);
        }
    }
}

Status DataTypeIPv6SerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                              const NullMap* null_map,
                                              orc::ColumnVectorBatch* orc_col_batch, int start,
                                              int end, std::vector<StringRef>& buffer_list) const {
    const auto& col_data = assert_cast<const ColumnIPv6&>(column).get_data();
    orc::StringVectorBatch* cur_batch = assert_cast<orc::StringVectorBatch*>(orc_col_batch);
    char* ptr = (char*)malloc(BUFFER_UNIT_SIZE);
    if (!ptr) {
        return Status::InternalError(
                "malloc memory error when write largeint column data to orc file.");
    }
    StringRef bufferRef;
    bufferRef.data = ptr;
    bufferRef.size = BUFFER_UNIT_SIZE;
    size_t offset = 0;
    const size_t begin_off = offset;

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }
        std::string ipv6_str = IPv6Value::to_string(col_data[row_id]);
        size_t len = ipv6_str.size();

        REALLOC_MEMORY_FOR_ORC_WRITER()

        strcpy(const_cast<char*>(bufferRef.data) + offset, ipv6_str.c_str());
        offset += len;
        cur_batch->length[row_id] = len;
    }
    size_t data_off = 0;
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + begin_off + data_off;
            data_off += cur_batch->length[row_id];
        }
    }
    buffer_list.emplace_back(bufferRef);
    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
