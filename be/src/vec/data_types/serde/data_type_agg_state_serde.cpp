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

#include "vec/data_types/serde/data_type_agg_state_serde.h"

#include <cmath>

#include "util/url_coding.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_buffer.hpp"

namespace doris::vectorized {

void DataTypeAggStateSerde::_encode_to_base64(const char* data, size_t size,
                                              BufferWritable& bw) const {
    // 使用util中的base64_encode函数进行编码，避免创建临时string对象
    // base64编码后的长度：4 * ceil(input_length / 3)
    size_t encoded_size = (size_t)(4.0 * std::ceil(size / 3.0));
    std::string base64_encoded(encoded_size, '\0');
    size_t actual_len = base64_encode(reinterpret_cast<const unsigned char*>(data), size,
                                      reinterpret_cast<unsigned char*>(base64_encoded.data()));
    base64_encoded.resize(actual_len);

    // 将base64编码后的字符串写入buffer
    bw.write(base64_encoded.data(), base64_encoded.size());
}

Status DataTypeAggStateSerde::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                         BufferWritable& bw,
                                                         FormatOptions& options) const {
    // 检查列类型：AggState可能存储为ColumnString或ColumnFixedLengthObject
    if (const auto* col_string = check_and_get_column<ColumnString>(column)) {
        // 如果已经是字符串类型，直接使用base64编码
        const auto& value = col_string->get_data_at(row_num);
        _encode_to_base64(value.data, value.size, bw);
    } else if (const auto* col_fixed = check_and_get_column<ColumnFixedLengthObject>(column)) {
        // 如果是固定长度对象类型，获取二进制数据并编码
        const auto& value = col_fixed->get_data_at(row_num);
        _encode_to_base64(value.data, value.size, bw);
    } else {
        // 如果不是预期的列类型，尝试使用nested_serde，但会对结果进行base64编码
        // 这种情况通常不会发生，因为AggState的序列化类型通常是string或fixed_length_object
        // 为了兼容性，我们尝试将列数据先序列化为字符串，然后再base64编码
        auto tmp_col = ColumnString::create();
        VectorBufferWriter buffer_writer(*tmp_col.get());
        RETURN_IF_ERROR(
                _nested_serde->serialize_one_cell_to_json(column, row_num, buffer_writer, options));
        buffer_writer.commit();

        // 获取序列化后的字符串数据
        const auto& serialized_data = tmp_col->get_data_at(0);
        _encode_to_base64(serialized_data.data, serialized_data.size, bw);
    }

    return Status::OK();
}

Status DataTypeAggStateSerde::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    // Hive text格式同样需要base64编码
    return serialize_one_cell_to_json(column, row_num, bw, options);
}

} // namespace doris::vectorized
