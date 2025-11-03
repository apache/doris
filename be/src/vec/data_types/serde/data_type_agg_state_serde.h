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

#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

// DataTypeAggStateSerde专门用于处理AggState类型在CSV导出时的序列化
// 它将AggState的二进制数据编码为base64字符串，以便在CSV中安全传输
class DataTypeAggStateSerde : public DataTypeSerDe {
public:
    // 构造函数：接收底层序列化类型的serde（通常是string或fixed_length_object）
    explicit DataTypeAggStateSerde(DataTypeSerDeSPtr nested_serde, int nesting_level = 1)
            : DataTypeSerDe(nesting_level), _nested_serde(nested_serde) {}

    std::string get_name() const override { return "AggState"; }

    // 重写serialize_one_cell_to_json方法，对AggState数据进行base64编码
    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    // 重写serialize_one_cell_to_hive_text方法，对AggState数据进行base64编码
    Status serialize_one_cell_to_hive_text(
            const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
            int hive_text_complex_type_delimiter_level) const override;

    // 其他方法委托给nested_serde处理
    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override {
        return _nested_serde->serialize_column_to_json(column, start_idx, end_idx, bw, options);
    }

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override {
        return _nested_serde->deserialize_one_cell_from_json(column, slice, options);
    }

    Status deserialize_one_cell_from_csv(IColumn& column, Slice& slice,
                                         const FormatOptions& options) const override {
        return _nested_serde->deserialize_one_cell_from_csv(column, slice, options);
    }

    Status deserialize_one_cell_from_hive_text(
            IColumn& column, Slice& slice, const FormatOptions& options,
            int hive_text_complex_type_delimiter_level) const override {
        return _nested_serde->deserialize_one_cell_from_hive_text(
                column, slice, options, hive_text_complex_type_delimiter_level);
    }

private:
    // 内部方法：将二进制数据编码为base64并写入buffer
    void _encode_to_base64(const char* data, size_t size, BufferWritable& bw) const;

    DataTypeSerDeSPtr _nested_serde; // 底层序列化类型的serde
};

} // namespace doris::vectorized
