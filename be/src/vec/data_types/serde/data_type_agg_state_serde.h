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

// DataTypeAggStateSerde is specifically used for serializing AggState type during CSV export
// It encodes AggState's binary data as base64 string for safe transmission in CSV
class DataTypeAggStateSerde : public DataTypeSerDe {
public:
    // Constructor: receives the underlying serialized type's serde (usually string or fixed_length_object)
    explicit DataTypeAggStateSerde(DataTypeSerDeSPtr nested_serde, int nesting_level = 1)
            : DataTypeSerDe(nesting_level), _nested_serde(nested_serde) {}

    std::string get_name() const override { return "AggState"; }

    // Override serialize_one_cell_to_json method to base64 encode AggState data
    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    // Override serialize_one_cell_to_hive_text method to base64 encode AggState data
    Status serialize_one_cell_to_hive_text(
            const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
            int hive_text_complex_type_delimiter_level) const override;

    // Other methods delegate to nested_serde
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
    // Internal method: encode binary data to base64 and write to buffer
    void _encode_to_base64(const char* data, size_t size, BufferWritable& bw) const;

    DataTypeSerDeSPtr _nested_serde; // Serde of the underlying serialized type
};

} // namespace doris::vectorized
