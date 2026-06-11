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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace doris::parquet {

class StructColumnReader final : public ParquetColumnReader {
public:
    StructColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                       std::vector<std::unique_ptr<ParquetColumnReader>> children,
                       std::vector<int> child_output_indices,
                       ParquetColumnReaderProfile profile = {})
            : ParquetColumnReader(schema, type, profile),
              _children(std::move(children)),
              _child_output_indices(std::move(child_output_indices)) {
        DCHECK_EQ(_children.size(), _child_output_indices.size());
    }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;
    Status load_nested_batch(int64_t rows) override;
    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override;
    const std::vector<int16_t>& nested_definition_levels() const override;
    const std::vector<int16_t>& nested_repetition_levels() const override;
    int64_t nested_levels_written() const override;
    bool is_or_has_repeated_child() const override;

    size_t child_count() const { return _children.size(); }
    ParquetColumnReader* child_reader(size_t child_idx) const { return _children[child_idx].get(); }
    int child_output_index(size_t child_idx) const { return _child_output_indices[child_idx]; }

private:
    std::vector<std::unique_ptr<ParquetColumnReader>> _children;
    std::vector<int> _child_output_indices;
};

} // namespace doris::parquet
