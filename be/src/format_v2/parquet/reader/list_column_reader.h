// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
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

#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace doris::format::parquet {

class ListColumnReader final : public ParquetColumnReader {
public:
    ListColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                     std::unique_ptr<ParquetColumnReader> element_reader,
                     ParquetColumnReaderProfile profile = {})
            : ParquetColumnReader(schema, type, profile),
              _element_reader(std::move(element_reader)) {}

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;
    Status load_nested_batch(int64_t rows) override;
    Status load_nested_levels_batch(int64_t rows) override;
    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override;
    Status consume_nested_column(int64_t length_upper_bound, int64_t* values_consumed) override;
    const std::vector<int16_t>& nested_definition_levels() const override;
    const std::vector<int16_t>& nested_repetition_levels() const override;
    int64_t nested_levels_written() const override;
    bool is_or_has_repeated_child() const override;
    void advance_nested_build_level_cursor_past_parent(int16_t parent_repetition_level) override;

private:
    Status _consume_or_build_nested_column(int64_t length_upper_bound, MutableColumnPtr* column,
                                           int64_t* values_processed);

    std::unique_ptr<ParquetColumnReader>
            _element_reader; // element reader (recursive; may be Scalar/Struct/List/Map)
};

} // namespace doris::format::parquet
