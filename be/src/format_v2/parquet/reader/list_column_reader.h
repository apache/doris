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

#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace doris::format::parquet {

// LIST（数组）列的读取器，持有单个 element reader。
//
// 实现策略：
//   LIST 的物理 rep/def level 流由 element reader 提供。ListColumnReader 消费这些
//   levels 来重建 ColumnArray 的 offsets 和 null_map，然后将实际值委托给 element reader。
//
// 嵌套协议流程：
//   1. load_nested_batch() → element reader 加载 def/rep levels
//   2. build_nested_column() → 从 rep levels 计算每行的 entry_count，
//      通过 append_offsets() 写入 ColumnArray offsets，
//      从 def levels 判断 LIST 本身是否为 NULL，
//      然后委托 element reader 的 build_nested_column() 填充值。
//
// 平铺 read() 也走同样的逻辑，只是入口不同。
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
    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override;
    const std::vector<int16_t>& nested_definition_levels() const override;
    const std::vector<int16_t>& nested_repetition_levels() const override;
    int64_t nested_levels_written() const override;
    bool is_or_has_repeated_child() const override;

private:
    std::unique_ptr<ParquetColumnReader>
            _element_reader; // 元素 reader（递归，可能为 Scalar/Struct/List/Map）
};

} // namespace doris::format::parquet
