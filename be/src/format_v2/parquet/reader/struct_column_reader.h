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

namespace doris::format::parquet {

// STRUCT 列的读取器，持有多个子 reader 并按需协调读取。
//
// 实现策略：平铺读取和嵌套协议都委托给子 reader 完成。
// - read(): 分别让每个被 projected 的子 reader 读取对应行数，组装 ColumnStruct。
//   如果子 reader 自身是复杂类型（如 LIST inside STRUCT），递归调用其 read()。
// - 嵌套协议：选择一个 shape source reader（第一个包含足够 level 信息的子 reader），
//   从它读取 def/rep levels 来确定 struct 的 null 状态，然后让所有子 reader 同步构建。
//
// _child_output_indices 用于部分 projection 场景：
//   例如 STRUCT<a INT, b STRING, c INT> 只读 a 和 c，
//   则 _children = [a_reader, c_reader], _child_output_indices = [0, 2]。
//   build_nested_column 时按 indices 将子列写入 ColumnStruct 的正确位置。
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

private:
    // 选择提供 shape 信息的子 reader（第一个非空或包含 repeated 子节点的 reader）。
    // shape source 的 def/rep levels 用于确定 struct 的 null 状态和嵌套边界。
    ParquetColumnReader* shape_source_reader() const;

    std::vector<std::unique_ptr<ParquetColumnReader>> _children; // 被 projected 的子 reader 列表
    std::vector<int> _child_output_indices; // 子 reader → struct 输出位置的映射
};

} // namespace doris::format::parquet
