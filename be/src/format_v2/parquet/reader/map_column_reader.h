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

// MAP 列的读取器，持有 key reader 和 value reader。
//
// key reader 始终完整读取（不做 projection 裁剪），因为它拥有：
//   - entry 的存在性：null key → entry 无效
//   - offsets 信息：从 key 的 rep levels 确定每个顶层行有多少个 entry
//   - key 唯一性语义：重复 key 的行为由引擎层决定
//
// 嵌套协议流程：
//   1. load_nested_batch() → 分别加载 key reader 和 value reader
//   2. build_nested_column() →
//      a. 从 key reader 的 rep levels 计算 entry_counts → 设置 ColumnMap offsets
//      b. 从 key reader 的 def levels 判断 MAP 本身和每个 entry 的 null 状态
//      c. 校验：key 为 NULL 的 entry 被标记为无效（兼容 Hive 的非标准 optional key）
//      d. 委托 key reader 的 build_nested_column() 填充 keys
//      e. 委托 value reader 的 build_nested_column() 填充 values
//
// MapColumnReader 是 ScalarColumnReader 的 friend，可以直接访问其内部方法
// 来逐个读取 key value 做 entry 校验。
class MapColumnReader final : public ParquetColumnReader {
public:
    MapColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                    std::unique_ptr<ParquetColumnReader> key_reader,
                    std::unique_ptr<ParquetColumnReader> value_reader,
                    ParquetColumnReaderProfile profile = {})
            : ParquetColumnReader(schema, type, profile),
              _key_reader(std::move(key_reader)),
              _value_reader(std::move(value_reader)) {}

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
    std::unique_ptr<ParquetColumnReader> _key_reader; // key 列 reader（始终完整读取）
    std::unique_ptr<ParquetColumnReader> _value_reader; // value 列 reader（可按 projection 裁剪）
};

} // namespace doris::format::parquet
