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

#include "format_v2/parquet/reader/nested_column_materializer.h"

#include <cstdint>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"

namespace doris::format::parquet {

// Doris 顶层列总是 Nullable 包装的：ColumnNullable → ColumnArray/ColumnMap/ColumnStruct。
// 这些函数封装了穿透 Nullable wrapper 的逻辑，让调用方可以直接拿到嵌套 column。

ColumnArray* array_column_from_output(MutableColumnPtr& column) {
    // 穿透外层 ColumnNullable → 取内部 ColumnArray
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnArray*>(&nullable_column->get_nested_column());
    }
    // 非 nullable 路径（嵌套在 struct 内部时可能出现）
    return assert_cast<ColumnArray*>(column.get());
}

ColumnMap* map_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnMap*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnMap*>(column.get());
}

ColumnStruct* struct_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnStruct*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnStruct*>(column.get());
}

NullMap* null_map_from_nullable_output(MutableColumnPtr& column) {
    // 只有被 ColumnNullable 包装时才存在 null_map
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return &nullable_column->get_null_map_data();
    }
    // 嵌套在 required 父节点内部 → 没有独立的 null_map，父级负责标记
    return nullptr;
}

void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts) {
    // offsets 是累积值：offsets[i] = sum(entry_counts[0..i])
    // 最后一个 offset = 当前已累积的总元素数，用来作为下一次追加的起点
    offsets.reserve(offsets.size() + entry_counts.size());
    uint64_t current_offset = offsets.empty() ? 0 : offsets.back();
    for (const auto entry_count : entry_counts) {
        current_offset += entry_count;
        offsets.push_back(current_offset);
    }
}

void append_parent_nulls(NullMap* dst, const NullMap& src) {
    if (dst == nullptr) {
        return; // 目标列不是 nullable → 无需写入 null 标记
    }
    dst->insert(src.begin(), src.end());
}

} // namespace doris::format::parquet
