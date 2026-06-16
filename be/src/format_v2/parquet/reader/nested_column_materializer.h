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
#include <vector>

#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"

namespace doris::format::parquet {

// ============================================================================
// Doris Column 嵌套类型访问辅助函数
//
// 复杂 reader（StructColumnReader、ListColumnReader、MapColumnReader）通过
// 这些函数从输出 Column 中获取嵌套部分的指针。
//
// 背景：Doris 的顶层列总是 Nullable 包装的（ColumnNullable），嵌套列本身
// （ColumnArray、ColumnMap、ColumnStruct）在 ColumnNullable 内部。这些函数
// 封装了"穿透 Nullable wrapper → 获取嵌套 column"的逻辑。
// ============================================================================

// 从输出列获取 ColumnArray* — 自动穿透外层的 ColumnNullable wrapper。
ColumnArray* array_column_from_output(MutableColumnPtr& column);

// 从输出列获取 ColumnMap* — 自动穿透外层的 ColumnNullable wrapper。
ColumnMap* map_column_from_output(MutableColumnPtr& column);

// 从输出列获取 ColumnStruct* — 自动穿透外层的 ColumnNullable wrapper。
ColumnStruct* struct_column_from_output(MutableColumnPtr& column);

// 从输出列获取 NullMap* — 自动穿透外层的 ColumnNullable wrapper。
// 如果输出列不是 ColumnNullable（嵌套在非 nullable 的 struct 内部），返回 nullptr。
NullMap* null_map_from_nullable_output(MutableColumnPtr& column);

// 将 entry_counts 数组追加为 ColumnArray 的 offsets。
// entry_counts[i] 表示第 i 个顶层行包含的元素数量，
// offsets[i] = offsets[i-1] + entry_counts[i]。
// 例如 entry_counts = [3, 0, 2] → offsets = [3, 3, 5]。
void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts);

// 将 src 中的 null 标记追加到 dst 之后。
// 用于 struct/literal null 嵌套场景：当父级为 NULL 时，其子列也需要对应行标记为 NULL。
void append_parent_nulls(NullMap* dst, const NullMap& src);

} // namespace doris::format::parquet
