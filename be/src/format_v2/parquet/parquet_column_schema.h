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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format_v2/parquet/parquet_type.h"

namespace parquet {
class ColumnDescriptor;
class SchemaDescriptor;
} // namespace parquet

namespace doris::format::parquet {

// Schema 节点类型枚举，决定 ParquetColumnReaderFactory 创建哪种 Reader。
enum class ParquetColumnSchemaKind {
    PRIMITIVE, // 基本类型叶子 → ScalarColumnReader
    STRUCT,    // 结构体 → StructColumnReader
    LIST,      // 数组 → ListColumnReader
    MAP,       // 字典 → MapColumnReader
};

// ============================================================================
// Parquet 文件的 file-local schema 树 — build_parquet_column_schema() 的输出
// ============================================================================
//
// 该树描述 Parquet 逻辑字段及其到物理 leaf column 的映射，将 Arrow 的物理 schema
// （含 wrapper groups、Dremel levels）转换为 Doris reader 可以直接消费的语义化 schema。
//
// 关键设计决策：
// - LIST/MAP 的物理 wrapper group 在构建时被折叠，children 直接是 [element] 或 [key, value]。
//   wrapper 的 repeated 属性转为父节点的 level 字段，reader 通过 levels 重建嵌套结构。
// - 所有类型统一 nullable（Doris external table 的策略：外部数据不可信）。
// - Dremel levels 在 child_context() 中逐层累加，复杂 reader 用它们从 leaf 的 level 流中
//   重建嵌套容器（offsets + null_map）。
// ============================================================================
struct ParquetColumnSchema {
    // ======== 标识 ========

    // 在父节点 children 中的序号。顶层字段使用 root field ordinal。
    // ParquetColumnReaderFactory 通过 LocalColumnIndex 携带的 local_id 路径递归定位 reader。
    int local_id = -1;

    // Parquet 序列化 schema 中的 field_id attribute（-1 表示文件未定义）。
    // 仅用于 Iceberg 等 table format 的 schema matching 标识，不用于 reader 寻址。
    int parquet_field_id = -1;

    std::string name;

    // ======== 类型 ========

    // Doris DataType。复杂类型的 children 已递归 nullable。
    DataTypePtr type = nullptr;

    // Parquet 物理 leaf column 序号。
    // PRIMITIVE 节点才有有效值，用于访问 ColumnDescriptor、RecordReader、ColumnChunk、Statistics。
    // 复杂类型节点本身不是物理列，值为 -1。
    int leaf_column_id = -1;

    // 从 leaf ColumnDescriptor 解析出的类型编码信息。仅 PRIMITIVE 节点有效。
    ParquetTypeDescriptor type_descriptor {};

    ParquetColumnSchemaKind kind = ParquetColumnSchemaKind::PRIMITIVE;

    // Arrow ColumnDescriptor 指针。仅 PRIMITIVE 节点有效，复杂节点为 nullptr。
    const ::parquet::ColumnDescriptor* descriptor = nullptr;

    // ======== Dremel Levels ========

    // 该子树中的最大 def/rep level。PRIMITIVE 从 ColumnDescriptor 获取，复杂节点从 children 上报。
    int16_t max_definition_level = 0;
    int16_t max_repetition_level = 0;

    // 使本节点自身变为 nullable 的 def level 阈值。
    // 复杂 reader 用此值区分"我的值是 NULL"（def < threshold）和"我有值但内容为空"（def >= threshold）。
    int16_t nullable_definition_level = 0;

    // 从 root 到本节点的累计 def/rep level。在 child_context() 中逐层 +1。
    int16_t definition_level = 0;
    int16_t repetition_level = 0;

    // 最近 repeated 祖先的 def level。
    int16_t repeated_ancestor_definition_level = 0;

    // 最近 repeated 祖先的 rep level。
    // LIST/MAP reader 用此值从孩子 rep level 流中判断"新元素开始"（rep_level >= threshold）。
    int16_t repeated_repetition_level = 0;

    // ======== 子树 ========
    // LIST: [element]，MAP: [key, value]，STRUCT: 直接孩子，PRIMITIVE: 空
    std::vector<std::unique_ptr<ParquetColumnSchema>> children {};
};

// 从 Arrow Parquet SchemaDescriptor 构造 file-local schema tree。
// 这是 init() 阶段最重要的转换：物理 schema（含 wrapper groups）→ 语义化 schema（wrapper 已折叠）。
Status build_parquet_column_schema(const ::parquet::SchemaDescriptor& schema,
                                   std::vector<std::unique_ptr<ParquetColumnSchema>>* fields);

} // namespace doris::format::parquet
