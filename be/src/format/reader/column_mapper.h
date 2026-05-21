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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
#include "format/reader/expr/literal.h"
#include "format/reader/file_reader.h"
namespace doris::reader {

struct TableColumn;
struct TableFilter;

enum class TableColumnMappingMode {
    BY_FIELD_ID,
    BY_NAME,
};

enum TableVirtualColumnType {
    INVALID = 0, // not a virtual column
    ROW_ID = 1,
    LAST_UPDATED_SEQUENCE_NUMBER = 2,
};

// 单个 table column 到 file column 的映射结果。
// 这是 table 层和 file 层的核心边界对象。
struct ColumnMapping {
    int32_t table_column_id = -1;
    std::optional<int32_t> file_column_id;
    std::optional<size_t> file_child_index;
    size_t table_child_index = 0;
    DataTypePtr file_type;
    DataTypePtr table_type;

    // 最终输出表达式。用于把 file-local value 转成 table/global value，例如 cast、
    // default、partition、generated column 或复杂列 remap。
    VExprContextSPtr projection;

    // 读时过滤 fallback 表达式。只在 table filter 不能安全转换成 file-local predicate
    // 时使用，服务 reader_expression_map，不等价于 finalize_expr。
    VExprContextSPtr reader_filter_expr;

    std::vector<ColumnMapping> child_mappings;
    bool is_trivial = false;
    bool is_constant = false;
    TableVirtualColumnType virtual_column_type = TableVirtualColumnType::INVALID;
    VExprContextSPtr default_expr;
};

struct TableColumnMapperOptions {
    TableColumnMappingMode mode = TableColumnMappingMode::BY_FIELD_ID;
    bool allow_missing_columns = true;
    bool enable_reader_expression_fallback = true;

    // Optional external name mapping for files that do not carry embedded field ids. The key is a
    // file-local field name/path and the value is the table/global field id. FileScanRequest still
    // uses SchemaField::id as the file-local column id.
    std::map<std::string, ColumnId> name_mapping;
};

// 通用 table schema 到 file schema 映射层。
// Iceberg 会使用 BY_FIELD_ID；普通 by-name 场景可以复用该组件，但不应把它命名成
// Iceberg-only 组件。
class TableColumnMapper {
public:
    explicit TableColumnMapper(TableColumnMapperOptions options = {})
            : _options(std::move(options)) {}
    virtual ~TableColumnMapper() = default;

    // 建立 table schema 到 file schema 的列映射。
    // 输出的 ColumnMapping 描述 table column 如何从 file column、常量列或表达式得到；
    // 后续 projection、filter localization 和 table block finalize 都应复用这份映射。
    virtual Status create_mapping(const std::vector<TableColumn>& projected_columns,
                                  const std::map<std::string, Field>& partition_values,
                                  const std::vector<SchemaField>& file_schema);

    // 把 table-level scan 请求转换成 file-local scan 请求。
    // table_request 使用 table/global schema；file_request 只包含 FileReader 能理解的
    // projected_file_columns、local_filters 和 reader_expression_map。
    virtual Status create_scan_request(const std::map<int32_t, TableFilter>& table_filters,
                                       const std::vector<TableColumn>& projected_columns,
                                       FileScanRequest* file_request);

    // 将 table-level filter 定位到文件 schema。
    // trivial mapping 可以直接复制结构化谓词；类型变化时可以尝试安全 cast；无法安全
    // 下推的表达式应通过 reader_expression_map 或 table-level finalize/filter fallback 处理。
    virtual Status localize_filters(const std::map<int32_t, TableFilter>& table_filters,
                                    FileScanRequest* file_request) const;
    const SchemaField* find_file_field_by_table_column_id(
            ColumnId table_column_id, const std::vector<SchemaField>& file_schema) const;
    void clear() { _mappings.clear(); }
    const std::vector<ColumnMapping>& mappings() const { return _mappings; }

private:
    Status _create_column_mapping(const TableColumn& table_column,
                                  std::optional<size_t> table_child_index,
                                  const std::vector<SchemaField>& file_schema,
                                  const std::map<std::string, Field>& partition_values,
                                  std::string_view field_path, size_t source_column_index,
                                  ColumnMapping* mapping);
    const SchemaField* _find_file_field(const TableColumn& table_column,
                                        const std::vector<SchemaField>& file_schema) const;
    const SchemaField* _find_file_field(const TableColumn& table_column,
                                        const std::vector<SchemaField>& file_schema,
                                        std::string_view field_path, size_t* field_index) const;
    std::optional<ColumnId> _mapped_field_id_by_name(std::string_view field_path) const;
    Status _create_struct_child_mappings(const TableColumn& table_column,
                                         const SchemaField& file_field,
                                         const std::map<std::string, Field>& partition_values,
                                         std::string_view field_path, ColumnMapping* mapping);
    Status _create_array_child_mapping(const TableColumn& table_column,
                                       const SchemaField& file_field,
                                       const std::map<std::string, Field>& partition_values,
                                       std::string_view field_path, ColumnMapping* mapping);
    Status _create_map_child_mappings(const TableColumn& table_column,
                                      const SchemaField& file_field,
                                      const std::map<std::string, Field>& partition_values,
                                      std::string_view field_path, ColumnMapping* mapping);

    const ColumnMapping* _find_mapping(ColumnId table_column_id) const {
        for (const auto& mapping : _mappings) {
            if (mapping.table_column_id == table_column_id) {
                return &mapping;
            }
        }
        return nullptr;
    }

    bool _is_same_type(const DataTypePtr& table_type, const DataTypePtr& file_type) const {
        return table_type != nullptr && file_type != nullptr && table_type->equals(*file_type);
    }

    TableColumnMapperOptions _options;
    std::vector<ColumnMapping> _mappings;
};

} // namespace doris::reader
