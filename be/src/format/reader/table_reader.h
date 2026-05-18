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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
#include "format/reader/file_reader.h"

namespace doris {
class Block;
class ColumnPredicate;
} // namespace doris

namespace doris::reader {

// table/global schema 中的列视图。
// Iceberg 场景下，id 默认对应 Iceberg field id。该结构不描述文件中的物理列。
struct TableColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
    std::vector<TableColumn> children;
};

// table-level filter。
// TableColumnMapper 负责把它转换成 FileLocalFilter 或 reader_expression_map。
struct TableFilter {
    ColumnId table_column_id = -1;

    // 表达式过滤，适合表达 cast、复杂表达式、复杂列提取等语义。
    VExprContextSPtr conjunct;

    // 结构化列谓词，适合下推到文件层做 row group stats、page index、dictionary、
    // bloom filter 等优化。
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};

// 单个 table column 到 file column 的映射结果。
// 这是 table 层和 file 层的核心边界对象。
struct ColumnMapping {
    ColumnId table_column_id = -1;
    std::optional<ColumnId> file_column_id;
    DataTypePtr file_type;
    DataTypePtr table_type;

    // 最终输出表达式。用于把 file-local value 转成 table/global value，例如 cast、
    // default、partition、generated column 或复杂列 remap。
    VExprContextSPtr finalize_expr;

    // 读时过滤 fallback 表达式。只在 table filter 不能安全转换成 file-local predicate
    // 时使用，服务 reader_expression_map，不等价于 finalize_expr。
    VExprContextSPtr reader_filter_expr;

    std::vector<ColumnMapping> child_mappings;
    bool is_trivial = false;
    bool is_constant = false;
};

enum class TableColumnMappingMode {
    BY_FIELD_ID,
    BY_NAME,
};

enum class TableFilterConversion {
    COPY_DIRECTLY,
    CAST_FILTER,
    EVALUATE_EXPRESSION,
    FINALIZE_ONLY,
};

struct TableColumnMapperOptions {
    TableColumnMappingMode mode = TableColumnMappingMode::BY_FIELD_ID;
    bool allow_missing_columns = true;
    bool enable_reader_expression_fallback = true;
};

// table-level scan 请求。
// 它仍然使用 table/global schema 语义，不能直接传给 FileReader。
struct TableScanRequest {
    std::vector<TableColumn> projected_table_columns;
    std::vector<TableFilter> table_filters;
};

// 通用 table schema 到 file schema 映射层。
// Iceberg 会使用 BY_FIELD_ID；普通 by-name 场景可以复用该组件，但不应把它命名成
// Iceberg-only 组件。
class TableColumnMapper {
public:
    explicit TableColumnMapper(TableColumnMapperOptions options = {}) : _options(std::move(options)) {}
    virtual ~TableColumnMapper() = default;

    virtual Status create_mapping(const std::vector<TableColumn>& table_schema,
                                  const std::vector<SchemaField>& file_schema,
                                  std::vector<ColumnMapping>* mappings) {
        // 真实实现会做 field id/name matching、类型转换、复杂列 child mapping、缺失列
        // default/partition/generated 表达式构造。
        mappings->clear();
        for (const auto& table_column : table_schema) {
            ColumnMapping mapping;
            mapping.table_column_id = table_column.id;
            mapping.table_type = table_column.type;
            if (const auto* file_field = find_file_field(table_column, file_schema)) {
                mapping.file_column_id = file_field->id;
                mapping.file_type = file_field->type;
                mapping.is_trivial = is_same_type(mapping.table_type, mapping.file_type);
            } else {
                mapping.is_constant = true;
            }
            mappings->push_back(std::move(mapping));
        }
        _mappings = *mappings;
        return Status::OK();
    }

    virtual Status create_scan_request(const TableScanRequest& table_request,
                                       const std::vector<ColumnMapping>& mappings,
                                       FileScanRequest* file_request) {
        // 真实实现会把 table projection/filter 转换成 file-local projection/filter。
        file_request->projected_file_columns.clear();
        file_request->local_filters.clear();
        file_request->reader_expression_map.clear();
        _mappings = mappings;
        for (const auto& table_column : table_request.projected_table_columns) {
            const auto* mapping = find_mapping(table_column.id);
            if (mapping != nullptr && mapping->file_column_id.has_value()) {
                file_request->projected_file_columns.push_back(*mapping->file_column_id);
            }
        }
        RETURN_IF_ERROR(localize_filters(table_request.table_filters, file_request));
        return Status::OK();
    }

    virtual Status localize_filters(const std::vector<TableFilter>& table_filters,
                                    FileScanRequest* file_request) const {
        // 真实实现会处理 trivial mapping、safe cast、reader expression fallback 和
        // finalize-only filter。stub 只复制能够直接定位到 file column 的谓词。
        for (const auto& filter : table_filters) {
            const auto* mapping = find_mapping(filter.table_column_id);
            if (mapping == nullptr || !mapping->file_column_id.has_value()) {
                continue;
            }
            FileLocalFilter local_filter;
            local_filter.file_column_id = *mapping->file_column_id;
            local_filter.conjunct = filter.conjunct;
            local_filter.predicates = filter.predicates;
            file_request->local_filters.push_back(std::move(local_filter));
        }
        return Status::OK();
    }

    const std::vector<ColumnMapping>& mappings() const { return _mappings; }

private:
    const SchemaField* find_file_field(
            const TableColumn& table_column,
            const std::vector<SchemaField>& file_schema) const {
        for (const auto& field : file_schema) {
            if (_options.mode == TableColumnMappingMode::BY_FIELD_ID && field.id == table_column.id) {
                return &field;
            }
            if (_options.mode == TableColumnMappingMode::BY_NAME && field.name == table_column.name) {
                return &field;
            }
        }
        return nullptr;
    }

    const ColumnMapping* find_mapping(ColumnId table_column_id) const {
        for (const auto& mapping : _mappings) {
            if (mapping.table_column_id == table_column_id) {
                return &mapping;
            }
        }
        return nullptr;
    }

    bool is_same_type(const DataTypePtr& table_type, const DataTypePtr& file_type) const {
        return table_type == file_type;
    }

private:
    TableColumnMapperOptions _options;
    std::vector<ColumnMapping> _mappings;
};

struct TableReadOptions {
    size_t batch_size = 4096;
};

// table-level reader 基类。
// 该层负责多文件编排和动态分区裁剪等通用 table-level 逻辑，对外输出 table block。
class TableReader {
public:
    virtual ~TableReader() = default;

    virtual Status init(const TableReadOptions& options) {
        _options = options;
        return Status::OK();
    }

    virtual Status filter(const VExprContextSPtr& expr, bool* can_filter_all) {
        // 真实实现会基于 split/partition/file stats 判断动态分区裁剪结果。
        (void)expr;
        if (can_filter_all != nullptr) {
            *can_filter_all = false;
        }
        return Status::OK();
    }

    virtual Status next_reader() {
        // 真实实现会切换到下一个 data file / split reader。
        return Status::OK();
    }

    virtual Status next(Block* table_block, size_t* rows, bool* eof) {
        (void)table_block;
        if (rows != nullptr) {
            *rows = 0;
        }
        if (eof != nullptr) {
            *eof = true;
        }
        return Status::OK();
    }

    virtual Status close() { return Status::OK(); }

protected:
    TableReadOptions _options;
};

} // namespace doris::reader
