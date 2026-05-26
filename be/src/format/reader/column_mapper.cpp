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

#include "format/reader/column_mapper.h"

#include <vector>

#include "common/status.h"
#include "format/reader/expr/cast.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"

namespace doris::reader {

static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
static constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER = "_last_updated_sequence_number";

Status TableColumnMapper::create_mapping(const std::vector<TableColumn>& projected_columns,
                                         const std::map<std::string, Field>& partition_values,
                                         const std::vector<SchemaField>& file_schema) {
    _mappings.clear();
    for (const auto& table_column : projected_columns) {
        ColumnMapping mapping;
        mapping.table_column_id = table_column.id;
        mapping.table_type = table_column.type;
        if (const auto* file_field = _find_file_field(table_column, file_schema)) {
            mapping.file_column_id = file_field->id;
            mapping.file_type = file_field->type;
            mapping.is_trivial = _is_same_type(mapping.table_type, mapping.file_type);
            if (!mapping.is_trivial) {
                // 1. Data type mismatch (caused by schema evolution) and casting is needed.
                auto expr = Cast::create_shared(mapping.table_type);
                expr->add_child(TableSlotRef::create_shared(mapping.file_column_id.value(),
                                                            mapping.file_column_id.value(), -1,
                                                            mapping.file_type, file_field->name));
                mapping.projection = VExprContext::create_shared(expr);
            } else {
                // 2. Data type matches, trivial mapping.
                mapping.projection = VExprContext::create_shared(TableSlotRef::create_shared(
                        mapping.file_column_id.value(), mapping.file_column_id.value(), -1,
                        mapping.file_type, file_field->name));
            }
        } else if (table_column.is_partition_key && partition_values.count(table_column.name) > 0) {
            // 3. Partition column, use partition value as a constant mapping. Note that partition column may also have default expression, but partition value should take precedence if it exists.
            mapping.default_expr = VExprContext::create_shared(TableLiteral::create_shared(
                    mapping.table_type, partition_values.at(table_column.name)));
        } else if (table_column.default_expr != nullptr) {
            // 4. Table column does not exist in file (column adding by schema evolution), which has a default expression, use it as a constant mapping.
            mapping.is_constant = true;
            mapping.default_expr = table_column.default_expr;
        } else if (table_column.name == ROW_LINEAGE_ROW_ID) {
            // 5. Virtual column, use special mapping to indicate it should be materialized by table reader instead of read from file or evaluated from expression.
            mapping.virtual_column_type = TableVirtualColumnType::ROW_ID;
        } else if (table_column.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
            mapping.virtual_column_type = TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER;
        } else {
            if (table_column.is_partition_key) {
                return Status::InvalidArgument(
                        "Table column '%s' (id=%d) does not have a matching partition value",
                        table_column.name);
            }
            if (!_options.allow_missing_columns) {
                return Status::InvalidArgument(
                        "Table column '%s' (id=%d) does not have a matching file column",
                        table_column.name, table_column.id);
            }
        }
        _mappings.push_back(std::move(mapping));
    }
    return Status::OK();
}

Status TableColumnMapper::create_scan_request(const std::map<int32_t, TableFilter>& table_filters,
                                              const std::vector<TableColumn>& projected_columns,
                                              FileScanRequest* file_request) {
    // 真实实现会把 table projection/filter 转换成 file-local projection/filter。
    file_request->predicate_columns.clear();
    file_request->non_predicate_columns.clear();
    file_request->local_filters.clear();
    file_request->reader_expression_map.clear();
    file_request->projected_columns.clear();
    for (const auto& table_column : projected_columns) {
        const auto* mapping = _find_mapping(table_column.id);
        if (mapping != nullptr && mapping->file_column_id.has_value()) {
            file_request->projected_columns.push_back(*mapping->file_column_id);
            if (table_filters.count(table_column.id) == 0) {
                file_request->non_predicate_columns.push_back(*mapping->file_column_id);
            }
        }
    }
    RETURN_IF_ERROR(localize_filters(table_filters, file_request));
    return Status::OK();
}

Status TableColumnMapper::localize_filters(const std::map<int32_t, TableFilter>& table_filters,
                                           FileScanRequest* file_request) const {
    // 真实实现会处理 trivial mapping、safe cast、reader expression fallback 和
    // finalize-only filter。stub 只复制能够直接定位到 file column 的谓词。
    for (const auto& it : table_filters) {
        const auto* mapping = _find_mapping(it.first);
        if (mapping == nullptr || !mapping->file_column_id.has_value()) {
            continue;
        }
        if (!it.second.can_be_localized()) {
            // TODO: Rewrite table filter to reader_expression_map
            // file_request->reader_expression_map.emplace_back(mapping->table_column_id, it.second.conjunct);
        } else {
            FileLocalFilter local_filter;
            local_filter.file_column_id = *mapping->file_column_id;
            local_filter.conjunct = it.second.conjunct;
            local_filter.predicates = it.second.predicates;
            file_request->local_filters.push_back(std::move(local_filter));
        }
        file_request->predicate_columns.push_back(*mapping->file_column_id);
    }
    return Status::OK();
}

const SchemaField* TableColumnMapper::_find_file_field(
        const TableColumn& table_column, const std::vector<SchemaField>& file_schema) const {
    for (const auto& field : file_schema) {
        if (_options.mode == TableColumnMappingMode::BY_FIELD_ID && field.id == table_column.id) {
            return &field;
        }
        if (field.name == table_column.name) {
            return &field;
        }
    }
    return nullptr;
}

} // namespace doris::reader
