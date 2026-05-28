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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format/reader/expr/cast.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"

namespace doris::reader {

struct FileSlotRewriteInfo {
    size_t block_position = 0;
    DataTypePtr file_type;
    DataTypePtr table_type;
    std::string file_column_name;
};

static VExprSPtr rewrite_table_expr_to_file_expr(
        const VExprSPtr& expr,
        const std::map<int32_t, FileSlotRewriteInfo>& table_column_to_file_slot) {
    if (expr == nullptr) {
        return nullptr;
    }
    if (expr->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
        const auto rewrite_it = table_column_to_file_slot.find(slot_ref->slot_id());
        if (rewrite_it != table_column_to_file_slot.end()) {
            const auto& rewrite_info = rewrite_it->second;
            auto file_slot = TableSlotRef::create_shared(
                    slot_ref->slot_id(), cast_set<int>(rewrite_info.block_position), -1,
                    rewrite_info.file_type, rewrite_info.file_column_name);
            if (rewrite_info.file_type->equals(*rewrite_info.table_type)) {
                return file_slot;
            }
            auto cast_expr = Cast::create_shared(rewrite_info.table_type);
            cast_expr->add_child(std::move(file_slot));
            return cast_expr;
        }
        return expr;
    }

    // VExpr currently does not provide a generic deep-clone API for arbitrary expression types.
    // Keep all slot-localization mutation inside ColumnMapper and rebuild it for every split
    // before the localized expression is prepared/opened by TableReader.
    VExprSPtrs rewritten_children;
    rewritten_children.reserve(expr->children().size());
    for (const auto& child : expr->children()) {
        rewritten_children.push_back(
                rewrite_table_expr_to_file_expr(child, table_column_to_file_slot));
    }
    expr->set_children(std::move(rewritten_children));
    return expr;
}

static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
static constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER = "_last_updated_sequence_number";

static void add_scan_column(FileScanRequest* file_request, ColumnId file_column_id,
                            std::vector<ColumnId>* scan_columns) {
    // column_positions is the global read-column index for this scan request, so it also
    // deduplicates predicate_columns and non_predicate_columns across all filter/projection paths.
    if (file_request->column_positions.count(file_column_id) == 0) {
        file_request->column_positions.emplace(file_column_id,
                                               file_request->column_positions.size());
        scan_columns->push_back(file_column_id);
    }
}

static void rebuild_projection(ColumnMapping* mapping, size_t block_position) {
    DORIS_CHECK(mapping->file_column_id.has_value());
    if (mapping->is_trivial) {
        mapping->projection = VExprContext::create_shared(TableSlotRef::create_shared(
                cast_set<int>(block_position), cast_set<int>(block_position), -1,
                mapping->file_type, mapping->file_column_name));
        return;
    }

    auto expr = Cast::create_shared(mapping->table_type);
    expr->add_child(TableSlotRef::create_shared(cast_set<int>(block_position),
                                                cast_set<int>(block_position), -1,
                                                mapping->file_type, mapping->file_column_name));
    mapping->projection = VExprContext::create_shared(expr);
}

// Build a map from table column id to file slot rewrite info for all columns in the given mappings that have a file column id and are present in the file request.
static std::map<int32_t, FileSlotRewriteInfo> build_file_slot_rewrite_map(
        const std::vector<ColumnMapping>& mappings, const FileScanRequest& file_request) {
    std::map<int32_t, FileSlotRewriteInfo> table_column_to_file_slot;
    for (const auto& mapping : mappings) {
        if (!mapping.file_column_id.has_value()) {
            continue;
        }
        const auto position_it = file_request.column_positions.find(*mapping.file_column_id);
        if (position_it != file_request.column_positions.end()) {
            table_column_to_file_slot.emplace(
                    mapping.table_column_id,
                    FileSlotRewriteInfo {.block_position = position_it->second,
                                         .file_type = mapping.file_type,
                                         .table_type = mapping.table_type,
                                         .file_column_name = mapping.file_column_name});
        }
    }
    return table_column_to_file_slot;
}

static bool is_complex_type(const DataTypePtr& type) {
    DORIS_CHECK(type != nullptr);
    const auto primitive_type = remove_nullable(type)->get_primitive_type();
    return primitive_type == TYPE_STRUCT || primitive_type == TYPE_ARRAY ||
           primitive_type == TYPE_MAP;
}

static const SchemaField* find_file_child_by_table_column(
        const TableColumn& table_column, const std::vector<SchemaField>& file_children,
        TableColumnMappingMode mode) {
    for (const auto& field : file_children) {
        if (mode == TableColumnMappingMode::BY_FIELD_ID && !field.field_id_path.empty() &&
            field.field_id_path.back() != -1 && field.field_id_path.back() == table_column.id) {
            return &field;
        }
        if (field.name == table_column.name) {
            return &field;
        }
    }
    return nullptr;
}

static bool complex_projection_has_pruned_children(const ColumnMapping& mapping) {
    if (!is_complex_type(mapping.file_type)) {
        return false;
    }
    if (mapping.child_mappings.empty()) {
        return false;
    }
    DORIS_CHECK(mapping.file_type != nullptr);
    DORIS_CHECK(mapping.table_type != nullptr);
    if (remove_nullable(mapping.file_type)->get_primitive_type() !=
        remove_nullable(mapping.table_type)->get_primitive_type()) {
        return true;
    }
    if (!mapping.table_type->equals(*mapping.file_type)) {
        return true;
    }
    for (const auto& child_mapping : mapping.child_mappings) {
        if (!child_mapping.file_column_id.has_value() ||
            complex_projection_has_pruned_children(child_mapping)) {
            return true;
        }
    }
    return false;
}

static Status rebuild_projected_file_type(ColumnMapping* mapping) {
    if (mapping == nullptr) {
        return Status::InvalidArgument("mapping is null");
    }
    DORIS_CHECK(is_complex_type(mapping->file_type));
    DataTypes child_types;
    Strings child_names;
    child_types.reserve(mapping->child_mappings.size());
    child_names.reserve(mapping->child_mappings.size());
    for (auto& child_mapping : mapping->child_mappings) {
        if (!child_mapping.file_column_id.has_value()) {
            continue;
        }
        if (complex_projection_has_pruned_children(child_mapping)) {
            RETURN_IF_ERROR(rebuild_projected_file_type(&child_mapping));
        }
        child_types.push_back(child_mapping.file_type);
        child_names.push_back(child_mapping.file_column_name);
    }
    if (child_types.empty()) {
        return Status::NotSupported("Projection for complex column {} contains no file children",
                                    mapping->file_column_name);
    }
    DataTypePtr projected_type;
    const auto primitive_type = remove_nullable(mapping->file_type)->get_primitive_type();
    switch (primitive_type) {
    case TYPE_STRUCT:
        projected_type = std::make_shared<DataTypeStruct>(child_types, child_names);
        break;
    case TYPE_ARRAY:
        DORIS_CHECK(child_types.size() == 1);
        projected_type = std::make_shared<DataTypeArray>(child_types[0]);
        break;
    case TYPE_MAP:
        DORIS_CHECK(child_types.size() == 1);
        DORIS_CHECK(remove_nullable(child_types[0])->get_primitive_type() == TYPE_STRUCT);
        {
            const auto* entry_type =
                    assert_cast<const DataTypeStruct*>(remove_nullable(child_types[0]).get());
            DORIS_CHECK(entry_type->get_elements().size() == 2);
            projected_type = std::make_shared<DataTypeMap>(entry_type->get_element(0),
                                                           entry_type->get_element(1));
        }
        break;
    default:
        return Status::InvalidArgument("Cannot project children from non-complex column {}",
                                       mapping->file_column_name);
    }
    mapping->file_type =
            mapping->file_type->is_nullable() ? make_nullable(projected_type) : projected_type;
    mapping->is_trivial =
            mapping->table_type != nullptr && mapping->table_type->equals(*mapping->file_type);
    mapping->has_complex_projection = true;
    return Status::OK();
}

static std::vector<int32_t> filter_slot_ids(const TableFilter& table_filter) {
    if (!table_filter.slot_ids.empty()) {
        return table_filter.slot_ids;
    }
    return {};
}

Status TableColumnMapper::create_mapping(const std::vector<TableColumn>& projected_columns,
                                         const std::map<std::string, Field>& partition_values,
                                         const std::vector<SchemaField>& file_schema) {
    _mappings.clear();
    for (const auto& table_column : projected_columns) {
        ColumnMapping mapping;
        mapping.table_column_id = table_column.id;
        mapping.table_type = table_column.type;
        if (table_column.is_partition_key && partition_values.count(table_column.name) > 0) {
            // 1. Partition column, use partition value as a constant mapping. Note that partition column may also have default expression, but partition value should take precedence if it exists.
            mapping.is_constant = true;
            mapping.default_expr = VExprContext::create_shared(TableLiteral::create_shared(
                    mapping.table_type, partition_values.at(table_column.name)));
        } else if (const auto* file_field = _find_file_field(table_column, file_schema)) {
            // 2. Table column has a matching file column, use it as a direct mapping.
            RETURN_IF_ERROR(_create_direct_mapping(table_column, *file_field, &mapping));
        } else if (table_column.default_expr != nullptr) {
            // 3. Table column does not exist in file (column adding by schema evolution), which has a default expression, use it as a constant mapping.
            mapping.is_constant = true;
            mapping.default_expr = table_column.default_expr;
        } else if (table_column.name == ROW_LINEAGE_ROW_ID) {
            // 4. Virtual column, use special mapping to indicate it should be materialized by table reader instead of read from file or evaluated from expression.
            mapping.virtual_column_type = TableVirtualColumnType::ROW_ID;
        } else if (table_column.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
            mapping.virtual_column_type = TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER;
        } else {
            if (table_column.is_partition_key) {
                return Status::InvalidArgument(
                        "Table column '{}' (id={}) does not have a matching partition value",
                        table_column.name, table_column.id);
            }
            if (!_options.allow_missing_columns) {
                return Status::InvalidArgument(
                        "Table column '{}' (id={}) does not have a matching file column",
                        table_column.name, table_column.id);
            }
        }
        _mappings.push_back(std::move(mapping));
    }
    return Status::OK();
}

Status TableColumnMapper::create_scan_request(const std::vector<TableFilter>& table_filters,
                                              const TableColumnPredicates& table_column_predicates,
                                              const std::vector<TableColumn>& projected_columns,
                                              FileScanRequest* file_request) {
    // FileReader evaluates expressions against a file-local block. This mapper owns the
    // table-column to file-column conversion, so it also owns the file-local block positions.
    file_request->predicate_columns.clear();
    file_request->non_predicate_columns.clear();
    file_request->column_positions.clear();
    file_request->complex_projections.clear();
    file_request->expression_filters.clear();
    file_request->column_predicate_filters.clear();
    file_request->reader_expression_map.clear();
    // 1. Build referenced non-predicate columns
    for (const auto& table_column : projected_columns) {
        auto* mapping = _find_mapping(table_column.id);
        if (mapping != nullptr && mapping->file_column_id.has_value()) {
            // A file column can be read lazily as a non-predicate column only when it is not used
            // by either expression filters or single-column predicate pruning.
            bool used_by_filter = table_column_predicates.count(table_column.id) > 0;
            if (!used_by_filter) {
                for (const auto& table_filter : table_filters) {
                    const auto slot_ids = filter_slot_ids(table_filter);
                    if (std::find(slot_ids.begin(), slot_ids.end(), table_column.id) !=
                        slot_ids.end()) {
                        used_by_filter = true;
                        break;
                    }
                }
            }
            if (!used_by_filter) {
                add_scan_column(file_request, *mapping->file_column_id,
                                &file_request->non_predicate_columns);
            }
            if (mapping->has_complex_projection ||
                complex_projection_has_pruned_children(*mapping)) {
                if (!mapping->has_complex_projection) {
                    RETURN_IF_ERROR(rebuild_projected_file_type(mapping));
                }
                FieldProjection projection;
                RETURN_IF_ERROR(_build_complex_projection(*mapping, &projection));
                file_request->complex_projections.emplace(*mapping->file_column_id,
                                                          std::move(projection));
            }
        }
    }
    // 2. Build referenced predicate columns
    RETURN_IF_ERROR(localize_filters(table_filters, table_column_predicates, file_request));
    // 3. Re-build projections for all referenced file columns to point to the correct file-local block positions.
    for (auto& mapping : _mappings) {
        if (!mapping.file_column_id.has_value()) {
            continue;
        }
        auto position_it = file_request->column_positions.find(*mapping.file_column_id);
        DORIS_CHECK(position_it != file_request->column_positions.end());
        rebuild_projection(&mapping, position_it->second);
    }
    return Status::OK();
}

Status TableColumnMapper::localize_filters(const std::vector<TableFilter>& table_filters,
                                           const TableColumnPredicates& table_column_predicates,
                                           FileScanRequest* file_request) const {
    // 真实实现会处理 trivial mapping、safe cast、reader expression fallback 和
    // finalize-only filter。stub 只复制能够直接定位到 file column 的谓词。
    for (const auto& table_filter : table_filters) {
        if (!table_filter.can_be_localized()) {
            // TODO: Rewrite table filter to reader_expression_map
            // file_request->reader_expression_map.emplace_back(..., table_filter.conjunct);
            continue;
        }
        for (const auto table_column_id : filter_slot_ids(table_filter)) {
            const auto* mapping = _find_mapping(table_column_id);
            if (mapping == nullptr || !mapping->file_column_id.has_value()) {
                continue;
            }
            add_scan_column(file_request, *mapping->file_column_id,
                            &file_request->predicate_columns);
        }
    }
    for (const auto& [table_column_id, _] : table_column_predicates) {
        const auto* mapping = _find_mapping(table_column_id);
        if (mapping == nullptr || !mapping->file_column_id.has_value()) {
            continue;
        }
        add_scan_column(file_request, *mapping->file_column_id, &file_request->predicate_columns);
    }

    // Build the complete table-slot rewrite map after all predicate columns have been assigned.
    // This keeps expression localization independent from filter iteration order.
    const auto table_column_to_file_slot = build_file_slot_rewrite_map(_mappings, *file_request);
    for (const auto& table_filter : table_filters) {
        if (!table_filter.can_be_localized()) {
            continue;
        }
        if (table_filter.conjunct != nullptr) {
            FileExpressionFilter expression_filter;
            expression_filter.conjunct =
                    VExprContext::create_shared(rewrite_table_expr_to_file_expr(
                            table_filter.conjunct->root(), table_column_to_file_slot));
            expression_filter.file_column_ids.reserve(table_filter.slot_ids.size());
            for (const auto table_column_id : table_filter.slot_ids) {
                const auto* mapping = _find_mapping(table_column_id);
                if (mapping == nullptr || !mapping->file_column_id.has_value()) {
                    continue;
                }
                expression_filter.file_column_ids.push_back(*mapping->file_column_id);
            }
            file_request->expression_filters.push_back(std::move(expression_filter));
        }
    }
    for (const auto& [table_column_id, predicates] : table_column_predicates) {
        const auto* mapping = _find_mapping(table_column_id);
        if (mapping == nullptr || !mapping->file_column_id.has_value() || predicates.empty()) {
            continue;
        }
        FileColumnPredicateFilter column_predicate_filter;
        column_predicate_filter.file_column_id = *mapping->file_column_id;
        column_predicate_filter.predicates = predicates;
        file_request->column_predicate_filters.push_back(std::move(column_predicate_filter));
    }
    return Status::OK();
}

const SchemaField* TableColumnMapper::_find_file_field(
        const TableColumn& table_column, const std::vector<SchemaField>& file_schema) const {
    for (const auto& field : file_schema) {
        if (_options.mode == TableColumnMappingMode::BY_FIELD_ID) {
            if (!field.field_id_path.empty() && field.field_id_path.back() != -1 &&
                field.field_id_path.back() == table_column.id) {
                return &field;
            }
            if ((field.field_id_path.empty() || field.field_id_path.back() == -1) &&
                field.id == table_column.id) {
                return &field;
            }
        }
        if (field.name == table_column.name) {
            return &field;
        }
    }
    return nullptr;
}

Status TableColumnMapper::_create_direct_mapping(const TableColumn& table_column,
                                                 const SchemaField& file_field,
                                                 ColumnMapping* mapping) const {
    if (mapping == nullptr) {
        return Status::InvalidArgument("mapping is null");
    }
    mapping->file_column_id = file_field.id;
    mapping->file_column_name = file_field.name;
    mapping->file_path = file_field.file_path;
    mapping->file_type = file_field.type;
    mapping->is_trivial = _is_same_type(mapping->table_type, mapping->file_type);
    mapping->child_mappings.clear();

    if (!table_column.children.empty() && is_complex_type(file_field.type)) {
        for (const auto& table_child : table_column.children) {
            const auto* file_child = find_file_child_by_table_column(
                    table_child, file_field.children, _options.mode);
            if (file_child == nullptr) {
                return Status::NotSupported(
                        "Complex schema change is not implemented: table child column '{}' "
                        "(id={}) does not have a matching file child under column '{}'",
                        table_child.name, table_child.id, table_column.name);
            }
            ColumnMapping child_mapping;
            child_mapping.table_column_id = table_child.id;
            child_mapping.table_type = table_child.type;
            RETURN_IF_ERROR(_create_direct_mapping(table_child, *file_child, &child_mapping));
            mapping->child_mappings.push_back(std::move(child_mapping));
        }
    }
    return Status::OK();
}

Status TableColumnMapper::_build_complex_projection(const ColumnMapping& mapping,
                                                    FieldProjection* projection) const {
    if (projection == nullptr) {
        return Status::InvalidArgument("projection is null");
    }
    DORIS_CHECK(mapping.file_column_id.has_value());
    projection->file_column_id = *mapping.file_column_id;
    projection->file_path = mapping.file_path;
    projection->project_all_children = mapping.child_mappings.empty();
    projection->children.clear();
    for (const auto& child_mapping : mapping.child_mappings) {
        if (!child_mapping.file_column_id.has_value()) {
            continue;
        }
        FieldProjection child_projection;
        RETURN_IF_ERROR(_build_complex_projection(child_mapping, &child_projection));
        projection->children.push_back(std::move(child_projection));
    }
    if (!projection->project_all_children && projection->children.empty()) {
        return Status::NotSupported("Projection for complex column {} contains no file children",
                                    mapping.file_column_name);
    }
    return Status::OK();
}

} // namespace doris::reader
