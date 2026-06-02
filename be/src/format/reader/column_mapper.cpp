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

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/data_type/convert_field_to_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format/reader/expr/cast.h"
#include "format/reader/expr/literal.h"
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

// A split-local literal produced by slot-literal predicate localization.
//
// TableColumnMapper currently rewrites VExpr trees in-place because VExpr has no generic deep
// clone API. The same table-level conjunct can therefore be localized repeatedly for different
// splits. This wrapper keeps the original table literal so the next split can restore table
// semantics before trying its own file-type literal rewrite.
class SplitLocalFileLiteral final : public TableLiteral {
public:
    SplitLocalFileLiteral(const DataTypePtr& file_type, const Field& file_field,
                          DataTypePtr original_type, Field original_field)
            : TableLiteral(file_type, file_field),
              _original_type(std::move(original_type)),
              _original_field(std::move(original_field)) {}

    const DataTypePtr& original_type() const { return _original_type; }
    const Field& original_field() const { return _original_field; }

private:
    DataTypePtr _original_type;
    Field _original_field;
};

static VExprSPtr create_file_slot_ref(const VSlotRef& slot_ref,
                                      const FileSlotRewriteInfo& rewrite_info) {
    return TableSlotRef::create_shared(slot_ref.slot_id(),
                                       cast_set<int>(rewrite_info.block_position), -1,
                                       rewrite_info.file_type, rewrite_info.file_column_name);
}

static bool is_cast_expr(const VExprSPtr& expr) {
    return dynamic_cast<const Cast*>(expr.get()) != nullptr;
}

static bool is_binary_comparison_predicate(const VExprSPtr& expr) {
    if (expr == nullptr || expr->get_num_children() != 2 ||
        (expr->node_type() != TExprNodeType::BINARY_PRED &&
         expr->node_type() != TExprNodeType::NULL_AWARE_BINARY_PRED)) {
        return false;
    }
    switch (expr->op()) {
    case TExprOpcode::EQ:
    case TExprOpcode::EQ_FOR_NULL:
    case TExprOpcode::NE:
    case TExprOpcode::GE:
    case TExprOpcode::GT:
    case TExprOpcode::LE:
    case TExprOpcode::LT:
        return true;
    default:
        return false;
    }
}

static const FileSlotRewriteInfo* find_slot_rewrite_info(
        const VExprSPtr& expr,
        const std::map<int32_t, FileSlotRewriteInfo>& table_column_to_file_slot,
        const VSlotRef** slot_ref) {
    if (expr == nullptr) {
        return nullptr;
    }
    VExprSPtr slot_expr = expr;
    const bool input_is_cast = is_cast_expr(expr) && expr->get_num_children() == 1;
    if (is_cast_expr(expr) && expr->get_num_children() == 1) {
        slot_expr = expr->children()[0];
    }
    if (!slot_expr->is_slot_ref()) {
        return nullptr;
    }
    const auto* candidate_slot_ref = assert_cast<const VSlotRef*>(slot_expr.get());
    const auto rewrite_it = table_column_to_file_slot.find(candidate_slot_ref->slot_id());
    if (rewrite_it == table_column_to_file_slot.end()) {
        return nullptr;
    }
    if (input_is_cast && !expr->data_type()->equals(*rewrite_it->second.table_type)) {
        return nullptr;
    }
    if (slot_ref != nullptr) {
        *slot_ref = candidate_slot_ref;
    }
    return &rewrite_it->second;
}

static VExprSPtr unwrap_literal_for_file_cast(const VExprSPtr& expr,
                                              const DataTypePtr& table_type) {
    if (expr == nullptr) {
        return nullptr;
    }
    if (expr->is_literal()) {
        return expr;
    }
    if (is_cast_expr(expr) && expr->get_num_children() == 1 && expr->children()[0]->is_literal() &&
        expr->children()[0]->data_type()->equals(*table_type)) {
        return expr->children()[0];
    }
    return nullptr;
}

static Field literal_field(const VExprSPtr& literal_expr) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto* literal = dynamic_cast<const VLiteral*>(literal_expr.get());
    DORIS_CHECK(literal != nullptr);
    Field field;
    literal->get_column_ptr()->get(0, field);
    return field;
}

static VExprSPtr original_table_literal(const VExprSPtr& literal_expr) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto* rewritten_literal = dynamic_cast<const SplitLocalFileLiteral*>(literal_expr.get());
    if (rewritten_literal == nullptr) {
        return literal_expr;
    }
    return TableLiteral::create_shared(rewritten_literal->original_type(),
                                       rewritten_literal->original_field());
}

static VExprSPtr rewrite_literal_to_file_type(const VExprSPtr& literal_expr,
                                              const FileSlotRewriteInfo& rewrite_info) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto original_literal = original_table_literal(literal_expr);
    const Field original_field = literal_field(original_literal);
    if (rewrite_info.file_type->equals(*original_literal->data_type())) {
        return original_literal;
    }
    Field file_field;
    try {
        convert_field_to_type(original_field, *rewrite_info.file_type, &file_field,
                              rewrite_info.table_type.get());
    } catch (const Exception&) {
        return nullptr;
    }
    if (file_field.is_null()) {
        return nullptr;
    }
    return std::make_shared<SplitLocalFileLiteral>(rewrite_info.file_type, file_field,
                                                   original_literal->data_type(), original_field);
}

// TODO: rewrite InPredicate
static bool rewrite_binary_slot_literal_predicate(
        const VExprSPtr& expr,
        const std::map<int32_t, FileSlotRewriteInfo>& table_column_to_file_slot) {
    if (!is_binary_comparison_predicate(expr)) {
        return false;
    }
    auto children = expr->children();
    const VSlotRef* slot_ref = nullptr;
    const FileSlotRewriteInfo* rewrite_info =
            find_slot_rewrite_info(children[0], table_column_to_file_slot, &slot_ref);
    int slot_child_idx = 0;
    int literal_child_idx = 1;
    if (rewrite_info == nullptr) {
        rewrite_info = find_slot_rewrite_info(children[1], table_column_to_file_slot, &slot_ref);
        slot_child_idx = 1;
        literal_child_idx = 0;
    }
    if (rewrite_info == nullptr || slot_ref == nullptr) {
        return false;
    }
    auto literal_expr =
            unwrap_literal_for_file_cast(children[literal_child_idx], rewrite_info->table_type);
    if (literal_expr == nullptr) {
        return false;
    }

    auto rewritten_literal = rewrite_literal_to_file_type(literal_expr, *rewrite_info);
    if (rewritten_literal == nullptr) {
        children[literal_child_idx] = original_table_literal(literal_expr);
        expr->set_children(std::move(children));
        return false;
    }

    children[slot_child_idx] = create_file_slot_ref(*slot_ref, *rewrite_info);
    children[literal_child_idx] = std::move(rewritten_literal);
    expr->set_children(std::move(children));
    return true;
}

static VExprSPtr rewrite_table_expr_to_file_expr(
        const VExprSPtr& expr,
        const std::map<int32_t, FileSlotRewriteInfo>& table_column_to_file_slot) {
    if (expr == nullptr) {
        return nullptr;
    }
    if (rewrite_binary_slot_literal_predicate(expr, table_column_to_file_slot)) {
        return expr;
    }
    if (expr->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
        const auto rewrite_it = table_column_to_file_slot.find(slot_ref->slot_id());
        if (rewrite_it != table_column_to_file_slot.end()) {
            const auto& rewrite_info = rewrite_it->second;
            auto file_slot = create_file_slot_ref(*slot_ref, rewrite_info);
            if (rewrite_info.file_type->equals(*rewrite_info.table_type)) {
                return file_slot;
            }
            auto cast_expr = Cast::create_shared(rewrite_info.table_type);
            cast_expr->add_child(std::move(file_slot));
            return cast_expr;
        }
        return expr;
    }
    // rewrite_table_expr_to_file_expr localizes the expression tree in-place because VExpr does
    // not provide a generic deep-clone API. A previous split may already have inserted Cast(slot)
    // for the same table-level conjunct. Keep that rewrite idempotent: rewrite the cast child
    // from table slot to the current split's file slot, and drop the cast when the current split
    // no longer needs it.
    if (is_cast_expr(expr) && expr->get_num_children() == 1) {
        const auto& child = expr->children()[0];
        if (child->is_slot_ref()) {
            const auto* slot_ref = assert_cast<const VSlotRef*>(child.get());
            const auto rewrite_it = table_column_to_file_slot.find(slot_ref->slot_id());
            if (rewrite_it != table_column_to_file_slot.end() &&
                expr->data_type()->equals(*rewrite_it->second.table_type)) {
                auto rewritten_child = create_file_slot_ref(*slot_ref, rewrite_it->second);
                if (rewrite_it->second.file_type->equals(*rewrite_it->second.table_type)) {
                    return rewritten_child;
                }
                expr->set_children({std::move(rewritten_child)});
                return expr;
            }
        }
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

static bool complex_projection_has_pruned_children(const ColumnMapping& mapping) {
    if (!is_complex_type(mapping.file_type->get_primitive_type())) {
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
        if (!child_mapping.field_id.has_value() ||
            complex_projection_has_pruned_children(child_mapping)) {
            return true;
        }
    }
    return false;
}

static DataTypePtr build_projected_child_type(const DataTypePtr& file_type,
                                              const std::vector<ColumnMapping>& child_mappings) {
    DORIS_CHECK(file_type != nullptr);
    DataTypes child_types;
    Strings child_names;
    child_types.reserve(child_mappings.size());
    child_names.reserve(child_mappings.size());
    for (const auto& child_mapping : child_mappings) {
        if (!child_mapping.field_id.has_value()) {
            continue;
        }
        child_types.push_back(child_mapping.file_type);
        child_names.push_back(child_mapping.file_column_name);
    }
    const auto primitive_type = remove_nullable(file_type)->get_primitive_type();
    DataTypePtr projected_type;
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
            DORIS_CHECK(entry_type->get_elements().size() == 1 ||
                        entry_type->get_elements().size() == 2);
            const auto value_idx = entry_type->get_elements().size() == 1 ? 0 : 1;
            projected_type = std::make_shared<DataTypeMap>(
                    assert_cast<const DataTypeMap*>(remove_nullable(file_type).get())
                            ->get_key_type(),
                    entry_type->get_element(value_idx));
        }
        break;
    default:
        DORIS_CHECK(false);
    }
    return file_type->is_nullable() ? make_nullable(projected_type) : projected_type;
}

static Status build_complex_projection(const ColumnMapping& mapping, FieldProjection* projection) {
    if (projection == nullptr) {
        return Status::InvalidArgument("projection is null");
    }
    DORIS_CHECK(mapping.field_id.has_value());
    projection->field_id = *mapping.field_id;
    projection->project_all_children = mapping.child_mappings.empty();
    projection->children.clear();
    for (const auto& child_mapping : mapping.child_mappings) {
        if (!child_mapping.field_id.has_value()) {
            continue;
        }
        FieldProjection child_projection;
        RETURN_IF_ERROR(build_complex_projection(child_mapping, &child_projection));
        projection->children.push_back(std::move(child_projection));
    }
    if (!projection->project_all_children && projection->children.empty()) {
        return Status::NotSupported("Projection for complex column {} contains no file children",
                                    mapping.file_column_name);
    }
    return Status::OK();
}

static Status rebuild_projected_file_type(ColumnMapping* mapping) {
    if (mapping == nullptr) {
        return Status::InvalidArgument("mapping is null");
    }
    DORIS_CHECK(is_complex_type(mapping->file_type->get_primitive_type()));
    DataTypes child_types;
    Strings child_names;
    child_types.reserve(mapping->child_mappings.size());
    child_names.reserve(mapping->child_mappings.size());
    for (auto& child_mapping : mapping->child_mappings) {
        if (!child_mapping.field_id.has_value()) {
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
    mapping->file_type = build_projected_child_type(mapping->file_type, mapping->child_mappings);
    mapping->is_trivial =
            mapping->table_type != nullptr && mapping->table_type->equals(*mapping->file_type);
    mapping->has_complex_projection = true;
    return Status::OK();
}

static Status add_scan_column(FileScanRequest* file_request, ColumnMapping* mapping,
                              std::vector<FieldProjection>* scan_columns) {
    auto file_column_id = mapping->field_id.value();
    if (scan_columns == &file_request->non_predicate_columns &&
        std::ranges::find_if(file_request->predicate_columns, [&](const FieldProjection& p) {
            return p.field_id == file_column_id;
        }) != file_request->predicate_columns.end()) {
        return Status::OK();
    }
    // column_positions is the global read-column index for this scan request, so it also
    // deduplicates predicate_columns and non_predicate_columns across all filter/projection paths.
    const bool newly_added = file_request->column_positions.count(file_column_id) == 0;
    if (newly_added) {
        file_request->column_positions.emplace(file_column_id,
                                               file_request->column_positions.size());
    }
    FieldProjection projection {.field_id = file_column_id};
    if (mapping->has_complex_projection || complex_projection_has_pruned_children(*mapping)) {
        if (!mapping->has_complex_projection) {
            RETURN_IF_ERROR(rebuild_projected_file_type(mapping));
        }
        RETURN_IF_ERROR(build_complex_projection(*mapping, &projection));
    }
    if (std::ranges::find_if(scan_columns->begin(), scan_columns->end(),
                             [&](const FieldProjection& p) {
                                 return p.field_id == file_column_id;
                             }) == scan_columns->end()) {
        scan_columns->push_back(std::move(projection));
    }
    if (scan_columns == &file_request->predicate_columns) {
        file_request->non_predicate_columns.erase(
                std::ranges::find_if(
                        file_request->non_predicate_columns,
                        [&](const FieldProjection& p) { return p.field_id == file_column_id; }),
                file_request->non_predicate_columns.end());
    }
    return Status::OK();
}

static void rebuild_projection(ColumnMapping* mapping, size_t block_position) {
    DORIS_CHECK(mapping->field_id.has_value());
    if (mapping->is_trivial || mapping->has_complex_projection) {
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
        if (!mapping.field_id.has_value()) {
            continue;
        }
        const auto position_it = file_request.column_positions.find(*mapping.field_id);
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

static const SchemaField* find_file_child_by_table_column(
        const TableColumn& table_column, const std::vector<SchemaField>& file_children,
        TableColumnMappingMode mode) {
    for (const auto& field : file_children) {
        if (mode == TableColumnMappingMode::BY_FIELD_ID && field.id == table_column.id) {
            return &field;
        }
        if (field.name == table_column.name) {
            return &field;
        }
    }
    return nullptr;
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
    file_request->conjuncts.clear();
    file_request->delete_conjuncts.clear();
    file_request->column_predicate_filters.clear();
    // 1. Build referenced non-predicate columns
    for (const auto& table_column : projected_columns) {
        auto* mapping = _find_mapping(table_column.id);
        if (mapping != nullptr && mapping->field_id.has_value()) {
            // A file column can be read lazily as a non-predicate column only when it is not used
            // by row-level expression filters. Single-column ColumnPredicate filters are pruning
            // hints only and must not force row-level predicate materialization.
            bool used_by_filter = false;
            for (const auto& table_filter : table_filters) {
                const auto slot_ids = filter_slot_ids(table_filter);
                if (std::find(slot_ids.begin(), slot_ids.end(), table_column.id) !=
                    slot_ids.end()) {
                    used_by_filter = true;
                    break;
                }
            }
            if (!used_by_filter) {
                RETURN_IF_ERROR(add_scan_column(file_request, mapping,
                                                &file_request->non_predicate_columns));
            }
        }
    }
    // 2. Build referenced predicate columns
    RETURN_IF_ERROR(localize_filters(table_filters, table_column_predicates, file_request));
    // 3. Re-build projections for all referenced file columns to point to the correct file-local block positions.
    for (auto& mapping : _mappings) {
        if (!mapping.field_id.has_value()) {
            continue;
        }
        auto position_it = file_request->column_positions.find(*mapping.field_id);
        DORIS_CHECK(position_it != file_request->column_positions.end());
        rebuild_projection(&mapping, position_it->second);
    }
    return Status::OK();
}

Status TableColumnMapper::localize_filters(const std::vector<TableFilter>& table_filters,
                                           const TableColumnPredicates& table_column_predicates,
                                           FileScanRequest* file_request) {
    // 真实实现会处理 trivial mapping、safe cast、reader expression fallback 和
    // finalize-only filter。stub 只复制能够直接定位到 file column 的谓词。
    for (const auto& table_filter : table_filters) {
        if (!table_filter.can_be_localized()) {
            // TODO: Rewrite table filter to reader_expression_map
            continue;
        }
        for (const auto table_column_id : filter_slot_ids(table_filter)) {
            auto* mapping = _find_mapping(table_column_id);
            if (mapping == nullptr || !mapping->field_id.has_value()) {
                continue;
            }
            RETURN_IF_ERROR(
                    add_scan_column(file_request, mapping, &file_request->predicate_columns));
        }
    }

    // Build the complete table-slot rewrite map after all predicate columns have been assigned.
    // This keeps expression localization independent from filter iteration order.
    const auto table_column_to_file_slot = build_file_slot_rewrite_map(_mappings, *file_request);
    for (const auto& table_filter : table_filters) {
        if (!table_filter.can_be_localized()) {
            continue;
        }
        if (table_filter.conjunct != nullptr) {
            file_request->conjuncts.push_back(
                    VExprContext::create_shared(rewrite_table_expr_to_file_expr(
                            table_filter.conjunct->root(), table_column_to_file_slot)));
        }
    }
    for (const auto& [table_column_id, predicates] : table_column_predicates) {
        const auto* mapping = _find_mapping(table_column_id);
        if (mapping == nullptr || !mapping->field_id.has_value() || predicates.empty()) {
            continue;
        }
        FileColumnPredicateFilter column_predicate_filter;
        column_predicate_filter.file_column_id = *mapping->field_id;
        column_predicate_filter.predicates = predicates;
        file_request->column_predicate_filters.push_back(std::move(column_predicate_filter));
    }
    return Status::OK();
}

const SchemaField* TableColumnMapper::_find_file_field(
        const TableColumn& table_column, const std::vector<SchemaField>& file_schema) const {
    for (const auto& field : file_schema) {
        if (_options.mode == TableColumnMappingMode::BY_FIELD_ID) {
            if (field.id == table_column.id) {
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
    mapping->field_id = file_field.id;
    mapping->file_column_name = file_field.name;
    mapping->file_type = file_field.type;
    mapping->is_trivial = _is_same_type(mapping->table_type, mapping->file_type);
    mapping->child_mappings.clear();

    if (!table_column.children.empty()) {
        for (const auto& table_child : table_column.children) {
            const auto* file_child = find_file_child_by_table_column(
                    table_child, file_field.children, _options.mode);
            if (file_child == nullptr) {
                if (!_options.allow_missing_columns) {
                    return Status::InvalidArgument(
                            "Table child column '{}' (id={}) does not have a matching file child "
                            "under column '{}'",
                            table_child.name, table_child.id, table_column.name);
                }
                ColumnMapping child_mapping;
                child_mapping.table_column_id = table_child.id;
                child_mapping.file_column_name = table_child.name;
                child_mapping.table_type = table_child.type;
                child_mapping.file_type = table_child.type;
                child_mapping.is_missing = true;
                child_mapping.has_complex_projection = !table_child.children.empty();
                mapping->child_mappings.push_back(std::move(child_mapping));
                continue;
            }
            ColumnMapping child_mapping;
            child_mapping.table_column_id = table_child.id;
            child_mapping.table_type = table_child.type;
            RETURN_IF_ERROR(_create_direct_mapping(table_child, *file_child, &child_mapping));
            mapping->child_mappings.push_back(std::move(child_mapping));
        }
        if (complex_projection_has_pruned_children(*mapping)) {
            mapping->has_complex_projection = true;
            mapping->file_type =
                    build_projected_child_type(mapping->file_type, mapping->child_mappings);
            mapping->is_trivial = mapping->table_type != nullptr &&
                                  mapping->table_type->equals(*mapping->file_type);
        }
    }
    return Status::OK();
}

} // namespace doris::reader
