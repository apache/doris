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

#include "format_v2/column_mapper_nested.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/data_type/convert_field_to_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "exprs/create_predicate_function.h"
#include "exprs/vexpr.h"
#include "exprs/vin_predicate.h"
#include "format_v2/expr/cast.h"
#include "gen_cpp/Exprs_types.h"
#include "storage/predicate/null_predicate.h"
#include "storage/predicate/predicate_creator.h"

namespace doris::format {

namespace {

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

static bool is_null_predicate_function(const VExprSPtr& expr, bool* is_null) {
    DORIS_CHECK(is_null != nullptr);
    if (expr == nullptr || expr->node_type() != TExprNodeType::FUNCTION_CALL ||
        expr->get_num_children() != 1) {
        return false;
    }
    if (expr->fn().name.function_name == "is_null_pred") {
        *is_null = true;
        return true;
    }
    if (expr->fn().name.function_name == "is_not_null_pred") {
        *is_null = false;
        return true;
    }
    return false;
}

static bool is_signed_integer_type(PrimitiveType type) {
    switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
        return true;
    default:
        return false;
    }
}

static int primitive_integer_width(PrimitiveType type) {
    switch (type) {
    case TYPE_TINYINT:
        return 1;
    case TYPE_SMALLINT:
        return 2;
    case TYPE_INT:
        return 4;
    case TYPE_BIGINT:
        return 8;
    case TYPE_LARGEINT:
        return 16;
    default:
        return 0;
    }
}

static bool is_decimal_type(PrimitiveType type) {
    switch (type) {
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return true;
    default:
        return false;
    }
}

static bool is_order_preserving_safe_cast(const DataTypePtr& from_type,
                                          const DataTypePtr& to_type) {
    if (from_type == nullptr || to_type == nullptr) {
        return false;
    }
    const auto from_nested_type = remove_nullable(from_type);
    const auto to_nested_type = remove_nullable(to_type);
    if (from_nested_type->equals(*to_nested_type)) {
        return true;
    }

    const auto from_primitive_type = from_nested_type->get_primitive_type();
    const auto to_primitive_type = to_nested_type->get_primitive_type();
    if (is_signed_integer_type(from_primitive_type) && is_signed_integer_type(to_primitive_type)) {
        return primitive_integer_width(to_primitive_type) >=
               primitive_integer_width(from_primitive_type);
    }
    if (from_primitive_type == TYPE_FLOAT && to_primitive_type == TYPE_DOUBLE) {
        return true;
    }
    if (is_decimal_type(from_primitive_type) && is_decimal_type(to_primitive_type)) {
        return from_nested_type->get_scale() == to_nested_type->get_scale() &&
               to_nested_type->get_precision() >= from_nested_type->get_precision();
    }
    return false;
}

static bool parse_struct_child_selector(const VExprSPtr& expr, StructChildSelector* selector) {
    DORIS_CHECK(selector != nullptr);
    if (expr == nullptr || !expr->is_literal()) {
        return false;
    }
    const Field field = literal_field(expr);
    switch (field.get_type()) {
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        selector->by_name = true;
        selector->name = std::string(field.as_string_view());
        return true;
    case TYPE_BOOLEAN:
        selector->by_name = false;
        selector->ordinal = field.get<TYPE_BOOLEAN>() ? 1 : 0;
        return selector->ordinal > 0;
    case TYPE_TINYINT:
        selector->by_name = false;
        if (field.get<TYPE_TINYINT>() <= 0) {
            return false;
        }
        selector->ordinal = cast_set<size_t>(field.get<TYPE_TINYINT>());
        return true;
    case TYPE_SMALLINT:
        selector->by_name = false;
        if (field.get<TYPE_SMALLINT>() <= 0) {
            return false;
        }
        selector->ordinal = cast_set<size_t>(field.get<TYPE_SMALLINT>());
        return true;
    case TYPE_INT:
        selector->by_name = false;
        if (field.get<TYPE_INT>() <= 0) {
            return false;
        }
        selector->ordinal = cast_set<size_t>(field.get<TYPE_INT>());
        return true;
    case TYPE_BIGINT:
        selector->by_name = false;
        if (field.get<TYPE_BIGINT>() <= 0) {
            return false;
        }
        selector->ordinal = cast_set<size_t>(field.get<TYPE_BIGINT>());
        return true;
    default:
        return false;
    }
}

static bool extract_nested_struct_path(const VExprSPtr& expr, NestedStructPath* path) {
    DORIS_CHECK(path != nullptr);
    if (!is_struct_element_expr(expr)) {
        return false;
    }

    // Process for element_at(struct, 'field') or element_at(struct, 1) expression.
    StructChildSelector selector;
    if (!parse_struct_child_selector(expr->children()[1], &selector)) {
        return false;
    }

    const auto& parent = expr->children()[0];
    if (parent->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(parent.get());
        path->root_global_index = slot_ref_global_index(*slot_ref);
        path->selectors.clear();
        path->selectors.push_back(std::move(selector));
        return true;
    }

    // Process for element_at(element_at(struct<struct>, 'field'), 'field') or
    // element_at(element_at(struct<struct>, 1), 1) expression.
    if (!extract_nested_struct_path(parent, path)) {
        return false;
    }
    path->selectors.push_back(std::move(selector));
    return true;
}

static bool extract_nested_struct_path_for_pruning(const VExprSPtr& expr, NestedStructPath* path) {
    DORIS_CHECK(path != nullptr);
    // Simple `ELEMENT_AT`
    if (extract_nested_struct_path(expr, path)) {
        return true;
    }

    // `ELEMENT_AT` with `CAST`
    if (!is_cast_expr(expr) || expr->get_num_children() != 1) {
        return false;
    }
    const auto& child = expr->children()[0];
    if (!is_order_preserving_safe_cast(child->data_type(), expr->data_type())) {
        return false;
    }
    // A safe widening cast is null-preserving and keeps the comparison ordering of the nested
    // primitive leaf, so file-layer pruning can target the original leaf statistics. The row-level
    // filter still evaluates the original cast expression after read.
    return extract_nested_struct_path_for_pruning(child, path);
}

static const ColumnDefinition* resolve_file_child(const std::vector<ColumnDefinition>& children,
                                                  const StructChildSelector& selector) {
    if (selector.by_name) {
        const auto child_it = std::ranges::find_if(children, [&](const ColumnDefinition& child) {
            return child.name == selector.name;
        });
        return child_it == children.end() ? nullptr : &*child_it;
    }
    if (selector.ordinal == 0 || selector.ordinal > children.size()) {
        return nullptr;
    }
    return &children[selector.ordinal - 1];
}

static const DataTypeStruct* struct_type_or_null(const DataTypePtr& type) {
    if (type == nullptr) {
        return nullptr;
    }
    const auto nested_type = remove_nullable(type);
    if (nested_type->get_primitive_type() != TYPE_STRUCT) {
        return nullptr;
    }
    return assert_cast<const DataTypeStruct*>(nested_type.get());
}

static std::optional<int32_t> struct_child_index(const ColumnMapping& mapping,
                                                 const StructChildSelector& selector) {
    const auto* struct_type = struct_type_or_null(mapping.table_type);
    if (struct_type == nullptr) {
        return std::nullopt;
    }
    if (selector.by_name) {
        const auto position = struct_type->try_get_position_by_name(selector.name);
        if (!position.has_value()) {
            return std::nullopt;
        }
        return cast_set<int32_t>(*position);
    }
    if (selector.ordinal == 0 || selector.ordinal > struct_type->get_elements().size()) {
        return std::nullopt;
    }
    return cast_set<int32_t>(selector.ordinal - 1);
}

// Get the global child index for a child mapping. If the mapping's table type is struct, resolve
// the child index by the child mapping's table column name; otherwise, use the fallback child index.
static int32_t child_mapping_global_index(const ColumnMapping& mapping,
                                          const ColumnMapping& child_mapping,
                                          size_t fallback_child_idx) {
    const auto* struct_type = struct_type_or_null(mapping.table_type);
    if (struct_type == nullptr) {
        return cast_set<int32_t>(fallback_child_idx);
    }
    const auto position = struct_type->try_get_position_by_name(child_mapping.table_column_name);
    DORIS_CHECK(position.has_value()) << "Cannot find child '" << child_mapping.table_column_name
                                      << "' in table type " << mapping.table_type->get_name();
    return cast_set<int32_t>(*position);
}

static const ColumnMapping* resolve_mapped_child(const ColumnMapping& mapping,
                                                 int32_t global_child_index) {
    for (size_t child_idx = 0; child_idx < mapping.child_mappings.size(); ++child_idx) {
        const auto& child_mapping = mapping.child_mappings[child_idx];
        if (child_mapping_global_index(mapping, child_mapping, child_idx) == global_child_index) {
            return &child_mapping;
        }
    }
    return nullptr;
}

enum class NestedProjectionResolveResult {
    RESOLVED,
    NOT_REPRESENTED,
    MISSING_FILE_CHILD,
};

// Resolve a table-side nested struct path through the existing ColumnMapping tree and build the
// corresponding file-local projection. For example, if table column `s` has children
// `{a, renamed_b}` and file column `s` has children `{a, b}`, the filter path
// `struct_element(s, 'renamed_b')` is resolved to the file projection `s -> b` by following the
// child mapping instead of matching the table child name against the file schema. Return
// MISSING_FILE_CHILD when ColumnMapping explicitly says a table child is absent from this file; in
// that case callers must not fall back to schema-name lookup, because Iceberg can drop a field and
// later add a different field with the same name.
static NestedProjectionResolveResult resolve_nested_projection_with_mapping(
        const NestedStructPath& path, const std::vector<ColumnMapping>& mappings,
        LocalColumnIndex* root_projection) {
    DORIS_CHECK(root_projection != nullptr);
    *root_projection = {};
    if (path.selectors.empty()) {
        return NestedProjectionResolveResult::NOT_REPRESENTED;
    }
    const auto mapping_it = std::ranges::find_if(mappings, [&](const ColumnMapping& mapping) {
        return mapping.global_index == path.root_global_index;
    });
    if (mapping_it == mappings.end() || !mapping_it->file_local_id.has_value()) {
        return NestedProjectionResolveResult::NOT_REPRESENTED;
    }

    *root_projection = LocalColumnIndex::partial_local(*mapping_it->file_local_id);
    auto* current_projection = root_projection;
    const auto* current_mapping = &*mapping_it;

    // Traverse the ColumnMapping tree according to the table-side struct selectors and emit the
    // corresponding file-local child ids. A missing child mapping means this predicate-only path
    // may need schema fallback; an existing child mapping without a file id means the table child
    // is genuinely absent from this file and must stay above the file reader.
    for (size_t selector_idx = 0; selector_idx < path.selectors.size(); ++selector_idx) {
        const auto global_child_index =
                struct_child_index(*current_mapping, path.selectors[selector_idx]);
        if (!global_child_index.has_value()) {
            *root_projection = {};
            return NestedProjectionResolveResult::NOT_REPRESENTED;
        }
        const auto* child_mapping = resolve_mapped_child(*current_mapping, *global_child_index);
        if (child_mapping == nullptr) {
            *root_projection = {};
            return NestedProjectionResolveResult::NOT_REPRESENTED;
        }
        if (!child_mapping->file_local_id.has_value()) {
            *root_projection = {};
            return NestedProjectionResolveResult::MISSING_FILE_CHILD;
        }

        auto child_projection = LocalColumnIndex::partial_local(*child_mapping->file_local_id);
        child_projection.project_all_children = selector_idx + 1 == path.selectors.size();
        current_projection->children.push_back(std::move(child_projection));
        current_projection = &current_projection->children.back();
        current_mapping = child_mapping;
    }
    return NestedProjectionResolveResult::RESOLVED;
}

static bool table_root_is_struct(const ColumnMapping& mapping) {
    return struct_type_or_null(mapping.table_type) != nullptr;
}

static const std::vector<ColumnDefinition>& scan_file_children(const ColumnMapping& mapping) {
    return !mapping.projected_file_children.empty() ? mapping.projected_file_children
                                                    : mapping.original_file_children;
}

static const ColumnDefinition* resolve_file_leaf_from_projection(
        const std::vector<ColumnDefinition>& children, const LocalColumnIndex& projection) {
    const auto child_it = std::ranges::find_if(children, [&](const ColumnDefinition& child) {
        return child.file_local_id() == projection.local_id();
    });
    if (child_it == children.end()) {
        return nullptr;
    }
    if (projection.children.empty()) {
        return &*child_it;
    }
    if (projection.children.size() != 1) {
        return nullptr;
    }
    return resolve_file_leaf_from_projection(child_it->children, projection.children[0]);
}

static bool collect_file_child_names_from_projection(const std::vector<ColumnDefinition>& children,
                                                     const LocalColumnIndex& projection,
                                                     std::vector<std::string>* file_child_names,
                                                     std::vector<DataTypePtr>* file_child_types) {
    DORIS_CHECK(file_child_names != nullptr);
    DORIS_CHECK(file_child_types != nullptr);
    const auto child_it = std::ranges::find_if(children, [&](const ColumnDefinition& child) {
        return child.file_local_id() == projection.local_id();
    });
    if (child_it == children.end()) {
        return false;
    }
    file_child_names->push_back(child_it->name);
    file_child_types->push_back(child_it->type);
    if (projection.children.empty()) {
        return true;
    }
    if (projection.children.size() != 1) {
        return false;
    }
    return collect_file_child_names_from_projection(child_it->children, projection.children[0],
                                                    file_child_names, file_child_types);
}

struct NestedPredicateTarget {
    LocalColumnIndex file_projection;
    FileNestedPredicateTarget file_target;
    std::string leaf_name;
    DataTypePtr leaf_type;
};

static std::unique_ptr<FileStructPredicateTarget> build_struct_predicate_target_from_projection(
        const std::vector<ColumnDefinition>& children, const LocalColumnIndex& projection) {
    const auto child_it = std::ranges::find_if(children, [&](const ColumnDefinition& child) {
        return child.file_local_id() == projection.local_id();
    });
    if (child_it == children.end()) {
        return nullptr;
    }
    std::unique_ptr<FileStructPredicateTarget> nested_child;
    if (!projection.children.empty()) {
        if (projection.children.size() != 1) {
            return nullptr;
        }
        nested_child = build_struct_predicate_target_from_projection(child_it->children,
                                                                     projection.children[0]);
        if (nested_child == nullptr) {
            return nullptr;
        }
    }
    return std::make_unique<FileStructPredicateTarget>(child_it->file_local_id(), child_it->name,
                                                       std::move(nested_child));
}

static bool build_struct_predicate_target(const ColumnMapping& root_mapping,
                                          const LocalColumnIndex& root_projection,
                                          FileNestedPredicateTarget* file_target) {
    DORIS_CHECK(file_target != nullptr);
    if (!root_projection.column_id().is_valid() || root_projection.children.size() != 1) {
        return false;
    }
    auto struct_target = build_struct_predicate_target_from_projection(
            root_mapping.original_file_children, root_projection.children[0]);
    if (struct_target == nullptr) {
        return false;
    }
    *file_target = FileNestedPredicateTarget(root_projection.column_id(), std::move(struct_target));
    return true;
}

static bool resolve_nested_predicate_target(const NestedStructPath& path,
                                            const std::vector<ColumnMapping>& mappings,
                                            NestedPredicateTarget* target) {
    DORIS_CHECK(target != nullptr);
    ResolvedNestedStructPath resolved;
    if (!resolve_nested_struct_path_for_file(path, mappings, &resolved)) {
        return false;
    }

    const auto mapping_it = std::ranges::find_if(mappings, [&](const ColumnMapping& mapping) {
        return mapping.global_index == path.root_global_index;
    });
    if (mapping_it == mappings.end() || resolved.file_projection.children.size() != 1) {
        return false;
    }
    const auto* file_leaf = resolve_file_leaf_from_projection(mapping_it->original_file_children,
                                                              resolved.file_projection.children[0]);
    if (file_leaf == nullptr || file_leaf->type == nullptr) {
        return false;
    }
    target->leaf_type = remove_nullable(file_leaf->type);
    if (is_complex_type(target->leaf_type->get_primitive_type())) {
        return false;
    }
    target->leaf_name = file_leaf->name;
    target->file_projection = std::move(resolved.file_projection);
    if (!build_struct_predicate_target(*mapping_it, target->file_projection,
                                       &target->file_target)) {
        return false;
    }
    return true;
}

static VExprSPtr original_table_literal_for_nested_predicate(const VExprSPtr& literal_expr) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto* rewritten_literal = dynamic_cast<const SplitLocalFileLiteral*>(literal_expr.get());
    if (rewritten_literal == nullptr) {
        return literal_expr;
    }
    return VLiteral::create_shared(rewritten_literal->original_type(),
                                   rewritten_literal->original_field());
}

static std::optional<PredicateType> to_column_predicate_type(TExprOpcode::type opcode) {
    switch (opcode) {
    case TExprOpcode::EQ:
        return PredicateType::EQ;
    case TExprOpcode::NE:
        return PredicateType::NE;
    case TExprOpcode::GT:
        return PredicateType::GT;
    case TExprOpcode::GE:
        return PredicateType::GE;
    case TExprOpcode::LT:
        return PredicateType::LT;
    case TExprOpcode::LE:
        return PredicateType::LE;
    default:
        return std::nullopt;
    }
}

static TExprOpcode::type reverse_comparison_opcode(TExprOpcode::type opcode) {
    switch (opcode) {
    case TExprOpcode::GT:
        return TExprOpcode::LT;
    case TExprOpcode::GE:
        return TExprOpcode::LE;
    case TExprOpcode::LT:
        return TExprOpcode::GT;
    case TExprOpcode::LE:
        return TExprOpcode::GE;
    default:
        return opcode;
    }
}

static std::shared_ptr<ColumnPredicate> create_comparison_column_predicate(
        PredicateType predicate_type, uint32_t column_id, const std::string& column_name,
        const DataTypePtr& data_type, const Field& value) {
    switch (predicate_type) {
    case PredicateType::EQ:
        return create_comparison_predicate<PredicateType::EQ>(column_id, column_name, data_type,
                                                              value, false);
    case PredicateType::NE:
        return create_comparison_predicate<PredicateType::NE>(column_id, column_name, data_type,
                                                              value, false);
    case PredicateType::GT:
        return create_comparison_predicate<PredicateType::GT>(column_id, column_name, data_type,
                                                              value, false);
    case PredicateType::GE:
        return create_comparison_predicate<PredicateType::GE>(column_id, column_name, data_type,
                                                              value, false);
    case PredicateType::LT:
        return create_comparison_predicate<PredicateType::LT>(column_id, column_name, data_type,
                                                              value, false);
    case PredicateType::LE:
        return create_comparison_predicate<PredicateType::LE>(column_id, column_name, data_type,
                                                              value, false);
    default:
        return nullptr;
    }
}

static bool extract_child_id_path_from_projection(const LocalColumnIndex& root_projection,
                                                  std::vector<int32_t>* file_child_id_path) {
    DORIS_CHECK(file_child_id_path != nullptr);
    file_child_id_path->clear();
    const auto* current_projection = &root_projection;
    while (!current_projection->children.empty()) {
        if (current_projection->children.size() != 1) {
            file_child_id_path->clear();
            return false;
        }
        current_projection = &current_projection->children[0];
        file_child_id_path->push_back(current_projection->local_id());
    }
    return !file_child_id_path->empty();
}

static std::shared_ptr<ColumnPredicate> build_nested_comparison_predicate(
        const VExprSPtr& literal_expr, TExprOpcode::type opcode, LocalColumnId root_file_column_id,
        const std::string& leaf_name, const DataTypePtr& file_leaf_type) {
    if (literal_expr == nullptr || !literal_expr->is_literal() || file_leaf_type == nullptr) {
        return nullptr;
    }
    const auto predicate_type = to_column_predicate_type(opcode);
    if (!predicate_type.has_value()) {
        return nullptr;
    }
    const auto original_literal = original_table_literal_for_nested_predicate(literal_expr);
    const Field original_field = literal_field(original_literal);
    Field file_field;
    try {
        convert_field_to_type(original_field, *file_leaf_type, &file_field,
                              original_literal->data_type().get());
    } catch (const Exception&) {
        return nullptr;
    }
    if (file_field.is_null()) {
        return nullptr;
    }
    try {
        return create_comparison_column_predicate(*predicate_type,
                                                  cast_set<uint32_t>(root_file_column_id.value()),
                                                  leaf_name, file_leaf_type, file_field);
    } catch (const Exception&) {
        return nullptr;
    }
}

static std::shared_ptr<ColumnPredicate> build_nested_in_list_predicate(
        const VExprSPtrs& literal_exprs, LocalColumnId root_file_column_id,
        const std::string& leaf_name, const DataTypePtr& file_leaf_type) {
    if (literal_exprs.empty() || file_leaf_type == nullptr) {
        return nullptr;
    }

    auto value_column = file_leaf_type->create_column();
    for (const auto& literal_expr : literal_exprs) {
        if (literal_expr == nullptr || !literal_expr->is_literal()) {
            return nullptr;
        }
        const auto original_literal = original_table_literal_for_nested_predicate(literal_expr);
        const Field original_field = literal_field(original_literal);
        Field file_field;
        try {
            convert_field_to_type(original_field, *file_leaf_type, &file_field,
                                  original_literal->data_type().get());
        } catch (const Exception&) {
            return nullptr;
        }
        if (file_field.is_null()) {
            return nullptr;
        }
        value_column->insert(file_field);
    }

    std::shared_ptr<HybridSetBase> values;
    try {
        values.reset(create_set(file_leaf_type->get_primitive_type(), literal_exprs.size(), false));
        ColumnPtr value_column_ptr = std::move(value_column);
        values->insert_range_from(value_column_ptr, 0, value_column_ptr->size());
        return create_in_list_predicate<PredicateType::IN_LIST>(
                cast_set<uint32_t>(root_file_column_id.value()), leaf_name, file_leaf_type, values,
                false);
    } catch (const Exception&) {
        return nullptr;
    }
}

static std::shared_ptr<ColumnPredicate> build_nested_null_predicate(
        bool is_null, LocalColumnId root_file_column_id, const std::string& leaf_name,
        const DataTypePtr& file_leaf_type) {
    if (file_leaf_type == nullptr) {
        return nullptr;
    }
    const auto leaf_primitive_type = remove_nullable(file_leaf_type)->get_primitive_type();
    return NullPredicate::create_shared(cast_set<uint32_t>(root_file_column_id.value()), leaf_name,
                                        is_null, leaf_primitive_type);
}

static bool set_nested_column_filter_target(const NestedPredicateTarget& target,
                                            FileColumnPredicateFilter* column_filter) {
    DORIS_CHECK(column_filter != nullptr);
    std::vector<int32_t> file_child_id_path;
    if (!extract_child_id_path_from_projection(target.file_projection, &file_child_id_path)) {
        return false;
    }
    column_filter->file_column_id = target.file_projection.column_id();
    column_filter->file_child_id_path = std::move(file_child_id_path);
    column_filter->target = target.file_target;
    return true;
}

static bool extract_nested_binary_comparison_filter(const VExprSPtr& expr,
                                                    const std::vector<ColumnMapping>& mappings,
                                                    FileColumnPredicateFilter* column_filter) {
    DORIS_CHECK(column_filter != nullptr);
    if (!is_binary_comparison_predicate(expr)) {
        return false;
    }
    NestedStructPath path;
    VExprSPtr literal_expr;
    TExprOpcode::type opcode = expr->op();
    if (extract_nested_struct_path_for_pruning(expr->children()[0], &path) &&
        expr->children()[1]->is_literal()) {
        literal_expr = expr->children()[1];
    } else if (extract_nested_struct_path_for_pruning(expr->children()[1], &path) &&
               expr->children()[0]->is_literal()) {
        literal_expr = expr->children()[0];
        opcode = reverse_comparison_opcode(opcode);
    } else {
        return false;
    }

    NestedPredicateTarget target;
    if (!resolve_nested_predicate_target(path, mappings, &target)) {
        return false;
    }
    auto predicate = build_nested_comparison_predicate(literal_expr, opcode,
                                                       target.file_projection.column_id(),
                                                       target.leaf_name, target.leaf_type);
    if (predicate == nullptr) {
        return false;
    }
    if (!set_nested_column_filter_target(target, column_filter)) {
        return false;
    }
    column_filter->predicates.push_back(std::move(predicate));
    return true;
}

static bool extract_nested_in_list_filter(const VExprSPtr& expr,
                                          const std::vector<ColumnMapping>& mappings,
                                          FileColumnPredicateFilter* column_filter) {
    DORIS_CHECK(column_filter != nullptr);
    if (expr == nullptr || expr->node_type() != TExprNodeType::IN_PRED ||
        expr->get_num_children() < 2) {
        return false;
    }
    if (const auto* in_predicate = dynamic_cast<const VInPredicate*>(expr.get());
        in_predicate != nullptr && in_predicate->is_not_in()) {
        return false;
    }

    NestedStructPath path;
    if (!extract_nested_struct_path_for_pruning(expr->children()[0], &path)) {
        return false;
    }

    VExprSPtrs literal_exprs;
    literal_exprs.reserve(expr->get_num_children() - 1);
    for (size_t child_idx = 1; child_idx < expr->children().size(); ++child_idx) {
        if (!expr->children()[child_idx]->is_literal()) {
            return false;
        }
        literal_exprs.push_back(expr->children()[child_idx]);
    }

    NestedPredicateTarget target;
    if (!resolve_nested_predicate_target(path, mappings, &target)) {
        return false;
    }
    auto predicate = build_nested_in_list_predicate(
            literal_exprs, target.file_projection.column_id(), target.leaf_name, target.leaf_type);
    if (predicate == nullptr) {
        return false;
    }
    if (!set_nested_column_filter_target(target, column_filter)) {
        return false;
    }
    column_filter->predicates.push_back(std::move(predicate));
    return true;
}

static bool extract_nested_null_filter(const VExprSPtr& expr,
                                       const std::vector<ColumnMapping>& mappings,
                                       FileColumnPredicateFilter* column_filter) {
    DORIS_CHECK(column_filter != nullptr);
    bool is_null = false;
    if (!is_null_predicate_function(expr, &is_null)) {
        return false;
    }

    NestedStructPath path;
    if (!extract_nested_struct_path_for_pruning(expr->children()[0], &path)) {
        return false;
    }

    NestedPredicateTarget target;
    if (!resolve_nested_predicate_target(path, mappings, &target)) {
        return false;
    }
    auto predicate = build_nested_null_predicate(is_null, target.file_projection.column_id(),
                                                 target.leaf_name, target.leaf_type);
    if (predicate == nullptr) {
        return false;
    }
    if (!set_nested_column_filter_target(target, column_filter)) {
        return false;
    }
    column_filter->predicates.push_back(std::move(predicate));
    return true;
}

} // namespace

SplitLocalFileLiteral::SplitLocalFileLiteral(const DataTypePtr& file_type, const Field& file_field,
                                             DataTypePtr original_type, Field original_field)
        : VLiteral(file_type, file_field),
          _original_type(std::move(original_type)),
          _original_field(std::move(original_field)) {}

GlobalIndex slot_ref_global_index(const VSlotRef& slot_ref) {
    DORIS_CHECK(slot_ref.column_id() >= 0);
    return GlobalIndex(cast_set<size_t>(slot_ref.column_id()));
}

bool is_struct_element_expr(const VExprSPtr& expr) {
    if (expr == nullptr || expr->get_num_children() != 2) {
        return false;
    }
    const auto& function_name = expr->fn().name.function_name;
    if (function_name == "struct_element") {
        return true;
    }
    if (function_name != "element_at") {
        return false;
    }
    const auto& parent_type = expr->children()[0]->data_type();
    return parent_type != nullptr &&
           remove_nullable(parent_type)->get_primitive_type() == TYPE_STRUCT;
}

Field literal_field(const VExprSPtr& literal_expr) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto* literal = dynamic_cast<const VLiteral*>(literal_expr.get());
    DORIS_CHECK(literal != nullptr);
    Field field;
    literal->get_column_ptr()->get(0, field);
    return field;
}

bool resolve_nested_struct_path_for_file(const NestedStructPath& path,
                                         const std::vector<ColumnMapping>& mappings,
                                         ResolvedNestedStructPath* resolved,
                                         bool require_scan_projection) {
    DORIS_CHECK(resolved != nullptr);
    *resolved = {};
    const auto mapping_it = std::ranges::find_if(mappings, [&](const ColumnMapping& mapping) {
        return mapping.global_index == path.root_global_index;
    });
    if (mapping_it == mappings.end() || !mapping_it->file_local_id.has_value() ||
        path.selectors.empty()) {
        return false;
    }

    // Prefer ColumnMapping over schema-name lookup. This is the only path that can correctly
    // localize renamed Iceberg fields: a table filter `element_at(s, 'renamed_b')` must become a
    // file filter on physical child `b`, even if the old file type is `STRUCT<b ...>`.
    const auto mapping_result =
            resolve_nested_projection_with_mapping(path, mappings, &resolved->file_projection);
    if (mapping_result == NestedProjectionResolveResult::MISSING_FILE_CHILD) {
        return false;
    }
    if (mapping_result == NestedProjectionResolveResult::NOT_REPRESENTED) {
        if (!table_root_is_struct(*mapping_it)) {
            return false;
        }
        LocalColumnIndex child_projection;
        if (!build_file_child_projection_from_schema(mapping_it->original_file_children,
                                                     path.selectors, &child_projection)
                     .ok() ||
            child_projection.local_id() < 0) {
            return false;
        }
        resolved->file_projection = LocalColumnIndex::partial_local(*mapping_it->file_local_id);
        resolved->file_projection.children.push_back(std::move(child_projection));
    }

    if (resolved->file_projection.children.size() != 1) {
        *resolved = {};
        return false;
    }
    // When rewriting the final localized element_at chain, it executes on the file column produced
    // by this scan, so the intermediate return types must match the projected file shape, not the
    // full historical file schema. Example:
    //   SELECT s.c WHERE element_at(element_at(s, 'b'), 'cc') LIKE 'NestedC%'
    // reads only b.cc and c; the inner element_at(s, 'b') returns Struct(cc), not
    // Struct(cc, new_dd).
    //
    // Earlier projection collection also calls this resolver before filter-only children have been
    // merged into the scan projection. That phase only needs the file path, so it still resolves
    // names/types from the original file schema.
    const auto& child_source = require_scan_projection ? scan_file_children(*mapping_it)
                                                       : mapping_it->original_file_children;
    if (!collect_file_child_names_from_projection(
                child_source, resolved->file_projection.children[0], &resolved->file_child_names,
                &resolved->file_child_types) ||
        resolved->file_child_names.size() != path.selectors.size() ||
        resolved->file_child_types.size() != path.selectors.size()) {
        *resolved = {};
        return false;
    }
    return true;
}

bool resolve_nested_struct_expr_for_file(const VExprSPtr& expr,
                                         const std::vector<ColumnMapping>& mappings,
                                         ResolvedNestedStructPath* resolved) {
    DORIS_CHECK(resolved != nullptr);
    NestedStructPath path;
    if (!extract_nested_struct_path(expr, &path)) {
        *resolved = {};
        return false;
    }
    return resolve_nested_struct_path_for_file(path, mappings, resolved, true);
}

// Collect nested struct leaf references that can be turned into file-reader projections and
// primitive pruning predicates. For example, from `s.a > 1 AND element_at(s, 'b') = 2`, this
// records two paths rooted at `s`: `s -> a` and `s -> b`. Non-struct expressions are traversed
// recursively, while a recognized struct path is emitted once so the caller can merge it into the
// scan projection for that top-level file column.
void collect_nested_struct_paths(const VExprSPtr& expr, std::vector<NestedStructPath>* paths) {
    DORIS_CHECK(paths != nullptr);
    if (expr == nullptr) {
        return;
    }
    NestedStructPath path;
    if (extract_nested_struct_path_for_pruning(expr, &path)) {
        paths->push_back(std::move(path));
        return;
    }
    for (const auto& child : expr->children()) {
        collect_nested_struct_paths(child, paths);
    }
}

std::vector<const ColumnMapping*> present_child_mappings_in_file_order(
        const std::vector<ColumnMapping>& child_mappings) {
    std::vector<const ColumnMapping*> result;
    result.reserve(child_mappings.size());
    for (const auto& child_mapping : child_mappings) {
        if (child_mapping.file_local_id.has_value()) {
            result.push_back(&child_mapping);
        }
    }
    std::ranges::sort(result, [](const ColumnMapping* lhs, const ColumnMapping* rhs) {
        DORIS_CHECK(lhs->file_local_id.has_value());
        DORIS_CHECK(rhs->file_local_id.has_value());
        return *lhs->file_local_id < *rhs->file_local_id;
    });
    return result;
}

// Build the nested child projection under a top-level file column by walking file schema children
// directly. The returned projection does not include the root column id; callers attach it under a
// `LocalColumnIndex::partial_local(root_id)` when merging into the scan request.
Status build_file_child_projection_from_schema(const std::vector<ColumnDefinition>& children,
                                               std::span<const StructChildSelector> selectors,
                                               LocalColumnIndex* projection) {
    DORIS_CHECK(projection != nullptr);
    if (selectors.empty()) {
        return Status::InvalidArgument("Nested struct selector path is empty");
    }
    const auto* child = resolve_file_child(children, selectors.front());
    if (child == nullptr) {
        return Status::OK();
    }
    *projection = LocalColumnIndex::local(child->file_local_id());
    projection->project_all_children = selectors.size() == 1;
    projection->children.clear();
    if (selectors.size() == 1) {
        return Status::OK();
    }
    if (child->children.empty() ||
        remove_nullable(child->type)->get_primitive_type() != TYPE_STRUCT) {
        *projection = LocalColumnIndex {};
        return Status::OK();
    }
    LocalColumnIndex child_projection;
    RETURN_IF_ERROR(build_file_child_projection_from_schema(child->children, selectors.subspan(1),
                                                            &child_projection));
    if (child_projection.local_id() < 0) {
        *projection = LocalColumnIndex {};
        return Status::OK();
    }
    projection->children.push_back(std::move(child_projection));
    return Status::OK();
}

// Merge predicates that target the same physical file column or nested leaf. For example,
// `WHERE s.b > 1 AND s.b < 10` produces two predicates for the same target `s -> b`; keeping them
// in one FileColumnPredicateFilter lets the file reader apply both pruning checks to the same leaf
// instead of carrying duplicate target entries.
void merge_column_predicate_filter(FileColumnPredicateFilter column_filter,
                                   std::vector<FileColumnPredicateFilter>* filters) {
    DORIS_CHECK(filters != nullptr);
    auto existing_filter_it = std::ranges::find_if(*filters, [&](const auto& existing_filter) {
        return existing_filter.same_target_as(column_filter);
    });
    if (existing_filter_it == filters->end()) {
        filters->push_back(std::move(column_filter));
        return;
    }
    existing_filter_it->predicates.insert(existing_filter_it->predicates.end(),
                                          column_filter.predicates.begin(),
                                          column_filter.predicates.end());
}

// Extract file-column pruning predicates from localized row-level conjuncts that reference nested
// struct leaves. This is separate from file_request->conjuncts: conjuncts do row filtering, while
// FileColumnPredicateFilter carries primitive leaf predicates for file/page/statistics pruning.
//
// Example: for `WHERE s.b.c > 10 AND element_at(s, 'd') IS NOT NULL`, this function emits pruning
// filters for the nested targets `s -> b -> c` and `s -> d`. The caller only invokes it after
// table_filter_has_only_local_entries() succeeds, so each root slot already has a file-local scan
// source in _filter_entries.
void collect_nested_column_predicate_filters(const VExprSPtr& expr,
                                             const std::vector<ColumnMapping>& mappings,
                                             std::vector<FileColumnPredicateFilter>* filters) {
    DORIS_CHECK(filters != nullptr);
    if (expr == nullptr) {
        return;
    }
    if (expr->node_type() == TExprNodeType::COMPOUND_PRED &&
        expr->op() == TExprOpcode::COMPOUND_AND) {
        for (const auto& child : expr->children()) {
            collect_nested_column_predicate_filters(child, mappings, filters);
        }
        return;
    }
    FileColumnPredicateFilter column_filter;
    if (extract_nested_binary_comparison_filter(expr, mappings, &column_filter) ||
        extract_nested_in_list_filter(expr, mappings, &column_filter) ||
        extract_nested_null_filter(expr, mappings, &column_filter)) {
        merge_column_predicate_filter(std::move(column_filter), filters);
    }
}

} // namespace doris::format
