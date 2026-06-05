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
#include <optional>
#include <span>
#include <sstream>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/data_type/convert_field_to_type.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/create_predicate_function.h"
#include "exprs/vin_predicate.h"
#include "format/reader/expr/cast.h"
#include "format/reader/expr/literal.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/file_reader.h"
#include "format/reader/schema_projection.h"
#include "format/reader/table_reader.h"
#include "storage/predicate/predicate_creator.h"

namespace doris::reader {

namespace {

std::string mapping_mode_to_string(TableColumnMappingMode mode) {
    switch (mode) {
    case TableColumnMappingMode::BY_FIELD_ID:
        return "BY_FIELD_ID";
    case TableColumnMappingMode::BY_NAME:
        return "BY_NAME";
    case TableColumnMappingMode::BY_INDEX:
        return "BY_INDEX";
    }
    return "UNKNOWN";
}

class ColumnMatcher {
public:
    virtual ~ColumnMatcher() = default;
    virtual const ColumnDefinition* find(
            const ColumnDefinition& table_column,
            const std::vector<ColumnDefinition>& file_schema) const = 0;
};

class FieldIdMatcher final : public ColumnMatcher {
public:
    const ColumnDefinition* find(const ColumnDefinition& table_column,
                                 const std::vector<ColumnDefinition>& file_schema) const override {
        if (!table_column.identifier.has_field_id()) {
            return nullptr;
        }
        const auto field_id = table_column.field_id();
        const auto field_it = std::ranges::find_if(file_schema, [&](const ColumnDefinition& field) {
            return field.identifier.has_field_id() && field.identifier.field_id == field_id;
        });
        return field_it == file_schema.end() ? nullptr : &*field_it;
    }
};

class NameMatcher final : public ColumnMatcher {
public:
    const ColumnDefinition* find(const ColumnDefinition& table_column,
                                 const std::vector<ColumnDefinition>& file_schema) const override {
        const auto match_name = to_lower(table_column.match_name());
        const auto field_it = std::ranges::find_if(file_schema, [&](const ColumnDefinition& field) {
            return to_lower(field.name) == match_name;
        });
        return field_it == file_schema.end() ? nullptr : &*field_it;
    }
};

class PositionMatcher final : public ColumnMatcher {
public:
    const ColumnDefinition* find(const ColumnDefinition& table_column,
                                 const std::vector<ColumnDefinition>& file_schema) const override {
        if (!table_column.identifier.has_position()) {
            return nullptr;
        }
        const auto position = table_column.file_position();
        if (position < 0 || static_cast<size_t>(position) >= file_schema.size()) {
            return nullptr;
        }
        return &file_schema[static_cast<size_t>(position)];
    }
};

const ColumnMatcher& matcher_for_mode(TableColumnMappingMode mode) {
    static const FieldIdMatcher field_id_matcher;
    static const NameMatcher name_matcher;
    static const PositionMatcher position_matcher;
    switch (mode) {
    case TableColumnMappingMode::BY_FIELD_ID:
        return field_id_matcher;
    case TableColumnMappingMode::BY_NAME:
        return name_matcher;
    case TableColumnMappingMode::BY_INDEX:
        return position_matcher;
    }
    return field_id_matcher;
}

std::string virtual_column_type_to_string(TableVirtualColumnType type) {
    switch (type) {
    case TableVirtualColumnType::INVALID:
        return "INVALID";
    case TableVirtualColumnType::ROW_ID:
        return "ROW_ID";
    case TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER:
        return "LAST_UPDATED_SEQUENCE_NUMBER";
    }
    return "UNKNOWN";
}

std::string filter_conversion_type_to_string(FilterConversionType type) {
    switch (type) {
    case FilterConversionType::COPY_DIRECTLY:
        return "COPY_DIRECTLY";
    case FilterConversionType::CAST_FILTER:
        return "CAST_FILTER";
    case FilterConversionType::READER_EXPRESSION:
        return "READER_EXPRESSION";
    case FilterConversionType::FINALIZE_ONLY:
        return "FINALIZE_ONLY";
    case FilterConversionType::CONSTANT:
        return "CONSTANT";
    }
    return "UNKNOWN";
}

std::string data_type_debug_string(const DataTypePtr& type) {
    return type == nullptr ? "null" : type->get_name();
}

template <typename T, typename Formatter>
std::string join_debug_strings(const std::vector<T>& values, Formatter formatter) {
    std::ostringstream out;
    out << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << formatter(values[i]);
    }
    out << "]";
    return out.str();
}

std::string int_vector_debug_string(const std::vector<int32_t>& values) {
    std::ostringstream out;
    out << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << values[i];
    }
    out << "]";
    return out.str();
}

} // namespace

struct FileSlotRewriteInfo {
    size_t block_position = 0;
    DataTypePtr file_type;
    DataTypePtr table_type;
    std::string file_column_name;
};

struct StructChildSelector {
    bool by_name = true;
    std::string name;
    size_t ordinal = 0;
};

struct NestedStructPath {
    GlobalIndex root_global_index;
    std::vector<StructChildSelector> selectors;
};

struct NestedPredicateTargetInfo {
    int32_t root_file_column_id = -1;
    std::vector<int32_t> file_child_id_path;
    std::string leaf_name;
    DataTypePtr file_leaf_type;
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

std::string TableColumnMapperOptions::debug_string() const {
    std::ostringstream out;
    out << "TableColumnMapperOptions{mode=" << mapping_mode_to_string(mode)
        << ", allow_missing_columns=" << allow_missing_columns
        << ", enable_reader_expression_fallback=" << enable_reader_expression_fallback << "}";
    return out.str();
}

std::string TableColumnMapper::debug_string(const ColumnDefinition& column) {
    std::ostringstream out;
    out << "ColumnDefinition{name=" << column.name
        << ", identifier_kind=" << static_cast<int>(column.identifier.kind)
        << ", type=" << data_type_debug_string(column.type) << ", children="
        << join_debug_strings(column.children,
                              [](const ColumnDefinition& child) {
                                  return TableColumnMapper::debug_string(child);
                              })
        << ", has_default_expr=" << (column.default_expr != nullptr)
        << ", is_partition_key=" << column.is_partition_key << "}";
    return out.str();
}

std::string TableColumnMapper::debug_string(const LocalColumnIndex& projection) {
    std::ostringstream out;
    out << "LocalColumnIndex{index=" << projection.index
        << ", project_all_children=" << projection.project_all_children << ", children="
        << join_debug_strings(projection.children,
                              [](const LocalColumnIndex& child) {
                                  return TableColumnMapper::debug_string(child);
                              })
        << "}";
    return out.str();
}

std::string TableColumnMapper::debug_string(const FileColumnPredicateFilter& filter) {
    std::ostringstream out;
    out << "FileColumnPredicateFilter{file_column_id=" << filter.file_column_id
        << ", file_child_id_path=" << int_vector_debug_string(filter.file_child_id_path)
        << ", predicate_count=" << filter.predicates.size() << "}";
    return out.str();
}

std::string TableColumnMapper::debug_string(const FileScanRequest& request) {
    std::ostringstream out;
    out << "FileScanRequest{predicate_columns="
        << join_debug_strings(request.predicate_columns,
                              [](const LocalColumnIndex& projection) {
                                  return TableColumnMapper::debug_string(projection);
                              })
        << ", non_predicate_columns="
        << join_debug_strings(request.non_predicate_columns,
                              [](const LocalColumnIndex& projection) {
                                  return TableColumnMapper::debug_string(projection);
                              })
        << ", local_positions={";
    size_t position_idx = 0;
    for (const auto& [column_id, block_position] : request.local_positions) {
        if (position_idx++ > 0) {
            out << ", ";
        }
        out << column_id << ":" << block_position;
    }
    out << "}, conjunct_count=" << request.conjuncts.size()
        << ", delete_conjunct_count=" << request.delete_conjuncts.size()
        << ", column_predicate_filters="
        << join_debug_strings(request.column_predicate_filters,
                              [](const FileColumnPredicateFilter& filter) {
                                  return TableColumnMapper::debug_string(filter);
                              })
        << "}";
    return out.str();
}

std::string ColumnMapping::debug_string() const {
    std::ostringstream out;
    out << "ColumnMapping{global_index=" << global_index
        << ", table_column_name=" << table_column_name << ", field_id=";
    if (field_id.has_value()) {
        out << *field_id;
    } else {
        out << "null";
    }
    out << ", constant_index=";
    if (constant_index.has_value()) {
        out << *constant_index;
    } else {
        out << "null";
    }
    out << ", file_column_name=" << file_column_name
        << ", original_file_type=" << data_type_debug_string(original_file_type)
        << ", original_file_children="
        << join_debug_strings(original_file_children,
                              [](const ColumnDefinition& child) {
                                  return TableColumnMapper::debug_string(child);
                              })
        << ", file_type=" << data_type_debug_string(file_type)
        << ", table_type=" << data_type_debug_string(table_type)
        << ", has_projection=" << (projection != nullptr) << ", child_mappings="
        << join_debug_strings(child_mappings,
                              [](const ColumnMapping& child) { return child.debug_string(); })
        << ", is_trivial=" << is_trivial << ", is_constant=" << constant_index.has_value()
        << ", has_complex_projection=" << has_complex_projection
        << ", filter_conversion=" << filter_conversion_type_to_string(filter_conversion)
        << ", virtual_column_type=" << virtual_column_type_to_string(virtual_column_type)
        << ", has_default_expr=" << (default_expr != nullptr) << "}";
    return out.str();
}

std::string TableColumnMapper::debug_string() const {
    std::ostringstream out;
    out << "TableColumnMapper{options=" << _options.debug_string() << ", mappings="
        << join_debug_strings(_mappings,
                              [](const ColumnMapping& mapping) { return mapping.debug_string(); })
        << ", constant_count=" << _constant_map.size() << "}";
    return out.str();
}

static const FileSlotRewriteInfo* find_slot_rewrite_info(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
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
    const auto rewrite_it =
            global_to_file_slot.find(GlobalIndex(cast_set<size_t>(candidate_slot_ref->slot_id())));
    if (rewrite_it == global_to_file_slot.end()) {
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

static bool filter_conversion_has_local_source(FilterConversionType conversion) {
    switch (conversion) {
    case FilterConversionType::COPY_DIRECTLY:
    case FilterConversionType::CAST_FILTER:
    case FilterConversionType::READER_EXPRESSION:
        return true;
    case FilterConversionType::FINALIZE_ONLY:
    case FilterConversionType::CONSTANT:
        return false;
    }
    return false;
}

static bool column_predicate_can_use_local_source(FilterConversionType conversion) {
    switch (conversion) {
    case FilterConversionType::COPY_DIRECTLY:
        return true;
    case FilterConversionType::CAST_FILTER:
    case FilterConversionType::READER_EXPRESSION:
    case FilterConversionType::FINALIZE_ONLY:
    case FilterConversionType::CONSTANT:
        return false;
    }
    return false;
}

static bool table_filter_has_only_local_entries(
        const TableFilter& table_filter, const std::map<GlobalIndex, FilterEntry>& filter_entries) {
    for (const auto global_index : table_filter.global_indices) {
        const auto entry_it = filter_entries.find(global_index);
        if (entry_it == filter_entries.end() || !entry_it->second.is_local()) {
            return false;
        }
    }
    return true;
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

static bool is_struct_element_expr(const VExprSPtr& expr) {
    return expr != nullptr && expr->get_num_children() == 2 &&
           expr->fn().name.function_name == "struct_element";
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

    StructChildSelector selector;
    if (!parse_struct_child_selector(expr->children()[1], &selector)) {
        return false;
    }

    const auto& parent = expr->children()[0];
    if (parent->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(parent.get());
        path->root_global_index = GlobalIndex(cast_set<size_t>(slot_ref->slot_id()));
        path->selectors.clear();
        path->selectors.push_back(std::move(selector));
        return true;
    }

    if (!extract_nested_struct_path(parent, path)) {
        return false;
    }
    path->selectors.push_back(std::move(selector));
    return true;
}

static void collect_nested_struct_paths(const VExprSPtr& expr,
                                        std::vector<NestedStructPath>* paths) {
    DORIS_CHECK(paths != nullptr);
    if (expr == nullptr) {
        return;
    }
    NestedStructPath path;
    if (extract_nested_struct_path(expr, &path)) {
        paths->push_back(std::move(path));
        return;
    }
    for (const auto& child : expr->children()) {
        collect_nested_struct_paths(child, paths);
    }
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

static const ColumnMapping* resolve_mapped_child(const std::vector<ColumnMapping>& child_mappings,
                                                 const StructChildSelector& selector) {
    if (selector.by_name) {
        const auto child_it = std::ranges::find_if(child_mappings, [&](const auto& child_mapping) {
            return child_mapping.table_column_name == selector.name;
        });
        return child_it == child_mappings.end() ? nullptr : &*child_it;
    }
    if (selector.ordinal == 0 || selector.ordinal > child_mappings.size()) {
        return nullptr;
    }
    return &child_mappings[selector.ordinal - 1];
}

static Status build_filter_projection_path(const std::vector<ColumnDefinition>& children,
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
    projection->index = child->field_id();
    projection->project_all_children = selectors.size() == 1;
    projection->children.clear();
    if (selectors.size() == 1) {
        return Status::OK();
    }
    if (child->children.empty() ||
        remove_nullable(child->type)->get_primitive_type() != TYPE_STRUCT) {
        projection->index = -1;
        return Status::OK();
    }
    LocalColumnIndex child_projection;
    RETURN_IF_ERROR(
            build_filter_projection_path(child->children, selectors.subspan(1), &child_projection));
    if (child_projection.index < 0) {
        projection->index = -1;
        return Status::OK();
    }
    projection->children.push_back(std::move(child_projection));
    return Status::OK();
}

// Prefer the table-to-file mapping tree for nested filter projection. This keeps renamed
// children and field-id schema evolution in the mapper instead of leaking table names into the
// file reader request. The file schema fallback below is only for filter-only children that do not
// have an output child mapping yet.
static Status build_filter_projection_path(const ColumnMapping& mapping,
                                           std::span<const StructChildSelector> selectors,
                                           LocalColumnIndex* projection) {
    DORIS_CHECK(projection != nullptr);
    if (selectors.empty()) {
        return Status::InvalidArgument("Nested struct selector path is empty");
    }
    const auto* child_mapping = resolve_mapped_child(mapping.child_mappings, selectors.front());
    if (child_mapping == nullptr) {
        return build_filter_projection_path(mapping.original_file_children, selectors, projection);
    }
    if (!child_mapping->field_id.has_value()) {
        projection->index = -1;
        return Status::OK();
    }
    projection->index = *child_mapping->field_id;
    projection->project_all_children = selectors.size() == 1;
    projection->children.clear();
    if (selectors.size() == 1) {
        return Status::OK();
    }
    LocalColumnIndex child_projection;
    if (child_mapping->child_mappings.empty()) {
        RETURN_IF_ERROR(build_filter_projection_path(child_mapping->original_file_children,
                                                     selectors.subspan(1), &child_projection));
    } else {
        RETURN_IF_ERROR(build_filter_projection_path(*child_mapping, selectors.subspan(1),
                                                     &child_projection));
    }
    if (child_projection.index < 0) {
        projection->index = -1;
        return Status::OK();
    }
    projection->children.push_back(std::move(child_projection));
    return Status::OK();
}

static const ColumnDefinition* resolve_filter_schema_path(
        const std::vector<ColumnDefinition>& children,
        std::span<const StructChildSelector> selectors, std::vector<int32_t>* file_child_id_path) {
    DORIS_CHECK(file_child_id_path != nullptr);
    if (selectors.empty()) {
        return nullptr;
    }
    const auto* child = resolve_file_child(children, selectors.front());
    if (child == nullptr) {
        return nullptr;
    }
    file_child_id_path->push_back(child->field_id());
    if (selectors.size() == 1) {
        return child;
    }
    if (child->children.empty() ||
        remove_nullable(child->type)->get_primitive_type() != TYPE_STRUCT) {
        file_child_id_path->clear();
        return nullptr;
    }
    const auto* leaf =
            resolve_filter_schema_path(child->children, selectors.subspan(1), file_child_id_path);
    if (leaf == nullptr) {
        file_child_id_path->clear();
    }
    return leaf;
}

// Resolve a nested predicate through ColumnMapping when possible. The returned child-id path and
// leaf type are file-local, so parquet pruning can stay independent from table/global schema.
static bool resolve_mapped_filter_schema_path(const ColumnMapping& mapping,
                                              std::span<const StructChildSelector> selectors,
                                              std::vector<int32_t>* file_child_id_path,
                                              std::string* leaf_name, DataTypePtr* leaf_type) {
    DORIS_CHECK(file_child_id_path != nullptr);
    DORIS_CHECK(leaf_name != nullptr);
    DORIS_CHECK(leaf_type != nullptr);
    if (selectors.empty()) {
        return false;
    }
    const auto* child_mapping = resolve_mapped_child(mapping.child_mappings, selectors.front());
    if (child_mapping == nullptr) {
        return false;
    }
    if (!child_mapping->field_id.has_value()) {
        file_child_id_path->clear();
        return false;
    }
    file_child_id_path->push_back(*child_mapping->field_id);
    if (selectors.size() == 1) {
        if (child_mapping->file_type == nullptr ||
            is_complex_type(remove_nullable(child_mapping->file_type)->get_primitive_type())) {
            file_child_id_path->clear();
            return false;
        }
        *leaf_name = child_mapping->file_column_name;
        *leaf_type = remove_nullable(child_mapping->file_type);
        return true;
    }
    if (child_mapping->child_mappings.empty()) {
        file_child_id_path->clear();
        return false;
    }
    if (!resolve_mapped_filter_schema_path(*child_mapping, selectors.subspan(1), file_child_id_path,
                                           leaf_name, leaf_type)) {
        file_child_id_path->clear();
        return false;
    }
    return true;
}

static bool resolve_nested_predicate_target(const NestedStructPath& path,
                                            const std::vector<ColumnMapping>& mappings,
                                            NestedPredicateTargetInfo* target) {
    DORIS_CHECK(target != nullptr);
    if (path.selectors.empty()) {
        return false;
    }
    const auto mapping_it = std::ranges::find_if(mappings, [&](const ColumnMapping& mapping) {
        return mapping.global_index == path.root_global_index;
    });
    if (mapping_it == mappings.end() || !mapping_it->field_id.has_value()) {
        return false;
    }
    std::vector<int32_t> file_child_id_path;
    std::string leaf_name;
    DataTypePtr file_leaf_type;
    if (resolve_mapped_filter_schema_path(*mapping_it, path.selectors, &file_child_id_path,
                                          &leaf_name, &file_leaf_type)) {
        target->root_file_column_id = *mapping_it->field_id;
        target->file_child_id_path = std::move(file_child_id_path);
        target->leaf_name = std::move(leaf_name);
        target->file_leaf_type = std::move(file_leaf_type);
        return true;
    }

    const auto* leaf = resolve_filter_schema_path(mapping_it->original_file_children,
                                                  path.selectors, &file_child_id_path);
    if (leaf == nullptr || leaf->type == nullptr ||
        is_complex_type(remove_nullable(leaf->type)->get_primitive_type())) {
        return false;
    }

    target->root_file_column_id = *mapping_it->field_id;
    target->file_child_id_path = std::move(file_child_id_path);
    target->leaf_name = leaf->name;
    target->file_leaf_type = remove_nullable(leaf->type);
    return true;
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

static std::shared_ptr<ColumnPredicate> build_nested_comparison_predicate(
        const VExprSPtr& literal_expr, TExprOpcode::type opcode,
        const NestedPredicateTargetInfo& target) {
    if (literal_expr == nullptr || !literal_expr->is_literal() ||
        target.file_leaf_type == nullptr) {
        return nullptr;
    }
    const auto predicate_type = to_column_predicate_type(opcode);
    if (!predicate_type.has_value()) {
        return nullptr;
    }
    const auto original_literal = original_table_literal(literal_expr);
    const Field original_field = literal_field(original_literal);
    Field file_field;
    try {
        convert_field_to_type(original_field, *target.file_leaf_type, &file_field,
                              original_literal->data_type().get());
    } catch (const Exception&) {
        return nullptr;
    }
    if (file_field.is_null()) {
        return nullptr;
    }
    try {
        return create_comparison_column_predicate(
                *predicate_type, cast_set<uint32_t>(target.root_file_column_id), target.leaf_name,
                target.file_leaf_type, file_field);
    } catch (const Exception&) {
        return nullptr;
    }
}

static std::shared_ptr<ColumnPredicate> build_nested_in_list_predicate(
        const VExprSPtrs& literal_exprs, const NestedPredicateTargetInfo& target) {
    if (literal_exprs.empty() || target.file_leaf_type == nullptr) {
        return nullptr;
    }

    auto value_column = target.file_leaf_type->create_column();
    for (const auto& literal_expr : literal_exprs) {
        if (literal_expr == nullptr || !literal_expr->is_literal()) {
            return nullptr;
        }
        const auto original_literal = original_table_literal(literal_expr);
        const Field original_field = literal_field(original_literal);
        Field file_field;
        try {
            convert_field_to_type(original_field, *target.file_leaf_type, &file_field,
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
        values.reset(create_set(target.file_leaf_type->get_primitive_type(), literal_exprs.size(),
                                false));
        ColumnPtr value_column_ptr = std::move(value_column);
        values->insert_range_from(value_column_ptr, 0, value_column_ptr->size());
        return create_in_list_predicate<PredicateType::IN_LIST>(
                cast_set<uint32_t>(target.root_file_column_id), target.leaf_name,
                target.file_leaf_type, values, false);
    } catch (const Exception&) {
        return nullptr;
    }
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
    if (extract_nested_struct_path(expr->children()[0], &path) &&
        expr->children()[1]->is_literal()) {
        literal_expr = expr->children()[1];
    } else if (extract_nested_struct_path(expr->children()[1], &path) &&
               expr->children()[0]->is_literal()) {
        literal_expr = expr->children()[0];
        opcode = reverse_comparison_opcode(opcode);
    } else {
        return false;
    }

    NestedPredicateTargetInfo target;
    if (!resolve_nested_predicate_target(path, mappings, &target)) {
        return false;
    }
    auto predicate = build_nested_comparison_predicate(literal_expr, opcode, target);
    if (predicate == nullptr) {
        return false;
    }
    column_filter->file_column_id = LocalColumnId(target.root_file_column_id);
    column_filter->file_child_id_path = std::move(target.file_child_id_path);
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
    if (!extract_nested_struct_path(expr->children()[0], &path)) {
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

    NestedPredicateTargetInfo target;
    if (!resolve_nested_predicate_target(path, mappings, &target)) {
        return false;
    }
    auto predicate = build_nested_in_list_predicate(literal_exprs, target);
    if (predicate == nullptr) {
        return false;
    }
    column_filter->file_column_id = LocalColumnId(target.root_file_column_id);
    column_filter->file_child_id_path = std::move(target.file_child_id_path);
    column_filter->predicates.push_back(std::move(predicate));
    return true;
}

static void merge_column_predicate_filter(FileColumnPredicateFilter column_filter,
                                          std::vector<FileColumnPredicateFilter>* filters) {
    DORIS_CHECK(filters != nullptr);
    auto existing_filter_it = std::ranges::find_if(*filters, [&](const auto& existing_filter) {
        return existing_filter.file_column_id == column_filter.file_column_id &&
               existing_filter.file_child_id_path == column_filter.file_child_id_path;
    });
    if (existing_filter_it == filters->end()) {
        filters->push_back(std::move(column_filter));
        return;
    }
    existing_filter_it->predicates.insert(existing_filter_it->predicates.end(),
                                          column_filter.predicates.begin(),
                                          column_filter.predicates.end());
}

static void collect_nested_column_predicate_filters(
        const VExprSPtr& expr, const std::vector<ColumnMapping>& mappings,
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
        extract_nested_in_list_filter(expr, mappings, &column_filter)) {
        merge_column_predicate_filter(std::move(column_filter), filters);
    }
}

static Status build_projected_type_from_projection(const DataTypePtr& file_type,
                                                   const std::vector<ColumnDefinition>& children,
                                                   const LocalColumnIndex& projection,
                                                   DataTypePtr* projected_type) {
    DORIS_CHECK(file_type != nullptr);
    DORIS_CHECK(projected_type != nullptr);
    ColumnDefinition field;
    field.type = file_type;
    field.children = children;
    ColumnDefinition projected_field;
    RETURN_IF_ERROR(project_column_definition(field, projection, &projected_field));
    *projected_type = std::move(projected_field.type);
    return Status::OK();
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

static bool rewrite_binary_slot_literal_predicate(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot) {
    if (!is_binary_comparison_predicate(expr)) {
        return false;
    }
    auto children = expr->children();
    const VSlotRef* slot_ref = nullptr;
    const FileSlotRewriteInfo* rewrite_info =
            find_slot_rewrite_info(children[0], global_to_file_slot, &slot_ref);
    int slot_child_idx = 0;
    int literal_child_idx = 1;
    if (rewrite_info == nullptr) {
        rewrite_info = find_slot_rewrite_info(children[1], global_to_file_slot, &slot_ref);
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

static bool rewrite_in_slot_literal_predicate(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot) {
    if (expr->node_type() != TExprNodeType::IN_PRED || expr->get_num_children() < 2) {
        return false;
    }
    auto children = expr->children();
    const VSlotRef* slot_ref = nullptr;
    const FileSlotRewriteInfo* rewrite_info =
            find_slot_rewrite_info(children[0], global_to_file_slot, &slot_ref);
    if (rewrite_info == nullptr || slot_ref == nullptr) {
        return false;
    }

    VExprSPtrs rewritten_literals;
    rewritten_literals.reserve(children.size() - 1);
    for (size_t child_idx = 1; child_idx < children.size(); ++child_idx) {
        auto literal_expr =
                unwrap_literal_for_file_cast(children[child_idx], rewrite_info->table_type);
        if (literal_expr == nullptr) {
            return false;
        }
        auto rewritten_literal = rewrite_literal_to_file_type(literal_expr, *rewrite_info);
        if (rewritten_literal == nullptr) {
            for (size_t restore_idx = 1; restore_idx < children.size(); ++restore_idx) {
                auto restore_literal = unwrap_literal_for_file_cast(children[restore_idx],
                                                                    rewrite_info->table_type);
                if (restore_literal != nullptr) {
                    children[restore_idx] = original_table_literal(restore_literal);
                }
            }
            expr->set_children(std::move(children));
            return false;
        }
        rewritten_literals.push_back(std::move(rewritten_literal));
    }

    children[0] = create_file_slot_ref(*slot_ref, *rewrite_info);
    for (size_t literal_idx = 0; literal_idx < rewritten_literals.size(); ++literal_idx) {
        children[literal_idx + 1] = std::move(rewritten_literals[literal_idx]);
    }
    expr->set_children(std::move(children));
    return true;
}

static VExprSPtr rewrite_table_expr_to_file_expr(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot) {
    if (expr == nullptr) {
        return nullptr;
    }
    if (rewrite_binary_slot_literal_predicate(expr, global_to_file_slot)) {
        return expr;
    }
    if (rewrite_in_slot_literal_predicate(expr, global_to_file_slot)) {
        return expr;
    }
    if (is_struct_element_expr(expr)) {
        auto children = expr->children();
        if (children[0]->is_slot_ref()) {
            const auto* slot_ref = assert_cast<const VSlotRef*>(children[0].get());
            const auto rewrite_it =
                    global_to_file_slot.find(GlobalIndex(cast_set<size_t>(slot_ref->slot_id())));
            if (rewrite_it != global_to_file_slot.end()) {
                // struct_element must see the actual file struct layout. Casting the parent struct
                // to the output projection can hide filter-only children such as `s.id` in
                // `SELECT s.name WHERE s.id > 5`.
                children[0] = create_file_slot_ref(*slot_ref, rewrite_it->second);
                expr->set_children(std::move(children));
                return expr;
            }
        }
        children[0] = rewrite_table_expr_to_file_expr(children[0], global_to_file_slot);
        expr->set_children(std::move(children));
        return expr;
    }
    if (expr->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
        const auto rewrite_it =
                global_to_file_slot.find(GlobalIndex(cast_set<size_t>(slot_ref->slot_id())));
        if (rewrite_it != global_to_file_slot.end()) {
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
            const auto rewrite_it =
                    global_to_file_slot.find(GlobalIndex(cast_set<size_t>(slot_ref->slot_id())));
            if (rewrite_it != global_to_file_slot.end() &&
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
        rewritten_children.push_back(rewrite_table_expr_to_file_expr(child, global_to_file_slot));
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
        if (child_mapping.table_column_name != child_mapping.file_column_name ||
            !child_mapping.field_id.has_value() ||
            complex_projection_has_pruned_children(child_mapping)) {
            return true;
        }
    }
    return false;
}

static Status build_projected_child_type(const DataTypePtr& file_type,
                                         const std::vector<ColumnMapping>& child_mappings,
                                         DataTypePtr* projected_type) {
    DORIS_CHECK(file_type != nullptr);
    DORIS_CHECK(projected_type != nullptr);
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
    return rebuild_projected_type(file_type, child_types, child_names, projected_type);
}

static Status build_complex_projection(const ColumnMapping& mapping, LocalColumnIndex* projection) {
    if (projection == nullptr) {
        return Status::InvalidArgument("projection is null");
    }
    DORIS_CHECK(mapping.field_id.has_value());
    projection->index = *mapping.field_id;
    projection->project_all_children = mapping.child_mappings.empty();
    projection->children.clear();
    for (const auto& child_mapping : mapping.child_mappings) {
        if (!child_mapping.field_id.has_value()) {
            continue;
        }
        LocalColumnIndex child_projection;
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
    if (mapping->original_file_type == nullptr) {
        mapping->original_file_type = mapping->file_type;
    }
    DORIS_CHECK(
            is_complex_type(remove_nullable(mapping->original_file_type)->get_primitive_type()));
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
    RETURN_IF_ERROR(build_projected_child_type(mapping->original_file_type, mapping->child_mappings,
                                               &mapping->file_type));
    mapping->is_trivial =
            mapping->table_type != nullptr && mapping->table_type->equals(*mapping->file_type);
    mapping->has_complex_projection = true;
    return Status::OK();
}

using FilterProjectionMap = std::map<LocalColumnId, LocalColumnIndex>;

static Status apply_projection_to_mapping_file_type(const LocalColumnIndex& projection,
                                                    ColumnMapping* mapping) {
    DORIS_CHECK(mapping != nullptr);
    if (mapping->original_file_type == nullptr) {
        mapping->original_file_type = mapping->file_type;
    }
    if (mapping->original_file_type == nullptr ||
        !is_complex_type(remove_nullable(mapping->original_file_type)->get_primitive_type())) {
        return Status::OK();
    }
    DataTypePtr projected_type;
    RETURN_IF_ERROR(build_projected_type_from_projection(mapping->original_file_type,
                                                         mapping->original_file_children,
                                                         projection, &projected_type));
    mapping->file_type = std::move(projected_type);
    mapping->has_complex_projection = !projection.project_all_children;
    mapping->is_trivial =
            mapping->table_type != nullptr && mapping->table_type->equals(*mapping->file_type);
    return Status::OK();
}

static Status merge_filter_projection(const FilterProjectionMap* filter_projections,
                                      LocalColumnIndex* projection) {
    DORIS_CHECK(projection != nullptr);
    if (filter_projections == nullptr) {
        return Status::OK();
    }
    const auto filter_projection_it = filter_projections->find(projection->column_id());
    if (filter_projection_it == filter_projections->end()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(merge_local_column_index(projection, filter_projection_it->second));
    return Status::OK();
}

static Status add_scan_column(FileScanRequest* file_request, ColumnMapping* mapping,
                              std::vector<LocalColumnIndex>* scan_columns,
                              const FilterProjectionMap* filter_projections = nullptr) {
    const auto file_column_id = LocalColumnId(mapping->field_id.value());
    if (scan_columns == &file_request->non_predicate_columns &&
        std::ranges::find_if(file_request->predicate_columns, [&](const LocalColumnIndex& p) {
            return p.column_id() == file_column_id;
        }) != file_request->predicate_columns.end()) {
        return Status::OK();
    }
    // local_positions is the global read-column index for this scan request, so it also
    // deduplicates predicate_columns and non_predicate_columns across all filter/projection paths.
    const bool newly_added = file_request->local_positions.count(file_column_id) == 0;
    if (newly_added) {
        file_request->local_positions.emplace(file_column_id,
                                              LocalIndex(file_request->local_positions.size()));
    }
    LocalColumnIndex projection {.index = file_column_id.value()};
    if (mapping->has_complex_projection || complex_projection_has_pruned_children(*mapping)) {
        if (!mapping->has_complex_projection) {
            RETURN_IF_ERROR(rebuild_projected_file_type(mapping));
        }
        RETURN_IF_ERROR(build_complex_projection(*mapping, &projection));
    }
    if (scan_columns == &file_request->predicate_columns) {
        RETURN_IF_ERROR(merge_filter_projection(filter_projections, &projection));
    }
    RETURN_IF_ERROR(apply_projection_to_mapping_file_type(projection, mapping));

    auto existing_projection_it = std::ranges::find_if(
            *scan_columns,
            [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; });
    if (existing_projection_it == scan_columns->end()) {
        scan_columns->push_back(std::move(projection));
    } else {
        RETURN_IF_ERROR(merge_local_column_index(&*existing_projection_it, projection));
        RETURN_IF_ERROR(apply_projection_to_mapping_file_type(*existing_projection_it, mapping));
    }
    if (scan_columns == &file_request->predicate_columns) {
        file_request->non_predicate_columns.erase(
                std::ranges::find_if(
                        file_request->non_predicate_columns,
                        [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; }),
                file_request->non_predicate_columns.end());
    }
    return Status::OK();
}

static Status build_filter_projection_map(const std::vector<TableFilter>& table_filters,
                                          std::vector<ColumnMapping>* mappings,
                                          FilterProjectionMap* filter_projections) {
    DORIS_CHECK(mappings != nullptr);
    DORIS_CHECK(filter_projections != nullptr);
    filter_projections->clear();
    for (const auto& table_filter : table_filters) {
        if (table_filter.conjunct == nullptr) {
            continue;
        }
        std::vector<NestedStructPath> paths;
        collect_nested_struct_paths(table_filter.conjunct->root(), &paths);
        for (const auto& path : paths) {
            auto mapping_it = std::ranges::find_if(*mappings, [&](const ColumnMapping& mapping) {
                return mapping.global_index == path.root_global_index;
            });
            if (mapping_it == mappings->end() || !mapping_it->field_id.has_value() ||
                path.selectors.empty()) {
                continue;
            }

            LocalColumnIndex child_projection;
            RETURN_IF_ERROR(
                    build_filter_projection_path(*mapping_it, path.selectors, &child_projection));
            if (child_projection.index < 0) {
                continue;
            }

            LocalColumnIndex root_projection {.index = *mapping_it->field_id,
                                              .project_all_children = false};
            root_projection.children.push_back(std::move(child_projection));
            auto filter_projection_it = filter_projections->find(root_projection.column_id());
            if (filter_projection_it == filter_projections->end()) {
                filter_projections->emplace(root_projection.column_id(),
                                            std::move(root_projection));
                continue;
            }
            RETURN_IF_ERROR(
                    merge_local_column_index(&filter_projection_it->second, root_projection));
        }
    }
    return Status::OK();
}

static void rebuild_projection(ColumnMapping* mapping, LocalIndex block_position) {
    DORIS_CHECK(mapping->field_id.has_value());
    if (mapping->is_trivial || mapping->has_complex_projection) {
        mapping->projection = VExprContext::create_shared(TableSlotRef::create_shared(
                cast_set<int>(block_position.value()), cast_set<int>(block_position.value()), -1,
                mapping->file_type, mapping->file_column_name));
        return;
    }

    auto expr = Cast::create_shared(mapping->table_type);
    expr->add_child(TableSlotRef::create_shared(cast_set<int>(block_position.value()),
                                                cast_set<int>(block_position.value()), -1,
                                                mapping->file_type, mapping->file_column_name));
    mapping->projection = VExprContext::create_shared(expr);
}

static std::shared_ptr<IndexMapping> build_child_index_mapping(const ColumnMapping& mapping) {
    DORIS_CHECK(mapping.field_id.has_value());
    auto result = std::make_shared<IndexMapping>();
    result->index = *mapping.field_id;
    for (size_t child_idx = 0; child_idx < mapping.child_mappings.size(); ++child_idx) {
        const auto& child_mapping = mapping.child_mappings[child_idx];
        if (!child_mapping.field_id.has_value()) {
            continue;
        }
        result->child_mapping.emplace(cast_set<int32_t>(child_idx),
                                      build_child_index_mapping(child_mapping));
    }
    return result;
}

static IndexMapping build_index_mapping(const ColumnMapping& mapping, LocalIndex block_position) {
    DORIS_CHECK(mapping.field_id.has_value());
    IndexMapping result;
    result.index = cast_set<int32_t>(block_position.value());
    for (size_t child_idx = 0; child_idx < mapping.child_mappings.size(); ++child_idx) {
        const auto& child_mapping = mapping.child_mappings[child_idx];
        if (!child_mapping.field_id.has_value()) {
            continue;
        }
        result.child_mapping.emplace(cast_set<int32_t>(child_idx),
                                     build_child_index_mapping(child_mapping));
    }
    return result;
}

static Status build_column_map_result(const ColumnMapping& mapping,
                                      const FileScanRequest& file_request,
                                      ColumnMapResult* result) {
    DORIS_CHECK(result != nullptr);
    *result = {};
    result->projection = mapping.projection;
    result->default_expr = mapping.default_expr;
    if (!mapping.field_id.has_value()) {
        return Status::OK();
    }

    const auto local_column_id = LocalColumnId(*mapping.field_id);
    result->local_column_id = local_column_id;
    LocalColumnIndex column_index {.index = local_column_id.value()};
    if (mapping.has_complex_projection || complex_projection_has_pruned_children(mapping)) {
        RETURN_IF_ERROR(build_complex_projection(mapping, &column_index));
    }
    result->column_index = std::move(column_index);

    const auto position_it = file_request.local_positions.find(local_column_id);
    DORIS_CHECK(position_it != file_request.local_positions.end())
            << file_request.local_positions.size() << " " << *mapping.field_id << " "
            << mapping.file_column_name;
    result->mapping = build_index_mapping(mapping, position_it->second);
    return Status::OK();
}

// Build file slot rewrite info from the localized filter targets. Only local targets can enter
// file-reader expressions; constant and unset targets stay above the file reader.
static std::map<GlobalIndex, FileSlotRewriteInfo> build_file_slot_rewrite_map(
        const std::vector<ColumnMapping>& mappings,
        const std::map<GlobalIndex, FilterEntry>& filter_entries) {
    std::map<GlobalIndex, FileSlotRewriteInfo> global_to_file_slot;
    for (const auto& mapping : mappings) {
        const auto entry_it = filter_entries.find(mapping.global_index);
        if (entry_it == filter_entries.end() || !entry_it->second.is_local()) {
            continue;
        }
        DORIS_CHECK(mapping.field_id.has_value());
        global_to_file_slot.emplace(
                mapping.global_index,
                FileSlotRewriteInfo {.block_position = entry_it->second.local_index().value(),
                                     .file_type = mapping.file_type,
                                     .table_type = mapping.table_type,
                                     .file_column_name = mapping.file_column_name});
    }
    return global_to_file_slot;
}

static const ColumnDefinition* find_file_child_by_table_column(
        const ColumnDefinition& table_column, const std::vector<ColumnDefinition>& file_children,
        TableColumnMappingMode mode) {
    return matcher_for_mode(mode).find(table_column, file_children);
}

static const ColumnDefinition* find_file_child_for_complex_wrapper(
        const ColumnDefinition& table_child, const ColumnDefinition& file_field,
        TableColumnMappingMode mode) {
    const auto primitive_type = remove_nullable(file_field.type)->get_primitive_type();
    if (primitive_type == TYPE_ARRAY || primitive_type == TYPE_MAP) {
        if (file_field.children.empty()) {
            return nullptr;
        }
        DORIS_CHECK(file_field.children.size() == 1);
        return &file_field.children[0];
    }
    return find_file_child_by_table_column(table_child, file_field.children, mode);
}

Status TableColumnMapper::create_mapping(const std::vector<ColumnDefinition>& projected_columns,
                                         const std::map<std::string, Field>& partition_values,
                                         const std::vector<ColumnDefinition>& file_schema) {
    clear();
    for (size_t column_idx = 0; column_idx < projected_columns.size(); ++column_idx) {
        const auto& table_column = projected_columns[column_idx];
        ColumnMapping mapping;
        mapping.global_index = GlobalIndex(column_idx);
        mapping.table_column_name = table_column.name;
        mapping.table_type = table_column.type;
        if (table_column.is_partition_key && partition_values.contains(table_column.name)) {
            // 1. Partition column, use partition value as a constant mapping. Note that partition column may also have default expression, but partition value should take precedence if it exists.
            _set_constant_mapping(
                    &mapping, VExprContext::create_shared(TableLiteral::create_shared(
                                      mapping.table_type, partition_values.at(table_column.name))));
        } else if (_options.mode == TableColumnMappingMode::BY_INDEX &&
                   !table_column.is_partition_key) {
            RETURN_IF_ERROR(_create_by_index_mapping(table_column, file_schema, &mapping));
        } else if (const auto* file_field = _find_file_field(table_column, file_schema)) {
            // 2. Table column has a matching file column, use it as a direct mapping.
            RETURN_IF_ERROR(_create_direct_mapping(table_column, *file_field, &mapping));
        } else if (table_column.default_expr != nullptr) {
            // 3. Table column does not exist in file (column adding by schema evolution), which has a default expression, use it as a constant mapping.
            _set_constant_mapping(&mapping, table_column.default_expr);
        } else if (table_column.name == ROW_LINEAGE_ROW_ID) {
            // 4. Virtual column, use special mapping to indicate it should be materialized by table reader instead of read from file or evaluated from expression.
            mapping.virtual_column_type = TableVirtualColumnType::ROW_ID;
        } else if (table_column.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
            mapping.virtual_column_type = TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER;
        } else {
            if (table_column.is_partition_key) {
                return Status::InvalidArgument(
                        "Table column '{}' (global_index={}) does not have a matching partition "
                        "value",
                        table_column.name, mapping.global_index.value());
            }
            if (!_options.allow_missing_columns) {
                return Status::InvalidArgument(
                        "Table column '{}' (global_index={}) does not have a matching file column",
                        table_column.name, mapping.global_index.value());
            }
        }
        _mappings.push_back(std::move(mapping));
    }
    return Status::OK();
}

Status TableColumnMapper::_create_by_index_mapping(const ColumnDefinition& table_column,
                                                   const std::vector<ColumnDefinition>& file_schema,
                                                   ColumnMapping* mapping) {
    DORIS_CHECK(mapping != nullptr);
    DORIS_CHECK(!table_column.is_partition_key);

    // Key contract: in BY_INDEX mode, `ColumnDefinition::Identifier::POSITION` is interpreted as the
    // 0-based position of this column inside `file_schema`. FE writes the physical file position
    // of each non-partition projected column into that identifier. This interpretation allows:
    //   - sparse projection: read only a subset of file columns (for example only `_col2`
    //     and `_col4`);
    //   - column reordering: table column order differs from file column order;
    //   - no many-to-one mapping: FE must guarantee that each file position is referenced by at
    //     most one table column.
    const auto file_index = table_column.file_position();

    // Case A: file_index is in range, so build a direct positional mapping.
    // The file column name (for example `_col0`) is intentionally ignored here.
    if (file_index >= 0 && static_cast<size_t>(file_index) < file_schema.size()) {
        return _create_direct_mapping(table_column, file_schema[static_cast<size_t>(file_index)],
                                      mapping);
    }

    // Case B: file_index is out of range, which means the file does not contain this column.
    // Route it through the missing-column path used by schema evolution.
    //   B1: the table column carries a default_expr injected by FE, so use the constant branch and
    //       materialize that value for every row.
    if (table_column.default_expr != nullptr) {
        _set_constant_mapping(mapping, table_column.default_expr);
        return Status::OK();
    }
    //   B2: if missing columns are not allowed, fail explicitly instead of silently producing
    //       NULLs and hiding the issue.
    if (!_options.allow_missing_columns) {
        return Status::InvalidArgument(
                "Table column '{}' (file_index={}) is out of range for file schema of size {}",
                table_column.name, file_index, file_schema.size());
    }
    //   B3: if missing columns are allowed, keep the mapping empty
    //       (`file_column_id` remains `nullopt`) and let the upper finalize stage fill
    //       NULL/default values.
    return Status::OK();
}

void TableColumnMapper::_set_constant_mapping(ColumnMapping* mapping, VExprContextSPtr expr) {
    DORIS_CHECK(mapping != nullptr);
    DORIS_CHECK(expr != nullptr);
    mapping->default_expr = std::move(expr);
    mapping->constant_index = _constant_map.add(ConstantEntry {
            .global_index = mapping->global_index,
            .expr = mapping->default_expr,
            .type = mapping->table_type,
    });
    mapping->filter_conversion = FilterConversionType::CONSTANT;
}

Status TableColumnMapper::_build_filter_entries(const FileScanRequest& file_request) {
    _filter_entries.clear();
    for (const auto& mapping : _mappings) {
        FilterEntry entry;
        if (mapping.constant_index.has_value()) {
            entry = FilterEntry::constant(*mapping.constant_index);
        } else if (mapping.field_id.has_value() &&
                   filter_conversion_has_local_source(mapping.filter_conversion)) {
            const auto local_position_it =
                    file_request.local_positions.find(LocalColumnId(*mapping.field_id));
            if (local_position_it != file_request.local_positions.end()) {
                entry = FilterEntry::local(local_position_it->second);
            }
        }
        _filter_entries.emplace(mapping.global_index, entry);
    }
    return Status::OK();
}

Status TableColumnMapper::_build_result_column_mapping(const FileScanRequest& file_request) {
    _column_map_results.clear();
    _result_mapping = {};
    for (const auto& mapping : _mappings) {
        ColumnMapResult map_result;
        RETURN_IF_ERROR(build_column_map_result(mapping, file_request, &map_result));

        if (map_result.mapping.has_value()) {
            _result_mapping.global_to_local.emplace(
                    mapping.global_index,
                    ColumnMapEntry {.mapping = *map_result.mapping,
                                    .local_type = mapping.file_type,
                                    .global_type = mapping.table_type,
                                    .filter_conversion = mapping.filter_conversion});
        }
        _column_map_results.emplace(mapping.global_index, std::move(map_result));
    }
    return Status::OK();
}

Status TableColumnMapper::create_scan_request(
        const std::vector<TableFilter>& table_filters,
        const TableColumnPredicates& table_column_predicates,
        const std::vector<ColumnDefinition>& projected_columns, FileScanRequest* file_request) {
    // FileReader evaluates expressions against a file-local block. This mapper owns the
    // table-column to file-column conversion, so it also owns the file-local block positions.
    file_request->predicate_columns.clear();
    file_request->non_predicate_columns.clear();
    file_request->local_positions.clear();
    file_request->conjuncts.clear();
    file_request->delete_conjuncts.clear();
    file_request->column_predicate_filters.clear();
    _filter_entries.clear();
    // 1. Build referenced non-predicate columns
    for (size_t column_idx = 0; column_idx < projected_columns.size(); ++column_idx) {
        const auto global_index = GlobalIndex(column_idx);
        auto* mapping = _find_mapping(global_index);
        if (mapping != nullptr && mapping->field_id.has_value()) {
            // A file column can be read lazily as a non-predicate column only when it is not used
            // by row-level expression filters. Single-column ColumnPredicate filters are pruning
            // hints only and must not force row-level predicate materialization.
            bool used_by_filter = false;
            for (const auto& table_filter : table_filters) {
                const auto& global_indices = table_filter.global_indices;
                if (std::find(global_indices.begin(), global_indices.end(), global_index) !=
                            global_indices.end() &&
                    filter_conversion_has_local_source(mapping->filter_conversion)) {
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
        auto position_it = file_request->local_positions.find(LocalColumnId(*mapping.field_id));
        DORIS_CHECK(position_it != file_request->local_positions.end())
                << file_request->local_positions.size() << " " << *mapping.field_id << " "
                << mapping.file_column_name;
        rebuild_projection(&mapping, position_it->second);
    }
    return _build_result_column_mapping(*file_request);
}

ColumnMapping* TableColumnMapper::_find_mapping(GlobalIndex global_index) {
    for (auto& mapping : _mappings) {
        if (mapping.global_index == global_index) {
            return &mapping;
        }
    }
    return nullptr;
}

const ColumnMapping* TableColumnMapper::_find_mapping(GlobalIndex global_index) const {
    for (const auto& mapping : _mappings) {
        if (mapping.global_index == global_index) {
            return &mapping;
        }
    }
    return nullptr;
}

Status TableColumnMapper::localize_filters(const std::vector<TableFilter>& table_filters,
                                           const TableColumnPredicates& table_column_predicates,
                                           FileScanRequest* file_request) {
    FilterProjectionMap filter_projections;
    RETURN_IF_ERROR(build_filter_projection_map(table_filters, &_mappings, &filter_projections));
    for (const auto& table_filter : table_filters) {
        for (const auto& global_index : table_filter.global_indices) {
            auto* mapping = _find_mapping(global_index);
            if (mapping == nullptr || !mapping->field_id.has_value() ||
                !filter_conversion_has_local_source(mapping->filter_conversion)) {
                continue;
            }
            RETURN_IF_ERROR(add_scan_column(file_request, mapping, &file_request->predicate_columns,
                                            &filter_projections));
        }
    }
    RETURN_IF_ERROR(_build_filter_entries(*file_request));

    // Build the complete table-slot rewrite map after all predicate columns have been assigned.
    // This keeps expression localization independent from filter iteration order.
    const auto global_to_file_slot = build_file_slot_rewrite_map(_mappings, _filter_entries);
    for (const auto& table_filter : table_filters) {
        if (table_filter.conjunct != nullptr &&
            table_filter_has_only_local_entries(table_filter, _filter_entries)) {
            file_request->conjuncts.push_back(
                    VExprContext::create_shared(rewrite_table_expr_to_file_expr(
                            table_filter.conjunct->root(), global_to_file_slot)));
            table_filter.conjunct->clone_fn_contexts(file_request->conjuncts.back().get());
        }
    }
    for (const auto& [global_index, predicates] : table_column_predicates) {
        const auto* mapping = _find_mapping(global_index);
        const auto entry_it = _filter_entries.find(global_index);
        if (mapping == nullptr || !mapping->field_id.has_value() || predicates.empty() ||
            entry_it == _filter_entries.end() || !entry_it->second.is_local() ||
            !column_predicate_can_use_local_source(mapping->filter_conversion)) {
            continue;
        }
        FileColumnPredicateFilter column_predicate_filter;
        column_predicate_filter.file_column_id = LocalColumnId(*mapping->field_id);
        column_predicate_filter.predicates = predicates;
        file_request->column_predicate_filters.push_back(std::move(column_predicate_filter));
    }
    for (const auto& table_filter : table_filters) {
        if (table_filter.conjunct == nullptr ||
            !table_filter_has_only_local_entries(table_filter, _filter_entries)) {
            continue;
        }
        std::vector<FileColumnPredicateFilter> nested_column_predicate_filters;
        collect_nested_column_predicate_filters(table_filter.conjunct->root(), _mappings,
                                                &nested_column_predicate_filters);
        for (auto& column_predicate_filter : nested_column_predicate_filters) {
            merge_column_predicate_filter(std::move(column_predicate_filter),
                                          &file_request->column_predicate_filters);
        }
    }
    return Status::OK();
}

const ColumnDefinition* TableColumnMapper::_find_file_field(
        const ColumnDefinition& table_column,
        const std::vector<ColumnDefinition>& file_schema) const {
    return matcher_for_mode(_options.mode).find(table_column, file_schema);
}

Status TableColumnMapper::_create_direct_mapping(const ColumnDefinition& table_column,
                                                 const ColumnDefinition& file_field,
                                                 ColumnMapping* mapping) const {
    DORIS_CHECK(mapping != nullptr);
    mapping->field_id = file_field.field_id();
    mapping->table_column_name = table_column.name;
    mapping->file_column_name = file_field.name;
    mapping->original_file_type = file_field.type;
    mapping->original_file_children = file_field.children;
    mapping->file_type = file_field.type;
    mapping->is_trivial = _is_same_type(mapping->table_type, mapping->file_type);
    mapping->filter_conversion = mapping->is_trivial ? FilterConversionType::COPY_DIRECTLY
                                                     : FilterConversionType::CAST_FILTER;
    mapping->child_mappings.clear();

    if (!table_column.children.empty()) {
        for (const auto& table_child : table_column.children) {
            const auto* file_child =
                    find_file_child_for_complex_wrapper(table_child, file_field, _options.mode);
            if (file_child == nullptr) {
                if (!_options.allow_missing_columns) {
                    return Status::InvalidArgument(
                            "Table child column '{}' does not have a matching file child "
                            "under column '{}'",
                            table_child.name, table_column.name);
                }
                ColumnMapping child_mapping;
                child_mapping.table_column_name = table_child.name;
                child_mapping.file_column_name = table_child.name;
                child_mapping.table_type = table_child.type;
                child_mapping.file_type = table_child.type;
                child_mapping.has_complex_projection = !table_child.children.empty();
                child_mapping.filter_conversion = FilterConversionType::FINALIZE_ONLY;
                mapping->child_mappings.push_back(std::move(child_mapping));
                continue;
            }
            ColumnMapping child_mapping;
            child_mapping.table_column_name = table_child.name;
            child_mapping.table_type = table_child.type;
            RETURN_IF_ERROR(_create_direct_mapping(table_child, *file_child, &child_mapping));
            mapping->child_mappings.push_back(std::move(child_mapping));
        }
        if (complex_projection_has_pruned_children(*mapping)) {
            mapping->has_complex_projection = true;
            RETURN_IF_ERROR(build_projected_child_type(mapping->file_type, mapping->child_mappings,
                                                       &mapping->file_type));
            mapping->is_trivial = mapping->table_type != nullptr &&
                                  mapping->table_type->equals(*mapping->file_type);
            mapping->filter_conversion = mapping->is_trivial
                                                 ? FilterConversionType::COPY_DIRECTLY
                                                 : FilterConversionType::READER_EXPRESSION;
        }
    }
    return Status::OK();
}

} // namespace doris::reader
