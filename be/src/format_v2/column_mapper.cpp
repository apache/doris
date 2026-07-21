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

#include "format_v2/column_mapper.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/data_type/convert_field_to_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/short_circuit_evaluation_expr.h"
#include "exprs/vcase_expr.h"
#include "exprs/vcast_expr.h"
#include "exprs/vcondition_expr.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vin_predicate.h"
#include "exprs/vliteral.h"
#include "format_v2/column_mapper_nested.h"
#include "format_v2/expr/cast.h"
#include "format_v2/file_reader.h"
#include "format_v2/schema_projection.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Exprs_types.h"
#include "util/url_coding.h"

namespace doris::format {

namespace {

Status build_initial_default_column(const ColumnDefinition& column, ColumnPtr* value) {
    DORIS_CHECK(value != nullptr);
    *value = nullptr;
    if (!column.initial_default_value.has_value()) {
        return Status::OK();
    }
    const auto nested_type = remove_nullable(column.type);
    Field parsed;
    if (column.initial_default_value_is_base64 ||
        nested_type->get_primitive_type() == TYPE_VARBINARY) {
        std::string decoded;
        if (!base64_decode(*column.initial_default_value, &decoded)) {
            return Status::InvalidArgument("Invalid Base64 Iceberg initial default for field {}",
                                           column.name);
        }
        parsed = nested_type->get_primitive_type() == TYPE_VARBINARY
                         ? Field::create_field<TYPE_VARBINARY>(StringView(decoded))
                         : Field::create_field<TYPE_STRING>(decoded);
        // Variable-width Fields borrow their input. Materialize while decoded is alive so the
        // resulting column owns the payload before it crosses a mapping/literal boundary.
        *value = column.type->create_column_const(1, parsed);
        return Status::OK();
    } else {
        RETURN_IF_ERROR(
                nested_type->get_serde()->from_fe_string(*column.initial_default_value, parsed));
    }
    *value = column.type->create_column_const(1, parsed);
    return Status::OK();
}

Status build_initial_default_literal(const ColumnDefinition& column, VExprContextSPtr* literal) {
    DORIS_CHECK(literal != nullptr);
    ColumnPtr owned_value;
    RETURN_IF_ERROR(build_initial_default_column(column, &owned_value));
    DORIS_CHECK(static_cast<bool>(owned_value));
    Field value;
    owned_value->get(0, value);
    // VLiteral copies the borrowed Field into its own column while owned_value is still alive.
    *literal = VExprContext::create_shared(VLiteral::create_shared(column.type, value));
    return Status::OK();
}

bool has_shared_descendant_field_id(const ColumnDefinition& table, const ColumnDefinition& file) {
    const auto& table_children =
            table.identity_children.empty() ? table.children : table.identity_children;
    for (const auto& table_child : table_children) {
        if (!table_child.has_identifier_field_id()) {
            continue;
        }
        const auto file_child =
                std::ranges::find_if(file.children, [&](const ColumnDefinition& candidate) {
                    return candidate.has_identifier_field_id() &&
                           candidate.get_identifier_field_id() ==
                                   table_child.get_identifier_field_id();
                });
        if (file_child != file.children.end() ||
            std::ranges::any_of(file.children, [&](const ColumnDefinition& candidate) {
                return has_shared_descendant_field_id(table_child, candidate);
            })) {
            return true;
        }
    }
    return false;
}

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

bool column_has_name(const ColumnDefinition& column, const std::string& name) {
    if (to_lower(column.name) == to_lower(name)) {
        return true;
    }
    if (column.has_identifier_name() && to_lower(column.get_identifier_name()) == to_lower(name)) {
        return true;
    }
    return std::ranges::any_of(column.name_mapping, [&](const std::string& alias) {
        return to_lower(alias) == to_lower(name);
    });
}

bool column_names_match(const ColumnDefinition& lhs, const ColumnDefinition& rhs) {
    if (!lhs.has_name_mapping) {
        if (column_has_name(rhs, lhs.name)) {
            return true;
        }
        if (lhs.has_identifier_name() && column_has_name(rhs, lhs.get_identifier_name())) {
            return true;
        }
    }
    // Explicit Iceberg name mapping is authoritative: an empty alias list represents a field that
    // did not exist in the imported file, so only transported aliases may match.
    return std::ranges::any_of(lhs.name_mapping, [&](const std::string& alias) {
        return column_has_name(rhs, alias);
    });
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
        if (!table_column.has_identifier_field_id()) {
            return nullptr;
        }
        const auto field_id = table_column.get_identifier_field_id();
        const auto field_it = std::ranges::find_if(file_schema, [&](const ColumnDefinition& field) {
            return field.has_identifier_field_id() && field.get_identifier_field_id() == field_id;
        });
        return field_it == file_schema.end() ? nullptr : &*field_it;
    }
};

class NameMatcher final : public ColumnMatcher {
public:
    const ColumnDefinition* find(const ColumnDefinition& table_column,
                                 const std::vector<ColumnDefinition>& file_schema) const override {
        const auto field_it = std::ranges::find_if(file_schema, [&](const ColumnDefinition& field) {
            return column_names_match(table_column, field);
        });
        return field_it == file_schema.end() ? nullptr : &*field_it;
    }
};

class PositionMatcher final : public ColumnMatcher {
public:
    const ColumnDefinition* find(const ColumnDefinition& table_column,
                                 const std::vector<ColumnDefinition>& file_schema) const override {
        if (!table_column.has_identifier_field_id()) {
            return nullptr;
        }
        const auto position = table_column.get_identifier_position();
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
    case TableVirtualColumnType::ICEBERG_ROWID:
        return "ICEBERG_ROWID";
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

std::string field_debug_string(const Field& field) {
    std::ostringstream out;
    out << "Field{type=" << type_to_string(field.get_type()) << ", value=";
    switch (field.get_type()) {
    case TYPE_NULL:
        out << "null";
        break;
    case TYPE_INT:
        out << field.get<TYPE_INT>();
        break;
    case TYPE_BIGINT:
        out << field.get<TYPE_BIGINT>();
        break;
    case TYPE_STRING:
        out << field.get<TYPE_STRING>();
        break;
    default:
        out << field.to_debug_string(0);
        break;
    }
    out << "}";
    return out.str();
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

} // namespace

const ColumnDefinition* find_column_by_name(const ColumnDefinition& table_column,
                                            const std::vector<ColumnDefinition>& file_schema) {
    return matcher_for_mode(TableColumnMappingMode::BY_NAME).find(table_column, file_schema);
}

const Field* find_partition_value(const ColumnDefinition& table_column,
                                  const std::map<std::string, Field>& partition_values) {
    const auto find_by_name = [&](const std::string& name) -> const Field* {
        const auto value_it = partition_values.find(name);
        return value_it == partition_values.end() ? nullptr : &value_it->second;
    };
    if (const auto* value = find_by_name(table_column.name); value != nullptr) {
        return value;
    }
    if (table_column.has_identifier_name()) {
        if (const auto* value = find_by_name(table_column.get_identifier_name());
            value != nullptr) {
            return value;
        }
    }
    for (const auto& alias : table_column.name_mapping) {
        if (const auto* value = find_by_name(alias); value != nullptr) {
            return value;
        }
    }
    return nullptr;
}

struct FileSlotRewriteInfo {
    size_t block_position = 0;
    DataTypePtr file_type;
    DataTypePtr table_type;
    std::string file_column_name;
};

struct RewriteContext {
    RuntimeState* runtime_state = nullptr;
    std::vector<VExprSPtr> created_exprs {};

    void add_created_expr(VExprSPtr expr) { created_exprs.push_back(std::move(expr)); }

    Status prepare_created_exprs(VExprContext* context) const {
        DORIS_CHECK(context != nullptr);
        RowDescriptor row_desc;
        for (const auto& expr : created_exprs) {
            if (dynamic_cast<const Cast*>(expr.get()) != nullptr && runtime_state == nullptr) {
                return Status::InvalidArgument(
                        "RuntimeState is required to prepare rewritten cast expression {}",
                        expr->expr_name());
            }
            RETURN_IF_ERROR(expr->prepare(runtime_state, row_desc, context));
        }
        return Status::OK();
    }
};

static VExprSPtr create_file_slot_ref(const VSlotRef& slot_ref,
                                      const FileSlotRewriteInfo& rewrite_info,
                                      RewriteContext* rewrite_context) {
    auto ref =
            VSlotRef::create_shared(slot_ref.slot_id(), cast_set<int>(rewrite_info.block_position),
                                    -1, rewrite_info.file_type, rewrite_info.file_column_name);
    rewrite_context->add_created_expr(ref);
    return ref;
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
        << ", allow_idless_complex_wrapper_projection=" << allow_idless_complex_wrapper_projection
        << "}";
    return out.str();
}

std::string ColumnDefinition::debug_string() const {
    std::ostringstream out;
    out << "ColumnDefinition{name=" << name << ", identifier=" << field_debug_string(identifier)
        << ", name_mapping="
        << join_debug_strings(name_mapping, [](const std::string& name) { return name; })
        << ", has_name_mapping=" << has_name_mapping << ", local_id=" << local_id
        << ", type=" << data_type_debug_string(type) << ", children="
        << join_debug_strings(children,
                              [](const ColumnDefinition& child) { return child.debug_string(); })
        << ", identity_children="
        << join_debug_strings(identity_children,
                              [](const ColumnDefinition& child) { return child.debug_string(); })
        << ", has_default_expr=" << (default_expr != nullptr)
        << ", is_partition_key=" << is_partition_key << ", is_output_slot=" << is_output_slot
        << "}";
    return out.str();
}

std::string LocalColumnIndex::debug_string() const {
    std::ostringstream out;
    out << "LocalColumnIndex{index=" << index << ", project_all_children=" << project_all_children
        << ", children="
        << join_debug_strings(children,
                              [](const LocalColumnIndex& child) { return child.debug_string(); })
        << "}";
    return out.str();
}

std::string ColumnMapping::debug_string() const {
    std::ostringstream out;
    out << "ColumnMapping{global_index=" << global_index
        << ", table_column_name=" << table_column_name << ", file_local_id=";
    if (file_local_id.has_value()) {
        out << *file_local_id;
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
                              [](const ColumnDefinition& child) { return child.debug_string(); })
        << ", file_type=" << data_type_debug_string(file_type)
        << ", table_type=" << data_type_debug_string(table_type)
        << ", has_projection=" << (projection != nullptr) << ", child_mappings="
        << join_debug_strings(child_mappings,
                              [](const ColumnMapping& child) { return child.debug_string(); })
        << ", is_trivial=" << is_trivial << ", is_output_slot=" << is_output_slot
        << ", is_constant=" << constant_index.has_value()
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
        << ", hidden_mappings="
        << join_debug_strings(_hidden_mappings,
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
    const auto rewrite_it = global_to_file_slot.find(slot_ref_global_index(*candidate_slot_ref));
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

static Field literal_field_from_expr(const VExpr& literal_expr) {
    DORIS_CHECK(literal_expr.is_literal());
    const auto* literal = dynamic_cast<const VLiteral*>(&literal_expr);
    DORIS_CHECK(literal != nullptr);
    Field field;
    literal->get_column_ptr()->get(0, field);
    return field;
}

// Table filter localization clones an already-prepared table expr and then rewrites it to file
// slots. Only split-local literals and BE cast nodes need table-reader-specific clone behavior;
// plain slot refs and literals use their own VExpr::clone_node().
static Status clone_table_expr_node(const VExpr& expr, VExprSPtr* cloned_expr) {
    DORIS_CHECK(cloned_expr != nullptr);
    if (const auto* split_literal = dynamic_cast<const SplitLocalFileLiteral*>(&expr)) {
        *cloned_expr = std::make_shared<SplitLocalFileLiteral>(
                split_literal->data_type(), literal_field_from_expr(expr),
                split_literal->original_type(), split_literal->original_field());
    } else if (const auto* vcast_expr = dynamic_cast<const VCastExpr*>(&expr);
               vcast_expr != nullptr && vcast_expr->node_type() == TExprNodeType::CAST_EXPR) {
        *cloned_expr = Cast::create_shared(vcast_expr->data_type());
    }
    return Status::OK();
}

Status clone_table_expr_tree(const VExprSPtr& expr, VExprSPtr* cloned_expr) {
    DORIS_CHECK(cloned_expr != nullptr);
    if (expr == nullptr) {
        *cloned_expr = nullptr;
        return Status::OK();
    }
    return expr->deep_clone(cloned_expr, clone_table_expr_node);
}

static VExprSPtr original_table_literal(const VExprSPtr& literal_expr,
                                        RewriteContext* rewrite_context = nullptr) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto* rewritten_literal = dynamic_cast<const SplitLocalFileLiteral*>(literal_expr.get());
    if (rewritten_literal == nullptr) {
        return literal_expr;
    }
    auto literal = VLiteral::create_shared(rewritten_literal->original_type(),
                                           rewritten_literal->original_field());
    if (rewrite_context != nullptr) {
        rewrite_context->add_created_expr(literal);
    }
    return literal;
}

static ColumnDefinition hidden_column_from_slot_ref(const VSlotRef& slot_ref) {
    ColumnDefinition column;
    column.name = slot_ref.column_name();
    column.identifier = Field::create_field<TYPE_STRING>(column.name);
    column.type = slot_ref.data_type();
    return column;
}

static void collect_top_level_slot_columns(const VExprSPtr& expr,
                                           std::map<GlobalIndex, ColumnDefinition>* columns) {
    DORIS_CHECK(columns != nullptr);
    if (expr == nullptr) {
        return;
    }
    if (expr->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
        columns->try_emplace(slot_ref_global_index(*slot_ref),
                             hidden_column_from_slot_ref(*slot_ref));
        return;
    }
    for (const auto& child : expr->children()) {
        collect_top_level_slot_columns(child, columns);
    }
}

static std::optional<uint8_t> signed_integer_width(PrimitiveType type) {
    switch (type) {
    case TYPE_TINYINT:
        return 8;
    case TYPE_SMALLINT:
        return 16;
    case TYPE_INT:
        return 32;
    case TYPE_BIGINT:
        return 64;
    case TYPE_LARGEINT:
        return 128;
    default:
        return std::nullopt;
    }
}

static std::optional<uint8_t> floating_width(PrimitiveType type) {
    switch (type) {
    case TYPE_FLOAT:
        return 32;
    case TYPE_DOUBLE:
        return 64;
    default:
        return std::nullopt;
    }
}

static std::optional<uint8_t> floating_exact_integer_width(PrimitiveType type) {
    switch (type) {
    case TYPE_FLOAT:
        return 24;
    case TYPE_DOUBLE:
        return 53;
    default:
        return std::nullopt;
    }
}

static bool is_lossless_file_to_table_numeric_cast(const DataTypePtr& file_type,
                                                   const DataTypePtr& table_type) {
    const auto file_nested_type = remove_nullable(file_type);
    const auto table_nested_type = remove_nullable(table_type);
    if (file_nested_type->equals(*table_nested_type)) {
        return true;
    }

    const auto file_primitive_type = file_nested_type->get_primitive_type();
    const auto table_primitive_type = table_nested_type->get_primitive_type();
    if (const auto file_width = signed_integer_width(file_primitive_type)) {
        if (const auto table_width = signed_integer_width(table_primitive_type)) {
            return *table_width >= *file_width;
        }
        if (const auto table_width = floating_exact_integer_width(table_primitive_type)) {
            return *table_width >= *file_width;
        }
        return false;
    }
    if (const auto file_width = floating_width(file_primitive_type)) {
        const auto table_width = floating_width(table_primitive_type);
        return table_width.has_value() && *table_width >= *file_width;
    }
    return false;
}

static VExprSPtr rewrite_literal_to_file_type(const VExprSPtr& literal_expr,
                                              const FileSlotRewriteInfo& rewrite_info,
                                              RewriteContext* rewrite_context) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto original_literal = original_table_literal(literal_expr, rewrite_context);
    const Field original_field = literal_field(original_literal);
    if (rewrite_info.file_type->equals(*original_literal->data_type())) {
        return original_literal;
    }
    // A literal round trip alone cannot prove that file-local evaluation is safe: the file slot
    // itself may lose information when materialized as the table type. For example, DOUBLE 1.5
    // becomes BIGINT 1, so table predicate `value = 1` is true while file predicate
    // `value = 1.0` is false. Complex Field equality also does not compare nested contents.
    // Restrict localization to scalar numeric casts that preserve every file value; unsupported
    // and complex casts keep the table predicate and evaluate after materialization.
    if (!is_lossless_file_to_table_numeric_cast(rewrite_info.file_type,
                                                original_literal->data_type())) {
        return nullptr;
    }
    Field file_field;
    try {
        convert_field_to_type(original_field, *rewrite_info.file_type, &file_field,
                              original_literal->data_type().get());
    } catch (const Exception&) {
        return nullptr;
    }
    if (file_field.is_null()) {
        return nullptr;
    }
    if (file_field.get_type() != remove_nullable(rewrite_info.file_type)->get_primitive_type()) {
        return nullptr;
    }
    Field round_trip_field;
    try {
        convert_field_to_type(file_field, *original_literal->data_type(), &round_trip_field,
                              rewrite_info.file_type.get());
    } catch (const Exception&) {
        return nullptr;
    }
    // The file-to-table type check protects every possible file value. This round trip separately
    // proves that the specific predicate boundary is exactly representable in the file type.
    if (round_trip_field != original_field) {
        return nullptr;
    }
    auto literal = std::make_shared<SplitLocalFileLiteral>(
            rewrite_info.file_type, file_field, original_literal->data_type(), original_field);
    rewrite_context->add_created_expr(literal);
    return literal;
}

static bool rewrite_binary_slot_literal_predicate(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        RewriteContext* rewrite_context) {
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

    auto rewritten_literal =
            rewrite_literal_to_file_type(literal_expr, *rewrite_info, rewrite_context);
    if (rewritten_literal == nullptr) {
        children[literal_child_idx] = original_table_literal(literal_expr, rewrite_context);
        expr->set_children(std::move(children));
        return false;
    }

    children[slot_child_idx] = create_file_slot_ref(*slot_ref, *rewrite_info, rewrite_context);
    children[literal_child_idx] = std::move(rewritten_literal);
    expr->set_children(std::move(children));
    return true;
}

static bool rewrite_in_slot_literal_predicate(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        RewriteContext* rewrite_context) {
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
        auto rewritten_literal =
                rewrite_literal_to_file_type(literal_expr, *rewrite_info, rewrite_context);
        if (rewritten_literal == nullptr) {
            for (size_t restore_idx = 1; restore_idx < children.size(); ++restore_idx) {
                auto restore_literal = unwrap_literal_for_file_cast(children[restore_idx],
                                                                    rewrite_info->table_type);
                if (restore_literal != nullptr) {
                    children[restore_idx] =
                            original_table_literal(restore_literal, rewrite_context);
                }
            }
            expr->set_children(std::move(children));
            return false;
        }
        rewritten_literals.push_back(std::move(rewritten_literal));
    }

    children[0] = create_file_slot_ref(*slot_ref, *rewrite_info, rewrite_context);
    for (size_t literal_idx = 0; literal_idx < rewritten_literals.size(); ++literal_idx) {
        children[literal_idx + 1] = std::move(rewritten_literals[literal_idx]);
    }
    expr->set_children(std::move(children));
    return true;
}

static VExprSPtr create_file_struct_child_name_literal(const std::string& file_child_name,
                                                       RewriteContext* rewrite_context) {
    auto literal = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                           Field::create_field<TYPE_STRING>(file_child_name));
    rewrite_context->add_created_expr(literal);
    return literal;
}

static bool needs_complex_file_slot_cast(const DataTypePtr& file_type,
                                         const DataTypePtr& table_type) {
    if (file_type == nullptr || table_type == nullptr || file_type->equals(*table_type)) {
        return false;
    }
    const auto file_nested_type = remove_nullable(file_type);
    const auto table_nested_type = remove_nullable(table_type);
    if (file_nested_type->equals(*table_nested_type)) {
        return false;
    }
    return is_complex_type(file_nested_type->get_primitive_type()) ||
           is_complex_type(table_nested_type->get_primitive_type());
}

static bool collect_struct_element_chain(const VExprSPtr& expr, std::vector<VExprSPtr>* chain) {
    DORIS_CHECK(chain != nullptr);
    if (!is_struct_element_expr(expr)) {
        return false;
    }
    const auto& parent = expr->children()[0];
    if (is_struct_element_expr(parent)) {
        if (!collect_struct_element_chain(parent, chain)) {
            return false;
        }
    } else if (!parent->is_slot_ref()) {
        // Only support file-local rewrite for struct child chains rooted directly at a top-level
        // slot, for example `element_at(s, 'a')` or `element_at(element_at(s, 'a'), 'b')`.
        //
        // Do not localize computed complex parents such as
        // `element_at(element_at(map_values(m), 1), 'full_name')`. The intermediate map/array
        // result has already been reshaped by scan projection and may have a different child order
        // from the table expression. Partially rewriting that expression against the file block can
        // silently evaluate the wrong struct child and filter out valid rows. Those predicates must
        // remain as table-level conjuncts and be evaluated after TableReader materialization.
        return false;
    }
    chain->push_back(expr);
    return true;
}

static bool can_filter_before_table_nullability_alignment(const DataTypePtr& file_type,
                                                          const DataTypePtr& table_type) {
    DORIS_CHECK(file_type != nullptr);
    DORIS_CHECK(table_type != nullptr);
    // File-local conjuncts run before TableReader validates the materialized table schema. A
    // nullable file value mapped to a required table value must therefore reach
    // _align_column_nullability(). For example, with file STRUCT<a: Nullable(INT)>, table
    // STRUCT<a: BIGINT>, rows [NULL, 20], and `s.a > 10`, filtering in the file domain would drop
    // NULL first and hide the table-contract violation. The reverse direction is safe: a required
    // file value can always be wrapped as a nullable table value after filtering.
    return !file_type->is_nullable() || table_type->is_nullable();
}

static bool rewrite_struct_element_path_to_file_expr(
        const VExprSPtr& expr, const std::vector<ColumnMapping>& mappings,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        RewriteContext* rewrite_context) {
    ResolvedNestedStructPath resolved;
    if (!resolve_nested_struct_expr_for_file(expr, mappings, &resolved)) {
        return false;
    }

    std::vector<VExprSPtr> struct_element_chain;
    if (!collect_struct_element_chain(expr, &struct_element_chain) ||
        struct_element_chain.size() != resolved.file_child_names.size() ||
        struct_element_chain.size() != resolved.file_child_types.size()) {
        return false;
    }

    auto root_children = struct_element_chain.front()->children();
    if (!root_children[0]->is_slot_ref()) {
        return false;
    }
    const auto* slot_ref = assert_cast<const VSlotRef*>(root_children[0].get());
    const auto rewrite_it = global_to_file_slot.find(slot_ref_global_index(*slot_ref));
    if (rewrite_it == global_to_file_slot.end()) {
        return false;
    }

    // Check every value-producing level, including the root struct. A nullable parent also makes
    // a child access nullable even when the child type itself is required, so checking only the
    // final leaf is insufficient. If any file level is more nullable than its table counterpart,
    // keep the complete predicate above TableReader so schema validation observes all NULLs before
    // row filtering.
    if (!can_filter_before_table_nullability_alignment(rewrite_it->second.file_type,
                                                       rewrite_it->second.table_type)) {
        return false;
    }
    for (size_t idx = 0; idx < struct_element_chain.size(); ++idx) {
        if (!can_filter_before_table_nullability_alignment(
                    resolved.file_child_types[idx], struct_element_chain[idx]->data_type())) {
            return false;
        }
    }

    // File-local conjuncts are prepared against the file-reader Block, so both the root slot and
    // every struct selector must be expressed in file schema terms. For a renamed Iceberg field,
    // keeping the table selector would prepare `element_at(file_struct<rename>, 'renamed')` and
    // fail before any rows are read. Rewrite the whole chain while ColumnMapping still preserves
    // the table-to-file relationship. Example:
    //   table filter: element_at(element_at(s, 'renamed_parent'), 'renamed_leaf')
    //   old file:     s<parent<leaf>>
    //   file filter:  element_at(element_at(s, 'parent'), 'leaf')
    root_children[0] = create_file_slot_ref(*slot_ref, rewrite_it->second, rewrite_context);
    struct_element_chain.front()->set_children(std::move(root_children));
    for (size_t idx = 0; idx < struct_element_chain.size(); ++idx) {
        auto children = struct_element_chain[idx]->children();
        children[1] = create_file_struct_child_name_literal(resolved.file_child_names[idx],
                                                            rewrite_context);
        struct_element_chain[idx]->set_children(std::move(children));
        // The selector name and the expression return type must be moved to file schema together.
        // Example:
        //   table filter: element_at(element_at(s, 'new_a'), 'new_aa') = 50
        //   old file:     s.new_a STRUCT<aa, bb>
        //   file filter:  element_at(element_at(s, 'new_a'), 'aa') = 50
        //
        // If the inner element_at keeps the table return type STRUCT<new_aa, bb>, preparing the
        // outer element_at(..., 'aa') fails before scanning because `aa` is not a table field.
        struct_element_chain[idx]->data_type() = resolved.file_child_types[idx];
    }
    return true;
}

static VExprSPtr cast_file_expr_to_table_type(const VExprSPtr& file_expr,
                                              const DataTypePtr& table_type,
                                              RewriteContext* rewrite_context) {
    DORIS_CHECK(file_expr != nullptr);
    DORIS_CHECK(table_type != nullptr);
    DORIS_CHECK(rewrite_context != nullptr);
    auto cast_expr = Cast::create_shared(table_type);
    cast_expr->add_child(file_expr);
    rewrite_context->add_created_expr(cast_expr);
    return cast_expr;
}

// Prefer comparing in the physical file leaf type when a table predicate uses a promoted struct
// child. For example, with table STRUCT<a: BIGINT>, old-file STRUCT<a: INT>, and `s.a = 10`, the
// localized predicate should be `file_s.a::INT = 10::INT`, not
// `CAST(file_s.a::INT AS BIGINT) = 10::BIGINT`. Converting one literal avoids a cast for every row.
//
// This rewrite is valid only when every possible file value survives file-to-table conversion and
// the particular literal survives a table-to-file-to-table round trip. A value such as BIGINT
// 2147483648 cannot be represented by an INT file leaf, so that case deliberately falls back to
// `CAST(file_s.a AS BIGINT) = 2147483648`, which preserves the original table-level semantics.
static bool rewrite_binary_struct_literal_predicate(
        const VExprSPtr& expr, const std::vector<ColumnMapping>& filter_mappings,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        RewriteContext* rewrite_context, bool* can_localize) {
    DORIS_CHECK(can_localize != nullptr);
    if (!is_binary_comparison_predicate(expr)) {
        return false;
    }
    auto children = expr->children();
    int struct_child_idx = -1;
    int literal_child_idx = -1;
    if (is_struct_element_expr(children[0])) {
        struct_child_idx = 0;
        literal_child_idx = 1;
    } else if (is_struct_element_expr(children[1])) {
        struct_child_idx = 1;
        literal_child_idx = 0;
    } else {
        return false;
    }

    const auto table_leaf_type = children[struct_child_idx]->data_type();
    DORIS_CHECK(table_leaf_type != nullptr);
    auto table_literal = unwrap_literal_for_file_cast(children[literal_child_idx], table_leaf_type);
    if (table_literal == nullptr ||
        !rewrite_struct_element_path_to_file_expr(children[struct_child_idx], filter_mappings,
                                                  global_to_file_slot, rewrite_context)) {
        return false;
    }

    const auto file_leaf_type = children[struct_child_idx]->data_type();
    DORIS_CHECK(file_leaf_type != nullptr);
    const FileSlotRewriteInfo leaf_rewrite_info {
            .block_position = 0,
            .file_type = file_leaf_type,
            .table_type = table_leaf_type,
            .file_column_name = {},
    };
    auto file_literal =
            rewrite_literal_to_file_type(table_literal, leaf_rewrite_info, rewrite_context);
    if (file_literal != nullptr) {
        children[literal_child_idx] = std::move(file_literal);
    } else {
        if (!is_lossless_file_to_table_numeric_cast(file_leaf_type, table_leaf_type)) {
            // A narrowing or otherwise lossy cast can fail or produce NULL while TableReader
            // materializes the table schema. Evaluating it here could filter the offending row
            // before that validation, so keep the complete predicate above TableReader.
            *can_localize = false;
            return true;
        }
        children[struct_child_idx] = cast_file_expr_to_table_type(children[struct_child_idx],
                                                                  table_leaf_type, rewrite_context);
        children[literal_child_idx] = original_table_literal(table_literal, rewrite_context);
    }
    expr->set_children(std::move(children));
    return true;
}

// IN must use one comparison type for its probe and every candidate. Rewrite the complete literal
// set only when all values are exactly representable in the file leaf type; one unsafe value makes
// the whole predicate fall back to a table-type cast. For example, an INT file leaf can evaluate
// `BIGINT IN (10, 20)` as `INT IN (10, 20)`, but `BIGINT IN (10, 2147483648)` must stay BIGINT.
static bool rewrite_in_struct_literal_predicate(
        const VExprSPtr& expr, const std::vector<ColumnMapping>& filter_mappings,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        RewriteContext* rewrite_context, bool* can_localize) {
    DORIS_CHECK(can_localize != nullptr);
    if (expr->node_type() != TExprNodeType::IN_PRED || expr->get_num_children() < 2 ||
        !is_struct_element_expr(expr->children()[0])) {
        return false;
    }
    auto children = expr->children();
    const auto table_leaf_type = children[0]->data_type();
    DORIS_CHECK(table_leaf_type != nullptr);
    VExprSPtrs table_literals;
    table_literals.reserve(children.size() - 1);
    for (size_t child_idx = 1; child_idx < children.size(); ++child_idx) {
        auto table_literal = unwrap_literal_for_file_cast(children[child_idx], table_leaf_type);
        if (table_literal == nullptr) {
            return false;
        }
        table_literals.push_back(std::move(table_literal));
    }
    if (!rewrite_struct_element_path_to_file_expr(children[0], filter_mappings, global_to_file_slot,
                                                  rewrite_context)) {
        return false;
    }

    const auto file_leaf_type = children[0]->data_type();
    DORIS_CHECK(file_leaf_type != nullptr);
    const FileSlotRewriteInfo leaf_rewrite_info {
            .block_position = 0,
            .file_type = file_leaf_type,
            .table_type = table_leaf_type,
            .file_column_name = {},
    };
    VExprSPtrs file_literals;
    file_literals.reserve(table_literals.size());
    for (const auto& table_literal : table_literals) {
        auto file_literal =
                rewrite_literal_to_file_type(table_literal, leaf_rewrite_info, rewrite_context);
        if (file_literal == nullptr) {
            if (!is_lossless_file_to_table_numeric_cast(file_leaf_type, table_leaf_type)) {
                *can_localize = false;
                return true;
            }
            children[0] =
                    cast_file_expr_to_table_type(children[0], table_leaf_type, rewrite_context);
            for (size_t literal_idx = 0; literal_idx < table_literals.size(); ++literal_idx) {
                children[literal_idx + 1] =
                        original_table_literal(table_literals[literal_idx], rewrite_context);
            }
            expr->set_children(std::move(children));
            return true;
        }
        file_literals.push_back(std::move(file_literal));
    }

    for (size_t literal_idx = 0; literal_idx < file_literals.size(); ++literal_idx) {
        children[literal_idx + 1] = std::move(file_literals[literal_idx]);
    }
    expr->set_children(std::move(children));
    return true;
}

static VExprSPtr rewrite_struct_or_slot_expr_to_file_expr(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        const std::vector<ColumnMapping>& filter_mappings, RewriteContext* rewrite_context,
        bool* can_localize) {
    if (is_struct_element_expr(expr)) {
        const auto table_leaf_type = expr->data_type();
        if (!rewrite_struct_element_path_to_file_expr(expr, filter_mappings, global_to_file_slot,
                                                      rewrite_context)) {
            // The scanner still evaluates the original table-level conjunct after TableReader
            // finalizes the output block. Skipping an unlocalizable file conjunct is therefore
            // safer than preparing a partially rewritten expression against the wrong struct
            // layout. In particular, do not generate file-local conjuncts for computed complex
            // parents such as `element_at(element_at(map_values(m), 1), 'field')`; only direct
            // slot-rooted struct chains are supported here.
            *can_localize = false;
            return expr;
        }
        DORIS_CHECK(table_leaf_type != nullptr);
        DORIS_CHECK(expr->data_type() != nullptr);
        if (!expr->data_type()->equals(*table_leaf_type)) {
            if (!is_lossless_file_to_table_numeric_cast(expr->data_type(), table_leaf_type)) {
                *can_localize = false;
                return expr;
            }
            // Path localization changes the leaf to the physical file type. For example, after an
            // Iceberg evolution from STRUCT<a: INT> to STRUCT<a: BIGINT>, the localized old-file
            // predicate is initially `element_at(file_col, 'a')::INT = 10::BIGINT`. Cast only the
            // leaf back to BIGINT so the comparison has matching operands without forcing a cast
            // of the entire evolved struct (whose children may also have been added or reordered).
            return cast_file_expr_to_table_type(expr, table_leaf_type, rewrite_context);
        }
        return expr;
    }

    DORIS_CHECK(expr->is_slot_ref());
    const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
    const auto rewrite_it = global_to_file_slot.find(slot_ref_global_index(*slot_ref));
    if (rewrite_it == global_to_file_slot.end()) {
        return expr;
    }
    const auto& rewrite_info = rewrite_it->second;
    auto file_slot = create_file_slot_ref(*slot_ref, rewrite_info, rewrite_context);
    if (rewrite_info.file_type->equals(*rewrite_info.table_type)) {
        return file_slot;
    }
    if (needs_complex_file_slot_cast(rewrite_info.file_type, rewrite_info.table_type)) {
        // Generic file-local expressions cannot safely cast an evolved complex file slot back to
        // the table type. For example, ARRAY_CONTAINS(MAP_KEYS(m), 'person5') only reads map keys,
        // but CAST(file_m AS table_m) first forces an incompatible old value struct into the new
        // layout. Keep such predicates at table level, after TableReader materializes evolution.
        *can_localize = false;
        return expr;
    }
    return cast_file_expr_to_table_type(file_slot, rewrite_info.table_type, rewrite_context);
}

static VExprSPtr rewrite_table_expr_to_file_expr(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        const std::vector<ColumnMapping>& filter_mappings, RewriteContext* rewrite_context,
        bool* can_localize) {
    if (expr == nullptr) {
        return nullptr;
    }
    DORIS_CHECK(rewrite_context != nullptr);
    DORIS_CHECK(can_localize != nullptr);
    if (auto* runtime_filter = dynamic_cast<RuntimeFilterExpr*>(expr.get());
        runtime_filter != nullptr) {
        auto impl = runtime_filter->get_impl();
        if (impl == nullptr) {
            *can_localize = false;
            return expr;
        }
        auto localized_impl = rewrite_table_expr_to_file_expr(
                impl, global_to_file_slot, filter_mappings, rewrite_context, can_localize);
        if (!*can_localize) {
            return expr;
        }
        runtime_filter->set_impl(std::move(localized_impl));
        return expr;
    }
    if (rewrite_binary_slot_literal_predicate(expr, global_to_file_slot, rewrite_context)) {
        return expr;
    }
    if (rewrite_in_slot_literal_predicate(expr, global_to_file_slot, rewrite_context)) {
        return expr;
    }
    if (rewrite_binary_struct_literal_predicate(expr, filter_mappings, global_to_file_slot,
                                                rewrite_context, can_localize)) {
        return expr;
    }
    if (rewrite_in_struct_literal_predicate(expr, filter_mappings, global_to_file_slot,
                                            rewrite_context, can_localize)) {
        return expr;
    }
    if (is_struct_element_expr(expr) || expr->is_slot_ref()) {
        return rewrite_struct_or_slot_expr_to_file_expr(expr, global_to_file_slot, filter_mappings,
                                                        rewrite_context, can_localize);
    }
    // The input is a split-local cloned tree. A previous split-local clone may already have
    // inserted Cast(slot). Keep that rewrite idempotent: rewrite the cast child from table slot to
    // the current split's file slot, and drop the cast when the current split no longer needs it.
    if (is_cast_expr(expr) && expr->get_num_children() == 1) {
        const auto& child = expr->children()[0];
        if (child->is_slot_ref()) {
            const auto* slot_ref = assert_cast<const VSlotRef*>(child.get());
            const auto rewrite_it = global_to_file_slot.find(slot_ref_global_index(*slot_ref));
            if (rewrite_it != global_to_file_slot.end() &&
                expr->data_type()->equals(*rewrite_it->second.table_type)) {
                auto rewritten_child =
                        create_file_slot_ref(*slot_ref, rewrite_it->second, rewrite_context);
                if (rewrite_it->second.file_type->equals(*rewrite_it->second.table_type)) {
                    return rewritten_child;
                }
                if (needs_complex_file_slot_cast(rewrite_it->second.file_type,
                                                 rewrite_it->second.table_type)) {
                    *can_localize = false;
                    return expr;
                }
                expr->set_children({std::move(rewritten_child)});
                return expr;
            }
        }
    }

    VExprSPtrs rewritten_children;
    rewritten_children.reserve(expr->children().size());
    for (const auto& child : expr->children()) {
        rewritten_children.push_back(rewrite_table_expr_to_file_expr(
                child, global_to_file_slot, filter_mappings, rewrite_context, can_localize));
    }
    expr->set_children(std::move(rewritten_children));
    return expr;
}

static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
static constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER = "_last_updated_sequence_number";
static constexpr int32_t ROW_LINEAGE_ROW_ID_FIELD_ID = 2147483540;
static constexpr int32_t ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER_FIELD_ID = 2147483539;

static TableVirtualColumnType row_lineage_virtual_column_type(const std::string& column_name) {
    if (column_name == ROW_LINEAGE_ROW_ID) {
        return TableVirtualColumnType::ROW_ID;
    }
    if (column_name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
        return TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER;
    }
    return TableVirtualColumnType::INVALID;
}

static TableVirtualColumnType row_lineage_virtual_column_type_by_field_id(
        const ColumnDefinition& column) {
    if (!column.has_identifier_field_id()) {
        return TableVirtualColumnType::INVALID;
    }
    switch (column.get_identifier_field_id()) {
    case ROW_LINEAGE_ROW_ID_FIELD_ID:
        return TableVirtualColumnType::ROW_ID;
    case ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER_FIELD_ID:
        return TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER;
    default:
        return TableVirtualColumnType::INVALID;
    }
}

static TableVirtualColumnType row_lineage_virtual_column_type(const ColumnDefinition& column,
                                                              TableColumnMappingMode mode) {
    switch (mode) {
    case TableColumnMappingMode::BY_FIELD_ID:
        return row_lineage_virtual_column_type_by_field_id(column);
    case TableColumnMappingMode::BY_NAME:
    case TableColumnMappingMode::BY_INDEX:
        return row_lineage_virtual_column_type(column.name);
    }
    return TableVirtualColumnType::INVALID;
}

// Returns true when the current file type is not the exact nested type the scan should expose.
// This is about building the projected file-side type/projection, not about whether TableReader
// later needs to rematerialize the complex value back to table layout.
static bool needs_projected_file_type_rebuild(const ColumnMapping& mapping) {
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
        // Rename-only child mappings do not change the file-side projected shape. If field-id
        // matching maps table child `renamed_b` to file child `b`, the file reader can still expose
        // the original file type as long as child count/order/types are unchanged.
        if (!child_mapping.file_local_id.has_value() ||
            needs_projected_file_type_rebuild(child_mapping)) {
            return true;
        }
    }
    return false;
}

static std::optional<size_t> file_child_ordinal_in_scan_type(const ColumnMapping& mapping,
                                                             const ColumnMapping& child_mapping) {
    if (!child_mapping.file_local_id.has_value()) {
        return std::nullopt;
    }
    const auto& file_children = !mapping.projected_file_children.empty()
                                        ? mapping.projected_file_children
                                        : mapping.original_file_children;
    const auto child_it = std::ranges::find_if(file_children, [&](const ColumnDefinition& child) {
        return child.file_local_id() == *child_mapping.file_local_id;
    });
    if (child_it == file_children.end()) {
        return std::nullopt;
    }
    return static_cast<size_t>(std::distance(file_children.begin(), child_it));
}

static bool needs_complex_rematerialize(const ColumnMapping& mapping) {
    if (mapping.child_mappings.empty()) {
        return false;
    }
    if (mapping.table_type == nullptr || mapping.file_type == nullptr ||
        !mapping.table_type->equals(*mapping.file_type)) {
        return true;
    }
    for (size_t table_child_idx = 0; table_child_idx < mapping.child_mappings.size();
         ++table_child_idx) {
        const auto& child_mapping = mapping.child_mappings[table_child_idx];
        const auto file_child_idx = file_child_ordinal_in_scan_type(mapping, child_mapping);
        if (!file_child_idx.has_value() || *file_child_idx != table_child_idx ||
            needs_complex_rematerialize(child_mapping) ||
            (child_mapping.table_type != nullptr && child_mapping.file_type != nullptr &&
             !child_mapping.table_type->equals(*child_mapping.file_type))) {
            return true;
        }
    }
    return false;
}

static bool mapping_can_use_file_column_directly(const ColumnMapping& mapping) {
    if (mapping.table_type == nullptr || mapping.file_type == nullptr) {
        return false;
    }
    const auto table_type = remove_nullable(mapping.table_type);
    const auto file_type = remove_nullable(mapping.file_type);
    const bool same_timestamptz_with_different_scale =
            table_type->get_primitive_type() == TYPE_TIMESTAMPTZ &&
            file_type->get_primitive_type() == TYPE_TIMESTAMPTZ;
    if (!mapping.table_type->equals(*mapping.file_type) && !same_timestamptz_with_different_scale) {
        return false;
    }
    return !needs_complex_rematerialize(mapping);
}

static bool type_contains_varbinary(const DataTypePtr& type) {
    DORIS_CHECK(type != nullptr);
    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_VARBINARY:
        return true;
    case TYPE_ARRAY:
        return type_contains_varbinary(
                assert_cast<const DataTypeArray&>(*nested_type).get_nested_type());
    case TYPE_MAP: {
        const auto& map_type = assert_cast<const DataTypeMap&>(*nested_type);
        return type_contains_varbinary(map_type.get_key_type()) ||
               type_contains_varbinary(map_type.get_value_type());
    }
    case TYPE_STRUCT:
        return std::ranges::any_of(
                assert_cast<const DataTypeStruct&>(*nested_type).get_elements(),
                [](const DataTypePtr& child_type) { return type_contains_varbinary(child_type); });
    default:
        return false;
    }
}

static FilterConversionType direct_filter_conversion(const ColumnMapping& mapping) {
    DORIS_CHECK(mapping.table_type != nullptr);
    DORIS_CHECK(mapping.file_type != nullptr);
    // FileScanOperator deliberately keeps VARBINARY predicates above external readers. Their
    // physical binary representations are not uniformly supported by reader-side expression and
    // metadata filtering, so localizing a late runtime filter here can incorrectly reject rows.
    // Apply the same rule to a complex root because generic array/map/struct expressions rewrite
    // the root slot and can otherwise expose a nested VARBINARY child to the reader.
    if (type_contains_varbinary(mapping.table_type)) {
        return FilterConversionType::FINALIZE_ONLY;
    }
    const auto table_type = remove_nullable(mapping.table_type);
    const auto file_type = remove_nullable(mapping.file_type);
    // TIMESTAMPTZ scale mismatch is intentionally materialized as pass-through: a SQL cast rounds
    // fractional seconds. A file-local cast would therefore filter different instants from the
    // scanner-level predicate evaluated on the pass-through value.
    if (table_type->get_primitive_type() == TYPE_TIMESTAMPTZ &&
        file_type->get_primitive_type() == TYPE_TIMESTAMPTZ &&
        !mapping.table_type->equals(*mapping.file_type)) {
        return FilterConversionType::FINALIZE_ONLY;
    }
    return mapping.is_trivial ? FilterConversionType::COPY_DIRECTLY
                              : FilterConversionType::CAST_FILTER;
}

static FilterConversionType projected_filter_conversion(const ColumnMapping& mapping) {
    const auto conversion = direct_filter_conversion(mapping);
    return !mapping.is_trivial && conversion != FilterConversionType::FINALIZE_ONLY
                   ? FilterConversionType::READER_EXPRESSION
                   : conversion;
}

static const ColumnDefinition* find_file_child_for_mapping(const ColumnDefinition& table_child,
                                                           const ColumnDefinition& file_parent,
                                                           TableColumnMappingMode mode,
                                                           size_t table_child_idx,
                                                           bool allow_ordinal_fallback) {
    const auto file_parent_type = remove_nullable(file_parent.type)->get_primitive_type();
    switch (file_parent_type) {
    case TYPE_ARRAY:
        DORIS_CHECK(file_parent.children.size() == 1);
        return &file_parent.children[0];
    case TYPE_MAP:
        DORIS_CHECK(file_parent.children.size() == 2);
        if (table_child.name == "key") {
            return &file_parent.children[0];
        }
        if (table_child.name == "value") {
            return &file_parent.children[1];
        }
        if (table_child.local_id == 0 || table_child.local_id == 1) {
            return &file_parent.children[table_child.local_id];
        }
        return nullptr;
    default:
        // Hive BY_INDEX is a top-level column matching rule. Once a complex root is selected by
        // file position, nested struct children follow Hive reader's historical name matching
        // semantics; their integer identifiers can be field ids, not file positions.
        const auto nested_mode =
                mode == TableColumnMappingMode::BY_INDEX ? TableColumnMappingMode::BY_NAME : mode;
        if (const auto* file_child =
                    matcher_for_mode(nested_mode).find(table_child, file_parent.children);
            file_child != nullptr) {
            return file_child;
        }
        if (allow_ordinal_fallback && mode == TableColumnMappingMode::BY_FIELD_ID &&
            !table_child.has_identifier_field_id()) {
            // Synthetic children are derived from the table DataType when nested ColumnDefinition
            // metadata has been pruned away. They do not carry Iceberg field ids, so try a name
            // match before falling back to ordinal order. Example:
            //   table value type: Struct(age, full_name, gender)
            //   old file value:   Struct(name, age)
            // Name matching keeps `age -> age`; the later unused-child fallback can then map the
            // renamed `full_name -> name` instead of consuming `age` twice.
            if (const auto* file_child = NameMatcher().find(table_child, file_parent.children);
                file_child != nullptr) {
                return file_child;
            }
        }
        // Some callers only carry the full complex DataType for a projected table column, without
        // expanded nested ColumnDefinitions. In that case we can still preserve full materialization
        // by walking table/file struct fields by ordinal. This is a fallback only: explicit
        // ColumnDefinition children keep using the requested table-format matching rule, which is
        // required for precise schema evolution.
        if (allow_ordinal_fallback && table_child_idx < file_parent.children.size()) {
            return &file_parent.children[table_child_idx];
        }
        return nullptr;
    }
}

static ColumnDefinition synthetic_child_definition(const std::string& name, DataTypePtr type,
                                                   int32_t local_id) {
    ColumnDefinition child;
    child.identifier = Field::create_field<TYPE_STRING>(name);
    child.local_id = local_id;
    child.name = name;
    child.type = std::move(type);
    return child;
}

static std::vector<ColumnDefinition> synthesize_complex_children_from_type(
        const DataTypePtr& type) {
    std::vector<ColumnDefinition> children;
    if (type == nullptr) {
        return children;
    }
    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_ARRAY: {
        const auto* array_type = assert_cast<const DataTypeArray*>(nested_type.get());
        children.push_back(synthetic_child_definition("element", array_type->get_nested_type(), 0));
        break;
    }
    case TYPE_MAP: {
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        children.push_back(synthetic_child_definition("key", map_type->get_key_type(), 0));
        children.push_back(synthetic_child_definition("value", map_type->get_value_type(), 1));
        break;
    }
    case TYPE_STRUCT: {
        const auto* struct_type = assert_cast<const DataTypeStruct*>(nested_type.get());
        children.reserve(struct_type->get_elements().size());
        for (size_t idx = 0; idx < struct_type->get_elements().size(); ++idx) {
            children.push_back(synthetic_child_definition(struct_type->get_element_name(idx),
                                                          struct_type->get_element(idx),
                                                          cast_set<int32_t>(idx)));
        }
        break;
    }
    default:
        break;
    }
    return children;
}

static void align_struct_child_types_with_parent(const DataTypePtr& parent_type,
                                                 std::vector<ColumnDefinition>& children) {
    const auto nested_parent_type = remove_nullable(parent_type);
    DORIS_CHECK(nested_parent_type->get_primitive_type() == TYPE_STRUCT);
    const auto type_children = synthesize_complex_children_from_type(parent_type);
    for (auto& child : children) {
        const auto type_child = std::ranges::find_if(
                type_children, [&](const auto& candidate) { return candidate.name == child.name; });
        DORIS_CHECK(type_child != type_children.end())
                << "Complex child '" << child.name
                << "' is absent from its parent table type: " << parent_type->get_name();
        // The parent DataType is the authoritative output contract. Nested schema descriptors can
        // omit child nullability even though the parent struct still declares Nullable(String).
        // For example, the Iceberg full-schema-change case maps nullable `location` to `city`, but
        // its child descriptor carries String. Keeping String here makes rematerialization strip
        // the child's null map and creates Struct(String) under a Struct(Nullable(String)) type.
        child.type = type_child->type;
    }
}

static bool has_table_child_named(const std::vector<ColumnDefinition>& children,
                                  std::string_view name) {
    return std::ranges::any_of(children, [&](const ColumnDefinition& child) {
        return std::string_view(child.name) == name;
    });
}

static void complete_required_complex_children_from_type(const DataTypePtr& type,
                                                         std::vector<ColumnDefinition>& children) {
    if (type == nullptr) {
        return;
    }
    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_MAP: {
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        // MAP key/value are structural children, not independently materializable table fields.
        // A key-only projection can still be attached to a whole-map output root, for example:
        //   SELECT * FROM t WHERE ARRAY_CONTAINS(MAP_KEYS(new_map_column), 'person5')
        //
        // In that shape the scanner keeps the value stream readable, but the table projection can
        // carry only the key child. Add the missing value child so recursive mapping can evolve the
        // value type instead of letting TableReader cast old/new value structs directly.
        if (has_table_child_named(children, "key") && !has_table_child_named(children, "value")) {
            children.push_back(synthetic_child_definition("value", map_type->get_value_type(), 1));
        }
        break;
    }
    case TYPE_ARRAY:
        // ARRAY has only one required structural child (`element`), so a non-empty projection is
        // already rooted at the element path.
        break;
    case TYPE_STRUCT:
        // STRUCT children are real fields and must remain prunable. Completing missing struct
        // fields here would turn `SELECT s.a` into a full-struct read and undo nested projection.
        break;
    default:
        break;
    }
}

struct PreparedTableChildren {
    std::vector<ColumnDefinition> children;
    bool synthesized_from_type = false;
};

static PreparedTableChildren prepare_table_children_for_mapping(
        const ColumnDefinition& table_column, const DataTypePtr& file_type) {
    PreparedTableChildren prepared {.children = table_column.children};
    const auto nested_table_type = remove_nullable(table_column.type);

    // Some scan paths, especially SELECT *, only carry the complete complex DataType for a table
    // column and leave ColumnDefinition::children empty. Synthesize the hierarchy so recursive
    // mapping can evolve nested fields instead of falling back to an invalid whole-column cast.
    prepared.synthesized_from_type = prepared.children.empty() &&
                                     is_complex_type(nested_table_type->get_primitive_type()) &&
                                     !table_column.type->equals(*file_type);
    if (prepared.synthesized_from_type) {
        prepared.children = synthesize_complex_children_from_type(table_column.type);
    } else if (!prepared.children.empty() && !table_column.type->equals(*file_type)) {
        complete_required_complex_children_from_type(table_column.type, prepared.children);
    }

    if (!prepared.children.empty() && nested_table_type->get_primitive_type() == TYPE_STRUCT) {
        // Struct children are table fields, so the parent Struct type is authoritative for their
        // nullability. ARRAY and MAP children are format-level structural wrappers and keep the
        // descriptor types used by their recursive mappings.
        align_struct_child_types_with_parent(table_column.type, prepared.children);
    }
    return prepared;
}

static Status validate_file_schema_children(const ColumnDefinition& file_field) {
    if (file_field.type == nullptr) {
        return Status::InternalError("File column '{}' has null type", file_field.name);
    }
    const auto nested_type = remove_nullable(file_field.type);
    size_t expected_children = 0;
    bool complex_with_fixed_children = true;
    switch (nested_type->get_primitive_type()) {
    case TYPE_ARRAY:
        expected_children = 1;
        break;
    case TYPE_MAP:
        expected_children = 2;
        break;
    case TYPE_STRUCT:
        expected_children =
                assert_cast<const DataTypeStruct*>(nested_type.get())->get_elements().size();
        break;
    default:
        complex_with_fixed_children = false;
        break;
    }
    if (!complex_with_fixed_children || file_field.children.size() == expected_children) {
        return Status::OK();
    }
    return Status::InternalError(
            "Malformed complex file schema for column '{}': type={}, expected_children={}, "
            "actual_children={}",
            file_field.name, file_field.type->get_name(), expected_children,
            file_field.children.size());
}

static bool has_projected_file_children(const ColumnMapping& mapping) {
    if (mapping.original_file_children.empty() || mapping.projected_file_children.empty()) {
        return false;
    }
    if (mapping.original_file_children.size() != mapping.projected_file_children.size()) {
        return true;
    }
    for (size_t idx = 0; idx < mapping.original_file_children.size(); ++idx) {
        if (mapping.original_file_children[idx].file_local_id() !=
            mapping.projected_file_children[idx].file_local_id()) {
            return true;
        }
    }
    return false;
}

static bool needs_nested_file_projection(const ColumnMapping& mapping) {
    if (has_projected_file_children(mapping)) {
        // Return True if the projected child column is missing / re-ordered
        return true;
    }
    return std::ranges::any_of(mapping.child_mappings, [](const ColumnMapping& child_mapping) {
        return needs_nested_file_projection(child_mapping);
    });
}

static Status build_complex_projection(const ColumnMapping& mapping, LocalColumnIndex* projection);

// Build the projected file children/type according to the pruned complex projection. For example,
// if we have a struct column `s` with children `id` and `name`, and the projection only keeps
// `s.name`, then the file reader should expose `STRUCT<name ...>`.
static Status rebuild_projected_file_children_and_type(
        const DataTypePtr& file_type, const std::vector<ColumnDefinition>& original_file_children,
        const std::vector<ColumnMapping>& child_mappings,
        std::vector<ColumnDefinition>* projected_file_children, DataTypePtr* projected_type) {
    DORIS_CHECK(file_type != nullptr);
    DORIS_CHECK(projected_file_children != nullptr);
    DORIS_CHECK(projected_type != nullptr);
    ColumnDefinition field;
    field.type = file_type;
    field.children = original_file_children;
    LocalColumnIndex projection = LocalColumnIndex::partial_local(-1);
    projection.children.reserve(child_mappings.size());
    for (const auto* child_mapping : present_child_mappings_in_file_order(child_mappings)) {
        DORIS_CHECK(child_mapping->file_local_id.has_value());
        LocalColumnIndex child_projection;
        RETURN_IF_ERROR(build_complex_projection(*child_mapping, &child_projection));
        projection.children.push_back(std::move(child_projection));
    }

    ColumnDefinition projected_field;
    RETURN_IF_ERROR(project_column_definition(field, projection, &projected_field));
    *projected_file_children = std::move(projected_field.children);
    *projected_type = std::move(projected_field.type);
    return Status::OK();
}

// Build the complex column projection according to the ColumnMapping which is re-ordered by the
// file-schema's order.
//
// For MAP, a partial projection represents value-subtree pruning only. The key child is not a
// projected output shape; file readers still read full keys to construct ColumnMap offsets and keep
// key semantics unchanged. If a caller tries to project only/prune the key child, the common schema
// projection helper rejects it.
static Status build_complex_projection(const ColumnMapping& mapping, LocalColumnIndex* projection) {
    if (projection == nullptr) {
        return Status::InvalidArgument("projection is null");
    }
    DORIS_CHECK(mapping.file_local_id.has_value());
    *projection = LocalColumnIndex::local(*mapping.file_local_id);
    projection->project_all_children = mapping.child_mappings.empty();
    projection->children.clear();
    const auto present_children = present_child_mappings_in_file_order(mapping.child_mappings);
    if (!projection->project_all_children && present_children.empty()) {
        // All requested table children under this complex node are missing/default-only. The file
        // reader cannot expose an empty complex projection, but TableReader can still rematerialize
        // the table shape from a full file subtree and fill the missing children with defaults.
        projection->project_all_children = true;
        return Status::OK();
    }
    for (const auto* child_mapping : present_children) {
        LocalColumnIndex child_projection;
        RETURN_IF_ERROR(build_complex_projection(*child_mapping, &child_projection));
        projection->children.push_back(std::move(child_projection));
    }
    if (!projection->project_all_children && projection->children.empty()) {
        return Status::NotSupported("Projection for complex column {} contains no file children",
                                    mapping.file_column_name);
    }
    return Status::OK();
}

using FilterProjectionMap = std::map<LocalColumnId, LocalColumnIndex>;

// Update the mapping's file type according to the projection, and determine whether the projection
// is trivial (i.e. the projected file type is the same as the table type, so no need to
// rematerialize the complex value back to table layout after reading from file).
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
    ColumnDefinition field;
    field.type = mapping->original_file_type;
    field.children = mapping->original_file_children;
    ColumnDefinition projected_field;
    RETURN_IF_ERROR(project_column_definition(field, projection, &projected_field));
    mapping->file_type = std::move(projected_field.type);
    mapping->projected_file_children = std::move(projected_field.children);
    mapping->is_trivial = mapping_can_use_file_column_directly(*mapping);
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
    // Merge predicate-only nested paths into the root projection that is about to be scanned.
    // Example: `SELECT s.a WHERE s.b > 1` first builds the output projection `s -> a` from
    // ColumnMapping, while build_nested_struct_filter_projection_map() records `s -> b`. This merge
    // produces one file scan projection `s -> a,b`.
    RETURN_IF_ERROR(merge_local_column_index(projection, filter_projection_it->second));
    return Status::OK();
}

static bool table_root_is_map(const ColumnMapping& mapping) {
    if (mapping.table_type == nullptr) {
        return false;
    }
    return remove_nullable(mapping.table_type)->get_primitive_type() == TYPE_MAP;
}

static Status add_scan_column(FileScanRequest* file_request, ColumnMapping* mapping,
                              bool is_predicate_column, bool force_full_complex_scan_projection,
                              const FilterProjectionMap* filter_projections = nullptr) {
    const auto file_column_id = LocalColumnId(mapping->file_local_id.value());
    LocalColumnIndex projection = LocalColumnIndex::top_level(file_column_id);
    // Columnar readers can turn a complex mapping into a nested file projection, but
    // row-oriented readers must scan the full top-level complex field because all children are
    // encoded in the same text cell.
    if (!force_full_complex_scan_projection && needs_nested_file_projection(*mapping)) {
        RETURN_IF_ERROR(build_complex_projection(*mapping, &projection));
    }
    if (is_predicate_column && !force_full_complex_scan_projection) {
        DCHECK(filter_projections != nullptr);
        // If a projected complex root is also used by a predicate, rebuild the predicate scan
        // projection from the output mapping before merging predicate-only children. For
        // `SELECT s.a WHERE s.b > 1`, build_complex_projection() produces `s -> a` and
        // merge_filter_projection() adds `s -> b`, so the predicate column reads both children.
        RETURN_IF_ERROR(merge_filter_projection(filter_projections, &projection));
    }
    FileScanRequestBuilder builder(file_request);
    if (is_predicate_column) {
        return builder.add_predicate_column(std::move(projection));
    }
    return builder.add_non_predicate_column(std::move(projection));
}

static const LocalColumnIndex* find_scan_projection(
        const std::vector<LocalColumnIndex>& scan_columns, LocalColumnId file_column_id) {
    const auto projection_it =
            std::ranges::find_if(scan_columns, [&](const LocalColumnIndex& projection) {
                return projection.column_id() == file_column_id;
            });
    return projection_it == scan_columns.end() ? nullptr : &*projection_it;
}

// Apply the final scan projection of one root file column back to its ColumnMapping. This updates
// mapping.file_type/projected_file_children from the original file schema to the exact shape that
// FileReader will return.
//
// Example: for `SELECT s.a WHERE s.b > 1`, add_scan_column() keeps only one predicate scan
// projection `s -> a,b`. Applying that projection changes the mapping's file type from the full
// file struct `s<a,b,c>` to the projected file struct `s<a,b>`, so later filter rewrite and
// TableReader final materialization use the same column shape as the file-local block.
static Status apply_scan_projection_to_mapping_file_type(const FileScanRequest& file_request,
                                                         ColumnMapping* mapping) {
    DORIS_CHECK(mapping != nullptr);
    DORIS_CHECK(mapping->file_local_id.has_value());
    const auto file_column_id = LocalColumnId(*mapping->file_local_id);
    // Predicate columns are the actual scan projection when a column is used by row-level filters:
    // add_scan_column() removes the duplicate non-predicate projection in that case.
    const auto* projection = find_scan_projection(file_request.predicate_columns, file_column_id);
    if (projection == nullptr) {
        projection = find_scan_projection(file_request.non_predicate_columns, file_column_id);
    }
    DORIS_CHECK(projection != nullptr);
    return apply_projection_to_mapping_file_type(*projection, mapping);
}

// Build extra scan projections required only by row-level filters on nested struct children.
//
// Example: for `SELECT s.a FROM t WHERE s.b.c > 1`, the output projection may only contain `s.a`,
// but the file reader must also read `s.b.c` to evaluate the predicate. This function collects the
// table-side filter path, resolves it through ColumnMapping first, and records the corresponding
// file-side projection in filter_projections. This keeps renamed fields consistent between the scan
// projection and row-level conjunct rewrite. Example:
//   table filter path: s -> renamed_b -> c
//   old file path:     s -> b -> c
//   recorded path:     s -> b -> c
// When add_scan_column() adds the same root as a predicate column, it rebuilds that root from the
// output mapping, merges this filter-only projection into it, and removes the duplicate
// non-predicate root entry.
static Status build_nested_struct_filter_projection_map(
        const std::vector<TableFilter>& table_filters, const std::vector<ColumnMapping>& mappings,
        FilterProjectionMap* filter_projections) {
    DORIS_CHECK(filter_projections != nullptr);
    filter_projections->clear();
    for (const auto& table_filter : table_filters) {
        if (table_filter.conjunct == nullptr) {
            continue;
        }
        // Collect all nested struct paths in the table filter. For example, for
        // `s.id > 5 AND element_at(s, 'renamed_name') = 'abc'`, collect the table paths
        // `s -> id` and `s -> renamed_name`, then resolve each one to its file-side projection.
        std::vector<NestedStructPath> paths;
        collect_nested_struct_paths(table_filter.conjunct->root(), &paths);
        for (const auto& path : paths) {
            auto mapping_it = std::ranges::find_if(mappings, [&](const ColumnMapping& mapping) {
                return mapping.global_index == path.root_global_index;
            });
            if (mapping_it == mappings.end() || !mapping_it->file_local_id.has_value() ||
                path.selectors.empty()) {
                continue;
            }

            ResolvedNestedStructPath resolved;
            LocalColumnIndex root_projection;
            if (!resolve_nested_struct_path_for_file(path, mappings, &resolved)) {
                if (!table_root_is_map(*mapping_it)) {
                    continue;
                }
                // Direct map value filters such as `m.value.a > 1` need the value leaf for row
                // evaluation even when the query only projects another value child. This is only a
                // scan projection fallback; complex map/array expressions are still not rewritten
                // into file-local conjuncts.
                LocalColumnIndex child_projection;
                RETURN_IF_ERROR(build_file_child_projection_from_schema(
                        mapping_it->original_file_children, path.selectors, &child_projection));
                if (child_projection.local_id() < 0) {
                    continue;
                }
                root_projection = LocalColumnIndex::partial_local(*mapping_it->file_local_id);
                root_projection.children.push_back(std::move(child_projection));
            } else {
                root_projection = std::move(resolved.file_projection);
            }
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
    DORIS_CHECK(mapping->file_local_id.has_value());
    if (mapping->is_trivial || needs_complex_rematerialize(*mapping)) {
        mapping->projection = VExprContext::create_shared(VSlotRef::create_shared(
                cast_set<int>(block_position.value()), cast_set<int>(block_position.value()), -1,
                mapping->file_type, mapping->file_column_name));
        return;
    }

    auto expr = Cast::create_shared(mapping->table_type);
    expr->add_child(VSlotRef::create_shared(cast_set<int>(block_position.value()),
                                            cast_set<int>(block_position.value()), -1,
                                            mapping->file_type, mapping->file_column_name));
    mapping->projection = VExprContext::create_shared(expr);
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
        DORIS_CHECK(mapping.file_local_id.has_value());
        global_to_file_slot.emplace(
                mapping.global_index,
                FileSlotRewriteInfo {.block_position = entry_it->second.local_index().value(),
                                     .file_type = mapping.file_type,
                                     .table_type = mapping.table_type,
                                     .file_column_name = mapping.file_column_name});
    }
    return global_to_file_slot;
}

Status TableColumnMapper::_create_by_index_mapping(const ColumnDefinition& table_column,
                                                   const std::vector<ColumnDefinition>& file_schema,
                                                   ColumnMapping* mapping) {
    DORIS_CHECK(mapping != nullptr);
    DORIS_CHECK(!table_column.is_partition_key);

    // Key contract: in BY_INDEX mode, `ColumnDefinition::identifier` TYPE_INT is interpreted as the
    // 0-based position of this column inside `file_schema`. FE writes the physical file position
    // of each non-partition projected column into that identifier. This interpretation allows:
    //   - sparse projection: read only a subset of file columns (for example only `_col2`
    //     and `_col4`);
    //   - column reordering: table column order differs from file column order;
    //   - no many-to-one mapping: FE must guarantee that each file position is referenced by at
    //     most one table column.
    const auto file_index = table_column.get_identifier_position();

    // Case A: file_index is in range, so build a direct positional mapping.
    // The file column name (for example `_col0`) is intentionally ignored here.
    if (file_index >= 0 && static_cast<size_t>(file_index) < file_schema.size()) {
        return _create_direct_mapping(table_column, file_schema[static_cast<size_t>(file_index)],
                                      mapping);
    }

    // Case B: file_index is out of range, which means the file does not contain this column.
    // Route it through the missing-column path used by schema evolution.
    if (table_column.default_expr != nullptr) {
        _set_constant_mapping(mapping, table_column.default_expr);
        return Status::OK();
    }
    // Keep the mapping empty (`file_local_id` remains `nullopt`) and let the upper finalize
    // stage fill NULL/default values.
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

Status TableColumnMapper::_create_mapping_for_column(const ColumnDefinition& table_column,
                                                     GlobalIndex global_index,
                                                     ColumnMapping* mapping) {
    DORIS_CHECK(mapping != nullptr);
    *mapping = ColumnMapping {};
    mapping->global_index = global_index;
    mapping->table_column_name = table_column.name;
    mapping->table_type = table_column.type;
    const auto row_lineage_type = row_lineage_virtual_column_type(table_column, _options.mode);
    if (const auto* partition_value = find_partition_value(table_column, _partition_values);
        table_column.is_partition_key && partition_value != nullptr) {
        // Partition values are split constants and must take precedence over defaults.
        _set_constant_mapping(mapping, VExprContext::create_shared(VLiteral::create_shared(
                                               mapping->table_type, *partition_value)));
    } else if (_options.mode == TableColumnMappingMode::BY_INDEX &&
               !table_column.is_partition_key && table_column.has_identifier_field_id()) {
        // BY_INDEX interprets ColumnDefinition::identifier as physical file position.
        RETURN_IF_ERROR(_create_by_index_mapping(table_column, _file_schema, mapping));
    } else if (const auto* file_field = _find_file_field(table_column, _file_schema)) {
        // Normal physical file column mapping.
        RETURN_IF_ERROR(_create_direct_mapping(table_column, *file_field, mapping));
        if (row_lineage_type != TableVirtualColumnType::INVALID) {
            // Iceberg v3 rewritten files may physically contain row lineage metadata fields.
            // File non-null values must be preserved, while file NULLs still inherit from data file
            // metadata in IcebergTableReader. Therefore the mapping has a real file source plus a
            // virtual post-materialization step, and filters must wait for finalize output.
            mapping->virtual_column_type = row_lineage_type;
            mapping->filter_conversion = FilterConversionType::FINALIZE_ONLY;
        }
    } else if (row_lineage_type != TableVirtualColumnType::INVALID) {
        // Iceberg row lineage metadata fields are optional in data files. Missing fields are exposed
        // as all-NULL table columns first; IcebergTableReader fills inherited values only when the
        // split carries first_row_id / last_updated_sequence_number metadata.
        // FE may attach a default_expr to these hidden metadata columns, but the Iceberg v3
        // inheritance rule must take precedence over the generic missing-column default path.
        mapping->virtual_column_type = row_lineage_type;
    } else if (table_column.name == BeConsts::ICEBERG_ROWID_COL) {
        // Doris internal Iceberg row locator is never a physical Iceberg data column. It is built
        // from file path, row position and partition metadata for delete/update/merge.
        mapping->virtual_column_type = TableVirtualColumnType::ICEBERG_ROWID;
    } else if (table_column.initial_default_value.has_value()) {
        VExprContextSPtr initial_default;
        RETURN_IF_ERROR(build_initial_default_literal(table_column, &initial_default));
        // Iceberg metadata is the authoritative logical value for files written before the field
        // existed; the generic FE expression may still contain its Base64 transport text.
        _set_constant_mapping(mapping, std::move(initial_default));
    } else if (table_column.default_expr != nullptr) {
        // Missing schema-evolution column with an explicit default expression.
        _set_constant_mapping(mapping, table_column.default_expr);
    } else {
        if (table_column.is_partition_key) {
            return Status::InvalidArgument(
                    "Table column '{}' (global_index={}) does not have a matching partition value",
                    table_column.name, mapping->global_index.value());
        }
    }
    return Status::OK();
}

Status TableColumnMapper::_create_hidden_filter_mapping(const ColumnDefinition& table_column,
                                                        GlobalIndex global_index,
                                                        ColumnMapping* mapping) {
    auto status = _create_mapping_for_column(table_column, global_index, mapping);
    if (mapping->file_local_id.has_value() || mapping->constant_index.has_value() ||
        mapping->virtual_column_type != TableVirtualColumnType::INVALID) {
        return Status::OK();
    }
    if (_options.mode == TableColumnMappingMode::BY_NAME) {
        return status;
    }

    // Predicate-only slot refs carry the table name/type but do not carry the table-format field
    // id used by BY_FIELD_ID or the file position used by BY_INDEX. Use a name fallback only for
    // hidden filter localization; projected columns still obey the requested mapping mode.
    const auto* file_field =
            matcher_for_mode(TableColumnMappingMode::BY_NAME).find(table_column, _file_schema);
    if (file_field == nullptr) {
        return status;
    }
    ColumnMapping fallback_mapping;
    fallback_mapping.global_index = global_index;
    fallback_mapping.table_column_name = table_column.name;
    fallback_mapping.table_type = table_column.type;
    RETURN_IF_ERROR(_create_direct_mapping(table_column, *file_field, &fallback_mapping));
    *mapping = std::move(fallback_mapping);
    return Status::OK();
}

Status TableColumnMapper::_build_hidden_filter_mappings(
        const std::vector<TableFilter>& table_filters) {
    _hidden_mappings.clear();

    std::map<GlobalIndex, ColumnDefinition> filter_columns;
    for (const auto& table_filter : table_filters) {
        if (table_filter.conjunct != nullptr) {
            collect_top_level_slot_columns(table_filter.conjunct->root(), &filter_columns);
        }
    }

    for (const auto& [global_index, table_column] : filter_columns) {
        if (_find_mapping(global_index) != nullptr) {
            // Ignore columns that are already mapped by the projected columns
            continue;
        }
        ColumnMapping mapping;
        RETURN_IF_ERROR(_create_hidden_filter_mapping(table_column, global_index, &mapping));
        if (mapping.file_local_id.has_value() || mapping.constant_index.has_value() ||
            mapping.virtual_column_type != TableVirtualColumnType::INVALID) {
            _hidden_mappings.push_back(std::move(mapping));
        }
    }
    return Status::OK();
}

Status TableColumnMapper::create_mapping(const std::vector<ColumnDefinition>& projected_columns,
                                         const std::map<std::string, Field>& partition_values,
                                         const std::vector<ColumnDefinition>& file_schema) {
    clear();
    _partition_values = partition_values;
    _file_schema = file_schema;
    for (size_t column_idx = 0; column_idx < projected_columns.size(); ++column_idx) {
        ColumnMapping mapping;
        RETURN_IF_ERROR(_create_mapping_for_column(projected_columns[column_idx],
                                                   GlobalIndex(column_idx), &mapping));
        mapping.is_output_slot = projected_columns[column_idx].is_output_slot;
        _mappings.push_back(std::move(mapping));
    }
    return Status::OK();
}

std::vector<ColumnMapping> TableColumnMapper::_filter_visible_mappings() const {
    std::vector<ColumnMapping> mappings;
    mappings.reserve(_mappings.size() + _hidden_mappings.size());
    mappings.insert(mappings.end(), _mappings.begin(), _mappings.end());
    mappings.insert(mappings.end(), _hidden_mappings.begin(), _hidden_mappings.end());
    return mappings;
}

Status TableColumnMapper::_build_filter_entries(const FileScanRequest& file_request) {
    _filter_entries.clear();
    const auto mappings = _filter_visible_mappings();
    for (const auto& mapping : mappings) {
        FilterEntry entry;
        if (mapping.constant_index.has_value()) {
            entry = FilterEntry::constant(*mapping.constant_index);
        } else if (mapping.file_local_id.has_value() &&
                   filter_conversion_has_local_source(mapping.filter_conversion)) {
            const auto local_position_it =
                    file_request.local_positions.find(LocalColumnId(*mapping.file_local_id));
            if (local_position_it != file_request.local_positions.end()) {
                entry = FilterEntry::local(local_position_it->second);
            }
        }
        _filter_entries.emplace(mapping.global_index, entry);
    }
    return Status::OK();
}

Status TableColumnMapper::create_scan_request(
        const std::vector<TableFilter>& table_filters,
        const std::vector<ColumnDefinition>& projected_columns, FileScanRequest* file_request,
        RuntimeState* runtime_state) {
    // FileReader evaluates expressions against a file-local block. This mapper owns the
    // table-column to file-column conversion, so it also owns the file-local block positions.
    file_request->predicate_columns.clear();
    file_request->non_predicate_columns.clear();
    file_request->predicate_only_columns.clear();
    file_request->local_positions.clear();
    file_request->conjuncts.clear();
    file_request->delete_conjuncts.clear();
    _filter_entries.clear();
    // 1. Build referenced non-predicate columns
    for (size_t column_idx = 0; column_idx < projected_columns.size(); ++column_idx) {
        const auto global_index = GlobalIndex(column_idx);
        auto* mapping = _find_mapping(global_index);
        if (mapping != nullptr && mapping->file_local_id.has_value()) {
            // A file column can be read lazily as a non-predicate column only when it is not used
            // by row-level expression filters.
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
            if (!used_by_filter || !enable_lazy_materialization()) {
                RETURN_IF_ERROR(add_scan_column(file_request, mapping, false,
                                                force_full_complex_scan_projection()));
            }
        }
    }
    // 2. Build referenced predicate columns
    // Hidden filter mappings must be built before localizing filters, so that they can be localized together with visible mappings and referenced by localized filter expressions.
    RETURN_IF_ERROR(_build_hidden_filter_mappings(table_filters));
    RETURN_IF_ERROR(localize_filters(table_filters, file_request, runtime_state));
    for (const auto& predicate_column : file_request->predicate_columns) {
        const auto local_id = predicate_column.column_id();
        const bool is_output =
                std::ranges::any_of(_mappings, [local_id](const ColumnMapping& mapping) {
                    return mapping.is_output_slot && mapping.file_local_id.has_value() &&
                           LocalColumnId(*mapping.file_local_id) == local_id;
                });
        if (!is_output && !file_request->is_predicate_only(local_id)) {
            file_request->predicate_only_columns.push_back(local_id);
        }
    }
    // 3. Rebuild output projection expressions for projected columns. localize_filters() has
    // already applied the final scan projection to mapping.file_type/projected_file_children before
    // rewriting filter expressions.
    for (auto& mapping : _mappings) {
        if (!mapping.file_local_id.has_value()) {
            continue;
        }
        auto position_it =
                file_request->local_positions.find(LocalColumnId(*mapping.file_local_id));
        DORIS_CHECK(position_it != file_request->local_positions.end())
                << file_request->local_positions.size() << " " << *mapping.file_local_id << " "
                << mapping.file_column_name;
        rebuild_projection(&mapping, position_it->second);
    }
    return Status::OK();
}

ColumnMapping* TableColumnMapper::_find_mapping(GlobalIndex global_index) {
    for (auto& mapping : _mappings) {
        if (mapping.global_index == global_index) {
            return &mapping;
        }
    }
    return nullptr;
}

ColumnMapping* TableColumnMapper::_find_filter_mapping(GlobalIndex global_index) {
    if (auto* mapping = _find_mapping(global_index); mapping != nullptr) {
        return mapping;
    }
    for (auto& mapping : _hidden_mappings) {
        if (mapping.global_index == global_index) {
            return &mapping;
        }
    }
    return nullptr;
}

Status TableColumnMapper::localize_filters(const std::vector<TableFilter>& table_filters,
                                           FileScanRequest* file_request,
                                           RuntimeState* runtime_state) {
    std::set<LocalColumnId> localized_predicate_columns;
    FilterProjectionMap filter_projections;
    auto filter_mappings = _filter_visible_mappings();
    RETURN_IF_ERROR(build_nested_struct_filter_projection_map(table_filters, filter_mappings,
                                                              &filter_projections));
    for (const auto& table_filter : table_filters) {
        for (const auto& global_index : table_filter.global_indices) {
            auto* mapping = _find_filter_mapping(global_index);
            if (mapping == nullptr || !mapping->file_local_id.has_value() ||
                !filter_conversion_has_local_source(mapping->filter_conversion)) {
                continue;
            }
            RETURN_IF_ERROR(add_scan_column(file_request, mapping, enable_lazy_materialization(),
                                            force_full_complex_scan_projection(),
                                            &filter_projections));
        }
    }
    // Rebuild the file type for every scan-local mapping before expression rewrite. Predicate-only
    // hidden mappings must see the same projected file type as the file reader will produce.
    for (auto& mapping : _mappings) {
        if (mapping.file_local_id.has_value() &&
            file_request->local_positions.contains(LocalColumnId(*mapping.file_local_id))) {
            RETURN_IF_ERROR(apply_scan_projection_to_mapping_file_type(*file_request, &mapping));
        }
    }
    for (auto& mapping : _hidden_mappings) {
        if (mapping.file_local_id.has_value() &&
            file_request->local_positions.contains(LocalColumnId(*mapping.file_local_id))) {
            RETURN_IF_ERROR(apply_scan_projection_to_mapping_file_type(*file_request, &mapping));
        }
    }
    RETURN_IF_ERROR(_build_filter_entries(*file_request));

    // Build the complete table-slot rewrite map after all predicate columns have been assigned.
    // This keeps expression localization independent from filter iteration order.
    filter_mappings = _filter_visible_mappings();
    const auto global_to_file_slot = build_file_slot_rewrite_map(filter_mappings, _filter_entries);
    for (const auto& table_filter : table_filters) {
        if (table_filter.conjunct != nullptr &&
            table_filter_has_only_local_entries(table_filter, _filter_entries)) {
            RewriteContext rewrite_context {.runtime_state = runtime_state};
            VExprSPtr rewrite_root;
            Status clone_status;
            try {
                clone_status = clone_table_expr_tree(table_filter.conjunct->root(), &rewrite_root);
            } catch ([[maybe_unused]] const Exception& e) {
                // Some table filters contain complex intermediate values, for example
                // `element_at(MAP_VALUES(m)[1], 'age') > 30`. The current file-local rewrite only
                // understands top-level slots and struct-element paths rooted at top-level slots;
                // cloning such expressions can hit the generic TExpr complex-type limitation.
                // Leave them above TableReader, where Scanner evaluates the original table-level
                // conjunct after final materialization.
#ifndef NDEBUG
                return Status::InternalError(
                        "Failed to clone table filter for file-local rewrite: {}, expr={}",
                        e.to_string(), table_filter.conjunct->root()->debug_string());
#else
                continue;
#endif
            } catch ([[maybe_unused]] const std::exception& e) {
#ifndef NDEBUG
                return Status::InternalError(
                        "Failed to clone table filter for file-local rewrite: {}, expr={}",
                        e.what(), table_filter.conjunct->root()->debug_string());
#else
                continue;
#endif
            }
            if (!clone_status.ok()) {
#ifndef NDEBUG
                return Status::InternalError(
                        "Failed to clone table filter for file-local rewrite: {}, expr={}",
                        clone_status.to_string(), table_filter.conjunct->root()->debug_string());
#else
                continue;
#endif
            }
            bool can_localize = true;
            auto localized_root = rewrite_table_expr_to_file_expr(rewrite_root, global_to_file_slot,
                                                                  filter_mappings, &rewrite_context,
                                                                  &can_localize);
            if (!can_localize) {
                continue;
            }
            auto localized_conjunct = VExprContext::create_shared(std::move(localized_root));
            RETURN_IF_ERROR(rewrite_context.prepare_created_exprs(localized_conjunct.get()));
            file_request->conjuncts.push_back(std::move(localized_conjunct));
            for (const auto global_index : table_filter.global_indices) {
                const auto* mapping = _find_filter_mapping(global_index);
                if (mapping != nullptr && mapping->file_local_id.has_value() &&
                    filter_conversion_has_local_source(mapping->filter_conversion)) {
                    localized_predicate_columns.emplace(*mapping->file_local_id);
                }
            }
        }
    }

    // Candidate columns are added before expression rewriting because their file-block positions
    // are needed to localize slot refs. If rewriting rejects every filter that references a visible
    // column, move its already-merged output/filter projection to the lazy non-predicate set
    // instead of forcing it through the eager predicate path.
    for (auto& mapping : _mappings) {
        if (!mapping.file_local_id.has_value()) {
            continue;
        }
        const auto local_id = LocalColumnId(*mapping.file_local_id);
        if (localized_predicate_columns.contains(local_id)) {
            continue;
        }
        const auto predicate_it = std::ranges::find_if(
                file_request->predicate_columns, [local_id](const LocalColumnIndex& projection) {
                    return projection.column_id() == local_id;
                });
        if (predicate_it == file_request->predicate_columns.end()) {
            continue;
        }
        file_request->non_predicate_columns.push_back(std::move(*predicate_it));
        file_request->predicate_columns.erase(predicate_it);
    }
    return Status::OK();
}

const ColumnDefinition* TableColumnMapper::_find_file_field(
        const ColumnDefinition& table_column,
        const std::vector<ColumnDefinition>& file_schema) const {
    if (table_column.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
        const auto field_it = std::ranges::find_if(file_schema, [](const ColumnDefinition& field) {
            return field.column_type == ColumnType::GLOBAL_ROWID;
        });
        return field_it == file_schema.end() ? nullptr : &*field_it;
    }
    const auto* matched = matcher_for_mode(_options.mode).find(table_column, file_schema);
    if (matched != nullptr || _options.mode != TableColumnMappingMode::BY_FIELD_ID ||
        !_options.allow_idless_complex_wrapper_projection || table_column.children.empty()) {
        return matched;
    }
    const ColumnDefinition* wrapper = nullptr;
    for (const auto& candidate : file_schema) {
        if (candidate.has_identifier_field_id() || candidate.children.empty() ||
            !has_shared_descendant_field_id(table_column, candidate)) {
            continue;
        }
        if (wrapper != nullptr) {
            return nullptr;
        }
        wrapper = &candidate;
    }
    // Iceberg Parquet's PruneColumns retains an ID-less complex wrapper when a nested field ID is
    // selected. Descendant IDs, not aliases, identify that wrapper; ambiguity remains unmapped.
    return wrapper;
}

Status TableColumnMapper::_create_direct_mapping(const ColumnDefinition& table_column,
                                                 const ColumnDefinition& file_field,
                                                 ColumnMapping* mapping) const {
    DORIS_CHECK(mapping != nullptr);
    DORIS_CHECK(file_field.local_id >= 0 || file_field.local_id == GLOBAL_ROWID_COLUMN_ID);
    mapping->file_local_id = file_field.local_id;
    mapping->table_column_name = table_column.name;
    mapping->file_column_name = file_field.name;
    mapping->original_file_type = file_field.type;
    mapping->original_file_children = file_field.children;
    mapping->projected_file_children = file_field.children;
    mapping->file_type = file_field.type;
    mapping->is_trivial = mapping_can_use_file_column_directly(*mapping);
    mapping->filter_conversion = direct_filter_conversion(*mapping);
    mapping->child_mappings.clear();

    auto [table_children, synthesized_table_children] =
            prepare_table_children_for_mapping(table_column, mapping->file_type);

    if (!table_children.empty()) {
        if (!is_complex_type(remove_nullable(mapping->file_type)->get_primitive_type())) {
            return Status::NotSupported(
                    "Cannot map complex table column '{}' to scalar parquet column '{}', table "
                    "type={}, file type={}",
                    table_column.name, file_field.name, mapping->table_type->get_name(),
                    mapping->file_type->get_name());
        }
        RETURN_IF_ERROR(validate_file_schema_children(file_field));
        std::vector<int32_t> synthesized_used_file_child_ids;
        for (size_t table_child_idx = 0; table_child_idx < table_children.size();
             ++table_child_idx) {
            const auto& table_child = table_children[table_child_idx];
            const auto* file_child =
                    find_file_child_for_mapping(table_child, file_field, _options.mode,
                                                table_child_idx, synthesized_table_children);
            if (file_child == nullptr && !synthesized_table_children &&
                _options.mode == TableColumnMappingMode::BY_FIELD_ID &&
                _options.allow_idless_complex_wrapper_projection) {
                // Parquet can retain an ID-less wrapper at any depth when a selected descendant
                // has an ID; apply the same opt-in fallback used for root lookup recursively.
                file_child = _find_file_field(table_child, file_field.children);
            }
            if (synthesized_table_children && file_child != nullptr) {
                const auto file_child_id = file_child->file_local_id();
                if (std::ranges::find(synthesized_used_file_child_ids, file_child_id) !=
                    synthesized_used_file_child_ids.end()) {
                    file_child = nullptr;
                    for (const auto& candidate : file_field.children) {
                        const auto candidate_id = candidate.file_local_id();
                        if (std::ranges::find(synthesized_used_file_child_ids, candidate_id) ==
                            synthesized_used_file_child_ids.end()) {
                            file_child = &candidate;
                            break;
                        }
                    }
                }
                if (file_child != nullptr) {
                    synthesized_used_file_child_ids.push_back(file_child->file_local_id());
                }
            }
            if (file_child == nullptr) {
                ColumnMapping child_mapping;
                child_mapping.table_column_name = table_child.name;
                child_mapping.file_column_name = table_child.name;
                child_mapping.table_type = table_child.type;
                child_mapping.file_type = table_child.type;
                child_mapping.filter_conversion = FilterConversionType::FINALIZE_ONLY;
                // A missing nested field still has its Iceberg initial-default value in every row
                // written before the field was added; carry it into recursive materialization.
                RETURN_IF_ERROR(build_initial_default_column(
                        table_child, &child_mapping.initial_default_column));
                mapping->child_mappings.push_back(std::move(child_mapping));
                continue;
            }
            ColumnMapping child_mapping;
            child_mapping.table_column_name = table_child.name;
            child_mapping.table_type = table_child.type;
            RETURN_IF_ERROR(_create_direct_mapping(table_child, *file_child, &child_mapping));
            mapping->child_mappings.push_back(std::move(child_mapping));
        }
        if (needs_projected_file_type_rebuild(*mapping)) {
            // If complex projection prunes some children, we have to rebuild the projected file type to make sure the reader expression can find the correct child types by name.
            RETURN_IF_ERROR(rebuild_projected_file_children_and_type(
                    mapping->file_type, mapping->original_file_children, mapping->child_mappings,
                    &mapping->projected_file_children, &mapping->file_type));
            DCHECK(mapping->table_type != nullptr);
            mapping->is_trivial = mapping_can_use_file_column_directly(*mapping);
            mapping->filter_conversion = projected_filter_conversion(*mapping);
        }
    }
    return Status::OK();
}

} // namespace doris::format
