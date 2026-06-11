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
#include <span>
#include <sstream>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/data_type/convert_field_to_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "exprs/create_predicate_function.h"
#include "exprs/vcast_expr.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vin_predicate.h"
#include "exprs/vtopn_pred.h"
#include "format_v2/expr/cast.h"
#include "format_v2/expr/literal.h"
#include "format_v2/expr/slot_ref.h"
#include "format_v2/file_reader.h"
#include "format_v2/schema_projection.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Exprs_types.h"
#include "storage/predicate/predicate_creator.h"

namespace doris::format {

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
    if (column_has_name(rhs, lhs.name)) {
        return true;
    }
    if (lhs.has_identifier_name() && column_has_name(rhs, lhs.get_identifier_name())) {
        return true;
    }
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

struct StructChildSelector {
    bool by_name = true;
    std::string name;
    size_t ordinal = 0;
};

struct NestedStructPath {
    GlobalIndex root_global_index;
    std::vector<StructChildSelector> selectors;
};

static GlobalIndex slot_ref_global_index(const VSlotRef& slot_ref) {
    DORIS_CHECK(slot_ref.column_id() >= 0);
    return GlobalIndex(cast_set<size_t>(slot_ref.slot_id()));
}

// A split-local literal produced by slot-literal predicate localization. This wrapper keeps the
// original table literal so a cloned conjunct can be localized again for another split.
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
                                      const FileSlotRewriteInfo& rewrite_info,
                                      RewriteContext* rewrite_context) {
    auto ref = TableSlotRef::create_shared(slot_ref.slot_id(),
                                           cast_set<int>(rewrite_info.block_position), -1,
                                           rewrite_info.file_type, rewrite_info.file_column_name);
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
        << ", allow_missing_columns=" << allow_missing_columns << "}";
    return out.str();
}

std::string ColumnDefinition::debug_string() const {
    std::ostringstream out;
    out << "ColumnDefinition{name=" << name << ", identifier=" << field_debug_string(identifier)
        << ", name_mapping="
        << join_debug_strings(name_mapping, [](const std::string& name) { return name; })
        << ", local_id=" << local_id << ", type=" << data_type_debug_string(type) << ", children="
        << join_debug_strings(children,
                              [](const ColumnDefinition& child) { return child.debug_string(); })
        << ", has_default_expr=" << (default_expr != nullptr)
        << ", is_partition_key=" << is_partition_key << "}";
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

static TExprNode rebuild_expr_node(const VExpr& expr) {
    TExprNode node;
    node.__set_node_type(expr.node_type());
    node.__set_opcode(expr.op());
    node.__set_type(create_type_desc(remove_nullable(expr.data_type())->get_primitive_type(),
                                     cast_set<int>(expr.data_type()->get_precision()),
                                     cast_set<int>(expr.data_type()->get_scale())));
    node.__set_is_nullable(expr.data_type()->is_nullable());
    node.__set_num_children(expr.get_num_children());
    node.__set_fn(expr.fn());
    if (const auto* in_pred = dynamic_cast<const VInPredicate*>(&expr)) {
        TInPredicate in_predicate;
        in_predicate.__set_is_not_in(in_pred->is_not_in());
        node.__set_in_predicate(in_predicate);
    }
    return node;
}

// TODO(gabriel): could we clone it in another way?
Status clone_table_expr_tree(const VExprSPtr& expr, VExprSPtr* cloned_expr) {
    DORIS_CHECK(cloned_expr != nullptr);
    if (expr == nullptr) {
        *cloned_expr = nullptr;
        return Status::OK();
    }

    VExprSPtr cloned;
    if (const auto* table_slot_ref = dynamic_cast<const TableSlotRef*>(expr.get())) {
        cloned = TableSlotRef::create_shared(table_slot_ref->slot_id(), table_slot_ref->column_id(),
                                             table_slot_ref->column_uniq_id(),
                                             table_slot_ref->data_type(),
                                             table_slot_ref->column_name());
    } else if (const auto* vslot_ref = dynamic_cast<const VSlotRef*>(expr.get())) {
        cloned = TableSlotRef::create_shared(vslot_ref->slot_id(), vslot_ref->column_id(),
                                             vslot_ref->column_uniq_id(), vslot_ref->data_type(),
                                             vslot_ref->column_name());
    } else if (const auto* split_literal = dynamic_cast<const SplitLocalFileLiteral*>(expr.get())) {
        cloned = std::make_shared<SplitLocalFileLiteral>(
                split_literal->data_type(), literal_field(expr), split_literal->original_type(),
                split_literal->original_field());
    } else if (dynamic_cast<const TableLiteral*>(expr.get()) != nullptr) {
        cloned = TableLiteral::create_shared(expr->data_type(), literal_field(expr));
    } else if (expr->is_literal()) {
        cloned = TableLiteral::create_shared(expr->data_type(), literal_field(expr));
    } else if (const auto* cast_expr = dynamic_cast<const Cast*>(expr.get())) {
        cloned = std::make_shared<Cast>(cast_expr->data_type());
    } else if (const auto* vcast_expr = dynamic_cast<const VCastExpr*>(expr.get());
               vcast_expr != nullptr && vcast_expr->node_type() == TExprNodeType::CAST_EXPR) {
        cloned = std::make_shared<Cast>(vcast_expr->data_type());
    } else if (const auto* in_pred = dynamic_cast<const VInPredicate*>(expr.get())) {
        cloned = VInPredicate::create_shared(rebuild_expr_node(*in_pred));
    } else if (const auto* direct_in_pred = dynamic_cast<const VDirectInPredicate*>(expr.get())) {
        cloned = std::make_shared<VDirectInPredicate>(rebuild_expr_node(*direct_in_pred),
                                                      direct_in_pred->get_set_func());
    } else if (const auto* compound_pred = dynamic_cast<const VCompoundPred*>(expr.get())) {
        cloned = VCompoundPred::create_shared(rebuild_expr_node(*compound_pred));
    } else if (const auto* topn_pred = dynamic_cast<const VTopNPred*>(expr.get())) {
        cloned = VTopNPred::create_shared(rebuild_expr_node(*topn_pred),
                                          topn_pred->source_node_id(), nullptr);
    } else if (const auto* fn_call = dynamic_cast<const VectorizedFnCall*>(expr.get())) {
        cloned = VectorizedFnCall::create_shared(rebuild_expr_node(*fn_call));
    } else {
        return Status::NotSupported("Cannot clone expression {} for file-local rewrite",
                                    expr->expr_name());
    }

    VExprSPtrs cloned_children;
    cloned_children.reserve(expr->children().size());
    for (const auto& child : expr->children()) {
        VExprSPtr cloned_child;
        RETURN_IF_ERROR(clone_table_expr_tree(child, &cloned_child));
        cloned_children.push_back(std::move(cloned_child));
    }
    cloned->set_children(std::move(cloned_children));
    cloned->reset_prepare_state();
    *cloned_expr = std::move(cloned);
    return Status::OK();
}

static VExprSPtr original_table_literal(const VExprSPtr& literal_expr,
                                        RewriteContext* rewrite_context = nullptr) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto* rewritten_literal = dynamic_cast<const SplitLocalFileLiteral*>(literal_expr.get());
    if (rewritten_literal == nullptr) {
        return literal_expr;
    }
    auto literal = TableLiteral::create_shared(rewritten_literal->original_type(),
                                               rewritten_literal->original_field());
    if (rewrite_context != nullptr) {
        rewrite_context->add_created_expr(literal);
    }
    return literal;
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
        path->root_global_index = slot_ref_global_index(*slot_ref);
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

static const ColumnMapping* resolve_mapped_child(const ColumnMapping& mapping,
                                                 const StructChildSelector& selector) {
    const auto global_child_index = struct_child_index(mapping, selector);
    if (!global_child_index.has_value()) {
        return nullptr;
    }
    return resolve_mapped_child(mapping, *global_child_index);
}

static std::shared_ptr<IndexMapping> build_child_index_mapping(const ColumnMapping& mapping) {
    DORIS_CHECK(mapping.file_local_id.has_value());
    auto result = std::make_shared<IndexMapping>();
    result->index = *mapping.file_local_id;
    for (size_t child_idx = 0; child_idx < mapping.child_mappings.size(); ++child_idx) {
        const auto& child_mapping = mapping.child_mappings[child_idx];
        if (!child_mapping.file_local_id.has_value()) {
            continue;
        }
        result->child_mapping.emplace(child_mapping_global_index(mapping, child_mapping, child_idx),
                                      build_child_index_mapping(child_mapping));
    }
    return result;
}

static IndexMapping build_index_mapping(const ColumnMapping& mapping, LocalIndex block_position) {
    DORIS_CHECK(mapping.file_local_id.has_value());
    IndexMapping result;
    result.index = cast_set<int32_t>(block_position.value());
    for (size_t child_idx = 0; child_idx < mapping.child_mappings.size(); ++child_idx) {
        const auto& child_mapping = mapping.child_mappings[child_idx];
        if (!child_mapping.file_local_id.has_value()) {
            continue;
        }
        result.child_mapping.emplace(child_mapping_global_index(mapping, child_mapping, child_idx),
                                     build_child_index_mapping(child_mapping));
    }
    return result;
}

static bool resolve_nested_projection_with_index_mapping(const NestedStructPath& path,
                                                         const std::vector<ColumnMapping>& mappings,
                                                         LocalColumnIndex* root_projection,
                                                         const ColumnMapping** leaf_mapping) {
    DORIS_CHECK(root_projection != nullptr);
    DORIS_CHECK(leaf_mapping != nullptr);
    *root_projection = {};
    *leaf_mapping = nullptr;
    if (path.selectors.empty()) {
        return false;
    }
    const auto mapping_it = std::ranges::find_if(mappings, [&](const ColumnMapping& mapping) {
        return mapping.global_index == path.root_global_index;
    });
    if (mapping_it == mappings.end() || !mapping_it->file_local_id.has_value()) {
        return false;
    }

    const auto root_index_mapping = build_index_mapping(*mapping_it, LocalIndex(0));
    *root_projection = LocalColumnIndex::partial_field(*mapping_it->file_local_id);
    auto* current_projection = root_projection;
    const auto* current_mapping = &*mapping_it;
    const auto* current_index_mapping = &root_index_mapping;
    for (size_t selector_idx = 0; selector_idx < path.selectors.size(); ++selector_idx) {
        const auto global_child_index =
                struct_child_index(*current_mapping, path.selectors[selector_idx]);
        if (!global_child_index.has_value()) {
            *root_projection = {};
            return false;
        }
        const auto index_mapping_it =
                current_index_mapping->child_mapping.find(*global_child_index);
        if (index_mapping_it == current_index_mapping->child_mapping.end()) {
            *root_projection = {};
            return false;
        }
        const auto* child_mapping = resolve_mapped_child(*current_mapping, *global_child_index);
        DORIS_CHECK(child_mapping != nullptr);
        DORIS_CHECK(child_mapping->file_local_id.has_value());

        auto child_projection = LocalColumnIndex::partial_field(index_mapping_it->second->index);
        child_projection.project_all_children = selector_idx + 1 == path.selectors.size();
        current_projection->children.push_back(std::move(child_projection));
        current_projection = &current_projection->children.back();
        current_mapping = child_mapping;
        current_index_mapping = index_mapping_it->second.get();
    }
    *leaf_mapping = current_mapping;
    return true;
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
    *projection = LocalColumnIndex::field(child->file_local_id());
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
    RETURN_IF_ERROR(
            build_filter_projection_path(child->children, selectors.subspan(1), &child_projection));
    if (child_projection.field_id() < 0) {
        *projection = LocalColumnIndex {};
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
    const auto* child_mapping = resolve_mapped_child(mapping, selectors.front());
    if (child_mapping == nullptr) {
        return build_filter_projection_path(mapping.original_file_children, selectors, projection);
    }
    if (!child_mapping->file_local_id.has_value()) {
        *projection = LocalColumnIndex {};
        return Status::OK();
    }
    *projection = LocalColumnIndex::field(*child_mapping->file_local_id);
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
    if (child_projection.field_id() < 0) {
        *projection = LocalColumnIndex {};
        return Status::OK();
    }
    projection->children.push_back(std::move(child_projection));
    return Status::OK();
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
        file_child_id_path->push_back(current_projection->field_id());
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
    const auto original_literal = original_table_literal(literal_expr);
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
        const auto original_literal = original_table_literal(literal_expr);
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

    LocalColumnIndex file_projection;
    const ColumnMapping* leaf_mapping = nullptr;
    if (!resolve_nested_projection_with_index_mapping(path, mappings, &file_projection,
                                                      &leaf_mapping) ||
        leaf_mapping == nullptr || leaf_mapping->file_type == nullptr ||
        is_complex_type(remove_nullable(leaf_mapping->file_type)->get_primitive_type())) {
        return false;
    }
    auto predicate = build_nested_comparison_predicate(
            literal_expr, opcode, file_projection.column_id(), leaf_mapping->file_column_name,
            remove_nullable(leaf_mapping->file_type));
    if (predicate == nullptr) {
        return false;
    }
    std::vector<int32_t> file_child_id_path;
    if (!extract_child_id_path_from_projection(file_projection, &file_child_id_path)) {
        return false;
    }
    column_filter->file_column_id = file_projection.column_id();
    column_filter->file_child_id_path = std::move(file_child_id_path);
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

    LocalColumnIndex file_projection;
    const ColumnMapping* leaf_mapping = nullptr;
    if (!resolve_nested_projection_with_index_mapping(path, mappings, &file_projection,
                                                      &leaf_mapping) ||
        leaf_mapping == nullptr || leaf_mapping->file_type == nullptr ||
        is_complex_type(remove_nullable(leaf_mapping->file_type)->get_primitive_type())) {
        return false;
    }
    auto predicate = build_nested_in_list_predicate(literal_exprs, file_projection.column_id(),
                                                    leaf_mapping->file_column_name,
                                                    remove_nullable(leaf_mapping->file_type));
    if (predicate == nullptr) {
        return false;
    }
    std::vector<int32_t> file_child_id_path;
    if (!extract_child_id_path_from_projection(file_projection, &file_child_id_path)) {
        return false;
    }
    column_filter->file_column_id = file_projection.column_id();
    column_filter->file_child_id_path = std::move(file_child_id_path);
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
                                              const FileSlotRewriteInfo& rewrite_info,
                                              RewriteContext* rewrite_context) {
    DORIS_CHECK(literal_expr != nullptr);
    DORIS_CHECK(literal_expr->is_literal());
    const auto original_literal = original_table_literal(literal_expr, rewrite_context);
    const Field original_field = literal_field(original_literal);
    if (rewrite_info.file_type->equals(*original_literal->data_type())) {
        return original_literal;
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

static VExprSPtr rewrite_table_expr_to_file_expr(
        const VExprSPtr& expr,
        const std::map<GlobalIndex, FileSlotRewriteInfo>& global_to_file_slot,
        RewriteContext* rewrite_context) {
    if (expr == nullptr) {
        return nullptr;
    }
    DORIS_CHECK(rewrite_context != nullptr);
    if (rewrite_binary_slot_literal_predicate(expr, global_to_file_slot, rewrite_context)) {
        return expr;
    }
    if (rewrite_in_slot_literal_predicate(expr, global_to_file_slot, rewrite_context)) {
        return expr;
    }
    if (is_struct_element_expr(expr)) {
        auto children = expr->children();
        if (children[0]->is_slot_ref()) {
            const auto* slot_ref = assert_cast<const VSlotRef*>(children[0].get());
            const auto rewrite_it = global_to_file_slot.find(slot_ref_global_index(*slot_ref));
            if (rewrite_it != global_to_file_slot.end()) {
                // struct_element must see the actual file struct layout. Casting the parent struct
                // to the output projection can hide filter-only children such as `s.id` in
                // `SELECT s.name WHERE s.id > 5`.
                children[0] = create_file_slot_ref(*slot_ref, rewrite_it->second, rewrite_context);
                expr->set_children(std::move(children));
                return expr;
            }
        }
        children[0] =
                rewrite_table_expr_to_file_expr(children[0], global_to_file_slot, rewrite_context);
        expr->set_children(std::move(children));
        return expr;
    }
    if (expr->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
        const auto rewrite_it = global_to_file_slot.find(slot_ref_global_index(*slot_ref));
        if (rewrite_it != global_to_file_slot.end()) {
            const auto& rewrite_info = rewrite_it->second;
            auto file_slot = create_file_slot_ref(*slot_ref, rewrite_info, rewrite_context);
            if (rewrite_info.file_type->equals(*rewrite_info.table_type)) {
                return file_slot;
            }
            auto cast_expr = Cast::create_shared(rewrite_info.table_type);
            cast_expr->add_child(std::move(file_slot));
            rewrite_context->add_created_expr(cast_expr);
            return cast_expr;
        }
        return expr;
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
                expr->set_children({std::move(rewritten_child)});
                return expr;
            }
        }
    }

    VExprSPtrs rewritten_children;
    rewritten_children.reserve(expr->children().size());
    for (const auto& child : expr->children()) {
        rewritten_children.push_back(
                rewrite_table_expr_to_file_expr(child, global_to_file_slot, rewrite_context));
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
        // `child_mapping.table_column_name != child_mapping.file_column_name` means this column is renamed
        // `!child_mapping.file_local_id.has_value()` means this column is miss in file
        if (child_mapping.table_column_name != child_mapping.file_column_name ||
            !child_mapping.file_local_id.has_value() ||
            complex_projection_has_pruned_children(child_mapping)) {
            return true;
        }
    }
    return false;
}

// Build the projected file type according to the pruned complex projection. For example, if we
// have a struct column `s` with children `id` and `name`, and the projection only keeps `s.name`,
// then we need to build the projected file type of `s` to only contain `name` so that the file
// reader can read it correctly.
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
        if (!child_mapping.file_local_id.has_value()) {
            // Missing child column
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
    DORIS_CHECK(mapping.file_local_id.has_value());
    *projection = LocalColumnIndex::field(*mapping.file_local_id);
    projection->project_all_children = mapping.child_mappings.empty();
    projection->children.clear();
    for (const auto& child_mapping : mapping.child_mappings) {
        if (!child_mapping.file_local_id.has_value()) {
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

// Re-build file type according to the pruned complex projection.
// For example, if we have a struct column `s` with children `id` and `name`,
// and the projection only keeps `s.name`, then we need to rebuild the file type of `s` to only
// contain `name` so that the file reader can read it correctly.
static Status rebuild_projected_file_type(ColumnMapping* mapping) {
    if (mapping == nullptr) {
        return Status::InvalidArgument("mapping is null");
    }
    if (mapping->original_file_type == nullptr) {
        mapping->original_file_type = mapping->file_type;
    }
    DORIS_CHECK(
            is_complex_type(remove_nullable(mapping->original_file_type)->get_primitive_type()));
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
                              std::vector<LocalColumnIndex>* scan_columns, bool is_predicate_column,
                              const FilterProjectionMap* filter_projections = nullptr) {
    const auto file_column_id = LocalColumnId(mapping->file_local_id.value());
    if (!is_predicate_column &&
        std::ranges::find_if(file_request->predicate_columns, [&](const LocalColumnIndex& p) {
            return p.column_id() == file_column_id;
        }) != file_request->predicate_columns.end()) {
        // This column is already projected for predicates, so skip adding it to non-predicate columns.
        return Status::OK();
    }
    // local_positions is the global read-column index for this scan request, so it also
    // deduplicates predicate_columns and non_predicate_columns across all filter/projection paths.
    const bool newly_added = file_request->local_positions.count(file_column_id) == 0;
    if (newly_added) {
        file_request->local_positions.emplace(file_column_id,
                                              LocalIndex(file_request->local_positions.size()));
    }
    LocalColumnIndex projection = LocalColumnIndex::top_level(file_column_id);
    if (mapping->has_complex_projection) {
        if (complex_projection_has_pruned_children(*mapping)) {
            RETURN_IF_ERROR(rebuild_projected_file_type(mapping));
        }
        RETURN_IF_ERROR(build_complex_projection(*mapping, &projection));
    }
    if (is_predicate_column) {
        DCHECK(filter_projections != nullptr);
        // TODO: merge non-predicate projections for the same column as well, to avoid duplicated projections when the same column is used in multiple predicates.
        RETURN_IF_ERROR(merge_filter_projection(filter_projections, &projection));
    }
    auto existing_projection_it = std::ranges::find_if(
            *scan_columns,
            [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; });
    auto exists = existing_projection_it != scan_columns->end();
    if (exists) {
        RETURN_IF_ERROR(merge_local_column_index(&*existing_projection_it, projection));
    } else {
        scan_columns->push_back(std::move(projection));
    }
    // FIXME: only `apply_projection_to_mapping_file_type` if exists == true ?
    RETURN_IF_ERROR(apply_projection_to_mapping_file_type(
            exists ? *existing_projection_it : scan_columns->back(), mapping));
    if (is_predicate_column) {
        // TODO: if the same column is used in both predicate and non-predicate projections, we can merge the two projections and only keep it in predicate_columns.
        auto it = std::ranges::find_if(
                file_request->non_predicate_columns,
                [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; });
        if (it != file_request->non_predicate_columns.end()) {
            file_request->non_predicate_columns.erase(it);
        }
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
            if (mapping_it == mappings->end() || !mapping_it->file_local_id.has_value() ||
                path.selectors.empty()) {
                continue;
            }

            LocalColumnIndex root_projection;
            const ColumnMapping* leaf_mapping = nullptr;
            if (!resolve_nested_projection_with_index_mapping(path, *mappings, &root_projection,
                                                              &leaf_mapping)) {
                LocalColumnIndex child_projection;
                RETURN_IF_ERROR(build_filter_projection_path(*mapping_it, path.selectors,
                                                             &child_projection));
                if (child_projection.field_id() < 0) {
                    continue;
                }
                root_projection = LocalColumnIndex::partial_field(*mapping_it->file_local_id);
                root_projection.children.push_back(std::move(child_projection));
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

static const ColumnDefinition* find_file_child_by_table_column(
        const ColumnDefinition& table_column, const std::vector<ColumnDefinition>& file_children,
        TableColumnMappingMode mode) {
    return matcher_for_mode(mode).find(table_column, file_children);
}

static const ColumnDefinition* find_file_child_for_complex_wrapper(
        const ColumnDefinition& table_child, const ColumnDefinition& file_field,
        TableColumnMappingMode mode) {
    if (file_field.children.empty()) {
        return nullptr;
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
        if (const auto* partition_value = find_partition_value(table_column, partition_values);
            table_column.is_partition_key && partition_value != nullptr) {
            // 1. Partition column, use partition value as a constant mapping. Note that partition column may also have default expression, but partition value should take precedence if it exists.
            _set_constant_mapping(&mapping, VExprContext::create_shared(TableLiteral::create_shared(
                                                    mapping.table_type, *partition_value)));
        } else if (_options.mode == TableColumnMappingMode::BY_INDEX &&
                   !table_column.is_partition_key && table_column.has_identifier_field_id()) {
            // 2. BY_INDEX mapping, use the file column at the position specified by `ColumnDefinition::identifier` as a direct mapping. This mode is only used by Hive.
            RETURN_IF_ERROR(_create_by_index_mapping(table_column, file_schema, &mapping));
        } else if (const auto* file_field = _find_file_field(table_column, file_schema)) {
            // 3. Table column has a matching file column, use it as a direct mapping.
            RETURN_IF_ERROR(_create_direct_mapping(table_column, *file_field, &mapping));
        } else if (table_column.default_expr != nullptr) {
            // 4. Table column does not exist in file (column adding by schema evolution), which has a default expression, use it as a constant mapping.
            _set_constant_mapping(&mapping, table_column.default_expr);
        } else if (table_column.name == ROW_LINEAGE_ROW_ID) {
            // 5. Virtual column, use special mapping to indicate it should be materialized by table reader instead of read from file or evaluated from expression.
            mapping.virtual_column_type = TableVirtualColumnType::ROW_ID;
        } else if (table_column.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
            mapping.virtual_column_type = TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER;
        } else if (table_column.name == BeConsts::ICEBERG_ROWID_COL) {
            mapping.virtual_column_type = TableVirtualColumnType::ICEBERG_ROWID;
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
        const TableColumnPredicates& table_column_predicates,
        const std::vector<ColumnDefinition>& projected_columns, FileScanRequest* file_request,
        RuntimeState* runtime_state) {
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
        if (mapping != nullptr && mapping->file_local_id.has_value()) {
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
                                                &file_request->non_predicate_columns, false));
            }
        }
    }
    // 2. Build referenced predicate columns
    RETURN_IF_ERROR(
            localize_filters(table_filters, table_column_predicates, file_request, runtime_state));
    // 3. Re-build projections for all referenced file columns to point to the correct file-local block positions.
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

Status TableColumnMapper::localize_filters(const std::vector<TableFilter>& table_filters,
                                           const TableColumnPredicates& table_column_predicates,
                                           FileScanRequest* file_request,
                                           RuntimeState* runtime_state) {
    FilterProjectionMap filter_projections;
    RETURN_IF_ERROR(build_filter_projection_map(table_filters, &_mappings, &filter_projections));
    for (const auto& table_filter : table_filters) {
        for (const auto& global_index : table_filter.global_indices) {
            auto* mapping = _find_mapping(global_index);
            if (mapping == nullptr || !mapping->file_local_id.has_value() ||
                !filter_conversion_has_local_source(mapping->filter_conversion)) {
                continue;
            }
            RETURN_IF_ERROR(add_scan_column(file_request, mapping, &file_request->predicate_columns,
                                            true, &filter_projections));
        }
    }
    RETURN_IF_ERROR(_build_filter_entries(*file_request));

    // Build the complete table-slot rewrite map after all predicate columns have been assigned.
    // This keeps expression localization independent from filter iteration order.
    const auto global_to_file_slot = build_file_slot_rewrite_map(_mappings, _filter_entries);
    for (const auto& table_filter : table_filters) {
        if (table_filter.conjunct != nullptr &&
            table_filter_has_only_local_entries(table_filter, _filter_entries)) {
            RewriteContext rewrite_context {.runtime_state = runtime_state};
            VExprSPtr rewrite_root;
            const auto clone_status =
                    clone_table_expr_tree(table_filter.conjunct->root(), &rewrite_root);
            if (!clone_status.ok()) {
                continue;
            }
            auto localized_root = rewrite_table_expr_to_file_expr(rewrite_root, global_to_file_slot,
                                                                  &rewrite_context);
            auto localized_conjunct = VExprContext::create_shared(std::move(localized_root));
            RETURN_IF_ERROR(rewrite_context.prepare_created_exprs(localized_conjunct.get()));
            file_request->conjuncts.push_back(std::move(localized_conjunct));
        }
    }
    for (const auto& [global_index, predicates] : table_column_predicates) {
        const auto* mapping = _find_mapping(global_index);
        const auto entry_it = _filter_entries.find(global_index);
        if (mapping == nullptr || !mapping->file_local_id.has_value() || predicates.empty() ||
            entry_it == _filter_entries.end() || !entry_it->second.is_local() ||
            !column_predicate_can_use_local_source(mapping->filter_conversion) ||
            mapping->file_type == nullptr) {
            continue;
        }
        FileColumnPredicateFilter column_predicate_filter;
        column_predicate_filter.file_column_id = LocalColumnId(*mapping->file_local_id);
        const auto file_primitive_type = remove_nullable(mapping->file_type)->get_primitive_type();
        for (const auto& predicate : predicates) {
            DORIS_CHECK(predicate != nullptr);
            if (predicate->primitive_type() == file_primitive_type) {
                column_predicate_filter.predicates.push_back(predicate);
            }
        }
        if (column_predicate_filter.predicates.empty()) {
            continue;
        }
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
    if (table_column.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
        const auto field_it = std::ranges::find_if(file_schema, [](const ColumnDefinition& field) {
            return field.column_type == ColumnType::GLOBAL_ROWID;
        });
        return field_it == file_schema.end() ? nullptr : &*field_it;
    }
    return matcher_for_mode(_options.mode).find(table_column, file_schema);
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
    mapping->file_type = file_field.type;
    mapping->is_trivial = mapping->table_type->equals(*mapping->file_type);
    mapping->filter_conversion = mapping->is_trivial ? FilterConversionType::COPY_DIRECTLY
                                                     : FilterConversionType::CAST_FILTER;
    mapping->child_mappings.clear();

    if (!table_column.children.empty()) {
        DORIS_CHECK(is_complex_type(mapping->file_type->get_primitive_type()));
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
            // If complex projection prunes some children, we have to rebuild the projected file type to make sure the reader expression can find the correct child types by name.
            RETURN_IF_ERROR(build_projected_child_type(mapping->file_type, mapping->child_mappings,
                                                       &mapping->file_type));
            DCHECK(mapping->table_type != nullptr);
            mapping->is_trivial = mapping->table_type->equals(*mapping->file_type);
            // TODO: ? READER_EXPRESSION
            mapping->filter_conversion = mapping->is_trivial
                                                 ? FilterConversionType::COPY_DIRECTLY
                                                 : FilterConversionType::READER_EXPRESSION;
        }
    }
    return Status::OK();
}

} // namespace doris::format
