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

#include "exprs/vsearch.h"

#include <fmt/format.h>

#include <memory>
#include <roaring/roaring.hh>

#include "common/logging.h"
#include "common/status.h"
#include "core/column/column_const.h"
#include "exprs/function/search/function_search.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "glog/logging.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/schema.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/variant_column_reader.h"

namespace doris {
using namespace segment_v2;

namespace {

struct SearchInputBundle {
    std::unordered_map<std::string, IndexIterator*> iterators;
    std::unordered_map<std::string, IndexFieldNameAndTypePair> field_types;
    const TabletColumn* nested_column = nullptr;
    std::vector<int> column_indexes;
    ColumnsWithTypeAndName literal_args;
};

void add_search_binding_diagnostic(const IndexExecContext* index_context,
                                   const std::string& diagnostic) {
    VLOG_DEBUG << diagnostic;
    if (index_context == nullptr) {
        return;
    }
    const auto& index_query_context = index_context->get_index_query_context();
    if (index_query_context != nullptr && index_query_context->stats != nullptr) {
        index_query_context->stats->inverted_index_stats.add_binding_diagnostic(diagnostic);
    }
}

// A missing iterator is not evidence of a missing value: doc/sparse paths and
// old rowsets may contain values without an executable SEARCH index.
Status verify_missing_variant_path(const TSearchFieldBinding& binding, int column_index,
                                   IndexExecContext* index_context) {
    auto* segment = index_context->segment();
    if (segment == nullptr || !binding.__isset.parent_field_name ||
        !binding.__isset.subcolumn_path) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SEARCH cannot establish missing path '{}'", binding.field_name);
    }
    const auto& read_schema = index_context->read_schema();
    DORIS_CHECK_GE(column_index, 0);
    DORIS_CHECK_LT(static_cast<size_t>(column_index), read_schema->num_read_columns());
    const auto& column = *read_schema->column(column_index);
    const auto& opts = index_context->column_iter_opts();
    DORIS_CHECK(opts.stats != nullptr);
    StorageReadOptions read_options(*opts.stats);
    read_options.io_ctx = opts.io_ctx;
    std::shared_ptr<VariantColumnReader> reader;
    Status st = segment->get_variant_root_reader(column, read_options, &reader);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        // After DROP v(uid=1), ADD v(uid=2), an old segment cannot contain this scan's root.
        // Resolve by the current uid so historical values of the same name cannot match.
        return Status::OK();
    }
    RETURN_IF_ERROR(st);
    DORIS_CHECK(reader != nullptr);
    RETURN_IF_ERROR(reader->load_external_meta_once(opts.stats, &opts.io_ctx));
    const PathInData path(binding.subcolumn_path);
    const auto* stats = reader->get_stats();
    // Doc statistics enumerate stored paths without the sparse statistics limit. For example,
    // a segment containing only `string4` can prove `string5` absent even in doc mode.
    // An opaque root/ancestor value may contain the requested path, so it remains unavailable.
    bool in_doc = stats->existed_in_doc_value_column("") ||
                  stats->has_prefix_path_in_doc_value_column(path.get_path() + ".");
    std::string ancestor = path.get_path();
    while (!in_doc) {
        in_doc = stats->existed_in_doc_value_column(ancestor);
        const auto dot = ancestor.rfind('.');
        if (dot == std::string::npos) {
            break;
        }
        ancestor.resize(dot);
    }
    // Truncated sparse statistics cannot prove absence, nor can an existing unindexed path.
    if (reader->get_subcolumn_meta_by_path(path) != nullptr ||
        reader->has_prefix_path(path, opts.stats, &opts.io_ctx) ||
        reader->exist_in_sparse_column(path) || reader->is_exceeded_sparse_column_limit() ||
        in_doc || stats->has_nested_group) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SEARCH index unavailable for Variant field '{}'", binding.field_name);
    }
    return Status::OK();
}

Status bind_nested_column(const VSearchExpr& expr, const VSlotRef& slot_ref,
                          const TSearchFieldBinding* binding, const IndexExecContext& index_context,
                          SearchInputBundle* bundle) {
    const auto& clause = expr.get_search_param().root;
    if (clause.clause_type != "NESTED") {
        return Status::OK();
    }
    const auto root_name = clause.nested_path.substr(0, clause.nested_path.find('.'));
    const auto& parent_name = binding != nullptr && binding->__isset.parent_field_name
                                      ? binding->parent_field_name
                                      : slot_ref.column_name();
    if (parent_name != root_name) {
        return Status::OK();
    }

    const auto& schema = index_context.read_schema();
    if (schema == nullptr) {
        return Status::InvalidArgument("NESTED query requires a read schema");
    }
    const int ordinal = slot_ref.column_id();
    if (ordinal < 0 || static_cast<size_t>(ordinal) >= schema->num_column_ids()) {
        return Status::InvalidArgument("NESTED query column index {} is outside the read schema",
                                       ordinal);
    }
    const auto* column = schema->column(schema->column_id(ordinal));
    if (bundle->nested_column != nullptr) {
        const auto root_uid = [](const TabletColumn& col) {
            return col.parent_unique_id() >= 0 ? col.parent_unique_id() : col.unique_id();
        };
        if (root_uid(*bundle->nested_column) != root_uid(*column)) {
            return Status::InvalidArgument(
                    "NESTED query binds columns from different VARIANT roots");
        }
    }
    bundle->nested_column = column;
    return Status::OK();
}

Status collect_slot_search_input(const VSearchExpr& expr, const VSlotRef& slot_ref,
                                 const TSearchFieldBinding* binding,
                                 IndexExecContext* index_context, SearchInputBundle* bundle) {
    DCHECK(index_context != nullptr);
    DCHECK(bundle != nullptr);

    // VSlotRef::column_id() is the scan-schema position used by IndexExecContext.
    const int column_index = slot_ref.column_id();
    const std::string field_name =
            binding != nullptr ? binding->field_name : slot_ref.column_name();
    const bool is_variant_subcolumn = binding != nullptr && binding->__isset.is_variant_subcolumn &&
                                      binding->is_variant_subcolumn;

    RETURN_IF_ERROR(bind_nested_column(expr, slot_ref, binding, *index_context, bundle));

    auto* iterator = index_context->get_inverted_index_iterator_by_column_id(column_index);
    if (iterator == nullptr) {
        // For example, `data.items.message` has its own SlotRef in the scan schema. The
        // storage layer may inherit index metadata from `data`, but it still constructs a
        // child iterator whose stored field name contains the complete Variant path.
        if (is_variant_subcolumn) {
            add_search_binding_diagnostic(
                    index_context,
                    fmt::format("[VariantSearchBinding] phase=collect_inputs "
                                "result=no_iterator logical_field={} column_index={} "
                                "reason=slot_iterator_missing",
                                field_name, column_index));
        }
        if (!is_variant_subcolumn) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "SEARCH index unavailable for field '{}'", field_name);
        }
        RETURN_IF_ERROR(verify_missing_variant_path(*binding, column_index, index_context));
        return Status::OK();
    }

    const auto* storage_name_type =
            index_context->get_storage_name_and_type_by_column_id(column_index);
    if (storage_name_type == nullptr) {
        return Status::InternalError("storage_name_type not found for column {} in {}",
                                     column_index, expr.expr_name());
    }

    bundle->iterators.emplace(field_name, iterator);
    bundle->field_types.emplace(field_name, *storage_name_type);
    bundle->column_indexes.emplace_back(column_index);
    if (is_variant_subcolumn) {
        add_search_binding_diagnostic(
                index_context,
                fmt::format("[VariantSearchBinding] phase=collect_inputs "
                            "result=direct_iterator logical_field={} column_index={} "
                            "stored_field={}",
                            field_name, column_index, storage_name_type->first));
    }
    return Status::OK();
}

Status bind_nested_element_at(const VSearchExpr& expr, const VExpr& child,
                              const TSearchFieldBinding& binding,
                              const IndexExecContext& index_context, SearchInputBundle* bundle) {
    if (expr.get_search_param().root.clause_type != "NESTED") {
        return Status::OK();
    }
    const VExpr* root_expr = &child;
    while (root_expr->expr_name() == "element_at") {
        if (root_expr->children().empty()) {
            return Status::InvalidArgument("NESTED query has an incomplete VARIANT path");
        }
        root_expr = root_expr->children().front().get();
    }
    if (!root_expr->is_slot_ref()) {
        return Status::InvalidArgument("NESTED query requires a slot reference root");
    }
    return bind_nested_column(expr, *assert_cast<const VSlotRef*>(root_expr), &binding,
                              index_context, bundle);
}

const VSlotRef& variant_root_slot(const VExpr& expr) {
    const VExpr* root = &expr;
    while (root->expr_name() == "element_at") {
        DORIS_CHECK(!root->children().empty());
        root = root->children().front().get();
    }
    DORIS_CHECK(root->is_slot_ref());
    return *assert_cast<const VSlotRef*>(root);
}

Status collect_search_inputs(const VSearchExpr& expr, VExprContext* context,
                             SearchInputBundle* bundle) {
    DCHECK(bundle != nullptr);

    auto index_context = context->get_index_context();
    if (index_context == nullptr) {
        LOG(WARNING) << "collect_search_inputs: No inverted index context available";
        return Status::InternalError("No inverted index context available");
    }

    const auto& search_param = expr.get_search_param();
    const auto& field_bindings = search_param.field_bindings;

    size_t child_index = 0;
    for (const auto& child : expr.children()) {
        if (child->is_slot_ref()) {
            auto* column_slot_ref = assert_cast<VSlotRef*>(child.get());
            const TSearchFieldBinding* binding =
                    child_index < field_bindings.size() ? &field_bindings[child_index] : nullptr;
            RETURN_IF_ERROR(collect_slot_search_input(expr, *column_slot_ref, binding,
                                                      index_context.get(), bundle));
            ++child_index;
        } else if (child->is_literal()) {
            auto* literal = assert_cast<VLiteral*>(child.get());
            bundle->literal_args.emplace_back(literal->get_column_ptr(), literal->get_data_type(),
                                              literal->expr_name());
        } else {
            // Check if this is ElementAt expression (for variant subcolumn access)
            if (child->expr_name() == "element_at" && child_index < field_bindings.size() &&
                field_bindings[child_index].__isset.is_variant_subcolumn &&
                field_bindings[child_index].is_variant_subcolumn) {
                RETURN_IF_ERROR(bind_nested_element_at(expr, *child, field_bindings[child_index],
                                                       *index_context, bundle));
                const auto& root_slot = variant_root_slot(*child);
                // Variant subcolumn not materialized - skip, will create empty BitSetQuery in function_search
                add_search_binding_diagnostic(
                        index_context.get(),
                        fmt::format("[VariantSearchBinding] phase=collect_inputs "
                                    "result=unmaterialized_element_at logical_field={} "
                                    "parent_field={} sub_path={} reason=no_slot_ref",
                                    field_bindings[child_index].field_name,
                                    field_bindings[child_index].__isset.parent_field_name
                                            ? field_bindings[child_index].parent_field_name
                                            : "",
                                    field_bindings[child_index].__isset.subcolumn_path
                                            ? field_bindings[child_index].subcolumn_path
                                            : ""));
                RETURN_IF_ERROR(verify_missing_variant_path(
                        field_bindings[child_index], root_slot.column_id(), index_context.get()));
                ++child_index;
                continue;
            }

            // Not a supported child type
            return Status::InvalidArgument("Unsupported child node type: {}", child->expr_name());
        }
    }

    return Status::OK();
}

bool search_status_allows_row_fallback(const Status& status) {
    DORIS_CHECK(!status.ok());
    return status.is<ErrorCode::INVERTED_INDEX_BYPASS>() ||
           status.is<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>() ||
           status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>() ||
           status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>() ||
           status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>();
}

Status prevent_search_row_fallback(Status status) {
    DORIS_CHECK(!status.ok());
    if (!search_status_allows_row_fallback(status)) {
        return status;
    }
    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
            "SEARCH cannot fall back to row execution: {}", status.to_string());
}

} // namespace

VSearchExpr::VSearchExpr(const TExprNode& node) : VExpr(node) {
    if (node.__isset.search_param) {
        _search_param = node.search_param;
        _original_dsl = _search_param.original_dsl;
    }
}

Status VSearchExpr::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                            VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, row_desc, context));
    const auto& query_options = state->query_options();
    if (query_options.__isset.enable_inverted_index_query_cache) {
        _enable_cache = query_options.enable_inverted_index_query_cache;
    }
    return Status::OK();
}

const std::string& VSearchExpr::expr_name() const {
    static const std::string name = "VSearchExpr";
    return name;
}

Status VSearchExpr::execute_column(VExprContext* context, const Block* block, Selector* selector,
                                   size_t count, ColumnPtr& result_column) const {
    if (fast_execute(context, selector, count, result_column)) {
        return Status::OK();
    }

    return Status::InternalError("SearchExpr should not be executed without inverted index");
}

Status VSearchExpr::evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
    if (_search_param.original_dsl.empty()) {
        return prevent_search_row_fallback(Status::InvalidArgument("search DSL is empty"));
    }

    auto index_context = context->get_index_context();
    if (!index_context) {
        LOG(WARNING) << "VSearchExpr: No inverted index context available";
        return Status::OK();
    }

    SearchInputBundle bundle;
    if (auto status = collect_search_inputs(*this, context, &bundle); !status.ok()) {
        return prevent_search_row_fallback(std::move(status));
    }

    VLOG_DEBUG << "VSearchExpr: bundle.iterators.size()=" << bundle.iterators.size();

    auto index_query_context = index_context->get_index_query_context();

    auto function = std::make_shared<FunctionSearch>();
    auto result_bitmap = InvertedIndexResultBitmap();
    auto status = function->evaluate_inverted_index_with_search_param(
            _search_param, bundle.field_types, bundle.iterators, segment_num_rows, result_bitmap,
            _enable_cache, index_context.get(), bundle.nested_column, index_query_context);

    if (!status.ok()) {
        LOG(WARNING) << "VSearchExpr: Function evaluation failed: " << status.to_string();
        return prevent_search_row_fallback(std::move(status));
    }

    index_context->set_index_result_for_expr(this, result_bitmap);
    for (int column_index : bundle.column_indexes) {
        index_context->set_true_for_index_status(this, column_index);
    }

    return Status::OK();
}

} // namespace doris
