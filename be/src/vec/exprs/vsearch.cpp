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

#include "vec/exprs/vsearch.h"

#include <memory>
#include <roaring/roaring.hh>

#include "common/logging.h"
#include "common/status.h"
#include "glog/logging.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/function_search.h"

namespace doris::vectorized {
using namespace segment_v2;

namespace {

struct SearchInputBundle {
    std::unordered_map<std::string, IndexIterator*> iterators;
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> field_types;
    std::vector<int> column_ids;
    vectorized::ColumnsWithTypeAndName literal_args;
};

Status collect_search_inputs(const VSearchExpr& expr, VExprContext* context,
                             SearchInputBundle* bundle) {
    DCHECK(bundle != nullptr);

    auto index_context = context->get_index_context();
    if (index_context == nullptr) {
        LOG(WARNING) << "collect_search_inputs: No inverted index context available";
        return Status::InternalError("No inverted index context available");
    }

    // Get field bindings for variant subcolumn support
    const auto& search_param = expr.get_search_param();
    const auto& field_bindings = search_param.field_bindings;

    int child_index = 0; // Index for iterating through children
    for (const auto& child : expr.children()) {
        if (child->is_slot_ref()) {
            auto* column_slot_ref = assert_cast<VSlotRef*>(child.get());
            int column_id = column_slot_ref->column_id();
            auto* iterator = index_context->get_inverted_index_iterator_by_column_id(column_id);

            // Determine the field_name from field_bindings (for variant subcolumns)
            // field_bindings and children should have the same order
            std::string field_name;
            if (child_index < field_bindings.size()) {
                // Use field_name from binding (may include "parent.subcolumn" for variant)
                field_name = field_bindings[child_index].field_name;
            } else {
                // Fallback to column_name if binding not found
                field_name = column_slot_ref->column_name();
            }

            // Only collect fields that have iterators (materialized columns with indexes)
            if (iterator != nullptr) {
                const auto* storage_name_type =
                        index_context->get_storage_name_and_type_by_column_id(column_id);
                if (storage_name_type == nullptr) {
                    return Status::InternalError("storage_name_type not found for column {} in {}",
                                                 column_id, expr.expr_name());
                }

                bundle->iterators.emplace(field_name, iterator);
                bundle->field_types.emplace(field_name, *storage_name_type);
                bundle->column_ids.emplace_back(column_id);
            }

            child_index++;
        } else if (child->is_literal()) {
            auto* literal = assert_cast<VLiteral*>(child.get());
            bundle->literal_args.emplace_back(literal->get_column_ptr(), literal->get_data_type(),
                                              literal->expr_name());
        } else {
            // Check if this is ElementAt expression (for variant subcolumn access)
            if (child->expr_name() == "element_at" && child_index < field_bindings.size() &&
                field_bindings[child_index].__isset.is_variant_subcolumn &&
                field_bindings[child_index].is_variant_subcolumn) {
                // Variant subcolumn not materialized - skip, will create empty BitSetQuery in function_search
                child_index++;
                continue;
            }

            // Not a supported child type
            return Status::InvalidArgument("Unsupported child node type: {}", child->expr_name());
        }
    }

    return Status::OK();
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
    LOG(INFO) << "VSearchExpr::evaluate_inverted_index called, DSL: " << _search_param.original_dsl;

    if (_search_param.original_dsl.empty()) {
        return Status::InvalidArgument("search DSL is empty");
    }

    auto index_context = context->get_index_context();
    if (!index_context) {
        LOG(WARNING) << "VSearchExpr: No inverted index context available";
        return Status::OK();
    }

    SearchInputBundle bundle;
    RETURN_IF_ERROR(collect_search_inputs(*this, context, &bundle));

    VLOG_DEBUG << "VSearchExpr: bundle.iterators.size()=" << bundle.iterators.size();

    if (bundle.iterators.empty()) {
        LOG(WARNING) << "VSearchExpr: No indexed columns available for evaluation, DSL: "
                     << _original_dsl;
        auto empty_bitmap = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                      std::make_shared<roaring::Roaring>());
        index_context->set_index_result_for_expr(this, std::move(empty_bitmap));
        return Status::OK();
    }

    auto function = std::make_shared<FunctionSearch>();
    auto result_bitmap = InvertedIndexResultBitmap();
    auto status = function->evaluate_inverted_index_with_search_param(
            _search_param, bundle.field_types, bundle.iterators, segment_num_rows, result_bitmap,
            _enable_cache);

    if (!status.ok()) {
        LOG(WARNING) << "VSearchExpr: Function evaluation failed: " << status.to_string();
        return status;
    }

    index_context->set_index_result_for_expr(this, result_bitmap);
    for (int column_id : bundle.column_ids) {
        index_context->set_true_for_index_status(this, column_id);
    }

    return Status::OK();
}

} // namespace doris::vectorized
