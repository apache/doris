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

#include "vec/exprs/vsearch_expr.h"

#include <memory>
#include <roaring/roaring.hh>

#include "common/logging.h"
#include "common/status.h"
#include "glog/logging.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/function_search.h"

namespace doris::vectorized {

VSearchExpr::VSearchExpr(const TExprNode& node) : VExpr(node) {
    if (node.__isset.search_param) {
        _search_param = node.search_param;
        _original_dsl = _search_param.original_dsl;
    }

    // DEBUG: Print thrift structure received from FE
    LOG(INFO) << "VSearchExpr constructor: dsl='" << _original_dsl
              << "', num_children=" << node.num_children
              << ", has_search_param=" << node.__isset.search_param
              << ", children_size=" << _children.size();

    for (size_t i = 0; i < _children.size(); i++) {
        LOG(INFO) << "VSearchExpr constructor: child[" << i
                  << "] expr_name=" << _children[i]->expr_name();
    }
}

// Return the name of the expression as a static string to avoid returning a reference to a local variable
const std::string& VSearchExpr::expr_name() const {
    static const std::string name = "VSearchExpr";
    return name;
}

Status VSearchExpr::execute(VExprContext* context, Block* block, int* result_column_id) {
    // query_string expressions should only be evaluated via inverted index
    return Status::InternalError(
            "VSearchExpr::execute should not be called - use inverted index evaluation");
}

Status VSearchExpr::evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
    LOG(INFO) << "VSearchExpr::evaluate_inverted_index called with DSL: " << _original_dsl;

    // Validate Thrift parameter (no __isset check needed for required fields)
    if (_search_param.original_dsl.empty()) {
        return Status::InvalidArgument("query_string DSL is empty");
    }

    // Get inverted index context
    auto index_context = context->get_inverted_index_context();
    if (!index_context) {
        LOG(WARNING) << "VSearchExpr: No inverted index context available";
        return Status::OK();
    }

    // Collect inverted index iterators and column information based on field bindings
    std::unordered_map<std::string, segment_v2::IndexIterator*> iterators;
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> data_type_with_names;
    std::vector<int> column_ids;
    // Prepare arguments for function evaluation
    vectorized::ColumnsWithTypeAndName arguments;

    LOG(INFO) << "VSearchExpr: Processing " << _search_param.field_bindings.size()
              << " field bindings";

    for (const auto& child : children()) {
        if (child->is_slot_ref()) {
            auto* column_slot_ref = assert_cast<VSlotRef*>(child.get());
            auto column_id = column_slot_ref->column_id();
            auto* iter = index_context->get_inverted_index_iterator_by_column_id(column_id);
            //column does not have inverted index
            if (iter == nullptr) {
                continue;
            }
            const auto* storage_name_type =
                    index_context->get_storage_name_and_type_by_column_id(column_id);
            if (storage_name_type == nullptr) {
                auto err_msg = fmt::format(
                        "storage_name_type cannot be found for column {} while in {} "
                        "evaluate_inverted_index",
                        column_id, expr_name());
                LOG(ERROR) << err_msg;
                return Status::InternalError(err_msg);
            }
            auto column_name = column_slot_ref->column_name();
            iterators.emplace(column_name, iter);
            data_type_with_names.emplace(column_name, *storage_name_type);
            column_ids.emplace_back(column_id);
        } else if (child->is_literal()) {
            auto* column_literal = assert_cast<VLiteral*>(child.get());
            arguments.emplace_back(column_literal->get_column_ptr(),
                                   column_literal->get_data_type(), column_literal->expr_name());
        } else {
            return Status::OK(); // others cases
        }
    }

    if (iterators.empty()) {
        LOG(WARNING) << "VSearchExpr: No indexed columns available for evaluation";
        return Status::OK();
    }

    // Create FunctionSearch to handle the actual inverted index evaluation
    // This reuses the existing logic from function_search.cpp
    auto function = std::make_shared<FunctionSearch>();

    // Use the new method that handles structured TQueryStringParam
    auto result_bitmap = segment_v2::InvertedIndexResultBitmap();
    auto status = function->evaluate_inverted_index_with_search_param(
            _search_param, data_type_with_names, iterators, segment_num_rows, result_bitmap);

    if (!status.ok()) {
        LOG(WARNING) << "VSearchExpr: Function evaluation failed: " << status.to_string();
        return status;
    }

    // Store results in index context if we have matches
    if (!result_bitmap.is_empty()) {
        index_context->set_inverted_index_result_for_expr(this, result_bitmap);
        for (int column_id : column_ids) {
            index_context->set_true_for_inverted_index_status(this, column_id);
        }
        LOG(INFO) << "VSearchExpr: Found " << result_bitmap.get_data_bitmap()->cardinality()
                  << " matching rows for DSL: " << _search_param.original_dsl;
    } else {
        LOG(INFO) << "VSearchExpr: No matches found for DSL: " << _search_param.original_dsl;
    }

    return Status::OK();
}

} // namespace doris::vectorized
