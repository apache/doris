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

    auto index_context = context->get_inverted_index_context();
    if (index_context == nullptr) {
        return Status::OK();
    }

    for (const auto& child : expr.children()) {
        if (child->is_slot_ref()) {
            auto* column_slot_ref = assert_cast<VSlotRef*>(child.get());
            int column_id = column_slot_ref->column_id();
            auto* iterator = index_context->get_inverted_index_iterator_by_column_id(column_id);
            if (iterator == nullptr) {
                continue;
            }

            const auto* storage_name_type =
                    index_context->get_storage_name_and_type_by_column_id(column_id);
            if (storage_name_type == nullptr) {
                auto err_msg = fmt::format(
                        "storage_name_type cannot be found for column {} while in {} evaluate",
                        column_id, expr.expr_name());
                LOG(ERROR) << err_msg;
                return Status::InternalError(err_msg);
            }

            auto column_name = column_slot_ref->column_name();
            bundle->iterators.emplace(column_name, iterator);
            bundle->field_types.emplace(column_name, *storage_name_type);
            bundle->column_ids.emplace_back(column_id);
        } else if (child->is_literal()) {
            auto* literal = assert_cast<VLiteral*>(child.get());
            bundle->literal_args.emplace_back(literal->get_column_ptr(), literal->get_data_type(),
                                              literal->expr_name());
        } else {
            LOG(WARNING) << "VSearchExpr: Unsupported child node type encountered";
            return Status::InvalidArgument("search expression child type unsupported");
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

    LOG(INFO) << "VSearchExpr constructor: dsl='" << _original_dsl
              << "', num_children=" << node.num_children
              << ", has_search_param=" << node.__isset.search_param
              << ", children_size=" << _children.size();

    for (size_t i = 0; i < _children.size(); i++) {
        LOG(INFO) << "VSearchExpr constructor: child[" << i
                  << "] expr_name=" << _children[i]->expr_name();
    }
}

const std::string& VSearchExpr::expr_name() const {
    static const std::string name = "VSearchExpr";
    return name;
}

Status VSearchExpr::execute(VExprContext* context, Block* block, int* result_column_id) {
    if (fast_execute(context, block, result_column_id)) {
        return Status::OK();
    }

    return Status::InternalError("SearchExpr should not be executed without inverted index");
}

Status VSearchExpr::evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
    LOG(INFO) << "VSearchExpr::evaluate_inverted_index called with DSL: " << _original_dsl;

    if (_search_param.original_dsl.empty()) {
        return Status::InvalidArgument("search DSL is empty");
    }

    auto index_context = context->get_inverted_index_context();
    if (!index_context) {
        LOG(WARNING) << "VSearchExpr: No inverted index context available";
        return Status::OK();
    }

    SearchInputBundle bundle;
    RETURN_IF_ERROR(collect_search_inputs(*this, context, &bundle));

    if (bundle.iterators.empty()) {
        LOG(WARNING) << "VSearchExpr: No indexed columns available for evaluation";
        return Status::OK();
    }

    auto function = std::make_shared<FunctionSearch>();
    auto result_bitmap = InvertedIndexResultBitmap();
    auto status = function->evaluate_inverted_index_with_search_param(
            _search_param, bundle.field_types, bundle.iterators, segment_num_rows, result_bitmap);

    if (!status.ok()) {
        LOG(WARNING) << "VSearchExpr: Function evaluation failed: " << status.to_string();
        return status;
    }

    index_context->set_inverted_index_result_for_expr(this, result_bitmap);
    for (int column_id : bundle.column_ids) {
        index_context->set_true_for_inverted_index_status(this, column_id);
    }

    const auto& data_bitmap = result_bitmap.get_data_bitmap();
    const uint64_t match_count = data_bitmap ? data_bitmap->cardinality() : 0;
    if (match_count > 0) {
        LOG(INFO) << "VSearchExpr: Found " << match_count
                  << " matching rows for DSL: " << _search_param.original_dsl;
    } else {
        LOG(INFO) << "VSearchExpr: No matches found for DSL: " << _search_param.original_dsl;
    }

    return Status::OK();
}

} // namespace doris::vectorized
