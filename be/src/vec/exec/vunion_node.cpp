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

#include "vec/exec/vunion_node.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <functional>
#include <memory>
#include <ostream>
#include <string>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class DescriptorTbl;
class ObjectPool;

namespace vectorized {

VUnionNode::VUnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _first_materialized_child_idx(tnode.union_node.first_materialized_child_idx),
          _const_expr_list_idx(0),
          _child_idx(0),
          _child_row_idx(0),
          _child_eos(false),
          _to_close_child_idx(-1) {}

Status VUnionNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.union_node);
    // Create const_expr_ctx_lists_ from thrift exprs.
    const auto& const_texpr_lists = tnode.union_node.const_expr_lists;
    for (const auto& texprs : const_texpr_lists) {
        VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(texprs, ctxs));
        _const_expr_lists.push_back(ctxs);
    }
    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& result_texpr_lists = tnode.union_node.result_expr_lists;
    for (auto& texprs : result_texpr_lists) {
        VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(texprs, ctxs));
        _child_expr_lists.push_back(ctxs);
    }
    return Status::OK();
}

Status VUnionNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _materialize_exprs_evaluate_timer =
            ADD_TIMER(_runtime_profile, "MaterializeExprsEvaluateTimer");
    // Prepare const expr lists.
    for (const VExprContextSPtrs& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(VExpr::prepare(exprs, state, _row_descriptor));
    }

    // Prepare result expr lists.
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(VExpr::prepare(_child_expr_lists[i], state, child(i)->row_desc()));
    }
    return Status::OK();
}

Status VUnionNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(alloc_resource(state));
    // Ensures that rows are available for clients to fetch after this open() has
    // succeeded.
    if (!_children.empty()) {
        RETURN_IF_ERROR(child(_child_idx)->open(state));
    }
    return Status::OK();
}

Status VUnionNode::alloc_resource(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    std::unique_lock<std::mutex> l(_resource_lock);
    if (_resource_allocated) {
        return Status::OK();
    }

    // open const expr lists.
    for (const auto& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }
    // open result expr lists.
    for (const auto& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    _resource_allocated = true;
    return Status::OK();
}

Status VUnionNode::get_next_pass_through(RuntimeState* state, Block* block) {
    DCHECK(!reached_limit());
    DCHECK(!is_in_subplan());
    DCHECK_LT(_child_idx, _children.size());
    DCHECK(is_child_passthrough(_child_idx));
    if (_child_eos) {
        RETURN_IF_ERROR(child(_child_idx)->open(state));
        _child_eos = false;
    }
    DCHECK_EQ(block->rows(), 0);
    RETURN_IF_ERROR(child(_child_idx)
                            ->get_next_after_projects(
                                    state, block, &_child_eos,
                                    std::bind((Status(ExecNode::*)(RuntimeState*,
                                                                   vectorized::Block*, bool*)) &
                                                      ExecNode::get_next,
                                              _children[_child_idx], std::placeholders::_1,
                                              std::placeholders::_2, std::placeholders::_3)));
    if (_child_eos) {
        // Even though the child is at eos, it's not OK to close() it here. Once we close
        // the child, the row batches that it produced are invalid. Marking the batch as
        // needing a deep copy let's us safely close the child in the next get_next() call.
        // TODO: Remove this as part of IMPALA-4179.
        _to_close_child_idx = _child_idx;
        ++_child_idx;
    }
    return Status::OK();
}

Status VUnionNode::get_next_materialized(RuntimeState* state, Block* block) {
    // Fetch from children, evaluate corresponding exprs and materialize.
    DCHECK(!reached_limit());
    DCHECK_LT(_child_idx, _children.size());

    MutableBlock mblock = VectorizedUtils::build_mutable_mem_reuse_block(block, _row_descriptor);

    Block child_block;
    while (has_more_materialized() && mblock.rows() <= state->batch_size()) {
        // The loop runs until we are either done iterating over the children that require
        // materialization, or the row batch is at capacity.
        DCHECK(!is_child_passthrough(_child_idx));
        // Child row batch was either never set or we're moving on to a different child.
        DCHECK_LT(_child_idx, _children.size());
        // open the current child unless it's the first child, which was already opened in
        // VUnionNode::open().
        if (_child_eos) {
            RETURN_IF_ERROR(child(_child_idx)->open(state));
            _child_eos = false;
            _child_row_idx = 0;
        }
        // Here need materialize block of child block, so here so not mem_reuse
        child_block.clear();
        // The first batch from each child is always fetched here.
        RETURN_IF_ERROR(child(_child_idx)
                                ->get_next_after_projects(
                                        state, &child_block, &_child_eos,
                                        std::bind((Status(ExecNode::*)(RuntimeState*,
                                                                       vectorized::Block*, bool*)) &
                                                          ExecNode::get_next,
                                                  _children[_child_idx], std::placeholders::_1,
                                                  std::placeholders::_2, std::placeholders::_3)));
        SCOPED_TIMER(_materialize_exprs_evaluate_timer);
        if (child_block.rows() > 0) {
            Block res;
            RETURN_IF_ERROR(materialize_block(&child_block, _child_idx, &res));
            RETURN_IF_ERROR(mblock.merge(res));
        }
        // It shouldn't be the case that we reached the limit because we shouldn't have
        // incremented '_num_rows_returned' yet.
        DCHECK(!reached_limit());
        if (_child_eos) {
            // Unless we are inside a subplan expecting to call open()/get_next() on the child
            // again, the child can be closed at this point.
            // TODO: Recheck whether is_in_subplan() is right
            //            if (!is_in_subplan()) {
            //                child(_child_idx)->close(state);
            //            }
            ++_child_idx;
        }
    }

    DCHECK_LE(_child_idx, _children.size());
    return Status::OK();
}

Status VUnionNode::get_next_const(RuntimeState* state, Block* block) {
    DCHECK_EQ(state->per_fragment_instance_idx(), 0);
    DCHECK_LT(_const_expr_list_idx, _const_expr_lists.size());

    MutableBlock mblock = VectorizedUtils::build_mutable_mem_reuse_block(block, _row_descriptor);
    for (; _const_expr_list_idx < _const_expr_lists.size() && mblock.rows() <= state->batch_size();
         ++_const_expr_list_idx) {
        Block tmp_block;
        tmp_block.insert({vectorized::ColumnUInt8::create(1),
                          std::make_shared<vectorized::DataTypeUInt8>(), ""});
        int const_expr_lists_size = _const_expr_lists[_const_expr_list_idx].size();
        std::vector<int> result_list(const_expr_lists_size);
        for (size_t i = 0; i < const_expr_lists_size; ++i) {
            RETURN_IF_ERROR(_const_expr_lists[_const_expr_list_idx][i]->execute(&tmp_block,
                                                                                &result_list[i]));
        }
        tmp_block.erase_not_in(result_list);
        if (tmp_block.rows() > 0) {
            RETURN_IF_ERROR(mblock.merge(tmp_block));
            tmp_block.clear();
        }
    }

    // some insert query like "insert into string_test select 1, repeat('a', 1024 * 1024);"
    // the const expr will be in output expr cause the union node return a empty block. so here we
    // need add one row to make sure the union node exec const expr return at least one row
    if (block->rows() == 0) {
        block->insert({vectorized::ColumnUInt8::create(1),
                       std::make_shared<vectorized::DataTypeUInt8>(), ""});
    }
    return Status::OK();
}

//for pipeline operator
Status VUnionNode::materialize_child_block(RuntimeState* state, int child_id,
                                           vectorized::Block* input_block,
                                           vectorized::Block* output_block) {
    DCHECK_LT(child_id, _children.size());
    DCHECK(!is_child_passthrough(child_id));
    if (input_block->rows() > 0) {
        MutableBlock mblock =
                VectorizedUtils::build_mutable_mem_reuse_block(output_block, _row_descriptor);
        Block res;
        RETURN_IF_ERROR(materialize_block(input_block, child_id, &res));
        RETURN_IF_ERROR(mblock.merge(res));
    }
    return Status::OK();
}

Status VUnionNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    // TODO: Rethink the logic, which cause close the exec node twice.
    if (_to_close_child_idx != -1) {
        // The previous child needs to be closed if passthrough was enabled for it. In the non
        // passthrough case, the child was already closed in the previous call to get_next().
        DCHECK(is_child_passthrough(_to_close_child_idx));
        DCHECK(!is_in_subplan());
        static_cast<void>(child(_to_close_child_idx)->close(state));
        _to_close_child_idx = -1;
    }

    // Save the number of rows in case get_next() is called with a non-empty batch, which can
    // happen in a subplan.
    if (has_more_passthrough()) {
        RETURN_IF_ERROR(get_next_pass_through(state, block));
    } else if (has_more_materialized()) {
        RETURN_IF_ERROR(get_next_materialized(state, block));
    } else if (has_more_const(state)) {
        RETURN_IF_ERROR(get_next_const(state, block));
    }
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block, block->columns()));

    *eos = (!has_more_passthrough() && !has_more_materialized() && !has_more_const(state));
    reached_limit(block, eos);

    return Status::OK();
}

Status VUnionNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    release_resource(state);
    return ExecNode::close(state);
}

void VUnionNode::release_resource(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    return ExecNode::release_resource(state);
}

void VUnionNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "_union(_first_materialized_child_idx=" << _first_materialized_child_idx
         << " _child_expr_lists=[";
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        *out << VExpr::debug_string(_child_expr_lists[i]) << ", ";
    }
    *out << "] \n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")" << std::endl;
}

Status VUnionNode::materialize_block(Block* src_block, int child_idx, Block* res_block) {
    const auto& child_exprs = _child_expr_lists[child_idx];
    ColumnsWithTypeAndName colunms;
    for (size_t i = 0; i < child_exprs.size(); ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(child_exprs[i]->execute(src_block, &result_column_id));
        colunms.emplace_back(src_block->get_by_position(result_column_id));
    }
    _child_row_idx += src_block->rows();
    *res_block = {colunms};
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
