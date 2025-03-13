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

#include "pipeline/exec/union_source_operator.h"

#include <algorithm>
#include <functional>
#include <utility>

#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "runtime/descriptors.h"
#include "util/defer_op.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

Status UnionSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<Parent>();
    if (p.get_child_count() != 0) {
        ((UnionSharedState*)_dependency->shared_state())
                ->data_queue.set_source_dependency(_shared_state->source_deps.front());
    } else {
        _only_const_dependency = Dependency::create_shared(
                _parent->operator_id(), _parent->node_id(), _parent->get_name() + "_DEPENDENCY");
        _dependency = _only_const_dependency.get();
        _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                _runtime_profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
        _dependency->set_ready();
    }

    return Status::OK();
}

Status UnionSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    for (const auto& texprs : tnode.union_node.const_expr_lists) {
        vectorized::VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texprs, ctxs));
        _const_expr_lists.push_back(ctxs);
    }
    if (!std::ranges::all_of(_const_expr_lists, [&](const auto& exprs) {
            return exprs.size() == _const_expr_lists.front().size();
        })) {
        return Status::InternalError("Const expr lists size not match");
    }
    return Status::OK();
}

Status UnionSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    for (const vectorized::VExprContextSPtrs& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(exprs, state, _row_descriptor));
    }
    for (const auto& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(vectorized::VExpr::open(exprs, state));
    }
    return Status::OK();
}

std::string UnionSourceLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    if (_shared_state) {
        fmt::format_to(debug_string_buffer, ", data_queue: (is_all_finish = {}, has_data = {})",
                       _shared_state->data_queue.is_all_finish(),
                       _shared_state->data_queue.has_more_data());
    }
    return fmt::to_string(debug_string_buffer);
}

Status UnionSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    Defer set_eos {[&]() {
        // the eos check of union operator is complex, need check all logical if you want modify
        // could ref this PR: https://github.com/apache/doris/pull/29677
        // have executing const expr, queue have no data anymore, and child could be closed
        if (_child_size == 0 && !local_state._need_read_for_const_expr) {
            *eos = true;
        } else if (has_data(state)) {
            *eos = false;
        } else if (local_state._shared_state->data_queue.is_all_finish()) {
            // Here, check the value of `_has_data(state)` again after `data_queue.is_all_finish()` is TRUE
            // as there may be one or more blocks when `data_queue.is_all_finish()` is TRUE.
            *eos = !has_data(state);
        } else {
            *eos = false;
        }
    }};

    SCOPED_TIMER(local_state.exec_time_counter());
    if (local_state._need_read_for_const_expr) {
        if (has_more_const(state)) {
            RETURN_IF_ERROR(get_next_const(state, block));
        }
        local_state._need_read_for_const_expr = has_more_const(state);
    } else if (_child_size != 0) {
        std::unique_ptr<vectorized::Block> output_block;
        int child_idx = 0;
        RETURN_IF_ERROR(local_state._shared_state->data_queue.get_block_from_queue(&output_block,
                                                                                   &child_idx));
        if (!output_block) {
            return Status::OK();
        }
        block->swap(*output_block);
        output_block->clear_column_data(_row_descriptor.num_materialized_slots());
        local_state._shared_state->data_queue.push_free_block(std::move(output_block), child_idx);
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}

Status UnionSourceOperatorX::get_next_const(RuntimeState* state, vectorized::Block* block) {
    DCHECK_EQ(state->per_fragment_instance_idx(), 0);
    auto& local_state = get_local_state(state);
    DCHECK_LT(local_state._const_expr_list_idx, _const_expr_lists.size());

    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);

    auto& const_expr_list_idx = local_state._const_expr_list_idx;
    vectorized::MutableBlock mutable_block =
            vectorized::VectorizedUtils::build_mutable_mem_reuse_block(block, _row_descriptor);
    for (; const_expr_list_idx < _const_expr_lists.size() &&
           mutable_block.rows() < state->batch_size();
         ++const_expr_list_idx) {
        vectorized::Block tmp_block;
        // When we execute a constant expression, we need one row of data because the expr may use the block's rows for some judgments
        tmp_block.insert({vectorized::ColumnUInt8::create(1),
                          std::make_shared<vectorized::DataTypeUInt8>(), ""});
        vectorized::ColumnsWithTypeAndName colunms;
        for (auto& expr : _const_expr_lists[const_expr_list_idx]) {
            int result_column_id = -1;
            RETURN_IF_ERROR(expr->execute(&tmp_block, &result_column_id));
            colunms.emplace_back(tmp_block.get_by_position(result_column_id));
        }
        RETURN_IF_ERROR(mutable_block.merge(vectorized::Block {colunms}));
    }

    // some insert query like "insert into string_test select 1, repeat('a', 1024 * 1024);"
    // the const expr will be in output expr cause the union node return a empty block. so here we
    // need add one row to make sure the union node exec const expr return at least one row
    /// TODO: maybe we can remove this
    if (block->rows() == 0) {
        block->insert({vectorized::ColumnUInt8::create(1),
                       std::make_shared<vectorized::DataTypeUInt8>(), ""});
    }
    return Status::OK();
}

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
