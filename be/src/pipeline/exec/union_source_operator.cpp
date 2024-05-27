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
    }

    if (p.get_child_count() == 0) {
        _dependency->set_ready();
    }
    return Status::OK();
}

Status UnionSourceLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    auto& p = _parent->cast<Parent>();
    // Const exprs materialized by this node. These exprs don't refer to any children.
    // Only materialized by the first fragment instance to avoid duplication.
    if (state->per_fragment_instance_idx() == 0) {
        auto clone_expr_list = [&](vectorized::VExprContextSPtrs& cur_expr_list,
                                   vectorized::VExprContextSPtrs& other_expr_list) {
            cur_expr_list.resize(other_expr_list.size());
            for (int i = 0; i < cur_expr_list.size(); i++) {
                RETURN_IF_ERROR(other_expr_list[i]->clone(state, cur_expr_list[i]));
            }
            return Status::OK();
        };
        _const_expr_lists.resize(p._const_expr_lists.size());
        for (int i = 0; i < _const_expr_lists.size(); i++) {
            auto& _const_expr_list = _const_expr_lists[i];
            auto& other_expr_list = p._const_expr_lists[i];
            RETURN_IF_ERROR(clone_expr_list(_const_expr_list, other_expr_list));
        }
    }

    return Status::OK();
}

std::string UnionSourceLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    if (_shared_state) {
        fmt::format_to(debug_string_buffer, ", data_queue: (is_all_finish = {}, has_data = {})",
                       _shared_state->data_queue.is_all_finish(),
                       _shared_state->data_queue.remaining_has_data());
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
        } else if (_has_data(state)) {
            *eos = false;
        } else if (local_state._shared_state->data_queue.is_all_finish()) {
            // Here, check the value of `_has_data(state)` again after `data_queue.is_all_finish()` is TRUE
            // as there may be one or more blocks when `data_queue.is_all_finish()` is TRUE.
            *eos = !_has_data(state);
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
    auto& local_state = state->get_local_state(operator_id())->cast<UnionSourceLocalState>();
    DCHECK_LT(local_state._const_expr_list_idx, _const_expr_lists.size());
    auto& _const_expr_list_idx = local_state._const_expr_list_idx;
    vectorized::MutableBlock mblock =
            vectorized::VectorizedUtils::build_mutable_mem_reuse_block(block, _row_descriptor);
    for (; _const_expr_list_idx < _const_expr_lists.size() && mblock.rows() < state->batch_size();
         ++_const_expr_list_idx) {
        vectorized::Block tmp_block;
        tmp_block.insert({vectorized::ColumnUInt8::create(1),
                          std::make_shared<vectorized::DataTypeUInt8>(), ""});
        int const_expr_lists_size = _const_expr_lists[_const_expr_list_idx].size();
        if (_const_expr_list_idx && const_expr_lists_size != _const_expr_lists[0].size()) {
            return Status::InternalError(
                    "[UnionNode]const expr at {}'s count({}) not matched({} expected)",
                    _const_expr_list_idx, const_expr_lists_size, _const_expr_lists[0].size());
        }

        std::vector<int> result_list(const_expr_lists_size);
        for (size_t i = 0; i < const_expr_lists_size; ++i) {
            RETURN_IF_ERROR(_const_expr_lists[_const_expr_list_idx][i]->execute(&tmp_block,
                                                                                &result_list[i]));
        }
        tmp_block.erase_not_in(result_list);
        if (tmp_block.columns() != mblock.columns()) {
            return Status::InternalError(
                    "[UnionNode]columns count of const expr block not matched ({} vs {})",
                    tmp_block.columns(), mblock.columns());
        }
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

} // namespace pipeline
} // namespace doris
