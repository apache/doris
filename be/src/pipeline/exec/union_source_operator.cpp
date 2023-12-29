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
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "runtime/descriptors.h"
#include "vec/core/block.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

UnionSourceOperatorBuilder::UnionSourceOperatorBuilder(int32_t id, ExecNode* node,
                                                       std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "UnionSourceOperator", node), _data_queue(queue) {};

OperatorPtr UnionSourceOperatorBuilder::build_operator() {
    return std::make_shared<UnionSourceOperator>(this, _node, _data_queue);
}

UnionSourceOperator::UnionSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* node,
                                         std::shared_ptr<DataQueue> queue)
        : SourceOperator(operator_builder, node),
          _data_queue(queue),
          _need_read_for_const_expr(true) {};

bool UnionSourceOperator::_has_data() {
    return _need_read_for_const_expr || _data_queue->remaining_has_data();
}

// we assumed it can read to process const expr， Although we don't know whether there is
// ,and queue have data, could read also
bool UnionSourceOperator::can_read() {
    return _has_data() || _data_queue->is_all_finish();
}

Status UnionSourceOperator::pull_data(RuntimeState* state, vectorized::Block* block, bool* eos) {
    // here we precess const expr firstly
    if (_need_read_for_const_expr) {
        if (_node->has_more_const(state)) {
            static_cast<void>(_node->get_next_const(state, block));
        }
        _need_read_for_const_expr = _node->has_more_const(state);
    } else {
        std::unique_ptr<vectorized::Block> output_block;
        int child_idx = 0;
        static_cast<void>(_data_queue->get_block_from_queue(&output_block, &child_idx));
        if (!output_block) {
            return Status::OK();
        }
        block->swap(*output_block);
        output_block->clear_column_data(_node->intermediate_row_desc().num_materialized_slots());
        _data_queue->push_free_block(std::move(output_block), child_idx);
    }

    _node->reached_limit(block, eos);
    return Status::OK();
}

Status UnionSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    bool eos = false;
    RETURN_IF_ERROR(_node->get_next_after_projects(
            state, block, &eos,
            std::bind(&UnionSourceOperator::pull_data, this, std::placeholders::_1,
                      std::placeholders::_2, std::placeholders::_3)));
    //have exectue const expr, queue have no data any more, and child could be colsed
    if (eos || (!_has_data() && _data_queue->is_all_finish())) {
        source_state = SourceState::FINISHED;
    } else if (_has_data()) {
        source_state = SourceState::MORE_DATA;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }

    return Status::OK();
}

Status UnionSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    auto& p = _parent->cast<Parent>();
    int child_count = p.get_child_count();
    auto ss = create_shared_state();
    if (child_count != 0) {
        auto& deps = info.upstream_dependencies;
        for (auto& dep : deps) {
            ((UnionSinkDependency*)dep.get())->set_shared_state(ss);
        }
    } else {
        auto& deps = info.upstream_dependencies;
        DCHECK(child_count == 0);
        DCHECK(deps.size() == 1);
        DCHECK(deps.front() == nullptr);
        //child_count == 0 , we need to creat a  UnionDependency
        deps.front() = std::make_shared<UnionSourceDependency>(
                _parent->operator_id(), _parent->node_id(), state->get_query_ctx());
        ((UnionSourceDependency*)deps.front().get())->set_shared_state(ss);
    }
    RETURN_IF_ERROR(Base::init(state, info));
    ss->data_queue.set_source_dependency(info.dependency);
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
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
    if (child_count == 0) {
        _dependency->set_ready();
    }
    return Status::OK();
}

std::shared_ptr<UnionSharedState> UnionSourceLocalState::create_shared_state() {
    auto& p = _parent->cast<Parent>();
    std::shared_ptr<UnionSharedState> data_queue =
            std::make_shared<UnionSharedState>(p._child_size);
    return data_queue;
}

std::string UnionSourceLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    fmt::format_to(debug_string_buffer, ", data_queue: (is_all_finish = {}, has_data = {})",
                   _shared_state->data_queue.is_all_finish(),
                   _shared_state->data_queue.remaining_has_data());
    return fmt::to_string(debug_string_buffer);
}

Status UnionSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                       SourceState& source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (local_state._need_read_for_const_expr) {
        if (has_more_const(state)) {
            RETURN_IF_ERROR(get_next_const(state, block));
        }
        local_state._need_read_for_const_expr = has_more_const(state);
    } else {
        std::unique_ptr<vectorized::Block> output_block = vectorized::Block::create_unique();
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
    local_state.reached_limit(block, source_state);
    //have exectue const expr, queue have no data any more, and child could be colsed
    if (_child_size == 0 && !local_state._need_read_for_const_expr) {
        source_state = SourceState::FINISHED;
    } else if ((!_has_data(state) && local_state._shared_state->data_queue.is_all_finish())) {
        source_state = SourceState::FINISHED;
    } else if (_has_data(state)) {
        source_state = SourceState::MORE_DATA;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }
    return Status::OK();
}

Status UnionSourceOperatorX::get_next_const(RuntimeState* state, vectorized::Block* block) {
    DCHECK_EQ(state->per_fragment_instance_idx(), 0);
    auto& local_state = state->get_local_state(operator_id())->cast<UnionSourceLocalState>();
    DCHECK_LT(local_state._const_expr_list_idx, _const_expr_lists.size());
    auto& _const_expr_list_idx = local_state._const_expr_list_idx;
    vectorized::MutableBlock mblock =
            vectorized::VectorizedUtils::build_mutable_mem_reuse_block(block, _row_descriptor);
    for (; _const_expr_list_idx < _const_expr_lists.size() && mblock.rows() <= state->batch_size();
         ++_const_expr_list_idx) {
        vectorized::Block tmp_block;
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

} // namespace pipeline
} // namespace doris
