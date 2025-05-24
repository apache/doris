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

#include "local_merge_sort_source_operator.h"

#include <glog/logging.h>

#include <string>

#include "pipeline/exec/operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

LocalMergeSortLocalState::LocalMergeSortLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<SortSharedState>(state, parent) {}

Status LocalMergeSortLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    _task_idx = info.task_idx;
    return Status::OK();
}

// The dependency process of LocalMergeSort:
// 1. The main_source's dependencies include its own source dependency and all other_source_deps (created in LocalMergeSortSourceOperatorX).
// 2. The other_source's dependencies include only its own source dependency.
// 3. The sort sink sets the corresponding source dependency to ready.
// 4. At this point, other_source will execute, but main_source will not.
// 5. After other_source executes, it sets the corresponding other_source_deps to ready.
// 6. Now, main_source will execute.
// You can check the simulated process in local_merge_sort_source_operator_test.cpp
std::vector<Dependency*> LocalMergeSortLocalState::dependencies() const {
    if (_task_idx == 0) {
        std::vector<Dependency*> deps;
        deps.push_back(_dependency);
        auto& p = _parent->cast<LocalMergeSortSourceOperatorX>();
        for (int i = 1; i < p._parallel_tasks; ++i) {
            auto dep = p._other_source_deps[i];
            deps.push_back(dep.get());
        }
        return deps;
    } else {
        return Base::dependencies();
    }
}

Status LocalMergeSortLocalState::build_merger(RuntimeState* state) {
    auto& p = _parent->cast<LocalMergeSortSourceOperatorX>();
    vectorized::VExprContextSPtrs ordering_expr_ctxs;
    ordering_expr_ctxs.resize(p._vsort_exec_exprs.ordering_expr_ctxs().size());
    for (size_t i = 0; i < ordering_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                p._vsort_exec_exprs.ordering_expr_ctxs()[i]->clone(state, ordering_expr_ctxs[i]));
    }
    _merger = std::make_unique<vectorized::VSortedRunMerger>(ordering_expr_ctxs, p._is_asc_order,
                                                             p._nulls_first, state->batch_size(),
                                                             p._limit, p._offset, custom_profile());
    std::vector<vectorized::BlockSupplier> child_block_suppliers;
    for (auto sorter : p._sorters) {
        vectorized::BlockSupplier block_supplier = [sorter, state](vectorized::Block* block,
                                                                   bool* eos) {
            return sorter->get_next(state, block, eos);
        };
        child_block_suppliers.push_back(block_supplier);
    }
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

LocalMergeSortSourceOperatorX::LocalMergeSortSourceOperatorX(ObjectPool* pool,
                                                             const TPlanNode& tnode,
                                                             int operator_id,
                                                             const DescriptorTbl& descs)
        : OperatorX<LocalMergeSortLocalState>(pool, tnode, operator_id, descs),
          _merge_by_exchange(tnode.sort_node.merge_by_exchange),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0) {}

Status LocalMergeSortSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;
    _op_name = "LOCAL_MERGE_SORT_SOURCE_OPERATOR";
    return Status::OK();
}

Status LocalMergeSortSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _child->row_desc(), _row_descriptor));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    init_dependencies_and_sorter();
    return Status::OK();
}

void LocalMergeSortSourceOperatorX::init_dependencies_and_sorter() {
    _other_source_deps.resize(_parallel_tasks);
    for (int i = 1; i < _parallel_tasks; ++i) {
        auto dep = Dependency::create_shared(operator_id(), node_id(),
                                             fmt::format("LocalMergeOtherDependency{}", i), false);
        _other_source_deps[i] = dep;
    }
    _sorters.resize(_parallel_tasks);
}

Status LocalMergeSortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);

    if (local_state._task_idx == 0) {
        RETURN_IF_ERROR(main_source_get_block(state, block, eos));
    } else {
        RETURN_IF_ERROR(other_source_get_block(state, block, eos));
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}

Status LocalMergeSortSourceOperatorX::main_source_get_block(RuntimeState* state,
                                                            vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    if (local_state._merger == nullptr) {
        // Since we cannot control the initialization order of different local states, we set the sorter to the operator during execution.
        _sorters[local_state._task_idx] = local_state._shared_state->sorter;
        RETURN_IF_ERROR(local_state.build_merger(state));
    }
    RETURN_IF_ERROR(local_state._merger->get_next(block, eos));
    return Status::OK();
}

Status LocalMergeSortSourceOperatorX::other_source_get_block(RuntimeState* state,
                                                             vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    DCHECK(_other_source_deps[local_state._task_idx] != nullptr);
    // Since we cannot control the initialization order of different local states, we set the sorter to the operator during execution.
    // Wake up main_source.
    _sorters[local_state._task_idx] = local_state._shared_state->sorter;
    _other_source_deps[local_state._task_idx]->set_ready();
    *eos = true;
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
