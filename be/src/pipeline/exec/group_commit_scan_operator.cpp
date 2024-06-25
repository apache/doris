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

#include "pipeline/exec/group_commit_scan_operator.h"

#include <fmt/format.h>

namespace doris::pipeline {

GroupCommitOperatorX::GroupCommitOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                           int operator_id, const DescriptorTbl& descs,
                                           int parallel_tasks)
        : ScanOperatorX<GroupCommitLocalState>(pool, tnode, operator_id, descs, parallel_tasks),
          _table_id(tnode.group_commit_scan_node.table_id) {
    _output_tuple_id = tnode.file_scan_node.tuple_id;
}

Status GroupCommitOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    bool find_node = false;
    while (!find_node && !*eos) {
        RETURN_IF_ERROR(local_state.load_block_queue->get_block(state, block, &find_node, eos,
                                                                local_state._get_block_dependency));
    }
    return Status::OK();
}

Status GroupCommitLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(ScanLocalState<GroupCommitLocalState>::init(state, info));
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<GroupCommitOperatorX>();
    _get_block_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                      "GroupCommitGetBlockDependency", true);
    return state->exec_env()->group_commit_mgr()->get_load_block_queue(
            p._table_id, state->fragment_instance_id(), load_block_queue, _get_block_dependency);
}

Status GroupCommitLocalState::_process_conjuncts(RuntimeState* state) {
    RETURN_IF_ERROR(ScanLocalState<GroupCommitLocalState>::_process_conjuncts(state));
    if (_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    return Status::OK();
}

} // namespace doris::pipeline
