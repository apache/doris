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

#include "vec/exec/scan/group_commit_scan_node.h"

#include "runtime/group_commit_mgr.h"
#include "vec/columns/column_const.h"
#include "vec/exec/scan/new_olap_scanner.h"
#include "vec/exec/scan/vfile_scanner.h"
#include "vec/functions/in.h"

namespace doris::vectorized {

GroupCommitScanNode::GroupCommitScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs) {
    _output_tuple_id = tnode.file_scan_node.tuple_id;
    _table_id = tnode.group_commit_scan_node.table_id;
}

Status GroupCommitScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    bool find_node = false;
    while (!find_node && !*eos) {
        RETURN_IF_ERROR(load_block_queue->get_block(block, &find_node, eos));
    }
    return Status::OK();
}

Status GroupCommitScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::init(tnode, state));
    return state->exec_env()->group_commit_mgr()->get_load_block_queue(
            _table_id, state->fragment_instance_id(), load_block_queue);
}

Status GroupCommitScanNode::prepare(RuntimeState* state) {
    return VScanNode::prepare(state);
}

void GroupCommitScanNode::set_scan_ranges(RuntimeState* state,
                                          const std::vector<TScanRangeParams>& scan_ranges) {}

Status GroupCommitScanNode::_init_profile() {
    return VScanNode::_init_profile();
}

Status GroupCommitScanNode::_process_conjuncts() {
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    return Status::OK();
}

std::string GroupCommitScanNode::get_name() {
    return fmt::format("GROUP_COMMIT_SCAN_NODE({0})", _table_id);
}

}; // namespace doris::vectorized
