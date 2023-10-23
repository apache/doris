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

#include "vec/exec/vselect_node.h"

#include <opentelemetry/nostd/shared_ptr.h>

#include <functional>
#include <memory>
#include <vector>

#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class TPlanNode;

namespace vectorized {

VSelectNode::VSelectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _child_eos(false) {}

Status VSelectNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return ExecNode::init(tnode, state);
}

Status VSelectNode::prepare(RuntimeState* state) {
    return ExecNode::prepare(state);
}

Status VSelectNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

Status VSelectNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    do {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_children[0]->get_next_after_projects(
                state, block, &_child_eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));
        if (_child_eos) {
            *eos = true;
            break;
        }
    } while (block->rows() == 0);

    return pull(state, block, eos);
}

Status VSelectNode::pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    SCOPED_TIMER(_exec_timer);
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    reached_limit(output_block, eos);

    return Status::OK();
}

Status VSelectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

} // namespace vectorized
} // namespace doris