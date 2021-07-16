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

#include "vec/exec/vblocking_join_node.h"

#include <sstream>
#include <boost/bind.hpp>

#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

VBlockingJoinNode::VBlockingJoinNode(const std::string& node_name, const TJoinOp::type join_op,
                                   ObjectPool* pool, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _node_name(node_name), _join_op(join_op) {}

Status VBlockingJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return ExecNode::init(tnode, state);
}

Status VBlockingJoinNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _build_pool.reset(new MemPool(mem_tracker().get()));
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _left_child_timer = ADD_TIMER(runtime_profile(), "LeftChildTime");
    _build_row_counter = ADD_COUNTER(runtime_profile(), "BuildRows", TUnit::UNIT);
    _left_child_row_counter = ADD_COUNTER(runtime_profile(), "LeftChildRows", TUnit::UNIT);

    _result_tuple_row_size = _row_descriptor.tuple_descriptors().size() * sizeof(Tuple*);

    // pre-compute the tuple index of build tuples in the output row
    int num_left_tuples = child(0)->row_desc().tuple_descriptors().size();
    int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();

    _build_tuple_size = num_build_tuples;
    _build_tuple_idx.reserve(_build_tuple_size);

    for (int i = 0; i < _build_tuple_size; ++i) {
        TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        _build_tuple_idx.push_back(_row_descriptor.get_tuple_idx(build_tuple_desc->id()));
    }

    _probe_tuple_row_size = num_left_tuples * sizeof(Tuple*);
    _build_tuple_row_size = num_build_tuples * sizeof(Tuple*);

    return Status::OK();
}

Status VBlockingJoinNode::close(RuntimeState* state) {
    if (is_closed()) return Status::OK();
    ExecNode::close(state);
    return Status::OK();
}

void VBlockingJoinNode::build_side_thread(RuntimeState* state, std::promise<Status>* status) {
    status->set_value(construct_build_side(state));
    // Release the thread token as soon as possible (before the main thread joins
    // on it).  This way, if we had a chain of 10 joins using 1 additional thread,
    // we'd keep the additional thread busy the whole time.
    state->resource_pool()->release_thread_token(false);
}

Status VBlockingJoinNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_CANCELLED(state);
    // TODO(zhaochun)
    // RETURN_IF_ERROR(state->check_query_state());

    _eos = false;

    // Kick-off the construction of the build-side table in a separate
    // thread, so that the left child can do any initialisation in parallel.
    // Only do this if we can get a thread token.  Otherwise, do this in the
    // main thread
    std::promise<Status> build_side_status;

    if (state->resource_pool()->try_acquire_thread_token()) {
        add_runtime_exec_option("Join Build-Side Prepared Asynchronously");
        boost::thread(boost::bind(&VBlockingJoinNode::build_side_thread, this, state, &build_side_status));
    } else {
        build_side_status.set_value(construct_build_side(state));
    }

    // Open the left child so that it may perform any initialisation in parallel.
    // Don't exit even if we see an error, we still need to wait for the build thread
    // to finish.
    Status open_status = child(0)->open(state);

    // Blocks until ConstructBuildSide has returned, after which the build side structures
    // are fully constructed.
    RETURN_IF_ERROR(build_side_status.get_future().get());
    // We can close the right child to release its resources because its input has been
    // fully consumed.
    child(1)->close(state);

    RETURN_IF_ERROR(open_status);

    // Seed left child in preparation for get_next().
    while (true) {
        _left_block.clear();
        RETURN_IF_ERROR(child(0)->get_next(state, &_left_block, &_left_side_eos));
        COUNTER_UPDATE(_left_child_row_counter, _left_block.rows());
        _left_block_pos = 0;

        if (_left_block.rows() == 0) {
            if (_left_side_eos) {
                init_get_next(-1);
                _eos = true;
                break;
            }

            continue;
        } else {
            init_get_next(_left_block_pos);
            break;
        }
    }

    return Status::OK();
}

void VBlockingJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << _node_name;
    *out << "(eos=" << (_eos ? "true" : "false") << " left_block_pos=" << _left_block_pos;
    add_to_debug_string(indentation_level, out);
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris
