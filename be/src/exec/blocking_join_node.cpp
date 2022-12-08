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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/blocking-join-node.cc
// and modified by Doris

#include "exec/blocking_join_node.h"

#include <sstream>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace doris {

BlockingJoinNode::BlockingJoinNode(const std::string& node_name, const TJoinOp::type join_op,
                                   ObjectPool* pool, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _node_name(node_name),
          _join_op(join_op),
          _left_side_eos(false) {}

Status BlockingJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return ExecNode::init(tnode, state);
}

BlockingJoinNode::~BlockingJoinNode() {
    // _left_batch must be cleaned up in close() to ensure proper resource freeing.
    DCHECK(_left_batch == nullptr);
}

Status BlockingJoinNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());

    _build_pool.reset(new MemPool(mem_tracker_held()));
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
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
        _build_tuple_idx.push_back(tuple_idx);
    }

    _probe_tuple_row_size = num_left_tuples * sizeof(Tuple*);
    _build_tuple_row_size = num_build_tuples * sizeof(Tuple*);

    _left_batch.reset(new RowBatch(child(0)->row_desc(), state->batch_size()));
    return Status::OK();
}

Status BlockingJoinNode::close(RuntimeState* state) {
    // TODO(zhaochun): avoid double close
    // if (is_closed()) return Status::OK();
    _left_batch.reset();
    ExecNode::close(state);
    return Status::OK();
}

void BlockingJoinNode::build_side_thread(RuntimeState* state, std::promise<Status>* status) {
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh_shared());
    status->set_value(construct_build_side(state));
}

Status BlockingJoinNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    // RETURN_IF_ERROR(Expr::open(_conjuncts, state));

    RETURN_IF_CANCELLED(state);
    // TODO(zhaochun)
    // RETURN_IF_ERROR(state->check_query_state());

    _eos = false;

    // Kick-off the construction of the build-side table in a separate
    // thread, so that the left child can do any initialisation in parallel.
    // Only do this if we can get a thread token.  Otherwise, do this in the
    // main thread
    std::promise<Status> build_side_status;

    add_runtime_exec_option("Join Build-Side Prepared Asynchronously");
    std::thread(bind(&BlockingJoinNode::build_side_thread, this, state, &build_side_status))
            .detach();

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
        RETURN_IF_ERROR(child(0)->get_next(state, _left_batch.get(), &_left_side_eos));
        COUNTER_UPDATE(_left_child_row_counter, _left_batch->num_rows());
        _left_batch_pos = 0;

        if (_left_batch->num_rows() == 0) {
            if (_left_side_eos) {
                init_get_next(nullptr /* eos */);
                _eos = true;
                break;
            }

            _left_batch->reset();
            continue;
        } else {
            _current_left_child_row = _left_batch->get_row(_left_batch_pos++);
            init_get_next(_current_left_child_row);
            break;
        }
    }

    return Status::OK();
}

void BlockingJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << _node_name;
    *out << "(eos=" << (_eos ? "true" : "false") << " left_batch_pos=" << _left_batch_pos;
    add_to_debug_string(indentation_level, out);
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

std::string BlockingJoinNode::get_left_child_row_string(TupleRow* row) {
    std::stringstream out;
    out << "[";
    int* _build_tuple_idx_ptr = &_build_tuple_idx[0];

    for (int i = 0; i < row_desc().tuple_descriptors().size(); ++i) {
        if (i != 0) {
            out << " ";
        }

        int* is_build_tuple =
                std::find(_build_tuple_idx_ptr, _build_tuple_idx_ptr + _build_tuple_size, i);

        if (is_build_tuple != _build_tuple_idx_ptr + _build_tuple_size) {
            out << Tuple::to_string(nullptr, *row_desc().tuple_descriptors()[i]);
        } else {
            out << Tuple::to_string(row->get_tuple(i), *row_desc().tuple_descriptors()[i]);
        }
    }

    out << "]";
    return out.str();
}

// This function is replaced by codegen
void BlockingJoinNode::create_output_row(TupleRow* out, TupleRow* left, TupleRow* build) {
    uint8_t* out_ptr = reinterpret_cast<uint8_t*>(out);
    if (left == nullptr) {
        memset(out_ptr, 0, _probe_tuple_row_size);
    } else {
        memcpy(out_ptr, left, _probe_tuple_row_size);
    }

    if (build == nullptr) {
        memset(out_ptr + _probe_tuple_row_size, 0, _build_tuple_row_size);
    } else {
        memcpy(out_ptr + _probe_tuple_row_size, build, _build_tuple_row_size);
    }
}

} // namespace doris
