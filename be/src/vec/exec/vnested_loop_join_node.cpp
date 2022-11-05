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

#include "vec/exec/vnested_loop_join_node.h"

#include <sstream>

#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

VNestedLoopJoinNode::VNestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _join_op(TJoinOp::CROSS_JOIN), _left_side_eos(false) {}

Status VNestedLoopJoinNode::prepare(RuntimeState* state) {
    DCHECK(_join_op == TJoinOp::CROSS_JOIN);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _left_child_timer = ADD_TIMER(runtime_profile(), "LeftChildTime");
    _build_row_counter = ADD_COUNTER(runtime_profile(), "BuildRows", TUnit::UNIT);
    _left_child_row_counter = ADD_COUNTER(runtime_profile(), "LeftChildRows", TUnit::UNIT);

    // pre-compute the tuple index of build tuples in the output row
    int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();

    for (int i = 0; i < num_build_tuples; ++i) {
        TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
    }

    _num_existing_columns = child(0)->row_desc().num_materialized_slots();
    _num_columns_to_add = child(1)->row_desc().num_materialized_slots();
    return Status::OK();
}

Status VNestedLoopJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VNestedLoopJoinNode::close");
    ExecNode::close(state);
    return Status::OK();
}

Status VNestedLoopJoinNode::_construct_build_side(RuntimeState* state) {
    // Do a full scan of child(1) and store all build row batches.
    RETURN_IF_ERROR(child(1)->open(state));

    bool eos = false;
    while (true) {
        SCOPED_TIMER(_build_timer);
        RETURN_IF_CANCELLED(state);

        Block block;
        RETURN_IF_ERROR_AND_CHECK_SPAN(child(1)->get_next_after_projects(state, &block, &eos),
                                       child(1)->get_next_span(), eos);
        auto rows = block.rows();
        auto mem_usage = block.allocated_bytes();

        if (rows != 0) {
            _build_rows += rows;
            _total_mem_usage += mem_usage;
            _build_blocks.emplace_back(std::move(block));
        }

        if (eos) {
            break;
        }
    }

    COUNTER_UPDATE(_build_row_counter, _build_rows);
    // If right table in join is empty, the node is eos
    _eos = _build_rows == 0;
    return Status::OK();
}

void VNestedLoopJoinNode::_init_get_next(int left_batch_row) {
    _current_build_pos = 0;
}

Status VNestedLoopJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span,
                                 "VNestedLoopJoinNode::get_next");
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    *eos = false;

    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    auto dst_columns = _get_mutable_columns(block);
    ScopedTimer<MonotonicStopWatch> timer(_left_child_timer);

    while (block->rows() < state->batch_size() && !_eos) {
        // Check to see if we're done processing the current left child batch
        if (_current_build_pos == _build_blocks.size()) {
            _current_build_pos = 0;
            _left_block_pos++;

            if (_left_block_pos == _left_block.rows()) {
                _left_block_pos = 0;

                if (_left_side_eos) {
                    *eos = _eos = true;
                } else {
                    do {
                        release_block_memory(_left_block);
                        timer.stop();
                        RETURN_IF_ERROR_AND_CHECK_SPAN(
                                child(0)->get_next_after_projects(state, &_left_block,
                                                                  &_left_side_eos),
                                child(0)->get_next_span(), _left_side_eos);
                        timer.start();
                    } while (_left_block.rows() == 0 && !_left_side_eos);
                    COUNTER_UPDATE(_left_child_row_counter, _left_block.rows());
                    if (_left_block.rows() == 0) {
                        *eos = _eos = _left_side_eos;
                    }
                }
            }
        }

        if (!_eos) {
            do {
                const auto& now_process_build_block = _build_blocks[_current_build_pos++];
                _process_left_child_block(dst_columns, now_process_build_block);
            } while (block->rows() < state->batch_size() &&
                     _current_build_pos < _build_blocks.size());
        }
    }
    dst_columns.clear();
    RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, block->columns()));

    reached_limit(block, eos);
    return Status::OK();
}

MutableColumns VNestedLoopJoinNode::_get_mutable_columns(Block* block) {
    bool mem_reuse = block->mem_reuse();
    if (!mem_reuse) {
        for (size_t i = 0; i < _num_existing_columns; ++i) {
            const ColumnWithTypeAndName& src_column = _left_block.get_by_position(i);
            block->insert({src_column.type->create_column(), src_column.type, src_column.name});
        }

        for (size_t i = 0; i < _num_columns_to_add; ++i) {
            const ColumnWithTypeAndName& src_column = _build_blocks[0].get_by_position(i);
            block->insert({src_column.type->create_column(), src_column.type, src_column.name});
        }
    }
    return block->mutate_columns();
}

void VNestedLoopJoinNode::_process_left_child_block(MutableColumns& dst_columns,
                                                    const Block& now_process_build_block) {
    const int max_added_rows = now_process_build_block.rows();
    for (size_t i = 0; i < _num_existing_columns; ++i) {
        const ColumnWithTypeAndName& src_column = _left_block.get_by_position(i);
        dst_columns[i]->insert_many_from(*src_column.column, _left_block_pos, max_added_rows);
    }
    for (size_t i = 0; i < _num_columns_to_add; ++i) {
        const ColumnWithTypeAndName& src_column = now_process_build_block.get_by_position(i);
        dst_columns[_num_existing_columns + i]->insert_range_from(*src_column.column.get(), 0,
                                                                  max_added_rows);
    }
}

Status VNestedLoopJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return ExecNode::init(tnode, state);
}

void VNestedLoopJoinNode::_build_side_thread(RuntimeState* state, std::promise<Status>* status) {
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_shared());
    status->set_value(_construct_build_side(state));
    // Release the thread token as soon as possible (before the main thread joins
    // on it).  This way, if we had a chain of 10 joins using 1 additional thread,
    // we'd keep the additional thread busy the whole time.
}

Status VNestedLoopJoinNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VNestedLoopJoinNode::open")
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_CANCELLED(state);

    _eos = false;

    // Kick-off the construction of the build-side table in a separate
    // thread, so that the left child can do any initialisation in parallel.
    // Only do this if we can get a thread token.  Otherwise, do this in the
    // main thread
    std::promise<Status> build_side_status;

    add_runtime_exec_option("Join Build-Side Prepared Asynchronously");
    std::thread(bind(&VNestedLoopJoinNode::_build_side_thread, this, state, &build_side_status))
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
        release_block_memory(_left_block);
        RETURN_IF_ERROR_AND_CHECK_SPAN(
                child(0)->get_next_after_projects(state, &_left_block, &_left_side_eos),
                child(0)->get_next_span(), _left_side_eos);
        COUNTER_UPDATE(_left_child_row_counter, _left_block.rows());
        _left_block_pos = 0;

        if (_left_block.rows() == 0) {
            if (_left_side_eos) {
                _init_get_next(-1);
                _eos = true;
                break;
            }

            continue;
        } else {
            _init_get_next(_left_block_pos);
            break;
        }
    }

    return Status::OK();
}

void VNestedLoopJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << "VNestedLoopJoinNode";
    *out << "(eos=" << (_eos ? "true" : "false") << " left_block_pos=" << _left_block_pos;
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris::vectorized
