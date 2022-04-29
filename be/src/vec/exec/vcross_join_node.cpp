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

#include "vec/exec/vcross_join_node.h"

#include <sstream>

#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

VCrossJoinNode::VCrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VBlockingJoinNode("VCrossJoinNode", TJoinOp::CROSS_JOIN, pool, tnode, descs) {}

Status VCrossJoinNode::prepare(RuntimeState* state) {
    DCHECK(_join_op == TJoinOp::CROSS_JOIN);
    RETURN_IF_ERROR(VBlockingJoinNode::prepare(state));
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    _block_mem_tracker =
            MemTracker::create_virtual_tracker(-1, "VCrossJoinNode:Block", mem_tracker());

    _num_existing_columns = child(0)->row_desc().num_materialized_slots();
    _num_columns_to_add = child(1)->row_desc().num_materialized_slots();
    return Status::OK();
}

Status VCrossJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    _block_mem_tracker->release(_total_mem_usage);
    VBlockingJoinNode::close(state);
    return Status::OK();
}

Status VCrossJoinNode::construct_build_side(RuntimeState* state) {
    // Do a full scan of child(1) and store all build row batches.
    RETURN_IF_ERROR(child(1)->open(state));
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_ERR_CB(
            "Vec Cross join, while getting next from the child 1");

    bool eos = false;
    while (true) {
        SCOPED_TIMER(_build_timer);
        RETURN_IF_CANCELLED(state);

        Block block;
        RETURN_IF_ERROR(child(1)->get_next(state, &block, &eos));
        auto rows = block.rows();
        auto mem_usage = block.allocated_bytes();

        if (rows != 0) {
            _build_rows += rows;
            _total_mem_usage += mem_usage;
            _build_blocks.emplace_back(std::move(block));
            _block_mem_tracker->consume(mem_usage);
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

void VCrossJoinNode::init_get_next(int left_batch_row) {
    _current_build_pos = 0;
}

Status VCrossJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(mem_tracker());
    *eos = false;

    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    auto dst_columns = get_mutable_columns(block);
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
                        RETURN_IF_ERROR(child(0)->get_next(state, &_left_block, &_left_side_eos));
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
                process_left_child_block(dst_columns, now_process_build_block);
            } while (block->rows() < state->batch_size() &&
                     _current_build_pos < _build_blocks.size());
        }
    }
    dst_columns.clear();
    RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, block->columns()));

    reached_limit(block, eos);
    return Status::OK();
}

std::string VCrossJoinNode::build_list_debug_string() {
    std::stringstream out;
    out << "BuildBlock(";
    for (const auto& block : _build_blocks) {
        out << block.dump_structure() << "\n";
    }
    out << ")";
    return out.str();
}

MutableColumns VCrossJoinNode::get_mutable_columns(Block* block) {
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

void VCrossJoinNode::process_left_child_block(MutableColumns& dst_columns,
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

} // namespace doris::vectorized
