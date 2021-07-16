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

#include <sstream>

#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

#include "vec/exec/vcross_join_node.h"

namespace doris::vectorized {

VCrossJoinNode::VCrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VBlockingJoinNode("VCrossJoinNode", TJoinOp::CROSS_JOIN, pool, tnode, descs) {
}

Status VCrossJoinNode::prepare(RuntimeState* state) {
    DCHECK(_join_op == TJoinOp::CROSS_JOIN);
    RETURN_IF_ERROR(VBlockingJoinNode::prepare(state));

    _num_existing_columns = child(0)->row_desc().num_materialized_slots();
    _num_columns_to_add = child(1)->row_desc().num_materialized_slots();
    return Status::OK();
}

Status VCrossJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    _mem_tracker->Release(_total_mem_usage);
    VBlockingJoinNode::close(state);
    return Status::OK();
}

Status VCrossJoinNode::construct_build_side(RuntimeState* state) {
    // Do a full scan of child(1) and store all build row batches.
    RETURN_IF_ERROR(child(1)->open(state));

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
            _mem_tracker->Consume(mem_usage);
        }
        // to prevent use too many memory
        RETURN_IF_LIMIT_EXCEEDED(state, "Cross join, while getting next from the child 1.");

        if (eos) {
            break;
        }
    }

    COUNTER_UPDATE(_build_row_counter, _build_rows);
    return Status::OK();
}

void VCrossJoinNode::init_get_next(int left_batch_row) {
    _current_build_pos = 0;
}

Status VCrossJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    RETURN_IF_CANCELLED(state);
    *eos = false;
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit() || _eos) {
        *eos = true;
        return Status::OK();
    }

    ScopedTimer<MonotonicStopWatch> timer(_left_child_timer);
    // Check to see if we're done processing the current left child batch
    if (_current_build_pos == _build_blocks.size()) {
        _current_build_pos = 0;
        _left_block_pos++;
        
        if (_left_block_pos == _left_block.rows()) {
            _left_block_pos = 0;

            if (_left_side_eos) {
                *eos = _eos = true;
            } else {
                _left_block.clear();
                timer.stop();
                RETURN_IF_ERROR(child(0)->get_next(state, &_left_block, &_left_side_eos));
                timer.start();
                COUNTER_UPDATE(_left_child_row_counter, _left_block.rows());

                *eos = _eos = _left_side_eos;
            }
        }
    }

    if (!_eos) {
        auto block_nums = 0;
        do {
            block->clear();
            // Compute max rows that should be added to block
            const auto &now_process_build_block = _build_blocks[_current_build_pos++];
            int64_t max_added_rows = now_process_build_block.rows();

            if (limit() != -1) {
                max_added_rows = std::min(max_added_rows, limit() - rows_returned());
            }
            block_nums = process_left_child_block(block, now_process_build_block, max_added_rows);
        } while (block_nums == 0 && _current_build_pos < _build_blocks.size());

        _num_rows_returned += block_nums;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        *eos = _eos = reached_limit();
    }

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

int VCrossJoinNode::process_left_child_block(Block* block, const Block& now_process_build_block,
                                            int max_added_rows) {
    MutableColumns dst_columns(_num_existing_columns + _num_columns_to_add);

    for (size_t i = 0; i < _num_existing_columns; ++i)
    {
        const ColumnWithTypeAndName & src_column = _left_block.get_by_position(i);
        dst_columns[i] = src_column.type->create_column_const(max_added_rows,
                src_column.column->operator[](_left_block_pos))->assume_mutable();
        block->insert(src_column);
    }

    for (size_t i = 0; i < _num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = now_process_build_block.get_by_position(i);
        dst_columns[_num_existing_columns + i] = src_column.column->clone_empty();
        dst_columns[_num_existing_columns + i]->insert_range_from(*src_column.column.get(), 0, max_added_rows);
        block->insert(src_column);
    }

    *block = block->clone_with_columns(std::move(dst_columns));

    if (_vconjunct_ctx_ptr) {
        int result_column_id = -1;
        int orig_columns = block->columns();
        (*_vconjunct_ctx_ptr)->execute(block, &result_column_id);
        Block::filter_block(block, result_column_id, orig_columns);
    }

    if (block->rows() > max_added_rows) {
        block->set_num_rows(max_added_rows);
    }

    return block->rows();
}
} // namespace doris
