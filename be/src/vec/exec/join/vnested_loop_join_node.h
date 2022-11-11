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

#pragma once

#include <boost/thread.hpp>
#include <future>
#include <stack>
#include <string>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "vec/core/block.h"
#include "vec/exec/join/vjoin_node_base.h"

namespace doris::vectorized {

// Node for nested loop joins.
class VNestedLoopJoinNode final : public VJoinNodeBase {
public:
    VNestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status prepare(RuntimeState* state) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    Status close(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VNestedLoopJoinNode::get_next scalar");
    }

    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    Status _materialize_build_side(RuntimeState* state) override;

    // Processes a block from the left child.
    //  dst_columns: left_child_row and now_process_build_block to construct a bundle column of new block
    //  now_process_build_block: right child block now to process
    void _process_left_child_block(MutableColumns& dst_columns,
                                   const Block& now_process_build_block) const;

    template <bool SetBuildSideFlag, bool SetProbeSideFlag>
    Status _do_filtering_and_update_visited_flags(Block* block, std::stack<uint16_t>& offset_stack);

    template <bool BuildSide>
    void _output_null_data(MutableColumns& dst_columns, size_t batch_size);

    void _reset_with_next_probe_row(MutableColumns& dst_columns);

    // List of build blocks, constructed in prepare()
    Blocks _build_blocks;
    // Visited flags for each row in build side.
    MutableColumns _build_side_visited_flags;
    // Visited flags for current row in probe side.
    bool _cur_probe_row_visited_flags;
    size_t _current_build_pos = 0;

    size_t _num_probe_side_columns = 0;
    size_t _num_build_side_columns = 0;

    uint64_t _build_rows = 0;
    uint64_t _total_mem_usage = 0;
    uint64_t _output_null_idx_build_side = 0;

    bool _matched_rows_done;

    // _left_block must be cleared before calling get_next().  The child node
    // does not initialize all tuple ptrs in the row, only the ones that it
    // is responsible for.
    Block _left_block;

    int _left_block_pos; // current scan pos in _left_block
    bool _left_side_eos; // if true, left child has no more rows to process
};

} // namespace doris::vectorized
