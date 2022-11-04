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

#ifndef DORIS_BE_SRC_QUERY_EXEC_VNESTED_LOOP_JOIN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_VNESTED_LOOP_JOIN_NODE_H

#include <boost/thread.hpp>
#include <future>
#include <string>

#include "exec/exec_node.h"
#include "exec/row_batch_list.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "vec/core/block.h"

namespace doris::vectorized {

// Node for cross joins.
// Iterates over the left child rows and then the right child rows and, for
// each combination, writes the output row if the conjuncts are satisfied. The
// build batches are kept in a list that is fully constructed from the right child in
// construct_build_side() (called by BlockingJoinNode::open()) while rows are fetched from
// the left child as necessary in get_next().
class VNestedLoopJoinNode final : public ExecNode {
public:
    VNestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status prepare(RuntimeState* state) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    Status close(RuntimeState* state) override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    // Open prepares the build side structures (subclasses should implement
    // construct_build_side()) and then prepares for GetNext with the first left child row
    // (subclasses should implement init_get_next()).
    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VNestedLoopJoinNode::get_next scalar");
    }

    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    // Supervises ConstructBuildSide in a separate thread, and returns its status in the
    // promise parameter.
    void _build_side_thread(RuntimeState* state, std::promise<Status>* status);
    // Init the build-side state for a new left child row (e.g. hash table iterator or list
    // iterator) given the first row. Used in open() to prepare for get_next().
    // -1 for left_side_pos indicates the left child eos.
    void _init_get_next(int first_left_row);

    // We parallelize building the build-side with Opening the
    // left child. If, for example, the left child is another
    // join node, it can start to build its own build-side at the
    // same time.
    Status _construct_build_side(RuntimeState* state);

    // Build mutable columns to insert data.
    // if block can mem reuse, just clear data in block
    // else build a new block and alloc mem of column from left and right child block
    MutableColumns _get_mutable_columns(Block* block);

    // Processes a block from the left child.
    //  dst_columns: left_child_row and now_process_build_block to construct a bundle column of new block
    //  now_process_build_block: right child block now to process
    void _process_left_child_block(MutableColumns& dst_columns,
                                   const Block& now_process_build_block);

    // List of build blocks, constructed in prepare()
    Blocks _build_blocks;
    size_t _current_build_pos = 0;

    size_t _num_existing_columns = 0;
    size_t _num_columns_to_add = 0;

    uint64_t _build_rows = 0;
    uint64_t _total_mem_usage = 0;
    TJoinOp::type _join_op;
    bool _eos; // if true, nothing left to return in get_next()

    // _left_block must be cleared before calling get_next().  The child node
    // does not initialize all tuple ptrs in the row, only the ones that it
    // is responsible for.
    Block _left_block;

    int _left_block_pos; // current scan pos in _left_block
    bool _left_side_eos; // if true, left child has no more rows to process

    RuntimeProfile::Counter* _build_timer;            // time to prepare build side
    RuntimeProfile::Counter* _left_child_timer;       // time to process left child batch
    RuntimeProfile::Counter* _build_row_counter;      // num build rows
    RuntimeProfile::Counter* _left_child_row_counter; // num left child rows
};

} // namespace doris::vectorized

#endif
