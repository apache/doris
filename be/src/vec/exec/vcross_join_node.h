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

#ifndef DORIS_BE_SRC_QUERY_EXEC_VCROSS_JOIN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_VCROSS_JOIN_NODE_H

#include <boost/thread.hpp>
#include <string>
#include <unordered_set>

#include "exec/exec_node.h"
#include "exec/row_batch_list.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

#include "vec/core/block.h"
#include "vec/exec/vblocking_join_node.h"

namespace doris::vectorized {
// Node for cross joins.
// Iterates over the left child rows and then the right child rows and, for
// each combination, writes the output row if the conjuncts are satisfied. The
// build batches are kept in a list that is fully constructed from the right child in
// construct_build_side() (called by BlockingJoinNode::open()) while rows are fetched from
// the left child as necessary in get_next().
class VCrossJoinNode final : public VBlockingJoinNode {
public:
    VCrossJoinNode(ObjectPool *pool, const TPlanNode &tnode, const DescriptorTbl &descs);

    Status prepare(RuntimeState *state) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    Status close(RuntimeState *state) override;

protected:
    void init_get_next(int first_left_row) override;

    Status construct_build_side(RuntimeState *state) override;

private:
    // List of build batches, constructed in prepare()
    Blocks _build_blocks;
    size_t _current_build_pos = 0;

    size_t _num_existing_columns = 0;
    size_t _num_columns_to_add = 0;

    uint64_t _build_rows = 0;
    uint64_t _total_mem_usage = 0;
    // Processes a batch from the left child.
    //  output_batch: the batch for resulting tuple rows
    //  batch: the batch from the left child to process.  This function can be called to
    //    continue processing a batch in the middle
    //  max_added_rows: maximum rows that can be added to output_batch
    // return the number of rows added to output_batch
    int process_left_child_block(Block* block, const Block& now_process_build_block,
                                            int max_added_rows);

    // Returns a debug string for _build_rows. This is used for debugging during the
    // build list construction and before doing the join.
    std::string build_list_debug_string();
};

}

#endif

