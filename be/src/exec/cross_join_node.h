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

#ifndef DORIS_BE_SRC_QUERY_EXEC_CROSS_JOIN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_CROSS_JOIN_NODE_H

#include <string>
#include <thread>
#include <unordered_set>

#include "exec/blocking_join_node.h"
#include "exec/exec_node.h"
#include "exec/row_batch_list.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

class RowBatch;
class TupleRow;

// Node for cross joins.
// Iterates over the left child rows and then the right child rows and, for
// each combination, writes the output row if the conjuncts are satisfied. The
// build batches are kept in a list that is fully constructed from the right child in
// construct_build_side() (called by BlockingJoinNode::open()) while rows are fetched from
// the left child as necessary in get_next().
class CrossJoinNode : public BlockingJoinNode {
public:
    CrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status prepare(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    virtual void init_get_next(TupleRow* first_left_row);
    virtual Status construct_build_side(RuntimeState* state);

private:
    // Object pool for build RowBatches, stores all BuildBatches in _build_rows
    std::unique_ptr<ObjectPool> _build_batch_pool;
    // List of build batches, constructed in prepare()
    RowBatchList _build_batches;
    RowBatchList::TupleRowIterator _current_build_row;

    // Processes a batch from the left child.
    //  output_batch: the batch for resulting tuple rows
    //  batch: the batch from the left child to process.  This function can be called to
    //    continue processing a batch in the middle
    //  max_added_rows: maximum rows that can be added to output_batch
    // return the number of rows added to output_batch
    int process_left_child_batch(RowBatch* output_batch, RowBatch* batch, int max_added_rows);

    // Returns a debug string for _build_rows. This is used for debugging during the
    // build list construction and before doing the join.
    std::string build_list_debug_string();
};

} // namespace doris

#endif
