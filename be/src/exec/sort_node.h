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

#ifndef INF_DORIS_QE_SRC_BE_EXEC_SORT_NODE_H
#define INF_DORIS_QE_SRC_BE_EXEC_SORT_NODE_H

#include "exec/exec_node.h"
#include "exec/sort_exec_exprs.h"
#include "runtime/merge_sorter.h"
#include "runtime/buffered_block_mgr.h"

namespace doris {

// Node that implements a full sort of its input with a fixed memory budget, spilling
// to disk if the input is larger than available memory.
// Uses Sorter and BufferedBlockMgr for the external sort implementation.
// Input rows to SortNode are materialized by the Sorter into a single tuple
// using the expressions specified in sort_exec_exprs_.
// In GetNext(), SortNode passes in the output batch to the sorter instance created
// in Open() to fill it with sorted rows.
// If a merge phase was performed in the sort, sorted rows are deep copied into
// the output batch. Otherwise, the sorter instance owns the sorted data.
class SortNode : public ExecNode {
public:
    SortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~SortNode();

    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    // Fetch input rows and feed them to the sorter until the input is exhausted.
    Status sort_input(RuntimeState* state);

    // Create a block manager object and set it in block_mgr_.
    // Returns and sets the query status to Status::MemoryLimitExceeded("Memory limit exceeded") if there is not
    // enough memory for the sort.
    Status create_block_mgr(RuntimeState* state);

    // Number of rows to skip.
    int64_t _offset;
    int64_t _num_rows_skipped;

    // Object used for external sorting.
    boost::scoped_ptr<MergeSorter> _sorter;

    // Expressions and parameters used for tuple materialization and tuple comparison.
    SortExecExprs _sort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    boost::scoped_ptr<MemPool> _tuple_pool;
};

}

#endif
