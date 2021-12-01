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

#ifndef DORIS_BE_SRC_EXEC_SPILL_SORT_NODE_H
#define DORIS_BE_SRC_EXEC_SPILL_SORT_NODE_H

#include "exec/exec_node.h"
#include "exec/sort_exec_exprs.h"
#include "runtime/buffered_block_mgr2.h"
#include "runtime/spill_sorter.h"

namespace doris {

// Node that implements a full sort of its input with a fixed memory budget, spilling
// to disk if the input is larger than available memory.
// Uses SpillSorter and BufferedBlockMgr for the external sort implementation.
// Input rows to SpillSortNode are materialized by the SpillSorter into a single tuple
// using the expressions specified in _sort_exec_exprs.
// In get_next(), SpillSortNode passes in the output batch to the sorter instance created
// in open() to fill it with sorted rows.
// If a merge phase was performed in the sort, sorted rows are deep copied into
// the output batch. Otherwise, the sorter instance owns the sorted data.
class SpillSortNode : public ExecNode {
public:
    SpillSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~SpillSortNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status reset(RuntimeState* state);
    virtual Status close(RuntimeState* state);

protected:
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    // Fetch input rows and feed them to the sorter until the input is exhausted.
    Status sort_input(RuntimeState* state);

    // Number of rows to skip.
    int64_t _offset;

    // Expressions and parameters used for tuple materialization and tuple comparison.
    SortExecExprs _sort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    /////////////////////////////////////////
    // BEGIN: Members that must be reset()

    // Object used for external sorting.
    std::unique_ptr<SpillSorter> _sorter;

    // Keeps track of the number of rows skipped for handling _offset.
    int64_t _num_rows_skipped;

    // END: Members that must be reset()
    /////////////////////////////////////////
};

} // end namespace doris

#endif // DORIS_BE_SRC_EXEC_SPILL_SORT_NODE_H
