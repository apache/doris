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

#include "exec/exec_node.h"

#include <queue>

#include "vec/core/block.h"
#include "vec/core/sort_cursor.h"
#include "vec/exec/vsort_exec_exprs.h"

namespace doris::vectorized {
// Node that implements a full sort of its input with a fixed memory budget
// In open() the input Block to VSortNode will sort firstly, using the expressions specified in _sort_exec_exprs.
// In get_next(), VSortNode do the merge sort to gather data to a new block

// support spill to disk in the future
class VSortNode : public doris::ExecNode {
public:
    VSortNode(ObjectPool *pool, const TPlanNode &tnode, const DescriptorTbl &descs);

    ~VSortNode() override = default;

    virtual Status init(const TPlanNode &tnode, RuntimeState *state = nullptr);

    virtual Status prepare(RuntimeState *state);

    virtual Status open(RuntimeState *state);

    virtual Status get_next(RuntimeState *state, RowBatch *row_batch, bool *eos);

    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);

    virtual Status reset(RuntimeState *state);

    virtual Status close(RuntimeState *state);

protected:
    virtual void debug_string(int indentation_level, std::stringstream *out) const;

private:
    // Fetch input rows and feed them to the sorter until the input is exhausted.
    Status sort_input(RuntimeState *state);

    Status pretreat_block(Block& block);

    void build_merge_tree();

    Status merge_sort_read(RuntimeState* state, Block* block, bool* eos);

    // Number of rows to skip.
    int64_t _offset;

    // Expressions and parameters used for build _sort_description
    VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    SortDescription _sort_description;
    std::vector<SortCursorImpl> _cursors;
    std::vector<Block> _sorted_blocks;
    std::priority_queue<SortCursor> _priority_queue;

    // TODO: Not using now, maybe should be delete
    // Keeps track of the number of rows skipped for handling _offset.
    int64_t _num_rows_skipped;
    uint64_t _total_mem_usage = 0;

    // only valid in TOP-N node
    uint64_t _num_rows_in_block = 0;
    std::priority_queue<SortBlockCursor> _block_priority_queue;
};

} // end namespace doris


