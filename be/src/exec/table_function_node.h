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

namespace doris {

class MemPool;
class RowBatch;
class TableFunction;
class TupleRow;

// TableFunctionNode
class TableFunctionNode : public ExecNode {
public:
    TableFunctionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~TableFunctionNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    Status _prepare_output_slot_ids(const TPlanNode& tnode);

    // return:
    //  0: all fns are eos
    // -1: all fns are not eos
    // >0: some of fns are eos
    int _find_last_fn_eos_idx();

    virtual Status _process_next_child_row();

    bool _roll_table_functions(int last_eos_idx);

    int64_t _cur_child_offset = 0;
    TupleRow* _cur_child_tuple_row = nullptr;
    std::shared_ptr<RowBatch> _cur_child_batch;
    // true means current child batch is completely consumed.
    // we should get next batch from child node.
    bool _child_batch_exhausted = true;

    std::vector<ExprContext*> _fn_ctxs;
    std::vector<vectorized::VExprContext*> _vfn_ctxs;

    std::vector<TableFunction*> _fns;
    std::vector<void*> _fn_values;
    std::vector<int64_t> _fn_value_lengths;
    int _fn_num = 0;

    // std::unordered_set<SlotId> _output_slot_ids;
    std::vector<bool> _output_slot_ids;

    int _parent_tuple_desc_size = -1;
    int _child_tuple_desc_size = -1;
    std::vector<int> _child_slot_sizes;
    // indicate if child node reach the end
    bool _child_eos = false;

    RuntimeProfile::Counter* _num_rows_filtered_counter = nullptr;
    uint64_t _num_rows_filtered = 0;
};

}; // namespace doris
