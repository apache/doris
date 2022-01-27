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

class Tuple;
class RuntimeState;
class RowBatch;

// repeat tuple of children and set given slots to null, this class generates tuple rows according to the given
// _repeat_id_list, and sets the value of the slot corresponding to the grouping function according to _grouping_list
class RepeatNode : public ExecNode {
public:
    RepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~RepeatNode();

    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    virtual Status close(RuntimeState* state) override;

protected:
    virtual void debug_string(int indentation_level, std::stringstream* out) const override;

protected:
    Status get_repeated_batch(RowBatch* child_row_batch, int repeat_id_idx, RowBatch* row_batch);

    // Slot id set used to indicate those slots need to set to null.
    std::vector<std::set<SlotId>> _slot_id_set_list;
    // all slot id
    std::set<SlotId> _all_slot_ids;
    // An integer bitmap list, it indicates the bit position of the exprs not null.
    std::vector<int64_t> _repeat_id_list;
    std::vector<std::vector<int64_t>> _grouping_list;
    // Tuple id used for output, it has new slots.
    TupleId _output_tuple_id;
    const TupleDescriptor* _tuple_desc;

    std::unique_ptr<RowBatch> _child_row_batch;
    bool _child_eos;
    int _repeat_id_idx;
    RuntimeState* _runtime_state;
};

} // namespace doris
