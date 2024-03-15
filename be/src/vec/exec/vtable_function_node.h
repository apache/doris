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

#include <cstdint>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class ObjectPool;
class TPlanNode;

namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class VTableFunctionNode final : public ExecNode {
public:
    VTableFunctionNode(doris::ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VTableFunctionNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(alloc_resource(state));
        return _children[0]->open(state);
    }
    Status alloc_resource(RuntimeState* state) override {
        SCOPED_TIMER(_exec_timer);
        RETURN_IF_ERROR(ExecNode::alloc_resource(state));
        return VExpr::open(_vfn_ctxs, state);
    }
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    bool need_more_input_data() const { return !_child_block->rows() && !_child_eos; }

    void release_resource(doris::RuntimeState* state) override {
        if (_num_rows_filtered_counter != nullptr) {
            COUNTER_SET(_num_rows_filtered_counter, static_cast<int64_t>(_num_rows_filtered));
        }
        ExecNode::release_resource(state);
    }

    Status push(RuntimeState* state, Block* input_block, bool eos) override {
        SCOPED_TIMER(_exec_timer);
        _child_eos = eos;
        if (input_block->rows() == 0) {
            return Status::OK();
        }

        for (TableFunction* fn : _fns) {
            RETURN_IF_ERROR(fn->process_init(input_block, state));
        }
        _process_next_child_row();
        return Status::OK();
    }

    Status pull(RuntimeState* state, Block* output_block, bool* eos) override {
        SCOPED_TIMER(_exec_timer);
        RETURN_IF_ERROR(_get_expanded_block(state, output_block, eos));
        reached_limit(output_block, eos);
        return Status::OK();
    }

    std::shared_ptr<Block> get_child_block() { return _child_block; }

private:
    Status _prepare_output_slot_ids(const TPlanNode& tnode);
    bool _is_inner_and_empty();

    // return:
    //  0: all fns are eos
    // -1: all fns are not eos
    // >0: some of fns are eos
    int _find_last_fn_eos_idx();

    bool _roll_table_functions(int last_eos_idx);

    void _process_next_child_row();

    /*  Now the output tuples for table function node is base_table_tuple + tf1 + tf2 + ...
        But not all slots are used, the real used slots are inside table_function_node.outputSlotIds.
        For case like explode_bitmap:
            SELECT a2,count(*) as a3 FROM A WHERE a1 IN
                (SELECT c1 FROM B LATERAL VIEW explode_bitmap(b1) C as c1)
            GROUP BY a2 ORDER BY a3;
        Actually we only need to output column c1, no need to output columns in bitmap table B.
        Copy large bitmap columns are very expensive and slow.

        Here we check if the slot is really used, otherwise we avoid copy it and just insert a default value.

        A better solution is:
            1. FE: create a new output tuple based on the real output slots;
            2. BE: refractor (V)TableFunctionNode output rows based no the new tuple;
    */
    inline bool _slot_need_copy(SlotId slot_id) const {
        auto id = _output_slots[slot_id]->id();
        return (id < _output_slot_ids.size()) && (_output_slot_ids[id]);
    }

    Status _get_expanded_block(RuntimeState* state, Block* output_block, bool* eos);

    void _copy_output_slots(std::vector<MutableColumnPtr>& columns) {
        if (!_current_row_insert_times) {
            return;
        }
        for (auto index : _output_slot_indexs) {
            auto src_column = _child_block->get_by_position(index).column;
            columns[index]->insert_many_from(*src_column, _cur_child_offset,
                                             _current_row_insert_times);
        }
        _current_row_insert_times = 0;
    }
    int _current_row_insert_times = 0;

    std::shared_ptr<Block> _child_block;
    std::vector<SlotDescriptor*> _child_slots;
    std::vector<SlotDescriptor*> _output_slots;
    int64_t _cur_child_offset = 0;

    VExprContextSPtrs _vfn_ctxs;

    std::vector<TableFunction*> _fns;
    int _fn_num = 0;

    std::vector<bool> _output_slot_ids;
    std::vector<int> _output_slot_indexs;
    std::vector<int> _useless_slot_indexs;

    std::vector<int> _child_slot_sizes;
    // indicate if child node reach the end
    bool _child_eos = false;

    RuntimeProfile::Counter* _num_rows_filtered_counter = nullptr;
    uint64_t _num_rows_filtered = 0;
};

} // namespace doris::vectorized
