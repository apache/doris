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

#include <stdint.h>

#include "common/status.h"
#include "exec/operator/operator.h"
#include "exprs/table_function/table_function.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris {

class TableFunctionOperatorX;
class TableFunctionLocalState MOCK_REMOVE(final) : public PipelineXLocalState<> {
public:
    using Parent = TableFunctionOperatorX;
    ENABLE_FACTORY_CREATOR(TableFunctionLocalState);
    TableFunctionLocalState(RuntimeState* state, OperatorXBase* parent);
    ~TableFunctionLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& infos) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override {
        for (auto* fn : _fns) {
            RETURN_IF_ERROR(fn->close());
        }
        RETURN_IF_ERROR(PipelineXLocalState<>::close(state));
        return Status::OK();
    }
    void process_next_child_row();
    Status get_expanded_block(RuntimeState* state, Block* output_block, bool* eos);

private:
    friend class TableFunctionOperatorX;
    friend class StatefulOperatorX<TableFunctionLocalState>;

    MOCK_FUNCTION Status _clone_table_function(RuntimeState* state);

    void _copy_output_slots(std::vector<MutableColumnPtr>& columns,
                            const TableFunctionOperatorX& p);
    bool _roll_table_functions(int last_eos_idx);
    // return:
    //  0: all fns are eos
    // -1: all fns are not eos
    // >0: some of fns are eos
    int _find_last_fn_eos_idx() const;
    bool _is_inner_and_empty();
    bool _can_use_block_fast_path() const;
    void _reset_block_fast_path_state();
    Status _prepare_block_fast_path(RuntimeState* state);
    bool _has_contiguous_block_fast_path_suffix() const;
    Status _get_expanded_block_block_fast_path(RuntimeState* state,
                                               std::vector<MutableColumnPtr>& columns);

    Status _get_expanded_block_for_outer_conjuncts(RuntimeState* state, Block* output_block,
                                                   bool* eos);

    // Fast path for the case where the node produces no real output columns and downstream
    // only needs the expanded row count (e.g. `SELECT COUNT(*)` over unnest/explode, where FE
    // prunes every output slot and adds a constant `final projections`). Instead of building
    // and materializing the full `_output_slots` block, it emits a single lightweight
    // placeholder column carrying the correct row count.
    Status _get_expanded_block_no_columns(RuntimeState* state, Block* output_block, bool* eos);
    Status _count_only_fast_path(RuntimeState* state, MutableColumnPtr& placeholder);

    std::vector<TableFunction*> _fns;
    VExprContextSPtrs _vfn_ctxs;
    VExprContextSPtrs _expand_conjuncts_ctxs;
    // for table function with outer conjuncts, need to handle those child rows which all expanded rows are filtered out
    bool _need_to_handle_outer_conjuncts = false;
    // Index of the child (input) row in `_child_block` that is currently being expanded.
    // -1 means there is no row to process right now (the current child block is exhausted or
    // not yet pushed); the operator then asks upstream for more input. It is advanced by
    // process_next_child_row() and used as the source row offset when replicating child columns.
    int64_t _cur_child_offset = -1;
    std::unique_ptr<Block> _child_block;
    int _current_row_insert_times = 0;
    bool _child_eos = false;
    DorisVector<bool> _child_rows_has_output;

    bool _block_fast_path_prepared = false;
    bool _block_fast_path_enabled = false;
    TableFunction::BlockFastPathContext _block_fast_path_ctx;
    // -1: undecided, 0: disabled (use the full-block path), 1: enabled (count-only path).
    // The decision is made once, when the first non-empty child block arrives, and stays
    // stable for the whole query so the placeholder output block schema never changes.
    int _no_columns_mode = -1;
    // Resume cursor for the block fast path. Because one input block can produce more output rows
    // than `batch_size()`, expansion is spread over multiple pull() calls. `_block_fast_path_row`
    // records the next child row to expand, and `_block_fast_path_in_row_offset` records how many
    // elements of that child row's array have already been emitted, so the next pull() continues
    // from the middle of a partially-consumed array instead of restarting.
    int64_t _block_fast_path_row = 0;
    uint64_t _block_fast_path_in_row_offset = 0;

    RuntimeProfile::Counter* _init_function_timer = nullptr;
    RuntimeProfile::Counter* _process_rows_timer = nullptr;
    RuntimeProfile::Counter* _filter_timer = nullptr;
};

class TableFunctionOperatorX MOCK_REMOVE(final)
        : public StatefulOperatorX<TableFunctionLocalState> {
public:
    using Base = StatefulOperatorX<TableFunctionLocalState>;
    TableFunctionOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                           const DescriptorTbl& descs);

#ifdef BE_TEST
    TableFunctionOperatorX() = default;
#endif

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(doris::RuntimeState* state) override;

    bool need_more_input_data(RuntimeState* state) const override {
        auto& local_state = state->get_local_state(operator_id())->cast<TableFunctionLocalState>();
        return !local_state._child_block->rows() && !local_state._child_eos;
    }

    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        return {ExchangeType::PASSTHROUGH};
    }

    Status push(RuntimeState* state, Block* input_block, bool eos) const override {
        auto& local_state = get_local_state(state);
        if (input_block->rows() == 0) {
            return Status::OK();
        }

        for (auto* fn : local_state._fns) {
            SCOPED_TIMER(local_state._init_function_timer);
            RETURN_IF_ERROR(fn->process_init(input_block, state));
        }
        local_state._child_rows_has_output.resize(input_block->rows(), false);
        local_state.process_next_child_row();
        return Status::OK();
    }

    Status pull(RuntimeState* state, Block* output_block, bool* eos) const override {
        auto& local_state = get_local_state(state);
        RETURN_IF_ERROR(local_state.get_expanded_block(state, output_block, eos));
        local_state.reached_limit(output_block, eos);
        return Status::OK();
    }

private:
    friend class TableFunctionLocalState;

    Status _prepare_output_slot_ids(const TPlanNode& tnode);

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
    [[nodiscard]] inline bool _slot_need_copy(SlotId slot_id) const {
        auto id = _output_slots[slot_id]->id();
        return (id < _output_slot_ids.size()) && (_output_slot_ids[id]);
    }

    // Slots of the child (input) tuple, in tuple order. These are the columns coming from the
    // child operator that may need to be replicated once per generated row. Their count
    // (`_child_slots.size()`) is also the index of the first table function output column in the
    // output block, since the output layout is `[child slots..., tf1, tf2, ...]`.
    std::vector<SlotDescriptor*> _child_slots;
    // All output slots of this node, in tuple order: `[child slots..., table function slots...]`.
    // Used both to build the output block and, via `_slot_need_copy()`, to decide which slots are
    // actually referenced downstream.
    std::vector<SlotDescriptor*> _output_slots;

    VExprContextSPtrs _vfn_ctxs;
    VExprContextSPtrs _expand_conjuncts_ctxs;

    std::vector<TableFunction*> _fns;
    int _fn_num = 0;

    // Indexed by SlotDescriptor::id(): whether that slot id is referenced downstream (present in
    // `tnode.table_function_node.outputSlotIds`). A slot not marked here is pruned and need not be
    // materialized. Populated by _prepare_output_slot_ids().
    std::vector<bool> _output_slot_ids;
    // Positions (indices into `_child_slots`) of child columns that ARE used downstream and so must
    // be replicated per generated row. Computed once in prepare() from `_slot_need_copy()`.
    std::vector<int> _output_slot_indexs;
    // Positions (indices into `_child_slots`) of child columns that are NOT used downstream; these
    // are only grown with default values to keep the block row count consistent, never copied.
    std::vector<int> _useless_slot_indexs;

    // True when the node emits no real output columns and downstream only needs the expanded
    // row count: all output slots are pruned, the only consumer is a constant projection (the
    // `final projections: 1` FE inserts for COUNT(*)), there are no conjuncts/expand-conjuncts,
    // and there is a single table function. When set, the runtime takes the count-only path.
    bool _output_no_columns_eligible = false;
};

} // namespace doris
