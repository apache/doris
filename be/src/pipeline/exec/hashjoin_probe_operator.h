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
#include "operator.h"
#include "pipeline/exec/join_probe_operator.h"
#include "pipeline/pipeline_x/operator.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class HashJoinProbeOperatorBuilder final : public OperatorBuilder<vectorized::HashJoinNode> {
public:
    HashJoinProbeOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
};

class HashJoinProbeOperator final : public StatefulOperator<HashJoinProbeOperatorBuilder> {
public:
    HashJoinProbeOperator(OperatorBuilderBase*, ExecNode*);
    // if exec node split to: sink, source operator. the source operator
    // should skip `alloc_resource()` function call, only sink operator
    // call the function
    Status open(RuntimeState*) override { return Status::OK(); }
};

class HashJoinProbeLocalState;

using HashTableCtxVariants = std::variant<
        std::monostate,
        vectorized::ProcessHashTableProbe<TJoinOp::INNER_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::LEFT_SEMI_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::LEFT_ANTI_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::LEFT_OUTER_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::FULL_OUTER_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::RIGHT_OUTER_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::CROSS_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::RIGHT_SEMI_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::RIGHT_ANTI_JOIN, HashJoinProbeLocalState>,
        vectorized::ProcessHashTableProbe<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN,
                                          HashJoinProbeLocalState>>;

class HashJoinProbeOperatorX;
class HashJoinProbeLocalState final
        : public JoinProbeLocalState<HashJoinDependency, HashJoinProbeLocalState> {
public:
    using Parent = HashJoinProbeOperatorX;
    ENABLE_FACTORY_CREATOR(HashJoinProbeLocalState);
    HashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent);
    ~HashJoinProbeLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    void prepare_for_next();
    void add_tuple_is_null_column(vectorized::Block* block) override;
    void init_for_probe(RuntimeState* state);
    Status filter_data_and_build_output(RuntimeState* state, vectorized::Block* output_block,
                                        SourceState& source_state, vectorized::Block* temp_block,
                                        bool check_rows_count = true);

    bool have_other_join_conjunct() const;
    bool is_right_semi_anti() const;
    bool is_outer_join() const;
    std::vector<bool>* left_output_slot_flags();
    std::vector<bool>* right_output_slot_flags();
    vectorized::DataTypes right_table_data_types();
    vectorized::DataTypes left_table_data_types();
    bool* has_null_in_build_side() { return &_shared_state->_has_null_in_build_side; }
    std::shared_ptr<std::vector<vectorized::Block>> build_blocks() const {
        return _shared_state->build_blocks;
    }

private:
    void _prepare_probe_block();
    bool _need_probe_null_map(vectorized::Block& block, const std::vector<int>& res_col_ids);
    friend class HashJoinProbeOperatorX;
    template <int JoinOpType, typename Parent>
    friend struct vectorized::ProcessHashTableProbe;

    int _probe_index = -1;
    bool _ready_probe = false;
    bool _probe_eos = false;
    std::atomic<bool> _probe_inited = false;

    vectorized::Block _probe_block;
    vectorized::ColumnRawPtrs _probe_columns;
    // other expr
    vectorized::VExprContextSPtrs _other_join_conjuncts;
    // probe expr
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    std::vector<uint16_t> _probe_column_disguise_null;
    std::vector<uint16_t> _probe_column_convert_to_null;

    bool _need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_probe = false;
    vectorized::ColumnUInt8::MutablePtr _null_map_column;
    // for cases when a probe row matches more than batch size build rows.
    bool _is_any_probe_match_row_output = false;
    std::unique_ptr<HashTableCtxVariants> _process_hashtable_ctx_variants =
            std::make_unique<HashTableCtxVariants>();

    // for full/right outer join
    vectorized::HashTableIteratorVariants _outer_join_pull_visited_iter;
    vectorized::HashTableIteratorVariants _probe_row_match_iter;

    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;
    RuntimeProfile::Counter* _probe_process_hashtable_timer;
    RuntimeProfile::HighWaterMarkCounter* _probe_arena_memory_usage;
    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _process_other_join_conjunct_timer;
};

class HashJoinProbeOperatorX final : public JoinProbeOperatorX<HashJoinProbeLocalState> {
public:
    HashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                           const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status push(RuntimeState* state, vectorized::Block* input_block,
                SourceState source_state) const override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block,
                SourceState& source_state) const override;

    bool need_more_input_data(RuntimeState* state) const override;

private:
    Status _do_evaluate(vectorized::Block& block, vectorized::VExprContextSPtrs& exprs,
                        RuntimeProfile::Counter& expr_call_timer,
                        std::vector<int>& res_col_ids) const;
    friend class HashJoinProbeLocalState;

    // other expr
    vectorized::VExprContextSPtrs _other_join_conjuncts;
    // probe expr
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    bool _probe_ignore_null = false;

    vectorized::DataTypes _right_table_data_types;
    vectorized::DataTypes _left_table_data_types;
    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;
    std::vector<std::string> _right_table_column_names;
};

} // namespace pipeline
} // namespace doris
