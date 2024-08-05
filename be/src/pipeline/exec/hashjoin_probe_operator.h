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

namespace doris {
class RuntimeState;

namespace pipeline {

class HashJoinProbeLocalState;

using HashTableCtxVariants =
        std::variant<std::monostate, ProcessHashTableProbe<TJoinOp::INNER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_SEMI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_ANTI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::FULL_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::CROSS_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_SEMI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_ANTI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN>>;

class HashJoinProbeOperatorX;
class HashJoinProbeLocalState final
        : public JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState> {
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
    Status filter_data_and_build_output(RuntimeState* state, vectorized::Block* output_block,
                                        bool* eos, vectorized::Block* temp_block,
                                        bool check_rows_count = true);

    bool have_other_join_conjunct() const;
    bool is_right_semi_anti() const;
    bool is_outer_join() const;
    std::vector<bool>* left_output_slot_flags();
    std::vector<bool>* right_output_slot_flags();
    vectorized::DataTypes right_table_data_types();
    vectorized::DataTypes left_table_data_types();
    bool* has_null_in_build_side() { return &_shared_state->_has_null_in_build_side; }
    const std::shared_ptr<vectorized::Block>& build_block() const {
        return _shared_state->build_block;
    }
    bool empty_right_table_shortcut() const {
        // !Base::_projections.empty() means nereids planner
        return _shared_state->empty_right_table_need_probe_dispose && !Base::_projections.empty();
    }
    std::string debug_string(int indentation_level) const override;

private:
    void _prepare_probe_block();
    bool _need_probe_null_map(vectorized::Block& block, const std::vector<int>& res_col_ids);
    std::vector<uint16_t> _convert_block_to_null(vectorized::Block& block);
    Status _extract_join_column(vectorized::Block& block, const std::vector<int>& res_col_ids);
    friend class HashJoinProbeOperatorX;
    template <int JoinOpType>
    friend struct ProcessHashTableProbe;

    int _probe_index = -1;
    uint32_t _build_index = 0;
    bool _ready_probe = false;
    bool _probe_eos = false;
    int _last_probe_match;

    // For mark join, last probe index of null mark
    int _last_probe_null_mark;

    vectorized::Block _probe_block;
    vectorized::ColumnRawPtrs _probe_columns;
    // other expr
    vectorized::VExprContextSPtrs _other_join_conjuncts;

    vectorized::VExprContextSPtrs _mark_join_conjuncts;

    std::vector<vectorized::ColumnPtr> _key_columns_holder;

    // probe expr
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    std::vector<uint16_t> _probe_column_disguise_null;
    std::vector<uint16_t> _probe_column_convert_to_null;

    bool _need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_probe = false;
    vectorized::ColumnUInt8::MutablePtr _null_map_column;
    std::unique_ptr<HashTableCtxVariants> _process_hashtable_ctx_variants =
            std::make_unique<HashTableCtxVariants>();

    RuntimeProfile::Counter* _probe_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _probe_next_timer = nullptr;
    RuntimeProfile::Counter* _probe_side_output_timer = nullptr;
    RuntimeProfile::Counter* _probe_process_hashtable_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _probe_arena_memory_usage = nullptr;
    RuntimeProfile::Counter* _search_hashtable_timer = nullptr;
    RuntimeProfile::Counter* _init_probe_side_timer = nullptr;
    RuntimeProfile::Counter* _build_side_output_timer = nullptr;
    RuntimeProfile::Counter* _process_other_join_conjunct_timer = nullptr;
};

class HashJoinProbeOperatorX final : public JoinProbeOperatorX<HashJoinProbeLocalState> {
public:
    HashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                           const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block,
                bool* eos) const override;

    bool need_more_input_data(RuntimeState* state) const override;
    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }
        return _is_broadcast_join
                       ? DataDistribution(ExchangeType::PASSTHROUGH)
                       : (_join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                                          _join_distribution == TJoinDistributionType::COLOCATE
                                  ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE,
                                                     _partition_exprs)
                                  : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs));
    }

    bool is_shuffled_hash_join() const override {
        return _join_distribution == TJoinDistributionType::PARTITIONED;
    }
    bool require_data_distribution() const override {
        return _join_distribution != TJoinDistributionType::BROADCAST &&
               _join_distribution != TJoinDistributionType::NONE;
    }

private:
    Status _do_evaluate(vectorized::Block& block, vectorized::VExprContextSPtrs& exprs,
                        RuntimeProfile::Counter& expr_call_timer,
                        std::vector<int>& res_col_ids) const;
    friend class HashJoinProbeLocalState;

    const TJoinDistributionType::type _join_distribution;

    const bool _is_broadcast_join;
    // other expr
    vectorized::VExprContextSPtrs _other_join_conjuncts;

    vectorized::VExprContextSPtrs _mark_join_conjuncts;

    // probe expr
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    bool _probe_ignore_null = false;

    std::vector<bool> _should_convert_to_nullable;

    vectorized::DataTypes _right_table_data_types;
    vectorized::DataTypes _left_table_data_types;
    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;
    std::vector<std::string> _right_table_column_names;
    const std::vector<TExpr> _partition_exprs;
};

} // namespace pipeline
} // namespace doris
