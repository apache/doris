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

#include "join_build_sink_operator.h"
#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/join/vhash_join_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class HashJoinBuildSinkBuilder final : public OperatorBuilder<vectorized::HashJoinNode> {
public:
    HashJoinBuildSinkBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
    bool is_sink() const override { return true; }
};

class HashJoinBuildSink final : public StreamingOperator<vectorized::HashJoinNode> {
public:
    HashJoinBuildSink(OperatorBuilderBase* operator_builder, ExecNode* node);
    bool can_write() override { return _node->can_sink_write(); }
    bool is_pending_finish() const override { return !_node->ready_for_finish(); }
};

class HashJoinBuildSinkOperatorX;

class SharedHashTableDependency final : public Dependency {
public:
    using SharedState = HashJoinSharedState;
    ENABLE_FACTORY_CREATOR(SharedHashTableDependency);
    SharedHashTableDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "SharedHashTableBuildDependency", true, query_ctx) {}
    ~SharedHashTableDependency() override = default;
};

class HashJoinBuildSinkLocalState final
        : public JoinBuildSinkLocalState<SharedHashTableDependency, HashJoinBuildSinkLocalState> {
public:
    ENABLE_FACTORY_CREATOR(HashJoinBuildSinkLocalState);
    using Base = JoinBuildSinkLocalState<SharedHashTableDependency, HashJoinBuildSinkLocalState>;
    using Parent = HashJoinBuildSinkOperatorX;
    HashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~HashJoinBuildSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status process_build_block(RuntimeState* state, vectorized::Block& block);

    void init_short_circuit_for_probe();

    bool build_unique() const;
    std::vector<TRuntimeFilterDesc>& runtime_filter_descs() const;
    std::shared_ptr<vectorized::Arena> arena() { return _shared_state->arena; }

    void add_hash_buckets_info(const std::string& info) const {
        _profile->add_info_string("HashTableBuckets", info);
    }
    void add_hash_buckets_filled_info(const std::string& info) const {
        _profile->add_info_string("HashTableFilledBuckets", info);
    }

protected:
    void _hash_table_init(RuntimeState* state);
    void _set_build_ignore_flag(vectorized::Block& block, const std::vector<int>& res_col_ids);
    Status _do_evaluate(vectorized::Block& block, vectorized::VExprContextSPtrs& exprs,
                        RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);
    std::vector<uint16_t> _convert_block_to_null(vectorized::Block& block);
    Status _extract_join_column(vectorized::Block& block,
                                vectorized::ColumnUInt8::MutablePtr& null_map,
                                vectorized::ColumnRawPtrs& raw_ptrs,
                                const std::vector<int>& res_col_ids);
    friend class HashJoinBuildSinkOperatorX;
    template <class HashTableContext, typename Parent>
    friend struct vectorized::ProcessHashTableBuild;
    friend struct vectorized::ProcessRuntimeFilterBuild;

    // build expr
    vectorized::VExprContextSPtrs _build_expr_ctxs;

    std::vector<IRuntimeFilter*> _runtime_filters;
    bool _should_build_hash_table = true;
    int64_t _build_side_mem_used = 0;
    int64_t _build_side_last_mem_used = 0;
    vectorized::MutableBlock _build_side_mutable_block;
    std::shared_ptr<VRuntimeFilterSlots> _runtime_filter_slots;
    bool _has_set_need_null_map_for_build = false;
    bool _build_side_ignore_null = false;
    std::unordered_set<const vectorized::Block*> _inserted_blocks;
    std::shared_ptr<SharedHashTableDependency> _shared_hash_table_dependency;
    std::vector<int> _build_col_ids;

    RuntimeProfile::Counter* _build_table_timer = nullptr;
    RuntimeProfile::Counter* _build_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _build_table_insert_timer = nullptr;
    RuntimeProfile::Counter* _build_side_compute_hash_timer = nullptr;
    RuntimeProfile::Counter* _build_side_merge_block_timer = nullptr;

    RuntimeProfile::Counter* _allocate_resource_timer = nullptr;

    RuntimeProfile::Counter* _memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _build_blocks_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _build_arena_memory_usage = nullptr;
};

class HashJoinBuildSinkOperatorX final
        : public JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState> {
public:
    HashJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                               const DescriptorTbl& descs, bool use_global_rf);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    bool should_dry_run(RuntimeState* state) override {
        return _is_broadcast_join && !state->get_sink_local_state(operator_id())
                                              ->cast<HashJoinBuildSinkLocalState>()
                                              ._should_build_hash_table;
    }

    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        } else if (_is_broadcast_join) {
            return _child_x->ignore_data_distribution()
                           ? DataDistribution(ExchangeType::PASS_TO_ONE)
                           : DataDistribution(ExchangeType::NOOP);
        }
        return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                               _join_distribution == TJoinDistributionType::COLOCATE
                       ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                       : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

private:
    friend class HashJoinBuildSinkLocalState;

    const TJoinDistributionType::type _join_distribution;
    // build expr
    vectorized::VExprContextSPtrs _build_expr_ctxs;
    // mark the build hash table whether it needs to store null value
    std::vector<bool> _store_null_in_hash_table;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    bool _is_broadcast_join = false;
    std::shared_ptr<vectorized::SharedHashTableController> _shared_hashtable_controller;

    vectorized::SharedHashTableContextPtr _shared_hash_table_context = nullptr;
    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::vector<TExpr> _partition_exprs;

    const bool _use_global_rf;
};

} // namespace pipeline
} // namespace doris
