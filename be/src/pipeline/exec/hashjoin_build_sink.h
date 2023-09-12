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

class HashJoinBuildSink final : public StreamingOperator<HashJoinBuildSinkBuilder> {
public:
    HashJoinBuildSink(OperatorBuilderBase* operator_builder, ExecNode* node);
    bool can_write() override { return _node->can_sink_write(); }
    bool is_pending_finish() const override { return !_node->ready_for_finish(); }
};

class HashJoinBuildSinkOperatorX;

class HashJoinBuildSinkLocalState final
        : public JoinBuildSinkLocalState<HashJoinDependency, HashJoinBuildSinkLocalState> {
public:
    ENABLE_FACTORY_CREATOR(HashJoinBuildSinkLocalState);
    using Parent = HashJoinBuildSinkOperatorX;
    HashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~HashJoinBuildSinkLocalState() = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status process_build_block(RuntimeState* state, vectorized::Block& block, uint8_t offset);

    void init_short_circuit_for_probe();
    HashJoinBuildSinkOperatorX* join_build() { return (HashJoinBuildSinkOperatorX*)_parent; }

protected:
    void _hash_table_init(RuntimeState* state);
    void _set_build_ignore_flag(vectorized::Block& block, const std::vector<int>& res_col_ids);
    friend class HashJoinBuildSinkOperatorX;
    friend struct vectorized::HashJoinBuildContext;
    friend struct vectorized::RuntimeFilterContext;

    // build expr
    vectorized::VExprContextSPtrs _build_expr_ctxs;

    std::vector<IRuntimeFilter*> _runtime_filters;
    bool _should_build_hash_table = true;
    uint8_t _build_block_idx = 0;
    int64_t _build_side_mem_used = 0;
    int64_t _build_side_last_mem_used = 0;
    vectorized::MutableBlock _build_side_mutable_block;
    std::shared_ptr<VRuntimeFilterSlots> _runtime_filter_slots = nullptr;
    bool _has_set_need_null_map_for_build = false;
    bool _build_side_ignore_null = false;
    size_t _build_bf_cardinality = 0;
    std::unordered_map<const vectorized::Block*, std::vector<int>> _inserted_rows;

    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _build_table_convert_timer;
    RuntimeProfile::Counter* _build_buckets_counter;
    RuntimeProfile::Counter* _build_buckets_fill_counter;

    RuntimeProfile::Counter* _build_side_compute_hash_timer;
    RuntimeProfile::Counter* _build_side_merge_block_timer;
    RuntimeProfile::Counter* _build_runtime_filter_timer;

    RuntimeProfile::Counter* _build_collisions_counter;

    RuntimeProfile::Counter* _open_timer;
    RuntimeProfile::Counter* _allocate_resource_timer;

    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _build_blocks_memory_usage;
    RuntimeProfile::Counter* _hash_table_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _build_arena_memory_usage;
};

class HashJoinBuildSinkOperatorX final
        : public JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState> {
public:
    HashJoinBuildSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                               const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    bool can_write(RuntimeState* state) override {
        if (state->get_sink_local_state(id())
                    ->cast<HashJoinBuildSinkLocalState>()
                    ._should_build_hash_table) {
            return true;
        }
        return _shared_hash_table_context && _shared_hash_table_context->signaled;
    }

    bool should_dry_run(RuntimeState* state) override {
        auto tmp = _is_broadcast_join && !state->get_sink_local_state(id())
                                                  ->cast<HashJoinBuildSinkLocalState>()
                                                  ._should_build_hash_table;
        return tmp;
    }

private:
    friend class HashJoinBuildSinkLocalState;
    friend struct vectorized::HashJoinBuildContext;
    friend struct vectorized::RuntimeFilterContext;

    // build expr
    vectorized::VExprContextSPtrs _build_expr_ctxs;
    // mark the build hash table whether it needs to store null value
    std::vector<bool> _store_null_in_hash_table;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    vectorized::Sizes _build_key_sz;

    bool _is_broadcast_join = false;
    std::shared_ptr<vectorized::SharedHashTableController> _shared_hashtable_controller = nullptr;

    vectorized::SharedHashTableContextPtr _shared_hash_table_context = nullptr;
    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;

    std::atomic_bool _probe_open_finish = false;
    bool _probe_ignore_null = false;
};

} // namespace pipeline
} // namespace doris
