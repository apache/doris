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
#include <limits>
#include <memory>

#include "aggregation_sink_operator.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "util/pretty_printer.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
class PartitionedAggSinkOperatorX;
class PartitionedAggSinkLocalState
        : public PipelineXSpillSinkLocalState<PartitionedAggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedAggSinkLocalState);
    using Base = PipelineXSpillSinkLocalState<PartitionedAggSharedState>;
    using Parent = PartitionedAggSinkOperatorX;

    PartitionedAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~PartitionedAggSinkLocalState() override = default;

    friend class PartitionedAggSinkOperatorX;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    Status revoke_memory(RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context);

    Status setup_in_memory_agg_op(RuntimeState* state);

    template <bool spilled>
    void update_profile(RuntimeProfile* child_profile);

    bool is_blockable() const override;

    template <typename KeyType>
    struct TmpSpillInfo {
        std::vector<KeyType> keys_;
        std::vector<vectorized::AggregateDataPtr> values_;
    };

    template <typename HashTableCtxType, typename HashTableType>
    Status _spill_hash_table(RuntimeState* state, HashTableCtxType& context,
                             HashTableType& hash_table, const size_t size_to_revoke, bool eos);

    template <typename HashTableCtxType, typename KeyType>
    Status _spill_partition(RuntimeState* state, HashTableCtxType& context,
                            AggSpillPartitionSPtr& spill_partition, std::vector<KeyType>& keys,
                            std::vector<vectorized::AggregateDataPtr>& values,
                            const vectorized::AggregateDataPtr null_key_data, bool is_last);

    template <typename HashTableCtxType, typename KeyType>
    Status to_block(HashTableCtxType& context, std::vector<KeyType>& keys,
                    std::vector<vectorized::AggregateDataPtr>& values,
                    const vectorized::AggregateDataPtr null_key_data);

    void _reset_tmp_data();
    void _clear_tmp_data();
    void _init_counters();

    std::unique_ptr<RuntimeState> _runtime_state;

    // temp structures during spilling
    vectorized::MutableColumns key_columns_;
    vectorized::MutableColumns value_columns_;
    vectorized::DataTypes value_data_types_;
    vectorized::Block block_;
    vectorized::Block key_block_;
    vectorized::Block value_block_;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    RuntimeProfile::Counter* _memory_usage_reserved = nullptr;

    RuntimeProfile::Counter* _spill_serialize_hash_table_timer = nullptr;

    std::atomic<bool> _eos = false;
};

class PartitionedAggSinkOperatorX : public DataSinkOperatorX<PartitionedAggSinkLocalState> {
public:
    PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                const TPlanNode& tnode, const DescriptorTbl& descs,
                                bool require_bucket_distribution);
    ~PartitionedAggSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<PartitionedAggSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    DataDistribution required_data_distribution() const override {
        return _agg_sink_operator->required_data_distribution();
    }

    bool require_data_distribution() const override {
        return _agg_sink_operator->require_data_distribution();
    }

    Status set_child(OperatorPtr child) override {
        RETURN_IF_ERROR(DataSinkOperatorX<PartitionedAggSinkLocalState>::set_child(child));
        return _agg_sink_operator->set_child(child);
    }
    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state,
                         const std::shared_ptr<SpillContext>& spill_context) override;

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

private:
    friend class PartitionedAggSinkLocalState;
    std::unique_ptr<AggSinkOperatorX> _agg_sink_operator;

    size_t _spill_partition_count = 32;
};
#include "common/compile_check_end.h"
} // namespace doris::pipeline