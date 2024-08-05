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

#include <memory>

#include "common/status.h"
#include "operator.h"

namespace doris {
class RuntimeState;

namespace pipeline {

class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState;

class PartitionedAggLocalState final : public PipelineXSpillLocalState<PartitionedAggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedAggLocalState);
    using Base = PipelineXSpillLocalState<PartitionedAggSharedState>;
    using Parent = PartitionedAggSourceOperatorX;
    PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~PartitionedAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status initiate_merge_spill_partition_agg_data(RuntimeState* state);
    Status setup_in_memory_agg_op(RuntimeState* state);

    void update_profile(RuntimeProfile* child_profile);

protected:
    void _init_counters();

    friend class PartitionedAggSourceOperatorX;
    std::unique_ptr<RuntimeState> _runtime_state;

    bool _opened = false;
    Status _status;
    std::unique_ptr<std::promise<Status>> _spill_merge_promise;
    std::future<Status> _spill_merge_future;
    bool _current_partition_eos = true;
    bool _is_merging = false;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _serialize_result_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_iterate_timer = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;
    RuntimeProfile::Counter* _serialize_data_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;

    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
};
class AggSourceOperatorX;
class PartitionedAggSourceOperatorX : public OperatorX<PartitionedAggLocalState> {
public:
    using Base = OperatorX<PartitionedAggLocalState>;
    PartitionedAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                  const DescriptorTbl& descs);
    ~PartitionedAggSourceOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

private:
    friend class PartitionedAggLocalState;

    std::unique_ptr<AggSourceOperatorX> _agg_source_operator;
};
} // namespace pipeline
} // namespace doris
