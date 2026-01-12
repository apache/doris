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
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState;

class PartitionedAggLocalState MOCK_REMOVE(final)
        : public PipelineXSpillLocalState<PartitionedAggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedAggLocalState);
    using Base = PipelineXSpillLocalState<PartitionedAggSharedState>;
    using Parent = PartitionedAggSourceOperatorX;
    PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~PartitionedAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status recover_blocks_from_disk(RuntimeState* state, bool& has_data);
    Status setup_in_memory_agg_op(RuntimeState* state);

    template <bool spilled>
    void update_profile(RuntimeProfile* child_profile);

    bool is_blockable() const override;

protected:
    friend class PartitionedAggSourceOperatorX;
    std::unique_ptr<RuntimeState> _runtime_state;

    bool _opened = false;
    std::unique_ptr<std::promise<Status>> _spill_merge_promise;
    std::future<Status> _spill_merge_future;
    bool _current_partition_eos = true;
    bool _need_to_merge_data_for_current_partition = true;

    std::vector<vectorized::Block> _blocks;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
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

    Status close(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    bool is_serial_operator() const override;

private:
    friend class PartitionedAggLocalState;

    std::unique_ptr<AggSourceOperatorX> _agg_source_operator;
};
} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
