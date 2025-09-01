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

#include "operator.h"
#include "sort_sink_operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
class SpillSortSinkLocalState;
class SpillSortSinkOperatorX;

class SpillSortSinkLocalState : public PipelineXSpillSinkLocalState<SpillSortSharedState> {
    ENABLE_FACTORY_CREATOR(SpillSortSinkLocalState);

public:
    using Base = PipelineXSpillSinkLocalState<SpillSortSharedState>;
    using Parent = SpillSortSinkOperatorX;
    SpillSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~SpillSortSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    Status setup_in_memory_sort_op(RuntimeState* state);
    [[nodiscard]] size_t get_reserve_mem_size(RuntimeState* state, bool eos);
    Status revoke_memory(RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context);

private:
    void _init_counters();
    void update_profile(RuntimeProfile* child_profile);

    friend class SpillSortSinkOperatorX;

    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    RuntimeProfile::Counter* _spill_merge_sort_timer = nullptr;

    vectorized::SpillStreamSPtr _spilling_stream;

    std::atomic<bool> _eos = false;
};

class SpillSortSinkOperatorX final : public DataSinkOperatorX<SpillSortSinkLocalState> {
public:
    using LocalStateType = SpillSortSinkLocalState;
    SpillSortSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id, const TPlanNode& tnode,
                           const DescriptorTbl& descs, bool require_bucket_distribution);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<SpillSortSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        return _sort_sink_operator->required_data_distribution();
    }
    bool require_data_distribution() const override {
        return _sort_sink_operator->require_data_distribution();
    }
    Status set_child(OperatorPtr child) override {
        RETURN_IF_ERROR(DataSinkOperatorX<SpillSortSinkLocalState>::set_child(child));
        return _sort_sink_operator->set_child(child);
    }

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state,
                         const std::shared_ptr<SpillContext>& spill_context) override;

    using DataSinkOperatorX<LocalStateType>::node_id;
    using DataSinkOperatorX<LocalStateType>::operator_id;
    using DataSinkOperatorX<LocalStateType>::get_local_state;

private:
    friend class SpillSortSinkLocalState;
    std::unique_ptr<SortSinkOperatorX> _sort_sink_operator;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline