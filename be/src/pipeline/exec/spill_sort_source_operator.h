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
class ExecNode;
class RuntimeState;

namespace pipeline {
class SpillSortSourceOperatorX;
class SpillSortLocalState;

class SpillSortLocalState final : public PipelineXSpillLocalState<SpillSortSharedState> {
public:
    ENABLE_FACTORY_CREATOR(SpillSortLocalState);
    using Base = PipelineXSpillLocalState<SpillSortSharedState>;
    using Parent = SpillSortSourceOperatorX;
    SpillSortLocalState(RuntimeState* state, OperatorXBase* parent);
    ~SpillSortLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status setup_in_memory_sort_op(RuntimeState* state);

    Status initiate_merge_sort_spill_streams(RuntimeState* state);

protected:
    int _calc_spill_blocks_to_merge() const;
    Status _create_intermediate_merger(int num_blocks,
                                       const vectorized::SortDescription& sort_description);
    friend class SpillSortSourceOperatorX;
    std::unique_ptr<RuntimeState> _runtime_state;

    bool _opened = false;
    Status _status;

    int64_t _external_sort_bytes_threshold = 134217728; // 128M
    std::vector<vectorized::SpillStreamSPtr> _current_merging_streams;
    std::unique_ptr<vectorized::VSortedRunMerger> _merger;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    // counters for spill merge sort
    RuntimeProfile::Counter* _spill_timer = nullptr;
    RuntimeProfile::Counter* _spill_merge_sort_timer = nullptr;
    RuntimeProfile::Counter* _spill_serialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_disk_timer = nullptr;
    RuntimeProfile::Counter* _spill_data_size = nullptr;
    RuntimeProfile::Counter* _spill_block_count = nullptr;
};
class SortSourceOperatorX;
class SpillSortSourceOperatorX : public OperatorX<SpillSortLocalState> {
public:
    using Base = OperatorX<SpillSortLocalState>;
    SpillSortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                             const DescriptorTbl& descs);
    ~SpillSortSourceOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

private:
    friend class SpillSortLocalState;

    std::unique_ptr<SortSourceOperatorX> _sort_source_operator;
};
} // namespace pipeline
} // namespace doris