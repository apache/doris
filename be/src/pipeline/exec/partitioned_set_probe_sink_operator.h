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
#include "pipeline/exec/set_probe_sink_operator.h"
#include "pipeline/exec/set_sink_operator.h"
#include "pipeline/exec/spill_utils.h"
#include "vec/runtime/partitioner.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
template <class HashTableContext, bool is_intersected>
struct HashTableProbe;
} // namespace vectorized

namespace pipeline {

template <bool is_intersect>
class PartitionedSetProbeSinkOperatorX;

template <bool is_intersect>
class PartitionedSetProbeSinkLocalState final
        : public PipelineXSpillSinkLocalState<PartitionedSetSharedState> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedSetProbeSinkLocalState);
    using Base = PipelineXSpillSinkLocalState<PartitionedSetSharedState>;
    using Parent = PartitionedSetProbeSinkOperatorX<is_intersect>;

    PartitionedSetProbeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;

    [[nodiscard]] Status revoke_memory(RuntimeState* state, bool force = false);

    [[nodiscard]] Status partition_block(vectorized::Block* block, RuntimeState* state);
    [[nodiscard]] Status async_spill_block(vectorized::Block&& block, RuntimeState* state,
                                           uint32_t partition_idx);

    [[nodiscard]] Status setup_inner_operator(RuntimeState* state);

private:
    friend class PartitionedSetProbeSinkOperatorX<is_intersect>;

    void _finalize_probe();
    [[nodiscard]] Status _make_spill_streams_eof();

    const uint32_t _partition_count {32};
    std::unique_ptr<SpillPartitionerType> _partitioner;
    std::atomic_int _spilling_tasks_count {0};
    AtomicStatus _spill_status;
    bool _child_eos {false};

    std::vector<std::unique_ptr<vectorized::MutableBlock>> _partitioned_blocks;
    std::vector<vectorized::SpillStreamSPtr> _spill_streams;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    RuntimeState* inner_runtime_state;

    RuntimeProfile::Counter* _spill_rows_counter {nullptr};
};

template <bool is_intersect>
class PartitionedSetProbeSinkOperatorX final
        : public DataSinkOperatorX<PartitionedSetProbeSinkLocalState<is_intersect>> {
public:
    using Base = DataSinkOperatorX<PartitionedSetProbeSinkLocalState<is_intersect>>;
    using DataSinkOperatorXBase::operator_id;
    using Base::get_local_state;
    using typename Base::LocalState;

    friend class PartitionedSetProbeSinkLocalState<is_intersect>;
    PartitionedSetProbeSinkOperatorX(int child_id, int sink_id, ObjectPool* pool,
                                     const TPlanNode& tnode, const DescriptorTbl& descs)
            : Base(sink_id, tnode.node_id, tnode.node_id),
              _cur_child_id(child_id),
              _is_colocate(is_intersect ? tnode.intersect_node.is_colocate
                                        : tnode.except_node.is_colocate),
              _partition_exprs(is_intersect ? tnode.intersect_node.result_expr_lists[child_id]
                                            : tnode.except_node.result_expr_lists[child_id]) {}
    ~PartitionedSetProbeSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError(
                "{} should not init with TDataSink",
                DataSinkOperatorX<PartitionedSetProbeSinkLocalState<is_intersect>>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        return _is_colocate ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                            : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

    [[nodiscard]] Status revoke_memory(RuntimeState* state) override;
    [[nodiscard]] size_t revocable_mem_size(RuntimeState* state) const override;

    std::shared_ptr<BasicSharedState> create_shared_state() const override { return nullptr; }

    void set_inner_operator(std::shared_ptr<SetProbeSinkOperatorX<is_intersect>> inner_operator) {
        _inner_sink_operator = std::move(inner_operator);
    }

private:
    const int _cur_child_id;
    const bool _is_colocate;
    const std::vector<TExpr> _partition_exprs;
    using OperatorBase::_child_x;

    std::shared_ptr<SetProbeSinkOperatorX<is_intersect>> _inner_sink_operator;
};

} // namespace pipeline
} // namespace doris
