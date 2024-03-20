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
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/join_build_sink_operator.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_sink_operator.h" // LocalExchangeChannelIds
#include "pipeline/pipeline_x/operator.h"
#include "vec/runtime/partitioner.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

using PartitionerType = vectorized::XXHashPartitioner<LocalExchangeChannelIds>;

class PartitionedHashJoinSinkOperatorX;

class PartitionedHashJoinSinkLocalState
        : public PipelineXSinkLocalState<PartitionedHashJoinSharedState> {
public:
    using Parent = PartitionedHashJoinSinkOperatorX;
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinSinkLocalState);
    ~PartitionedHashJoinSinkLocalState() override = default;
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status revoke_memory(RuntimeState* state);

protected:
    PartitionedHashJoinSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<PartitionedHashJoinSharedState>(parent, state) {}

    void _spill_to_disk(uint32_t partition_index,
                        const vectorized::SpillStreamSPtr& spilling_stream);

    friend class PartitionedHashJoinSinkOperatorX;

    std::atomic_int _spilling_streams_count {0};
    std::atomic<bool> _spill_status_ok {true};
    std::mutex _spill_lock;

    bool _child_eos {false};

    Status _spill_status;
    std::mutex _spill_status_lock;

    std::unique_ptr<PartitionerType> _partitioner;

    RuntimeProfile::Counter* _partition_timer = nullptr;
    RuntimeProfile::Counter* _partition_shuffle_timer = nullptr;
    RuntimeProfile::Counter* _spill_serialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_disk_timer = nullptr;
    RuntimeProfile::Counter* _spill_data_size = nullptr;
    RuntimeProfile::Counter* _spill_block_count = nullptr;
};

class PartitionedHashJoinSinkOperatorX
        : public JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState> {
public:
    PartitionedHashJoinSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                     const DescriptorTbl& descs, bool use_global_rf,
                                     uint32_t partition_count);

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     PartitionedHashJoinSinkOperatorX::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    bool should_dry_run(RuntimeState* state) override { return false; }

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state) override;

    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }

        return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                               _join_distribution == TJoinDistributionType::COLOCATE
                       ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE,
                                          _distribution_partition_exprs)
                       : DataDistribution(ExchangeType::HASH_SHUFFLE,
                                          _distribution_partition_exprs);
    }

    bool is_shuffled_hash_join() const override {
        return _join_distribution == TJoinDistributionType::PARTITIONED;
    }

private:
    friend class PartitionedHashJoinSinkLocalState;

    const TJoinDistributionType::type _join_distribution;

    std::vector<TExpr> _build_exprs;

    const std::vector<TExpr> _distribution_partition_exprs;
    const TPlanNode _tnode;
    const DescriptorTbl _descriptor_tbl;
    const uint32_t _partition_count;
};

} // namespace pipeline
} // namespace doris