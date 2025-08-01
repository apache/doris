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

#include <cstdint>
#include <memory>

#include "common/be_mock_util.h"
#include "common/status.h"
#include "operator.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/join_build_sink_operator.h"
#include "pipeline/exec/spill_utils.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class PartitionedHashJoinProbeOperatorX;

class PartitionedHashJoinProbeLocalState MOCK_REMOVE(final)
        : public PipelineXSpillLocalState<PartitionedHashJoinSharedState> {
public:
    using Parent = PartitionedHashJoinProbeOperatorX;
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinProbeLocalState);
    PartitionedHashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent);
    ~PartitionedHashJoinProbeLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status spill_probe_blocks(RuntimeState* state);

    Status recover_build_blocks_from_disk(RuntimeState* state, uint32_t partition_index,
                                          bool& has_data);
    Status recover_probe_blocks_from_disk(RuntimeState* state, uint32_t partition_index,
                                          bool& has_data);

    Status finish_spilling(uint32_t partition_index);

    template <bool spilled>
    void update_build_custom_profile(RuntimeProfile* child_profile);

    template <bool spilled>
    void update_probe_custom_profile(RuntimeProfile* child_profile);

    template <bool spilled>
    void update_build_common_profile(RuntimeProfile* child_profile);

    template <bool spilled>
    void update_probe_common_profile(RuntimeProfile* child_profile);

    std::string debug_string(int indentation_level = 0) const override;

    MOCK_FUNCTION void update_profile_from_inner();

    void init_counters();

    friend class PartitionedHashJoinProbeOperatorX;

private:
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    std::shared_ptr<BasicSharedState> _in_mem_shared_state_sptr;
    uint32_t _partition_cursor {0};

    std::unique_ptr<vectorized::Block> _child_block;
    bool _child_eos {false};

    std::vector<std::unique_ptr<vectorized::MutableBlock>> _partitioned_blocks;
    std::unique_ptr<vectorized::MutableBlock> _recovered_build_block;
    std::map<uint32_t, std::vector<vectorized::Block>> _probe_blocks;

    std::vector<vectorized::SpillStreamSPtr> _probe_spilling_streams;

    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    bool _need_to_setup_internal_operators {true};

    RuntimeProfile::Counter* _partition_timer = nullptr;
    RuntimeProfile::Counter* _partition_shuffle_timer = nullptr;
    RuntimeProfile::Counter* _spill_build_rows = nullptr;
    RuntimeProfile::Counter* _spill_build_blocks = nullptr;
    RuntimeProfile::Counter* _spill_build_timer = nullptr;
    RuntimeProfile::Counter* _recovery_build_rows = nullptr;
    RuntimeProfile::Counter* _recovery_build_blocks = nullptr;
    RuntimeProfile::Counter* _recovery_build_timer = nullptr;
    RuntimeProfile::Counter* _spill_probe_rows = nullptr;
    RuntimeProfile::Counter* _spill_probe_blocks = nullptr;
    RuntimeProfile::Counter* _spill_probe_timer = nullptr;
    RuntimeProfile::Counter* _recovery_probe_rows = nullptr;
    RuntimeProfile::Counter* _recovery_probe_blocks = nullptr;
    RuntimeProfile::Counter* _recovery_probe_timer = nullptr;

    RuntimeProfile::Counter* _probe_blocks_bytes = nullptr;
    RuntimeProfile::Counter* _memory_usage_reserved = nullptr;
    RuntimeProfile::Counter* _get_child_next_timer = nullptr;
};

class PartitionedHashJoinProbeOperatorX final
        : public JoinProbeOperatorX<PartitionedHashJoinProbeLocalState> {
public:
    PartitionedHashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                      const DescriptorTbl& descs, uint32_t partition_count);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    [[nodiscard]] Status get_block(RuntimeState* state, vectorized::Block* block,
                                   bool* eos) override;

    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block,
                bool* eos) const override;

    bool need_more_input_data(RuntimeState* state) const override;
    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }
        return (_join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                                _join_distribution == TJoinDistributionType::COLOCATE
                        ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE,
                                           _distribution_partition_exprs)
                        : DataDistribution(ExchangeType::HASH_SHUFFLE,
                                           _distribution_partition_exprs));
    }

    bool is_shuffled_operator() const override {
        return _join_distribution == TJoinDistributionType::PARTITIONED;
    }

    size_t revocable_mem_size(RuntimeState* state) const override;

    size_t get_reserve_mem_size(RuntimeState* state) override;

    void set_inner_operators(const std::shared_ptr<HashJoinBuildSinkOperatorX>& sink_operator,
                             const std::shared_ptr<HashJoinProbeOperatorX>& probe_operator) {
        _inner_sink_operator = sink_operator;
        _inner_probe_operator = probe_operator;
    }
    bool require_data_distribution() const override {
        return _inner_probe_operator->require_data_distribution();
    }

private:
    Status _revoke_memory(RuntimeState* state);

    size_t _revocable_mem_size(RuntimeState* state, bool force = false) const;

    friend class PartitionedHashJoinProbeLocalState;

    [[nodiscard]] Status _setup_internal_operators(PartitionedHashJoinProbeLocalState& local_state,
                                                   RuntimeState* state) const;

    bool _should_revoke_memory(RuntimeState* state) const;

    const TJoinDistributionType::type _join_distribution;

    std::shared_ptr<HashJoinBuildSinkOperatorX> _inner_sink_operator;
    std::shared_ptr<HashJoinProbeOperatorX> _inner_probe_operator;

    // probe expr
    std::vector<TExpr> _probe_exprs;

    const std::vector<TExpr> _distribution_partition_exprs;

    const TPlanNode _tnode;
    const DescriptorTbl _descriptor_tbl;

    const uint32_t _partition_count;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris