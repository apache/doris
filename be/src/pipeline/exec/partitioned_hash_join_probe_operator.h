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

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/join_build_sink_operator.h"
#include "pipeline/exec/spill_utils.h"

namespace doris {
class RuntimeState;

namespace pipeline {

class PartitionedHashJoinProbeOperatorX;

class PartitionedHashJoinProbeLocalState final
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

    Status recovery_build_blocks_from_disk(RuntimeState* state, uint32_t partition_index,
                                           bool& has_data);
    Status recovery_probe_blocks_from_disk(RuntimeState* state, uint32_t partition_index,
                                           bool& has_data);

    Status finish_spilling(uint32_t partition_index);

    void update_build_profile(RuntimeProfile* child_profile);
    void update_probe_profile(RuntimeProfile* child_profile);

    std::string debug_string(int indentation_level = 0) const override;

    friend class PartitionedHashJoinProbeOperatorX;

private:
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    std::shared_ptr<BasicSharedState> _in_mem_shared_state_sptr;
    uint32_t _partition_cursor {0};

    std::unique_ptr<vectorized::Block> _child_block;
    bool _child_eos {false};

    std::mutex _spill_lock;
    Status _spill_status;

    std::atomic<int> _spilling_task_count {0};
    std::atomic<bool> _spill_status_ok {true};

    std::vector<std::unique_ptr<vectorized::MutableBlock>> _partitioned_blocks;
    std::map<uint32_t, std::vector<vectorized::Block>> _probe_blocks;

    std::vector<vectorized::SpillStreamSPtr> _probe_spilling_streams;

    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    bool _need_to_setup_internal_operators {true};

    RuntimeProfile::Counter* _spill_and_partition_label = nullptr;
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

    RuntimeProfile::Counter* _spill_serialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_disk_timer = nullptr;
    RuntimeProfile::Counter* _spill_data_size = nullptr;
    RuntimeProfile::Counter* _spill_block_count = nullptr;

    RuntimeProfile::Counter* _build_phase_label = nullptr;
    RuntimeProfile::Counter* _build_rows_counter = nullptr;
    RuntimeProfile::Counter* _publish_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* _runtime_filter_compute_timer = nullptr;

    RuntimeProfile::Counter* _build_table_timer = nullptr;
    RuntimeProfile::Counter* _build_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _build_table_insert_timer = nullptr;
    RuntimeProfile::Counter* _build_side_compute_hash_timer = nullptr;
    RuntimeProfile::Counter* _build_side_merge_block_timer = nullptr;

    RuntimeProfile::Counter* _allocate_resource_timer = nullptr;

    RuntimeProfile::Counter* _probe_phase_label = nullptr;
    RuntimeProfile::Counter* _probe_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _probe_next_timer = nullptr;
    RuntimeProfile::Counter* _probe_side_output_timer = nullptr;
    RuntimeProfile::Counter* _probe_process_hashtable_timer = nullptr;
    RuntimeProfile::Counter* _search_hashtable_timer = nullptr;
    RuntimeProfile::Counter* _init_probe_side_timer = nullptr;
    RuntimeProfile::Counter* _build_side_output_timer = nullptr;
    RuntimeProfile::Counter* _process_other_join_conjunct_timer = nullptr;
    RuntimeProfile::Counter* _probe_timer = nullptr;
    RuntimeProfile::Counter* _probe_rows_counter = nullptr;
    RuntimeProfile::Counter* _join_filter_timer = nullptr;
    RuntimeProfile::Counter* _build_output_block_timer = nullptr;
};

class PartitionedHashJoinProbeOperatorX final
        : public JoinProbeOperatorX<PartitionedHashJoinProbeLocalState> {
public:
    PartitionedHashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                      const DescriptorTbl& descs, uint32_t partition_count);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

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

    bool is_shuffled_hash_join() const override {
        return _join_distribution == TJoinDistributionType::PARTITIONED;
    }

    size_t revocable_mem_size(RuntimeState* state) const override;

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

    friend class PartitionedHashJoinProbeLocalState;

    [[nodiscard]] Status _setup_internal_operators(PartitionedHashJoinProbeLocalState& local_state,
                                                   RuntimeState* state) const;
    [[nodiscard]] Status _setup_internal_operator_for_non_spill(
            PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state);

    bool _should_revoke_memory(RuntimeState* state) const;

    void _update_profile_from_internal_states(
            PartitionedHashJoinProbeLocalState& local_state) const;

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
} // namespace doris