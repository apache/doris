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
#include "exec/operator/hashjoin_build_sink.h"
#include "exec/operator/hashjoin_probe_operator.h"
#include "exec/operator/join_build_sink_operator.h"
#include "exec/operator/operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/dependency.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "exec/spill/spill_repartitioner.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

class PartitionedHashJoinProbeOperatorX;

/// Represents a spilled partition pair (build + probe file) that needs to be processed
/// during recovery. For multi-level spill, when a partition is too large to fit in
/// memory, it gets repartitioned into FANOUT sub-partitions, each represented by a
/// new JoinSpillPartitionInfo at level + 1.
///
/// Lifecycle of partition progress:
///   build_file == nullptr:
///     - all build-side spill data has been read from disk for this partition
///   probe_file == nullptr:
///     - all probe-side spill data has been read from disk for this partition
///   build_finished = true:
///     - build side has completed hash table construction
///   probe_finished = true:
///     - probe side has completed probing all rows for this partition
///
/// A default-constructed instance has is_valid() == false, representing "no partition".
/// New sub-partitions created by repartitioning start with both flags = false and
/// initialized = true.
struct JoinSpillPartitionInfo {
    // build_file == nullptr means all build data has been read from disk.
    SpillFileSPtr build_file;
    // probe_file == nullptr means all probe data has been read from disk.
    SpillFileSPtr probe_file;
    int level = 0; // 0 = original level-0 partition, 1+ = repartitioned sub-partition

    // Read all build data from disk and finished building the hash table.
    bool build_finished = false;
    // Read all probe data from disk and probed all rows against the hash table.
    bool probe_finished = false;
    // Whether this struct currently represents an active queue partition.
    bool initialized = false;

    JoinSpillPartitionInfo() = default;
    JoinSpillPartitionInfo(SpillFileSPtr build, SpillFileSPtr probe, int lvl)
            : build_file(std::move(build)),
              probe_file(std::move(probe)),
              level(lvl),
              initialized(true) {}

    /// Returns true if this struct currently represents an active partition entry
    /// from the spill queue. A default-constructed partition is "invalid" and
    /// serves as a sentinel meaning "no partition is being processed".
    bool is_valid() const { return initialized; }
};

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

    Status spill_probe_blocks(RuntimeState* state, bool flush_all);

    /// Revoke in-memory build data by repartitioning it and pushing the result back onto
    /// _spill_partition_queue. Used by revoke_memory when child_eos is true (recovery/build
    /// phase) and we have significant in-memory build data that cannot be kept in memory.
    ///
    /// After queue initialization, all partitions are represented as JoinSpillPartitionInfo entries
    /// in _spill_partition_queue. Repartition reads from _current_partition's streams (or the
    /// already-recovered _recovered_build_block) and pushes FANOUT sub-partitions back onto the
    /// queue.
    Status revoke_build_data(RuntimeState* state);

    /// Recover build blocks from a JoinSpillPartitionInfo's build stream (for multi-level recovery).
    Status recover_build_blocks_from_partition(RuntimeState* state,
                                               JoinSpillPartitionInfo& partition_info);
    /// Recover probe blocks from a JoinSpillPartitionInfo's probe stream (for multi-level recovery).
    Status recover_probe_blocks_from_partition(RuntimeState* state,
                                               JoinSpillPartitionInfo& partition_info);

    /// Repartition the current partition's build and probe streams into FANOUT sub-partitions
    /// and push them into _spill_partition_queue for subsequent processing.
    Status repartition_current_partition(RuntimeState* state, JoinSpillPartitionInfo& partition);

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

    bool is_blockable() const override;

    Status acquire_spill_writer(RuntimeState* state, int partition_index,
                                SpillFileWriterSPtr& writer);

    friend class PartitionedHashJoinProbeOperatorX;

private:
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    std::shared_ptr<BasicSharedState> _in_mem_shared_state_sptr;

    std::unique_ptr<Block> _child_block;
    bool _child_eos {false};

    std::vector<std::unique_ptr<MutableBlock>> _partitioned_blocks;
    std::unique_ptr<MutableBlock> _recovered_build_block;

    std::vector<SpillFileSPtr> _probe_spilling_groups;
    std::vector<SpillFileWriterSPtr> _probe_writers;

    std::unique_ptr<PartitionerBase> _partitioner;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    // Persistent readers for recovery across scheduling slices
    SpillFileReaderSPtr _current_build_reader;
    SpillFileReaderSPtr _current_probe_reader;

    // ---- Spill partition queue state ----
    // Whether _spill_partition_queue has been initialized from spilled build groups +
    // _probe_spilling_groups. Set to true the first time pull() enters the spill
    // path after child EOS. Once true, all partitions are accessed via the queue.
    bool _spill_queue_initialized {false};
    // Work queue of spilled partition pairs to process. Populated during
    // initialization from the level-0 spilled streams and also when a partition is
    // too large to build a hash table (repartitioned into FANOUT new entries).
    std::deque<JoinSpillPartitionInfo> _spill_partition_queue;
    // The partition currently being processed from _spill_partition_queue.
    JoinSpillPartitionInfo _current_partition;
    // Repartitioner instance (reused across repartition calls)
    SpillRepartitioner _repartitioner;
    // A partitioner with partition_count = FANOUT for use during repartitioning.
    // The main _partitioner uses the original _partition_count (e.g., 32), which
    // is wrong for repartitioning that needs FANOUT (8) sub-partitions.
    std::unique_ptr<PartitionerBase> _fanout_partitioner;
    std::unique_ptr<PartitionerBase> _build_fanout_partitioner;
    // Whether internal operators need to be set up for the current queue partition.
    bool _need_to_setup_queue_partition {true};
    // Probe blocks recovered from the current queue partition's probe stream.
    std::vector<Block> _queue_probe_blocks;

    RuntimeProfile::Counter* _partition_shuffle_timer = nullptr;
    RuntimeProfile::Counter* _spill_build_rows = nullptr;
    RuntimeProfile::Counter* _spill_build_blocks = nullptr;
    RuntimeProfile::Counter* _spill_build_timer = nullptr;
    RuntimeProfile::Counter* _recovery_build_rows = nullptr;
    RuntimeProfile::Counter* _recovery_level0_build_rows = nullptr;
    RuntimeProfile::Counter* _recovery_build_blocks = nullptr;
    RuntimeProfile::Counter* _recovery_build_timer = nullptr;
    RuntimeProfile::Counter* _spill_probe_rows = nullptr;
    RuntimeProfile::Counter* _spill_probe_blocks = nullptr;
    RuntimeProfile::Counter* _spill_probe_timer = nullptr;
    RuntimeProfile::Counter* _build_rows = nullptr;
    RuntimeProfile::Counter* _recovery_probe_rows = nullptr;
    RuntimeProfile::Counter* _recovery_probe_blocks = nullptr;
    RuntimeProfile::Counter* _recovery_probe_timer = nullptr;

    // Counters to track spill partition metrics
    RuntimeProfile::Counter* _max_partition_level = nullptr;
    RuntimeProfile::Counter* _total_partition_spills = nullptr;
    int _max_partition_level_seen = 0;

    RuntimeProfile::Counter* _probe_blocks_bytes = nullptr;
    RuntimeProfile::Counter* _memory_usage_reserved = nullptr;
    RuntimeProfile::Counter* _get_child_next_timer = nullptr;
};

class PartitionedHashJoinProbeOperatorX final
        : public JoinProbeOperatorX<PartitionedHashJoinProbeLocalState> {
public:
    PartitionedHashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                      const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    [[nodiscard]] Status get_block(RuntimeState* state, Block* block, bool* eos) override;

    Status push(RuntimeState* state, Block* input_block, bool eos) const override;
    Status pull(doris::RuntimeState* state, Block* output_block, bool* eos) const override;

    bool need_more_input_data(RuntimeState* state) const override;
    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
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

    size_t revocable_mem_size(RuntimeState* state) const override;

    // Called by the pipeline task scheduler when memory pressure requires spilling
    // probe-side blocks. Probe-side memory is NOT managed by the sink, so the
    // probe operator must expose this interface so the scheduler can reach it.
    Status revoke_memory(RuntimeState* state) override;

    size_t get_reserve_mem_size(RuntimeState* state) override;

    void set_inner_operators(const std::shared_ptr<HashJoinBuildSinkOperatorX>& sink_operator,
                             const std::shared_ptr<HashJoinProbeOperatorX>& probe_operator) {
        _inner_sink_operator = sink_operator;
        _inner_probe_operator = probe_operator;
    }
    bool is_shuffled_operator() const override {
        return _inner_probe_operator->is_shuffled_operator();
    }
    bool is_colocated_operator() const override {
        return _inner_probe_operator->is_colocated_operator();
    }
    bool followed_by_shuffled_operator() const override {
        return _inner_probe_operator->followed_by_shuffled_operator();
    }

    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override {
        _inner_probe_operator->update_operator(tnode, followed_by_shuffled_operator,
                                               require_bucket_distribution);
    }

private:
    friend class PartitionedHashJoinProbeLocalState;

    /// Setup internal operators using build data from a JoinSpillPartitionInfo
    /// (for multi-level recovery, where build data comes from repartitioned streams).
    [[nodiscard]] Status _setup_internal_operators_from_partition(
            PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const;

    /// Process entries from the _spill_partition_queue.
    /// All spilled partitions (both original level-0 and repartitioned sub-partitions)
    /// are processed via this single path.
    [[nodiscard]] Status _pull_from_spill_queue(PartitionedHashJoinProbeLocalState& local_state,
                                                RuntimeState* state, Block* output_block,
                                                bool* eos) const;

    const TJoinDistributionType::type _join_distribution;

    std::shared_ptr<HashJoinBuildSinkOperatorX> _inner_sink_operator;
    std::shared_ptr<HashJoinProbeOperatorX> _inner_probe_operator;

    // probe expr
    std::vector<TExpr> _probe_exprs;
    std::vector<TExpr> _build_exprs;

    const std::vector<TExpr> _distribution_partition_exprs;

    const TPlanNode _tnode;
    const DescriptorTbl _descriptor_tbl;

    uint32_t _partition_count;
    std::unique_ptr<PartitionerBase> _partitioner;
    // max repartition depth configured per-operator (default to static MAX_DEPTH)
    int _repartition_max_depth = SpillRepartitioner::MAX_DEPTH;
};

#include "common/compile_check_end.h"
} // namespace doris