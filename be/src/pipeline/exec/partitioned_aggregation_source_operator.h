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

#include <deque>
#include <memory>
#include <vector>

#include "common/status.h"
#include "operator.h"
#include "vec/spill/spill_repartitioner.h"
#include "vec/spill/spill_stream.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState;

/// Represents one partition in the multi-level spill queue for aggregation.
/// Unlike Join (which has build + probe), Agg only has a single data flow:
/// spilled aggregation intermediate results stored in one or more SpillStreams.
struct AggSpillPartitionInfo {
    // All spill streams for this partition (may come from multiple spill rounds).
    std::deque<vectorized::SpillStreamSPtr> streams;
    // The depth level in the repartition tree (level-0 = original).
    int level = 0;

    AggSpillPartitionInfo() = default;
    AggSpillPartitionInfo(std::deque<vectorized::SpillStreamSPtr> s, int lvl)
            : streams(std::move(s)), level(lvl) {}

    bool has_data() const {
        for (auto& stream : streams) {
            if (stream && stream->get_written_bytes() > 0) return true;
        }
        return false;
    }

    int64_t total_bytes() const {
        int64_t total = 0;
        for (auto& stream : streams) {
            if (stream) total += stream->get_written_bytes();
        }
        return total;
    }
};

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

    Status setup_in_memory_agg_op(RuntimeState* state);

    template <bool spilled>
    void update_profile(RuntimeProfile* child_profile);

    bool is_blockable() const override;

    /// Flush the current in-memory hash table by draining it as blocks and routing
    /// each block through the repartitioner into the output sub-streams.
    Status flush_hash_table_to_sub_streams(
            RuntimeState* state, std::vector<vectorized::SpillStreamSPtr>& output_streams);

    /// Flush the in-memory hash table into FANOUT sub-streams, repartition remaining
    /// unread streams from `remaining_streams`, and push resulting sub-partitions into
    /// `_partition_queue`. After this call the hash table is reset and
    /// `remaining_streams` is cleared.
    Status flush_and_repartition(RuntimeState* state,
                                 std::deque<vectorized::SpillStreamSPtr>& remaining_streams,
                                 int level);

private:
    friend class PartitionedAggSourceOperatorX;

    /// Move all original spill_partitions from shared state into `_partition_queue`.
    /// Called once when spilled get_block is first entered.
    void _init_partition_queue();

    /// Read up to vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM bytes from `partition.streams` into
    /// `_blocks`. Returns has_data=true if any blocks were read.
    /// Consumes and deletes exhausted streams from the partition.
    Status _recover_blocks_from_partition(RuntimeState* state, AggSpillPartitionInfo& partition,
                                          bool& has_data);

    /// Repartition a partition's streams (without hash table) into FANOUT sub-partitions
    /// and push them to `_partition_queue`.
    Status _repartition_partition(RuntimeState* state, AggSpillPartitionInfo& partition);

    // ── State ──────────────────────────────────────────────────────────
    std::unique_ptr<RuntimeState> _runtime_state;
    bool _opened = false;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    // ── Partition queue (unified for original + repartitioned) ────────
    std::deque<AggSpillPartitionInfo> _partition_queue;
    AggSpillPartitionInfo _current_partition;
    // True when we need to pop the next partition from `_partition_queue`.
    bool _need_to_setup_partition = true;

    // Blocks recovered from disk, pending merge into hash table.
    std::vector<vectorized::Block> _blocks;

    // Estimated in-memory hash table size for the current partition.
    //size_t _estimate_memory_usage = 0;

    // Counters to track spill partition metrics
    RuntimeProfile::Counter* _max_partition_level = nullptr;
    RuntimeProfile::Counter* _total_partition_spills = nullptr;
    int _max_partition_level_seen = 0;

    SpillRepartitioner _repartitioner;
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
    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override;

    DataDistribution required_data_distribution(RuntimeState* state) const override;
    bool is_colocated_operator() const override;
    bool is_shuffled_operator() const override;

    // Returns the current in-memory hash table size for the active partition.
    // The scheduler uses this to decide whether to trigger revoke_memory.
    size_t revocable_mem_size(RuntimeState* state) const override;

    // Called by the pipeline task scheduler under memory pressure. Flushes the
    // current in-memory aggregation hash table to sub-streams and repartitions,
    // freeing the hash table memory so it can be recovered in smaller slices.
    Status revoke_memory(RuntimeState* state) override;

private:
    friend class PartitionedAggLocalState;

    std::unique_ptr<AggSourceOperatorX> _agg_source_operator;
    // number of spill partitions configured for this operator
    size_t _partition_count = 0;
    // max repartition depth (configured from session variable in FE)
    size_t _repartition_max_depth = SpillRepartitioner::MAX_DEPTH;
};
} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
