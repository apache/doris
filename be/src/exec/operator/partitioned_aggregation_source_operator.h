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
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_repartitioner.h"
#include "operator.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState;

/// Represents one partition in the multi-level spill queue for aggregation.
/// Unlike Join (which has build + probe), Agg only has a single data flow:
/// spilled aggregation intermediate results stored in one SpillFile.
struct AggSpillPartitionInfo {
    // The spill file for this partition.
    SpillFileSPtr spill_file;
    // The depth level in the repartition tree (level-0 = original).
    int level = 0;

    AggSpillPartitionInfo() = default;
    AggSpillPartitionInfo(SpillFileSPtr s, int lvl) : spill_file(std::move(s)), level(lvl) {}
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

    bool is_blockable() const override;

private:
    friend class PartitionedAggSourceOperatorX;

    Status _setup_in_memory_agg_op(RuntimeState* state);

    template <bool spilled>
    void _update_profile(RuntimeProfile* child_profile);

    /// Flush the current in-memory hash table by draining it as blocks and routing
    /// each block through the repartitioner into the output sub-spill-files.
    Status _flush_hash_table_to_sub_spill_files(RuntimeState* state);

    /// Flush the in-memory hash table into FANOUT sub-spill-files, repartition remaining
    /// unread spill files from `remaining_spill_files`, and push resulting sub-partitions into
    /// `_partition_queue`. After this call the hash table is reset and
    /// `remaining_spill_files` is cleared.
    Status _flush_and_repartition(RuntimeState* state);

    /// Move all original spill_partitions from shared state into `_partition_queue`.
    /// Called once when spilled get_block is first entered.
    void _init_partition_queue();

    /// Read up to SpillFile::MAX_SPILL_WRITE_BATCH_MEM bytes from `partition.spill_files` into
    /// `_blocks`. Returns has_data=true if any blocks were read.
    /// Consumes and deletes exhausted spill files from the partition.
    Status _recover_blocks_from_partition(RuntimeState* state, AggSpillPartitionInfo& partition);

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
    std::vector<Block> _blocks;

    // Counters to track spill partition metrics
    RuntimeProfile::Counter* _max_partition_level = nullptr;
    RuntimeProfile::Counter* _total_partition_spills = nullptr;
    int _max_partition_level_seen = 0;

    SpillRepartitioner _repartitioner;

    // Persistent reader for _recover_blocks_from_partition (survives across yield calls)
    SpillFileReaderSPtr _current_reader;
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

    Status get_block(RuntimeState* state, Block* block, bool* eos) override;

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
    // current in-memory aggregation hash table to sub-spill-files and repartitions,
    // freeing the hash table memory so it can be recovered in smaller slices.
    Status revoke_memory(RuntimeState* state) override;

private:
    friend class PartitionedAggLocalState;

    std::unique_ptr<AggSourceOperatorX> _agg_source_operator;
    // number of spill partitions configured for this operator
    size_t _partition_count = 0;
    // max repartition depth (configured from session variable in FE)
    int _repartition_max_depth = SpillRepartitioner::MAX_DEPTH;
};
#include "common/compile_check_end.h"
} // namespace doris
