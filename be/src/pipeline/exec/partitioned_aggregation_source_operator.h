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
#include "vec/spill/spill_stream.h"

// Forward declare for member pointers; full definition lives in `aggregation_source_operator.h`.
namespace doris::pipeline {
class AggSourceOperatorX;
struct SpillContext;
} // namespace doris::pipeline

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
    SpillPartitionId _current_partition_id;
    bool _has_current_partition = false;

    std::vector<vectorized::Block> _blocks;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    RuntimeProfile::Counter* _memory_usage_reserved = nullptr;
    RuntimeProfile::Counter* _spill_serialize_hash_table_timer = nullptr;
    RuntimeProfile::Counter* _spill_partition_splits = nullptr;

    // Temp structures for hash table serialization during split
    vectorized::MutableColumns _spill_key_columns;
    vectorized::MutableColumns _spill_value_columns;
    vectorized::DataTypes _spill_value_data_types;
    vectorized::Block _spill_block;
    bool _spill_columns_initialized = false;

    void _init_counters();

    // Initialize spill columns for hash table serialization
    Status _init_spill_columns();

    // Reset spill columns after each batch
    void _reset_spill_columns();

    // Serialize hash table batch to block
    template <typename HashTableCtxType, typename KeyType>
    Status _to_block(HashTableCtxType& context, std::vector<KeyType>& keys,
                     std::vector<uint32_t>& hashes,
                     std::vector<vectorized::AggregateDataPtr>& values,
                     const vectorized::AggregateDataPtr null_key_data);

private:
    // Spill recovery helpers for hierarchical partitions.
    //
    // Goal: keep each helper small (and readable), while the overall behavior remains:
    // - pick next leaf partition from shared pending queue
    // - read some spilled blocks (up to MAX_SPILL_WRITE_BATCH_MEM)
    // - return once we have at least one non-empty block to merge
    bool _select_next_leaf_partition();
    Status _read_some_blocks_from_current_partition(RuntimeState* state, bool& has_agg_data,
                                                    size_t& accumulated_blocks_size);
    Status _read_some_blocks_from_stream(RuntimeState* state, vectorized::SpillStreamSPtr& stream,
                                         bool& stream_eos, bool& has_agg_data,
                                         size_t& accumulated_blocks_size);

    Status _recover_blocks_from_disk_impl(RuntimeState* state, bool& has_agg_data,
                                          size_t& accumulated_blocks_size);
};

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

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state,
                         const std::shared_ptr<SpillContext>& spill_context) override;

private:
    friend class PartitionedAggLocalState;

    // Spill-mode helper:
    // When sink spilled, source must recover+merge one leaf partition's spilled blocks into the
    // in-memory agg hash table before producing results.
    Status _maybe_merge_spilled_partitions(RuntimeState* state,
                                           PartitionedAggLocalState& local_state, bool* eos,
                                           bool* should_return);

    // Split current partition and respill all data (hash table + _blocks + remaining spill data)
    // to child partitions. Called by revoke_memory() when memory pressure is high.
    Status _split_and_respill_current_partition(RuntimeState* state,
                                                PartitionedAggLocalState& local_state);

    // Spill hash table data to child partitions during split
    template <typename HashTableCtxType, typename HashTableType>
    Status _spill_hash_table_to_children(
            RuntimeState* state, PartitionedAggLocalState& local_state, HashTableCtxType& context,
            HashTableType& hash_table, const SpillPartitionId& parent_id,
            const std::array<SpillPartitionId, kSpillFanout>& child_ids);

    // Respill _blocks to child partitions based on __spill_hash column
    Status _respill_blocks_to_children(RuntimeState* state, PartitionedAggLocalState& local_state,
                                       const SpillPartitionId& parent_id,
                                       const std::array<SpillPartitionId, kSpillFanout>& child_ids);

    // Respill remaining spill stream data to child partitions
    Status _respill_stream_to_children(RuntimeState* state, PartitionedAggLocalState& local_state,
                                       AggSpillPartition& parent_partition,
                                       const std::array<SpillPartitionId, kSpillFanout>& child_ids);

    std::unique_ptr<AggSourceOperatorX> _agg_source_operator;
};
} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris