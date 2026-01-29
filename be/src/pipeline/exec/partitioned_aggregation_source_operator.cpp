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

#include "partitioned_aggregation_source_operator.h"

#include <glog/logging.h>

#include <algorithm>
#include <string>

#include "aggregation_source_operator.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "partitioned_agg_spill_utils.h"
#include "pipeline/common/agg_utils.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

namespace {

inline AggSpillPartition& get_or_create_agg_partition(
        std::unordered_map<uint32_t, AggSpillPartition>& partitions, const SpillPartitionId& id) {
    auto [it, inserted] = partitions.try_emplace(id.key());
    if (inserted) {
        it->second.id = id;
    }
    return it->second;
}

} // namespace

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    init_spill_write_counters();
    _init_counters();
    return Status::OK();
}

Status PartitionedAggLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    SCOPED_TIMER(_open_timer);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    RETURN_IF_ERROR(setup_in_memory_agg_op(state));
    return Status::OK();
}

void PartitionedAggLocalState::_init_counters() {
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);

    _spill_serialize_hash_table_timer =
            ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillSerializeHashTableTime", 1);
    // Sink-side split reads spilled blocks; ensure read counters exist for SpillReader.
    ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadFileTime", 1);
    ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadDerializeBlockTime", 1);
    ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadBlockCount", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadBlockBytes", TUnit::BYTES, 1);
    ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadFileBytes", TUnit::BYTES, 1);
    ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadRows", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadFileCount", TUnit::UNIT, 1);

    _spill_partition_splits =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "AggPartitionSplits", TUnit::UNIT, 1);
    Base::custom_profile()->add_info_string("AggMaxPartitionDepth", std::to_string(kSpillMaxDepth));
}

#define UPDATE_COUNTER_FROM_INNER(name) \
    update_profile_from_inner_profile<spilled>(name, custom_profile(), child_profile)

template <bool spilled>
void PartitionedAggLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_COUNTER_FROM_INNER("GetResultsTime");
    UPDATE_COUNTER_FROM_INNER("HashTableIterateTime");
    UPDATE_COUNTER_FROM_INNER("InsertKeysToColumnTime");
    UPDATE_COUNTER_FROM_INNER("InsertValuesToColumnTime");
    UPDATE_COUNTER_FROM_INNER("MergeTime");
    UPDATE_COUNTER_FROM_INNER("DeserializeAndMergeTime");
    UPDATE_COUNTER_FROM_INNER("HashTableComputeTime");
    UPDATE_COUNTER_FROM_INNER("HashTableEmplaceTime");
    UPDATE_COUNTER_FROM_INNER("HashTableInputCount");
    UPDATE_COUNTER_FROM_INNER("MemoryUsageHashTable");
    UPDATE_COUNTER_FROM_INNER("HashTableSize");
    UPDATE_COUNTER_FROM_INNER("MemoryUsageContainer");
    UPDATE_COUNTER_FROM_INNER("MemoryUsageArena");
}

#undef UPDATE_COUNTER_FROM_INNER

Status PartitionedAggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    return Base::close(state);
}
PartitionedAggSourceOperatorX::PartitionedAggSourceOperatorX(ObjectPool* pool,
                                                             const TPlanNode& tnode,
                                                             int operator_id,
                                                             const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs) {
    _agg_source_operator = std::make_unique<AggSourceOperatorX>(pool, tnode, operator_id, descs);
}

Status PartitionedAggSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
    _op_name = "PARTITIONED_AGGREGATION_OPERATOR";
    return _agg_source_operator->init(tnode, state);
}

Status PartitionedAggSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    return _agg_source_operator->prepare(state);
}

Status PartitionedAggSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _agg_source_operator->close(state);
}

bool PartitionedAggSourceOperatorX::is_serial_operator() const {
    return _agg_source_operator->is_serial_operator();
}

size_t PartitionedAggSourceOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (!local_state._shared_state->is_spilled || !local_state._has_current_partition) {
        return 0;
    }

    // If current partition reached max depth, no revocable memory
    if (local_state._current_partition_id.level >= kSpillMaxDepth) {
        return 0;
    }

    // If current partition is already split, no revocable memory
    auto it = local_state._shared_state->spill_partitions.find(
            local_state._current_partition_id.key());
    if (it == local_state._shared_state->spill_partitions.end() || it->second.is_split) {
        return 0;
    }

    size_t bytes = 0;
    for (const auto& block : local_state._blocks) {
        bytes += block.allocated_bytes();
    }
    if (local_state._shared_state->in_mem_shared_state != nullptr &&
        local_state._shared_state->in_mem_shared_state->agg_data != nullptr) {
        auto* agg_data = local_state._shared_state->in_mem_shared_state->agg_data.get();
        bytes += std::visit(
                vectorized::Overload {[&](std::monostate& arg) -> size_t { return 0; },
                                      [&](auto& agg_method) -> size_t {
                                          return agg_method.hash_table->get_buffer_size_in_bytes();
                                      }},
                agg_data->method_variant);

        if (auto& aggregate_data_container =
                    local_state._shared_state->in_mem_shared_state->aggregate_data_container;
            aggregate_data_container) {
            bytes += aggregate_data_container->memory_usage();
        }
    }
    return bytes;
}

Status PartitionedAggLocalState::_init_spill_columns() {
    if (_spill_columns_initialized) {
        return Status::OK();
    }
    agg_spill_utils::init_spill_columns(_shared_state->in_mem_shared_state, _spill_key_columns,
                                        _spill_value_columns, _spill_value_data_types);
    _spill_columns_initialized = true;
    return Status::OK();
}

void PartitionedAggLocalState::_reset_spill_columns() {
    agg_spill_utils::reset_spill_columns(_shared_state->in_mem_shared_state, _spill_key_columns,
                                         _spill_value_columns, _spill_value_data_types,
                                         _spill_block);
}

template <typename HashTableCtxType, typename KeyType>
Status PartitionedAggLocalState::_to_block(HashTableCtxType& context, std::vector<KeyType>& keys,
                                           std::vector<uint32_t>& hashes,
                                           std::vector<vectorized::AggregateDataPtr>& values,
                                           const vectorized::AggregateDataPtr null_key_data) {
    return agg_spill_utils::serialize_agg_data_to_block(
            context, keys, hashes, values, null_key_data, _shared_state->in_mem_shared_state,
            _spill_key_columns, _spill_value_columns, _spill_value_data_types, _spill_block);
}

template <typename HashTableCtxType, typename HashTableType>
Status PartitionedAggSourceOperatorX::_spill_hash_table_to_children(
        RuntimeState* state, PartitionedAggLocalState& local_state, HashTableCtxType& context,
        HashTableType& hash_table, const SpillPartitionId& parent_id,
        const std::array<SpillPartitionId, kSpillFanout>& child_ids) {
    auto& partitions = local_state._shared_state->spill_partitions;
    auto* in_mem_state = local_state._shared_state->in_mem_shared_state;
    const auto total_rows = in_mem_state->aggregate_data_container->total_count();
    if (total_rows == 0) {
        return Status::OK();
    }
    LOG(INFO) << fmt::format(
            "Query:{}, agg source:{}, task:{}, spill hash table to children, rows:{}",
            print_id(state->query_id()), node_id(), state->task_id(), total_rows);
    RETURN_IF_ERROR(local_state._init_spill_columns());
    const size_t batch_size = std::min<size_t>(
            1024 * 1024,
            std::max<size_t>(4096, vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM * total_rows /
                                           std::max<size_t>(1, total_rows)));
    struct ChildBatch {
        std::vector<typename HashTableType::key_type> keys;
        std::vector<uint32_t> hashes;
        std::vector<vectorized::AggregateDataPtr> values;
    };
    std::array<ChildBatch, kSpillFanout> child_batches;
    context.init_iterator();
    in_mem_state->aggregate_data_container->init_once();
    size_t row_count = 0;
    auto& iter = in_mem_state->aggregate_data_container->iterator;
    while (iter != in_mem_state->aggregate_data_container->end() && !state->is_cancelled()) {
        const auto& key = iter.template get_key<typename HashTableType::key_type>();
        const auto hash = static_cast<uint32_t>(hash_table.hash(key));
        const auto child_index = spill_partition_index(hash, parent_id.level + 1);
        child_batches[child_index].keys.emplace_back(key);
        child_batches[child_index].hashes.emplace_back(hash);
        child_batches[child_index].values.emplace_back(iter.get_aggregate_data());
        if (++row_count >= batch_size) {
            row_count = 0;
            for (uint32_t i = 0; i < kSpillFanout; ++i) {
                auto& batch = child_batches[i];
                if (batch.keys.size() >= batch_size) {
                    local_state._reset_spill_columns();
                    RETURN_IF_ERROR(local_state._to_block(context, batch.keys, batch.hashes,
                                                          batch.values, nullptr));
                    if (!local_state._spill_block.empty()) {
                        auto& cp = get_or_create_agg_partition(partitions, child_ids[i]);
                        vectorized::SpillStreamSPtr stream;
                        RETURN_IF_ERROR(cp.get_spill_stream(
                                state, node_id(), local_state.operator_profile(), stream));
                        RETURN_IF_ERROR(
                                stream->spill_block(state, local_state._spill_block, false));
                        cp.spilled_bytes += local_state._spill_block.allocated_bytes();
                    }
                    batch.keys.clear();
                    batch.hashes.clear();
                    batch.values.clear();
                }
            }
        }
        ++iter;
    }
    for (uint32_t i = 0; i < kSpillFanout; ++i) {
        auto& batch = child_batches[i];
        if (!batch.keys.empty()) {
            local_state._reset_spill_columns();
            RETURN_IF_ERROR(local_state._to_block(context, batch.keys, batch.hashes, batch.values,
                                                  nullptr));
            if (!local_state._spill_block.empty()) {
                auto& cp = get_or_create_agg_partition(partitions, child_ids[i]);
                vectorized::SpillStreamSPtr stream;
                RETURN_IF_ERROR(cp.get_spill_stream(state, node_id(),
                                                    local_state.operator_profile(), stream));
                RETURN_IF_ERROR(stream->spill_block(state, local_state._spill_block, false));
                cp.spilled_bytes += local_state._spill_block.allocated_bytes();
            }
        }
    }
    if (hash_table.has_null_key_data() && !state->is_cancelled()) {
        const auto child_index = spill_partition_index(kAggSpillNullKeyHash, parent_id.level + 1);
        auto& cp = get_or_create_agg_partition(partitions, child_ids[child_index]);
        local_state._reset_spill_columns();
        std::vector<typename HashTableType::key_type> empty_keys;
        std::vector<uint32_t> empty_hashes;
        std::vector<vectorized::AggregateDataPtr> empty_values;
        RETURN_IF_ERROR(local_state._to_block(
                context, empty_keys, empty_hashes, empty_values,
                hash_table.template get_null_key_data<vectorized::AggregateDataPtr>()));
        if (!local_state._spill_block.empty()) {
            vectorized::SpillStreamSPtr stream;
            RETURN_IF_ERROR(
                    cp.get_spill_stream(state, node_id(), local_state.operator_profile(), stream));
            RETURN_IF_ERROR(stream->spill_block(state, local_state._spill_block, false));
            cp.spilled_bytes += local_state._spill_block.allocated_bytes();
        }
    }
    return Status::OK();
}

Status PartitionedAggSourceOperatorX::revoke_memory(
        RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context) {
    auto& local_state = get_local_state(state);
    LOG(INFO) << fmt::format(
            "Query:{}, agg source:{}, task:{}, revoke_memory, blocks_size:{}, has_partition:{}",
            print_id(state->query_id()), node_id(), state->task_id(), local_state._blocks.size(),
            local_state._has_current_partition);

    // 如果没有 spill 或没有当前 partition，直接返回
    if (!local_state._shared_state->is_spilled || !local_state._has_current_partition) {
        return Status::OK();
    }

    return _split_and_respill_current_partition(state, local_state);
}

Status PartitionedAggSourceOperatorX::_split_and_respill_current_partition(
        RuntimeState* state, PartitionedAggLocalState& local_state) {
    const auto& parent_id = local_state._current_partition_id;

    if (parent_id.level >= kSpillMaxDepth) {
        LOG(WARNING) << fmt::format(
                "Query:{}, agg source:{}, task:{}, partition level {} >= max depth, cannot split",
                print_id(state->query_id()), node_id(), state->task_id(), parent_id.level);
        return Status::OK();
    }

    auto& partitions = local_state._shared_state->spill_partitions;
    auto it = partitions.find(parent_id.key());
    if (it == partitions.end()) {
        DCHECK(local_state._current_partition_eos);
        return Status::OK();
    }

    auto& parent_partition = it->second;
    if (parent_partition.is_split) {
        return Status::OK();
    }

    LOG(INFO) << fmt::format(
            "Query:{}, agg source:{}, task:{}, splitting partition level:{}, path:{}",
            print_id(state->query_id()), node_id(), state->task_id(), parent_id.level,
            parent_id.path);

    std::array<SpillPartitionId, kSpillFanout> child_ids;
    for (uint32_t i = 0; i < kSpillFanout; ++i) {
        child_ids[i] = parent_id.child(i);
        get_or_create_agg_partition(partitions, child_ids[i]);
        local_state._shared_state->pending_partitions.emplace_back(child_ids[i]);
    }

    auto* agg_data = local_state._shared_state->in_mem_shared_state->agg_data.get();
    Status status = std::visit(vectorized::Overload {[&](std::monostate& arg) -> Status {
                                                         // 空 hash table，跳过
                                                         return Status::OK();
                                                     },
                                                     [&](auto& agg_method) -> Status {
                                                         auto& hash_table = *agg_method.hash_table;
                                                         return _spill_hash_table_to_children(
                                                                 state, local_state, agg_method,
                                                                 hash_table, parent_id, child_ids);
                                                     }},
                               agg_data->method_variant);
    RETURN_IF_ERROR(status);

    RETURN_IF_ERROR(_respill_blocks_to_children(state, local_state, parent_id, child_ids));

    RETURN_IF_ERROR(_respill_stream_to_children(state, local_state, parent_partition, child_ids));

    RETURN_IF_ERROR(local_state._shared_state->in_mem_shared_state->reset_hash_table());
    local_state._shared_state->in_mem_shared_state->agg_arena_pool.clear(true);

    COUNTER_UPDATE(local_state._spill_partition_splits, 1);

    local_state._blocks.clear();

    parent_partition.spill_streams.clear();
    parent_partition.spilling_stream.reset();
    parent_partition.is_split = true;
    parent_partition.spilled_bytes = 0;

    local_state._has_current_partition = false;
    local_state._current_partition_eos = true;
    local_state._need_to_merge_data_for_current_partition = true;

    for (const auto& child_id : child_ids) {
        auto& child_partition = get_or_create_agg_partition(partitions, child_id);
        RETURN_IF_ERROR(child_partition.finish_current_spilling(true));
    }

    LOG(INFO) << fmt::format(
            "Query:{}, agg source:{}, task:{}, split partition done, level:{}, path:{}, "
            "children added to pending queue",
            print_id(state->query_id()), node_id(), state->task_id(), parent_id.level,
            parent_id.path);

    return Status::OK();
}

Status PartitionedAggSourceOperatorX::_respill_blocks_to_children(
        RuntimeState* state, PartitionedAggLocalState& local_state,
        const SpillPartitionId& parent_id,
        const std::array<SpillPartitionId, kSpillFanout>& child_ids) {
    if (local_state._blocks.empty()) {
        return Status::OK();
    }

    auto& partitions = local_state._shared_state->spill_partitions;

    for (auto& block : local_state._blocks) {
        if (block.empty()) {
            continue;
        }

        // 获取 __spill_hash 列
        const int hash_pos = block.get_position_by_name("__spill_hash");
        if (hash_pos < 0) {
            return Status::InternalError("agg split requires __spill_hash column in _blocks");
        }

        const auto& hash_col = assert_cast<const vectorized::ColumnInt32&>(
                *block.get_by_position(hash_pos).column);
        const auto* hashes = reinterpret_cast<const uint32_t*>(hash_col.get_data().data());

        // 按 hash 分配到子 partition
        std::vector<std::vector<uint32_t>> partition_indexes(kSpillFanout);
        for (uint32_t r = 0; r < block.rows(); ++r) {
            const auto child_index = spill_partition_index(hashes[r], parent_id.level + 1);
            partition_indexes[child_index].emplace_back(r);
        }

        for (uint32_t i = 0; i < kSpillFanout; ++i) {
            if (partition_indexes[i].empty()) {
                continue;
            }
            auto child_block = vectorized::MutableBlock::create_unique(block.clone_empty());
            RETURN_IF_ERROR(child_block->add_rows(
                    &block, partition_indexes[i].data(),
                    partition_indexes[i].data() + partition_indexes[i].size()));
            auto out = child_block->to_block();

            auto& child_partition = get_or_create_agg_partition(partitions, child_ids[i]);
            vectorized::SpillStreamSPtr stream;
            RETURN_IF_ERROR(child_partition.get_spill_stream(
                    state, node_id(), local_state.operator_profile(), stream));
            RETURN_IF_ERROR(stream->spill_block(state, out, false));
            child_partition.spilled_bytes += out.allocated_bytes();
        }
    }

    return Status::OK();
}

Status PartitionedAggSourceOperatorX::_respill_stream_to_children(
        RuntimeState* state, PartitionedAggLocalState& local_state,
        AggSpillPartition& parent_partition,
        const std::array<SpillPartitionId, kSpillFanout>& child_ids) {
    auto& partitions = local_state._shared_state->spill_partitions;
    const auto parent_level = parent_partition.id.level;

    for (auto& parent_stream : parent_partition.spill_streams) {
        if (!parent_stream) {
            continue;
        }
        parent_stream->set_read_counters(local_state.operator_profile());
        bool eos = false;
        while (!eos && !state->is_cancelled()) {
            vectorized::Block block;
            RETURN_IF_ERROR(parent_stream->read_next_block_sync(&block, &eos));
            if (block.empty()) {
                continue;
            }

            const int hash_pos = block.get_position_by_name("__spill_hash");
            if (hash_pos < 0) {
                return Status::InternalError("agg split requires __spill_hash column");
            }

            const auto& hash_col = assert_cast<const vectorized::ColumnInt32&>(
                    *block.get_by_position(hash_pos).column);
            const auto* hashes = reinterpret_cast<const uint32_t*>(hash_col.get_data().data());

            std::vector<std::vector<uint32_t>> partition_indexes(kSpillFanout);
            for (uint32_t r = 0; r < block.rows(); ++r) {
                const auto child_index = spill_partition_index(hashes[r], parent_level + 1);
                partition_indexes[child_index].emplace_back(r);
            }

            for (uint32_t i = 0; i < kSpillFanout; ++i) {
                if (partition_indexes[i].empty()) {
                    continue;
                }
                auto child_block = vectorized::MutableBlock::create_unique(block.clone_empty());
                RETURN_IF_ERROR(child_block->add_rows(
                        &block, partition_indexes[i].data(),
                        partition_indexes[i].data() + partition_indexes[i].size()));
                auto out = child_block->to_block();

                auto& child_partition = get_or_create_agg_partition(partitions, child_ids[i]);
                vectorized::SpillStreamSPtr stream;
                RETURN_IF_ERROR(child_partition.get_spill_stream(
                        state, node_id(), local_state.operator_profile(), stream));
                RETURN_IF_ERROR(stream->spill_block(state, out, false));
                child_partition.spilled_bytes += out.allocated_bytes();
            }
        }
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(parent_stream);
    }

    return Status::OK();
}

Status PartitionedAggSourceOperatorX::_maybe_merge_spilled_partitions(
        RuntimeState* state, PartitionedAggLocalState& local_state, bool* eos,
        bool* should_return) {
    *should_return = false;
    if (!local_state._shared_state->is_spilled ||
        !local_state._need_to_merge_data_for_current_partition) {
        return Status::OK();
    }

    if (!local_state._has_current_partition && !local_state._select_next_leaf_partition()) {
        *eos = true;
        *should_return = true;
        return Status::OK();
    }

    // If we still have spill data to read for the current partition, request recovery first.
    if (local_state._blocks.empty() && !local_state._current_partition_eos) {
        bool has_recovering_data = false;
        RETURN_IF_ERROR(local_state.recover_blocks_from_disk(state, has_recovering_data));
        *eos = !has_recovering_data;
        *should_return = true;
        return Status::OK();
    }

    // Merge recovered blocks into the in-memory agg hash table.
    if (!local_state._blocks.empty()) {
        auto* mem_dep = state->get_query_ctx()->get_memory_sufficient_dependency();
        if (mem_dep && !mem_dep->ready()) {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, agg source:{}, task:{}, memory not sufficient, pause merge",
                    print_id(state->query_id()), node_id(), state->task_id());
            *should_return = true;
            return Status::OK();
        }

        size_t merged_rows = 0;
        while (!local_state._blocks.empty()) {
            auto block_ = std::move(local_state._blocks.front());
            merged_rows += block_.rows();
            local_state._blocks.erase(local_state._blocks.begin());
            // Drop the internal spill routing column before merging into the in-memory agg.
            const int spill_hash_pos = block_.get_position_by_name("__spill_hash");
            if (spill_hash_pos >= 0) {
                block_.erase(spill_hash_pos);
            }
            RETURN_IF_ERROR(_agg_source_operator->merge_with_serialized_key_helper(
                    local_state._runtime_state.get(), &block_));
        }
        local_state._estimate_memory_usage +=
                _agg_source_operator->get_estimated_memory_size_for_merging(
                        local_state._runtime_state.get(), merged_rows);

        if (!local_state._current_partition_eos) {
            *should_return = true;
            return Status::OK();
        }
    }

    local_state._need_to_merge_data_for_current_partition = false;
    return Status::OK();
}

Status PartitionedAggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                bool* eos) {
    auto& local_state = get_local_state(state);
    local_state.copy_shared_spill_profile();
    Status status;
    Defer defer {[&]() {
        if (!status.ok() || *eos) {
            local_state._shared_state->close();
        }
    }};

    SCOPED_TIMER(local_state.exec_time_counter());

    bool should_return = false;
    RETURN_IF_ERROR(_maybe_merge_spilled_partitions(state, local_state, eos, &should_return));
    if (should_return) {
        return Status::OK();
    }

    // not spilled in sink or current partition still has data
    auto* runtime_state = local_state._runtime_state.get();
    local_state._shared_state->in_mem_shared_state->aggregate_data_container->init_once();
    status = _agg_source_operator->get_block(runtime_state, block, eos);
    if (!local_state._shared_state->is_spilled) {
        auto* source_local_state =
                local_state._runtime_state->get_local_state(_agg_source_operator->operator_id());
        local_state.update_profile<false>(source_local_state->custom_profile());
    }

    RETURN_IF_ERROR(status);
    if (*eos) {
        if (local_state._shared_state->is_spilled) {
            local_state._has_current_partition = false;
            auto* source_local_state = local_state._runtime_state->get_local_state(
                    _agg_source_operator->operator_id());
            local_state.update_profile<true>(source_local_state->custom_profile());

            if (!local_state._shared_state->pending_partitions.empty()) {
                local_state._current_partition_eos = false;
                local_state._need_to_merge_data_for_current_partition = true;
                status = local_state._shared_state->in_mem_shared_state->reset_hash_table();
                RETURN_IF_ERROR(status);
                *eos = false;
            }
        }
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}

Status PartitionedAggLocalState::setup_in_memory_agg_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->resize_op_id_to_local_state(state->max_operator_id());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();

    DCHECK(Base::_shared_state->in_mem_shared_state);
    LocalStateInfo state_info {.parent_profile = _internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = Base::_shared_state->in_mem_shared_state,
                               .shared_state_map = {},
                               .task_idx = 0};

    RETURN_IF_ERROR(
            parent._agg_source_operator->setup_local_state(_runtime_state.get(), state_info));

    auto* source_local_state =
            _runtime_state->get_local_state(parent._agg_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    return source_local_state->open(state);
}

bool PartitionedAggLocalState::_select_next_leaf_partition() {
    // Select the next leaf partition to merge.
    //
    // - pending_partitions is filled with level-0 base partitions on init, and with ALL children
    //   when a partition is split on sink side.
    // - We skip internal nodes (is_split==true) and nodes with no spill streams.
    while (!_shared_state->pending_partitions.empty()) {
        auto id = _shared_state->pending_partitions.front();
        _shared_state->pending_partitions.pop_front();
        auto it = _shared_state->spill_partitions.find(id.key());
        if (it == _shared_state->spill_partitions.end()) {
            continue;
        }
        if (it->second.is_split) {
            continue;
        }
        if (it->second.spill_streams.empty()) {
            continue;
        }
        _current_partition_id = id;
        _has_current_partition = true;
        _current_partition_eos = false;
        return true;
    }
    return false;
}

Status PartitionedAggLocalState::_read_some_blocks_from_stream(RuntimeState* state,
                                                               vectorized::SpillStreamSPtr& stream,
                                                               bool& stream_eos, bool& has_agg_data,
                                                               size_t& accumulated_blocks_size) {
    stream->set_read_counters(operator_profile());
    vectorized::Block block;
    stream_eos = false;
    Status st;
    size_t read_limit =
            static_cast<size_t>(std::max<int64_t>(state->spill_recover_max_read_bytes(), 1));
    read_limit = std::min(read_limit, vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM);
    while (!stream_eos && !state->is_cancelled()) {
        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::recover_spill_data", {
            st = Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_agg_source recover_spill_data failed");
        });
        if (st.ok()) {
            st = stream->read_next_block_sync(&block, &stream_eos);
        }
        RETURN_IF_ERROR(st);

        if (!block.empty()) {
            has_agg_data = true;
            accumulated_blocks_size += block.allocated_bytes();
            _blocks.emplace_back(std::move(block));
            if (accumulated_blocks_size >= read_limit) {
                break;
            }
        }

        auto* mem_dep = state->get_query_ctx()->get_memory_sufficient_dependency();
        if (mem_dep && !mem_dep->ready()) {
            break;
        }
    }
    return Status::OK();
}

Status PartitionedAggLocalState::_read_some_blocks_from_current_partition(
        RuntimeState* state, bool& has_agg_data, size_t& accumulated_blocks_size) {
    auto it = _shared_state->spill_partitions.find(_current_partition_id.key());
    if (it == _shared_state->spill_partitions.end() || it->second.is_split) {
        _has_current_partition = false;
        return Status::OK();
    }

    auto& partition = it->second;
    while (!partition.spill_streams.empty() && !state->is_cancelled() && !has_agg_data) {
        auto& stream = partition.spill_streams.front();
        bool stream_eos = false;
        RETURN_IF_ERROR(_read_some_blocks_from_stream(state, stream, stream_eos, has_agg_data,
                                                      accumulated_blocks_size));

        if (stream_eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            partition.spill_streams.pop_front();
        }
    }

    if (partition.spill_streams.empty()) {
        _current_partition_eos = true;
        _shared_state->spill_partitions.erase(it);
    }
    return Status::OK();
}

Status PartitionedAggLocalState::_recover_blocks_from_disk_impl(RuntimeState* state,
                                                                bool& has_agg_data,
                                                                size_t& accumulated_blocks_size) {
    while (!state->is_cancelled() && !has_agg_data && !_current_partition_eos) {
        RETURN_IF_ERROR(_read_some_blocks_from_current_partition(state, has_agg_data,
                                                                 accumulated_blocks_size));
    }
    VLOG_DEBUG << fmt::format(
            "Query:{}, agg probe:{}, task:{}, recover partitioned finished, partitions left:{}, "
            "bytes read:{}",
            print_id(state->query_id()), _parent->node_id(), state->task_id(),
            _shared_state->pending_partitions.size(), accumulated_blocks_size);
    return Status::OK();
}

Status PartitionedAggLocalState::recover_blocks_from_disk(RuntimeState* state, bool& has_data) {
    const auto query_id = state->query_id();

    if (_shared_state->pending_partitions.empty() && !_has_current_partition) {
        _shared_state->close();
        has_data = false;
        return Status::OK();
    }

    has_data = true;
    auto spill_func = [this, state, query_id] {
        Status status;
        Defer defer {[&]() {
            if (!status.ok() || state->is_cancelled()) {
                if (!status.ok()) {
                    LOG(WARNING) << fmt::format(
                            "Query:{}, agg probe:{}, task:{}, recover agg data error:{}",
                            print_id(query_id), _parent->node_id(), state->task_id(), status);
                }
                _shared_state->close();
            }
        }};
        bool has_agg_data = false;
        size_t accumulated_blocks_size = 0;
        status = _recover_blocks_from_disk_impl(state, has_agg_data, accumulated_blocks_size);
        RETURN_IF_ERROR(status);
        return Status::OK();
    };

    auto exception_catch_func = [this, state, spill_func, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::merge_spill_data_cancel", {
            auto st = Status::InternalError(
                    "fault_inject partitioned_agg_source "
                    "merge spill data canceled");
            state->get_query_ctx()->cancel(st);
            return st;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
        LOG_IF(INFO, !status.ok()) << fmt::format(
                "Query:{}, agg probe:{}, task:{}, recover exception:{}", print_id(query_id),
                _parent->node_id(), state->task_id(), status.to_string());
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_source submit_func failed");
    });

    VLOG_DEBUG << fmt::format(
            "Query:{}, agg probe:{}, task:{}, begin to recover, partitions left:{}, ",
            print_id(query_id), _parent->node_id(), state->task_id(),
            _shared_state->pending_partitions.size());
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

bool PartitionedAggLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline