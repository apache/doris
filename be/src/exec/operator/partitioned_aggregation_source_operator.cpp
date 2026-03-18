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

#include "exec/operator/partitioned_aggregation_source_operator.h"

#include <glog/logging.h>

#include <limits>
#include <string>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/operator/aggregation_source_operator.h"
#include "exec/operator/operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_repartitioner.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_profile.h"

namespace doris {

#include "common/compile_check_begin.h"

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    // Counters for partition spill metrics
    _max_partition_level = ADD_COUNTER(custom_profile(), "SpillMaxPartitionLevel", TUnit::UNIT);
    _total_partition_spills = ADD_COUNTER(custom_profile(), "SpillTotalPartitions", TUnit::UNIT);

    init_spill_write_counters();

    // Nothing else to init for repartitioner here; fanout is configured when
    // repartitioner is initialized with key columns during actual repartition.
    return Status::OK();
}

Status PartitionedAggLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    SCOPED_TIMER(_open_timer);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    RETURN_IF_ERROR(_setup_in_memory_agg_op(state));

    return Status::OK();
}

#define UPDATE_COUNTER_FROM_INNER(name) \
    update_profile_from_inner_profile<spilled>(name, custom_profile(), child_profile)

template <bool spilled>
void PartitionedAggLocalState::_update_profile(RuntimeProfile* child_profile) {
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

    Status first_error;
    if (_current_reader) {
        auto st = _current_reader->close();
        if (!st.ok() && first_error.ok()) {
            first_error = st;
        }
        _current_reader.reset();
    }

    // Clean up partition queue resources.
    for (auto& partition : _partition_queue) {
        if (partition.spill_file) {
            ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(partition.spill_file);
        }
    }
    _partition_queue.clear();
    if (_current_partition.spill_file) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(_current_partition.spill_file);
    }
    _current_partition.spill_file.reset();

    auto st = Base::close(state);
    if (!first_error.ok()) {
        return first_error;
    }
    return st;
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
    // copy partition count from session variable so source knows how many
    // spill partitions to expect (used by local states during spill).
    _partition_count = state->spill_aggregation_partition_count();
    // default repartition max depth; can be overridden from session variable
    _repartition_max_depth = state->spill_repartition_max_depth();
    return _agg_source_operator->init(tnode, state);
}

Status PartitionedAggSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    return _agg_source_operator->prepare(state);
}

Status PartitionedAggSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));

    // Centralize shared_state cleanup here so resources are released when
    // the pipeline task finishes, matching the Sort operator pattern.
    auto& local_state = get_local_state(state);
    if (local_state._shared_state) {
        local_state._shared_state->close();
    }

    return _agg_source_operator->close(state);
}

bool PartitionedAggSourceOperatorX::is_serial_operator() const {
    return _agg_source_operator->is_serial_operator();
}

void PartitionedAggSourceOperatorX::update_operator(const TPlanNode& tnode,
                                                    bool followed_by_shuffled_operator,
                                                    bool require_bucket_distribution) {
    _agg_source_operator->update_operator(tnode, followed_by_shuffled_operator,
                                          require_bucket_distribution);
}

DataDistribution PartitionedAggSourceOperatorX::required_data_distribution(
        RuntimeState* state) const {
    return _agg_source_operator->required_data_distribution(state);
}

bool PartitionedAggSourceOperatorX::is_colocated_operator() const {
    return _agg_source_operator->is_colocated_operator();
}
bool PartitionedAggSourceOperatorX::is_shuffled_operator() const {
    return _agg_source_operator->is_shuffled_operator();
}

size_t PartitionedAggSourceOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (!local_state._shared_state->_is_spilled || !local_state._current_partition.spill_file) {
        return 0;
    }

    size_t bytes = 0;
    for (const auto& block : local_state._blocks) {
        bytes += block.allocated_bytes();
    }
    if (local_state._shared_state->_in_mem_shared_state != nullptr &&
        local_state._shared_state->_in_mem_shared_state->agg_data != nullptr) {
        auto* agg_data = local_state._shared_state->_in_mem_shared_state->agg_data.get();
        bytes += std::visit(Overload {[&](std::monostate& arg) -> size_t { return 0; },
                                      [&](auto& agg_method) -> size_t {
                                          return agg_method.hash_table->get_buffer_size_in_bytes();
                                      }},
                            agg_data->method_variant);

        if (auto& aggregate_data_container =
                    local_state._shared_state->_in_mem_shared_state->aggregate_data_container;
            aggregate_data_container) {
            bytes += aggregate_data_container->memory_usage();
        }
    }
    return bytes > state->spill_min_revocable_mem() ? bytes : 0;
}

Status PartitionedAggSourceOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    if (!local_state._shared_state->_is_spilled) {
        return Status::OK();
    }
    VLOG_DEBUG << fmt::format("Query:{}, agg source:{}, task:{}, revoke_memory, hash table size:{}",
                              print_id(state->query_id()), node_id(), state->task_id(),
                              PrettyPrinter::print_bytes(local_state._estimate_memory_usage));

    // Flush hash table + repartition remaining spill files of the current partition.
    RETURN_IF_ERROR(local_state._flush_and_repartition(state));
    local_state._current_partition = AggSpillPartitionInfo {};
    local_state._need_to_setup_partition = true;
    return Status::OK();
}

Status PartitionedAggSourceOperatorX::get_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    Status status;

    SCOPED_TIMER(local_state.exec_time_counter());

    // ── Fast path: not spilled ─────────────────────────────────────────
    if (!local_state._shared_state->_is_spilled) {
        auto* runtime_state = local_state._runtime_state.get();
        local_state._shared_state->_in_mem_shared_state->aggregate_data_container->init_once();
        status = _agg_source_operator->get_block(runtime_state, block, eos);
        RETURN_IF_ERROR(status);
        if (*eos) {
            auto* source_local_state =
                    runtime_state->get_local_state(_agg_source_operator->operator_id());
            local_state._update_profile<false>(source_local_state->custom_profile());
        }
        local_state.reached_limit(block, eos);
        return Status::OK();
    }

    // ── Spilled path ───────────────────────────────────────────────────
    // One-time: move original spill_partitions from shared state into unified queue.
    if (local_state._partition_queue.empty() && local_state._need_to_setup_partition &&
        !local_state._shared_state->_spill_partitions.empty()) {
        local_state._init_partition_queue();
    }

    // Phase 1: Pop next partition from queue if needed.
    if (local_state._need_to_setup_partition) {
        if (local_state._partition_queue.empty()) {
            *eos = true;
            return Status::OK();
        }

        local_state._current_partition = std::move(local_state._partition_queue.front());
        local_state._partition_queue.pop_front();
        local_state._blocks.clear();
        local_state._estimate_memory_usage = 0;

        VLOG_DEBUG << fmt::format(
                "Query:{}, agg source:{}, task:{}, setup partition level:{}, "
                "queue remaining:{}",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._current_partition.level, local_state._partition_queue.size());
        local_state._need_to_setup_partition = false;
    }

    // Phase 2: Recover blocks from disk into _blocks (batch of ~8MB).
    if (local_state._blocks.empty() && local_state._current_partition.spill_file) {
        RETURN_IF_ERROR(
                local_state._recover_blocks_from_partition(state, local_state._current_partition));
        // Return empty block to yield to pipeline scheduler.
        // Pipeline task will check memory and call revoke_memory if needed.
        *eos = false;
        return Status::OK();
    }

    auto* memory_sufficient_dependency = state->get_query_ctx()->get_memory_sufficient_dependency();
    // Phase 3: Merge recovered blocks into hash table.
    if (!local_state._blocks.empty()) {
        size_t merged_rows = 0;
        while (!local_state._blocks.empty()) {
            auto blk = std::move(local_state._blocks.front());
            merged_rows += blk.rows();
            local_state._blocks.erase(local_state._blocks.begin());
            status = _agg_source_operator->merge_with_serialized_key_helper(
                    local_state._runtime_state.get(), &blk);
            RETURN_IF_ERROR(status);

            if (memory_sufficient_dependency && !memory_sufficient_dependency->ready()) {
                break;
            }
        }

        local_state._estimate_memory_usage +=
                _agg_source_operator->get_estimated_memory_size_for_merging(
                        local_state._runtime_state.get(), merged_rows);

        // Return empty block to yield — pipeline task will check memory pressure
        // and call revoke_memory() if the hash table grew too large.
        *eos = false;
        return Status::OK();
    }

    // Phase 4: All spill files consumed and merged — output aggregated results from hash table.
    auto* runtime_state = local_state._runtime_state.get();
    local_state._shared_state->_in_mem_shared_state->aggregate_data_container->init_once();
    bool inner_eos = false;
    RETURN_IF_ERROR(_agg_source_operator->get_block(runtime_state, block, &inner_eos));

    if (inner_eos) {
        auto* source_local_state =
                runtime_state->get_local_state(_agg_source_operator->operator_id());
        local_state._update_profile<true>(source_local_state->custom_profile());

        // Current partition fully output. Reset hash table, pop next partition.
        RETURN_IF_ERROR(_agg_source_operator->reset_hash_table(runtime_state));

        local_state._current_partition = AggSpillPartitionInfo {};
        local_state._estimate_memory_usage = 0;
        local_state._need_to_setup_partition = true;

        if (local_state._partition_queue.empty()) {
            *eos = true;
        }
    }

    local_state.reached_limit(block, eos);
    return Status::OK();
}

// ════════════════════════════════════════════════════════════════════════
// PartitionedAggLocalState implementation
// ════════════════════════════════════════════════════════════════════════

void PartitionedAggLocalState::_init_partition_queue() {
    for (auto& spill_file : _shared_state->_spill_partitions) {
        _partition_queue.emplace_back(std::move(spill_file), /*level=*/0);
        // Track metrics: each queued partition counts as one spill at level 0
        COUNTER_UPDATE(_total_partition_spills, 1);
        _max_partition_level_seen = 0;
        COUNTER_SET(_max_partition_level, int64_t(_max_partition_level_seen));
    }
    _shared_state->_spill_partitions.clear();
}

Status PartitionedAggLocalState::_recover_blocks_from_partition(RuntimeState* state,
                                                                AggSpillPartitionInfo& partition) {
    size_t accumulated_bytes = 0;
    if (!partition.spill_file || state->is_cancelled()) {
        return Status::OK();
    }

    // Create or reuse a persistent reader for this file
    if (!_current_reader) {
        _current_reader = partition.spill_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(_current_reader->open());
    }

    bool eos = false;

    while (!eos && !state->is_cancelled()) {
        Block block;
        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::recover_spill_data", {
            return Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_agg_source recover_spill_data failed");
        });
        RETURN_IF_ERROR(_current_reader->read(&block, &eos));

        if (!block.empty()) {
            accumulated_bytes += block.allocated_bytes();
            _blocks.emplace_back(std::move(block));

            if (accumulated_bytes >= state->spill_buffer_size_bytes()) {
                return Status::OK();
            }
        }
    }

    if (eos) {
        _current_reader.reset();
        partition.spill_file.reset();
    }
    return Status::OK();
}

Status PartitionedAggLocalState::_setup_in_memory_agg_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->resize_op_id_to_local_state(state->max_operator_id());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();

    DCHECK(Base::_shared_state->_in_mem_shared_state);
    LocalStateInfo state_info {.parent_profile = _internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = Base::_shared_state->_in_mem_shared_state,
                               .shared_state_map = {},
                               .task_idx = 0};

    RETURN_IF_ERROR(
            parent._agg_source_operator->setup_local_state(_runtime_state.get(), state_info));

    auto* source_local_state =
            _runtime_state->get_local_state(parent._agg_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    return source_local_state->open(state);
}

Status PartitionedAggLocalState::_flush_hash_table_to_sub_spill_files(RuntimeState* state) {
    auto* runtime_state = _runtime_state.get();
    auto& p = _parent->cast<PartitionedAggSourceOperatorX>();
    auto* in_mem_state = _shared_state->_in_mem_shared_state;

    // setup_output must have been called by the caller (_flush_and_repartition)
    // before calling this function. The repartitioner writes to the persistent output writers.

    in_mem_state->aggregate_data_container->init_once();
    bool inner_eos = false;
    while (!inner_eos && !state->is_cancelled()) {
        Block block;
        RETURN_IF_ERROR(
                p._agg_source_operator->get_serialized_block(runtime_state, &block, &inner_eos));
        if (!block.empty()) {
            RETURN_IF_ERROR(_repartitioner.route_block(state, block));
        }
    }

    RETURN_IF_ERROR(p._agg_source_operator->reset_hash_table(runtime_state));
    return Status::OK();
}

Status PartitionedAggLocalState::_flush_and_repartition(RuntimeState* state) {
    auto& p = _parent->cast<PartitionedAggSourceOperatorX>();
    const int new_level = _current_partition.level + 1;

    if (new_level >= p._repartition_max_depth) {
        return Status::InternalError(
                "query:{}, node:{}, Agg spill repartition exceeded max depth {} during "
                "_flush_and_repartition. Likely due to extreme data skew.",
                print_id(state->query_id()), p.node_id(), p._repartition_max_depth);
    }

    VLOG_DEBUG << fmt::format(
            "Query:{}, agg source:{}, task:{}, _flush_and_repartition: "
            "flushing hash table and repartitioning remaining spill file at level {} -> {}",
            print_id(state->query_id()), p.node_id(), state->task_id(), _current_partition.level,
            new_level);

    {
        auto* source_local_state =
                _runtime_state->get_local_state(p._agg_source_operator->operator_id());
        _update_profile<true>(source_local_state->custom_profile());
    }

    // 1. Create FANOUT output sub-spill-files.
    std::vector<SpillFileSPtr> output_spill_files;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_spill_files(
            state, p.node_id(), fmt::format("agg_repart_l{}", new_level),
            static_cast<int>(p._partition_count), output_spill_files));

    auto* in_mem_state = _shared_state->_in_mem_shared_state;
    size_t num_keys = in_mem_state->probe_expr_ctxs.size();
    std::vector<size_t> key_column_indices(num_keys);
    std::vector<DataTypePtr> key_data_types(num_keys);
    for (size_t i = 0; i < num_keys; ++i) {
        key_column_indices[i] = i;
        key_data_types[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type();
    }

    _repartitioner.init_with_key_columns(std::move(key_column_indices), std::move(key_data_types),
                                         operator_profile(), static_cast<int>(p._partition_count),
                                         new_level);

    // Setup persistent output writers for the repartitioner.
    RETURN_IF_ERROR(_repartitioner.setup_output(state, output_spill_files));

    // 2. Flush the in-memory hash table into the sub-spill-files.
    RETURN_IF_ERROR(_flush_hash_table_to_sub_spill_files(state));

    // 3. Route any in-memory blocks that were recovered but not yet merged
    //    into the hash table. These blocks were already read from the file
    //    by _current_reader and must not be re-read.
    for (auto& blk : _blocks) {
        if (!blk.empty()) {
            RETURN_IF_ERROR(_repartitioner.route_block(state, blk));
        }
    }
    _blocks.clear();

    // 4. Repartition remaining unread data from the spill file.
    //
    // If _current_reader exists, the file has been partially read. Pass
    // the existing reader to repartitioner so it continues from the current
    // position. This avoids re-reading from block 0 and duplicating data
    // already represented by the hash table flush + _blocks routed above.
    if (_current_reader) {
        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(_repartitioner.repartition(state, _current_reader, &done));
        }
        // reader is reset by repartitioner on completion
    } else if (_current_partition.spill_file) {
        // No partial read — repartition the entire file from scratch.
        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(
                    _repartitioner.repartition(state, _current_partition.spill_file, &done));
        }
    }
    _current_reader.reset();
    _current_partition.spill_file.reset();

    RETURN_IF_ERROR(_repartitioner.finalize());

    // 4. Push non-empty sub-partitions into the work queue.
    for (int i = 0; i < static_cast<int>(p._partition_count); ++i) {
        _partition_queue.emplace_back(std::move(output_spill_files[i]), new_level);
        // Metrics
        COUNTER_UPDATE(_total_partition_spills, 1);
        if (new_level > _max_partition_level_seen) {
            _max_partition_level_seen = new_level;
            COUNTER_SET(_max_partition_level, int64_t(_max_partition_level_seen));
        }
    }

    _estimate_memory_usage = 0;
    return Status::OK();
}

bool PartitionedAggLocalState::is_blockable() const {
    return _shared_state->_is_spilled;
}

#include "common/compile_check_end.h"
} // namespace doris
