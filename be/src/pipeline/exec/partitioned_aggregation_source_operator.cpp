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

#include <string>

#include "aggregation_source_operator.h"
#include "common/exception.h"
#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _init_counters();
    _spill_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                  "AggSourceSpillDependency", true);
    state->get_task()->add_spill_dependency(_spill_dependency.get());

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
    _get_results_timer = ADD_TIMER(profile(), "GetResultsTime");
    _serialize_result_timer = ADD_TIMER(profile(), "SerializeResultTime");
    _hash_table_iterate_timer = ADD_TIMER(profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(profile(), "InsertKeysToColumnTime");
    _serialize_data_timer = ADD_TIMER(profile(), "SerializeDataTime");
    _hash_table_size_counter = ADD_COUNTER_WITH_LEVEL(profile(), "HashTableSize", TUnit::UNIT, 1);

    _merge_timer = ADD_TIMER(profile(), "MergeTime");
    _deserialize_data_timer = ADD_TIMER(profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(profile(), "HashTableEmplaceTime");
    _hash_table_input_counter =
            ADD_COUNTER_WITH_LEVEL(profile(), "HashTableInputCount", TUnit::UNIT, 1);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(profile(), "HashTableMemoryUsage", TUnit::BYTES, 1);

    _memory_usage_container =
            ADD_COUNTER_WITH_LEVEL(profile(), "MemoryUsageContainer", TUnit::BYTES, 1);
    _memory_usage_arena = ADD_COUNTER_WITH_LEVEL(profile(), "MemoryUsageArena", TUnit::BYTES, 1);
}

#define UPDATE_PROFILE(counter, name)                           \
    do {                                                        \
        auto* child_counter = child_profile->get_counter(name); \
        if (child_counter != nullptr) {                         \
            COUNTER_SET(counter, child_counter->value());       \
        }                                                       \
    } while (false)

void PartitionedAggLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE(_get_results_timer, "GetResultsTime");
    UPDATE_PROFILE(_serialize_result_timer, "SerializeResultTime");
    UPDATE_PROFILE(_hash_table_iterate_timer, "HashTableIterateTime");
    UPDATE_PROFILE(_insert_keys_to_column_timer, "InsertKeysToColumnTime");
    UPDATE_PROFILE(_serialize_data_timer, "SerializeDataTime");
    UPDATE_PROFILE(_hash_table_size_counter, "HashTableSize");
    UPDATE_PROFILE(_hash_table_memory_usage, "HashTableMemoryUsage");
    UPDATE_PROFILE(_memory_usage_container, "MemoryUsageContainer");
    UPDATE_PROFILE(_memory_usage_arena, "MemoryUsageArena");
}

Status PartitionedAggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    dec_running_big_mem_op_num(state);
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

Status PartitionedAggSourceOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::open(state));
    return _agg_source_operator->open(state);
}

Status PartitionedAggSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _agg_source_operator->close(state);
}

bool PartitionedAggSourceOperatorX::is_serial_operator() const {
    return _agg_source_operator->is_serial_operator();
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

    local_state.inc_running_big_mem_op_num(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    if (local_state._shared_state->is_spilled &&
        local_state._need_to_merge_data_for_current_partition) {
        if (local_state._blocks.empty() && !local_state._current_partition_eos) {
            bool has_recovering_data = false;
            status = local_state.recover_blocks_from_disk(state, has_recovering_data);
            RETURN_IF_ERROR(status);
            *eos = !has_recovering_data;
            return Status::OK();
        } else if (!local_state._blocks.empty()) {
            size_t merged_rows = 0;
            while (!local_state._blocks.empty()) {
                auto block = std::move(local_state._blocks.front());
                merged_rows += block.rows();
                local_state._blocks.erase(local_state._blocks.begin());
                status = _agg_source_operator->merge_with_serialized_key_helper<false>(
                        local_state._runtime_state.get(), &block);
                RETURN_IF_ERROR(status);
            }
            local_state._estimate_memory_usage +=
                    _agg_source_operator->get_estimated_memory_size_for_merging(
                            local_state._runtime_state.get(), merged_rows);

            if (!local_state._current_partition_eos) {
                return Status::OK();
            }
        }

        local_state._need_to_merge_data_for_current_partition = false;
    }

    // not spilled in sink or current partition still has data
    auto* runtime_state = local_state._runtime_state.get();
    local_state._shared_state->in_mem_shared_state->aggregate_data_container->init_once();
    status = _agg_source_operator->get_block(runtime_state, block, eos);
    RETURN_IF_ERROR(status);
    if (local_state._runtime_state) {
        auto* source_local_state =
                local_state._runtime_state->get_local_state(_agg_source_operator->operator_id());
        local_state.update_profile(source_local_state->profile());
    }
    if (*eos) {
        if (local_state._shared_state->is_spilled &&
            !local_state._shared_state->spill_partitions.empty()) {
            local_state._current_partition_eos = false;
            local_state._need_to_merge_data_for_current_partition = true;
            status = local_state._shared_state->in_mem_shared_state->reset_hash_table();
            RETURN_IF_ERROR(status);
            *eos = false;
        }
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}

Status PartitionedAggLocalState::setup_in_memory_agg_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->resize_op_id_to_local_state(state->max_operator_id());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();

    DCHECK(Base::_shared_state->in_mem_shared_state);
    LocalStateInfo state_info {
            _internal_runtime_profile.get(), {}, Base::_shared_state->in_mem_shared_state, {}, 0};

    RETURN_IF_ERROR(
            parent._agg_source_operator->setup_local_state(_runtime_state.get(), state_info));

    auto* source_local_state =
            _runtime_state->get_local_state(parent._agg_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    return source_local_state->open(state);
}

Status PartitionedAggLocalState::recover_blocks_from_disk(RuntimeState* state, bool& has_data) {
    const auto query_id = state->query_id();

    if (_shared_state->spill_partitions.empty()) {
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
                    LOG(WARNING) << "Query " << print_id(query_id) << " agg node "
                                 << _parent->node_id() << " recover agg data error: " << status;
                }
                _shared_state->close();
            }
        }};
        bool has_agg_data = false;
        size_t accumulated_blocks_size = 0;
        while (!state->is_cancelled() && !has_agg_data &&
               !_shared_state->spill_partitions.empty()) {
            for (auto& stream : _shared_state->spill_partitions[0]->spill_streams_) {
                stream->set_read_counters(profile());
                vectorized::Block block;
                bool eos = false;
                while (!eos && !state->is_cancelled()) {
                    {
                        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::recover_spill_data",
                                        {
                                            status = Status::Error<INTERNAL_ERROR>(
                                                    "fault_inject partitioned_agg_source "
                                                    "recover_spill_data failed");
                                        });
                        if (status.ok()) {
                            status = stream->read_next_block_sync(&block, &eos);
                        }
                    }
                    RETURN_IF_ERROR(status);

                    if (!block.empty()) {
                        has_agg_data = true;
                        accumulated_blocks_size += block.allocated_bytes();
                        _blocks.emplace_back(std::move(block));

                        if (accumulated_blocks_size >=
                            vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                            break;
                        }
                    }
                }

                _current_partition_eos = eos;

                if (_current_partition_eos) {
                    (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                    _shared_state->spill_partitions.pop_front();
                }
            }
        }
        return status;
    };

    auto exception_catch_func = [spill_func, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::merge_spill_data_cancel", {
            auto st = Status::InternalError(
                    "fault_inject partitioned_agg_source "
                    "merge spill data canceled");
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, st);
            return st;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_source submit_func failed");
    });
    _spill_dependency->block();

    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::make_shared<SpillRecoverRunnable>(state, _spill_dependency, _runtime_profile.get(),
                                                   _shared_state->shared_from_this(),
                                                   exception_catch_func));
}
} // namespace doris::pipeline
