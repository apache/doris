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
#include "runtime/fragment_mgr.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _init_counters();
    return Status::OK();
}

Status PartitionedAggLocalState::open(RuntimeState* state) {
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    RETURN_IF_ERROR(setup_in_memory_agg_op(state));
    return Base::open(state);
}

void PartitionedAggLocalState::_init_counters() {
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    _get_results_timer = ADD_TIMER(profile(), "GetResultsTime");
    _serialize_result_timer = ADD_TIMER(profile(), "SerializeResultTime");
    _hash_table_iterate_timer = ADD_TIMER(profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(profile(), "InsertKeysToColumnTime");
    _serialize_data_timer = ADD_TIMER(profile(), "SerializeDataTime");
    _hash_table_size_counter = ADD_COUNTER(profile(), "HashTableSize", TUnit::UNIT);

    _merge_timer = ADD_TIMER(profile(), "MergeTime");
    _deserialize_data_timer = ADD_TIMER(profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(profile(), "HashTableInputCount", TUnit::UNIT);
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

Status PartitionedAggSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    return _agg_source_operator->prepare(state);
}

Status PartitionedAggSourceOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::open(state));
    return _agg_source_operator->open(state);
}

Status PartitionedAggSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _agg_source_operator->close(state);
}

Status PartitionedAggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                bool* eos) {
    auto& local_state = get_local_state(state);
    Defer defer {[&]() {
        if (!local_state._status.ok() || *eos) {
            local_state._shared_state->close();
        }
    }};

    local_state.inc_running_big_mem_op_num(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_ERROR(local_state._status);

    if (local_state._shared_state->is_spilled) {
        local_state._status = local_state.initiate_merge_spill_partition_agg_data(state);
        RETURN_IF_ERROR(local_state._status);

        /// When `_is_merging` is true means we are reading spilled data and merging the data into hash table.
        if (local_state._is_merging) {
            return Status::OK();
        }
    }

    // not spilled in sink or current partition still has data
    auto* runtime_state = local_state._runtime_state.get();
    local_state._status = _agg_source_operator->get_block(runtime_state, block, eos);
    RETURN_IF_ERROR(local_state._status);
    if (local_state._runtime_state) {
        auto* source_local_state =
                local_state._runtime_state->get_local_state(_agg_source_operator->operator_id());
        local_state.update_profile(source_local_state->profile());
    }
    if (*eos) {
        if (local_state._shared_state->is_spilled &&
            !local_state._shared_state->spill_partitions.empty()) {
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
    _runtime_state->set_pipeline_x_runtime_filter_mgr(state->local_runtime_filter_mgr());

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

Status PartitionedAggLocalState::initiate_merge_spill_partition_agg_data(RuntimeState* state) {
    DCHECK(!_is_merging);
    Base::_shared_state->in_mem_shared_state->aggregate_data_container->init_once();
    if (Base::_shared_state->in_mem_shared_state->aggregate_data_container->iterator !=
                Base::_shared_state->in_mem_shared_state->aggregate_data_container->end() ||
        _shared_state->spill_partitions.empty()) {
        return Status::OK();
    }

    _is_merging = true;
    VLOG_DEBUG << "query " << print_id(state->query_id()) << " agg node " << _parent->node_id()
               << " merge spilled agg data";

    RETURN_IF_ERROR(Base::_shared_state->in_mem_shared_state->reset_hash_table());
    _dependency->Dependency::block();

    auto query_id = state->query_id();

    MonotonicStopWatch submit_timer;
    submit_timer.start();
    auto spill_func = [this, state, query_id, submit_timer] {
        _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
        Defer defer {[&]() {
            if (!_status.ok() || state->is_cancelled()) {
                if (!_status.ok()) {
                    LOG(WARNING) << "query " << print_id(query_id) << " agg node "
                                 << _parent->node_id()
                                 << " merge spilled agg data error: " << _status;
                }
                _shared_state->close();
            } else if (_shared_state->spill_partitions.empty()) {
                VLOG_DEBUG << "query " << print_id(query_id) << " agg node " << _parent->node_id()
                           << " merge spilled agg data finish";
            }
            Base::_shared_state->in_mem_shared_state->aggregate_data_container->init_once();
            _is_merging = false;
            _dependency->Dependency::set_ready();
        }};
        bool has_agg_data = false;
        auto& parent = Base::_parent->template cast<Parent>();
        while (!state->is_cancelled() && !has_agg_data &&
               !_shared_state->spill_partitions.empty()) {
            for (auto& stream : _shared_state->spill_partitions[0]->spill_streams_) {
                stream->set_read_counters(Base::_spill_read_data_time,
                                          Base::_spill_deserialize_time, Base::_spill_read_bytes,
                                          Base::_spill_read_wait_io_timer);
                vectorized::Block block;
                bool eos = false;
                while (!eos && !state->is_cancelled()) {
                    {
                        SCOPED_TIMER(Base::_spill_recover_time);
                        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::recover_spill_data",
                                        {
                                            _status = Status::Error<INTERNAL_ERROR>(
                                                    "fault_inject partitioned_agg_source "
                                                    "recover_spill_data failed");
                                        });
                        if (_status.ok()) {
                            _status = stream->read_next_block_sync(&block, &eos);
                        }
                    }
                    RETURN_IF_ERROR(_status);

                    if (!block.empty()) {
                        has_agg_data = true;
                        _status = parent._agg_source_operator
                                          ->merge_with_serialized_key_helper<false>(
                                                  _runtime_state.get(), &block);
                        RETURN_IF_ERROR(_status);
                    }
                }
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            _shared_state->spill_partitions.pop_front();
        }
        if (_shared_state->spill_partitions.empty()) {
            _shared_state->close();
        }
        return _status;
    };

    auto exception_catch_func = [spill_func, query_id, this]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::merge_spill_data_cancel", {
            auto st = Status::InternalError(
                    "fault_inject partitioned_agg_source "
                    "merge spill data canceled");
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, st);
            return;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();

        if (!status.ok()) {
            _status = status;
        }
    };

    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_source submit_func failed");
    });
    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::make_shared<SpillRunnable>(state, _shared_state->shared_from_this(),
                                            exception_catch_func));
}
} // namespace doris::pipeline
