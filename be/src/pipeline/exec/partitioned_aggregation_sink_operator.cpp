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

#include "partitioned_aggregation_sink_operator.h"

#include <cstdint>
#include <memory>

#include "aggregation_sink_operator.h"
#include "common/status.h"
#include "pipeline/exec/spill_utils.h"
#include "runtime/fragment_mgr.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
PartitionedAggSinkLocalState::PartitionedAggSinkLocalState(DataSinkOperatorXBase* parent,
                                                           RuntimeState* state)
        : Base(parent, state) {
    _finish_dependency =
            std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                         parent->get_name() + "_SPILL_DEPENDENCY", true);
}
Status PartitionedAggSinkLocalState::init(doris::RuntimeState* state,
                                          doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));

    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);

    _init_counters();

    auto& parent = Base::_parent->template cast<Parent>();
    Base::_shared_state->init_spill_params(parent._spill_partition_count_bits);

    RETURN_IF_ERROR(setup_in_memory_agg_op(state));

    for (const auto& probe_expr_ctx : Base::_shared_state->in_mem_shared_state->probe_expr_ctxs) {
        key_columns_.emplace_back(probe_expr_ctx->root()->data_type()->create_column());
    }
    for (const auto& aggregate_evaluator :
         Base::_shared_state->in_mem_shared_state->aggregate_evaluators) {
        value_data_types_.emplace_back(aggregate_evaluator->function()->get_serialized_type());
        value_columns_.emplace_back(aggregate_evaluator->function()->create_serialize_column());
    }

    _finish_dependency->block();
    return Status::OK();
}

Status PartitionedAggSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    return Base::open(state);
}
Status PartitionedAggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    dec_running_big_mem_op_num(state);
    return Base::close(state, exec_status);
}

void PartitionedAggSinkLocalState::_init_counters() {
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    _hash_table_memory_usage = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "HashTable",
                                                            TUnit::BYTES, "MemoryUsage", 1);
    _serialize_key_arena_memory_usage = Base::profile()->AddHighWaterMarkCounter(
            "SerializeKeyArena", TUnit::BYTES, "MemoryUsage", 1);

    _build_timer = ADD_TIMER(Base::profile(), "BuildTime");
    _serialize_key_timer = ADD_TIMER(Base::profile(), "SerializeKeyTime");
    _merge_timer = ADD_TIMER(Base::profile(), "MergeTime");
    _expr_timer = ADD_TIMER(Base::profile(), "ExprTime");
    _serialize_data_timer = ADD_TIMER(Base::profile(), "SerializeDataTime");
    _deserialize_data_timer = ADD_TIMER(Base::profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(Base::profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(Base::profile(), "HashTableInputCount", TUnit::UNIT);
    _max_row_size_counter = ADD_COUNTER(Base::profile(), "MaxRowSizeInBytes", TUnit::UNIT);
    COUNTER_SET(_max_row_size_counter, (int64_t)0);

    _spill_serialize_hash_table_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillSerializeHashTableTime", "Spill", 1);
}
#define UPDATE_PROFILE(counter, name)                           \
    do {                                                        \
        auto* child_counter = child_profile->get_counter(name); \
        if (child_counter != nullptr) {                         \
            COUNTER_SET(counter, child_counter->value());       \
        }                                                       \
    } while (false)

void PartitionedAggSinkLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE(_hash_table_memory_usage, "HashTable");
    UPDATE_PROFILE(_serialize_key_arena_memory_usage, "SerializeKeyArena");
    UPDATE_PROFILE(_build_timer, "BuildTime");
    UPDATE_PROFILE(_serialize_key_timer, "SerializeKeyTime");
    UPDATE_PROFILE(_merge_timer, "MergeTime");
    UPDATE_PROFILE(_expr_timer, "MergeTime");
    UPDATE_PROFILE(_serialize_data_timer, "SerializeDataTime");
    UPDATE_PROFILE(_deserialize_data_timer, "DeserializeAndMergeTime");
    UPDATE_PROFILE(_hash_table_compute_timer, "HashTableComputeTime");
    UPDATE_PROFILE(_hash_table_emplace_timer, "HashTableEmplaceTime");
    UPDATE_PROFILE(_hash_table_input_counter, "HashTableInputCount");
    UPDATE_PROFILE(_max_row_size_counter, "MaxRowSizeInBytes");
}

PartitionedAggSinkOperatorX::PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id,
                                                         const TPlanNode& tnode,
                                                         const DescriptorTbl& descs,
                                                         bool require_bucket_distribution)
        : DataSinkOperatorX<PartitionedAggSinkLocalState>(operator_id, tnode.node_id) {
    _agg_sink_operator = std::make_unique<AggSinkOperatorX>(pool, operator_id, tnode, descs,
                                                            require_bucket_distribution);
}

Status PartitionedAggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<PartitionedAggSinkLocalState>::init(tnode, state));
    _name = "PARTITIONED_AGGREGATION_SINK_OPERATOR";
    if (state->query_options().__isset.external_agg_partition_bits) {
        _spill_partition_count_bits = state->query_options().external_agg_partition_bits;
    }

    _agg_sink_operator->set_dests_id(DataSinkOperatorX<PartitionedAggSinkLocalState>::dests_id());
    RETURN_IF_ERROR(_agg_sink_operator->set_child(
            DataSinkOperatorX<PartitionedAggSinkLocalState>::_child_x));
    return _agg_sink_operator->init(tnode, state);
}

Status PartitionedAggSinkOperatorX::prepare(RuntimeState* state) {
    return _agg_sink_operator->prepare(state);
}

Status PartitionedAggSinkOperatorX::open(RuntimeState* state) {
    return _agg_sink_operator->open(state);
}

Status PartitionedAggSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                                         bool eos) {
    auto& local_state = get_local_state(state);
    local_state.inc_running_big_mem_op_num(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    RETURN_IF_ERROR(local_state.Base::_shared_state->sink_status);
    local_state._eos = eos;
    auto* runtime_state = local_state._runtime_state.get();
    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::sink", {
        return Status::Error<INTERNAL_ERROR>("fault_inject partitioned_agg_sink sink failed");
    });
    RETURN_IF_ERROR(_agg_sink_operator->sink(runtime_state, in_block, false));
    if (eos) {
        if (local_state._shared_state->is_spilled) {
            if (revocable_mem_size(state) > 0) {
                RETURN_IF_ERROR(revoke_memory(state));
            } else {
                for (auto& partition : local_state._shared_state->spill_partitions) {
                    RETURN_IF_ERROR(partition->finish_current_spilling(eos));
                }
                local_state._dependency->set_ready_to_read();
                local_state._finish_dependency->set_ready();
            }
        } else {
            local_state._dependency->set_ready_to_read();
            local_state._finish_dependency->set_ready();
        }
    }
    if (local_state._runtime_state) {
        auto* sink_local_state = local_state._runtime_state->get_sink_local_state();
        local_state.update_profile(sink_local_state->profile());
    }
    return Status::OK();
}
Status PartitionedAggSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state);
}

size_t PartitionedAggSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (!local_state.Base::_shared_state->sink_status.ok()) {
        return UINT64_MAX;
    }
    auto* runtime_state = local_state._runtime_state.get();
    auto size = _agg_sink_operator->get_revocable_mem_size(runtime_state);
    return size;
}

Status PartitionedAggSinkLocalState::setup_in_memory_agg_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->set_pipeline_x_runtime_filter_mgr(state->local_runtime_filter_mgr());
    _runtime_state->set_task_id(state->task_id());

    auto& parent = Base::_parent->template cast<Parent>();
    Base::_shared_state->in_mem_shared_state_sptr =
            parent._agg_sink_operator->create_shared_state();
    Base::_shared_state->in_mem_shared_state =
            static_cast<AggSharedState*>(Base::_shared_state->in_mem_shared_state_sptr.get());
    Base::_shared_state->in_mem_shared_state->enable_spill = true;

    LocalSinkStateInfo info {0,  _internal_runtime_profile.get(),
                             -1, Base::_shared_state->in_mem_shared_state_sptr.get(),
                             {}, {}};
    RETURN_IF_ERROR(parent._agg_sink_operator->setup_local_state(_runtime_state.get(), info));

    auto* sink_local_state = _runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    return sink_local_state->open(state);
}

Status PartitionedAggSinkLocalState::revoke_memory(RuntimeState* state) {
    VLOG_DEBUG << "query " << print_id(state->query_id()) << " agg node "
               << Base::_parent->node_id() << " revoke_memory"
               << ", eos: " << _eos;
    RETURN_IF_ERROR(Base::_shared_state->sink_status);
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        profile()->add_info_string("Spilled", "true");
    }

    // TODO: spill thread may set_ready before the task::execute thread put the task to blocked state
    if (!_eos) {
        Base::_dependency->Dependency::block();
    }
    auto& parent = Base::_parent->template cast<Parent>();
    Status status;
    Defer defer {[&]() {
        if (!status.ok()) {
            if (!_eos) {
                Base::_dependency->Dependency::set_ready();
            }
        }
    }};

    auto query_id = state->query_id();

    MonotonicStopWatch submit_timer;
    submit_timer.start();
    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::revoke_memory_submit_func", {
        status = Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_sink revoke_memory submit_func failed");
        return status;
    });

    auto spill_runnable = std::make_shared<SpillRunnable>(
            state, _shared_state->shared_from_this(),
            [this, &parent, state, query_id, submit_timer] {
                DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::revoke_memory_cancel", {
                    auto st = Status::InternalError(
                            "fault_inject partitioned_agg_sink "
                            "revoke_memory canceled");
                    ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, st);
                    return st;
                });
                _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
                SCOPED_TIMER(Base::_spill_timer);
                Defer defer {[&]() {
                    if (!_shared_state->sink_status.ok() || state->is_cancelled()) {
                        if (!_shared_state->sink_status.ok()) {
                            LOG(WARNING)
                                    << "query " << print_id(query_id) << " agg node "
                                    << Base::_parent->node_id()
                                    << " revoke_memory error: " << Base::_shared_state->sink_status;
                        }
                        _shared_state->close();
                    } else {
                        VLOG_DEBUG << "query " << print_id(query_id) << " agg node "
                                   << Base::_parent->node_id() << " revoke_memory finish"
                                   << ", eos: " << _eos;
                    }

                    if (_eos) {
                        Base::_dependency->set_ready_to_read();
                        _finish_dependency->set_ready();
                    } else {
                        Base::_dependency->Dependency::set_ready();
                    }
                }};
                auto* runtime_state = _runtime_state.get();
                auto* agg_data = parent._agg_sink_operator->get_agg_data(runtime_state);
                Base::_shared_state->sink_status =
                        std::visit(vectorized::Overload {
                                           [&](std::monostate& arg) -> Status {
                                               return Status::InternalError("Unit hash table");
                                           },
                                           [&](auto& agg_method) -> Status {
                                               auto& hash_table = *agg_method.hash_table;
                                               RETURN_IF_CATCH_EXCEPTION(return _spill_hash_table(
                                                       state, agg_method, hash_table, _eos));
                                           }},
                                   agg_data->method_variant);
                RETURN_IF_ERROR(Base::_shared_state->sink_status);
                Base::_shared_state->sink_status =
                        parent._agg_sink_operator->reset_hash_table(runtime_state);
                return Base::_shared_state->sink_status;
            });

    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::move(spill_runnable));
}

} // namespace doris::pipeline
