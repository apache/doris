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
#include <limits>
#include <memory>

#include "aggregation_sink_operator.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
PartitionedAggSinkLocalState::PartitionedAggSinkLocalState(DataSinkOperatorXBase* parent,
                                                           RuntimeState* state)
        : Base(parent, state) {}

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

    _rows_in_partitions.assign(Base::_shared_state->partition_count, 0);

    _spill_dependency = Dependency::create_shared(parent.operator_id(), parent.node_id(),
                                                  "AggSinkSpillDependency", true);
    state->get_task()->add_spill_dependency(_spill_dependency.get());

    return Status::OK();
}

Status PartitionedAggSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    _shared_state->setup_shared_profile(_profile);
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

    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(Base::profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);
    _serialize_key_arena_memory_usage = Base::profile()->AddHighWaterMarkCounter(
            "MemoryUsageSerializeKeyArena", TUnit::BYTES, "", 1);

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
    _memory_usage_container =
            ADD_COUNTER_WITH_LEVEL(Base::profile(), "MemoryUsageContainer", TUnit::BYTES, 1);
    _memory_usage_arena =
            ADD_COUNTER_WITH_LEVEL(Base::profile(), "MemoryUsageArena", TUnit::BYTES, 1);
    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(Base::profile(), "MemoryUsageReserved", TUnit::BYTES, 1);
    COUNTER_SET(_max_row_size_counter, (int64_t)0);

    _spill_serialize_hash_table_timer =
            ADD_TIMER_WITH_LEVEL(Base::profile(), "SpillSerializeHashTableTime", 1);
}
#define UPDATE_PROFILE(counter, name)                           \
    do {                                                        \
        auto* child_counter = child_profile->get_counter(name); \
        if (child_counter != nullptr) {                         \
            COUNTER_SET(counter, child_counter->value());       \
        }                                                       \
    } while (false)

void PartitionedAggSinkLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE(_hash_table_memory_usage, "MemoryUsageHashTable");
    UPDATE_PROFILE(_serialize_key_arena_memory_usage, "MemoryUsageSerializeKeyArena");
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
    UPDATE_PROFILE(_memory_usage_container, "MemoryUsageContainer");
    UPDATE_PROFILE(_memory_usage_arena, "MemoryUsageArena");

    update_max_min_rows_counter();
}

PartitionedAggSinkOperatorX::PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id,
                                                         const TPlanNode& tnode,
                                                         const DescriptorTbl& descs,
                                                         bool require_bucket_distribution)
        : DataSinkOperatorX<PartitionedAggSinkLocalState>(operator_id, tnode.node_id) {
    _agg_sink_operator = std::make_unique<AggSinkOperatorX>(pool, operator_id, tnode, descs,
                                                            require_bucket_distribution);
    _spillable = true;
}

Status PartitionedAggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<PartitionedAggSinkLocalState>::init(tnode, state));
    _name = "PARTITIONED_AGGREGATION_SINK_OPERATOR";
    if (state->query_options().__isset.external_agg_partition_bits) {
        _spill_partition_count_bits = state->query_options().external_agg_partition_bits;
    }

    _agg_sink_operator->set_dests_id(DataSinkOperatorX<PartitionedAggSinkLocalState>::dests_id());
    RETURN_IF_ERROR(
            _agg_sink_operator->set_child(DataSinkOperatorX<PartitionedAggSinkLocalState>::_child));
    return _agg_sink_operator->init(tnode, state);
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
    local_state._eos = eos;
    auto* runtime_state = local_state._runtime_state.get();
    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::sink", {
        return Status::Error<INTERNAL_ERROR>("fault_inject partitioned_agg_sink sink failed");
    });
    RETURN_IF_ERROR(_agg_sink_operator->sink(runtime_state, in_block, false));

    size_t revocable_size = 0;
    int64_t query_mem_limit = 0;
    if (eos) {
        revocable_size = revocable_mem_size(state);
        query_mem_limit = state->get_query_ctx()->get_mem_limit();
        LOG(INFO) << fmt::format(
                "Query: {}, task {}, agg sink {} eos, need spill: {}, query mem limit: {}, "
                "revocable memory: {}",
                print_id(state->query_id()), state->task_id(), node_id(),
                local_state._shared_state->is_spilled, PrettyPrinter::print_bytes(query_mem_limit),
                PrettyPrinter::print_bytes(revocable_size));

        if (local_state._shared_state->is_spilled) {
            if (revocable_mem_size(state) > 0) {
                RETURN_IF_ERROR(revoke_memory(state, nullptr));
            } else {
                for (auto& partition : local_state._shared_state->spill_partitions) {
                    RETURN_IF_ERROR(partition->finish_current_spilling(eos));
                }
                local_state._dependency->set_ready_to_read();
            }
        } else {
            local_state._dependency->set_ready_to_read();
        }
    } else if (local_state._shared_state->is_spilled) {
        if (revocable_mem_size(state) >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
            return revoke_memory(state, nullptr);
        }
    }
    if (local_state._runtime_state) {
        auto* sink_local_state = local_state._runtime_state->get_sink_local_state();
        local_state.update_profile(sink_local_state->profile());
    }
    return Status::OK();
}
Status PartitionedAggSinkOperatorX::revoke_memory(
        RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state, spill_context);
}

size_t PartitionedAggSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
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
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());
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

size_t PartitionedAggSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    auto* runtime_state = local_state._runtime_state.get();
    auto size = _agg_sink_operator->get_reserve_mem_size(runtime_state, eos);
    COUNTER_SET(local_state._memory_usage_reserved, int64_t(size));
    return size;
}

Status PartitionedAggSinkLocalState::revoke_memory(
        RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context) {
    const auto size_to_revoke = _parent->revocable_mem_size(state);
    LOG(INFO) << fmt::format(
            "Query: {}, task {}, agg sink {} revoke_memory, eos: {}, need spill: {}, revocable "
            "memory: {}",
            print_id(state->query_id()), state->task_id(), _parent->node_id(), _eos,
            _shared_state->is_spilled,
            PrettyPrinter::print_bytes(_parent->revocable_mem_size(state)));
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        profile()->add_info_string("Spilled", "true");
    }

    // TODO: spill thread may set_ready before the task::execute thread put the task to blocked state
    if (!_eos) {
        Base::_spill_dependency->Dependency::block();
    }
    auto& parent = Base::_parent->template cast<Parent>();
    Status status;
    Defer defer {[&]() {
        if (!status.ok()) {
            if (!_eos) {
                Base::_spill_dependency->Dependency::set_ready();
            }
        }
    }};

    auto query_id = state->query_id();

    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::revoke_memory_submit_func", {
        status = Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_sink revoke_memory submit_func failed");
        return status;
    });

    state->get_query_ctx()->increase_revoking_tasks_count();

    auto spill_runnable = std::make_shared<SpillSinkRunnable>(
            state, spill_context, _spill_dependency, _profile, _shared_state->shared_from_this(),
            [this, &parent, state, query_id, size_to_revoke] {
                Status status;
                DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::revoke_memory_cancel", {
                    status = Status::InternalError(
                            "fault_inject partitioned_agg_sink "
                            "revoke_memory canceled");
                    ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, status);
                    return status;
                });
                Defer defer {[&]() {
                    if (!status.ok() || state->is_cancelled()) {
                        if (!status.ok()) {
                            LOG(WARNING) << "Query " << print_id(query_id) << " agg node "
                                         << Base::_parent->node_id()
                                         << " revoke_memory error: " << status;
                        }
                        _shared_state->close();
                    } else {
                        LOG(INFO) << fmt::format(
                                "Query: {}, task {}, agg sink {} revoke_memory finish, eos: {}, "
                                "revocable memory: {}",
                                print_id(state->query_id()), state->task_id(), _parent->node_id(),
                                _eos,
                                PrettyPrinter::print_bytes(_parent->revocable_mem_size(state)));
                    }

                    if (_eos) {
                        Base::_dependency->set_ready_to_read();
                    }
                    state->get_query_ctx()->decrease_revoking_tasks_count();
                }};
                auto* runtime_state = _runtime_state.get();
                auto* agg_data = parent._agg_sink_operator->get_agg_data(runtime_state);
                status = std::visit(
                        vectorized::Overload {
                                [&](std::monostate& arg) -> Status {
                                    return Status::InternalError("Unit hash table");
                                },
                                [&](auto& agg_method) -> Status {
                                    auto& hash_table = *agg_method.hash_table;
                                    RETURN_IF_CATCH_EXCEPTION(return _spill_hash_table(
                                            state, agg_method, hash_table, size_to_revoke, _eos));
                                }},
                        agg_data->method_variant);
                RETURN_IF_ERROR(status);
                status = parent._agg_sink_operator->reset_hash_table(runtime_state);
                return status;
            });

    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::move(spill_runnable));
}

} // namespace doris::pipeline
