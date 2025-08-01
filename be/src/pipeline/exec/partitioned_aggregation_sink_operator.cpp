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

#include <gen_cpp/Types_types.h>

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
#include "common/compile_check_begin.h"
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
    Base::_shared_state->init_spill_params(parent._spill_partition_count);

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
    return Status::OK();
}

Status PartitionedAggSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    _shared_state->setup_shared_profile(custom_profile());
    return Base::open(state);
}

Status PartitionedAggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    return Base::close(state, exec_status);
}

void PartitionedAggSinkLocalState::_init_counters() {
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);

    _spill_serialize_hash_table_timer =
            ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillSerializeHashTableTime", 1);
}
#define UPDATE_PROFILE(name) \
    update_profile_from_inner_profile<spilled>(name, custom_profile(), child_profile)

template <bool spilled>
void PartitionedAggSinkLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE("MemoryUsageHashTable");
    UPDATE_PROFILE("MemoryUsageSerializeKeyArena");
    UPDATE_PROFILE("BuildTime");
    UPDATE_PROFILE("MergeTime");
    UPDATE_PROFILE("DeserializeAndMergeTime");
    UPDATE_PROFILE("HashTableComputeTime");
    UPDATE_PROFILE("HashTableEmplaceTime");
    UPDATE_PROFILE("HashTableInputCount");
    UPDATE_PROFILE("MemoryUsageContainer");
    UPDATE_PROFILE("MemoryUsageArena");

    update_max_min_rows_counter();
}

PartitionedAggSinkOperatorX::PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id,
                                                         int dest_id, const TPlanNode& tnode,
                                                         const DescriptorTbl& descs,
                                                         bool require_bucket_distribution)
        : DataSinkOperatorX<PartitionedAggSinkLocalState>(operator_id, tnode.node_id, dest_id) {
    _agg_sink_operator = std::make_unique<AggSinkOperatorX>(pool, operator_id, dest_id, tnode,
                                                            descs, require_bucket_distribution);
    _spillable = true;
}

Status PartitionedAggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<PartitionedAggSinkLocalState>::init(tnode, state));
    _name = "PARTITIONED_AGGREGATION_SINK_OPERATOR";
    _spill_partition_count = state->spill_aggregation_partition_count();
    return _agg_sink_operator->init(tnode, state);
}

Status PartitionedAggSinkOperatorX::prepare(RuntimeState* state) {
    return _agg_sink_operator->prepare(state);
}

Status PartitionedAggSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                                         bool eos) {
    auto& local_state = get_local_state(state);
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
        query_mem_limit = state->get_query_ctx()->resource_ctx()->memory_context()->mem_limit();
        LOG(INFO) << fmt::format(
                "Query:{}, agg sink:{}, task:{}, eos, need spill:{}, query mem limit:{}, "
                "revocable memory:{}",
                print_id(state->query_id()), node_id(), state->task_id(),
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

    if (!local_state._shared_state->is_spilled) {
        auto* sink_local_state = local_state._runtime_state->get_sink_local_state();
        local_state.update_profile<false>(sink_local_state->custom_profile());
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
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
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
            "Query:{}, agg sink:{}, task:{}, revoke_memory, eos:{}, need spill:{}, revocable "
            "memory:{}",
            print_id(state->query_id()), _parent->node_id(), state->task_id(), _eos,
            _shared_state->is_spilled,
            PrettyPrinter::print_bytes(_parent->revocable_mem_size(state)));
    auto* sink_local_state = _runtime_state->get_sink_local_state();
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        custom_profile()->add_info_string("Spilled", "true");
        update_profile<false>(sink_local_state->custom_profile());
    } else {
        update_profile<true>(sink_local_state->custom_profile());
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

    state->get_query_ctx()->resource_ctx()->task_controller()->increase_revoking_tasks_count();

    auto spill_runnable = std::make_shared<SpillSinkRunnable>(
            state, spill_context, _spill_dependency, operator_profile(),
            _shared_state->shared_from_this(), [this, &parent, state, query_id, size_to_revoke] {
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
                            LOG(WARNING) << fmt::format(
                                    "Query:{}, agg sink:{}, task:{}, revoke_memory error:{}",
                                    print_id(query_id), Base::_parent->node_id(), state->task_id(),
                                    status);
                        }
                        _shared_state->close();
                    } else {
                        LOG(INFO) << fmt::format(
                                "Query:{}, agg sink:{}, task:{}, revoke_memory finish, eos:{}, "
                                "revocable memory:{}",
                                print_id(state->query_id()), _parent->node_id(), state->task_id(),
                                _eos,
                                PrettyPrinter::print_bytes(_parent->revocable_mem_size(state)));
                    }

                    if (_eos) {
                        Base::_dependency->set_ready_to_read();
                    }
                    state->get_query_ctx()
                            ->resource_ctx()
                            ->task_controller()
                            ->decrease_revoking_tasks_count();
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

    Base::_spill_dependency->Dependency::block();
    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::move(spill_runnable));
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
