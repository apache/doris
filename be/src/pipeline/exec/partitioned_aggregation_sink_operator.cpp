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

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = Base::_shared_state->in_mem_shared_state_sptr.get(),
                             .shared_state_map = {},
                             .tsink = {}};
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

template <typename HashTableCtxType, typename KeyType>
Status PartitionedAggSinkLocalState::to_block(HashTableCtxType& context, std::vector<KeyType>& keys,
                                              std::vector<vectorized::AggregateDataPtr>& values,
                                              const vectorized::AggregateDataPtr null_key_data) {
    SCOPED_TIMER(_spill_serialize_hash_table_timer);
    context.insert_keys_into_columns(keys, key_columns_, (uint32_t)keys.size());

    if (null_key_data) {
        // only one key of group by support wrap null key
        // here need additional processing logic on the null key / value
        CHECK(key_columns_.size() == 1);
        CHECK(key_columns_[0]->is_nullable());
        key_columns_[0]->insert_data(nullptr, 0);

        values.emplace_back(null_key_data);
    }

    for (size_t i = 0; i < Base::_shared_state->in_mem_shared_state->aggregate_evaluators.size();
         ++i) {
        Base::_shared_state->in_mem_shared_state->aggregate_evaluators[i]
                ->function()
                ->serialize_to_column(
                        values,
                        Base::_shared_state->in_mem_shared_state->offsets_of_aggregate_states[i],
                        value_columns_[i], values.size());
    }

    vectorized::ColumnsWithTypeAndName key_columns_with_schema;
    for (int i = 0; i < key_columns_.size(); ++i) {
        key_columns_with_schema.emplace_back(
                std::move(key_columns_[i]),
                Base::_shared_state->in_mem_shared_state->probe_expr_ctxs[i]->root()->data_type(),
                Base::_shared_state->in_mem_shared_state->probe_expr_ctxs[i]->root()->expr_name());
    }
    key_block_ = key_columns_with_schema;

    vectorized::ColumnsWithTypeAndName value_columns_with_schema;
    for (int i = 0; i < value_columns_.size(); ++i) {
        value_columns_with_schema.emplace_back(
                std::move(value_columns_[i]), value_data_types_[i],
                Base::_shared_state->in_mem_shared_state->aggregate_evaluators[i]
                        ->function()
                        ->get_name());
    }
    value_block_ = value_columns_with_schema;

    for (const auto& column : key_block_.get_columns_with_type_and_name()) {
        block_.insert(column);
    }
    for (const auto& column : value_block_.get_columns_with_type_and_name()) {
        block_.insert(column);
    }
    return Status::OK();
}

template <typename HashTableCtxType, typename KeyType>
Status PartitionedAggSinkLocalState::_spill_partition(
        RuntimeState* state, HashTableCtxType& context, AggSpillPartitionSPtr& spill_partition,
        std::vector<KeyType>& keys, std::vector<vectorized::AggregateDataPtr>& values,
        const vectorized::AggregateDataPtr null_key_data, bool is_last) {
    vectorized::SpillStreamSPtr spill_stream;
    auto status = spill_partition->get_spill_stream(state, Base::_parent->node_id(),
                                                    Base::operator_profile(), spill_stream);
    RETURN_IF_ERROR(status);

    status = to_block(context, keys, values, null_key_data);
    RETURN_IF_ERROR(status);

    if (is_last) {
        std::vector<KeyType> tmp_keys;
        std::vector<vectorized::AggregateDataPtr> tmp_values;
        keys.swap(tmp_keys);
        values.swap(tmp_values);

    } else {
        keys.clear();
        values.clear();
    }
    status = spill_stream->spill_block(state, block_, false);
    RETURN_IF_ERROR(status);

    status = spill_partition->flush_if_full();
    _reset_tmp_data();
    return status;
}

template <typename HashTableCtxType, typename HashTableType>
Status PartitionedAggSinkLocalState::_spill_hash_table(RuntimeState* state,
                                                       HashTableCtxType& context,
                                                       HashTableType& hash_table,
                                                       const size_t size_to_revoke, bool eos) {
    Status status;
    Defer defer {[&]() {
        if (!status.ok()) {
            Base::_shared_state->close();
        }
    }};

    context.init_iterator();

    Base::_shared_state->in_mem_shared_state->aggregate_data_container->init_once();

    const auto total_rows =
            Base::_shared_state->in_mem_shared_state->aggregate_data_container->total_count();

    const size_t size_to_revoke_ = std::max<size_t>(size_to_revoke, 1);

    // `spill_batch_rows` will be between 4k and 1M
    // and each block to spill will not be larger than 32MB(`MAX_SPILL_WRITE_BATCH_MEM`)
    const auto spill_batch_rows = std::min<size_t>(
            1024 * 1024, std::max<size_t>(4096, vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM *
                                                        total_rows / size_to_revoke_));

    VLOG_DEBUG << "Query: " << print_id(state->query_id()) << ", node: " << _parent->node_id()
               << ", spill_batch_rows: " << spill_batch_rows << ", total rows: " << total_rows;
    size_t row_count = 0;

    std::vector<TmpSpillInfo<typename HashTableType::key_type>> spill_infos(
            Base::_shared_state->partition_count);
    auto& iter = Base::_shared_state->in_mem_shared_state->aggregate_data_container->iterator;
    while (iter != Base::_shared_state->in_mem_shared_state->aggregate_data_container->end() &&
           !state->is_cancelled()) {
        const auto& key = iter.template get_key<typename HashTableType::key_type>();
        auto partition_index = Base::_shared_state->get_partition_index(hash_table.hash(key));
        spill_infos[partition_index].keys_.emplace_back(key);
        spill_infos[partition_index].values_.emplace_back(iter.get_aggregate_data());

        if (++row_count == spill_batch_rows) {
            row_count = 0;
            for (int i = 0; i < Base::_shared_state->partition_count && !state->is_cancelled();
                 ++i) {
                if (spill_infos[i].keys_.size() >= spill_batch_rows) {
                    _rows_in_partitions[i] += spill_infos[i].keys_.size();
                    status = _spill_partition(
                            state, context, Base::_shared_state->spill_partitions[i],
                            spill_infos[i].keys_, spill_infos[i].values_, nullptr, false);
                    RETURN_IF_ERROR(status);
                }
            }
        }

        ++iter;
    }
    auto hash_null_key_data = hash_table.has_null_key_data();
    for (int i = 0; i < Base::_shared_state->partition_count && !state->is_cancelled(); ++i) {
        auto spill_null_key_data =
                (hash_null_key_data && i == Base::_shared_state->partition_count - 1);
        if (spill_infos[i].keys_.size() > 0 || spill_null_key_data) {
            _rows_in_partitions[i] += spill_infos[i].keys_.size();
            status = _spill_partition(
                    state, context, Base::_shared_state->spill_partitions[i], spill_infos[i].keys_,
                    spill_infos[i].values_,
                    spill_null_key_data
                            ? hash_table.template get_null_key_data<vectorized::AggregateDataPtr>()
                            : nullptr,
                    true);
            RETURN_IF_ERROR(status);
        }
    }

    for (auto& partition : Base::_shared_state->spill_partitions) {
        status = partition->finish_current_spilling(eos);
        RETURN_IF_ERROR(status);
    }
    if (eos) {
        _clear_tmp_data();
    }
    return Status::OK();
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
    auto query_id = state->query_id();

    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::revoke_memory_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_sink revoke_memory submit_func failed");
    });

    state->get_query_ctx()->resource_ctx()->task_controller()->increase_revoking_tasks_count();

    SpillSinkRunnable spill_runnable(
            state, spill_context, operator_profile(),
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

    return spill_runnable.run();
}

void PartitionedAggSinkLocalState::_reset_tmp_data() {
    block_.clear();
    key_columns_.clear();
    value_columns_.clear();
    key_block_.clear_column_data();
    value_block_.clear_column_data();
    key_columns_ = key_block_.mutate_columns();
    value_columns_ = value_block_.mutate_columns();
}

void PartitionedAggSinkLocalState::_clear_tmp_data() {
    {
        vectorized::Block empty_block;
        block_.swap(empty_block);
    }
    {
        vectorized::Block empty_block;
        key_block_.swap(empty_block);
    }
    {
        vectorized::Block empty_block;
        value_block_.swap(empty_block);
    }
    {
        vectorized::MutableColumns cols;
        key_columns_.swap(cols);
    }
    {
        vectorized::MutableColumns cols;
        value_columns_.swap(cols);
    }

    vectorized::DataTypes tmp_value_data_types;
    value_data_types_.swap(tmp_value_data_types);
}

bool PartitionedAggSinkLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
