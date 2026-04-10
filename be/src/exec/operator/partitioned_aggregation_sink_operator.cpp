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

#include "exec/operator/partitioned_aggregation_sink_operator.h"

#include <gen_cpp/Types_types.h>

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "exec/operator/aggregation_sink_operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_profile.h"
#include "util/pretty_printer.h"

namespace doris {
PartitionedAggSinkLocalState::PartitionedAggSinkLocalState(DataSinkOperatorXBase* parent,
                                                           RuntimeState* state)
        : Base(parent, state) {}

Status PartitionedAggSinkLocalState::init(doris::RuntimeState* state,
                                          doris::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));

    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);

    _init_counters();

    auto& parent = Base::_parent->template cast<Parent>();
    _spill_writers.resize(parent._partition_count);
    RETURN_IF_ERROR(_setup_in_memory_agg_op(state));

    for (const auto& probe_expr_ctx : Base::_shared_state->_in_mem_shared_state->probe_expr_ctxs) {
        _key_columns.emplace_back(probe_expr_ctx->root()->data_type()->create_column());
    }
    for (const auto& aggregate_evaluator :
         Base::_shared_state->_in_mem_shared_state->aggregate_evaluators) {
        _value_data_types.emplace_back(aggregate_evaluator->function()->get_serialized_type());
        _value_columns.emplace_back(aggregate_evaluator->function()->create_serialize_column());
    }
    _rows_in_partitions.assign(parent._partition_count, 0);
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

    Status first_error;
    for (auto& writer : _spill_writers) {
        if (writer) {
            auto st = writer->close();
            if (!st.ok() && first_error.ok()) {
                first_error = st;
            }
            writer.reset();
        }
    }
    _spill_writers.clear();

    auto st = Base::close(state, exec_status);
    if (!first_error.ok()) {
        return first_error;
    }
    return st;
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
void PartitionedAggSinkLocalState::_update_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE("MemoryUsageHashTable");
    UPDATE_PROFILE("MemoryUsageSerializeKeyArena");
    UPDATE_PROFILE("BuildTime");
    UPDATE_PROFILE("MergeTime");
    UPDATE_PROFILE("DeserializeAndMergeTime");
    UPDATE_PROFILE("HashTableComputeTime");
    UPDATE_PROFILE("HashTableEmplaceTime");
    UPDATE_PROFILE("HashTableInputCount");
    UPDATE_PROFILE("HashTableSize");
    UPDATE_PROFILE("MemoryUsageContainer");
    UPDATE_PROFILE("MemoryUsageArena");

    update_max_min_rows_counter();
}

PartitionedAggSinkOperatorX::PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id,
                                                         int dest_id, const TPlanNode& tnode,
                                                         const DescriptorTbl& descs)
        : DataSinkOperatorX<PartitionedAggSinkLocalState>(operator_id, tnode.node_id, dest_id) {
    _agg_sink_operator =
            std::make_unique<AggSinkOperatorX>(pool, operator_id, dest_id, tnode, descs);
    _spillable = true;
}

Status PartitionedAggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<PartitionedAggSinkLocalState>::init(tnode, state));
    _name = "PARTITIONED_AGGREGATION_SINK_OPERATOR";
    _partition_count = state->spill_aggregation_partition_count();
    if (_partition_count < 2 || _partition_count > 32) {
        return Status::InvalidArgument(fmt::format(
                "Invalid partition count {} for PartitionedAggSinkOperatorX, should be in [2, 32]",
                _partition_count));
    }
    return _agg_sink_operator->init(tnode, state);
}

Status PartitionedAggSinkOperatorX::prepare(RuntimeState* state) {
    return _agg_sink_operator->prepare(state);
}

Status PartitionedAggSinkOperatorX::sink(doris::RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    auto* runtime_state = local_state._runtime_state.get();
    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::sink", {
        return Status::Error<INTERNAL_ERROR>("fault_inject partitioned_agg_sink sink failed");
    });
    RETURN_IF_ERROR(_agg_sink_operator->sink(runtime_state, in_block, false));

    // handle spill condition first, independent of eos
    if (local_state._shared_state->_is_spilled) {
        if (revocable_mem_size(state) >= state->spill_aggregation_sink_mem_limit_bytes()) {
            RETURN_IF_ERROR(revoke_memory(state));
            DCHECK(local_state._shared_state->_in_mem_shared_state->aggregate_data_container
                           ->total_count() == 0);
        }
    } else {
        auto* sink_local_state = local_state._runtime_state->get_sink_local_state();
        local_state._update_profile<false>(sink_local_state->custom_profile());
    }

    // finally perform EOS bookkeeping
    if (eos) {
        if (local_state._shared_state->_is_spilled) {
            // If there are still memory aggregation data, revoke memory, it is a flush operation.
            if (_agg_sink_operator->get_hash_table_size(runtime_state) > 0) {
                RETURN_IF_ERROR(revoke_memory(state));
                DCHECK(local_state._shared_state->_in_mem_shared_state->aggregate_data_container
                               ->total_count() == 0);
            }
            // Close all writers (finalizes SpillFile metadata)
            for (auto& writer : local_state._spill_writers) {
                if (writer) {
                    RETURN_IF_ERROR(writer->close());
                }
            }
            local_state._clear_tmp_data();
        }
        // Should set here, not at the beginning, because revoke memory will check eos flag.
        local_state._eos = eos;
        local_state._dependency->set_ready_to_read();
    }

    return Status::OK();
}

Status PartitionedAggSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state._revoke_memory(state);
}

size_t PartitionedAggSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    // If the agg sink already has all data, then not able to spill.
    if (local_state._eos) {
        return 0;
    }
    auto* runtime_state = local_state._runtime_state.get();
    auto size = _agg_sink_operator->get_revocable_mem_size(runtime_state);
    return size > state->spill_min_revocable_mem() ? size : 0;
}

Status PartitionedAggSinkLocalState::_setup_in_memory_agg_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());
    _runtime_state->set_task_id(state->task_id());

    auto& parent = Base::_parent->template cast<Parent>();
    Base::_shared_state->_in_mem_shared_state_sptr =
            parent._agg_sink_operator->create_shared_state();
    Base::_shared_state->_in_mem_shared_state =
            static_cast<AggSharedState*>(Base::_shared_state->_in_mem_shared_state_sptr.get());
    Base::_shared_state->_in_mem_shared_state->enable_spill = true;

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = Base::_shared_state->_in_mem_shared_state_sptr.get(),
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
Status PartitionedAggSinkLocalState::_to_block(HashTableCtxType& context,
                                               std::vector<KeyType>& keys,
                                               std::vector<AggregateDataPtr>& values,
                                               const AggregateDataPtr null_key_data) {
    SCOPED_TIMER(_spill_serialize_hash_table_timer);
    context.insert_keys_into_columns(keys, _key_columns, (uint32_t)keys.size());

    if (null_key_data) {
        // only one key of group by support wrap null key
        // here need additional processing logic on the null key / value
        CHECK(_key_columns.size() == 1);
        CHECK(_key_columns[0]->is_nullable());
        _key_columns[0]->insert_data(nullptr, 0);

        values.emplace_back(null_key_data);
    }

    for (size_t i = 0; i < Base::_shared_state->_in_mem_shared_state->aggregate_evaluators.size();
         ++i) {
        Base::_shared_state->_in_mem_shared_state->aggregate_evaluators[i]
                ->function()
                ->serialize_to_column(
                        values,
                        Base::_shared_state->_in_mem_shared_state->offsets_of_aggregate_states[i],
                        _value_columns[i], values.size());
    }

    ColumnsWithTypeAndName key_columns_with_schema;
    for (int i = 0; i < _key_columns.size(); ++i) {
        key_columns_with_schema.emplace_back(
                std::move(_key_columns[i]),
                Base::_shared_state->_in_mem_shared_state->probe_expr_ctxs[i]->root()->data_type(),
                Base::_shared_state->_in_mem_shared_state->probe_expr_ctxs[i]->root()->expr_name());
    }
    _key_block = key_columns_with_schema;

    ColumnsWithTypeAndName value_columns_with_schema;
    for (int i = 0; i < _value_columns.size(); ++i) {
        value_columns_with_schema.emplace_back(
                std::move(_value_columns[i]), _value_data_types[i],
                Base::_shared_state->_in_mem_shared_state->aggregate_evaluators[i]
                        ->function()
                        ->get_name());
    }
    _value_block = value_columns_with_schema;

    for (const auto& column : _key_block.get_columns_with_type_and_name()) {
        _block.insert(column);
    }
    for (const auto& column : _value_block.get_columns_with_type_and_name()) {
        _block.insert(column);
    }
    return Status::OK();
}

template <typename HashTableCtxType, typename KeyType>
Status PartitionedAggSinkLocalState::_spill_partition(
        RuntimeState* state, HashTableCtxType& context, size_t partition_idx,
        std::vector<KeyType>& keys, std::vector<AggregateDataPtr>& values,
        const AggregateDataPtr null_key_data, bool is_last) {
    auto status = _to_block(context, keys, values, null_key_data);
    RETURN_IF_ERROR(status);

    if (is_last) {
        std::vector<KeyType> tmp_keys;
        std::vector<AggregateDataPtr> tmp_values;
        keys.swap(tmp_keys);
        values.swap(tmp_values);
    } else {
        keys.clear();
        values.clear();
    }

    // Ensure _spill_partitions is initialized to correct size
    auto& partitions = Base::_shared_state->_spill_partitions;
    auto& parent = Base::_parent->template cast<Parent>();
    if (partitions.size() == 0) {
        partitions.resize(parent._partition_count);
    }

    // Lazy-create SpillFile + writer on first write for this partition
    auto& spill_file = partitions[partition_idx];
    auto& writer = _spill_writers[partition_idx];
    if (!writer) {
        auto relative_path = fmt::format("{}/agg_{}-{}-{}-{}", print_id(state->query_id()),
                                         partition_idx, parent.node_id(), state->task_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                    spill_file));
        RETURN_IF_ERROR(spill_file->create_writer(state, Base::operator_profile(), writer));
    }

    RETURN_IF_ERROR(writer->write_block(state, _block));
    _reset_tmp_data();
    return Status::OK();
}

template <typename HashTableCtxType, typename HashTableType>
Status PartitionedAggSinkLocalState::_spill_hash_table(RuntimeState* state,
                                                       HashTableCtxType& context,
                                                       HashTableType& hash_table,
                                                       const size_t size_to_revoke, bool eos) {
    Status status;

    context.init_iterator();
    auto& parent = _parent->template cast<PartitionedAggSinkOperatorX>();

    Base::_shared_state->_in_mem_shared_state->aggregate_data_container->init_once();

    const auto total_rows = parent._agg_sink_operator->get_hash_table_size(_runtime_state.get());

    if (total_rows == 0) {
        return Status::OK();
    }

    const size_t size_to_revoke_ = std::max<size_t>(size_to_revoke, 1);

    // `spill_batch_rows` will be between 4k and 1M
    // and each block to spill will not be larger than 32MB(`MAX_SPILL_WRITE_BATCH_MEM`)
    // TODO: yiguolei, should review this logic
    const auto spill_batch_rows = std::min<size_t>(

            1024 * 1024, std::max<size_t>(4096, SpillFile::MAX_SPILL_WRITE_BATCH_MEM * total_rows /
                                                        size_to_revoke_));

    VLOG_DEBUG << "Query: " << print_id(state->query_id()) << ", node: " << _parent->node_id()
               << ", spill_batch_rows: " << spill_batch_rows << ", total rows: " << total_rows
               << ", size_to_revoke: " << size_to_revoke;
    size_t row_count = 0;

    std::vector<TmpSpillInfo<typename HashTableType::key_type>> spill_infos(
            parent._partition_count);
    auto& iter = Base::_shared_state->_in_mem_shared_state->aggregate_data_container->iterator;
    while (iter != Base::_shared_state->_in_mem_shared_state->aggregate_data_container->end() &&
           !state->is_cancelled()) {
        const auto& key = iter.template get_key<typename HashTableType::key_type>();
        auto partition_index = hash_table.hash(key) % parent._partition_count;
        spill_infos[partition_index].keys_.emplace_back(key);
        spill_infos[partition_index].values_.emplace_back(iter.get_aggregate_data());

        if (++row_count == spill_batch_rows) {
            row_count = 0;
            for (int i = 0; i < parent._partition_count && !state->is_cancelled(); ++i) {
                if (spill_infos[i].keys_.size() >= spill_batch_rows) {
                    _rows_in_partitions[i] += spill_infos[i].keys_.size();
                    status = _spill_partition(state, context, i, spill_infos[i].keys_,
                                              spill_infos[i].values_, nullptr, false);
                    RETURN_IF_ERROR(status);
                    spill_infos[i].keys_.clear();
                    spill_infos[i].values_.clear();
                }
            }
        }

        ++iter;
    }
    const auto has_null_key_data = hash_table.has_null_key_data();
    for (int i = 0; i < parent._partition_count && !state->is_cancelled(); ++i) {
        auto spill_null_key_data = (has_null_key_data && i == parent._partition_count - 1);
        if (spill_infos[i].keys_.size() > 0 || spill_null_key_data) {
            _rows_in_partitions[i] += spill_infos[i].keys_.size();
            status = _spill_partition(
                    state, context, i, spill_infos[i].keys_, spill_infos[i].values_,
                    spill_null_key_data ? hash_table.template get_null_key_data<AggregateDataPtr>()
                                        : nullptr,
                    true);
            RETURN_IF_ERROR(status);
        }
    }
    return Status::OK();
}

Status PartitionedAggSinkLocalState::_revoke_memory(RuntimeState* state) {
    if (_eos) {
        return Status::OK();
    }

    const auto size_to_revoke = _parent->revocable_mem_size(state);
    VLOG_DEBUG << fmt::format(
            "Query:{}, agg sink:{}, task:{}, revoke_memory, eos:{}, is_spilled:{}, revocable "
            "memory:{}",
            print_id(state->query_id()), _parent->node_id(), state->task_id(), _eos,
            _shared_state->_is_spilled, PrettyPrinter::print_bytes(size_to_revoke));
    auto* sink_local_state = _runtime_state->get_sink_local_state();
    if (!_shared_state->_is_spilled) {
        _shared_state->_is_spilled = true;
        custom_profile()->add_info_string("Spilled", "true");
        _update_profile<false>(sink_local_state->custom_profile());
    } else {
        _update_profile<false>(sink_local_state->custom_profile());
    }

    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::revoke_memory_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_sink revoke_memory submit_func failed");
    });

    state->get_query_ctx()->resource_ctx()->task_controller()->increase_revoking_tasks_count();

    auto& parent = Base::_parent->template cast<Parent>();
    auto query_id = state->query_id();

    auto spill_func = [this, state, &parent, query_id, size_to_revoke]() -> Status {
        Status status;

        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_sink::revoke_memory_cancel", {
            status = Status::InternalError(
                    "fault_inject partitioned_agg_sink revoke_memory canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        Defer defer {[&]() {
            if (!status.ok()) {
                LOG(WARNING) << fmt::format(
                        "Query:{}, agg sink:{}, task:{}, revoke_memory error:{}",
                        print_id(query_id), Base::_parent->node_id(), state->task_id(), status);
            } else {
                VLOG_DEBUG << fmt::format(
                        "Query:{}, agg sink:{}, task:{}, revoke_memory finish, eos:{}, "
                        "revocable "
                        "memory:{}",
                        print_id(state->query_id()), _parent->node_id(), state->task_id(), _eos,
                        PrettyPrinter::print_bytes(_parent->revocable_mem_size(state)));
            }
            state->get_query_ctx()
                    ->resource_ctx()
                    ->task_controller()
                    ->decrease_revoking_tasks_count();
        }};

        auto* runtime_state = _runtime_state.get();
        auto* agg_data = parent._agg_sink_operator->get_agg_data(runtime_state);
        status = std::visit(
                Overload {[&](std::monostate& arg) -> Status {
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
    };

    // old code used SpillSinkRunnable, but spills are synchronous and counters
    // are tracked externally.  Call the spill function directly.
    return run_spill_task(state, std::move(spill_func));
}

void PartitionedAggSinkLocalState::_reset_tmp_data() {
    _block.clear();
    _key_columns.clear();
    _value_columns.clear();
    _key_block.clear_column_data();
    _value_block.clear_column_data();
    _key_columns = _key_block.mutate_columns();
    _value_columns = _value_block.mutate_columns();
}

void PartitionedAggSinkLocalState::_clear_tmp_data() {
    {
        Block empty_block;
        _block.swap(empty_block);
    }
    {
        Block empty_block;
        _key_block.swap(empty_block);
    }
    {
        Block empty_block;
        _value_block.swap(empty_block);
    }
    {
        MutableColumns cols;
        _key_columns.swap(cols);
    }
    {
        MutableColumns cols;
        _value_columns.swap(cols);
    }

    DataTypes tmp_value_data_types;
    _value_data_types.swap(tmp_value_data_types);
}

bool PartitionedAggSinkLocalState::is_blockable() const {
    return _shared_state->_is_spilled;
}

} // namespace doris
