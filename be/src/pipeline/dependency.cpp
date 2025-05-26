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

#include "dependency.h"

#include <memory>
#include <mutex>

#include "common/logging.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime_filter/runtime_filter_consumer.h"
#include "util/brpc_client_cache.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/spill/spill_stream_manager.h"
#include "vec/utils/util.hpp"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Dependency* BasicSharedState::create_source_dependency(int operator_id, int node_id,
                                                       const std::string& name) {
    source_deps.push_back(std::make_shared<Dependency>(operator_id, node_id, name + "_DEPENDENCY"));
    source_deps.back()->set_shared_state(this);
    return source_deps.back().get();
}

void BasicSharedState::create_source_dependencies(int num_sources, int operator_id, int node_id,
                                                  const std::string& name) {
    source_deps.resize(num_sources, nullptr);
    for (auto& source_dep : source_deps) {
        source_dep = std::make_shared<Dependency>(operator_id, node_id, name + "_DEPENDENCY");
        source_dep->set_shared_state(this);
    }
}

Dependency* BasicSharedState::create_sink_dependency(int dest_id, int node_id,
                                                     const std::string& name) {
    sink_deps.push_back(std::make_shared<Dependency>(dest_id, node_id, name + "_DEPENDENCY", true));
    sink_deps.back()->set_shared_state(this);
    return sink_deps.back().get();
}

void Dependency::_add_block_task(std::shared_ptr<PipelineTask> task) {
    DCHECK(_blocked_task.empty() || _blocked_task[_blocked_task.size() - 1].lock() == nullptr ||
           _blocked_task[_blocked_task.size() - 1].lock().get() != task.get())
            << "Duplicate task: " << task->debug_string();
    _blocked_task.push_back(task);
}

void Dependency::set_ready() {
    if (_ready) {
        return;
    }
    _watcher.stop();
    std::vector<std::weak_ptr<PipelineTask>> local_block_task {};
    {
        std::unique_lock<std::mutex> lc(_task_lock);
        if (_ready) {
            return;
        }
        _ready = true;
        local_block_task.swap(_blocked_task);
    }
    for (auto task : local_block_task) {
        if (auto t = task.lock()) {
            std::unique_lock<std::mutex> lc(_task_lock);
            THROW_IF_ERROR(t->wake_up(this));
        }
    }
}

Dependency* Dependency::is_blocked_by(std::shared_ptr<PipelineTask> task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    auto ready = _ready.load();
    if (!ready && task) {
        _add_block_task(task);
        start_watcher();
        THROW_IF_ERROR(task->blocked(this));
    }
    return ready ? nullptr : this;
}

std::string Dependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, block task = {}, ready={}, _always_ready={}",
                   std::string(indentation_level * 2, ' '), _name, _node_id, _blocked_task.size(),
                   _ready, _always_ready);
    return fmt::to_string(debug_string_buffer);
}

std::string CountedFinishDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}{}: id={}, block_task={}, ready={}, _always_ready={}, count={}",
                   std::string(indentation_level * 2, ' '), _name, _node_id, _blocked_task.size(),
                   _ready, _always_ready, _counter);
    return fmt::to_string(debug_string_buffer);
}

void RuntimeFilterTimer::call_timeout() {
    _parent->set_ready();
}

void RuntimeFilterTimer::call_ready() {
    _parent->set_ready();
}

// should check rf timeout in two case:
// 1. the rf is ready just remove the wait queue
// 2. if the rf have local dependency, the rf should start wait when all local dependency is ready
bool RuntimeFilterTimer::should_be_check_timeout() {
    if (!_parent->ready() && !_local_runtime_filter_dependencies.empty()) {
        bool all_ready = true;
        for (auto& dep : _local_runtime_filter_dependencies) {
            if (!dep->ready()) {
                all_ready = false;
                break;
            }
        }
        if (all_ready) {
            _local_runtime_filter_dependencies.clear();
            _registration_time = MonotonicMillis();
        }
        return all_ready;
    }
    return true;
}

void RuntimeFilterTimerQueue::start() {
    while (!_stop) {
        std::unique_lock<std::mutex> lk(cv_m);

        while (_que.empty() && !_stop) {
            cv.wait_for(lk, std::chrono::seconds(3), [this] { return !_que.empty() || _stop; });
        }
        if (_stop) {
            break;
        }
        {
            std::unique_lock<std::mutex> lc(_que_lock);
            std::list<std::shared_ptr<pipeline::RuntimeFilterTimer>> new_que;
            for (auto& it : _que) {
                if (it.use_count() == 1) {
                    // `use_count == 1` means this runtime filter has been released
                } else if (it->should_be_check_timeout()) {
                    if (it->_parent->is_blocked_by()) {
                        // This means runtime filter is not ready, so we call timeout or continue to poll this timer.
                        int64_t ms_since_registration = MonotonicMillis() - it->registration_time();
                        if (ms_since_registration > it->wait_time_ms()) {
                            it->call_timeout();
                        } else {
                            new_que.push_back(std::move(it));
                        }
                    }
                } else {
                    new_que.push_back(std::move(it));
                }
            }
            new_que.swap(_que);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
    _shutdown = true;
}

void LocalExchangeSharedState::sub_running_sink_operators() {
    std::unique_lock<std::mutex> lc(le_lock);
    if (exchanger->_running_sink_operators.fetch_sub(1) == 1) {
        _set_always_ready();
    }
}

void LocalExchangeSharedState::sub_running_source_operators() {
    std::unique_lock<std::mutex> lc(le_lock);
    if (exchanger->_running_source_operators.fetch_sub(1) == 1) {
        _set_always_ready();
        exchanger->finalize();
    }
}

LocalExchangeSharedState::LocalExchangeSharedState(int num_instances) {
    source_deps.resize(num_instances, nullptr);
    mem_counters.resize(num_instances, nullptr);
}

vectorized::MutableColumns AggSharedState::_get_keys_hash_table() {
    return std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                        return vectorized::MutableColumns();
                    },
                    [&](auto&& agg_method) -> vectorized::MutableColumns {
                        vectorized::MutableColumns key_columns;
                        for (int i = 0; i < probe_expr_ctxs.size(); ++i) {
                            key_columns.emplace_back(
                                    probe_expr_ctxs[i]->root()->data_type()->create_column());
                        }
                        auto& data = *agg_method.hash_table;
                        bool has_null_key = data.has_null_key_data();
                        const auto size = data.size() - has_null_key;
                        using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                        std::vector<KeyType> keys(size);

                        size_t num_rows = 0;
                        auto iter = aggregate_data_container->begin();
                        {
                            while (iter != aggregate_data_container->end()) {
                                keys[num_rows] = iter.get_key<KeyType>();
                                ++iter;
                                ++num_rows;
                            }
                        }
                        agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                        if (has_null_key) {
                            key_columns[0]->insert_data(nullptr, 0);
                        }
                        return key_columns;
                    }},
            agg_data->method_variant);
}

void AggSharedState::build_limit_heap(size_t hash_table_size) {
    limit_columns = _get_keys_hash_table();
    for (size_t i = 0; i < hash_table_size; ++i) {
        limit_heap.emplace(i, limit_columns, order_directions, null_directions);
    }
    while (hash_table_size > limit) {
        limit_heap.pop();
        hash_table_size--;
    }
    limit_columns_min = limit_heap.top()._row_id;
}

bool AggSharedState::do_limit_filter(vectorized::Block* block, size_t num_rows,
                                     const std::vector<int>* key_locs) {
    if (num_rows) {
        cmp_res.resize(num_rows);
        need_computes.resize(num_rows);
        memset(need_computes.data(), 0, need_computes.size());
        memset(cmp_res.data(), 0, cmp_res.size());

        const auto key_size = null_directions.size();
        for (int i = 0; i < key_size; i++) {
            block->get_by_position(key_locs ? key_locs->operator[](i) : i)
                    .column->compare_internal(limit_columns_min, *limit_columns[i],
                                              null_directions[i], order_directions[i], cmp_res,
                                              need_computes.data());
        }

        auto set_computes_arr = [](auto* __restrict res, auto* __restrict computes, size_t rows) {
            for (size_t i = 0; i < rows; ++i) {
                computes[i] = computes[i] == res[i];
            }
        };
        set_computes_arr(cmp_res.data(), need_computes.data(), num_rows);

        return std::find(need_computes.begin(), need_computes.end(), 0) != need_computes.end();
    }

    return false;
}

Status AggSharedState::reset_hash_table() {
    return std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> Status {
                        return Status::InternalError("Uninited hash table");
                    },
                    [&](auto& agg_method) {
                        auto& hash_table = *agg_method.hash_table;
                        using HashTableType = std::decay_t<decltype(hash_table)>;

                        agg_method.reset();

                        hash_table.for_each_mapped([&](auto& mapped) {
                            if (mapped) {
                                static_cast<void>(_destroy_agg_status(mapped));
                                mapped = nullptr;
                            }
                        });

                        if (hash_table.has_null_key_data()) {
                            auto st = _destroy_agg_status(hash_table.template get_null_key_data<
                                                          vectorized::AggregateDataPtr>());
                            RETURN_IF_ERROR(st);
                        }

                        aggregate_data_container.reset(new AggregateDataContainer(
                                sizeof(typename HashTableType::key_type),
                                ((total_size_of_aggregate_states + align_aggregate_states - 1) /
                                 align_aggregate_states) *
                                        align_aggregate_states));
                        agg_method.hash_table.reset(new HashTableType());
                        agg_arena_pool.reset(new vectorized::Arena);
                        return Status::OK();
                    }},
            agg_data->method_variant);
}

void PartitionedAggSharedState::init_spill_params(size_t spill_partition_count) {
    partition_count = spill_partition_count;
    max_partition_index = partition_count - 1;

    for (int i = 0; i < partition_count; ++i) {
        spill_partitions.emplace_back(std::make_shared<AggSpillPartition>());
    }
}

void PartitionedAggSharedState::update_spill_stream_profiles(RuntimeProfile* source_profile) {
    for (auto& partition : spill_partitions) {
        if (partition->spilling_stream_) {
            partition->spilling_stream_->update_shared_profiles(source_profile);
        }
        for (auto& stream : partition->spill_streams_) {
            if (stream) {
                stream->update_shared_profiles(source_profile);
            }
        }
    }
}

Status AggSpillPartition::get_spill_stream(RuntimeState* state, int node_id,
                                           RuntimeProfile* profile,
                                           vectorized::SpillStreamSPtr& spill_stream) {
    if (spilling_stream_) {
        spill_stream = spilling_stream_;
        return Status::OK();
    }
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            state, spilling_stream_, print_id(state->query_id()), "agg", node_id,
            std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(), profile));
    spill_streams_.emplace_back(spilling_stream_);
    spill_stream = spilling_stream_;
    return Status::OK();
}
void AggSpillPartition::close() {
    if (spilling_stream_) {
        spilling_stream_.reset();
    }
    for (auto& stream : spill_streams_) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
    spill_streams_.clear();
}

void PartitionedAggSharedState::close() {
    // need to use CAS instead of only `if (!is_closed)` statement,
    // to avoid concurrent entry of close() both pass the if statement
    bool false_close = false;
    if (!is_closed.compare_exchange_strong(false_close, true)) {
        return;
    }
    DCHECK(!false_close && is_closed);
    for (auto partition : spill_partitions) {
        partition->close();
    }
    spill_partitions.clear();
}

void SpillSortSharedState::update_spill_stream_profiles(RuntimeProfile* source_profile) {
    for (auto& stream : sorted_streams) {
        if (stream) {
            stream->update_shared_profiles(source_profile);
        }
    }
}

void SpillSortSharedState::close() {
    // need to use CAS instead of only `if (!is_closed)` statement,
    // to avoid concurrent entry of close() both pass the if statement
    bool false_close = false;
    if (!is_closed.compare_exchange_strong(false_close, true)) {
        return;
    }
    DCHECK(!false_close && is_closed);
    for (auto& stream : sorted_streams) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
    sorted_streams.clear();
}

MultiCastSharedState::MultiCastSharedState(ObjectPool* pool, int cast_sender_count, int node_id)
        : multi_cast_data_streamer(std::make_unique<pipeline::MultiCastDataStreamer>(
                  this, pool, cast_sender_count, node_id)) {}

void MultiCastSharedState::update_spill_stream_profiles(RuntimeProfile* source_profile) {}

int AggSharedState::get_slot_column_id(const vectorized::AggFnEvaluator* evaluator) {
    auto ctxs = evaluator->input_exprs_ctxs();
    CHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
            << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
            << ctxs[0]->root()->debug_string();
    return ((vectorized::VSlotRef*)ctxs[0]->root().get())->column_id();
}

Status AggSharedState::_destroy_agg_status(vectorized::AggregateDataPtr data) {
    for (int i = 0; i < aggregate_evaluators.size(); ++i) {
        aggregate_evaluators[i]->function()->destroy(data + offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

LocalExchangeSharedState::~LocalExchangeSharedState() = default;

Status SetSharedState::update_build_not_ignore_null(const vectorized::VExprContextSPtrs& ctxs) {
    if (ctxs.size() > build_not_ignore_null.size()) {
        return Status::InternalError("build_not_ignore_null not initialized");
    }

    for (int i = 0; i < ctxs.size(); ++i) {
        build_not_ignore_null[i] = build_not_ignore_null[i] || ctxs[i]->root()->is_nullable();
    }

    return Status::OK();
}

size_t SetSharedState::get_hash_table_size() const {
    size_t hash_table_size = 0;
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    hash_table_size = arg.hash_table->size();
                }
            },
            hash_table_variants->method_variant);
    return hash_table_size;
}

Status SetSharedState::hash_table_init() {
    std::vector<vectorized::DataTypePtr> data_types;
    for (size_t i = 0; i != child_exprs_lists[0].size(); ++i) {
        auto& ctx = child_exprs_lists[0][i];
        auto data_type = ctx->root()->data_type();
        if (build_not_ignore_null[i]) {
            data_type = vectorized::make_nullable(data_type);
        }
        data_types.emplace_back(std::move(data_type));
    }
    return init_hash_method<SetDataVariants>(hash_table_variants.get(), data_types, true);
}

void AggSharedState::refresh_top_limit(size_t row_id,
                                       const vectorized::ColumnRawPtrs& key_columns) {
    for (int j = 0; j < key_columns.size(); ++j) {
        limit_columns[j]->insert_from(*key_columns[j], row_id);
    }
    limit_heap.emplace(limit_columns[0]->size() - 1, limit_columns, order_directions,
                       null_directions);

    limit_heap.pop();
    limit_columns_min = limit_heap.top()._row_id;
}

Status MaterializationSharedState::merge_multi_response(vectorized::Block* block) {
    std::map<int64_t, std::pair<vectorized::Block, int>> _block_maps;
    for (int i = 0; i < block_order_results.size(); ++i) {
        for (auto& [backend_id, rpc_struct] : rpc_struct_map) {
            vectorized::Block partial_block;
            RETURN_IF_ERROR(
                    partial_block.deserialize(rpc_struct.callback->response_->blocks(i).block()));

            if (!partial_block.is_empty_column()) {
                _block_maps[backend_id] = std::make_pair(std::move(partial_block), 0);
            }
        }

        for (int j = 0; j < block_order_results[i].size(); ++j) {
            auto backend_id = block_order_results[i][j];
            if (backend_id) {
                auto& source_block_rows = _block_maps[backend_id];
                DCHECK(source_block_rows.second < source_block_rows.first.rows());
                for (int k = 0; k < response_blocks[i].columns(); ++k) {
                    response_blocks[i].get_column_by_position(k)->insert_from(
                            *source_block_rows.first.get_by_position(k).column,
                            source_block_rows.second);
                }
                source_block_rows.second++;
            } else {
                for (int k = 0; k < response_blocks[i].columns(); ++k) {
                    response_blocks[i].get_column_by_position(k)->insert_default();
                }
            }
        }
    }

    // clear request/response
    for (auto& [_, rpc_struct] : rpc_struct_map) {
        for (int i = 0; i < rpc_struct.request.request_block_descs_size(); ++i) {
            rpc_struct.request.mutable_request_block_descs(i)->clear_row_id();
            rpc_struct.request.mutable_request_block_descs(i)->clear_file_id();
        }
    }

    for (int i = 0, j = 0, rowid_to_block_loc = rowid_locs[j]; i < origin_block.columns(); i++) {
        if (i != rowid_to_block_loc) {
            block->insert(origin_block.get_by_position(i));
        } else {
            auto response_block = response_blocks[j].to_block();
            for (int k = 0; k < response_block.columns(); k++) {
                auto& data = response_block.get_by_position(k);
                response_blocks[j].mutable_columns()[k] = data.column->clone_empty();
                block->insert(data);
            }
            if (++j < rowid_locs.size()) {
                rowid_to_block_loc = rowid_locs[j];
            }
        }
    }
    origin_block.clear();

    return Status::OK();
}

Dependency* MaterializationSharedState::create_source_dependency(int operator_id, int node_id,
                                                                 const std::string& name) {
    auto dep =
            std::make_shared<CountedFinishDependency>(operator_id, node_id, name + "_DEPENDENCY");
    dep->set_shared_state(this);
    // just block source wait for add the counter in sink
    dep->add(0);

    source_deps.push_back(dep);
    return source_deps.back().get();
}

Status MaterializationSharedState::create_muiltget_result(const vectorized::Columns& columns,
                                                          bool eos, bool gc_id_map) {
    const auto rows = columns.empty() ? 0 : columns[0]->size();
    block_order_results.resize(columns.size());

    for (int i = 0; i < columns.size(); ++i) {
        const uint8_t* null_map = nullptr;
        const vectorized::ColumnString* column_rowid = nullptr;
        auto& column = columns[i];

        if (auto column_ptr = check_and_get_column<vectorized::ColumnNullable>(*column)) {
            null_map = column_ptr->get_null_map_data().data();
            column_rowid = assert_cast<const vectorized::ColumnString*>(
                    column_ptr->get_nested_column_ptr().get());
        } else {
            column_rowid = assert_cast<const vectorized::ColumnString*>(column.get());
        }

        auto& block_order = block_order_results[i];
        block_order.resize(rows);

        for (int j = 0; j < rows; ++j) {
            if (!null_map || !null_map[j]) {
                DCHECK(column_rowid->get_data_at(j).size == sizeof(GlobalRowLoacationV2));
                GlobalRowLoacationV2 row_location =
                        *((GlobalRowLoacationV2*)column_rowid->get_data_at(j).data);
                auto rpc_struct = rpc_struct_map.find(row_location.backend_id);
                if (UNLIKELY(rpc_struct == rpc_struct_map.end())) {
                    return Status::InternalError(
                            "MaterializationSinkOperatorX failed to find rpc_struct, backend_id={}",
                            row_location.backend_id);
                }
                rpc_struct->second.request.mutable_request_block_descs(i)->add_row_id(
                        row_location.row_id);
                rpc_struct->second.request.mutable_request_block_descs(i)->add_file_id(
                        row_location.file_id);
                block_order[j] = row_location.backend_id;
            } else {
                block_order[j] = 0;
            }
        }
    }

    if (eos && gc_id_map) {
        for (auto& [_, rpc_struct] : rpc_struct_map) {
            rpc_struct.request.set_gc_id_map(true);
        }
    }
    last_block = eos;
    need_merge_block = rows > 0;

    return Status::OK();
}

Status MaterializationSharedState::init_multi_requests(
        const TMaterializationNode& materialization_node, RuntimeState* state) {
    rpc_struct_inited = true;
    PMultiGetRequestV2 multi_get_request;
    // Initialize the base struct of PMultiGetRequestV2
    multi_get_request.set_be_exec_version(state->be_exec_version());
    multi_get_request.set_wg_id(state->get_query_ctx()->workload_group()->id());
    auto query_id = multi_get_request.mutable_query_id();
    query_id->set_hi(state->query_id().hi);
    query_id->set_lo(state->query_id().lo);
    DCHECK_EQ(materialization_node.column_descs_lists.size(),
              materialization_node.slot_locs_lists.size());

    const auto& tuple_desc =
            state->desc_tbl().get_tuple_descriptor(materialization_node.intermediate_tuple_id);
    const auto& slots = tuple_desc->slots();
    response_blocks =
            std::vector<vectorized::MutableBlock>(materialization_node.column_descs_lists.size());

    for (int i = 0; i < materialization_node.column_descs_lists.size(); ++i) {
        auto request_block_desc = multi_get_request.add_request_block_descs();
        request_block_desc->set_fetch_row_store(materialization_node.fetch_row_stores[i]);
        // Initialize the column_descs and slot_locs
        auto& column_descs = materialization_node.column_descs_lists[i];
        for (auto& column_desc_item : column_descs) {
            TabletColumn(column_desc_item).to_schema_pb(request_block_desc->add_column_descs());
        }

        auto& slot_locs = materialization_node.slot_locs_lists[i];
        tuple_desc->to_protobuf(request_block_desc->mutable_desc());

        auto& column_idxs = materialization_node.column_idxs_lists[i];
        for (auto idx : column_idxs) {
            request_block_desc->add_column_idxs(idx);
        }

        std::vector<SlotDescriptor*> slots_res;
        for (auto& slot_loc_item : slot_locs) {
            slots[slot_loc_item]->to_protobuf(request_block_desc->add_slots());
            slots_res.emplace_back(slots[slot_loc_item]);
        }
        response_blocks[i] = vectorized::MutableBlock(vectorized::Block(slots_res, 10));
    }

    // Initialize the stubs and requests for each BE
    for (const auto& node_info : materialization_node.nodes_info.nodes) {
        auto client = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.async_internal_port);
        if (!client) {
            LOG(WARNING) << "Get rpc stub failed, host=" << node_info.host
                         << ", port=" << node_info.async_internal_port;
            return Status::InternalError("RowIDFetcher failed to init rpc client, host={}, port={}",
                                         node_info.host, node_info.async_internal_port);
        }
        rpc_struct_map.emplace(node_info.id, FetchRpcStruct {.stub = std::move(client),
                                                             .request = multi_get_request,
                                                             .callback = nullptr});
    }
    // add be_num ad count finish counter for source dependency
    ((CountedFinishDependency*)source_deps.back().get())->add((int)rpc_struct_map.size());

    return Status::OK();
}

} // namespace doris::pipeline
