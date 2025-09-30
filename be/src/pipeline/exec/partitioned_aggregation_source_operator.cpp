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

#include <string>

#include "aggregation_source_operator.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
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
                auto block_ = std::move(local_state._blocks.front());
                merged_rows += block_.rows();
                local_state._blocks.erase(local_state._blocks.begin());
                status = _agg_source_operator->merge_with_serialized_key_helper(
                        local_state._runtime_state.get(), &block_);
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
    if (!local_state._shared_state->is_spilled) {
        auto* source_local_state =
                local_state._runtime_state->get_local_state(_agg_source_operator->operator_id());
        local_state.update_profile<false>(source_local_state->custom_profile());
    }

    RETURN_IF_ERROR(status);
    if (*eos) {
        if (local_state._shared_state->is_spilled) {
            auto* source_local_state = local_state._runtime_state->get_local_state(
                    _agg_source_operator->operator_id());
            local_state.update_profile<true>(source_local_state->custom_profile());

            if (!local_state._shared_state->spill_partitions.empty()) {
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
                    LOG(WARNING) << fmt::format(
                            "Query:{}, agg probe:{}, task:{}, recover agg data error:{}",
                            print_id(query_id), _parent->node_id(), state->task_id(), status);
                }
                _shared_state->close();
            }
        }};
        bool has_agg_data = false;
        size_t accumulated_blocks_size = 0;
        while (!state->is_cancelled() && !has_agg_data &&
               !_shared_state->spill_partitions.empty()) {
            while (!_shared_state->spill_partitions[0]->spill_streams_.empty() &&
                   !state->is_cancelled() && !has_agg_data) {
                auto& stream = _shared_state->spill_partitions[0]->spill_streams_[0];
                stream->set_read_counters(operator_profile());
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
                    ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                    _shared_state->spill_partitions[0]->spill_streams_.pop_front();
                }
            }

            if (_shared_state->spill_partitions[0]->spill_streams_.empty()) {
                _shared_state->spill_partitions.pop_front();
            }
        }

        VLOG_DEBUG << fmt::format(
                "Query:{}, agg probe:{}, task:{}, recover partitioned finished, partitions "
                "left:{}, bytes read:{}",
                print_id(query_id), _parent->node_id(), state->task_id(),
                _shared_state->spill_partitions.size(), accumulated_blocks_size);
        return status;
    };

    auto exception_catch_func = [this, state, spill_func, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::merge_spill_data_cancel", {
            auto st = Status::InternalError(
                    "fault_inject partitioned_agg_source "
                    "merge spill data canceled");
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, st);
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
            _shared_state->spill_partitions.size());
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

bool PartitionedAggLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
