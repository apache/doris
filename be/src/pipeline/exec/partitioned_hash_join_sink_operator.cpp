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

#include "partitioned_hash_join_sink_operator.h"

#include "pipeline/exec/operator.h"
#include "runtime/fragment_mgr.h"
#include "util/mem_info.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

Status PartitionedHashJoinSinkLocalState::init(doris::RuntimeState* state,
                                               doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    _shared_state->partitioned_build_blocks.resize(p._partition_count);
    _shared_state->spilled_streams.resize(p._partition_count);

    _internal_runtime_profile.reset(new RuntimeProfile("internal_profile"));

    _partition_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "PartitionTime", "Spill", 1);
    _partition_shuffle_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile(), "PartitionShuffleTime", "Spill", 1);
    _spill_build_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "SpillBuildTime", "Spill", 1);

    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::open(state));
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    for (uint32_t i = 0; i != p._partition_count; ++i) {
        auto& spilling_stream = _shared_state->spilled_streams[i];
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                state, spilling_stream, print_id(state->query_id()),
                fmt::format("hash_build_sink_{}", i), _parent->node_id(),
                std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(), _profile));
        RETURN_IF_ERROR(spilling_stream->prepare_spill());
        spilling_stream->set_write_counters(_spill_serialize_block_timer, _spill_block_count,
                                            _spill_data_size, _spill_write_disk_timer,
                                            _spill_write_wait_io_timer);
    }
    return p._partitioner->clone(state, _partitioner);
}

Status PartitionedHashJoinSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(PipelineXSpillSinkLocalState::exec_time_counter());
    SCOPED_TIMER(PipelineXSpillSinkLocalState::_close_timer);
    if (PipelineXSpillSinkLocalState::_closed) {
        return Status::OK();
    }
    dec_running_big_mem_op_num(state);
    return PipelineXSpillSinkLocalState::close(state, exec_status);
}

size_t PartitionedHashJoinSinkLocalState::revocable_mem_size(RuntimeState* state) const {
    /// If no need to spill, all rows were sunk into the `_inner_sink_operator` without partitioned.
    if (!_shared_state->need_to_spill) {
        if (_shared_state->inner_shared_state) {
            auto inner_sink_state_ = _shared_state->inner_runtime_state->get_sink_local_state();
            if (inner_sink_state_) {
                auto inner_sink_state =
                        assert_cast<HashJoinBuildSinkLocalState*>(inner_sink_state_);
                return inner_sink_state->_build_side_mem_used;
            }
        }
        return 0;
    }

    size_t mem_size = 0;
    auto& partitioned_blocks = _shared_state->partitioned_build_blocks;
    for (auto& block : partitioned_blocks) {
        if (block) {
            auto block_bytes = block->allocated_bytes();
            if (block_bytes >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                mem_size += block_bytes;
            }
        }
    }
    return mem_size;
}

Status PartitionedHashJoinSinkLocalState::_revoke_unpartitioned_block(RuntimeState* state) {
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    _shared_state->inner_shared_state->hash_table_variants.reset();
    auto row_desc = p._child_x->row_desc();
    const auto num_slots = row_desc.num_slots();
    vectorized::Block build_block;
    auto inner_sink_state_ = _shared_state->inner_runtime_state->get_sink_local_state();
    if (inner_sink_state_) {
        auto inner_sink_state = assert_cast<HashJoinBuildSinkLocalState*>(inner_sink_state_);
        build_block = inner_sink_state->_build_side_mutable_block.to_block();
    }

    if (build_block.rows() <= 1) {
        LOG(WARNING) << "has no data to revoke, node: " << _parent->node_id()
                     << ", task: " << state->task_id();
        return Status::OK();
    }

    if (build_block.columns() > num_slots) {
        build_block.erase(num_slots);
    }

    auto spill_func = [build_block = std::move(build_block), state, this]() mutable {
        auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
        auto& partitioned_blocks = _shared_state->partitioned_build_blocks;
        std::vector<std::vector<uint32_t>> partitions_indexes(p._partition_count);

        const size_t reserved_size = 4096;
        std::for_each(partitions_indexes.begin(), partitions_indexes.end(),
                      [](std::vector<uint32_t>& indices) { indices.reserve(reserved_size); });

        auto flush_rows = [&state, this](std::unique_ptr<vectorized::MutableBlock>& partition_block,
                                         vectorized::SpillStreamSPtr& spilling_stream) {
            auto block = partition_block->to_block();
            auto status = spilling_stream->spill_block(state, block, false);

            if (!status.ok()) {
                std::unique_lock<std::mutex> lock(_spill_lock);
                _spill_status = status;
                _spill_status_ok = false;
                _dependency->set_ready();
                return false;
            }
            return true;
        };

        size_t total_rows = build_block.rows();
        size_t offset = 1;
        while (offset < total_rows) {
            auto sub_block = build_block.clone_empty();
            size_t this_run = std::min(reserved_size, total_rows - offset);

            for (size_t i = 0; i != build_block.columns(); ++i) {
                sub_block.get_by_position(i).column =
                        build_block.get_by_position(i).column->cut(offset, this_run);
            }

            offset += this_run;
            const auto is_last_block = offset == total_rows;

            {
                SCOPED_TIMER(_partition_timer);
                (void)_partitioner->do_partitioning(state, &sub_block, _mem_tracker.get());
            }

            const auto* channel_ids = _partitioner->get_channel_ids().get<uint32_t>();
            for (size_t i = 0; i != sub_block.rows(); ++i) {
                partitions_indexes[channel_ids[i]].emplace_back(i);
            }

            for (uint32_t partition_idx = 0; partition_idx != p._partition_count; ++partition_idx) {
                auto* begin = partitions_indexes[partition_idx].data();
                auto* end = begin + partitions_indexes[partition_idx].size();
                auto& partition_block = partitioned_blocks[partition_idx];
                vectorized::SpillStreamSPtr& spilling_stream =
                        _shared_state->spilled_streams[partition_idx];
                if (UNLIKELY(!partition_block)) {
                    partition_block =
                            vectorized::MutableBlock::create_unique(build_block.clone_empty());
                }

                {
                    SCOPED_TIMER(_partition_shuffle_timer);
                    Status st = partition_block->add_rows(&sub_block, begin, end);
                    if (!st.ok()) {
                        std::unique_lock<std::mutex> lock(_spill_lock);
                        _spill_status = st;
                        _spill_status_ok = false;
                        _dependency->set_ready();
                        return;
                    }
                    partitions_indexes[partition_idx].clear();
                }

                if (partition_block->rows() >= reserved_size || is_last_block) {
                    if (!flush_rows(partition_block, spilling_stream)) {
                        return;
                    }
                    partition_block =
                            vectorized::MutableBlock::create_unique(build_block.clone_empty());
                }
            }
        }

        _dependency->set_ready();
    };

    auto exception_catch_func = [spill_func, this]() mutable {
        auto status = [&]() {
            RETURN_IF_CATCH_EXCEPTION(spill_func());
            return Status::OK();
        }();

        if (!status.ok()) {
            std::unique_lock<std::mutex> lock(_spill_lock);
            _spill_status = status;
            _spill_status_ok = false;
            _dependency->set_ready();
        }
    };

    auto spill_runnable = std::make_shared<SpillRunnable>(state, _shared_state->shared_from_this(),
                                                          exception_catch_func);

    auto* thread_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();

    _dependency->block();
    DBUG_EXECUTE_IF(
            "fault_inject::partitioned_hash_join_sink::revoke_unpartitioned_block_submit_func", {
                return Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_sink "
                        "revoke_unpartitioned_block submit_func failed");
            });
    return thread_pool->submit(std::move(spill_runnable));
}

Status PartitionedHashJoinSinkLocalState::revoke_memory(RuntimeState* state) {
    LOG(INFO) << "hash join sink " << _parent->node_id() << " revoke_memory"
              << ", eos: " << _child_eos;
    DCHECK_EQ(_spilling_streams_count, 0);

    if (!_shared_state->need_to_spill) {
        profile()->add_info_string("Spilled", "true");
        _shared_state->need_to_spill = true;
        return _revoke_unpartitioned_block(state);
    }

    _spilling_streams_count = _shared_state->partitioned_build_blocks.size();

    auto query_id = state->query_id();

    for (size_t i = 0; i != _shared_state->partitioned_build_blocks.size(); ++i) {
        vectorized::SpillStreamSPtr& spilling_stream = _shared_state->spilled_streams[i];
        auto& mutable_block = _shared_state->partitioned_build_blocks[i];

        if (!mutable_block ||
            mutable_block->allocated_bytes() < vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            --_spilling_streams_count;
            continue;
        }

        DCHECK(spilling_stream != nullptr);

        auto* spill_io_pool =
                ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
        DCHECK(spill_io_pool != nullptr);

        MonotonicStopWatch submit_timer;
        submit_timer.start();

        Status st;
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::revoke_memory_submit_func", {
            st = Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_sink revoke_memory submit_func failed");
        });

        auto spill_runnable = std::make_shared<SpillRunnable>(
                state, _shared_state->shared_from_this(),
                [this, query_id, spilling_stream, i, submit_timer] {
                    DBUG_EXECUTE_IF(
                            "fault_inject::partitioned_hash_join_sink::revoke_memory_cancel", {
                                ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                                        query_id, Status::InternalError(
                                                          "fault_inject partitioned_hash_join_sink "
                                                          "revoke_memory canceled"));
                                return;
                            });
                    _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
                    SCOPED_TIMER(_spill_build_timer);

                    auto status = [&]() {
                        RETURN_IF_CATCH_EXCEPTION(_spill_to_disk(i, spilling_stream));
                        return Status::OK();
                    }();

                    if (!status.OK()) {
                        std::unique_lock<std::mutex> lock(_spill_lock);
                        _dependency->set_ready();
                        _spill_status_ok = false;
                        _spill_status = std::move(status);
                    }
                });
        if (st.ok()) {
            st = spill_io_pool->submit(std::move(spill_runnable));
        }

        if (!st.ok()) {
            --_spilling_streams_count;
            return st;
        }
    }

    if (_spilling_streams_count > 0) {
        std::unique_lock<std::mutex> lock(_spill_lock);
        if (_spilling_streams_count > 0) {
            _dependency->block();
        } else if (_child_eos) {
            LOG(INFO) << "hash join sink " << _parent->node_id() << " set_ready_to_read"
                      << ", task id: " << state->task_id();
            _dependency->set_ready_to_read();
        }
    }
    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::_partition_block(RuntimeState* state,
                                                           vectorized::Block* in_block,
                                                           size_t begin, size_t end) {
    const auto rows = in_block->rows();
    if (!rows) {
        return Status::OK();
    }
    {
        /// TODO: DO NOT execute build exprs twice(when partition and building hash table)
        SCOPED_TIMER(_partition_timer);
        RETURN_IF_ERROR(_partitioner->do_partitioning(state, in_block, _mem_tracker.get()));
    }

    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    SCOPED_TIMER(_partition_shuffle_timer);
    const auto* channel_ids = _partitioner->get_channel_ids().get<uint32_t>();
    std::vector<std::vector<uint32_t>> partition_indexes(p._partition_count);
    DCHECK_LT(begin, end);
    for (size_t i = begin; i != end; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    auto& partitioned_blocks = _shared_state->partitioned_build_blocks;
    for (uint32_t i = 0; i != p._partition_count; ++i) {
        const auto count = partition_indexes[i].size();
        if (UNLIKELY(count == 0)) {
            continue;
        }

        if (UNLIKELY(!partitioned_blocks[i])) {
            partitioned_blocks[i] =
                    vectorized::MutableBlock::create_unique(in_block->clone_empty());
        }
        RETURN_IF_ERROR(partitioned_blocks[i]->add_rows(in_block, partition_indexes[i].data(),
                                                        partition_indexes[i].data() + count));
    }

    return Status::OK();
}

void PartitionedHashJoinSinkLocalState::_spill_to_disk(
        uint32_t partition_index, const vectorized::SpillStreamSPtr& spilling_stream) {
    auto& partitioned_block = _shared_state->partitioned_build_blocks[partition_index];

    if (_spill_status_ok) {
        auto block = partitioned_block->to_block();
        partitioned_block = vectorized::MutableBlock::create_unique(block.clone_empty());
        auto st = spilling_stream->spill_block(state(), block, false);
        if (!st.ok()) {
            _spill_status_ok = false;
            std::lock_guard<std::mutex> l(_spill_status_lock);
            _spill_status = st;
        }
    }

    auto num = _spilling_streams_count.fetch_sub(1);
    DCHECK_GE(_spilling_streams_count, 0);

    if (num == 1) {
        std::unique_lock<std::mutex> lock(_spill_lock);
        _dependency->set_ready();
        if (_child_eos) {
            LOG(INFO) << "hash join sink " << _parent->node_id() << " set_ready_to_read"
                      << ", task id: " << state()->task_id();
            _dependency->set_ready_to_read();
        }
    }
}

PartitionedHashJoinSinkOperatorX::PartitionedHashJoinSinkOperatorX(
        ObjectPool* pool, int operator_id, const TPlanNode& tnode, const DescriptorTbl& descs,
        bool use_global_rf, uint32_t partition_count)
        : JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState>(pool, operator_id, tnode,
                                                                    descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[1]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs),
          _partition_count(partition_count) {}

Status PartitionedHashJoinSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    _name = "PARTITIONED_HASH_JOIN_SINK_OPERATOR";
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<TExpr> partition_exprs;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _build_exprs.emplace_back(eq_join_conjunct.right);
        partition_exprs.emplace_back(eq_join_conjunct.right);
    }
    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_build_exprs));

    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(_inner_sink_operator->set_child(_child_x));
    RETURN_IF_ERROR(_partitioner->prepare(state, _child_x->row_desc()));
    return _inner_sink_operator->prepare(state);
}

Status PartitionedHashJoinSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(_partitioner->open(state));
    return _inner_sink_operator->open(state);
}

Status PartitionedHashJoinSinkOperatorX::_setup_internal_operator(RuntimeState* state) {
    auto& local_state = get_local_state(state);

    local_state._shared_state->inner_runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    local_state._shared_state->inner_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._shared_state->inner_runtime_state->set_be_number(state->be_number());

    local_state._shared_state->inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._shared_state->inner_runtime_state->resize_op_id_to_local_state(-1);
    local_state._shared_state->inner_runtime_state->set_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    local_state._shared_state->inner_shared_state = std::dynamic_pointer_cast<HashJoinSharedState>(
            _inner_sink_operator->create_shared_state());
    LocalSinkStateInfo info {0,  local_state._internal_runtime_profile.get(),
                             -1, local_state._shared_state->inner_shared_state.get(),
                             {}, {}};

    RETURN_IF_ERROR(_inner_sink_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), info));
    auto* sink_local_state = local_state._shared_state->inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    LocalStateInfo state_info {local_state._internal_runtime_profile.get(),
                               {},
                               local_state._shared_state->inner_shared_state.get(),
                               {},
                               0};

    RETURN_IF_ERROR(_inner_probe_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), state_info));
    auto* probe_local_state = local_state._shared_state->inner_runtime_state->get_local_state(
            _inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));
    RETURN_IF_ERROR(sink_local_state->open(state));
    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                              bool eos) {
    auto& local_state = get_local_state(state);
    local_state.inc_running_big_mem_op_num(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (!local_state._spill_status_ok) {
        DCHECK_NE(local_state._spill_status.code(), 0);
        return local_state._spill_status;
    }

    local_state._child_eos = eos;

    const auto rows = in_block->rows();

    const auto need_to_spill = local_state._shared_state->need_to_spill;
    if (rows == 0) {
        if (eos) {
            LOG(INFO) << "hash join sink " << node_id() << " sink eos, set_ready_to_read"
                      << ", task id: " << state->task_id();

            if (!need_to_spill) {
                if (UNLIKELY(!local_state._shared_state->inner_runtime_state)) {
                    RETURN_IF_ERROR(_setup_internal_operator(state));
                }
                DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::sink_eos", {
                    return Status::Error<INTERNAL_ERROR>(
                            "fault_inject partitioned_hash_join_sink "
                            "sink_eos failed");
                });
                RETURN_IF_ERROR(_inner_sink_operator->sink(
                        local_state._shared_state->inner_runtime_state.get(), in_block, eos));
            }
            local_state._dependency->set_ready_to_read();
        }
        return Status::OK();
    }

    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (need_to_spill) {
        RETURN_IF_ERROR(local_state._partition_block(state, in_block, 0, rows));

        const auto revocable_size = revocable_mem_size(state);
        if (revocable_size > state->min_revocable_mem()) {
            return local_state.revoke_memory(state);
        }
    } else {
        if (UNLIKELY(!local_state._shared_state->inner_runtime_state)) {
            RETURN_IF_ERROR(_setup_internal_operator(state));
        }
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::sink", {
            return Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_sink "
                    "sink failed");
        });
        RETURN_IF_ERROR(_inner_sink_operator->sink(
                local_state._shared_state->inner_runtime_state.get(), in_block, eos));
    }

    if (eos) {
        LOG(INFO) << "hash join sink " << node_id() << " sink eos, set_ready_to_read"
                  << ", task id: " << state->task_id();
        local_state._dependency->set_ready_to_read();
    }

    return Status::OK();
}

size_t PartitionedHashJoinSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    return local_state.revocable_mem_size(state);
}

Status PartitionedHashJoinSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    return local_state.revoke_memory(state);
}

} // namespace doris::pipeline
