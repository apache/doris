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
#include "pipeline/local_exchange/local_exchanger.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

Dependency* BasicSharedState::create_source_dependency(int operator_id, int node_id,
                                                       std::string name) {
    source_deps.push_back(std::make_shared<Dependency>(operator_id, node_id, name + "_DEPENDENCY"));
    source_deps.back()->set_shared_state(this);
    return source_deps.back().get();
}

Dependency* BasicSharedState::create_sink_dependency(int dest_id, int node_id, std::string name) {
    sink_deps.push_back(std::make_shared<Dependency>(dest_id, node_id, name + "_DEPENDENCY", true));
    sink_deps.back()->set_shared_state(this);
    return sink_deps.back().get();
}

void Dependency::_add_block_task(PipelineTask* task) {
    DCHECK(_blocked_task.empty() || _blocked_task[_blocked_task.size() - 1] != task)
            << "Duplicate task: " << task->debug_string();
    _blocked_task.push_back(task);
}

void Dependency::set_ready() {
    if (_ready) {
        return;
    }
    _watcher.stop();
    std::vector<PipelineTask*> local_block_task {};
    {
        std::unique_lock<std::mutex> lc(_task_lock);
        if (_ready) {
            return;
        }
        _ready = true;
        local_block_task.swap(_blocked_task);
    }
    for (auto* task : local_block_task) {
        task->wake_up();
    }
}

Dependency* Dependency::is_blocked_by(PipelineTask* task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    auto ready = _ready.load();
    if (!ready && task) {
        _add_block_task(task);
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
                   "{}{}: id={}, block task = {}, ready={}, _always_ready={}, count={}",
                   std::string(indentation_level * 2, ' '), _name, _node_id, _blocked_task.size(),
                   _ready, _always_ready, _counter);
    return fmt::to_string(debug_string_buffer);
}

std::string RuntimeFilterDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, runtime filter: {}",
                   Dependency::debug_string(indentation_level), _runtime_filter->formatted_state());
    return fmt::to_string(debug_string_buffer);
}

Dependency* RuntimeFilterDependency::is_blocked_by(PipelineTask* task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    auto ready = _ready.load();
    if (!ready && task) {
        _add_block_task(task);
        task->_blocked_dep = this;
    }
    return ready ? nullptr : this;
}

void RuntimeFilterTimer::call_timeout() {
    _parent->set_ready();
}

void RuntimeFilterTimer::call_ready() {
    _parent->set_ready();
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
                } else if (it->_parent->is_blocked_by(nullptr)) {
                    // This means runtime filter is not ready, so we call timeout or continue to poll this timer.
                    int64_t ms_since_registration = MonotonicMillis() - it->registration_time();
                    if (ms_since_registration > it->wait_time_ms()) {
                        it->call_timeout();
                    } else {
                        new_que.push_back(std::move(it));
                    }
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

LocalExchangeSharedState::LocalExchangeSharedState(int num_instances) {
    source_deps.resize(num_instances, nullptr);
    mem_trackers.resize(num_instances, nullptr);
}

Status AggSharedState::reset_hash_table() {
    return std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> Status {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
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

                        aggregate_data_container.reset(new vectorized::AggregateDataContainer(
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

void PartitionedAggSharedState::init_spill_params(size_t spill_partition_count_bits) {
    partition_count_bits = spill_partition_count_bits;
    partition_count = (1 << spill_partition_count_bits);
    max_partition_index = partition_count - 1;

    for (int i = 0; i < partition_count; ++i) {
        spill_partitions.emplace_back(std::make_shared<AggSpillPartition>());
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
} // namespace doris::pipeline
