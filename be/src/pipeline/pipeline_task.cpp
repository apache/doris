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

#include "pipeline_task.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/container_util.hpp"
#include "util/defer_op.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

PipelineTask::PipelineTask(
        PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
        PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile,
        std::map<int,
                 std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
                le_state_map,
        int task_idx)
        : _index(task_id),
          _pipeline(pipeline),
          _opened(false),
          _state(state),
          _fragment_context(fragment_context),
          _parent_profile(parent_profile),
          _operators(pipeline->operators()),
          _source(_operators.front().get()),
          _root(_operators.back().get()),
          _sink(pipeline->sink_shared_pointer()),
          _le_state_map(std::move(le_state_map)),
          _task_idx(task_idx),
          _execution_dep(state->get_query_ctx()->get_execution_dependency()) {
    _pipeline_task_watcher.start();

    auto shared_state = _sink->create_shared_state();
    if (shared_state) {
        _sink_shared_state = shared_state;
    }

    const auto dependency_name =
            fmt::format("MemorySufficientDependency_{}_{}", _sink->node_id(), task_id);
    _memory_sufficient_dependency =
            pipeline::Dependency::create_unique(-1, -1, dependency_name, true);
}

Status PipelineTask::prepare(const std::vector<TScanRangeParams>& scan_range, const int sender_id,
                             const TDataSink& tsink, QueryContext* query_ctx) {
    DCHECK(_sink);
    _init_profile();
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_prepare_timer);
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::prepare", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task prepare failed");
        return status;
    });
    {
        // set sink local state
        LocalSinkStateInfo info {_task_idx,     _task_profile.get(),
                                 sender_id,     get_sink_shared_state().get(),
                                 _le_state_map, tsink};
        RETURN_IF_ERROR(_sink->setup_local_state(_state, info));
    }

    _scan_ranges = scan_range;
    auto* parent_profile = _state->get_sink_local_state()->profile();

    for (int op_idx = _operators.size() - 1; op_idx >= 0; op_idx--) {
        auto& op = _operators[op_idx];
        LocalStateInfo info {parent_profile, _scan_ranges, get_op_shared_state(op->operator_id()),
                             _le_state_map, _task_idx};
        RETURN_IF_ERROR(op->setup_local_state(_state, info));
        parent_profile = _state->get_local_state(op->operator_id())->profile();
    }
    {
        std::vector<Dependency*> filter_dependencies;
        const auto& deps = _state->get_local_state(_source->operator_id())->filter_dependencies();
        std::copy(deps.begin(), deps.end(),
                  std::inserter(filter_dependencies, filter_dependencies.end()));

        std::unique_lock<std::mutex> lc(_dependency_lock);
        filter_dependencies.swap(_filter_dependencies);
    }
    if (query_context()->is_cancelled()) {
        clear_blocking_state();
    }
    return Status::OK();
}

Status PipelineTask::_extract_dependencies() {
    std::vector<std::vector<Dependency*>> read_dependencies;
    std::vector<Dependency*> write_dependencies;
    std::vector<Dependency*> finish_dependencies;
    read_dependencies.resize(_operators.size());
    size_t i = 0;
    for (auto& op : _operators) {
        auto result = _state->get_local_state_result(op->operator_id());
        if (!result) {
            return result.error();
        }
        auto* local_state = result.value();
        read_dependencies[i] = local_state->dependencies();
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            finish_dependencies.push_back(fin_dep);
        }
        i++;
    }
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::_extract_dependencies", {
        Status status = Status::Error<INTERNAL_ERROR>(
                "fault_inject pipeline_task _extract_dependencies failed");
        return status;
    });
    {
        auto* local_state = _state->get_sink_local_state();
        write_dependencies = local_state->dependencies();
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            finish_dependencies.push_back(fin_dep);
        }
    }
    {
        std::unique_lock<std::mutex> lc(_dependency_lock);
        read_dependencies.swap(_read_dependencies);
        write_dependencies.swap(_write_dependencies);
        finish_dependencies.swap(_finish_dependencies);
    }
    return Status::OK();
}

void PipelineTask::_init_profile() {
    _task_profile =
            std::make_unique<RuntimeProfile>(fmt::format("PipelineTask (index={})", _index));
    _parent_profile->add_child(_task_profile.get(), true, nullptr);
    _task_cpu_timer = ADD_TIMER(_task_profile, "TaskCpuTime");

    static const char* exec_time = "ExecuteTime";
    _exec_timer = ADD_TIMER(_task_profile, exec_time);
    _prepare_timer = ADD_CHILD_TIMER(_task_profile, "PrepareTime", exec_time);
    _open_timer = ADD_CHILD_TIMER(_task_profile, "OpenTime", exec_time);
    _get_block_timer = ADD_CHILD_TIMER(_task_profile, "GetBlockTime", exec_time);
    _get_block_counter = ADD_COUNTER(_task_profile, "GetBlockCounter", TUnit::UNIT);
    _sink_timer = ADD_CHILD_TIMER(_task_profile, "SinkTime", exec_time);
    _close_timer = ADD_CHILD_TIMER(_task_profile, "CloseTime", exec_time);

    _wait_worker_timer = ADD_TIMER_WITH_LEVEL(_task_profile, "WaitWorkerTime", 1);

    _schedule_counts = ADD_COUNTER(_task_profile, "NumScheduleTimes", TUnit::UNIT);
    _yield_counts = ADD_COUNTER(_task_profile, "NumYieldTimes", TUnit::UNIT);
    _core_change_times = ADD_COUNTER(_task_profile, "CoreChangeTimes", TUnit::UNIT);
    _memory_reserve_times = ADD_COUNTER(_task_profile, "MemoryReserveTimes", TUnit::UNIT);
    _memory_reserve_failed_times =
            ADD_COUNTER(_task_profile, "MemoryReserveFailedTimes", TUnit::UNIT);
}

void PipelineTask::_fresh_profile_counter() {
    COUNTER_SET(_schedule_counts, (int64_t)_schedule_time);
    COUNTER_SET(_wait_worker_timer, (int64_t)_wait_worker_watcher.elapsed_time());
}

Status PipelineTask::_open() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_open_timer);
    _dry_run = _sink->should_dry_run(_state);
    for (auto& o : _operators) {
        auto* local_state = _state->get_local_state(o->operator_id());
        auto st = local_state->open(_state);
        DCHECK(st.is<ErrorCode::PIP_WAIT_FOR_RF>() ? !_filter_dependencies.empty() : true)
                << debug_string();
        RETURN_IF_ERROR(st);
    }
    RETURN_IF_ERROR(_state->get_sink_local_state()->open(_state));
    RETURN_IF_ERROR(_extract_dependencies());
    _block = doris::vectorized::Block::create_unique();
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::open", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task open failed");
        return status;
    });
    _opened = true;
    return Status::OK();
}

bool PipelineTask::_wait_to_start() {
    // Before task starting, we should make sure
    // 1. Execution dependency is ready (which is controlled by FE 2-phase commit)
    // 2. Runtime filter dependencies are ready
    _blocked_dep = _execution_dep->is_blocked_by(this);
    if (_blocked_dep != nullptr) {
        static_cast<Dependency*>(_blocked_dep)->start_watcher();
        return true;
    }

    for (auto* op_dep : _filter_dependencies) {
        _blocked_dep = op_dep->is_blocked_by(this);
        if (_blocked_dep != nullptr) {
            _blocked_dep->start_watcher();
            return true;
        }
    }
    return false;
}

bool PipelineTask::_is_blocked() {
    Defer defer([this] {
        if (_blocked_dep != nullptr) {
            _task_profile->add_info_string("TaskState", "Blocked");
            _task_profile->add_info_string("BlockedByDependency", _blocked_dep->name());
        }
    });

    if (_wake_up_early) {
        return false;
    }

    for (auto* spill_dependency : _spill_dependencies) {
        _blocked_dep = spill_dependency->is_blocked_by(this);
        if (_blocked_dep != nullptr) {
            _blocked_dep->start_watcher();
            return true;
        }
    }

    _blocked_dep = _memory_sufficient_dependency->is_blocked_by(this);
    if (_blocked_dep != nullptr) {
        _blocked_dep->start_watcher();
        return true;
    }

    // `_dry_run = true` means we do not need data from source operator.
    if (!_dry_run) {
        for (int i = _read_dependencies.size() - 1; i >= 0; i--) {
            // `_read_dependencies` is organized according to operators. For each operator, running condition is met iff all dependencies are ready.
            for (auto* dep : _read_dependencies[i]) {
                _blocked_dep = dep->is_blocked_by(this);
                if (_blocked_dep != nullptr) {
                    _blocked_dep->start_watcher();
                    return true;
                }
            }
            // If all dependencies are ready for this operator, we can execute this task if no datum is needed from upstream operators.
            if (!_operators[i]->need_more_input_data(_state)) {
                break;
            }
        }
    }
    for (auto* op_dep : _write_dependencies) {
        _blocked_dep = op_dep->is_blocked_by(this);
        if (_blocked_dep != nullptr) {
            _blocked_dep->start_watcher();
            return true;
        }
    }
    return false;
}

Status PipelineTask::execute(bool* eos) {
    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_TIMER(_exec_timer);
    SCOPED_ATTACH_TASK(_state);

    int64_t time_spent = 0;
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::execute", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task execute failed");
        return status;
    });
    ThreadCpuStopWatch cpu_time_stop_watch;
    cpu_time_stop_watch.start();
    Defer defer {[&]() {
        if (_task_queue) {
            _task_queue->update_statistics(this, time_spent);
        }
        int64_t delta_cpu_time = cpu_time_stop_watch.elapsed_time();
        _task_cpu_timer->update(delta_cpu_time);
        query_context()->resource_ctx()->cpu_context()->update_cpu_cost_ms(delta_cpu_time);
    }};
    if (_wait_to_start()) {
        if (config::enable_prefetch_tablet) {
            RETURN_IF_ERROR(_source->hold_tablets(_state));
        }
        return Status::OK();
    }

    // The status must be runnable
    if (!_opened && !_fragment_context->is_canceled()) {
        DBUG_EXECUTE_IF("PipelineTask::execute.open_sleep", {
            auto required_pipeline_id =
                    DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                            "PipelineTask::execute.open_sleep", "pipeline_id", -1);
            auto required_task_id = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                    "PipelineTask::execute.open_sleep", "task_id", -1);
            if (required_pipeline_id == pipeline_id() && required_task_id == task_id()) {
                LOG(WARNING) << "PipelineTask::execute.open_sleep sleep 5s";
                sleep(5);
            }
        });

        if (_wake_up_early) {
            *eos = true;
            _eos = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(_open());
    }

    auto set_wake_up_and_dep_ready = [&]() {
        if (wake_up_early()) {
            return;
        }
        set_wake_up_early();
        clear_blocking_state();
    };

    _task_profile->add_info_string("TaskState", "Runnable");
    _task_profile->add_info_string("BlockedByDependency", "");
    const auto query_id = _state->query_id();

    while (!_fragment_context->is_canceled()) {
        if (_is_blocked()) {
            return Status::OK();
        }

        /// When a task is cancelled,
        /// its blocking state will be cleared and it will transition to a ready state (though it is not truly ready).
        /// Here, checking whether it is cancelled to prevent tasks in a blocking state from being re-executed.
        if (_fragment_context->is_canceled()) {
            break;
        }

        if (time_spent > THREAD_TIME_SLICE) {
            COUNTER_UPDATE(_yield_counts, 1);
            break;
        }
        _block->clear_column_data(_root->row_desc().num_materialized_slots());
        auto* block = _block.get();

        DBUG_EXECUTE_IF("fault_inject::PipelineXTask::executing", {
            Status status =
                    Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task executing failed");
            return status;
        });

        // `_sink->is_finished(_state)` means sink operator should be finished
        if (_sink->is_finished(_state)) {
            set_wake_up_and_dep_ready();
        }

        // `_dry_run` means sink operator need no more data
        *eos = wake_up_early() || _dry_run;
        auto workload_group = _state->get_query_ctx()->workload_group();
        if (*eos) {
            _pending_block.reset();
        } else if (_pending_block) [[unlikely]] {
            LOG(INFO) << "Query: " << print_id(query_id) << " has pending block, size: "
                      << PrettyPrinter::print_bytes(_pending_block->allocated_bytes());
            _block = std::move(_pending_block);
            block = _block.get();
            *eos = _pending_eos;
        } else {
            SCOPED_TIMER(_get_block_timer);
            if (_state->low_memory_mode()) {
                _sink->set_low_memory_mode(_state);
                _root->set_low_memory_mode(_state);
            }
            DEFER_RELEASE_RESERVED();
            _get_block_counter->update(1);
            const auto reserve_size = _root->get_reserve_mem_size(_state);
            _root->reset_reserve_mem_size(_state);

            if (workload_group && _state->get_query_ctx()->enable_reserve_memory() &&
                reserve_size > 0) {
                auto st = thread_context()->try_reserve_memory(reserve_size);

                COUNTER_UPDATE(_memory_reserve_times, 1);
                if (!st.ok() && !_state->enable_force_spill()) {
                    COUNTER_UPDATE(_memory_reserve_failed_times, 1);
                    auto sink_revokable_mem_size = _sink->revocable_mem_size(_state);
                    auto debug_msg = fmt::format(
                            "Query: {} , try to reserve: {}, operator name: {}, operator id: {}, "
                            "task id: "
                            "{}, root revocable mem size: {}, sink revocable mem size: {}, failed: "
                            "{}",
                            print_id(query_id), PrettyPrinter::print_bytes(reserve_size),
                            _root->get_name(), _root->node_id(), _state->task_id(),
                            PrettyPrinter::print_bytes(_root->revocable_mem_size(_state)),
                            PrettyPrinter::print_bytes(sink_revokable_mem_size), st.to_string());
                    // PROCESS_MEMORY_EXCEEDED error msg alread contains process_mem_log_str
                    if (!st.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
                        debug_msg += fmt::format(", debug info: {}",
                                                 GlobalMemoryArbitrator::process_mem_log_str());
                    }
                    LOG_EVERY_N(INFO, 100) << debug_msg;
                    // If sink has enough revocable memory, trigger revoke memory
                    if (sink_revokable_mem_size >= _state->spill_min_revocable_mem()) {
                        LOG(INFO) << fmt::format(
                                "Query: {} sink: {}, node id: {}, task id: "
                                "{}, revocable mem size: {}",
                                print_id(query_id), _sink->get_name(), _sink->node_id(),
                                _state->task_id(),
                                PrettyPrinter::print_bytes(sink_revokable_mem_size));
                        ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                                _state->get_query_ctx()->shared_from_this(), reserve_size, st);
                        continue;
                    } else {
                        // If reserve failed, not add this query to paused list, because it is very small, will not
                        // consume a lot of memory. But need set low memory mode to indicate that the system should
                        // not use too much memory.
                        _state->get_query_ctx()->set_low_memory_mode();
                    }
                }
            }

            DCHECK_EQ(_pending_block.get(), nullptr);
            RETURN_IF_ERROR(_root->get_block_after_projects(_state, block, eos));
        }

        if (_block->rows() != 0 || *eos) {
            SCOPED_TIMER(_sink_timer);
            Status status = Status::OK();
            DEFER_RELEASE_RESERVED();
            COUNTER_UPDATE(_memory_reserve_times, 1);
            auto workload_group = _state->get_query_ctx()->workload_group();
            if (_state->get_query_ctx()->enable_reserve_memory() && workload_group &&
                !(wake_up_early() || _dry_run)) {
                const auto sink_reserve_size = _sink->get_reserve_mem_size(_state, *eos);
                status = sink_reserve_size != 0
                                 ? thread_context()->try_reserve_memory(sink_reserve_size)
                                 : Status::OK();

                auto sink_revocable_mem_size = _sink->revocable_mem_size(_state);
                if (status.ok() && _state->enable_force_spill() && _sink->is_spillable() &&
                    sink_revocable_mem_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                    status = Status(ErrorCode::QUERY_MEMORY_EXCEEDED, "Force Spill");
                }

                if (!status.ok()) {
                    COUNTER_UPDATE(_memory_reserve_failed_times, 1);
                    auto debug_msg = fmt::format(
                            "Query: {} try to reserve: {}, sink name: {}, node id: {}, task id: "
                            "{}, sink revocable mem size: {}, failed: {}",
                            print_id(query_id), PrettyPrinter::print_bytes(sink_reserve_size),
                            _sink->get_name(), _sink->node_id(), _state->task_id(),
                            PrettyPrinter::print_bytes(sink_revocable_mem_size),
                            status.to_string());
                    // PROCESS_MEMORY_EXCEEDED error msg alread contains process_mem_log_str
                    if (!status.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
                        debug_msg += fmt::format(", debug info: {}",
                                                 GlobalMemoryArbitrator::process_mem_log_str());
                    }
                    // If the operator is not spillable or it is spillable but not has much memory to spill
                    // not need add to paused list, just let it go.
                    if (sink_revocable_mem_size >=
                        vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                        VLOG_DEBUG << debug_msg;
                        DCHECK_EQ(_pending_block.get(), nullptr);
                        _pending_block = std::move(_block);
                        _block = vectorized::Block::create_unique(_pending_block->clone_empty());
                        ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                                _state->get_query_ctx()->shared_from_this(), sink_reserve_size,
                                status);
                        _pending_eos = *eos;
                        *eos = false;
                        continue;
                    } else {
                        _state->get_query_ctx()->set_low_memory_mode();
                    }
                }
            }

            if (*eos) {
                RETURN_IF_ERROR(close(Status::OK(), false));
            }

            DBUG_EXECUTE_IF("PipelineTask::execute.sink_eos_sleep", {
                auto required_pipeline_id =
                        DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                "PipelineTask::execute.sink_eos_sleep", "pipeline_id", -1);
                auto required_task_id =
                        DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                "PipelineTask::execute.sink_eos_sleep", "task_id", -1);
                if (required_pipeline_id == pipeline_id() && required_task_id == task_id()) {
                    LOG(WARNING) << "PipelineTask::execute.sink_eos_sleep sleep 10s";
                    sleep(10);
                }
            });

            status = _sink->sink(_state, block, *eos);

            if (status.is<ErrorCode::END_OF_FILE>()) {
                set_wake_up_and_dep_ready();
            } else if (!status) {
                return status;
            }

            if (*eos) { // just return, the scheduler will do finish work
                _task_profile->add_info_string("TaskState", "Finished");
                _eos = true;
                return Status::OK();
            }
        }
    }

    RETURN_IF_ERROR(get_task_queue()->push_back(this));
    return Status::OK();
}

void PipelineTask::finalize() {
    std::unique_lock<std::mutex> lc(_dependency_lock);
    _finalized = true;
    _sink_shared_state.reset();
    _op_shared_states.clear();
    _le_state_map.clear();
}

Status PipelineTask::close(Status exec_status, bool close_sink) {
    int64_t close_ns = 0;
    Status s;
    {
        SCOPED_RAW_TIMER(&close_ns);
        if (close_sink) {
            s = _sink->close(_state, exec_status);
        }
        for (auto& op : _operators) {
            auto tem = op->close(_state);
            if (!tem.ok() && s.ok()) {
                s = tem;
            }
        }
    }
    if (_opened) {
        COUNTER_UPDATE(_close_timer, close_ns);
        COUNTER_UPDATE(_task_profile->total_time_counter(), close_ns);
    }

    if (close_sink && _opened) {
        _task_profile->add_info_string("WakeUpEarly", wake_up_early() ? "true" : "false");
        _fresh_profile_counter();
    }

    if (_task_queue) {
        _task_queue->update_statistics(this, close_ns);
    }
    return s;
}

std::string PipelineTask::debug_string() {
    std::unique_lock<std::mutex> lc(_dependency_lock);
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "QueryId: {}\n", print_id(query_context()->query_id()));
    fmt::format_to(debug_string_buffer, "InstanceId: {}\n",
                   print_id(_state->fragment_instance_id()));

    auto* cur_blocked_dep = _blocked_dep;
    auto elapsed = _fragment_context->elapsed_time() / 1000000000.0;
    fmt::format_to(debug_string_buffer,
                   "PipelineTask[this = {}, id = {}, open = {}, eos = {}, finish = {}, dry run = "
                   "{}, elapse time = {}s, _wake_up_early = {}], block dependency = {}, is "
                   "running = {}\noperators: ",
                   (void*)this, _index, _opened, _eos, _finalized, _dry_run, elapsed,
                   _wake_up_early.load(),
                   cur_blocked_dep && !_finalized ? cur_blocked_dep->debug_string() : "NULL",
                   is_running());
    for (size_t i = 0; i < _operators.size(); i++) {
        fmt::format_to(debug_string_buffer, "\n{}",
                       _opened && !_finalized ? _operators[i]->debug_string(_state, i)
                                              : _operators[i]->debug_string(i));
    }
    fmt::format_to(debug_string_buffer, "\n{}\n",
                   _opened && !_finalized ? _sink->debug_string(_state, _operators.size())
                                          : _sink->debug_string(_operators.size()));
    if (_finalized) {
        return fmt::to_string(debug_string_buffer);
    }

    size_t i = 0;
    for (; i < _read_dependencies.size(); i++) {
        for (size_t j = 0; j < _read_dependencies[i].size(); j++) {
            fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                           _read_dependencies[i][j]->debug_string(i + 1));
        }
    }

    fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                   _memory_sufficient_dependency->debug_string(i++));

    fmt::format_to(debug_string_buffer, "Write Dependency Information: \n");
    for (size_t j = 0; j < _write_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _write_dependencies[j]->debug_string(i + 1));
    }

    fmt::format_to(debug_string_buffer, "\nRuntime Filter Dependency Information: \n");
    for (size_t j = 0; j < _filter_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _filter_dependencies[j]->debug_string(i + 1));
    }

    fmt::format_to(debug_string_buffer, "Finish Dependency Information: \n");
    for (size_t j = 0; j < _finish_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _finish_dependencies[j]->debug_string(j + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

size_t PipelineTask::get_revocable_size() const {
    if (_finalized || _running || (_eos && !_pending_block)) {
        return 0;
    }

    return _sink->revocable_mem_size(_state);
}

Status PipelineTask::revoke_memory(const std::shared_ptr<SpillContext>& spill_context) {
    if (_finalized) {
        if (spill_context) {
            spill_context->on_task_finished();
            VLOG_DEBUG << "Query: " << print_id(_state->query_id()) << ", task: " << ((void*)this)
                       << " finalized";
        }
        return Status::OK();
    }

    const auto revocable_size = _sink->revocable_mem_size(_state);
    if (revocable_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
        RETURN_IF_ERROR(_sink->revoke_memory(_state, spill_context));
    } else if (spill_context) {
        spill_context->on_task_finished();
        LOG(INFO) << "Query: " << print_id(_state->query_id()) << ", task: " << ((void*)this)
                  << " has not enough data to revoke: " << revocable_size;
    }
    return Status::OK();
}

void PipelineTask::wake_up() {
    // call by dependency
    static_cast<void>(get_task_queue()->push_back(this));
}

QueryContext* PipelineTask::query_context() {
    return _fragment_context->get_query_ctx();
}
} // namespace doris::pipeline
