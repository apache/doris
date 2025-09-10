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

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <ostream>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/task_queue.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "util/container_util.hpp"
#include "util/defer_op.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

PipelineTask::PipelineTask(
        PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
        std::shared_ptr<PipelineFragmentContext> fragment_context, RuntimeProfile* parent_profile,
        std::map<int,
                 std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
                le_state_map,
        int task_idx)
        :
#ifdef BE_TEST
          _query_id(fragment_context ? fragment_context->get_query_id() : TUniqueId()),
#else
          _query_id(fragment_context->get_query_id()),
#endif
          _pip_id(pipeline->id()),
          _index(task_id),
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
          _pipeline_name(_pipeline->name()) {
    _pipeline_task_watcher.start();
#ifndef BE_TEST
    _query_mem_tracker = fragment_context->get_query_ctx()->query_mem_tracker;
#endif
    _execution_dependencies.push_back(state->get_query_ctx()->get_execution_dependency());
    auto shared_state = _sink->create_shared_state();
    if (shared_state) {
        _sink_shared_state = shared_state;
    }
}
PipelineTask::~PipelineTask() {
// PipelineTask is also hold by task queue( https://github.com/apache/doris/pull/49753),
// so that it maybe the last one to be destructed.
// But pipeline task hold some objects, like operators, shared state, etc. So that should release
// memory manually.
#ifndef BE_TEST
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_mem_tracker);
#endif
    _sink_shared_state.reset();
    _op_shared_states.clear();
    _sink.reset();
    _operators.clear();
    _block.reset();
    _pipeline.reset();
}
Status PipelineTask::prepare(const TPipelineInstanceParams& local_params, const TDataSink& tsink,
                             QueryContext* query_ctx) {
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
        LocalSinkStateInfo info {_task_idx,
                                 _task_profile.get(),
                                 local_params.sender_id,
                                 get_sink_shared_state().get(),
                                 _le_state_map,
                                 tsink};
        RETURN_IF_ERROR(_sink->setup_local_state(_state, info));
    }

    _scan_ranges = find_with_default(local_params.per_node_scan_ranges,
                                     _operators.front()->node_id(), _scan_ranges);
    auto* parent_profile = _state->get_sink_local_state()->profile();
    query_ctx->register_query_statistics(
            _state->get_sink_local_state()->get_query_statistics_ptr());

    for (int op_idx = _operators.size() - 1; op_idx >= 0; op_idx--) {
        auto& op = _operators[op_idx];
        LocalStateInfo info {parent_profile, _scan_ranges, get_op_shared_state(op->operator_id()),
                             _le_state_map, _task_idx};
        RETURN_IF_ERROR(op->setup_local_state(_state, info));
        parent_profile = _state->get_local_state(op->operator_id())->profile();
        query_ctx->register_query_statistics(
                _state->get_local_state(op->operator_id())->get_query_statistics_ptr());
    }
    {
        const auto& deps =
                _state->get_local_state(_source->operator_id())->execution_dependencies();
        std::unique_lock<std::mutex> lc(_dependency_lock);
        std::copy(deps.begin(), deps.end(),
                  std::inserter(_execution_dependencies, _execution_dependencies.end()));
    }
    if (auto fragment = _fragment_context.lock()) {
        if (fragment->get_query_ctx()->is_cancelled()) {
            clear_blocking_state();
        }
    } else {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
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

    _wait_worker_timer = ADD_TIMER(_task_profile, "WaitWorkerTime");

    _schedule_counts = ADD_COUNTER(_task_profile, "NumScheduleTimes", TUnit::UNIT);
    _yield_counts = ADD_COUNTER(_task_profile, "NumYieldTimes", TUnit::UNIT);
    _core_change_times = ADD_COUNTER(_task_profile, "CoreChangeTimes", TUnit::UNIT);
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
        RETURN_IF_ERROR(local_state->open(_state));
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

void PipelineTask::set_task_queue(TaskQueue* task_queue) {
    _task_queue = task_queue;
}

Status PipelineTask::_prepare() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    for (auto& o : _operators) {
        RETURN_IF_ERROR(_state->get_local_state(o->operator_id())->prepare(_state));
    }
    RETURN_IF_ERROR(_state->get_sink_local_state()->prepare(_state));
    return Status::OK();
}

bool PipelineTask::_wait_to_start() {
    // Before task starting, we should make sure
    // 1. Execution dependency is ready (which is controlled by FE 2-phase commit)
    // 2. Runtime filter dependencies are ready
    // 3. All tablets are loaded into local storage
    for (auto* op_dep : _execution_dependencies) {
        _blocked_dep = op_dep->is_blocked_by(shared_from_this());
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
    // `_dry_run = true` means we do not need data from source operator.
    if (!_dry_run) {
        for (int i = _read_dependencies.size() - 1; i >= 0; i--) {
            // `_read_dependencies` is organized according to operators. For each operator, running condition is met iff all dependencies are ready.
            for (auto* dep : _read_dependencies[i]) {
                _blocked_dep = dep->is_blocked_by(shared_from_this());
                if (_blocked_dep != nullptr) {
                    _blocked_dep->start_watcher();
                    return true;
                }
            }
            // If all dependencies are ready for this operator, we can execute this task if no datum is needed from upstream operators.
            if (!_operators[i]->need_more_input_data(_state)) {
                if (VLOG_DEBUG_IS_ON) {
                    VLOG_DEBUG << "query: " << print_id(_state->query_id())
                               << ", task id: " << _index << ", operator " << i
                               << " not need_more_input_data";
                }
                break;
            }
        }
    }

    for (auto* op_dep : _write_dependencies) {
        _blocked_dep = op_dep->is_blocked_by(shared_from_this());
        if (_blocked_dep != nullptr) {
            _blocked_dep->start_watcher();
            return true;
        }
    }
    return false;
}

Status PipelineTask::execute(bool* eos) {
    auto fragment_context = _fragment_context.lock();
    if (!fragment_context) {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }
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
        auto cpu_qs = fragment_context->get_query_ctx()->get_cpu_statistics();
        if (cpu_qs) {
            cpu_qs->add_cpu_nanos(delta_cpu_time);
        }
        fragment_context->get_query_ctx()->update_cpu_time(delta_cpu_time);
    }};
    if (!_wake_up_early) {
        RETURN_IF_ERROR(_prepare());
    }
    if (_wait_to_start()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_prepare());

    // The status must be runnable
    if (!_opened && !fragment_context->is_canceled()) {
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

        SCOPED_RAW_TIMER(&time_spent);
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
    while (!fragment_context->is_canceled()) {
        SCOPED_RAW_TIMER(&time_spent);
        if (_is_blocked()) {
            return Status::OK();
        }

        /// When a task is cancelled,
        /// its blocking state will be cleared and it will transition to a ready state (though it is not truly ready).
        /// Here, checking whether it is cancelled to prevent tasks in a blocking state from being re-executed.
        if (fragment_context->is_canceled()) {
            break;
        }

        if (time_spent > THREAD_TIME_SLICE) {
            COUNTER_UPDATE(_yield_counts, 1);
            break;
        }
        _block->clear_column_data(_root->row_desc().num_materialized_slots());
        auto* block = _block.get();

        auto sink_revocable_mem_size = _sink->revocable_mem_size(_state);
        if (should_revoke_memory(_state, sink_revocable_mem_size)) {
            RETURN_IF_ERROR(_sink->revoke_memory(_state));
            continue;
        }
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
        if (!*eos) {
            SCOPED_TIMER(_get_block_timer);
            _get_block_counter->update(1);
            RETURN_IF_ERROR(_root->get_block_after_projects(_state, block, eos));
        }

        if (*eos) {
            RETURN_IF_ERROR(close(Status::OK(), false));
        }

        if (_block->rows() != 0 || *eos) {
            SCOPED_TIMER(_sink_timer);
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

            Status status = _sink->sink(_state, block, *eos);

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

    RETURN_IF_ERROR(get_task_queue()->push_back(shared_from_this()));
    return Status::OK();
}

bool PipelineTask::should_revoke_memory(RuntimeState* state, int64_t revocable_mem_bytes) {
    auto* query_ctx = state->get_query_ctx();
    auto wg = query_ctx->workload_group();
    if (!wg) {
        LOG_ONCE(INFO) << "no workload group for query " << print_id(state->query_id());
        return false;
    }
    const auto min_revocable_mem_bytes = state->min_revocable_mem();

    if (UNLIKELY(state->enable_force_spill())) {
        if (revocable_mem_bytes >= min_revocable_mem_bytes) {
            LOG_ONCE(INFO) << "spill force, query: " << print_id(state->query_id());
            return true;
        }
    }

    bool is_wg_mem_low_water_mark = false;
    bool is_wg_mem_high_water_mark = false;
    wg->check_mem_used(&is_wg_mem_low_water_mark, &is_wg_mem_high_water_mark);
    if (is_wg_mem_high_water_mark) {
        if (revocable_mem_bytes > min_revocable_mem_bytes) {
            VLOG_DEBUG << "query " << print_id(state->query_id())
                       << " revoke memory, hight water mark";
            return true;
        }
        return false;
    } else if (is_wg_mem_low_water_mark) {
        int64_t spill_threshold = query_ctx->spill_threshold();
        int64_t memory_usage = query_ctx->query_mem_tracker->consumption();
        if (spill_threshold == 0 || memory_usage < spill_threshold) {
            return false;
        }
        auto big_memory_operator_num = query_ctx->get_running_big_mem_op_num();
        DCHECK(big_memory_operator_num >= 0);
        int64_t mem_limit_of_op;
        if (0 == big_memory_operator_num) {
            return false;
        } else {
            mem_limit_of_op = spill_threshold / big_memory_operator_num;
        }

        LOG_EVERY_T(INFO, 1) << "query " << print_id(state->query_id())
                             << " revoke memory, low water mark, revocable_mem_bytes: "
                             << PrettyPrinter::print_bytes(revocable_mem_bytes)
                             << ", mem_limit_of_op: " << PrettyPrinter::print_bytes(mem_limit_of_op)
                             << ", min_revocable_mem_bytes: "
                             << PrettyPrinter::print_bytes(min_revocable_mem_bytes)
                             << ", memory_usage: " << PrettyPrinter::print_bytes(memory_usage)
                             << ", spill_threshold: " << PrettyPrinter::print_bytes(spill_threshold)
                             << ", big_memory_operator_num: " << big_memory_operator_num;
        return (revocable_mem_bytes > mem_limit_of_op ||
                revocable_mem_bytes > min_revocable_mem_bytes);
    } else {
        return false;
    }
}

void PipelineTask::stop_if_finished() {
    auto fragment = _fragment_context.lock();
    if (!fragment) {
        return;
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(fragment->get_query_ctx()->query_mem_tracker);
    if (auto sink = _sink) {
        if (sink->is_finished(_state)) {
            clear_blocking_state();
        }
    }
}

void PipelineTask::finalize() {
    auto fragment = _fragment_context.lock();
    if (!fragment) {
        return;
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(fragment->get_query_ctx()->query_mem_tracker);
    std::unique_lock<std::mutex> lc(_dependency_lock);
    _finalized = true;
    _sink_shared_state.reset();
    _op_shared_states.clear();
    _le_state_map.clear();
    _block.reset();
    _operators.clear();
    _sink.reset();
    _pipeline.reset();
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
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "QueryId: {}\n", print_id(_query_id));
    fmt::format_to(debug_string_buffer, "InstanceId: {}\n",
                   print_id(_state->fragment_instance_id()));

    fmt::format_to(
            debug_string_buffer,
            "PipelineTask[this = {}, id = {}, open = {}, eos = {}, finalized = {}, dry run = "
            "{}, _wake_up_early = {}, is running = {}]",
            (void*)this, _index, _opened, _eos, _finalized, _dry_run, _wake_up_early.load(),
            is_running());
    std::unique_lock<std::mutex> lc(_dependency_lock);
    auto* cur_blocked_dep = _blocked_dep;
    auto fragment = _fragment_context.lock();
    if (is_finalized() || !fragment) {
        fmt::format_to(debug_string_buffer, " pipeline name = {}", _pipeline_name);
        return fmt::to_string(debug_string_buffer);
    }
    auto elapsed = fragment->elapsed_time() / NANOS_PER_SEC;
    fmt::format_to(debug_string_buffer,
                   " elapse time = {}s, block dependency = [{}]\noperators: ", elapsed,
                   cur_blocked_dep && !is_finalized() ? cur_blocked_dep->debug_string() : "NULL");
    for (size_t i = 0; i < _operators.size(); i++) {
        fmt::format_to(debug_string_buffer, "\n{}",
                       _opened && !_finalized ? _operators[i]->debug_string(_state, i)
                                              : _operators[i]->debug_string(i));
    }
    fmt::format_to(debug_string_buffer, "\n{}\n",
                   _opened && !is_finalized() ? _sink->debug_string(_state, _operators.size())
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

    fmt::format_to(debug_string_buffer, "Write Dependency Information: \n");
    for (size_t j = 0; j < _write_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _write_dependencies[j]->debug_string(i + 1));
    }

    fmt::format_to(debug_string_buffer, "\nExecution Dependency Information: \n");
    for (size_t j = 0; j < _execution_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _execution_dependencies[j]->debug_string(i + 1));
    }

    fmt::format_to(debug_string_buffer, "Finish Dependency Information: \n");
    for (size_t j = 0; j < _finish_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _finish_dependencies[j]->debug_string(j + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

void PipelineTask::wake_up() {
    // call by dependency
    static_cast<void>(get_task_queue()->push_back(shared_from_this()));
}

} // namespace doris::pipeline
