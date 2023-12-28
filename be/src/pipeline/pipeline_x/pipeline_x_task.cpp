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

#include "pipeline_x_task.h"

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <ostream>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/pipeline.h"
#include "pipeline/task_queue.h"
#include "pipeline_x_fragment_context.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "util/container_util.hpp"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

PipelineXTask::PipelineXTask(PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
                             PipelineFragmentContext* fragment_context,
                             RuntimeProfile* parent_profile,
                             std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                                                     std::shared_ptr<LocalExchangeSinkDependency>>>
                                     le_state_map,
                             int task_idx)
        : PipelineTask(pipeline, task_id, state, fragment_context, parent_profile),
          _operators(pipeline->operator_xs()),
          _source(_operators.front()),
          _root(_operators.back()),
          _sink(pipeline->sink_shared_pointer()),
          _le_state_map(std::move(le_state_map)),
          _task_idx(task_idx),
          _execution_dep(state->get_query_ctx()->get_execution_dependency()) {
    _pipeline_task_watcher.start();
    _sink->get_dependency(_downstream_dependency, state->get_query_ctx());
    for (auto& op : _operators) {
        _source_dependency.insert({op->operator_id(), op->get_dependency(state->get_query_ctx())});
    }
    pipeline->incr_created_tasks();
}

Status PipelineXTask::prepare(const TPipelineInstanceParams& local_params, const TDataSink& tsink) {
    DCHECK(_sink);
    DCHECK(_cur_state == PipelineTaskState::NOT_READY) << get_state_name(_cur_state);
    _init_profile();
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_prepare_timer);

    {
        // set sink local state
        LocalSinkStateInfo info {_task_idx,
                                 _task_profile.get(),
                                 local_params.sender_id,
                                 get_downstream_dependency(),
                                 _le_state_map,
                                 tsink};
        RETURN_IF_ERROR(_sink->setup_local_state(_state, info));
    }

    std::vector<TScanRangeParams> no_scan_ranges;
    auto scan_ranges = find_with_default(local_params.per_node_scan_ranges,
                                         _operators.front()->node_id(), no_scan_ranges);
    auto* parent_profile = _state->get_sink_local_state(_sink->operator_id())->profile();
    for (int op_idx = _operators.size() - 1; op_idx >= 0; op_idx--) {
        auto& op = _operators[op_idx];
        auto& deps = get_upstream_dependency(op->operator_id());
        LocalStateInfo info {parent_profile, scan_ranges, deps,
                             _le_state_map,  _task_idx,   _source_dependency[op->operator_id()]};
        RETURN_IF_ERROR(op->setup_local_state(_state, info));
        parent_profile = _state->get_local_state(op->operator_id())->profile();
    }

    _block = doris::vectorized::Block::create_unique();
    RETURN_IF_ERROR(_extract_dependencies());
    // We should make sure initial state for task are runnable so that we can do some preparation jobs (e.g. initialize runtime filters).
    set_state(PipelineTaskState::RUNNABLE);
    _prepared = true;
    return Status::OK();
}

Status PipelineXTask::_extract_dependencies() {
    for (auto op : _operators) {
        auto result = _state->get_local_state_result(op->operator_id());
        if (!result) {
            return result.error();
        }
        auto* local_state = result.value();
        auto* dep = local_state->dependency();
        DCHECK(dep != nullptr);
        _read_dependencies.push_back(dep);
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            _finish_dependencies.push_back(fin_dep);
        }
    }
    {
        auto result = _state->get_sink_local_state_result(_sink->operator_id());
        if (!result) {
            return result.error();
        }
        auto* local_state = result.value();
        auto* dep = local_state->dependency();
        DCHECK(dep != nullptr);
        _write_dependencies = dep;
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            _finish_dependencies.push_back(fin_dep);
        }
    }
    {
        auto result = _state->get_local_state_result(_source->operator_id());
        if (!result) {
            return result.error();
        }
        _filter_dependency = result.value()->filterdependency();
    }
    return Status::OK();
}

void PipelineXTask::_init_profile() {
    std::stringstream ss;
    ss << "PipelineXTask"
       << " (index=" << _index << ")";
    auto* task_profile = new RuntimeProfile(ss.str());
    _parent_profile->add_child(task_profile, true, nullptr);
    _task_profile.reset(task_profile);
    _task_cpu_timer = ADD_TIMER(_task_profile, "TaskCpuTime");

    static const char* exec_time = "ExecuteTime";
    _exec_timer = ADD_TIMER(_task_profile, exec_time);
    _prepare_timer = ADD_CHILD_TIMER(_task_profile, "PrepareTime", exec_time);
    _open_timer = ADD_CHILD_TIMER(_task_profile, "OpenTime", exec_time);
    _get_block_timer = ADD_CHILD_TIMER(_task_profile, "GetBlockTime", exec_time);
    _get_block_counter = ADD_COUNTER(_task_profile, "GetBlockCounter", TUnit::UNIT);
    _sink_timer = ADD_CHILD_TIMER(_task_profile, "SinkTime", exec_time);
    _close_timer = ADD_CHILD_TIMER(_task_profile, "CloseTime", exec_time);

    _wait_bf_timer = ADD_TIMER(_task_profile, "WaitBfTime");
    _wait_worker_timer = ADD_TIMER(_task_profile, "WaitWorkerTime");

    _block_counts = ADD_COUNTER(_task_profile, "NumBlockedTimes", TUnit::UNIT);
    _block_by_source_counts = ADD_COUNTER(_task_profile, "NumBlockedBySrcTimes", TUnit::UNIT);
    _block_by_sink_counts = ADD_COUNTER(_task_profile, "NumBlockedBySinkTimes", TUnit::UNIT);
    _schedule_counts = ADD_COUNTER(_task_profile, "NumScheduleTimes", TUnit::UNIT);
    _yield_counts = ADD_COUNTER(_task_profile, "NumYieldTimes", TUnit::UNIT);
    _core_change_times = ADD_COUNTER(_task_profile, "CoreChangeTimes", TUnit::UNIT);

    _wait_bf_counts = ADD_COUNTER(_task_profile, "WaitBfTimes", TUnit::UNIT);
    _wait_dependency_counts = ADD_COUNTER(_task_profile, "WaitDenpendencyTimes", TUnit::UNIT);
    _pending_finish_counts = ADD_COUNTER(_task_profile, "PendingFinishTimes", TUnit::UNIT);
}

void PipelineXTask::_fresh_profile_counter() {
    COUNTER_SET(_wait_bf_timer, (int64_t)_wait_bf_watcher.elapsed_time());
    COUNTER_SET(_schedule_counts, (int64_t)_schedule_time);
    COUNTER_SET(_wait_worker_timer, (int64_t)_wait_worker_watcher.elapsed_time());
}

Status PipelineXTask::_open() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_open_timer);
    _dry_run = _sink->should_dry_run(_state);
    for (auto& o : _operators) {
        auto* local_state = _state->get_local_state(o->operator_id());
        for (size_t i = 0; i < 2; i++) {
            auto st = local_state->open(_state);
            if (st.is<ErrorCode::PIP_WAIT_FOR_RF>()) {
                DCHECK(_filter_dependency);
                _blocked_dep = _filter_dependency->is_blocked_by(this);
                if (_blocked_dep) {
                    set_state(PipelineTaskState::BLOCKED_FOR_RF);
                    RETURN_IF_ERROR(st);
                } else if (i == 1) {
                    CHECK(false) << debug_string();
                }
            } else {
                break;
            }
        }
    }
    RETURN_IF_ERROR(_state->get_sink_local_state(_sink->operator_id())->open(_state));
    _opened = true;
    return Status::OK();
}

Status PipelineXTask::execute(bool* eos) {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_exec_timer);
    SCOPED_ATTACH_TASK(_state);
    int64_t time_spent = 0;
    Defer defer {[&]() {
        if (_task_queue) {
            _task_queue->update_statistics(this, time_spent);
        }
    }};
    // The status must be runnable
    *eos = false;
    if (!_opened) {
        {
            SCOPED_RAW_TIMER(&time_spent);
            auto st = _open();
            if (st.is<ErrorCode::PIP_WAIT_FOR_RF>()) {
                return Status::OK();
            }
            RETURN_IF_ERROR(st);
        }
        if (has_dependency()) {
            set_state(PipelineTaskState::BLOCKED_FOR_DEPENDENCY);
            return Status::OK();
        }
        if (!source_can_read()) {
            set_state(PipelineTaskState::BLOCKED_FOR_SOURCE);
            return Status::OK();
        }
        if (!sink_can_write()) {
            set_state(PipelineTaskState::BLOCKED_FOR_SINK);
            return Status::OK();
        }
    }

    set_begin_execute_time();
    while (!_fragment_context->is_canceled()) {
        if (_data_state != SourceState::MORE_DATA && !source_can_read()) {
            set_state(PipelineTaskState::BLOCKED_FOR_SOURCE);
            break;
        }
        if (!sink_can_write()) {
            set_state(PipelineTaskState::BLOCKED_FOR_SINK);
            break;
        }
        if (time_spent > THREAD_TIME_SLICE) {
            COUNTER_UPDATE(_yield_counts, 1);
            break;
        }
        // TODO llj: Pipeline entity should_yield
        SCOPED_RAW_TIMER(&time_spent);
        _block->clear_column_data(_root->row_desc().num_materialized_slots());
        auto* block = _block.get();

        // Pull block from operator chain
        if (!_dry_run) {
            SCOPED_TIMER(_get_block_timer);
            _get_block_counter->update(1);
            RETURN_IF_ERROR(_root->get_block_after_projects(_state, block, _data_state));
        } else {
            _data_state = SourceState::FINISHED;
        }

        *eos = _data_state == SourceState::FINISHED;
        if (_block->rows() != 0 || *eos) {
            SCOPED_TIMER(_sink_timer);
            auto status = _sink->sink(_state, block, _data_state);
            if (!status.is<ErrorCode::END_OF_FILE>()) {
                RETURN_IF_ERROR(status);
            }
            *eos = status.is<ErrorCode::END_OF_FILE>() ? true : *eos;
            if (*eos) { // just return, the scheduler will do finish work
                break;
            }
        }
    }

    return Status::OK();
}

void PipelineXTask::finalize() {
    PipelineTask::finalize();
    std::unique_lock<std::mutex> lc(_release_lock);
    _finished = true;
    std::vector<DependencySPtr> {}.swap(_downstream_dependency);
    DependencyMap {}.swap(_upstream_dependency);
    std::map<int, DependencySPtr> {}.swap(_source_dependency);

    _le_state_map.clear();
}

Status PipelineXTask::try_close(Status exec_status) {
    if (_try_close_flag) {
        return Status::OK();
    }
    _try_close_flag = true;
    Status status1 = _sink->try_close(_state, exec_status);
    Status status2 = _source->try_close(_state);
    return status1.ok() ? status2 : status1;
}

Status PipelineXTask::close(Status exec_status) {
    int64_t close_ns = 0;
    Defer defer {[&]() {
        if (_task_queue) {
            _task_queue->update_statistics(this, close_ns);
        }
    }};
    Status s;
    {
        SCOPED_RAW_TIMER(&close_ns);
        s = _sink->close(_state, exec_status);
        for (auto& op : _operators) {
            auto tem = op->close(_state);
            if (!tem.ok() && s.ok()) {
                s = tem;
            }
        }
    }
    if (_opened) {
        _fresh_profile_counter();
        COUNTER_SET(_close_timer, close_ns);
        COUNTER_UPDATE(_task_profile->total_time_counter(), close_ns);
    }
    return s;
}

Status PipelineXTask::close_sink(Status exec_status) {
    return _sink->close(_state, exec_status);
}

std::string PipelineXTask::debug_string() {
    std::unique_lock<std::mutex> lc(_release_lock);
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "QueryId: {}\n", print_id(query_context()->query_id()));
    fmt::format_to(debug_string_buffer, "InstanceId: {}\n",
                   print_id(_state->fragment_instance_id()));

    fmt::format_to(debug_string_buffer,
                   "PipelineTask[this = {}, state = {}, data state = {}, dry run = {}, elapse time "
                   "= {}ns], block dependency = {}, is running = {}\noperators: ",
                   (void*)this, get_state_name(_cur_state), (int)_data_state, _dry_run,
                   MonotonicNanos() - _fragment_context->create_time(),
                   _blocked_dep ? _blocked_dep->debug_string() : "NULL", is_running());
    for (size_t i = 0; i < _operators.size(); i++) {
        fmt::format_to(
                debug_string_buffer, "\n{}",
                _opened ? _operators[i]->debug_string(_state, i) : _operators[i]->debug_string(i));
    }
    fmt::format_to(debug_string_buffer, "\n{}",
                   _opened ? _sink->debug_string(_state, _operators.size())
                           : _sink->debug_string(_operators.size()));
    if (_finished) {
        return fmt::to_string(debug_string_buffer);
    }
    fmt::format_to(debug_string_buffer, "\nRead Dependency Information: \n");
    size_t i = 0;
    for (; i < _read_dependencies.size(); i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _read_dependencies[i]->debug_string(i + 1));
    }

    fmt::format_to(debug_string_buffer, "Write Dependency Information: \n");
    fmt::format_to(debug_string_buffer, "{}. {}\n", i, _write_dependencies->debug_string(1));
    i++;

    if (_filter_dependency) {
        fmt::format_to(debug_string_buffer, "Runtime Filter Dependency Information: \n");
        fmt::format_to(debug_string_buffer, "{}. {}\n", i, _filter_dependency->debug_string(1));
        i++;
    }

    fmt::format_to(debug_string_buffer, "Finish Dependency Information: \n");
    for (size_t j = 0; j < _finish_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _finish_dependencies[j]->debug_string(j + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

void PipelineXTask::wake_up() {
    // call by dependency
    static_cast<void>(get_task_queue()->push_back(this));
}

} // namespace doris::pipeline
