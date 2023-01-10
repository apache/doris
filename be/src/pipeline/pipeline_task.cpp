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

#include "pipeline/pipeline_fragment_context.h"

namespace doris::pipeline {

void PipelineTask::_init_profile() {
    std::stringstream ss;
    ss << "PipelineTask"
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
    _sink_timer = ADD_CHILD_TIMER(_task_profile, "SinkTime", exec_time);
    _finalize_timer = ADD_CHILD_TIMER(_task_profile, "FinalizeTime", exec_time);
    _close_timer = ADD_CHILD_TIMER(_task_profile, "CloseTime", exec_time);

    _wait_source_timer = ADD_TIMER(_task_profile, "WaitSourceTime");
    _wait_sink_timer = ADD_TIMER(_task_profile, "WaitSinkTime");
    _wait_worker_timer = ADD_TIMER(_task_profile, "WaitWorkerTime");
    _wait_schedule_timer = ADD_TIMER(_task_profile, "WaitScheduleTime");
    _block_counts = ADD_COUNTER(_task_profile, "NumBlockedTimes", TUnit::UNIT);
    _block_by_source_counts = ADD_COUNTER(_task_profile, "NumBlockedBySrcTimes", TUnit::UNIT);
    _block_by_sink_counts = ADD_COUNTER(_task_profile, "NumBlockedBySinkTimes", TUnit::UNIT);
    _schedule_counts = ADD_COUNTER(_task_profile, "NumScheduleTimes", TUnit::UNIT);
    _yield_counts = ADD_COUNTER(_task_profile, "NumYieldTimes", TUnit::UNIT);
    _core_change_times = ADD_COUNTER(_task_profile, "CoreChangeTimes", TUnit::UNIT);
}

Status PipelineTask::prepare(RuntimeState* state) {
    DCHECK(_sink);
    DCHECK(_cur_state == NOT_READY);
    _init_profile();
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_prepare_timer);
    RETURN_IF_ERROR(_sink->prepare(state));
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->prepare(state));
    }

    _task_profile->add_info_string("Sink", fmt::format("{}({})", _sink->get_name(), _sink->id()));
    fmt::memory_buffer operator_ids_str;
    for (size_t i = 0; i < _operators.size(); i++) {
        if (i == 0) {
            fmt::format_to(operator_ids_str,
                           fmt::format("[{}({})", _operators[i]->get_name(), _operators[i]->id()));
        } else {
            fmt::format_to(operator_ids_str,
                           fmt::format(", {}({})", _operators[i]->get_name(), _operators[i]->id()));
        }
    }
    fmt::format_to(operator_ids_str, "]");
    _task_profile->add_info_string("OperatorIds(source2root)", fmt::to_string(operator_ids_str));

    _block.reset(new doris::vectorized::Block());

    // We should make sure initial state for task are runnable so that we can do some preparation jobs (e.g. initialize runtime filters).
    set_state(RUNNABLE);
    _prepared = true;
    return Status::OK();
}

bool PipelineTask::has_dependency() {
    if (_dependency_finish) {
        return false;
    }
    if (_fragment_context->is_canceled()) {
        _dependency_finish = true;
        return false;
    }
    if (_pipeline->has_dependency()) {
        return true;
    }

    if (!query_fragments_context()->is_ready_to_execute()) {
        return true;
    }

    // runtime filter is a dependency
    _dependency_finish = true;
    return false;
}

Status PipelineTask::open() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_open_timer);
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->open(_state));
    }
    if (_sink) {
        RETURN_IF_ERROR(_sink->open(_state));
    }
    _opened = true;
    return Status::OK();
}

Status PipelineTask::execute(bool* eos) {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_exec_timer);
    SCOPED_ATTACH_TASK(runtime_state());
    int64_t time_spent = 0;
    // The status must be runnable
    *eos = false;
    if (!_opened) {
        {
            SCOPED_RAW_TIMER(&time_spent);
            auto st = open();
            if (st.is_blocked_by_rf()) {
                set_state(BLOCKED_FOR_RF);
                return Status::OK();
            }
            RETURN_IF_ERROR(st);
        }
        if (has_dependency()) {
            set_state(BLOCKED_FOR_DEPENDENCY);
            return Status::OK();
        }
        if (!_source->can_read()) {
            set_state(BLOCKED_FOR_SOURCE);
            return Status::OK();
        }
        if (!_sink->can_write()) {
            set_state(BLOCKED_FOR_SINK);
            return Status::OK();
        }
    }

    while (!_fragment_context->is_canceled()) {
        if (_data_state != SourceState::MORE_DATA && !_source->can_read()) {
            set_state(BLOCKED_FOR_SOURCE);
            break;
        }
        if (!_sink->can_write()) {
            set_state(BLOCKED_FOR_SINK);
            break;
        }
        if (time_spent > THREAD_TIME_SLICE) {
            COUNTER_UPDATE(_yield_counts, 1);
            break;
        }
        SCOPED_RAW_TIMER(&time_spent);
        _block->clear_column_data(_root->row_desc().num_materialized_slots());
        auto* block = _block.get();

        // Pull block from operator chain
        {
            SCOPED_TIMER(_get_block_timer);
            RETURN_IF_ERROR(_root->get_block(_state, block, _data_state));
        }
        *eos = _data_state == SourceState::FINISHED;
        if (_block->rows() != 0 || *eos) {
            SCOPED_TIMER(_sink_timer);
            RETURN_IF_ERROR(_sink->sink(_state, block, _data_state));
            if (*eos) { // just return, the scheduler will do finish work
                break;
            }
        }
    }

    return Status::OK();
}

Status PipelineTask::finalize() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_finalize_timer);
    return _sink->finalize(_state);
}

Status PipelineTask::close() {
    int64_t close_ns = 0;
    Status s;
    {
        SCOPED_RAW_TIMER(&close_ns);
        s = _sink->close(_state);
        for (auto& op : _operators) {
            auto tem = op->close(_state);
            if (!tem.ok() && s.ok()) {
                s = tem;
            }
        }
    }
    if (_opened) {
        COUNTER_UPDATE(_wait_source_timer, _wait_source_watcher.elapsed_time());
        COUNTER_UPDATE(_schedule_counts, _schedule_time);
        COUNTER_UPDATE(_wait_sink_timer, _wait_sink_watcher.elapsed_time());
        COUNTER_UPDATE(_wait_worker_timer, _wait_worker_watcher.elapsed_time());
        COUNTER_UPDATE(_wait_schedule_timer, _wait_schedule_watcher.elapsed_time());
        COUNTER_UPDATE(_close_timer, close_ns);
        COUNTER_UPDATE(_task_profile->total_time_counter(), close_ns);
    }
    return s;
}

QueryFragmentsCtx* PipelineTask::query_fragments_context() {
    return _fragment_context->get_query_context();
}

// The FSM see PipelineTaskState's comment
void PipelineTask::set_state(PipelineTaskState state) {
    if (_cur_state == state) {
        return;
    }
    if (_cur_state == BLOCKED_FOR_SOURCE) {
        if (state == RUNNABLE) {
            _wait_source_watcher.stop();
        }
    } else if (_cur_state == BLOCKED_FOR_SINK) {
        if (state == RUNNABLE) {
            _wait_sink_watcher.stop();
        }
    } else if (_cur_state == RUNNABLE) {
        COUNTER_UPDATE(_block_counts, 1);
        if (state == BLOCKED_FOR_SOURCE) {
            _wait_source_watcher.start();
            COUNTER_UPDATE(_block_by_source_counts, 1);
        } else if (state == BLOCKED_FOR_SINK) {
            _wait_sink_watcher.start();
            COUNTER_UPDATE(_block_by_sink_counts, 1);
        }
    }
    _cur_state = state;
}

std::string PipelineTask::debug_string() const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "PipelineTask[id = {}, state = {}]\noperators: ", _index,
                   get_state_name(_cur_state));
    for (size_t i = 0; i < _operators.size(); i++) {
        fmt::format_to(debug_string_buffer, "\n{}{}", std::string(i * 2, ' '),
                       _operators[i]->debug_string());
    }
    fmt::format_to(debug_string_buffer, "\n{}{}", std::string(_operators.size() * 2, ' '),
                   _sink->debug_string());
    return fmt::to_string(debug_string_buffer);
}

} // namespace doris::pipeline