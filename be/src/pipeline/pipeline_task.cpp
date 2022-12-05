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
    _sink_timer = ADD_TIMER(_task_profile, "SinkTime");
    _get_block_timer = ADD_TIMER(_task_profile, "GetBlockTime");
    _wait_source_timer = ADD_TIMER(_task_profile, "WaitSourceTime");
    _wait_sink_timer = ADD_TIMER(_task_profile, "WaitSinkTime");
    _wait_worker_timer = ADD_TIMER(_task_profile, "WaitWorkerTime");
    _wait_schedule_timer = ADD_TIMER(_task_profile, "WaitScheduleTime");
    _block_counts = ADD_COUNTER(_task_profile, "NumBlockedTimes", TUnit::UNIT);
    _yield_counts = ADD_COUNTER(_task_profile, "NumYieldTimes", TUnit::UNIT);
}

Status PipelineTask::prepare(RuntimeState* state) {
    DCHECK(_sink);
    DCHECK(_cur_state == NOT_READY);
    _init_profile();
    RETURN_IF_ERROR(_sink->prepare(state));
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->prepare(state));
    }
    _block.reset(new doris::vectorized::Block());
    _init_state();
    _prepared = true;
    return Status::OK();
}

void PipelineTask::_init_state() {
    if (has_dependency()) {
        set_state(BLOCKED_FOR_DEPENDENCY);
    } else if (!(_source->can_read())) {
        set_state(BLOCKED_FOR_SOURCE);
    } else if (!(_sink->can_write())) {
        set_state(BLOCKED_FOR_SINK);
    } else {
        set_state(RUNNABLE);
    }
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
    // FE do not call execute
    if (!_state->get_query_fragments_ctx()
                 ->is_ready_to_execute()) { // TODO pipeline config::s_ready_to_execute
        return true;
    }

    // runtime filter is a dependency
    _dependency_finish = true;
    return false;
}

Status PipelineTask::open() {
    if (_sink) {
        RETURN_IF_ERROR(_sink->open(_state));
    }
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->open(_state));
    }
    _opened = true;
    return Status::OK();
}

Status PipelineTask::execute(bool* eos) {
    SCOPED_ATTACH_TASK(runtime_state());
    SCOPED_TIMER(_task_profile->total_time_counter());
    int64_t time_spent = 0;
    // The status must be runnable
    *eos = false;
    if (!_opened) {
        if (!_source->can_read()) {
            set_state(BLOCKED_FOR_SOURCE);
            return Status::OK();
        }
        if (!_sink->can_write()) {
            set_state(BLOCKED_FOR_SINK);
            return Status::OK();
        }
        SCOPED_RAW_TIMER(&time_spent);
        RETURN_IF_ERROR(open());
    }

    while (!_fragment_context->is_canceled()) {
        if (!_source->can_read() && _data_state != SourceState::MORE_DATA) {
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
        *eos = false;
    }

    return Status::OK();
}

Status PipelineTask::finalize() {
    return _sink->finalize(_state);
}

Status PipelineTask::close() {
    auto s = _sink->close(_state);
    for (auto& op : _operators) {
        auto tem = op->close(_state);
        if (!tem.ok() && s.ok()) {
            s = tem;
        }
    }
    if (_opened) {
        COUNTER_UPDATE(_wait_source_timer, _wait_source_watcher.elapsed_time());
        COUNTER_UPDATE(_wait_sink_timer, _wait_sink_watcher.elapsed_time());
        COUNTER_UPDATE(_wait_worker_timer, _wait_worker_watcher.elapsed_time());
        COUNTER_UPDATE(_wait_schedule_timer, _wait_schedule_watcher.elapsed_time());
    }
    _pipeline->close(_state);
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
        if (state == BLOCKED_FOR_SOURCE) {
            _wait_source_watcher.start();
            COUNTER_UPDATE(_block_counts, 1);
        } else if (state == BLOCKED_FOR_SINK) {
            _wait_sink_watcher.start();
            COUNTER_UPDATE(_block_counts, 1);
        } else if (state == BLOCKED_FOR_DEPENDENCY) {
            COUNTER_UPDATE(_block_counts, 1);
        }
    }
    _cur_state = state;
}

std::string PipelineTask::debug_string() const {
    std::stringstream ss;
    ss << "PipelineTask(" << _index << ")" << get_state_name(_cur_state) << "\nsink: ";
    ss << _sink->debug_string();
    ss << "\n operators(from source to root)";
    for (auto operatr : _operators) {
        ss << "\n" << operatr->debug_string();
    }
    return ss.str();
}

} // namespace doris::pipeline