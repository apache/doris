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

#include <ostream>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline.h"
#include "pipeline_fragment_context.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "task_queue.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "vec/core/future_block.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

PipelineTask::PipelineTask(PipelinePtr& pipeline, uint32_t index, RuntimeState* state,
                           OperatorPtr& sink, PipelineFragmentContext* fragment_context,
                           RuntimeProfile* parent_profile)
        : _index(index),
          _pipeline(pipeline),
          _prepared(false),
          _opened(false),
          _state(state),
          _cur_state(PipelineTaskState::NOT_READY),
          _data_state(SourceState::DEPEND_ON_SOURCE),
          _fragment_context(fragment_context),
          _parent_profile(parent_profile),
          _operators(pipeline->_operators),
          _source(_operators.front()),
          _root(_operators.back()),
          _sink(sink) {
    _pipeline_task_watcher.start();
    _query_statistics.reset(new QueryStatistics());
    _sink->set_query_statistics(_query_statistics);
    _collect_query_statistics_with_every_batch =
            _pipeline->collect_query_statistics_with_every_batch();
}

PipelineTask::PipelineTask(PipelinePtr& pipeline, uint32_t index, RuntimeState* state,
                           PipelineFragmentContext* fragment_context,
                           RuntimeProfile* parent_profile)
        : _index(index),
          _pipeline(pipeline),
          _prepared(false),
          _opened(false),
          _state(state),
          _cur_state(PipelineTaskState::NOT_READY),
          _data_state(SourceState::DEPEND_ON_SOURCE),
          _fragment_context(fragment_context),
          _parent_profile(parent_profile),
          _operators({}),
          _source(nullptr),
          _root(nullptr),
          _sink(nullptr) {
    _pipeline_task_watcher.start();
}

void PipelineTask::_fresh_profile_counter() {
    COUNTER_SET(_wait_source_timer, (int64_t)_wait_source_watcher.elapsed_time());
    COUNTER_SET(_wait_bf_timer, (int64_t)_wait_bf_watcher.elapsed_time());
    COUNTER_SET(_schedule_counts, (int64_t)_schedule_time);
    COUNTER_SET(_wait_sink_timer, (int64_t)_wait_sink_watcher.elapsed_time());
    COUNTER_SET(_wait_worker_timer, (int64_t)_wait_worker_watcher.elapsed_time());
    COUNTER_SET(_begin_execute_timer, _begin_execute_time);
    COUNTER_SET(_eos_timer, _eos_time);
    COUNTER_SET(_src_pending_finish_over_timer, _src_pending_finish_over_time);
    COUNTER_SET(_dst_pending_finish_over_timer, _dst_pending_finish_over_time);
    COUNTER_SET(_pip_task_total_timer, (int64_t)_pipeline_task_watcher.elapsed_time());
}

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
    _get_block_counter = ADD_COUNTER(_task_profile, "GetBlockCounter", TUnit::UNIT);
    _sink_timer = ADD_CHILD_TIMER(_task_profile, "SinkTime", exec_time);
    _finalize_timer = ADD_CHILD_TIMER(_task_profile, "FinalizeTime", exec_time);
    _close_timer = ADD_CHILD_TIMER(_task_profile, "CloseTime", exec_time);

    _wait_source_timer = ADD_TIMER(_task_profile, "WaitSourceTime");
    _wait_bf_timer = ADD_TIMER(_task_profile, "WaitBfTime");
    _wait_sink_timer = ADD_TIMER(_task_profile, "WaitSinkTime");
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

    _begin_execute_timer = ADD_TIMER(_task_profile, "Task1BeginExecuteTime");
    _eos_timer = ADD_TIMER(_task_profile, "Task2EosTime");
    _src_pending_finish_over_timer = ADD_TIMER(_task_profile, "Task3SrcPendingFinishOverTime");
    _dst_pending_finish_over_timer = ADD_TIMER(_task_profile, "Task4DstPendingFinishOverTime");
    _pip_task_total_timer = ADD_TIMER(_task_profile, "Task5TotalTime");
    _close_pipeline_timer = ADD_TIMER(_task_profile, "Task6ClosePipelineTime");
}

Status PipelineTask::prepare(RuntimeState* state) {
    DCHECK(_sink);
    DCHECK(_cur_state == PipelineTaskState::NOT_READY);
    _init_profile();
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_prepare_timer);
    RETURN_IF_ERROR(_sink->prepare(state));
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->prepare(state));
    }

    _task_profile->add_info_string("Sink",
                                   fmt::format("{}(dst_id={})", _sink->get_name(), _sink->id()));
    fmt::memory_buffer operator_ids_str;
    for (size_t i = 0; i < _operators.size(); i++) {
        if (i == 0) {
            fmt::format_to(
                    operator_ids_str,
                    fmt::format("[{}(node_id={})", _operators[i]->get_name(), _operators[i]->id()));
        } else {
            fmt::format_to(operator_ids_str,
                           fmt::format(", {}(node_id={})", _operators[i]->get_name(),
                                       _operators[i]->id()));
        }
    }
    fmt::format_to(operator_ids_str, "]");
    _task_profile->add_info_string("OperatorIds(source2root)", fmt::to_string(operator_ids_str));

    _block = _fragment_context->is_group_commit() ? doris::vectorized::FutureBlock::create_unique()
                                                  : doris::vectorized::Block::create_unique();

    // We should make sure initial state for task are runnable so that we can do some preparation jobs (e.g. initialize runtime filters).
    set_state(PipelineTaskState::RUNNABLE);
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

    if (!query_context()->is_ready_to_execute()) {
        return true;
    }

    // runtime filter is a dependency
    _dependency_finish = true;
    return false;
}

Status PipelineTask::_open() {
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

void PipelineTask::set_task_queue(TaskQueue* task_queue) {
    _task_queue = task_queue;
}

Status PipelineTask::execute(bool* eos) {
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
                set_state(PipelineTaskState::BLOCKED_FOR_RF);
                return Status::OK();
            } else if (st.is<ErrorCode::PIP_WAIT_FOR_SC>()) {
                set_state(PipelineTaskState::BLOCKED_FOR_SOURCE);
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

    this->set_begin_execute_time();
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
        {
            SCOPED_TIMER(_get_block_timer);
            _get_block_counter->update(1);
            RETURN_IF_ERROR(_root->get_block(_state, block, _data_state));
        }
        *eos = _data_state == SourceState::FINISHED;

        if (_block->rows() != 0 || *eos) {
            SCOPED_TIMER(_sink_timer);
            if (_data_state == SourceState::FINISHED ||
                _collect_query_statistics_with_every_batch) {
                RETURN_IF_ERROR(_collect_query_statistics());
            }
            auto status = _sink->sink(_state, block, _data_state);
            if (UNLIKELY(!status.ok() || block->rows() == 0)) {
                if (_fragment_context->is_group_commit()) {
                    auto* future_block = dynamic_cast<vectorized::FutureBlock*>(block);
                    std::unique_lock<doris::Mutex> l(*(future_block->lock));
                    if (!future_block->is_handled()) {
                        future_block->set_result(status, 0, 0);
                        future_block->cv->notify_all();
                    }
                }
            }
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

Status PipelineTask::finalize() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    Defer defer {[&]() {
        if (_task_queue) {
            _task_queue->update_statistics(this, _finalize_timer->value());
        }
    }};
    SCOPED_TIMER(_finalize_timer);
    return _sink->finalize(_state);
}

Status PipelineTask::_collect_query_statistics() {
    // The execnode tree of a fragment will be split into multiple pipelines, we only need to collect the root pipeline.
    if (_pipeline->is_root_pipeline()) {
        // If the current fragment has only one instance, we can collect all of them;
        // otherwise, we need to collect them based on the sender_id.
        DCHECK(_query_statistics);
        if (_state->num_per_fragment_instances() == 1) {
            _query_statistics->clear();
            RETURN_IF_ERROR(_root->collect_query_statistics(_query_statistics.get()));
        } else {
            _query_statistics->clear();
            RETURN_IF_ERROR(_root->collect_query_statistics(_query_statistics.get(),
                                                            _state->per_fragment_instance_idx()));
        }
    }
    return Status::OK();
}

Status PipelineTask::try_close(Status exec_status) {
    if (_try_close_flag) {
        return Status::OK();
    }
    _try_close_flag = true;
    Status status1 = _sink->try_close(_state);
    Status status2 = _source->try_close(_state);
    return status1.ok() ? status2 : status1;
}

Status PipelineTask::close(Status exec_status) {
    int64_t close_ns = 0;
    Defer defer {[&]() {
        if (_task_queue) {
            _task_queue->update_statistics(this, close_ns);
        }
    }};
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
        _fresh_profile_counter();
        COUNTER_SET(_close_timer, close_ns);
        COUNTER_UPDATE(_task_profile->total_time_counter(), close_ns);
    }
    return s;
}

QueryContext* PipelineTask::query_context() {
    return _fragment_context->get_query_context();
}

// The FSM see PipelineTaskState's comment
void PipelineTask::set_state(PipelineTaskState state) {
    DCHECK(_cur_state != PipelineTaskState::FINISHED);

    if (_cur_state == state) {
        return;
    }
    if (_cur_state == PipelineTaskState::BLOCKED_FOR_SOURCE) {
        if (state == PipelineTaskState::RUNNABLE) {
            _wait_source_watcher.stop();
        }
    } else if (_cur_state == PipelineTaskState::BLOCKED_FOR_SINK) {
        if (state == PipelineTaskState::RUNNABLE) {
            _wait_sink_watcher.stop();
        }
    } else if (_cur_state == PipelineTaskState::BLOCKED_FOR_RF) {
        if (state == PipelineTaskState::RUNNABLE) {
            _wait_bf_watcher.stop();
        }
    } else if (_cur_state == PipelineTaskState::RUNNABLE) {
        COUNTER_UPDATE(_block_counts, 1);
        if (state == PipelineTaskState::BLOCKED_FOR_SOURCE) {
            _wait_source_watcher.start();
            COUNTER_UPDATE(_block_by_source_counts, 1);
        } else if (state == PipelineTaskState::BLOCKED_FOR_SINK) {
            _wait_sink_watcher.start();
            COUNTER_UPDATE(_block_by_sink_counts, 1);
        } else if (state == PipelineTaskState::BLOCKED_FOR_RF) {
            _wait_bf_watcher.start();
            COUNTER_UPDATE(_wait_bf_counts, 1);
        } else if (state == PipelineTaskState::BLOCKED_FOR_DEPENDENCY) {
            COUNTER_UPDATE(_wait_dependency_counts, 1);
        } else if (state == PipelineTaskState::PENDING_FINISH) {
            COUNTER_UPDATE(_pending_finish_counts, 1);
        }
    }

    if (state == PipelineTaskState::FINISHED) {
        _finish_p_dependency();
    }

    _cur_state = state;
}

std::string PipelineTask::debug_string() {
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "QueryId: {}\n", print_id(query_context()->query_id()));
    fmt::format_to(debug_string_buffer, "InstanceId: {}\n",
                   print_id(fragment_context()->get_fragment_instance_id()));

    fmt::format_to(debug_string_buffer, "RuntimeUsage: {}\n",
                   PrettyPrinter::print(get_runtime_ns(), TUnit::TIME_NS));
    {
        std::stringstream profile_ss;
        _fresh_profile_counter();
        _task_profile->pretty_print(&profile_ss, "");
        fmt::format_to(debug_string_buffer, "Profile: {}\n", profile_ss.str());
    }
    fmt::format_to(debug_string_buffer,
                   "PipelineTask[this = {}, state = {}]\noperators: ", (void*)this,
                   get_state_name(_cur_state));
    for (size_t i = 0; i < _operators.size(); i++) {
        fmt::format_to(debug_string_buffer, "\n{}{}", std::string(i * 2, ' '),
                       _operators[i]->debug_string());
        std::stringstream profile_ss;
        _operators[i]->get_runtime_profile()->pretty_print(&profile_ss, std::string(i * 2, ' '));
        fmt::format_to(debug_string_buffer, "\n{}", profile_ss.str());
    }
    fmt::format_to(debug_string_buffer, "\n{}{}", std::string(_operators.size() * 2, ' '),
                   _sink->debug_string());
    {
        std::stringstream profile_ss;
        _sink->get_runtime_profile()->pretty_print(&profile_ss,
                                                   std::string(_operators.size() * 2, ' '));
        fmt::format_to(debug_string_buffer, "\n{}", profile_ss.str());
    }
    return fmt::to_string(debug_string_buffer);
}

taskgroup::TaskGroupPipelineTaskEntity* PipelineTask::get_task_group_entity() const {
    return _fragment_context->get_task_group_entity();
}

} // namespace doris::pipeline
