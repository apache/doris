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
        PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile,
        std::map<int,
                 std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
                le_state_map,
        int task_idx)
        : _index(task_id),
          _pipeline(pipeline),
          _prepared(false),
          _opened(false),
          _state(state),
          _fragment_context(fragment_context),
          _parent_profile(parent_profile),
          _operators(pipeline->operator_xs()),
          _source(_operators.front()),
          _root(_operators.back()),
          _sink(pipeline->sink_shared_pointer()),
          _le_state_map(std::move(le_state_map)),
          _task_idx(task_idx),
          _execution_dep(state->get_query_ctx()->get_execution_dependency()) {
    _pipeline_task_watcher.start();

    auto shared_state = _sink->create_shared_state();
    if (shared_state) {
        _sink_shared_state = shared_state;
    }
    pipeline->incr_created_tasks();
}

Status PipelineTask::prepare(const TPipelineInstanceParams& local_params, const TDataSink& tsink,
                             QueryContext* query_ctx) {
    DCHECK(_sink);
    _init_profile();
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_prepare_timer);

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
        const auto& deps = _state->get_local_state(_source->operator_id())->filter_dependencies();
        std::copy(deps.begin(), deps.end(),
                  std::inserter(_filter_dependencies, _filter_dependencies.end()));
    }
    _prepared = true;
    return Status::OK();
}

Status PipelineTask::_extract_dependencies() {
    for (auto& op : _operators) {
        auto result = _state->get_local_state_result(op->operator_id());
        if (!result) {
            return result.error();
        }
        auto* local_state = result.value();
        const auto& deps = local_state->dependencies();
        std::copy(deps.begin(), deps.end(),
                  std::inserter(_read_dependencies, _read_dependencies.end()));
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            _finish_dependencies.push_back(fin_dep);
        }
    }
    {
        auto* local_state = _state->get_sink_local_state();
        _write_dependencies = local_state->dependencies();
        DCHECK(std::all_of(_write_dependencies.begin(), _write_dependencies.end(),
                           [](auto* dep) { return dep->is_write_dependency(); }));
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            _finish_dependencies.push_back(fin_dep);
        }
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

    _block_counts = ADD_COUNTER(_task_profile, "NumBlockedTimes", TUnit::UNIT);
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
        auto st = local_state->open(_state);
        DCHECK(st.is<ErrorCode::PIP_WAIT_FOR_RF>() ? !_filter_dependencies.empty() : true)
                << debug_string();
        RETURN_IF_ERROR(st);
    }
    RETURN_IF_ERROR(_state->get_sink_local_state()->open(_state));
    RETURN_IF_ERROR(_extract_dependencies());
    _block = doris::vectorized::Block::create_unique();
    _opened = true;
    return Status::OK();
}

void PipelineTask::set_task_queue(TaskQueue* task_queue) {
    _task_queue = task_queue;
}

Status PipelineTask::execute(bool* eos) {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_TIMER(_exec_timer);
    SCOPED_ATTACH_TASK(_state);
    *eos = _eos;
    if (_eos) {
        // If task is waken up by finish dependency, `_eos` is set to true by last execution, and we should return here.
        return Status::OK();
    }
    int64_t time_spent = 0;

    ThreadCpuStopWatch cpu_time_stop_watch;
    cpu_time_stop_watch.start();
    Defer defer {[&]() {
        if (_task_queue) {
            _task_queue->update_statistics(this, time_spent);
        }
        int64_t delta_cpu_time = cpu_time_stop_watch.elapsed_time();
        _task_cpu_timer->update(delta_cpu_time);
        auto cpu_qs = query_context()->get_cpu_statistics();
        if (cpu_qs) {
            cpu_qs->add_cpu_nanos(delta_cpu_time);
        }
    }};
    if (has_dependency() || _runtime_filter_blocked_dependency() != nullptr) {
        return Status::OK();
    }
    // The status must be runnable
    if (!_opened) {
        {
            RETURN_IF_ERROR(_open());
        }
        if (!source_can_read() || !sink_can_write()) {
            return Status::OK();
        }
    }

    while (!_fragment_context->is_canceled()) {
        if ((_root->need_data_from_children(_state) && !source_can_read()) || !sink_can_write()) {
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

        auto sink_revocable_mem_size = _sink->revocable_mem_size(_state);
        if (should_revoke_memory(_state, sink_revocable_mem_size)) {
            RETURN_IF_ERROR(_sink->revoke_memory(_state));
            continue;
        }

        *eos = _eos;
        // Pull block from operator chain
        if (!_dry_run) {
            SCOPED_TIMER(_get_block_timer);
            _get_block_counter->update(1);
            try {
                RETURN_IF_ERROR(_root->get_block_after_projects(_state, block, eos));
            } catch (const Exception& e) {
                return Status::InternalError(e.to_string() +
                                             " task debug string: " + debug_string());
            }
        } else {
            *eos = true;
            _eos = true;
        }

        if (_block->rows() != 0 || *eos) {
            SCOPED_TIMER(_sink_timer);
            Status status = Status::OK();
            status = _sink->sink(_state, block, *eos);
            if (!status.is<ErrorCode::END_OF_FILE>()) {
                RETURN_IF_ERROR(status);
            }
            *eos = status.is<ErrorCode::END_OF_FILE>() ? true : *eos;
            if (*eos) { // just return, the scheduler will do finish work
                _eos = true;
                return Status::OK();
            }
        }
    }

    static_cast<void>(get_task_queue()->push_back(this));
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
            LOG_EVERY_N(INFO, 10) << "revoke memory, hight water mark";
            return true;
        }
        return false;
    } else if (is_wg_mem_low_water_mark) {
        int64_t query_weighted_limit = 0;
        int64_t query_weighted_consumption = 0;
        query_ctx->get_weighted_mem_info(query_weighted_limit, query_weighted_consumption);
        if (query_weighted_consumption < query_weighted_limit) {
            return false;
        }
        auto big_memory_operator_num = query_ctx->get_running_big_mem_op_num();
        DCHECK(big_memory_operator_num >= 0);
        int64_t mem_limit_of_op;
        if (0 == big_memory_operator_num) {
            mem_limit_of_op = int64_t(query_weighted_limit * 0.8);
        } else {
            mem_limit_of_op = query_weighted_limit / big_memory_operator_num;
        }

        LOG_EVERY_N(INFO, 10) << "revoke memory, low water mark, revocable_mem_bytes: "
                              << PrettyPrinter::print_bytes(revocable_mem_bytes)
                              << ", mem_limit_of_op: "
                              << PrettyPrinter::print_bytes(mem_limit_of_op)
                              << ", min_revocable_mem_bytes: "
                              << PrettyPrinter::print_bytes(min_revocable_mem_bytes);
        return (revocable_mem_bytes > mem_limit_of_op ||
                revocable_mem_bytes > min_revocable_mem_bytes);
    } else {
        return false;
    }
}

void PipelineTask::finalize() {
    std::unique_lock<std::mutex> lc(_release_lock);
    _finished = true;
    _sink_shared_state.reset();
    _op_shared_states.clear();
    _le_state_map.clear();
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

Status PipelineTask::close_sink(Status exec_status) {
    return _sink->close(_state, exec_status);
}

std::string PipelineTask::debug_string() {
    std::unique_lock<std::mutex> lc(_release_lock);
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "QueryId: {}\n", print_id(query_context()->query_id()));
    fmt::format_to(debug_string_buffer, "InstanceId: {}\n",
                   print_id(_state->fragment_instance_id()));

    auto* cur_blocked_dep = _blocked_dep;
    auto elapsed = _fragment_context->elapsed_time() / 1000000000.0;
    fmt::format_to(debug_string_buffer,
                   "PipelineTask[this = {}, dry run = {}, elapse time "
                   "= {}s], block dependency = {}, is running = {}\noperators: ",
                   (void*)this, _dry_run, elapsed,
                   cur_blocked_dep && !_finished ? cur_blocked_dep->debug_string() : "NULL",
                   is_running());
    for (size_t i = 0; i < _operators.size(); i++) {
        fmt::format_to(debug_string_buffer, "\n{}",
                       _opened && !_finished ? _operators[i]->debug_string(_state, i)
                                             : _operators[i]->debug_string(i));
    }
    fmt::format_to(debug_string_buffer, "\n{}",
                   _opened && !_finished ? _sink->debug_string(_state, _operators.size())
                                         : _sink->debug_string(_operators.size()));
    if (_finished) {
        return fmt::to_string(debug_string_buffer);
    }

    size_t i = 0;
    for (; i < _read_dependencies.size(); i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _read_dependencies[i]->debug_string(i + 1));
    }

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

void PipelineTask::wake_up() {
    // call by dependency
    static_cast<void>(get_task_queue()->push_back(this));
}

QueryContext* PipelineTask::query_context() {
    return _fragment_context->get_query_ctx();
}
} // namespace doris::pipeline
