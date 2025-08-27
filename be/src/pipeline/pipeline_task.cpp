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
#include "common/compile_check_begin.h"

PipelineTask::PipelineTask(PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
                           std::shared_ptr<PipelineFragmentContext> fragment_context,
                           RuntimeProfile* parent_profile,
                           std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                                   std::vector<std::shared_ptr<Dependency>>>>
                                   shared_state_map,
                           int task_idx)
        :
#ifdef BE_TEST
          _query_id(fragment_context ? fragment_context->get_query_id() : TUniqueId()),
#else
          _query_id(fragment_context->get_query_id()),
#endif
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
          _shared_state_map(std::move(shared_state_map)),
          _task_idx(task_idx),
          _memory_sufficient_dependency(state->get_query_ctx()->get_memory_sufficient_dependency()),
          _pipeline_name(_pipeline->name()) {
    _execution_dependencies.push_back(state->get_query_ctx()->get_execution_dependency());
    if (!_shared_state_map.contains(_sink->dests_id().front())) {
        auto shared_state = _sink->create_shared_state();
        if (shared_state) {
            _sink_shared_state = shared_state;
        }
    }
}

Status PipelineTask::prepare(const std::vector<TScanRangeParams>& scan_range, const int sender_id,
                             const TDataSink& tsink) {
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
        LocalSinkStateInfo info {_task_idx,         _task_profile.get(),
                                 sender_id,         get_sink_shared_state().get(),
                                 _shared_state_map, tsink};
        RETURN_IF_ERROR(_sink->setup_local_state(_state, info));
    }

    _scan_ranges = scan_range;
    auto* parent_profile = _state->get_sink_local_state()->operator_profile();

    for (int op_idx = cast_set<int>(_operators.size() - 1); op_idx >= 0; op_idx--) {
        auto& op = _operators[op_idx];
        LocalStateInfo info {parent_profile, _scan_ranges, get_op_shared_state(op->operator_id()),
                             _shared_state_map, _task_idx};
        RETURN_IF_ERROR(op->setup_local_state(_state, info));
        parent_profile = _state->get_local_state(op->operator_id())->operator_profile();
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
            terminate();
            return fragment->get_query_ctx()->exec_status();
        }
    } else {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }
    _block = doris::vectorized::Block::create_unique();
    return _state_transition(State::RUNNABLE);
}

Status PipelineTask::_extract_dependencies() {
    std::vector<std::vector<Dependency*>> read_dependencies;
    std::vector<Dependency*> write_dependencies;
    std::vector<Dependency*> finish_dependencies;
    std::vector<Dependency*> spill_dependencies;
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
        if (auto* spill_dependency = local_state->spill_dependency()) {
            spill_dependencies.push_back(spill_dependency);
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
        if (auto* spill_dependency = local_state->spill_dependency()) {
            spill_dependencies.push_back(spill_dependency);
        }
    }
    {
        std::unique_lock<std::mutex> lc(_dependency_lock);
        read_dependencies.swap(_read_dependencies);
        write_dependencies.swap(_write_dependencies);
        finish_dependencies.swap(_finish_dependencies);
        spill_dependencies.swap(_spill_dependencies);
    }
    return Status::OK();
}

bool PipelineTask::inject_shared_state(std::shared_ptr<BasicSharedState> shared_state) {
    if (!shared_state) {
        return false;
    }
    // Shared state is created by upstream task's sink operator and shared by source operator of
    // this task.
    for (auto& op : _operators) {
        if (shared_state->related_op_ids.contains(op->operator_id())) {
            _op_shared_states.insert({op->operator_id(), shared_state});
            return true;
        }
    }
    // Shared state is created by the first sink operator and shared by sink operator of this task.
    // For example, Set operations.
    if (shared_state->related_op_ids.contains(_sink->dests_id().front())) {
        DCHECK_EQ(_sink_shared_state, nullptr)
                << " Sink: " << _sink->get_name() << " dest id: " << _sink->dests_id().front();
        _sink_shared_state = shared_state;
        return true;
    }
    return false;
}

void PipelineTask::_init_profile() {
    _task_profile = std::make_unique<RuntimeProfile>(fmt::format("PipelineTask(index={})", _index));
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
        RETURN_IF_ERROR(_state->get_local_state(o->operator_id())->open(_state));
    }
    RETURN_IF_ERROR(_state->get_sink_local_state()->open(_state));
    RETURN_IF_ERROR(_extract_dependencies());
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::open", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task open failed");
        return status;
    });
    _opened = true;
    return Status::OK();
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
    return std::any_of(
            _execution_dependencies.begin(), _execution_dependencies.end(),
            [&](Dependency* dep) -> bool { return dep->is_blocked_by(shared_from_this()); });
}

bool PipelineTask::_is_pending_finish() {
    // Spilling may be in progress if eos is true.
    return std::any_of(_spill_dependencies.begin(), _spill_dependencies.end(),
                       [&](Dependency* dep) -> bool {
                           return dep->is_blocked_by(shared_from_this());
                       }) ||
           std::any_of(
                   _finish_dependencies.begin(), _finish_dependencies.end(),
                   [&](Dependency* dep) -> bool { return dep->is_blocked_by(shared_from_this()); });
}

bool PipelineTask::is_revoking() const {
    // Spilling may be in progress if eos is true.
    return std::any_of(_spill_dependencies.begin(), _spill_dependencies.end(),
                       [&](Dependency* dep) -> bool { return dep->is_blocked_by(); });
}

bool PipelineTask::_is_blocked() {
    // `_dry_run = true` means we do not need data from source operator.
    if (!_dry_run) {
        for (int i = cast_set<int>(_read_dependencies.size() - 1); i >= 0; i--) {
            // `_read_dependencies` is organized according to operators. For each operator, running condition is met iff all dependencies are ready.
            for (auto* dep : _read_dependencies[i]) {
                if (dep->is_blocked_by(shared_from_this())) {
                    return true;
                }
            }
            // If all dependencies are ready for this operator, we can execute this task if no datum is needed from upstream operators.
            if (!_operators[i]->need_more_input_data(_state)) {
                break;
            }
        }
    }
    return std::any_of(_spill_dependencies.begin(), _spill_dependencies.end(),
                       [&](Dependency* dep) -> bool {
                           return dep->is_blocked_by(shared_from_this());
                       }) ||
           _memory_sufficient_dependency->is_blocked_by(shared_from_this()) ||
           std::any_of(
                   _write_dependencies.begin(), _write_dependencies.end(),
                   [&](Dependency* dep) -> bool { return dep->is_blocked_by(shared_from_this()); });
}

void PipelineTask::terminate() {
    // We use a lock to assure all dependencies are not deconstructed here.
    std::unique_lock<std::mutex> lc(_dependency_lock);
    auto fragment = _fragment_context.lock();
    if (!is_finalized() && fragment) {
        try {
            DCHECK(_wake_up_early || fragment->is_canceled());
            std::for_each(_spill_dependencies.begin(), _spill_dependencies.end(),
                          [&](Dependency* dep) { dep->set_always_ready(); });
            std::for_each(_write_dependencies.begin(), _write_dependencies.end(),
                          [&](Dependency* dep) { dep->set_always_ready(); });
            std::for_each(_finish_dependencies.begin(), _finish_dependencies.end(),
                          [&](Dependency* dep) { dep->set_always_ready(); });
            std::for_each(_read_dependencies.begin(), _read_dependencies.end(),
                          [&](std::vector<Dependency*>& deps) {
                              std::for_each(deps.begin(), deps.end(),
                                            [&](Dependency* dep) { dep->set_always_ready(); });
                          });
            // All `_execution_deps` will never be set blocking from ready. So we just set ready here.
            std::for_each(_execution_dependencies.begin(), _execution_dependencies.end(),
                          [&](Dependency* dep) { dep->set_ready(); });
            _memory_sufficient_dependency->set_ready();
        } catch (const doris::Exception& e) {
            LOG(WARNING) << "Terminate failed: " << e.code() << ", " << e.to_string();
        }
    }
}

/**
 * `_eos` indicates whether the execution phase is done. `done` indicates whether we could close
 * this task.
 *
 * For example,
 * 1. if `_eos` is false which means we should continue to get next block so we cannot close (e.g.
 *    `done` is false)
 * 2. if `_eos` is true which means all blocks from source are exhausted but `_is_pending_finish()`
 *    is true which means we should wait for a pending dependency ready (maybe a running rpc), so we
 *    cannot close (e.g. `done` is false)
 * 3. if `_eos` is true which means all blocks from source are exhausted and `_is_pending_finish()`
 *    is false which means we can close immediately (e.g. `done` is true)
 * @param done
 * @return
 */
Status PipelineTask::execute(bool* done) {
    if (_exec_state != State::RUNNABLE || _blocked_dep != nullptr) [[unlikely]] {
        return Status::InternalError("Pipeline task is not runnable! Task info: {}",
                                     debug_string());
    }
    auto fragment_context = _fragment_context.lock();
    if (!fragment_context) {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }
    int64_t time_spent = 0;
    ThreadCpuStopWatch cpu_time_stop_watch;
    cpu_time_stop_watch.start();
    SCOPED_ATTACH_TASK(_state);
    Defer running_defer {[&]() {
        if (_task_queue) {
            _task_queue->update_statistics(this, time_spent);
        }
        int64_t delta_cpu_time = cpu_time_stop_watch.elapsed_time();
        _task_cpu_timer->update(delta_cpu_time);
        fragment_context->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(
                delta_cpu_time);

        // If task is woke up early, we should terminate all operators, and this task could be closed immediately.
        if (_wake_up_early) {
            terminate();
            THROW_IF_ERROR(_root->terminate(_state));
            THROW_IF_ERROR(_sink->terminate(_state));
            _eos = true;
            *done = true;
        } else if (_eos && !_spilling &&
                   (fragment_context->is_canceled() || !_is_pending_finish())) {
            *done = true;
        }
    }};
    const auto query_id = _state->query_id();
    // If this task is already EOS and block is empty (which means we already output all blocks),
    // just return here.
    if (_eos && !_spilling) {
        return Status::OK();
    }
    // If this task is blocked by a spilling request and waken up immediately, the spilling
    // dependency will not block this task and we should just run here.
    if (!_block->empty()) {
        LOG(INFO) << "Query: " << print_id(query_id) << " has pending block, size: "
                  << PrettyPrinter::print_bytes(_block->allocated_bytes());
        DCHECK(_spilling);
    }

    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_TIMER(_exec_timer);

    if (!_wake_up_early) {
        RETURN_IF_ERROR(_prepare());
    }
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::execute", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task execute failed");
        return status;
    });
    // `_wake_up_early` must be after `_wait_to_start()`
    if (_wait_to_start() || _wake_up_early) {
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
        RETURN_IF_ERROR(_open());
    }

    while (!fragment_context->is_canceled()) {
        SCOPED_RAW_TIMER(&time_spent);
        Defer defer {[&]() {
            // If this run is pended by a spilling request, the block will be output in next run.
            if (!_spilling) {
                _block->clear_column_data(_root->row_desc().num_materialized_slots());
            }
        }};
        // `_wake_up_early` must be after `_is_blocked()`
        if (_is_blocked() || _wake_up_early) {
            return Status::OK();
        }

        /// When a task is cancelled,
        /// its blocking state will be cleared and it will transition to a ready state (though it is not truly ready).
        /// Here, checking whether it is cancelled to prevent tasks in a blocking state from being re-executed.
        if (fragment_context->is_canceled()) {
            break;
        }

        if (time_spent > _exec_time_slice) {
            COUNTER_UPDATE(_yield_counts, 1);
            break;
        }
        auto* block = _block.get();

        DBUG_EXECUTE_IF("fault_inject::PipelineXTask::executing", {
            Status status =
                    Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task executing failed");
            return status;
        });

        // `_sink->is_finished(_state)` means sink operator should be finished
        if (_sink->is_finished(_state)) {
            set_wake_up_early();
            return Status::OK();
        }

        // `_dry_run` means sink operator need no more data
        _eos = _dry_run || _eos;
        _spilling = false;
        auto workload_group = _state->workload_group();
        // If last run is pended by a spilling request, `_block` is produced with some rows in last
        // run, so we will resume execution using the block.
        if (!_eos && _block->empty()) {
            SCOPED_TIMER(_get_block_timer);
            if (_state->low_memory_mode()) {
                _sink->set_low_memory_mode(_state);
                _root->set_low_memory_mode(_state);
            }
            DEFER_RELEASE_RESERVED();
            _get_block_counter->update(1);
            const auto reserve_size = _root->get_reserve_mem_size(_state);
            _root->reset_reserve_mem_size(_state);

            if (workload_group &&
                _state->get_query_ctx()
                        ->resource_ctx()
                        ->task_controller()
                        ->is_enable_reserve_memory() &&
                reserve_size > 0) {
                if (!_try_to_reserve_memory(reserve_size, _root)) {
                    continue;
                }
            }

            bool eos = false;
            RETURN_IF_ERROR(_root->get_block_after_projects(_state, block, &eos));
            RETURN_IF_ERROR(block->check_type_and_column());
            _eos = eos;
        }

        if (!_block->empty() || _eos) {
            SCOPED_TIMER(_sink_timer);
            Status status = Status::OK();
            DEFER_RELEASE_RESERVED();
            COUNTER_UPDATE(_memory_reserve_times, 1);
            if (_state->get_query_ctx()
                        ->resource_ctx()
                        ->task_controller()
                        ->is_enable_reserve_memory() &&
                workload_group && !(_wake_up_early || _dry_run)) {
                const auto sink_reserve_size = _sink->get_reserve_mem_size(_state, _eos);
                if (sink_reserve_size > 0 &&
                    !_try_to_reserve_memory(sink_reserve_size, _sink.get())) {
                    continue;
                }
            }

            if (_eos) {
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

            DBUG_EXECUTE_IF("PipelineTask::execute.terminate", {
                if (_eos) {
                    auto required_pipeline_id =
                            DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                    "PipelineTask::execute.terminate", "pipeline_id", -1);
                    auto required_task_id =
                            DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                    "PipelineTask::execute.terminate", "task_id", -1);
                    auto required_fragment_id =
                            DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                    "PipelineTask::execute.terminate", "fragment_id", -1);
                    if (required_pipeline_id == pipeline_id() && required_task_id == task_id() &&
                        fragment_context->get_fragment_id() == required_fragment_id) {
                        _wake_up_early = true;
                        terminate();
                    } else if (required_pipeline_id == pipeline_id() &&
                               fragment_context->get_fragment_id() == required_fragment_id) {
                        LOG(WARNING) << "PipelineTask::execute.terminate sleep 5s";
                        sleep(5);
                    }
                }
            });
            RETURN_IF_ERROR(block->check_type_and_column());
            status = _sink->sink(_state, block, _eos);

            if (status.is<ErrorCode::END_OF_FILE>()) {
                set_wake_up_early();
                return Status::OK();
            } else if (!status) {
                return status;
            }

            if (_eos) { // just return, the scheduler will do finish work
                return Status::OK();
            }
        }
    }

    RETURN_IF_ERROR(get_task_queue()->push_back(shared_from_this()));
    return Status::OK();
}

bool PipelineTask::_try_to_reserve_memory(const size_t reserve_size, OperatorBase* op) {
    auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(reserve_size);
    COUNTER_UPDATE(_memory_reserve_times, 1);
    auto sink_revocable_mem_size = _sink->revocable_mem_size(_state);
    if (st.ok() && _state->enable_force_spill() && _sink->is_spillable() &&
        sink_revocable_mem_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
        st = Status(ErrorCode::QUERY_MEMORY_EXCEEDED, "Force Spill");
    }
    if (!st.ok()) {
        COUNTER_UPDATE(_memory_reserve_failed_times, 1);
        auto debug_msg = fmt::format(
                "Query: {} , try to reserve: {}, operator name: {}, operator "
                "id: {}, task id: {}, root revocable mem size: {}, sink revocable mem"
                "size: {}, failed: {}",
                print_id(_query_id), PrettyPrinter::print_bytes(reserve_size), op->get_name(),
                op->node_id(), _state->task_id(),
                PrettyPrinter::print_bytes(op->revocable_mem_size(_state)),
                PrettyPrinter::print_bytes(sink_revocable_mem_size), st.to_string());
        // PROCESS_MEMORY_EXCEEDED error msg alread contains process_mem_log_str
        if (!st.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
            debug_msg +=
                    fmt::format(", debug info: {}", GlobalMemoryArbitrator::process_mem_log_str());
        }
        LOG_EVERY_N(INFO, 100) << debug_msg;
        // If sink has enough revocable memory, trigger revoke memory
        if (sink_revocable_mem_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            LOG(INFO) << fmt::format(
                    "Query: {} sink: {}, node id: {}, task id: "
                    "{}, revocable mem size: {}",
                    print_id(_query_id), _sink->get_name(), _sink->node_id(), _state->task_id(),
                    PrettyPrinter::print_bytes(sink_revocable_mem_size));
            ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                    _state->get_query_ctx()->resource_ctx()->shared_from_this(), reserve_size, st);
            _spilling = true;
            return false;
        } else {
            // If reserve failed, not add this query to paused list, because it is very small, will not
            // consume a lot of memory. But need set low memory mode to indicate that the system should
            // not use too much memory.
            _state->get_query_ctx()->set_low_memory_mode();
        }
    }
    return true;
}

void PipelineTask::stop_if_finished() {
    auto fragment = _fragment_context.lock();
    if (!fragment) {
        return;
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(fragment->get_query_ctx()->query_mem_tracker());
    if (auto sink = _sink) {
        if (sink->is_finished(_state)) {
            set_wake_up_early();
            terminate();
        }
    }
}

Status PipelineTask::finalize() {
    auto fragment = _fragment_context.lock();
    if (!fragment) {
        return Status::OK();
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(fragment->get_query_ctx()->query_mem_tracker());
    std::unique_lock<std::mutex> lc(_dependency_lock);
    RETURN_IF_ERROR(_state_transition(State::FINALIZED));
    _sink_shared_state.reset();
    _op_shared_states.clear();
    _shared_state_map.clear();
    _block.reset();
    _operators.clear();
    _sink.reset();
    _pipeline.reset();
    return Status::OK();
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
        _task_profile->add_info_string("WakeUpEarly", std::to_string(_wake_up_early.load()));
        _fresh_profile_counter();
    }

    if (_task_queue) {
        _task_queue->update_statistics(this, close_ns);
    }
    if (close_sink) {
        RETURN_IF_ERROR(_state_transition(State::FINISHED));
    }
    return s;
}

std::string PipelineTask::debug_string() {
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "QueryId: {}\n", print_id(_query_id));
    fmt::format_to(debug_string_buffer, "InstanceId: {}\n",
                   print_id(_state->fragment_instance_id()));

    fmt::format_to(debug_string_buffer,
                   "PipelineTask[id = {}, open = {}, eos = {}, state = {}, dry run = "
                   "{}, _wake_up_early = {}, time elapsed since last state changing = {}s, spilling"
                   " = {}, is running = {}]",
                   _index, _opened, _eos, _to_string(_exec_state), _dry_run, _wake_up_early.load(),
                   _state_change_watcher.elapsed_time() / NANOS_PER_SEC, _spilling, is_running());
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
                       _opened && !is_finalized()
                               ? _operators[i]->debug_string(_state, cast_set<int>(i))
                               : _operators[i]->debug_string(cast_set<int>(i)));
    }
    fmt::format_to(debug_string_buffer, "\n{}\n",
                   _opened && !is_finalized()
                           ? _sink->debug_string(_state, cast_set<int>(_operators.size()))
                           : _sink->debug_string(cast_set<int>(_operators.size())));

    fmt::format_to(debug_string_buffer, "\nRead Dependency Information: \n");

    size_t i = 0;
    for (; i < _read_dependencies.size(); i++) {
        for (size_t j = 0; j < _read_dependencies[i].size(); j++) {
            fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                           _read_dependencies[i][j]->debug_string(cast_set<int>(i) + 1));
        }
    }

    fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                   _memory_sufficient_dependency->debug_string(cast_set<int>(i++)));

    fmt::format_to(debug_string_buffer, "\nWrite Dependency Information: \n");
    for (size_t j = 0; j < _write_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _write_dependencies[j]->debug_string(cast_set<int>(j) + 1));
    }

    fmt::format_to(debug_string_buffer, "\nExecution Dependency Information: \n");
    for (size_t j = 0; j < _execution_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _execution_dependencies[j]->debug_string(cast_set<int>(i) + 1));
    }

    fmt::format_to(debug_string_buffer, "\nSpill Dependency Information: \n");
    for (size_t j = 0; j < _spill_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _spill_dependencies[j]->debug_string(cast_set<int>(i) + 1));
    }

    fmt::format_to(debug_string_buffer, "Finish Dependency Information: \n");
    for (size_t j = 0; j < _finish_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _finish_dependencies[j]->debug_string(cast_set<int>(i) + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

size_t PipelineTask::get_revocable_size() const {
    if (is_finalized() || _running || (_eos && !_spilling)) {
        return 0;
    }

    return _sink->revocable_mem_size(_state);
}

Status PipelineTask::revoke_memory(const std::shared_ptr<SpillContext>& spill_context) {
    if (is_finalized()) {
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

Status PipelineTask::wake_up(Dependency* dep) {
    // call by dependency
    DCHECK_EQ(_blocked_dep, dep) << "dep : " << dep->debug_string(0) << "task: " << debug_string();
    _blocked_dep = nullptr;
    auto holder = std::dynamic_pointer_cast<PipelineTask>(shared_from_this());
    RETURN_IF_ERROR(_state_transition(PipelineTask::State::RUNNABLE));
    RETURN_IF_ERROR(get_task_queue()->push_back(holder));
    return Status::OK();
}

Status PipelineTask::_state_transition(State new_state) {
    if (_exec_state != new_state) {
        _state_change_watcher.reset();
        _state_change_watcher.start();
    }
    _task_profile->add_info_string("TaskState", _to_string(new_state));
    _task_profile->add_info_string("BlockedByDependency", _blocked_dep ? _blocked_dep->name() : "");
    if (!LEGAL_STATE_TRANSITION[(int)new_state].contains(_exec_state)) {
        return Status::InternalError(
                "Task state transition from {} to {} is not allowed! Task info: {}",
                _to_string(_exec_state), _to_string(new_state), debug_string());
    }
    _exec_state = new_state;
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
