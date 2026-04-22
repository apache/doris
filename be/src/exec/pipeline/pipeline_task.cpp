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

#include "exec/pipeline/pipeline_task.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/exchange_source_operator.h"
#include "exec/operator/operator.h"
#include "exec/operator/rec_cte_source_operator.h"
#include "exec/operator/scan_operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/pipeline/revokable_task.h"
#include "exec/pipeline/task_queue.h"
#include "exec/pipeline/task_scheduler.h"
#include "exec/spill/spill_file.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/defer_op.h"
#include "util/mem_info.h"
#include "util/uid_util.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris {

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
#ifndef BE_TEST
    _query_mem_tracker = fragment_context->get_query_ctx()->query_mem_tracker();
#endif
    _execution_dependencies.push_back(state->get_query_ctx()->get_execution_dependency());
    if (!_shared_state_map.contains(_sink->dests_id().front())) {
        auto shared_state = _sink->create_shared_state();
        if (shared_state) {
            _sink_shared_state = shared_state;
        }
    }
}

PipelineTask::~PipelineTask() {
    auto reset_member = [&]() {
        _shared_state_map.clear();
        _sink_shared_state.reset();
        _op_shared_states.clear();
        _sink.reset();
        _operators.clear();
        _block.reset();
        _pipeline.reset();
    };
// PipelineTask is also hold by task queue( https://github.com/apache/doris/pull/49753),
// so that it maybe the last one to be destructed.
// But pipeline task hold some objects, like operators, shared state, etc. So that should release
// memory manually.
#ifndef BE_TEST
    if (_query_mem_tracker) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_mem_tracker);
        reset_member();
        return;
    }
#endif
    reset_member();
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
            unblock_all_dependencies();
            return fragment->get_query_ctx()->exec_status();
        }
    } else {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }
    _block = doris::Block::create_unique();
    return _state_transition(State::RUNNABLE);
}

Status PipelineTask::_extract_dependencies() {
    std::vector<std::vector<Dependency*>> read_dependencies;
    std::vector<Dependency*> write_dependencies;
    std::vector<Dependency*> finish_dependencies;
    read_dependencies.resize(_operators.size());
    size_t i = 0;
    for (auto& op : _operators) {
        auto* local_state = _state->get_local_state(op->operator_id());
        DCHECK(local_state);
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
    _task_cpu_timer = ADD_TIMER(_task_profile, profile::TASK_CPU_TIME);

    static const char* exec_time = profile::EXECUTE_TIME;
    _exec_timer = ADD_TIMER(_task_profile, exec_time);
    _prepare_timer = ADD_CHILD_TIMER(_task_profile, profile::PREPARE_TIME, exec_time);
    _open_timer = ADD_CHILD_TIMER(_task_profile, profile::OPEN_TIME, exec_time);
    _get_block_timer = ADD_CHILD_TIMER(_task_profile, profile::GET_BLOCK_TIME, exec_time);
    _get_block_counter = ADD_COUNTER(_task_profile, profile::GET_BLOCK_COUNTER, TUnit::UNIT);
    _sink_timer = ADD_CHILD_TIMER(_task_profile, profile::SINK_TIME, exec_time);
    _close_timer = ADD_CHILD_TIMER(_task_profile, profile::CLOSE_TIME, exec_time);

    _wait_worker_timer = ADD_TIMER_WITH_LEVEL(_task_profile, profile::WAIT_WORKER_TIME, 1);

    _schedule_counts = ADD_COUNTER(_task_profile, profile::NUM_SCHEDULE_TIMES, TUnit::UNIT);
    _yield_counts = ADD_COUNTER(_task_profile, profile::NUM_YIELD_TIMES, TUnit::UNIT);
    _core_change_times = ADD_COUNTER(_task_profile, profile::CORE_CHANGE_TIMES, TUnit::UNIT);
    _memory_reserve_times = ADD_COUNTER(_task_profile, profile::MEMORY_RESERVE_TIMES, TUnit::UNIT);
    _memory_reserve_failed_times =
            ADD_COUNTER(_task_profile, profile::MEMORY_RESERVE_FAILED_TIMES, TUnit::UNIT);
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
    return std::ranges::any_of(_finish_dependencies, [&](Dependency* dep) -> bool {
        return dep->is_blocked_by(shared_from_this());
    });
}

bool PipelineTask::is_blockable() const {
    // Before task starting, we should make sure
    // 1. Execution dependency is ready (which is controlled by FE 2-phase commit)
    // 2. Runtime filter dependencies are ready
    // 3. All tablets are loaded into local storage

    if (_state->enable_fuzzy_blockable_task()) {
        if ((_schedule_time + _task_idx) % 2 == 0) {
            return true;
        }
    }

    return std::ranges::any_of(_operators,
                               [&](OperatorPtr op) -> bool { return op->is_blockable(_state); }) ||
           _sink->is_blockable(_state);
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
    return _memory_sufficient_dependency->is_blocked_by(shared_from_this()) ||
           std::ranges::any_of(_write_dependencies, [&](Dependency* dep) -> bool {
               return dep->is_blocked_by(shared_from_this());
           });
}

void PipelineTask::unblock_all_dependencies() {
    // We use a lock to assure all dependencies are not deconstructed here.
    std::unique_lock<std::mutex> lc(_dependency_lock);
    auto fragment = _fragment_context.lock();
    if (!is_finalized() && fragment) {
        try {
            DCHECK(_wake_up_early || fragment->is_canceled());
            std::ranges::for_each(_write_dependencies,
                                  [&](Dependency* dep) { dep->set_always_ready(); });
            std::ranges::for_each(_finish_dependencies,
                                  [&](Dependency* dep) { dep->set_always_ready(); });
            std::ranges::for_each(_read_dependencies, [&](std::vector<Dependency*>& deps) {
                std::ranges::for_each(deps, [&](Dependency* dep) { dep->set_always_ready(); });
            });
            // All `_execution_deps` will never be set blocking from ready. So we just set ready here.
            std::ranges::for_each(_execution_dependencies,
                                  [&](Dependency* dep) { dep->set_ready(); });
            _memory_sufficient_dependency->set_ready();
        } catch (const doris::Exception& e) {
            LOG(WARNING) << "unblock_all_dependencies failed: " << e.code() << ", "
                         << e.to_string();
        }
    }
}

// When current memory pressure is low, memory usage may increase significantly in the next
// operator run, while there is no revocable memory available for spilling.
// Trigger memory revoking when pressure is high and revocable memory is significant.
// Memory pressure is evaluated using two signals:
// 1. Query memory usage exceeds a threshold ratio of the query memory limit.
// 2. Workload group memory usage reaches the workload group low-watermark threshold.
bool PipelineTask::_should_trigger_revoking(const size_t reserve_size) const {
    if (!_state->enable_spill()) {
        return false;
    }

    auto query_mem_tracker = _state->get_query_ctx()->query_mem_tracker();
    auto wg = _state->get_query_ctx()->workload_group();
    if (!query_mem_tracker || !wg) {
        return false;
    }

    const auto parallelism = std::max(1, _pipeline->num_tasks());
    const auto query_water_mark = 90; // 90%
    const auto group_mem_limit = wg->memory_limit();
    auto query_limit = query_mem_tracker->limit();
    if (query_limit <= 0) {
        query_limit = group_mem_limit;
    } else if (query_limit > group_mem_limit && group_mem_limit > 0) {
        query_limit = group_mem_limit;
    }

    if (query_limit <= 0) {
        return false;
    }

    if ((reserve_size * parallelism) <= (query_limit / 5)) {
        return false;
    }

    bool is_high_memory_pressure = false;
    const auto used_mem = query_mem_tracker->consumption() + reserve_size * parallelism;
    if (used_mem >= int64_t((double(query_limit) * query_water_mark / 100))) {
        is_high_memory_pressure = true;
    }

    if (!is_high_memory_pressure) {
        bool is_low_watermark;
        bool is_high_watermark;
        wg->check_mem_used(&is_low_watermark, &is_high_watermark);
        is_high_memory_pressure = is_low_watermark || is_high_watermark;
    }

    if (is_high_memory_pressure) {
        const auto revocable_size = [&]() {
            size_t total = _sink->revocable_mem_size(_state);
            for (const auto& op : _operators) {
                total += op->revocable_mem_size(_state);
            }
            return total;
        }();

        const auto total_estimated_revocable = revocable_size * parallelism;
        return total_estimated_revocable >= int64_t(double(query_limit) * 0.2);
    }

    return false;
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
#ifdef BE_TEST
        return Status::InternalError("Pipeline task is not runnable! Task info: {}",
                                     debug_string());
#else
        return Status::FatalError("Pipeline task is not runnable! Task info: {}", debug_string());
#endif
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
        int64_t delta_cpu_time = cpu_time_stop_watch.elapsed_time();
        _task_cpu_timer->update(delta_cpu_time);
        fragment_context->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(
                delta_cpu_time);

        // If task is woke up early, we should terminate all operators, and this task could be closed immediately.
        if (_wake_up_early) {
            _eos = true;
            *done = true;
        } else if (_eos && !_spilling &&
                   (fragment_context->is_canceled() || !_is_pending_finish())) {
            // Debug point for testing the race condition fix: inject set_wake_up_early() +
            // unblock_all_dependencies() here to simulate Thread B writing A then B between
            // Thread A's two reads of _wake_up_early.
            DBUG_EXECUTE_IF("PipelineTask::execute.wake_up_early_in_else_if", {
                set_wake_up_early();
                unblock_all_dependencies();
            });
            *done = true;
        }

        // NOTE: The operator terminate() call is intentionally placed AFTER the
        // _is_pending_finish() check above, not before. This ordering is critical to avoid a race
        // condition with the seq_cst memory ordering guarantee:
        //
        // Pipeline::make_all_runnable() writes in this order:
        //   (A) set_wake_up_early()  ->  (B) unblock_all_dependencies() [sets finish_dep._always_ready]
        //
        // If we checked _wake_up_early (A) before _is_pending_finish() (B), there would be a
        // window where Thread A reads _wake_up_early=false, then Thread B writes both A and B,
        // then Thread A reads _is_pending_finish()=false (due to _always_ready). Thread A would
        // then set *done=true without ever calling operator terminate(), causing close() to run
        // on operators that were never properly terminated (e.g. RuntimeFilterProducer still in
        // WAITING_FOR_SYNCED_SIZE state when insert() is called).
        //
        // By reading _is_pending_finish() (B) before the second read of _wake_up_early (A),
        // if Thread A observes B's effect (_always_ready=true), it is guaranteed to also observe
        // A's effect (_wake_up_early=true) on this second read, ensuring operator terminate() is
        // called. This relies on _wake_up_early and _always_ready both being std::atomic with the
        // default seq_cst ordering — do not weaken them to relaxed or acq/rel.
        if (_wake_up_early) {
            THROW_IF_ERROR(_root->terminate(_state));
            THROW_IF_ERROR(_sink->terminate(_state));
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
                for (auto& op : _operators) {
                    op->set_low_memory_mode(_state);
                }
            }
            DEFER_RELEASE_RESERVED();
            _get_block_counter->update(1);
            // Sum reserve sizes across all operators in this pipeline.
            // Each operator reports only its own requirement (non-recursive).
            size_t reserve_size = 0;
            for (auto& op : _operators) {
                reserve_size += op->get_reserve_mem_size(_state);
                op->reset_reserve_mem_size(_state);
            }
            if (workload_group &&
                _state->get_query_ctx()
                        ->resource_ctx()
                        ->task_controller()
                        ->is_enable_reserve_memory() &&
                reserve_size > 0) {
                if (_should_trigger_revoking(reserve_size)) {
                    LOG(INFO) << fmt::format(
                            "Query: {} sink: {}, node id: {}, task id: {}, reserve size: {} when "
                            "high memory pressure, try to spill",
                            print_id(_query_id), _sink->get_name(), _sink->node_id(),
                            _state->task_id(), reserve_size);
                    ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                            _state->get_query_ctx()->resource_ctx()->shared_from_this(),
                            reserve_size,
                            Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                                    "high memory pressure, try to spill"));
                    _spilling = true;
                    continue;
                }
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
            if (_state->get_query_ctx()
                        ->resource_ctx()
                        ->task_controller()
                        ->is_enable_reserve_memory() &&
                workload_group && !(_wake_up_early || _dry_run)) {
                const auto sink_reserve_size = _sink->get_reserve_mem_size(_state, _eos);

                if (sink_reserve_size > 0 && _should_trigger_revoking(sink_reserve_size)) {
                    LOG(INFO) << fmt::format(
                            "Query: {} sink: {}, node id: {}, task id: {}, reserve size: {} when "
                            "high memory pressure, try to spill",
                            print_id(_query_id), _sink->get_name(), _sink->node_id(),
                            _state->task_id(), sink_reserve_size);
                    ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                            _state->get_query_ctx()->resource_ctx()->shared_from_this(),
                            sink_reserve_size,
                            Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                                    "high memory pressure, try to spill"));
                    _spilling = true;
                    continue;
                }

                if (sink_reserve_size > 0 &&
                    !_try_to_reserve_memory(sink_reserve_size, _sink.get())) {
                    continue;
                }
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
                        unblock_all_dependencies();
                    } else if (required_pipeline_id == pipeline_id() &&
                               fragment_context->get_fragment_id() == required_fragment_id) {
                        LOG(WARNING) << "PipelineTask::execute.terminate sleep 5s";
                        sleep(5);
                    }
                }
            });
            RETURN_IF_ERROR(block->check_type_and_column());
            status = _sink->sink(_state, block, _eos);

            if (_eos) {
                if (_sink->reset_to_rerun(_state, _root)) {
                    _eos = false;
                } else {
                    RETURN_IF_ERROR(close(Status::OK(), false));
                }
            }

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

    RETURN_IF_ERROR(_state->get_query_ctx()->get_pipe_exec_scheduler()->submit(shared_from_this()));
    return Status::OK();
}

Status PipelineTask::do_revoke_memory(const std::shared_ptr<SpillContext>& spill_context) {
    auto fragment_context = _fragment_context.lock();
    if (!fragment_context) {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }

    SCOPED_ATTACH_TASK(_state);
    ThreadCpuStopWatch cpu_time_stop_watch;
    cpu_time_stop_watch.start();
    Defer running_defer {[&]() {
        int64_t delta_cpu_time = cpu_time_stop_watch.elapsed_time();
        _task_cpu_timer->update(delta_cpu_time);
        fragment_context->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(
                delta_cpu_time);

        // If task is woke up early, unblock all dependencies and terminate all operators,
        // so this task could be closed immediately.
        if (_wake_up_early) {
            unblock_all_dependencies();
            THROW_IF_ERROR(_root->terminate(_state));
            THROW_IF_ERROR(_sink->terminate(_state));
            _eos = true;
        }

        // SpillContext tracks pipeline task count, not operator count.
        // Notify completion once after all operators + sink have finished revoking.
        if (spill_context) {
            spill_context->on_task_finished();
        }
    }};

    // Revoke memory from every operator that has enough revocable memory,
    // then revoke from the sink.
    for (auto& op : _operators) {
        if (op->revocable_mem_size(_state) >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
            RETURN_IF_ERROR(op->revoke_memory(_state));
        }
    }

    if (_sink->revocable_mem_size(_state) >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
        RETURN_IF_ERROR(_sink->revoke_memory(_state));
    }
    return Status::OK();
}

bool PipelineTask::_try_to_reserve_memory(const size_t reserve_size, OperatorBase* op) {
    auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(reserve_size);
    // If reserve memory failed and the query is not enable spill, just disable reserve memory(this will enable
    // memory hard limit check, and will cancel the query if allocate memory failed) and let it run.
    if (!st.ok() && !_state->enable_spill()) {
        LOG(INFO) << print_id(_query_id) << " reserve memory failed due to " << st
                  << ", and it is not enable spill, disable reserve memory and let it run";
        _state->get_query_ctx()->resource_ctx()->task_controller()->disable_reserve_memory();
        return true;
    }
    COUNTER_UPDATE(_memory_reserve_times, 1);

    // Compute total revocable memory across all operators and the sink.
    size_t total_revocable_mem_size = 0;
    size_t operator_max_revocable_mem_size = 0;

    if (!st.ok() || _state->enable_force_spill()) {
        // Compute total revocable memory across all operators and the sink.
        total_revocable_mem_size = _sink->revocable_mem_size(_state);
        operator_max_revocable_mem_size = total_revocable_mem_size;
        for (auto& cur_op : _operators) {
            total_revocable_mem_size += cur_op->revocable_mem_size(_state);
            operator_max_revocable_mem_size =
                    std::max(cur_op->revocable_mem_size(_state), operator_max_revocable_mem_size);
        }
    }

    // During enable force spill, other operators like scan opeartor will also try to reserve memory and will failed
    // here, if not add this check, it will always paused and resumed again.
    if (st.ok() && _state->enable_force_spill()) {
        if (operator_max_revocable_mem_size >= _state->spill_min_revocable_mem()) {
            st = Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                    "force spill and there is an operator has memory "
                    "size {} exceeds min mem size {}",
                    PrettyPrinter::print_bytes(operator_max_revocable_mem_size),
                    PrettyPrinter::print_bytes(_state->spill_min_revocable_mem()));
        }
    }

    if (!st.ok()) {
        COUNTER_UPDATE(_memory_reserve_failed_times, 1);
        // build per-operator revocable memory info string for debugging
        std::string ops_revocable_info;
        {
            fmt::memory_buffer buf;
            for (auto& cur_op : _operators) {
                fmt::format_to(buf, "{}({})-> ", cur_op->get_name(),
                               PrettyPrinter::print_bytes(cur_op->revocable_mem_size(_state)));
            }
            if (_sink) {
                fmt::format_to(buf, "{}({}) ", _sink->get_name(),
                               PrettyPrinter::print_bytes(_sink->revocable_mem_size(_state)));
            }
            ops_revocable_info = fmt::to_string(buf);
        }

        auto debug_msg = fmt::format(
                "Query: {} , try to reserve: {}, total revocable mem size: {}, failed reason: {}",
                print_id(_query_id), PrettyPrinter::print_bytes(reserve_size),
                PrettyPrinter::print_bytes(total_revocable_mem_size), st.to_string());
        if (!ops_revocable_info.empty()) {
            debug_msg += fmt::format(", ops_revocable=[{}]", ops_revocable_info);
        }
        // PROCESS_MEMORY_EXCEEDED error msg already contains process_mem_log_str
        if (!st.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
            debug_msg +=
                    fmt::format(", debug info: {}", GlobalMemoryArbitrator::process_mem_log_str());
        }
        LOG(INFO) << debug_msg;
        ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                _state->get_query_ctx()->resource_ctx()->shared_from_this(), reserve_size, st);
        _spilling = true;
        return false;
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
            unblock_all_dependencies();
        }
    }
}

Status PipelineTask::finalize() {
    auto fragment = _fragment_context.lock();
    if (!fragment) {
        return Status::OK();
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(fragment->get_query_ctx()->query_mem_tracker());
    RETURN_IF_ERROR(_state_transition(State::FINALIZED));
    std::unique_lock<std::mutex> lc(_dependency_lock);
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
                s = std::move(tem);
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
                   "{}, _wake_up_early = {}, _wake_up_by = {}, time elapsed since last state "
                   "changing = {}s, spilling = {}, is running = {}]",
                   _index, _opened, _eos, _to_string(_exec_state), _dry_run, _wake_up_early.load(),
                   _wake_by, _state_change_watcher.elapsed_time() / NANOS_PER_SEC, _spilling,
                   is_running());
    std::unique_lock<std::mutex> lc(_dependency_lock);
    auto* cur_blocked_dep = _blocked_dep;
    auto fragment = _fragment_context.lock();
    if (is_finalized() || !fragment) {
        fmt::format_to(debug_string_buffer, " pipeline name = {}", _pipeline_name);
        return fmt::to_string(debug_string_buffer);
    }
    auto elapsed = fragment->elapsed_time() / NANOS_PER_SEC;
    fmt::format_to(debug_string_buffer, " elapse time = {}s, block dependency = [{}]\n", elapsed,
                   cur_blocked_dep && !is_finalized() ? cur_blocked_dep->debug_string() : "NULL");

    if (_state && _state->local_runtime_filter_mgr()) {
        fmt::format_to(debug_string_buffer, "local_runtime_filter_mgr: [{}]\n",
                       _state->local_runtime_filter_mgr()->debug_string());
    }

    fmt::format_to(debug_string_buffer, "operators: ");
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

    fmt::format_to(debug_string_buffer, "Finish Dependency Information: \n");
    for (size_t j = 0; j < _finish_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _finish_dependencies[j]->debug_string(cast_set<int>(i) + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

size_t PipelineTask::get_revocable_size() const {
    if (!_opened || is_finalized() || _running || (_eos && !_spilling)) {
        return 0;
    }

    // Sum revocable memory from every operator in the pipeline + the sink.
    // Each operator reports only its own revocable memory (no child recursion).
    size_t total = _sink->revocable_mem_size(_state);
    for (const auto& op : _operators) {
        total += op->revocable_mem_size(_state);
    }
    return total;
}

Status PipelineTask::revoke_memory(const std::shared_ptr<SpillContext>& spill_context) {
    DCHECK(spill_context);
    if (is_finalized()) {
        spill_context->on_task_finished();
        VLOG_DEBUG << "Query: " << print_id(_state->query_id()) << ", task: " << ((void*)this)
                   << " finalized";
        return Status::OK();
    }

    const auto revocable_size = get_revocable_size();
    if (revocable_size >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
        auto revokable_task = std::make_shared<RevokableTask>(shared_from_this(), spill_context);
        // Submit a revocable task to run, the run method will call revoke memory. Currently the
        // underline pipeline task is still blocked.
        RETURN_IF_ERROR(_state->get_query_ctx()->get_pipe_exec_scheduler()->submit(revokable_task));
    } else {
        spill_context->on_task_finished();
        VLOG_DEBUG << "Query: " << print_id(_state->query_id()) << ", task: " << ((void*)this)
                   << " has not enough data to revoke: " << revocable_size;
    }
    return Status::OK();
}

void PipelineTask::wake_up(Dependency* dep, std::unique_lock<std::mutex>& /* dep_lock */) {
    auto cancel_if_error = [&](const Status& st) {
        if (!st.ok()) {
            if (auto frag = fragment_context().lock()) {
                frag->cancel(st);
            }
        }
    };
    // call by dependency
    DCHECK_EQ(_blocked_dep, dep) << "dep : " << dep->debug_string(0) << "task: " << debug_string();
    _blocked_dep = nullptr;
    auto holder = std::dynamic_pointer_cast<PipelineTask>(shared_from_this());
    cancel_if_error(_state_transition(PipelineTask::State::RUNNABLE));
    // Under _wake_up_early, FINISHED/FINALIZED → RUNNABLE is a legal no-op
    // (_state_transition returns OK but state stays unchanged). We must not
    // resubmit a terminated task: finalize() clears _sink/_operators, and
    // submit() → is_blockable() would dereference them → SIGSEGV.
    if (_exec_state == State::FINISHED || _exec_state == State::FINALIZED) {
        return;
    }
    if (auto f = _fragment_context.lock(); f) {
        cancel_if_error(_state->get_query_ctx()->get_pipe_exec_scheduler()->submit(holder));
    }
}

Status PipelineTask::_state_transition(State new_state) {
    const auto& table =
            _wake_up_early ? WAKE_UP_EARLY_LEGAL_STATE_TRANSITION : LEGAL_STATE_TRANSITION;
    if (!table[(int)new_state].contains(_exec_state)) {
        return Status::InternalError(
                "Task state transition from {} to {} is not allowed! Task info: {}",
                _to_string(_exec_state), _to_string(new_state), debug_string());
    }
    // FINISHED/FINALIZED → RUNNABLE is legal under wake_up_early (delayed wake_up() arriving
    // after the task already terminated), but we must not actually move the state backwards
    // or update profile info (which would misleadingly show RUNNABLE for a terminated task).
    bool need_move = !((_exec_state == State::FINISHED || _exec_state == State::FINALIZED) &&
                       new_state == State::RUNNABLE);
    if (need_move) {
        if (_exec_state != new_state) {
            _state_change_watcher.reset();
            _state_change_watcher.start();
        }
        _task_profile->add_info_string("TaskState", _to_string(new_state));
        _task_profile->add_info_string("BlockedByDependency",
                                       _blocked_dep ? _blocked_dep->name() : "");
        _exec_state = new_state;
    }
    return Status::OK();
}

} // namespace doris
