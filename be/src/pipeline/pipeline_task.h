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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/core/block.h"

namespace doris {
class QueryContext;
class RuntimeState;
namespace pipeline {
class PipelineFragmentContext;
} // namespace pipeline
} // namespace doris

namespace doris::pipeline {

class MultiCoreTaskQueue;
class PriorityTaskQueue;
class Dependency;

class PipelineTask : public std::enable_shared_from_this<PipelineTask> {
public:
    PipelineTask(PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
                 std::shared_ptr<PipelineFragmentContext> fragment_context,
                 RuntimeProfile* parent_profile,
                 std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                         std::vector<std::shared_ptr<Dependency>>>>
                         shared_state_map,
                 int task_idx);

    Status prepare(const std::vector<TScanRangeParams>& scan_range, const int sender_id,
                   const TDataSink& tsink);

    Status execute(bool* done);

    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    Status close(Status exec_status, bool close_sink = true);

    std::weak_ptr<PipelineFragmentContext>& fragment_context() { return _fragment_context; }

    int get_thread_id(int num_threads) const {
        return _thread_id == -1 ? _thread_id : _thread_id % num_threads;
    }

    PipelineTask& set_thread_id(int thread_id) {
        _thread_id = thread_id;
        COUNTER_UPDATE(_core_change_times, 1);
        return *this;
    }

    Status finalize();

    std::string debug_string();

    std::shared_ptr<BasicSharedState> get_source_shared_state() {
        return _op_shared_states.contains(_source->operator_id())
                       ? _op_shared_states[_source->operator_id()]
                       : nullptr;
    }

    /**
     * Pipeline task is blockable means it will be blocked in the next run. So we should put it into
     * the blocking task scheduler.
     */
    bool is_blockable() const;

    /**
     * `shared_state` is shared by different pipeline tasks. This function aims to establish
     * connections across related tasks.
     *
     * There are 2 kinds of relationships to share state by tasks.
     * 1. For regular operators, for example, Aggregation, we use the AggSinkOperator to create a
     *    shared state and then inject it into downstream task which contains the corresponding
     *    AggSourceOperator.
     * 2. For multiple-sink-single-source operator, for example, Set operations, the shared state is
     *    created once and shared by multiple sink operators and single source operator. For this
     *    case, we use the first sink operator create shared state and then inject into all of other
     *    tasks.
     */
    bool inject_shared_state(std::shared_ptr<BasicSharedState> shared_state);

    std::shared_ptr<BasicSharedState> get_sink_shared_state() { return _sink_shared_state; }

    BasicSharedState* get_op_shared_state(int id) {
        if (!_op_shared_states.contains(id)) {
            return nullptr;
        }
        return _op_shared_states[id].get();
    }

    Status wake_up(Dependency* dep);

    DataSinkOperatorPtr sink() const { return _sink; }

    int task_id() const { return _index; };
    bool is_finalized() const { return _exec_state == State::FINALIZED; }

    void set_wake_up_early() { _wake_up_early = true; }

    // Execution phase should be terminated. This is called if this task is canceled or waken up early.
    void terminate();

    // 1 used for update priority queue
    // note(wb) an ugly implementation, need refactor later
    // 1.1 pipeline task
    void inc_runtime_ns(uint64_t delta_time) { this->_runtime += delta_time; }
    uint64_t get_runtime_ns() const { return this->_runtime; }

    // 1.2 priority queue's queue level
    void update_queue_level(int queue_level) { this->_queue_level = queue_level; }
    int get_queue_level() const { return this->_queue_level; }

    void put_in_runnable_queue() {
        _schedule_time++;
        _wait_worker_watcher.start();
    }

    void pop_out_runnable_queue() { _wait_worker_watcher.stop(); }

    bool is_running() { return _running.load(); }
    PipelineTask& set_running(bool running) {
        _running.exchange(running);
        return *this;
    }

    RuntimeState* runtime_state() const { return _state; }

    std::string task_name() const { return fmt::format("task{}({})", _index, _pipeline->_name); }

    // TODO: Maybe we do not need this safe code anymore
    void stop_if_finished();

    PipelineId pipeline_id() const { return _pipeline->id(); }
    [[nodiscard]] size_t get_revocable_size() const;
    [[nodiscard]] Status revoke_memory(const std::shared_ptr<SpillContext>& spill_context);

    Status blocked(Dependency* dependency) {
        DCHECK_EQ(_blocked_dep, nullptr) << "task: " << debug_string();
        _blocked_dep = dependency;
        return _state_transition(PipelineTask::State::BLOCKED);
    }

private:
    // Whether this task is blocked before execution (FE 2-phase commit trigger, runtime filters)
    bool _wait_to_start();
    // Whether this task is blocked during execution (read dependency, write dependency)
    bool _is_blocked();
    // Whether this task is blocked after execution (pending finish dependency)
    bool _is_pending_finish();

    Status _extract_dependencies();
    void _init_profile();
    void _fresh_profile_counter();
    Status _open();
    Status _prepare();

    // Operator `op` try to reserve memory before executing. Return false if reserve failed
    // otherwise return true.
    bool _try_to_reserve_memory(const size_t reserve_size, OperatorBase* op);

    const TUniqueId _query_id;
    const uint32_t _index;
    PipelinePtr _pipeline;
    bool _opened;
    RuntimeState* _state = nullptr;
    int _thread_id = -1;
    uint32_t _schedule_time = 0;
    std::unique_ptr<vectorized::Block> _block;

    std::weak_ptr<PipelineFragmentContext> _fragment_context;

    // used for priority queue
    // it may be visited by different thread but there is no race condition
    // so no need to add lock
    uint64_t _runtime = 0;
    // it's visited in one thread, so no need to thread synchronization
    // 1 get task, (set _queue_level/_core_id)
    // 2 exe task
    // 3 update task statistics(update _queue_level/_core_id)
    int _queue_level = 0;

    bool _need_to_revoke_memory = false;
    std::shared_ptr<SpillContext> _spill_context;

    RuntimeProfile* _parent_profile = nullptr;
    std::unique_ptr<RuntimeProfile> _task_profile;
    RuntimeProfile::Counter* _task_cpu_timer = nullptr;
    RuntimeProfile::Counter* _prepare_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::Counter* _get_block_timer = nullptr;
    RuntimeProfile::Counter* _get_block_counter = nullptr;
    RuntimeProfile::Counter* _sink_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _schedule_counts = nullptr;
    MonotonicStopWatch _wait_worker_watcher;
    RuntimeProfile::Counter* _wait_worker_timer = nullptr;
    // TODO we should calculate the time between when really runnable and runnable
    RuntimeProfile::Counter* _yield_counts = nullptr;
    RuntimeProfile::Counter* _core_change_times = nullptr;
    RuntimeProfile::Counter* _memory_reserve_times = nullptr;
    RuntimeProfile::Counter* _memory_reserve_failed_times = nullptr;

    Operators _operators; // left is _source, right is _root
    OperatorXBase* _source;
    OperatorXBase* _root;
    DataSinkOperatorPtr _sink;

    // `_read_dependencies` is stored as same order as `_operators`
    std::vector<std::vector<Dependency*>> _read_dependencies;
    std::vector<Dependency*> _write_dependencies;
    std::vector<Dependency*> _finish_dependencies;
    std::vector<Dependency*> _execution_dependencies;

    // All shared states of this pipeline task.
    std::map<int, std::shared_ptr<BasicSharedState>> _op_shared_states;
    std::shared_ptr<BasicSharedState> _sink_shared_state;
    std::vector<TScanRangeParams> _scan_ranges;
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            _shared_state_map;
    int _task_idx;
    bool _dry_run = false;
    MOCK_REMOVE(const)
    unsigned long long _exec_time_slice = config::pipeline_task_exec_time_slice * NANOS_PER_MILLIS;
    Dependency* _blocked_dep = nullptr;

    Dependency* _memory_sufficient_dependency;
    std::mutex _dependency_lock;

    std::atomic<bool> _running {false};
    std::atomic<bool> _eos {false};
    std::atomic<bool> _wake_up_early {false};

    /**
         *
         * INITED -----> RUNNABLE -------------------------+----> FINISHED ---+---> FINALIZED
         *                   ^                             |                  |
         *                   |                             |                  |
         *                   +----------- BLOCKED <--------+------------------+
         */
    enum class State : int {
        INITED,
        RUNNABLE,
        BLOCKED,
        FINISHED,
        FINALIZED,
    };
    const std::vector<std::set<State>> LEGAL_STATE_TRANSITION = {
            {},                                               // Target state is INITED
            {State::INITED, State::RUNNABLE, State::BLOCKED}, // Target state is RUNNABLE
            {State::RUNNABLE, State::FINISHED},               // Target state is BLOCKED
            {State::RUNNABLE},                                // Target state is FINISHED
            {State::INITED, State::FINISHED}};                // Target state is FINALIZED

    std::string _to_string(State state) const {
        switch (state) {
        case State::INITED:
            return "INITED";
        case State::RUNNABLE:
            return "RUNNABLE";
        case State::BLOCKED:
            return "BLOCKED";
        case State::FINISHED:
            return "FINISHED";
        case State::FINALIZED:
            return "FINALIZED";
        default:
            __builtin_unreachable();
        }
    }

    Status _state_transition(State new_state);
    std::atomic<State> _exec_state = State::INITED;
    MonotonicStopWatch _state_change_watcher;
    std::atomic<bool> _spilling = false;
    const std::string _pipeline_name;
};

using PipelineTaskSPtr = std::shared_ptr<PipelineTask>;

} // namespace doris::pipeline
