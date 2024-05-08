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

#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/core/block.h"
#include "vec/sink/vresult_sink.h"

namespace doris {
class QueryContext;
class RuntimeState;
namespace pipeline {
class PipelineFragmentContext;
} // namespace pipeline
} // namespace doris

namespace doris::pipeline {

/**
 * PipelineTaskState indicates all possible states of a pipeline task.
 * A FSM is described as below:
 *
 *                 |-----------------------------------------------------|
 *                 |---|                  transfer 2    transfer 3       |   transfer 4
 *                     |-------> BLOCKED ------------|                   |---------------------------------------> CANCELED
 *              |------|                             |                   | transfer 5           transfer 6|
 * NOT_READY ---| transfer 0                         |-----> RUNNABLE ---|---------> PENDING_FINISH ------|
 *              |                                    |          ^        |                      transfer 7|
 *              |------------------------------------|          |--------|---------------------------------------> FINISHED
 *                transfer 1                                   transfer 9          transfer 8
 * BLOCKED include BLOCKED_FOR_DEPENDENCY, BLOCKED_FOR_SOURCE and BLOCKED_FOR_SINK.
 *
 * transfer 0 (NOT_READY -> BLOCKED): this pipeline task has some incomplete dependencies
 * transfer 1 (NOT_READY -> RUNNABLE): this pipeline task has no incomplete dependencies
 * transfer 2 (BLOCKED -> RUNNABLE): runnable condition for this pipeline task is met (e.g. get a new block from rpc)
 * transfer 3 (RUNNABLE -> BLOCKED): runnable condition for this pipeline task is not met (e.g. sink operator send a block by RPC and wait for a response)
 * transfer 4 (RUNNABLE -> CANCELED): current fragment is cancelled
 * transfer 5 (RUNNABLE -> PENDING_FINISH): this pipeline task completed but wait for releasing resources hold by itself
 * transfer 6 (PENDING_FINISH -> CANCELED): current fragment is cancelled
 * transfer 7 (PENDING_FINISH -> FINISHED): this pipeline task completed and resources hold by itself have been released already
 * transfer 8 (RUNNABLE -> FINISHED): this pipeline task completed and no resource need to be released
 * transfer 9 (RUNNABLE -> RUNNABLE): this pipeline task yields CPU and re-enters the runnable queue if it is runnable and has occupied CPU for a max time slice
 */
enum class PipelineTaskState : uint8_t {
    NOT_READY = 0, // do not prepare
    BLOCKED_FOR_DEPENDENCY = 1,
    BLOCKED_FOR_SOURCE = 2,
    BLOCKED_FOR_SINK = 3,
    RUNNABLE = 4, // can execute
    PENDING_FINISH =
            5, // compute task is over, but still hold resource. like some scan and sink task
    FINISHED = 6,
    CANCELED = 7,
    BLOCKED_FOR_RF = 8,
};

inline const char* get_state_name(PipelineTaskState idx) {
    switch (idx) {
    case PipelineTaskState::NOT_READY:
        return "NOT_READY";
    case PipelineTaskState::BLOCKED_FOR_DEPENDENCY:
        return "BLOCKED_FOR_DEPENDENCY";
    case PipelineTaskState::BLOCKED_FOR_SOURCE:
        return "BLOCKED_FOR_SOURCE";
    case PipelineTaskState::BLOCKED_FOR_SINK:
        return "BLOCKED_FOR_SINK";
    case PipelineTaskState::RUNNABLE:
        return "RUNNABLE";
    case PipelineTaskState::PENDING_FINISH:
        return "PENDING_FINISH";
    case PipelineTaskState::FINISHED:
        return "FINISHED";
    case PipelineTaskState::CANCELED:
        return "CANCELED";
    case PipelineTaskState::BLOCKED_FOR_RF:
        return "BLOCKED_FOR_RF";
    }
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

inline bool is_final_state(PipelineTaskState idx) {
    switch (idx) {
    case PipelineTaskState::FINISHED:
    case PipelineTaskState::CANCELED:
        return true;
    default:
        return false;
    }
}

class TaskQueue;
class PriorityTaskQueue;
class Dependency;

class PipelineTask {
public:
    PipelineTask(PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
                 PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile,
                 std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                                         std::shared_ptr<Dependency>>>
                         le_state_map,
                 int task_idx);

    Status prepare(const TPipelineInstanceParams& local_params, const TDataSink& tsink,
                   QueryContext* query_ctx);

    Status execute(bool* eos);

    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    Status close(Status exec_status);

    Status close_sink(Status exec_status);
    bool source_can_read() {
        if (_dry_run) {
            return true;
        }
        return _read_blocked_dependency() == nullptr;
    }

    bool sink_can_write() { return _write_blocked_dependency() == nullptr; }

    PipelineFragmentContext* fragment_context() { return _fragment_context; }

    QueryContext* query_context();

    int get_previous_core_id() const {
        return _previous_schedule_id != -1 ? _previous_schedule_id
                                           : _pipeline->_previous_schedule_id;
    }

    void set_previous_core_id(int id) {
        if (id == _previous_schedule_id) {
            return;
        }
        if (_previous_schedule_id != -1) {
            COUNTER_UPDATE(_core_change_times, 1);
        }
        _previous_schedule_id = id;
    }

    void finalize();

    bool is_finished() const { return _finished.load(); }

    std::string debug_string();

    bool is_pending_finish() { return _finish_blocked_dependency() != nullptr; }

    std::shared_ptr<BasicSharedState> get_source_shared_state() {
        return _op_shared_states.contains(_source->operator_id())
                       ? _op_shared_states[_source->operator_id()]
                       : nullptr;
    }

    void inject_shared_state(std::shared_ptr<BasicSharedState> shared_state) {
        if (!shared_state) {
            return;
        }
        // Shared state is created by upstream task's sink operator and shared by source operator of this task.
        for (auto& op : _operators) {
            if (shared_state->related_op_ids.contains(op->operator_id())) {
                _op_shared_states.insert({op->operator_id(), shared_state});
                return;
            }
        }
        if (shared_state->related_op_ids.contains(_sink->dests_id().front())) {
            DCHECK(_sink_shared_state == nullptr);
            _sink_shared_state = shared_state;
        }
    }

    std::shared_ptr<BasicSharedState> get_sink_shared_state() { return _sink_shared_state; }

    BasicSharedState* get_op_shared_state(int id) {
        if (!_op_shared_states.contains(id)) {
            return nullptr;
        }
        return _op_shared_states[id].get();
    }

    void wake_up();

    DataSinkOperatorXPtr sink() const { return _sink; }

    OperatorXPtr source() const { return _source; }

    OperatorXs operatorXs() { return _operators; }

    int task_id() const { return _index; };

    void clear_blocking_state() {
        if (!_finished && get_state() != PipelineTaskState::PENDING_FINISH && _blocked_dep) {
            _blocked_dep->set_ready();
            _blocked_dep = nullptr;
        }
    }

    void set_task_queue(TaskQueue* task_queue);
    TaskQueue* get_task_queue() { return _task_queue; }

    static constexpr auto THREAD_TIME_SLICE = 100'000'000ULL;

    // 1 used for update priority queue
    // note(wb) an ugly implementation, need refactor later
    // 1.1 pipeline task
    void inc_runtime_ns(uint64_t delta_time) { this->_runtime += delta_time; }
    uint64_t get_runtime_ns() const { return this->_runtime; }

    // 1.2 priority queue's queue level
    void update_queue_level(int queue_level) { this->_queue_level = queue_level; }
    int get_queue_level() const { return this->_queue_level; }

    // 1.3 priority queue's core id
    void set_core_id(int core_id) { this->_core_id = core_id; }
    int get_core_id() const { return this->_core_id; }

    bool has_dependency() {
        _blocked_dep = _execution_dep->is_blocked_by(this);
        if (_blocked_dep != nullptr) {
            static_cast<Dependency*>(_blocked_dep)->start_watcher();
            return true;
        }
        return false;
    }

    static bool should_revoke_memory(RuntimeState* state, int64_t revocable_mem_bytes);

    void put_in_runnable_queue() {
        _schedule_time++;
        _wait_worker_watcher.start();
    }

    void pop_out_runnable_queue() { _wait_worker_watcher.stop(); }
    PipelineTaskState get_state() const { return _cur_state; }
    void set_state(PipelineTaskState state);
    TUniqueId instance_id() const { return _state->fragment_instance_id(); }

    bool is_running() { return _running.load(); }
    void set_running(bool running) { _running = running; }

    bool is_exceed_debug_timeout() {
        if (_has_exceed_timeout) {
            return true;
        }
        // If enable_debug_log_timeout_secs <= 0, then disable the log
        if (_pipeline_task_watcher.elapsed_time() >
            config::enable_debug_log_timeout_secs * 1000L * 1000L * 1000L) {
            _has_exceed_timeout = true;
            return true;
        }
        return false;
    }

    void log_detail_if_need() {
        if (config::enable_debug_log_timeout_secs < 1) {
            return;
        }
        if (is_exceed_debug_timeout()) {
            LOG(INFO) << "query id|instanceid " << print_id(_state->query_id()) << "|"
                      << print_id(_state->fragment_instance_id())
                      << " current pipeline exceed run time "
                      << config::enable_debug_log_timeout_secs << " seconds. Task state "
                      << get_state_name(get_state()) << "/n task detail:" << debug_string();
        }
    }

    RuntimeState* runtime_state() const { return _state; }

    std::string task_name() const { return fmt::format("task{}({})", _index, _pipeline->_name); }

private:
    friend class RuntimeFilterDependency;
    Dependency* _write_blocked_dependency() {
        for (auto* op_dep : _write_dependencies) {
            _blocked_dep = op_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Dependency* _finish_blocked_dependency() {
        for (auto* fin_dep : _finish_dependencies) {
            _blocked_dep = fin_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Dependency* _read_blocked_dependency() {
        for (auto* op_dep : _read_dependencies) {
            _blocked_dep = op_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Dependency* _runtime_filter_blocked_dependency() {
        for (auto* op_dep : _filter_dependencies) {
            _blocked_dep = op_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Status _extract_dependencies();
    void _init_profile();
    void _fresh_profile_counter();
    Status _open();

    uint32_t _index;
    PipelinePtr _pipeline;
    bool _dependency_finish = false;
    bool _has_exceed_timeout = false;
    bool _prepared;
    bool _opened;
    RuntimeState* _state = nullptr;
    int _previous_schedule_id = -1;
    uint32_t _schedule_time = 0;
    PipelineTaskState _cur_state;
    std::unique_ptr<doris::vectorized::Block> _block;
    PipelineFragmentContext* _fragment_context = nullptr;
    TaskQueue* _task_queue = nullptr;

    // used for priority queue
    // it may be visited by different thread but there is no race condition
    // so no need to add lock
    uint64_t _runtime = 0;
    // it's visited in one thread, so no need to thread synchronization
    // 1 get task, (set _queue_level/_core_id)
    // 2 exe task
    // 3 update task statistics(update _queue_level/_core_id)
    int _queue_level = 0;
    int _core_id = 0;
    Status _open_status = Status::OK();

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
    RuntimeProfile::Counter* _block_counts = nullptr;
    RuntimeProfile::Counter* _block_by_source_counts = nullptr;
    RuntimeProfile::Counter* _block_by_sink_counts = nullptr;
    RuntimeProfile::Counter* _schedule_counts = nullptr;
    MonotonicStopWatch _wait_source_watcher;
    MonotonicStopWatch _wait_bf_watcher;
    RuntimeProfile::Counter* _wait_bf_timer = nullptr;
    RuntimeProfile::Counter* _wait_bf_counts = nullptr;
    MonotonicStopWatch _wait_sink_watcher;
    MonotonicStopWatch _wait_worker_watcher;
    RuntimeProfile::Counter* _wait_worker_timer = nullptr;
    RuntimeProfile::Counter* _wait_dependency_counts = nullptr;
    RuntimeProfile::Counter* _pending_finish_counts = nullptr;
    // TODO we should calculate the time between when really runnable and runnable
    RuntimeProfile::Counter* _yield_counts = nullptr;
    RuntimeProfile::Counter* _core_change_times = nullptr;

    MonotonicStopWatch _pipeline_task_watcher;

    OperatorXs _operators; // left is _source, right is _root
    OperatorXPtr _source;
    OperatorXPtr _root;
    DataSinkOperatorXPtr _sink;

    std::vector<Dependency*> _read_dependencies;
    std::vector<Dependency*> _write_dependencies;
    std::vector<Dependency*> _finish_dependencies;
    std::vector<Dependency*> _filter_dependencies;

    // All shared states of this pipeline task.
    std::map<int, std::shared_ptr<BasicSharedState>> _op_shared_states;
    std::shared_ptr<BasicSharedState> _sink_shared_state;
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
            _le_state_map;
    int _task_idx;
    bool _dry_run = false;

    Dependency* _blocked_dep = nullptr;

    Dependency* _execution_dep = nullptr;

    std::atomic<bool> _finished {false};
    std::mutex _release_lock;

    std::atomic<bool> _running {false};
};

} // namespace doris::pipeline
