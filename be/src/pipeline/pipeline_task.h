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

namespace doris {
class QueryContext;
class RuntimeState;
namespace pipeline {
class PipelineFragmentContext;
} // namespace pipeline
} // namespace doris

namespace doris::pipeline {

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

    PipelineFragmentContext* fragment_context() { return _fragment_context; }

    QueryContext* query_context();

    int get_previous_core_id() const {
        return _previous_schedule_id != -1 ? _previous_schedule_id
                                           : _pipeline->_previous_schedule_id;
    }

    void set_previous_core_id(int id) {
        if (id != _previous_schedule_id) {
            if (_previous_schedule_id != -1) {
                COUNTER_UPDATE(_core_change_times, 1);
            }
            _previous_schedule_id = id;
        }
    }

    void finalize();

    std::string debug_string();

    bool is_pending_finish() {
        for (auto* fin_dep : _finish_dependencies) {
            _blocked_dep = fin_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return true;
            }
        }
        return false;
    }

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

    int task_id() const { return _index; };
    bool is_finalized() const { return _finalized; }

    void clear_blocking_state() {
        // We use a lock to assure all dependencies are not deconstructed here.
        std::unique_lock<std::mutex> lc(_dependency_lock);
        if (!_finalized) {
            _execution_dep->set_always_ready();
            for (auto* dep : _filter_dependencies) {
                dep->set_always_ready();
            }
            for (auto& deps : _read_dependencies) {
                for (auto* dep : deps) {
                    dep->set_always_ready();
                }
            }
            for (auto* dep : _write_dependencies) {
                dep->set_always_ready();
            }
            for (auto* dep : _finish_dependencies) {
                dep->set_always_ready();
            }
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

    /**
     * Return true if:
     * 1. `enable_force_spill` is true which forces this task to spill data.
     * 2. Or memory consumption reaches the high water mark of current workload group (80% of memory limitation by default) and revocable_mem_bytes is bigger than min_revocable_mem_bytes.
     * 3. Or memory consumption is higher than the low water mark of current workload group (50% of memory limitation by default) and `query_weighted_consumption >= query_weighted_limit` and revocable memory is big enough.
     */
    static bool should_revoke_memory(RuntimeState* state, int64_t revocable_mem_bytes);

    void put_in_runnable_queue() {
        _schedule_time++;
        _wait_worker_watcher.start();
    }

    void pop_out_runnable_queue() { _wait_worker_watcher.stop(); }

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
                      << config::enable_debug_log_timeout_secs << " seconds. "
                      << "/n task detail:" << debug_string();
        }
    }

    RuntimeState* runtime_state() const { return _state; }

    std::string task_name() const { return fmt::format("task{}({})", _index, _pipeline->_name); }

    void stop_if_finished() {
        if (_sink->is_finished(_state)) {
            clear_blocking_state();
        }
    }

private:
    friend class RuntimeFilterDependency;
    bool _is_blocked();
    bool _wait_to_start();

    Status _extract_dependencies();
    void _init_profile();
    void _fresh_profile_counter();
    Status _open();

    uint32_t _index;
    PipelinePtr _pipeline;
    bool _has_exceed_timeout = false;
    bool _opened;
    RuntimeState* _state = nullptr;
    int _previous_schedule_id = -1;
    uint32_t _schedule_time = 0;
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

    MonotonicStopWatch _pipeline_task_watcher;

    OperatorXs _operators; // left is _source, right is _root
    OperatorXBase* _source;
    OperatorXBase* _root;
    DataSinkOperatorXPtr _sink;

    // `_read_dependencies` is stored as same order as `_operators`
    std::vector<std::vector<Dependency*>> _read_dependencies;
    std::vector<Dependency*> _write_dependencies;
    std::vector<Dependency*> _finish_dependencies;
    std::vector<Dependency*> _filter_dependencies;

    // All shared states of this pipeline task.
    std::map<int, std::shared_ptr<BasicSharedState>> _op_shared_states;
    std::shared_ptr<BasicSharedState> _sink_shared_state;
    std::vector<TScanRangeParams> _scan_ranges;
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
            _le_state_map;
    int _task_idx;
    bool _dry_run = false;

    Dependency* _blocked_dep = nullptr;

    Dependency* _execution_dep = nullptr;

    std::atomic<bool> _finalized {false};
    std::mutex _dependency_lock;

    std::atomic<bool> _running {false};
    std::atomic<bool> _eos {false};
};

} // namespace doris::pipeline
