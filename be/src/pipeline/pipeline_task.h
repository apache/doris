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
#include "exec/operator.h"
#include "pipeline.h"
#include "runtime/task_group/task_group.h"
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

class TaskQueue;
class PriorityTaskQueue;

// The class do the pipeline task. Minest schdule union by task scheduler
class PipelineTask {
public:
    PipelineTask(PipelinePtr& pipeline, uint32_t index, RuntimeState* state, OperatorPtr& sink,
                 PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile);

    PipelineTask(PipelinePtr& pipeline, uint32_t index, RuntimeState* state,
                 PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile);
    virtual ~PipelineTask() = default;

    virtual Status prepare(RuntimeState* state);

    virtual Status execute(bool* eos);

    // Try to close this pipeline task. If there are still some resources need to be released after `try_close`,
    // this task will enter the `PENDING_FINISH` state.
    virtual Status try_close(Status exec_status);
    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    virtual Status close(Status exec_status);

    void put_in_runnable_queue() {
        _schedule_time++;
        _wait_worker_watcher.start();
    }
    void pop_out_runnable_queue() { _wait_worker_watcher.stop(); }
    PipelineTaskState get_state() { return _cur_state; }
    void set_state(PipelineTaskState state);

    virtual bool is_pending_finish() {
        bool source_ret = _source->is_pending_finish();
        if (source_ret) {
            return true;
        } else {
            this->set_src_pending_finish_time();
        }

        bool sink_ret = _sink->is_pending_finish();
        if (sink_ret) {
            return true;
        } else {
            this->set_dst_pending_finish_time();
        }
        return false;
    }

    virtual bool source_can_read() { return _source->can_read() || _pipeline->_always_can_read; }

    virtual bool runtime_filters_are_ready_or_timeout() {
        return _source->runtime_filters_are_ready_or_timeout();
    }

    virtual bool sink_can_write() { return _sink->can_write() || _pipeline->_always_can_write; }

    virtual Status finalize();

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

    virtual void release_dependency() {}

    bool has_dependency();

    OperatorPtr get_root() { return _root; }

    virtual std::string debug_string();

    taskgroup::TaskGroupPipelineTaskEntity* get_task_group_entity() const;

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

    void set_begin_execute_time() {
        if (!_is_first_time_to_execute) {
            _begin_execute_time = _pipeline_task_watcher.elapsed_time();
            _is_first_time_to_execute = true;
        }
    }

    void set_eos_time() {
        if (!_is_eos) {
            _eos_time = _pipeline_task_watcher.elapsed_time();
            _is_eos = true;
        }
    }

    void set_src_pending_finish_time() {
        if (!_is_src_pending_finish_over) {
            _src_pending_finish_over_time = _pipeline_task_watcher.elapsed_time();
            _is_src_pending_finish_over = true;
        }
    }

    void set_dst_pending_finish_time() {
        if (!_is_dst_pending_finish_over) {
            _dst_pending_finish_over_time = _pipeline_task_watcher.elapsed_time();
            _is_dst_pending_finish_over = true;
        }
    }

    virtual void set_close_pipeline_time() {
        if (!_is_close_pipeline) {
            _close_pipeline_time = _pipeline_task_watcher.elapsed_time();
            _is_close_pipeline = true;
            COUNTER_SET(_close_pipeline_timer, _close_pipeline_time);
        }
    }

    TUniqueId instance_id() const { return _state->fragment_instance_id(); }

    void set_parent_profile(RuntimeProfile* profile) { _parent_profile = profile; }

protected:
    void _finish_p_dependency() {
        for (const auto& p : _pipeline->_parents) {
            p.second.lock()->finish_one_dependency(p.first, _previous_schedule_id);
        }
    }

    virtual Status _open();
    virtual void _init_profile();
    virtual void _fresh_profile_counter();

    uint32_t _index;
    PipelinePtr _pipeline;
    bool _dependency_finish = false;

    bool _prepared;
    bool _opened;
    RuntimeState* _state;
    int _previous_schedule_id = -1;
    uint32_t _schedule_time = 0;
    PipelineTaskState _cur_state;
    SourceState _data_state;
    std::unique_ptr<doris::vectorized::Block> _block;
    PipelineFragmentContext* _fragment_context;
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

    bool _try_close_flag = false;

    RuntimeProfile* _parent_profile;
    std::unique_ptr<RuntimeProfile> _task_profile;
    RuntimeProfile::Counter* _task_cpu_timer;
    RuntimeProfile::Counter* _prepare_timer;
    RuntimeProfile::Counter* _open_timer;
    RuntimeProfile::Counter* _exec_timer;
    RuntimeProfile::Counter* _get_block_timer;
    RuntimeProfile::Counter* _get_block_counter;
    RuntimeProfile::Counter* _sink_timer;
    RuntimeProfile::Counter* _finalize_timer;
    RuntimeProfile::Counter* _close_timer;
    RuntimeProfile::Counter* _block_counts;
    RuntimeProfile::Counter* _block_by_source_counts;
    RuntimeProfile::Counter* _block_by_sink_counts;
    RuntimeProfile::Counter* _schedule_counts;
    MonotonicStopWatch _wait_source_watcher;
    RuntimeProfile::Counter* _wait_source_timer;
    MonotonicStopWatch _wait_bf_watcher;
    RuntimeProfile::Counter* _wait_bf_timer;
    RuntimeProfile::Counter* _wait_bf_counts;
    MonotonicStopWatch _wait_sink_watcher;
    RuntimeProfile::Counter* _wait_sink_timer;
    MonotonicStopWatch _wait_worker_watcher;
    RuntimeProfile::Counter* _wait_worker_timer;
    RuntimeProfile::Counter* _wait_dependency_counts;
    RuntimeProfile::Counter* _pending_finish_counts;
    // TODO we should calculate the time between when really runnable and runnable
    RuntimeProfile::Counter* _yield_counts;
    RuntimeProfile::Counter* _core_change_times;

    // The monotonic time of the entire lifecycle of the pipelinetask, almost synchronized with the pipfragmentctx
    // There are several important time points:
    // 1 first time pipelinetask to execute
    // 2 task eos
    // 3 src pending finish over
    // 4 dst pending finish over
    // 5 close pipeline time, we mark this beacause pending finish state may change
    MonotonicStopWatch _pipeline_task_watcher;
    // time 1
    bool _is_first_time_to_execute = false;
    RuntimeProfile::Counter* _begin_execute_timer;
    int64_t _begin_execute_time = 0;
    // time 2
    bool _is_eos = false;
    RuntimeProfile::Counter* _eos_timer;
    int64_t _eos_time = 0;
    //time 3
    bool _is_src_pending_finish_over = false;
    RuntimeProfile::Counter* _src_pending_finish_over_timer;
    int64_t _src_pending_finish_over_time = 0;
    // time 4
    bool _is_dst_pending_finish_over = false;
    RuntimeProfile::Counter* _dst_pending_finish_over_timer;
    int64_t _dst_pending_finish_over_time = 0;
    // time 5
    bool _is_close_pipeline = false;
    RuntimeProfile::Counter* _close_pipeline_timer;
    int64_t _close_pipeline_time = 0;

    RuntimeProfile::Counter* _pip_task_total_timer;
    std::shared_ptr<QueryStatistics> _query_statistics;
    Status _collect_query_statistics();
    bool _collect_query_statistics_with_every_batch = false;

private:
    Operators _operators; // left is _source, right is _root
    OperatorPtr _source;
    OperatorPtr _root;
    OperatorPtr _sink;
};
} // namespace doris::pipeline
