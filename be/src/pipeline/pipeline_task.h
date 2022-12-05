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

#include "exec/operator.h"
#include "pipeline.h"
#include "util/stopwatch.hpp"

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
enum PipelineTaskState : uint8_t {
    NOT_READY = 0, // do not prepare
    BLOCKED_FOR_DEPENDENCY = 1,
    BLOCKED_FOR_SOURCE = 2,
    BLOCKED_FOR_SINK = 3,
    RUNNABLE = 4, // can execute
    PENDING_FINISH =
            5, // compute task is over, but still hold resource. like some scan and sink task
    FINISHED = 6,
    CANCELED = 7
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
    }
    __builtin_unreachable();
}

// The class do the pipeline task. Minest schdule union by task scheduler
class PipelineTask {
public:
    PipelineTask(PipelinePtr& pipeline, uint32_t index, RuntimeState* state, Operators& operators,
                 OperatorPtr& sink, PipelineFragmentContext* fragment_context,
                 RuntimeProfile* parent_profile)
            : _index(index),
              _pipeline(pipeline),
              _operators(operators),
              _source(_operators.front()),
              _root(_operators.back()),
              _sink(sink),
              _prepared(false),
              _opened(false),
              _state(state),
              _cur_state(NOT_READY),
              _data_state(SourceState::DEPEND_ON_SOURCE),
              _fragment_context(fragment_context),
              _parent_profile(parent_profile) {}

    Status prepare(RuntimeState* state);

    Status execute(bool* eos);

    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    Status close();

    void start_worker_watcher() { _wait_worker_watcher.start(); }
    void stop_worker_watcher() { _wait_worker_watcher.stop(); }
    void start_schedule_watcher() { _wait_schedule_watcher.start(); }
    void stop_schedule_watcher() { _wait_schedule_watcher.stop(); }

    PipelineTaskState get_state() { return _cur_state; }
    void set_state(PipelineTaskState state);
    bool is_blocking_state() {
        switch (_cur_state) {
        case BLOCKED_FOR_DEPENDENCY:
        case BLOCKED_FOR_SOURCE:
        case BLOCKED_FOR_SINK:
            return true;
        default:
            return false;
        }
    }

    bool is_pending_finish() { return _source->is_pending_finish() || _sink->is_pending_finish(); }

    bool source_can_read() { return _source->can_read(); }

    bool sink_can_write() { return _sink->can_write(); }

    Status finalize();

    void finish_p_dependency() {
        for (const auto& p : _pipeline->_parents) {
            p->finish_one_dependency();
        }
    }

    PipelineFragmentContext* fragment_context() { return _fragment_context; }

    QueryFragmentsCtx* query_fragments_context();

    int get_previous_core_id() const { return _previous_schedule_id; }

    void set_previous_core_id(int id) { _previous_schedule_id = id; }

    bool has_dependency();

    uint32_t index() const { return _index; }

    OperatorPtr get_root() { return _root; }

    std::string debug_string() const;

    RuntimeState* runtime_state() { return _state; }

    static constexpr auto THREAD_TIME_SLICE = 100'000'000L;

private:
    Status open();
    void _init_profile();
    void _init_state();

private:
    uint32_t _index;
    PipelinePtr _pipeline;
    bool _dependency_finish = false;
    Operators _operators; // left is _source, right is _root
    OperatorPtr _source;
    OperatorPtr _root;
    OperatorPtr _sink;

    bool _prepared;
    bool _opened;
    RuntimeState* _state;
    int _previous_schedule_id = -1;
    PipelineTaskState _cur_state;
    SourceState _data_state;
    std::unique_ptr<doris::vectorized::Block> _block;
    PipelineFragmentContext* _fragment_context;

    RuntimeProfile* _parent_profile;
    std::unique_ptr<RuntimeProfile> _task_profile;
    RuntimeProfile::Counter* _sink_timer;
    RuntimeProfile::Counter* _get_block_timer;
    RuntimeProfile::Counter* _block_counts;
    MonotonicStopWatch _wait_source_watcher;
    RuntimeProfile::Counter* _wait_source_timer;
    MonotonicStopWatch _wait_sink_watcher;
    RuntimeProfile::Counter* _wait_sink_timer;
    MonotonicStopWatch _wait_worker_watcher;
    RuntimeProfile::Counter* _wait_worker_timer;
    // TODO we should calculate the time between when really runnable and runnable
    MonotonicStopWatch _wait_schedule_watcher;
    RuntimeProfile::Counter* _wait_schedule_timer;
    RuntimeProfile::Counter* _yield_counts;
};
} // namespace doris::pipeline