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

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <utility>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/task_execution_context.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"
#include "vec/runtime/partitioner.h"

namespace doris::pipeline {
using SpillPartitionerType = vectorized::Crc32HashPartitioner<vectorized::SpillPartitionChannelIds>;

struct SpillContext {
    std::atomic_int running_tasks_count;
    TUniqueId query_id;
    std::function<void(SpillContext*)> all_tasks_finished_callback;

    SpillContext(int running_tasks_count_, TUniqueId query_id_,
                 std::function<void(SpillContext*)> all_tasks_finished_callback_)
            : running_tasks_count(running_tasks_count_),
              query_id(std::move(query_id_)),
              all_tasks_finished_callback(std::move(all_tasks_finished_callback_)) {}

    ~SpillContext() {
        LOG_IF(WARNING, running_tasks_count.load() != 0)
                << "Query: " << print_id(query_id)
                << " not all spill tasks finished, remaining tasks: " << running_tasks_count.load();

        LOG_IF(WARNING, _running_non_sink_tasks_count.load() != 0)
                << "Query: " << print_id(query_id)
                << " not all spill tasks(non sink tasks) finished, remaining tasks: "
                << _running_non_sink_tasks_count.load();
    }

    void on_task_finished() {
        auto count = running_tasks_count.fetch_sub(1);
        if (count == 1 && _running_non_sink_tasks_count.load() == 0) {
            all_tasks_finished_callback(this);
        }
    }

    void on_non_sink_task_started() { _running_non_sink_tasks_count.fetch_add(1); }
    void on_non_sink_task_finished() {
        const auto count = _running_non_sink_tasks_count.fetch_sub(1);
        if (count == 1 && running_tasks_count.load() == 0) {
            all_tasks_finished_callback(this);
        }
    }

private:
    std::atomic_int _running_non_sink_tasks_count {0};
};

class SpillRunnable : public Runnable {
protected:
    SpillRunnable(RuntimeState* state, std::shared_ptr<SpillContext> spill_context,
                  std::shared_ptr<Dependency> spill_dependency, RuntimeProfile* profile,
                  const std::shared_ptr<BasicSpillSharedState>& shared_state, bool is_write,
                  std::function<Status()> spill_exec_func,
                  std::function<Status()> spill_fin_cb = {})
            : _state(state),
              _profile(profile),
              _spill_context(std::move(spill_context)),
              _spill_dependency(std::move(spill_dependency)),
              _is_write_task(is_write),
              _task_context_holder(state->get_task_execution_context()),
              _shared_state_holder(shared_state),
              _spill_exec_func(std::move(spill_exec_func)),
              _spill_fin_cb(std::move(spill_fin_cb)) {
        _exec_timer = profile->get_counter("ExecTime");
        _spill_total_timer = profile->get_counter("SpillTotalTime");

        if (is_write) {
            _spill_write_wait_in_queue_timer =
                    profile->get_counter("SpillWriteTaskWaitInQueueTime");
            _write_wait_in_queue_task_count =
                    profile->get_counter("SpillWriteTaskWaitInQueueCount");
            _writing_task_count = profile->get_counter("SpillWriteTaskCount");
            COUNTER_UPDATE(_write_wait_in_queue_task_count, 1);
        }

        _submit_timer.start();
    }

public:
    ~SpillRunnable() override = default;

    void run() override {
        const auto submit_elapsed_time = _submit_timer.elapsed_time();
        // Should lock task context before scope task, because the _state maybe
        // destroyed when run is called.
        auto task_context_holder = _task_context_holder.lock();
        if (!task_context_holder) {
            return;
        }
        SCOPED_ATTACH_TASK(_state);

        _exec_timer->update(submit_elapsed_time);
        _spill_total_timer->update(submit_elapsed_time);

        SCOPED_TIMER(_exec_timer);
        SCOPED_TIMER(_spill_total_timer);

        auto* spill_timer = _get_spill_timer();
        DCHECK(spill_timer != nullptr);
        SCOPED_TIMER(spill_timer);

        _on_task_started(submit_elapsed_time);

        Defer defer([&] {
            {
                std::function<Status()> tmp;
                std::swap(tmp, _spill_exec_func);
            }
            {
                std::function<Status()> tmp;
                std::swap(tmp, _spill_fin_cb);
            }
        });

        auto shared_state_holder = _shared_state_holder.lock();
        if (!shared_state_holder) {
            return;
        }

        if (_state->is_cancelled()) {
            return;
        }
        shared_state_holder->_spill_status.update(_spill_exec_func());

        _on_task_finished();
        if (_spill_fin_cb) {
            shared_state_holder->_spill_status.update(_spill_fin_cb());
        }

        if (_spill_dependency) {
            _spill_dependency->set_ready();
        }
    }

protected:
    virtual void _on_task_finished() {
        if (_spill_context) {
            _spill_context->on_task_finished();
        }
    }

    virtual RuntimeProfile::Counter* _get_spill_timer() {
        return _profile->get_counter("SpillWriteTime");
    }

    virtual void _on_task_started(uint64_t submit_elapsed_time) {
        if (_is_write_task) {
            COUNTER_UPDATE(_spill_write_wait_in_queue_timer, submit_elapsed_time);
            COUNTER_UPDATE(_write_wait_in_queue_task_count, -1);
            COUNTER_UPDATE(_writing_task_count, 1);
        }
    }

    RuntimeState* _state;
    RuntimeProfile* _profile;
    std::shared_ptr<SpillContext> _spill_context;
    std::shared_ptr<Dependency> _spill_dependency;

    bool _is_write_task;

private:
    MonotonicStopWatch _submit_timer;

    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::Counter* _spill_total_timer;

    RuntimeProfile::Counter* _spill_write_wait_in_queue_timer = nullptr;
    RuntimeProfile::Counter* _write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _writing_task_count = nullptr;

    std::weak_ptr<TaskExecutionContext> _task_context_holder;
    std::weak_ptr<BasicSpillSharedState> _shared_state_holder;
    std::function<Status()> _spill_exec_func;
    std::function<Status()> _spill_fin_cb;
};

class SpillSinkRunnable : public SpillRunnable {
public:
    SpillSinkRunnable(RuntimeState* state, std::shared_ptr<SpillContext> spill_context,
                      std::shared_ptr<Dependency> spill_dependency, RuntimeProfile* profile,
                      const std::shared_ptr<BasicSpillSharedState>& shared_state,
                      std::function<Status()> spill_exec_func,
                      std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, spill_context, spill_dependency, profile, shared_state, true,
                            spill_exec_func, spill_fin_cb) {}
};

class SpillNonSinkRunnable : public SpillRunnable {
public:
    SpillNonSinkRunnable(RuntimeState* state, std::shared_ptr<SpillContext> spill_context,
                         std::shared_ptr<Dependency> spill_dependency, RuntimeProfile* profile,
                         const std::shared_ptr<BasicSpillSharedState>& shared_state,
                         std::function<Status()> spill_exec_func,
                         std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, spill_context, spill_dependency, profile, shared_state, true,
                            spill_exec_func, spill_fin_cb) {}

protected:
    void _on_task_finished() override {
        if (_spill_context) {
            _spill_context->on_non_sink_task_finished();
        }
    }
};

class SpillRecoverRunnable : public SpillRunnable {
public:
    SpillRecoverRunnable(RuntimeState* state, std::shared_ptr<Dependency> spill_dependency,
                         RuntimeProfile* profile,
                         const std::shared_ptr<BasicSpillSharedState>& shared_state,
                         std::function<Status()> spill_exec_func,
                         std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, nullptr, spill_dependency, profile, shared_state, false,
                            spill_exec_func, spill_fin_cb) {
        _spill_revover_timer = profile->get_counter("SpillRecoverTime");
        _spill_read_wait_in_queue_timer = profile->get_counter("SpillReadTaskWaitInQueueTime");
        _read_wait_in_queue_task_count = profile->get_counter("SpillReadTaskWaitInQueueCount");
        _reading_task_count = profile->get_counter("SpillReadTaskCount");

        COUNTER_UPDATE(_read_wait_in_queue_task_count, 1);
    }

protected:
    RuntimeProfile::Counter* _get_spill_timer() override {
        return _profile->get_counter("SpillRecoverTime");
    }

    void _on_task_started(uint64_t submit_elapsed_time) override {
        COUNTER_UPDATE(_spill_read_wait_in_queue_timer, submit_elapsed_time);
        COUNTER_UPDATE(_read_wait_in_queue_task_count, -1);
        COUNTER_UPDATE(_reading_task_count, 1);
    }

private:
    RuntimeProfile::Counter* _spill_revover_timer;
    RuntimeProfile::Counter* _spill_read_wait_in_queue_timer = nullptr;
    RuntimeProfile::Counter* _read_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _reading_task_count = nullptr;
};
} // namespace doris::pipeline