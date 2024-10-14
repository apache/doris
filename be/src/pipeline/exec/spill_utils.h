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
                << "query: " << print_id(query_id)
                << " not all spill tasks finished, remaining tasks: " << running_tasks_count.load();

        LOG_IF(WARNING, _running_non_sink_tasks_count.load() != 0)
                << "query: " << print_id(query_id)
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
public:
    SpillRunnable(RuntimeState* state, RuntimeProfile* profile, bool is_write,
                  const std::shared_ptr<BasicSharedState>& shared_state, std::function<void()> func)
            : _state(state),
              _is_write(is_write),
              _task_context_holder(state->get_task_execution_context()),
              _shared_state_holder(shared_state),
              _func(std::move(func)) {
        write_wait_in_queue_task_count = profile->get_counter("SpillWriteTaskWaitInQueueCount");
        writing_task_count = profile->get_counter("SpillWriteTaskCount");
        read_wait_in_queue_task_count = profile->get_counter("SpillReadTaskWaitInQueueCount");
        reading_task_count = profile->get_counter("SpillReadTaskCount");
        if (is_write) {
            COUNTER_UPDATE(write_wait_in_queue_task_count, 1);
        } else {
            COUNTER_UPDATE(read_wait_in_queue_task_count, 1);
        }
    }

    ~SpillRunnable() override = default;

    void run() override {
        // Should lock task context before scope task, because the _state maybe
        // destroyed when run is called.
        auto task_context_holder = _task_context_holder.lock();
        if (!task_context_holder) {
            return;
        }
        if (_is_write) {
            COUNTER_UPDATE(write_wait_in_queue_task_count, -1);
            COUNTER_UPDATE(writing_task_count, 1);
        } else {
            COUNTER_UPDATE(read_wait_in_queue_task_count, -1);
            COUNTER_UPDATE(reading_task_count, 1);
        }
        SCOPED_ATTACH_TASK(_state);
        Defer defer([&] {
            if (_is_write) {
                COUNTER_UPDATE(writing_task_count, -1);
            } else {
                COUNTER_UPDATE(reading_task_count, -1);
            }
            std::function<void()> tmp;
            std::swap(tmp, _func);
        });

        auto shared_state_holder = _shared_state_holder.lock();
        if (!shared_state_holder) {
            return;
        }

        if (_state->is_cancelled()) {
            return;
        }
        _func();
    }

private:
    RuntimeState* _state;
    bool _is_write;
    RuntimeProfile::Counter* write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* writing_task_count = nullptr;
    RuntimeProfile::Counter* read_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* reading_task_count = nullptr;
    std::weak_ptr<TaskExecutionContext> _task_context_holder;
    std::weak_ptr<BasicSharedState> _shared_state_holder;
    std::function<void()> _func;
};

} // namespace doris::pipeline