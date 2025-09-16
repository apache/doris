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

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <utility>

#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "vec/runtime/partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
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
        if (running_tasks_count.load() != 0) {
            LOG(WARNING) << "Query: " << print_id(query_id)
                         << " not all spill tasks finished, remaining tasks: "
                         << running_tasks_count.load();
        }
    }

    void on_task_finished() {
        auto count = running_tasks_count.fetch_sub(1);
        if (count == 1) {
            all_tasks_finished_callback(this);
        }
    }
};

class SpillRunnable {
protected:
    SpillRunnable(RuntimeState* state, std::shared_ptr<SpillContext> spill_context,
                  RuntimeProfile* operator_profile, bool is_write,
                  std::function<Status()> spill_exec_func,
                  std::function<Status()> spill_fin_cb = {})
            : _state(state),
              _custom_profile(operator_profile->get_child("CustomCounters")),
              _spill_context(std::move(spill_context)),
              _is_write_task(is_write),
              _spill_exec_func(std::move(spill_exec_func)),
              _spill_fin_cb(std::move(spill_fin_cb)) {
        RuntimeProfile* common_profile = operator_profile->get_child("CommonCounters");
        DCHECK(common_profile != nullptr);
        DCHECK(_custom_profile != nullptr);
        _spill_total_timer = _custom_profile->get_counter("SpillTotalTime");

        if (is_write) {
            _write_wait_in_queue_task_count =
                    _custom_profile->get_counter("SpillWriteTaskWaitInQueueCount");
            _writing_task_count = _custom_profile->get_counter("SpillWriteTaskCount");
            COUNTER_UPDATE(_write_wait_in_queue_task_count, 1);
        }
    }

public:
    virtual ~SpillRunnable() = default;

    [[nodiscard]] Status run() {
        SCOPED_TIMER(_spill_total_timer);

        auto* spill_timer = _get_spill_timer();
        DCHECK(spill_timer != nullptr);
        SCOPED_TIMER(spill_timer);

        _on_task_started();

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

        if (_state->is_cancelled()) {
            return _state->cancel_reason();
        }

        RETURN_IF_ERROR(_spill_exec_func());
        _on_task_finished();
        if (_spill_fin_cb) {
            return _spill_fin_cb();
        }

        return Status::OK();
    }

protected:
    virtual void _on_task_finished() {
        if (_spill_context) {
            _spill_context->on_task_finished();
        }
    }

    virtual RuntimeProfile::Counter* _get_spill_timer() {
        return _custom_profile->get_counter("SpillWriteTime");
    }

    virtual void _on_task_started() {
        VLOG_DEBUG << "Query: " << print_id(_state->query_id())
                   << " spill task started, pipeline task id: " << _state->task_id();
        if (_is_write_task) {
            COUNTER_UPDATE(_write_wait_in_queue_task_count, -1);
            COUNTER_UPDATE(_writing_task_count, 1);
        }
    }

    RuntimeState* _state;
    RuntimeProfile* _custom_profile;
    std::shared_ptr<SpillContext> _spill_context;
    bool _is_write_task;

private:
    RuntimeProfile::Counter* _spill_total_timer;

    RuntimeProfile::Counter* _write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _writing_task_count = nullptr;

    std::function<Status()> _spill_exec_func;
    std::function<Status()> _spill_fin_cb;
};

class SpillSinkRunnable : public SpillRunnable {
public:
    SpillSinkRunnable(RuntimeState* state, std::shared_ptr<SpillContext> spill_context,
                      RuntimeProfile* operator_profile, std::function<Status()> spill_exec_func,
                      std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, spill_context, operator_profile, true, spill_exec_func,
                            spill_fin_cb) {}
};

class SpillNonSinkRunnable : public SpillRunnable {
public:
    SpillNonSinkRunnable(RuntimeState* state, RuntimeProfile* operator_profile,
                         std::function<Status()> spill_exec_func,
                         std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, nullptr, operator_profile, true, spill_exec_func, spill_fin_cb) {
    }
};

class SpillRecoverRunnable : public SpillRunnable {
public:
    SpillRecoverRunnable(RuntimeState* state, RuntimeProfile* operator_profile,
                         std::function<Status()> spill_exec_func,
                         std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, nullptr, operator_profile, false, spill_exec_func,
                            spill_fin_cb) {
        RuntimeProfile* custom_profile = operator_profile->get_child("CustomCounters");
        DCHECK(custom_profile != nullptr);
        _spill_revover_timer = custom_profile->get_counter("SpillRecoverTime");
        _read_wait_in_queue_task_count =
                custom_profile->get_counter("SpillReadTaskWaitInQueueCount");
        _reading_task_count = custom_profile->get_counter("SpillReadTaskCount");

        COUNTER_UPDATE(_read_wait_in_queue_task_count, 1);
    }

protected:
    RuntimeProfile::Counter* _get_spill_timer() override {
        return _custom_profile->get_counter("SpillRecoverTime");
    }

    void _on_task_started() override {
        LOG(INFO) << "SpillRecoverRunnable, Query: " << print_id(_state->query_id())
                  << " spill task started, pipeline task id: " << _state->task_id();
        COUNTER_UPDATE(_read_wait_in_queue_task_count, -1);
        COUNTER_UPDATE(_reading_task_count, 1);
    }

private:
    RuntimeProfile::Counter* _spill_revover_timer;
    RuntimeProfile::Counter* _read_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _reading_task_count = nullptr;
};

template <bool accumulating>
inline void update_profile_from_inner_profile(const std::string& name,
                                              RuntimeProfile* runtime_profile,
                                              RuntimeProfile* inner_profile) {
    auto* inner_counter = inner_profile->get_counter(name);
    DCHECK(inner_counter != nullptr) << "inner counter " << name << " not found";
    if (inner_counter == nullptr) [[unlikely]] {
        return;
    }
    auto* counter = runtime_profile->get_counter(name);
    if (counter == nullptr) [[unlikely]] {
        counter = runtime_profile->add_counter(name, inner_counter->type(), "",
                                               inner_counter->level());
    }
    if constexpr (accumulating) {
        // Memory usage should not be accumulated.
        if (counter->type() == TUnit::BYTES) {
            counter->set(inner_counter->value());
        } else {
            counter->update(inner_counter->value());
        }
    } else {
        counter->set(inner_counter->value());
    }
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline