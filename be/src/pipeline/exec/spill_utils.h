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

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/task_execution_context.h"
#include "runtime/thread_context.h"
#include "util/threadpool.h"
#include "vec/runtime/partitioner.h"

namespace doris::pipeline {
using SpillPartitionerType = vectorized::Crc32HashPartitioner<vectorized::SpillPartitionChannelIds>;

class SpillRunnable : public Runnable {
public:
    SpillRunnable(RuntimeState* state, const std::shared_ptr<BasicSharedState>& shared_state,
                  std::function<void()> func)
            : _state(state),
              _task_context_holder(state->get_task_execution_context()),
              _shared_state_holder(shared_state),
              _func(std::move(func)) {}

    ~SpillRunnable() override = default;

    void run() override {
        // Should lock task context before scope task, because the _state maybe
        // destroyed when run is called.
        auto task_context_holder = _task_context_holder.lock();
        if (!task_context_holder) {
            return;
        }
        SCOPED_ATTACH_TASK(_state);
        Defer defer([&] {
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
    std::weak_ptr<TaskExecutionContext> _task_context_holder;
    std::weak_ptr<BasicSharedState> _shared_state_holder;
    std::function<void()> _func;
};

} // namespace doris::pipeline