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

#include "pipeline/task_queue.h"

namespace doris::pipeline {

class DummyTaskQueue final : public MultiCoreTaskQueue {
    explicit DummyTaskQueue(int core_size) : MultiCoreTaskQueue(core_size) {}
    ~DummyTaskQueue() override = default;
    PipelineTask* take(int core_id) override {
        PipelineTask* task = nullptr;
        do {
            DCHECK(_prio_task_queues.size() > core_id)
                    << " list size: " << _prio_task_queues.size() << " core_id: " << core_id
                    << " _core_size: " << _core_size << " _next_core: " << _next_core.load();
            task = _prio_task_queues[core_id].try_take(false);
            if (task) {
                break;
            }
            task = _steal_take(core_id);
            if (task) {
                break;
            }
            task = _prio_task_queues[core_id].take(1);
            if (task) {
                break;
            }
        } while (false);
        if (task) {
            task->pop_out_runnable_queue();
        }
        return task;
    }
};
} // namespace doris::pipeline
