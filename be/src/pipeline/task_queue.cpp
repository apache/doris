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

#include "task_queue.h"

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <string>

#include "common/logging.h"
#include "pipeline/pipeline_task.h"
#include "runtime/workload_group/workload_group.h"

namespace doris::pipeline {

TaskQueue::~TaskQueue() = default;

PipelineTask* SubTaskQueue::try_take(bool is_steal) {
    if (_queue.empty()) {
        return nullptr;
    }
    auto task = _queue.front();
    _queue.pop();
    return task;
}

////////////////////  PriorityTaskQueue ////////////////////

PriorityTaskQueue::PriorityTaskQueue() : _closed(false) {
    double factor = 1;
    for (int i = SUB_QUEUE_LEVEL - 1; i >= 0; i--) {
        _sub_queues[i].set_level_factor(factor);
        factor *= LEVEL_QUEUE_TIME_FACTOR;
    }
}

void PriorityTaskQueue::close() {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    _closed = true;
    _wait_task.notify_all();
}

PipelineTask* PriorityTaskQueue::_try_take_unprotected(bool is_steal) {
    if (_total_task_size == 0 || _closed) {
        return nullptr;
    }

    double min_vruntime = 0;
    int level = -1;
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        double cur_queue_vruntime = _sub_queues[i].get_vruntime();
        if (!_sub_queues[i].empty()) {
            if (level == -1 || cur_queue_vruntime < min_vruntime) {
                level = i;
                min_vruntime = cur_queue_vruntime;
            }
        }
    }
    DCHECK(level != -1);
    _queue_level_min_vruntime = uint64_t(min_vruntime);

    auto task = _sub_queues[level].try_take(is_steal);
    if (task) {
        task->update_queue_level(level);
        _total_task_size--;
    }
    return task;
}

int PriorityTaskQueue::_compute_level(uint64_t runtime) {
    for (int i = 0; i < SUB_QUEUE_LEVEL - 1; ++i) {
        if (runtime <= _queue_level_limit[i]) {
            return i;
        }
    }
    return SUB_QUEUE_LEVEL - 1;
}

PipelineTask* PriorityTaskQueue::try_take(bool is_steal) {
    // TODO other efficient lock? e.g. if get lock fail, return null_ptr
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    return _try_take_unprotected(is_steal);
}

PipelineTask* PriorityTaskQueue::take(uint32_t timeout_ms) {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    auto task = _try_take_unprotected(false);
    if (task) {
        return task;
    } else {
        if (timeout_ms > 0) {
            _wait_task.wait_for(lock, std::chrono::milliseconds(timeout_ms));
        } else {
            _wait_task.wait(lock);
        }
        return _try_take_unprotected(false);
    }
}

Status PriorityTaskQueue::push(PipelineTask* task) {
    if (_closed) {
        return Status::InternalError("WorkTaskQueue closed");
    }
    auto level = _compute_level(task->get_runtime_ns());
    std::unique_lock<std::mutex> lock(_work_size_mutex);

    // update empty queue's  runtime, to avoid too high priority
    if (_sub_queues[level].empty() &&
        _queue_level_min_vruntime > _sub_queues[level].get_vruntime()) {
        _sub_queues[level].adjust_runtime(_queue_level_min_vruntime);
    }

    _sub_queues[level].push_back(task);
    _total_task_size++;
    _wait_task.notify_one();
    return Status::OK();
}

MultiCoreTaskQueue::~MultiCoreTaskQueue() = default;

MultiCoreTaskQueue::MultiCoreTaskQueue(int core_size) : TaskQueue(core_size), _closed(false) {
    _prio_task_queue_list =
            std::make_shared<std::vector<std::unique_ptr<PriorityTaskQueue>>>(core_size);
    for (int i = 0; i < core_size; i++) {
        (*_prio_task_queue_list)[i] = std::make_unique<PriorityTaskQueue>();
    }
}

void MultiCoreTaskQueue::close() {
    if (_closed) {
        return;
    }
    _closed = true;
    for (int i = 0; i < _core_size; ++i) {
        (*_prio_task_queue_list)[i]->close();
    }
    std::atomic_store(&_prio_task_queue_list,
                      std::shared_ptr<std::vector<std::unique_ptr<PriorityTaskQueue>>>(nullptr));
}

PipelineTask* MultiCoreTaskQueue::take(int core_id) {
    PipelineTask* task = nullptr;
    auto prio_task_queue_list =
            std::atomic_load_explicit(&_prio_task_queue_list, std::memory_order_relaxed);
    while (!_closed) {
        DCHECK(prio_task_queue_list->size() > core_id)
                << " list size: " << prio_task_queue_list->size() << " core_id: " << core_id
                << " _core_size: " << _core_size << " _next_core: " << _next_core.load();
        task = (*prio_task_queue_list)[core_id]->try_take(false);
        if (task) {
            task->set_core_id(core_id);
            break;
        }
        task = _steal_take(core_id, *prio_task_queue_list);
        if (task) {
            break;
        }
        task = (*prio_task_queue_list)[core_id]->take(WAIT_CORE_TASK_TIMEOUT_MS /* timeout_ms */);
        if (task) {
            task->set_core_id(core_id);
            break;
        }
    }
    if (task) {
        task->pop_out_runnable_queue();
    }
    return task;
}

PipelineTask* MultiCoreTaskQueue::_steal_take(
        int core_id, std::vector<std::unique_ptr<PriorityTaskQueue>>& prio_task_queue_list) {
    DCHECK(core_id < _core_size);
    int next_id = core_id;
    for (int i = 1; i < _core_size; ++i) {
        ++next_id;
        if (next_id == _core_size) {
            next_id = 0;
        }
        DCHECK(next_id < _core_size);
        auto task = prio_task_queue_list[next_id]->try_take(true);
        if (task) {
            task->set_core_id(next_id);
            return task;
        }
    }
    return nullptr;
}

Status MultiCoreTaskQueue::push_back(PipelineTask* task) {
    int core_id = task->get_previous_core_id();
    if (core_id < 0) {
        core_id = _next_core.fetch_add(1) % _core_size;
    }
    return push_back(task, core_id);
}

Status MultiCoreTaskQueue::push_back(PipelineTask* task, int core_id) {
    DCHECK(core_id < _core_size);
    task->put_in_runnable_queue();
    auto prio_task_queue_list =
            std::atomic_load_explicit(&_prio_task_queue_list, std::memory_order_relaxed);
    return (*prio_task_queue_list)[core_id]->push(task);
}

void MultiCoreTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    task->inc_runtime_ns(time_spent);
    auto prio_task_queue_list =
            std::atomic_load_explicit(&_prio_task_queue_list, std::memory_order_relaxed);
    (*prio_task_queue_list)[task->get_core_id()]->inc_sub_queue_runtime(task->get_queue_level(),
                                                                        time_spent);
}

} // namespace doris::pipeline