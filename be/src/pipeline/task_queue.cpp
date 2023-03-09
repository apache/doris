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
#include "runtime/task_group/task_group.h"

namespace doris {
namespace pipeline {

PipelineTask* SubWorkTaskQueue::try_take(bool is_steal) {
    if (_queue.empty()) {
        return nullptr;
    }
    auto task = _queue.front();
    if (!task->can_steal() && is_steal) {
        return nullptr;
    }
    ++_schedule_time;
    _queue.pop();
    return task;
}

////////////////////  WorkTaskQueue ////////////////////

NormalWorkTaskQueue::NormalWorkTaskQueue() : _closed(false) {
    double factor = 1;
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        _sub_queues[i].set_factor_for_normal(factor);
        factor *= LEVEL_QUEUE_TIME_FACTOR;
    }

    int i = 0;
    _task_schedule_limit[i] = BASE_LIMIT * (i + 1);
    for (i = 1; i < SUB_QUEUE_LEVEL - 1; ++i) {
        _task_schedule_limit[i] = _task_schedule_limit[i - 1] + BASE_LIMIT * (i + 1);
    }
}

void NormalWorkTaskQueue::close() {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    _closed = true;
    _wait_task.notify_all();
}

PipelineTask* NormalWorkTaskQueue::try_take_unprotected(bool is_steal) {
    if (_total_task_size == 0 || _closed) {
        return nullptr;
    }
    double normal_schedule_times[SUB_QUEUE_LEVEL];
    double min_schedule_time = 0;
    int idx = -1;
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        normal_schedule_times[i] = _sub_queues[i].schedule_time_after_normal();
        if (!_sub_queues[i].empty()) {
            if (idx == -1 || normal_schedule_times[i] < min_schedule_time) {
                idx = i;
                min_schedule_time = normal_schedule_times[i];
            }
        }
    }
    DCHECK(idx != -1);
    // update empty queue's schedule time, to avoid too high priority
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        if (_sub_queues[i].empty() && normal_schedule_times[i] < min_schedule_time) {
            _sub_queues[i]._schedule_time = min_schedule_time / _sub_queues[i]._factor_for_normal;
        }
    }

    auto task = _sub_queues[idx].try_take(is_steal);
    if (task) {
        _total_task_size--;
    }
    return task;
}

int NormalWorkTaskQueue::_compute_level(PipelineTask* task) {
    uint32_t schedule_time = task->total_schedule_time();
    for (int i = 0; i < SUB_QUEUE_LEVEL - 1; ++i) {
        if (schedule_time <= _task_schedule_limit[i]) {
            return i;
        }
    }
    return SUB_QUEUE_LEVEL - 1;
}

PipelineTask* NormalWorkTaskQueue::try_take(bool is_steal) {
    // TODO other efficient lock? e.g. if get lock fail, return null_ptr
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    return try_take_unprotected(is_steal);
}

PipelineTask* NormalWorkTaskQueue::take(uint32_t timeout_ms) {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    auto task = try_take_unprotected(false);
    if (task) {
        return task;
    } else {
        if (timeout_ms > 0) {
            _wait_task.wait_for(lock, std::chrono::milliseconds(timeout_ms));
        } else {
            _wait_task.wait(lock);
        }
        return try_take_unprotected(false);
    }
}

Status NormalWorkTaskQueue::push(PipelineTask* task) {
    if (_closed) {
        return Status::InternalError("WorkTaskQueue closed");
    }
    auto level = _compute_level(task);
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    _sub_queues[level].push_back(task);
    _total_task_size++;
    _wait_task.notify_one();
    return Status::OK();
}

////////////////// TaskQueue ////////////


////////////////// Resource Group ////////

bool TaskGroupTaskQueue::TaskGroupSchedEntityComparator::operator()(
        const taskgroup::TGEntityPtr& lhs_ptr, const taskgroup::TGEntityPtr& rhs_ptr) const {
    int64_t lhs_val = lhs_ptr->vruntime_ns();
    int64_t rhs_val = rhs_ptr->vruntime_ns();
    return lhs_val < rhs_val;
//    sr
//    if (lhs_val != rhs_val) {
//        return lhs_val < rhs_val;
//    }
//    return lhs_ptr < rhs_ptr;
}

TaskGroupTaskQueue::TaskGroupTaskQueue(size_t core_size) : TaskQueue(core_size) {}

TaskGroupTaskQueue::~TaskGroupTaskQueue() = default;

void TaskGroupTaskQueue::close() {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    _closed = true;
    _wait_task.notify_all();
}

Status TaskGroupTaskQueue::push_back(PipelineTask* task) {
    return _push_back<false>(task);
}

Status TaskGroupTaskQueue::push_back(PipelineTask* task, size_t core_id) {
    return _push_back<true>(task);
}

template <bool from_executor>
Status TaskGroupTaskQueue::_push_back(PipelineTask* task) {
    auto* entry = task->get_task_group()->task_entity();
    std::unique_lock<std::mutex> lock(_rs_mutex);
    entry->push_back(task);
    if (_groups.find(entry) == _groups.end()) {
        _enqueue_task_group<from_executor>(entry);
    }
    _wait_task.notify_one();
    return Status::OK();
}

// TODO pipeline support steal
PipelineTask* TaskGroupTaskQueue::take(size_t core_id) {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    taskgroup::TGEntityPtr entry = nullptr;
    while (entry == nullptr) {
        if (_closed) {
            return nullptr;
        }
        if (_groups.empty()) {
            _wait_task.wait(lock);
        } else {
            entry = _next_ts_entity();
            if (!entry) {
                _wait_task.wait_for(lock, std::chrono::milliseconds(WAIT_CORE_TASK_TIMEOUT_MS));
            }
        }
    }
    DCHECK(entry->task_size() > 0);
    if (entry->task_size() == 1) {
        _dequeue_task_group(entry);
    }
    return entry->take();
}

template <bool from_worker>
void TaskGroupTaskQueue::_enqueue_task_group(taskgroup::TGEntityPtr ts_entity) {
    _total_cpu_share += ts_entity->cpu_share();
    if constexpr (!from_worker) {
        auto* min_entity = _min_ts_entity.load();
        if (!min_entity) {
            int64_t new_vruntime_ns = min_entity->vruntime_ns() - _ideal_runtime_ns(ts_entity) / 2;
            if (new_vruntime_ns > ts_entity->vruntime_ns()) {
                ts_entity->adjust_vruntime_ns(new_vruntime_ns);
            }
        }
    }
    _groups.emplace(ts_entity);
    _update_min_rg();
}

void TaskGroupTaskQueue::_dequeue_task_group(taskgroup::TGEntityPtr ts_entity) {
    _total_cpu_share -= ts_entity->cpu_share();
    _groups.erase(ts_entity);
    _update_min_rg();
}

void TaskGroupTaskQueue::_update_min_rg() {
    auto* min_entity = _next_ts_entity();
    _min_ts_entity = min_entity;
}

// like sched_fair.c calc_delta_fair
int64_t TaskGroupTaskQueue::_ideal_runtime_ns(taskgroup::TGEntityPtr ts_entity) const {
    // _groups.size() -> _core_size?
    return SCHEDULE_PERIOD_PER_WG_NS * _groups.size() * ts_entity->cpu_share() / _total_cpu_share;
}

taskgroup::TGEntityPtr TaskGroupTaskQueue::_next_ts_entity() {
    taskgroup::TGEntityPtr res = nullptr;
    for (auto* entry: _groups) {
        res = entry;
        break;
    }
    return res;
}

void TaskGroupTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    auto* group = task->get_task_group();
    auto* entry = group->task_entity();
    bool is_in_queue = _groups.find(entry) != _groups.end();
    if (is_in_queue) {
        _groups.erase(entry);
    }
    entry->incr_runtime_ns(time_spent);
    if (is_in_queue) {
        _groups.emplace(entry);
        _update_min_rg();
    }
}


TaskQueue::~TaskQueue() = default;

NormalTaskQueue::~NormalTaskQueue() = default;

NormalTaskQueue::NormalTaskQueue(size_t core_size) : TaskQueue(core_size), _closed(false) {
//    for (int i = 0; i < core_size; ++i) {
//        _async_queue.emplace_back(new NormalWorkTaskQueue());
//    }
    _async_queue.reset(new NormalWorkTaskQueue[core_size]);
}

void NormalTaskQueue::close() {
    _closed = true;
    for (int i = 0; i < _core_size; ++i) {
        _async_queue[i].close();
    }
}

PipelineTask* NormalTaskQueue::take(size_t core_id) {
    PipelineTask* task = nullptr;
    while (!_closed) {
        task = _async_queue[core_id].try_take(false);
        if (task) {
            break;
        }
        task = _steal_take(core_id);
        if (task) {
            break;
        }
        task = _async_queue[core_id].take(WAIT_CORE_TASK_TIMEOUT_MS /* timeout_ms */);
        if (task) {
            break;
        }
    }
    if (task) {
        task->pop_out_runnable_queue();
    }
    return task;
}

PipelineTask* NormalTaskQueue::_steal_take(size_t core_id) {
    DCHECK(core_id < _core_size);
    size_t next_id = core_id;
    for (size_t i = 1; i < _core_size; ++i) {
        ++next_id;
        if (next_id == _core_size) {
            next_id = 0;
        }
        DCHECK(next_id < _core_size);
        auto task = _async_queue[next_id].try_take(true);
        if (task) {
            return task;
        }
    }
    return nullptr;
}

Status NormalTaskQueue::push_back(PipelineTask* task) {
    int core_id = task->get_previous_core_id();
    if (core_id < 0) {
        core_id = _next_core.fetch_add(1) % _core_size;
    }
    return push_back(task, core_id);
}

Status NormalTaskQueue::push_back(PipelineTask* task, size_t core_id) {
    DCHECK(core_id < _core_size);
    task->put_in_runnable_queue();
    return _async_queue[core_id].push(task);
}

} // namespace pipeline
} // namespace doris