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
#include <string>

#include "common/logging.h"
#include "pipeline/pipeline_task.h"

namespace doris {
namespace pipeline {

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
    _queue_level_min_vruntime = min_vruntime;

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

int PriorityTaskQueue::task_size() {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    return _total_task_size;
}

MultiCoreTaskQueue::~MultiCoreTaskQueue() = default;

MultiCoreTaskQueue::MultiCoreTaskQueue(size_t core_size) : TaskQueue(core_size), _closed(false) {
    _prio_task_queue_list.reset(new PriorityTaskQueue[core_size]);
}

void MultiCoreTaskQueue::close() {
    _closed = true;
    for (int i = 0; i < _core_size; ++i) {
        _prio_task_queue_list[i].close();
    }
}

PipelineTask* MultiCoreTaskQueue::take(size_t core_id) {
    PipelineTask* task = nullptr;
    while (!_closed) {
        task = _prio_task_queue_list[core_id].try_take(false);
        if (task) {
            task->set_core_id(core_id);
            break;
        }
        task = _steal_take(core_id);
        if (task) {
            break;
        }
        task = _prio_task_queue_list[core_id].take(WAIT_CORE_TASK_TIMEOUT_MS /* timeout_ms */);
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

PipelineTask* MultiCoreTaskQueue::_steal_take(size_t core_id) {
    DCHECK(core_id < _core_size);
    size_t next_id = core_id;
    for (size_t i = 1; i < _core_size; ++i) {
        ++next_id;
        if (next_id == _core_size) {
            next_id = 0;
        }
        DCHECK(next_id < _core_size);
        auto task = _prio_task_queue_list[next_id].try_take(true);
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

Status MultiCoreTaskQueue::push_back(PipelineTask* task, size_t core_id) {
    DCHECK(core_id < _core_size);
    task->put_in_runnable_queue();
    return _prio_task_queue_list[core_id].push(task);
}

bool TaskGroupTaskQueue::TaskGroupSchedEntityComparator::operator()(
        const taskgroup::TGPTEntityPtr& lhs_ptr, const taskgroup::TGPTEntityPtr& rhs_ptr) const {
    auto lhs_val = lhs_ptr->vruntime_ns();
    auto rhs_val = rhs_ptr->vruntime_ns();
    if (lhs_val != rhs_val) {
        return lhs_val < rhs_val;
    } else {
        auto l_share = lhs_ptr->cpu_share();
        auto r_share = rhs_ptr->cpu_share();
        if (l_share != r_share) {
            return l_share < r_share;
        } else {
            return lhs_ptr->task_group_id() < rhs_ptr->task_group_id();
        }
    }
}

TaskGroupTaskQueue::TaskGroupTaskQueue(size_t core_size)
        : TaskQueue(core_size), _min_tg_entity(nullptr) {
    _empty_pip_task->set_empty_task(true);
    _empty_pip_task->set_task_queue(this);
    _empty_pip_task->set_task_group_entity(_empty_group_entity);
    _empty_group_entity->set_empty_group_entity(true);
}

TaskGroupTaskQueue::~TaskGroupTaskQueue() {
    delete _empty_group_entity;
    delete _empty_pip_task;
}

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
    task->put_in_runnable_queue();
    auto* entity = task->get_task_group_entity();
    std::unique_lock<std::mutex> lock(_rs_mutex);
    entity->task_queue()->emplace(task);
    if (_group_entities.find(entity) == _group_entities.end()) {
        _enqueue_task_group<from_executor>(entity);
        if (_enable_cpu_hard_limit) {
            reset_empty_group_entity();
        }
    }
    _wait_task.notify_one();
    return Status::OK();
}

// TODO pipeline support steal
PipelineTask* TaskGroupTaskQueue::take(size_t core_id) {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    taskgroup::TGPTEntityPtr entity = nullptr;
    while (entity == nullptr) {
        if (_closed) {
            return nullptr;
        }
        if (_group_entities.empty()) {
            _wait_task.wait(lock);
        } else {
            entity = _next_tg_entity();
            if (!entity) {
                _wait_task.wait_for(lock, std::chrono::milliseconds(WAIT_CORE_TASK_TIMEOUT_MS));
            }
        }
    }
    if (entity->is_empty_group_entity()) {
        return _empty_pip_task;
    }
    DCHECK(entity->task_size() > 0);
    if (entity->task_size() == 1) {
        _dequeue_task_group(entity);
        if (_enable_cpu_hard_limit) {
            reset_empty_group_entity();
        }
    }
    auto task = entity->task_queue()->front();
    if (task) {
        entity->task_queue()->pop();
        task->pop_out_runnable_queue();
    }
    return task;
}

template <bool from_worker>
void TaskGroupTaskQueue::_enqueue_task_group(taskgroup::TGPTEntityPtr tg_entity) {
    _total_cpu_share += tg_entity->cpu_share();
    if constexpr (!from_worker) {
        /**
         * If a task group entity leaves task queue for a long time, its v runtime will be very
         * small. This can cause it to preempt too many execution time. So, in order to avoid this
         * situation, it is necessary to adjust the task group's v runtime.
         * */
        auto old_v_ns = tg_entity->vruntime_ns();
        auto* min_entity = _min_tg_entity.load();
        if (min_entity) {
            auto min_tg_v = min_entity->vruntime_ns();
            auto ideal_r = _ideal_runtime_ns(tg_entity) / 2;
            uint64_t new_vruntime_ns = min_tg_v > ideal_r ? min_tg_v - ideal_r : min_tg_v;
            if (new_vruntime_ns > old_v_ns) {
                tg_entity->adjust_vruntime_ns(new_vruntime_ns);
            }
        } else if (old_v_ns < _min_tg_v_runtime_ns) {
            tg_entity->adjust_vruntime_ns(_min_tg_v_runtime_ns);
        }
    }
    _group_entities.emplace(tg_entity);
    VLOG_DEBUG << "enqueue tg " << tg_entity->debug_string()
               << ", group entity size: " << _group_entities.size();
    _update_min_tg();
}

void TaskGroupTaskQueue::_dequeue_task_group(taskgroup::TGPTEntityPtr tg_entity) {
    _total_cpu_share -= tg_entity->cpu_share();
    _group_entities.erase(tg_entity);
    VLOG_DEBUG << "dequeue tg " << tg_entity->debug_string()
               << ", group entity size: " << _group_entities.size();
    _update_min_tg();
}

void TaskGroupTaskQueue::_update_min_tg() {
    auto* min_entity = _next_tg_entity();
    _min_tg_entity = min_entity;
    if (min_entity) {
        auto min_v_runtime = min_entity->vruntime_ns();
        if (min_v_runtime > _min_tg_v_runtime_ns) {
            _min_tg_v_runtime_ns = min_v_runtime;
        }
    }
}

// like sched_fair.c calc_delta_fair, THREAD_TIME_SLICE maybe a dynamic value.
uint64_t TaskGroupTaskQueue::_ideal_runtime_ns(taskgroup::TGPTEntityPtr tg_entity) const {
    return PipelineTask::THREAD_TIME_SLICE * _core_size * tg_entity->cpu_share() / _total_cpu_share;
}

taskgroup::TGPTEntityPtr TaskGroupTaskQueue::_next_tg_entity() {
    taskgroup::TGPTEntityPtr res = nullptr;
    for (auto* entity : _group_entities) {
        res = entity;
        break;
    }
    return res;
}

void TaskGroupTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    auto* entity = task->get_task_group_entity();
    auto find_entity = _group_entities.find(entity);
    bool is_in_queue = find_entity != _group_entities.end();
    VLOG_DEBUG << "update_statistics " << entity->debug_string() << ", in queue:" << is_in_queue;
    if (is_in_queue) {
        _group_entities.erase(entity);
    }
    entity->incr_runtime_ns(time_spent);
    if (is_in_queue) {
        _group_entities.emplace(entity);
        _update_min_tg();
    }
}

void TaskGroupTaskQueue::update_tg_cpu_share(const taskgroup::TaskGroupInfo& task_group_info,
                                             taskgroup::TGPTEntityPtr entity) {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    bool is_in_queue = _group_entities.find(entity) != _group_entities.end();
    if (is_in_queue) {
        _group_entities.erase(entity);
        _total_cpu_share -= entity->cpu_share();
    }
    entity->check_and_update_cpu_share(task_group_info);
    if (is_in_queue) {
        _group_entities.emplace(entity);
        _total_cpu_share += entity->cpu_share();
    }
}

void TaskGroupTaskQueue::reset_empty_group_entity() {
    int user_g_cpu_hard_limit = 0;
    bool contains_empty_group = false;
    for (auto* entity : _group_entities) {
        if (!entity->is_empty_group_entity()) {
            user_g_cpu_hard_limit += entity->cpu_share();
        } else {
            contains_empty_group = true;
        }
    }

    // 0 <= user_g_cpu_hard_limit <= 100, bound by FE
    // user_g_cpu_hard_limit = 0 means no group exists
    int empty_group_cpu_share = 100 - user_g_cpu_hard_limit;
    if (empty_group_cpu_share > 0 && empty_group_cpu_share < 100 && !contains_empty_group) {
        _empty_group_entity->update_empty_cpu_share(empty_group_cpu_share);
        _enqueue_task_group<true>(_empty_group_entity);
    } else if ((empty_group_cpu_share == 0 || empty_group_cpu_share == 100) &&
               contains_empty_group) {
        // no need to update empty group here
        // only update empty group's cpu share when exec enqueue
        _dequeue_task_group(_empty_group_entity);
    }
}

} // namespace pipeline
} // namespace doris