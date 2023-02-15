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

#include "util/priority_work_stealing_thread_pool.hpp"

#include <cstdio>
#include <mutex>
#include <utility>

#include "util/blocking_priority_queue.hpp"
#include "util/thread.h"

namespace doris {
PriorityWorkStealingThreadPool::PriorityWorkStealingThreadPool(uint32_t num_threads,
                                                               uint32_t num_queues,
                                                               uint32_t queue_size,
                                                               std::string name)
        : PriorityThreadPool(0, 0, std::move(name)) {
    DCHECK_GT(num_queues, 0);
    DCHECK_GE(num_threads, num_queues);
    // init _work_queues first because the work thread needs it
    for (int i = 0; i < num_queues; ++i) {
        _work_queues.emplace_back(std::make_shared<BlockingPriorityQueue<Task>>(queue_size));
    }
    for (int i = 0; i < num_threads; ++i) {
        _threads.create_thread(std::bind<void>(
                std::mem_fn(&PriorityWorkStealingThreadPool::work_thread), this, i));
    }
}

PriorityWorkStealingThreadPool::~PriorityWorkStealingThreadPool() {
    shutdown();
    join();
}

bool PriorityWorkStealingThreadPool::offer(Task task) {
    return _work_queues[task.queue_id]->blocking_put(std::move(task));
}

bool PriorityWorkStealingThreadPool::offer(WorkFunction func) {
    return offer({0, func, 0});
}

void PriorityWorkStealingThreadPool::shutdown() {
    PriorityThreadPool::shutdown();
    for (auto work_queue : _work_queues) {
        work_queue->shutdown();
    }
}

uint32_t PriorityWorkStealingThreadPool::get_queue_size() const {
    uint32_t size = 0;
    for (auto work_queue : _work_queues) {
        size += work_queue->get_size();
    }
    return size;
}

void PriorityWorkStealingThreadPool::drain_and_shutdown() {
    {
        std::unique_lock l(_lock);
        while (get_queue_size() != 0) {
            _empty_cv.wait(l);
        }
    }
    shutdown();
    join();
}

void PriorityWorkStealingThreadPool::work_thread(int thread_id) {
    auto queue_id = thread_id % _work_queues.size();
    auto steal_queue_id = (queue_id + 1) % _work_queues.size();
    while (!is_shutdown()) {
        Task task;
        // avoid blocking get
        bool is_other_queues_empty = true;
        // steal work in round-robin if nothing to do
        while (_work_queues[queue_id]->get_size() == 0 && queue_id != steal_queue_id &&
               !is_shutdown()) {
            if (_work_queues[steal_queue_id]->non_blocking_get(&task)) {
                is_other_queues_empty = false;
                task.work_function();
            }
            steal_queue_id = (steal_queue_id + 1) % _work_queues.size();
        }
        if (queue_id == steal_queue_id) {
            steal_queue_id = (steal_queue_id + 1) % _work_queues.size();
        }
        if (is_other_queues_empty &&
            _work_queues[queue_id]->blocking_get(
                    &task, config::doris_blocking_priority_queue_wait_timeout_ms)) {
            task.work_function();
        }
        if (_work_queues[queue_id]->get_size() == 0) {
            _empty_cv.notify_all();
        }
    }
}
} // namespace doris
