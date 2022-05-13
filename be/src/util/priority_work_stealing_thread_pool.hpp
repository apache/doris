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

#include <mutex>
#include <thread>

#include "util/blocking_priority_queue.hpp"
#include "util/thread_group.h"

namespace doris {

// Work-Stealing threadpool which processes items (of type T) in parallel which were placed on multi
// blocking queues by Offer(). Each item is processed by a single user-supplied method.
class PriorityWorkStealingThreadPool : public PriorityThreadPool {
public:
    // Creates a new thread pool and start num_threads threads.
    //  -- num_threads: how many threads are part of this pool
    //  -- num_queues: how many queues are part of this pool
    //  -- queue_size: the maximum size of the queue on which work items are offered. If the
    //     queue exceeds this size, subsequent calls to Offer will block until there is
    //     capacity available.
    //  -- work_function: the function to run every time an item is consumed from the queue
    PriorityWorkStealingThreadPool(uint32_t num_threads, uint32_t num_queues, uint32_t queue_size)
            : PriorityThreadPool(0, 0) {
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

    // Blocking operation that puts a work item on the queue. If the queue is full, blocks
    // until there is capacity available.
    //
    // 'work' is copied into the work queue, but may be referenced at any time in the
    // future. Therefore the caller needs to ensure that any data referenced by work (if T
    // is, e.g., a pointer type) remains valid until work has been processed, and it's up to
    // the caller to provide their own signalling mechanism to detect this (or to wait until
    // after DrainAndshutdown returns).
    //
    // Returns true if the work item was successfully added to the queue, false otherwise
    // (which typically means that the thread pool has already been shut down).
    bool offer(Task task) override { return _work_queues[task.queue_id]->blocking_put(task); }

    bool offer(WorkFunction func) override {
        PriorityThreadPool::Task task = {0, func, 0};
        return _work_queues[task.queue_id]->blocking_put(task);
    }

    // Shuts the thread pool down, causing the work queue to cease accepting offered work
    // and the worker threads to terminate once they have processed their current work item.
    // Returns once the shutdown flag has been set, does not wait for the threads to
    // terminate.
    void shutdown() override {
        PriorityThreadPool::shutdown();
        for (auto work_queue : _work_queues) {
            work_queue->shutdown();
        }
    }

    // Blocks until all threads are finished. shutdown does not need to have been called,
    // since it may be called on a separate thread.
    void join() override { _threads.join_all(); }

    uint32_t get_queue_size() const override {
        uint32_t size = 0;
        for (auto work_queue : _work_queues) {
            size += work_queue->get_size();
        }
        return size;
    }

    // Blocks until the work queue is empty, and then calls shutdown to stop the worker
    // threads and Join to wait until they are finished.
    // Any work Offer()'ed during DrainAndshutdown may or may not be processed.
    void drain_and_shutdown() override {
        {
            std::unique_lock<std::mutex> l(_lock);
            while (get_queue_size() != 0) {
                _empty_cv.wait(l);
            }
        }
        shutdown();
        join();
    }

private:
    // Driver method for each thread in the pool. Continues to read work from the queue
    // until the pool is shutdown.
    void work_thread(int thread_id) {
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

    // Queue on which work items are held until a thread is available to process them in
    // FIFO order.
    std::vector<std::shared_ptr<BlockingPriorityQueue<Task>>> _work_queues;

    // Collection of worker threads that process work from the queues.
    ThreadGroup _threads;

    // Guards _empty_cv
    std::mutex _lock;

    // Signalled when the queue becomes empty
    std::condition_variable _empty_cv;
};

} // namespace doris