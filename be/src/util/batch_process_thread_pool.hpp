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

#ifndef DORIS_BE_SRC_COMMON_UTIL_BATCH_PROCESS_THREAD_POOL_HPP
#define DORIS_BE_SRC_COMMON_UTIL_BATCH_PROCESS_THREAD_POOL_HPP

#include <unistd.h>

#include <condition_variable>
#include <mutex>
#include <queue>

#include "common/config.h"
#include "util/blocking_priority_queue.hpp"
#include "util/stopwatch.hpp"
#include "util/thread_group.h"

namespace doris {

// Fixed capacity FIFO queue, where both blocking_get and blocking_put operations block
// if the queue is empty or full, respectively.
template <typename T>
class BatchProcessThreadPool {
public:
    // Signature of function that process task batch by batch not one by one
    typedef std::function<void(std::vector<T>)> BatchProcessFunction;

    // Creates a new thread pool and start num_threads threads.
    //  -- num_threads: how many threads are part of this pool
    //  -- queue_size: the maximum size of the queue on which work items are offered. If the
    //     queue exceeds this size, subsequent calls to Offer will block until there is
    //     capacity available.
    //  -- work_function: the function to run every time an item is consumed from the queue
    BatchProcessThreadPool(uint32_t num_threads, uint32_t queue_size, uint32_t batch_size,
                           BatchProcessFunction work_func)
            : _thread_num(num_threads),
              _work_queue(queue_size),
              _shutdown(false),
              _batch_size(batch_size),
              _work_func(work_func) {
        for (int i = 0; i < num_threads; ++i) {
            _threads.create_thread(
                    std::bind<void>(std::mem_fn(&BatchProcessThreadPool::work_thread), this, i));
        }
    }

    // Destructor ensures that all threads are terminated before this object is freed
    // (otherwise they may continue to run and reference member variables)
    ~BatchProcessThreadPool() {
        shutdown();
        join();
    }

    // Blocking operation that puts a work item on the queue. If the queue is full, blocks
    // until there is capacity available.
    //
    // 'work' is copied into the work queue, but may be referenced at any time in the
    // future. Therefore the caller needs to ensure that any data referenced by work (if T
    // is, e.g., a pointer type) remains valid until work has been processed, and it's up to
    // the caller to provide their own signalling mechanism to detect this (or to wait until
    // after DrainAndShutdown returns).
    //
    // Returns true if the work item was successfully added to the queue, false otherwise
    // (which typically means that the thread pool has already been shut down).
    bool offer(T task) { return _work_queue.blocking_put(task); }

    // Shuts the thread pool down, causing the work queue to cease accepting offered work
    // and the worker threads to terminate once they have processed their current work item.
    // Returns once the shutdown flag has been set, does not wait for the threads to
    // terminate.
    void shutdown() {
        {
            std::lock_guard<std::mutex> l(_lock);
            _shutdown = true;
        }
        _work_queue.shutdown();
    }

    // Blocks until all threads are finished. shutdown does not need to have been called,
    // since it may be called on a separate thread.
    void join() { _threads.join_all(); }

    uint32_t get_queue_size() const { return _work_queue.get_size(); }

    // Blocks until the work queue is empty, and then calls shutdown to stop the worker
    // threads and Join to wait until they are finished.
    // Any work Offer()'ed during DrainAndShutdown may or may not be processed.
    void drain_and_shutdown() {
        {
            std::unique_lock<std::mutex> l(_lock);
            while (_work_queue.get_size() != 0) {
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
        while (!is_shutdown()) {
            std::vector<T> tasks;
            T task;
            int32_t task_counter = 0;
            while (task_counter < _batch_size) {
                bool has_task = false;
                if (task_counter == 0) {
                    // the first task should blocking, or the tasks queue is empty
                    has_task = _work_queue.blocking_get(&task);
                } else {
                    // the 2rd, 3rd... task should non blocking get
                    has_task = _work_queue.non_blocking_get(&task);
                    if (!has_task) {
                        break;
                    }
                }
                if (has_task) {
                    tasks.push_back(task);
                    ++task_counter;
                }
            }
            if (!tasks.empty()) {
                _work_func(tasks);
            }
            if (_work_queue.get_size() == 0) {
                _empty_cv.notify_all();
            }
        }
    }

    // Returns value of _shutdown under a lock, forcing visibility to threads in the pool.
    bool is_shutdown() {
        std::lock_guard<std::mutex> l(_lock);
        return _shutdown;
    }

    uint32_t _thread_num;

    // Queue on which work items are held until a thread is available to process them in
    // FIFO order.
    BlockingPriorityQueue<T> _work_queue;

    // Collection of worker threads that process work from the queue.
    ThreadGroup _threads;

    // Guards _shutdown and _empty_cv
    std::mutex _lock;

    // Set to true when threads should stop doing work and terminate.
    bool _shutdown;

    // Signalled when the queue becomes empty
    std::condition_variable _empty_cv;

    uint32_t _batch_size;

    BatchProcessFunction _work_func;
};

} // namespace doris

#endif
