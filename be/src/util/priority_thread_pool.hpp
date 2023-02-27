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
#include "util/thread.h"
#include "util/thread_group.h"

namespace doris {

// Simple threadpool which processes items (of type T) in parallel which were placed on a
// blocking queue by Offer(). Each item is processed by a single user-supplied method.
class PriorityThreadPool {
public:
    // Signature of a work-processing function. Takes the integer id of the thread which is
    // calling it (ids run from 0 to num_threads - 1) and a reference to the item to
    // process.
    typedef std::function<void()> WorkFunction;

    struct Task {
    public:
        int priority;
        WorkFunction work_function;
        int queue_id;
        bool operator<(const Task& o) const { return priority < o.priority; }

        Task& operator++() {
            priority += 2;
            return *this;
        }
    };

    // Creates a new thread pool and start num_threads threads.
    //  -- num_threads: how many threads are part of this pool
    //  -- queue_size: the maximum size of the queue on which work items are offered. If the
    //     queue exceeds this size, subsequent calls to Offer will block until there is
    //     capacity available.
    PriorityThreadPool(uint32_t num_threads, uint32_t queue_size, const std::string& name)
            : _work_queue(queue_size), _shutdown(false), _name(name) {
        for (int i = 0; i < num_threads; ++i) {
            _threads.create_thread(
                    std::bind<void>(std::mem_fn(&PriorityThreadPool::work_thread), this, i));
        }
    }

    // Destructor ensures that all threads are terminated before this object is freed
    // (otherwise they may continue to run and reference member variables)
    virtual ~PriorityThreadPool() {
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
    // after DrainAndshutdown returns).
    //
    // Returns true if the work item was successfully added to the queue, false otherwise
    // (which typically means that the thread pool has already been shut down).
    virtual bool offer(Task task) { return _work_queue.blocking_put(task); }

    virtual bool offer(WorkFunction func) {
        PriorityThreadPool::Task task = {0, func, 0};
        return _work_queue.blocking_put(task);
    }

    // Shuts the thread pool down, causing the work queue to cease accepting offered work
    // and the worker threads to terminate once they have processed their current work item.
    // Returns once the shutdown flag has been set, does not wait for the threads to
    // terminate.
    virtual void shutdown() {
        _shutdown = true;
        _work_queue.shutdown();
    }

    // Blocks until all threads are finished. shutdown does not need to have been called,
    // since it may be called on a separate thread.
    virtual void join() { _threads.join_all(); }

    virtual uint32_t get_queue_size() const { return _work_queue.get_size(); }

    // Blocks until the work queue is empty, and then calls shutdown to stop the worker
    // threads and Join to wait until they are finished.
    // Any work Offer()'ed during DrainAndshutdown may or may not be processed.
    virtual void drain_and_shutdown() {
        {
            std::unique_lock<std::mutex> l(_lock);
            while (_work_queue.get_size() != 0) {
                _empty_cv.wait(l);
            }
        }
        shutdown();
        join();
    }

protected:
    virtual bool is_shutdown() { return _shutdown; }

    // Collection of worker threads that process work from the queue.
    ThreadGroup _threads;

    // Guards _empty_cv
    std::mutex _lock;

    // Signalled when the queue becomes empty
    std::condition_variable _empty_cv;

private:
    // Driver method for each thread in the pool. Continues to read work from the queue
    // until the pool is shutdown.
    void work_thread(int thread_id) {
        Thread::set_self_name(_name.c_str());
        while (!is_shutdown()) {
            Task task;
            if (_work_queue.blocking_get(&task)) {
                task.work_function();
            }
            if (_work_queue.get_size() == 0) {
                _empty_cv.notify_all();
            }
        }
    }

    // Queue on which work items are held until a thread is available to process them in
    // FIFO order.
    BlockingPriorityQueue<Task> _work_queue;

    // Set to true when threads should stop doing work and terminate.
    std::atomic<bool> _shutdown;
    std::string _name;
};

} // namespace doris
