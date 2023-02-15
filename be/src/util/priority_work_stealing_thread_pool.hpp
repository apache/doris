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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "util/priority_thread_pool.hpp"

namespace doris {
template <typename T>
class BlockingPriorityQueue;

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
    PriorityWorkStealingThreadPool(uint32_t num_threads, uint32_t num_queues, uint32_t queue_size,
                                   std::string name);
    virtual ~PriorityWorkStealingThreadPool();

    bool offer(Task task) override;
    bool offer(WorkFunction func) override;

    // Shuts the thread pool down, causing the work queue to cease accepting offered work
    // and the worker threads to terminate once they have processed their current work item.
    // Returns once the shutdown flag has been set, does not wait for the threads to
    // terminate.
    void shutdown() override;

    // Blocks until the work queue is empty, and then calls shutdown to stop the worker
    // threads and Join to wait until they are finished.
    // Any work Offer()'ed during DrainAndshutdown may or may not be processed.
    void drain_and_shutdown() override;

    uint32_t get_queue_size() const override;

private:
    // Driver method for each thread in the pool. Continues to read work from the queue
    // until the pool is shutdown.
    void work_thread(int thread_id);

private:
    // Queue on which work items are held until a thread is available to process them in
    // FIFO order.
    std::vector<std::shared_ptr<BlockingPriorityQueue<Task>>> _work_queues;
};

} // namespace doris
