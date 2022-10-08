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

#include "common/status.h"
#include "util/blocking_queue.hpp"
#include "vec/exec/scan/scanner_context.h"

namespace doris::vectorized {

// Responsible for the scheduling and execution of all Scanners of a BE node.
// ScannerScheduler has two types of thread pools:
// 1. Scheduling thread pool
//     Responsible for Scanner scheduling.
//     A set of Scanners for a query will be encapsulated into a ScannerContext
//     and submitted to the ScannerScheduler's scheduling queue.
//     There are multiple scheduling queues in ScannerScheduler, and each scheduling queue
//     is handled by a scheduling thread.
//     The scheduling thread is scheduled in granularity of ScannerContext,
//     that is, a group of Scanners in a ScannerContext are scheduled at a time.
//
//2. Execution thread pool
//     The scheduling thread will submit the Scanners selected from the ScannerContext
//     to the execution thread pool to do the actual scan task.
//     Each Scanner will act as a producer, read a group of blocks and put them into
//     the corresponding block queue.
//     The corresponding ScanNode will act as a consumer to consume blocks from the block queue.
class Env;
class ScannerScheduler {
public:
    ScannerScheduler();
    ~ScannerScheduler();

    Status init(ExecEnv* env);

    Status submit(ScannerContext* ctx);

private:
    // scheduling thread function
    void _schedule_thread(int queue_id);
    // schedule scanners in a certain ScannerContext
    void _schedule_scanners(ScannerContext* ctx);
    // execution thread function
    void _scanner_scan(ScannerScheduler* scheduler, ScannerContext* ctx, VScanner* scanner);

private:
    // Scheduling queue number.
    // TODO: make it configurable.
    static const int QUEUE_NUM = 4;
    // The ScannerContext will be submitted to the pending queue roundrobin.
    // _queue_idx pointer to the current queue.
    // The scheduler thread will take ctx from pending queue, schedule it,
    // and put it to the _scheduling_map.
    // If any scanner finish, it will take ctx from and put it to pending queue again.
    std::atomic_int _queue_idx = {0};
    BlockingQueue<ScannerContext*>** _pending_queues;

    // scheduling thread pool
    std::unique_ptr<ThreadPool> _scheduler_pool;
    // execution thread pool
    // _local_scan_thread_pool is for local scan task(typically, olap scanner)
    // _remote_scan_thread_pool is for remote scan task(cold data on s3, hdfs, etc.)
    std::unique_ptr<PriorityThreadPool> _local_scan_thread_pool;
    std::unique_ptr<PriorityThreadPool> _remote_scan_thread_pool;

    // true is the scheduler is closed.
    std::atomic_bool _is_closed = {false};
    bool _is_init = false;
};

} // namespace doris::vectorized
