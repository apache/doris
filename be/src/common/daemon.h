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

#include <vector>

#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"
#include "util/thread.h"

namespace doris {

class Daemon {
public:
    Daemon() : _stop_background_threads_latch(1) {}
    ~Daemon() = default;

    // Start background threads
    void start();

    // Stop background threads
    void stop();

private:
    void tcmalloc_gc_thread();
    void memory_maintenance_thread();
    void memory_gc_thread();
    void memtable_memory_refresh_thread();
    void calculate_metrics_thread();
    void je_purge_dirty_pages_thread() const;
    void cache_adjust_capacity_thread();
    void cache_prune_stale_thread();
    void report_runtime_query_statistics_thread();
    void be_proc_monitor_thread();

    CountDownLatch _stop_background_threads_latch;
    std::vector<scoped_refptr<Thread>> _threads;
};
} // namespace doris
