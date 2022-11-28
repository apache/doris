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

#include <memory>
#include <vector>

#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"
#include "util/thread.h"

namespace doris {

struct StorePath;
class Thread;

class Daemon {
public:
    Daemon() : _stop_background_threads_latch(1) {}

    // Initialises logging, flags etc. Callers that want to override default gflags
    // variables should do so before calling this method; no logging should be
    // performed until after this method returns.
    void init(int argc, char** argv, const std::vector<StorePath>& paths);

    // Start background threads
    void start();

    // Stop background threads
    void stop();

private:
    void tcmalloc_gc_thread();
    void buffer_pool_gc_thread();
    void memory_maintenance_thread();
    void load_channel_tracker_refresh_thread();
    void calculate_metrics_thread();

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _tcmalloc_gc_thread;
    // only buffer pool gc, will be removed after.
    scoped_refptr<Thread> _buffer_pool_gc_thread;
    scoped_refptr<Thread> _memory_maintenance_thread;
    scoped_refptr<Thread> _load_channel_tracker_refresh_thread;
    scoped_refptr<Thread> _calculate_metrics_thread;
};
} // namespace doris
