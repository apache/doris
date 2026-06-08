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

#include <omp.h>
#include <pthread.h>

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "util/cpu_info.h"
#include "util/thread.h"

namespace doris::segment_v2 {

// Guard that ensures the total OpenMP threads used by concurrent index builds
// never exceed the configured omp_threads_limit.
//
// Shared across all ANN index types (FAISS IVF, PQ-on-disk, etc.) so that
// concurrent builds compete for a single global budget.
class ScopedOmpThreadBudget {
public:
    // For each index build, reserve at most half of the remaining threads, at least 1 thread.
    ScopedOmpThreadBudget();
    ~ScopedOmpThreadBudget();

    ScopedOmpThreadBudget(const ScopedOmpThreadBudget&) = delete;
    ScopedOmpThreadBudget& operator=(const ScopedOmpThreadBudget&) = delete;

    static int get_omp_threads_limit();

private:
    int _reserved_threads = 1;
};

// Temporarily rename the current thread so ANN build phases are easier to spot
// in debuggers and log output.  Restores the previous name on destruction.
class ScopedThreadName {
public:
    explicit ScopedThreadName(const std::string& new_name) {
        // POSIX limits thread names to 15 visible chars plus the null terminator.
        char current_name[16] = {0};
        int ret = pthread_getname_np(pthread_self(), current_name, sizeof(current_name));
        if (ret == 0) {
            _has_previous_name = true;
            _previous_name = current_name;
        }
        Thread::set_self_name(new_name);
    }

    ~ScopedThreadName() {
        if (_has_previous_name) {
            Thread::set_self_name(_previous_name);
        }
    }

    ScopedThreadName(const ScopedThreadName&) = delete;
    ScopedThreadName& operator=(const ScopedThreadName&) = delete;

private:
    bool _has_previous_name = false;
    std::string _previous_name;
};

} // namespace doris::segment_v2
