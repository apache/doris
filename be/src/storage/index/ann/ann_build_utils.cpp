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

#include "storage/index/ann/ann_build_utils.h"

namespace doris::segment_v2 {

static std::mutex g_omp_thread_mutex;
static std::condition_variable g_omp_thread_cv;
static int g_index_threads_in_use = 0;

ScopedOmpThreadBudget::ScopedOmpThreadBudget() {
    std::unique_lock<std::mutex> lock(g_omp_thread_mutex);
    auto omp_threads_limit = get_omp_threads_limit();
    // Block until there is at least one OpenMP slot available under the global cap.
    g_omp_thread_cv.wait(lock, [&] { return g_index_threads_in_use < omp_threads_limit; });
    auto thread_cap = omp_threads_limit - g_index_threads_in_use;
    // Keep headroom for other concurrent index builds: take up to half of remaining budget.
    _reserved_threads = std::max(1, thread_cap / 2);
    g_index_threads_in_use += _reserved_threads;
    DorisMetrics::instance()->ann_index_build_index_threads->increment(_reserved_threads);
    omp_set_num_threads(_reserved_threads);
    VLOG_DEBUG << fmt::format(
            "ScopedOmpThreadBudget reserve threads reserved={}, in_use={}, limit={}",
            _reserved_threads, g_index_threads_in_use, get_omp_threads_limit());
}

ScopedOmpThreadBudget::~ScopedOmpThreadBudget() {
    std::lock_guard<std::mutex> lock(g_omp_thread_mutex);
    g_index_threads_in_use -= _reserved_threads;
    DorisMetrics::instance()->ann_index_build_index_threads->increment(-_reserved_threads);
    if (g_index_threads_in_use < 0) {
        g_index_threads_in_use = 0;
    }
    // Wake waiting index builders so they can compete for the released OpenMP budget.
    g_omp_thread_cv.notify_all();
    VLOG_DEBUG << fmt::format(
            "ScopedOmpThreadBudget release threads reserved={}, remaining_in_use={}, limit={}",
            _reserved_threads, g_index_threads_in_use, get_omp_threads_limit());
}

int ScopedOmpThreadBudget::get_omp_threads_limit() {
    if (config::omp_threads_limit > 0) {
        return config::omp_threads_limit;
    }
    int core_cap = std::max(1, CpuInfo::num_cores());
    // Use at most 80% of the available CPU cores.
    return std::max(1, core_cap * 4 / 5);
}

} // namespace doris::segment_v2
