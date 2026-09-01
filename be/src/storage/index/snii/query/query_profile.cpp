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

#include "storage/index/snii/query/query_profile.h"

#include <algorithm>
#include <atomic>
#include <chrono>

#include "storage/index/snii/io/file_reader.h"

namespace doris::snii::query {

namespace {

#ifdef BE_TEST
std::atomic<uint64_t> query_profile_clock_reads {0};
#endif

std::chrono::steady_clock::time_point query_profile_clock_now() {
#ifdef BE_TEST
    query_profile_clock_reads.fetch_add(1, std::memory_order_relaxed);
#endif
    return std::chrono::steady_clock::now();
}

} // namespace

QueryProfileScope::QueryProfileScope(io::FileReader* reader, QueryProfile* profile)
        : reader_(reader), profile_(profile) {
    if (profile_ == nullptr) return;

    start_ = query_profile_clock_now();
    *profile_ = QueryProfile {};
    if (reader_ == nullptr) return;

    const io::IoMetrics* metrics = reader_->io_metrics();
    if (metrics == nullptr) return;

    profile_->has_io_metrics = true;
    profile_->io_before = *metrics;
}

QueryProfileScope::~QueryProfileScope() {
    finish();
}

void QueryProfileScope::finish() {
    if (profile_ == nullptr || finished_) return;
    finished_ = true;

    const auto end = query_profile_clock_now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count();
    profile_->elapsed_ns = std::max<uint64_t>(1, static_cast<uint64_t>(elapsed));

    if (!profile_->has_io_metrics || reader_ == nullptr) return;
    const io::IoMetrics* metrics = reader_->io_metrics();
    if (metrics == nullptr) {
        profile_->has_io_metrics = false;
        return;
    }
    profile_->io_after = *metrics;
    profile_->io_delta = io::delta(profile_->io_after, profile_->io_before);
}

#ifdef BE_TEST
namespace testing {

uint64_t query_profile_clock_read_count() {
    return query_profile_clock_reads.load(std::memory_order_relaxed);
}

void reset_query_profile_clock_read_count() {
    query_profile_clock_reads.store(0, std::memory_order_relaxed);
}

} // namespace testing
#endif

} // namespace doris::snii::query
