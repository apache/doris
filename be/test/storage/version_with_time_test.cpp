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

#include "storage/olap_common.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "util/time.h"

namespace doris {

// Regression test for the (version, update_ts) publication order in
// VersionWithTime::update_version_monoto. update_ts must be stored before the
// release CAS that publishes the new version; otherwise a reader that
// acquire-loads version and then update_ts can observe "new version + stale
// timestamp", skewing MonotonicMillis()-update_ts upward and wrongly selecting
// compaction_keep_invisible_version_min_count. With the correct order the
// release/acquire chain on version guarantees ts >= the timestamp stored for
// that version's publication, so this test passes deterministically.
TEST(VersionWithTimeTest, ReadersNeverSeeStaleTimestamp) {
    constexpr int64_t kMaxVersion = 20000;
    constexpr int kReaders = 4;
    VersionWithTime vwt;
    // lower_bound[v]: sampled right before publishing v, so the timestamp
    // stored for v's publication is guaranteed to be >= lower_bound[v].
    std::vector<std::atomic<int64_t>> lower_bound(kMaxVersion + 1);
    for (auto& bound : lower_bound) {
        bound.store(0, std::memory_order_relaxed);
    }

    std::atomic<bool> stop {false};
    std::atomic<int64_t> violations {0};
    std::atomic<int64_t> first_offending_version {0};

    auto reader = [&] {
        while (!stop.load(std::memory_order_relaxed)) {
            int64_t v = vwt.version.load(std::memory_order_acquire);
            int64_t ts = vwt.update_ts.load(std::memory_order_acquire);
            if (v > 0 && ts < lower_bound[v].load(std::memory_order_acquire)) {
                violations.fetch_add(1, std::memory_order_relaxed);
                int64_t expected = 0;
                first_offending_version.compare_exchange_strong(expected, v,
                                                                std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> readers;
    readers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        readers.emplace_back(reader);
    }

    for (int64_t v = 1; v <= kMaxVersion; ++v) {
        lower_bound[v].store(MonotonicMillis(), std::memory_order_release);
        vwt.update_version_monoto(v);
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& t : readers) {
        t.join();
    }

    EXPECT_EQ(violations.load(std::memory_order_relaxed), 0)
            << "first offending version: "
            << first_offending_version.load(std::memory_order_relaxed);
}

TEST(VersionWithTimeTest, MonotoneVersionRejectsOlder) {
    VersionWithTime vwt;
    vwt.update_version_monoto(10);
    ASSERT_EQ(vwt.version.load(std::memory_order_relaxed), 10);
    int64_t ts = vwt.update_ts.load(std::memory_order_relaxed);

    // Older or equal versions must be a no-op: version and ts stay unchanged.
    vwt.update_version_monoto(5);
    EXPECT_EQ(vwt.version.load(std::memory_order_relaxed), 10);
    EXPECT_EQ(vwt.update_ts.load(std::memory_order_relaxed), ts);
    vwt.update_version_monoto(10);
    EXPECT_EQ(vwt.version.load(std::memory_order_relaxed), 10);
    EXPECT_EQ(vwt.update_ts.load(std::memory_order_relaxed), ts);
}

} // namespace doris
