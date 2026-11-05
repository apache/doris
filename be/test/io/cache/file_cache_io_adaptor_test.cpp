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

#include "io/cache/file_cache_io_adaptor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>
#include <vector>

namespace doris::io {

class FileCacheIOAdaptorTest : public testing::Test {
protected:
    static constexpr size_t READ_SIZE = 4096;
    void seed(size_t count, int64_t local_us = 20000, int64_t remote_us = 1000,
              size_t bytes = READ_SIZE) {
        for (size_t i = 0; i < count; ++i) {
            adaptor.record_local(bytes, local_us);
            adaptor.record_remote(bytes, remote_us);
        }
    }
    FileCacheIOAdaptor adaptor;
};

TEST_F(FileCacheIOAdaptorTest, RequiresFiftyRealSamplesOnBothPaths) {
    seed(49);
    EXPECT_FALSE(adaptor._select(READ_SIZE, 1.0, 0).read_remote());
    adaptor.record_local(READ_SIZE, 20000);
    EXPECT_FALSE(adaptor._select(READ_SIZE, 1.0, 0).read_remote());
    adaptor.record_remote(READ_SIZE, 1000);
    EXPECT_TRUE(adaptor._select(READ_SIZE, 1.0, 0).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, SmallReadsKeepTheirActualLatency) {
    seed(50, 100, 1000);
    const auto decision = adaptor.select(READ_SIZE);
    EXPECT_EQ(decision.local_latency_us, 100);
    EXPECT_FALSE(decision.read_remote());
}

TEST_F(FileCacheIOAdaptorTest, MixedSizesDoNotShareHistory) {
    seed(50, 100, 1000);
    seed(50, 20000, 1000, 1 << 20);
    EXPECT_FALSE(adaptor.select(4096).read_remote());
    EXPECT_TRUE(adaptor.select(1 << 20).read_remote());
    // Large remote misses cannot bootstrap the unrelated small-read remote window.
    FileCacheIOAdaptor other;
    for (int i = 0; i < 50; ++i) {
        other.record_local(4096, 20000);
        other.record_remote(1 << 20, 1000);
    }
    EXPECT_EQ(other.select(4096).remote_latency_us, 0);
    EXPECT_FALSE(other.select(4096).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, SizeBucketBoundaries) {
    for (size_t i = 0; i < FileCacheIOAdaptor::SIZE_LIMITS.size(); ++i) {
        const auto limit = FileCacheIOAdaptor::SIZE_LIMITS[i];
        EXPECT_EQ(FileCacheIOAdaptor::bucket_index(limit), i);
        EXPECT_EQ(FileCacheIOAdaptor::bucket_index(limit + 1), i + 1);
    }
    EXPECT_EQ(FileCacheIOAdaptor::bucket_index(std::numeric_limits<size_t>::max()),
              FileCacheIOAdaptor::BUCKET_COUNT - 1);
}

TEST_F(FileCacheIOAdaptorTest, FiveMillisecondFloorAndExactBoundary) {
    seed(50, 4999);
    EXPECT_FALSE(adaptor.select(READ_SIZE).read_remote());
    seed(50, 5000);
    EXPECT_TRUE(adaptor.select(READ_SIZE).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, EqualityPrefersLocalAndFractionalFactorIsPreserved) {
    seed(50, 8000, 8000);
    EXPECT_FALSE(adaptor.select(READ_SIZE).read_remote());
    seed(50, 8000, 10000);
    EXPECT_FALSE(adaptor.select(READ_SIZE).read_remote());
    EXPECT_TRUE(adaptor.select(READ_SIZE, 1.5).read_remote());
    EXPECT_FALSE(adaptor.select(READ_SIZE, 0.5).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, SlidingWindowReplacesOldSamples) {
    seed(50);
    EXPECT_TRUE(adaptor.select(READ_SIZE).read_remote());
    seed(50, 100, 1000);
    EXPECT_FALSE(adaptor.select(READ_SIZE).read_remote());
    EXPECT_EQ(adaptor.select(READ_SIZE).local_latency_us, 100);
}

TEST_F(FileCacheIOAdaptorTest, RevisitLocalWithoutPreviouslyObservedFastSamples) {
    seed(50);
    size_t local_probes = 0;
    for (int64_t i = 0; i < 4096; ++i) {
        const auto decision = adaptor._select(READ_SIZE, 1.0, i * 2'000'000);
        local_probes += decision.local_probe;
        if (decision.read_remote()) {
            adaptor.record_remote(READ_SIZE, 1000);
        } else {
            adaptor.record_local(READ_SIZE, 100);
        }
        if (decision.is_probe()) {
            adaptor.finish_probe();
        }
    }
    EXPECT_GT(local_probes, 0);
    EXPECT_EQ(adaptor._buckets[0].local.average(), 100);
    EXPECT_FALSE(adaptor._select(READ_SIZE, 1.0, 0).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, MissingRemoteHistoryIsPopulatedByLimitedRealProbes) {
    for (int i = 0; i < 50; ++i) {
        adaptor.record_local(READ_SIZE, 20000);
    }
    size_t remote_probes = 0;
    for (int64_t i = 0; i < 50 * 64; ++i) {
        const auto decision = adaptor._select(READ_SIZE, 1.0, i * 2'000'000);
        if (decision.remote_probe) {
            ++remote_probes;
            adaptor.record_remote(READ_SIZE, 1000);
            adaptor.finish_probe();
        }
    }
    EXPECT_EQ(remote_probes, 50);
    EXPECT_TRUE(adaptor._select(READ_SIZE, 1.0, 0).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, SelectionNeverCreatesSyntheticSamples) {
    seed(50);
    for (int64_t i = 0; i < 1000; ++i) {
        const auto decision = adaptor._select(READ_SIZE, 1.0, i * 2'000'000);
        if (decision.is_probe()) {
            adaptor.finish_probe();
        }
    }
    EXPECT_EQ(adaptor._buckets[0].local.average(), 20000);
    EXPECT_EQ(adaptor._buckets[0].remote.average(), 1000);
    EXPECT_EQ(adaptor._buckets[0].local.next, 0);
    EXPECT_EQ(adaptor._buckets[0].remote.next, 0);
}

TEST_F(FileCacheIOAdaptorTest, ProbeLimitsSpanAllSizeBuckets) {
    seed(50);
    seed(50, 20000, 1000, 1 << 20);
    for (int i = 0; i < 63; ++i) {
        EXPECT_FALSE(adaptor._select(READ_SIZE, 1.0, 0).is_probe());
    }
    EXPECT_TRUE(adaptor._select(READ_SIZE, 1.0, 0).local_probe);
    for (int i = 0; i < 128; ++i) {
        EXPECT_FALSE(adaptor._select(1 << 20, 1.0, 2'000'000).is_probe());
    }
    adaptor.finish_probe();
    EXPECT_FALSE(adaptor._select(1 << 20, 1.0, 999999).is_probe());
    EXPECT_TRUE(adaptor._select(1 << 20, 1.0, 1'000'000).local_probe);
    adaptor.finish_probe();
    for (int i = 0; i < 63; ++i) {
        EXPECT_FALSE(adaptor._select(READ_SIZE, 1.0, 2'000'000).is_probe());
    }
    EXPECT_TRUE(adaptor._select(READ_SIZE, 1.0, 2'000'000).local_probe);
    adaptor.finish_probe();
}

TEST_F(FileCacheIOAdaptorTest, MixedSizeRecoveryCannotStarveAtProbeBoundary) {
    seed(50);
    seed(50, 20000, 1000, 1 << 20);
    std::array<size_t, 2> probes {};
    size_t requests_since_probe = 0;
    int64_t last_probe_us = -2'000'000;
    // The large request always lands on the global quota boundary. Both sizes must still
    // discover a recovered disk, rather than letting that request consume every probe.
    for (int64_t request = 0; request < 192 * 64; ++request) {
        const bool large = request % 64 == 63;
        const size_t bytes = large ? 1 << 20 : READ_SIZE;
        const auto now_us = (request / 64) * 2'000'000;
        const auto decision = adaptor._select(bytes, 1.0, now_us);
        ++requests_since_probe;
        if (decision.read_remote()) {
            adaptor.record_remote(bytes, 1000);
        } else {
            adaptor.record_local(bytes, 100);
        }
        if (decision.is_probe()) {
            EXPECT_GE(requests_since_probe, 64);
            EXPECT_GE(now_us - last_probe_us, 1'000'000);
            ++probes[large];
            requests_since_probe = 0;
            last_probe_us = now_us;
            adaptor.finish_probe();
        }
    }
    EXPECT_GE(probes[0], 50);
    EXPECT_GE(probes[1], 50);
    EXPECT_EQ(adaptor._buckets[FileCacheIOAdaptor::bucket_index(READ_SIZE)].local.average(), 100);
    EXPECT_EQ(adaptor._buckets[FileCacheIOAdaptor::bucket_index(1 << 20)].local.average(), 100);
    const auto decision = adaptor._select(READ_SIZE, 1.0, 384'000'000);
    EXPECT_EQ(decision.read_remote(), decision.remote_probe);
    if (decision.is_probe()) {
        adaptor.finish_probe();
    }
}

TEST_F(FileCacheIOAdaptorTest, AllActiveBucketsShareProbeSlots) {
    std::array<size_t, FileCacheIOAdaptor::BUCKET_COUNT> probes {};
    for (size_t bucket = 0; bucket < probes.size(); ++bucket) {
        seed(50, 20000, 1000, READ_SIZE << (bucket * 2));
    }
    for (size_t request = 0; request < probes.size() * 64 * 3; ++request) {
        const size_t bucket = request % probes.size();
        const auto decision = adaptor._select(READ_SIZE << (bucket * 2), 1.0, request * 2'000'000);
        if (decision.is_probe()) {
            ++probes[bucket];
            adaptor.finish_probe();
        }
    }
    for (const auto count : probes) {
        EXPECT_GT(count, 0);
    }
}

TEST_F(FileCacheIOAdaptorTest, InactiveBucketDoesNotBlockRemainingTraffic) {
    seed(50);
    seed(50, 20000, 1000, 1 << 20);
    static_cast<void>(adaptor._select(1 << 20, 1.0, 0));
    size_t probes = 0;
    for (int64_t request = 1; request <= 512; ++request) {
        const auto decision = adaptor._select(READ_SIZE, 1.0, request * 2'000'000);
        if (decision.is_probe()) {
            ++probes;
            adaptor.finish_probe();
        }
    }
    EXPECT_GE(probes, 6);
}

TEST_F(FileCacheIOAdaptorTest, NegativeObservationsAreIgnoredAndZeroIsValid) {
    adaptor.record_local(READ_SIZE, -1);
    adaptor.record_remote(READ_SIZE, -1);
    EXPECT_EQ(adaptor._buckets[0].local.count, 0);
    EXPECT_EQ(adaptor._buckets[0].remote.count, 0);
    seed(50, 0, 0);
    EXPECT_EQ(adaptor._buckets[0].local.count, 50);
    EXPECT_EQ(adaptor._buckets[0].local.average(), 0);
    EXPECT_FALSE(adaptor.select(READ_SIZE).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, VeryLargeLatenciesDoNotOverflowWindow) {
    const auto maximum = std::numeric_limits<int64_t>::max();
    seed(100, maximum, maximum);
    EXPECT_EQ(adaptor.select(READ_SIZE).local_latency_us, maximum);
    EXPECT_EQ(adaptor.select(READ_SIZE).remote_latency_us, maximum);
    EXPECT_FALSE(adaptor.select(READ_SIZE).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, ConcurrentQueriesShareBoundedHistoryAndOneProbe) {
    seed(50);
    std::atomic<int> inflight = 0;
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            for (int sample = 0; sample < 10000; ++sample) {
                const auto decision = adaptor.select(READ_SIZE);
                if (decision.is_probe()) {
                    EXPECT_EQ(inflight.fetch_add(1), 0);
                    inflight.fetch_sub(1);
                    adaptor.finish_probe();
                }
                adaptor.record_local(READ_SIZE, 20000);
                adaptor.record_remote(READ_SIZE, 1000);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_FALSE(adaptor._probe_inflight);
    EXPECT_EQ(adaptor._buckets[0].local.count, 50);
    EXPECT_EQ(adaptor._buckets[0].remote.count, 50);
    EXPECT_EQ(adaptor._buckets[0].local.average(), 20000);
}

} // namespace doris::io
