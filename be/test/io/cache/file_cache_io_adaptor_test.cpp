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

#include <thread>
#include <vector>

namespace doris::io {

class FileCacheIOAdaptorTest : public testing::Test {
protected:
    void seed(size_t count, int64_t local_us = 20000, int64_t remote_us = 1000) {
        for (size_t i = 0; i < count; ++i) {
            adaptor.record_local(local_us);
            adaptor.record_remote(remote_us);
        }
    }
    FileCacheIOAdaptor adaptor;
};

TEST_F(FileCacheIOAdaptorTest, RequiresFiftyRealSamplesOnBothPaths) {
    seed(49);
    EXPECT_FALSE(adaptor.select().read_remote());
    adaptor.record_local(20000);
    EXPECT_FALSE(adaptor.select().read_remote());
    adaptor.record_remote(1000);
    EXPECT_TRUE(adaptor.select().read_remote());
}

TEST_F(FileCacheIOAdaptorTest, MissingRemoteHistoryDoesNotGenerateProbes) {
    for (int i = 0; i < 100; ++i) {
        adaptor.record_local(20000);
        const auto decision = adaptor.select();
        EXPECT_FALSE(decision.read_remote());
        EXPECT_FALSE(decision.local_feedback);
        EXPECT_FALSE(decision.remote_feedback);
    }
}

TEST_F(FileCacheIOAdaptorTest, FiveMillisecondFloorAndExactBoundary) {
    seed(50, 4999);
    EXPECT_FALSE(adaptor.select().read_remote());
    seed(50, 5000);
    EXPECT_TRUE(adaptor.select().read_remote());
}

TEST_F(FileCacheIOAdaptorTest, EqualityPrefersLocalAndFractionalFactorIsPreserved) {
    seed(50, 8000, 8000);
    EXPECT_FALSE(adaptor.select().read_remote());
    seed(50, 8000, 10000);
    EXPECT_FALSE(adaptor.select().read_remote());
    EXPECT_TRUE(adaptor.select(1.5).read_remote());
    EXPECT_FALSE(adaptor.select(0.5).read_remote());
}

TEST_F(FileCacheIOAdaptorTest, SlidingWindowReplacesOldSamples) {
    seed(50);
    EXPECT_TRUE(adaptor.select().read_remote());
    seed(50, 100, 1000);
    EXPECT_FALSE(adaptor.select().read_remote());
    EXPECT_EQ(adaptor.select().local_latency_us, 100);
}

TEST_F(FileCacheIOAdaptorTest, RememberedLocalMinimumEventuallyReconsidersLocal) {
    adaptor.record_local(100);
    seed(50, 20000, 1000);
    EXPECT_TRUE(adaptor.select().local_feedback);
    for (int i = 0; i < 49; ++i) {
        adaptor.select();
    }
    const auto decision = adaptor.select();
    EXPECT_LT(decision.local_latency_us, 5000);
    EXPECT_FALSE(decision.read_remote());
}

TEST_F(FileCacheIOAdaptorTest, MinimumFeedbackDoesNotInventUnobservedFastLocalSamples) {
    seed(50, 20000, 1000);
    const auto minimum_life = adaptor._local.minimum_life;
    for (int i = 0; i < 1000; ++i) {
        const auto decision = adaptor.select();
        EXPECT_TRUE(decision.read_remote());
        EXPECT_TRUE(decision.local_feedback);
        EXPECT_EQ(decision.local_latency_us, 20000);
        adaptor.record_remote(1000);
    }
    EXPECT_EQ(adaptor._local.minimum, 20000);
    EXPECT_EQ(adaptor._local.minimum_life, minimum_life);
    EXPECT_EQ(adaptor._local.count, 50);
}

TEST_F(FileCacheIOAdaptorTest, RememberedRemoteMinimumReconsidersRemote) {
    adaptor.record_remote(1000);
    seed(50, 8000, 20000);
    EXPECT_TRUE(adaptor.select().remote_feedback);
    for (int i = 0; i < 49; ++i) {
        adaptor.select();
    }
    EXPECT_TRUE(adaptor.select().read_remote());
}

TEST_F(FileCacheIOAdaptorTest, FeedbackDoesNotAgeOrReplaceObservedMinimum) {
    FileCacheIOAdaptor::Window window;
    window.record(100);
    window.record(200);
    const auto age = window.minimum_life;
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(window.feedback());
    }
    EXPECT_EQ(window.minimum, 100);
    EXPECT_EQ(window.minimum_life, age);
    EXPECT_EQ(window.average(), 100);
}

TEST_F(FileCacheIOAdaptorTest, RealSamplesExpireRememberedMinimumAfterLifeLimit) {
    FileCacheIOAdaptor::Window window;
    window.record(100);
    for (uint64_t i = 0; i <= FileCacheIOAdaptor::MINIMUM_LIFE_LIMIT; ++i) {
        window.record(200);
    }
    EXPECT_EQ(window.minimum, 100);
    window.record(300);
    EXPECT_EQ(window.minimum, 300);
    EXPECT_EQ(window.minimum_life, 0);
}

TEST_F(FileCacheIOAdaptorTest, FeedbackRejectsUninitializedOrExcessiveMinimum) {
    FileCacheIOAdaptor::Window window;
    EXPECT_FALSE(window.feedback());
    window.record(100000001);
    EXPECT_FALSE(window.feedback());
    window.record(100000000);
    EXPECT_TRUE(window.feedback());
}

TEST_F(FileCacheIOAdaptorTest, NegativeObservationsAreIgnoredAndZeroIsValid) {
    adaptor.record_local(-1);
    adaptor.record_remote(-1);
    EXPECT_EQ(adaptor._local.count, 0);
    EXPECT_EQ(adaptor._remote.count, 0);
    seed(50, 0, 0);
    EXPECT_EQ(adaptor._local.count, 50);
    EXPECT_EQ(adaptor._local.minimum, 0);
    EXPECT_FALSE(adaptor.select().read_remote());
}

TEST_F(FileCacheIOAdaptorTest, VeryLargeLatenciesDoNotOverflowWindow) {
    const auto maximum = std::numeric_limits<int64_t>::max();
    seed(100, maximum, maximum);
    const auto decision = adaptor.select();
    EXPECT_EQ(decision.local_latency_us, maximum);
    EXPECT_EQ(decision.remote_latency_us, maximum);
    EXPECT_FALSE(decision.read_remote());
}

TEST_F(FileCacheIOAdaptorTest, NormalizationUsesNativeBlockAndEmpiricalRatio) {
    constexpr size_t block = 1 << 20;
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(block, 8000000, block), 8000);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(block * 2, 8000000, block), 8000);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(block * 4, 8000000, block), 4000);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(block / 2, 8000000, block), 8000);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(block / 4, 8000000, block), 16000);
    // Integer division precedes multiplication, so rounding must retain the expected estimate.
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(block * 3, 1000000, block), 666);
}

TEST_F(FileCacheIOAdaptorTest, NormalizationHandlesInvalidInputsAndSaturates) {
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(0, 1000, 1), -1);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(1, 1000, 0), -1);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(1, -1, 1), -1);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(1, 999, 1), 0);
    EXPECT_EQ(FileCacheIOAdaptor::latency_per_block_us(1, std::numeric_limits<int64_t>::max(),
                                                       std::numeric_limits<size_t>::max()),
              std::numeric_limits<int64_t>::max());
}

TEST_F(FileCacheIOAdaptorTest, ConcurrentQueriesShareBoundedHistory) {
    std::vector<std::thread> threads;
    for (int thread = 0; thread < 8; ++thread) {
        threads.emplace_back([&] {
            for (int i = 0; i < 10000; ++i) {
                adaptor.record_local(20000);
                adaptor.record_remote(1000);
                adaptor.select();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(adaptor._local.count, 50);
    EXPECT_EQ(adaptor._remote.count, 50);
    EXPECT_EQ(adaptor.select().local_latency_us, 20000);
    EXPECT_TRUE(adaptor.select().read_remote());
}

} // namespace doris::io
