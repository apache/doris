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

#include "storage/index/snii/io/batch_range_fetcher.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"

using namespace doris::snii;
using doris::Status;
using doris::snii::io::BatchRangeFetcher;
using doris::snii::io::LocalFileReader;
using doris::snii::io::LocalFileWriter;
using doris::snii::io::MeteredFileReader;

namespace {

std::string MakeRampFile() {
    const std::string path = "/tmp/snii_brf_ramp.bin";
    LocalFileWriter w;
    EXPECT_TRUE(w.open(path).ok());
    std::vector<uint8_t> data(256);
    for (int i = 0; i < 256; ++i) {
        data[i] = static_cast<uint8_t>(i);
    }
    EXPECT_TRUE(w.append(Slice(data)).ok());
    EXPECT_TRUE(w.finalize().ok());
    return path;
}

} // namespace

// Disjoint ranges: each handle returns its own bytes; the whole fetch is one
// serial round on the metered reader.
TEST(SniiBatchRangeFetcher, DisjointRanges) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    MeteredFileReader m(&inner, 16);

    BatchRangeFetcher f(&m);
    size_t h0 = f.add(0, 4);
    size_t h1 = f.add(100, 4);
    size_t h2 = f.add(200, 4);
    ASSERT_TRUE(f.fetch().ok());

    EXPECT_EQ(f.get(h0)[0], 0U);
    EXPECT_EQ(f.get(h1)[0], 100U);
    EXPECT_EQ(f.get(h2)[3], 203U);
    EXPECT_EQ(m.metrics().serial_rounds, 1U); // single batched round
}

// Overlapping requests coalesce into one physical read; bytes still map back.
TEST(SniiBatchRangeFetcher, OverlappingCoalesced) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    MeteredFileReader m(&inner, 16);

    BatchRangeFetcher f(&m);
    size_t h0 = f.add(0, 4); // [0,4)
    size_t h1 = f.add(2, 4); // [2,6) overlaps
    size_t h2 = f.add(5, 3); // [5,8) adjacent/overlaps
    ASSERT_TRUE(f.fetch().ok());

    EXPECT_EQ(f.get(h0)[0], 0U);
    EXPECT_EQ(f.get(h1)[0], 2U);
    EXPECT_EQ(f.get(h2)[0], 5U);
    EXPECT_EQ(f.get(h2)[2], 7U);
    // Coalesced into a single physical read -> one read_at_call on the metered reader.
    EXPECT_EQ(m.metrics().read_at_calls, 1U);
}

// clear() lets the fetcher be reused for a new round.
TEST(SniiBatchRangeFetcher, ClearAndReuse) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    MeteredFileReader m(&inner, 16);

    BatchRangeFetcher f(&m);
    f.add(0, 4);
    ASSERT_TRUE(f.fetch().ok());
    f.clear();
    EXPECT_EQ(f.pending(), 0U);
    size_t h = f.add(64, 8);
    ASSERT_TRUE(f.fetch().ok());
    EXPECT_EQ(f.get(h)[0], 64U);
}

TEST(SniiBatchRangeFetcher, RejectsOverflowingRangeEnd) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());

    BatchRangeFetcher f(&inner);
    f.add(std::numeric_limits<uint64_t>::max() - 1, 8);
    const Status st = f.fetch();
    // Integrated fetch() reports range-end overflow as INVERTED_INDEX_FILE_CORRUPTED.
    EXPECT_TRUE(st.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << st.to_string();
}

TEST(SniiBatchRangeFetcher, RejectsNullReaderAtFetch) {
    BatchRangeFetcher f(nullptr);
    f.add(0, 1);
    const Status st = f.fetch();
    EXPECT_TRUE(st.is<doris::ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST(SniiBatchRangeFetcher, TryAddRespectsCoalescedByteAndRangeLimits) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    MeteredFileReader metered(&inner, 1);
    BatchRangeFetcher fetcher(&metered);
    const size_t first = fetcher.add(16, 8);
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(fetcher.try_add(20, 8, 16, 1, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    const size_t overlapping = handle;
    ASSERT_TRUE(fetcher.try_add(32, 4, 16, 1, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(handle, overlapping);
    ASSERT_TRUE(fetcher.try_add(8, 8, 16, 1, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(fetcher.pending(), 2U);
    ASSERT_TRUE(fetcher.try_add(12, 4, 16, 1, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    EXPECT_EQ(metered.metrics().read_at_calls, 0U);
    ASSERT_TRUE(fetcher.fetch().ok());
    EXPECT_EQ(metered.metrics().read_at_calls, 1U);
    EXPECT_EQ(fetcher.get(first)[0], 16U);
    EXPECT_EQ(fetcher.get(overlapping)[7], 27U);
    EXPECT_EQ(fetcher.get(handle)[0], 12U);
}

TEST(SniiBatchRangeFetcher, TryAddCountsGapBytesAndResetsOnClear) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    BatchRangeFetcher fetcher(&inner, 4);
    fetcher.add(8, 4);
    bool accepted = false;
    size_t handle = 99;
    ASSERT_TRUE(fetcher.try_add(16, 4, 8, 1, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(handle, 99U);
    ASSERT_TRUE(fetcher.try_add(16, 4, 12, 1, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(fetcher.fetch().ok());
    EXPECT_EQ(fetcher.get(handle)[3], 19U);
    fetcher.clear();
    ASSERT_TRUE(fetcher.try_add(240, 16, 16, 1, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(fetcher.fetch().ok());
    EXPECT_EQ(fetcher.get(handle)[15], 255U);
}

TEST(SniiBatchRangeFetcher, TryAddCanBridgeRangesAtTheRangeLimit) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    MeteredFileReader metered(&inner, 1);
    BatchRangeFetcher fetcher(&metered);
    const size_t right = fetcher.add(24, 4);
    const size_t left = fetcher.add(8, 4);
    bool accepted = false;
    size_t bridge = 0;
    ASSERT_TRUE(fetcher.try_add(12, 12, 20, 1, &accepted, &bridge).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(fetcher.fetch().ok());
    EXPECT_EQ(metered.metrics().read_at_calls, 1U);
    EXPECT_EQ(fetcher.get(right)[3], 27U);
    EXPECT_EQ(fetcher.get(left)[0], 8U);
    EXPECT_EQ(fetcher.get(bridge)[11], 23U);
}

TEST(SniiBatchRangeFetcher, TryAddRefreshesAfterUnboundedRegistration) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    MeteredFileReader metered(&inner, 1);
    BatchRangeFetcher fetcher(&metered);
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(fetcher.try_add(0, 4, 8, 2, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    fetcher.add(16, 4);
    ASSERT_TRUE(fetcher.try_add(32, 4, 8, 3, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    ASSERT_TRUE(fetcher.try_add(0, 4, 8, 2, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(fetcher.fetch().ok());
    EXPECT_EQ(fetcher.pending(), 3U);
    EXPECT_EQ(metered.metrics().read_at_calls, 2U);
    EXPECT_EQ(fetcher.get(handle)[3], 3U);
}

TEST(SniiBatchRangeFetcher, TryAddRejectsOverflowingRangesWithoutReading) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    MeteredFileReader metered(&inner, 1);
    BatchRangeFetcher fetcher(&metered);
    bool accepted = true;
    size_t handle = 99;
    const auto check_status = [&](const Status& status) {
        EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
                << status.to_string();
        EXPECT_FALSE(accepted);
        EXPECT_EQ(handle, 99U);
        EXPECT_EQ(metered.metrics().read_at_calls, 0U);
    };
    check_status(fetcher.try_add(std::numeric_limits<uint64_t>::max() - 1, 8, 16, 2, &accepted,
                                 &handle));
    EXPECT_EQ(fetcher.pending(), 0U);
    fetcher.add(std::numeric_limits<uint64_t>::max() - 1, 8);
    check_status(fetcher.try_add(0, 4, 16, 2, &accepted, &handle));
    EXPECT_EQ(fetcher.pending(), 1U);
}

namespace {

std::pair<size_t, size_t> covered_read_shape(const std::array<bool, 256>& covered) {
    size_t bytes = 0;
    size_t ranges = 0;
    bool previous = false;
    for (bool present : covered) {
        bytes += present;
        ranges += present && !previous;
        previous = present;
    }
    return {bytes, ranges};
}

void check_seeded_admission(LocalFileReader* inner, std::mt19937* random, size_t max_bytes,
                            size_t max_ranges) {
    MeteredFileReader metered(inner, 1);
    BatchRangeFetcher fetcher(&metered);
    std::array<bool, 256> covered {};
    std::vector<std::pair<size_t, std::vector<uint8_t>>> reads;
    for (size_t request = 0; request < 32; ++request) {
        const size_t len = 1 + (*random)() % 16;
        const size_t offset = (*random)() % (257 - len);
        auto proposed = covered;
        for (size_t byte = offset; byte < offset + len; ++byte) {
            proposed[byte] = true;
        }
        const auto [bytes, ranges] = covered_read_shape(proposed);
        const bool expected = bytes <= max_bytes && ranges <= max_ranges;
        bool accepted = false;
        size_t handle = 0;
        ASSERT_TRUE(fetcher.try_add(offset, len, max_bytes, max_ranges, &accepted, &handle).ok());
        ASSERT_EQ(accepted, expected);
        if (accepted) {
            covered = proposed;
            std::vector<uint8_t> data(len);
            std::iota(data.begin(), data.end(), offset);
            reads.emplace_back(handle, std::move(data));
        }
    }
    EXPECT_EQ(metered.metrics().read_at_calls, 0U);
    ASSERT_TRUE(fetcher.fetch().ok());
    EXPECT_EQ(metered.metrics().read_at_calls, covered_read_shape(covered).second);
    for (const auto& [handle, expected] : reads) {
        const Slice actual = fetcher.get(handle);
        EXPECT_EQ(std::vector<uint8_t>(actual.data(), actual.data() + actual.size()), expected);
    }
}

} // namespace

TEST(SniiBatchRangeFetcher, BoundedAdmissionMatchesIndependentByteCoverage) {
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(MakeRampFile()).ok());
    std::mt19937 random(20260920);
    for (size_t trial = 0; trial < 64; ++trial) {
        SCOPED_TRACE(trial);
        check_seeded_admission(&inner, &random, (trial % 9) * 16, trial % 5);
    }
}
