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

#include <cstdint>
#include <limits>
#include <string>
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
