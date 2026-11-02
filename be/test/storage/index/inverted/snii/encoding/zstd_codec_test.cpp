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

#include "storage/index/inverted/snii/encoding/zstd_codec.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "common/status.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"

using namespace doris::snii;
using doris::AttachTask; // SCOPED_ATTACH_TASK names it unqualified

TEST(SniiZstd, RoundTrip) {
    std::vector<uint8_t> in;
    for (int i = 0; i < 10000; ++i) {
        in.push_back(static_cast<uint8_t>(i % 7));
    }
    std::vector<uint8_t> comp, decomp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    EXPECT_LT(comp.size(), in.size());
    ASSERT_TRUE(zstd_decompress(Slice(comp), in.size(), &decomp).ok());
    EXPECT_EQ(decomp, in);
}

TEST(SniiZstd, WrongLenFails) {
    std::vector<uint8_t> in(100, 7), comp, decomp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    EXPECT_FALSE(zstd_decompress(Slice(comp), 99, &decomp).ok());
}

TEST(SniiZstd, EmptyInput) {
    std::vector<uint8_t> in, comp, decomp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    ASSERT_TRUE(zstd_decompress(Slice(comp), 0, &decomp).ok());
    EXPECT_TRUE(decomp.empty());
}

namespace {
std::vector<uint8_t> pattern(size_t n, int seed) {
    std::vector<uint8_t> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<uint8_t>((i * 31 + seed) % 13);
    }
    return v;
}
} // namespace

// One decompressor serves frames that grow and shrink; every output equals its input.
TEST(SniiZstdDecompressor, ReusedAcrossFramesOfDifferentSizes) {
    ZstdDecompressor decompressor;
    for (size_t n : {100UL, 5000UL, 300UL, 0UL, 20000UL, 7UL}) {
        const std::vector<uint8_t> in = pattern(n, static_cast<int>(n));
        std::vector<uint8_t> comp;
        ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
        Slice out;
        ASSERT_TRUE(decompressor.decompress(Slice(comp), in.size(), &out).ok()) << n;
        ASSERT_EQ(out.size(), in.size());
        EXPECT_TRUE(std::equal(in.begin(), in.end(), out.data())) << n;
    }
}

// The errors are the ones zstd_decompress reports, and a failed frame does not spoil the next one.
TEST(SniiZstdDecompressor, ReportsCorruptionAndRecovers) {
    const std::vector<uint8_t> in = pattern(1000, 3);
    std::vector<uint8_t> comp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    ZstdDecompressor decompressor;
    Slice out;

    const doris::Status wrong_length = decompressor.decompress(Slice(comp), in.size() - 1, &out);
    EXPECT_TRUE(wrong_length.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
            << wrong_length.to_string();

    std::vector<uint8_t> garbage = comp;
    garbage[garbage.size() / 2] ^= 0xFF;
    garbage.resize(garbage.size() - 3);
    const doris::Status corrupt = decompressor.decompress(Slice(garbage), in.size(), &out);
    EXPECT_TRUE(corrupt.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
            << corrupt.to_string();

    ASSERT_TRUE(decompressor.decompress(Slice(comp), in.size(), &out).ok());
    ASSERT_EQ(out.size(), in.size());
    EXPECT_TRUE(std::equal(in.begin(), in.end(), out.data()));
}

// The buffer a decompressor keeps between frames is as large as the largest frame it decoded, so
// it is charged to the memory tracker of the task that decodes, until the decompressor goes away.
TEST(SniiZstdDecompressor, ChargesItsBufferToTheTaskMemTracker) {
    const std::vector<uint8_t> in = pattern(8 << 20, 5);
    std::vector<uint8_t> comp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    auto tracker = doris::MemTrackerLimiter::create_shared(doris::MemTrackerLimiter::Type::OTHER,
                                                           "UT-SniiZstdDecompressor");
    SCOPED_ATTACH_TASK(tracker);
    const int64_t before = tracker->consumption();
    {
        ZstdDecompressor decompressor;
        Slice out;
        ASSERT_TRUE(decompressor.decompress(Slice(comp), in.size(), &out).ok());
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_GE(tracker->consumption() - before, static_cast<int64_t>(in.size()));
    }
    doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_LT(tracker->consumption() - before, static_cast<int64_t>(in.size()));
}
