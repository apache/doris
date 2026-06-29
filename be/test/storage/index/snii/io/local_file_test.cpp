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

#include "snii/io/local_file.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

using namespace snii;
using snii::io::LocalFileReader;
using snii::io::LocalFileWriter;

namespace {

std::string TempPath(const char* name) {
    return std::string("/tmp/snii_io_test_") + name + ".bin";
}

} // namespace

TEST(SniiLocalFile, AppendThenReadBack) {
    const std::string path = TempPath("append");
    {
        LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        const uint8_t a[] = {1, 2, 3, 4};
        const uint8_t b[] = {5, 6, 7, 8};
        ASSERT_TRUE(w.append(Slice(a, 4)).ok());
        ASSERT_TRUE(w.append(Slice(b, 4)).ok());
        EXPECT_EQ(w.bytes_written(), 8U);
        ASSERT_TRUE(w.finalize().ok());
    }

    LocalFileReader r;
    ASSERT_TRUE(r.open(path).ok());
    EXPECT_EQ(r.size(), 8U);
    std::vector<uint8_t> out;
    ASSERT_TRUE(r.read_at(2, 4, &out).ok());
    ASSERT_EQ(out.size(), 4U);
    EXPECT_EQ(out[0], 3U);
    EXPECT_EQ(out[3], 6U);
}

TEST(SniiLocalFile, ReadPastEndFails) {
    const std::string path = TempPath("past_end");
    {
        LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        const uint8_t a[] = {1, 2, 3};
        ASSERT_TRUE(w.append(Slice(a, 3)).ok());
        ASSERT_TRUE(w.finalize().ok());
    }
    LocalFileReader r;
    ASSERT_TRUE(r.open(path).ok());
    std::vector<uint8_t> out;
    EXPECT_FALSE(r.read_at(2, 10, &out).ok());
}

TEST(SniiLocalFile, OpenMissingFails) {
    LocalFileReader r;
    EXPECT_FALSE(r.open("/tmp/snii_io_test_does_not_exist_zzz.bin").ok());
}

// The buffered writer must produce byte-identical output regardless of how the
// stream is chopped: tiny appends that overflow the 256 KiB buffer, a single
// over-buffer append (the direct-to-fd fast path), and mixes of the two.
TEST(SniiLocalFile, BufferedAppendsByteIdentical) {
    const std::string path = TempPath("buffered");
    // Build a deterministic reference stream larger than the 256 KiB buffer so the
    // buffer flushes several times and at least one big append bypasses it.
    std::vector<uint8_t> expected;
    expected.reserve(1U << 20);
    uint32_t x = 0xC0FFEEU;
    auto next = [&]() {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        return static_cast<uint8_t>(x);
    };
    for (size_t i = 0; i < (1U << 20); ++i) {
        expected.push_back(next());
    }

    {
        LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        size_t off = 0;
        // Phase 1: many small appends (700 B) -> repeated buffer overflow/flush.
        while (off + 700 <= 600U * 1024) {
            ASSERT_TRUE(w.append(Slice(expected.data() + off, 700)).ok());
            off += 700;
        }
        // Phase 2: one append larger than the buffer (direct-to-fd path).
        const size_t big = 300U * 1024;
        ASSERT_TRUE(w.append(Slice(expected.data() + off, big)).ok());
        off += big;
        // Phase 3: drain the remainder in a final small append.
        ASSERT_TRUE(w.append(Slice(expected.data() + off, expected.size() - off)).ok());
        EXPECT_EQ(w.bytes_written(), expected.size());
        ASSERT_TRUE(w.finalize().ok());
    }

    LocalFileReader r;
    ASSERT_TRUE(r.open(path).ok());
    ASSERT_EQ(r.size(), expected.size());
    std::vector<uint8_t> got;
    ASSERT_TRUE(r.read_at(0, expected.size(), &got).ok());
    EXPECT_EQ(got, expected);
}

// Empty appends are no-ops and an unused open->finalize yields an empty file.
TEST(SniiLocalFile, EmptyAppendsAndFinalize) {
    const std::string path = TempPath("empty_buf");
    {
        LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        ASSERT_TRUE(w.append(Slice(nullptr, 0)).ok());
        EXPECT_EQ(w.bytes_written(), 0U);
        ASSERT_TRUE(w.finalize().ok());
    }
    LocalFileReader r;
    ASSERT_TRUE(r.open(path).ok());
    EXPECT_EQ(r.size(), 0U);
}
