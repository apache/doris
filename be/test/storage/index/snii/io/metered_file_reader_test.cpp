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

#include "storage/index/snii/io/metered_file_reader.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/local_file.h"

using namespace doris::snii;
using doris::Status;
using doris::snii::io::LocalFileReader;
using doris::snii::io::LocalFileWriter;
using doris::snii::io::MeteredFileReader;
using doris::snii::io::Range;

namespace {

// Owns one scratch file holding 256 bytes (byte[i] = i) and removes it on scope
// exit. The name carries the pid and a counter, like the other scratch paths in
// this directory: a fixed /tmp name left behind by another user on a shared
// machine makes ::open fail with EACCES and takes every test here down with it.
class RampFile {
public:
    RampFile() {
        static int counter = 0;
        path_ = "/tmp/snii_metered_ramp_" + std::to_string(::getpid()) + "_" +
                std::to_string(counter++) + ".bin";
        LocalFileWriter w;
        EXPECT_TRUE(w.open(path_).ok());
        std::vector<uint8_t> data(256);
        for (int i = 0; i < 256; ++i) {
            data[i] = static_cast<uint8_t>(i);
        }
        EXPECT_TRUE(w.append(Slice(data)).ok());
        EXPECT_TRUE(w.finalize().ok());
    }
    ~RampFile() { std::remove(path_.c_str()); }

    const std::string& path() const { return path_; }

private:
    std::string path_;
};

} // namespace

// Single reads: first read to a block is a cache miss (1 round, 1 GET, 1 block of
// remote bytes); a second read to the same 16-byte block is a hit (no new round).
TEST(SniiMeteredFileReader, SingleReadCacheAccounting) {
    const RampFile ramp;
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(ramp.path()).ok());
    MeteredFileReader m(&inner, /*block_size=*/16);

    std::vector<uint8_t> out;
    ASSERT_TRUE(m.read_at(0, 4, &out).ok());
    EXPECT_EQ(out[0], 0U);
    EXPECT_EQ(out[3], 3U);
    EXPECT_EQ(m.metrics().read_at_calls, 1U);
    EXPECT_EQ(m.metrics().serial_rounds, 1U);
    EXPECT_EQ(m.metrics().range_gets, 1U);
    EXPECT_EQ(m.metrics().remote_bytes, 16U);

    // Same block (offset 8..11) -> cache hit.
    ASSERT_TRUE(m.read_at(8, 4, &out).ok());
    EXPECT_EQ(m.metrics().read_at_calls, 2U);
    EXPECT_EQ(m.metrics().serial_rounds, 1U); // unchanged
    EXPECT_EQ(m.metrics().range_gets, 1U);
    EXPECT_EQ(m.metrics().remote_bytes, 16U);

    // Different block (offset 20 -> block 1) -> miss.
    ASSERT_TRUE(m.read_at(20, 4, &out).ok());
    EXPECT_EQ(out[0], 20U);
    EXPECT_EQ(m.metrics().serial_rounds, 2U);
    EXPECT_EQ(m.metrics().range_gets, 2U);
    EXPECT_EQ(m.metrics().remote_bytes, 32U);
}

// A read spanning 3 contiguous blocks is one round and one coalesced GET.
TEST(SniiMeteredFileReader, SpanMultipleBlocksCoalesced) {
    const RampFile ramp;
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(ramp.path()).ok());
    MeteredFileReader m(&inner, 16);

    std::vector<uint8_t> out;
    ASSERT_TRUE(m.read_at(0, 40, &out).ok()); // blocks 0,1,2
    EXPECT_EQ(out.size(), 40U);
    EXPECT_EQ(m.metrics().serial_rounds, 1U);
    EXPECT_EQ(m.metrics().range_gets, 1U);
    EXPECT_EQ(m.metrics().remote_bytes, 48U); // 3 * 16
}

// A batch of reads to non-adjacent blocks: one serial round, one GET per run.
TEST(SniiMeteredFileReader, BatchNonAdjacent) {
    const RampFile ramp;
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(ramp.path()).ok());
    MeteredFileReader m(&inner, 16);

    std::vector<Range> ranges = {{.offset = 0, .len = 4},
                                 {.offset = 100, .len = 4},
                                 {.offset = 200, .len = 4}}; // blocks 0, 6, 12
    std::vector<std::vector<uint8_t>> outs;
    ASSERT_TRUE(m.read_batch(ranges, &outs).ok());
    ASSERT_EQ(outs.size(), 3U);
    EXPECT_EQ(outs[1][0], 100U);
    EXPECT_EQ(m.metrics().read_at_calls, 3U);
    EXPECT_EQ(m.metrics().serial_rounds, 1U); // one batch = one round
    EXPECT_EQ(m.metrics().range_gets, 3U);    // 3 disjoint runs
    EXPECT_EQ(m.metrics().remote_bytes, 48U);
}

// A batch of reads to adjacent blocks coalesces into a single GET.
TEST(SniiMeteredFileReader, BatchAdjacentCoalesced) {
    const RampFile ramp;
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(ramp.path()).ok());
    MeteredFileReader m(&inner, 16);

    std::vector<Range> ranges = {{.offset = 0, .len = 4},
                                 {.offset = 16, .len = 4},
                                 {.offset = 32, .len = 4}}; // blocks 0,1,2
    std::vector<std::vector<uint8_t>> outs;
    ASSERT_TRUE(m.read_batch(ranges, &outs).ok());
    EXPECT_EQ(m.metrics().read_at_calls, 3U);
    EXPECT_EQ(m.metrics().serial_rounds, 1U);
    EXPECT_EQ(m.metrics().range_gets, 1U); // coalesced
    EXPECT_EQ(m.metrics().remote_bytes, 48U);
}

// reset_metrics clears both counters and the resident cache (cold query).
TEST(SniiMeteredFileReader, ResetClearsCacheAndCounters) {
    const RampFile ramp;
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(ramp.path()).ok());
    MeteredFileReader m(&inner, 16);

    std::vector<uint8_t> out;
    ASSERT_TRUE(m.read_at(0, 4, &out).ok());
    m.reset_metrics();
    EXPECT_EQ(m.metrics().read_at_calls, 0U);
    EXPECT_EQ(m.metrics().serial_rounds, 0U);
    // Cache cleared -> same read misses again.
    ASSERT_TRUE(m.read_at(0, 4, &out).ok());
    EXPECT_EQ(m.metrics().serial_rounds, 1U);
    EXPECT_EQ(m.metrics().remote_bytes, 16U);
}

TEST(SniiMeteredFileReader, InvalidRangeDoesNotPolluteMetrics) {
    const RampFile ramp;
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(ramp.path()).ok());
    MeteredFileReader m(&inner, 16);

    std::vector<uint8_t> out;
    const Status st = m.read_at(250, 16, &out);
    EXPECT_TRUE(st.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << st.to_string();
    EXPECT_EQ(m.metrics().read_at_calls, 0U);
    EXPECT_EQ(m.metrics().serial_rounds, 0U);
    EXPECT_EQ(m.metrics().range_gets, 0U);
    EXPECT_EQ(m.metrics().remote_bytes, 0U);
    EXPECT_EQ(m.metrics().total_request_bytes, 0U);
}

TEST(SniiMeteredFileReader, InvalidBatchRangeDoesNotPolluteMetrics) {
    const RampFile ramp;
    LocalFileReader inner;
    ASSERT_TRUE(inner.open(ramp.path()).ok());
    MeteredFileReader m(&inner, 16);

    std::vector<std::vector<uint8_t>> outs;
    const Status st =
            m.read_batch({Range {.offset = 0, .len = 4}, Range {.offset = 250, .len = 16}}, &outs);
    EXPECT_TRUE(st.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << st.to_string();
    EXPECT_EQ(m.metrics().read_at_calls, 0U);
    EXPECT_EQ(m.metrics().serial_rounds, 0U);
    EXPECT_EQ(m.metrics().range_gets, 0U);
    EXPECT_EQ(m.metrics().remote_bytes, 0U);
    EXPECT_EQ(m.metrics().total_request_bytes, 0U);
}
