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

// R10-io-local-s3 spill backend tests.
//
// SpillableByteBuffer now spills to disk via Doris IO (io::global_local_filesystem()
// create_file / appendv / close + read_at on read-back) instead of the standalone
// snii::io::Local{File}Writer/Reader. These tests pin the externally observable contract:
// whatever lands on the spill scratch file, streamed back in append order, is byte-for-byte
// identical to the concatenation of every append -- across the 0-byte branch, the
// cap-crossing spill trigger, post-spill appends, and a >256 KiB chunk that also spans the
// stream_into() copy window. The scratch file is a process-private intermediate (NOT the
// published on-disk format v2), so this is an equivalence/round-trip guarantee, not a golden
// byte test. Everything is deterministic: fixed cap_bytes and a fixed byte pattern.

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/io/file_writer.h"
#include "snii/writer/spillable_byte_buffer.h"

namespace snii::writer {
namespace {

// Deterministic, position- and seed-dependent byte pattern so a reorder, truncation, or
// in-place corruption of any chunk is caught by a full-buffer comparison.
std::vector<uint8_t> make_bytes(uint32_t seed, size_t n) {
    std::vector<uint8_t> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<uint8_t>((seed * 1103515245U + static_cast<uint32_t>(i) * 12345U + 7U) &
                                    0xFFU);
    }
    return v;
}

// In-memory snii::io::FileWriter that records the exact bytes (and order) streamed into it.
class CapturingSniiFileWriter final : public snii::io::FileWriter {
public:
    snii::Status append(snii::Slice data) override {
        bytes_.insert(bytes_.end(), data.data(), data.data() + data.size());
        return snii::Status::OK();
    }
    snii::Status finalize() override { return snii::Status::OK(); }
    uint64_t bytes_written() const override { return bytes_.size(); }

    const std::vector<uint8_t>& bytes() const { return bytes_; }

private:
    std::vector<uint8_t> bytes_;
};

void expect_bytes_eq(const std::vector<uint8_t>& actual, const std::vector<uint8_t>& expected) {
    // Compare sizes first, then memcmp, so a mismatch on a multi-MiB buffer does not dump the
    // whole vector into the test log.
    ASSERT_EQ(actual.size(), expected.size());
    if (!expected.empty()) {
        EXPECT_EQ(std::memcmp(actual.data(), expected.data(), expected.size()), 0);
    }
}

// Points SNII_TEMP_DIR (resolve_temp_dir()'s first choice) at a throwaway directory so the
// spill scratch files are hermetic to the test and cleaned up afterwards.
class SniiSpillIoTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::error_code ec;
        std::filesystem::path base = std::filesystem::temp_directory_path(ec);
        if (ec) {
            base = "/tmp";
        }
        dir_ = base / ("snii_spill_io_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(dir_, ec);
        ::setenv("SNII_TEMP_DIR", dir_.c_str(), 1);
    }

    void TearDown() override {
        ::unsetenv("SNII_TEMP_DIR");
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    std::filesystem::path dir_;
};

// Append a mix of pre-spill, cap-crossing, and post-spill chunks (including a 0-byte append on
// each side of the spill), then assert the streamed-back bytes equal the concatenation in order.
TEST_F(SniiSpillIoTest, SpillRoundTripPreservesBytesAndOrder) {
    SpillableByteBuffer buf(/*cap_bytes=*/16, "roundtrip");

    const std::vector<std::vector<uint8_t>> chunks = {
            make_bytes(1, 5), // pre-spill (ram=5)
            make_bytes(2, 0), // 0-byte append, pre-spill (must be a no-op, not a chunk)
            make_bytes(3, 9), // pre-spill (ram=14, still under cap)
            make_bytes(4, 7), // crosses cap (ram=21 >= 16) -> spill flushes [c0,c2,c3]
            make_bytes(5, 0), // 0-byte append, post-spill (len==0 appendv no-op)
            make_bytes(6, 4), // post-spill, routed straight to the scratch file
            make_bytes(7, 33) // post-spill
    };

    std::vector<uint8_t> expected;
    for (const auto& c : chunks) {
        ASSERT_TRUE(buf.append(snii::Slice(c)).ok());
        expected.insert(expected.end(), c.begin(), c.end());
    }

    EXPECT_TRUE(buf.spilled());
    EXPECT_EQ(buf.size(), static_cast<uint64_t>(expected.size()));

    ASSERT_TRUE(buf.seal().ok());

    CapturingSniiFileWriter out;
    ASSERT_TRUE(buf.stream_into(&out).ok());
    expect_bytes_eq(out.bytes(), expected);
}

// Exercises the 0-byte branch and a >256 KiB chunk appended after the spill (the old standalone
// writer's "direct write" path); the total also exceeds stream_into()'s 1 MiB copy window so the
// read-back loop runs more than once.
TEST_F(SniiSpillIoTest, EmptyAndLargeChunk) {
    SpillableByteBuffer buf(/*cap_bytes=*/16, "largechunk");

    constexpr size_t kLarge = 1300U * 1024U; // > 256 KiB and > the 1 MiB stream window

    std::vector<std::vector<uint8_t>> chunks;
    chunks.push_back(make_bytes(10, 20));     // 20 >= 16 -> spill immediately, flushes [c0]
    chunks.push_back(make_bytes(11, 0));      // post-spill 0-byte append (len==0 branch)
    chunks.push_back(make_bytes(12, kLarge)); // post-spill large append straight to disk
    chunks.push_back(make_bytes(13, 5));      // post-spill tail

    std::vector<uint8_t> expected;
    for (const auto& c : chunks) {
        ASSERT_TRUE(buf.append(snii::Slice(c)).ok());
        expected.insert(expected.end(), c.begin(), c.end());
    }

    EXPECT_TRUE(buf.spilled());
    EXPECT_EQ(buf.size(), static_cast<uint64_t>(expected.size()));

    ASSERT_TRUE(buf.seal().ok());

    CapturingSniiFileWriter out;
    ASSERT_TRUE(buf.stream_into(&out).ok());
    expect_bytes_eq(out.bytes(), expected);
}

// A buffer that never crosses the cap stays RAM-resident; stream_into() must still reproduce the
// exact bytes from the in-memory chunk chain (no scratch file is ever created).
TEST_F(SniiSpillIoTest, RamOnlyRoundTripNoSpill) {
    SpillableByteBuffer buf(/*cap_bytes=*/UINT64_MAX, "ramonly"); // spilling disabled

    const std::vector<std::vector<uint8_t>> chunks = {make_bytes(30, 7), make_bytes(31, 0),
                                                      make_bytes(32, 13), make_bytes(33, 50)};

    std::vector<uint8_t> expected;
    for (const auto& c : chunks) {
        ASSERT_TRUE(buf.append(snii::Slice(c)).ok());
        expected.insert(expected.end(), c.begin(), c.end());
    }

    EXPECT_FALSE(buf.spilled());
    EXPECT_EQ(buf.size(), static_cast<uint64_t>(expected.size()));

    ASSERT_TRUE(buf.seal().ok()); // no-op for a RAM-resident buffer

    CapturingSniiFileWriter out;
    ASSERT_TRUE(buf.stream_into(&out).ok());
    expect_bytes_eq(out.bytes(), expected);
}

// The move-append path must spill and round-trip identically to the copy-append path.
TEST_F(SniiSpillIoTest, AppendMoveSpillRoundTrip) {
    SpillableByteBuffer buf(/*cap_bytes=*/16, "movespill");

    const std::vector<std::pair<uint32_t, size_t>> spec = {
            {40, 10}, // pre-spill (ram=10)
            {41, 0},  // empty, pre-spill
            {42, 12}, // crosses cap (ram=22) -> spill flushes [c0,c2]
            {43, 0},  // empty, post-spill
            {44, 7}   // post-spill
    };

    std::vector<uint8_t> expected;
    for (const auto& [seed, n] : spec) {
        std::vector<uint8_t> c = make_bytes(seed, n);
        expected.insert(expected.end(), c.begin(), c.end());
        ASSERT_TRUE(buf.append_move(std::move(c)).ok());
    }

    EXPECT_TRUE(buf.spilled());
    EXPECT_EQ(buf.size(), static_cast<uint64_t>(expected.size()));

    ASSERT_TRUE(buf.seal().ok());

    CapturingSniiFileWriter out;
    ASSERT_TRUE(buf.stream_into(&out).ok());
    expect_bytes_eq(out.bytes(), expected);
}

} // namespace
} // namespace snii::writer
