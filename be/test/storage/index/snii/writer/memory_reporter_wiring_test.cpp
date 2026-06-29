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

// P1 wiring + P3 seam: verifies that the writer-level MemoryReporter sees the REAL
// resident-byte deltas reported by SpillableByteBuffer (dict side: ram_bytes_) and
// SpimiTermBuffer (SPIMI side: arena_bytes() + slot_of_.capacity()*4), positive on
// grow and negative on every free, exactly once -- so the counter returns to a
// known measurement after a build and to 0 after a full drain (a missed negative
// would be a Doris LOAD-tracker leak). The reporter must NEVER count live_bytes_.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/io/file_writer.h"
#include "snii/writer/memory_reporter.h"
#include "snii/writer/spillable_byte_buffer.h"
#include "snii/writer/spimi_term_buffer.h"

namespace snii::writer {
using doris::Status;
namespace {

// Discards appended bytes; only counts how many were written (SpillableByteBuffer
// stream_into / spill targets need a sink, but these tests check the reporter).
class NullWriter : public snii::io::FileWriter {
public:
    Status append(Slice s) override {
        written_ += s.size();
        return Status::OK();
    }
    Status finalize() override { return Status::OK(); }
    uint64_t bytes_written() const override { return written_; }

private:
    uint64_t written_ = 0;
};

std::vector<uint8_t> Block(size_t n, uint8_t seed) {
    std::vector<uint8_t> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<uint8_t>((i + seed) & 0xFF);
    }
    return v;
}

// ---- dict side (SpillableByteBuffer) ---------------------------------------

// Every RAM append reports exactly its byte count; the reporter equals the
// RAM-resident size, which (un-spilled) equals buf.size().
TEST(SniiMemoryReporterWiring, DictRamDeltasMatchSize) {
    MemoryReporter reporter;
    SpillableByteBuffer buf(/*cap=*/UINT64_MAX, "dict", &reporter);
    int64_t total = 0;
    for (int b = 0; b < 5; ++b) {
        auto chunk = Block(1000 + b, static_cast<uint8_t>(b));
        total += static_cast<int64_t>(chunk.size());
        ASSERT_TRUE(buf.append(Slice(chunk)).ok());
        EXPECT_EQ(reporter.current_bytes(), total);
    }
    // Move-append path reports identically.
    auto moved = Block(777, 9);
    total += 777;
    ASSERT_TRUE(buf.append_move(std::move(moved)).ok());
    EXPECT_EQ(reporter.current_bytes(), total);
    EXPECT_EQ(reporter.current_bytes(), static_cast<int64_t>(buf.size()));
}

// Destroying an UN-spilled buffer must release its resident RAM (the common path:
// most dict buffers never spill). A missed negative here would leak into Doris's
// LOAD MemTracker. After the buffer's scope, the reporter must be back to 0.
TEST(SniiMemoryReporterWiring, DictDtorReleasesUnspilledRam) {
    MemoryReporter reporter;
    {
        SpillableByteBuffer buf(/*cap=*/UINT64_MAX, "dict", &reporter);
        ASSERT_TRUE(buf.append(Slice(Block(5000, 1))).ok());
        ASSERT_TRUE(buf.append_move(Block(3000, 2)).ok());
        EXPECT_EQ(reporter.current_bytes(), 8000);
        EXPECT_FALSE(buf.spilled());
    } // ~SpillableByteBuffer must report -8000
    EXPECT_EQ(reporter.current_bytes(), 0);
}

// UNIFIED gate-2: a small cap on the shared reporter triggers the dict spill BEFORE
// its (huge) local cap_bytes_ is reached -- proving the spill is driven by the writer's
// total RAM (reporter->over_cap()), not a per-buffer threshold.
TEST(SniiMemoryReporterWiring, UnifiedCapDrivesDictSpill) {
    MemoryReporter reporter(/*consume_release=*/nullptr, /*cap_bytes=*/8000);
    SpillableByteBuffer buf(/*local cap=*/UINT64_MAX, "dict", &reporter);
    ASSERT_TRUE(buf.append(Slice(Block(5000, 1))).ok());
    EXPECT_FALSE(buf.spilled());                         // 5000 < unified cap 8000
    ASSERT_TRUE(buf.append(Slice(Block(4000, 2))).ok()); // 9000 >= 8000 -> spill
    EXPECT_TRUE(buf.spilled());                          // unified cap fired, not local
    EXPECT_EQ(reporter.current_bytes(), 0);              // resident handed to disk
    ASSERT_TRUE(buf.seal().ok());
}

// A spill drops the resident tier: the reporter returns to 0 (one negative ==
// prior ram_bytes_) while buf.size() still counts the on-disk bytes.
TEST(SniiMemoryReporterWiring, DictSpillFreesRam) {
    MemoryReporter reporter;
    SpillableByteBuffer buf(/*cap=*/8192, "dict", &reporter);
    uint64_t total = 0;
    for (int b = 0; b < 8; ++b) { // 8 x 4 KiB through an 8 KiB cap -> spills
        auto chunk = Block(4096, static_cast<uint8_t>(b));
        total += chunk.size();
        ASSERT_TRUE(buf.append(Slice(chunk)).ok());
    }
    EXPECT_TRUE(buf.spilled());
    // RAM tier was handed to disk -> reporter back to 0; size() still totals bytes.
    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_EQ(buf.size(), total);
    ASSERT_TRUE(buf.seal().ok());
}

// ---- SPIMI side (SpimiTermBuffer) ------------------------------------------

// A null reporter must not affect behavior (default off-Doris path).
TEST(SniiMemoryReporterWiring, SpimiNullReporterIsSafe) {
    std::vector<std::string> vocab = {"a", "b", "c"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/true, /*spill=*/0, /*reporter=*/nullptr);
    buf.add_token(0, 0, 0);
    buf.add_token(1, 0, 1);
    auto terms = buf.finalize_sorted();
    EXPECT_EQ(terms.size(), 2U);
}

// Borrowed-vocab construction reports the vocab-sized slot index up front
// (slot_of_.capacity()*4). assign(N,0) makes capacity == N, so the initial report
// is exactly N*4 -- a directly-measurable accuracy check.
TEST(SniiMemoryReporterWiring, SpimiInitialSlotIndexIsExact) {
    std::vector<std::string> vocab(64, "x");
    MemoryReporter reporter;
    SpimiTermBuffer buf(&vocab, /*has_positions=*/true, /*spill=*/0, &reporter);
    EXPECT_EQ(reporter.current_bytes(), static_cast<int64_t>(vocab.size()) * 4);
}

// Adding tokens grows the reporter ABOVE the initial slot-index figure (the arena
// is now non-empty), and a full drain returns it to 0 -- every negative reported,
// no leak. Also proves the counter tracks arena_bytes(), not the (default-mode)
// live_bytes_ estimate, which stays 0 when spill_threshold_bytes_ == 0.
TEST(SniiMemoryReporterWiring, SpimiGrowThenDrainNetZero) {
    std::vector<std::string> vocab = {"alpha", "beta", "gamma", "delta"};
    MemoryReporter reporter;
    SpimiTermBuffer buf(&vocab, /*has_positions=*/true, /*spill=*/0, &reporter);
    const int64_t base = reporter.current_bytes(); // slot index only
    EXPECT_EQ(base, static_cast<int64_t>(vocab.size()) * 4);
    for (uint32_t doc = 0; doc < 200; ++doc) {
        for (uint32_t t = 0; t < 4; ++t) {
            buf.add_token(t, doc, /*pos=*/0);
        }
    }
    // Arena is now resident -> strictly above the slot-index-only baseline. Because
    // spill_threshold_bytes_ == 0, live_bytes_ is never accumulated; a counter that
    // mistakenly reported live_bytes_ would still be at `base` here.
    EXPECT_GT(reporter.current_bytes(), base);
    auto terms = buf.finalize_sorted();
    EXPECT_EQ(terms.size(), 4U);
    // Drain freed the arena + slot index: every negative reported -> exactly 0.
    EXPECT_EQ(reporter.current_bytes(), 0);
}

// Streaming drain (for_each_term_sorted) frees identically -> net zero.
TEST(SniiMemoryReporterWiring, SpimiStreamingDrainNetZero) {
    std::vector<std::string> vocab = {"k0", "k1", "k2"};
    MemoryReporter reporter;
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false, /*spill=*/0, &reporter);
    for (uint32_t doc = 0; doc < 100; ++doc) {
        for (uint32_t t = 0; t < 3; ++t) {
            buf.add_token(t, doc, 0);
        }
    }
    EXPECT_GT(reporter.current_bytes(), 0);
    size_t seen = 0;
    // Integrated for_each_term_sorted returns a [[nodiscard]] doris::Status (drift
    // from the standalone void); consume it and assert the happy-path drain succeeds.
    ASSERT_TRUE(buf.for_each_term_sorted([&seen](TermPostings&&) { ++seen; }).ok());
    EXPECT_EQ(seen, 3U);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

// The spilled finalize path (merge_runs) must also balance: a small spill threshold
// forces at least one spill mid-build; after the full drain the reporter is 0 (the
// spill's arena-reset negative AND merge_runs' slot_of_ free are both reported).
TEST(SniiMemoryReporterWiring, SpimiSpillPathNetZero) {
    std::vector<std::string> vocab = {"w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7"};
    // Tiny UNIFIED cap on the reporter so the REAL resident size (arena + slot index)
    // crosses it early and forces spills, exercising the drain_to_writer / merge_runs
    // free sites. (The local spill_threshold arg is unused once a reporter is attached.)
    MemoryReporter reporter(/*consume_release=*/nullptr, /*cap_bytes=*/256);
    SpimiTermBuffer buf(&vocab, /*has_positions=*/true, /*spill=*/0, &reporter);
    for (uint32_t doc = 0; doc < 500; ++doc) {
        for (uint32_t t = 0; t < 8; ++t) {
            buf.add_token(t, doc, doc & 7);
        }
    }
    size_t seen = 0;
    // for_each_term_sorted now surfaces spill/merge I/O via a [[nodiscard]] Status.
    ASSERT_TRUE(buf.for_each_term_sorted([&seen](TermPostings&&) { ++seen; }).ok());
    EXPECT_TRUE(buf.status().ok());
    EXPECT_EQ(seen, 8U);
    // All spill/merge frees reported -> back to 0 (no MemTracker leak).
    EXPECT_EQ(reporter.current_bytes(), 0);
}

// Destructor balance: a buffer destroyed WITHOUT a drain (e.g. an aborted build)
// must return its reported resident bytes so nothing leaks in the tracker.
TEST(SniiMemoryReporterWiring, SpimiDestructorBalancesOnAbort) {
    std::vector<std::string> vocab = {"p", "q", "r"};
    MemoryReporter reporter;
    {
        SpimiTermBuffer buf(&vocab, /*has_positions=*/true, /*spill=*/0, &reporter);
        for (uint32_t doc = 0; doc < 50; ++doc) {
            for (uint32_t t = 0; t < 3; ++t) {
                buf.add_token(t, doc, 0);
            }
        }
        EXPECT_GT(reporter.current_bytes(), 0);
        // No drain: leave the build incomplete and let the destructor run.
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

} // namespace
} // namespace snii::writer
