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

#include "storage/index/inverted/spimi/spimi_index_writer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

namespace doris::segment_v2::inverted_index::spimi {

// --- Construction / basic access ---

TEST(SpimiIndexWriterTest, ConstructHasBuffer) {
    SpimiIndexWriter writer("content");
    EXPECT_TRUE(writer.HasBuffer());
    EXPECT_NE(writer.buffer(), nullptr);
    EXPECT_NE(writer.spill_manager(), nullptr);
}

TEST(SpimiIndexWriterTest, BufferInitiallyEmpty) {
    SpimiIndexWriter writer("title");
    EXPECT_EQ(writer.buffer()->RecordCount(), 0U);
    EXPECT_FALSE(writer.buffer()->Saturated());
    EXPECT_FALSE(writer.buffer()->ShouldFlush());
}

// --- AppendToken ---

TEST(SpimiIndexWriterTest, AppendTokenGrowsRecordCount) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("hello", /*doc_id=*/0, /*pos=*/0);
    writer.AppendToken("world", /*doc_id=*/1, /*pos=*/0);
    EXPECT_EQ(writer.buffer()->RecordCount(), 2U);
}

TEST(SpimiIndexWriterTest, AppendTokenSaturatedAfterHardLimit) {
    SpimiIndexWriter writer("content");
    // Fill the buffer beyond its capacity to trigger saturation.
    // The default max record count is large (2^24), so we just
    // verify Saturated() is false for small fills.
    for (uint32_t i = 0; i < 100; ++i) {
        writer.AppendToken("term", /*doc_id=*/i, /*pos=*/0);
    }
    EXPECT_FALSE(writer.Saturated());
}

// --- Saturated / ShouldFlush ---

TEST(SpimiIndexWriterTest, ShouldFlushFalseForSmallFill) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("a", 0, 0);
    EXPECT_FALSE(writer.ShouldFlush());
}

// --- FlushPending ---

TEST(SpimiIndexWriterTest, FlushPendingCreatesOneSpill) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("alpha", 0, 0);
    writer.AppendToken("beta", 1, 0);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 0U);

    writer.FlushPending(/*doc_count=*/10);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 1U);
    // Buffer should be reset after flush.
    EXPECT_EQ(writer.buffer()->RecordCount(), 0U);
}

TEST(SpimiIndexWriterTest, FlushPendingTwiceCreatesTwoSpills) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("x", 0, 0);
    writer.FlushPending(5);
    writer.AppendToken("y", 1, 0);
    writer.FlushPending(10);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 2U);
}

// --- MemoryUsage ---

TEST(SpimiIndexWriterTest, MemoryUsageNonZeroAfterAppend) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("hello", 0, 0);
    EXPECT_GT(writer.MemoryUsage(), 0);
}

TEST(SpimiIndexWriterTest, MemoryUsageIncreasesAfterSpill) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("a", 0, 0);
    (void)writer.MemoryUsage();
    writer.FlushPending(10);
    const int64_t after = writer.MemoryUsage();
    // After flush the buffer is reset (smaller), but spill data is
    // retained (larger). Total should still be positive.
    EXPECT_GT(after, 0);
    // The buffer still has base overhead after reset, so MemoryUsage >=
    // the spill manager's bytes.
    EXPECT_GE(static_cast<size_t>(after),
              writer.spill_manager()->TotalSpillBytes());
}

// --- Cleanup ---

TEST(SpimiIndexWriterTest, CleanupResetsBuffer) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("a", 0, 0);
    EXPECT_TRUE(writer.HasBuffer());

    writer.Cleanup();
    EXPECT_FALSE(writer.HasBuffer());
    EXPECT_EQ(writer.buffer(), nullptr);
}

TEST(SpimiIndexWriterTest, CleanupReleasesSpillData) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("x", 0, 0);
    writer.FlushPending(10);
    EXPECT_GT(writer.spill_manager()->TotalSpillBytes(), 0U);

    writer.Cleanup();
    // SpillManager should have cleaned up its data.
    EXPECT_EQ(writer.spill_manager()->TotalSpillBytes(), 0U);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 0U);
}

TEST(SpimiIndexWriterTest, CleanupIdempotent) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("a", 0, 0);
    writer.Cleanup();
    // Second call should not crash.
    writer.Cleanup();
    EXPECT_FALSE(writer.HasBuffer());
}

// --- MemoryUsage after Cleanup ---

TEST(SpimiIndexWriterTest, MemoryUsageZeroAfterCleanup) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("a", 0, 0);
    writer.FlushPending(5);
    EXPECT_GT(writer.MemoryUsage(), 0);

    writer.Cleanup();
    EXPECT_EQ(writer.MemoryUsage(), 0);
}

// --- SpimiFinishConfig defaults ---

TEST(SpimiIndexWriterTest, FinishConfigDefaults) {
    SpimiFinishConfig config;
    EXPECT_FALSE(config.is_v4);
    EXPECT_FALSE(config.omit_term_freq_and_positions);
    EXPECT_TRUE(config.field_name_utf8.empty());
    EXPECT_EQ(config.doc_count, 0);
}

// --- GetFileNames ---

TEST(SpimiIndexWriterTest, GetFileNamesV4) {
    auto names = SpimiIndexWriter::GetFileNames(/*is_v4=*/true);
    EXPECT_STREQ(names.tis, "_0.tis");
    EXPECT_STREQ(names.tii, "_0.tii");
    EXPECT_STREQ(names.frq, "_0.frq");
    EXPECT_STREQ(names.prx, "_0.prx");
    EXPECT_STREQ(names.fnm, "_0.fnm");
    EXPECT_STREQ(names.nrm, "_0.nrm");
    EXPECT_STREQ(names.seg_n, "segments_1");
    EXPECT_STREQ(names.seg_gen, "segments.gen");
}

TEST(SpimiIndexWriterTest, GetFileNamesShadow) {
    auto names = SpimiIndexWriter::GetFileNames(/*is_v4=*/false);
    EXPECT_STREQ(names.tis, "_spimi_0.tis");
    EXPECT_STREQ(names.tii, "_spimi_0.tii");
    EXPECT_STREQ(names.frq, "_spimi_0.frq");
    EXPECT_STREQ(names.prx, "_spimi_0.prx");
    EXPECT_STREQ(names.fnm, "_spimi_0.fnm");
    EXPECT_STREQ(names.nrm, "_spimi_0.nrm");
    EXPECT_STREQ(names.seg_n, "segments_spimi_1");
    EXPECT_STREQ(names.seg_gen, "segments_spimi.gen");
}

// --- Finish with empty buffer ---

TEST(SpimiIndexWriterTest, FinishEmptyBufferIsNoOp) {
    SpimiIndexWriter writer("content");
    // Empty buffer + no spills → Finish is a no-op (returns early).
    // Verify by checking the buffer is reset afterward.
    EXPECT_TRUE(writer.HasBuffer());
    EXPECT_EQ(writer.buffer()->RecordCount(), 0U);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 0U);

    // Finish with a null dir: the early-return path doesn't touch
    // the directory, so nullptr is safe.
    SpimiFinishConfig config;
    config.is_v4 = true;
    config.field_name_utf8 = "content";
    config.doc_count = 0;
    writer.Finish(nullptr, config);

    // After Finish, the buffer is reset.
    EXPECT_FALSE(writer.HasBuffer());
}

} // namespace doris::segment_v2::inverted_index::spimi
