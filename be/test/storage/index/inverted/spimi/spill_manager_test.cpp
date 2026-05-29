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

#include "storage/index/inverted/spimi/spill_manager.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2::inverted_index::spimi {

TEST(SpillManagerTest, EmptyBufferFlushProducesNoSpill) {
    SpillManager mgr("content");
    SpimiPostingBuffer buffer;
    const int64_t terms = mgr.FlushBuffer(buffer, /*doc_count=*/100);
    EXPECT_EQ(terms, 0);
    EXPECT_EQ(mgr.SpillCount(), 0U);
    EXPECT_TRUE(mgr.Spills().empty());
    EXPECT_EQ(mgr.TotalSpillBytes(), 0U);
}

TEST(SpillManagerTest, SingleFlushCreatesOneSpill) {
    SpillManager mgr("title");
    SpimiPostingBuffer buffer;
    buffer.Append("hello", /*doc=*/0, /*pos=*/0);
    buffer.Append("world", /*doc=*/1, /*pos=*/0);

    const int64_t terms = mgr.FlushBuffer(buffer, /*doc_count=*/10);
    EXPECT_EQ(terms, 2);
    EXPECT_EQ(mgr.SpillCount(), 1U);

    const auto& spill = mgr.Spills()[0];
    EXPECT_EQ(spill.segment_name, "_spill_0");
    EXPECT_EQ(spill.doc_count, 10);
    EXPECT_EQ(spill.term_count, 2);
    EXPECT_FALSE(spill.tis_bytes.empty());
    EXPECT_FALSE(spill.frq_bytes.empty());
    EXPECT_FALSE(spill.prx_bytes.empty());
}

TEST(SpillManagerTest, MultipleFlushesIncrementCount) {
    SpillManager mgr("body");
    {
        SpimiPostingBuffer buf;
        buf.Append("alpha", 0, 0);
        mgr.FlushBuffer(buf, 5);
    }
    {
        SpimiPostingBuffer buf;
        buf.Append("beta", 0, 0);
        buf.Append("gamma", 1, 0);
        mgr.FlushBuffer(buf, 10);
    }
    {
        SpimiPostingBuffer buf;
        buf.Append("delta", 0, 0);
        mgr.FlushBuffer(buf, 15);
    }

    EXPECT_EQ(mgr.SpillCount(), 3U);
    EXPECT_EQ(mgr.Spills()[0].segment_name, "_spill_0");
    EXPECT_EQ(mgr.Spills()[1].segment_name, "_spill_1");
    EXPECT_EQ(mgr.Spills()[2].segment_name, "_spill_2");
    EXPECT_EQ(mgr.Spills()[0].doc_count, 5);
    EXPECT_EQ(mgr.Spills()[1].doc_count, 10);
    EXPECT_EQ(mgr.Spills()[2].doc_count, 15);
}

TEST(SpillManagerTest, CleanupSpillFilesClearsAll) {
    SpillManager mgr("field");
    SpimiPostingBuffer buf;
    buf.Append("term", 0, 0);
    mgr.FlushBuffer(buf, 1);
    EXPECT_GT(mgr.TotalSpillBytes(), 0U);

    mgr.CleanupSpillFiles();
    EXPECT_EQ(mgr.SpillCount(), 0U);
    EXPECT_EQ(mgr.TotalSpillBytes(), 0U);
}

TEST(SpillManagerTest, TotalSpillBytesAccountsForAllStreams) {
    SpillManager mgr("f");
    SpimiPostingBuffer buf;
    buf.Append("test", 0, 0);
    buf.Append("test", 0, 1);
    buf.Append("test", 1, 0);
    mgr.FlushBuffer(buf, 2);

    const size_t total = mgr.TotalSpillBytes();
    const auto& s = mgr.Spills()[0];
    const size_t expected = s.tis_bytes.size() + s.tii_bytes.size() + s.frq_bytes.size() +
                            s.prx_bytes.size() + s.fnm_bytes.size() +
                            s.segments_n_bytes.size() + s.segments_gen_bytes.size();
    EXPECT_EQ(total, expected);
}

TEST(SpillManagerTest, BufferIsEmptyAfterFlush) {
    SpillManager mgr("f");
    SpimiPostingBuffer buf;
    buf.Append("a", 0, 0);
    buf.Append("b", 1, 0);
    EXPECT_EQ(buf.RecordCount(), 2U);
    EXPECT_EQ(buf.TermCount(), 2U);

    mgr.FlushBuffer(buf, 2);
    // After flush, buffer should be reset.
    EXPECT_EQ(buf.RecordCount(), 0U);
    EXPECT_EQ(buf.TermCount(), 0U);
}

TEST(SpillManagerTest, SpillSegmentsHaveCompleteManifest) {
    SpillManager mgr("content");
    SpimiPostingBuffer buf;
    buf.Append("word", 0, 0);
    mgr.FlushBuffer(buf, 1);

    const auto& s = mgr.Spills()[0];
    // segments_N and segments.gen must be populated.
    EXPECT_FALSE(s.segments_n_bytes.empty());
    EXPECT_FALSE(s.segments_gen_bytes.empty());
    // .fnm must be populated (field info).
    EXPECT_FALSE(s.fnm_bytes.empty());
}

TEST(SpillManagerTest, FlushResetsFlushNeededLatch) {
    SpillManager mgr("f");
    SpimiPostingBuffer buf(0U, SpimiPostingBuffer::Limits {
                                       .max_term_bytes = 65536,
                                       .max_arena_bytes = 1 << 20,
                                       .max_record_count = 1 << 20,
                                       .memory_budget_bytes = 64,
                               });
    // Append enough to cross the tiny budget.
    for (int i = 0; i < 100; ++i) {
        buf.Append("term", static_cast<uint32_t>(i), 0);
    }
    EXPECT_TRUE(buf.ShouldFlush());

    mgr.FlushBuffer(buf, 100);
    // After flush, the latch should be cleared.
    EXPECT_FALSE(buf.ShouldFlush());
}

// Helper: fill buffer with enough records to activate compact mode.
// Uses sequential doc_ids (matching Doris's monotonic guarantee)
// so _compact_streams_sorted stays true and the fast path is used.
static void FillBufferForCompact(SpimiPostingBuffer& buffer,
                                 const std::vector<std::string>& vocab,
                                 int64_t num_records) {
    uint32_t doc_id = 0;
    uint32_t pos = 0;
    for (int64_t i = 0; i < num_records; ++i) {
        const auto& term = vocab[i % vocab.size()];
        buffer.Append(term, doc_id, pos++);
        if (pos >= 10) {
            ++doc_id;
            pos = 0;
        }
    }
}

TEST(SpillManagerTest, FlushBufferWithCompactModeData) {
    // Regression test: FlushBuffer previously used buffer.records().empty()
    // which incorrectly returned true in compact mode, causing data loss.
    // With the fix (RecordCount() == 0 check), compact-mode buffers
    // must be flushed correctly.
    SpillManager mgr("content");
    SpimiPostingBuffer buffer;
    const std::vector<std::string> vocab = {"a", "b", "c"};
    // Must use >= 600 records to cross kCompactCheckEvery=512 boundary.
    FillBufferForCompact(buffer, vocab, 600);

    // Verify compact mode activated.
    EXPECT_TRUE(buffer.records().empty());
    EXPECT_EQ(buffer.RecordCount(), 600U);

    const int64_t terms = mgr.FlushBuffer(buffer, 60);
    EXPECT_GT(terms, 0);
    EXPECT_EQ(mgr.SpillCount(), 1U);

    const auto& spill = mgr.Spills()[0];
    EXPECT_EQ(spill.term_count, terms);
    EXPECT_FALSE(spill.tis_bytes.empty());
}

} // namespace doris::segment_v2::inverted_index::spimi
