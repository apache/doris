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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/term_enum.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Helper: emit a segment from buffer data and return the .tis bytes.
std::vector<uint8_t> EmitTis(SpimiPostingBuffer& buffer, int32_t doc_count = 100) {
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    SpimiFulltextWriter::EmitSegment(buffer, sink, "_0", "content", doc_count,
                                     FieldInfosWriter::kIndexVersionV1,
                                     /*omit_term_freq_and_positions=*/false,
                                     /*omit_norms=*/true);
    return tis.bytes();
}

} // namespace

TEST(TermEnumTest, EmptySegmentIsImmediatelyDone) {
    SpimiPostingBuffer buffer;
    auto tis = EmitTis(buffer);
    TermEnum en(tis);
    EXPECT_TRUE(en.Done());
    EXPECT_EQ(en.TotalEntries(), 0);
    EXPECT_FALSE(en.Next());
}

TEST(TermEnumTest, SingleTermRoundTrip) {
    SpimiPostingBuffer buffer;
    buffer.Append("hello", 0, 0);
    auto tis = EmitTis(buffer);

    TermEnum en(tis);
    EXPECT_FALSE(en.Done());
    EXPECT_EQ(en.TotalEntries(), 1);
    ASSERT_TRUE(en.Next());
    EXPECT_EQ(en.Current().field_number, 0);
    EXPECT_EQ(en.Current().term_utf8, "hello");
    EXPECT_EQ(en.Current().info.doc_freq, 1);
    EXPECT_EQ(en.Current().info.freq_pointer, 0);
    EXPECT_EQ(en.Current().info.prox_pointer, 0);
    EXPECT_TRUE(en.Done());
    EXPECT_FALSE(en.Next());
}

TEST(TermEnumTest, MultipleTermsInSortedOrder) {
    SpimiPostingBuffer buffer;
    // Append out of order; EmitSegment sorts them.
    buffer.Append("cherry", 3, 0);
    buffer.Append("apple", 0, 0);
    buffer.Append("banana", 1, 0);
    buffer.Append("apple", 2, 1); // second doc for "apple"
    auto tis = EmitTis(buffer);

    TermEnum en(tis);
    EXPECT_EQ(en.TotalEntries(), 3);

    ASSERT_TRUE(en.Next());
    EXPECT_EQ(en.Current().term_utf8, "apple");
    EXPECT_EQ(en.Current().info.doc_freq, 2);

    ASSERT_TRUE(en.Next());
    EXPECT_EQ(en.Current().term_utf8, "banana");
    EXPECT_EQ(en.Current().info.doc_freq, 1);

    ASSERT_TRUE(en.Next());
    EXPECT_EQ(en.Current().term_utf8, "cherry");
    EXPECT_EQ(en.Current().info.doc_freq, 1);

    EXPECT_TRUE(en.Done());
}

TEST(TermEnumTest, PrefixDecodingSharesCommonPrefix) {
    SpimiPostingBuffer buffer;
    buffer.Append("apple", 0, 0);
    buffer.Append("apply", 1, 0); // shares "appl" with "apple"
    buffer.Append("application", 2, 0); // shares "appl" with "apply"
    auto tis = EmitTis(buffer);

    TermEnum en(tis);
    std::vector<std::string> terms;
    while (en.Next()) {
        terms.push_back(en.Current().term_utf8);
    }
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0], "apple");
    EXPECT_EQ(terms[1], "application");
    EXPECT_EQ(terms[2], "apply");
}

TEST(TermEnumTest, FreqAndProxPointersAccumulate) {
    SpimiPostingBuffer buffer;
    buffer.Append("aaa", 0, 0);
    buffer.Append("bbb", 1, 0);
    buffer.Append("ccc", 2, 0);
    auto tis = EmitTis(buffer);

    TermEnum en(tis);

    ASSERT_TRUE(en.Next());
    const auto fp0 = en.Current().info.freq_pointer;
    const auto pp0 = en.Current().info.prox_pointer;
    EXPECT_EQ(fp0, 0); // first term starts at 0
    EXPECT_EQ(pp0, 0);

    ASSERT_TRUE(en.Next());
    EXPECT_GT(en.Current().info.freq_pointer, fp0);
    EXPECT_GT(en.Current().info.prox_pointer, pp0);

    const auto fp1 = en.Current().info.freq_pointer;
    const auto pp1 = en.Current().info.prox_pointer;

    ASSERT_TRUE(en.Next());
    EXPECT_GT(en.Current().info.freq_pointer, fp1);
    EXPECT_GT(en.Current().info.prox_pointer, pp1);
}

TEST(TermEnumTest, HeaderFieldsAreCorrect) {
    SpimiPostingBuffer buffer;
    buffer.Append("test", 0, 0);
    auto tis = EmitTis(buffer);

    TermEnum en(tis);
    EXPECT_EQ(en.IndexInterval(), TermDictWriter::kDefaultIndexInterval);
    EXPECT_EQ(en.SkipInterval(), TermDictWriter::kDefaultSkipInterval);
}

TEST(TermEnumTest, TooSmallBufferThrows) {
    std::vector<uint8_t> tiny(16, 0); // too small (< 32)
    EXPECT_ANY_THROW({ TermEnum en(tiny); });
}

TEST(TermEnumTest, WrongFormatThrows) {
    // Construct a minimal buffer with correct size but wrong FORMAT.
    std::vector<uint8_t> bad(64, 0);
    // Write wrong FORMAT (big-endian): 0x00000001 instead of 0xFFFFFFFC (-4).
    bad[0] = 0x00;
    bad[1] = 0x00;
    bad[2] = 0x00;
    bad[3] = 0x01;
    EXPECT_ANY_THROW({ TermEnum en(bad); });
}

TEST(TermEnumTest, SkipOffsetPresentForLargePostingList) {
    SpimiPostingBuffer buffer;
    // Create a term with many docs (>= skip_interval=512).
    for (int d = 0; d < 600; ++d) {
        buffer.Append("frequent", static_cast<uint32_t>(d), 0);
    }
    auto tis = EmitTis(buffer);

    TermEnum en(tis);
    ASSERT_TRUE(en.Next());
    EXPECT_EQ(en.Current().term_utf8, "frequent");
    EXPECT_EQ(en.Current().info.doc_freq, 600);
    EXPECT_GT(en.Current().info.skip_offset, 0) << "df >= skip_interval → skip_offset > 0";
}

TEST(TermEnumTest, MultiPositionDocPreservesFreq) {
    SpimiPostingBuffer buffer;
    buffer.Append("multi", 0, 0);
    buffer.Append("multi", 0, 3);
    buffer.Append("multi", 0, 7);
    buffer.Append("multi", 5, 0);
    auto tis = EmitTis(buffer);

    TermEnum en(tis);
    ASSERT_TRUE(en.Next());
    EXPECT_EQ(en.Current().term_utf8, "multi");
    EXPECT_EQ(en.Current().info.doc_freq, 2); // 2 distinct docs
}

} // namespace doris::segment_v2::inverted_index::spimi
