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

// T05: SPIMI vocab transparent-hash interning + single-string storage.
//
// These tests pin the writer-side SpimiTermBuffer owned-mode interning to its new
// shape: each distinct vocab string is materialized into owned_vocab_ EXACTLY ONCE
// (no double-store, no per-token temporary probe std::string), and the term-id
// assignment / finalize output is byte-identical to the prior behavior. Writer-only,
// no reader fixture (build_reader) needed -- the buffer is driven directly via
// add_token(string_view) and drained via finalize_sorted().

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "util/md5.h"

using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::StreamedTermPostings;
using doris::snii::writer::TermPostingBuffer;
using doris::snii::writer::TermPostings;
using doris::Status;
// Alias the writer's TEST-ONLY counter namespace so it never collides with gtest's
// own ::testing namespace.
namespace stb_testing = doris::snii::writer::testing;

namespace {

// An ordinary >SSO ASCII term with a prefix shared by several cases below.
std::string MixedLongAscii() {
    return "ordinary-prefix-sharing-quick-brown-term";
}

// A >SSO multibyte (CJK) token (a direct UTF-8 string literal): 18 bytes (6 chars x
// 3 bytes), well past libstdc++'s 15-byte SSO, exercising a long non-ASCII vocab key.
std::string MixedCjk() {
    return "中文长词条目";
}

// Feeds a fixed, implementation-independent token script mixing a long ASCII
// term, a short ASCII term, and a >SSO CJK token across several docids (some
// re-touched). Two buffers fed this script must finalize identically.
void FeedMixedScript(SpimiTermBuffer& b) {
    const std::string long_ascii = MixedLongAscii();
    const std::string cjk = MixedCjk();
    b.add_token(std::string_view("alpha"), 0, 0);
    b.add_token(std::string_view(long_ascii), 0, 1);
    b.add_token(std::string_view(cjk), 0, 2);
    b.add_token(std::string_view("alpha"), 1, 0);
    b.add_token(std::string_view(long_ascii), 1, 1);
    b.add_token(std::string_view("alpha"), 5, 3);
    b.add_token(std::string_view(cjk), 5, 4);
    b.add_token(std::string_view("alpha"), 9, 0);
}

void ExpectPostingsEqual(const std::vector<TermPostings>& a, const std::vector<TermPostings>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].term, b[i].term);
        EXPECT_EQ(a[i].docids, b[i].docids);
        EXPECT_EQ(a[i].freqs, b[i].freqs);
        EXPECT_EQ(a[i].positions_flat, b[i].positions_flat);
        EXPECT_EQ(a[i].retain_positions, b[i].retain_positions);
    }
}


} // namespace

TEST(SniiSpimiTermBufferTest, InternHashUsesFastStringViewHash) {
    const std::vector<std::string> terms = {
            "",
            "short",
            MixedLongAscii(),
            MixedCjk(),
            std::string("\x1F\x01\0\x03gram-key", 12),
            std::string("embedded\0nul", 12),
    };

    for (const std::string& term : terms) {
        const std::string_view view(term);
        EXPECT_EQ(SpimiTermBuffer::hash_term_bytes_for_test(view),
                  std::hash<std::string_view> {}(view));
    }
}

TEST(SniiSpimiTermBufferTest, OrdinaryDocsOnlyMarkerTermRetainsFrequency) {
    // 落在内部命名空间（\x1f 开头）里的普通词项也只是普通词项：照常记录频次。
    const std::string literal_marker_term = std::string("\x1f") + "SNII_TEST_MARKER\x1f" + "literal";
    SpimiTermBuffer ordinary(/*has_positions=*/false);
    ordinary.add_token(literal_marker_term, /*docid=*/3, /*pos=*/0,
                       /*retain_positions=*/false);

    const std::vector<TermPostings> terms = ordinary.finalize_sorted();
    ASSERT_TRUE(ordinary.status().ok()) << ordinary.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, literal_marker_term);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {3U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U}));
}

TEST(SniiSpimiTermBufferTest, CompactPostingPoolVarintFastPathCrossesSliceBoundary) {
    doris::snii::writer::CompactPostingPool pool;
    doris::snii::writer::CompactPostingPool::SliceWriter writer;
    uint8_t level = 0;
    const uint32_t head = pool.start_chain(&writer, &level);
    for (uint32_t i = 1; i < doris::snii::writer::CompactPostingPool::kSliceSizes_level0(); ++i) {
        pool.append_byte(&writer, &level, 0);
    }

    const std::array<uint64_t, 5> values = {
            300, 0, 127, 128, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())};
    for (const uint64_t value : values) {
        pool.append_varint(&writer, &level, value);
    }

    auto cursor = pool.cursor(head, writer.cur);
    for (uint32_t i = 1; i < doris::snii::writer::CompactPostingPool::kSliceSizes_level0(); ++i) {
        EXPECT_EQ(cursor.next(), 0U);
    }
    for (const uint64_t expected : values) {
        EXPECT_EQ(cursor.read_varint(), expected);
    }
}

TEST(SniiSpimiTermBufferTest, PerTermDocsOnlyDropsSameDocOccurrencesBeforeDecode) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token("docs", /*docid=*/5, /*pos=*/10, /*retain_positions=*/false);
    buf.add_token("docs", /*docid=*/5, /*pos=*/11, /*retain_positions=*/false);
    buf.add_token("docs", /*docid=*/1, /*pos=*/3, /*retain_positions=*/false);
    buf.add_token("docs", /*docid=*/5, /*pos=*/12, /*retain_positions=*/false);
    buf.add_token("positioned", /*docid=*/5, /*pos=*/20, /*retain_positions=*/true);
    buf.add_token("positioned", /*docid=*/5, /*pos=*/21, /*retain_positions=*/true);

    EXPECT_EQ(buf.total_tokens(), 6U);
    const std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term, "docs");
    EXPECT_FALSE(terms[0].retain_positions);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U, 5U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 1U}));
    EXPECT_TRUE(terms[0].positions_flat.empty());
    EXPECT_EQ(terms[1].term, "positioned");
    EXPECT_TRUE(terms[1].retain_positions);
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {5U}));
    EXPECT_EQ(terms[1].freqs, (std::vector<uint32_t> {2U}));
    EXPECT_EQ(terms[1].positions_flat, (std::vector<uint32_t> {20U, 21U}));
}

// ---------------------------------------------------------------------------------
// Functional verification (FV1-FV9)
// ---------------------------------------------------------------------------------

// FV1: ids are assigned in first-seen order (b=0,a=1,c=2) but the emitted order is
// lexicographic (a,b,c); docids/freqs are recovered correctly.
TEST(SniiSpimiTermBufferTest, VocabAssignsIdsInFirstSeenOrder) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token(std::string_view("b"), 0, 0);
    buf.add_token(std::string_view("a"), 1, 0);
    buf.add_token(std::string_view("b"), 2, 0);
    buf.add_token(std::string_view("c"), 3, 0);
    buf.add_token(std::string_view("a"), 4, 0);

    EXPECT_EQ(buf.unique_terms(), 3U);
    EXPECT_EQ(buf.total_tokens(), 5U);
    EXPECT_TRUE(buf.status().ok());

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[1].term, "b");
    EXPECT_EQ(terms[2].term, "c");
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U, 4U}));
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {0U, 2U}));
    EXPECT_EQ(terms[2].docids, (std::vector<uint32_t> {3U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 1U}));
    EXPECT_EQ(terms[1].freqs, (std::vector<uint32_t> {1U, 1U}));
    EXPECT_EQ(terms[2].freqs, (std::vector<uint32_t> {1U}));
}

// FV2: the same >SSO ordinary term fed 1000 times reuses ONE id (heterogeneous hit
// path), yielding a single term with 1000 ascending docids and freq 1 each.
TEST(SniiSpimiTermBufferTest, RepeatedTermReusesSingleId) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    const std::string term = MixedLongAscii();
    ASSERT_GT(term.size(), 15U); // exceeds libstdc++ SSO: the OLD probe heap-allocated

    constexpr uint32_t kRepeats = 1000;
    for (uint32_t d = 0; d < kRepeats; ++d) {
        buf.add_token(std::string_view(term), d, 0);
    }
    EXPECT_EQ(buf.unique_terms(), 1U);
    EXPECT_EQ(buf.total_tokens(), kRepeats);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, term);
    ASSERT_EQ(terms[0].docids.size(), kRepeats);
    for (uint32_t d = 0; d < kRepeats; ++d) {
        EXPECT_EQ(terms[0].docids[d], d);
        EXPECT_EQ(terms[0].freqs[d], 1U);
    }
}

// FV3 (also the byte-identity perf gate): two independent buffers fed the same mixed
// script (long ASCII + plain + CJK token) finalize to element-identical postings.
TEST(SniiSpimiTermBufferTest, FinalizeIsByteIdenticalAcrossRuns) {
    SpimiTermBuffer a(/*has_positions=*/true);
    SpimiTermBuffer b(/*has_positions=*/true);
    FeedMixedScript(a);
    FeedMixedScript(b);

    std::vector<TermPostings> ra = a.finalize_sorted();
    std::vector<TermPostings> rb = b.finalize_sorted();
    ExpectPostingsEqual(ra, rb);
    EXPECT_TRUE(a.status().ok());
    EXPECT_TRUE(b.status().ok());

    bool saw_long_ascii = false;
    bool saw_cjk = false;
    for (const auto& tp : ra) {
        if (tp.term == MixedLongAscii()) {
            saw_long_ascii = true;
        }
        if (tp.term == MixedCjk()) {
            saw_cjk = true;
        }
    }
    EXPECT_TRUE(saw_long_ascii);
    EXPECT_TRUE(saw_cjk);
}

// FV4: no tokens -> empty result, status stays OK.
TEST(SniiSpimiTermBufferTest, EmptyVocabProducesNoTerms) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    EXPECT_EQ(buf.unique_terms(), 0U);
    std::vector<TermPostings> terms = buf.finalize_sorted();
    EXPECT_TRUE(terms.empty());
    EXPECT_TRUE(buf.status().ok());
}

// FV5: a single token yields a single term, single docid, freq 1.
TEST(SniiSpimiTermBufferTest, SingleTokenProducesSingleTerm) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token(std::string_view("solo"), 7, 3);
    EXPECT_EQ(buf.unique_terms(), 1U);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, "solo");
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {7U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U}));
    EXPECT_EQ(terms[0].positions_flat, (std::vector<uint32_t> {3U}));
}

// FV6: the empty string is a valid distinct term; the heterogeneous equality functor
// matches "" against a stored "" so a repeat empty token reuses the same id.
TEST(SniiSpimiTermBufferTest, EmptyStringIsAValidDistinctTerm) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token(std::string_view(""), 0, 0); // empty term, first occurrence
    buf.add_token(std::string_view("x"), 1, 0);
    buf.add_token(std::string_view(""), 2, 0); // empty term reused via transparent eq

    EXPECT_EQ(buf.unique_terms(), 2U);
    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term, ""); // "" sorts before "x"
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {0U, 2U}));
    EXPECT_EQ(terms[1].term, "x");
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {1U}));
    EXPECT_TRUE(buf.status().ok());
}

// FV7: add_token(string_view) on a BORROWED-vocab buffer is rejected (latches
// InvalidArgument, token ignored). The interning functors hold &owned_vocab_ but are
// never dereferenced on this path (reject happens before the find), so empty
// owned_vocab_ is never indexed out of bounds.
TEST(SniiSpimiTermBufferTest, BorrowedModeRejectsStringView) {
    const std::vector<std::string> vocab = {"a", "b"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false);
    buf.add_token(0U, 0, 0);                    // valid id-path token
    buf.add_token(std::string_view("a"), 1, 0); // illegal on a borrowed-vocab buffer

    EXPECT_FALSE(buf.status().ok());
    EXPECT_EQ(buf.total_tokens(), 1U); // the string-view token was ignored
    EXPECT_EQ(buf.unique_terms(), 1U);
}

// FV8: long ordinary terms sharing a prefix remain distinct and round-trip with
// exact bytes.
TEST(SniiSpimiTermBufferTest, PrefixSharingOrdinaryTermsRoundTrip) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    const std::string first = "ordinary-prefix-sharing-alpha-suffix";
    const std::string second = "ordinary-prefix-sharing-beta-suffix";
    const std::string third = "ordinary-prefix-sharing-gamma-suffix";
    buf.add_token(std::string_view(first), 0, 0);
    buf.add_token(std::string_view(second), 0, 1);
    buf.add_token(std::string_view(third), 0, 2);

    EXPECT_EQ(buf.unique_terms(), 3U);
    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 3U);

    bool saw_first = false;
    bool saw_second = false;
    bool saw_third = false;
    for (const auto& tp : terms) {
        if (tp.term == first) {
            saw_first = true;
        } else if (tp.term == second) {
            saw_second = true;
        } else if (tp.term == third) {
            saw_third = true;
        }
    }
    EXPECT_TRUE(saw_first);
    EXPECT_TRUE(saw_second);
    EXPECT_TRUE(saw_third);
}

// FV9: out-of-order / revisited docids for one term coalesce into one strictly
// ascending entry per docid (orthogonal to the intern change; guards no regression).
TEST(SniiSpimiTermBufferTest, OutOfOrderDocidCoalesces) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token(std::string_view("t"), 5, 50);
    buf.add_token(std::string_view("t"), 1, 10);
    buf.add_token(std::string_view("t"), 5, 52); // revisit doc 5

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U, 5U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 2U}));
    EXPECT_EQ(terms[0].positions_flat, (std::vector<uint32_t> {10U, 50U, 52U}));
    EXPECT_TRUE(buf.status().ok());
}

// ---------------------------------------------------------------------------------
// Deterministic performance verification (allocation seam counts)
// ---------------------------------------------------------------------------------

// The same >SSO term fed M times materializes its string EXACTLY ONCE: no per-token
// temporary probe std::string (F21) and no second owned-string map key (F03). The
// OLD instrumented baseline would have been M temporaries + 1 emplace = M+1.
TEST(SniiSpimiTermBufferTest, VocabInterningMaterializesEachStringOnce) {
    stb_testing::reset_vocab_string_materialization_count();
    SpimiTermBuffer buf(/*has_positions=*/false);
    const std::string term = MixedLongAscii();
    ASSERT_GT(term.size(), 15U);

    constexpr uint32_t kRepeats = 1000;
    for (uint32_t d = 0; d < kRepeats; ++d) {
        buf.add_token(std::string_view(term), d, 0);
    }
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 1U);
    EXPECT_EQ(buf.unique_terms(), 1U);
}

// N distinct >SSO terms, each fed twice, materialize exactly N strings: the count
// tracks DISTINCT terms (one owned_vocab_.emplace_back each), not total tokens -- the
// repeat of an already-seen term allocates nothing on the heterogeneous hit path.
TEST(SniiSpimiTermBufferTest, VocabMaterializesOncePerDistinctTerm) {
    stb_testing::reset_vocab_string_materialization_count();
    SpimiTermBuffer buf(/*has_positions=*/false);

    constexpr uint32_t kDistinct = 500;
    for (uint32_t i = 0; i < kDistinct; ++i) {
        const std::string term =
                "ordinary-prefix-sharing-distinct-term-number-" + std::to_string(i);
        buf.add_token(std::string_view(term), i, 0);
        buf.add_token(std::string_view(term), i + kDistinct, 0); // repeat: zero materialization
    }
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), static_cast<uint64_t>(kDistinct));
    EXPECT_EQ(buf.unique_terms(), static_cast<size_t>(kDistinct));
}

// The seam resets cleanly between measurements (guards the reset_/count_ contract the
// two perf tests above rely on for determinism in a shared process).
TEST(SniiSpimiTermBufferTest, MaterializationCounterResets) {
    stb_testing::reset_vocab_string_materialization_count();
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 0U);
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token(std::string_view("one"), 0, 0);
    buf.add_token(std::string_view("two"), 0, 0);
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 2U);
    stb_testing::reset_vocab_string_materialization_count();
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 0U);
}
