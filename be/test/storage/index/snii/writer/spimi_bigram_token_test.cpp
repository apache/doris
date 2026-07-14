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
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// G01 part C: SpimiTermBuffer::add_bigram_token interns the synthetic
// marker+varint(len(left))+left+right term by PIECEWISE hash/compare, composing
// the owned std::string exactly once per DISTINCT pair. These tests pin (a) byte
// equivalence with the compose-then-add_token path, (b) the single-composition
// guarantee via the vocab-materialization seam, (c) that both entry points land
// on the SAME interned term, (d) that ambiguous concatenations stay distinct
// (the varint length prefix disambiguates), and (e) the owned-vocab-mode
// contract.
using doris::Status;
using doris::snii::format::make_phrase_bigram_term;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;
namespace spimi_testing = doris::snii::writer::testing;

namespace {

void expect_same_postings(const std::vector<TermPostings>& a, const std::vector<TermPostings>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].term, b[i].term);
        EXPECT_EQ(a[i].docids, b[i].docids);
        EXPECT_EQ(a[i].freqs, b[i].freqs);
        EXPECT_EQ(a[i].positions_flat, b[i].positions_flat);
    }
}

} // namespace

// The zero-alloc entry point accumulates byte-identically to composing the
// synthetic term and feeding add_token(string_view).
TEST(SniiSpimiBigramToken, MatchesComposedStringPath) {
    const std::vector<std::pair<std::string, std::string>> pairs {
            {"failed", "order"}, {"order", "ordinal"}, {"failed", "order"}, {"repeat", "repeat"}};

    SpimiTermBuffer composed(/*has_positions=*/true);
    SpimiTermBuffer piecewise(/*has_positions=*/true);
    uint32_t docid = 0;
    uint32_t pos = 0;
    for (const auto& [left, right] : pairs) {
        composed.add_token(make_phrase_bigram_term(left, right), docid, pos);
        // Also mix in a real unigram so bigrams and unigrams share the intern set.
        composed.add_token(left, docid, pos + 1);
        piecewise.add_bigram_token(left, right, docid, pos);
        piecewise.add_token(left, docid, pos + 1);
        ++docid;
        pos += 2;
    }
    ASSERT_TRUE(composed.status().ok());
    ASSERT_TRUE(piecewise.status().ok());
    EXPECT_EQ(piecewise.unique_terms(), composed.unique_terms());
    EXPECT_EQ(piecewise.total_tokens(), composed.total_tokens());

    expect_same_postings(piecewise.finalize_sorted(), composed.finalize_sorted());
}

// The owned string is composed exactly once per DISTINCT pair: repeats are
// answered from the piecewise intern probe with zero materializations.
TEST(SniiSpimiBigramToken, RepeatPairMaterializesOnce) {
    spimi_testing::reset_vocab_string_materialization_count();
    SpimiTermBuffer buf(/*has_positions=*/true);
    for (uint32_t docid = 0; docid < 1000; ++docid) {
        buf.add_bigram_token("failed", "order", docid, 0);
    }
    EXPECT_EQ(spimi_testing::vocab_string_materialization_count(), 1U);

    buf.add_bigram_token("order", "ordinal", 1000, 0);
    EXPECT_EQ(spimi_testing::vocab_string_materialization_count(), 2U);
    ASSERT_TRUE(buf.status().ok());
    EXPECT_EQ(buf.unique_terms(), 2U);

    const std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 2U);
    // Lexicographic order: marker + varint(5) + "order"... sorts before
    // marker + varint(6) + "failed"... (the length byte compares first).
    EXPECT_EQ(terms[0].term, make_phrase_bigram_term("order", "ordinal"));
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1000}));
    EXPECT_EQ(terms[1].term, make_phrase_bigram_term("failed", "order"));
    EXPECT_EQ(terms[1].docids.size(), 1000U);
}

// Both entry points must land on the SAME interned term-id (one shared content
// hash/equality across composed-string and piecewise probes).
TEST(SniiSpimiBigramToken, MixedEntryPointsInternToOneTerm) {
    spimi_testing::reset_vocab_string_materialization_count();
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token(make_phrase_bigram_term("failed", "order"), 1, 0);
    buf.add_bigram_token("failed", "order", 2, 0);
    buf.add_token(make_phrase_bigram_term("failed", "order"), 3, 0);
    ASSERT_TRUE(buf.status().ok());
    EXPECT_EQ(buf.unique_terms(), 1U);
    EXPECT_EQ(spimi_testing::vocab_string_materialization_count(), 1U);

    const std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1, 2, 3}));
}

// ("ab","c") and ("a","bc") concatenate identically; the varint(len(left))
// fragment must keep them DISTINCT terms in both hash and equality.
TEST(SniiSpimiBigramToken, AmbiguousConcatenationStaysDistinct) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_bigram_token("ab", "c", 1, 0);
    buf.add_bigram_token("a", "bc", 2, 0);
    buf.add_bigram_token("ab", "c", 3, 1);
    ASSERT_TRUE(buf.status().ok());
    EXPECT_EQ(buf.unique_terms(), 2U);

    const std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 2U);
    // marker + varint(1) + "abc" sorts before marker + varint(2) + "abc".
    EXPECT_EQ(terms[0].term, make_phrase_bigram_term("a", "bc"));
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {2}));
    EXPECT_EQ(terms[1].term, make_phrase_bigram_term("ab", "c"));
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {1, 3}));
}

// Same contract as add_token(string_view): interning is owned-vocab-mode only; a
// borrowed-vocab buffer latches InvalidArgument and ignores the token.
TEST(SniiSpimiBigramToken, RejectedInBorrowedVocabMode) {
    const std::vector<std::string> vocab {"aa", "bb"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/true);
    buf.add_bigram_token("aa", "bb", 0, 0);
    EXPECT_FALSE(buf.status().ok());
    EXPECT_EQ(buf.unique_terms(), 0U);
    EXPECT_EQ(buf.total_tokens(), 0U);
}
