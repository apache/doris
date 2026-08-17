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

#include "storage/index/inverted/inverted_index_term_bloom_filter.h"

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <CLucene/store/RAMDirectory.h>
#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_term_bf_query.h"

namespace doris::segment_v2 {

// CLucene's Directory::openInput is the 3-arg (name, ret, error) form; wrap it for tests.
static lucene::store::IndexInput* open_ram_input(lucene::store::RAMDirectory& dir,
                                                 const char* name) {
    lucene::store::IndexInput* in = nullptr;
    CLuceneError err;
    dir.openInput(name, in, err);
    return in;
}

// Unit tests for the token-exists Bloom Filter ("tbf") sub-file primitive.
//
// These exercise the low-level contract that the FullTextIndexReader fast path relies on:
//   - serialize/load round-trip through a CLucene directory (the real on-disk path)
//   - ABSENT is authoritative (no false negatives) while present tokens never report ABSENT
//   - the empty keyword token is on the data path, not the null flag (A5 primitive)
//   - analyzer_sig stability and mismatch detection (A3 primitive)
//   - the multi-term OR rule + phrase position-group OR rule the reader applies (A2 / A1
//     primitives), validated directly against probe() so the grouping invariant is pinned
//   - corrupt / truncated sub-files fail load() so the reader treats them as "unknown"
class InvertedIndexTermBloomFilterTest : public testing::Test {
protected:
    // Round-trip a freshly-built BF through a RAMDirectory and return the reloaded copy.
    static std::unique_ptr<InvertedIndexTermBloomFilter> round_trip(
            const InvertedIndexTermBloomFilter& bf) {
        lucene::store::RAMDirectory dir;
        {
            std::unique_ptr<lucene::store::IndexOutput> out(dir.createOutput("tbf"));
            bf.serialize(out.get());
            out->close();
        }
        std::unique_ptr<lucene::store::IndexInput> in(open_ram_input(dir, "tbf"));
        auto res = InvertedIndexTermBloomFilter::load(in.get());
        in->close();
        EXPECT_TRUE(res.has_value()) << res.error();
        return res.has_value() ? std::move(res.value()) : nullptr;
    }

    static InvertedIndexAnalyzerConfig english_cfg() {
        InvertedIndexAnalyzerConfig cfg;
        cfg.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
        cfg.lower_case = "true";
        return cfg;
    }
};

// serialize -> load round-trip preserves header fields and probe answers.
TEST_F(InvertedIndexTermBloomFilterTest, SerializeLoadRoundTrip) {
    auto sig = compute_analyzer_sig(english_cfg());
    auto built = InvertedIndexTermBloomFilter::create_for_write(/*distinct_terms=*/3, sig);
    ASSERT_TRUE(built.has_value()) << built.error();
    auto bf = std::move(built.value());

    const std::vector<std::string> present = {"apple", "banana", "cherry"};
    for (const auto& t : present) {
        bf->add_token(t.data(), t.size());
    }

    auto loaded = round_trip(*bf);
    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(loaded->analyzer_sig(), sig);
    EXPECT_EQ(loaded->distinct_terms(), 3U);

    // Present tokens must never report ABSENT (no false negative).
    for (const auto& t : present) {
        EXPECT_EQ(loaded->probe(t), InvertedIndexTermBloomFilter::Probe::MAYBE) << t;
    }
}

// ABSENT is authoritative: a token that was never added is reported ABSENT (modulo the
// tiny configured fpp; the chosen out-of-vocabulary tokens are checked to actually answer
// ABSENT here, which validates the absent fast path is reachable at all).
TEST_F(InvertedIndexTermBloomFilterTest, AbsentTokenProbesAbsent) {
    auto built = InvertedIndexTermBloomFilter::create_for_write(
            /*distinct_terms=*/4, compute_analyzer_sig(english_cfg()));
    ASSERT_TRUE(built.has_value());
    auto bf = std::move(built.value());
    for (const auto& t : {"alpha", "beta", "gamma", "delta"}) {
        bf->add_token(t, std::strlen(t));
    }
    auto loaded = round_trip(*bf);
    ASSERT_NE(loaded, nullptr);

    // None of these were added; with fpp=0.01 over a 4-element set they probe ABSENT.
    int absent = 0;
    for (const auto& t : {"epsilon", "zeta", "omega", "qqqqq"}) {
        if (loaded->probe(t) == InvertedIndexTermBloomFilter::Probe::ABSENT) {
            ++absent;
        }
    }
    EXPECT_GE(absent, 1) << "absent fast path must be reachable for out-of-vocabulary tokens";
}

// A5 primitive: the empty keyword token is a legal token on the data path, distinct from a
// genuinely-absent token. After adding "", probe("") must be MAYBE; a non-empty unrelated
// token may still be ABSENT.
TEST_F(InvertedIndexTermBloomFilterTest, EmptyTokenIsOnDataPath) {
    auto built = InvertedIndexTermBloomFilter::create_for_write(
            /*distinct_terms=*/2, compute_analyzer_sig(english_cfg()));
    ASSERT_TRUE(built.has_value());
    auto bf = std::move(built.value());
    bf->add_token("", 0);        // empty keyword token
    bf->add_token("present", 7); // a normal token

    auto loaded = round_trip(*bf);
    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(loaded->probe(""), InvertedIndexTermBloomFilter::Probe::MAYBE)
            << "empty keyword token must be matchable, not treated as null";
    EXPECT_EQ(loaded->probe("present"), InvertedIndexTermBloomFilter::Probe::MAYBE);
}

// An empty BF where "" was never added: probe("") is ABSENT, proving the empty token is not
// silently special-cased to always-present.
TEST_F(InvertedIndexTermBloomFilterTest, EmptyTokenAbsentWhenNotAdded) {
    auto built = InvertedIndexTermBloomFilter::create_for_write(
            /*distinct_terms=*/1, compute_analyzer_sig(english_cfg()));
    ASSERT_TRUE(built.has_value());
    auto bf = std::move(built.value());
    bf->add_token("nonempty", 8);

    auto loaded = round_trip(*bf);
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->probe(""), InvertedIndexTermBloomFilter::Probe::ABSENT);
}

// A3 primitive: equal analyzer configs produce equal signatures; any tokenization-affecting
// difference changes the signature so the reader can reject a foreign BF.
TEST_F(InvertedIndexTermBloomFilterTest, AnalyzerSignatureStableAndDiscriminating) {
    auto a = english_cfg();
    auto b = english_cfg();
    EXPECT_EQ(compute_analyzer_sig(a), compute_analyzer_sig(b));

    auto lower_off = english_cfg();
    lower_off.lower_case = "false";
    EXPECT_NE(compute_analyzer_sig(a), compute_analyzer_sig(lower_off));

    auto unicode = english_cfg();
    unicode.parser_type = InvertedIndexParserType::PARSER_UNICODE;
    EXPECT_NE(compute_analyzer_sig(a), compute_analyzer_sig(unicode));

    auto with_stop = english_cfg();
    with_stop.stop_words = "none";
    EXPECT_NE(compute_analyzer_sig(a), compute_analyzer_sig(with_stop));

    auto with_charfilter = english_cfg();
    with_charfilter.char_filter_map["char_filter_pattern"] = ".";
    EXPECT_NE(compute_analyzer_sig(a), compute_analyzer_sig(with_charfilter));
}

// A2 primitive: a multi-term slot (synonyms / CJK overlap at one position) is an OR. The
// reader treats it as ABSENT only when *every* sub-term is ABSENT. This mirrors that rule
// directly over probe() so the grouping invariant is pinned even though the reader helper
// lives in an anonymous namespace.
// A2: a multi-term slot (CJK overlap / synonyms at one position) is an OR of its sub-terms --
// ABSENT only when every sub-term is ABSENT. Drives the *shipped* bf_query_proven_empty (the
// reader fast path calls the same function), so a regression in the grouping fails this test.
TEST_F(InvertedIndexTermBloomFilterTest, MultiTermSlotIsOrOfSubTerms) {
    auto built = InvertedIndexTermBloomFilter::create_for_write(
            /*distinct_terms=*/2, compute_analyzer_sig(english_cfg()));
    ASSERT_TRUE(built.has_value());
    auto bf = std::move(built.value());
    bf->add_token("present_a", 9);
    bf->add_token("present_b", 9);
    auto loaded = round_trip(*bf);
    ASSERT_NE(loaded, nullptr);

    // MATCH_ALL with a single multi-term slot: brace-init the vector's TermInfo elements (avoids
    // naming TermInfo, which would clash with lucene::index::TermInfo under <CLucene.h>).
    auto match_all = [&](std::vector<std::string> alts) {
        InvertedIndexQueryInfo qi;
        qi.term_infos.push_back({std::move(alts), /*position=*/0});
        return bf_query_proven_empty(InvertedIndexQueryType::MATCH_ALL_QUERY, qi, *loaded);
    };

    // One present alternative keeps the multi-term slot alive (must NOT short-circuit).
    EXPECT_FALSE(match_all({"present_a", "absent_zzz"}));
    EXPECT_FALSE(match_all({"absent_zzz", "present_b"}));
    // Every alternative absent -> slot provably dead -> proven empty.
    EXPECT_TRUE(match_all({"absent_xxx", "absent_yyy"}));
}

// A1: a MATCH_PHRASE groups alternatives by position; a slot is dead only when all alternatives
// at that position are ABSENT, and the phrase is empty only when some slot is dead. Drives the
// *shipped* bf_query_proven_empty over same-position alternatives -- the exact silent-drop hazard.
TEST_F(InvertedIndexTermBloomFilterTest, PhrasePositionGroupOrRule) {
    auto built = InvertedIndexTermBloomFilter::create_for_write(
            /*distinct_terms=*/3, compute_analyzer_sig(english_cfg()));
    ASSERT_TRUE(built.has_value());
    auto bf = std::move(built.value());
    bf->add_token("quick", 5);
    bf->add_token("brown", 5);
    bf->add_token("fox", 3);
    auto loaded = round_trip(*bf);
    ASSERT_NE(loaded, nullptr);

    auto phrase_empty = [&](const std::vector<std::pair<int32_t, std::string>>& terms) {
        InvertedIndexQueryInfo qi;
        for (const auto& [pos, tok] : terms) {
            qi.term_infos.push_back({tok, pos});
        }
        return bf_query_proven_empty(InvertedIndexQueryType::MATCH_PHRASE_QUERY, qi, *loaded);
    };

    // Position 0 is an OR slot {brown(present), absent_alt}: one present alternative keeps it
    // alive, so the phrase must NOT be reported empty even though one alternative is absent.
    EXPECT_FALSE(phrase_empty({{0, "brown"}, {0, "absent_alt"}, {1, "fox"}}));
    // A position whose every alternative is absent kills the phrase.
    EXPECT_TRUE(phrase_empty({{0, "quick"}, {1, "absent_a"}, {1, "absent_b"}}));
}

// load() rejects a buffer that is too small to hold the header (reader treats it as unknown).
TEST_F(InvertedIndexTermBloomFilterTest, LoadRejectsTruncatedFile) {
    lucene::store::RAMDirectory dir;
    {
        std::unique_ptr<lucene::store::IndexOutput> out(dir.createOutput("tbf"));
        const char junk[8] = {'T', 'B', 'F', '1', 0, 0, 0, 0};
        out->writeBytes(reinterpret_cast<const uint8_t*>(junk), sizeof(junk));
        out->close();
    }
    std::unique_ptr<lucene::store::IndexInput> in(open_ram_input(dir, "tbf"));
    auto res = InvertedIndexTermBloomFilter::load(in.get());
    in->close();
    EXPECT_FALSE(res.has_value());
}

// load() rejects a wrong magic (a stale / foreign sub-file -> unknown -> normal query).
TEST_F(InvertedIndexTermBloomFilterTest, LoadRejectsBadMagic) {
    auto built = InvertedIndexTermBloomFilter::create_for_write(
            /*distinct_terms=*/1, compute_analyzer_sig(english_cfg()));
    ASSERT_TRUE(built.has_value());
    auto bf = std::move(built.value());
    bf->add_token("apple", 5);

    lucene::store::RAMDirectory dir;
    {
        std::unique_ptr<lucene::store::IndexOutput> out(dir.createOutput("tbf"));
        bf->serialize(out.get());
        out->close();
    }
    // Corrupt the magic in place by rewriting the file with a flipped first byte.
    std::vector<uint8_t> bytes;
    {
        std::unique_ptr<lucene::store::IndexInput> in(open_ram_input(dir, "tbf"));
        bytes.resize(in->length());
        in->readBytes(bytes.data(), static_cast<int32_t>(bytes.size()));
        in->close();
    }
    bytes[0] = 'X';
    dir.deleteFile("tbf");
    {
        std::unique_ptr<lucene::store::IndexOutput> out(dir.createOutput("tbf"));
        out->writeBytes(bytes.data(), static_cast<int32_t>(bytes.size()));
        out->close();
    }
    std::unique_ptr<lucene::store::IndexInput> in(open_ram_input(dir, "tbf"));
    auto res = InvertedIndexTermBloomFilter::load(in.get());
    in->close();
    EXPECT_FALSE(res.has_value());
}

} // namespace doris::segment_v2
