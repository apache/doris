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

// T08 -- wildcard matcher.
//
// Proves index_query::WildcardMatcher matches like the original DP, a byte-for-byte
// copy of which serves as the ASCII equivalence oracle, while matching UTF-8 code
// points.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/query/docid_sink.h"
#include "storage/index/query/term_pattern.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_query_oracle.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::query {
using doris::Status; // RETURN_IF_ERROR / Status::OK() expand to a bare Status.
namespace {

// Shared reader/writer fixtures live in snii_query_test_util.h; pull the ones
// this suite needs into scope unqualified.
using snii_test::assert_ok;
using snii_test::build_reader;
using snii_test::MemoryFile;

// Byte-for-byte copy of the former wildcard_query.cpp DP, the equivalence oracle
// the matcher must reproduce exactly.
bool wildcard_match_dp_reference(std::string_view pattern, std::string_view text) {
    std::vector<uint8_t> prev(text.size() + 1, 0);
    std::vector<uint8_t> curr(text.size() + 1, 0);
    prev[0] = 1;

    for (char p : pattern) {
        std::fill(curr.begin(), curr.end(), 0);
        if (p == '*') {
            curr[0] = prev[0];
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i] || curr[i - 1];
            }
        } else {
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i - 1] && (p == '?' || p == text[i - 1]);
            }
        }
        prev.swap(curr);
    }
    return prev[text.size()] != 0;
}

// All strings of length 0..max_len over `alphabet`, for exhaustive equivalence.
std::vector<std::string> all_strings_up_to(std::string_view alphabet, size_t max_len) {
    std::vector<std::string> out;
    out.emplace_back();
    size_t level_begin = 0;
    for (size_t len = 1; len <= max_len; ++len) {
        const size_t level_end = out.size();
        for (size_t i = level_begin; i < level_end; ++i) {
            for (char c : alphabet) {
                out.push_back(out[i] + c);
            }
        }
        level_begin = level_end;
    }
    return out;
}

// W-EQ-DP: the optimized matcher reproduces the reference DP for ASCII over an
// exhaustive small-alphabet battery (covers "", leading/trailing '*'/'?',
// consecutive "**", '?' interplay) plus realistic dictionary patterns/terms. One
// matcher serves every term of a pattern.
TEST(SniiWildcardQueryTest, MatcherEquivalentToReferenceDp) {
    const std::vector<std::string> exhaustive_patterns = all_strings_up_to("ab*?", 4);
    const std::vector<std::string> exhaustive_texts = all_strings_up_to("ab", 6);
    for (const std::string& pattern : exhaustive_patterns) {
        index_query::WildcardMatcher matcher(pattern);
        for (const std::string& text : exhaustive_texts) {
            EXPECT_EQ(matcher(text), wildcard_match_dp_reference(pattern, text))
                    << "pattern=\"" << pattern << "\" text=\"" << text << "\"";
        }
    }

    const std::vector<std::string> patterns = {
            "",      "*",     "?",   "ord*", "?rder",  "*failed*order*", "ordinal",
            "order", "**a**", "a?b", "*a*",  "?ailed", "sparse_*",       "*_left"};
    const std::vector<std::string> terms = {"",
                                            "order",
                                            "ordinal",
                                            "failed",
                                            "needle",
                                            "driver",
                                            "almost",
                                            "123",
                                            "repeat",
                                            "sparse_left",
                                            "sparse_right",
                                            "trace",
                                            "ordering",
                                            std::string(40, 'a')};
    for (const std::string& pattern : patterns) {
        index_query::WildcardMatcher matcher(pattern);
        for (const std::string& text : terms) {
            EXPECT_EQ(matcher(text), wildcard_match_dp_reference(pattern, text))
                    << "pattern=\"" << pattern << "\" text=\"" << text << "\"";
        }
    }
}

// W-EMPTY-PAT: an empty pattern matches only the empty string.
TEST(SniiWildcardQueryTest, EmptyPatternMatchesOnlyEmptyText) {
    index_query::WildcardMatcher matcher("");
    EXPECT_TRUE(matcher(""));
    EXPECT_FALSE(matcher("a"));
}

// W-STAR-ONLY: "*" matches the empty string and any non-empty string.
TEST(SniiWildcardQueryTest, StarMatchesEverything) {
    index_query::WildcardMatcher matcher("*");
    EXPECT_TRUE(matcher(""));
    EXPECT_TRUE(matcher("x"));
    EXPECT_TRUE(matcher("xyz"));
}

// W-QMARK: "?" matches exactly one UTF-8 code point.
TEST(SniiWildcardQueryTest, QuestionMarkMatchesExactlyOneUtf8CodePoint) {
    index_query::WildcardMatcher matcher("?");
    EXPECT_FALSE(matcher(""));
    EXPECT_TRUE(matcher("a"));
    EXPECT_TRUE(matcher("猫"));
    EXPECT_TRUE(matcher("🔥"));
    EXPECT_FALSE(matcher("ab"));

    index_query::WildcardMatcher surrounded("a?b");
    EXPECT_TRUE(surrounded("a猫b"));
    EXPECT_TRUE(surrounded("a🔥b"));
    EXPECT_FALSE(surrounded("a猫猫b"));

    index_query::WildcardMatcher three("a???b");
    EXPECT_FALSE(three("a猫b"));
    EXPECT_TRUE(three("a猫🔥éb"));

    index_query::WildcardMatcher star_then_two("a*??b");
    EXPECT_FALSE(star_then_two("a猫b"));
    EXPECT_TRUE(star_then_two("a猫🔥b"));
}

// W-UTF8-LITERAL: non-ASCII literals are compared as complete code points.
TEST(SniiWildcardQueryTest, Utf8LiteralsMatchCompleteCodePoints) {
    index_query::WildcardMatcher matcher("猫?火");
    EXPECT_TRUE(matcher("猫🔥火"));
    EXPECT_FALSE(matcher("猫🔥🔥火"));
    EXPECT_FALSE(matcher("狗🔥火"));
}

// W-ASCII: a pattern of ASCII literals and '*' reads text as bytes, which gives the
// code-point answer for multi-byte text.
TEST(SniiWildcardQueryTest, AsciiPatternMatchesMultiByteTextLikeCodePoints) {
    index_query::WildcardMatcher matcher("a*b");
    EXPECT_TRUE(matcher("a猫b"));
    EXPECT_TRUE(matcher("a🔥猫b"));
    EXPECT_FALSE(matcher("a猫"));
    index_query::WildcardMatcher suffix("*b");
    EXPECT_TRUE(suffix("猫b"));
    EXPECT_FALSE(suffix("b猫"));
}

// W-INVALID-UTF8: patterns remain strict UTF-8, while malformed raw keyword
// terms retain the byte-wise semantics used before code-point matching.
TEST(SniiWildcardQueryTest, MalformedTermsRetainByteCompatibleMatching) {
    const std::string invalid_lead("\xff", 1);
    const std::string truncated("\xe7\x8c", 2);
    const std::string invalid_continuation("\xe7x\xab", 3);

    index_query::WildcardMatcher any("*");
    EXPECT_TRUE(any(invalid_lead));
    EXPECT_TRUE(any(truncated));
    EXPECT_TRUE(any(invalid_continuation));

    index_query::WildcardMatcher two_bytes("??");
    EXPECT_FALSE(two_bytes(invalid_lead));
    EXPECT_TRUE(two_bytes(truncated));
    EXPECT_FALSE(two_bytes(invalid_continuation));

    index_query::WildcardMatcher invalid_pattern(invalid_lead);
    EXPECT_FALSE(invalid_pattern(invalid_lead));
    EXPECT_FALSE(invalid_pattern("猫"));
}

TEST(SniiWildcardQueryTest, InvalidUtf8PatternReturnsInvalidArgument) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    const std::string invalid_pattern("\xff*", 2);
    std::vector<uint32_t> docids;
    EXPECT_TRUE(wildcard_query(index_reader, invalid_pattern, &docids)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// W-CONSEC-STAR: consecutive '*' degrade gracefully.
TEST(SniiWildcardQueryTest, ConsecutiveStars) {
    index_query::WildcardMatcher matcher("**a**");
    EXPECT_TRUE(matcher("a"));
    EXPECT_TRUE(matcher("xax"));
    EXPECT_FALSE(matcher("b"));
}

// W-ANCHOR: a literal pattern is anchored at both ends (full match only).
TEST(SniiWildcardQueryTest, LiteralIsFullyAnchored) {
    index_query::WildcardMatcher matcher("ab");
    EXPECT_TRUE(matcher("ab"));
    EXPECT_FALSE(matcher("abc"));
    EXPECT_FALSE(matcher("xab"));
}

// W-RESULT: end-to-end, "ord*" returns the sorted deduplicated union of the
// "order" and "ordinal" docid sets (independently computed expected set).
TEST(SniiWildcardQueryTest, WildcardResultIsTermUnion) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> order_docids;
    std::vector<uint32_t> ordinal_docids;
    assert_ok(term_query(index_reader, "order", &order_docids));
    assert_ok(term_query(index_reader, "ordinal", &ordinal_docids));
    std::vector<uint32_t> expected;
    std::set_union(order_docids.begin(), order_docids.end(), ordinal_docids.begin(),
                   ordinal_docids.end(), std::back_inserter(expected));

    std::vector<uint32_t> docids;
    assert_ok(wildcard_query(index_reader, "ord*", &docids));
    EXPECT_EQ(docids, expected);
}

// W-QMARK-FULL: a leading '?' forces a full-dictionary scan; "?rder" matches only
// the 5-byte "order" term (not 7-byte "ordinal").
TEST(SniiWildcardQueryTest, LeadingQuestionMarkResult) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> order_docids;
    assert_ok(term_query(index_reader, "order", &order_docids));

    std::vector<uint32_t> docids;
    assert_ok(wildcard_query(index_reader, "?rder", &docids));
    EXPECT_EQ(docids, order_docids);
}

// W-MAXEXP: max_expansions caps the number of expanded terms; terms enumerate in
// sorted order, so "*" with max_expansions=1 yields only the first term "123".
TEST(SniiWildcardQueryTest, MaxExpansionsCapsExpansion) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> first_term_docids;
    assert_ok(term_query(index_reader, "123", &first_term_docids));

    std::vector<uint32_t> docids;
    assert_ok(wildcard_query(index_reader, "*", &docids, /*max_expansions=*/1));
    EXPECT_EQ(docids, first_term_docids);
}

// W-NULL-OUT / W-NULL-SINK: null output and null sink return InvalidArgument
// (no crash, no throw).
TEST(SniiWildcardQueryTest, NullArgumentsReturnInvalidArgument) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t>* const null_docids = nullptr;
    EXPECT_TRUE(wildcard_query(index_reader, "a*", null_docids)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());

    ::doris::index_query::DocIdSink* const null_sink = nullptr;
    EXPECT_TRUE(
            wildcard_query(index_reader, "a*", null_sink).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris::snii::query
