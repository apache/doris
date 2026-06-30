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

// T08 -- wildcard matcher scratch reuse.
//
// Proves the request-scoped internal::WildcardMatcher (a) matches bit-for-bit
// identically to the former per-call DP in wildcard_query.cpp, and (b) reuses its
// two DP scratch rows across every visited term so a whole-dictionary scan
// performs O(1) heap allocations (<= 2) instead of O(2N). A header-only
// CountingAllocator gives the deterministic allocation counts; a byte-for-byte
// copy of the original DP serves as the equivalence oracle.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/internal/wildcard_matcher.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/query/wildcard_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::query {
using doris::Status; // RETURN_IF_ERROR / Status::OK() expand to a bare Status.
namespace {

// Shared reader/writer fixtures live in snii_query_test_util.h; pull the ones
// this suite needs into scope unqualified.
using snii_test::assert_ok;
using snii_test::build_reader;
using snii_test::MemoryFile;

// Minimal counting allocator: every allocate() bumps a per-type static counter so
// tests can assert exact heap-allocation counts without overriding global new.
// Stateless (all instances compare equal), so std::vector::swap stays a pointer
// swap that performs no allocation -- exactly what the matcher relies on.
template <class T>
struct CountingAllocator {
    using value_type = T;

    CountingAllocator() noexcept = default;
    // Converting (rebind) constructor: intentionally non-explicit, as required by
    // the Allocator named requirement / std::allocator_traits.
    template <class U>
    CountingAllocator(const CountingAllocator<U>& /*other*/) noexcept {} // NOLINT(*-explicit-*)

    T* allocate(std::size_t n) {
        ++s_total_allocs;
        ++s_live;
        return static_cast<T*>(::operator new(n * sizeof(T)));
    }
    void deallocate(T* p, std::size_t /*n*/) noexcept {
        --s_live;
        ::operator delete(p);
    }

    template <class U>
    bool operator==(const CountingAllocator<U>& /*other*/) const noexcept {
        return true;
    }
    template <class U>
    bool operator!=(const CountingAllocator<U>& /*other*/) const noexcept {
        return false;
    }

    static void reset() {
        s_total_allocs = 0;
        s_live = 0;
    }
    static std::size_t total_allocs() { return s_total_allocs; }
    static std::size_t live() { return s_live; }

    static inline std::size_t s_total_allocs = 0;
    static inline std::size_t s_live = 0;
};

// Byte-for-byte copy of the former wildcard_query.cpp DP (templated only so the
// baseline-characterization test can count its per-call allocations). This is the
// equivalence oracle the optimized matcher must reproduce exactly.
template <class Alloc = std::allocator<uint8_t>>
bool wildcard_match_dp_reference(std::string_view pattern, std::string_view text) {
    std::vector<uint8_t, Alloc> prev(text.size() + 1, 0);
    std::vector<uint8_t, Alloc> curr(text.size() + 1, 0);
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

// `count` terms whose first (warmup) entry is the longest (`max_len`); every later
// term is <= max_len, so a matcher that reuses scratch reallocates only on the
// first call. The lengths still vary across terms (cycling 0..max_len).
std::vector<std::string> make_varied_length_terms(size_t count, size_t max_len) {
    std::vector<std::string> terms;
    terms.reserve(count);
    terms.push_back(std::string(max_len, 'a'));
    for (size_t i = 1; i < count; ++i) {
        const size_t len = i % (max_len + 1);
        terms.push_back(std::string(len, static_cast<char>('a' + (i % 26))));
    }
    return terms;
}

// W-EQ-DP: the optimized matcher reproduces the reference DP bit-for-bit over an
// exhaustive small-alphabet battery (covers "", leading/trailing '*'/'?',
// consecutive "**", '?' interplay) plus realistic dictionary patterns/terms. One
// matcher is reused across all terms of a pattern, so this also proves scratch
// reuse never corrupts a result.
TEST(SniiWildcardQueryTest, MatcherEquivalentToReferenceDp) {
    const std::vector<std::string> exhaustive_patterns = all_strings_up_to("ab*?", 4);
    const std::vector<std::string> exhaustive_texts = all_strings_up_to("ab", 4);
    for (const std::string& pattern : exhaustive_patterns) {
        internal::WildcardMatcher<> matcher(pattern);
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
        internal::WildcardMatcher<> matcher(pattern);
        for (const std::string& text : terms) {
            EXPECT_EQ(matcher(text), wildcard_match_dp_reference(pattern, text))
                    << "pattern=\"" << pattern << "\" text=\"" << text << "\"";
        }
    }
}

// W-EMPTY-PAT: an empty pattern matches only the empty string.
TEST(SniiWildcardQueryTest, EmptyPatternMatchesOnlyEmptyText) {
    internal::WildcardMatcher<> matcher("");
    EXPECT_TRUE(matcher(""));
    EXPECT_FALSE(matcher("a"));
}

// W-STAR-ONLY: "*" matches the empty string and any non-empty string.
TEST(SniiWildcardQueryTest, StarMatchesEverything) {
    internal::WildcardMatcher<> matcher("*");
    EXPECT_TRUE(matcher(""));
    EXPECT_TRUE(matcher("x"));
    EXPECT_TRUE(matcher("xyz"));
}

// W-QMARK: "?" matches exactly one byte.
TEST(SniiWildcardQueryTest, QuestionMarkMatchesExactlyOneByte) {
    internal::WildcardMatcher<> matcher("?");
    EXPECT_FALSE(matcher(""));
    EXPECT_TRUE(matcher("a"));
    EXPECT_FALSE(matcher("ab"));
}

// W-CONSEC-STAR: consecutive '*' degrade gracefully.
TEST(SniiWildcardQueryTest, ConsecutiveStars) {
    internal::WildcardMatcher<> matcher("**a**");
    EXPECT_TRUE(matcher("a"));
    EXPECT_TRUE(matcher("xax"));
    EXPECT_FALSE(matcher("b"));
}

// W-ANCHOR: a literal pattern is anchored at both ends (full match only).
TEST(SniiWildcardQueryTest, LiteralIsFullyAnchored) {
    internal::WildcardMatcher<> matcher("ab");
    EXPECT_TRUE(matcher("ab"));
    EXPECT_FALSE(matcher("abc"));
    EXPECT_FALSE(matcher("xab"));
}

// Perf (deterministic): the matcher allocates its two scratch rows once and reuses
// them across every term -- total heap allocations stay <= 2 and are independent
// of the term count N.
TEST(SniiWildcardQueryTest, MatcherReusesScratchAcrossTerms) {
    using Alloc = CountingAllocator<uint8_t>;

    auto allocs_for_n = [](size_t n) {
        Alloc::reset();
        internal::WildcardMatcher<Alloc> matcher("*a*");
        for (const std::string& term : make_varied_length_terms(n, /*max_len=*/64)) {
            matcher(term);
        }
        return Alloc::total_allocs();
    };

    EXPECT_LE(allocs_for_n(1000), 2U);
    // N-independent: exactly the two scratch rows, whether N=10 or N=1000.
    EXPECT_EQ(allocs_for_n(10), 2U);
    EXPECT_EQ(allocs_for_n(1000), 2U);
    EXPECT_EQ(Alloc::live(), 0U); // matcher destroyed each lambda call: no leak.
}

// Perf (deterministic): once warmed up with the longest term, the scratch capacity
// never changes across subsequent shorter/equal-length terms (no realloc).
TEST(SniiWildcardQueryTest, MatcherScratchCapacityStable) {
    internal::WildcardMatcher<> matcher("*x*");
    matcher(std::string(64, 'a')); // warmup with the longest term
    const size_t cap_after_warmup = matcher.scratch_capacity();
    EXPECT_GE(cap_after_warmup, 65U);
    for (size_t len : {size_t {0}, size_t {1}, size_t {7}, size_t {63}, size_t {64}}) {
        matcher(std::string(len, 'b'));
        EXPECT_EQ(matcher.scratch_capacity(), cap_after_warmup);
    }
}

// Perf baseline (deterministic, contrast): the former per-call DP constructs two
// std::vectors every call, so N terms cost exactly 2N allocations -- the cost the
// reused matcher above eliminates.
TEST(SniiWildcardQueryTest, PerCallDpReferenceAllocatesTwicePerTerm) {
    using Alloc = CountingAllocator<uint8_t>;
    Alloc::reset();
    const std::vector<std::string> terms = make_varied_length_terms(/*count=*/100, /*max_len=*/64);
    for (const std::string& term : terms) {
        wildcard_match_dp_reference<Alloc>("*a*", term);
    }
    EXPECT_EQ(Alloc::total_allocs(), 2U * terms.size());
    EXPECT_EQ(Alloc::live(), 0U);
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

// W-HIDDEN-BIGRAM: hidden phrase-bigram terms must never leak through a leading
// wildcard scan (the bigram filter in term_expansion hides them).
TEST(SniiWildcardQueryTest, DoesNotExposeHiddenBigramTerms) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> docids;
    assert_ok(wildcard_query(index_reader, "*failed*order*", &docids));
    EXPECT_TRUE(docids.empty());
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

    DocIdSink* const null_sink = nullptr;
    EXPECT_TRUE(
            wildcard_query(index_reader, "a*", null_sink).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris::snii::query
