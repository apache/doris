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

// T25 unit tests:
//   A (F28) -- emit_adjacent_phrase_bigrams: the writer's phrase-bigram pair
//     emission extracted as a dependency-free template. Asserts the refactored
//     set equals the pre-refactor "sort (position, term) then window" baseline,
//     the is_sorted guard short-circuits on analyzer-ordered input (did_sort ==
//     false) yet still corrects a shuffled input, and the reused positioned-term
//     vector keeps its capacity across batches (no per-row realloc).
//   B (F35) -- heap_bytes() accessors on the resident format readers
//     (SampledTermIndexReader / DictBlockDirectoryReader / DictBlockReader) that
//     LogicalIndexReader::memory_usage() sums so the searcher-cache charge stops
//     under-counting. Exact hand-computed equality for SSO terms; the string-heap
//     accumulation is exercised with an over-15-byte term.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/snii_phrase_bigram_build.h"

namespace {

using doris::segment_v2::emit_adjacent_phrase_bigrams;
using doris::segment_v2::PhrasePositionedTerm;
using doris::snii::ByteSink;
using doris::snii::Slice;
using namespace doris::snii::format; // NOLINT(google-build-using-namespace)

void assert_ok(const doris::Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// ============================ sub-task A ============================

// One emitted phrase-bigram pair, owning its term bytes so it survives past the
// backing string_view storage. Ordered/compared as a multiset element.
struct BigramTriple {
    std::string left;
    std::string right;
    uint32_t position = 0;

    bool operator==(const BigramTriple&) const = default;
    bool operator<(const BigramTriple& o) const {
        return std::tie(position, left, right) < std::tie(o.position, o.left, o.right);
    }
};

// Builds position-tagged views over `storage` (which must outlive the returned
// vector -- callers keep it as a stable local).
std::vector<PhrasePositionedTerm> make_positioned(const std::vector<std::string>& storage,
                                                  const std::vector<uint32_t>& positions) {
    EXPECT_EQ(storage.size(), positions.size());
    std::vector<PhrasePositionedTerm> out;
    out.reserve(storage.size());
    for (size_t i = 0; i < storage.size(); ++i) {
        out.push_back({std::string_view(storage[i]), positions[i]});
    }
    return out;
}

// The pre-refactor implementation: sort by (position, term) -- primary AND the
// now-dropped secondary key -- then run the identical position-group window. The
// result is returned as a sorted multiset so a comparison proves dropping the
// secondary key never changes the emitted set.
std::vector<BigramTriple> reference_old_bigrams(std::vector<PhrasePositionedTerm> terms) {
    std::ranges::sort(terms, [](const auto& a, const auto& b) {
        if (a.position != b.position) {
            return a.position < b.position;
        }
        return a.term < b.term;
    });
    std::vector<BigramTriple> out;
    size_t left_begin = 0;
    while (left_begin < terms.size()) {
        size_t left_end = left_begin + 1;
        while (left_end < terms.size() && terms[left_end].position == terms[left_begin].position) {
            ++left_end;
        }
        size_t right_begin = left_end;
        while (right_begin < terms.size() &&
               terms[right_begin].position <= terms[left_begin].position) {
            ++right_begin;
        }
        if (right_begin == terms.size() ||
            terms[right_begin].position != terms[left_begin].position + 1) {
            left_begin = left_end;
            continue;
        }
        size_t right_end = right_begin + 1;
        while (right_end < terms.size() &&
               terms[right_end].position == terms[right_begin].position) {
            ++right_end;
        }
        for (size_t l = left_begin; l < left_end; ++l) {
            for (size_t r = right_begin; r < right_end; ++r) {
                out.push_back({std::string(terms[l].term), std::string(terms[r].term),
                               terms[l].position});
            }
        }
        left_begin = left_end;
    }
    // BigramTriple has operator< but is not std::totally_ordered, so std::ranges::sort's
    // sortable concept is not satisfied.
    std::sort(out.begin(), out.end()); // NOLINT(modernize-use-ranges)
    return out;
}

// Runs the refactored emitter, collecting the pairs as a sorted multiset and (if
// requested) reporting the guard's did_sort.
std::vector<BigramTriple> collect_new_bigrams(std::vector<PhrasePositionedTerm> terms,
                                              bool* did_sort = nullptr) {
    std::vector<BigramTriple> out;
    const bool ds = emit_adjacent_phrase_bigrams(
            terms, [&](std::string_view left, std::string_view right, uint32_t position) {
                out.push_back({std::string(left), std::string(right), position});
            });
    if (did_sort != nullptr) {
        *did_sort = ds;
    }
    // BigramTriple has operator< but is not std::totally_ordered, so std::ranges::sort's
    // sortable concept is not satisfied.
    std::sort(out.begin(), out.end()); // NOLINT(modernize-use-ranges)
    return out;
}

// A1: monotone positions {0,1,2} -> two adjacent pairs, guard short-circuits.
TEST(SniiPhraseBigramBuildTest, EmitsSamePairsAsSortedBaseline) {
    const std::vector<std::string> storage = {"alpha", "beta", "gamma"};
    auto terms = make_positioned(storage, {0, 1, 2});

    bool did_sort = true;
    const auto got = collect_new_bigrams(terms, &did_sort);

    const std::vector<BigramTriple> expected = {{.left = "alpha", .right = "beta", .position = 0},
                                                {.left = "beta", .right = "gamma", .position = 1}};
    EXPECT_EQ(got, expected);
    EXPECT_EQ(got, reference_old_bigrams(terms));
    EXPECT_FALSE(did_sort); // analyzer-ordered input never trips the sort guard
}

// A2: two tokens share position 0 -> full left x right product at the boundary.
TEST(SniiPhraseBigramBuildTest, SamePositionEmitsFullProduct) {
    const std::vector<std::string> storage = {"aa", "bb", "cc"};
    auto terms = make_positioned(storage, {0, 0, 1});

    bool did_sort = true;
    const auto got = collect_new_bigrams(terms, &did_sort);

    const std::vector<BigramTriple> expected = {{.left = "aa", .right = "cc", .position = 0},
                                                {.left = "bb", .right = "cc", .position = 0}};
    EXPECT_EQ(got, expected);
    EXPECT_EQ(got, reference_old_bigrams(terms));
    EXPECT_FALSE(did_sort);
}

// A3: a position gap {0,2} has no adjacent (+1) pair -> empty, guard clean.
TEST(SniiPhraseBigramBuildTest, PositionGapEmitsNothing) {
    const std::vector<std::string> storage = {"aa", "bb"};
    auto terms = make_positioned(storage, {0, 2});

    bool did_sort = true;
    const auto got = collect_new_bigrams(terms, &did_sort);

    EXPECT_TRUE(got.empty());
    EXPECT_EQ(got, reference_old_bigrams(terms));
    EXPECT_FALSE(did_sort);
}

// A4 (degenerate): 0 and 1 element inputs emit nothing and do not crash.
TEST(SniiPhraseBigramBuildTest, EmptyAndSingleEmitNothing) {
    std::vector<PhrasePositionedTerm> empty;
    bool did_sort = true;
    EXPECT_TRUE(collect_new_bigrams(empty, &did_sort).empty());
    EXPECT_FALSE(did_sort);

    const std::vector<std::string> storage = {"solo"};
    auto single = make_positioned(storage, {0});
    EXPECT_TRUE(collect_new_bigrams(single, &did_sort).empty());
    EXPECT_FALSE(did_sort);
}

// A5 (equivalence): a shuffled input trips the guard (did_sort == true) yet the
// emitted set still equals the sorted baseline -- new path == old path.
TEST(SniiPhraseBigramBuildTest, UnsortedInputSortsAndMatches) {
    const std::vector<std::string> storage = {"gamma", "alpha", "beta"};
    auto terms = make_positioned(storage, {2, 0, 1});

    bool did_sort = false;
    const auto got = collect_new_bigrams(terms, &did_sort);

    const std::vector<BigramTriple> expected = {{.left = "alpha", .right = "beta", .position = 0},
                                                {.left = "beta", .right = "gamma", .position = 1}};
    EXPECT_EQ(got, expected);
    EXPECT_EQ(got, reference_old_bigrams(terms));
    EXPECT_TRUE(did_sort); // shuffled input forces the cold-path sort
}

// A6 (hidden term): the writer filters tokens through
// is_phrase_bigram_indexable_term before emitting, so a non-indexable token (too
// long / non-ASCII-alpha) never leaks into a bigram pair. Mirror that filter and
// assert the excluded token appears in no emitted pair.
TEST(SniiPhraseBigramBuildTest, NonIndexableTermIsFilteredOut) {
    const std::string too_long(40, 'z');  // > 32 bytes: not indexable
    const std::string non_alpha = "ab12"; // digits: not indexable
    const std::vector<std::string> storage = {"left", too_long, non_alpha, "right"};
    const std::vector<uint32_t> positions = {0, 1, 2, 3};

    // Filter exactly as SniiIndexColumnWriter::_add_phrase_bigram_tokens does.
    std::vector<PhrasePositionedTerm> filtered;
    for (size_t i = 0; i < storage.size(); ++i) {
        if (is_phrase_bigram_indexable_term(storage[i])) {
            filtered.push_back({std::string_view(storage[i]), positions[i]});
        }
    }
    // Only "left"@0 and "right"@3 survive; they are not adjacent (+1), so no pair
    // is emitted and neither hidden token is ever referenced.
    const auto got = collect_new_bigrams(filtered);
    EXPECT_TRUE(got.empty());
    for (const auto& triple : got) {
        EXPECT_NE(triple.left, too_long);
        EXPECT_NE(triple.right, too_long);
        EXPECT_NE(triple.left, non_alpha);
        EXPECT_NE(triple.right, non_alpha);
    }
}

// Deterministic perf seam: the writer reuses one positioned-term vector via
// clear() (capacity retained). Emulate two consecutive rows and assert the second
// batch triggers no reallocation.
TEST(SniiPhraseBigramBuildTest, ReusedVectorKeepsCapacityAcrossBatches) {
    const std::vector<std::string> batch1 = {"aa", "bb", "cc", "dd", "ee", "ff"};
    const std::vector<std::string> batch2 = {"gg", "hh", "ii"};

    std::vector<PhrasePositionedTerm> reused;
    auto fill = [&](const std::vector<std::string>& storage) {
        reused.clear(); // retains capacity, exactly like _bigram_positioned
        for (uint32_t i = 0; i < storage.size(); ++i) {
            reused.push_back({std::string_view(storage[i]), i});
        }
    };

    fill(batch1);
    static_cast<void>(emit_adjacent_phrase_bigrams(
            reused, [](std::string_view, std::string_view, uint32_t) {}));
    const size_t cap_after_batch1 = reused.capacity();
    ASSERT_GE(cap_after_batch1, batch1.size());

    fill(batch2); // batch2.size() <= cap_after_batch1 -> no realloc
    static_cast<void>(emit_adjacent_phrase_bigrams(
            reused, [](std::string_view, std::string_view, uint32_t) {}));
    EXPECT_EQ(reused.capacity(), cap_after_batch1);
}

// ============================ sub-task B ============================

// SampledTermIndexReader::heap_bytes(): all-SSO sample terms have no per-string
// heap, so the charge is exactly n_blocks * sizeof(std::string) (reserve-exact
// backing buffer).
TEST(SniiSegmentReaderTest, SampledTermIndexHeapBytesMatchesFormula) {
    const std::vector<std::string> terms = {"s000", "s001", "s002", "s003", "s004", "s005"};
    SampledTermIndexBuilder builder;
    for (const auto& term : terms) {
        builder.add_block_first_term(term); // strictly ascending, SSO
    }
    ByteSink sink;
    builder.finish(&sink);

    SampledTermIndexReader reader;
    assert_ok(SampledTermIndexReader::open(sink.view(), &reader));
    ASSERT_EQ(reader.n_blocks(), terms.size());
    EXPECT_EQ(reader.heap_bytes(), terms.size() * sizeof(std::string));
}

// The std_string_heap_bytes accumulation: an over-15-byte sample term adds its
// heap buffer on top of the vector buffer.
TEST(SniiSegmentReaderTest, SampledTermIndexHeapBytesCountsLongTerms) {
    const std::string long_term = "b_this_is_a_long_sample_term_well_over_15_bytes";
    ASSERT_GT(long_term.size(), 15U);
    const std::vector<std::string> terms = {"a_short", long_term, "c_short"};
    SampledTermIndexBuilder builder;
    for (const auto& term : terms) {
        builder.add_block_first_term(term);
    }
    ByteSink sink;
    builder.finish(&sink);

    SampledTermIndexReader reader;
    assert_ok(SampledTermIndexReader::open(sink.view(), &reader));
    const size_t vector_only = terms.size() * sizeof(std::string);
    EXPECT_GT(reader.heap_bytes(), vector_only);
    // capacity() >= size(), so the long term contributes >= size()+1 heap bytes.
    EXPECT_GE(reader.heap_bytes(), vector_only + long_term.size() + 1);
    // Cross-check the shared helper on an SSO vs non-SSO string.
    EXPECT_EQ(std_string_heap_bytes(std::string("short")), 0U);
    EXPECT_GT(std_string_heap_bytes(long_term), 0U);
}

// DictBlockDirectoryReader::heap_bytes(): BlockRef is trivially copyable, so the
// charge is exactly n_blocks * sizeof(BlockRef).
TEST(SniiSegmentReaderTest, DictBlockDirectoryHeapBytesMatchesFormula) {
    DictBlockDirectoryBuilder builder;
    constexpr uint32_t kBlocks = 5;
    for (uint32_t i = 0; i < kBlocks; ++i) {
        BlockRef ref;
        ref.offset = 100000ULL * (i + 1); // multi-byte varints -> each ref > 8 bytes
        ref.length = 640;
        ref.n_entries = 3;
        ref.flags = 0;
        ref.checksum = 0xDEAD0000U + i;
        builder.add(ref);
    }
    ByteSink sink;
    builder.finish(&sink);

    DictBlockDirectoryReader reader;
    assert_ok(DictBlockDirectoryReader::open(sink.view(), &reader));
    ASSERT_EQ(reader.n_blocks(), kBlocks);
    EXPECT_EQ(reader.heap_bytes(), static_cast<size_t>(kBlocks) * sizeof(BlockRef));
}

// A minimal slim pod_ref entry that round-trips at tier T1 (extra tier>=T2 fields
// are ignored on encode). Terms are supplied by the caller in ascending order.
DictEntry make_pod_ref(std::string term) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kPodRef;
    e.enc = DictEntryEnc::kSlim;
    e.df = 3;
    e.ttf_delta = 6;
    e.max_freq = 9;
    e.frq_off_delta = 0;
    e.frq_len = 128;
    e.frq_docs_len = 64; // dd region on-disk length (<= frq_len)
    e.dd_meta.uncomp_len = 70;
    e.dd_meta.crc = 0xABCD1234U;
    e.freq_meta.uncomp_len = 40;
    e.freq_meta.crc = 0x55AA00FFU;
    e.prx_off_delta = 0;
    e.prx_len = 64;
    return e;
}

std::vector<uint8_t> build_dict_block(const std::vector<std::string>& terms,
                                      uint32_t anchor_interval) {
    DictBlockBuilder builder(IndexTier::kT1, /*has_positions=*/false, /*frq_base=*/0,
                             /*prx_base=*/0, anchor_interval);
    for (const auto& term : terms) {
        builder.add_entry(make_pod_ref(term));
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

// DictBlockReader::heap_bytes(): with anchor_interval 16 and 20 SSO entries there
// are two anchors (indices 0 and 16), each with an SSO anchor term, so the charge
// is exactly n_anchors * (sizeof(uint32_t) + sizeof(std::string)).
TEST(SniiSegmentReaderTest, DictBlockAnchorHeapBytesMatchesFormula) {
    constexpr uint32_t kEntries = 20;
    constexpr uint32_t kAnchorInterval = 16;
    std::vector<std::string> terms;
    terms.reserve(kEntries);
    for (uint32_t i = 0; i < kEntries; ++i) {
        // "dt_00".."dt_19": strictly ascending, 5 bytes (SSO).
        terms.push_back("dt_" + std::string(1, static_cast<char>('0' + i / 10)) +
                        std::string(1, static_cast<char>('0' + i % 10)));
    }
    const std::vector<uint8_t> bytes = build_dict_block(terms, kAnchorInterval);

    DictBlockReader reader;
    assert_ok(
            DictBlockReader::open(Slice(bytes), IndexTier::kT1, /*has_positions=*/false, &reader));
    ASSERT_EQ(reader.n_entries(), kEntries);

    const size_t n_anchors = (kEntries + kAnchorInterval - 1) / kAnchorInterval; // == 2
    EXPECT_EQ(reader.heap_bytes(), n_anchors * (sizeof(uint32_t) + sizeof(std::string)));
}

// A long (> 15 byte) anchor term adds its heap buffer beyond the anchor vectors.
TEST(SniiSegmentReaderTest, DictBlockAnchorHeapBytesCountsLongAnchor) {
    // One entry -> one anchor (entry 0), whose term is > 15 bytes.
    const std::string long_term = "a_dict_anchor_term_well_over_15_bytes";
    ASSERT_GT(long_term.size(), 15U);
    const std::vector<uint8_t> bytes = build_dict_block({long_term}, /*anchor_interval=*/16);

    DictBlockReader reader;
    assert_ok(
            DictBlockReader::open(Slice(bytes), IndexTier::kT1, /*has_positions=*/false, &reader));
    ASSERT_EQ(reader.n_entries(), 1U);
    const size_t vector_only = sizeof(uint32_t) + sizeof(std::string); // one anchor
    EXPECT_GT(reader.heap_bytes(), vector_only);
    EXPECT_GE(reader.heap_bytes(), vector_only + long_term.size() + 1);
}

} // namespace
