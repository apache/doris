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

#include "storage/index/query/exec/block_doc_set.h"

#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <vector>

#include "storage/index/query/exec/bitmap_conjunction.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/exec/nullable_conjunction.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::index_query {
namespace {

class TestBlockCursor final : public PostingsCursor {
public:
    explicit TestBlockCursor(std::vector<PostingsBlock> blocks) : _blocks(std::move(blocks)) {}
    uint32_t doc_freq() const override { return frequency_hint; }
    bool cheap_seek() const override { return true; }
    Status next_block(PostingsBlock* block, bool* eof) override {
        ++reads;
        if (fail_after_first_read && reads > 1) {
            return Status::IOError("Injected postings read failure");
        }
        *eof = _next == _blocks.size();
        *block = *eof ? PostingsBlock() : _blocks[_next++];
        return Status::OK();
    }
    Status seek_block(uint32_t target, PostingsBlock* block, bool* eof) override {
        bool moved = false;
        RETURN_IF_ERROR(shallow_seek(target, &moved));
        return next_block(block, eof);
    }
    Status shallow_seek(uint32_t target, bool* moved) override {
        const auto before = _next;
        while (_next < _blocks.size() &&
               _blocks[_next].doc_at(_blocks[_next].size() - 1) < target) {
            ++_next;
        }
        *moved = before != _next;
        return Status::OK();
    }
    BlockBound current_block_bound() const override { return {}; }
    uint32_t frequency_hint = 0;
    size_t reads = 0;
    bool fail_after_first_read = false;

private:
    std::vector<PostingsBlock> _blocks;
    size_t _next = 0;
};

class BlockDocSetReference {
public:
    explicit BlockDocSetReference(BlockDocSet& docs) : _docs(&docs) {}
    uint64_t doc() const { return _docs->exhausted() ? kDocIdEnd : _docs->doc(); }
    uint64_t seek(uint32_t target) {
        _docs->seek(target);
        return doc();
    }

private:
    BlockDocSet* _docs;
};

TEST(NullableConjunctionTest, PreservesTrueAndUnknownAtUint32Boundary) {
    constexpr auto last = std::numeric_limits<uint32_t>::max();
    const std::array<uint32_t, 3> left_rows {0, last - 1, last};
    const std::array<uint32_t, 3> right_rows {0, 5, last};
    const auto left_nulls = roaring::Roaring::bitmapOf(1, 5);
    const auto right_nulls = roaring::Roaring::bitmapOf(1, last - 1);
    for (const uint64_t limit : {uint64_t {10}, kDocIdEnd}) {
        SCOPED_TRACE(limit);
        TestBlockCursor left_source({{.docs = left_rows, .freqs = {}, .norms = {}}});
        TestBlockCursor right_source({{.docs = right_rows, .freqs = {}, .norms = {}}});
        BlockDocSet left(left_source);
        BlockDocSet right(right_source);
        std::array inputs {NullableDocSet(BlockDocSetReference(left), &left_nulls),
                           NullableDocSet(BlockDocSetReference(right), &right_nulls)};
        roaring::Roaring truths;
        roaring::Roaring nulls;
        RoaringDocIdSink true_sink(truths);
        RoaringDocIdSink null_sink(nulls);
        std::vector<uint32_t> visited;
        ASSERT_TRUE(collect_nullable_conjunction(std::span(inputs), limit, true_sink, null_sink,
                                                 [&](uint32_t doc) { visited.push_back(doc); })
                            .ok());
        auto expected_true = roaring::Roaring::bitmapOf(1, 0);
        auto expected_null = roaring::Roaring::bitmapOf(1, 5);
        if (limit == kDocIdEnd) {
            expected_true.add(last);
            expected_null.add(last - 1);
        }
        EXPECT_EQ(truths, expected_true);
        EXPECT_EQ(nulls, expected_null);
        EXPECT_EQ(visited, (std::vector<uint32_t>(expected_true.begin(), expected_true.end())));
    }
}

void expect_statistics(const DorisVector<PostingStatistics>& rows, uint32_t doc, uint32_t frequency,
                       uint32_t norm) {
    const auto row = std::ranges::find(rows, doc, &PostingStatistics::doc);
    ASSERT_NE(row, rows.end());
    EXPECT_EQ(row->frequency, frequency);
    EXPECT_EQ(row->norm, norm);
}

TEST(BitmapConjunctionTest, PreservesSelectedStatisticsAndClipsDocumentSpace) {
    const std::array<uint32_t, 4> left_rows {3, 8, 13, 21};
    const std::array<uint32_t, 4> left_freqs {2, 3, 5, 7};
    const std::array<uint32_t, 4> left_norms {11, 13, 17, 19};
    const std::array<uint32_t, 4> right_rows {3, 5, 13, 34};
    const std::array<uint32_t, 4> right_freqs {23, 29, 31, 37};
    const std::array<uint32_t, 4> right_norms {41, 43, 47, 53};
    const auto left_nulls = roaring::Roaring::bitmapOf(2, 5, 34);
    const auto right_nulls = roaring::Roaring::bitmapOf(2, 8, 21);
    TestBlockCursor left_source({{.docs = left_rows, .freqs = left_freqs, .norms = left_norms}});
    TestBlockCursor right_source(
            {{.docs = right_rows, .freqs = right_freqs, .norms = right_norms}});
    BlockDocSet left(left_source);
    BlockDocSet right(right_source);
    const std::array inputs {ConjunctionPostings {.docs = &left, .null_rows = &left_nulls},
                             ConjunctionPostings {.docs = &right, .null_rows = &right_nulls}};
    BitmapConjunctionResult result;
    ASSERT_TRUE(collect_bitmap_conjunction<true>(std::span(inputs), 32, &result).ok());
    EXPECT_EQ(result.truth.true_rows, roaring::Roaring::bitmapOf(2, 3, 13));
    EXPECT_EQ(result.truth.null_rows, roaring::Roaring::bitmapOf(3, 5, 8, 21));
    ASSERT_EQ(result.statistics.size(), 2);
    expect_statistics(result.statistics[0], 3, 2, 11);
    expect_statistics(result.statistics[0], 13, 5, 17);
    expect_statistics(result.statistics[1], 3, 23, 41);
    expect_statistics(result.statistics[1], 13, 31, 47);
}

TEST(BitmapConjunctionTest, EqualNullGroupsPreserveOriginalStatisticsSlots) {
    const std::array<uint32_t, 4> a {0, 3, 8, 13};
    const std::array<uint32_t, 4> b {0, 5, 13, 21};
    const std::array<uint32_t, 4> c {0, 3, 13, 34};
    const std::array<uint32_t, 4> d {0, 5, 13, 34};
    const auto left_nulls = roaring::Roaring::bitmapOf(3, 5, 21, 40);
    const auto right_nulls = roaring::Roaring::bitmapOf(2, 3, 40);
    const auto equal_left_nulls = left_nulls;
    const auto equal_right_nulls = right_nulls;
    const std::array<uint32_t, 4> frequencies {2, 3, 5, 7};
    const std::array<uint32_t, 4> norms {11, 13, 17, 19};
    TestBlockCursor source_a({{.docs = a, .freqs = frequencies, .norms = norms}});
    TestBlockCursor source_b({{.docs = b, .freqs = norms, .norms = frequencies}});
    TestBlockCursor source_c({{.docs = c, .freqs = frequencies, .norms = norms}});
    TestBlockCursor source_d({{.docs = d, .freqs = norms, .norms = frequencies}});
    BlockDocSet docs_a(source_a);
    BlockDocSet docs_b(source_b);
    BlockDocSet docs_c(source_c);
    BlockDocSet docs_d(source_d);
    const std::array inputs {
            ConjunctionPostings {.docs = &docs_a, .null_rows = &left_nulls},
            ConjunctionPostings {.docs = &docs_b, .null_rows = &right_nulls},
            ConjunctionPostings {.docs = &docs_c, .null_rows = &equal_left_nulls},
            ConjunctionPostings {.docs = &docs_d, .null_rows = &equal_right_nulls}};
    BitmapConjunctionResult result;
    ASSERT_TRUE(collect_bitmap_conjunction<true>(std::span(inputs), 64, &result).ok());
    EXPECT_EQ(result.truth.true_rows, roaring::Roaring::bitmapOf(2, 0, 13));
    EXPECT_EQ(result.truth.null_rows, roaring::Roaring::bitmapOf(3, 3, 5, 40));
    ASSERT_EQ(result.statistics.size(), 4);
    expect_statistics(result.statistics[0], 13, 7, 19);
    expect_statistics(result.statistics[1], 13, 17, 5);
    expect_statistics(result.statistics[2], 13, 5, 17);
    expect_statistics(result.statistics[3], 13, 17, 5);
}

TEST(BitmapConjunctionTest, EqualNullGroupsStopAtTheLastCandidate) {
    const std::array<uint32_t, 1> first_rows {3};
    const auto first_nulls = roaring::Roaring::bitmapOf(1, 5);
    TestBlockCursor first_source({{.docs = first_rows, .freqs = {}, .norms = {}}});
    const PostingsBlock block {
            .docs = {}, .freqs = {}, .norms = {}, .range_end = 1000, .dense = true};
    TestBlockCursor second_source({block});
    TestBlockCursor third_source({block});
    second_source.frequency_hint = 1000;
    third_source.frequency_hint = 1000;
    second_source.fail_after_first_read = true;
    third_source.fail_after_first_read = true;
    BlockDocSet first(first_source);
    BlockDocSet second(second_source);
    BlockDocSet third(third_source);
    const std::array inputs {ConjunctionPostings {.docs = &first, .null_rows = &first_nulls},
                             ConjunctionPostings {.docs = &second, .null_rows = nullptr},
                             ConjunctionPostings {.docs = &third, .null_rows = nullptr}};
    BitmapConjunctionResult result;
    ASSERT_TRUE(collect_bitmap_conjunction<true>(std::span(inputs), 65536, &result).ok());
    EXPECT_EQ(result.truth.true_rows, roaring::Roaring::bitmapOf(1, 3));
    EXPECT_EQ(result.truth.null_rows, first_nulls);
    EXPECT_EQ(second_source.reads, 1);
    EXPECT_EQ(third_source.reads, 1);
    expect_statistics(result.statistics[1], 3, 1, 1);
    expect_statistics(result.statistics[2], 3, 1, 1);
}

void expect_dense_conjunction(uint64_t end) {
    const PostingsBlock block {
            .docs = {}, .freqs = {}, .norms = {}, .range_end = end, .dense = true};
    TestBlockCursor first_source({block});
    TestBlockCursor second_source({block});
    const auto frequency_hint =
            static_cast<uint32_t>(std::min(end, uint64_t(std::numeric_limits<uint32_t>::max())));
    first_source.frequency_hint = frequency_hint;
    second_source.frequency_hint = frequency_hint;
    BlockDocSet first(first_source);
    BlockDocSet second(second_source);
    const std::array inputs {ConjunctionPostings {.docs = &first, .null_rows = nullptr},
                             ConjunctionPostings {.docs = &second, .null_rows = nullptr}};
    BitmapConjunctionResult result;
    ASSERT_TRUE(collect_bitmap_conjunction<false>(std::span(inputs), end, &result).ok());
    roaring::Roaring expected;
    expected.addRange(0, end);
    EXPECT_EQ(result.truth.true_rows, expected);
    EXPECT_TRUE(result.truth.null_rows.isEmpty());
    EXPECT_LE(result.truth.true_rows.getSizeInBytes(), expected.getSizeInBytes());
}

TEST(BitmapConjunctionTest, DenseConjunctionPreservesRangeCompression) {
    expect_dense_conjunction(uint64_t {1} << 20);
    expect_dense_conjunction(uint64_t {1} << 32);
}

TEST(BitmapConjunctionTest, EmptyPossibleRowsStopLaterPostingsReads) {
    TestBlockCursor empty_source({});
    const std::array<uint32_t, 2> rows {3, 7};
    TestBlockCursor later_source({{.docs = rows, .freqs = {}, .norms = {}}});
    later_source.fail_after_first_read = true;
    BlockDocSet empty(empty_source);
    BlockDocSet later(later_source);
    const std::array inputs {ConjunctionPostings {.docs = &empty, .null_rows = nullptr},
                             ConjunctionPostings {.docs = &later, .null_rows = nullptr}};
    BitmapConjunctionResult result;
    ASSERT_TRUE(collect_bitmap_conjunction<true>(std::span(inputs), 32, &result).ok());
    EXPECT_TRUE(result.truth.true_rows.isEmpty());
    EXPECT_TRUE(result.truth.null_rows.isEmpty());
    EXPECT_TRUE(result.statistics[1].empty());
    EXPECT_EQ(later_source.reads, 1);
}

TEST(BlockDocSetTest, ReadFailureDoesNotBecomeEndOfPostings) {
    TestBlockCursor source({{.docs = {},
                             .freqs = {},
                             .norms = {},
                             .range_begin = 1,
                             .range_end = 2,
                             .dense = true}});
    source.fail_after_first_read = true;
    BlockDocSet docs(source);
    EXPECT_EQ(docs.doc(), 1);
    EXPECT_THROW(docs.advance(), Exception);
    EXPECT_EQ(source.reads, 2);
}

std::vector<uint32_t> collect_rows(BlockDocSet& docs) {
    std::vector<uint32_t> actual;
    do {
        actual.push_back(docs.doc());
        const auto expected_freq = docs.doc() < 11 ? docs.doc() - 5 : 1;
        EXPECT_EQ(docs.freq(), expected_freq);
        EXPECT_EQ(docs.norm(), 1);
    } while (docs.advance());
    return actual;
}

TEST(BlockDocSetTest, DenseAndExplicitBlocksPreserveFullDocumentSpace) {
    constexpr auto max_doc = std::numeric_limits<uint32_t>::max();
    const std::array<uint32_t, 2> sparse {17, 29};
    const std::array<uint32_t, 4> freqs {2, 3, 4, 5};
    const std::vector<PostingsBlock> blocks {{.docs = {},
                                              .freqs = freqs,
                                              .norms = {},
                                              .range_begin = 7,
                                              .range_end = 11,
                                              .dense = true},
                                             {.docs = sparse, .freqs = {}, .norms = {}},
                                             {.docs = {},
                                              .freqs = {},
                                              .norms = {},
                                              .range_begin = max_doc - 1,
                                              .range_end = uint64_t(max_doc) + 1,
                                              .dense = true}};
    const std::vector<uint32_t> expected {7, 8, 9, 10, 17, 29, max_doc - 1, max_doc};
    for (const uint32_t target : {0U, 8U, 10U, 11U, 17U, 18U, 29U, 30U, max_doc}) {
        TestBlockCursor source(blocks);
        BlockDocSet docs(source);
        ASSERT_TRUE(docs.seek(target));
        const auto first = std::ranges::lower_bound(expected, target);
        const auto actual = collect_rows(docs);
        EXPECT_EQ(actual, (std::vector<uint32_t>(first, expected.end())));
        const auto reads_at_end = source.reads;
        EXPECT_FALSE(docs.seek(max_doc));
        EXPECT_FALSE(docs.advance());
        EXPECT_EQ(source.reads, reads_at_end);
    }
}

TEST(BlockDocSetTest, DenseRangeCanSeekTheLastUint32Document) {
    constexpr auto max_doc = std::numeric_limits<uint32_t>::max();
    TestBlockCursor whole_space({{.docs = {},
                                  .freqs = {},
                                  .norms = {},
                                  .range_end = uint64_t(max_doc) + 1,
                                  .dense = true}});
    BlockDocSet docs(whole_space);
    ASSERT_TRUE(docs.seek(max_doc));
    EXPECT_EQ(docs.doc(), max_doc);
    EXPECT_FALSE(docs.advance());
}

TEST(BlockDocSetTest, CollectionKeepsSelectedRowsAndStatisticsAligned) {
    const std::array<uint32_t, 3> sparse {3, 8, 13};
    const std::array<uint32_t, 3> sparse_freqs {2, 3, 5};
    const std::array<uint32_t, 3> sparse_norms {10, 12, 14};
    const std::array<uint32_t, 3> dense_freqs {7, 11, 13};
    const std::array<uint32_t, 3> dense_norms {19, 21, 23};
    TestBlockCursor source({{.docs = sparse, .freqs = sparse_freqs, .norms = sparse_norms},
                            {.docs = {},
                             .freqs = dense_freqs,
                             .norms = dense_norms,
                             .range_begin = 17,
                             .range_end = 20,
                             .dense = true}});
    BlockDocSet docs(source);
    const auto candidates = roaring::Roaring::bitmapOf(3, 8, 19, 22);
    roaring::Roaring rows;
    RoaringDocIdSink sink(rows);
    std::vector<std::array<uint32_t, 3>> selected;
    ASSERT_TRUE(collect_postings<true>(docs, &candidates, sink,
                                       [&](uint32_t doc, uint32_t freq, uint32_t norm) {
                                           selected.push_back({doc, freq, norm});
                                       })
                        .ok());
    EXPECT_EQ(rows, roaring::Roaring::bitmapOf(2, 8, 19));
    const std::vector<std::array<uint32_t, 3>> expected {{8, 3, 12}, {19, 13, 23}};
    EXPECT_EQ(selected, expected);
}

class RangeCountingSink final : public DocIdSink {
public:
    Status append_sorted(std::span<const uint32_t>) override {
        ++sorted_calls;
        return Status::InternalError("Expected a range handoff");
    }
    Status append_range(uint32_t first, uint64_t end) override {
        ++range_calls;
        begin = first;
        last = end;
        return fail ? Status::IOError("Injected sink failure") : Status::OK();
    }
    size_t sorted_calls = 0;
    size_t range_calls = 0;
    uint32_t begin = 0;
    uint64_t last = 0;
    bool fail = false;
};

TEST(BlockDocSetTest, CollectionPreservesFullDocumentRangeWithoutExpansion) {
    constexpr uint64_t end = uint64_t(std::numeric_limits<uint32_t>::max()) + 1;
    for (bool restricted : {false, true}) {
        TestBlockCursor source(
                {{.docs = {}, .freqs = {}, .norms = {}, .range_end = end, .dense = true}});
        BlockDocSet docs(source);
        roaring::Roaring domain;
        domain.addRange(0, end);
        RangeCountingSink sink;
        const auto* candidates = restricted ? &domain : nullptr;
        ASSERT_TRUE(collect_postings<false>(docs, candidates, sink, nullptr).ok());
        EXPECT_EQ(sink.sorted_calls, 0);
        EXPECT_EQ(sink.range_calls, 1);
        EXPECT_EQ(sink.begin, 0);
        EXPECT_EQ(sink.last, end);
    }
}

TEST(BlockDocSetTest, SparseCandidatesCanSkipToTheLastDocumentInADenseRange) {
    constexpr auto max_doc = std::numeric_limits<uint32_t>::max();
    TestBlockCursor source({{.docs = {},
                             .freqs = {},
                             .norms = {},
                             .range_end = uint64_t(max_doc) + 1,
                             .dense = true}});
    BlockDocSet docs(source);
    const auto candidates = roaring::Roaring::bitmapOf(2, 8U, max_doc);
    roaring::Roaring rows;
    RoaringDocIdSink sink(rows);
    ASSERT_TRUE(collect_postings<false>(docs, &candidates, sink, nullptr).ok());
    EXPECT_EQ(rows, candidates);
    EXPECT_EQ(source.reads, 1);
}

TEST(BlockDocSetTest, EmptyDomainDoesNotReadOrVisitRows) {
    TestBlockCursor source({{.docs = {}, .freqs = {}, .norms = {}, .range_end = 8, .dense = true}});
    source.fail_after_first_read = true;
    BlockDocSet docs(source);
    RangeCountingSink sink;
    sink.fail = true;
    const roaring::Roaring domain;
    size_t visits = 0;
    const auto status = collect_postings<true>(docs, &domain, sink,
                                               [&](uint32_t, uint32_t, uint32_t) { ++visits; });
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(source.reads, 1);
    EXPECT_EQ(visits, 0);
    EXPECT_EQ(sink.range_calls + sink.sorted_calls, 0);
}

TEST(BlockDocSetTest, SinkFailureStopsBeforeReadingAnotherBlock) {
    TestBlockCursor source({{.docs = {}, .freqs = {}, .norms = {}, .range_end = 8, .dense = true}});
    source.fail_after_first_read = true;
    BlockDocSet docs(source);
    RangeCountingSink sink;
    sink.fail = true;
    const auto status = collect_postings<false>(docs, nullptr, sink, nullptr);
    EXPECT_EQ(status.code(), ErrorCode::IO_ERROR);
    EXPECT_EQ(source.reads, 1);
}

} // namespace
} // namespace doris::index_query
