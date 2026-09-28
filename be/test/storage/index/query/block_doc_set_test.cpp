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

#include <algorithm>
#include <array>
#include <limits>
#include <vector>

#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::index_query {
namespace {

class TestBlockCursor final : public PostingsCursor {
public:
    explicit TestBlockCursor(std::vector<PostingsBlock> blocks) : _blocks(std::move(blocks)) {}
    uint32_t doc_freq() const override { return frequency_hint; }
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
