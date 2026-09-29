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

#include "storage/index/query/exec/cursor_chained_postings.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "storage/index/query/fake_index_source.h"

namespace doris::index_query {
namespace {

using testing::FakePostingsCursor;

// A posting of several blocks, dense or listed, so the walks cross block bounds.
class BlockedCursor final : public PostingsCursor {
public:
    struct Block {
        std::vector<uint32_t> docs;
        uint32_t range_begin = 0;
        uint32_t range_end = 0;
        bool dense = false;

        uint32_t last() const { return dense ? range_end - 1 : docs.back(); }
    };

    static Block listed(std::vector<uint32_t> docs) {
        Block block;
        block.docs = std::move(docs);
        return block;
    }

    static Block dense(uint32_t range_begin, uint32_t range_end) {
        Block block;
        block.range_begin = range_begin;
        block.range_end = range_end;
        block.dense = true;
        return block;
    }

    explicit BlockedCursor(std::vector<Block> blocks) : _blocks(std::move(blocks)) {}

    uint32_t doc_freq() const override {
        uint32_t count = 0;
        for (const Block& block : _blocks) {
            count += block.dense ? block.range_end - block.range_begin : block.docs.size();
        }
        return count;
    }

    Status next_block(PostingsBlock* block, bool* eof) override {
        *block = {};
        *eof = _next >= _blocks.size();
        if (*eof) {
            return Status::OK();
        }
        const Block& source = _blocks[_next++];
        ++decoded;
        block->dense = source.dense;
        block->range_begin = source.range_begin;
        block->range_end = source.range_end;
        block->docs = source.docs;
        return Status::OK();
    }

    Status seek_block(uint32_t target, PostingsBlock* block, bool* eof) override {
        while (_next < _blocks.size() && _blocks[_next].last() < target) {
            ++_next;
        }
        return next_block(block, eof);
    }

    Status shallow_seek(uint32_t /*target*/, bool* moved) override {
        *moved = false;
        return Status::OK();
    }

    BlockBound current_block_bound() const override { return {}; }

    size_t decoded = 0;

private:
    std::vector<Block> _blocks;
    size_t _next = 0;
};

// Each visited block's first document, followed by the candidates handed with it.
std::vector<uint32_t> visited(PostingsCursor& cursor, const std::vector<uint32_t>& candidates) {
    std::vector<uint32_t> seen;
    EXPECT_TRUE(for_each_candidate_block(
                        cursor, candidates,
                        [&](const PostingsBlock& block, std::span<const uint32_t> slice) {
                            seen.push_back(block.doc_at(0));
                            seen.insert(seen.end(), slice.begin(), slice.end());
                            return Status::OK();
                        })
                        .ok());
    return seen;
}

std::vector<uint32_t> collected(PostingsCursor& cursor, const std::vector<uint32_t>& candidates) {
    CursorChainedPostings term(cursor);
    EXPECT_TRUE(term.start(&candidates).ok());
    std::vector<uint32_t> docs;
    EXPECT_TRUE(term.collect(&docs).ok());
    return docs;
}

FakePostingsCursor::Posting posting(uint32_t doc) {
    return {.doc = doc, .positions = {}};
}

TEST(CursorChainedPostings, WalksOnlyTheBlocksThatCanHoldCandidates) {
    BlockedCursor cursor({BlockedCursor::listed({1, 4, 6}), BlockedCursor::dense(10, 14),
                          BlockedCursor::listed({20, 25}), BlockedCursor::listed({30, 31}),
                          BlockedCursor::listed({40, 41})});
    // 15 falls between two blocks and is dropped; 26 makes the walk decode the block that
    // could hold it, which holds no candidate; nothing asks for the last block.
    EXPECT_EQ(visited(cursor, {2, 4, 5, 11, 12, 15, 25, 26}),
              (std::vector<uint32_t> {1, 2, 4, 5, 10, 11, 12, 20, 25}));
    EXPECT_EQ(cursor.decoded, 4U);
}

TEST(CursorChainedPostings, CandidatesPastTheLastBlockEndTheWalk) {
    BlockedCursor cursor({BlockedCursor::listed({1, 2}), BlockedCursor::listed({8, 9})});
    EXPECT_EQ(visited(cursor, {9, 40, 41}), (std::vector<uint32_t> {8, 9}));
    EXPECT_EQ(cursor.decoded, 1U);
    BlockedCursor untouched({BlockedCursor::listed({1, 2})});
    EXPECT_TRUE(visited(untouched, {5}).empty());
    EXPECT_EQ(untouched.decoded, 0U);
}

TEST(CursorChainedPostings, ListsEveryBlockWithoutCandidates) {
    BlockedCursor cursor({BlockedCursor::listed({1, 4}), BlockedCursor::dense(10, 13),
                          BlockedCursor::listed({20})});
    CursorChainedPostings term(cursor);
    EXPECT_EQ(term.doc_freq(), 6U);
    ASSERT_TRUE(term.start(nullptr).ok());
    std::vector<uint32_t> docs;
    ASSERT_TRUE(term.collect(&docs).ok());
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 4, 10, 11, 12, 20}));
}

// A block of exactly the candidates, a dense block, and blocks the candidates partly meet.
TEST(CursorChainedPostings, CollectsTheCandidatesEachBlockHolds) {
    BlockedCursor cursor({BlockedCursor::listed({1, 4, 6}), BlockedCursor::dense(10, 14),
                          BlockedCursor::listed({20, 25}), BlockedCursor::listed({30, 31})});
    EXPECT_EQ(collected(cursor, {1, 4, 6, 11, 12, 21, 25, 31, 40}),
              (std::vector<uint32_t> {1, 4, 6, 11, 12, 25, 31}));
}

// Many candidates in a narrow block go through the bit set, a few in a wide one are searched.
TEST(CursorChainedPostings, CollectsThroughEveryIntersection) {
    std::vector<uint32_t> evens;
    std::vector<uint32_t> thirds;
    std::vector<uint32_t> sixths;
    for (uint32_t doc = 0; doc < 128; ++doc) {
        if (doc % 2 == 0) {
            evens.push_back(doc);
        }
        if (doc % 3 == 0) {
            thirds.push_back(doc);
        }
        if (doc % 6 == 0) {
            sixths.push_back(doc);
        }
    }
    BlockedCursor narrow({BlockedCursor::listed(evens)});
    EXPECT_EQ(collected(narrow, thirds), sixths);
    std::vector<uint32_t> wide;
    for (uint32_t doc = 0; doc < 3000; doc += 3) {
        wide.push_back(doc);
    }
    BlockedCursor searched({BlockedCursor::listed(wide)});
    EXPECT_EQ(collected(searched, {3, 4, 2997}), (std::vector<uint32_t> {3, 2997}));
}

// A run of candidates filling a block's span keeps every document of the block.
TEST(CursorChainedPostings, ACandidateRunFillingABlockKeepsItsDocuments) {
    BlockedCursor cursor(
            {BlockedCursor::listed({10, 13, 20}), BlockedCursor::listed({30, 31, 40})});
    EXPECT_EQ(collected(cursor, {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 31, 35}),
              (std::vector<uint32_t> {10, 13, 20, 31}));
}

// A block of a few documents among many candidates searches the candidates for each.
TEST(CursorChainedPostings, AFewDocumentsAmongManyCandidatesAreSearched) {
    BlockedCursor cursor({BlockedCursor::listed({100, 5000, 9000})});
    std::vector<uint32_t> candidates;
    for (uint32_t doc = 100; doc <= 9000; doc += 2) {
        candidates.push_back(doc);
    }
    EXPECT_EQ(collected(cursor, candidates), (std::vector<uint32_t> {100, 5000, 9000}));
    BlockedCursor odd({BlockedCursor::listed({101, 5001, 9000})});
    EXPECT_EQ(collected(odd, candidates), (std::vector<uint32_t> {9000}));
}

TEST(CursorChainedPostings, ChainsCursorsThroughTheSharedConjunction) {
    std::vector<FakePostingsCursor::Prefetch> rare_prefetches;
    std::vector<FakePostingsCursor::Prefetch> wide_prefetches;
    FakePostingsCursor rare({posting(3), posting(9), posting(12)}, false, false, &rare_prefetches);
    FakePostingsCursor wide({posting(1), posting(3), posting(4), posting(9), posting(10)}, false,
                            false, &wide_prefetches);
    CursorChainedPostings rare_term(rare);
    CursorChainedPostings wide_term(wide);
    const std::vector<ChainedPostings*> chain = {&wide_term, &rare_term};
    std::vector<uint32_t> docs;
    std::vector<size_t> order;
    ASSERT_TRUE(chained_conjunction(chain, nullptr, &docs, &order).ok());
    EXPECT_EQ(docs, (std::vector<uint32_t> {3, 9}));
    EXPECT_EQ(order, (std::vector<size_t> {1, 0}));
    // The rare term read itself whole; the wide one was asked only for the rare one's rows.
    ASSERT_EQ(rare_prefetches.size(), 1U);
    EXPECT_TRUE(rare_prefetches[0].whole);
    EXPECT_FALSE(rare_prefetches[0].positions);
    ASSERT_EQ(wide_prefetches.size(), 1U);
    EXPECT_EQ(wide_prefetches[0].candidates, (std::vector<uint32_t> {3, 9, 12}));
}

// A cursor without its own block positions reads each asked document through its positions.
TEST(CursorChainedPostings, BlockPositionsByDefaultReadEachDocument) {
    FakePostingsCursor cursor({{.doc = 2, .positions = {1, 4}},
                               {.doc = 5, .positions = {0}},
                               {.doc = 9, .positions = {3, 6, 8}}},
                              /*positions=*/true, /*scoring=*/false);
    PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(cursor.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    PositionsBuffer buffer;
    BlockPositions view;
    const std::vector<uint32_t> ordinals = {0, 2};
    ASSERT_TRUE(cursor.block_positions(ordinals, &buffer, &view).ok());
    EXPECT_FALSE(view.by_ordinal);
    EXPECT_EQ(std::vector<uint32_t>(view.flat.begin(), view.flat.end()),
              (std::vector<uint32_t> {1, 4, 3, 6, 8}));
    EXPECT_EQ(std::vector<uint32_t>(view.offsets.begin(), view.offsets.end()),
              (std::vector<uint32_t> {0, 2, 5}));
}

TEST(CursorChainedPostings, GallopsPastAPrefix) {
    const std::vector<uint32_t> values = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
    const auto below = [](uint32_t bound) { return [bound](uint32_t doc) { return doc < bound; }; };
    EXPECT_EQ(gallop_past(values, 0, below(0)), 0U);
    EXPECT_EQ(gallop_past(values, 0, below(6)), 3U);
    EXPECT_EQ(gallop_past(values, 3, below(6)), 3U);
    EXPECT_EQ(gallop_past(values, 2, below(18)), 9U);
    EXPECT_EQ(gallop_past(values, 0, below(100)), values.size());
    EXPECT_EQ(gallop_past(values, values.size(), below(100)), values.size());
}

} // namespace
} // namespace doris::index_query
