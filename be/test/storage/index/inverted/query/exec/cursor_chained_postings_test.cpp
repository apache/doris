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

#include "storage/index/inverted/query/exec/cursor_chained_postings.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <limits>
#include <numeric>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "storage/index/inverted/query/spi/fake_index_source.h"

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

// The rows a sink is handed, batch by batch, a range as its rows.
class BatchSink final : public DocIdSink {
public:
    Status append_sorted(std::span<const uint32_t> docids) override {
        batches.emplace_back(docids.begin(), docids.end());
        return Status::OK();
    }
    Status append_range(uint32_t first, uint64_t last_exclusive) override {
        std::vector<uint32_t> rows(last_exclusive - first);
        std::iota(rows.begin(), rows.end(), first);
        batches.push_back(std::move(rows));
        return Status::OK();
    }

    std::vector<uint32_t> rows() const {
        std::vector<uint32_t> all;
        for (const auto& batch : batches) {
            all.insert(all.end(), batch.begin(), batch.end());
        }
        return all;
    }

    std::vector<std::vector<uint32_t>> batches;
};

constexpr uint32_t kBatch = 1U << 16;

// A stream hands on at most 2^16 of the cheapest cursor's rows at a time, and another cursor's
// block reaching into the next batch is read once.
TEST(CursorChainedPostings, AStreamHandsOnABatchOfRowsAtATime) {
    BlockedCursor rare({BlockedCursor::dense(0, 40000), BlockedCursor::dense(40000, 70000)});
    BlockedCursor wide({BlockedCursor::listed({5, kBatch - 1, kBatch, 69999, 70001}),
                        BlockedCursor::dense(80000, 200000)});
    const std::vector<PostingsCursor*> cursors = {&wide, &rare};
    BatchSink sink;
    ASSERT_TRUE(stream_cursors(cursors, sink).ok());
    EXPECT_EQ(sink.batches,
              (std::vector<std::vector<uint32_t>> {{5, kBatch - 1}, {kBatch, 69999}}));
    EXPECT_EQ(rare.decoded, 2U);
    EXPECT_EQ(wide.decoded, 1U);
}

// The other cursors of a stream read only the blocks the cheapest cursor's rows reach.
TEST(CursorChainedPostings, AStreamReadsOnlyTheBlocksTheCheapestRowsReach) {
    BlockedCursor rare({BlockedCursor::listed({10, 3 * kBatch + 10})});
    std::vector<BlockedCursor::Block> blocks;
    for (uint32_t block = 0; block < 64; ++block) {
        blocks.push_back(BlockedCursor::dense(block * 4096, block * 4096 + 4000));
    }
    BlockedCursor wide(blocks);
    const std::vector<PostingsCursor*> cursors = {&wide, &rare};
    BatchSink sink;
    ASSERT_TRUE(stream_cursors(cursors, sink).ok());
    EXPECT_EQ(sink.batches, (std::vector<std::vector<uint32_t>> {{10, 3 * kBatch + 10}}));
    EXPECT_EQ(wide.decoded, 2U);
}

// A stream ends once a cursor holds no further row, without reading the cheapest cursor's later
// blocks.
TEST(CursorChainedPostings, AStreamEndsWhenACursorEnds) {
    BlockedCursor rare({BlockedCursor::dense(0, kBatch),
                        BlockedCursor::dense(100000, 100000 + kBatch),
                        BlockedCursor::listed({300000}), BlockedCursor::listed({400000})});
    BlockedCursor wide({BlockedCursor::dense(0, 140000)});
    const std::vector<PostingsCursor*> cursors = {&rare, &wide};
    BatchSink sink;
    ASSERT_TRUE(stream_cursors(cursors, sink).ok());
    ASSERT_EQ(sink.batches.size(), 2U);
    EXPECT_EQ(sink.batches[0].size(), kBatch);
    EXPECT_EQ(sink.batches[1].front(), 100000U);
    EXPECT_EQ(sink.batches[1].back(), 139999U);
    EXPECT_EQ(sink.batches[1].size(), 40000U);
    EXPECT_EQ(rare.decoded, 3U);
}

// Random cursors of dense and listed blocks, some cheapest ones holding several batches: the
// stream lists the rows the chain lists.
TEST(CursorChainedPostings, AStreamListsTheRowsTheChainLists) {
    std::mt19937 random(20261017);
    for (int round = 0; round < 24; ++round) {
        const size_t count = 2 + round % 3;
        std::vector<std::vector<BlockedCursor::Block>> layouts(count);
        std::vector<uint32_t> expected;
        for (size_t c = 0; c < count; ++c) {
            const double rate = std::array {0.002, 0.05, 0.5, 0.97}[(round + c) % 4];
            std::bernoulli_distribution holds(rate);
            std::vector<uint32_t> docs;
            for (uint32_t row = 0; row < 3 * kBatch + 4096; ++row) {
                if (holds(random)) {
                    docs.push_back(row);
                }
            }
            for (size_t begin = 0; begin < docs.size();) {
                const size_t end = std::min(docs.size(), begin + 1 + random() % 600);
                if (docs[end - 1] - docs[begin] == end - begin - 1) {
                    layouts[c].push_back(BlockedCursor::dense(docs[begin], docs[end - 1] + 1));
                } else {
                    layouts[c].push_back(BlockedCursor::listed(
                            std::vector<uint32_t>(docs.begin() + begin, docs.begin() + end)));
                }
                begin = end;
            }
            if (c == 0) {
                expected = docs;
            } else {
                std::vector<uint32_t> both;
                std::ranges::set_intersection(expected, docs, std::back_inserter(both));
                expected.swap(both);
            }
        }
        std::vector<BlockedCursor> streamed;
        std::vector<BlockedCursor> chained;
        streamed.reserve(count);
        chained.reserve(count);
        for (const auto& layout : layouts) {
            streamed.emplace_back(layout);
            chained.emplace_back(layout);
        }
        std::vector<PostingsCursor*> stream_input;
        std::vector<PostingsCursor*> chain_input;
        for (size_t c = 0; c < count; ++c) {
            stream_input.push_back(&streamed[c]);
            chain_input.push_back(&chained[c]);
        }
        BatchSink sink;
        ASSERT_TRUE(stream_cursors(stream_input, sink).ok());
        std::vector<uint32_t> rows;
        ASSERT_TRUE(chain_cursors(chain_input, nullptr, &rows).ok());
        EXPECT_EQ(rows, expected) << round;
        EXPECT_EQ(sink.rows(), expected) << round;
        for (const auto& batch : sink.batches) {
            EXPECT_LE(batch.size(), kBatch) << round;
        }
    }
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

void check_ordinals(const std::vector<uint32_t>& docs, const std::vector<uint32_t>& candidates) {
    std::vector<uint32_t> expected;
    std::vector<uint32_t> matched;
    for (size_t ordinal = 0; ordinal < docs.size(); ++ordinal) {
        if (std::ranges::binary_search(candidates, docs[ordinal])) {
            expected.push_back(static_cast<uint32_t>(ordinal));
            matched.push_back(docs[ordinal]);
        }
    }
    std::vector<uint32_t> actual = {123};
    intersect_block_ordinals(docs, candidates, &actual);
    expected.insert(expected.begin(), 123);
    EXPECT_EQ(actual, expected);
    if (!docs.empty()) {
        BlockedCursor cursor({BlockedCursor::listed(docs)});
        EXPECT_EQ(collected(cursor, candidates), matched);
    }
}

TEST(CursorChainedPostings, OrdinalIntersectionHandlesEverySmallSubset) {
    check_ordinals({}, {});
    for (uint32_t document_mask = 1; document_mask < 128; ++document_mask) {
        std::vector<uint32_t> docs;
        for (uint32_t doc = 0; doc < 7; ++doc) {
            if ((document_mask & (1U << doc)) != 0) {
                docs.push_back(doc);
            }
        }
        for (uint32_t candidate_mask = 0; candidate_mask < 128; ++candidate_mask) {
            std::vector<uint32_t> candidates;
            for (uint32_t doc = docs.front(); doc <= docs.back(); ++doc) {
                if ((candidate_mask & (1U << doc)) != 0) {
                    candidates.push_back(doc);
                }
            }
            check_ordinals(docs, candidates);
        }
    }
}

TEST(CursorChainedPostings, OrdinalIntersectionCoversSparseWideAndBoundedBlocks) {
    for (const uint32_t width : {128U, 16384U, 65536U}) {
        for (const uint32_t base : {0U, std::numeric_limits<uint32_t>::max() - width}) {
            for (const uint32_t doc_step : {1U, 2U, 3U, 31U, 257U}) {
                std::vector<uint32_t> docs;
                for (uint32_t offset = 0; offset < width; offset += doc_step) {
                    docs.push_back(base + offset);
                }
                for (const uint32_t candidate_step : {1U, 2U, 7U, 103U}) {
                    std::vector<uint32_t> candidates;
                    for (uint32_t offset = 0; offset <= docs.back() - base;
                         offset += candidate_step) {
                        candidates.push_back(base + offset);
                    }
                    check_ordinals(docs, candidates);
                }
            }
        }
    }
}

// Random documents and candidates of a block's span, as unrelated terms of several densities meet.
TEST(CursorChainedPostings, IntersectionHandlesRandomDensities) {
    std::mt19937 random(20261016);
    for (const uint32_t span : {256U, 1024U, 16384U}) {
        for (const double doc_rate : {0.05, 0.5, 0.95}) {
            for (const double candidate_rate : {0.05, 0.5, 0.95}) {
                std::bernoulli_distribution holds_doc(doc_rate);
                std::bernoulli_distribution holds_candidate(candidate_rate);
                std::vector<uint32_t> docs;
                std::vector<uint32_t> candidates;
                for (uint32_t doc = 0; doc < span; ++doc) {
                    if (holds_doc(random)) {
                        docs.push_back(doc);
                    }
                    if (holds_candidate(random)) {
                        candidates.push_back(doc);
                    }
                }
                ASSERT_FALSE(docs.empty());
                std::erase_if(candidates, [&](uint32_t doc) {
                    return doc < docs.front() || doc > docs.back();
                });
                check_ordinals(docs, candidates);
            }
        }
    }
}

TEST(CursorChainedPostings, RetainsPartialSparseBlockOrdinals) {
    BlockedCursor cursor({BlockedCursor::listed({1, 3, 8}), BlockedCursor::dense(10, 13),
                          BlockedCursor::listed({20, 21}), BlockedCursor::listed({30, 34, 40})});
    const std::vector<uint32_t> candidates = {3, 8, 10, 12, 20, 21, 31, 34};
    SelectedPostings selected;
    CursorChainedPostings term(cursor, &selected);
    ASSERT_TRUE(term.start(&candidates).ok());
    std::vector<uint32_t> rows;
    ASSERT_TRUE(term.collect(&rows).ok());
    EXPECT_EQ(rows, (std::vector<uint32_t> {3, 8, 10, 12, 20, 21, 34}));
    EXPECT_EQ(selected.ordinals, (std::vector<uint32_t> {1, 2, 1}));
    ASSERT_EQ(selected.blocks.size(), 2U);
    EXPECT_EQ(selected.blocks[0].last_doc, 8U);
    EXPECT_EQ(selected.blocks[0].end, 2U);
    EXPECT_EQ(selected.blocks[1].last_doc, 40U);
    EXPECT_EQ(selected.blocks[1].end, 3U);
}

TEST(CursorChainedPostings, WholePostingRestartClearsRetainedOrdinals) {
    FakePostingsCursor cursor({posting(1), posting(3), posting(8)}, false, false);
    const std::vector<uint32_t> candidates = {3};
    SelectedPostings selected;
    CursorChainedPostings term(cursor, &selected);
    ASSERT_TRUE(term.start(&candidates).ok());
    std::vector<uint32_t> rows;
    ASSERT_TRUE(term.collect(&rows).ok());
    EXPECT_EQ(selected.ordinals, (std::vector<uint32_t> {1}));
    ASSERT_EQ(selected.blocks.size(), 1U);
    EXPECT_EQ(selected.blocks.front().last_doc, 8U);
    EXPECT_EQ(selected.blocks.front().end, 1U);
    ASSERT_TRUE(cursor.rewind().ok());
    ASSERT_TRUE(term.start(nullptr).ok());
    rows.clear();
    ASSERT_TRUE(term.collect(&rows).ok());
    EXPECT_EQ(rows, (std::vector<uint32_t> {1, 3, 8}));
    EXPECT_TRUE(selected.ordinals.empty());
    EXPECT_TRUE(selected.blocks.empty());
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
