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

#include "storage/index/snii/reader/snii_postings_cursor.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::reader {
namespace {

using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;

struct Term {
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

// An index reopened through a metered reader, with the decoders as oracles.
class Fixture {
public:
    // The shared 9000-document corpus of the SNII query tests.
    Status open_standard() {
        SniiSegmentReader written;
        LogicalIndexReader written_index;
        RETURN_IF_ERROR(snii_test::build_reader(&file, &written, &written_index));
        return _reopen(7, "Body");
    }

    // Every posting kind with norms: "wide" is windowed, "mid" slim and "rare" inline.
    Status open_scored() {
        constexpr uint32_t kDocCount = 20000;
        std::vector<PostingDoc> wide;
        for (uint32_t doc = 0; doc < kDocCount; doc += 2) {
            std::vector<uint32_t> positions;
            for (uint32_t p = 0; p <= doc % 3; ++p) {
                positions.push_back(p * 2);
            }
            wide.push_back({doc, std::move(positions)});
        }
        std::vector<PostingDoc> mid;
        uint32_t docid = 0;
        for (uint32_t i = 0; i < 500; ++i) {
            docid += 1 + ((i * 2654435761U) >> 16) % 32;
            mid.push_back({docid, {i % 2 + 1, 7}});
        }
        std::vector<PostingDoc> rare {{.docid = 4444, .positions = {3, 5, 9}}};
        writer::SniiIndexInput input;
        input.index_id = 9;
        input.index_suffix = "body";
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = kDocCount;
        input.encoded_norms.resize(kDocCount);
        for (uint32_t doc = 0; doc < kDocCount; ++doc) {
            input.encoded_norms[doc] = static_cast<uint8_t>(doc % 251 + 1);
        }
        input.terms = {make_term("mid", std::move(mid)), make_term("rare", std::move(rare)),
                       make_term("wide", std::move(wide))};
        writer::SniiCompoundWriter compound_writer(&file);
        RETURN_IF_ERROR(compound_writer.add_logical_index(input));
        RETURN_IF_ERROR(compound_writer.finish());
        return _reopen(9, "body");
    }

    Term lookup(std::string_view name) const {
        Term term;
        bool found = false;
        EXPECT_TRUE(index.lookup(name, &found, &term.entry, &term.frq_base, &term.prx_base).ok());
        EXPECT_TRUE(found) << name;
        return term;
    }

    std::vector<uint32_t> oracle_docids(const Term& term) const {
        std::vector<uint32_t> docids;
        EXPECT_TRUE(query::internal::read_docid_posting(index, term.entry, term.frq_base,
                                                        term.prx_base, &docids)
                            .ok());
        return docids;
    }

    std::vector<std::vector<uint32_t>> oracle_positions(const Term& term) const {
        if (term.entry.kind == format::DictEntryKind::kPodRef &&
            term.entry.enc == format::DictEntryEnc::kWindowed) {
            DecodedPosting posting;
            EXPECT_TRUE(read_windowed_posting(index, term.entry, term.frq_base, term.prx_base,
                                              /*want_positions=*/true, &posting)
                                .ok());
            return posting.positions;
        }
        std::vector<uint8_t> frame_bytes;
        Slice frame(term.entry.prx_bytes);
        if (term.entry.kind == format::DictEntryKind::kPodRef) {
            uint64_t offset = 0;
            uint64_t length = 0;
            EXPECT_TRUE(index.resolve_prx_window(term.entry, term.prx_base, &offset, &length).ok());
            EXPECT_TRUE(index.reader()->read_at(offset, length, &frame_bytes).ok());
            frame = Slice(frame_bytes);
        }
        std::vector<std::vector<uint32_t>> positions;
        ByteSource source(frame);
        EXPECT_TRUE(format::read_prx_window(&source, &positions).ok());
        return positions;
    }

    std::unique_ptr<SniiPostingsCursor> cursor(
            const Term& term, bool positions = false, bool scoring = false,
            const format::NormsPodReader* norms = nullptr) const {
        auto result = std::make_unique<SniiPostingsCursor>(
                index, term.entry, term.frq_base, term.prx_base, positions, scoring, norms);
        EXPECT_TRUE(result->open().ok());
        return result;
    }

    uint64_t rounds() const { return metered.metrics().serial_rounds; }
    uint64_t bytes() const { return metered.metrics().total_request_bytes; }

    MemoryFile file;
    io::MeteredFileReader metered {&file, /*block_size=*/256};
    SniiSegmentReader segment;
    LogicalIndexReader index;

private:
    Status _reopen(uint64_t index_id, const std::string& suffix) {
        RETURN_IF_ERROR(SniiSegmentReader::open(&metered, &segment));
        RETURN_IF_ERROR(segment.open_index(index_id, suffix, &index));
        metered.reset_metrics();
        return Status::OK();
    }
};

std::vector<uint32_t> list_docs(SniiPostingsCursor& cursor) {
    std::vector<uint32_t> docs;
    index_query::BlockDocSet set(cursor);
    while (!set.exhausted()) {
        docs.push_back(set.doc());
        set.advance();
    }
    return docs;
}

std::vector<uint32_t> positions_of(SniiPostingsCursor& cursor, uint32_t ordinal) {
    index_query::PositionCursor* positions = nullptr;
    EXPECT_TRUE(cursor.open_positions(ordinal, &positions).ok());
    std::vector<uint32_t> out;
    EXPECT_TRUE(positions->append_remaining_positions(0, out).ok());
    return out;
}

// The open document's remaining positions, pulled `chunk` at a time.
std::vector<uint32_t> read_in_chunks(index_query::PositionCursor* positions, size_t chunk) {
    std::vector<uint32_t> out;
    std::vector<uint32_t> buffer(chunk);
    size_t count = 0;
    do {
        EXPECT_TRUE(positions->next_positions(buffer, &count).ok());
        out.insert(out.end(), buffer.begin(), buffer.begin() + count);
    } while (count != 0);
    return out;
}

// Compares every block's positions with the decoder's, in listing order.
void expect_positions(const Fixture& fixture, const Term& term, const char* name) {
    const auto expected = fixture.oracle_positions(term);
    auto cursor = fixture.cursor(term, /*positions=*/true);
    index_query::PostingsBlock block;
    bool eof = false;
    size_t doc_index = 0;
    while (true) {
        assert_ok(cursor->next_block(&block, &eof));
        if (eof) {
            break;
        }
        for (uint64_t ordinal = 0; ordinal < block.size(); ++ordinal, ++doc_index) {
            ASSERT_LT(doc_index, expected.size()) << name;
            EXPECT_EQ(positions_of(*cursor, static_cast<uint32_t>(ordinal)), expected[doc_index])
                    << name << " doc " << block.doc_at(ordinal);
        }
    }
    EXPECT_EQ(doc_index, expected.size()) << name;
}

TEST(SniiPostingsCursor, ListsEveryPostingKindLikeTheDecoder) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    for (const char* name : {"123", "needle", "sparse_left", "sparse_right", "failed", "driver",
                             "almost", "repeat", "order", "trace"}) {
        const Term term = fixture.lookup(name);
        auto cursor = fixture.cursor(term);
        EXPECT_EQ(cursor->doc_freq(), term.entry.df) << name;
        EXPECT_EQ(list_docs(*cursor), fixture.oracle_docids(term)) << name;
    }
}

TEST(SniiPostingsCursor, DenseFullWindowsAreDenseBlocks) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term term = fixture.lookup("driver");
    auto cursor = fixture.cursor(term);
    std::vector<uint32_t> docs;
    size_t dense_blocks = 0;
    index_query::PostingsBlock block;
    bool eof = false;
    while (true) {
        assert_ok(cursor->next_block(&block, &eof));
        if (eof) {
            break;
        }
        dense_blocks += block.dense ? 1 : 0;
        for (uint64_t i = 0; i < block.size(); ++i) {
            docs.push_back(block.doc_at(i));
        }
    }
    EXPECT_GT(dense_blocks, 0U);
    EXPECT_EQ(docs, fixture.oracle_docids(term));
}

TEST(SniiPostingsCursor, PositionsMatchTheDecoder) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    for (const char* name : {"needle", "failed", "repeat", "order", "almost"}) {
        expect_positions(fixture, fixture.lookup(name), name);
    }
}

TEST(SniiPostingsCursor, SeeksLikeTheDecodedList) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    for (const char* name : {"sparse_left", "failed", "needle"}) {
        const Term term = fixture.lookup(name);
        const auto expected = fixture.oracle_docids(term);
        auto cursor = fixture.cursor(term);
        index_query::BlockDocSet set(*cursor);
        for (const uint32_t target :
             {0U, 1U, 2U, 3U, 7U, 8U, 101U, 4000U, 4001U, 4002U, 8997U, 8998U, 8999U, 9000U}) {
            const auto it = std::ranges::lower_bound(expected, target);
            const bool found = set.seek(target);
            EXPECT_EQ(found, it != expected.end()) << name << " target " << target;
            if (found) {
                EXPECT_EQ(set.doc(), *it) << name << " target " << target;
            }
        }
    }
}

TEST(SniiPostingsCursor, ShallowSeekPositionsTheBoundWithoutDecoding) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term term = fixture.lookup("failed");
    auto cursor = fixture.cursor(term);
    bool moved = false;
    assert_ok(cursor->shallow_seek(5000, &moved));
    EXPECT_TRUE(moved);
    const auto bound = cursor->current_block_bound();
    EXPECT_TRUE(bound.last_doc_known);
    EXPECT_GE(bound.last_doc, 5000U);
    index_query::PostingsBlock block;
    bool eof = false;
    assert_ok(cursor->next_block(&block, &eof));
    ASSERT_FALSE(eof);
    EXPECT_LE(block.doc_at(0), 5000U);
    EXPECT_EQ(block.doc_at(block.size() - 1), bound.last_doc);
    assert_ok(cursor->shallow_seek(5001, &moved));
    EXPECT_FALSE(moved);
}

TEST(SniiPostingsCursor, CandidatesReadOnlyTheirCoveringWindows) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term term = fixture.lookup("sparse_left");
    fixture.metered.reset_metrics();
    auto whole = fixture.cursor(term);
    // The prelude and the whole dd-block, in one round.
    EXPECT_EQ(fixture.rounds(), 1U);
    const uint64_t whole_bytes = fixture.bytes();

    // Candidates in the first two windows: same-term reads coalesce across a 16 KiB gap, so
    // windows far apart in this small posting would read the whole block anyway.
    const std::vector<uint32_t> candidates = {9, 702, 1500};
    fixture.metered.reset_metrics();
    SniiPostingsCursor restricted(fixture.index, term.entry, term.frq_base, term.prx_base,
                                  /*positions=*/false, /*scoring=*/false, nullptr);
    assert_ok(restricted.prefetch(&candidates, /*positions=*/false));
    // The prelude, then the covering windows in one batch.
    EXPECT_EQ(fixture.rounds(), 2U);
    EXPECT_LT(fixture.bytes(), whole_bytes);
    index_query::BlockDocSet set(restricted);
    for (const uint32_t candidate : candidates) {
        ASSERT_TRUE(set.seek(candidate));
        EXPECT_EQ(set.doc(), candidate);
    }
    EXPECT_EQ(fixture.rounds(), 2U);

    // Windows the batch did not cover are read on demand, so the full listing still holds.
    SniiPostingsCursor again(fixture.index, term.entry, term.frq_base, term.prx_base,
                             /*positions=*/false, /*scoring=*/false, nullptr);
    assert_ok(again.prefetch(&candidates, /*positions=*/false));
    fixture.metered.reset_metrics();
    EXPECT_EQ(list_docs(again), fixture.oracle_docids(term));
    EXPECT_GT(fixture.rounds(), 0U);
}

TEST(SniiPostingsCursor, InlineTermReadsNothing) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term term = fixture.lookup("123");
    ASSERT_EQ(term.entry.kind, format::DictEntryKind::kInline);
    fixture.metered.reset_metrics();
    auto cursor = fixture.cursor(term, /*positions=*/true);
    EXPECT_EQ(fixture.rounds(), 0U);
    index_query::PostingsBlock block;
    bool eof = false;
    assert_ok(cursor->next_block(&block, &eof));
    ASSERT_FALSE(eof);
    EXPECT_EQ(block.size(), 1U);
    EXPECT_EQ(block.doc_at(0), 42U);
    EXPECT_EQ(positions_of(*cursor, 0), (std::vector<uint32_t> {1}));
    EXPECT_EQ(fixture.rounds(), 0U);
    assert_ok(cursor->next_block(&block, &eof));
    EXPECT_TRUE(eof);
}

TEST(SniiPostingsCursor, SlimTermReadsItsRegionsInOneRound) {
    Fixture fixture;
    assert_ok(fixture.open_scored());
    const Term term = fixture.lookup("mid");
    ASSERT_EQ(term.entry.kind, format::DictEntryKind::kPodRef);
    ASSERT_EQ(term.entry.enc, format::DictEntryEnc::kSlim);
    const auto expected_docs = fixture.oracle_docids(term);
    const auto expected_positions = fixture.oracle_positions(term);
    fixture.metered.reset_metrics();
    auto cursor = fixture.cursor(term, /*positions=*/true);
    EXPECT_EQ(fixture.rounds(), 1U);
    index_query::PostingsBlock block;
    bool eof = false;
    assert_ok(cursor->next_block(&block, &eof));
    ASSERT_FALSE(eof);
    ASSERT_EQ(block.size(), expected_docs.size());
    for (uint64_t ordinal = 0; ordinal < block.size(); ++ordinal) {
        EXPECT_EQ(block.doc_at(ordinal), expected_docs[ordinal]);
        EXPECT_EQ(positions_of(*cursor, static_cast<uint32_t>(ordinal)),
                  expected_positions[ordinal]);
    }
    EXPECT_EQ(fixture.rounds(), 1U);
}

TEST(SniiPostingsCursor, FrequenciesAndNormsWhenScoring) {
    Fixture fixture;
    assert_ok(fixture.open_scored());
    ASSERT_TRUE(fixture.index.has_norms());
    format::NormsPodReader norms;
    assert_ok(fixture.index.open_norms(&norms));
    for (const char* name : {"wide", "mid", "rare"}) {
        const Term term = fixture.lookup(name);
        const auto expected = fixture.oracle_positions(term);
        auto cursor = fixture.cursor(term, /*positions=*/true, /*scoring=*/true, &norms);
        index_query::PostingsBlock block;
        bool eof = false;
        size_t doc_index = 0;
        while (true) {
            assert_ok(cursor->next_block(&block, &eof));
            if (eof) {
                break;
            }
            ASSERT_EQ(block.freqs.size(), block.size()) << name;
            ASSERT_EQ(block.norms.size(), block.size()) << name;
            for (uint64_t ordinal = 0; ordinal < block.size(); ++ordinal, ++doc_index) {
                const uint32_t doc = block.doc_at(ordinal);
                EXPECT_EQ(block.freq_at(ordinal), expected[doc_index].size()) << name;
                EXPECT_EQ(block.norm_at(ordinal), norms.encoded_norm(doc)) << name;
                index_query::PositionCursor* positions = nullptr;
                assert_ok(cursor->open_positions(static_cast<uint32_t>(ordinal), &positions));
                ASSERT_TRUE(positions->view().has_value());
                EXPECT_EQ(
                        std::vector<uint32_t>(positions->view()->begin(), positions->view()->end()),
                        expected[doc_index]);
                EXPECT_EQ(positions->frequency(), expected[doc_index].size());
                EXPECT_EQ(read_in_chunks(positions, 3), expected[doc_index]);
                assert_ok(positions->finish_doc());
            }
        }
        EXPECT_EQ(doc_index, expected.size()) << name;
    }
}

// The ordinal of `doc` in the block holding it.
uint32_t ordinal_of(const index_query::PostingsBlock& block, uint32_t doc) {
    if (block.dense) {
        return doc - block.range_begin;
    }
    return static_cast<uint32_t>(std::ranges::lower_bound(block.docs, doc) - block.docs.begin());
}

// A scoring cursor given candidates reads their windows' frames with their docids, since every
// block it decodes needs its frame for the frequencies.
TEST(SniiPostingsCursor, AScoringCursorPrefetchReadsTheFramesWithTheWindows) {
    Fixture fixture;
    assert_ok(fixture.open_scored());
    format::NormsPodReader norms;
    assert_ok(fixture.index.open_norms(&norms));
    const Term term = fixture.lookup("wide");
    const auto docs = fixture.oracle_docids(term);
    const auto expected = fixture.oracle_positions(term);
    const std::vector<size_t> chosen = {10, 11, docs.size() - 5};
    std::vector<uint32_t> candidates;
    for (const size_t index : chosen) {
        candidates.push_back(docs[index]);
    }
    fixture.metered.reset_metrics();
    SniiPostingsCursor cursor(fixture.index, term.entry, term.frq_base, term.prx_base,
                              /*positions=*/false, /*scoring=*/true, &norms);
    assert_ok(cursor.prefetch(&candidates, /*positions=*/false));
    const uint64_t prefetched = fixture.rounds();
    EXPECT_LE(prefetched, 2U);
    index_query::PostingsBlock block;
    bool eof = false;
    for (size_t i = 0; i < chosen.size(); ++i) {
        assert_ok(cursor.seek_block(candidates[i], &block, &eof));
        ASSERT_FALSE(eof);
        const uint32_t ordinal = ordinal_of(block, candidates[i]);
        EXPECT_EQ(block.doc_at(ordinal), candidates[i]);
        EXPECT_EQ(block.freq_at(ordinal), expected[chosen[i]].size());
        EXPECT_EQ(block.norm_at(ordinal), norms.encoded_norm(candidates[i]));
    }
    EXPECT_EQ(fixture.rounds(), prefetched);
}

// Streams the `chosen` documents of the cursor's current block, the odd-numbered of them left
// after their first position, each compared with the decoder's positions of the block's documents
// in `expected`.
void expect_streamed_block(SniiPostingsCursor& cursor, const std::vector<uint32_t>& chosen,
                           std::span<const std::vector<uint32_t>> expected) {
    assert_ok(cursor.stream_positions(chosen));
    for (size_t i = 0; i < chosen.size(); ++i) {
        index_query::PositionCursor* positions = nullptr;
        assert_ok(cursor.open_positions(chosen[i], &positions));
        EXPECT_FALSE(positions->view().has_value());
        const std::vector<uint32_t>& want = expected[chosen[i]];
        EXPECT_EQ(positions->frequency(), want.size());
        if (i % 3 == 0) {
            std::vector<uint32_t> streamed;
            assert_ok(positions->append_remaining_positions(0, streamed));
            EXPECT_EQ(streamed, want);
            continue;
        }
        if (i % 3 == 1) {
            EXPECT_EQ(read_in_chunks(positions, 3), want);
            assert_ok(positions->finish_doc());
            continue;
        }
        uint32_t position = 0;
        bool available = false;
        assert_ok(positions->next_position(&position, &available));
        ASSERT_TRUE(available);
        EXPECT_EQ(position, want.front());
        assert_ok(positions->finish_doc());
    }
}

// Every other document of each block streams the decoder's positions, read whole, in chunks or
// only up to the first, and each block's frame is checked once its last chosen document is
// finished.
TEST(SniiPostingsCursor, StreamedPositionsMatchTheDecoder) {
    Fixture fixture;
    assert_ok(fixture.open_scored());
    const Term term = fixture.lookup("wide");
    const auto expected = fixture.oracle_positions(term);
    format::PrxDecodeStats stats;
    SniiPostingsCursor cursor(fixture.index, term.entry, term.frq_base, term.prx_base,
                              /*positions=*/true, /*scoring=*/false, nullptr, nullptr, &stats);
    index_query::PostingsBlock block;
    bool eof = false;
    size_t doc_index = 0;
    uint64_t blocks = 0;
    while (true) {
        assert_ok(cursor.next_block(&block, &eof));
        if (eof) {
            break;
        }
        std::vector<uint32_t> chosen;
        for (uint32_t ordinal = 0; ordinal < block.size(); ordinal += 2) {
            chosen.push_back(ordinal);
        }
        expect_streamed_block(cursor, chosen, std::span(expected).subspan(doc_index));
        doc_index += block.size();
        ++blocks;
    }
    EXPECT_GT(blocks, 1U);
    EXPECT_EQ(stats.streaming_frames, blocks);
}

// A term holding one to three positions per document reports a light decode for each.
TEST(SniiPostingsCursor, PositionsPerDocumentEstimatesTheDecodeWork) {
    Fixture fixture;
    assert_ok(fixture.open_scored());
    const Term term = fixture.lookup("wide");
    auto cursor = fixture.cursor(term, /*positions=*/true);
    uint64_t per_doc = 0;
    assert_ok(cursor->positions_per_doc(&per_doc));
    EXPECT_GE(per_doc, 1U);
    EXPECT_LT(per_doc, 8U);
}

TEST(SniiPostingsCursor, AGivenPreludeMakesTheSpanOneRound) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term term = fixture.lookup("failed");
    auto prelude = std::make_shared<format::FrqPreludeReader>();
    assert_ok(fetch_windowed_prelude(fixture.index, term.entry, term.frq_base, prelude.get()));
    fixture.metered.reset_metrics();
    SniiPostingsCursor cursor(fixture.index, term.entry, term.frq_base, term.prx_base,
                              /*positions=*/true, /*scoring=*/false, nullptr);
    cursor.set_prelude(prelude);
    assert_ok(cursor.open());
    EXPECT_EQ(fixture.rounds(), 1U);
    EXPECT_EQ(list_docs(cursor), fixture.oracle_docids(term));
}

TEST(SniiPostingsCursor, PrefetchWithoutCandidatesReadsTheSpanOnce) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term term = fixture.lookup("sparse_left");
    fixture.metered.reset_metrics();
    SniiPostingsCursor cursor(fixture.index, term.entry, term.frq_base, term.prx_base,
                              /*positions=*/false, /*scoring=*/false, nullptr);
    assert_ok(cursor.prefetch(nullptr, /*positions=*/false));
    // The prelude and the whole dd-block in one round; the listing reads nothing more.
    EXPECT_EQ(fixture.rounds(), 1U);
    EXPECT_EQ(list_docs(cursor), fixture.oracle_docids(term));
    EXPECT_EQ(fixture.rounds(), 1U);
}

TEST(SniiPostingsCursor, RewindListsAgainWithoutReading) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    for (const char* name : {"sparse_left", "123", "needle"}) {
        const Term term = fixture.lookup(name);
        auto cursor = fixture.cursor(term, /*positions=*/true);
        const auto expected = fixture.oracle_docids(term);
        EXPECT_EQ(list_docs(*cursor), expected) << name;
        assert_ok(cursor->rewind());
        fixture.metered.reset_metrics();
        EXPECT_EQ(list_docs(*cursor), expected) << name;
        EXPECT_EQ(fixture.rounds(), 0U) << name;
    }
}

TEST(SniiPostingsCursor, CursorsOnAWaveShareTheirRounds) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term left = fixture.lookup("sparse_left");
    const Term right = fixture.lookup("sparse_right");
    SniiReadWave wave(fixture.index.reader());
    fixture.metered.reset_metrics();
    SniiPostingsCursor a(fixture.index, left.entry, left.frq_base, left.prx_base,
                         /*positions=*/false, /*scoring=*/false, nullptr, &wave);
    SniiPostingsCursor b(fixture.index, right.entry, right.frq_base, right.prx_base,
                         /*positions=*/false, /*scoring=*/false, nullptr, &wave);
    assert_ok(a.prepare());
    assert_ok(b.prepare());
    EXPECT_EQ(fixture.rounds(), 0U);
    EXPECT_TRUE(wave.pending());
    assert_ok(wave.fetch());
    EXPECT_EQ(fixture.rounds(), 1U);
    EXPECT_EQ(wave.rounds(), 1U);
    EXPECT_NE(a.prelude(), nullptr);
    // Both spans register on the wave and arrive in one round.
    assert_ok(a.prefetch(nullptr, /*positions=*/false));
    assert_ok(b.prefetch(nullptr, /*positions=*/false));
    EXPECT_EQ(fixture.rounds(), 1U);
    assert_ok(wave.fetch());
    EXPECT_EQ(fixture.rounds(), 2U);
    EXPECT_EQ(list_docs(a), fixture.oracle_docids(left));
    EXPECT_EQ(list_docs(b), fixture.oracle_docids(right));
    EXPECT_EQ(fixture.rounds(), 2U);
}

// Prepared cursors listing whole postings read in one round: the windowed term's prelude and
// span, and the slim term's posting, arrive together.
TEST(SniiPostingsCursor, PreparedCursorsListInOneRound) {
    Fixture fixture;
    assert_ok(fixture.open_scored());
    const Term windowed = fixture.lookup("wide");
    const Term slim = fixture.lookup("mid");
    ASSERT_EQ(windowed.entry.enc, format::DictEntryEnc::kWindowed);
    ASSERT_EQ(slim.entry.enc, format::DictEntryEnc::kSlim);
    SniiReadWave wave(fixture.index.reader());
    fixture.metered.reset_metrics();
    SniiPostingsCursor a(fixture.index, windowed.entry, windowed.frq_base, windowed.prx_base,
                         /*positions=*/false, /*scoring=*/false, nullptr, &wave);
    SniiPostingsCursor b(fixture.index, slim.entry, slim.frq_base, slim.prx_base,
                         /*positions=*/false, /*scoring=*/false, nullptr, &wave);
    assert_ok(a.prepare());
    assert_ok(b.prepare());
    assert_ok(a.prefetch(nullptr, /*positions=*/false));
    assert_ok(b.prefetch(nullptr, /*positions=*/false));
    EXPECT_EQ(fixture.rounds(), 0U);
    assert_ok(wave.fetch());
    EXPECT_EQ(fixture.rounds(), 1U);
    EXPECT_EQ(list_docs(a), fixture.oracle_docids(windowed));
    EXPECT_EQ(list_docs(b), fixture.oracle_docids(slim));
    EXPECT_EQ(fixture.rounds(), 1U);
}

TEST(SniiPostingsCursor, AWaveCursorFetchesTheWaveWhenItNeedsTheBytes) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term term = fixture.lookup("failed");
    SniiReadWave wave(fixture.index.reader());
    fixture.metered.reset_metrics();
    SniiPostingsCursor cursor(fixture.index, term.entry, term.frq_base, term.prx_base,
                              /*positions=*/false, /*scoring=*/false, nullptr, &wave);
    assert_ok(cursor.prepare());
    // The prelude and the span, fetched by the cursor in one round when it needs them.
    EXPECT_EQ(list_docs(cursor), fixture.oracle_docids(term));
    EXPECT_EQ(fixture.rounds(), 1U);
    EXPECT_EQ(wave.rounds(), 1U);
}

TEST(SniiPostingsCursor, ChainedCursorsListTheIntersectionLikeTheDecoder) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    const Term rare = fixture.lookup("needle");
    const Term wide = fixture.lookup("failed");
    const auto rare_docs = fixture.oracle_docids(rare);
    const auto wide_docs = fixture.oracle_docids(wide);
    std::vector<uint32_t> expected;
    std::ranges::set_intersection(rare_docs, wide_docs, std::back_inserter(expected));
    auto rare_cursor = fixture.cursor(rare);
    fixture.metered.reset_metrics();
    SniiPostingsCursor wide_cursor(fixture.index, wide.entry, wide.frq_base, wide.prx_base,
                                   /*positions=*/false, /*scoring=*/false, nullptr);
    index_query::CursorChainedPostings rare_term(*rare_cursor);
    index_query::CursorChainedPostings wide_term(wide_cursor);
    const std::vector<index_query::ChainedPostings*> chain = {&wide_term, &rare_term};
    std::vector<uint32_t> docs;
    std::vector<size_t> order;
    assert_ok(index_query::chained_conjunction(chain, nullptr, &docs, &order));
    EXPECT_EQ(docs, expected);
    EXPECT_EQ(order, (std::vector<size_t> {1, 0}));
    // The wide term read its prelude, then the windows holding the rare term's rows unless
    // they are full windows, which need no read.
    EXPECT_GE(fixture.rounds(), 1U);
    EXPECT_LE(fixture.rounds(), 2U);
}

// Every third document of a block decodes alone; the other two thirds and the whole block
// decode the frame, which answers by ordinal; all give the decoder's positions.
TEST(SniiPostingsCursor, BlockPositionsMatchTheDecoderForAnySelection) {
    Fixture fixture;
    assert_ok(fixture.open_standard());
    for (const char* name : {"needle", "failed", "repeat", "order", "almost"}) {
        const Term term = fixture.lookup(name);
        const auto expected = fixture.oracle_positions(term);
        auto cursor = fixture.cursor(term, /*positions=*/true);
        index_query::PostingsBlock block;
        bool eof = false;
        size_t doc_index = 0;
        index_query::PositionsBuffer buffer;
        index_query::BlockPositions view;
        // The positions of the i-th of the `ordinals` the last call chose.
        const auto chosen = [&view](size_t i, const std::vector<uint32_t>& ordinals) {
            const auto positions = view.of(i, ordinals);
            return std::vector<uint32_t>(positions.begin(), positions.end());
        };
        while (true) {
            assert_ok(cursor->next_block(&block, &eof));
            if (eof) {
                break;
            }
            std::vector<uint32_t> sparse;
            std::vector<uint32_t> most;
            for (uint32_t ordinal = 0; ordinal < block.size(); ++ordinal) {
                (ordinal % 3 == 0 ? sparse : most).push_back(ordinal);
            }
            assert_ok(cursor->block_positions(sparse, &buffer, &view));
            for (size_t i = 0; i < sparse.size(); ++i) {
                EXPECT_EQ(chosen(i, sparse), expected[doc_index + sparse[i]]) << name;
            }
            assert_ok(cursor->block_positions(most, &buffer, &view));
            EXPECT_TRUE(view.by_ordinal) << name;
            for (size_t i = 0; i < most.size(); ++i) {
                EXPECT_EQ(chosen(i, most), expected[doc_index + most[i]]) << name;
            }
            std::vector<uint32_t> all(block.size());
            std::iota(all.begin(), all.end(), 0);
            assert_ok(cursor->block_positions(all, &buffer, &view));
            for (size_t i = 0; i < all.size(); ++i) {
                EXPECT_EQ(chosen(i, all), expected[doc_index + i]) << name;
            }
            doc_index += block.size();
        }
        EXPECT_EQ(doc_index, expected.size()) << name;
    }
}

} // namespace
} // namespace doris::snii::reader
