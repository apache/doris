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

#include "storage/index/inverted/query_v2/segment_postings.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "CLucene/index/DocRange.h"
#include "CLucene/index/MultiReader.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"
#include "storage/index/inverted/query_v2/postings/listed_walk.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/inverted/spi/clucene_index_source.h"
#include "storage/index/inverted/spi/clucene_postings_cursor.h"
#include "storage/index/query/fake_index_source.h"
#include "storage/index/query/phrase/phrase_verifier.h"
#include "storage/index/query/phrase/position_stream.h"
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class MockTermDocs : public lucene::index::TermDocs {
public:
    MockTermDocs(std::vector<uint32_t> docs, std::vector<uint32_t> freqs,
                 std::vector<uint32_t> norms, int32_t doc_freq)
            : _docs(std::move(docs)),
              _freqs(std::move(freqs)),
              _norms(std::move(norms)),
              _doc_freq(doc_freq) {}

    void seek(lucene::index::Term* term) override {}
    void seek(lucene::index::TermEnum* termEnum) override {}

    int32_t doc() const override { return 0; }
    int32_t freq() const override { return 0; }
    int32_t norm() const override { return 1; }
    bool next() override { return false; }

    int32_t read(int32_t*, int32_t*, int32_t) override { return 0; }
    int32_t read(int32_t*, int32_t*, int32_t*, int32_t) override { return 0; }

    bool readRange(DocRange* docRange) override { return _fillDocRange(docRange); }
    bool readBlock(DocRange* docRange) override { return _fillDocRange(docRange); }
    bool _fillDocRange(DocRange* docRange) {
        if (_read_done || _docs.empty()) {
            return false;
        }
        docRange->type_ = DocRangeType::kMany;
        docRange->doc_many = &_docs;
        docRange->freq_many = &_freqs;
        docRange->norm_many = &_norms;
        docRange->doc_many_size_ = static_cast<uint32_t>(_docs.size());
        docRange->freq_many_size_ = static_cast<uint32_t>(_freqs.size());
        docRange->norm_many_size_ = static_cast<uint32_t>(_norms.size());
        _read_done = true;
        return true;
    }

    bool skipTo(const int32_t target) override { return false; }
    bool skipToBlock(const int32_t target) override { return false; }

    void close() override {}
    lucene::index::TermPositions* __asTermPositions() override { return nullptr; }
    int32_t docFreq() override { return _doc_freq; }

protected:
    std::vector<uint32_t> _docs;
    std::vector<uint32_t> _freqs;
    std::vector<uint32_t> _norms;
    int32_t _doc_freq;
    bool _read_done = false;
};

class ChunkedTermDocs : public MockTermDocs {
public:
    ChunkedTermDocs()
            : MockTermDocs({1, 3, 8, 10, 21, 30}, {2, 3, 4, 5, 6, 7}, {11, 12, 13, 14, 15, 16}, 6) {
    }

    bool readBlock(DocRange* block) override {
        if (_next == _docs.size()) {
            return false;
        }
        const size_t end = std::min(_next + 2, _docs.size());
        _block_docs.assign(_docs.begin() + _next, _docs.begin() + end);
        _block_freqs.assign(_freqs.begin() + _next, _freqs.begin() + end);
        _block_norms.assign(_norms.begin() + _next, _norms.begin() + end);
        block->type_ = DocRangeType::kMany;
        block->doc_many = &_block_docs;
        block->freq_many = &_block_freqs;
        block->norm_many = &_block_norms;
        block->doc_many_size_ = block->freq_many_size_ = block->norm_many_size_ = end - _next;
        _next = end;
        ++blocks_read;
        return true;
    }

    bool skipToBlock(int32_t target) override {
        const size_t before = _next;
        while (_next < _docs.size() && std::cmp_less(_docs[_next + 1], target)) {
            _next += 2;
        }
        return before != _next;
    }

    size_t blocks_read = 0;

private:
    size_t _next = 0;
    std::vector<uint32_t> _block_docs;
    std::vector<uint32_t> _block_freqs;
    std::vector<uint32_t> _block_norms;
};

class MockTermPositions : public lucene::index::TermPositions {
public:
    MockTermPositions(std::vector<uint32_t> docs, std::vector<uint32_t> freqs,
                      std::vector<uint32_t> norms, std::vector<std::vector<uint32_t>> positions,
                      int32_t doc_freq)
            : _docs(std::move(docs)),
              _freqs(std::move(freqs)),
              _norms(std::move(norms)),
              _doc_freq(doc_freq) {
        for (const auto& doc_pos : positions) {
            uint32_t last_pos = 0;
            for (uint32_t pos : doc_pos) {
                _deltas.push_back(pos - last_pos);
                last_pos = pos;
            }
        }
    }

    void seek(lucene::index::Term* term) override {}
    void seek(lucene::index::TermEnum* termEnum) override {}

    int32_t doc() const override { return 0; }
    int32_t freq() const override { return 0; }
    int32_t norm() const override { return 1; }
    bool next() override { return false; }

    int32_t read(int32_t*, int32_t*, int32_t) override { return 0; }
    int32_t read(int32_t*, int32_t*, int32_t*, int32_t) override { return 0; }

    bool readRange(DocRange* docRange) override { return _fillDocRange(docRange); }
    bool readBlock(DocRange* docRange) override { return _fillDocRange(docRange); }
    bool _fillDocRange(DocRange* docRange) {
        if (_read_done || _docs.empty()) {
            return false;
        }
        docRange->type_ = DocRangeType::kMany;
        docRange->doc_many = &_docs;
        docRange->freq_many = &_freqs;
        docRange->norm_many = &_norms;
        docRange->doc_many_size_ = static_cast<uint32_t>(_docs.size());
        docRange->freq_many_size_ = static_cast<uint32_t>(_freqs.size());
        docRange->norm_many_size_ = static_cast<uint32_t>(_norms.size());
        _read_done = true;
        return true;
    }

    bool skipTo(const int32_t target) override { return false; }
    bool skipToBlock(const int32_t target) override { return false; }

    void close() override {}
    lucene::index::TermPositions* __asTermPositions() override { return this; }
    lucene::index::TermDocs* __asTermDocs() override { return this; }

    int32_t nextPosition() override { return 0; }
    int32_t getPayloadLength() const override { return 0; }
    uint8_t* getPayload(uint8_t*) override { return nullptr; }
    bool isPayloadAvailable() const override { return false; }
    int32_t docFreq() override { return _doc_freq; }

    void addLazySkipProxCount(int32_t count) override { _prox_idx += count; }
    int32_t nextDeltaPosition() override {
        if (_prox_idx < _deltas.size()) {
            return _deltas[_prox_idx++];
        }
        return 0;
    }

private:
    std::vector<uint32_t> _docs;
    std::vector<uint32_t> _freqs;
    std::vector<uint32_t> _norms;
    std::vector<uint32_t> _deltas;
    int32_t _doc_freq;
    size_t _prox_idx = 0;
    bool _read_done = false;
};

class SegmentPostingsTest : public testing::Test {};

class ChunkedPositions final : public MockTermPositions {
public:
    explicit ChunkedPositions(size_t block_size)
            : MockTermPositions({1, 2, 5, 8, 11, 20, 27}, {1, 1, 1, 1, 1, 1, 1},
                                {1, 1, 1, 1, 1, 1, 1},
                                {{10}, {20}, {50}, {80}, {110}, {200}, {270}}, 7),
              _block_size(block_size) {}

    bool readBlock(DocRange* block) override {
        ++reads;
        if (fail_read == reads) {
            _CLTHROWA(CL_ERR_IO, "Injected position-block failure");
        }
        if (_next == _docs.size()) {
            return false;
        }
        const size_t end = std::min(_next + _block_size, _docs.size());
        _block_docs.assign(_docs.begin() + _next, _docs.begin() + end);
        _block_freqs.assign(end - _next, 1);
        _prox_idx = _next;
        block->type_ = DocRangeType::kMany;
        block->doc_many = &_block_docs;
        block->freq_many = &_block_freqs;
        block->doc_many_size_ = block->freq_many_size_ = end - _next;
        _next = end;
        return true;
    }

    bool skipToBlock(int32_t target) override {
        const size_t before = _next;
        while (_next < _docs.size()) {
            const size_t end = std::min(_next + _block_size, _docs.size());
            if (std::cmp_greater_equal(_docs[end - 1], target)) {
                break;
            }
            _next = end;
        }
        return _next != before;
    }

    size_t reads = 0;
    size_t fail_read = 0;

private:
    size_t _block_size;
    size_t _next = 0;
    std::vector<uint32_t> _block_docs;
    std::vector<uint32_t> _block_freqs;
};

class CandidatePhraseSource final : public index_query::IndexSource {
public:
    CandidatePhraseSource() {
        for (size_t i = 0; i < readers.size(); ++i) {
            auto* reader = new ChunkedPositions(i + 2);
            for (auto& position : reader->_deltas) {
                position += i;
            }
            readers[i] = reader;
            _positions[i].reset(reader);
        }
    }

    uint32_t doc_count() const override { return 28; }

    Status open_term(std::string_view term, bool, bool,
                     std::unique_ptr<index_query::PostingsCursor>* out) override {
        DORIS_CHECK(term == "left" || term == "right");
        const size_t index = term == "left" ? 0 : 1;
        *out = std::make_unique<ClucenePostingsCursor>(std::move(_positions[index]));
        return Status::OK();
    }

    Status expand_terms(index_query::TermPattern&, int32_t, std::vector<std::string>*) override {
        return Status::NotSupported("This source only opens exact terms");
    }

    std::array<ChunkedPositions*, 2> readers {};

private:
    std::array<TermPositionsPtr, 2> _positions;
};

TEST_F(SegmentPostingsTest, CandidatePhraseDoesNotDecodeBlocksBeforeItsFirstCandidate) {
    const std::wstring field = L"content";
    const std::vector<TermInfo> terms {{.term = std::string("left"), .position = 0},
                                       {.term = std::string("right"), .position = 1}};
    const auto candidates = roaring::Roaring::bitmapOf(2, 11, 20);
    for (const bool scoring : {false, true}) {
        auto source = std::make_shared<CandidatePhraseSource>();
        auto similarity = scoring ? std::make_shared<BM25Similarity>(2.0F, 8.0F) : nullptr;
        PhraseWeight weight(field, terms, {.candidates = &candidates}, similarity, scoring, false);
        QueryExecutionContext context;
        context.segment_num_rows = source->doc_count();
        context.field_sources.emplace(field, source);
        auto scorer = weight.scorer(context, "");

        ASSERT_EQ(scorer->doc(), 11);
        for (const auto* reader : source->readers) {
            EXPECT_EQ(reader->reads, 1U);
        }
        const float expected_score = scoring ? similarity->score(1.0F, 1) : 1.0F;
        EXPECT_FLOAT_EQ(scorer->score(), expected_score);
        ASSERT_EQ(scorer->advance(), 20);
        EXPECT_FLOAT_EQ(scorer->score(), expected_score);
        EXPECT_EQ(scorer->advance(), TERMINATED);
    }
}

class DenseTermPositions final : public MockTermPositions {
public:
    explicit DenseTermPositions(uint32_t first)
            : MockTermPositions({first, first + 1, first + 2}, {1, 2, 1}, {3, 4, 5},
                                {{5}, {8, 13}, {21}}, 3) {}

    bool readBlock(DocRange* block) override {
        const bool found = MockTermPositions::readBlock(block);
        if (found) {
            block->type_ = DocRangeType::kRange;
            block->doc_range = {_docs.front(), _docs.back() + 1};
        }
        return found;
    }
};

void check_dense_block_view(const index_query::PostingsBlock& block, uint32_t first) {
    EXPECT_TRUE(block.dense);
    EXPECT_EQ(block.range_begin, first);
    EXPECT_EQ(block.range_end, static_cast<uint64_t>(first) + 3);
    ASSERT_EQ(block.size(), 3U);
    const auto suffix = block.suffix(1);
    ASSERT_EQ(suffix.size(), 2U);
    EXPECT_EQ(suffix.doc_at(0), first + 1);
    EXPECT_EQ(suffix.doc_at(1), first + 2);
    EXPECT_EQ(suffix.freq_at(0), 2U);
    EXPECT_EQ(suffix.norm_at(0), 4U);
}

void check_dense_block(uint32_t first) {
    ClucenePostingsCursor cursor {TermPositionsPtr(new DenseTermPositions(first))};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(cursor.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    check_dense_block_view(block, first);
    std::vector<uint32_t> positions;
    ASSERT_TRUE(cursor.append_positions(1, 0, positions).ok());
    EXPECT_EQ(positions, (std::vector<uint32_t> {8, 13}));
    ASSERT_TRUE(cursor.next_block(&block, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(block.size(), 0U);
}

TEST_F(SegmentPostingsTest, CommonCursorPreservesDenseRangesAndPositionOrdinals) {
    check_dense_block(0);
    check_dense_block(123);
    check_dense_block(std::numeric_limits<uint32_t>::max() - 2);
}

void check_prepared_position_ranges(size_t width, const std::vector<size_t>& ends) {
    const std::vector<uint32_t> rows {2, 5, 11, 20, 27};
    auto* reader = new ChunkedPositions(width);
    ClucenePostingsCursor source {TermPositionsPtr(reader)};
    TermWalk walk(source, rows);
    size_t previous_end = 0;
    size_t expected_reads = 0;
    for (size_t row = 0; row < rows.size(); ++row) {
        expected_reads += row >= previous_end;
        ASSERT_TRUE(walk.prepare(row, rows[row]).ok());
        ASSERT_EQ(walk.end(), ends[row]);
        previous_end = walk.end();
        for (size_t prepared = row; prepared < walk.end(); ++prepared) {
            const auto positions = walk.positions(prepared);
            ASSERT_EQ(positions.second - positions.first, 1);
            EXPECT_EQ(*positions.first, rows[prepared] * 10);
        }
        EXPECT_EQ(reader->reads, expected_reads);
    }
}

TEST_F(SegmentPostingsTest, PreparedPositionRangesFollowSelectedBlockRows) {
    check_prepared_position_ranges(2, {1, 2, 4, 4, 5});
    check_prepared_position_ranges(3, {2, 2, 4, 4, 5});
}

TEST_F(SegmentPostingsTest, PositionPreparationPropagatesBlockErrors) {
    auto* reader = new ChunkedPositions(2);
    reader->fail_read = 2;
    ClucenePostingsCursor source {TermPositionsPtr(reader)};
    const std::vector<uint32_t> rows {2, 5};
    TermWalk walk(source, rows);
    ASSERT_TRUE(walk.prepare(0, rows[0]).ok());
    EXPECT_EQ(walk.prepare(1, rows[1]).code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

void check_streamed_block(const StreamWalk& walk, const ChunkedPositions& reader, size_t end,
                          size_t reads) {
    EXPECT_EQ(walk.end(), end);
    EXPECT_EQ(reader.reads, reads);
}

TEST_F(SegmentPostingsTest, StreamedPhrasePreservesRowsAcrossDifferentBlockPartitions) {
    const std::vector<uint32_t> rows {2, 5, 11, 20, 27};
    auto* left_reader = new ChunkedPositions(2);
    auto* right_reader = new ChunkedPositions(3);
    for (auto& position : right_reader->_deltas) {
        ++position;
    }
    ClucenePostingsCursor left {TermPositionsPtr(left_reader)};
    ClucenePostingsCursor right {TermPositionsPtr(right_reader)};
    std::array<StreamWalk, 2> walks {StreamWalk(left, rows), StreamWalk(right, rows)};
    const std::array<size_t, 2> plan {0, 1};
    const std::array<uint32_t, 2> offsets {0, 1};
    const std::array<uint64_t, 2> costs {1, 1};
    const index_query::PhraseVerifier verifier(std::vector<size_t>(plan.begin(), plan.end()),
                                               offsets, costs, 0, false);
    verifier.validate_stream(walks.size());
    const std::array<size_t, 5> left_reads {1, 2, 3, 3, 4};
    const std::array<size_t, 5> right_reads {1, 1, 2, 2, 3};
    const std::array<size_t, 5> left_ends {1, 2, 4, 4, 5};
    const std::array<size_t, 5> right_ends {2, 2, 4, 4, 5};
    for (size_t row = 0; row < rows.size(); ++row) {
        ASSERT_TRUE(walks[0].prepare().ok());
        ASSERT_TRUE(walks[1].prepare().ok());
        bool matched = false;
        ASSERT_TRUE(
                verifier.verify_stream_document(std::span<StreamWalk>(walks), rows[row], &matched)
                        .ok());
        EXPECT_TRUE(matched);
        check_streamed_block(walks[0], *left_reader, left_ends[row], left_reads[row]);
        check_streamed_block(walks[1], *right_reader, right_ends[row], right_reads[row]);
    }
}

TEST_F(SegmentPostingsTest, StreamedPhrasePropagatesBlockReadErrors) {
    auto* reader = new ChunkedPositions(2);
    reader->fail_read = 2;
    ClucenePostingsCursor source {TermPositionsPtr(reader)};
    const std::vector<uint32_t> rows {2, 5};
    StreamWalk walk(source, rows);
    ASSERT_TRUE(walk.prepare().ok());
    ASSERT_TRUE(walk.seek(rows[0]).ok());
    ASSERT_TRUE(walk.finish_doc().ok());
    EXPECT_EQ(walk.prepare().code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

TEST_F(SegmentPostingsTest, PositionBlockRejectsMissingFrequencies) {
    class MissingFrequencies final : public MockTermPositions {
    public:
        MissingFrequencies() : MockTermPositions({1}, {1}, {1}, {{5}}, 1) {}
        bool readBlock(DocRange* block) override {
            const bool found = MockTermPositions::readBlock(block);
            block->freq_many = nullptr;
            return found;
        }
    };
    ClucenePostingsCursor cursor {TermPositionsPtr(new MissingFrequencies())};
    index_query::PostingsBlock block;
    bool eof = false;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(cursor.next_block(&block, &eof)), "Check failed");
}

TEST_F(SegmentPostingsTest, PositionBlockRejectsIncompleteFrequencies) {
    ClucenePostingsCursor cursor {
            TermPositionsPtr(new MockTermPositions({1, 3}, {1}, {1, 1}, {{5}, {8}}, 2))};
    index_query::PostingsBlock block;
    bool eof = false;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(cursor.next_block(&block, &eof)), "Check failed");
}

TEST_F(SegmentPostingsTest, CommonCursorReturnsCLuceneReadFailures) {
    class FailingTermDocs final : public MockTermDocs {
    public:
        FailingTermDocs() : MockTermDocs({1}, {2}, {3}, 1) {}
        bool readBlock(DocRange* block) override {
            if (_read_done) {
                _CLTHROWA(CL_ERR_IO, "Injected CLucene postings read failure");
            }
            return _fillDocRange(block);
        }
    };
    ClucenePostingsCursor cursor {TermDocsPtr(new FailingTermDocs())};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(cursor.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    ASSERT_EQ(block.docs.size(), 1);
    EXPECT_EQ(block.docs[0], 1);
    const auto status = cursor.next_block(&block, &eof);
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_TRUE(block.docs.empty());
}

TEST_F(SegmentPostingsTest, CommonCursorReturnsCLuceneSeekFailures) {
    class FailingTermDocs final : public MockTermDocs {
    public:
        FailingTermDocs() : MockTermDocs({1}, {2}, {3}, 1) {}
        bool skipToBlock(int32_t /*target*/) override {
            _CLTHROWA(CL_ERR_IO, "Injected CLucene postings seek failure");
        }
    };
    ClucenePostingsCursor cursor {TermDocsPtr(new FailingTermDocs())};
    bool moved = false;
    const auto status = cursor.shallow_seek(1, &moved);
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_NE(status.to_string().find("Injected CLucene postings seek failure"), std::string::npos);
}

TEST_F(SegmentPostingsTest, CommonCursorPropagatesNonCLuceneFailures) {
    class FailingTermDocs final : public MockTermDocs {
    public:
        FailingTermDocs() : MockTermDocs({1}, {2}, {3}, 1) {}
        bool readBlock(DocRange* /*block*/) override {
            throw std::runtime_error("Injected non-CLucene failure");
        }
    };
    ClucenePostingsCursor cursor {TermDocsPtr(new FailingTermDocs())};
    index_query::PostingsBlock block;
    bool eof = false;
    EXPECT_THROW(static_cast<void>(cursor.next_block(&block, &eof)), std::runtime_error);
}

TEST_F(SegmentPostingsTest, PositionReadFailuresBecomeDorisErrors) {
    class FailingPositions final : public MockTermPositions {
    public:
        FailingPositions() : MockTermPositions({1}, {2}, {1}, {{3, 7}}, 1) {}
        int32_t nextDeltaPosition() override {
            _CLTHROWA(CL_ERR_IO, "Injected CLucene position read failure");
        }
    };
    SegmentPostings postings(
            std::make_unique<ClucenePostingsCursor>(TermPositionsPtr(new FailingPositions())),
            false, nullptr);
    std::vector<uint32_t> output;
    try {
        postings.append_positions_with_offset(0, output);
        FAIL() << "Expected a Doris exception for the position read failure";
    } catch (const Exception& error) {
        EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    }
    EXPECT_TRUE(output.empty());
}

TEST_F(SegmentPostingsTest, MaterializedPhrasePreservesPositionReadErrors) {
    class FailingPositions final : public MockTermPositions {
    public:
        FailingPositions() : MockTermPositions({1}, {1}, {1}, {{3}}, 1) {}
        int32_t nextDeltaPosition() override {
            _CLTHROWA(CL_ERR_IO, "Injected phrase position read failure");
        }
    };
    auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);
    for (uint32_t slop : {0, 1}) {
        for (uint32_t clauses : {2, 3}) {
            SCOPED_TRACE(::testing::Message() << "slop=" << slop << ", clauses=" << clauses);
            std::vector<std::pair<size_t, SegmentPostingsPtr>> terms;
            for (uint32_t clause = 0; clause < clauses; ++clause) {
                TermPositionsPtr positions;
                if (clause + 1 == clauses) {
                    positions.reset(new FailingPositions());
                } else {
                    positions.reset(new MockTermPositions({1}, {1}, {1}, {{clause + 1}}, 1));
                }
                terms.emplace_back(clause,
                                   make_segment_postings(std::make_unique<ClucenePostingsCursor>(
                                                                 std::move(positions)),
                                                         true, nullptr));
            }
            try {
                PhraseScorer<SegmentPostingsPtr>::create(terms, similarity, {.slop = slop}, 2);
                FAIL() << "Expected the phrase position read error";
            } catch (const Exception& error) {
                EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
                EXPECT_NE(std::string(error.what()).find("Injected phrase position read failure"),
                          std::string::npos);
            }
        }
    }
}

TEST_F(SegmentPostingsTest, StreamedPhrasePreservesUnreadClauseErrorsAfterPositionOverflow) {
    class FailingPositions final : public MockTermPositions {
    public:
        FailingPositions() : MockTermPositions({1}, {1}, {1}, {{7}}, 1) {}
        int32_t nextDeltaPosition() override {
            _CLTHROWA(CL_ERR_IO, "Injected unread phrase clause failure");
        }
    };
    for (uint32_t clauses : {2, 3}) {
        SCOPED_TRACE(clauses);
        std::vector<std::pair<size_t, SegmentPostingsPtr>> terms;
        for (uint32_t clause = 0; clause < clauses; ++clause) {
            TermPositionsPtr positions;
            if (clause + 1 == clauses) {
                positions.reset(new FailingPositions());
            } else {
                const uint32_t position = clause == 0 ? std::numeric_limits<uint32_t>::max() : 7;
                positions.reset(new MockTermPositions({1}, {1}, {1}, {{position}}, 1));
            }
            terms.emplace_back(
                    clause, make_segment_postings(
                                    std::make_unique<ClucenePostingsCursor>(std::move(positions)),
                                    false, nullptr));
        }
        try {
            PhraseScorer<SegmentPostingsPtr>::create(terms, nullptr, {}, 2);
            FAIL() << "Expected the unread phrase clause error";
        } catch (const Exception& error) {
            EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
            EXPECT_NE(std::string(error.what()).find("Injected unread phrase clause failure"),
                      std::string::npos);
        }
    }
}

TEST_F(SegmentPostingsTest, StreamedPhraseMatchesWithAndWithoutFrequencyMetadata) {
    using Cursor = index_query::testing::FakePostingsCursor;
    const std::vector<Cursor::Posting> left {
            {.doc = 1, .positions = {3}},
            {.doc = 2, .positions = {0, 10}},
            {.doc = 3, .positions = {4}},
            {.doc = 4, .positions = {std::numeric_limits<uint32_t>::max()}}};
    const std::vector<Cursor::Posting> right {{.doc = 1, .positions = {4}},
                                              {.doc = 2, .positions = {5, 11}},
                                              {.doc = 3, .positions = {6}},
                                              {.doc = 4, .positions = {0}}};
    for (bool frequencies : {false, true}) {
        SCOPED_TRACE(frequencies);
        std::vector<std::pair<size_t, SegmentPostingsPtr>> terms;
        terms.emplace_back(
                0, make_segment_postings(std::make_unique<Cursor>(left, true, frequencies), false,
                                         nullptr));
        terms.emplace_back(
                1, make_segment_postings(std::make_unique<Cursor>(right, true, frequencies), false,
                                         nullptr));
        auto scorer = PhraseScorer<SegmentPostingsPtr>::create(terms, nullptr, {}, 5);
        std::vector<uint32_t> matched;
        for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
            matched.push_back(doc);
        }
        EXPECT_EQ(matched, (std::vector<uint32_t> {1, 2}));
    }
}

void expect_initial_positions(index_query::PositionCursor& positions) {
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(positions.next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 10);
    ASSERT_TRUE(positions.finish_doc().ok());
    ASSERT_TRUE(positions.finish_doc().ok());
}

void expect_remaining_positions(index_query::PositionCursor& positions) {
    std::vector<uint32_t> output {999};
    ASSERT_TRUE(positions.append_remaining_positions(100, output).ok());
    EXPECT_EQ(output, (std::vector<uint32_t> {999, 140, 141}));
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(positions.next_position(&position, &available).ok());
    EXPECT_FALSE(available);
}

#ifndef NDEBUG
template <typename Operation>
void expect_position_contract_failure(Operation&& operation) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(operation()), "Check failed");
}
#endif

TEST_F(SegmentPostingsTest, CommonPositionCursorStreamsAndSkipsUnselectedDocuments) {
    class CountingPositions final : public MockTermPositions {
    public:
        CountingPositions()
                : MockTermPositions({1, 3, 5}, {3, 2, 2}, {1, 1, 1},
                                    {{10, 20, 30}, {5, 8}, {40, 41}}, 3) {}
        int32_t nextDeltaPosition() override {
            ++reads;
            return MockTermPositions::nextDeltaPosition();
        }
        void addLazySkipProxCount(int32_t count) override {
            skipped += count;
            MockTermPositions::addLazySkipProxCount(count);
        }
        size_t reads = 0;
        size_t skipped = 0;
    };
    auto* reader = new CountingPositions();
    ClucenePostingsCursor source {TermPositionsPtr(reader)};
    index_query::PostingsCursor& postings = source;
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(postings.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(postings.open_positions(0, &positions).ok());
    ASSERT_NE(positions, nullptr);
    EXPECT_EQ(reader->reads, 0);
    expect_initial_positions(*positions);
    EXPECT_EQ(reader->reads, 1);
    EXPECT_EQ(reader->skipped, 2);
    ASSERT_TRUE(postings.open_positions(2, &positions).ok());
    EXPECT_EQ(reader->reads, 1);
    EXPECT_EQ(reader->skipped, 4);
    expect_remaining_positions(*positions);
    EXPECT_EQ(reader->reads, 3);
}

void check_open_position_chunk_doc(index_query::PostingsCursor& source, uint32_t ordinal,
                                   std::span<uint32_t> buffer,
                                   const std::vector<uint32_t>& expected, const size_t& reads) {
    const size_t before = reads;
    index_query::PositionCursor* positions = nullptr;
    size_t count = 0;
    ASSERT_TRUE(source.open_position_stream(ordinal, buffer, &count, &positions).ok());
    ASSERT_EQ(count, std::min(buffer.size(), expected.size()));
    if (count < expected.size()) {
        ASSERT_NE(positions, nullptr);
    }
    EXPECT_EQ(reads, before + count);
    std::vector<uint32_t> all(buffer.begin(), buffer.begin() + count);
    if (positions != nullptr) {
        ASSERT_TRUE(positions->append_remaining_positions(0, all).ok());
        ASSERT_TRUE(positions->finish_doc().ok());
    }
    EXPECT_EQ(all, expected);
}

void check_open_position_chunk(index_query::PostingsCursor& source, size_t capacity,
                               const size_t& reads) {
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    std::vector<uint32_t> buffer(capacity);
    check_open_position_chunk_doc(source, 0, buffer, {10, 20, 30}, reads);
    check_open_position_chunk_doc(source, 2, buffer, {40, 41}, reads);
}

TEST_F(SegmentPostingsTest, FusedPositionOpenReadsOnlyTheRequestedChunk) {
    class CountingPositions final : public MockTermPositions {
    public:
        CountingPositions()
                : MockTermPositions({1, 3, 5}, {3, 1, 2}, {1, 1, 1}, {{10, 20, 30}, {7}, {40, 41}},
                                    3) {}
        int32_t nextDeltaPosition() override {
            ++reads;
            return MockTermPositions::nextDeltaPosition();
        }
        size_t reads = 0;
    };
    for (const size_t capacity : {0, 1, 2, 4}) {
        auto* reader = new CountingPositions();
        ClucenePostingsCursor clucene {TermPositionsPtr(reader)};
        check_open_position_chunk(clucene, capacity, reader->reads);
        index_query::testing::FakePostingsCursor generic({{.doc = 1, .positions = {10, 20, 30}},
                                                          {.doc = 3, .positions = {7}},
                                                          {.doc = 5, .positions = {40, 41}}},
                                                         true, false);
        check_open_position_chunk(generic, capacity, generic.positions_read);
    }
}

void check_completed_first_chunk(size_t capacity) {
    const std::vector<uint32_t> expected = {3, 5, 7};
    ClucenePostingsCursor source {
            TermPositionsPtr(new MockTermPositions({1}, {3}, {1}, {expected}, 1))};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    std::vector<uint32_t> buffer(capacity);
    size_t count = 0;
    index_query::PositionCursor* positions = nullptr;

    ASSERT_TRUE(source.open_position_stream(0, buffer, &count, &positions).ok());

    ASSERT_EQ(count, std::min(capacity, expected.size()));
    EXPECT_EQ(positions == nullptr, count == expected.size());
    std::vector<uint32_t> all(buffer.begin(), buffer.begin() + count);
    if (positions != nullptr) {
        ASSERT_TRUE(positions->append_remaining_positions(0, all).ok());
        ASSERT_TRUE(positions->finish_doc().ok());
    }
    EXPECT_EQ(all, expected);
}

TEST_F(SegmentPostingsTest, CompletedFirstChunkHasNoResidualCursor) {
    for (const size_t capacity : {0, 1, 4}) {
        SCOPED_TRACE(capacity);
        check_completed_first_chunk(capacity);
    }
}

TEST_F(SegmentPostingsTest, PositionStreamExposesCompletedPrefix) {
    ClucenePostingsCursor source {TermPositionsPtr(new MockTermPositions({1}, {1}, {1}, {{7}}, 1))};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    index_query::PositionStream stream;
    ASSERT_TRUE(stream.reset(source, 0).ok());
    index_query::PhrasePositionSpan positions;

    ASSERT_TRUE(stream.whole(&positions));
    ASSERT_EQ(positions.second - positions.first, 1);
    EXPECT_EQ(*positions.first, 7U);
    ASSERT_TRUE(stream.advance_to(7).ok());
    EXPECT_TRUE(stream.available());
    EXPECT_EQ(stream.position(), 7U);
    ASSERT_TRUE(stream.advance_to(8).ok());
    EXPECT_FALSE(stream.available());
    ASSERT_TRUE(stream.whole(&positions));
    EXPECT_EQ(positions.first, positions.second);
    ASSERT_TRUE(stream.finish_doc().ok());
}

TEST_F(SegmentPostingsTest, FusedPositionOpenConvertsReadErrorsAndPreservesRemainingPositions) {
    class FailOncePositions final : public MockTermPositions {
    public:
        FailOncePositions() : MockTermPositions({1}, {3}, {1}, {{3, 5, 7}}, 1) {}
        int32_t nextDeltaPosition() override {
            if (_reads++ == 1) {
                _CLTHROWA(CL_ERR_IO, "injected first-chunk failure");
            }
            return MockTermPositions::nextDeltaPosition();
        }

    private:
        size_t _reads = 0;
    };
    ClucenePostingsCursor source {TermPositionsPtr(new FailOncePositions())};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    std::vector<uint32_t> buffer(4);
    size_t count = 0;
    index_query::PositionCursor* positions = nullptr;
    EXPECT_EQ(source.open_position_stream(0, buffer, &count, &positions).code(),
              ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    ASSERT_EQ(count, 1);
    EXPECT_EQ(buffer.front(), 3);
    std::vector<uint32_t> remaining;
    ASSERT_TRUE(source.append_remaining_positions(0, remaining).ok());
    EXPECT_EQ(remaining, (std::vector<uint32_t> {5, 7}));
}

void expect_advanced_position(index_query::PositionCursor& positions, uint32_t target,
                              uint32_t expected) {
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(positions.next_position_at_least(target, &position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, expected);
}

void check_position_advancement(index_query::PostingsCursor& source) {
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(source.open_positions(0, &positions).ok());
    expect_advanced_position(*positions, 2, 2);
    expect_advanced_position(*positions, 2, 2);
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(positions->next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 7);
    expect_advanced_position(*positions, 8, 9);
    ASSERT_TRUE(positions->next_position_at_least(10, &position, &available).ok());
    EXPECT_FALSE(available);
    ASSERT_TRUE(positions->finish_doc().ok());
    ASSERT_TRUE(source.open_positions(1, &positions).ok());
    expect_advanced_position(*positions, 4, 5);
}

TEST_F(SegmentPostingsTest, PositionAdvanceConsumesOnlyThroughItsTarget) {
    ClucenePostingsCursor clucene {TermPositionsPtr(
            new MockTermPositions({1, 2}, {5, 1}, {1, 1}, {{0, 2, 2, 7, 9}, {5}}, 2))};
    check_position_advancement(clucene);
    using FakeCursor = index_query::testing::FakePostingsCursor;
    FakeCursor fake({{.doc = 1, .positions = {0, 2, 2, 7, 9}}, {.doc = 2, .positions = {5}}}, true,
                    true);
    check_position_advancement(fake);
}

TEST_F(SegmentPostingsTest, PositionAdvanceRetainsProgressAfterAReadError) {
    class FailOncePositions final : public MockTermPositions {
    public:
        FailOncePositions() : MockTermPositions({1}, {4}, {1}, {{3, 5, 7, 9}}, 1) {}
        int32_t nextDeltaPosition() override {
            if (++reads == 2) {
                _CLTHROWA(CL_ERR_IO, "Injected failure after a skipped position");
            }
            return MockTermPositions::nextDeltaPosition();
        }
        size_t reads = 0;
    };
    ClucenePostingsCursor source {TermPositionsPtr(new FailOncePositions())};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(source.open_positions(0, &positions).ok());
    uint32_t position = 123;
    bool available = true;
    EXPECT_EQ(positions->next_position_at_least(7, &position, &available).code(),
              ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_FALSE(available);
    EXPECT_EQ(position, 123);
    ASSERT_TRUE(positions->next_position_at_least(7, &position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 7);
    std::vector<uint32_t> remaining;
    ASSERT_TRUE(positions->append_remaining_positions(0, remaining).ok());
    EXPECT_EQ(remaining, (std::vector<uint32_t> {9}));
}

TEST_F(SegmentPostingsTest, BulkPositionFailureKeepsOnlySuccessfullyDecodedPositions) {
    class FailOncePositions final : public MockTermPositions {
    public:
        FailOncePositions() : MockTermPositions({1}, {4}, {1}, {{3, 5, 7, 9}}, 1) {}
        int32_t nextDeltaPosition() override {
            if (++reads == 3) {
                _CLTHROWA(CL_ERR_IO, "Injected failure during a bulk position read");
            }
            return MockTermPositions::nextDeltaPosition();
        }
        size_t reads = 0;
    };
    ClucenePostingsCursor source {TermPositionsPtr(new FailOncePositions())};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    std::vector<uint32_t> positions {999};
    EXPECT_EQ(source.append_positions(0, 10, positions).code(),
              ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_EQ(positions, (std::vector<uint32_t> {999, 13, 15}));
    ASSERT_TRUE(source.append_remaining_positions(10, positions).ok());
    EXPECT_EQ(positions, (std::vector<uint32_t> {999, 13, 15, 17, 19}));
}

TEST_F(SegmentPostingsTest, CommonPositionCursorRejectsReplayAndInvalidatedBlocks) {
#ifdef NDEBUG
    GTEST_SKIP() << "Cursor precondition assertions require a debug build";
#else
    ClucenePostingsCursor postings {
            TermPositionsPtr(new MockTermPositions({1, 3}, {2, 1}, {1, 1}, {{5, 8}, {13}}, 2))};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(postings.next_block(&block, &eof).ok());
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(postings.open_positions(0, &positions).ok());
    ASSERT_TRUE(positions->finish_doc().ok());
    expect_position_contract_failure([&] { return postings.open_positions(0, &positions); });
    ASSERT_TRUE(postings.open_positions(1, &positions).ok());
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(positions->next_position(&position, &available).ok());
    EXPECT_TRUE(available);
    EXPECT_EQ(position, 13);
    ASSERT_TRUE(postings.next_block(&block, &eof).ok());
    EXPECT_TRUE(eof);
    expect_position_contract_failure(
            [&] { return positions->next_position(&position, &available); });
    expect_position_contract_failure([&] { return postings.open_positions(0, &positions); });
#endif
}

TEST_F(SegmentPostingsTest, FrequencyPaddingDoesNotExtendDocumentOrdinals) {
    ClucenePostingsCursor postings {TermPositionsPtr(
            new MockTermPositions({1, 3}, {2, 1, 999}, {1, 1}, {{5, 8}, {13}}, 2))};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(postings.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    ASSERT_EQ(block.docs.size(), 2);
    ASSERT_EQ(block.freqs.size(), 3);
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(postings.open_positions(1, &positions).ok());
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(positions->next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 13);
    ASSERT_TRUE(positions->finish_doc().ok());
#ifndef NDEBUG
    expect_position_contract_failure([&] { return postings.open_positions(2, &positions); });
#endif
}

TEST_F(SegmentPostingsTest, BulkOpenFailurePreservesTheRemainingPositionState) {
    class FailOncePositions final : public MockTermPositions {
    public:
        FailOncePositions()
                : MockTermPositions({1, 3, 5}, {3, 2, 2}, {1, 1, 1},
                                    {{10, 20, 30}, {5, 8}, {40, 41}}, 3) {}
        void addLazySkipProxCount(int32_t count) override {
            if (++skip_calls == 2) {
                _CLTHROWA(CL_ERR_IO, "Injected failure while skipping an unselected document");
            }
            skipped += count;
            MockTermPositions::addLazySkipProxCount(count);
        }
        size_t skip_calls = 0;
        size_t skipped = 0;
    };
    auto* reader = new FailOncePositions();
    ClucenePostingsCursor source {TermPositionsPtr(reader)};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(source.open_positions(0, &positions).ok());
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(positions->next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 10);
    std::vector<uint32_t> output {999};
    EXPECT_EQ(source.open_positions(2, &positions).code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_EQ(output, (std::vector<uint32_t> {999}));
    EXPECT_EQ(reader->skipped, 2);
    ASSERT_TRUE(source.open_positions(2, &positions).ok());
    ASSERT_TRUE(positions->append_remaining_positions(100, output).ok());
    EXPECT_EQ(output, (std::vector<uint32_t> {999, 140, 141}));
    EXPECT_EQ(reader->skipped, 4);
}

TEST_F(SegmentPostingsTest, StreamingReadFailureDoesNotConsumeAPosition) {
    class FailOncePositions final : public MockTermPositions {
    public:
        FailOncePositions() : MockTermPositions({1}, {2}, {1}, {{3, 7}}, 1) {}
        int32_t nextDeltaPosition() override {
            if (fail) {
                fail = false;
                _CLTHROWA(CL_ERR_IO, "Injected CLucene streaming position read failure");
            }
            return MockTermPositions::nextDeltaPosition();
        }
        bool fail = true;
    };
    ClucenePostingsCursor source {TermPositionsPtr(new FailOncePositions())};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(source.next_block(&block, &eof).ok());
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(source.open_positions(0, &positions).ok());
    uint32_t position = 123;
    bool available = true;
    EXPECT_EQ(positions->next_position(&position, &available).code(),
              ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_FALSE(available);
    EXPECT_EQ(position, 123);
    ASSERT_TRUE(positions->next_position(&position, &available).ok());
    EXPECT_TRUE(available);
    EXPECT_EQ(position, 3);
    std::vector<uint32_t> remaining;
    ASSERT_TRUE(positions->append_remaining_positions(0, remaining).ok());
    EXPECT_EQ(remaining, (std::vector<uint32_t> {7}));
}

TEST_F(SegmentPostingsTest, CommonPositionCursorReturnsFinishErrors) {
    class FailingFinishPositions final : public MockTermPositions {
    public:
        FailingFinishPositions() : MockTermPositions({1}, {2}, {1}, {{3, 7}}, 1) {}
        void addLazySkipProxCount(int32_t) override {
            _CLTHROWA(CL_ERR_IO, "Injected CLucene position skip failure");
        }
    };
    ClucenePostingsCursor postings {TermPositionsPtr(new FailingFinishPositions())};
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(postings.next_block(&block, &eof).ok());
    index_query::PositionCursor* positions = nullptr;
    ASSERT_TRUE(postings.open_positions(0, &positions).ok());
    EXPECT_EQ(positions->finish_doc().code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

void expect_posting(const SegmentPostings& postings, uint32_t actual_doc, uint32_t doc,
                    uint32_t freq, uint32_t norm, bool scoring) {
    EXPECT_EQ(actual_doc, doc);
    EXPECT_EQ(postings.doc(), doc);
    const uint32_t expected_freq = scoring ? freq : 1;
    const uint32_t expected_norm = scoring ? norm : 1;
    EXPECT_EQ(postings.freq(), expected_freq);
    EXPECT_EQ(postings.norm(), expected_norm);
}

TEST_F(SegmentPostingsTest, TraversalAndScoresSurviveBlockRefillsAndSeeks) {
    for (bool scoring : {false, true}) {
        auto* reader = new ChunkedTermDocs();
        SegmentPostings postings(std::make_unique<ClucenePostingsCursor>(TermDocsPtr(reader)),
                                 scoring, nullptr);
        expect_posting(postings, postings.doc(), 1, 2, 11, scoring);
        expect_posting(postings, postings.advance(), 3, 3, 12, scoring);
        expect_posting(postings, postings.advance(), 8, 4, 13, scoring);
        expect_posting(postings, postings.seek(9), 10, 5, 14, scoring);
        expect_posting(postings, postings.seek(9), 10, 5, 14, scoring);
        expect_posting(postings, postings.seek(22), 30, 7, 16, scoring);
        EXPECT_EQ(postings.advance(), TERMINATED);
        EXPECT_EQ(reader->blocks_read, 3);
    }
}

TEST_F(SegmentPostingsTest, SeekSkipsUnneededBlocks) {
    for (bool scoring : {false, true}) {
        auto* reader = new ChunkedTermDocs();
        SegmentPostings postings(std::make_unique<ClucenePostingsCursor>(TermDocsPtr(reader)),
                                 scoring, nullptr);
        expect_posting(postings, postings.seek(22), 30, 7, 16, scoring);
        EXPECT_EQ(reader->blocks_read, 2);
    }
}

TEST_F(SegmentPostingsTest, test_postings_positions_with_offset) {
    class TestPostings : public Postings {
    public:
        void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override {
            output.push_back(offset + 10);
            output.push_back(offset + 20);
        }
    };

    TestPostings postings;
    std::vector<uint32_t> output = {999};
    postings.positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 2);
    EXPECT_EQ(output[0], 110);
    EXPECT_EQ(output[1], 120);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_next_true) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.doc(), 1);
    EXPECT_EQ(base.size_hint(), 3);
    EXPECT_EQ(base.freq(), 2);
    EXPECT_EQ(base.norm(), 1);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_next_false) {
    TermDocsPtr ptr(new MockTermDocs({}, {}, {}, 0));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_doc_terminate) {
    TermDocsPtr ptr(new MockTermDocs({TERMINATED}, {1}, {1}, 1));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_advance_success) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.doc(), 1);
    EXPECT_EQ(base.advance(), 3);
    EXPECT_EQ(base.advance(), 5);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_advance_end) {
    TermDocsPtr ptr(new MockTermDocs({1}, {2}, {1}, 1));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.advance(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_target_le_doc) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.seek(0), 1);
    EXPECT_EQ(base.seek(1), 1);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_in_block_success) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5, 7}, {2, 4, 6, 8}, {1, 1, 1, 1}, 4));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.seek(5), 5);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_fail) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    EXPECT_EQ(base.seek(10), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_append_positions_exception) {
    TermDocsPtr ptr(new MockTermDocs({1}, {2}, {1}, 1));
    SegmentPostings base(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true, nullptr);

    std::vector<uint32_t> output;
    EXPECT_THROW(base.append_positions_with_offset(0, output), Exception);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termdocs) {
    TermDocsPtr ptr(new MockTermDocs({1, 3}, {2, 4}, {1, 1}, 2));
    SegmentPostings postings(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true,
                             nullptr);

    EXPECT_EQ(postings.doc(), 1);
    EXPECT_EQ(postings.size_hint(), 2);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termpositions) {
    TermPositionsPtr ptr(
            new MockTermPositions({1, 3}, {2, 3}, {1, 1}, {{10, 20}, {30, 40, 50}}, 2));
    SegmentPostings postings(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true,
                             nullptr);
    EXPECT_EQ(postings.freq(), 2);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termpositions_append_positions) {
    TermPositionsPtr ptr(
            new MockTermPositions({1, 3}, {2, 3}, {1, 1}, {{10, 20}, {30, 40, 50}}, 2));
    SegmentPostings postings(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), true,
                             nullptr);

    std::vector<uint32_t> output = {999};
    postings.append_positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], 999);
    EXPECT_EQ(output[1], 110);
    EXPECT_EQ(output[2], 120);
}

TEST_F(SegmentPostingsTest, test_no_score_segment_posting) {
    TermDocsPtr ptr(new MockTermDocs({1, 3}, {5, 7}, {10, 20}, 2));
    SegmentPostings posting(std::make_unique<ClucenePostingsCursor>(std::move(ptr)), false,
                            nullptr);

    EXPECT_EQ(posting.doc(), 1);
    EXPECT_EQ(posting.freq(), 1);
    EXPECT_EQ(posting.norm(), 1);
}

namespace {

struct EnumerationFailure {
    lucene::index::TermEnum* alive = nullptr;
    lucene::index::TermDocs* postings_alive = nullptr;
    bool fail_seek = false;
    bool fail_open = false;
    bool fail_next = false;
    bool fail_close = false;
    size_t closes = 0;

    ~EnumerationFailure() {
        delete alive;
        delete postings_alive;
    }
};

class FailingTermEnum final : public lucene::index::TermEnum {
public:
    explicit FailingTermEnum(EnumerationFailure& failure)
            : _failure(failure), _term(make_term_ptr(L"body", L"abc")) {
        _failure.alive = this;
    }
    ~FailingTermEnum() override { _failure.alive = nullptr; }
    const char* getObjectName() const override { return "FailingTermEnum"; }
    bool next() override {
        if (_failure.fail_next) {
            _CLTHROWA(CL_ERR_IO, "Injected term enumeration failure");
        }
        return false;
    }
    lucene::index::Term* term(bool retain) override {
        if (retain) {
            return _CL_POINTER(_term.get());
        }
        return _term.get();
    }
    int32_t docFreq() const override { return 1; }
    void close() override {
        ++_failure.closes;
        if (_failure.fail_close) {
            _CLTHROWA(CL_ERR_IO, "Injected term enumeration close failure");
        }
    }

private:
    EnumerationFailure& _failure;
    TermPtr _term;
};

class FailingSeekDocs final : public MockTermDocs {
public:
    explicit FailingSeekDocs(EnumerationFailure& failure)
            : MockTermDocs({}, {}, {}, 0), _failure(failure) {
        _failure.postings_alive = this;
    }
    ~FailingSeekDocs() override { _failure.postings_alive = nullptr; }
    using MockTermDocs::seek;
    void seek(lucene::index::Term*) override {
        _CLTHROWA(CL_ERR_IO, "Injected postings seek failure");
    }

private:
    EnumerationFailure& _failure;
};

class FailingSeekPositions final : public MockTermPositions {
public:
    explicit FailingSeekPositions(EnumerationFailure& failure)
            : MockTermPositions({}, {}, {}, {}, 0), _failure(failure) {
        _failure.postings_alive = this;
    }
    ~FailingSeekPositions() override { _failure.postings_alive = nullptr; }
    using MockTermPositions::seek;
    void seek(lucene::index::Term*) override {
        _CLTHROWA(CL_ERR_IO, "Injected positions seek failure");
    }

private:
    EnumerationFailure& _failure;
};

class FailingSourceReader final : public lucene::index::MultiReader {
public:
    FailingSourceReader(const lucene::util::ArrayBase<lucene::index::IndexReader*>* readers,
                        EnumerationFailure& failure)
            : MultiReader(readers, false), _failure(failure) {}
    using MultiReader::terms;
    lucene::index::TermEnum* terms(const lucene::index::Term*, const void*) override {
        if (_failure.fail_open) {
            _CLTHROWA(CL_ERR_IO, "Injected term enumeration open failure");
        }
        return new FailingTermEnum(_failure);
    }
    lucene::index::TermDocs* termDocs(bool, const void*) override {
        if (_failure.fail_seek) {
            return new FailingSeekDocs(_failure);
        }
        _CLTHROWA(CL_ERR_IO, "Injected postings open failure");
    }
    lucene::index::TermPositions* termPositions(bool, const void*) override {
        if (_failure.fail_seek) {
            return new FailingSeekPositions(_failure);
        }
        _CLTHROWA(CL_ERR_IO, "Injected positions open failure");
    }

private:
    EnumerationFailure& _failure;
};

} // namespace

void check_postings_failure(bool positions, bool fail_seek) {
    EnumerationFailure failure;
    failure.fail_seek = fail_seek;
    lucene::util::ValueArray<lucene::index::IndexReader*> empty(0);
    auto reader = std::make_shared<FailingSourceReader>(&empty, failure);
    auto source = clucene_index_source(reader, L"body", nullptr);
    std::unique_ptr<index_query::PostingsCursor> cursor;
    Status status;
    EXPECT_NO_THROW(status = source->open_term("abc", positions, false, &cursor));
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_EQ(cursor, nullptr);
    EXPECT_EQ(failure.postings_alive, nullptr);
}

TEST(CluceneIndexSourceTest, OpeningPostingsReturnsAnErrorStatus) {
    check_postings_failure(false, false);
    check_postings_failure(true, false);
}

TEST(CluceneIndexSourceTest, SeekFailureDestroysThePostingsCursor) {
    check_postings_failure(false, true);
    check_postings_failure(true, true);
}

void check_enumeration_failure(EnumerationFailure& failure) {
    lucene::util::ValueArray<lucene::index::IndexReader*> empty(0);
    auto reader = std::make_shared<FailingSourceReader>(&empty, failure);
    auto source = clucene_index_source(reader, L"body", nullptr);
    index_query::TermPattern pattern;
    ASSERT_TRUE(
            index_query::TermPattern::create(index_query::TermPatternKind::kPrefix, "a", &pattern)
                    .ok());
    std::vector<std::string> terms;
    Status status;
    EXPECT_NO_THROW(status = source->expand_terms(pattern, 0, &terms));
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    EXPECT_EQ(failure.alive, nullptr);
    EXPECT_EQ(failure.closes, failure.fail_open ? 0U : 1U);
}

TEST(CluceneIndexSourceTest, OpeningEnumerationReturnsAnErrorStatus) {
    EnumerationFailure failure;
    failure.fail_open = true;
    check_enumeration_failure(failure);
}

TEST(CluceneIndexSourceTest, EnumerationFailureClosesAndDestroysTheCursor) {
    EnumerationFailure failure;
    failure.fail_next = true;
    check_enumeration_failure(failure);
}

TEST(CluceneIndexSourceTest, CloseFailureStillDestroysTheEnumerationCursor) {
    EnumerationFailure failure;
    failure.fail_close = true;
    check_enumeration_failure(failure);
}

} // namespace doris::segment_v2::inverted_index::query_v2
