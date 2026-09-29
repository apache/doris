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

#include <utility>
#include <vector>

#include "CLucene/index/DocRange.h"
#include "storage/index/inverted/spi/clucene_postings_cursor.h"

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

void expect_initial_positions(index_query::PositionCursor& positions) {
    EXPECT_EQ(positions.frequency(), 3);
    EXPECT_FALSE(positions.view().has_value());
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

template <typename Operation>
void expect_position_contract_failure(Operation&& operation) {
#ifndef NDEBUG
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(operation()), "Check failed");
#else
    EXPECT_THROW(static_cast<void>(operation()), Exception);
#endif
}

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

TEST_F(SegmentPostingsTest, CommonPositionCursorRejectsReplayAndInvalidatedBlocks) {
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

} // namespace doris::segment_v2::inverted_index::query_v2