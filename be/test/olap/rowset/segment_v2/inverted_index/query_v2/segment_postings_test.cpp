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

#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <vector>

#include "CLucene/index/DocRange.h"

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

    bool readRange(DocRange* docRange) override {
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
    void skipToBlock(const int32_t target) override {}

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

    bool readRange(DocRange* docRange) override {
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
    void skipToBlock(const int32_t target) override {}

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
    SegmentPostings base(std::move(ptr), true);

    EXPECT_EQ(base.doc(), 1);
    EXPECT_EQ(base.size_hint(), 3);
    EXPECT_EQ(base.freq(), 2);
    EXPECT_EQ(base.norm(), 1);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_next_false) {
    TermDocsPtr ptr(new MockTermDocs({}, {}, {}, 0));
    SegmentPostings base(std::move(ptr));

    EXPECT_EQ(base.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_doc_terminate) {
    TermDocsPtr ptr(new MockTermDocs({TERMINATED}, {1}, {1}, 1));
    SegmentPostings base(std::move(ptr));

    EXPECT_EQ(base.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_advance_success) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostings base(std::move(ptr));

    EXPECT_EQ(base.doc(), 1);
    EXPECT_EQ(base.advance(), 3);
    EXPECT_EQ(base.advance(), 5);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_advance_end) {
    TermDocsPtr ptr(new MockTermDocs({1}, {2}, {1}, 1));
    SegmentPostings base(std::move(ptr));

    EXPECT_EQ(base.advance(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_target_le_doc) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostings base(std::move(ptr));

    EXPECT_EQ(base.seek(0), 1);
    EXPECT_EQ(base.seek(1), 1);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_in_block_success) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5, 7}, {2, 4, 6, 8}, {1, 1, 1, 1}, 4));
    SegmentPostings base(std::move(ptr));

    EXPECT_EQ(base.seek(5), 5);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_fail) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostings base(std::move(ptr));

    EXPECT_EQ(base.seek(10), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_append_positions_exception) {
    TermDocsPtr ptr(new MockTermDocs({1}, {2}, {1}, 1));
    SegmentPostings base(std::move(ptr));

    std::vector<uint32_t> output;
    EXPECT_THROW(base.append_positions_with_offset(0, output), Exception);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termdocs) {
    TermDocsPtr ptr(new MockTermDocs({1, 3}, {2, 4}, {1, 1}, 2));
    SegmentPostings postings(std::move(ptr));

    EXPECT_EQ(postings.doc(), 1);
    EXPECT_EQ(postings.size_hint(), 2);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termpositions) {
    TermPositionsPtr ptr(
            new MockTermPositions({1, 3}, {2, 3}, {1, 1}, {{10, 20}, {30, 40, 50}}, 2));
    SegmentPostings postings(std::move(ptr), true);

    EXPECT_EQ(postings.doc(), 1);
    EXPECT_EQ(postings.freq(), 2);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termpositions_append_positions) {
    TermPositionsPtr ptr(
            new MockTermPositions({1, 3}, {2, 3}, {1, 1}, {{10, 20}, {30, 40, 50}}, 2));
    SegmentPostings postings(std::move(ptr), true);

    std::vector<uint32_t> output = {999};
    postings.append_positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], 999);
    EXPECT_EQ(output[1], 110);
    EXPECT_EQ(output[2], 120);
}

TEST_F(SegmentPostingsTest, test_no_score_segment_posting) {
    TermDocsPtr ptr(new MockTermDocs({1, 3}, {5, 7}, {10, 20}, 2));
    SegmentPostings posting(std::move(ptr));

    EXPECT_EQ(posting.doc(), 1);
    EXPECT_EQ(posting.freq(), 1);
    EXPECT_EQ(posting.norm(), 1);
}

} // namespace doris::segment_v2::inverted_index::query_v2