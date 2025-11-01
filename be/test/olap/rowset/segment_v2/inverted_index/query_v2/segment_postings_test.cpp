#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <climits>
#include <memory>
#include <vector>

#include "CLucene/index/DocRange.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class MockTermDocs : public lucene::index::TermDocs {
public:
    MockTermDocs(std::vector<int32_t> docs, std::vector<int32_t> freqs, std::vector<int32_t> norms,
                 int32_t doc_freq)
            : _docs(std::move(docs)),
              _freqs(std::move(freqs)),
              _norms(std::move(norms)),
              _doc_freq(doc_freq) {}

    void seek(lucene::index::Term* term) override {}
    void seek(lucene::index::TermEnum* termEnum) override {}

    int32_t doc() const override {
        if (_index >= 0 && _index < static_cast<int32_t>(_docs.size())) {
            return _docs[_index];
        }
        return INT_MAX;
    }

    int32_t freq() const override {
        if (_index >= 0 && _index < static_cast<int32_t>(_freqs.size())) {
            return _freqs[_index];
        }
        return 0;
    }

    int32_t norm() const override {
        if (_index >= 0 && _index < static_cast<int32_t>(_norms.size())) {
            return _norms[_index];
        }
        return 1;
    }

    bool next() override {
        if (_index + 1 < static_cast<int32_t>(_docs.size())) {
            ++_index;
            return true;
        }
        return false;
    }

    int32_t read(int32_t*, int32_t*, int32_t) override { return 0; }
    int32_t read(int32_t*, int32_t*, int32_t*, int32_t) override { return 0; }
    bool readRange(DocRange*) override { return false; }

    bool skipTo(const int32_t target) override {
        auto size = static_cast<int32_t>(_docs.size());
        while (_index + 1 < size && _docs[_index + 1] < target) {
            ++_index;
        }
        if (_index + 1 < size) {
            ++_index;
            return true;
        }
        return false;
    }

    void close() override {}
    lucene::index::TermPositions* __asTermPositions() override { return nullptr; }
    int32_t docFreq() override { return _doc_freq; }

private:
    std::vector<int32_t> _docs;
    std::vector<int32_t> _freqs;
    std::vector<int32_t> _norms;
    int32_t _doc_freq;
    int32_t _index = -1;
};

class MockTermPositions : public lucene::index::TermPositions {
public:
    MockTermPositions(std::vector<int32_t> docs, std::vector<int32_t> freqs,
                      std::vector<int32_t> norms, std::vector<std::vector<int32_t>> positions,
                      int32_t doc_freq)
            : _docs(std::move(docs)),
              _freqs(std::move(freqs)),
              _norms(std::move(norms)),
              _positions(std::move(positions)),
              _doc_freq(doc_freq) {}

    void seek(lucene::index::Term* term) override {}
    void seek(lucene::index::TermEnum* termEnum) override {}

    int32_t doc() const override {
        if (_index >= 0 && _index < static_cast<int32_t>(_docs.size())) {
            return _docs[_index];
        }
        return INT_MAX;
    }

    int32_t freq() const override {
        if (_index >= 0 && _index < static_cast<int32_t>(_freqs.size())) {
            return _freqs[_index];
        }
        return 0;
    }

    int32_t norm() const override {
        if (_index >= 0 && _index < static_cast<int32_t>(_norms.size())) {
            return _norms[_index];
        }
        return 1;
    }

    bool next() override {
        if (_index + 1 < static_cast<int32_t>(_docs.size())) {
            ++_index;
            _pos_index = 0;
            return true;
        }
        return false;
    }

    int32_t read(int32_t*, int32_t*, int32_t) override { return 0; }
    int32_t read(int32_t*, int32_t*, int32_t*, int32_t) override { return 0; }
    bool readRange(DocRange*) override { return false; }

    bool skipTo(const int32_t target) override {
        auto size = static_cast<int32_t>(_docs.size());
        while (_index + 1 < size && _docs[_index + 1] < target) {
            ++_index;
        }
        _pos_index = 0;
        if (_index + 1 < size) {
            ++_index;
            return true;
        }
        return false;
    }

    void close() override {}
    lucene::index::TermPositions* __asTermPositions() override { return this; }
    lucene::index::TermDocs* __asTermDocs() override { return this; }

    int32_t nextPosition() override {
        if (_index >= 0 && _index < static_cast<int32_t>(_positions.size()) &&
            _pos_index < _positions[_index].size()) {
            return _positions[_index][_pos_index++];
        }
        return 0;
    }

    int32_t getPayloadLength() const override { return 0; }
    uint8_t* getPayload(uint8_t*) override { return nullptr; }
    bool isPayloadAvailable() const override { return false; }
    int32_t docFreq() override { return _doc_freq; }

private:
    std::vector<int32_t> _docs;
    std::vector<int32_t> _freqs;
    std::vector<int32_t> _norms;
    std::vector<std::vector<int32_t>> _positions;
    int32_t _doc_freq;
    int32_t _index = -1;
    size_t _pos_index = 0;
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

TEST_F(SegmentPostingsTest, test_segment_postings_base_default_constructor) {
    SegmentPostingsBase<TermDocsPtr> base;
    EXPECT_EQ(base.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_next_true) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.doc(), 1);
    EXPECT_EQ(base.size_hint(), 3);
    EXPECT_EQ(base.freq(), 2);
    EXPECT_EQ(base.norm(), 1);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_next_false) {
    TermDocsPtr ptr(new MockTermDocs({}, {}, {}, 0));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_constructor_doc_int_max) {
    TermDocsPtr ptr(new MockTermDocs({INT_MAX}, {1}, {1}, 1));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_advance_success) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.doc(), 1);
    EXPECT_EQ(base.advance(), 3);
    EXPECT_EQ(base.advance(), 5);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_advance_end) {
    TermDocsPtr ptr(new MockTermDocs({1}, {2}, {1}, 1));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.advance(), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_target_le_doc) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.seek(0), 1);
    EXPECT_EQ(base.seek(1), 1);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_skipTo_success) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5, 7}, {2, 4, 6, 8}, {1, 1, 1, 1}, 4));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.seek(5), 5);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_seek_skipTo_fail) {
    TermDocsPtr ptr(new MockTermDocs({1, 3, 5}, {2, 4, 6}, {1, 1, 1}, 3));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    EXPECT_EQ(base.seek(10), TERMINATED);
}

TEST_F(SegmentPostingsTest, test_segment_postings_base_append_positions_exception) {
    TermDocsPtr ptr(new MockTermDocs({1}, {2}, {1}, 1));
    SegmentPostingsBase<TermDocsPtr> base(std::move(ptr));

    std::vector<uint32_t> output;
    EXPECT_THROW(base.append_positions_with_offset(0, output), Exception);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termdocs) {
    TermDocsPtr ptr(new MockTermDocs({1, 3}, {2, 4}, {1, 1}, 2));
    SegmentPostings<TermDocsPtr> postings(std::move(ptr));

    EXPECT_EQ(postings.doc(), 1);
    EXPECT_EQ(postings.size_hint(), 2);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termpositions) {
    TermPositionsPtr ptr(
            new MockTermPositions({1, 3}, {2, 3}, {1, 1}, {{10, 20}, {30, 40, 50}}, 2));
    SegmentPostings<TermPositionsPtr> postings(std::move(ptr));

    EXPECT_EQ(postings.doc(), 1);
    EXPECT_EQ(postings.freq(), 2);
}

TEST_F(SegmentPostingsTest, test_segment_postings_termpositions_append_positions) {
    TermPositionsPtr ptr(
            new MockTermPositions({1, 3}, {2, 3}, {1, 1}, {{10, 20}, {30, 40, 50}}, 2));
    SegmentPostings<TermPositionsPtr> postings(std::move(ptr));

    std::vector<uint32_t> output = {999};
    postings.append_positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], 999);
    EXPECT_EQ(output[1], 110);
    EXPECT_EQ(output[2], 120);
}

TEST_F(SegmentPostingsTest, test_no_score_segment_posting) {
    TermDocsPtr ptr(new MockTermDocs({1, 3}, {5, 7}, {10, 20}, 2));
    NoScoreSegmentPosting<TermDocsPtr> posting(std::move(ptr));

    EXPECT_EQ(posting.doc(), 1);
    EXPECT_EQ(posting.freq(), 1);
    EXPECT_EQ(posting.norm(), 1);
}

TEST_F(SegmentPostingsTest, test_empty_segment_posting) {
    EmptySegmentPosting<TermDocsPtr> posting;

    EXPECT_EQ(posting.doc(), TERMINATED);
    EXPECT_EQ(posting.size_hint(), 0);
    EXPECT_EQ(posting.freq(), 1);
    EXPECT_EQ(posting.norm(), 1);
    EXPECT_EQ(posting.advance(), TERMINATED);
    EXPECT_EQ(posting.seek(100), TERMINATED);
}

} // namespace doris::segment_v2::inverted_index::query_v2