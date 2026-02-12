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

#include "olap/rowset/segment_v2/inverted_index/query_v2/union_postings.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <vector>

#include "CLucene/index/DocRange.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// --- Mock helpers (same pattern as segment_postings_test.cpp) ---

class MockTermPositionsForUnion : public lucene::index::TermPositions {
public:
    MockTermPositionsForUnion(std::vector<uint32_t> docs, std::vector<uint32_t> freqs,
                              std::vector<uint32_t> norms,
                              std::vector<std::vector<uint32_t>> positions, int32_t doc_freq)
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

static SegmentPostingsPtr make_pos_postings(std::vector<uint32_t> docs, std::vector<uint32_t> freqs,
                                            std::vector<uint32_t> norms,
                                            std::vector<std::vector<uint32_t>> positions) {
    int32_t df = static_cast<int32_t>(docs.size());
    TermPositionsPtr ptr(new MockTermPositionsForUnion(std::move(docs), std::move(freqs),
                                                       std::move(norms), std::move(positions), df));
    return std::make_shared<SegmentPostings>(std::move(ptr), true);
}

class UnionPostingsTest : public testing::Test {};

// --- advance() tests ---

// Two subs with disjoint docs: advance walks through the union in order
TEST_F(UnionPostingsTest, advance_disjoint) {
    // sub0: {1, 5}  sub1: {3, 7}
    auto s0 = make_pos_postings({1, 5}, {1, 1}, {10, 10}, {{0}, {0}});
    auto s1 = make_pos_postings({3, 7}, {1, 1}, {20, 20}, {{0}, {0}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 1);
    EXPECT_EQ(u.advance(), 3);
    EXPECT_EQ(u.advance(), 5);
    EXPECT_EQ(u.advance(), 7);
    EXPECT_EQ(u.advance(), TERMINATED);
}

// Two subs with overlapping docs
TEST_F(UnionPostingsTest, advance_overlapping) {
    // sub0: {1, 3, 5}  sub1: {2, 3, 6}
    auto s0 = make_pos_postings({1, 3, 5}, {1, 1, 1}, {1, 1, 1}, {{0}, {0}, {0}});
    auto s1 = make_pos_postings({2, 3, 6}, {1, 1, 1}, {1, 1, 1}, {{0}, {0}, {0}});
    UnionPostings u({s0, s1});

    std::vector<uint32_t> result;
    uint32_t d = u.doc();
    while (d != TERMINATED) {
        result.push_back(d);
        d = u.advance();
    }
    EXPECT_EQ(result, (std::vector<uint32_t> {1, 2, 3, 5, 6}));
}

// Single sub
TEST_F(UnionPostingsTest, advance_single_sub) {
    auto s0 = make_pos_postings({10, 20}, {1, 1}, {1, 1}, {{0}, {0}});
    UnionPostings u({s0});

    EXPECT_EQ(u.doc(), 10);
    EXPECT_EQ(u.advance(), 20);
    EXPECT_EQ(u.advance(), TERMINATED);
}

// All subs empty → initial doc is TERMINATED
TEST_F(UnionPostingsTest, advance_all_empty) {
    auto s0 = make_pos_postings({}, {}, {}, {});
    auto s1 = make_pos_postings({}, {}, {}, {});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), TERMINATED);
    EXPECT_EQ(u.advance(), TERMINATED);
}

// --- seek() tests ---

// seek target <= current doc → returns current doc (early return branch)
TEST_F(UnionPostingsTest, seek_target_le_current) {
    auto s0 = make_pos_postings({5, 10}, {1, 1}, {1, 1}, {{0}, {0}});
    UnionPostings u({s0});

    EXPECT_EQ(u.doc(), 5);
    EXPECT_EQ(u.seek(3), 5); // target < doc
    EXPECT_EQ(u.seek(5), 5); // target == doc
}

// seek forward, some subs need to advance
TEST_F(UnionPostingsTest, seek_forward) {
    auto s0 = make_pos_postings({1, 5, 10}, {1, 1, 1}, {1, 1, 1}, {{0}, {0}, {0}});
    auto s1 = make_pos_postings({3, 7, 12}, {1, 1, 1}, {1, 1, 1}, {{0}, {0}, {0}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 1);
    // seek to 7: s0 has 10 (>=7), s1 has 7 (>=7), min=7
    EXPECT_EQ(u.seek(7), 7);
    EXPECT_EQ(u.advance(), 10);
    EXPECT_EQ(u.advance(), 12);
    EXPECT_EQ(u.advance(), TERMINATED);
}

// seek past all docs → TERMINATED
TEST_F(UnionPostingsTest, seek_past_end) {
    auto s0 = make_pos_postings({1, 3}, {1, 1}, {1, 1}, {{0}, {0}});
    UnionPostings u({s0});

    EXPECT_EQ(u.seek(100), TERMINATED);
}

// seek where sub.doc() >= target already (d >= target branch, no sub.seek needed)
TEST_F(UnionPostingsTest, seek_sub_already_past_target) {
    auto s0 = make_pos_postings({1, 10}, {1, 1}, {1, 1}, {{0}, {0}});
    auto s1 = make_pos_postings({8, 20}, {1, 1}, {1, 1}, {{0}, {0}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 1);
    // advance to 8
    EXPECT_EQ(u.seek(8), 8);
    // now seek to 9: s0 has 10 (>=9, no seek needed), s1 has 20 (>=9, no seek needed)
    EXPECT_EQ(u.seek(9), 10);
}

// --- size_hint() tests ---

TEST_F(UnionPostingsTest, size_hint_sums_subs) {
    auto s0 = make_pos_postings({1, 2, 3}, {1, 1, 1}, {1, 1, 1}, {{0}, {0}, {0}});
    auto s1 = make_pos_postings({4, 5}, {1, 1}, {1, 1}, {{0}, {0}});
    UnionPostings u({s0, s1});

    // size_hint = sum of sub size_hints = 3 + 2 = 5
    EXPECT_EQ(u.size_hint(), 5);
}

// --- freq() tests ---

// freq aggregates across subs on the same doc
TEST_F(UnionPostingsTest, freq_aggregates_on_same_doc) {
    // doc 3 appears in both subs with freq 2 and 3
    auto s0 = make_pos_postings({3}, {2}, {1}, {{10, 20}});
    auto s1 = make_pos_postings({3}, {3}, {1}, {{30, 40, 50}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 3);
    EXPECT_EQ(u.freq(), 5); // 2 + 3
}

// freq only counts subs on current doc
TEST_F(UnionPostingsTest, freq_only_current_doc) {
    auto s0 = make_pos_postings({1, 5}, {2, 3}, {1, 1}, {{10, 20}, {30, 40, 50}});
    auto s1 = make_pos_postings({5, 10}, {4, 1}, {1, 1}, {{60, 70, 80, 90}, {100}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 1);
    EXPECT_EQ(u.freq(), 2); // only s0 is on doc 1

    u.advance(); // doc 5
    EXPECT_EQ(u.doc(), 5);
    EXPECT_EQ(u.freq(), 7); // s0 freq=3, s1 freq=4
}

// --- norm() tests ---

// norm returns first matching sub's norm
TEST_F(UnionPostingsTest, norm_returns_first_matching) {
    auto s0 = make_pos_postings({3}, {1}, {42}, {{0}});
    auto s1 = make_pos_postings({3}, {1}, {99}, {{0}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 3);
    EXPECT_EQ(u.norm(), 42); // first sub that matches
}

// norm returns 1 when no sub matches (TERMINATED state)
TEST_F(UnionPostingsTest, norm_no_match_returns_1) {
    auto s0 = make_pos_postings({1}, {1}, {50}, {{0}});
    UnionPostings u({s0});

    u.advance(); // TERMINATED
    EXPECT_EQ(u.doc(), TERMINATED);
    EXPECT_EQ(u.norm(), 1);
}

// --- append_positions_with_offset() tests ---

// Positions from multiple subs are merged and sorted
TEST_F(UnionPostingsTest, positions_merged_and_sorted) {
    // doc 5: s0 has positions {20, 40}, s1 has positions {10, 30}
    auto s0 = make_pos_postings({5}, {2}, {1}, {{20, 40}});
    auto s1 = make_pos_postings({5}, {2}, {1}, {{10, 30}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 5);
    std::vector<uint32_t> output;
    u.append_positions_with_offset(100, output);

    // offset=100: {120, 140} from s0, {110, 130} from s1 → sorted: {110, 120, 130, 140}
    EXPECT_EQ(output, (std::vector<uint32_t> {110, 120, 130, 140}));
}

// Positions only from subs on current doc
TEST_F(UnionPostingsTest, positions_only_current_doc) {
    auto s0 = make_pos_postings({1, 5}, {1, 2}, {1, 1}, {{0}, {10, 20}});
    auto s1 = make_pos_postings({5, 10}, {1, 1}, {1, 1}, {{30}, {40}});
    UnionPostings u({s0, s1});

    EXPECT_EQ(u.doc(), 1);
    std::vector<uint32_t> output;
    u.append_positions_with_offset(0, output);
    EXPECT_EQ(output, (std::vector<uint32_t> {0})); // only s0 on doc 1
}

// append preserves existing content in output vector
TEST_F(UnionPostingsTest, positions_append_preserves_existing) {
    auto s0 = make_pos_postings({1}, {1}, {1}, {{5}});
    UnionPostings u({s0});

    std::vector<uint32_t> output = {999};
    u.append_positions_with_offset(0, output);
    EXPECT_EQ(output.size(), 2);
    EXPECT_EQ(output[0], 999);
    EXPECT_EQ(output[1], 5);
}

// Single position from single sub → no sort needed (size - start <= 1)
TEST_F(UnionPostingsTest, positions_single_no_sort) {
    auto s0 = make_pos_postings({1}, {1}, {1}, {{7}});
    UnionPostings u({s0});

    std::vector<uint32_t> output;
    u.append_positions_with_offset(10, output);
    EXPECT_EQ(output, (std::vector<uint32_t> {17}));
}

// --- positions_with_offset() (inherited from Postings base) ---

TEST_F(UnionPostingsTest, positions_with_offset_clears_and_appends) {
    auto s0 = make_pos_postings({1}, {2}, {1}, {{3, 8}});
    UnionPostings u({s0});

    std::vector<uint32_t> output = {999, 888};
    u.positions_with_offset(0, output);
    // Should clear existing content, then append
    EXPECT_EQ(output, (std::vector<uint32_t> {3, 8}));
}

// --- make_union_postings() factory ---

TEST_F(UnionPostingsTest, make_union_postings_factory) {
    auto s0 = make_pos_postings({2, 4}, {1, 1}, {1, 1}, {{0}, {0}});
    auto s1 = make_pos_postings({3}, {1}, {1}, {{0}});
    auto u = make_union_postings({s0, s1});

    ASSERT_NE(u, nullptr);
    EXPECT_EQ(u->doc(), 2);
    EXPECT_EQ(u->advance(), 3);
    EXPECT_EQ(u->advance(), 4);
    EXPECT_EQ(u->advance(), TERMINATED);
}

// --- Three subs ---

TEST_F(UnionPostingsTest, three_subs) {
    auto s0 = make_pos_postings({1, 10}, {1, 1}, {1, 1}, {{0}, {0}});
    auto s1 = make_pos_postings({5, 10}, {1, 1}, {1, 1}, {{0}, {0}});
    auto s2 = make_pos_postings({3, 10}, {1, 1}, {1, 1}, {{0}, {0}});
    UnionPostings u({s0, s1, s2});

    std::vector<uint32_t> result;
    uint32_t d = u.doc();
    while (d != TERMINATED) {
        result.push_back(d);
        d = u.advance();
    }
    EXPECT_EQ(result, (std::vector<uint32_t> {1, 3, 5, 10}));
}

} // namespace doris::segment_v2::inverted_index::query_v2
