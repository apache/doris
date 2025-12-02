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

#include "olap/rowset/segment_v2/inverted_index/query_v2/postings/loaded_postings.h"

#include <gtest/gtest.h>

namespace doris {

using namespace segment_v2::inverted_index::query_v2;

class LoadedPostingsTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(LoadedPostingsTest, EmptyPostings) {
    LoadedPostings postings;
    EXPECT_EQ(postings.doc(), TERMINATED);
    EXPECT_EQ(postings.advance(), TERMINATED);
    EXPECT_EQ(postings.size_hint(), 0);
    EXPECT_EQ(postings.freq(), 0);
}

TEST_F(LoadedPostingsTest, BasicAdvance) {
    std::vector<uint32_t> doc_ids;
    for (uint32_t i = 0; i < 1024; ++i) {
        doc_ids.push_back(i * 3);
    }
    std::vector<std::vector<uint32_t>> positions(1024);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 0);
    EXPECT_EQ(postings.advance(), 3);
    EXPECT_EQ(postings.doc(), 3);
}

TEST_F(LoadedPostingsTest, SeekExactMatch) {
    std::vector<uint32_t> doc_ids;
    for (uint32_t i = 0; i < 1024; ++i) {
        doc_ids.push_back(i * 3);
    }
    std::vector<std::vector<uint32_t>> positions(1024);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 0);
    EXPECT_EQ(postings.seek(15), 15);
    EXPECT_EQ(postings.doc(), 15);
    EXPECT_EQ(postings.seek(300), 300);
    EXPECT_EQ(postings.doc(), 300);
}

TEST_F(LoadedPostingsTest, SeekBeyondEnd) {
    std::vector<uint32_t> doc_ids;
    for (uint32_t i = 0; i < 1024; ++i) {
        doc_ids.push_back(i * 3);
    }
    std::vector<std::vector<uint32_t>> positions(1024);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.seek(6000), TERMINATED);
}

TEST_F(LoadedPostingsTest, SeekNonExactMatch) {
    std::vector<uint32_t> doc_ids;
    for (uint32_t i = 0; i < 1024; ++i) {
        doc_ids.push_back(i * 3);
    }
    std::vector<std::vector<uint32_t>> positions(1024);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 0);
    // Seek to 14, should land on 15 (next available)
    EXPECT_EQ(postings.seek(14), 15);
    EXPECT_EQ(postings.doc(), 15);
}

TEST_F(LoadedPostingsTest, WithPositions) {
    std::vector<uint32_t> doc_ids;
    for (uint32_t i = 0; i < 1024; ++i) {
        doc_ids.push_back(i * 3);
    }

    std::vector<std::vector<uint32_t>> positions(1024);
    positions[0] = {1, 2, 3};
    positions[1] = {30};
    positions[2] = {10};
    positions[4] = {50};

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 0);

    std::vector<uint32_t> pos;
    postings.positions_with_offset(0, pos);
    EXPECT_EQ(pos.size(), 3);
    EXPECT_EQ(pos[0], 1);
    EXPECT_EQ(pos[1], 2);
    EXPECT_EQ(pos[2], 3);

    EXPECT_EQ(postings.advance(), 3);
    EXPECT_EQ(postings.doc(), 3);

    postings.positions_with_offset(0, pos);
    EXPECT_EQ(pos.size(), 1);
    EXPECT_EQ(pos[0], 30);
}

TEST_F(LoadedPostingsTest, TermFreq) {
    std::vector<uint32_t> doc_ids = {0, 3, 6, 9};
    std::vector<std::vector<uint32_t>> positions(4);
    positions[0] = {1, 2, 3};        // freq = 3
    positions[1] = {30};             // freq = 1
    positions[2] = {10, 20};         // freq = 2
    positions[3] = {50, 60, 70, 80}; // freq = 4

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 0);
    EXPECT_EQ(postings.freq(), 3);
    EXPECT_EQ(postings.freq(), 3);

    postings.advance();
    EXPECT_EQ(postings.doc(), 3);
    EXPECT_EQ(postings.freq(), 1);

    postings.advance();
    EXPECT_EQ(postings.doc(), 6);
    EXPECT_EQ(postings.freq(), 2);

    postings.advance();
    EXPECT_EQ(postings.doc(), 9);
    EXPECT_EQ(postings.freq(), 4);
}

TEST_F(LoadedPostingsTest, AppendPositionsWithOffset) {
    std::vector<uint32_t> doc_ids = {0, 3};
    std::vector<std::vector<uint32_t>> positions(2);
    positions[0] = {1, 2, 3};
    positions[1] = {10, 20};

    LoadedPostings postings(doc_ids, positions);

    std::vector<uint32_t> output;
    postings.append_positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], 101);
    EXPECT_EQ(output[1], 102);
    EXPECT_EQ(output[2], 103);

    // Append more from the same doc
    postings.append_positions_with_offset(200, output);
    EXPECT_EQ(output.size(), 6);
    EXPECT_EQ(output[3], 201);
    EXPECT_EQ(output[4], 202);
    EXPECT_EQ(output[5], 203);
}

TEST_F(LoadedPostingsTest, PositionsWithOffset) {
    std::vector<uint32_t> doc_ids = {0, 3};
    std::vector<std::vector<uint32_t>> positions(2);
    positions[0] = {1, 2, 3};
    positions[1] = {10, 20};

    LoadedPostings postings(doc_ids, positions);

    std::vector<uint32_t> output;
    postings.positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], 101);
    EXPECT_EQ(output[1], 102);
    EXPECT_EQ(output[2], 103);
}

TEST_F(LoadedPostingsTest, SizeHint) {
    std::vector<uint32_t> doc_ids = {1, 5, 10, 15, 20};
    std::vector<std::vector<uint32_t>> positions(5);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.size_hint(), 5);
}

TEST_F(LoadedPostingsTest, Norm) {
    std::vector<uint32_t> doc_ids = {1, 5};
    std::vector<std::vector<uint32_t>> positions(2);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.norm(), 1);
}

TEST_F(LoadedPostingsTest, SingleDocument) {
    std::vector<uint32_t> doc_ids = {42};
    std::vector<std::vector<uint32_t>> positions(1);
    positions[0] = {1, 5, 10};

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 42);
    EXPECT_EQ(postings.freq(), 3);
    EXPECT_EQ(postings.size_hint(), 1);
    EXPECT_EQ(postings.advance(), TERMINATED);
}

TEST_F(LoadedPostingsTest, SeekAfterAdvance) {
    std::vector<uint32_t> doc_ids = {1, 5, 10, 15, 20, 25, 30};
    std::vector<std::vector<uint32_t>> positions(7);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.seek(10), 10);
    EXPECT_EQ(postings.advance(), 15);
    EXPECT_EQ(postings.advance(), 20);
    EXPECT_EQ(postings.seek(25), 25);
    EXPECT_EQ(postings.advance(), 30);
    EXPECT_EQ(postings.advance(), TERMINATED);
}

TEST_F(LoadedPostingsTest, SeekToCurrentDoc) {
    std::vector<uint32_t> doc_ids = {1, 5, 10, 15, 20};
    std::vector<std::vector<uint32_t>> positions(5);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 1);
    EXPECT_EQ(postings.seek(1), 1);
    EXPECT_EQ(postings.doc(), 1);

    EXPECT_EQ(postings.advance(), 5);
    EXPECT_EQ(postings.seek(5), 5);
    EXPECT_EQ(postings.doc(), 5);
}

TEST_F(LoadedPostingsTest, SeekBeforeCurrent) {
    std::vector<uint32_t> doc_ids = {1, 5, 10, 15, 20};
    std::vector<std::vector<uint32_t>> positions(5);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.advance(), 5);
    EXPECT_EQ(postings.doc(), 5);

    // Seeking to a value less than current should return current
    EXPECT_EQ(postings.seek(3), 5);
    EXPECT_EQ(postings.doc(), 5);
}

TEST_F(LoadedPostingsTest, NoPositions) {
    std::vector<uint32_t> doc_ids = {1, 5, 10};
    std::vector<std::vector<uint32_t>> positions(3); // All empty

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.doc(), 1);
    EXPECT_EQ(postings.freq(), 0);

    std::vector<uint32_t> pos;
    postings.positions_with_offset(0, pos);
    EXPECT_EQ(pos.size(), 0);
}

TEST_F(LoadedPostingsTest, AdvanceToEnd) {
    std::vector<uint32_t> doc_ids = {1, 2, 3};
    std::vector<std::vector<uint32_t>> positions(3);

    LoadedPostings postings(doc_ids, positions);

    EXPECT_EQ(postings.advance(), 2);
    EXPECT_EQ(postings.advance(), 3);
    EXPECT_EQ(postings.advance(), TERMINATED);
    EXPECT_EQ(postings.advance(), TERMINATED); // Stay at TERMINATED
}

} // namespace doris