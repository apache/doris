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

#include "olap/rowset/segment_v2/inverted_index/util/union_term_iterator.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"

namespace doris::segment_v2 {

TEST(PositionsQueueTest, InitialState) {
    PositionsQueue q;
    EXPECT_EQ(q.size(), 0);
}

TEST(PositionsQueueTest, AddAndGrow) {
    PositionsQueue q;
    for (int32_t i = 0; i < 16; ++i) {
        q.add(i);
    }
    EXPECT_EQ(q.size(), 16);

    q.add(16);
    EXPECT_EQ(q.size(), 17);

    for (int32_t i = 0; i < 17; ++i) {
        EXPECT_EQ(q.next(), i);
    }
}

TEST(PositionsQueueTest, NextOrder) {
    PositionsQueue q;
    q.add(3);
    q.add(1);
    q.add(2);

    EXPECT_EQ(q.next(), 3);
    EXPECT_EQ(q.next(), 1);
    EXPECT_EQ(q.next(), 2);
}

TEST(PositionsQueueTest, SortFunctionality) {
    PositionsQueue q;
    q.add(3);
    q.add(1);
    q.add(2);
    q.sort();

    EXPECT_EQ(q.next(), 1);
    EXPECT_EQ(q.next(), 2);
    EXPECT_EQ(q.next(), 3);
}

TEST(PositionsQueueTest, SortAfterPartialRead) {
    PositionsQueue q;
    q.add(5);
    q.add(3);
    q.add(4);
    q.add(1);
    q.add(2);
    q.sort();

    q.next();
    q.next();
    q.sort();

    EXPECT_EQ(q.next(), 0);
    EXPECT_EQ(q.next(), 0);
    EXPECT_EQ(q.next(), 3);
}

TEST(PositionsQueueTest, ClearResetsState) {
    PositionsQueue q;
    q.add(1);
    q.add(2);
    q.next();
    q.clear();

    EXPECT_EQ(q.size(), 0);
    q.add(3);
    EXPECT_EQ(q.next(), 3);
}

TEST(PositionsQueueTest, MultipleGrowth) {
    PositionsQueue q;
    for (int32_t i = 0; i < 1000; ++i) {
        q.add(i);
    }
    for (int32_t i = 0; i < 1000; ++i) {
        EXPECT_EQ(q.next(), i);
    }
}

TEST(UnionTermIteratorTest, HighFrequencySingleDoc) {
    MockIterPtr it1 = std::make_shared<MockIterator>();
    MockIterPtr it2 = std::make_shared<MockIterator>();
    MockIterPtr it3 = std::make_shared<MockIterator>();

    it1->set_postings({{100, {50, 150, 250, 350, 450, 550}}});
    it2->set_postings({{100, {100, 200, 300, 400, 500, 600, 700}}});
    it3->set_postings({{100, {75, 175, 275, 375, 475}}});

    UnionTermIterator<MockIterator> uti({it1, it2, it3});
    uti.advance(100);

    std::vector<int32_t> positions;
    for (int32_t i = 0; i < uti.freq(); i++) {
        positions.push_back(uti.next_position());
    }

    const std::vector<int32_t> expected = {50,  75,  100, 150, 175, 200, 250, 275, 300,
                                           350, 375, 400, 450, 475, 500, 550, 600, 700};
    ASSERT_EQ(positions.size(), 18);
    EXPECT_EQ(positions, expected);
}

TEST(UnionTermIteratorTest, CrossDocumentPositionHandling) {
    MockIterPtr it1 = std::make_shared<MockIterator>();
    MockIterPtr it2 = std::make_shared<MockIterator>();
    it1->set_postings({{10, {5, 15, 25, 35, 45, 55}}, {20, {10, 20, 30, 40, 50, 60, 70}}});
    it2->set_postings({{10, {2, 12, 22, 32, 42}}, {30, {100, 200, 300, 400, 500, 600}}});

    UnionTermIterator<MockIterator> uti({it1, it2});

    uti.advance(10);
    std::vector<int32_t> doc10_pos;
    for (int32_t i = 0; i < uti.freq(); i++) {
        doc10_pos.push_back(uti.next_position());
    }
    EXPECT_EQ(doc10_pos, (std::vector<int32_t> {2, 5, 12, 15, 22, 25, 32, 35, 42, 45, 55}));

    uti.advance(20);
    std::vector<int32_t> doc20_pos;
    for (int32_t i = 0; i < uti.freq(); i++) {
        doc20_pos.push_back(uti.next_position());
    }
    EXPECT_EQ(doc20_pos, (std::vector<int32_t> {10, 20, 30, 40, 50, 60, 70}));

    uti.advance(30);
    std::vector<int32_t> doc30_pos;
    for (int32_t i = 0; i < uti.freq(); i++) {
        doc30_pos.push_back(uti.next_position());
    }
    EXPECT_EQ(doc30_pos, (std::vector<int32_t> {100, 200, 300, 400, 500, 600}));
}

TEST(UnionTermIteratorTest, PositionAfterAdvance) {
    MockIterPtr it1 = std::make_shared<MockIterator>();
    MockIterPtr it2 = std::make_shared<MockIterator>();
    it1->set_postings({{100, {50, 150, 250, 350, 450}}, {200, {10, 20, 30, 40, 50, 60}}});
    it2->set_postings({{150, {100, 200, 300}}, {200, {15, 25, 35, 45, 55, 65}}});

    UnionTermIterator<MockIterator> uti({it1, it2});

    ASSERT_EQ(uti.advance(200), 200);
    std::vector<int32_t> positions;
    for (int32_t i = 0; i < uti.freq(); i++) {
        int32_t pos = uti.next_position();
        positions.push_back(pos);
    }
    EXPECT_EQ(positions, (std::vector<int32_t> {10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65}));
}

TEST(UnionTermIteratorTest, HighVolumeStressTest) {
    std::map<int32_t, std::vector<int32_t>> data;
    for (int32_t doc_id = 1; doc_id <= 1000; ++doc_id) {
        std::vector<int32_t> positions;
        for (int32_t pos = 1; pos <= 10; ++pos) {
            positions.push_back(doc_id * 100 + pos);
        }
        data[doc_id] = positions;
    }

    MockIterPtr it1 = std::make_shared<MockIterator>();
    MockIterPtr it2 = std::make_shared<MockIterator>();
    it1->set_postings(data);
    it2->set_postings(data);

    UnionTermIterator<MockIterator> uti({it1, it2});

    for (int32_t doc_id = 1; doc_id <= 1000; ++doc_id) {
        ASSERT_EQ(uti.advance(doc_id), doc_id);

        int32_t count = 0;
        for (int32_t i = 0; i < uti.freq(); i++) {
            count++;
        }
        EXPECT_EQ(count, 20);
    }
}

TEST(UnionTermIteratorTest, PositionExhaustionAndError) {
    MockIterPtr it1 = std::make_shared<MockIterator>();
    MockIterPtr it2 = std::make_shared<MockIterator>();
    it1->set_postings({{100, {10, 20, 30, 40, 50, 60}}});
    it2->set_postings({{100, {15, 25, 35, 45, 55, 65}}});

    UnionTermIterator<MockIterator> uti({it1, it2});
    uti.advance(100);

    std::vector<int32_t> positions;
    do {
        for (int32_t i = 0; i < uti.freq(); ++i) {
            int32_t pos = uti.next_position();
            positions.push_back(pos);
        }
    } while (uti.next_doc() != INT_MAX);

    EXPECT_EQ(positions, (std::vector<int32_t> {10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65}));

    EXPECT_EQ(positions.size(), 12);
    EXPECT_EQ(uti.freq(), 0);
}

} // namespace doris::segment_v2