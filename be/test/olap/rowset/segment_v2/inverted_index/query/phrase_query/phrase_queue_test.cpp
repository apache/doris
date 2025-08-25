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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/phrase_queue.h"

#include <gtest/gtest.h>

using namespace testing;

namespace doris::segment_v2 {

class PhraseQueueTest : public ::testing::Test {
protected:
    void SetUp() override {
        mock_iter = std::make_shared<MockIterator>();
        terms = {};
    }

    void TearDown() override {
        for (auto* pp : pointers) {
            delete pp;
        }
        pointers.clear();
    }

    PhrasePositions* createPP(int32_t position, int32_t offset, int32_t ord) {
        auto* pp = new PhrasePositions(mock_iter, offset, ord, terms);
        pp->_position = position;
        pointers.push_back(pp);
        return pp;
    }

    MockIterPtr mock_iter;
    std::vector<std::string> terms;
    std::vector<PhrasePositions*> pointers;
};

TEST_F(PhraseQueueTest, OrdersByPosition) {
    PhraseQueue queue(3);
    PhrasePositions* pp1 = createPP(5, 0, 0);
    PhrasePositions* pp2 = createPP(3, 0, 1);
    PhrasePositions* pp3 = createPP(7, 0, 2);

    queue.add(pp1);
    queue.add(pp2);
    queue.add(pp3);

    ASSERT_EQ(queue.size(), 3);
    EXPECT_EQ(queue.pop()->_position, 3);
    EXPECT_EQ(queue.pop()->_position, 5);
    EXPECT_EQ(queue.pop()->_position, 7);
}

TEST_F(PhraseQueueTest, OrdersByOffsetWhenPositionEqual) {
    PhraseQueue queue(2);
    PhrasePositions* pp1 = createPP(5, 2, 0);
    PhrasePositions* pp2 = createPP(5, 4, 1);

    queue.add(pp1);
    queue.add(pp2);

    ASSERT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.pop()->_offset, 2);
    EXPECT_EQ(queue.pop()->_offset, 4);
}

TEST_F(PhraseQueueTest, OrdersByOrdWhenPositionAndOffsetEqual) {
    PhraseQueue queue(2);
    PhrasePositions* pp1 = createPP(5, 3, 1);
    PhrasePositions* pp2 = createPP(5, 3, 2);

    queue.add(pp1);
    queue.add(pp2);

    ASSERT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.pop()->_ord, 1);
    EXPECT_EQ(queue.pop()->_ord, 2);
}

TEST_F(PhraseQueueTest, InsertWithOverflowReplacesWhenLarger) {
    PhraseQueue queue(2);
    PhrasePositions* pp1 = createPP(3, 0, 0);
    PhrasePositions* pp2 = createPP(5, 0, 1);
    PhrasePositions* pp3 = createPP(4, 0, 2);

    ASSERT_FALSE(queue.insert_with_overflow(pp1));
    ASSERT_FALSE(queue.insert_with_overflow(pp2));
    PhrasePositions* overflow = queue.insert_with_overflow(pp3);

    EXPECT_EQ(overflow->_position, 3);

    ASSERT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.pop()->_position, 4);
    EXPECT_EQ(queue.pop()->_position, 5);
}

TEST_F(PhraseQueueTest, UpdateTopReordersCorrectly) {
    PhraseQueue queue(3);
    PhrasePositions* pp1 = createPP(3, 0, 0);
    PhrasePositions* pp2 = createPP(5, 0, 1);
    PhrasePositions* pp3 = createPP(7, 0, 2);

    queue.add(pp1);
    queue.add(pp2);
    queue.add(pp3);

    pp1->_position = 6;
    queue.update_top();

    ASSERT_EQ(queue.size(), 3);
    EXPECT_EQ(queue.pop()->_position, 5);
    EXPECT_EQ(queue.pop()->_position, 6);
    EXPECT_EQ(queue.pop()->_position, 7);
}

} // namespace doris::segment_v2