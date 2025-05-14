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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/phrase_positions.h"

#include <gtest/gtest.h>

namespace doris::segment_v2 {

class PhrasePositionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        mock1 = std::make_shared<MockIterator>();
        mock2 = std::make_shared<MockIterator>();
    }

    void TearDown() override {}

    DISI create_mock_disi(std::map<int32_t, std::vector<int32_t>> postings) {
        auto mock = std::make_shared<MockIterator>();
        mock->set_postings(postings);
        return mock;
    }

    std::shared_ptr<MockIterator> mock1;
    std::shared_ptr<MockIterator> mock2;
};

TEST_F(PhrasePositionsTest, BasicFunctionality) {
    auto disi = create_mock_disi({{1, {5, 10}}});

    PhrasePositions pp(disi, 2, 1, {"test"});

    EXPECT_EQ(pp._offset, 2);
    EXPECT_EQ(pp._ord, 1);
    ASSERT_EQ(pp._terms.size(), 1);
    EXPECT_EQ(pp._terms[0], "test");
    EXPECT_EQ(pp._position, 0);
    EXPECT_EQ(pp._count, 0);

    pp.first_position();
    EXPECT_EQ(pp._count, 1);
    EXPECT_EQ(pp._position, 5 - 2);
    EXPECT_TRUE(pp.next_position());
    EXPECT_EQ(pp._position, 10 - 2);
    EXPECT_FALSE(pp.next_position());
}

TEST_F(PhrasePositionsTest, MultipleDocuments) {
    auto disi = create_mock_disi({{1, {2, 5}}, {3, {8, 10, 12}}});

    PhrasePositions pp(disi, 1, 0, {"multi"});

    pp.first_position();
    EXPECT_EQ(pp._count, 1);
    EXPECT_EQ(pp._position, 2 - 1);
    EXPECT_TRUE(pp.next_position());
    EXPECT_EQ(pp._position, 5 - 1);

    std::get<MockIterPtr>(pp._postings)->next_doc();
    pp.first_position();
    EXPECT_EQ(pp._count, 2);
    EXPECT_EQ(pp._position, 8 - 1);
    EXPECT_TRUE(pp.next_position());
    EXPECT_EQ(pp._position, 10 - 1);
}

TEST_F(PhrasePositionsTest, EdgeCases) {
    {
        auto disi = create_mock_disi({{2, {100, 200}}});
        PhrasePositions pp(disi, 0, 2, {"zero"});
        pp.first_position();
        pp.next_position();
        EXPECT_EQ(pp._position, 200 - 0);
    }

    {
        auto disi = create_mock_disi({{5, {50}}});
        PhrasePositions pp(disi, -3, 3, {"negative"});
        pp.first_position();
        pp.next_position();
        EXPECT_EQ(pp._position, 50 - (-3));
    }
}

TEST_F(PhrasePositionsTest, EmptyPositions) {
    auto disi = create_mock_disi({});
    PhrasePositions pp(disi, 0, 0, {"empty"});

    pp.first_position();
    EXPECT_EQ(pp._count, -1);
    EXPECT_FALSE(pp.next_position());
}

TEST_F(PhrasePositionsTest, PositionSequence) {
    auto disi = create_mock_disi({{1, {10, 20, 30}}, {5, {100}}});

    PhrasePositions pp(disi, 5, 0, {"sequence"});

    pp.first_position();
    EXPECT_EQ(pp._count, 2);
    EXPECT_EQ(pp._position, 5);
    EXPECT_TRUE(pp.next_position());
    EXPECT_EQ(pp._position, 15);
    EXPECT_TRUE(pp.next_position());
    EXPECT_EQ(pp._position, 25);
    EXPECT_FALSE(pp.next_position());

    std::get<MockIterPtr>(pp._postings)->advance(5);
    pp.first_position();
    EXPECT_EQ(pp._count, 0);
    EXPECT_EQ(pp._position, 95);
    EXPECT_FALSE(pp.next_position());
}

TEST_F(PhrasePositionsTest, MultipleTerms) {
    auto disi = create_mock_disi({{1, {5}}});
    PhrasePositions pp(disi, 0, 0, {"term1", "term2"});

    pp.first_position();
    EXPECT_EQ(pp._count, 0);
    EXPECT_EQ(pp._position, 5);
    EXPECT_FALSE(pp.next_position());

    ASSERT_EQ(pp._terms.size(), 2);
    EXPECT_EQ(pp._terms[0], "term1");
    EXPECT_EQ(pp._terms[1], "term2");
}

} // namespace doris::segment_v2