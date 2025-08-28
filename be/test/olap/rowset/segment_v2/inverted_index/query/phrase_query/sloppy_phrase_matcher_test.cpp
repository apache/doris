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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/sloppy_phrase_matcher.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"

namespace doris::segment_v2 {

MockIterPtr create_mock_iterator(const std::map<int32_t, std::vector<int32_t>>& postings) {
    auto iter = std::make_shared<MockIterator>();
    iter->set_postings(postings);
    return iter;
}

class SloppyPhraseMatcherTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(SloppyPhraseMatcherTest, BasicMatchNoRepeats) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 3, 5, 7, 9, 11}},
                                                         {1, {2, 4, 6, 8, 10, 12}}};
    std::map<int32_t, std::vector<int32_t>> postings2 = {{0, {2, 4, 6, 8, 10, 12}},
                                                         {1, {3, 5, 7, 9, 11, 13}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings2);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term2"})};

    SloppyPhraseMatcher matcher(postings, 1);
    matcher.reset(0);
    EXPECT_TRUE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, RepeatingTerms) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 3, 5, 7, 9, 11}},
                                                         {1, {2, 4, 6, 8, 10, 12}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings1);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term1"})};

    SloppyPhraseMatcher matcher(postings, 0);
    matcher.reset(0);
    EXPECT_FALSE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, SlopEffect) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 5, 9, 13, 17, 21}},
                                                         {1, {2, 6, 10, 14, 18, 22}}};
    std::map<int32_t, std::vector<int32_t>> postings2 = {{0, {4, 8, 12, 16, 20, 24}},
                                                         {1, {5, 9, 13, 17, 21, 25}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings2);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term2"})};

    SloppyPhraseMatcher matcher(postings, 1);
    matcher.reset(0);
    EXPECT_FALSE(matcher.next_match());

    SloppyPhraseMatcher matcher2(postings, 2);
    matcher2.reset(0);
    EXPECT_TRUE(matcher2.next_match());
}

TEST_F(SloppyPhraseMatcherTest, MultiTerms) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 4, 7, 10, 13, 16}},
                                                         {1, {2, 5, 8, 11, 14, 17}}};
    std::map<int32_t, std::vector<int32_t>> postings2 = {{0, {2, 5, 8, 11, 14, 17}},
                                                         {1, {3, 6, 9, 12, 15, 18}}};
    std::map<int32_t, std::vector<int32_t>> postings3 = {{0, {3, 6, 9, 12, 15, 18}},
                                                         {1, {4, 7, 10, 13, 16, 19}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings2);
    auto iter3 = create_mock_iterator(postings3);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term2"}),
                                             PostingsAndFreq(iter3, 2, {"term3"})};

    SloppyPhraseMatcher matcher(postings, 2);
    matcher.reset(0);
    EXPECT_TRUE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, PositionOverlap) {
    std::map<int32_t, std::vector<int32_t>> postings = {{0, {1, 2, 3, 4, 5, 6}},
                                                        {1, {1, 2, 3, 4, 5, 6}}};
    auto iter1 = create_mock_iterator(postings);
    auto iter2 = create_mock_iterator(postings);

    std::vector<PostingsAndFreq> postings_vec = {PostingsAndFreq(iter1, 0, {"term1"}),
                                                 PostingsAndFreq(iter2, 1, {"term2"})};

    SloppyPhraseMatcher matcher(postings_vec, 0);
    matcher.reset(0);
    EXPECT_TRUE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, NoMatch) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 3, 5, 7, 9, 11}},
                                                         {1, {2, 4, 6, 8, 10, 12}}};
    std::map<int32_t, std::vector<int32_t>> postings2 = {{0, {20, 22, 24, 26, 28, 30}},
                                                         {1, {11, 13, 15, 17, 19, 21}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings2);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term2"})};

    SloppyPhraseMatcher matcher(postings, 5);
    matcher.reset(0);
    EXPECT_FALSE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, SingleTerm) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 3, 5, 7, 9, 11}},
                                                         {1, {2, 4, 6, 8, 10, 12}}};
    auto iter1 = create_mock_iterator(postings1);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"})};

    SloppyPhraseMatcher matcher(postings, 0);
    matcher.reset(0);
    EXPECT_FALSE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, EmptyPostings) {
    std::vector<PostingsAndFreq> postings;
    SloppyPhraseMatcher matcher(postings, 0);
    matcher.reset(0);
    EXPECT_FALSE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, MultiplePositions) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 5, 9, 13, 17, 21}},
                                                         {1, {2, 6, 10, 14, 18, 22}}};
    std::map<int32_t, std::vector<int32_t>> postings2 = {{0, {2, 6, 10, 14, 18, 22}},
                                                         {1, {3, 7, 11, 15, 19, 23}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings2);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term2"})};

    SloppyPhraseMatcher matcher(postings, 1);
    matcher.reset(0);
    EXPECT_TRUE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, InterleavedPositions) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 3, 5, 7, 9, 11}},
                                                         {1, {2, 4, 6, 8, 10, 12}}};
    std::map<int32_t, std::vector<int32_t>> postings2 = {{0, {2, 4, 6, 8, 10, 12}},
                                                         {1, {3, 5, 7, 9, 11, 13}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings2);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term2"})};

    SloppyPhraseMatcher matcher(postings, 2);
    matcher.reset(0);
    EXPECT_TRUE(matcher.next_match());
}

TEST_F(SloppyPhraseMatcherTest, LargeSlop) {
    std::map<int32_t, std::vector<int32_t>> postings1 = {{0, {1, 5, 9, 13, 17, 21}},
                                                         {1, {2, 6, 10, 14, 18, 22}}};
    std::map<int32_t, std::vector<int32_t>> postings2 = {{0, {10, 14, 18, 22, 26, 30}},
                                                         {1, {11, 15, 19, 23, 27, 31}}};
    auto iter1 = create_mock_iterator(postings1);
    auto iter2 = create_mock_iterator(postings2);

    std::vector<PostingsAndFreq> postings = {PostingsAndFreq(iter1, 0, {"term1"}),
                                             PostingsAndFreq(iter2, 1, {"term2"})};

    SloppyPhraseMatcher matcher(postings, 10);
    matcher.reset(0);
    EXPECT_TRUE(matcher.next_match());
}

} // namespace doris::segment_v2