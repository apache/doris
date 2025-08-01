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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/phrase_matcher.h"

#include <gtest/gtest.h>

namespace doris::segment_v2 {

class PostingsTest : public ::testing::Test {
protected:
    DISI create_mock_disi(std::map<int32_t, std::vector<int32_t>> postings) {
        auto mock = std::make_shared<MockIterator>();
        mock->set_postings(postings);
        return mock;
    }
};

TEST_F(PostingsTest, PostingsAndFreq_Basic) {
    auto disi = create_mock_disi({{1, {5}}});
    std::vector<std::string> terms {"zterm", "aterm"};

    PostingsAndFreq pf(disi, 10, terms);

    // 验证基础属性
    EXPECT_EQ(pf._position, 10);
    EXPECT_EQ(pf._n_terms, 2);

    // 验证terms排序
    ASSERT_EQ(pf._terms.size(), 2);
    EXPECT_EQ(pf._terms[0], "aterm");
    EXPECT_EQ(pf._terms[1], "zterm");

    // 验证postings类型
    EXPECT_TRUE(std::holds_alternative<MockIterPtr>(pf._postings));
}

TEST_F(PostingsTest, PostingsAndFreq_SingleTerm) {
    auto disi = create_mock_disi({{2, {8}}});
    std::vector<std::string> terms {"single"};

    PostingsAndFreq pf(disi, 5, terms);

    // 验证单term不排序
    EXPECT_EQ(pf._n_terms, 1);
    ASSERT_EQ(pf._terms.size(), 1);
    EXPECT_EQ(pf._terms[0], "single");
}

TEST_F(PostingsTest, PostingsAndFreq_EdgeCases) {
    {
        auto disi = create_mock_disi({});
        PostingsAndFreq pf(disi, 0, {});
        EXPECT_EQ(pf._n_terms, 0);
        EXPECT_TRUE(pf._terms.empty());
    }
    {
        auto disi = create_mock_disi({{3, {100}}});
        PostingsAndFreq pf(disi, INT32_MAX, {"max"});
        EXPECT_EQ(pf._position, INT32_MAX);
    }
}

TEST_F(PostingsTest, PostingsAndPosition_Init) {
    auto disi = create_mock_disi({{1, {5, 10}}});

    PostingsAndPosition pp(disi, 3);

    // 验证基础属性
    EXPECT_EQ(pp._offset, 3);
    EXPECT_EQ(pp._freq, 0);
    EXPECT_EQ(pp._upTo, 0);
    EXPECT_EQ(pp._pos, 0);

    auto& mock = *std::get<MockIterPtr>(pp._postings);
    EXPECT_EQ(mock.doc_id(), 1);
}

TEST_F(PostingsTest, PostingsAndPosition_WithAdvance) {
    auto disi = create_mock_disi({{1, {5}}, {5, {10}}});

    PostingsAndPosition pp(disi, 2);

    auto& mock = *std::get<MockIterPtr>(pp._postings);
    mock.advance(3);

    EXPECT_EQ(pp._offset, 2);
    EXPECT_EQ(mock.doc_id(), 5);
}

TEST_F(PostingsTest, PostingsAndPosition_NegativeOffset) {
    auto disi = create_mock_disi({{2, {20}}});

    PostingsAndPosition pp(disi, -5);

    EXPECT_EQ(pp._offset, -5);
}

} // namespace doris::segment_v2