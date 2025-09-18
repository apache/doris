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

#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris {

using segment_v2::inverted_index::query_v2::DoNothingCombiner;
using segment_v2::inverted_index::query_v2::DoNothingCombinerPtr;
using segment_v2::inverted_index::query_v2::EmptyScorer;
using segment_v2::inverted_index::query_v2::Scorer;
using segment_v2::inverted_index::query_v2::ScorerPtr;
using segment_v2::inverted_index::query_v2::SumCombiner;
using segment_v2::inverted_index::query_v2::SumCombinerPtr;

class ConstScorer final : public Scorer {
public:
    explicit ConstScorer(float v) : _v(v) {}
    ~ConstScorer() override = default;

    float score() override { return _v; }

private:
    float _v;
};

class ScoreCombinerTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ScoreCombinerTest, SumCombinerBasic) {
    SumCombiner comb;
    EXPECT_FLOAT_EQ(comb.score(), 0.0F);

    auto s1 = std::make_shared<ConstScorer>(1.5F);
    auto s2 = std::make_shared<ConstScorer>(2.25F);
    comb.update(s1);
    comb.update(s2);
    EXPECT_FLOAT_EQ(comb.score(), 3.75F);

    comb.clear();
    EXPECT_FLOAT_EQ(comb.score(), 0.0F);

    auto empty = std::make_shared<EmptyScorer>();
    comb.update(empty);
    EXPECT_FLOAT_EQ(comb.score(), 0.0F);
}

TEST_F(ScoreCombinerTest, SumCombinerCloneIndependence) {
    SumCombiner c1;
    c1.update(std::make_shared<ConstScorer>(5.0F));
    EXPECT_FLOAT_EQ(c1.score(), 5.0F);

    SumCombinerPtr c2 = c1.clone();
    ASSERT_TRUE(c2 != nullptr);
    EXPECT_FLOAT_EQ(c2->score(), 0.0F);

    c2->update(std::make_shared<ConstScorer>(1.0F));
    EXPECT_FLOAT_EQ(c2->score(), 1.0F);
    EXPECT_FLOAT_EQ(c1.score(), 5.0F);
}

TEST_F(ScoreCombinerTest, DoNothingCombinerBehavior) {
    DoNothingCombiner comb;
    EXPECT_FLOAT_EQ(comb.score(), 0.0F);

    comb.update(std::make_shared<ConstScorer>(10.0F));
    EXPECT_FLOAT_EQ(comb.score(), 0.0F);

    comb.clear();
    EXPECT_FLOAT_EQ(comb.score(), 0.0F);

    DoNothingCombinerPtr cloned = comb.clone();
    ASSERT_TRUE(cloned != nullptr);
    EXPECT_FLOAT_EQ(cloned->score(), 0.0F);

    cloned->update(std::make_shared<ConstScorer>(3.14F));
    EXPECT_FLOAT_EQ(cloned->score(), 0.0F);
}

} // namespace doris
