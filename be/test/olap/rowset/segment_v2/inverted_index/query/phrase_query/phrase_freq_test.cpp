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

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/exact_phrase_matcher.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/ordered_sloppy_phrase_matcher.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/sloppy_phrase_matcher.h"
#include "olap/rowset/segment_v2/inverted_index/util/docid_set_iterator.h"
#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"
#include "olap/rowset/segment_v2/inverted_index/util/union_term_iterator.h"

namespace doris::segment_v2 {

using namespace inverted_index;

class PhraseFreqTest : public ::testing::Test {
protected:
    DISI create_mock_disi(std::map<int32_t, std::vector<int32_t>> postings) {
        auto mock = std::make_shared<MockIterator>();
        mock->set_postings(postings);
        return mock;
    }
};

TEST_F(PhraseFreqTest, ExactPhraseMatcher_SingleMatch) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 1.0F);
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_MultipleMatches) {
    auto disi1 = create_mock_disi({{1, {0, 2}}});
    auto disi2 = create_mock_disi({{1, {1, 3}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 2.0F);
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_NoMatch) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {2}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 0.0F);
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_ThreeTermPhrase) {
    auto disi1 = create_mock_disi({{1, {1}}});
    auto disi2 = create_mock_disi({{1, {2}}});
    auto disi3 = create_mock_disi({{1, {3}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);
    postings.emplace_back(disi3, 2);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 1.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_ExactMatch) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 2);
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 1.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_WithSlop) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {2}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 2);
    float freq = matcher.phrase_freq(1);

    EXPECT_GT(freq, 0.0F);
    EXPECT_LE(freq, 1.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_SloppyWeight_AfterMatch) {
    // Test sloppy_weight after next_match() returns true
    // Positions: term1 at 0, term2 at 1 (exact match, match_width = 0)
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 5);
    matcher.reset(1);
    ASSERT_TRUE(matcher.next_match());

    // match_width = 0 for exact match, so weight = 1/(1+0) = 1.0
    float weight = matcher.sloppy_weight();
    EXPECT_FLOAT_EQ(weight, 1.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_SloppyWeight_WithGap) {
    // Test sloppy_weight with gap between terms
    // Positions: term1 at 0, term2 at 3 (gap of 2, match_width = 2)
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {3}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 5);
    matcher.reset(1);
    ASSERT_TRUE(matcher.next_match());

    // match_width = 2, so weight = 1/(1+2) = 0.333...
    float weight = matcher.sloppy_weight();
    EXPECT_FLOAT_EQ(weight, 1.0F / 3.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_ExceedsSlop) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {5}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 2);
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 0.0F);
}

TEST_F(PhraseFreqTest, SloppyPhraseMatcher_ExactMatch) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    std::vector<PostingsAndFreq> postings;
    postings.emplace_back(disi1, 0, std::vector<std::string>{"big"});
    postings.emplace_back(disi2, 1, std::vector<std::string>{"red"});

    SloppyPhraseMatcher matcher(postings, 2);
    float freq = matcher.phrase_freq(1);

    EXPECT_GT(freq, 0.0F);
}

TEST_F(PhraseFreqTest, SloppyPhraseMatcher_SloppyWeight_AfterMatch) {
    // Test sloppy_weight after next_match() returns true
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    std::vector<PostingsAndFreq> postings;
    postings.emplace_back(disi1, 0, std::vector<std::string>{"big"});
    postings.emplace_back(disi2, 1, std::vector<std::string>{"red"});

    SloppyPhraseMatcher matcher(postings, 5);
    matcher.reset(1);
    ASSERT_TRUE(matcher.next_match());

    float weight = matcher.sloppy_weight();
    EXPECT_GT(weight, 0.0F);
    EXPECT_LE(weight, 1.0F);
}

TEST_F(PhraseFreqTest, SloppyPhraseMatcher_ReorderedTerms) {
    auto disi1 = create_mock_disi({{1, {1}}});
    auto disi2 = create_mock_disi({{1, {0}}});

    std::vector<PostingsAndFreq> postings;
    postings.emplace_back(disi1, 0, std::vector<std::string>{"big"});
    postings.emplace_back(disi2, 1, std::vector<std::string>{"red"});

    SloppyPhraseMatcher matcher(postings, 3);
    float freq = matcher.phrase_freq(1);

    EXPECT_GE(freq, 0.0F);
}

TEST_F(PhraseFreqTest, NormVisitor_MockIterator) {
    auto mock = std::make_shared<MockIterator>();
    mock->set_postings({{1, {0, 1, 2}}});

    int32_t norm = mock->norm();
    EXPECT_EQ(norm, 1);
}

TEST_F(PhraseFreqTest, NormVisitor_WithDISI) {
    auto disi = create_mock_disi({{1, {0}}});

    int32_t norm = visit_node(disi, Norm{});
    EXPECT_EQ(norm, 1);
}

TEST_F(PhraseFreqTest, UnionTermIterator_NormThrowsException) {
    auto mock1 = std::make_shared<MockIterator>();
    mock1->set_postings({{1, {0}}});
    auto mock2 = std::make_shared<MockIterator>();
    mock2->set_postings({{1, {1}}});

    std::vector<std::shared_ptr<MockIterator>> subs = {mock1, mock2};
    UnionTermIterator<MockIterator> union_iter(subs);

    EXPECT_THROW(union_iter.norm(), Exception);
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_MultipleDocuments) {
    {
        auto d1 = create_mock_disi({{1, {0}}});
        auto d2 = create_mock_disi({{1, {1}}});
        std::vector<PostingsAndPosition> postings;
        postings.emplace_back(d1, 0);
        postings.emplace_back(d2, 1);
        ExactPhraseMatcher matcher(std::move(postings));
        EXPECT_FLOAT_EQ(matcher.phrase_freq(1), 1.0F);
    }

    {
        auto d1 = create_mock_disi({{3, {0, 2}}});
        auto d2 = create_mock_disi({{3, {1, 3}}});
        std::vector<PostingsAndPosition> postings;
        postings.emplace_back(d1, 0);
        postings.emplace_back(d2, 1);
        ExactPhraseMatcher matcher(std::move(postings));
        EXPECT_FLOAT_EQ(matcher.phrase_freq(3), 2.0F);
    }
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_MultipleMatches) {
    auto disi1 = create_mock_disi({{1, {0, 3}}});
    auto disi2 = create_mock_disi({{1, {2, 5}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 3);
    float freq = matcher.phrase_freq(1);

    EXPECT_GT(freq, 0.0F);
}

// ==================== Additional Coverage Tests ====================

TEST_F(PhraseFreqTest, ExactPhraseMatcher_FourTermPhrase) {
    // Test longer phrase matching
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});
    auto disi3 = create_mock_disi({{1, {2}}});
    auto disi4 = create_mock_disi({{1, {3}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);
    postings.emplace_back(disi3, 2);
    postings.emplace_back(disi4, 3);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 1.0F);
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_PartialMatch) {
    // First two terms match but third doesn't
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});
    auto disi3 = create_mock_disi({{1, {5}}}); // Gap breaks the phrase

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);
    postings.emplace_back(disi3, 2);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 0.0F);
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_OverlappingMatches) {
    // Positions that could form overlapping phrases: "a b a b"
    // Phrase "a b" appears at positions (0,1) and (2,3)
    auto disi1 = create_mock_disi({{1, {0, 2}}});
    auto disi2 = create_mock_disi({{1, {1, 3}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 2.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_ThreeTerms) {
    // Three term phrase with slop
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {2}}});
    auto disi3 = create_mock_disi({{1, {4}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);
    postings.emplace_back(disi3, 2);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 3);
    float freq = matcher.phrase_freq(1);

    EXPECT_GT(freq, 0.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_ThreeTerms_ExceedsSlop) {
    // Three term phrase exceeds slop
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {5}}});
    auto disi3 = create_mock_disi({{1, {10}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);
    postings.emplace_back(disi3, 2);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 2);
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 0.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_PhraseFreqAccumulation) {
    // Multiple matches with different sloppy weights
    // Match 1: positions 0, 1 (match_width=0, weight=1.0)
    // Match 2: positions 3, 5 (match_width=1, weight=0.5)
    auto disi1 = create_mock_disi({{1, {0, 3}}});
    auto disi2 = create_mock_disi({{1, {1, 5}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 5);
    float freq = matcher.phrase_freq(1);

    // Expected: 1.0 + 0.5 = 1.5
    EXPECT_FLOAT_EQ(freq, 1.5F);
}

TEST_F(PhraseFreqTest, SloppyPhraseMatcher_NoMatch) {
    // Terms too far apart
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {100}}});

    std::vector<PostingsAndFreq> postings;
    postings.emplace_back(disi1, 0, std::vector<std::string>{"hello"});
    postings.emplace_back(disi2, 1, std::vector<std::string>{"world"});

    SloppyPhraseMatcher matcher(postings, 2);
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 0.0F);
}

TEST_F(PhraseFreqTest, SloppyPhraseMatcher_ThreeTerms) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});
    auto disi3 = create_mock_disi({{1, {2}}});

    std::vector<PostingsAndFreq> postings;
    postings.emplace_back(disi1, 0, std::vector<std::string>{"the"});
    postings.emplace_back(disi2, 1, std::vector<std::string>{"quick"});
    postings.emplace_back(disi3, 2, std::vector<std::string>{"fox"});

    SloppyPhraseMatcher matcher(postings, 3);
    float freq = matcher.phrase_freq(1);

    EXPECT_GT(freq, 0.0F);
}

TEST_F(PhraseFreqTest, SloppyPhraseMatcher_MultipleMatches) {
    // Document with phrase appearing twice
    auto disi1 = create_mock_disi({{1, {0, 5}}});
    auto disi2 = create_mock_disi({{1, {1, 6}}});

    std::vector<PostingsAndFreq> postings;
    postings.emplace_back(disi1, 0, std::vector<std::string>{"hello"});
    postings.emplace_back(disi2, 1, std::vector<std::string>{"world"});

    SloppyPhraseMatcher matcher(postings, 2);
    float freq = matcher.phrase_freq(1);

    EXPECT_GT(freq, 1.0F); // Should have accumulated weight from multiple matches
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_Matches_Consistency) {
    // Verify matches() and phrase_freq() are consistent
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    {
        std::vector<PostingsAndPosition> postings;
        postings.emplace_back(disi1, 0);
        postings.emplace_back(disi2, 1);
        ExactPhraseMatcher matcher(std::move(postings));
        EXPECT_TRUE(matcher.matches(1));
    }

    // Reset iterators
    disi1 = create_mock_disi({{1, {0}}});
    disi2 = create_mock_disi({{1, {1}}});

    {
        std::vector<PostingsAndPosition> postings;
        postings.emplace_back(disi1, 0);
        postings.emplace_back(disi2, 1);
        ExactPhraseMatcher matcher(std::move(postings));
        EXPECT_GT(matcher.phrase_freq(1), 0.0F);
    }
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_Matches_Consistency) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {2}}});

    {
        std::vector<PostingsAndPosition> postings;
        postings.emplace_back(disi1, 0);
        postings.emplace_back(disi2, 1);
        OrderedSloppyPhraseMatcher matcher(std::move(postings), 3);
        EXPECT_TRUE(matcher.matches(1));
    }

    disi1 = create_mock_disi({{1, {0}}});
    disi2 = create_mock_disi({{1, {2}}});

    {
        std::vector<PostingsAndPosition> postings;
        postings.emplace_back(disi1, 0);
        postings.emplace_back(disi2, 1);
        OrderedSloppyPhraseMatcher matcher(std::move(postings), 3);
        EXPECT_GT(matcher.phrase_freq(1), 0.0F);
    }
}

TEST_F(PhraseFreqTest, SloppyPhraseMatcher_Matches_Consistency) {
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    {
        std::vector<PostingsAndFreq> postings;
        postings.emplace_back(disi1, 0, std::vector<std::string>{"a"});
        postings.emplace_back(disi2, 1, std::vector<std::string>{"b"});
        SloppyPhraseMatcher matcher(postings, 2);
        EXPECT_TRUE(matcher.matches(1));
    }

    disi1 = create_mock_disi({{1, {0}}});
    disi2 = create_mock_disi({{1, {1}}});

    {
        std::vector<PostingsAndFreq> postings;
        postings.emplace_back(disi1, 0, std::vector<std::string>{"a"});
        postings.emplace_back(disi2, 1, std::vector<std::string>{"b"});
        SloppyPhraseMatcher matcher(postings, 2);
        EXPECT_GT(matcher.phrase_freq(1), 0.0F);
    }
}

TEST_F(PhraseFreqTest, ExactPhraseMatcher_HighFrequencyTerms) {
    // Test with terms appearing many times
    auto disi1 = create_mock_disi({{1, {0, 2, 4, 6, 8}}});
    auto disi2 = create_mock_disi({{1, {1, 3, 5, 7, 9}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    ExactPhraseMatcher matcher(std::move(postings));
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 5.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_ZeroSlop) {
    // Zero slop should behave like exact match
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {1}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 0);
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 1.0F);
}

TEST_F(PhraseFreqTest, OrderedSloppyPhraseMatcher_ZeroSlop_NoMatch) {
    // Zero slop with gap should not match
    auto disi1 = create_mock_disi({{1, {0}}});
    auto disi2 = create_mock_disi({{1, {2}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(disi1, 0);
    postings.emplace_back(disi2, 1);

    OrderedSloppyPhraseMatcher matcher(std::move(postings), 0);
    float freq = matcher.phrase_freq(1);

    EXPECT_FLOAT_EQ(freq, 0.0F);
}

} // namespace doris::segment_v2
