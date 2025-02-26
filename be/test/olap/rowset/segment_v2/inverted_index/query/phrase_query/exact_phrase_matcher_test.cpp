#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/exact_phrase_matcher.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"

using namespace testing;

namespace doris::segment_v2 {

class ExactPhraseMatcherTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(ExactPhraseMatcherTest, BasicMatch) {
    auto mockIter1 = std::make_shared<MockIterator>();
    mockIter1->set_postings({{1, {2, 5}}});
    auto mockIter2 = std::make_shared<MockIterator>();
    mockIter2->set_postings({{1, {3, 6}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(mockIter1, 0);
    postings.emplace_back(mockIter2, 1);

    ExactPhraseMatcher matcher(postings);
    matcher.reset(1);

    EXPECT_TRUE(matcher.next_match());
    EXPECT_TRUE(matcher.next_match());
    EXPECT_FALSE(matcher.next_match());
}

TEST_F(ExactPhraseMatcherTest, NoMatchDueToPositionGap) {
    auto mockIter1 = std::make_shared<MockIterator>();
    mockIter1->set_postings({{1, {2}}});
    auto mockIter2 = std::make_shared<MockIterator>();
    mockIter2->set_postings({{1, {4}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(mockIter1, 0);
    postings.emplace_back(mockIter2, 1);

    ExactPhraseMatcher matcher(postings);
    matcher.reset(1);

    EXPECT_FALSE(matcher.next_match());
}

TEST_F(ExactPhraseMatcherTest, DocIDMismatchThrows) {
    auto mockIter = std::make_shared<MockIterator>();
    mockIter->set_postings({{1, {2}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(mockIter, 0);

    ExactPhraseMatcher matcher(postings);

    EXPECT_THROW({ matcher.reset(2); }, Exception);
}

TEST_F(ExactPhraseMatcherTest, ThreeTermsMatch) {
    auto mockIter1 = std::make_shared<MockIterator>();
    mockIter1->set_postings({{1, {5}}});
    auto mockIter2 = std::make_shared<MockIterator>();
    mockIter2->set_postings({{1, {6}}});
    auto mockIter3 = std::make_shared<MockIterator>();
    mockIter3->set_postings({{1, {7}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(mockIter1, 0);
    postings.emplace_back(mockIter2, 1);
    postings.emplace_back(mockIter3, 2);

    ExactPhraseMatcher matcher(postings);
    matcher.reset(1);

    EXPECT_TRUE(matcher.next_match());
    EXPECT_FALSE(matcher.next_match());
}

TEST_F(ExactPhraseMatcherTest, MultipleMatchesWithSkips) {
    auto mockIter1 = std::make_shared<MockIterator>();
    mockIter1->set_postings({{1, {2, 4, 6}}});
    auto mockIter2 = std::make_shared<MockIterator>();
    mockIter2->set_postings({{1, {3, 5, 7}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(mockIter1, 0);
    postings.emplace_back(mockIter2, 1);

    ExactPhraseMatcher matcher(postings);
    matcher.reset(1);

    EXPECT_TRUE(matcher.next_match());
    EXPECT_TRUE(matcher.next_match());
    EXPECT_TRUE(matcher.next_match());
    EXPECT_FALSE(matcher.next_match());
}

TEST_F(ExactPhraseMatcherTest, PartialAdvanceFails) {
    auto mockIter1 = std::make_shared<MockIterator>();
    mockIter1->set_postings({{1, {2, 5}}});
    auto mockIter2 = std::make_shared<MockIterator>();
    mockIter2->set_postings({{1, {4}}});

    std::vector<PostingsAndPosition> postings;
    postings.emplace_back(mockIter1, 0);
    postings.emplace_back(mockIter2, 1);

    ExactPhraseMatcher matcher(postings);
    matcher.reset(1);

    EXPECT_FALSE(matcher.next_match());
}

} // namespace doris::segment_v2