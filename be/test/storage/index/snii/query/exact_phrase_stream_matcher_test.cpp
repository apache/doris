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

#include "storage/index/snii/query/internal/exact_phrase_stream_matcher.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <map>
#include <numeric>
#include <span>
#include <utility>
#include <vector>

#include "common/status.h"

namespace doris::snii::query::internal {
namespace {

using DocumentPositions = std::map<uint32_t, std::vector<uint32_t>>;

DocumentPositions doc_positions(
        std::initializer_list<std::pair<const uint32_t, std::vector<uint32_t>>> positions) {
    return {positions};
}

class FakeCursor {
public:
    enum class FailurePoint {
        kNone,
        kSeek,
        kNextPosition,
        kFinishDocIo,
        kFinishDocInvalidArgument
    };

    explicit FakeCursor(DocumentPositions positions, FailurePoint failure = FailurePoint::kNone)
            : positions_(std::move(positions)), failure_(failure) {}

    Status seek(uint32_t docid) {
        if (failure_ == FailurePoint::kSeek) {
            return Status::IOError<false>("injected seek failure");
        }
        const auto document = positions_.find(docid);
        active_positions_ = document == positions_.end() ? &empty_positions_ : &document->second;
        next_position_ = 0;
        active_ = true;
        doc_finished_ = false;
        return Status::OK();
    }

    Status next_position(uint32_t* position, bool* available) {
        if (failure_ == FailurePoint::kNextPosition) {
            return Status::IOError<false>("injected position failure");
        }
        if (next_position_ == active_positions_->size()) {
            *available = false;
            return Status::OK();
        }
        *position = (*active_positions_)[next_position_++];
        returned_position_values_.push_back(*position);
        *available = true;
        return Status::OK();
    }

    Status finish_doc() {
        ++finish_doc_calls_;
        if (failure_ == FailurePoint::kFinishDocIo) {
            return Status::IOError<false>("injected finish_doc failure");
        }
        if (failure_ == FailurePoint::kFinishDocInvalidArgument) {
            return Status::InvalidArgument<false>("injected later finish_doc failure");
        }
        active_ = false;
        doc_finished_ = true;
        return Status::OK();
    }

    [[maybe_unused]] Status finish() {
        ++finish_calls_;
        return Status::OK();
    }

    [[nodiscard]] size_t returned_positions() const { return returned_position_values_.size(); }
    [[nodiscard]] const std::vector<uint32_t>& returned_position_values() const {
        return returned_position_values_;
    }
    [[nodiscard]] size_t finish_doc_calls() const { return finish_doc_calls_; }
    [[nodiscard]] size_t finish_calls() const { return finish_calls_; }
    [[nodiscard]] bool doc_finished() const { return doc_finished_; }
    [[nodiscard]] bool active() const { return active_; }

private:
    DocumentPositions positions_;
    FailurePoint failure_ = FailurePoint::kNone;
    const std::vector<uint32_t>* active_positions_ = nullptr;
    std::vector<uint32_t> empty_positions_;
    size_t next_position_ = 0;
    std::vector<uint32_t> returned_position_values_;
    size_t finish_doc_calls_ = 0;
    size_t finish_calls_ = 0;
    bool active_ = false;
    bool doc_finished_ = false;
};

template <size_t ClauseCount>
void expect_immediate_match() {
    std::vector<FakeCursor> cursors;
    cursors.reserve(ClauseCount);
    for (uint32_t clause = 0; clause < ClauseCount; ++clause) {
        cursors.emplace_back(doc_positions({{9, {100 + clause, 200 + clause}}}));
    }
    std::array<size_t, ClauseCount> plan;
    std::iota(plan.begin(), plan.end(), 0U);
    std::array<uint32_t, ClauseCount> offsets;
    std::iota(offsets.begin(), offsets.end(), 0U);

    bool matched = false;
    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 9, &matched);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(matched);
    for (const FakeCursor& cursor : cursors) {
        EXPECT_EQ(cursor.returned_positions(), 1U);
        EXPECT_EQ(cursor.finish_doc_calls(), 1U);
        EXPECT_EQ(cursor.finish_calls(), 0U);
        EXPECT_TRUE(cursor.doc_finished());
        EXPECT_FALSE(cursor.active());
    }
}

TEST(ExactPhraseStreamMatcherTest, StopsAfterFirstMultiTermMatch) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{7, {0, 10, 20}}}));
    cursors.emplace_back(doc_positions({{7, {1, 11, 21}}}));
    cursors.emplace_back(doc_positions({{7, {2, 12, 22}}}));
    const std::array<size_t, 3> plan = {0, 1, 2};
    const std::array<uint32_t, 3> offsets = {0, 1, 2};
    bool matched = false;

    ASSERT_TRUE(match_exact_phrase_document(std::span(cursors), std::span(plan), std::span(offsets),
                                            7, &matched)
                        .ok());

    EXPECT_TRUE(matched);
    EXPECT_EQ(cursors[0].returned_positions(), 1U);
    EXPECT_EQ(cursors[1].returned_positions(), 1U);
    EXPECT_EQ(cursors[2].returned_positions(), 1U);
    for (const FakeCursor& cursor : cursors) {
        EXPECT_EQ(cursor.finish_doc_calls(), 1U);
        EXPECT_TRUE(cursor.doc_finished());
    }
}

TEST(ExactPhraseStreamMatcherTest, MatchesTwoSixAndTenTerms) {
    expect_immediate_match<2>();
    expect_immediate_match<6>();
    expect_immediate_match<10>();
}

TEST(ExactPhraseStreamMatcherTest, ReusesOvershootingPositionAfterAligningLead) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{11, {0, 4, 10}}}));
    cursors.emplace_back(doc_positions({{11, {5, 11}}}));
    cursors.emplace_back(doc_positions({{11, {6, 12}}}));
    const std::array<size_t, 3> plan = {0, 1, 2};
    const std::array<uint32_t, 3> offsets = {0, 1, 2};
    bool matched = false;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 11, &matched);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(matched);
    EXPECT_EQ(cursors[0].returned_positions(), 2U);
    EXPECT_EQ(cursors[1].returned_positions(), 1U);
    EXPECT_EQ(cursors[2].returned_positions(), 1U);
    EXPECT_EQ(cursors[0].returned_position_values(), (std::vector<uint32_t> {0, 4}));
    EXPECT_EQ(cursors[1].returned_position_values(), (std::vector<uint32_t> {5}));
    EXPECT_EQ(cursors[2].returned_position_values(), (std::vector<uint32_t> {6}));
    EXPECT_TRUE(std::ranges::all_of(cursors, &FakeCursor::doc_finished));
}

TEST(ExactPhraseStreamMatcherTest, FindsLateMatchAfterLeadOvershoot) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{13, {0, 10, 20}}}));
    cursors.emplace_back(doc_positions({{13, {5, 11, 21}}}));
    cursors.emplace_back(doc_positions({{13, {12, 22}}}));
    const std::array<size_t, 3> plan = {0, 1, 2};
    const std::array<uint32_t, 3> offsets = {0, 1, 2};
    bool matched = false;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 13, &matched);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(matched);
    EXPECT_EQ(cursors[0].returned_positions(), 2U);
    EXPECT_EQ(cursors[1].returned_positions(), 2U);
    EXPECT_EQ(cursors[2].returned_positions(), 1U);
    EXPECT_EQ(cursors[0].returned_position_values(), (std::vector<uint32_t> {0, 10}));
    EXPECT_EQ(cursors[1].returned_position_values(), (std::vector<uint32_t> {5, 11}));
    EXPECT_EQ(cursors[2].returned_position_values(), (std::vector<uint32_t> {12}));
    EXPECT_TRUE(std::ranges::all_of(cursors, &FakeCursor::doc_finished));
}

TEST(ExactPhraseStreamMatcherTest, FinishesAllCursorsWhenNoPhraseMatches) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{17, {0, 10}}}));
    cursors.emplace_back(doc_positions({{17, {3, 13}}}));
    const std::array<size_t, 2> plan = {0, 1};
    const std::array<uint32_t, 2> offsets = {0, 1};
    bool matched = true;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 17, &matched);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(matched);
    EXPECT_EQ(cursors[0].returned_positions(), 2U);
    EXPECT_EQ(cursors[1].returned_positions(), 2U);
    EXPECT_EQ(cursors[0].returned_position_values(), (std::vector<uint32_t> {0, 10}));
    EXPECT_EQ(cursors[1].returned_position_values(), (std::vector<uint32_t> {3, 13}));
    for (const FakeCursor& cursor : cursors) {
        EXPECT_EQ(cursor.finish_doc_calls(), 1U);
        EXPECT_EQ(cursor.finish_calls(), 0U);
        EXPECT_TRUE(cursor.doc_finished());
    }
}

TEST(ExactPhraseStreamMatcherTest, FinishesEveryCursorWhenOneCursorIsExhausted) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{19, {0, 10}}}));
    cursors.emplace_back(doc_positions({{19, {1, 11}}}));
    cursors.emplace_back(doc_positions({{19, {}}}));
    const std::array<size_t, 3> plan = {0, 1, 2};
    const std::array<uint32_t, 3> offsets = {0, 1, 2};
    bool matched = true;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 19, &matched);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(matched);
    EXPECT_EQ(cursors[0].returned_positions(), 1U);
    EXPECT_EQ(cursors[1].returned_positions(), 1U);
    EXPECT_EQ(cursors[2].returned_positions(), 0U);
    for (const FakeCursor& cursor : cursors) {
        EXPECT_EQ(cursor.finish_doc_calls(), 1U);
        EXPECT_TRUE(cursor.doc_finished());
    }
}

TEST(ExactPhraseStreamMatcherTest, MatchesWithNonzeroPositionOffsets) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{23, {100}}}));
    cursors.emplace_back(doc_positions({{23, {102}}}));
    cursors.emplace_back(doc_positions({{23, {104}}}));
    const std::array<size_t, 3> plan = {0, 1, 2};
    const std::array<uint32_t, 3> offsets = {5, 7, 9};
    bool matched = false;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 23, &matched);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(matched);
    EXPECT_TRUE(std::ranges::all_of(cursors, &FakeCursor::doc_finished));
}

TEST(ExactPhraseStreamMatcherTest, TreatsExpectedPositionOverflowAsCleanNoMatch) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{29, {std::numeric_limits<uint32_t>::max() - 1}}}));
    cursors.emplace_back(doc_positions({{29, {std::numeric_limits<uint32_t>::max()}}}));
    const std::array<size_t, 2> plan = {0, 1};
    const std::array<uint32_t, 2> offsets = {7, 9};
    bool matched = true;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 29, &matched);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(matched);
    EXPECT_EQ(cursors[0].returned_positions(), 1U);
    EXPECT_EQ(cursors[1].returned_positions(), 0U);
    EXPECT_TRUE(std::ranges::all_of(cursors, &FakeCursor::doc_finished));
}

TEST(ExactPhraseStreamMatcherTest, PropagatesSeekError) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{31, {0}}}));
    cursors.emplace_back(doc_positions({{31, {1}}}), FakeCursor::FailurePoint::kSeek);
    const std::array<size_t, 2> plan = {0, 1};
    const std::array<uint32_t, 2> offsets = {0, 1};
    bool matched = true;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 31, &matched);

    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>()) << status.to_string();
}

TEST(ExactPhraseStreamMatcherTest, PropagatesPositionError) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{37, {0}}}));
    cursors.emplace_back(doc_positions({{37, {1}}}), FakeCursor::FailurePoint::kNextPosition);
    const std::array<size_t, 2> plan = {0, 1};
    const std::array<uint32_t, 2> offsets = {0, 1};
    bool matched = true;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 37, &matched);

    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>()) << status.to_string();
}

TEST(ExactPhraseStreamMatcherTest, AttemptsEveryFinishAndReturnsFirstError) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{41, {0}}}), FakeCursor::FailurePoint::kFinishDocIo);
    cursors.emplace_back(doc_positions({{41, {1}}}));
    cursors.emplace_back(doc_positions({{41, {2}}}),
                         FakeCursor::FailurePoint::kFinishDocInvalidArgument);
    const std::array<size_t, 3> plan = {0, 1, 2};
    const std::array<uint32_t, 3> offsets = {0, 1, 2};
    bool matched = false;

    const Status status = match_exact_phrase_document(std::span(cursors), std::span(plan),
                                                      std::span(offsets), 41, &matched);

    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>()) << status.to_string();
    EXPECT_TRUE(matched);
    EXPECT_EQ(cursors[0].finish_doc_calls(), 1U);
    EXPECT_EQ(cursors[1].finish_doc_calls(), 1U);
    EXPECT_EQ(cursors[2].finish_doc_calls(), 1U);
}

TEST(ExactPhraseStreamMatcherTest, RejectsRepeatedCursorIndices) {
    std::vector<FakeCursor> cursors;
    cursors.emplace_back(doc_positions({{43, {0, 1}}}));
    const std::array<size_t, 2> repeated_plan = {0, 0};
    const std::array<uint32_t, 2> offsets = {0, 1};
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(validate_exact_phrase_stream_inputs(
                         std::span(cursors), std::span(repeated_plan), std::span(offsets))),
                 "");
}

} // namespace
} // namespace doris::snii::query::internal
