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

#include "storage/index/query/phrase/sloppy_phrase_matcher.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <vector>

namespace doris::index_query {
namespace {

std::vector<PhrasePositionSpan> make_spans(const std::vector<std::vector<uint32_t>>& positions) {
    std::vector<PhrasePositionSpan> spans;
    spans.reserve(positions.size());
    for (const auto& clause : positions) {
        spans.emplace_back(clause.data(), clause.data() + clause.size());
    }
    return spans;
}

std::vector<uint32_t> generate_positions(std::mt19937* generator) {
    std::uniform_int_distribution<uint32_t> frequency_distribution(1, 5);
    std::uniform_int_distribution<uint32_t> gap_distribution(1, 4);
    const uint32_t frequency = frequency_distribution(*generator);
    std::vector<uint32_t> positions;
    positions.reserve(frequency);
    uint32_t position = gap_distribution(*generator) - 1;
    while (positions.size() < frequency) {
        positions.push_back(position);
        position += gap_distribution(*generator);
    }
    return positions;
}

TEST(SniiSloppyPhraseMatcher, UnorderedTranspositionCostsTwoPositions) {
    const std::vector<size_t> plan_index {0, 1};
    const std::vector<uint32_t> offsets {0, 1};
    const std::vector<std::vector<uint32_t>> positions {{1}, {0}};
    const auto spans = make_spans(positions);

    SloppyPhraseMatcher below_threshold(plan_index, offsets, 1, false);
    EXPECT_EQ(below_threshold.match(spans, false), 0.0F);

    SloppyPhraseMatcher at_threshold(plan_index, offsets, 2, false);
    EXPECT_EQ(at_threshold.match(spans, false), 1.0F);
    EXPECT_FLOAT_EQ(at_threshold.match(spans, true), 1.0F / 3.0F);
}

TEST(SniiSloppyPhraseMatcher, RepeatedClauseCannotReuseOneOccurrence) {
    const std::vector<size_t> plan_index {0, 0};
    const std::vector<uint32_t> offsets {0, 1};
    const std::vector<uint32_t> repeated_positions {4};
    const std::vector<PhrasePositionSpan> spans {
            {repeated_positions.data(), repeated_positions.data() + repeated_positions.size()},
            {repeated_positions.data(), repeated_positions.data() + repeated_positions.size()}};

    SloppyPhraseMatcher matcher(plan_index, offsets, 100, false);
    EXPECT_EQ(matcher.match(spans, false), 0.0F);
}

TEST(SniiSloppyPhraseMatcher, RepeatedClauseUsesDistinctOccurrences) {
    const std::vector<size_t> plan_index {0, 0};
    const std::vector<uint32_t> offsets {0, 1};
    const std::vector<uint32_t> repeated_positions {3, 5, 7};
    const std::vector<PhrasePositionSpan> spans {
            {repeated_positions.data(), repeated_positions.data() + repeated_positions.size()},
            {repeated_positions.data(), repeated_positions.data() + repeated_positions.size()}};

    SloppyPhraseMatcher matcher(plan_index, offsets, 1, false);
    EXPECT_EQ(matcher.match(spans, false), 1.0F);
}

TEST(SniiSloppyPhraseMatcher, UnorderedMultiTermUsesWholeAdjustedWindow) {
    const std::vector<size_t> plan_index {0, 1, 2};
    const std::vector<uint32_t> offsets {0, 1, 2};
    const std::vector<std::vector<uint32_t>> positions {{0}, {2}, {1}};
    const auto spans = make_spans(positions);

    SloppyPhraseMatcher matcher(plan_index, offsets, 2, false);
    EXPECT_FLOAT_EQ(matcher.match(spans, true), 1.0F / 3.0F);
}

TEST(SniiSloppyPhraseMatcher, OrderedMatcherAccumulatesGapsAndFrequencies) {
    const std::vector<size_t> plan_index {0, 1};
    const std::vector<uint32_t> offsets {0, 1};
    const std::vector<std::vector<uint32_t>> positions {{1, 5}, {3, 7}};
    const auto spans = make_spans(positions);

    SloppyPhraseMatcher matcher(plan_index, offsets, 1, true);
    EXPECT_FLOAT_EQ(matcher.match(spans, true), 1.0F);
}

// Frequencies the legacy CLucene (V3) sloppy and ordered matchers computed for these cases.
TEST(SniiSloppyPhraseMatcher, FrequenciesMatchV3Values) {
    struct UnorderedCase {
        std::vector<size_t> plan_index;
        std::vector<std::vector<uint32_t>> positions;
        std::vector<float> expected; // For slops 1, 2 and 4.
    };
    const std::vector<UnorderedCase> unordered_cases {
            {.plan_index = {0, 1},
             .positions = {{1, 5, 9}, {3, 7, 11}},
             .expected = {1.5F, 1.5F, 2.0F}},
            {.plan_index = {0, 1}, .positions = {{1}, {0}}, .expected = {0.0F, 1.0F / 3, 1.0F / 3}},
            {.plan_index = {0, 1, 2},
             .positions = {{0, 4}, {2, 6}, {1, 8}},
             .expected = {0.0F, 2.0F / 3, 2.0F / 3}},
            {.plan_index = {0, 0},
             .positions = {{3, 5, 7}, {3, 5, 7}},
             .expected = {1.0F, 1.0F, 1.0F}},
            {.plan_index = {0, 0}, .positions = {{4}, {4}}, .expected = {0.0F, 0.0F, 0.0F}},
            {.plan_index = {0, 1},
             .positions = {{1, 2, 3}, {1, 2, 3}},
             .expected = {2.5F, 2.5F, 2.5F}},
    };
    const std::vector<uint32_t> slops {1, 2, 4};
    for (const auto& test_case : unordered_cases) {
        std::vector<uint32_t> sequential_offsets(test_case.positions.size());
        std::iota(sequential_offsets.begin(), sequential_offsets.end(), 0U);
        const auto spans = make_spans(test_case.positions);
        for (size_t i = 0; i < slops.size(); ++i) {
            SCOPED_TRACE(::testing::Message() << "unordered clauses=" << test_case.positions.size()
                                              << " slop=" << slops[i]);
            SloppyPhraseMatcher matcher(test_case.plan_index, sequential_offsets, slops[i], false);
            EXPECT_FLOAT_EQ(matcher.match(spans, true), test_case.expected[i]);
        }
    }

    struct OrderedCase {
        std::vector<std::vector<uint32_t>> positions;
        std::vector<float> expected; // For slops 1, 2 and 4.
    };
    const std::vector<OrderedCase> ordered_cases {
            {.positions = {{1, 5}, {3, 7}}, .expected = {1.0F, 1.0F, 1.0F}},
            {.positions = {{3}, {2}}, .expected = {0.0F, 0.0F, 0.0F}},
            {.positions = {{1, 8}, {3, 10}, {5, 12}}, .expected = {0.0F, 2.0F / 3, 2.0F / 3}},
            {.positions = {{1, 3, 5}, {2, 4, 6}}, .expected = {3.0F, 3.0F, 3.0F}},
    };
    for (const auto& test_case : ordered_cases) {
        std::vector<size_t> plan_index(test_case.positions.size());
        std::iota(plan_index.begin(), plan_index.end(), 0U);
        std::vector<uint32_t> offsets(test_case.positions.size());
        std::iota(offsets.begin(), offsets.end(), 0U);
        const auto spans = make_spans(test_case.positions);
        for (size_t i = 0; i < slops.size(); ++i) {
            SCOPED_TRACE(::testing::Message() << "ordered clauses=" << test_case.positions.size()
                                              << " slop=" << slops[i]);
            SloppyPhraseMatcher matcher(plan_index, offsets, slops[i], true);
            EXPECT_FLOAT_EQ(matcher.match(spans, true), test_case.expected[i]);
        }
    }
}

TEST(SniiSloppyPhraseMatcher, InterleavedClausesPreserveRepeatedTermCollisions) {
    const std::vector<size_t> plan_index {0, 1, 0};
    const std::vector<uint32_t> offsets {0, 1, 2};
    const std::vector<std::vector<uint32_t>> positions {
            {0, 4, 8, 12}, {1, 5, 9, 13}, {0, 4, 8, 12}};
    const auto spans = make_spans(positions);
    // The legacy CLucene (V3) sloppy matcher's frequency for these positions.
    const float expected = 1.2F;

    SloppyPhraseMatcher matcher(plan_index, offsets, 4, false);
    EXPECT_FLOAT_EQ(matcher.match(spans, true), expected);
    EXPECT_EQ(matcher.match(spans, false), 1.0F);
    EXPECT_FLOAT_EQ(matcher.match(spans, true), expected);
}

// The generated frequencies, rounded to millionths, hash to the value the legacy CLucene (V3)
// matchers gave for the same cases.
TEST(SniiSloppyPhraseMatcher, GeneratedCasesMatchV3Digest) {
    std::mt19937 generator(0x27011U);
    std::uniform_int_distribution<size_t> clause_count_distribution(2, 4);
    uint64_t digest = 14695981039346656037ULL;
    size_t count = 0;
    const auto mix = [&digest, &count](float value) {
        digest = (digest ^ static_cast<uint64_t>(std::llround(double(value) * 1e6))) *
                 1099511628211ULL;
        ++count;
    };
    for (size_t iteration = 0; iteration < 256; ++iteration) {
        const size_t clause_count = clause_count_distribution(generator);
        std::vector<size_t> plan_index(clause_count);
        std::vector<std::vector<uint32_t>> positions(clause_count);
        for (size_t i = 0; i < clause_count; ++i) {
            plan_index[i] = i;
            positions[i] = generate_positions(&generator);
        }
        if (iteration % 3 == 1) {
            plan_index.back() = plan_index.front();
            positions.back() = positions.front();
        } else if (iteration % 3 == 2) {
            std::ranges::fill(plan_index, 0);
            std::ranges::fill(positions, positions.front());
        }

        std::vector<uint32_t> offsets(clause_count);
        std::iota(offsets.begin(), offsets.end(), 0U);
        const auto spans = make_spans(positions);
        for (uint32_t slop : {1U, 3U, 7U}) {
            SloppyPhraseMatcher unordered(plan_index, offsets, slop, false);
            mix(unordered.match(spans, true));
            SloppyPhraseMatcher ordered(plan_index, offsets, slop, true);
            mix(ordered.match(spans, true));
        }
    }
    EXPECT_EQ(count, 1536U);
    EXPECT_EQ(digest, 8055439586674593017ULL);
}

} // namespace
} // namespace doris::index_query
