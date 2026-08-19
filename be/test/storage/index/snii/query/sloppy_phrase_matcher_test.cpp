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

#include "storage/index/snii/query/internal/sloppy_phrase_matcher.h"

#include <gtest/gtest.h>

#include <map>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "storage/index/inverted/query/phrase_query/ordered_sloppy_phrase_matcher.h"
#include "storage/index/inverted/query/phrase_query/sloppy_phrase_matcher.h"
#include "storage/index/inverted/util/mock_iterator.h"

namespace doris::snii::query::internal {
namespace {

std::vector<PhrasePositionSpan> make_spans(const std::vector<std::vector<uint32_t>>& positions) {
    std::vector<PhrasePositionSpan> spans;
    spans.reserve(positions.size());
    for (const auto& clause : positions) {
        spans.emplace_back(clause.data(), clause.data() + clause.size());
    }
    return spans;
}

segment_v2::inverted_index::MockIterPtr make_v3_iterator(const std::vector<uint32_t>& positions) {
    std::vector<int32_t> signed_positions;
    signed_positions.reserve(positions.size());
    for (uint32_t position : positions) {
        signed_positions.push_back(static_cast<int32_t>(position));
    }
    auto iterator = std::make_shared<segment_v2::inverted_index::MockIterator>();
    iterator->set_postings({{0, std::move(signed_positions)}});
    return iterator;
}

float v3_unordered_frequency(const std::vector<size_t>& plan_index,
                             const std::vector<std::vector<uint32_t>>& positions, int32_t slop) {
    std::vector<segment_v2::inverted_index::PostingsAndFreq> postings;
    postings.reserve(positions.size());
    for (size_t i = 0; i < positions.size(); ++i) {
        postings.emplace_back(make_v3_iterator(positions[i]), static_cast<int32_t>(i),
                              std::vector<std::string> {std::to_string(plan_index[i])});
    }
    segment_v2::inverted_index::SloppyPhraseMatcher matcher(postings, slop);
    return matcher.phrase_freq(0);
}

float v3_ordered_frequency(const std::vector<std::vector<uint32_t>>& positions, int32_t slop) {
    std::vector<segment_v2::inverted_index::PostingsAndPosition> postings;
    postings.reserve(positions.size());
    for (size_t i = 0; i < positions.size(); ++i) {
        postings.emplace_back(make_v3_iterator(positions[i]), static_cast<int32_t>(i));
    }
    segment_v2::inverted_index::OrderedSloppyPhraseMatcher matcher(std::move(postings), slop);
    return matcher.phrase_freq(0);
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

TEST(SniiSloppyPhraseMatcher, FrequenciesMatchV3Oracle) {
    struct UnorderedCase {
        std::vector<size_t> plan_index;
        std::vector<std::vector<uint32_t>> positions;
    };
    const std::vector<UnorderedCase> unordered_cases {
            {{0, 1}, {{1, 5, 9}, {3, 7, 11}}},
            {{0, 1}, {{1}, {0}}},
            {{0, 1, 2}, {{0, 4}, {2, 6}, {1, 8}}},
            {{0, 0}, {{3, 5, 7}, {3, 5, 7}}},
            {{0, 0}, {{4}, {4}}},
            {{0, 1}, {{1, 2, 3}, {1, 2, 3}}},
    };
    for (const auto& test_case : unordered_cases) {
        std::vector<uint32_t> sequential_offsets(test_case.positions.size());
        std::iota(sequential_offsets.begin(), sequential_offsets.end(), 0U);
        const auto spans = make_spans(test_case.positions);
        for (uint32_t slop : {1U, 2U, 4U}) {
            SCOPED_TRACE(::testing::Message()
                         << "unordered clauses=" << test_case.positions.size() << " slop=" << slop);
            SloppyPhraseMatcher matcher(test_case.plan_index, sequential_offsets, slop, false);
            EXPECT_FLOAT_EQ(matcher.match(spans, true),
                            v3_unordered_frequency(test_case.plan_index, test_case.positions,
                                                   static_cast<int32_t>(slop)));
        }
    }

    const std::vector<std::vector<std::vector<uint32_t>>> ordered_cases {
            {{1, 5}, {3, 7}},
            {{3}, {2}},
            {{1, 8}, {3, 10}, {5, 12}},
            {{1, 3, 5}, {2, 4, 6}},
    };
    for (const auto& positions : ordered_cases) {
        std::vector<size_t> plan_index(positions.size());
        std::iota(plan_index.begin(), plan_index.end(), 0U);
        std::vector<uint32_t> offsets(positions.size());
        std::iota(offsets.begin(), offsets.end(), 0U);
        const auto spans = make_spans(positions);
        for (uint32_t slop : {1U, 2U, 4U}) {
            SCOPED_TRACE(::testing::Message()
                         << "ordered clauses=" << positions.size() << " slop=" << slop);
            SloppyPhraseMatcher matcher(plan_index, offsets, slop, true);
            EXPECT_FLOAT_EQ(matcher.match(spans, true),
                            v3_ordered_frequency(positions, static_cast<int32_t>(slop)));
        }
    }
}

TEST(SniiSloppyPhraseMatcher, GeneratedCasesMatchV3Oracle) {
    std::mt19937 generator(0x27011U);
    std::uniform_int_distribution<size_t> clause_count_distribution(2, 4);
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
            std::fill(plan_index.begin(), plan_index.end(), 0);
            std::fill(positions.begin(), positions.end(), positions.front());
        }

        std::vector<uint32_t> offsets(clause_count);
        std::iota(offsets.begin(), offsets.end(), 0U);
        const auto spans = make_spans(positions);
        for (uint32_t slop : {1U, 3U, 7U}) {
            SCOPED_TRACE(::testing::Message() << "iteration=" << iteration
                                              << " clauses=" << clause_count << " slop=" << slop);
            SloppyPhraseMatcher unordered(plan_index, offsets, slop, false);
            EXPECT_FLOAT_EQ(
                    unordered.match(spans, true),
                    v3_unordered_frequency(plan_index, positions, static_cast<int32_t>(slop)));

            SloppyPhraseMatcher ordered(plan_index, offsets, slop, true);
            EXPECT_FLOAT_EQ(ordered.match(spans, true),
                            v3_ordered_frequency(positions, static_cast<int32_t>(slop)));
        }
    }
}

} // namespace
} // namespace doris::snii::query::internal
