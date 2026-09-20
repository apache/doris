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

#include <cstdlib>
#include <ctime>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "common/check.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/postings/loaded_postings.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/query/phrase/sloppy_phrase_matcher.h"
#include "testutil/benchmark_control.h"

namespace doris {
namespace {

uint32_t benchmark_parameter(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

uint64_t thread_cpu_ns() {
    timespec now {};
    DORIS_CHECK_EQ(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now), 0);
    return static_cast<uint64_t>(now.tv_sec) * 1000000000ULL + now.tv_nsec;
}

void report_sample(std::string_view label, uint32_t sample, uint32_t iterations, uint64_t elapsed,
                   double checksum) {
    benchmark::report_sample(label, sample, iterations, elapsed, checksum, true);
}

void benchmark_sloppy_case(bool ordered, bool collect_frequency, bool repeated, uint32_t samples,
                           uint32_t iterations) {
    using index_query::PhrasePositionSpan;
    using index_query::SloppyPhraseMatcher;
    const std::vector<size_t> identities =
            repeated ? std::vector<size_t> {0, 1, 0} : std::vector<size_t> {0, 1, 2};
    const std::vector<uint32_t> offsets {0, 1, 2};
    std::vector<std::vector<uint32_t>> positions(3);
    for (size_t term = 0; term < positions.size(); ++term) {
        for (uint32_t occurrence = 0; occurrence < 64; ++occurrence) {
            positions[term].push_back(occurrence * 4 + static_cast<uint32_t>(term));
        }
    }
    if (repeated) {
        positions[2] = positions[0];
    }
    std::vector<PhrasePositionSpan> spans;
    for (const auto& values : positions) {
        spans.emplace_back(values.data(), values.data() + values.size());
    }
    SloppyPhraseMatcher matcher(identities, offsets, 4, ordered);
    const float expected = matcher.match(spans, collect_frequency);
    ASSERT_GT(expected, 0);
    const std::string label = std::string(ordered ? "ordered" : "unordered") +
                              (collect_frequency ? "_frequency" : "_exists") +
                              (repeated ? "_repeated" : "_distinct");
    for (uint32_t sample = 0; sample < samples; ++sample) {
        benchmark::wait_for_turn(label, sample);
        double checksum = 0;
        const uint64_t start = thread_cpu_ns();
        for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            checksum += matcher.match(spans, collect_frequency);
        }
        const uint64_t elapsed = thread_cpu_ns() - start;
        ASSERT_DOUBLE_EQ(checksum, static_cast<double>(expected) * iterations);
        report_sample(label, sample, iterations, elapsed, checksum);
    }
}

TEST(PhraseKernelBench, DISABLED_SloppyPositionWorkloads) {
    const uint32_t samples = benchmark_parameter("QUERY_ENGINE_BENCH_SAMPLES", 21);
    const uint32_t iterations = benchmark_parameter("QUERY_ENGINE_BENCH_ITERATIONS", 32768);
    ASSERT_GT(samples, 0);
    ASSERT_GT(iterations, 0);
    for (bool ordered : {false, true}) {
        for (bool collect_frequency : {false, true}) {
            for (bool repeated : {false, true}) {
                benchmark_sloppy_case(ordered, collect_frequency, repeated, samples, iterations);
            }
        }
    }
}

TEST(PhraseKernelBench, DISABLED_ExactScorerWorkloads) {
    using namespace segment_v2::inverted_index::query_v2;
    const uint32_t samples = benchmark_parameter("QUERY_ENGINE_BENCH_SAMPLES", 21);
    const uint32_t iterations = benchmark_parameter("QUERY_ENGINE_BENCH_EXACT_ITERATIONS", 32);
    constexpr uint32_t kDocs = 16384;
    ASSERT_GT(samples, 0);
    ASSERT_GT(iterations, 0);
    std::vector<uint32_t> docs(kDocs);
    std::iota(docs.begin(), docs.end(), 0);

    for (bool matches : {true, false}) {
        const std::vector<std::vector<uint32_t>> left_positions(kDocs, {1, 5, 9, 13});
        const std::vector<std::vector<uint32_t>> right_positions(
                kDocs, matches ? std::vector<uint32_t> {2, 6, 10, 14}
                               : std::vector<uint32_t> {3, 7, 11, 15});
        const std::string_view label = matches ? "exact_matches" : "exact_misses";
        for (uint32_t sample = 0; sample < samples; ++sample) {
            benchmark::wait_for_turn(label, sample);
            uint64_t checksum = 0;
            uint64_t elapsed = 0;
            for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
                PostingsPtr left = std::make_shared<LoadedPostings>(docs, left_positions);
                PostingsPtr right = std::make_shared<LoadedPostings>(docs, right_positions);
                const std::vector<std::pair<size_t, PostingsPtr>> terms {{0, left}, {1, right}};
                const uint64_t start = thread_cpu_ns();
                auto scorer = PhraseScorer<PostingsPtr>::create(terms, nullptr, 0, kDocs);
                for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
                    ++checksum;
                }
                elapsed += thread_cpu_ns() - start;
            }
            ASSERT_EQ(checksum, matches ? static_cast<uint64_t>(kDocs) * iterations : 0);
            report_sample(label, sample, iterations, elapsed, checksum);
        }
    }
}

std::vector<uint32_t> exact_benchmark_positions(size_t clause, size_t clause_count,
                                                uint32_t position_count, std::string_view pattern) {
    std::vector<uint32_t> values;
    for (uint32_t occurrence = 0; occurrence < position_count; ++occurrence) {
        values.push_back(1 + occurrence * 8 + static_cast<uint32_t>(clause));
    }
    if (clause + 1 == clause_count && pattern != "dense") {
        if (pattern == "partial") {
            ++values[position_count / 2];
        } else if (pattern == "alternating" || pattern == "shifted") {
            const size_t stride = pattern == "alternating" ? 2 : 1;
            for (size_t i = 0; i < values.size(); i += stride) {
                ++values[i];
            }
        } else {
            values = {pattern == "early" ? values.front() : values.back()};
            if (pattern == "miss") {
                ++values.front();
            }
        }
    }
    return values;
}

void benchmark_exact_workload(size_t clause_count, std::string_view pattern, bool scored,
                              uint32_t samples, uint32_t iterations) {
    using namespace segment_v2::inverted_index::query_v2;
    constexpr uint32_t kDocs = 1024;
    constexpr uint32_t kPositions = 64;
    std::vector<uint32_t> docs(kDocs);
    std::iota(docs.begin(), docs.end(), 0);
    std::vector<std::vector<std::vector<uint32_t>>> positions(clause_count);
    for (size_t clause = 0; clause < clause_count; ++clause) {
        const auto values = exact_benchmark_positions(clause, clause_count, kPositions, pattern);
        positions[clause].assign(kDocs, values);
    }
    uint32_t frequency = 1;
    if (pattern == "dense") {
        frequency = kPositions;
    } else if (pattern == "miss" || pattern == "shifted") {
        frequency = 0;
    } else if (pattern == "partial") {
        frequency = kPositions - 1;
    } else if (pattern == "alternating") {
        frequency = kPositions / 2;
    }
    const auto similarity =
            scored ? std::make_shared<segment_v2::BM25Similarity>(2.0F, 8.0F) : nullptr;
    const double per_doc = scored ? similarity->score(static_cast<float>(frequency), 1) : 1;
    const std::string label = "exact/" + std::string(scored ? "scored/" : "unscored/") +
                              std::to_string(clause_count) + "/" + std::string(pattern);
    for (uint32_t sample = 0; sample < samples; ++sample) {
        benchmark::wait_for_turn(label, sample);
        double checksum = 0;
        uint64_t elapsed = 0;
        for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            std::vector<std::pair<size_t, PostingsPtr>> terms;
            for (size_t clause = 0; clause < clause_count; ++clause) {
                terms.emplace_back(clause,
                                   std::make_shared<LoadedPostings>(docs, positions[clause]));
            }
            const uint64_t start = thread_cpu_ns();
            auto scorer = PhraseScorer<PostingsPtr>::create(terms, similarity, 0, kDocs);
            for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
                checksum += scored ? scorer->score() : 1;
            }
            elapsed += thread_cpu_ns() - start;
        }
        ASSERT_DOUBLE_EQ(checksum, frequency == 0 ? 0 : per_doc * kDocs * iterations);
        report_sample(label, sample, iterations, elapsed, checksum);
    }
}

TEST(PhraseKernelBench, DISABLED_ExactPositionAndFrequencyWorkloads) {
    const uint32_t samples = benchmark_parameter("QUERY_ENGINE_BENCH_SAMPLES", 21);
    const uint32_t iterations = benchmark_parameter("QUERY_ENGINE_BENCH_EXACT_ITERATIONS", 32);
    ASSERT_GT(samples, 0);
    ASSERT_GT(iterations, 0);
    for (const size_t clauses : {2U, 4U}) {
        for (const std::string_view pattern :
             {"dense", "early", "late", "miss", "partial", "alternating", "shifted"}) {
            for (const bool scored : {false, true}) {
                benchmark_exact_workload(clauses, pattern, scored, samples, iterations);
            }
        }
    }
}

} // namespace
} // namespace doris
