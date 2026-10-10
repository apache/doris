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

#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "testutil/benchmark_control.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

constexpr uint32_t kOccurBenchRows = 32768;

struct OccurBenchSpec {
    const char* label;
    std::vector<Occur> clauses;
    uint32_t minimum_should_match;
    bool supports_null_comparison = true;
};

std::pair<bool, uint32_t> evaluate_benchmark_row(const OccurBenchSpec& spec,
                                                 const std::vector<bool>& matches) {
    bool required = true;
    uint32_t optional_matches = 0;
    uint32_t score = 0;
    for (size_t index = 0; index < spec.clauses.size(); ++index) {
        switch (spec.clauses[index]) {
        case Occur::MUST:
            required &= matches[index];
            score += matches[index];
            break;
        case Occur::SHOULD:
            optional_matches += matches[index];
            score += matches[index];
            break;
        case Occur::MUST_NOT:
            required &= !matches[index];
            break;
        }
    }
    return {required && optional_matches >= spec.minimum_should_match, score};
}

struct OccurBenchFixture {
    QueryPtr query;
    uint64_t expected_checksum = 0;

    OccurBenchFixture(const OccurBenchSpec& spec, bool sparse, bool nullable, bool scoring) {
        std::vector<std::shared_ptr<roaring::Roaring>> truths;
        std::vector<std::shared_ptr<roaring::Roaring>> nulls;
        for (size_t clause = 0; clause < spec.clauses.size(); ++clause) {
            truths.emplace_back(std::make_shared<roaring::Roaring>());
            nulls.emplace_back(std::make_shared<roaring::Roaring>());
        }
        const uint32_t threshold = sparse ? 4 : 64;
        std::vector<bool> matches(spec.clauses.size());
        for (uint32_t doc = 0; doc < kOccurBenchRows; ++doc) {
            for (uint32_t clause = 0; clause < spec.clauses.size(); ++clause) {
                uint32_t value = (doc + 1) * (clause * 4 + 17) * 2654435761U;
                value ^= value >> 16;
                matches[clause] = value % 97 < threshold;
                if (matches[clause]) {
                    truths[clause]->add(doc);
                } else if (nullable && (doc + 7 * clause) % 11 == 0) {
                    nulls[clause]->add(doc);
                }
            }
            const auto [matched, score] = evaluate_benchmark_row(spec, matches);
            if (matched) {
                expected_checksum += static_cast<uint64_t>(doc + 1) * 1099511628211ULL;
                if (scoring) {
                    expected_checksum += score;
                }
            }
        }
        OccurBooleanQueryBuilder builder;
        builder.set_minimum_number_should_match(spec.minimum_should_match);
        for (size_t clause = 0; clause < spec.clauses.size(); ++clause) {
            builder.add(std::make_shared<BitSetQuery>(truths[clause], nulls[clause]),
                        spec.clauses[clause]);
        }
        query = builder.build();
    }
};

uint32_t occur_parameter(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

uint64_t occur_cpu_ns() {
    timespec value {};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value);
    return static_cast<uint64_t>(value.tv_sec) * 1000000000 + value.tv_nsec;
}

uint64_t execute_occur(const QueryPtr& query, bool scoring) {
    QueryExecutionContext context;
    context.segment_num_rows = kOccurBenchRows;
    auto scorer = query->weight(scoring)->scorer(context);
    uint64_t checksum = 0;
    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        checksum += static_cast<uint64_t>(doc + 1) * 1099511628211ULL;
        if (scoring) {
            checksum += static_cast<uint64_t>(scorer->score());
        }
        doc = scorer->advance();
    }
    (void)scorer->get_null_bitmap();
    return checksum;
}

void benchmark_occur_case(const OccurBenchSpec& spec, bool sparse, bool nullable, bool scoring,
                          uint32_t samples, uint32_t iterations) {
    const OccurBenchFixture fixture(spec, sparse, nullable, scoring);
    std::string label = "occur/" + std::string(spec.label);
    label += sparse ? "/sparse" : "/dense";
    label += nullable ? "/nullable" : "/nonnull";
    label += scoring ? "/scored" : "/unscored";
    ASSERT_EQ(execute_occur(fixture.query, scoring), fixture.expected_checksum) << label;
    for (uint32_t sample = 0; sample < samples; ++sample) {
        doris::benchmark::wait_for_turn(label, sample);
        uint64_t checksum = 0;
        const uint64_t start = occur_cpu_ns();
        for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            checksum += execute_occur(fixture.query, scoring);
        }
        const uint64_t elapsed = occur_cpu_ns() - start;
        ASSERT_EQ(checksum, fixture.expected_checksum * iterations) << label;
        doris::benchmark::report_sample(label, sample, iterations, elapsed, checksum);
    }
}

TEST(OccurBooleanKernelBench, DISABLED_OptionalThresholdAndExclusion) {
    const uint32_t samples = occur_parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
    const uint32_t iterations = occur_parameter("BOOLEAN_KERNEL_BENCH_ITERATIONS", 16);
    ASSERT_GT(samples, 0);
    ASSERT_GT(iterations, 0);
    const std::vector<Occur> shoulds(4, Occur::SHOULD);
    const std::vector<OccurBenchSpec> specs {
            {.label = "optional",
             .clauses = {Occur::MUST, Occur::SHOULD},
             .minimum_should_match = 0},
            {.label = "threshold_1", .clauses = shoulds, .minimum_should_match = 1},
            {.label = "threshold_2", .clauses = shoulds, .minimum_should_match = 2},
            {.label = "threshold_3", .clauses = shoulds, .minimum_should_match = 3},
            {.label = "threshold_4", .clauses = shoulds, .minimum_should_match = 4},
            {.label = "exclude",
             .clauses = {Occur::MUST, Occur::MUST_NOT},
             .minimum_should_match = 0,
             .supports_null_comparison = false}};
    for (const auto& spec : specs) {
        for (bool sparse : {false, true}) {
            for (bool nullable : {false, true}) {
                if (nullable && !spec.supports_null_comparison) {
                    continue;
                }
                for (bool scoring : {false, true}) {
                    benchmark_occur_case(spec, sparse, nullable, scoring, samples, iterations);
                }
            }
        }
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
