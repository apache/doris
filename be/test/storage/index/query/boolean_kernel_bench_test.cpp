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
#include <vector>

#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "testutil/benchmark_control.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

constexpr uint32_t kBooleanBenchRows = 32768;

uint64_t boolean_cpu_ns() {
    timespec value {};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value);
    return static_cast<uint64_t>(value.tv_sec) * 1000000000 + value.tv_nsec;
}

uint32_t boolean_parameter(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

struct BooleanBenchFixture {
    QueryPtr query;
    uint64_t expected_checksum = 0;

    BooleanBenchFixture(OperatorType op, uint32_t clauses, bool sparse, bool nullable,
                        bool scoring) {
        std::vector<std::shared_ptr<roaring::Roaring>> truths;
        std::vector<std::shared_ptr<roaring::Roaring>> nulls;
        for (uint32_t clause = 0; clause < clauses; ++clause) {
            truths.emplace_back(std::make_shared<roaring::Roaring>());
            nulls.emplace_back(std::make_shared<roaring::Roaring>());
        }
        const uint32_t threshold = sparse ? 4 : 64;
        for (uint32_t doc = 0; doc < kBooleanBenchRows; ++doc) {
            uint32_t matches = 0;
            for (uint32_t clause = 0; clause < clauses; ++clause) {
                uint32_t value = (doc + 1) * (clause * 4 + 17) * 2654435761U;
                value ^= value >> 16;
                if (value % 97 < threshold) {
                    truths[clause]->add(doc);
                    ++matches;
                } else if (nullable && (doc + 7 * clause) % 11 == 0) {
                    nulls[clause]->add(doc);
                }
            }
            bool matched = matches > 0;
            if (op == OperatorType::OP_AND) {
                matched = matches == clauses;
            } else if (op == OperatorType::OP_NOT) {
                matched = matches == 0;
            }
            if (matched) {
                expected_checksum += static_cast<uint64_t>(doc + 1) * 1099511628211ULL;
                if (scoring) {
                    expected_checksum += op == OperatorType::OP_NOT ? 1 : matches;
                }
            }
        }
        OperatorBooleanQueryBuilder builder(op);
        for (uint32_t clause = 0; clause < clauses; ++clause) {
            builder.add(std::make_shared<BitSetQuery>(truths[clause], nulls[clause]));
        }
        query = builder.build();
    }
};

uint64_t execute_boolean(const QueryPtr& query, bool scoring) {
    QueryExecutionContext context;
    context.segment_num_rows = kBooleanBenchRows;
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
    // The contract tests separately verify the complete null result.
    (void)scorer->get_null_bitmap();
    return checksum;
}

void benchmark_boolean_case(OperatorType op, uint32_t clauses, bool sparse, bool nullable,
                            bool scoring, uint32_t samples, uint32_t iterations) {
    const BooleanBenchFixture fixture(op, clauses, sparse, nullable, scoring);
    std::string label = "boolean/or/";
    if (op == OperatorType::OP_AND) {
        label = "boolean/and/";
    } else if (op == OperatorType::OP_NOT) {
        label = "boolean/not/";
    }
    label += std::to_string(clauses);
    label += sparse ? "/sparse" : "/dense";
    label += nullable ? "/nullable" : "/nonnull";
    label += scoring ? "/scored" : "/unscored";
    ASSERT_EQ(execute_boolean(fixture.query, scoring), fixture.expected_checksum) << label;
    for (uint32_t sample = 0; sample < samples; ++sample) {
        doris::benchmark::wait_for_turn(label, sample);
        uint64_t checksum = 0;
        const uint64_t start = boolean_cpu_ns();
        for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            checksum += execute_boolean(fixture.query, scoring);
        }
        const uint64_t elapsed = boolean_cpu_ns() - start;
        ASSERT_EQ(checksum, fixture.expected_checksum * iterations) << label;
        doris::benchmark::report_sample(label, sample, iterations, elapsed, checksum);
    }
}

TEST(BooleanKernelBench, DISABLED_NullableAndNonNullScoring) {
    const uint32_t samples = boolean_parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
    const uint32_t iterations = boolean_parameter("BOOLEAN_KERNEL_BENCH_ITERATIONS", 16);
    ASSERT_GT(samples, 0);
    ASSERT_GT(iterations, 0);
    for (OperatorType op : {OperatorType::OP_AND, OperatorType::OP_OR}) {
        for (uint32_t clauses : {2U, 4U}) {
            for (bool sparse : {false, true}) {
                for (bool nullable : {false, true}) {
                    for (bool scoring : {false, true}) {
                        benchmark_boolean_case(op, clauses, sparse, nullable, scoring, samples,
                                               iterations);
                    }
                }
            }
        }
    }
}

TEST(BooleanKernelBench, DISABLED_NegationScoring) {
    const uint32_t samples = boolean_parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
    const uint32_t iterations = boolean_parameter("BOOLEAN_KERNEL_BENCH_ITERATIONS", 16);
    ASSERT_GT(samples, 0);
    ASSERT_GT(iterations, 0);
    for (uint32_t clauses : {2U, 4U}) {
        for (bool sparse : {false, true}) {
            for (bool scoring : {false, true}) {
                benchmark_boolean_case(OperatorType::OP_NOT, clauses, sparse, false, scoring,
                                       samples, iterations);
            }
        }
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
