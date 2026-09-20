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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "storage/index/query/docid_set_ops.h"
#include "storage/index/query/docid_sink.h"
#include "testutil/benchmark_control.h"

namespace doris::index_query {
namespace {

uint64_t docid_cpu_ns() {
    timespec value {};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value);
    return static_cast<uint64_t>(value.tv_sec) * 1000000000 + value.tv_nsec;
}

uint32_t docid_parameter(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

uint64_t docid_checksum(const std::vector<uint32_t>& docs) {
    uint64_t hash = 14695981039346656037ULL;
    for (uint32_t doc : docs) {
        hash = (hash ^ doc) * 1099511628211ULL;
    }
    return hash;
}

template <typename Operation>
void benchmark_docids(const std::string& label, Operation operation,
                      const std::vector<uint32_t>& expected, uint32_t samples,
                      uint32_t iterations) {
    ASSERT_EQ(operation(), expected);
    const uint64_t expected_checksum = docid_checksum(expected) * iterations;
    for (uint32_t sample = 0; sample < samples; ++sample) {
        benchmark::wait_for_turn(label, sample);
        uint64_t elapsed = 0;
        uint64_t checksum = 0;
        for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            const uint64_t start = docid_cpu_ns();
            const auto docs = operation();
            elapsed += docid_cpu_ns() - start;
            checksum += docid_checksum(docs);
        }
        ASSERT_EQ(checksum, expected_checksum);
        benchmark::report_sample(label, sample, iterations, elapsed, checksum);
    }
}

TEST(DocIdKernelBench, DISABLED_SetOperationsAndBulkSink) {
    const uint32_t samples = docid_parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
    const uint32_t iterations = docid_parameter("DOCID_KERNEL_BENCH_ITERATIONS", 64);
    ASSERT_GT(samples, 0);
    ASSERT_GT(iterations, 0);
    for (uint32_t fan_in : {2U, 8U, 9U, 32U}) {
        for (bool disjoint : {false, true}) {
            std::vector<std::vector<uint32_t>> lists(fan_in);
            std::set<uint32_t> membership;
            for (uint32_t list = 0; list < fan_in; ++list) {
                for (uint32_t index = 0; index < 512; ++index) {
                    const uint32_t doc = disjoint ? index * fan_in + list : index + list;
                    lists[list].push_back(doc);
                    membership.insert(doc);
                }
            }
            const std::vector<uint32_t> expected(membership.begin(), membership.end());
            benchmark_docids(
                    "docids/union/" + std::to_string(fan_in) +
                            (disjoint ? "/disjoint" : "/overlap"),
                    [&] { return union_sorted_many(lists, expected.size()); }, expected, samples,
                    iterations);
        }
    }
    std::vector<uint32_t> dense(65536);
    std::iota(dense.begin(), dense.end(), 0);
    for (uint32_t step : {1U, 64U, 1024U}) {
        std::vector<uint32_t> sparse;
        for (uint32_t doc = 0; doc < dense.size(); doc += step) {
            sparse.push_back(doc);
        }
        benchmark_docids(
                "docids/intersection/step_" + std::to_string(step),
                [&] { return intersect_sorted(dense, sparse); }, sparse, samples, iterations);
    }
    for (uint32_t width : {64U, 1024U, 65536U}) {
        benchmark_docids(
                "docids/vector_ranges/width_" + std::to_string(width),
                [&] {
                    std::vector<uint32_t> docs;
                    VectorDocIdSink sink(docs);
                    for (uint32_t first = 0; first < dense.size(); first += width) {
                        EXPECT_TRUE(sink.append_range(first, first + width).ok());
                    }
                    return docs;
                },
                dense, samples, iterations);
    }
}

} // namespace
} // namespace doris::index_query
