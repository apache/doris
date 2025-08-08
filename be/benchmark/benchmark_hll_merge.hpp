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

#include <benchmark/benchmark.h>

#include "olap/hll.h"
#include "util/hash_util.hpp"

namespace doris {
    static uint64_t hash(uint64_t value) {
        return HashUtil::murmur_hash64A(&value, 8, 0);
    }

    static std::pair<HyperLogLog, HyperLogLog> prepare_test_data() {
        HyperLogLog hll1, hll2;
        for (int i = 0; i < 64 * 1024; ++i) {
            hll1.update(hash(i));
            hll2.update(hash(i + 1));
        }
        return {std::move(hll1), std::move(hll2)};
    }
} // namespace doris

static auto [hll1, hll2] = doris::prepare_test_data();

static void BM_HllMerge(benchmark::State& state) {
    for (auto _ : state) {
        doris::HyperLogLog copy1(hll1);
        doris::HyperLogLog copy2(hll2);
        copy1.merge(copy2);

        benchmark::DoNotOptimize(copy1);
    }
}

BENCHMARK(BM_HllMerge)
        ->Unit(benchmark::kNanosecond)
        ->Repetitions(5)
        ->DisplayAggregatesOnly()
        ->ComputeStatistics("min",
                            [](const std::vector<double>& v) -> double {
                                return *std::min_element(v.begin(), v.end());
                            })
        ->ComputeStatistics("max", [](const std::vector<double>& v) -> double {
            return *std::max_element(v.begin(), v.end());
        });