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

#include "common/status.h"
#include "exprs/block_bloom_filter.hpp"

namespace doris {
    static std::unique_ptr<BlockBloomFilter> create_bloom_filter(int batch, int log_space_bytes = 20) {
        auto bloom_filter = std::make_unique<BlockBloomFilter>();
        [[maybe_unused]] Status status = bloom_filter->init(log_space_bytes, 0);

        for (int i = 0; i < batch; i++) {
            bloom_filter->insert(i);
        }

        return bloom_filter;
    }
} // namespace doris

static void BM_BBF_BucketInsert(benchmark::State& state) {
    const int batch = static_cast<int>(state.range(0));
    auto bf = doris::create_bloom_filter(batch);

    for (auto _ : state) {
        for (int i = 0; i < batch; i++) {
            bf->insert(i);
        }
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch));
}

static void BM_BBF_BucketFind_Hit(benchmark::State& state) {
    const int batch = static_cast<int>(state.range(0));
    auto bf = doris::create_bloom_filter(batch);

    for (auto _ : state) {
        bool acc = false;
        for (int i = 0; i < batch; i++) {
            acc ^= bf->find(i);
        }
        benchmark::DoNotOptimize(acc);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch));
}

static void BM_BBF_BucketFind_Miss(benchmark::State& state) {
    const int batch = static_cast<int>(state.range(0));
    auto bf = doris::create_bloom_filter(batch);

    for (auto _ : state) {
        bool acc = false;
        for (int i = 0; i < batch; i++) {
            acc ^= bf->find(i + batch);
        }
        benchmark::DoNotOptimize(acc);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch));
}

BENCHMARK(BM_BBF_BucketInsert)
    ->Unit(benchmark::kNanosecond)
    ->Arg(1 << 12)
    ->Arg(1 << 15)
    ->Arg(1 << 18)
    ->Repetitions(5)
    ->DisplayAggregatesOnly();
BENCHMARK(BM_BBF_BucketFind_Hit)
    ->Unit(benchmark::kNanosecond)
    ->Arg(1 << 12)
    ->Arg(1 << 15)
    ->Arg(1 << 18)
    ->Repetitions(5)
    ->DisplayAggregatesOnly();
BENCHMARK(BM_BBF_BucketFind_Miss)
    ->Unit(benchmark::kNanosecond)
    ->Arg(1 << 12)
    ->Arg(1 << 15)
    ->Arg(1 << 18)
    ->Repetitions(5)
    ->DisplayAggregatesOnly();
