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
#include <pdqsort.h>

#include <algorithm>
#include <random>
#include <vector>

#include "vec/core/timsort.hpp"

namespace doris {

enum class DataPattern {
    RANDOM,
    ASCENDING_SAW,
    DESCENDING_SAW,
    GENERIC,
    RANDOM_TAIL,
    RANDOM_HALF,
    WAVE,
    SMALL_RANDOM
};

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wconversion"


class SortDataGenerator {
public:
    static std::vector<int32_t> generate(DataPattern pattern, size_t size) {
        std::vector<int32_t> data;
        data.reserve(size);
        std::default_random_engine rng(42);

        switch (pattern) {
        case DataPattern::RANDOM: {
            std::uniform_int_distribution<int32_t> dist(0, size);
            for (size_t i = 0; i < size; ++i) {
                data.push_back(dist(rng));
            }
            break;
        }
        case DataPattern::ASCENDING_SAW: {
            // 升序锯齿: 多个递增段
            size_t saw_length = size / 100;
            for (size_t i = 0; i < size; ++i) {
                data.push_back(i % saw_length);
            }
            break;
        }
        case DataPattern::DESCENDING_SAW: {
            // 降序锯齿: 多个递减段
            size_t saw_length = size / 100;
            for (size_t i = 0; i < size; ++i) {
                data.push_back(saw_length - (i % saw_length));
            }
            break;
        }
        case DataPattern::GENERIC: {
            // 部分有序
            for (size_t i = 0; i < size; ++i) {
                data.push_back(i);
            }
            std::uniform_int_distribution<int32_t> dist(0, size - 1);
            // 随机交换20%的元素
            for (size_t i = 0; i < size / 5; ++i) {
                size_t pos1 = dist(rng);
                size_t pos2 = dist(rng);
                std::swap(data[pos1], data[pos2]);
            }
            break;
        }
        case DataPattern::RANDOM_TAIL: {
            // 前80%有序，后20%随机
            size_t ordered_size = size * 4 / 5;
            for (size_t i = 0; i < ordered_size; ++i) {
                data.push_back(i);
            }
            std::uniform_int_distribution<int32_t> dist(0, size);
            for (size_t i = ordered_size; i < size; ++i) {
                data.push_back(dist(rng));
            }
            break;
        }
        case DataPattern::RANDOM_HALF: {
            // 前50%有序，后50%随机
            size_t half = size / 2;
            for (size_t i = 0; i < half; ++i) {
                data.push_back(i);
            }
            std::uniform_int_distribution<int32_t> dist(0, size);
            for (size_t i = half; i < size; ++i) {
                data.push_back(dist(rng));
            }
            break;
        }
        case DataPattern::WAVE: {
            // 波浪形: 升序-降序交替
            size_t wave_length = size / 50;
            for (size_t i = 0; i < size; ++i) {
                size_t pos = i % (wave_length * 2);
                if (pos < wave_length) {
                    data.push_back(pos);
                } else {
                    data.push_back(wave_length * 2 - pos);
                }
            }
            break;
        }
        case DataPattern::SMALL_RANDOM: {
            // 小规模随机数据 (1-1023)
            std::uniform_int_distribution<int32_t> dist(0, 1023);
            for (size_t i = 0; i < size; ++i) {
                data.push_back(dist(rng));
            }
            break;
        }
        }
        return data;
    }

    static const char* pattern_name(DataPattern pattern) {
        switch (pattern) {
        case DataPattern::RANDOM:
            return "random";
        case DataPattern::ASCENDING_SAW:
            return "ascending_saw";
        case DataPattern::DESCENDING_SAW:
            return "descending_saw";
        case DataPattern::GENERIC:
            return "generic";
        case DataPattern::RANDOM_TAIL:
            return "random_tail";
        case DataPattern::RANDOM_HALF:
            return "random_half";
        case DataPattern::WAVE:
            return "wave";
        case DataPattern::SMALL_RANDOM:
            return "small_random";
        default:
            return "unknown";
        }
    }
};

static void BM_PdqSort(benchmark::State& state) {
    size_t size = state.range(0);
    DataPattern pattern = static_cast<DataPattern>(state.range(1));

    // 预生成测试数据（不计入性能统计）
    std::vector<int32_t> original_data = SortDataGenerator::generate(pattern, size);

    for (auto _ : state) {
        // 为每次迭代复制数据
        std::vector<int32_t> data = original_data;
        benchmark::DoNotOptimize(data.data());

        // 执行排序
        pdqsort(data.begin(), data.end());

        benchmark::ClobberMemory();
    }

    state.SetLabel(SortDataGenerator::pattern_name(pattern));
    state.SetItemsProcessed(int64_t(state.iterations()) * size);
}

static void BM_TimSort(benchmark::State& state) {
    size_t size = state.range(0);
    DataPattern pattern = static_cast<DataPattern>(state.range(1));

    // 预生成测试数据（不计入性能统计）
    std::vector<int32_t> original_data = SortDataGenerator::generate(pattern, size);

    for (auto _ : state) {
        // 为每次迭代复制数据
        std::vector<int32_t> data = original_data;
        benchmark::DoNotOptimize(data.data());

        // 执行排序
        gfx::timsort(data.begin(), data.end(), std::less<int32_t>());

        benchmark::ClobberMemory();
    }

    state.SetLabel(SortDataGenerator::pattern_name(pattern));
    state.SetItemsProcessed(int64_t(state.iterations()) * size);
}

// 注册benchmark - 大规模数据测试 (1000000 elements)
#define REGISTER_SORT_BENCHMARK(NAME, SIZE)                                   \
    BENCHMARK(NAME)                                                           \
            ->Args({SIZE, static_cast<int64_t>(DataPattern::RANDOM)})         \
            ->Args({SIZE, static_cast<int64_t>(DataPattern::ASCENDING_SAW)})  \
            ->Args({SIZE, static_cast<int64_t>(DataPattern::DESCENDING_SAW)}) \
            ->Args({SIZE, static_cast<int64_t>(DataPattern::GENERIC)})        \
            ->Args({SIZE, static_cast<int64_t>(DataPattern::RANDOM_TAIL)})    \
            ->Args({SIZE, static_cast<int64_t>(DataPattern::RANDOM_HALF)})    \
            ->Args({SIZE, static_cast<int64_t>(DataPattern::WAVE)})           \
            ->Unit(benchmark::kMillisecond)

REGISTER_SORT_BENCHMARK(BM_PdqSort, 10000000);
REGISTER_SORT_BENCHMARK(BM_TimSort, 10000000);
} // namespace doris

BENCHMARK_MAIN();
