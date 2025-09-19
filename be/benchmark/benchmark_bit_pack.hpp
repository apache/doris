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

#include <random>
#include <vector>

#include "util/frame_of_reference_coding.h"

namespace doris {

// original bit_pack function
template <typename T>
void bit_pack(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    if (in_num == 0 || bit_width == 0) {
        return;
    }

    T in_mask = 0;
    int bit_index = 0;
    *output = 0;
    for (int i = 0; i < in_num; i++) {
        in_mask = ((T)1) << (bit_width - 1);
        for (int k = 0; k < bit_width; k++) {
            if (bit_index > 7) {
                bit_index = 0;
                output++;
                *output = 0;
            }
            *output |= (((input[i] & in_mask) >> (bit_width - k - 1)) << (7 - bit_index));
            in_mask >>= 1;
            bit_index++;
        }
    }
}

static void BM_BitPack(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;

    std::default_random_engine e;
    std::uniform_int_distribution<int64_t> u;
    std::vector<__int128_t> test_data(n);
    __int128_t in_mask = ((__int128_t(1)) << w) - 1;
    for (int i = 0; i < n; i++) {
        test_data[i] = u(e) & in_mask;
    }

    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}

static void BM_BitPackOptimized(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;

    std::default_random_engine e;
    std::uniform_int_distribution<int64_t> u;
    std::vector<__int128_t> test_data(n);
    __int128_t in_mask = ((__int128_t(1)) << w) - 1;
    for (int i = 0; i < n; i++) {
        test_data[i] = u(e) & in_mask;
    }

    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    ForEncoder<__int128_t> forEncoder(nullptr);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        forEncoder.bit_pack(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}

BENCHMARK(BM_BitPack)->DenseRange(1, 127, 16)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_BitPackOptimized)->DenseRange(1, 127, 16)->Unit(benchmark::kNanosecond);
} // namespace doris
