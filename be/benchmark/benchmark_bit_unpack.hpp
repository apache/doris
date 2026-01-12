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

#pragma once

#include <benchmark/benchmark.h>

#include <random>
#include <vector>

#include "util/frame_of_reference_coding.h"

namespace doris {

// original bit_unpack function
template <typename T>
void bit_unpack(const uint8_t* input, uint8_t in_num, int bit_width, T* output) {
    unsigned char in_mask = 0x80;
    int bit_index = 0;
    while (in_num > 0) {
        *output = 0;
        for (int i = 0; i < bit_width; i++) {
            if (bit_index > 7) {
                input++;
                bit_index = 0;
            }
            *output |= ((T)((*input & (in_mask >> bit_index)) >> (7 - bit_index)))
                       << (bit_width - i - 1);
            bit_index++;
        }
        output++;
        in_num--;
    }
}

static void BM_BitUnpack(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;

    std::default_random_engine e;
    std::uniform_int_distribution<__int128_t> u;
    ForEncoder<__int128_t> encoder(nullptr);
    ForDecoder<__int128_t> decoder(nullptr, 0);
    std::vector<__int128_t> test_data(n);
    __int128_t in_mask = (((__int128_t)1) << w) - 1;

    for (int i = 0; i < n; i++) {
        test_data[i] = u(e) & in_mask;
    }
    std::vector<uint8_t> o((n * w + 7) / 8);
    encoder.bit_pack(test_data.data(), n, w, o.data());

    std::vector<__int128_t> output(n);

    for (auto _ : state) {
        benchmark::DoNotOptimize(o.data());
        benchmark::DoNotOptimize(output.data());
        bit_unpack(o.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    int64_t size = o.size();
    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}

static void BM_BitUnpackOptimized(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;

    std::default_random_engine e;
    std::uniform_int_distribution<__int128_t> u;

    ForEncoder<__int128_t> encoder(nullptr);
    ForDecoder<__int128_t> decoder(nullptr, 0);
    std::vector<__int128_t> test_data(n);
    __int128_t in_mask = (((__int128_t)1) << w) - 1;

    for (int i = 0; i < n; i++) {
        test_data[i] = u(e) & in_mask;
    }
    std::vector<uint8_t> o((n * w + 7) / 8);
    encoder.bit_pack(test_data.data(), n, w, o.data());

    std::vector<__int128_t> output(n);

    for (auto _ : state) {
        benchmark::DoNotOptimize(o.data());
        benchmark::DoNotOptimize(output.data());
        decoder.bit_unpack(o.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    int64_t size = o.size();
    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}

BENCHMARK(BM_BitUnpack)->DenseRange(1, 127)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_BitUnpackOptimized)->DenseRange(1, 127)->Unit(benchmark::kNanosecond);
} // namespace doris