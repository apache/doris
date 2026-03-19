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
#include <vector>
#include <random>

#include "util/simd/bits.h"

namespace doris {

static void create_test_data(std::vector<uint8_t> &data, size_t size) {
    std::mt19937 rng(123);
    for (int i = 0; i < size; ++i) {
        data[i] = rng() & 1;
    }
}

inline uint32_t bitmask_8(const uint8_t* data) {
    static constexpr uint8_t mask_arr[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    svbool_t pg = svwhilelt_b8(0, 16);
    svuint8_t mask_vec = svld1_u8(pg, mask_arr);

    svbool_t nonzero = svcmpne_n_u8(pg, svld1_u8(pg, data), 0);
    svuint8_t masked = svsel_u8(nonzero, mask_vec, svdup_u8(0));

    svuint8_t idx = svindex_u8(0, 1);
    svuint8_t rev_idx = svsub_u8_x(pg, svdup_u8(15), idx);
    svuint8_t res_hi = svtbl_u8(masked, rev_idx);

    uint32_t mask = static_cast<uint32_t>(svaddv_u16(pg, svreinterpret_u16_u8(svzip1_u8(masked, res_hi))));
    return mask;
}

uint32_t bytes32_mask_to_bits32_mask(const uint8_t* data) {
    return bitmask_8(data) | (bitmask_8(data + 16) << 16);
}  

static void BM_bytes32_mask_to_bits32_mask(benchmark::State& state) {
    const size_t size = state.range(0);

    std::vector<uint8_t> data(size);
    create_test_data(data, size);

    for (auto _ : state) {
        uint32_t acc = 0;

        for (size_t i = 0; i + 32 <= size; i += 32) {
            acc ^= simd::bytes32_mask_to_bits32_mask(data.data() + i);
        }

        benchmark::DoNotOptimize(acc);
    }

    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * size
    );
}

static void BM_bytes32_mask_to_bits32_mask_2(benchmark::State& state) {
    const size_t size = state.range(0);

    std::vector<uint8_t> data(size);
    create_test_data(data, size);

    for (auto _ : state) {
        uint32_t acc = 0;

        for (size_t i = 0; i + 32 <= size; i += 32) {
            acc ^= bytes32_mask_to_bits32_mask(data.data() + i);
        }

        benchmark::DoNotOptimize(acc);
    }

    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * size
    );
}

BENCHMARK(BM_bytes32_mask_to_bits32_mask)
        ->Unit(benchmark::kNanosecond)
        ->Arg(32)   // 32 bytes
        ->Arg(64)   // 64 bytes
        ->Arg(256)  // 256 bytes
        ->Arg(1024) // 1KB
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

BENCHMARK(BM_bytes32_mask_to_bits32_mask_2)
        ->Unit(benchmark::kNanosecond)
        ->Arg(32)   // 32 bytes
        ->Arg(64)   // 64 bytes
        ->Arg(256)  // 256 bytes
        ->Arg(1024) // 1KB
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

} // namespace doris

