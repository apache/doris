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

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

#include "util/bit_stream_utils.inline.h"
#include "util/pdep_unpack.h"

namespace doris {
namespace {

constexpr int kPdepUnpackBatchSize = 32;
constexpr int kPdepUnpackL1NumValues = 1 << 12;
constexpr int kPdepUnpackL2NumValues = 1 << 18;
constexpr int kPdepUnpackLargeNumValues = 1 << 20;
const std::vector<int64_t> kPdepUnpackNumValues = {
        32, 64, 128, kPdepUnpackL1NumValues, kPdepUnpackL2NumValues, kPdepUnpackLargeNumValues};

template <typename T, int BIT_WIDTH>
void scalar_unpack(const uint8_t* input, int num_values, T* output) {
    constexpr int bytes_per_batch = kPdepUnpackBatchSize * BIT_WIDTH / 8;
    for (int i = 0; i < num_values; i += kPdepUnpackBatchSize) {
        BitPacking::Unpack32Values<T, BIT_WIDTH>(input, bytes_per_batch, output + i);
        input += bytes_per_batch;
    }
}

template <typename T, int BIT_WIDTH>
void pdep_unpack(const uint8_t* input, int num_values, T* output) {
    for (int i = 0; i < num_values; i += kPdepUnpackBatchSize) {
        PdepUnpack::unpack32<T, BIT_WIDTH>(input, output + i);
        input += kPdepUnpackBatchSize * BIT_WIDTH / 8;
    }
}

template <typename T, int BIT_WIDTH, bool USE_PDEP>
void unpack_if_supported(const uint8_t* input, int num_values, T* output) {
    if constexpr (PdepUnpack::is_supported_type<T, BIT_WIDTH>()) {
        if constexpr (USE_PDEP) {
            pdep_unpack<T, BIT_WIDTH>(input, num_values, output);
        } else {
            scalar_unpack<T, BIT_WIDTH>(input, num_values, output);
        }
    } else {
        __builtin_unreachable();
    }
}

template <typename T, bool USE_PDEP>
void unpack(int bit_width, const uint8_t* input, int num_values, T* output) {
#define UNPACK_CASE(width)                                                  \
    case width:                                                             \
        unpack_if_supported<T, width, USE_PDEP>(input, num_values, output); \
        return
    switch (bit_width) {
        UNPACK_CASE(1);
        UNPACK_CASE(2);
        UNPACK_CASE(3);
        UNPACK_CASE(4);
        UNPACK_CASE(5);
        UNPACK_CASE(6);
        UNPACK_CASE(7);
        UNPACK_CASE(8);
        UNPACK_CASE(9);
        UNPACK_CASE(10);
        UNPACK_CASE(11);
        UNPACK_CASE(12);
        UNPACK_CASE(13);
        UNPACK_CASE(14);
        UNPACK_CASE(15);
        UNPACK_CASE(16);
        UNPACK_CASE(17);
        UNPACK_CASE(18);
        UNPACK_CASE(19);
        UNPACK_CASE(20);
        UNPACK_CASE(21);
        UNPACK_CASE(22);
        UNPACK_CASE(23);
        UNPACK_CASE(24);
        UNPACK_CASE(25);
        UNPACK_CASE(26);
        UNPACK_CASE(27);
        UNPACK_CASE(28);
        UNPACK_CASE(29);
        UNPACK_CASE(30);
        UNPACK_CASE(31);
        UNPACK_CASE(32);
    default:
        __builtin_unreachable();
    }
#undef UNPACK_CASE
}

template <typename T>
struct PdepUnpackBenchmarkData {
    PdepUnpackBenchmarkData(int bit_width, int num_values)
            : input(num_values * bit_width / 8),
              scalar_output(num_values),
              pdep_output(num_values) {
        std::mt19937_64 rng(0x17993);
        for (auto& byte : input) {
            byte = static_cast<uint8_t>(rng());
        }
        unpack<T, false>(bit_width, input.data(), num_values, scalar_output.data());
    }

    std::vector<uint8_t> input;
    std::vector<T> scalar_output;
    std::vector<T> pdep_output;
};

template <typename T>
void BM_ScalarUnpack(benchmark::State& state) {
    const int bit_width = static_cast<int>(state.range(0));
    const int num_values = static_cast<int>(state.range(1));
    PdepUnpackBenchmarkData<T> data(bit_width, num_values);
    for (auto _ : state) {
        unpack<T, false>(bit_width, data.input.data(), num_values, data.scalar_output.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
}

template <typename T>
void BM_PdepUnpack(benchmark::State& state) {
    const int bit_width = static_cast<int>(state.range(0));
    const int num_values = static_cast<int>(state.range(1));
    if (!PdepUnpack::is_supported()) {
        state.SkipWithError("CPU does not support BMI2 and AVX2");
        return;
    }
    PdepUnpackBenchmarkData<T> data(bit_width, num_values);
    unpack<T, true>(bit_width, data.input.data(), num_values, data.pdep_output.data());
    if (data.scalar_output != data.pdep_output) {
        state.SkipWithError("PDEP+AVX2 output differs from scalar output");
        return;
    }
    for (auto _ : state) {
        unpack<T, true>(bit_width, data.input.data(), num_values, data.pdep_output.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
}

template <typename T>
void BM_ActualUnpack(benchmark::State& state) {
    const int bit_width = static_cast<int>(state.range(0));
    const int num_values = static_cast<int>(state.range(1));
    PdepUnpackBenchmarkData<T> data(bit_width, num_values);
    auto result = BitPacking::UnpackValues(bit_width, data.input.data(), data.input.size(),
                                           num_values, data.pdep_output.data());
    if (data.scalar_output != data.pdep_output) {
        state.SkipWithError("Actual output differs from scalar output");
        return;
    }
    for (auto _ : state) {
        result = BitPacking::UnpackValues(bit_width, data.input.data(), data.input.size(),
                                          num_values, data.pdep_output.data());
        benchmark::DoNotOptimize(result);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
}

#define REGISTER_PDEP_UNPACK_BENCHMARK(type, max_bit_width)                                    \
    BENCHMARK_TEMPLATE(BM_ScalarUnpack, type)                                                  \
            ->ArgsProduct(                                                                     \
                    {benchmark::CreateDenseRange(1, max_bit_width, 1), kPdepUnpackNumValues}); \
    BENCHMARK_TEMPLATE(BM_PdepUnpack, type)                                                    \
            ->ArgsProduct(                                                                     \
                    {benchmark::CreateDenseRange(1, max_bit_width, 1), kPdepUnpackNumValues}); \
    BENCHMARK_TEMPLATE(BM_ActualUnpack, type)                                                  \
            ->ArgsProduct(                                                                     \
                    {benchmark::CreateDenseRange(1, max_bit_width, 1), kPdepUnpackNumValues})

REGISTER_PDEP_UNPACK_BENCHMARK(uint8_t, 8);
REGISTER_PDEP_UNPACK_BENCHMARK(uint16_t, 16);
REGISTER_PDEP_UNPACK_BENCHMARK(uint32_t, 32);

#undef REGISTER_PDEP_UNPACK_BENCHMARK

} // namespace
} // namespace doris

#endif
