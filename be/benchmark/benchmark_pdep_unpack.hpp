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

constexpr int kPdepUnpackNumValues = 4096;
constexpr int kPdepUnpackBatchSize = 32;

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
    explicit PdepUnpackBenchmarkData(int bit_width)
            : input(kPdepUnpackNumValues * bit_width / 8),
              scalar_output(kPdepUnpackNumValues),
              pdep_output(kPdepUnpackNumValues) {
        std::mt19937_64 rng(0x17993);
        for (auto& byte : input) {
            byte = static_cast<uint8_t>(rng());
        }
        unpack<T, false>(bit_width, input.data(), kPdepUnpackNumValues, scalar_output.data());
        unpack<T, true>(bit_width, input.data(), kPdepUnpackNumValues, pdep_output.data());
    }

    std::vector<uint8_t> input;
    std::vector<T> scalar_output;
    std::vector<T> pdep_output;
};

template <typename T>
void BM_ScalarUnpack(benchmark::State& state) {
    const int bit_width = static_cast<int>(state.range(0));
    if (!PdepUnpack::is_supported()) {
        state.SkipWithError("CPU does not support BMI2 and AVX2");
        return;
    }
    PdepUnpackBenchmarkData<T> data(bit_width);
    if (data.scalar_output != data.pdep_output) {
        state.SkipWithError("PDEP+AVX2 output differs from scalar output");
        return;
    }
    for (auto _ : state) {
        unpack<T, false>(bit_width, data.input.data(), kPdepUnpackNumValues,
                         data.scalar_output.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * kPdepUnpackNumValues);
}

template <typename T>
void BM_PdepUnpack(benchmark::State& state) {
    const int bit_width = static_cast<int>(state.range(0));
    if (!PdepUnpack::is_supported()) {
        state.SkipWithError("CPU does not support BMI2 and AVX2");
        return;
    }
    PdepUnpackBenchmarkData<T> data(bit_width);
    if (data.scalar_output != data.pdep_output) {
        state.SkipWithError("PDEP+AVX2 output differs from scalar output");
        return;
    }
    for (auto _ : state) {
        unpack<T, true>(bit_width, data.input.data(), kPdepUnpackNumValues,
                        data.pdep_output.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * kPdepUnpackNumValues);
}

BENCHMARK_TEMPLATE(BM_ScalarUnpack, uint8_t)->DenseRange(1, 8);
BENCHMARK_TEMPLATE(BM_PdepUnpack, uint8_t)->DenseRange(1, 8);
BENCHMARK_TEMPLATE(BM_ScalarUnpack, uint16_t)->DenseRange(1, 16);
BENCHMARK_TEMPLATE(BM_PdepUnpack, uint16_t)->DenseRange(1, 16);
BENCHMARK_TEMPLATE(BM_ScalarUnpack, uint32_t)->DenseRange(1, 32);
BENCHMARK_TEMPLATE(BM_PdepUnpack, uint32_t)->DenseRange(1, 32);

} // namespace
} // namespace doris

#endif
