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

#include <cstdint>
#include <vector>

#include "util/hash_util.hpp"

namespace doris {
namespace {

enum class Crc32BenchPath { ZLIB, ZLIB_Z, OPTIMIZED };

template <Crc32BenchPath path, bool seeded>
void BM_Crc32Strings(benchmark::State& state) {
    constexpr size_t rows = 1024;
    constexpr size_t mixed_lengths[] = {7, 32, 63, 128, 255, 256, 1024, 4096};
    std::vector<uint8_t> chars(1);
    std::vector<size_t> offsets(rows + 1, 1);
    std::vector<uint32_t> hashes(rows);
    auto* hash_data = hashes.data();
    for (size_t row = 0; row < rows; ++row) {
        size_t length;
        if (state.range(0) == -1) {
            length = (row * 37 + 11) % 256;
        } else if (state.range(0) == -2) {
            length = mixed_lengths[row % 8];
        } else {
            length = static_cast<size_t>(state.range(0));
        }
        offsets[row + 1] = offsets[row] + length;
        chars.resize(offsets[row + 1]);
        for (size_t i = 0; i < length; ++i) {
            chars[offsets[row] + i] = static_cast<uint8_t>(row * 17 + i * 37);
        }
    }

    for (auto _ : state) {
        for (size_t row = 0; row < rows; ++row) {
            const auto* data = chars.data() + offsets[row];
            size_t length = offsets[row + 1] - offsets[row];
            uint32_t seed = seeded ? static_cast<uint32_t>(row * 2654435761U) : 0;
            if constexpr (path == Crc32BenchPath::ZLIB) {
                hashes[row] = static_cast<uint32_t>(crc32(seed, data, static_cast<uInt>(length)));
            } else if constexpr (path == Crc32BenchPath::ZLIB_Z) {
                hashes[row] = static_cast<uint32_t>(crc32_z(seed, data, length));
            } else {
                hashes[row] = HashUtil::zlib_crc_hash(data, length, seed);
            }
        }
        benchmark::DoNotOptimize(hash_data);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows);
    state.SetBytesProcessed(state.iterations() * (chars.size() - 1));
}

void crc32_string_args(benchmark::internal::Benchmark* benchmark) {
    for (int64_t length : {0,  1,   3,   4,   5,   7,   8,   9,    16,   32, 63, 64,
                           65, 128, 192, 255, 256, 257, 512, 1024, 4096, -1, -2}) {
        benchmark->Arg(length);
    }
}

BENCHMARK_TEMPLATE(BM_Crc32Strings, Crc32BenchPath::ZLIB, true)->Apply(crc32_string_args);
BENCHMARK_TEMPLATE(BM_Crc32Strings, Crc32BenchPath::ZLIB_Z, false)->Apply(crc32_string_args);
BENCHMARK_TEMPLATE(BM_Crc32Strings, Crc32BenchPath::OPTIMIZED, true)->Apply(crc32_string_args);
BENCHMARK_TEMPLATE(BM_Crc32Strings, Crc32BenchPath::OPTIMIZED, false)->Apply(crc32_string_args);

} // namespace
} // namespace doris
