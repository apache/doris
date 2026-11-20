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
#include <string>
#include <vector>

#include "util/md5.h"

namespace doris {
namespace {

void md5_serial_hex_batch(const unsigned char* const inputs[], const size_t lengths[],
                          char* outputs, size_t count) {
    static constexpr char digits[] = "0123456789abcdef";
    for (size_t i = 0; i < count; ++i) {
        unsigned char digest[MD5_DIGEST_LENGTH];
        MD5(inputs[i], lengths[i], digest);
        for (size_t j = 0; j < MD5_DIGEST_LENGTH; ++j) {
            outputs[i * MD5_HEX_LENGTH + 2 * j] = digits[digest[j] >> 4];
            outputs[i * MD5_HEX_LENGTH + 2 * j + 1] = digits[digest[j] & 15];
        }
    }
}

struct Md5BenchmarkData {
    Md5BenchmarkData(size_t count, int64_t length)
            : strings(count), inputs(count), lengths(count), output(count * MD5_HEX_LENGTH) {
        for (size_t i = 0; i < count; ++i) {
            // -1 mixes block counts; -2 puts a long message beside seven short messages.
            const size_t size = length == -1   ? (i * 131 + 17) % 1025
                                : length == -2 ? (i % 8 == 0 ? 8192 : 16)
                                               : static_cast<size_t>(length);
            strings[i].resize(size);
            for (size_t j = 0; j < size; ++j) {
                strings[i][j] = static_cast<char>((i * 37 + j * 13) & 255);
            }
            inputs[i] = reinterpret_cast<const unsigned char*>(strings[i].data());
            lengths[i] = size;
            bytes += size;
        }
    }

    std::vector<std::string> strings;
    std::vector<const unsigned char*> inputs;
    std::vector<size_t> lengths;
    std::vector<char> output;
    int64_t bytes = 0;
};

template <bool BATCH>
void BM_Md5(benchmark::State& state) {
    const size_t count = state.range(0);
    Md5BenchmarkData data(count, state.range(1));
    for (auto _ : state) {
        if constexpr (BATCH) {
            md5_hex_batch(data.inputs.data(), data.lengths.data(), data.output.data(), count);
        } else {
            md5_serial_hex_batch(data.inputs.data(), data.lengths.data(), data.output.data(),
                                 count);
        }
        benchmark::DoNotOptimize(data.output.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * count);
    state.SetBytesProcessed(state.iterations() * data.bytes);
}

BENCHMARK_TEMPLATE(BM_Md5, false)
        ->Name("MD5/serial")
        ->ArgsProduct({{1, 4, 8, 64, 4096}, {0, 16, 64, 256, 1024, -1, -2}});
BENCHMARK_TEMPLATE(BM_Md5, true)
        ->Name("MD5/batch")
        ->ArgsProduct({{1, 4, 8, 64, 4096}, {0, 16, 64, 256, 1024, -1, -2}});

} // namespace
} // namespace doris
