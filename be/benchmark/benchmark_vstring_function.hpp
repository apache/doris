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

#include "util/simd/vstring_function.h"

namespace doris {

static std::string create_test_data(size_t length, char fill_char = 'a') {
    return std::string(length, fill_char);
}
} // namespace doris

static void BM_VString_RTrim(benchmark::State& state) {
    size_t data_size = state.range(0);

    std::string test_data = 'a' + doris::create_test_data(data_size, ' ');

    const auto* begin = reinterpret_cast<const unsigned char*>(test_data.data());
    const auto* end = begin + test_data.size();
    doris::StringRef remove_str(" ");

    for (auto _ : state) {
        const auto* result = doris::simd::VStringFunctions::rtrim<true>(begin, end, remove_str);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * test_data.size());
}

static void BM_VString_LTrim(benchmark::State& state) {
    size_t data_size = state.range(0);

    std::string test_data = doris::create_test_data(data_size, ' ') + 'a';

    const auto* begin = reinterpret_cast<const unsigned char*>(test_data.data());
    const auto* end = begin + test_data.size();
    doris::StringRef remove_str(" ");

    for (auto _ : state) {
        const auto* result = doris::simd::VStringFunctions::ltrim<true>(begin, end, remove_str);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * test_data.size());
}

static void BM_VString_IsAscii(benchmark::State& state) {
    size_t data_size = state.range(0);

    std::string test_data = doris::create_test_data(data_size, 'a');
    doris::StringRef str(test_data.c_str(), test_data.size());

    for (auto _ : state) {
        bool result = doris::simd::VStringFunctions::is_ascii(str);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * test_data.size());
}

BENCHMARK(BM_VString_RTrim)
        ->Unit(benchmark::kNanosecond)
        ->Arg(16)   // 16 bytes
        ->Arg(32)   // 32 bytes
        ->Arg(64)   // 64 bytes
        ->Arg(256)  // 256 bytes
        ->Arg(1024) // 1KB
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

BENCHMARK(BM_VString_LTrim)
        ->Unit(benchmark::kNanosecond)
        ->Arg(16)   // 16 bytes
        ->Arg(32)   // 32 bytes
        ->Arg(64)   // 64 bytes
        ->Arg(256)  // 256 bytes
        ->Arg(1024) // 1KB
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

BENCHMARK(BM_VString_IsAscii)
        ->Unit(benchmark::kNanosecond)
        ->Arg(16)   // 16 bytes
        ->Arg(32)   // 32 bytes
        ->Arg(64)   // 64 bytes
        ->Arg(256)  // 256 bytes
        ->Arg(1024) // 1KB
        ->Repetitions(5)
        ->DisplayAggregatesOnly();
