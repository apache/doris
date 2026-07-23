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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

#include "parquet_benchmark_scenarios.h"
#include "util/simd/parquet_kernels.h"

namespace doris::parquet_benchmark {
namespace detail {

constexpr size_t KERNEL_ROWS = 1UL << 16;

template <typename T>
void run_kernel(benchmark::State& state, const KernelScenario& scenario) {
    constexpr size_t width = sizeof(T);
    std::vector<T> input(KERNEL_ROWS);
    for (size_t row = 0; row < input.size(); ++row) {
        input[row] = scenario.kernel == Kernel::RAW_PREDICATE ? static_cast<T>(row % 100)
                                                              : static_cast<T>((row * 17) % 1009);
    }
    std::vector<T> output(KERNEL_ROWS);

    std::vector<uint8_t> encoded(KERNEL_ROWS * width);
    for (size_t row = 0; row < KERNEL_ROWS; ++row) {
        for (size_t byte = 0; byte < width; ++byte) {
            encoded[byte * KERNEL_ROWS + row] =
                    reinterpret_cast<const uint8_t*>(input.data())[row * width + byte];
        }
    }

    std::vector<T> dictionary(scenario.dictionary_entries);
    for (size_t row = 0; row < dictionary.size(); ++row) {
        dictionary[row] = static_cast<T>(row * 31);
    }
    std::vector<uint32_t> ids(KERNEL_ROWS);
    for (size_t row = 0; row < ids.size(); ++row) {
        ids[row] = static_cast<uint32_t>((row * 13) % dictionary.size());
    }

    std::vector<uint8_t> nulls(KERNEL_ROWS, 0);
    const size_t null_count = KERNEL_ROWS * static_cast<size_t>(scenario.null_percent) / 100;
    if (scenario.pattern == Pattern::CLUSTERED) {
        std::fill_n(nulls.begin(), null_count, uint8_t {1});
    } else if (null_count != 0) {
        for (size_t value = 0; value < null_count; ++value) {
            nulls[value * KERNEL_ROWS / null_count] = 1;
        }
    }
    std::vector<T> compact;
    compact.reserve(KERNEL_ROWS - null_count);
    for (size_t row = 0; row < KERNEL_ROWS; ++row) {
        if (nulls[row] == 0) {
            compact.push_back(input[row]);
        }
    }

    const T literal = static_cast<T>(scenario.selectivity_percent);
    std::vector<uint8_t> matches(KERNEL_ROWS, 1);

    switch (scenario.kernel) {
    case Kernel::BYTE_STREAM_SPLIT:
        parquet_simd::byte_stream_split_decode(encoded.data(), width, 0, KERNEL_ROWS, KERNEL_ROWS,
                                               reinterpret_cast<uint8_t*>(output.data()));
        if (output != input) {
            state.SkipWithError("byte-stream-split kernel produced incorrect values");
            return;
        }
        break;
    case Kernel::DELTA_PREFIX_SUM: {
        if constexpr (std::is_integral_v<T>) {
            std::copy(input.begin(), input.end(), output.begin());
            auto expected = output;
            T expected_last = 7;
            using Unsigned = std::make_unsigned_t<T>;
            for (auto& value : expected) {
                value = static_cast<T>(static_cast<Unsigned>(value) +
                                       static_cast<Unsigned>(static_cast<T>(-3)) +
                                       static_cast<Unsigned>(expected_last));
                expected_last = value;
            }
            T last = 7;
            parquet_simd::delta_decode(output.data(), output.size(), static_cast<T>(-3), &last);
            if (output != expected || last != expected_last) {
                state.SkipWithError("delta prefix-sum kernel produced incorrect values");
                return;
            }
        } else {
            state.SkipWithError("delta prefix-sum requires an integer physical type");
            return;
        }
        break;
    }
    case Kernel::DICTIONARY_GATHER:
        parquet_simd::dictionary_gather(reinterpret_cast<const uint8_t*>(dictionary.data()),
                                        ids.data(), ids.size(), width,
                                        reinterpret_cast<uint8_t*>(output.data()));
        for (size_t row = 0; row < output.size(); ++row) {
            if (output[row] != dictionary[ids[row]]) {
                state.SkipWithError("dictionary gather kernel produced incorrect values");
                return;
            }
        }
        break;
    case Kernel::NULLABLE_EXPAND:
        std::copy(compact.begin(), compact.end(), output.begin());
        parquet_simd::expand_nullable_values(reinterpret_cast<uint8_t*>(output.data()),
                                             compact.size(), nulls.data(), nulls.size(), width);
        for (size_t row = 0; row < output.size(); ++row) {
            const T expected = nulls[row] == 0 ? input[row] : T {};
            if (output[row] != expected) {
                state.SkipWithError("nullable expansion kernel produced incorrect values");
                return;
            }
        }
        break;
    case Kernel::RAW_PREDICATE:
        parquet_simd::raw_compare(reinterpret_cast<const uint8_t*>(input.data()), input.size(),
                                  literal, parquet_simd::RawComparisonOp::LT, matches.data());
        for (size_t row = 0; row < matches.size(); ++row) {
            if (matches[row] != static_cast<uint8_t>(input[row] < literal)) {
                state.SkipWithError("raw predicate kernel produced incorrect values");
                return;
            }
        }
        break;
    }

    for (auto _ : state) {
        state.PauseTiming();
        if (scenario.kernel == Kernel::DELTA_PREFIX_SUM) {
            std::copy(input.begin(), input.end(), output.begin());
        } else if (scenario.kernel == Kernel::NULLABLE_EXPAND) {
            std::copy(compact.begin(), compact.end(), output.begin());
        } else if (scenario.kernel == Kernel::RAW_PREDICATE) {
            std::fill(matches.begin(), matches.end(), uint8_t {1});
        }
        state.ResumeTiming();

        switch (scenario.kernel) {
        case Kernel::BYTE_STREAM_SPLIT:
            parquet_simd::byte_stream_split_decode(encoded.data(), width, 0, KERNEL_ROWS,
                                                   KERNEL_ROWS,
                                                   reinterpret_cast<uint8_t*>(output.data()));
            break;
        case Kernel::DELTA_PREFIX_SUM: {
            if constexpr (std::is_integral_v<T>) {
                T last = 7;
                parquet_simd::delta_decode(output.data(), output.size(), static_cast<T>(-3), &last);
                benchmark::DoNotOptimize(last);
            }
            break;
        }
        case Kernel::DICTIONARY_GATHER:
            parquet_simd::dictionary_gather(reinterpret_cast<const uint8_t*>(dictionary.data()),
                                            ids.data(), ids.size(), width,
                                            reinterpret_cast<uint8_t*>(output.data()));
            break;
        case Kernel::NULLABLE_EXPAND:
            parquet_simd::expand_nullable_values(reinterpret_cast<uint8_t*>(output.data()),
                                                 compact.size(), nulls.data(), nulls.size(), width);
            break;
        case Kernel::RAW_PREDICATE:
            parquet_simd::raw_compare(reinterpret_cast<const uint8_t*>(input.data()), input.size(),
                                      literal, parquet_simd::RawComparisonOp::LT, matches.data());
            break;
        }
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(KERNEL_ROWS));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(KERNEL_ROWS * width));
    state.counters["rows"] = static_cast<double>(KERNEL_ROWS);
    state.counters["value_width"] = static_cast<double>(width);
}

inline bool register_kernel_benchmarks() {
    for (const auto& scenario : kernel_scenarios()) {
        const std::string name = "ParquetKernel/" + to_string(scenario.kernel) + "/" +
                                 to_string(scenario.value_type) + "/sel_" +
                                 std::to_string(scenario.selectivity_percent) + "/null_" +
                                 std::to_string(scenario.null_percent) + "/" +
                                 to_string(scenario.pattern) + "/dict_" +
                                 std::to_string(scenario.dictionary_entries);
        benchmark::RegisterBenchmark(name.c_str(), [=](benchmark::State& state) {
            switch (scenario.value_type) {
            case ValueType::INT32:
                run_kernel<int32_t>(state, scenario);
                break;
            case ValueType::INT64:
                run_kernel<int64_t>(state, scenario);
                break;
            case ValueType::FLOAT:
                run_kernel<float>(state, scenario);
                break;
            case ValueType::DOUBLE:
                run_kernel<double>(state, scenario);
                break;
            case ValueType::BYTE_ARRAY:
            case ValueType::FIXED_LEN_BYTE_ARRAY:
                state.SkipWithError("kernel benchmark requires a fixed-width primitive type");
                break;
            }
        })->Unit(benchmark::kNanosecond);
    }
    return true;
}

inline const bool KERNEL_BENCHMARKS_REGISTERED = register_kernel_benchmarks();

} // namespace detail
} // namespace doris::parquet_benchmark
