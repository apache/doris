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

#include "format_v2/parquet/reader/native/common.h"
#include "parquet_benchmark_scenarios.h"
#include "util/byte_stream_split.h"
#include "util/simd/parquet_kernels.h"

namespace doris::parquet_benchmark {
namespace detail {

constexpr size_t KERNEL_ROWS = 1UL << 16;
constexpr size_t NESTED_VALUES_PER_ROW = 8;

inline void run_nested_selection_kernel(benchmark::State& state, const KernelScenario& scenario) {
    using format::parquet::native::ColumnSelectVector;
    using format::parquet::native::FilterMap;
    using format::parquet::native::level_t;

    std::vector<level_t> source_repetition_levels;
    std::vector<level_t> source_definition_levels;
    source_repetition_levels.reserve(KERNEL_ROWS * NESTED_VALUES_PER_ROW);
    source_definition_levels.reserve(KERNEL_ROWS * NESTED_VALUES_PER_ROW);
    for (size_t row = 0; row < KERNEL_ROWS; ++row) {
        if (row % 10 == 0) {
            source_repetition_levels.push_back(0);
            source_definition_levels.push_back(0);
            continue;
        }
        for (size_t value = 0; value < NESTED_VALUES_PER_ROW; ++value) {
            source_repetition_levels.push_back(value == 0 ? 0 : 1);
            source_definition_levels.push_back((row + value) % 10 == 0 ? 2 : 3);
        }
    }

    const auto parent_selection =
            make_selection_plan(KERNEL_ROWS, scenario.selectivity_percent, scenario.pattern);
    std::vector<uint8_t> parent_filter_data(KERNEL_ROWS, 0);
    visit_selected_rows(parent_selection,
                        [&](size_t row) { parent_filter_data[row] = uint8_t {1}; });
    FilterMap parent_filter;
    auto status = parent_filter.init(parent_filter_data.data(), parent_filter_data.size(), false);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }

    std::vector<level_t> repetition_levels = source_repetition_levels;
    std::vector<level_t> definition_levels = source_definition_levels;
    ColumnSelectVector selection;
    NullMap selected_nulls;
    size_t ancestor_null_count = 0;
    status = selection.init_nested(&repetition_levels, &definition_levels, 0,
                                   /*repeated_parent_def_level=*/2,
                                   /*definition_level=*/3, &selected_nulls, &parent_filter, 0,
                                   &ancestor_null_count);
    if (!status.ok() || repetition_levels.size() != definition_levels.size() ||
        selection.num_values() + ancestor_null_count != source_repetition_levels.size()) {
        state.SkipWithError(status.ok() ? "nested selection produced inconsistent counts"
                                        : status.to_string().c_str());
        return;
    }

    for (auto _ : state) {
        state.PauseTiming();
        repetition_levels = source_repetition_levels;
        definition_levels = source_definition_levels;
        selected_nulls.clear();
        state.ResumeTiming();
        status = selection.init_nested(&repetition_levels, &definition_levels, 0,
                                       /*repeated_parent_def_level=*/2,
                                       /*definition_level=*/3, &selected_nulls, &parent_filter, 0,
                                       &ancestor_null_count);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            return;
        }
        auto* compacted_levels = repetition_levels.data();
        size_t filtered_values = selection.num_filtered();
        benchmark::DoNotOptimize(compacted_levels);
        benchmark::DoNotOptimize(filtered_values);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(source_repetition_levels.size()));
    state.SetBytesProcessed(
            static_cast<int64_t>(state.iterations()) *
            static_cast<int64_t>(source_repetition_levels.size() * 2 * sizeof(level_t)));
    state.counters["parent_rows"] = static_cast<double>(KERNEL_ROWS);
    state.counters["selected_parent_rows"] = static_cast<double>(parent_selection.selected_rows);
    state.counters["level_entries"] = static_cast<double>(source_repetition_levels.size());
}

inline void decode_byte_stream_split(const uint8_t* src, size_t width, size_t offset,
                                     size_t num_values, size_t stride, uint8_t* dest) {
    if (!simd::try_byte_stream_split_decode(src, width, offset, num_values, stride, dest)) {
        doris::byte_stream_split_decode(src, static_cast<int>(width), offset, num_values, stride,
                                        dest);
    }
}

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
        decode_byte_stream_split(encoded.data(), width, 0, KERNEL_ROWS, KERNEL_ROWS,
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
            simd::delta_decode(output.data(), output.size(), static_cast<T>(-3), &last);
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
        simd::dictionary_gather(reinterpret_cast<const uint8_t*>(dictionary.data()), ids.data(),
                                ids.size(), width, reinterpret_cast<uint8_t*>(output.data()));
        for (size_t row = 0; row < output.size(); ++row) {
            if (output[row] != dictionary[ids[row]]) {
                state.SkipWithError("dictionary gather kernel produced incorrect values");
                return;
            }
        }
        break;
    case Kernel::NULLABLE_EXPAND:
        std::copy(compact.begin(), compact.end(), output.begin());
        simd::expand_nullable_values(reinterpret_cast<uint8_t*>(output.data()), compact.size(),
                                     nulls.data(), nulls.size(), width);
        for (size_t row = 0; row < output.size(); ++row) {
            const T expected = nulls[row] == 0 ? input[row] : T {};
            if (output[row] != expected) {
                state.SkipWithError("nullable expansion kernel produced incorrect values");
                return;
            }
        }
        break;
    case Kernel::RAW_PREDICATE:
        simd::raw_compare(reinterpret_cast<const uint8_t*>(input.data()), input.size(), literal,
                          simd::RawComparisonOp::LT, matches.data());
        for (size_t row = 0; row < matches.size(); ++row) {
            if (matches[row] != static_cast<uint8_t>(input[row] < literal)) {
                state.SkipWithError("raw predicate kernel produced incorrect values");
                return;
            }
        }
        break;
    case Kernel::NESTED_SELECTION:
        state.SkipWithError("nested selection uses its dedicated level kernel");
        return;
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
            decode_byte_stream_split(encoded.data(), width, 0, KERNEL_ROWS, KERNEL_ROWS,
                                     reinterpret_cast<uint8_t*>(output.data()));
            break;
        case Kernel::DELTA_PREFIX_SUM: {
            if constexpr (std::is_integral_v<T>) {
                T last = 7;
                simd::delta_decode(output.data(), output.size(), static_cast<T>(-3), &last);
                benchmark::DoNotOptimize(last);
            }
            break;
        }
        case Kernel::DICTIONARY_GATHER:
            simd::dictionary_gather(reinterpret_cast<const uint8_t*>(dictionary.data()), ids.data(),
                                    ids.size(), width, reinterpret_cast<uint8_t*>(output.data()));
            break;
        case Kernel::NULLABLE_EXPAND:
            simd::expand_nullable_values(reinterpret_cast<uint8_t*>(output.data()), compact.size(),
                                         nulls.data(), nulls.size(), width);
            break;
        case Kernel::RAW_PREDICATE:
            simd::raw_compare(reinterpret_cast<const uint8_t*>(input.data()), input.size(), literal,
                              simd::RawComparisonOp::LT, matches.data());
            break;
        case Kernel::NESTED_SELECTION:
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
            if (scenario.kernel == Kernel::NESTED_SELECTION) {
                run_nested_selection_kernel(state, scenario);
                return;
            }
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
