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
#include <climits>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "format_v2/parquet/reader/native/common.h"
#include "parquet_benchmark_scenarios.h"
#include "util/byte_stream_split.h"
#include "util/simd/parquet_kernels.h"

namespace doris::parquet_benchmark {
namespace detail {

constexpr size_t KERNEL_ROWS = 1UL << 16;
constexpr size_t NESTED_VALUES_PER_ROW = 8;

using NestedReadType = format::parquet::native::ColumnSelectVector::DataReadType;
using NestedLevel = format::parquet::native::level_t;

struct NestedSelectionOracle {
    std::vector<NestedLevel> repetition_levels;
    std::vector<NestedLevel> definition_levels;
    NullMap selected_nulls;
    std::vector<NestedReadType> reads;
    size_t ancestor_null_count = 0;
    size_t filtered_count = 0;
};

struct NestedSelectionScratch {
    std::vector<NestedLevel> repetition_levels;
    std::vector<NestedLevel> definition_levels;
    std::vector<uint8_t> nested_filter_data;
    std::vector<uint16_t> null_runs;
    std::unordered_set<size_t> ancestor_null_indices;
    format::parquet::native::FilterMap nested_filter;
    format::parquet::native::ColumnSelectVector selection;
    NullMap selected_nulls;
    size_t ancestor_null_count = 0;
};

struct NullableSelectionScratch {
    format::parquet::native::ColumnSelectVector legacy_selection;
    ParquetSelection physical_selection;
    NullMap output_nulls;
    NullMap selected_nulls;
    size_t num_filtered = 0;
};

inline void append_nullable_run(std::vector<uint16_t>* runs, bool is_null, size_t run_length,
                                bool* previous_is_null) {
    if (runs->empty()) {
        if (is_null) {
            runs->push_back(0);
        }
    } else if (*previous_is_null == is_null) {
        runs->push_back(0);
    }
    while (run_length > USHRT_MAX) {
        runs->push_back(USHRT_MAX);
        runs->push_back(0);
        run_length -= USHRT_MAX;
    }
    runs->push_back(static_cast<uint16_t>(run_length));
    *previous_is_null = is_null;
}

inline std::vector<uint16_t> build_nullable_runs(const NullMap& nulls) {
    std::vector<uint16_t> runs;
    bool previous_is_null = false;
    size_t row = 0;
    while (row < nulls.size()) {
        const bool is_null = nulls[row] != 0;
        const size_t begin = row++;
        while (row < nulls.size() && (nulls[row] != 0) == is_null) {
            ++row;
        }
        append_nullable_run(&runs, is_null, row - begin, &previous_is_null);
    }
    return runs;
}

inline Status run_legacy_nullable_selection(NullableSelectionScratch* scratch,
                                            const std::vector<uint16_t>& null_runs,
                                            size_t num_values,
                                            format::parquet::native::FilterMap* filter) {
    using ReadType = format::parquet::native::ColumnSelectVector::DataReadType;
    scratch->output_nulls.clear();
    scratch->selected_nulls.clear();
    scratch->physical_selection.ranges.clear();
    scratch->physical_selection.total_values = 0;
    scratch->physical_selection.selected_values = 0;
    RETURN_IF_ERROR(scratch->legacy_selection.init(null_runs, num_values, &scratch->output_nulls,
                                                   filter, 0));
    scratch->num_filtered = scratch->legacy_selection.num_filtered();

    size_t physical_cursor = 0;
    ReadType type;
    while (const size_t run_length = scratch->legacy_selection.get_next_run<true>(&type)) {
        switch (type) {
        case ReadType::CONTENT:
            if (!scratch->physical_selection.ranges.empty() &&
                scratch->physical_selection.ranges.back().first +
                                scratch->physical_selection.ranges.back().count ==
                        physical_cursor) {
                scratch->physical_selection.ranges.back().count += run_length;
            } else {
                scratch->physical_selection.ranges.push_back(
                        {.first = physical_cursor, .count = run_length});
            }
            scratch->physical_selection.selected_values += run_length;
            scratch->selected_nulls.resize_fill(scratch->selected_nulls.size() + run_length, 0);
            physical_cursor += run_length;
            break;
        case ReadType::NULL_DATA:
            scratch->selected_nulls.resize_fill(scratch->selected_nulls.size() + run_length, 1);
            break;
        case ReadType::FILTERED_CONTENT:
            physical_cursor += run_length;
            break;
        case ReadType::FILTERED_NULL:
            break;
        }
    }
    scratch->physical_selection.total_values = physical_cursor;
    return Status::OK();
}

inline Status run_nullable_selection_once(NullableSelectionScratch* scratch,
                                          const std::vector<uint16_t>& null_runs, size_t num_values,
                                          size_t num_nulls,
                                          format::parquet::native::FilterMap* filter,
                                          NullableSelectionImplementation implementation) {
    if (implementation == NullableSelectionImplementation::LEGACY) {
        return run_legacy_nullable_selection(scratch, null_runs, num_values, filter);
    }
    scratch->output_nulls.clear();
    return format::parquet::native::build_filtered_nullable_selection(
            null_runs, num_values, num_nulls, &scratch->output_nulls, filter, 0,
            &scratch->physical_selection, &scratch->selected_nulls, &scratch->num_filtered);
}

inline bool equal_selection(const ParquetSelection& lhs, const ParquetSelection& rhs) {
    if (lhs.total_values != rhs.total_values || lhs.selected_values != rhs.selected_values ||
        lhs.ranges.size() != rhs.ranges.size()) {
        return false;
    }
    for (size_t range = 0; range < lhs.ranges.size(); ++range) {
        if (lhs.ranges[range].first != rhs.ranges[range].first ||
            lhs.ranges[range].count != rhs.ranges[range].count) {
            return false;
        }
    }
    return true;
}

inline void run_nullable_selection_kernel(benchmark::State& state,
                                          const NullableSelectionScenario& scenario) {
    using format::parquet::native::FilterMap;

    std::vector<uint8_t> filter_data(KERNEL_ROWS, 0);
    const auto selected = make_selection_plan(KERNEL_ROWS, scenario.selectivity_percent,
                                              scenario.selection_pattern);
    visit_selected_rows(selected, [&](size_t row) { filter_data[row] = 1; });
    FilterMap filter;
    auto status = filter.init(filter_data.data(), filter_data.size(), false);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }

    NullMap nulls;
    nulls.resize_fill(KERNEL_ROWS, 0);
    const auto null_plan =
            make_selection_plan(KERNEL_ROWS, scenario.null_percent, scenario.null_pattern);
    visit_selected_rows(null_plan, [&](size_t row) { nulls[row] = 1; });
    const auto null_runs = build_nullable_runs(nulls);

    NullableSelectionScratch legacy;
    NullableSelectionScratch fused;
    status = run_nullable_selection_once(&legacy, null_runs, KERNEL_ROWS, null_plan.selected_rows,
                                         &filter, NullableSelectionImplementation::LEGACY);
    if (status.ok()) {
        status =
                run_nullable_selection_once(&fused, null_runs, KERNEL_ROWS, null_plan.selected_rows,
                                            &filter, NullableSelectionImplementation::FUSED);
    }
    if (!status.ok() || !equal_selection(legacy.physical_selection, fused.physical_selection) ||
        legacy.output_nulls != fused.output_nulls ||
        legacy.selected_nulls != fused.selected_nulls ||
        legacy.num_filtered != fused.num_filtered) {
        if (status.ok()) {
            state.SkipWithError("nullable selection implementations disagree");
        } else {
            state.SkipWithError(status.to_string().c_str());
        }
        return;
    }

    NullableSelectionScratch scratch;
    status = run_nullable_selection_once(&scratch, null_runs, KERNEL_ROWS, null_plan.selected_rows,
                                         &filter, scenario.implementation);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }
    for (auto _ : state) {
        status = run_nullable_selection_once(&scratch, null_runs, KERNEL_ROWS,
                                             null_plan.selected_rows, &filter,
                                             scenario.implementation);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            return;
        }
        benchmark::DoNotOptimize(scratch.physical_selection.ranges.data());
        benchmark::DoNotOptimize(scratch.selected_nulls.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(KERNEL_ROWS));
    state.counters["rows"] = static_cast<double>(KERNEL_ROWS);
    state.counters["selected_rows"] = static_cast<double>(selected.selected_rows);
    state.counters["null_rows"] = static_cast<double>(null_plan.selected_rows);
}

inline NestedSelectionOracle build_nested_selection_oracle(
        const std::vector<NestedLevel>& repetition_levels,
        const std::vector<NestedLevel>& definition_levels,
        const std::vector<uint8_t>& parent_filter_data) {
    // Derive expectations from source levels so validation cannot inherit a mistake from either
    // measured implementation.
    NestedSelectionOracle oracle;
    size_t parent = 0;
    for (size_t level = 0; level < repetition_levels.size(); ++level) {
        if (level != 0 && repetition_levels[level] == 0) {
            ++parent;
        }
        const bool selected = parent_filter_data[parent] != 0;
        if (selected) {
            oracle.repetition_levels.push_back(repetition_levels[level]);
            oracle.definition_levels.push_back(definition_levels[level]);
        }
        if (definition_levels[level] < 2) {
            ++oracle.ancestor_null_count;
            continue;
        }
        const bool is_null = definition_levels[level] < 3;
        if (selected) {
            oracle.selected_nulls.push_back(static_cast<UInt8>(is_null));
            oracle.reads.push_back(is_null ? NestedReadType::NULL_DATA : NestedReadType::CONTENT);
        } else {
            ++oracle.filtered_count;
            oracle.reads.push_back(is_null ? NestedReadType::FILTERED_NULL
                                           : NestedReadType::FILTERED_CONTENT);
        }
    }
    return oracle;
}

inline void append_nested_null_run(std::vector<uint16_t>* null_runs, bool is_null,
                                   size_t run_length, bool* previous_is_null) {
    if (*previous_is_null == is_null && USHRT_MAX - null_runs->back() >= run_length) {
        null_runs->back() += static_cast<uint16_t>(run_length);
        return;
    }
    if (!(*previous_is_null ^ is_null)) {
        null_runs->push_back(0);
    }
    while (run_length > USHRT_MAX) {
        null_runs->push_back(USHRT_MAX);
        null_runs->push_back(0);
        run_length -= USHRT_MAX;
    }
    null_runs->push_back(static_cast<uint16_t>(run_length));
    *previous_is_null = is_null;
}

inline Status run_legacy_nested_selection(NestedSelectionScratch* scratch,
                                          format::parquet::native::FilterMap* parent_filter) {
    // Keep the pre-fusion passes selectable in the same binary so comparisons share compiler,
    // fixtures, and process state.
    scratch->nested_filter_data.resize(scratch->repetition_levels.size());
    size_t parent = 0;
    for (size_t level = 0; level < scratch->repetition_levels.size(); ++level) {
        if (level != 0 && scratch->repetition_levels[level] == 0) {
            ++parent;
        }
        scratch->nested_filter_data[level] = parent_filter->filter_map_data()[parent];
    }
    RETURN_IF_ERROR(scratch->nested_filter.init(scratch->nested_filter_data.data(),
                                                scratch->nested_filter_data.size(), false));

    scratch->null_runs.clear();
    scratch->null_runs.push_back(0);
    scratch->ancestor_null_indices.clear();
    bool previous_is_null = false;
    size_t level = 0;
    while (level < scratch->definition_levels.size()) {
        const NestedLevel definition_level = scratch->definition_levels[level];
        const size_t run_start = level++;
        while (level < scratch->definition_levels.size() &&
               scratch->definition_levels[level] == definition_level) {
            ++level;
        }
        const size_t run_length = level - run_start;
        if (definition_level < 2) {
            for (size_t index = run_start; index < level; ++index) {
                scratch->ancestor_null_indices.insert(index);
            }
            continue;
        }
        append_nested_null_run(&scratch->null_runs, definition_level < 3, run_length,
                               &previous_is_null);
    }
    scratch->ancestor_null_count = scratch->ancestor_null_indices.size();
    RETURN_IF_ERROR(scratch->selection.init(
            scratch->null_runs, scratch->repetition_levels.size() - scratch->ancestor_null_count,
            &scratch->selected_nulls, &scratch->nested_filter, 0, &scratch->ancestor_null_indices));

    size_t output_level = 0;
    for (size_t input_level = 0; input_level < scratch->repetition_levels.size(); ++input_level) {
        if (scratch->nested_filter_data[input_level] != 0) {
            scratch->repetition_levels[output_level] = scratch->repetition_levels[input_level];
            scratch->definition_levels[output_level] = scratch->definition_levels[input_level];
            ++output_level;
        }
    }
    scratch->repetition_levels.resize(output_level);
    scratch->definition_levels.resize(output_level);
    return Status::OK();
}

inline Status run_nested_selection_once(NestedSelectionScratch* scratch,
                                        format::parquet::native::FilterMap* parent_filter,
                                        NestedSelectionImplementation implementation) {
    if (implementation == NestedSelectionImplementation::LEGACY) {
        return run_legacy_nested_selection(scratch, parent_filter);
    }
    return scratch->selection.init_nested(
            &scratch->repetition_levels, &scratch->definition_levels, 0,
            /*repeated_parent_def_level=*/2, /*definition_level=*/3, &scratch->selected_nulls,
            parent_filter, 0, &scratch->ancestor_null_count);
}

inline Status validate_nested_selection(NestedSelectionScratch& scratch,
                                        const NestedSelectionOracle& oracle) {
    if (scratch.repetition_levels != oracle.repetition_levels ||
        scratch.definition_levels != oracle.definition_levels ||
        scratch.selected_nulls != oracle.selected_nulls ||
        scratch.ancestor_null_count != oracle.ancestor_null_count ||
        scratch.selection.num_filtered() != oracle.filtered_count) {
        return Status::InternalError("nested selection differs from independent oracle");
    }
    std::vector<NestedReadType> actual_reads;
    NestedReadType type;
    size_t run_length = 0;
    while ((run_length = scratch.selection.get_next_run<true>(&type)) != 0) {
        actual_reads.insert(actual_reads.end(), run_length, type);
    }
    if (actual_reads != oracle.reads) {
        return Status::InternalError("nested selection read sequence differs from oracle");
    }
    return Status::OK();
}

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

    const auto oracle = build_nested_selection_oracle(source_repetition_levels,
                                                      source_definition_levels, parent_filter_data);
    NestedSelectionScratch scratch;
    scratch.repetition_levels = source_repetition_levels;
    scratch.definition_levels = source_definition_levels;
    status = run_nested_selection_once(&scratch, &parent_filter, scenario.nested_implementation);
    if (status.ok()) {
        status = validate_nested_selection(scratch, oracle);
    }
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }

    for (auto _ : state) {
        state.PauseTiming();
        scratch.repetition_levels = source_repetition_levels;
        scratch.definition_levels = source_definition_levels;
        scratch.selected_nulls.clear();
        state.ResumeTiming();
        status =
                run_nested_selection_once(&scratch, &parent_filter, scenario.nested_implementation);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            return;
        }
        auto* compacted_levels = scratch.repetition_levels.data();
        size_t filtered_values = scratch.selection.num_filtered();
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
        std::string name = "ParquetKernel/" + to_string(scenario.kernel) + "/" +
                           to_string(scenario.value_type) + "/sel_" +
                           std::to_string(scenario.selectivity_percent) + "/null_" +
                           std::to_string(scenario.null_percent) + "/" +
                           to_string(scenario.pattern) + "/dict_" +
                           std::to_string(scenario.dictionary_entries);
        if (scenario.kernel == Kernel::NESTED_SELECTION) {
            name += "/impl_" + to_string(scenario.nested_implementation);
        }
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

inline bool register_nullable_selection_benchmarks() {
    for (const auto& scenario : nullable_selection_scenarios()) {
        const std::string name = "ParquetKernel/nullable_selection/sel_" +
                                 std::to_string(scenario.selectivity_percent) + "/null_" +
                                 std::to_string(scenario.null_percent) + "/selection_" +
                                 to_string(scenario.selection_pattern) + "/nulls_" +
                                 to_string(scenario.null_pattern) + "/impl_" +
                                 to_string(scenario.implementation);
        benchmark::RegisterBenchmark(name.c_str(), [=](benchmark::State& state) {
            run_nullable_selection_kernel(state, scenario);
        })->Unit(benchmark::kNanosecond);
    }
    return true;
}

inline const bool KERNEL_BENCHMARKS_REGISTERED = register_kernel_benchmarks();
inline const bool NULLABLE_SELECTION_BENCHMARKS_REGISTERED =
        register_nullable_selection_benchmarks();

} // namespace detail
} // namespace doris::parquet_benchmark
