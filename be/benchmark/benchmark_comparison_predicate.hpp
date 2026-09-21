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
#include <memory>
#include <string>

#include "common/cast_set.h"
#include "common/check.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/field.h"
#include "storage/predicate/comparison_predicate.h"
#include "util/simd/bits.h"

namespace doris::comparison_predicate_benchmark {

constexpr int64_t MIN_LOGICAL_ROWS = 110'000'000;

enum class Nullability {
    NON_NULLABLE,
    NULLABLE_NO_NULL,
    NULLABLE_4_5_PERCENT,
    NULLABLE_20_PERCENT,
};

enum class Operation {
    GE_1_ASSIGN,
    LE_100_AND_ALL_SELECTED,
    LE_100_AND_AFTER_GE_1,
    GE_1_THEN_LE_100,
    GE_21_THEN_LE_40,
    LE_40_AND_AFTER_GE_21,
};

enum class DiagnosticLoop {
    RAW_LOGICAL_AND,
    RAW_BITWISE_AND,
    VIEW_LOGICAL_AND,
    VIEW_BITWISE_AND,
    VIEW_HOISTED_LOGICAL_AND,
    VIEW_HOISTED_BITWISE_AND,
};

struct Input {
    ColumnPtr column;
    const int32_t* values = nullptr;
    const uint8_t* null_map = nullptr;
    size_t size = 0;
};

static bool is_null_at(Nullability nullability, size_t index) {
    switch (nullability) {
    case Nullability::NON_NULLABLE:
    case Nullability::NULLABLE_NO_NULL:
        return false;
    case Nullability::NULLABLE_4_5_PERCENT:
        return index % 200 < 9;
    case Nullability::NULLABLE_20_PERCENT:
        return index % 5 == 0;
    }
    __builtin_unreachable();
}

static Input make_input(size_t size, Nullability nullability) {
    auto values = ColumnInt32::create();
    values->reserve(size);
    for (size_t i = 0; i < size; ++i) {
        values->insert_value(static_cast<int32_t>(i % 100 + 1));
    }

    if (nullability == Nullability::NON_NULLABLE) {
        const auto* values_ptr = values->get_data().data();
        return {std::move(values), values_ptr, nullptr, size};
    }

    auto null_map = ColumnUInt8::create();
    null_map->reserve(size);
    for (size_t i = 0; i < size; ++i) {
        const bool is_null = is_null_at(nullability, i);
        null_map->insert_value(is_null);
    }

    const auto* values_ptr = values->get_data().data();
    const auto* null_map_ptr = null_map->get_data().data();
    auto column = ColumnNullable::create(std::move(values), std::move(null_map));
    return {std::move(column), values_ptr, null_map_ptr, size};
}

using GePredicate = ComparisonPredicateBase<TYPE_INT, PredicateType::GE>;
using LePredicate = ComparisonPredicateBase<TYPE_INT, PredicateType::LE>;

struct DorisPredicates {
    GePredicate ge_1 {0, "", Field::create_field<TYPE_INT>(1)};
    LePredicate le_100 {0, "", Field::create_field<TYPE_INT>(100)};
    GePredicate ge_21 {0, "", Field::create_field<TYPE_INT>(21)};
    LePredicate le_40 {0, "", Field::create_field<TYPE_INT>(40)};
};

static void initialize_doris_selection(Operation operation, bool* selection, size_t size,
                                       const Input& input, const DorisPredicates& predicates) {
    std::fill_n(selection, size, true);
    if (operation == Operation::LE_100_AND_AFTER_GE_1) {
        predicates.ge_1.evaluate_vec(*input.column, cast_set<uint16_t>(size), selection);
    } else if (operation == Operation::LE_40_AND_AFTER_GE_21) {
        predicates.ge_21.evaluate_vec(*input.column, cast_set<uint16_t>(size), selection);
    }
}

static void evaluate_doris(Operation operation, bool* selection, const Input& input,
                           const DorisPredicates& predicates) {
    const auto size = cast_set<uint16_t>(input.size);
    switch (operation) {
    case Operation::GE_1_ASSIGN:
        predicates.ge_1.evaluate_vec(*input.column, size, selection);
        return;
    case Operation::LE_100_AND_ALL_SELECTED:
    case Operation::LE_100_AND_AFTER_GE_1:
        predicates.le_100.evaluate_and_vec(*input.column, size, selection);
        return;
    case Operation::GE_1_THEN_LE_100:
        predicates.ge_1.evaluate_vec(*input.column, size, selection);
        predicates.le_100.evaluate_and_vec(*input.column, size, selection);
        return;
    case Operation::GE_21_THEN_LE_40:
        predicates.ge_21.evaluate_vec(*input.column, size, selection);
        predicates.le_40.evaluate_and_vec(*input.column, size, selection);
        return;
    case Operation::LE_40_AND_AFTER_GE_21:
        predicates.le_40.evaluate_and_vec(*input.column, size, selection);
        return;
    }
    __builtin_unreachable();
}

static size_t compact_doris_selection(const bool* selection, size_t size,
                                      uint16_t* selected_row_ids) {
    size_t selected_size = 0;
    size_t position = 0;
    static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
    const size_t simd_end = size / SIMD_BYTES * SIMD_BYTES;

    while (position < simd_end) {
        const auto mask = simd::bytes_mask_to_bits_mask(
                reinterpret_cast<const uint8_t*>(selection) + position);
        if (mask == simd::bits_mask_all()) {
            for (size_t i = 0; i < SIMD_BYTES; ++i) {
                selected_row_ids[selected_size++] = cast_set<uint16_t>(position + i);
            }
        } else if (mask != 0) {
            simd::iterate_through_bits_mask(
                    [&](size_t bit_position) {
                        selected_row_ids[selected_size++] =
                                cast_set<uint16_t>(position + bit_position);
                    },
                    mask);
        }
        position += SIMD_BYTES;
    }
    for (; position < size; ++position) {
        if (selection[position]) {
            selected_row_ids[selected_size++] = cast_set<uint16_t>(position);
        }
    }
    return selected_size;
}

static size_t expected_selected_rows(const Input& input, Operation operation) {
    size_t selected = 0;
    for (size_t i = 0; i < input.size; ++i) {
        const bool not_null = input.null_map == nullptr || !input.null_map[i];
        const bool in_range = operation == Operation::GE_21_THEN_LE_40 ||
                              operation == Operation::LE_40_AND_AFTER_GE_21;
        selected += not_null && (!in_range || (input.values[i] >= 21 && input.values[i] <= 40));
    }
    return selected;
}

template <bool use_bitwise_and>
[[gnu::noinline]] static void evaluate_raw_diagnostic(uint8_t* __restrict selection,
                                                      const int32_t* values,
                                                      const uint8_t* __restrict null_map,
                                                      size_t size, int32_t value) {
    for (size_t i = 0; i < size; ++i) {
        if constexpr (use_bitwise_and) {
            selection[i] &= static_cast<uint8_t>((!null_map[i]) & (values[i] <= value));
        } else {
            selection[i] &= static_cast<uint8_t>((!null_map[i]) && (values[i] <= value));
        }
    }
}

template <bool use_bitwise_and>
[[gnu::noinline]] static void evaluate_view_diagnostic(uint8_t* __restrict selection,
                                                       ColumnElementView<TYPE_INT> values,
                                                       const uint8_t* __restrict null_map,
                                                       size_t size, int32_t value) {
    for (size_t i = 0; i < size; ++i) {
        if constexpr (use_bitwise_and) {
            selection[i] &= static_cast<uint8_t>((!null_map[i]) & (values[i] <= value));
        } else {
            selection[i] &= static_cast<uint8_t>((!null_map[i]) && (values[i] <= value));
        }
    }
}

template <bool use_bitwise_and>
[[gnu::noinline]] static void evaluate_view_hoisted_diagnostic(uint8_t* __restrict selection,
                                                               ColumnElementView<TYPE_INT> values,
                                                               const uint8_t* __restrict null_map,
                                                               size_t size, int32_t value) {
    const auto* raw_values = values.get_data();
    for (size_t i = 0; i < size; ++i) {
        if constexpr (use_bitwise_and) {
            selection[i] &= static_cast<uint8_t>((!null_map[i]) & (raw_values[i] <= value));
        } else {
            selection[i] &= static_cast<uint8_t>((!null_map[i]) && (raw_values[i] <= value));
        }
    }
}

static void evaluate_diagnostic(DiagnosticLoop loop, uint8_t* selection, const Input& input,
                                ColumnElementView<TYPE_INT> values) {
    switch (loop) {
    case DiagnosticLoop::RAW_LOGICAL_AND:
        evaluate_raw_diagnostic<false>(selection, input.values, input.null_map, input.size, 100);
        return;
    case DiagnosticLoop::RAW_BITWISE_AND:
        evaluate_raw_diagnostic<true>(selection, input.values, input.null_map, input.size, 100);
        return;
    case DiagnosticLoop::VIEW_LOGICAL_AND:
        evaluate_view_diagnostic<false>(selection, values, input.null_map, input.size, 100);
        return;
    case DiagnosticLoop::VIEW_BITWISE_AND:
        evaluate_view_diagnostic<true>(selection, values, input.null_map, input.size, 100);
        return;
    case DiagnosticLoop::VIEW_HOISTED_LOGICAL_AND:
        evaluate_view_hoisted_diagnostic<false>(selection, values, input.null_map, input.size, 100);
        return;
    case DiagnosticLoop::VIEW_HOISTED_BITWISE_AND:
        evaluate_view_hoisted_diagnostic<true>(selection, values, input.null_map, input.size, 100);
        return;
    }
    __builtin_unreachable();
}

template <typename T>
static size_t count_selected(const T* selection, size_t size) {
    size_t selected = 0;
    for (size_t i = 0; i < size; ++i) {
        selected += selection[i];
    }
    return selected;
}

static void run_doris(benchmark::State& state, size_t size, Nullability nullability,
                      Operation operation) {
    const auto input = make_input(size, nullability);
    const DorisPredicates predicates;
    auto selection = std::make_unique<bool[]>(size);
    initialize_doris_selection(operation, selection.get(), size, input, predicates);
    evaluate_doris(operation, selection.get(), input, predicates);
    DORIS_CHECK_EQ(count_selected(selection.get(), size), expected_selected_rows(input, operation));

    const int64_t batches_per_iteration = (MIN_LOGICAL_ROWS + size - 1) / size;
    for (auto _ : state) {
        for (int64_t batch = 0; batch < batches_per_iteration; ++batch) {
            evaluate_doris(operation, selection.get(), input, predicates);
            benchmark::ClobberMemory();
        }
    }
    auto selected = count_selected(selection.get(), size);
    benchmark::DoNotOptimize(selected);
    DORIS_CHECK_EQ(selected, expected_selected_rows(input, operation));
    state.SetItemsProcessed(state.iterations() * batches_per_iteration *
                            static_cast<int64_t>(size));
}

static void run_doris_profile_scope(benchmark::State& state, size_t size, Nullability nullability,
                                    Operation operation) {
    const auto input = make_input(size, nullability);
    const DorisPredicates predicates;
    auto selection = std::make_unique<bool[]>(size);
    auto selected_row_ids = std::make_unique<uint16_t[]>(size);
    initialize_doris_selection(operation, selection.get(), size, input, predicates);

    const int64_t batches_per_iteration = (MIN_LOGICAL_ROWS + size - 1) / size;
    size_t checksum = 0;
    for (auto _ : state) {
        for (int64_t batch = 0; batch < batches_per_iteration; ++batch) {
            evaluate_doris(operation, selection.get(), input, predicates);
            checksum += compact_doris_selection(selection.get(), size, selected_row_ids.get());
            benchmark::ClobberMemory();
        }
    }
    benchmark::DoNotOptimize(checksum);
    state.SetItemsProcessed(state.iterations() * batches_per_iteration *
                            static_cast<int64_t>(size));
}

static void run_diagnostic_loop(benchmark::State& state, size_t size, DiagnosticLoop loop) {
    const auto input = make_input(size, Nullability::NULLABLE_4_5_PERCENT);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*input.column);
    ColumnElementView<TYPE_INT> values {nullable_column.get_nested_column()};
    auto selection = std::make_unique<uint8_t[]>(size);
    for (size_t i = 0; i < size; ++i) {
        selection[i] = static_cast<uint8_t>((!input.null_map[i]) & (input.values[i] >= 1));
    }
    evaluate_diagnostic(loop, selection.get(), input, values);
    DORIS_CHECK_EQ(count_selected(selection.get(), size),
                   expected_selected_rows(input, Operation::LE_100_AND_AFTER_GE_1));

    const int64_t batches_per_iteration = (MIN_LOGICAL_ROWS + size - 1) / size;
    for (auto _ : state) {
        for (int64_t batch = 0; batch < batches_per_iteration; ++batch) {
            evaluate_diagnostic(loop, selection.get(), input, values);
            benchmark::ClobberMemory();
        }
    }
    auto selected = count_selected(selection.get(), size);
    benchmark::DoNotOptimize(selected);
    DORIS_CHECK_EQ(selected, expected_selected_rows(input, Operation::LE_100_AND_AFTER_GE_1));
    state.SetItemsProcessed(state.iterations() * batches_per_iteration *
                            static_cast<int64_t>(size));
}

static const char* diagnostic_loop_name(DiagnosticLoop loop) {
    switch (loop) {
    case DiagnosticLoop::RAW_LOGICAL_AND:
        return "RawLogicalAnd";
    case DiagnosticLoop::RAW_BITWISE_AND:
        return "RawBitwiseAnd";
    case DiagnosticLoop::VIEW_LOGICAL_AND:
        return "ViewLogicalAnd";
    case DiagnosticLoop::VIEW_BITWISE_AND:
        return "ViewBitwiseAnd";
    case DiagnosticLoop::VIEW_HOISTED_LOGICAL_AND:
        return "ViewHoistedLogicalAnd";
    case DiagnosticLoop::VIEW_HOISTED_BITWISE_AND:
        return "ViewHoistedBitwiseAnd";
    }
    __builtin_unreachable();
}

static const char* nullability_name(Nullability nullability) {
    switch (nullability) {
    case Nullability::NON_NULLABLE:
        return "NonNullable";
    case Nullability::NULLABLE_NO_NULL:
        return "NullableNoNull";
    case Nullability::NULLABLE_4_5_PERCENT:
        return "Nullable4_5Percent";
    case Nullability::NULLABLE_20_PERCENT:
        return "Nullable20Percent";
    }
    __builtin_unreachable();
}

static const char* operation_name(Operation operation) {
    switch (operation) {
    case Operation::GE_1_ASSIGN:
        return "GE1Assign";
    case Operation::LE_100_AND_ALL_SELECTED:
        return "LE100AndAllSelected";
    case Operation::LE_100_AND_AFTER_GE_1:
        return "LE100AndAfterGE1";
    case Operation::GE_1_THEN_LE_100:
        return "GE1ThenLE100";
    case Operation::GE_21_THEN_LE_40:
        return "GE21ThenLE40";
    case Operation::LE_40_AND_AFTER_GE_21:
        return "LE40AndAfterGE21";
    }
    __builtin_unreachable();
}

static void register_benchmarks() {
    constexpr size_t sizes[] = {4096, 8192};
    constexpr Nullability nullabilities[] = {
            Nullability::NON_NULLABLE, Nullability::NULLABLE_NO_NULL,
            Nullability::NULLABLE_4_5_PERCENT, Nullability::NULLABLE_20_PERCENT};
    constexpr Operation operations[] = {
            Operation::GE_1_ASSIGN,           Operation::LE_100_AND_ALL_SELECTED,
            Operation::LE_100_AND_AFTER_GE_1, Operation::GE_1_THEN_LE_100,
            Operation::GE_21_THEN_LE_40,      Operation::LE_40_AND_AFTER_GE_21};

    for (const auto size : sizes) {
        for (const auto nullability : nullabilities) {
            for (const auto operation : operations) {
                const auto suffix = std::string(nullability_name(nullability)) + "/" +
                                    operation_name(operation) + "/" + std::to_string(size);
                benchmark::RegisterBenchmark(("Predicate/DorisProduction/" + suffix).c_str(),
                                             run_doris, size, nullability, operation)
                        ->Unit(benchmark::kNanosecond);
                benchmark::RegisterBenchmark(("Predicate/DorisProfileScope/" + suffix).c_str(),
                                             run_doris_profile_scope, size, nullability, operation)
                        ->Unit(benchmark::kNanosecond);
            }
        }
    }

    constexpr DiagnosticLoop diagnostic_loops[] = {
            DiagnosticLoop::RAW_LOGICAL_AND,          DiagnosticLoop::RAW_BITWISE_AND,
            DiagnosticLoop::VIEW_LOGICAL_AND,         DiagnosticLoop::VIEW_BITWISE_AND,
            DiagnosticLoop::VIEW_HOISTED_LOGICAL_AND, DiagnosticLoop::VIEW_HOISTED_BITWISE_AND};
    for (const auto size : sizes) {
        for (const auto loop : diagnostic_loops) {
            const auto name = std::string("PredicateCodegen/") + diagnostic_loop_name(loop) + "/" +
                              std::to_string(size);
            benchmark::RegisterBenchmark(name.c_str(), run_diagnostic_loop, size, loop)
                    ->Unit(benchmark::kNanosecond);
        }
    }
}

[[maybe_unused]] const bool BENCHMARKS_REGISTERED = [] {
    register_benchmarks();
    return true;
}();

} // namespace doris::comparison_predicate_benchmark
