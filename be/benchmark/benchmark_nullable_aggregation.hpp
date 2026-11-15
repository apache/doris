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

#include <charconv>
#include <cstdlib>
#include <string_view>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "nullable_allocation_counter.h"

namespace doris::nullable_aggregation_benchmark {

inline AggregateFunctionPtr make_function(const char* name, int64_t reference_v2,
                                          bool decimal = false, int64_t nullable_mask = 0) {
    AggregateFunctionAttr attr;
#ifdef DORIS_NULLABLE_AGGREGATION_REFERENCE
    // Compile this same harness against the unmodified reference checkout to obtain V1/V2 A/B.
    attr.enable_aggregate_function_null_v2 = reference_v2;
#else
    (void)reference_v2;
#endif
    DataTypePtr type = decimal ? DataTypePtr(std::make_shared<DataTypeDecimal128>(27, 9))
                               : DataTypePtr(std::make_shared<DataTypeInt64>());
    DataTypePtr result_type = decimal ? std::make_shared<DataTypeDecimal128>(38, 9) : nullptr;
    DataTypes arguments {make_nullable(type)};
    if (nullable_mask != 0) {
        // Bit 0 marks the value nullable. Bit 1 marks the weight nullable.
        // The factory registers avg_weighted for two Float64 inputs, as supplied by FE casts.
        arguments = {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()};
        for (size_t i = 0; i < arguments.size(); ++i) {
            if (nullable_mask & (1 << i)) {
                arguments[i] = make_nullable(arguments[i]);
            }
        }
    }
    auto function = AggregateFunctionSimpleFactory::instance().get(
            name, arguments, result_type, true, BeExecVersionManager::get_newest_version(), attr);
    DORIS_CHECK(function != nullptr);
    if (nullable_mask != 0) {
        // The old factory uses Variadic V1 even when its V2 switch is true.
        // Check the wrapper and column format so that a label cannot hide that fallback.
#ifdef DORIS_NULLABLE_AGGREGATION_REFERENCE
        DORIS_CHECK_EQ(reference_v2, 0);
        DORIS_CHECK_EQ(function->get_name(), "Nullable(avg_weighted)");
        DORIS_CHECK(!function->get_serialized_type()->is_nullable());
#else
        DORIS_CHECK_EQ(reference_v2, 1);
        DORIS_CHECK_EQ(function->get_name(), "NullableV2(avg_weighted)");
        DORIS_CHECK(function->get_serialized_type()->is_nullable());
#endif
    }
    return function;
}

inline MutableColumnPtr make_input(size_t rows, int64_t null_percent, bool decimal = false) {
    DataTypePtr type = decimal ? DataTypePtr(std::make_shared<DataTypeDecimal128>(27, 9))
                               : DataTypePtr(std::make_shared<DataTypeInt64>());
    auto values = type->create_column();
    auto null_map = ColumnUInt8::create();
    for (size_t row = 0; row < rows; ++row) {
        if (decimal) {
            assert_cast<ColumnDecimal128V3&>(*values).insert_value(
                    Decimal128V3(static_cast<Int128>(row % 1000) * 1'000'000'000));
        } else {
            assert_cast<ColumnInt64&>(*values).insert_value(static_cast<Int64>(row % 1000));
        }
        null_map->insert_value((row * 17 % 100) < null_percent);
    }
    return ColumnNullable::create(std::move(values), std::move(null_map));
}

inline MutableColumns make_inputs(size_t rows, int64_t null_percent, bool decimal,
                                  int64_t nullable_mask) {
    MutableColumns inputs;
    if (nullable_mask == 0) {
        inputs.push_back(make_input(rows, null_percent, decimal));
        return inputs;
    }
    auto values = ColumnFloat64::create();
    auto weights = ColumnFloat64::create();
    for (size_t row = 0; row < rows; ++row) {
        values->insert_value(static_cast<double>(row % 7 + 1));
        weights->insert_value(static_cast<double>(row % 3 + 1));
    }
    inputs.emplace_back(std::move(values));
    inputs.emplace_back(std::move(weights));
    for (size_t i = 0; i < inputs.size(); ++i) {
        if (nullable_mask & (1 << i)) {
            auto null_map = ColumnUInt8::create();
            for (size_t row = 0; row < rows; ++row) {
                // Offset the second map. At 50%, the batch includes all four NULL pairs:
                // (value, weight), (NULL, weight), (value, NULL), and (NULL, NULL).
                null_map->insert_value((row * 17 + i * 37) % 100 < null_percent);
            }
            inputs[i] = ColumnNullable::create(std::move(inputs[i]), std::move(null_map));
        }
    }
    return inputs;
}

inline void check_weighted_results(const AggregateFunctionPtr& function,
                                   const std::vector<AggregateDataPtr>& places,
                                   const MutableColumns& inputs, size_t rows) {
    // Compute the expected result without an aggregate function. Small positive integers
    // keep both sums exact in Float64. A NULL in either argument must exclude that row.
    std::vector<double> sums(places.size(), 0);
    std::vector<double> weights(places.size(), 0);
    for (size_t row = 0; row < rows; ++row) {
        if (!inputs[0]->is_null_at(row) && !inputs[1]->is_null_at(row)) {
            const auto weight = static_cast<double>(row % 3 + 1);
            sums[row % places.size()] += static_cast<double>(row % 7 + 1) * weight;
            weights[row % places.size()] += weight;
        }
    }
    auto result = function->get_return_type()->create_column();
    for (auto* place : places) {
        function->insert_result_into(place, *result);
    }
    const auto& nullable = assert_cast<const ColumnNullable&>(*result);
    const auto& values = assert_cast<const ColumnFloat64&>(nullable.get_nested_column());
    for (size_t group = 0; group < places.size(); ++group) {
        DORIS_CHECK_EQ(nullable.is_null_at(group), weights[group] == 0);
        if (weights[group] != 0) {
            DORIS_CHECK_EQ(values.get_element(group), sums[group] / weights[group]);
        }
    }
}

inline void name_allocation_sample(const benchmark::State& state, int argument_count) {
    auto allocation_name = state.name();
    for (int argument = 0; argument < argument_count; ++argument) {
        allocation_name += "/" + std::to_string(state.range(argument));
    }
    set_allocation_benchmark(allocation_name);
}

inline void streaming(benchmark::State& state, const char* name, bool decimal = false,
                      bool variadic = false) {
    name_allocation_sample(state, variadic ? 5 : 3);
    const auto rows = static_cast<size_t>(state.range(0));
    const auto nullable_mask = variadic ? state.range(3) : 0;
    const auto batches = variadic ? state.range(4) : 1;
    auto function = make_function(name, state.range(2), decimal, nullable_mask);
    auto inputs = make_inputs(rows, state.range(1), decimal, nullable_mask);
    std::vector<const IColumn*> columns;
    for (const auto& input : inputs) {
        columns.push_back(input.get());
    }
    function->check_input_columns_type(columns.data());
    Arena arena;
    if (variadic) {
        // This check runs before timing. Decode the streaming output and check its result.
        auto serialized = function->create_serialize_column();
        function->streaming_agg_serialize_to_column(columns.data(), serialized, rows, arena);
        DORIS_CHECK_EQ(serialized->size(), rows);
        AggregateFunctionGuard result(function.get());
        function->deserialize_and_merge_from_column(result.data(), *serialized, arena);
        check_weighted_results(function, {result.data()}, inputs, rows);
        state.SetLabel(function->get_name() + ";" + serialized->get_name());
    }
    for ([[maybe_unused]] auto _ : state) {
        [[maybe_unused]] AllocationScope allocation_scope;
        for (int64_t batch = 0; batch < batches; ++batch) {
            auto output = function->create_serialize_column();
            function->streaming_agg_serialize_to_column(columns.data(), output, rows, arena);
            auto* output_column = output.get();
            benchmark::DoNotOptimize(output_column);
            benchmark::ClobberMemory();
        }
        arena.clear();
    }
    state.SetItemsProcessed(state.iterations() * rows * batches);
}

inline void merge(benchmark::State& state, const char* name, bool decimal = false,
                  bool variadic = false) {
    name_allocation_sample(state, variadic ? 6 : 4);
    const auto rows = static_cast<size_t>(state.range(0));
    const auto groups = static_cast<size_t>(state.range(3));
    const auto nullable_mask = variadic ? state.range(4) : 0;
    const auto batches = variadic ? state.range(5) : 1;
    auto function = make_function(name, state.range(2), decimal, nullable_mask);
    auto inputs = make_inputs(rows, state.range(1), decimal, nullable_mask);
    std::vector<const IColumn*> columns;
    for (const auto& input : inputs) {
        columns.push_back(input.get());
    }
    function->check_input_columns_type(columns.data());
    Arena arena;
    auto serialized = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns.data(), serialized, rows, arena);
    if (variadic) {
        state.SetLabel(function->get_name() + ";" + serialized->get_name());
    }
    Arena states_arena;
    std::vector<AggregateDataPtr> group_places(groups);
    for (auto& place : group_places) {
        place = states_arena.aligned_alloc(function->size_of_data(), function->align_of_data());
        function->create(place);
    }
    DEFER({
        for (auto* place : group_places) {
            function->destroy(place);
        }
    });
    std::vector<AggregateDataPtr> places(rows);
    for (size_t row = 0; row < rows; ++row) {
        places[row] = group_places[row % groups];
    }
    // Model the actual sink's scratch allocation policy on each revision.
#ifdef DORIS_NULLABLE_AGGREGATION_REFERENCE
    const bool allocate_scratch = true;
    std::vector<char> legacy_scratch;
#else
    const bool allocate_scratch = function->needs_deserialize_and_merge_scratch();
#endif
    Arena scratch_arena;
    for ([[maybe_unused]] auto _ : state) {
        [[maybe_unused]] AllocationScope allocation_scope;
        for (auto* place : group_places) {
            function->reset(place);
        }
        AggregateDataPtr scratch = nullptr;
        if (allocate_scratch) {
#ifdef DORIS_NULLABLE_AGGREGATION_REFERENCE
            // The old sink grows a retained byte vector, rather than allocating every batch.
            const auto bytes = rows * function->size_of_data();
            if (legacy_scratch.size() < bytes) {
                legacy_scratch.resize(bytes);
            }
            scratch = legacy_scratch.data();
#else
            scratch_arena.clear();
            scratch = scratch_arena.aligned_alloc(rows * function->size_of_data(),
                                                  function->align_of_data());
#endif
        }
        for (int64_t batch = 0; batch < batches; ++batch) {
            function->deserialize_and_merge_vec(places.data(), 0, scratch, serialized.get(), arena,
                                                rows);
        }
        benchmark::ClobberMemory();
    }
    if (variadic) {
        // Each batch has the same rows. Repeating them leaves the weighted mean unchanged.
        // Check every group, including groups whose rows are all NULL.
        check_weighted_results(function, group_places, inputs, rows);
    }
    // This is the reader's declared scratch requirement, not a malloc allocation measurement.
    state.counters["reader_scratch_bytes"] =
            function->needs_deserialize_and_merge_scratch() ? rows * function->size_of_data() : 0;
    state.counters["allocated_scratch_bytes"] =
            allocate_scratch ? rows * function->size_of_data() : 0;
    state.SetItemsProcessed(state.iterations() * rows * batches);
}

inline void arguments(benchmark::internal::Benchmark* benchmark, bool with_groups) {
    // Both option values are needed on the reference branch. This branch runs V2 for both.
    // Use the function label and the binary revision to identify the measured implementation.
    for (auto null_percent : {0, 1, 50, 100}) {
        for (auto reference_v2 : {0, 1}) {
            if (with_groups) {
                for (auto groups : {64, 4096}) {
                    benchmark->Args({4096, null_percent, reference_v2, groups});
                }
            } else {
                benchmark->Args({4096, null_percent, reference_v2});
            }
        }
    }
}

inline void variadic_arguments(benchmark::internal::Benchmark* benchmark, bool with_groups) {
    // V1 exists only in the reference checkout. The migrated checkout runs actual V2.
    // Do not register the old V2 switch as a second variadic implementation: it is V1 too.
    int64_t batches = 1000000000 / 4096;
    if (const char* value = std::getenv("DORIS_NULLABLE_AGGREGATION_BATCHES")) {
        // Keep the large standalone workload. A paired run can set 4 to match an older binary.
        // Reject values such as "0" and "4x": both would invalidate per-batch measurements.
        const std::string_view text(value);
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), batches);
        DORIS_CHECK(parsed.ec == std::errc {} && parsed.ptr == text.data() + text.size() &&
                    batches > 0);
    }
#ifdef DORIS_NULLABLE_AGGREGATION_REFERENCE
    constexpr int mode = 0;
#else
    constexpr int mode = 1;
#endif
    for (auto null_percent : {0, 1, 50, 100}) {
        for (auto nullable_mask : {1, 2, 3}) {
            if (with_groups) {
                for (auto groups : {64, 4096}) {
                    benchmark->Args({4096, null_percent, mode, groups, nullable_mask, batches});
                }
            } else {
                benchmark->Args({4096, null_percent, mode, nullable_mask, batches});
            }
        }
    }
}

BENCHMARK_CAPTURE(streaming, nullable_avg_weighted, "avg_weighted", false, true)
        ->Apply([](auto* benchmark) {
            doris::nullable_aggregation_benchmark::variadic_arguments(benchmark, false);
        });
BENCHMARK_CAPTURE(merge, nullable_avg_weighted, "avg_weighted", false, true)
        ->Apply([](auto* benchmark) {
            doris::nullable_aggregation_benchmark::variadic_arguments(benchmark, true);
        });

BENCHMARK_CAPTURE(streaming, nullable_sum, "sum")->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, false);
});
BENCHMARK_CAPTURE(streaming, nullable_avg, "avg")->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, false);
});
BENCHMARK_CAPTURE(merge, nullable_sum, "sum")->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, true);
});
BENCHMARK_CAPTURE(merge, nullable_avg, "avg")->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, true);
});

BENCHMARK_CAPTURE(streaming, nullable_decimal_sum, "sum", true)->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, false);
});
BENCHMARK_CAPTURE(streaming, nullable_decimal_avg, "avg", true)->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, false);
});
BENCHMARK_CAPTURE(merge, nullable_decimal_sum, "sum", true)->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, true);
});
BENCHMARK_CAPTURE(merge, nullable_decimal_avg, "avg", true)->Apply([](auto* benchmark) {
    doris::nullable_aggregation_benchmark::arguments(benchmark, true);
});

} // namespace doris::nullable_aggregation_benchmark
