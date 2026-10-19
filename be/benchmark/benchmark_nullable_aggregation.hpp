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
                                          bool decimal = false) {
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
    return AggregateFunctionSimpleFactory::instance().get(
            name, {make_nullable(type)}, result_type, true,
            BeExecVersionManager::get_newest_version(), attr);
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

inline void streaming(benchmark::State& state, const char* name, bool decimal = false) {
    set_allocation_benchmark(state.name() + "/" + std::to_string(state.range(0)) + "/" +
                             std::to_string(state.range(1)) + "/" + std::to_string(state.range(2)));
    const auto rows = state.range(0);
    auto function = make_function(name, state.range(2), decimal);
    auto input = make_input(rows, state.range(1), decimal);
    const IColumn* columns[] = {input.get()};
    Arena arena;
    for (auto _ : state) {
        [[maybe_unused]] AllocationScope allocation_scope;
        {
            auto output = function->create_serialize_column();
            function->streaming_agg_serialize_to_column(columns, output, rows, arena);
            auto* output_column = output.get();
            benchmark::DoNotOptimize(output_column);
            benchmark::ClobberMemory();
        }
        arena.clear();
    }
    state.SetItemsProcessed(state.iterations() * rows);
}

inline void merge(benchmark::State& state, const char* name, bool decimal = false) {
    set_allocation_benchmark(state.name() + "/" + std::to_string(state.range(0)) + "/" +
                             std::to_string(state.range(1)) + "/" + std::to_string(state.range(2)) +
                             "/" + std::to_string(state.range(3)));
    const auto rows = state.range(0);
    const auto groups = state.range(3);
    auto function = make_function(name, state.range(2), decimal);
    auto input = make_input(rows, state.range(1), decimal);
    const IColumn* columns[] = {input.get()};
    Arena arena;
    auto serialized = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns, serialized, rows, arena);
    Arena states_arena;
    std::vector<AggregateDataPtr> group_places(groups);
    for (auto& place : group_places) {
        place = states_arena.aligned_alloc(function->size_of_data(), function->align_of_data());
        function->create(place);
    }
    DEFER({
        for (auto place : group_places) {
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
#else
    const bool allocate_scratch = function->needs_deserialize_and_merge_scratch();
#endif
    for (auto _ : state) {
        [[maybe_unused]] AllocationScope allocation_scope;
        for (auto place : group_places) {
            function->reset(place);
        }
        Arena scratch_arena;
        AggregateDataPtr scratch = nullptr;
        if (allocate_scratch) {
            scratch = scratch_arena.aligned_alloc(rows * function->size_of_data(),
                                                  function->align_of_data());
        }
        function->deserialize_and_merge_vec(places.data(), 0, scratch, serialized.get(), arena,
                                            rows);
        benchmark::ClobberMemory();
    }
    // This is the reader's declared scratch requirement, not a malloc allocation measurement.
    state.counters["reader_scratch_bytes"] =
            function->needs_deserialize_and_merge_scratch() ? rows * function->size_of_data() : 0;
    state.counters["allocated_scratch_bytes"] =
            allocate_scratch ? rows * function->size_of_data() : 0;
    state.SetItemsProcessed(state.iterations() * rows);
}

inline void arguments(benchmark::internal::Benchmark* benchmark, bool with_groups) {
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
