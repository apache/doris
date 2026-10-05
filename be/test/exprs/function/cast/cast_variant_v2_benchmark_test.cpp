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

#include <chrono>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>

#include "common/exception.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2.h"
#include "exprs/function_context.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"

namespace doris::CastWrapper {
namespace {

constexpr size_t BENCHMARK_ROWS = 1U << 15;
constexpr size_t ARRAY_BENCHMARK_ROWS = 1U << 13;
constexpr size_t BENCHMARK_ITERATIONS = 5;

ColumnVariantV2::MutablePtr finish(VariantBatchBuilder* builder) {
    VariantBatchBuilder batch = builder->finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(batch);
    return result;
}

ColumnPtr cast_from_variant(const ColumnPtr& source, const DataTypePtr& target_type) {
    const auto variant_type = std::make_shared<DataTypeVariant>();
    Block block {{source, variant_type, "source"},
                 {target_type->create_column(), target_type, "result"}};
    RuntimeState state;
    state.set_timezone("UTC");
    auto context = FunctionContext::create_context(&state, {}, {});
    Status status = create_cast_from_variant_v2_wrapper(target_type)(context.get(), block, {0}, 1,
                                                                     source->size(), nullptr);
    if (!status.ok()) {
        throw Exception(status);
    }
    return block.get_by_position(1).column;
}

ColumnPtr cast_to_variant(const ColumnPtr& source, const DataTypePtr& source_type) {
    const auto variant_type = std::make_shared<DataTypeVariant>();
    Block block {{source, source_type, "source"},
                 {variant_type->create_column(), variant_type, "result"}};
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    Status status = create_cast_to_variant_v2_wrapper(source_type)(context.get(), block, {0}, 1,
                                                                   source->size(), nullptr);
    if (!status.ok()) {
        throw Exception(status);
    }
    return block.get_by_position(1).column;
}

template <typename Callback>
int64_t measure_ns(Callback&& callback) {
    const auto start = std::chrono::steady_clock::now();
    for (size_t iteration = 0; iteration < BENCHMARK_ITERATIONS; ++iteration) {
        callback();
    }
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() -
                                                                start)
            .count();
}

void report(std::string_view name, int64_t duration_ns, size_t rows) {
    const double ns_per_row = static_cast<double>(duration_ns) / (rows * BENCHMARK_ITERATIONS);
    std::cout << name << ": " << ns_per_row << " ns/row" << std::endl;
}

ColumnVariantV2::MutablePtr homogeneous_scalars() {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {
            .rows = BENCHMARK_ROWS, .scalar_bytes = BENCHMARK_ROWS * 5});
    for (size_t row_index = 0; row_index < BENCHMARK_ROWS; ++row_index) {
        auto row = builder.begin_row();
        row.add_int(static_cast<int64_t>(row_index));
        row.finish();
    }
    return finish(&builder);
}

ColumnVariantV2::MutablePtr mixed_scalars() {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = BENCHMARK_ROWS});
    for (size_t row_index = 0; row_index < BENCHMARK_ROWS; ++row_index) {
        auto row = builder.begin_row();
        switch (row_index % 4) {
        case 0:
            row.add_int(static_cast<int64_t>(row_index));
            break;
        case 1:
            row.add_string(StringRef("42"));
            break;
        case 2:
            row.add_double(static_cast<double>(row_index));
            break;
        default:
            row.add_decimal(static_cast<__int128>(row_index), 2);
            break;
        }
        row.finish();
    }
    return finish(&builder);
}

ColumnVariantV2::MutablePtr typed_scalars() {
    auto values = ColumnInt32::create();
    values->get_data().resize(BENCHMARK_ROWS);
    for (size_t row = 0; row < BENCHMARK_ROWS; ++row) {
        values->get_data()[row] = static_cast<int32_t>(row);
    }
    auto nulls = ColumnUInt8::create(BENCHMARK_ROWS, 0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(nulls)),
            std::make_shared<DataTypeInt32>());
}

std::pair<ColumnPtr, DataTypePtr> nested_arrays() {
    auto values = ColumnInt32::create();
    values->get_data().resize(ARRAY_BENCHMARK_ROWS * 4);
    for (size_t row = 0; row < values->size(); ++row) {
        values->get_data()[row] = static_cast<int32_t>(row);
    }
    auto value_nulls = ColumnUInt8::create(values->size(), 0);
    auto nullable_values = ColumnNullable::create(std::move(values), std::move(value_nulls));

    auto inner_offsets = ColumnArray::ColumnOffsets::create();
    inner_offsets->get_data().resize(ARRAY_BENCHMARK_ROWS * 2);
    for (size_t row = 0; row < inner_offsets->size(); ++row) {
        inner_offsets->get_data()[row] = (row + 1) * 2;
    }
    auto inner = ColumnArray::create(std::move(nullable_values), std::move(inner_offsets));
    auto inner_nulls = ColumnUInt8::create(inner->size(), 0);
    auto nullable_inner = ColumnNullable::create(std::move(inner), std::move(inner_nulls));

    auto outer_offsets = ColumnArray::ColumnOffsets::create();
    outer_offsets->get_data().resize(ARRAY_BENCHMARK_ROWS);
    for (size_t row = 0; row < ARRAY_BENCHMARK_ROWS; ++row) {
        outer_offsets->get_data()[row] = (row + 1) * 2;
    }
    ColumnPtr outer = ColumnArray::create(std::move(nullable_inner), std::move(outer_offsets));
    DataTypePtr type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    return {std::move(outer), std::move(type)};
}

} // namespace

TEST(CastVariantV2Benchmark, DISABLED_BenchmarkHomogeneousScalar) {
    const ColumnPtr source = homogeneous_scalars();
    const DataTypePtr target = std::make_shared<DataTypeInt64>();
    ColumnPtr output = cast_from_variant(source, target);
    ASSERT_EQ(output->size(), BENCHMARK_ROWS);
    const int64_t duration = measure_ns([&] { output = cast_from_variant(source, target); });
    ASSERT_EQ(output->size(), BENCHMARK_ROWS);
    report("homogeneous encoded Variant -> BIGINT", duration, BENCHMARK_ROWS);
}

TEST(CastVariantV2Benchmark, DISABLED_BenchmarkMixedScalar) {
    const ColumnPtr source = mixed_scalars();
    const DataTypePtr target = std::make_shared<DataTypeInt64>();
    ColumnPtr output = cast_from_variant(source, target);
    ASSERT_EQ(output->size(), BENCHMARK_ROWS);
    const int64_t duration = measure_ns([&] { output = cast_from_variant(source, target); });
    ASSERT_EQ(output->size(), BENCHMARK_ROWS);
    report("mixed encoded Variant -> BIGINT", duration, BENCHMARK_ROWS);
}

TEST(CastVariantV2Benchmark, DISABLED_BenchmarkTypedStringAndJsonb) {
    const ColumnPtr source = typed_scalars();
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr jsonb_type = std::make_shared<DataTypeJsonb>();
    ColumnPtr strings = cast_from_variant(source, string_type);
    ASSERT_EQ(strings->size(), BENCHMARK_ROWS);
    const int64_t string_duration =
            measure_ns([&] { strings = cast_from_variant(source, string_type); });
    ASSERT_EQ(strings->size(), BENCHMARK_ROWS);
    report("typed Variant -> STRING", string_duration, BENCHMARK_ROWS);

    ColumnPtr jsonb = cast_from_variant(source, jsonb_type);
    ASSERT_EQ(jsonb->size(), BENCHMARK_ROWS);
    const int64_t jsonb_duration =
            measure_ns([&] { jsonb = cast_from_variant(source, jsonb_type); });
    ASSERT_EQ(jsonb->size(), BENCHMARK_ROWS);
    report("typed Variant -> JSONB", jsonb_duration, BENCHMARK_ROWS);
}

TEST(CastVariantV2Benchmark, DISABLED_BenchmarkNestedArrayRoundTrip) {
    auto [source, type] = nested_arrays();
    ColumnPtr round_trip = cast_from_variant(cast_to_variant(source, type), type);
    ASSERT_EQ(round_trip->size(), ARRAY_BENCHMARK_ROWS);
    const int64_t duration = measure_ns([&] {
        ColumnPtr variant = cast_to_variant(source, type);
        round_trip = cast_from_variant(variant, type);
    });
    ASSERT_EQ(round_trip->size(), ARRAY_BENCHMARK_ROWS);
    report("ARRAY<ARRAY<INT>> Variant round-trip", duration, ARRAY_BENCHMARK_ROWS);
}

} // namespace doris::CastWrapper
