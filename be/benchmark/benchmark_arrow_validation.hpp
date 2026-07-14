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

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/type.h>
#include <benchmark/benchmark.h>
#include <cctz/time_zone.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "util/unaligned.h"

namespace doris {
namespace {

enum class ArrowValidationBenchMode { OLD, CHECK_DISABLED, CHECK_ENABLED };

void throw_if_not_ok(const arrow::Status& status) {
    if (!status.ok()) {
        throw std::runtime_error(status.ToString());
    }
}

void throw_if_not_ok(const Status& status) {
    if (!status.ok()) {
        throw std::runtime_error(status.to_string());
    }
}

void keep_column_size(size_t size) {
    benchmark::DoNotOptimize(size);
}

template <typename Func>
double measure_seconds(Func&& func) {
    const auto start = std::chrono::steady_clock::now();
    std::forward<Func>(func)();
    const auto finish = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(finish - start).count();
}

std::shared_ptr<arrow::StringArray> make_string_array(int64_t rows) {
    arrow::StringBuilder builder;
    throw_if_not_ok(builder.Reserve(rows));
    for (int64_t i = 0; i < rows; ++i) {
        const std::string value = "arrow_validation_" + std::to_string(i % 1000);
        throw_if_not_ok(builder.Append(value));
    }
    std::shared_ptr<arrow::Array> array;
    throw_if_not_ok(builder.Finish(&array));
    return std::static_pointer_cast<arrow::StringArray>(array);
}

std::shared_ptr<arrow::Int64Array> make_int64_array(int64_t rows) {
    arrow::Int64Builder builder;
    throw_if_not_ok(builder.Reserve(rows));
    for (int64_t i = 0; i < rows; ++i) {
        throw_if_not_ok(builder.Append(i));
    }
    std::shared_ptr<arrow::Array> array;
    throw_if_not_ok(builder.Finish(&array));
    return std::static_pointer_cast<arrow::Int64Array>(array);
}

std::shared_ptr<arrow::BooleanArray> make_boolean_array(int64_t rows) {
    arrow::BooleanBuilder builder;
    throw_if_not_ok(builder.Reserve(rows));
    for (int64_t i = 0; i < rows; ++i) {
        throw_if_not_ok(builder.Append((i & 1) == 0));
    }
    std::shared_ptr<arrow::Array> array;
    throw_if_not_ok(builder.Finish(&array));
    return std::static_pointer_cast<arrow::BooleanArray>(array);
}

std::shared_ptr<arrow::ListArray> make_string_list_array(int64_t rows) {
    auto value_builder = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    throw_if_not_ok(builder.Reserve(rows));
    throw_if_not_ok(value_builder->Reserve(rows * 3));
    for (int64_t i = 0; i < rows; ++i) {
        throw_if_not_ok(builder.Append());
        for (int64_t j = 0; j < 3; ++j) {
            const std::string value = "item_" + std::to_string((i + j) % 1000);
            throw_if_not_ok(value_builder->Append(value));
        }
    }
    std::shared_ptr<arrow::Array> array;
    throw_if_not_ok(builder.Finish(&array));
    return std::static_pointer_cast<arrow::ListArray>(array);
}

std::shared_ptr<arrow::MapArray> make_string_int_map_array(int64_t rows) {
    auto keys = make_string_array(rows * 2);
    auto items = make_int64_array(rows * 2);
    std::vector<int32_t> offsets(rows + 1);
    for (int64_t i = 0; i <= rows; ++i) {
        offsets[i] = static_cast<int32_t>(i * 2);
    }
    auto offsets_buffer = arrow::Buffer::FromVector(std::move(offsets));
    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    return std::make_shared<arrow::MapArray>(map_type, rows, offsets_buffer, keys, items);
}

void read_string_old(ColumnString& column, const arrow::BinaryArray& array, int64_t start,
                     int64_t end) {
    std::shared_ptr<arrow::Buffer> buffer = array.value_data();
    const uint8_t* offsets_data = array.value_offsets()->data();
    const size_t offset_size = sizeof(int32_t);

    for (auto offset_i = start; offset_i < end; ++offset_i) {
        if (!array.IsNull(offset_i)) {
            auto start_offset = unaligned_load<int32_t>(offsets_data + offset_i * offset_size);
            auto end_offset = unaligned_load<int32_t>(offsets_data + (offset_i + 1) * offset_size);
            int32_t length = end_offset - start_offset;
            const auto* raw_data = buffer->data() + start_offset;
            column.insert_data(reinterpret_cast<const char*>(raw_data), length);
        } else {
            column.insert_default();
        }
    }
}

void read_int64_old(ColumnInt64& column, const arrow::Array& array, int64_t start, int64_t end) {
    auto& col_data = column.get_data();
    std::shared_ptr<arrow::Buffer> buffer = array.data()->buffers[1];
    const auto* raw_data = reinterpret_cast<const int64_t*>(buffer->data()) + start;
    col_data.insert(raw_data, raw_data + end - start);
}

void read_boolean_old(ColumnUInt8& column, const arrow::BooleanArray& array, int64_t start,
                      int64_t end) {
    auto& col_data = column.get_data();
    for (int64_t bool_i = start; bool_i != end; ++bool_i) {
        col_data.emplace_back(array.Value(bool_i));
    }
}

void read_array_string_old(ColumnArray& column, const arrow::ListArray& array, int64_t start,
                           int64_t end) {
    auto& offsets_data = column.get_offsets();
    auto arrow_offsets_array = array.offsets();
    auto* arrow_offsets = dynamic_cast<arrow::Int32Array*>(arrow_offsets_array.get());
    auto prev_size = offsets_data.back();
    const auto* base_offsets_ptr = reinterpret_cast<const uint8_t*>(arrow_offsets->raw_values());
    const size_t offset_element_size = sizeof(int32_t);
    const uint8_t* start_offset_ptr = base_offsets_ptr + start * offset_element_size;
    const uint8_t* end_offset_ptr = base_offsets_ptr + end * offset_element_size;
    auto arrow_nested_start_offset = unaligned_load<int32_t>(start_offset_ptr);
    auto arrow_nested_end_offset = unaligned_load<int32_t>(end_offset_ptr);

    for (auto i = start + 1; i < end + 1; ++i) {
        const uint8_t* current_offset_ptr = base_offsets_ptr + i * offset_element_size;
        auto current_offset = unaligned_load<int32_t>(current_offset_ptr);
        offsets_data.emplace_back(prev_size + current_offset - arrow_nested_start_offset);
    }
    auto& nested_nullable = assert_cast<ColumnNullable&>(column.get_data());
    read_string_old(assert_cast<ColumnString&>(nested_nullable.get_nested_column()),
                    static_cast<const arrow::BinaryArray&>(*array.values()),
                    arrow_nested_start_offset, arrow_nested_end_offset);
    auto& null_map = nested_nullable.get_null_map_data();
    null_map.resize_fill(null_map.size() + arrow_nested_end_offset - arrow_nested_start_offset, 0);
}

void read_map_string_int_old(ColumnMap& column, const arrow::MapArray& array, int64_t start,
                             int64_t end) {
    auto& offsets_data = column.get_offsets();
    auto arrow_offsets_array = array.offsets();
    auto* arrow_offsets = dynamic_cast<arrow::Int32Array*>(arrow_offsets_array.get());
    auto prev_size = offsets_data.back();
    const auto* base_offsets_ptr = reinterpret_cast<const uint8_t*>(arrow_offsets->raw_values());
    const size_t offset_element_size = sizeof(int32_t);
    const uint8_t* start_offset_ptr = base_offsets_ptr + start * offset_element_size;
    const uint8_t* end_offset_ptr = base_offsets_ptr + end * offset_element_size;
    auto arrow_nested_start_offset = unaligned_load<int32_t>(start_offset_ptr);
    auto arrow_nested_end_offset = unaligned_load<int32_t>(end_offset_ptr);
    for (int64_t i = start + 1; i < end + 1; ++i) {
        const uint8_t* current_offset_ptr = base_offsets_ptr + i * offset_element_size;
        auto current_offset = unaligned_load<int32_t>(current_offset_ptr);
        offsets_data.emplace_back(prev_size + current_offset - arrow_nested_start_offset);
    }
    read_string_old(assert_cast<ColumnString&>(column.get_keys()),
                    static_cast<const arrow::BinaryArray&>(*array.keys()),
                    arrow_nested_start_offset, arrow_nested_end_offset);
    read_int64_old(assert_cast<ColumnInt64&>(column.get_values()), *array.items(),
                   arrow_nested_start_offset, arrow_nested_end_offset);
}

template <ArrowValidationBenchMode mode, typename DataType, typename ArrowArray>
void run_serde_benchmark(benchmark::State& state, const std::shared_ptr<DataType>& type,
                         const std::shared_ptr<ArrowArray>& array) {
    const bool old_config = config::enable_arrow_input_validation;
    if constexpr (mode == ArrowValidationBenchMode::CHECK_DISABLED) {
        config::enable_arrow_input_validation = false;
    } else if constexpr (mode == ArrowValidationBenchMode::CHECK_ENABLED) {
        config::enable_arrow_input_validation = true;
    }

    for (auto _ : state) {
        const double seconds = measure_seconds([&] {
            auto column = type->create_column();
            throw_if_not_ok(type->get_serde()->read_column_from_arrow(
                    *column, array.get(), 0, array->length(), cctz::utc_time_zone()));
            keep_column_size(column->size());
        });
        state.SetIterationTime(seconds);
    }
    config::enable_arrow_input_validation = old_config;
}

template <ArrowValidationBenchMode mode>
void BM_ArrowValidation_String(benchmark::State& state) {
    auto array = make_string_array(state.range(0));
    auto type = std::make_shared<DataTypeString>();

    if constexpr (mode == ArrowValidationBenchMode::OLD) {
        for (auto _ : state) {
            const double seconds = measure_seconds([&] {
                auto column = ColumnString::create();
                read_string_old(*column, *array, 0, array->length());
                keep_column_size(column->size());
            });
            state.SetIterationTime(seconds);
        }
    } else {
        run_serde_benchmark<mode>(state, type, array);
    }
}

template <ArrowValidationBenchMode mode>
void BM_ArrowValidation_Int64(benchmark::State& state) {
    auto array = make_int64_array(state.range(0));
    auto type = std::make_shared<DataTypeInt64>();

    if constexpr (mode == ArrowValidationBenchMode::OLD) {
        for (auto _ : state) {
            const double seconds = measure_seconds([&] {
                auto column = ColumnInt64::create();
                read_int64_old(*column, *array, 0, array->length());
                keep_column_size(column->size());
            });
            state.SetIterationTime(seconds);
        }
    } else {
        run_serde_benchmark<mode>(state, type, array);
    }
}

template <ArrowValidationBenchMode mode>
void BM_ArrowValidation_Boolean(benchmark::State& state) {
    auto array = make_boolean_array(state.range(0));
    auto type = std::make_shared<DataTypeBool>();

    if constexpr (mode == ArrowValidationBenchMode::OLD) {
        for (auto _ : state) {
            const double seconds = measure_seconds([&] {
                auto column = ColumnUInt8::create();
                read_boolean_old(*column, *array, 0, array->length());
                keep_column_size(column->size());
            });
            state.SetIterationTime(seconds);
        }
    } else {
        run_serde_benchmark<mode>(state, type, array);
    }
}

template <ArrowValidationBenchMode mode>
void BM_ArrowValidation_ArrayString(benchmark::State& state) {
    auto array = make_string_list_array(state.range(0));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());

    if constexpr (mode == ArrowValidationBenchMode::OLD) {
        for (auto _ : state) {
            const double seconds = measure_seconds([&] {
                auto column = type->create_column();
                read_array_string_old(assert_cast<ColumnArray&>(*column), *array, 0,
                                      array->length());
                keep_column_size(column->size());
            });
            state.SetIterationTime(seconds);
        }
    } else {
        run_serde_benchmark<mode>(state, type, array);
    }
}

template <ArrowValidationBenchMode mode>
void BM_ArrowValidation_MapStringInt(benchmark::State& state) {
    auto array = make_string_int_map_array(state.range(0));
    auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                              std::make_shared<DataTypeInt64>());

    if constexpr (mode == ArrowValidationBenchMode::OLD) {
        for (auto _ : state) {
            const double seconds = measure_seconds([&] {
                auto column = type->create_column();
                read_map_string_int_old(assert_cast<ColumnMap&>(*column), *array, 0,
                                        array->length());
                keep_column_size(column->size());
            });
            state.SetIterationTime(seconds);
        }
    } else {
        run_serde_benchmark<mode>(state, type, array);
    }
}

#define REGISTER_ARROW_VALIDATION_BENCH(FUNC, MODE, NAME)               \
    BENCHMARK_TEMPLATE(FUNC, ArrowValidationBenchMode::MODE)            \
            ->Name("BM_ArrowValidation_" #NAME "_" #MODE "/rows:4096")  \
            ->Arg(4096)                                                 \
            ->UseManualTime();                                          \
    BENCHMARK_TEMPLATE(FUNC, ArrowValidationBenchMode::MODE)            \
            ->Name("BM_ArrowValidation_" #NAME "_" #MODE "/rows:65536") \
            ->Arg(65536)                                                \
            ->UseManualTime()

REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_String, OLD, String);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_String, CHECK_DISABLED, String);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_String, CHECK_ENABLED, String);

REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_Int64, OLD, Int64);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_Int64, CHECK_DISABLED, Int64);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_Int64, CHECK_ENABLED, Int64);

REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_Boolean, OLD, Boolean);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_Boolean, CHECK_DISABLED, Boolean);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_Boolean, CHECK_ENABLED, Boolean);

REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_ArrayString, OLD, ArrayString);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_ArrayString, CHECK_DISABLED, ArrayString);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_ArrayString, CHECK_ENABLED, ArrayString);

REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_MapStringInt, OLD, MapStringInt);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_MapStringInt, CHECK_DISABLED, MapStringInt);
REGISTER_ARROW_VALIDATION_BENCH(BM_ArrowValidation_MapStringInt, CHECK_ENABLED, MapStringInt);

#undef REGISTER_ARROW_VALIDATION_BENCH

} // namespace
} // namespace doris
