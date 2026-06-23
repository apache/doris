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

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/status.h>
#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_serde.h"

namespace doris {
namespace {

enum class ArrowValidateCaseKind {
    INT32,
    STRING,
    NULLABLE_STRING,
    ARRAY_STRING,
};

enum class ArrowValidateMode {
    VALIDATE,
    VALIDATE_FULL,
    READ_ONLY,
    VALIDATE_THEN_READ,
    VALIDATE_FULL_THEN_READ,
};

struct ArrowValidateCase {
    DataTypePtr data_type;
    std::shared_ptr<arrow::Array> array;
    std::vector<int32_t> int32_values;
    std::vector<int32_t> string_offsets;
    std::vector<int32_t> list_offsets;
    std::vector<uint8_t> validity_bitmap;
    std::string string_values;
    int64_t rows = 0;
    int64_t value_bytes = 0;
};

static void set_valid(std::vector<uint8_t>& bitmap, size_t index) {
    bitmap[index >> 3] |= static_cast<uint8_t>(1U << (index & 7));
}

static void fill_string_data(ArrowValidateCase& data, int64_t rows, int64_t string_length) {
    data.string_offsets.clear();
    data.string_values.clear();
    data.string_offsets.reserve(rows + 1);
    data.string_values.reserve(rows * string_length);
    data.string_offsets.push_back(0);
    for (int64_t i = 0; i < rows; ++i) {
        for (int64_t j = 0; j < string_length; ++j) {
            data.string_values.push_back(static_cast<char>('a' + (i + j) % 26));
        }
        data.string_offsets.push_back(static_cast<int32_t>(data.string_values.size()));
    }
    data.value_bytes = static_cast<int64_t>(data.string_values.size());
}

static std::shared_ptr<arrow::Array> make_string_array(ArrowValidateCase& data, int64_t rows,
                                                       const std::shared_ptr<arrow::Buffer>& nulls,
                                                       int64_t null_count) {
    auto offsets = arrow::Buffer::Wrap(data.string_offsets);
    auto values = arrow::Buffer::Wrap(reinterpret_cast<const uint8_t*>(data.string_values.data()),
                                      data.string_values.size());
    return std::make_shared<arrow::StringArray>(rows, offsets, values, nulls, null_count);
}

static std::shared_ptr<ArrowValidateCase> make_arrow_validate_case(ArrowValidateCaseKind kind,
                                                                   int64_t rows,
                                                                   int64_t string_length) {
    auto data = std::make_shared<ArrowValidateCase>();
    data->rows = rows;
    switch (kind) {
    case ArrowValidateCaseKind::INT32: {
        data->data_type = std::make_shared<DataTypeInt32>();
        data->int32_values.reserve(rows);
        for (int64_t i = 0; i < rows; ++i) {
            data->int32_values.push_back(static_cast<int32_t>(i));
        }
        auto values = arrow::Buffer::Wrap(data->int32_values);
        data->array = std::make_shared<arrow::Int32Array>(rows, values);
        data->value_bytes = rows * sizeof(int32_t);
        break;
    }
    case ArrowValidateCaseKind::STRING: {
        data->data_type = std::make_shared<DataTypeString>();
        fill_string_data(*data, rows, string_length);
        data->array = make_string_array(*data, rows, nullptr, 0);
        break;
    }
    case ArrowValidateCaseKind::NULLABLE_STRING: {
        data->data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        fill_string_data(*data, rows, string_length);
        data->validity_bitmap.assign((rows + 7) / 8, 0);
        int64_t null_count = 0;
        for (int64_t i = 0; i < rows; ++i) {
            if (i % 7 == 0) {
                ++null_count;
            } else {
                set_valid(data->validity_bitmap, static_cast<size_t>(i));
            }
        }
        auto nulls = arrow::Buffer::Wrap(data->validity_bitmap);
        data->array = make_string_array(*data, rows, nulls, null_count);
        break;
    }
    case ArrowValidateCaseKind::ARRAY_STRING: {
        static constexpr int64_t elements_per_row = 4;
        const int64_t values_count = rows * elements_per_row;
        data->data_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        fill_string_data(*data, values_count, string_length);
        auto values_array = make_string_array(*data, values_count, nullptr, 0);
        data->list_offsets.reserve(rows + 1);
        for (int64_t i = 0; i <= rows; ++i) {
            data->list_offsets.push_back(static_cast<int32_t>(i * elements_per_row));
        }
        auto offsets = arrow::Buffer::Wrap(data->list_offsets);
        data->array = std::make_shared<arrow::ListArray>(arrow::list(arrow::utf8()), rows, offsets,
                                                         values_array, nullptr, 0);
        break;
    }
    }
    return data;
}

static void check_arrow_status(benchmark::State& state, const arrow::Status& status) {
    if (!status.ok()) {
        state.SkipWithError(status.ToString().c_str());
    }
}

static void check_doris_status(benchmark::State& state, const Status& status) {
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
    }
}

static void run_arrow_validate_only(benchmark::State& state, ArrowValidateCaseKind kind,
                                    ArrowValidateMode mode) {
    const auto data = make_arrow_validate_case(kind, state.range(0), state.range(1));
    for (auto _ : state) {
        auto status = mode == ArrowValidateMode::VALIDATE ? data->array->Validate()
                                                          : data->array->ValidateFull();
        check_arrow_status(state, status);
        benchmark::DoNotOptimize(status);
    }
    state.SetItemsProcessed(state.iterations() * data->rows);
    state.SetBytesProcessed(state.iterations() * data->value_bytes);
}

static void run_arrow_read(benchmark::State& state, ArrowValidateCaseKind kind,
                           ArrowValidateMode mode) {
    const auto data = make_arrow_validate_case(kind, state.range(0), state.range(1));
    auto serde = data->data_type->get_serde();
    const auto timezone = cctz::utc_time_zone();
    for (auto _ : state) {
        if (mode == ArrowValidateMode::VALIDATE_THEN_READ) {
            auto arrow_status = data->array->Validate();
            check_arrow_status(state, arrow_status);
            benchmark::DoNotOptimize(arrow_status);
        } else if (mode == ArrowValidateMode::VALIDATE_FULL_THEN_READ) {
            auto arrow_status = data->array->ValidateFull();
            check_arrow_status(state, arrow_status);
            benchmark::DoNotOptimize(arrow_status);
        }
        auto column = data->data_type->create_column();
        auto status =
                serde->read_column_from_arrow(*column, data->array.get(), 0, data->rows, timezone);
        check_doris_status(state, status);
        benchmark::DoNotOptimize(status);
        auto column_size = column->size();
        benchmark::DoNotOptimize(column_size);
    }
    state.SetItemsProcessed(state.iterations() * data->rows);
    state.SetBytesProcessed(state.iterations() * data->value_bytes);
}

static void BM_ArrowValidateOnly(benchmark::State& state, ArrowValidateCaseKind kind,
                                 ArrowValidateMode mode) {
    run_arrow_validate_only(state, kind, mode);
}

static void BM_ArrowValidateRead(benchmark::State& state, ArrowValidateCaseKind kind,
                                 ArrowValidateMode mode) {
    run_arrow_read(state, kind, mode);
}

static void apply_arrow_validate_args(benchmark::internal::Benchmark* benchmark, int64_t str_len) {
    benchmark->Args({4096, str_len})->Args({65536, str_len})->Unit(benchmark::kMicrosecond);
}

BENCHMARK_CAPTURE(BM_ArrowValidateOnly, Int32_Validate, ArrowValidateCaseKind::INT32,
                  ArrowValidateMode::VALIDATE)
        ->Args({4096, 0})
        ->Args({65536, 0})
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_ArrowValidateOnly, Int32_ValidateFull, ArrowValidateCaseKind::INT32,
                  ArrowValidateMode::VALIDATE_FULL)
        ->Args({4096, 0})
        ->Args({65536, 0})
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_ArrowValidateRead, Int32_ReadOnly, ArrowValidateCaseKind::INT32,
                  ArrowValidateMode::READ_ONLY)
        ->Args({4096, 0})
        ->Args({65536, 0})
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_ArrowValidateRead, Int32_ValidateThenRead, ArrowValidateCaseKind::INT32,
                  ArrowValidateMode::VALIDATE_THEN_READ)
        ->Args({4096, 0})
        ->Args({65536, 0})
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_ArrowValidateRead, Int32_ValidateFullThenRead, ArrowValidateCaseKind::INT32,
                  ArrowValidateMode::VALIDATE_FULL_THEN_READ)
        ->Args({4096, 0})
        ->Args({65536, 0})
        ->Unit(benchmark::kMicrosecond);

#define REGISTER_ARROW_VALIDATE_STRING_CASES(kind_name, kind, str_len)                        \
    BENCHMARK_CAPTURE(BM_ArrowValidateOnly, kind_name##_Validate_##str_len, kind,             \
                      ArrowValidateMode::VALIDATE)                                            \
            ->Apply([](benchmark::internal::Benchmark* b) {                                   \
                apply_arrow_validate_args(b, str_len);                                        \
            });                                                                               \
    BENCHMARK_CAPTURE(BM_ArrowValidateOnly, kind_name##_ValidateFull_##str_len, kind,         \
                      ArrowValidateMode::VALIDATE_FULL)                                       \
            ->Apply([](benchmark::internal::Benchmark* b) {                                   \
                apply_arrow_validate_args(b, str_len);                                        \
            });                                                                               \
    BENCHMARK_CAPTURE(BM_ArrowValidateRead, kind_name##_ReadOnly_##str_len, kind,             \
                      ArrowValidateMode::READ_ONLY)                                           \
            ->Apply([](benchmark::internal::Benchmark* b) {                                   \
                apply_arrow_validate_args(b, str_len);                                        \
            });                                                                               \
    BENCHMARK_CAPTURE(BM_ArrowValidateRead, kind_name##_ValidateThenRead_##str_len, kind,     \
                      ArrowValidateMode::VALIDATE_THEN_READ)                                  \
            ->Apply([](benchmark::internal::Benchmark* b) {                                   \
                apply_arrow_validate_args(b, str_len);                                        \
            });                                                                               \
    BENCHMARK_CAPTURE(BM_ArrowValidateRead, kind_name##_ValidateFullThenRead_##str_len, kind, \
                      ArrowValidateMode::VALIDATE_FULL_THEN_READ)                             \
            ->Apply([](benchmark::internal::Benchmark* b) {                                   \
                apply_arrow_validate_args(b, str_len);                                        \
            })

REGISTER_ARROW_VALIDATE_STRING_CASES(String, ArrowValidateCaseKind::STRING, 8);
REGISTER_ARROW_VALIDATE_STRING_CASES(String, ArrowValidateCaseKind::STRING, 128);
REGISTER_ARROW_VALIDATE_STRING_CASES(NullableString, ArrowValidateCaseKind::NULLABLE_STRING, 8);
REGISTER_ARROW_VALIDATE_STRING_CASES(NullableString, ArrowValidateCaseKind::NULLABLE_STRING, 128);
REGISTER_ARROW_VALIDATE_STRING_CASES(ArrayString, ArrowValidateCaseKind::ARRAY_STRING, 8);
REGISTER_ARROW_VALIDATE_STRING_CASES(ArrayString, ArrowValidateCaseKind::ARRAY_STRING, 128);

#undef REGISTER_ARROW_VALIDATE_STRING_CASES

} // namespace
} // namespace doris
