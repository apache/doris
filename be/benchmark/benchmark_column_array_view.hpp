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

// ============================================================
// Benchmark: ColumnArrayView vs hand-written array column access
//
// ColumnArrayView (see column_array_view.h) provides a unified interface
// to read array column elements regardless of whether the underlying
// column is Plain, ColumnConst, or ColumnNullable.
//
// This benchmark measures whether ColumnArrayView introduces measurable
// overhead compared to hand-written (direct) array column access code.
//
// Test scenarios:
//   1. Int64 array: sum all elements across all rows
//   2. String array: sum lengths of all elements across all rows
//   3. Const array: same as above but with ColumnConst wrapper
//   4. Nullable array: with outer nullable wrapper
// ============================================================

#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_array_view.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/primitive_type.h"

namespace doris {

static constexpr size_t ARR_NUM_ROWS = 4096;
static constexpr size_t ARR_ELEM_PER_ROW = 8;

// ============================================================
// Array column factory helpers
// ============================================================

// Build Array<Nullable(Int64)> with ARR_NUM_ROWS rows, each having ARR_ELEM_PER_ROW elements.
static ColumnPtr make_int64_array_column() {
    auto data_col = ColumnInt64::create();
    auto null_col = ColumnUInt8::create();
    auto offsets = ColumnArray::ColumnOffsets::create();

    data_col->reserve(ARR_NUM_ROWS * ARR_ELEM_PER_ROW);
    null_col->reserve(ARR_NUM_ROWS * ARR_ELEM_PER_ROW);

    size_t offset = 0;
    for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
        for (size_t j = 0; j < ARR_ELEM_PER_ROW; ++j) {
            data_col->insert_value(static_cast<int64_t>(i * ARR_ELEM_PER_ROW + j + 1));
            null_col->insert_value(0);
        }
        offset += ARR_ELEM_PER_ROW;
        offsets->insert_value(offset);
    }

    auto nullable_data = ColumnNullable::create(std::move(data_col), std::move(null_col));
    return ColumnArray::create(std::move(nullable_data), std::move(offsets));
}

// Build Array<Nullable(Int64)> with some null elements (every 5th element is null).
static ColumnPtr make_int64_array_column_with_nulls() {
    auto data_col = ColumnInt64::create();
    auto null_col = ColumnUInt8::create();
    auto offsets = ColumnArray::ColumnOffsets::create();

    data_col->reserve(ARR_NUM_ROWS * ARR_ELEM_PER_ROW);
    null_col->reserve(ARR_NUM_ROWS * ARR_ELEM_PER_ROW);

    size_t offset = 0;
    size_t flat_idx = 0;
    for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
        for (size_t j = 0; j < ARR_ELEM_PER_ROW; ++j) {
            data_col->insert_value(static_cast<int64_t>(flat_idx + 1));
            null_col->insert_value(flat_idx % 5 == 0 ? 1 : 0);
            flat_idx++;
        }
        offset += ARR_ELEM_PER_ROW;
        offsets->insert_value(offset);
    }

    auto nullable_data = ColumnNullable::create(std::move(data_col), std::move(null_col));
    return ColumnArray::create(std::move(nullable_data), std::move(offsets));
}

// Build Array<Nullable(String)> with ARR_NUM_ROWS rows.
static ColumnPtr make_string_array_column() {
    auto data_col = ColumnString::create();
    auto null_col = ColumnUInt8::create();
    auto offsets = ColumnArray::ColumnOffsets::create();

    size_t offset = 0;
    for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
        for (size_t j = 0; j < ARR_ELEM_PER_ROW; ++j) {
            std::string val = "str_" + std::to_string(i * ARR_ELEM_PER_ROW + j);
            data_col->insert_data(val.data(), val.size());
            null_col->insert_value(0);
        }
        offset += ARR_ELEM_PER_ROW;
        offsets->insert_value(offset);
    }

    auto nullable_data = ColumnNullable::create(std::move(data_col), std::move(null_col));
    return ColumnArray::create(std::move(nullable_data), std::move(offsets));
}

// Wrap with outer Nullable (no rows are actually null, just the wrapper overhead).
static ColumnPtr wrap_nullable(const ColumnPtr& col) {
    return ColumnNullable::create(col->assume_mutable(),
                                  ColumnUInt8::create(col->size(), 0));
}

// Wrap as Const.
static ColumnPtr wrap_const(const ColumnPtr& col) {
    // Take the first row of the array column, make a 1-row column, then const-expand.
    auto single = col->clone_empty();
    single->insert_from(*col, 0);
    return ColumnConst::create(std::move(single), ARR_NUM_ROWS);
}

// ============================================================
// Hand-written accessor for Array<Nullable(Int64)>
// ============================================================

struct HandwrittenArrayAccessor {
    const ColumnArray::Offsets64& offsets;
    const ColumnInt64::Container& data;
    const NullMap& nested_null_map;

    explicit HandwrittenArrayAccessor(const ColumnPtr& col) 
            : offsets(assert_cast<const ColumnArray&>(*col).get_offsets()),
              data(assert_cast<const ColumnInt64&>(
                       assert_cast<const ColumnNullable&>(
                           assert_cast<const ColumnArray&>(*col).get_data())
                       .get_nested_column())
                   .get_data()),
              nested_null_map(assert_cast<const ColumnNullable&>(
                                  assert_cast<const ColumnArray&>(*col).get_data())
                              .get_null_map_data()) {}

    size_t row_begin(size_t row) const { return offsets[row - 1]; }
    size_t row_end(size_t row) const { return offsets[row]; }
    int64_t value_at(size_t flat_idx) const { return data[flat_idx]; }
    bool is_null_at(size_t flat_idx) const { return nested_null_map[flat_idx]; }
};

// ============================================================
// 1. Int64 Plain Array: sum all elements
// ============================================================

static void Handwritten_ArrayInt64_Plain(benchmark::State& state) {
    const auto col = make_int64_array_column();
    HandwrittenArrayAccessor acc(col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            size_t begin = acc.row_begin(i);
            size_t end = acc.row_end(i);
            for (size_t j = begin; j < end; ++j) {
                sum += acc.value_at(j);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_ArrayInt64_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_ArrayInt64_Plain(benchmark::State& state) {
    const auto col = make_int64_array_column();
    const auto view = ColumnArrayView<TYPE_BIGINT>::create(col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            auto arr = view[i];
            for (size_t j = 0; j < arr.size(); ++j) {
                sum += arr.value_at(j);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ArrayView_ArrayInt64_Plain)->Unit(benchmark::kNanosecond);

// ============================================================
// 2. Int64 Array with null elements: sum non-null elements
// ============================================================

static void Handwritten_ArrayInt64_WithNulls(benchmark::State& state) {
    const auto col = make_int64_array_column_with_nulls();
    HandwrittenArrayAccessor acc(col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            size_t begin = acc.row_begin(i);
            size_t end = acc.row_end(i);
            for (size_t j = begin; j < end; ++j) {
                if (!acc.is_null_at(j)) {
                    sum += acc.value_at(j);
                }
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_ArrayInt64_WithNulls)->Unit(benchmark::kNanosecond);

static void ArrayView_ArrayInt64_WithNulls(benchmark::State& state) {
    const auto col = make_int64_array_column_with_nulls();
    const auto view = ColumnArrayView<TYPE_BIGINT>::create(col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            auto arr = view[i];
            for (size_t j = 0; j < arr.size(); ++j) {
                if (!arr.is_null_at(j)) {
                    sum += arr.value_at(j);
                }
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ArrayView_ArrayInt64_WithNulls)->Unit(benchmark::kNanosecond);

// ============================================================
// 3. String Array: sum string lengths
// ============================================================

struct HandwrittenStringArrayAccessor {
    const ColumnArray::Offsets64& offsets;
    const ColumnString& str_col;
    const NullMap& nested_null_map;

    explicit HandwrittenStringArrayAccessor(const ColumnPtr& col) 
            : offsets(assert_cast<const ColumnArray&>(*col).get_offsets()),
              str_col(assert_cast<const ColumnString&>(
                          assert_cast<const ColumnNullable&>(
                              assert_cast<const ColumnArray&>(*col).get_data())
                          .get_nested_column())),
              nested_null_map(assert_cast<const ColumnNullable&>(
                                  assert_cast<const ColumnArray&>(*col).get_data())
                              .get_null_map_data()) {}

    size_t row_begin(size_t row) const { return offsets[row - 1]; }
    size_t row_end(size_t row) const { return offsets[row]; }
    StringRef value_at(size_t flat_idx) const { return str_col.get_data_at(flat_idx); }
    bool is_null_at(size_t flat_idx) const { return nested_null_map[flat_idx]; }
};

static void Handwritten_ArrayString_Plain(benchmark::State& state) {
    const auto col = make_string_array_column();
    HandwrittenStringArrayAccessor acc(col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            size_t begin = acc.row_begin(i);
            size_t end = acc.row_end(i);
            for (size_t j = begin; j < end; ++j) {
                sum += acc.value_at(j).size;
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_ArrayString_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_ArrayString_Plain(benchmark::State& state) {
    const auto col = make_string_array_column();
    const auto view = ColumnArrayView<TYPE_STRING>::create(col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            auto arr = view[i];
            for (size_t j = 0; j < arr.size(); ++j) {
                sum += arr.value_at(j).size;
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ArrayView_ArrayString_Plain)->Unit(benchmark::kNanosecond);

// ============================================================
// 4. Const Array: Const(Array<Int64>)
// ============================================================

static void Handwritten_ArrayInt64_Const(benchmark::State& state) {
    const auto base = make_int64_array_column();
    const auto const_col = wrap_const(base);
    // Hand-written: unpack const, then access the single row repeatedly
    const auto& inner = assert_cast<const ColumnConst&>(*const_col).get_data_column();
    const auto& array_col = assert_cast<const ColumnArray&>(inner);
    const auto& arr_offsets = array_col.get_offsets();
    const auto& nested_nullable = assert_cast<const ColumnNullable&>(array_col.get_data());
    const auto& int_data = assert_cast<const ColumnInt64&>(nested_nullable.get_nested_column()).get_data();

    size_t begin = arr_offsets[-1]; // sentinel = 0
    size_t end = arr_offsets[0];

    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            for (size_t j = begin; j < end; ++j) {
                sum += int_data[j];
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_ArrayInt64_Const)->Unit(benchmark::kNanosecond);

static void ArrayView_ArrayInt64_Const(benchmark::State& state) {
    const auto base = make_int64_array_column();
    const auto const_col = wrap_const(base);
    const auto view = ColumnArrayView<TYPE_BIGINT>::create(const_col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            auto arr = view[i];
            for (size_t j = 0; j < arr.size(); ++j) {
                sum += arr.value_at(j);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ArrayView_ArrayInt64_Const)->Unit(benchmark::kNanosecond);

// ============================================================
// 5. Nullable Array: Nullable(Array<Int64>)
// ============================================================

static void Handwritten_ArrayInt64_Nullable(benchmark::State& state) {
    const auto base = make_int64_array_column();
    const auto nullable_col = wrap_nullable(base);
    // Hand-written: unpack nullable
    const auto& nullable = assert_cast<const ColumnNullable&>(*nullable_col);
    const auto& outer_null_map = nullable.get_null_map_data();
    const auto& array_col = assert_cast<const ColumnArray&>(nullable.get_nested_column());
    const auto& arr_offsets = array_col.get_offsets();
    const auto& nested_nullable = assert_cast<const ColumnNullable&>(array_col.get_data());
    const auto& int_data = assert_cast<const ColumnInt64&>(nested_nullable.get_nested_column()).get_data();

    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            if (outer_null_map[i]) continue;
            size_t begin = arr_offsets[i - 1];
            size_t end = arr_offsets[i];
            for (size_t j = begin; j < end; ++j) {
                sum += int_data[j];
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_ArrayInt64_Nullable)->Unit(benchmark::kNanosecond);

static void ArrayView_ArrayInt64_Nullable(benchmark::State& state) {
    const auto base = make_int64_array_column();
    const auto nullable_col = wrap_nullable(base);
    const auto view = ColumnArrayView<TYPE_BIGINT>::create(nullable_col);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < ARR_NUM_ROWS; ++i) {
            if (view.is_null_at(i)) continue;
            auto arr = view[i];
            for (size_t j = 0; j < arr.size(); ++j) {
                sum += arr.value_at(j);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ArrayView_ArrayInt64_Nullable)->Unit(benchmark::kNanosecond);

} // namespace doris
