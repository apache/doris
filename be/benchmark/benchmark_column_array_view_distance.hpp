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
// Benchmark: ColumnArrayView vs hand-written for array distance
//
// Simulates the FunctionArrayDistance pattern:
//   - Build Array<Nullable(Float32)> columns
//   - Extract raw float* pointers + dimensions per row
//   - Call faiss L2 distance on each row pair
//
// Compares:
//   1. Hand-written: manual Const/Nullable unwrapping + offsets
//   2. ColumnArrayView: original row-view access via ArrayDataView::get_data()
//   3. ColumnArrayView flat access: prefetch flat data pointer + row offsets
// ============================================================

#include <benchmark/benchmark.h>

#include <cmath>
#include <cstdint>
#include <random>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_array_view.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/primitive_type.h"

namespace doris {

// Inline L2 distance to avoid faiss build dependency in benchmark.
// Both paths call the same function, so the measurement is purely
// about pointer-extraction overhead, not about the distance kernel.
static inline float inline_l2_distance(const float* x, const float* y, size_t d) {
    float sum = 0.0f;
    for (size_t i = 0; i < d; ++i) {
        float diff = x[i] - y[i];
        sum += diff * diff;
    }
    return std::sqrt(sum);
}

static constexpr size_t DIST_NUM_ROWS = 4096;
static constexpr size_t DIST_DIM = 128; // typical embedding dimension

// ============================================================
// Column factory: Array<Nullable(Float32)> with fixed dimension
// ============================================================

static ColumnPtr make_float_array_column_for_dist(size_t num_rows, size_t dim) {
    auto data_col = ColumnFloat32::create();
    auto null_col = ColumnUInt8::create();
    auto offsets = ColumnArray::ColumnOffsets::create();

    data_col->reserve(num_rows * dim);
    null_col->reserve(num_rows * dim);

    std::mt19937 rng(42);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

    size_t offset = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        for (size_t j = 0; j < dim; ++j) {
            data_col->insert_value(dist(rng));
            null_col->insert_value(0);
        }
        offset += dim;
        offsets->insert_value(offset);
    }

    auto nullable_data = ColumnNullable::create(std::move(data_col), std::move(null_col));
    return ColumnArray::create(std::move(nullable_data), std::move(offsets));
}

static ColumnPtr make_const_float_array_for_dist(size_t dim) {
    auto single = make_float_array_column_for_dist(1, dim);
    return ColumnConst::create(std::move(single), DIST_NUM_ROWS);
}

// ============================================================
// 1. Both columns non-const: L2 distance per row
// ============================================================

static void Handwritten_Distance_Plain_Plain(benchmark::State& state) {
    const auto col1 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    // Hand-written extraction (mirrors FunctionArrayDistance::execute_impl)
    const auto& arr1 = assert_cast<const ColumnArray&>(*col1);
    const auto& arr2 = assert_cast<const ColumnArray&>(*col2);
    const auto& nested1 = assert_cast<const ColumnNullable&>(arr1.get_data());
    const auto& nested2 = assert_cast<const ColumnNullable&>(arr2.get_data());
    const auto& float1 = assert_cast<const ColumnFloat32&>(nested1.get_nested_column());
    const auto& float2 = assert_cast<const ColumnFloat32&>(nested2.get_nested_column());
    const auto* fdata1 = float1.get_data().data();
    const auto* fdata2 = float2.get_data().data();
    const auto& offsets1 = arr1.get_offsets();
    const auto& offsets2 = arr2.get_offsets();

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            auto prev1 = offsets1[row - 1];
            auto prev2 = offsets2[row - 1];
            auto size1 = offsets1[row] - prev1;
            dst_data[row] = inline_l2_distance(fdata1 + prev1, fdata2 + prev2, size1);
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(Handwritten_Distance_Plain_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_Distance_Plain_Plain(benchmark::State& state) {
    const auto col1 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    const auto view1 = ColumnArrayView<TYPE_FLOAT>::create(col1);
    const auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            auto a1 = view1[row];
            auto a2 = view2[row];
            const float* p1 = a1.get_data();
            const float* p2 = a2.get_data();
            dst_data[row] = inline_l2_distance(p1, p2, a1.size());
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(ArrayView_Distance_Plain_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_Distance_Plain_Plain_Flat(benchmark::State& state) {
    const auto col1 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    const auto view1 = ColumnArrayView<TYPE_FLOAT>::create(col1);
    const auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);
    const auto* data1 = view1.get_data();
    const auto* data2 = view2.get_data();

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            size_t begin1 = view1.row_begin(row);
            size_t begin2 = view2.row_begin(row);
            size_t dim1 = view1.row_end(row) - begin1;
            dst_data[row] = inline_l2_distance(data1 + begin1, data2 + begin2, dim1);
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(ArrayView_Distance_Plain_Plain_Flat)->Unit(benchmark::kNanosecond);

// ============================================================
// 2. One column const (query vs many vectors)
// ============================================================

static void Handwritten_Distance_Const_Plain(benchmark::State& state) {
    const auto const_col = make_const_float_array_for_dist(DIST_DIM);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    // Extract const array once
    const auto& const_inner = assert_cast<const ColumnConst&>(*const_col).get_data_column();
    const auto& const_arr = assert_cast<const ColumnArray&>(const_inner);
    const auto& const_nested = assert_cast<const ColumnNullable&>(const_arr.get_data());
    const auto& const_float = assert_cast<const ColumnFloat32&>(const_nested.get_nested_column());
    const float* const_data = const_float.get_data().data();
    size_t const_dim = const_float.size();

    // Extract non-const array
    const auto& arr2 = assert_cast<const ColumnArray&>(*col2);
    const auto& nested2 = assert_cast<const ColumnNullable&>(arr2.get_data());
    const auto& float2 = assert_cast<const ColumnFloat32&>(nested2.get_nested_column());
    const auto* fdata2 = float2.get_data().data();
    const auto& offsets2 = arr2.get_offsets();

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            auto prev2 = offsets2[row - 1];
            dst_data[row] = inline_l2_distance(const_data, fdata2 + prev2, const_dim);
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(Handwritten_Distance_Const_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_Distance_Const_Plain(benchmark::State& state) {
    const auto const_col = make_const_float_array_for_dist(DIST_DIM);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    const auto view1 = ColumnArrayView<TYPE_FLOAT>::create(const_col);
    const auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            auto a1 = view1[row];
            auto a2 = view2[row];
            const float* p1 = a1.get_data();
            const float* p2 = a2.get_data();
            dst_data[row] = inline_l2_distance(p1, p2, a1.size());
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(ArrayView_Distance_Const_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_Distance_Const_Plain_Flat(benchmark::State& state) {
    const auto const_col = make_const_float_array_for_dist(DIST_DIM);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    const auto view1 = ColumnArrayView<TYPE_FLOAT>::create(const_col);
    const auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);
    const auto* data1 = view1.get_data();
    const auto* data2 = view2.get_data();

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            size_t begin1 = view1.row_begin(row);
            size_t begin2 = view2.row_begin(row);
            size_t dim1 = view1.row_end(row) - begin1;
            dst_data[row] = inline_l2_distance(data1 + begin1, data2 + begin2, dim1);
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(ArrayView_Distance_Const_Plain_Flat)->Unit(benchmark::kNanosecond);

// ============================================================
// 3. Nullable(Array) vs plain Array
// ============================================================

static ColumnPtr wrap_nullable_for_dist(const ColumnPtr& col) {
    return ColumnNullable::create(col->assert_mutable(), ColumnUInt8::create(col->size(), 0));
}

static void Handwritten_Distance_Nullable_Plain(benchmark::State& state) {
    const auto base1 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);
    const auto nullable_col1 = wrap_nullable_for_dist(base1);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    // Unwrap nullable
    const auto& nullable1 = assert_cast<const ColumnNullable&>(*nullable_col1);
    const auto& arr1 = assert_cast<const ColumnArray&>(nullable1.get_nested_column());
    const auto& nested1 = assert_cast<const ColumnNullable&>(arr1.get_data());
    const auto& float1 = assert_cast<const ColumnFloat32&>(nested1.get_nested_column());
    const auto* fdata1 = float1.get_data().data();
    const auto& offsets1 = arr1.get_offsets();

    const auto& arr2 = assert_cast<const ColumnArray&>(*col2);
    const auto& nested2 = assert_cast<const ColumnNullable&>(arr2.get_data());
    const auto& float2 = assert_cast<const ColumnFloat32&>(nested2.get_nested_column());
    const auto* fdata2 = float2.get_data().data();
    const auto& offsets2 = arr2.get_offsets();

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            auto prev1 = offsets1[row - 1];
            auto prev2 = offsets2[row - 1];
            auto size1 = offsets1[row] - prev1;
            dst_data[row] = inline_l2_distance(fdata1 + prev1, fdata2 + prev2, size1);
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(Handwritten_Distance_Nullable_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_Distance_Nullable_Plain(benchmark::State& state) {
    const auto base1 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);
    const auto nullable_col1 = wrap_nullable_for_dist(base1);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    const auto view1 = ColumnArrayView<TYPE_FLOAT>::create(nullable_col1);
    const auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            auto a1 = view1[row];
            auto a2 = view2[row];
            const float* p1 = a1.get_data();
            const float* p2 = a2.get_data();
            dst_data[row] = inline_l2_distance(p1, p2, a1.size());
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(ArrayView_Distance_Nullable_Plain)->Unit(benchmark::kNanosecond);

static void ArrayView_Distance_Nullable_Plain_Flat(benchmark::State& state) {
    const auto base1 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);
    const auto nullable_col1 = wrap_nullable_for_dist(base1);
    const auto col2 = make_float_array_column_for_dist(DIST_NUM_ROWS, DIST_DIM);

    const auto view1 = ColumnArrayView<TYPE_FLOAT>::create(nullable_col1);
    const auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);
    const auto* data1 = view1.get_data();
    const auto* data2 = view2.get_data();

    auto dst = ColumnFloat32::create(DIST_NUM_ROWS);
    auto& dst_data = dst->get_data();

    for (auto _ : state) {
        for (size_t row = 0; row < DIST_NUM_ROWS; ++row) {
            size_t begin1 = view1.row_begin(row);
            size_t begin2 = view2.row_begin(row);
            size_t dim1 = view1.row_end(row) - begin1;
            dst_data[row] = inline_l2_distance(data1 + begin1, data2 + begin2, dim1);
        }
        benchmark::ClobberMemory();
    }
}
BENCHMARK(ArrayView_Distance_Nullable_Plain_Flat)->Unit(benchmark::kNanosecond);

} // namespace doris
