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
// Benchmark: ColumnView vs hand-written column access (Int64)
//
// ColumnView (see column_execute_util.h) provides a unified interface
// to read column values regardless of whether the underlying column is
// Plain, ColumnConst, ColumnNullable, or Const(Nullable).
//
// This benchmark measures whether ColumnView introduces measurable
// overhead compared to hand-written (direct) column access code.
// ============================================================

#include <benchmark/benchmark.h>

#include <cstdint>

#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/primitive_type.h"

namespace doris {

static constexpr size_t NUM_ROWS = 4096;

// ============================================================
// Column factory helpers
// ============================================================

static ColumnPtr make_plain_column() {
    auto col = ColumnInt64::create();
    col->reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        col->insert_value(static_cast<int64_t>(i + 1));
    }
    return col;
}

static ColumnPtr make_const_column() {
    auto inner = ColumnInt64::create();
    inner->insert_value(42);
    return ColumnConst::create(std::move(inner), NUM_ROWS);
}

static ColumnPtr make_nullable_column() {
    return ColumnNullable::create(make_plain_column()->assume_mutable(),
                                  ColumnUInt8::create(NUM_ROWS, 0));
}

// ============================================================
// Helper: extract Int64 data from various column forms
// ============================================================

struct PlainAccessor {
    const ColumnInt64::Container& data;

    explicit PlainAccessor(const ColumnPtr& col)
            : data(assert_cast<const ColumnInt64&>(*col).get_data()) {}

    int64_t get(size_t i) const { return data[i]; }
};

struct ConstAccessor {
    const int64_t value;

    explicit ConstAccessor(const ColumnPtr& col)
            : value(assert_cast<const ColumnInt64&>(
                            assert_cast<const ColumnConst&>(*col).get_data_column())
                            .get_data()[0]) {}

    int64_t get(size_t /*i*/) const { return value; }
};

struct NullableAccessor {
    const ColumnInt64::Container& data;
    const NullMap& null_map;

    explicit NullableAccessor(const ColumnPtr& col)
            : data(assert_cast<const ColumnInt64&>(
                           assert_cast<const ColumnNullable&>(*col).get_nested_column())
                           .get_data()),
              null_map(assert_cast<const ColumnNullable&>(*col).get_null_map_data()) {}

    int64_t get(size_t i) const { return data[i]; }
    bool is_null(size_t i) const { return null_map[i]; }
};

struct ConstNullableAccessor {
    const int64_t value;
    const bool is_null_value;

    explicit ConstNullableAccessor(const ColumnPtr& col)
            : value(assert_cast<const ColumnInt64&>(
                            assert_cast<const ColumnNullable&>(
                                    assert_cast<const ColumnConst&>(*col).get_data_column())
                                    .get_nested_column())
                            .get_data()[0]),
              is_null_value(assert_cast<const ColumnNullable&>(
                                    assert_cast<const ColumnConst&>(*col).get_data_column())
                                    .get_null_map_data()[0]) {}

    int64_t get(size_t /*i*/) const { return value; }
    bool is_null(size_t /*i*/) const { return is_null_value; }
};

// ============================================================
// Unary benchmarks: sum = Σ a[i]
// ============================================================

// ---- Unary: Plain ----

static void Handwritten_Unary_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    PlainAccessor a(col_a);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += a.get(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Unary_Plain)->Unit(benchmark::kNanosecond);

static void ColumnView_Unary_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += view_a.value_at(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Unary_Plain)->Unit(benchmark::kNanosecond);

// ---- Unary: Nullable ----

static void Handwritten_Unary_Nullable(benchmark::State& state) {
    const auto col_a = make_nullable_column();
    NullableAccessor a(col_a);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            if (!a.is_null(i)) {
                sum += a.get(i);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Unary_Nullable)->Unit(benchmark::kNanosecond);

static void ColumnView_Unary_Nullable(benchmark::State& state) {
    const auto col_a = make_nullable_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            if (!view_a.is_null_at(i)) {
                sum += view_a.value_at(i);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Unary_Nullable)->Unit(benchmark::kNanosecond);

// ============================================================
// Binary benchmarks: sum = Σ (a[i] + b[i])
// ============================================================

// ---- Binary: (Plain, Plain) ----

static void Handwritten_Binary_Plain_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_plain_column();
    PlainAccessor a(col_a);
    PlainAccessor b(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += a.get(i) + b.get(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Binary_Plain_Plain)->Unit(benchmark::kNanosecond);

static void ColumnView_Binary_Plain_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_plain_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += view_a.value_at(i) + view_b.value_at(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Binary_Plain_Plain)->Unit(benchmark::kNanosecond);

// ---- Binary: (Plain, Const) ----

static void Handwritten_Binary_Plain_Const(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_const_column();
    PlainAccessor a(col_a);
    ConstAccessor b(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += a.get(i) + b.get(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Binary_Plain_Const)->Unit(benchmark::kNanosecond);

static void ColumnView_Binary_Plain_Const(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_const_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += view_a.value_at(i) + view_b.value_at(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Binary_Plain_Const)->Unit(benchmark::kNanosecond);

// ---- Binary: (Plain, Nullable) ----

static void Handwritten_Binary_Plain_Nullable(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_nullable_column();
    PlainAccessor a(col_a);
    NullableAccessor b(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            int64_t val = a.get(i);
            if (!b.is_null(i)) {
                val += b.get(i);
            }
            sum += val;
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Binary_Plain_Nullable)->Unit(benchmark::kNanosecond);

static void ColumnView_Binary_Plain_Nullable(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_nullable_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            int64_t val = view_a.value_at(i);
            if (!view_b.is_null_at(i)) {
                val += view_b.value_at(i);
            }
            sum += val;
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Binary_Plain_Nullable)->Unit(benchmark::kNanosecond);

// ---- Binary: (Nullable, Nullable) ----

static void Handwritten_Binary_Nullable_Nullable(benchmark::State& state) {
    const auto col_a = make_nullable_column();
    const auto col_b = make_nullable_column();
    NullableAccessor a(col_a);
    NullableAccessor b(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            if (!a.is_null(i) && !b.is_null(i)) {
                sum += a.get(i) + b.get(i);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Binary_Nullable_Nullable)->Unit(benchmark::kNanosecond);

static void ColumnView_Binary_Nullable_Nullable(benchmark::State& state) {
    const auto col_a = make_nullable_column();
    const auto col_b = make_nullable_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            if (!view_a.is_null_at(i) && !view_b.is_null_at(i)) {
                sum += view_a.value_at(i) + view_b.value_at(i);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Binary_Nullable_Nullable)->Unit(benchmark::kNanosecond);

// ============================================================
// Ternary benchmarks: sum = Σ (a[i] + b[i] + c[i])
// ============================================================

// ---- Ternary: (Plain, Plain, Plain) ----

static void Handwritten_Ternary_Plain_Plain_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_plain_column();
    const auto col_c = make_plain_column();
    PlainAccessor a(col_a);
    PlainAccessor b(col_b);
    PlainAccessor c(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += a.get(i) + b.get(i) + c.get(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Ternary_Plain_Plain_Plain)->Unit(benchmark::kNanosecond);

static void ColumnView_Ternary_Plain_Plain_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_plain_column();
    const auto col_c = make_plain_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    const auto view_c = ColumnView<TYPE_BIGINT>::create(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += view_a.value_at(i) + view_b.value_at(i) + view_c.value_at(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Ternary_Plain_Plain_Plain)->Unit(benchmark::kNanosecond);

// ---- Ternary: (Const, Const, Plain) ----

static void Handwritten_Ternary_Const_Const_Plain(benchmark::State& state) {
    const auto col_a = make_const_column();
    const auto col_b = make_const_column();
    const auto col_c = make_plain_column();
    ConstAccessor a(col_a);
    ConstAccessor b(col_b);
    PlainAccessor c(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += a.get(i) + b.get(i) + c.get(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Ternary_Const_Const_Plain)->Unit(benchmark::kNanosecond);

static void ColumnView_Ternary_Const_Const_Plain(benchmark::State& state) {
    const auto col_a = make_const_column();
    const auto col_b = make_const_column();
    const auto col_c = make_plain_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    const auto view_c = ColumnView<TYPE_BIGINT>::create(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += view_a.value_at(i) + view_b.value_at(i) + view_c.value_at(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Ternary_Const_Const_Plain)->Unit(benchmark::kNanosecond);

// ---- Ternary: (Plain, Const, Plain) ----

static void Handwritten_Ternary_Plain_Const_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_const_column();
    const auto col_c = make_plain_column();
    PlainAccessor a(col_a);
    ConstAccessor b(col_b);
    PlainAccessor c(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += a.get(i) + b.get(i) + c.get(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Ternary_Plain_Const_Plain)->Unit(benchmark::kNanosecond);

static void ColumnView_Ternary_Plain_Const_Plain(benchmark::State& state) {
    const auto col_a = make_plain_column();
    const auto col_b = make_const_column();
    const auto col_c = make_plain_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    const auto view_c = ColumnView<TYPE_BIGINT>::create(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            sum += view_a.value_at(i) + view_b.value_at(i) + view_c.value_at(i);
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Ternary_Plain_Const_Plain)->Unit(benchmark::kNanosecond);

// ---- Ternary: (Nullable, Nullable, Nullable) ----

static void Handwritten_Ternary_Nullable_Nullable_Nullable(benchmark::State& state) {
    const auto col_a = make_nullable_column();
    const auto col_b = make_nullable_column();
    const auto col_c = make_nullable_column();
    NullableAccessor a(col_a);
    NullableAccessor b(col_b);
    NullableAccessor c(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            if (!a.is_null(i) && !b.is_null(i) && !c.is_null(i)) {
                sum += a.get(i) + b.get(i) + c.get(i);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(Handwritten_Ternary_Nullable_Nullable_Nullable)->Unit(benchmark::kNanosecond);

static void ColumnView_Ternary_Nullable_Nullable_Nullable(benchmark::State& state) {
    const auto col_a = make_nullable_column();
    const auto col_b = make_nullable_column();
    const auto col_c = make_nullable_column();
    const auto view_a = ColumnView<TYPE_BIGINT>::create(col_a);
    const auto view_b = ColumnView<TYPE_BIGINT>::create(col_b);
    const auto view_c = ColumnView<TYPE_BIGINT>::create(col_c);
    for (auto _ : state) {
        int64_t sum = 0;
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            if (!view_a.is_null_at(i) && !view_b.is_null_at(i) && !view_c.is_null_at(i)) {
                sum += view_a.value_at(i) + view_b.value_at(i) + view_c.value_at(i);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(ColumnView_Ternary_Nullable_Nullable_Nullable)->Unit(benchmark::kNanosecond);

} // namespace doris
