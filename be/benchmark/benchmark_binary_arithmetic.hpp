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

#include <cstdint>
#include <random>
#include <stdexcept>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"

namespace doris {
namespace {

// Runtime guardrails for the binary-arithmetic template refactors
// (compile-opt Phase 2/4/5): every case goes through the real
// SimpleFunctionFactory dispatch, so kernel swaps and dead-registration
// removals are covered end to end. check_overflow_for_decimal is pinned to
// the production default (true). Data is deterministic and sized so the
// overflow check never actually throws.

constexpr size_t kBinaryArithmeticRows = 4096;

// Drives "multiply" on a prepared two-column block. Constructed outside the
// timed loop; run_once is the measured unit.
struct BinaryArithmeticRunner {
    Block block;
    FunctionBasePtr func;
    std::unique_ptr<FunctionContext> ctx;
    uint32_t result_idx;

    BinaryArithmeticRunner(ColumnPtr col_a, DataTypePtr type_a, ColumnPtr col_b,
                           DataTypePtr type_b, DataTypePtr res_type) {
        block.insert({std::move(col_a), type_a, "a"});
        block.insert({std::move(col_b), type_b, "b"});
        func = SimpleFunctionFactory::instance().get_function(
                "multiply", block.get_columns_with_type_and_name(), res_type);
        if (func == nullptr) {
            throw std::runtime_error("multiply not found for benchmark argument types");
        }
        ctx = FunctionContext::create_context(nullptr, res_type, {type_a, type_b});
        ctx->set_check_overflow_for_decimal(true);
        if (!func->open(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok() ||
            !func->open(ctx.get(), FunctionContext::THREAD_LOCAL).ok()) {
            throw std::runtime_error("multiply open failed");
        }
        block.insert({nullptr, res_type, "result"});
        result_idx = block.columns() - 1;
    }

    void run_once(size_t rows) {
        Status st = func->execute(ctx.get(), block, {0, 1}, result_idx, rows);
        if (!st.ok()) {
            throw std::runtime_error(st.to_string());
        }
        benchmark::DoNotOptimize(block.get_by_position(result_idx).column);
    }
};

template <PrimitiveType PT>
ColumnPtr make_decimal_bench_column(size_t rows, UInt32 scale, int64_t native_lo,
                                    int64_t native_hi, uint64_t seed) {
    auto col = ColumnDecimal<PT>::create(rows, scale);
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int64_t> dist(native_lo, native_hi - 1);
    auto& data = col->get_data();
    for (size_t i = 0; i < rows; ++i) {
        data[i] = typename ColumnDecimal<PT>::value_type(dist(rng));
    }
    return col;
}

ColumnPtr make_int64_bench_column(size_t rows, uint64_t seed) {
    auto col = ColumnVector<TYPE_BIGINT>::create(rows);
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int64_t> dist(1, 999'999);
    auto& data = col->get_data();
    for (size_t i = 0; i < rows; ++i) {
        data[i] = dist(rng);
    }
    return col;
}

// BIGINT * BIGINT -> BIGINT, vector_vector.
void BM_multiply_int64_vec_vec(benchmark::State& state) {
    auto type = std::make_shared<DataTypeInt64>();
    BinaryArithmeticRunner runner(make_int64_bench_column(kBinaryArithmeticRows, 0x1001),
                                  type, make_int64_bench_column(kBinaryArithmeticRows, 0x1002),
                                  type, type);
    for (auto _ : state) {
        runner.run_once(kBinaryArithmeticRows);
    }
    state.SetItemsProcessed(state.iterations() * kBinaryArithmeticRows);
}

// DECIMAL64(18,4) * DECIMAL64(18,4) -> DECIMAL128(36,8): the same-width kernel
// that survives every phase of the refactor. Native values stay < 1e9 so the
// int128 product never trips the overflow check, and 4+4 == 8 means no scale
// adjustment (the common FE-planned shape).
void BM_multiply_d64_d64_vec_vec(benchmark::State& state) {
    auto type_a = std::make_shared<DataTypeDecimal64>(18, 4);
    auto res_type = std::make_shared<DataTypeDecimal128>(36, 8);
    BinaryArithmeticRunner runner(
            make_decimal_bench_column<TYPE_DECIMAL64>(kBinaryArithmeticRows, 4, 10'000,
                                                      999'999'999, 0x2001),
            type_a,
            make_decimal_bench_column<TYPE_DECIMAL64>(kBinaryArithmeticRows, 4, 10'000,
                                                      999'999'999, 0x2002),
            type_a, res_type);
    for (auto _ : state) {
        runner.run_once(kBinaryArithmeticRows);
    }
    state.SetItemsProcessed(state.iterations() * kBinaryArithmeticRows);
}

// DECIMAL32(9,2) * DECIMAL64(18,4) -> DECIMAL128(27,6): the mixed-width shape
// Phase 5 wants to eliminate; its rows/s before/after is the A/B material for
// the FE cast-to-same-width decision.
void BM_multiply_d32_d64_vec_vec(benchmark::State& state) {
    auto type_a = std::make_shared<DataTypeDecimal32>(9, 2);
    auto type_b = std::make_shared<DataTypeDecimal64>(18, 4);
    auto res_type = std::make_shared<DataTypeDecimal128>(27, 6);
    BinaryArithmeticRunner runner(
            make_decimal_bench_column<TYPE_DECIMAL32>(kBinaryArithmeticRows, 2, 100, 9'999'999,
                                                      0x3001),
            type_a,
            make_decimal_bench_column<TYPE_DECIMAL64>(kBinaryArithmeticRows, 4, 10'000,
                                                      999'999'999, 0x3002),
            type_b, res_type);
    for (auto _ : state) {
        runner.run_once(kBinaryArithmeticRows);
    }
    state.SetItemsProcessed(state.iterations() * kBinaryArithmeticRows);
}

// DECIMAL64 column * DECIMAL64 constant: the vector_constant fast path.
void BM_multiply_d64_d64_vec_const(benchmark::State& state) {
    auto type_a = std::make_shared<DataTypeDecimal64>(18, 4);
    auto res_type = std::make_shared<DataTypeDecimal128>(36, 8);
    auto const_col = ColumnConst::create(
            make_decimal_bench_column<TYPE_DECIMAL64>(1, 4, 12'345, 12'346, 0x4002),
            kBinaryArithmeticRows);
    BinaryArithmeticRunner runner(
            make_decimal_bench_column<TYPE_DECIMAL64>(kBinaryArithmeticRows, 4, 10'000,
                                                      999'999'999, 0x4001),
            type_a, std::move(const_col), type_a, res_type);
    for (auto _ : state) {
        runner.run_once(kBinaryArithmeticRows);
    }
    state.SetItemsProcessed(state.iterations() * kBinaryArithmeticRows);
}

// DECIMAL64 constant * DECIMAL64 constant, one row: measures per-call cost of
// the constant_constant path Phase 2b removes in favor of the default
// unwrap-execute-rewrap implementation. Items == calls, not rows.
void BM_multiply_d64_d64_const_const(benchmark::State& state) {
    auto type_a = std::make_shared<DataTypeDecimal64>(18, 4);
    auto res_type = std::make_shared<DataTypeDecimal128>(36, 8);
    auto const_a = ColumnConst::create(
            make_decimal_bench_column<TYPE_DECIMAL64>(1, 4, 54'321, 54'322, 0x5001), 1);
    auto const_b = ColumnConst::create(
            make_decimal_bench_column<TYPE_DECIMAL64>(1, 4, 12'345, 12'346, 0x5002), 1);
    BinaryArithmeticRunner runner(std::move(const_a), type_a, std::move(const_b), type_a,
                                  res_type);
    for (auto _ : state) {
        runner.run_once(1);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_multiply_int64_vec_vec);
BENCHMARK(BM_multiply_d64_d64_vec_vec);
BENCHMARK(BM_multiply_d32_d64_vec_vec);
BENCHMARK(BM_multiply_d64_d64_vec_const);
BENCHMARK(BM_multiply_d64_d64_const_const);

} // namespace
} // namespace doris
