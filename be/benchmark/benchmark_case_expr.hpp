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

#include <random>

#include "core/data_type/data_type_number.h"
#include "exprs/vcase_expr.h"

namespace doris {

// Keep the production result assembly visible in disassembly as well as timing it.
template <typename IndexType, PrimitiveType PT>
NO_INLINE ColumnPtr run_case_selection(const VCaseExpr& expr, const IndexType* indices,
                                       std::vector<ColumnPtr>& columns, size_t rows) {
    return expr._execute_update_result_impl<IndexType, ColumnVector<PT>>(indices, columns, rows);
}

template <typename IndexType, PrimitiveType PT>
void BM_CaseFloatSelection(benchmark::State& state) {
    const size_t rows = state.range(0);
    const size_t branches = state.range(1);
    const auto distribution = state.range(2);
    TExprNode node;
    node.__set_node_type(TExprNodeType::CASE_EXPR);
    node.__set_type(DataTypeNumber<PT>().to_thrift());
    node.__set_is_nullable(false);
    node.case_expr.__set_has_else_expr(true);
    VCaseExpr expr(node);
    std::mt19937 rng(20260912);
    std::vector<IndexType> indices(rows);
    for (size_t row = 0; row < rows; ++row) {
        // Interleaved, random, or 99% ELSE. All inputs are finite for before/after comparison.
        indices[row] = static_cast<IndexType>(distribution == 0 ? row % branches
                                              : distribution == 1
                                                      ? rng() % branches
                                                      : (row % 100 == 0 ? rng() % branches : 0));
    }
    std::vector<ColumnPtr> columns;
    for (size_t branch = 0; branch < branches; ++branch) {
        auto column = ColumnVector<PT>::create(rows);
        for (size_t row = 0; row < rows; ++row) {
            column->get_data()[row] =
                    static_cast<typename ColumnVector<PT>::value_type>((row + branch + 1) * 0.125);
        }
        columns.push_back(std::move(column));
    }
    for (auto _ : state) {
        auto result = run_case_selection<IndexType, PT>(expr, indices.data(), columns, rows);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * rows);
}

inline void case_float_arguments(benchmark::internal::Benchmark* benchmark) {
    for (int64_t rows : {31, 4096, 65536}) {
        for (int64_t branches : {3, 16}) {
            for (int64_t distribution : {0, 1, 2}) {
                benchmark->Args({rows, branches, distribution});
            }
        }
    }
}

BENCHMARK_TEMPLATE(BM_CaseFloatSelection, uint8_t, TYPE_FLOAT)->Apply(case_float_arguments);
BENCHMARK_TEMPLATE(BM_CaseFloatSelection, uint8_t, TYPE_DOUBLE)->Apply(case_float_arguments);
BENCHMARK_TEMPLATE(BM_CaseFloatSelection, uint16_t, TYPE_FLOAT)
        ->Args({4096, 257, 0})
        ->Args({4096, 257, 1})
        ->Args({4096, 257, 2});
BENCHMARK_TEMPLATE(BM_CaseFloatSelection, uint16_t, TYPE_DOUBLE)
        ->Args({4096, 257, 0})
        ->Args({4096, 257, 1})
        ->Args({4096, 257, 2});

} // namespace doris
