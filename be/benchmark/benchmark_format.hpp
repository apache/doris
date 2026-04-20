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
// Benchmark: format(const_pattern, string_col...) — Old vs New
//
// Old path (current Doris):
//   2-arg:     per-row fmt::format(pattern, string_value)
//   multi-arg: per-row dynamic_format_arg_store construction + fmt::vformat
//
// New path (fast path):
//   parse pattern once → substrings + arg-index list
//   pre-allocate entire result buffer
//   write via direct memcpy: literals + column data
//
// Scenarios:
//   2-arg:     format("Greeting: {}!", user_col)         — most common case
//   3-arg:     format("{} + {} = {}", a_col, b_col, c_col) — multi-arg
// ============================================================

#pragma once

#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_string.h"

namespace doris {

// ---- Data helpers -------------------------------------------------------

static ColumnString::MutablePtr make_string_col(size_t rows, const std::string& prefix) {
    auto col = ColumnString::create();
    col->reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        std::string s = prefix + std::to_string(i);
        col->insert_data(s.data(), s.size());
    }
    return col;
}

// ---- Inline fast-path (mirrors the implementation in function_format.cpp) --

struct BenchFastPattern {
    std::vector<std::string> substrings; // N+1 literal parts
    std::vector<size_t> indices;         // N arg indices (0-based into arg_cols)
};

static std::optional<BenchFastPattern> bench_parse_pattern(std::string_view pattern,
                                                           size_t arg_count) {
    BenchFastPattern result;
    size_t auto_idx = 0;
    int numbering = -1; // -1=undetermined, 0=auto, 1=manual
    std::string cur;

    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];
        if (c == '{') {
            if (i + 1 < pattern.size() && pattern[i + 1] == '{') {
                cur += '{';
                ++i;
                continue;
            }
            size_t j = i + 1;
            while (j < pattern.size() && pattern[j] != '}' && pattern[j] != ':') {
                ++j;
            }
            if (j >= pattern.size() || pattern[j] == ':') {
                return std::nullopt;
            }
            std::string_view idx_str = pattern.substr(i + 1, j - i - 1);
            result.substrings.push_back(std::move(cur));
            cur.clear();

            size_t arg_idx = 0;
            if (idx_str.empty()) {
                if (numbering == 1) return std::nullopt;
                numbering = 0;
                arg_idx = auto_idx++;
            } else {
                if (numbering == 0) return std::nullopt;
                numbering = 1;
                for (char ch : idx_str) {
                    if (ch < '0' || ch > '9') return std::nullopt;
                    arg_idx = arg_idx * 10 + static_cast<size_t>(ch - '0');
                }
            }
            if (arg_idx >= arg_count) return std::nullopt;
            result.indices.push_back(arg_idx);
            i = j;
        } else if (c == '}') {
            if (i + 1 < pattern.size() && pattern[i + 1] == '}') {
                cur += '}';
                ++i;
                continue;
            }
            return std::nullopt;
        } else {
            cur += c;
        }
    }
    result.substrings.push_back(std::move(cur));
    return result;
}

// Fast-path: parse once, single alloc, direct memcpy.
// arg_cols must be non-const (already unpacked from ColumnConst if needed).
static void bench_format_fast(const BenchFastPattern& parsed,
                              const std::vector<const ColumnString*>& arg_cols,
                              ColumnString* result, size_t rows) {
    auto& res_chars = result->get_chars();
    auto& res_offsets = result->get_offsets();
    res_offsets.resize(rows);

    size_t total_substr_per_row = 0;
    for (const auto& s : parsed.substrings) total_substr_per_row += s.size();

    size_t total_arg = 0;
    for (size_t idx : parsed.indices) {
        total_arg += arg_cols[idx]->get_offsets().back();
    }

    const size_t total = rows * total_substr_per_row + total_arg;
    res_chars.resize(total);

    const auto& subs = parsed.substrings;
    const size_t n_phs = parsed.indices.size();
    uint32_t dst = 0;

    for (size_t i = 0; i < rows; ++i) {
        if (!subs[0].empty()) {
            memcpy(res_chars.data() + dst, subs[0].data(), subs[0].size());
            dst += static_cast<uint32_t>(subs[0].size());
        }
        for (size_t j = 0; j < n_phs; ++j) {
            const size_t idx = parsed.indices[j];
            const auto& col_offsets = arg_cols[idx]->get_offsets();
            const auto& col_chars = arg_cols[idx]->get_chars();
            // col_offsets[i - 1]: PODArray pre-element at [-1] == 0 when i == 0
            const uint32_t arg_start = col_offsets[i - 1];
            const uint32_t arg_size = col_offsets[i] - arg_start;
            if (arg_size > 0) {
                memcpy(res_chars.data() + dst, col_chars.data() + arg_start, arg_size);
                dst += arg_size;
            }
            if (!subs[j + 1].empty()) {
                memcpy(res_chars.data() + dst, subs[j + 1].data(), subs[j + 1].size());
                dst += static_cast<uint32_t>(subs[j + 1].size());
            }
        }
        res_offsets[i] = dst;
    }
}

// ---- 2-arg benchmarks ---------------------------------------------------

static constexpr size_t kNumRows = 4096;
static const std::string k2ArgFormat = "Greeting: {}!";
static const std::string k3ArgFormat = "{} + {} = {}";

// Old: fmt::format per row
static void BM_Format_Old_2Arg(benchmark::State& state) {
    auto data_col = make_string_col(kNumRows, "user_");
    const std::string fmt_str = k2ArgFormat;
    std::string_view fmt_sv(fmt_str);

    for (auto _ : state) {
        auto result = ColumnString::create();
        std::string res;
        for (size_t i = 0; i < kNumRows; ++i) {
            StringRef val = data_col->get_data_at(i);
            res = fmt::format(fmt_sv, std::string_view(val.data, val.size));
            result->insert_data(res.data(), res.size());
        }
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kNumRows);
}

// New: parse once + single alloc + memcpy
static void BM_Format_New_2Arg(benchmark::State& state) {
    auto data_col = make_string_col(kNumRows, "user_");
    const std::string fmt_str = k2ArgFormat;
    std::vector<const ColumnString*> arg_cols = {data_col.get()};

    auto parsed = bench_parse_pattern(fmt_str, 1);
    DCHECK(parsed.has_value());

    for (auto _ : state) {
        auto result = ColumnString::create();
        bench_format_fast(*parsed, arg_cols, result.get(), kNumRows);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kNumRows);
}

// ---- 3-arg benchmarks ---------------------------------------------------

// Old: dynamic_format_arg_store per row
static void BM_Format_Old_3Arg(benchmark::State& state) {
    auto col_a = make_string_col(kNumRows, "alpha_");
    auto col_b = make_string_col(kNumRows, "beta_");
    auto col_c = make_string_col(kNumRows, "result_");
    const std::string fmt_str = k3ArgFormat;
    std::string_view fmt_sv(fmt_str);

    for (auto _ : state) {
        auto result = ColumnString::create();
        std::string res;
        for (size_t i = 0; i < kNumRows; ++i) {
            fmt::dynamic_format_arg_store<fmt::format_context> args;
            StringRef va = col_a->get_data_at(i);
            StringRef vb = col_b->get_data_at(i);
            StringRef vc = col_c->get_data_at(i);
            args.push_back(std::string_view(va.data, va.size));
            args.push_back(std::string_view(vb.data, vb.size));
            args.push_back(std::string_view(vc.data, vc.size));
            res = fmt::vformat(fmt_sv, args);
            result->insert_data(res.data(), res.size());
        }
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kNumRows);
}

// New: parse once + single alloc + memcpy (3 args)
static void BM_Format_New_3Arg(benchmark::State& state) {
    auto col_a = make_string_col(kNumRows, "alpha_");
    auto col_b = make_string_col(kNumRows, "beta_");
    auto col_c = make_string_col(kNumRows, "result_");
    const std::string fmt_str = k3ArgFormat;
    std::vector<const ColumnString*> arg_cols = {col_a.get(), col_b.get(), col_c.get()};

    auto parsed = bench_parse_pattern(fmt_str, 3);
    DCHECK(parsed.has_value());

    for (auto _ : state) {
        auto result = ColumnString::create();
        bench_format_fast(*parsed, arg_cols, result.get(), kNumRows);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kNumRows);
}

BENCHMARK(BM_Format_Old_2Arg)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Format_New_2Arg)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Format_Old_3Arg)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Format_New_3Arg)->Unit(benchmark::kMicrosecond);

} // namespace doris
