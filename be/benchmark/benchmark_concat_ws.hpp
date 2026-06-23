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
// Benchmark: concat_ws — old (per-row fmt::join) vs new (columnar two-pass)
//
// Old: clears a views vector per row, calls fmt::format_to(buffer, "{}", fmt::join(views, sep))
//      then StringOP::push_value_string — one heap-buffer write + one copy per row.
// New: pass-1 counts total output bytes; single res_data.resize(); pass-2 writes
//      directly to the output buffer via memcpy, no fmt overhead.
//
// Scenarios:
//   - 3 string args, small strings (~20 bytes each)
//   - 3 string args, medium strings (~100 bytes each)
//   - 8 string args, small strings (~20 bytes each)
//   - 3 string args, 30% null args (skip nulls)
// ============================================================

#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_string.h"
#include "exec/common/stringop_substring.h"

namespace doris {

using Chars = ColumnString::Chars;
using Offsets = ColumnString::Offsets;

// ---- Helpers ----

struct ColData {
    Chars chars;
    Offsets offsets;
    std::vector<uint8_t> nullmap; // 0 = not null
};

// Build a ColData with random strings of given avg_len, null_rate in [0,1]
static ColData build_col(size_t rows, size_t avg_len, double null_rate, unsigned seed = 42) {
    ColData col;
    col.offsets.resize(rows);
    col.nullmap.resize(rows, 0);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint8_t> char_dist('a', 'z');
    std::uniform_int_distribution<size_t> len_dist(avg_len > 4 ? avg_len - 4 : 0, avg_len + 4);
    std::uniform_real_distribution<double> null_dist(0.0, 1.0);

    size_t offset = 0;
    for (size_t i = 0; i < rows; ++i) {
        if (null_rate > 0.0 && null_dist(rng) < null_rate) {
            col.nullmap[i] = 1;
            col.offsets[i] = static_cast<uint32_t>(offset);
            continue;
        }
        size_t len = len_dist(rng);
        col.chars.resize(offset + len);
        for (size_t k = 0; k < len; ++k) {
            col.chars[offset + k] = static_cast<uint8_t>(char_dist(rng));
        }
        offset += len;
        col.offsets[i] = static_cast<uint32_t>(offset);
    }
    return col;
}

// Build a separator ColData (constant sep repeated for all rows)
static ColData build_sep_col(size_t rows, const std::string& sep) {
    ColData col;
    col.offsets.resize(rows);
    col.nullmap.resize(rows, 0);
    col.chars.resize(sep.size() * rows);
    size_t offset = 0;
    for (size_t i = 0; i < rows; ++i) {
        memcpy(col.chars.data() + offset, sep.data(), sep.size());
        offset += sep.size();
        col.offsets[i] = static_cast<uint32_t>(offset);
    }
    return col;
}

// ---- Old implementation: per-row fmt::join ----
static void concat_ws_old(size_t input_rows_count, const std::vector<const Offsets*>& offsets_list,
                          const std::vector<const Chars*>& chars_list,
                          const std::vector<const std::vector<uint8_t>*>& null_list,
                          Chars& res_data, Offsets& res_offset) {
    const size_t argument_size = offsets_list.size();
    fmt::memory_buffer buffer;
    std::vector<std::string_view> views;

    for (size_t i = 0; i < input_rows_count; ++i) {
        const auto& sep_offsets = *offsets_list[0];
        const auto& sep_chars = *chars_list[0];
        const auto& sep_nullmap = *null_list[0];
        if (sep_nullmap[i]) {
            res_offset[i] = static_cast<uint32_t>(res_data.size());
            continue;
        }

        uint32_t sep_size = sep_offsets[i] - sep_offsets[i - 1];
        const char* sep_data_ptr =
                reinterpret_cast<const char*>(&sep_chars[sep_offsets[i - 1]]);
        std::string_view sep(sep_data_ptr, sep_size);

        buffer.clear();
        views.clear();
        for (size_t j = 1; j < argument_size; ++j) {
            const auto& current_offsets = *offsets_list[j];
            const auto& current_chars = *chars_list[j];
            const auto& current_nullmap = *null_list[j];
            uint32_t size = current_offsets[i] - current_offsets[i - 1];
            const char* ptr =
                    reinterpret_cast<const char*>(&current_chars[current_offsets[i - 1]]);
            if (!current_nullmap[i]) {
                views.emplace_back(ptr, size);
            }
        }
        fmt::format_to(buffer, "{}", fmt::join(views, sep));
        StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                    res_offset);
    }
}

// ---- New implementation: two-pass + direct memcpy ----
// Pass 1: sum up output bytes; Pass 2: write directly to res_data.
static void concat_ws_new(size_t input_rows_count, const std::vector<const Offsets*>& offsets_list,
                          const std::vector<const Chars*>& chars_list,
                          const std::vector<const std::vector<uint8_t>*>& null_list,
                          Chars& res_data, Offsets& res_offset) {
    const size_t argument_size = offsets_list.size();

    // Pass 1: compute total output size
    size_t total_size = 0;
    for (size_t i = 0; i < input_rows_count; ++i) {
        if ((*null_list[0])[i]) {
            continue;
        }
        const uint32_t sep_size = (*offsets_list[0])[i] - (*offsets_list[0])[i - 1];
        int non_null_count = 0;
        for (size_t j = 1; j < argument_size; ++j) {
            if (!(*null_list[j])[i]) {
                total_size += (*offsets_list[j])[i] - (*offsets_list[j])[i - 1];
                ++non_null_count;
            }
        }
        if (non_null_count > 1) {
            total_size += static_cast<size_t>(sep_size) * (non_null_count - 1);
        }
    }

    res_data.resize(total_size);

    // Pass 2: write data
    size_t dst_offset = 0;
    auto* dst = reinterpret_cast<char*>(res_data.data());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if ((*null_list[0])[i]) {
            res_offset[i] = static_cast<uint32_t>(dst_offset);
            continue;
        }

        const auto& sep_offsets = *offsets_list[0];
        const auto& sep_chars = *chars_list[0];
        const uint32_t sep_size = sep_offsets[i] - sep_offsets[i - 1];
        const char* sep_data_ptr =
                reinterpret_cast<const char*>(&sep_chars[sep_offsets[i - 1]]);

        bool first = true;
        for (size_t j = 1; j < argument_size; ++j) {
            if ((*null_list[j])[i]) {
                continue;
            }
            const auto& cur_offsets = *offsets_list[j];
            const auto& cur_chars = *chars_list[j];
            const uint32_t size = cur_offsets[i] - cur_offsets[i - 1];

            if (!first && sep_size > 0) {
                memcpy(dst + dst_offset, sep_data_ptr, sep_size);
                dst_offset += sep_size;
            }
            first = false;

            if (size > 0) {
                memcpy(dst + dst_offset, cur_chars.data() + cur_offsets[i - 1], size);
                dst_offset += size;
            }
        }
        res_offset[i] = static_cast<uint32_t>(dst_offset);
    }
}

// ---- Dataset builder ----
struct ConcatWsDataset {
    // Index 0 = sep, 1..N = args
    std::vector<ColData> cols;
    std::vector<const Offsets*> offsets_list;
    std::vector<const Chars*> chars_list;
    std::vector<const std::vector<uint8_t>*> null_list;
    size_t rows;
};

static ConcatWsDataset build_dataset(size_t rows, size_t num_args, size_t avg_len,
                                     double null_rate, const std::string& sep = ",") {
    ConcatWsDataset ds;
    ds.rows = rows;
    ds.cols.reserve(num_args + 1);

    // Sep column (constant)
    ds.cols.push_back(build_sep_col(rows, sep));
    for (size_t j = 0; j < num_args; ++j) {
        ds.cols.push_back(build_col(rows, avg_len, null_rate, static_cast<unsigned>(42 + j)));
    }

    ds.offsets_list.resize(num_args + 1);
    ds.chars_list.resize(num_args + 1);
    ds.null_list.resize(num_args + 1);
    for (size_t i = 0; i <= num_args; ++i) {
        ds.offsets_list[i] = &ds.cols[i].offsets;
        ds.chars_list[i] = &ds.cols[i].chars;
        ds.null_list[i] = &ds.cols[i].nullmap;
    }
    return ds;
}

// ---- Benchmarks ----

// 3 args, small strings (~20 bytes), no nulls
static void BM_ConcatWs_Old_3Args_Small(benchmark::State& state) {
    auto ds = build_dataset(10000, 3, 20, 0.0);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_old(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

static void BM_ConcatWs_New_3Args_Small(benchmark::State& state) {
    auto ds = build_dataset(10000, 3, 20, 0.0);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_new(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

// 3 args, medium strings (~100 bytes), no nulls
static void BM_ConcatWs_Old_3Args_Medium(benchmark::State& state) {
    auto ds = build_dataset(5000, 3, 100, 0.0);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_old(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

static void BM_ConcatWs_New_3Args_Medium(benchmark::State& state) {
    auto ds = build_dataset(5000, 3, 100, 0.0);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_new(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

// 8 args, small strings, no nulls
static void BM_ConcatWs_Old_8Args_Small(benchmark::State& state) {
    auto ds = build_dataset(5000, 8, 20, 0.0);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_old(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

static void BM_ConcatWs_New_8Args_Small(benchmark::State& state) {
    auto ds = build_dataset(5000, 8, 20, 0.0);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_new(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

// 3 args, small strings, 30% null args
static void BM_ConcatWs_Old_3Args_Null(benchmark::State& state) {
    auto ds = build_dataset(10000, 3, 20, 0.3);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_old(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

static void BM_ConcatWs_New_3Args_Null(benchmark::State& state) {
    auto ds = build_dataset(10000, 3, 20, 0.3);
    for (auto _ : state) {
        Chars res_data;
        Offsets res_offset(ds.rows);
        concat_ws_new(ds.rows, ds.offsets_list, ds.chars_list, ds.null_list, res_data, res_offset);
        benchmark::DoNotOptimize(res_data);
    }
}

BENCHMARK(BM_ConcatWs_Old_3Args_Small);
BENCHMARK(BM_ConcatWs_New_3Args_Small);
BENCHMARK(BM_ConcatWs_Old_3Args_Medium);
BENCHMARK(BM_ConcatWs_New_3Args_Medium);
BENCHMARK(BM_ConcatWs_Old_8Args_Small);
BENCHMARK(BM_ConcatWs_New_8Args_Small);
BENCHMARK(BM_ConcatWs_Old_3Args_Null);
BENCHMARK(BM_ConcatWs_New_3Args_Null);

} // namespace doris
