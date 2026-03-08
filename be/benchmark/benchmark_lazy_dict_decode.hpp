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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "util/coding.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"
#include "util/slice.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/custom_allocator.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/format/parquet/byte_array_dict_decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {

// ============================================================================
// P0-3 Benchmark: Lazy Dictionary Decode for Lazy String Columns
//
// This benchmark isolates the P0-3 optimization from P0-1, measuring four
// configurations for lazy string column reading in Phase 2:
//
//  1. Baseline (No P0-1, No P0-3):
//     Decode all N rows directly from dict -> ColumnString.
//     This is the original path for lazy columns.
//
//  2. P0-1 Only (No P0-3):
//     Decode with filter_data pushdown: only surviving rows are decoded
//     directly from dict -> ColumnString (via _lazy_decode_string_values).
//
//  3. P0-3 Only (No P0-1):
//     Decode all N rows to ColumnInt32 (dict codes), then filter the int32
//     column to keep only survivors, then convert_dict_column_to_string_column
//     on the filtered (smaller) ColumnInt32.
//
//  4. P0-3 + P0-1:
//     Decode with filter_data pushdown to ColumnInt32 (only surviving rows
//     get dict codes), then convert_dict_column_to_string_column on the
//     result. No intermediate filtering needed since decoder already skipped.
//
// Key dimensions: dict_size (cache effects), selectivity (filter ratio),
// avg_str_len (string materialization cost).
// ============================================================================

// ---- Reuse helpers from P0-1 benchmark ----

// Build dictionary data buffer for ByteArrayDictDecoder
static std::tuple<DorisUniqueBufferPtr<uint8_t>, int32_t, size_t> p03_build_string_dict(
        int dict_size, int avg_str_len) {
    std::mt19937 rng(42);
    std::vector<std::string> dict_strings;
    dict_strings.reserve(dict_size);
    for (int i = 0; i < dict_size; ++i) {
        std::string s(avg_str_len, 'a');
        for (int j = 0; j < avg_str_len; ++j) {
            s[j] = 'a' + (rng() % 26);
        }
        std::string suffix = "_" + std::to_string(i);
        if (static_cast<int>(suffix.size()) < avg_str_len) {
            s = s.substr(0, avg_str_len - suffix.size()) + suffix;
        }
        dict_strings.push_back(s);
    }

    size_t total_size = 0;
    for (auto& s : dict_strings) {
        total_size += 4 + s.size();
    }

    auto dict_data = make_unique_buffer<uint8_t>(total_size);
    size_t offset = 0;
    for (auto& s : dict_strings) {
        auto len = static_cast<uint32_t>(s.size());
        encode_fixed32_le(dict_data.get() + offset, len);
        offset += 4;
        memcpy(dict_data.get() + offset, s.data(), len);
        offset += len;
    }

    return {std::move(dict_data), static_cast<int32_t>(total_size), static_cast<size_t>(dict_size)};
}

// Build RLE-encoded dict index data
static std::vector<uint8_t> p03_build_rle_dict_indexes(int num_values, int dict_size,
                                                       unsigned seed = 123) {
    std::mt19937 rng(seed);
    int bit_width = 0;
    int tmp = dict_size - 1;
    while (tmp > 0) {
        bit_width++;
        tmp >>= 1;
    }
    if (bit_width == 0) bit_width = 1;

    faststring buffer;
    RleEncoder<uint32_t> encoder(&buffer, bit_width);
    for (int i = 0; i < num_values; ++i) {
        encoder.Put(rng() % dict_size);
    }
    encoder.Flush();

    std::vector<uint8_t> result;
    result.reserve(1 + buffer.size());
    result.push_back(static_cast<uint8_t>(bit_width));
    result.insert(result.end(), buffer.data(), buffer.data() + buffer.size());
    return result;
}

// Build run_length_null_map (no nulls)
static std::vector<uint16_t> p03_build_run_length_null_map(int num_values) {
    std::vector<uint16_t> result;
    int remaining = num_values;
    while (remaining > 0) {
        uint16_t chunk = static_cast<uint16_t>(std::min(remaining, 65535));
        if (!result.empty()) {
            result.push_back(0);
        }
        result.push_back(chunk);
        remaining -= chunk;
    }
    return result;
}

// Build filter bitmap with given selectivity
static std::vector<uint8_t> p03_build_filter_bitmap(int num_values, double selectivity,
                                                    unsigned seed = 456) {
    std::mt19937 rng(seed);
    std::vector<uint8_t> filter(num_values);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (int i = 0; i < num_values; ++i) {
        filter[i] = dist(rng) < selectivity ? 1 : 0;
    }
    return filter;
}

// Helper: filter a ColumnInt32 by bitmap, keeping only rows where filter[i]==1
static MutableColumnPtr p03_filter_int32_column(const ColumnInt32* src,
                                                const std::vector<uint8_t>& filter_bitmap) {
    auto result = ColumnInt32::create();
    const auto& data = src->get_data();
    for (size_t i = 0; i < data.size(); ++i) {
        if (filter_bitmap[i]) {
            result->insert_value(data[i]);
        }
    }
    return result;
}

// ============================================================================
// Group 1: Baseline — No P0-1, No P0-3
//
// Decode all rows dict -> ColumnString directly.
// decode_values(ColumnString, is_dict_filter=false, filter_data=nullptr)
// ============================================================================
static void BM_P03_Baseline(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;
    int avg_str_len = static_cast<int>(state.range(3));

    auto [dict_buf, dict_len, dict_count] = p03_build_string_dict(dict_size, avg_str_len);
    auto rle_data = p03_build_rle_dict_indexes(num_values, dict_size);
    auto filter_bitmap = p03_build_filter_bitmap(num_values, selectivity);

    for (auto _ : state) {
        state.PauseTiming();
        ByteArrayDictDecoder decoder;
        {
            auto dict_copy = make_unique_buffer<uint8_t>(dict_len);
            memcpy(dict_copy.get(), dict_buf.get(), dict_len);
            static_cast<void>(decoder.set_dict(dict_copy, dict_len, dict_count));
        }
        Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
        static_cast<void>(decoder.set_data(&data_slice));

        MutableColumnPtr column = ColumnString::create();
        DataTypePtr data_type = std::make_shared<DataTypeString>();

        auto run_length_null_map = p03_build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        // Decode ALL rows to ColumnString (no P0-1, no P0-3)
        static_cast<void>(decoder.decode_values(column, data_type, select_vector, false, nullptr));
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values) +
                   " strlen=" + std::to_string(avg_str_len));
}

// ============================================================================
// Group 2: P0-1 Only — filter bitmap pushdown, decode to ColumnString
//
// decode_values(ColumnString, is_dict_filter=false, filter_data=bitmap)
// Only surviving rows are decoded via _lazy_decode_string_values.
// ============================================================================
static void BM_P03_P01Only(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;
    int avg_str_len = static_cast<int>(state.range(3));

    auto [dict_buf, dict_len, dict_count] = p03_build_string_dict(dict_size, avg_str_len);
    auto rle_data = p03_build_rle_dict_indexes(num_values, dict_size);
    auto filter_bitmap = p03_build_filter_bitmap(num_values, selectivity);

    for (auto _ : state) {
        state.PauseTiming();
        ByteArrayDictDecoder decoder;
        {
            auto dict_copy = make_unique_buffer<uint8_t>(dict_len);
            memcpy(dict_copy.get(), dict_buf.get(), dict_len);
            static_cast<void>(decoder.set_dict(dict_copy, dict_len, dict_count));
        }
        Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
        static_cast<void>(decoder.set_data(&data_slice));

        MutableColumnPtr column = ColumnString::create();
        DataTypePtr data_type = std::make_shared<DataTypeString>();

        auto run_length_null_map = p03_build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        // Decode with P0-1 pushdown: only surviving rows get string materialized
        static_cast<void>(decoder.decode_values(column, data_type, select_vector, false,
                                                filter_bitmap.data()));
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values) +
                   " strlen=" + std::to_string(avg_str_len));
}

// ============================================================================
// Group 3: P0-3 Only — decode all rows to int32, filter, then convert survivors
//
// decode_values(ColumnInt32, is_dict_filter=true, filter_data=nullptr)
// Then filter ColumnInt32 by bitmap, then convert_dict_column_to_string_column.
// ============================================================================
static void BM_P03_P03Only(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;
    int avg_str_len = static_cast<int>(state.range(3));

    auto [dict_buf, dict_len, dict_count] = p03_build_string_dict(dict_size, avg_str_len);
    auto rle_data = p03_build_rle_dict_indexes(num_values, dict_size);
    auto filter_bitmap = p03_build_filter_bitmap(num_values, selectivity);

    for (auto _ : state) {
        state.PauseTiming();
        ByteArrayDictDecoder decoder;
        {
            auto dict_copy = make_unique_buffer<uint8_t>(dict_len);
            memcpy(dict_copy.get(), dict_buf.get(), dict_len);
            static_cast<void>(decoder.set_dict(dict_copy, dict_len, dict_count));
        }
        Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
        static_cast<void>(decoder.set_data(&data_slice));

        MutableColumnPtr column = ColumnInt32::create();
        DataTypePtr data_type = std::make_shared<DataTypeInt32>();

        auto run_length_null_map = p03_build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        // Step 1: Decode ALL rows to ColumnInt32 dict codes (no P0-1 pushdown)
        static_cast<void>(decoder.decode_values(column, data_type, select_vector, true, nullptr));

        // Step 2: Filter the int32 column (simulate Phase 2 filtering)
        const auto* int32_col = assert_cast<const ColumnInt32*>(column.get());
        auto filtered_col = p03_filter_int32_column(int32_col, filter_bitmap);

        // Step 3: Convert surviving dict codes to strings
        const auto* filtered_int32 = assert_cast<const ColumnInt32*>(filtered_col.get());
        auto string_col = decoder.convert_dict_column_to_string_column(filtered_int32);

        benchmark::DoNotOptimize(string_col);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values) +
                   " strlen=" + std::to_string(avg_str_len));
}

// ============================================================================
// Group 4: P0-3 + P0-1 — decode surviving rows to int32, then convert
//
// decode_values(ColumnInt32, is_dict_filter=true, filter_data=bitmap)
// Decoder skips filtered rows (P0-1). Output is already filtered int32 codes.
// Then convert_dict_column_to_string_column on the (small) result.
// ============================================================================
static void BM_P03_P03PlusP01(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;
    int avg_str_len = static_cast<int>(state.range(3));

    auto [dict_buf, dict_len, dict_count] = p03_build_string_dict(dict_size, avg_str_len);
    auto rle_data = p03_build_rle_dict_indexes(num_values, dict_size);
    auto filter_bitmap = p03_build_filter_bitmap(num_values, selectivity);

    for (auto _ : state) {
        state.PauseTiming();
        ByteArrayDictDecoder decoder;
        {
            auto dict_copy = make_unique_buffer<uint8_t>(dict_len);
            memcpy(dict_copy.get(), dict_buf.get(), dict_len);
            static_cast<void>(decoder.set_dict(dict_copy, dict_len, dict_count));
        }
        Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
        static_cast<void>(decoder.set_data(&data_slice));

        MutableColumnPtr column = ColumnInt32::create();
        DataTypePtr data_type = std::make_shared<DataTypeInt32>();

        auto run_length_null_map = p03_build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        // Step 1: Decode with P0-1 pushdown to ColumnInt32 (only survivors)
        static_cast<void>(decoder.decode_values(column, data_type, select_vector, true,
                                                filter_bitmap.data()));

        // Step 2: Convert surviving dict codes to strings (column already filtered)
        const auto* int32_col = assert_cast<const ColumnInt32*>(column.get());
        auto string_col = decoder.convert_dict_column_to_string_column(int32_col);

        benchmark::DoNotOptimize(string_col);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values) +
                   " strlen=" + std::to_string(avg_str_len));
}

// ============================================================================
// Group 5: Convert overhead — just the dict code -> string conversion
//
// Measure convert_dict_column_to_string_column in isolation for N rows.
// ============================================================================
static void BM_P03_ConvertOverhead(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    int num_values = static_cast<int>(state.range(1)) * 1000;
    int avg_str_len = static_cast<int>(state.range(2));

    auto [dict_buf, dict_len, dict_count] = p03_build_string_dict(dict_size, avg_str_len);

    // Build a ColumnInt32 with random dict codes
    std::mt19937 rng(789);
    auto int32_col = ColumnInt32::create();
    for (int i = 0; i < num_values; ++i) {
        int32_col->insert_value(rng() % dict_size);
    }

    // We need a decoder with dict loaded for convert_dict_column_to_string_column
    ByteArrayDictDecoder decoder;
    {
        auto dict_copy = make_unique_buffer<uint8_t>(dict_len);
        memcpy(dict_copy.get(), dict_buf.get(), dict_len);
        static_cast<void>(decoder.set_dict(dict_copy, dict_len, dict_count));
    }

    const ColumnInt32* raw_ptr = int32_col.get();

    for (auto _ : state) {
        auto string_col = decoder.convert_dict_column_to_string_column(raw_ptr);
        benchmark::DoNotOptimize(string_col);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " rows=" + std::to_string(num_values) +
                   " strlen=" + std::to_string(avg_str_len));
}

// ============================================================================
// Registrations
// ============================================================================
// Args: (dict_size, selectivity_percent, num_values_in_thousands, avg_str_len)

// Core comparison: small dict (100), various selectivities, 100K rows
// String lengths: 32 (short) and 128 (medium-long)
#define P03_CORE_ARGS                     \
    ->Args({100, 5, 100, 32})             \
            ->Args({100, 10, 100, 32})    \
            ->Args({100, 20, 100, 32})    \
            ->Args({100, 50, 100, 32})    \
            ->Args({100, 100, 100, 32})   \
            ->Args({100, 5, 100, 128})    \
            ->Args({100, 20, 100, 128})   \
            ->Args({100, 50, 100, 128})   \
            ->Args({100, 100, 100, 128})  \
            ->Args({10000, 5, 100, 32})   \
            ->Args({10000, 20, 100, 32})  \
            ->Args({10000, 50, 100, 32})  \
            ->Args({10000, 5, 100, 128})  \
            ->Args({10000, 20, 100, 128}) \
            ->Unit(benchmark::kMicrosecond)

// --- Group 1: Baseline ---
BENCHMARK(BM_P03_Baseline) P03_CORE_ARGS;

// --- Group 2: P0-1 Only ---
BENCHMARK(BM_P03_P01Only) P03_CORE_ARGS;

// --- Group 3: P0-3 Only ---
BENCHMARK(BM_P03_P03Only) P03_CORE_ARGS;

// --- Group 4: P0-3 + P0-1 ---
BENCHMARK(BM_P03_P03PlusP01) P03_CORE_ARGS;

// --- Group 5: Convert overhead ---
BENCHMARK(BM_P03_ConvertOverhead)
        ->Args({100, 5, 32})
        ->Args({100, 50, 32})
        ->Args({100, 100, 32})
        ->Args({100, 5, 128})
        ->Args({100, 100, 128})
        ->Args({10000, 5, 32})
        ->Args({10000, 100, 32})
        ->Args({10000, 5, 128})
        ->Args({10000, 100, 128})
        ->Unit(benchmark::kMicrosecond);

} // namespace doris::vectorized
