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

#include "common/config.h"
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
#include "vec/exec/format/parquet/fix_length_dict_decoder.hpp"
#include "vec/exec/format/parquet/fix_length_plain_decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {

// ============================================================================
// P1-4/5/6 Benchmark: Independent + Combined Test Groups
//
// Test Matrix (for dict decoders):
//   Group A (Baseline):   SIMD=off, Prefetch=off — pure scalar loop
//   Group B (P1-4 Only):  SIMD=on,  Prefetch=off — SIMD gather, no prefetch
//   Group C (P1-5 Only):  SIMD=off, Prefetch=on  — scalar loop + sw prefetch
//   Group D (P1-4+P1-5):  SIMD=on,  Prefetch=on  — full optimized path
//
// For each group: INT32 / INT64 / String × dict={100, 10K, 1M} × rows={100K, 500K}
//
// Group E: P1-6 Plain Fast Path (independent, no config interaction)
//   No-null memcpy fast path vs with-nulls run-loop × INT32/INT64 × rows={100K, 500K, 1M}
// ============================================================================

// ---- Helpers ----

static std::tuple<DorisUniqueBufferPtr<uint8_t>, int32_t, size_t> p1_build_int32_dict(
        int dict_size) {
    auto dict_data = make_unique_buffer<uint8_t>(dict_size * sizeof(int32_t));
    auto* ptr = reinterpret_cast<int32_t*>(dict_data.get());
    for (int i = 0; i < dict_size; ++i) {
        ptr[i] = i * 7 + 13;
    }
    return {std::move(dict_data), static_cast<int32_t>(dict_size * sizeof(int32_t)),
            static_cast<size_t>(dict_size)};
}

static std::tuple<DorisUniqueBufferPtr<uint8_t>, int32_t, size_t> p1_build_int64_dict(
        int dict_size) {
    auto dict_data = make_unique_buffer<uint8_t>(dict_size * sizeof(int64_t));
    auto* ptr = reinterpret_cast<int64_t*>(dict_data.get());
    for (int i = 0; i < dict_size; ++i) {
        ptr[i] = static_cast<int64_t>(i) * 17 + 42;
    }
    return {std::move(dict_data), static_cast<int32_t>(dict_size * sizeof(int64_t)),
            static_cast<size_t>(dict_size)};
}

static std::tuple<DorisUniqueBufferPtr<uint8_t>, int32_t, size_t> p1_build_string_dict(
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
        s = s.substr(0, avg_str_len - suffix.size()) + suffix;
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

static std::vector<uint8_t> p1_build_rle_dict_indexes(int num_values, int dict_size,
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

static std::vector<uint16_t> p1_build_run_length_null_map(int num_values) {
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

// ---- RAII config guard ----
struct ConfigGuard {
    bool saved_simd;
    bool saved_prefetch;
    ConfigGuard(bool simd, bool prefetch) {
        saved_simd = config::enable_parquet_simd_dict_decode;
        saved_prefetch = config::enable_parquet_dict_prefetch;
        config::enable_parquet_simd_dict_decode = simd;
        config::enable_parquet_dict_prefetch = prefetch;
    }
    ~ConfigGuard() {
        config::enable_parquet_simd_dict_decode = saved_simd;
        config::enable_parquet_dict_prefetch = saved_prefetch;
    }
};

// ============================================================================
// Parameterized INT32 Dict Decode Benchmark
// Args: (dict_size, num_values_k)
// The config mode is set before calling.
// ============================================================================

static void BM_INT32_DictDecode(benchmark::State& state, bool simd, bool prefetch,
                                const std::string& label_prefix) {
    int dict_size = static_cast<int>(state.range(0));
    int num_values = static_cast<int>(state.range(1)) * 1000;

    auto [dict_buf, dict_len, dict_count] = p1_build_int32_dict(dict_size);
    auto rle_data = p1_build_rle_dict_indexes(num_values, dict_size);

    ConfigGuard guard(simd, prefetch);

    for (auto _ : state) {
        state.PauseTiming();
        FixLengthDictDecoder<tparquet::Type::INT32> decoder;
        decoder.set_type_length(sizeof(int32_t));
        {
            auto dict_copy = make_unique_buffer<uint8_t>(dict_len);
            memcpy(dict_copy.get(), dict_buf.get(), dict_len);
            static_cast<void>(decoder.set_dict(dict_copy, dict_len, dict_count));
        }
        Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
        static_cast<void>(decoder.set_data(&data_slice));
        MutableColumnPtr column = ColumnInt32::create();
        DataTypePtr data_type = std::make_shared<DataTypeInt32>();
        auto run_length_null_map = p1_build_run_length_null_map(num_values);
        FilterMap filter_map;
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        auto status = decoder.decode_values(column, data_type, select_vector, false, nullptr);
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel(label_prefix + " dict=" + std::to_string(dict_size) +
                   " rows=" + std::to_string(num_values));
}

// ============================================================================
// Parameterized INT64 Dict Decode Benchmark
// ============================================================================

static void BM_INT64_DictDecode(benchmark::State& state, bool simd, bool prefetch,
                                const std::string& label_prefix) {
    int dict_size = static_cast<int>(state.range(0));
    int num_values = static_cast<int>(state.range(1)) * 1000;

    auto [dict_buf, dict_len, dict_count] = p1_build_int64_dict(dict_size);
    auto rle_data = p1_build_rle_dict_indexes(num_values, dict_size);

    ConfigGuard guard(simd, prefetch);

    for (auto _ : state) {
        state.PauseTiming();
        FixLengthDictDecoder<tparquet::Type::INT64> decoder;
        decoder.set_type_length(sizeof(int64_t));
        {
            auto dict_copy = make_unique_buffer<uint8_t>(dict_len);
            memcpy(dict_copy.get(), dict_buf.get(), dict_len);
            static_cast<void>(decoder.set_dict(dict_copy, dict_len, dict_count));
        }
        Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
        static_cast<void>(decoder.set_data(&data_slice));
        MutableColumnPtr column = ColumnInt64::create();
        DataTypePtr data_type = std::make_shared<DataTypeInt64>();
        auto run_length_null_map = p1_build_run_length_null_map(num_values);
        FilterMap filter_map;
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        auto status = decoder.decode_values(column, data_type, select_vector, false, nullptr);
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel(label_prefix + " dict=" + std::to_string(dict_size) +
                   " rows=" + std::to_string(num_values));
}

// ============================================================================
// Parameterized String Dict Decode Benchmark
// ============================================================================

static void BM_String_DictDecode(benchmark::State& state, bool simd, bool prefetch,
                                 const std::string& label_prefix) {
    int dict_size = static_cast<int>(state.range(0));
    int num_values = static_cast<int>(state.range(1)) * 1000;
    int avg_str_len = 32;

    auto [dict_buf, dict_len, dict_count] = p1_build_string_dict(dict_size, avg_str_len);
    auto rle_data = p1_build_rle_dict_indexes(num_values, dict_size);

    ConfigGuard guard(simd, prefetch);

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
        auto run_length_null_map = p1_build_run_length_null_map(num_values);
        FilterMap filter_map;
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        auto status = decoder.decode_values(column, data_type, select_vector, false, nullptr);
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel(label_prefix + " dict=" + std::to_string(dict_size) +
                   " rows=" + std::to_string(num_values));
}

// ============================================================================
// Group A: Baseline (SIMD=off, Prefetch=off)
// ============================================================================

static void BM_GroupA_INT32_Baseline(benchmark::State& state) {
    BM_INT32_DictDecode(state, false, false, "A:Baseline");
}
static void BM_GroupA_INT64_Baseline(benchmark::State& state) {
    BM_INT64_DictDecode(state, false, false, "A:Baseline");
}
static void BM_GroupA_String_Baseline(benchmark::State& state) {
    BM_String_DictDecode(state, false, false, "A:Baseline");
}

// ============================================================================
// Group B: P1-4 Only (SIMD=on, Prefetch=off)
// ============================================================================

static void BM_GroupB_INT32_SIMD(benchmark::State& state) {
    BM_INT32_DictDecode(state, true, false, "B:SIMD");
}
static void BM_GroupB_INT64_SIMD(benchmark::State& state) {
    BM_INT64_DictDecode(state, true, false, "B:SIMD");
}
static void BM_GroupB_String_SIMD(benchmark::State& state) {
    BM_String_DictDecode(state, true, false, "B:SIMD");
}

// ============================================================================
// Group C: P1-5 Only (SIMD=off, Prefetch=on)
// ============================================================================

static void BM_GroupC_INT32_Prefetch(benchmark::State& state) {
    BM_INT32_DictDecode(state, false, true, "C:Prefetch");
}
static void BM_GroupC_INT64_Prefetch(benchmark::State& state) {
    BM_INT64_DictDecode(state, false, true, "C:Prefetch");
}
static void BM_GroupC_String_Prefetch(benchmark::State& state) {
    BM_String_DictDecode(state, false, true, "C:Prefetch");
}

// ============================================================================
// Group D: P1-4+P1-5 Combined (SIMD=on, Prefetch=on)
// ============================================================================

static void BM_GroupD_INT32_SIMD_Prefetch(benchmark::State& state) {
    BM_INT32_DictDecode(state, true, true, "D:SIMD+PF");
}
static void BM_GroupD_INT64_SIMD_Prefetch(benchmark::State& state) {
    BM_INT64_DictDecode(state, true, true, "D:SIMD+PF");
}
static void BM_GroupD_String_SIMD_Prefetch(benchmark::State& state) {
    BM_String_DictDecode(state, true, true, "D:SIMD+PF");
}

// ============================================================================
// Group E: P1-6 Plain Fast Path (Independent)
// ============================================================================

static void BM_GroupE_PlainFastPath(benchmark::State& state) {
    int num_values = static_cast<int>(state.range(0)) * 1000;
    int type_length = static_cast<int>(state.range(1));

    std::mt19937 rng(789);
    size_t total_bytes = static_cast<size_t>(num_values) * type_length;
    std::vector<char> plain_data(total_bytes);
    for (size_t i = 0; i < total_bytes; ++i) {
        plain_data[i] = static_cast<char>(rng() % 256);
    }

    for (auto _ : state) {
        state.PauseTiming();
        FixLengthPlainDecoder decoder;
        decoder.set_type_length(type_length);
        Slice data_slice(plain_data.data(), plain_data.size());
        static_cast<void>(decoder.set_data(&data_slice));

        MutableColumnPtr column = ColumnInt32::create();
        DataTypePtr data_type = std::make_shared<DataTypeInt32>();
        if (type_length == 8) {
            column = ColumnInt64::create();
            data_type = std::make_shared<DataTypeInt64>();
        }

        auto run_length_null_map = p1_build_run_length_null_map(num_values);
        FilterMap filter_map;
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        auto status = decoder.decode_values(column, data_type, select_vector, false, nullptr);
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("E:FastPath rows=" + std::to_string(num_values) +
                   " type_len=" + std::to_string(type_length));
}

static void BM_GroupE_PlainWithNulls(benchmark::State& state) {
    int num_values = static_cast<int>(state.range(0)) * 1000;
    int type_length = static_cast<int>(state.range(1));

    std::mt19937 rng(789);
    size_t total_bytes = static_cast<size_t>(num_values) * type_length;
    std::vector<char> plain_data(total_bytes);
    for (size_t i = 0; i < total_bytes; ++i) {
        plain_data[i] = static_cast<char>(rng() % 256);
    }

    // Build null map with ~10% nulls
    std::vector<uint16_t> null_map;
    std::mt19937 null_rng(456);
    int remaining = num_values;
    bool is_content = true;
    while (remaining > 0) {
        int run;
        if (is_content) {
            run = std::min(remaining, static_cast<int>(null_rng() % 50 + 5));
        } else {
            run = std::min(remaining, static_cast<int>(null_rng() % 5 + 1));
        }
        null_map.push_back(static_cast<uint16_t>(run));
        remaining -= run;
        is_content = !is_content;
    }

    for (auto _ : state) {
        state.PauseTiming();
        FixLengthPlainDecoder decoder;
        decoder.set_type_length(type_length);
        Slice data_slice(plain_data.data(), plain_data.size());
        static_cast<void>(decoder.set_data(&data_slice));

        MutableColumnPtr column = ColumnInt32::create();
        DataTypePtr data_type = std::make_shared<DataTypeInt32>();
        if (type_length == 8) {
            column = ColumnInt64::create();
            data_type = std::make_shared<DataTypeInt64>();
        }

        FilterMap null_filter_map;
        ColumnSelectVector select_vector;
        static_cast<void>(select_vector.init(null_map, num_values, nullptr, &null_filter_map, 0));
        state.ResumeTiming();

        auto status = decoder.decode_values(column, data_type, select_vector, false, nullptr);
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("E:WithNulls rows=" + std::to_string(num_values) +
                   " type_len=" + std::to_string(type_length) + " nulls=10%");
}

// ============================================================================
// Benchmark Registrations
// ============================================================================

// Standard args for dict decoders: (dict_size, num_values_k)
// dict_size: 100 (L1), 10000 (L2), 1000000 (>L2)
// rows_k: 100, 500

#define DICT_BENCH_ARGS            \
    ->Args({100, 100})             \
            ->Args({100, 500})     \
            ->Args({10000, 100})   \
            ->Args({10000, 500})   \
            ->Args({1000000, 100}) \
            ->Args({1000000, 500}) \
            ->Unit(benchmark::kMicrosecond)

// =============================================
// INT32 (all 4 groups in sequence for easy comparison)
// =============================================

BENCHMARK(BM_GroupA_INT32_Baseline) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupB_INT32_SIMD) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupC_INT32_Prefetch) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupD_INT32_SIMD_Prefetch) DICT_BENCH_ARGS;

// =============================================
// INT64 (all 4 groups)
// =============================================

BENCHMARK(BM_GroupA_INT64_Baseline) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupB_INT64_SIMD) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupC_INT64_Prefetch) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupD_INT64_SIMD_Prefetch) DICT_BENCH_ARGS;

// =============================================
// String (all 4 groups)
// =============================================

BENCHMARK(BM_GroupA_String_Baseline) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupB_String_SIMD) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupC_String_Prefetch) DICT_BENCH_ARGS;
BENCHMARK(BM_GroupD_String_SIMD_Prefetch) DICT_BENCH_ARGS;

#undef DICT_BENCH_ARGS

// =============================================
// P1-6 Plain Fast Path (Group E)
// =============================================

BENCHMARK(BM_GroupE_PlainFastPath)
        ->Args({100, 4})
        ->Args({500, 4})
        ->Args({1000, 4})
        ->Args({100, 8})
        ->Args({500, 8})
        ->Args({1000, 8})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_GroupE_PlainWithNulls)
        ->Args({100, 4})
        ->Args({500, 4})
        ->Args({1000, 4})
        ->Args({100, 8})
        ->Args({500, 8})
        ->Args({1000, 8})
        ->Unit(benchmark::kMicrosecond);

} // namespace doris::vectorized
