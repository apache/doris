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
#include "vec/exec/format/parquet/fix_length_dict_decoder.hpp"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {

// ============================================================================
// Helper: Build dictionary data buffer for ByteArrayDictDecoder
// ============================================================================
// Returns (dict_buffer, total_size, num_entries)
static std::tuple<DorisUniqueBufferPtr<uint8_t>, int32_t, size_t> build_string_dict(
        int dict_size, int avg_str_len) {
    // Generate deterministic dictionary strings
    std::mt19937 rng(42);
    std::vector<std::string> dict_strings;
    dict_strings.reserve(dict_size);
    for (int i = 0; i < dict_size; ++i) {
        // Create a string of avg_str_len with random content
        std::string s(avg_str_len, 'a');
        for (int j = 0; j < avg_str_len; ++j) {
            s[j] = 'a' + (rng() % 26);
        }
        // Append index to ensure uniqueness
        std::string suffix = "_" + std::to_string(i);
        s = s.substr(0, avg_str_len - suffix.size()) + suffix;
        dict_strings.push_back(s);
    }

    // Calculate total dict data size (4-byte length prefix + string data)
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

// ============================================================================
// Helper: Build dictionary data buffer for FixLengthDictDecoder<INT32>
// ============================================================================
static std::tuple<DorisUniqueBufferPtr<uint8_t>, int32_t, size_t> build_int32_dict(int dict_size) {
    auto dict_data = make_unique_buffer<uint8_t>(dict_size * sizeof(int32_t));
    auto* ptr = reinterpret_cast<int32_t*>(dict_data.get());
    for (int i = 0; i < dict_size; ++i) {
        ptr[i] = i * 7 + 13; // Arbitrary distinct values
    }
    return {std::move(dict_data), static_cast<int32_t>(dict_size * sizeof(int32_t)),
            static_cast<size_t>(dict_size)};
}

// ============================================================================
// Helper: Build RLE-encoded dict index data
// ============================================================================
// Generates RLE-encoded data for num_values dict indexes in [0, dict_size).
// The first byte is the bit_width, followed by the RLE-encoded data.
// Returns a vector<uint8_t> that can be used as the data slice.
static std::vector<uint8_t> build_rle_dict_indexes(int num_values, int dict_size,
                                                   unsigned seed = 123) {
    std::mt19937 rng(seed);
    int bit_width = 0;
    int tmp = dict_size - 1;
    while (tmp > 0) {
        bit_width++;
        tmp >>= 1;
    }
    if (bit_width == 0) bit_width = 1;

    // Use RleEncoder to generate proper RLE data
    faststring buffer;
    RleEncoder<uint32_t> encoder(&buffer, bit_width);
    for (int i = 0; i < num_values; ++i) {
        encoder.Put(rng() % dict_size);
    }
    encoder.Flush();

    // Build the final data: [bit_width_byte] [rle_data...]
    std::vector<uint8_t> result;
    result.reserve(1 + buffer.size());
    result.push_back(static_cast<uint8_t>(bit_width));
    result.insert(result.end(), buffer.data(), buffer.data() + buffer.size());
    return result;
}

// ============================================================================
// Helper: Build run_length_null_map for ColumnSelectVector
// ============================================================================
// The map uses uint16_t entries in alternating pattern: [content, null, content, null, ...]
// Since uint16_t max is 65535, we need to split large num_values into multiple chunks.
// For benchmarks we have no nulls, so we use [chunk, 0, chunk, 0, ...] pattern.
static std::vector<uint16_t> build_run_length_null_map(int num_values) {
    std::vector<uint16_t> result;
    int remaining = num_values;
    while (remaining > 0) {
        uint16_t chunk = static_cast<uint16_t>(std::min(remaining, 65535));
        if (!result.empty()) {
            // Need a 0-length null entry before the next content entry
            result.push_back(0);
        }
        result.push_back(chunk);
        remaining -= chunk;
    }
    return result;
}

// ============================================================================
// Helper: Build filter bitmap with given selectivity
// ============================================================================
static std::vector<uint8_t> build_filter_bitmap(int num_values, double selectivity,
                                                unsigned seed = 456) {
    std::mt19937 rng(seed);
    std::vector<uint8_t> filter(num_values);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (int i = 0; i < num_values; ++i) {
        filter[i] = dist(rng) < selectivity ? 1 : 0;
    }
    return filter;
}

// ============================================================================
// ByteArrayDictDecoder Benchmark: No Filter vs With Filter
// ============================================================================
// Args: (dict_size, selectivity_percent, num_values_k)
// selectivity_percent: e.g. 5 means 5% rows survive
// num_values_k: number of values in thousands

static void BM_ByteArrayDictDecode_NoFilter(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;
    int avg_str_len = 32;

    // Setup decoder and dict
    auto [dict_buf, dict_len, dict_count] = build_string_dict(dict_size, avg_str_len);
    auto rle_data = build_rle_dict_indexes(num_values, dict_size);

    // Build filter map (selectivity-based)
    auto filter_bitmap = build_filter_bitmap(num_values, selectivity);

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

        // Init ColumnSelectVector with filter
        auto run_length_null_map = build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        // Decode WITHOUT filter_data pushdown (original path)
        auto status = decoder.decode_values(column, data_type, select_vector, false, nullptr);
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values));
}

static void BM_ByteArrayDictDecode_WithFilter(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;
    int avg_str_len = 32;

    // Setup decoder and dict
    auto [dict_buf, dict_len, dict_count] = build_string_dict(dict_size, avg_str_len);
    auto rle_data = build_rle_dict_indexes(num_values, dict_size);

    // Build filter map (selectivity-based)
    auto filter_bitmap = build_filter_bitmap(num_values, selectivity);

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

        // Init ColumnSelectVector with filter
        auto run_length_null_map = build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        // Decode WITH filter_data pushdown (optimized path)
        auto status = decoder.decode_values(column, data_type, select_vector, false,
                                            filter_bitmap.data());
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values));
}

// ============================================================================
// FixLengthDictDecoder<INT32> Benchmark: No Filter vs With Filter
// ============================================================================

static void BM_FixLenDictDecode_NoFilter(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;

    auto [dict_buf, dict_len, dict_count] = build_int32_dict(dict_size);
    auto rle_data = build_rle_dict_indexes(num_values, dict_size);
    auto filter_bitmap = build_filter_bitmap(num_values, selectivity);

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

        auto run_length_null_map = build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        auto status = decoder.decode_values(column, data_type, select_vector, false, nullptr);
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values));
}

static void BM_FixLenDictDecode_WithFilter(benchmark::State& state) {
    int dict_size = static_cast<int>(state.range(0));
    double selectivity = state.range(1) / 100.0;
    int num_values = static_cast<int>(state.range(2)) * 1000;

    auto [dict_buf, dict_len, dict_count] = build_int32_dict(dict_size);
    auto rle_data = build_rle_dict_indexes(num_values, dict_size);
    auto filter_bitmap = build_filter_bitmap(num_values, selectivity);

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

        auto run_length_null_map = build_run_length_null_map(num_values);
        FilterMap filter_map;
        static_cast<void>(filter_map.init(filter_bitmap.data(), filter_bitmap.size(), false));
        ColumnSelectVector select_vector;
        static_cast<void>(
                select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0));
        state.ResumeTiming();

        auto status = decoder.decode_values(column, data_type, select_vector, false,
                                            filter_bitmap.data());
        benchmark::DoNotOptimize(column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
    state.SetLabel("dict=" + std::to_string(dict_size) + " sel=" + std::to_string(state.range(1)) +
                   "%" + " rows=" + std::to_string(num_values));
}

// ============================================================================
// RleBatchDecoder SkipBatch Benchmark: Old (GetBatch+discard) vs New (SkipBatch)
// ============================================================================

static void BM_RleSkip_GetBatch(benchmark::State& state) {
    int num_values = static_cast<int>(state.range(0)) * 1000;
    int dict_size = 1000;

    auto rle_data = build_rle_dict_indexes(num_values, dict_size);
    uint8_t bit_width = rle_data[0];

    for (auto _ : state) {
        state.PauseTiming();
        RleBatchDecoder<uint32_t> decoder(rle_data.data() + 1,
                                          static_cast<int>(rle_data.size()) - 1, bit_width);
        // Old approach: allocate buffer + GetBatch then discard
        std::vector<uint32_t> discard_buf(num_values);
        state.ResumeTiming();

        decoder.GetBatch(discard_buf.data(), num_values);
        benchmark::DoNotOptimize(discard_buf);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
}

static void BM_RleSkip_SkipBatch(benchmark::State& state) {
    int num_values = static_cast<int>(state.range(0)) * 1000;
    int dict_size = 1000;

    auto rle_data = build_rle_dict_indexes(num_values, dict_size);
    uint8_t bit_width = rle_data[0];

    for (auto _ : state) {
        state.PauseTiming();
        RleBatchDecoder<uint32_t> decoder(rle_data.data() + 1,
                                          static_cast<int>(rle_data.size()) - 1, bit_width);
        state.ResumeTiming();

        decoder.SkipBatch(num_values);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_values);
}

// ============================================================================
// Benchmark Registrations
// ============================================================================

// --- ByteArrayDictDecoder ---
// Args: (dict_size, selectivity_percent, num_values_in_thousands)

// Small dict (fits in L2 cache), various selectivities
BENCHMARK(BM_ByteArrayDictDecode_NoFilter)
        ->Args({100, 5, 100})
        ->Args({100, 20, 100})
        ->Args({100, 50, 100})
        ->Args({100, 100, 100})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_ByteArrayDictDecode_WithFilter)
        ->Args({100, 5, 100})
        ->Args({100, 20, 100})
        ->Args({100, 50, 100})
        ->Args({100, 100, 100})
        ->Unit(benchmark::kMicrosecond);

// Large dict (exceeds L2 cache), various selectivities
// 100K entries × 32 bytes ≈ 3.2MB > typical L2 cache (256KB-1MB)
BENCHMARK(BM_ByteArrayDictDecode_NoFilter)
        ->Args({100000, 1, 100})
        ->Args({100000, 5, 100})
        ->Args({100000, 20, 100})
        ->Args({100000, 50, 100})
        ->Args({100000, 100, 100})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_ByteArrayDictDecode_WithFilter)
        ->Args({100000, 1, 100})
        ->Args({100000, 5, 100})
        ->Args({100000, 20, 100})
        ->Args({100000, 50, 100})
        ->Args({100000, 100, 100})
        ->Unit(benchmark::kMicrosecond);

// Medium dict (borderline L2 cache)
BENCHMARK(BM_ByteArrayDictDecode_NoFilter)
        ->Args({10000, 5, 100})
        ->Args({10000, 20, 100})
        ->Args({10000, 50, 100})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_ByteArrayDictDecode_WithFilter)
        ->Args({10000, 5, 100})
        ->Args({10000, 20, 100})
        ->Args({10000, 50, 100})
        ->Unit(benchmark::kMicrosecond);

// --- FixLengthDictDecoder<INT32> ---

// Small dict
BENCHMARK(BM_FixLenDictDecode_NoFilter)
        ->Args({100, 5, 100})
        ->Args({100, 20, 100})
        ->Args({100, 50, 100})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_FixLenDictDecode_WithFilter)
        ->Args({100, 5, 100})
        ->Args({100, 20, 100})
        ->Args({100, 50, 100})
        ->Unit(benchmark::kMicrosecond);

// Large dict (exceeds L2 cache)
// 100K entries × 4 bytes = 400KB (still might fit in L2 for large caches)
// Use 1M entries for guaranteed L2 miss: 1M × 4 bytes = 4MB
BENCHMARK(BM_FixLenDictDecode_NoFilter)
        ->Args({1000000, 5, 100})
        ->Args({1000000, 20, 100})
        ->Args({1000000, 50, 100})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_FixLenDictDecode_WithFilter)
        ->Args({1000000, 5, 100})
        ->Args({1000000, 20, 100})
        ->Args({1000000, 50, 100})
        ->Unit(benchmark::kMicrosecond);

// --- RLE SkipBatch ---
BENCHMARK(BM_RleSkip_GetBatch)
        ->Args({10})
        ->Args({100})
        ->Args({1000})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_RleSkip_SkipBatch)
        ->Args({10})
        ->Args({100})
        ->Args({1000})
        ->Unit(benchmark::kMicrosecond);

} // namespace doris::vectorized
