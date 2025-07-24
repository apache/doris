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

#include <cstring>
#include <random>
#include <vector>

#include "util/binary_cast.hpp"

namespace doris {

// similar to reinterpret_cast but won't break strict-aliasing rules
template <typename From, typename To>
To old_binary_cast(From from) {
    constexpr bool from_u64_to_db = match_v<From, uint64_t, To, double>;
    constexpr bool from_i64_to_db = match_v<From, int64_t, To, double>;
    constexpr bool from_db_to_i64 = match_v<From, double, To, int64_t>;
    constexpr bool from_db_to_u64 = match_v<From, double, To, uint64_t>;
    constexpr bool from_i64_to_vec_dt = match_v<From, __int64_t, To, doris::VecDateTimeValue>;
    constexpr bool from_vec_dt_to_i64 = match_v<From, doris::VecDateTimeValue, To, __int64_t>;
    constexpr bool from_i128_to_decv2 = match_v<From, __int128_t, To, DecimalV2Value>;
    constexpr bool from_decv2_to_i128 = match_v<From, DecimalV2Value, To, __int128_t>;
    constexpr bool from_decv2_to_i256 = match_v<From, DecimalV2Value, To, wide::Int256>;

    constexpr bool from_ui32_to_date_v2 = match_v<From, uint32_t, To, DateV2Value<DateV2ValueType>>;

    constexpr bool from_date_v2_to_ui32 = match_v<From, DateV2Value<DateV2ValueType>, To, uint32_t>;

    constexpr bool from_ui64_to_datetime_v2 =
            match_v<From, uint64_t, To, DateV2Value<DateTimeV2ValueType>>;

    constexpr bool from_datetime_v2_to_ui64 =
            match_v<From, DateV2Value<DateTimeV2ValueType>, To, uint64_t>;

    static_assert(from_u64_to_db || from_i64_to_db || from_db_to_i64 || from_db_to_u64 ||
                  from_i64_to_vec_dt || from_vec_dt_to_i64 || from_i128_to_decv2 ||
                  from_decv2_to_i128 || from_decv2_to_i256 || from_ui32_to_date_v2 ||
                  from_date_v2_to_ui32 || from_ui64_to_datetime_v2 || from_datetime_v2_to_ui64);

    if constexpr (from_u64_to_db) {
        TypeConverter conv;
        conv.u64 = from;
        return conv.dbl;
    } else if constexpr (from_i64_to_db) {
        TypeConverter conv;
        conv.i64 = from;
        return conv.dbl;
    } else if constexpr (from_db_to_i64) {
        TypeConverter conv;
        conv.dbl = from;
        return conv.i64;
    } else if constexpr (from_db_to_u64) {
        TypeConverter conv;
        conv.dbl = from;
        return conv.u64;
    } else if constexpr (from_i64_to_vec_dt) {
        VecDateTimeInt64Union conv = {.i64 = from};
        return conv.dt;
    } else if constexpr (from_ui32_to_date_v2) {
        DateV2UInt32Union conv = {.ui32 = from};
        return conv.dt;
    } else if constexpr (from_date_v2_to_ui32) {
        DateV2UInt32Union conv = {.dt = from};
        return conv.ui32;
    } else if constexpr (from_ui64_to_datetime_v2) {
        DateTimeV2UInt64Union conv = {.ui64 = from};
        return conv.dt;
    } else if constexpr (from_datetime_v2_to_ui64) {
        DateTimeV2UInt64Union conv = {.dt = from};
        return conv.ui64;
    } else if constexpr (from_vec_dt_to_i64) {
        VecDateTimeInt64Union conv = {.dt = from};
        return conv.i64;
    } else if constexpr (from_i128_to_decv2) {
        DecimalInt128Union conv;
        conv.i128 = from;
        return conv.decimal;
    } else if constexpr (from_decv2_to_i128) {
        DecimalInt128Union conv;
        conv.decimal = from;
        return conv.i128;
    } else {
        throw Exception(Status::FatalError("__builtin_unreachable"));
    }
}

// Generate random datetime values in uint64_t format for testing
std::vector<uint64_t> generate_datetime_v2_ui64_data(size_t count) {
    std::vector<uint64_t> data;
    data.reserve(count);

    static std::random_device rd;
    static std::mt19937_64 gen(rd());

    std::uniform_int_distribution<uint64_t> dis(MIN_DATETIME_V2, MAX_DATETIME_V2);

    for (size_t i = 0; i < count; ++i) {
        data.push_back(dis(gen));
    }

    return data;
}

// Generate DateTimeV2Value objects from uint64_t data
std::vector<DateV2Value<DateTimeV2ValueType>> convert_u_int64_to_date_time_v2(
        const std::vector<uint64_t>& ui64_data) {
    std::vector<DateV2Value<DateTimeV2ValueType>> result;
    result.reserve(ui64_data.size());

    for (const auto& ui64_val : ui64_data) {
        result.push_back(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(ui64_val));
    }

    return result;
}

// Benchmark binary_cast from uint64_t to DateTimeV2Value
static void BM_BinaryCast_UI64_to_DateTimeV2(benchmark::State& state) {
    state.PauseTiming();
    const size_t data_size = state.range(0);
    auto test_data = generate_datetime_v2_ui64_data(data_size);
    state.ResumeTiming();

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());

        for (size_t i = 0; i < data_size; ++i) {
            auto result = binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(test_data[i]);
            benchmark::DoNotOptimize(result);
        }

        benchmark::ClobberMemory();
    }

    // Set the number of items processed per second
    state.SetItemsProcessed(int64_t(state.iterations()) * data_size);
}

// Benchmark old_binary_cast from uint64_t to DateTimeV2Value
static void BM_OldBinaryCast_UI64_to_DateTimeV2(benchmark::State& state) {
    state.PauseTiming();
    const size_t data_size = state.range(0);
    auto test_data = generate_datetime_v2_ui64_data(data_size);
    state.ResumeTiming();

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());

        for (size_t i = 0; i < data_size; ++i) {
            auto result = old_binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(test_data[i]);
            benchmark::DoNotOptimize(result);
        }

        benchmark::ClobberMemory();
    }

    // Set the number of items processed per second
    state.SetItemsProcessed(int64_t(state.iterations()) * data_size);
}

// Benchmark binary_cast from DateTimeV2Value to uint64_t
static void BM_BinaryCast_DateTimeV2_to_UI64(benchmark::State& state) {
    state.PauseTiming();
    const size_t data_size = state.range(0);
    auto ui64_data = generate_datetime_v2_ui64_data(data_size);
    auto test_data = convert_u_int64_to_date_time_v2(ui64_data);
    state.ResumeTiming();

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());

        for (size_t i = 0; i < data_size; ++i) {
            auto result = binary_cast<DateV2Value<DateTimeV2ValueType>, uint64_t>(test_data[i]);
            benchmark::DoNotOptimize(result);
        }

        benchmark::ClobberMemory();
    }

    // Set the number of items processed per second
    state.SetItemsProcessed(int64_t(state.iterations()) * data_size);
}

// Benchmark old_binary_cast from DateTimeV2Value to uint64_t
static void BM_OldBinaryCast_DateTimeV2_to_UI64(benchmark::State& state) {
    state.PauseTiming();
    const size_t data_size = state.range(0);
    auto ui64_data = generate_datetime_v2_ui64_data(data_size);
    auto test_data = convert_u_int64_to_date_time_v2(ui64_data);
    state.ResumeTiming();

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());

        for (size_t i = 0; i < data_size; ++i) {
            auto result = old_binary_cast<DateV2Value<DateTimeV2ValueType>, uint64_t>(test_data[i]);
            benchmark::DoNotOptimize(result);
        }

        benchmark::ClobberMemory();
    }

    // Set the number of items processed per second
    state.SetItemsProcessed(int64_t(state.iterations()) * data_size);
}

// Register benchmarks with only large data sizes
// Use fixed larger sizes and more iterations for more reliable comparisons
BENCHMARK(BM_BinaryCast_UI64_to_DateTimeV2)
        ->Arg(1000000)
        ->Arg(10000000)
        ->Iterations(100)
        ->MinTime(2.0) // Run each benchmark for at least 2 seconds
        ->Unit(benchmark::
                       kMicrosecond); // Use microseconds for more readable results with large datasets

BENCHMARK(BM_OldBinaryCast_UI64_to_DateTimeV2)
        ->Arg(1000000)
        ->Arg(10000000)
        ->Iterations(100)
        ->MinTime(2.0)
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BinaryCast_DateTimeV2_to_UI64)
        ->Arg(1000000)
        ->Arg(10000000)
        ->Iterations(100)
        ->MinTime(2.0)
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_OldBinaryCast_DateTimeV2_to_UI64)
        ->Arg(1000000)
        ->Arg(10000000)
        ->Iterations(100)
        ->MinTime(2.0)
        ->Unit(benchmark::kMicrosecond);

} // namespace doris
