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

#include <array>
#include <string_view>

#include "core/types.h"
#include "core/value/timestamptz_value.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "exprs/function/cast/cast_to_datev2_impl.hpp"
#include "util/timezone_utils.h"

namespace doris {

namespace {

inline const cctz::time_zone& benchmark_local_tz() {
    static const cctz::time_zone tz = [] {
        TimezoneUtils::load_timezones_to_cache();
        TimezoneUtils::load_offsets_to_cache();
        cctz::time_zone local_tz;
        bool found = TimezoneUtils::find_cctz_time_zone("+08:00", local_tz);
        DCHECK(found);
        return local_tz;
    }();
    return tz;
}

inline const std::array<std::string_view, 4>& date_fast_hit_inputs() {
    static const std::array<std::string_view, 4> values = {
            "2024-01-02", "2023-12-31", "1997-11-18", "2000-02-29"};
    return values;
}

inline const std::array<std::string_view, 4>& date_fast_miss_inputs() {
    static const std::array<std::string_view, 4> values = {
            "2024-1-2", "2024/01/02", "24-01-02", "20240102"};
    return values;
}

inline const std::array<std::string_view, 4>& datetime_fast_hit_inputs() {
    static const std::array<std::string_view, 4> values = {
            "2024-01-02 03:04:05",
            "2023-12-31 23:59:59",
            "1997-11-18 09:12:46",
            "2000-02-29 12:34:56"};
    return values;
}

inline const std::array<std::string_view, 4>& datetime_fast_hit_suffix_inputs() {
    static const std::array<std::string_view, 4> values = {
            "2024-01-02 03:04:05.123456",
            "2024-01-02 03:04:05 +08:00",
            "2024-01-02 03:04:05.123456 +08:00",
            "2023-12-31 23:59:59.999999 +08:00"};
    return values;
}

inline const std::array<std::string_view, 4>& datetime_fast_miss_inputs() {
    static const std::array<std::string_view, 4> values = {
            "2024-1-2 03:04:05",
            "2024/01/02 03:04:05",
            "24-01-02 03:04:05",
            "20240102030405"};
    return values;
}

template <typename T, typename Inputs, typename ParseFn>
void run_parse_benchmark(benchmark::State& state, const Inputs& inputs, ParseFn&& parse_fn) {
    size_t index = 0;
    for (auto _ : state) {
        T value;
        CastParameters params;
        params.is_strict = true;
        const auto& input = inputs[index++ % inputs.size()];
        StringRef ref {input.data(), input.size()};
        bool ok = parse_fn(ref, value, params);
        benchmark::DoNotOptimize(ref);
        DCHECK(ok) << "benchmark input should parse successfully: " << input;
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(value);
    }
    state.counters["items_per_second"] =
            benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);
}

void bm_parse_date_v2_hit(benchmark::State& state) {
    run_parse_benchmark<DateV2Value<DateV2ValueType>>(
            state, date_fast_hit_inputs(), [](const StringRef& ref, auto& value, auto& params) {
                return CastToDateV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                        ref, value, nullptr, params);
            });
}

void bm_parse_date_v2_miss(benchmark::State& state) {
    run_parse_benchmark<DateV2Value<DateV2ValueType>>(
            state, date_fast_miss_inputs(), [](const StringRef& ref, auto& value, auto& params) {
                return CastToDateV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                        ref, value, nullptr, params);
            });
}

void bm_parse_date_v2_hit_suffix(benchmark::State& state) {
    run_parse_benchmark<DateV2Value<DateV2ValueType>>(
            state, datetime_fast_hit_suffix_inputs(), [](const StringRef& ref, auto& value,
                                                         auto& params) {
                return CastToDateV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                        ref, value, nullptr, params);
            });
}

void bm_parse_datetime_v2_hit(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<DateV2Value<DateTimeV2ValueType>>(
            state, datetime_fast_hit_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                return CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                        ref, value, &local_tz, 6, params);
            });
}

void bm_parse_datetime_v2_miss(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<DateV2Value<DateTimeV2ValueType>>(
            state, datetime_fast_miss_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                return CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                        ref, value, &local_tz, 6, params);
            });
}

void bm_parse_datetime_v2_hit_suffix(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<DateV2Value<DateTimeV2ValueType>>(
            state, datetime_fast_hit_suffix_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                return CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                        ref, value, &local_tz, 6, params);
            });
}

void bm_parse_datetime_hit(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<VecDateTimeValue>(
            state, datetime_fast_hit_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                value.set_type(TIME_DATETIME);
                return CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                                     DatelikeTargetType::DATE_TIME>(
                        ref, value, &local_tz, params);
            });
}

void bm_parse_datetime_miss(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<VecDateTimeValue>(
            state, datetime_fast_miss_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                value.set_type(TIME_DATETIME);
                return CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                                     DatelikeTargetType::DATE_TIME>(
                        ref, value, &local_tz, params);
            });
}

void bm_parse_datetime_hit_suffix(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<VecDateTimeValue>(
            state, datetime_fast_hit_suffix_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                value.set_type(TIME_DATETIME);
                return CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                                     DatelikeTargetType::DATE_TIME>(
                        ref, value, &local_tz, params);
            });
}

void bm_parse_date_hit(benchmark::State& state) {
    run_parse_benchmark<VecDateTimeValue>(
            state, date_fast_hit_inputs(), [](const StringRef& ref, auto& value, auto& params) {
                value.set_type(TIME_DATE);
                return CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                                     DatelikeTargetType::DATE>(
                        ref, value, nullptr, params);
            });
}

void bm_parse_date_miss(benchmark::State& state) {
    run_parse_benchmark<VecDateTimeValue>(
            state, date_fast_miss_inputs(), [](const StringRef& ref, auto& value, auto& params) {
                value.set_type(TIME_DATE);
                return CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                                     DatelikeTargetType::DATE>(
                        ref, value, nullptr, params);
            });
}

void bm_parse_date_hit_suffix(benchmark::State& state) {
    run_parse_benchmark<VecDateTimeValue>(
            state, datetime_fast_hit_suffix_inputs(), [](const StringRef& ref, auto& value,
                                                         auto& params) {
                value.set_type(TIME_DATE);
                return CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                                     DatelikeTargetType::DATE>(
                        ref, value, nullptr, params);
            });
}

void bm_parse_timestamptz_hit(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<TimestampTzValue>(
            state, datetime_fast_hit_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                return value.from_string(ref, &local_tz, params, 6);
            });
}

void bm_parse_timestamptz_miss(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<TimestampTzValue>(
            state, datetime_fast_miss_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                return value.from_string(ref, &local_tz, params, 6);
            });
}

void bm_parse_timestamptz_hit_suffix(benchmark::State& state) {
    const auto& local_tz = benchmark_local_tz();
    run_parse_benchmark<TimestampTzValue>(
            state, datetime_fast_hit_suffix_inputs(),
            [&](const StringRef& ref, auto& value, auto& params) {
                return value.from_string(ref, &local_tz, params, 6);
            });
}

BENCHMARK(bm_parse_date_hit)->Name("parse_date/hit")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_date_hit_suffix)->Name("parse_date/hit_suffix")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_date_miss)->Name("parse_date/miss")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_date_v2_hit)->Name("parse_datev2/hit")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_date_v2_hit_suffix)
        ->Name("parse_datev2/hit_suffix")
        ->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_date_v2_miss)->Name("parse_datev2/miss")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_datetime_hit)->Name("parse_datetime/hit")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_datetime_hit_suffix)
        ->Name("parse_datetime/hit_suffix")
        ->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_datetime_miss)->Name("parse_datetime/miss")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_datetime_v2_hit)->Name("parse_datetimev2/hit")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_datetime_v2_hit_suffix)
        ->Name("parse_datetimev2/hit_suffix")
        ->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_datetime_v2_miss)->Name("parse_datetimev2/miss")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_timestamptz_hit)->Name("parse_timestamptz/hit")->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_timestamptz_hit_suffix)
        ->Name("parse_timestamptz/hit_suffix")
        ->Unit(benchmark::kNanosecond);
BENCHMARK(bm_parse_timestamptz_miss)->Name("parse_timestamptz/miss")->Unit(benchmark::kNanosecond);

} // namespace

} // namespace doris
