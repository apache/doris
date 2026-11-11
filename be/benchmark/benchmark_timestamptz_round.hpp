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

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamptz.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {
namespace {

// Exercise the registered BE function on a nonconstant column, including result
// allocation. Preparation and function open/close are outside the timed loop.
template <int Arguments>
void BM_timestamptz_year_floor(benchmark::State& state) {
    const size_t rows = state.range(0);
    const int period = static_cast<int>(state.range(1));
    const char* zone = state.range(2) == 0 ? "America/New_York" : "UTC";
    TimezoneUtils::load_timezones_to_cache();
    RuntimeState runtime_state;
    runtime_state.set_timezone(zone);
    const auto& timezone = runtime_state.timezone_obj();
    const auto type = std::make_shared<DataTypeTimeStampTz>(6);
    const auto int_type = std::make_shared<DataTypeInt32>();
    auto input = ColumnTimeStampTz::create();
    for (size_t i = 0; i < rows; ++i) {
        const auto local = cctz::civil_second(2024, 7, 1, 12, 34, 56) + i % 3600;
        const auto utc = cctz::convert(TimezoneUtils::convert_local_to_instant(local, timezone),
                                       cctz::utc_time_zone());
        TimestampTzValue value;
        value.unchecked_set_time(
                static_cast<uint16_t>(utc.year()), static_cast<uint8_t>(utc.month()),
                static_cast<uint8_t>(utc.day()), static_cast<uint8_t>(utc.hour()),
                static_cast<uint8_t>(utc.minute()), static_cast<uint8_t>(utc.second()),
                static_cast<uint32_t>(i % 1000000));
        input->insert_value(value);
    }
    Block block {{std::move(input), type, "input"}};
    DataTypes argument_types {type};
    ColumnNumbers arguments {0};
    if constexpr (Arguments >= 2) {
        auto periods = ColumnInt32::create();
        periods->insert_value(period);
        block.insert({ColumnConst::create(std::move(periods), rows), int_type, "period"});
        argument_types.push_back(int_type);
        arguments.push_back(1);
    }
    if constexpr (Arguments == 3) {
        TimestampTzValue origin;
        origin.unchecked_set_time(1900, 1, 1, 5, 0, 0, 123456);
        auto origins = ColumnTimeStampTz::create();
        origins->insert_value(origin);
        block.insert({ColumnConst::create(std::move(origins), rows), type, "origin"});
        argument_types.push_back(type);
        arguments.push_back(2);
    }
    auto function = SimpleFunctionFactory::instance().get_function(
            "year_floor", block.get_columns_with_type_and_name(), type);
    DORIS_CHECK(function != nullptr);
    auto context = FunctionContext::create_context(&runtime_state, type, argument_types);
    DORIS_CHECK(function->open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    DORIS_CHECK(
            function->open(context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    block.insert({nullptr, type, "result"});
    for (auto _ : state) {
        auto status = function->execute(context.get(), block, arguments, Arguments, rows);
        if (!status.ok()) {
            state.SkipWithError(status.to_string());
            break;
        }
        benchmark::DoNotOptimize(block.get_by_position(Arguments).column);
        benchmark::ClobberMemory();
    }
    DORIS_CHECK(
            function->close(context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    DORIS_CHECK(function->close(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    state.SetItemsProcessed(state.iterations() * rows);
    state.SetLabel(zone);
}

BENCHMARK_TEMPLATE(BM_timestamptz_year_floor, 1)->Args({4096, 1, 0})->Args({4096, 1, 1});
BENCHMARK_TEMPLATE(BM_timestamptz_year_floor, 2)
        ->ArgsProduct({{1, 512, 4096}, {1, 100, 10000}, {0}})
        ->Args({4096, 10000, 1});
BENCHMARK_TEMPLATE(BM_timestamptz_year_floor, 3)->ArgsProduct({{4096}, {1, 100, 10000}, {0}});

} // namespace
} // namespace doris
