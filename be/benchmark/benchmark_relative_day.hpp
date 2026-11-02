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

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"

namespace doris {
namespace {

// Measure the registered SQL function, including output allocation and date arithmetic.
// Scenario 0 mixes all aliases, 1 uses a constant weekday, and 2 adds nullable dates.
template <bool Previous, int Scenario>
void BM_relative_day(benchmark::State& state) {
    constexpr std::array<std::string_view, 21> weekdays = {
            "MO",     "mon",       "MoNdAy", "TU",       "tue",      "TuEsDaY", "WE",
            "wed",    "WeDnEsDaY", "TH",     "thu",      "ThUrSdAy", "FR",      "fri",
            "FrIdAy", "SA",        "sat",    "SaTuRdAy", "SU",       "sun",     "SuNdAy"};
    const size_t rows = state.range(0);
    auto dates = ColumnDateV2::create();
    auto names = ColumnString::create();
    auto nulls = ColumnUInt8::create(rows, 0);
    DateV2Value<DateV2ValueType> date;
    date.unchecked_set_time(2024, 1, 1, 0, 0, 0);
    for (size_t i = 0; i < rows; ++i) {
        dates->insert_value(date);
        const auto weekday = weekdays[(i * 13) % weekdays.size()];
        names->insert_data(weekday.data(), weekday.size());
        if constexpr (Scenario == 2) {
            nulls->get_data()[i] = i % 4 == 0;
        }
    }
    DataTypePtr date_type = std::make_shared<DataTypeDateV2>();
    ColumnPtr date_column = std::move(dates);
    if constexpr (Scenario == 2) {
        date_column = ColumnNullable::create(date_column, std::move(nulls));
        date_type = make_nullable(date_type);
    }
    ColumnPtr name_column = std::move(names);
    if constexpr (Scenario == 1) {
        auto constant_name = ColumnString::create();
        constant_name->insert_data("WeDnEsDaY", 9);
        name_column = ColumnConst::create(std::move(constant_name), rows);
    }
    auto string_type = std::make_shared<DataTypeString>();
    Block block {{std::move(date_column), date_type, "date"},
                 {std::move(name_column), string_type, "weekday"}};
    auto function = SimpleFunctionFactory::instance().get_function(
            Previous ? "previous_day" : "next_day", block.get_columns_with_type_and_name(),
            date_type);
    DORIS_CHECK(function != nullptr);
    auto context = FunctionContext::create_context(nullptr, date_type, {date_type, string_type});
    block.insert({nullptr, date_type, "result"});
    auto status = function->execute(context.get(), block, {0, 1}, 2, rows);
    if (!status.ok()) {
        state.SkipWithError(status.to_string());
        return;
    }
    const auto result = ColumnView<TYPE_DATEV2>::create(block.get_by_position(2).column);
    for (size_t i = 0; i < rows; ++i) {
        const bool is_null = Scenario == 2 && i % 4 == 0;
        DORIS_CHECK_EQ(result.is_null_at(i), is_null);
        if (is_null) {
            continue;
        }
        const size_t weekday = Scenario == 1 ? 2 : ((i * 13) % weekdays.size()) / 3;
        DateV2Value<DateV2ValueType> expected;
        if constexpr (Previous) {
            expected.unchecked_set_time(2023, 12, static_cast<uint8_t>(25 + weekday), 0, 0, 0);
        } else {
            expected.unchecked_set_time(
                    2024, 1, static_cast<uint8_t>(weekday == 0 ? 8 : weekday + 1), 0, 0, 0);
        }
        DORIS_CHECK_EQ(result.value_at(i).to_date_int_val(), expected.to_date_int_val());
    }
    for (auto _ : state) {
        status = function->execute(context.get(), block, {0, 1}, 2, rows);
        if (!status.ok()) {
            state.SkipWithError(status.to_string());
            break;
        }
        benchmark::DoNotOptimize(block.get_by_position(2).column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows);
}

BENCHMARK_TEMPLATE(BM_relative_day, false, 0)->Name("next_day/variable")->Arg(4096);
BENCHMARK_TEMPLATE(BM_relative_day, false, 1)->Name("next_day/constant")->Arg(4096);
BENCHMARK_TEMPLATE(BM_relative_day, false, 2)->Name("next_day/nullable")->Arg(4096);
BENCHMARK_TEMPLATE(BM_relative_day, true, 0)->Name("previous_day/variable")->Arg(4096);
BENCHMARK_TEMPLATE(BM_relative_day, true, 1)->Name("previous_day/constant")->Arg(4096);
BENCHMARK_TEMPLATE(BM_relative_day, true, 2)->Name("previous_day/nullable")->Arg(4096);

} // namespace
} // namespace doris
