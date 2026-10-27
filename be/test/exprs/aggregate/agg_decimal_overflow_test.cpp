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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/column/column_decimal.h"
#include "core/data_type/data_type_decimal.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "core/arena.h"

namespace doris {

namespace {

Decimal128V3 max_decimal128(UInt32 precision) {
    return Decimal128V3(DataTypeDecimal128::get_max_digits_number(precision));
}

AggregateFunctionPtr create_sum_decimal128() {
    auto arg = std::make_shared<DataTypeDecimal128>(38, 0);
    auto result = std::make_shared<DataTypeDecimal128>(38, 0);
    return AggregateFunctionSimpleFactory::instance().get(
            "sum", {arg}, result, false, BeExecVersionManager::get_newest_version(),
            {.column_names = {}});
}

AggregateFunctionPtr create_avg_decimal128() {
    auto arg = std::make_shared<DataTypeDecimal128>(38, 0);
    // AVG widens scale to at least 4.
    auto result = std::make_shared<DataTypeDecimal128>(38, 4);
    return AggregateFunctionSimpleFactory::instance().get(
            "avg", {arg}, result, false, BeExecVersionManager::get_newest_version(),
            {.column_names = {}});
}

ColumnDecimal128V3::MutablePtr make_decimal128_column(const std::vector<Decimal128V3>& values,
                                                      UInt32 scale = 0) {
    auto column = ColumnDecimal128V3::create(0, scale);
    for (const auto& v : values) {
        column->insert_value(v);
    }
    return column;
}

} // namespace

TEST(AggDecimalOverflowTest, sum_overflow_when_check_enabled) {
    Arena arena;
    auto agg = create_sum_decimal128();
    ASSERT_NE(agg, nullptr);
    agg->set_check_overflow_for_decimal(true);

    // 10^38-1 + 1 = 10^38 still fits in int128 (~1.7e38) but exceeds Decimal(38,*) precision.
    auto max_v = max_decimal128(38);
    auto column = make_decimal128_column({max_v, Decimal128V3(1)});
    const IColumn* cols[1] = {column.get()};

    std::unique_ptr<char[]> memory(new char[agg->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg->create(place);

    EXPECT_THROW(
            {
                for (size_t i = 0; i < column->size(); ++i) {
                    agg->add(place, cols, i, arena);
                }
            },
            Exception);

    agg->destroy(place);
}

TEST(AggDecimalOverflowTest, sum_wraps_when_check_disabled) {
    Arena arena;
    auto agg = create_sum_decimal128();
    ASSERT_NE(agg, nullptr);
    agg->set_check_overflow_for_decimal(false);

    auto max_v = max_decimal128(38);
    auto column = make_decimal128_column({max_v, Decimal128V3(1)});
    const IColumn* cols[1] = {column.get()};

    std::unique_ptr<char[]> memory(new char[agg->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg->create(place);

    for (size_t i = 0; i < column->size(); ++i) {
        EXPECT_NO_THROW(agg->add(place, cols, i, arena));
    }

    auto result_col = ColumnDecimal128V3::create(0, 0);
    agg->insert_result_into(place, *result_col);
    ASSERT_EQ(result_col->size(), 1);
    // Check off: allow out-of-precision / wraparound legacy behavior (result != max).
    // Use .value: EXPECT_NE(Decimal, Decimal) does not compile under C++20/gtest.
    EXPECT_NE(result_col->get_element(0).value, max_v.value);
    agg->destroy(place);
}

TEST(AggDecimalOverflowTest, sum_merge_overflow_when_check_enabled) {
    Arena arena;
    auto agg = create_sum_decimal128();
    ASSERT_NE(agg, nullptr);
    agg->set_check_overflow_for_decimal(true);

    auto max_v = max_decimal128(38);
    auto column = make_decimal128_column({max_v});
    const IColumn* cols[1] = {column.get()};

    std::unique_ptr<char[]> mem1(new char[agg->size_of_data()]);
    std::unique_ptr<char[]> mem2(new char[agg->size_of_data()]);
    AggregateDataPtr place1 = mem1.get();
    AggregateDataPtr place2 = mem2.get();
    agg->create(place1);
    agg->create(place2);

    agg->add(place1, cols, 0, arena);
    auto one = make_decimal128_column({Decimal128V3(1)});
    const IColumn* cols_one[1] = {one.get()};
    agg->add(place2, cols_one, 0, arena);

    EXPECT_THROW(agg->merge(place1, place2, arena), Exception);

    agg->destroy(place1);
    agg->destroy(place2);
}

TEST(AggDecimalOverflowTest, sum_normal_value_ok) {
    Arena arena;
    auto agg = create_sum_decimal128();
    ASSERT_NE(agg, nullptr);
    agg->set_check_overflow_for_decimal(true);

    auto column = make_decimal128_column({Decimal128V3(10), Decimal128V3(20), Decimal128V3(30)});
    const IColumn* cols[1] = {column.get()};

    std::unique_ptr<char[]> memory(new char[agg->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg->create(place);

    for (size_t i = 0; i < column->size(); ++i) {
        EXPECT_NO_THROW(agg->add(place, cols, i, arena));
    }

    auto result_col = ColumnDecimal128V3::create(0, 0);
    agg->insert_result_into(place, *result_col);
    ASSERT_EQ(result_col->size(), 1);
    EXPECT_EQ(result_col->get_element(0), Decimal128V3(60));
    agg->destroy(place);
}

TEST(AggDecimalOverflowTest, avg_overflow_when_check_enabled) {
    Arena arena;
    auto agg = create_avg_decimal128();
    ASSERT_NE(agg, nullptr);
    agg->set_check_overflow_for_decimal(true);

    auto max_v = max_decimal128(38);
    auto column = make_decimal128_column({max_v, Decimal128V3(1)});
    const IColumn* cols[1] = {column.get()};

    std::unique_ptr<char[]> memory(new char[agg->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg->create(place);

    EXPECT_THROW(
            {
                for (size_t i = 0; i < column->size(); ++i) {
                    agg->add(place, cols, i, arena);
                }
            },
            Exception);

    agg->destroy(place);
}

TEST(AggDecimalOverflowTest, avg_finalize_scale_overflow) {
    Arena arena;
    // Input scale 0 -> AVG output scale 4, multiplier = 10000.
    // Choose sum so that sum * 10000 overflows int128.
    auto agg = create_avg_decimal128();
    ASSERT_NE(agg, nullptr);
    agg->set_check_overflow_for_decimal(true);

    // max_i128 / 10000 + 1, times 10000 overflows.
    constexpr __int128 max_int128 =
            (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
    const auto limit = max_int128 / 10000 + 1;
    auto column = make_decimal128_column({Decimal128V3(limit)});
    const IColumn* cols[1] = {column.get()};

    std::unique_ptr<char[]> memory(new char[agg->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg->create(place);

    EXPECT_NO_THROW(agg->add(place, cols, 0, arena));

    auto result_type = std::make_shared<DataTypeDecimal128>(38, 4);
    auto result_col = result_type->create_column();
    EXPECT_THROW(agg->insert_result_into(place, *result_col), Exception);

    agg->destroy(place);
}

TEST(AggDecimalOverflowTest, avg_normal_value_ok) {
    Arena arena;
    auto agg = create_avg_decimal128();
    ASSERT_NE(agg, nullptr);
    agg->set_check_overflow_for_decimal(true);

    auto column = make_decimal128_column({Decimal128V3(10), Decimal128V3(20), Decimal128V3(30)});
    const IColumn* cols[1] = {column.get()};

    std::unique_ptr<char[]> memory(new char[agg->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg->create(place);

    for (size_t i = 0; i < column->size(); ++i) {
        EXPECT_NO_THROW(agg->add(place, cols, i, arena));
    }

    auto result_type = std::make_shared<DataTypeDecimal128>(38, 4);
    auto result_col = result_type->create_column();
    EXPECT_NO_THROW(agg->insert_result_into(place, *result_col));
    ASSERT_EQ(result_col->size(), 1);
    // avg = 20, with scale 4 -> 200000
    EXPECT_EQ(assert_cast<ColumnDecimal128V3&>(*result_col).get_element(0), Decimal128V3(200000));
    agg->destroy(place);
}

} // namespace doris
