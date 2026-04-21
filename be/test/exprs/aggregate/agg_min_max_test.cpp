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

#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "gtest/gtest_pred_impl.h"

const int agg_test_batch_size = 4096;

namespace doris {
// declare function
void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory);

class AggMinMaxTest : public ::testing::TestWithParam<std::string> {};

TEST_P(AggMinMaxTest, min_max_test) {
    Arena arena;
    std::string min_max_type = GetParam();
    // Prepare test data.
    auto column_vector_int32 = ColumnInt32::create();
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_int32->insert(Field::create_field<TYPE_INT>(i));
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {std::make_shared<DataTypeInt32>()};
    auto agg_function = factory.get(min_max_type, data_types, nullptr, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_vector_int32.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnInt32 ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(min_max_type == "min" ? 0 : agg_test_batch_size - 1, ans.get_element(0));
    agg_function->destroy(place);
}

TEST_P(AggMinMaxTest, min_max_decimal_test) {
    Arena arena;
    std::string min_max_type = GetParam();
    auto data_type = std::make_shared<DataTypeDecimalV2>();
    // Prepare test data.
    auto column_vector_decimal128 = data_type->create_column();
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_decimal128->insert(Field::create_field<TYPE_DECIMALV2>(DecimalV2Value(i)));
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(min_max_type, data_types, nullptr, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_vector_decimal128.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnDecimal128V2 ans(0, 9);
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(min_max_type == "min" ? 0 : agg_test_batch_size - 1, ans.get_element(0).value());
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    agg_function->streaming_agg_serialize_to_column(column, dst, agg_test_batch_size, arena);

    std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data() * agg_test_batch_size]);
    std::unique_ptr<char[]> memory2_tmp(
            new char[agg_function->size_of_data() * agg_test_batch_size]);
    std::vector<AggregateDataPtr> places(agg_test_batch_size);
    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        places[i] = memory2.get() + agg_function->size_of_data() * i;
        agg_function->create(places[i]);
    }
    agg_function->deserialize_and_merge_vec(places.data(), 0, memory2_tmp.get(), dst.get(), arena,
                                            agg_test_batch_size);

    ColumnDecimal128V2 result(0, 9);
    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        agg_function->insert_result_into(places[i], result);
    }

    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        EXPECT_EQ(i, result.get_element(i).value());
    }
}

TEST_P(AggMinMaxTest, min_max_string_test) {
    Arena arena;
    std::string min_max_type = GetParam();
    // Prepare test data.
    auto column_vector_str = ColumnString::create();
    std::vector<std::string> str_data = {"", "xyz", "z", "zzz", "abc", "foo", "bar"};
    for (const auto& s : str_data) {
        column_vector_str->insert_data(s.c_str(), s.length());
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {std::make_shared<DataTypeString>()};
    auto agg_function = factory.get(min_max_type, data_types, nullptr, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_vector_str.get()};
    for (int i = 0; i < str_data.size(); i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnString ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(min_max_type == "min" ? StringRef("") : StringRef("zzz"), ans.get_data_at(0));
    agg_function->destroy(place);
}

TEST_P(AggMinMaxTest, any_json_test) {
    Arena arena;
    // Prepare test data with JSON
    auto column_vector_json = ColumnString::create();
    std::string json_data = "{}";
    column_vector_json->insert_data(json_data.c_str(), json_data.length());

    // Set up the any function with JSONB type
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {std::make_shared<DataTypeJsonb>()};
    auto agg_function = factory.get("any", data_types, nullptr, false, -1);

    // Create and initialize place for aggregation
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation
    const IColumn* column[1] = {column_vector_json.get()};
    agg_function->add(place, column, 0, arena);

    // Verify result
    ColumnString ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(StringRef(json_data), ans.get_data_at(0));
    agg_function->destroy(place);
}

INSTANTIATE_TEST_SUITE_P(Params, AggMinMaxTest,
                         ::testing::ValuesIn(std::vector<std::string> {"min", "max"}));

// Test that nullable min/max streaming_agg_serialize_to_column produces correct results.
// This is a regression test for a bug where min/max with is_trivial()=true caused the
// streaming aggregation path to skip create(), leaving states zero-initialized instead of
// sentinel-initialized (MAX_VALUE for min, MIN_VALUE for max). This led to incorrect results
// (e.g., 0 for numeric types, empty strings for datetime types).
TEST_P(AggMinMaxTest, nullable_streaming_agg_int32_test) {
    Arena arena;
    std::string min_max_type = GetParam();

    // Create nullable Int32 column with values [100, 200, 300, 400]
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    std::vector<int32_t> values = {100, 200, 300, 400};
    for (auto v : values) {
        nested_col->insert_value(v);
        null_map->insert_value(0); // not null
    }
    auto nullable_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    // Create nullable aggregate function
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt32>())};
    auto agg_function = factory.get(min_max_type, data_types, nullptr, true, -1);
    ASSERT_NE(agg_function, nullptr);

    // Call streaming_agg_serialize_to_column — this is the bug path where the V2 nullable
    // wrapper allocates per-row states and may skip create() if is_trivial() returns true.
    auto dst = agg_function->create_serialize_column();
    const IColumn* columns[1] = {nullable_col.get()};
    agg_function->streaming_agg_serialize_to_column(columns, dst, values.size(), arena);

    // Deserialize each row and verify the value matches the original.
    // In streaming mode, each row is independently aggregated (single value per state),
    // so the result should be the original value itself.
    for (size_t i = 0; i < values.size(); ++i) {
        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        agg_function->deserialize_and_merge_from_column_range(place, *dst, i, i, arena);

        auto result_col = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        agg_function->insert_result_into(place, *result_col);

        ASSERT_FALSE(result_col->is_null_at(0)) << "Row " << i << " should not be null";
        const auto& result_nested =
                assert_cast<const ColumnInt32&>(result_col->get_nested_column());
        EXPECT_EQ(values[i], result_nested.get_element(0))
                << "Row " << i << " mismatch for " << min_max_type;
        agg_function->destroy(place);
    }
}

// Test nullable min/max streaming with some null values
TEST_P(AggMinMaxTest, nullable_streaming_agg_with_nulls_test) {
    Arena arena;
    std::string min_max_type = GetParam();

    // Create nullable Int64 column: [10, NULL, 30, NULL, 50]
    auto nested_col = ColumnInt64::create();
    auto null_map = ColumnUInt8::create();
    std::vector<int64_t> values = {10, 0, 30, 0, 50};
    std::vector<uint8_t> nulls = {0, 1, 0, 1, 0};
    for (size_t i = 0; i < values.size(); i++) {
        nested_col->insert_value(values[i]);
        null_map->insert_value(nulls[i]);
    }
    auto nullable_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt64>())};
    auto agg_function = factory.get(min_max_type, data_types, nullptr, true, -1);
    ASSERT_NE(agg_function, nullptr);

    auto dst = agg_function->create_serialize_column();
    const IColumn* columns[1] = {nullable_col.get()};
    agg_function->streaming_agg_serialize_to_column(columns, dst, values.size(), arena);

    for (size_t i = 0; i < values.size(); ++i) {
        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        agg_function->deserialize_and_merge_from_column_range(place, *dst, i, i, arena);

        auto result_col = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
        agg_function->insert_result_into(place, *result_col);

        if (nulls[i]) {
            EXPECT_TRUE(result_col->is_null_at(0)) << "Row " << i << " should be null";
        } else {
            ASSERT_FALSE(result_col->is_null_at(0)) << "Row " << i << " should not be null";
            const auto& result_nested =
                    assert_cast<const ColumnInt64&>(result_col->get_nested_column());
            EXPECT_EQ(values[i], result_nested.get_element(0))
                    << "Row " << i << " mismatch for " << min_max_type;
        }
        agg_function->destroy(place);
    }
}

// Test nullable min/max streaming with DateTimeV2 type — this was the original symptom
// where zero-initialized DateTimeV2 (0000-00-00 00:00:00) produced empty strings.
TEST_P(AggMinMaxTest, nullable_streaming_agg_datetimev2_test) {
    Arena arena;
    std::string min_max_type = GetParam();

    // Create nullable DateTimeV2 column with some timestamps.
    // DateTimeV2 is stored as UInt64 internally via ColumnDateTimeV2.
    auto nested_col = ColumnDateTimeV2::create();
    auto null_map = ColumnUInt8::create();
    // Use valid DateTimeV2 encoded values (year<<46 | month<<42 | day<<37 | hour<<32 | min<<26 |
    // sec<<20 | usec). These are 2000-01-01, 2024-06-15 10:30:45, 2024-12-25 23:59:59.
    std::vector<uint64_t> values = {140742023840792576ULL, 142454833089085440ULL,
                                    142482653553098752ULL};
    for (auto v : values) {
        nested_col->insert_value(binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(v));
        null_map->insert_value(0);
    }
    auto nullable_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeDateTimeV2>())};
    auto agg_function = factory.get(min_max_type, data_types, nullptr, true, -1);
    ASSERT_NE(agg_function, nullptr);

    auto dst = agg_function->create_serialize_column();
    const IColumn* columns[1] = {nullable_col.get()};
    agg_function->streaming_agg_serialize_to_column(columns, dst, values.size(), arena);

    for (size_t i = 0; i < values.size(); ++i) {
        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        agg_function->deserialize_and_merge_from_column_range(place, *dst, i, i, arena);

        auto result_col = ColumnNullable::create(ColumnDateTimeV2::create(), ColumnUInt8::create());
        agg_function->insert_result_into(place, *result_col);

        ASSERT_FALSE(result_col->is_null_at(0)) << "Row " << i << " should not be null";
        const auto& result_nested =
                assert_cast<const ColumnDateTimeV2&>(result_col->get_nested_column());
        EXPECT_EQ(values[i], result_nested.get_element(0).to_date_int_val())
                << "Row " << i << " mismatch for " << min_max_type;
        agg_function->destroy(place);
    }
}

// Test that any_value still works correctly with the trivial path (is_trivial() should
// still return true for any_value with fixed-length types since it uses has_value guard).
TEST(AggMinMaxTest, any_value_nullable_streaming_agg_test) {
    Arena arena;

    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    std::vector<int32_t> values = {42, 99, 7};
    for (auto v : values) {
        nested_col->insert_value(v);
        null_map->insert_value(0);
    }
    auto nullable_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt32>())};
    auto agg_function = factory.get("any", data_types, nullptr, true, -1);
    ASSERT_NE(agg_function, nullptr);

    auto dst = agg_function->create_serialize_column();
    const IColumn* columns[1] = {nullable_col.get()};
    agg_function->streaming_agg_serialize_to_column(columns, dst, values.size(), arena);

    for (size_t i = 0; i < values.size(); ++i) {
        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        agg_function->deserialize_and_merge_from_column_range(place, *dst, i, i, arena);

        auto result_col = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        agg_function->insert_result_into(place, *result_col);

        ASSERT_FALSE(result_col->is_null_at(0)) << "Row " << i << " should not be null";
        const auto& result_nested =
                assert_cast<const ColumnInt32&>(result_col->get_nested_column());
        EXPECT_EQ(values[i], result_nested.get_element(0))
                << "Row " << i << " mismatch for any_value";
        agg_function->destroy(place);
    }
}

} // namespace doris
