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
#include <memory.h>
#include <rapidjson/document.h>

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/sink/vmysql_result_writer.h"

namespace {

using namespace doris::vectorized;
using Offsets = ColumnArray::Offsets;

template <typename T, typename... Ts>
DataTypePtr create_type() {
    if constexpr (sizeof...(Ts) == 0) {
        return std::make_shared<T>();
    } else {
        return std::make_shared<T>(create_type<Ts...>());
    }
}

std::string convert_to_json_string(MutableColumnPtr& column_ptr, DataTypePtr data_type_ptr) {
    const auto& column_array = assert_cast<const ColumnArray&>(*column_ptr);
    VMysqlResultWriter writer(nullptr, {}, nullptr);
    const Offsets& offsets = column_array.get_offsets();
    const auto [json, status] = writer._convert_array_to_json_string(
            column_array, 0, offsets[0] - offsets[-1],
            assert_cast<const DataTypeArray&>(*data_type_ptr).get_nested_type());
    EXPECT_TRUE(status.ok());
    return json;
}

void check_array_to_json(MutableColumnPtr& column_ptr, DataTypePtr data_type_ptr, const Array& data,
                         std::string_view expected_json) {
    column_ptr->clear();
    column_ptr->insert(data);
    std::string actual_json = convert_to_json_string(column_ptr, data_type_ptr);
    rapidjson::Document document;
    EXPECT_FALSE(document.Parse(actual_json.c_str(), actual_json.length()).HasParseError());
    EXPECT_EQ(expected_json, actual_json);
}

int64_t create_date_time_int64(std::string_view str) {
    VecDateTimeValue datetime;
    datetime.from_date_str(str.data(), str.size());
    return *reinterpret_cast<int64_t*>(&datetime);
}

} // namespace

namespace doris::vectorized {

using Offsets = ColumnArray::Offsets;

TEST(ColumnArrayToJSONTest, TestBoolean) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeUInt8>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {true, false}, "[true, false]");
    check_array_to_json(column_ptr, data_type_ptr, {false, Field(), true}, "[false, null, true]");
}

TEST(ColumnArrayToJSONTest, TestTinyInt) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeInt8>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {1, 2, 3}, "[1, 2, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {1, Field(), 3}, "[1, null, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), 2, Field()}, "[null, 2, null]");
}

TEST(ColumnArrayToJSONTest, TestSmallInt) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeInt16>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {1, 2, 3}, "[1, 2, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {1, Field(), 3}, "[1, null, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), 2, Field()}, "[null, 2, null]");
}

TEST(ColumnArrayToJSONTest, TestInt) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeInt32>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {1, 2, 3}, "[1, 2, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {1, Field(), 3}, "[1, null, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), 2, Field()}, "[null, 2, null]");
}

TEST(ColumnArrayToJSONTest, TestBigInt) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeInt64>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {1, 2, 3}, "[1, 2, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {1, Field(), 3}, "[1, null, 3]");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), 2, Field()}, "[null, 2, null]");
}

TEST(ColumnArrayToJSONTest, TestLargeInt) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeInt128>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {Int128(1), Int128(2), Int128(3)},
                        R"(["1", "2", "3"])");
    check_array_to_json(column_ptr, data_type_ptr, {Int128(1), Field(), Int128(3)},
                        R"(["1", null, "3"])");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), Int128(2), Field()},
                        R"([null, "2", null])");
}

TEST(ColumnArrayToJSONTest, TestFloat32) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeFloat32>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {1.5, 2.5, 3.5}, "[1.5, 2.5, 3.5]");
    check_array_to_json(column_ptr, data_type_ptr, {1.5, Field(), 3.5}, "[1.5, null, 3.5]");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), 2.5, Field()}, "[null, 2.5, null]");
}

TEST(ColumnArrayToJSONTest, TestFloat64) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeFloat64>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {1.5, 2.5, 3.5}, "[1.5, 2.5, 3.5]");
    check_array_to_json(column_ptr, data_type_ptr, {1.5, Field(), 3.5}, "[1.5, null, 3.5]");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), 2.5, Field()}, "[null, 2.5, null]");
}

TEST(ColumnArrayToJSONTest, TestDecimal) {
    auto data_type_ptr =
            create_type<DataTypeArray, DataTypeNullable, DataTypeDecimal<Decimal128>>();
    const auto& data_type_decimal = assert_cast<const DataTypeDecimal<Decimal128>&>(
            *assert_cast<const DataTypeNullable&>(
                     *assert_cast<const DataTypeArray&>(*data_type_ptr).get_nested_type())
                     .get_nested_type());
    auto scale = data_type_decimal.get_scale();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr,
                        {DecimalField<Decimal128>(Decimal128::double_to_decimal(1.5), scale),
                         DecimalField<Decimal128>(Decimal128::double_to_decimal(2.5), scale),
                         DecimalField<Decimal128>(Decimal128::double_to_decimal(3.5), scale)},
                        R"(["1.500000000", "2.500000000", "3.500000000"])");
    check_array_to_json(
            column_ptr, data_type_ptr,
            {DecimalField<Decimal128>(Decimal128::double_to_decimal(1.5), scale), Field(),
             DecimalField<Decimal128>(Decimal128::double_to_decimal(3.5), scale)},
            R"(["1.500000000", null, "3.500000000"])");
    check_array_to_json(
            column_ptr, data_type_ptr,
            {Field(), DecimalField<Decimal128>(Decimal128::double_to_decimal(2.5), scale), Field()},
            R"([null, "2.500000000", null])");
}

TEST(ColumnArrayToJSONTest, TestDate) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeDate>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr,
                        {create_date_time_int64("2022-09-01"), create_date_time_int64("2022-09-02"),
                         create_date_time_int64("2022-09-03")},
                        R"(["2022-09-01", "2022-09-02", "2022-09-03"])");
    check_array_to_json(
            column_ptr, data_type_ptr,
            {create_date_time_int64("2022-09-01"), Field(), create_date_time_int64("2022-09-03")},
            R"(["2022-09-01", null, "2022-09-03"])");
    check_array_to_json(column_ptr, data_type_ptr,
                        {Field(), create_date_time_int64("2022-09-02"), Field()},
                        R"([null, "2022-09-02", null])");
}

TEST(ColumnArrayToJSONTest, TestDateTime) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeDateTime>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr,
                        {create_date_time_int64("2022-09-01 01:10:01"),
                         create_date_time_int64("2022-09-02 02:20:02"),
                         create_date_time_int64("2022-09-03 03:30:03")},
                        R"(["2022-09-01 01:10:01", "2022-09-02 02:20:02", "2022-09-03 03:30:03"])");
    check_array_to_json(column_ptr, data_type_ptr,
                        {create_date_time_int64("2022-09-01 01:10:01"), Field(),
                         create_date_time_int64("2022-09-03 03:30:03")},
                        R"(["2022-09-01 01:10:01", null, "2022-09-03 03:30:03"])");
    check_array_to_json(column_ptr, data_type_ptr,
                        {Field(), create_date_time_int64("2022-09-02 02:20:02"), Field()},
                        R"([null, "2022-09-02 02:20:02", null])");
}

TEST(ColumnArrayToJSONTest, TestArray) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeArray,
                                     DataTypeNullable, DataTypeInt32>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    Array data;
    data.push_back(Array {});
    check_array_to_json(column_ptr, data_type_ptr, data, "[[]]");
    check_array_to_json(column_ptr, data_type_ptr,
                        {Array {1, 2, 3}, Array {4, 5, 6}, Array {7, 8, 9}},
                        "[[1, 2, 3], [4, 5, 6], [7, 8, 9]]");
    check_array_to_json(column_ptr, data_type_ptr, {Array {1, 2, 3}, Field(), Array {7, 8, 9}},
                        "[[1, 2, 3], null, [7, 8, 9]]");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), Array {4, 5, 6}, Field()},
                        "[null, [4, 5, 6], null]");
}

TEST(ColumnArrayToJSONTest, TestString) {
    auto data_type_ptr = create_type<DataTypeArray, DataTypeNullable, DataTypeString>();
    auto column_ptr = data_type_ptr->create_column();
    check_array_to_json(column_ptr, data_type_ptr, {}, "[]");
    check_array_to_json(column_ptr, data_type_ptr, {Field()}, "[null]");
    check_array_to_json(column_ptr, data_type_ptr, {"a", "b", "c"}, R"(["a", "b", "c"])");
    check_array_to_json(column_ptr, data_type_ptr, {"a", Field(), "c"}, R"(["a", null, "c"])");
    check_array_to_json(column_ptr, data_type_ptr, {Field(), "b", Field()}, R"([null, "b", null])");
    check_array_to_json(column_ptr, data_type_ptr, {R"("Hello, world")", R"(\a, \b)"},
                        R"(["\"Hello, world\"", "\\a, \\b"])");
}

} // namespace doris::vectorized
