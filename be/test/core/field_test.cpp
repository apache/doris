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

#include "core/field.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <bit>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <string>

#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_buffer.hpp"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_string.h"
#include "gtest/gtest_pred_impl.h" // IWYU pragma: keep

namespace doris {
TEST(VFieldTest, detects_floating_point_nan) {
    EXPECT_TRUE(Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::quiet_NaN()).is_nan());
    EXPECT_TRUE(
            Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN()).is_nan());
    EXPECT_FALSE(Field::create_field<TYPE_FLOAT>(0.0F).is_nan());
    EXPECT_FALSE(Field::create_field<TYPE_DOUBLE>(-0.0).is_nan());
    EXPECT_FALSE(Field::create_field<TYPE_INT>(0).is_nan());
    EXPECT_FALSE(Field().is_nan());
}

TEST(VFieldTest, field_string) {
    Field f;

    f = Field::create_field<TYPE_STRING>(String {"Hello, world (1)"});
    ASSERT_EQ(f.get<TYPE_STRING>(), "Hello, world (1)");
    f = Field::create_field<TYPE_STRING>(String {"Hello, world (2)"});
    ASSERT_EQ(f.get<TYPE_STRING>(), "Hello, world (2)");
    f = Field::create_field<TYPE_ARRAY>(
            Array {Field ::create_field<TYPE_STRING>(String {"Hello, world (3)"})});
    ASSERT_EQ(f.get<TYPE_ARRAY>()[0].get<TYPE_STRING>(), "Hello, world (3)");
    f = Field::create_field<TYPE_STRING>(String {"Hello, world (4)"});
    ASSERT_EQ(f.get<TYPE_STRING>(), "Hello, world (4)");
    f = Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_STRING>(String {"Hello, world (5)"})});
    ASSERT_EQ(f.get<TYPE_ARRAY>()[0].get<TYPE_STRING>(), "Hello, world (5)");
    f = Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_STRING>(String {"Hello, world (6)"})});
    ASSERT_EQ(f.get<TYPE_ARRAY>()[0].get<TYPE_STRING>(), "Hello, world (6)");
}

TEST(VFieldTest, field_timestamptz) {
    Field f;
    f = Field::create_field<TYPE_TIMESTAMPTZ>(*(TimestampTzValue*)&MIN_DATETIME_V2);
    ASSERT_EQ(f.get_type(), TYPE_TIMESTAMPTZ);
    ASSERT_EQ(f.get<TYPE_TIMESTAMPTZ>().to_date_int_val(), MIN_DATETIME_V2);
}

TEST(VFieldTest, jsonb_field_unique_ptr) {
    // Test default constructor
    JsonbField empty;
    ASSERT_EQ(empty.get_value(), nullptr);
    ASSERT_EQ(empty.get_size(), 0);

    // Test constructor with data
    const char* test_data = R"({ "key": "value" })";
    size_t test_size = strlen(test_data);
    JsonbField jf1(test_data, test_size);
    ASSERT_NE(jf1.get_value(), nullptr);
    ASSERT_EQ(jf1.get_size(), test_size);
    ASSERT_EQ(std::string(jf1.get_value(), jf1.get_size()), std::string(test_data));

    // Test copy constructor
    JsonbField jf2(jf1);
    ASSERT_NE(jf2.get_value(), nullptr);
    ASSERT_NE(jf2.get_value(), jf1.get_value()); // Different memory locations
    ASSERT_EQ(jf2.get_size(), jf1.get_size());
    ASSERT_EQ(std::string(jf2.get_value(), jf2.get_size()),
              std::string(jf1.get_value(), jf1.get_size()));

    // Test move constructor
    JsonbField jf3(std::move(jf2));
    ASSERT_NE(jf3.get_value(), nullptr);
    ASSERT_EQ(jf2.get_value(), nullptr); // jf2 should be empty after move
    ASSERT_EQ(jf2.get_size(), 0);        // jf2 size should be 0 after move
    ASSERT_EQ(jf3.get_size(), test_size);
    ASSERT_EQ(std::string(jf3.get_value(), jf3.get_size()), std::string(test_data));

    // Test copy assignment
    JsonbField jf4;
    jf4 = jf1;
    ASSERT_NE(jf4.get_value(), nullptr);
    ASSERT_NE(jf4.get_value(), jf1.get_value()); // Different memory locations
    ASSERT_EQ(jf4.get_size(), jf1.get_size());
    ASSERT_EQ(std::string(jf4.get_value(), jf4.get_size()),
              std::string(jf1.get_value(), jf1.get_size()));

    // Test move assignment
    JsonbField jf5;
    jf5 = std::move(jf4);
    ASSERT_NE(jf5.get_value(), nullptr);
    ASSERT_EQ(jf4.get_value(), nullptr); // jf4 should be empty after move
    ASSERT_EQ(jf4.get_size(), 0);        // jf4 size should be 0 after move
    ASSERT_EQ(jf5.get_size(), test_size);
    ASSERT_EQ(std::string(jf5.get_value(), jf5.get_size()), std::string(test_data));

    // Test JsonbField with Field
    Field field_jf = Field::create_field<TYPE_JSONB>(jf1);
    ASSERT_EQ(field_jf.get_type(), TYPE_JSONB);
    ASSERT_NE(field_jf.get<TYPE_JSONB>().get_value(), nullptr);
    ASSERT_EQ(field_jf.get<TYPE_JSONB>().get_size(), test_size);
    ASSERT_EQ(std::string(field_jf.get<TYPE_JSONB>().get_value(),
                          field_jf.get<TYPE_JSONB>().get_size()),
              std::string(test_data));
}

// Test for JsonbField I/O operations
TEST(VFieldTest, jsonb_field_io) {
    // Prepare a JsonbField
    const char* test_data = R"({ "key": "value" })";
    size_t test_size = strlen(test_data);
    JsonbField original(test_data, test_size);

    // TEST 1: write_json_binary - From JsonbField to buffer
    // Create a ColumnString to use with BufferWritable
    ColumnString column_str;

    // Write the JsonbField to the buffer
    {
        BufferWritable buf(column_str);
        buf.write_binary(StringRef {original.get_value(), original.get_size()});
        buf.commit(); // Important: commit the write operation
    }

    // Verify data was written
    ASSERT_GT(column_str.size(), 0);

    // Read the JsonbField back using BufferReadable
    {
        // Get the StringRef from ColumnString
        StringRef str_ref = column_str.get_data_at(0);

        // Create a BufferReadable from StringRef
        BufferReadable read_buf(str_ref);

        // Read the data back into a new JsonbField
        StringRef result;
        read_buf.read_binary(result);
        JsonbField read_field = JsonbField(result.data, result.size);

        // Verify the data
        ASSERT_NE(read_field.get_value(), nullptr);
        ASSERT_EQ(read_field.get_size(), original.get_size());
        ASSERT_EQ(std::string(read_field.get_value(), read_field.get_size()),
                  std::string(original.get_value(), original.get_size()));
    }

    // Test with JsonbField as a Field and serde it
    {
        ColumnString field_column;

        // ser
        {
            BufferWritable field_buf(field_column);
            field_buf.write_binary(StringRef {original.get_value(), original.get_size()});
            field_buf.commit();
        }

        // Verify field was written
        ASSERT_GT(field_column.size(), 0);

        // de
        {
            StringRef field_str_ref = field_column.get_data_at(0);
            BufferReadable read_field_buf(field_str_ref);

            // we can't use read_binary because of the JsonbField is not POD type
            StringRef result;
            read_field_buf.read_binary(result);
            JsonbField jsonb_from_field = JsonbField(result.data, result.size);
            Field f2 = Field::create_field<TYPE_JSONB>(jsonb_from_field);

            ASSERT_EQ(f2.get_type(), TYPE_JSONB);
            ASSERT_NE(f2.get<TYPE_JSONB>().get_value(), nullptr);
            ASSERT_EQ(
                    std::string(f2.get<TYPE_JSONB>().get_value(), f2.get<TYPE_JSONB>().get_size()),
                    std::string(test_data));
        }
    }
}

TEST(VFieldTest, field_create) {
    Field string_f = Field::create_field<TYPE_STRING>(String {"Hello, world (1)"});
    Field string_copy = Field::create_field<TYPE_STRING>(String {"Hello, world (1)"});

    string_copy = std::move(string_f);
    string_copy = string_f;

    Field int_f = Field::create_field<TYPE_INT>(1);
    Field int_copy = Field::create_field<TYPE_INT>(1);
    int_copy = std::move(int_f);
    int_copy = int_f;

    Field jsonb = Field::create_field<TYPE_JSONB>(JsonbField {R"({ "key": "value" })", 13});
    Field jsonb_copy = Field::create_field<TYPE_JSONB>(JsonbField {R"({ "key": "value" })", 13});
    jsonb_copy = std::move(jsonb);
    jsonb_copy = jsonb;

    Field double_f = Field::create_field<TYPE_DOUBLE>(1.0);
    Field double_copy = Field::create_field<TYPE_DOUBLE>(1.0);
    double_copy = std::move(double_f);
    double_copy = double_f;

    Field largeint = Field::create_field<TYPE_LARGEINT>(Int128(1));
    Field largeint_copy = Field::create_field<TYPE_LARGEINT>(Int128(1));
    largeint_copy = std::move(largeint);
    largeint_copy = largeint;

    Field array_f = Field::create_field<TYPE_ARRAY>(Array {int_f});
    Field array_copy = Field::create_field<TYPE_ARRAY>(Array {int_f});
    array_copy = std::move(array_f);
    array_copy = array_f;

    Field map_f = Field::create_field<TYPE_MAP>(Map {int_f, string_f});
    Field map_copy = Field::create_field<TYPE_MAP>(Map {int_f, string_f});
    map_copy = std::move(map_f);
    map_copy = map_f;

    Field ipv4_f = Field::create_field<TYPE_IPV4>(IPv4(1));
    Field ipv4_copy = Field::create_field<TYPE_IPV4>(IPv4(1));
    ipv4_copy = std::move(ipv4_f);
    ipv4_copy = ipv4_f;

    Field ipv6_f = Field::create_field<TYPE_IPV6>(IPv6(1));
    Field ipv6_copy = Field::create_field<TYPE_IPV6>(IPv6(1));
    ipv6_copy = std::move(ipv6_f);
    ipv6_copy = ipv6_f;

    Field decimal32_f = Field::create_field<TYPE_DECIMAL32>(Decimal32(1));
    Field decimal32_copy = Field::create_field<TYPE_DECIMAL32>(Decimal32(1));
    decimal32_copy = std::move(decimal32_f);
    decimal32_copy = decimal32_f;

    Field decimal64_f = Field::create_field<TYPE_DECIMAL64>(Decimal64(1));
    Field decimal64_copy = Field::create_field<TYPE_DECIMAL64>(Decimal64(1));
    decimal64_copy = std::move(decimal64_f);
    decimal64_copy = decimal64_f;

    Field decimal128_f = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(1));
    Field decimal128_copy = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(1));
    decimal128_copy = std::move(decimal128_f);
    decimal128_copy = decimal128_f;

    Field decimal256_f = Field::create_field<TYPE_DECIMAL256>(Decimal256(1));
    Field decimal256_copy = Field::create_field<TYPE_DECIMAL256>(Decimal256(1));
    decimal256_copy = std::move(decimal256_f);
    decimal256_copy = decimal256_f;

    Field bitmap_f = Field::create_field<TYPE_BITMAP>(BitmapValue(1));
    Field bitmap_copy = Field::create_field<TYPE_BITMAP>(BitmapValue(1));
    bitmap_copy = std::move(bitmap_f);
    bitmap_copy = bitmap_f;

    Field hll_f = Field::create_field<TYPE_HLL>(HyperLogLog(1));
    Field hll_copy = Field::create_field<TYPE_HLL>(HyperLogLog(1));
    hll_copy = std::move(hll_f);
    hll_copy = hll_f;

    Field quantile_state_f = Field::create_field<TYPE_QUANTILE_STATE>(QuantileState(1));
    Field quantile_state_copy = Field::create_field<TYPE_QUANTILE_STATE>(QuantileState(1));
    quantile_state_copy = std::move(quantile_state_f);
    quantile_state_copy = quantile_state_f;

    Field bitmap_value_f = Field::create_field<TYPE_BITMAP>(BitmapValue(1));
    Field bitmap_value_copy = Field::create_field<TYPE_BITMAP>(BitmapValue(1));
    bitmap_value_copy = std::move(bitmap_value_f);
    bitmap_value_copy = bitmap_value_f;
}

TEST(VFieldTest, array_comparison) {
    auto make_array = [](std::initializer_list<Field> values) {
        return Field::create_field<TYPE_ARRAY>(Array(values));
    };
    EXPECT_LT(make_array({Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}),
              make_array({Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(3)}));
    EXPECT_LT(make_array({Field::create_field<TYPE_INT>(1)}),
              make_array({Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}));
    EXPECT_GT(make_array({Field(TYPE_NULL)}), make_array({Field::create_field<TYPE_INT>(1)}));
}

TEST(VFieldTest, nested_array_comparison) {
    auto make_array = [](std::initializer_list<Field> values) {
        return Field::create_field<TYPE_ARRAY>(Array(values));
    };
    const auto one = Field::create_field<TYPE_INT>(1);
    const auto two = Field::create_field<TYPE_INT>(2);
    const auto null = Field();
    EXPECT_EQ(make_array({}), make_array({}));
    EXPECT_LT(make_array({}), make_array({null}));
    EXPECT_EQ(make_array({null, one}), make_array({null, one}));
    EXPECT_LT(make_array({null, one}), make_array({null, two}));
    EXPECT_LT(make_array({make_array({one})}), make_array({make_array({one, two})}));
    EXPECT_LT(make_array({make_array({one, two})}), make_array({make_array({one, null})}));
    EXPECT_LT(make_array({make_array({})}), make_array({make_array({null})}));
    EXPECT_LT(make_array({make_array({null})}), make_array({null}));
    EXPECT_EQ(make_array({make_array({null}), null}), make_array({make_array({null}), null}));
}

namespace {

void expect_same_field(const Field& actual, const Field& expected) {
    ASSERT_EQ(actual.get_type(), expected.get_type());
    if (expected.get_type() == TYPE_ARRAY) {
        const auto& lhs = actual.get<TYPE_ARRAY>();
        const auto& rhs = expected.get<TYPE_ARRAY>();
        ASSERT_EQ(lhs.size(), rhs.size());
        for (size_t i = 0; i < lhs.size(); ++i) {
            expect_same_field(lhs[i], rhs[i]);
        }
    } else if (expected.get_type() == TYPE_FLOAT) {
        EXPECT_EQ(std::bit_cast<uint32_t>(actual.get<TYPE_FLOAT>()),
                  std::bit_cast<uint32_t>(expected.get<TYPE_FLOAT>()));
    } else if (expected.get_type() == TYPE_DOUBLE) {
        EXPECT_EQ(std::bit_cast<uint64_t>(actual.get<TYPE_DOUBLE>()),
                  std::bit_cast<uint64_t>(expected.get<TYPE_DOUBLE>()));
    } else {
        EXPECT_EQ(actual, expected);
    }
}

void check_column_comparison(const DataTypePtr& type, const FieldVector& values) {
    SCOPED_TRACE(type->get_name());
    auto column = type->create_column();
    for (const auto& value : values) {
        column->insert(value);
    }
    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            SCOPED_TRACE(std::to_string(i) + "," + std::to_string(j));
            // SQL's generic complex-type comparison uses NULLS LAST.
            const int expected = column->compare_at(i, j, *column, 1);
            EXPECT_EQ(values[i] < values[j], expected < 0);
            EXPECT_EQ(values[i] > values[j], expected > 0);
            EXPECT_EQ(values[i] == values[j], expected == 0);
        }
    }
}

void check_scalar_array_comparison(const DataTypePtr& type, const FieldVector& values) {
    check_column_comparison(type, values);
    auto wrap = [](const FieldVector& elements) {
        FieldVector arrays {Field::create_field<TYPE_ARRAY>(Array()),
                            Field::create_field<TYPE_ARRAY>(Array {Field()})};
        for (const auto& element : elements) {
            arrays.push_back(Field::create_field<TYPE_ARRAY>(Array {element}));
            arrays.push_back(Field::create_field<TYPE_ARRAY>(Array {element, Field()}));
            arrays.push_back(Field::create_field<TYPE_ARRAY>(Array {Field(), element}));
        }
        return arrays;
    };
    auto array_type = std::make_shared<DataTypeArray>(type);
    auto arrays = wrap(values);
    check_column_comparison(array_type, arrays);
    check_column_comparison(std::make_shared<DataTypeArray>(array_type), wrap(arrays));
}

template <PrimitiveType T>
void check_number_comparison(
        std::initializer_list<typename PrimitiveTypeTraits<T>::CppType> values) {
    FieldVector fields;
    for (const auto& value : values) {
        fields.push_back(Field::create_field<T>(value));
    }
    check_scalar_array_comparison(std::make_shared<typename PrimitiveTypeTraits<T>::DataType>(),
                                  fields);
}

template <PrimitiveType T>
void check_decimal_comparison(int precision, int scale) {
    using Value = typename PrimitiveTypeTraits<T>::CppType;
    check_scalar_array_comparison(
            std::make_shared<DataTypeDecimal<T>>(precision, scale),
            {Field::create_field<T>(Value(-12345)), Field::create_field<T>(Value(0)),
             Field::create_field<T>(Value(12345))});
}

void check_field_binary(const Field& value) {
    ColumnString column;
    BufferWritable writer(column);
    write_field_binary(value, writer);
    writer.commit();
    auto bytes = column.get_data_at(0);
    EXPECT_EQ(static_cast<uint8_t>(bytes.data[0]), static_cast<uint8_t>(value.get_type()));
    BufferReadable reader(bytes);
    Field decoded = Field::create_field<TYPE_STRING>(String("old value"));
    read_field_binary(decoded, reader);
    EXPECT_EQ(reader.data(), bytes.data + bytes.size);
    expect_same_field(decoded, value);
    write_field_binary(decoded, writer);
    writer.commit();
    EXPECT_EQ(column.get_data_at(0), column.get_data_at(1));
}

template <PrimitiveType T>
void check_scalar_binary(typename PrimitiveTypeTraits<T>::CppType value) {
    const auto field = Field::create_field<T>(value);
    check_field_binary(field);
    check_field_binary(Field::create_field<TYPE_ARRAY>(Array {field, Field(), field}));
}

} // namespace

TEST(VFieldTest, comparison_matches_column) {
    check_number_comparison<TYPE_BOOLEAN>({0, 1});
    check_number_comparison<TYPE_TINYINT>({-128, 0, 127});
    check_number_comparison<TYPE_SMALLINT>({-32768, 0, 32767});
    check_number_comparison<TYPE_INT>(
            {std::numeric_limits<Int32>::min(), 0, std::numeric_limits<Int32>::max()});
    check_number_comparison<TYPE_BIGINT>(
            {std::numeric_limits<Int64>::min(), 0, std::numeric_limits<Int64>::max()});
    check_number_comparison<TYPE_LARGEINT>({-(Int128(1) << 100), 0, Int128(1) << 100});
    check_number_comparison<TYPE_FLOAT>({-std::numeric_limits<float>::infinity(), -1.5F, -0.0F,
                                         0.0F, 1.5F, std::numeric_limits<float>::infinity(),
                                         std::numeric_limits<float>::quiet_NaN()});
    check_number_comparison<TYPE_DOUBLE>({-std::numeric_limits<double>::infinity(), -1.5, -0.0, 0.0,
                                          1.5, std::numeric_limits<double>::infinity(),
                                          std::numeric_limits<double>::quiet_NaN()});
    check_number_comparison<TYPE_TIMEV2>({-3600000000.0, 0.0, 3600123456.0});
    check_number_comparison<TYPE_IPV4>({IPv4(0), IPv4(1), IPv4(0xffffffff)});
    check_number_comparison<TYPE_IPV6>({IPv6(0), IPv6(1), IPv6(1) << 100});
    check_decimal_comparison<TYPE_DECIMAL32>(9, 2);
    check_decimal_comparison<TYPE_DECIMAL64>(18, 6);
    check_decimal_comparison<TYPE_DECIMAL128I>(38, 12);
    check_decimal_comparison<TYPE_DECIMAL256>(76, 30);
    check_decimal_comparison<TYPE_DECIMALV2>(27, 9);
    for (auto tag : {TYPE_STRING, TYPE_CHAR, TYPE_VARCHAR}) {
        check_scalar_array_comparison(std::make_shared<DataTypeString>(32, tag),
                                      {Field::create_field<TYPE_STRING>(String()),
                                       Field::create_field<TYPE_CHAR>(String("a")),
                                       Field::create_field<TYPE_VARCHAR>(String("a\0b", 3)),
                                       Field::create_field<TYPE_STRING>(String("ab")),
                                       Field::create_field<TYPE_STRING>(String("\xff", 1))});
    }
    VecDateTimeValue first, second;
    ASSERT_TRUE(first.from_date_int64(20260101));
    ASSERT_TRUE(second.from_date_int64(20260201));
    check_number_comparison<TYPE_DATE>({first, second});
    ASSERT_TRUE(first.from_date_int64(20260101123456));
    ASSERT_TRUE(second.from_date_int64(20260201123456));
    check_number_comparison<TYPE_DATETIME>({first, second});
    DateV2Value<DateV2ValueType> date1, date2;
    ASSERT_TRUE(date1.from_date_int64(20260101));
    ASSERT_TRUE(date2.from_date_int64(20260201));
    check_number_comparison<TYPE_DATEV2>({date1, date2});
    auto dt1 = DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(20260101123456);
    auto dt2 = dt1;
    dt2.set_microsecond(123456);
    check_scalar_array_comparison(
            std::make_shared<DataTypeDateTimeV2>(6),
            {Field::create_field<TYPE_DATETIMEV2>(dt1), Field::create_field<TYPE_DATETIMEV2>(dt2)});
    check_scalar_array_comparison(std::make_shared<DataTypeTimeStampTz>(6),
                                  {Field::create_field<TYPE_TIMESTAMPTZ>(TimestampTzValue(dt1)),
                                   Field::create_field<TYPE_TIMESTAMPTZ>(TimestampTzValue(dt2))});
}

TEST(VFieldTest, comparison_context_boundaries) {
    auto nullable = make_nullable(std::make_shared<DataTypeInt32>())->create_column();
    const auto one = Field::create_field<TYPE_INT>(1);
    nullable->insert(Field());
    nullable->insert(one);
    EXPECT_LT(Field(), one);
    EXPECT_GT(nullable->compare_at(0, 1, *nullable, 1), 0);
    EXPECT_LT(nullable->compare_at(0, 1, *nullable, -1), 0);

    auto lhs = DataTypeDecimal32(9, 2).create_column();
    auto rhs = DataTypeDecimal32(9, 3).create_column();
    auto a = Field::create_field<TYPE_DECIMAL32>(Decimal32(123));
    auto b = Field::create_field<TYPE_DECIMAL32>(Decimal32(200));
    lhs->insert(a);
    rhs->insert(b);
    // Fields compare raw integers; columns can compare 1.23 against 0.200 using scales.
    EXPECT_LT(a, b);
    EXPECT_GT(lhs->compare_at(0, 0, *rhs, 1), 0);
}

TEST(VFieldTest, binary_scalars) {
    check_field_binary(Field());
    check_scalar_binary<TYPE_BOOLEAN>(0);
    check_scalar_binary<TYPE_BOOLEAN>(1);
    check_scalar_binary<TYPE_TINYINT>(-128);
    check_scalar_binary<TYPE_SMALLINT>(32767);
    check_scalar_binary<TYPE_INT>(std::numeric_limits<Int32>::min());
    check_scalar_binary<TYPE_BIGINT>(std::numeric_limits<Int64>::max());
    check_scalar_binary<TYPE_LARGEINT>(-(Int128(1) << 100));
    check_scalar_binary<TYPE_FLOAT>(1.25F);
    check_scalar_binary<TYPE_FLOAT>(-0.0F);
    check_scalar_binary<TYPE_FLOAT>(std::numeric_limits<float>::infinity());
    check_scalar_binary<TYPE_FLOAT>(std::bit_cast<float>(uint32_t(0x7fc00042)));
    check_scalar_binary<TYPE_DOUBLE>(-1.25);
    check_scalar_binary<TYPE_DOUBLE>(-0.0);
    check_scalar_binary<TYPE_DOUBLE>(-std::numeric_limits<double>::infinity());
    check_scalar_binary<TYPE_DOUBLE>(std::bit_cast<double>(uint64_t(0x7ff8000000000042)));
    check_scalar_binary<TYPE_TIMEV2>(-3600123456.0);
    check_scalar_binary<TYPE_IPV4>(IPv4(0xffffffff));
    check_scalar_binary<TYPE_IPV6>((IPv6(1) << 100) + 42);
    check_scalar_binary<TYPE_DECIMAL32>(Decimal32(-12345));
    check_scalar_binary<TYPE_DECIMAL64>(Decimal64(1234567890123));
    check_scalar_binary<TYPE_DECIMAL128I>(Decimal128V3(Int128(1) << 100));
    check_scalar_binary<TYPE_DECIMAL256>(Decimal256(wide::Int256(1) << 200));
    check_scalar_binary<TYPE_DECIMALV2>(DecimalV2Value(Int128(-1234567890)));
    check_scalar_binary<TYPE_STRING>(String());
    check_scalar_binary<TYPE_STRING>(String("a\0b", 3));
    check_scalar_binary<TYPE_CHAR>(String("abc "));
    check_scalar_binary<TYPE_VARCHAR>(String("abc"));

    VecDateTimeValue date;
    ASSERT_TRUE(date.from_date_int64(20260101));
    check_scalar_binary<TYPE_DATE>(date);
    VecDateTimeValue datetime;
    ASSERT_TRUE(datetime.from_date_int64(20260101123456));
    check_scalar_binary<TYPE_DATETIME>(datetime);
    DateV2Value<DateV2ValueType> datev2;
    ASSERT_TRUE(datev2.from_date_int64(20260101));
    check_scalar_binary<TYPE_DATEV2>(datev2);
    auto datetimev2 = DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(20260101123456);
    datetimev2.set_microsecond(123456);
    check_scalar_binary<TYPE_DATETIMEV2>(datetimev2);
    check_scalar_binary<TYPE_TIMESTAMPTZ>(TimestampTzValue(datetimev2));
}

TEST(VFieldTest, binary_nested_and_consecutive_values) {
    const auto empty = Field::create_field<TYPE_ARRAY>(Array());
    const auto nulls = Field::create_field<TYPE_ARRAY>(Array {Field(), Field()});
    const auto nested = Field::create_field<TYPE_ARRAY>(
            Array {empty, Field(), nulls, Field::create_field<TYPE_ARRAY>(Array {empty, nulls})});
    check_field_binary(empty);
    check_field_binary(nulls);
    check_field_binary(nested);
    const FieldVector values {nested, Field(), Field::create_field<TYPE_INT>(42),
                              Field::create_field<TYPE_STRING>(String("tail")), empty};
    ColumnString column;
    BufferWritable writer(column);
    std::vector<size_t> ends;
    for (const auto& value : values) {
        write_field_binary(value, writer);
        ends.push_back(column.get_chars().size());
    }
    writer.commit();
    auto bytes = column.get_data_at(0);
    BufferReadable reader(bytes);
    Field decoded;
    for (size_t i = 0; i < values.size(); ++i) {
        read_field_binary(decoded, reader);
        expect_same_field(decoded, values[i]);
        EXPECT_EQ(reader.data(), bytes.data + ends[i]);
    }
}

TEST(VFieldTest, binary_rejects_unsupported_tags) {
    ColumnString column;
    BufferWritable writer(column);
    EXPECT_THROW(write_field_binary(Field::create_field<TYPE_MAP>(Map()), writer), Exception);
    EXPECT_THROW(write_field_binary(Field::create_field<TYPE_STRUCT>(Struct()), writer), Exception);
    EXPECT_THROW(write_field_binary(Field::create_field<TYPE_JSONB>(JsonbField()), writer),
                 Exception);
    for (uint8_t tag : {uint8_t(255), uint8_t(TYPE_MAP), uint8_t(TYPE_STRUCT), uint8_t(TYPE_JSONB),
                        uint8_t(TYPE_VARIANT), uint8_t(TYPE_AGG_STATE), uint8_t(TYPE_BITMAP),
                        uint8_t(TYPE_HLL), uint8_t(TYPE_QUANTILE_STATE)}) {
        writer.write_binary(tag);
        writer.commit();
        auto bytes = column.get_data_at(column.size() - 1);
        BufferReadable reader(bytes);
        Field value;
        EXPECT_THROW(read_field_binary(value, reader), Exception);
    }
}

} // namespace doris
