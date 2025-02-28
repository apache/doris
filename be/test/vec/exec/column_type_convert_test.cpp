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

#include "vec/exec/format/column_type_convert.h"

#include <gtest/gtest.h>

#include <limits>

#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class ColumnTypeConverterTest : public testing::Test {
public:
    ColumnTypeConverterTest() = default;
    virtual ~ColumnTypeConverterTest() = default;
};

// Test integer type conversions (widening)
TEST_F(ColumnTypeConverterTest, TestIntegerWideningConversions) {
    // Test TINYINT -> SMALLINT
    {
        TypeDescriptor src_type(TYPE_TINYINT);
        auto dst_type = std::make_shared<DataTypeInt16>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt8::create();
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.push_back(42);
        src_data.push_back(-42);
        // Test boundary values
        src_data.push_back(std::numeric_limits<int8_t>::max());
        src_data.push_back(std::numeric_limits<int8_t>::min());

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnInt16&>(*mutable_dst).get_data();
        ASSERT_EQ(4, dst_data.size());
        EXPECT_EQ(42, dst_data[0]);
        EXPECT_EQ(-42, dst_data[1]);
        EXPECT_EQ(std::numeric_limits<int8_t>::max(), dst_data[2]);
        EXPECT_EQ(std::numeric_limits<int8_t>::min(), dst_data[3]);
    }

    // Test SMALLINT -> INT
    {
        TypeDescriptor src_type(TYPE_SMALLINT);
        auto dst_type = std::make_shared<DataTypeInt32>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt16::create();
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.push_back(1234);
        src_data.push_back(-1234);
        // Test boundary values
        src_data.push_back(std::numeric_limits<int16_t>::max());
        src_data.push_back(std::numeric_limits<int16_t>::min());

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnInt32&>(*mutable_dst).get_data();
        ASSERT_EQ(4, dst_data.size());
        EXPECT_EQ(1234, dst_data[0]);
        EXPECT_EQ(-1234, dst_data[1]);
        EXPECT_EQ(std::numeric_limits<int16_t>::max(), dst_data[2]);
        EXPECT_EQ(std::numeric_limits<int16_t>::min(), dst_data[3]);
    }
}

// Test integer type conversions (narrowing)
TEST_F(ColumnTypeConverterTest, TestIntegerNarrowingConversions) {
    // Test INT -> SMALLINT with values in range
    {
        TypeDescriptor src_type(TYPE_INT);
        auto dst_type = std::make_shared<DataTypeInt16>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt32::create();
        auto& src_data = src_col->get_data();
        src_data.push_back(1234);
        src_data.push_back(-1234);
        src_data.push_back(std::numeric_limits<int16_t>::max());
        src_data.push_back(std::numeric_limits<int16_t>::min());

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnInt16&>(*mutable_dst).get_data();
        ASSERT_EQ(4, dst_data.size());
        EXPECT_EQ(1234, dst_data[0]);
        EXPECT_EQ(-1234, dst_data[1]);
        EXPECT_EQ(std::numeric_limits<int16_t>::max(), dst_data[2]);
        EXPECT_EQ(std::numeric_limits<int16_t>::min(), dst_data[3]);
    }

    // Test INT -> SMALLINT with out of range values
    {
        TypeDescriptor src_type(TYPE_INT);
        auto dst_type = std::make_shared<DataTypeInt16>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt32::create();
        auto& src_data = src_col->get_data();
        src_data.push_back(std::numeric_limits<int16_t>::max() + 1);
        src_data.push_back(std::numeric_limits<int16_t>::min() - 1);

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(!st.ok());
    }
}

// Test floating point type conversions
TEST_F(ColumnTypeConverterTest, TestFloatingPointConversions) {
    // TEST INT ->  FLOAT
    {
        TypeDescriptor src_type(TYPE_INT);
        auto dst_type = std::make_shared<DataTypeFloat32>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt32::create();
        auto& src_data = src_col->get_data();
        src_data.resize(0);
        // Add test values
        src_data.push_back(12345);
        src_data.push_back(-67890);
        src_data.push_back((1L << 23) - 1);
        src_data.push_back(1L << 23);
        src_data.push_back((1L << 23) + 1);
        auto dst_nullable_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_nullable_col->assume_mutable();
        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnFloat32&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_data.size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(5, nested_col.size());
        EXPECT_FLOAT_EQ(12345.0f, nested_col.get_data()[0]);
        EXPECT_FLOAT_EQ(-67890.0f, nested_col.get_data()[1]);
        EXPECT_FLOAT_EQ((float)((1L << 23) - 1), nested_col.get_data()[2]);
        EXPECT_FLOAT_EQ(1, null_map[3]);
        EXPECT_FLOAT_EQ(1, null_map[4]);
    }
    // TEST STRING -> FLOAT
    {
        TypeDescriptor src_type(TYPE_STRING);
        auto dst_type = std::make_shared<DataTypeFloat32>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnString::create();
        // Add test strings
        src_col->insert_data("0", 1);             // Zero
        src_col->insert_data("123.45", 6);        // Positive float
        src_col->insert_data("-678.90", 7);       // Negative float
        src_col->insert_data("1.17549e-38", 11);  // Smallest positive float
        src_col->insert_data("3.40282e+38", 11);  // Largest positive float (FLT_MAX)
        src_col->insert_data("-3.40282e+38", 12); // Largest negative float (-FLT_MAX)
        src_col->insert_data("Infinity", 8);      // Infinity
        src_col->insert_data("-Infinity", 9);     // Negative infinity
        src_col->insert_data("NaN", 3);           // Not-a-number
        src_col->insert_data("invalid", 7);       // Invalid string
        src_col->insert_data("", 0);              // Empty string

        auto dst_nullable_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_nullable_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnFloat32&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(11, nested_col.size());

        // Valid conversions
        EXPECT_FLOAT_EQ(0.0f, nested_col.get_data()[0]);          // "0"
        EXPECT_FLOAT_EQ(123.45f, nested_col.get_data()[1]);       // "123.45"
        EXPECT_FLOAT_EQ(-678.90f, nested_col.get_data()[2]);      // "-678.90"
        EXPECT_FLOAT_EQ(1.17549e-38f, nested_col.get_data()[3]);  // Smallest positive float
        EXPECT_FLOAT_EQ(3.40282e+38f, nested_col.get_data()[4]);  // Largest positive float
        EXPECT_FLOAT_EQ(-3.40282e+38f, nested_col.get_data()[5]); // Largest negative float

        EXPECT_TRUE(std::isinf(nested_col.get_data()[6]) &&
                    nested_col.get_data()[6] > 0); // Infinity
        EXPECT_TRUE(std::isinf(nested_col.get_data()[7]) &&
                    nested_col.get_data()[7] < 0);         // Negative infinity
        EXPECT_TRUE(std::isnan(nested_col.get_data()[8])); // NaN

        // Invalid conversions marked as null
        for (int i = 0; i < 9; i++) {
            EXPECT_EQ(0, null_map[i]);
        }

        EXPECT_EQ(1, null_map[9]);  // "invalid"
        EXPECT_EQ(1, null_map[10]); // Empty string
    }

    // Test FLOAT -> DOUBLE (widening)
    {
        TypeDescriptor src_type(TYPE_FLOAT);
        auto dst_type = std::make_shared<DataTypeFloat64>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnFloat32::create();
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.push_back(3.14159f);
        src_data.push_back(-2.71828f);
        // Test special values
        src_data.push_back(std::numeric_limits<float>::infinity());
        src_data.push_back(-std::numeric_limits<float>::infinity());
        src_data.push_back(std::numeric_limits<float>::quiet_NaN());

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnFloat64&>(*mutable_dst).get_data();
        ASSERT_EQ(5, dst_data.size());
        EXPECT_FLOAT_EQ(3.14159, dst_data[0]);
        EXPECT_FLOAT_EQ(-2.71828, dst_data[1]);
        EXPECT_TRUE(std::isinf(dst_data[2]) && dst_data[2] > 0);
        EXPECT_TRUE(std::isinf(dst_data[3]) && dst_data[3] < 0);
        EXPECT_TRUE(std::isnan(dst_data[4]));
    }
}

// Test decimal type conversions
TEST_F(ColumnTypeConverterTest, TestDecimalConversions) {
    // Test DECIMAL32 -> DECIMAL64 (widening)
    {
        TypeDescriptor src_type(TYPE_DECIMAL32);
        src_type.precision = 9;
        src_type.scale = 2;

        auto dst_type = std::make_shared<DataTypeDecimal<Decimal64>>(18, 2);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal<Decimal32>::create(9, 2);
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.resize(0);
        src_data.push_back(Decimal32(12345));  // 123.45
        src_data.push_back(Decimal32(-12345)); // -123.45

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& dst_data = static_cast<ColumnDecimal<Decimal64>&>(*mutable_dst).get_data();
        ASSERT_EQ(2, dst_data.size());
        EXPECT_EQ(12345, dst_data[0].value);
        EXPECT_EQ(-12345, dst_data[1].value);
    }

    // Test DECIMAL32 -> DECIMAL128 (from small decimal to large decimal)
    {
        TypeDescriptor src_type(TYPE_DECIMAL32);
        src_type.precision = 9;
        src_type.scale = 2;

        auto dst_type = std::make_shared<DataTypeDecimal<Decimal128V3>>(38, 10);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal<Decimal32>::create(9, 2);
        src_col->resize(0);
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.push_back(Decimal32(12345));  // 123.45
        src_data.push_back(Decimal32(-67890)); // -678.90

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnDecimal<Decimal128V3>&>(*mutable_dst).get_data();
        ASSERT_EQ(2, dst_data.size());
        EXPECT_EQ(1234500000000L, dst_data[0].value);  // 12345 scaled to 123.45000000
        EXPECT_EQ(-6789000000000L, dst_data[1].value); // -67890 scaled to -678.90000000
    }

    // Test DECIMAL64 -> DECIMAL256 (from medium decimal to large decimal)
    {
        TypeDescriptor src_type(TYPE_DECIMAL64);
        src_type.precision = 18;
        src_type.scale = 4;

        auto dst_type = std::make_shared<DataTypeDecimal<Decimal256>>(76, 35);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal<Decimal64>::create(18, 4);
        src_col->resize(0);
        auto& src_data = src_col->get_data();

        // Add test values
        src_data.push_back(Decimal64(12345678901234));  // Normal value: 1234567890.1234
        src_data.push_back(Decimal64(-98765432109876)); // Negative value: -9876543210.9876

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnDecimal<Decimal256>&>(*mutable_dst).get_data();
        ASSERT_EQ(2, dst_data.size());
        // Verify data
        EXPECT_EQ("1234567890.12340000000000000000000000000000000",
                  dst_data[0].to_string(76, 35)); // Scaled correctly
        EXPECT_EQ("-9876543210.98760000000000000000000000000000000", dst_data[1].to_string(76, 35));
    }

    // Test DECIMAL -> INT (with potential precision loss)
    {
        TypeDescriptor src_type(TYPE_DECIMAL32);
        src_type.precision = 9;
        src_type.scale = 2;

        auto dst_type = std::make_shared<DataTypeInt8>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal<Decimal32>::create(9, 2);
        auto& src_data = src_col->get_data();
        src_data.resize(0);
        src_data.push_back(Decimal32(12345));  // 123.45
        src_data.push_back(Decimal32(-12345)); // -123.45
        src_data.push_back(Decimal32(23345));  // Too large 233.45

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();
        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnInt8&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_data.size(), 0);

        ASSERT_EQ(3, src_data.size());
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(3, nested_col.size());
        EXPECT_EQ(123, nested_col.get_data()[0]);  // Truncated to 123
        EXPECT_EQ(-123, nested_col.get_data()[1]); // Truncated to -123
        for (int i = 0; i < 2; i++) {
            EXPECT_EQ(0, null_map[i]);
        }
        EXPECT_EQ(1, null_map[2]); // Should be null due to overflow
    }
    // TEST INT -> DECIMAL
    {
        TypeDescriptor src_type(TYPE_INT);
        auto dst_type = std::make_shared<DataTypeDecimal<Decimal64>>(10, 2);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt32::create();
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.resize(0);
        src_data.push_back(12345);  // 123.45 after scaling
        src_data.push_back(-67890); // -678.90 after scaling
        src_data.push_back(0);      // Zero check

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& dst_data = static_cast<ColumnDecimal<Decimal64>&>(*mutable_dst).get_data();
        ASSERT_EQ(3, dst_data.size());
        EXPECT_EQ(1234500, dst_data[0].value);  // 1234500 represents 123.45
        EXPECT_EQ(-6789000, dst_data[1].value); // -6789000 represents -678.90
        EXPECT_EQ(0, dst_data[2].value);        // Zero remains zero
    }

    // TEST DECIMAL64 -> DECIMAL32 (narrowing) (1)
    {
        TypeDescriptor src_type(TYPE_DECIMAL64);
        src_type.precision = 18;
        src_type.scale = 4;

        auto dst_type = std::make_shared<DataTypeDecimal<Decimal32>>(9, 4);
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal<Decimal64>::create(18, 4);
        auto& src_data = src_col->get_data();
        src_data.resize(0);
        // Add test values
        src_data.push_back(Decimal64(1234567890));  // In range
        src_data.push_back(Decimal64(999999999));   // Edge case: max for Decimal32
        src_data.push_back(Decimal64(1000000000));  // Out of range (overflow)
        src_data.push_back(Decimal64(-999999999));  // Edge case: negative max for Decimal32
        src_data.push_back(Decimal64(-1000000000)); // Out of range (underflow)

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_data.size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_FALSE(st.ok());
    }

    // TEST DECIMAL64 -> DECIMAL32 (narrowing) (2)
    {
        TypeDescriptor src_type(TYPE_DECIMAL64);
        src_type.precision = 18;
        src_type.scale = 4;

        auto dst_type = std::make_shared<DataTypeDecimal<Decimal32>>(9, 4);
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal<Decimal64>::create(18, 4);
        auto& src_data = src_col->get_data();
        // Add test values
        src_data.resize(0);
        src_data.push_back(Decimal64(123456789));  // In range
        src_data.push_back(Decimal64(999999999));  // Edge case: max for Decimal32
        src_data.push_back(Decimal64(-999999999)); // Edge case: negative max for Decimal32
        ASSERT_EQ(3, src_data.size());
        auto dst_col = nullable_dst_type->create_column();
        dst_col->resize(0);
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnDecimal<Decimal32>&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_data.size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(3, nested_col.size());
        EXPECT_EQ(123456789, nested_col.get_data()[0].value);  // Valid conversion
        EXPECT_EQ(999999999, nested_col.get_data()[1].value);  // Valid edge case
        EXPECT_EQ(-999999999, nested_col.get_data()[2].value); // Valid negative edge case

        for (int i = 0; i < 3; i++) {
            EXPECT_EQ(0, null_map[i]);
        }
    }

    // TEST FLOAT -> DECIMAL
    {
        TypeDescriptor src_type(TYPE_FLOAT);
        auto dst_type = std::make_shared<DataTypeDecimal<Decimal64>>(10, 2);
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnFloat32::create();
        auto& src_data = src_col->get_data();
        // Add test values
        src_data.resize(0);
        src_data.push_back(123.45f);                           // Normal value
        src_data.push_back(-678.90f);                          // Negative value
        src_data.push_back(std::numeric_limits<float>::max()); // Overflow (too large for Decimal64)
        src_data.push_back(0.0f);                              // Zero value
        src_data.push_back(std::numeric_limits<float>::infinity());  // Infinity
        src_data.push_back(std::numeric_limits<float>::quiet_NaN()); // NaN

        auto dst_col = nullable_dst_type->create_column();
        dst_col->resize(0);
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnDecimal<Decimal64>&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_data.size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(6, nested_col.size());
        EXPECT_EQ(12345, nested_col.get_data()[0].value);  // 123.45 scaled to 12345
        EXPECT_EQ(-67890, nested_col.get_data()[1].value); // -678.90 scaled to -67890
        for (int i = 0; i < 2; i++) {
            EXPECT_EQ(0, null_map[i]);
        }
        EXPECT_EQ(0, null_map[3]);
        EXPECT_EQ(1, null_map[2]);                    // Overflow: value too large
        EXPECT_EQ(0, nested_col.get_data()[3].value); // Zero remains zero
        EXPECT_EQ(1, null_map[4]);                    // Infinity should be null
        EXPECT_EQ(1, null_map[5]);                    // NaN should be null
    }

    // TEST STRING -> DECIMAL
    {
        TypeDescriptor src_type(TYPE_STRING);
        auto dst_type = std::make_shared<DataTypeDecimal<Decimal64>>(10, 2);
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnString::create();
        // Add test strings
        src_col->resize(0);
        src_col->insert_data("123.45", 6);         // Normal string
        src_col->insert_data("-678.90", 7);        // Negative value string
        src_col->insert_data("1e20", 4);           // Out of range for Decimal64
        src_col->insert_data("abc", 3);            // Invalid format
        src_col->insert_data("", 0);               // Empty string
        src_col->insert_data("0.0", 3);            // Zero value
        src_col->insert_data("9999999999.99", 13); // Edge case: max valid value within precision

        auto dst_col = nullable_dst_type->create_column();
        dst_col->resize(0);
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnDecimal<Decimal64>&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(7, nested_col.size());
        EXPECT_EQ(12345, nested_col.get_data()[0].value);  // "123.45" -> 12345
        EXPECT_EQ(-67890, nested_col.get_data()[1].value); // "-678.90" -> -67890
        EXPECT_EQ(1, null_map[2]);                         // Out of range -> null
        EXPECT_EQ(1, null_map[3]);                         // Invalid format -> null
        EXPECT_EQ(1, null_map[4]);                         // Empty string -> null
        EXPECT_EQ(0, nested_col.get_data()[5].value);      // "0.0" -> 0
        EXPECT_EQ(1, null_map[6]);                         // Edge case: maximum valid conversion
        for (int i = 0; i < 2; i++) {
            EXPECT_EQ(0, null_map[i]);
        }
        EXPECT_EQ(0, null_map[5]);
    }
}

// Test string type conversions
TEST_F(ColumnTypeConverterTest, TestStringConversions) {
    // Test numeric to string conversions
    {// INT -> STRING
     {TypeDescriptor src_type(TYPE_INT);
    auto dst_type = std::make_shared<DataTypeString>();

    auto converter =
            converter::ColumnTypeConverter::get_converter(src_type, dst_type, converter::COMMON);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto src_col = ColumnInt32::create();
    auto& src_data = src_col->get_data();
    src_data.push_back(std::numeric_limits<int32_t>::max());
    src_data.push_back(std::numeric_limits<int32_t>::min());
    src_data.push_back(0);

    auto dst_col = dst_type->create_column();
    auto mutable_dst = dst_col->assume_mutable();

    Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
    ASSERT_TRUE(st.ok());

    auto& string_col = static_cast<ColumnString&>(*mutable_dst);
    ASSERT_EQ(3, string_col.size());
    EXPECT_EQ(std::to_string(std::numeric_limits<int32_t>::max()),
              string_col.get_data_at(0).to_string());
    EXPECT_EQ(std::to_string(std::numeric_limits<int32_t>::min()),
              string_col.get_data_at(1).to_string());
    EXPECT_EQ("0", string_col.get_data_at(2).to_string());
}

// DOUBLE -> STRING
{
    TypeDescriptor src_type(TYPE_DOUBLE);
    auto dst_type = std::make_shared<DataTypeString>();

    auto converter =
            converter::ColumnTypeConverter::get_converter(src_type, dst_type, converter::COMMON);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto src_col = ColumnFloat64::create();
    auto& src_data = src_col->get_data();
    src_data.push_back(3.14159265359);
    src_data.push_back(-2.71828182846);
    src_data.push_back(std::numeric_limits<double>::infinity());
    src_data.push_back(std::numeric_limits<double>::quiet_NaN());

    auto dst_col = dst_type->create_column();
    auto mutable_dst = dst_col->assume_mutable();

    Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
    ASSERT_TRUE(st.ok());

    auto& string_col = static_cast<ColumnString&>(*mutable_dst);
    ASSERT_EQ(4, string_col.size());
    // Note: Exact string representation may vary by platform
    EXPECT_TRUE(string_col.get_data_at(0).to_string().find("3.14159") == 0);
    EXPECT_TRUE(string_col.get_data_at(1).to_string().find("-2.71828") == 0);
    EXPECT_TRUE(string_col.get_data_at(2).to_string().find("inf") != std::string::npos);
    EXPECT_TRUE(string_col.get_data_at(3).to_string().find("nan") != std::string::npos);
}
} // namespace doris::vectorized

// Test string to numeric conversions with invalid input
{
    TypeDescriptor src_type(TYPE_STRING);
    auto dst_type = std::make_shared<DataTypeInt32>();
    auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

    auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                   converter::COMMON);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto src_col = ColumnString::create();
    src_col->resize(0);
    src_col->insert_data("42", 2);
    src_col->insert_data("not a number", 11);
    src_col->insert_data("2147483648", 10); // Greater than INT32_MAX

    auto dst_col = nullable_dst_type->create_column();
    auto mutable_dst = dst_col->assume_mutable();
    auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
    auto& nested_col = static_cast<ColumnInt32&>(nullable_col.get_nested_column());
    auto& null_map = nullable_col.get_null_map_data();
    null_map.resize_fill(src_col->size(), 0);
    Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(3, nested_col.size());
    EXPECT_EQ(42, nested_col.get_data()[0]);
    EXPECT_EQ(1, null_map[1]); // Invalid format
    EXPECT_EQ(1, null_map[2]); // Out of range
    EXPECT_EQ(0, null_map[0]);
}
// TEST DECIMAL -> STRING
{
    TypeDescriptor src_type(TYPE_DECIMAL32);
    src_type.precision = 9;
    src_type.scale = 2;

    auto dst_type = std::make_shared<DataTypeString>();

    auto converter =
            converter::ColumnTypeConverter::get_converter(src_type, dst_type, converter::COMMON);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto src_col = ColumnDecimal<Decimal32>::create(9, 2);
    auto& src_data = src_col->get_data();
    // Add test values
    src_data.resize(0);
    src_data.push_back(Decimal32(12345));  // 123.45
    src_data.push_back(Decimal32(-67890)); // -678.90
    src_data.push_back(Decimal32(0));      // Zero

    auto dst_col = dst_type->create_column();
    dst_col->resize(0);
    auto mutable_dst = dst_col->assume_mutable();

    Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
    ASSERT_TRUE(st.ok());

    auto& string_col = static_cast<ColumnString&>(*mutable_dst);
    ASSERT_EQ(3, string_col.size());
    EXPECT_EQ("123.45", string_col.get_data_at(0).to_string());  // 123.45
    EXPECT_EQ("-678.90", string_col.get_data_at(1).to_string()); // -678.90
    EXPECT_EQ("0.00", string_col.get_data_at(2).to_string());    // Zero value
}

// TEST DATE/TIMESTAMP -> STRING
{
    TypeDescriptor src_type(TYPE_DATEV2);
    auto dst_type = std::make_shared<DataTypeString>();

    auto converter =
            converter::ColumnTypeConverter::get_converter(src_type, dst_type, converter::COMMON);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto src_col = ColumnDateV2::create();
    auto& src_data = src_col->get_data();
    // Add test date values
    src_data.resize(0);
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(2021, 1, 1, 0, 0, 0);
    src_data.push_back(
            *reinterpret_cast<vectorized::UInt32*>(&value)); // "2021-01-01" in days format
    value.unchecked_set_time(1970, 1, 1, 0, 0, 0);
    src_data.push_back(*reinterpret_cast<vectorized::UInt32*>(
            &value)); // "1970-01-01" in days format (epoch start)
    value.unchecked_set_time(2070, 1, 1, 0, 0, 0);
    src_data.push_back(
            *reinterpret_cast<vectorized::UInt32*>(&value)); // "2070-01-01" in days format

    auto dst_col = dst_type->create_column();
    dst_col->resize(0);
    auto mutable_dst = dst_col->assume_mutable();

    Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
    ASSERT_TRUE(st.ok());

    auto& string_col = static_cast<ColumnString&>(*mutable_dst);
    ASSERT_EQ(3, string_col.size());
    EXPECT_EQ("2021-01-01", string_col.get_data_at(0).to_string());
    EXPECT_EQ("1970-01-01", string_col.get_data_at(1).to_string());
    EXPECT_EQ("2070-01-01", string_col.get_data_at(2).to_string());
}

// TEST BOOLEAN -> STRING
{
    TypeDescriptor src_type(TYPE_BOOLEAN);
    auto dst_type = std::make_shared<DataTypeString>();

    auto converter =
            converter::ColumnTypeConverter::get_converter(src_type, dst_type, converter::COMMON);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto src_col = ColumnUInt8::create();
    auto& src_data = src_col->get_data();
    src_data.resize(0);
    // Add boolean values
    src_data.push_back(1); // true
    src_data.push_back(0); // false
    src_data.push_back(1); // true
    src_data.push_back(0); // false

    auto dst_col = dst_type->create_column();
    auto mutable_dst = dst_col->assume_mutable();

    Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
    ASSERT_TRUE(st.ok());

    auto& string_col = static_cast<ColumnString&>(*mutable_dst);
    ASSERT_EQ(4, string_col.size());
    EXPECT_EQ("TRUE", string_col.get_data_at(0).to_string());  // true
    EXPECT_EQ("FALSE", string_col.get_data_at(1).to_string()); // false
    EXPECT_EQ("TRUE", string_col.get_data_at(2).to_string());  // true
    EXPECT_EQ("FALSE", string_col.get_data_at(3).to_string()); // false
}

// TEST STRING -> BOOLEAN (for ORC file format, Apache Hive behavior)
{
    TypeDescriptor src_type(TYPE_STRING);
    auto dst_type = std::make_shared<DataTypeUInt8>(); // BOOLEAN represented as UInt8
    auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

    auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                   converter::ORC);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto src_col = ColumnString::create();
    // Add test strings
    src_col->resize(0);
    src_col->insert_data("0", 1);             // Hive: false
    src_col->insert_data("123", 3);           // Hive: true
    src_col->insert_data("-1", 2);            // Hive: true
    src_col->insert_data(" ", 1);             // Hive: null
    src_col->insert_data("not_a_number", 13); // Hive: null
    src_col->insert_data("1.5", 3);           // Hive: null (not an integer)
    src_col->insert_data("", 0);              // Hive: null

    auto dst_col = nullable_dst_type->create_column();
    auto mutable_dst = dst_col->assume_mutable();

    auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
    auto& nested_col = static_cast<ColumnUInt8&>(
            nullable_col.get_nested_column()); // Boolean as UInt8 (0 or 1)
    auto& null_map = nullable_col.get_null_map_data();
    null_map.resize_fill(src_col->size(), 0);

    // Perform conversion
    Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(7, nested_col.size());
    EXPECT_EQ(0, nested_col.get_data()[0]); // "0" -> false (0)
    EXPECT_EQ(1, nested_col.get_data()[1]); // "123" -> true (1)
    EXPECT_EQ(1, nested_col.get_data()[2]); // "-1" -> true (1)
    EXPECT_EQ(1, null_map[3]);              // " " -> null
    EXPECT_EQ(1, null_map[4]);              // "not_a_number" -> null
    EXPECT_EQ(1, null_map[5]);              // "1.5" -> null
    EXPECT_EQ(1, null_map[6]);              // "" -> null

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(0, null_map[i]);
    }
}
}

TEST_F(ColumnTypeConverterTest, TestUnsupportedConversions) {
    {
        std::vector<std::pair<PrimitiveType, PrimitiveType>> unsupported_conversions = {

                {TYPE_BOOLEAN, TYPE_TINYINT},
                {TYPE_BOOLEAN, TYPE_SMALLINT},
                {TYPE_BOOLEAN, TYPE_INT},
                {TYPE_BOOLEAN, TYPE_BIGINT},
                {TYPE_BOOLEAN, TYPE_FLOAT},
                {TYPE_BOOLEAN, TYPE_DOUBLE},
                {TYPE_BOOLEAN, TYPE_DATE},
                {TYPE_BOOLEAN, TYPE_DATEV2},
                {TYPE_BOOLEAN, TYPE_TIMEV2},
                {TYPE_BOOLEAN, TYPE_DATETIME},
                {TYPE_BOOLEAN, TYPE_DATETIMEV2},

                {TYPE_TINYINT, TYPE_BOOLEAN},
                {TYPE_SMALLINT, TYPE_BOOLEAN},
                {TYPE_INT, TYPE_BOOLEAN},
                {TYPE_BIGINT, TYPE_BOOLEAN},

                {TYPE_TINYINT, TYPE_DATE},
                {TYPE_SMALLINT, TYPE_DATE},
                {TYPE_INT, TYPE_DATE},
                {TYPE_BIGINT, TYPE_DATE},
                {TYPE_TINYINT, TYPE_DATEV2},
                {TYPE_SMALLINT, TYPE_DATEV2},
                {TYPE_INT, TYPE_DATEV2},
                {TYPE_BIGINT, TYPE_DATEV2},
                {TYPE_TINYINT, TYPE_DATETIME},
                {TYPE_SMALLINT, TYPE_DATETIME},
                {TYPE_INT, TYPE_DATETIME},
                {TYPE_BIGINT, TYPE_DATETIME},
                {TYPE_TINYINT, TYPE_DATETIMEV2},
                {TYPE_SMALLINT, TYPE_DATETIMEV2},
                {TYPE_INT, TYPE_DATETIMEV2},
                {TYPE_BIGINT, TYPE_DATETIMEV2},
                {TYPE_TINYINT, TYPE_TIMEV2},
                {TYPE_SMALLINT, TYPE_TIMEV2},
                {TYPE_INT, TYPE_TIMEV2},
                {TYPE_BIGINT, TYPE_TIMEV2},

                {TYPE_FLOAT, TYPE_BOOLEAN},
                {TYPE_FLOAT, TYPE_INT},
                {TYPE_FLOAT, TYPE_SMALLINT},
                {TYPE_FLOAT, TYPE_TINYINT},
                {TYPE_FLOAT, TYPE_BIGINT},
                {TYPE_FLOAT, TYPE_DATE},
                {TYPE_FLOAT, TYPE_DATEV2},
                {TYPE_FLOAT, TYPE_TIMEV2},
                {TYPE_FLOAT, TYPE_DATETIME},
                {TYPE_FLOAT, TYPE_DATETIMEV2},

                {TYPE_DOUBLE, TYPE_BOOLEAN},
                {TYPE_DOUBLE, TYPE_INT},
                {TYPE_DOUBLE, TYPE_SMALLINT},
                {TYPE_DOUBLE, TYPE_TINYINT},
                {TYPE_DOUBLE, TYPE_BIGINT},
                {TYPE_DOUBLE, TYPE_DATE},
                {TYPE_DOUBLE, TYPE_DATEV2},
                {TYPE_DOUBLE, TYPE_TIMEV2},
                {TYPE_DOUBLE, TYPE_DATETIME},
                {TYPE_DOUBLE, TYPE_DATETIMEV2},

                {TYPE_DOUBLE, TYPE_FLOAT},

                {TYPE_DATE, TYPE_BOOLEAN},
                {TYPE_DATE, TYPE_TINYINT},
                {TYPE_DATE, TYPE_SMALLINT},
                {TYPE_DATE, TYPE_INT},
                {TYPE_DATE, TYPE_BIGINT},
                {TYPE_DATE, TYPE_FLOAT},
                {TYPE_DATE, TYPE_DOUBLE},
                {TYPE_DATEV2, TYPE_BOOLEAN},
                {TYPE_DATEV2, TYPE_TINYINT},
                {TYPE_DATEV2, TYPE_SMALLINT},
                {TYPE_DATEV2, TYPE_INT},
                {TYPE_DATEV2, TYPE_BIGINT},
                {TYPE_DATEV2, TYPE_FLOAT},
                {TYPE_DATEV2, TYPE_DOUBLE},
                {TYPE_TIMEV2, TYPE_BOOLEAN},
                {TYPE_TIMEV2, TYPE_TINYINT},
                {TYPE_TIMEV2, TYPE_SMALLINT},
                {TYPE_TIMEV2, TYPE_INT},
                {TYPE_TIMEV2, TYPE_BIGINT},
                {TYPE_TIMEV2, TYPE_FLOAT},
                {TYPE_TIMEV2, TYPE_DOUBLE},
                {TYPE_DATETIME, TYPE_BOOLEAN},
                {TYPE_DATETIME, TYPE_TINYINT},
                {TYPE_DATETIME, TYPE_SMALLINT},
                {TYPE_DATETIME, TYPE_INT},
                {TYPE_DATETIME, TYPE_BIGINT},
                {TYPE_DATETIME, TYPE_FLOAT},
                {TYPE_DATETIME, TYPE_DOUBLE},
                {TYPE_DATETIMEV2, TYPE_BOOLEAN},
                {TYPE_DATETIMEV2, TYPE_TINYINT},
                {TYPE_DATETIMEV2, TYPE_SMALLINT},
                {TYPE_DATETIMEV2, TYPE_INT},
                {TYPE_DATETIMEV2, TYPE_BIGINT},
                {TYPE_DATETIMEV2, TYPE_FLOAT},
                {TYPE_DATETIMEV2, TYPE_DOUBLE},
        };

        for (const auto& [src_type_enum, dst_type_enum] : unsupported_conversions) {
            TypeDescriptor src_type(src_type_enum);
            for (auto len : {-1, 1, 2}) {
                TypeDescriptor dst_type(dst_type_enum);
                dst_type.len = len;
                auto converter = converter::ColumnTypeConverter::get_converter(
                        src_type, DataTypeFactory::instance().create_data_type(dst_type, false),
                        converter::COMMON);

                ASSERT_FALSE(converter->support())
                        << "Conversion from " << src_type.debug_string() << " to "
                        << dst_type.debug_string() << " should not be supported";
            }
        }
    }
    //to decimal
    {
        std::vector<std::pair<PrimitiveType, PrimitiveType>> unsupported_conversions = {
                {TYPE_BOOLEAN, TYPE_DECIMAL32},  {TYPE_DATE, TYPE_DECIMAL32},
                {TYPE_DATEV2, TYPE_DECIMAL32},   {TYPE_TIMEV2, TYPE_DECIMAL32},
                {TYPE_DATETIME, TYPE_DECIMAL32}, {TYPE_DATETIMEV2, TYPE_DECIMAL32},
        };

        for (const auto& [src_type_enum, dst_type_enum] : unsupported_conversions) {
            TypeDescriptor src_type(src_type_enum);

            for (int precision = min_decimal_precision();
                 precision <= BeConsts::MAX_DECIMAL256_PRECISION; precision++) {
                for (int scale = 0; scale <= precision; scale++) {
                    TypeDescriptor dst_type(dst_type_enum);
                    dst_type.precision = precision;
                    dst_type.scale = scale;
                    auto converter = converter::ColumnTypeConverter::get_converter(
                            src_type, DataTypeFactory::instance().create_data_type(dst_type, false),
                            converter::COMMON);

                    ASSERT_FALSE(converter->support())
                            << "Conversion from " << src_type.debug_string() << " to "
                            << dst_type.debug_string() << " should not be supported";
                }
            }
        }
    }

    //from decimal
    {
        std::vector<std::pair<PrimitiveType, PrimitiveType>> unsupported_conversions = {
                {TYPE_DECIMAL32, TYPE_BOOLEAN},  {TYPE_DECIMAL32, TYPE_DATE},
                {TYPE_DECIMAL32, TYPE_DATEV2},   {TYPE_DECIMAL32, TYPE_TIMEV2},
                {TYPE_DECIMAL32, TYPE_DATETIME}, {TYPE_DECIMAL32, TYPE_DATETIMEV2},
        };

        for (const auto& [src_type_enum, dst_type_enum] : unsupported_conversions) {
            TypeDescriptor src_type(src_type_enum);

            for (int precision = min_decimal_precision();
                 precision <= BeConsts::MAX_DECIMAL256_PRECISION; precision++) {
                for (int scale = 0; scale <= precision; scale++) {
                    src_type.precision = precision;
                    src_type.scale = scale;
                    auto decimal_date_type =
                            DataTypeFactory::instance().create_data_type(src_type, false);

                    TypeDescriptor dst_type(dst_type_enum);
                    auto converter = converter::ColumnTypeConverter::get_converter(
                            decimal_date_type->get_type_as_type_descriptor(),
                            DataTypeFactory::instance().create_data_type(dst_type, false),
                            converter::COMMON);

                    ASSERT_FALSE(converter->support())
                            << "Conversion from " << src_type.debug_string() << " to "
                            << dst_type.debug_string() << " should not be supported";
                }
            }
        }
    }
}

TEST_F(ColumnTypeConverterTest, TestEmptyColumnConversions) {
    // Test empty column
    {
        TypeDescriptor src_type(TYPE_INT);
        auto dst_type = std::make_shared<DataTypeFloat32>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt32::create(); // Empty column (no data)
        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();
        src_col->resize(0);
        dst_col->resize(0);
        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        // Check size remains zero
        ASSERT_EQ(0, static_cast<ColumnFloat32&>(*mutable_dst).get_data().size());
    }
}

} // namespace doris::vectorized