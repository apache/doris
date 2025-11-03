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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_TINYINT, false);
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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, false);
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32,
                                                                                 false, 9, 2);

        auto dst_type = std::make_shared<DataTypeDecimal64>(18, 2);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal32::create(9, 2);
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.resize(0);
        src_data.push_back(Decimal32(12345));  // 123.45
        src_data.push_back(Decimal32(-12345)); // -123.45

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& dst_data = static_cast<ColumnDecimal64&>(*mutable_dst).get_data();
        ASSERT_EQ(2, dst_data.size());
        EXPECT_EQ(12345, dst_data[0].value);
        EXPECT_EQ(-12345, dst_data[1].value);
    }

    // Test DECIMAL32 -> DECIMAL128 (from small decimal to large decimal)
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32,
                                                                                 false, 9, 2);

        auto dst_type = std::make_shared<DataTypeDecimal128>(38, 10);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal32::create(9, 2);
        src_col->resize(0);
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.push_back(Decimal32(12345));  // 123.45
        src_data.push_back(Decimal32(-67890)); // -678.90

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnDecimal128V3&>(*mutable_dst).get_data();
        ASSERT_EQ(2, dst_data.size());
        EXPECT_EQ(1234500000000L, dst_data[0].value);  // 12345 scaled to 123.45000000
        EXPECT_EQ(-6789000000000L, dst_data[1].value); // -67890 scaled to -678.90000000
    }

    // Test DECIMAL64 -> DECIMAL256 (from medium decimal to large decimal)
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL64,
                                                                                 false, 18, 4);

        auto dst_type = std::make_shared<DataTypeDecimal256>(76, 35);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal64::create(18, 4);
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

        auto& dst_data = static_cast<ColumnDecimal256&>(*mutable_dst).get_data();
        ASSERT_EQ(2, dst_data.size());
        // Verify data
        EXPECT_EQ("1234567890.12340000000000000000000000000000000",
                  dst_data[0].to_string(76, 35)); // Scaled correctly
        EXPECT_EQ("-9876543210.98760000000000000000000000000000000", dst_data[1].to_string(76, 35));
    }

    // Test DECIMAL -> INT (with potential precision loss)
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32,
                                                                                 false, 9, 2);

        auto dst_type = std::make_shared<DataTypeInt8>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal32::create(9, 2);
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

    // Test DECIMAL -> INT (with potential precision loss)
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL128I,
                                                                                 false, 36, 4);

        auto dst_type = std::make_shared<DataTypeInt8>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal128V3::create(36, 4);
        auto& src_data = src_col->get_data();
        src_data.resize(0);
        src_data.push_back(Decimal128V3(102345));
        src_data.push_back(Decimal128V3(-102345));
        src_data.push_back(Decimal128V3(203345));

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
        EXPECT_EQ(10, nested_col.get_data()[0]);
        EXPECT_EQ(-10, nested_col.get_data()[1]);
        EXPECT_EQ(20, nested_col.get_data()[2]);
        for (int i = 0; i < 3; i++) {
            EXPECT_EQ(0, null_map[i]);
        }
    }

    // Test DECIMAL -> INT (with potential precision loss)
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL256,
                                                                                 false, 70, 4);

        auto dst_type = std::make_shared<DataTypeInt16>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal256::create(70, 4);
        auto& src_data = src_col->get_data();
        src_data.resize(0);
        src_data.push_back(Decimal256(-102345));
        src_data.push_back(Decimal256(203345));
        src_data.push_back(Decimal256(327673345));
        src_data.push_back(Decimal256(655353345));
        src_data.push_back(Decimal256(655363345));
        src_data.push_back(Decimal256(3333333333332345));

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();
        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnInt16&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_data.size(), 0);

        ASSERT_EQ(6, src_data.size());
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(6, nested_col.size());
        EXPECT_EQ(-10, nested_col.get_data()[0]);
        EXPECT_EQ(20, nested_col.get_data()[1]);
        EXPECT_EQ(32767, nested_col.get_data()[2]);
        for (int i = 0; i < 3; i++) {
            EXPECT_EQ(0, null_map[i]);
        }
        EXPECT_EQ(1, null_map[3]);
        EXPECT_EQ(1, null_map[4]);
        EXPECT_EQ(1, null_map[5]);
    }

    // TEST INT -> DECIMAL
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
        auto dst_type = std::make_shared<DataTypeDecimal64>(10, 2);

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
        auto& dst_data = static_cast<ColumnDecimal64&>(*mutable_dst).get_data();
        ASSERT_EQ(3, dst_data.size());
        EXPECT_EQ(1234500, dst_data[0].value);  // 1234500 represents 123.45
        EXPECT_EQ(-6789000, dst_data[1].value); // -6789000 represents -678.90
        EXPECT_EQ(0, dst_data[2].value);        // Zero remains zero
    }

    // TEST INT -> DECIMAL
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
        auto dst_type = std::make_shared<DataTypeDecimal128>(18, 5);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt32::create();
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.resize(0);
        src_data.push_back(12345);
        src_data.push_back(-67890);
        src_data.push_back(0);

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& dst_data = static_cast<ColumnDecimal128V3&>(*mutable_dst).get_data();
        ASSERT_EQ(3, dst_data.size());
        EXPECT_EQ(1234500000, dst_data[0].value);
        EXPECT_EQ(-6789000000, dst_data[1].value);
        EXPECT_EQ(0, dst_data[2].value);
    }

    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_TINYINT, false);
        auto dst_type = std::make_shared<DataTypeDecimal256>(38, 10);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt8::create();
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.resize(0);
        src_data.push_back(123);  // 123.45 after scaling
        src_data.push_back(-123); // -678.90 after scaling
        src_data.push_back(0);    // Zero check

        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& dst_data = static_cast<ColumnDecimal256&>(*mutable_dst).get_data();
        ASSERT_EQ(3, dst_data.size());
        EXPECT_EQ(1230000000000, dst_data[0].value);
        EXPECT_EQ(-1230000000000, dst_data[1].value);
        EXPECT_EQ(0, dst_data[2].value);
    }

    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_TINYINT, false);
        auto dst_type = std::make_shared<DataTypeDecimal256>(12, 10);
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnInt8::create();
        auto& src_data = src_col->get_data();
        // Test normal values
        src_data.resize(0);
        src_data.push_back(123);  // 123.45 after scaling
        src_data.push_back(-123); // -678.90 after scaling
        src_data.push_back(0);    // Zero check

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_data.size(), 0);
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& dst_data =
                static_cast<ColumnDecimal256&>(nullable_col.get_nested_column()).get_data();
        ASSERT_EQ(3, dst_data.size());
        ASSERT_EQ(1, null_map[0]);
        ASSERT_EQ(1, null_map[1]);
        ASSERT_EQ(0, null_map[2]);
        EXPECT_EQ(0, dst_data[2].value);
    }

    // TEST DECIMAL64 -> DECIMAL32 (narrowing) (1)
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL64,
                                                                                 false, 18, 4);

        auto dst_type = std::make_shared<DataTypeDecimal32>(9, 4);
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal64::create(18, 4);
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL64,
                                                                                 false, 18, 4);
        auto dst_type = std::make_shared<DataTypeDecimal32>(9, 4);
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal64::create(18, 4);
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
        auto& nested_col = static_cast<ColumnDecimal32&>(nullable_col.get_nested_column());
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
        auto dst_type = std::make_shared<DataTypeDecimal64>(10, 2);
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
        auto& nested_col = static_cast<ColumnDecimal64&>(nullable_col.get_nested_column());
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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
        auto dst_type = std::make_shared<DataTypeDecimal64>(10, 2);
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
        auto& nested_col = static_cast<ColumnDecimal64&>(nullable_col.get_nested_column());
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
    { // INT -> STRING
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
        auto dst_type = std::make_shared<DataTypeString>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
        auto dst_type = std::make_shared<DataTypeString>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
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
        EXPECT_TRUE(string_col.get_data_at(2).to_string().find("Infinity") != std::string::npos);
        EXPECT_TRUE(string_col.get_data_at(3).to_string().find("NaN") != std::string::npos);
    }

    // Test string to numeric conversions with invalid input
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
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
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32,
                                                                                 false, 9, 2);

        auto dst_type = std::make_shared<DataTypeString>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnDecimal32::create(9, 2);
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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATEV2, false);
        auto dst_type = std::make_shared<DataTypeString>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, false);
        auto dst_type = std::make_shared<DataTypeString>();

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);
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
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
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
TEST_F(ColumnTypeConverterTest, TestStringToIntegerTypes) {
    // 1. Test STRING -> TINYINT
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
        auto dst_type = std::make_shared<DataTypeInt8>(); // TINYINT represented as Int8
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnString::create();
        // Add test strings
        src_col->insert_data("123", 3);  // Valid value
        src_col->insert_data("-128", 4); // Min value for TINYINT
        src_col->insert_data("127", 3);  // Max value for TINYINT
        src_col->insert_data("128", 3);  // Overflow - should be NULL
        src_col->insert_data("-129", 4); // Underflow - should be NULL
        src_col->insert_data("abc", 3);  // Invalid - should be NULL
        src_col->insert_data("", 0);     // Empty - should be NULL

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnInt8&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(7, nested_col.size());
        EXPECT_EQ(123, nested_col.get_data()[0]);  // "123" -> 123
        EXPECT_EQ(-128, nested_col.get_data()[1]); // "-128" -> -128
        EXPECT_EQ(127, nested_col.get_data()[2]);  // "127" -> 127

        // Check NULL values for invalid inputs
        EXPECT_EQ(0, null_map[0]); // Valid
        EXPECT_EQ(0, null_map[1]); // Valid
        EXPECT_EQ(0, null_map[2]); // Valid
        EXPECT_EQ(1, null_map[3]); // Overflow -> NULL
        EXPECT_EQ(1, null_map[4]); // Underflow -> NULL
        EXPECT_EQ(1, null_map[5]); // Invalid -> NULL
        EXPECT_EQ(1, null_map[6]); // Empty -> NULL
    }

    // 2. Test STRING -> SMALLINT
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
        auto dst_type = std::make_shared<DataTypeInt16>(); // SMALLINT represented as Int16
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnString::create();
        // Add test strings
        src_col->insert_data("12345", 5);  // Valid value
        src_col->insert_data("-32768", 6); // Min value for SMALLINT
        src_col->insert_data("32767", 5);  // Max value for SMALLINT
        src_col->insert_data("32768", 5);  // Overflow - should be NULL
        src_col->insert_data("-32769", 6); // Underflow - should be NULL
        src_col->insert_data("123.45", 6); // Decimal - should be NULL

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnInt16&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(6, nested_col.size());
        EXPECT_EQ(12345, nested_col.get_data()[0]);  // "12345" -> 12345
        EXPECT_EQ(-32768, nested_col.get_data()[1]); // "-32768" -> -32768
        EXPECT_EQ(32767, nested_col.get_data()[2]);  // "32767" -> 32767

        // Check NULL values for invalid inputs
        EXPECT_EQ(0, null_map[0]); // Valid
        EXPECT_EQ(0, null_map[1]); // Valid
        EXPECT_EQ(0, null_map[2]); // Valid
        EXPECT_EQ(1, null_map[3]); // Overflow -> NULL
        EXPECT_EQ(1, null_map[4]); // Underflow -> NULL
        EXPECT_EQ(1, null_map[5]); // Decimal -> NULL
    }

    // 3. Test STRING -> INT
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
        auto dst_type = std::make_shared<DataTypeInt32>(); // INT represented as Int32
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnString::create();
        // Add test strings
        src_col->insert_data("2147483647", 10);  // Max value for INT
        src_col->insert_data("-2147483648", 11); // Min value for INT
        src_col->insert_data("0", 1);            // Zero
        src_col->insert_data("1000000", 7);      // Million
        src_col->insert_data("2147483648", 10);  // Overflow - should be NULL

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnInt32&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(5, nested_col.size());
        EXPECT_EQ(2147483647, nested_col.get_data()[0]);  // "2147483647" -> 2147483647
        EXPECT_EQ(-2147483648, nested_col.get_data()[1]); // "-2147483648" -> -2147483648
        EXPECT_EQ(0, nested_col.get_data()[2]);           // "0" -> 0
        EXPECT_EQ(1000000, nested_col.get_data()[3]);     // "1000000" -> 1000000

        // Check NULL values for invalid inputs
        EXPECT_EQ(0, null_map[0]); // Valid
        EXPECT_EQ(0, null_map[1]); // Valid
        EXPECT_EQ(0, null_map[2]); // Valid
        EXPECT_EQ(0, null_map[3]); // Valid
        EXPECT_EQ(1, null_map[4]); // Overflow -> NULL
    }

    // 4. Test STRING -> BIGINT
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
        auto dst_type = std::make_shared<DataTypeInt64>(); // BIGINT represented as Int64
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnString::create();
        // Add test strings
        src_col->insert_data("9223372036854775807", 19);  // Max value for BIGINT
        src_col->insert_data("-9223372036854775808", 20); // Min value for BIGINT
        src_col->insert_data("123456789012345", 15);      // Regular big number
        src_col->insert_data("9223372036854775808", 19);  // Overflow - should be NULL
        src_col->insert_data("123abc", 6);                // Invalid - should be NULL

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnInt64&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(5, nested_col.size());
        EXPECT_EQ(9223372036854775807LL, nested_col.get_data()[0]);   // Max value
        EXPECT_EQ(-9223372036854775808ULL, nested_col.get_data()[1]); // Min value
        EXPECT_EQ(123456789012345LL, nested_col.get_data()[2]);       // Regular big number

        // Check NULL values for invalid inputs
        EXPECT_EQ(0, null_map[0]); // Valid
        EXPECT_EQ(0, null_map[1]); // Valid
        EXPECT_EQ(0, null_map[2]); // Valid
        EXPECT_EQ(1, null_map[3]); // Overflow -> NULL
        EXPECT_EQ(1, null_map[4]); // Invalid -> NULL
    }

    // 5. Test STRING -> LARGEINT
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
        auto dst_type = std::make_shared<DataTypeInt128>(); // LARGEINT represented as Int128
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);

        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);
        ASSERT_TRUE(converter->support());
        ASSERT_FALSE(converter->is_consistent());

        auto src_col = ColumnString::create();
        // Add test strings
        src_col->insert_data("123456789012345678901234567890", 30);  // Valid large number
        src_col->insert_data("-123456789012345678901234567890", 31); // Negative large number
        src_col->insert_data("0", 1);                                // Zero
        src_col->insert_data("123e45", 6); // Scientific notation - should be NULL

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& nested_col = static_cast<ColumnInt128&>(nullable_col.get_nested_column());
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        // Perform conversion
        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(4, nested_col.size());
        EXPECT_EQ("123456789012345678901234567890", int128_to_string(nested_col.get_data()[0]));

        EXPECT_EQ("-123456789012345678901234567890", int128_to_string(nested_col.get_data()[1]));

        // Check zero
        EXPECT_EQ(0, nested_col.get_data()[2]);

        // Check NULL values for invalid inputs
        EXPECT_EQ(0, null_map[0]); // Valid
        EXPECT_EQ(0, null_map[1]); // Valid
        EXPECT_EQ(0, null_map[2]); // Valid
        EXPECT_EQ(1, null_map[3]); // Scientific notation -> NULL
    }
}

TEST_F(ColumnTypeConverterTest, TestUnsupportedConversions) {
    {
        std::vector<std::pair<PrimitiveType, PrimitiveType>> unsupported_conversions = {

                {TYPE_BOOLEAN, TYPE_TINYINT},    {TYPE_BOOLEAN, TYPE_SMALLINT},
                {TYPE_BOOLEAN, TYPE_INT},        {TYPE_BOOLEAN, TYPE_BIGINT},
                {TYPE_BOOLEAN, TYPE_FLOAT},      {TYPE_BOOLEAN, TYPE_DOUBLE},
                {TYPE_BOOLEAN, TYPE_DATE},       {TYPE_BOOLEAN, TYPE_DATEV2},
                {TYPE_BOOLEAN, TYPE_TIMEV2},     {TYPE_BOOLEAN, TYPE_DATETIME},
                {TYPE_BOOLEAN, TYPE_DATETIMEV2},

                {TYPE_TINYINT, TYPE_BOOLEAN},    {TYPE_SMALLINT, TYPE_BOOLEAN},
                {TYPE_INT, TYPE_BOOLEAN},        {TYPE_BIGINT, TYPE_BOOLEAN},

                {TYPE_TINYINT, TYPE_DATE},       {TYPE_SMALLINT, TYPE_DATE},
                {TYPE_INT, TYPE_DATE},           {TYPE_BIGINT, TYPE_DATE},
                {TYPE_TINYINT, TYPE_DATEV2},     {TYPE_SMALLINT, TYPE_DATEV2},
                {TYPE_INT, TYPE_DATEV2},         {TYPE_BIGINT, TYPE_DATEV2},
                {TYPE_TINYINT, TYPE_DATETIME},   {TYPE_SMALLINT, TYPE_DATETIME},
                {TYPE_INT, TYPE_DATETIME},       {TYPE_BIGINT, TYPE_DATETIME},
                {TYPE_TINYINT, TYPE_DATETIMEV2}, {TYPE_SMALLINT, TYPE_DATETIMEV2},
                {TYPE_INT, TYPE_DATETIMEV2},     {TYPE_BIGINT, TYPE_DATETIMEV2},
                {TYPE_TINYINT, TYPE_TIMEV2},     {TYPE_SMALLINT, TYPE_TIMEV2},
                {TYPE_INT, TYPE_TIMEV2},         {TYPE_BIGINT, TYPE_TIMEV2},

                {TYPE_FLOAT, TYPE_BOOLEAN},      {TYPE_FLOAT, TYPE_INT},
                {TYPE_FLOAT, TYPE_SMALLINT},     {TYPE_FLOAT, TYPE_TINYINT},
                {TYPE_FLOAT, TYPE_BIGINT},       {TYPE_FLOAT, TYPE_DATE},
                {TYPE_FLOAT, TYPE_DATEV2},       {TYPE_FLOAT, TYPE_TIMEV2},
                {TYPE_FLOAT, TYPE_DATETIME},     {TYPE_FLOAT, TYPE_DATETIMEV2},

                {TYPE_DOUBLE, TYPE_BOOLEAN},     {TYPE_DOUBLE, TYPE_INT},
                {TYPE_DOUBLE, TYPE_SMALLINT},    {TYPE_DOUBLE, TYPE_TINYINT},
                {TYPE_DOUBLE, TYPE_BIGINT},      {TYPE_DOUBLE, TYPE_DATE},
                {TYPE_DOUBLE, TYPE_DATEV2},      {TYPE_DOUBLE, TYPE_TIMEV2},
                {TYPE_DOUBLE, TYPE_DATETIME},    {TYPE_DOUBLE, TYPE_DATETIMEV2},

                {TYPE_DOUBLE, TYPE_FLOAT},

                {TYPE_DATE, TYPE_BOOLEAN},       {TYPE_DATE, TYPE_TINYINT},
                {TYPE_DATE, TYPE_SMALLINT},      {TYPE_DATE, TYPE_INT},
                {TYPE_DATE, TYPE_BIGINT},        {TYPE_DATE, TYPE_FLOAT},
                {TYPE_DATE, TYPE_DOUBLE},        {TYPE_DATEV2, TYPE_BOOLEAN},
                {TYPE_DATEV2, TYPE_TINYINT},     {TYPE_DATEV2, TYPE_SMALLINT},
                {TYPE_DATEV2, TYPE_INT},         {TYPE_DATEV2, TYPE_BIGINT},
                {TYPE_DATEV2, TYPE_FLOAT},       {TYPE_DATEV2, TYPE_DOUBLE},
                {TYPE_TIMEV2, TYPE_BOOLEAN},     {TYPE_TIMEV2, TYPE_TINYINT},
                {TYPE_TIMEV2, TYPE_SMALLINT},    {TYPE_TIMEV2, TYPE_INT},
                {TYPE_TIMEV2, TYPE_BIGINT},      {TYPE_TIMEV2, TYPE_FLOAT},
                {TYPE_TIMEV2, TYPE_DOUBLE},      {TYPE_DATETIME, TYPE_BOOLEAN},
                {TYPE_DATETIME, TYPE_TINYINT},   {TYPE_DATETIME, TYPE_SMALLINT},
                {TYPE_DATETIME, TYPE_INT},       {TYPE_DATETIME, TYPE_BIGINT},
                {TYPE_DATETIME, TYPE_FLOAT},     {TYPE_DATETIME, TYPE_DOUBLE},
                {TYPE_DATETIMEV2, TYPE_BOOLEAN}, {TYPE_DATETIMEV2, TYPE_FLOAT},
                {TYPE_DATETIMEV2, TYPE_DOUBLE},
        };

        for (const auto& [src_type_enum, dst_type_enum] : unsupported_conversions) {
            auto src_type =
                    vectorized::DataTypeFactory::instance().create_data_type(src_type_enum, false);
            auto dst_type = DataTypeFactory::instance().create_data_type(dst_type_enum, false);
            auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                           converter::COMMON);

            ASSERT_FALSE(converter->support())
                    << "Conversion from " << src_type->get_name() << " to " << dst_type->get_name()
                    << " should not be supported";
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
            auto src_type =
                    vectorized::DataTypeFactory::instance().create_data_type(src_type_enum, false);

            for (int precision = min_decimal_precision();
                 precision <= BeConsts::MAX_DECIMAL256_PRECISION; precision++) {
                for (int scale = 0; scale <= precision; scale++) {
                    auto dst_type = DataTypeFactory::instance().create_data_type(
                            dst_type_enum, false, precision, scale);
                    auto converter = converter::ColumnTypeConverter::get_converter(
                            src_type, dst_type, converter::COMMON);

                    ASSERT_FALSE(converter->support())
                            << "Conversion from " << src_type->get_name() << " to "
                            << dst_type->get_name() << " should not be supported";
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
            for (int precision = min_decimal_precision();
                 precision <= BeConsts::MAX_DECIMAL256_PRECISION; precision++) {
                for (int scale = 0; scale <= precision; scale++) {
                    auto src_type = vectorized::DataTypeFactory::instance().create_data_type(
                            src_type_enum, false, precision, scale);
                    auto decimal_date_type =
                            vectorized::DataTypeFactory::instance().create_data_type(
                                    src_type_enum, false, precision, scale);

                    auto dst_type =
                            DataTypeFactory::instance().create_data_type(dst_type_enum, false);
                    auto converter = converter::ColumnTypeConverter::get_converter(
                            decimal_date_type, dst_type, converter::COMMON);

                    ASSERT_FALSE(converter->support())
                            << "Conversion from " << src_type->get_name() << " to "
                            << dst_type->get_name() << " should not be supported";
                }
            }
        }
    }
}

TEST_F(ColumnTypeConverterTest, TestDateTimeV2ToNumericConversions) {
    using namespace doris::vectorized;

    auto make_datetimev2_col =
            [](const std::vector<std::tuple<int, int, int, int, int, int, int>>& datetimes) {
                auto col = ColumnDateTimeV2::create();
                for (const auto& [y, m, d, h, min, s, micro] : datetimes) {
                    DateV2Value<DateTimeV2ValueType> v;
                    v.unchecked_set_time(y, m, d, h, min, s, micro);
                    col->get_data().push_back(*reinterpret_cast<vectorized::UInt64*>(&v));
                }
                return col;
            };

    auto parse_datetimev2_str = [](const std::string& datetime_str) {
        UInt64 x = 0;
        StringRef buf((char*)datetime_str.data(), datetime_str.size());
        bool ok = read_datetime_v2_text_impl(x, buf, 6);
        CHECK(ok) << "parse_datetimev2_str failed for: " << datetime_str;
        return x;
    };

    // 1. DATETIMEV2 -> BIGINT
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
        auto dst_type = std::make_shared<DataTypeInt64>();
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);

        ASSERT_TRUE(converter->support());

        // 2024-01-01 00:00:00.123456
        auto src_col = make_datetimev2_col({{2024, 1, 1, 0, 0, 0, 123456}});
        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        auto& dst_data = static_cast<ColumnInt64&>(*mutable_dst).get_data();
        ASSERT_EQ(1, dst_data.size());
        EXPECT_EQ(1704067200123, dst_data[0]);
    }

    // 2. DATETIMEV2 -> INT
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
        auto dst_type = std::make_shared<DataTypeInt32>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);

        ASSERT_TRUE(converter->support());

        // 1970-01-01 00:00:00.000000
        // 3000-01-01 00:00:00.000000
        auto src_col = make_datetimev2_col({{1970, 1, 1, 0, 0, 0, 0}, {3000, 1, 1, 0, 0, 0, 0}});
        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();
        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());
        auto& nested_col = static_cast<ColumnInt32&>(nullable_col.get_nested_column());
        auto& dst_data = nested_col.get_data();

        ASSERT_EQ(2, nested_col.size());
        EXPECT_EQ(0, null_map[0]);
        ASSERT_EQ(0, dst_data[0]);
        EXPECT_EQ(1, null_map[1]);
    }

    // 3. DATETIMEV2 -> INT, non-nullable
    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
        auto dst_type = std::make_shared<DataTypeInt32>();
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_type,
                                                                       converter::COMMON);

        ASSERT_TRUE(converter->support());

        // 3000-01-01 00:00:00.000000（会溢出int32）
        auto src_col = make_datetimev2_col({{3000, 1, 1, 0, 0, 0, 0}});
        auto dst_col = dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_FALSE(st.ok());
    }

    {
        auto src_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
        auto dst_type = std::make_shared<DataTypeInt64>();
        auto nullable_dst_type = std::make_shared<DataTypeNullable>(dst_type);
        auto converter = converter::ColumnTypeConverter::get_converter(src_type, nullable_dst_type,
                                                                       converter::COMMON);

        ASSERT_TRUE(converter->support());

        auto src_col = ColumnDateTimeV2::create();
        src_col->get_data().push_back(parse_datetimev2_str("2024-01-01 12:34:56.123456"));
        src_col->get_data().push_back(parse_datetimev2_str("1970-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("3000-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("1900-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("1999-12-31 23:59:59.999999"));
        src_col->get_data().push_back(parse_datetimev2_str("2000-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("2025-07-08 16:00:00.123456"));
        src_col->get_data().push_back(parse_datetimev2_str("2100-01-01 00:00:00.000000"));
        src_col->get_data().push_back(parse_datetimev2_str("9999-12-31 23:59:59.999999"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 12:00:00.000001"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 13:00:00.000002"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 14:00:00.000004"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 12:00:00"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 13:00:00"));
        src_col->get_data().push_back(parse_datetimev2_str("2022-05-01 14:00:00"));

        auto dst_col = nullable_dst_type->create_column();
        auto mutable_dst = dst_col->assume_mutable();
        auto& nullable_col = static_cast<ColumnNullable&>(*mutable_dst);
        auto& null_map = nullable_col.get_null_map_data();
        null_map.resize_fill(src_col->size(), 0);

        Status st = converter->convert(reinterpret_cast<ColumnPtr&>(src_col), mutable_dst);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(15, null_map.size());
        EXPECT_EQ(0, null_map[0]);
        EXPECT_EQ(0, null_map[1]);
        EXPECT_EQ(0, null_map[2]);
        EXPECT_EQ(0, null_map[3]);
        EXPECT_EQ(0, null_map[4]);
        EXPECT_EQ(0, null_map[5]);
        EXPECT_EQ(0, null_map[6]);
        EXPECT_EQ(0, null_map[7]);
        EXPECT_EQ(0, null_map[8]);
        EXPECT_EQ(0, null_map[9]);
        EXPECT_EQ(0, null_map[10]);
        EXPECT_EQ(0, null_map[11]);
        EXPECT_EQ(0, null_map[12]);
        EXPECT_EQ(0, null_map[13]);
        EXPECT_EQ(0, null_map[14]);

        auto& dst_data = static_cast<ColumnInt64&>(nullable_col.get_nested_column()).get_data();
        ASSERT_EQ(15, dst_data.size());
        EXPECT_EQ(1704112496123L, dst_data[0]);
        EXPECT_EQ(0L, dst_data[1]);
        EXPECT_EQ(32503680000000L, dst_data[2]);
        EXPECT_EQ(-2208988800000L, dst_data[3]);
        EXPECT_EQ(946684799999L, dst_data[4]);
        EXPECT_EQ(946684800000L, dst_data[5]);
        EXPECT_EQ(1751990400123, dst_data[6]);
        EXPECT_EQ(4102444800000L, dst_data[7]);
        EXPECT_EQ(253402300799999, dst_data[8]);
        EXPECT_EQ(1651406400000, dst_data[9]);
        EXPECT_EQ(1651410000000, dst_data[10]);
        EXPECT_EQ(1651413600000, dst_data[11]);
        EXPECT_EQ(1651406400000, dst_data[12]);
        EXPECT_EQ(1651410000000, dst_data[13]);
        EXPECT_EQ(1651413600000, dst_data[14]);
    }
}

TEST_F(ColumnTypeConverterTest, TestEmptyColumnConversions) {
    // Test empty column
    {
        auto src_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
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