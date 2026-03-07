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

#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2 {

class InvertedIndexQueryParamTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// ==================== Integer Types Tests ====================

TEST_F(InvertedIndexQueryParamTest, TestBooleanWithField) {
    auto field = Field::create_field<TYPE_BOOLEAN>(static_cast<UInt8>(1));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_BOOLEAN,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
    const auto* value = static_cast<const bool*>(param->get_value());
    EXPECT_EQ(*value, true);
}

TEST_F(InvertedIndexQueryParamTest, TestBooleanWithFieldFalse) {
    auto field = Field::create_field<TYPE_BOOLEAN>(static_cast<UInt8>(0));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_BOOLEAN,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const bool*>(param->get_value());
    EXPECT_EQ(*value, false);
}

TEST_F(InvertedIndexQueryParamTest, TestBooleanTemplateWithNativeValue) {
    bool input_value = true;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_BOOLEAN, bool>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const bool*>(param->get_value());
    EXPECT_EQ(*value, true);
}

TEST_F(InvertedIndexQueryParamTest, TestTinyIntWithField) {
    auto field = Field::create_field<TYPE_TINYINT>(static_cast<Int8>(42));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_TINYINT,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int8_t*>(param->get_value());
    EXPECT_EQ(*value, 42);
}

TEST_F(InvertedIndexQueryParamTest, TestTinyIntTemplateWithNativeValue) {
    int8_t input_value = -100;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_TINYINT, int8_t>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int8_t*>(param->get_value());
    EXPECT_EQ(*value, -100);
}

TEST_F(InvertedIndexQueryParamTest, TestSmallIntWithField) {
    auto field =
            Field::create_field<TYPE_SMALLINT>(static_cast<Int16>(1234));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_SMALLINT,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int16_t*>(param->get_value());
    EXPECT_EQ(*value, 1234);
}

TEST_F(InvertedIndexQueryParamTest, TestSmallIntTemplateWithNativeValue) {
    int16_t input_value = -32000;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_SMALLINT, int16_t>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int16_t*>(param->get_value());
    EXPECT_EQ(*value, -32000);
}

TEST_F(InvertedIndexQueryParamTest, TestIntWithField) {
    auto field = Field::create_field<TYPE_INT>(static_cast<Int32>(123456));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_INT,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int32_t*>(param->get_value());
    EXPECT_EQ(*value, 123456);
}

TEST_F(InvertedIndexQueryParamTest, TestIntTemplateWithNativeValue) {
    int32_t input_value = -2147483647;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_INT, int32_t>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int32_t*>(param->get_value());
    EXPECT_EQ(*value, -2147483647);
}

TEST_F(InvertedIndexQueryParamTest, TestBigIntWithField) {
    auto field = Field::create_field<TYPE_BIGINT>(
            static_cast<Int64>(9223372036854775807LL));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_BIGINT,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int64_t*>(param->get_value());
    EXPECT_EQ(*value, 9223372036854775807LL);
}

TEST_F(InvertedIndexQueryParamTest, TestBigIntTemplateWithNativeValue) {
    int64_t input_value = -9223372036854775807LL;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_BIGINT, int64_t>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const int64_t*>(param->get_value());
    EXPECT_EQ(*value, -9223372036854775807LL);
}

TEST_F(InvertedIndexQueryParamTest, TestLargeIntWithField) {
    Int128 large_value = 12345678901234567890ULL;
    auto field = Field::create_field<TYPE_LARGEINT>(large_value);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_LARGEINT,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const __int128_t*>(param->get_value());
    EXPECT_EQ(*value, static_cast<__int128_t>(large_value));
}

TEST_F(InvertedIndexQueryParamTest, TestLargeIntTemplateWithNativeValue) {
    __int128_t input_value = 12345678901234567890ULL;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_LARGEINT, __int128_t>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const __int128_t*>(param->get_value());
    EXPECT_EQ(*value, input_value);
}

// ==================== Float/Double Types Tests ====================

TEST_F(InvertedIndexQueryParamTest, TestFloatWithField) {
    auto field =
            Field::create_field<TYPE_FLOAT>(static_cast<Float32>(3.14f));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_FLOAT,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const float*>(param->get_value());
    EXPECT_FLOAT_EQ(*value, 3.14f);
}

TEST_F(InvertedIndexQueryParamTest, TestFloatTemplateWithNativeValue) {
    float input_value = -1.23456f;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_FLOAT, float>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const float*>(param->get_value());
    EXPECT_FLOAT_EQ(*value, -1.23456f);
}

TEST_F(InvertedIndexQueryParamTest, TestDoubleWithField) {
    auto field = Field::create_field<TYPE_DOUBLE>(
            static_cast<Float64>(3.14159265358979));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DOUBLE,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const double*>(param->get_value());
    EXPECT_DOUBLE_EQ(*value, 3.14159265358979);
}

TEST_F(InvertedIndexQueryParamTest, TestDoubleTemplateWithNativeValue) {
    double input_value = -9.87654321e10;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_DOUBLE, double>(
            &input_value, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const double*>(param->get_value());
    EXPECT_DOUBLE_EQ(*value, -9.87654321e10);
}

// ==================== String Types Tests ====================

TEST_F(InvertedIndexQueryParamTest, TestCharWithField) {
    String str = "hello";
    auto field = Field::create_field<TYPE_STRING>(str);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_CHAR,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "hello");
}

TEST_F(InvertedIndexQueryParamTest, TestVarcharWithField) {
    String str = "world";
    auto field = Field::create_field<TYPE_STRING>(str);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_VARCHAR,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "world");
}

TEST_F(InvertedIndexQueryParamTest, TestStringWithField) {
    String str = "test string content";
    auto field = Field::create_field<TYPE_STRING>(str);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_STRING,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "test string content");
}

TEST_F(InvertedIndexQueryParamTest, TestStringTemplateWithStringRef) {
    std::string str_data = "string ref test";
    StringRef str_ref(str_data.data(), str_data.size());
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_STRING, StringRef>(
            &str_ref, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "string ref test");
}

TEST_F(InvertedIndexQueryParamTest, TestVarcharTemplateWithStringRef) {
    std::string str_data = "varchar ref test";
    StringRef str_ref(str_data.data(), str_data.size());
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_VARCHAR, StringRef>(
            &str_ref, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "varchar ref test");
}

TEST_F(InvertedIndexQueryParamTest, TestCharTemplateWithStringRef) {
    std::string str_data = "char ref test";
    StringRef str_ref(str_data.data(), str_data.size());
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_CHAR, StringRef>(&str_ref,
                                                                                           param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "char ref test");
}

TEST_F(InvertedIndexQueryParamTest, TestStringWithEmptyValue) {
    String str = "";
    auto field = Field::create_field<TYPE_STRING>(str);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_STRING,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "");
}

TEST_F(InvertedIndexQueryParamTest, TestStringWithSpecialCharacters) {
    String str = "hello\nworld\t!@#$%^&*()";
    auto field = Field::create_field<TYPE_STRING>(str);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_STRING,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "hello\nworld\t!@#$%^&*()");
}

// ==================== Decimal Types Tests ====================

TEST_F(InvertedIndexQueryParamTest, TestDecimalV2WithField) {
    // DecimalV2 uses Int128 as underlying storage
    Int128 dec_value = 123456789;
    auto field = Field::create_field<TYPE_DECIMALV2>(DecimalV2Value(dec_value));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DECIMALV2,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

TEST_F(InvertedIndexQueryParamTest, TestDecimal32WithField) {
    // Decimal32 uses Int64 for Field storage
    Int64 dec_value = 12345;
    auto field = Field::create_field<TYPE_DECIMAL32>(dec_value);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DECIMAL32,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

TEST_F(InvertedIndexQueryParamTest, TestDecimal64WithField) {
    // Decimal64 uses Int64 for Field storage
    Int64 dec_value = 123456789012;
    auto field = Field::create_field<TYPE_DECIMAL64>(dec_value);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DECIMAL64,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

TEST_F(InvertedIndexQueryParamTest, TestDecimal128IWithField) {
    // Decimal128I uses Int128 for Field storage
    Int128 dec_value = 123456789012345LL;
    auto field = Field::create_field<TYPE_DECIMAL128I>(dec_value);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(
            PrimitiveType::TYPE_DECIMAL128I, &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

TEST_F(InvertedIndexQueryParamTest, TestDecimal256WithField) {
    // Decimal256 uses Int128 for Field storage
    Int128 dec_value = 123456789012345LL;
    auto field = Field::create_field<TYPE_DECIMAL256>(
            Decimal<wide::Int256>(dec_value));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DECIMAL256,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

// ==================== Date/Time Types Tests ====================

TEST_F(InvertedIndexQueryParamTest, TestDateWithField) {
    VecDateTimeValue tmp;
    tmp.from_date_int64(20231205);
    auto field = Field::create_field<TYPE_DATE>(tmp);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DATE,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

TEST_F(InvertedIndexQueryParamTest, TestDateTimeWithField) {
    VecDateTimeValue tmp;
    tmp.create_from_olap_datetime(20231205120000LL);
    auto field = Field::create_field<TYPE_DATETIME>(tmp);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DATETIME,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

TEST_F(InvertedIndexQueryParamTest, TestDateV2WithField) {
    UInt64 v = 20231205;
    typename PrimitiveTypeTraits<TYPE_DATEV2>::CppType tmp;
    tmp.from_date_int64(v);
    auto field = Field::create_field<TYPE_DATEV2>(tmp);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DATEV2,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

TEST_F(InvertedIndexQueryParamTest, TestDateTimeV2WithField) {
    UInt64 v = 20231205120000LL;
    auto field = Field::create_field<TYPE_DATETIMEV2>(
            *(typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType*)&v);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DATETIMEV2,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(param, nullptr);
}

// ==================== IP Types Tests ====================

TEST_F(InvertedIndexQueryParamTest, TestIPv4WithField) {
    auto field = Field::create_field<TYPE_IPV4>(IPv4(3232235521)); // 192.168.0.1
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_IPV4,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const IPv4*>(param->get_value());
    EXPECT_EQ(*value, IPv4(3232235521));
}

TEST_F(InvertedIndexQueryParamTest, TestIPv4TemplateWithNativeValue) {
    IPv4 input_value(2130706433); // 127.0.0.1
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_IPV4, IPv4>(&input_value,
                                                                                      param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const IPv4*>(param->get_value());
    EXPECT_EQ(*value, IPv4(2130706433));
}

TEST_F(InvertedIndexQueryParamTest, TestIPv6WithField) {
    IPv6 ipv6_value = 1;
    auto field = Field::create_field<TYPE_IPV6>(ipv6_value);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_IPV6,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const IPv6*>(param->get_value());
    EXPECT_EQ(*value, ipv6_value);
}

TEST_F(InvertedIndexQueryParamTest, TestIPv6TemplateWithNativeValue) {
    IPv6 input_value = 12345678901234567890ULL;
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value<TYPE_IPV6, IPv6>(&input_value,
                                                                                      param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const IPv6*>(param->get_value());
    EXPECT_EQ(*value, input_value);
}

// ==================== Unsupported Type Test ====================

TEST_F(InvertedIndexQueryParamTest, TestUnsupportedType) {
    auto field = Field::create_field<TYPE_BIGINT>(static_cast<Int64>(0));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_ARRAY,
                                                                     &field, param);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
}

TEST_F(InvertedIndexQueryParamTest, TestUnsupportedTypeMap) {
    auto field = Field::create_field<TYPE_BIGINT>(static_cast<Int64>(0));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_MAP,
                                                                     &field, param);
    ASSERT_FALSE(status.ok());
}

TEST_F(InvertedIndexQueryParamTest, TestUnsupportedTypeStruct) {
    auto field = Field::create_field<TYPE_BIGINT>(static_cast<Int64>(0));
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_STRUCT,
                                                                     &field, param);
    ASSERT_FALSE(status.ok());
}

// ==================== Edge Cases Tests ====================

TEST_F(InvertedIndexQueryParamTest, TestIntegerBoundaryMin) {
    // Test minimum values
    {
        auto field =
                Field::create_field<TYPE_TINYINT>(static_cast<Int8>(-128));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(
                PrimitiveType::TYPE_TINYINT, &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const int8_t*>(param->get_value());
        EXPECT_EQ(*value, -128);
    }
    {
        auto field = Field::create_field<TYPE_SMALLINT>(
                static_cast<Int16>(-32768));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(
                PrimitiveType::TYPE_SMALLINT, &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const int16_t*>(param->get_value());
        EXPECT_EQ(*value, -32768);
    }
}

TEST_F(InvertedIndexQueryParamTest, TestIntegerBoundaryMax) {
    // Test maximum values
    {
        auto field =
                Field::create_field<TYPE_TINYINT>(static_cast<Int8>(127));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(
                PrimitiveType::TYPE_TINYINT, &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const int8_t*>(param->get_value());
        EXPECT_EQ(*value, 127);
    }
    {
        auto field = Field::create_field<TYPE_SMALLINT>(
                static_cast<Int16>(32767));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(
                PrimitiveType::TYPE_SMALLINT, &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const int16_t*>(param->get_value());
        EXPECT_EQ(*value, 32767);
    }
}

TEST_F(InvertedIndexQueryParamTest, TestZeroValues) {
    // Test zero values for different types
    {
        auto field = Field::create_field<TYPE_INT>(static_cast<Int32>(0));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_INT,
                                                                         &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const int32_t*>(param->get_value());
        EXPECT_EQ(*value, 0);
    }
    {
        auto field =
                Field::create_field<TYPE_DOUBLE>(static_cast<Float64>(0.0));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DOUBLE,
                                                                         &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const double*>(param->get_value());
        EXPECT_DOUBLE_EQ(*value, 0.0);
    }
}

TEST_F(InvertedIndexQueryParamTest, TestFloatSpecialValues) {
    // Test infinity
    {
        auto field = Field::create_field<TYPE_DOUBLE>(
                static_cast<Float64>(std::numeric_limits<double>::infinity()));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DOUBLE,
                                                                         &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const double*>(param->get_value());
        EXPECT_TRUE(std::isinf(*value));
    }
    // Test negative infinity
    {
        auto field = Field::create_field<TYPE_DOUBLE>(
                static_cast<Float64>(-std::numeric_limits<double>::infinity()));
        std::unique_ptr<InvertedIndexQueryParamFactory> param;
        auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_DOUBLE,
                                                                         &field, param);
        ASSERT_TRUE(status.ok());
        const auto* value = static_cast<const double*>(param->get_value());
        EXPECT_TRUE(std::isinf(*value));
        EXPECT_LT(*value, 0);
    }
}

TEST_F(InvertedIndexQueryParamTest, TestStringWithUnicodeCharacters) {
    String str = "你好世界 🌍 日本語";
    auto field = Field::create_field<TYPE_STRING>(str);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_STRING,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(*value, "你好世界 🌍 日本語");
}

TEST_F(InvertedIndexQueryParamTest, TestLongString) {
    std::string long_str(10000, 'x');
    String str(long_str);
    auto field = Field::create_field<TYPE_STRING>(str);
    std::unique_ptr<InvertedIndexQueryParamFactory> param;
    auto status = InvertedIndexQueryParamFactory::create_query_value(PrimitiveType::TYPE_STRING,
                                                                     &field, param);
    ASSERT_TRUE(status.ok());
    const auto* value = static_cast<const std::string*>(param->get_value());
    EXPECT_EQ(value->size(), 10000);
    EXPECT_EQ(*value, long_str);
}

} // namespace doris::segment_v2