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

#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "olap/olap_common.h"
#include "olap/types.h"
#include "runtime/define_primitive_type.h"
#include "util/binary_cast.hpp"
#include "vec/core/field.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

class ToOlapStringTest : public testing::Test {
public:
    ToOlapStringTest() = default;
    ~ToOlapStringTest() override = default;
};

// ========================= Test: CHAR type with padding =========================
TEST_F(ToOlapStringTest, char_type_with_padding) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_CHAR, 0, 0, 20);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_CHAR);

    // Case 1: "hello" in CHAR(20) - 5 chars followed by 15 null bytes
    {
        char buf[20];
        memset(buf, 0, sizeof(buf));
        memcpy(buf, "hello", 5);
        Slice olap_value(buf, 20);

        // Expected: "hello" + 15 null bytes (raw CHAR representation preserves padding)
        std::string expected("hello", 5);
        expected.append(15, '\0');
        std::string expected_serde = expected;

        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_CHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected, type_info_str) << "TypeInfo mismatch for CHAR(20) 'hello'"
                                           << "\n  expected len=" << expected.size()
                                           << "\n  actual   len=" << type_info_str.size();
        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for CHAR(20) 'hello'"
                                             << "\n  expected len=" << expected_serde.size()
                                             << "\n  actual   len=" << serde_str.size();
    }

    // Case 2: String exactly filling CHAR(20)
    {
        char buf[20];
        memset(buf, 'x', 20);
        Slice olap_value(buf, 20);

        std::string expected(20, 'x'); // "xxxxxxxxxxxxxxxxxxxx"
        std::string expected_serde = expected;

        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_CHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected, type_info_str) << "TypeInfo mismatch for CHAR(20) filled 'x'";
        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for CHAR(20) filled 'x'";
    }

    // Case 3: Empty CHAR(20) - all null bytes
    {
        char buf[20];
        memset(buf, 0, 20);
        Slice olap_value(buf, 20);

        std::string expected(20, '\0');
        std::string expected_serde = expected;

        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_CHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected, type_info_str) << "TypeInfo mismatch for CHAR(20) empty"
                                           << "\n  expected len=" << expected.size()
                                           << "\n  actual   len=" << type_info_str.size();
        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for CHAR(20) empty"
                                             << "\n  expected len=" << expected_serde.size()
                                             << "\n  actual   len=" << serde_str.size();
    }
}

// ========================= Test: VARCHAR type =========================
TEST_F(ToOlapStringTest, varchar_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_VARCHAR, 0, 0, 100);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR);

    struct TestCase {
        std::string input;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {"hello world", "hello world", "hello world"},
            {"", "", ""},
    };

    for (auto& tc : test_cases) {
        Slice olap_value(tc.input.data(), tc.input.size());

        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_VARCHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for VARCHAR '" << tc.input << "'";
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for VARCHAR '" << tc.input << "'";
    }
}

// ========================= Test: DATE (V1) type =========================
// Format: strftime "%Y-%m-%d"
TEST_F(ToOlapStringTest, date_v1_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATE, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DATE);

    // DATE V1 olap storage: uint24_t = year * 16 * 32 + month * 32 + day
    auto make_olap_date = [](int year, int mon, int day) -> uint24_t {
        return uint24_t(year * 16 * 32 + mon * 32 + day);
    };

    struct TestCase {
        int year, month, day;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {2023, 6, 15, "2023-06-15", "2023-06-15"},
            {2000, 1, 1, "2000-01-01", "2000-01-01"},
            {9999, 12, 31, "9999-12-31", "9999-12-31"},
            {1, 1, 1, "0001-01-01", "0001-01-01"},
    };

    for (auto& tc : test_cases) {
        uint24_t olap_value = make_olap_date(tc.year, tc.month, tc.day);

        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DATE>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for DATE " << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for DATE " << tc.expected;
    }
}

// ========================= Test: DATETIME (V1) type =========================
// Format: strftime "%Y-%m-%d %H:%M:%S" (no sub-second precision)
TEST_F(ToOlapStringTest, datetime_v1_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIME, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DATETIME);

    struct TestCase {
        int64_t olap_value;         // YYYYMMDDHHMMSS
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {20230615120000L, "2023-06-15 12:00:00", "2023-06-15 12:00:00"},
            {20000101000000L, "2000-01-01 00:00:00", "2000-01-01 00:00:00"},
            {99991231235959L, "9999-12-31 23:59:59", "9999-12-31 23:59:59"},
            {20230615123456L, "2023-06-15 12:34:56", "2023-06-15 12:34:56"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DATETIME>(
                (uint64_t)tc.olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for DATETIME " << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for DATETIME " << tc.expected;
    }
}

// ========================= Test: DATEV2 type =========================
// Format: "YYYY-MM-DD"
TEST_F(ToOlapStringTest, datev2_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATEV2, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DATEV2);

    // DateV2 storage: (year << 9) | (month << 5) | day as uint32_t
    auto make_datev2 = [](int year, int month, int day) -> uint32_t {
        return (year << 9) | (month << 5) | day;
    };

    struct TestCase {
        int year, month, day;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {2023, 6, 15, "2023-06-15", "2023-06-15"},
            {2000, 1, 1, "2000-01-01", "2000-01-01"},
            {9999, 12, 31, "9999-12-31", "9999-12-31"},
            {1, 1, 1, "0001-01-01", "0001-01-01"},
    };

    for (auto& tc : test_cases) {
        uint32_t olap_value = make_datev2(tc.year, tc.month, tc.day);

        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DATEV2>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for DATEV2 " << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for DATEV2 " << tc.expected;
    }
}

// ========================= Test: DATETIMEV2 type =========================
// TypeInfo always uses to_string(scale=6): always prints ".ffffff" (6 fractional digits)
// Serde uses from_datetimev2(value, scale=-1): prints fractional part only when microsecond > 0
// This means zero-microsecond cases WILL differ: TypeInfo produces ".000000", serde omits it.
TEST_F(ToOlapStringTest, datetimev2_type) {
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DATETIMEV2);

    // DateTimeV2 storage layout (uint64_t):
    //   year(18) | month(4) | day(5) | hour(5) | minute(6) | second(6) | microsecond(20)
    auto make_datetimev2 = [](int year, int month, int day, int hour, int minute, int second,
                              int microsecond) -> uint64_t {
        return ((uint64_t)year << 46) | ((uint64_t)month << 42) | ((uint64_t)day << 37) |
               ((uint64_t)hour << 32) | ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
               (uint64_t)microsecond;
    };

    // TypeInfo always prints with scale=6, expected always has 6 fractional digits
    // Serde uses from_datetimev2(value, scale=-1): omits fractional part when microsecond==0
    struct TestCase {
        uint64_t olap_value;
        std::string expected;       // TypeInfo::to_string (always scale=6)
        std::string expected_serde; // serde::to_olap_string (scale=-1, omits .000000)
        std::string desc;
    };
    std::vector<TestCase> test_cases = {
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 123456), "2023-06-15 12:34:56.123456",
             "2023-06-15 12:34:56.123456", "non-zero microseconds"},
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 0), "2023-06-15 12:34:56.000000",
             "2023-06-15 12:34:56", "zero microseconds"},
            {make_datetimev2(2023, 1, 1, 0, 0, 0, 123000), "2023-01-01 00:00:00.123000",
             "2023-01-01 00:00:00.123000", "trailing zeros in microseconds"},
            {make_datetimev2(2000, 1, 1, 0, 0, 0, 0), "2000-01-01 00:00:00.000000",
             "2000-01-01 00:00:00", "epoch zero microseconds"},
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 1), "2023-06-15 12:34:56.000001",
             "2023-06-15 12:34:56.000001", "1 microsecond"},
            {make_datetimev2(9999, 12, 31, 23, 59, 59, 999999), "9999-12-31 23:59:59.999999",
             "9999-12-31 23:59:59.999999", "max datetime"},
    };

    // Verify TypeInfo produces the expected string (always scale=6)
    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.olap_value);
        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for DATETIMEV2: " << tc.desc;
    }

    // Verify serde produces the expected string
    // serde calls from_datetimev2(value) with default scale=-1:
    //   - microsecond > 0: prints 6 fractional digits → matches TypeInfo
    //   - microsecond == 0: omits fractional part → DIFFERS from TypeInfo
    for (int scale = 0; scale <= 6; ++scale) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2,
                                                                                  false, 0, scale);
        auto serde = data_type->get_serde();

        for (auto& tc : test_cases) {
            auto field =
                    vectorized::Field::create_field_from_olap_value<TYPE_DATETIMEV2>(tc.olap_value);
            std::string serde_str = serde->to_olap_string(field);

            EXPECT_EQ(tc.expected_serde, serde_str)
                    << "serde mismatch for DATETIMEV2 scale=" << scale << ": " << tc.desc
                    << "\n  expected: " << tc.expected_serde << "\n  serde:    " << serde_str;
        }
    }
}

// ========================= Test: DATETIME V1 vs DATETIMEV2 precision difference =========================
TEST_F(ToOlapStringTest, datetime_v1_vs_v2_precision_difference) {
    // DATETIME V1: no sub-second precision
    {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIME, false);
        auto serde = data_type->get_serde();
        const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DATETIME);

        int64_t olap_value = 20230615123456L;
        std::string expected =
                "2023-06-15 12:34:56"; // No fractional seconds        std::string expected_serde = expected;
        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DATETIME>(
                (uint64_t)olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected, type_info_str) << "TypeInfo mismatch for DATETIME V1";
        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for DATETIME V1";
        EXPECT_EQ(expected.find('.'), std::string::npos)
                << "DATETIME V1 should NOT have fractional seconds";
    }

    // DATETIMEV2: has sub-second precision (TypeInfo always scale=6)
    {
        auto make_datetimev2 = [](int year, int month, int day, int hour, int minute, int second,
                                  int microsecond) -> uint64_t {
            return ((uint64_t)year << 46) | ((uint64_t)month << 42) | ((uint64_t)day << 37) |
                   ((uint64_t)hour << 32) | ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                   (uint64_t)microsecond;
        };

        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2,
                                                                                  false, 0, 6);
        auto serde = data_type->get_serde();
        const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DATETIMEV2);

        uint64_t olap_value = make_datetimev2(2023, 6, 15, 12, 34, 56, 123456);
        std::string expected =
                "2023-06-15 12:34:56.123456"; // With fractional seconds        std::string expected_serde = expected;
        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DATETIMEV2>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected, type_info_str) << "TypeInfo mismatch for DATETIMEV2";
        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for DATETIMEV2";
        EXPECT_NE(expected.find('.'), std::string::npos)
                << "DATETIMEV2 should have fractional seconds";
    }
}

// ========================= Test: DECIMALV2 (decimal12_t) =========================
// decimal12_t::to_string() always prints "integer.%09u(fraction)" — 9 fractional digits
TEST_F(ToOlapStringTest, decimalv2_type) {
    auto data_type =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_DECIMALV2, false, 27, 9);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DECIMAL);

    struct TestCase {
        int64_t integer;
        int32_t fraction;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, 0, "0.000000000", "0.000000000"},
            {1, 0, "1.000000000", "1.000000000"},
            {0, 100000000, "0.100000000", "0.100000000"},
            {123, 456000000, "123.456000000", "123.456000000"},
            {-123, -456000000, "-123.456000000", "-123.456000000"},
            {999999999999999999L, 999999999, "999999999999999999.999999999",
             "999999999999999999.999999999"},
            {-999999999999999999L, -999999999, "-999999999999999999.999999999",
             "-999999999999999999.999999999"},
            {1, 1, "1.000000001", "1.000000001"},
            {1, 10, "1.000000010", "1.000000010"},
            {1, 100, "1.000000100", "1.000000100"},
            {1, 1000, "1.000001000", "1.000001000"},
            {1, 10000, "1.000010000", "1.000010000"},
            {1, 100000, "1.000100000", "1.000100000"},
            {1, 1000000, "1.001000000", "1.001000000"},
            {1, 10000000, "1.010000000", "1.010000000"},
            {1, 100000000, "1.100000000", "1.100000000"},
            {0, 123456789, "0.123456789", "0.123456789"},
            {42, 500000000, "42.500000000", "42.500000000"},
    };

    for (auto& tc : test_cases) {
        decimal12_t olap_value;
        olap_value.integer = tc.integer;
        olap_value.fraction = tc.fraction;

        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DECIMALV2>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for DECIMALV2 (" << tc.integer << ", " << tc.fraction << ")";
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMALV2 (" << tc.integer << ", " << tc.fraction << ")";
    }
}

// ========================= Test: DECIMAL32 (DecimalV3) =========================
// Both TypeInfo and serde output raw integer via std::to_string (scale NOT applied)
TEST_F(ToOlapStringTest, decimal32_type) {
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DECIMAL32);

    struct TestCase {
        int32_t value;
        int precision;
        int scale;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, 9, 0, "0", "0"},
            {12345, 9, 0, "12345", "12345"},
            {12345, 9, 2, "12345", "12345"},    // NOT "123.45" — raw integer
            {12345, 9, 4, "12345", "12345"},    // NOT "1.2345" — raw integer
            {-12345, 9, 2, "-12345", "-12345"}, // NOT "-123.45" — raw integer
            {1, 9, 9, "1", "1"},
            {999999999, 9, 0, "999999999", "999999999"},
            {-999999999, 9, 0, "-999999999", "-999999999"},
            {100000000, 9, 9, "100000000", "100000000"},
    };

    for (auto& tc : test_cases) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL32, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        int32_t olap_value = tc.value;
        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DECIMAL32>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for DECIMAL32 value=" << tc.value;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL32 value=" << tc.value;
    }
}

// ========================= Test: DECIMAL64 (DecimalV3) =========================
TEST_F(ToOlapStringTest, decimal64_type) {
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DECIMAL64);

    struct TestCase {
        int64_t value;
        int precision;
        int scale;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, 18, 0, "0", "0"},
            {123456789012345678L, 18, 0, "123456789012345678", "123456789012345678"},
            {123456789012345678L, 18, 6, "123456789012345678", "123456789012345678"}, // raw integer
            {-123456789012345678L, 18, 6, "-123456789012345678", "-123456789012345678"},
            {1, 18, 18, "1", "1"},
            {100000, 18, 5, "100000", "100000"},
            {1000000000000L, 18, 6, "1000000000000", "1000000000000"},
    };

    for (auto& tc : test_cases) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL64, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        int64_t olap_value = tc.value;
        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DECIMAL64>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for DECIMAL64 value=" << tc.value;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL64 value=" << tc.value;
    }
}

// ========================= Test: DECIMAL128I (DecimalV3) =========================
// Both TypeInfo and serde use fmt::format("{}", int128_t)
TEST_F(ToOlapStringTest, decimal128i_type) {
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DECIMAL128I);

    struct TestCase {
        int128_t value;
        int precision;
        int scale;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, 38, 0, "0", "0"},
            {123456789, 38, 0, "123456789", "123456789"},
            {123456789, 38, 6, "123456789", "123456789"}, // raw integer
            {-123456789, 38, 6, "-123456789", "-123456789"},
            {1, 38, 38, "1", "1"},
            // 999999999999999999 * 1e9 + 999999999 = 999999999999999999999999999 (27 nines)
            {(int128_t)999999999999999999L * 1000000000L + 999999999, 38, 9,
             "999999999999999999999999999", "999999999999999999999999999"},
    };

    for (auto& tc : test_cases) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL128I, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        int128_t olap_value = tc.value;
        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DECIMAL128I>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for DECIMAL128I expected=" << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL128I expected=" << tc.expected;
    }
}

// ========================= Test: DECIMAL256 (DecimalV3) =========================
// Both TypeInfo and serde use wide::to_string
TEST_F(ToOlapStringTest, decimal256_type) {
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DECIMAL256);

    struct TestCase {
        wide::Int256 value;
        int precision;
        int scale;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {wide::Int256(0), 76, 0, "0", "0"},
            {wide::Int256(123456789), 76, 0, "123456789", "123456789"},
            {wide::Int256(123456789), 76, 6, "123456789", "123456789"}, // raw integer
            {wide::Int256(-123456789), 76, 6, "-123456789", "-123456789"},
    };

    for (auto& tc : test_cases) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL256, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        wide::Int256 olap_value = tc.value;
        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field_from_olap_value<TYPE_DECIMAL256>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for DECIMAL256 expected=" << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL256 expected=" << tc.expected;
    }
}

// ========================= Test: FLOAT type with special values =========================
// Both TypeInfo and serde use CastToString::from_number → _fast_to_buffer:
//   NaN → "NaN", +Infinity → "Infinity", -Infinity → "-Infinity"
//   Normal values: fmt::format("{:.7g}", value) — 7 significant digits, trailing zeros removed
TEST_F(ToOlapStringTest, float_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_FLOAT);

    struct TestCase {
        float value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            // Normal values (exactly representable in float, {:.7g} output is predictable)
            {0.0f, "0", "0"},
            {-0.0f, "-0", "-0"},
            {1.0f, "1", "1"},
            {-1.0f, "-1", "-1"},
            {0.5f, "0.5", "0.5"},
            {1.5f, "1.5", "1.5"},
            {0.25f, "0.25", "0.25"},
            {100.0f, "100", "100"},
            {0.001f, "0.001", "0.001"},

            // Special values
            {std::numeric_limits<float>::quiet_NaN(), "NaN", "NaN"},
            {std::numeric_limits<float>::infinity(), "Infinity", "Infinity"},
            {-std::numeric_limits<float>::infinity(), "-Infinity", "-Infinity"},

            // Extreme values ({:.7g}: 7 significant digits, scientific notation)
            {std::numeric_limits<float>::max(), "3.402823e+38", "3.402823e+38"},
            {std::numeric_limits<float>::lowest(), "-3.402823e+38", "-3.402823e+38"},
            {std::numeric_limits<float>::min(), "1.175494e-38", "1.175494e-38"},
            {std::numeric_limits<float>::denorm_min(), "1.401298e-45", "1.401298e-45"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_FLOAT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for FLOAT expected='" << tc.expected << "'";
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for FLOAT expected='" << tc.expected << "'";
    }
}

// ========================= Test: DOUBLE type with special values =========================
// Both TypeInfo and serde use CastToString::from_number → _fast_to_buffer:
//   NaN → "NaN", +Infinity → "Infinity", -Infinity → "-Infinity"
//   Normal values: fmt::format("{:.16g}", value) — 16 significant digits
TEST_F(ToOlapStringTest, double_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_DOUBLE);

    struct TestCase {
        double value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            // Normal values (exactly representable, predictable {:.16g} output)
            {0.0, "0", "0"},
            {-0.0, "-0", "-0"},
            {1.0, "1", "1"},
            {-1.0, "-1", "-1"},
            {0.5, "0.5", "0.5"},
            {1.5, "1.5", "1.5"},
            {0.25, "0.25", "0.25"},
            {100.0, "100", "100"},
            {3.141592653589793, "3.141592653589793", "3.141592653589793"},
            {0.001, "0.001", "0.001"},

            // Special values
            {std::numeric_limits<double>::quiet_NaN(), "NaN", "NaN"},
            {std::numeric_limits<double>::infinity(), "Infinity", "Infinity"},
            {-std::numeric_limits<double>::infinity(), "-Infinity", "-Infinity"},

            // Extreme values ({:.16g}: 16 significant digits)
            {std::numeric_limits<double>::max(), "1.797693134862316e+308",
             "1.797693134862316e+308"},
            {std::numeric_limits<double>::lowest(), "-1.797693134862316e+308",
             "-1.797693134862316e+308"},
            {std::numeric_limits<double>::min(), "2.225073858507201e-308",
             "2.225073858507201e-308"},
            {std::numeric_limits<double>::denorm_min(), "5e-324", "5e-324"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_DOUBLE>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for DOUBLE expected='" << tc.expected << "'";
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DOUBLE expected='" << tc.expected << "'";
    }
}

// ========================= Test: BOOL type =========================
// TypeInfo: snprintf("%d", bool) → "0" or "1"
// Serde:   snprintf("%d", uint8_t) → "0" or "1"
TEST_F(ToOlapStringTest, bool_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_BOOL);

    struct TestCase {
        uint8_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_BOOLEAN>((bool)tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for BOOL=" << (int)tc.value;
        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for BOOL=" << (int)tc.value;
    }
}

// ========================= Test: TINYINT type =========================
// TypeInfo: std::to_string(int8_t) — Serde: CastToString::from_number → fmt::format_int
TEST_F(ToOlapStringTest, tinyint_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_TINYINT, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_TINYINT);

    struct TestCase {
        int8_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},       {1, "1", "1"},          {-1, "-1", "-1"},
            {127, "127", "127"}, {-128, "-128", "-128"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_TINYINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for TINYINT=" << (int)tc.value;
        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for TINYINT=" << (int)tc.value;
    }
}

// ========================= Test: SMALLINT type =========================
TEST_F(ToOlapStringTest, smallint_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_SMALLINT);

    struct TestCase {
        int16_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
            {-1, "-1", "-1"},
            {32767, "32767", "32767"},
            {-32768, "-32768", "-32768"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_SMALLINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for SMALLINT=" << tc.value;
        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for SMALLINT=" << tc.value;
    }
}

// ========================= Test: INT type =========================
TEST_F(ToOlapStringTest, int_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_INT);

    struct TestCase {
        int32_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
            {-1, "-1", "-1"},
            {2147483647, "2147483647", "2147483647"},
            {-2147483648, "-2147483648", "-2147483648"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_INT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str) << "TypeInfo mismatch for INT=" << tc.value;
        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for INT=" << tc.value;
    }
}

// ========================= Test: BIGINT type =========================
TEST_F(ToOlapStringTest, bigint_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_BIGINT);

    struct TestCase {
        int64_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
            {-1, "-1", "-1"},
            {9223372036854775807L, "9223372036854775807", "9223372036854775807"},
            {-9223372036854775807L - 1, "-9223372036854775808", "-9223372036854775808"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_BIGINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for BIGINT expected=" << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for BIGINT expected=" << tc.expected;
    }
}

// ========================= Test: LARGEINT type =========================
// TypeInfo: custom snprintf (divides by 10^19 for large values)
// Serde:   fmt::format("{}", int128_t)
TEST_F(ToOlapStringTest, largeint_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_LARGEINT);

    struct TestCase {
        int128_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {(int128_t)0, "0", "0"},
            {(int128_t)1, "1", "1"},
            {(int128_t)-1, "-1", "-1"},
            {(int128_t)9223372036854775807L, "9223372036854775807", "9223372036854775807"},
            {(int128_t)(-9223372036854775807L - 1), "-9223372036854775808", "-9223372036854775808"},
            {~((int128_t)(1) << 127), "170141183460469231731687303715884105727",
             "170141183460469231731687303715884105727"}, // max int128
            {(int128_t)(1) << 127, "-170141183460469231731687303715884105728",
             "-170141183460469231731687303715884105728"}, // min int128
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_LARGEINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for LARGEINT expected=" << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for LARGEINT expected=" << tc.expected;
    }
}

// ========================= Test: IPV4 type =========================
// Both use inet_ntop-style formatting
TEST_F(ToOlapStringTest, ipv4_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_IPV4, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_IPV4);

    struct TestCase {
        uint32_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {0, "0.0.0.0", "0.0.0.0"},
            {0xFFFFFFFF, "255.255.255.255", "255.255.255.255"},
            {0x7F000001, "127.0.0.1", "127.0.0.1"},
            {0xC0A80001, "192.168.0.1", "192.168.0.1"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.value);
        auto field = vectorized::Field::create_field<TYPE_IPV4>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for IPV4 expected=" << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for IPV4 expected=" << tc.expected;
    }
}

// ========================= Test: IPV6 type =========================
TEST_F(ToOlapStringTest, ipv6_type) {
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_IPV6, false);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_IPV6);

    struct TestCase {
        uint128_t value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {(uint128_t)0, "::", "::"},
            {(uint128_t)1, "::1", "::1"},
            {(uint128_t)(-1), "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
             "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
    };

    for (auto& tc : test_cases) {
        uint128_t olap_value = tc.value;
        std::string type_info_str = type_info->to_string(&olap_value);
        auto field = vectorized::Field::create_field<TYPE_IPV6>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for IPV6 expected=" << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for IPV6 expected=" << tc.expected;
    }
}

// ========================= Test: TIMESTAMPTZ type =========================
// TypeInfo: TimestampTzValue::to_string(cctz::utc_time_zone(), 6) → "YYYY-MM-DD HH:MM:SS.ffffff+00:00"
// Serde:   CastToString::from_timestamptz(value, 6) → same format with UTC
TEST_F(ToOlapStringTest, timestamptz_type) {
    auto data_type =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6);
    auto serde = data_type->get_serde();
    const TypeInfo* type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ);

    auto make_datetimev2 = [](int year, int month, int day, int hour, int minute, int second,
                              int microsecond) -> uint64_t {
        return ((uint64_t)year << 46) | ((uint64_t)month << 42) | ((uint64_t)day << 37) |
               ((uint64_t)hour << 32) | ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
               (uint64_t)microsecond;
    };

    struct TestCase {
        uint64_t olap_value;
        std::string expected;       // TypeInfo::to_string
        std::string expected_serde; // serde::to_olap_string
    };
    std::vector<TestCase> test_cases = {
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 123456), "2023-06-15 12:34:56.123456+00:00",
             "2023-06-15 12:34:56.123456+00:00"},
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 0), "2023-06-15 12:34:56.000000+00:00",
             "2023-06-15 12:34:56.000000+00:00"},
            {make_datetimev2(2000, 1, 1, 0, 0, 0, 0), "2000-01-01 00:00:00.000000+00:00",
             "2000-01-01 00:00:00.000000+00:00"},
    };

    for (auto& tc : test_cases) {
        std::string type_info_str = type_info->to_string(&tc.olap_value);
        auto field =
                vectorized::Field::create_field_from_olap_value<TYPE_TIMESTAMPTZ>(tc.olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected, type_info_str)
                << "TypeInfo mismatch for TIMESTAMPTZ expected=" << tc.expected;
        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for TIMESTAMPTZ expected=" << tc.expected;
    }
}

} // namespace doris
