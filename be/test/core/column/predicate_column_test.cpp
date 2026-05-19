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

#include "core/column/predicate_column.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "common/status.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/decimal12.h"
#include "core/field.h"
#include "core/types.h"
#include "core/uint24.h"
#include "core/value/vdatetime_value.h"
#include "testutil/column_helper.h"

namespace doris {

// ============================================================================
// Test size() for all types
// ============================================================================
TEST(PredicateColumnTest, SizeBoolean) {
    auto col = PredicateColumnType<TYPE_BOOLEAN>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
    col->insert_default();
    EXPECT_EQ(col->size(), 2);
}

TEST(PredicateColumnTest, SizeTinyInt) {
    auto col = PredicateColumnType<TYPE_TINYINT>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeSmallInt) {
    auto col = PredicateColumnType<TYPE_SMALLINT>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeBigInt) {
    auto col = PredicateColumnType<TYPE_BIGINT>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeLargeInt) {
    auto col = PredicateColumnType<TYPE_LARGEINT>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeFloat) {
    auto col = PredicateColumnType<TYPE_FLOAT>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDouble) {
    auto col = PredicateColumnType<TYPE_DOUBLE>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDate) {
    auto col = PredicateColumnType<TYPE_DATE>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDateV2) {
    auto col = PredicateColumnType<TYPE_DATEV2>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDateTime) {
    auto col = PredicateColumnType<TYPE_DATETIME>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDateTimeV2) {
    auto col = PredicateColumnType<TYPE_DATETIMEV2>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeChar) {
    auto col = PredicateColumnType<TYPE_CHAR>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeVarchar) {
    auto col = PredicateColumnType<TYPE_VARCHAR>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDecimalV2) {
    auto col = PredicateColumnType<TYPE_DECIMALV2>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDecimal32) {
    auto col = PredicateColumnType<TYPE_DECIMAL32>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDecimal64) {
    auto col = PredicateColumnType<TYPE_DECIMAL64>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDecimal128I) {
    auto col = PredicateColumnType<TYPE_DECIMAL128I>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeDecimal256) {
    auto col = PredicateColumnType<TYPE_DECIMAL256>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeIPv4) {
    auto col = PredicateColumnType<TYPE_IPV4>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, SizeIPv6) {
    auto col = PredicateColumnType<TYPE_IPV6>::create();
    EXPECT_EQ(col->size(), 0);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

// ============================================================================
// Test insert_data() for all types
// ============================================================================
TEST(PredicateColumnTest, InsertDataBoolean) {
    auto col = PredicateColumnType<TYPE_BOOLEAN>::create();
    col->reserve(3);
    UInt8 vals[] = {0, 1, 1};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], 0);
    EXPECT_EQ(col->get_data()[1], 1);
    EXPECT_EQ(col->get_data()[2], 1);
}

TEST(PredicateColumnTest, InsertDataTinyInt) {
    auto col = PredicateColumnType<TYPE_TINYINT>::create();
    col->reserve(3);
    Int8 vals[] = {-128, 0, 127};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], -128);
    EXPECT_EQ(col->get_data()[1], 0);
    EXPECT_EQ(col->get_data()[2], 127);
}

TEST(PredicateColumnTest, InsertDataSmallInt) {
    auto col = PredicateColumnType<TYPE_SMALLINT>::create();
    col->reserve(3);
    Int16 vals[] = {-32768, 0, 32767};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], -32768);
    EXPECT_EQ(col->get_data()[1], 0);
    EXPECT_EQ(col->get_data()[2], 32767);
}

TEST(PredicateColumnTest, InsertDataInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(3);
    Int32 vals[] = {INT32_MIN, 0, INT32_MAX};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], INT32_MIN);
    EXPECT_EQ(col->get_data()[1], 0);
    EXPECT_EQ(col->get_data()[2], INT32_MAX);
}

TEST(PredicateColumnTest, InsertDataBigInt) {
    auto col = PredicateColumnType<TYPE_BIGINT>::create();
    col->reserve(3);
    Int64 vals[] = {INT64_MIN, 0, INT64_MAX};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], INT64_MIN);
    EXPECT_EQ(col->get_data()[1], 0);
    EXPECT_EQ(col->get_data()[2], INT64_MAX);
}

TEST(PredicateColumnTest, InsertDataLargeInt) {
    auto col = PredicateColumnType<TYPE_LARGEINT>::create();
    col->reserve(3);
    Int128 vals[] = {-(Int128(1) << 100), Int128(0), Int128(1) << 100};
    for (auto& v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], vals[0]);
    EXPECT_EQ(col->get_data()[1], vals[1]);
    EXPECT_EQ(col->get_data()[2], vals[2]);
}

TEST(PredicateColumnTest, InsertDataFloat) {
    auto col = PredicateColumnType<TYPE_FLOAT>::create();
    col->reserve(3);
    Float32 vals[] = {-3.14f, 0.0f, 2.718f};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_FLOAT_EQ(col->get_data()[0], -3.14f);
    EXPECT_FLOAT_EQ(col->get_data()[1], 0.0f);
    EXPECT_FLOAT_EQ(col->get_data()[2], 2.718f);
}

TEST(PredicateColumnTest, InsertDataDouble) {
    auto col = PredicateColumnType<TYPE_DOUBLE>::create();
    col->reserve(3);
    Float64 vals[] = {-3.14159265358979, 0.0, 2.71828182845904};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_DOUBLE_EQ(col->get_data()[0], -3.14159265358979);
    EXPECT_DOUBLE_EQ(col->get_data()[1], 0.0);
    EXPECT_DOUBLE_EQ(col->get_data()[2], 2.71828182845904);
}

TEST(PredicateColumnTest, InsertDataDateV2) {
    auto col = PredicateColumnType<TYPE_DATEV2>::create();
    col->reserve(3);
    uint32_t vals[] = {20230115, 20230620, 20231231};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
}

TEST(PredicateColumnTest, InsertDataDateTimeV2) {
    auto col = PredicateColumnType<TYPE_DATETIMEV2>::create();
    col->reserve(3);
    uint64_t vals[] = {1234567890123ULL, 2345678901234ULL, 3456789012345ULL};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
}

TEST(PredicateColumnTest, InsertDataString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(3);
    std::string s1 = "hello";
    std::string s2 = "world";
    std::string s3 = "";
    col->insert_data(s1.data(), s1.size());
    col->insert_data(s2.data(), s2.size());
    col->insert_data(s3.data(), s3.size());
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(std::string(col->get_data()[0].data, col->get_data()[0].size), "hello");
    EXPECT_EQ(std::string(col->get_data()[1].data, col->get_data()[1].size), "world");
    EXPECT_EQ(col->get_data()[2].size, 0);
}

TEST(PredicateColumnTest, InsertDataDecimal32) {
    auto col = PredicateColumnType<TYPE_DECIMAL32>::create();
    col->reserve(3);
    Decimal32 vals[] = {Decimal32(12345), Decimal32(-67890), Decimal32(0)};
    for (auto& v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], Decimal32(12345));
    EXPECT_EQ(col->get_data()[1], Decimal32(-67890));
    EXPECT_EQ(col->get_data()[2], Decimal32(0));
}

TEST(PredicateColumnTest, InsertDataDecimal64) {
    auto col = PredicateColumnType<TYPE_DECIMAL64>::create();
    col->reserve(3);
    Decimal64 vals[] = {Decimal64(Int64(123456789012LL)), Decimal64(Int64(-987654321098LL)),
                        Decimal64(Int64(0))};
    for (auto& v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], Decimal64(Int64(123456789012LL)));
    EXPECT_EQ(col->get_data()[1], Decimal64(Int64(-987654321098LL)));
    EXPECT_EQ(col->get_data()[2], Decimal64(Int64(0)));
}

TEST(PredicateColumnTest, InsertDataIPv4) {
    auto col = PredicateColumnType<TYPE_IPV4>::create();
    col->reserve(3);
    IPv4 vals[] = {IPv4(0x7F000001), IPv4(0xC0A80001), IPv4(0x00000000)};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], IPv4(0x7F000001));
    EXPECT_EQ(col->get_data()[1], IPv4(0xC0A80001));
    EXPECT_EQ(col->get_data()[2], IPv4(0x00000000));
}

TEST(PredicateColumnTest, InsertDataIPv6) {
    auto col = PredicateColumnType<TYPE_IPV6>::create();
    col->reserve(2);
    IPv6 vals[] = {IPv6(1), IPv6(0)};
    for (auto& v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    EXPECT_EQ(col->size(), 2);
    EXPECT_EQ(col->get_data()[0], IPv6(1));
    EXPECT_EQ(col->get_data()[1], IPv6(0));
}

// ============================================================================
// Test insert_many_fix_len_data() for numeric types
// ============================================================================
TEST(PredicateColumnTest, InsertManyFixLenDataInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(5);
    Int32 vals[] = {1, 2, 3, 4, 5};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 5);
    EXPECT_EQ(col->size(), 5);
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(col->get_data()[i], i + 1);
    }
}

TEST(PredicateColumnTest, InsertManyFixLenDataBigInt) {
    auto col = PredicateColumnType<TYPE_BIGINT>::create();
    col->reserve(3);
    Int64 vals[] = {100000000000LL, 200000000000LL, 300000000000LL};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], 100000000000LL);
    EXPECT_EQ(col->get_data()[1], 200000000000LL);
    EXPECT_EQ(col->get_data()[2], 300000000000LL);
}

TEST(PredicateColumnTest, InsertManyFixLenDataFloat) {
    auto col = PredicateColumnType<TYPE_FLOAT>::create();
    col->reserve(3);
    Float32 vals[] = {1.1f, 2.2f, 3.3f};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_FLOAT_EQ(col->get_data()[0], 1.1f);
    EXPECT_FLOAT_EQ(col->get_data()[1], 2.2f);
    EXPECT_FLOAT_EQ(col->get_data()[2], 3.3f);
}

TEST(PredicateColumnTest, InsertManyFixLenDataDouble) {
    auto col = PredicateColumnType<TYPE_DOUBLE>::create();
    col->reserve(3);
    Float64 vals[] = {1.11, 2.22, 3.33};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_DOUBLE_EQ(col->get_data()[0], 1.11);
    EXPECT_DOUBLE_EQ(col->get_data()[1], 2.22);
    EXPECT_DOUBLE_EQ(col->get_data()[2], 3.33);
}

TEST(PredicateColumnTest, InsertManyFixLenDataDateV2) {
    auto col = PredicateColumnType<TYPE_DATEV2>::create();
    col->reserve(3);
    uint32_t vals[] = {20230115, 20230620, 20231231};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
}

TEST(PredicateColumnTest, InsertManyFixLenDataDateTimeV2) {
    auto col = PredicateColumnType<TYPE_DATETIMEV2>::create();
    col->reserve(3);
    uint64_t vals[] = {1234567890123ULL, 2345678901234ULL, 3456789012345ULL};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
}

TEST(PredicateColumnTest, InsertManyFixLenDataDecimal32) {
    auto col = PredicateColumnType<TYPE_DECIMAL32>::create();
    col->reserve(3);
    Decimal32 vals[] = {Decimal32(100), Decimal32(200), Decimal32(300)};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], Decimal32(100));
    EXPECT_EQ(col->get_data()[1], Decimal32(200));
    EXPECT_EQ(col->get_data()[2], Decimal32(300));
}

TEST(PredicateColumnTest, InsertManyFixLenDataDecimal64) {
    auto col = PredicateColumnType<TYPE_DECIMAL64>::create();
    col->reserve(3);
    Decimal64 vals[] = {Decimal64(Int64(1000)), Decimal64(Int64(2000)), Decimal64(Int64(3000))};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], Decimal64(Int64(1000)));
    EXPECT_EQ(col->get_data()[1], Decimal64(Int64(2000)));
    EXPECT_EQ(col->get_data()[2], Decimal64(Int64(3000)));
}

TEST(PredicateColumnTest, InsertManyFixLenDataIPv4) {
    auto col = PredicateColumnType<TYPE_IPV4>::create();
    col->reserve(3);
    IPv4 vals[] = {IPv4(1), IPv4(2), IPv4(3)};
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(vals), 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], IPv4(1));
    EXPECT_EQ(col->get_data()[1], IPv4(2));
    EXPECT_EQ(col->get_data()[2], IPv4(3));
}

// ============================================================================
// Test insert_many_date() for TYPE_DATE
// ============================================================================
TEST(PredicateColumnTest, InsertManyDate) {
    auto col = PredicateColumnType<TYPE_DATE>::create();
    col->reserve(3);
    uint24_t date_vals[3];
    date_vals[0] = (2023 << 9) | (1 << 5) | 15;
    date_vals[1] = (2023 << 9) | (6 << 5) | 20;
    date_vals[2] = (2023 << 9) | (12 << 5) | 31;
    col->insert_many_date(reinterpret_cast<const char*>(date_vals), 3);
    EXPECT_EQ(col->size(), 3);
}

// ============================================================================
// Test insert_many_datetime() for TYPE_DATETIME
// ============================================================================
TEST(PredicateColumnTest, InsertManyDateTime) {
    auto col = PredicateColumnType<TYPE_DATETIME>::create();
    col->reserve(3);
    uint64_t datetime_vals[] = {20230115120000ULL, 20230620153045ULL, 20231231235959ULL};
    col->insert_many_datetime(reinterpret_cast<const char*>(datetime_vals), 3);
    EXPECT_EQ(col->size(), 3);
}

// ============================================================================
// Test insert_many_decimalv2() for TYPE_DECIMALV2
// ============================================================================
TEST(PredicateColumnTest, InsertManyDecimalV2) {
    auto col = PredicateColumnType<TYPE_DECIMALV2>::create();
    col->reserve(3);
    decimal12_t decimals[3];
    decimals[0].integer = 123;
    decimals[0].fraction = 456000000;
    decimals[1].integer = -789;
    decimals[1].fraction = 123000000;
    decimals[2].integer = 0;
    decimals[2].fraction = 999000000;
    col->insert_many_fix_len_data(reinterpret_cast<const char*>(decimals), 3);
    EXPECT_EQ(col->size(), 3);
}

// ============================================================================
// Test insert_many_strings() for string types
// ============================================================================
TEST(PredicateColumnTest, InsertManyStringsString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(5);
    StringRef strings[] = {StringRef("one", 3), StringRef("two", 3), StringRef("three", 5)};
    col->insert_many_strings(strings, 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(std::string(col->get_data()[0].data, col->get_data()[0].size), "one");
    EXPECT_EQ(std::string(col->get_data()[1].data, col->get_data()[1].size), "two");
    EXPECT_EQ(std::string(col->get_data()[2].data, col->get_data()[2].size), "three");
}

TEST(PredicateColumnTest, InsertManyStringsVarchar) {
    auto col = PredicateColumnType<TYPE_VARCHAR>::create();
    col->reserve(3);
    StringRef strings[] = {StringRef("abc", 3), StringRef("defgh", 5)};
    col->insert_many_strings(strings, 2);
    EXPECT_EQ(col->size(), 2);
    EXPECT_EQ(std::string(col->get_data()[0].data, col->get_data()[0].size), "abc");
    EXPECT_EQ(std::string(col->get_data()[1].data, col->get_data()[1].size), "defgh");
}

TEST(PredicateColumnTest, InsertManyStringsChar) {
    auto col = PredicateColumnType<TYPE_CHAR>::create();
    col->reserve(3);
    StringRef strings[] = {StringRef("ab", 2), StringRef("cd", 2)};
    col->insert_many_strings(strings, 2);
    EXPECT_EQ(col->size(), 2);
}

TEST(PredicateColumnTest, InsertManyStringsEmpty) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(3);
    col->insert_many_strings(nullptr, 0);
    EXPECT_EQ(col->size(), 0);
}

// ============================================================================
// Test insert_many_continuous_binary_data() for string types
// ============================================================================
TEST(PredicateColumnTest, InsertManyContinuousBinaryDataString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(3);
    const char* data = "helloworld!";
    uint32_t offsets[] = {0, 5, 10, 11};
    col->insert_many_continuous_binary_data(data, offsets, 3);
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(std::string(col->get_data()[0].data, col->get_data()[0].size), "hello");
    EXPECT_EQ(std::string(col->get_data()[1].data, col->get_data()[1].size), "world");
    EXPECT_EQ(std::string(col->get_data()[2].data, col->get_data()[2].size), "!");
}

TEST(PredicateColumnTest, InsertManyContinuousBinaryDataVarchar) {
    auto col = PredicateColumnType<TYPE_VARCHAR>::create();
    col->reserve(2);
    const char* data = "abcdef";
    uint32_t offsets[] = {0, 3, 6};
    col->insert_many_continuous_binary_data(data, offsets, 2);
    EXPECT_EQ(col->size(), 2);
    EXPECT_EQ(std::string(col->get_data()[0].data, col->get_data()[0].size), "abc");
    EXPECT_EQ(std::string(col->get_data()[1].data, col->get_data()[1].size), "def");
}

TEST(PredicateColumnTest, InsertManyContinuousBinaryDataEmpty) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(3);
    const char* data = "test";
    uint32_t offsets[] = {0};
    col->insert_many_continuous_binary_data(data, offsets, 0);
    EXPECT_EQ(col->size(), 0);
}

// ============================================================================
// Test insert_many_dict_data() for string types
// ============================================================================
TEST(PredicateColumnTest, InsertManyDictDataString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(5);
    StringRef dict[] = {StringRef("apple", 5), StringRef("banana", 6), StringRef("cherry", 6)};
    int32_t codewords[] = {0, 1, 2, 0, 1};
    col->insert_many_dict_data(codewords, 0, dict, 5, 3);
    EXPECT_EQ(col->size(), 5);
    EXPECT_EQ(std::string(col->get_data()[0].data, col->get_data()[0].size), "apple");
    EXPECT_EQ(std::string(col->get_data()[1].data, col->get_data()[1].size), "banana");
    EXPECT_EQ(std::string(col->get_data()[2].data, col->get_data()[2].size), "cherry");
    EXPECT_EQ(std::string(col->get_data()[3].data, col->get_data()[3].size), "apple");
    EXPECT_EQ(std::string(col->get_data()[4].data, col->get_data()[4].size), "banana");
}

TEST(PredicateColumnTest, InsertManyDictDataVarchar) {
    auto col = PredicateColumnType<TYPE_VARCHAR>::create();
    col->reserve(3);
    StringRef dict[] = {StringRef("x", 1), StringRef("yy", 2)};
    int32_t codewords[] = {0, 1, 0};
    col->insert_many_dict_data(codewords, 0, dict, 3, 2);
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(std::string(col->get_data()[0].data, col->get_data()[0].size), "x");
    EXPECT_EQ(std::string(col->get_data()[1].data, col->get_data()[1].size), "yy");
    EXPECT_EQ(std::string(col->get_data()[2].data, col->get_data()[2].size), "x");
}

// ============================================================================
// Test insert_duplicate_fields() for all types
// ============================================================================
TEST(PredicateColumnTest, InsertDuplicateFieldsString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(10);
    std::string test_str = "hello";
    Field field = Field::create_field<TYPE_STRING>(test_str);
    col->insert_duplicate_fields(field, 5);
    EXPECT_EQ(col->get_data()[col->size() - 1].data,
              col->get_data()[0].data + (col->size() - 1) * test_str.size());
    EXPECT_EQ(col->size(), 5);
    for (size_t i = 0; i < 5; i++) {
        StringRef ref = col->get_data()[i];
        EXPECT_EQ(std::string(ref.data, ref.size), test_str);
    }
    // Insert another batch to verify memory doesn't overlap
    std::string str2 = "world";
    Field field2 = Field::create_field<TYPE_STRING>(str2);
    col->insert_duplicate_fields(field2, 3);
    EXPECT_EQ(col->size(), 8);
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(std::string(col->get_data()[i].data, col->get_data()[i].size), test_str);
    }
    for (size_t i = 5; i < 8; i++) {
        EXPECT_EQ(std::string(col->get_data()[i].data, col->get_data()[i].size), str2);
    }
}

TEST(PredicateColumnTest, InsertDuplicateFieldsInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(10);
    Int32 val = 42;
    Field field = Field::create_field<TYPE_INT>(val);
    col->insert_duplicate_fields(field, 5);
    EXPECT_EQ(col->size(), 5);
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(col->get_data()[i], 42);
    }
}

TEST(PredicateColumnTest, InsertDuplicateFieldsLargeInt) {
    auto col = PredicateColumnType<TYPE_LARGEINT>::create();
    col->reserve(10);
    Int128 val = Int128(123456789012345LL) * Int128(1000000000LL);
    Field field = Field::create_field<TYPE_LARGEINT>(val);
    col->insert_duplicate_fields(field, 3);
    EXPECT_EQ(col->size(), 3);
    for (size_t i = 0; i < 3; i++) {
        EXPECT_EQ(col->get_data()[i], val);
    }
}

TEST(PredicateColumnTest, InsertDuplicateFieldsZeroCount) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(5);
    Int32 val = 42;
    Field field = Field::create_field<TYPE_INT>(val);
    col->insert_duplicate_fields(field, 0);
    EXPECT_EQ(col->size(), 0);
}

// ============================================================================
// Test insert_default() for all types
// ============================================================================
TEST(PredicateColumnTest, InsertDefaultInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(3);
    col->insert_default();
    col->insert_default();
    EXPECT_EQ(col->size(), 2);
    EXPECT_EQ(col->get_data()[0], 0);
    EXPECT_EQ(col->get_data()[1], 0);
}

TEST(PredicateColumnTest, InsertDefaultDouble) {
    auto col = PredicateColumnType<TYPE_DOUBLE>::create();
    col->reserve(2);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
    EXPECT_DOUBLE_EQ(col->get_data()[0], 0.0);
}

TEST(PredicateColumnTest, InsertDefaultString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(2);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
    EXPECT_EQ(col->get_data()[0].size, 0);
}

// ============================================================================
// Test clear() for all types
// ============================================================================
TEST(PredicateColumnTest, ClearInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(10);
    Int32 val = 42;
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    EXPECT_EQ(col->size(), 2);
    col->clear();
    EXPECT_EQ(col->size(), 0);
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    EXPECT_EQ(col->size(), 1);
}

TEST(PredicateColumnTest, ClearString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(5);
    std::string s = "test";
    col->insert_data(s.data(), s.size());
    EXPECT_EQ(col->size(), 1);
    col->clear();
    EXPECT_EQ(col->size(), 0);
}

// ============================================================================
// Test reserve() for all types
// ============================================================================
TEST(PredicateColumnTest, ReserveInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(100);
    EXPECT_EQ(col->size(), 0);
    for (int i = 0; i < 100; i++) {
        Int32 val = i;
        col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    }
    EXPECT_EQ(col->size(), 100);
}

TEST(PredicateColumnTest, ReserveString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(50);
    EXPECT_EQ(col->size(), 0);
}

// ============================================================================
// Test byte_size() and allocated_bytes() for all types
// ============================================================================
TEST(PredicateColumnTest, ByteSizeInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(5);
    EXPECT_EQ(col->byte_size(), 0);
    Int32 val = 1;
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    EXPECT_EQ(col->byte_size(), sizeof(Int32));
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    EXPECT_EQ(col->byte_size(), 2 * sizeof(Int32));
    EXPECT_EQ(col->allocated_bytes(), col->byte_size());
}

TEST(PredicateColumnTest, ByteSizeBigInt) {
    auto col = PredicateColumnType<TYPE_BIGINT>::create();
    col->reserve(3);
    Int64 val = 12345;
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    EXPECT_EQ(col->byte_size(), 3 * sizeof(Int64));
    EXPECT_EQ(col->allocated_bytes(), 3 * sizeof(Int64));
}

TEST(PredicateColumnTest, ByteSizeString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(3);
    std::string s = "hello";
    col->insert_data(s.data(), s.size());
    EXPECT_EQ(col->byte_size(), sizeof(StringRef));
}

// ============================================================================
// Test get_name() for all types
// ============================================================================
TEST(PredicateColumnTest, GetNameAllTypes) {
    EXPECT_EQ(PredicateColumnType<TYPE_BOOLEAN>::create()->get_name(), "BOOL");
    EXPECT_EQ(PredicateColumnType<TYPE_TINYINT>::create()->get_name(), "TINYINT");
    EXPECT_EQ(PredicateColumnType<TYPE_SMALLINT>::create()->get_name(), "SMALLINT");
    EXPECT_EQ(PredicateColumnType<TYPE_INT>::create()->get_name(), "INT");
    EXPECT_EQ(PredicateColumnType<TYPE_BIGINT>::create()->get_name(), "BIGINT");
    EXPECT_EQ(PredicateColumnType<TYPE_LARGEINT>::create()->get_name(), "LARGEINT");
    EXPECT_EQ(PredicateColumnType<TYPE_FLOAT>::create()->get_name(), "FLOAT");
    EXPECT_EQ(PredicateColumnType<TYPE_DOUBLE>::create()->get_name(), "DOUBLE");
    EXPECT_EQ(PredicateColumnType<TYPE_DATE>::create()->get_name(), "DATE");
    EXPECT_EQ(PredicateColumnType<TYPE_DATEV2>::create()->get_name(), "DATEV2");
    EXPECT_EQ(PredicateColumnType<TYPE_DATETIME>::create()->get_name(), "DATETIME");
    EXPECT_EQ(PredicateColumnType<TYPE_DATETIMEV2>::create()->get_name(), "DATETIMEV2");
    EXPECT_EQ(PredicateColumnType<TYPE_CHAR>::create()->get_name(), "CHAR");
    EXPECT_EQ(PredicateColumnType<TYPE_VARCHAR>::create()->get_name(), "VARCHAR");
    EXPECT_EQ(PredicateColumnType<TYPE_STRING>::create()->get_name(), "STRING");
    EXPECT_EQ(PredicateColumnType<TYPE_DECIMALV2>::create()->get_name(), "DECIMALV2");
    EXPECT_EQ(PredicateColumnType<TYPE_DECIMAL32>::create()->get_name(), "DECIMAL32");
    EXPECT_EQ(PredicateColumnType<TYPE_DECIMAL64>::create()->get_name(), "DECIMAL64");
    EXPECT_EQ(PredicateColumnType<TYPE_DECIMAL128I>::create()->get_name(), "DECIMAL128I");
    EXPECT_EQ(PredicateColumnType<TYPE_DECIMAL256>::create()->get_name(), "DECIMAL256");
    EXPECT_EQ(PredicateColumnType<TYPE_IPV4>::create()->get_name(), "IPV4");
    EXPECT_EQ(PredicateColumnType<TYPE_IPV6>::create()->get_name(), "IPV6");
}

// ============================================================================
// Test clone_resized() for all types
// ============================================================================
TEST(PredicateColumnTest, CloneResizedInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(5);
    Int32 val = 42;
    col->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    auto cloned = col->clone_resized(0);
    EXPECT_EQ(cloned->size(), 0);
}

TEST(PredicateColumnTest, CloneResizedString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(5);
    std::string s = "test";
    col->insert_data(s.data(), s.size());
    auto cloned = col->clone_resized(0);
    EXPECT_EQ(cloned->size(), 0);
}

// ============================================================================
// Test get_data_at() for string types
// ============================================================================
TEST(PredicateColumnTest, GetDataAtString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(3);
    std::string s1 = "abc";
    std::string s2 = "defgh";
    col->insert_data(s1.data(), s1.size());
    col->insert_data(s2.data(), s2.size());
    StringRef ref0 = col->get_data_at(0);
    StringRef ref1 = col->get_data_at(1);
    EXPECT_EQ(std::string(ref0.data, ref0.size), "abc");
    EXPECT_EQ(std::string(ref1.data, ref1.size), "defgh");
}

TEST(PredicateColumnTest, GetDataAtVarchar) {
    auto col = PredicateColumnType<TYPE_VARCHAR>::create();
    col->reserve(2);
    std::string s1 = "test";
    col->insert_data(s1.data(), s1.size());
    StringRef ref = col->get_data_at(0);
    EXPECT_EQ(std::string(ref.data, ref.size), "test");
}

// ============================================================================
// Test get_data() for all types
// ============================================================================
TEST(PredicateColumnTest, GetDataInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(3);
    Int32 vals[] = {1, 2, 3};
    for (auto v : vals) {
        col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    auto& data = col->get_data();
    EXPECT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 2);
    EXPECT_EQ(data[2], 3);
}

TEST(PredicateColumnTest, GetDataString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(2);
    std::string s1 = "a";
    std::string s2 = "bb";
    col->insert_data(s1.data(), s1.size());
    col->insert_data(s2.data(), s2.size());
    const auto& data = col->get_data();
    EXPECT_EQ(data.size(), 2);
    EXPECT_EQ(std::string(data[0].data, data[0].size), "a");
    EXPECT_EQ(std::string(data[1].data, data[1].size), "bb");
}

// ============================================================================
// Test filter_by_selector() for all types
// ============================================================================
TEST(PredicateColumnTest, FilterBySelectorInt) {
    auto pred_col = PredicateColumnType<TYPE_INT>::create();
    pred_col->reserve(5);
    Int32 vals[] = {10, 20, 30, 40, 50};
    for (auto v : vals) {
        pred_col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    auto result_col = ColumnInt32::create();
    uint16_t selector[] = {0, 2, 4};
    EXPECT_EQ(pred_col->filter_by_selector(selector, 3, result_col.get()), Status::OK());
    EXPECT_EQ(result_col->size(), 3);
    EXPECT_EQ(result_col->get_data()[0], 10);
    EXPECT_EQ(result_col->get_data()[1], 30);
    EXPECT_EQ(result_col->get_data()[2], 50);
}

TEST(PredicateColumnTest, FilterBySelectorBigInt) {
    auto pred_col = PredicateColumnType<TYPE_BIGINT>::create();
    pred_col->reserve(4);
    Int64 vals[] = {100LL, 200LL, 300LL, 400LL};
    for (auto v : vals) {
        pred_col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    auto result_col = ColumnInt64::create();
    uint16_t selector[] = {1, 3};
    EXPECT_EQ(pred_col->filter_by_selector(selector, 2, result_col.get()), Status::OK());
    EXPECT_EQ(result_col->size(), 2);
    EXPECT_EQ(result_col->get_data()[0], 200LL);
    EXPECT_EQ(result_col->get_data()[1], 400LL);
}

TEST(PredicateColumnTest, FilterBySelectorFloat) {
    auto pred_col = PredicateColumnType<TYPE_FLOAT>::create();
    pred_col->reserve(3);
    Float32 vals[] = {1.1f, 2.2f, 3.3f};
    for (auto v : vals) {
        pred_col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    auto result_col = ColumnFloat32::create();
    uint16_t selector[] = {0, 2};
    EXPECT_EQ(pred_col->filter_by_selector(selector, 2, result_col.get()), Status::OK());
    EXPECT_EQ(result_col->size(), 2);
    EXPECT_FLOAT_EQ(result_col->get_data()[0], 1.1f);
    EXPECT_FLOAT_EQ(result_col->get_data()[1], 3.3f);
}

TEST(PredicateColumnTest, FilterBySelectorDouble) {
    auto pred_col = PredicateColumnType<TYPE_DOUBLE>::create();
    pred_col->reserve(3);
    Float64 vals[] = {1.11, 2.22, 3.33};
    for (auto v : vals) {
        pred_col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    auto result_col = ColumnFloat64::create();
    uint16_t selector[] = {1};
    EXPECT_EQ(pred_col->filter_by_selector(selector, 1, result_col.get()), Status::OK());
    EXPECT_EQ(result_col->size(), 1);
    EXPECT_DOUBLE_EQ(result_col->get_data()[0], 2.22);
}

TEST(PredicateColumnTest, FilterBySelectorBoolean) {
    auto pred_col = PredicateColumnType<TYPE_BOOLEAN>::create();
    pred_col->reserve(5);
    UInt8 vals[] = {0, 1, 0, 1, 1};
    for (auto v : vals) {
        pred_col->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    auto result_col = ColumnUInt8::create();
    uint16_t selector[] = {1, 3, 4};
    EXPECT_EQ(pred_col->filter_by_selector(selector, 3, result_col.get()), Status::OK());
    EXPECT_EQ(result_col->size(), 3);
    EXPECT_EQ(result_col->get_data()[0], 1);
    EXPECT_EQ(result_col->get_data()[1], 1);
    EXPECT_EQ(result_col->get_data()[2], 1);
}

TEST(PredicateColumnTest, FilterBySelectorString) {
    auto pred_col = PredicateColumnType<TYPE_STRING>::create();
    pred_col->reserve(5);
    std::vector<std::string> strings = {"a", "bb", "ccc", "dddd", "eeeee"};
    for (const auto& s : strings) {
        pred_col->insert_data(s.data(), s.size());
    }
    auto result_col = ColumnString::create();
    uint16_t selector[] = {1, 3};
    EXPECT_EQ(pred_col->filter_by_selector(selector, 2, result_col.get()), Status::OK());
    EXPECT_EQ(result_col->size(), 2);
    EXPECT_EQ(result_col->get_data_at(0).to_string(), "bb");
    EXPECT_EQ(result_col->get_data_at(1).to_string(), "dddd");
}

TEST(PredicateColumnTest, FilterBySelectorVarchar) {
    auto pred_col = PredicateColumnType<TYPE_VARCHAR>::create();
    pred_col->reserve(3);
    std::vector<std::string> strings = {"x", "yy", "zzz"};
    for (const auto& s : strings) {
        pred_col->insert_data(s.data(), s.size());
    }
    auto result_col = ColumnString::create();
    uint16_t selector[] = {0, 2};
    EXPECT_EQ(pred_col->filter_by_selector(selector, 2, result_col.get()), Status::OK());
    EXPECT_EQ(result_col->size(), 2);
    EXPECT_EQ(result_col->get_data_at(0).to_string(), "x");
    EXPECT_EQ(result_col->get_data_at(1).to_string(), "zzz");
}

// ============================================================================
// Test insert_string_value() for string types
// ============================================================================
TEST(PredicateColumnTest, InsertStringValueString) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(5);
    std::vector<std::string> strings = {"hello", "world", "", "test123", "long string"};
    for (const auto& s : strings) {
        col->insert_string_value(s.data(), s.size());
    }
    EXPECT_EQ(col->size(), 5);
    for (size_t i = 0; i < strings.size(); i++) {
        StringRef ref = col->get_data()[i];
        EXPECT_EQ(std::string(ref.data, ref.size), strings[i]);
    }
}

// ============================================================================
// Test insert_in_copy_way() for LARGEINT
// ============================================================================
TEST(PredicateColumnTest, InsertInCopyWayLargeInt) {
    auto col = PredicateColumnType<TYPE_LARGEINT>::create();
    col->reserve(3);
    Int128 val1 = Int128(1) << 100;
    Int128 val2 = -val1;
    Int128 val3 = 0;
    col->insert_in_copy_way(reinterpret_cast<const char*>(&val1), sizeof(val1));
    col->insert_in_copy_way(reinterpret_cast<const char*>(&val2), sizeof(val2));
    col->insert_in_copy_way(reinterpret_cast<const char*>(&val3), sizeof(val3));
    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data()[0], val1);
    EXPECT_EQ(col->get_data()[1], val2);
    EXPECT_EQ(col->get_data()[2], val3);
}

// ============================================================================
// Test insert_default_type() for numeric types
// ============================================================================
TEST(PredicateColumnTest, InsertDefaultTypeInt) {
    auto col = PredicateColumnType<TYPE_INT>::create();
    col->reserve(3);
    Int32 val = 999;
    col->insert_default_type(reinterpret_cast<const char*>(&val), sizeof(val));
    EXPECT_EQ(col->size(), 1);
    EXPECT_EQ(col->get_data()[0], 999);
}

// ============================================================================
// Test empty string handling
// ============================================================================
TEST(PredicateColumnTest, EmptyStringHandling) {
    auto col = PredicateColumnType<TYPE_STRING>::create();
    col->reserve(5);
    std::vector<std::string> strings = {"", "a", "", "bc", ""};
    for (const auto& s : strings) {
        col->insert_data(s.data(), s.size());
    }
    EXPECT_EQ(col->size(), 5);
    EXPECT_EQ(col->get_data()[0].size, 0);
    EXPECT_EQ(col->get_data()[1].size, 1);
    EXPECT_EQ(col->get_data()[2].size, 0);
    EXPECT_EQ(col->get_data()[3].size, 2);
    EXPECT_EQ(col->get_data()[4].size, 0);
}

} // namespace doris