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

#include "core/types.h"
#include "core/value/ipv4_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "exprs/function/cast/cast_to_datev2_impl.hpp"
#include "exprs/function/cast/cast_to_string.h"

namespace doris {

TEST(CastToStringTest, test) {
    {
        UInt8 num = 1;
        std::string str = CastToString::from_number(num);
        EXPECT_EQ(str, "1");
    }
    {
        Int16 num = 12345;
        std::string str = CastToString::from_number(num);
        EXPECT_EQ(str, "12345");
    }
    {
        Int32 num = -123456789;
        std::string str = CastToString::from_number(num);
        EXPECT_EQ(str, "-123456789");
    }
    {
        Int64 num = 9223372036854775807;
        std::string str = CastToString::from_number(num);
        EXPECT_EQ(str, "9223372036854775807");
    }
    {
        Int128 num = 9223372036854775807;
        std::string str = CastToString::from_number(num);
        EXPECT_EQ(str, "9223372036854775807");
    }
    {
        Float32 num = 12345.6789;
        std::string str = CastToString::from_number(num);
        EXPECT_EQ(str, "12345.68");
    }
    {
        Float64 num = -123456789.987654321;
        std::string str = CastToString::from_number(num);
        EXPECT_EQ(str, "-123456789.9876543");
    }
    {
        Decimal32 num = 123456789;
        std::string str = CastToString::from_decimal(num, 2);
        EXPECT_EQ(str, "1234567.89");
    }
    {
        Decimal64 num = -123456789012345678;
        std::string str = CastToString::from_decimal(num, 4);
        EXPECT_EQ(str, "-12345678901234.5678");
    }
    {
        Decimal128V2 num = 1234567890123;
        std::string str = CastToString::from_decimal(num, 6);
        EXPECT_EQ(str, "1234.567890");
    }
    {
        Decimal128V3 num = 1234567890567890;
        std::string str = CastToString::from_decimal(num, 8);
        EXPECT_EQ(str, "12345678.90567890");
    }
    {
        Decimal256 num {1234567890567890};
        std::string str = CastToString::from_decimal(num, 10);
        EXPECT_EQ(str, "123456.7890567890");
    }

    {
        VecDateTimeValue date;
        std::string from_str = "2024-01-01 12:34:56";
        {
            CastParameters p;
            CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                          DatelikeTargetType::DATE_TIME>(
                    {from_str.c_str(), from_str.size()}, date, nullptr, p);
        }
        date.cast_to_date();
        std::string str = CastToString::from_date_or_datetime(date);
        EXPECT_EQ(str, "2024-01-01");
    }
    {
        VecDateTimeValue datetime;
        std::string from_str = "2024-01-01 12:34:56";
        {
            CastParameters p;
            CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                          DatelikeTargetType::DATE_TIME>(
                    {from_str.c_str(), from_str.size()}, datetime, nullptr, p);
        }
        std::string str = CastToString::from_date_or_datetime(datetime);
        EXPECT_EQ(str, "2024-01-01 12:34:56");
    }

    {
        DateV2Value<DateV2ValueType> datev2;
        std::string from_str = "2024-01-01";
        {
            CastParameters p;
            CastToDateV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                    {from_str.c_str(), from_str.size()}, datev2, nullptr, p);
        }
        std::string str = CastToString::from_datev2(datev2);
        EXPECT_EQ(str, "2024-01-01");
    }
    {
        DateV2Value<DateTimeV2ValueType> datetimev2;
        std::string from_str = "2024-01-01 12:34:56.123456";
        {
            CastParameters p;
            CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                    {from_str.c_str(), from_str.size()}, datetimev2, nullptr, 6, p);
        }
        std::string str = CastToString::from_datetimev2(datetimev2, 6);
        EXPECT_EQ(str, "2024-01-01 12:34:56.123456");
    }

    {
        TimeValue::TimeType time_value = TimeValue::make_time(23, 22, 21);
        std::string str = CastToString::from_time(time_value, 6);
        EXPECT_EQ(str, "23:22:21.000000");
    }

    {
        IPv4Value ip;
        ip.from_string("192.168.1.1");
        std::string str = CastToString::from_ip(ip.value());
        EXPECT_EQ(str, "192.168.1.1");
    }

    {
        IPv6Value ip;
        ip.from_string("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        std::string str = CastToString::from_ip(ip.value());
        EXPECT_EQ(str, "2001:db8:85a3::8a2e:370:7334");
    }
}

TEST(CastToStringTest, from_int128_overloads) {
    EXPECT_EQ(CastToString::from_int128(static_cast<int128_t>(-1234567890123456789LL)),
              "-1234567890123456789");
    EXPECT_EQ(CastToString::from_uint128(static_cast<uint128_t>(12345678901234567890ULL)),
              "12345678901234567890");

    UInt128 value;
    value.items[0] = 0x0123456789ABCDEFULL;
    value.items[1] = 0x0FEDCBA987654321ULL;
    EXPECT_EQ(CastToString::from_uint128(value), "0123456789abcdeffedcba987654321");
}

} // namespace doris
