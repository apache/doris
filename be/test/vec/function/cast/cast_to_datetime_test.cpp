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

#include "cast_test.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"

namespace doris::vectorized {
using namespace ut_type;

TEST_F(FunctionCastTest, string_to_datetime6_valid_case_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Valid ISO 8601 format with timezone
            {{std::string("2023-07-16T19:20:30.123+08:00")},
             std::string("2023-07-16 19:20:30.123000")},
            {{std::string("2023-07-16T19+08:00")}, std::string("2023-07-16 19:00:00.000000")},
            {{std::string("2023-07-16T1920+08:00")}, std::string("2023-07-16 19:20:00.000000")},
            {{std::string("70-1-1T00:00:00-0000")}, std::string("1970-01-01 08:00:00.000000")},

            // Date with timezone names
            {{std::string("2024-02-29 12:00:00 Europe/Paris")},
             std::string("2024-02-29 19:00:00.000000")},
            {{std::string("2024-05-01T00:00Asia/Shanghai")},
             std::string("2024-05-01 00:00:00.000000")},
            {{std::string("20231005T081530Europe/London")}, std::string("2023-10-05 15:15:30")},
            {{std::string("85-12-25T0000gMt")}, std::string("1985-12-25 08:00:00")},

            // Simple date formats
            {{std::string("2024-05-01")}, std::string("2024-05-01")},
            {{std::string("24-5-1")}, std::string("2024-05-01")},
            {{std::string("2024-05-01 0:1:2.333")}, std::string("2024-05-01 00:01:02.333000")},
            {{std::string("2024-05-01 0:1:2.")}, std::string("2024-05-01 00:01:02")},

            // Compact formats
            {{std::string("20240501 01")}, std::string("2024-05-01 01:00:00")},
            {{std::string("20230716 1920Z")}, std::string("2023-07-17 03:20:00")},
            {{std::string("20240501T0000")}, std::string("2024-05-01")},

            // High precision timestamps
            {{std::string("2024-12-31 23:59:59.9999999999999999999")}, std::string("2025-01-01")},

            // Various timezone offsets
            {{std::string("2020-12-12 13:12:12-03:00")}, std::string("2020-12-13 00:12:12")},
            {{std::string("0023-01-01T00:00Z")},
             std::string(
                     "0023-01-01 08:05:43")}, // CST of shanghai before 1900 is not +080000 but +080543

            // Year cutoff cases
            {{std::string("69-12-31")}, std::string("2069-12-31")},
            {{std::string("70-01-01")}, std::string("1970-01-01")},

            // Compact numeric formats
            {{std::string("230102")}, std::string("2023-01-02")},
            {{std::string("19230101")}, std::string("1923-01-01")},
            {{std::string("20120102030405")}, std::string("2012-01-02 03:04:05")},

            {{std::string("2024-02-29T23:59:59.999999 UTC")},
             std::string("2024-03-01 07:59:59.999999")},
            {{std::string("70-01-01T00:00:00+14")}, std::string("1969-12-31 18:00:00")},
            {{std::string("0023-1-1T1:2:3. -00:00")}, std::string("0023-01-01 09:07:46")},
            {{std::string("20-1-1")}, std::string("2020-01-01")},
            {{std::string("0123-12-12")}, std::string("0123-12-12")},
            {{std::string("01231212")}, std::string("0123-12-12")},
            {{std::string("68-01-01")}, std::string("2068-01-01")},
            {{std::string("69-01-01")}, std::string("2069-01-01")},
            {{std::string("71-01-01")}, std::string("1971-01-01")},
            {{std::string("99-12-31")}, std::string("1999-12-31")},
            {{std::string("00-01-01")}, std::string("2000-01-01")},
            {{std::string("12010203040506.999")}, std::string("1201-02-03 04:05:06.999000")},
            {{std::string("12010203040506.")}, std::string("1201-02-03 04:05:06")},
    };
    check_function_for_cast_strict_mode<DataTypeDateTimeV2>(input_types, data_set, "", 6);
}

TEST_F(FunctionCastTest, string_to_datetime6_invalid_cases_in_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Invalid formats. in strict
            {{std::string("abc")}, Null()},
            {{std::string("2020-05-05 12:30:60")}, Null()},
            {{std::string("2023-07-16T19.123+08:00")}, Null()},
            {{std::string("2024/05/01")}, Null()},
            {{std::string("24012")}, Null()},
            {{std::string("2411 123")}, Null()},
            {{std::string("2024-05-01 01:030:02")}, Null()},
            {{std::string("10000-01-01 00:00:00")}, Null()},
            {{std::string("2024-0131T12:00")}, Null()},
            {{std::string("2024-05-01@00:00")}, Null()},
            {{std::string("20120212051")}, Null()},
            {{std::string("2024-05-01T00:00XYZ")}, Null()},
            {{std::string("2024-5-1T24:00")}, Null()},
            {{std::string("2024-02-30")}, Null()},
            {{std::string("2024-05-01T12:60")}, Null()},
            {{std::string("2012-06-30T23:59:60")}, Null()},
            {{std::string("2024-05-01T00:00+14:30")}, Null()},
            {{std::string("2024-05-01T00:00+08:25")}, Null()},
            {{std::string("2020-12-12   12:12:12")}, Null()},
            {{std::string("2020-12-12T 12:12:12")}, Null()},
            {{std::string("2020-12-12 +12:12:12")}, Null()},
            {{std::string("2020-12-12 -12:12:12")}, Null()},
            {{std::string("2011")}, Null()},
            {{std::string("123-12-12")}, Null()},
            {{std::string("1-12-12")}, Null()},
            {{std::string("00123-12-12")}, Null()},
            {{std::string("1231212")}, Null()},
            {{std::string("0000-02-29")}, Null()}, // should fix
            {{std::string("")}, Null()},
            {{std::string("   ")}, Null()},
            {{std::string("2024−05−01")}, Null()},
            {{std::string("２０２４－０５－０１")}, Null()},
            {{std::string("2024/05/01#12:00")}, Null()},
            {{std::string("2024-05-01&12:00:00")}, Null()},
            {{std::string("2024-5-1-12:00")}, Null()},
            {{std::string("05/01/2024T12")}, Null()},
            {{std::string("2024年05月01日")}, Null()},
            {{std::string("2024-05-01\t12:00:00")}, Null()},
            {{std::string(" 2024-05-01 ")}, Null()},
            {{std::string("  2024-05-01T12:00:00  ")}, Null()},
            {{std::string("2024 - 05 - 01")}, Null()},
            {{std::string("2024-05-01  12:00:00")}, Null()},
            {{std::string("2024.05.01")}, Null()},
            {{std::string("2024.05.01 12.30.45")}, Null()},
            {{std::string("2024/05-01T12:30:45")}, Null()},
            {{std::string("2024-05/01 12.30.45")}, Null()},
            {{std::string("2025/06/15T00:00:00.99999999999999")}, Null()},
            {{std::string("-1")}, Null()},
            {{std::string("-12")}, Null()},
            {{std::string("-1234")}, Null()},
    };
    check_function_for_cast_strict_mode<DataTypeDateTimeV2>(input_types, data_set, "datetime", 6);
}

TEST_F(FunctionCastTest, string_to_datetime6_strict_case_non_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Valid ISO 8601 format with timezone
            {{std::string("2023-07-16T19:20:30.123+08:00")},
             std::string("2023-07-16 19:20:30.123000")},
            {{std::string("2023-07-16T19+08:00")}, std::string("2023-07-16 19:00:00.000000")},
            {{std::string("2023-07-16T1920+08:00")}, std::string("2023-07-16 19:20:00.000000")},
            {{std::string("70-1-1T00:00:00-0000")}, std::string("1970-01-01 08:00:00.000000")},

            // Date with timezone names
            {{std::string("2024-02-29 12:00:00 Europe/Paris")},
             std::string("2024-02-29 19:00:00.000000")},
            {{std::string("2024-05-01T00:00Asia/Shanghai")},
             std::string("2024-05-01 00:00:00.000000")},
            {{std::string("20231005T081530Europe/London")}, std::string("2023-10-05 15:15:30")},
            {{std::string("85-12-25T0000gMt")}, std::string("1985-12-25 08:00:00")},

            // Simple date formats
            {{std::string("2024-05-01")}, std::string("2024-05-01")},
            {{std::string("24-5-1")}, std::string("2024-05-01")},
            {{std::string("2024-05-01 0:1:2.333")}, std::string("2024-05-01 00:01:02.333000")},
            {{std::string("2024-05-01 0:1:2.")}, std::string("2024-05-01 00:01:02")},

            // Compact formats
            {{std::string("20240501 01")}, std::string("2024-05-01 01:00:00")},
            {{std::string("20230716 1920Z")}, std::string("2023-07-17 03:20:00")},
            {{std::string("20240501T0000")}, std::string("2024-05-01")},

            // High precision timestamps
            {{std::string("2024-12-31 23:59:59.9999999999999999999")}, std::string("2025-01-01")},

            // Various timezone offsets
            {{std::string("2020-12-12 13:12:12-03:00")}, std::string("2020-12-13 00:12:12")},
            {{std::string("0023-01-01T00:00Z")},
             std::string(
                     "0023-01-01 08:05:43")}, // CST of shanghai before 1900 is not +080000 but +080543

            // Year cutoff cases
            {{std::string("69-12-31")}, std::string("2069-12-31")},
            {{std::string("70-01-01")}, std::string("1970-01-01")},

            // Compact numeric formats
            {{std::string("230102")}, std::string("2023-01-02")},
            {{std::string("19230101")}, std::string("1923-01-01")},
            {{std::string("20120102030405")}, std::string("2012-01-02 03:04:05")},

            {{std::string("2024-02-29T23:59:59.999999 UTC")},
             std::string("2024-03-01 07:59:59.999999")},
            {{std::string("70-01-01T00:00:00+14")}, std::string("1969-12-31 18:00:00")},
            {{std::string("0023-1-1T1:2:3. -00:00")}, std::string("0023-01-01 09:07:46")},
            {{std::string("20-1-1")}, std::string("2020-01-01")},
            {{std::string("0123-12-12")}, std::string("0123-12-12")},
            {{std::string("01231212")}, std::string("0123-12-12")},
            {{std::string("68-01-01")}, std::string("2068-01-01")},
            {{std::string("69-01-01")}, std::string("2069-01-01")},
            {{std::string("71-01-01")}, std::string("1971-01-01")},
            {{std::string("99-12-31")}, std::string("1999-12-31")},
            {{std::string("00-01-01")}, std::string("2000-01-01")},
            {{std::string("12010203040506.999")}, std::string("1201-02-03 04:05:06.999000")},
            {{std::string("12010203040506.")}, std::string("1201-02-03 04:05:06")},
            {{std::string("2024/05/01")}, std::string("2024-05-01")},
            {{std::string("2024-05-01:12:12:12.1230")}, std::string("2024-05-01 12:12:12.123")},

            // Invalid formats (should return NULL)
            {{std::string("19991231T235960.5UTC")}, Null()},
            {{std::string("2020-05-05 12:30:60")}, Null()},
            {{std::string("2023-07-16T19.123+08:00")}, Null()},
            {{std::string("24012")}, Null()},
            {{std::string("2411 123")}, Null()},
            {{std::string("2024-05-01 01:030:02")}, Null()},
            {{std::string("10000-01-01 00:00:00")}, Null()},
            {{std::string("2024-0131T12:00")}, Null()},
            {{std::string("2024-05-01@00:00")}, Null()},
            {{std::string("20120212051")}, Null()},
            {{std::string("2024-05-01T00:00XYZ")}, Null()},
            {{std::string("2024-5-1T24:00")}, Null()},
            {{std::string("2024-02-30")}, Null()},
            {{std::string("2024-05-01T12:60")}, Null()},
            {{std::string("2012-06-30T23:59:60")}, Null()},
            {{std::string("2024-05-01T00:00+14:30")}, Null()},
            {{std::string("2024-05-01T00:00+08:25")}, Null()},
            {{std::string("2024-02-29T23-59-60ZULU")}, Null()},
            {{std::string("2024 12 31T121212.123456 America/New_York")}, Null()},
            {{std::string("123.123")}, Null()},
            {{std::string("12121")}, Null()},
            {{std::string("2024/05/01#12:00")}, Null()},
            {{std::string("2024-05-01&12:00:00")}, Null()},
            {{std::string("2024-5-1-12:00")}, Null()},
            {{std::string("05/01/2024T12")}, Null()},
            {{std::string("2024年05月01日")}, Null()},
            {{std::string("2024-05-01\t12:00:00")}, Null()},
            {{std::string("-1")}, Null()},
            {{std::string("-12")}, Null()},
            {{std::string("-1234")}, Null()},
            {{std::string("2024~05~01~12~00~00")}, Null()},
            {{std::string("2024#05#01#12#00#00")}, Null()},
            {{std::string("2024- 05- 01")}, Null()},
            {{std::string("2024 -05 -01")}, Null()},
            {{std::string("2024-05 -01T12: 00: 00")}, Null()},
            {{std::string("2024--05--01")}, Null()},
            {{std::string("2024//05//01")}, Null()},
    };
    check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
}

TEST_F(FunctionCastTest, non_strict_test_from_string_to_datetime) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Flexible date formats
            {{std::string("2023-7-4T9-5-3.1Z")}, std::string("2023-07-04 17:05:03.100000")},
            {{std::string("99.12.31 23.59.59+05:30")}, std::string("2000-01-01 02:29:59")},
            {{std::string("2000/01/01T00/00/00")}, std::string("2000-01-01 00:00:00.000000")},
            {{std::string("85 1 1T0 0 0. CST")}, std::string("1985-01-01 00:00:00.000000")},
            {{std::string("2024-02-29T23:59:59.999999Z")},
             std::string("2024-03-01 07:59:59.999999")},
            {{std::string("70-01-01T00:00:00+14")}, std::string("1969-12-31 18:00:00.000000")},
            {{std::string("0023-1-1T1:2:3. -08:05:43")}, Null()},
            {{std::string("2025/06/15T00:00:00")}, std::string("2025-06-15 00:00:00.000000")},
            {{std::string("2025/06/15T00:00:00.99999999999")},
             std::string("2025-06-15 00:00:01.000000")},

            {{std::string("2025/06/15T00:00:00.0-0")}, std::string("2025-06-15 08:00:00")},
            {{std::string("2025/06/15T00:00:00.99999999999")}, std::string("2025-06-15 00:00:01")},
            {{std::string("  2024-05-01T12:00:00  ")}, std::string("2024-05-01 12:00:00")},
            {{std::string("2024.05.01")}, std::string("2024-05-01")},
            {{std::string("2024.05.01 12.30.45")}, std::string("2024-05-01 12:30:45")},
            {{std::string("2024/05-01T12:30:45")}, std::string("2024-05-01 12:30:45")},
            {{std::string("2024-05/01 12.30.45")}, std::string("2024-05-01 12:30:45")},
            {{std::string(" 2024-05-01 ")}, std::string("2024-05-01")},
            {{std::string("2024|05|01")}, std::string("2024-05-01")},
            {{std::string("2024^05^01")}, std::string("2024-05-01")},
            {{std::string("2024~05~01 12~00~00")}, std::string("2024-05-01 12:00:00")},
            {{std::string("2024#05#01T12#00#00")}, std::string("2024-05-01 12:00:00")},
    };
    check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
}

TEST_F(FunctionCastTest, test_from_numeric_to_datetime) {
    // Test casting from Double
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {
                {{123.123}, std::string("2000-01-23 00:00:00.123000")},
                {{20150102030405.0}, std::string("2015-01-02 03:04:05")},
                {{20150102030405.123456}, std::string("2015-01-02 03:04:05.125")}, // not precise
                {{20151231235959.99999999999}, Null()},
                {{1000.0}, Null()},
        };
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
    }

    // Test casting from Int64
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        DataSet data_set = {
                {{int64_t(123)}, std::string("2000-01-23")},
                {{int64_t(1000)}, Null()},
                {{int64_t(20150102030405)}, std::string("2015-01-02 03:04:05")},
        };
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
    }
}

TEST_F(FunctionCastTest, test_from_numeric_to_datetime_invalid) {
    // Test casting from Int64
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        DataSet data_set = {
                {{int64_t(1)}, Null()},
                {{int64_t(22)}, Null()},
                {{int64_t(-222)}, Null()},
                {{int64_t(7777777)}, Null()},
                {{int64_t(2015010203040516)}, Null()},
        };
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTimeV2>(input_types, data_set, "datetime");
    }
    // Test casting from Double
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {
                {{1.}, Null()},
                {{22.223}, Null()},
                {{-222.}, Null()},
                {{7777777.}, Null()},
                {{2015010203040516.}, Null()},
        };
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTimeV2>(input_types, data_set, "datetime");
    }
}

TEST_F(FunctionCastTest, test_from_decimal_to_datetime) {
    // Test casting from Decimal(9,3)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 3, 10}};
        DataSet data_set = {
                {{DECIMAL64(123, 123, 3)}, std::string("2000-01-23 00:00:00.123000")},
                {{DECIMAL64(20150102, 123, 3)}, std::string("2015-01-02 00:00:00.123000")},
                {{DECIMAL64(20151231, 999, 3)}, std::string("2015-12-31 00:00:00.999000")},
                {{DECIMAL64(1000, 0, 3)}, Null()},
                {{Null()}, Null()}};
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
    }
    // Test casting from Decimal(18,6)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 6, 18}};
        DataSet data_set = {
                {{DECIMAL64(123123, 123456, 6)}, Null()},
                {{DECIMAL64(20150102, 123456, 6)}, std::string("2015-01-02 00:00:00.123456")},
                {{DECIMAL64(20151231, 999999, 6)}, std::string("2015-12-31 00:00:00.999999")},
                {{Null()}, Null()}};
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
    }
}

TEST_F(FunctionCastTest, test_from_decimal_to_datetime_invalid) {
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 3, 17}};
        DataSet data_set = {
                {{DECIMAL64(99991231235959, 999, 3)}, Null()},
        };
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 2);
        check_function_for_cast_strict_mode<DataTypeDateTimeV2>(input_types, data_set, "datetime");
    }
}

TEST_F(FunctionCastTest, test_from_date_to_datetime) {
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATEV2}};
        DataSet data_set = {{{std::string("2012-02-05")}, std::string("2012-02-05 00:00:00")},
                            {{Null()}, Null()}};
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 0);
    }
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATEV2}};
        DataSet data_set = {
                {{std::string("2012-02-05")}, std::string("2012-02-05 00:00:00.000000")},
                {{Null()}, Null()}};
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
    }
}

TEST_F(FunctionCastTest, test_from_time_to_datetime) {
    // we mocked current time in function test util
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_TIMEV2, 3}};
        DataSet data_set = {
                {{std::string("500:00:00.123")}, std::string("2019-08-26 20:00:00.123")},
                {{std::string("23:59:59")}, std::string("2019-08-06 23:59:59.000")},
                {{std::string("-128:00:00")}, std::string("2019-07-31 16:00:00.000")},
                {{Null()}, Null()}};
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 3);
    }

    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_TIMEV2, 6}};
        DataSet data_set = {
                {{std::string("500:00:00.123456")}, std::string("2019-08-26 20:00:00.123456")},
                {{std::string("23:59:59")}, std::string("2019-08-06 23:59:59.000000")},
                {{std::string("-128:00:00")}, std::string("2019-07-31 16:00:00.000000")},
                {{Null()}, Null()}};
        check_function_for_cast<DataTypeDateTimeV2>(input_types, data_set, 6);
    }
}
//FIXME: fix cast with different scale then add cases about datetime to datetime

} // namespace doris::vectorized
