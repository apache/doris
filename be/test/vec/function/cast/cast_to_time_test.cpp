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

namespace doris::vectorized {

TEST_F(FunctionCastTest, test_from_string_to_time) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    // now it's wrong. here input can't parse microseconds.
    DataSet data_set = {
            {{std::string("1")}, std::string("00:00:01.000000")},
            {{std::string("123")}, std::string("00:01:23.000000")},
            {{std::string("2005959.12")}, std::string("200:59:59.120000")},
            {{std::string("0.12")}, std::string("00:00:00.120000")},
            {{std::string("00:00:00.12")}, std::string("00:00:00.120000")},
            {{std::string("123.")}, std::string("00:01:23.000000")},
            {{std::string("123.0")}, std::string("00:01:23.000000")},
            {{std::string("123.123")}, std::string("00:01:23.000000")},
            // {{std::string("-1")}, std::string("-00:00:01.000000")}, // FIXME: it's wrong now
            {{std::string("-800:05:05")}, std::string("-800:05:05.000000")},
            {{std::string("-991213.56")}, std::string("-99:12:13.560000")},
            {{std::string("80302.9999999")}, std::string("08:03:02.000000")},
            {{std::string("5656.3000000009")}, std::string("00:56:56.300000")},
            {{std::string("5656.3000007001")}, std::string("00:56:56.300001")},
            {{std::string("   1   ")}, std::string("00:00:01.000000")},
            {{std::string(".123")}, Null()},
            {{std::string(":12:34")}, Null()},
            {{std::string("12-34:56.1")}, Null()},
            {{std::string("12 : 34 : 56")}, std::string("12:34:56.000000")},
            {{std::string("76")}, Null()},
            {{std::string("200595912")}, Null()},
            {{std::string("8385959.9999999")}, std::string("838:59:59.000000")},
            {{Null()}, Null()}};
    check_function_for_cast<DataTypeTimeV2, 6>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_numeric_to_time) {
    // Test casting from Int64
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        DataSet data_set = {{{(int64_t)123456}, std::string("12:34:56.000")},
                            {{(int64_t)-123456}, std::string("-12:34:56.000")},
                            {{(int64_t)123}, std::string("00:01:23.000")},
                            {{(int64_t)8501212}, Null()},
                            {{(int64_t)20001212}, Null()},
                            {{(int64_t)9000000}, Null()},
                            {{(int64_t)67}, Null()},
                            {{Null()}, Null()}};
        check_function_for_cast<DataTypeTimeV2, 3>(input_types, data_set);
    }

    // Test casting from Float64
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {{{(double)123456.0}, std::string("12:34:56.000")},
                            {{(double)-123456.0}, std::string("-12:34:56.000")},
                            {{(double)123.0}, std::string("00:01:23.000")},
                            {{(double)6.99999}, std::string("00:00:06.000")},
                            {{(double)-0.99}, std::string("-00:00:00.990")},
                            {{(double)8501212.0}, Null()},
                            {{(double)20001212.0}, Null()},
                            {{(double)9000000.0}, Null()},
                            {{(double)67.0}, Null()},
                            {{Null()}, Null()}};
        check_function_for_cast<DataTypeTimeV2, 3>(input_types, data_set);
    }

    // Test casting from Decimal Type FIXME:not support now
    {
        InputTypeSet input_types_d32_p0s0 = {{PrimitiveType::TYPE_DECIMAL64, 5, 18}};
        DataSet data_set_d32_p0s0 = {{{DECIMAL64(123456, 0, 5)}, std::string("12:34:56.000")},
                                     {{DECIMAL64(-123456, 0, 5)}, std::string("-12:34:56.000")},
                                     {{DECIMAL64(123, 0, 5)}, std::string("00:01:23.000")},
                                     {{DECIMAL64(6, 99999, 5)}, std::string("00:00:06.000")},
                                     {{DECIMAL64(-0, 99, 5)}, std::string("-00:00:00.990")},
                                     {{DECIMAL64(8501212, 0, 5)}, std::string("838:59:59.000")},
                                     {{DECIMAL64(9000000, 0, 5)}, std::string("838:59:59.000")},
                                     {{DECIMAL64(20001212, 0, 5)}, std::string("838:59:59.000")},
                                     {{DECIMAL64(67, 0, 5)}, std::string("00:01:07.000")},
                                     {{Null()}, Null()}};
        check_function_for_cast<DataTypeTimeV2, 3>(input_types_d32_p0s0, data_set_d32_p0s0, true,
                                                   true);
    }
}

TEST_F(FunctionCastTest, test_from_datetime_to_time) {
    // Cast from DateTimeV2 (as string) to TimeV2
    InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 6}};
    // FIXME: now it's wrong. need support cast with scale. and must store legal value in memory.
    DataSet data_set = {
            {{std::string("2012-02-05 12:12:12.123456")}, std::string("838:59:59.0000")}};
    check_function_for_cast<DataTypeTimeV2, 4>(input_types, data_set, true, false, true);
}

} // namespace doris::vectorized
