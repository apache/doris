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
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"
#include "vec/function/function_test_util.h"

namespace doris::vectorized {
using namespace ut_type;

IPV4 ipv4_from_string(const std::string& str) {
    IPV4 ipv4;
    IPv4Value::from_string(ipv4, str.data(), str.size());
    return ipv4;
}

IPV6 ipv6_from_string(const std::string& str) {
    IPV6 ipv6;
    IPv6Value::from_string(ipv6, str.data(), str.size());
    return ipv6;
}

TEST_F(FunctionCastTest, test_from_string_to_ipv4) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Edge cases
            {{std::string("null")}, Null()},
            {{std::string("")}, Null()},
            {{std::string(" ")}, Null()},

            // Invalid formats
            {{std::string("x")}, Null()},
            {{std::string("192.168")}, Null()},
            {{std::string("192.168.0")}, Null()},
            {{std::string("192.168.0.256")}, Null()},
            {{std::string("192.168.0.1.5")}, Null()},
            {{std::string("192.168.0.a")}, Null()},
            {{std::string("2001:db8::1")}, Null()}, // IPv6 format

            // Valid formats
            {{std::string("0.0.0.0")}, ipv4_from_string("0.0.0.0")},
            {{std::string("127.0.0.1")}, ipv4_from_string("127.0.0.1")},
            {{std::string("192.168.0.1")}, ipv4_from_string("192.168.0.1")},
            {{std::string("255.255.255.255")}, ipv4_from_string("255.255.255.255")},

            // With spaces
            {{std::string(" 10.0.0.1 ")}, ipv4_from_string("10.0.0.1")},
            {{std::string("  172.16.0.1  ")}, ipv4_from_string("172.16.0.1")},

            // With leading zeros
            {{std::string("010.000.000.001")}, ipv4_from_string("10.0.0.1")},
            {{std::string("001.002.003.004")}, ipv4_from_string("1.2.3.4")},
    };
    check_function_for_cast<DataTypeIPv4>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_string_to_ipv6) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Edge cases
            {{std::string("null")}, Null()},
            {{std::string("")}, Null()},
            {{std::string(" ")}, Null()},

            // Invalid formats
            {{std::string("x")}, Null()},
            {{std::string("2001:db8::gggg")}, Null()},
            {{std::string("2001:db8:::1")}, Null()},           // Double ::
            {{std::string("2001:db8::1::2")}, Null()},         // Multiple ::
            {{std::string("1:1:::1")}, Null()},                // Double ::
            {{std::string("1:1:1::1:1:1:1:1")}, Null()},       //  :: not use
            {{std::string("2001:db8:0:0:0:0:0:0:1")}, Null()}, // Too many segments

            // Valid formats - standard
            {{std::string("::1")}, ipv6_from_string("::1")}, // Localhost
            {{std::string("::")}, ipv6_from_string("::")},   // All zeros
            {{std::string("2001:db8::1")}, ipv6_from_string("2001:db8::1")},
            {{std::string("2001:db8:0:0:0:0:0:1")},
             ipv6_from_string("2001:db8:0:0:0:0:0:1")}, // Full form
            {{std::string("2001:db8::0:1")},
             ipv6_from_string("2001:db8::0:1")}, // Compressed with non-zero after ::

            // Mixed IPv6/IPv4
            {{std::string("::ffff:192.168.0.1")},
             ipv6_from_string("::ffff:192.168.0.1")}, // IPv4-mapped IPv6
            {{std::string("::ffff:c0a8:1")},
             ipv6_from_string("::ffff:c0a8:1")}, // IPv4-mapped IPv6 in hex
            {{std::string("::192.168.0.1")},
             ipv6_from_string("::192.168.0.1")}, // IPv4-compatible IPv6

            // With spaces
            {{std::string(" 2001:db8::1 ")}, ipv6_from_string("2001:db8::1")},
            {{std::string("  fe80::1  ")}, ipv6_from_string("fe80::1")},

            // Case sensitivity
            {{std::string("2001:DB8::1")}, ipv6_from_string("2001:db8::1")},
            {{std::string("FEDC:BA98:7654:3210:FEDC:BA98:7654:3210")},
             ipv6_from_string("fedc:ba98:7654:3210:fedc:ba98:7654:3210")},
    };
    check_function_for_cast<DataTypeIPv6>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_string_to_ipv4_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    // Valid formats in strict mode
    {
        DataSet data_set = {
                {{std::string("0.0.0.0")}, ipv4_from_string("0.0.0.0")},
                {{std::string("127.0.0.1")}, ipv4_from_string("127.0.0.1")},
                {{std::string("192.168.0.1")}, ipv4_from_string("192.168.0.1")},
                {{std::string("255.255.255.255")}, ipv4_from_string("255.255.255.255")},
                {{std::string(" 10.0.0.1 ")}, ipv4_from_string("10.0.0.1")},
                {{std::string("010.000.000.001")}, ipv4_from_string("10.0.0.1")},
        };
        check_function_for_cast_strict_mode<DataTypeIPv4>(input_types, data_set);
    }

    // Invalid formats in strict mode - each should raise error
    {
        check_function_for_cast_strict_mode<DataTypeIPv4>(input_types,
                                                          {{{std::string("x")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv4>(
                input_types, {{{std::string("192.168")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv4>(
                input_types, {{{std::string("192.168.0.256")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv4>(input_types,
                                                          {{{std::string("")}, Null()}}, "fail");
    }
}

TEST_F(FunctionCastTest, test_from_string_to_ipv6_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    // Valid formats in strict mode
    {
        DataSet data_set = {
                {{std::string("::1")}, ipv6_from_string("::1")},
                {{std::string("::")}, ipv6_from_string("::")},
                {{std::string("2001:db8::1")}, ipv6_from_string("2001:db8::1")},
                {{std::string("2001:db8:0:0:0:0:0:1")}, ipv6_from_string("2001:db8:0:0:0:0:0:1")},
                {{std::string("::ffff:192.168.0.1")}, ipv6_from_string("::ffff:192.168.0.1")},
                {{std::string(" 2001:db8::1 ")}, ipv6_from_string("2001:db8::1")},
        };
        check_function_for_cast_strict_mode<DataTypeIPv6>(input_types, data_set);
    }

    // Invalid formats in strict mode - each should raise error
    {
        check_function_for_cast_strict_mode<DataTypeIPv6>(input_types,
                                                          {{{std::string("x")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv6>(
                input_types, {{{std::string("2001:db8::gggg")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv6>(
                input_types, {{{std::string("2001:db8:::1")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv6>(
                input_types, {{{std::string("1:1:::1")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv6>(
                input_types, {{{std::string(" 1:1:1::1:1:1:1:1")}, Null()}}, "fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeIPv6>(input_types,
                                                          {{{std::string("")}, Null()}}, "fail");
    }
}

TEST_F(FunctionCastTest, test_ipv4_to_ipv6) {
    InputTypeSet input_types = {PrimitiveType::TYPE_IPV4};
    DataSet data_set = {
            {{ipv4_from_string("0.0.0.0")}, ipv6_from_string("::ffff:0.0.0.0")},
            {{ipv4_from_string("127.0.0.1")}, ipv6_from_string("::ffff:127.0.0.1")},
            {{ipv4_from_string("192.168.0.1")}, ipv6_from_string("::ffff:192.168.0.1")},
            {{ipv4_from_string("255.255.255.255")}, ipv6_from_string("::ffff:255.255.255.255")},
    };
    check_function_for_cast<DataTypeIPv6>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_ip_to_string) {
    // IPv4 to string
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_IPV4};
        DataSet data_set = {
                {{ipv4_from_string("0.0.0.0")}, std::string("0.0.0.0")},
                {{ipv4_from_string("127.0.0.1")}, std::string("127.0.0.1")},
                {{ipv4_from_string("192.168.0.1")}, std::string("192.168.0.1")},
                {{ipv4_from_string("255.255.255.255")}, std::string("255.255.255.255")},
        };
        check_function_for_cast<DataTypeString>(input_types, data_set);
    }

    // IPv6 to string
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_IPV6};
        DataSet data_set = {
                {{ipv6_from_string("::")}, std::string("::")},
                {{ipv6_from_string("::1")}, std::string("::1")},
                {{ipv6_from_string("2001:db8::1")}, std::string("2001:db8::1")},
                {{ipv6_from_string("::ffff:192.168.0.1")}, std::string("::ffff:192.168.0.1")},
        };
        check_function_for_cast<DataTypeString>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_non_strict_cast_string_to_ipv4) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("192.168.1.1")}, ipv4_from_string("192.168.1.1")},
            {{std::string("0.0.0.0")}, ipv4_from_string("0.0.0.0")},
            {{std::string("255.255.255.255")}, ipv4_from_string("255.255.255.255")},
            {{std::string("10.20.30.40")}, ipv4_from_string("10.20.30.40")},
            {{std::string(" 192.168.1.1 ")}, ipv4_from_string("192.168.1.1")},
            {{std::string("192.168.01.1")}, ipv4_from_string("192.168.1.1")},
            {{std::string("1.2.3")}, Null()},
            {{std::string("1.2.3.4.5")}, Null()},
            {{std::string("256.0.0.1")}, Null()},
            {{std::string("1.300.2.3")}, Null()},
            {{std::string("1.2.3.")}, Null()},
            {{std::string(".1.2.3")}, Null()},
            {{std::string("1..2.3")}, Null()},
            {{std::string("a.b.c.d")}, Null()},
            {{std::string("1.2.+3.4")}, Null()},
    };
    check_function_for_cast<DataTypeIPv4>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_non_strict_cast_string_to_ipv6) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2001:db8:85a3:0000:0000:8a2e:0370:7334")},
             ipv6_from_string("2001:db8:85a3:0000:0000:8a2e:0370:7334")},
            {{std::string("::")}, ipv6_from_string("::")},
            {{std::string("2001:db8::")}, ipv6_from_string("2001:db8::")},
            {{std::string("::ffff:192.168.1.1")}, ipv6_from_string("::ffff:192.168.1.1")},
            {{std::string(" 2001:db8::1 ")}, ipv6_from_string("2001:db8::1")},
            {{std::string("2001:db8::1::2")}, Null()},
            {{std::string("2001:db8:85a3:0000:0000:8a2e:0370:7334:1234")}, Null()},
            {{std::string("2001:db8:85a3:0000:8a2e:0370")}, Null()},
            {{std::string("2001:db8:85g3:0000:0000:8a2e:0370:7334")}, Null()},
            {{std::string("2001:db8::ffff:192.168.1.260")}, Null()},
            {{std::string("2001:db8::ffff:192.168..1")}, Null()},
            {{std::string("2001:0db8:85a3:::8a2e:0370:7334")}, Null()},
            {{std::string("20001:db8::1")}, Null()},
    };
    check_function_for_cast<DataTypeIPv6>(input_types, data_set);
}

} // namespace doris::vectorized
