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

#include <limits>

#include "exec/common/int_exp.h"
#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

template <typename Decimal>
void check_format_round_padding(PrimitiveType type, int precision) {
    const InputTypeSet arguments = {{type, 2, precision}, TYPE_INT};
    DataSet data;
    for (int places : {2, 9, 10, 18, 19, 20, 38, 39, 1024}) {
        const std::string zeros(places - 2, '0');
        data.push_back({{Decimal(144), places}, std::string("1.44") + zeros});
        data.push_back({{Decimal(-144), places}, std::string("-1.44") + zeros});
        data.push_back({{Decimal(-44), places}, std::string("-0.44") + zeros});
        data.push_back({{Decimal(1), places}, std::string("0.01") + zeros});
        data.push_back({{Decimal(0), places}, std::string("0.00") + zeros});
    }
    data.push_back({{Null(), 20}, Null()});
    data.push_back({{Decimal(144), Null()}, Null()});
    check_function_all_arg_comb<DataTypeString, true>("format_round", arguments, data);
}

TEST(FormatRoundTest, decimal_padding) {
    check_format_round_padding<Decimal32>(TYPE_DECIMAL32, 9);
    check_format_round_padding<Decimal64>(TYPE_DECIMAL64, 18);
    check_format_round_padding<Decimal128V3>(TYPE_DECIMAL128I, 20);
    check_format_round_padding<Decimal128V3>(TYPE_DECIMAL128I, 38);
}

TEST(FormatRoundTest, decimal64_rounding) {
    const InputTypeSet arguments = {{TYPE_DECIMAL64, 17, 18}, TYPE_INT};
    const DataSet data = {
            {{Decimal64(Int64(112499999999999999)), 2}, std::string("1.12")},
            {{Decimal64(Int64(-112499999999999999)), 2}, std::string("-1.12")},
            {{Decimal64(Int64(112499999999999999)), 10}, std::string("1.1250000000")},
            {{Decimal64(Int64(112345678901234567)), 17}, std::string("1.12345678901234567")},
            {{Decimal64(Int64(-112345678901234567)), 16}, std::string("-1.1234567890123457")},
            {{Decimal64(Int64(999999999999999999)), 16}, std::string("10.0000000000000000")},
            {{Decimal64(Int64(-999999999999999999)), 10}, std::string("-10.0000000000")},
            {{Decimal64(Int64(999999999999999999)), 0}, std::string("10")},
            {{Decimal64(Int64(-49999999999999999)), 0}, std::string("-0")},
            {{Decimal64(Int64(-50000000000000000)), 0}, std::string("-1")}};
    check_function_all_arg_comb<DataTypeString, true>("format_round", arguments, data);
}

TEST(FormatRoundTest, decimal128_rounding) {
    const InputTypeSet arguments = {{TYPE_DECIMAL128I, 38, 38}, TYPE_INT};
    const Int128 fraction = common::exp10_i128(37) + 1234567890123456789LL;
    const Int128 max_fraction = common::exp10_i128(38) - 1;
    const DataSet data = {
            {{Decimal128V3(fraction), 38}, std::string("0.10000000000000000001234567890123456789")},
            {{Decimal128V3(-fraction), 38},
             std::string("-0.10000000000000000001234567890123456789")},
            {{Decimal128V3(fraction), 20}, std::string("0.10000000000000000001")},
            {{Decimal128V3(-fraction), 37},
             std::string("-0.1000000000000000000123456789012345679")},
            {{Decimal128V3(max_fraction), 37}, std::string("1.") + std::string(37, '0')},
            {{Decimal128V3(-max_fraction), 37}, std::string("-1.") + std::string(37, '0')},
            {{Decimal128V3(max_fraction), 1024},
             std::string("0.") + std::string(38, '9') + std::string(986, '0')}};
    check_function_all_arg_comb<DataTypeString, true>("format_round", arguments, data);
}

TEST(FormatRoundTest, decimal256_padding) {
    check_format_round_padding<Decimal256>(TYPE_DECIMAL256, 76);
}

TEST(FormatRoundTest, decimal256_rounding) {
    const InputTypeSet arguments = {{TYPE_DECIMAL256, 76, 76}, TYPE_INT};
    const auto max_fraction = common::exp10_i256(76) - 1;
    DataSet data;
    for (int places : {0, 1, 20, 38, 39, 40, 74, 75, 76, 77, 1024}) {
        std::string expected;
        if (places == 0) {
            expected = "1";
        } else if (places < 76) {
            expected = std::string("1.") + std::string(places, '0');
        } else {
            expected = std::string("0.") + std::string(76, '9') + std::string(places - 76, '0');
        }
        data.push_back({{Decimal256(max_fraction), places}, expected});
        data.push_back({{Decimal256(-max_fraction), places}, std::string("-") + expected});
    }
    const auto fraction = common::exp10_i256(75) + 1234567890123456789LL;
    data.push_back({{Decimal256(fraction), 76},
                    std::string("0.1") + std::string(56, '0') + "1234567890123456789"});
    data.push_back({{Decimal256(-fraction), 75},
                    std::string("-0.1") + std::string(56, '0') + "123456789012345679"});
    data.push_back({{Decimal256(fraction), 20}, std::string("0.10000000000000000000")});
    data.push_back({{Decimal256(1), 76}, std::string("0.") + std::string(75, '0') + "1"});
    data.push_back({{Decimal256(-1), 75}, std::string("-0.") + std::string(75, '0')});
    check_function_all_arg_comb<DataTypeString, true>("format_round", arguments, data);
}

TEST(FormatRoundTest, decimal256_large_integer) {
    const auto max_value = common::exp10_i256(76) - 1;
    std::string max_integer = "9";
    std::string carry_integer = "100";
    for (int i = 0; i < 25; ++i) {
        max_integer += ",999";
    }
    for (int i = 0; i < 24; ++i) {
        carry_integer += ",000";
    }
    check_function_all_arg_comb<DataTypeString, true>(
            "format_round", {{TYPE_DECIMAL256, 0, 76}, TYPE_INT},
            {{{Decimal256(max_value), 0}, max_integer},
             {{Decimal256(-max_value), 20},
              std::string("-") + max_integer + "." + std::string(20, '0')}});
    check_function_all_arg_comb<DataTypeString, true>(
            "format_round", {{TYPE_DECIMAL256, 2, 76}, TYPE_INT},
            {{{Decimal256(max_value), 1}, carry_integer + ".0"},
             {{Decimal256(-max_value), 0}, std::string("-") + carry_integer}});
}

TEST(FormatRoundTest, integer_padding) {
    check_function_all_arg_comb<DataTypeString, true>(
            "format_round", {TYPE_BIGINT, TYPE_INT},
            {{{Int64(-1234), 20}, std::string("-1,234.") + std::string(20, '0')},
             {{std::numeric_limits<Int64>::min(), 0}, std::string("-9,223,372,036,854,775,808")}});
    check_function_all_arg_comb<DataTypeString, true>(
            "format_round", {TYPE_LARGEINT, TYPE_INT},
            {{{Int128(1234), 1024}, std::string("1,234.") + std::string(1024, '0')}});
}

TEST(FormatRoundTest, money_format_high_scale) {
    check_function_all_arg_comb<DataTypeString, true>(
            "money_format", {{TYPE_DECIMAL32, 8, 9}},
            {{{Decimal32(112499999)}, std::string("1.12")}});
    check_function_all_arg_comb<DataTypeString, true>(
            "money_format", {{TYPE_DECIMAL64, 17, 18}},
            {{{Decimal64(Int64(112499999999999999))}, std::string("1.12")}});
    const Int128 value =
            common::exp10_i128(37) + common::exp10_i128(36) + common::exp10_i128(34) * 25 - 1;
    check_function_all_arg_comb<DataTypeString, true>(
            "money_format", {{TYPE_DECIMAL128I, 37, 38}},
            {{{Decimal128V3(value)}, std::string("1.12")}});
}

} // namespace doris
