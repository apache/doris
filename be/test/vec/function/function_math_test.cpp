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

#include <climits>
#include <cstdint>
#include <limits>
#include <string>

#include "function_test_util.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

using namespace ut_type;

TEST(MathFunctionTest, acos_test) {
    std::string func_name = "acos"; //[-1,1] -->[0,pi]

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{-1.0}, 3.1415926535897931},
                        {{0.0}, M_PI / 2},
                        {{0.5}, 1.0471975511965979},
                        //{{3.14},nan("")},
                        {{1.0}, 0.0}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, acosh_test) {
    std::string func_name = "acosh"; // acosh(x) = ln(x + sqrt(x^2 - 1)), x ∈ [1, +∞)

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{1.0}, 0.0},
                        {{2.0}, 1.3169578969248166},
                        {{3.0}, 1.7627471740390861},
                        {{10.0}, 2.9932228461263808},
                        {{100.0}, 5.298292365610484}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, asin_test) {
    std::string func_name = "asin"; //[-1,1] -->[-pi_2, pi_2]

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {
            {{-1.0}, -M_PI / 2}, {{0.0}, 0.0}, {{0.5}, 0.52359877559829893}, {{1.0}, M_PI / 2}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, asinh_test) {
    std::string func_name = "asinh"; // asinh(x) = ln(x + sqrt(x^2 + 1)), x ∈ (-∞, +∞)

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{0.0}, 0.0},
                        {{1.0}, 0.8813735870195430},
                        {{-1.0}, -0.8813735870195430},
                        {{2.0}, 1.4436354751788103},
                        {{-2.0}, -1.4436354751788103}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, atan_test) {
    std::string func_name = "atan"; //[-,+] -->(pi_2,pi_2)

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{-1.0}, -0.78539816339744828},
                        {{0.0}, 0.0},
                        {{0.5}, 0.46364760900080609},
                        {{1.0}, 0.78539816339744828}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, atanh_test) {
    std::string func_name = "atanh"; // atanh(x) = 0.5 * ln((1 + x) / (1 - x)), x ∈ (-1, 1)

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{0.0}, 0.0},
                        {{0.5}, 0.5493061443340548},
                        {{-0.5}, -0.5493061443340548},
                        {{0.9}, 1.4722194895832204},
                        {{-0.9}, -1.4722194895832204}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, cos_test) {
    std::string func_name = "cos";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{-1.0}, 0.54030230586813977},
                        {{0.0}, 1.0},
                        {{0.5}, 0.87758256189037276},
                        {{M_PI}, -1.0},
                        {{1.0}, 0.54030230586813977}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, sin_test) {
    std::string func_name = "sin";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{-1.0}, -0.8414709848078965},
                        {{0.0}, 0.0},
                        {{0.5}, 0.479425538604203},
                        {{M_PI / 2}, 1.0},
                        {{1.0}, 0.8414709848078965}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, sinh_test) {
    std::string func_name = "sinh"; // sinh(x) = (e^x - e^(-x)) / 2, x ∈ (-∞, +∞)

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{0.0}, 0.0},
                        {{1.0}, 1.1752011936438014},
                        {{-1.0}, -1.1752011936438014},
                        {{2.0}, 3.626860407847019},
                        {{-2.0}, -3.626860407847019}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, sqrt_test) {
    std::string func_name = "sqrt"; //sqrt(x) x>=0

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{0.0}, 0.0},
                        {{2.0}, 1.4142135623730951},
                        {{9.0}, 3.0},
                        {{1000.0}, 31.622776601683793}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, cbrt_test) {
    std::string func_name = "cbrt";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {
            {{0.0}, 0.0}, {{2.0}, 1.2599210498948734}, {{8.0}, 2.0}, {{-1000.0}, -10.0}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, cot_test) {
    std::string func_name = "cot";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{1.0}, 0.6420926159343306}, {{M_PI / 4}, 1.0000000000000002}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, sec_test) {
    std::string func_name = "sec";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{1.0}, 1.8508157176809255}, {{1000.0}, 1.7781600385912715}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, cosec_test) {
    std::string func_name = "csc";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {
            {{1.0}, 1.1883951057781212}, {{2.0}, 1.0997501702946164}, {{1000.0}, 1.20936599707935}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, tan_test) {
    std::string func_name = "tan"; //tan(x)

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{0.0}, 0.0},
                        {{2.0}, -2.1850398632615189},
                        {{-1.0}, -1.5574077246549023},
                        {{1000.0}, 1.4703241557027185}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, exp_test) {
    std::string func_name = "exp";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{-1.0}, 0.36787944117144233},
                        {{0.0}, 1.0},
                        {{0.5}, 1.6487212707001282},
                        {{-800.0}, 0.0},
                        {{1.0}, 2.7182818284590451}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, ln_test) {
    std::string func_name = "ln"; // ln(x) x>0

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{1.0}, 0.0},
                        {{0.5}, -0.69314718055994529},
                        {{-2.0}, Null()},
                        {{100.0}, 4.6051701859880918},
                        {{1000.0}, 6.9077552789821368}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, log2_test) {
    std::string func_name = "log2"; // log2(x) x>0

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{1.0}, 0.0},
                        {{0.5}, -1.0},
                        {{100.0}, 6.6438561897747244},
                        {{1000.0}, 9.965784284662087},
                        {{-1.0}, Null()}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, log10_test) {
    std::string func_name = "log10"; // log10(x) x>0

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{1.0}, 0.0},
                        {{0.5}, -0.3010299956639812},
                        {{100.0}, 2.0},
                        {{-1.0}, Null()},
                        {{1000.0}, 3.0}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, log_test) {
    std::string func_name = "log"; // log(x,y) x>0 y>0

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {
            {{10.0, 1.0}, 0.0},    {{10.0, 100.0}, 2.0},  {{0.1, 5.0}, -0.69897000433601886},
            {{-2.0, 5.0}, Null()}, {{2.0, -5.0}, Null()}, {{2.0, 0.5}, -1.0}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, pow_test) {
    std::string func_name = "pow"; // pow(x,y)

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{10.0, 1.0}, 10.0},
                        {{10.0, 10.0}, 10000000000.0},
                        {{100.0, -2.0}, 0.0001},
                        {{2.0, 0.5}, 1.4142135623730951}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, ceil_test) {
    std::string func_name = "ceil";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{2.3}, 3.0}, {{2.8}, 3.0}, {{-2.3}, -2.0}, {{2.8}, 3.0}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, floor_test) {
    std::string func_name = "floor";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{2.3}, 2.0}, {{2.8}, 2.0}, {{-2.3}, -3.0}, {{-2.8}, -3.0}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, degrees_test) {
    std::string func_name = "degrees"; // degrees(x) rad-->C

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{1.0}, 57.295779513082323},
                        {{M_PI / 2}, 90.0},
                        {{0.0}, 0.0},
                        {{-2.0}, -114.59155902616465}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, radians_test) {
    std::string func_name = "radians"; // radians(x) C--->rad

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{30.0}, 0.52359877559829882},
                        {{90.0}, M_PI / 2},
                        {{0.0}, 0.0},
                        {{-60.0}, -1.0471975511965976}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, abs_test) {
    std::string func_name = "abs";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

        DataSet data_set = {{{Null()}, Null()},
                            {{-0.0123}, 0.0123},
                            {{90.45}, 90.45},
                            {{0.0}, 0.0},
                            {{-60.0}, 60.0}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};

        DataSet data_set = {{{Null()}, Null()},
                            {{INT(3)}, BIGINT(3)},
                            {{INT(-3)}, BIGINT(3)},
                            {{INT(0)}, BIGINT(0)},
                            {{INT(-60)}, BIGINT(60)},
                            {{INT(INT_MAX)}, BIGINT(INT_MAX)},
                            {{INT(INT_MIN)}, BIGINT(-1LL * INT_MIN)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, positive_test) {
    std::string func_name = "positive";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

        DataSet data_set = {{{0.0123}, 0.0123}, {{90.45}, 90.45}, {{0.0}, 0.0}, {{-60.0}, -60.0}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};

        DataSet data_set = {{{(int32_t)3}, (int32_t)3},
                            {{(int32_t)-3}, (int32_t)-3},
                            {{(int32_t)0}, (int32_t)0},
                            {{(int32_t)-60}, (int32_t)-60}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {AnyType {Notnull {PrimitiveType::TYPE_DECIMAL64}, 5, 11}};

        DataSet data_set = {
                {{DECIMAL64(12345, 123, 5)}, {DECIMAL64(12345, 123, 5)}},
                {{DECIMAL64(12345, 12345, 5)}, {DECIMAL64(12345, 12345, 5)}},
        };

        static_cast<void>(
                check_function<DataTypeDecimal64, false>(func_name, input_types, data_set, 5, 11));
    }
    // negative case
    {
        InputTypeSet input_types = {AnyType {Notnull {PrimitiveType::TYPE_DECIMAL64}, 5, 11}};

        DataSet data_set = {
                // column's data should keep all the same scale. will be reinterpreted as the same scale.
                {{DECIMAL64(12345, 123, 3)}, {DECIMAL64(12345, 12300, 5)}},
        };

        static_cast<void>(check_function<DataTypeDecimal64, false>(func_name, input_types, data_set,
                                                                   5, 11, false, true));
    }
    // negative case
    {
        InputTypeSet input_types = {AnyType {Notnull {PrimitiveType::TYPE_DECIMAL64}, 5, 11}};

        DataSet data_set = {
                {{DECIMAL64(12345, 12345, 5)}, {DECIMAL64(12345, 12345, 5)}},
        };

        static_cast<void>(check_function<DataTypeDecimal64, false>(func_name, input_types, data_set,
                                                                   6, 12, false, true));
    }
}

TEST(MathFunctionTest, negative_test) {
    std::string func_name = "negative";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

        DataSet data_set = {{{0.0123}, -0.0123}, {{90.45}, -90.45}, {{0.0}, 0.0}, {{-60.0}, 60.0}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};

        DataSet data_set = {{{(int32_t)3}, (int32_t)-3},
                            {{(int32_t)-3}, (int32_t)3},
                            {{(int32_t)0}, (int32_t)0},
                            {{(int32_t)-60}, (int32_t)60}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, sign_test) {
    std::string func_name = "sign"; // sign(x) // 1 0 -1

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};

        DataSet data_set = {{{(int32_t)30}, (int8_t)1.0},
                            {{(int32_t)0}, (int8_t)0.0},
                            {{(int32_t)-10}, (int8_t)-1.0}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

        DataSet data_set = {{{30.7}, (int8_t)1.0}, {{0.0}, (int8_t)0.0}, {{-10.6}, (int8_t)-1.0}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, round_test) {
    std::string func_name = "round"; // round(double) && round(double, int)

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

        DataSet data_set = {{{30.1}, 30.0}, {{90.6}, 91.0}, {{Null()}, Null()},
                            {{0.0}, 0.0},   {{-1.1}, -1.0}, {{-60.7}, -61.0}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, round_bankers_test) {
    std::string func_name = "round_bankers";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

        DataSet data_set = {{{0.4}, 0.0}, {{-3.5}, -4.0}, {{4.5}, 4.0}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, least_test) {
    std::string func_name = "least";

    InputTypeSet input_types = {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT};

    DataSet data_set = {
            {{3, 2}, 2}, {{3, 3}, 3}, {{Null(), -2}, Null()}, {{193, -2}, -2}, {{193, -1}, -1}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, greatest_test) {
    std::string func_name = "greatest";

    InputTypeSet input_types = {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT};

    DataSet data_set = {
            {{3, 2}, 3}, {{3, 3}, 3}, {{Null(), -2}, Null()}, {{193, -2}, 193}, {{193, -1}, 193}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, bin_test) {
    std::string func_name = "bin";

    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};

    DataSet data_set = {{{(int64_t)10}, std::string("1010")},
                        {{(int64_t)1}, std::string("1")},
                        {{(int64_t)0}, std::string("0")},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, hex_test) {
    std::string func_name = "hex"; // hex(int)

    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};

    DataSet data_set = {{{Null()}, Null()},
                        {{(int64_t)-1}, std::string("FFFFFFFFFFFFFFFF")},
                        {{(int64_t)-2}, std::string("FFFFFFFFFFFFFFFE")},
                        {{(int64_t)12}, std::string("C")},
                        {{(int64_t)144}, std::string("90")},
                        {{(int64_t)151233}, std::string("24EC1")},
                        {{(int64_t)0}, std::string("0")},
                        {{(int64_t)9223372036854775807}, std::string("7FFFFFFFFFFFFFFF")},
                        {{(int64_t)-7453337203775808}, std::string("FFE5853AB393E6C0")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, random_test) {
    std::string func_name = "random"; // random(x)
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_BIGINT}};
    DataSet data_set = {{{Null()}, Null()},
                        {{(int64_t)0}, 0.15979336337046085},
                        {{(int64_t)10}, 0.60128310734097479},
                        {{(int64_t)123}, 0.31320017867847078},
                        {{(int64_t)std::numeric_limits<int64_t>::max()}, 0.20676730979843233},
                        {{(int64_t)std::numeric_limits<int64_t>::min()}, 0.15979336337046085}};

    for (const auto& data : data_set) {
        DataSet data_line = {data};
        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_line));
    }
}

TEST(MathFunctionTest, conv_test) {
    std::string func_name = "conv";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_TINYINT,
                                    PrimitiveType::TYPE_TINYINT};
        DataSet data_set = {{{Null(), Null(), Null()}, Null()},
                            {{BIGINT(230), TINYINT(10), TINYINT(16)}, VARCHAR("E6")},
                            {{BIGINT(15), TINYINT(10), TINYINT(2)}, VARCHAR("1111")}};

        for (const auto& data : data_set) {
            DataSet data_line = {data};
            static_cast<void>(
                    check_function<DataTypeString, true>(func_name, input_types, data_line));
        }
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_TINYINT,
                                    PrimitiveType::TYPE_TINYINT};
        DataSet data_set = {{{Null(), Null(), Null()}, Null()},
                            {{VARCHAR("ff"), TINYINT(16), TINYINT(10)}, VARCHAR("255")}};

        for (const auto& data : data_set) {
            DataSet data_line = {data};
            static_cast<void>(
                    check_function<DataTypeString, true>(func_name, input_types, data_line));
        }
    }
}

TEST(MathFunctionTest, money_format_test) {
    std::string func_name = "money_format";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        DataSet data_set = {{{Null()}, Null()},
                            {{BIGINT(17014116)}, VARCHAR("17,014,116.00")},
                            {{BIGINT(-17014116)}, VARCHAR("-17,014,116.00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_LARGEINT};
        DataSet data_set = {{{Null()}, Null()},
                            {{LARGEINT(17014116)}, VARCHAR("17,014,116.00")},
                            {{LARGEINT(-17014116)}, VARCHAR("-17,014,116.00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {{{Null()}, Null()},
                            {{DOUBLE(17014116.67)}, VARCHAR("17,014,116.67")},
                            {{DOUBLE(-17014116.67)}, VARCHAR("-17,014,116.67")},
                            {{DOUBLE(-123.45)}, VARCHAR("-123.45")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMALV2};
        DataSet data_set = {{{Null()}, Null()},
                            {{DECIMALV2(17014116.67)}, VARCHAR("17,014,116.67")},
                            {{DECIMALV2(-17014116.67)}, VARCHAR("-17,014,116.67")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL64};
        DataSet data_set = {{{Null()}, Null()},
                            {{DECIMAL64(17014116, 670000000, 9)}, VARCHAR("17,014,116.67")},
                            {{DECIMAL64(-17014116, -670000000, 9)}, VARCHAR("-17,014,116.67")}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(MathFunctionTest, format_round_test) {
    std::string func_name = "format_round";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_INT};
        DataSet data_set = {{{Null(), INT(2)}, Null()},
                            {{BIGINT(17014116), INT(2)}, VARCHAR("17,014,116.00")},
                            {{BIGINT(-17014116), INT(2)}, VARCHAR("-17,014,116.00")},
                            {{BIGINT(1), INT(0)}, VARCHAR("1")},
                            {{BIGINT(123456), INT(0)}, VARCHAR("123,456")},
                            {{BIGINT(123456), INT(3)}, VARCHAR("123,456.000")},
                            {{BIGINT(123456), INT(10)}, VARCHAR("123,456.0000000000")},
                            {{BIGINT(123456), INT(20)}, VARCHAR("123,456.00000000000000000000")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {{Null(), INT(2)}, Null()},
                {{LARGEINT(17014116), INT(2)}, VARCHAR("17,014,116.00")},
                {{LARGEINT(-17014116), INT(2)}, VARCHAR("-17,014,116.00")},
                {{LARGEINT(1), INT(0)}, VARCHAR("1")},
                {{LARGEINT(123456), INT(0)}, VARCHAR("123,456")},
                {{LARGEINT(123456), INT(3)}, VARCHAR("123,456.000")},
                {{LARGEINT(123456), INT(10)}, VARCHAR("123,456.0000000000")},
                {{LARGEINT(123456), INT(20)}, VARCHAR("123,456.00000000000000000000")},
                {{LARGEINT(123456789123456789), INT(2)}, VARCHAR("123,456,789,123,456,789.00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_INT};
        DataSet data_set = {{{Null(), INT(2)}, Null()},
                            {{DOUBLE(17014116.67), INT(2)}, VARCHAR("17,014,116.67")},
                            {{DOUBLE(-17014116.67), INT(2)}, VARCHAR("-17,014,116.67")},
                            {{DOUBLE(-123.45), INT(2)}, VARCHAR("-123.45")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMALV2, PrimitiveType::TYPE_INT};
        DataSet data_set = {{{Null(), INT(2)}, Null()},
                            {{DECIMALV2(17014116.67), INT(2)}, VARCHAR("17,014,116.67")},
                            {{DECIMALV2(-17014116.67), INT(2)}, VARCHAR("-17,014,116.67")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 5, 18},
                                    PrimitiveType::TYPE_INT};
        DataSet data_set = {{{Null(), INT(2)}, Null()},
                            {{DECIMAL64(17014116, 67000, 5), INT(2)}, VARCHAR("17,014,116.67")},
                            {{DECIMAL64(-17014116, -67000, 5), INT(2)}, VARCHAR("-17,014,116.67")}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
    // negative case
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 5, 18},
                                    PrimitiveType::TYPE_INT};
        DataSet data_set = {
                // oob of 9e19(int64)
                {{DECIMAL64(123456789012345, 67000, 5), INT(2)}, VARCHAR("12,345,678,901,234.67")},
                // different scale
                {{DECIMAL64(-17014116, -671, 3), INT(2)}, VARCHAR("-17,014,116.67")},
        };

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set, -1,
                                                               -1, false, true));
    }
}

TEST(MathFunctionTest, signbit_test) {
    std::string func_name = "signbit";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

        DataSet data_set = {{{1.0}, UInt8(0)},  {{0.9}, UInt8(0)}, {{-1.0}, UInt8(1)},
                            {{-0.9}, UInt8(1)}, {{0.0}, UInt8(0)}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeBool, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};

        DataSet data_set = {
                {{(int32_t)1}, UInt8(0)}, {{(int32_t)-1}, UInt8(1)}, {{(int32_t)0}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeBool, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, even_test) {
    std::string func_name = "even";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {{{-1.0}, -2.0}, {{-2.2}, -4.0}, {{2.2}, 4.0},
                        {{0.0}, 0.0},   {{0.5}, 2.0},   {{3.2}, 4.0},
                        {{-3.2}, -4.0}, {{-0.5}, -2.0}, {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, gcd_test) {
    std::string func_name = "gcd";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_TINYINT};

        DataSet data_set = {
                {{TINYINT(1), TINYINT(2)}, TINYINT(1)},  {{TINYINT(2), TINYINT(4)}, TINYINT(2)},
                {{TINYINT(-2), TINYINT(4)}, TINYINT(2)}, {{TINYINT(2), TINYINT(-4)}, TINYINT(2)},
                {{TINYINT(0), TINYINT(0)}, TINYINT(0)},  {{Null(), TINYINT(2)}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT};

        DataSet data_set = {{{SMALLINT(2), SMALLINT(4)}, SMALLINT(2)},
                            {{SMALLINT(2), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{INT(1), INT(2)}, 1},   {{INT(2), INT(4)}, 2},
                            {{INT(-2), INT(4)}, 2},  {{INT(2), INT(-4)}, 2},
                            {{INT(-2), INT(-4)}, 2}, {{INT(0), INT(4)}, 4},
                            {{INT(0), INT(0)}, 0},   {{Null(), INT(2)}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT};

        DataSet data_set = {{{BIGINT(2), BIGINT(4)}, BIGINT(2)}, {{BIGINT(2), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT};

        DataSet data_set = {{{LARGEINT(2), LARGEINT(4)}, LARGEINT(2)},
                            {{LARGEINT(2), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt128, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, lcm_test) {
    std::string func_name = "lcm";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT};

        DataSet data_set = {{{SMALLINT(2), SMALLINT(4)}, SMALLINT(4)},
                            {{SMALLINT(2), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{INT(3), INT(5)}, INT(15)},  {{INT(2), INT(4)}, INT(4)},
                            {{INT(-2), INT(4)}, INT(4)},  {{INT(2), INT(-4)}, INT(4)},
                            {{INT(-2), INT(-4)}, INT(4)}, {{INT(0), INT(4)}, INT(0)},
                            {{INT(0), INT(0)}, INT(0)},   {{Null(), INT(2)}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT};

        DataSet data_set = {{{BIGINT(2), BIGINT(4)}, BIGINT(4)}, {{BIGINT(2), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT};

        DataSet data_set = {{{LARGEINT(2), LARGEINT(4)}, LARGEINT(4)},
                            {{LARGEINT(2), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt128, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, isnan_test) {
    std::string func_name = "isnan";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {{{Null()}, Null()},
                            {{DOUBLE(0.0)}, uint8_t {0}},
                            {{DOUBLE(-1.0)}, uint8_t {0}},
                            {{DOUBLE(1.0)}, uint8_t {0}},
                            {{DOUBLE(NAN)}, uint8_t {1}},
                            {{DOUBLE(INFINITY)}, uint8_t {0}},
                            {{DOUBLE(-INFINITY)}, uint8_t {0}}};
        static_cast<void>(check_function<DataTypeBool, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_FLOAT};
        DataSet data_set = {{{Null()}, Null()},
                            {{FLOAT(0.0)}, uint8_t {0}},
                            {{FLOAT(-1.0)}, uint8_t {0}},
                            {{FLOAT(1.0)}, uint8_t {0}},
                            {{FLOAT(NAN)}, uint8_t {1}},
                            {{FLOAT(INFINITY)}, uint8_t {0}},
                            {{FLOAT(-INFINITY)}, uint8_t {0}}};
        static_cast<void>(check_function<DataTypeBool, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, isinf_test) {
    std::string func_name = "isinf";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {{{Null()}, Null()},
                            {{DOUBLE(0.0)}, uint8_t {0}},
                            {{DOUBLE(-1.0)}, uint8_t {0}},
                            {{DOUBLE(1.0)}, uint8_t {0}},
                            {{DOUBLE(NAN)}, uint8_t {0}},
                            {{DOUBLE(INFINITY)}, uint8_t {1}},
                            {{DOUBLE(-INFINITY)}, uint8_t {1}}};
        static_cast<void>(check_function<DataTypeBool, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_FLOAT};
        DataSet data_set = {{{Null()}, Null()},
                            {{FLOAT(0.0)}, uint8_t {0}},
                            {{FLOAT(-1.0)}, uint8_t {0}},
                            {{FLOAT(1.0)}, uint8_t {0}},
                            {{FLOAT(NAN)}, uint8_t {0}},
                            {{FLOAT(INFINITY)}, uint8_t {1}},
                            {{FLOAT(-INFINITY)}, uint8_t {1}}};
        static_cast<void>(check_function<DataTypeBool, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
