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

#include <limits.h>
#include <stdint.h>

#include <cmath>
#include <iomanip>
#include <limits>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

using namespace ut_type;

// test

TEST(MathFunctionTest, acos_test) {
    std::string func_name = "acos"; //[-1,1] -->[0,pi]

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {{{double(1)}, double(0)},
                        {{double(-1)}, double(3.141592653589793)},
                        {{double(0)}, double(1.5707963267948966)},
                        {{double(0.5)}, double(1.0471975511965976)},
                        {{double(-0.5)}, double(2.0943951023931957)},
                        {{Null()}, Null()}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, asin_test) {
    std::string func_name = "asin"; //[-1,1] -->[-pi_2, pi_2]

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {{{double(1)}, double(1.5707963267948966)},
                        {{double(-1)}, double(-1.5707963267948966)},
                        {{double(0)}, double(0)},
                        {{double(0.5)}, double(0.5235987755982988)},
                        {{double(-0.5)}, double(-0.5235987755982988)},
                        {{Null()}, Null()}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, atan_test) {
    std::string func_name = "atan"; //[-,+] -->(pi_2,pi_2)

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{-1.0}, -0.78539816339744828},
            {{double(1)}, double(0.7853981633974483)},
            {{double(-1)}, double(-0.7853981633974483)},
            {{double(0)}, double(0)},
            {{double(0.5)}, double(0.46364760900080615)},
            {{double(-0.5)}, double(-0.46364760900080615)},
            {{double(9223372036854775807)}, double(1.5707963267948966)},
            {{Null()}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, cos_test) {
    std::string func_name = "cos";

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(1)}, double(0.5403023058681398)},
            {{double(-1)}, double(0.5403023058681398)},
            {{double(0)}, double(1)},
            {{double(0.5)}, double(0.8775825618903728)},
            {{double(-0.5)}, double(0.8775825618903728)},
            {{double(9223372036854775807)}, double(0.011800076512800238)},
            {{Null()}, Null()},
            {{double(-4611686018427387904)}, double(-0.7112665029764864)},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, sin_test) {
    std::string func_name = "sin";

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(1)}, double(0.8414709848078965)},
            {{double(-1)}, double(-0.8414709848078965)},
            {{double(0)}, double(0)},
            {{double(0.5)}, double(0.479425538604203)},
            {{double(-0.5)}, double(-0.479425538604203)},
            {{double(9223372036854775807)}, double(0.9999303766734422)},
            {{Null()}, Null()},
            {{double(-4611686018427387904)}, double(0.7029224436192089)},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, sqrt_test) {
    std::string func_name = "sqrt"; //sqrt(x) x>=0

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(4)}, double(2)},
            {{double(0)}, double(0)},
            {{double(0.00000000013)}, double(0.000011401754250991379)},
            {{double(2147483647)}, double(46340.950001051984)},
            {{double(9)}, double(3)},
            {{double(8)}, double(2.8284271247461903)},
            {{Null()}, Null()},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, cbrt_test) {
    std::string func_name = "cbrt";

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{0.0}, 0.0}, {{2.0}, 1.2599210498948734}, {{8.0}, 2.0}, {{-1000.0}, -10.0}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, tan_test) {
    std::string func_name = "tan"; //tan(x)

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(0.785398)}, double(0.9999996732051569)},
            {{double(-0.785398)}, double(-0.9999996732051569)},
            {{double(1)}, double(1.557407724654902)},
            {{double(-1)}, double(-1.557407724654902)},
            {{double(1.570796)}, double(3060023.306952844)},
            {{double(-1.570796)}, double(-3060023.306952844)},
            {{Null()}, Null()},
            {{double(100000)}, double(-0.03577166295289877)},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, exp_test) {
    std::string func_name = "exp";

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(0.785398)}, double(2.1932796923616813)},
            {{double(-0.785398)}, double(0.455938202265129)},
            {{double(1)}, double(2.718281828459045)},
            {{double(-1)}, double(0.36787944117144233)},
            {{double(1.570796)}, double(4.81047580892615)},
            {{double(-1.570796)}, double(0.20787964428475766)},
            {{Null()}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, ln_test) {
    std::string func_name = "ln"; // ln(x) x>0

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(0.785398)}, double(-0.24156468331460473)},
            {{double(-0.785398)}, Null()},
            {{double(1)}, double(0)},
            {{double(-1)}, Null()},
            {{double(1.570796)}, double(0.45158249724534055)},
            {{double(-1.570796)}, Null()},
            {{Null()}, Null()},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, log2_test) {
    std::string func_name = "log2"; // log2(x) x>0

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(0.785398)}, double(-0.34850417067189315)},
            {{double(-0.785398)}, Null()},
            {{double(1)}, double(0)},
            {{double(-1)}, Null()},
            {{double(1.570796)}, double(0.6514958293281068)},
            {{double(-1.570796)}, Null()},
            {{Null()}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, log10_test) {
    std::string func_name = "log10"; // log10(x) x>0

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{1.0}, 0.0},
            {{0.5}, -0.3010299956639812},
            {{100.0}, 2.0},
            {{-1.0}, Null()},
            {{1000.0}, 3.0},
            {{double(0.785398)}, double(-0.10491020898623936)},
            {{double(-0.785398)}, Null()},
            {{double(1)}, double(0)},
            {{double(-1)}, Null()},
            {{double(1.570796)}, double(0.19611978667774183)},
            {{double(-1.570796)}, Null()},
            {{Null()}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, log_test) {
    std::string func_name = "log"; // log(x,y) x>0 y>0

    BaseInputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64};

    DataSet data_set = {
            {{10.0, 1.0}, 0.0},    {{10.0, 100.0}, 2.0},  {{0.1, 5.0}, -0.69897000433601886},
            {{-2.0, 5.0}, Null()}, {{2.0, -5.0}, Null()}, {{2.0, 0.5}, -1.0},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, pow_test) {
    std::string func_name = "pow"; // pow(x,y)

    BaseInputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64};

    DataSet data_set = {{{10.0, 1.0}, 10.0},
                        {{10.0, 10.0}, 10000000000.0},
                        {{100.0, -2.0}, 0.0001},
                        {{2.0, 0.5}, 1.4142135623730951}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, ceil_test) {
    std::string func_name = "ceil";

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{double(0.785398)}, double(1)},
            {{double(-0.785398)}, double(0)},
            {{double(1)}, double(1)},
            {{double(-1)}, double(-1)},
            {{double(1.570796)}, double(2)},
            {{double(-1.570796)}, double(-1)},
            {{Null()}, Null()},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, floor_test) {
    std::string func_name = "floor";

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{2.3}, 2.0},
            {{2.8}, 2.0},
            {{-2.3}, -3.0},
            {{-2.8}, -3.0},
            {{double(0.785398)}, double(0)},
            {{double(-0.785398)}, double(-1)},
            {{double(1)}, double(1)},
            {{double(-1)}, double(-1)},
            {{double(1.570796)}, double(1)},
            {{double(-1.570796)}, double(-2)},
            {{Null()}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, degrees_test) {
    std::string func_name = "degrees"; // degrees(x) rad-->C

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{1.0}, 57.295779513082323},
            {{M_PI / 2}, 90.0},
            {{0.0}, 0.0},
            {{-2.0}, -114.59155902616465},
            {{Null()}, Null()},
            {{double(3289472.3)}, double(188472879.6151918)},
            {{double(98237429.2)}, double(5628590083.375236)},
            {{double(98234729.32)}, double(5628435391.646043)},
            {{double(98237498374.3)}, double(5628594046770.676)},
            {{double(9834729.8)}, double(563488510.1915402)},
            {{double(2398472384.8)}, double(137422344927.71756)},
            {{double(8324987.23)}, double(476986632.779306)},
            {{double(3284792.83)}, double(188204765.7338337)},
            {{double(239847.9283472)}, double(13742274.01925074)},
            {{double(382974.8)}, double(21942839.699866798)},
            {{double(23984728347.2)}, double(1374223707062.3467)},
            {{double(92837492734.3)}, double(5319196514251.835)},
            {{double(9238427.32)}, double(529322894.77435607)},
            {{double(9823748273.4)}, double(562859315064.7495)},
            {{double(2309847230.2)}, double(132344497610.4431)},
            {{double(9223372036854775807)}, double(5.2846029059076024e20)},
            {{double(0)}, double(0)},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, radians_test) {
    std::string func_name = "radians"; // radians(x) C--->rad

    BaseInputTypeSet input_types = {TypeIndex::Float64};

    DataSet data_set = {
            {{30.0}, 0.52359877559829882},
            {{90.0}, M_PI / 2},
            {{0.0}, 0.0},
            {{-60.0}, -1.0471975511965976},
            {{double(0.785398)}, double(0.013707781038578426)},
            {{double(-0.785398)}, double(-0.013707781038578426)},
            {{double(1)}, double(0.017453292519943295)},
            {{double(-1)}, double(-0.017453292519943295)},
            {{double(1.570796)}, double(0.02741556207715685)},
            {{double(-1.570796)}, double(-0.02741556207715685)},
            {{Null()}, Null()},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, abs_test) {
    std::string func_name = "abs";

    {
        BaseInputTypeSet input_types = {TypeIndex::Float64};

        DataSet data_set = {{{Null()}, Null()},
                            {{-0.0123}, 0.0123},
                            {{90.45}, 90.45},
                            {{0.0}, 0.0},
                            {{-60.0}, 60.0}};

        static_cast<void>(check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types,
                                                                             data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Int32};

        DataSet data_set = {{{Null()}, Null()},
                            {{INT(3)}, BIGINT(3)},
                            {{INT(-3)}, BIGINT(3)},
                            {{INT(0)}, BIGINT(0)},
                            {{INT(-60)}, BIGINT(60)},
                            {{INT(INT_MAX)}, BIGINT(INT_MAX)},
                            {{INT(INT_MIN)}, BIGINT(-1LL * INT_MIN)}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, positive_test) {
    std::string func_name = "positive";

    {
        BaseInputTypeSet input_types = {TypeIndex::Float64};

        DataSet data_set = {{{0.0123}, 0.0123}, {{90.45}, 90.45}, {{0.0}, 0.0}, {{-60.0}, -60.0}};

        static_cast<void>(check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types,
                                                                             data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)3}, (int32_t)3},
                            {{(int32_t)-3}, (int32_t)-3},
                            {{(int32_t)0}, (int32_t)0},
                            {{(int32_t)-60}, (int32_t)-60}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, negative_test) {
    std::string func_name = "negative";

    {
        BaseInputTypeSet input_types = {TypeIndex::Float64};

        DataSet data_set = {{{0.0123}, -0.0123}, {{90.45}, -90.45}, {{0.0}, 0.0}, {{-60.0}, 60.0}};

        static_cast<void>(check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types,
                                                                             data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)3}, (int32_t)-3},
                            {{(int32_t)-3}, (int32_t)3},
                            {{(int32_t)0}, (int32_t)0},
                            {{(int32_t)-60}, (int32_t)60}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, sign_test) {
    std::string func_name = "sign"; // sign(x) // 1 0 -1

    {
        BaseInputTypeSet input_types = {TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)30}, (int8_t)1.0},
                            {{(int32_t)0}, (int8_t)0.0},
                            {{(int32_t)-10}, (int8_t)-1.0}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        BaseInputTypeSet input_types = {TypeIndex::Float64};

        DataSet data_set = {{{30.7}, (int8_t)1.0}, {{0.0}, (int8_t)0.0}, {{-10.6}, (int8_t)-1.0}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(MathFunctionTest, round_test) {
    std::string func_name = "round"; // round(double) && round(double, int)

    {
        BaseInputTypeSet input_types = {TypeIndex::Float64};

        DataSet data_set = {
                {{30.1}, 30.0},
                {{90.6}, 91.0},
                {{Null()}, Null()},
                {{double(0.785398)}, double(1)},
                {{double(-0.785398)}, double(-1)},
                {{double(1)}, double(1)},
                {{double(-1)}, double(-1)},
                {{double(1.570796)}, double(2)},
                {{double(-1.570796)}, double(-2)},
                {{Null()}, Null()},
        };

        static_cast<void>(check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types,
                                                                             data_set));
    }
}

TEST(MathFunctionTest, round_bankers_test) {
    std::string func_name = "round_bankers";

    {
        BaseInputTypeSet input_types = {TypeIndex::Float64};

        DataSet data_set = {{{0.4}, 0.0}, {{-3.5}, -4.0}, {{4.5}, 4.0}, {{Null()}, Null()}};

        static_cast<void>(check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types,
                                                                             data_set));
    }
}

TEST(MathFunctionTest, least_test) {
    std::string func_name = "least";

    BaseInputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

    DataSet data_set = {
            {{3, 2}, 2}, {{3, 3}, 3}, {{Null(), -2}, Null()}, {{193, -2}, -2}, {{193, -1}, -1}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, greatest_test) {
    std::string func_name = "greatest";

    BaseInputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

    DataSet data_set = {
            {{3, 2}, 3}, {{3, 3}, 3}, {{Null(), -2}, Null()}, {{193, -2}, 193}, {{193, -1}, 193}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, bin_test) {
    std::string func_name = "bin";

    BaseInputTypeSet input_types = {TypeIndex::Int64};

    DataSet data_set = {
            {{(int64_t)10}, std::string("1010")},
            {{(int64_t)1}, std::string("1")},
            {{(int64_t)0}, std::string("0")},
            {{Null()}, Null()},
            {{std::int64_t(18927498124)}, std::string("10001101000001010101011001110001100")},
            {{std::int64_t(2193810923)}, std::string("10000010110000101110010111101011")},
            {{std::int64_t(219499184)}, std::string("1101000101010100101010110000")},
            {{std::int64_t(123124124)}, std::string("111010101101011100110011100")},
            {{std::int64_t(-12941024)},
             std::string("1111111111111111111111111111111111111111001110101000100100100000")},
            {{Null()}, Null()},
            {{std::int64_t(12371294)}, std::string("101111001100010101011110")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, hex_test) {
    std::string func_name = "hex"; // hex(int)

    BaseInputTypeSet input_types = {TypeIndex::Int64};

    DataSet data_set = {
            {{Null()}, Null()},
            {{(int64_t)-1}, std::string("FFFFFFFFFFFFFFFF")},
            {{(int64_t)-2}, std::string("FFFFFFFFFFFFFFFE")},
            {{(int64_t)12}, std::string("C")},
            {{(int64_t)144}, std::string("90")},
            {{(int64_t)151233}, std::string("24EC1")},
            {{(int64_t)0}, std::string("0")},
            {{(int64_t)9223372036854775807}, std::string("7FFFFFFFFFFFFFFF")},
            {{(int64_t)-7453337203775808}, std::string("FFE5853AB393E6C0")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(MathFunctionTest, random_test) {
    std::string func_name = "random"; // random(x)
    InputTypeSet input_types = {Consted {TypeIndex::Int64}};
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
        BaseInputTypeSet input_types = {TypeIndex::Int64, TypeIndex::Int8, TypeIndex::Int8};
        DataSet data_set = {
                {{Null(), Null(), Null()}, Null()},
                {{BIGINT(230), TINYINT(10), TINYINT(16)}, VARCHAR("E6")},
                {{BIGINT(15), TINYINT(10), TINYINT(2)}, VARCHAR("1111")},
                // 极端和边界值
                {{BIGINT(255), TINYINT(10), TINYINT(2)}, VARCHAR("11111111")}, // 十进制转二进制
                {{BIGINT(255), TINYINT(10), TINYINT(36)}, VARCHAR("73")}, // 十进制转36进制
                {{BIGINT(9223372036854775807), TINYINT(10), TINYINT(2)},
                 VARCHAR("11111111111111111111111111111111111111111111111111111111111111"
                         "1")},                                       // BIGINT最大值转二进制
                {{BIGINT(0), TINYINT(10), TINYINT(2)}, VARCHAR("0")}, // 0值转换
                // 无效的进制转换尝试
                {{BIGINT(1234), TINYINT(1), TINYINT(10)},
                 Null()}, // 无效的进制转换尝试，from_base无效
                {{BIGINT(1234), TINYINT(10), TINYINT(37)},
                 Null()}, // 无效的进制转换尝试，to_base无效
                //todo 十六进制转换
                // {{BIGINT(0xFF), TINYINT(16), TINYINT(10)}, VARCHAR("255")}, // 十六进制转十进制
                // {{BIGINT(0xF), TINYINT(16), TINYINT(2)}, VARCHAR("1111")}, // 十六进制转二进制
                // 负数转换
                {{BIGINT(-100), TINYINT(10), TINYINT(16)},
                 VARCHAR("FFFFFFFFFFFFFF9C")}, // 十进制转十六进制
                // 大整数进制转换
                {{BIGINT(922337203685477), TINYINT(10), TINYINT(36)},
                 VARCHAR("92XVV117C5")}, // 大整数转36进制
                {{BIGINT(36), TINYINT(10), TINYINT(36)},
                 VARCHAR("10")}, // 36（十进）转换为 36（三十六进）制，展示基数间的直接映射
        };

        (void)check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int8, TypeIndex::Int8};
        DataSet data_set = {
                {{Null(), Null(), Null()}, Null()},
                {{VARCHAR("ff"), TINYINT(16), TINYINT(10)}, VARCHAR("255")},
                {{Null(), Null(), Null()}, Null()},

                // 正常进制转换
                {{VARCHAR("ffff"), TINYINT(16), TINYINT(10)}, VARCHAR("65535")},

                // 大小写混合的进制字符
                {{VARCHAR("Ff"), TINYINT(16), TINYINT(10)}, VARCHAR("255")},
                {{VARCHAR("aBcDeF"), TINYINT(16), TINYINT(10)}, VARCHAR("11259375")},

                // 极端和边界值
                {{VARCHAR("7fffffff"), TINYINT(16), TINYINT(10)}, VARCHAR("2147483647")},
                {{VARCHAR("80000000"), TINYINT(16), TINYINT(10)}, VARCHAR("2147483648")},

                // 非法输入
                {{VARCHAR("11010210"), TINYINT(2), TINYINT(10)}, VARCHAR("26")},
                {{VARCHAR("gg"), TINYINT(16), TINYINT(10)}, VARCHAR("0")},

                // 无效的进制转换尝试
                {{VARCHAR("100"), TINYINT(0), TINYINT(10)}, Null()},
                {{VARCHAR("100"), TINYINT(10), TINYINT(0)}, Null()},

                // 特殊情况处理
                {{VARCHAR("-1"), TINYINT(10), TINYINT(2)},
                 VARCHAR("1111111111111111111111111111111111111111111111111111111111111111")},
                {{VARCHAR("-z"), TINYINT(36), TINYINT(10)}, VARCHAR("18446744073709551581")},

                // 更多常规转换
                {{VARCHAR("123"), TINYINT(10), TINYINT(8)}, VARCHAR("173")},
                {{VARCHAR("123"), TINYINT(10), TINYINT(16)}, VARCHAR("7B")},

                // 将一些字母转换成较高的进制
                {{VARCHAR("a"), TINYINT(16), TINYINT(10)}, VARCHAR("10")},

                // 测试一些具体的字符到较高进制的转换
                {{VARCHAR("36"), TINYINT(10), TINYINT(36)}, VARCHAR("10")},

                // 测试一些具体的较高进制字符到10进制的转换
                {{VARCHAR("1"), TINYINT(36), TINYINT(10)}, VARCHAR("1")},

                // 测试从10进制到2进制的多个转换
                {{VARCHAR("0"), TINYINT(10), TINYINT(2)}, VARCHAR("0")},
                {{VARCHAR("1"), TINYINT(10), TINYINT(2)}, VARCHAR("1")},
                // 更多的从10进制到16进制的转换
                {{VARCHAR("10"), TINYINT(10), TINYINT(16)}, VARCHAR("A")},
                {{VARCHAR("15"), TINYINT(10), TINYINT(16)}, VARCHAR("F")},
                {{VARCHAR("16"), TINYINT(10), TINYINT(16)}, VARCHAR("10")},

                // 更多的16进制到10进制的转换
                {{VARCHAR("10"), TINYINT(16), TINYINT(10)}, VARCHAR("16")},
                {{VARCHAR("F"), TINYINT(16), TINYINT(10)}, VARCHAR("15")},
                {{VARCHAR("A"), TINYINT(16), TINYINT(10)}, VARCHAR("10")},

                // 边缘值的十进制到2进制
                {{VARCHAR("2147483647"), TINYINT(10), TINYINT(2)},
                 VARCHAR("1111111111111111111111111111111")},

                {{VARCHAR("1G"), TINYINT(16), TINYINT(10)}, VARCHAR("1")},
                {{VARCHAR("H"), TINYINT(16), TINYINT(10)}, VARCHAR("0")},
                {{VARCHAR("1Z"), TINYINT(36), TINYINT(10)}, VARCHAR("71")},

                // 不同进制的0值转换
                {{VARCHAR("0"), TINYINT(2), TINYINT(10)}, VARCHAR("0")},
                {{VARCHAR("0"), TINYINT(8), TINYINT(10)}, VARCHAR("0")},
                {{VARCHAR("0"), TINYINT(16), TINYINT(10)}, VARCHAR("0")},

                // 随机字符转换
                {{VARCHAR("1Ae3"), TINYINT(16), TINYINT(10)}, VARCHAR("6883")},
                {{VARCHAR("10"), TINYINT(5), TINYINT(2)}, VARCHAR("101")},

                // 错误进制转换
                {{VARCHAR("123"), TINYINT(2), TINYINT(10)}, VARCHAR("1")},
                {{VARCHAR("78"), TINYINT(8), TINYINT(10)}, VARCHAR("7")},

                // 更大的数值转换
                {{VARCHAR("1234567890abcdef"), TINYINT(16), TINYINT(10)},
                 VARCHAR("1311768467294899695")},

        };

        (void)check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(MathFunctionTest, money_format_test) {
    std::string func_name = "money_format";

    {
        BaseInputTypeSet input_types = {TypeIndex::Int64};
        DataSet data_set = {{{Null()}, Null()},
                            {{BIGINT(17014116)}, VARCHAR("17,014,116.00")},
                            {{BIGINT(-17014116)}, VARCHAR("-17,014,116.00")}};

        static_cast<void>(check_function_all_arg_comb<DataTypeString, true>(func_name, input_types,
                                                                            data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Int128};
        DataSet data_set = {{{Null()}, Null()},
                            {{LARGEINT(17014116)}, VARCHAR("17,014,116.00")},
                            {{LARGEINT(-17014116)}, VARCHAR("-17,014,116.00")}};

        static_cast<void>(check_function_all_arg_comb<DataTypeString, true>(func_name, input_types,
                                                                            data_set));
    }
    {
        BaseInputTypeSet input_types = {TypeIndex::Float64};
        DataSet data_set = {{{Null()}, Null()},
                            {{DOUBLE(17014116.67)}, VARCHAR("17,014,116.67")},
                            {{DOUBLE(-17014116.67)}, VARCHAR("-17,014,116.67")}};

        static_cast<void>(check_function_all_arg_comb<DataTypeString, true>(func_name, input_types,
                                                                            data_set));
    }
    {
        BaseInputTypeSet input_types = {TypeIndex::Decimal128V2};
        DataSet data_set = {{{Null()}, Null()},
                            {{DECIMAL(17014116.67)}, VARCHAR("17,014,116.67")},
                            {{DECIMAL(-17014116.67)}, VARCHAR("-17,014,116.67")}};

        static_cast<void>(check_function_all_arg_comb<DataTypeString, true>(func_name, input_types,
                                                                            data_set));
    }
}

TEST(MathFunctionTest, truncate) {
    std::string func_name = "truncate";
    BaseInputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Int32};
    DataSet data_set = {
            {{Null(), Null()}, Null()},
            {{Null(), std::int32_t(-1)}, Null()},
            {{double(3289472.3), Null()}, Null()},
            {{double(3289472.3), std::int32_t(-1)}, double(3289470)},
            {{double(98237429.2), Null()}, Null()},
            {{double(98237429.2), std::int32_t(-1)}, double(98237420)},
            {{double(98234729.32), std::int32_t(-1)}, double(98234720)},
            {{double(9834729.8), Null()}, Null()},
            {{double(9834729.8), std::int32_t(-1)}, double(9834720)},
            {{double(2309847230.2), std::int32_t(-2)}, double(2309847200)},
            {{double(2309847230.2), std::int32_t(0)}, double(2309847230)},
            {{double(2309847230.2), std::int32_t(1)}, double(2309847230.2)},
            {{double(2309847230.2), std::int32_t(100)}, double(2309847230.2)},
            {{double(2309847230.2), std::int32_t(-100)}, double(0)},
    };
    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
