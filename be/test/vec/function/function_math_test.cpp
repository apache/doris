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
#include <time.h>

#include <any>
#include <cmath>
#include <iostream>
#include <string>

#include "function_test_util.h"
#include "runtime/tuple_row.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {

using vectorized::Null;
using vectorized::DataSet;

TEST(MathFunctionTest, acos_test) {
    std::string func_name = "acos"; //[-1,1] -->[0,pi]

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{-1.0}, 3.1415926535897931},
                        {{0.0}, M_PI / 2},
                        {{0.5}, 1.0471975511965979},
                        //{{3.14},nan("")},
                        {{1.0}, 0.0}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, asin_test) {
    std::string func_name = "asin"; //[-1,1] -->[-pi_2, pi_2]

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {
            {{-1.0}, -M_PI / 2}, {{0.0}, 0.0}, {{0.5}, 0.52359877559829893}, {{1.0}, M_PI / 2}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, atan_test) {
    std::string func_name = "atan"; //[-,+] -->(pi_2,pi_2)

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{-1.0}, -0.78539816339744828},
                        {{0.0}, 0.0},
                        {{0.5}, 0.46364760900080609},
                        {{1.0}, 0.78539816339744828}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, cos_test) {
    std::string func_name = "cos";

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{-1.0}, 0.54030230586813977},
                        {{0.0}, 1.0},
                        {{0.5}, 0.87758256189037276},
                        {{M_PI}, -1.0},
                        {{1.0}, 0.54030230586813977}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, sin_test) {
    std::string func_name = "sin";

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{-1.0}, -0.8414709848078965},
                        {{0.0}, 0.0},
                        {{0.5}, 0.479425538604203},
                        {{M_PI / 2}, 1.0},
                        {{1.0}, 0.8414709848078965}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, sqrt_test) {
    std::string func_name = "sqrt"; //sqrt(x) x>=0

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{0.0}, 0.0},
                        {{2.0}, 1.4142135623730951},
                        {{9.0}, 3.0},
                        {{1000.0}, 31.622776601683793}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, tan_test) {
    std::string func_name = "tan"; //tan(x)

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{0.0}, 0.0},
                        {{2.0}, -2.1850398632615189},
                        {{-1.0}, -1.5574077246549023},
                        {{1000.0}, 1.4703241557027185}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, exp_test) {
    std::string func_name = "exp";

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{-1.0}, 0.36787944117144233},
                        {{0.0}, 1.0},
                        {{0.5}, 1.6487212707001282},
                        {{-800.0}, 0.0},
                        {{1.0}, 2.7182818284590451}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, ln_test) {
    std::string func_name = "ln"; // ln(x) x>0

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{1.0}, 0.0},
                        {{0.5}, -0.69314718055994529},
                        {{100.0}, 4.6051701859880918},
                        {{1000.0}, 6.9077552789821368}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, log2_test) {
    std::string func_name = "log2"; // log2(x) x>0

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{1.0}, 0.0},
                        {{0.5}, -1.0},
                        {{100.0}, 6.6438561897747244},
                        {{1000.0}, 9.965784284662087}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, log10_test) {
    std::string func_name = "log10"; // log10(x) x>0

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {
            {{1.0}, 0.0}, {{0.5}, -0.3010299956639812}, {{100.0}, 2.0}, {{1000.0}, 3.0}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, log_test) {
    std::string func_name = "log"; // log(x,y) x>0 y>0

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64,
                                         vectorized::TypeIndex::Float64};

    DataSet data_set = {{{10.0, 1.0}, 0.0},
                        {{10.0, 100.0}, 2.0},
                        {{0.1, 5.0}, -0.69897000433601886},
                        {{2.0, 0.5}, -1.0}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, pow_test) {
    std::string func_name = "pow"; // pow(x,y)

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64,
                                         vectorized::TypeIndex::Float64};

    DataSet data_set = {{{10.0, 1.0}, 10.0},
                        {{10.0, 10.0}, 10000000000.0},
                        {{100.0, -2.0}, 0.0001},
                        {{2.0, 0.5}, 1.4142135623730951}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, truncate_test) {
    std::string func_name = "truncate"; // truncate(x,y)

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64,
                                         vectorized::TypeIndex::Float64};

    DataSet data_set = {{{123.4567, 3.0}, 123.456}, {{-123.4567, 3.0}, -123.456},
                        {{123.4567, 0.0}, 123.0},   {{-123.4567, 0.0}, -123.0},
                        {{123.4567, -2.0}, 100.0},  {{-123.4567, -2.0}, -100.0},
                        {{-123.4567, -3.0}, 0.0}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, ceil_test) {
    std::string func_name = "ceil";

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {
            {{2.3}, (int64_t)3}, {{2.8}, (int64_t)3}, {{-2.3}, (int64_t)-2}, {{2.8}, (int64_t)3.0}};

    vectorized::check_function<vectorized::DataTypeInt64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, floor_test) {
    std::string func_name = "floor";

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {
            {{2.3}, (int64_t)2}, {{2.8}, (int64_t)2}, {{-2.3}, (int64_t)-3}, {{-2.8}, (int64_t)-3}};

    vectorized::check_function<vectorized::DataTypeInt64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, degrees_test) {
    std::string func_name = "degrees"; // degrees(x) rad-->C

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{1.0}, 57.295779513082323},
                        {{M_PI / 2}, 90.0},
                        {{0.0}, 0.0},
                        {{-2.0}, -114.59155902616465}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, radians_test) {
    std::string func_name = "radians"; // radians(x) C--->rad

    std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

    DataSet data_set = {{{30.0}, 0.52359877559829882},
                        {{90.0}, M_PI / 2},
                        {{0.0}, 0.0},
                        {{-60.0}, -1.0471975511965976}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, positive_test) {
    std::string func_name = "positive";

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

        DataSet data_set = {{{0.0123}, 0.0123}, {{90.45}, 90.45}, {{0.0}, 0.0}, {{-60.0}, -60.0}};

        vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types,
                                                                      data_set);
    }

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)3}, (int32_t)3},
                            {{(int32_t)-3}, (int32_t)-3},
                            {{(int32_t)0}, (int32_t)0},
                            {{(int32_t)-60}, (int32_t)-60}};

        vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types,
                                                                    data_set);
    }
}

TEST(MathFunctionTest, negative_test) {
    std::string func_name = "negative";

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

        DataSet data_set = {{{0.0123}, -0.0123}, {{90.45}, -90.45}, {{0.0}, 0.0}, {{-60.0}, 60.0}};

        vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types,
                                                                      data_set);
    }

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)3}, (int32_t)-3},
                            {{(int32_t)-3}, (int32_t)-3},
                            {{(int32_t)0}, (int32_t)0},
                            {{(int32_t)-60}, (int32_t)-60}};

        vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types,
                                                                    data_set);
    }
}

TEST(MathFunctionTest, sign_test) {
    std::string func_name = "sign"; // sign(x) // 1 0 -1

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)30}, (int8_t)1.0},
                            {{(int32_t)0}, (int8_t)0.0},
                            {{(int32_t)-10}, (int8_t)-1.0}};

        vectorized::check_function<vectorized::DataTypeInt8, true>(func_name, input_types,
                                                                   data_set);
    }
    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

        DataSet data_set = {{{30.7}, (int8_t)1.0}, {{0.0}, (int8_t)0.0}, {{-10.6}, (int8_t)-1.0}};

        vectorized::check_function<vectorized::DataTypeInt8, true>(func_name, input_types,
                                                                   data_set);
    }
}

TEST(MathFunctionTest, round_test) {
    std::string func_name = "round"; // round(double) && round(double, int)

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64};

        DataSet data_set = {{{30.1}, (int64_t)30},
                            {{90.6}, (int64_t)91},
                            {{Null()}, Null()},
                            {{0.0}, (int64_t)0},
                            {{-1.1}, (int64_t)-1},
                            {{-60.7}, (int64_t)-61}};

        vectorized::check_function<vectorized::DataTypeInt64, true>(func_name, input_types,
                                                                    data_set);
    }
    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64,
                                             vectorized::TypeIndex::Int32};

        DataSet data_set = {{{3.1415926, 2}, 3.14},
                            {{3.1415926, 3}, 3.142},
                            {{Null(), -2}, Null()},
                            {{193.0, -2}, 200.0},
                            {{193.0, -1}, 190.0},
                            {{193.0, -3}, 0.0}};

        vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types,
                                                                      data_set);
    }
}

TEST(MathFunctionTest, bin_test) {
    std::string func_name = "bin";

    std::vector<std::any> input_types = {vectorized::TypeIndex::Int64};

    DataSet data_set = {{{(int64_t) 10}, std::string("1010")},
                        {{(int64_t) 1}, std::string("1")},
                        {{(int64_t) 0}, std::string("0")},
                        {{Null()}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, hex_test) {
    std::string func_name = "hex"; // hex(int)
    
    std::vector<std::any> input_types = {vectorized::TypeIndex::Int64};

    DataSet data_set = {{{Null()}, Null()},
                        {{(int64_t)-1}, std::string("FFFFFFFFFFFFFFFF")},
                        {{(int64_t)-2}, std::string("FFFFFFFFFFFFFFFE")},
                        {{(int64_t)12}, std::string("C")},
                        {{(int64_t)144}, std::string("90")},
                        {{(int64_t)151233}, std::string("24EC1")},
                        {{(int64_t)0}, std::string("0")},
                        {{(int64_t)9223372036854775807}, std::string("7FFFFFFFFFFFFFFF")},
                        {{(int64_t)-7453337203775808}, std::string("FFE5853AB393E6C0")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(MathFunctionTest, random_test) {
    std::string func_name = "random"; // random(x)
    std::vector<std::any> input_types = {vectorized::Consted {vectorized::TypeIndex::Int64}};
    DataSet data_set = {{{Null()}, Null()},
                        {{(int64_t)0}, 0.15979336337046085},
                        {{(int64_t)10}, 0.60128310734097479},
                        {{(int64_t)123}, 0.31320017867847078},
                        {{(int64_t)std::numeric_limits<int64_t>::max()}, 0.20676730979843233},
                        {{(int64_t)std::numeric_limits<int64_t>::min()}, 0.15979336337046085}};

    for (const auto& data : data_set) {
        DataSet data_line = {data};
        vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_line);
     }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
