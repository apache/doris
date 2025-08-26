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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <fstream>
#include <iostream>

#include "gtest/gtest_pred_impl.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "vec/functions/cast/cast_to_string.h"

namespace doris {

static std::string test_data_dir;
class OlapTypeTest : public testing::Test {
public:
    OlapTypeTest() = default;
    ~OlapTypeTest() override = default;
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/olap";
    }
};

// deserialize float string serialized by old version of Doris
TEST_F(OlapTypeTest, deser_float_old) {
    std::vector<float> normal_input_values = {
            1.230F,
            123.456000F,
            123.000F,
            1234567.000F,
            123456.12345F,
            1234567.12345F,
            12345678.12345F,
            123456789.12345F,
            1234567890000.12345F,
            0.33F,
            123.456F,
            123.456789F,
            123.456789123F,
            123456.123456789F,
            1234567.123456789F,
            987654336.0F,
            16777216.0F,
            0.000123456F,
            0.0001234567F,
            0.00012345678F,
            0.0000123456F,
            0.00001234567F,
            0.0000123456789F,
            0.000000000000001234567F,
            0.000000000000001234567890123456F,
            0.1234567F,
            0.123456789F,
            1234567890123456.12345F,
            12345678901234567.12345F,
    };
    for (int i = 1; i < 10; ++i) {
        normal_input_values.emplace_back(i * 0.0000001F);
        normal_input_values.emplace_back(i * 0.000000001F);
    }
    std::vector<float> test_input_values;
    for (const auto& float_value : normal_input_values) {
        test_input_values.emplace_back(float_value);
        test_input_values.emplace_back(-float_value);
    }
    std::vector<float> special_input_values = {0.0,
                                               -0.0,
                                               std::numeric_limits<float>::min(),
                                               std::numeric_limits<float>::lowest(),
                                               std::numeric_limits<float>::denorm_min(),
                                               std::numeric_limits<float>::max(),
                                               std::numeric_limits<float>::infinity(),
                                               -std::numeric_limits<float>::infinity(),
                                               std::numeric_limits<float>::quiet_NaN()};
    test_input_values.insert(test_input_values.end(), special_input_values.begin(),
                             special_input_values.end());
    std::ifstream input_file(test_data_dir + "/ser_float_3.0.txt");
    EXPECT_TRUE(input_file.is_open());
    std::string line;
    int line_index = 0;
    while (std::getline(input_file, line)) {
        float deser_float_value = 0.0F;
        auto status = FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_FLOAT>::from_string(
                &deser_float_value, line, 0, 0);
        EXPECT_TRUE(status.ok()) << status.to_string();
        float diff_ratio = std::abs(deser_float_value - test_input_values[line_index]) /
                           abs(test_input_values[line_index]);
        EXPECT_TRUE((test_input_values[line_index] == 0 && deser_float_value == 0) ||
                    diff_ratio < 1e-6 ||
                    (std::isnan(deser_float_value) && std::isnan(test_input_values[line_index])) ||
                    (std::isinf(deser_float_value) && std::isinf(test_input_values[line_index])))
                << "expected float value: " << fmt::format("{:.9g}", test_input_values[line_index])
                << ", deser float value: " << fmt::format("{:.9g}", deser_float_value)
                << ", diff_ratio: " << fmt::format("{:.9g}", diff_ratio);
        line_index++;
    }

    input_file.close();
}
// deserialize double string serialized by old version of Doris
TEST_F(OlapTypeTest, deser_double_old) {
    std::vector<double> normal_input_values = {1.230,
                                               123.456000,
                                               123.000,
                                               1234567.000,
                                               123456.12345,
                                               1234567.12345,
                                               12345678.12345,
                                               123456789.12345,
                                               1234567890000.12345,
                                               0.33,
                                               123.456,
                                               123.456789,
                                               123.456789123,
                                               123456.123456789,
                                               1234567.123456789,
                                               987654336.0,
                                               16777216.0,
                                               0.000123456,
                                               0.0001234567,
                                               0.00012345678,
                                               0.0000123456,
                                               0.00001234567,
                                               0.0000123456789,
                                               0.000000000000001234567,
                                               0.000000000000001234567890123456,
                                               0.1234567,
                                               0.123456789,
                                               1234567890123456.12345,
                                               12345678901234567.12345,
                                               123456789012345678.12345};
    for (int i = 1; i < 10; ++i) {
        normal_input_values.emplace_back(i * 0.0000000000000001);
    }
    std::vector<double> test_input_values;
    for (const auto& float_value : normal_input_values) {
        test_input_values.emplace_back(float_value);
        test_input_values.emplace_back(-float_value);
    }
    std::vector<double> special_input_values = {0.0,
                                                -0.0,
                                                std::numeric_limits<float>::min(),
                                                std::numeric_limits<float>::lowest(),
                                                std::numeric_limits<float>::denorm_min(),
                                                std::numeric_limits<float>::max(),
                                                std::numeric_limits<double>::min(),
                                                std::numeric_limits<double>::lowest(),
                                                std::numeric_limits<double>::denorm_min(),
                                                std::numeric_limits<double>::max(),
                                                std::numeric_limits<double>::infinity(),
                                                -std::numeric_limits<double>::infinity(),
                                                std::numeric_limits<double>::quiet_NaN(),
                                                std::numeric_limits<float>::infinity(),
                                                -std::numeric_limits<float>::infinity(),
                                                std::numeric_limits<float>::quiet_NaN()};
    test_input_values.insert(test_input_values.end(), special_input_values.begin(),
                             special_input_values.end());
    std::ifstream input_file(test_data_dir + "/ser_double_3.0.txt");
    EXPECT_TRUE(input_file.is_open());
    std::string line;
    int line_index = 0;
    while (std::getline(input_file, line)) {
        double deser_float_value = 0.0;
        auto status = FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE>::from_string(
                &deser_float_value, line, 0, 0);
        EXPECT_TRUE(status.ok()) << status.to_string();
        double diff_ratio = std::abs(deser_float_value - test_input_values[line_index]) /
                            abs(test_input_values[line_index]);
        EXPECT_TRUE((test_input_values[line_index] == 0 && deser_float_value == 0) ||
                    diff_ratio < 1e-15 ||
                    (std::isnan(deser_float_value) && std::isnan(test_input_values[line_index])) ||
                    (std::isinf(deser_float_value) &&
                     (std::isinf(test_input_values[line_index]) ||
                      test_input_values[line_index] == std::numeric_limits<double>::max() ||
                      test_input_values[line_index] == std::numeric_limits<double>::lowest())))
                << "expected double value: "
                << fmt::format("{:.17g}", test_input_values[line_index])
                << ", deser double value: " << fmt::format("{:.17g}", deser_float_value)
                << ", diff_ratio: " << fmt::format("{:.17g}", diff_ratio);
        line_index++;
    }

    input_file.close();
}
TEST_F(OlapTypeTest, ser_deser_float) {
    std::vector<std::pair<float, std::string>> normal_input_values = {
            {
                    1.230F,
                    "1.23", // trailing zero of fractional part is not printed
            },
            {
                    123.456000F,
                    "123.456",
            },
            {
                    123.000F,
                    "123", // decimal point is printed if fractional part is zero
            },
            {
                    1234567.000F,
                    "1234567",
            },
            // 7 > e >= -4: decimal format.
            // e < -4 or e >= 7: scientific format.
            {
                    123456.12345F,
                    "123456.1",
            },
            {
                    1234567.12345F,
                    "1234567",
            },
            {
                    12345678.12345F,
                    "1.234568e+07",
            },
            {
                    123456789.12345F,
                    "1.234568e+08",
            },
            {
                    1234567890000.12345F,
                    "1.234568e+12",
            },
            {
                    0.33F,
                    "0.33",
            },
            {
                    123.456F,
                    "123.456",
            },
            {
                    123.456789F,
                    "123.4568",
            },
            {
                    123.456789123F,
                    "123.4568",
            },
            {
                    123456.123456789F,
                    "123456.1",
            },
            {
                    1234567.123456789F,
                    "1234567",
            },
            {
                    987654336.0F,
                    "9.876543e+08",
            },
            {
                    16777216.0F,
                    "1.677722e+07",
            },
            {
                    0.000123456F,
                    "0.000123456",
            },
            {
                    0.0001234567F,
                    "0.0001234567",
            },
            {
                    0.00012345678F,
                    "0.0001234568",
            },
            {
                    0.0000123456F,
                    "1.23456e-05", // e < -4
            },
            {
                    0.00001234567F,
                    "1.234567e-05", // e < -4
            },
            {
                    0.0000123456789F,
                    "1.234568e-05",
            },
            {
                    0.000000000000001234567F,
                    "1.234567e-15",
            },
            {
                    0.000000000000001234567890123456F,
                    "1.234568e-15",
            },
            {
                    0.1234567F,
                    "0.1234567",
            },
            {
                    0.123456789F,
                    "0.1234568",
            },

            {
                    1234567890123456.12345F,
                    "1.234568e+15",
            },
            {
                    12345678901234567.12345F,
                    "1.234568e+16",
            }};
    for (int i = 1; i < 10; ++i) {
        normal_input_values.emplace_back(i * 0.0000001F, fmt::format("{}e-07", i));
        normal_input_values.emplace_back(i * 0.000000001F, fmt::format("{}e-09", i));
    }
    std::vector<std::pair<float, std::string>> test_input_values;
    for (const auto& [float_value, expected_str] : normal_input_values) {
        test_input_values.emplace_back(float_value, expected_str);
        test_input_values.emplace_back(-float_value, fmt::format("-{}", expected_str));
    }
    std::vector<std::pair<float, std::string>> special_input_values = {
            {
                    0.0,
                    "0",
            },
            {
                    -0.0,
                    "-0",
            },
            {std::numeric_limits<float>::min(), "1.175494e-38"},
            {std::numeric_limits<float>::lowest(), "-3.402823e+38"},
            {std::numeric_limits<float>::denorm_min(), "1.401298e-45"},
            {std::numeric_limits<float>::max(), "3.402823e+38"},
            {
                    std::numeric_limits<float>::infinity(),
                    "Infinity",
            },
            {
                    -std::numeric_limits<float>::infinity(),
                    "-Infinity",
            },
            {
                    std::numeric_limits<float>::quiet_NaN(),
                    "NaN",
            }};
    test_input_values.insert(test_input_values.end(), special_input_values.begin(),
                             special_input_values.end());
    for (const auto& [float_value, expected_str] : test_input_values) {
        auto result_str =
                FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_FLOAT>::to_string(&float_value);
        EXPECT_EQ(result_str, expected_str);
        float deser_float_value = 0.0F;
        auto status = FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_FLOAT>::from_string(
                &deser_float_value, result_str, 0, 0);
        EXPECT_TRUE(status.ok()) << status.to_string();
        float diff_ratio = std::abs(deser_float_value - float_value) / abs(float_value);
        EXPECT_TRUE((float_value == 0 && deser_float_value == 0) || diff_ratio < 1e-6 ||
                    (std::isnan(deser_float_value) && std::isnan(float_value)) ||
                    (std::isinf(deser_float_value) && std::isinf(float_value)))
                << "expected float value: " << fmt::format("{:.9g}", float_value)
                << ", expected float str: " << expected_str
                << ", deser float value: " << fmt::format("{:.9g}", deser_float_value)
                << ", diff_ratio: " << fmt::format("{:.9g}", diff_ratio);
    }
}
TEST_F(OlapTypeTest, ser_deser_double) {
    std::vector<std::pair<double, std::string>> normal_input_values = {
            {
                    1.230,
                    "1.23", // trailing zero of fractional part is not printed
            },
            {
                    123.456000,
                    "123.456",
            },
            {
                    123.000,
                    "123", // decimal point is printed if fractional part is zero
            },
            {
                    1234567.000,
                    "1234567",
            },
            // 16 > e >= -4: decimal format.
            // e < -4 or e >= 16: scientific format.
            {
                    123456.12345,
                    "123456.12345",
            },
            {
                    1234567.12345,
                    "1234567.12345",
            },
            {
                    12345678.12345,
                    "12345678.12345",
            },
            {
                    123456789.12345,
                    "123456789.12345",
            },
            {
                    1234567890000.12345,
                    "1234567890000.124",
            },
            {
                    0.33,
                    "0.33",
            },
            {
                    123.456,
                    "123.456",
            },
            {
                    123.456789,
                    "123.456789",
            },
            {
                    123.456789123,
                    "123.456789123",
            },
            {
                    123456.123456789,
                    "123456.123456789",
            },
            {
                    1234567.123456789,
                    "1234567.123456789",
            },
            {
                    987654336.0,
                    "987654336",
            },
            {
                    16777216.0,
                    "16777216",
            },
            {
                    0.000123456,
                    "0.000123456",
            },
            {
                    0.0001234567,
                    "0.0001234567",
            },
            {
                    0.00012345678,
                    "0.00012345678",
            },
            {
                    0.0000123456,
                    "1.23456e-05", // e < -4
            },
            {
                    0.00001234567,
                    "1.234567e-05", // e < -4
            },
            {
                    0.0000123456789,
                    "1.23456789e-05",
            },
            {
                    0.000000000000001234567,
                    "1.234567e-15",
            },
            {
                    0.000000000000001234567890123456,
                    "1.234567890123456e-15",
            },
            {
                    0.1234567,
                    "0.1234567",
            },
            {
                    0.123456789,
                    "0.123456789",
            },

            {
                    1234567890123456.12345,
                    "1234567890123456",
            },
            {
                    12345678901234567.12345,
                    "1.234567890123457e+16",
            },
            {
                    123456789012345678.12345,
                    "1.234567890123457e+17",
            }};
    for (int i = 1; i < 10; ++i) {
        if (i == 7) {
            normal_input_values.emplace_back(i * 0.0000000000000001, "6.999999999999999e-16");
            continue;
        }
        normal_input_values.emplace_back(i * 0.0000000000000001, fmt::format("{}e-16", i));
    }
    std::vector<std::pair<double, std::string>> test_input_values;
    for (const auto& [float_value, expected_str] : normal_input_values) {
        test_input_values.emplace_back(float_value, expected_str);
        test_input_values.emplace_back(-float_value, fmt::format("-{}", expected_str));
    }
    std::vector<std::pair<double, std::string>> special_input_values = {
            {
                    0.0,
                    "0",
            },
            {
                    -0.0,
                    "-0",
            },
            {std::numeric_limits<float>::min(), "1.175494350822288e-38"},
            {std::numeric_limits<float>::lowest(), "-3.402823466385289e+38"},
            {std::numeric_limits<float>::denorm_min(), "1.401298464324817e-45"},
            {std::numeric_limits<float>::max(), "3.402823466385289e+38"},
            {std::numeric_limits<double>::min(), "2.225073858507201e-308"},
            {std::numeric_limits<double>::lowest(), "-1.797693134862316e+308"},
            {std::numeric_limits<double>::denorm_min(), "4.940656458412465e-324"},
            {std::numeric_limits<double>::max(), "1.797693134862316e+308"},
            {
                    std::numeric_limits<double>::infinity(),
                    "Infinity",
            },
            {
                    -std::numeric_limits<double>::infinity(),
                    "-Infinity",
            },
            {
                    std::numeric_limits<double>::quiet_NaN(),
                    "NaN",
            },
            {
                    std::numeric_limits<float>::infinity(),
                    "Infinity",
            },
            {
                    -std::numeric_limits<float>::infinity(),
                    "-Infinity",
            },
            {
                    std::numeric_limits<float>::quiet_NaN(),
                    "NaN",
            }};
    test_input_values.insert(test_input_values.end(), special_input_values.begin(),
                             special_input_values.end());
    for (const auto& [float_value, expected_str] : test_input_values) {
        auto result_str =
                FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE>::to_string(&float_value);
        EXPECT_EQ(result_str, expected_str);
        double deser_float_value = 0.0;
        auto status = FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE>::from_string(
                &deser_float_value, result_str, 0, 0);
        EXPECT_TRUE(status.ok()) << status.to_string();
        double diff_ratio = std::abs(deser_float_value - float_value) / abs(float_value);
        EXPECT_TRUE(
                (float_value == 0 && deser_float_value == 0) || diff_ratio < 1e-15 ||
                (std::isnan(deser_float_value) && std::isnan(float_value)) ||
                (std::isinf(deser_float_value) &&
                 (std::isinf(float_value) || float_value == std::numeric_limits<double>::max() ||
                  float_value == std::numeric_limits<double>::lowest())))
                << "expected double value: " << fmt::format("{:.17g}", float_value)
                << ", expected double str: " << expected_str
                << ", deser double value: " << fmt::format("{:.17g}", deser_float_value)
                << ", diff_ratio: " << fmt::format("{:.17g}", diff_ratio);
    }
}
} // namespace doris