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

#include <cstring>
#include <fstream>
#include <iostream>

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/decimal12.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_string.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/olap_common.h"

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
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
    auto data_type_serde = data_type_ptr->get_serde();
    std::ifstream input_file(test_data_dir + "/ser_float_3.0.txt");
    EXPECT_TRUE(input_file.is_open());
    std::string line;
    int line_index = 0;
    while (std::getline(input_file, line)) {
        Field restored_field;
        auto status = data_type_serde->from_fe_string(line, restored_field);
        // from_fe_string rejects NaN/Infinity strings
        if (std::isnan(test_input_values[line_index]) ||
            std::isinf(test_input_values[line_index])) {
            EXPECT_FALSE(status.ok());
            line_index++;
            continue;
        }
        EXPECT_TRUE(status.ok()) << status.to_string();
        float deser_float_value = restored_field.get<TYPE_FLOAT>();
        float diff_ratio = std::abs(deser_float_value - test_input_values[line_index]) /
                           abs(test_input_values[line_index]);
        EXPECT_TRUE((test_input_values[line_index] == 0 && deser_float_value == 0) ||
                    diff_ratio < 1e-6)
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
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
    auto data_type_serde = data_type_ptr->get_serde();
    std::ifstream input_file(test_data_dir + "/ser_double_3.0.txt");
    EXPECT_TRUE(input_file.is_open());
    std::string line;
    int line_index = 0;
    while (std::getline(input_file, line)) {
        Field restored_field;
        auto status = data_type_serde->from_fe_string(line, restored_field);
        // from_fe_string rejects NaN/Infinity strings, and also rejects
        // double::max()/lowest() whose string representation parses to Infinity
        if (std::isnan(test_input_values[line_index]) ||
            std::isinf(test_input_values[line_index])) {
            EXPECT_FALSE(status.ok());
            line_index++;
            continue;
        }
        EXPECT_TRUE(status.ok()) << status.to_string();
        double deser_float_value = restored_field.get<TYPE_DOUBLE>();
        double diff_ratio = std::abs(deser_float_value - test_input_values[line_index]) /
                            abs(test_input_values[line_index]);
        EXPECT_TRUE((test_input_values[line_index] == 0 && deser_float_value == 0) ||
                    diff_ratio < 1e-15)
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
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
    auto data_type_serde = data_type_ptr->get_serde();
    for (const auto& [float_value, expected_str] : test_input_values) {
        auto field = Field::create_field<TYPE_FLOAT>(float_value);
        auto result_str = data_type_serde->to_olap_string(field);
        EXPECT_EQ(result_str, expected_str);
        Field restored_field;
        auto status = data_type_serde->from_fe_string(result_str, restored_field);
        // from_fe_string rejects NaN/Infinity strings
        if (std::isnan(float_value) || std::isinf(float_value)) {
            EXPECT_FALSE(status.ok());
            continue;
        }
        EXPECT_TRUE(status.ok()) << status.to_string();
        float deser_float_value = restored_field.get<TYPE_FLOAT>();
        float diff_ratio = std::abs(deser_float_value - float_value) / abs(float_value);
        EXPECT_TRUE((float_value == 0 && deser_float_value == 0) || diff_ratio < 1e-6)
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
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
    auto data_type_serde = data_type_ptr->get_serde();
    for (const auto& [float_value, expected_str] : test_input_values) {
        auto field = Field::create_field<TYPE_DOUBLE>(float_value);
        auto result_str = data_type_serde->to_olap_string(field);
        EXPECT_EQ(result_str, expected_str);
        Field restored_field;
        auto status = data_type_serde->from_fe_string(result_str, restored_field);
        // from_fe_string rejects NaN/Infinity strings, and also rejects
        // double::max()/lowest() whose string representation parses to Infinity
        if (std::isnan(float_value) || std::isinf(float_value) ||
            float_value == std::numeric_limits<double>::max() ||
            float_value == std::numeric_limits<double>::lowest()) {
            EXPECT_FALSE(status.ok());
            continue;
        }
        EXPECT_TRUE(status.ok()) << status.to_string();
        double deser_float_value = restored_field.get<TYPE_DOUBLE>();
        double diff_ratio = std::abs(deser_float_value - float_value) / abs(float_value);
        EXPECT_TRUE((float_value == 0 && deser_float_value == 0) || diff_ratio < 1e-15)
                << "expected double value: " << fmt::format("{:.17g}", float_value)
                << ", expected double str: " << expected_str
                << ", deser double value: " << fmt::format("{:.17g}", deser_float_value)
                << ", diff_ratio: " << fmt::format("{:.17g}", diff_ratio);
    }
}

// =============================================================================
// Tests for to_olap_string / from_zonemap_string on DataTypeSerDe
//
// Background:
//   ZoneMap index serializes min/max values via to_olap_string()
//   and deserializes via from_zonemap_string(). The from_zonemap_string()
//   method internally sets ignore_scale=true for DecimalV3 types to avoid
//   double-scaling the raw unscaled integer stored in ZoneMap.
//
//   Key difference vs normal from_fe_string:
//     - DecimalV2: to_olap_string uses decimal12_t::to_string() which outputs
//       "integer.fraction" with 9 zero-padded fractional digits (e.g. "123.456000000").
//       from_zonemap_string still works correctly because DecimalV2's parser
//       hardcodes scale=9 regardless of the ignore_scale setting.
//     - Decimal32/64/128I/256: to_olap_string outputs the RAW INTEGER string
//       (the unscaled internal value). E.g., Decimal(9,2) value 123.45 has
//       internal integer 12345, so to_olap_string outputs "12345".
//       from_zonemap_string uses ignore_scale=true → scale=0, parsing as integer.
//     - Float/Double: to_olap_string uses CastToString::from_number, which outputs
//       "NaN", "Infinity", "-Infinity" for special values. But from_zonemap_string
//       uses fast_float::from_chars which REJECTS these strings. In practice, ZoneMap
//       tracks NaN/Inf via boolean flags (has_nan, has_positive_inf, has_negative_inf),
//       so the min/max values never contain NaN/Inf.
//     - DateV1 (TYPE_DATE): to_olap_string outputs "YYYY-MM-DD".
//     - DateTimeV1 (TYPE_DATETIME): to_olap_string outputs "YYYY-MM-DD HH:MM:SS".
//     - DateV2: to_olap_string outputs "YYYY-MM-DD".
//     - DateTimeV2: to_olap_string outputs "YYYY-MM-DD HH:MM:SS[.ffffff]".
//       Microsecond part only appears when microsecond > 0 (default scale=-1).
//       Note: the old ZoneMap code used hardcoded scale=6 (always 6 fractional digits),
//       but the new to_olap_string omits trailing fractional zeros.
// =============================================================================

// ---------------------------------------------------------------------------
// Decimal32: to_olap_string outputs RAW integer (unscaled value).
//   Internal representation: value * 10^scale.
//   E.g., Decimal(9,2) value 123.45 → internal int32 = 12345 → "12345".
//   from_zonemap_string reads "12345" as integer 12345 (ignore_scale=true internally).
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_decimal32) {
    // Create Decimal(9,2) data type (precision=9, scale=2)
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL32, /*precision=*/9, /*scale=*/2);
    auto serde = data_type_ptr->get_serde();

    // Test cases: {internal_int32_value, expected_olap_string}
    // actual_decimal_value = internal / 10^scale
    std::vector<std::pair<int32_t, std::string>> test_cases = {
            // 123.45 → internal=12345 → "12345"
            {12345, "12345"},
            // -1.00 → internal=-100 → "-100"
            {-100, "-100"},
            // 0.00 → internal=0 → "0"
            {0, "0"},
            // 999999999 → max for Decimal(9,2): 9999999.99
            {999999999, "999999999"},
            // -999999999 → min for Decimal(9,2): -9999999.99
            {-999999999, "-999999999"},
            // 1 → 0.01
            {1, "1"},
            // -1 → -0.01
            {-1, "-1"},
    };

    for (const auto& [int_val, expected_str] : test_cases) {
        Decimal32 dec(int_val);
        auto field = Field::create_field<TYPE_DECIMAL32>(dec);
        // Verify to_olap_string output matches expected raw integer string
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, expected_str)
                << "Decimal32 to_olap_string failed for internal value " << int_val;

        // Verify round-trip: from_zonemap_string should restore the same internal value
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_value = restored_field.get<TYPE_DECIMAL32>();
        EXPECT_EQ(restored_value.value, int_val)
                << "Decimal32 round-trip failed for string '" << result_str << "'";
    }
}

// ---------------------------------------------------------------------------
// Decimal64: same pattern as Decimal32, but 64-bit integer.
//   E.g., Decimal(18,4) value 12345.6789 → internal int64 = 123456789 → "123456789".
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_decimal64) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL64, /*precision=*/18, /*scale=*/4);
    auto serde = data_type_ptr->get_serde();

    std::vector<std::pair<int64_t, std::string>> test_cases = {
            // 12345.6789 → internal=123456789
            {123456789L, "123456789"},
            // 0 → "0"
            {0L, "0"},
            // -1 → -0.0001
            {-1L, "-1"},
            // Large value near max
            {999999999999999999L, "999999999999999999"},
            {-999999999999999999L, "-999999999999999999"},
            // Small fractional: 0.0001
            {1L, "1"},
    };

    for (const auto& [int_val, expected_str] : test_cases) {
        Decimal64 dec(int_val);
        auto field = Field::create_field<TYPE_DECIMAL64>(dec);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, expected_str)
                << "Decimal64 to_olap_string failed for internal value " << int_val;

        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_value = restored_field.get<TYPE_DECIMAL64>();
        EXPECT_EQ(restored_value.value, int_val)
                << "Decimal64 round-trip failed for string '" << result_str << "'";
    }
}

// ---------------------------------------------------------------------------
// Decimal128I: to_olap_string uses fmt::format("{}", int128_value).
//   E.g., Decimal(38,6) value 123456789.123456 → internal int128 = 123456789123456.
//   Output: "123456789123456".
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_decimal128i) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL128I, /*precision=*/38, /*scale=*/6);
    auto serde = data_type_ptr->get_serde();

    // int128_t values and expected strings
    struct TestCase {
        int128_t int_val;
        std::string expected_str;
    };
    std::vector<TestCase> test_cases = {
            // 123456789.123456 → internal=123456789123456
            {(int128_t)123456789123456LL, "123456789123456"},
            // 0
            {(int128_t)0, "0"},
            // -1
            {(int128_t)-1, "-1"},
            // Positive large value exceeding int64 range
            // 10^18 * 100 = 10^20
            {(int128_t)1000000000000000000LL * 100, "100000000000000000000"},
    };

    for (const auto& tc : test_cases) {
        Decimal128V3 dec(tc.int_val);
        auto field = Field::create_field<TYPE_DECIMAL128I>(dec);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str)
                << "Decimal128I to_olap_string failed for expected '" << tc.expected_str << "'";

        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_value = restored_field.get<TYPE_DECIMAL128I>();
        EXPECT_EQ(restored_value.value, tc.int_val)
                << "Decimal128I round-trip failed for string '" << result_str << "'";
    }
}

// ---------------------------------------------------------------------------
// Decimal256: to_olap_string uses wide::to_string(value).
//   Same pattern: raw integer string from internal representation.
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_decimal256) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL256, /*precision=*/76, /*scale=*/10);
    auto serde = data_type_ptr->get_serde();

    // Use int128_t-constructible values for simplicity
    // (wide::Int256 can be constructed from int128_t)
    struct TestCase {
        wide::Int256 int_val;
        std::string expected_str;
    };
    std::vector<TestCase> test_cases = {
            // Simple positive
            {wide::Int256(123456789LL), "123456789"},
            // Zero
            {wide::Int256(0), "0"},
            // Negative
            {wide::Int256(-99999LL), "-99999"},
            // Large value: 10^20
            {wide::Int256((int128_t)1000000000000000000LL * 100), "100000000000000000000"},
    };

    for (const auto& tc : test_cases) {
        Decimal256 dec(tc.int_val);
        auto field = Field::create_field<TYPE_DECIMAL256>(dec);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str)
                << "Decimal256 to_olap_string failed for expected '" << tc.expected_str << "'";

        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_value = restored_field.get<TYPE_DECIMAL256>();
        EXPECT_EQ(restored_value.value, tc.int_val)
                << "Decimal256 round-trip failed for string '" << result_str << "'";
    }
}

// ---------------------------------------------------------------------------
// DecimalV2: to_olap_string uses decimal12_t(int_value, frac_value).to_string().
//   decimal12_t::to_string() outputs "integer.fraction" with 9 zero-padded fractional
//   digits. E.g., DecimalV2(123.456) → int_value=123, frac_value=456000000 →
//   decimal12_t(123, 456000000).to_string() → "123.456000000".
//
//   from_zonemap_string with ignore_scale=TRUE internally parses this as a normal decimal string
//   with the data type's scale (9). With ignore_scale=TRUE, scale would be 0 and the
//   fractional part would be truncated — that is WRONG for DecimalV2.
//   However, from_zonemap_string uses ignore_scale=TRUE, and this still works because
//   DecimalV2's parser (read_decimal_text_impl) hardcodes DecimalV2Value::SCALE=9
//   regardless of the passed-in scale, making ignore_scale irrelevant for DecimalV2.
//
//   Note: this is different from DecimalV3 where storage is raw integer.
//   DecimalV2 storage string always contains a decimal point.
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_decimalv2) {
    auto data_type_ptr =
            DataTypeFactory::instance().create_data_type(TYPE_DECIMALV2, /*is_nullable=*/false,
                                                         /*precision=*/27, /*scale=*/9);
    auto serde = data_type_ptr->get_serde();
    // DecimalV2 storage string has decimal point. from_zonemap_string sets ignore_scale=true,
    // but DecimalV2's parser hardcodes scale=9 regardless, so round-trip works correctly.

    // Test cases: {DecimalV2Value, expected_to_olap_string}
    // DecimalV2Value internally stores value * 10^9.
    // decimal12_t::to_string format: "integer.fraction" with %09u for fraction.
    struct TestCase {
        DecimalV2Value value;
        std::string expected_str;
    };

    std::vector<TestCase> test_cases = {
            // 123.456 → int=123, frac=456000000 → "123.456000000"
            {DecimalV2Value(123, 456000000), "123.456000000"},
            // 0.0 → int=0, frac=0 → "0.000000000"
            {DecimalV2Value(0, 0), "0.000000000"},
            // -1.5 → int=-1, frac=-500000000 → "-1.500000000"
            {DecimalV2Value(-1, -500000000), "-1.500000000"},
            // Pure integer: 42.0 → "42.000000000"
            {DecimalV2Value(42, 0), "42.000000000"},
            // Tiny fraction: 0.000000001 → int=0, frac=1 → "0.000000001"
            {DecimalV2Value(0, 1), "0.000000001"},
            // Max fraction: 0.999999999 → int=0, frac=999999999 → "0.999999999"
            {DecimalV2Value(0, 999999999), "0.999999999"},
            // Large integer: 999999999999999999.0
            {DecimalV2Value(999999999999999999LL, 0), "999999999999999999.000000000"},
            // Negative with fraction
            {DecimalV2Value(-123, -456000000), "-123.456000000"},
    };

    for (const auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_DECIMALV2>(tc.value);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str) << "DecimalV2 to_olap_string failed";

        // Round-trip: from_zonemap_string should restore the same value
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_value = restored_field.get<TYPE_DECIMALV2>();
        EXPECT_EQ(restored_value, tc.value)
                << "DecimalV2 round-trip failed for string '" << result_str << "'"
                << ", expected int_value=" << tc.value.int_value()
                << ", frac_value=" << tc.value.frac_value()
                << ", got int_value=" << restored_value.int_value()
                << ", frac_value=" << restored_value.frac_value();
    }
}

// ---------------------------------------------------------------------------
// Float: to_olap_string / from_zonemap_string for normal values.
//   to_olap_string uses CastToString::from_number which calls _fast_to_buffer.
//   Format: fmt "{:.7g}" (digits10+1=7 significant digits).
//   NaN/Inf are serialized as "NaN", "Infinity", "-Infinity" but from_zonemap_string
//   (which uses fast_float::from_chars) CANNOT parse them back → returns error.
//   In ZoneMap, NaN/Inf are tracked via boolean flags, not stored in min/max values.
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_float_olap_string) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
    auto serde = data_type_ptr->get_serde();

    // Normal float values: to_olap_string → from_zonemap_string round-trip
    std::vector<std::pair<float, std::string>> normal_cases = {
            {0.0f, "0"},       {1.0f, "1"},
            {-1.0f, "-1"},     {123.456f, "123.456"},
            {0.001f, "0.001"}, {1234567.0f, "1234567"},
            {1e-10f, "1e-10"}, {3.402823e+38f, "3.402823e+38"},
    };

    for (const auto& [val, expected_str] : normal_cases) {
        auto field = Field::create_field<TYPE_FLOAT>(val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, expected_str)
                << "Float to_olap_string failed for " << fmt::format("{:.9g}", val);

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        float restored_val = restored_field.get<TYPE_FLOAT>();
        float diff = std::abs(restored_val - val);
        EXPECT_TRUE(val == 0 ? restored_val == 0 : diff / std::abs(val) < 1e-6)
                << "Float round-trip: expected " << val << ", got " << restored_val;
    }

    // Special values: to_olap_string produces strings, but from_zonemap_string FAILS
    // This documents the intentional behavior: ZoneMap uses boolean flags for these.
    {
        // NaN → "NaN", but from_zonemap_string cannot parse "NaN"
        auto field = Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::quiet_NaN());
        EXPECT_EQ(serde->to_olap_string(field), "NaN");
        Field restored_field;
        auto status = serde->from_zonemap_string("NaN", restored_field);
        EXPECT_FALSE(status.ok()) << "from_zonemap_string should reject 'NaN'";
    }
    {
        // +Infinity → "Infinity"
        auto field = Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::infinity());
        EXPECT_EQ(serde->to_olap_string(field), "Infinity");
        Field restored_field;
        auto status = serde->from_zonemap_string("Infinity", restored_field);
        EXPECT_FALSE(status.ok()) << "from_zonemap_string should reject 'Infinity'";
    }
    {
        // -Infinity → "-Infinity"
        auto field = Field::create_field<TYPE_FLOAT>(-std::numeric_limits<float>::infinity());
        EXPECT_EQ(serde->to_olap_string(field), "-Infinity");
        Field restored_field;
        auto status = serde->from_zonemap_string("-Infinity", restored_field);
        EXPECT_FALSE(status.ok()) << "from_zonemap_string should reject '-Infinity'";
    }
}

// ---------------------------------------------------------------------------
// Double: same pattern as Float.
//   The expected strings in this case follow current serializer behavior.
//   Note: for DBL_MAX/lowest, current formatting rounds to a boundary string that
//   is rejected by from_zonemap_string (parsed as Infinity), so these two values
//   are validated for to_olap_string only.
//   NaN/Inf same behavior: to_olap_string works, from_zonemap_string rejects.
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_double_olap_string) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
    auto serde = data_type_ptr->get_serde();

    std::vector<std::pair<double, std::string>> normal_cases = {
            {0.0, "0"},
            {1.0, "1"},
            {-1.0, "-1"},
            {123.456789, "123.456789"},
            {0.001, "0.001"},
            {1234567890123456.0, "1234567890123456"},
            {1e-100, "1e-100"},
            {std::numeric_limits<double>::lowest(), "-1.797693134862316e+308"},
            {std::numeric_limits<double>::max(), "1.797693134862316e+308"},
    };

    for (const auto& [val, expected_str] : normal_cases) {
        auto field = Field::create_field<TYPE_DOUBLE>(val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, expected_str)
                << "Double to_olap_string failed for " << fmt::format("{:.17g}", val);

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        if (val == std::numeric_limits<double>::lowest() ||
            val == std::numeric_limits<double>::max()) {
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("NaN/Infinity not allowed in olap string"),
                      std::string::npos)
                    << status.to_string();
            continue;
        }

        EXPECT_TRUE(status.ok()) << status.to_string();
        double restored_val = restored_field.get<TYPE_DOUBLE>();
        double diff = std::abs(restored_val - val);
        EXPECT_TRUE(val == 0 ? restored_val == 0 : diff / std::abs(val) < 1e-15)
                << "Double round-trip: expected " << val << ", got " << restored_val;
    }

    // Special values
    {
        auto field = Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN());
        EXPECT_EQ(serde->to_olap_string(field), "NaN");
        Field restored_field;
        EXPECT_FALSE(serde->from_zonemap_string("NaN", restored_field).ok());
    }
    {
        auto field = Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::infinity());
        EXPECT_EQ(serde->to_olap_string(field), "Infinity");
        Field restored_field;
        EXPECT_FALSE(serde->from_zonemap_string("Infinity", restored_field).ok());
    }
    {
        auto field = Field::create_field<TYPE_DOUBLE>(-std::numeric_limits<double>::infinity());
        EXPECT_EQ(serde->to_olap_string(field), "-Infinity");
        Field restored_field;
        EXPECT_FALSE(serde->from_zonemap_string("-Infinity", restored_field).ok());
    }
    {
        // -0.0 → "-0"
        auto field = Field::create_field<TYPE_DOUBLE>(-0.0);
        EXPECT_EQ(serde->to_olap_string(field), "-0");
    }
}

// ---------------------------------------------------------------------------
// DateV1 (TYPE_DATE): to_olap_string outputs "YYYY-MM-DD".
//   Internal representation: VecDateTimeValue, stored as uint24_t in OLAP.
//   The old ZoneMap used VecDateTimeValue::to_string(buf) → "YYYY-MM-DD\0".
//   from_zonemap_string uses CastToDateOrDatetime::from_string_non_strict_mode.
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_datev1) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_DATE, false);
    auto serde = data_type_ptr->get_serde();

    struct TestCase {
        int year, month, day;
        std::string expected_str;
    };
    std::vector<TestCase> test_cases = {
            {2023, 1, 1, "2023-01-01"},   {2000, 12, 31, "2000-12-31"}, {1970, 1, 1, "1970-01-01"},
            {9999, 12, 31, "9999-12-31"}, {1, 1, 1, "0001-01-01"},
    };

    for (const auto& tc : test_cases) {
        VecDateTimeValue date_val;
        date_val.unchecked_set_time(tc.year, tc.month, tc.day, 0, 0, 0);
        date_val.set_type(TIME_DATE);

        auto field = Field::create_field<TYPE_DATE>(date_val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str) << "DateV1 to_olap_string failed for " << tc.year
                                               << "-" << tc.month << "-" << tc.day;

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_val = restored_field.get<TYPE_DATE>();
        EXPECT_EQ(restored_val.year(), tc.year);
        EXPECT_EQ(restored_val.month(), tc.month);
        EXPECT_EQ(restored_val.day(), tc.day);
    }
}

// ---------------------------------------------------------------------------
// DateTimeV1 (TYPE_DATETIME): to_olap_string outputs "YYYY-MM-DD HH:MM:SS".
//   Internal representation: VecDateTimeValue, stored as uint64_t in OLAP.
//   The old ZoneMap used the format:
//     YYYYMMDDHHMMSSxxxxxx → "YYYY-MM-DD HH:MM:SS".
//   from_zonemap_string uses CastToDateOrDatetime::from_string_non_strict_mode<true>.
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_datetimev1) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_DATETIME, false);
    auto serde = data_type_ptr->get_serde();

    struct TestCase {
        int year, month, day, hour, minute, second;
        std::string expected_str;
    };
    std::vector<TestCase> test_cases = {
            {2023, 6, 15, 14, 30, 59, "2023-06-15 14:30:59"},
            {2000, 1, 1, 0, 0, 0, "2000-01-01 00:00:00"},
            {1970, 1, 1, 0, 0, 0, "1970-01-01 00:00:00"},
            {9999, 12, 31, 23, 59, 59, "9999-12-31 23:59:59"},
    };

    for (const auto& tc : test_cases) {
        VecDateTimeValue dt_val;
        dt_val.unchecked_set_time(tc.year, tc.month, tc.day, tc.hour, tc.minute, tc.second);
        dt_val.set_type(TIME_DATETIME);

        auto field = Field::create_field<TYPE_DATETIME>(dt_val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str)
                << "DateTimeV1 to_olap_string failed for " << tc.expected_str;

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_val = restored_field.get<TYPE_DATETIME>();
        EXPECT_EQ(restored_val.year(), tc.year);
        EXPECT_EQ(restored_val.month(), tc.month);
        EXPECT_EQ(restored_val.day(), tc.day);
        EXPECT_EQ(restored_val.hour(), tc.hour);
        EXPECT_EQ(restored_val.minute(), tc.minute);
        EXPECT_EQ(restored_val.second(), tc.second);
    }
}

// ---------------------------------------------------------------------------
// DateV2 (TYPE_DATEV2): to_olap_string outputs "YYYY-MM-DD".
//   Internal: DateV2Value<DateV2ValueType>, stored as uint32_t (bit-packed).
//   Bit layout: year(16bits) << 9 | month(4bits) << 5 | day(5bits).
//   from_zonemap_string uses strptime "%Y-%m-%d", then bit-packs the parsed date.
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_datev2) {
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(TYPE_DATEV2, false);
    auto serde = data_type_ptr->get_serde();

    struct TestCase {
        int year, month, day;
        std::string expected_str;
    };
    std::vector<TestCase> test_cases = {
            {2023, 1, 15, "2023-01-15"},  {2000, 12, 31, "2000-12-31"}, {1970, 1, 1, "1970-01-01"},
            {9999, 12, 31, "9999-12-31"}, {1, 1, 1, "0001-01-01"},
    };

    for (const auto& tc : test_cases) {
        DateV2Value<DateV2ValueType> date_val;
        date_val.unchecked_set_time(tc.year, tc.month, tc.day, 0, 0, 0, 0);

        auto field = Field::create_field<TYPE_DATEV2>(date_val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str)
                << "DateV2 to_olap_string failed for " << tc.expected_str;

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_val = restored_field.get<TYPE_DATEV2>();
        EXPECT_EQ(restored_val.year(), tc.year);
        EXPECT_EQ(restored_val.month(), tc.month);
        EXPECT_EQ(restored_val.day(), tc.day);
    }
}

// ---------------------------------------------------------------------------
// DateTimeV2 (TYPE_DATETIMEV2): to_olap_string outputs "YYYY-MM-DD HH:MM:SS.ffffff".
//   Internal: DateV2Value<DateTimeV2ValueType>, stored as uint64_t (bit-packed).
//   to_olap_string always calls CastToString::from_datetimev2(value, 6) because
//   historically the Field type for DateTimeV2 always stores 6-digit (microsecond) precision.
//   With scale=6, the fractional part is ALWAYS written with 6 digits, even when microsecond=0.
//
//   Multiple serde scale values are tested, but since to_olap_string always uses scale=6,
//   the output format is the same regardless of the serde's own scale:
//     scale=0: output is still "YYYY-MM-DD HH:MM:SS.000000" (fractional part always present)
//     scale=3: fractional part always present, 6 digits
//     scale=6: fractional part always present, 6 digits
//
//   from_zonemap_string uses from_date_format_str("%Y-%m-%d %H:%i:%s.%f").
// ---------------------------------------------------------------------------
TEST_F(OlapTypeTest, ser_deser_datetimev2_no_microsecond) {
    // Test with scale=0 serde, but to_olap_string always uses scale=6:
    // fractional part is always written even when microsecond=0.
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(
            TYPE_DATETIMEV2, /*is_nullable=*/false, /*precision=*/0, /*scale=*/0);
    auto serde = data_type_ptr->get_serde();

    struct TestCase {
        int year, month, day, hour, minute, second;
        uint32_t microsecond;
        std::string expected_str;
    };
    std::vector<TestCase> test_cases = {
            // No microseconds → fractional part is still written as .000000 (scale=6 always)
            {2023, 6, 15, 14, 30, 59, 0, "2023-06-15 14:30:59.000000"},
            {2000, 1, 1, 0, 0, 0, 0, "2000-01-01 00:00:00.000000"},
            {9999, 12, 31, 23, 59, 59, 0, "9999-12-31 23:59:59.000000"},
    };

    for (const auto& tc : test_cases) {
        DateV2Value<DateTimeV2ValueType> dt_val;
        dt_val.unchecked_set_time(tc.year, tc.month, tc.day, tc.hour, tc.minute, tc.second,
                                  tc.microsecond);
        auto field = Field::create_field<TYPE_DATETIMEV2>(dt_val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str)
                << "DateTimeV2(scale=0) to_olap_string failed for " << tc.expected_str;

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_val = restored_field.get<TYPE_DATETIMEV2>();
        EXPECT_EQ(restored_val.year(), tc.year);
        EXPECT_EQ(restored_val.month(), tc.month);
        EXPECT_EQ(restored_val.day(), tc.day);
        EXPECT_EQ(restored_val.hour(), tc.hour);
        EXPECT_EQ(restored_val.minute(), tc.minute);
        EXPECT_EQ(restored_val.second(), tc.second);
    }
}

TEST_F(OlapTypeTest, ser_deser_datetimev2_with_microsecond) {
    // Test with scale=6 (full microsecond precision)
    // to_olap_string always uses scale=6: fractional part is always written with 6 digits.
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(
            TYPE_DATETIMEV2, /*is_nullable=*/false, /*precision=*/0, /*scale=*/6);
    auto serde = data_type_ptr->get_serde();

    struct TestCase {
        int year, month, day, hour, minute, second;
        uint32_t microsecond;
        std::string expected_str;
    };
    std::vector<TestCase> test_cases = {
            // microsecond=123456 → ".123456"
            {2023, 6, 15, 14, 30, 59, 123456, "2023-06-15 14:30:59.123456"},
            // microsecond=1 → ".000001"
            {2023, 1, 1, 0, 0, 0, 1, "2023-01-01 00:00:00.000001"},
            // microsecond=999999 → ".999999"
            {9999, 12, 31, 23, 59, 59, 999999, "9999-12-31 23:59:59.999999"},
            // microsecond=100000 → ".100000"
            {2023, 3, 15, 12, 0, 0, 100000, "2023-03-15 12:00:00.100000"},
            // microsecond=0 → fractional part is still written as .000000 (scale=6 always)
            {2023, 3, 15, 12, 0, 0, 0, "2023-03-15 12:00:00.000000"},
    };

    for (const auto& tc : test_cases) {
        DateV2Value<DateTimeV2ValueType> dt_val;
        dt_val.unchecked_set_time(tc.year, tc.month, tc.day, tc.hour, tc.minute, tc.second,
                                  tc.microsecond);
        auto field = Field::create_field<TYPE_DATETIMEV2>(dt_val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, tc.expected_str)
                << "DateTimeV2(scale=6) to_olap_string failed for " << tc.expected_str;

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_val = restored_field.get<TYPE_DATETIMEV2>();
        EXPECT_EQ(restored_val.year(), tc.year);
        EXPECT_EQ(restored_val.month(), tc.month);
        EXPECT_EQ(restored_val.day(), tc.day);
        EXPECT_EQ(restored_val.hour(), tc.hour);
        EXPECT_EQ(restored_val.minute(), tc.minute);
        EXPECT_EQ(restored_val.second(), tc.second);
        EXPECT_EQ(restored_val.microsecond(), tc.microsecond);
    }
}

TEST_F(OlapTypeTest, ser_deser_datetimev2_scale3) {
    // Test with scale=3 (millisecond precision)
    // to_olap_string always uses scale=6: fractional part is always written with 6 digits.
    // The data type has scale=3, but to_olap_string ignores this and always uses scale=6
    // because historically Field type for DateTimeV2 always stores 6-digit precision.
    // from_zonemap_string should still parse back the full microsecond value stored in the field.
    auto data_type_ptr = DataTypeFactory::instance().create_data_type(
            TYPE_DATETIMEV2, /*is_nullable=*/false, /*precision=*/0, /*scale=*/3);
    auto serde = data_type_ptr->get_serde();

    {
        // 123000 microseconds (= 123 milliseconds)
        // to_olap_string outputs full 6+digit microsecond: ".123000"
        DateV2Value<DateTimeV2ValueType> dt_val;
        dt_val.unchecked_set_time(2023, 6, 15, 14, 30, 59, 123000);
        auto field = Field::create_field<TYPE_DATETIMEV2>(dt_val);
        auto result_str = serde->to_olap_string(field);
        EXPECT_EQ(result_str, "2023-06-15 14:30:59.123000")
                << "DateTimeV2(scale=3) to_olap_string failed";

        // Round-trip
        Field restored_field;
        auto status = serde->from_zonemap_string(result_str, restored_field);
        EXPECT_TRUE(status.ok()) << status.to_string();
        auto restored_val = restored_field.get<TYPE_DATETIMEV2>();
        EXPECT_EQ(restored_val.year(), 2023);
        EXPECT_EQ(restored_val.month(), 6);
        EXPECT_EQ(restored_val.day(), 15);
        EXPECT_EQ(restored_val.hour(), 14);
        EXPECT_EQ(restored_val.minute(), 30);
        EXPECT_EQ(restored_val.second(), 59);
        EXPECT_EQ(restored_val.microsecond(), 123000);
    }
}

TEST_F(OlapTypeTest, char_type_with_padding) {
    auto data_type =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_CHAR, 0, 0, 20);
    auto serde = data_type->get_serde();

    {
        char buf[20];
        memset(buf, 0, sizeof(buf));
        memcpy(buf, "hello", 5);
        Slice olap_value(buf, 20);

        std::string expected("hello", 5);
        expected.append(15, '\0');
        std::string expected_serde = expected;

        auto field = Field::create_field_from_olap_value<TYPE_CHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for CHAR(20) 'hello'"
                                             << "\n  expected len=" << expected_serde.size()
                                             << "\n  actual   len=" << serde_str.size();
    }

    {
        char buf[20];
        memset(buf, 'x', 20);
        Slice olap_value(buf, 20);

        std::string expected(20, 'x');
        std::string expected_serde = expected;

        auto field = Field::create_field_from_olap_value<TYPE_CHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for CHAR(20) filled 'x'";
    }

    {
        char buf[20];
        memset(buf, 0, 20);
        Slice olap_value(buf, 20);

        std::string expected(20, '\0');
        std::string expected_serde = expected;

        auto field = Field::create_field_from_olap_value<TYPE_CHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for CHAR(20) empty"
                                             << "\n  expected len=" << expected_serde.size()
                                             << "\n  actual   len=" << serde_str.size();
    }
}

TEST_F(OlapTypeTest, varchar_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_VARCHAR, 0, 0, 100);
    auto serde = data_type->get_serde();

    struct TestCase {
        std::string input;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {"hello world", "hello world", "hello world"},
            {"", "", ""},
    };

    for (auto& tc : test_cases) {
        Slice olap_value(tc.input.data(), tc.input.size());

        auto field = Field::create_field_from_olap_value<TYPE_VARCHAR>(
                StringRef(olap_value.data, olap_value.size));
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for VARCHAR '" << tc.input << "'";
    }
}

TEST_F(OlapTypeTest, date_v1_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_DATE, false);
    auto serde = data_type->get_serde();

    auto make_olap_date = [](int year, int mon, int day) -> uint24_t {
        return uint24_t(year * 16 * 32 + mon * 32 + day);
    };

    struct TestCase {
        int year, month, day;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {2023, 6, 15, "2023-06-15", "2023-06-15"},
            {2000, 1, 1, "2000-01-01", "2000-01-01"},
            {9999, 12, 31, "9999-12-31", "9999-12-31"},
            {1, 1, 1, "0001-01-01", "0001-01-01"},
    };

    for (auto& tc : test_cases) {
        uint24_t olap_value = make_olap_date(tc.year, tc.month, tc.day);

        auto field = Field::create_field_from_olap_value<TYPE_DATE>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for DATE " << tc.expected;
    }
}

TEST_F(OlapTypeTest, datetime_v1_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_DATETIME, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        int64_t olap_value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {20230615120000L, "2023-06-15 12:00:00", "2023-06-15 12:00:00"},
            {20000101000000L, "2000-01-01 00:00:00", "2000-01-01 00:00:00"},
            {99991231235959L, "9999-12-31 23:59:59", "9999-12-31 23:59:59"},
            {20230615123456L, "2023-06-15 12:34:56", "2023-06-15 12:34:56"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field_from_olap_value<TYPE_DATETIME>((uint64_t)tc.olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for DATETIME " << tc.expected;
    }
}

TEST_F(OlapTypeTest, datev2_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_DATEV2, false);
    auto serde = data_type->get_serde();

    auto make_datev2 = [](int year, int month, int day) -> uint32_t {
        return (year << 9) | (month << 5) | day;
    };

    struct TestCase {
        int year, month, day;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {2023, 6, 15, "2023-06-15", "2023-06-15"},
            {2000, 1, 1, "2000-01-01", "2000-01-01"},
            {9999, 12, 31, "9999-12-31", "9999-12-31"},
            {1, 1, 1, "0001-01-01", "0001-01-01"},
    };

    for (auto& tc : test_cases) {
        uint32_t olap_value = make_datev2(tc.year, tc.month, tc.day);

        auto field = Field::create_field_from_olap_value<TYPE_DATEV2>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for DATEV2 " << tc.expected;
    }
}

TEST_F(OlapTypeTest, datetimev2_type) {
    auto make_datetimev2 = [](int year, int month, int day, int hour, int minute, int second,
                              int microsecond) -> uint64_t {
        return ((uint64_t)year << 46) | ((uint64_t)month << 42) | ((uint64_t)day << 37) |
               ((uint64_t)hour << 32) | ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
               (uint64_t)microsecond;
    };

    struct TestCase {
        uint64_t olap_value;
        std::string expected;
        std::string expected_serde;
        std::string desc;
    };
    std::vector<TestCase> test_cases = {
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 123456), "2023-06-15 12:34:56.123456",
             "2023-06-15 12:34:56.123456", "non-zero microseconds"},
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 0), "2023-06-15 12:34:56.000000",
             "2023-06-15 12:34:56", "zero microseconds"},
            {make_datetimev2(2023, 1, 1, 0, 0, 0, 123000), "2023-01-01 00:00:00.123000",
             "2023-01-01 00:00:00.123000", "trailing zeros in microseconds"},
            {make_datetimev2(2000, 1, 1, 0, 0, 0, 0), "2000-01-01 00:00:00.000000",
             "2000-01-01 00:00:00", "epoch zero microseconds"},
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 1), "2023-06-15 12:34:56.000001",
             "2023-06-15 12:34:56.000001", "1 microsecond"},
            {make_datetimev2(9999, 12, 31, 23, 59, 59, 999999), "9999-12-31 23:59:59.999999",
             "9999-12-31 23:59:59.999999", "max datetime"},
    };

    for (int scale = 0; scale <= 6; ++scale) {
        auto data_type =
                DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, scale);
        auto serde = data_type->get_serde();

        for (auto& tc : test_cases) {
            auto field = Field::create_field_from_olap_value<TYPE_DATETIMEV2>(tc.olap_value);
            std::string serde_str = serde->to_olap_string(field);

            EXPECT_EQ(tc.expected, serde_str)
                    << "serde mismatch for DATETIMEV2 scale=" << scale << ": " << tc.desc
                    << "\n  expected: " << tc.expected << "\n  serde:    " << serde_str;
        }
    }
}

TEST_F(OlapTypeTest, datetime_v1_vs_v2_precision_difference) {
    {
        auto data_type = DataTypeFactory::instance().create_data_type(TYPE_DATETIME, false);
        auto serde = data_type->get_serde();

        int64_t olap_value = 20230615123456L;
        std::string expected = "2023-06-15 12:34:56";
        std::string expected_serde = expected;
        auto field = Field::create_field_from_olap_value<TYPE_DATETIME>((uint64_t)olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for DATETIME V1";
        EXPECT_EQ(expected.find('.'), std::string::npos)
                << "DATETIME V1 should NOT have fractional seconds";
    }

    {
        auto make_datetimev2 = [](int year, int month, int day, int hour, int minute, int second,
                                  int microsecond) -> uint64_t {
            return ((uint64_t)year << 46) | ((uint64_t)month << 42) | ((uint64_t)day << 37) |
                   ((uint64_t)hour << 32) | ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                   (uint64_t)microsecond;
        };

        auto data_type = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6);
        auto serde = data_type->get_serde();

        uint64_t olap_value = make_datetimev2(2023, 6, 15, 12, 34, 56, 123456);
        std::string expected = "2023-06-15 12:34:56.123456";
        std::string expected_serde = expected;
        auto field = Field::create_field_from_olap_value<TYPE_DATETIMEV2>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(expected_serde, serde_str) << "serde mismatch for DATETIMEV2";
        EXPECT_NE(expected.find('.'), std::string::npos)
                << "DATETIMEV2 should have fractional seconds";
    }
}

TEST_F(OlapTypeTest, decimalv2_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_DECIMALV2, false, 27, 9);
    auto serde = data_type->get_serde();

    struct TestCase {
        int64_t integer;
        int32_t fraction;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, 0, "0.000000000", "0.000000000"},
            {1, 0, "1.000000000", "1.000000000"},
            {0, 100000000, "0.100000000", "0.100000000"},
            {123, 456000000, "123.456000000", "123.456000000"},
            {-123, -456000000, "-123.456000000", "-123.456000000"},
            {999999999999999999L, 999999999, "999999999999999999.999999999",
             "999999999999999999.999999999"},
            {-999999999999999999L, -999999999, "-999999999999999999.999999999",
             "-999999999999999999.999999999"},
            {1, 1, "1.000000001", "1.000000001"},
            {1, 10, "1.000000010", "1.000000010"},
            {1, 100, "1.000000100", "1.000000100"},
            {1, 1000, "1.000001000", "1.000001000"},
            {1, 10000, "1.000010000", "1.000010000"},
            {1, 100000, "1.000100000", "1.000100000"},
            {1, 1000000, "1.001000000", "1.001000000"},
            {1, 10000000, "1.010000000", "1.010000000"},
            {1, 100000000, "1.100000000", "1.100000000"},
            {0, 123456789, "0.123456789", "0.123456789"},
            {42, 500000000, "42.500000000", "42.500000000"},
    };

    for (auto& tc : test_cases) {
        decimal12_t olap_value;
        olap_value.integer = tc.integer;
        olap_value.fraction = tc.fraction;

        auto field = Field::create_field_from_olap_value<TYPE_DECIMALV2>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMALV2 (" << tc.integer << ", " << tc.fraction << ")";
    }
}

TEST_F(OlapTypeTest, decimal32_type) {
    struct TestCase {
        int32_t value;
        int precision;
        int scale;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, 9, 0, "0", "0"},
            {12345, 9, 0, "12345", "12345"},
            {12345, 9, 2, "12345", "12345"},
            {12345, 9, 4, "12345", "12345"},
            {-12345, 9, 2, "-12345", "-12345"},
            {1, 9, 9, "1", "1"},
            {999999999, 9, 0, "999999999", "999999999"},
            {-999999999, 9, 0, "-999999999", "-999999999"},
            {100000000, 9, 9, "100000000", "100000000"},
    };

    for (auto& tc : test_cases) {
        auto data_type = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL32, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        int32_t olap_value = tc.value;
        auto field = Field::create_field_from_olap_value<TYPE_DECIMAL32>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL32 value=" << tc.value;
    }
}

TEST_F(OlapTypeTest, decimal64_type) {
    struct TestCase {
        int64_t value;
        int precision;
        int scale;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, 18, 0, "0", "0"},
            {123456789012345678L, 18, 0, "123456789012345678", "123456789012345678"},
            {123456789012345678L, 18, 6, "123456789012345678", "123456789012345678"},
            {-123456789012345678L, 18, 6, "-123456789012345678", "-123456789012345678"},
            {1, 18, 18, "1", "1"},
            {100000, 18, 5, "100000", "100000"},
            {1000000000000L, 18, 6, "1000000000000", "1000000000000"},
    };

    for (auto& tc : test_cases) {
        auto data_type = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL64, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        int64_t olap_value = tc.value;
        auto field = Field::create_field_from_olap_value<TYPE_DECIMAL64>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL64 value=" << tc.value;
    }
}

TEST_F(OlapTypeTest, decimal128i_type) {
    struct TestCase {
        int128_t value;
        int precision;
        int scale;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, 38, 0, "0", "0"},
            {123456789, 38, 0, "123456789", "123456789"},
            {123456789, 38, 6, "123456789", "123456789"},
            {-123456789, 38, 6, "-123456789", "-123456789"},
            {1, 38, 38, "1", "1"},
            {(int128_t)999999999999999999L * 1000000000L + 999999999, 38, 9,
             "999999999999999999999999999", "999999999999999999999999999"},
    };

    for (auto& tc : test_cases) {
        auto data_type = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL128I, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        int128_t olap_value = tc.value;
        auto field = Field::create_field_from_olap_value<TYPE_DECIMAL128I>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL128I expected=" << tc.expected;
    }
}

TEST_F(OlapTypeTest, decimal256_type) {
    struct TestCase {
        wide::Int256 value;
        int precision;
        int scale;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {wide::Int256(0), 76, 0, "0", "0"},
            {wide::Int256(123456789), 76, 0, "123456789", "123456789"},
            {wide::Int256(123456789), 76, 6, "123456789", "123456789"},
            {wide::Int256(-123456789), 76, 6, "-123456789", "-123456789"},
    };

    for (auto& tc : test_cases) {
        auto data_type = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_DECIMAL256, tc.precision, tc.scale);
        auto serde = data_type->get_serde();

        wide::Int256 olap_value = tc.value;
        auto field = Field::create_field_from_olap_value<TYPE_DECIMAL256>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DECIMAL256 expected=" << tc.expected;
    }
}

TEST_F(OlapTypeTest, float_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        float value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0.0f, "0", "0"},
            {-0.0f, "-0", "-0"},
            {1.0f, "1", "1"},
            {-1.0f, "-1", "-1"},
            {0.5f, "0.5", "0.5"},
            {1.5f, "1.5", "1.5"},
            {0.25f, "0.25", "0.25"},
            {100.0f, "100", "100"},
            {0.001f, "0.001", "0.001"},
            {std::numeric_limits<float>::quiet_NaN(), "NaN", "NaN"},
            {std::numeric_limits<float>::infinity(), "Infinity", "Infinity"},
            {-std::numeric_limits<float>::infinity(), "-Infinity", "-Infinity"},
            {std::numeric_limits<float>::max(), "3.402823e+38", "3.402823e+38"},
            {std::numeric_limits<float>::lowest(), "-3.402823e+38", "-3.402823e+38"},
            {std::numeric_limits<float>::min(), "1.175494e-38", "1.175494e-38"},
            {std::numeric_limits<float>::denorm_min(), "1.401298e-45", "1.401298e-45"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_FLOAT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for FLOAT expected='" << tc.expected << "'";
    }
}

TEST_F(OlapTypeTest, double_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        double value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0.0, "0", "0"},
            {-0.0, "-0", "-0"},
            {1.0, "1", "1"},
            {-1.0, "-1", "-1"},
            {0.5, "0.5", "0.5"},
            {1.5, "1.5", "1.5"},
            {0.25, "0.25", "0.25"},
            {100.0, "100", "100"},
            {3.141592653589793, "3.141592653589793", "3.141592653589793"},
            {0.001, "0.001", "0.001"},
            {std::numeric_limits<double>::quiet_NaN(), "NaN", "NaN"},
            {std::numeric_limits<double>::infinity(), "Infinity", "Infinity"},
            {-std::numeric_limits<double>::infinity(), "-Infinity", "-Infinity"},
            {std::numeric_limits<double>::max(), "1.797693134862316e+308",
             "1.797693134862316e+308"},
            {std::numeric_limits<double>::lowest(), "-1.797693134862316e+308",
             "-1.797693134862316e+308"},
            {std::numeric_limits<double>::min(), "2.225073858507201e-308",
             "2.225073858507201e-308"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_DOUBLE>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for DOUBLE expected='" << tc.expected << "'";
    }
}

TEST_F(OlapTypeTest, bool_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        uint8_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_BOOLEAN>((bool)tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for BOOL=" << (int)tc.value;
    }
}

TEST_F(OlapTypeTest, tinyint_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_TINYINT, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        int8_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},       {1, "1", "1"},          {-1, "-1", "-1"},
            {127, "127", "127"}, {-128, "-128", "-128"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_TINYINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for TINYINT=" << (int)tc.value;
    }
}

TEST_F(OlapTypeTest, smallint_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        int16_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
            {-1, "-1", "-1"},
            {32767, "32767", "32767"},
            {-32768, "-32768", "-32768"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_SMALLINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for SMALLINT=" << tc.value;
    }
}

TEST_F(OlapTypeTest, int_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_INT, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        int32_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
            {-1, "-1", "-1"},
            {2147483647, "2147483647", "2147483647"},
            {-2147483648, "-2147483648", "-2147483648"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_INT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str) << "serde mismatch for INT=" << tc.value;
    }
}

TEST_F(OlapTypeTest, bigint_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        int64_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, "0", "0"},
            {1, "1", "1"},
            {-1, "-1", "-1"},
            {9223372036854775807L, "9223372036854775807", "9223372036854775807"},
            {-9223372036854775807L - 1, "-9223372036854775808", "-9223372036854775808"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_BIGINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for BIGINT expected=" << tc.expected;
    }
}

TEST_F(OlapTypeTest, largeint_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        int128_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {(int128_t)0, "0", "0"},
            {(int128_t)1, "1", "1"},
            {(int128_t)-1, "-1", "-1"},
            {(int128_t)9223372036854775807L, "9223372036854775807", "9223372036854775807"},
            {(int128_t)(-9223372036854775807L - 1), "-9223372036854775808", "-9223372036854775808"},
            {~((int128_t)(1) << 127), "170141183460469231731687303715884105727",
             "170141183460469231731687303715884105727"},
            {(int128_t)(1) << 127, "-170141183460469231731687303715884105728",
             "-170141183460469231731687303715884105728"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_LARGEINT>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for LARGEINT expected=" << tc.expected;
    }
}

TEST_F(OlapTypeTest, ipv4_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_IPV4, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        uint32_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {0, "0.0.0.0", "0.0.0.0"},
            {0xFFFFFFFF, "255.255.255.255", "255.255.255.255"},
            {0x7F000001, "127.0.0.1", "127.0.0.1"},
            {0xC0A80001, "192.168.0.1", "192.168.0.1"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field<TYPE_IPV4>(tc.value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for IPV4 expected=" << tc.expected;
    }
}

TEST_F(OlapTypeTest, ipv6_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_IPV6, false);
    auto serde = data_type->get_serde();

    struct TestCase {
        uint128_t value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {(uint128_t)0, "::", "::"},
            {(uint128_t)1, "::1", "::1"},
            {(uint128_t)(-1), "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
             "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
    };

    for (auto& tc : test_cases) {
        uint128_t olap_value = tc.value;
        auto field = Field::create_field<TYPE_IPV6>(olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for IPV6 expected=" << tc.expected;
    }
}

TEST_F(OlapTypeTest, timestamptz_type) {
    auto data_type = DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6);
    auto serde = data_type->get_serde();

    auto make_datetimev2 = [](int year, int month, int day, int hour, int minute, int second,
                              int microsecond) -> uint64_t {
        return ((uint64_t)year << 46) | ((uint64_t)month << 42) | ((uint64_t)day << 37) |
               ((uint64_t)hour << 32) | ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
               (uint64_t)microsecond;
    };

    struct TestCase {
        uint64_t olap_value;
        std::string expected;
        std::string expected_serde;
    };
    std::vector<TestCase> test_cases = {
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 123456), "2023-06-15 12:34:56.123456+00:00",
             "2023-06-15 12:34:56.123456+00:00"},
            {make_datetimev2(2023, 6, 15, 12, 34, 56, 0), "2023-06-15 12:34:56.000000+00:00",
             "2023-06-15 12:34:56.000000+00:00"},
            {make_datetimev2(2000, 1, 1, 0, 0, 0, 0), "2000-01-01 00:00:00.000000+00:00",
             "2000-01-01 00:00:00.000000+00:00"},
    };

    for (auto& tc : test_cases) {
        auto field = Field::create_field_from_olap_value<TYPE_TIMESTAMPTZ>(tc.olap_value);
        std::string serde_str = serde->to_olap_string(field);

        EXPECT_EQ(tc.expected_serde, serde_str)
                << "serde mismatch for TIMESTAMPTZ expected=" << tc.expected;
    }
}
} // namespace doris