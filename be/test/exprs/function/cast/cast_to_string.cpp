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

#include <cstdlib>
#include <limits>
#include <string>
#include <utility>

#include "core/column/column_map.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/cast/cast_test.h"
#include "util/to_string.h"

namespace doris {
using namespace ut_type;

struct FunctionCastToStringTest : public FunctionCastTest {};
TEST_F(FunctionCastToStringTest, from_float) {
    std::vector<std::pair<float, std::string>> input_values = {
            {
                    0.0,
                    "0",
            },
            {
                    -0.0,
                    "-0",
            },
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
            {
                    123456.12345F,
                    "123456.125",
            },
            {
                    1234567.12345F,
                    "1234567.1",
            },
            {
                    12345678.12345F,
                    "12345678",
            },
            {
                    123456789.12345F,
                    "123456790",
            },
            {
                    1234567890000.12345F,
                    "1234568000000",
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
                    "123.45679",
            },
            {
                    123.456789123F,
                    "123.45679",
            },
            {
                    123456.123456789F,
                    "123456.125",
            },
            {
                    1234567.123456789F,
                    "1234567.1",
            },
            {
                    987654336.0F,
                    "987654340",
            },
            {
                    16777216.0F,
                    "16777216",
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
                    "0.00012345678",
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
                    "1.2345679e-05",
            },
            {
                    0.000000000000001234567F,
                    "1.234567e-15",
            },
            {
                    0.000000000000001234567890123456F,
                    "1.2345679e-15",
            },
            {
                    0.1234567F,
                    "0.1234567",
            },
            {
                    0.123456789F,
                    "0.12345679",
            },

            {
                    1234567890123456.12345F,
                    "1234568000000000",
            },
            {
                    12345678901234567.12345F,
                    "1.2345678e+16",
            },
            {std::numeric_limits<float>::min(), "1.1754944e-38"},
            {std::numeric_limits<float>::lowest(), "-3.4028235e+38"},
            {std::numeric_limits<float>::denorm_min(), "1e-45"},
            {std::numeric_limits<float>::max(), "3.4028235e+38"},
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
    char buffer[64] = {0};
    for (const auto& value_pair : input_values) {
        int len = CastToString::from_number(value_pair.first, buffer);
        EXPECT_EQ(std::string(buffer, len), std::string(value_pair.second));
    }
}
TEST_F(FunctionCastToStringTest, from_double) {
    std::vector<std::pair<double, std::string>> input_values = {
            {
                    0.0,
                    "0",
            },
            {
                    -0.0,
                    "-0",
            },
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
                    "1234567890000.1235",
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
                    "1.2345678901234568e+16",
            },
            {
                    123456789012345678.12345,
                    "1.2345678901234568e+17",
            },
            {std::numeric_limits<float>::min(), "1.1754943508222875e-38"},
            {std::numeric_limits<float>::lowest(), "-3.4028234663852886e+38"},
            {std::numeric_limits<float>::denorm_min(), "1.401298464324817e-45"},
            {std::numeric_limits<float>::max(), "3.4028234663852886e+38"},
            {std::numeric_limits<double>::min(), "2.2250738585072014e-308"},
            {std::numeric_limits<double>::lowest(), "-1.7976931348623157e+308"},
            {std::numeric_limits<double>::denorm_min(), "5e-324"},
            {std::numeric_limits<double>::max(), "1.7976931348623157e+308"},
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
    char buffer[64] = {0};
    for (const auto& value_pair : input_values) {
        int len = CastToString::from_number(value_pair.first, buffer);
        EXPECT_EQ(std::string(buffer, len), std::string(value_pair.second));
    }
}

TEST_F(FunctionCastToStringTest, from_map) {
    auto map_type = std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));

    auto serde = map_type->get_serde();

    auto key_column = ColumnHelper::create_nullable_column<DataTypeString>({"123", "456"}, {0, 1});
    auto value_column =
            ColumnHelper::create_nullable_column<DataTypeString>({"abc", "def"}, {1, 0});
    auto offset_column = ColumnArray::ColumnOffsets::create();
    offset_column->insert_value(2);
    auto column = ColumnMap::create(key_column, value_column, std::move(offset_column));
    // {"123":null,"456":"def"}
    ColumnString tmp_col;
    auto format_options = DataTypeSerDe::FormatOptions();
    serde->to_string_batch(*column, tmp_col, format_options);
    EXPECT_EQ(tmp_col.get_data_at(0).to_string(), "{\"123\":null, null:\"def\"}");
}
} // namespace doris
