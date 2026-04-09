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

#include <string>

#include "function_test_util.h"
#include "util/timezone_utils.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

// ==================== String input tests ====================

TEST(VTimestampSparkTest, string_date_only) {
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2024-01-15")}, std::string("2024-01-15 00:00:00")},
            {{std::string("2020-02-29")}, std::string("2020-02-29 00:00:00")},
            {{std::string("1970-01-01")}, std::string("1970-01-01 00:00:00")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, string_datetime_no_frac) {
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2024-01-15 14:30:45")}, std::string("2024-01-15 14:30:45")},
            {{std::string("2020-02-29 00:00:00")}, std::string("2020-02-29 00:00:00")},
            {{std::string("9999-12-31 23:59:59")}, std::string("9999-12-31 23:59:59")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, string_datetime_with_microseconds) {
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2024-01-15 14:30:45.123456")},
             std::string("2024-01-15 14:30:45.123456")},
            {{std::string("2024-01-15 14:30:45.100000")},
             std::string("2024-01-15 14:30:45.100000")},
            {{std::string("2024-01-15 14:30:45.000001")},
             std::string("2024-01-15 14:30:45.000001")},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

TEST(VTimestampSparkTest, string_truncate_beyond_microseconds) {
    // Spark truncates (not rounds) fractional seconds beyond 6 digits
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2024-01-15 14:30:45.1234567")},
             std::string("2024-01-15 14:30:45.123456")},
            {{std::string("2024-01-15 14:30:45.123456789")},
             std::string("2024-01-15 14:30:45.123456")},
            {{std::string("2024-01-15 14:30:45.9999999")},
             std::string("2024-01-15 14:30:45.999999")},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

TEST(VTimestampSparkTest, string_t_separator) {
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2024-01-15T14:30:45")}, std::string("2024-01-15 14:30:45")},
            {{std::string("2024-01-15T14:30:45.123456")},
             std::string("2024-01-15 14:30:45.123456")},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

TEST(VTimestampSparkTest, string_with_timezone) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Z means UTC
            {{std::string("2024-01-15 14:30:45Z")}, std::string("2024-01-15 22:30:45")},
            // +08:00 is same as session tz (Asia/Shanghai), no conversion
            {{std::string("2024-01-15 14:30:45+08:00")}, std::string("2024-01-15 14:30:45")},
            // -05:00 → +08:00 = +13 hours
            {{std::string("2024-01-15 01:00:00-05:00")}, std::string("2024-01-15 14:00:00")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, string_invalid_formats) {
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // Spark only accepts '-' as date separator
            {{std::string("2024/01/15")}, Null()},
            {{std::string("2024.01.15")}, Null()},
            {{std::string("2024_01_15")}, Null()},
            // Empty and garbage
            {{std::string("")}, Null()},
            {{std::string("not-a-date")}, Null()},
            // Invalid date values
            {{std::string("2024-13-01")}, Null()},
            {{std::string("2024-01-32")}, Null()},
            // Non-leap year Feb 29
            {{std::string("2023-02-29")}, Null()},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, string_leading_trailing_whitespace) {
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("  2024-01-15 14:30:45  ")}, std::string("2024-01-15 14:30:45")},
            {{std::string("\t2024-01-15\t")}, Null()}, // tab after date part is not T or space
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

// ==================== Integer input tests ====================

TEST(VTimestampSparkTest, int_bigint_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
    DataSet data_set = {
            {{int64_t(0)}, std::string("1970-01-01 08:00:00")},
            {{int64_t(1705308045)}, std::string("2024-01-15 16:40:45")},
            {{int64_t(1000000000)}, std::string("2001-09-09 09:46:40")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, int_negative_returns_null) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
    DataSet data_set = {
            {{int64_t(-1)}, Null()},
            {{int64_t(-100)}, Null()},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, int_tinyint_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_TINYINT};
    DataSet data_set = {
            {{int8_t(0)}, std::string("1970-01-01 08:00:00")},
            {{int8_t(100)}, std::string("1970-01-01 08:01:40")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, int_smallint_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_SMALLINT};
    DataSet data_set = {
            {{int16_t(0)}, std::string("1970-01-01 08:00:00")},
            {{int16_t(3600)}, std::string("1970-01-01 09:00:00")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, int_int32_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_INT};
    DataSet data_set = {
            {{int32_t(0)}, std::string("1970-01-01 08:00:00")},
            {{int32_t(86400)}, std::string("1970-01-02 08:00:00")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

// ==================== Float/Double input tests ====================

TEST(VTimestampSparkTest, double_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
    DataSet data_set = {
            {{double(0.0)}, std::string("1970-01-01 08:00:00.000000")},
            {{double(1705308045.5)}, std::string("2024-01-15 16:40:45.500000")},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

TEST(VTimestampSparkTest, double_negative_returns_null) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
    DataSet data_set = {
            {{double(-1.0)}, Null()},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

TEST(VTimestampSparkTest, float_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {PrimitiveType::TYPE_FLOAT};
    DataSet data_set = {
            {{float(0.0f)}, std::string("1970-01-01 08:00:00.000000")},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

// ==================== Decimal input tests ====================

TEST(VTimestampSparkTest, decimal64_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 6, 18}};
    DataSet data_set = {
            {{DECIMAL64(1705308045, 999999, 6)}, std::string("2024-01-15 16:40:45.999999")},
            {{DECIMAL64(1000000000, 0, 6)}, std::string("2001-09-09 09:46:40.000000")},
            {{DECIMAL64(0, 0, 6)}, std::string("1970-01-01 08:00:00.000000")},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

TEST(VTimestampSparkTest, decimal128_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL128I, 6, 38}};
    DataSet data_set = {
            {{DECIMAL128V3(int128_t(1705308045), int128_t(999999), 6)},
             std::string("2024-01-15 16:40:45.999999")},
    };
    static_cast<void>(
            check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 6));
}

// ==================== NULL handling tests ====================

TEST(VTimestampSparkTest, nullable_string_input) {
    std::string func_name = "timestamp_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR}};
    DataSet data_set = {
            {{std::string("2024-01-15 14:30:45")}, std::string("2024-01-15 14:30:45")},
            {{Null()}, Null()},
            {{std::string("2024-06-01")}, std::string("2024-06-01 00:00:00")},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampSparkTest, nullable_int_input) {
    std::string func_name = "timestamp_spark";
    TimezoneUtils::load_timezones_to_cache();
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_BIGINT}};
    DataSet data_set = {
            {{int64_t(1705308045)}, std::string("2024-01-15 16:40:45")},
            {{Null()}, Null()},
    };
    static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
