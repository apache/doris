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

#include "vec/data_types/data_type_timestamptz.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>

#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_varbinary.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

class DataTypeTimeStampTzTest : public ::testing::Test {
public:
    void SetUp() override {
        type = std::make_shared<DataTypeTimeStampTz>();
        serder = type->get_serde();
    }

    std::shared_ptr<IDataType> type;
    DataTypeSerDeSPtr serder;
};

TEST_F(DataTypeTimeStampTzTest, test_normal) {
    EXPECT_EQ(type->get_family_name(), "TimeStampTz");
    EXPECT_EQ(type->get_primitive_type(), PrimitiveType::TYPE_TIMESTAMPTZ);
    EXPECT_EQ(type->get_scale(), 0);

    auto other_type = std::make_shared<DataTypeTimeStampTz>();
    EXPECT_TRUE(type->equals(*other_type));
    EXPECT_TRUE(type->equals_ignore_precision(*other_type));
}

TEST_F(DataTypeTimeStampTzTest, test_serder) {
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(8));
    TimezoneUtils::load_offsets_to_cache();

    DataTypeSerDe::FormatOptions options;
    options.timezone = &time_zone;

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:00:00 +08:00"};
        EXPECT_TRUE(serder->from_string(str, *column, options).ok());
        EXPECT_EQ(column->size(), 1);
        auto& col_data = assert_cast<ColumnTimeStampTz&>(*column);
        EXPECT_EQ(col_data.get_data()[0], 142430890880925696);
    }

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:abc:00 +08:00"};
        EXPECT_FALSE(serder->from_string(str, *column, options).ok());
    }

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:00:00 +08:00"};
        EXPECT_TRUE(serder->from_string_strict_mode(str, *column, options).ok());
        EXPECT_EQ(column->size(), 1);
        auto& col_data = assert_cast<ColumnTimeStampTz&>(*column);
        EXPECT_EQ(col_data.get_data()[0], 142430890880925696);
    }

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:abc:00 +08:00"};
        EXPECT_FALSE(serder->from_string_strict_mode(str, *column, options).ok());
    }
}

} // namespace doris::vectorized