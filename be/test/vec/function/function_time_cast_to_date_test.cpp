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

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>
#include <vec/runtime/time_value.h>

#include <memory>

#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function_cast.h"

namespace doris::vectorized {

struct MockFunctionCastFromTimeToDateName {
    static constexpr auto name = "FunctionCastFromTimeToDate";
};

struct FunctionCastFromTimeToDate : public ::testing::Test {
    void SetUp() override {
        // 2025-3-25 11:45:14
        state._timestamp_ms = 1742874314000;
        context._state = &state;
        to_type = nullptr;
        from_data.clear();
        expected.clear();
    }

    template <typename ToData>
    void test() {
        using Convert =
                ConvertImplToTimeType<DataTypeTimeV2, ToData, MockFunctionCastFromTimeToDateName>;

        Block block = ColumnHelper::create_block<DataTypeTimeV2>(from_data);
        block.insert(ColumnWithTypeAndName {nullptr, to_type, "to"});
        auto st = Convert::execute(&context, block, ColumnNumbers {0}, 1, from_data.size());

        EXPECT_TRUE(st) << st.ok();
        for (int i = 0; i < expected.size(); i++) {
            auto column = block.get_by_position(1).column;
            EXPECT_EQ(to_type->to_string(*column, i), expected[i])
                    << " time value : " << DataTypeTimeV2 {}.to_string(from_data[i]);
        }
    }

    void add_case(TimeValue::TimeType time, std::string str) {
        from_data.push_back(time);
        expected.push_back(str);
    }

    using FromType = DataTypeDateTimeV2;
    MockRuntimeState state;
    FunctionContext context;

    DataTypePtr to_type;
    std::vector<TimeValue::TimeType> from_data;
    std::vector<std::string> expected;
};

TEST_F(FunctionCastFromTimeToDate, time_to_datetimev2_6) {
    to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6));
    add_case(TimeValue::make_time(1, 2, 3, 123456), "2025-03-25 01:02:03.123456");
    add_case(TimeValue::make_time(1, 2, 3, 654321), "2025-03-25 01:02:03.654321");
    add_case(-TimeValue::make_time(1, 2, 3, 123456), "2025-03-24 22:57:56.876544");
    add_case(-TimeValue::make_time(1, 2, 3, 654321), "2025-03-24 22:57:56.345679");
    test<DataTypeDateTimeV2>();
}

TEST_F(FunctionCastFromTimeToDate, time_to_datetimev2_3) {
    to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(3));
    add_case(TimeValue::make_time(1, 2, 3, 123500), "2025-03-25 01:02:03.124");
    add_case(TimeValue::make_time(1, 2, 59, 999500), "2025-03-25 01:03:00.000");
    add_case(TimeValue::make_time(1, 2, 3, 123499), "2025-03-25 01:02:03.123");
    add_case(-TimeValue::make_time(1, 2, 3, 123456), "2025-03-24 22:57:56.877");
    add_case(-TimeValue::make_time(1, 2, 3, 654321), "2025-03-24 22:57:56.346");
    test<DataTypeDateTimeV2>();
}

TEST_F(FunctionCastFromTimeToDate, time_to_datetimev2_0) {
    to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(0));
    add_case(TimeValue::make_time(1, 2, 3, 123500), "2025-03-25 01:02:03");
    add_case(TimeValue::make_time(1, 2, 59, 999500), "2025-03-25 01:03:00");
    add_case(TimeValue::make_time(1, 2, 3, 123499), "2025-03-25 01:02:03");
    add_case(-TimeValue::make_time(1, 2, 3, 123456), "2025-03-24 22:57:57");
    add_case(-TimeValue::make_time(1, 2, 3, 654321), "2025-03-24 22:57:56");
    test<DataTypeDateTimeV2>();
}

TEST_F(FunctionCastFromTimeToDate, time_to_datetimev1_0) {
    to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>());
    add_case(TimeValue::make_time(1, 2, 3, 123500), "2025-03-25 01:02:03");
    add_case(TimeValue::make_time(1, 2, 59, 999500), "2025-03-25 01:03:00");
    add_case(TimeValue::make_time(1, 2, 3, 123499), "2025-03-25 01:02:03");
    add_case(-TimeValue::make_time(1, 2, 3, 123456), "2025-03-24 22:57:57");
    add_case(-TimeValue::make_time(1, 2, 3, 654321), "2025-03-24 22:57:56");
    test<DataTypeDateTime>();
}

TEST_F(FunctionCastFromTimeToDate, time_to_datetv2) {
    to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateV2>());
    add_case(TimeValue::make_time(1, 2, 3, 123500), "2025-03-25");
    add_case(TimeValue::make_time(1, 2, 59, 999500), "2025-03-25");
    add_case(TimeValue::make_time(1, 2, 3, 123499), "2025-03-25");
    add_case(-TimeValue::make_time(1, 2, 3, 123456), "2025-03-24");
    add_case(-TimeValue::make_time(1, 2, 3, 654321), "2025-03-24");
    test<DataTypeDateV2>();
}

TEST_F(FunctionCastFromTimeToDate, time_to_datetv1) {
    to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDate>());
    add_case(TimeValue::make_time(1, 2, 3, 123500), "2025-03-25");
    add_case(TimeValue::make_time(1, 2, 59, 999500), "2025-03-25");
    add_case(TimeValue::make_time(1, 2, 3, 123499), "2025-03-25");
    add_case(-TimeValue::make_time(1, 2, 3, 123456), "2025-03-24");
    add_case(-TimeValue::make_time(1, 2, 3, 654321), "2025-03-24");
    test<DataTypeDate>();
}
} // namespace doris::vectorized