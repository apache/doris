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

#include "vec/functions/function_convert_tz.h"

#include <gtest/gtest.h>
#include <stdint.h>

#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "testutil/column_helper.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(FunctionConvertTZTest, test_no_open_but_execute_const) {
    FunctionConvertTZ<DataTypeDateV2> func;
    FunctionContext ctx;
    std::shared_ptr<ConvertTzState> state = std::make_shared<ConvertTzState>();
    state->use_state = false;
    ctx.set_function_state(FunctionContext::FRAGMENT_LOCAL, state);

    ColumnNumbers arguments {0, 1, 2};

    Block block {ColumnWithTypeAndName {ColumnHelper::create_column<DataTypeDateV2>({1}),
                                        std::make_shared<DataTypeDateV2>(), "date"},
                 ColumnWithTypeAndName {
                         ColumnConst::create(ColumnHelper::create_column<DataTypeString>({""}), 1),
                         std::make_shared<DataTypeString>(), "from_tz"},
                 ColumnWithTypeAndName {
                         ColumnConst::create(ColumnHelper::create_column<DataTypeString>({""}), 1),
                         std::make_shared<DataTypeString>(), "to_tz"},
                 ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeDateV2>(), "result"}};

    auto st = func.execute(&ctx, block, arguments, 3, 1);

    EXPECT_EQ(st.ok(), true) << st.msg();
}

} // namespace doris::vectorized
