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

#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_string.h"

namespace doris::vectorized {
TEST(SubReplaceTest, test) {
    const int rows = 10240;
    auto str = ColumnString::create();
    auto new_str = ColumnString::create();
    auto start = ColumnInt32::create();
    auto length = ColumnInt32::create();

    for (int i = 0; i < rows; i++) {
        str->insert_default();
        new_str->insert_default();
        start->insert_default();
        length->insert_default();
    }

    Block block {
            ColumnWithTypeAndName {std::move(str), std::make_shared<DataTypeString>(), "str"},
            ColumnWithTypeAndName {std::move(new_str), std::make_shared<DataTypeString>(),
                                   "new_str"},
            ColumnWithTypeAndName {std::move(start), std::make_shared<DataTypeInt32>(), "start"},
            ColumnWithTypeAndName {std::move(length), std::make_shared<DataTypeInt32>(), "length"},
            ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeInt32>(), "res"},
    };

    EXPECT_TRUE(SubReplaceImpl::replace_execute(block, ColumnNumbers {0, 1, 2, 3}, 4, rows));
}
} // namespace doris::vectorized
