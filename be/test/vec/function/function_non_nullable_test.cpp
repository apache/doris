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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "function_test_util.h"
#include "testutil/column_helper.h"
#include "util/encryption_util.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(function_non_nullable_test, test) {
    ColumnsWithTypeAndName argument_template;
    argument_template.emplace_back(
            nullptr, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            "col_nullable_int32");

    auto function = SimpleFunctionFactory::instance().get_function(
            "non_nullable", argument_template,
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), {},
            BeExecVersionManager::get_newest_version());

    EXPECT_NE(function, nullptr);

    Block block;

    // prepare input column
    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3}, {0, 1, 0});

        block.insert({nullptr,
                      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                      "col_nullable_int32"});

        auto st = function->execute(nullptr, block, {0}, 1, 3);

        std::cout << st.to_string() << std::endl;
        EXPECT_EQ(st.to_string(),
                  "[INVALID_ARGUMENT]There's NULL value in column column which is illegal for "
                  "non_nullable , null map: 0,1,0,");
        EXPECT_FALSE(st.ok());
    }
}

} // namespace doris::vectorized