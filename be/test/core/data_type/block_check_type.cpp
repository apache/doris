
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

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "testutil/column_helper.h"

namespace doris {
TEST(BlockCheckType, test1) {
    auto block = Block {
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4}),
    };

    auto st = block.check_type_and_column();
    EXPECT_TRUE(st);

    block.get_by_position(1).column =
            ColumnHelper::create_column<DataTypeFloat64>({1.1, 2.2, 3.3, 4.4});
    st = block.check_type_and_column();
    EXPECT_FALSE(st.ok());
    std::cout << st.msg() << std::endl;
}

TEST(BlockCheckType, CheckColumnAndTypeNotNull) {
    auto block = Block {
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4}),
    };

    EXPECT_TRUE(block.check_column_and_type_not_null());

    block.get_by_position(0).column = nullptr;
    auto st = block.check_column_and_type_not_null();
    EXPECT_FALSE(st.ok());

    block.get_by_position(0).column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});
    block.get_by_position(1).type = nullptr;
    st = block.check_column_and_type_not_null();
    EXPECT_FALSE(st.ok());
}

TEST(BlockCheckType, CheckNoColumnString64) {
    Block block {{ColumnString::create(), std::make_shared<DataTypeString>(), "string"}};
    EXPECT_TRUE(block.check_no_column_string64());

    block.get_by_position(0).column = ColumnString64::create();
    auto st = block.check_no_column_string64();
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.msg().find("column index: 0, name: string"), std::string::npos);

    block.get_by_position(0).column =
            ColumnNullable::create(ColumnString64::create(), ColumnUInt8::create());
    block.get_by_position(0).type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    st = block.check_no_column_string64();
    EXPECT_FALSE(st.ok());
}
} // namespace doris
