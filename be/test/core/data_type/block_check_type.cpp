
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

#include <cmath>
#include <limits>

#include "core/block/block.h"
#include "core/column/column_const.h"
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

#ifndef NDEBUG
TEST(BlockCheckType, InjectDebugNullablePayload) {
    auto original = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4}, {1, 1, 0, 0});
    Block block {{original, make_nullable(std::make_shared<DataTypeInt32>()), "nullable_int"}};

    ASSERT_TRUE(block.check_type_and_column().ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& values = assert_cast<const ColumnInt32&>(nullable.get_nested_column()).get_data();
    EXPECT_EQ(values[0], std::numeric_limits<int32_t>::lowest());
    EXPECT_EQ(values[1], std::numeric_limits<int32_t>::max());
    EXPECT_EQ(values[2], 3);
    EXPECT_EQ(values[3], 4);

    const auto& original_nullable = assert_cast<const ColumnNullable&>(*original);
    const auto& original_values =
            assert_cast<const ColumnInt32&>(original_nullable.get_nested_column()).get_data();
    EXPECT_EQ(original_values[0], 1);
    EXPECT_EQ(original_values[1], 2);
}

TEST(BlockCheckType, InjectDebugNullableBooleanAndFloatPayload) {
    Block block {{ColumnHelper::create_nullable_column<DataTypeUInt8>({0, 1}, {1, 0}),
                  make_nullable(std::make_shared<DataTypeUInt8>()), "nullable_bool"},
                 {ColumnHelper::create_nullable_column<DataTypeFloat64>({0.0, 1.0}, {1, 0}),
                  make_nullable(std::make_shared<DataTypeFloat64>()), "nullable_double"}};

    ASSERT_TRUE(block.check_type_and_column().ok());

    const auto& nullable_bool =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& bool_values =
            assert_cast<const ColumnUInt8&>(nullable_bool.get_nested_column()).get_data();
    EXPECT_EQ(bool_values[0], 0xA5);
    EXPECT_EQ(bool_values[1], 1);

    const auto& nullable_double =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& double_values =
            assert_cast<const ColumnFloat64&>(nullable_double.get_nested_column()).get_data();
    EXPECT_TRUE(std::isnan(double_values[0]));
    EXPECT_EQ(double_values[1], 1.0);
}

TEST(BlockCheckType, InjectDebugNullablePayloadThroughConstColumn) {
    auto nullable = ColumnHelper::create_nullable_column<DataTypeInt32>({0}, {1});
    Block block {{ColumnConst::create(std::move(nullable), 3),
                  make_nullable(std::make_shared<DataTypeInt32>()), "const_null"}};

    ASSERT_TRUE(block.check_type_and_column().ok());

    const auto& column_const = assert_cast<const ColumnConst&>(*block.get_by_position(0).column);
    const auto& nullable_data = assert_cast<const ColumnNullable&>(column_const.get_data_column());
    const auto& values =
            assert_cast<const ColumnInt32&>(nullable_data.get_nested_column()).get_data();
    EXPECT_EQ(values[0], std::numeric_limits<int32_t>::lowest());
}
#endif
} // namespace doris
