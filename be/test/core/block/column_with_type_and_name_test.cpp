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

#include "core/block/column_with_type_and_name.h"

#include <gtest/gtest.h>

#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"
#include "testutil/column_helper.h"

namespace doris {

TEST(ColumnWithTypeAndNameTest, get_nested_test) {
    ColumnWithTypeAndName column_with_type_and_name;
    auto null_column = ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                              ColumnHelper::create_column<DataTypeUInt8>({true}));
    column_with_type_and_name.column = ColumnConst::create(std::move(null_column), 3);
    column_with_type_and_name.type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    column_with_type_and_name.name = "column_with_type_and_name";
    auto result = column_with_type_and_name.unnest_nullable(
            column_with_type_and_name.get_nullable_column_info(), true);
    EXPECT_TRUE(is_column_const(*result.column));
    EXPECT_EQ(result.column->size(), 3);
    EXPECT_EQ(result.column->get_int(0), 0);
}

TEST(ColumnWithTypeAndNameTest, get_nullable_column_info_for_const_column) {
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    auto null_column = ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                              ColumnHelper::create_column<DataTypeUInt8>({true}));
    ColumnWithTypeAndName const_null {ColumnConst::create(std::move(null_column), 3), nullable_type,
                                      "const_null"};
    auto null_info = const_null.get_nullable_column_info();
    EXPECT_TRUE(null_info.is_const);
    EXPECT_TRUE(null_info.has_null);
    EXPECT_TRUE(null_info.only_null);
    EXPECT_EQ(const_null.get_nullable_null_map_column()->size(), 1);

    auto non_null_column =
            ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                   ColumnHelper::create_column<DataTypeUInt8>({false}));
    ColumnWithTypeAndName const_non_null {ColumnConst::create(std::move(non_null_column), 3),
                                          nullable_type, "const_non_null"};
    auto non_null_info = const_non_null.get_nullable_column_info();
    EXPECT_TRUE(non_null_info.is_const);
    EXPECT_FALSE(non_null_info.has_null);
    EXPECT_FALSE(non_null_info.only_null);
    EXPECT_EQ(const_non_null.get_nullable_null_map_column()->size(), 1);
}

TEST(ColumnWithTypeAndNameTest, get_nullable_column_info_null_map_states) {
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    const auto check_state = [&](std::initializer_list<int32_t> values,
                                 std::initializer_list<uint8_t> null_map, bool has_null,
                                 bool only_null) {
        ColumnWithTypeAndName column {
                ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>(values),
                                       ColumnHelper::create_column<DataTypeUInt8>(null_map)),
                nullable_type, "nullable"};
        const auto info = column.get_nullable_column_info();
        EXPECT_EQ(info.has_null, has_null);
        EXPECT_EQ(info.only_null, only_null);
    };

    check_state({}, {}, false, true);
    check_state({1, 2, 3}, {false, false, false}, false, false);
    check_state({1, 2, 3}, {true, true, true}, true, true);
    check_state({1, 2, 3}, {false, true, false}, true, false);
    check_state({1, 2, 3}, {true, false, true}, true, false);
}

TEST(ColumnWithTypeAndNameTest, unnest_nullable_without_null_reuses_nested_column) {
    auto nested_column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto nullable_column = ColumnNullable::create(
            nested_column, ColumnHelper::create_column<DataTypeUInt8>({false, false, false}));
    ColumnWithTypeAndName column_with_type_and_name {
            std::move(nullable_column),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "nullable"};

    auto result = column_with_type_and_name.unnest_nullable(
            column_with_type_and_name.get_nullable_column_info(), true);

    EXPECT_EQ(result.column.get(), nested_column.get());
}

TEST(ColumnWithTypeAndNameTest, unnest_nullable_with_unique_nested_replaces_data_in_place) {
    auto nullable_column = ColumnNullable::create(
            ColumnHelper::create_column<DataTypeInt32>({1, 2, 3}),
            ColumnHelper::create_column<DataTypeUInt8>({false, true, false}));
    const auto* original_nested_column =
            static_cast<const ColumnNullable&>(*nullable_column).get_nested_column_ptr().get();
    ColumnWithTypeAndName column_with_type_and_name {
            std::move(nullable_column),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "nullable"};

    const auto info = column_with_type_and_name.get_nullable_column_info();
    auto result = column_with_type_and_name.unnest_nullable(info, true);

    EXPECT_EQ(result.column.get(), original_nested_column);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*result.column).get_data()[1], 0);
}

TEST(ColumnWithTypeAndNameTest, unnest_nullable_with_shared_nested_preserves_visible_alias) {
    auto nested_column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto nullable_column = ColumnNullable::create(
            nested_column, ColumnHelper::create_column<DataTypeUInt8>({false, true, false}));
    auto visible_alias = ColumnNullable::create(
            nested_column, ColumnHelper::create_column<DataTypeUInt8>({false, false, false}));
    ColumnWithTypeAndName column_with_type_and_name {
            std::move(nullable_column),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "nullable"};

    const auto info = column_with_type_and_name.get_nullable_column_info();
    auto result = column_with_type_and_name.unnest_nullable(info, true);

    EXPECT_NE(result.column.get(), nested_column.get());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*result.column).get_data()[1], 0);
    EXPECT_FALSE(visible_alias->is_null_at(1));
    const ColumnNullable& visible_alias_column = *visible_alias;
    EXPECT_EQ(
            assert_cast<const ColumnInt32&>(visible_alias_column.get_nested_column()).get_data()[1],
            2);
}

TEST(ColumnWithTypeAndNameTest, unnest_nullable_with_shared_source_replaces_data_on_copy) {
    auto nullable_column = ColumnNullable::create(
            ColumnHelper::create_column<DataTypeInt32>({1, 2, 3}),
            ColumnHelper::create_column<DataTypeUInt8>({false, true, false}));
    ColumnWithTypeAndName column_with_type_and_name {
            std::move(nullable_column),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "nullable"};
    ColumnPtr source_alias = column_with_type_and_name.column;
    const auto& original_nested_column =
            assert_cast<const ColumnNullable&>(*source_alias).get_nested_column();

    const auto info = column_with_type_and_name.get_nullable_column_info();
    auto result = column_with_type_and_name.unnest_nullable(info, true);

    EXPECT_NE(result.column.get(), &original_nested_column);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*result.column).get_data()[1], 0);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(original_nested_column).get_data()[1], 2);
}

TEST(ColumnWithTypeAndNameTest, unnest_const_nullable_with_shared_source_replaces_data_on_copy) {
    auto nullable_column =
            ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                   ColumnHelper::create_column<DataTypeUInt8>({true}));
    ColumnWithTypeAndName column_with_type_and_name {
            ColumnConst::create(std::move(nullable_column), 3),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "nullable"};
    ColumnPtr source_alias = column_with_type_and_name.column;
    const auto& original_nullable_column = assert_cast<const ColumnNullable&>(
            assert_cast<const ColumnConst&>(*source_alias).get_data_column());

    const auto info = column_with_type_and_name.get_nullable_column_info();
    auto result = column_with_type_and_name.unnest_nullable(info, true);

    EXPECT_TRUE(is_column_const(*result.column));
    EXPECT_EQ(result.column->get_int(0), 0);
    EXPECT_EQ(original_nullable_column.get_nested_column().get_int(0), 1);
}

} // namespace doris
