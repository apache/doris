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
#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_map.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_test.h"
#include "testutil/column_helper.h"

namespace doris {
using namespace ut_type;

template <typename KeyDataType, typename ValueDataType>
struct ColumnMapBuilder {
    using Ty = std::pair<std::variant<NullTag, typename KeyDataType::FieldType>,
                         std::variant<NullTag, typename ValueDataType::FieldType>>;

    void add(const std::vector<Ty>& values) {
        for (const auto& kv : values) {
            auto key = kv.first;
            auto value = kv.second;
            if (std::holds_alternative<typename KeyDataType::FieldType>(key)) {
                key_data.push_back(std::get<typename KeyDataType::FieldType>(key));
                key_null_map.push_back(0);
            } else {
                key_data.push_back(typename KeyDataType::FieldType {});
                key_null_map.push_back(1);
            }

            if (std::holds_alternative<typename ValueDataType::FieldType>(value)) {
                value_data.push_back(std::get<typename ValueDataType::FieldType>(value));
                value_null_map.push_back(0);
            } else {
                value_data.push_back(typename ValueDataType::FieldType {});
                value_null_map.push_back(1);
            }
            size++;
        }
        offsets.push_back(size);
        null_map.push_back(0);
    }

    void add_null() {
        null_map.push_back(1);
        offsets.push_back(size);
    }

    ColumnWithTypeAndName build() {
        auto key_column = ColumnHelper::create_nullable_column<KeyDataType>(key_data, key_null_map);
        auto value_column =
                ColumnHelper::create_nullable_column<ValueDataType>(value_data, value_null_map);
        auto offsets_column = ColumnHelper::create_column_offsets<TYPE_UINT64>(offsets);

        auto col_map = ColumnMap::create(std::move(key_column), std::move(value_column),
                                         std::move(offsets_column));
        auto col_null_map = ColumnHelper::create_column<DataTypeUInt8>(null_map);
        auto col_nullable = ColumnNullable::create(std::move(col_map), col_null_map);
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeNullable>(std::make_shared<KeyDataType>()),
                std::make_shared<DataTypeNullable>(std::make_shared<ValueDataType>())));

        return ColumnWithTypeAndName(std::move(col_nullable), data_type, "column");
    }

    std::vector<typename KeyDataType::FieldType> key_data;
    std::vector<typename ValueDataType::FieldType> value_data;
    std::vector<uint8_t> key_null_map;
    std::vector<uint8_t> value_null_map;
    std::vector<uint8_t> null_map;
    std::vector<uint64_t> offsets;
    int size = 0;
};

TEST_F(FunctionCastTest, test_from_string_to_map_int_string) {
    ColumnMapBuilder<DataTypeInt32, DataTypeString> builder;
    std::vector<std::string> from_str;

    from_str.push_back(R"({123:456})");
    builder.add({{123, "456"}});

    from_str.push_back(R"({789:101112})");
    builder.add({{789, "101112"}});

    from_str.push_back(R"({1:2, 3:4})");
    builder.add({{1, "2"}, {3, "4"}});

    from_str.push_back(R"({abc:101112})");
    builder.add({{NullTag {}, "101112"}});

    from_str.push_back(R"(  {}  )");
    builder.add_null();

    from_str.push_back(R"({})");
    builder.add({});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_map_int_bool) {
    ColumnMapBuilder<DataTypeInt32, DataTypeBool> builder;
    std::vector<std::string> from_str;
    from_str.push_back(R"({123:true})");
    builder.add({{123, true}});

    from_str.push_back(R"({789:false})");
    builder.add({{789, false}});

    from_str.push_back(R"({1:true, 3:false})");
    builder.add({{1, true}, {3, false}});

    from_str.push_back(R"({abc:true})");
    builder.add({{NullTag {}, true}});

    from_str.push_back(R"(  {}  )");
    builder.add_null();

    from_str.push_back(R"({})");
    builder.add({});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_map_string_string) {
    ColumnMapBuilder<DataTypeString, DataTypeString> builder;
    std::vector<std::string> from_str;

    from_str.push_back(R"({"abc":"def"})");
    builder.add({{"abc", "def"}});

    from_str.push_back(R"({"ghi":"jkl"})");
    builder.add({{"ghi", "jkl"}});

    from_str.push_back(R"({"mno":"pqr", "stu":"vwx"})");
    builder.add({{"mno", "pqr"}, {"stu", "vwx"}});

    from_str.push_back(R"({"xyz":"123"})");
    builder.add({{"xyz", "123"}});

    from_str.push_back(R"(  {}  )");
    builder.add_null();

    from_str.push_back(R"({})");
    builder.add({});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

// A row that the input null map of a MAP marks as NULL may still keep a hidden payload in the
// entries that belong to it (for example the branch of an IF() that was not taken). Keys and values
// are stored flattened, so the mask of the row has to be expanded to its entries: applying the row
// aligned mask to the entry indexes would check the wrong entries and read behind the mask.
TEST_F(FunctionCastTest, test_cast_map_null_row_skips_hidden_entry_payload) {
    auto from_key_column =
            ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 128}, {0, 0, 0});
    auto from_value_column =
            ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 300}, {0, 0, 0});
    auto from_map = ColumnMap::create(from_key_column, from_value_column,
                                      ColumnHelper::create_column_offsets<TYPE_UINT64>({2, 3}));
    ColumnPtr from_column = ColumnNullable::create(
            std::move(from_map), ColumnHelper::create_column<DataTypeUInt8>({0, 1}));

    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())));
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>())));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    // Row 0 is {1: 10, 2: 20}, row 1 is NULL and keeps the hidden entry {128: 300}.
    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_FALSE(result.is_null_at(0));
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 0), "{1:10, 2:20}");
    EXPECT_TRUE(result.is_null_at(1));
}

// The hidden entry of a visible row is still validated.
TEST_F(FunctionCastTest, test_cast_map_visible_entry_still_fails) {
    auto from_key_column = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 128}, {0, 0});
    auto from_value_column = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 300}, {0, 0});
    auto from_map = ColumnMap::create(from_key_column, from_value_column,
                                      ColumnHelper::create_column_offsets<TYPE_UINT64>({1, 2}));
    ColumnPtr from_column = ColumnNullable::create(
            std::move(from_map), ColumnHelper::create_column<DataTypeUInt8>({0, 0}));

    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())));
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>())));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    EXPECT_FALSE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
}

// A key whose type does not change is passed through, while the value that does change still
// inherits the NULL of its row, so the hidden payload of the value is not validated.
TEST_F(FunctionCastTest, test_cast_map_unchanged_key_with_null_row) {
    auto from_key_column = ColumnHelper::create_nullable_column<DataTypeInt32>({5, 1}, {0, 0});
    auto from_value_column = ColumnHelper::create_nullable_column<DataTypeInt32>({128, 2}, {0, 0});
    auto from_map = ColumnMap::create(from_key_column, from_value_column,
                                      ColumnHelper::create_column_offsets<TYPE_UINT64>({1, 2}));
    ColumnPtr from_column = ColumnNullable::create(
            std::move(from_map), ColumnHelper::create_column<DataTypeUInt8>({1, 0}));

    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())));
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>())));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(result.is_null_at(0));
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), "{1:2}");
}

} // namespace doris
