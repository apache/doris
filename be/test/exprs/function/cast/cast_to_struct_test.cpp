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
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_test.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "testutil/column_helper.h"

namespace doris {
using namespace ut_type;

template <typename... DataType>
struct ColumnStructBuilder {
    void add(const typename ColumnBuilder<DataType>::Ty... ty) {
        std::apply(
                [&](auto&... col) {
                    // 展开调用每个 ColumnBuilder 的 add
                    (col.add(ty), ...);
                },
                columns);
        null_map.push_back(0);
    }

    void add_null() {
        std::apply(
                [&](auto&... col) {
                    // 展开调用每个 ColumnBuilder 的 add_null
                    (col.add(NullTag {}), ...);
                },
                columns);
        null_map.push_back(1);
    }

    ColumnWithTypeAndName build(Strings names) {
        std::vector<ColumnWithTypeAndName> nested_column;
        std::apply(
                [&](auto&... col) {
                    // 展开调用每个 ColumnBuilder 的 build
                    (nested_column.push_back(col.build()), ...);
                },
                columns);

        DataTypes data_types;
        Columns data_columns;
        for (const auto& col : nested_column) {
            data_types.push_back(col.type);
        }
        for (const auto& col : nested_column) {
            data_columns.push_back(col.column);
        }

        auto col_struct = ColumnStruct::create(std::move(data_columns));
        auto col_null_map = ColumnHelper::create_column<DataTypeUInt8>(null_map);
        auto col_nullable = ColumnNullable::create(std::move(col_struct), col_null_map);

        auto data_type = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::move(data_types), std::move(names)));

        return ColumnWithTypeAndName(std::move(col_nullable), data_type, "column_struct");
    }

    std::tuple<ColumnBuilder<DataType>...> columns;
    std::vector<DataTypeUInt8::FieldType> null_map;
};

TEST_F(FunctionCastTest, test_from_string_to_struct1) {
    ColumnStructBuilder<DataTypeInt32, DataTypeString, DataTypeInt32> builder;
    std::vector<std::string> from_str;

    from_str.push_back(R"({123,"abc", 456})");
    builder.add(123, "abc", 456);

    from_str.push_back(R"({789,"def", 101112})");
    builder.add(789, "def", 101112);

    from_str.push_back(R"(  {}   )");
    builder.add_null();

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build({"a", "b", "c"}), false);
}

TEST_F(FunctionCastTest, test_from_string_to_struct2) {
    ColumnStructBuilder<DataTypeInt32, DataTypeBool, DataTypeInt32, DataTypeBool> builder;
    std::vector<std::string> from_str;
    from_str.push_back(R"({123,true, 456, false})");
    builder.add(123, true, 456, false);
    from_str.push_back(R"({789,false, 101112, true})");
    builder.add(789, false, 101112, true);
    from_str.push_back(R"(  {}   )");
    builder.add_null();

    from_str.push_back(R"({789,abc, 101112, true})");
    builder.add(789, NullTag {}, 101112, true);

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build({"a", "b", "c", "d"}), false);
}

// A field of a row that the input null map of a STRUCT marks as NULL may still keep a hidden payload
// (for example the branch of an IF() that was not taken), and the child cast of a nullable field
// reads its NULL state from the field column, so the mask of the row has to be inherited by it.
TEST_F(FunctionCastTest, test_cast_struct_null_row_skips_hidden_field_payload) {
    auto from_field_column = ColumnHelper::create_nullable_column<DataTypeInt32>({128, 1}, {0, 0});
    auto from_struct = ColumnStruct::create(Columns {from_field_column});
    ColumnPtr from_column = ColumnNullable::create(
            std::move(from_struct), ColumnHelper::create_column<DataTypeUInt8>({1, 0}));

    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())}));
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>())}));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    // Row 0 is NULL and keeps the hidden payload 128, which does not fit into TINYINT.
    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(result.is_null_at(0));
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), R"({"1":1})");
}

// An out of range field of a visible row still fails.
TEST_F(FunctionCastTest, test_cast_struct_visible_field_still_fails) {
    auto from_field_column =
            ColumnHelper::create_nullable_column<DataTypeInt32>({128, 300}, {0, 0});
    auto from_struct = ColumnStruct::create(Columns {from_field_column});
    ColumnPtr from_column = ColumnNullable::create(
            std::move(from_struct), ColumnHelper::create_column<DataTypeUInt8>({1, 0}));

    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())}));
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>())}));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    EXPECT_FALSE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
}

// A field whose type does not change is passed through, while the fields that do change still
// inherit the NULL of their row, so the hidden payload of the changed field is not validated.
TEST_F(FunctionCastTest, test_cast_struct_unchanged_field_with_null_row) {
    auto first_column = ColumnHelper::create_nullable_column<DataTypeInt32>({5, 1}, {0, 0});
    auto second_column = ColumnHelper::create_nullable_column<DataTypeInt32>({128, 2}, {0, 0});
    auto from_struct = ColumnStruct::create(Columns {first_column, second_column});
    ColumnPtr from_column = ColumnNullable::create(
            std::move(from_struct), ColumnHelper::create_column<DataTypeUInt8>({1, 0}));

    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())},
            Strings {"f1", "f2"}));
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>())},
            Strings {"f1", "f2"}));

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
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), R"({"f1":1, "f2":2})");
}

TEST_F(FunctionCastTest, StringToComplexCastChargesScratchBuffer) {
    constexpr size_t text_size = 9 * 1024 * 1024;
    auto from_type = std::make_shared<DataTypeString>();
    auto to_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>())));
    for (const bool strict : {false, true}) {
        SCOPED_TRACE(strict);
        auto ctx = create_context(strict);
        auto source = ColumnHelper::create_column<DataTypeString>({std::string(text_size, 'x')});
        Block block {{source, from_type, "from"}, {nullptr, to_type, "to"}};
        auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
        ASSERT_NE(fn, nullptr);
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "ComplexCastScratch");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        auto status = fn(ctx.get(), block, {0}, 1, 1, nullptr);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(status.ok(), !strict) << status;
        // The invalid first character produces no large output. The peak must still include the
        // entire owned scratch copy, and that copy must be released on success and error alike.
        EXPECT_GE(tracker->peak_consumption() - baseline, text_size);
        EXPECT_LT(tracker->consumption() - baseline, 1024 * 1024);
    }
}

TEST_F(FunctionCastTest, StringToComplexCastKeepsSourceText) {
    // Case: a load mapping such as `columns: k, s, s = replace_value(w, NULL), w` casts the source
    // column w to the STRUCT type while the same row also imports w. The nested string SerDe
    // unescapes escaped characters in place, so parsing the borrowed StringRef of w would rewrite
    // the bytes that are written into w afterwards. The cast must parse an owned copy.
    auto from_type = std::make_shared<DataTypeString>();
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())));
    // The escaped quote is removed in place by escape_string: ["a\"b"] would become ["a"bb"],
    // the length drops by one but the tail bytes of the callers buffer stay overwritten.
    const std::string text = R"(["a\"b"])";

    for (const bool strict : {false, true}) {
        SCOPED_TRACE(strict ? "strict" : "non-strict");
        auto ctx = create_context(strict);
        auto from_column = ColumnHelper::create_column<DataTypeString>({text});
        const auto* source = assert_cast<const ColumnString*>(from_column.get());
        const StringRef before = source->get_data_at(0);
        const std::string expected(before.data, before.size);

        Block block = {
                {from_column, from_type, "from"},
                {nullptr, to_type, "to"},
        };
        auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
        ASSERT_TRUE(fn != nullptr);
        ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

        const StringRef after = source->get_data_at(0);
        EXPECT_EQ(expected, std::string(after.data, after.size));
        // the cast still produced the parsed array; the display text is not compared because it
        // does not escape the quote again
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        ASSERT_FALSE(result.is_null_at(0));
        const auto& array = assert_cast<const ColumnArray&>(result.get_nested_column());
        const auto& elements = assert_cast<const ColumnNullable&>(array.get_data());
        ASSERT_EQ(1, elements.size());
        ASSERT_FALSE(elements.is_null_at(0));
        EXPECT_EQ(R"(a"b)", elements.get_nested_column().get_data_at(0).to_string());
    }
}
} // namespace doris
