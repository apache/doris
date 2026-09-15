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

#include <cstdint>
#include <memory>
#include <variant>
#include <vector>

#include "core/column/column_array.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_test.h"
#include "testutil/column_helper.h"

namespace doris {
using namespace ut_type;

template <typename DataType>
struct ColumnArrayBuilder {
    using FieldType = typename DataType::FieldType;
    using Ty = std::variant<FieldType, NullTag>;

    void add(const std::vector<Ty>& values) {
        for (const auto& value : values) {
            if (std::holds_alternative<FieldType>(value)) {
                nested_values.push_back(std::get<FieldType>(value));
                nested_null_map.push_back(false);
            } else {
                nested_values.push_back(FieldType {});
                nested_null_map.push_back(true);
            }
        }
        size += values.size();
        null_map.push_back(0);
        offsets.push_back(size);
    }

    ColumnWithTypeAndName build() {
        auto nested_column =
                ColumnHelper::create_nullable_column<DataType>(nested_values, nested_null_map);
        auto offsets_column = ColumnHelper::create_column_offsets<TYPE_UINT64>(offsets);
        auto col_array = ColumnArray::create(std::move(nested_column), offsets_column);
        auto col_null_map = ColumnHelper::create_column<DataTypeUInt8>(null_map);
        auto col_nullable = ColumnNullable::create(std::move(col_array), std::move(col_null_map));
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeNullable>(std::make_shared<DataType>())));

        return ColumnWithTypeAndName(std::move(col_nullable), data_type, "column");
    }

    void add_null() {
        null_map.push_back(1);
        offsets.push_back(size);
    }

    int size = 0;
    std::vector<FieldType> nested_values;
    std::vector<DataTypeUInt8::FieldType> nested_null_map;
    std::vector<uint64_t> offsets;
    std::vector<DataTypeUInt8::FieldType> null_map;
};

TEST_F(FunctionCastTest, test_from_string_to_array_int) {
    ColumnArrayBuilder<DataTypeInt32> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([123,456])");
    builder.add({123, 456});

    from_str.push_back(R"([789, 101112])");
    builder.add({789, 101112});

    from_str.push_back(R"([1, 2, 3])");
    builder.add({1, 2, 3});

    from_str.push_back(R"([abc, 101112])");
    builder.add({NullTag {}, 101112});

    from_str.push_back(R"([1, 2, 3, 4])");
    builder.add({1, 2, 3, 4});

    from_str.push_back(R"([)");
    builder.add_null();

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_array_double) {
    ColumnArrayBuilder<DataTypeFloat64> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([123.456, 789.101])");
    builder.add({123.456, 789.101});

    from_str.push_back(R"([1.2, 3.4, 5.6])");
    builder.add({1.2, 3.4, 5.6});

    from_str.push_back(R"(["1.2", '3.4', "5.6"])");
    builder.add({1.2, 3.4, 5.6});

    from_str.push_back(R"([abc, 101112])");
    builder.add({NullTag {}, 101112.0});

    from_str.push_back(R"([1.2, 3.4, 5.6, 7.8])");
    builder.add({1.2, 3.4, 5.6, 7.8});

    from_str.push_back(R"([)");
    builder.add_null();

    from_str.push_back(R"([])");
    builder.add({});

    from_str.push_back(R"([      ])");
    builder.add({NullTag {}});

    from_str.push_back(R"([123;123])");
    builder.add({NullTag {}});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_array_string) {
    ColumnArrayBuilder<DataTypeString> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([123,456])");
    builder.add({"123", "456"});

    from_str.push_back(R"([789, 101112])");
    builder.add({"789", "101112"});

    from_str.push_back(R"([1, 2, 3])");
    builder.add({"1", "2", "3"});

    from_str.push_back(R"([abc, 101112])");
    builder.add({"abc", "101112"});

    from_str.push_back(R"([1, 2, 3, 4])");
    builder.add({"1", "2", "3", "4"});

    from_str.push_back(R"([)");
    builder.add_null();

    from_str.push_back(R"([])");
    builder.add({});

    from_str.push_back(R"([      ])");
    builder.add({""});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_array_bool) {
    ColumnArrayBuilder<DataTypeBool> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([true, false])");
    builder.add({true, false});

    from_str.push_back(R"([1, 0])");
    builder.add({true, false});

    from_str.push_back(R"(['true', "0", "false"])");
    builder.add({true, false, false});

    from_str.push_back(R"([abc, 101112])");
    builder.add({NullTag {}, NullTag {}});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_null_literal_array_to_nested_array) {
    auto null_literal_type = std::make_shared<DataTypeBool>();
    null_literal_type->set_null_literal(true);
    auto from_nested_type = std::make_shared<DataTypeNullable>(null_literal_type);
    auto from_type = std::make_shared<DataTypeArray>(from_nested_type);

    auto from_nested_column =
            ColumnHelper::create_nullable_column<DataTypeBool>({0, 0, 0}, {true, true, true});
    auto from_offsets = ColumnHelper::create_column_offsets<TYPE_UINT64>({0, 1, 3});
    auto from_column = ColumnArray::create(std::move(from_nested_column), std::move(from_offsets));

    auto to_type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));

    auto ctx = create_context(false);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto* result_array =
            check_and_get_column<ColumnArray>(block.get_by_position(1).column.get());
    ASSERT_NE(result_array, nullptr);
    EXPECT_EQ(result_array->get_offsets(), ColumnArray::Offsets64({0, 1, 3}));

    const auto* result_nested_nullable =
            check_and_get_column<ColumnNullable>(&result_array->get_data());
    ASSERT_NE(result_nested_nullable, nullptr);
    EXPECT_EQ(result_nested_nullable->get_null_map_data(), NullMap({1, 1, 1}));

    const auto* result_nested_array =
            check_and_get_column<ColumnArray>(&result_nested_nullable->get_nested_column());
    ASSERT_NE(result_nested_array, nullptr);
    EXPECT_EQ(result_nested_array->size(), 3);
    EXPECT_EQ(result_nested_array->get_offsets(), ColumnArray::Offsets64({0, 0, 0}));
}

TEST_F(FunctionCastTest, test_from_nested_null_literal_array_to_deeper_array) {
    auto null_literal_type = std::make_shared<DataTypeBool>();
    null_literal_type->set_null_literal(true);
    auto from_inner_type = std::make_shared<DataTypeArray>(null_literal_type);
    auto from_type = std::make_shared<DataTypeArray>(from_inner_type);

    auto leaf_column = ColumnHelper::create_nullable_column<DataTypeBool>({0}, {true});
    auto inner_offsets = ColumnHelper::create_column_offsets<TYPE_UINT64>({1});
    auto inner_column = ColumnArray::create(std::move(leaf_column), std::move(inner_offsets));
    auto inner_null_map = ColumnHelper::create_column<DataTypeUInt8>({false});
    auto nullable_inner_column =
            ColumnNullable::create(std::move(inner_column), std::move(inner_null_map));
    auto outer_offsets = ColumnHelper::create_column_offsets<TYPE_UINT64>({1});
    auto from_column =
            ColumnArray::create(std::move(nullable_inner_column), std::move(outer_offsets));

    auto to_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())));

    auto ctx = create_context(false);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 0), "[[null]]");
}

TEST_F(FunctionCastTest, test_from_non_null_array_to_nested_array_is_rejected) {
    ColumnArrayBuilder<DataTypeInt32> builder;
    builder.add({1});
    auto from = builder.build();
    auto to_type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));

    auto ctx = create_context(false);
    auto fn = get_cast_wrapper(ctx.get(), from.type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            from,
            {nullptr, to_type, "to"},
    };
    EXPECT_FALSE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));
}

// Build a Nullable(Array(Nullable(ElementDataType))) column. A row marked as NULL by `null_map`
// keeps the elements that its offsets describe as hidden payload.
template <typename ElementDataType>
static ColumnPtr build_nullable_array_column(
        const std::vector<typename ElementDataType::FieldType>& elements,
        const std::vector<NullMap::value_type>& element_null_map,
        const std::vector<uint64_t>& offsets, const std::vector<NullMap::value_type>& null_map) {
    auto nested = ColumnHelper::create_nullable_column<ElementDataType>(elements, element_null_map);
    auto array = ColumnArray::create(std::move(nested),
                                     ColumnHelper::create_column_offsets<TYPE_UINT64>(offsets));
    return ColumnNullable::create(std::move(array),
                                  ColumnHelper::create_column<DataTypeUInt8>(null_map));
}

// A row that the input null map of an ARRAY marks as NULL may still keep a hidden payload in the
// elements that belong to it (for example the branch of an IF() that was not taken). Those elements
// are NULL by SQL semantics, so the strict cast must not validate them, and the mask of the row has
// to be expanded to its elements instead of being applied to the element indexes directly.
TEST_F(FunctionCastTest, test_cast_array_null_row_skips_hidden_payload) {
    auto from_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    auto to_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>()));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    // Row 0 is NULL and keeps the hidden payload 128, which does not fit into TINYINT.
    Block block = {
            {build_nullable_array_column<DataTypeInt32>({128, 1, 2}, {0, 0, 0}, {1, 3}, {1, 0}),
             from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(result.is_null_at(0));
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), "[1, 2]");
}

// The mask of a NULL row has to be inherited through every level of nesting.
TEST_F(FunctionCastTest, test_cast_nested_array_null_row_skips_hidden_payload) {
    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())));
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>())));

    // Row 0 is NULL and keeps the hidden inner array [128], row 1 is [[1, 2]].
    auto inner_array =
            build_nullable_array_column<DataTypeInt32>({128, 1, 2}, {0, 0, 0}, {1, 3}, {0, 0});
    auto outer_array = ColumnArray::create(
            inner_array, ColumnHelper::create_column_offsets<TYPE_UINT64>({1, 2}));
    ColumnPtr from_column = ColumnNullable::create(
            std::move(outer_array), ColumnHelper::create_column<DataTypeUInt8>({1, 0}));

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
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), "[[1, 2]]");
}

// The same masking gap exists when the element cast cannot replace the hidden payload with a
// default, so the mask itself has to reach the element cast.
TEST_F(FunctionCastTest, test_cast_array_null_row_skips_hidden_string_payload) {
    auto from_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    auto to_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>()));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {build_nullable_array_column<DataTypeString>({"abc", "1", "2"}, {0, 0, 0}, {1, 3},
                                                         {1, 0}),
             from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(result.is_null_at(0));
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), "[1, 2]");
}

// Only the rows that are NULL are skipped: an out of range element of a visible row still fails.
TEST_F(FunctionCastTest, test_cast_array_visible_element_still_fails) {
    auto from_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    auto to_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>()));

    auto ctx = create_context(true);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    // Row 0 is NULL with a hidden payload, row 1 keeps the element 300 that overflows TINYINT.
    Block block = {
            {build_nullable_array_column<DataTypeInt32>({128, 1, 300}, {0, 0, 0}, {1, 3}, {1, 0}),
             from_type, "from"},
            {nullptr, to_type, "to"},
    };
    EXPECT_FALSE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
}

// Without strict mode the out of range element of a visible row becomes NULL, while the NULL row
// and its hidden payload are untouched.
TEST_F(FunctionCastTest, test_cast_array_non_strict_null_row) {
    auto from_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    auto to_type = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>()));

    auto ctx = create_context(false);
    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {build_nullable_array_column<DataTypeInt32>({128, 1, 300}, {0, 0, 0}, {1, 3}, {1, 0}),
             from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(result.is_null_at(0));
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), "[1, null]");
}

} // namespace doris
