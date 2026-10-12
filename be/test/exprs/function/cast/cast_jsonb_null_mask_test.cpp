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

#include "core/column/column_array.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_struct.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/cast/cast_test.h"
#include "runtime/runtime_state.h"

namespace doris {

static ColumnPtr make_nullable_jsonb_column(const std::vector<std::string>& values,
                                            const std::vector<UInt8>& null_map) {
    auto nested = ColumnString::create();
    JsonBinaryValue jsonb;
    for (const auto& value : values) {
        EXPECT_TRUE(jsonb.from_json_string(value).ok());
        nested->insert_data(jsonb.value(), jsonb.size());
    }
    return ColumnNullable::create(std::move(nested),
                                  ColumnHelper::create_column<DataTypeUInt8>(null_map));
}

TEST_F(FunctionCastTest, jsonb_source_skips_null_rows) {
    const auto from_type = make_nullable(std::make_shared<DataTypeJsonb>());
    const DataTypes targets = {
            std::make_shared<DataTypeInt8>(), std::make_shared<DataTypeDecimal32>(9, 0),
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt8>()))};
    for (const auto& target : targets) {
        const auto to_type = make_nullable(target);
        const auto valid = target->get_primitive_type() == TYPE_ARRAY ? "[1]" : "1";
        for (bool strict : {false, true}) {
            SCOPED_TRACE(target->get_name() + (strict ? " strict" : " non-strict"));
            auto ctx = create_context(strict);
            auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
            // The invalid JSONB strings are hidden. The valid rows must keep their positions.
            Block block {{make_nullable_jsonb_column({R"("bad")", valid, R"("bad")", valid},
                                                     {1, 0, 1, 0}),
                          from_type, "from"},
                         {nullptr, to_type, "to"}};
            ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
            const auto& result =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            ASSERT_EQ(result.size(), 4);
            for (size_t i = 0; i < 4; ++i) {
                EXPECT_EQ(result.is_null_at(i), i % 2 == 0);
            }
            EXPECT_EQ(target->to_string(result.get_nested_column(), 1), valid);
            EXPECT_EQ(target->to_string(result.get_nested_column(), 3), valid);
            auto default_value = target->create_column();
            default_value->insert_default();
            EXPECT_EQ(result.get_nested_column().compare_at(0, 0, *default_value, 1), 0);
            EXPECT_EQ(result.get_nested_column().compare_at(2, 0, *default_value, 1), 0);

            // Reveal one invalid row. Strict mode must fail. Non-strict mode must return NULL.
            Block invalid {{make_nullable_jsonb_column({R"("bad")", valid, R"("bad")", valid},
                                                       {1, 0, 0, 0}),
                            from_type, "from"},
                           {nullptr, to_type, "to"}};
            auto st = fn(ctx.get(), invalid, {0}, 1, invalid.rows(), nullptr);
            if (strict) {
                EXPECT_FALSE(st.ok());
            } else {
                ASSERT_TRUE(st.ok());
                const auto& invalid_result =
                        assert_cast<const ColumnNullable&>(*invalid.get_by_position(1).column);
                ASSERT_EQ(invalid_result.size(), 4);
                EXPECT_TRUE(invalid_result.is_null_at(2));
                EXPECT_EQ(target->to_string(invalid_result.get_nested_column(), 3), valid);
            }
        }
    }
}

TEST_F(FunctionCastTest, jsonb_source_string_skips_null_rows) {
    const auto from_type = make_nullable(std::make_shared<DataTypeJsonb>());
    const auto to_type = make_nullable(std::make_shared<DataTypeString>());
    RuntimeState state;
    for (bool string_as_string : {false, true}) {
        auto ctx = FunctionContext::create_context(&state, to_type, {from_type});
        ctx->set_jsonb_string_as_string(string_as_string);
        auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
        Block block {
                {make_nullable_jsonb_column({R"("bad")", R"("ok")"}, {1, 0}), from_type, "from"},
                {nullptr, to_type, "to"}};
        ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        ASSERT_EQ(result.size(), 2);
        EXPECT_TRUE(result.is_null_at(0));
        EXPECT_FALSE(result.is_null_at(1));
        EXPECT_EQ(result.get_nested_column().get_data_at(0).size, 0);
        EXPECT_EQ(result.get_nested_column().get_data_at(1).to_string(), "ok");
    }
}

TEST_F(FunctionCastTest, jsonb_destination_skips_null_struct_rows) {
    const auto struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>()}, Strings {std::string(256, 'k')});
    const auto from_type = make_nullable(struct_type);
    const auto to_type = make_nullable(std::make_shared<DataTypeJsonb>());
    for (bool strict : {false, true}) {
        auto ctx = create_context(strict);
        auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
        const auto build_source = [](const std::vector<UInt8>& null_map) {
            auto nested = ColumnStruct::create(
                    Columns {ColumnHelper::create_column<DataTypeInt32>({1, 2})});
            return ColumnNullable::create(std::move(nested),
                                          ColumnHelper::create_column<DataTypeUInt8>(null_map));
        };
        Block block {{build_source({1, 1}), from_type, "from"}, {nullptr, to_type, "to"}};
        ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        ASSERT_EQ(result.size(), 2);
        EXPECT_TRUE(result.is_null_at(0));
        EXPECT_TRUE(result.is_null_at(1));
        EXPECT_EQ(result.get_nested_column().get_data_at(0).size, 0);
        EXPECT_EQ(result.get_nested_column().get_data_at(1).size, 0);

        // The same field name must cause an error when its row is visible, in either mode.
        for (const auto& mask : {std::vector<UInt8> {1, 0}, std::vector<UInt8> {0, 0}}) {
            Block invalid {{build_source(mask), from_type, "from"}, {nullptr, to_type, "to"}};
            auto st = fn(ctx.get(), invalid, {0}, 1, invalid.rows(), nullptr);
            EXPECT_FALSE(st.ok());
            EXPECT_NE(st.to_string().find("key size exceeds max limit"), std::string::npos);
        }
    }
}

TEST_F(FunctionCastTest, jsonb_destination_skips_hidden_struct_and_keeps_visible_array) {
    const auto struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>()}, Strings {std::string(256, 'k')});
    const auto from_type =
            make_nullable(std::make_shared<DataTypeArray>(make_nullable(struct_type)));
    const auto to_type = make_nullable(std::make_shared<DataTypeJsonb>());
    for (bool strict : {false, true}) {
        auto ctx = create_context(strict);
        auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
        const auto build_source = [](const std::vector<UInt8>& null_map) {
            auto structs = ColumnStruct::create(
                    Columns {ColumnHelper::create_column<DataTypeInt32>({1, 2})});
            auto elements = ColumnNullable::create(std::move(structs), ColumnUInt8::create(2, 0));
            auto arrays = ColumnArray::create(
                    std::move(elements),
                    ColumnHelper::create_column_offsets<TYPE_UINT64>({1, 1, 2, 2}));
            return ColumnNullable::create(std::move(arrays),
                                          ColumnHelper::create_column<DataTypeUInt8>(null_map));
        };
        Block block {{build_source({1, 0, 1, 0}), from_type, "from"}, {nullptr, to_type, "to"}};
        ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        ASSERT_EQ(result.size(), 4);
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_EQ(result.is_null_at(i), i % 2 == 0);
        }
        EXPECT_EQ(to_type->to_string(result, 1), "[]");
        EXPECT_EQ(to_type->to_string(result, 3), "[]");
        EXPECT_EQ(result.get_nested_column().get_data_at(0).size, 0);
        EXPECT_EQ(result.get_nested_column().get_data_at(2).size, 0);

        Block invalid {{build_source({1, 0, 0, 0}), from_type, "from"}, {nullptr, to_type, "to"}};
        auto st = fn(ctx.get(), invalid, {0}, 1, invalid.rows(), nullptr);
        EXPECT_FALSE(st.ok());
        EXPECT_NE(st.to_string().find("key size exceeds max limit"), std::string::npos);
    }
}

TEST_F(FunctionCastTest, jsonb_destination_numeric_skips_null_rows) {
    const DataTypes targets = {std::make_shared<DataTypeInt32>(),
                               std::make_shared<DataTypeDecimal32>(9, 0)};
    for (const auto& nested_type : targets) {
        auto nested = nested_type->create_column();
        nested->insert_default();
        nested->insert_default();
        const auto from_type = make_nullable(nested_type);
        const auto to_type = make_nullable(std::make_shared<DataTypeJsonb>());
        ColumnPtr source = ColumnNullable::create(
                std::move(nested), ColumnHelper::create_column<DataTypeUInt8>({1, 0}));
        for (bool strict : {false, true}) {
            auto ctx = create_context(strict);
            auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
            Block block {{source, from_type, "from"}, {nullptr, to_type, "to"}};
            ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr).ok());
            const auto& result =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            ASSERT_EQ(result.size(), 2);
            EXPECT_TRUE(result.is_null_at(0));
            EXPECT_FALSE(result.is_null_at(1));
            EXPECT_EQ(result.get_nested_column().get_data_at(0).size, 0);
            EXPECT_EQ(to_type->to_string(result, 1), "0");
        }
    }
}

} // namespace doris
