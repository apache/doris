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

#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/field.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_variant.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
static doris::vectorized::Field construct_variant_map(
        const std::vector<std::pair<std::string, doris::vectorized::Field>>& key_and_values) {
    doris::vectorized::Field res = Field::create_field<TYPE_VARIANT>(VariantMap {});
    auto& object = res.get<VariantMap&>();
    for (const auto& [k, v] : key_and_values) {
        PathInData path(k);
        object.try_emplace(path, v);
    }
    return res;
}

static auto construct_basic_varint_column() {
    // 1. create an empty variant column
    auto variant = ColumnVariant::create(5);

    std::vector<std::pair<std::string, doris::vectorized::Field>> data;

    // 2. subcolumn path
    data.emplace_back("v.a", Field::create_field<TYPE_INT>(20));
    data.emplace_back("v.b", Field::create_field<TYPE_STRING>("20"));
    data.emplace_back("v.c", Field::create_field<TYPE_INT>(20));
    data.emplace_back("v.f", Field::create_field<TYPE_INT>(20));
    data.emplace_back("v.e", Field::create_field<TYPE_STRING>("50"));
    for (int i = 0; i < 5; ++i) {
        auto field = construct_variant_map(data);
        variant->try_insert(field);
    }

    return variant;
}

TEST(FunctionVariantCast, CastToVariant) {
    // Test casting from basic types to variant
    {
        // Test Int32 to variant
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto int32_col = ColumnInt32::create();
        int32_col->insert(Field::create_field<TYPE_INT>(42));
        int32_col->insert(Field::create_field<TYPE_INT>(100));
        int32_col->insert(Field::create_field<TYPE_INT>(-1));

        ColumnsWithTypeAndName arguments {{int32_col->get_ptr(), int32_type, "int32_col"},
                                          {nullptr, variant_type, "variant_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, variant_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, variant_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 3).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* variant_col = assert_cast<const ColumnVariant*>(result_col.get());
        ASSERT_EQ(variant_col->size(), 3);
    }

    // Test casting from string to variant
    {
        auto string_type = std::make_shared<DataTypeString>();
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto string_col = ColumnString::create();
        string_col->insert_data("hello", 5);
        string_col->insert_data("world", 5);

        ColumnsWithTypeAndName arguments {{string_col->get_ptr(), string_type, "string_col"},
                                          {nullptr, variant_type, "variant_type"}};

        auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments,
                                                                       make_nullable(variant_type));
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, variant_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 2).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* variant_col =
                assert_cast<const ColumnVariant*>(remove_nullable(result_col).get());
        ASSERT_EQ(variant_col->size(), 2);
    }

    // Test casting from array to variant
    {
        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto array_col =
                ColumnArray::create(ColumnInt32::create(), ColumnArray::ColumnOffsets::create());
        auto& data = assert_cast<ColumnInt32&>(array_col->get_data());
        auto& offsets = array_col->get_offsets();

        data.insert(Field::create_field<TYPE_INT>(1));
        data.insert(Field::create_field<TYPE_INT>(2));
        data.insert(Field::create_field<TYPE_INT>(3));
        offsets.push_back(3);

        ColumnsWithTypeAndName arguments {{array_col->get_ptr(), array_type, "array_col"},
                                          {nullptr, variant_type, "variant_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, variant_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, variant_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* variant_col =
                assert_cast<const ColumnVariant*>(remove_nullable(result_col).get());
        ASSERT_EQ(variant_col->size(), 1);
    }
}

TEST(FunctionVariantCast, CastFromVariant) {
    // Test casting from variant to basic types
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_col = ColumnVariant::create(0);

        // Create a variant column with integer values
        variant_col->create_root(int32_type, ColumnInt32::create());
        MutableColumnPtr data = variant_col->get_root();
        data->insert(Field::create_field<TYPE_INT>(42));
        data->insert(Field::create_field<TYPE_INT>(100));
        data->insert(Field::create_field<TYPE_INT>(-1));

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, int32_type, "int32_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, int32_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, int32_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 3).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        // always nullable
        const auto* int32_result =
                assert_cast<const ColumnInt32*>(remove_nullable(result_col).get());
        ASSERT_EQ(int32_result->size(), 3);
        ASSERT_EQ(int32_result->get_element(0), 42);
        ASSERT_EQ(int32_result->get_element(1), 100);
        ASSERT_EQ(int32_result->get_element(2), -1);
    }

    // Test casting from variant to string
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto string_type = std::make_shared<DataTypeString>();
        auto variant_col = ColumnVariant::create(0);

        // Create a variant column with string values
        variant_col->create_root(string_type, ColumnString::create());
        MutableColumnPtr data = variant_col->get_root();
        data->insert_data("hello", 5);
        data->insert_data("world", 5);

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, string_type, "string_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, string_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, string_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 2).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* string_result =
                assert_cast<const ColumnString*>(remove_nullable(result_col).get());
        ASSERT_EQ(string_result->size(), 2);
        ASSERT_EQ(string_result->get_data_at(0).to_string(), "hello");
        ASSERT_EQ(string_result->get_data_at(1).to_string(), "world");
    }

    // Test casting from variant to array
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
        auto variant_col = ColumnVariant::create(0);

        // Create a variant column with array values
        variant_col->create_root(
                array_type,
                ColumnArray::create(ColumnInt32::create(), ColumnArray::ColumnOffsets::create()));
        MutableColumnPtr data = variant_col->get_root();

        Field a = Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(1),
                                                         Field::create_field<TYPE_INT>(2),
                                                         Field::create_field<TYPE_INT>(3)});

        data->insert(a);

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, array_type, "array_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, array_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, array_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* array_result =
                assert_cast<const ColumnArray*>(remove_nullable(result_col).get());
        ASSERT_EQ(array_result->size(), 1);
        const auto& result_data = assert_cast<const ColumnInt32&>(array_result->get_data());
        ASSERT_EQ(result_data.size(), 3);
        ASSERT_EQ(result_data.get_element(0), 1);
        ASSERT_EQ(result_data.get_element(1), 2);
        ASSERT_EQ(result_data.get_element(2), 3);
    }
}

TEST(FunctionVariantCast, CastVariantWithNull) {
    auto variant_type = std::make_shared<DataTypeVariant>();
    auto int32_type = std::make_shared<DataTypeInt32>();
    auto nullable_int32_type = std::make_shared<DataTypeNullable>(int32_type);

    // Create a variant column with nullable integer values
    auto variant_col = ColumnVariant::create(0);
    variant_col->create_root(nullable_int32_type,
                             ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create()));
    MutableColumnPtr data = variant_col->get_root();

    data->insert(Field::create_field<TYPE_INT>(42));
    data->insert(Field::create_field<TYPE_NULL>(Null()));
    data->insert(Field::create_field<TYPE_INT>(100));

    ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                      {nullptr, nullable_int32_type, "nullable_int32_type"}};

    variant_col->finalize();
    auto function =
            SimpleFunctionFactory::instance().get_function("CAST", arguments, nullable_int32_type);
    ASSERT_NE(function, nullptr);

    Block block {arguments};
    size_t result_column = block.columns();
    block.insert({nullptr, nullable_int32_type, "result"});

    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});
    ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 3).ok());

    auto result_col = block.get_by_position(result_column).column;
    ASSERT_NE(result_col.get(), nullptr);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result_col.get());
    ASSERT_EQ(nullable_result->size(), 3);

    const auto& result_data = assert_cast<const ColumnInt32&>(nullable_result->get_nested_column());
    const auto& result_null_map = nullable_result->get_null_map_data();

    ASSERT_EQ(result_data.get_element(0), 42);
    ASSERT_EQ(result_null_map[0], 0);
    ASSERT_EQ(result_null_map[1], 1);
    ASSERT_EQ(result_data.get_element(2), 100);
}

TEST(FunctionVariantCast, CastFromVariantWithEmptyRoot) {
    // Test case 1: variant.empty() branch
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto int32_type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr root = ColumnInt32::create();
        root->insert(Field::create_field<TYPE_INT>(42));
        vectorized::ColumnVariant::Subcolumns dynamic_subcolumns;
        dynamic_subcolumns.add(
                vectorized::PathInData(ColumnVariant::COLUMN_NAME_DUMMY),
                vectorized::ColumnVariant::Subcolumn {root->get_ptr(), int32_type, true, true});
        auto variant_col = ColumnVariant::create(0, std::move(dynamic_subcolumns));

        variant_col->finalize();
        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, int32_type, "int32_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, int32_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, int32_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        // always nullable
        const auto* int32_result =
                assert_cast<const ColumnInt32*>(remove_nullable(result_col).get());
        ASSERT_EQ(int32_result->size(), 1);
        // because of variant.empty() we insert_default with data_type_to
        ASSERT_EQ(int32_result->get_element(0), 0);
    }

    // Test case 2: !data_type_to->is_nullable() && !WhichDataType(data_type_to).is_string() branch
    {
        // object has sparse column
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_col = construct_basic_varint_column();
        auto variant_type = std::make_shared<DataTypeVariant>();

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, int32_type, "int32_type"}};

        variant_col->finalize();
        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, int32_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, int32_type, "result"});
        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* nullable_result = assert_cast<const ColumnNullable*>(result_col.get());
        ASSERT_EQ(nullable_result->size(), 1);
        ASSERT_TRUE(nullable_result->is_null_at(0));
    }

    // Test case 3: WhichDataType(data_type_to).is_string() branch
    {
        // variant has sparse column
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_col = construct_basic_varint_column();

        auto string_type = std::make_shared<DataTypeString>();
        auto variant_type = std::make_shared<DataTypeVariant>();

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, string_type, "string_type"}};

        variant_col->finalize();
        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, string_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, string_type, "result"});
        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* string_result = assert_cast<const ColumnString*>(result_col.get());
        // just call ConvertImplGenericToString which will insert all source column data to ColumnString
        ASSERT_EQ(string_result->size(), variant_col->size());
        ASSERT_EQ(string_result->get_data_at(0).to_string(),
                  "{\"v\":{\"a\":20,\"b\":\"20\",\"c\":20,\"e\":\"50\",\"f\":20}}");
    }

    // Test case 4: else branch (nullable type)
    {
        auto variant_col = construct_basic_varint_column();
        variant_col->finalize();
        auto nullable_variant_col = make_nullable(variant_col->get_ptr());

        auto nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto nullable_variant_type = make_nullable(variant_type);

        ColumnsWithTypeAndName arguments {
                {nullable_variant_col->get_ptr(), nullable_variant_type, "variant_col"},
                {nullptr, nullable_string_type, "nullable_string_type"}};

        auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments,
                                                                       nullable_string_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, nullable_string_type, "result"});
        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* nullable_result = assert_cast<const ColumnNullable*>(result_col.get());
        ASSERT_EQ(nullable_result->size(), 1);
        ASSERT_TRUE(nullable_result->is_null_at(1));
    }
}

} // namespace doris::vectorized