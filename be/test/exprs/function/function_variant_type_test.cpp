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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

struct ExecutionResult {
    Status status;
    ColumnPtr output;
};

ExecutionResult execute_variant_type(ColumnPtr input, const DataTypePtr& input_type) {
    const DataTypePtr result_type = make_nullable(std::make_shared<DataTypeString>());
    Block block {{std::move(input), input_type, "source"},
                 {result_type->create_column(), result_type, "result"}};
    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            "variant_type", {block.get_by_position(0)}, result_type);
    DORIS_CHECK(function != nullptr);
    FunctionUtils function_utils(result_type, {input_type}, false);
    const Status status =
            function->execute(function_utils.get_fn_ctx(), block, {0}, 1, block.rows());
    return {.status = status, .output = block.get_by_position(1).column};
}

ColumnVariantV2::MutablePtr encoded_json(std::initializer_list<std::string_view> rows) {
    JsonStringToVariantEncoder encoder;
    for (std::string_view row : rows) {
        encoder.add_json({row.data(), row.size()});
    }
    VariantBatchBuilder block = encoder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

void expect_values(const ExecutionResult& result,
                   std::initializer_list<std::optional<std::string_view>> expected) {
    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*result.output);
    const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
    ASSERT_EQ(nullable.size(), expected.size());
    size_t row = 0;
    for (const auto& value : expected) {
        EXPECT_EQ(nullable.is_null_at(row), !value.has_value()) << "row=" << row;
        if (value.has_value()) {
            EXPECT_EQ(strings.get_data_at(row).to_string_view(), *value) << "row=" << row;
        }
        ++row;
    }
}

} // namespace

TEST(FunctionVariantTypeTest, VariantV2EncodedReturnsScalarTypeWords) {
    const auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto source = encoded_json({"null", "true", "1", "1.5", R"("x")", "{}", "[]"});

    const ExecutionResult result = execute_variant_type(std::move(source), variant_type);

    expect_values(result, {"null", "bool", "tinyint", "double", "string", "object", "array"});
}

TEST(FunctionVariantTypeTest, VariantV2TypedValuesRetainNullAsVariantValue) {
    const auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto integers = ColumnInt32::create();
    integers->insert_value(1);
    integers->insert_value(70000);
    auto inner_nulls = ColumnUInt8::create();
    inner_nulls->insert_many_defaults(2);
    auto source = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(integers), std::move(inner_nulls)),
            std::make_shared<DataTypeInt32>());

    const ExecutionResult result = execute_variant_type(std::move(source), variant_type);

    expect_values(result, {"tinyint", "int"});
}

TEST(FunctionVariantTypeTest, VariantV2OuterNullRemainsSqlNull) {
    const auto variant_type = std::make_shared<DataTypeVariantV2>();
    MutableColumnPtr source = encoded_json({"null", "true"});
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_value(1);
    outer_nulls->insert_value(0);

    const ExecutionResult result =
            execute_variant_type(ColumnNullable::create(std::move(source), std::move(outer_nulls)),
                                 make_nullable(variant_type));

    expect_values(result, {std::nullopt, "bool"});
}

TEST(FunctionVariantTypeTest, PublicVariantAliasOuterNullRemainsSqlNull) {
    const auto variant_type = std::make_shared<DataTypeVariant>();
    MutableColumnPtr source = variant_type->create_column();
    source->insert_default();
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_value(1);

    const ExecutionResult result =
            execute_variant_type(ColumnNullable::create(std::move(source), std::move(outer_nulls)),
                                 make_nullable(variant_type));

    ASSERT_TRUE(result.status.ok()) << result.status;
    const auto& output = assert_cast<const ColumnNullable&>(*result.output);
    ASSERT_EQ(output.size(), 1);
    EXPECT_TRUE(output.is_null_at(0));
}

TEST(FunctionVariantTypeTest, PublicVariantAliasDefaultIsObject) {
    const auto variant_type = std::make_shared<DataTypeVariant>();
    MutableColumnPtr source = variant_type->create_column();
    source->insert_default();

    const ExecutionResult result = execute_variant_type(std::move(source), variant_type);

    expect_values(result, {"object"});
}

} // namespace doris
