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

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

template <typename T>
class ScopedValue {
public:
    ScopedValue(T& target, T value) : _target(target), _old(target) { _target = value; }
    ~ScopedValue() { _target = _old; }

private:
    T& _target;
    T _old;
};

ColumnPtr make_strings(const std::vector<std::string>& values) {
    auto column = ColumnString::create();
    for (const std::string& value : values) {
        column->insert_data(value.data(), value.size());
    }
    return column;
}

ColumnPtr make_nullable_strings(const std::vector<std::optional<std::string>>& values) {
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create(values.size(), uint8_t {0});
    for (size_t row = 0; row < values.size(); ++row) {
        if (values[row].has_value()) {
            strings->insert_data(values[row]->data(), values[row]->size());
        } else {
            strings->insert_default();
            nulls->get_data()[row] = 1;
        }
    }
    return ColumnNullable::create(std::move(strings), std::move(nulls));
}

struct ExecutionResult {
    Status status;
    ColumnPtr output;
    DataTypePtr return_type;
};

ExecutionResult execute_parse(std::string_view function_name, ColumnPtr input,
                              const DataTypePtr& input_type, size_t rows,
                              DataTypePtr result_type = nullptr, bool use_variant_v2 = true) {
    if (result_type == nullptr) {
        if (use_variant_v2) {
            result_type = std::make_shared<DataTypeVariantV2>();
        } else {
            result_type = std::make_shared<DataTypeVariant>();
        }
        if (function_name == "parse_to_variant_error_to_null" || input_type->is_nullable()) {
            result_type = make_nullable(result_type);
        }
    }

    Block block;
    block.insert({std::move(input), input_type, "json"});
    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            std::string(function_name), block.get_columns_with_type_and_name(), result_type);
    DORIS_CHECK(function != nullptr);
    block.insert({nullptr, result_type, "result"});

    FunctionUtils function_utils(result_type, {input_type}, false);
    FunctionContext* context = function_utils.get_fn_ctx();
    DORIS_CHECK(function->open(context, FunctionContext::FRAGMENT_LOCAL).ok());
    DORIS_CHECK(function->open(context, FunctionContext::THREAD_LOCAL).ok());
    Status status = function->execute(context, block, {0}, 1, rows);
    static_cast<void>(function->close(context, FunctionContext::THREAD_LOCAL));
    static_cast<void>(function->close(context, FunctionContext::FRAGMENT_LOCAL));
    return {.status = std::move(status),
            .output = block.get_by_position(1).column,
            .return_type = function->get_return_type()};
}

ExecutionResult execute_parse(std::string_view function_name, ColumnPtr input,
                              const DataTypePtr& input_type, size_t rows, bool use_variant_v2) {
    return execute_parse(function_name, std::move(input), input_type, rows, nullptr,
                         use_variant_v2);
}

const IColumn& physical_column(const ColumnPtr& output, size_t* row) {
    DORIS_CHECK(static_cast<bool>(output));
    if (const auto* constant = check_and_get_column<ColumnConst>(output.get())) {
        *row = 0;
        return constant->get_data_column();
    }
    return *output;
}

bool is_sql_null_at(const ColumnPtr& output, size_t row) {
    const IColumn& physical = physical_column(output, &row);
    const auto* nullable = check_and_get_column<ColumnNullable>(&physical);
    return nullable != nullptr && nullable->is_null_at(row);
}

struct StringWriter {
    void write(const char* data, size_t size) { value.append(data, size); }
    std::string value;
};

std::string variant_json_at(const ColumnPtr& output, size_t row) {
    const IColumn* physical = &physical_column(output, &row);
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        DORIS_CHECK(!nullable->is_null_at(row));
        physical = &nullable->get_nested_column();
    }
    const auto& variant = assert_cast<const ColumnVariantV2&>(*physical);
    StringWriter writer;
    to_json(variant.get_value_ref(row), writer, VariantJsonFormatOptions {});
    return writer.value;
}

std::string nested_array_json(uint32_t depth) {
    std::string json(depth, '[');
    json.push_back('0');
    json.append(depth, ']');
    return json;
}

} // namespace

TEST(FunctionVariantParseTest, ExecutionTypeSelectsPhysicalColumn) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    ExecutionResult legacy =
            execute_parse("parse_to_variant", make_strings({R"({"a":1})"}), string_type, 1, false);
    ASSERT_TRUE(legacy.status.ok()) << legacy.status.to_string();
    EXPECT_NE(check_and_get_column_with_const<ColumnVariant>(*legacy.output), nullptr);

    ExecutionResult v2 =
            execute_parse("parse_to_variant", make_strings({R"({"a":1})"}), string_type, 1, true);
    ASSERT_TRUE(v2.status.ok()) << v2.status.to_string();
    EXPECT_NE(check_and_get_column_with_const<ColumnVariantV2>(*v2.output), nullptr);
}

TEST(FunctionVariantParseTest, LegacyPathPreservesSqlNullAndErrorToNull) {
    const DataTypePtr nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
    ExecutionResult nullable =
            execute_parse("parse_to_variant",
                          make_nullable_strings({std::nullopt, std::string(R"({"value":1})")}),
                          nullable_string_type, 2, false);
    ASSERT_TRUE(nullable.status.ok()) << nullable.status.to_string();
    EXPECT_TRUE(is_sql_null_at(nullable.output, 0));
    const auto& nullable_output = assert_cast<const ColumnNullable&>(*nullable.output);
    EXPECT_NE(check_and_get_column<ColumnVariant>(&nullable_output.get_nested_column()), nullptr);

    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const std::string invalid_utf8(1, static_cast<char>(0xFF));
    ScopedValue strict(config::variant_throw_exeception_on_invalid_json, true);
    ExecutionResult failure = execute_parse(
            "parse_to_variant", make_strings({R"({"before":1})", invalid_utf8, R"({"after":2})"}),
            string_type, 3, false);
    EXPECT_FALSE(failure.status.ok());
    EXPECT_FALSE(static_cast<bool>(failure.output));

    ExecutionResult recoverable =
            execute_parse("parse_to_variant_error_to_null",
                          make_strings({R"({"before":1})", invalid_utf8, R"({"after":2})"}),
                          string_type, 3, false);
    ASSERT_TRUE(recoverable.status.ok()) << recoverable.status.to_string();
    EXPECT_FALSE(is_sql_null_at(recoverable.output, 0));
    EXPECT_TRUE(is_sql_null_at(recoverable.output, 1));
    EXPECT_FALSE(is_sql_null_at(recoverable.output, 2));
    const auto& recoverable_output = assert_cast<const ColumnNullable&>(*recoverable.output);
    EXPECT_NE(check_and_get_column<ColumnVariant>(&recoverable_output.get_nested_column()),
              nullptr);
}

TEST(FunctionVariantParseTest, FunctionsAreRegistered) {
    const DataTypePtr argument_type = make_nullable(std::make_shared<DataTypeString>());
    const DataTypePtr result_type = make_nullable(std::make_shared<DataTypeVariant>());
    const ColumnsWithTypeAndName arguments {
            {argument_type->create_column(), argument_type, "json"}};

    EXPECT_NE(SimpleFunctionFactory::instance().get_function("parse_to_variant", arguments,
                                                             result_type),
              nullptr);
    EXPECT_NE(SimpleFunctionFactory::instance().get_function("parse_to_variant_error_to_null",
                                                             arguments, result_type),
              nullptr);
}

TEST(FunctionVariantParseTest, ConfiguredVariantReturnTypeBuildsAndExecutes) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr max_subcolumns_variant = std::make_shared<DataTypeVariantV2>(2048, false);
    ExecutionResult result = execute_parse("parse_to_variant", make_strings({R"({"a":1})"}),
                                           string_type, 1, max_subcolumns_variant);

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_TRUE(static_cast<bool>(result.output));
    ASSERT_TRUE(static_cast<bool>(result.return_type));
    EXPECT_TRUE(result.return_type->equals(*max_subcolumns_variant));
    EXPECT_NE(check_and_get_column<ColumnVariantV2>(result.output.get()), nullptr);
    EXPECT_EQ(variant_json_at(result.output, 0), R"({"a":1})");

    const DataTypePtr nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
    const DataTypePtr nullable_doc_mode_variant =
            make_nullable(std::make_shared<DataTypeVariantV2>(0, true));
    ExecutionResult nullable = execute_parse(
            "parse_to_variant", make_nullable_strings({std::nullopt, std::string(R"([1,2])")}),
            nullable_string_type, 2, nullable_doc_mode_variant);
    ASSERT_TRUE(nullable.status.ok()) << nullable.status.to_string();
    ASSERT_TRUE(static_cast<bool>(nullable.return_type));
    EXPECT_TRUE(nullable.return_type->equals(*nullable_doc_mode_variant));
    EXPECT_TRUE(is_sql_null_at(nullable.output, 0));
    EXPECT_EQ(variant_json_at(nullable.output, 1), "[1,2]");

    const DataTypePtr nullable_max_subcolumns_variant =
            make_nullable(std::make_shared<DataTypeVariantV2>(2048, false));
    ExecutionResult error_to_null =
            execute_parse("parse_to_variant_error_to_null", make_strings({"true"}), string_type, 1,
                          nullable_max_subcolumns_variant);
    ASSERT_TRUE(error_to_null.status.ok()) << error_to_null.status.to_string();
    ASSERT_TRUE(static_cast<bool>(error_to_null.return_type));
    EXPECT_TRUE(error_to_null.return_type->equals(*nullable_max_subcolumns_variant));
    EXPECT_FALSE(is_sql_null_at(error_to_null.output, 0));
    EXPECT_EQ(variant_json_at(error_to_null.output, 0), "true");
}

TEST(FunctionVariantParseTest, DistinguishesSqlNullJsonNullEmptyAndConst) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    ExecutionResult values = execute_parse(
            "parse_to_variant", make_strings({R"({"a":1})", "null", ""}), string_type, 3);
    ASSERT_TRUE(values.status.ok()) << values.status.to_string();
    ASSERT_EQ(values.output->size(), 3);
    EXPECT_FALSE(is_sql_null_at(values.output, 0));
    EXPECT_FALSE(is_sql_null_at(values.output, 1));
    EXPECT_EQ(variant_json_at(values.output, 0), R"({"a":1})");
    EXPECT_EQ(variant_json_at(values.output, 1), "null");
    EXPECT_EQ(variant_json_at(values.output, 2), "{}");

    const DataTypePtr nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
    ExecutionResult nullable = execute_parse(
            "parse_to_variant",
            make_nullable_strings({std::nullopt, std::string("null"), std::string(R"({"b":2})")}),
            nullable_string_type, 3);
    ASSERT_TRUE(nullable.status.ok()) << nullable.status.to_string();
    EXPECT_TRUE(is_sql_null_at(nullable.output, 0));
    EXPECT_FALSE(is_sql_null_at(nullable.output, 1));
    EXPECT_EQ(variant_json_at(nullable.output, 1), "null");
    EXPECT_EQ(variant_json_at(nullable.output, 2), R"({"b":2})");

    ColumnPtr constant = ColumnConst::create(make_strings({R"([1,2])"}), 4);
    ExecutionResult constant_result =
            execute_parse("parse_to_variant", std::move(constant), string_type, 4);
    ASSERT_TRUE(constant_result.status.ok()) << constant_result.status.to_string();
    ASSERT_TRUE(is_column_const(*constant_result.output));
    ASSERT_EQ(constant_result.output->size(), 4);
    EXPECT_EQ(variant_json_at(constant_result.output, 3), "[1,2]");
}

TEST(FunctionVariantParseTest, StrictFailureDoesNotPublishPartialBatch) {
    ScopedValue strict(config::variant_throw_exeception_on_invalid_json, true);
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    ExecutionResult result =
            execute_parse("parse_to_variant",
                          make_strings({R"({"before":1})", "{", R"({"after":2})"}), string_type, 3);

    EXPECT_FALSE(result.status.ok());
    EXPECT_EQ(result.status.code(), ErrorCode::INVALID_ARGUMENT) << result.status.to_string();
    EXPECT_NE(result.status.to_string().find("Parse json document failed at row 1, error: "),
              std::string::npos)
            << result.status.to_string();
    EXPECT_NE(result.status.to_string().find("Failed to parse JSON as Variant"), std::string::npos)
            << result.status.to_string();
    EXPECT_FALSE(static_cast<bool>(result.output));
}

TEST(FunctionVariantParseTest, ErrorToNullOnlyNullsRecoverableFailures) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    {
        ScopedValue strict(config::variant_throw_exeception_on_invalid_json, true);
        ExecutionResult result = execute_parse(
                "parse_to_variant_error_to_null",
                make_strings({R"({"before":1})", "{", "null", R"({"after":2})"}), string_type, 4);
        ASSERT_TRUE(result.status.ok()) << result.status.to_string();
        EXPECT_FALSE(is_sql_null_at(result.output, 0));
        EXPECT_TRUE(is_sql_null_at(result.output, 1));
        EXPECT_FALSE(is_sql_null_at(result.output, 2));
        EXPECT_EQ(variant_json_at(result.output, 2), "null");
        EXPECT_EQ(variant_json_at(result.output, 3), R"({"after":2})");
    }
    {
        ScopedValue permissive(config::variant_throw_exeception_on_invalid_json, false);
        ExecutionResult result = execute_parse("parse_to_variant_error_to_null",
                                               make_strings({"{"}), string_type, 1);
        ASSERT_TRUE(result.status.ok()) << result.status.to_string();
        EXPECT_FALSE(is_sql_null_at(result.output, 0));
        EXPECT_EQ(variant_json_at(result.output, 0), R"("{")");
    }
}

TEST(FunctionVariantParseTest, ConfiguredInputValidationUsesFailOrOuterNull) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    {
        ScopedValue key_limit(config::variant_max_json_key_length, 3);
        ExecutionResult fail =
                execute_parse("parse_to_variant", make_strings({R"({"abcd":1})"}), string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        EXPECT_FALSE(static_cast<bool>(fail.output));
        ExecutionResult null = execute_parse("parse_to_variant_error_to_null",
                                             make_strings({R"({"abcd":1})"}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
    {
        ScopedValue reject_duplicates(config::variant_enable_duplicate_json_path_check, false);
        ExecutionResult fail = execute_parse("parse_to_variant", make_strings({R"({"a":1,"a":2})"}),
                                             string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        ExecutionResult null = execute_parse("parse_to_variant_error_to_null",
                                             make_strings({R"({"a":1,"a":2})"}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
    {
        ScopedValue keep_first(config::variant_enable_duplicate_json_path_check, true);
        ExecutionResult result = execute_parse("parse_to_variant_error_to_null",
                                               make_strings({R"({"a":1,"a":2})"}), string_type, 1);
        ASSERT_TRUE(result.status.ok()) << result.status.to_string();
        EXPECT_FALSE(is_sql_null_at(result.output, 0));
        EXPECT_EQ(variant_json_at(result.output, 0), R"({"a":1})");
    }
    {
        ScopedValue permissive(config::variant_throw_exeception_on_invalid_json, false);
        const std::string invalid_utf8(1, static_cast<char>(0xFF));
        ExecutionResult fail =
                execute_parse("parse_to_variant", make_strings({invalid_utf8}), string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        ExecutionResult null = execute_parse("parse_to_variant_error_to_null",
                                             make_strings({invalid_utf8}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
    {
        const std::string too_deep = nested_array_json(VARIANT_MAX_NESTING_DEPTH + 1);
        ExecutionResult fail =
                execute_parse("parse_to_variant", make_strings({too_deep}), string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        ExecutionResult null = execute_parse("parse_to_variant_error_to_null",
                                             make_strings({too_deep}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
}

} // namespace doris
