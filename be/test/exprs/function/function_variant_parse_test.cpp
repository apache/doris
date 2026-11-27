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
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/define_primitive_type.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_template_paths.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vectorized_fn_call.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"

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
                              DataTypePtr result_type = nullptr) {
    if (result_type == nullptr) {
        result_type = std::make_shared<DataTypeVariantV2>();
        if (function_name == "try_parse_to_variant" || input_type->is_nullable()) {
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
    DORIS_CHECK(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    DORIS_CHECK(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    Status status = function->execute(context, block, {0}, 1, rows);
    static_cast<void>(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL));
    static_cast<void>(
            function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    return {.status = std::move(status),
            .output = block.get_by_position(1).column,
            .return_type = function->get_return_type()};
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

// The row's encoded metadata and value, to compare two reads byte for byte.
std::pair<std::string, std::string> variant_bytes_at(const ColumnPtr& output, size_t row) {
    const IColumn* physical = &physical_column(output, &row);
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        DORIS_CHECK(!nullable->is_null_at(row));
        physical = &nullable->get_nested_column();
    }
    const VariantRef value = assert_cast<const ColumnVariantV2&>(*physical).get_value_ref(row);
    return {std::string(value.metadata.data, value.metadata.size),
            std::string(value.value.data, value.value.size)};
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
    ExecutionResult variant =
            execute_parse("parse_to_variant", make_strings({R"({"a":1})"}), string_type, 1);
    ASSERT_TRUE(variant.status.ok()) << variant.status.to_string();
    EXPECT_NE(check_and_get_column_with_const<ColumnVariantV2>(*variant.output), nullptr);

    ExecutionResult v2 = execute_parse("parse_to_variant", make_strings({R"({"a":1})"}),
                                       string_type, 1, std::make_shared<DataTypeVariant>());
    ASSERT_TRUE(v2.status.ok()) << v2.status.to_string();
    EXPECT_NE(check_and_get_column_with_const<ColumnVariantV2>(*v2.output), nullptr);
}

TEST(FunctionVariantParseTest, PreservesSqlNullAndErrorToNull) {
    const DataTypePtr nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
    ExecutionResult nullable =
            execute_parse("parse_to_variant",
                          make_nullable_strings({std::nullopt, std::string(R"({"value":1})")}),
                          nullable_string_type, 2);
    ASSERT_TRUE(nullable.status.ok()) << nullable.status.to_string();
    EXPECT_TRUE(is_sql_null_at(nullable.output, 0));
    const auto& nullable_output = assert_cast<const ColumnNullable&>(*nullable.output);
    EXPECT_NE(check_and_get_column<ColumnVariantV2>(&nullable_output.get_nested_column()), nullptr);

    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const std::string invalid_utf8(1, static_cast<char>(0xFF));
    ExecutionResult failure = execute_parse(
            "parse_to_variant", make_strings({R"({"before":1})", invalid_utf8, R"({"after":2})"}),
            string_type, 3);
    EXPECT_FALSE(failure.status.ok());
    EXPECT_FALSE(static_cast<bool>(failure.output));

    ExecutionResult recoverable = execute_parse(
            "try_parse_to_variant",
            make_strings({R"({"before":1})", invalid_utf8, R"({"after":2})"}), string_type, 3);
    ASSERT_TRUE(recoverable.status.ok()) << recoverable.status.to_string();
    EXPECT_FALSE(is_sql_null_at(recoverable.output, 0));
    EXPECT_TRUE(is_sql_null_at(recoverable.output, 1));
    EXPECT_FALSE(is_sql_null_at(recoverable.output, 2));
    const auto& recoverable_output = assert_cast<const ColumnNullable&>(*recoverable.output);
    EXPECT_NE(check_and_get_column<ColumnVariantV2>(&recoverable_output.get_nested_column()),
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
    EXPECT_NE(SimpleFunctionFactory::instance().get_function("try_parse_to_variant", arguments,
                                                             result_type),
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
    ExecutionResult error_to_null = execute_parse("try_parse_to_variant", make_strings({"true"}),
                                                  string_type, 1, nullable_max_subcolumns_variant);
    ASSERT_TRUE(error_to_null.status.ok()) << error_to_null.status.to_string();
    ASSERT_TRUE(static_cast<bool>(error_to_null.return_type));
    EXPECT_TRUE(error_to_null.return_type->equals(*nullable_max_subcolumns_variant));
    EXPECT_FALSE(is_sql_null_at(error_to_null.output, 0));
    EXPECT_EQ(variant_json_at(error_to_null.output, 0), "true");
}

TEST(FunctionVariantParseTest, DistinguishesSqlNullJsonNullEmptyAndConst) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    ExecutionResult values =
            execute_parse("parse_to_variant", make_strings({R"({"a":1})", "null"}), string_type, 2);
    ASSERT_TRUE(values.status.ok()) << values.status.to_string();
    ASSERT_EQ(values.output->size(), 2);
    EXPECT_FALSE(is_sql_null_at(values.output, 0));
    EXPECT_FALSE(is_sql_null_at(values.output, 1));
    EXPECT_EQ(variant_json_at(values.output, 0), R"({"a":1})");
    EXPECT_EQ(variant_json_at(values.output, 1), "null");

    // An empty string is not a JSON document.
    ExecutionResult empty = execute_parse("parse_to_variant", make_strings({""}), string_type, 1);
    EXPECT_EQ(empty.status.code(), ErrorCode::INVALID_ARGUMENT) << empty.status.to_string();
    ExecutionResult empty_to_null =
            execute_parse("try_parse_to_variant", make_strings({""}), string_type, 1);
    ASSERT_TRUE(empty_to_null.status.ok()) << empty_to_null.status.to_string();
    EXPECT_TRUE(is_sql_null_at(empty_to_null.output, 0));

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

TEST(FunctionVariantParseTest, AMalformedDocumentReportsTheSyntaxErrorUnderATemplateToo) {
    // With a DECIMAL template the document is read by the ondemand pass, which checks keys and
    // depth as it reads, where the DOM parser reads the whole text first. On a document that is
    // duplicate-keyed and then malformed the strict function still reports the syntax error, as
    // it does without a template, and a well-formed duplicate-keyed document still fails on its
    // key in both.
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    auto paths = std::make_shared<const VariantTemplatePaths>(std::vector<VariantTemplatePath> {
            {.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL128I}});
    const DataTypePtr typed = std::make_shared<DataTypeVariantV2>(0, false, paths);
    const std::string syntax_error = "Failed to parse JSON as Variant";

    const std::string malformed = R"({"o":{"a":1,"a":2},"k":x})";
    ExecutionResult plain =
            execute_parse("parse_to_variant", make_strings({malformed}), string_type, 1);
    ExecutionResult templated =
            execute_parse("parse_to_variant", make_strings({malformed}), string_type, 1, typed);
    EXPECT_FALSE(plain.status.ok());
    EXPECT_FALSE(templated.status.ok());
    EXPECT_NE(plain.status.to_string().find(syntax_error), std::string::npos)
            << plain.status.to_string();
    EXPECT_NE(templated.status.to_string().find(syntax_error), std::string::npos)
            << templated.status.to_string();

    const std::string duplicate = R"({"o":{"a":1,"a":2}})";
    ExecutionResult plain_duplicate =
            execute_parse("parse_to_variant", make_strings({duplicate}), string_type, 1);
    ExecutionResult templated_duplicate =
            execute_parse("parse_to_variant", make_strings({duplicate}), string_type, 1, typed);
    EXPECT_FALSE(plain_duplicate.status.ok());
    EXPECT_FALSE(templated_duplicate.status.ok());
    EXPECT_EQ(plain_duplicate.status.to_string().find(syntax_error) != std::string::npos,
              templated_duplicate.status.to_string().find(syntax_error) != std::string::npos)
            << plain_duplicate.status.to_string() << " / "
            << templated_duplicate.status.to_string();
}

TEST(FunctionVariantParseTest, ErrorToNullOnlyNullsRecoverableFailures) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    ExecutionResult result =
            execute_parse("try_parse_to_variant",
                          make_strings({R"({"before":1})", "{", "null", "hello", R"({"after":2})"}),
                          string_type, 5);
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    EXPECT_FALSE(is_sql_null_at(result.output, 0));
    EXPECT_TRUE(is_sql_null_at(result.output, 1));
    EXPECT_FALSE(is_sql_null_at(result.output, 2));
    EXPECT_EQ(variant_json_at(result.output, 2), "null");
    EXPECT_TRUE(is_sql_null_at(result.output, 3));
    EXPECT_EQ(variant_json_at(result.output, 4), R"({"after":2})");
}

TEST(FunctionVariantParseTest, ConfiguredInputValidationUsesFailOrOuterNull) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    {
        ScopedValue key_limit(config::variant_max_json_key_length, 3);
        ExecutionResult fail =
                execute_parse("parse_to_variant", make_strings({R"({"abcd":1})"}), string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        EXPECT_FALSE(static_cast<bool>(fail.output));
        ExecutionResult null = execute_parse("try_parse_to_variant",
                                             make_strings({R"({"abcd":1})"}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
    {
        ExecutionResult fail = execute_parse("parse_to_variant", make_strings({R"({"a":1,"a":2})"}),
                                             string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        ExecutionResult null = execute_parse("try_parse_to_variant",
                                             make_strings({R"({"a":1,"a":2})"}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
    {
        const std::string invalid_utf8(1, static_cast<char>(0xFF));
        ExecutionResult fail =
                execute_parse("parse_to_variant", make_strings({invalid_utf8}), string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        ExecutionResult null =
                execute_parse("try_parse_to_variant", make_strings({invalid_utf8}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
    {
        // The DOM parser cannot read an integer beyond 64 bits, so the document is invalid JSON.
        ExecutionResult fail = execute_parse(
                "parse_to_variant", make_strings({"18446744073709551616"}), string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        ExecutionResult null =
                execute_parse("try_parse_to_variant",
                              make_strings({R"({"n":18446744073709551616})"}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
    {
        const std::string too_deep = nested_array_json(VARIANT_MAX_NESTING_DEPTH + 1);
        ExecutionResult fail =
                execute_parse("parse_to_variant", make_strings({too_deep}), string_type, 1);
        EXPECT_FALSE(fail.status.ok());
        ExecutionResult null =
                execute_parse("try_parse_to_variant", make_strings({too_deep}), string_type, 1);
        ASSERT_TRUE(null.status.ok()) << null.status.to_string();
        EXPECT_TRUE(is_sql_null_at(null.output, 0));
    }
}

TEST(FunctionVariantParseTest, DecimalTemplatePathsOnTheResultTypeParseNumbersFromText) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    auto paths = std::make_shared<const VariantTemplatePaths>(std::vector<VariantTemplatePath> {
            {.pattern = "money.amount", .is_glob = false, .type = TYPE_DECIMAL128I}});
    const std::string document = R"({"money":{"amount":123456789012.345678},"score":0.5})";
    for (const char* name : {"parse_to_variant", "try_parse_to_variant"}) {
        DataTypePtr typed = std::make_shared<DataTypeVariantV2>(0, false, paths);
        if (std::string_view(name) == "try_parse_to_variant") {
            typed = make_nullable(typed);
        }
        ExecutionResult result =
                execute_parse(name, make_strings({document}), string_type, 1, typed);
        ASSERT_TRUE(result.status.ok()) << name << ": " << result.status.to_string();
        const std::string json = variant_json_at(result.output, 0);
        EXPECT_NE(std::string::npos, json.find(R"("amount":123456789012.345678)")) << name << json;
        EXPECT_NE(std::string::npos, json.find(R"("score":0.5)")) << name << json;
    }
    // Without paths on the result type the number stays a number.
    ExecutionResult plain =
            execute_parse("parse_to_variant", make_strings({document}), string_type, 1);
    ASSERT_TRUE(plain.status.ok()) << plain.status.to_string();
    EXPECT_EQ(std::string::npos, variant_json_at(plain.output, 0).find("123456789012.345678"));
}

TEST(FunctionVariantParseTest, DecimalTemplatePathsStopAtAnArrayInsideAnArray) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    auto paths = std::make_shared<const VariantTemplatePaths>(std::vector<VariantTemplatePath> {
            {.pattern = "prices", .is_glob = false, .type = TYPE_DECIMAL128I}});
    const DataTypePtr typed = std::make_shared<DataTypeVariantV2>(0, false, paths);
    // Scalars directly inside the declared array are the ARRAY<DECIMAL> field's: read from text.
    ExecutionResult direct = execute_parse(
            "parse_to_variant", make_strings({R"({"prices":[1.50]})"}), string_type, 1, typed);
    ASSERT_TRUE(direct.status.ok()) << direct.status.to_string();
    EXPECT_NE(std::string::npos, variant_json_at(direct.output, 0).find(R"("prices":[1.50])"));
    // An array nested inside it is off the template's path like an object element: the template
    // addresses an array as a leaf, so its number keeps the parser's double, rendered 1.5.
    ExecutionResult nested = execute_parse(
            "parse_to_variant", make_strings({R"({"prices":[[1.50]]})"}), string_type, 1, typed);
    ASSERT_TRUE(nested.status.ok()) << nested.status.to_string();
    const std::string json = variant_json_at(nested.output, 0);
    EXPECT_NE(std::string::npos, json.find(R"("prices":[[1.5]])")) << json;
    EXPECT_EQ(std::string::npos, json.find("1.50")) << json;
}

TEST(FunctionVariantParseTest, DecimalTemplatePathsDoNotRescueALeadingZero) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    auto paths = std::make_shared<const VariantTemplatePaths>(std::vector<VariantTemplatePath> {
            {.pattern = "money.amount", .is_glob = false, .type = TYPE_DECIMAL128I}});
    // "01" is not a JSON number; a declared path does not change the document's verdict.
    const std::string document = R"({"money":{"amount":01}})";
    ExecutionResult failure =
            execute_parse("parse_to_variant", make_strings({document}), string_type, 1,
                          std::make_shared<DataTypeVariantV2>(0, false, paths));
    EXPECT_FALSE(failure.status.ok());
    ExecutionResult recoverable =
            execute_parse("try_parse_to_variant", make_strings({document}), string_type, 1,
                          make_nullable(std::make_shared<DataTypeVariantV2>(0, false, paths)));
    ASSERT_TRUE(recoverable.status.ok()) << recoverable.status.to_string();
    EXPECT_TRUE(is_sql_null_at(recoverable.output, 0));
}

TEST(FunctionVariantParseTest, LargeIntTemplatePathsReadAnIntegerBeyond64BitsFromItsText) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    auto paths = std::make_shared<const VariantTemplatePaths>(std::vector<VariantTemplatePath> {
            {.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
            {.pattern = "ids", .is_glob = false, .type = TYPE_LARGEINT},
            {.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL64}});
    // document, and the JSON its value renders back to
    const std::vector<std::pair<std::string, std::string>> cases = {
            // 2^63 fits a uint64: the parser reads it as before, without asking the template
            {R"({"id":9223372036854775808})", R"({"id":9223372036854775808})"},
            // 2^64 and -2^63 - 1 fit no 64-bit type: read from the text, kept as an exact integer
            {R"({"id":18446744073709551616})", R"({"id":18446744073709551616})"},
            {R"({"id":-9223372036854775809})", R"({"id":-9223372036854775809})"},
            // 10^38 - 1 is the largest integer a Variant decimal16 holds
            {R"({"id":99999999999999999999999999999999999999})",
             R"({"id":99999999999999999999999999999999999999})"},
            // above it a LARGEINT is kept as its digits: 10^38, 2^127 - 1 and -2^127
            {R"({"id":100000000000000000000000000000000000000})",
             R"({"id":"100000000000000000000000000000000000000"})"},
            {R"({"id":170141183460469231731687303715884105727})",
             R"({"id":"170141183460469231731687303715884105727"})"},
            {R"({"id":-170141183460469231731687303715884105728})",
             R"({"id":"-170141183460469231731687303715884105728"})"},
            // 2^127 is no LARGEINT: the token is kept as it is and the template's cast decides
            {R"({"id":170141183460469231731687303715884105728})",
             R"({"id":"170141183460469231731687303715884105728"})"},
            // the scalars directly inside a declared array are the ARRAY<LARGEINT> field's
            {R"({"ids":[18446744073709551616,1]})", R"({"ids":[18446744073709551616,1]})"},
            // a fraction is not an integer as written: the parser's double stays
            {R"({"id":1.5})", R"({"id":1.5})"},
            // a DECIMAL path of the same template still reads its number from the text
            {R"({"amount":1.50,"id":18446744073709551616})",
             R"({"amount":1.50,"id":18446744073709551616})"},
    };
    std::vector<std::string> documents;
    for (const auto& document_and_rendering : cases) {
        documents.push_back(document_and_rendering.first);
    }
    for (const char* name : {"parse_to_variant", "try_parse_to_variant"}) {
        DataTypePtr typed = std::make_shared<DataTypeVariantV2>(0, false, paths);
        if (std::string_view(name) == "try_parse_to_variant") {
            typed = make_nullable(typed);
        }
        ExecutionResult result =
                execute_parse(name, make_strings(documents), string_type, documents.size(), typed);
        ASSERT_TRUE(result.status.ok()) << name << ": " << result.status.to_string();
        for (size_t row = 0; row < cases.size(); ++row) {
            ASSERT_FALSE(is_sql_null_at(result.output, row)) << name << ": " << cases[row].first;
            EXPECT_EQ(cases[row].second, variant_json_at(result.output, row))
                    << name << ": " << cases[row].first;
        }
    }
}

TEST(FunctionVariantParseTest, ALargeIntOnlyTemplateLeavesWhatTheDomParserReadsAsItWas) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    // No DECIMAL field: the DOM parser goes first and the text read gets only a document it
    // rejects, so a document the DOM parser reads comes out as it does without a template.
    auto paths = std::make_shared<const VariantTemplatePaths>(std::vector<VariantTemplatePath> {
            {.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
            {.pattern = "ids", .is_glob = false, .type = TYPE_LARGEINT},
            {.pattern = "a.*", .is_glob = true, .type = TYPE_LARGEINT}});
    ASSERT_FALSE(paths->reads_decimal_text());
    const std::vector<std::string> readable = {
            R"({"id":1,"ids":[1,-2,3],"name":"x"})",
            // 2^63 and -2^63: a uint64 and an int64
            R"({"id":9223372036854775808,"ids":[-9223372036854775808]})",
            // doubles on the declared paths stay doubles
            R"({"id":1.5,"ids":[2.5,1e3],"a":{"b":0.1}})",
            R"({"a":{"b":1,"c":[{"d":2}]},"other":null,"t":true})",
            R"([1,2.5,"s",{"id":3}])",
            R"("text")",
            "42",
    };
    const ExecutionResult plain =
            execute_parse("parse_to_variant", make_strings(readable), string_type, readable.size());
    ASSERT_TRUE(plain.status.ok()) << plain.status.to_string();
    const ExecutionResult typed =
            execute_parse("parse_to_variant", make_strings(readable), string_type, readable.size(),
                          std::make_shared<DataTypeVariantV2>(0, false, paths));
    ASSERT_TRUE(typed.status.ok()) << typed.status.to_string();
    for (size_t row = 0; row < readable.size(); ++row) {
        EXPECT_EQ(variant_json_at(plain.output, row), variant_json_at(typed.output, row))
                << readable[row];
        EXPECT_EQ(variant_bytes_at(plain.output, row), variant_bytes_at(typed.output, row))
                << readable[row];
    }

    // One encoder alternating the two reads: a document only the text read takes, one neither
    // takes (NULL from try_parse_to_variant, its partial row rolled back) and one the DOM parser
    // reads, each followed by the others.
    const std::vector<std::pair<std::string, std::optional<std::string>>> mixed = {
            {R"({"id":1})", R"({"id":1})"},
            {R"({"id":18446744073709551616})", R"({"id":18446744073709551616})"},
            {R"({"id":2.5})", R"({"id":2.5})"},
            {R"({"id":18446744073709551616,"other":18446744073709551616})", std::nullopt},
            {R"({"ids":[1,170141183460469231731687303715884105727]})",
             R"({"ids":[1,"170141183460469231731687303715884105727"]})"},
            {R"({"x":"s"})", R"({"x":"s"})"},
            {R"({"a":{"b":-18446744073709551616}})", R"({"a":{"b":-18446744073709551616}})"},
    };
    std::vector<std::string> documents;
    for (const auto& document_and_rendering : mixed) {
        documents.push_back(document_and_rendering.first);
    }
    const ExecutionResult result = execute_parse(
            "try_parse_to_variant", make_strings(documents), string_type, documents.size(),
            make_nullable(std::make_shared<DataTypeVariantV2>(0, false, paths)));
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    for (size_t row = 0; row < mixed.size(); ++row) {
        if (!mixed[row].second.has_value()) {
            EXPECT_TRUE(is_sql_null_at(result.output, row)) << mixed[row].first;
            continue;
        }
        ASSERT_FALSE(is_sql_null_at(result.output, row)) << mixed[row].first;
        EXPECT_EQ(*mixed[row].second, variant_json_at(result.output, row)) << mixed[row].first;
    }
}

TEST(FunctionVariantParseTest, AnIntegerBeyond64BitsOffALargeIntPathKeepsTheDocumentsVerdict) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    auto paths = std::make_shared<const VariantTemplatePaths>(std::vector<VariantTemplatePath> {
            {.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
            {.pattern = "a.id", .is_glob = false, .type = TYPE_LARGEINT},
            {.pattern = "ids", .is_glob = false, .type = TYPE_LARGEINT},
            {.pattern = "n", .is_glob = false, .type = TYPE_BIGINT}});
    // The DOM parser cannot read an integer beyond 64 bits, so each document is invalid JSON as
    // it is without a template: an error from parse_to_variant, NULL from try_parse_to_variant.
    for (const char* document : {
                 // on no template path
                 R"({"other":18446744073709551616})",
                 // on a path declared BIGINT
                 R"({"n":18446744073709551616})",
                 // after a LARGEINT path was already read: the row is rolled back
                 R"({"id":18446744073709551616,"other":18446744073709551616})",
                 // under an object that is an array element nothing is on a declared path
                 R"({"a":[{"id":18446744073709551616}]})",
                 // nor in an array nested inside the declared array
                 R"({"ids":[[18446744073709551616]]})",
                 // a root scalar has no path
                 "18446744073709551616",
                 // not JSON integers: a declared LARGEINT path does not rescue them
                 R"({"id":01})",
                 R"({"id":-01})",
                 R"({"id":1e400})",
         }) {
        ExecutionResult failure =
                execute_parse("parse_to_variant", make_strings({document}), string_type, 1,
                              std::make_shared<DataTypeVariantV2>(0, false, paths));
        EXPECT_FALSE(failure.status.ok()) << document;
        ExecutionResult recoverable =
                execute_parse("try_parse_to_variant", make_strings({document}), string_type, 1,
                              make_nullable(std::make_shared<DataTypeVariantV2>(0, false, paths)));
        ASSERT_TRUE(recoverable.status.ok()) << document << recoverable.status.to_string();
        EXPECT_TRUE(is_sql_null_at(recoverable.output, 0)) << document;
    }
    // The same paths through objects only are declared and exact.
    ExecutionResult declared = execute_parse(
            "parse_to_variant", make_strings({R"({"a":{"id":18446744073709551616}})"}), string_type,
            1, std::make_shared<DataTypeVariantV2>(0, false, paths));
    ASSERT_TRUE(declared.status.ok()) << declared.status.to_string();
    EXPECT_EQ(R"({"a":{"id":18446744073709551616}})", variant_json_at(declared.output, 0));
}

namespace {

TVariantTemplatePath template_field(const std::string& pattern, bool is_glob,
                                    TPrimitiveType::type type) {
    TVariantTemplatePath field;
    field.__set_pattern(pattern);
    field.__set_is_glob(is_glob);
    field.__set_type(type);
    return field;
}

// The expression node of a parse call as the FE sends it: the template rides on the result type.
VExprSPtr parse_call(const std::string& function_name,
                     const std::vector<TVariantTemplatePath>& template_paths) {
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::VARIANT);
    scalar_type.__set_variant_max_subcolumns_count(0);
    scalar_type.__set_variant_is_v2(true);
    if (!template_paths.empty()) {
        scalar_type.__set_variant_template_paths(template_paths);
    }
    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    TTypeDesc type_desc;
    type_desc.types.push_back(type_node);

    TFunctionName name;
    name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(type_desc);
    node.__set_fn(fn);
    node.__set_num_children(0);
    node.__set_is_nullable(false);
    return VectorizedFnCall::create_shared(node);
}

} // namespace

TEST(FunctionVariantParseTest, TheTemplateOnTheResultTypeIsPartOfTheCallsDigest) {
    // The digest keys the scan's condition cache. A parse call reads a number by the template on
    // its result type, so two calls that differ only there filter differently and must not share
    // a key: under one, cast(v['amount'] as string) = '1.5' keeps {"amount":1.50}; under the
    // other it does not.
    const uint64_t seed = 42;
    const uint64_t untyped = parse_call("parse_to_variant", {})->get_digest(seed);
    const uint64_t decimal =
            parse_call("parse_to_variant",
                       {template_field("amount", false, TPrimitiveType::DECIMAL64)})
                    ->get_digest(seed);
    ASSERT_NE(0, untyped);
    ASSERT_NE(0, decimal);
    EXPECT_NE(untyped, decimal);
    // the same template gives the same key
    EXPECT_EQ(decimal, parse_call("parse_to_variant",
                                  {template_field("amount", false, TPrimitiveType::DECIMAL64)})
                               ->get_digest(seed));
    // every part of a field counts: its pattern, how the pattern matches, its type
    EXPECT_NE(decimal, parse_call("parse_to_variant",
                                  {template_field("amounts", false, TPrimitiveType::DECIMAL64)})
                               ->get_digest(seed));
    EXPECT_NE(decimal, parse_call("parse_to_variant",
                                  {template_field("amount", true, TPrimitiveType::DECIMAL64)})
                               ->get_digest(seed));
    EXPECT_NE(decimal, parse_call("parse_to_variant",
                                  {template_field("amount", false, TPrimitiveType::LARGEINT)})
                               ->get_digest(seed));
    // and so does where one pattern ends and the next begins
    EXPECT_NE(
            parse_call("parse_to_variant", {template_field("ab", false, TPrimitiveType::DECIMAL64),
                                            template_field("c", false, TPrimitiveType::DECIMAL64)})
                    ->get_digest(seed),
            parse_call("parse_to_variant", {template_field("a", false, TPrimitiveType::DECIMAL64),
                                            template_field("bc", false, TPrimitiveType::DECIMAL64)})
                    ->get_digest(seed));
    // the other function keeps its own key, with a template and without
    EXPECT_NE(untyped, parse_call("try_parse_to_variant", {})->get_digest(seed));
    EXPECT_NE(decimal, parse_call("try_parse_to_variant",
                                  {template_field("amount", false, TPrimitiveType::DECIMAL64)})
                               ->get_digest(seed));
}

} // namespace doris
