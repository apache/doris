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

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/variant/variant_decimal_paths.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"

namespace doris {
namespace {

Status deserialize_json(const DataTypeVariantV2SerDe& serde, ColumnVariantV2& column,
                        std::string_view text) {
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    return serde.deserialize_one_cell_from_json(column, slice, options);
}

Status deserialize_csv(const DataTypeVariantV2SerDe& serde, ColumnVariantV2& column,
                       std::string_view text) {
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    return serde.deserialize_one_cell_from_csv(column, slice, options);
}

std::string json_at(const DataTypeVariantV2SerDe& serde, const IColumn& column, size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.serialize_one_cell_to_json(column, row, writer, options);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok()) {
        return {};
    }
    writer.commit();
    return output->get_data_at(0).to_string();
}

VariantDecimalPathSetPtr template_of(std::vector<VariantTemplatePath> fields) {
    return std::make_shared<const VariantDecimalPathSet>(std::move(fields));
}

ColumnVariantV2::MutablePtr typed_int(int32_t value) {
    auto nested = ColumnInt32::create();
    nested->insert_value(value);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(nested), std::move(nulls)),
            std::make_shared<DataTypeInt32>());
}

} // namespace

TEST(DataTypeVariantV2SerdeInputTest, JsonUsesT15ValidEmptyAndInvalidPolicies) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    // The serde re-reads JSON that it wrote, so text the parser rejects keeps its text instead of
    // failing, for example an integer beyond 64 bits in a serialized aggregate state.
    EXPECT_TRUE(deserialize_json(serde, *column, R"({"b":[1,null]})").ok());
    EXPECT_TRUE(deserialize_json(serde, *column, {}).ok());
    EXPECT_TRUE(deserialize_json(serde, *column, "not-json").ok());
    EXPECT_TRUE(deserialize_json(serde, *column, "18446744073709551616").ok());
    ASSERT_EQ(column->size(), 4);
    EXPECT_EQ(json_at(serde, *column, 0), R"({"b":[1,null]})");
    EXPECT_EQ(json_at(serde, *column, 1), R"("")");
    EXPECT_EQ(json_at(serde, *column, 2), R"("not-json")");
    EXPECT_EQ(json_at(serde, *column, 3), R"("18446744073709551616")");

    const std::string long_key(300, 'k');
    const std::string long_key_json = R"({")" + long_key + R"(":1})";
    ASSERT_TRUE(deserialize_json(serde, *column, long_key_json).ok());
    EXPECT_EQ(json_at(serde, *column, 4), long_key_json);

    const size_t before = column->size();
    const std::string invalid_utf8("\xC3\x28", 2);
    EXPECT_EQ(deserialize_json(serde, *column, invalid_utf8).code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), before);
}

TEST(DataTypeVariantV2SerdeInputTest, VectorFailureIsAtomicAndCounterIsNotAdvanced) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"sentinel":1})").ok());
    const size_t before = column->size();

    std::string valid = R"({"a":1})";
    std::string invalid("\xC3\x28", 2);
    std::vector<Slice> slices {{valid.data(), valid.size()}, {invalid.data(), invalid.size()}};
    uint64_t num_deserialized = 0;
    DataTypeSerDe::FormatOptions options;
    const Status status =
            serde.deserialize_column_from_json_vector(*column, slices, &num_deserialized, options);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), before);
    EXPECT_EQ(num_deserialized, 0);
    EXPECT_EQ(json_at(serde, *column, 0), R"({"sentinel":1})");
}

TEST(DataTypeVariantV2SerdeInputTest, EmptyVectorAndCounterOverflowDoNotMutateState) {
    DataTypeVariantV2SerDe serde;
    auto column = typed_int(1);
    DataTypeSerDe::FormatOptions options;
    std::vector<Slice> empty;
    uint64_t counter = 7;
    EXPECT_TRUE(serde.deserialize_column_from_json_vector(*column, empty, &counter, options).ok());
    EXPECT_EQ(counter, 7);
    EXPECT_EQ(column->size(), 1);
    EXPECT_TRUE(column->is_typed());

    std::string value = "2";
    std::vector<Slice> one {{value.data(), value.size()}};
    counter = std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(serde.deserialize_column_from_json_vector(*column, one, &counter, options).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(counter, std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(column->size(), 1);
    EXPECT_TRUE(column->is_typed());
    EXPECT_EQ(json_at(serde, *column, 0), "1");
}

TEST(DataTypeVariantV2SerdeInputTest, SuccessfulVectorAdvancesExistingCounter) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    std::string first = "1";
    std::string second = R"({"a":2})";
    std::vector<Slice> slices {{first.data(), first.size()}, {second.data(), second.size()}};
    uint64_t counter = 5;
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(serde.deserialize_column_from_json_vector(*column, slices, &counter, options).ok());
    EXPECT_EQ(counter, 7);
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(json_at(serde, *column, 0), "1");
    EXPECT_EQ(json_at(serde, *column, 1), R"({"a":2})");
}

TEST(DataTypeVariantV2SerdeInputTest, CsvIsAlwaysRawStringScalar) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_csv(serde, *column, R"({"a":1})").ok());
    ASSERT_TRUE(deserialize_csv(serde, *column, "null").ok());
    EXPECT_EQ(json_at(serde, *column, 0), R"("{\"a\":1}")");
    EXPECT_EQ(json_at(serde, *column, 1), R"("null")");
}

TEST(DataTypeVariantV2SerdeInputTest, CsvUnescapesStringScalarBeforeEncoding) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    std::string text = "left,right\"\"quoted\\\\path\nnext";
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    options.quote_char = '"';
    ASSERT_TRUE(serde.deserialize_one_cell_from_csv(*column, slice, options).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(json_at(serde, *column, 0), R"("left,right\"quoted\\path\nnext")");
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvPreservesSqlNullAndRawStringScalar) {
    DataTypeVariantV2SerDe serde;
    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    DataTypeSerDe::FormatOptions options;
    options.null_format = "NULL";
    options.null_len = 4;

    std::string sql_null = "NULL";
    Slice sql_null_slice(sql_null.data(), sql_null.size());
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, sql_null_slice, options).ok());

    std::string raw_null = "null";
    Slice raw_null_slice(raw_null.data(), raw_null.size());
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, raw_null_slice, options).ok());

    options.converted_from_string = true;
    std::string quoted_null_format = R"("NULL")";
    Slice quoted_null_format_slice(quoted_null_format.data(), quoted_null_format.size());
    ASSERT_TRUE(
            nullable_serde.deserialize_one_cell_from_csv(*outer, quoted_null_format_slice, options)
                    .ok());

    ASSERT_EQ(outer->size(), 3);
    EXPECT_TRUE(outer->is_null_at(0));
    EXPECT_FALSE(outer->is_null_at(1));
    EXPECT_FALSE(outer->is_null_at(2));
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 1), R"("null")");
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 2), R"("NULL")");
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvNestedFailureAppendsOneSqlNull) {
    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    Slice invalid(static_cast<char*>(nullptr), 1);
    DataTypeSerDe::FormatOptions options;
    options.null_len = 0;
    options.escape_char = '\\';

    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, invalid, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_TRUE(outer->is_null_at(0));
    EXPECT_EQ(outer->get_nested_column().size(), 1);
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvDispatchesForNonVariantNestedSerde) {
    MutableColumnPtr nested = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeStringSerDe>(TYPE_STRING));
    std::string text = R"(left""right\\tail)";
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    options.quote_char = '"';

    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, slice, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_FALSE(outer->is_null_at(0));
    EXPECT_EQ(assert_cast<const ColumnString&>(outer->get_nested_column()).get_data_at(0),
              StringRef("left\"right\\tail"));
}

TEST(DataTypeVariantV2SerdeInputTest, InvalidCsvInputDoesNotPublishARow) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_csv(serde, *column, "sentinel").ok());
    const size_t before = column->size();
    Slice invalid(static_cast<char*>(nullptr), 1);
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    EXPECT_EQ(serde.deserialize_one_cell_from_csv(*column, invalid, options).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), before);
    EXPECT_EQ(json_at(serde, *column, 0), R"("sentinel")");
}

TEST(DataTypeVariantV2SerdeInputTest, OuterSqlNullAndVariantNullRemainDistinct) {
    DataTypeVariantV2SerDe serde;
    auto variant_null = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *variant_null, "null").ok());
    EXPECT_EQ(json_at(serde, *variant_null, 0), "null");

    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    std::string sql_null = "\\N";
    Slice slice(sql_null.data(), sql_null.size());
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_json(*outer, slice, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_TRUE(outer->is_null_at(0));
}

TEST(DataTypeVariantV2SerdeInputTest, PbAndArrowReadKeepExplicitGuards) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    PValues values;
    EXPECT_EQ(serde.write_column_to_pb(*column, values, 0, 0).code(),
              ErrorCode::NOT_IMPLEMENTED_ERROR);
    EXPECT_EQ(serde.read_column_from_pb(*column, values).code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    // Native Arrow reads are supported; a missing array is an invalid request, not a missing codec.
    EXPECT_EQ(serde.read_column_from_arrow(*column, nullptr, 0, 0, cctz::utc_time_zone()).code(),
              ErrorCode::INVALID_ARGUMENT);
}

TEST(DataTypeVariantV2SerdeInputTest, DecimalTemplatePathsParseNumbersFromText) {
    auto paths = std::make_shared<const VariantDecimalPathSet>(std::vector<VariantTemplatePath> {
            {.pattern = "money.amount", .is_glob = false, .type = TYPE_DECIMAL128I},
            {.pattern = "ratio*", .is_glob = true, .type = TYPE_DECIMAL128I}});
    DataTypeVariantV2SerDe serde(1, paths);
    EXPECT_EQ(paths, serde.decimal_paths());
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *column,
                                 R"({"money":{"amount":123456789012.345678,"ccy":"INR"},)"
                                 R"("ratio_a":0.123456789012345678901,"score":0.5,"n":7})")
                        .ok());
    const std::string json = json_at(serde, *column, 0);
    // The declared DECIMAL paths hold the number exactly, the rest is parsed as before.
    EXPECT_NE(std::string::npos, json.find(R"("amount":123456789012.345678)")) << json;
    EXPECT_NE(std::string::npos, json.find(R"("ratio_a":0.123456789012345678901)")) << json;
    EXPECT_NE(std::string::npos, json.find(R"("ccy":"INR")")) << json;
    EXPECT_NE(std::string::npos, json.find(R"("score":0.5)")) << json;
    EXPECT_NE(std::string::npos, json.find(R"("n":7)")) << json;

    // The vector entry point takes the same route.
    std::vector<std::string> texts {
            R"({"money":{"amount":1.000001}})", R"({"money":{"amount":2}})",
            R"({"money":{"amount":1.5e3}})", R"({"money":{"amount":123456789012345678901}})",
            R"({"money":{"amount":0.1234567890123456789012345678901234567890123}})"};
    std::vector<Slice> slices;
    for (const std::string& text : texts) {
        slices.emplace_back(text.data(), text.size());
    }
    uint64_t deserialized = 0;
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(serde.deserialize_column_from_json_vector(*column, slices, &deserialized, options)
                        .ok());
    EXPECT_EQ(5, deserialized);
    EXPECT_NE(std::string::npos, json_at(serde, *column, 1).find(R"("amount":1.000001)"));
    EXPECT_NE(std::string::npos, json_at(serde, *column, 2).find(R"("amount":2)"));
    // An exponent form and an integer beyond 64 bits are exact on the path as well.
    EXPECT_NE(std::string::npos, json_at(serde, *column, 3).find(R"("amount":1500)"));
    EXPECT_NE(std::string::npos,
              json_at(serde, *column, 4).find(R"("amount":123456789012345678901)"));
    // Beyond 38 significant digits the text is kept for the template's cast to decide.
    EXPECT_NE(std::string::npos,
              json_at(serde, *column, 5)
                      .find(R"("amount":"0.1234567890123456789012345678901234567890123")"));

    // A serde without paths parses the same number through a binary64, as before.
    DataTypeVariantV2SerDe plain;
    EXPECT_EQ(nullptr, plain.decimal_paths());
    auto plain_column = ColumnVariantV2::create();
    ASSERT_TRUE(
            deserialize_json(plain, *plain_column, R"({"ratio_a":0.123456789012345678901})").ok());
    EXPECT_EQ(std::string::npos, json_at(plain, *plain_column, 0).find("0.123456789012345678901"));

    // Off the declared paths a path-bearing serde keeps the parser's verdict: an integer beyond
    // 64 bits is not a value the parser reads back, so the text stays a string row.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"n":123456789012345678901})").ok());
    EXPECT_EQ('"', json_at(serde, *column, 6).front());

    // Invalid text on a path-bearing serde is still the parser's verdict on the original text: the
    // serde reads leniently, so the text stays a string row.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"money":{"amount":1.5)").ok());
    const std::string fallback = json_at(serde, *column, 7);
    EXPECT_EQ('"', fallback.front()) << fallback;
    EXPECT_NE(std::string::npos, fallback.find("amount")) << fallback;
}

TEST(DataTypeVariantV2SerdeInputTest, DecimalTemplatePathsAtTheEdgesOfTheTextReader) {
    DataTypeVariantV2SerDe serde(
            1,
            template_of({{.pattern = "money.amount", .is_glob = false, .type = TYPE_DECIMAL128I}}));
    auto column = ColumnVariantV2::create();
    // An exponent a double cannot hold is valid JSON the DOM parser rejected as a whole; on a
    // declared path the text is now kept as it is, for the template's cast to decide.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"money":{"amount":1e400}})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 0).find(R"("amount":"1e400")"));
    // Beyond 38 digits once the exponent is folded in, either way: text as well.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"money":{"amount":1e40}})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 1).find(R"("amount":"1e40")"));
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"money":{"amount":1e-40}})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 2).find(R"("amount":"1e-40")"));
    // A decimal has no negative zero; the scale the text carries stays.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"money":{"amount":-0.0}})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 3).find(R"("amount":0.0)"));
    // A leading zero is not a JSON number: the declared path does not rescue it, and the lenient
    // serde keeps the whole text as a string row, as it does with the number off the path.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"money":{"amount":01}})").ok());
    EXPECT_EQ('"', json_at(serde, *column, 4).front());
    // A document that fails after the declared path was already written rolls the partial row
    // back and takes the DOM parser's verdict on the whole text: a string row.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"money":{"amount":1.5},"n":01})").ok());
    const std::string after_leading_zero = json_at(serde, *column, 5);
    EXPECT_EQ('"', after_leading_zero.front()) << after_leading_zero;
    EXPECT_NE(std::string::npos, after_leading_zero.find("amount")) << after_leading_zero;
    ASSERT_TRUE(deserialize_json(serde, *column,
                                 R"({"money":{"amount":1.5},"n":123456789012345678901})")
                        .ok());
    const std::string after_big_integer = json_at(serde, *column, 6);
    EXPECT_EQ('"', after_big_integer.front()) << after_big_integer;
    EXPECT_NE(std::string::npos, after_big_integer.find("amount")) << after_big_integer;
}

TEST(DataTypeVariantV2SerdeInputTest, DecimalTemplatePathsStopAtAnObjectInsideAnArray) {
    DataTypeVariantV2SerDe serde(
            1, template_of({{.pattern = "a.b", .is_glob = false, .type = TYPE_DECIMAL128I},
                            {.pattern = "a.b.c", .is_glob = false, .type = TYPE_DECIMAL128I},
                            {.pattern = "prices", .is_glob = false, .type = TYPE_DECIMAL128I}}));
    auto column = ColumnVariantV2::create();
    // The shredder treats the array under "a" as the leaf and never types the members of its
    // element objects, so their numbers are parsed as before: doubles, rendered 1.5.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"a":[{"b":1.50}]})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 0).find(R"("b":1.5)"));
    EXPECT_EQ(std::string::npos, json_at(serde, *column, 0).find(R"("b":1.50)"));
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"a":[{"b":{"c":1.50}}]})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 1).find(R"("c":1.5)"));
    EXPECT_EQ(std::string::npos, json_at(serde, *column, 1).find(R"("c":1.50)"));
    // The same path through objects only is declared and exact.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"a":{"b":1.50}})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 2).find(R"("b":1.50)"));
    // Scalars directly inside an array keep the array's path: an ARRAY<DECIMAL> field.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"prices":[1.50,2.5]})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 3).find(R"("prices":[1.50,2.5])"));
    // Back out of the array, a sibling path is matched again.
    ASSERT_TRUE(
            deserialize_json(serde, *column, R"({"x":[{"a":{"b":1.50}}],"a":{"b":1.50}})").ok());
    const std::string sibling = json_at(serde, *column, 4);
    EXPECT_NE(std::string::npos, sibling.find(R"("x":[{"a":{"b":1.5}}])")) << sibling;
    EXPECT_NE(std::string::npos, sibling.find(R"("a":{"b":1.50})")) << sibling;
    // An array nested inside the declared array is off the path like an object element.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"prices":[[1.50]]})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 5).find(R"("prices":[[1.5]])"));
    EXPECT_EQ(std::string::npos, json_at(serde, *column, 5).find("1.50"));
}

TEST(DataTypeVariantV2SerdeInputTest, DecimalTemplatePathsKeepTheLenientVerdictOnAMalformedTail) {
    DataTypeVariantV2SerDe serde(
            1, template_of({{.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL128I}}));
    auto column = ColumnVariantV2::create();
    // The ondemand pass checks depth and keys as it reads, the DOM parser rejects the malformed
    // tail first and the lenient serde keeps the text: both documents must become string rows.
    const std::string deep_then_malformed =
            R"({"d":)" + std::string(130, '[') + "0" + std::string(130, ']') + R"(,"k":x})";
    ASSERT_TRUE(deserialize_json(serde, *column, deep_then_malformed).ok());
    EXPECT_EQ('"', json_at(serde, *column, 0).front());
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"o":{"a":1,"a":2},"k":x})").ok());
    EXPECT_EQ('"', json_at(serde, *column, 1).front());
    // A well-formed over-deep or duplicate-keyed document fails as it always did: the DOM
    // collector runs the same checks.
    const std::string deep_and_valid =
            R"({"d":)" + std::string(130, '[') + "0" + std::string(130, ']') + "}";
    EXPECT_FALSE(deserialize_json(serde, *column, deep_and_valid).ok());
    EXPECT_FALSE(deserialize_json(serde, *column, R"({"o":{"a":1,"a":2}})").ok());
    // and a later row still takes the text path
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"amount":1.50})").ok());
    EXPECT_NE(std::string::npos, json_at(serde, *column, 2).find(R"("amount":1.50)"));
}

TEST(DataTypeVariantV2SerdeInputTest, DataTypeHandsItsDecimalPathsToTheSerde) {
    auto paths = std::make_shared<const VariantDecimalPathSet>(std::vector<VariantTemplatePath> {
            {.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL128I}});
    DataTypeVariantV2 typed(0, false, paths);
    DataTypeVariantV2 plain(0, false);
    EXPECT_EQ(paths, typed.decimal_paths());
    EXPECT_EQ(nullptr, plain.decimal_paths());
    // The paths are an ingestion hint, never part of the type's identity.
    EXPECT_TRUE(typed.equals(plain));
    auto serde = std::dynamic_pointer_cast<DataTypeVariantV2SerDe>(typed.get_serde());
    ASSERT_NE(nullptr, serde);
    EXPECT_EQ(paths, serde->decimal_paths());
}

TEST(DataTypeVariantV2SerdeInputTest, ThriftTemplatePathsReachTheDataType) {
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::VARIANT);
    scalar_type.__set_variant_max_subcolumns_count(0);
    scalar_type.__set_variant_is_v2(true);
    std::vector<TVariantTemplatePath> template_paths(5);
    template_paths[0].__set_pattern("name");
    template_paths[0].__set_is_glob(false);
    template_paths[0].__set_type(TPrimitiveType::STRING);
    template_paths[1].__set_pattern("money.amount");
    template_paths[1].__set_is_glob(false);
    template_paths[1].__set_type(TPrimitiveType::DECIMAL128I);
    template_paths[2].__set_pattern("ratio*");
    template_paths[2].__set_is_glob(true);
    template_paths[2].__set_type(TPrimitiveType::DECIMAL64);
    template_paths[3].__set_pattern("id");
    template_paths[3].__set_is_glob(false);
    template_paths[3].__set_type(TPrimitiveType::LARGEINT);
    template_paths[4].__set_pattern("count");
    template_paths[4].__set_is_glob(false);
    template_paths[4].__set_type(TPrimitiveType::BIGINT);
    scalar_type.__set_variant_template_paths(template_paths);
    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    TTypeDesc type_desc;
    type_desc.types.push_back(type_node);

    DataTypePtr from_thrift = DataTypeFactory::instance().create_data_type(type_desc);
    const auto* variant = dynamic_cast<const DataTypeVariantV2*>(from_thrift.get());
    ASSERT_NE(nullptr, variant);
    ASSERT_NE(nullptr, variant->decimal_paths());
    // each field arrives with its own type, in declaration order
    const std::vector<VariantTemplatePath>& fields = variant->decimal_paths()->paths();
    ASSERT_EQ(5, fields.size());
    EXPECT_EQ("name", fields[0].pattern);
    EXPECT_FALSE(fields[0].is_glob);
    EXPECT_EQ(TYPE_STRING, fields[0].type);
    EXPECT_EQ(TYPE_DECIMAL128I, fields[1].type);
    EXPECT_EQ("ratio*", fields[2].pattern);
    EXPECT_TRUE(fields[2].is_glob);
    EXPECT_EQ(TYPE_DECIMAL64, fields[2].type);
    EXPECT_EQ(TYPE_LARGEINT, fields[3].type);
    EXPECT_EQ(TYPE_BIGINT, fields[4].type);
    // and the type decides how a number on the path is read
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT,
              variant->decimal_paths()->number_read("money.amount"));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, variant->decimal_paths()->number_read("ratio_x"));
    EXPECT_EQ(VariantNumberRead::LARGEINT_TEXT, variant->decimal_paths()->number_read("id"));
    EXPECT_EQ(VariantNumberRead::NATIVE, variant->decimal_paths()->number_read("count"));
    EXPECT_EQ(VariantNumberRead::NATIVE, variant->decimal_paths()->number_read("name"));
    EXPECT_EQ(VariantNumberRead::NATIVE, variant->decimal_paths()->number_read("money"));
    auto serde = std::dynamic_pointer_cast<DataTypeVariantV2SerDe>(from_thrift->get_serde());
    ASSERT_NE(nullptr, serde);
    EXPECT_EQ(variant->decimal_paths(), serde->decimal_paths());
    // the serde the type hands out reads with the template: exact on both kinds of path
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(*serde, *column,
                                 R"({"id":18446744073709551616,"money":{"amount":1.50}})")
                        .ok());
    EXPECT_EQ(R"({"id":18446744073709551616,"money":{"amount":1.50}})",
              json_at(*serde, *column, 0));

    // Without the field, from an older FE or a template without a DECIMAL or LARGEINT field, the
    // type has no paths.
    TScalarType plain_scalar_type;
    plain_scalar_type.__set_type(TPrimitiveType::VARIANT);
    plain_scalar_type.__set_variant_max_subcolumns_count(0);
    plain_scalar_type.__set_variant_is_v2(true);
    TTypeNode plain_node;
    plain_node.__set_type(TTypeNodeType::SCALAR);
    plain_node.__set_scalar_type(plain_scalar_type);
    TTypeDesc plain_desc;
    plain_desc.types.push_back(plain_node);
    DataTypePtr plain = DataTypeFactory::instance().create_data_type(plain_desc);
    const auto* plain_variant = dynamic_cast<const DataTypeVariantV2*>(plain.get());
    ASSERT_NE(nullptr, plain_variant);
    EXPECT_EQ(nullptr, plain_variant->decimal_paths());
}

TEST(DataTypeVariantV2SerdeInputTest, TheFirstMatchingTemplateFieldDecidesADecimalPath) {
    // 'amount*':string before 'amount':decimal(10,2): the shredder types "amount" as STRING, so the
    // number is parsed as before (a double, rendered 1.5) rather than as the decimal 1.50.
    auto string_first =
            template_of({{.pattern = "amount*", .is_glob = true, .type = TYPE_STRING},
                         {.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL128I}});
    DataTypeVariantV2SerDe serde(1, string_first);
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"amount":1.50,"total":2.50})").ok());
    const std::string json = json_at(serde, *column, 0);
    EXPECT_NE(std::string::npos, json.find(R"("amount":1.5)")) << json;
    EXPECT_EQ(std::string::npos, json.find(R"("amount":1.50)")) << json;
    EXPECT_NE(std::string::npos, json.find(R"("total":2.5)")) << json;

    auto decimal_first =
            template_of({{.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL128I},
                         {.pattern = "amount*", .is_glob = true, .type = TYPE_STRING}});
    DataTypeVariantV2SerDe decimal_serde(1, decimal_first);
    ASSERT_TRUE(deserialize_json(decimal_serde, *column, R"({"amount":1.50,"total":2.50})").ok());
    const std::string decimal_json = json_at(decimal_serde, *column, 1);
    EXPECT_NE(std::string::npos, decimal_json.find(R"("amount":1.50)")) << decimal_json;
    EXPECT_NE(std::string::npos, decimal_json.find(R"("total":2.5)")) << decimal_json;
}

TEST(DataTypeVariantV2SerdeInputTest, LargeIntTemplatePathsReadIntegersBeyond64BitsFromText) {
    DataTypeVariantV2SerDe serde(
            1, template_of({{.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
                            {.pattern = "ids", .is_glob = false, .type = TYPE_LARGEINT},
                            {.pattern = "n", .is_glob = false, .type = TYPE_BIGINT}}));
    auto column = ColumnVariantV2::create();
    // 2^64 fits no 64-bit type, so the DOM parser rejects the whole document; on a declared
    // LARGEINT path the integer is now read from its text and kept exactly.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"id":18446744073709551616})").ok());
    EXPECT_EQ(R"({"id":18446744073709551616})", json_at(serde, *column, 0));
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"id":-9223372036854775809})").ok());
    EXPECT_EQ(R"({"id":-9223372036854775809})", json_at(serde, *column, 1));
    // 2^63 fits a uint64 and is read as before, without the template.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"id":9223372036854775808})").ok());
    EXPECT_EQ(R"({"id":9223372036854775808})", json_at(serde, *column, 2));
    // Above 10^38 - 1, what a Variant decimal16 holds, a LARGEINT is kept as its digits: the
    // largest and the smallest LARGEINT, 2^127 - 1 and -2^127.
    ASSERT_TRUE(
            deserialize_json(serde, *column, R"({"id":170141183460469231731687303715884105727})")
                    .ok());
    EXPECT_EQ(R"({"id":"170141183460469231731687303715884105727"})", json_at(serde, *column, 3));
    ASSERT_TRUE(
            deserialize_json(serde, *column, R"({"id":-170141183460469231731687303715884105728})")
                    .ok());
    EXPECT_EQ(R"({"id":"-170141183460469231731687303715884105728"})", json_at(serde, *column, 4));
    // 2^127 is no LARGEINT: its text is kept as it is, for the template's cast to decide.
    ASSERT_TRUE(
            deserialize_json(serde, *column, R"({"id":170141183460469231731687303715884105728})")
                    .ok());
    EXPECT_EQ(R"({"id":"170141183460469231731687303715884105728"})", json_at(serde, *column, 5));
    // A fraction is not an integer as written: the parser's double stays.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"id":1.5})").ok());
    EXPECT_EQ(R"({"id":1.5})", json_at(serde, *column, 6));
    // Scalars directly inside a declared array are the ARRAY<LARGEINT> field's.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"ids":[18446744073709551616,-1]})").ok());
    EXPECT_EQ(R"({"ids":[18446744073709551616,-1]})", json_at(serde, *column, 7));

    // The vector entry point takes the same route.
    std::vector<std::string> texts {R"({"id":18446744073709551617})",
                                    R"({"id":340282366920938463463374607431768211456})"};
    std::vector<Slice> slices;
    for (const std::string& text : texts) {
        slices.emplace_back(text.data(), text.size());
    }
    uint64_t deserialized = 0;
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(serde.deserialize_column_from_json_vector(*column, slices, &deserialized, options)
                        .ok());
    EXPECT_EQ(2, deserialized);
    EXPECT_EQ(R"({"id":18446744073709551617})", json_at(serde, *column, 8));
    EXPECT_EQ(R"({"id":"340282366920938463463374607431768211456"})", json_at(serde, *column, 9));
}

TEST(DataTypeVariantV2SerdeInputTest, AnIntegerBeyond64BitsOffALargeIntPathKeepsTheLenientVerdict) {
    DataTypeVariantV2SerDe serde(
            1, template_of({{.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
                            {.pattern = "a.id", .is_glob = false, .type = TYPE_LARGEINT},
                            {.pattern = "ids", .is_glob = false, .type = TYPE_LARGEINT},
                            {.pattern = "n", .is_glob = false, .type = TYPE_BIGINT}}));
    auto column = ColumnVariantV2::create();
    // The DOM parser cannot read an integer beyond 64 bits, and the lenient serde keeps a document
    // the parser rejects as a string row. Each of these keeps that verdict, as without a template.
    const std::vector<std::string> documents {
            // on no template path
            R"({"other":18446744073709551616})",
            // on a path declared BIGINT
            R"({"n":18446744073709551616})",
            // after a LARGEINT path was already read: the partial row is rolled back
            R"({"id":18446744073709551616,"other":18446744073709551616})",
            // under an object that is an array element nothing is on a declared path
            R"({"a":[{"id":18446744073709551616}]})",
            // nor in an array nested inside the declared array
            R"({"ids":[[18446744073709551616]]})",
            // not JSON integers: a declared LARGEINT path does not rescue them
            R"({"id":01})",
            R"({"id":1e400})",
    };
    for (size_t row = 0; row < documents.size(); ++row) {
        ASSERT_TRUE(deserialize_json(serde, *column, documents[row]).ok()) << documents[row];
        const std::string kept = json_at(serde, *column, row);
        EXPECT_EQ('"', kept.front()) << documents[row] << " -> " << kept;
    }
    // The same path through objects only is declared and exact, and a serde without a template
    // keeps its verdict on the document that is exact above.
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"a":{"id":18446744073709551616}})").ok());
    EXPECT_EQ(R"({"a":{"id":18446744073709551616}})", json_at(serde, *column, documents.size()));
    DataTypeVariantV2SerDe plain;
    auto plain_column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(plain, *plain_column, R"({"id":18446744073709551616})").ok());
    EXPECT_EQ('"', json_at(plain, *plain_column, 0).front());
}

} // namespace doris
