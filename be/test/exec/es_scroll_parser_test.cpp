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

#include "format/table/es/es_scroll_parser.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <re2/re2.h>

#include <cstring>
#include <string>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "format/table/es/es_scroll_query.h"
#include "runtime/descriptors.h"
#include "util/jsonb_utils.h"

namespace doris {

class EsScrollParserTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    static TSlotDescriptor value_slot(TPrimitiveType::type type, bool array = false) {
        TSlotDescriptor slot;
        slot.colName = "value";
        slot.nullIndicatorBit = -1;
        if (array) {
            TTypeNode node;
            node.__set_type(TTypeNodeType::ARRAY);
            node.__set_contains_null(true);
            node.__set_contains_nulls({true});
            slot.slotType.types.push_back(node);
        }
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar;
        scalar.__set_type(type);
        node.__set_scalar_type(scalar);
        slot.slotType.types.push_back(node);
        return slot;
    }

    static Status read_scalar(TPrimitiveType::type type, const std::string& json,
                              MutableColumnPtr* column, bool array = false) {
        auto descriptor = value_slot(type, array);
        descriptor.nullIndicatorBit = 0;
        SlotDescriptor slot(descriptor);
        ScrollParser parser(false);
        RETURN_IF_ERROR(
                parser.parse(R"({"hits":{"hits":[{"_source":{"value":)" + json + "}}]}}", true));
        std::vector<SlotDescriptor*> slots {&slot};
        std::vector<MutableColumnPtr> columns;
        columns.push_back(slot.type()->create_column());
        bool eof = false;
        RETURN_IF_ERROR(parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone()));
        *column = std::move(columns[0]);
        return Status::OK();
    }

    static TSlotDescriptor boolean_slot(bool array) {
        return value_slot(TPrimitiveType::BOOLEAN, array);
    }
};

TEST_F(EsScrollParserTest, JsonbSourcePreservesArrayContainers) {
    for (const std::string value : {R"([{"x":1}])", "[]", "[null]", R"({"x":1})"}) {
        MutableColumnPtr column;
        auto status = read_scalar(TPrimitiveType::JSONB, value, &column);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(column->size(), 1);
        ASSERT_FALSE(column->is_null_at(0)) << value;
        const auto bytes = column->get_data_at(0);
        EXPECT_EQ(JsonbToJson::jsonb_to_json_string(bytes.data, bytes.size), value);
    }
}

TEST_F(EsScrollParserTest, IntegerNumericDoublePreservesLongBoundary) {
    for (const std::string value : {"-9223372036854775808.0", "-9.223372036854776E18"}) {
        MutableColumnPtr column;
        auto status = read_scalar(TPrimitiveType::BIGINT, value, &column);
        ASSERT_TRUE(status.ok()) << value << ": " << status.to_string();
        ASSERT_FALSE(column->is_null_at(0));
        EXPECT_EQ(assert_cast<const ColumnNullable&>(*column).get_nested_column().get_int(0),
                  INT64_MIN);
    }
}

TEST_F(EsScrollParserTest, NumericSourceEmptyStringsAreNull) {
    for (auto type : {TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT, TPrimitiveType::INT,
                      TPrimitiveType::BIGINT, TPrimitiveType::FLOAT, TPrimitiveType::DOUBLE}) {
        for (const std::string value : {R"("")", R"([""])"}) {
            MutableColumnPtr column;
            auto status = read_scalar(type, value, &column);
            ASSERT_TRUE(status.ok()) << type << ": " << value << ": " << status.to_string();
            ASSERT_EQ(column->size(), 1);
            EXPECT_TRUE(column->is_null_at(0));
        }
    }
}

TEST_F(EsScrollParserTest, DeclaredSourceArraysPreserveNullElements) {
    for (auto type : {TPrimitiveType::BIGINT, TPrimitiveType::BOOLEAN, TPrimitiveType::DOUBLE,
                      TPrimitiveType::STRING, TPrimitiveType::DATEV2, TPrimitiveType::JSONB}) {
        std::string value;
        switch (type) {
        case TPrimitiveType::BOOLEAN:
            value = "true";
            break;
        case TPrimitiveType::STRING:
            value = R"("text")";
            break;
        case TPrimitiveType::DATEV2:
            value = R"("2026-01-01")";
            break;
        case TPrimitiveType::JSONB:
            value = R"({"x":1})";
            break;
        default:
            value = "1";
        }
        MutableColumnPtr column;
        auto status = read_scalar(type, "[null," + value + ",null]", &column, true);
        ASSERT_TRUE(status.ok()) << type << ": " << status.to_string();
        ASSERT_FALSE(column->is_null_at(0));
        const auto& array = assert_cast<const ColumnArray&>(
                assert_cast<const ColumnNullable&>(*column).get_nested_column());
        ASSERT_EQ(array.get_offsets()[0], 3);
        EXPECT_TRUE(array.get_data().is_null_at(0));
        EXPECT_FALSE(array.get_data().is_null_at(1));
        EXPECT_TRUE(array.get_data().is_null_at(2));
    }
}

TEST_F(EsScrollParserTest, NarrowIntegerSourceAcceptsJavaFloatingGrammar) {
    for (auto type : {TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT, TPrimitiveType::INT}) {
        for (const std::string value : {"0x1.8p1", "0X1.8P1f", "3d", "+3F", " 3d "}) {
            MutableColumnPtr column;
            auto status = read_scalar(type, "\"" + value + "\"", &column);
            ASSERT_TRUE(status.ok()) << type << ": " << value << ": " << status.to_string();
            EXPECT_EQ(assert_cast<const ColumnNullable&>(*column).get_nested_column().get_int(0),
                      3);
        }
        for (const std::string value : {"0x3", "0x1.8", "0x1p1junk", "3dd", "3 f"}) {
            MutableColumnPtr column;
            EXPECT_FALSE(read_scalar(type, "\"" + value + "\"", &column).ok()) << value;
        }
    }
}

TEST_F(EsScrollParserTest, FloatingSourceAcceptsJavaFloatingGrammar) {
    for (auto type : {TPrimitiveType::FLOAT, TPrimitiveType::DOUBLE}) {
        for (const std::string value : {"0x1.8p1", "3d", "+3F"}) {
            MutableColumnPtr column;
            auto status = read_scalar(type, "\"" + value + "\"", &column);
            ASSERT_TRUE(status.ok()) << value << ": " << status.to_string();
            auto bytes = column->get_data_at(0);
            if (type == TPrimitiveType::FLOAT) {
                float actual;
                memcpy(&actual, bytes.data, sizeof(actual));
                EXPECT_FLOAT_EQ(actual, 3.0F);
            } else {
                double actual;
                memcpy(&actual, bytes.data, sizeof(actual));
                EXPECT_DOUBLE_EQ(actual, 3.0);
            }
        }
    }
}

TEST_F(EsScrollParserTest, FloatSourceRoundsDirectlyToFloat) {
    MutableColumnPtr column;
    auto status = read_scalar(TPrimitiveType::FLOAT,
                              R"("1.000000059604644775390625000000000000000000000001")", &column);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto bytes = column->get_data_at(0);
    float actual;
    memcpy(&actual, bytes.data, sizeof(actual));
    EXPECT_EQ(actual, 1.00000011920928955078125F);
}

TEST_F(EsScrollParserTest, EmptyAndNullScalarSourceArrays) {
    for (auto type : {TPrimitiveType::FLOAT, TPrimitiveType::DOUBLE, TPrimitiveType::BOOLEAN,
                      TPrimitiveType::BIGINT}) {
        for (const std::string value : {"[]", "[null]", "null"}) {
            MutableColumnPtr column;
            auto status = read_scalar(type, value, &column);
            ASSERT_TRUE(status.ok()) << type << ": " << value << ": " << status.to_string();
            ASSERT_EQ(column->size(), 1);
            EXPECT_TRUE(column->is_null_at(0));
        }
    }
}

TEST_F(EsScrollParserTest, FloatingPointSourceSingletonArrays) {
    for (auto type : {TPrimitiveType::FLOAT, TPrimitiveType::DOUBLE}) {
        MutableColumnPtr column;
        auto status = read_scalar(type, "[1.5]", &column);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_FALSE(column->is_null_at(0));
        auto data = assert_cast<const ColumnNullable&>(*column).get_nested_column().get_data_at(0);
        if (type == TPrimitiveType::FLOAT) {
            float actual;
            memcpy(&actual, data.data, sizeof(actual));
            EXPECT_FLOAT_EQ(actual, 1.5F);
        } else {
            double actual;
            memcpy(&actual, data.data, sizeof(actual));
            EXPECT_DOUBLE_EQ(actual, 1.5);
        }
    }
}

TEST_F(EsScrollParserTest, BooleanAndIntegerSourceSingletonArrays) {
    MutableColumnPtr column;
    auto status = read_scalar(TPrimitiveType::BOOLEAN, R"([""])", &column);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(column->is_null_at(0));
    EXPECT_FALSE(column->get_bool(0));
    status = read_scalar(TPrimitiveType::BIGINT, "[1]", &column);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(column->is_null_at(0));
    EXPECT_EQ(assert_cast<const ColumnNullable&>(*column).get_nested_column().get_int(0), 1);
}

TEST_F(EsScrollParserTest, NarrowIntegerNaNAndInfinity) {
    for (auto type : {TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT, TPrimitiveType::INT}) {
        MutableColumnPtr column;
        auto status = read_scalar(type, R"("NaN")", &column);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(assert_cast<const ColumnNullable&>(*column).get_nested_column().get_int(0), 0);
        EXPECT_FALSE(read_scalar(type, R"("Infinity")", &column).ok());
    }
}

TEST_F(EsScrollParserTest, NarrowIntegerStringRounding) {
    for (auto type : {TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT, TPrimitiveType::INT,
                      TPrimitiveType::BIGINT}) {
        SlotDescriptor slot(value_slot(type));
        std::vector<SlotDescriptor*> slots {&slot};
        for (const auto& [value, expected] : std::vector<std::pair<std::string, int64_t>> {
                     {"0.99999999999999999", type == TPrimitiveType::BIGINT ? 0 : 1},
                     {"-0.99999999999999999", type == TPrimitiveType::BIGINT ? 0 : -1},
                     {"1.2e1", 12},
                     {"127.9", 127}}) {
            ScrollParser parser(false);
            ASSERT_TRUE(
                    parser.parse(R"({"hits":{"hits":[{"_source":{"value":")" + value + R"("}}]}})",
                                 true)
                            .ok());
            std::vector<MutableColumnPtr> columns;
            columns.push_back(slot.type()->create_column());
            bool eof = false;
            auto status = parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone());
            ASSERT_TRUE(status.ok()) << status.to_string();
            EXPECT_EQ(columns[0]->get_int(0), expected) << type << ": " << value;
        }
    }
}

TEST_F(EsScrollParserTest, IntegerSourceCoercion) {
    SlotDescriptor slot(value_slot(TPrimitiveType::BIGINT));
    std::vector<SlotDescriptor*> slots {&slot};
    for (const auto& [value, expected] :
         std::vector<std::pair<std::string, int64_t>> {{R"("1.2e3")", 1200},
                                                       {R"("-1.9E1")", -19},
                                                       {R"("1.9e-1")", 0},
                                                       {R"(".9e1")", 9},
                                                       {R"("00012.9")", 12},
                                                       {R"("-0.9")", 0},
                                                       {R"("9223372036854775807")", INT64_MAX},
                                                       {R"("-9.223372036854775808e18")", INT64_MIN},
                                                       {R"("922337203685477580.7e1")", INT64_MAX},
                                                       {"1.9", 1},
                                                       {"[5]", 5},
                                                       {R"(["1.2e3"])", 1200}}) {
        ScrollParser parser(false);
        ASSERT_TRUE(parser.parse(R"({"hits":{"hits":[{"_source":{"value":)" + value + "}}]}}", true)
                            .ok());
        std::vector<MutableColumnPtr> columns;
        columns.push_back(slot.type()->create_column());
        bool eof = false;
        auto status = parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone());
        ASSERT_TRUE(status.ok()) << value << ": " << status.to_string();
        ASSERT_EQ(columns[0]->size(), 1);
        EXPECT_EQ(columns[0]->get_int(0), expected) << value;
    }
    for (const std::string value :
         {R"("9223372036854775808")", R"("1e1000000")", R"("1e")", R"("1.2junk")", "[1,2]"}) {
        ScrollParser parser(false);
        ASSERT_TRUE(parser.parse(R"({"hits":{"hits":[{"_source":{"value":)" + value + "}}]}}", true)
                            .ok());
        std::vector<MutableColumnPtr> columns;
        columns.push_back(slot.type()->create_column());
        bool eof = false;
        EXPECT_FALSE(parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone()).ok())
                << value;
    }
}

TEST_F(EsScrollParserTest, FullSourceRequestOnlyAffectsSourceMode) {
    std::map<std::string, std::string> properties {
            {"query_dsl", "{\"match_all\":{}}"}, {"batch_size", "100"}, {"doc_values_mode", "0"}};
    bool docvalues = true;
    rapidjson::Document body;
    auto query = ESScrollQueryBuilder::build(properties, {"value"}, {}, &docvalues);
    body.Parse(query.c_str());
    ASSERT_FALSE(body.HasParseError());
    EXPECT_TRUE(body["_source"].IsArray());
    EXPECT_FALSE(docvalues);
    properties["read_full_source"] = "true";
    query = ESScrollQueryBuilder::build(properties, {"value"}, {}, &docvalues);
    body.Parse(query.c_str());
    ASSERT_TRUE(body["_source"].IsBool());
    EXPECT_TRUE(body["_source"].GetBool());
    properties["doc_values_mode"] = "1";
    query = ESScrollQueryBuilder::build(properties, {"value"}, {{"value", "value.raw"}},
                                        &docvalues);
    body.Parse(query.c_str());
    EXPECT_TRUE(docvalues);
    EXPECT_FALSE(body.HasMember("_source"));
    ASSERT_TRUE(body["docvalue_fields"].IsArray());
    EXPECT_STREQ(body["docvalue_fields"][0].GetString(), "value.raw");
}

TEST_F(EsScrollParserTest, IntegerArraySourceCoercion) {
    SlotDescriptor slot(value_slot(TPrimitiveType::BIGINT, true));
    std::vector<SlotDescriptor*> slots {&slot};
    ScrollParser parser(false);
    ASSERT_TRUE(
            parser.parse(R"({"hits":{"hits":[{"_source":{"value":["1.2e3",-1.9,"9223372036854775807"]}}]}})",
                         true)
                    .ok());
    std::vector<MutableColumnPtr> columns;
    columns.push_back(slot.type()->create_column());
    bool eof = false;
    auto status = parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone());
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& array = assert_cast<const ColumnArray&>(*columns[0]);
    const auto& values = assert_cast<const ColumnNullable&>(array.get_data()).get_nested_column();
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values.get_int(0), 1200);
    EXPECT_EQ(values.get_int(1), -1);
    EXPECT_EQ(values.get_int(2), INT64_MAX);
}

TEST_F(EsScrollParserTest, IntegerCoercionChecksNarrowBounds) {
    SlotDescriptor slot(value_slot(TPrimitiveType::TINYINT));
    std::vector<SlotDescriptor*> slots {&slot};
    for (const std::string value : {"128", "-129", R"("1.28e2")", R"("-1.29e2")"}) {
        ScrollParser parser(false);
        ASSERT_TRUE(parser.parse(R"({"hits":{"hits":[{"_source":{"value":)" + value + "}}]}}", true)
                            .ok());
        std::vector<MutableColumnPtr> columns;
        columns.push_back(slot.type()->create_column());
        bool eof = false;
        EXPECT_FALSE(parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone()).ok())
                << value;
    }
}

TEST_F(EsScrollParserTest, BooleanSourceRepresentations) {
    SlotDescriptor slot(boolean_slot(false));
    std::vector<SlotDescriptor*> slots {&slot};
    for (const auto& [value, expected] :
         std::vector<std::pair<std::string, bool>> {{R"("")", false},
                                                    {R"("false")", false},
                                                    {R"("true")", true},
                                                    {"false", false},
                                                    {"true", true}}) {
        ScrollParser parser(false);
        auto status =
                parser.parse(R"({"hits":{"hits":[{"_source":{"value":)" + value + "}}]}}", true);
        ASSERT_TRUE(status.ok()) << status.to_string();
        std::vector<MutableColumnPtr> columns;
        columns.push_back(slot.type()->create_column());
        bool eof = false;
        status = parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone());
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(columns[0]->size(), 1);
        EXPECT_EQ(columns[0]->get_bool(0), expected);
    }
}

TEST_F(EsScrollParserTest, BooleanArraySourceEmptyString) {
    SlotDescriptor slot(boolean_slot(true));
    std::vector<SlotDescriptor*> slots {&slot};
    ScrollParser parser(false);
    auto status =
            parser.parse(R"({"hits":{"hits":[{"_source":{"value":["","true",false]}}]}})", true);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::vector<MutableColumnPtr> columns;
    columns.push_back(slot.type()->create_column());
    bool eof = false;
    status = parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone());
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& array = assert_cast<const ColumnArray&>(*columns[0]);
    const auto& values = assert_cast<const ColumnNullable&>(array.get_data()).get_nested_column();
    ASSERT_EQ(values.size(), 3);
    EXPECT_FALSE(values.get_bool(0));
    EXPECT_TRUE(values.get_bool(1));
    EXPECT_FALSE(values.get_bool(2));
}

TEST_F(EsScrollParserTest, InvalidBooleanSourceIsRejected) {
    SlotDescriptor slot(boolean_slot(false));
    std::vector<SlotDescriptor*> slots {&slot};
    ScrollParser parser(false);
    ASSERT_TRUE(parser.parse(R"({"hits":{"hits":[{"_source":{"value":"invalid"}}]}})", true).ok());
    std::vector<MutableColumnPtr> columns;
    columns.push_back(slot.type()->create_column());
    bool eof = false;
    EXPECT_FALSE(parser.fill_columns(slots, columns, &eof, {}, cctz::utc_time_zone()).ok());
}

// Test timezone pattern matching for ES datetime parsing fix
TEST_F(EsScrollParserTest, TestTimezonePatternMatching) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::vector<std::string> valid_timezone_formats = {
            "2025-05-23T20:56:52.052+0900",  "2025-05-23T20:56:52.052-0500",
            "2025-05-23T20:56:52.052+08:00", "2025-05-23T20:56:52.052-04:30",
            "2025-05-23T20:56:52.052Z",      "2022-08-08T12:10:10.151Z",
            "2022-08-08T12:10:10+0900",      "2022-08-08T12:10:10-0500"};

    for (const auto& datetime_str : valid_timezone_formats) {
        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);
        EXPECT_TRUE(matched) << "Failed to match timezone in: " << datetime_str;

        std::string timezone = timezone_value.as_string();
        EXPECT_FALSE(timezone.empty()) << "Empty timezone captured from: " << datetime_str;

        if (timezone == "Z") {
            EXPECT_EQ(timezone, "Z");
        } else {
            EXPECT_TRUE(timezone[0] == '+' || timezone[0] == '-')
                    << "Invalid timezone sign in: " << timezone;
            // Valid timezone lengths: 5 for +0900, 6 for +08:00
            EXPECT_TRUE(timezone.length() == 5 || timezone.length() == 6)
                    << "Invalid timezone length in: " << timezone
                    << " (length: " << timezone.length() << ")";
        }
    }
}

TEST_F(EsScrollParserTest, TestInvalidTimezonePatterns) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::vector<std::string> invalid_formats = {
            "2025-05-23T20:56:52.052", "2025-05-23T20:56:52.052+9", "2025-05-23T20:56:52.052+090",
            "2025-05-23T20:56:52.052+9:00"};

    for (const auto& datetime_str : invalid_formats) {
        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);
        if (matched) {
            std::string timezone = timezone_value.as_string();
            EXPECT_TRUE(timezone.empty()) << "Should not capture timezone from: " << datetime_str;
        }
    }
}

TEST_F(EsScrollParserTest, TestBugScenarioTimezoneFormat) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::string problematic_format = "2025-05-23T20:56:52.052+0900";

    re2::StringPiece timezone_value;
    bool matched = time_zone_pattern.Match(problematic_format, 0, problematic_format.size(),
                                           RE2::UNANCHORED, &timezone_value, 1);

    EXPECT_TRUE(matched) << "Failed to match the bug scenario format: " << problematic_format;

    std::string timezone = timezone_value.as_string();
    EXPECT_EQ(timezone, "+0900") << "Incorrect timezone captured: " << timezone;
}

TEST_F(EsScrollParserTest, TestEdgeCaseTimezoneFormats) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::vector<std::string> edge_cases = {"+00:00", "-00:00", "+23:59", "-23:59",
                                           "+99:99", "Z",      "+0800",  ""};

    // Test each edge case
    std::vector<std::string> test_datetime_strings = {
            "2025-05-23T20:56:52.052+00:00", // +00:00 (UTC with colon)
            "2025-05-23T20:56:52.052-00:00", // -00:00 (UTC with colon)
            "2025-05-23T20:56:52.052+23:59", // +23:59 (max valid timezone)
            "2025-05-23T20:56:52.052-23:59", // -23:59 (max valid timezone)
            "2025-05-23T20:56:52.052+99:99", // +99:99 (invalid but should match pattern)
            "2025-05-23T20:56:52.052Z",      // Z (UTC)
            "2025-05-23T20:56:52.052+0800",  // +0800 (no colon)
            "2025-05-23T20:56:52.052"        // empty timezone (no timezone)
    };

    std::vector<std::string> expected_matches = {"+00:00", "-00:00", "+23:59", "-23:59",
                                                 "+99:99", "Z",      "+0800",  ""};

    std::vector<bool> should_match = {true, true, true, true, true, true, true, false};

    for (size_t i = 0; i < test_datetime_strings.size(); ++i) {
        const std::string& datetime_str = test_datetime_strings[i];
        const std::string& expected_match = expected_matches[i];
        bool should_match_expected = should_match[i];

        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);

        EXPECT_EQ(matched, should_match_expected)
                << "Edge case test failed for: " << datetime_str
                << " (expected match: " << should_match_expected << ")";

        if (matched && should_match_expected) {
            std::string timezone = timezone_value.as_string();
            EXPECT_EQ(timezone, expected_match)
                    << "Incorrect timezone captured from: " << datetime_str
                    << " (expected: " << expected_match << ", got: " << timezone << ")";
        }
    }
}

TEST_F(EsScrollParserTest, TestSpecialTimezoneEdgeCases) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    // Additional edge cases for comprehensive testing
    std::vector<std::pair<std::string, std::pair<std::string, bool>>> special_cases = {
            // {datetime_string, {expected_timezone, should_match}}
            {"2025-05-23T20:56:52+0000", {"+0000", true}},          // +0000 without colon
            {"2025-05-23T20:56:52-0000", {"-0000", true}},          // -0000 without colon
            {"2025-05-23T20:56:52+12:30", {"+12:30", true}},        // +12:30 with colon
            {"2025-05-23T20:56:52-12:30", {"-12:30", true}},        // -12:30 with colon
            {"2025-05-23T20:56:52+1200", {"+1200", true}},          // +1200 without colon
            {"2025-05-23T20:56:52-1200", {"-1200", true}},          // -1200 without colon
            {"2025-05-23T20:56:52.000Z", {"Z", true}},              // Z with milliseconds
            {"2025-05-23T20:56:52.123456+05:30", {"+05:30", true}}, // microseconds with timezone
            {"2025-05-23T20:56:52.123456-05:30", {"-05:30", true}}, // microseconds with timezone
            {"2025-05-23T20:56:52.123456+0530", {"+0530", true}},   // microseconds without colon
            {"2025-05-23T20:56:52.123456-0530", {"-0530", true}},   // microseconds without colon
            {"2025-05-23T20:56:52+14:00", {"+14:00", true}},        // +14:00 (valid max timezone)
            {"2025-05-23T20:56:52-12:00", {"-12:00", true}},        // -12:00 (valid min timezone)
    };

    for (const auto& test_case : special_cases) {
        const std::string& datetime_str = test_case.first;
        const std::string& expected_timezone = test_case.second.first;
        bool should_match = test_case.second.second;

        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);

        EXPECT_EQ(matched, should_match) << "Special case test failed for: " << datetime_str
                                         << " (expected match: " << should_match << ")";

        if (matched && should_match) {
            std::string timezone = timezone_value.as_string();
            EXPECT_EQ(timezone, expected_timezone)
                    << "Incorrect timezone captured from: " << datetime_str
                    << " (expected: " << expected_timezone << ", got: " << timezone << ")";
        }
    }
}

} // namespace doris
