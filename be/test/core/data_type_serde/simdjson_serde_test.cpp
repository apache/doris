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

#include "core/column/column_map.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/simdjson_value.h"
#include "core/value/jsonb_value.h"

namespace doris {
namespace {
Status append_simdjson(const DataTypeSerDe& serde, IColumn& column, const std::string& json,
                       SimdJsonReadContext& context) {
    context.string_values.clear();
    simdjson::ondemand::parser parser;
    simdjson::padded_string text("{\"v\":" + json + ",\"tail\":42}");
    try {
        auto doc = parser.iterate(text);
        auto object = doc.get_object().value();
        auto value = object.find_field_unordered("v").value();
        SimdJsonValue input {value};
        auto status = serde.deserialize_one_cell_from_simdjson(
                column, input, DataTypeSerDe::get_default_format_options(), context, "v");
        if (!status.ok()) {
            return status;
        }
        EXPECT_EQ(object.find_field_unordered("tail").get_int64().value(), 42);
        return Status::OK();
    } catch (const simdjson::simdjson_error& e) {
        return Status::DataQualityError("{}", e.what());
    }
}

Status append_legacy(const DataTypeSerDe& serde, IColumn& column, const std::string& json) {
    simdjson::ondemand::parser parser;
    simdjson::padded_string text("{\"v\":" + json + "}");
    auto doc = parser.iterate(text);
    auto object = doc.get_object().value();
    auto value = object.find_field_unordered("v").value();
    std::string_view token;
    if (value.type().value() == simdjson::ondemand::json_type::string) {
        token = value.get_string();
    } else if (value.type().value() == simdjson::ondemand::json_type::boolean) {
        token = value.get_bool() ? "1" : "0";
    } else {
        token = simdjson::to_json_string(value);
    }
    Slice slice {token.data(), token.size()};
    return serde.deserialize_one_cell_from_json(column, slice,
                                                DataTypeSerDe::get_default_format_options());
}
} // namespace

TEST(SimdJsonSerDeTest, ScalarConversionsMatchReader) {
    const DataTypes types {std::make_shared<DataTypeInt8>(),
                           std::make_shared<DataTypeInt16>(),
                           std::make_shared<DataTypeInt32>(),
                           std::make_shared<DataTypeInt64>(),
                           std::make_shared<DataTypeInt128>(),
                           std::make_shared<DataTypeBool>(),
                           std::make_shared<DataTypeFloat64>(),
                           std::make_shared<DataTypeString>(),
                           std::make_shared<DataTypeDecimal<TYPE_DECIMAL128I>>(30, 5)};
    const std::vector<std::string> values {"0",
                                           "-1",
                                           "127",
                                           "128",
                                           "-129",
                                           "32768",
                                           "2147483648",
                                           "9223372036854775807",
                                           "-9223372036854775808",
                                           "9223372036854775808",
                                           "18446744073709551616",
                                           "1.5",
                                           "1e3",
                                           "true",
                                           "false",
                                           R"("123")",
                                           R"("bad")",
                                           R"("a\nb\u4e2d")"};
    SimdJsonReadContext context;
    for (const auto& type : types) {
        const auto serde = type->get_serde();
        for (const auto& json : values) {
            SCOPED_TRACE(type->get_name() + ": " + json);
            auto actual = type->create_column();
            auto expected = type->create_column();
            auto old_status = append_legacy(*serde, *expected, json);
            auto status = append_simdjson(*serde, *actual, json, context);
            ASSERT_EQ(status.ok(), old_status.ok()) << status << " / " << old_status;
            ASSERT_EQ(actual->size(), expected->size());
            if (status.ok()) {
                EXPECT_EQ(actual->compare_at(0, 0, *expected, 1), 0);
            }
        }
    }
}

TEST(SimdJsonSerDeTest, JsonbBytesAndLegacyScalarSemantics) {
    DataTypeJsonb type;
    auto serde = type.get_serde();
    auto actual = type.create_column();
    auto expected = type.create_column();
    SimdJsonReadContext context;
    const std::vector<std::string> values {
            R"({"x":[1,"a\nb",true,null]})",
            "[]",
            "{}",
            R"([18446744073709551616,-170141183460469231731687303715884105728,1.25])",
            R"("{\"x\":1}")",
            "true",
            "false",
            "123",
            "\"" + std::string(8192, 'x') + "\""};
    for (const auto& json : values) {
        const auto old_status = append_legacy(*serde, *expected, json);
        const auto status = append_simdjson(*serde, *actual, json, context);
        ASSERT_EQ(status.ok(), old_status.ok()) << json << ": " << status;
    }
    ASSERT_EQ(actual->size(), expected->size());
    for (size_t i = 0; i < actual->size(); ++i) {
        EXPECT_EQ(actual->get_data_at(i).to_string(), expected->get_data_at(i).to_string());
    }
}

TEST(SimdJsonSerDeTest, NestedConversionsAndErrors) {
    auto integer = make_nullable(std::make_shared<DataTypeInt8>());
    auto structure =
            std::make_shared<DataTypeStruct>(DataTypes {integer, integer}, Strings {"a", "b"});
    auto array = std::make_shared<DataTypeArray>(make_nullable(structure));
    auto map = std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                             make_nullable(array));
    SimdJsonReadContext context;
    auto column = map->create_column();
    auto serde = map->get_serde();
    ASSERT_TRUE(append_simdjson(*serde, *column, R"({"ok":[{"a":1,"A":2,"ignored":5}]})", context)
                        .ok());
    for (const auto& json :
         {R"({"first":[{"a":3}],"bad":[{"a":4,"b":128}]})", R"({"bad":[{"a":1},{"a":"oops"}]})"}) {
        auto invalid_column = map->create_column();
        EXPECT_FALSE(append_simdjson(*serde, *invalid_column, json, context).ok());
    }
    EXPECT_TRUE(append_simdjson(*serde, *column, R"({"next":[null,{"b":7}]})", context).ok());
    EXPECT_EQ(column->size(), 2);
}

TEST(SimdJsonSerDeTest, RepeatedJsonpathStringAndSubtree) {
    simdjson::ondemand::parser parser;
    simdjson::padded_string text(std::string_view(R"({"payload":{"s":"a\nb","n":7},"tail":42})"));
    auto doc = parser.iterate(text);
    auto object = doc.get_object().value();
    SimdJsonReadContext context;
    context.use_string_cache = true;
    DataTypeString string_type;
    auto strings = string_type.create_column();
    auto string_serde = string_type.get_serde();
    DataTypeJsonb jsonb_type;
    auto jsonbs = jsonb_type.create_column();
    auto jsonb_serde = jsonb_type.get_serde();
    for (int i = 0; i < 32; ++i) {
        auto value = object.find_field_unordered("payload").find_field_unordered("s").value();
        SimdJsonValue input {value};
        ASSERT_TRUE(string_serde
                            ->deserialize_one_cell_from_simdjson(
                                    *strings, input, DataTypeSerDe::get_default_format_options(),
                                    context, "s")
                            .ok());
        auto payload = object.find_field_unordered("payload").value();
        SimdJsonValue subtree {payload};
        ASSERT_TRUE(jsonb_serde
                            ->deserialize_one_cell_from_simdjson(
                                    *jsonbs, subtree, DataTypeSerDe::get_default_format_options(),
                                    context, "payload")
                            .ok());
    }
    EXPECT_EQ(strings->get_data_at(0).to_string(), "a\nb");
    EXPECT_EQ(strings->get_data_at(0).to_string(), strings->get_data_at(1).to_string());
    EXPECT_EQ(jsonbs->get_data_at(0).to_string(), jsonbs->get_data_at(1).to_string());
    EXPECT_EQ(object.find_field_unordered("tail").get_int64().value(), 42);
}
} // namespace doris
