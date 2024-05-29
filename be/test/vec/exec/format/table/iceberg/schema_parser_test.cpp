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

#include "vec/exec/format/table/iceberg/schema_parser.h"

#include <gtest/gtest.h>

#include "vec/exec/format/table/iceberg/schema.h"

namespace doris {
namespace iceberg {

class TypeTest : public testing::Test {
public:
    TypeTest() = default;
    virtual ~TypeTest() = default;
};

const std::string valid_struct_json = R"({
    "type": "struct",
    "fields": [
        {
            "id": 1,
            "name": "name",
            "type": "string",
            "required": true,
            "doc": "This is a name field"
        },
        {
            "id": 2,
            "name": "age",
            "type": "int",
            "required": false
        }
    ]
})";

const std::string invalid_json = R"({
    "type": "struct",
    "fields": [
        {
            "id": "invalid_id",
            "name": "name",
            "type": "string",
            "required": true,
            "doc": "This is a name field"
        }
    ]
})";

const std::string valid_list_json = R"({
    "type": "list",
    "element-id": 3,
    "element": "string",
    "element-required": true
})";

const std::string valid_map_json = R"({
    "type": "map",
    "key-id": 4,
    "key": "string",
    "value-id": 5,
    "value": "int",
    "value-required": true
})";

const std::string nested_list_json = R"({
    "type": "list",
    "element-id": 6,
    "element": {
        "type": "list",
        "element-id": 7,
        "element": "int",
        "element-required": false
    },
    "element-required": true
})";

const std::string nested_map_json = R"({
    "type": "map",
    "key-id": 8,
    "key": "string",
    "value-id": 9,
    "value": {
        "type": "map",
        "key-id": 10,
        "key": "string",
        "value-id": 11,
        "value": "int",
        "value-required": false
    },
    "value-required": true
})";

const std::string list_of_map_json = R"({
    "type": "list",
    "element-id": 12,
    "element": {
        "type": "map",
        "key-id": 13,
        "key": "string",
        "value-id": 14,
        "value": "int",
        "value-required": true
    },
    "element-required": true
})";

const std::string map_of_list_json = R"({
    "type": "map",
    "key-id": 15,
    "key": "string",
    "value-id": 16,
    "value": {
        "type": "list",
        "element-id": 17,
        "element": "int",
        "element-required": true
    },
    "value-required": true
})";

const std::string struct_with_nested_list_map_json = R"({
    "type": "struct",
    "fields": [
        {
            "id": 18,
            "name": "nested_list",
            "type": {
                "type": "list",
                "element-id": 19,
                "element": {
                    "type": "map",
                    "key-id": 20,
                    "key": "string",
                    "value-id": 21,
                    "value": "int",
                    "value-required": true
                },
                "element-required": true
            },
            "required": true
        },
        {
            "id": 22,
            "name": "nested_map",
            "type": {
                "type": "map",
                "key-id": 23,
                "key": "string",
                "value-id": 24,
                "value": {
                    "type": "list",
                    "element-id": 25,
                    "element": "int",
                    "element-required": true
                },
                "value-required": true
            },
            "required": true
        }
    ]
})";

TEST(SchemaParserTest, parse_valid_struct) {
    std::unique_ptr<Schema> schema = SchemaParser::from_json(valid_struct_json);
    ASSERT_NE(schema, nullptr);

    const NestedField* name_field = schema->find_field(1);
    ASSERT_NE(name_field, nullptr);
    EXPECT_EQ(name_field->field_name(), "name");
    EXPECT_EQ(name_field->is_required(), true);
    EXPECT_EQ(name_field->field_type()->as_primitive_type()->to_string(), "string");

    const NestedField* age_field = schema->find_field(2);
    ASSERT_NE(age_field, nullptr);
    EXPECT_EQ(age_field->field_name(), "age");
    EXPECT_EQ(age_field->is_optional(), true);
    EXPECT_EQ(age_field->field_type()->as_primitive_type()->to_string(), "int");
}

//TEST(SchemaParserTest, parse_invalid_json) {
//    EXPECT_THROW(SchemaParser::from_json(invalid_json), doris::Exception);
//}

TEST(SchemaParserTest, parse_valid_list) {
    std::unique_ptr<Type> type =
            SchemaParser::_type_from_json(rapidjson::Document().Parse(valid_list_json.c_str()));
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->to_string(), "list<string>");
}

TEST(SchemaParserTest, parse_valid_map) {
    std::unique_ptr<Type> type =
            SchemaParser::_type_from_json(rapidjson::Document().Parse(valid_map_json.c_str()));
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->to_string(), "map<string, int>");
}

TEST(SchemaParserTest, parse_nested_list) {
    rapidjson::Document doc;
    doc.Parse(nested_list_json.c_str());
    std::unique_ptr<Type> type = SchemaParser::_type_from_json(doc);
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->to_string(), "list<list<int>>");
}

TEST(SchemaParserTest, parse_nested_map) {
    rapidjson::Document doc;
    doc.Parse(nested_map_json.c_str());
    std::unique_ptr<Type> type = SchemaParser::_type_from_json(doc);
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->to_string(), "map<string, map<string, int>>");
}

TEST(SchemaParserTest, parse_list_of_map) {
    rapidjson::Document doc;
    doc.Parse(list_of_map_json.c_str());
    std::unique_ptr<Type> type = SchemaParser::_type_from_json(doc);
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->to_string(), "list<map<string, int>>");
}

TEST(SchemaParserTest, parse_map_of_list) {
    rapidjson::Document doc;
    doc.Parse(map_of_list_json.c_str());
    std::unique_ptr<Type> type = SchemaParser::_type_from_json(doc);
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->to_string(), "map<string, list<int>>");
}

TEST(SchemaParserTest, parse_struct_with_nested_list_and_map) {
    std::unique_ptr<Schema> schema = SchemaParser::from_json(struct_with_nested_list_map_json);
    ASSERT_NE(schema, nullptr);

    const NestedField* nested_list_field = schema->find_field(18);
    ASSERT_NE(nested_list_field, nullptr);
    EXPECT_EQ(nested_list_field->field_name(), "nested_list");
    EXPECT_EQ(nested_list_field->is_required(), true);
    EXPECT_EQ(nested_list_field->field_type()->to_string(), "list<map<string, int>>");

    const NestedField* nested_map_field = schema->find_field(22);
    ASSERT_NE(nested_map_field, nullptr);
    EXPECT_EQ(nested_map_field->field_name(), "nested_map");
    EXPECT_EQ(nested_map_field->is_required(), true);
    EXPECT_EQ(nested_map_field->field_type()->to_string(), "map<string, list<int>>");
}

} // namespace iceberg
} // namespace doris
