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
// KIND, either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

#include "vec/json/simd_json_parser.h"

#include <gtest/gtest.h>

namespace doris::vectorized {

class SimdJSONParserTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SimdJSONParserTest, ElementTypeCheck) {
    const char* json =
            R"({"int": 42, "uint": 4294967295, "double": 3.14, "string": "hello", "array": [1, 2, 3], "object": {"key": "value"}, "bool": true, "null": null})";
    SimdJSONParser parser;
    SimdJSONParser::Element element;
    ASSERT_TRUE(parser.parse(json, strlen(json), element));

    auto obj = element.getObject();

    // Test int64
    SimdJSONParser::Element int_elem;
    ASSERT_TRUE(obj.find("int", int_elem));
    EXPECT_TRUE(int_elem.isInt64());
    EXPECT_FALSE(int_elem.isUInt64());
    EXPECT_FALSE(int_elem.isDouble());
    EXPECT_FALSE(int_elem.isString());
    EXPECT_FALSE(int_elem.isArray());
    EXPECT_FALSE(int_elem.isObject());
    EXPECT_FALSE(int_elem.isBool());
    EXPECT_FALSE(int_elem.isNull());
    EXPECT_EQ(int_elem.getInt64(), 42);

    // Test uint64
    SimdJSONParser::Element uint_elem;
    ASSERT_TRUE(obj.find("uint", uint_elem));
    EXPECT_FALSE(uint_elem.isUInt64());
    EXPECT_EQ(uint_elem.getUInt64(), 4294967295);

    // Test double
    SimdJSONParser::Element double_elem;
    ASSERT_TRUE(obj.find("double", double_elem));
    EXPECT_TRUE(double_elem.isDouble());
    EXPECT_DOUBLE_EQ(double_elem.getDouble(), 3.14);

    // Test string
    SimdJSONParser::Element string_elem;
    ASSERT_TRUE(obj.find("string", string_elem));
    EXPECT_TRUE(string_elem.isString());
    EXPECT_EQ(string_elem.getString(), "hello");

    // Test array
    SimdJSONParser::Element array_elem;
    ASSERT_TRUE(obj.find("array", array_elem));
    EXPECT_TRUE(array_elem.isArray());
    auto array = array_elem.getArray();
    EXPECT_EQ(array.size(), 3);
    EXPECT_EQ(array[0].getInt64(), 1);
    EXPECT_EQ(array[1].getInt64(), 2);
    EXPECT_EQ(array[2].getInt64(), 3);

    // Test object
    SimdJSONParser::Element object_elem;
    ASSERT_TRUE(obj.find("object", object_elem));
    EXPECT_TRUE(object_elem.isObject());
    auto nested_obj = object_elem.getObject();
    SimdJSONParser::Element nested_elem;
    ASSERT_TRUE(nested_obj.find("key", nested_elem));
    EXPECT_EQ(nested_elem.getString(), "value");

    // Test bool
    SimdJSONParser::Element bool_elem;
    ASSERT_TRUE(obj.find("bool", bool_elem));
    EXPECT_TRUE(bool_elem.isBool());
    EXPECT_TRUE(bool_elem.getBool());

    // Test null
    SimdJSONParser::Element null_elem;
    ASSERT_TRUE(obj.find("null", null_elem));
    EXPECT_TRUE(null_elem.isNull());
}

TEST_F(SimdJSONParserTest, ArrayOperations) {
    const char* json = R"([1, "two", 3.14, true, null, [1, 2], {"key": "value"}])";
    SimdJSONParser parser;
    SimdJSONParser::Element element;
    ASSERT_TRUE(parser.parse(json, strlen(json), element));

    auto array = element.getArray();
    EXPECT_EQ(array.size(), 7);

    // Test array iteration
    auto it1 = array.begin();
    auto it2 = array.begin();
    EXPECT_TRUE(it1 == it2);  // Test operator==
    EXPECT_FALSE(it1 != it2); // Test operator!=

    EXPECT_EQ((*it1).getInt64(), 1);
    ++it1;
    EXPECT_TRUE(it1 != it2);  // Test operator!= after increment
    EXPECT_FALSE(it1 == it2); // Test operator== after increment

    EXPECT_EQ((*it1).getString(), "two");
    ++it1;
    EXPECT_DOUBLE_EQ((*it1).getDouble(), 3.14);
    ++it1;
    EXPECT_TRUE((*it1).getBool());
    ++it1;
    EXPECT_TRUE((*it1).isNull());
    ++it1;
    EXPECT_TRUE((*it1).isArray());
    ++it1;
    EXPECT_TRUE((*it1).isObject());
    ++it1;
    EXPECT_EQ(it1, array.end());

    // Test array indexing
    EXPECT_EQ(array[0].getInt64(), 1);
    EXPECT_EQ(array[1].getString(), "two");
    EXPECT_DOUBLE_EQ(array[2].getDouble(), 3.14);
    EXPECT_TRUE(array[3].getBool());
    EXPECT_TRUE(array[4].isNull());
    EXPECT_TRUE(array[5].isArray());
    EXPECT_TRUE(array[6].isObject());

    // Test iterator comparison with end
    auto end_it = array.end();
    EXPECT_TRUE(it1 == end_it);
    EXPECT_FALSE(it1 != end_it);
    EXPECT_FALSE(it2 == end_it);
    EXPECT_TRUE(it2 != end_it);
}

TEST_F(SimdJSONParserTest, ObjectOperations) {
    const char* json =
            R"({"int": 42, "string": "hello", "array": [1, 2, 3], "object": {"nested": "value"}})";
    SimdJSONParser parser;
    SimdJSONParser::Element element;
    ASSERT_TRUE(parser.parse(json, strlen(json), element));

    auto obj = element.getObject();
    EXPECT_EQ(obj.size(), 4);

    // Test object iteration
    auto it1 = obj.begin();
    auto it2 = obj.begin();
    EXPECT_TRUE(it1 == it2);  // Test operator==
    EXPECT_FALSE(it1 != it2); // Test operator!=

    EXPECT_EQ((*it1).first, "int");
    EXPECT_EQ((*it1).second.getInt64(), 42);
    ++it1;
    EXPECT_TRUE(it1 != it2);  // Test operator!= after increment
    EXPECT_FALSE(it1 == it2); // Test operator== after increment

    EXPECT_EQ((*it1).first, "string");
    EXPECT_EQ((*it1).second.getString(), "hello");
    ++it1;
    EXPECT_EQ((*it1).first, "array");
    EXPECT_TRUE((*it1).second.isArray());
    ++it1;
    EXPECT_EQ((*it1).first, "object");
    EXPECT_TRUE((*it1).second.isObject());
    ++it1;
    EXPECT_EQ(it1, obj.end());

    // Test iterator comparison with end
    auto end_it = obj.end();
    EXPECT_TRUE(it1 == end_it);
    EXPECT_FALSE(it1 != end_it);
    EXPECT_FALSE(it2 == end_it);
    EXPECT_TRUE(it2 != end_it);

    // Test find operation with string key
    SimdJSONParser::Element found_elem;
    EXPECT_TRUE(obj.find("int", found_elem));
    EXPECT_EQ(found_elem.getInt64(), 42);
    EXPECT_TRUE(obj.find("string", found_elem));
    EXPECT_EQ(found_elem.getString(), "hello");
    EXPECT_FALSE(obj.find("nonexistent", found_elem));

    // Test find operation with PathInData
    // Test simple path
    PathInData simple_path("int");
    EXPECT_TRUE(obj.find(simple_path, found_elem));
    EXPECT_EQ(found_elem.getInt64(), 42);

    // Test nested path
    PathInData nested_path("object.nested");
    EXPECT_TRUE(obj.find(nested_path, found_elem));
    EXPECT_EQ(found_elem.getString(), "value");

    // Test array path
    PathInData array_path("array");
    EXPECT_TRUE(obj.find(array_path, found_elem));
    EXPECT_TRUE(found_elem.isArray());
    auto array = found_elem.getArray();
    EXPECT_EQ(array.size(), 3);
    EXPECT_EQ(array[0].getInt64(), 1);

    // Test non-existent path
    PathInData non_existent_path("nonexistent");
    EXPECT_FALSE(obj.find(non_existent_path, found_elem));

    // Test invalid nested path
    PathInData invalid_nested_path("int.nested");
    EXPECT_FALSE(obj.find(invalid_nested_path, found_elem));

    // Test object indexing
    EXPECT_EQ(obj[0].first, "int");
    EXPECT_EQ(obj[0].second.getInt64(), 42);
    EXPECT_EQ(obj[1].first, "string");
    EXPECT_EQ(obj[1].second.getString(), "hello");
}

TEST_F(SimdJSONParserTest, NestedStructures) {
    const char* json = R"({
        "array_of_objects": [
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second"}
        ],
        "object_with_array": {
            "numbers": [1, 2, 3],
            "strings": ["a", "b", "c"]
        }
    })";
    SimdJSONParser parser;
    SimdJSONParser::Element element;
    ASSERT_TRUE(parser.parse(json, strlen(json), element));

    auto obj = element.getObject();

    // Test array of objects
    SimdJSONParser::Element array_elem;
    ASSERT_TRUE(obj.find("array_of_objects", array_elem));
    auto array = array_elem.getArray();
    EXPECT_EQ(array.size(), 2);

    auto first_obj = array[0].getObject();
    SimdJSONParser::Element id_elem, name_elem;
    ASSERT_TRUE(first_obj.find("id", id_elem));
    ASSERT_TRUE(first_obj.find("name", name_elem));
    EXPECT_EQ(id_elem.getInt64(), 1);
    EXPECT_EQ(name_elem.getString(), "first");

    // Test object with arrays
    SimdJSONParser::Element obj_elem;
    ASSERT_TRUE(obj.find("object_with_array", obj_elem));
    auto nested_obj = obj_elem.getObject();

    SimdJSONParser::Element numbers_elem, strings_elem;
    ASSERT_TRUE(nested_obj.find("numbers", numbers_elem));
    ASSERT_TRUE(nested_obj.find("strings", strings_elem));

    auto numbers = numbers_elem.getArray();
    auto strings = strings_elem.getArray();

    EXPECT_EQ(numbers.size(), 3);
    EXPECT_EQ(numbers[0].getInt64(), 1);
    EXPECT_EQ(numbers[1].getInt64(), 2);
    EXPECT_EQ(numbers[2].getInt64(), 3);

    EXPECT_EQ(strings.size(), 3);
    EXPECT_EQ(strings[0].getString(), "a");
    EXPECT_EQ(strings[1].getString(), "b");
    EXPECT_EQ(strings[2].getString(), "c");
}

TEST_F(SimdJSONParserTest, InvalidJSON) {
    const char* invalid_json = R"({"unclosed": "object")";
    SimdJSONParser parser;
    SimdJSONParser::Element element;
    EXPECT_FALSE(parser.parse(invalid_json, strlen(invalid_json), element));
}

// Add a new test case for complex nested paths
TEST_F(SimdJSONParserTest, ComplexNestedPaths) {
    const char* json = R"({
        "level1": {
            "level2": {
                "level3": {
                    "value": 42
                }
            },
            "array": [
                {"id": 1, "name": "first"},
                {"id": 2, "name": "second"}
            ]
        }
    })";
    SimdJSONParser parser;
    SimdJSONParser::Element element;
    ASSERT_TRUE(parser.parse(json, strlen(json), element));

    auto obj = element.getObject();
    SimdJSONParser::Element found_elem;

    // Test deep nested path
    PathInData deep_path("level1.level2.level3.value");
    EXPECT_TRUE(obj.find(deep_path, found_elem));
    EXPECT_EQ(found_elem.getInt64(), 42);

    // Test path with array
    PathInData array_path("level1.array");
    EXPECT_TRUE(obj.find(array_path, found_elem));
    EXPECT_TRUE(found_elem.isArray());
    auto array = found_elem.getArray();
    EXPECT_EQ(array.size(), 2);
    EXPECT_EQ(array[0].getObject().find("id", found_elem), true);
    EXPECT_EQ(found_elem.getInt64(), 1);

    // Test invalid deep path
    PathInData invalid_deep_path("level1.level2.nonexistent.value");
    EXPECT_FALSE(obj.find(invalid_deep_path, found_elem));

    // Test path with wrong type
    PathInData wrong_type_path("level1.level2.level3");
    EXPECT_TRUE(obj.find(wrong_type_path, found_elem));
    EXPECT_TRUE(found_elem.isObject());
    EXPECT_FALSE(obj.find(PathInData("level1.level2.level3.value.nonexistent"), found_elem));
}

} // namespace doris::vectorized