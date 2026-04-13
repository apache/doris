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
#include <string>

#include "function_test_util.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

using namespace ut_type;

TEST(FunctionJsonExtractSparkTest, BasicExtraction) {
    std::string func_name = "json_extract_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR},
                                Nullable {PrimitiveType::TYPE_VARCHAR}};

    DataSet data_set = {
            // Basic types
            {{STRING(R"({"k": "v"})"), STRING("$.k")}, STRING("v")},
            {{STRING(R"({"k": 123})"), STRING("$.k")}, STRING("123")},
            {{STRING(R"({"k": true})"), STRING("$.k")}, STRING("true")},
            {{STRING(R"({"k": false})"), STRING("$.k")}, STRING("false")},
            {{STRING(R"({"k": [1,2,3]})"), STRING("$.k")}, STRING("[1,2,3]")},
            {{STRING(R"({"k": {"sub": "val"}})"), STRING("$.k")}, STRING(R"({"sub":"val"})")},

            // Nested paths
            {{STRING(R"({"a": {"b": {"c": 123}}})"), STRING("$.a.b.c")}, STRING("123")},
            {{STRING(R"({"arr": ["a", "b", "c"]})"), STRING("$.arr[1]")}, STRING("b")},
            {{STRING(R"({"k": [{"sub": 1}, {"sub": 2}]})"), STRING("$.k[0].sub")}, STRING("1")},

            // JSON null -> SQL NULL (Spark compatibility)
            {{STRING(R"({"k": null})"), STRING("$.k")}, Null()},

            // SQL NULL inputs
            {{Null(), STRING("$.k")}, Null()},
            {{STRING(R"({"k": "v"})"), Null()}, Null()},
            {{Null(), Null()}, Null()},

            // Path not found
            {{STRING(R"({"k": "v"})"), STRING("$.missing")}, Null()},
            {{STRING(R"({"k": "v"})"), STRING("$.k.sub")}, Null()},

            // Empty JSON
            {{STRING(""), STRING("$.k")}, Null()},
            {{STRING("{}"), STRING("$.k")}, Null()},

            // Root path
            {{STRING(R"({"k": "v"})"), STRING("$")}, STRING(R"({"k":"v"})")},

            // Array at root
            {{STRING(R"([1,2,3])"), STRING("$[0]")}, STRING("1")},
            {{STRING(R"([1,2,3])"), STRING("$[2]")}, STRING("3")},

            // String values are unquoted
            {{STRING(R"({"name": "John Doe"})"), STRING("$.name")}, STRING("John Doe")},
            {{STRING(R"({"text": "hello world"})"), STRING("$.text")}, STRING("hello world")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonExtractSparkTest, SuperWildcardRejection) {
    std::string func_name = "json_extract_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR},
                                Nullable {PrimitiveType::TYPE_VARCHAR}};

    // $** (super wildcard) should return NULL in Spark
    DataSet data_set = {
            {{STRING(R"({"k": 123, "b": {"k": ["ab", "cd"]}})"), STRING("$**.k")}, Null()},
            {{STRING(R"({"key1": "v1", "key2": {"key3": "v3"}})"), STRING("$**.*")}, Null()},
            {{STRING(R"([[1,2,3], {"k": [4,5]}])"), STRING("$**.k")}, Null()},
            {{STRING(R"([1])"), STRING("$**[0]")}, Null()},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonExtractSparkTest, WildcardSupport) {
    std::string func_name = "json_extract_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR},
                                Nullable {PrimitiveType::TYPE_VARCHAR}};

    // Regular wildcards [*] and .* should work
    DataSet data_set = {
            // Array wildcard
            {{STRING(R"({"arr": [1,2,3]})"), STRING("$.arr[*]")}, STRING("[1,2,3]")},

            // Object wildcard
            {{STRING(R"({"a": 1, "b": 2})"), STRING("$.*")}, STRING("[1,2]")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonExtractSparkTest, MalformedJSON) {
    std::string func_name = "json_extract_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR},
                                Nullable {PrimitiveType::TYPE_VARCHAR}};

    // Malformed JSON should return NULL
    DataSet data_set = {
            {{STRING(R"({invalid})"), STRING("$.k")}, Null()},
            {{STRING(R"({"k": )"), STRING("$.k")}, Null()},
            {{STRING(R"(not json)"), STRING("$.k")}, Null()},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonExtractSparkTest, ComplexNestedStructures) {
    std::string func_name = "json_extract_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR},
                                Nullable {PrimitiveType::TYPE_VARCHAR}};

    DataSet data_set = {
            // Deeply nested object
            {{STRING(R"({"a": {"b": {"c": {"d": {"e": 123}}}}})"), STRING("$.a.b.c.d.e")},
             STRING("123")},

            // Array of objects
            {{STRING(R"({"users": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]})"),
              STRING("$.users[0].name")},
             STRING("Alice")},

            // Mixed types in array
            {{STRING(R"({"data": [1, "two", true, null, {"key": "val"}]})"),
              STRING("$.data[4].key")},
             STRING("val")},

            // Nested arrays
            {{STRING(R"({"matrix": [[1,2],[3,4]]})"), STRING("$.matrix[1][0]")}, STRING("3")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonExtractSparkTest, EdgeCases) {
    std::string func_name = "json_extract_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR},
                                Nullable {PrimitiveType::TYPE_VARCHAR}};

    DataSet data_set = {
            // Empty string value
            {{STRING(R"({"k": ""})"), STRING("$.k")}, STRING("")},

            // Zero
            {{STRING(R"({"k": 0})"), STRING("$.k")}, STRING("0")},

            // Negative number
            {{STRING(R"({"k": -123})"), STRING("$.k")}, STRING("-123")},

            // Float
            {{STRING(R"({"k": 3.14})"), STRING("$.k")}, STRING("3.14")},

            // Empty array
            {{STRING(R"({"k": []})"), STRING("$.k")}, STRING("[]")},

            // Empty object
            {{STRING(R"({"k": {}})"), STRING("$.k")}, STRING("{}")},

            // Array index out of bounds
            {{STRING(R"({"arr": [1,2,3]})"), STRING("$.arr[10]")}, Null()},

            // Negative array index (not supported in Spark)
            {{STRING(R"({"arr": [1,2,3]})"), STRING("$.arr[-1]")}, Null()},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonExtractSparkTest, SparkPathValidation) {
    std::string func_name = "json_extract_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR},
                                Nullable {PrimitiveType::TYPE_VARCHAR}};

    DataSet data_set = {
            // Path not starting with $ — Spark returns NULL
            {{STRING(R"({"name": "John"})"), STRING("name")}, Null()},

            // Empty path — Spark returns NULL
            {{STRING(R"({"name": "John"})"), STRING("")}, Null()},

            // Path starting with '.' instead of '$' — Spark returns NULL
            {{STRING(R"({"a": 1})"), STRING(".a")}, Null()},

            // Recursive descent (double dots) — Spark returns NULL
            {{STRING(R"({"a": {"b": 1}})"), STRING("$.a..b")}, Null()},

            // Leading whitespace — Spark returns NULL
            {{STRING(R"({"name": "John", "age": 30})"), STRING(" $.name")}, Null()},

            // Trailing whitespace — Spark returns NULL
            {{STRING(R"({"name": "John", "age": 30})"), STRING("$.name ")}, Null()},

            // Double-quoted field names — Spark returns NULL
            {{STRING(R"({"field name": "value"})"), STRING(R"($."field name")")}, Null()},
            {{STRING(R"({"a.b": "value"})"), STRING(R"($."a.b")")}, Null()},

            // Array slicing — Spark returns NULL
            {{STRING(R"({"items": [1,2,3]})"), STRING("$.items[0:2]")}, Null()},

            // Filter expressions — Spark returns NULL
            {{STRING(R"({"items": [{"id": 1}, {"id": 2}]})"), STRING("$.items[?(@.id > 1)]")},
             Null()},

            // Trailing dot — Spark returns NULL
            {{STRING(R"({"a": 1})"), STRING("$.a.")}, Null()},

            // Negative array index in bracket — Spark returns NULL
            {{STRING(R"({"arr": [1,2,3]})"), STRING("$.arr[-1]")}, Null()},

            // Valid paths should still work correctly
            {{STRING(R"({"name": "John"})"), STRING("$.name")}, STRING("John")},
            {{STRING(R"({"a": {"b": 1}})"), STRING("$.a.b")}, STRING("1")},
            {{STRING(R"({"items": [1,2,3]})"), STRING("$.items[0]")}, STRING("1")},
            {{STRING(R"({"k": "v"})"), STRING("$")}, STRING(R"({"k":"v"})")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
