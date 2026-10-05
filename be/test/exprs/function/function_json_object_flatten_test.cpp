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

#include <string>
#include <utility>
#include <vector>

#include "exprs/function/function_jsonb_transform.cpp"
#include "util/jsonb_document.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"

namespace doris {

namespace {

// Parse JSON text into JSONB bytes via the standard simdjson-backed parser.
std::string json_to_jsonb(const std::string& json) {
    JsonbWriter writer;
    auto status = JsonbParser::parse(json.data(), json.size(), writer);
    EXPECT_TRUE(status.ok()) << "parse failed: " << json << " -> " << status.to_string();
    return std::string(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
}

// Run flatten_json_object end-to-end starting from a JSON text input and
// returning the flattened result rendered back to JSON text. The parse →
// flatten → re-render trip exercises the same code path the SQL function
// follows: ColumnString(JSONB) -> flatten_json_object -> ColumnString(JSONB).
std::string flatten(const std::string& json_in) {
    const std::string in_bytes = json_to_jsonb(json_in);
    const JsonbDocument* doc = nullptr;
    auto status = JsonbDocument::checkAndCreateDocument(in_bytes.data(), in_bytes.size(), &doc);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(doc, nullptr);

    JsonbWriter writer;
    flatten_json_object(writer, doc->getValue());
    return JsonbToJson::jsonb_to_json_string(writer.getOutput()->getBuffer(),
                                             writer.getOutput()->getSize());
}

void check(const std::string& input, const std::string& expected) {
    EXPECT_EQ(flatten(input), expected) << "input: " << input;
}

} // namespace

TEST(function_json_object_flatten_test, two_level) {
    check(R"({"a":{"b":2}})", R"({"a.b":2})");
}

TEST(function_json_object_flatten_test, three_level) {
    check(R"({"a":{"b":{"c":3}}})", R"({"a.b.c":3})");
}

TEST(function_json_object_flatten_test, already_flat) {
    check(R"({"a":1,"b":"hi"})", R"({"a":1,"b":"hi"})");
}

TEST(function_json_object_flatten_test, empty_top_level_object) {
    check("{}", "{}");
}

TEST(function_json_object_flatten_test, empty_nested_object_is_leaf) {
    check(R"({"a":{}})", R"({"a":{}})");
}

TEST(function_json_object_flatten_test, deep_nesting) {
    check(R"({"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":1}}}}}}}}}}})",
          R"({"a.b.c.d.e.f.g.h.i.j.k":1})");
}

TEST(function_json_object_flatten_test, prefix_buffer_is_reset_across_siblings) {
    check(R"({"a":{"x":1},"b":{"y":2}})", R"({"a.x":1,"b.y":2})");
}

TEST(function_json_object_flatten_test, array_of_scalars_under_nested_path_stays_opaque) {
    check(R"({"a":{"b":[1,2,3]}})", R"({"a.b":[1,2,3]})");
}

TEST(function_json_object_flatten_test, array_of_objects_under_nested_path_stays_opaque) {
    // keep-arrays semantics: the array is a leaf value under "a.b"; the
    // inner object's key "d" must NOT show up at the flat level.
    check(R"({"a":{"b":[{"d":1},{"d":2}]}})", R"({"a.b":[{"d":1},{"d":2}]})");
}

TEST(function_json_object_flatten_test, mixed_object_scalar_and_array_leaves) {
    check(R"({"x":{"s":1,"a":[1,2],"o":{"k":"v"}}})", R"({"x.s":1,"x.a":[1,2],"x.o.k":"v"})");
}

TEST(function_json_object_flatten_test, null_leaf_at_top) {
    check(R"({"a":null})", R"({"a":null})");
}

TEST(function_json_object_flatten_test, null_leaf_nested) {
    check(R"({"a":{"b":null}})", R"({"a.b":null})");
}

TEST(function_json_object_flatten_test, top_level_scalar_pass_through) {
    check("42", "42");
    check("\"hello\"", "\"hello\"");
    check("null", "null");
    check("true", "true");
}

TEST(function_json_object_flatten_test, top_level_array_pass_through) {
    check(R"([1,2,3])", R"([1,2,3])");
    check(R"([{"x":1}])", R"([{"x":1}])");
}

TEST(function_json_object_flatten_test, literal_dotted_key_round_trips) {
    // A literal '.' inside a key collapses with real nesting at the flat layer
    // — the same documented-lossy collision NiFi FlattenJson accepts.
    check(R"({"a.b":2})", R"({"a.b":2})");
}

} // namespace doris
