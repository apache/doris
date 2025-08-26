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

#include "vec/json/flatten_jsonb_util.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/exception.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "util/jsonb_document.h"
#include "vec/core/field.h"
#include "vec/json/path_in_data.h"

using doris::JsonbDocument;
using doris::JsonbValue;
using doris::JsonbType;
using doris::vectorized::Array;
using doris::vectorized::FlattenJsonbUtil;
using doris::vectorized::FlattenResult;
using doris::vectorized::PathInData;
using doris::PrimitiveType;

namespace {

std::string join_path_keys(const PathInData& path) {
    const auto& parts = path.get_parts();
    std::string out;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            out.push_back('.');
        }
        out.append(parts[i].key);
    }
    return out;
}

FlattenResult run_flatten_from_string(const std::string& json_text) {
    doris::JsonBinaryValue jsonb_value;
    EXPECT_TRUE(jsonb_value.from_json_string(json_text).ok());
    FlattenJsonbUtil util;
    FlattenResult result;
    util.flatten(jsonb_value.value(), jsonb_value.size(), &result);
    return result;
}

void assert_jsonb_top_type(const doris::vectorized::Field& field, JsonbType expect_type) {
    const auto& jb = field.get<doris::vectorized::JsonbField>();
    JsonbDocument* doc = nullptr;
    EXPECT_TRUE(JsonbDocument::checkAndCreateDocument(jb.get_value(), jb.get_size(), &doc).ok());
    ASSERT_NE(doc, nullptr);
    JsonbValue* val = doc->getValue();
    ASSERT_EQ(val->type, expect_type);
}

} // namespace

TEST(FlattenJsonbUtilTest, FlattenBasicTypes) {
    // null
    {
        auto res = run_flatten_from_string("null");
        ASSERT_EQ(res.paths.size(), 1U);
        EXPECT_TRUE(res.paths[0].get_parts().empty());
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_NULL);
        EXPECT_TRUE(res.values[0].field.is_null());
    }
    // true
    {
        auto res = run_flatten_from_string("true");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_BOOLEAN);
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::Int64>(), 1);
    }
    // false
    {
        auto res = run_flatten_from_string("false");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_BOOLEAN);
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::Int64>(), 0);
    }
    // int8
    {
        auto res = run_flatten_from_string("-7");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::Int64>(), -7);
    }
    // int16
    {
        auto res = run_flatten_from_string("1234");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::Int64>(), 1234);
    }
    // int32
    {
        auto res = run_flatten_from_string("123456");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::Int64>(), 123456);
    }
    // int64
    {
        auto res = run_flatten_from_string("9223372036854770000");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::Int64>(), 9223372036854770000LL);
    }
    // int128
    {
        auto res = run_flatten_from_string("18446744073709551616");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_LARGEINT);
        auto expected = static_cast<doris::int128_t>(1);
        expected <<= 64; // 2^64
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::Int128>(), expected);
    }
    // double
    {
        auto res = run_flatten_from_string("3.14159");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_DOUBLE);
        EXPECT_DOUBLE_EQ(res.values[0].field.get<doris::vectorized::Float64>(), 3.14159);
    }
    // float
    {
        auto res = run_flatten_from_string("2.5");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_DOUBLE);
        EXPECT_FLOAT_EQ(static_cast<float>(res.values[0].field.get<doris::vectorized::Float64>()),
                        2.5F);
    }
    // string
    {
        auto res = run_flatten_from_string(R"("hello")");
        ASSERT_EQ(res.values.size(), 1U);
        EXPECT_EQ(res.values[0].base_scalar_type_id, PrimitiveType::TYPE_STRING);
        EXPECT_EQ(res.values[0].field.get<doris::vectorized::String>(), "hello");
    }
}

TEST(FlattenJsonbUtilTest, FlattenSimpleObject) {
    auto res = run_flatten_from_string(R"({"a":1,"b":true,"c":null,"d":"x"})");
    ASSERT_EQ(res.paths.size(), 4U);
    ASSERT_EQ(res.values.size(), 4U);

    std::map<std::string, size_t> idx;
    for (size_t i = 0; i < res.paths.size(); ++i) {
        idx[res.paths[i].get_path()] = i;
    }

    EXPECT_TRUE(idx.count("a"));
    EXPECT_TRUE(idx.count("b"));
    EXPECT_TRUE(idx.count("c"));
    EXPECT_TRUE(idx.count("d"));

    {
        const auto& v = res.values[idx["a"]];
        EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_EQ(v.field.get<doris::vectorized::Int64>(), 1);
    }
    {
        const auto& v = res.values[idx["b"]];
        EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_BOOLEAN);
        EXPECT_EQ(v.field.get<doris::vectorized::Int64>(), 1);
    }
    {
        const auto& v = res.values[idx["c"]];
        EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_NULL);
        EXPECT_TRUE(v.field.is_null());
    }
    {
        const auto& v = res.values[idx["d"]];
        EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_STRING);
        EXPECT_EQ(v.field.get<doris::vectorized::String>(), "x");
    }
}

TEST(FlattenJsonbUtilTest, FlattenNestedObjectAndArrays) {
    auto res = run_flatten_from_string(R"({"o":{"x":1,"y":[1,2]},"arr":[3,4]})");

    std::map<std::string, size_t> idx;
    for (size_t i = 0; i < res.paths.size(); ++i) {
        idx[res.paths[i].get_path()] = i;
    }

    EXPECT_TRUE(idx.count("o.x"));
    EXPECT_TRUE(idx.count("o.y"));
    EXPECT_TRUE(idx.count("arr"));

    {
        const auto& v = res.values[idx["o.x"]];
        EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_EQ(v.field.get<doris::vectorized::Int64>(), 1);
    }
    {
        const auto& v = res.values[idx["o.y"]];
        EXPECT_EQ(v.num_dimensions, 1);
        EXPECT_EQ(v.field.get_type(), PrimitiveType::TYPE_ARRAY);
        EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        const auto& arr = v.field.get<Array>();
        ASSERT_EQ(arr.size(), 2U);
        EXPECT_EQ(arr[0].get<doris::vectorized::Int64>(), 1);
        EXPECT_EQ(arr[1].get<doris::vectorized::Int64>(), 2);
    }
    {
        const auto& v = res.values[idx["arr"]];
        EXPECT_EQ(v.num_dimensions, 1);
        EXPECT_EQ(v.field.get_type(), PrimitiveType::TYPE_ARRAY);
        EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
        const auto& arr = v.field.get<Array>();
        ASSERT_EQ(arr.size(), 2U);
        EXPECT_EQ(arr[0].get<doris::vectorized::Int64>(), 3);
        EXPECT_EQ(arr[1].get<doris::vectorized::Int64>(), 4);
    }
}

TEST(FlattenJsonbUtilTest, FlattenEmptyObject) {
    auto res = run_flatten_from_string(R"({})");
    EXPECT_TRUE(res.paths.empty());
    EXPECT_TRUE(res.values.empty());
}

TEST(FlattenJsonbUtilTest, FlattenEmptyArrayObject) {
    auto res = run_flatten_from_string(R"([{},{},{}])");
    ASSERT_EQ(res.values.size(), 1U);
    const auto& v = res.values[0];
    EXPECT_EQ(v.num_dimensions, 0);
    EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_NULL);
    EXPECT_TRUE(v.field.is_null());

    auto res2 = run_flatten_from_string(R"([])");
    ASSERT_EQ(res2.values.size(), 1U);
    const auto& v2 = res2.values[0];
    EXPECT_EQ(v2.num_dimensions, 0);
    EXPECT_EQ(v2.base_scalar_type_id, PrimitiveType::TYPE_NULL);
    EXPECT_TRUE(v2.field.is_null());

    auto res3 = run_flatten_from_string(R"([[[]]])");
    ASSERT_EQ(res3.values.size(), 1U);
    const auto& v3 = res3.values[0];
    EXPECT_EQ(v3.num_dimensions, 0);
    EXPECT_EQ(v3.base_scalar_type_id, PrimitiveType::TYPE_NULL);
    EXPECT_TRUE(v3.field.is_null());
}

TEST(FlattenJsonbUtilTest, FlattenArraySingleNull) {
    auto res = run_flatten_from_string(R"([null])");
    ASSERT_EQ(res.values.size(), 1U);
    const auto& v = res.values[0];
    EXPECT_EQ(v.num_dimensions, 0);
    EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_NULL);
    EXPECT_TRUE(v.field.is_null());

    auto res2 = run_flatten_from_string(R"([[null, null]])");
    ASSERT_EQ(res2.values.size(), 1U);
    const auto& v2 = res2.values[0];
    EXPECT_EQ(v2.num_dimensions, 0);
    EXPECT_EQ(v2.base_scalar_type_id, PrimitiveType::TYPE_NULL);
    EXPECT_TRUE(v2.field.is_null());
}

TEST(FlattenJsonbUtilTest, FlattenSingleNullField) {
    auto res = run_flatten_from_string(R"({"a": null})");
    ASSERT_EQ(res.paths.size(), 1U);
    ASSERT_EQ(res.values.size(), 1U);
    EXPECT_EQ(join_path_keys(res.paths[0]), "a");
    const auto& v = res.values[0];
    EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_NULL);
    EXPECT_TRUE(v.field.is_null());
}

// [null, 1, null, 2] -> array<int> with base scalar TYPE_BIGINT
TEST(FlattenJsonbUtilTest, FlattenArraySameTypeAndNulls) {
    auto res = run_flatten_from_string("[null,1,null,2]");
    ASSERT_EQ(res.values.size(), 1U);
    const auto& v = res.values[0];
    EXPECT_EQ(v.num_dimensions, 1);
    EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_BIGINT);
    const auto& arr = v.field.get<Array>();
    ASSERT_EQ(arr.size(), 4U);
    EXPECT_TRUE(arr[0].is_null());
    EXPECT_EQ(arr[1].get<doris::vectorized::Int64>(), 1);
    EXPECT_TRUE(arr[2].is_null());
    EXPECT_EQ(arr[3].get<doris::vectorized::Int64>(), 2);
}

// [null, true, false, int8, int16, int32, int64, int128, double, float, "s"]
TEST(FlattenJsonbUtilTest, FlattenArrayDifferentTypesToJsonb) {
    auto res = run_flatten_from_string("[null,true,false,7,16,32,64,1.25,0.5,\"s\"]");
    ASSERT_EQ(res.values.size(), 1U);
    const auto& v = res.values[0];
    EXPECT_EQ(v.num_dimensions, 1);
    EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_JSONB);
    const auto& arr = v.field.get<Array>();
    ASSERT_EQ(arr.size(), 10U);
    // check a few representative jsonb element types
    assert_jsonb_top_type(arr[0], JsonbType::T_Null);
    assert_jsonb_top_type(arr[1], JsonbType::T_True);
    assert_jsonb_top_type(arr[2], JsonbType::T_False);
    assert_jsonb_top_type(arr[3], JsonbType::T_Int64);
    assert_jsonb_top_type(arr[4], JsonbType::T_Int64);
    assert_jsonb_top_type(arr[5], JsonbType::T_Int64);
    assert_jsonb_top_type(arr[6], JsonbType::T_Int64);
    assert_jsonb_top_type(arr[7], JsonbType::T_Double);
    assert_jsonb_top_type(arr[8], JsonbType::T_Double);
    assert_jsonb_top_type(arr[9], JsonbType::T_String);
}

// [ {"k":1}, 2 ] -> whole array serialized as a single jsonb
TEST(FlattenJsonbUtilTest, FlattenArrayShouldParseAsJsonb) {
    auto res = run_flatten_from_string("[{\"k\":1},2]");
    ASSERT_EQ(res.values.size(), 1U);
    const auto& v = res.values[0];
    EXPECT_EQ(v.num_dimensions, 0);
    EXPECT_EQ(v.base_scalar_type_id, PrimitiveType::TYPE_JSONB);
    assert_jsonb_top_type(v.field, JsonbType::T_Array);
}
