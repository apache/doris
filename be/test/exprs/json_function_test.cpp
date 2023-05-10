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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>

#include "exprs/json_functions.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

// mock
class JsonFunctionTest : public testing::Test {
public:
    JsonFunctionTest() {}
};

TEST_F(JsonFunctionTest, json_path1) {
    bool wrap_explicitly;
    std::string json_raw_data(
            "[{\"k1\":\"v1\",\"keyname\":{\"ip\":\"10.10.0.1\",\"value\":20}},{\"k1\":\"v1-1\","
            "\"keyname\":{\"ip\":\"10.20.10.1\",\"value\":20}}]");
    rapidjson::Document jsonDoc;
    if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
        EXPECT_TRUE(false);
    }
    rapidjson::Value* res3;
    res3 = JsonFunctions::get_json_array_from_parsed_json("$.[*].keyname.ip", &jsonDoc,
                                                          jsonDoc.GetAllocator(), &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());

    res3 = JsonFunctions::get_json_array_from_parsed_json("$.[*].k1", &jsonDoc,
                                                          jsonDoc.GetAllocator(), &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());

    res3 = JsonFunctions::get_json_array_from_parsed_json("$", &jsonDoc, jsonDoc.GetAllocator(),
                                                          &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());
    rapidjson::StringBuffer buffer;
    buffer.Clear();
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    (*res3)[0].Accept(writer);
    EXPECT_EQ(json_raw_data, std::string(buffer.GetString()));
}

TEST_F(JsonFunctionTest, json_path_get_nullobject) {
    bool wrap_explicitly;
    std::string json_raw_data(
            "[{\"a\":\"a1\", \"b\":\"b1\", \"c\":\"c1\"},{\"a\":\"a2\", "
            "\"c\":\"c2\"},{\"a\":\"a3\", \"b\":\"b3\", \"c\":\"c3\"}]");
    rapidjson::Document jsonDoc;
    if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
        EXPECT_TRUE(false);
    }

    rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
            "$.[*].b", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
    EXPECT_TRUE(res3->IsArray());
    EXPECT_EQ(res3->Size(), 3);
}

TEST_F(JsonFunctionTest, json_path_test) {
    bool wrap_explicitly;
    {
        std::string json_raw_data("[{\"a\":\"a1\", \"b\":\"b1\"}, {\"a\":\"a2\", \"b\":\"b2\"}]");
        rapidjson::Document jsonDoc;
        if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
            EXPECT_TRUE(false);
        }

        rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
                "$.[*].a", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res3->IsArray());
        EXPECT_EQ(res3->Size(), 2);
    }
    {
        std::string json_raw_data(
                "{\"a\":[\"a1\",\"a2\"], \"b\":[\"b1\",\"b2\"], \"c\":[\"c1\"], \"d\":[], "
                "\"e\":\"e1\"}");
        rapidjson::Document jsonDoc;
        if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
            EXPECT_TRUE(false);
        }

        rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
                "$.a", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res3->IsArray());
        EXPECT_EQ(res3->Size(), 2);

        rapidjson::Value* res4 = JsonFunctions::get_json_array_from_parsed_json(
                "$.c", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res4->IsArray());
        EXPECT_EQ(res4->Size(), 1);
        EXPECT_FALSE(wrap_explicitly);

        rapidjson::Value* res5 = JsonFunctions::get_json_array_from_parsed_json(
                "$.d", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res5->IsArray());
        EXPECT_EQ(res5->Size(), 0);
        EXPECT_FALSE(wrap_explicitly);

        rapidjson::Value* res6 = JsonFunctions::get_json_array_from_parsed_json(
                "$.e", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        EXPECT_TRUE(res6->IsArray());
        EXPECT_EQ(res6->Size(), 1);
        EXPECT_TRUE(wrap_explicitly);
    }
}

} // namespace doris
