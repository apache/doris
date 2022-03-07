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
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <re2/re2.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <string>

#include "common/object_pool.h"
#include "exprs/anyval_util.h"
#include "exprs/json_functions.h"
#include "runtime/runtime_state.h"
#include "util/logging.h"
#include "util/stopwatch.hpp"
namespace doris {

// mock
class JsonFunctionTest : public testing::Test {
public:
    JsonFunctionTest() {}
};

TEST_F(JsonFunctionTest, string) {
    std::string json_string("{\"id\":\"name\",\"age\":11,\"money\":123000.789}");
    std::string path_string("$.id");
    rapidjson::Document document1;
    rapidjson::Value* res1 = JsonFunctions::get_json_object(nullptr, json_string, path_string,
                                                            JSON_FUN_STRING, &document1);
    ASSERT_EQ(std::string(res1->GetString()), "name");

    std::string json_string2("{\"price a\": [0,1,2],\"couponFee\":0}");
    std::string path_string2("$.price a");
    rapidjson::Document document2;
    rapidjson::Value* res2 = JsonFunctions::get_json_object(nullptr, json_string2, path_string2,
                                                            JSON_FUN_STRING, &document2);
    rapidjson::StringBuffer buf2;
    rapidjson::Writer<rapidjson::StringBuffer> writer2(buf2);
    res2->Accept(writer2);
    ASSERT_EQ(std::string(buf2.GetString()), "[0,1,2]");

    std::string json_string3("{\"price a\": [],\"couponFee\":0}");
    std::string path_string3("$.price a");
    rapidjson::Document document3;
    rapidjson::Value* res3 = JsonFunctions::get_json_object(nullptr, json_string3, path_string3,
                                                            JSON_FUN_STRING, &document3);
    rapidjson::StringBuffer buf3;
    rapidjson::Writer<rapidjson::StringBuffer> writer3(buf3);
    res3->Accept(writer3);
    ASSERT_EQ(std::string(buf3.GetString()), "[]");

    std::string json_string4("{\"price a\": [],\"couponFee\":null}");
    std::string path_string4("$.couponFee");
    rapidjson::Document document4;
    rapidjson::Value* res4 = JsonFunctions::get_json_object(nullptr, json_string4, path_string4,
                                                            JSON_FUN_STRING, &document4);
    ASSERT_TRUE(res4->IsNull());

    std::string json_string5(
            "{\"blockNames\": {},"
            "\"seatCategories\": [{\"areas\": [{\"areaId\": 205705999,\"blockIds\": []},"
            "{\"areaId\": 205705998,\"blockIds\": []}],\"seatCategoryId\": 338937290}]}");
    std::string path_string5_1("$.blockNames");
    rapidjson::Document document5_1;
    rapidjson::Value* res5_1 = JsonFunctions::get_json_object(nullptr, json_string5, path_string5_1,
                                                              JSON_FUN_STRING, &document5_1);
    rapidjson::StringBuffer buf5_1;
    rapidjson::Writer<rapidjson::StringBuffer> writer5_1(buf5_1);
    res5_1->Accept(writer5_1);
    ASSERT_EQ(std::string(buf5_1.GetString()), "{}");

    std::string path_string5_2("$.seatCategories.areas.blockIds");
    rapidjson::Document document5_2;
    rapidjson::Value* res5_2 = JsonFunctions::get_json_object(nullptr, json_string5, path_string5_2,
                                                              JSON_FUN_STRING, &document5_2);
    rapidjson::StringBuffer buf5_2;
    rapidjson::Writer<rapidjson::StringBuffer> writer5_2(buf5_2);
    res5_2->Accept(writer5_2);
    ASSERT_EQ(std::string(buf5_2.GetString()), "[]");

    std::string path_string5_3("$.seatCategories.areas[0].areaId");
    rapidjson::Document document5_3;
    rapidjson::Value* res5_3 = JsonFunctions::get_json_object(nullptr, json_string5, path_string5_3,
                                                              JSON_FUN_STRING, &document5_2);
    rapidjson::StringBuffer buf5_3;
    rapidjson::Writer<rapidjson::StringBuffer> writer5_3(buf5_3);
    res5_3->Accept(writer5_3);
    ASSERT_EQ(std::string(buf5_3.GetString()), "205705999");
}

TEST_F(JsonFunctionTest, json_quote) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(StringVal::null(), JsonFunctions::json_quote(context, StringVal::null()));

    doris_udf::StringVal res1 = JsonFunctions::json_quote(context, StringVal("null"));
    ASSERT_EQ(std::string("\"null\""), std::string((char*)res1.ptr, res1.len));

    doris_udf::StringVal res2 = JsonFunctions::json_quote(context, StringVal("[1, 2, 3]"));
    ASSERT_EQ(std::string("\"[1, 2, 3]\""), std::string((char*)res2.ptr, res2.len));

    doris_udf::StringVal res3 = JsonFunctions::json_quote(context, StringVal("\n\b\r\t"));
    ASSERT_EQ(std::string("\"\\n\\b\\r\\t\""), std::string((char*)res3.ptr, res3.len));

    doris_udf::StringVal res4 = JsonFunctions::json_quote(context, StringVal("\""));
    ASSERT_EQ(std::string("\"\\\"\""), std::string((char*)res4.ptr, res4.len));

    doris_udf::StringVal json_str= {""};
    doris_udf::StringVal res5 = JsonFunctions::json_quote(context, json_str);
    ASSERT_EQ(std::string("\"\""), std::string((char*)res5.ptr, res5.len));
    delete context;
}

TEST_F(JsonFunctionTest, json_array) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    doris_udf::StringVal json_str1[2] = {"[1,2,3]", "5"};
    doris_udf::StringVal res1 = JsonFunctions::json_array(context, 2, json_str1);
    ASSERT_EQ(std::string("[\"[1,2,3]\"]"), std::string((char*)res1.ptr, res1.len));

    doris_udf::StringVal json_str2[4] = {"1", "abc", "null", "250"};
    doris_udf::StringVal res2 = JsonFunctions::json_array(context, 4, json_str2);
    ASSERT_EQ(std::string("[1,\"abc\",null]"), std::string((char*)res2.ptr, res2.len));

    doris_udf::StringVal json_str3[1]= {""};
    doris_udf::StringVal res3 = JsonFunctions::json_array(context, 1, json_str3);
    ASSERT_EQ(std::string("[]"), std::string((char*)res3.ptr, res3.len));

    doris_udf::StringVal json_str4[2]= {"null","0"};
    doris_udf::StringVal res4 = JsonFunctions::json_array(context, 2, json_str4);
    ASSERT_EQ(std::string("[null]"), std::string((char*)res4.ptr, res4.len));
    delete context;
}

TEST_F(JsonFunctionTest, json_object) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::StringVal json_str1[3] = {"id", "87", "52"};
    doris_udf::StringVal res1 = JsonFunctions::json_object(context, 3, json_str1);
    ASSERT_EQ(std::string("{\"id\":87}"), std::string((char*)res1.ptr, res1.len));

    doris_udf::StringVal json_str2[5] = {"name", "Jack", "score", "[87,98,90]", "5555"};
    doris_udf::StringVal res2 = JsonFunctions::json_object(context, 5, json_str2);
    ASSERT_EQ(std::string("{\"name\":\"Jack\",\"score\":\"[87,98,90]\"}"),
              std::string((char*)res2.ptr, res2.len));

    doris_udf::StringVal json_str3[3] = {"key", "null","50"};
    doris_udf::StringVal res3 = JsonFunctions::json_object(context, 3, json_str3);
    ASSERT_EQ(std::string("{\"key\":null}"), std::string((char*)res3.ptr, res3.len));  

    doris_udf::StringVal json_str4[1]= {""};
    doris_udf::StringVal res4 = JsonFunctions::json_object(context, 1, json_str4);
    ASSERT_EQ(std::string("{}"), std::string((char*)res4.ptr, res4.len));        
    delete context;
}

TEST_F(JsonFunctionTest, int) {
    std::string json_string("{\"id\":\"name\",\"age\":11,\"money\":123000.789}");
    std::string path_string("$.age");
    rapidjson::Document document;
    rapidjson::Value* res = JsonFunctions::get_json_object(nullptr, json_string, path_string,
                                                           JSON_FUN_INT, &document);
    ASSERT_EQ(res->GetInt(), 11);

    std::string json_string1(
            "{\"list\":[{\"id\":[{\"aa\":1}]},{\"id\":[{\"aa\":\"cc\"}]},"
            "{\"id\":[{\"kk\":\"cc\"}]}]}");
    std::string path_string1("$.list.id.aa[0]");
    rapidjson::Document document1;
    rapidjson::Value* res1 = JsonFunctions::get_json_object(nullptr, json_string1, path_string1,
                                                            JSON_FUN_INT, &document1);
    ASSERT_EQ(res1->GetInt(), 1);

    std::string json_string2("[1,2,3,5,8,0]");
    std::string path_string2("$.[3]");
    rapidjson::Document document2;
    rapidjson::Value* res2 = JsonFunctions::get_json_object(nullptr, json_string2, path_string2,
                                                            JSON_FUN_INT, &document2);
    ASSERT_EQ(res2->GetInt(), 5);

    std::string json_string3("{\"price a\": [0,1,2],\"couponFee\":0.0}");
    std::string path_string3_1("$.price a[3]");
    rapidjson::Document document3_1;
    rapidjson::Value* res3_1 = JsonFunctions::get_json_object(nullptr, json_string3, path_string3_1,
                                                              JSON_FUN_INT, &document3_1);
    ASSERT_TRUE(res3_1 == nullptr);

    std::string path_string3_2("$.couponFee");
    rapidjson::Document document3_2;
    rapidjson::Value* res3_2 = JsonFunctions::get_json_object(nullptr, json_string3, path_string3_2,
                                                              JSON_FUN_INT, &document3_2);
    ASSERT_FALSE(res3_2->IsInt());
}

TEST_F(JsonFunctionTest, double) {
    std::string json_string("{\"id\":\"name\",\"age\":11,\"money\":123000.789}");
    std::string path_string("$.money");
    rapidjson::Document document;
    rapidjson::Value* res = JsonFunctions::get_json_object(nullptr, json_string, path_string,
                                                           JSON_FUN_DOUBLE, &document);
    ASSERT_EQ(res->GetDouble(), 123000.789);

    std::string path_string2("$.age");
    rapidjson::Document document2;
    rapidjson::Value* res2 = JsonFunctions::get_json_object(nullptr, json_string, path_string2,
                                                            JSON_FUN_DOUBLE, &document2);
    ASSERT_EQ(res2->GetInt(), 11);
}

TEST_F(JsonFunctionTest, special_char) {
    std::string json_string("{\"key with.dot\": [\"v1\", \"v2\"]}");
    std::string path_string("$.\"key with.dot\"[1]");
    rapidjson::Document document;
    rapidjson::Value* res = JsonFunctions::get_json_object(nullptr, json_string, path_string,
                                                           JSON_FUN_DOUBLE, &document);
    ASSERT_FALSE(res->GetString() == nullptr);
    ASSERT_EQ(std::string(res->GetString()), "v2");

    std::string json_string2("{\"key with|\": [\"v1\", \"v2\"]}");
    std::string path_string2("$.key with|[0]");
    rapidjson::Document document2;
    rapidjson::Value* res2 = JsonFunctions::get_json_object(nullptr, json_string2, path_string2,
                                                            JSON_FUN_DOUBLE, &document2);
    ASSERT_FALSE(res2->GetString() == nullptr);
    ASSERT_EQ(std::string(res2->GetString()), "v1");

    std::string json_string3("{\"key with.dot\": [{\"key2.dot\":\"v1\"}, {\"key3.dot\":\"v2\"}]}");
    std::string path_string3("$.\"key with.dot\"[0].\"key2.dot\"");
    rapidjson::Document document3;
    rapidjson::Value* res3 = JsonFunctions::get_json_object(nullptr, json_string3, path_string3,
                                                            JSON_FUN_DOUBLE, &document3);
    ASSERT_FALSE(res3->GetString() == nullptr);
    ASSERT_EQ(std::string(res3->GetString()), "v1");
}

TEST_F(JsonFunctionTest, json_path1) {
    bool wrap_explicitly;
    std::string json_raw_data(
            "[{\"k1\":\"v1\",\"keyname\":{\"ip\":\"10.10.0.1\",\"value\":20}},{\"k1\":\"v1-1\","
            "\"keyname\":{\"ip\":\"10.20.10.1\",\"value\":20}}]");
    rapidjson::Document jsonDoc;
    if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
        ASSERT_TRUE(false);
    }
    rapidjson::Value* res3;
    res3 = JsonFunctions::get_json_array_from_parsed_json("$.[*].keyname.ip", &jsonDoc,
                                                          jsonDoc.GetAllocator(), &wrap_explicitly);
    ASSERT_TRUE(res3->IsArray());
    for (int i = 0; i < res3->Size(); i++) {
        std::cout << (*res3)[i].GetString() << std::endl;
    }

    res3 = JsonFunctions::get_json_array_from_parsed_json("$.[*].k1", &jsonDoc,
                                                          jsonDoc.GetAllocator(), &wrap_explicitly);
    ASSERT_TRUE(res3->IsArray());
    for (int i = 0; i < res3->Size(); i++) {
        std::cout << (*res3)[i].GetString() << std::endl;
    }

    res3 = JsonFunctions::get_json_array_from_parsed_json("$", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
    ASSERT_TRUE(res3->IsArray());
    rapidjson::StringBuffer buffer;
    buffer.Clear();
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    (*res3)[0].Accept(writer);
    ASSERT_EQ(json_raw_data, std::string(buffer.GetString()));
}

TEST_F(JsonFunctionTest, json_path_get_nullobject) {
    bool wrap_explicitly;
    std::string json_raw_data(
            "[{\"a\":\"a1\", \"b\":\"b1\", \"c\":\"c1\"},{\"a\":\"a2\", "
            "\"c\":\"c2\"},{\"a\":\"a3\", \"b\":\"b3\", \"c\":\"c3\"}]");
    rapidjson::Document jsonDoc;
    if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
        ASSERT_TRUE(false);
    }

    rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json("$.[*].b", &jsonDoc,
                                                                            jsonDoc.GetAllocator(), &wrap_explicitly);
    ASSERT_TRUE(res3->IsArray());
    ASSERT_EQ(res3->Size(), 3);
    for (int i = 0; i < res3->Size(); i++) {
        if ((*res3)[i].GetType() == rapidjson::Type::kNullType) {
            std::cout << "null ";
        } else {
            std::cout << (*res3)[i].GetString() << " ";
        }
    }
    std::cout << " " << std::endl;
}

TEST_F(JsonFunctionTest, json_path_test) {
    bool wrap_explicitly;
    {
        std::string json_raw_data("[{\"a\":\"a1\", \"b\":\"b1\"}, {\"a\":\"a2\", \"b\":\"b2\"}]");
        rapidjson::Document jsonDoc;
        if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
            ASSERT_TRUE(false);
        }

        rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
                "$.[*].a", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        ASSERT_TRUE(res3->IsArray());
        ASSERT_EQ(res3->Size(), 2);
        for (int i = 0; i < res3->Size(); i++) {
            if ((*res3)[i].GetType() == rapidjson::Type::kNullType) {
                std::cout << "null ";
            } else {
                std::cout << (*res3)[i].GetString() << " ";
            }
        }
        std::cout << " " << std::endl;
    }
    {
        std::string json_raw_data("{\"a\":[\"a1\",\"a2\"], \"b\":[\"b1\",\"b2\"], \"c\":[\"c1\"], \"d\":[], \"e\":\"e1\"}");
        rapidjson::Document jsonDoc;
        if (jsonDoc.Parse(json_raw_data.c_str()).HasParseError()) {
            ASSERT_TRUE(false);
        }

        rapidjson::Value* res3 = JsonFunctions::get_json_array_from_parsed_json(
                "$.a", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        ASSERT_TRUE(res3->IsArray());
        ASSERT_EQ(res3->Size(), 2);
        for (int i = 0; i < res3->Size(); i++) {
            if ((*res3)[i].GetType() == rapidjson::Type::kNullType) {
                std::cout << "null ";
            } else {
                std::cout << (*res3)[i].GetString() << " ";
            }
        }
        std::cout << " " << std::endl;

        rapidjson::Value* res4 = JsonFunctions::get_json_array_from_parsed_json(
                "$.c", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        ASSERT_TRUE(res4->IsArray());
        ASSERT_EQ(res4->Size(), 1);
        ASSERT_FALSE(wrap_explicitly);

        rapidjson::Value* res5 = JsonFunctions::get_json_array_from_parsed_json(
                "$.d", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        ASSERT_TRUE(res5->IsArray());
        ASSERT_EQ(res5->Size(), 0);
        ASSERT_FALSE(wrap_explicitly);

        rapidjson::Value* res6 = JsonFunctions::get_json_array_from_parsed_json(
                "$.e", &jsonDoc, jsonDoc.GetAllocator(), &wrap_explicitly);
        ASSERT_TRUE(res6->IsArray());
        ASSERT_EQ(res6->Size(), 1);
        ASSERT_TRUE(wrap_explicitly);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    std::string home(getenv("DORIS_HOME"));
    if (home.empty()) {
        home = ".";
    }
    std::string conffile = home + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
