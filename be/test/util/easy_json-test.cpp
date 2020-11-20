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

#include "util/easy_json.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <string>

#include "gutil/integral_types.h"

using rapidjson::SizeType;
using rapidjson::Value;
using std::string;

namespace doris {

class EasyJsonTest : public ::testing::Test {};

TEST_F(EasyJsonTest, TestNull) {
    EasyJson ej;
    ASSERT_TRUE(ej.value().IsNull());
}

TEST_F(EasyJsonTest, TestBasic) {
    EasyJson ej;
    ej.SetObject();
    ej.Set("1", true);
    ej.Set("2", kint32min);
    ej.Set("4", kint64min);
    ej.Set("6", 1.0);
    ej.Set("7", "string");

    Value& v = ej.value();

    ASSERT_EQ(v["1"].GetBool(), true);
    ASSERT_EQ(v["2"].GetInt(), kint32min);
    ASSERT_EQ(v["4"].GetInt64(), kint64min);
    ASSERT_EQ(v["6"].GetDouble(), 1.0);
    ASSERT_EQ(string(v["7"].GetString()), "string");
}

TEST_F(EasyJsonTest, TestNested) {
    EasyJson ej;
    ej.SetObject();
    ej.Get("nested").SetObject();
    ej.Get("nested").Set("nested_attr", true);
    ASSERT_EQ(ej.value()["nested"]["nested_attr"].GetBool(), true);

    ej.Get("nested_array").SetArray();
    ej.Get("nested_array").PushBack(1);
    ej.Get("nested_array").PushBack(2);
    ASSERT_EQ(ej.value()["nested_array"][SizeType(0)].GetInt(), 1);
    ASSERT_EQ(ej.value()["nested_array"][SizeType(1)].GetInt(), 2);
}

TEST_F(EasyJsonTest, TestCompactSyntax) {
    EasyJson ej;
    ej["nested"]["nested_attr"] = true;
    ASSERT_EQ(ej.value()["nested"]["nested_attr"].GetBool(), true);

    for (int i = 0; i < 2; i++) {
        ej["nested_array"][i] = i + 1;
    }
    ASSERT_EQ(ej.value()["nested_array"][SizeType(0)].GetInt(), 1);
    ASSERT_EQ(ej.value()["nested_array"][SizeType(1)].GetInt(), 2);
}

TEST_F(EasyJsonTest, TestComplexInitializer) {
    EasyJson ej;
    ej = EasyJson::kObject;
    ASSERT_TRUE(ej.value().IsObject());

    EasyJson nested_arr = ej.Set("nested_arr", EasyJson::kArray);
    ASSERT_TRUE(nested_arr.value().IsArray());

    EasyJson nested_obj = nested_arr.PushBack(EasyJson::kObject);
    ASSERT_TRUE(ej["nested_arr"][0].value().IsObject());
}

TEST_F(EasyJsonTest, TestAllocatorLifetime) {
    EasyJson* root = new EasyJson;
    EasyJson child = (*root)["child"];
    delete root;

    child["child_attr"] = 1;
    ASSERT_EQ(child.value()["child_attr"].GetInt(), 1);
}
} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
