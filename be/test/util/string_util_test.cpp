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

#include "util/string_util.h"

#include <gtest/gtest.h>

#include "util/cpu_info.h"

namespace doris {

class StringUtilTest : public testing::Test {
public:
    StringUtilTest() { }
    virtual ~StringUtilTest() {
    }
};

TEST_F(StringUtilTest, normal) {
    {
        StringCaseSet test_set;
        test_set.emplace("AbC");
        test_set.emplace("AbCD");
        test_set.emplace("AbCE");
        ASSERT_EQ(1, test_set.count("abc"));
        ASSERT_EQ(1, test_set.count("abcd"));
        ASSERT_EQ(1, test_set.count("abce"));
        ASSERT_EQ(0, test_set.count("ab"));
    }
    {
        StringCaseUnorderedSet test_set;
        test_set.emplace("AbC");
        test_set.emplace("AbCD");
        test_set.emplace("AbCE");
        ASSERT_EQ(1, test_set.count("abc"));
        ASSERT_EQ(0, test_set.count("ab"));
    }
    {
        StringCaseMap<int> test_map;
        test_map.emplace("AbC", 123);
        test_map.emplace("AbCD", 234);
        test_map.emplace("AbCE", 345);
        ASSERT_EQ(123, test_map["abc"]);
        ASSERT_EQ(234, test_map["aBcD"]);
        ASSERT_EQ(345, test_map["abcE"]);
        ASSERT_EQ(0, test_map.count("ab"));
    }
    {
        StringCaseUnorderedMap<int> test_map;
        test_map.emplace("AbC", 123);
        test_map.emplace("AbCD", 234);
        test_map.emplace("AbCE", 345);
        ASSERT_EQ(123, test_map["abc"]);
        ASSERT_EQ(234, test_map["aBcD"]);
        ASSERT_EQ(345, test_map["abcE"]);
        ASSERT_EQ(0, test_map.count("ab"));
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
