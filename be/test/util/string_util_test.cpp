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
    StringUtilTest() {}
    virtual ~StringUtilTest() {}
};

TEST_F(StringUtilTest, normal) {
    {
        ASSERT_EQ("abc", to_lower("ABC"));
        ASSERT_EQ("abc", to_lower("abc"));
        ASSERT_EQ("123", to_lower("123"));
        ASSERT_EQ("abc,", to_lower("Abc,"));
    }
    {
        ASSERT_EQ("ABC", to_upper("ABC"));
        ASSERT_EQ("ABC", to_upper("abc"));
        ASSERT_EQ("123", to_upper("123"));
        ASSERT_EQ("ABC,", to_upper("Abc,"));
    }
    {
        ASSERT_EQ("ABC", to_upper("ABC"));
        ASSERT_EQ("ABC", to_upper("abc"));
        ASSERT_EQ("123", to_upper("123"));
        ASSERT_EQ("ABC,", to_upper("Abc,"));
    }
    {
        ASSERT_TRUE(iequal("ABC", "ABC"));
        ASSERT_TRUE(iequal("ABC", "abc"));
        ASSERT_TRUE(iequal("ABC", "ABc"));
        ASSERT_TRUE(iequal("123", "123"));
        ASSERT_TRUE(iequal("123A,", "123a,"));
        ASSERT_FALSE(iequal("12A,", "123a,"));
        ASSERT_FALSE(iequal("abc", "ABD"));
    }
    {
        ASSERT_TRUE(starts_with("abcd", "ab"));
        ASSERT_TRUE(starts_with("abcd", "abc"));
        ASSERT_TRUE(starts_with("abcd", "abcd"));
        ASSERT_TRUE(starts_with("1234", "123"));
        ASSERT_TRUE(starts_with("a", "a"));
        ASSERT_TRUE(starts_with("", ""));
        ASSERT_TRUE(starts_with("a", ""));
        ASSERT_FALSE(starts_with("", " "));
        ASSERT_FALSE(starts_with("1234", "123a"));
    }
    {
        ASSERT_TRUE(ends_with("abcd", "cd"));
        ASSERT_TRUE(ends_with("abcd", "bcd"));
        ASSERT_TRUE(ends_with("abcd", "abcd"));
        ASSERT_TRUE(ends_with("1234", "234"));
        ASSERT_TRUE(ends_with("a", "a"));
        ASSERT_TRUE(ends_with("", ""));
        ASSERT_TRUE(ends_with("a", ""));
        ASSERT_FALSE(ends_with("", " "));
    }
    {
        std::vector<std::string> tokens1;
        tokens1.push_back("xx");
        tokens1.push_back("abc");
        tokens1.push_back("xx");

        std::vector<std::string> tokens2;
        tokens2.push_back("");
        tokens2.push_back("xx");
        tokens2.push_back("abc");
        tokens2.push_back("");
        tokens2.push_back("abc");
        tokens2.push_back("xx");
        tokens2.push_back("");

        std::vector<std::string> tokens3;
        tokens3.push_back("");
        tokens3.push_back("");
        tokens3.push_back("");

        std::vector<std::string> empty_tokens;

        ASSERT_EQ(join(tokens1, "-"), "xx-abc-xx");
        ASSERT_EQ(join(tokens2, "-"), "-xx-abc--abc-xx-");
        ASSERT_EQ(join(empty_tokens, "-"), "");
    }
    {
        std::string str1("xx-abc--xx-abb");
        std::string str2("Xx-abc--xX-abb-xx");
        std::string str3("xx");
        std::string strempty("");
        const char* pch1 = "xx-abc--xx-abb";
        std::vector<std::string> tokens;
        tokens = split(str2, "Xx");
        ASSERT_EQ(tokens.size(), 2);
        ASSERT_EQ(tokens[0], "");
        ASSERT_EQ(tokens[1], "-abc--xX-abb-xx");
        tokens = split(pch1, "x");
        ASSERT_EQ(tokens.size(), 5);
        ASSERT_EQ(tokens[0], "");
        ASSERT_EQ(tokens[1], "");
        ASSERT_EQ(tokens[2], "-abc--");
        ASSERT_EQ(tokens[3], "");
        ASSERT_EQ(tokens[4], "-abb");

        tokens = split(str1, "-");
        ASSERT_EQ(tokens.size(), 5);
        ASSERT_EQ(tokens[0], "xx");
        ASSERT_EQ(tokens[1], "abc");
        ASSERT_EQ(tokens[2], "");
        ASSERT_EQ(tokens[3], "xx");
        ASSERT_EQ(tokens[4], "abb");

        tokens = split(str3, ",");

        ASSERT_EQ(tokens.size(), 1);
        ASSERT_EQ(tokens[0], "xx");

        tokens = split(strempty, "-");

        ASSERT_EQ(tokens.size(), 1);
        ASSERT_EQ(tokens[0], "");
    }
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
    {
        std::string ip1 = "192.168.1.1";
        std::string ip2 = "192.168.1.2";
        int64_t hash1 = hash_of_path(ip1, "/home/disk1/palo2.HDD");
        int64_t hash2 = hash_of_path(ip1, "/home/disk1//palo2.HDD/");
        int64_t hash3 = hash_of_path(ip1, "home/disk1/palo2.HDD/");
        ASSERT_EQ(hash1, hash2);
        ASSERT_EQ(hash3, hash2);

        int64_t hash4 = hash_of_path(ip1, "/home/disk1/palo2.HDD/");
        int64_t hash5 = hash_of_path(ip2, "/home/disk1/palo2.HDD/");
        ASSERT_NE(hash4, hash5);

        int64_t hash6 = hash_of_path(ip1, "/");
        int64_t hash7 = hash_of_path(ip1, "//");
        ASSERT_EQ(hash6, hash7);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
