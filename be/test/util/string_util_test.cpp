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
        EXPECT_EQ("abc", to_lower("ABC"));
        EXPECT_EQ("abc", to_lower("abc"));
        EXPECT_EQ("123", to_lower("123"));
        EXPECT_EQ("abc,", to_lower("Abc,"));
    }
    {
        EXPECT_EQ("ABC", to_upper("ABC"));
        EXPECT_EQ("ABC", to_upper("abc"));
        EXPECT_EQ("123", to_upper("123"));
        EXPECT_EQ("ABC,", to_upper("Abc,"));
    }
    {
        EXPECT_EQ("ABC", to_upper("ABC"));
        EXPECT_EQ("ABC", to_upper("abc"));
        EXPECT_EQ("123", to_upper("123"));
        EXPECT_EQ("ABC,", to_upper("Abc,"));
    }
    {
        EXPECT_TRUE(iequal("ABC", "ABC"));
        EXPECT_TRUE(iequal("ABC", "abc"));
        EXPECT_TRUE(iequal("ABC", "ABc"));
        EXPECT_TRUE(iequal("123", "123"));
        EXPECT_TRUE(iequal("123A,", "123a,"));
        EXPECT_FALSE(iequal("12A,", "123a,"));
        EXPECT_FALSE(iequal("abc", "ABD"));
    }
    {
        EXPECT_TRUE(starts_with("abcd", "ab"));
        EXPECT_TRUE(starts_with("abcd", "abc"));
        EXPECT_TRUE(starts_with("abcd", "abcd"));
        EXPECT_TRUE(starts_with("1234", "123"));
        EXPECT_TRUE(starts_with("a", "a"));
        EXPECT_TRUE(starts_with("", ""));
        EXPECT_TRUE(starts_with("a", ""));
        EXPECT_FALSE(starts_with("", " "));
        EXPECT_FALSE(starts_with("1234", "123a"));
    }
    {
        EXPECT_TRUE(ends_with("abcd", "cd"));
        EXPECT_TRUE(ends_with("abcd", "bcd"));
        EXPECT_TRUE(ends_with("abcd", "abcd"));
        EXPECT_TRUE(ends_with("1234", "234"));
        EXPECT_TRUE(ends_with("a", "a"));
        EXPECT_TRUE(ends_with("", ""));
        EXPECT_TRUE(ends_with("a", ""));
        EXPECT_FALSE(ends_with("", " "));
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

        EXPECT_EQ(join(tokens1, "-"), "xx-abc-xx");
        EXPECT_EQ(join(tokens2, "-"), "-xx-abc--abc-xx-");
        EXPECT_EQ(join(empty_tokens, "-"), "");
    }
    {
        std::string str1("xx-abc--xx-abb");
        std::string str2("Xx-abc--xX-abb-xx");
        std::string str3("xx");
        std::string strempty("");
        const char* pch1 = "xx-abc--xx-abb";
        std::vector<std::string> tokens;
        tokens = split(str2, "Xx");
        EXPECT_EQ(tokens.size(), 2);
        EXPECT_EQ(tokens[0], "");
        EXPECT_EQ(tokens[1], "-abc--xX-abb-xx");
        tokens = split(pch1, "x");
        EXPECT_EQ(tokens.size(), 5);
        EXPECT_EQ(tokens[0], "");
        EXPECT_EQ(tokens[1], "");
        EXPECT_EQ(tokens[2], "-abc--");
        EXPECT_EQ(tokens[3], "");
        EXPECT_EQ(tokens[4], "-abb");

        tokens = split(str1, "-");
        EXPECT_EQ(tokens.size(), 5);
        EXPECT_EQ(tokens[0], "xx");
        EXPECT_EQ(tokens[1], "abc");
        EXPECT_EQ(tokens[2], "");
        EXPECT_EQ(tokens[3], "xx");
        EXPECT_EQ(tokens[4], "abb");

        tokens = split(str3, ",");

        EXPECT_EQ(tokens.size(), 1);
        EXPECT_EQ(tokens[0], "xx");

        tokens = split(strempty, "-");

        EXPECT_EQ(tokens.size(), 1);
        EXPECT_EQ(tokens[0], "");
    }
    {
        StringCaseSet test_set;
        test_set.emplace("AbC");
        test_set.emplace("AbCD");
        test_set.emplace("AbCE");
        EXPECT_EQ(1, test_set.count("abc"));
        EXPECT_EQ(1, test_set.count("abcd"));
        EXPECT_EQ(1, test_set.count("abce"));
        EXPECT_EQ(0, test_set.count("ab"));
    }
    {
        StringCaseUnorderedSet test_set;
        test_set.emplace("AbC");
        test_set.emplace("AbCD");
        test_set.emplace("AbCE");
        EXPECT_EQ(1, test_set.count("abc"));
        EXPECT_EQ(0, test_set.count("ab"));
    }
    {
        StringCaseMap<int> test_map;
        test_map.emplace("AbC", 123);
        test_map.emplace("AbCD", 234);
        test_map.emplace("AbCE", 345);
        EXPECT_EQ(123, test_map["abc"]);
        EXPECT_EQ(234, test_map["aBcD"]);
        EXPECT_EQ(345, test_map["abcE"]);
        EXPECT_EQ(0, test_map.count("ab"));
    }
    {
        StringCaseUnorderedMap<int> test_map;
        test_map.emplace("AbC", 123);
        test_map.emplace("AbCD", 234);
        test_map.emplace("AbCE", 345);
        EXPECT_EQ(123, test_map["abc"]);
        EXPECT_EQ(234, test_map["aBcD"]);
        EXPECT_EQ(345, test_map["abcE"]);
        EXPECT_EQ(0, test_map.count("ab"));
    }
    {
        std::string ip1 = "192.168.1.1";
        std::string ip2 = "192.168.1.2";
        int64_t hash1 = hash_of_path(ip1, "/home/disk1/palo2.HDD");
        int64_t hash2 = hash_of_path(ip1, "/home/disk1//palo2.HDD/");
        int64_t hash3 = hash_of_path(ip1, "home/disk1/palo2.HDD/");
        EXPECT_EQ(hash1, hash2);
        EXPECT_EQ(hash3, hash2);

        int64_t hash4 = hash_of_path(ip1, "/home/disk1/palo2.HDD/");
        int64_t hash5 = hash_of_path(ip2, "/home/disk1/palo2.HDD/");
        EXPECT_NE(hash4, hash5);

        int64_t hash6 = hash_of_path(ip1, "/");
        int64_t hash7 = hash_of_path(ip1, "//");
        EXPECT_EQ(hash6, hash7);
    }
}

} // namespace doris
