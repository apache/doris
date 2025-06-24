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

#include "olap/rowset/segment_v2/inverted_index/query/regexp_query.h"

#include <gtest/gtest.h>

#include <fstream>
#include <memory>

#include "io/fs/local_file_system.h"

namespace doris::segment_v2 {

class RegexpQueryTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/regexp_query_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }
};

TEST_F(RegexpQueryTest, EmptyPatternReturnsNullopt) {
    auto result = RegexpQuery::get_regex_prefix("");
    EXPECT_FALSE(result.has_value());
}

TEST_F(RegexpQueryTest, NonCaretPrefixReturnsNullopt) {
    auto result = RegexpQuery::get_regex_prefix("abc");
    EXPECT_FALSE(result.has_value());
}

TEST_F(RegexpQueryTest, InvalidRegexReturnsNullopt) {
    auto result = RegexpQuery::get_regex_prefix("^[a-z");
    EXPECT_FALSE(result.has_value());
}

TEST_F(RegexpQueryTest, ValidRegexWithCaretPrefixReturnsBounds) {
    auto result = RegexpQuery::get_regex_prefix("^abc");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "abc");
}

TEST_F(RegexpQueryTest, MultipleRegexPatternsWithCaretPrefix) {
    {
        auto result = RegexpQuery::get_regex_prefix("^hello");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "hello");
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[0-9]");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[a-z]+");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[A-Za-z]");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^\\d{3}");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[[:alpha:]]");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[^0-9]");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^a*b");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^a+b");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "a");
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^a?b");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^user_[0-9]{4}");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "user_");
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[A-Z][a-z]+");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[0-9]{3}-[A-Z]{2}");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^.");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^\\w+");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^\\s+");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^(19|20)\\d{2}");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result =
                RegexpQuery::get_regex_prefix("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^https?://[^/]+");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "http");
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[+-]?\\d+");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^[\\u4e00-\\u9fff]");
        ASSERT_FALSE(result.has_value());
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^chjxg.*$");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "chjxg");
    }

    {
        auto result = RegexpQuery::get_regex_prefix("^ozmv13tl7vfpwdq.*$");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "ozmv13tl7vfpwdq");
    }
}

} // namespace doris::segment_v2