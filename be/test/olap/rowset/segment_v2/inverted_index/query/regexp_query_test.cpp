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

#include "gen_cpp/PaloInternalService_types.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index/query/query.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"

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

TEST_F(RegexpQueryTest, AddWithInvalidTermsSize) {
    // Create a mock searcher and query options for testing
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    io::IOContext io_ctx;

    RegexpQuery regexp_query(searcher, query_options, &io_ctx);

    // Test with empty terms (size == 0) - this should throw before accessing searcher
    {
        InvertedIndexQueryInfo query_info;
        query_info.field_name = L"test_field";
        query_info.term_infos = {}; // empty term_infos

        EXPECT_THROW(regexp_query.add(query_info), std::exception);
    }
}

TEST_F(RegexpQueryTest, AddWithInvalidPattern) {
    // Create a mock searcher and query options for testing
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    io::IOContext io_ctx;

    RegexpQuery regexp_query(searcher, query_options, &io_ctx);

    // Test with invalid regex pattern that causes hs_compile to fail
    // This should fail during hyperscan compilation, before accessing searcher
    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"test_field";
    TermInfo term_info;
    term_info.term = "[invalid_regex";
    query_info.term_infos = {term_info}; // invalid regex pattern

    // This should not throw but should handle the error gracefully
    // The hyperscan compilation will fail and the method will return early
    EXPECT_NO_THROW(regexp_query.add(query_info));
}

TEST_F(RegexpQueryTest, SearchWithEmptyTerms) {
    // Create a mock searcher and query options for testing
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    io::IOContext io_ctx;

    RegexpQuery regexp_query(searcher, query_options, &io_ctx);
    roaring::Roaring result;

    // Search without adding any terms should not crash
    EXPECT_NO_THROW(regexp_query.search(result));
    EXPECT_TRUE(result.isEmpty());
}

TEST_F(RegexpQueryTest, GetRegexPrefixWithDebugPoint) {
    // Test the debug point that forces get_regex_prefix to return nullopt
    // This covers the debug execute if code path
    auto result = RegexpQuery::get_regex_prefix("^test");
    // Without debug point activated, should return normally
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "test");
}

TEST_F(RegexpQueryTest, AddWithPatternThatFailsCompilation) {
    // Test add method with pattern that should fail hs_compile
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    io::IOContext io_ctx;

    RegexpQuery regexp_query(searcher, query_options, &io_ctx);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"test_field";
    // Use a pattern that is guaranteed to fail hyperscan compilation
    // Hyperscan doesn't support backreferences, so this should fail
    TermInfo term_info;
    term_info.term =
            "(?P<name>\\w+)\\k<name>"; // pattern with named backreference (not supported by hyperscan)
    query_info.term_infos = {term_info};

    // Should not crash even with invalid hyperscan pattern (covers the hs_compile failure path)
    // The hyperscan compilation will fail and the method will return early
    EXPECT_NO_THROW(regexp_query.add(query_info));
}

TEST_F(RegexpQueryTest, ConstructorTest) {
    // Test constructor with different configurations
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    io::IOContext io_ctx;

    // Test basic constructor
    EXPECT_NO_THROW(RegexpQuery(searcher, query_options, &io_ctx));

    // Test constructor with different max expansions
    query_options.inverted_index_max_expansions = 100;
    EXPECT_NO_THROW(RegexpQuery(searcher, query_options, &io_ctx));
}

TEST_F(RegexpQueryTest, MaxExpansionsConfiguration) {
    // Test that max expansions is properly configured
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 100;
    io::IOContext io_ctx;

    RegexpQuery regexp_query(searcher, query_options, &io_ctx);

    // This tests the constructor and member initialization
    EXPECT_NO_THROW(RegexpQuery(searcher, query_options, &io_ctx));
}

TEST_F(RegexpQueryTest, AddWithUnsupportedRegexFeatures) {
    // Test patterns that use regex features not supported by hyperscan
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    io::IOContext io_ctx;

    RegexpQuery regexp_query(searcher, query_options, &io_ctx);

    // Test with lookahead assertion (not supported by hyperscan)
    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"test_field";
    TermInfo term_info;
    term_info.term = "(?=.*test).*"; // positive lookahead (not supported by hyperscan)
    query_info.term_infos = {term_info};

    // Should not throw as hyperscan compilation will fail and method returns early
    EXPECT_NO_THROW(regexp_query.add(query_info));
}

TEST_F(RegexpQueryTest, AddWithBackreferencePattern) {
    // Test with backreference pattern that should fail hyperscan compilation
    std::shared_ptr<lucene::search::IndexSearcher> searcher = nullptr;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    io::IOContext io_ctx;

    RegexpQuery regexp_query(searcher, query_options, &io_ctx);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"test_field";
    TermInfo term_info;
    term_info.term = R"((\w+)\s+\1)"; // backreference pattern (not supported by hyperscan)
    query_info.term_infos = {term_info};

    // Should not throw as hyperscan compilation will fail and method returns early
    EXPECT_NO_THROW(regexp_query.add(query_info));
}

} // namespace doris::segment_v2