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

#include "olap/rowset/segment_v2/inverted_index_query_type.h"

#include <gtest/gtest.h>

#include <string>

namespace doris::segment_v2 {

class InvertedIndexQueryTypeTest : public testing::Test {};

// Test reader_type_to_string for all InvertedIndexReaderType enum values
TEST_F(InvertedIndexQueryTypeTest, TestReaderTypeToString) {
    EXPECT_EQ("UNKNOWN", reader_type_to_string(InvertedIndexReaderType::UNKNOWN));
    EXPECT_EQ("FULLTEXT", reader_type_to_string(InvertedIndexReaderType::FULLTEXT));
    EXPECT_EQ("STRING_TYPE", reader_type_to_string(InvertedIndexReaderType::STRING_TYPE));
    EXPECT_EQ("BKD", reader_type_to_string(InvertedIndexReaderType::BKD));
}

// Test query_type_to_string for all InvertedIndexQueryType enum values
TEST_F(InvertedIndexQueryTypeTest, TestQueryTypeToString) {
    EXPECT_EQ("UNKNOWN", query_type_to_string(InvertedIndexQueryType::UNKNOWN_QUERY));
    EXPECT_EQ("EQ", query_type_to_string(InvertedIndexQueryType::EQUAL_QUERY));
    EXPECT_EQ("LT", query_type_to_string(InvertedIndexQueryType::LESS_THAN_QUERY));
    EXPECT_EQ("LE", query_type_to_string(InvertedIndexQueryType::LESS_EQUAL_QUERY));
    EXPECT_EQ("GT", query_type_to_string(InvertedIndexQueryType::GREATER_THAN_QUERY));
    EXPECT_EQ("GE", query_type_to_string(InvertedIndexQueryType::GREATER_EQUAL_QUERY));
    EXPECT_EQ("MANY", query_type_to_string(InvertedIndexQueryType::MATCH_ANY_QUERY));
    EXPECT_EQ("MALL", query_type_to_string(InvertedIndexQueryType::MATCH_ALL_QUERY));
    EXPECT_EQ("MPHRASE", query_type_to_string(InvertedIndexQueryType::MATCH_PHRASE_QUERY));
    EXPECT_EQ("MPHRASEPREFIX",
              query_type_to_string(InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY));
    EXPECT_EQ("MREGEXP", query_type_to_string(InvertedIndexQueryType::MATCH_REGEXP_QUERY));
    EXPECT_EQ("MPHRASEEDGE", query_type_to_string(InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY));
    EXPECT_EQ("BOOLEAN", query_type_to_string(InvertedIndexQueryType::BOOLEAN_QUERY));
    EXPECT_EQ("WILDCARD", query_type_to_string(InvertedIndexQueryType::WILDCARD_QUERY));
    EXPECT_EQ("RANGE", query_type_to_string(InvertedIndexQueryType::RANGE_QUERY));
    EXPECT_EQ("LIST", query_type_to_string(InvertedIndexQueryType::LIST_QUERY));
}

// Test query_type_to_string default case (should not happen in practice)
TEST_F(InvertedIndexQueryTypeTest, TestQueryTypeToStringDefault) {
    // Cast an invalid enum value to test default case
    auto invalid_type = static_cast<InvertedIndexQueryType>(999);
    EXPECT_EQ("", query_type_to_string(invalid_type));
}

// Test is_equal_query function
TEST_F(InvertedIndexQueryTypeTest, TestIsEqualQuery) {
    EXPECT_TRUE(is_equal_query(InvertedIndexQueryType::EQUAL_QUERY));
    EXPECT_FALSE(is_equal_query(InvertedIndexQueryType::LESS_THAN_QUERY));
    EXPECT_FALSE(is_equal_query(InvertedIndexQueryType::GREATER_THAN_QUERY));
    EXPECT_FALSE(is_equal_query(InvertedIndexQueryType::MATCH_ANY_QUERY));
    EXPECT_FALSE(is_equal_query(InvertedIndexQueryType::BOOLEAN_QUERY));
    EXPECT_FALSE(is_equal_query(InvertedIndexQueryType::UNKNOWN_QUERY));
}

// Test is_range_query function for all range query types
TEST_F(InvertedIndexQueryTypeTest, TestIsRangeQuery) {
    // Test all range query types
    EXPECT_TRUE(is_range_query(InvertedIndexQueryType::GREATER_THAN_QUERY));
    EXPECT_TRUE(is_range_query(InvertedIndexQueryType::GREATER_EQUAL_QUERY));
    EXPECT_TRUE(is_range_query(InvertedIndexQueryType::LESS_THAN_QUERY));
    EXPECT_TRUE(is_range_query(InvertedIndexQueryType::LESS_EQUAL_QUERY));

    // Test non-range query types
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::EQUAL_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::MATCH_ANY_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::MATCH_ALL_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::MATCH_PHRASE_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::MATCH_REGEXP_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::BOOLEAN_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::WILDCARD_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::RANGE_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::LIST_QUERY));
    EXPECT_FALSE(is_range_query(InvertedIndexQueryType::UNKNOWN_QUERY));
}

// Test is_match_query function for all match query types
TEST_F(InvertedIndexQueryTypeTest, TestIsMatchQuery) {
    // Test all match query types
    EXPECT_TRUE(is_match_query(InvertedIndexQueryType::MATCH_ANY_QUERY));
    EXPECT_TRUE(is_match_query(InvertedIndexQueryType::MATCH_ALL_QUERY));
    EXPECT_TRUE(is_match_query(InvertedIndexQueryType::MATCH_PHRASE_QUERY));
    EXPECT_TRUE(is_match_query(InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY));
    EXPECT_TRUE(is_match_query(InvertedIndexQueryType::MATCH_REGEXP_QUERY));
    EXPECT_TRUE(is_match_query(InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY));

    // Test non-match query types
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::EQUAL_QUERY));
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::LESS_THAN_QUERY));
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::GREATER_THAN_QUERY));
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::BOOLEAN_QUERY));
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::WILDCARD_QUERY));
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::RANGE_QUERY));
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::LIST_QUERY));
    EXPECT_FALSE(is_match_query(InvertedIndexQueryType::UNKNOWN_QUERY));
}

// Test template specialization for InvertedIndexReaderTypeToString
TEST_F(InvertedIndexQueryTypeTest, TestInvertedIndexReaderTypeToStringTemplate) {
    EXPECT_STREQ("UNKNOWN", InvertedIndexReaderTypeToString<InvertedIndexReaderType::UNKNOWN>());
    EXPECT_STREQ("FULLTEXT", InvertedIndexReaderTypeToString<InvertedIndexReaderType::FULLTEXT>());
    EXPECT_STREQ("STRING_TYPE",
                 InvertedIndexReaderTypeToString<InvertedIndexReaderType::STRING_TYPE>());
    EXPECT_STREQ("BKD", InvertedIndexReaderTypeToString<InvertedIndexReaderType::BKD>());
}

} // namespace doris::segment_v2
