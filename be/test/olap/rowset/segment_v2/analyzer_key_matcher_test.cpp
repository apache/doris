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

#include "olap/rowset/segment_v2/analyzer_key_matcher.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index_iterator.h"

namespace doris::segment_v2 {

// New design:
// - Empty string "" means "user did not specify" (BE auto-selects index)
// - Non-empty string means "user specified this analyzer" (exact match)
// - "__default__" is no longer special - it's just another explicit key

class AnalyzerKeyMatcherTest : public testing::Test {
protected:
    void SetUp() override {
        // Create test entries
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "chinese", nullptr});
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "english", nullptr});
        entries_.push_back({InvertedIndexReaderType::STRING_TYPE, "", nullptr}); // empty key reader

        // Build index
        for (size_t i = 0; i < entries_.size(); ++i) {
            key_index_[entries_[i].analyzer_key].push_back(i);
        }
    }

    std::vector<ReaderEntry> entries_;
    std::unordered_map<std::string, std::vector<size_t>> key_index_;
};

// Test exact match
TEST_F(AnalyzerKeyMatcherTest, ExactMatch) {
    auto result = AnalyzerKeyMatcher::match("chinese", entries_, key_index_);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result.candidates[0]->analyzer_key, "chinese");
    EXPECT_FALSE(result.used_fallback);
}

TEST_F(AnalyzerKeyMatcherTest, ExactMatchEnglish) {
    auto result = AnalyzerKeyMatcher::match("english", entries_, key_index_);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result.candidates[0]->analyzer_key, "english");
    EXPECT_FALSE(result.used_fallback);
}

// Test empty string returns all readers for query-type-based selection
TEST_F(AnalyzerKeyMatcherTest, EmptyKeyReturnsAll) {
    auto result = AnalyzerKeyMatcher::match("", entries_, key_index_);

    // Empty string means "no specific analyzer requested", so return all readers
    // to allow query-type-based selection (e.g., FULLTEXT for MATCH, STRING_TYPE for EQUAL)
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(result.used_fallback);
}

// Test explicit key with no match returns empty
TEST_F(AnalyzerKeyMatcherTest, ExplicitKeyNoMatchReturnsEmpty) {
    auto result = AnalyzerKeyMatcher::match("japanese", entries_, key_index_);

    EXPECT_TRUE(result.empty());
    EXPECT_FALSE(result.used_fallback);
}

// Test is_explicit
TEST_F(AnalyzerKeyMatcherTest, IsExplicit) {
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit("chinese"));
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit("english"));
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit("none")); // "none" is explicit
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit(
            "__default__")); // "__default__" is explicit (just another key)
    EXPECT_FALSE(AnalyzerKeyMatcher::is_explicit("")); // Only empty is not explicit
}

// Test allows_fallback
TEST_F(AnalyzerKeyMatcherTest, AllowsFallback) {
    EXPECT_TRUE(AnalyzerKeyMatcher::allows_fallback("")); // Only empty allows fallback
    EXPECT_FALSE(AnalyzerKeyMatcher::allows_fallback("chinese"));
    EXPECT_FALSE(AnalyzerKeyMatcher::allows_fallback("english"));
    EXPECT_FALSE(AnalyzerKeyMatcher::allows_fallback("none"));
    EXPECT_FALSE(AnalyzerKeyMatcher::allows_fallback("__default__")); // Not special anymore
}

// Test empty entries
TEST_F(AnalyzerKeyMatcherTest, EmptyEntries) {
    std::vector<ReaderEntry> empty_entries;
    std::unordered_map<std::string, std::vector<size_t>> empty_index;

    auto result = AnalyzerKeyMatcher::match("chinese", empty_entries, empty_index);
    EXPECT_TRUE(result.empty());
}

// Test multiple entries with same analyzer key
class AnalyzerKeyMatcherMultiEntryTest : public testing::Test {
protected:
    void SetUp() override {
        // Multiple readers with same analyzer key (different types)
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "chinese", nullptr});
        entries_.push_back({InvertedIndexReaderType::STRING_TYPE, "chinese", nullptr});

        for (size_t i = 0; i < entries_.size(); ++i) {
            key_index_[entries_[i].analyzer_key].push_back(i);
        }
    }

    std::vector<ReaderEntry> entries_;
    std::unordered_map<std::string, std::vector<size_t>> key_index_;
};

TEST_F(AnalyzerKeyMatcherMultiEntryTest, MatchReturnsAllWithSameKey) {
    auto result = AnalyzerKeyMatcher::match("chinese", entries_, key_index_);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 2);
    EXPECT_FALSE(result.used_fallback);

    // Both should be "chinese"
    for (const auto* entry : result.candidates) {
        EXPECT_EQ(entry->analyzer_key, "chinese");
    }
}

// Test empty key fallback when no exact match
class AnalyzerKeyMatcherFallbackTest : public testing::Test {
protected:
    void SetUp() override {
        // Only specific entries (no empty key)
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "chinese", nullptr});
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "english", nullptr});

        for (size_t i = 0; i < entries_.size(); ++i) {
            key_index_[entries_[i].analyzer_key].push_back(i);
        }
    }

    std::vector<ReaderEntry> entries_;
    std::unordered_map<std::string, std::vector<size_t>> key_index_;
};

TEST_F(AnalyzerKeyMatcherFallbackTest, EmptyKeyFallbacksToAll) {
    auto result = AnalyzerKeyMatcher::match("", entries_, key_index_);

    // When empty string is requested, fallback to all readers
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 2);
    EXPECT_TRUE(result.used_fallback);
}

TEST_F(AnalyzerKeyMatcherFallbackTest, ExplicitKeyNoFallback) {
    auto result = AnalyzerKeyMatcher::match("japanese", entries_, key_index_);

    // Explicit key (not empty) should not fallback
    EXPECT_TRUE(result.empty());
    EXPECT_FALSE(result.used_fallback);
}

// Test "none" analyzer is an explicit key (no tokenization index)
class AnalyzerKeyMatcherNoneAnalyzerTest : public testing::Test {
protected:
    void SetUp() override {
        // Entry with empty key (default/no properties) and entry with "none" key
        entries_.push_back({InvertedIndexReaderType::STRING_TYPE, "", nullptr});
        entries_.push_back(
                {InvertedIndexReaderType::STRING_TYPE, INVERTED_INDEX_PARSER_NONE, nullptr});

        for (size_t i = 0; i < entries_.size(); ++i) {
            key_index_[entries_[i].analyzer_key].push_back(i);
        }
    }

    std::vector<ReaderEntry> entries_;
    std::unordered_map<std::string, std::vector<size_t>> key_index_;
};

TEST_F(AnalyzerKeyMatcherNoneAnalyzerTest, NoneIsDistinctFromEmpty) {
    // "none" is an explicit analyzer key, should do exact match
    auto result_none = AnalyzerKeyMatcher::match(INVERTED_INDEX_PARSER_NONE, entries_, key_index_);
    EXPECT_FALSE(result_none.empty());
    EXPECT_EQ(result_none.size(), 1);
    EXPECT_EQ(result_none.candidates[0]->analyzer_key, INVERTED_INDEX_PARSER_NONE);
    EXPECT_FALSE(result_none.used_fallback);

    // Empty string means "no specific analyzer", should return all readers
    auto result_empty = AnalyzerKeyMatcher::match("", entries_, key_index_);
    EXPECT_FALSE(result_empty.empty());
    EXPECT_EQ(result_empty.size(), 2); // Should return both entries
    EXPECT_TRUE(result_empty.used_fallback);
}

TEST_F(AnalyzerKeyMatcherNoneAnalyzerTest, NoneIsExplicit) {
    // "none" is treated as an explicit analyzer key
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit(INVERTED_INDEX_PARSER_NONE));
    EXPECT_FALSE(AnalyzerKeyMatcher::allows_fallback(INVERTED_INDEX_PARSER_NONE));
}

} // namespace doris::segment_v2
