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

#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdio>
#include <fstream>
#include <utility>

#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

TokenStreamPtr create_filter(const std::string& text, int32_t flags,
                             const std::unordered_set<std::string>& prot_words = {}) {
    static lucene::util::SStringReader<char> reader;
    reader.init(text.data(), text.size(), false);

    Settings settings;
    KeywordTokenizerFactory tokenizer_factory;
    tokenizer_factory.initialize(settings);
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(&reader);

    auto token_filter = std::make_shared<WordDelimiterFilter>(
            tokenizer, WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE, flags, prot_words);
    token_filter->reset();
    return token_filter;
}

struct ExpectedToken {
    std::string term;
    int32_t pos_inc;
};

TEST(WordDelimiterFilterTest, SplitOnDelimiter) {
    std::string text = "foo-bar";
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS);

    std::vector<ExpectedToken> expected = {{"foo", 1}, {"bar", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term) << "Term mismatch at index " << i;
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc)
                << "Pos increment mismatch at index " << i;
        ++i;
    }
    EXPECT_EQ(i, expected.size()) << "Number of tokens mismatch";
}

TEST(WordDelimiterFilterTest, HandleNumbers) {
    std::string text = "123-456";
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_NUMBER_PARTS);

    std::vector<ExpectedToken> expected = {{"123", 1}, {"456", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, CatenateWords) {
    std::string text = "foo-bar";
    auto filter = create_filter(text, WordDelimiterFilter::CATENATE_WORDS);

    std::vector<ExpectedToken> expected = {{"foobar", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, CatenateNumbers) {
    std::string text = "123-456";
    auto filter = create_filter(text, WordDelimiterFilter::CATENATE_NUMBERS);

    std::vector<ExpectedToken> expected = {{"123456", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, CatenateAll) {
    std::string text = "foo-123-bar";
    auto filter = create_filter(text, WordDelimiterFilter::CATENATE_ALL);

    std::vector<ExpectedToken> expected = {{"foo123bar", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, PreserveOriginal) {
    auto cut_words = [](const auto& text, const std::vector<ExpectedToken>& expected) {
        auto filter = create_filter(text, WordDelimiterFilter::PRESERVE_ORIGINAL);

        Token t;
        size_t i = 0;
        while (filter->next(&t)) {
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            EXPECT_EQ(term, expected[i].term);
            EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
            ++i;
        }
        EXPECT_EQ(i, expected.size());
    };

    {
        std::string text = "foo-bar";
        std::vector<ExpectedToken> expected = {{"foo-bar", 1}};
        cut_words(text, expected);
    }

    {
        std::string text = "🧔🏻";
        std::vector<ExpectedToken> expected = {{"🧔🏻", 1}};
        cut_words(text, expected);
    }
}

TEST(WordDelimiterFilterTest, PreserveOriginalAndGenerateParts) {
    std::string text = "foo-bar";
    auto filter = create_filter(text, WordDelimiterFilter::PRESERVE_ORIGINAL |
                                              WordDelimiterFilter::GENERATE_WORD_PARTS);

    std::vector<ExpectedToken> expected = {{"foo-bar", 1}, {"foo", 0}, {"bar", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, SplitOnCaseChange) {
    std::string text = "fooBar";
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS |
                                              WordDelimiterFilter::SPLIT_ON_CASE_CHANGE);

    std::vector<ExpectedToken> expected = {{"foo", 1}, {"Bar", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, SplitOnNumerics) {
    std::string text = "foo123bar";
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS |
                                              WordDelimiterFilter::GENERATE_NUMBER_PARTS |
                                              WordDelimiterFilter::SPLIT_ON_NUMERICS);

    std::vector<ExpectedToken> expected = {{"foo", 1}, {"123", 1}, {"bar", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, StemEnglishPossessive) {
    std::string text = "John's";
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS |
                                              WordDelimiterFilter::STEM_ENGLISH_POSSESSIVE);

    std::vector<ExpectedToken> expected = {{"John", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, IgnoreKeywords) {
    std::string text = "foo-bar";
    std::unordered_set<std::string> prot_words = {"foo-bar"};
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS, prot_words);

    std::vector<ExpectedToken> expected = {{"foo-bar", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, SplitNonKeywords) {
    std::string text = "baz-qux";
    std::unordered_set<std::string> prot_words = {"foo-bar"};
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS, prot_words);

    std::vector<ExpectedToken> expected = {{"baz", 1}, {"qux", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, MultipleTokens) {
    std::string text = "foo-bar baz-qux";
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS);

    std::vector<ExpectedToken> expected = {{"foo", 1}, {"bar", 1}, {"baz", 1}, {"qux", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term);
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc);
        ++i;
    }
    EXPECT_EQ(i, expected.size());
}

TEST(WordDelimiterFilterTest, EmptyString) {
    std::string text;
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS);

    Token t;
    EXPECT_EQ(filter->next(&t), nullptr);
}

TEST(WordDelimiterFilterTest, SingleDelimiter) {
    std::string text = "-";
    auto filter = create_filter(text, WordDelimiterFilter::GENERATE_WORD_PARTS);

    Token t;
    EXPECT_EQ(filter->next(&t), nullptr);
}

TEST(WordDelimiterFilterTest, SortedTokensWithPositionIncrement) {
    std::string text = "01-02-03";
    auto filter = create_filter(text, WordDelimiterFilter::CATENATE_NUMBERS |
                                              WordDelimiterFilter::GENERATE_NUMBER_PARTS |
                                              WordDelimiterFilter::PRESERVE_ORIGINAL);

    std::vector<ExpectedToken> expected = {
            {"01-02-03", 1}, {"01", 0}, {"010203", 0}, {"02", 1}, {"03", 1}};

    Token t;
    size_t i = 0;
    while (filter->next(&t)) {
        std::string term(t.termBuffer<char>(), t.termLength<char>());
        EXPECT_EQ(term, expected[i].term) << "Mismatch at position " << i;
        EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc)
                << "Wrong position increment at " << i;
        ++i;
    }
    EXPECT_EQ(i, expected.size()) << "Token count mismatch. Check splitting rules.";
}

} // namespace doris::segment_v2::inverted_index