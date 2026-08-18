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

#include "storage/index/inverted/common_grams/common_word_set.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "common/config.h"

namespace doris::segment_v2::inverted_index {

namespace common_grams_testing {
uint64_t common_word_hash_lookup_count();
void reset_common_word_hash_lookup_count();
} // namespace common_grams_testing

namespace {

TEST(CommonWordSetTest, DefaultWordSetLivesUnderTheSharedDictionaryRoot) {
    // The word list follows the same layout as the icu, ik and pinyin dictionaries, so relocating
    // inverted_index_dict_path moves all of them together instead of leaving this one behind.
    const std::string saved = config::inverted_index_dict_path;
    config::inverted_index_dict_path = "/somewhere/else";
    EXPECT_EQ(CommonWordSet::default_word_set_path(),
              "/somewhere/else/common_grams/default_words.txt");
    config::inverted_index_dict_path = saved;
}

TEST(CommonWordSetTest, BuiltinEnglishStopV1IsTheExactFrozen33WordSet) {
    EXPECT_EQ(BUILTIN_COMMON_WORDS_RESOURCE, "builtin:lucene_english_stop:v1");
    const auto& words = CommonWordSet::builtin_english_stop_words_v1();
    constexpr std::array<std::string_view, 33> expected = {
            "a",   "an",    "and",  "are",   "as",    "at",   "be",   "but", "by",  "for",  "if",
            "in",  "into",  "is",   "it",    "no",    "not",  "of",   "on",  "or",  "such", "that",
            "the", "their", "then", "there", "these", "they", "this", "to",  "was", "will", "with"};

    EXPECT_EQ(words.size(), expected.size());
    for (std::string_view word : expected) {
        EXPECT_TRUE(words.contains(word)) << word;
    }
    EXPECT_FALSE(words.contains("english"));
    EXPECT_FALSE(words.contains("The"));
}

TEST(CommonWordSetTest, RejectsImpossibleShapesBeforeHashLookup) {
    const auto& words = CommonWordSet::builtin_english_stop_words_v1();
    common_grams_testing::reset_common_word_hash_lookup_count();

    EXPECT_FALSE(words.contains("encyclopedia")); // longer than every builtin word
    EXPECT_FALSE(words.contains("zoo"));          // impossible first byte
    EXPECT_FALSE(words.contains("tea"));          // possible shape, absent from the set
    EXPECT_TRUE(words.contains("the"));

    EXPECT_EQ(common_grams_testing::common_word_hash_lookup_count(), 2);
}

TEST(CommonWordSetTest, WordsetV1IgnoresEmptyAndCommentLinesAndDeduplicates) {
    EXPECT_EQ(WORDSET_FORMAT_V1, "wordset:v1");
    const std::string content =
            "# comment\n"
            "alpha\n"
            "\n"
            "beta\n"
            "alpha\n"
            "# trailing comment\n"
            "\xe4\xbd\xa0\xe5\xa5\xbd\n";
    auto parsed = CommonWordSet::parse_words(content);
    ASSERT_TRUE(parsed.has_value()) << parsed.error();
    EXPECT_EQ(parsed->size(), 3);
    EXPECT_TRUE(parsed->contains("alpha"));
    EXPECT_TRUE(parsed->contains("beta"));
    EXPECT_TRUE(parsed->contains("\xe4\xbd\xa0\xe5\xa5\xbd"));
    EXPECT_FALSE(parsed->contains("# comment"));
    EXPECT_FALSE(parsed->contains(""));
}

TEST(CommonWordSetTest, WordsetV1PreservesTermBytesAndAcceptsCrLfAndFinalLine) {
    const std::string content =
            "alpha\r\n"
            "beta\r\n"
            "inline#hash\n"
            " leading\n"
            "trailing \n"
            " #not-comment\n"
            "final";
    auto parsed = CommonWordSet::parse_words(content);
    ASSERT_TRUE(parsed.has_value()) << parsed.error();
    EXPECT_EQ(parsed->size(), 7);
    for (std::string_view term :
         {"alpha", "beta", "inline#hash", " leading", "trailing ", " #not-comment", "final"}) {
        EXPECT_TRUE(parsed->contains(term)) << term;
    }
    EXPECT_FALSE(parsed->contains("alpha\r"));
    EXPECT_FALSE(parsed->contains("beta\r"));
}

TEST(CommonWordSetTest, UnterminatedFinalCarriageReturnIsTermData) {
    auto final_with_carriage_return = CommonWordSet::parse_words("final\r");
    ASSERT_TRUE(final_with_carriage_return.has_value()) << final_with_carriage_return.error();
    EXPECT_EQ(final_with_carriage_return->size(), 1);
    EXPECT_TRUE(final_with_carriage_return->contains("final\r"));
    EXPECT_FALSE(final_with_carriage_return->contains("final"));

    auto lone_carriage_return = CommonWordSet::parse_words("\r");
    ASSERT_TRUE(lone_carriage_return.has_value()) << lone_carriage_return.error();
    EXPECT_EQ(lone_carriage_return->size(), 1);
    EXPECT_TRUE(lone_carriage_return->contains("\r"));
    EXPECT_FALSE(lone_carriage_return->contains(""));
}

TEST(CommonWordSetTest, RejectsNulAndInvalidUtf8Terms) {
    const std::string nul_content("alpha\na\0b\n", 10);
    EXPECT_FALSE(CommonWordSet::parse_words(nul_content).has_value());

    const std::string invalid_utf8("alpha\n\xc3\x28\n", 9);
    EXPECT_FALSE(CommonWordSet::parse_words(invalid_utf8).has_value());

    const std::string nul_comment("# bad\0comment\nalpha\n", 20);
    EXPECT_FALSE(CommonWordSet::parse_words(nul_comment).has_value());
    const std::string invalid_comment("# bad\xc3\x28\nalpha\n", 14);
    EXPECT_FALSE(CommonWordSet::parse_words(invalid_comment).has_value());
}

TEST(CommonWordSetTest, WordsetV1IsCaseSensitive) {
    auto parsed = CommonWordSet::parse_words("Alpha\nalpha\n");
    ASSERT_TRUE(parsed.has_value()) << parsed.error();
    EXPECT_EQ(parsed->size(), 2);
    EXPECT_TRUE(parsed->contains("Alpha"));
    EXPECT_TRUE(parsed->contains("alpha"));
}

TEST(CommonWordSetTest, BuiltinIdentityIsTheFrozenResourceName) {
    EXPECT_EQ(CommonWordSet::builtin_english_stop_words_v1().identity(),
              BUILTIN_COMMON_WORDS_RESOURCE);
}

// A segment stamps this identity and the querying analyzer compares against it, so two BEs
// reading different word lists must not agree. Deriving it from the file bytes is what makes
// the BE-local word list safe; a fixed constant would let mismatched grams pass unnoticed.
TEST(CommonWordSetTest, ParsedIdentityIsDerivedFromContent) {
    auto first = CommonWordSet::parse_words("alpha\nbeta\n");
    auto same = CommonWordSet::parse_words("alpha\nbeta\n");
    auto different = CommonWordSet::parse_words("alpha\ngamma\n");
    ASSERT_TRUE(first.has_value()) << first.error();
    ASSERT_TRUE(same.has_value()) << same.error();
    ASSERT_TRUE(different.has_value()) << different.error();

    EXPECT_EQ(first->identity(), same->identity());
    EXPECT_NE(first->identity(), different->identity());
    EXPECT_TRUE(first->identity().starts_with("wordset:md5:"));
    EXPECT_NE(first->identity(), BUILTIN_COMMON_WORDS_RESOURCE);
}

// Comments and blank lines do not change the parsed word set, but they do change the identity.
// That direction is the safe one: a superfluous re-plan costs a cost comparison, whereas reusing
// grams across an edit that DID change the list would be wrong.
TEST(CommonWordSetTest, IdentityTracksRawBytesNotJustTheParsedWords) {
    auto plain = CommonWordSet::parse_words("alpha\nbeta\n");
    auto commented = CommonWordSet::parse_words("# a note\nalpha\nbeta\n");
    ASSERT_TRUE(plain.has_value()) << plain.error();
    ASSERT_TRUE(commented.has_value()) << commented.error();

    EXPECT_EQ(plain->size(), commented->size());
    EXPECT_NE(plain->identity(), commented->identity());
}

} // namespace
} // namespace doris::segment_v2::inverted_index
