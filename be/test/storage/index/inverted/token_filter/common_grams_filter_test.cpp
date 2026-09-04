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

#include "storage/index/inverted/token_filter/common_grams_filter.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_word_set.h"

namespace doris::segment_v2::inverted_index {
namespace {

struct ScriptedToken {
    std::string term;
    int32_t position_increment = 1;
    int32_t start_offset = 0;
    int32_t end_offset = 0;
    const TCHAR* type = Token::getDefaultType();
};

class ScriptedTokenStream final : public TokenStream {
public:
    explicit ScriptedTokenStream(std::vector<ScriptedToken> tokens) : _tokens(std::move(tokens)) {}

    Token* next(Token* token) override {
        if (_next == _tokens.size()) {
            return nullptr;
        }
        const auto& scripted = _tokens[_next++];
        _scratch = scripted.term;
        token->clear();
        token->setTextNoCopy(_scratch.data(), static_cast<int32_t>(_scratch.size()));
        token->positionIncrement = scripted.position_increment;
        token->setStartOffset(scripted.start_offset);
        token->setEndOffset(scripted.end_offset);
        token->setType(scripted.type);
        return token;
    }

    void close() override {}
    void reset() override { _next = 0; }

    void set_tokens(std::vector<ScriptedToken> tokens) {
        _tokens = std::move(tokens);
        _next = 0;
    }

    size_t consumed() const { return _next; }

private:
    std::vector<ScriptedToken> _tokens;
    size_t _next = 0;
    std::string _scratch;
};

struct ActualToken {
    std::string term;
    int32_t position_increment;
    int32_t start_offset;
    int32_t end_offset;
    std::wstring type;

    bool operator==(const ActualToken&) const = default;
};

struct TokenSemantics {
    std::string term;
    int32_t position;
    std::wstring type;

    bool operator==(const TokenSemantics&) const = default;
};

template <typename Event>
concept HasAnalyzerCommonGramClassification = requires(Event event) {
    event.has_preceding_gram;
    event.preceding_gram_both_common;
};

static_assert(!HasAnalyzerCommonGramClassification<SniiCommonGramsIndexEvent>);

std::string gram(std::string_view left, std::string_view right) {
    auto encoded = encode_common_gram(left, right);
    EXPECT_TRUE(encoded.has_value()) << encoded.error();
    return encoded.value();
}

std::vector<ActualToken> collect(const TokenStreamPtr& stream) {
    std::vector<ActualToken> result;
    Token token;
    while (stream->next(&token) != nullptr) {
        result.push_back({std::string(token.termBuffer<char>(), token.termLength<char>()),
                          token.getPositionIncrement(), token.startOffset(), token.endOffset(),
                          token.type()});
    }
    return result;
}

std::vector<TokenSemantics> collect_semantics(const TokenStreamPtr& stream) {
    std::vector<TokenSemantics> result;
    Token token;
    int32_t position = 0;
    while (stream->next(&token) != nullptr) {
        position += token.getPositionIncrement();
        result.push_back({std::string(token.termBuffer<char>(), token.termLength<char>()), position,
                          token.type()});
    }
    return result;
}

std::vector<TokenSemantics> expand_snii_index_events(CommonGramsFilter* stream, size_t* event_count,
                                                     size_t* both_common_gram_count) {
    std::vector<TokenSemantics> result;
    std::optional<std::string> previous_logical_term;
    bool previous_is_common = false;
    int32_t position = 0;
    SniiCommonGramsIndexEvent event;
    while (stream->next_snii_index_event(&event)) {
        ++*event_count;
        const std::string physical_plain_term(event.plain_term);
        const std::string logical_term =
                decode_plain_term(physical_plain_term, PlainTermKeyVersion::kEscapedV1).value();
        EXPECT_EQ(event.logical_term, logical_term);
        const bool current_is_common =
                CommonWordSet::builtin_english_stop_words_v1().contains(logical_term);
        const bool has_preceding_gram = previous_logical_term.has_value() &&
                                        (previous_is_common || current_is_common) &&
                                        common_gram_component_sizes_encodable(
                                                previous_logical_term->size(), logical_term.size());
        if (has_preceding_gram) {
            EXPECT_TRUE(previous_logical_term.has_value());
            *both_common_gram_count += previous_is_common && current_is_common;
            result.push_back({gram(previous_logical_term.value(), logical_term), position,
                              COMMON_GRAM_TOKEN_TYPE});
        }
        ++position;
        result.push_back({physical_plain_term, position, Token::getDefaultType()});
        previous_logical_term = logical_term;
        previous_is_common = current_is_common;
    }
    return result;
}

std::vector<ScriptedToken> words(std::initializer_list<std::string_view> terms) {
    std::vector<ScriptedToken> result;
    int32_t offset = 0;
    for (auto term : terms) {
        result.push_back(
                {std::string(term), 1, offset, offset + static_cast<int32_t>(term.size())});
        offset += static_cast<int32_t>(term.size()) + 1;
    }
    return result;
}

std::vector<std::string> terms(const std::vector<ActualToken>& tokens) {
    std::vector<std::string> result;
    for (const auto& token : tokens) {
        EXPECT_EQ(token.position_increment, 1);
        result.push_back(token.term);
    }
    return result;
}

std::shared_ptr<const CommonWordSet> builtin_common_words() {
    static const auto common_words = std::shared_ptr<const CommonWordSet>(
            &CommonWordSet::builtin_english_stop_words_v1(), [](const CommonWordSet*) {});
    return common_words;
}

TokenStreamPtr index_stream(const std::shared_ptr<ScriptedTokenStream>& input) {
    return std::make_shared<CommonGramsFilter>(input, builtin_common_words());
}

TokenStreamPtr escaped_index_stream(const std::shared_ptr<ScriptedTokenStream>& input) {
    return std::make_shared<CommonGramsFilter>(input, builtin_common_words(),
                                               CommonGramsOutputMode::kEscapedV1Index);
}

TokenStreamPtr spimi_index_stream(const std::shared_ptr<ScriptedTokenStream>& input) {
    return std::make_shared<CommonGramsFilter>(input, builtin_common_words(),
                                               CommonGramsOutputMode::kEscapedV1SpimiIndex);
}

TokenStreamPtr plain_stream(const std::shared_ptr<ScriptedTokenStream>& input) {
    return std::make_shared<CommonGramsPositionFilter>(input);
}

TokenStreamPtr exact_stream(const std::shared_ptr<ScriptedTokenStream>& input) {
    return std::make_shared<CommonGramsQueryFilter>(index_stream(input), builtin_common_words());
}

TokenStreamPtr prefix_stream(const std::shared_ptr<ScriptedTokenStream>& input) {
    return std::make_shared<CommonGramsPhrasePrefixFilter>(index_stream(input),
                                                           builtin_common_words());
}

TEST(CommonGramsFilterTest, IndexPreservesUnigramsAndAddsEligibleOwnedGrams) {
    auto input = std::make_shared<ScriptedTokenStream>(words({"man", "of", "the", "year"}));
    auto stream = index_stream(input);

    EXPECT_EQ(collect(stream), (std::vector<ActualToken> {
                                       {"man", 1, 0, 3, Token::getDefaultType()},
                                       {gram("man", "of"), 0, 0, 6, COMMON_GRAM_TOKEN_TYPE},
                                       {"of", 1, 4, 6, Token::getDefaultType()},
                                       {gram("of", "the"), 0, 4, 10, COMMON_GRAM_TOKEN_TYPE},
                                       {"the", 1, 7, 10, Token::getDefaultType()},
                                       {gram("the", "year"), 0, 7, 15, COMMON_GRAM_TOKEN_TYPE},
                                       {"year", 1, 11, 15, Token::getDefaultType()},
                               }));
}

TEST(CommonGramsFilterTest, QueryModesNormalizeBothCommonGramType) {
    auto input = std::make_shared<ScriptedTokenStream>(words({"of", "the"}));
    EXPECT_EQ(collect(exact_stream(input)),
              (std::vector<ActualToken> {
                      {gram("of", "the"), 1, 0, 6, COMMON_GRAM_TOKEN_TYPE},
              }));

    input = std::make_shared<ScriptedTokenStream>(words({"of", "the"}));
    EXPECT_EQ(collect(prefix_stream(input)),
              (std::vector<ActualToken> {
                      {gram("of", "the"), 1, 0, 6, COMMON_GRAM_TOKEN_TYPE},
              }));
}

TEST(CommonGramsFilterTest, QueryGramEligibilityMatchesPurposeSpecificFilters) {
    const std::vector<std::vector<std::string>> cases {
            {},
            {"alpha"},
            {"alpha", "beta"},
            {"alpha", "the"},
            {"the", "alpha"},
            {"alpha", "beta", "the"},
            {"alpha", "the", "beta"},
            {"alpha", "beta", "gamma", "delta"},
    };
    for (const auto& query_terms : cases) {
        SCOPED_TRACE(testing::PrintToString(query_terms));
        std::vector<ScriptedToken> scripted;
        scripted.reserve(query_terms.size());
        for (const auto& term : query_terms) {
            scripted.push_back({.term = term});
        }

        for (const auto mode :
             {CommonGramsQueryMode::kExact, CommonGramsQueryMode::kPhrasePrefix}) {
            auto input = std::make_shared<ScriptedTokenStream>(scripted);
            const auto output =
                    collect(mode == CommonGramsQueryMode::kExact ? exact_stream(input)
                                                                 : prefix_stream(input));
            bool filter_used_gram = false;
            for (const auto& token : output) {
                filter_used_gram =
                        filter_used_gram || token.type == std::wstring(COMMON_GRAM_TOKEN_TYPE);
            }
            EXPECT_EQ(common_grams_query_may_use_gram(query_terms, mode, *builtin_common_words()),
                      filter_used_gram);
        }
    }
}

TEST(CommonGramsFilterTest, PhysicalIndexModeEscapesPlainTermsButGramsUseLogicalBytes) {
    const std::string internal_plain = std::string(1, '\x1f') + "literal";
    auto input = std::make_shared<ScriptedTokenStream>(words({internal_plain, "of"}));
    auto stream = escaped_index_stream(input);

    EXPECT_EQ(collect(stream),
              (std::vector<ActualToken> {
                      {std::string(1, PLAIN_ESCAPE_PREFIX) + "Gliteral", 1, 0,
                       static_cast<int32_t>(internal_plain.size()), Token::getDefaultType()},
                      {gram(internal_plain, "of"), 0, 0,
                       static_cast<int32_t>(internal_plain.size() + 3), COMMON_GRAM_TOKEN_TYPE},
                      {"of", 1, static_cast<int32_t>(internal_plain.size() + 1),
                       static_cast<int32_t>(internal_plain.size() + 3), Token::getDefaultType()},
              }));

    const std::string escape_plain = std::string(1, PLAIN_ESCAPE_PREFIX) + "literal";
    input->set_tokens(words({escape_plain}));
    stream->reset();
    EXPECT_EQ(terms(collect(stream)),
              (std::vector<std::string> {std::string(1, PLAIN_ESCAPE_PREFIX) + "Eliteral"}));
}

TEST(CommonGramsFilterTest, SpimiIndexModeEmitsPhysicalGramsAndEscapedPlainTerms) {
    const std::string internal_plain = std::string(1, '\x1f') + "literal";
    auto input = std::make_shared<ScriptedTokenStream>(words({internal_plain, "of"}));

    const std::string physical = gram(internal_plain, "of");
    EXPECT_EQ(collect(spimi_index_stream(input)),
              (std::vector<ActualToken> {
                      {std::string(1, PLAIN_ESCAPE_PREFIX) + "Gliteral", 1, 0,
                       static_cast<int32_t>(internal_plain.size()), Token::getDefaultType()},
                      {physical, 0, 0, static_cast<int32_t>(internal_plain.size() + 3),
                       COMMON_GRAM_TOKEN_TYPE},
                      {"of", 1, static_cast<int32_t>(internal_plain.size() + 1),
                       static_cast<int32_t>(internal_plain.size() + 3), Token::getDefaultType()},
              }));
}

TEST(CommonGramsFilterTest, SniiIndexEventsExpandToExistingSpimiTokenSemantics) {
    const std::string internal_plain = std::string(1, '\x1f') + "literal";
    const std::string escaped_plain = std::string(1, PLAIN_ESCAPE_PREFIX) + "literal";
    const std::vector<ScriptedToken> input_tokens =
            words({internal_plain, "of", "the", escaped_plain, "中文词"});

    auto legacy_input = std::make_shared<ScriptedTokenStream>(input_tokens);
    const std::vector<TokenSemantics> expected =
            collect_semantics(spimi_index_stream(legacy_input));

    auto event_input = std::make_shared<ScriptedTokenStream>(input_tokens);
    CommonGramsFilter event_stream(event_input, builtin_common_words(),
                                   CommonGramsOutputMode::kEscapedV1SpimiIndex);
    size_t event_count = 0;
    size_t both_common_gram_count = 0;
    const std::vector<TokenSemantics> actual =
            expand_snii_index_events(&event_stream, &event_count, &both_common_gram_count);

    EXPECT_EQ(event_count, input_tokens.size());
    EXPECT_EQ(both_common_gram_count, 1U);
    EXPECT_EQ(actual, expected);
}

TEST(CommonGramsFilterTest, SniiIndexEventsDeferCommonWordClassificationToWriter) {
    auto input = std::make_shared<ScriptedTokenStream>(
            words({"the", "database", "of", "the", "world", "and", "search"}));
    CommonGramsFilter stream(input, builtin_common_words(),
                             CommonGramsOutputMode::kEscapedV1SpimiIndex);

    common_grams_testing::reset_common_word_membership_lookup_count();
    SniiCommonGramsIndexEvent event;
    size_t event_count = 0;
    while (stream.next_snii_index_event(&event)) {
        EXPECT_FALSE(event.logical_term.empty());
        EXPECT_FALSE(event.plain_term.empty());
        ++event_count;
    }

    EXPECT_EQ(event_count, 7U);
    EXPECT_EQ(common_grams_testing::common_word_membership_lookup_count(), 0U);
}

TEST(CommonGramsFilterTest, IndexOmitsNonCommonPair) {
    auto input = std::make_shared<ScriptedTokenStream>(words({"man", "year"}));
    EXPECT_EQ(terms(collect(index_stream(input))), (std::vector<std::string> {"man", "year"}));
}

TEST(CommonGramsFilterTest, CachesCommonWordMembershipWithoutDefeatingShortCircuit) {
    auto input = std::make_shared<ScriptedTokenStream>(words({"single"}));
    common_grams_testing::reset_common_word_membership_lookup_count();
    collect(index_stream(input));
    EXPECT_EQ(common_grams_testing::common_word_membership_lookup_count(), 0);

    input = std::make_shared<ScriptedTokenStream>(words({"of", "dog"}));
    common_grams_testing::reset_common_word_membership_lookup_count();
    collect(index_stream(input));
    EXPECT_EQ(common_grams_testing::common_word_membership_lookup_count(), 1);

    input = std::make_shared<ScriptedTokenStream>(words({"of", "the", "and", "to"}));
    common_grams_testing::reset_common_word_membership_lookup_count();
    collect(exact_stream(input));
    EXPECT_EQ(common_grams_testing::common_word_membership_lookup_count(), 6);

    input = std::make_shared<ScriptedTokenStream>(words({"man", "dog"}));
    common_grams_testing::reset_common_word_membership_lookup_count();
    collect(prefix_stream(input));
    EXPECT_EQ(common_grams_testing::common_word_membership_lookup_count(), 3);

    input = std::make_shared<ScriptedTokenStream>(words({"man", "dog", "year", "thing"}));
    common_grams_testing::reset_common_word_membership_lookup_count();
    EXPECT_EQ(terms(collect(index_stream(input))),
              (std::vector<std::string> {"man", "dog", "year", "thing"}));
    EXPECT_EQ(common_grams_testing::common_word_membership_lookup_count(), 4);

    input = std::make_shared<ScriptedTokenStream>(words({"man", "dog", "year", "thing"}));
    common_grams_testing::reset_common_word_membership_lookup_count();
    EXPECT_EQ(terms(collect(exact_stream(input))),
              (std::vector<std::string> {"man", "dog", "year", "thing"}));
    EXPECT_EQ(common_grams_testing::common_word_membership_lookup_count(), 8);
}

TEST(CommonGramsFilterTest, ExactRewriteTruthTable) {
    struct Case {
        std::vector<ScriptedToken> input;
        std::vector<std::string> expected;
    };
    const std::vector<Case> cases = {
            {words({"man", "dog", "year"}), {"man", "dog", "year"}},
            {words({"man", "dog", "the"}), {"man", gram("dog", "the")}},
            {words({"man", "of", "year"}), {gram("man", "of"), gram("of", "year")}},
            {words({"man", "of", "the"}), {gram("man", "of"), gram("of", "the")}},
            {words({"of", "dog", "year"}), {gram("of", "dog"), "dog", "year"}},
            {words({"of", "dog", "the"}), {gram("of", "dog"), gram("dog", "the")}},
            {words({"of", "the", "year"}), {gram("of", "the"), gram("the", "year")}},
            {words({"of", "the", "and"}), {gram("of", "the"), gram("the", "and")}},
            {words({"the", "the", "the"}), {gram("the", "the"), gram("the", "the")}},
    };

    for (const auto& test_case : cases) {
        auto input = std::make_shared<ScriptedTokenStream>(test_case.input);
        EXPECT_EQ(terms(collect(exact_stream(input))), test_case.expected);
    }
}

TEST(CommonGramsFilterTest, PhrasePrefixRewritesOnlyCommonLeftBoundary) {
    struct Case {
        std::vector<ScriptedToken> input;
        std::vector<std::string> expected;
    };
    const std::vector<Case> cases = {
            {words({"the", "wo"}), {gram("the", "wo")}},
            {words({"foo", "the"}), {"foo", "the"}},
            {words({"foo", "of", "th"}), {gram("foo", "of"), gram("of", "th")}},
            {words({"the", "bar", "ba"}), {gram("the", "bar"), "bar", "ba"}},
            {words({"the"}), {"the"}},
    };

    for (const auto& test_case : cases) {
        auto input = std::make_shared<ScriptedTokenStream>(test_case.input);
        EXPECT_EQ(terms(collect(prefix_stream(input))), test_case.expected);
    }
}

TEST(CommonGramsFilterTest, ResetAndReuseRestoresUnigramMetadata) {
    auto input = std::make_shared<ScriptedTokenStream>(words({"man", "of"}));
    auto stream = index_stream(input);
    ASSERT_EQ(collect(stream).size(), 3);

    input->set_tokens({{"plain", 1, 17, 22, Token::getDefaultType()}});
    stream->reset();
    EXPECT_EQ(collect(stream), (std::vector<ActualToken> {
                                       {"plain", 1, 17, 22, Token::getDefaultType()},
                               }));
}

TEST(CommonGramsFilterTest, PreservesUpstreamUnigramTypes) {
    auto input = std::make_shared<ScriptedTokenStream>(std::vector<ScriptedToken> {
            {"man", 1, 3, 6, L"left_type"}, {"of", 1, 7, 9, L"right_type"}});
    EXPECT_EQ(collect(index_stream(input)),
              (std::vector<ActualToken> {
                      {"man", 1, 3, 6, L"left_type"},
                      {gram("man", "of"), 0, 3, 9, COMMON_GRAM_TOKEN_TYPE},
                      {"of", 1, 7, 9, L"right_type"},
              }));

    input->set_tokens({{"man", 1, 3, 6, L"left_type"}, {"year", 1, 7, 11, L"right_type"}});
    auto exact = exact_stream(input);
    EXPECT_EQ(collect(exact), (std::vector<ActualToken> {
                                      {"man", 1, 3, 6, L"left_type"},
                                      {"year", 1, 7, 11, L"right_type"},
                              }));
}

TEST(CommonGramsFilterTest, ExactAndPrefixResetReuseIndependentInput) {
    auto exact_input = std::make_shared<ScriptedTokenStream>(words({"man", "of"}));
    auto exact = exact_stream(exact_input);
    EXPECT_EQ(terms(collect(exact)), (std::vector<std::string> {gram("man", "of")}));
    exact_input->set_tokens(words({"plain", "terms"}));
    exact->reset();
    EXPECT_EQ(terms(collect(exact)), (std::vector<std::string> {"plain", "terms"}));

    auto prefix_input = std::make_shared<ScriptedTokenStream>(words({"the", "term"}));
    auto prefix = prefix_stream(prefix_input);
    EXPECT_EQ(terms(collect(prefix)), (std::vector<std::string> {gram("the", "term")}));
    prefix_input->set_tokens(words({"plain", "terms"}));
    prefix->reset();
    EXPECT_EQ(terms(collect(prefix)), (std::vector<std::string> {"plain", "terms"}));
}

TEST(CommonGramsFilterTest, EmptyAndSingleTokenStreamsAreStable) {
    auto input = std::make_shared<ScriptedTokenStream>(std::vector<ScriptedToken> {});
    auto stream = index_stream(input);
    EXPECT_TRUE(collect(stream).empty());

    input->set_tokens(words({"of"}));
    stream->reset();
    EXPECT_EQ(terms(collect(stream)), (std::vector<std::string> {"of"}));
}

TEST(CommonGramsFilterTest, RejectsNonUnitInputForEveryStreamPurpose) {
    using Factory = TokenStreamPtr (*)(const std::shared_ptr<ScriptedTokenStream>&);
    for (Factory factory : {index_stream, plain_stream, exact_stream, prefix_stream}) {
        for (int32_t bad_increment : {-1, 0, 2}) {
            auto input = std::make_shared<ScriptedTokenStream>(
                    std::vector<ScriptedToken> {{"of", bad_increment, 0, 2}});
            try {
                collect(factory(input));
                FAIL() << "expected analyzer error for increment " << bad_increment;
            } catch (const Exception& error) {
                EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
            }
        }

        for (int32_t bad_increment : {-1, 0, 2}) {
            auto empty_input = std::make_shared<ScriptedTokenStream>(
                    std::vector<ScriptedToken> {{"", bad_increment, 0, 0}});
            try {
                collect(factory(empty_input));
                FAIL() << "expected analyzer error for empty token increment " << bad_increment;
            } catch (const Exception& error) {
                EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
            }
        }

        auto input = std::make_shared<ScriptedTokenStream>(
                std::vector<ScriptedToken> {{"man", 1, 0, 3}, {"of", 2, 4, 6}});
        auto stream = factory(input);
        try {
            collect(stream);
            FAIL() << "expected analyzer error after a valid token";
        } catch (const Exception& error) {
            EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
            EXPECT_EQ(input->consumed(), 2);
        }
    }
}

TEST(CommonGramsFilterTest, RejectsEmptyInputForEveryStreamPurpose) {
    using Factory = TokenStreamPtr (*)(const std::shared_ptr<ScriptedTokenStream>&);
    for (Factory factory : {index_stream, plain_stream, exact_stream, prefix_stream}) {
        auto input =
                std::make_shared<ScriptedTokenStream>(std::vector<ScriptedToken> {{"", 1, 0, 0}});
        try {
            collect(factory(input));
            FAIL() << "expected analyzer error for an empty token";
        } catch (const Exception& error) {
            EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
        }
    }
}

TEST(CommonGramsFilterTest, QueryPreparationLatchesFailureUntilReset) {
    for (auto factory : {exact_stream, prefix_stream}) {
        auto input = std::make_shared<ScriptedTokenStream>(
                std::vector<ScriptedToken> {{"the", 1, 0, 3}, {"term", 2, 4, 8}});
        auto stream = factory(input);
        for (int attempt = 0; attempt < 2; ++attempt) {
            try {
                collect(stream);
                FAIL() << "expected latched analyzer error on attempt " << attempt;
            } catch (const Exception& error) {
                EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
            }
        }

        input->set_tokens(words({"the", "term"}));
        stream->reset();
        EXPECT_EQ(terms(collect(stream)), (std::vector<std::string> {gram("the", "term")}));
    }
}

TEST(CommonGramsFilterTest, UnencodableRequiredGramFallsBackToWholePlainSequence) {
    const std::string huge(COMMON_GRAM_MAX_ENCODED_BYTES, 'x');

    for (auto factory : {exact_stream, prefix_stream}) {
        auto input = std::make_shared<ScriptedTokenStream>(words({"the", huge}));
        EXPECT_EQ(terms(collect(factory(input))), (std::vector<std::string> {"the", huge}));

        input = std::make_shared<ScriptedTokenStream>(words({"the", "of", huge}));
        EXPECT_EQ(terms(collect(factory(input))), (std::vector<std::string> {"the", "of", huge}));
    }

    auto input = std::make_shared<ScriptedTokenStream>(words({"the", huge}));
    EXPECT_EQ(terms(collect(index_stream(input))), (std::vector<std::string> {"the", huge}));
}

TEST(CommonGramsFilterTest, MaximumMarkerLeadingUnigramFailsWithBuildSwitchRecovery) {
    for (const char marker : {PLAIN_ESCAPE_PREFIX, '\x1f'}) {
        std::string term(COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
        term.front() = marker;
        auto input = std::make_shared<ScriptedTokenStream>(
                std::vector<ScriptedToken> {{term, 1, 0, static_cast<int32_t>(term.size())}});
        try {
            static_cast<void>(collect(escaped_index_stream(input)));
            FAIL() << "expected CommonGrams escaped-term overflow";
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
            EXPECT_NE(std::string(e.what()).find("enable_common_grams_index_build=false"),
                      std::string::npos);
            EXPECT_NE(std::string(e.what()).find("new transaction"), std::string::npos);
        }
    }
}

TEST(CommonGramsFilterTest, LargestEscapableMarkerLeadingUnigramUsesPhysicalKeyLimit) {
    for (const char marker : {PLAIN_ESCAPE_PREFIX, '\x1f'}) {
        std::string term(COMMON_GRAM_MAX_ENCODED_BYTES - 1, 'x');
        term.front() = marker;
        auto input = std::make_shared<ScriptedTokenStream>(
                std::vector<ScriptedToken> {{term, 1, 0, static_cast<int32_t>(term.size())}});
        const auto tokens = collect(escaped_index_stream(input));
        ASSERT_EQ(tokens.size(), 1);
        EXPECT_EQ(tokens[0].term.size(), COMMON_GRAM_MAX_ENCODED_BYTES);
    }
}

TEST(CommonGramsFilterTest, InvalidLogicalTermsRemainHardAnalyzerErrors) {
    for (const std::string& term : {std::string("bad\0term", 8), std::string("\xc3", 1)}) {
        auto input = std::make_shared<ScriptedTokenStream>(
                std::vector<ScriptedToken> {{term, 1, 0, static_cast<int32_t>(term.size())}});
        try {
            collect(escaped_index_stream(input));
            FAIL() << "expected analyzer error";
        } catch (const Exception& error) {
            EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
        }
    }
}

TEST(CommonGramsFilterTest, RejectsOverlongLogicalToken) {
    auto input = std::make_shared<ScriptedTokenStream>(
            std::vector<ScriptedToken> {{std::string(COMMON_GRAM_MAX_ENCODED_BYTES + 1, 'x'), 1, 0,
                                         static_cast<int32_t>(COMMON_GRAM_MAX_ENCODED_BYTES + 1)}});
    try {
        collect(index_stream(input));
        FAIL() << "expected analyzer error";
    } catch (const Exception& error) {
        EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index
