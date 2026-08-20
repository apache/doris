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

#include "storage/index/inverted/analyzer/custom_analyzer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <string_view>

#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "roaring/roaring.hh"
#include "runtime/exec_env.h"
#include "storage/index/inverted/analysis_factory_mgr.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_word_set.h"
#include "storage/index/inverted/query/phrase_prefix_query.h"
#include "storage/index/inverted/query/phrase_query.h"
#include "storage/index/inverted/setting.h"

CL_NS_USE(util)
CL_NS_USE(store)
CL_NS_USE(search)
CL_NS_USE(index)

namespace doris::segment_v2::inverted_index {

class TimeGuard {
public:
    TimeGuard(std::string message) : message_(std::move(message)) {
        begin_ = duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    }

    ~TimeGuard() {
        int64_t end = duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
        std::cout << message_ << ": " << end - begin_ << std::endl;
    }

private:
    std::string message_;
    int64_t begin_ = 0;
};

constexpr static uint32_t MAX_PATH_LEN = 1024;

class CustomAnalyzerTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _curreent_dir = std::string(buffer);

        _word_delimiter_params1.set("generate_word_parts", "true");
        _word_delimiter_params1.set("generate_number_parts", "true");
        _word_delimiter_params1.set("catenate_words", "true");
        _word_delimiter_params1.set("catenate_numbers", "true");
        _word_delimiter_params1.set("catenate_all", "true");
        _word_delimiter_params1.set("split_on_case_change", "true");
        _word_delimiter_params1.set("preserve_original", "true");
        _word_delimiter_params1.set("split_on_numerics", "true");
        _word_delimiter_params1.set("stem_english_possessive", "true");

        _word_delimiter_params2.set("generate_word_parts", "false");
        _word_delimiter_params2.set("generate_number_parts", "false");
        _word_delimiter_params2.set("catenate_words", "false");
        _word_delimiter_params2.set("catenate_numbers", "false");
        _word_delimiter_params2.set("catenate_all", "false");
        _word_delimiter_params2.set("split_on_case_change", "false");
        _word_delimiter_params2.set("preserve_original", "false");
        _word_delimiter_params2.set("split_on_numerics", "false");
        _word_delimiter_params2.set("stem_english_possessive", "false");
    }

    std::string _curreent_dir;
    Settings _word_delimiter_params1;
    Settings _word_delimiter_params2;
};

int32_t tokenize(const CustomAnalyzerPtr& custom_analyzer, const std::vector<std::string>& lines) {
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    size_t total_count = 0;
    Token t;
    for (size_t i = 0; i < lines.size(); ++i) {
        reader->init(lines[i].data(), lines[i].size(), false);
        auto* token_stream = custom_analyzer->reusableTokenStream(L"", reader);
        token_stream->reset();
        while (token_stream->next(&t)) {
            total_count++;
        }
    }
    return total_count;
}

struct ExpectedToken {
    std::string term;
    int32_t pos = 0;

    bool operator==(const ExpectedToken& other) const {
        return term == other.term && pos == other.pos;
    }
};

std::vector<ExpectedToken> tokenize1(const CustomAnalyzerPtr& custom_analyzer,
                                     const std::string line) {
    std::vector<ExpectedToken> results;
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(line.data(), line.size(), false);
    auto* token_stream = custom_analyzer->reusableTokenStream(L"", reader);
    token_stream->reset();
    Token t;
    while (token_stream->next(&t)) {
        results.emplace_back(std::string(t.termBuffer<char>(), t.termLength<char>()),
                             t.getPositionIncrement());
    }
    return results;
}

TEST_F(CustomAnalyzerTest, CustomStandardAnalyzer) {
    std::vector<std::string> lines;
    {
        std::ifstream ifs(_curreent_dir +
                          "/be/test/storage/index/inverted/data/sorted_wikipedia-50-1.json");
        std::string line;
        while (getline(ifs, line)) {
            lines.emplace_back(line);
        }
        ifs.close();
    }

    auto custom_tokenize = [&lines](const std::string& tokenizer,
                                    const Settings& word_delimiter_params) {
        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config(tokenizer, {});
        builder.add_token_filter_config("asciifolding", {});
        builder.add_token_filter_config("word_delimiter", word_delimiter_params);
        builder.add_token_filter_config("lowercase", {});
        auto custom_analyzer_config = builder.build();
        auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);
        return tokenize(custom_analyzer, lines);
    };

    {
        auto total1 = custom_tokenize("standard", _word_delimiter_params1);
        auto total2 = custom_tokenize("standard", _word_delimiter_params2);
        EXPECT_EQ(total1, 51658);
        EXPECT_EQ(total2, 42774);
    }
}

TEST_F(CustomAnalyzerTest, CustomNgramAnalyzer) {
    {
        std::string line = "a b";

        Settings ngram_params;
        ngram_params.set("max_gram", "2");
        ngram_params.set("token_chars", "");

        Settings word_delimiter_params;
        word_delimiter_params.set("stem_english_possessive", "true");
        word_delimiter_params.set("catenate_words", "true");
        word_delimiter_params.set("generate_number_parts", "true");
        word_delimiter_params.set("preserve_original", "true");
        word_delimiter_params.set("split_on_case_change", "true");
        word_delimiter_params.set("split_on_numerics", "false");

        Settings ascii_folding_params;
        ascii_folding_params.set("preserve_original", "true");

        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("ngram", ngram_params);
        builder.add_token_filter_config("word_delimiter", word_delimiter_params);
        auto custom_analyzer_config = builder.build();
        auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

        std::vector<ExpectedToken> expected = {{"a", 1},  {"a ", 1}, {"a", 0}, {" ", 1},
                                               {" b", 1}, {"b", 0},  {"b", 1}};
        EXPECT_EQ(tokenize1(custom_analyzer, line), expected);
    }
}

TEST_F(CustomAnalyzerTest, TokenStreamNotSupported) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("standard", {});
    auto custom_analyzer_config = builder.build();
    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init("test content", 12, false);

    EXPECT_THROW({ custom_analyzer->tokenStream(L"field", reader.get()); }, Exception);

    EXPECT_THROW({ custom_analyzer->reusableTokenStream(L"field", reader.get()); }, Exception);
}

TEST_F(CustomAnalyzerTest, ReusableTokenStreamNotSupported) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("standard", {});
    auto custom_analyzer_config = builder.build();
    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init("test content", 12, false);

    EXPECT_THROW({ custom_analyzer->reusableTokenStream(L"field", reader.get()); }, Exception);

    try {
        custom_analyzer->reusableTokenStream(L"field", reader.get());
        FAIL() << "Expected Exception to be thrown";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
        EXPECT_STREQ(e.what(), "[E-6001] CustomAnalyzer::reusableTokenStream not supported");
    }
}

TEST_F(CustomAnalyzerTest, TokenStreamWithReaderPtr) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("standard", {});
    builder.add_token_filter_config("lowercase", {});
    auto custom_analyzer_config = builder.build();
    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init("Hello World Test", 16, false);

    auto* token_stream = custom_analyzer->tokenStream(L"field", reader);
    EXPECT_NE(token_stream, nullptr);

    Token t;
    std::vector<std::string> tokens;
    token_stream->reset();
    while (token_stream->next(&t)) {
        tokens.emplace_back(std::string(t.termBuffer<char>(), t.termLength<char>()));
    }

    std::vector<std::string> expected = {"hello", "world", "test"};
    EXPECT_EQ(tokens, expected);

    delete token_stream;
}

CustomAnalyzerConfigPtr common_grams_config(
        const std::string& tokenizer, const Settings& tokenizer_settings,
        const std::vector<std::pair<std::string, Settings>>& filters = {},
        const Settings& common_grams_settings = {}) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config(tokenizer, tokenizer_settings);
    for (const auto& [name, settings] : filters) {
        builder.add_token_filter_config(name, settings);
    }
    builder.add_token_filter_config("common_grams", common_grams_settings);
    return builder.build();
}

Settings whitespace_tokenizer_settings() {
    Settings settings;
    settings.set("tokenize_on_chars", "[whitespace]");
    return settings;
}

std::string expected_gram(std::string_view left, std::string_view right) {
    auto result = encode_common_gram(left, right);
    EXPECT_TRUE(result.has_value()) << result.error();
    return result.value();
}

void expect_common_grams_analyzer_error(const CustomAnalyzerConfigPtr& config,
                                        AnalysisPurpose purpose) {
    try {
        CustomAnalyzer::build_custom_analyzer(config, purpose);
        FAIL() << "expected CommonGrams analyzer error";
    } catch (const Exception& error) {
        EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    }
}

TEST_F(CustomAnalyzerTest, CommonGramsBuildsIndependentPurposeStreams) {
    auto config =
            common_grams_config("char_group", whitespace_tokenizer_settings(), {{"lowercase", {}}});
    auto index = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kIndex);
    auto snii_index =
            CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kSniiTransientIndex);
    auto plain = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kPlainQuery);
    auto exact = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kExactPhraseQuery);
    auto prefix =
            CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kPhrasePrefixQuery);

    EXPECT_EQ(tokenize1(index, "Man of the Year"),
              (std::vector<ExpectedToken> {{"man", 1},
                                           {expected_gram("man", "of"), 0},
                                           {"of", 1},
                                           {expected_gram("of", "the"), 0},
                                           {"the", 1},
                                           {expected_gram("the", "year"), 0},
                                           {"year", 1}}));
    EXPECT_EQ(tokenize1(snii_index, "Man of the Year"),
              (std::vector<ExpectedToken> {{"man", 1},
                                           {expected_gram("man", "of"), 0},
                                           {"of", 1},
                                           {expected_gram("of", "the"), 0},
                                           {"the", 1},
                                           {expected_gram("the", "year"), 0},
                                           {"year", 1}}));
    EXPECT_EQ(tokenize1(plain, "Man of the Year"),
              (std::vector<ExpectedToken> {{"man", 1}, {"of", 1}, {"the", 1}, {"year", 1}}));
    EXPECT_EQ(tokenize1(exact, "Man of the Year"),
              (std::vector<ExpectedToken> {{expected_gram("man", "of"), 1},
                                           {expected_gram("of", "the"), 1},
                                           {expected_gram("the", "year"), 1}}));
    EXPECT_EQ(tokenize1(prefix, "the wo"),
              (std::vector<ExpectedToken> {{expected_gram("the", "wo"), 1}}));

    EXPECT_EQ(tokenize1(exact, "plain terms"),
              (std::vector<ExpectedToken> {{"plain", 1}, {"terms", 1}}));
    EXPECT_EQ(tokenize1(prefix, "of term"),
              (std::vector<ExpectedToken> {{expected_gram("of", "term"), 1}}));
}

TEST_F(CustomAnalyzerTest, CommonGramsReusableIndexStreamDoesNotBridgeRows) {
    auto analyzer = CustomAnalyzer::build_custom_analyzer(
            common_grams_config("char_group", whitespace_tokenizer_settings(), {{"lowercase", {}}}),
            AnalysisPurpose::kIndex);
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();

    auto tokenize_row = [&](std::string_view row) {
        reader->init(row.data(), static_cast<int32_t>(row.size()), false);
        auto* stream = analyzer->reusableTokenStream(L"", reader);
        stream->reset();
        std::vector<ExpectedToken> tokens;
        Token token;
        while (stream->next(&token)) {
            tokens.emplace_back(std::string(token.termBuffer<char>(), token.termLength<char>()),
                                token.getPositionIncrement());
        }
        return std::pair {stream, std::move(tokens)};
    };

    auto [first_stream, first] = tokenize_row("foo of");
    auto [second_stream, second] = tokenize_row("the bar");

    EXPECT_EQ(first_stream, second_stream);
    EXPECT_EQ(first, (std::vector<ExpectedToken> {
                             {"foo", 1}, {expected_gram("foo", "of"), 0}, {"of", 1}}));
    EXPECT_EQ(second, (std::vector<ExpectedToken> {
                              {"the", 1}, {expected_gram("the", "bar"), 0}, {"bar", 1}}));
}

TEST_F(CustomAnalyzerTest, CommonGramsEscapesPlainKeysOnlyForIndexPurpose) {
    Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[\\u0020]");
    auto config = common_grams_config("char_group", tokenizer_settings);
    auto index = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kIndex);
    auto plain = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kPlainQuery);
    auto exact = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kExactPhraseQuery);
    auto prefix =
            CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kPhrasePrefixQuery);

    const std::string logical = std::string(1, '\x1f') + "literal";
    const std::string input = "the " + logical;
    const std::string physical = std::string(1, PLAIN_ESCAPE_PREFIX) + "Gliteral";
    const std::string common_gram = expected_gram("the", logical);

    EXPECT_EQ(tokenize1(index, input),
              (std::vector<ExpectedToken> {{"the", 1}, {common_gram, 0}, {physical, 1}}));
    EXPECT_EQ(tokenize1(plain, input), (std::vector<ExpectedToken> {{"the", 1}, {logical, 1}}));
    EXPECT_EQ(tokenize1(exact, input), (std::vector<ExpectedToken> {{common_gram, 1}}));
    EXPECT_EQ(tokenize1(prefix, input), (std::vector<ExpectedToken> {{common_gram, 1}}));
}

TEST_F(CustomAnalyzerTest, AnalyseResultPreservesCommonGramTermKind) {
    auto config =
            common_grams_config("char_group", whitespace_tokenizer_settings(), {{"lowercase", {}}});
    auto exact = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kExactPhraseQuery);
    auto reader = InvertedIndexAnalyzer::create_reader({});
    const std::string input = "Man of year";
    reader->init(input.data(), static_cast<int32_t>(input.size()), true);

    const auto terms = InvertedIndexAnalyzer::get_analyse_result(reader, exact.get());
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].get_single_term(), expected_gram("man", "of"));
    EXPECT_EQ(terms[0].key_kind, TermKeyKind::kCommonGram);
    EXPECT_EQ(terms[1].get_single_term(), expected_gram("of", "year"));
    EXPECT_EQ(terms[1].key_kind, TermKeyKind::kCommonGram);

    auto plain = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kPlainQuery);
    reader = InvertedIndexAnalyzer::create_reader({});
    reader->init(input.data(), static_cast<int32_t>(input.size()), true);
    const auto plain_terms = InvertedIndexAnalyzer::get_analyse_result(reader, plain.get());
    ASSERT_EQ(plain_terms.size(), 3U);
    for (const auto& term : plain_terms) {
        EXPECT_EQ(term.key_kind, TermKeyKind::kPlain);
    }
}

TEST_F(CustomAnalyzerTest, AnalyseResultPreservesBothCommonGramTermKind) {
    auto config =
            common_grams_config("char_group", whitespace_tokenizer_settings(), {{"lowercase", {}}});
    auto index = CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kIndex);
    auto reader = InvertedIndexAnalyzer::create_reader({});
    const std::string input = "of the";
    reader->init(input.data(), static_cast<int32_t>(input.size()), true);

    const auto terms = InvertedIndexAnalyzer::get_analyse_result(reader, index.get());
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[1].get_single_term(), expected_gram("of", "the"));
    EXPECT_EQ(terms[1].key_kind, TermKeyKind::kCommonGram);
}

TEST_F(CustomAnalyzerTest, CommonGramsAllowsAbsentOrOneTerminalFilter) {
    CustomAnalyzerConfig::Builder missing;
    missing.with_tokenizer_config("char_group", whitespace_tokenizer_settings());
    auto missing_config = missing.build();
    for (AnalysisPurpose purpose :
         {AnalysisPurpose::kIndex, AnalysisPurpose::kPlainQuery, AnalysisPurpose::kExactPhraseQuery,
          AnalysisPurpose::kPhrasePrefixQuery}) {
        EXPECT_NO_THROW(CustomAnalyzer::build_custom_analyzer(missing_config, purpose));
    }

    CustomAnalyzerConfig::Builder duplicate;
    duplicate.with_tokenizer_config("char_group", whitespace_tokenizer_settings());
    duplicate.add_token_filter_config("common_grams", {});
    duplicate.add_token_filter_config("common_grams", {});
    expect_common_grams_analyzer_error(duplicate.build(), AnalysisPurpose::kIndex);

    CustomAnalyzerConfig::Builder not_last;
    not_last.with_tokenizer_config("char_group", whitespace_tokenizer_settings());
    not_last.add_token_filter_config("common_grams", {});
    not_last.add_token_filter_config("lowercase", {});
    expect_common_grams_analyzer_error(not_last.build(), AnalysisPurpose::kIndex);
}

TEST_F(CustomAnalyzerTest, CommonGramsValidatesPlainConfigurationButOmitsGrams) {
    Settings unknown;
    unknown.set("unknown_setting", "true");
    auto invalid = common_grams_config("char_group", whitespace_tokenizer_settings(), {}, unknown);
    expect_common_grams_analyzer_error(invalid, AnalysisPurpose::kPlainQuery);

    auto valid = common_grams_config("char_group", whitespace_tokenizer_settings());
    auto plain = CustomAnalyzer::build_custom_analyzer(valid, AnalysisPurpose::kPlainQuery);
    EXPECT_EQ(tokenize1(plain, "the term"), (std::vector<ExpectedToken> {{"the", 1}, {"term", 1}}));
}

TEST_F(CustomAnalyzerTest, CommonGramsIndexChainRejectsInvalidUtf8AfterAValidToken) {
    auto analyzer = CustomAnalyzer::build_custom_analyzer(
            common_grams_config("char_group", whitespace_tokenizer_settings(), {{"lowercase", {}}}),
            AnalysisPurpose::kIndex);
    const std::string input = std::string("valid b") + static_cast<char>(0xFF) + std::string("ad");
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(input.data(), static_cast<int32_t>(input.size()), false);
    auto* stream = analyzer->reusableTokenStream(L"", reader);
    stream->reset();

    Token token;
    ASSERT_NE(stream->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "valid");
    try {
        stream->next(&token);
        FAIL() << "expected malformed UTF-8 to fail the analyzer chain";
    } catch (const Exception& error) {
        EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    }
}

TEST_F(CustomAnalyzerTest, CommonGramsDeniesUnsafePositionFactories) {
    for (const std::string& tokenizer : {"standard", "pinyin", "basic", "icu", "keyword"}) {
        expect_common_grams_analyzer_error(common_grams_config(tokenizer, {}),
                                           AnalysisPurpose::kIndex);
    }

    Settings preserve_original;
    preserve_original.set("preserve_original", "true");
    expect_common_grams_analyzer_error(
            common_grams_config("char_group", whitespace_tokenizer_settings(),
                                {{"asciifolding", preserve_original}}),
            AnalysisPurpose::kIndex);
    expect_common_grams_analyzer_error(
            common_grams_config("char_group", whitespace_tokenizer_settings(),
                                {{"word_delimiter", {}}}),
            AnalysisPurpose::kIndex);
    expect_common_grams_analyzer_error(
            common_grams_config("char_group", whitespace_tokenizer_settings(), {{"pinyin", {}}}),
            AnalysisPurpose::kIndex);
}

TEST_F(CustomAnalyzerTest, CommonGramsAcceptsReviewedUnitPositionFactories) {
    for (const std::string& tokenizer : {"empty", "char_group", "ngram", "edge_ngram"}) {
        EXPECT_NO_THROW(CustomAnalyzer::build_custom_analyzer(
                common_grams_config(tokenizer, tokenizer == "char_group"
                                                       ? whitespace_tokenizer_settings()
                                                       : Settings {}),
                AnalysisPurpose::kIndex));
    }

    for (const std::string& filter : {"empty", "lowercase", "icu_normalizer", "asciifolding"}) {
        EXPECT_NO_THROW(CustomAnalyzer::build_custom_analyzer(
                common_grams_config("char_group", whitespace_tokenizer_settings(), {{filter, {}}}),
                AnalysisPurpose::kIndex));
    }
}

TEST_F(CustomAnalyzerTest, CommonGramsRejectsTokensNormalizedToEmpty) {
    const auto config = common_grams_config("char_group", whitespace_tokenizer_settings(),
                                            {{"icu_normalizer", {}}});
    const std::string input = std::string("\xC2\xAD") + " the";
    for (AnalysisPurpose purpose :
         {AnalysisPurpose::kIndex, AnalysisPurpose::kPlainQuery, AnalysisPurpose::kExactPhraseQuery,
          AnalysisPurpose::kPhrasePrefixQuery}) {
        auto analyzer = CustomAnalyzer::build_custom_analyzer(config, purpose);
        try {
            tokenize1(analyzer, input);
            ADD_FAILURE() << "expected analyzer error for purpose " << static_cast<int>(purpose);
        } catch (const Exception& error) {
            EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
        }
    }
}

TEST_F(CustomAnalyzerTest, CommonGramsInternalQueryFiltersAreNotRegistered) {
    for (const std::string& internal : {"common_grams_query", "common_grams_phrase_prefix"}) {
        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("char_group", whitespace_tokenizer_settings());
        builder.add_token_filter_config(internal, {});
        builder.add_token_filter_config("common_grams", {});
        EXPECT_THROW(
                CustomAnalyzer::build_custom_analyzer(builder.build(), AnalysisPurpose::kIndex),
                Exception);
    }
}

// "the" is a member of the built-in stop-word list, which is what default_word_set() resolves to
// when no wordset file is installed -- the provider can no longer be handed a word list of its own.
TEST_F(CustomAnalyzerTest, CommonGramsProviderCachesPurposeAnalyzersWithOneWordSetSnapshot) {
    auto provider = std::make_shared<CustomAnalyzerProvider>(common_grams_config(
            "char_group", whitespace_tokenizer_settings(), {{"lowercase", {}}}));

    auto index = provider->get_analyzer(AnalysisPurpose::kIndex);
    auto snii_index = provider->get_analyzer(AnalysisPurpose::kSniiTransientIndex);
    auto plain = provider->get_analyzer(AnalysisPurpose::kPlainQuery);
    auto exact = provider->get_analyzer(AnalysisPurpose::kExactPhraseQuery);
    auto prefix = provider->get_analyzer(AnalysisPurpose::kPhrasePrefixQuery);

    EXPECT_EQ(tokenize1(std::dynamic_pointer_cast<CustomAnalyzer>(index), "The year"),
              (std::vector<ExpectedToken> {
                      {"the", 1}, {expected_gram("the", "year"), 0}, {"year", 1}}));
    EXPECT_EQ(tokenize1(std::dynamic_pointer_cast<CustomAnalyzer>(snii_index), "The year"),
              (std::vector<ExpectedToken> {
                      {"the", 1}, {expected_gram("the", "year"), 0}, {"year", 1}}));
    EXPECT_EQ(tokenize1(std::dynamic_pointer_cast<CustomAnalyzer>(plain), "The year"),
              (std::vector<ExpectedToken> {{"the", 1}, {"year", 1}}));
    EXPECT_EQ(tokenize1(std::dynamic_pointer_cast<CustomAnalyzer>(exact), "The year"),
              (std::vector<ExpectedToken> {{expected_gram("the", "year"), 1}}));
    EXPECT_EQ(tokenize1(std::dynamic_pointer_cast<CustomAnalyzer>(prefix), "The ye"),
              (std::vector<ExpectedToken> {{expected_gram("the", "ye"), 1}}));

    // Every purpose analyzer shares the one process-wide word list.
    EXPECT_EQ(provider->common_words(), CommonWordSet::default_word_set());
    EXPECT_EQ(provider->get_analyzer(AnalysisPurpose::kPlainQuery), plain);
    EXPECT_NE(index, plain);
    EXPECT_NE(index, snii_index);
    EXPECT_NE(plain, exact);
    EXPECT_NE(exact, prefix);
}

TEST_F(CustomAnalyzerTest, CommonGramsProviderOwnsDeterministicBuiltinIdentity) {
    Settings first_settings;
    first_settings.set("max_token_length", "16383");
    first_settings.set("tokenize_on_chars", "[whitespace]");
    Settings second_settings;
    second_settings.set("tokenize_on_chars", "[whitespace]");
    second_settings.set("max_token_length", "16383");

    CustomAnalyzerProvider first(
            common_grams_config("char_group", first_settings, {{"lowercase", {}}}));
    CustomAnalyzerProvider second(
            common_grams_config("char_group", second_settings, {{"lowercase", {}}}));

    ASSERT_NE(first.common_grams_identity(), nullptr);
    ASSERT_NE(second.common_grams_identity(), nullptr);
    EXPECT_EQ(first.common_grams_identity()->common_grams_dictionary_identity,
              BUILTIN_COMMON_WORDS_RESOURCE);
    EXPECT_EQ(*first.common_grams_identity(), *second.common_grams_identity());
    EXPECT_EQ(first.common_grams_identity()->base_analyzer_fingerprint.size(), 64U);
    EXPECT_EQ(first.common_grams_identity()->common_grams_fingerprint.size(), 64U);
}

TEST_F(CustomAnalyzerTest, CommonGramsProviderSeparatesBaseAndDictionaryIdentity) {
    Settings first_settings = whitespace_tokenizer_settings();
    first_settings.set("max_token_length", "100");
    Settings second_settings = whitespace_tokenizer_settings();
    second_settings.set("max_token_length", "101");
    CustomAnalyzerProvider first(common_grams_config("char_group", first_settings));
    CustomAnalyzerProvider second(common_grams_config("char_group", second_settings));

    ASSERT_NE(first.common_grams_identity(), nullptr);
    ASSERT_NE(second.common_grams_identity(), nullptr);
    EXPECT_NE(first.common_grams_identity()->base_analyzer_fingerprint,
              second.common_grams_identity()->base_analyzer_fingerprint);
    EXPECT_EQ(first.common_grams_identity()->common_grams_fingerprint,
              second.common_grams_identity()->common_grams_fingerprint);

    // The dictionary half of the identity is the word list's own content identity. Neither the
    // provider nor an index policy can supply one, so every provider in this process agrees on it.
    EXPECT_EQ(first.common_grams_identity()->common_grams_dictionary_identity,
              CommonWordSet::default_word_set()->identity());
    EXPECT_EQ(second.common_grams_identity()->common_grams_dictionary_identity,
              first.common_grams_identity()->common_grams_dictionary_identity);
}

TEST_F(CustomAnalyzerTest, CommonGramsIdentityIncludesOuterCharFilter) {
    CustomAnalyzerProvider no_outer_filter(
            common_grams_config("char_group", whitespace_tokenizer_settings()));
    ASSERT_NE(no_outer_filter.common_grams_identity(), nullptr);

    const std::map<std::string, std::string> slash_to_space = {
            {INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "/"},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " "}};
    const std::map<std::string, std::string> dash_to_space = {
            {INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "-"},
            {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " "}};
    CustomAnalyzerProvider slash_filter(
            common_grams_config("char_group", whitespace_tokenizer_settings()), slash_to_space);
    CustomAnalyzerProvider repeated_slash_filter(
            common_grams_config("char_group", whitespace_tokenizer_settings()), slash_to_space);
    CustomAnalyzerProvider dash_filter(
            common_grams_config("char_group", whitespace_tokenizer_settings()), dash_to_space);
    ASSERT_NE(slash_filter.common_grams_identity(), nullptr);
    ASSERT_NE(repeated_slash_filter.common_grams_identity(), nullptr);
    ASSERT_NE(dash_filter.common_grams_identity(), nullptr);

    EXPECT_EQ(*slash_filter.common_grams_identity(),
              *repeated_slash_filter.common_grams_identity());
    EXPECT_EQ(slash_filter.common_grams_identity()->common_grams_dictionary_identity,
              no_outer_filter.common_grams_identity()->common_grams_dictionary_identity);
    EXPECT_EQ(slash_filter.common_grams_identity()->common_grams_fingerprint,
              no_outer_filter.common_grams_identity()->common_grams_fingerprint);
    EXPECT_NE(slash_filter.common_grams_identity()->base_analyzer_fingerprint,
              no_outer_filter.common_grams_identity()->base_analyzer_fingerprint);
    EXPECT_NE(slash_filter.common_grams_identity()->base_analyzer_fingerprint,
              dash_filter.common_grams_identity()->base_analyzer_fingerprint);
}

TEST_F(CustomAnalyzerTest, ProviderSharesOneLegacyAnalyzerWhenCommonGramsIsAbsent) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("char_group", whitespace_tokenizer_settings());
    builder.add_token_filter_config("lowercase", {});
    auto provider = std::make_shared<CustomAnalyzerProvider>(builder.build());

    auto analyzer = provider->get_analyzer(AnalysisPurpose::kIndex);
    EXPECT_EQ(provider->get_analyzer(AnalysisPurpose::kPlainQuery), analyzer);
    EXPECT_EQ(provider->get_analyzer(AnalysisPurpose::kExactPhraseQuery), analyzer);
    EXPECT_EQ(provider->get_analyzer(AnalysisPurpose::kPhrasePrefixQuery), analyzer);
    EXPECT_EQ(provider->common_grams_identity(), nullptr);
}

TEST_F(CustomAnalyzerTest, ProviderExposesBaseFingerprintWithoutCommonGrams) {
    CustomAnalyzerConfig::Builder plain_builder;
    plain_builder.with_tokenizer_config("char_group", whitespace_tokenizer_settings());
    plain_builder.add_token_filter_config("lowercase", {});
    CustomAnalyzerProvider plain(plain_builder.build());

    CustomAnalyzerProvider common_grams(common_grams_config(
            "char_group", whitespace_tokenizer_settings(), {{"lowercase", {}}}));

    EXPECT_EQ(plain.base_analyzer_fingerprint().size(), 64U);
    EXPECT_EQ(plain.base_analyzer_fingerprint(), common_grams.base_analyzer_fingerprint());
    ASSERT_NE(common_grams.common_grams_identity(), nullptr);
    EXPECT_EQ(common_grams.base_analyzer_fingerprint(),
              common_grams.common_grams_identity()->base_analyzer_fingerprint);
}

// TEST_F(CustomAnalyzerTest, test) {
//     std::string name = "name";
//     std::string path = "/mnt/disk3/yangsiyu/clucene";

//     std::vector<std::string> lines;

//     // std::ifstream ifs("/mnt/disk2/yangsiyu/httplogs/wikipedia/wikipedia.json000");
//     // std::string line;
//     // while (getline(ifs, line)) {
//     //     lines.emplace_back(line);
//     // }
//     // ifs.close();

//     lines.emplace_back("A Super_Duper b c d");

//     std::cout << "lines size: " << lines.size() << std::endl;

//     Settings char_replace_params;
//     char_replace_params.set("char_filter_pattern", "_");
//     char_replace_params.set("char_filter_replacement", " ");

//     Settings word_delimiter_params;
//     word_delimiter_params.set("preserve_original", "true");

//     CustomAnalyzerConfig::Builder builder;
//     builder.with_tokenizer_config("standard", {});
//     builder.add_char_filter_config("char_replace", char_replace_params);
//     // builder.add_token_filter_config("word_delimiter", word_delimiter_params);
//     // builder.add_token_filter_config("asciifolding", {});
//     builder.add_token_filter_config("lowercase", {});
//     auto custom_analyzer_config = builder.build();

//     auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

//     auto result = tokenize1(custom_analyzer, lines[0]);
//     for (const auto& token : result) {
//         std::cout << token.term << " " << token.pos << std::endl;
//     }

//     // {
//     //     TimeGuard t("load time");

//     //     lucene::index::IndexWriter indexwriter(path.c_str(), custom_analyzer.get(), true);
//     //     indexwriter.setRAMBufferSizeMB(512);
//     //     indexwriter.setMaxFieldLength(0x7FFFFFFFL);
//     //     indexwriter.setMergeFactor(1000000000);
//     //     indexwriter.setUseCompoundFile(false);

//     //     auto reader = std::make_shared<lucene::util::SStringReader<char>>();

//     //     lucene::document::Document doc;
//     //     int32_t field_config = lucene::document::Field::STORE_NO;
//     //     field_config |= lucene::document::Field::INDEX_NONORMS;
//     //     field_config |= lucene::document::Field::INDEX_TOKENIZED;
//     //     auto field_name = std::wstring(name.begin(), name.end());
//     //     auto* field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
//     //     field->setOmitTermFreqAndPositions(false);
//     //     doc.add(*field);

//     //     for (int32_t j = 0; j < 1; j++) {
//     //         for (size_t k = 0; k < lines.size(); k++) {
//     //             reader->init(lines[k].data(), lines[k].size(), false);
//     //             auto* stream = custom_analyzer->reusableTokenStream(field->name(), reader);
//     //             field->setValue(stream);

//     //             indexwriter.addDocument(&doc);
//     //         }
//     //     }

//     //     std::cout << "---------------------" << std::endl;

//     //     indexwriter.close();
//     // }

//     // std::cout << "-----------" << std::endl;

//     // try {
//     //     {
//     //         auto* dir = FSDirectory::getDirectory(path.c_str());
//     //         auto* reader = IndexReader::open(dir, 1024 * 1024, true);
//     //         auto searcher = std::make_shared<IndexSearcher>(reader);

//     //         // std::cout << "macDoc: " << reader->maxDoc() << std::endl;

//     //         {
//     //             TimeGuard time("query time");

//     //             {
//     //                 IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();

//     //                 TQueryOptions query_options;
//     //                 doris::segment_v2::PhraseQuery query(searcher, context);

//     //                 InvertedIndexQueryInfo query_info;
//     //                 query_info.field_name = L"name";
//     //                 {
//     //                     doris::segment_v2::TermInfo t;
//     //                     t.term = "Super_Duper";
//     //                     t.position = 1;
//     //                     query_info.term_infos.emplace_back(std::move(t));
//     //                 }
//     //                 {
//     //                     doris::segment_v2::TermInfo t;
//     //                     t.term = "Super";
//     //                     t.position = 1;
//     //                     query_info.term_infos.emplace_back(std::move(t));
//     //                 }
//     //                 {
//     //                     doris::segment_v2::TermInfo t;
//     //                     t.term = "Duper";
//     //                     t.position = 2;
//     //                     query_info.term_infos.emplace_back(std::move(t));
//     //                 }
//     //                 {
//     //                     doris::segment_v2::TermInfo t;
//     //                     t.term = "c";
//     //                     t.position = 3;
//     //                     query_info.term_infos.emplace_back(std::move(t));
//     //                 }
//     //                 query_info.slop = 1;
//     //                 query_info.ordered = true;
//     //                 query.add(query_info);

//     //                 roaring::Roaring result;
//     //                 query.search(result);

//     //                 std::cout << "phrase_query count: " << result.cardinality() << std::endl;
//     //             }
//     //             // {
//     //             //     TQueryOptions query_options;
//     //             //     doris::segment_v2::PhrasePrefixQuery query(searcher, query_options, nullptr);

//     //             //     InvertedIndexQueryInfo query_info;
//     //             //     query_info.field_name = L"name";
//     //             //     {
//     //             //         doris::segment_v2::TermInfo t;
//     //             //         t.term = "Super_Duper";
//     //             //         t.position = 1;
//     //             //         query_info.term_infos.emplace_back(std::move(t));
//     //             //     }
//     //             //     {
//     //             //         doris::segment_v2::TermInfo t;
//     //             //         t.term = "Super";
//     //             //         t.position = 1;
//     //             //         query_info.term_infos.emplace_back(std::move(t));
//     //             //     }
//     //             //     {
//     //             //         doris::segment_v2::TermInfo t;
//     //             //         t.term = "Dup";
//     //             //         t.position = 2;
//     //             //         query_info.term_infos.emplace_back(std::move(t));
//     //             //     }
//     //             //     query.add(query_info);

//     //             //     roaring::Roaring result;
//     //             //     query.search(result);

//     //             //     std::cout << "phrase_prefix_query count: " << result.cardinality() << std::endl;
//     //             // }
//     //         }

//     //         reader->close();
//     //         _CLLDELETE(reader);
//     //         _CLDECDELETE(dir);
//     //     }
//     // } catch (const CLuceneError& e) {
//     //     std::cout << e.number() << ": " << e.what() << std::endl;
//     // }
// }

} // namespace doris::segment_v2::inverted_index