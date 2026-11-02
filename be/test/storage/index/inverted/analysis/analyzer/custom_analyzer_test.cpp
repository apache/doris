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

#include "storage/index/inverted/analysis/analyzer/custom_analyzer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "roaring/roaring.hh"
#include "runtime/exec_env.h"
#include "storage/index/inverted/analysis/analysis_factory_mgr.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/setting.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.h"

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

// A gram analyzer must reuse one unfiltered tokenizer across rows.
TEST_F(CustomAnalyzerTest, GramAnalyzerReusesOneTokenizerAcrossRows) {
    Settings gram_params;
    gram_params.set("mode", "sparse");
    gram_params.set("min_gram", "3");
    gram_params.set("max_gram", "16");
    gram_params.set("density", "0.25");

    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("ngram", gram_params);
    auto analyzer = CustomAnalyzer::build_custom_analyzer(builder.build());

    const std::vector<std::string> rows {"rpc error: code = Unavailable desc = transport closing",
                                         "手机微博", "SELECT COUNT(*) FROM tbl WHERE k = 42"};
    const auto drain = [](lucene::analysis::TokenStream* stream) {
        std::vector<std::string> tokens;
        Token token;
        while (stream->next(&token)) {
            tokens.emplace_back(token.termBuffer<char>(), token.termLength<char>());
        }
        return tokens;
    };

    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    lucene::analysis::TokenStream* cached = nullptr;
    std::vector<std::vector<std::string>> reused;
    for (const auto& row : rows) {
        reader->init(row.data(), static_cast<int32_t>(row.size()), false);
        auto* stream = analyzer->reusableTokenStream(L"", reader);
        if (cached == nullptr) {
            cached = stream;
            EXPECT_NE(dynamic_cast<GramTokenizer*>(stream), nullptr)
                    << "a gram analyzer must expose its tokenizer directly, with no filter on top";
        } else {
            EXPECT_EQ(stream, cached) << "reusableTokenStream must reuse one cached stream";
        }
        stream->reset(); // reusableTokenStream does not do this; the caller must
        reused.push_back(drain(stream));
    }

    for (size_t i = 0; i < rows.size(); i++) {
        reader->init(rows[i].data(), static_cast<int32_t>(rows[i].size()), false);
        std::unique_ptr<lucene::analysis::TokenStream> fresh(analyzer->tokenStream(L"", reader));
        const std::vector<std::string> tokens = drain(fresh.get());
        fresh->close();
        // Row 1 is non-ASCII and yields nothing now that the index declines that text;
        // reuse still has to agree with a fresh stream on it, which is the point here.
        EXPECT_EQ(reused[i], tokens) << "row " << i;
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

Settings whitespace_tokenizer_settings() {
    Settings settings;
    settings.set("tokenize_on_chars", "[whitespace]");
    return settings;
}

// "the" is a member of the built-in stop-word list, which is what default_word_set() resolves to
// when no wordset file is installed -- the provider can no longer be handed a word list of its own.
TEST_F(CustomAnalyzerTest, ProviderReturnsOneSharedAnalyzer) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("char_group", whitespace_tokenizer_settings());
    builder.add_token_filter_config("lowercase", {});
    auto provider = std::make_shared<CustomAnalyzerProvider>(builder.build());

    auto analyzer = provider->get_analyzer();
    ASSERT_NE(analyzer, nullptr);
    EXPECT_EQ(provider->get_analyzer(), analyzer);
}

} // namespace doris::segment_v2::inverted_index