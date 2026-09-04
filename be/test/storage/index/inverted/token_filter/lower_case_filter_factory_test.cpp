
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

#include "storage/index/inverted/token_filter/lower_case_filter_factory.h"

#include <gtest/gtest.h>

#include <vector>

#include "storage/index/inverted/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

namespace lower_case_testing {
uint64_t unicode_path_count();
void reset_unicode_path_count();
} // namespace lower_case_testing

TokenStreamPtr create_lowercase_filter(const std::string& text, Settings settings = Settings()) {
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), text.size(), false);

    KeywordTokenizerFactory tokenizer_factory;
    tokenizer_factory.initialize(Settings());
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(reader);

    LowerCaseFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);
    filter->reset();
    return filter;
}

struct ExpectedToken {
    std::string term;
    int pos_inc;
};

class ScriptedLowercaseInput final : public TokenStream {
public:
    explicit ScriptedLowercaseInput(std::vector<std::string> terms) : _terms(std::move(terms)) {}

    Token* next(Token* token) override {
        if (_next == _terms.size()) {
            return nullptr;
        }
        const auto& term = _terms[_next++];
        token->clear();
        token->setTextNoCopy(term.data(), static_cast<int32_t>(term.size()));
        token->setPositionIncrement(3);
        token->setStartOffset(7);
        token->setEndOffset(19);
        token->setType(_T("scripted"));
        return token;
    }

    void close() override {}
    void reset() override { _next = 0; }

private:
    std::vector<std::string> _terms;
    size_t _next = 0;
};

class LowerCaseFilterTest : public ::testing::Test {
protected:
    void assert_filter_output(const std::string& text, const std::vector<ExpectedToken>& expected) {
        auto filter = create_lowercase_filter(text);

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
};

TEST_F(LowerCaseFilterTest, LeavesASCIIUntouched) {
    assert_filter_output("hello world", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, ConvertsUpperCaseASCII) {
    assert_filter_output("HELLO WORLD", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, HandlesMixedCase) {
    assert_filter_output("HeLLo WoRLd", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, ASCIIBypassesUnicodeConversion) {
    lower_case_testing::reset_unicode_path_count();
    assert_filter_output("already lowercase", {{"already lowercase", 1}});
    assert_filter_output("ASCII UPPER", {{"ascii upper", 1}});
    EXPECT_EQ(lower_case_testing::unicode_path_count(), 0);

    assert_filter_output(
            "\xC3\x9C"
            "BER",
            {{"\xC3\xBC"
              "ber",
              1}});
    EXPECT_EQ(lower_case_testing::unicode_path_count(), 1);
}

TEST_F(LowerCaseFilterTest, ASCIIPreservesMetadataAndEmbeddedNul) {
    auto input = std::make_shared<ScriptedLowercaseInput>(
            std::vector<std::string> {std::string("A\0B", 3), "already lower"});
    LowerCaseFilterFactory factory;
    factory.initialize({});
    auto filter = factory.create(input);
    filter->reset();

    Token token;
    ASSERT_NE(filter->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()),
              std::string("a\0b", 3));
    EXPECT_EQ(token.getPositionIncrement(), 3);
    EXPECT_EQ(token.startOffset(), 7);
    EXPECT_EQ(token.endOffset(), 19);
    EXPECT_EQ(std::wstring(token.type()), L"scripted");

    ASSERT_NE(filter->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "already lower");
    EXPECT_EQ(token.getPositionIncrement(), 3);
    EXPECT_EQ(token.startOffset(), 7);
    EXPECT_EQ(token.endOffset(), 19);
    EXPECT_EQ(std::wstring(token.type()), L"scripted");
}

TEST_F(LowerCaseFilterTest, ConvertsUnicodeCharacters) {
    assert_filter_output("ÜBER ΜΈΓΑ", {{"über μέγα", 1}});
}

TEST_F(LowerCaseFilterTest, RetriesUnicodeExpansionWithRequiredBufferSize) {
    assert_filter_output("\xC4\xB0", {{"i\xCC\x87", 1}});
}

TEST_F(LowerCaseFilterTest, RejectsInvalidUtf8WithAnalyzerError) {
    auto input = std::make_shared<ScriptedLowercaseInput>(
            std::vector<std::string> {"VALID", std::string(1, static_cast<char>(0xFF))});
    LowerCaseFilterFactory factory;
    factory.initialize({});
    auto filter = factory.create(input);
    filter->reset();

    Token token;
    ASSERT_NE(filter->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "valid");
    try {
        filter->next(&token);
        FAIL() << "expected malformed UTF-8 to fail analysis";
    } catch (const Exception& error) {
        EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    }
}

TEST_F(LowerCaseFilterTest, HandlesNumbersAndSymbols) {
    assert_filter_output("123!@# ABC", {{"123!@# abc", 1}});
}

TEST_F(LowerCaseFilterTest, HandlesEmptyString) {
    assert_filter_output("", {});
}

TEST_F(LowerCaseFilterTest, PreservesPositionIncrements) {
    // Test with multiple tokens to verify position increments
    assert_filter_output("HELLO WORLD", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, FactoryInitialization) {
    Settings settings;
    LowerCaseFilterFactory factory;

    // Should not throw or crash
    factory.initialize(settings);

    // Verify factory creates correct filter type
    auto mockStream = std::make_shared<KeywordTokenizer>();
    auto filter = factory.create(mockStream);
    EXPECT_NE(filter, nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<LowerCaseFilter>(filter), nullptr);
}

} // namespace doris::segment_v2::inverted_index
