
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

#include "storage/index/inverted/analysis/token_filter/lower_case_filter_factory.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

namespace lower_case_testing {
uint64_t unicode_path_count();
void reset_unicode_path_count();
} // namespace lower_case_testing

std::unique_ptr<TextTokenStream> create_lowercase_filter(
        std::unique_ptr<TextTokenStream> input = nullptr, Settings settings = Settings()) {
    if (input == nullptr) {
        KeywordTokenizerFactory tokenizer_factory;
        tokenizer_factory.initialize(Settings());
        input = tokenizer_factory.create();
    }

    LowerCaseFilterFactory filter_factory;
    filter_factory.initialize(settings);
    return filter_factory.create(std::move(input));
}

struct ExpectedToken {
    std::string term;
    int pos_inc;
};

// Hands out the scripted terms, each with position increment 3, whatever the text.
class ScriptedLowercaseInput final : public TextTokenizer {
public:
    explicit ScriptedLowercaseInput(std::vector<std::string> terms) : _terms(std::move(terms)) {}

    void reset(std::string_view /*text*/) override { _next = 0; }

    bool next_batch(TokenBatch& batch) override {
        batch.clear();
        if (_next == _terms.size()) {
            return false;
        }
        batch.push(_terms[_next++], 3);
        return true;
    }

    size_t reserved_bytes() const override { return 0; }

private:
    std::vector<std::string> _terms;
    size_t _next = 0;
};

class LowerCaseFilterTest : public ::testing::Test {
protected:
    void assert_filter_output(const std::string& text, const std::vector<ExpectedToken>& expected) {
        auto filter = create_lowercase_filter();

        size_t i = 0;
        for (const CollectedToken& t : collect_checked(*filter, text)) {
            const std::string& term = t.term;
            EXPECT_EQ(term, expected[i].term) << "Term mismatch at index " << i;
            EXPECT_EQ(t.position_increment, expected[i].pos_inc)
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

TEST_F(LowerCaseFilterTest, ASCIIPreservesIncrementAndEmbeddedNul) {
    auto filter = create_lowercase_filter(std::make_unique<ScriptedLowercaseInput>(
            std::vector<std::string> {std::string("A\0B", 3), "already lower"}));

    const std::vector<CollectedToken> tokens = collect_checked(*filter, "");
    ASSERT_GE(tokens.size(), 1);
    EXPECT_EQ(tokens[0].term, std::string("a\0b", 3));
    EXPECT_EQ(tokens[0].position_increment, 3);

    ASSERT_GE(tokens.size(), 2);
    EXPECT_EQ(tokens[1].term, "already lower");
    EXPECT_EQ(tokens[1].position_increment, 3);
}

TEST_F(LowerCaseFilterTest, ConvertsUnicodeCharacters) {
    assert_filter_output("ÜBER ΜΈΓΑ", {{"über μέγα", 1}});
}

TEST_F(LowerCaseFilterTest, RetriesUnicodeExpansionWithRequiredBufferSize) {
    assert_filter_output("\xC4\xB0", {{"i\xCC\x87", 1}});
}

TEST_F(LowerCaseFilterTest, RejectsInvalidUtf8WithAnalyzerError) {
    auto valid = create_lowercase_filter(
            std::make_unique<ScriptedLowercaseInput>(std::vector<std::string> {"VALID"}));
    const std::vector<std::string> valid_terms = collect_terms(*valid, "");
    ASSERT_EQ(valid_terms.size(), 1);
    EXPECT_EQ(valid_terms[0], "valid");

    // The input hands out one token per batch, and the filter rewrites the batches of its input:
    // the first comes out lowercased, the second fails, and the stream is not read after that.
    auto filter = create_lowercase_filter(std::make_unique<ScriptedLowercaseInput>(
            std::vector<std::string> {"VALID", std::string(1, static_cast<char>(0xFF))}));
    filter->reset("");
    TokenBatch batch;
    ASSERT_TRUE(filter->next_batch(batch));
    ASSERT_EQ(batch.size(), 1);
    EXPECT_EQ(batch.tokens()[0].term(), "valid");
    try {
        filter->next_batch(batch);
        FAIL() << "expected malformed UTF-8 to fail analysis";
    } catch (const Exception& error) {
        EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    }
}

namespace {

// Hands out all its terms in one batch, as views of the strings it keeps, whatever the text.
class OneBatchLowercaseInput final : public TextTokenizer {
public:
    explicit OneBatchLowercaseInput(std::vector<std::string> terms) : _terms(std::move(terms)) {}

    void reset(std::string_view /*text*/) override { _done = false; }

    bool next_batch(TokenBatch& batch) override {
        batch.clear();
        if (_done) {
            return false;
        }
        for (const std::string& term : _terms) {
            batch.push(term, 1);
        }
        _done = true;
        return true;
    }

    size_t reserved_bytes() const override { return 0; }

    const std::vector<std::string>& terms() const { return _terms; }

private:
    std::vector<std::string> _terms;
    bool _done = true;
};

// The terms the lowercase filter makes of `terms` in one batch, each with whether it still views
// the input.
std::vector<std::pair<std::string, bool>> lower_in_one_batch(
        const std::vector<std::string>& terms) {
    auto input = std::make_unique<OneBatchLowercaseInput>(terms);
    const OneBatchLowercaseInput& source = *input;
    auto filter = create_lowercase_filter(std::move(input));
    filter->reset("");
    TokenBatch batch;
    EXPECT_TRUE(filter->next_batch(batch));
    std::vector<std::pair<std::string, bool>> lowered;
    for (size_t i = 0; i < batch.size(); ++i) {
        const TokenRef& token = batch.tokens()[i];
        EXPECT_EQ(token.position_increment, 1);
        lowered.emplace_back(token.term(), token.data == source.terms()[i].data());
    }
    EXPECT_FALSE(filter->next_batch(batch));
    return lowered;
}

} // namespace

// The four classes of term: a term left as it is keeps its view of the input, a lowercased one
// points into the batch, and only the last class goes through validation and ICU.
TEST_F(LowerCaseFilterTest, HandlesEachClassOfTerm) {
    const std::vector<std::string> terms = {
            // Left as they are: ASCII without a capital, the empty term, and one character of
            // three bytes led by E3..E9: 中, U+3000 and U+9FFF.
            "abc", "", "\xE4\xB8\xAD", "\xE3\x80\x80", "\xE9\xBF\xBF",
            // Lowercased byte by byte: ASCII with a capital.
            "aBC", std::string("A\0B", 3),
            // Validated and lowercased by ICU: Über, two CJK characters, the leads E2 (U+2FFF)
            // and EA (U+A000) next to the range, and a capital of three bytes, Ⓐ (U+24B6).
            "\xC3\x9C"
            "ber",
            "\xE4\xB8\xAD\xE6\x96\x87", "\xE2\xBF\xBF", "\xEA\x80\x80", "\xE2\x92\xB6"};
    const std::vector<std::pair<std::string, bool>> expected = {{"abc", true},
                                                                {"", true},
                                                                {"\xE4\xB8\xAD", true},
                                                                {"\xE3\x80\x80", true},
                                                                {"\xE9\xBF\xBF", true},
                                                                {"abc", false},
                                                                {std::string("a\0b", 3), false},
                                                                {"\xC3\xBC"
                                                                 "ber",
                                                                 false},
                                                                {"\xE4\xB8\xAD\xE6\x96\x87", false},
                                                                {"\xE2\xBF\xBF", false},
                                                                {"\xEA\x80\x80", false},
                                                                {"\xE2\x93\x90", false}};
    lower_case_testing::reset_unicode_path_count();
    EXPECT_EQ(lower_in_one_batch(terms), expected);
    EXPECT_EQ(lower_case_testing::unicode_path_count(), 5);
}

// A lead byte of E3..E9 needs two continuation bytes to be left as it is; without them the term is
// validated and fails, as it did when every term that is not ASCII was validated.
TEST_F(LowerCaseFilterTest, RejectsALeadWithoutContinuationBytes) {
    for (const std::string& text :
         {std::string("\xE4"
                      "AA"),
          std::string("\xE4\xB8"
                      "A"),
          std::string("\xE4\xB8"), std::string("\xE4\xB8\xAD\xB8"), std::string("\xE9\xBF\xFF")}) {
        SCOPED_TRACE(text);
        auto filter = create_lowercase_filter();
        try {
            collect_checked(*filter, text);
            FAIL() << "expected malformed UTF-8 to fail analysis";
        } catch (const Exception& error) {
            EXPECT_EQ(error.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
        }
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
    auto filter = factory.create(std::make_unique<KeywordTokenizer>());
    EXPECT_NE(filter, nullptr);
    EXPECT_NE(dynamic_cast<LowerCaseFilter*>(filter.get()), nullptr);
}

} // namespace doris::segment_v2::inverted_index
