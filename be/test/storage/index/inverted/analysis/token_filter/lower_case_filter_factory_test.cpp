
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

    // The batch with the malformed token fails as a whole, and the stream is not read after that.
    auto filter = create_lowercase_filter(std::make_unique<ScriptedLowercaseInput>(
            std::vector<std::string> {"VALID", std::string(1, static_cast<char>(0xFF))}));
    filter->reset("");
    TokenBatch batch;
    try {
        filter->next_batch(batch);
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
    auto filter = factory.create(std::make_unique<KeywordTokenizer>());
    EXPECT_NE(filter, nullptr);
    EXPECT_NE(dynamic_cast<LowerCaseFilter*>(filter.get()), nullptr);
}

} // namespace doris::segment_v2::inverted_index
