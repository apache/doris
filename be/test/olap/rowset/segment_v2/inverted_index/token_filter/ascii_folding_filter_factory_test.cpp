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

#include "olap/rowset/segment_v2/inverted_index/token_filter/ascii_folding_filter_factory.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

TokenStreamPtr create_filter(const std::string& text, Settings token_filter_settings) {
    static lucene::util::SStringReader<char> reader;
    reader.init(text.data(), text.size(), false);

    Settings settings;
    KeywordTokenizerFactory tokenizer_factory;
    tokenizer_factory.initialize(settings);
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(&reader);

    ASCIIFoldingFilterFactory token_filter_factory;
    token_filter_factory.initialize(token_filter_settings);
    auto token_filter = token_filter_factory.create(tokenizer);
    token_filter->reset();
    return token_filter;
}

struct ExpectedToken {
    std::string term;
    int pos_inc;
};

class ASCIIFoldingFilterFactoryTest : public ::testing::Test {
protected:
    void assert_filter_output(const std::string& text, const std::vector<ExpectedToken>& expected,
                              bool preserve_original = false) {
        Settings settings;
        if (preserve_original) {
            settings.set("preserve_original", "true");
        } else {
            settings.set("preserve_original", "false");
        }
        auto filter = create_filter(text, settings);

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

TEST_F(ASCIIFoldingFilterFactoryTest, LeavesASCIIUntouched) {
    assert_filter_output("hello world", {{"hello world", 1}});
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesAccentedCharacters) {
    assert_filter_output("éclair", {{"eclair", 1}});
    assert_filter_output("café", {{"cafe", 1}});
    assert_filter_output("naïve", {{"naive", 1}});
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesAccentedCharactersWithOriginal) {
    assert_filter_output("éclair", {{"eclair", 1}, {"éclair", 0}}, true);
    assert_filter_output("café", {{"cafe", 1}, {"café", 0}}, true);
    assert_filter_output("naïve", {{"naive", 1}, {"naïve", 0}}, true);
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesUmlauts) {
    assert_filter_output("über", {{"uber", 1}});
    assert_filter_output("Häagen-Dazs", {{"Haagen-Dazs", 1}});
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesUmlautsWithOriginal) {
    assert_filter_output("über", {{"uber", 1}, {"über", 0}}, true);
    assert_filter_output("Häagen-Dazs", {{"Haagen-Dazs", 1}, {"Häagen-Dazs", 0}}, true);
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesScandinavianCharacters) {
    assert_filter_output("fjörd", {{"fjord", 1}});
    assert_filter_output("Ångström", {{"Angstrom", 1}});
    assert_filter_output("Æsir", {{"AEsir", 1}});
    assert_filter_output("Øresund", {{"Oresund", 1}});
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesMixedInput) {
    assert_filter_output("Héllø Wörld 123", {{"Hello World 123", 1}});
    assert_filter_output("Mötley Crüe", {{"Motley Crue", 1}});
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesMixedInputWithOriginal) {
    assert_filter_output("Héllø Wörld 123", {{"Hello World 123", 1}, {"Héllø Wörld 123", 0}}, true);
    assert_filter_output("Mötley Crüe", {{"Motley Crue", 1}, {"Mötley Crüe", 0}}, true);
}

TEST_F(ASCIIFoldingFilterFactoryTest, HandlesEmptyString) {
    assert_filter_output("", {});
}

TEST_F(ASCIIFoldingFilterFactoryTest, PositionIncrementsCorrect) {
    assert_filter_output("á b c", {{"a b c", 1}});
    assert_filter_output("á b c", {{"a b c", 1}, {"á b c", 0}}, true);
}

} // namespace doris::segment_v2::inverted_index