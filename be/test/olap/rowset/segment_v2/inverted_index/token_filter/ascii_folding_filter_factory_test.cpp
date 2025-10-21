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

TEST_F(ASCIIFoldingFilterFactoryTest, ExhaustiveUnicodeCharacters) {
    // This test includes a wide range of Unicode characters to ensure comprehensive
    // coverage of the folding logic in ASCIIFoldingFilter.
    // It is designed to test all branches of the switch statement in the fold_to_ascii function.

    // Helper structure for test cases
    struct TestCase {
        std::string description;
        std::string input;
        std::string expected;
    };

    std::vector<TestCase> test_cases = {
            {"A", "ÀàꜲÆꜴꜶꜸꜼ⒜ꜳæꜵꜷꜹꜽ", "AaAAAEAOAUAVAY(a)aaaeaoauavay"},
            {"B", "Ɓƀ⒝", "Bb(b)"},
            {"C", "Çç⒞", "Cc(c)"},
            {"D", "ÐðǄǅ⒟ȸǆ", "DdDZDz(d)dbdz"},
            {"E", "Èè⒠", "Ee(e)"},
            {"F", "Ƒƒ⒡ﬀﬃﬄﬁﬂ", "Ff(f)ffffifflfifl"},
            {"G", "Ĝĝ⒢", "Gg(g)"},
            {"H", "ĤĥǶ⒣ƕ", "HhHV(h)hv"},
            {"I", "ÌìĲ⒤ĳ", "IiIJ(i)ij"},
            {"J", "Ĵĵ⒥", "Jj(j)"},
            {"K", "Ķķ⒦", "Kk(k)"},
            {"L", "ĹĺǇỺǈ⒧ǉỻʪʫ", "LlLJLLLj(l)ljlllslz"},
            {"M", "Ɯɯ⒨", "Mm(m)"},
            {"N", "ÑñǊǋ⒩ǌ", "NnNJNj(n)nj"},
            {"O", "ÒòŒꝎȢ⒪œꝏȣ", "OoOEOOOU(o)oeooou"},
            {"P", "Ƥƥ⒫", "Pp(p)"},
            {"Q", "Ɋĸ⒬ȹ", "Qq(q)qp"},
            {"R", "Ŕŕ⒭", "Rr(r)"},
            {"S", "Śśẞ⒮ßﬆ", "SsSS(s)ssst"},
            {"T", "ŢţÞꜨ⒯ʨþʦꜩ", "TtTHTZ(t)tcthtstz"},
            {"U", "Ùù⒰ᵫ", "Uu(u)ue"},
            {"V", "ƲʋꝠ⒱ꝡ", "VvVY(v)vy"},
            {"W", "Ŵŵ⒲", "Ww(w)"},
            {"X", "Ẋᶍ⒳", "Xx(x)"},
            {"Y", "Ýý⒴", "Yy(y)"},
            {"Z", "Źź⒵", "Zz(z)"},
            {"Digits 0-9", "⁰¹⒈⑴²⒉⑵³⒊⑶⁴⒋⑷⁵⒌⑸⁶⒍⑹⁷⒎⑺⁸⒏⑻⁹⒐⑼",
             "011.(1)22.(2)33.(3)44.(4)55.(5)66.(6)77.(7)88.(8)99.(9)"},
            {"Digits 10-20", "⑩⒑⑽⑪⒒⑾⑫⒓⑿⑬⒔⒀⑭⒕⒁⑮⒖⒂⑯⒗⒃⑰⒘⒄⑱⒙⒅⑲⒚⒆⑳⒛⒇",
             "1010.(10)1111.(11)1212.(12)1313.(13)1414.(14)1515.(15)1616.(16)1717.(17)1818.(18)"
             "1919.("
             "19)2020.(20)"},
            {"Punctuation", R"(«»“”„″‶❝❞❮❯＂)", R"("""""""""""")"},
            {"Punctuation", "‘’‚‛′‵‹›❛❜＇", "'''''''''''"},
            {"Punctuation", "‐‑‒–—⁻₋－", "--------"},
            {"Punctuation", "⁅❲［", "[[["},
            {"Punctuation", "⁆❳］", "]]]"},
            {"Punctuation", "⁽₍❨❪（", "((((("},
            {"Punctuation", "⸨", "(("},
            {"Punctuation", "⁾₎❩❫）", ")))))"},
            {"Punctuation", "⸩", "))"},
            {"Punctuation", "❬❰＜", "<<<"},
            {"Punctuation", "❭❱＞", ">>>"},
            {"Punctuation", "❴｛", "{{"},
            {"Punctuation", "❵｝", "}}"},
            {"Punctuation", "⁺₊＋", "+++"},
            {"Punctuation", "⁼₌＝", "==="},
            {"Punctuation", "！", "!"},
            {"Punctuation", "‼", "!!"},
            {"Punctuation", "⁉", "!?"},
            {"Punctuation", "＃", "#"},
            {"Punctuation", "＄", "$"},
            {"Punctuation", "⁒％", "%%"},
            {"Punctuation", "＆", "&"},
            {"Punctuation", "⁎＊", "**"},
            {"Punctuation", "，", ","},
            {"Punctuation", "．", "."},
            {"Punctuation", "⁄／", "//"},
            {"Punctuation", "：", ":"},
            {"Punctuation", "⁏；", ";;"},
            {"Punctuation", "？", "?"},
            {"Punctuation", "⁇", "??"},
            {"Punctuation", "⁈", "?!"},
            {"Punctuation", "＠", "@"},
            {"Punctuation", "＼", "\\"},
            {"Punctuation", "‸＾", "^^"},
            {"Punctuation", "＿", "_"},
            {"Punctuation", "⁓～", "~~"},
            {"Default Case", "中", "中"},
            {"Invalid UTF-8", "\x80", "\x80"},
    };

    for (const auto& test : test_cases) {
        assert_filter_output(test.input, {{test.expected, 1}});
    }
}

} // namespace doris::segment_v2::inverted_index