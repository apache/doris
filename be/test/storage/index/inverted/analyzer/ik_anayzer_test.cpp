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

#include <gtest/gtest.h>

#include <fstream>
#include <limits>
#include <memory>
#include <sstream>

#include "core/arena.h"
#include "storage/index/inverted/analyzer/ik/IKAnalyzer.h"
#include "storage/index/inverted/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analyzer/ik/core/AnalyzeContext.h"
#include "storage/index/inverted/analyzer/ik/core/IKSegmenter.h"
#include "storage/index/inverted/analyzer/ik/core/Lexeme.h"
using namespace lucene::analysis;

namespace doris::segment_v2 {

class IKTokenizerTest : public ::testing::Test {
protected:
    void tokenize(const std::string& s, std::vector<std::string>& datas, bool isSmart) {
        try {
            IKAnalyzer analyzer;
            analyzer.initDict("./be/dict/ik");
            analyzer.setMode(isSmart);
            analyzer.set_lowercase(true);

            lucene::util::SStringReader<char> reader;
            reader.init(s.data(), s.size(), false);

            std::unique_ptr<IKTokenizer> tokenizer;
            tokenizer.reset((IKTokenizer*)analyzer.tokenStream(L"", &reader));

            Token t;
            while (tokenizer->next(&t)) {
                std::string term(t.termBuffer<char>(), t.termLength<char>());
                datas.emplace_back(term);
            }
        } catch (CLuceneError& e) {
            std::cout << e.what() << std::endl;
            throw;
        }
    }

    /**
     * Token information class, contains token text and type
     */
    struct TokenInfo {
        std::string text;
        Lexeme::Type type;

        TokenInfo(const std::string& text, Lexeme::Type type) : text(text), type(type) {}
    };

    /**
     * Simplified function to get tokens with type information
     * Reuses the same configuration as tokenize()
     */
    void getTokensWithType(const std::string& s, std::vector<TokenInfo>& tokenInfos,
                           bool isSmart = false) {
        std::shared_ptr<Configuration> config = std::make_shared<Configuration>(isSmart, true);
        config->setDictPath("./be/dict/ik");
        IKSegmenter segmenter(config);

        lucene::util::SStringReader<char> reader;
        reader.init(s.data(), s.size(), false);
        segmenter.reset(&reader);

        Lexeme lexeme;
        while (segmenter.next(lexeme)) {
            tokenInfos.emplace_back(lexeme.getText(), lexeme.getType());
        }
    }

    /**
     * Helper function to check if a token with specific type exists
     */
    bool hasTokenWithType(const std::vector<TokenInfo>& tokens, const std::string& text,
                          Lexeme::Type type) {
        for (const auto& token : tokens) {
            if (token.text == text && token.type == type) {
                return true;
            }
        }
        return false;
    }

    /**
     * Helper function to check if a token exists (any type)
     */
    bool hasToken(const std::vector<TokenInfo>& tokens, const std::string& text) {
        for (const auto& token : tokens) {
            if (token.text == text) {
                return true;
            }
        }
        return false;
    }

    // Helper method to create a temporary dictionary file for testing
    std::string createTempDictFile(const std::string& content) {
        std::string temp_file = "/tmp/temp_dict_" + std::to_string(rand()) + ".dic";
        std::ofstream out(temp_file);
        out << content;
        out.close();
        return temp_file;
    }

    // Flag to indicate if the current test is an exception test
    bool is_exception_test = true;
};

// Test for Dictionary exception handling
TEST_F(IKTokenizerTest, TestDictionaryExceptionHandling) {
    // Test 1: Load non-existent path
    {
        Configuration cfg;
        cfg.setDictPath("/non_existent_path");
        cfg.setMainDictFile("main.dic");
        cfg.setQuantifierDictFile("quantifier.dic");
        cfg.setStopWordDictFile("stopword.dic");

        // Expect an exception
        ASSERT_THROW({ Dictionary::initial(cfg, false); }, CLuceneError);

        // Singleton should be nullptr, getSingleton() will throw an exception
        ASSERT_NO_THROW({ Dictionary::getSingleton(); });
    }

    // Test 2: Use reload method with invalid path
    {
        Dictionary* dict = Dictionary::getSingleton();
        // Set path to invalid path
        dict->getConfiguration()->setDictPath("/non_existent_path");

        ASSERT_THROW({ Dictionary::reload(); }, CLuceneError);
    }

    // Test 3: Initialize with valid dictionary path
    {
        Dictionary::getSingleton()->getConfiguration()->setDictPath("./be/dict/ik");

        // Expect no exception
        ASSERT_NO_THROW({ Dictionary::reload(); });

        // Singleton should be successfully created
        Dictionary* dict = nullptr;
        ASSERT_NO_THROW({ dict = Dictionary::getSingleton(); });
        ASSERT_NE(dict, nullptr);
    }
}

TEST_F(IKTokenizerTest, TestIKTokenizer) {
    std::vector<std::string> datas;

    // Test with max_word mode
    std::string Text1 = "中华人民共和国国歌";
    tokenize(Text1, datas, false);
    ASSERT_EQ(datas.size(), 10);
    datas.clear();

    // Test with smart mode
    tokenize(Text1, datas, true);
    ASSERT_EQ(datas.size(), 2);
    datas.clear();

    std::string Text2 = "人民可以得到更多实惠";
    tokenize(Text2, datas, false);
    ASSERT_EQ(datas.size(), 5);
    datas.clear();

    // Test with smart mode
    tokenize(Text2, datas, true);
    ASSERT_EQ(datas.size(), 5);
    datas.clear();

    std::string Text3 = "中国人民银行";
    tokenize(Text3, datas, false);
    ASSERT_EQ(datas.size(), 8);
    datas.clear();

    // Test with smart mode
    tokenize(Text3, datas, true);
    ASSERT_EQ(datas.size(), 1);
    datas.clear();
}

TEST_F(IKTokenizerTest, TestIKRareTokenizer) {
    std::vector<std::string> datas;

    // Test with rare characters
    std::string Text = "菩𪜮龟龙麟凤凤";
    tokenize(Text, datas, true);
    ASSERT_EQ(datas.size(), 4);
    std::vector<std::string> result = {"菩", "𪜮", "龟龙麟凤", "凤"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(IKTokenizerTest, TestIKSmartModeTokenizer) {
    std::vector<std::string> datas;

    // Test smart mode tokenization
    std::string Text1 = "我来到北京清华大学";
    tokenize(Text1, datas, true);
    ASSERT_EQ(datas.size(), 4);
    std::vector<std::string> result1 = {"我", "来到", "北京", "清华大学"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result1[i]);
    }
    datas.clear();

    // Test another example with smart mode
    std::string Text2 = "中国的科技发展在世界上处于领先";
    tokenize(Text2, datas, true);
    ASSERT_EQ(datas.size(), 7);
    std::vector<std::string> result2 = {"中国", "的", "科技", "发展", "在世界上", "处于", "领先"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result2[i]);
    }
    datas.clear();
}

TEST_F(IKTokenizerTest, TestIKMaxWordModeTokenizer) {
    std::vector<std::string> datas;

    // Test max word mode tokenization
    std::string Text1 = "我来到北京清华大学";
    tokenize(Text1, datas, false);
    ASSERT_EQ(datas.size(), 6);
    std::vector<std::string> result1 = {"我", "来到", "北京", "清华大学", "清华", "大学"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result1[i]);
    }
    datas.clear();

    // Test another example with max word mode
    std::string Text2 = "中国的科技发展在世界上处于领先";
    tokenize(Text2, datas, false);
    ASSERT_EQ(datas.size(), 11);
    std::vector<std::string> result2 = {"中国",   "的",   "科技", "发展", "在世界上", "在世",
                                        "世界上", "世界", "上",   "处于", "领先"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result2[i]);
    }
    datas.clear();
}

TEST_F(IKTokenizerTest, TestEmptyInput) {
    std::vector<std::string> datas;
    // Test with empty input
    std::string emptyText = "";
    tokenize(emptyText, datas, true);
    ASSERT_EQ(datas.size(), 0);
}

TEST_F(IKTokenizerTest, TestSingleByteInput) {
    std::vector<std::string> datas;
    // Test with single byte input
    std::string singleByteText = "b";
    tokenize(singleByteText, datas, true);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "b");
}

TEST_F(IKTokenizerTest, TestLargeInput) {
    std::vector<std::string> datas;
    // Test with large input
    std::string largeText;
    for (int i = 0; i < 1000; i++) {
        largeText += "中国的科技发展在世界上处于领先";
    }
    tokenize(largeText, datas, true);
    ASSERT_EQ(datas.size(), 7000);
}

TEST_F(IKTokenizerTest, TestBufferExhaustCritical) {
    std::vector<std::string> datas;
    // Test with buffer exhaustion critical case
    std::string criticalText;
    for (int i = 0; i < 95; i++) {
        criticalText += "的";
    }
    tokenize(criticalText, datas, true);
    ASSERT_EQ(datas.size(), 95);
}

TEST_F(IKTokenizerTest, TestMixedLanguageInput) {
    std::vector<std::string> datas;
    // Test with mixed language input
    std::string mixedText =
            "Doris是一个现代化的MPP分析型数据库，可以处理PB级别的数据，支持SQL92和SQL99。";
    tokenize(mixedText, datas, true);

    std::vector<std::string> expectedTokens = {
            "doris", "是", "一个", "现代化", "的",   "mpp",  "分析",  "型", "数据库", "可以",
            "处理",  "pb", "级",   "别的",   "数据", "支持", "sql92", "和", "sql99"};
    ASSERT_EQ(datas.size(), expectedTokens.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedTokens[i]);
    }
}

TEST_F(IKTokenizerTest, TestSpecialCharacters) {
    std::vector<std::string> datas;
    // Test with special characters
    std::string specialText = "😊🚀👍测试特殊符号：@#¥%……&*（）";
    tokenize(specialText, datas, true);
    ASSERT_EQ(datas.size(), 5);
    std::vector<std::string> expectedTokens = {"😊", "🚀", "👍", "测试", "特殊符号"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedTokens[i]);
    }
}

TEST_F(IKTokenizerTest, TestBufferBoundaryWithSpace) {
    std::vector<std::string> datas;

    // Test with exact buffer boundary
    std::string exactText;
    int charCount = 4096 / 3;
    for (int i = 0; i < charCount; i++) {
        exactText += "中";
    }
    exactText += " ";

    tokenize(exactText, datas, true);
    ASSERT_EQ(datas.size(), charCount);
    datas.clear();

    // Test with buffer boundary overflow
    std::string overText;
    charCount = 4096 / 3 + 1;
    for (int i = 0; i < charCount; i++) {
        overText += "中";
    }
    overText += " ";

    tokenize(overText, datas, true);
    ASSERT_EQ(datas.size(), charCount);
    datas.clear();

    // Test with multiple spaces at buffer boundary
    std::string multiSpaceText;
    charCount = 4096 / 3 - 3;
    for (int i = 0; i < charCount; i++) {
        multiSpaceText += "中";
    }
    multiSpaceText += "   ";

    tokenize(multiSpaceText, datas, true);
    ASSERT_EQ(datas.size(), charCount);
    datas.clear();

    // Test with spaces around buffer boundary
    std::string spaceAroundBoundaryText;
    charCount = 4096 / 3 - 2;
    for (int i = 0; i < charCount / 2; i++) {
        spaceAroundBoundaryText += "中";
    }
    spaceAroundBoundaryText += " ";
    for (int i = 0; i < charCount / 2; i++) {
        spaceAroundBoundaryText += "中";
    }
    spaceAroundBoundaryText += "  ";

    tokenize(spaceAroundBoundaryText, datas, true);
    ASSERT_EQ(datas.size(), charCount - 1);
}

TEST_F(IKTokenizerTest, TestChineseCharacterAtBufferBoundary) {
    std::vector<std::string> datas;

    std::string boundaryText;
    // Test with a complete Chinese character cut at the first byte
    int completeChars = 4096 / 3;
    for (int i = 0; i < completeChars; i++) {
        boundaryText += "中";
    }

    boundaryText += "国";

    tokenize(boundaryText, datas, true);
    ASSERT_EQ(datas.size(), completeChars + 1);
    ASSERT_EQ(datas[datas.size() - 1], "国");
    datas.clear();
    boundaryText.clear();
    // Test with a complete Chinese character cut at the second byte
    boundaryText += "  ";

    for (int i = 0; i < completeChars; i++) {
        boundaryText += "中";
    }

    boundaryText += "国";

    tokenize(boundaryText, datas, true);
    ASSERT_EQ(datas.size(), completeChars);
    ASSERT_EQ(datas[datas.size() - 1], "中国");

    datas.clear();
    boundaryText.clear();
}

TEST_F(IKTokenizerTest, TestLongTextCompareWithJava) {
    std::vector<std::string> datas;

    // Test with long text and compare results with Java implementation
    std::string longText =
            "随着人工智能技术的快速发展，深度学习、机器学习和神经网络等技术已经在各个领域得到了广泛"
            "应用。"
            "从语音识别、图像处理到自然语言处理，人工智能正在改变我们的生活方式和工作方式。"
            "在医疗领域，AI辅助诊断系统可以帮助医生更准确地识别疾病；在金融领域，智能算法可以预测市"
            "场趋势和风险；"
            "在教育领域，个性化学习平台可以根据学生的学习情况提供定制化的教学内容。"
            "然而，随着AI技术的普及，也带来了一系列的伦理和隐私问题。如何确保AI系统的公平性和透明度"
            "，"
            "如何保护用户数据的安全，如何防止AI被滥用，这些都是我们需要思考的问题。"
            "此外，AI的发展也可能对就业市场产生影响，一些传统工作可能会被自动化系统取代，"
            "但同时也会创造出新的工作岗位和机会。因此，我们需要积极适应这一变化，"
            "提升自己的技能和知识，以便在AI时代保持竞争力。"
            "总的来说，人工智能是一把双刃剑，它既带来了巨大的机遇，也带来了挑战。"
            "我们需要理性看待AI的发展，既要充分利用它的优势，也要警惕可能的风险，"
            "共同推动AI技术向着更加健康、可持续的方向发展。";

    // Repeat 4 times to create a long text
    int i = 0;
    while (i < 4) {
        longText += longText;
        i++;
    }
    // Test with smart mode
    tokenize(longText, datas, true);

    ASSERT_EQ(datas.size(), 3312);

    // Compare first 20 tokens with Java implementation
    std::vector<std::string> javaFirst20Results = {
            "随着",     "人工智能技术", "的",   "快速",     "发展", "深度", "学习",
            "机器",     "学习",         "和",   "神经网络", "等",   "技术", "已经在",
            "各个领域", "得",           "到了", "广泛应用", "从",   "语音"};
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[i], javaFirst20Results[i]);
    }

    // Compare last 20 tokens with Java implementation
    std::vector<std::string> javaLast20Results = {
            "发展", "方向", "的",   "持续", "可",   "健康", "更加", "向着", "技术", "ai",
            "推动", "共同", "风险", "的",   "可能", "警惕", "也要", "优势", "的",   "它"};
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[datas.size() - i - 1], javaLast20Results[i]);
    }

    // Test with max_word mode
    datas.clear();
    javaFirst20Results = {"随着",     "人工智能技术", "人工智能", "人工", "智能", "技术",  "的",
                          "快速",     "发展",         "深度",     "学习", "机器", "学习",  "和",
                          "神经网络", "神经",         "网络",     "等",   "技术", "已经在"};
    javaLast20Results = {"发展", "方向", "的",   "持续", "可",   "健康", "更加",
                         "向着", "技术", "ai",   "推动", "共同", "风险", "的",
                         "可能", "警惕", "也要", "优势", "的",   "用它"};

    tokenize(longText, datas, false);
    ASSERT_EQ(datas.size(), 4336);

    // Compare first 20 tokens with Java implementation
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[i], javaFirst20Results[i]);
    }

    // Compare last 20 tokens with Java implementation
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[datas.size() - i - 1], javaLast20Results[i]);
    }
}

TEST_F(IKTokenizerTest, TestFullWidthCharacters) {
    std::vector<std::string> datas;

    // test full width numbers
    std::string fullWidthNumbersText = "４ ３ ２";
    tokenize(fullWidthNumbersText, datas, true);
    std::vector<std::string> expectedNumbers = {"4", "3", "2"}; // half width numbers
    ASSERT_EQ(datas.size(), expectedNumbers.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedNumbers[i]);
    }
    datas.clear();

    fullWidthNumbersText = "４３２";
    tokenize(fullWidthNumbersText, datas, false);
    expectedNumbers = {"432"};
    ASSERT_EQ(datas.size(), expectedNumbers.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedNumbers[i]);
    }
    datas.clear();

    // test full width currency symbol
    std::string currencyText = "￥";
    tokenize(currencyText, datas, false);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "￥");
    datas.clear();

    // test full width symbol in word
    std::string mixedText = "High＆Low";
    tokenize(mixedText, datas, false);
    std::vector<std::string> expectedMixed = {"high&low", "high", "low"};
    ASSERT_EQ(datas.size(), expectedMixed.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedMixed[i]);
    }
    datas.clear();

    // test special separator
    std::string specialSeparatorText = "1･2";
    tokenize(specialSeparatorText, datas, false);
    std::vector<std::string> expectedSeparator = {"1", "･", "2"};
    ASSERT_EQ(datas.size(), expectedSeparator.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedSeparator[i]);
    }
    datas.clear();

    // test special character
    std::string specialCharText = "﨑";
    tokenize(specialCharText, datas, false);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "﨑");
    datas.clear();
}

TEST_F(IKTokenizerTest, TestEmojiAndSpecialCharacters) {
    std::vector<std::string> datas;

    // test emoji
    std::string emojiText = "🐼";
    tokenize(emojiText, datas, false);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "🐼");
    datas.clear();

    std::string emojiText2 = "🝢";
    tokenize(emojiText2, datas, false);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "🝢");
    datas.clear();

    // test special latin character
    std::string specialLatinText1 = "abcşabc";
    tokenize(specialLatinText1, datas, false);
    ASSERT_EQ(datas.size(), 2);
    ASSERT_EQ(datas[0], "abc");
    ASSERT_EQ(datas[1], "abc");
    datas.clear();

    std::string specialLatinText2 = "abcīabc";
    tokenize(specialLatinText2, datas, false);
    ASSERT_EQ(datas.size(), 2);
    ASSERT_EQ(datas[0], "abc");
    ASSERT_EQ(datas[1], "abc");
    datas.clear();

    std::string specialLatinText3 = "celebrity…get";
    tokenize(specialLatinText3, datas, false);
    std::vector<std::string> expectedEllipsis = {"celebrity", "get"};
    ASSERT_EQ(datas.size(), expectedEllipsis.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedEllipsis[i]);
    }
    datas.clear();

    // test mixed alphabet word
    std::string mixedAlphabetText1 = "Hulyaiрole";
    tokenize(mixedAlphabetText1, datas, false);
    ASSERT_EQ(datas.size(), 2);
    ASSERT_EQ(datas[0], "hulyai");
    ASSERT_EQ(datas[1], "ole");
    datas.clear();

    std::string mixedAlphabetText2 = "Nisa Aşgabat";
    tokenize(mixedAlphabetText2, datas, false);
    std::vector<std::string> expectedName = {"nisa", "gabat"};
    ASSERT_EQ(datas.size(), expectedName.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedName[i]);
    }
    datas.clear();

    // test special connector
    std::string specialConnectorText = "alـameer";
    tokenize(specialConnectorText, datas, false);
    ASSERT_EQ(datas.size(), 2);
    ASSERT_EQ(datas[0], "al");
    ASSERT_EQ(datas[1], "ameer");
    datas.clear();

    // test rare unicode character
    std::string rareUnicodeText1 = "𐓚";
    tokenize(rareUnicodeText1, datas, false);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "𐓚");
    datas.clear();

    std::string rareUnicodeText2 = "𑪱";
    tokenize(rareUnicodeText2, datas, false);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "𑪱");
    datas.clear();

    std::string rareUnicodeText3 = "𐴗";
    tokenize(rareUnicodeText3, datas, false);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "𐴗");
    datas.clear();
}

TEST_F(IKTokenizerTest, TestCNQuantifierSegmenter) {
    std::vector<TokenInfo> tokenInfos;

    // Test case: "2023年人才" - core test for the fix
    std::string testText = "2023年人才";
    getTokensWithType(testText, tokenInfos, false);

    // Verify basic tokens exist
    ASSERT_TRUE(hasToken(tokenInfos, "2023")) << "Should contain token '2023'";
    ASSERT_TRUE(hasToken(tokenInfos, "年")) << "Should contain token '年'";
    ASSERT_TRUE(hasToken(tokenInfos, "人才")) << "Should contain token '人才'";

    // Core assertion: "人" should NOT be segmented as COUNT type separately
    ASSERT_FALSE(hasTokenWithType(tokenInfos, "人", Lexeme::Type::Count))
            << "'人' should not be segmented as COUNT type when not preceded by number";

    // "年" should be COUNT type
    ASSERT_TRUE(hasTokenWithType(tokenInfos, "年", Lexeme::Type::Count))
            << "'年' should be COUNT type";
}

// Test the exception handling capabilities of the IKTokenizer and AnalyzeContext
TEST_F(IKTokenizerTest, TestExceptionHandling) {
    // Common mock reader class for testing exception handling
    class MockReader : public lucene::util::Reader {
    public:
        enum class ExceptionType { NONE, RUNTIME_ERROR, LENGTH_ERROR, OUT_OF_MEMORY };

        MockReader(ExceptionType type = ExceptionType::NONE, const std::string& errorMsg = "")
                : exceptionType(type), message(errorMsg), returnSize(0) {}

        ~MockReader() override {}

        int32_t read(const void** start, int32_t min, int32_t max) override {
            throwIfNeeded();
            return 0;
        }

        int32_t readCopy(void* start, int32_t off, int32_t len) override {
            throwIfNeeded();

            int32_t safeSize = std::min(returnSize, len);

            if (safeSize > 0) {
                memset(start, 'A', safeSize);
            }

            return safeSize;
        }

        int64_t skip(int64_t ntoskip) override {
            throwIfNeeded();
            return 0;
        }

        int64_t position() override { return 0; }

        size_t size() override { return std::numeric_limits<size_t>::max(); }

        void setReturnSize(int32_t size) { returnSize = size; }

    private:
        ExceptionType exceptionType;
        std::string message;
        int32_t returnSize;

        void throwIfNeeded() {
            switch (exceptionType) {
            case ExceptionType::RUNTIME_ERROR:
                throw std::runtime_error(message.empty() ? "Simulated runtime error" : message);
            case ExceptionType::LENGTH_ERROR:
                throw std::length_error(message.empty() ? "basic_string::_M_create" : message);
            case ExceptionType::OUT_OF_MEMORY:
                throw std::bad_alloc();
            case ExceptionType::NONE:
            default:
                break;
            }
        }
    };

    // PART 1: Test IKTokenizer::reset exception handling
    {
        // Set up analyzer and tokenizer
        IKAnalyzer analyzer;
        analyzer.initDict("./be/dict/ik");

        // Initialize with a valid reader
        lucene::util::SStringReader<char> validReader;
        validReader.init("Test text", 9, false);
        std::unique_ptr<IKTokenizer> tokenizer;
        tokenizer.reset((IKTokenizer*)analyzer.tokenStream(L"", &validReader));

        // Test case 1: Reader throwing runtime error
        MockReader runtimeErrorReader(MockReader::ExceptionType::RUNTIME_ERROR);
        // This may throw different exceptions depending on implementation details
        ASSERT_THROW({ tokenizer->reset(&runtimeErrorReader); }, CLuceneError);

        // Test case 2: Reader throwing length error
        MockReader lengthErrorReader(MockReader::ExceptionType::LENGTH_ERROR);
        ASSERT_THROW({ tokenizer->reset(&lengthErrorReader); }, CLuceneError);

        // Test case 3: Recovery after exception
        lucene::util::SStringReader<char> recoveryReader;
        recoveryReader.init("Recovery text", 13, false);
        ASSERT_NO_THROW({ tokenizer->reset(&recoveryReader); });

        // Verify tokenizer works after recovery
        Token t;
        std::vector<std::string> tokens;
        while (tokenizer->next(&t)) {
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            tokens.emplace_back(term);
        }
        ASSERT_EQ(tokens.size(), 2);
        ASSERT_EQ(tokens[0], "recovery");
        ASSERT_EQ(tokens[1], "text");
    }

    // PART 2: Test AnalyzeContext::fillBuffer exception handling
    {
        // Create AnalyzeContext
        std::shared_ptr<Configuration> config = std::make_shared<Configuration>();
        vectorized::Arena arena {};
        AnalyzeContext context(arena, config);

        // Test case 1: Reader throwing length error
        MockReader lengthErrorReader(MockReader::ExceptionType::LENGTH_ERROR);
        ASSERT_THROW({ context.fillBuffer(&lengthErrorReader); }, CLuceneError);

        // Test case 2: Reader throwing runtime error
        MockReader runtimeErrorReader(MockReader::ExceptionType::RUNTIME_ERROR);
        ASSERT_THROW({ context.fillBuffer(&runtimeErrorReader); }, CLuceneError);

        // Test case 3: Reader simulating memory allocation failure
        MockReader largeDataReader;
        largeDataReader.setReturnSize(std::numeric_limits<int32_t>::max() - 1);
        try {
            context.fillBuffer(&largeDataReader);
            // If no exception is thrown, this is acceptable too
        } catch (const CLuceneError& e) {
            // Verify the exception is properly converted
            ASSERT_TRUE(std::string(e.what()).find("buffer filling") != std::string::npos);
        }

        // Test case 4: Reader returning empty data
        MockReader emptyReader;
        emptyReader.setReturnSize(0);
        ASSERT_EQ(context.fillBuffer(&emptyReader), 0);
    }
}

} // namespace doris::segment_v2
