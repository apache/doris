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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/smart_get_word.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/config.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/smart_forest.h"

namespace doris::segment_v2::inverted_index {

class SmartGetWordTest : public ::testing::Test {
protected:
    std::unique_ptr<SmartForest> forest_;
    std::string original_dict_path_;

    void SetUp() override {
        original_dict_path_ = config::inverted_index_dict_path;
        const char* doris_home = std::getenv("DORIS_HOME");
        config::inverted_index_dict_path = std::string(doris_home) + "../../dict";

        forest_ = std::make_unique<SmartForest>();
    }

    void TearDown() override { config::inverted_index_dict_path = original_dict_path_; }
};

// Test basic word matching
TEST_F(SmartGetWordTest, TestBasicWordMatching) {
    // Add some test phrases to the forest
    forest_->add("你呢", std::vector<std::string> {"ni", "ne"});
    forest_->add("做不了", std::vector<std::string> {"zuo", "bu", "liao"});
    forest_->add("空调", std::vector<std::string> {"kong", "tiao"});
    forest_->add("厦门", std::vector<std::string> {"xia", "men"});

    // Test "你呢"
    SmartGetWord word_matcher1(forest_.get(), "你呢");
    std::string matched1 = word_matcher1.getFrontWords();
    EXPECT_EQ(matched1, "你呢") << "Should match entire phrase '你呢'";
    EXPECT_EQ(word_matcher1.getParam().size(), 2) << "Should have 2 pinyins for '你呢'";
    EXPECT_EQ(word_matcher1.getParam()[0], "ni");
    EXPECT_EQ(word_matcher1.getParam()[1], "ne");

    // Test "做不了"
    SmartGetWord word_matcher2(forest_.get(), "做不了");
    std::string matched2 = word_matcher2.getFrontWords();
    EXPECT_EQ(matched2, "做不了") << "Should match entire phrase '做不了'";
    EXPECT_EQ(word_matcher2.getParam().size(), 3) << "Should have 3 pinyins for '做不了'";
    EXPECT_EQ(word_matcher2.getParam()[0], "zuo");
    EXPECT_EQ(word_matcher2.getParam()[1], "bu");
    EXPECT_EQ(word_matcher2.getParam()[2], "liao");

    // Test "空调"
    SmartGetWord word_matcher3(forest_.get(), "空调");
    std::string matched3 = word_matcher3.getFrontWords();
    EXPECT_EQ(matched3, "空调") << "Should match entire phrase '空调'";
    EXPECT_EQ(word_matcher3.getParam().size(), 2) << "Should have 2 pinyins for '空调'";
    EXPECT_EQ(word_matcher3.getParam()[0], "kong");
    EXPECT_EQ(word_matcher3.getParam()[1], "tiao");

    // Test "厦门"
    SmartGetWord word_matcher4(forest_.get(), "厦门");
    std::string matched4 = word_matcher4.getFrontWords();
    EXPECT_EQ(matched4, "厦门") << "Should match entire phrase '厦门'";
    EXPECT_EQ(word_matcher4.getParam().size(), 2) << "Should have 2 pinyins for '厦门'";
    EXPECT_EQ(word_matcher4.getParam()[0], "xia");
    EXPECT_EQ(word_matcher4.getParam()[1], "men");
}

// Test that longer matches take precedence
TEST_F(SmartGetWordTest, TestLongestMatchPrecedence) {
    // Add overlapping phrases
    forest_->add("长", std::vector<std::string> {"zhang"});
    forest_->add("长城", std::vector<std::string> {"chang", "cheng"});
    forest_->add("长城很长", std::vector<std::string> {"chang", "cheng", "hen", "chang"});

    // Test with "长城很长" - should match the longest phrase
    SmartGetWord word_matcher(forest_.get(), "长城很长");
    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "长城很长") << "Should match longest phrase '长城很长'";
    EXPECT_EQ(word_matcher.getParam().size(), 4);
}

// Test partial matching within longer text
TEST_F(SmartGetWordTest, TestPartialMatching) {
    forest_->add("你呢", std::vector<std::string> {"ni", "ne"});
    forest_->add("好的", std::vector<std::string> {"hao", "de"});

    // Test "你呢好的吗" - should match "你呢" first, then "好的"
    SmartGetWord word_matcher(forest_.get(), "你呢好的吗");

    // First match: "你呢"
    std::string matched1 = word_matcher.getFrontWords();
    EXPECT_EQ(matched1, "你呢");
    EXPECT_EQ(word_matcher.getParam().size(), 2);

    // Second match: "好的"
    std::string matched2 = word_matcher.getFrontWords();
    EXPECT_EQ(matched2, "好的");
    EXPECT_EQ(word_matcher.getParam().size(), 2);

    // No more matches (吗 is not in dictionary)
    std::string matched3 = word_matcher.getFrontWords();
    EXPECT_EQ(matched3, word_matcher.getNullResult());
}

// Test empty and edge cases
TEST_F(SmartGetWordTest, TestEdgeCases) {
    forest_->add("你呢", std::vector<std::string> {"ni", "ne"});

    // Empty string
    SmartGetWord word_matcher1(forest_.get(), "");
    std::string matched1 = word_matcher1.getFrontWords();
    EXPECT_EQ(matched1, word_matcher1.getNullResult());

    // Single character not in dictionary
    SmartGetWord word_matcher2(forest_.get(), "啊");
    std::string matched2 = word_matcher2.getFrontWords();
    EXPECT_EQ(matched2, word_matcher2.getNullResult());

    // Text with no matches
    SmartGetWord word_matcher3(forest_.get(), "完全没有匹配");
    std::string matched3 = word_matcher3.getFrontWords();
    EXPECT_EQ(matched3, word_matcher3.getNullResult());
}

// Test matching order - should return matches in order they appear
TEST_F(SmartGetWordTest, TestMatchingOrder) {
    forest_->add("你呢", std::vector<std::string> {"ni", "ne"});
    forest_->add("做不了", std::vector<std::string> {"zuo", "bu", "liao"});
    forest_->add("今天", std::vector<std::string> {"jin", "tian"});
    forest_->add("空调", std::vector<std::string> {"kong", "tiao"});
    forest_->add("厦门", std::vector<std::string> {"xia", "men"});

    SmartGetWord word_matcher(forest_.get(), "你呢今天做不了");

    std::vector<std::string> matched_words;
    std::string matched;
    while ((matched = word_matcher.getFrontWords()) != word_matcher.getNullResult() &&
           !matched.empty()) {
        matched_words.push_back(matched);
    }

    EXPECT_EQ(matched_words.size(), 3);
    EXPECT_EQ(matched_words[0], "你呢");
    EXPECT_EQ(matched_words[1], "今天");
    EXPECT_EQ(matched_words[2], "做不了");

    // Test with more polyphone phrases
    SmartGetWord word_matcher2(forest_.get(), "厦门的空调很好");
    std::vector<std::string> matched_words2;
    while ((matched = word_matcher2.getFrontWords()) != word_matcher2.getNullResult() &&
           !matched.empty()) {
        matched_words2.push_back(matched);
    }

    EXPECT_EQ(matched_words2.size(), 2);
    EXPECT_EQ(matched_words2[0], "厦门");
    EXPECT_EQ(matched_words2[1], "空调");
}

// Test byte offset tracking
TEST_F(SmartGetWordTest, TestByteOffsetTracking) {
    forest_->add("你呢", std::vector<std::string> {"ni", "ne"});

    SmartGetWord word_matcher(forest_.get(), "你呢");
    std::string matched = word_matcher.getFrontWords();

    EXPECT_EQ(matched, "你呢");
    EXPECT_EQ(word_matcher.offe, 0) << "First match should start at byte offset 0";
}

// Test reset functionality
TEST_F(SmartGetWordTest, TestReset) {
    forest_->add("你呢", std::vector<std::string> {"ni", "ne"});

    SmartGetWord word_matcher(forest_.get(), "你呢");

    // First run
    std::string matched1 = word_matcher.getFrontWords();
    EXPECT_EQ(matched1, "你呢");

    // Should be exhausted now
    std::string matched2 = word_matcher.getFrontWords();
    EXPECT_EQ(matched2, word_matcher.getNullResult());

    // Reset with new content
    word_matcher.reset("你呢");

    // Should match again
    std::string matched3 = word_matcher.getFrontWords();
    EXPECT_EQ(matched3, "你呢");
}

// Test with mixed Chinese and ASCII
TEST_F(SmartGetWordTest, TestMixedChineseASCII) {
    forest_->add("你呢", std::vector<std::string> {"ni", "ne"});

    // Text with English letters (should skip them)
    SmartGetWord word_matcher(forest_.get(), "ABC你呢DEF");

    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "你呢") << "Should match '你呢' skipping ASCII";
}

// Test WORD_END vs WORD_CONTINUE status
TEST_F(SmartGetWordTest, TestWordEndStatus) {
    // Add nested phrases
    forest_->add("长", std::vector<std::string> {"chang"});
    forest_->add("长城", std::vector<std::string> {"chang", "cheng"});

    // Test "长" - should match if followed by non-matching char
    SmartGetWord word_matcher1(forest_.get(), "长江");
    std::string matched1 = word_matcher1.getFrontWords();
    EXPECT_EQ(matched1, "长") << "Should match '长' when followed by non-matching '江'";

    // Test "长城" - should match the longer phrase
    SmartGetWord word_matcher2(forest_.get(), "长城");
    std::string matched2 = word_matcher2.getFrontWords();
    EXPECT_EQ(matched2, "长城") << "Should match complete phrase '长城'";
}

// Test multiple consecutive matches
TEST_F(SmartGetWordTest, TestConsecutiveMatches) {
    forest_->add("北京", std::vector<std::string> {"bei", "jing"});
    forest_->add("上海", std::vector<std::string> {"shang", "hai"});
    forest_->add("广州", std::vector<std::string> {"guang", "zhou"});

    SmartGetWord word_matcher(forest_.get(), "北京上海广州");

    std::string matched1 = word_matcher.getFrontWords();
    EXPECT_EQ(matched1, "北京");

    std::string matched2 = word_matcher.getFrontWords();
    EXPECT_EQ(matched2, "上海");

    std::string matched3 = word_matcher.getFrontWords();
    EXPECT_EQ(matched3, "广州");
}

// Test single character phrases
TEST_F(SmartGetWordTest, TestSingleCharacterPhrases) {
    forest_->add("我", std::vector<std::string> {"wo"});
    forest_->add("你", std::vector<std::string> {"ni"});
    forest_->add("他", std::vector<std::string> {"ta"});

    SmartGetWord word_matcher(forest_.get(), "我你他");

    std::string matched1 = word_matcher.getFrontWords();
    EXPECT_EQ(matched1, "我");

    std::string matched2 = word_matcher.getFrontWords();
    EXPECT_EQ(matched2, "你");

    std::string matched3 = word_matcher.getFrontWords();
    EXPECT_EQ(matched3, "他");
}

// Test very long phrase
TEST_F(SmartGetWordTest, TestVeryLongPhrase) {
    forest_->add("中华人民共和国",
                 std::vector<std::string> {"zhong", "hua", "ren", "min", "gong", "he", "guo"});

    SmartGetWord word_matcher(forest_.get(), "中华人民共和国是伟大的国家");
    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "中华人民共和国");
}

// Test phrase with punctuation
TEST_F(SmartGetWordTest, TestPhraseWithPunctuation) {
    forest_->add("你好", std::vector<std::string> {"ni", "hao"});

    SmartGetWord word_matcher(forest_.get(), "你好，世界！");
    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "你好");
}

// Test overlapping but different phrases
TEST_F(SmartGetWordTest, TestOverlappingPhrases) {
    forest_->add("中国", std::vector<std::string> {"zhong", "guo"});
    forest_->add("中国人", std::vector<std::string> {"zhong", "guo", "ren"});
    forest_->add("国人", std::vector<std::string> {"guo", "ren"});

    SmartGetWord word_matcher(forest_.get(), "中国人民");
    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "中国人") << "Should match the longest phrase";
}

// Test text starting with non-Chinese
TEST_F(SmartGetWordTest, TestStartingWithNonChinese) {
    forest_->add("测试", std::vector<std::string> {"ce", "shi"});

    SmartGetWord word_matcher(forest_.get(), "123测试456");
    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "测试");
}

// Test multiple resets
TEST_F(SmartGetWordTest, TestMultipleResets) {
    forest_->add("测试", std::vector<std::string> {"ce", "shi"});

    SmartGetWord word_matcher(forest_.get(), "测试");

    for (int i = 0; i < 3; i++) {
        std::string matched = word_matcher.getFrontWords();
        EXPECT_EQ(matched, "测试");
        word_matcher.reset("测试");
    }
}

// Test empty pinyin vector
TEST_F(SmartGetWordTest, TestEmptyPinyinVector) {
    forest_->add("空", std::vector<std::string> {});

    SmartGetWord word_matcher(forest_.get(), "空调");
    std::string matched = word_matcher.getFrontWords();
    // Behavior depends on implementation
    EXPECT_TRUE(matched == "空" || matched == word_matcher.getNullResult());
}

// Test number merging - single digit in dictionary followed by consecutive digits
TEST_F(SmartGetWordTest, TestNumberMerging) {
    // Add single digit to dictionary
    forest_->add("1", std::vector<std::string> {"yi"});

    // Test merging consecutive digits
    SmartGetWord word_matcher(forest_.get(), "12345");
    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "12345") << "Should merge consecutive digits starting with '1'";
}

// Test letter merging - single letter in dictionary followed by same letters
TEST_F(SmartGetWordTest, TestSameLetterMerging) {
    // Add single letter to dictionary
    forest_->add("a", std::vector<std::string> {"a"});

    // Test merging consecutive same letters
    SmartGetWord word_matcher(forest_.get(), "aaaa");
    std::string matched = word_matcher.getFrontWords();
    EXPECT_EQ(matched, "aaaa") << "Should merge consecutive same letters 'a'";
}

} // namespace doris::segment_v2::inverted_index
