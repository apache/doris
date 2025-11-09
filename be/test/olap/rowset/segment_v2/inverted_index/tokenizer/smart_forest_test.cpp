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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/smart_forest.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include "common/config.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/smart_get_word.h"

namespace doris::segment_v2::inverted_index {
class SmartForestTest : public ::testing::Test {
protected:
    std::string original_dict_path_;
    void SetUp() override {
        original_dict_path_ = config::inverted_index_dict_path;

        const char* doris_home = std::getenv("DORIS_HOME");
        config::inverted_index_dict_path = std::string(doris_home) + "../../dict";
    }

    void TearDown() override { config::inverted_index_dict_path = original_dict_path_; }
};

TEST_F(SmartForestTest, TestSmartGetWordBasic) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("中国", {"zhong1", "guo2"});
    forest->add("android", {"android"});
    forest->add("java", {"java"});
    forest->add("中国人", {"zhong1", "guo2", "ren2"});

    std::string content = " Android-java-中国人";

    forest->remove("中国人");

    std::transform(content.begin(), content.end(), content.begin(), ::tolower);
    auto word_getter = forest->getWord(content);

    std::vector<std::string> expected_words = {"android", "java", "中国"};
    std::vector<std::string> actual_words;

    std::string temp;
    while ((temp = word_getter->getFrontWords()) != word_getter->getNullResult()) {
        auto param = word_getter->getParam();
        actual_words.push_back(temp);
    }

    ASSERT_EQ(expected_words.size(), actual_words.size());

    for (size_t i = 0; i < expected_words.size(); i++) {
        EXPECT_EQ(expected_words[i], actual_words[i]);
    }
}

TEST_F(SmartForestTest, TestAddAndRemoveMultiple) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("北京", {"bei", "jing"});
    forest->add("上海", {"shang", "hai"});
    forest->add("广州", {"guang", "zhou"});
    forest->add("深圳", {"shen", "zhen"});

    forest->remove("深圳");
    forest->remove("广州");

    std::string content = "北京上海广州深圳";
    auto word_getter = forest->getWord(content);

    std::vector<std::string> actual_words;
    std::string temp;
    while ((temp = word_getter->getFrontWords()) != word_getter->getNullResult()) {
        actual_words.push_back(temp);
    }

    EXPECT_EQ(actual_words.size(), 2);
    EXPECT_EQ(actual_words[0], "北京");
    EXPECT_EQ(actual_words[1], "上海");
}

TEST_F(SmartForestTest, TestEmptyForest) {
    auto forest = std::make_unique<SmartForest>();

    std::string content = "测试内容";
    auto word_getter = forest->getWord(content);

    std::string matched = word_getter->getFrontWords();
    EXPECT_EQ(matched, word_getter->getNullResult());
}

TEST_F(SmartForestTest, TestOverwriteExistingWord) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("测试", {"ce", "shi"});
    forest->add("测试", {"test"});

    std::string content = "测试";
    auto word_getter = forest->getWord(content);

    std::string matched = word_getter->getFrontWords();
    EXPECT_EQ(matched, "测试");
}

TEST_F(SmartForestTest, TestAddLongPhrase) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("中华人民共和国", {"zhong", "hua", "ren", "min", "gong", "he", "guo"});

    std::string content = "中华人民共和国";
    auto word_getter = forest->getWord(content);

    std::string matched = word_getter->getFrontWords();
    EXPECT_EQ(matched, "中华人民共和国");
}

TEST_F(SmartForestTest, TestRemoveNonExistentWord) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("测试", {"ce", "shi"});
    forest->remove("不存在");

    std::string content = "测试";
    auto word_getter = forest->getWord(content);

    std::string matched = word_getter->getFrontWords();
    EXPECT_EQ(matched, "测试");
}

TEST_F(SmartForestTest, TestAddEmptyString) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("", {"empty"});
    forest->add("测试", {"ce", "shi"});

    std::string content = "测试";
    auto word_getter = forest->getWord(content);

    std::string matched = word_getter->getFrontWords();
    EXPECT_EQ(matched, "测试");
}

TEST_F(SmartForestTest, TestMultipleWordsWithSamePrefix) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("长", {"chang"});
    forest->add("长城", {"chang", "cheng"});
    forest->add("长江", {"chang", "jiang"});
    forest->add("长安", {"chang", "an"});

    std::string content = "长城长江长安";
    auto word_getter = forest->getWord(content);

    std::vector<std::string> actual_words;
    std::string temp;
    while ((temp = word_getter->getFrontWords()) != word_getter->getNullResult()) {
        actual_words.push_back(temp);
    }

    EXPECT_EQ(actual_words.size(), 3);
    EXPECT_EQ(actual_words[0], "长城");
    EXPECT_EQ(actual_words[1], "长江");
    EXPECT_EQ(actual_words[2], "长安");
}

TEST_F(SmartForestTest, TestSingleCharacterWords) {
    auto forest = std::make_unique<SmartForest>();

    forest->add("我", {"wo"});
    forest->add("你", {"ni"});
    forest->add("他", {"ta"});

    std::string content = "我你他";
    auto word_getter = forest->getWord(content);

    std::vector<std::string> actual_words;
    std::string temp;
    while ((temp = word_getter->getFrontWords()) != word_getter->getNullResult()) {
        actual_words.push_back(temp);
    }

    EXPECT_EQ(actual_words.size(), 3);
}

} // namespace doris::segment_v2::inverted_index
