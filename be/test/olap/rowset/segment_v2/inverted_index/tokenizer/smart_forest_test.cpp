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

} // namespace doris::segment_v2::inverted_index
