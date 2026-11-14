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

#include <string>

#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"

namespace doris {

class CharReplaceCHarFilterTest : public ::testing::Test {};

TEST_F(CharReplaceCHarFilterTest, BasicTest) {
    std::string s = "hello,world";

    doris::CharFilterMap char_filter_map;
    char_filter_map["char_filter_type"] = "char_replace";
    char_filter_map["char_filter_pattern"] = ",";
    char_filter_map["char_filter_replacement"] = " ";
    const std::string filtered =
            segment_v2::inverted_index::InvertedIndexAnalyzer::filter_characters(char_filter_map,
                                                                                 s);
    ASSERT_FALSE(filtered.empty());

    ASSERT_EQ(filtered, "hello world");
}

TEST_F(CharReplaceCHarFilterTest, ChineseTest) {
    std::string s = "异常";

    doris::CharFilterMap char_filter_map;
    char_filter_map["char_filter_type"] = "char_replace";
    char_filter_map["char_filter_pattern"] = "，";
    char_filter_map["char_filter_replacement"] = " ";
    const std::string filtered =
            segment_v2::inverted_index::InvertedIndexAnalyzer::filter_characters(char_filter_map,
                                                                                 s);
    ASSERT_FALSE(filtered.empty());

    ASSERT_EQ(filtered, "\xE5 \x82\xE5\xB8\xB8");
}

} // namespace doris
