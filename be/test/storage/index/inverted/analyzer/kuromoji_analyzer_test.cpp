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

#include <memory>
#include <string>
#include <vector>

#include "CLucene.h"
#include "common/config.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_parser.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

class KuromojiAnalyzerTest : public ::testing::Test {
protected:
    bool _saved = false;
    void SetUp() override { _saved = config::enable_kuromoji_analyzer; }
    void TearDown() override { config::enable_kuromoji_analyzer = _saved; }
};

TEST_F(KuromojiAnalyzerTest, DictMissingThrows) {
    config::enable_kuromoji_analyzer = true;
    EXPECT_ANY_THROW({
        (void)InvertedIndexAnalyzer::create_builtin_analyzer(
                InvertedIndexParserType::PARSER_KUROMOJI, INVERTED_INDEX_PARSER_KUROMOJI_SEARCH,
                "true", "none");
    });
}

TEST_F(KuromojiAnalyzerTest, DisabledByConfigThrows) {
    config::enable_kuromoji_analyzer = false;
    EXPECT_ANY_THROW({
        (void)InvertedIndexAnalyzer::create_builtin_analyzer(
                InvertedIndexParserType::PARSER_KUROMOJI, INVERTED_INDEX_PARSER_KUROMOJI_SEARCH,
                "true", "none");
    });
}

TEST_F(KuromojiAnalyzerTest, RejectsUnknownParserMode) {
    config::enable_kuromoji_analyzer = true;
    EXPECT_ANY_THROW({
        (void)InvertedIndexAnalyzer::create_builtin_analyzer(
                InvertedIndexParserType::PARSER_KUROMOJI, "bogus", "true", "none");
    });
}

} // namespace doris::segment_v2::inverted_index
