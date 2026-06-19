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
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_parser.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

TEST(KuromojiAnalyzerTest, BuiltinDispatchTokenizes) {
    auto analyzer = InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_KUROMOJI, INVERTED_INDEX_PARSER_KUROMOJI_SEARCH, "true",
            "none");
    ASSERT_NE(analyzer, nullptr);

    std::string s = "東京都";
    lucene::util::SStringReader<char> reader;
    reader.init(s.data(), s.size(), false);

    std::unique_ptr<TokenStream> ts((TokenStream*)analyzer->tokenStream(L"", &reader));
    std::vector<std::string> out;
    Token t;
    while (ts->next(&t)) {
        out.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }
    EXPECT_EQ(out, (std::vector<std::string> {"東", "京", "都"}));
}

} // namespace doris::segment_v2::inverted_index
