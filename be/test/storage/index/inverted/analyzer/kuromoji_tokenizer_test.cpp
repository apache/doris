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
#include <vector>

#include "CLucene.h"
#include "storage/index/inverted/analyzer/kuromoji/KuromojiTokenizer.h"

using namespace lucene::analysis;

namespace doris::segment_v2 {

static std::vector<std::string> tokenize(const std::string& s) {
    KuromojiTokenizer tk(KuromojiMode::Search, true, false);
    lucene::util::SStringReader<char> reader;
    reader.init(s.data(), s.size(), false);
    tk.reset(&reader);
    std::vector<std::string> out;
    Token t;
    while (tk.next(&t)) {
        out.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }
    return out;
}

TEST(KuromojiTokenizerStubTest, CjkUnigram) {
    EXPECT_EQ(tokenize("東京都"), (std::vector<std::string> {"東", "京", "都"}));
}

TEST(KuromojiTokenizerStubTest, SkipsAsciiSpaces) {
    EXPECT_EQ(tokenize("a b"), (std::vector<std::string> {"a", "b"}));
}

} // namespace doris::segment_v2
