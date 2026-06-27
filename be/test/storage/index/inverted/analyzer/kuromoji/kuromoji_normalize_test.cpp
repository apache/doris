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

#include "storage/index/inverted/analyzer/kuromoji/kuromoji_normalize.h"

#include <gtest/gtest.h>

#include <string>

namespace doris::segment_v2::inverted_index::kuromoji {

TEST(KuromojiNormalizeTest, FullWidthAsciiToAscii) {
    EXPECT_EQ(cjk_width_normalize("\xEF\xBC\xA1\xEF\xBC\xA2\xEF\xBC\xA3"), "ABC"); // ＡＢＣ
    EXPECT_EQ(cjk_width_normalize("\xEF\xBC\x91\xEF\xBC\x92\xEF\xBC\x93"), "123"); // １２３
    // ＡＢＣ１２３
    EXPECT_EQ(cjk_width_normalize("\xEF\xBC\xA1\xEF\xBC\xA2\xEF\xBC\xA3\xEF\xBC\x91\xEF\xBC\x92"
                                  "\xEF\xBC\x93"),
              "ABC123");
}

TEST(KuromojiNormalizeTest, IdeographicSpaceToSpace) {
    EXPECT_EQ(cjk_width_normalize("\xE3\x80\x80"), " "); // U+3000
}

TEST(KuromojiNormalizeTest, PreservesOtherText) {
    EXPECT_EQ(cjk_width_normalize("ABC123"), "ABC123"); // already ASCII
    EXPECT_EQ(cjk_width_normalize("\xE6\x9D\xB1\xE4\xBA\xAC"),
              "\xE6\x9D\xB1\xE4\xBA\xAC"); // 東京 unchanged
    EXPECT_EQ(cjk_width_normalize(""), "");
    // mixed: Ａ東Ｂ -> A東B
    EXPECT_EQ(cjk_width_normalize("\xEF\xBC\xA1\xE6\x9D\xB1\xEF\xBC\xA2"),
              "A\xE6\x9D\xB1"
              "B");
}

} // namespace doris::segment_v2::inverted_index::kuromoji
