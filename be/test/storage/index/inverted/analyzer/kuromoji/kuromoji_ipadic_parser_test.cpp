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

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_ipadic_parser.h"

#include <gtest/gtest.h>

#include <string>

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dict_format.h"

namespace doris::segment_v2::kuromoji {

TEST(KuromojiIpadicParserTest, CategoryOrdinal) {
    EXPECT_EQ(ipadic_category_ordinal("KANJI"), CAT_KANJI);
    EXPECT_EQ(ipadic_category_ordinal(" DEFAULT "), CAT_DEFAULT);
    EXPECT_EQ(ipadic_category_ordinal("KANJINUMERIC"), CAT_KANJINUMERIC);
    EXPECT_EQ(ipadic_category_ordinal("NOPE"), CAT_CLASS_COUNT);
}

TEST(KuromojiIpadicParserTest, LexiconLine) {
    std::string surface;
    BuilderWord w;
    // Real IPADIC row shape: surface,left,right,cost,<9 feature cols>.
    ASSERT_TRUE(parse_lexicon_line("\xE4\xBB\x95\xE8\x88\x9E\xE3\x81\x84,1285,1285,5543,"
                                   "noun,general,*,*,*,*,base,reading,pron",
                                   &surface, &w)
                        .ok());
    EXPECT_EQ(surface, "\xE4\xBB\x95\xE8\x88\x9E\xE3\x81\x84"); // 仕舞い
    EXPECT_EQ(w.left_id, 1285);
    EXPECT_EQ(w.right_id, 1285);
    EXPECT_EQ(w.word_cost, 5543);
    EXPECT_EQ(w.feature, "noun,general,*,*,*,*,base,reading,pron");

    // Negative cost is valid.
    ASSERT_TRUE(parse_lexicon_line("x,1,2,-300,noun,*", &surface, &w).ok());
    EXPECT_EQ(w.word_cost, -300);

    // Too few columns -> error.
    EXPECT_FALSE(parse_lexicon_line("x,1,2", &surface, &w).ok());
}

TEST(KuromojiIpadicParserTest, MatrixDef) {
    MatrixInput m;
    ASSERT_TRUE(parse_matrix_def("2 2\n0 0 -100\n1 1 50\n# trailing comment\n", &m).ok());
    EXPECT_EQ(m.forward_size, 2U);
    EXPECT_EQ(m.backward_size, 2U);
    ASSERT_EQ(m.cells.size(), 4U);
    // data "a b cost" -> cells[b*forward_size + a]
    EXPECT_EQ(connection_cost(m.cells.data(), m.forward_size, /*fwd*/ 0, /*bwd*/ 0), -100);
    EXPECT_EQ(connection_cost(m.cells.data(), m.forward_size, /*fwd*/ 1, /*bwd*/ 1), 50);
    EXPECT_EQ(connection_cost(m.cells.data(), m.forward_size, /*fwd*/ 1, /*bwd*/ 0), 0);
}

TEST(KuromojiIpadicParserTest, CharDef) {
    const std::string content =
            "# category definitions\n"
            "DEFAULT 0 1 0\n"
            "KANJI 0 0 2\n"
            "ALPHA 1 1 0\n"
            "SYMBOL 1 1 0\n"
            "# mappings\n"
            "0x4E00..0x9FA5 KANJI\n"
            "0x0041..0x005A ALPHA\n"
            "0x3007 SYMBOL KANJINUMERIC\n";
    CharDefInput cd;
    ASSERT_TRUE(parse_char_def(content, &cd).ok());

    EXPECT_EQ(cd.defs[CAT_KANJI].invoke, 0);
    EXPECT_EQ(cd.defs[CAT_KANJI].group, 0);
    EXPECT_EQ(cd.defs[CAT_KANJI].length, 2);
    EXPECT_EQ(cd.defs[CAT_ALPHA].invoke, 1);
    EXPECT_EQ(cd.defs[CAT_DEFAULT].group, 1);

    EXPECT_EQ(cd.catmap[0x4E00], CAT_KANJI);
    EXPECT_EQ(cd.catmap[0x9FA5], CAT_KANJI);
    EXPECT_EQ(cd.catmap[0x0041], CAT_ALPHA);   // 'A'
    EXPECT_EQ(cd.catmap[0x3007], CAT_SYMBOL);  // first category is primary
    EXPECT_EQ(cd.catmap[0x0040], CAT_DEFAULT); // unmapped -> DEFAULT
}

TEST(KuromojiIpadicParserTest, UnkDef) {
    const std::string content =
            "DEFAULT,5,5,4769,symbol,general,*,*,*,*,*\n"
            "KANJI,1285,1285,11426,noun,general,*,*,*,*,*\n";
    UnkDictInput unk;
    ASSERT_TRUE(parse_unk_def(content, &unk).ok());
    ASSERT_EQ(unk.per_category.size(), static_cast<std::size_t>(CAT_CLASS_COUNT));
    ASSERT_EQ(unk.per_category[CAT_DEFAULT].size(), 1U);
    EXPECT_EQ(unk.per_category[CAT_DEFAULT][0].word_cost, 4769);
    ASSERT_EQ(unk.per_category[CAT_KANJI].size(), 1U);
    EXPECT_EQ(unk.per_category[CAT_KANJI][0].left_id, 1285);
}

} // namespace doris::segment_v2::kuromoji
