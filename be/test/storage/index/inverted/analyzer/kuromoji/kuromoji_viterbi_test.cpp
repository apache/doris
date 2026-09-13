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

#include "storage/index/inverted/analyzer/kuromoji/kuromoji_viterbi.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "CLucene.h"
#include "storage/index/inverted/analyzer/kuromoji/KuromojiTokenizer.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dict_format.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h"

namespace doris::segment_v2::inverted_index::kuromoji {

// 東 = E6 9D B1 (U+6771), 京 = E4 BA AC (U+4EAC), 府 = E5 BA 9C (U+5E9C)
static const std::string TOU = "\xE6\x9D\xB1";
static const std::string KYO = "\xE4\xBA\xAC";
static const std::string FU = "\xE5\xBA\x9C";

class KuromojiViterbiTest : public ::testing::Test {
protected:
    std::string _dir;
    std::unique_ptr<KuromojiDictionary> _dict;

    void SetUp() override {
        _dir = std::string(::testing::TempDir()) + "/kmj_viterbi";
        ::mkdir(_dir.c_str(), 0755);

        // Lexicon: single chars cost 1000 each; the compound "東京" costs 100.
        // With an all-zero connection matrix, the min-cost path must pick "東京".
        SystemDictInput sys;
        sys.surfaces.push_back({TOU, {{1, 1, 1000, "noun,East"}}});
        sys.surfaces.push_back({KYO, {{1, 1, 1000, "noun,Capital"}}});
        sys.surfaces.push_back({TOU + KYO, {{1, 1, 100, "noun,Tokyo"}}});
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_system(_dir + "/system.bin", sys).ok());

        MatrixInput m;
        m.forward_size = 2; // context ids: 0 = BOS/EOS, 1 = word
        m.backward_size = 2;
        m.cells.assign(4, 0);
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_matrix(_dir + "/matrix.bin", m).ok());

        CharDefInput cd;
        cd.catmap.fill(CAT_DEFAULT);
        cd.catmap[0x6771] = CAT_KANJI; // 東
        cd.catmap[0x4EAC] = CAT_KANJI; // 京
        cd.catmap[0x5E9C] = CAT_KANJI; // 府 (not in lexicon -> unknown)
        cd.defs.assign(CAT_CLASS_COUNT, CategoryDef {0, 0, 0});
        cd.defs[CAT_KANJI] = CategoryDef {0, 0, 0};   // invoke=0, group=0 -> single-char unknown
        cd.defs[CAT_DEFAULT] = CategoryDef {1, 1, 0}; // invoke=1, group=1
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_chardef(_dir + "/chardef.bin", cd).ok());

        UnkDictInput unk;
        unk.per_category.resize(CAT_CLASS_COUNT);
        unk.per_category[CAT_KANJI].push_back({1, 1, 5000, "unknown,kanji"});
        unk.per_category[CAT_DEFAULT].push_back({1, 1, 5000, "unknown,default"});
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_unkdict(_dir + "/unkdict.bin", unk).ok());

        ASSERT_TRUE(KuromojiDictionary::load(_dir, &_dict).ok());
    }

    std::vector<std::string> surfaces(std::string_view text) const {
        KuromojiViterbi v(*_dict);
        std::vector<KuromojiMorpheme> ms;
        v.segment(text, &ms);
        std::vector<std::string> out;
        for (const auto& m : ms) {
            out.emplace_back(text.substr(m.byte_start, m.byte_len));
        }
        return out;
    }
};

TEST_F(KuromojiViterbiTest, PrefersCompoundOverSingleChars) {
    // "東京" must segment as one word, not 東 + 京.
    EXPECT_EQ(surfaces(TOU + KYO), (std::vector<std::string> {TOU + KYO}));
}

TEST_F(KuromojiViterbiTest, KnownPlusUnknown) {
    // "東京府" -> 東京 (known) + 府 (unknown), since 東京(100)+府(5000) beats 東+京+府.
    KuromojiViterbi v(*_dict);
    std::vector<KuromojiMorpheme> ms;
    v.segment(TOU + KYO + FU, &ms);
    ASSERT_EQ(ms.size(), 2U);
    EXPECT_EQ(ms[0].byte_start, 0U);
    EXPECT_EQ(ms[0].byte_len, 6U);
    EXPECT_TRUE(ms[0].known);
    EXPECT_EQ(ms[1].byte_start, 6U);
    EXPECT_EQ(ms[1].byte_len, 3U);
    EXPECT_FALSE(ms[1].known);
    EXPECT_EQ((TOU + KYO + FU).substr(ms[1].byte_start, ms[1].byte_len), FU);
}

TEST_F(KuromojiViterbiTest, SingleKnownWord) {
    EXPECT_EQ(surfaces(TOU), (std::vector<std::string> {TOU}));
}

TEST_F(KuromojiViterbiTest, EmptyInput) {
    EXPECT_TRUE(surfaces("").empty());
}

TEST_F(KuromojiViterbiTest, AllUnknown) {
    // A lone unknown kanji 府 -> exactly one unknown morpheme covering it.
    KuromojiViterbi v(*_dict);
    std::vector<KuromojiMorpheme> ms;
    v.segment(FU, &ms);
    ASSERT_EQ(ms.size(), 1U);
    EXPECT_FALSE(ms[0].known);
    EXPECT_EQ(ms[0].byte_len, 3U);
}

// End-to-end through the CLucene Tokenizer wrapper: with a dictionary, the
// tokenizer emits Viterbi morphemes ("東京", "府") rather than unigrams.
TEST_F(KuromojiViterbiTest, TokenizerUsesDictionary) {
    const std::string s = TOU + KYO + FU;
    lucene::util::SStringReader<char> reader;
    reader.init(s.data(), s.size(), false);

    KuromojiTokenizer tk(KuromojiMode::Search, true, false, _dict.get());
    tk.reset(&reader);

    std::vector<std::string> toks;
    lucene::analysis::Token t;
    while (tk.next(&t) != nullptr) {
        toks.emplace_back(t.termBuffer<char>(), static_cast<std::size_t>(t.termLength<char>()));
    }
    EXPECT_EQ(toks, (std::vector<std::string> {TOU + KYO, FU}));
}

// 都 = E9 83 BD (U+90FD), 山 = E5 B1 B1 (U+5C71), 川 = E5 B7 9D (U+5DDD)
static const std::string TO = "\xE9\x83\xBD";
static const std::string YAMA = "\xE5\xB1\xB1";
static const std::string KAWA = "\xE5\xB7\x9D";

// Search-mode compound decomposition, mirroring Lucene's JapaneseTokenizer:
// long all-kanji dictionary words are penalized so the Viterbi prefers their
// shorter parts (better recall), while Normal mode keeps the whole compound and
// short (<= 2 kanji) compounds are never split.
class KuromojiSearchModeTest : public ::testing::Test {
protected:
    std::string _dir;
    std::unique_ptr<KuromojiDictionary> _dict;

    void SetUp() override {
        _dir = std::string(::testing::TempDir()) + "/kmj_search";
        ::mkdir(_dir.c_str(), 0755);

        // Single kanji cost 1000 each; compounds "東京都" and "山川" cost 100.
        SystemDictInput sys;
        sys.surfaces.push_back({TOU, {{1, 1, 1000, "noun"}}});
        sys.surfaces.push_back({KYO, {{1, 1, 1000, "noun"}}});
        sys.surfaces.push_back({TO, {{1, 1, 1000, "noun"}}});
        sys.surfaces.push_back({YAMA, {{1, 1, 1000, "noun"}}});
        sys.surfaces.push_back({KAWA, {{1, 1, 1000, "noun"}}});
        sys.surfaces.push_back({TOU + KYO + TO, {{1, 1, 100, "noun"}}});
        sys.surfaces.push_back({YAMA + KAWA, {{1, 1, 100, "noun"}}});
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_system(_dir + "/system.bin", sys).ok());

        MatrixInput m;
        m.forward_size = 2;
        m.backward_size = 2;
        m.cells.assign(4, 0);
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_matrix(_dir + "/matrix.bin", m).ok());

        CharDefInput cd;
        cd.catmap.fill(CAT_DEFAULT);
        cd.catmap[0x6771] = CAT_KANJI; // 東
        cd.catmap[0x4EAC] = CAT_KANJI; // 京
        cd.catmap[0x90FD] = CAT_KANJI; // 都
        cd.catmap[0x5C71] = CAT_KANJI; // 山
        cd.catmap[0x5DDD] = CAT_KANJI; // 川
        cd.defs.assign(CAT_CLASS_COUNT, CategoryDef {0, 0, 0});
        cd.defs[CAT_KANJI] = CategoryDef {0, 0, 0};
        cd.defs[CAT_DEFAULT] = CategoryDef {1, 1, 0};
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_chardef(_dir + "/chardef.bin", cd).ok());

        UnkDictInput unk;
        unk.per_category.resize(CAT_CLASS_COUNT);
        unk.per_category[CAT_KANJI].push_back({1, 1, 5000, "unknown"});
        unk.per_category[CAT_DEFAULT].push_back({1, 1, 5000, "unknown"});
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_unkdict(_dir + "/unkdict.bin", unk).ok());

        ASSERT_TRUE(KuromojiDictionary::load(_dir, &_dict).ok());
    }

    std::vector<std::string> surfaces(std::string_view text, KuromojiMode mode) const {
        KuromojiViterbi v(*_dict, mode);
        std::vector<KuromojiMorpheme> ms;
        v.segment(text, &ms);
        std::vector<std::string> out;
        for (const auto& m : ms) {
            out.emplace_back(text.substr(m.byte_start, m.byte_len));
        }
        return out;
    }
};

TEST_F(KuromojiSearchModeTest, NormalModeKeepsCompound) {
    // 東京都(100) beats 東+京+都(3000): the compound is one token.
    EXPECT_EQ(surfaces(TOU + KYO + TO, KuromojiMode::Normal),
              (std::vector<std::string> {TOU + KYO + TO}));
}

TEST_F(KuromojiSearchModeTest, SearchModeDecompoundsKanjiCompound) {
    // 東京都 is 3 kanji: penalty (3-2)*3000 makes it 3100, so 東+京+都(3000) wins.
    EXPECT_EQ(surfaces(TOU + KYO + TO, KuromojiMode::Search),
              (std::vector<std::string> {TOU, KYO, TO}));
}

TEST_F(KuromojiSearchModeTest, SearchModeKeepsShortCompound) {
    // 山川 is only 2 kanji (not > SEARCH_MODE_KANJI_LENGTH), so no penalty: it is
    // kept whole even in search mode.
    EXPECT_EQ(surfaces(YAMA + KAWA, KuromojiMode::Search),
              (std::vector<std::string> {YAMA + KAWA}));
}

TEST_F(KuromojiSearchModeTest, SearchModeKeepsUnknownGroupWhole) {
    // "abc" is an out-of-vocabulary run that segments as one unknown group token
    // in normal/search mode.
    EXPECT_EQ(surfaces("abc", KuromojiMode::Search), (std::vector<std::string> {"abc"}));
}

TEST_F(KuromojiSearchModeTest, ExtendedModeSplitsUnknownIntoUnigrams) {
    // Extended mode additionally decomposes an unknown word into per-character
    // unigrams (mirroring Lucene JapaneseTokenizer's EXTENDED mode).
    EXPECT_EQ(surfaces("abc", KuromojiMode::Extended), (std::vector<std::string> {"a", "b", "c"}));
}

TEST_F(KuromojiSearchModeTest, ExtendedModeStillDecompoundsKnownCompound) {
    // Extended mode keeps search-mode behavior for known compounds.
    EXPECT_EQ(surfaces(TOU + KYO + TO, KuromojiMode::Extended),
              (std::vector<std::string> {TOU, KYO, TO}));
}

} // namespace doris::segment_v2::inverted_index::kuromoji
