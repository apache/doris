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

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <cstddef>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dict_format.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h"

namespace doris::segment_v2::inverted_index::kuromoji {

class KuromojiDictionaryTest : public ::testing::Test {
protected:
    std::string _dir;

    void SetUp() override {
        _dir = std::string(::testing::TempDir()) + "/kmj_dict_rt";
        ::mkdir(_dir.c_str(), 0755);

        SystemDictInput sys;
        sys.surfaces.push_back({"\xE6\x9D\xB1", {{1, 1, 100, "f-east"}}});             // 東
        sys.surfaces.push_back({"\xE6\x9D\xB1\xE4\xBA\xAC", {{2, 2, 50, "f-tokyo"}}}); // 東京
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_system(_dir + "/system.bin", sys).ok());

        MatrixInput m;
        m.forward_size = 3;
        m.backward_size = 3;
        m.cells = {0, 0, 0, 0, 0, 0, 0, 0, 42}; // cells[backward=2 * 3 + forward=2] = 42
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_matrix(_dir + "/matrix.bin", m).ok());

        CharDefInput cd;
        cd.catmap.fill(CAT_DEFAULT);
        cd.catmap[0x6771] = CAT_KANJI; // 東
        cd.defs.assign(CAT_CLASS_COUNT, CategoryDef {0, 0, 0});
        cd.defs[CAT_DEFAULT] = CategoryDef {1, 1, 0}; // invoke=1, group=1
        cd.defs[CAT_KANJI] = CategoryDef {0, 0, 2};   // invoke=0, group=0
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_chardef(_dir + "/chardef.bin", cd).ok());

        UnkDictInput unk;
        unk.per_category.resize(CAT_CLASS_COUNT);
        unk.per_category[CAT_DEFAULT].push_back({2, 2, 4769, "unk-default"});
        ASSERT_TRUE(KuromojiDictionaryBuilder::write_unkdict(_dir + "/unkdict.bin", unk).ok());
    }
};

TEST_F(KuromojiDictionaryTest, LoadAndQuery) {
    std::unique_ptr<KuromojiDictionary> dict;
    ASSERT_TRUE(KuromojiDictionary::load(_dir, &dict).ok());

    // common-prefix search over "東京" finds "東" (len 3) and "東京" (len 6).
    const std::string text = "\xE6\x9D\xB1\xE4\xBA\xAC";
    std::vector<KuromojiDictionary::PrefixMatch> matches;
    dict->common_prefix_search(text.data(), text.size(), &matches);
    ASSERT_EQ(matches.size(), 2U);

    // Resolve "東京" (length 6) -> its single entry: cost 50, leftId 2, feature "f-tokyo".
    bool checked = false;
    for (const auto& mt : matches) {
        if (mt.length == 6) {
            WordIdRun run = dict->run_for_value(mt.trie_value);
            ASSERT_EQ(run.count, 1U);
            const WordEntry& e = dict->word(run.entry_start);
            EXPECT_EQ(e.left_id, 2);
            EXPECT_EQ(e.word_cost, 50);
            EXPECT_EQ(dict->feature(e), std::string_view("f-tokyo"));
            checked = true;
        }
    }
    EXPECT_TRUE(checked);

    EXPECT_EQ(dict->connection_cost(/*forward*/ 2, /*backward*/ 2), 42);

    EXPECT_EQ(dict->char_category(0x6771), CAT_KANJI);
    EXPECT_EQ(dict->char_category(U'a'), CAT_DEFAULT);
    EXPECT_TRUE(dict->is_invoke(U'a'));    // DEFAULT invoke=1
    EXPECT_FALSE(dict->is_invoke(0x6771)); // KANJI invoke=0

    WordIdRun urun = dict->unknown_run(CAT_DEFAULT);
    ASSERT_EQ(urun.count, 1U);
    const WordEntry& ue = dict->unknown_word(urun.entry_start);
    EXPECT_EQ(ue.word_cost, 4769);
    EXPECT_EQ(dict->unknown_feature(ue), std::string_view("unk-default"));
}

TEST_F(KuromojiDictionaryTest, RejectsMissingFiles) {
    // Point at a directory with no dictionary files -> must fail, not crash.
    std::string bad = std::string(::testing::TempDir()) + "/kmj_dict_bad";
    ::mkdir(bad.c_str(), 0755);
    std::unique_ptr<KuromojiDictionary> dict;
    EXPECT_FALSE(KuromojiDictionary::load(bad, &dict).ok());
}

TEST_F(KuromojiDictionaryTest, RejectsEmptyTrie) {
    const std::string path = _dir + "/system.bin";
    std::string bytes;
    {
        std::ifstream in(path, std::ios::binary);
        bytes.assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    }
    const std::size_t off = sizeof(KmjFileHeader) + offsetof(KmjSystemHeader, trie_bytes);
    ASSERT_LT(off + sizeof(uint64_t), bytes.size());
    const uint64_t zero = 0;
    std::memcpy(&bytes[off], &zero, sizeof(zero));
    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    std::unique_ptr<KuromojiDictionary> dict;
    EXPECT_FALSE(KuromojiDictionary::load(_dir, &dict).ok());
}

TEST_F(KuromojiDictionaryTest, RejectsOutOfRangeCatmap) {
    CharDefInput cd;
    cd.catmap.fill(CAT_DEFAULT);
    cd.catmap[U'A'] = CAT_CLASS_COUNT; 
    cd.defs.assign(CAT_CLASS_COUNT, CategoryDef {0, 0, 0});
    cd.defs[CAT_DEFAULT] = CategoryDef {1, 1, 0};
    ASSERT_TRUE(KuromojiDictionaryBuilder::write_chardef(_dir + "/chardef.bin", cd).ok());
    std::unique_ptr<KuromojiDictionary> dict;
    EXPECT_FALSE(KuromojiDictionary::load(_dir, &dict).ok());
}

TEST_F(KuromojiDictionaryTest, RejectsEmptyUnkDict) {
    UnkDictInput unk;
    unk.per_category.resize(CAT_CLASS_COUNT); // all categories present but empty
    ASSERT_TRUE(KuromojiDictionaryBuilder::write_unkdict(_dir + "/unkdict.bin", unk).ok());
    std::unique_ptr<KuromojiDictionary> dict;
    EXPECT_FALSE(KuromojiDictionary::load(_dir, &dict).ok());
}

} // namespace doris::segment_v2::inverted_index::kuromoji
