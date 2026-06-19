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
#include <sys/stat.h>

#include <string>

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dict_format.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h"

namespace doris::segment_v2::kuromoji {

static bool file_nonempty(const std::string& p) {
    struct stat st {};
    return ::stat(p.c_str(), &st) == 0 && st.st_size > static_cast<off_t>(sizeof(KmjFileHeader));
}

TEST(KuromojiDictionaryBuilderTest, WritesFourFiles) {
    std::string dir = std::string(::testing::TempDir()) + "/kmj_build_test";
    ::mkdir(dir.c_str(), 0755);

    SystemDictInput sys;
    sys.surfaces.push_back({"\xE6\x9D\xB1", {{1, 1, 100, "POS,East"}}});             // 東
    sys.surfaces.push_back({"\xE6\x9D\xB1\xE4\xBA\xAC", {{2, 2, 50, "POS,Tokyo"}}}); // 東京
    ASSERT_TRUE(KuromojiDictionaryBuilder::write_system(dir + "/system.bin", sys).ok());

    MatrixInput m;
    m.forward_size = 3;
    m.backward_size = 3;
    m.cells.assign(9, 7);
    ASSERT_TRUE(KuromojiDictionaryBuilder::write_matrix(dir + "/matrix.bin", m).ok());

    CharDefInput cd;
    cd.catmap.fill(CAT_DEFAULT);
    cd.catmap[0x6771] = CAT_KANJI; // 東
    cd.defs.assign(CAT_CLASS_COUNT, CategoryDef {0, 0, 0});
    cd.defs[CAT_DEFAULT] = CategoryDef {1, 1, 0};
    ASSERT_TRUE(KuromojiDictionaryBuilder::write_chardef(dir + "/chardef.bin", cd).ok());

    UnkDictInput unk;
    unk.per_category.resize(CAT_CLASS_COUNT);
    unk.per_category[CAT_DEFAULT].push_back({5, 5, 4769, "SYMBOL"});
    ASSERT_TRUE(KuromojiDictionaryBuilder::write_unkdict(dir + "/unkdict.bin", unk).ok());

    EXPECT_TRUE(file_nonempty(dir + "/system.bin"));
    EXPECT_TRUE(file_nonempty(dir + "/matrix.bin"));
    EXPECT_TRUE(file_nonempty(dir + "/chardef.bin"));
    EXPECT_TRUE(file_nonempty(dir + "/unkdict.bin"));

    // matrix rejects a wrong cell count.
    MatrixInput bad;
    bad.forward_size = 2;
    bad.backward_size = 2;
    bad.cells.assign(3, 0);
    EXPECT_FALSE(KuromojiDictionaryBuilder::write_matrix(dir + "/bad.bin", bad).ok());
}

} // namespace doris::segment_v2::kuromoji
