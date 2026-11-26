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

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "CLucene.h"
#include "common/config.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "util/defer_op.h"

// End-to-end against the REAL IPADIC dictionary generated under be/dict/kuromoji.
// Skips if the dictionary has not been generated on this host (e.g. plain CI),
// so it is safe to keep in the suite.
namespace doris::segment_v2::inverted_index::kuromoji {

static std::string real_dict_dir() {
#ifdef KUROMOJI_SOURCE_DICT_DIR
    return KUROMOJI_SOURCE_DICT_DIR;
#else
    const char* home = std::getenv("DORIS_HOME");
    return std::string(home != nullptr ? home : ".") + "/be/dict/kuromoji";
#endif
}

static bool real_dict_present() {
    struct stat st {};
    return ::stat((real_dict_dir() + "/system.bin").c_str(), &st) == 0;
}

static std::vector<std::string> segment_surfaces(const KuromojiDictionary& dict,
                                                 const std::string& text) {
    KuromojiViterbi v(dict);
    std::vector<KuromojiMorpheme> ms;
    v.segment(text, &ms);
    std::vector<std::string> out;
    out.reserve(ms.size());
    for (const auto& m : ms) {
        out.emplace_back(text.substr(m.byte_start, m.byte_len));
    }
    return out;
}

TEST(KuromojiRealDictTest, SegmentsRealJapanese) {
    if (!real_dict_present()) {
        GTEST_SKIP() << "real IPADIC dictionary not generated at " << real_dict_dir();
    }
    std::unique_ptr<KuromojiDictionary> dict;
    ASSERT_TRUE(KuromojiDictionary::load(real_dict_dir(), &dict).ok());

    // U+6771 U+4EAC U+90FD U+306B U+4F4F U+3093 U+3067 U+3044 U+307E U+3059  ("I live in Tokyo"), 10 code points.
    const std::string text =
            "\xE6\x9D\xB1\xE4\xBA\xAC\xE9\x83\xBD\xE3\x81\xAB\xE4\xBD\x8F"
            "\xE3\x82\x93\xE3\x81\xA7\xE3\x81\x84\xE3\x81\xBE\xE3\x81\x99";
    const std::vector<std::string> toks = segment_surfaces(*dict, text);

    // Lossless coverage: morphemes concatenate back to the input.
    std::string concat;
    for (const auto& t : toks) {
        concat += t;
    }
    EXPECT_EQ(concat, text);

    // Real morphological segmentation: more than one token, but well under the
    // 10 code points a per-character split would produce.
    EXPECT_GT(toks.size(), 1U);
    EXPECT_LT(toks.size(), 10U);

    // The particle "U+306B" must be isolated as its own token.
    EXPECT_NE(std::find(toks.begin(), toks.end(), std::string("\xE3\x81\xAB")), toks.end());

    std::cout << "Segmentation of the Japanese sentence:";
    for (const auto& t : toks) {
        std::cout << " [" << t << "]";
    }
    std::cout << std::endl;
}

TEST(KuromojiRealDictTest, KnownCompoundWord) {
    if (!real_dict_present()) {
        GTEST_SKIP() << "real IPADIC dictionary not generated at " << real_dict_dir();
    }
    std::unique_ptr<KuromojiDictionary> dict;
    ASSERT_TRUE(KuromojiDictionary::load(real_dict_dir(), &dict).ok());

    // U+65E5 U+672C U+8A9E  ("Japanese language") is a single IPADIC entry -> one token.
    const std::string nihongo = "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E";
    const std::vector<std::string> toks = segment_surfaces(*dict, nihongo);
    std::cout << "Segmentation of the Japanese word:";
    for (const auto& t : toks) {
        std::cout << " [" << t << "]";
    }
    std::cout << std::endl;
    EXPECT_EQ(toks.size(), 1U);
    EXPECT_EQ(toks[0], nihongo);
}

// Full analyzer path: parser="kuromoji" -> create_builtin_analyzer -> initDict loads
// the real dictionary -> tokenStream emits morphemes. Points the dict-path config at
// be/dict (so + "/kuromoji" resolves) and restores it so other tests are unaffected.
TEST(KuromojiRealDictTest, AnalyzerSegmentsViaBuiltinParser) {
    if (!real_dict_present()) {
        GTEST_SKIP() << "real IPADIC dictionary not generated at " << real_dict_dir();
    }
    const std::string saved = doris::config::inverted_index_dict_path;
    const bool saved_enable = doris::config::enable_kuromoji_analyzer;
    Defer restore_config {[&] {
        doris::config::inverted_index_dict_path = saved;
        doris::config::enable_kuromoji_analyzer = saved_enable;
    }};
    doris::config::inverted_index_dict_path =
            std::filesystem::path(real_dict_dir()).parent_path().string();
    doris::config::enable_kuromoji_analyzer = true;

    std::vector<std::string> toks;
    {
        auto analyzer =
                doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_builtin_analyzer(
                        doris::InvertedIndexParserType::PARSER_KUROMOJI,
                        doris::INVERTED_INDEX_PARSER_KUROMOJI_SEARCH, "true", "none");
        ASSERT_NE(analyzer, nullptr);

        const std::string text =
                "\xE6\x9D\xB1\xE4\xBA\xAC\xE9\x83\xBD\xE3\x81\xAB\xE4\xBD\x8F"
                "\xE3\x82\x93\xE3\x81\xA7\xE3\x81\x84\xE3\x81\xBE\xE3\x81\x99";
        lucene::util::SStringReader<char> reader;
        reader.init(text.data(), text.size(), false);
        std::unique_ptr<lucene::analysis::TokenStream> ts(
                (lucene::analysis::TokenStream*)analyzer->tokenStream(L"", &reader));
        lucene::analysis::Token t;
        while (ts->next(&t) != nullptr) {
            toks.emplace_back(t.termBuffer<char>(), static_cast<std::size_t>(t.termLength<char>()));
        }
    }

    std::cout << "Analyzer tokens for the Japanese sentence:";
    for (const auto& t : toks) {
        std::cout << " [" << t << "]";
    }
    std::cout << std::endl;

    EXPECT_GT(toks.size(), 1U);
    // Part-of-speech stop filtering removes the particle U+306B.
    EXPECT_EQ(std::find(toks.begin(), toks.end(), std::string("\xE3\x81\xAB")),
              toks.end()); // U+306B
    // Content words survive; U+6771 U+4EAC  is kept as-is.
    EXPECT_NE(std::find(toks.begin(), toks.end(), std::string("\xE6\x9D\xB1\xE4\xBA\xAC")),
              toks.end()); // U+6771 U+4EAC
    // Base-form normalization rewrites the conjugated U+4F4F U+3093  to its lemma U+4F4F U+3080.
    EXPECT_EQ(std::find(toks.begin(), toks.end(), std::string("\xE4\xBD\x8F\xE3\x82\x93")),
              toks.end()); // U+4F4F U+3093  (surface) absent
    EXPECT_NE(std::find(toks.begin(), toks.end(), std::string("\xE4\xBD\x8F\xE3\x82\x80")),
              toks.end()); // U+4F4F U+3080  (base form) present
}

} // namespace doris::segment_v2::inverted_index::kuromoji
