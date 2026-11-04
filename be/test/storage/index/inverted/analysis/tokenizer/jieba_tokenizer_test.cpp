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

#include "storage/index/inverted/analysis/tokenizer/jieba/jieba_tokenizer.h"

#include <CLucene.h> // IWYU pragma: keep
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "CLucene/analysis/LanguageBasedAnalyzer.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/analyzer/chain_analyzer.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/golden/golden_driver.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/jieba.h"

namespace doris::segment_v2::inverted_index {
namespace {

const std::filesystem::path kShippedDict =
        "./contrib/clucene/src/contribs-lib/CLucene/analysis/jieba/dict";

// Points the dictionary path at jieba's shipped dictionaries while a test runs.
class JiebaDictScope {
public:
    JiebaDictScope() : _saved(config::inverted_index_dict_path) {
        config::inverted_index_dict_path = kShippedDict.string();
    }
    ~JiebaDictScope() { config::inverted_index_dict_path = _saved; }

private:
    const std::string _saved;
};

// A dictionary directory of a test's own: the shipped files, with user.dict.utf8 replaced by
// `user_words` and the files in `missing` left out.
class OwnDict {
public:
    explicit OwnDict(const std::vector<std::string>& user_words,
                     const std::vector<std::string>& missing = {}) {
        static int counter = 0;
        _dir = std::filesystem::temp_directory_path() /
               ("jieba_own_dict_" + std::to_string(::getpid()) + "_" + std::to_string(counter++));
        std::filesystem::create_directories(_dir);
        for (const char* name :
             {"jieba.dict.utf8", "hmm_model.utf8", "stop_words.utf8", "idf.utf8"}) {
            if (std::ranges::find(missing, name) == missing.end()) {
                std::filesystem::copy_file(kShippedDict / name, _dir / name);
            }
        }
        if (std::ranges::find(missing, "user.dict.utf8") == missing.end()) {
            std::ofstream user(_dir / "user.dict.utf8");
            for (const std::string& word : user_words) {
                user << word << "\n";
            }
        }
    }
    ~OwnDict() { std::filesystem::remove_all(_dir); }

    std::string dir() const { return _dir.string(); }
    void add(const char* name) const {
        std::filesystem::copy_file(kShippedDict / name, _dir / name);
    }

private:
    std::filesystem::path _dir;
};

// The chinese parser as production builds it.
TextAnalyzerPtr chinese_analyzer(bool fine, bool lowercase) {
    return InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType::PARSER_CHINESE,
                                                          fine ? "fine_grained" : "coarse_grained",
                                                          lowercase ? "true" : "false", "");
}

// CLucene's LanguageBasedAnalyzer, which made the chinese tokens before.
std::unique_ptr<golden::GoldenDriver> clucene_chinese(bool fine, bool lowercase) {
    return golden::make_lucene_driver([fine, lowercase] {
        auto analyzer =
                std::make_shared<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
        analyzer->initDict(kShippedDict.string());
        analyzer->setMode(fine ? lucene::analysis::AnalyzerMode::All
                               : lucene::analysis::AnalyzerMode::Default);
        analyzer->set_lowercase(lowercase);
        return analyzer;
    });
}

std::string bytes(std::initializer_list<int> values) {
    std::string text;
    for (int value : values) {
        text.push_back(static_cast<char>(value));
    }
    return text;
}

// Values that stress the segmenters: Chinese text, dictionary words, ASCII in both cases, rare
// and supplementary-plane characters jieba has no word for, separators and invalid bytes.
std::string random_value(std::mt19937_64& random) {
    static const std::vector<std::string> kPieces = {"我们",
                                                     "中华人民共和国",
                                                     "云计算",
                                                     "区块链",
                                                     "研究生命起源",
                                                     "T恤",
                                                     "B超",
                                                     "AT&T",
                                                     "C++",
                                                     "Hello",
                                                     "WORLD",
                                                     "abc",
                                                     "123",
                                                     "3.14",
                                                     " ",
                                                     "，",
                                                     "。",
                                                     "\t",
                                                     "\n",
                                                     "、",
                                                     "！",
                                                     "的",
                                                     "了",
                                                     "𠮷",
                                                     "𪚥",
                                                     "ㄅ",
                                                     "ひらがな",
                                                     "カタカナ",
                                                     "한국어",
                                                     "é",
                                                     "😀",
                                                     "-",
                                                     "_"};
    std::string value;
    const size_t pieces = random() % 40;
    for (size_t i = 0; i < pieces; ++i) {
        if (random() % 25 == 0) {
            value.push_back(static_cast<char>(random()));
        } else {
            value += kPieces[random() % kPieces.size()];
        }
    }
    return value;
}

void expect_same_as_clucene(const std::string& value, golden::GoldenDriver& candidate,
                            golden::GoldenDriver& oracle) {
    ASSERT_EQ(candidate.fresh(value), oracle.fresh(value))
            << golden::format_line(oracle.fresh(value));
    ASSERT_EQ(candidate.reused(value), oracle.reused(value));
}

struct Mode {
    bool fine;
    bool lowercase;
};
constexpr Mode kModes[] = {{.fine = false, .lowercase = false},
                           {.fine = false, .lowercase = true},
                           {.fine = true, .lowercase = false},
                           {.fine = true, .lowercase = true}};

TEST(JiebaTokenizerTest, MatchesCLuceneOnRandomValues) {
    JiebaDictScope dict;
    for (const Mode mode : kModes) {
        SCOPED_TRACE(testing::Message() << "fine " << mode.fine << " lowercase " << mode.lowercase);
        auto oracle = clucene_chinese(mode.fine, mode.lowercase);
        auto candidate = golden::make_text_driver(chinese_analyzer(mode.fine, mode.lowercase));
        std::mt19937_64 random(20261104 + mode.fine * 2 + mode.lowercase);
        for (int round = 0; round < 3000; ++round) {
            expect_same_as_clucene(random_value(random), *candidate, *oracle);
        }
    }
}

// Every word of the dictionaries, 64 to a value with a random separator or none between them, in
// its own case and with its ASCII letters raised.
TEST(JiebaTokenizerTest, MatchesCLuceneOnEveryDictionaryWord) {
    JiebaDictScope dict;
    std::vector<std::string> words;
    for (const char* name : {"jieba.dict.utf8", "user.dict.utf8"}) {
        std::ifstream file(kShippedDict / name);
        std::string line;
        while (std::getline(file, line)) {
            words.push_back(line.substr(0, line.find(' ')));
        }
    }
    ASSERT_GT(words.size(), 340000);
    static const std::vector<std::string> kSeparators = {"", "", " ", "，", "a", "Z9"};
    std::mt19937_64 random(20261106);
    std::vector<std::string> values;
    for (size_t i = 0; i < words.size(); i += 64) {
        std::string value;
        for (size_t j = i; j < std::min(words.size(), i + 64); ++j) {
            std::string word = words[j];
            if (random() % 2 == 0) {
                for (char& c : word) {
                    c = static_cast<char>(c >= 'a' && c <= 'z' ? c - 32 : c);
                }
            }
            value += word + kSeparators[random() % kSeparators.size()];
        }
        values.push_back(std::move(value));
    }
    for (const Mode mode : kModes) {
        SCOPED_TRACE(testing::Message() << "fine " << mode.fine << " lowercase " << mode.lowercase);
        auto oracle = clucene_chinese(mode.fine, mode.lowercase);
        auto candidate = golden::make_text_driver(chinese_analyzer(mode.fine, mode.lowercase));
        for (const std::string& value : values) {
            ASSERT_EQ(candidate->fresh(value), oracle->fresh(value))
                    << golden::format_line(oracle->fresh(value));
        }
    }
}

TEST(JiebaTokenizerTest, MatchesCLuceneOnMixedCaseUnknownWordsAndInvalidBytes) {
    JiebaDictScope dict;
    const std::vector<std::string> values = {"Hello世界HELLO",
                                             "我买了一件T恤和B超",
                                             "AT&T和C++以及c#",
                                             "MiXeD大小写MiXeD",
                                             "𠮷野家的𪚥𪚥",
                                             "ㄅㄆㄇ未登录词ㄈ",
                                             "你好" + bytes({0xE4, 0xB8}),
                                             "abc" + bytes({0xFF}) + "中文",
                                             bytes({0x80}) + "中文",
                                             "中文" + bytes({0xC3}),
                                             std::string(600, 'A'),
                                             "中" + std::string(300, 'b') + "文",
                                             ""};
    for (const Mode mode : kModes) {
        SCOPED_TRACE(testing::Message() << "fine " << mode.fine << " lowercase " << mode.lowercase);
        auto oracle = clucene_chinese(mode.fine, mode.lowercase);
        auto candidate = golden::make_text_driver(chinese_analyzer(mode.fine, mode.lowercase));
        for (const std::string& value : values) {
            expect_same_as_clucene(value, *candidate, *oracle);
        }
    }
}

// A value that does not decode has no token at all, the decodable head included.
TEST(JiebaTokenizerTest, AValueThatDoesNotDecodeHasNoToken) {
    JiebaDictScope dict;
    for (const Mode mode : kModes) {
        EXPECT_TRUE(collect_terms(*chinese_analyzer(mode.fine, mode.lowercase),
                                  "你好" + bytes({0xE4, 0xB8}))
                            .empty());
    }
}

// The terms the old tokenizer handed out, one at a time: each word lowercased where it lies when
// its first byte is ASCII, and read right then, so that a later word's lowercasing cannot reach it.
std::vector<std::string> handed_out_one_at_a_time(const jieba::Jieba& jieba, std::string value,
                                                  bool fine) {
    std::vector<std::string_view> words;
    if (fine) {
        jieba.cut_all(value, words);
    } else {
        jieba.cut(value, words);
    }
    std::vector<std::string> terms;
    for (const std::string_view word : words) {
        if (!word.empty() && static_cast<uint8_t>(word[0]) < 0x80) {
            char* begin = value.data() + (word.data() - value.data());
            for (size_t i = 0; i < word.size(); ++i) {
                begin[i] = ascii_lower(begin[i]);
            }
        }
        terms.emplace_back(word.substr(0, MAX_WORD_LEN));
    }
    return terms;
}

// Random values with the words of kMixedWords in them, and runs of ASCII letters and digits that
// start with a stray continuation byte: after a separator or a Chinese character, "\x80" "7abcB"
// is one run whose first byte is not ASCII.
std::vector<std::string> mixed_values() {
    std::mt19937_64 random(20261108);
    std::vector<std::string> values;
    for (int i = 0; i < 2000; ++i) {
        values.push_back(random_value(random) + "卡拉OK拉OK，\x80" + "7abcB" +
                         random_value(random) + "Q版本 \x80" + "9XyZ中\x80" + "Ab1");
    }
    return values;
}

const std::vector<std::string> kMixedWords = {"卡拉OK", "OK",  "拉O", "Q版本",
                                              "AT&T",   "C++", "中AB"};

// The fine mode hands out a run of ASCII letters and digits whole and then the runs of letters and
// of digits in it, and a stray continuation byte reads as one character with the byte after it:
// "\x80" "7" is U+0037. So "\x807abcB" comes out keeping its capital, the first byte not being
// ASCII, and "abcB" lowers that capital after it; the first word must keep it in the batch.
TEST(JiebaTokenizerTest, BatchesHandOutWhatOneWordAtATimeWould) {
    const OwnDict own(kMixedWords);
    const std::shared_ptr<const jieba::Jieba> jieba = load_jieba(own.dir());
    for (bool fine : {false, true}) {
        JiebaTokenizerFactory factory(fine, true, own.dir());
        auto tokenizer = factory.create();
        for (const std::string& value : mixed_values()) {
            ASSERT_EQ(collect_terms(*tokenizer, value),
                      handed_out_one_at_a_time(*jieba, value, fine))
                    << "fine " << fine << ": " << value;
        }
    }
}

// With lowercase on, a word is lowercased whole when its first byte is ASCII and left as it is
// otherwise, capitals and all.
TEST(JiebaTokenizerTest, TheFirstByteDecidesWhetherAWordIsLowercased) {
    const OwnDict own({"卡拉OK", "Q版本"});
    JiebaTokenizerFactory coarse(false, true, own.dir());
    EXPECT_EQ(collect_terms(*coarse.create(), "卡拉OK"), (std::vector<std::string> {"卡拉OK"}));
    EXPECT_EQ(collect_terms(*coarse.create(), "Q版本"), (std::vector<std::string> {"q版本"}));
    EXPECT_EQ(collect_terms(*coarse.create(), "T恤"), (std::vector<std::string> {"t恤"}));
    JiebaTokenizerFactory keep_case(false, false, own.dir());
    EXPECT_EQ(collect_terms(*keep_case.create(), "Q版本"), (std::vector<std::string> {"Q版本"}));
}

TEST(JiebaTokenizerTest, KeepsSupplementaryPlaneCharactersAndUserWords) {
    const OwnDict own({"𠮷野家", "𪚥𪚥"});
    for (bool fine : {false, true}) {
        JiebaTokenizerFactory factory(fine, true, own.dir());
        const std::vector<std::string> terms = collect_terms(*factory.create(), "去𠮷野家吃𪚥𪚥饭");
        EXPECT_NE(std::ranges::find(terms, "𠮷野家"), terms.end()) << fine;
        EXPECT_NE(std::ranges::find(terms, "𪚥𪚥"), terms.end()) << fine;
        const std::shared_ptr<const jieba::Jieba> jieba = load_jieba(own.dir());
        EXPECT_EQ(terms, handed_out_one_at_a_time(*jieba, "去𠮷野家吃𪚥𪚥饭", fine));
    }
}

// How many of the values below the two sides cut differently, each printed.
int mismatches_on_a_dictionary_of_its_own() {
    const OwnDict own({"卡拉OK", "OK", "拉O", "Q版本", "𠮷野家", "𪚥𪚥", "云计算"});
    const std::vector<std::string> values = {"卡拉OK",
                                             "我们唱卡拉OK吧，OK",
                                             "拉OK卡拉OK",
                                             "Q版本和q版本",
                                             "去𠮷野家吃𪚥𪚥饭",
                                             "云计算KTV卡拉OK",
                                             "OK卡拉OKOK",
                                             "卡拉ok",
                                             "T恤B超"};
    int mismatches = 0;
    for (const Mode mode : kModes) {
        auto oracle = golden::make_lucene_driver([&own, mode] {
            auto analyzer =
                    std::make_shared<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
            analyzer->initDict(own.dir());
            analyzer->setMode(mode.fine ? lucene::analysis::AnalyzerMode::All
                                        : lucene::analysis::AnalyzerMode::Default);
            analyzer->set_lowercase(mode.lowercase);
            return analyzer;
        });
        auto candidate = golden::make_text_driver(std::make_shared<ChainAnalyzer>(
                std::vector<CharFilterFactoryPtr> {},
                std::make_shared<JiebaTokenizerFactory>(mode.fine, mode.lowercase, own.dir()),
                std::vector<TokenFilterFactoryPtr> {}));
        for (const std::string& value : values) {
            if (candidate->fresh(value) != oracle->fresh(value) ||
                candidate->reused(value) != oracle->reused(value)) {
                std::cerr << "fine " << mode.fine << " lowercase " << mode.lowercase << " " << value
                          << ": " << golden::format_line(oracle->fresh(value)) << " vs "
                          << golden::format_line(candidate->fresh(value)) << "\n";
                ++mismatches;
            }
        }
    }
    return mismatches;
}

// The live differential on a dictionary of the test's own, with words that start with a Chinese
// character and hold capitals, overlap ASCII words, or lie in a supplementary plane: the shipped
// dictionaries have none. CLucene's jieba keeps the first dictionary it loads for the rest of the
// process, so this runs in a process of its own, where it loads this one.
TEST(JiebaTokenizerDeathTest, MatchesCLuceneOnADictionaryOfItsOwn) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    // _Exit: static destructors of the test binary are not safe to run in the child.
    EXPECT_EXIT(std::_Exit(mismatches_on_a_dictionary_of_its_own() == 0 ? 0 : 1),
                testing::ExitedWithCode(0), "");
}

TEST(JiebaTokenizerTest, LoadsWithoutIdf) {
    const OwnDict own({"云计算"}, {"idf.utf8"});
    JiebaTokenizerFactory factory(false, false, own.dir());
    EXPECT_EQ(collect_terms(*factory.create(), "云计算"), (std::vector<std::string> {"云计算"}));
}

// A missing file fails the factory, and a later load of the same directory tries again.
TEST(JiebaTokenizerTest, AFailedLoadIsTriedAgain) {
    const OwnDict own({}, {"hmm_model.utf8"});
    try {
        JiebaTokenizerFactory factory(false, false, own.dir());
        FAIL() << "a missing hmm_model.utf8 must fail";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
        EXPECT_NE(std::string(e.what()).find("hmm_model.utf8"), std::string::npos);
    }
    own.add("hmm_model.utf8");
    JiebaTokenizerFactory factory(false, false, own.dir());
    EXPECT_EQ(collect_terms(*factory.create(), "中华人民共和国"),
              (std::vector<std::string> {"中华人民共和国"}));
}

TEST(JiebaTokenizerTest, EightThreadsShareTheDictionaries) {
    JiebaDictScope dict;
    std::mt19937_64 random(20261107);
    std::vector<std::string> values;
    for (int i = 0; i < 400; ++i) {
        values.push_back(random_value(random));
    }
    for (const Mode mode : kModes) {
        const TextAnalyzerPtr analyzer = chinese_analyzer(mode.fine, mode.lowercase);
        std::vector<std::vector<std::string>> expected;
        for (const std::string& value : values) {
            expected.push_back(collect_terms(*analyzer, value));
        }
        std::vector<int> mismatches(8, 0);
        std::vector<std::thread> threads;
        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([&, t] {
                auto stream = analyzer->create_stream();
                for (int round = 0; round < 3; ++round) {
                    for (size_t i = 0; i < values.size(); ++i) {
                        mismatches[t] += collect_terms(*stream, values[i]) != expected[i];
                    }
                }
            });
        }
        for (std::thread& thread : threads) {
            thread.join();
        }
        EXPECT_EQ(mismatches, std::vector<int>(8, 0));
    }
}

TEST(JiebaTokenizerTest, BatchesLoseNoTokenAroundTheBatchSize) {
    JiebaDictScope dict;
    for (size_t count : {size_t {255}, size_t {256}, size_t {257}, size_t {512}}) {
        std::string value;
        for (size_t i = 0; i < count; ++i) {
            value += "w" + std::to_string(i) + " ";
        }
        for (const Mode mode : kModes) {
            auto oracle = clucene_chinese(mode.fine, mode.lowercase);
            auto candidate = golden::make_text_driver(chinese_analyzer(mode.fine, mode.lowercase));
            expect_same_as_clucene(value, *candidate, *oracle);
        }
    }
}

TEST(JiebaTokenizerTest, KeepsTheStreamContract) {
    JiebaDictScope dict;
    std::string many_batches;
    for (int i = 0; i < 1500; ++i) {
        many_batches += "我们研究" + std::to_string(i % 97) + "号AB ";
    }
    for (const Mode mode : kModes) {
        const TextAnalyzerPtr analyzer = chinese_analyzer(mode.fine, mode.lowercase);
        expect_text_stream_contract([&] { return analyzer->create_stream(); },
                                    {"", "我们", "中华人民共和国", "Hello世界", "a",
                                     "你好" + bytes({0xE4, 0xB8}), many_batches});
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index
