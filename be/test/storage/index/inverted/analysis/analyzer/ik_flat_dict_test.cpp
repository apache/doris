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
#include <unicode/utf8.h>

#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/ik/IKTokenizer.h"
#include "storage/index/inverted/analysis/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/DictSegment.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/Dictionary.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/FlatDict.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/Hit.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {
namespace {

using TypedRune = CharacterUtil::TypedRune;
using TypedRuneArray = CharacterUtil::TypedRuneArray;

constexpr std::string_view kDictDir = "./be/dict/ik";

// A hit as DictSegment::match() made it before the trie was flattened: the reference the flat
// dictionary is held to.
struct ReferenceHit {
    bool match = false;
    bool prefix = false;
    const DictSegment* matched = nullptr;
    size_t byte_begin = 0;
    size_t byte_end = 0;
    size_t char_begin = 0;
    size_t char_end = 0;
};

// DictSegment::match(typed_runes, unicode_offset, length, hit) of the pointer trie, as it was.
void reference_match(const DictSegment& segment, const TypedRuneArray& typed_runes,
                     size_t unicode_offset, size_t length, ReferenceHit& hit) {
    hit.match = false;
    hit.prefix = false;
    if (!typed_runes.empty() && unicode_offset < typed_runes.size()) {
        hit.byte_end =
                typed_runes[unicode_offset].offset + typed_runes[unicode_offset].getByteLength();
        hit.char_end = unicode_offset;
    }
    length = (length == 0 || unicode_offset + length > typed_runes.size())
                     ? typed_runes.size() - unicode_offset
                     : length;
    const DictSegment* ds = segment.findChild(typed_runes[unicode_offset].getChar());
    if (ds != nullptr) {
        if (length == 1) {
            hit.match = ds->isWordEnd();
            if (ds->hasNextNode()) {
                hit.prefix = true;
                hit.matched = ds;
            }
        } else if (length > 1) {
            reference_match(*ds, typed_runes, unicode_offset + 1, length - 1, hit);
        }
    }
}

ReferenceHit reference_match(const DictSegment& root, const TypedRuneArray& typed_runes,
                             size_t unicode_offset, size_t length) {
    ReferenceHit hit;
    hit.byte_begin = typed_runes[unicode_offset].offset;
    hit.char_begin = unicode_offset;
    reference_match(root, typed_runes, unicode_offset, length, hit);
    return hit;
}

// Where a comparison is: the input, the walk and the character or length; formatted only when a
// comparison fails.
struct Where {
    const std::string& input;
    const char* walk;
    size_t at;
};

std::ostream& operator<<(std::ostream& out, const Where& where) {
    return out << where.input << ", " << where.walk << " " << where.at;
}

// A prefix leads to the same node: the same key, the same word end and the same number of
// children.
void expect_same_node(const DictSegment& expected, const Hit& actual, const FlatDict& flat,
                      const Where& where) {
    ASSERT_EQ(actual.getMatchedDict(), &flat) << where;
    const uint32_t node = actual.getMatchedNode();
    ASSERT_EQ(flat.nodes_[node].key, expected.keyChar()) << where;
    ASSERT_EQ(flat.isWordEnd(node), expected.isWordEnd()) << where;
    ASSERT_EQ(flat.nodes_[node].child_count, expected.childCount()) << where;
}

// The same state and ends, and a prefix leads to the same node.
void expect_same_hit(const ReferenceHit& expected, const Hit& actual, const FlatDict& flat,
                     const Where& where) {
    ASSERT_EQ(expected.match, actual.isMatch()) << where;
    ASSERT_EQ(expected.prefix, actual.isPrefix()) << where;
    ASSERT_EQ(!expected.match && !expected.prefix, actual.isUnmatch()) << where;
    ASSERT_EQ(expected.byte_begin, actual.getByteBegin()) << where;
    ASSERT_EQ(expected.byte_end, actual.getByteEnd()) << where;
    ASSERT_EQ(expected.char_begin, actual.getCharBegin()) << where;
    ASSERT_EQ(expected.char_end, actual.getCharEnd()) << where;
    if (expected.prefix) {
        expect_same_node(*expected.matched, actual, flat, where);
    }
}

// The bytes decode_rune reads for `code_point`, four for a 21-bit one from a four-byte lead too.
size_t utf8_length_of(int32_t code_point) {
    if (code_point < 0x80) {
        return 1;
    }
    if (code_point < 0x800) {
        return 2;
    }
    return code_point < 0x10000 ? 3 : 4;
}

// The runes of `code_points`, as the window holds them: each with its offset and length in bytes.
TypedRuneArray runes_of(const std::vector<int32_t>& code_points) {
    TypedRuneArray runes;
    size_t offset = 0;
    for (const int32_t code_point : code_points) {
        const size_t length = utf8_length_of(code_point);
        runes.emplace_back(code_point, offset, length, runes.size(), 1);
        offset += length;
    }
    return runes;
}

std::vector<int32_t> code_points_of(std::string_view word) {
    CharacterUtil::RuneStrArray decoded;
    if (!CharacterUtil::decodeString(word.data(), word.size(), decoded)) {
        return {};
    }
    std::vector<int32_t> code_points;
    for (const auto& rune : decoded) {
        code_points.push_back(static_cast<int32_t>(rune.rune));
    }
    return code_points;
}

// Every walk the analysis makes over `runes` from `begin`: the hit of one character continued
// character by character as a segmenter does, a match of every length from the root, and the
// stop word test of every length. Stops at the first difference.
void expect_same_walks(const DictSegment& root, const FlatDict& flat, const TypedRuneArray& runes,
                       size_t begin, const std::string& input) {
    ReferenceHit expected = reference_match(root, runes, begin, 1);
    Hit actual = flat.match(runes, begin, 1);
    expect_same_hit(expected, actual, flat,
                    {.input = input, .walk = "first character at", .at = begin});
    for (size_t i = begin + 1; i < runes.size() && expected.prefix; ++i) {
        if (::testing::Test::HasFatalFailure()) {
            return;
        }
        reference_match(*expected.matched, runes, i, 1, expected);
        flat.match(runes, i, 1, actual.getMatchedNode(), actual);
        expect_same_hit(expected, actual, flat,
                        {.input = input, .walk = "next character at", .at = i});
    }
    // A length of 0, and lengths beyond the end, mean all that is left.
    for (size_t length = 0; length <= runes.size() - begin + 1; ++length) {
        if (::testing::Test::HasFatalFailure()) {
            return;
        }
        const ReferenceHit whole = reference_match(root, runes, begin, length);
        expect_same_hit(whole, flat.match(runes, begin, length), flat,
                        {.input = input, .walk = "match of length", .at = length});
        ASSERT_EQ(whole.match, flat.isWord(runes, begin, length))
                << Where {.input = input, .walk = "word of length", .at = length};
    }
}

// A trie loaded as Dictionary loads a file.
std::unique_ptr<DictSegment> load_trie(const std::string& file) {
    auto root = std::make_unique<DictSegment>(0);
    Dictionary::loadDictFile(root.get(), std::string(kDictDir) + "/" + file, true, file);
    return root;
}

std::vector<std::string> read_lines(const std::string& file) {
    std::ifstream in(std::string(kDictDir) + "/" + file);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    return lines;
}

// Every word of a dictionary file, each of its prefixes and each of its suffixes, against the
// trie the file was loaded into; the words are also read as the window would hold them.
void expect_flat_dictionary_matches_trie(const std::string& file) {
    SCOPED_TRACE(file);
    const std::unique_ptr<DictSegment> root = load_trie(file);
    const FlatDict flat(*root);
    size_t words = 0;
    for (const std::string& line : read_lines(file)) {
        const std::vector<int32_t> code_points = code_points_of(line);
        if (code_points.empty()) {
            continue;
        }
        ++words;
        const TypedRuneArray runes = runes_of(code_points);
        for (size_t begin = 0; begin < runes.size(); ++begin) {
            expect_same_walks(*root, flat, runes, begin, line);
        }
        std::string window = line;
        TypedRuneArray regularized;
        CharacterUtil::decodeStringToRunes(window.data(), window.size(), regularized, true);
        expect_same_walks(*root, flat, regularized, 0, line);
        if (::testing::Test::HasFatalFailure()) {
            return;
        }
    }
    EXPECT_GT(words, 0U);
}

TEST(IKFlatDictTest, MainDictionaryMatchesItsTrie) {
    expect_flat_dictionary_matches_trie("main.dic");
}

TEST(IKFlatDictTest, QuantifierAndStopWordDictionariesMatchTheirTries) {
    expect_flat_dictionary_matches_trie("quantifier.dic");
    expect_flat_dictionary_matches_trie("stopword.dic");
    expect_flat_dictionary_matches_trie("extra_stopword.dic");
}

// Random strings of characters of the dictionary, ASCII, other first-plane characters and
// supplementary ones, from every start, against a trie with every kind of node: words that start
// or go on with a supplementary character, which the root's table does not hold, nodes of more
// than eight children, which are searched by bisection, and the main dictionary's words.
TEST(IKFlatDictTest, RandomStringsMatchTheTrie) {
    const std::unique_ptr<DictSegment> root = load_trie("main.dic");
    std::vector<int32_t> pool;
    for (const char* word : {"\U00020000\U00020001", "\U00020000\U00020001\U00020002", "\U0001F600",
                             "\U0001F600中", "中\U0001F600", "中\U00020000国", "\U0010FFFF",
                             "a\U0001F600b", "ab", "abc", "\U000E0001x"}) {
        root->fillSegment(word);
    }
    // Twelve children below 测 and below \U00020000, beyond kLinearChildren.
    for (int32_t i = 0; i < 12; ++i) {
        std::string word = "测";
        std::string supplementary = "\U00020000";
        char encoded[U8_MAX_LENGTH];
        int32_t length = 0;
        U8_APPEND_UNSAFE(encoded, length, 0x20010 + i * 7);
        word.append(encoded, length);
        supplementary.append(encoded, length);
        root->fillSegment(word.c_str());
        root->fillSegment(supplementary.c_str());
        pool.push_back(0x20010 + i * 7);
    }
    const FlatDict flat(*root);
    const std::vector<std::string> lines = read_lines("main.dic");
    for (size_t i = 0; i < lines.size(); i += 101) {
        for (const int32_t code_point : code_points_of(lines[i])) {
            pool.push_back(code_point);
        }
    }
    for (const int32_t code_point : std::initializer_list<int32_t> {
                 'a', 'b', 'c', 'x', '1', ' ', 0x3000, 0xFF21, 0x00E9, 0x20000, 0x20001, 0x20002,
                 0x1F600, 0x10FFFF, 0xE0001, 0x1FFFFF, 0x6D4B, 0x4E2D, 0x56FD}) {
        pool.push_back(code_point);
    }
    std::mt19937 rng(20261104);
    std::uniform_int_distribution<size_t> pick(0, pool.size() - 1);
    std::uniform_int_distribution<size_t> size(1, 8);
    for (int round = 0; round < 20000; ++round) {
        std::vector<int32_t> code_points(size(rng));
        for (int32_t& code_point : code_points) {
            code_point = pool[pick(rng)];
        }
        const TypedRuneArray runes = runes_of(code_points);
        const std::string input = "round " + std::to_string(round);
        for (size_t begin = 0; begin < runes.size(); ++begin) {
            expect_same_walks(*root, flat, runes, begin, input);
        }
        if (::testing::Test::HasFatalFailure()) {
            return;
        }
    }
}

// A loaded Dictionary keeps only its flat copies: the tries it loaded the files into are gone.
TEST(IKFlatDictTest, LoadedDictionaryKeepsNoTrie) {
    {
        const std::unique_ptr<DictSegment> trie = load_trie("quantifier.dic");
        EXPECT_GT(DictSegment::liveCount(), 300U);
    }
    EXPECT_EQ(DictSegment::liveCount(), 0U);

    Dictionary::destroy();
    Configuration config;
    config.setDictPath(std::string(kDictDir));
    Dictionary::initial(config);
    EXPECT_EQ(DictSegment::liveCount(), 0U);

    // And the copies hold the words: 中华人民共和国 is a word of main.dic, 个 of quantifier.dic and
    // "the" a stop word.
    const Dictionary& dictionary = *Dictionary::getSingleton();
    std::string text = "中华人民共和国个the";
    TypedRuneArray runes;
    CharacterUtil::decodeStringToRunes(text.data(), text.size(), runes, true);
    Hit hit = dictionary.matchInMainDict(runes, 0, 1);
    for (size_t i = 1; i < 7; ++i) {
        ASSERT_TRUE(hit.isPrefix()) << i;
        dictionary.matchWithHit(runes, i, hit);
    }
    EXPECT_TRUE(hit.isMatch());
    EXPECT_TRUE(dictionary.matchInQuantifierDict(runes, 7, 1).isMatch());
    EXPECT_TRUE(dictionary.isStopWord(runes, 8, 3));
    EXPECT_FALSE(dictionary.isStopWord(runes, 0, 2));
}

// The tokens of `text` with the singleton taken away, which makes Dictionary::getSingleton() throw,
// from a tokenizer made before; the singleton is put back after.
std::vector<std::string> collect_without_singleton(IKTokenizer& tokenizer,
                                                   const std::string& text) {
    Dictionary* const loaded = Dictionary::singleton_.exchange(nullptr);
    Defer restore {[&] { Dictionary::singleton_.store(loaded); }};
    EXPECT_THROW(Dictionary::getSingleton(), Exception);
    std::vector<std::string> terms;
    EXPECT_NO_THROW(terms = inverted_index::collect_terms(tokenizer, text));
    return terms;
}

// A tokenizer reads the Dictionary it was given when it was made: without the singleton, a value
// that runs every segmenter (letters, digits, a numeral with a quantifier, Chinese words, a stop
// word, full-width forms and a surrogate pair) gives the tokens it gave before.
TEST(IKFlatDictTest, SegmenterPathNeverCallsGetSingleton) {
    auto config = std::make_shared<Configuration>(false, true);
    config->setDictPath(std::string(kDictDir));
    Dictionary::initial(*config);
    const std::string text =
            "Doris 2023年 三个 中华人民共和国的 SQL92 ＡＢＣ１２ \U0001F600 the 数据库 a-b_c 3.14";
    for (const bool smart : {false, true}) {
        auto stream_config = std::make_shared<Configuration>(smart, true);
        stream_config->setDictPath(std::string(kDictDir));
        IKTokenizer tokenizer(stream_config, true);
        const std::vector<std::string> expected = inverted_index::collect_terms(tokenizer, text);
        EXPECT_GT(expected.size(), 10U);
        EXPECT_EQ(collect_without_singleton(tokenizer, text), expected) << smart;
    }
}

// The bytes of `text`, as hexadecimal pairs.
std::string hex_of(std::string_view text) {
    static constexpr char kDigits[] = "0123456789ABCDEF";
    std::string hex;
    for (const char c : text) {
        hex += kDigits[static_cast<uint8_t>(c) >> 4];
        hex += kDigits[static_cast<uint8_t>(c) & 0xF];
        hex += ' ';
    }
    return hex;
}

// The original CharacterUtil::regularizeString(), which built a new string for every word: the
// reference of the in-place fold and its shortcut.
std::string reference_regularize(const std::string& input, bool use_lowercase) {
    std::string temp;
    const size_t len = input.size();
    temp.reserve(len);
    for (size_t i = 0; i < len;) {
        const auto c = static_cast<unsigned char>(input[i]);
        if ((c & 0xF0) == 0xE0 && i + 2 < len) {
            const int rune =
                    ((c & 0x0F) << 12) | ((input[i + 1] & 0x3F) << 6) | (input[i + 2] & 0x3F);
            if (rune == 0x3000) {
                temp += ' ';
            } else if (rune >= 0xFF01 && rune <= 0xFF5E) {
                char half = static_cast<char>(rune - 0xFEE0);
                if (use_lowercase && half >= 'A' && half <= 'Z') {
                    half += 32;
                }
                temp += half;
            } else {
                temp += input[i];
                temp += input[i + 1];
                temp += input[i + 2];
            }
            i += 3;
        } else {
            char ch = input[i];
            if (use_lowercase && ch >= 'A' && ch <= 'Z') {
                ch += 32;
            }
            temp += ch;
            i += 1;
        }
    }
    return temp;
}

TEST(IKCharacterUtilTest, CharTypeTableMatchesTheBlockRules) {
    // decode_rune makes runes of up to 21 bits.
    size_t mismatches = 0;
    for (int32_t rune = 0; rune < 0x200000; ++rune) {
        if (CharacterUtil::identifyCharType(rune) != CharacterUtil::identifyCharTypeByBlock(rune)) {
            ADD_FAILURE_AT(__FILE__, __LINE__) << "rune " << rune;
            if (++mismatches > 10) {
                break;
            }
        }
    }
    EXPECT_EQ(mismatches, 0U);
    EXPECT_EQ(CharacterUtil::identifyCharType(0x4E2D), CharacterUtil::CharType::CHINESE);
    EXPECT_EQ(CharacterUtil::identifyCharType(0x3042), CharacterUtil::CharType::OTHER_CJK);
    EXPECT_EQ(CharacterUtil::identifyCharType(0x1F600), CharacterUtil::CharType::SURROGATE);
    EXPECT_EQ(CharacterUtil::identifyCharType('7'), CharacterUtil::CharType::ARABIC);
    EXPECT_EQ(CharacterUtil::identifyCharType('Q'), CharacterUtil::CharType::ENGLISH);
    EXPECT_EQ(CharacterUtil::identifyCharType(0x00E9), CharacterUtil::CharType::USELESS);
}

// Random bytes, most of them those the fold looks at: the leads E3 and EF, the continuation bytes
// of U+3000 and of U+FF01 to U+FF5E, other leads, and ASCII capitals.
TEST(IKCharacterUtilTest, RegularizeStringMatchesTheFullFold) {
    const std::vector<uint8_t> alphabet = {0xE3, 0xEF, 0x80, 0xBC, 0xBD, 0x81, 0x9E, 0x9F, 0xA1,
                                           0xBA, 0xE4, 0xE0, 0xE6, 0xF0, 0xC3, 'A',  'Z',  'M',
                                           'a',  'z',  '@',  '[',  ' ',  '0',  0x00, 0xFF};
    std::mt19937 rng(4096);
    std::uniform_int_distribution<size_t> pick(0, alphabet.size() - 1);
    std::uniform_int_distribution<int> any_byte(0, 255);
    std::uniform_int_distribution<size_t> size(0, 12);
    std::vector<std::string> inputs = {"",
                                       "数据库",
                                       "Doris",
                                       "ＳＱＬ",
                                       "Hello ＡＢＣ",
                                       "\xE3\x80\x80",
                                       "\xEF\xBC",
                                       "\xE3",
                                       "ｈｉ",
                                       "\xEF\xBD\x9E\xEF\xBD\x9F"};
    for (int i = 0; i < 300000; ++i) {
        std::string input(size(rng), '\0');
        for (char& c : input) {
            c = static_cast<char>(i % 4 == 0 ? any_byte(rng) : alphabet[pick(rng)]);
        }
        inputs.push_back(std::move(input));
    }
    for (const std::string& input : inputs) {
        for (const bool use_lowercase : {false, true}) {
            std::string actual = input;
            CharacterUtil::regularizeString(actual, use_lowercase);
            ASSERT_EQ(actual, reference_regularize(input, use_lowercase))
                    << hex_of(input) << "use_lowercase " << use_lowercase;
        }
    }
}

} // namespace
} // namespace doris::segment_v2
