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

#include "storage/index/inverted/analysis/golden/golden_environment.h"

#include <gen_cpp/AgentService_types.h>

#include <cstdint>
#include <map>
#include <set>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dict_format.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h"

namespace doris::segment_v2::inverted_index::golden {

namespace {

constexpr std::string_view kJiebaDictDir =
        "contrib/clucene/src/contribs-lib/CLucene/analysis/jieba/dict";

using kuromoji::CharCategory;

// Context ids of the fixed kuromoji dictionary. Every word has the same left and right id.
enum ContextId : int16_t {
    BOUNDARY = 0,
    NOUN = 1,
    PROPER_NOUN = 2,
    PARTICLE = 3,
    VERB = 4,
    AUXILIARY = 5,
    SYMBOL = 6,
    NUMERAL = 7,
    SUFFIX = 8,
    ADJECTIVE = 9,
    CONJUNCTION = 10,
    CONTEXT_COUNT = 11,
};

int16_t connection_cost(int previous, int next) {
    const auto is_noun = [](int id) {
        return id == NOUN || id == PROPER_NOUN || id == NUMERAL || id == SUFFIX;
    };
    if (previous == BOUNDARY || next == BOUNDARY) {
        return 0;
    }
    if (next == PARTICLE) {
        return is_noun(previous) || previous == VERB ? -300 : 200;
    }
    if (previous == PARTICLE) {
        return next == NOUN || next == PROPER_NOUN || next == VERB || next == ADJECTIVE ||
                               next == NUMERAL
                       ? -200
                       : 300;
    }
    if (next == AUXILIARY) {
        return previous == VERB || previous == AUXILIARY || previous == ADJECTIVE ? -400 : 400;
    }
    if (next == SUFFIX) {
        return previous == NOUN || previous == PROPER_NOUN || previous == NUMERAL ? -300 : 500;
    }
    if (previous == NUMERAL && next == NUMERAL) {
        return -200;
    }
    if (previous == SYMBOL || next == SYMBOL) {
        return 0;
    }
    return 100;
}

struct Word {
    const char* surface;
    int16_t id;
    int16_t cost;
    const char* feature; // POS1-4, conjugation type and form, base form, reading, pronunciation
};

// Compounds with their parts, so the search and extended modes have something to split;
// conjugated verbs, whose base form is the term; and the parts of speech the tokenizer drops.
// NOLINTBEGIN(modernize-use-designated-initializers): rows of a table, columns as in Word
constexpr Word kWords[] = {
        {"東京", PROPER_NOUN, 3000, "名詞,固有名詞,地域,一般,*,*,東京,トウキョウ,トーキョー"},
        {"東", NOUN, 5000, "名詞,一般,*,*,*,*,東,ヒガシ,ヒガシ"},
        {"京都", PROPER_NOUN, 3000, "名詞,固有名詞,地域,一般,*,*,京都,キョウト,キョート"},
        {"京", PROPER_NOUN, 6000, "名詞,固有名詞,地域,一般,*,*,京,キョウ,キョー"},
        {"都", SUFFIX, 4000, "名詞,接尾,地域,*,*,*,都,ト,ト"},
        {"大学", NOUN, 3500, "名詞,一般,*,*,*,*,大学,ダイガク,ダイガク"},
        {"東京大学", PROPER_NOUN, 4000,
         "名詞,固有名詞,組織,*,*,*,東京大学,トウキョウダイガク,トーキョーダイガク"},
        {"学生", NOUN, 3500, "名詞,一般,*,*,*,*,学生,ガクセイ,ガクセイ"},
        {"関西", PROPER_NOUN, 3500, "名詞,固有名詞,地域,一般,*,*,関西,カンサイ,カンサイ"},
        {"国際", NOUN, 3500, "名詞,一般,*,*,*,*,国際,コクサイ,コクサイ"},
        {"空港", NOUN, 3500, "名詞,一般,*,*,*,*,空港,クウコウ,クーコー"},
        {"関西国際空港", PROPER_NOUN, 5000,
         "名詞,固有名詞,組織,*,*,*,関西国際空港,カンサイコクサイクウコウ,カンサイコクサイクーコー"},
        {"日本", PROPER_NOUN, 3000, "名詞,固有名詞,地域,国,*,*,日本,ニッポン,ニッポン"},
        {"日本語", NOUN, 3500, "名詞,一般,*,*,*,*,日本語,ニホンゴ,ニホンゴ"},
        {"私", NOUN, 3000, "名詞,代名詞,一般,*,*,*,私,ワタシ,ワタシ"},
        {"今日", NOUN, 3000, "名詞,副詞可能,*,*,*,*,今日,キョウ,キョー"},
        {"天気", NOUN, 3500, "名詞,一般,*,*,*,*,天気,テンキ,テンキ"},
        {"いい", ADJECTIVE, 3000, "形容詞,自立,*,*,形容詞・イイ,基本形,いい,イイ,イイ"},
        {"食べ", VERB, 3500, "動詞,自立,*,*,一段,連用形,食べる,タベ,タベ"},
        {"食べる", VERB, 3500, "動詞,自立,*,*,一段,基本形,食べる,タベル,タベル"},
        {"住ん", VERB, 3500, "動詞,自立,*,*,五段・マ行,連用タ接続,住む,スン,スン"},
        {"行っ", VERB, 3500, "動詞,自立,*,*,五段・カ行促音便,連用タ接続,行く,イッ,イッ"},
        {"行き", VERB, 3500, "動詞,自立,*,*,五段・カ行促音便,連用形,行く,イキ,イキ"},
        {"行く", VERB, 3500, "動詞,自立,*,*,五段・カ行促音便,基本形,行く,イク,イク"},
        {"い", VERB, 4000, "動詞,非自立,*,*,一段,連用形,いる,イ,イ"},
        {"乗る", VERB, 3500, "動詞,自立,*,*,五段・ラ行,基本形,乗る,ノル,ノル"},
        {"コンピューター", NOUN, 3000,
         "名詞,一般,*,*,*,*,コンピューター,コンピューター,コンピューター"},
        {"コンピュータ", NOUN, 3200, "名詞,一般,*,*,*,*,コンピュータ,コンピュータ,コンピュータ"},
        {"データベース", NOUN, 3000, "名詞,一般,*,*,*,*,データベース,データベース,データベース"},
        {"データ", NOUN, 3200, "名詞,一般,*,*,*,*,データ,データ,データ"},
        {"ベース", NOUN, 3500, "名詞,一般,*,*,*,*,ベース,ベース,ベース"},
        {"は", PARTICLE, 1000, "助詞,係助詞,*,*,*,*,は,ハ,ワ"},
        {"に", PARTICLE, 1000, "助詞,格助詞,一般,*,*,*,に,ニ,ニ"},
        {"を", PARTICLE, 1000, "助詞,格助詞,一般,*,*,*,を,ヲ,ヲ"},
        {"の", PARTICLE, 1000, "助詞,連体化,*,*,*,*,の,ノ,ノ"},
        {"が", PARTICLE, 1000, "助詞,格助詞,一般,*,*,*,が,ガ,ガ"},
        {"で", PARTICLE, 1200, "助詞,接続助詞,*,*,*,*,で,デ,デ"},
        {"です", AUXILIARY, 1000, "助動詞,*,*,*,特殊・デス,基本形,です,デス,デス"},
        {"ます", AUXILIARY, 1000, "助動詞,*,*,*,特殊・マス,基本形,ます,マス,マス"},
        {"まし", AUXILIARY, 1000, "助動詞,*,*,*,特殊・マス,連用形,ます,マシ,マシ"},
        {"た", AUXILIARY, 1000, "助動詞,*,*,*,特殊・タ,基本形,た,タ,タ"},
        {"、", SYMBOL, 500, "記号,読点,*,*,*,*,、,、,、"},
        {"。", SYMBOL, 500, "記号,句点,*,*,*,*,。,。,。"},
        {"えーと", CONJUNCTION, 2000, "フィラー,*,*,*,*,*,えーと,エート,エート"},
        {"そして", CONJUNCTION, 2000, "接続詞,*,*,*,*,*,そして,ソシテ,ソシテ"},
        {"ＡＢＣ", PROPER_NOUN, 3000, "名詞,固有名詞,組織,*,*,*,ＡＢＣ,エービーシー,エービーシー"},
        {"一", NUMERAL, 2000, "名詞,数,*,*,*,*,一,イチ,イチ"},
        {"二", NUMERAL, 2000, "名詞,数,*,*,*,*,二,ニ,ニ"},
        {"三", NUMERAL, 2000, "名詞,数,*,*,*,*,三,サン,サン"},
        {"十", NUMERAL, 2000, "名詞,数,*,*,*,*,十,ジュウ,ジュー"},
        {"百", NUMERAL, 2000, "名詞,数,*,*,*,*,百,ヒャク,ヒャク"},
        {"円", SUFFIX, 2000, "名詞,接尾,助数詞,*,*,*,円,エン,エン"},
        {"人", SUFFIX, 3000, "名詞,接尾,一般,*,*,*,人,ジン,ジン"},
        {"人々", NOUN, 3500, "名詞,一般,*,*,*,*,人々,ヒトビト,ヒトビト"},
        {"時々", NOUN, 3500, "名詞,副詞可能,*,*,*,*,時々,トキドキ,トキドキ"},
};
// NOLINTEND(modernize-use-designated-initializers)

struct CategoryRange {
    uint32_t first;
    uint32_t last;
    CharCategory category;
};

// The IPADIC character classes, BMP only. A later range overrides an earlier one.
// NOLINTBEGIN(modernize-use-designated-initializers): rows of a table, columns as in CategoryRange
constexpr CategoryRange kCategoryRanges[] = {
        {0x0009, 0x000D, CharCategory::CAT_SPACE},    {0x0020, 0x0020, CharCategory::CAT_SPACE},
        {0x0021, 0x002F, CharCategory::CAT_SYMBOL},   {0x0030, 0x0039, CharCategory::CAT_NUMERIC},
        {0x003A, 0x0040, CharCategory::CAT_SYMBOL},   {0x0041, 0x005A, CharCategory::CAT_ALPHA},
        {0x005B, 0x0060, CharCategory::CAT_SYMBOL},   {0x0061, 0x007A, CharCategory::CAT_ALPHA},
        {0x007B, 0x007E, CharCategory::CAT_SYMBOL},   {0x00A1, 0x00BF, CharCategory::CAT_SYMBOL},
        {0x00C0, 0x024F, CharCategory::CAT_ALPHA},    {0x1E00, 0x1EFF, CharCategory::CAT_ALPHA},
        {0x0370, 0x03FF, CharCategory::CAT_GREEK},    {0x0400, 0x052F, CharCategory::CAT_CYRILLIC},
        {0x2000, 0x206F, CharCategory::CAT_SYMBOL},   {0x3000, 0x3000, CharCategory::CAT_SPACE},
        {0x3001, 0x303F, CharCategory::CAT_SYMBOL},   {0x3041, 0x309F, CharCategory::CAT_HIRAGANA},
        {0x30A1, 0x30FF, CharCategory::CAT_KATAKANA}, {0x31F0, 0x31FF, CharCategory::CAT_KATAKANA},
        {0xFF66, 0xFF9F, CharCategory::CAT_KATAKANA}, {0x2E80, 0x2FDF, CharCategory::CAT_KANJI},
        {0x3005, 0x3005, CharCategory::CAT_KANJI},    {0x3400, 0x4DBF, CharCategory::CAT_KANJI},
        {0x4E00, 0x9FFF, CharCategory::CAT_KANJI},    {0xF900, 0xFAFF, CharCategory::CAT_KANJI},
        {0xFF10, 0xFF19, CharCategory::CAT_NUMERIC},  {0xFF21, 0xFF3A, CharCategory::CAT_ALPHA},
        {0xFF41, 0xFF5A, CharCategory::CAT_ALPHA},    {0xFF01, 0xFF0F, CharCategory::CAT_SYMBOL},
        {0xFF1A, 0xFF20, CharCategory::CAT_SYMBOL},   {0xFF3B, 0xFF40, CharCategory::CAT_SYMBOL},
        {0xFF5B, 0xFF65, CharCategory::CAT_SYMBOL},
};
// NOLINTEND(modernize-use-designated-initializers)

// 〇 一 二 三 四 五 六 七 八 九 十 百 千 万 億 兆
constexpr uint32_t kKanjiNumerals[] = {0x3007, 0x4E00, 0x4E8C, 0x4E09, 0x56DB, 0x4E94,
                                       0x516D, 0x4E03, 0x516B, 0x4E5D, 0x5341, 0x767E,
                                       0x5343, 0x4E07, 0x5104, 0x5146};

size_t ordinal(CharCategory category) {
    return static_cast<size_t>(category);
}

} // namespace

Status write_golden_kuromoji_dictionary(const std::string& dir) {
    std::map<std::string, std::vector<kuromoji::BuilderWord>> surfaces;
    for (const Word& word : kWords) {
        surfaces[word.surface].push_back({.left_id = word.id,
                                          .right_id = word.id,
                                          .word_cost = word.cost,
                                          .feature = word.feature});
    }
    kuromoji::SystemDictInput system;
    for (auto& [surface, words] : surfaces) {
        system.surfaces.emplace_back(surface, std::move(words));
    }
    RETURN_IF_ERROR(kuromoji::KuromojiDictionaryBuilder::write_system(dir + "/system.bin", system));

    kuromoji::MatrixInput matrix;
    matrix.forward_size = CONTEXT_COUNT;
    matrix.backward_size = CONTEXT_COUNT;
    matrix.cells.resize(CONTEXT_COUNT * CONTEXT_COUNT);
    for (int backward = 0; backward < CONTEXT_COUNT; ++backward) {
        for (int forward = 0; forward < CONTEXT_COUNT; ++forward) {
            matrix.cells[backward * CONTEXT_COUNT + forward] = connection_cost(forward, backward);
        }
    }
    RETURN_IF_ERROR(kuromoji::KuromojiDictionaryBuilder::write_matrix(dir + "/matrix.bin", matrix));

    kuromoji::CharDefInput chardef;
    chardef.catmap.fill(CharCategory::CAT_DEFAULT);
    for (const CategoryRange& range : kCategoryRanges) {
        for (uint32_t code_point = range.first; code_point <= range.last; ++code_point) {
            chardef.catmap[code_point] = range.category;
        }
    }
    for (uint32_t code_point : kKanjiNumerals) {
        chardef.catmap[code_point] = CharCategory::CAT_KANJINUMERIC;
    }
    // invoke, group, length, as in the IPADIC char.def.
    chardef.defs.assign(ordinal(CharCategory::CAT_CLASS_COUNT), kuromoji::CategoryDef {});
    const auto define = [&](CharCategory category, uint8_t invoke, uint8_t group, uint16_t length) {
        chardef.defs[ordinal(category)] = {.invoke = invoke, .group = group, .length = length};
    };
    define(CharCategory::CAT_DEFAULT, 0, 1, 0);
    define(CharCategory::CAT_SPACE, 0, 1, 0);
    define(CharCategory::CAT_SYMBOL, 1, 1, 0);
    define(CharCategory::CAT_NUMERIC, 1, 1, 0);
    define(CharCategory::CAT_ALPHA, 1, 1, 0);
    define(CharCategory::CAT_CYRILLIC, 1, 1, 0);
    define(CharCategory::CAT_GREEK, 1, 1, 0);
    define(CharCategory::CAT_HIRAGANA, 0, 1, 2);
    define(CharCategory::CAT_KATAKANA, 1, 1, 2);
    define(CharCategory::CAT_KANJI, 0, 0, 2);
    define(CharCategory::CAT_KANJINUMERIC, 1, 1, 0);
    RETURN_IF_ERROR(
            kuromoji::KuromojiDictionaryBuilder::write_chardef(dir + "/chardef.bin", chardef));

    kuromoji::UnkDictInput unknown;
    unknown.per_category.resize(ordinal(CharCategory::CAT_CLASS_COUNT));
    const auto add_unknown = [&](CharCategory category, ContextId id, int16_t cost,
                                 const char* feature) {
        unknown.per_category[ordinal(category)].push_back(
                {.left_id = id, .right_id = id, .word_cost = cost, .feature = feature});
    };
    add_unknown(CharCategory::CAT_DEFAULT, SYMBOL, 4769, "記号,一般,*,*,*,*,*");
    add_unknown(CharCategory::CAT_SPACE, SYMBOL, 8903, "記号,空白,*,*,*,*,*");
    add_unknown(CharCategory::CAT_SYMBOL, NOUN, 17585, "名詞,サ変接続,*,*,*,*,*");
    add_unknown(CharCategory::CAT_NUMERIC, NUMERAL, 27473, "名詞,数,*,*,*,*,*");
    add_unknown(CharCategory::CAT_ALPHA, NOUN, 13398, "名詞,一般,*,*,*,*,*");
    add_unknown(CharCategory::CAT_ALPHA, PROPER_NOUN, 14228, "名詞,固有名詞,組織,*,*,*,*");
    add_unknown(CharCategory::CAT_CYRILLIC, NOUN, 7966, "名詞,一般,*,*,*,*,*");
    add_unknown(CharCategory::CAT_GREEK, NOUN, 7884, "名詞,一般,*,*,*,*,*");
    add_unknown(CharCategory::CAT_HIRAGANA, NOUN, 13069, "名詞,一般,*,*,*,*,*");
    add_unknown(CharCategory::CAT_KATAKANA, NOUN, 9461, "名詞,一般,*,*,*,*,*");
    add_unknown(CharCategory::CAT_KANJI, NOUN, 11426, "名詞,一般,*,*,*,*,*");
    add_unknown(CharCategory::CAT_KANJI, PROPER_NOUN, 14000, "名詞,固有名詞,一般,*,*,*,*");
    add_unknown(CharCategory::CAT_KANJINUMERIC, NUMERAL, 27473, "名詞,数,*,*,*,*,*");
    return kuromoji::KuromojiDictionaryBuilder::write_unkdict(dir + "/unkdict.bin", unknown);
}

GoldenEnvironment::GoldenEnvironment()
        : _source_root(std::filesystem::current_path()),
          _saved_dict_path(config::inverted_index_dict_path),
          _saved_enable_kuromoji(config::enable_kuromoji_analyzer),
          _saved_policy_mgr(ExecEnv::GetInstance()->_index_policy_mgr) {}

GoldenEnvironment::~GoldenEnvironment() {
    ExecEnv::GetInstance()->_index_policy_mgr = _saved_policy_mgr;
    config::inverted_index_dict_path = _saved_dict_path;
    config::enable_kuromoji_analyzer = _saved_enable_kuromoji;
}

Status GoldenEnvironment::init(const std::vector<GoldenConfig>& configs) {
    // The layout of the BE package: jieba's files at the top, the other dictionaries below.
    const std::filesystem::path& dir = _dict_dir.path();
    for (const auto& entry : std::filesystem::directory_iterator(_source_root / kJiebaDictDir)) {
        std::filesystem::create_symlink(entry.path(), dir / entry.path().filename());
    }
    for (const char* name : {"icu", "ik", "pinyin"}) {
        std::filesystem::create_directory_symlink(_source_root / "be/dict" / name, dir / name);
    }
    std::filesystem::create_directory(dir / "kuromoji");
    RETURN_IF_ERROR(write_golden_kuromoji_dictionary((dir / "kuromoji").string()));
    config::inverted_index_dict_path = dir.string();
    config::enable_kuromoji_analyzer = true;

    std::vector<TIndexPolicy> policies;
    std::set<std::string> names;
    for (const GoldenConfig& config : configs) {
        for (const GoldenPolicy& golden : config.policies) {
            if (!names.insert(golden.name).second) {
                return Status::InternalError("policy {} is defined twice", golden.name);
            }
            TIndexPolicy policy;
            policy.id = static_cast<int64_t>(policies.size()) + 1;
            policy.name = golden.name;
            policy.type = golden.type;
            policy.properties = golden.properties;
            policies.push_back(std::move(policy));
        }
    }
    _policy_mgr = std::make_unique<IndexPolicyMgr>();
    _policy_mgr->apply_policy_changes(policies, {});
    ExecEnv::GetInstance()->_index_policy_mgr = _policy_mgr.get();
    return Status::OK();
}

CorpusSources GoldenEnvironment::corpus_sources() const {
    return {.jieba_dict = (_source_root / kJiebaDictDir / "jieba.dict.utf8").string(),
            .ik_main_dict = (_source_root / "be/dict/ik/main.dic").string(),
            .ik_quantifier_dict = (_source_root / "be/dict/ik/quantifier.dic").string()};
}

} // namespace doris::segment_v2::inverted_index::golden
