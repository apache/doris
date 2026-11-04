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

// Every analysis component on its own: the TextTokenStream contract, the memory it keeps from one
// value to the next, and the strict UTF-8 switch. The member sums below read private members,
// which the unit tests may with -fno-access-control.

#include <gtest/gtest.h>
#include <unicode/umachine.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/analysis_factory_mgr.h"
#include "storage/index/inverted/analysis/analyzer/chain_analyzer.h"
#include "storage/index/inverted/analysis/analyzer/ik/IKTokenizer.h"
#include "storage/index/inverted/analysis/char_filter/char_filter_factory.h"
#include "storage/index/inverted/analysis/char_filter/char_filtered_stream.h"
#include "storage/index/inverted/analysis/gram/gram_scheme.h"
#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/token_filter/ascii_folding_filter.h"
#include "storage/index/inverted/analysis/token_filter/icu_normalizer_filter.h"
#include "storage/index/inverted/analysis/token_filter/lower_case_filter.h"
#include "storage/index/inverted/analysis/token_filter/pinyin_filter.h"
#include "storage/index/inverted/analysis/token_filter/token_filter_factory.h"
#include "storage/index/inverted/analysis/token_filter/word_delimiter_filter.h"
#include "storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/english/english_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/jieba_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/pinyin/pinyin_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/standard/uax29_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"
#include "util/defer_op.h"

namespace doris::segment_v2::inverted_index {
namespace {

constexpr size_t kLongRowBytes = 2 * 1024 * 1024;
constexpr size_t kReservedAfterShortRowLimit = 1024 * 1024;

Settings settings_of(std::unordered_map<std::string, std::string> args) {
    return {std::move(args)};
}

std::string repeat_to(std::string_view unit, size_t bytes) {
    std::string row;
    row.reserve(bytes + unit.size());
    while (row.size() < bytes) {
        row.append(unit);
    }
    return row;
}

// Values for the contract: empty, short, mixed scripts and one that takes several batches.
std::vector<std::string> contract_texts() {
    return {"",
            "Hello World",
            "中文分词 测试 abc-DEF_ghi",
            "Ｆｕｌｌ ｗｉｄｔｈ ＡＢＣ",
            "café naïve Ærøskøbing",
            repeat_to("word Alpha-beta 中文 ", 12000)};
}

// The dictionaries of the source tree, for icu, ik and pinyin.
class DictPathScope {
public:
    DictPathScope() : _saved(config::inverted_index_dict_path) {
        config::inverted_index_dict_path = "./be/dict";
    }
    ~DictPathScope() { config::inverted_index_dict_path = _saved; }

private:
    const std::string _saved;
};

// Component factories, kept alive with the streams they make: a stream must not outlive its
// factory.
class Components {
public:
    std::unique_ptr<TextTokenizer> tokenizer(const std::string& name,
                                             const Settings& settings = {}) {
        auto factory = AnalysisFactoryMgr::instance().create<TokenizerFactory>(name, settings);
        _factories.push_back(factory);
        return factory->create();
    }

    std::unique_ptr<TextTokenStream> filter(const std::string& name,
                                            std::unique_ptr<TextTokenStream> in,
                                            const Settings& settings = {}) {
        auto factory = AnalysisFactoryMgr::instance().create<TokenFilterFactory>(name, settings);
        _factories.push_back(factory);
        return factory->create(std::move(in));
    }

    std::unique_ptr<TextTokenStream> char_filtered(const std::string& name,
                                                   std::unique_ptr<TextTokenStream> in,
                                                   const Settings& settings = {}) {
        auto factory = AnalysisFactoryMgr::instance().create<CharFilterFactory>(name, settings);
        _factories.push_back(factory);
        return make_char_filtered_stream({factory}, std::move(in));
    }

private:
    std::vector<std::shared_ptr<AbstractAnalysisFactory>> _factories;
};

// Reads `long_row` and then `short_row`. What the stream keeps is what `member_bytes` adds up
// after each, more than the limit after the long row when the stream keeps memory in proportion
// to it (`grows`), and at most the limit after the short one.
void expect_released_at_next_row(TextTokenStream& stream, std::string_view long_row,
                                 std::string_view short_row, bool grows,
                                 const std::function<size_t()>& member_bytes) {
    collect_checked(stream, long_row);
    EXPECT_EQ(stream.reserved_bytes(), member_bytes());
    if (grows) {
        EXPECT_GT(stream.reserved_bytes(), kReservedAfterShortRowLimit);
    }
    collect_checked(stream, short_row);
    EXPECT_LE(stream.reserved_bytes(), kReservedAfterShortRowLimit);
    EXPECT_EQ(stream.reserved_bytes(), member_bytes());
}

template <typename T>
T& as(TextTokenStream& stream) {
    auto* typed = dynamic_cast<T*>(&stream);
    DORIS_CHECK(typed != nullptr);
    return *typed;
}

// What a filter keeps besides its own buffers: the private input batch of PerTokenFilter and the
// stream it reads.
size_t per_token_filter_bytes(const PerTokenFilter& filter) {
    return filter._input.capacity_bytes() + filter._in->reserved_bytes();
}

TEST(ComponentStreamTest, KeywordAndEmptyTokenizersKeepNothing) {
    Components components;
    for (const char* name : {"keyword", "empty"}) {
        SCOPED_TRACE(name);
        expect_text_stream_contract([&] { return components.tokenizer(name); }, contract_texts());
        auto tokenizer = components.tokenizer(name);
        expect_released_at_next_row(*tokenizer, repeat_to("a", kLongRowBytes), "short", false,
                                    [] { return size_t {0}; });
    }
}

TEST(ComponentStreamTest, StandardTokenizerKeepsItsWindow) {
    Components components;
    expect_text_stream_contract([&] { return components.tokenizer("standard"); }, contract_texts());
    auto tokenizer = components.tokenizer("standard");
    auto& standard = as<StandardTokenizer>(*tokenizer);
    expect_released_at_next_row(*tokenizer, repeat_to("word ", kLongRowBytes), "short", false,
                                [&] { return standard._scanner->_zz_buffer.capacity(); });
}

TEST(ComponentStreamTest, NGramTokenizersKeepTheirWindow) {
    Components components;
    const Settings token_chars({{"min_gram", "1"}, {"max_gram", "2"}, {"token_chars", "letter"}});
    for (const char* name : {"ngram", "edge_ngram"}) {
        for (const Settings& settings : {Settings {}, token_chars}) {
            SCOPED_TRACE(name);
            expect_text_stream_contract([&] { return components.tokenizer(name, settings); },
                                        contract_texts());
            auto tokenizer = components.tokenizer(name, settings);
            auto& ngram = as<NGramTokenizer>(*tokenizer);
            expect_released_at_next_row(*tokenizer, repeat_to("abc ", kLongRowBytes), "short",
                                        false, [&] {
                                            return ngram._buffer.capacity() * sizeof(UChar32) +
                                                   ngram._utf8_buffer.capacity();
                                        });
        }
    }
}

TEST(ComponentStreamTest, GramTokenizerReleasesItsGrams) {
    gram::GramScheme scheme;
    expect_text_stream_contract([&] { return std::make_unique<GramTokenizer>(scheme); },
                                contract_texts());
    GramTokenizer tokenizer(scheme);
    expect_released_at_next_row(
            tokenizer, repeat_to("abcdefgh ", kLongRowBytes), "short", true, [&] {
                return tokenizer._grams.capacity() * sizeof(std::string_view) +
                       tokenizer._extractor._folded.capacity() +
                       tokenizer._extractor._is_boundary_at.capacity() +
                       tokenizer._extractor._dedupe_slots.capacity() * sizeof(uint32_t);
            });
}

TEST(ComponentStreamTest, CharGroupTokenizerKeepsNothing) {
    Components components;
    const Settings settings =
            settings_of({{"tokenize_on_chars", "[whitespace], [punctuation], [cjk]"}});
    expect_text_stream_contract([&] { return components.tokenizer("char_group", settings); },
                                contract_texts());
    auto tokenizer = components.tokenizer("char_group", settings);
    expect_released_at_next_row(*tokenizer, repeat_to("ab,cd ", kLongRowBytes), "short", false,
                                [] { return size_t {0}; });
}

size_t basic_member_bytes(const BasicTokenizer& basic) {
    return (basic._lowercase ? basic._lowered.capacity() : 0) +
           basic._tokens_text.capacity() * sizeof(std::string_view);
}

TEST(ComponentStreamTest, BasicTokenizerReleasesItsBuffers) {
    Components components;
    // The block loop keeps nothing in proportion to the value; the scalar loop of extra chars
    // keeps its token list.
    for (const Settings& settings : {Settings {}, settings_of({{"extra_chars", "-_"}})}) {
        expect_text_stream_contract([&] { return components.tokenizer("basic", settings); },
                                    contract_texts());
        auto tokenizer = components.tokenizer("basic", settings);
        auto& basic = as<BasicTokenizer>(*tokenizer);
        expect_released_at_next_row(*tokenizer, repeat_to("ab ", kLongRowBytes), "short",
                                    basic._has_extra_chars,
                                    [&] { return basic_member_bytes(basic); });
    }
    // The builtin basic parser lowercases into a copy of the value.
    expect_text_stream_contract([] { return std::make_unique<BasicTokenizer>(true); },
                                contract_texts());
    BasicTokenizer lowering(true);
    lowering.initialize();
    expect_released_at_next_row(lowering, repeat_to("Ab ", kLongRowBytes), "short", true,
                                [&] { return basic_member_bytes(lowering); });
}

TEST(ComponentStreamTest, EnglishTokenizerReleasesItsLoweredCopy) {
    for (bool lowercase : {true, false}) {
        SCOPED_TRACE(lowercase);
        expect_text_stream_contract(
                [lowercase] { return std::make_unique<EnglishTokenizer>(lowercase); },
                contract_texts());
        EnglishTokenizer english(lowercase);
        expect_released_at_next_row(english, repeat_to("Ab ", kLongRowBytes), "short", lowercase,
                                    [&] { return lowercase ? english._lowered.capacity() : 0; });
    }
}

TEST(ComponentStreamTest, JiebaTokenizerReleasesItsCopyAndWordList) {
    const JiebaTokenizerFactory factory(
            false, true, "./contrib/clucene/src/contribs-lib/CLucene/analysis/jieba/dict");
    expect_text_stream_contract([&] { return factory.create(); }, contract_texts());
    auto tokenizer = factory.create();
    auto& jieba = as<JiebaTokenizer>(*tokenizer);
    expect_released_at_next_row(*tokenizer, repeat_to("我们 ", kLongRowBytes), "short", true, [&] {
        return jieba._value.capacity() + jieba._words.capacity() * sizeof(std::string_view) +
               jieba._scratch.reserved_bytes();
    });
}

TEST(ComponentStreamTest, Uax29TokenizerReleasesItsLoweredCopy) {
    for (bool lowercase : {true, false}) {
        SCOPED_TRACE(lowercase);
        expect_text_stream_contract(
                [lowercase] { return std::make_unique<Uax29Tokenizer>(lowercase, true); },
                contract_texts());
        Uax29Tokenizer standard(lowercase, true);
        expect_released_at_next_row(standard, repeat_to("Ab ", kLongRowBytes), "short", lowercase,
                                    [&] { return lowercase ? standard._lowered.capacity() : 0; });
    }
}

TEST(ComponentStreamTest, ICUTokenizerReleasesItsCopyOfTheValue) {
    DictPathScope dict_path;
    Components components;
    expect_text_stream_contract([&] { return components.tokenizer("icu"); }, contract_texts());
    auto tokenizer = components.tokenizer("icu");
    auto& icu = as<ICUTokenizer>(*tokenizer);
    expect_released_at_next_row(*tokenizer, repeat_to("word ", kLongRowBytes), "short", true, [&] {
        return static_cast<size_t>(icu.buffer_.getCapacity()) * sizeof(UChar) +
               icu.utf8Str_.capacity();
    });
}

TEST(ComponentStreamTest, PinyinTokenizerReleasesItsCandidates) {
    DictPathScope dict_path;
    Components components;
    const Settings joined({{"keep_joined_full_pinyin", "true"}, {"keep_original", "true"}});
    for (const Settings& settings : {Settings {}, joined}) {
        expect_text_stream_contract([&] { return components.tokenizer("pinyin", settings); },
                                    contract_texts());
        auto tokenizer = components.tokenizer("pinyin", settings);
        auto& pinyin = as<PinyinTokenizer>(*tokenizer);
        expect_released_at_next_row(
                *tokenizer, repeat_to("刘德华 ab ", kLongRowBytes), "short", true, [&] {
                    size_t bytes = pinyin.candidate_.capacity() * sizeof(TermItem) +
                                   pinyin.terms_filter_.bucket_count() * sizeof(void*) +
                                   pinyin.first_letters_.capacity() +
                                   pinyin.full_pinyin_letters_.capacity() +
                                   pinyin.source_codepoints_.capacity() * sizeof(UChar32) +
                                   pinyin.runes_.capacity() * sizeof(Rune);
                    for (const TermItem& item : pinyin.candidate_) {
                        bytes += item.term.capacity();
                    }
                    for (const std::string& key : pinyin.terms_filter_) {
                        bytes += sizeof(std::string) + key.capacity();
                    }
                    return bytes;
                });
    }
}

TEST(ComponentStreamTest, IKTokenizerReleasesItsWords) {
    DictPathScope dict_path;
    Components components;
    for (const char* name : {"ik_smart", "ik_max_word"}) {
        SCOPED_TRACE(name);
        expect_text_stream_contract([&] { return components.tokenizer(name); }, contract_texts());
        auto tokenizer = components.tokenizer(name);
        auto& ik = as<IKTokenizer>(*tokenizer);
        expect_released_at_next_row(
                *tokenizer, repeat_to("中华人民共和国 ab ", kLongRowBytes), "short", true, [&] {
                    return ik.words_.capacity() * sizeof(IKTokenizer::Word) +
                           ik.words_bytes_.capacity() + ik.ik_segmenter_->arena_.size() +
                           ik.ik_segmenter_->context_->segment_buff_.capacity() +
                           ik.ik_segmenter_->context_->typed_runes_.capacity() *
                                   sizeof(CharacterUtil::TypedRune) +
                           ik.ik_segmenter_->context_->results_.capacity() * sizeof(Lexeme);
                });
    }
}

// The upstream of the filter tests: the whole value as one token, however long.
std::unique_ptr<TextTokenStream> whole_value(Components& components) {
    return components.tokenizer("empty");
}

TEST(ComponentStreamTest, LowerCaseFilterKeepsNothingOfItsOwn) {
    Components components;
    expect_text_stream_contract(
            [&] { return components.filter("lowercase", components.tokenizer("standard")); },
            contract_texts());
    auto stream = components.filter("lowercase", whole_value(components));
    auto& lower = as<LowerCaseFilter>(*stream);
    // A lowercased term goes to the batch, which the consumer owns.
    expect_released_at_next_row(*stream, repeat_to("ÄBC", kLongRowBytes), "short", false,
                                [&] { return lower._in->reserved_bytes(); });
}

TEST(ComponentStreamTest, ASCIIFoldingFilterReleasesItsScratch) {
    Components components;
    for (const char* preserve : {"false", "true"}) {
        SCOPED_TRACE(preserve);
        const Settings settings = settings_of({{"preserve_original", preserve}});
        expect_text_stream_contract(
                [&] {
                    return components.filter("asciifolding", components.tokenizer("standard"),
                                             settings);
                },
                contract_texts());
    }
    // Without preserve_original the filter folds into its scratch and copies the folded term into
    // the batch; with it, a per-token filter keeps its input batch as well.
    auto stream = components.filter("asciifolding", whole_value(components),
                                    settings_of({{"preserve_original", "false"}}));
    auto& folding = as<ASCIIFoldingFilter>(*stream);
    expect_released_at_next_row(*stream, repeat_to("café", kLongRowBytes), "short", true, [&] {
        return folding._output.capacity() + folding._in->reserved_bytes();
    });
    auto preserving = components.filter("asciifolding", whole_value(components),
                                        settings_of({{"preserve_original", "true"}}));
    auto& preserve = as<ASCIIFoldingPreserveOriginalFilter>(*preserving);
    expect_released_at_next_row(*preserving, repeat_to("café", kLongRowBytes), "short", true, [&] {
        return preserve._output.capacity() + per_token_filter_bytes(preserve);
    });
}

TEST(ComponentStreamTest, ICUNormalizerFilterReleasesItsScratch) {
    Components components;
    expect_text_stream_contract(
            [&] { return components.filter("icu_normalizer", components.tokenizer("standard")); },
            contract_texts());
    auto stream = components.filter("icu_normalizer", whole_value(components));
    auto& normalizer = as<ICUNormalizerFilter>(*stream);
    // NFKC_CF folds case, so the normalized token is as long as the row.
    expect_released_at_next_row(*stream, repeat_to("ABc", kLongRowBytes), "short", true, [&] {
        return normalizer._output_buffer.capacity() + normalizer._in->reserved_bytes();
    });
}

TEST(ComponentStreamTest, WordDelimiterFilterReleasesItsParts) {
    Components components;
    const Settings catenate({{"catenate_all", "true"}, {"preserve_original", "true"}});
    for (const Settings& settings : {Settings {}, catenate}) {
        expect_text_stream_contract(
                [&] {
                    return components.filter("word_delimiter", components.tokenizer("standard"),
                                             settings);
                },
                contract_texts());
        auto stream = components.filter("word_delimiter", whole_value(components), settings);
        auto& delimiter = as<WordDelimiterFilter>(*stream);
        expect_released_at_next_row(
                *stream, repeat_to("aB-c1_", kLongRowBytes), "short", true, [&] {
                    size_t bytes =
                            delimiter._attribute.buffered.capacity() +
                            delimiter._concat->_buffer.capacity() +
                            delimiter._concat_all->_buffer.capacity() +
                            delimiter._states.capacity() * sizeof(WordDelimiterFilter::Attribute);
                    for (const auto& state : delimiter._states) {
                        bytes += state.buffered.capacity();
                    }
                    return bytes + per_token_filter_bytes(delimiter);
                });
    }
}

TEST(ComponentStreamTest, PinyinFilterReleasesItsCandidates) {
    DictPathScope dict_path;
    Components components;
    expect_text_stream_contract(
            [&] { return components.filter("pinyin", components.tokenizer("standard")); },
            contract_texts());
    auto stream = components.filter("pinyin", whole_value(components));
    auto& pinyin = as<PinyinFilter>(*stream);
    expect_released_at_next_row(*stream, repeat_to("刘德华ab", kLongRowBytes), "short", true, [&] {
        size_t bytes = pinyin.candidate_.capacity() * sizeof(TermItem) +
                       pinyin.terms_filter_.bucket_count() * sizeof(void*) +
                       pinyin.first_letters_.capacity() + pinyin.full_pinyin_letters_.capacity() +
                       pinyin.current_source_.capacity() + pinyin.current_token_text_.capacity();
        for (const TermItem& item : pinyin.candidate_) {
            bytes += item.term.capacity();
        }
        for (const std::string& key : pinyin.terms_filter_) {
            bytes += sizeof(std::string) + key.capacity();
        }
        return bytes + per_token_filter_bytes(pinyin);
    });
}

TEST(ComponentStreamTest, EmptyTokenFilterIsItsInput) {
    Components components;
    auto tokenizer = components.tokenizer("standard");
    TextTokenStream* const raw = tokenizer.get();
    EXPECT_EQ(components.filter("empty", std::move(tokenizer)).get(), raw);
}

TEST(ComponentStreamTest, CharFiltersReleaseTheirCopies) {
    Components components;
    const Settings replace({{"pattern", ",-"}, {"replacement", " "}});
    for (const auto& [name, settings] :
         std::vector<std::pair<std::string, Settings>> {{"char_replace", replace},
                                                        {"icu_normalizer", Settings {}},
                                                        {"empty", Settings {}}}) {
        SCOPED_TRACE(name);
        expect_text_stream_contract(
                [&] {
                    return components.char_filtered(name, components.tokenizer("standard"),
                                                    settings);
                },
                contract_texts());
        auto stream = components.char_filtered(name, components.tokenizer("keyword"), settings);
        auto& filtered = as<CharFilteredStream>(*stream);
        expect_released_at_next_row(*stream, repeat_to("Ａ,b-", kLongRowBytes), "short",
                                    name != "empty", [&] {
                                        size_t bytes = filtered._in->reserved_bytes();
                                        for (const std::string& scratch : filtered._scratch) {
                                            bytes += scratch.capacity();
                                        }
                                        return bytes;
                                    });
    }
}

TEST(ComponentStreamTest, AValueAChainFoldsToNothingMakesNoToken) {
    Components components;
    // NFKC_CF removes the soft hyphen, so the keyword tokenizer sees an empty view that does not
    // point to null.
    auto stream = components.char_filtered("icu_normalizer", components.tokenizer("keyword"));
    EXPECT_TRUE(collect_checked(*stream, "\xC2\xAD").empty());
    EXPECT_TRUE(collect_checked(*stream, "\xC2\xAD\xC2\xAD").empty());
    EXPECT_EQ(collect_terms(*stream, "A\xC2\xAD"), std::vector<std::string> {"a"});
    EXPECT_TRUE(collect_checked(*stream, "").empty());
}

TEST(ComponentStreamTest, StrictUtf8ThrowsInIcuAndNGram) {
    DictPathScope dict_path;
    Components components;
    const std::string invalid =
            "ab\xFF"
            "cd";
    for (const char* name : {"icu", "ngram", "edge_ngram"}) {
        SCOPED_TRACE(name);
        auto lenient = components.tokenizer(name);
        const std::vector<std::string> lenient_terms = collect_terms(*lenient, invalid);
        EXPECT_FALSE(lenient_terms.empty());

        auto strict = components.tokenizer(name);
        strict->set_strict_utf8(true);
        EXPECT_THROW(collect_checked(*strict, invalid), Exception);
        // A valid value is read the same either way.
        EXPECT_EQ(collect_terms(*strict, "abcd"), collect_terms(*lenient, "abcd"));
        // Switched off again, the stream is lenient again.
        strict->set_strict_utf8(false);
        EXPECT_EQ(collect_terms(*strict, invalid), lenient_terms);
    }
}

TEST(ComponentStreamTest, StrictUtf8LeavesTheOtherTokenizersAlone) {
    Components components;
    const std::string invalid =
            "ab\xFF"
            "cd";
    for (const char* name : {"standard", "keyword", "basic", "char_group"}) {
        SCOPED_TRACE(name);
        auto lenient = components.tokenizer(name);
        auto strict = components.tokenizer(name);
        strict->set_strict_utf8(true);
        EXPECT_EQ(collect_terms(*strict, invalid), collect_terms(*lenient, invalid));
    }
}

TEST(ComponentStreamTest, StrictUtf8FoldsIkRuneByRune) {
    DictPathScope dict_path;
    Components components;
    // 0xE4 starts a three-byte rune for IK's decoder, but "AB" are no continuation bytes: the
    // lenient fold copies the three bytes, the strict one keeps 0xE4 and lowercases "AB".
    const std::string invalid =
            "\xE4"
            "AB";
    auto lenient = components.tokenizer("ik_max_word");
    auto strict = components.tokenizer("ik_max_word");
    strict->set_strict_utf8(true);
    const std::vector<std::string> lenient_terms = collect_terms(*lenient, invalid);
    const std::vector<std::string> strict_terms = collect_terms(*strict, invalid);
    EXPECT_EQ(lenient_terms, std::vector<std::string> {"\xE4"
                                                       "AB"});
    EXPECT_EQ(strict_terms, std::vector<std::string> {"\xE4"
                                                      "ab"});
    // Valid text folds the same either way.
    const std::string valid = "Ｆｕｌｌ ＡＢＣ 中华人民共和国";
    EXPECT_EQ(collect_terms(*strict, valid), collect_terms(*lenient, valid));
}

TEST(ComponentStreamTest, PinyinFilterTurnsStrictUtf8OnUpstream) {
    DictPathScope dict_path;
    Components components;
    const std::string invalid =
            "ab\xFF"
            "cd";
    auto strict = components.filter("pinyin", components.tokenizer("ngram"),
                                    settings_of({{"ignore_pinyin_offset", "false"}}));
    EXPECT_THROW(collect_checked(*strict, invalid), Exception);
    auto lenient = components.filter("pinyin", components.tokenizer("ngram"),
                                     settings_of({{"ignore_pinyin_offset", "true"}}));
    EXPECT_FALSE(collect_checked(*lenient, invalid).empty());
    // A filter after the pinyin filter does not take the switch back.
    auto chained = components.filter(
            "lowercase", components.filter("pinyin", components.tokenizer("icu"),
                                           settings_of({{"ignore_pinyin_offset", "false"}})));
    EXPECT_THROW(collect_checked(*chained, invalid), Exception);
}

} // namespace
} // namespace doris::segment_v2::inverted_index
