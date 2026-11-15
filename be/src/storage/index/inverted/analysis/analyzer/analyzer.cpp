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

#include "storage/index/inverted/analysis/analyzer/analyzer.h"

#include <glog/logging.h>

#include "CLucene.h"
#include "CLucene/analysis/LanguageBasedAnalyzer.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
// clang-format off
#include "common/compile_check_avoid_begin.h"
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "common/compile_check_avoid_end.h"
// clang-format on
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/analysis/analyzer/basic/basic_analyzer.h"
#include "storage/index/inverted/analysis/analyzer/icu/icu_analyzer.h"
#include "storage/index/inverted/analysis/analyzer/ik/IKAnalyzer.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/KuromojiAnalyzer.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize.h"
#include "storage/index/inverted/analysis/char_filter/char_replace_char_filter_factory.h"
#include "storage/index/inverted/analysis/gram/gram_scheme.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/token_filter/lower_case_filter.h"
#include "storage/index/inverted/clucene/lucene_backed_text_analyzer.h"
#include "util/string_util.h"

namespace doris::segment_v2::inverted_index {
namespace {

// The parser of a configuration whose analyzer is builtin, named by the parser or by the analyzer.
InvertedIndexParserType builtin_parser_type(const InvertedIndexAnalyzerConfig& config) {
    return config.analyzer_name.empty()
                   ? config.parser_type
                   : get_inverted_index_parser_type_from_string(config.analyzer_name);
}

TextAnalyzerPtr builtin_text_analyzer(const InvertedIndexAnalyzerConfig& config) {
    return std::make_shared<LuceneBackedTextAnalyzer>(
            InvertedIndexAnalyzer::create_builtin_analyzer(builtin_parser_type(config),
                                                           config.parser_mode, config.lower_case,
                                                           config.stop_words));
}

class BuiltinAnalyzerProvider final : public AnalyzerProvider {
public:
    explicit BuiltinAnalyzerProvider(const InvertedIndexAnalyzerConfig& config)
            : _analyzer(builtin_text_analyzer(config)) {}

    TextAnalyzerPtr get_analyzer() const override { return _analyzer; }

private:
    const TextAnalyzerPtr _analyzer;
};

// The index's char filter in front of an analyzer. The writers and the query side used to apply it
// through the reader they gave the analyzer (create_reader); it now belongs to the analyzer, so
// every consumer reads every value through it, whatever the analyzer: a normalizer index with
// char_filter_pattern "-" writes "A-B" as "a b". It runs the CLucene char filter over the whole
// value until the char filters are native.
class IndexCharFilterStream final : public TextTokenStream {
public:
    IndexCharFilterStream(const CharFilterMap* char_filters, std::unique_ptr<TextTokenStream> in)
            : _char_filters(char_filters), _in(std::move(in)) {}

    void reset(std::string_view text) override {
        // Assigning a result short enough for the string's inline buffer keeps the buffer a long
        // value grew, so release it first when it is above the high water mark.
        release_oversized_scratch(_filtered);
        _filtered = InvertedIndexAnalyzer::filter_characters(*_char_filters, text);
        _in->reset(_filtered);
    }

    bool next_batch(TokenBatch& batch) override { return _in->next_batch(batch); }

    // The filtered copy of the current value.
    size_t reserved_bytes() const override { return _filtered.capacity() + _in->reserved_bytes(); }

    void set_strict_utf8(bool strict) override { _in->set_strict_utf8(strict); }

private:
    const CharFilterMap* const _char_filters;
    const std::unique_ptr<TextTokenStream> _in;
    std::string _filtered;
};

class IndexCharFilterAnalyzer final : public TextAnalyzer {
public:
    IndexCharFilterAnalyzer(CharFilterMap char_filters, TextAnalyzerPtr analyzer)
            : _char_filters(std::move(char_filters)), _analyzer(std::move(analyzer)) {}

    std::unique_ptr<TextTokenStream> create_stream() const override {
        return std::make_unique<IndexCharFilterStream>(&_char_filters, _analyzer->create_stream());
    }

private:
    const CharFilterMap _char_filters;
    const TextAnalyzerPtr _analyzer;
};

// `analyzer` behind the index's char filter, or `analyzer` itself for an index without one.
TextAnalyzerPtr with_index_char_filter(const CharFilterMap& char_filters,
                                       TextAnalyzerPtr analyzer) {
    if (char_filters.empty()) {
        return analyzer;
    }
    return std::make_shared<IndexCharFilterAnalyzer>(char_filters, std::move(analyzer));
}

// A provider whose analyzer is behind the index's char filter; the gram scheme is the inner one.
class IndexCharFilterProvider final : public AnalyzerProvider {
public:
    IndexCharFilterProvider(const CharFilterMap& char_filters, AnalyzerProviderPtr provider)
            : _provider(std::move(provider)),
              _analyzer(with_index_char_filter(char_filters, _provider->get_analyzer())) {}

    TextAnalyzerPtr get_analyzer() const override { return _analyzer; }

    std::optional<gram::GramScheme> gram_scheme() const override {
        return _provider->gram_scheme();
    }

private:
    const AnalyzerProviderPtr _provider;
    const TextAnalyzerPtr _analyzer;
};

// Normalizes text the way a builtin parser normalizes the terms it writes: the IK and kuromoji
// parsers fold full-width ASCII to ASCII, and when lowercasing, the ICU, basic and kuromoji parsers
// lowercase every letter and the other parsers ASCII letters only.
std::string normalize_builtin_term(InvertedIndexParserType parser_type, bool lower_case,
                                   std::string text) {
    switch (parser_type) {
    case InvertedIndexParserType::PARSER_IK:
        CharacterUtil::regularizeString(text, lower_case);
        return text;
    case InvertedIndexParserType::PARSER_KUROMOJI:
        text = kuromoji::cjk_width_normalize(text);
        if (lower_case) {
            unicode_lower(text);
        }
        return text;
    case InvertedIndexParserType::PARSER_ICU:
    case InvertedIndexParserType::PARSER_BASIC:
        if (lower_case) {
            unicode_lower(text);
        }
        return text;
    default:
        return lower_case ? to_lower(text) : text;
    }
}

} // namespace

ReaderPtr InvertedIndexAnalyzer::create_reader(const CharFilterMap& char_filter_map) {
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    if (!char_filter_map.empty()) {
        auto it_type = char_filter_map.find(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);
        if (it_type != char_filter_map.end() &&
            it_type->second == INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE) {
            auto it_pattern = char_filter_map.find(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
            auto it_replacement =
                    char_filter_map.find(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT);
            if (it_pattern != char_filter_map.end() && it_replacement != char_filter_map.end()) {
                reader = std::make_shared<CharReplaceCharFilter>(reader, it_pattern->second,
                                                                 it_replacement->second);
            }
        }
    }
    return reader;
}

std::string InvertedIndexAnalyzer::filter_characters(const CharFilterMap& char_filter_map,
                                                     std::string_view value) {
    if (char_filter_map.empty()) {
        return std::string(value);
    }
    auto reader = create_reader(char_filter_map);
    reader->init(value.data(), static_cast<int32_t>(value.size()), true);
    std::string filtered(reader->size(), '\0');
    reader->readCopy(filtered.data(), 0, static_cast<int32_t>(filtered.size()));
    return filtered;
}

bool InvertedIndexAnalyzer::is_builtin_analyzer(const std::string& analyzer_name) {
    return analyzer_name == INVERTED_INDEX_PARSER_NONE ||
           analyzer_name == INVERTED_INDEX_PARSER_STANDARD ||
           analyzer_name == INVERTED_INDEX_PARSER_UNICODE ||
           analyzer_name == INVERTED_INDEX_PARSER_ENGLISH ||
           analyzer_name == INVERTED_INDEX_PARSER_CHINESE ||
           analyzer_name == INVERTED_INDEX_PARSER_ICU ||
           analyzer_name == INVERTED_INDEX_PARSER_BASIC ||
           analyzer_name == INVERTED_INDEX_PARSER_IK ||
           analyzer_name == INVERTED_INDEX_PARSER_KUROMOJI;
}

namespace {

AnalyzerPtr build_builtin_analyzer(InvertedIndexParserType parser_type,
                                   const std::string& parser_mode, const std::string& lower_case,
                                   const std::string& stop_words) {
    std::shared_ptr<lucene::analysis::Analyzer> analyzer;

    if (parser_type == InvertedIndexParserType::PARSER_STANDARD ||
        parser_type == InvertedIndexParserType::PARSER_UNICODE) {
        analyzer = std::make_shared<lucene::analysis::standard95::StandardAnalyzer>();
    } else if (parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
        analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<char>>();
    } else if (parser_type == InvertedIndexParserType::PARSER_CHINESE) {
        auto chinese_analyzer =
                std::make_shared<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
        chinese_analyzer->initDict(config::inverted_index_dict_path);
        if (parser_mode == INVERTED_INDEX_PARSER_COARSE_GRANULARITY) {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
        } else {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::All);
        }
        analyzer = std::move(chinese_analyzer);
    } else if (parser_type == InvertedIndexParserType::PARSER_ICU) {
        analyzer = std::make_shared<ICUAnalyzer>();
        analyzer->initDict(config::inverted_index_dict_path + "/icu");
    } else if (parser_type == InvertedIndexParserType::PARSER_BASIC) {
        analyzer = std::make_shared<BasicAnalyzer>();
    } else if (parser_type == InvertedIndexParserType::PARSER_IK) {
        auto ik_analyzer = std::make_shared<IKAnalyzer>();
        ik_analyzer->initDict(config::inverted_index_dict_path + "/ik");
        if (parser_mode == INVERTED_INDEX_PARSER_SMART) {
            ik_analyzer->setMode(true);
        } else {
            ik_analyzer->setMode(false);
        }
        analyzer = std::move(ik_analyzer);
    } else if (parser_type == InvertedIndexParserType::PARSER_KUROMOJI) {
        if (!config::enable_kuromoji_analyzer) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "kuromoji analyzer is disabled by default. Set "
                            "enable_kuromoji_analyzer=true in "
                            "be.conf (or via the BE config HTTP API) to enable it.");
        }

        std::string kuromoji_mode = parser_mode;
        if (kuromoji_mode.empty() || kuromoji_mode == INVERTED_INDEX_PARSER_COARSE_GRANULARITY) {
            kuromoji_mode = INVERTED_INDEX_PARSER_KUROMOJI_SEARCH;
        }
        auto kuromoji_analyzer = std::make_shared<KuromojiAnalyzer>();
        kuromoji_analyzer->setMode(kuromoji_mode_from_string(kuromoji_mode));
        kuromoji_analyzer->initDict(config::inverted_index_dict_path + "/kuromoji");
        analyzer = std::move(kuromoji_analyzer);
    } else {
        // default
        analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<char>>();
    }

    // set lowercase
    if (lower_case == INVERTED_INDEX_PARSER_TRUE) {
        analyzer->set_lowercase(true);
    } else if (lower_case == INVERTED_INDEX_PARSER_FALSE) {
        analyzer->set_lowercase(false);
    }

    // set stop words
    if (stop_words == "none") {
        analyzer->set_stopwords(nullptr);
    } else {
        analyzer->set_stopwords(&lucene::analysis::standard95::stop_words);
    }

    return analyzer;
}

} // namespace

AnalyzerPtr InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType parser_type,
                                                           const std::string& parser_mode,
                                                           const std::string& lower_case,
                                                           const std::string& stop_words) {
    try {
        return build_builtin_analyzer(parser_type, parser_mode, lower_case, stop_words);
    } catch (const CLuceneError& e) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "building the {} analyzer failed: {}",
                        inverted_index_parser_type_to_string(parser_type), e.what());
    }
}

TextAnalyzerPtr InvertedIndexAnalyzer::create_analyzer(const InvertedIndexAnalyzerConfig* config) {
    DCHECK(config != nullptr);
    if (config->analyzer_name.empty() || is_builtin_analyzer(config->analyzer_name)) {
        return with_index_char_filter(config->char_filter_map, builtin_text_analyzer(*config));
    }

    auto* index_policy_mgr = doris::ExecEnv::GetInstance()->index_policy_mgr();
    if (index_policy_mgr == nullptr) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "Index policy manager is not initialized");
    }
    return with_index_char_filter(config->char_filter_map,
                                  index_policy_mgr->get_analyzer_by_name(config->analyzer_name));
}

AnalyzerProviderPtr InvertedIndexAnalyzer::create_analyzer_provider(
        const InvertedIndexAnalyzerConfig* config, std::string* resolved_name,
        std::string* legacy_name) {
    DCHECK(config != nullptr);
    if (legacy_name != nullptr) {
        legacy_name->clear();
    }
    AnalyzerProviderPtr provider;
    if (config->analyzer_name.empty() || is_builtin_analyzer(config->analyzer_name)) {
        if (resolved_name != nullptr) {
            *resolved_name = config->analyzer_name;
        }
        provider = std::make_shared<BuiltinAnalyzerProvider>(*config);
    } else {
        auto* index_policy_mgr = doris::ExecEnv::GetInstance()->index_policy_mgr();
        if (index_policy_mgr == nullptr) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "Index policy manager is not initialized");
        }
        provider = index_policy_mgr->get_analyzer_provider_by_name(config->analyzer_name,
                                                                   resolved_name, legacy_name);
    }
    if (config->char_filter_map.empty()) {
        return provider;
    }
    return std::make_shared<IndexCharFilterProvider>(config->char_filter_map, std::move(provider));
}

void InvertedIndexAnalyzer::get_analyse_result(std::string_view text, TextTokenStream& stream,
                                               TokenBatch& batch, std::vector<TermInfo>* out) {
    out->clear();
    stream.reset(text);
    int32_t position = 0;
    while (stream.next_batch(batch)) {
        for (const TokenRef& token : batch.tokens()) {
            // An empty term is dropped together with its increment, as the writers drop it.
            if (token.size != 0) {
                position += token.position_increment;
                out->push_back({.term = std::string(token.term()), .position = position});
            }
        }
    }
    batch.release_oversized();
}

std::vector<TermInfo> InvertedIndexAnalyzer::get_analyse_result(std::string_view text,
                                                                const TextAnalyzer& analyzer) {
    std::vector<TermInfo> terms;
    TokenBatch batch;
    const std::unique_ptr<TextTokenStream> stream = analyzer.create_stream();
    get_analyse_result(text, *stream, batch, &terms);
    return terms;
}

std::vector<TermInfo> InvertedIndexAnalyzer::get_analyse_result(
        std::string_view search_str, const std::map<std::string, std::string>& properties) {
    if (!should_analyzer(properties)) {
        // Keyword index: all strings (including empty) are valid tokens for exact match.
        // Empty string is a valid value in keyword index and should be matchable.
        std::vector<TermInfo> result;
        result.emplace_back(std::string(search_str));
        return result;
    }
    InvertedIndexAnalyzerConfig config;
    config.analyzer_name = get_analyzer_name_from_properties(properties);
    config.parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(properties));
    config.parser_mode = get_parser_mode_string_from_properties(properties);
    config.lower_case = get_parser_lowercase_from_properties(properties);
    config.stop_words = get_parser_stopwords_from_properties(properties);
    config.char_filter_map = get_parser_char_filter_map_from_properties(properties);
    return get_analyse_result(search_str, *create_analyzer(&config));
}

Status InvertedIndexAnalyzer::analyze(std::string_view value, const InvertedIndexAnalyzerCtx* ctx,
                                      const std::map<std::string, std::string>& properties,
                                      std::vector<TermInfo>* out) {
    try {
        if (ctx == nullptr) {
            *out = get_analyse_result(value, properties);
        } else if (!ctx->requires_analysis()) {
            // Every string, the empty one included, is a term of an untokenized index.
            *out = {TermInfo {.term = std::string(value)}};
        } else if (const auto analyzer = ctx->get_analyzer(); analyzer != nullptr) {
            *out = get_analyse_result(value, *analyzer);
        } else {
            *out = get_analyse_result(value, properties);
        }
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>("analyzing '{}' failed: {}",
                                                                       value, e.what());
    }
    return Status::OK();
}

std::string InvertedIndexAnalyzer::normalize(const std::string& value,
                                             const std::map<std::string, std::string>& properties) {
    try {
        return normalize_term(value, properties);
    } catch (const CLuceneError& e) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR, "normalizing '{}' failed: {}",
                        value, e.what());
    }
}

std::string InvertedIndexAnalyzer::normalize_term(
        const std::string& value, const std::map<std::string, std::string>& properties) {
    const std::string analyzer_name = get_analyzer_name_from_properties(properties);
    if (analyzer_name.empty() || is_builtin_analyzer(analyzer_name)) {
        // The index writers read a builtin analyzer's text through the index's character filter.
        const InvertedIndexParserType parser_type = get_inverted_index_parser_type_from_string(
                analyzer_name.empty() ? get_parser_string_from_properties(properties)
                                      : analyzer_name);
        return normalize_builtin_term(
                parser_type,
                get_parser_lowercase_from_properties<true>(properties) ==
                        INVERTED_INDEX_PARSER_TRUE,
                filter_characters(get_parser_char_filter_map_from_properties(properties), value));
    }
    auto* index_policy_mgr = doris::ExecEnv::GetInstance()->index_policy_mgr();
    if (index_policy_mgr == nullptr) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "Index policy manager is not initialized");
    }
    // The writers read a custom analyzer's text through the index's character filter as well,
    // before the analyzer's own character filters.
    const CharFilterMap char_filter_map = get_parser_char_filter_map_from_properties(properties);
    const auto normalizer = index_policy_mgr->get_normalizer_by_name(analyzer_name);
    if (normalizer == nullptr) {
        return filter_characters(char_filter_map, value);
    }
    const auto terms = get_analyse_result(filter_characters(char_filter_map, value), *normalizer);
    return terms.empty() ? std::string() : terms.front().get_single_term();
}

bool InvertedIndexAnalyzer::should_analyzer(const std::map<std::string, std::string>& properties) {
    auto parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(properties));
    auto analyzer_name = get_analyzer_name_from_properties(properties);
    if (!analyzer_name.empty()) {
        return true;
    }
    if (parser_type != InvertedIndexParserType::PARSER_UNKNOWN &&
        parser_type != InvertedIndexParserType::PARSER_NONE) {
        return true;
    }
    return false;
}

} // namespace doris::segment_v2::inverted_index
