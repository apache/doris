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

#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/analysis/analyzer/chain_analyzer.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/KuromojiMode.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize.h"
#include "storage/index/inverted/analysis/char_filter/char_filtered_stream.h"
#include "storage/index/inverted/analysis/char_filter/char_replace_char_filter_factory.h"
#include "storage/index/inverted/analysis/gram/gram_scheme.h"
#include "storage/index/inverted/analysis/token_filter/lower_case_filter.h"
#include "storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"
#include "storage/index/inverted/analysis/tokenizer/english/english_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/ik/ik_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/jieba_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/kuromoji/kuromoji_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/standard/uax29_tokenizer.h"

namespace doris::segment_v2::inverted_index {
namespace {

// The parser of a configuration whose analyzer is builtin, named by the parser or by the analyzer.
InvertedIndexParserType builtin_parser_type(const InvertedIndexAnalyzerConfig& config) {
    return config.analyzer_name.empty()
                   ? config.parser_type
                   : get_inverted_index_parser_type_from_string(config.analyzer_name);
}

TextAnalyzerPtr builtin_text_analyzer(const InvertedIndexAnalyzerConfig& config) {
    return InvertedIndexAnalyzer::create_builtin_analyzer(
            builtin_parser_type(config), config.parser_mode, config.lower_case, config.stop_words);
}

class BuiltinAnalyzerProvider final : public AnalyzerProvider {
public:
    explicit BuiltinAnalyzerProvider(const InvertedIndexAnalyzerConfig& config)
            : _analyzer(builtin_text_analyzer(config)) {}

    TextAnalyzerPtr get_analyzer() const override { return _analyzer; }

private:
    const TextAnalyzerPtr _analyzer;
};

// The index's char filter, which a char filter map that is not empty always describes as a
// char_replace of char_filter_pattern by char_filter_replacement.
CharFilterFactoryPtr index_char_filter(const CharFilterMap& char_filter_map) {
    DORIS_CHECK_EQ(char_filter_map.at(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE),
                   INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE);
    return std::make_shared<CharReplaceCharFilterFactory>(
            char_filter_map.at(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN),
            char_filter_map.at(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT));
}

// `analyzer` behind the index's char filter, or `analyzer` itself for an index without one. Every
// consumer reads every value through it, whatever the analyzer: a normalizer index with
// char_filter_pattern "-" writes "A-B" as "a b".
TextAnalyzerPtr with_index_char_filter(const CharFilterMap& char_filter_map,
                                       TextAnalyzerPtr analyzer) {
    if (char_filter_map.empty()) {
        return analyzer;
    }
    return std::make_shared<CharFilteredAnalyzer>(
            std::vector<CharFilterFactoryPtr> {index_char_filter(char_filter_map)},
            std::move(analyzer));
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
        if (lower_case) {
            ascii_lower(text);
        }
        return text;
    }
}

} // namespace

std::string InvertedIndexAnalyzer::filter_characters(const CharFilterMap& char_filter_map,
                                                     std::string_view value) {
    if (char_filter_map.empty()) {
        return std::string(value);
    }
    std::string scratch;
    return std::string(index_char_filter(char_filter_map)->create()->apply(value, scratch));
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

// The lowercasing of a builtin parser: lower_case "true" or "false", and the parser's own default
// when it is empty.
bool builtin_lowercase(const std::string& lower_case, bool parser_default) {
    if (lower_case == INVERTED_INDEX_PARSER_TRUE) {
        return true;
    }
    if (lower_case == INVERTED_INDEX_PARSER_FALSE) {
        return false;
    }
    return parser_default;
}

// The builtin parsers lowercase by default, except chinese; stop_words only applies to the
// standard tokenizer.
TextAnalyzerPtr build_builtin_analyzer(InvertedIndexParserType parser_type,
                                       const std::string& parser_mode,
                                       const std::string& lower_case,
                                       const std::string& stop_words) {
    if (parser_type == InvertedIndexParserType::PARSER_STANDARD ||
        parser_type == InvertedIndexParserType::PARSER_UNICODE) {
        return std::make_shared<ChainAnalyzer>(
                std::vector<CharFilterFactoryPtr> {},
                std::make_shared<Uax29TokenizerFactory>(builtin_lowercase(lower_case, true),
                                                        stop_words != "none"),
                std::vector<TokenFilterFactoryPtr> {});
    }
    if (parser_type == InvertedIndexParserType::PARSER_CHINESE) {
        // The fine mode cuts every dictionary word; coarse_grained mixes the dictionary and the HMM.
        return std::make_shared<ChainAnalyzer>(
                std::vector<CharFilterFactoryPtr> {},
                std::make_shared<JiebaTokenizerFactory>(
                        parser_mode != INVERTED_INDEX_PARSER_COARSE_GRANULARITY,
                        builtin_lowercase(lower_case, false), config::inverted_index_dict_path),
                std::vector<TokenFilterFactoryPtr> {});
    }
    if (parser_type == InvertedIndexParserType::PARSER_ICU) {
        // The tokenizer lowercases its own terms as the lowercase filter did.
        return std::make_shared<ChainAnalyzer>(
                std::vector<CharFilterFactoryPtr> {},
                std::make_shared<ICUTokenizerFactory>(builtin_lowercase(lower_case, true)),
                std::vector<TokenFilterFactoryPtr> {});
    }
    if (parser_type == InvertedIndexParserType::PARSER_BASIC) {
        // The tokenizer lowercases its own tokens, which are all a lowercase filter would change.
        return std::make_shared<ChainAnalyzer>(
                std::vector<CharFilterFactoryPtr> {},
                std::make_shared<BasicTokenizerFactory>(builtin_lowercase(lower_case, true)),
                std::vector<TokenFilterFactoryPtr> {});
    }
    if (parser_type == InvertedIndexParserType::PARSER_IK) {
        // lower_case only applies to the words: IK always matches its dictionary in lowercase.
        auto tokenizer = std::make_shared<IKTokenizerFactory>(
                parser_mode == INVERTED_INDEX_PARSER_SMART, builtin_lowercase(lower_case, true));
        // Loaded here so that a missing dictionary fails the analyzer, not its first stream.
        tokenizer->load_dictionary();
        return std::make_shared<ChainAnalyzer>(std::vector<CharFilterFactoryPtr> {},
                                               std::move(tokenizer),
                                               std::vector<TokenFilterFactoryPtr> {});
    }
    if (parser_type == InvertedIndexParserType::PARSER_KUROMOJI) {
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
        const std::string dict_path = config::inverted_index_dict_path + "/kuromoji";
        const auto* dict = kuromoji::KuromojiDictionary::get_or_load(dict_path);
        if (dict == nullptr) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "kuromoji dictionary could not be loaded from {}; ensure system.bin, "
                            "matrix.bin, chardef.bin and unkdict.bin are present in the BE package",
                            dict_path);
        }
        return std::make_shared<ChainAnalyzer>(std::vector<CharFilterFactoryPtr> {},
                                               std::make_shared<KuromojiTokenizerFactory>(
                                                       kuromoji_mode_from_string(kuromoji_mode),
                                                       builtin_lowercase(lower_case, true), dict),
                                               std::vector<TokenFilterFactoryPtr> {});
    }
    // english, and the default for an unknown parser; neither has stop words.
    return std::make_shared<ChainAnalyzer>(
            std::vector<CharFilterFactoryPtr> {},
            std::make_shared<EnglishTokenizerFactory>(builtin_lowercase(lower_case, true)),
            std::vector<TokenFilterFactoryPtr> {});
}

} // namespace

TextAnalyzerPtr InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType parser_type,
                                                               const std::string& parser_mode,
                                                               const std::string& lower_case,
                                                               const std::string& stop_words) {
    return build_builtin_analyzer(parser_type, parser_mode, lower_case, stop_words);
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
