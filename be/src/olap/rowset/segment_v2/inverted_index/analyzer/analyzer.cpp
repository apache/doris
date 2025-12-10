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

#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"

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
#include "olap/rowset/segment_v2/inverted_index/analyzer/basic/basic_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/icu/icu_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/IKAnalyzer.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_replace_char_filter_factory.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

ReaderPtr InvertedIndexAnalyzer::create_reader(CharFilterMap& char_filter_map) {
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    if (!char_filter_map.empty()) {
        if (char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE] ==
            INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE) {
            reader = std::make_shared<CharReplaceCharFilter>(
                    reader, char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN],
                    char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT]);
        }
    }
    return reader;
}

bool InvertedIndexAnalyzer::is_builtin_analyzer(const std::string& analyzer_name) {
    return analyzer_name == INVERTED_INDEX_PARSER_NONE ||
           analyzer_name == INVERTED_INDEX_PARSER_STANDARD ||
           analyzer_name == INVERTED_INDEX_PARSER_UNICODE ||
           analyzer_name == INVERTED_INDEX_PARSER_ENGLISH ||
           analyzer_name == INVERTED_INDEX_PARSER_CHINESE ||
           analyzer_name == INVERTED_INDEX_PARSER_ICU ||
           analyzer_name == INVERTED_INDEX_PARSER_BASIC ||
           analyzer_name == INVERTED_INDEX_PARSER_IK;
}

AnalyzerPtr InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType parser_type,
                                                           const std::string& parser_mode,
                                                           const std::string& lower_case,
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

std::shared_ptr<lucene::analysis::Analyzer> InvertedIndexAnalyzer::create_analyzer(
        const InvertedIndexCtx* inverted_index_ctx) {
    const std::string& analyzer_name = inverted_index_ctx->custom_analyzer;
    if (analyzer_name.empty()) {
        return create_builtin_analyzer(
                inverted_index_ctx->parser_type, inverted_index_ctx->parser_mode,
                inverted_index_ctx->lower_case, inverted_index_ctx->stop_words);
    }

    if (is_builtin_analyzer(analyzer_name)) {
        InvertedIndexParserType parser_type =
                get_inverted_index_parser_type_from_string(analyzer_name);
        return create_builtin_analyzer(parser_type, inverted_index_ctx->parser_mode,
                                       inverted_index_ctx->lower_case,
                                       inverted_index_ctx->stop_words);
    }

    auto* index_policy_mgr = doris::ExecEnv::GetInstance()->index_policy_mgr();
    if (!index_policy_mgr) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "Index policy manager is not initialized");
    }

    return index_policy_mgr->get_policy_by_name(analyzer_name);
}

std::vector<TermInfo> InvertedIndexAnalyzer::get_analyse_result(
        ReaderPtr reader, lucene::analysis::Analyzer* analyzer) {
    std::vector<TermInfo> analyse_result;

    std::unique_ptr<lucene::analysis::TokenStream> token_stream(analyzer->tokenStream(L"", reader));

    lucene::analysis::Token token;
    int32_t position = 0;
    while (token_stream->next(&token)) {
        if (token.termLength<char>() != 0) {
            TermInfo t;
            t.term = std::string(token.termBuffer<char>(), token.termLength<char>());
            position += token.getPositionIncrement();
            t.position = position;
            analyse_result.emplace_back(std::move(t));
        }
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    return analyse_result;
}

std::vector<TermInfo> InvertedIndexAnalyzer::get_analyse_result(
        const std::string& search_str, const std::map<std::string, std::string>& properties) {
    InvertedIndexCtxSPtr inverted_index_ctx = std::make_shared<InvertedIndexCtx>(
            get_custom_analyzer_string_from_properties(properties),
            get_inverted_index_parser_type_from_string(
                    get_parser_string_from_properties(properties)),
            get_parser_mode_string_from_properties(properties),
            get_parser_phrase_support_string_from_properties(properties),
            get_parser_char_filter_map_from_properties(properties),
            get_parser_lowercase_from_properties(properties),
            get_parser_stopwords_from_properties(properties));
    auto analyzer = create_analyzer(inverted_index_ctx.get());
    inverted_index_ctx->analyzer = analyzer.get();
    auto reader = create_reader(inverted_index_ctx->char_filter_map);
    reader->init(search_str.data(), static_cast<int32_t>(search_str.size()), true);
    return get_analyse_result(reader, analyzer.get());
}

bool InvertedIndexAnalyzer::should_analyzer(const std::map<std::string, std::string>& properties) {
    auto parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(properties));
    auto analyzer_name = get_custom_analyzer_string_from_properties(properties);
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
#include "common/compile_check_end.h"