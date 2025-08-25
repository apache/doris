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
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include "olap/rowset/segment_v2/inverted_index/analyzer/basic/basic_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/icu/icu_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/IKAnalyzer.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "util/runtime_profile.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

std::unique_ptr<lucene::util::Reader> InvertedIndexAnalyzer::create_reader(
        CharFilterMap& char_filter_map) {
    std::unique_ptr<lucene::util::Reader> reader =
            std::make_unique<lucene::util::SStringReader<char>>();
    if (!char_filter_map.empty()) {
        reader = std::unique_ptr<lucene::util::Reader>(CharFilterFactory::create(
                char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE], reader.release(),
                char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN],
                char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT]));
    }
    return reader;
}

std::shared_ptr<lucene::analysis::Analyzer> InvertedIndexAnalyzer::create_analyzer(
        const InvertedIndexCtx* inverted_index_ctx) {
    std::shared_ptr<lucene::analysis::Analyzer> analyzer;
    if (!inverted_index_ctx->custom_analyzer.empty()) {
        auto index_policy_mgr = doris::ExecEnv::GetInstance()->index_policy_mgr();
        if (!index_policy_mgr) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "index policy mgr is not initialized");
        }
        analyzer = index_policy_mgr->get_policy_by_name(inverted_index_ctx->custom_analyzer);
    } else {
        auto analyser_type = inverted_index_ctx->parser_type;
        if (analyser_type == InvertedIndexParserType::PARSER_STANDARD ||
            analyser_type == InvertedIndexParserType::PARSER_UNICODE) {
            analyzer = std::make_shared<lucene::analysis::standard95::StandardAnalyzer>();
        } else if (analyser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<char>>();
        } else if (analyser_type == InvertedIndexParserType::PARSER_CHINESE) {
            auto chinese_analyzer =
                    std::make_shared<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
            chinese_analyzer->initDict(config::inverted_index_dict_path);
            auto mode = inverted_index_ctx->parser_mode;
            if (mode == INVERTED_INDEX_PARSER_COARSE_GRANULARITY) {
                chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
            } else {
                chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::All);
            }
            analyzer = std::move(chinese_analyzer);
        } else if (analyser_type == InvertedIndexParserType::PARSER_ICU) {
            analyzer = std::make_shared<ICUAnalyzer>();
            analyzer->initDict(config::inverted_index_dict_path + "/icu");
        } else if (analyser_type == InvertedIndexParserType::PARSER_BASIC) {
            analyzer = std::make_shared<BasicAnalyzer>();
        } else if (analyser_type == InvertedIndexParserType::PARSER_IK) {
            auto ik_analyzer = std::make_shared<IKAnalyzer>();
            ik_analyzer->initDict(config::inverted_index_dict_path + "/ik");
            auto mode = inverted_index_ctx->parser_mode;
            if (mode == INVERTED_INDEX_PARSER_SMART) {
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
        auto lowercase = inverted_index_ctx->lower_case;
        if (lowercase == INVERTED_INDEX_PARSER_TRUE) {
            analyzer->set_lowercase(true);
        } else if (lowercase == INVERTED_INDEX_PARSER_FALSE) {
            analyzer->set_lowercase(false);
        }
        // set stop words
        auto stop_words = inverted_index_ctx->stop_words;
        if (stop_words == "none") {
            analyzer->set_stopwords(nullptr);
        } else {
            analyzer->set_stopwords(&lucene::analysis::standard95::stop_words);
        }
    }
    return analyzer;
}

std::vector<TermInfo> InvertedIndexAnalyzer::get_analyse_result(
        lucene::util::Reader* reader, lucene::analysis::Analyzer* analyzer) {
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
    return get_analyse_result(reader.get(), analyzer.get());
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