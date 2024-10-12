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
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"

namespace doris::segment_v2::inverted_index {

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

std::unique_ptr<lucene::analysis::Analyzer> InvertedIndexAnalyzer::create_analyzer(
        const InvertedIndexCtx* inverted_index_ctx) {
    std::unique_ptr<lucene::analysis::Analyzer> analyzer;
    auto analyser_type = inverted_index_ctx->parser_type;
    if (analyser_type == InvertedIndexParserType::PARSER_STANDARD ||
        analyser_type == InvertedIndexParserType::PARSER_UNICODE) {
        analyzer = std::make_unique<lucene::analysis::standard95::StandardAnalyzer>();
    } else if (analyser_type == InvertedIndexParserType::PARSER_ENGLISH) {
        analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
    } else if (analyser_type == InvertedIndexParserType::PARSER_CHINESE) {
        auto chinese_analyzer =
                std::make_unique<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
        chinese_analyzer->initDict(config::inverted_index_dict_path);
        auto mode = inverted_index_ctx->parser_mode;
        if (mode == INVERTED_INDEX_PARSER_COARSE_GRANULARITY) {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
        } else {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::All);
        }
        analyzer = std::move(chinese_analyzer);
    } else {
        // default
        analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
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
    return analyzer;
}

std::vector<std::string> InvertedIndexAnalyzer::get_analyse_result(
        lucene::util::Reader* reader, lucene::analysis::Analyzer* analyzer,
        const std::string& field_name, InvertedIndexQueryType query_type, bool drop_duplicates) {
    std::vector<std::string> analyse_result;

    std::wstring field_ws = StringUtil::string_to_wstring(field_name);
    std::unique_ptr<lucene::analysis::TokenStream> token_stream(
            analyzer->tokenStream(field_ws.c_str(), reader));

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        if (token.termLength<char>() != 0) {
            analyse_result.emplace_back(token.termBuffer<char>(), token.termLength<char>());
        }
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    if (drop_duplicates && (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
                            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY)) {
        std::set<std::string> unrepeated_result(analyse_result.begin(), analyse_result.end());
        analyse_result.assign(unrepeated_result.begin(), unrepeated_result.end());
    }
    return analyse_result;
}

} // namespace doris::segment_v2::inverted_index
