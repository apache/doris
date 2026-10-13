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

#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/analysis/analyzer/analyzer_provider.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/reader.h"
#include "storage/index/inverted/common/inverted_index_query_type.h"
#include "storage/index/inverted/common/query_info.h"
#include "storage/olap_common.h"

namespace lucene {
namespace util {
class Reader;
}
namespace analysis {
class Analyzer;
}
} // namespace lucene

namespace doris::segment_v2::inverted_index {

using AnalyzerPtr = std::shared_ptr<lucene::analysis::Analyzer>;

class InvertedIndexAnalyzer {
public:
    static ReaderPtr create_reader(const CharFilterMap& char_filter_map);
    // `value` as a builtin analyzer reads it, through the index's character filter.
    static std::string filter_characters(const CharFilterMap& char_filter_map,
                                         const std::string& value);

    static bool is_builtin_analyzer(const std::string& analyzer_name);
    static AnalyzerPtr create_builtin_analyzer(InvertedIndexParserType parser_type,
                                               const std::string& parser_mode,
                                               const std::string& lower_case,
                                               const std::string& stop_words);
    static AnalyzerPtr create_analyzer(const InvertedIndexAnalyzerConfig* config);
    static AnalyzerProviderPtr create_analyzer_provider(const InvertedIndexAnalyzerConfig* config,
                                                        std::string* resolved_name = nullptr,
                                                        std::string* legacy_name = nullptr);

    static std::vector<TermInfo> get_analyse_result(ReaderPtr reader,
                                                    lucene::analysis::Analyzer* analyzer);

    static std::vector<TermInfo> get_analyse_result(
            std::string_view search_str, const std::map<std::string, std::string>& properties);

    // Tokenizes a query value: with the analyzer of `ctx` when it has one, as one untouched term
    // when `ctx` does not analyze, and otherwise as the index `properties` describe.
    static Status analyze(std::string_view value, const InvertedIndexAnalyzerCtx* ctx,
                          const std::map<std::string, std::string>& properties,
                          std::vector<TermInfo>* out);

    // Normalizes the literal text of a prefix or a wildcard the way the index normalizes its
    // terms, without splitting it: through the index's character filter, then a builtin parser's
    // own term normalization or the per-character part of a custom analyzer or normalizer.
    static std::string normalize(const std::string& value,
                                 const std::map<std::string, std::string>& properties);

    static bool should_analyzer(const std::map<std::string, std::string>& properties);
};

} // namespace doris::segment_v2::inverted_index
