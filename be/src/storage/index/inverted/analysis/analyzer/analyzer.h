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

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/analysis/analyzer/analyzer_provider.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/reader.h"
#include "storage/index/inverted/analysis/text_analyzer.h"
#include "storage/index/inverted/common/inverted_index_query_type.h"
#include "storage/index/inverted/common/query_info.h"
#include "storage/olap_common.h"

namespace lucene {
namespace analysis {
class Analyzer;
}
} // namespace lucene

namespace doris::segment_v2::inverted_index {

// A CLucene analyzer: what the analysis layer builds its TextAnalyzers from until every analyzer
// is native. Consumers get a TextAnalyzer.
using AnalyzerPtr = std::shared_ptr<lucene::analysis::Analyzer>;

class InvertedIndexAnalyzer {
public:
    static ReaderPtr create_reader(const CharFilterMap& char_filter_map);
    // `value` as the index's character filter leaves it.
    static std::string filter_characters(const CharFilterMap& char_filter_map,
                                         std::string_view value);

    static bool is_builtin_analyzer(const std::string& analyzer_name);
    // The CLucene analyzer of a builtin parser. A CLuceneError, such as the one a missing
    // dictionary raises, comes out as an INVERTED_INDEX_ANALYZER_ERROR doris::Exception.
    static AnalyzerPtr create_builtin_analyzer(InvertedIndexParserType parser_type,
                                               const std::string& parser_mode,
                                               const std::string& lower_case,
                                               const std::string& stop_words);
    // The analyzer of `config`, behind the index's char filter when it has one: the writers and
    // the query side read every value through it, whatever the analyzer.
    static TextAnalyzerPtr create_analyzer(const InvertedIndexAnalyzerConfig* config);
    static AnalyzerProviderPtr create_analyzer_provider(const InvertedIndexAnalyzerConfig* config,
                                                        std::string* resolved_name = nullptr,
                                                        std::string* legacy_name = nullptr);

    // The terms of `text` with their positions, for callers that tokenize many values with one
    // stream and one batch. An empty term is dropped together with its position increment.
    static void get_analyse_result(std::string_view text, TextTokenStream& stream,
                                   TokenBatch& batch, std::vector<TermInfo>* out);
    // The terms of one value.
    static std::vector<TermInfo> get_analyse_result(std::string_view text,
                                                    const TextAnalyzer& analyzer);
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

private:
    // normalize() without the conversion of a CLuceneError.
    static std::string normalize_term(const std::string& value,
                                      const std::map<std::string, std::string>& properties);
};

} // namespace doris::segment_v2::inverted_index
