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
#include <vector>

#include "olap/inverted_index_parser.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index/query/query.h"
#include "olap/rowset/segment_v2/inverted_index/util/reader.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"

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

    static bool is_builtin_analyzer(const std::string& analyzer_name);
    static AnalyzerPtr create_builtin_analyzer(InvertedIndexParserType parser_type,
                                               const std::string& parser_mode,
                                               const std::string& lower_case,
                                               const std::string& stop_words);
    static AnalyzerPtr create_analyzer(const InvertedIndexAnalyzerConfig* config);

    static std::vector<TermInfo> get_analyse_result(ReaderPtr reader,
                                                    lucene::analysis::Analyzer* analyzer);

    static std::vector<TermInfo> get_analyse_result(
            const std::string& search_str, const std::map<std::string, std::string>& properties);

    static bool should_analyzer(const std::map<std::string, std::string>& properties);
};

} // namespace doris::segment_v2::inverted_index