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

#include "olap/inverted_index_parser.h"
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
class InvertedIndexAnalyzer {
public:
    static std::unique_ptr<lucene::util::Reader> create_reader(CharFilterMap& char_filter_map);

    static std::unique_ptr<lucene::analysis::Analyzer> create_analyzer(
            const InvertedIndexCtx* inverted_index_ctx);

    static std::vector<std::string> get_analyse_result(lucene::util::Reader* reader,
                                                       lucene::analysis::Analyzer* analyzer,
                                                       const std::string& field_name,
                                                       InvertedIndexQueryType query_type,
                                                       bool drop_duplicates = true);
};
} // namespace doris::segment_v2::inverted_index