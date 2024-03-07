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

#include "olap/rowset/segment_v2/inverted_index/query/conjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/disjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_edge_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_prefix_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/regexp_query.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"

namespace doris::segment_v2 {

class QueryFactory {
public:
    template <typename... Args>
    static std::unique_ptr<Query> create(InvertedIndexQueryType query_type, Args&&... args) {
        switch (query_type) {
        case InvertedIndexQueryType::MATCH_ANY_QUERY:
            return std::make_unique<DisjunctionQuery>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::EQUAL_QUERY:
        case InvertedIndexQueryType::MATCH_ALL_QUERY:
            return std::make_unique<ConjunctionQuery>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
            return std::make_unique<PhraseQuery>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
            return std::make_unique<PhrasePrefixQuery>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_REGEXP_QUERY:
            return std::make_unique<RegexpQuery>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY:
            return std::make_unique<PhraseEdgeQuery>(std::forward<Args>(args)...);
        default:
            return nullptr;
        }
    }
};

} // namespace doris::segment_v2