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

#include "olap/rowset/segment_v2/inverted_index/query/query.h"
#include "olap/rowset/segment_v2/inverted_index/query/term_query.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class ConjunctionQuery : public Query {
public:
    ConjunctionQuery() = default;
    ConjunctionQuery(SearcherPtr searcher, IndexQueryContextPtr context);
    ~ConjunctionQuery() override = default;

    void add(const InvertedIndexQueryInfo& query_info) override;
    void search(roaring::Roaring& roaring) override;

private:
    void search_by_bitmap(roaring::Roaring& roaring);
    void search_by_skiplist(roaring::Roaring& roaring);

    int32_t do_next(int32_t doc);

    SearcherPtr _searcher;
    IndexQueryContextPtr _context;

    TermQuery _term_query;

    IndexVersion _index_version = IndexVersion::kV0;
    int32_t _conjunction_ratio = 1000;
    bool _use_skip = false;

    TermIterPtr _lead1;
    TermIterPtr _lead2;
    std::vector<TermIterPtr> _others;
    std::vector<TermIterPtr> _iterators;

    std::vector<SimilarityPtr> _similarities;

    friend class ConjunctionQueryTest;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2