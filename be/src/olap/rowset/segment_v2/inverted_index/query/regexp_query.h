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

#include <hs/hs.h>
#include <re2/re2.h>

#include <optional>

#include "olap/rowset/segment_v2/inverted_index/query/disjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/query.h"

CL_NS_USE(index)
CL_NS_USE(search)

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class RegexpQuery : public Query {
public:
    RegexpQuery(SearcherPtr searcher, IndexQueryContextPtr context);
    ~RegexpQuery() override = default;

    void add(const InvertedIndexQueryInfo& query_info) override;
    void search(roaring::Roaring& roaring) override;

private:
    static std::optional<std::string> get_regex_prefix(const std::string& pattern);

    void collect_matching_terms(const std::wstring& field_name, std::vector<std::string>& terms,
                                hs_database_t* database, hs_scratch_t* scratch,
                                const std::optional<std::string>& prefix);

    SearcherPtr _searcher;
    IndexQueryContextPtr _context;

    int32_t _max_expansions = 50;
    DisjunctionQuery _query;

    friend class RegexpQueryTest;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
