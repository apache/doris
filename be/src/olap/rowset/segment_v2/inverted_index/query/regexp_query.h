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

class RegexpQuery : public Query {
public:
    RegexpQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                const TQueryOptions& query_options, const io::IOContext* io_ctx);
    ~RegexpQuery() override = default;

    void add(const InvertedIndexQueryInfo& query_info) override;
    void search(roaring::Roaring& roaring) override;

private:
    static std::optional<std::string> get_regex_prefix(const std::string& pattern);

    void collect_matching_terms(const std::wstring& field_name, std::vector<std::string>& terms,
                                hs_database_t* database, hs_scratch_t* scratch,
                                const std::optional<std::string>& prefix);

    std::shared_ptr<lucene::search::IndexSearcher> _searcher;
    const io::IOContext* _io_ctx = nullptr;

    int32_t _max_expansions = 50;
    DisjunctionQuery _query;

    friend class RegexpQueryTest;
};

} // namespace doris::segment_v2
