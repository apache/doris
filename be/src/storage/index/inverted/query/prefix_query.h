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

#include "storage/index/inverted/query/query.h"

CL_NS_USE(index)

namespace doris::segment_v2 {

class PrefixQuery : public Query {
public:
    PrefixQuery(SearcherPtr searcher, IndexQueryContextPtr context);
    ~PrefixQuery() override = default;

    void add(const InvertedIndexQueryInfo& query_info) override;
    void search(roaring::Roaring& roaring) override;

    // Explicitly qualified: unqualified IndexReader inside doris::segment_v2
    // resolves to doris::segment_v2::IndexReader whenever that type is in
    // scope (e.g. in a unity TU), not to the CL_NS_USE(index) one intended.
    void get_prefix_terms(lucene::index::IndexReader* reader, const std::wstring& field_name,
                          const std::string& prefix, std::vector<std::string>& prefix_terms,
                          int32_t max_expansions = 50);

private:
    SearcherPtr _searcher;
    IndexQueryContextPtr _context;

    UnionTermIterPtr _lead1;
};

} // namespace doris::segment_v2