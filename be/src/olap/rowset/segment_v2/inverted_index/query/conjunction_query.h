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

CL_NS_USE(index)
CL_NS_USE(search)

namespace doris::segment_v2 {

class ConjunctionQuery : public Query {
public:
    ConjunctionQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                     const TQueryOptions& query_options);
    ~ConjunctionQuery() override;

    void add(const std::wstring& field_name, const std::vector<std::string>& terms) override;
    void search(roaring::Roaring& roaring) override;

private:
    void search_by_bitmap(roaring::Roaring& roaring);
    void search_by_skiplist(roaring::Roaring& roaring);

    int32_t do_next(int32_t doc);

public:
    std::shared_ptr<lucene::search::IndexSearcher> _searcher;

    IndexVersion _index_version = IndexVersion::kV0;
    int32_t _conjunction_ratio = 1000;
    bool _use_skip = false;

    TermIterator _lead1;
    TermIterator _lead2;
    std::vector<TermIterator> _others;

    std::vector<Term*> _terms;
    std::vector<TermDocs*> _term_docs;
};

} // namespace doris::segment_v2