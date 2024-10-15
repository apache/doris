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

class DisjunctionQuery : public Query {
public:
    DisjunctionQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                     const TQueryOptions& query_options);
    ~DisjunctionQuery() override = default;

    void add(const std::wstring& field_name, const std::vector<std::string>& terms) override;
    void search(roaring::Roaring& roaring) override;

private:
    std::shared_ptr<lucene::search::IndexSearcher> _searcher;

    std::wstring _field_name;
    std::vector<std::string> _terms;
};

} // namespace doris::segment_v2