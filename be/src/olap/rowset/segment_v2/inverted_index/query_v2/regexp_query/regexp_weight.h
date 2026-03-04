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

#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class RegexpWeight : public Weight {
public:
    RegexpWeight(IndexQueryContextPtr context, std::wstring field, std::string pattern,
                 bool enable_scoring, bool nullable);
    ~RegexpWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& context, const std::string& binding_key) override;

private:
    ScorerPtr regexp_scorer(const QueryExecutionContext& context, const std::string& binding_key);

    std::optional<std::string> get_regex_prefix(const std::string& pattern);
    void collect_matching_terms(const QueryExecutionContext& context,
                                const std::string& binding_key, std::vector<std::wstring>& terms,
                                hs_database_t* database, hs_scratch_t* scratch,
                                const std::optional<std::string>& prefix);

    IndexQueryContextPtr _context;

    std::wstring _field;
    std::string _pattern;
    bool _enable_scoring = false;
    bool _nullable = true;
    // Set to 0 to disable limit (ES has no default limit for prefix queries)
    // The limit prevents collecting too many terms, but can cause incorrect results
    int32_t _max_expansions = 0;
};

} // namespace doris::segment_v2::inverted_index::query_v2
