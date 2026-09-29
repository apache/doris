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

#include <string>

#include "storage/index/index_query_context.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// Every document that holds a term the pattern expands to, with a constant score.
class ExpandWeight : public Weight {
public:
    ExpandWeight(IndexQueryContextPtr context, std::wstring field,
                 index_query::TermPatternKind kind, std::string pattern);
    ~ExpandWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& context, const std::string& binding_key) override;

private:
    IndexQueryContextPtr _context;
    std::wstring _field;
    index_query::TermPatternKind _kind;
    std::string _pattern;
};

} // namespace doris::segment_v2::inverted_index::query_v2
