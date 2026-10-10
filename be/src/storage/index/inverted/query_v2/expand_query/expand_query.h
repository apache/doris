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
#include <string>

#include "storage/index/index_query_context.h"
#include "storage/index/inverted/query_v2/expand_query/expand_weight.h"
#include "storage/index/inverted/query_v2/query.h"
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// Matches the documents holding a term that a prefix, glob or regular expression expands to.
class ExpandQuery : public Query {
public:
    ExpandQuery(IndexQueryContextPtr context, std::wstring field, index_query::TermPatternKind kind,
                std::string pattern)
            : _context(std::move(context)),
              _field(std::move(field)),
              _kind(kind),
              _pattern(std::move(pattern)) {}
    ~ExpandQuery() override = default;

    // Scores are constant, so scoring changes nothing.
    WeightPtr weight(bool /*enable_scoring*/) override {
        return std::make_shared<ExpandWeight>(_context, _field, _kind, _pattern);
    }

private:
    IndexQueryContextPtr _context;
    std::wstring _field;
    index_query::TermPatternKind _kind;
    std::string _pattern;
};

} // namespace doris::segment_v2::inverted_index::query_v2
