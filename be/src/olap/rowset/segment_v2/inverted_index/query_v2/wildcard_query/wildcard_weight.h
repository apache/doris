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

#include <re2/re2.h>

#include <regex>

#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/regexp_query/regexp_weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class WildcardWeight : public Weight {
public:
    WildcardWeight(IndexQueryContextPtr context, std::wstring field, std::string pattern,
                   bool enable_scoring)
            : _context(std::move(context)),
              _field(std::move(field)),
              _pattern(std::move(pattern)),
              _enable_scoring(enable_scoring) {}

    ~WildcardWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        std::string regex_pattern = wildcard_to_regex(_pattern);
        auto regexp_weight = std::make_shared<RegexpWeight>(
                _context, std::move(_field), std::move(regex_pattern), _enable_scoring);
        return regexp_weight->scorer(ctx, binding_key);
    }

private:
    std::string wildcard_to_regex(const std::string& pattern) {
        std::string escaped = RE2::QuoteMeta(pattern);
        // Replace wildcard characters with regex equivalents
        // * -> .* (zero or more of any character)
        escaped = std::regex_replace(escaped, std::regex(R"(\\\*)"), ".*");
        // ? -> . (exactly one of any character)
        escaped = std::regex_replace(escaped, std::regex(R"(\\\?)"), ".");
        return "^" + escaped + "$";
    }

    IndexQueryContextPtr _context;

    std::wstring _field;
    std::string _pattern;
    bool _enable_scoring = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
