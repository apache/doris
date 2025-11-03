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

#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/wildcard_query/wildcard_weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class WildcardQuery : public Query {
public:
    WildcardQuery(IndexQueryContextPtr context, std::wstring field, std::string pattern)
            : _context(std::move(context)),
              _field(std::move(field)),
              _pattern(std::move(pattern)) {}
    ~WildcardQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        return std::make_shared<WildcardWeight>(std::move(_context), std::move(_field),
                                                std::move(_pattern), enable_scoring);
    }

private:
    IndexQueryContextPtr _context;

    std::wstring _field;
    std::string _pattern;
};

} // namespace doris::segment_v2::inverted_index::query_v2
