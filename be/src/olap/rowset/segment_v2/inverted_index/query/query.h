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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/query/TermIterator.h>
#include <CLucene/search/query/TermPositionIterator.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <memory>

#include "common/status.h"
#include "roaring/roaring.hh"

CL_NS_USE(index)
CL_NS_USE(search)
CL_NS_USE(util)

namespace doris::segment_v2 {

struct InvertedIndexQueryInfo {
    std::wstring field_name;
    std::vector<std::string> terms;
    int32_t slop = 0;
    bool ordered = false;
};

class Query {
public:
    virtual ~Query() = default;

    virtual void add(const InvertedIndexQueryInfo& query_info) {
        add(query_info.field_name, query_info.terms);
    }

    // a unified data preparation interface that provides the field names to be queried and the terms for the query.
    // @param field_name The name of the field within the data source to search against.
    // @param terms a vector of tokenized strings that represent the search terms.
    virtual void add(const std::wstring& field_name, const std::vector<std::string>& terms) = 0;

    // a unified query interface for retrieving the ids obtained from the search.
    // @param roaring a Roaring bitmap to be populated with the search results,
    virtual void search(roaring::Roaring& roaring) = 0;
};

} // namespace doris::segment_v2