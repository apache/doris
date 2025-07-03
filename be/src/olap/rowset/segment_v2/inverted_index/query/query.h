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
#include <gen_cpp/PaloInternalService_types.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "olap/rowset/segment_v2/inverted_index/util/docid_set_iterator.h"
#include "roaring/roaring.hh"

CL_NS_USE(index)
CL_NS_USE(search)
CL_NS_USE(util)

namespace doris::segment_v2 {

class Query {
public:
    virtual ~Query() = default;

    // a unified data preparation interface that provides the field names to be queried and the terms for the query.
    // @param field_name The name of the field within the data source to search against.
    // @param terms a vector of tokenized strings that represent the search terms.
    virtual void add(const InvertedIndexQueryInfo& query_info) = 0;

    // a unified query interface for retrieving the ids obtained from the search.
    // @param roaring a Roaring bitmap to be populated with the search results,
    virtual void search(roaring::Roaring& roaring) = 0;
};

} // namespace doris::segment_v2