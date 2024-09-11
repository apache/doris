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
#include <roaring/roaring.hh>
#include <variant>

#include "common/status.h"

namespace doris::segment_v2::idx_query_v2 {

enum class QueryType { TERM_QUERY, PHRASE_QUERY, ROARING_QUERY };

struct QueryInfo {
    std::wstring field_name;
    std::vector<std::string> terms;
    int32_t slop = 0;
};

class Query;
using QueryPtr = std::shared_ptr<Query>;

class Query {
public:
    Query() = default;
    virtual ~Query() = default;
};

} // namespace doris::segment_v2::idx_query_v2