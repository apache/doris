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

#include "factory.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/roaring_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query.h"

namespace doris::segment_v2::idx_query_v2 {

template <typename... Args>
Result<Node> QueryFactory::create(QueryType query_type, Args&&... args) {
    if constexpr (sizeof...(args) == 1) {
        switch (query_type) {
        case QueryType::ROARING_QUERY:
            return std::make_shared<RoaringQuery>(std::forward<Args>(args)...);
        default:
            return ResultError(Status::InternalError("failed create query: {}", query_type));
        }
    } else if (sizeof...(args) == 3) {
        switch (query_type) {
        case QueryType::TERM_QUERY:
            return std::make_shared<TermQuery>(std::forward<Args>(args)...);
        case QueryType::PHRASE_QUERY:
            // return std::make_shared<PhraseQuery>(std::forward<Args>(args)...);
        default:
            return ResultError(Status::InternalError("failed create query: {}", query_type));
        }
    } else {
        return ResultError(Status::InternalError("Invalid arguments"));
    }
}

} // namespace doris::segment_v2::idx_query_v2