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

#include "storage/index/snii/snii_bkd_query.h"

#include "common/check.h"

namespace doris::segment_v2 {

Status build_bkd_query_bounds(InvertedIndexQueryType query_type, snii::Slice value,
                              BkdQueryBounds* out) {
    DORIS_CHECK(out != nullptr);
    *out = BkdQueryBounds();
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
        out->lower = value;
        out->lower_inclusive = true;
        out->upper = value;
        out->upper_inclusive = true;
        return Status::OK();
    case InvertedIndexQueryType::LESS_THAN_QUERY:
        out->upper = value;
        out->upper_inclusive = false;
        return Status::OK();
    case InvertedIndexQueryType::LESS_EQUAL_QUERY:
        out->upper = value;
        out->upper_inclusive = true;
        return Status::OK();
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
        out->lower = value;
        out->lower_inclusive = false;
        return Status::OK();
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY:
        out->lower = value;
        out->lower_inclusive = true;
        return Status::OK();
    default:
        break;
    }
    // Refused, not approximated: the caller keeps the predicate and evaluates it
    // normally. Answering an unsupported shape with some nearby interval would be
    // a wrong answer with no error attached.
    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
            "bkd index does not support query type {}", static_cast<int>(query_type));
}

} // namespace doris::segment_v2
