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

#include <string_view>

#include "common/status.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/snii/format/phrase_bigram.h"

namespace doris::snii::query::internal {

// SNII term keys are raw analyzed bytes, without escaping, versioning, or a physical/logical
// key distinction. The sole exception is the internal phrase-bigram namespace starting with
// \x1F: overlapping user terms return INVERTED_INDEX_BYPASS for execution without the index.
inline Status check_term_outside_internal_namespace(std::string_view term) {
    if (format::term_overlaps_internal_namespace(term)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII raw term overlaps an internal term namespace");
    }
    return Status::OK();
}

inline Status check_query_terms_outside_internal_namespace(
        const segment_v2::InvertedIndexQueryInfo& query_info) {
    for (const auto& term_info : query_info.term_infos) {
        DORIS_CHECK(term_info.is_single_term());
        RETURN_IF_ERROR(check_term_outside_internal_namespace(term_info.get_single_term()));
    }
    return Status::OK();
}

inline Status check_enumeration_prefix_outside_internal_namespace(std::string_view prefix) {
    if (format::prefix_overlaps_internal_namespace(prefix)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII raw expansion overlaps an internal term namespace");
    }
    return Status::OK();
}

} // namespace doris::snii::query::internal
