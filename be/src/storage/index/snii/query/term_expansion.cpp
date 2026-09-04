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

#include "storage/index/snii/query/internal/term_expansion.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/reader/dict_block_cache.h"

namespace doris::snii::query::internal {
namespace {

Status legacy_raw_prefix_exists(const reader::LogicalIndexReader& idx, std::string_view prefix,
                                bool* exists, reader::DictBlockCache* cache) {
    DORIS_CHECK(exists != nullptr);
    *exists = false;
    return idx.visit_prefix_terms(
            prefix,
            [&](reader::LogicalIndexReader::PrefixHit&&, bool* stop) -> Status {
                *exists = true;
                *stop = true;
                return Status::OK();
            },
            cache);
}

// 空前缀会枚举整个词典：段里若存在内部命名空间的词项（phrase-bigram 标记），枚举结果会混入
// 内部词项，此时整条查询绕过 SNII。
Status prove_no_internal_terms(const reader::LogicalIndexReader& idx,
                               reader::DictBlockCache* cache) {
    bool exists = false;
    RETURN_IF_ERROR(legacy_raw_prefix_exists(idx, format::kPhraseBigramTermMarker, &exists, cache));
    if (exists) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII raw expansion overlaps an existing internal term namespace");
    }
    return Status::OK();
}

} // namespace

Status visit_expanded_plain_terms(const reader::LogicalIndexReader& idx,
                                  std::string_view enum_prefix, const TermMatcher& matches,
                                  const reader::LogicalIndexReader::PrefixHitVisitor& visitor,
                                  int32_t max_expansions) {
    if (!matches || !visitor) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "term_expansion: null matcher or visitor");
    }

    reader::DictBlockCache dict_cache(/*max_entries=*/1);
    if (enum_prefix.empty()) {
        RETURN_IF_ERROR(prove_no_internal_terms(idx, &dict_cache));
    } else {
        RETURN_IF_ERROR(check_enumeration_prefix_outside_internal_namespace(enum_prefix));
    }

    int32_t count = 0;
    return idx.visit_prefix_terms(
            enum_prefix,
            [&](reader::LogicalIndexReader::PrefixHit&& hit, bool* stop) -> Status {
                if (!matches(hit.term)) {
                    return Status::OK();
                }
                bool visitor_stop = false;
                RETURN_IF_ERROR(visitor(std::move(hit), &visitor_stop));
                ++count;
                *stop = visitor_stop || (max_expansions > 0 && count >= max_expansions);
                return Status::OK();
            },
            &dict_cache);
}

Status emit_expanded_docid_union(const reader::LogicalIndexReader& idx,
                                 std::string_view enum_prefix, const TermMatcher& matches,
                                 DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("term_expansion: null sink");
    }

    std::vector<ResolvedDocidPosting> postings;
    RETURN_IF_ERROR(visit_expanded_plain_terms(
            idx, enum_prefix, matches,
            [&](reader::LogicalIndexReader::PrefixHit&& hit, bool*) {
                postings.push_back({std::move(hit.entry), hit.frq_base, hit.prx_base});
                return Status::OK();
            },
            max_expansions));
    return emit_docid_union(idx, postings, sink);
}

} // namespace doris::snii::query::internal
