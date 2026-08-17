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

#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
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

Status prove_legacy_raw_has_no_reserved_terms(const reader::LogicalIndexReader& idx,
                                              reader::DictBlockCache* cache) {
    bool exists = false;
    RETURN_IF_ERROR(legacy_raw_prefix_exists(idx, segment_v2::inverted_index::CG_V1_MARKER, &exists,
                                             cache));
    if (!exists) {
        RETURN_IF_ERROR(
                legacy_raw_prefix_exists(idx, format::kPhraseBigramTermMarker, &exists, cache));
    }
    if (exists) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII legacy raw expansion overlaps an existing internal term namespace");
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

    std::string physical_prefix;
    bool representable = false;
    reader::DictBlockCache dict_cache(/*max_entries=*/1);
    const auto version = plain_term_key_version(idx);
    if (version == segment_v2::inverted_index::PlainTermKeyVersion::kLegacyRaw &&
        enum_prefix.empty()) {
        RETURN_IF_ERROR(prove_legacy_raw_has_no_reserved_terms(idx, &dict_cache));
        representable = true;
    } else {
        RETURN_IF_ERROR(
                route_plain_enumeration_prefix(idx, enum_prefix, &physical_prefix, &representable));
    }
    if (!representable) {
        return Status::OK();
    }

    int32_t count = 0;
    bool stop_expansion = false;
    std::string decoded_scratch;
    const auto visit_hit = [&](reader::LogicalIndexReader::PrefixHit&& hit, bool* stop) -> Status {
        std::string_view logical_term;
        if (version != segment_v2::inverted_index::PlainTermKeyVersion::kEscapedV1 ||
            !hit.term.starts_with(segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX)) {
            if (version == segment_v2::inverted_index::PlainTermKeyVersion::kEscapedV1) {
                DCHECK(!segment_v2::inverted_index::is_internal_term_key(hit.term));
            }
            logical_term = hit.term;
            decoded_scratch.clear();
        } else {
            auto decoded = segment_v2::inverted_index::decode_plain_term_view(hit.term, version,
                                                                              &decoded_scratch);
            if (!decoded.has_value()) {
                return std::move(decoded.error());
            }
            logical_term = *decoded;
        }
        if (!matches(logical_term)) {
            return Status::OK();
        }
        if (!decoded_scratch.empty()) {
            hit.term = decoded_scratch;
        }
        bool visitor_stop = false;
        RETURN_IF_ERROR(visitor(std::move(hit), &visitor_stop));
        ++count;
        *stop = visitor_stop || (max_expansions > 0 && count >= max_expansions);
        stop_expansion = *stop;
        return Status::OK();
    };

    if (version == segment_v2::inverted_index::PlainTermKeyVersion::kEscapedV1 &&
        physical_prefix.empty()) {
        RETURN_IF_ERROR(idx.visit_term_range(
                /*lower_inclusive=*/ {}, segment_v2::inverted_index::INTERNAL_TERM_NAMESPACE_BEGIN,
                visit_hit, &dict_cache));
        if (!stop_expansion && (max_expansions <= 0 || count < max_expansions)) {
            RETURN_IF_ERROR(
                    idx.visit_term_range(segment_v2::inverted_index::INTERNAL_TERM_NAMESPACE_END,
                                         /*upper_exclusive=*/std::nullopt, visit_hit, &dict_cache));
        }
    } else {
        RETURN_IF_ERROR(idx.visit_prefix_terms(
                physical_prefix,
                [&](reader::LogicalIndexReader::PrefixHit&& hit, bool* stop) -> Status {
                    if (version == segment_v2::inverted_index::PlainTermKeyVersion::kEscapedV1) {
                        DCHECK(!segment_v2::inverted_index::is_internal_term_key(hit.term));
                    }
                    return visit_hit(std::move(hit), stop);
                },
                &dict_cache));
    }
    return Status::OK();
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
