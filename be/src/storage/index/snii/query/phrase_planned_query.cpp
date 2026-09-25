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

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/check.h"
#include "roaring/roaring.hh"
#include "storage/index/query/docid_set_ops.h"
#include "storage/index/query/phrase/position_math.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"
#include "storage/index/snii/query/internal/phrase_query_split.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/internal/resolved_phrase_plan.h"
#include "storage/index/snii/query/internal/term_expansion.h"
#include "storage/index/snii/query/phrase_prx_validation.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/phrase_verify_timer.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "util/debug_points.h"

namespace doris::snii::query::phrase_impl {

using query::internal::DocidChunk;
using query::internal::DocidSource;
using query::internal::ResolvedQueryTerm;
using query::internal::TermPlan;
using reader::LogicalIndexReader;
using internal::PhraseVerifyTimer;

namespace {

// Appends the terms `text` expands to as a `kind` pattern, at most `max_expansions` of them when
// that is positive. The visitor range-seeks past typed internal namespaces before counting and
// decodes escaped physical keys before matching.
Status expand_resolved_terms(const LogicalIndexReader& idx, index_query::TermPatternKind kind,
                             const std::string& text, int32_t max_expansions,
                             std::vector<ResolvedQueryTerm>* terms) {
    index_query::TermPattern pattern;
    RETURN_IF_ERROR(index_query::TermPattern::create(kind, text, &pattern));
    return internal::visit_expanded_plain_terms(
            idx, pattern,
            [terms](LogicalIndexReader::PrefixHit&& hit, bool*) {
                terms->push_back(ResolvedQueryTerm {.entry = std::move(hit.entry),
                                                    .frq_base = hit.frq_base,
                                                    .prx_base = hit.prx_base});
                return Status::OK();
            },
            max_expansions);
}

} // namespace

Status phrase_query_impl(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                         std::vector<uint32_t>* const docids,
                         format::PrxDecodeContext* decode_context,
                         std::vector<PhraseMatch>* matches, const PhraseQueryOptions& options) {
    if (docids == nullptr && matches == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("phrase_query: null out");
    }
    if (docids != nullptr) {
        docids->clear();
    }
    if (matches != nullptr) {
        matches->clear();
    }
    if (terms.empty()) {
        return Status::OK();
    }
    if (terms.size() == 1) {
        DORIS_CHECK(matches == nullptr);
        RETURN_IF_ERROR(term_query(idx, terms.front(), docids));
        if (options.candidates != nullptr) {
            retain_candidates(*options.candidates, docids);
        }
        return Status::OK();
    }
    if (!idx.has_positions()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "phrase_query: index has no positions");
    }
    io::BatchRangeFetcher round1(idx.reader());
    const PhraseTermMapping mapping = build_phrase_term_mapping(terms);
    std::vector<TermPlan> plans;
    bool all_present = false;
    RETURN_IF_ERROR(internal::plan_terms(idx, mapping.unique_terms, &round1, &plans, &all_present,
                                         /*need_positions=*/false));
    if (!all_present) {
        return Status::OK();
    }
    return execute_phrase_plans(idx, &round1, &plans, mapping.phrase_plan_index, docids,
                                decode_context, matches, options);
}

Status phrase_prefix_query_impl(const LogicalIndexReader& idx,
                                const std::vector<std::string>& terms,
                                std::vector<uint32_t>* const docids, int32_t max_expansions,
                                format::PrxDecodeContext* decode_context,
                                std::vector<PhraseMatch>* matches,
                                const roaring::Roaring* candidates) {
    if (docids == nullptr && matches == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("phrase_prefix_query: null out");
    }
    if (docids != nullptr) {
        docids->clear();
    }
    if (matches != nullptr) {
        matches->clear();
    }
    if (terms.empty()) {
        return Status::OK();
    }
    if (terms.size() == 1) {
        DORIS_CHECK(matches == nullptr);
        RETURN_IF_ERROR(prefix_query(idx, terms.front(), docids, max_expansions));
        if (candidates != nullptr) {
            retain_candidates(*candidates, docids);
        }
        return Status::OK();
    }
    const std::vector<std::string> exact(terms.begin(), terms.end() - 1);
    for (const std::string& term : exact) {
        RETURN_IF_ERROR(internal::check_term_outside_internal_namespace(term));
    }
    std::vector<ResolvedQueryTerm> exact_terms;
    bool all_present = false;
    RETURN_IF_ERROR(internal::resolve_all_query_terms(idx, exact, &exact_terms, &all_present));
    if (!all_present) {
        return Status::OK();
    }

    std::vector<ResolvedQueryTerm> tail_terms;
    RETURN_IF_ERROR(expand_resolved_terms(idx, index_query::TermPatternKind::kPrefix, terms.back(),
                                          max_expansions, &tail_terms));
    if (tail_terms.empty()) {
        return Status::OK();
    }
    auto exact_plan = build_resolved_phrase_plan(std::move(exact_terms));
    DORIS_CHECK_LE(terms.size() - 1, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    return execute_resolved_phrase_prefix_terms(idx, std::move(exact_plan), std::move(tail_terms),
                                                static_cast<uint32_t>(terms.size() - 1), docids,
                                                decode_context, matches, candidates);
}

Status phrase_edge_query_impl(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                              std::vector<uint32_t>* const docids, int32_t max_expansions,
                              format::PrxDecodeContext* decode_context,
                              const roaring::Roaring* candidates) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("phrase_edge_query: null out");
    }
    docids->clear();
    if (terms.empty()) {
        return Status::OK();
    }
    if (terms.size() == 1) {
        index_query::TermPattern pattern;
        RETURN_IF_ERROR(index_query::TermPattern::create(index_query::TermPatternKind::kContains,
                                                         terms.front(), &pattern));
        index_query::VectorDocIdSink sink(*docids);
        RETURN_IF_ERROR(internal::emit_expanded_docid_union(
                idx, pattern, &sink,
                index_query::expansion_limit(index_query::TermPatternKind::kContains,
                                             max_expansions)));
        if (candidates != nullptr) {
            retain_candidates(*candidates, docids);
        }
        return Status::OK();
    }
    const std::vector<std::string> middle(terms.begin() + 1, terms.end() - 1);
    for (const std::string& term : middle) {
        RETURN_IF_ERROR(internal::check_term_outside_internal_namespace(term));
    }
    std::vector<ResolvedQueryTerm> middle_terms;
    bool all_present = false;
    RETURN_IF_ERROR(internal::resolve_all_query_terms(idx, middle, &middle_terms, &all_present));
    if (!all_present) {
        return Status::OK();
    }
    std::vector<ResolvedQueryTerm> heads;
    RETURN_IF_ERROR(expand_resolved_terms(idx, index_query::TermPatternKind::kSuffix, terms.front(),
                                          max_expansions, &heads));
    std::vector<ResolvedQueryTerm> tails;
    RETURN_IF_ERROR(expand_resolved_terms(idx, index_query::TermPatternKind::kPrefix, terms.back(),
                                          max_expansions, &tails));
    if (heads.empty() || tails.empty()) {
        return Status::OK();
    }
    // Each expanded head leads its own phrase prefix, and a row matches when any of them does.
    roaring::Roaring matched;
    for (ResolvedQueryTerm& head : heads) {
        std::vector<ResolvedQueryTerm> leading {std::move(head)};
        leading.insert(leading.end(), middle_terms.begin(), middle_terms.end());
        std::vector<uint32_t> head_docids;
        RETURN_IF_ERROR(execute_resolved_phrase_prefix_terms(
                idx, build_resolved_phrase_plan(std::move(leading)), tails,
                static_cast<uint32_t>(terms.size() - 1), &head_docids, decode_context, nullptr,
                candidates));
        matched.addMany(head_docids.size(), head_docids.data());
    }
    docids->resize(matched.cardinality());
    matched.toUint32Array(docids->data());
    return Status::OK();
}

} // namespace doris::snii::query::phrase_impl
