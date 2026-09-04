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
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_set_ops.h"
#include "storage/index/snii/query/internal/docid_union.h"
#include "storage/index/snii/query/internal/phrase_query_split.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/query/internal/position_math.h"
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


size_t position_span_size(std::pair<const uint32_t*, const uint32_t*> span) {
    if (span.first == span.second) {
        return 0;
    }
    DCHECK(span.first != nullptr);
    DCHECK(span.second != nullptr);
    return static_cast<size_t>(span.second - span.first);
}

bool should_use_monotonic_position_scan(std::pair<const uint32_t*, const uint32_t*> anchor_span,
                                        size_t checked_span_size, uint32_t anchor_offset,
                                        uint32_t checked_offset) {
    const uint64_t anchor_count = position_span_size(anchor_span);
    const uint64_t binary_search_upper_bound =
            anchor_count * (static_cast<uint64_t>(std::bit_width(checked_span_size)) + 1);
    const uint64_t monotonic_scan_upper_bound = checked_span_size + 2 * anchor_count + 2;

    // Require a 2x comparison margin before paying even the O(1) validity
    // checks and adding the scan-path branches. This keeps low-TF spans on the
    // simpler binary-search path while retaining the high-TF dense case.
    if (2 * monotonic_scan_upper_bound > binary_search_upper_bound) {
        return false;
    }

    // Scanning is considered only when every anchor yields a representable
    // phrase start and checked-term position. Endpoint checks are sufficient
    // because anchor positions are sorted; invalid boundary shapes stay on the
    // existing per-anchor path without extra binary searches in this gate.
    if (*anchor_span.first < anchor_offset) {
        return false;
    }
    if (checked_offset <= anchor_offset) {
        return true;
    }
    const uint32_t offset_delta = checked_offset - anchor_offset;
    return anchor_span.second[-1] <= std::numeric_limits<uint32_t>::max() - offset_delta;
}


bool entry_has_positions(const format::DictEntry& entry) {
    return entry.kind == format::DictEntryKind::kInline ? !entry.prx_bytes.empty()
                                                        : entry.prx_len != 0;
}







namespace {


} // namespace

namespace {


} // namespace


namespace {

} // namespace


void append_resolved_phrase_clause(ResolvedQueryTerm term, uint32_t position_offset,
                                   internal::ResolvedPhrasePlan* plan) {
    const auto unique = std::ranges::find(plan->unique_terms, term.entry.term,
                                          [](const ResolvedQueryTerm& candidate) {
                                              return std::string_view(candidate.entry.term);
                                          });
    if (unique == plan->unique_terms.end()) {
        plan->phrase_plan_index.push_back(plan->unique_terms.size());
        plan->unique_terms.push_back(std::move(term));
    } else {
        plan->phrase_plan_index.push_back(static_cast<size_t>(unique - plan->unique_terms.begin()));
    }
    plan->position_offsets.push_back(position_offset);
}

internal::ResolvedPhrasePlan build_resolved_phrase_plan(
        std::vector<ResolvedQueryTerm> resolved_terms) {
    internal::ResolvedPhrasePlan plan;
    plan.unique_terms.reserve(resolved_terms.size());
    plan.phrase_plan_index.reserve(resolved_terms.size());
    plan.position_offsets.reserve(resolved_terms.size());
    for (size_t i = 0; i < resolved_terms.size(); ++i) {
        DORIS_CHECK_LE(i, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
        append_resolved_phrase_clause(std::move(resolved_terms[i]), static_cast<uint32_t>(i),
                                      &plan);
    }
    return plan;
}


} // namespace doris::snii::query::phrase_impl
