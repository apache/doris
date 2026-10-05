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

#include <cstddef>
#include <cstdint>
#include <span>

#include "common/check.h"
#include "common/status.h"
#include "storage/index/query/phrase/exact_phrase_matcher.h"
#include "storage/index/query/phrase/position_math.h"
#include "storage/index/query/phrase/position_span.h"

namespace doris::index_query {
namespace exact_phrase_stream_matcher_detail {

template <typename Cursor>
Status finish_document(std::span<Cursor> cursors, std::span<const size_t> phrase_plan_index) {
    Status first_error;
    for (size_t cursor_index : phrase_plan_index) {
        const Status status = cursors[cursor_index].finish_doc();
        if (!status.ok() && first_error.ok()) {
            first_error = status;
        }
    }
    return first_error;
}

template <typename Cursor>
Status seek_document(std::span<Cursor> cursors, std::span<const size_t> phrase_plan_index,
                     uint32_t docid) {
    for (size_t cursor_index : phrase_plan_index) {
        RETURN_IF_ERROR(cursors[cursor_index].seek(docid));
    }
    return Status::OK();
}

} // namespace exact_phrase_stream_matcher_detail

template <typename Cursor>
void validate_exact_phrase_stream_inputs(std::span<Cursor> cursors,
                                         std::span<const size_t> phrase_plan_index,
                                         std::span<const uint32_t> position_offsets) {
    DORIS_CHECK_GT(phrase_plan_index.size(), 1);
    DORIS_CHECK_EQ(phrase_plan_index.size(), position_offsets.size());
    for (size_t clause = 0; clause < phrase_plan_index.size(); ++clause) {
        DORIS_CHECK_LT(phrase_plan_index[clause], cursors.size());
        if (clause != 0) {
            DORIS_CHECK_LT(position_offsets[clause - 1], position_offsets[clause]);
        }
        for (size_t preceding = 0; preceding < clause; ++preceding) {
            DORIS_CHECK_NE(phrase_plan_index[preceding], phrase_plan_index[clause]);
        }
    }
}

// Cursors advance within one document: advance_to(target) gives the first position at or after
// target and stays on it, so a later call may give it again, whole(span) gives the document's
// remaining positions when the cursor already holds all of them, and finish_doc() validates
// skipped data. Every referenced cursor is finished before a successful match returns.
template <typename Cursor>
Status match_exact_phrase_positions(std::span<Cursor> cursors,
                                    std::span<const size_t> phrase_plan_index,
                                    std::span<const uint32_t> position_offsets, bool* matched) {
    DCHECK(matched != nullptr);

    *matched = false;

    Cursor& lead = cursors[phrase_plan_index.front()];
    // Two clauses held whole check with the block kernel.
    PhrasePositionSpan lead_span;
    PhrasePositionSpan other_span;
    if (phrase_plan_index.size() == 2 && lead.whole(&lead_span) &&
        cursors[phrase_plan_index[1]].whole(&other_span)) {
        *matched = contains_two_term_phrase(lead_span, other_span,
                                            position_offsets[1] - position_offsets[0]);
        return exact_phrase_stream_matcher_detail::finish_document(cursors, phrase_plan_index);
    }
    uint32_t lead_position = 0;
    bool available = false;
    RETURN_IF_ERROR(lead.advance_to(0, &lead_position, &available));
    // Each clause is checked against the lead's position in turn; a clause past it moves the
    // lead up and starts the checks over.
    size_t clause = 1;
    while (available && clause < phrase_plan_index.size()) {
        const uint32_t offset = position_offsets[clause] - position_offsets.front();
        uint32_t expected = 0;
        if (!add_position_offset(lead_position, offset, &expected)) {
            break;
        }
        uint32_t clause_position = 0;
        RETURN_IF_ERROR(cursors[phrase_plan_index[clause]].advance_to(expected, &clause_position,
                                                                      &available));
        if (available && clause_position != expected) {
            RETURN_IF_ERROR(lead.advance_to(clause_position - offset, &lead_position, &available));
            clause = 1;
            continue;
        }
        ++clause;
    }
    *matched = available && clause == phrase_plan_index.size();
    return exact_phrase_stream_matcher_detail::finish_document(cursors, phrase_plan_index);
}

// Opens the document before matching its buffered positions.
template <typename Cursor>
Status match_exact_phrase_document(std::span<Cursor> cursors,
                                   std::span<const size_t> phrase_plan_index,
                                   std::span<const uint32_t> position_offsets, uint32_t docid,
                                   bool* matched) {
    RETURN_IF_ERROR(
            exact_phrase_stream_matcher_detail::seek_document(cursors, phrase_plan_index, docid));
    return match_exact_phrase_positions(cursors, phrase_plan_index, position_offsets, matched);
}

} // namespace doris::index_query
