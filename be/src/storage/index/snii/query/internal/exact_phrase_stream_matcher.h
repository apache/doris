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
#include "storage/index/snii/query/internal/position_math.h"

namespace doris::snii::query::internal {
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

template <typename Cursor>
Status advance_to(Cursor* cursor, uint32_t target, uint32_t* position, bool* available) {
    do {
        RETURN_IF_ERROR(cursor->next_position(position, available));
    } while (*available && *position < target);
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

template <typename Cursor>
Status match_exact_phrase_document(std::span<Cursor> cursors,
                                   std::span<const size_t> phrase_plan_index,
                                   std::span<const uint32_t> position_offsets, uint32_t docid,
                                   bool* matched) {
    DORIS_CHECK(matched != nullptr);

    *matched = false;
    RETURN_IF_ERROR(
            exact_phrase_stream_matcher_detail::seek_document(cursors, phrase_plan_index, docid));

    Cursor& lead = cursors[phrase_plan_index.front()];
    uint32_t lead_position = 0;
    bool available = false;
    RETURN_IF_ERROR(
            exact_phrase_stream_matcher_detail::advance_to(&lead, 0, &lead_position, &available));
    if (!available) {
        return exact_phrase_stream_matcher_detail::finish_document(cursors, phrase_plan_index);
    }

    const size_t no_retained_clause = phrase_plan_index.size();
    size_t retained_clause = no_retained_clause;
    uint32_t retained_position = 0;
    while (true) {
        bool restart = false;
        for (size_t clause = 1; clause < phrase_plan_index.size(); ++clause) {
            const uint32_t offset = position_offsets[clause] - position_offsets.front();
            uint32_t expected_position = 0;
            if (!add_position_offset(lead_position, offset, &expected_position)) {
                return exact_phrase_stream_matcher_detail::finish_document(cursors,
                                                                           phrase_plan_index);
            }

            uint32_t clause_position = 0;
            if (retained_clause == clause) {
                clause_position = retained_position;
                retained_clause = no_retained_clause;
            } else {
                RETURN_IF_ERROR(exact_phrase_stream_matcher_detail::advance_to(
                        &cursors[phrase_plan_index[clause]], expected_position, &clause_position,
                        &available));
                if (!available) {
                    return exact_phrase_stream_matcher_detail::finish_document(cursors,
                                                                               phrase_plan_index);
                }
            }
            if (clause_position == expected_position) {
                continue;
            }

            const uint32_t lead_target = clause_position - offset;
            RETURN_IF_ERROR(exact_phrase_stream_matcher_detail::advance_to(
                    &lead, lead_target, &lead_position, &available));
            if (!available) {
                return exact_phrase_stream_matcher_detail::finish_document(cursors,
                                                                           phrase_plan_index);
            }
            retained_clause = lead_position == lead_target ? clause : no_retained_clause;
            retained_position = clause_position;
            restart = true;
            break;
        }
        if (restart) {
            continue;
        }

        *matched = true;
        return exact_phrase_stream_matcher_detail::finish_document(cursors, phrase_plan_index);
    }
}

} // namespace doris::snii::query::internal
