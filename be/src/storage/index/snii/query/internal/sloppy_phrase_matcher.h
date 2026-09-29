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
#include <utility>
#include <vector>

namespace doris::snii::query::internal {

using PhrasePositionSpan = std::pair<const uint32_t*, const uint32_t*>;

// Matches one candidate document at a time against already-decoded position
// spans. Query-shape storage and all scratch buffers are allocated once and
// reused across documents.
class SloppyPhraseMatcher {
public:
    SloppyPhraseMatcher(std::span<const size_t> phrase_plan_index,
                        std::span<const uint32_t> position_offsets, uint32_t slop, bool ordered);

    // Returns 1 for the first match when frequencies are not requested. For
    // scoring, returns the V3 sloppy frequency: sum(1 / (1 + match_width)).
    float match(std::span<const PhrasePositionSpan> positions, bool collect_frequency);

private:
    struct Clause {
        PhrasePositionSpan positions;
        const uint32_t* next = nullptr;
        uint32_t raw_position = 0;
        int64_t adjusted_position = 0;
        bool has_position = false;
    };

    bool initialize_unordered(std::span<const PhrasePositionSpan> positions);
    bool advance_clause(size_t clause, bool update_end);
    bool advance_repeat_collisions(size_t clause);
    size_t collision(size_t clause) const;
    bool clause_less(size_t left, size_t right) const;
    bool clause_greater(size_t left, size_t right) const;
    void rebuild_heap();
    size_t pop_heap();
    void push_heap(size_t clause);
    bool next_unordered_match(uint64_t* match_width);
    float match_unordered(std::span<const PhrasePositionSpan> positions, bool collect_frequency);
    float match_ordered(std::span<const PhrasePositionSpan> positions, bool collect_frequency);
    bool advance_ordered_to(size_t clause, int64_t target);

    std::vector<size_t> phrase_plan_index_;
    std::vector<uint32_t> position_offsets_;
    uint32_t slop_ = 0;
    bool ordered_ = false;
    bool has_repeats_ = false;
    bool positioned_ = false;
    int64_t end_ = 0;
    std::vector<Clause> clauses_;
    std::vector<size_t> heap_;
};

} // namespace doris::snii::query::internal
