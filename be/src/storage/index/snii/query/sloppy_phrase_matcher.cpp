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

#include "storage/index/snii/query/internal/sloppy_phrase_matcher.h"

#include <algorithm>
#include <limits>

#include "common/check.h"

namespace doris::snii::query::internal {

SloppyPhraseMatcher::SloppyPhraseMatcher(std::span<const size_t> phrase_plan_index,
                                         std::span<const uint32_t> position_offsets, uint32_t slop,
                                         bool ordered)
        : phrase_plan_index_(phrase_plan_index.begin(), phrase_plan_index.end()),
          position_offsets_(position_offsets.begin(), position_offsets.end()),
          slop_(slop),
          ordered_(ordered),
          clauses_(phrase_plan_index.size()) {
    DORIS_CHECK_EQ(phrase_plan_index_.size(), position_offsets_.size());
    DORIS_CHECK_GT(phrase_plan_index_.size(), 1);
    DORIS_CHECK_GT(slop_, 0);
    heap_.reserve(phrase_plan_index_.size());
    for (size_t i = 0; i < phrase_plan_index_.size() && !has_repeats_; ++i) {
        for (size_t j = 0; j < i; ++j) {
            has_repeats_ = phrase_plan_index_[i] == phrase_plan_index_[j];
            if (has_repeats_) {
                break;
            }
        }
    }
}

float SloppyPhraseMatcher::match(std::span<const PhrasePositionSpan> positions,
                                 bool collect_frequency) {
    DCHECK_EQ(positions.size(), clauses_.size());
    return ordered_ ? match_ordered(positions, collect_frequency)
                    : match_unordered(positions, collect_frequency);
}

bool SloppyPhraseMatcher::initialize_unordered(std::span<const PhrasePositionSpan> positions) {
    heap_.clear();
    end_ = std::numeric_limits<int64_t>::min();
    for (size_t i = 0; i < clauses_.size(); ++i) {
        Clause& clause = clauses_[i];
        clause.positions = positions[i];
        DCHECK(clause.positions.first != clause.positions.second);
        clause.raw_position = *clause.positions.first;
        clause.next = clause.positions.first + 1;
        clause.adjusted_position = static_cast<int64_t>(clause.raw_position) - position_offsets_[i];
        clause.has_position = true;
    }

    if (has_repeats_) {
        for (size_t i = 0; i < clauses_.size(); ++i) {
            size_t preceding_repeats = 0;
            for (size_t j = 0; j < i; ++j) {
                preceding_repeats += phrase_plan_index_[i] == phrase_plan_index_[j];
            }
            for (size_t repeat = 0; repeat < preceding_repeats; ++repeat) {
                if (!advance_clause(i, false)) {
                    positioned_ = false;
                    return false;
                }
            }
        }
    }

    for (size_t i = 0; i < clauses_.size(); ++i) {
        end_ = std::max(end_, clauses_[i].adjusted_position);
        heap_.push_back(i);
    }
    rebuild_heap();
    positioned_ = true;
    return true;
}

bool SloppyPhraseMatcher::advance_clause(size_t clause_index, bool update_end) {
    Clause& clause = clauses_[clause_index];
    if (clause.next == clause.positions.second) {
        return false;
    }
    clause.raw_position = *clause.next++;
    clause.adjusted_position =
            static_cast<int64_t>(clause.raw_position) - position_offsets_[clause_index];
    if (update_end) {
        end_ = std::max(end_, clause.adjusted_position);
    }
    return true;
}

size_t SloppyPhraseMatcher::collision(size_t clause_index) const {
    const Clause& clause = clauses_[clause_index];
    for (size_t i = 0; i < clauses_.size(); ++i) {
        if (i != clause_index && phrase_plan_index_[i] == phrase_plan_index_[clause_index] &&
            clauses_[i].raw_position == clause.raw_position) {
            return i;
        }
    }
    return clauses_.size();
}

bool SloppyPhraseMatcher::clause_less(size_t left, size_t right) const {
    const Clause& left_clause = clauses_[left];
    const Clause& right_clause = clauses_[right];
    if (left_clause.adjusted_position != right_clause.adjusted_position) {
        return left_clause.adjusted_position < right_clause.adjusted_position;
    }
    if (position_offsets_[left] != position_offsets_[right]) {
        return position_offsets_[left] < position_offsets_[right];
    }
    return left < right;
}

bool SloppyPhraseMatcher::clause_greater(size_t left, size_t right) const {
    return left != right && !clause_less(left, right);
}

bool SloppyPhraseMatcher::advance_repeat_collisions(size_t clause_index) {
    size_t current = clause_index;
    size_t other = collision(current);
    while (other != clauses_.size()) {
        current = clause_less(current, other) ? current : other;
        if (!advance_clause(current, true)) {
            return false;
        }
        other = collision(current);
    }
    rebuild_heap();
    return true;
}

void SloppyPhraseMatcher::rebuild_heap() {
    const auto greater = [this](size_t left, size_t right) { return clause_greater(left, right); };
    std::make_heap(heap_.begin(), heap_.end(), greater);
}

size_t SloppyPhraseMatcher::pop_heap() {
    const auto greater = [this](size_t left, size_t right) { return clause_greater(left, right); };
    std::pop_heap(heap_.begin(), heap_.end(), greater);
    const size_t result = heap_.back();
    heap_.pop_back();
    return result;
}

void SloppyPhraseMatcher::push_heap(size_t clause) {
    const auto greater = [this](size_t left, size_t right) { return clause_greater(left, right); };
    heap_.push_back(clause);
    std::push_heap(heap_.begin(), heap_.end(), greater);
}

bool SloppyPhraseMatcher::next_unordered_match(uint64_t* match_width) {
    if (!positioned_ || heap_.size() < 2) {
        return false;
    }
    size_t clause = pop_heap();
    *match_width = static_cast<uint64_t>(end_ - clauses_[clause].adjusted_position);
    int64_t next_position = clauses_[heap_.front()].adjusted_position;
    while (advance_clause(clause, true)) {
        if (has_repeats_ && !advance_repeat_collisions(clause)) {
            break;
        }
        if (clauses_[clause].adjusted_position > next_position) {
            push_heap(clause);
            if (*match_width <= slop_) {
                return true;
            }
            clause = pop_heap();
            next_position = clauses_[heap_.front()].adjusted_position;
            *match_width = static_cast<uint64_t>(end_ - clauses_[clause].adjusted_position);
        } else {
            *match_width = std::min(
                    *match_width, static_cast<uint64_t>(end_ - clauses_[clause].adjusted_position));
        }
    }
    positioned_ = false;
    return *match_width <= slop_;
}

float SloppyPhraseMatcher::match_unordered(std::span<const PhrasePositionSpan> positions,
                                           bool collect_frequency) {
    if (!initialize_unordered(positions)) {
        return 0.0F;
    }
    float frequency = 0.0F;
    uint64_t match_width = 0;
    while (next_unordered_match(&match_width)) {
        if (!collect_frequency) {
            return 1.0F;
        }
        frequency += 1.0F / (1.0F + static_cast<float>(match_width));
    }
    return frequency;
}

bool SloppyPhraseMatcher::advance_ordered_to(size_t clause_index, int64_t target) {
    Clause& clause = clauses_[clause_index];
    while (!clause.has_position || static_cast<int64_t>(clause.raw_position) < target) {
        if (clause.next == clause.positions.second) {
            return false;
        }
        clause.raw_position = *clause.next++;
        clause.has_position = true;
    }
    return true;
}

float SloppyPhraseMatcher::match_ordered(std::span<const PhrasePositionSpan> positions,
                                         bool collect_frequency) {
    for (size_t i = 0; i < clauses_.size(); ++i) {
        clauses_[i].positions = positions[i];
        clauses_[i].next = positions[i].first;
        clauses_[i].has_position = false;
    }

    float frequency = 0.0F;
    Clause& first = clauses_.front();
    while (first.next != first.positions.second) {
        first.raw_position = *first.next++;
        first.has_position = true;
        int64_t previous_start =
                static_cast<int64_t>(first.raw_position) - position_offsets_.front();
        uint64_t match_width = 0;
        bool all_terms_positioned = true;
        for (size_t i = 1; i < clauses_.size(); ++i) {
            const int64_t target = previous_start + position_offsets_[i];
            if (!advance_ordered_to(i, target)) {
                all_terms_positioned = false;
                break;
            }
            const int64_t current_start =
                    static_cast<int64_t>(clauses_[i].raw_position) - position_offsets_[i];
            DCHECK_GE(current_start, previous_start);
            match_width += static_cast<uint64_t>(current_start - previous_start);
            if (match_width > slop_) {
                all_terms_positioned = false;
                break;
            }
            previous_start = current_start;
        }
        if (all_terms_positioned) {
            if (!collect_frequency) {
                return 1.0F;
            }
            frequency += 1.0F / (1.0F + static_cast<float>(match_width));
        }
    }
    return frequency;
}

} // namespace doris::snii::query::internal
