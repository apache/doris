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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

#include "common/check.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "storage/index/query/phrase/position_math.h"
#include "storage/index/query/phrase/position_span.h"

namespace doris::index_query {

ALWAYS_INLINE inline bool contains_two_term_phrase(PhrasePositionSpan left_span,
                                                   PhrasePositionSpan right_span,
                                                   uint32_t right_delta) {
    const uint32_t* left = left_span.first;
    const uint32_t* right = right_span.first;
    if (left == left_span.second || right == right_span.second) {
        return false;
    }
    if (right + 1 == right_span.second) {
        if (*right < right_delta) {
            return false;
        }
        const uint32_t target = *right - right_delta;
        if (*left >= target) {
            return *left == target;
        }
        return std::ranges::binary_search(left + 1, left_span.second, target);
    }
    const uint32_t max_start = std::numeric_limits<uint32_t>::max() - right_delta;
    uint32_t left_value = *left;
    uint32_t right_value = *right;
    while (true) {
        if (left_value > max_start) {
            return false;
        }
        const uint32_t expected = left_value + right_delta;
        if (expected < right_value) {
            const uint32_t target = right_value - right_delta;
            do {
                if (++left == left_span.second) {
                    return false;
                }
                left_value = *left;
            } while (left_value < target);
        } else if (expected == right_value) {
            return true;
        } else {
            do {
                if (++right == right_span.second) {
                    return false;
                }
                right_value = *right;
            } while (right_value < expected);
        }
    }
}

class TwoTermPhraseStartCursor {
public:
    TwoTermPhraseStartCursor(PhrasePositionSpan left_span, PhrasePositionSpan right_span,
                             uint32_t right_delta, uint32_t left_offset)
            : left_(left_span.first),
              left_end_(left_span.second),
              right_(right_span.first),
              right_end_(right_span.second),
              right_delta_(right_delta),
              left_offset_(left_offset),
              max_left_(std::numeric_limits<uint32_t>::max() - right_delta) {}

    bool next(uint32_t* start) {
        DCHECK(start != nullptr);
        while (left_ != left_end_ && right_ != right_end_) {
            if (*left_ > max_left_) {
                return false;
            }
            const uint32_t want = *left_ + right_delta_;
            while (right_ != right_end_ && *right_ < want) {
                ++right_;
            }
            if (right_ == right_end_) {
                return false;
            }
            const uint32_t left_position = *left_++;
            if (*right_ == want && left_position >= left_offset_) {
                *start = left_position - left_offset_;
                return true;
            }
        }
        return false;
    }

private:
    const uint32_t* left_;
    const uint32_t* left_end_;
    const uint32_t* right_;
    const uint32_t* right_end_;
    uint32_t right_delta_;
    uint32_t left_offset_;
    uint32_t max_left_;
};

namespace exact_phrase_matcher_detail {

inline size_t consume_matching_prefix(PhrasePositionSpan& anchor, PhrasePositionSpan& other) {
    if (anchor.first == anchor.second || other.first == other.second) {
        return 0;
    }
    auto [left, right] = std::mismatch(anchor.first, anchor.second, other.first, other.second);
    auto count = static_cast<size_t>(left - anchor.first);
    if (count > 0) {
        const uint32_t last_match = left[-1];
        // The other span may end before the anchor's duplicate positions do.
        while (left != anchor.second && *left == last_match) {
            ++left;
            ++count;
        }
    }
    anchor.first = left;
    other.first = right;
    return count;
}

template <typename Visit>
void visit_position_matches(PhrasePositionSpan anchor, PhrasePositionSpan other, uint32_t delta,
                            Visit visit) {
    const uint32_t max_start = std::numeric_limits<uint32_t>::max() - delta;
    const uint32_t* right = other.first;
    if (right == other.second) {
        return;
    }
    for (const uint32_t* left = anchor.first; left != anchor.second; ++left) {
        if (*left > max_start) {
            return;
        }
        const uint32_t expected = *left + delta;
        while (*right < expected) {
            if (++right == other.second) {
                return;
            }
        }
        visit(*left, *right == expected);
    }
}

} // namespace exact_phrase_matcher_detail

ALWAYS_INLINE inline uint32_t count_two_term_phrase(PhrasePositionSpan left_span,
                                                    PhrasePositionSpan right_span,
                                                    uint32_t right_delta) {
    if (left_span.first == left_span.second || right_span.first == right_span.second) {
        return 0;
    }
    uint32_t frequency = 0;
    if (right_span.first + 1 == right_span.second) {
        if (*right_span.first < right_delta) {
            return 0;
        }
        const uint32_t target = *right_span.first - right_delta;
        const uint32_t* left = left_span.first;
        if (*left < target) {
            left = std::ranges::lower_bound(left + 1, left_span.second, target);
        }
        while (left != left_span.second && *left == target) {
            DCHECK_NE(frequency, std::numeric_limits<uint32_t>::max());
            ++frequency;
            ++left;
        }
        return frequency;
    }
    if (right_delta == 0) {
        const size_t prefix =
                exact_phrase_matcher_detail::consume_matching_prefix(left_span, right_span);
        DCHECK_LE(prefix, std::numeric_limits<uint32_t>::max());
        frequency = static_cast<uint32_t>(prefix);
    }
    exact_phrase_matcher_detail::visit_position_matches(
            left_span, right_span, right_delta, [&](uint32_t, bool matched) {
                DCHECK(!matched || frequency != std::numeric_limits<uint32_t>::max());
                frequency += static_cast<uint32_t>(matched);
            });
    return frequency;
}

template <typename Cost>
size_t select_phrase_verification_pair(size_t clause_count, Cost cost) {
    DORIS_CHECK_GT(clause_count, 1);
    size_t best_left = 0;
    uint64_t best_score = std::numeric_limits<uint64_t>::max();
    for (size_t left = 0; left + 1 < clause_count; ++left) {
        const uint64_t score = static_cast<uint64_t>(cost(left)) + cost(left + 1);
        if (score < best_score) {
            best_score = score;
            best_left = left;
        }
    }
    return best_left;
}

// Checks the selected adjacent pair before loading positions from the other clauses.
class ExactPhraseMatcher {
public:
    ExactPhraseMatcher(std::span<const uint32_t> offsets, size_t pair_left)
            : offsets_(offsets), pair_left_(pair_left), spans_(offsets.size()) {
        DORIS_CHECK_GT(offsets_.size(), 2);
        DORIS_CHECK_LT(pair_left_ + 1, offsets_.size());
    }

    // The loader must keep every returned span valid until match() returns.
    template <typename LoadPositions>
    Status match(LoadPositions load, bool collect_frequency, uint32_t* frequency) {
        DORIS_CHECK(frequency != nullptr);
        *frequency = 0;
        const size_t pair_right = pair_left_ + 1;
        RETURN_IF_ERROR(load(pair_left_, &spans_[pair_left_]));
        RETURN_IF_ERROR(load(pair_right, &spans_[pair_right]));
        TwoTermPhraseStartCursor starts(spans_[pair_left_], spans_[pair_right],
                                        offsets_[pair_right] - offsets_[pair_left_],
                                        offsets_[pair_left_]);
        uint32_t start = 0;
        if (!starts.next(&start)) {
            return Status::OK();
        }
        for (size_t clause = 0; clause < offsets_.size(); ++clause) {
            if (clause != pair_left_ && clause != pair_right) {
                RETURN_IF_ERROR(load(clause, &spans_[clause]));
            }
        }
        if (collect_frequency) {
            *frequency = count_matches(starts, start);
            return Status::OK();
        }
        do {
            if (matches_start(start)) {
                *frequency = 1;
                return Status::OK();
            }
        } while (starts.next(&start));
        return Status::OK();
    }

private:
    bool matches_start(uint32_t start) const {
        for (size_t clause = 0; clause < offsets_.size(); ++clause) {
            if (clause == pair_left_ || clause == pair_left_ + 1) {
                continue;
            }
            uint32_t expected = 0;
            if (!add_position_offset(start, offsets_[clause], &expected) ||
                !std::ranges::binary_search(spans_[clause].first, spans_[clause].second,
                                            expected)) {
                return false;
            }
        }
        return true;
    }

    uint32_t count_matches(TwoTermPhraseStartCursor& starts, uint32_t start) const {
        uint32_t frequency = 0;
        bool has_previous_start = false;
        uint32_t previous_start = 0;
        const uint32_t* first_position = spans_[0].first;
        do {
            if (has_previous_start && start == previous_start) {
                continue;
            }
            has_previous_start = true;
            previous_start = start;
            if (!matches_start(start)) {
                continue;
            }
            uint32_t expected = 0;
            const bool representable = add_position_offset(start, offsets_[0], &expected);
            DCHECK(representable);
            while (first_position != spans_[0].second && *first_position < expected) {
                ++first_position;
            }
            const uint32_t* run_end = first_position;
            while (run_end != spans_[0].second && *run_end == expected) {
                ++run_end;
            }
            const auto multiplicity = static_cast<uint32_t>(run_end - first_position);
            DCHECK_NE(multiplicity, 0);
            DCHECK_LE(frequency, std::numeric_limits<uint32_t>::max() - multiplicity);
            frequency += multiplicity;
            first_position = run_end;
        } while (starts.next(&start));
        return frequency;
    }

    std::span<const uint32_t> offsets_;
    size_t pair_left_;
    std::vector<PhrasePositionSpan> spans_;
};

} // namespace doris::index_query
