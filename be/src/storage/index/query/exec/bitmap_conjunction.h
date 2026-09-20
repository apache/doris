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
#include <optional>
#include <span>
#include <utility>

#include "common/check.h"
#include "core/custom_allocator.h"
#include "storage/index/query/boolean/truth_set.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::index_query {

struct ConjunctionPostings {
    BlockDocSet* docs;
    const roaring::Roaring* null_rows;
};

struct PostingStatistics {
    uint32_t doc;
    uint32_t frequency;
    uint32_t norm;
};

struct BitmapConjunctionResult {
    TruthSet truth;
    DorisVector<DorisVector<PostingStatistics>> statistics;
};

namespace detail {

class PostingStatisticsCollector {
public:
    explicit PostingStatisticsCollector(DorisVector<PostingStatistics>& rows) : _rows(rows) {}

    void operator()(uint32_t doc, uint32_t frequency, uint32_t norm) {
        _rows.push_back({.doc = doc, .frequency = frequency, .norm = norm});
    }

    void visit_block(const PostingsBlock& block) {
        const size_t begin = _rows.size();
        _rows.resize(begin + block.size());
        for (uint64_t ordinal = 0; ordinal < block.size(); ++ordinal) {
            _rows[begin + ordinal] = {.doc = block.doc_at(ordinal),
                                      .frequency = block.freq_at(ordinal),
                                      .norm = block.norm_at(ordinal)};
        }
    }

private:
    DorisVector<PostingStatistics>& _rows;
};

template <bool KeepStatistics>
Status collect_conjunction_rows(BlockDocSet& docs, const roaring::Roaring* candidates,
                                roaring::Roaring& true_rows, BitmapConjunctionResult* result,
                                size_t slot) {
    RoaringDocIdSink sink(true_rows);
    if constexpr (KeepStatistics) {
        PostingStatisticsCollector collect(result->statistics[slot]);
        return collect_postings<true>(docs, candidates, sink, collect);
    } else {
        return collect_postings<false>(docs, candidates, sink, [](uint32_t, uint32_t, uint32_t) {});
    }
}

struct ConjunctionGroup {
    DorisVector<size_t> slots;
    const roaring::Roaring* null_rows;
};

inline DorisVector<ConjunctionGroup> group_conjunction_inputs(
        std::span<const ConjunctionPostings> inputs, uint64_t row_limit) {
    const auto use_daat = [row_limit](const BlockDocSet& docs) {
        constexpr uint64_t seek_cost_multiplier = 4;
        return docs.cheap_seek() && docs.size_hint() * seek_cost_multiplier < row_limit;
    };
    DorisVector<ConjunctionGroup> groups;
    for (size_t slot = 0; slot < inputs.size(); ++slot) {
        const auto* nulls = inputs[slot].null_rows;
        if (nulls != nullptr && nulls->isEmpty()) {
            nulls = nullptr;
        }
        auto group = std::ranges::find_if(groups, [&](const auto& candidate) {
            const auto* previous = inputs[candidate.slots.front()].docs;
            return use_daat(*previous) && use_daat(*inputs[slot].docs) &&
                   (candidate.null_rows == nulls ||
                    (nulls != nullptr && candidate.null_rows != nullptr &&
                     *candidate.null_rows == *nulls));
        });
        if (group == groups.end()) {
            groups.push_back({.slots = {slot}, .null_rows = nulls});
        } else {
            group->slots.push_back(slot);
        }
    }
    const auto cheaper = [&](size_t left, size_t right) {
        return inputs[left].docs->size_hint() < inputs[right].docs->size_hint();
    };
    for (auto& group : groups) {
        std::ranges::stable_sort(group.slots, cheaper);
    }
    std::ranges::stable_sort(groups, [&](const auto& left, const auto& right) {
        return cheaper(left.slots.front(), right.slots.front());
    });
    return groups;
}

inline bool seek_conjunction_group(std::span<const ConjunctionPostings> inputs,
                                   const ConjunctionGroup& group, uint32_t* candidate) {
    size_t matched = 0;
    while (matched < group.slots.size()) {
        auto& docs = *inputs[group.slots[matched]].docs;
        if (!docs.seek(*candidate)) {
            return false;
        }
        if (docs.doc() != *candidate) {
            *candidate = docs.doc();
            matched = 0;
        } else {
            ++matched;
        }
    }
    return true;
}

template <bool KeepStatistics>
Status collect_conjunction_group(std::span<const ConjunctionPostings> inputs,
                                 const ConjunctionGroup& group, const roaring::Roaring* candidates,
                                 roaring::Roaring& true_rows, BitmapConjunctionResult* result) {
    if (group.slots.size() == 1) {
        return collect_conjunction_rows<KeepStatistics>(*inputs[group.slots.front()].docs,
                                                        candidates, true_rows, result,
                                                        group.slots.front());
    }
    auto& lead = *inputs[group.slots.front()].docs;
    RoaringDocIdSink sink(true_rows);
    DocIdBuffer selected(sink);
    std::optional<roaring::Roaring::const_iterator> selected_candidate;
    if (candidates != nullptr) {
        selected_candidate = candidates->begin();
    }
    while (!lead.exhausted()) {
        uint32_t candidate = lead.doc();
        if (selected_candidate) {
            if (!selected_candidate->i.has_value) {
                break;
            }
            if (**selected_candidate < candidate) {
                selected_candidate->equalorlarger(candidate);
                if (!selected_candidate->i.has_value) {
                    break;
                }
            }
            candidate = **selected_candidate;
        }
        if (!seek_conjunction_group(inputs, group, &candidate)) {
            break;
        }
        if (selected_candidate && **selected_candidate != candidate) {
            continue;
        }
        RETURN_IF_ERROR(selected.append(candidate));
        if constexpr (KeepStatistics) {
            for (const size_t slot : group.slots) {
                const auto& docs = *inputs[slot].docs;
                result->statistics[slot].push_back(
                        {.doc = candidate, .frequency = docs.freq(), .norm = docs.norm()});
            }
        }
        if (selected_candidate) {
            ++*selected_candidate;
        } else {
            lead.advance();
        }
    }
    return selected.flush();
}

} // namespace detail

// Dense candidate domains use bulk bitmap intersection. Sparse domains retain
// seek-based collection, and an empty possible result stops later postings reads.
template <bool KeepStatistics, size_t Extent>
Status collect_bitmap_conjunction(std::span<const ConjunctionPostings, Extent> inputs,
                                  uint64_t row_limit, BitmapConjunctionResult* result) {
    DORIS_CHECK(!inputs.empty());
    DORIS_CHECK_LE(row_limit, uint64_t {1} << 32);
    *result = {};
    result->truth.true_rows.addRange(0, row_limit);
    if constexpr (KeepStatistics) {
        result->statistics.resize(inputs.size());
    }
    const auto groups = detail::group_conjunction_inputs(inputs, row_limit);
    auto possible = result->truth.true_rows;
    for (const auto& group : groups) {
        if (possible.isEmpty()) {
            break;
        }
        const auto& docs = *inputs[group.slots.front()].docs;
        const auto* nulls = group.null_rows;
        const uint64_t unknown_count = nulls == nullptr ? 0 : possible.and_cardinality(*nulls);
        const uint64_t candidate_count = possible.cardinality() - unknown_count;
        roaring::Roaring true_rows;
        if (candidate_count != 0) {
            constexpr uint64_t seek_cost_multiplier = 4;
            const bool collect_all = candidate_count * seek_cost_multiplier >= docs.size_hint();
            std::optional<roaring::Roaring> candidates;
            if (!collect_all) {
                candidates = nulls == nullptr ? possible : possible - *nulls;
            }
            RETURN_IF_ERROR(detail::collect_conjunction_group<KeepStatistics>(
                    inputs, group, candidates ? &*candidates : nullptr, true_rows, result));
        }
        possible &= nulls == nullptr ? true_rows : true_rows | *nulls;
        result->truth.true_rows &= true_rows;
    }
    result->truth.null_rows = std::move(possible);
    result->truth.null_rows -= result->truth.true_rows;
    return Status::OK();
}

} // namespace doris::index_query
