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

#include "storage/sampling/key_range_block_sampler.h"

#include <algorithm>
#include <map>
#include <numeric>
#include <optional>
#include <queue>
#include <random>

#include "common/check.h"
#include "common/status.h"
#include "storage/index/short_key_index.h"
#include "storage/sampling/dependent_rounding.h"
#include "storage/sampling/tablet_sample_plan.h"
#include "storage/segment/segment_key_seeker.h"

namespace doris {
namespace {
constexpr size_t kMaxSubdivisionHeads = 16;
constexpr size_t kMaxTotalSubdivisionProbes = 16384;

struct BlockHead {
    const SampleSegment* segment;
    size_t seg_idx;
    uint32_t block;
    uint32_t start;
    uint32_t end;
    bool advance_next_block;
    std::optional<std::string> custom_prefix;

    const ShortKeyIndexDecoder& index() const { return segment->keys->index(); }
    Slice prefix() const {
        return custom_prefix.has_value() ? Slice(*custom_prefix)
                                         : segment->keys->index_prefix(block);
    }
    uint32_t weight() const { return end - start; }
};

using CutKey = std::pair<std::string, std::vector<std::string>>;

struct SubdivisionState {
    uint64_t unit_rows = 0;
    uint64_t remaining_rows = 0;
    size_t probes_left = kMaxTotalSubdivisionProbes;
    size_t bytes_left = 8 * 1024 * 1024;
    uint64_t accumulated = 0;
    std::vector<KeyRangeBound>* cuts = nullptr;
};

uint64_t carry_residual(uint64_t accumulated, uint64_t unit_rows) {
    return accumulated < 2 * unit_rows ? accumulated - unit_rows : 0;
}

// Coalesce sequential blocks of the same segment that share `prefix` into one contiguous span
// so stride probing covers that segment's entire prefix range rather than only its first block.
std::vector<BlockHead> merge_same_segment_heads(std::vector<BlockHead> probes) {
    std::ranges::sort(probes, [](const BlockHead& lhs, const BlockHead& rhs) {
        if (lhs.seg_idx != rhs.seg_idx) {
            return lhs.seg_idx < rhs.seg_idx;
        }
        return lhs.start < rhs.start;
    });
    std::vector<BlockHead> merged;
    merged.reserve(probes.size());
    for (auto& head : probes) {
        if (!merged.empty() && merged.back().seg_idx == head.seg_idx &&
            merged.back().end == head.start) {
            merged.back().end = head.end;
            merged.back().block = std::max(merged.back().block, head.block);
        } else {
            merged.push_back(std::move(head));
        }
    }
    return merged;
}

// Cumulative-weight midpoint selection picks up to kMaxSubdivisionHeads spans proportionally to
// row weight while spanning sequential equal-sized segments across the entire key range.
std::vector<size_t> select_active_heads(const std::vector<BlockHead>& spans,
                                        uint64_t total_weight) {
    if (spans.size() <= kMaxSubdivisionHeads || total_weight == 0) {
        std::vector<size_t> all(spans.size());
        std::iota(all.begin(), all.end(), 0);
        return all;
    }
    std::vector<uint64_t> cum_end(spans.size());
    uint64_t running = 0;
    for (size_t i = 0; i < spans.size(); ++i) {
        running += spans[i].weight();
        cum_end[i] = running;
    }
    std::vector<size_t> selected;
    selected.reserve(kMaxSubdivisionHeads);
    for (size_t j = 0; j < kMaxSubdivisionHeads; ++j) {
        const auto target =
                static_cast<uint64_t>((static_cast<unsigned __int128>(2 * j + 1) * total_weight) /
                                      (2 * kMaxSubdivisionHeads));
        auto it = std::ranges::upper_bound(cum_end, target);
        const size_t idx =
                it == cum_end.end() ? spans.size() - 1 : static_cast<size_t>(it - cum_end.begin());
        if (selected.empty() || selected.back() != idx) {
            selected.push_back(idx);
        }
    }
    return selected;
}

Status probe_selected_spans(const std::vector<BlockHead>& spans,
                            const std::vector<size_t>& selected_indices,
                            uint64_t sampled_head_weight, uint64_t desired_probes,
                            size_t run_probe_budget, const std::optional<std::string>& upper_prefix,
                            SubdivisionState* state, std::map<CutKey, uint64_t>* tuples,
                            uint64_t* in_range_sampled_rows, uint64_t* deferred_sampled_rows,
                            size_t* used_probes, size_t* used_bytes, bool* budget_exceeded,
                            std::vector<BlockHead>* continuations) {
    for (size_t sel_pos = 0; sel_pos < selected_indices.size(); ++sel_pos) {
        const auto& head = spans[selected_indices[sel_pos]];
        const uint64_t w = head.weight();
        const uint64_t span_probes = std::max<uint64_t>(
                1, (desired_probes * w + sampled_head_weight - 1) / sampled_head_weight);
        const uint32_t step =
                std::max<uint32_t>(1, static_cast<uint32_t>((w + span_probes - 1) / span_probes));
        const auto phase = static_cast<uint32_t>((static_cast<uint64_t>(sel_pos) * step) /
                                                 selected_indices.size());
        bool first_slice = phase > 0;
        for (uint32_t row = head.start; row < head.end;) {
            if (*used_probes >= run_probe_budget) {
                *budget_exceeded = true;
                return Status::OK();
            }
            std::string row_prefix;
            std::vector<std::string> key;
            size_t bytes = 0;
            RETURN_IF_ERROR(head.segment->keys->read_key(row, &row_prefix, &key, &bytes));
            ++(*used_probes);
            bytes += row_prefix.size();
            if (*used_bytes + bytes > state->bytes_left) {
                *budget_exceeded = true;
                return Status::OK();
            }
            *used_bytes += bytes;
            if (upper_prefix.has_value() && row_prefix >= *upper_prefix) {
                *deferred_sampled_rows += head.end - row;
                continuations->push_back({head.segment, head.seg_idx, head.block, row, head.end,
                                          false, std::move(row_prefix)});
                break;
            }
            const uint32_t target_step = first_slice ? phase : step;
            first_slice = false;
            const uint32_t slice_rows = std::min(target_step, head.end - row);
            *in_range_sampled_rows += slice_rows;
            (*tuples)[{std::move(row_prefix), std::move(key)}] += slice_rows;
            row += slice_rows;
        }
    }
    return Status::OK();
}

Status subdivide_prefix_run(const std::vector<BlockHead>& spans, uint64_t weight, uint64_t splits,
                            const std::optional<std::string>& upper_prefix, SubdivisionState* state,
                            std::vector<BlockHead>* continuations) {
    const std::vector<size_t> selected_indices = select_active_heads(spans, weight);
    const auto fair_share =
            static_cast<size_t>((static_cast<unsigned __int128>(state->probes_left) * weight) /
                                std::max<uint64_t>(weight, state->remaining_rows));
    const size_t run_probe_budget =
            std::min(state->probes_left, std::max(selected_indices.size() * 2, fair_share));
    if (run_probe_budget < selected_indices.size()) {
        state->accumulated += weight;
        state->remaining_rows -= std::min(state->remaining_rows, weight);
        return Status::OK();
    }
    uint64_t sampled_head_weight = 0;
    for (size_t idx : selected_indices) {
        sampled_head_weight += spans[idx].weight();
    }
    const uint64_t unprobed_weight = weight - sampled_head_weight;
    const uint64_t usable_probes = run_probe_budget > selected_indices.size() * 2
                                           ? run_probe_budget - selected_indices.size() * 2
                                           : 1;
    const uint64_t desired_probes = std::min<uint64_t>(
            usable_probes, std::max<uint64_t>(splits * 2, selected_indices.size() * 8));
    std::map<CutKey, uint64_t> tuples;
    uint64_t in_range_sampled_rows = 0;
    uint64_t deferred_sampled_rows = 0;
    size_t used_probes = 0;
    size_t used_bytes = 0;
    bool budget_exceeded = false;
    RETURN_IF_ERROR(probe_selected_spans(
            spans, selected_indices, sampled_head_weight, desired_probes, run_probe_budget,
            upper_prefix, state, &tuples, &in_range_sampled_rows, &deferred_sampled_rows,
            &used_probes, &used_bytes, &budget_exceeded, continuations));
    state->probes_left -= std::min(state->probes_left, used_probes);
    if (budget_exceeded || tuples.empty()) {
        if (budget_exceeded && used_bytes >= state->bytes_left) {
            state->bytes_left = 0;
        }
        continuations->clear();
        state->accumulated += weight;
        state->remaining_rows -= std::min(state->remaining_rows, weight);
        return Status::OK();
    }
    state->bytes_left -= used_bytes;
    const uint64_t target_in_range_weight = weight - deferred_sampled_rows;
    state->remaining_rows -= std::min(state->remaining_rows, target_in_range_weight);
    uint64_t seen_raw = 0;
    uint64_t prev_scaled = 0;
    for (const auto& [cut_key, raw_rows] : tuples) {
        seen_raw += raw_rows;
        const uint64_t next_scaled =
                unprobed_weight == 0
                        ? seen_raw
                        : static_cast<uint64_t>((static_cast<unsigned __int128>(seen_raw) *
                                                 target_in_range_weight) /
                                                in_range_sampled_rows);
        const uint64_t scaled_rows = next_scaled - prev_scaled;
        prev_scaled = next_scaled;
        if (state->accumulated >= state->unit_rows) {
            state->cuts->push_back({cut_key.first, cut_key.second});
            state->accumulated = carry_residual(state->accumulated, state->unit_rows);
        }
        state->accumulated += scaled_rows;
    }
    return Status::OK();
}

// Sparse block heads charge each physical row once. When a prefix run spans multiple
// unit_rows (either from overlapping rowsets, shared VARCHAR prefixes, or unit_rows
// smaller than the short-key block size), intra-block stride probes subdivide it along
// full sort-key boundaries while deferring any suffix >= the next queued block head.
Status build_boundaries(std::span<const SampleSegment> segments, uint64_t unit_rows,
                        std::vector<KeyRangeBound>* cuts, uint64_t* population) {
    auto later = [](const BlockHead& a, const BlockHead& b) {
        const int cmp = a.prefix().compare(b.prefix());
        if (cmp != 0) {
            return cmp > 0;
        }
        return a.seg_idx > b.seg_idx;
    };
    std::priority_queue<BlockHead, std::vector<BlockHead>, decltype(later)> heads(later);
    uint32_t max_segment_blocks = 0;
    uint64_t total_rows = 0;
    for (size_t seg_idx = 0; seg_idx < segments.size(); ++seg_idx) {
        const auto& seg = segments[seg_idx];
        if (seg.rows) {
            total_rows += seg.rows;
            const uint32_t stride = seg.keys->index().num_rows_per_block();
            max_segment_blocks = std::max(max_segment_blocks, seg.keys->index().num_items());
            heads.push({&seg, seg_idx, 0, 0, std::min(seg.rows, stride), true, std::nullopt});
        }
    }
    if (heads.empty()) {
        *population = 0;
        return Status::OK();
    }
    SubdivisionState state {
            .unit_rows = unit_rows,
            .remaining_rows = total_rows,
            .cuts = cuts,
    };
    while (!heads.empty()) {
        const std::string prefix = heads.top().prefix().to_string();
        if (state.accumulated >= unit_rows) {
            cuts->push_back({prefix, {}});
            state.accumulated = carry_residual(state.accumulated, unit_rows);
        }
        std::vector<BlockHead> raw_probes;
        uint64_t weight = 0;
        bool coarse_block = false;
        bool same_segment_run = false;
        do {
            auto head = heads.top();
            heads.pop();
            weight += head.weight();
            coarse_block |= head.weight() > unit_rows;
            same_segment_run |= std::ranges::any_of(raw_probes, [&](const BlockHead& prev) {
                return prev.seg_idx == head.seg_idx;
            });
            if (head.advance_next_block && head.block + 1 < head.index().num_items()) {
                const uint32_t next_block = head.block + 1;
                const uint32_t stride = head.index().num_rows_per_block();
                const uint32_t next_start = next_block * stride;
                heads.push({head.segment, head.seg_idx, next_block, next_start,
                            std::min(head.segment->rows, next_start + stride), true, std::nullopt});
            }
            raw_probes.push_back(std::move(head));
        } while (!heads.empty() && heads.top().prefix().compare(prefix) == 0);

        const uint64_t splits = weight / unit_rows;
        const bool needs_subdivision = coarse_block || same_segment_run || max_segment_blocks < 128;
        std::vector<BlockHead> spans = merge_same_segment_heads(std::move(raw_probes));
        const size_t active_heads = std::min(spans.size(), kMaxSubdivisionHeads);
        if (splits < 2 || !needs_subdivision || active_heads > state.probes_left ||
            state.bytes_left == 0) {
            state.accumulated += weight;
            state.remaining_rows -= std::min(state.remaining_rows, weight);
            continue;
        }
        const std::optional<std::string> upper_prefix =
                heads.empty() ? std::nullopt : std::make_optional(heads.top().prefix().to_string());
        std::vector<BlockHead> continuations;
        RETURN_IF_ERROR(
                subdivide_prefix_run(spans, weight, splits, upper_prefix, &state, &continuations));
        for (auto& cont : continuations) {
            heads.push(std::move(cont));
        }
    }
    *population = 1 + cuts->size();
    return Status::OK();
}

// REPEATABLE must give the same draw on replicas built with different C++ libraries.
// Discard the incomplete residue classes before reducing the standardized engine's output.
uint64_t draw_below(std::mt19937_64& rng, uint64_t bound) {
    const uint64_t threshold = (uint64_t {0} - bound) % bound;
    uint64_t draw;
    do {
        draw = rng();
    } while (draw < threshold);
    return draw % bound;
}

} // namespace

KeyRangeBlockSampler::KeyRangeBlockSampler(double probability, int64_t seed, int64_t unit_rows,
                                           int64_t cap,
                                           std::optional<KeyRangeSamplePlanSpec> shared_spec)
        : _probability(probability),
          _seed(seed),
          _unit_rows(unit_rows),
          _cap(cap),
          _shared_spec(std::move(shared_spec)) {
    DORIS_CHECK_GT(unit_rows, 0);
}

Status KeyRangeBlockSampler::collect_weighted_cuts(std::span<const SampleSegment> segments,
                                                   uint64_t min_unit_rows, uint64_t max_cuts,
                                                   std::vector<WeightedKeyRangeCut>* weighted_cuts,
                                                   uint64_t* total_rows) {
    weighted_cuts->clear();
    uint64_t rows = 0;
    std::optional<Slice> min_prefix;
    for (const auto& seg : segments) {
        if (seg.rows > 0) {
            rows += seg.rows;
            const Slice p = seg.keys->index_prefix(0);
            if (!min_prefix.has_value() || p.compare(*min_prefix) < 0) {
                min_prefix = p;
            }
        }
    }
    *total_rows = rows;
    if (rows == 0) {
        return Status::OK();
    }
    std::optional<CutKey> first_key;
    size_t min_probes = 0;
    for (const auto& seg : segments) {
        if (seg.rows > 0 && seg.keys->index_prefix(0).compare(*min_prefix) == 0) {
            std::string row_prefix;
            std::vector<std::string> tuple;
            size_t bytes = 0;
            RETURN_IF_ERROR(seg.keys->read_key(0, &row_prefix, &tuple, &bytes));
            CutKey candidate {std::move(row_prefix), std::move(tuple)};
            if (!first_key.has_value() || candidate < *first_key) {
                first_key = std::move(candidate);
            }
            if (++min_probes >= kMaxSubdivisionHeads) {
                break;
            }
        }
    }
    const auto cap = std::max<uint64_t>(1, max_cuts);
    const auto eff_unit = std::max<uint64_t>({1, min_unit_rows, (rows + cap - 1) / cap});
    std::vector<KeyRangeBound> cuts;
    uint64_t population = 0;
    RETURN_IF_ERROR(build_boundaries(segments, eff_unit, &cuts, &population));
    weighted_cuts->reserve(population);
    auto chunk_weight = [&](size_t idx) -> uint64_t {
        const auto lo =
                static_cast<uint64_t>((static_cast<unsigned __int128>(idx) * rows) / population);
        const auto hi = static_cast<uint64_t>((static_cast<unsigned __int128>(idx + 1) * rows) /
                                              population);
        return hi - lo;
    };
    weighted_cuts->push_back({.bound = {.prefix = std::move(first_key->first),
                                        .tuple = std::move(first_key->second)},
                              .weight = chunk_weight(0)});
    for (size_t i = 0; i < cuts.size(); ++i) {
        weighted_cuts->push_back({.bound = std::move(cuts[i]), .weight = chunk_weight(i + 1)});
    }
    return Status::OK();
}

Status KeyRangeBlockSampler::sample_intervals(int64_t domain_id,
                                              std::span<const SampleSegment> segments,
                                              bool empty_as_universal_unit,
                                              KeyRangeSamplePlanSpec* spec) const {
    std::vector<KeyRangeBound> cuts;
    uint64_t population = 0;
    RETURN_IF_ERROR(build_boundaries(segments, _unit_rows, &cuts, &population));
    if (population == 0 && empty_as_universal_unit) {
        population = 1;
    }
    DependentRounding rounding(_probability, _seed, domain_id);
    const uint64_t selected = rounding.cumulative(population);
    if (selected > _cap) {
        return Status::InvalidArgument(
                "tablet_id={} population_units={} selected_units={} exceed cap={}", domain_id,
                population, selected, _cap);
    }
    // Sequential simple random sampling without replacement. With r units and s seats
    // remaining the next unit gets a seat with probability s/r. Coalesce adjacent seats.
    std::mt19937_64 rng(
            sampling_mix(sampling_mix(sampling_domain("key-range-block"), _seed), domain_id));
    std::vector<std::pair<size_t, size_t>> runs;
    uint64_t seats = selected;
    for (size_t i = 0; seats && i < population; ++i) {
        if (seats == population - i || draw_below(rng, population - i) < seats) {
            if (!runs.empty() && runs.back().second == i) {
                ++runs.back().second;
            } else {
                runs.emplace_back(i, i + 1);
            }
            --seats;
        }
    }
    spec->intervals.clear();
    spec->intervals.reserve(runs.size());
    for (auto [start, end] : runs) {
        std::optional<KeyRangeBound> lower =
                start > 0 ? std::make_optional(cuts[start - 1]) : std::nullopt;
        std::optional<KeyRangeBound> upper =
                end < population ? std::make_optional(cuts[end - 1]) : std::nullopt;
        spec->intervals.push_back({std::move(lower), std::move(upper)});
    }
    spec->population_units = population;
    spec->selected_units = selected;
    return Status::OK();
}

namespace {
Status locate_segment_intervals(const SampleSegment& seg,
                                std::span<const KeyRangeSampleInterval> intervals,
                                segment_v2::RowRanges* out, uint64_t* selected_rows) {
    const Slice seg_min_prefix = seg.keys->index_prefix(0);
    if (intervals.back().upper.has_value() &&
        Slice(intervals.back().upper->prefix).compare(seg_min_prefix) < 0) {
        return Status::OK();
    }
    uint32_t cursor = 0;
    for (const auto& interval : intervals) {
        if (interval.upper.has_value() &&
            Slice(interval.upper->prefix).compare(seg_min_prefix) < 0) {
            continue;
        }
        uint32_t lo = 0;
        if (interval.lower.has_value()) {
            RETURN_IF_ERROR(seg.keys->lower_bound(interval.lower->prefix, interval.lower->tuple,
                                                  seg.rows, &lo, cursor));
            cursor = lo;
            if (cursor >= seg.rows) {
                break;
            }
        }
        uint32_t hi = seg.rows;
        if (interval.upper.has_value()) {
            RETURN_IF_ERROR(seg.keys->lower_bound(interval.upper->prefix, interval.upper->tuple,
                                                  seg.rows, &hi, lo));
            cursor = hi;
        }
        if (hi > lo) {
            out->add({lo, hi});
            *selected_rows += hi - lo;
        }
        if (!interval.upper.has_value() || cursor >= seg.rows) {
            break;
        }
    }
    return Status::OK();
}
} // namespace

Status KeyRangeBlockSampler::locate_intervals(int64_t tablet_id,
                                              std::span<const SampleSegment> segments,
                                              const KeyRangeSamplePlanSpec& spec,
                                              TabletSamplePlan* plan) const {
    if (spec.selected_units > _cap) {
        return Status::InvalidArgument(
                "tablet_id={} population_units={} selected_units={} exceed cap={}", tablet_id,
                spec.population_units, spec.selected_units, _cap);
    }
    uint64_t rows = 0;
    std::vector<segment_v2::RowRanges> ranges(segments.size());
    if (!spec.intervals.empty()) {
        for (size_t pos = 0; pos < segments.size(); ++pos) {
            if (segments[pos].rows > 0) {
                RETURN_IF_ERROR(locate_segment_intervals(segments[pos], spec.intervals,
                                                         &ranges[pos], &rows));
            }
        }
    }
    *plan = TabletSamplePlan(std::move(ranges), spec.population_units, spec.selected_units, rows);
    return Status::OK();
}

Status KeyRangeBlockSampler::build(int64_t tablet_id, std::span<const SampleSegment> segments,
                                   TabletSamplePlan* plan) const {
    if (_shared_spec.has_value()) {
        return locate_intervals(tablet_id, segments, *_shared_spec, plan);
    }
    KeyRangeSamplePlanSpec spec;
    RETURN_IF_ERROR(sample_intervals(tablet_id, segments, false, &spec));
    return locate_intervals(tablet_id, segments, spec, plan);
}
} // namespace doris
