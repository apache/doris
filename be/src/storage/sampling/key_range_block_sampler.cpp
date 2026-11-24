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
#include <optional>
#include <queue>
#include <random>

#include "common/check.h"
#include "common/status.h"
#include "core/field.h"
#include "storage/index/short_key_index.h"
#include "storage/sampling/dependent_rounding.h"
#include "storage/sampling/tablet_sample_plan.h"
#include "storage/segment/segment_key_seeker.h"

namespace doris {
namespace {
struct Boundary {
    std::string prefix;
    std::vector<Field> tuple;
};

struct BlockHead {
    const SampleSegment* segment;
    uint32_t block;
    uint32_t start;
    uint32_t end;
    bool advance_next_block;
    std::optional<std::string> custom_prefix;

    const ShortKeyIndexDecoder& index() const { return segment->keys->index(); }
    Slice prefix() const {
        return custom_prefix.has_value() ? Slice(*custom_prefix) : index().key(block);
    }
    uint32_t weight() const { return end - start; }
};

// Sparse block heads charge each physical row once. When a prefix run spans multiple
// unit_rows (either from overlapping rowsets, shared VARCHAR prefixes, or unit_rows
// smaller than the short-key block size), intra-block stride probes subdivide it along
// full sort-key boundaries while deferring any suffix >= the next queued block head.
Status build_boundaries(std::span<const SampleSegment> segments, uint64_t unit_rows,
                        std::vector<Boundary>* cuts, uint64_t* population) {
    auto later = [](const BlockHead& a, const BlockHead& b) {
        return a.prefix().compare(b.prefix()) > 0;
    };
    std::priority_queue<BlockHead, std::vector<BlockHead>, decltype(later)> heads(later);
    uint32_t max_segment_blocks = 0;
    for (const auto& seg : segments) {
        if (seg.rows) {
            const uint32_t stride = seg.keys->index().num_rows_per_block();
            max_segment_blocks = std::max(max_segment_blocks, seg.keys->index().num_items());
            heads.push({&seg, 0, 0, std::min(seg.rows, stride), true, std::nullopt});
        }
    }
    if (heads.empty()) {
        *population = 0;
        return Status::OK();
    }
    uint64_t accumulated = 0;
    size_t probes_left = 8192;
    size_t bytes_left = 8 * 1024 * 1024;
    while (!heads.empty()) {
        const std::string prefix = heads.top().prefix().to_string();
        if (accumulated >= unit_rows) {
            cuts->push_back({prefix, {}});
            accumulated = 0;
        }
        std::vector<BlockHead> probes;
        uint64_t weight = 0;
        bool coarse_block = false;
        bool same_segment_run = false;
        do {
            auto head = heads.top();
            heads.pop();
            weight += head.weight();
            coarse_block |= head.weight() > unit_rows;
            same_segment_run |= std::any_of(
                    probes.begin(), probes.end(),
                    [&](const BlockHead& prev) { return prev.segment == head.segment; });
            if (head.advance_next_block && head.block + 1 < head.index().num_items()) {
                const uint32_t next_block = head.block + 1;
                const uint32_t stride = head.index().num_rows_per_block();
                const uint32_t next_start = next_block * stride;
                heads.push({head.segment, next_block, next_start,
                            std::min(head.segment->rows, next_start + stride), true, std::nullopt});
            }
            probes.push_back(std::move(head));
        } while (!heads.empty() && heads.top().prefix().compare(prefix) == 0);

        const uint64_t splits = weight / unit_rows;
        const bool needs_subdivision = coarse_block || same_segment_run || max_segment_blocks < 128;
        if (splits < 2 || !needs_subdivision || probes.size() > probes_left || bytes_left == 0) {
            accumulated += weight;
            continue;
        }
        const std::optional<std::string> upper_prefix =
                heads.empty() ? std::nullopt : std::make_optional(heads.top().prefix().to_string());
        const uint64_t per_head_splits =
                std::max<uint64_t>(1, std::min<uint64_t>(splits, probes_left / probes.size()));
        using CutKey = std::pair<std::string, std::vector<Field>>;
        std::map<CutKey, uint64_t> tuples;
        std::vector<BlockHead> continuations;
        size_t used_probes = 0;
        size_t used_bytes = 0;
        bool budget_exceeded = false;
        for (const auto& head : probes) {
            const uint32_t step = std::max<uint32_t>(
                    1,
                    static_cast<uint32_t>((head.weight() + per_head_splits - 1) / per_head_splits));
            for (uint32_t row = head.start; row < head.end;) {
                if (used_probes >= probes_left) {
                    budget_exceeded = true;
                    break;
                }
                std::string row_prefix;
                std::vector<Field> key;
                size_t bytes = 0;
                RETURN_IF_ERROR(head.segment->keys->read_key(row, &row_prefix, &key, &bytes));
                ++used_probes;
                bytes += row_prefix.size();
                if (used_bytes + bytes > bytes_left) {
                    budget_exceeded = true;
                    break;
                }
                used_bytes += bytes;
                if (upper_prefix.has_value() && row_prefix >= *upper_prefix) {
                    continuations.push_back({head.segment, head.block, row, head.end, false,
                                             std::move(row_prefix)});
                    break;
                }
                const uint32_t slice_rows = std::min(step, head.end - row);
                tuples[{std::move(row_prefix), std::move(key)}] += slice_rows;
                row += slice_rows;
            }
            if (budget_exceeded) {
                break;
            }
        }
        probes_left -= std::min(probes_left, used_probes);
        if (budget_exceeded || tuples.empty()) {
            if (budget_exceeded && used_bytes >= bytes_left) {
                bytes_left = 0;
            }
            accumulated += weight;
            continue;
        }
        bytes_left -= used_bytes;
        for (auto& cont : continuations) {
            heads.push(std::move(cont));
        }
        for (const auto& [cut_key, rows] : tuples) {
            if (accumulated >= unit_rows) {
                cuts->push_back({cut_key.first, cut_key.second});
                accumulated = 0;
            }
            accumulated += rows;
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
                                           int64_t cap)
        : _probability(probability), _seed(seed), _unit_rows(unit_rows), _cap(cap) {
    DORIS_CHECK_GT(unit_rows, 0);
}

Status KeyRangeBlockSampler::build(int64_t tablet_id, std::span<const SampleSegment> segments,
                                   TabletSamplePlan* plan) const {
    std::vector<Boundary> cuts;
    uint64_t population;
    RETURN_IF_ERROR(build_boundaries(segments, _unit_rows, &cuts, &population));
    DependentRounding rounding(_probability, _seed, tablet_id);
    const uint64_t selected = rounding.cumulative(population);
    if (selected > _cap) {
        return Status::InvalidArgument(
                "tablet_id={} population_units={} selected_units={} exceed cap={}", tablet_id,
                population, selected, _cap);
    }
    // Sequential simple random sampling without replacement. With r units and s seats
    // remaining the next unit gets a seat with probability s/r. Coalesce adjacent seats.
    std::mt19937_64 rng(
            sampling_mix(sampling_mix(sampling_domain("key-range-block"), _seed), tablet_id));
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
    uint64_t rows = 0;
    std::vector<segment_v2::RowRanges> ranges(segments.size());
    for (size_t pos = 0; pos < segments.size(); ++pos) {
        const auto& seg = segments[pos];
        if (seg.rows == 0) {
            continue;
        }
        for (auto [start, end] : runs) {
            uint32_t lo = 0, hi = seg.rows;
            if (start > 0) {
                const auto& cut = cuts[start - 1];
                RETURN_IF_ERROR(seg.keys->lower_bound(cut.prefix, cut.tuple, seg.rows, &lo));
            }
            if (end < population) {
                const auto& cut = cuts[end - 1];
                RETURN_IF_ERROR(seg.keys->lower_bound(cut.prefix, cut.tuple, seg.rows, &hi));
            }
            if (hi > lo) {
                ranges[pos].add({lo, hi});
                rows += hi - lo;
            }
        }
    }
    *plan = TabletSamplePlan(std::move(ranges), population, selected, rows);
    return Status::OK();
}
} // namespace doris
