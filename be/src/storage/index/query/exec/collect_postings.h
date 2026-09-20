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
#include <limits>
#include <roaring/roaring.hh>
#include <span>

#include "storage/index/query/docid_sink.h"
#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/exec/docid_buffer.h"

namespace doris::index_query {
namespace detail {

template <bool VisitRows, typename Visitor>
Status collect_unrestricted_postings(BlockDocSet& docs, DocIdSink& sink, Visitor&& visit) {
    while (!docs.exhausted()) {
        const auto block = docs.take_remaining_block();
        if (block.dense) {
            RETURN_IF_ERROR(sink.append_range(block.range_begin, block.range_end));
        } else {
            RETURN_IF_ERROR(sink.append_sorted(block.docs));
        }
        if constexpr (VisitRows) {
            if constexpr (requires { visit.visit_block(block); }) {
                visit.visit_block(block);
            } else {
                for (uint64_t i = 0; i < block.size(); ++i) {
                    visit(block.doc_at(i), block.freq_at(i), block.norm_at(i));
                }
            }
        }
        docs.advance_block();
    }
    return Status::OK();
}

template <bool VisitRows, typename Visitor>
Status collect_candidate_postings_block(const PostingsBlock& block,
                                        roaring::Roaring::const_iterator& candidate,
                                        DocIdBuffer& selected, Visitor&& visit) {
    uint64_t ordinal = 0;
    while (candidate.i.has_value && ordinal < block.size()) {
        const uint32_t doc = block.doc_at(ordinal);
        if (doc < *candidate) {
            ordinal = block.dense ? uint64_t(*candidate) - block.range_begin : ordinal + 1;
        } else if (doc > *candidate) {
            candidate.equalorlarger(doc);
        } else {
            if constexpr (VisitRows) {
                visit(doc, block.freq_at(ordinal), block.norm_at(ordinal));
            }
            RETURN_IF_ERROR(selected.append(doc));
            ++ordinal;
            ++candidate;
        }
    }
    return Status::OK();
}

template <bool VisitRows, typename Visitor>
Status collect_scanned_postings_block(const PostingsBlock& block,
                                      const roaring::Roaring& candidates, uint32_t last,
                                      DocIdSink& sink, DocIdBuffer& selected, Visitor&& visit) {
    if (block.dense) {
        if constexpr (!VisitRows) {
            if (candidates.containsRange(block.range_begin, block.range_end)) {
                RETURN_IF_ERROR(selected.flush());
                return sink.append_range(block.range_begin, block.range_end);
            }
        }
        auto candidate = candidates.begin();
        candidate.equalorlarger(block.range_begin);
        return collect_candidate_postings_block<VisitRows>(block, candidate, selected, visit);
    }
    for (size_t ordinal = 0; ordinal < block.docs.size(); ++ordinal) {
        const uint32_t doc = block.docs[ordinal];
        if (doc > last) {
            break;
        }
        if (candidates.contains(doc)) {
            if constexpr (VisitRows) {
                visit(doc, block.freq_at(ordinal), block.norm_at(ordinal));
            }
            RETURN_IF_ERROR(selected.append(doc));
        }
    }
    return Status::OK();
}

template <bool VisitRows, typename Visitor>
Status scan_candidate_postings(BlockDocSet& docs, const roaring::Roaring& candidates,
                               DocIdSink& sink, Visitor&& visit) {
    if (!docs.seek(candidates.minimum())) {
        return Status::OK();
    }
    const uint32_t last = candidates.maximum();
    DocIdBuffer selected(sink);
    while (!docs.exhausted() && docs.doc() <= last) {
        const auto block = docs.take_remaining_block();
        RETURN_IF_ERROR(collect_scanned_postings_block<VisitRows>(block, candidates, last, sink,
                                                                  selected, visit));
        if (block.doc_at(block.size() - 1) >= last) {
            break;
        }
        docs.advance_block();
    }
    return selected.flush();
}

} // namespace detail

// Calls visit(doc, freq, norm) only for selected rows when VisitRows is true.
template <bool VisitRows, typename Visitor>
Status collect_postings(BlockDocSet& docs, const roaring::Roaring* candidates, DocIdSink& sink,
                        Visitor&& visit) {
    if (candidates == nullptr) {
        return detail::collect_unrestricted_postings<VisitRows>(docs, sink, visit);
    }
    if (candidates->isEmpty()) {
        return Status::OK();
    }
    // Dense candidates favor sequential block reads over repeated seeks.
    constexpr uint64_t seek_cost_multiplier = 4;
    if (candidates->cardinality() * seek_cost_multiplier >= docs.size_hint()) {
        return detail::scan_candidate_postings<VisitRows>(docs, *candidates, sink, visit);
    }
    DocIdBuffer selected(sink);
    auto candidate = candidates->begin();
    while (candidate.i.has_value && !docs.exhausted()) {
        if (!docs.seek(*candidate)) {
            break;
        }
        const auto block = docs.take_remaining_block();
        if constexpr (!VisitRows) {
            if (block.dense && candidates->containsRange(block.range_begin, block.range_end)) {
                RETURN_IF_ERROR(selected.flush());
                RETURN_IF_ERROR(sink.append_range(block.range_begin, block.range_end));
                if (block.range_end > std::numeric_limits<uint32_t>::max()) {
                    return Status::OK();
                }
                candidate.equalorlarger(static_cast<uint32_t>(block.range_end));
                continue;
            }
        }
        RETURN_IF_ERROR(detail::collect_candidate_postings_block<VisitRows>(block, candidate,
                                                                            selected, visit));
    }
    return selected.flush();
}

} // namespace doris::index_query
