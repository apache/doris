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

#include "storage/index/query/exec/cursor_chained_postings.h"

#include <algorithm>
#include <array>
#include <bit>
#include <iterator>

#include "storage/index/query/docid_sink.h"

namespace doris::index_query {

namespace {

constexpr size_t kBitSetDocs = 16 * 1024;
constexpr size_t kBitSetMinInput = 32;

// The candidates a listed block holds, probed through a bit set over the block's span.
void intersect_through_bit_set(std::span<const uint32_t> docs, std::span<const uint32_t> candidates,
                               std::vector<uint32_t>* out) {
    const uint32_t first = docs.front();
    const size_t words = ((docs.back() - first) >> 6) + 1;
    std::array<uint64_t, kBitSetDocs / 64> bits;
    std::fill_n(bits.begin(), words, 0);
    for (const uint32_t doc : docs) {
        const uint32_t off = doc - first;
        bits[off >> 6] |= uint64_t {1} << (off & 63);
    }
    for (const uint32_t candidate : candidates) {
        const uint32_t off = candidate - first;
        if ((bits[off >> 6] >> (off & 63)) & 1) {
            out->push_back(candidate);
        }
    }
}

// The candidates a listed block holds, all within its span: identical lists are the result as
// they are, a few candidates are searched, many within a narrow span are probed through a bit
// set, and the rest merge.
void intersect_block(std::span<const uint32_t> docs, std::span<const uint32_t> candidates,
                     std::vector<uint32_t>* out) {
    if (candidates.size() == docs.size() && candidates.front() == docs.front() &&
        candidates.back() == docs.back() && std::ranges::equal(candidates, docs)) {
        out->insert(out->end(), candidates.begin(), candidates.end());
        return;
    }
    // The candidates lie within the block's span, so as many as its width fill it and hold every
    // document of the block.
    const uint64_t width = static_cast<uint64_t>(docs.back()) - docs.front() + 1;
    if (candidates.size() == width) {
        out->insert(out->end(), docs.begin(), docs.end());
        return;
    }
    const size_t probes_per_candidate = std::bit_width(docs.size()) + 1;
    if (candidates.size() < docs.size() / probes_per_candidate) {
        for (const uint32_t candidate : candidates) {
            if (std::ranges::binary_search(docs, candidate)) {
                out->push_back(candidate);
            }
        }
        return;
    }
    const size_t probes_per_doc = std::bit_width(candidates.size()) + 1;
    if (docs.size() < candidates.size() / probes_per_doc) {
        for (const uint32_t doc : docs) {
            if (std::ranges::binary_search(candidates, doc)) {
                out->push_back(doc);
            }
        }
        return;
    }
    if (candidates.size() >= kBitSetMinInput && docs.size() >= kBitSetMinInput &&
        width <= kBitSetDocs) {
        intersect_through_bit_set(docs, candidates, out);
        return;
    }
    std::ranges::set_intersection(candidates, docs, std::back_inserter(*out));
}

} // namespace

Status CursorChainedPostings::start(const std::vector<uint32_t>* candidates) {
    _candidates = candidates;
    return _cursor.prefetch(candidates, /*positions=*/false);
}

Status CursorChainedPostings::collect(std::vector<uint32_t>* out) {
    if (_candidates == nullptr) {
        out->reserve(out->size() + _cursor.doc_freq());
        VectorDocIdSink sink(*out);
        return for_each_block(_cursor, [&sink](const PostingsBlock& block) {
            return block.dense ? sink.append_range(block.range_begin, block.range_end)
                               : sink.append_sorted(block.docs);
        });
    }
    out->reserve(out->size() + std::min<uint64_t>(_candidates->size(), _cursor.doc_freq()));
    return for_each_candidate_block(
            _cursor, *_candidates,
            [out](const PostingsBlock& block, std::span<const uint32_t> slice) {
                // A dense block holds every candidate in its span.
                if (block.dense) {
                    out->insert(out->end(), slice.begin(), slice.end());
                } else {
                    intersect_block(block.docs, slice, out);
                }
                return Status::OK();
            });
}

Status chain_cursors(std::span<const std::unique_ptr<PostingsCursor>> cursors,
                     const std::vector<uint32_t>* candidates, std::vector<uint32_t>* rows) {
    std::vector<CursorChainedPostings> terms;
    terms.reserve(cursors.size());
    std::vector<ChainedPostings*> chain;
    chain.reserve(cursors.size());
    for (const auto& cursor : cursors) {
        terms.emplace_back(*cursor);
        chain.push_back(&terms.back());
    }
    return chained_conjunction(chain, candidates, rows);
}

} // namespace doris::index_query
