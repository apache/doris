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
#include <numeric>

#include "storage/index/query/docid_sink.h"

namespace doris::index_query {

namespace {

constexpr size_t kBitSetDocs = 16 * 1024;
constexpr size_t kBitSetMinInput = 32;

struct DocumentOutput {
    static constexpr bool needs_ordinals = false;
    std::vector<uint32_t>* rows;

    void append(uint32_t doc, uint32_t /*ordinal*/) const { rows->push_back(doc); }
    void append_all(std::span<const uint32_t> docs) const {
        rows->insert(rows->end(), docs.begin(), docs.end());
    }
};

struct OrdinalOutput {
    static constexpr bool needs_ordinals = true;
    std::vector<uint32_t>* ordinals;

    void append(uint32_t /*doc*/, uint32_t ordinal) const { ordinals->push_back(ordinal); }
    void append_all(std::span<const uint32_t> docs) const {
        const size_t begin = ordinals->size();
        ordinals->resize(begin + docs.size());
        std::iota(ordinals->begin() + begin, ordinals->end(), 0U);
    }
};

struct SelectedOutput : DocumentOutput {
    static constexpr bool needs_ordinals = true;
    std::vector<uint32_t>* ordinals;

    void append(uint32_t doc, uint32_t ordinal) const {
        rows->push_back(doc);
        ordinals->push_back(ordinal);
    }
};

// Ordinal selection intersects one word at a time and ranks matches among the document bits.
template <typename Output>
void intersect_through_bit_set(std::span<const uint32_t> docs, std::span<const uint32_t> candidates,
                               Output out) {
    const uint32_t first = docs.front();
    const size_t words = ((docs.back() - first) >> 6) + 1;
    std::array<uint64_t, kBitSetDocs / 64> bits;
    std::fill_n(bits.begin(), words, 0);
    for (const uint32_t doc : docs) {
        const uint32_t off = doc - first;
        bits[off >> 6] |= uint64_t {1} << (off & 63);
    }
    if constexpr (Output::needs_ordinals) {
        size_t candidate = 0;
        uint32_t preceding = 0;
        for (size_t word = 0; word < words; ++word) {
            uint64_t matches = 0;
            const auto end = static_cast<uint32_t>((word + 1) * 64);
            while (candidate < candidates.size() && candidates[candidate] - first < end) {
                matches |= uint64_t {1} << ((candidates[candidate++] - first) & 63);
            }
            matches &= bits[word];
            while (matches != 0) {
                const auto bit = static_cast<uint32_t>(std::countr_zero(matches));
                const uint32_t ordinal =
                        preceding + static_cast<uint32_t>(std::popcount(
                                            bits[word] & ((uint64_t {1} << bit) - 1)));
                out.append(first + static_cast<uint32_t>(word * 64) + bit, ordinal);
                matches &= matches - 1;
            }
            preceding += static_cast<uint32_t>(std::popcount(bits[word]));
        }
    } else {
        for (const uint32_t candidate : candidates) {
            const uint32_t off = candidate - first;
            if ((bits[off >> 6] & (uint64_t {1} << (off & 63))) != 0) {
                out.append(candidate, 0);
            }
        }
    }
}

// Selects documents or their block ordinals with the same intersection strategy.
template <typename Output>
void intersect_block(std::span<const uint32_t> docs, std::span<const uint32_t> candidates,
                     Output out) {
    if (docs.empty() || candidates.empty()) {
        return;
    }
    if (candidates.size() == docs.size() && candidates.front() == docs.front() &&
        candidates.back() == docs.back() && std::ranges::equal(candidates, docs)) {
        out.append_all(docs);
        return;
    }
    const uint64_t width = static_cast<uint64_t>(docs.back()) - docs.front() + 1;
    if (candidates.size() == width) {
        out.append_all(docs);
        return;
    }
    const size_t probes_per_candidate = std::bit_width(docs.size()) + 1;
    if (candidates.size() < docs.size() / probes_per_candidate) {
        auto next = docs.begin();
        for (const uint32_t candidate : candidates) {
            next = std::lower_bound(next, docs.end(), candidate);
            if (next == docs.end()) {
                break;
            }
            if (*next == candidate) {
                out.append(*next, static_cast<uint32_t>(next - docs.begin()));
                ++next;
            }
        }
        return;
    }
    const size_t probes_per_doc = std::bit_width(candidates.size()) + 1;
    if (docs.size() < candidates.size() / probes_per_doc) {
        auto next = candidates.begin();
        for (size_t ordinal = 0; ordinal < docs.size(); ++ordinal) {
            next = std::lower_bound(next, candidates.end(), docs[ordinal]);
            if (next == candidates.end()) {
                break;
            }
            if (*next == docs[ordinal]) {
                out.append(docs[ordinal], static_cast<uint32_t>(ordinal));
                ++next;
            }
        }
        return;
    }
    if (candidates.size() >= kBitSetMinInput && docs.size() >= kBitSetMinInput &&
        width <= kBitSetDocs) {
        intersect_through_bit_set(docs, candidates, out);
        return;
    }
    size_t ordinal = 0;
    size_t candidate = 0;
    while (ordinal < docs.size() && candidate < candidates.size()) {
        if (docs[ordinal] < candidates[candidate]) {
            ++ordinal;
        } else if (candidates[candidate] < docs[ordinal]) {
            ++candidate;
        } else {
            out.append(docs[ordinal], static_cast<uint32_t>(ordinal));
            ++ordinal;
            ++candidate;
        }
    }
}

} // namespace

void intersect_block_ordinals(std::span<const uint32_t> docs, std::span<const uint32_t> candidates,
                              std::vector<uint32_t>* ordinals) {
    intersect_block(docs, candidates, OrdinalOutput {ordinals});
}

Status CursorChainedPostings::start(const std::vector<uint32_t>* candidates) {
    _candidates = candidates;
    if (_selected != nullptr) {
        _selected->ordinals.clear();
        _selected->blocks.clear();
    }
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
            [this, out](const PostingsBlock& block, std::span<const uint32_t> slice) {
                // A dense block holds every candidate in its span.
                if (block.dense) {
                    out->insert(out->end(), slice.begin(), slice.end());
                } else if (_selected != nullptr) {
                    const size_t begin = _selected->ordinals.size();
                    intersect_block(block.docs, slice,
                                    SelectedOutput {{out}, &_selected->ordinals});
                    if (_selected->ordinals.size() != begin) {
                        _selected->blocks.push_back(
                                {block.docs.back(), _selected->ordinals.size()});
                    }
                } else {
                    intersect_block(block.docs, slice, DocumentOutput {out});
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
