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

#include "storage/index/snii/query/internal/docid_conjunction.h"

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <utility>

#include "common/check.h"
#include "storage/index/query/docid_set_ops.h"
#include "storage/index/query/exec/chained_conjunction.h"
#include "storage/index/query/spi/io_read_batch.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/reader/windowed_posting.h"

namespace doris::snii::query::internal {

using format::DictEntry;
using format::DictEntryEnc;
using format::DictEntryKind;
using format::FrqPreludeReader;
using format::WindowMeta;
using reader::LogicalIndexReader;

namespace {

using CandidateIt = std::vector<uint32_t>::const_iterator;

constexpr uint32_t kBoundedSpanBitsetDocs = 16 * 1024;
constexpr size_t kBoundedSpanBitsetWords = kBoundedSpanBitsetDocs / 64;
constexpr size_t kBoundedSpanBitsetMinInput = 32;

struct CandidateRange {
    size_t begin = 0;
    size_t end = 0;
};

Status configure_term_plan(const LogicalIndexReader& idx, bool need_positions,
                           io::BatchRangeFetcher* fetcher, TermPlan* p) {
    p->df = p->entry.df;
    p->pod_ref = (p->entry.kind == DictEntryKind::kPodRef);
    p->windowed = p->pod_ref && p->entry.enc == DictEntryEnc::kWindowed;
    if (p->windowed) {
        uint64_t prelude_abs = 0;
        RETURN_IF_ERROR(reader::prelude_abs_offset(idx, p->entry, p->frq_base, &prelude_abs));
        p->prelude_handle = fetcher->add(prelude_abs, p->entry.prelude_len);
    } else if (p->pod_ref) {
        uint64_t foff = 0;
        uint64_t flen = 0;
        uint64_t poff = 0;
        uint64_t plen = 0;
        RETURN_IF_ERROR(idx.resolve_frq_window(p->entry, p->frq_base, &foff, &flen));
        p->frq_handle = fetcher->add(foff, flen);
        if (need_positions) {
            RETURN_IF_ERROR(idx.resolve_prx_window(p->entry, p->prx_base, &poff, &plen));
            p->prx_handle = fetcher->add(poff, plen);
        }
    }
    return Status::OK();
}

std::vector<uint32_t> all_windows(const FrqPreludeReader& prelude) {
    std::vector<uint32_t> ws(prelude.n_windows());
    for (uint32_t i = 0; i < prelude.n_windows(); ++i) ws[i] = i;
    return ws;
}

Status append_docid_range(uint32_t first, uint32_t last, std::vector<uint32_t>* out) {
    if (last < first) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "docid_conjunction: invalid dense docid range");
    }
    const uint64_t count64 = static_cast<uint64_t>(last) - first + 1;
    if (count64 > static_cast<uint64_t>(std::numeric_limits<size_t>::max() - out->size())) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "docid_conjunction: dense docid range too large");
    }
    out->reserve(out->size() + static_cast<size_t>(count64));
    uint32_t docid = first;
    while (true) {
        out->push_back(docid);
        if (docid == last) break;
        ++docid;
    }
    return Status::OK();
}

CandidateRange find_candidate_range(const std::vector<uint32_t>& candidates, size_t* search_begin,
                                    uint32_t first, uint32_t last) {
    const auto from = candidates.begin() + *search_begin;
    const auto begin = std::lower_bound(from, candidates.end(), first);
    const auto end = std::upper_bound(begin, candidates.end(), last);
    *search_begin = static_cast<size_t>(end - candidates.begin());
    return {.begin = static_cast<size_t>(begin - candidates.begin()),
            .end = static_cast<size_t>(end - candidates.begin())};
}

void append_candidate_range(CandidateIt begin, CandidateIt end, std::vector<uint32_t>* out) {
    out->insert(out->end(), begin, end);
}

void append_new_chunk_docids_to_out(const DocidChunk& chunk, size_t chunk_docids_begin,
                                    std::vector<uint32_t>* out) {
    out->insert(out->end(), chunk.docids.begin() + chunk_docids_begin, chunk.docids.end());
}

void clear_ordinals_if_all_term_docs_selected(const std::vector<uint32_t>& term_docids,
                                              DocidChunk* chunk) {
    if (chunk->docids.size() == term_docids.size() && !chunk->docids.empty() &&
        chunk->docids.front() == term_docids.front() &&
        chunk->docids.back() == term_docids.back()) {
        chunk->prx_doc_ordinals.clear();
    }
}

bool append_term_docs_if_candidates_cover_span(CandidateIt begin, CandidateIt end,
                                               const std::vector<uint32_t>& term_docids,
                                               std::vector<uint32_t>* out, DocidChunk* chunk) {
    const uint32_t first = term_docids.front();
    const uint32_t last = term_docids.back();
    const uint64_t width = static_cast<uint64_t>(last) - first + 1;
    const size_t candidate_count = static_cast<size_t>(end - begin);
    if (width > candidate_count) {
        return false;
    }

    const auto span_begin = *begin == first ? begin : std::lower_bound(begin, end, first);
    if (span_begin == end || *span_begin != first) {
        return false;
    }
    if (static_cast<uint64_t>(end - span_begin) < width) {
        return false;
    }

    const auto span_last = span_begin + static_cast<size_t>(width) - 1;
    if (*span_last != last) {
        return false;
    }

    const size_t chunk_docids_begin = chunk->docids.size();
    chunk->docids.insert(chunk->docids.end(), term_docids.begin(), term_docids.end());
    append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
    return true;
}

Status append_candidate_range_with_ordinals(CandidateIt begin, CandidateIt end, uint32_t first,
                                            uint32_t last, std::vector<uint32_t>* out,
                                            DocidChunk* chunk) {
    const size_t candidate_count = static_cast<size_t>(end - begin);
    chunk->docids.reserve(candidate_count);
    const uint64_t width = static_cast<uint64_t>(last) - first + 1;
    if (width > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "docid_conjunction: dense window exceeds doc count range");
    }
    chunk->prx_doc_count = static_cast<uint32_t>(width);
    const bool full_dense_range =
            candidate_count == width && begin != end && *begin == first && *(end - 1) == last;
    const size_t chunk_docids_begin = chunk->docids.size();
    if (full_dense_range) {
        chunk->docids.insert(chunk->docids.end(), begin, end);
        append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
        return Status::OK();
    }
    chunk->prx_doc_ordinals.reserve(chunk->prx_doc_ordinals.size() + candidate_count);
    for (auto it = begin; it != end; ++it) {
        chunk->docids.push_back(*it);
        chunk->prx_doc_ordinals.push_back(*it - first);
    }
    append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
    return Status::OK();
}

bool intersect_dense_term_span_with_ordinals(CandidateIt begin, CandidateIt end,
                                             const std::vector<uint32_t>& term_docids,
                                             size_t candidate_count, std::vector<uint32_t>* out,
                                             DocidChunk* chunk) {
    const uint32_t first = term_docids.front();
    const uint32_t last = term_docids.back();
    const uint64_t width = static_cast<uint64_t>(last) - first + 1;
    if (term_docids.size() > width) {
        return false;
    }
    const uint64_t missing_count = width - term_docids.size();
    if (missing_count != 0 &&
        (missing_count * 8 > width || missing_count >= candidate_count ||
         missing_count > static_cast<uint64_t>(std::numeric_limits<size_t>::max()))) {
        return false;
    }

    const size_t chunk_docids_begin = chunk->docids.size();
    if (missing_count == 0) {
        for (auto it = begin; it != end; ++it) {
            if (*it < first) {
                continue;
            }
            if (*it > last) {
                break;
            }
            chunk->docids.push_back(*it);
            chunk->prx_doc_ordinals.push_back(*it - first);
        }
        append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
        clear_ordinals_if_all_term_docs_selected(term_docids, chunk);
        return true;
    }

    std::vector<uint32_t> missing;
    missing.reserve(static_cast<size_t>(missing_count));
    uint32_t expect = first;
    for (uint32_t docid : term_docids) {
        while (expect < docid) {
            missing.push_back(expect);
            ++expect;
        }
        if (docid < std::numeric_limits<uint32_t>::max()) {
            expect = docid + 1;
        }
    }
    while (expect <= last) {
        missing.push_back(expect);
        if (expect == std::numeric_limits<uint32_t>::max()) {
            break;
        }
        ++expect;
    }

    size_t miss = 0;
    for (auto it = begin; it != end; ++it) {
        if (*it < first) {
            continue;
        }
        if (*it > last) {
            break;
        }
        while (miss < missing.size() && missing[miss] < *it) {
            ++miss;
        }
        if (miss < missing.size() && missing[miss] == *it) {
            continue;
        }
        chunk->docids.push_back(*it);
        chunk->prx_doc_ordinals.push_back(static_cast<uint32_t>(*it - first - miss));
    }
    append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
    clear_ordinals_if_all_term_docs_selected(term_docids, chunk);
    return true;
}

bool intersect_bounded_span_with_ordinals(CandidateIt begin, CandidateIt end,
                                          const std::vector<uint32_t>& term_docids,
                                          size_t candidate_count, std::vector<uint32_t>* out,
                                          DocidChunk* chunk) {
    if (candidate_count < kBoundedSpanBitsetMinInput ||
        term_docids.size() < kBoundedSpanBitsetMinInput) {
        return false;
    }

    const uint32_t first = std::min(*begin, term_docids.front());
    const uint32_t last = std::max(*(end - 1), term_docids.back());
    const uint64_t width = static_cast<uint64_t>(last) - first + 1;
    if (width > kBoundedSpanBitsetDocs || term_docids.size() > width) {
        return false;
    }

    const auto word_count = static_cast<size_t>((width + 63) >> 6);
    std::array<uint64_t, kBoundedSpanBitsetWords> bits;
    std::fill_n(bits.begin(), word_count, 0);
    for (uint32_t docid : term_docids) {
        const uint32_t off = docid - first;
        bits[off >> 6] |= 1ULL << (off & 63);
    }

    std::array<uint32_t, kBoundedSpanBitsetWords> ordinal_base;
    uint32_t ordinal = 0;
    for (size_t word = 0; word < word_count; ++word) {
        ordinal_base[word] = ordinal;
        ordinal += static_cast<uint32_t>(__builtin_popcountll(bits[word]));
    }

    const size_t chunk_docids_begin = chunk->docids.size();
    for (auto it = begin; it != end; ++it) {
        const uint32_t off = *it - first;
        const size_t word = off >> 6;
        const uint64_t mask = 1ULL << (off & 63);
        if ((bits[word] & mask) == 0) {
            continue;
        }
        chunk->docids.push_back(*it);
        chunk->prx_doc_ordinals.push_back(
                ordinal_base[word] +
                static_cast<uint32_t>(__builtin_popcountll(bits[word] & (mask - 1))));
    }
    append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
    clear_ordinals_if_all_term_docs_selected(term_docids, chunk);
    return true;
}

// Interleaved inputs merge with a branch per step; within a bounded span a bitset of the
// term's documents answers each candidate with one test instead.
bool intersect_bounded_span(CandidateIt begin, CandidateIt end,
                            const std::vector<uint32_t>& term_docids, size_t candidate_count,
                            std::vector<uint32_t>* out) {
    if (candidate_count < kBoundedSpanBitsetMinInput ||
        term_docids.size() < kBoundedSpanBitsetMinInput) {
        return false;
    }

    const uint32_t first = std::min(*begin, term_docids.front());
    const uint32_t last = std::max(*(end - 1), term_docids.back());
    const uint64_t width = static_cast<uint64_t>(last) - first + 1;
    if (width > kBoundedSpanBitsetDocs || term_docids.size() > width) {
        return false;
    }

    const auto word_count = static_cast<size_t>((width + 63) >> 6);
    std::array<uint64_t, kBoundedSpanBitsetWords> bits;
    std::fill_n(bits.begin(), word_count, 0);
    for (uint32_t docid : term_docids) {
        const uint32_t off = docid - first;
        bits[off >> 6] |= 1ULL << (off & 63);
    }
    for (auto it = begin; it != end; ++it) {
        const uint32_t off = *it - first;
        if ((bits[off >> 6] & (1ULL << (off & 63))) != 0) {
            out->push_back(*it);
        }
    }
    return true;
}

size_t log2_ceil(size_t n) {
    if (n <= 1) return 1;
    --n;
    size_t bits = 0;
    while (n != 0) {
        ++bits;
        n >>= 1;
    }
    return bits;
}

void intersect_window_candidate_range(CandidateIt begin, CandidateIt end,
                                      const std::vector<uint32_t>& term_docids, uint32_t first,
                                      uint32_t last, std::vector<uint32_t>* out) {
    const size_t candidate_count = static_cast<size_t>(end - begin);
    if (candidate_count == 0 || term_docids.empty()) return;
    // Terms of the same documents meet identical inputs, which are the result as they are.
    if (candidate_count == term_docids.size() && *begin == term_docids.front() &&
        *(end - 1) == term_docids.back() && std::equal(begin, end, term_docids.begin())) {
        out->insert(out->end(), begin, end);
        return;
    }

    const uint64_t width = static_cast<uint64_t>(last) - first + 1;
    const uint64_t missing_count = term_docids.size() <= width ? width - term_docids.size() : width;
    if (term_docids.size() <= width && missing_count != 0 && missing_count * 8 <= width &&
        missing_count < candidate_count) {
        std::vector<uint32_t> missing;
        missing.reserve(static_cast<size_t>(missing_count));
        uint32_t expect = first;
        for (uint32_t docid : term_docids) {
            while (expect < docid) {
                missing.push_back(expect);
                ++expect;
            }
            if (docid < std::numeric_limits<uint32_t>::max()) expect = docid + 1;
        }
        while (expect <= last) {
            missing.push_back(expect);
            if (expect == std::numeric_limits<uint32_t>::max()) break;
            ++expect;
        }
        size_t miss = 0;
        for (auto it = begin; it != end; ++it) {
            while (miss < missing.size() && missing[miss] < *it) ++miss;
            if (miss == missing.size() || missing[miss] != *it) out->push_back(*it);
        }
        return;
    }

    const size_t probes_per_candidate = log2_ceil(term_docids.size()) + 1;
    if (candidate_count < term_docids.size() / probes_per_candidate) {
        for (auto it = begin; it != end; ++it) {
            if (std::binary_search(term_docids.begin(), term_docids.end(), *it)) {
                out->push_back(*it);
            }
        }
        return;
    }
    if (intersect_bounded_span(begin, end, term_docids, candidate_count, out)) {
        return;
    }
    std::set_intersection(begin, end, term_docids.begin(), term_docids.end(),
                          std::back_inserter(*out));
}

Status intersect_window_candidate_range_with_ordinals(CandidateIt begin, CandidateIt end,
                                                      const std::vector<uint32_t>& term_docids,
                                                      std::vector<uint32_t>* out,
                                                      DocidChunk* chunk) {
    if (term_docids.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "docid_conjunction: prx doc count exceeds u32");
    }
    chunk->prx_doc_count = static_cast<uint32_t>(term_docids.size());
    if (begin == end || term_docids.empty()) return Status::OK();

    const size_t candidate_count = static_cast<size_t>(end - begin);
    const size_t max_matches = std::min(candidate_count, term_docids.size());
    out->reserve(out->size() + max_matches);
    chunk->docids.reserve(chunk->docids.size() + max_matches);
    if (candidate_count == term_docids.size() && *begin == term_docids.front() &&
        *(end - 1) == term_docids.back() && std::equal(begin, end, term_docids.begin())) {
        const size_t chunk_docids_begin = chunk->docids.size();
        chunk->docids.insert(chunk->docids.end(), begin, end);
        append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
        return Status::OK();
    }
    if (append_term_docs_if_candidates_cover_span(begin, end, term_docids, out, chunk)) {
        return Status::OK();
    }

    chunk->prx_doc_ordinals.reserve(chunk->prx_doc_ordinals.size() + max_matches);
    if (intersect_dense_term_span_with_ordinals(begin, end, term_docids, candidate_count, out,
                                                chunk)) {
        return Status::OK();
    }
    if (intersect_bounded_span_with_ordinals(begin, end, term_docids, candidate_count, out,
                                             chunk)) {
        return Status::OK();
    }

    const size_t probes_per_candidate = log2_ceil(term_docids.size()) + 1;
    if (candidate_count < term_docids.size() / probes_per_candidate) {
        const size_t chunk_docids_begin = chunk->docids.size();
        size_t doc_index = 0;
        for (auto it = begin; it != end; ++it) {
            const auto found =
                    std::lower_bound(term_docids.begin() + doc_index, term_docids.end(), *it);
            if (found == term_docids.end()) break;
            doc_index = static_cast<size_t>(found - term_docids.begin());
            if (*found != *it) continue;
            chunk->docids.push_back(*it);
            chunk->prx_doc_ordinals.push_back(static_cast<uint32_t>(doc_index));
            ++doc_index;
        }
        append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
        clear_ordinals_if_all_term_docs_selected(term_docids, chunk);
        return Status::OK();
    }

    const size_t probes_per_term_doc = log2_ceil(candidate_count) + 1;
    if (term_docids.size() < candidate_count / probes_per_term_doc) {
        const size_t chunk_docids_begin = chunk->docids.size();
        auto candidate_it = begin;
        for (size_t doc_index = 0; doc_index < term_docids.size(); ++doc_index) {
            const uint32_t docid = term_docids[doc_index];
            candidate_it = std::lower_bound(candidate_it, end, docid);
            if (candidate_it == end) break;
            if (*candidate_it != docid) continue;
            chunk->docids.push_back(docid);
            chunk->prx_doc_ordinals.push_back(static_cast<uint32_t>(doc_index));
            ++candidate_it;
        }
        append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
        clear_ordinals_if_all_term_docs_selected(term_docids, chunk);
        return Status::OK();
    }

    const size_t chunk_docids_begin = chunk->docids.size();
    size_t doc_index = 0;
    for (auto it = begin; it != end; ++it) {
        while (doc_index < term_docids.size() && term_docids[doc_index] < *it) {
            ++doc_index;
        }
        if (doc_index == term_docids.size()) break;
        if (term_docids[doc_index] != *it) continue;
        chunk->docids.push_back(*it);
        chunk->prx_doc_ordinals.push_back(static_cast<uint32_t>(doc_index));
        ++doc_index;
    }
    append_new_chunk_docids_to_out(*chunk, chunk_docids_begin, out);
    clear_ordinals_if_all_term_docs_selected(term_docids, chunk);
    return Status::OK();
}

Status decode_flat_docids_only(const io::BatchRangeFetcher& round1, const TermPlan& p,
                               std::vector<uint32_t>* docids) {
    Slice dd;
    if (p.pod_ref) {
        dd = round1.get(p.frq_handle);
    } else {
        RETURN_IF_ERROR(inline_dd_region(p.entry, &dd));
    }
    return format::decode_dd_region(dd, p.entry.dd_meta, /*win_base=*/0, docids);
}

struct WindowWork {
    uint32_t ordinal = 0;
    WindowMeta meta;
    CandidateRange candidates;
    size_t handle = 0;
    bool dense_full = false;
};

Status emit_dense_full_window_docids(const WindowWork& f, const std::vector<uint32_t>* candidates,
                                     std::vector<uint32_t>& out, DocidSource* source) {
    uint32_t first = 0;
    RETURN_IF_ERROR(reader::first_docid_in_window(f.meta, f.ordinal, &first));
    if (source != nullptr) {
        DocidChunk chunk;
        chunk.windowed = true;
        chunk.window = f.ordinal;
        chunk.prx_doc_count = f.meta.doc_count;
        if (candidates == nullptr) {
            RETURN_IF_ERROR(append_docid_range(first, f.meta.last_docid, &chunk.docids));
        } else {
            const auto begin = candidates->begin() + f.candidates.begin;
            const auto end = candidates->begin() + f.candidates.end;
            RETURN_IF_ERROR(append_candidate_range_with_ordinals(begin, end, first,
                                                                 f.meta.last_docid, &out, &chunk));
        }
        source->chunks.push_back(std::move(chunk));
    }
    if (candidates == nullptr) {
        RETURN_IF_ERROR(append_docid_range(first, f.meta.last_docid, &out));
    } else if (source == nullptr) {
        append_candidate_range(candidates->begin() + f.candidates.begin,
                               candidates->begin() + f.candidates.end, &out);
    }
    return Status::OK();
}

Status emit_decoded_window_docids(const WindowWork& f, Slice window_bytes,
                                  const std::vector<uint32_t>* candidates,
                                  std::vector<uint32_t>& out, DocidSource* source,
                                  std::vector<uint32_t>& docs,
                                  std::vector<std::vector<uint32_t>>& positions) {
    docs.clear();
    positions.clear();
    RETURN_IF_ERROR(reader::decode_window_slices(f.meta, window_bytes, Slice(),
                                                 /*want_positions=*/false, &docs, &positions));
    if (source != nullptr) {
        DocidChunk chunk;
        chunk.windowed = true;
        chunk.window = f.ordinal;
        if (candidates == nullptr) {
            chunk.docids = docs;
            if (docs.size() > std::numeric_limits<uint32_t>::max()) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "docid_conjunction: prx doc count exceeds u32");
            }
            chunk.prx_doc_count = static_cast<uint32_t>(docs.size());
            source->chunks.push_back(std::move(chunk));
        } else {
            const auto begin = candidates->begin() + f.candidates.begin;
            const auto end = candidates->begin() + f.candidates.end;
            RETURN_IF_ERROR(
                    intersect_window_candidate_range_with_ordinals(begin, end, docs, &out, &chunk));
            if (!chunk.docids.empty()) {
                source->chunks.push_back(std::move(chunk));
            }
        }
    }
    if (candidates == nullptr) {
        out.insert(out.end(), docs.begin(), docs.end());
        return Status::OK();
    }
    if (source != nullptr) {
        return Status::OK();
    }
    uint32_t first = 0;
    RETURN_IF_ERROR(reader::first_docid_in_window(f.meta, f.ordinal, &first));
    intersect_window_candidate_range(candidates->begin() + f.candidates.begin,
                                     candidates->begin() + f.candidates.end, docs, first,
                                     f.meta.last_docid, &out);
    return Status::OK();
}

// Lists one planned term for the shared chained conjunction. A windowed term reads
// only the windows that can hold candidates, merging reads within the same-term
// gap; full windows need no read. A flat term decodes the posting fetched with the
// term plans.
class ChainedTermPostings final : public index_query::ChainedPostings {
public:
    ChainedTermPostings(const LogicalIndexReader& idx, const io::BatchRangeFetcher& round1,
                        const TermPlan& plan, DocidSource* source)
            : _idx(idx), _round1(round1), _plan(plan), _source(source) {}

    uint64_t doc_freq() const override { return _plan.df; }

    Status start(const std::vector<uint32_t>* candidates) override {
        _candidates = candidates;
        _windows.clear();
        _next_window = 0;
        _candidate_search_begin = 0;
        _reserved = false;
        if (!_plan.windowed) {
            return Status::OK();
        }
        if (candidates == nullptr ||
            reader::scan_all_windows(_idx, _plan.df, _plan.prelude.n_windows(),
                                     candidates->size())) {
            // Dense candidate sets cover most windows; for near-full terms this also
            // avoids a thousands-to-millions probe covering-window cursor pass with no
            // byte win.
            _windows = all_windows(_plan.prelude);
        } else {
            _plan.prelude.select_covering_windows(*candidates, &_windows);
        }
        return Status::OK();
    }

    Status register_reads(index_query::IoReadBatch& batch) override {
        _work.clear();
        while (_plan.windowed && _next_window < _windows.size()) {
            RETURN_IF_ERROR(_prepare_window(batch));
        }
        return Status::OK();
    }

    Status collect(const index_query::IoReadBatch& batch, std::vector<uint32_t>* out) override {
        if (!_plan.windowed) {
            return _collect_flat(out);
        }
        if (!_reserved) {
            out->reserve(out->size() +
                         (_candidates == nullptr ? _plan.entry.df : _candidates->size()));
            _reserved = true;
        }
        for (const WindowWork& work : _work) {
            if (work.dense_full) {
                RETURN_IF_ERROR(emit_dense_full_window_docids(work, _candidates, *out, _source));
                continue;
            }
            const auto bytes = batch.get(work.handle);
            RETURN_IF_ERROR(emit_decoded_window_docids(work, Slice(bytes.data(), bytes.size()),
                                                       _candidates, *out, _source, _docs,
                                                       _positions));
        }
        return Status::OK();
    }

private:
    // Adds the next selected window; one without candidates or a full one needs no read.
    Status _prepare_window(index_query::IoReadBatch& batch) {
        const uint32_t window = _windows[_next_window];
        WindowMeta meta;
        RETURN_IF_ERROR(_plan.prelude.window(window, &meta));
        uint32_t first = 0;
        RETURN_IF_ERROR(reader::first_docid_in_window(meta, window, &first));
        CandidateRange candidate_range;
        size_t search_begin = _candidate_search_begin;
        if (_candidates != nullptr) {
            candidate_range =
                    find_candidate_range(*_candidates, &search_begin, first, meta.last_docid);
            if (candidate_range.begin == candidate_range.end) {
                _candidate_search_begin = search_begin;
                ++_next_window;
                return Status::OK();
            }
        }
        WindowWork work {.ordinal = window, .meta = meta, .candidates = candidate_range};
        RETURN_IF_ERROR(reader::is_dense_full_window(meta, window, &work.dense_full));
        if (!work.dense_full) {
            reader::WindowAbsRange range;
            RETURN_IF_ERROR(reader::windowed_window_range(_idx, _plan.entry, _plan.frq_base,
                                                          _plan.prx_base, _plan.prelude, window,
                                                          /*want_positions=*/false, &range));
            work.handle = batch.add(range.dd_off, range.dd_len);
        }
        _candidate_search_begin = search_begin;
        _work.push_back(work);
        ++_next_window;
        return Status::OK();
    }

    Status _collect_flat(std::vector<uint32_t>* out) {
        std::vector<uint32_t> term_docids;
        RETURN_IF_ERROR(decode_flat_docids_only(_round1, _plan, &term_docids));
        if (_source != nullptr) {
            DocidChunk chunk;
            if (term_docids.size() > std::numeric_limits<uint32_t>::max()) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "docid_conjunction: prx doc count exceeds u32");
            }
            chunk.prx_doc_count = static_cast<uint32_t>(term_docids.size());
            if (_candidates == nullptr) {
                chunk.docids = term_docids;
            } else if (!term_docids.empty()) {
                const auto begin = std::ranges::lower_bound(*_candidates, term_docids.front());
                const auto end = std::upper_bound(begin, _candidates->end(), term_docids.back());
                RETURN_IF_ERROR(intersect_window_candidate_range_with_ordinals(
                        begin, end, term_docids, out, &chunk));
            }
            if (_candidates == nullptr || !chunk.docids.empty()) {
                _source->chunks.push_back(std::move(chunk));
            }
        }
        if (_candidates == nullptr) {
            append_docids(std::move(term_docids), out);
            return Status::OK();
        }
        if (_source != nullptr) {
            return Status::OK();
        }
        append_docids(index_query::intersect_sorted(*_candidates, term_docids), out);
        return Status::OK();
    }

    // Moves `docids` into an empty output instead of copying them.
    static void append_docids(std::vector<uint32_t>&& docids, std::vector<uint32_t>* out) {
        if (out->empty()) {
            *out = std::move(docids);
        } else {
            out->insert(out->end(), docids.begin(), docids.end());
        }
    }

    const LogicalIndexReader& _idx;
    const io::BatchRangeFetcher& _round1;
    const TermPlan& _plan;
    DocidSource* _source;
    const std::vector<uint32_t>* _candidates = nullptr;
    std::vector<uint32_t> _windows;
    size_t _next_window = 0;
    size_t _candidate_search_begin = 0;
    bool _reserved = false;
    std::vector<WindowWork> _work;
    std::vector<uint32_t> _docs;
    std::vector<std::vector<uint32_t>> _positions;
};

// Runs the shared chained conjunction over the planned terms. `sources`, when
// given, receives each term's listed documents; the last listed term's source
// holds the final candidates when every term was listed.
Status run_chained_conjunction(const LogicalIndexReader& idx, const io::BatchRangeFetcher& round1,
                               const std::vector<TermPlan>& plans,
                               const std::vector<uint32_t>* initial_candidates,
                               std::vector<uint32_t>* candidates,
                               std::vector<DocidSource>* sources) {
    if (sources != nullptr) {
        sources->assign(plans.size(), DocidSource {});
    }
    std::vector<ChainedTermPostings> terms;
    terms.reserve(plans.size());
    for (size_t i = 0; i < plans.size(); ++i) {
        terms.emplace_back(idx, round1, plans[i], sources == nullptr ? nullptr : &(*sources)[i]);
    }
    std::vector<index_query::ChainedPostings*> chain;
    chain.reserve(terms.size());
    for (ChainedTermPostings& term : terms) {
        chain.push_back(&term);
    }
    // One batch per term, its reads merged within the same-term gap.
    io::BatchRangeFetcher batch(idx.reader(), reader::kSameTermCoalesceGap);
    std::vector<size_t> visited;
    RETURN_IF_ERROR(index_query::chained_conjunction(chain, initial_candidates, batch, candidates,
                                                     sources == nullptr ? nullptr : &visited));
    if (sources != nullptr && !plans.empty() && visited.size() == plans.size()) {
        (*sources)[visited.back()].docids_are_final_candidates = true;
    }
    return Status::OK();
}

} // namespace

Status resolve_query_terms_batch(const LogicalIndexReader& idx,
                                 const std::vector<std::string>& terms,
                                 std::vector<ResolvedQueryTerm>* resolved,
                                 std::vector<uint8_t>* found) {
    std::vector<std::string> distinct = terms;
    std::ranges::sort(distinct);
    distinct.erase(std::ranges::unique(distinct).begin(), distinct.end());
    std::vector<LogicalIndexReader::BatchLookupResult> lookup_results;
    RETURN_IF_ERROR(idx.lookup_batch(distinct, &lookup_results));
    resolved->assign(terms.size(), ResolvedQueryTerm {});
    found->assign(terms.size(), 0);
    for (size_t i = 0; i < terms.size(); ++i) {
        const auto slot = std::ranges::lower_bound(distinct, terms[i]) - distinct.begin();
        const auto& result = lookup_results[static_cast<size_t>(slot)];
        (*found)[i] = result.found;
        if (result.found) {
            (*resolved)[i] = {.entry = result.entry,
                              .frq_base = result.frq_base,
                              .prx_base = result.prx_base};
        }
    }
    return Status::OK();
}

Status resolve_all_query_terms(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                               std::vector<ResolvedQueryTerm>* resolved, bool* all_present) {
    *all_present = false;
    for (const std::string& term : terms) {
        bool maybe_present = false;
        RETURN_IF_ERROR(idx.may_contain(term, &maybe_present));
        if (!maybe_present) {
            return Status::OK();
        }
    }
    std::vector<uint8_t> found;
    RETURN_IF_ERROR(resolve_query_terms_batch(idx, terms, resolved, &found));
    *all_present = std::ranges::all_of(found, [](uint8_t present) { return present != 0; });
    return Status::OK();
}

Status plan_terms(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                  io::BatchRangeFetcher* fetcher, std::vector<TermPlan>* plans, bool* all_present,
                  bool need_positions) {
    std::vector<ResolvedQueryTerm> resolved;
    RETURN_IF_ERROR(resolve_all_query_terms(idx, terms, &resolved, all_present));
    if (!*all_present) {
        return Status::OK();
    }
    plans->resize(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermPlan& p = (*plans)[i];
        p.order = i;
        p.entry = std::move(resolved[i].entry);
        p.frq_base = resolved[i].frq_base;
        p.prx_base = resolved[i].prx_base;
        RETURN_IF_ERROR(configure_term_plan(idx, need_positions, fetcher, &p));
    }
    return Status::OK();
}

Status plan_resolved_terms(const LogicalIndexReader& idx, std::vector<ResolvedQueryTerm>&& terms,
                           io::BatchRangeFetcher* fetcher, std::vector<TermPlan>* plans,
                           bool need_positions) {
    plans->resize(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermPlan& p = (*plans)[i];
        p.order = i;
#ifdef BE_TEST
        const bool has_frq_payload = !terms[i].entry.frq_bytes.empty();
        const bool has_prx_payload = !terms[i].entry.prx_bytes.empty();
        const uint8_t* const frq_payload = terms[i].entry.frq_bytes.data();
        const uint8_t* const prx_payload = terms[i].entry.prx_bytes.data();
#endif
        p.entry = std::move(terms[i].entry);
        SNII_QUERY_COUNT(resolved_term_entry_moves);
#ifdef BE_TEST
        SNII_QUERY_ADD(
                resolved_term_payload_pointer_reuses,
                static_cast<uint64_t>(has_frq_payload && p.entry.frq_bytes.data() == frq_payload) +
                        static_cast<uint64_t>(has_prx_payload &&
                                              p.entry.prx_bytes.data() == prx_payload));
#endif
        p.frq_base = terms[i].frq_base;
        p.prx_base = terms[i].prx_base;
        RETURN_IF_ERROR(configure_term_plan(idx, need_positions, fetcher, &p));
    }
    return Status::OK();
}

Status open_preludes(const io::BatchRangeFetcher& fetcher, std::vector<TermPlan>* plans,
                     bool need_positions) {
    for (TermPlan& p : *plans) {
        if (!p.windowed) continue;
        RETURN_IF_ERROR(FrqPreludeReader::open(fetcher.get(p.prelude_handle), &p.prelude));
        if (need_positions && !p.prelude.has_prx()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "docid_conjunction: windowed prelude has no positions");
        }
    }
    return Status::OK();
}

Status inline_dd_region(const DictEntry& entry, Slice* out) {
    if (entry.dd_meta.disk_len > entry.frq_bytes.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "docid_conjunction: inline dd region exceeds frq bytes");
    }
    *out = Slice(entry.frq_bytes.data(), static_cast<size_t>(entry.dd_meta.disk_len));
    return Status::OK();
}

Status build_docid_only_conjunction(const LogicalIndexReader& idx,
                                    const io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates) {
    return run_chained_conjunction(idx, round1, plans, nullptr, candidates, nullptr);
}

Status build_docid_only_conjunction(const LogicalIndexReader& idx,
                                    const io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates,
                                    std::vector<DocidSource>* sources) {
    return run_chained_conjunction(idx, round1, plans, nullptr, candidates, sources);
}

Status filter_docids_by_conjunction(const LogicalIndexReader& idx,
                                    const io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    const std::vector<uint32_t>& initial_candidates,
                                    std::vector<uint32_t>* candidates,
                                    std::vector<DocidSource>* sources) {
    return run_chained_conjunction(idx, round1, plans, &initial_candidates, candidates, sources);
}

} // namespace doris::snii::query::internal
