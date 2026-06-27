#include "snii/query/internal/docid_conjunction.h"

#include <algorithm>
#include <iterator>
#include <limits>

#include "snii/format/frq_pod.h"
#include "snii/query/internal/docid_set_ops.h"
#include "snii/reader/windowed_posting.h"

namespace snii::query::internal {

using snii::format::DictEntry;
using snii::format::DictEntryEnc;
using snii::format::DictEntryKind;
using snii::format::FrqPreludeReader;
using snii::format::WindowMeta;
using snii::reader::LogicalIndexReader;

namespace {

using CandidateIt = std::vector<uint32_t>::const_iterator;

struct CandidateRange {
    size_t begin = 0;
    size_t end = 0;
};

Status slim_frq_docs_len(const DictEntry& entry, uint64_t win_len, uint64_t* out) {
    if (entry.frq_docs_len > win_len) {
        return Status::Corruption("docid_conjunction: slim frq_docs_len exceeds frq window");
    }
    *out = entry.frq_docs_len > 0 ? entry.frq_docs_len : win_len;
    return Status::OK();
}

Status add_u64(uint64_t lhs, uint64_t rhs, const char* message, uint64_t* out) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) {
        return Status::Corruption(message);
    }
    *out = lhs + rhs;
    return Status::OK();
}

Status posting_abs_offset(const LogicalIndexReader& idx, uint64_t base, uint64_t delta,
                          const char* message, uint64_t* out) {
    uint64_t with_base = 0;
    SNII_RETURN_IF_ERROR(
            add_u64(idx.section_refs().posting_region.offset, base, message, &with_base));
    return add_u64(with_base, delta, message, out);
}

Status configure_term_plan(const LogicalIndexReader& idx, bool need_positions,
                           snii::io::BatchRangeFetcher* fetcher, TermPlan* p) {
    p->df = p->entry.df;
    p->pod_ref = (p->entry.kind == DictEntryKind::kPodRef);
    p->windowed = p->pod_ref && p->entry.enc == DictEntryEnc::kWindowed;
    if (p->windowed) {
        uint64_t prelude_abs = 0;
        SNII_RETURN_IF_ERROR(posting_abs_offset(idx, p->frq_base, p->entry.frq_off_delta,
                                                "docid_conjunction: prelude offset overflow",
                                                &prelude_abs));
        p->prelude_handle = fetcher->add(prelude_abs, p->entry.prelude_len);
    } else if (p->pod_ref) {
        uint64_t foff = 0;
        uint64_t flen = 0;
        uint64_t poff = 0;
        uint64_t plen = 0;
        SNII_RETURN_IF_ERROR(idx.resolve_frq_window(p->entry, p->frq_base, &foff, &flen));
        uint64_t frq_fetch = flen;
        SNII_RETURN_IF_ERROR(slim_frq_docs_len(p->entry, flen, &frq_fetch));
        p->frq_handle = fetcher->add(foff, frq_fetch);
        if (need_positions) {
            SNII_RETURN_IF_ERROR(idx.resolve_prx_window(p->entry, p->prx_base, &poff, &plen));
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

std::vector<size_t> ascending_df_order(const std::vector<TermPlan>& plans) {
    std::vector<size_t> order(plans.size());
    for (size_t i = 0; i < plans.size(); ++i) order[i] = i;
    std::sort(order.begin(), order.end(),
              [&](size_t a, size_t b) { return plans[a].df < plans[b].df; });
    return order;
}

Status first_docid_in_window(const WindowMeta& meta, uint32_t window_ordinal, uint32_t* first) {
    if (window_ordinal == 0) {
        *first = 0;
        return Status::OK();
    }
    if (meta.win_base >= std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("docid_conjunction: window base exceeds docid range");
    }
    *first = static_cast<uint32_t>(meta.win_base + 1);
    if (*first > meta.last_docid) {
        return Status::Corruption("docid_conjunction: invalid window docid range");
    }
    return Status::OK();
}

Status is_dense_full_window(const WindowMeta& meta, uint32_t window_ordinal, bool* full) {
    uint32_t first = 0;
    SNII_RETURN_IF_ERROR(first_docid_in_window(meta, window_ordinal, &first));
    const uint64_t width = static_cast<uint64_t>(meta.last_docid) - first + 1;
    *full = meta.doc_count == width;
    return Status::OK();
}

Status append_docid_range(uint32_t first, uint32_t last, std::vector<uint32_t>* out) {
    if (last < first) {
        return Status::Corruption("docid_conjunction: invalid dense docid range");
    }
    const uint64_t count64 = static_cast<uint64_t>(last) - first + 1;
    if (count64 > static_cast<uint64_t>(std::numeric_limits<size_t>::max() - out->size())) {
        return Status::Corruption("docid_conjunction: dense docid range too large");
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

Status append_candidate_range_with_ordinals(CandidateIt begin, CandidateIt end, uint32_t first,
                                            uint32_t last, std::vector<uint32_t>* out,
                                            DocidChunk* chunk) {
    const size_t candidate_count = static_cast<size_t>(end - begin);
    chunk->docids.reserve(candidate_count);
    const uint64_t width = static_cast<uint64_t>(last) - first + 1;
    if (width > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("docid_conjunction: dense window exceeds doc count range");
    }
    chunk->prx_doc_count = static_cast<uint32_t>(width);
    const bool full_dense_range =
            candidate_count == width && begin != end && *begin == first && *(end - 1) == last;
    if (full_dense_range) {
        out->insert(out->end(), begin, end);
        chunk->docids.insert(chunk->docids.end(), begin, end);
        return Status::OK();
    }
    chunk->prx_doc_ordinals.reserve(candidate_count);
    for (auto it = begin; it != end; ++it) {
        out->push_back(*it);
        chunk->docids.push_back(*it);
        chunk->prx_doc_ordinals.push_back(*it - first);
    }
    return Status::OK();
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
    std::set_intersection(begin, end, term_docids.begin(), term_docids.end(),
                          std::back_inserter(*out));
}

Status intersect_window_candidate_range_with_ordinals(CandidateIt begin, CandidateIt end,
                                                      const std::vector<uint32_t>& term_docids,
                                                      std::vector<uint32_t>* out,
                                                      DocidChunk* chunk) {
    if (term_docids.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("docid_conjunction: prx doc count exceeds u32");
    }
    chunk->prx_doc_count = static_cast<uint32_t>(term_docids.size());
    if (begin == end || term_docids.empty()) return Status::OK();

    const size_t candidate_count = static_cast<size_t>(end - begin);
    const size_t max_matches = std::min(candidate_count, term_docids.size());
    out->reserve(out->size() + max_matches);
    chunk->docids.reserve(max_matches);
    if (candidate_count == term_docids.size() && *begin == term_docids.front() &&
        *(end - 1) == term_docids.back() && std::equal(begin, end, term_docids.begin())) {
        out->insert(out->end(), begin, end);
        chunk->docids.insert(chunk->docids.end(), begin, end);
        return Status::OK();
    }

    chunk->prx_doc_ordinals.reserve(max_matches);
    const size_t probes_per_candidate = log2_ceil(term_docids.size()) + 1;
    if (candidate_count < term_docids.size() / probes_per_candidate) {
        size_t doc_index = 0;
        for (auto it = begin; it != end; ++it) {
            const auto found =
                    std::lower_bound(term_docids.begin() + doc_index, term_docids.end(), *it);
            if (found == term_docids.end()) break;
            doc_index = static_cast<size_t>(found - term_docids.begin());
            if (*found != *it) continue;
            out->push_back(*it);
            chunk->docids.push_back(*it);
            chunk->prx_doc_ordinals.push_back(static_cast<uint32_t>(doc_index));
            ++doc_index;
        }
        if (chunk->docids.size() == term_docids.size() && !chunk->docids.empty() &&
            chunk->docids.front() == term_docids.front() &&
            chunk->docids.back() == term_docids.back()) {
            chunk->prx_doc_ordinals.clear();
        }
        return Status::OK();
    }

    const size_t probes_per_term_doc = log2_ceil(candidate_count) + 1;
    if (term_docids.size() < candidate_count / probes_per_term_doc) {
        auto candidate_it = begin;
        for (size_t doc_index = 0; doc_index < term_docids.size(); ++doc_index) {
            const uint32_t docid = term_docids[doc_index];
            candidate_it = std::lower_bound(candidate_it, end, docid);
            if (candidate_it == end) break;
            if (*candidate_it != docid) continue;
            out->push_back(docid);
            chunk->docids.push_back(docid);
            chunk->prx_doc_ordinals.push_back(static_cast<uint32_t>(doc_index));
            ++candidate_it;
        }
        if (chunk->docids.size() == term_docids.size() && !chunk->docids.empty() &&
            chunk->docids.front() == term_docids.front() &&
            chunk->docids.back() == term_docids.back()) {
            chunk->prx_doc_ordinals.clear();
        }
        return Status::OK();
    }

    size_t doc_index = 0;
    for (auto it = begin; it != end; ++it) {
        while (doc_index < term_docids.size() && term_docids[doc_index] < *it) {
            ++doc_index;
        }
        if (doc_index == term_docids.size()) break;
        if (term_docids[doc_index] != *it) continue;
        out->push_back(*it);
        chunk->docids.push_back(*it);
        chunk->prx_doc_ordinals.push_back(static_cast<uint32_t>(doc_index));
        ++doc_index;
    }
    if (chunk->docids.size() == term_docids.size() && !chunk->docids.empty() &&
        chunk->docids.front() == term_docids.front() &&
        chunk->docids.back() == term_docids.back()) {
        chunk->prx_doc_ordinals.clear();
    }
    return Status::OK();
}

Status select_covering_windows(const FrqPreludeReader& prelude,
                               const std::vector<uint32_t>& candidates,
                               std::vector<uint32_t>* windows) {
    std::vector<uint32_t> sel;
    uint32_t last = UINT32_MAX;
    for (uint32_t d : candidates) {
        bool found = false;
        uint32_t w = 0;
        SNII_RETURN_IF_ERROR(prelude.locate_window(d, &found, &w));
        if (!found) continue;
        if (w != last) {
            sel.push_back(w);
            last = w;
        }
    }
    *windows = std::move(sel);
    return Status::OK();
}

bool should_scan_all_windows(const LogicalIndexReader& idx, const TermPlan& p,
                             size_t candidate_count) {
    const size_t window_count = p.prelude.n_windows();
    if (candidate_count > window_count * 64) return true;

    const uint64_t doc_count = idx.stats().doc_count;
    const bool near_full = doc_count != 0 && static_cast<uint64_t>(p.df) * 10 >= doc_count * 9;
    return near_full && candidate_count > window_count * 4;
}

Status decode_flat_docids_only(const snii::io::BatchRangeFetcher& round1, const TermPlan& p,
                               std::vector<uint32_t>* docids) {
    Slice dd;
    if (p.pod_ref) {
        dd = round1.get(p.frq_handle);
    } else {
        SNII_RETURN_IF_ERROR(inline_dd_region(p.entry, &dd));
    }
    return snii::format::decode_dd_region(dd, p.entry.dd_meta, /*win_base=*/0, docids);
}

Status collect_windowed_docids_only(const LogicalIndexReader& idx, const TermPlan& p,
                                    const std::vector<uint32_t>& windows,
                                    const std::vector<uint32_t>* candidates,
                                    std::vector<uint32_t>* out, DocidSource* source) {
    struct FetchedWindow {
        uint32_t ordinal = 0;
        WindowMeta meta;
        CandidateRange candidates;
        size_t handle = 0;
    };

    snii::io::BatchRangeFetcher fetcher(idx.reader(), snii::reader::kSameTermCoalesceGap);
    std::vector<FetchedWindow> fetched;
    fetched.reserve(windows.size());
    out->reserve(candidates == nullptr ? p.entry.df : candidates->size());
    size_t candidate_search_begin = 0;
    for (uint32_t w : windows) {
        WindowMeta meta;
        SNII_RETURN_IF_ERROR(p.prelude.window(w, &meta));
        uint32_t first = 0;
        SNII_RETURN_IF_ERROR(first_docid_in_window(meta, w, &first));
        CandidateRange candidate_range;
        if (candidates != nullptr) {
            candidate_range = find_candidate_range(*candidates, &candidate_search_begin, first,
                                                   meta.last_docid);
            if (candidate_range.begin == candidate_range.end) {
                continue;
            }
        }
        bool dense_full = false;
        SNII_RETURN_IF_ERROR(is_dense_full_window(meta, w, &dense_full));
        if (dense_full) {
            if (source != nullptr) {
                DocidChunk chunk;
                chunk.windowed = true;
                chunk.window = w;
                chunk.prx_doc_count = meta.doc_count;
                if (candidates == nullptr) {
                    SNII_RETURN_IF_ERROR(append_docid_range(first, meta.last_docid, &chunk.docids));
                } else {
                    const auto begin = candidates->begin() + candidate_range.begin;
                    const auto end = candidates->begin() + candidate_range.end;
                    SNII_RETURN_IF_ERROR(append_candidate_range_with_ordinals(
                            begin, end, first, meta.last_docid, out, &chunk));
                }
                source->chunks.push_back(std::move(chunk));
            }
            if (candidates == nullptr) {
                SNII_RETURN_IF_ERROR(append_docid_range(first, meta.last_docid, out));
            } else if (source == nullptr) {
                append_candidate_range(candidates->begin() + candidate_range.begin,
                                       candidates->begin() + candidate_range.end, out);
            }
            continue;
        }

        snii::reader::WindowAbsRange range;
        SNII_RETURN_IF_ERROR(snii::reader::windowed_window_range(
                idx, p.entry, p.frq_base, p.prx_base, p.prelude, w,
                /*want_positions=*/false, /*want_freq=*/false, &range));
        FetchedWindow f;
        f.ordinal = w;
        f.meta = meta;
        f.candidates = candidate_range;
        f.handle = fetcher.add(range.dd_off, range.dd_len);
        fetched.push_back(f);
    }
    if (fetcher.pending() > 0) SNII_RETURN_IF_ERROR(fetcher.fetch());

    std::vector<uint32_t> docs;
    std::vector<uint32_t> freqs;
    std::vector<std::vector<uint32_t>> positions;
    for (const FetchedWindow& f : fetched) {
        docs.clear();
        freqs.clear();
        positions.clear();
        SNII_RETURN_IF_ERROR(snii::reader::decode_window_slices(
                f.meta, fetcher.get(f.handle), Slice(), Slice(),
                /*want_positions=*/false, /*want_freq=*/false, &docs, &freqs, &positions));
        if (source != nullptr) {
            DocidChunk chunk;
            chunk.windowed = true;
            chunk.window = f.ordinal;
            if (candidates == nullptr) {
                chunk.docids = docs;
                if (docs.size() > std::numeric_limits<uint32_t>::max()) {
                    return Status::Corruption("docid_conjunction: prx doc count exceeds u32");
                }
                chunk.prx_doc_count = static_cast<uint32_t>(docs.size());
                source->chunks.push_back(std::move(chunk));
            } else {
                const auto begin = candidates->begin() + f.candidates.begin;
                const auto end = candidates->begin() + f.candidates.end;
                SNII_RETURN_IF_ERROR(intersect_window_candidate_range_with_ordinals(
                        begin, end, docs, out, &chunk));
                if (!chunk.docids.empty()) source->chunks.push_back(std::move(chunk));
            }
        }
        if (candidates == nullptr) {
            out->insert(out->end(), docs.begin(), docs.end());
            continue;
        }
        if (source != nullptr) continue;
        uint32_t first = 0;
        SNII_RETURN_IF_ERROR(first_docid_in_window(f.meta, f.ordinal, &first));
        intersect_window_candidate_range(candidates->begin() + f.candidates.begin,
                                         candidates->begin() + f.candidates.end, docs, first,
                                         f.meta.last_docid, out);
    }
    return Status::OK();
}

Status collect_docids_only(const LogicalIndexReader& idx, const snii::io::BatchRangeFetcher& round1,
                           const TermPlan& p, const std::vector<uint32_t>* candidates,
                           std::vector<uint32_t>* out, DocidSource* source) {
    if (p.windowed) {
        std::vector<uint32_t> windows;
        if (candidates == nullptr) {
            windows = all_windows(p.prelude);
        } else if (should_scan_all_windows(idx, p, candidates->size())) {
            // Dense candidate sets cover most windows; for near-full terms this also
            // avoids thousands-to-millions of locate_window probes with no byte win.
            windows = all_windows(p.prelude);
        } else {
            SNII_RETURN_IF_ERROR(select_covering_windows(p.prelude, *candidates, &windows));
        }
        return collect_windowed_docids_only(idx, p, windows, candidates, out, source);
    }

    std::vector<uint32_t> term_docids;
    SNII_RETURN_IF_ERROR(decode_flat_docids_only(round1, p, &term_docids));
    if (source != nullptr) {
        DocidChunk chunk;
        if (term_docids.size() > std::numeric_limits<uint32_t>::max()) {
            return Status::Corruption("docid_conjunction: prx doc count exceeds u32");
        }
        chunk.prx_doc_count = static_cast<uint32_t>(term_docids.size());
        if (candidates == nullptr) {
            chunk.docids = term_docids;
        } else if (!term_docids.empty()) {
            const auto begin = std::ranges::lower_bound(*candidates, term_docids.front());
            const auto end = std::upper_bound(begin, candidates->end(), term_docids.back());
            SNII_RETURN_IF_ERROR(intersect_window_candidate_range_with_ordinals(
                    begin, end, term_docids, out, &chunk));
        }
        if (candidates == nullptr || !chunk.docids.empty())
            source->chunks.push_back(std::move(chunk));
    }
    if (candidates == nullptr) {
        *out = std::move(term_docids);
        return Status::OK();
    }
    if (source != nullptr) return Status::OK();
    *out = intersect_sorted(*candidates, term_docids);
    return Status::OK();
}

Status build_docid_only_conjunction_impl(const LogicalIndexReader& idx,
                                         const snii::io::BatchRangeFetcher& round1,
                                         const std::vector<TermPlan>& plans,
                                         std::vector<uint32_t>* candidates,
                                         std::vector<DocidSource>* sources) {
    if (sources != nullptr) sources->assign(plans.size(), DocidSource {});
    const std::vector<size_t> order = ascending_df_order(plans);
    for (size_t k = 0; k < order.size(); ++k) {
        const size_t ti = order[k];
        std::vector<uint32_t> next;
        DocidSource* source = sources == nullptr ? nullptr : &(*sources)[ti];
        SNII_RETURN_IF_ERROR(collect_docids_only(idx, round1, plans[ti],
                                                 k == 0 ? nullptr : candidates, &next, source));
        if (source != nullptr && k + 1 == order.size()) {
            source->docids_are_final_candidates = true;
        }
        *candidates = std::move(next);
        if (candidates->empty()) return Status::OK();
    }
    return Status::OK();
}

} // namespace

Status resolve_query_term(const LogicalIndexReader& idx, const std::string& term,
                          ResolvedQueryTerm* resolved, bool* found) {
    *found = false;
    SNII_RETURN_IF_ERROR(
            idx.lookup(term, found, &resolved->entry, &resolved->frq_base, &resolved->prx_base));
    return Status::OK();
}

Status plan_terms(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                  snii::io::BatchRangeFetcher* fetcher, std::vector<TermPlan>* plans,
                  bool* all_present, bool need_positions) {
    *all_present = true;
    plans->resize(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        ResolvedQueryTerm resolved;
        bool found = false;
        SNII_RETURN_IF_ERROR(resolve_query_term(idx, terms[i], &resolved, &found));
        if (!found) {
            *all_present = false;
            return Status::OK();
        }
        TermPlan& p = (*plans)[i];
        p.order = i;
        p.entry = std::move(resolved.entry);
        p.frq_base = resolved.frq_base;
        p.prx_base = resolved.prx_base;
        SNII_RETURN_IF_ERROR(configure_term_plan(idx, need_positions, fetcher, &p));
    }
    return Status::OK();
}

Status plan_resolved_terms(const LogicalIndexReader& idx,
                           const std::vector<ResolvedQueryTerm>& terms,
                           snii::io::BatchRangeFetcher* fetcher, std::vector<TermPlan>* plans,
                           bool need_positions) {
    plans->resize(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermPlan& p = (*plans)[i];
        p.order = i;
        p.entry = terms[i].entry;
        p.frq_base = terms[i].frq_base;
        p.prx_base = terms[i].prx_base;
        SNII_RETURN_IF_ERROR(configure_term_plan(idx, need_positions, fetcher, &p));
    }
    return Status::OK();
}

Status open_preludes(const snii::io::BatchRangeFetcher& fetcher, std::vector<TermPlan>* plans,
                     bool need_positions) {
    for (TermPlan& p : *plans) {
        if (!p.windowed) continue;
        SNII_RETURN_IF_ERROR(FrqPreludeReader::open(fetcher.get(p.prelude_handle), &p.prelude));
        if (need_positions && !p.prelude.has_prx()) {
            return Status::Corruption("docid_conjunction: windowed prelude has no positions");
        }
    }
    return Status::OK();
}

Status inline_dd_region(const DictEntry& entry, Slice* out) {
    if (entry.dd_meta.disk_len > entry.frq_bytes.size()) {
        return Status::Corruption("docid_conjunction: inline dd region exceeds frq bytes");
    }
    *out = Slice(entry.frq_bytes.data(), static_cast<size_t>(entry.dd_meta.disk_len));
    return Status::OK();
}

Status build_docid_only_conjunction(const LogicalIndexReader& idx,
                                    const snii::io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates) {
    return build_docid_only_conjunction_impl(idx, round1, plans, candidates, nullptr);
}

Status build_docid_only_conjunction(const LogicalIndexReader& idx,
                                    const snii::io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates,
                                    std::vector<DocidSource>* sources) {
    return build_docid_only_conjunction_impl(idx, round1, plans, candidates, sources);
}

} // namespace snii::query::internal
