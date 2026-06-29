#include "snii/query/phrase_query.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "snii/common/slice.h"
#include "snii/encoding/byte_source.h"
#include "snii/format/dict_entry.h"
#include "snii/format/frq_pod.h"
#include "snii/format/frq_prelude.h"
#include "snii/format/phrase_bigram.h"
#include "snii/format/prx_pod.h"
#include "snii/io/batch_range_fetcher.h"
#include "snii/query/internal/docid_conjunction.h"
#include "snii/query/internal/docid_posting_reader.h"
#include "snii/query/internal/docid_set_ops.h"
#include "snii/query/internal/position_math.h"
#include "snii/query/prefix_query.h"
#include "snii/query/term_query.h"
#include "snii/reader/windowed_posting.h"

// phrase_query implements MATCH_PHRASE with WINDOW (sub-block) SKIPPING for
// high-df windowed terms (design spec section 6.2):
//   1. Resolve every term; reject if any is absent.
//   2. Batch-read each windowed term's prelude + each slim/inline term's full
//      docid posting in one round; open the two-level prelude readers.
//   3. Pick the DRIVER = smallest-df term; materialize it fully -> the initial
//      candidate docid set.
//   4. For every other term in ascending-df order, narrow the candidate set:
//        - slim/inline: intersect with its (already decoded) full posting.
//        - windowed:    locate_window() the CURRENT candidates -> the SET of
//                       windows covering them; batch-fetch ONLY those windows'
//                       .frq docid regions; keep candidates present in some
//                       covering window. A high-df term thus reads
//                       O(candidates) windows instead of its whole O(df)
//                       posting.
//   5. Fetch PRX only for retained chunks and run the positional phrase check
//      (term[0]@p, term[1]@p+1, ...) on the survivors.
// The result is identical to a full-read intersection; only the bytes read for
// high-df windowed terms shrink.
namespace snii::query {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

using snii::query::internal::DocidChunk;
using snii::query::internal::DocidSource;
using snii::query::internal::ResolvedQueryTerm;
using snii::query::internal::TermPlan;
using snii::reader::LogicalIndexReader;

namespace {

struct ExpectedTailPositions {
    uint32_t docid = 0;
    size_t positions_begin = 0;
    size_t positions_end = 0;
};

struct ExpectedTailPositionSet {
    std::vector<ExpectedTailPositions> docs;
    std::vector<uint32_t> positions;

    void clear() {
        docs.clear();
        positions.clear();
    }

    void reserve_docs(size_t count) {
        docs.reserve(count);
        positions.reserve(count);
    }
};

// One decoded chunk of a term's posting: a windowed term's covering window, or
// a slim/inline term's single posting. `docids` is decoded in the conjunction
// phase (and reused by the streaming cursor -- the dd region is decoded exactly
// once); `prx` is the on-disk positions bytes, decoded lazily by the cursor
// (once per chunk) during phrase verification.
struct PosChunk {
    std::vector<uint32_t> docids; // ascending, absolute
    // Empty means the chunk keeps every PRX doc in on-disk order. Non-empty means
    // `docids[i]` corresponds to on-disk local document ordinal
    // `prx_doc_ordinals[i]`, allowing PRX decode to skip positions for docs that
    // were removed by the docid-only conjunction.
    std::vector<uint32_t> prx_doc_ordinals;
    uint32_t prx_doc_count = 0;
    Slice prx; // .prx window bytes (reference fetcher/round1/entry)
    bool windowed = false;
    uint32_t window = 0;
};

// A term's retained posting as an ordered list of chunks (windowed: covering
// windows in docid order; slim/inline: one). The referenced prx bytes live in
// `round1` / the per-term fetchers kept alive in phrase_query::owners for the
// whole query, so the cursor can decode positions during verification.
struct PosSource {
    std::vector<PosChunk> chunks;
};

struct PhraseExecutionState {
    std::vector<PosSource> srcs;
    std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>> owners;
    std::vector<uint32_t> candidates;
};

struct PhraseTermMapping {
    std::vector<std::string> unique_terms;
    std::vector<size_t> phrase_plan_index;
};

PhraseTermMapping BuildPhraseTermMapping(const std::vector<std::string>& terms) {
    PhraseTermMapping mapping;
    mapping.phrase_plan_index.reserve(terms.size());
    for (const std::string& term : terms) {
        auto it = std::find(mapping.unique_terms.begin(), mapping.unique_terms.end(), term);
        if (it == mapping.unique_terms.end()) {
            mapping.phrase_plan_index.push_back(mapping.unique_terms.size());
            mapping.unique_terms.push_back(term);
            continue;
        }
        mapping.phrase_plan_index.push_back(static_cast<size_t>(it - mapping.unique_terms.begin()));
    }
    return mapping;
}

doris::Status phrase_bigram_enabled(const LogicalIndexReader& idx, bool* enabled) {
    ResolvedQueryTerm sentinel;
    return internal::resolve_query_term(idx, snii::format::make_phrase_bigram_sentinel_term(),
                                        &sentinel, enabled);
}

doris::Status TryTwoTermPhraseBigram(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                              std::vector<uint32_t>* const docids, bool* handled) {
    *handled = false;
    if (terms.size() != 2) {
        return doris::Status::OK();
    }
    if (!snii::format::is_phrase_bigram_indexable_term(terms[0]) ||
        !snii::format::is_phrase_bigram_indexable_term(terms[1])) {
        return doris::Status::OK();
    }

    ResolvedQueryTerm resolved;
    bool found = false;
    RETURN_IF_ERROR(internal::resolve_query_term(
            idx, snii::format::make_phrase_bigram_term(terms[0], terms[1]), &resolved, &found));
    if (found) {
        *handled = true;
        return internal::read_docid_posting(idx, resolved.entry, resolved.frq_base,
                                            resolved.prx_base, docids);
    }

    bool enabled = false;
    RETURN_IF_ERROR(phrase_bigram_enabled(idx, &enabled));
    if (!enabled) {
        return doris::Status::OK();
    }
    docids->clear();
    *handled = true;
    return doris::Status::OK();
}

doris::Status append_prx_doc_ordinal(size_t ordinal, std::vector<uint32_t>* out) {
    if (ordinal > std::numeric_limits<uint32_t>::max()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: prx doc ordinal exceeds u32");
    }
    out->push_back(static_cast<uint32_t>(ordinal));
    return doris::Status::OK();
}

doris::Status append_selected_ordinal(size_t doc_index, const std::vector<uint32_t>& prx_doc_ordinals,
                               std::vector<uint32_t>* selected_ordinals) {
    if (!prx_doc_ordinals.empty()) {
        selected_ordinals->push_back(prx_doc_ordinals[doc_index]);
        return doris::Status::OK();
    }
    return append_prx_doc_ordinal(doc_index, selected_ordinals);
}

doris::Status append_selected_doc(size_t doc_index, uint32_t docid,
                           const std::vector<uint32_t>& prx_doc_ordinals,
                           std::vector<uint32_t>* selected_docids,
                           std::vector<uint32_t>* selected_ordinals) {
    selected_docids->push_back(docid);
    return append_selected_ordinal(doc_index, prx_doc_ordinals, selected_ordinals);
}

doris::Status materialize_selected_prefix(size_t count, size_t capacity,
                                   const std::vector<uint32_t>& docids,
                                   const std::vector<uint32_t>& prx_doc_ordinals,
                                   std::vector<uint32_t>* selected_docids,
                                   std::vector<uint32_t>* selected_ordinals) {
    selected_docids->reserve(capacity);
    selected_ordinals->reserve(capacity);
    selected_docids->insert(selected_docids->end(), docids.begin(), docids.begin() + count);
    for (size_t i = 0; i < count; ++i) {
        RETURN_IF_ERROR(append_selected_ordinal(i, prx_doc_ordinals, selected_ordinals));
    }
    return doris::Status::OK();
}

doris::Status materialize_selected_prefix_if_needed(bool* selected_all, size_t count, size_t capacity,
                                             const std::vector<uint32_t>& docids,
                                             const std::vector<uint32_t>& prx_doc_ordinals,
                                             std::vector<uint32_t>* selected_docids,
                                             std::vector<uint32_t>* selected_ordinals) {
    if (!*selected_all) {
        return doris::Status::OK();
    }
    *selected_all = false;
    return materialize_selected_prefix(count, capacity, docids, prx_doc_ordinals, selected_docids,
                                       selected_ordinals);
}

doris::Status SelectCandidateDocsForPrx(std::vector<uint32_t>* docids,
                                 std::vector<uint32_t>* prx_doc_ordinals, uint32_t prx_doc_count,
                                 const std::vector<uint32_t>& candidates, PosChunk* chunk) {
    chunk->docids.clear();
    chunk->prx_doc_ordinals.clear();
    if (prx_doc_count == 0 && docids->size() > std::numeric_limits<uint32_t>::max()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: prx doc count exceeds u32");
    }
    chunk->prx_doc_count =
            prx_doc_count == 0 ? static_cast<uint32_t>(docids->size()) : prx_doc_count;
    if (docids->empty() || candidates.empty()) {
        return doris::Status::OK();
    }
    if (!prx_doc_ordinals->empty() && prx_doc_ordinals->size() != docids->size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: prx ordinal/docid count mismatch");
    }

    std::vector<uint32_t> selected_docids;
    std::vector<uint32_t> selected_ordinals;
    bool selected_all = true;
    const size_t selected_capacity = std::min(docids->size(), candidates.size());

    auto candidate_it = std::ranges::lower_bound(candidates, docids->front());
    size_t candidate_index = static_cast<size_t>(candidate_it - candidates.begin());
    for (size_t doc_index = 0; doc_index < docids->size(); ++doc_index) {
        const uint32_t docid = (*docids)[doc_index];
        while (candidate_index < candidates.size() && candidates[candidate_index] < docid) {
            ++candidate_index;
        }
        if (candidate_index == candidates.size()) {
            RETURN_IF_ERROR(materialize_selected_prefix_if_needed(
                    &selected_all, doc_index, selected_capacity, *docids, *prx_doc_ordinals,
                    &selected_docids, &selected_ordinals));
            break;
        }
        if (candidates[candidate_index] != docid) {
            RETURN_IF_ERROR(materialize_selected_prefix_if_needed(
                    &selected_all, doc_index, selected_capacity, *docids, *prx_doc_ordinals,
                    &selected_docids, &selected_ordinals));
            continue;
        }

        if (!selected_all) {
            RETURN_IF_ERROR(append_selected_doc(doc_index, docid, *prx_doc_ordinals,
                                                     &selected_docids, &selected_ordinals));
        }
        ++candidate_index;
    }

    if (selected_all) {
        chunk->docids = std::move(*docids);
        chunk->prx_doc_ordinals = std::move(*prx_doc_ordinals);
        docids->clear();
        prx_doc_ordinals->clear();
        return doris::Status::OK();
    }
    if (selected_docids.empty()) {
        return doris::Status::OK();
    }
    chunk->docids = std::move(selected_docids);
    chunk->prx_doc_ordinals = std::move(selected_ordinals);
    return doris::Status::OK();
}

doris::Status BuildFlatPositionSource(const LogicalIndexReader& idx,
                               const snii::io::BatchRangeFetcher& round1, DocidSource* doc_source,
                               const TermPlan& p, const std::vector<uint32_t>& candidates,
                               std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>>* owners,
                               PosSource* src) {
    PosChunk chunk;
    std::vector<uint32_t> docids;
    std::vector<uint32_t> prx_doc_ordinals;
    const bool docids_are_final_candidates =
            doc_source->docids_are_final_candidates && !doc_source->chunks.empty();
    if (!doc_source->chunks.empty()) {
        DocidChunk& doc_chunk = doc_source->chunks.front();
        docids = std::move(doc_chunk.docids);
        prx_doc_ordinals = std::move(doc_chunk.prx_doc_ordinals);
        chunk.prx_doc_count = doc_chunk.prx_doc_count;
    }
    if (p.pod_ref) {
        uint64_t poff = 0;
        uint64_t plen = 0;
        RETURN_IF_ERROR(idx.resolve_prx_window(p.entry, p.prx_base, &poff, &plen));
        auto fetcher = std::make_unique<snii::io::BatchRangeFetcher>(idx.reader());
        const size_t prx_handle = fetcher->add(poff, plen);
        RETURN_IF_ERROR(fetcher->fetch());
        chunk.prx = fetcher->get(prx_handle);
        owners->push_back(std::move(fetcher));
    } else {
        chunk.prx = Slice(p.entry.prx_bytes);
    }
    if (docids.empty()) {
        Slice dd;
        if (p.pod_ref) {
            dd = round1.get(p.frq_handle);
        } else {
            RETURN_IF_ERROR(internal::inline_dd_region(p.entry, &dd));
        }
        RETURN_IF_ERROR(snii::format::decode_dd_region(dd, p.entry.dd_meta,
                                                            /*win_base=*/0, &docids));
        if (docids.size() > std::numeric_limits<uint32_t>::max()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: prx doc count exceeds u32");
        }
        chunk.prx_doc_count = static_cast<uint32_t>(docids.size());
    }
    if (docids_are_final_candidates) {
        chunk.docids = std::move(docids);
        chunk.prx_doc_ordinals = std::move(prx_doc_ordinals);
        if (!chunk.docids.empty()) src->chunks.push_back(std::move(chunk));
        return doris::Status::OK();
    }
    RETURN_IF_ERROR(SelectCandidateDocsForPrx(&docids, &prx_doc_ordinals, chunk.prx_doc_count,
                                                   candidates, &chunk));
    if (!chunk.docids.empty()) src->chunks.push_back(std::move(chunk));
    return doris::Status::OK();
}

bool ChunkMayContainCandidate(const DocidChunk& chunk, const std::vector<uint32_t>& candidates) {
    if (chunk.docids.empty() || candidates.empty()) return false;
    const auto it = std::lower_bound(candidates.begin(), candidates.end(), chunk.docids.front());
    return it != candidates.end() && *it <= chunk.docids.back();
}

doris::Status DecodeWindowedPositionSource(
        const LogicalIndexReader& idx, const TermPlan& p, DocidSource* doc_source,
        const std::vector<uint32_t>& candidates,
        std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>>* owners, PosSource* src) {
    struct WindowFetch {
        size_t chunk_index = 0;
        size_t prx_handle = 0;
    };

    auto prx_fetcher = std::make_unique<snii::io::BatchRangeFetcher>(
            idx.reader(), snii::reader::kSameTermCoalesceGap);
    std::vector<WindowFetch> fetched;
    fetched.reserve(doc_source->chunks.size());
    for (size_t i = 0; i < doc_source->chunks.size(); ++i) {
        DocidChunk& doc_chunk = doc_source->chunks[i];
        if (!doc_source->docids_are_final_candidates &&
            !ChunkMayContainCandidate(doc_chunk, candidates)) {
            continue;
        }
        if (!doc_chunk.windowed) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: expected windowed doc chunk");
        }
        PosChunk chunk;
        if (doc_source->docids_are_final_candidates) {
            chunk.docids = std::move(doc_chunk.docids);
            chunk.prx_doc_ordinals = std::move(doc_chunk.prx_doc_ordinals);
            chunk.prx_doc_count = doc_chunk.prx_doc_count;
        } else {
            RETURN_IF_ERROR(
                    SelectCandidateDocsForPrx(&doc_chunk.docids, &doc_chunk.prx_doc_ordinals,
                                              doc_chunk.prx_doc_count, candidates, &chunk));
        }
        if (chunk.docids.empty()) continue;

        snii::reader::WindowAbsRange range;
        RETURN_IF_ERROR(snii::reader::windowed_window_range(
                idx, p.entry, p.frq_base, p.prx_base, p.prelude, doc_chunk.window,
                /*want_positions=*/true, /*want_freq=*/false, &range));
        chunk.windowed = true;
        chunk.window = doc_chunk.window;
        WindowFetch f;
        f.chunk_index = src->chunks.size();
        f.prx_handle = prx_fetcher->add(range.prx_off, range.prx_len);
        fetched.push_back(f);
        src->chunks.push_back(std::move(chunk));
    }
    if (prx_fetcher->pending() > 0) RETURN_IF_ERROR(prx_fetcher->fetch());

    for (const WindowFetch& f : fetched) {
        src->chunks[f.chunk_index].prx = prx_fetcher->get(f.prx_handle);
    }
    if (!fetched.empty()) owners->push_back(std::move(prx_fetcher));
    return doris::Status::OK();
}

doris::Status BuildPositionSourcesForCandidates(
        const LogicalIndexReader& idx, const snii::io::BatchRangeFetcher& round1,
        const std::vector<TermPlan>& plans, std::vector<DocidSource>* doc_sources,
        const std::vector<uint32_t>& candidates,
        std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>>* owners,
        std::vector<PosSource>* srcs) {
    srcs->assign(plans.size(), PosSource {});
    for (size_t i = 0; i < plans.size(); ++i) {
        const TermPlan& p = plans[i];
        if (p.windowed) {
            RETURN_IF_ERROR(DecodeWindowedPositionSource(idx, p, &(*doc_sources)[i],
                                                              candidates, owners, &(*srcs)[i]));
            continue;
        }
        RETURN_IF_ERROR(BuildFlatPositionSource(idx, round1, &(*doc_sources)[i], p, candidates,
                                                     owners, &(*srcs)[i]));
    }
    return doris::Status::OK();
}

class PosChunkDecoder {
public:
    void reset() {
        chunk_ = nullptr;
        offsets_by_prx_ordinal_ = false;
    }

    doris::Status decode(const PosChunk& chunk) {
        chunk_ = &chunk;
        ByteSource ps(chunk.prx);
        offsets_by_prx_ordinal_ = false;
        if (chunk.prx_doc_ordinals.empty()) {
            RETURN_IF_ERROR(snii::format::read_prx_window_csr(&ps, &pflat_, &poff_));
        } else if (should_decode_full_prx_window(chunk)) {
            RETURN_IF_ERROR(snii::format::read_prx_window_csr(&ps, &pflat_, &poff_));
            offsets_by_prx_ordinal_ = true;
        } else {
            RETURN_IF_ERROR(snii::format::read_prx_window_csr_selective(
                    &ps, chunk.prx_doc_ordinals, &pflat_, &poff_));
        }
        if (offsets_by_prx_ordinal_) {
            if (poff_.size() != static_cast<size_t>(chunk.prx_doc_count) + 1) {
                return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: full prx doc-count mismatch");
            }
        } else if (poff_.size() != chunk.docids.size() + 1) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: selected prx/doc-count mismatch");
        }
        if (poff_.back() > pflat_.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: prx final offset out of range");
        }
        return doris::Status::OK();
    }

    doris::Status positions(size_t doc_index, std::pair<const uint32_t*, const uint32_t*>* out) const {
        if (chunk_ == nullptr || doc_index >= chunk_->docids.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: decoded chunk doc index out of range");
        }
        const size_t pos_index =
                offsets_by_prx_ordinal_ ? chunk_->prx_doc_ordinals[doc_index] : doc_index;
        if (pos_index + 1 >= poff_.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: prx ordinal offset out of range");
        }
        const uint32_t begin = poff_[pos_index];
        const uint32_t end = poff_[pos_index + 1];
        if (begin == end) {
            *out = {nullptr, nullptr};
            return doris::Status::OK();
        }
        if (end > pflat_.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: prx offset out of range");
        }
        *out = {pflat_.data() + begin, pflat_.data() + end};
        return doris::Status::OK();
    }

    inline __attribute__((always_inline)) std::pair<const uint32_t*, const uint32_t*>
    positions_unchecked(size_t doc_index) const {
        const size_t pos_index =
                offsets_by_prx_ordinal_ ? chunk_->prx_doc_ordinals[doc_index] : doc_index;
        const uint32_t begin = poff_[pos_index];
        const uint32_t end = poff_[pos_index + 1];
        if (begin == end) {
            return {nullptr, nullptr};
        }
        return {pflat_.data() + begin, pflat_.data() + end};
    }

private:
    static bool should_decode_full_prx_window(const PosChunk& chunk) {
        return chunk.prx_doc_count != 0 &&
               static_cast<uint64_t>(chunk.prx_doc_ordinals.size()) * 2 >= chunk.prx_doc_count;
    }

    const PosChunk* chunk_ = nullptr;
    bool offsets_by_prx_ordinal_ = false;
    std::vector<uint32_t> pflat_;
    std::vector<uint32_t> poff_;
};

// Streaming position cursor over one term's retained chunks. It advances ONLY
// forward (callers seek ascending candidate docids), decodes each chunk's
// docids once (reused from the conjunction phase) and each chunk's positions at
// most once (lazily, into a flat CSR whose capacity is retained across chunks).
// No per-doc allocation, no per-candidate docid binary search: positions are
// addressed by the doc's local index within its chunk. This is the read-side
// dual of the windowed posting layout -- the S3-native batch fetch already
// pulled every needed chunk into memory; the cursor is pure in-memory column
// iteration.
class PostingCursor {
public:
    void init(const PosSource* src) {
        src_ = src;
        ci_ = 0;
        li_ = 0;
        decoded_pos_chunk_ = kNoChunk;
        decoder_.reset();
    }

    // Positions the cursor at `target` (guaranteed present: candidates are the
    // intersection of exactly these chunks' docids). Monotonic forward advance.
    doris::Status seek(uint32_t target) {
        while (ci_ < src_->chunks.size() &&
               (src_->chunks[ci_].docids.empty() || src_->chunks[ci_].docids.back() < target)) {
            ++ci_;
            li_ = 0;
        }
        if (ci_ >= src_->chunks.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: cursor exhausted before target docid");
        }
        const std::vector<uint32_t>& d = src_->chunks[ci_].docids;
        while (li_ < d.size() && d[li_] < target) ++li_;
        if (li_ >= d.size() || d[li_] != target) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: candidate missing from posting chunk");
        }
        return doris::Status::OK();
    }

    // [begin,end) of the current doc's positions, decoding the current chunk's
    // .prx exactly once (cached). Must follow a seek that landed on a real doc.
    doris::Status positions(std::pair<const uint32_t*, const uint32_t*>* out) {
        if (ci_ >= src_->chunks.size() || li_ >= src_->chunks[ci_].docids.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: cursor positions out of range");
        }
        if (decoded_pos_chunk_ != ci_) {
            RETURN_IF_ERROR(decoder_.decode(src_->chunks[ci_]));
            decoded_pos_chunk_ = ci_;
        }
        return decoder_.positions(li_, out);
    }

    doris::Status next(uint32_t* docid, std::pair<const uint32_t*, const uint32_t*>* out) {
        while (ci_ < src_->chunks.size() &&
               (src_->chunks[ci_].docids.empty() || li_ >= src_->chunks[ci_].docids.size())) {
            ++ci_;
            li_ = 0;
        }
        if (ci_ >= src_->chunks.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: cursor exhausted before next docid");
        }
        *docid = src_->chunks[ci_].docids[li_];
        RETURN_IF_ERROR(positions(out));
        ++li_;
        return doris::Status::OK();
    }

private:
    static constexpr size_t kNoChunk = static_cast<size_t>(-1);

    const PosSource* src_ = nullptr;
    size_t ci_ = 0;                       // current chunk
    size_t li_ = 0;                       // current local doc index within the chunk
    size_t decoded_pos_chunk_ = kNoChunk; // which chunk decoder_ currently holds
    PosChunkDecoder decoder_;
};

class PhrasePositionLoader {
public:
    PhrasePositionLoader(size_t plan_count, std::vector<PosSource>& srcs)
            : cursors_(plan_count), plan_spans_(plan_count), loaded_epoch_(plan_count, 0) {
        for (size_t i = 0; i < plan_count; ++i) {
            cursors_[i].init(&srcs[i]);
        }
    }

    void begin_doc(uint32_t docid) {
        docid_ = docid;
        ++epoch_;
        if (epoch_ == 0) {
            std::ranges::fill(loaded_epoch_, 0);
            epoch_ = 1;
        }
    }

    doris::Status positions_for_phrase_pos(const std::vector<size_t>& phrase_plan_index, size_t phrase_pos,
                                    std::pair<const uint32_t*, const uint32_t*>* out) {
        const size_t plan_index = phrase_plan_index[phrase_pos];
        if (loaded_epoch_[plan_index] != epoch_) {
            RETURN_IF_ERROR(cursors_[plan_index].seek(docid_));
            RETURN_IF_ERROR(cursors_[plan_index].positions(&plan_spans_[plan_index]));
            loaded_epoch_[plan_index] = epoch_;
        }
        *out = plan_spans_[plan_index];
        return doris::Status::OK();
    }

private:
    std::vector<PostingCursor> cursors_;
    std::vector<std::pair<const uint32_t*, const uint32_t*>> plan_spans_;
    std::vector<uint32_t> loaded_epoch_;
    uint32_t docid_ = 0;
    uint32_t epoch_ = 0;
};

bool ContainsTwoTermPhrase(std::pair<const uint32_t*, const uint32_t*> left_span,
                           std::pair<const uint32_t*, const uint32_t*> right_span,
                           uint32_t right_delta) {
    const uint32_t* left = left_span.first;
    const uint32_t* right = right_span.first;
    if (left == left_span.second || right == right_span.second) {
        return false;
    }
    const uint32_t max_start = std::numeric_limits<uint32_t>::max() - right_delta;
    if (left + 1 == left_span.second && right + 1 == right_span.second) {
        return *left <= max_start && *right == *left + right_delta;
    }
    while (left != left_span.second && right != right_span.second) {
        if (*left > max_start) {
            return false;
        }
        const uint32_t want = *left + right_delta;
        while (right != right_span.second && *right < want) {
            ++right;
        }
        if (right == right_span.second) {
            return false;
        }
        if (*right == want) {
            return true;
        }
        ++left;
    }
    return false;
}

size_t SelectPhraseVerificationPair(const std::vector<TermPlan>& plans,
                                    const std::vector<size_t>& phrase_plan_index) {
    size_t best_left = 0;
    uint64_t best_score = std::numeric_limits<uint64_t>::max();
    for (size_t left = 0; left + 1 < phrase_plan_index.size(); ++left) {
        const uint64_t score = static_cast<uint64_t>(plans[phrase_plan_index[left]].df) +
                               plans[phrase_plan_index[left + 1]].df;
        if (score < best_score) {
            best_score = score;
            best_left = left;
        }
    }
    return best_left;
}

void CollectTwoTermPhraseStarts(std::pair<const uint32_t*, const uint32_t*> left_span,
                                std::pair<const uint32_t*, const uint32_t*> right_span,
                                uint32_t right_delta, uint32_t left_offset,
                                std::vector<uint32_t>& starts) {
    starts.clear();
    const uint32_t* left = left_span.first;
    const uint32_t* right = right_span.first;
    const uint32_t max_left = std::numeric_limits<uint32_t>::max() - right_delta;
    while (left != left_span.second && right != right_span.second) {
        if (*left > max_left) {
            return;
        }
        const uint32_t want = *left + right_delta;
        while (right != right_span.second && *right < want) {
            ++right;
        }
        if (right == right_span.second) {
            return;
        }
        if (*right == want && *left >= left_offset) {
            starts.push_back(*left - left_offset);
        }
        ++left;
    }
}

doris::Status EmitTwoTermPhraseStreaming(const std::vector<size_t>& phrase_plan_index,
                                  const std::vector<uint32_t>& position_offsets,
                                  std::vector<PosSource>& srcs,
                                  const std::vector<uint32_t>& candidates,
                                  std::vector<uint32_t>* docids) {
    const size_t left_plan = phrase_plan_index[0];
    const size_t right_plan = phrase_plan_index[1];
    const uint32_t right_delta = position_offsets[1] - position_offsets[0];

    if (left_plan == right_plan) {
        PostingCursor cursor;
        cursor.init(&srcs[left_plan]);
        for (uint32_t expected_docid : candidates) {
            uint32_t docid = 0;
            std::pair<const uint32_t*, const uint32_t*> span;
            RETURN_IF_ERROR(cursor.next(&docid, &span));
            if (docid != expected_docid) {
                return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: repeated-term cursor/docid mismatch");
            }
            if (ContainsTwoTermPhrase(span, span, right_delta)) {
                docids->push_back(docid);
            }
        }
        return doris::Status::OK();
    }

    PostingCursor left_cursor;
    PostingCursor right_cursor;
    left_cursor.init(&srcs[left_plan]);
    right_cursor.init(&srcs[right_plan]);
    for (uint32_t expected_docid : candidates) {
        uint32_t left_docid = 0;
        uint32_t right_docid = 0;
        std::pair<const uint32_t*, const uint32_t*> left_span;
        std::pair<const uint32_t*, const uint32_t*> right_span;
        RETURN_IF_ERROR(left_cursor.next(&left_docid, &left_span));
        RETURN_IF_ERROR(right_cursor.next(&right_docid, &right_span));
        if (left_docid != expected_docid || right_docid != expected_docid) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("phrase_query: two-term cursor/docid mismatch");
        }
        if (ContainsTwoTermPhrase(left_span, right_span, right_delta)) {
            docids->push_back(expected_docid);
        }
    }
    return doris::Status::OK();
}

void EmitTwoTermPhraseChunkPair(const PosChunk& left, const PosChunk& right,
                                const PosChunkDecoder& left_decoder,
                                const PosChunkDecoder& right_decoder, uint32_t right_delta,
                                std::vector<uint32_t>& docids) {
    size_t li = static_cast<size_t>(
            std::lower_bound(left.docids.begin(), left.docids.end(), right.docids.front()) -
            left.docids.begin());
    size_t ri = static_cast<size_t>(
            std::lower_bound(right.docids.begin(), right.docids.end(), left.docids.front()) -
            right.docids.begin());
    while (li < left.docids.size() && ri < right.docids.size()) {
        const uint32_t left_docid = left.docids[li];
        const uint32_t right_docid = right.docids[ri];
        if (left_docid < right_docid) {
            ++li;
            continue;
        }
        if (right_docid < left_docid) {
            ++ri;
            continue;
        }

        const std::pair<const uint32_t*, const uint32_t*> left_span =
                left_decoder.positions_unchecked(li);
        const std::pair<const uint32_t*, const uint32_t*> right_span =
                right_decoder.positions_unchecked(ri);
        if (ContainsTwoTermPhrase(left_span, right_span, right_delta)) {
            docids.push_back(left_docid);
        }
        ++li;
        ++ri;
    }
}

doris::Status EmitTwoTermPhraseChunkMerge(const std::vector<size_t>& phrase_plan_index,
                                   const std::vector<uint32_t>& position_offsets,
                                   std::vector<PosSource>& srcs,
                                   std::vector<uint32_t>* const docids) {
    const size_t left_plan = phrase_plan_index[0];
    const size_t right_plan = phrase_plan_index[1];
    const uint32_t right_delta = position_offsets[1] - position_offsets[0];
    const PosSource& left_src = srcs[left_plan];
    const PosSource& right_src = srcs[right_plan];

    PosChunkDecoder left_decoder;
    PosChunkDecoder right_decoder;
    size_t decoded_left_chunk = static_cast<size_t>(-1);
    size_t decoded_right_chunk = static_cast<size_t>(-1);
    size_t left_chunk = 0;
    size_t right_chunk = 0;
    while (left_chunk < left_src.chunks.size() && right_chunk < right_src.chunks.size()) {
        const PosChunk& left = left_src.chunks[left_chunk];
        const PosChunk& right = right_src.chunks[right_chunk];
        if (left.docids.empty()) {
            ++left_chunk;
            continue;
        }
        if (right.docids.empty()) {
            ++right_chunk;
            continue;
        }
        if (left.docids.back() < right.docids.front()) {
            ++left_chunk;
            continue;
        }
        if (right.docids.back() < left.docids.front()) {
            ++right_chunk;
            continue;
        }

        if (decoded_left_chunk != left_chunk) {
            RETURN_IF_ERROR(left_decoder.decode(left));
            decoded_left_chunk = left_chunk;
        }
        if (decoded_right_chunk != right_chunk) {
            RETURN_IF_ERROR(right_decoder.decode(right));
            decoded_right_chunk = right_chunk;
        }

        EmitTwoTermPhraseChunkPair(left, right, left_decoder, right_decoder, right_delta, *docids);

        const uint32_t left_last = left.docids.back();
        const uint32_t right_last = right.docids.back();
        if (left_last <= right_last) {
            ++left_chunk;
        }
        if (right_last <= left_last) {
            ++right_chunk;
        }
    }
    return doris::Status::OK();
}

bool PhraseStartMatchesAllTerms(
        uint32_t start, size_t phrase_len, size_t pair_left, size_t pair_right,
        const std::vector<uint32_t>& position_offsets,
        const std::vector<std::pair<const uint32_t*, const uint32_t*>>& span) {
    for (size_t t = 0; t < phrase_len; ++t) {
        if (t == pair_left || t == pair_right) {
            continue;
        }
        uint32_t want = 0;
        if (!internal::add_position_offset(start, position_offsets[t], &want)) {
            return false;
        }
        if (!std::binary_search(span[t].first, span[t].second, want)) {
            return false;
        }
    }
    return true;
}

doris::Status EmitSingleTermPhraseStreaming(const std::vector<size_t>& phrase_plan_index,
                                     std::vector<PosSource>& srcs,
                                     const std::vector<uint32_t>& candidates,
                                     std::vector<uint32_t>* docids) {
    PhrasePositionLoader loader(srcs.size(), srcs);
    for (uint32_t d : candidates) {
        loader.begin_doc(d);
        std::pair<const uint32_t*, const uint32_t*> single_span;
        RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, 0, &single_span));
        if (single_span.first != single_span.second) {
            docids->push_back(d);
        }
    }
    return doris::Status::OK();
}

doris::Status EmitMultiTermPhraseStreaming(const std::vector<TermPlan>& plans,
                                    const std::vector<size_t>& phrase_plan_index,
                                    const std::vector<uint32_t>& position_offsets,
                                    std::vector<PosSource>& srcs,
                                    const std::vector<uint32_t>& candidates,
                                    std::vector<uint32_t>* docids) {
    const size_t phrase_len = phrase_plan_index.size();
    PhrasePositionLoader loader(plans.size(), srcs);
    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(phrase_len);
    std::vector<uint32_t> starts;
    const size_t pair_left = SelectPhraseVerificationPair(plans, phrase_plan_index);
    const size_t pair_right = pair_left + 1;
    for (uint32_t d : candidates) {
        loader.begin_doc(d);
        std::pair<const uint32_t*, const uint32_t*> left_span;
        std::pair<const uint32_t*, const uint32_t*> right_span;
        RETURN_IF_ERROR(
                loader.positions_for_phrase_pos(phrase_plan_index, pair_left, &left_span));
        RETURN_IF_ERROR(
                loader.positions_for_phrase_pos(phrase_plan_index, pair_right, &right_span));

        CollectTwoTermPhraseStarts(left_span, right_span,
                                   position_offsets[pair_right] - position_offsets[pair_left],
                                   position_offsets[pair_left], starts);
        if (starts.empty()) {
            continue;
        }

        span[pair_left] = left_span;
        span[pair_right] = right_span;
        for (size_t pp = 0; pp < phrase_len; ++pp) {
            if (pp == pair_left || pp == pair_right) {
                continue;
            }
            RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, pp, &span[pp]));
        }

        for (uint32_t start : starts) {
            if (PhraseStartMatchesAllTerms(start, phrase_len, pair_left, pair_right,
                                           position_offsets, span)) {
                docids->push_back(d);
                break;
            }
        }
    }
    return doris::Status::OK();
}

// Single streaming pass over the candidates: for each (ascending) candidate,
// gather positions lazily, and test the consecutive-phrase predicate
// (term[0]@p, term[1]@p+1, ...). Multi-term phrases first test the cheapest
// adjacent pair by df before decoding the remaining terms for that document.
// Cursors decode each retained chunk at most once and address positions by
// local index -- no per-candidate docid binary search, no full-candidate
// position materialization. Candidates are ascending so the emitted docids are
// already sorted.
doris::Status EmitPhraseStreaming(const std::vector<TermPlan>& plans,
                           const std::vector<size_t>& phrase_plan_index,
                           const std::vector<uint32_t>& position_offsets,
                           std::vector<PosSource>& srcs, const std::vector<uint32_t>& candidates,
                           std::vector<uint32_t>* docids) {
    const size_t phrase_len = phrase_plan_index.size();
    if (phrase_len == 1) {
        return EmitSingleTermPhraseStreaming(phrase_plan_index, srcs, candidates, docids);
    }
    if (phrase_len == 2) {
        if (phrase_plan_index[0] != phrase_plan_index[1]) {
            return EmitTwoTermPhraseChunkMerge(phrase_plan_index, position_offsets, srcs, docids);
        }
        return EmitTwoTermPhraseStreaming(phrase_plan_index, position_offsets, srcs, candidates,
                                          docids);
    }
    return EmitMultiTermPhraseStreaming(plans, phrase_plan_index, position_offsets, srcs,
                                        candidates, docids);
}

doris::Status BuildPhraseExecutionState(const LogicalIndexReader& idx, snii::io::BatchRangeFetcher* round1,
                                 std::vector<TermPlan>* plans, PhraseExecutionState* state) {
    if (round1->pending() > 0) RETURN_IF_ERROR(round1->fetch());
    RETURN_IF_ERROR(internal::open_preludes(*round1, plans,
                                                 /*need_positions=*/true));

    state->owners.clear();
    state->candidates.clear();
    std::vector<DocidSource> doc_sources;
    RETURN_IF_ERROR(internal::build_docid_only_conjunction(idx, *round1, *plans,
                                                                &state->candidates, &doc_sources));
    if (state->candidates.empty()) return doris::Status::OK();
    RETURN_IF_ERROR(BuildPositionSourcesForCandidates(
            idx, *round1, *plans, &doc_sources, state->candidates, &state->owners, &state->srcs));
    return doris::Status::OK();
}

doris::Status ExecutePhrasePlans(const LogicalIndexReader& idx, snii::io::BatchRangeFetcher* round1,
                          std::vector<TermPlan>* plans,
                          const std::vector<size_t>& phrase_plan_index,
                          std::vector<uint32_t>* docids) {
    PhraseExecutionState state;
    RETURN_IF_ERROR(BuildPhraseExecutionState(idx, round1, plans, &state));
    if (state.candidates.empty()) return doris::Status::OK();

    std::vector<uint32_t> position_offsets;
    if (!internal::build_position_offsets(phrase_plan_index.size(), &position_offsets)) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("phrase_query: phrase length exceeds doc position range");
    }
    return EmitPhraseStreaming(*plans, phrase_plan_index, position_offsets, state.srcs,
                               state.candidates, docids);
}

doris::Status ExecuteResolvedPhraseTerms(const LogicalIndexReader& idx,
                                  const std::vector<ResolvedQueryTerm>& terms,
                                  std::vector<uint32_t>* docids) {
    snii::io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, terms, &round1, &plans,
                                                       /*need_positions=*/false));
    std::vector<size_t> phrase_plan_index(terms.size());
    std::iota(phrase_plan_index.begin(), phrase_plan_index.end(), 0);
    return ExecutePhrasePlans(idx, &round1, &plans, phrase_plan_index, docids);
}

doris::Status CollectExpectedTailPositions(const std::vector<TermPlan>& plans,
                                    const std::vector<uint32_t>& position_offsets,
                                    std::vector<PosSource>& srcs,
                                    const std::vector<uint32_t>& candidates,
                                    ExpectedTailPositionSet* out) {
    const size_t n = plans.size();
    std::vector<PostingCursor> cur(n);
    for (size_t i = 0; i < n; ++i) cur[i].init(&srcs[i]);

    std::vector<PostingCursor*> ordered(n);
    for (size_t i = 0; i < n; ++i) ordered[plans[i].order] = &cur[i];

    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(n);
    for (uint32_t d : candidates) {
        for (size_t i = 0; i < n; ++i) RETURN_IF_ERROR(cur[i].seek(d));
        for (size_t pp = 0; pp < n; ++pp) {
            RETURN_IF_ERROR(ordered[pp]->positions(&span[pp]));
        }

        const size_t expected_begin = out->positions.size();
        for (const uint32_t* p = span[0].first; p != span[0].second; ++p) {
            const uint32_t start = *p;
            bool ok = true;
            for (size_t t = 1; t < n; ++t) {
                uint32_t want = 0;
                if (!internal::add_position_offset(start, position_offsets[t], &want)) {
                    ok = false;
                    break;
                }
                if (!std::binary_search(span[t].first, span[t].second, want)) {
                    ok = false;
                    break;
                }
            }
            uint32_t tail_pos = 0;
            if (ok && internal::add_position_offset(start, position_offsets[n], &tail_pos)) {
                out->positions.push_back(tail_pos);
            }
        }
        const size_t expected_end = out->positions.size();
        if (expected_end != expected_begin) {
            out->docs.push_back({d, expected_begin, expected_end});
        }
    }
    return doris::Status::OK();
}

doris::Status CollectSingleTermExpectedTailPositions(std::vector<PosSource>& srcs,
                                              const std::vector<uint32_t>& candidates,
                                              uint32_t tail_offset, ExpectedTailPositionSet* out) {
    PostingCursor cursor;
    cursor.init(srcs.data());
    out->reserve_docs(out->docs.size() + candidates.size());

    for (uint32_t d : candidates) {
        RETURN_IF_ERROR(cursor.seek(d));
        std::pair<const uint32_t*, const uint32_t*> span;
        RETURN_IF_ERROR(cursor.positions(&span));

        const size_t expected_begin = out->positions.size();
        for (const uint32_t* p = span.first; p != span.second; ++p) {
            uint32_t tail_pos = 0;
            if (internal::add_position_offset(*p, tail_offset, &tail_pos)) {
                out->positions.push_back(tail_pos);
            }
        }
        const size_t expected_end = out->positions.size();
        if (expected_end != expected_begin) {
            out->docs.push_back({d, expected_begin, expected_end});
        }
    }
    return doris::Status::OK();
}

doris::Status CollectExpectedTailPositions(const LogicalIndexReader& idx,
                                    const std::vector<ResolvedQueryTerm>& exact_terms,
                                    ExpectedTailPositionSet* out) {
    out->clear();
    snii::io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, exact_terms, &round1, &plans,
                                                       /*need_positions=*/false));

    PhraseExecutionState state;
    RETURN_IF_ERROR(BuildPhraseExecutionState(idx, &round1, &plans, &state));
    if (state.candidates.empty()) return doris::Status::OK();
    out->reserve_docs(state.candidates.size());
    std::vector<uint32_t> position_offsets;
    if (!internal::build_position_offsets(plans.size() + 1, &position_offsets)) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_prefix_query: phrase length exceeds doc position range");
    }
    if (plans.size() == 1) {
        return CollectSingleTermExpectedTailPositions(state.srcs, state.candidates,
                                                      position_offsets[1], out);
    }
    return CollectExpectedTailPositions(plans, position_offsets, state.srcs, state.candidates, out);
}

bool contains_any_position(const ExpectedTailPositionSet& expected,
                           const ExpectedTailPositions& wanted,
                           std::pair<const uint32_t*, const uint32_t*> actual) {
    for (size_t i = wanted.positions_begin; i < wanted.positions_end; ++i) {
        if (std::binary_search(actual.first, actual.second, expected.positions[i])) {
            return true;
        }
    }
    return false;
}

doris::Status CollectTailMatchesAtExpectedPositions(const LogicalIndexReader& idx,
                                             const ResolvedQueryTerm& tail,
                                             const ExpectedTailPositionSet& expected,
                                             std::vector<uint32_t>* out) {
    if (expected.docs.empty()) {
        return doris::Status::OK();
    }

    snii::io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, {tail}, &round1, &plans,
                                                       /*need_positions=*/false));

    if (round1.pending() > 0) RETURN_IF_ERROR(round1.fetch());
    RETURN_IF_ERROR(internal::open_preludes(round1, &plans,
                                                 /*need_positions=*/true));

    std::vector<uint32_t> expected_docids;
    expected_docids.reserve(expected.docs.size());
    for (const ExpectedTailPositions& doc : expected.docs) {
        expected_docids.push_back(doc.docid);
    }

    std::vector<uint32_t> tail_candidates;
    std::vector<DocidSource> doc_sources;
    RETURN_IF_ERROR(internal::filter_docids_by_conjunction(idx, round1, plans, expected_docids,
                                                                &tail_candidates, &doc_sources));
    if (tail_candidates.empty()) return doris::Status::OK();

    std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>> owners;
    std::vector<PosSource> srcs;
    RETURN_IF_ERROR(BuildPositionSourcesForCandidates(idx, round1, plans, &doc_sources,
                                                           tail_candidates, &owners, &srcs));

    PostingCursor cursor;
    cursor.init(&srcs[0]);
    size_t ei = 0;
    size_t ti = 0;
    while (ei < expected.docs.size() && ti < tail_candidates.size()) {
        const uint32_t want_doc = expected.docs[ei].docid;
        const uint32_t tail_doc = tail_candidates[ti];
        if (want_doc < tail_doc) {
            ++ei;
            continue;
        }
        if (tail_doc < want_doc) {
            ++ti;
            continue;
        }

        RETURN_IF_ERROR(cursor.seek(want_doc));
        std::pair<const uint32_t*, const uint32_t*> actual;
        RETURN_IF_ERROR(cursor.positions(&actual));
        if (contains_any_position(expected, expected.docs[ei], actual)) {
            out->push_back(want_doc);
        }
        ++ei;
        ++ti;
    }
    return doris::Status::OK();
}

} // namespace

doris::Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* const docids) {
    if (docids == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("phrase_query: null out");
    }
    docids->clear();
    if (terms.empty()) {
        return doris::Status::OK();
    }
    if (terms.size() == 1) {
        return term_query(idx, terms.front(), docids);
    }
    if (!idx.has_positions()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>("phrase_query: index has no positions");
    }
    bool handled_by_bigram = false;
    RETURN_IF_ERROR(TryTwoTermPhraseBigram(idx, terms, docids, &handled_by_bigram));
    if (handled_by_bigram) {
        return doris::Status::OK();
    }

    // Round 1: preludes (windowed) + docid postings (slim/inline) batched
    // together. Positions are fetched after the docid-only conjunction has
    // produced final candidates, so phrase verification does not read PRX for
    // windows later removed by the docid intersection.
    snii::io::BatchRangeFetcher round1(idx.reader());
    const PhraseTermMapping mapping = BuildPhraseTermMapping(terms);
    std::vector<TermPlan> plans;
    bool all_present = false;
    RETURN_IF_ERROR(internal::plan_terms(idx, mapping.unique_terms, &round1, &plans,
                                              &all_present,
                                              /*need_positions=*/false));
    if (!all_present) return doris::Status::OK();
    return ExecutePhrasePlans(idx, &round1, &plans, mapping.phrase_plan_index, docids);
}

doris::Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* const docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return phrase_query(idx, terms, docids);
}

doris::Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, int32_t max_expansions) {
    if (docids == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("phrase_prefix_query: null out");
    }
    docids->clear();
    if (terms.empty()) {
        return doris::Status::OK();
    }
    if (terms.size() == 1) {
        return prefix_query(idx, terms.front(), docids, max_expansions);
    }
    if (!idx.has_positions()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>("phrase_prefix_query: index has no positions");
    }
    std::vector<ResolvedQueryTerm> exact_terms;
    exact_terms.reserve(terms.size() - 1);
    for (size_t i = 0; i + 1 < terms.size(); ++i) {
        ResolvedQueryTerm resolved;
        bool found = false;
        RETURN_IF_ERROR(internal::resolve_query_term(idx, terms[i], &resolved, &found));
        if (!found) {
            return doris::Status::OK();
        }
        exact_terms.push_back(std::move(resolved));
    }

    std::vector<LogicalIndexReader::PrefixHit> tail_hits;
    RETURN_IF_ERROR(idx.prefix_terms(terms.back(), &tail_hits, max_expansions));
    std::erase_if(tail_hits, [](const LogicalIndexReader::PrefixHit& hit) {
        return snii::format::is_phrase_bigram_term(hit.term);
    });
    if (tail_hits.empty()) {
        return doris::Status::OK();
    }
    if (tail_hits.size() == 1) {
        std::vector<ResolvedQueryTerm> resolved_terms = exact_terms;
        resolved_terms.push_back(ResolvedQueryTerm {std::move(tail_hits.front().entry),
                                                    tail_hits.front().frq_base,
                                                    tail_hits.front().prx_base});
        return ExecuteResolvedPhraseTerms(idx, resolved_terms, docids);
    }

    ExpectedTailPositionSet expected;
    RETURN_IF_ERROR(CollectExpectedTailPositions(idx, exact_terms, &expected));
    if (expected.docs.empty()) {
        return doris::Status::OK();
    }

    std::vector<uint32_t> acc;
    for (LogicalIndexReader::PrefixHit& hit : tail_hits) {
        ResolvedQueryTerm tail {std::move(hit.entry), hit.frq_base, hit.prx_base};
        std::vector<uint32_t> tail_docs;
        RETURN_IF_ERROR(
                CollectTailMatchesAtExpectedPositions(idx, tail, expected, &tail_docs));
        internal::union_sorted_into(&acc, tail_docs);
    }
    *docids = std::move(acc);
    return doris::Status::OK();
}

doris::Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, QueryProfile* profile,
                           int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return phrase_prefix_query(idx, terms, docids, max_expansions);
}

} // namespace snii::query
