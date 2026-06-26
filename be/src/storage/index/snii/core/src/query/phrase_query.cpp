#include "snii/query/phrase_query.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "snii/common/slice.h"
#include "snii/encoding/byte_source.h"
#include "snii/format/dict_entry.h"
#include "snii/format/frq_pod.h"
#include "snii/format/frq_prelude.h"
#include "snii/format/prx_pod.h"
#include "snii/io/batch_range_fetcher.h"
#include "snii/query/internal/docid_conjunction.h"
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

using snii::query::internal::DocidChunk;
using snii::query::internal::DocidSource;
using snii::query::internal::ResolvedQueryTerm;
using snii::query::internal::TermPlan;
using snii::reader::LogicalIndexReader;

namespace {

struct ExpectedTailPositions {
    uint32_t docid = 0;
    std::vector<uint32_t> positions;
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

Status append_prx_doc_ordinal(size_t ordinal, std::vector<uint32_t>* out) {
    if (ordinal > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("phrase_query: prx doc ordinal exceeds u32");
    }
    out->push_back(static_cast<uint32_t>(ordinal));
    return Status::OK();
}

Status SelectCandidateDocsForPrx(std::vector<uint32_t>* docids,
                                 std::vector<uint32_t>* prx_doc_ordinals,
                                 const std::vector<uint32_t>& candidates, PosChunk* chunk) {
    chunk->docids.clear();
    chunk->prx_doc_ordinals.clear();
    if (docids->empty() || candidates.empty()) return Status::OK();
    if (!prx_doc_ordinals->empty() && prx_doc_ordinals->size() != docids->size()) {
        return Status::Corruption("phrase_query: prx ordinal/docid count mismatch");
    }

    std::vector<uint32_t> selected_docids;
    std::vector<uint32_t> selected_ordinals;
    selected_docids.reserve(std::min(docids->size(), candidates.size()));
    selected_ordinals.reserve(selected_docids.capacity());

    size_t candidate_index = 0;
    for (size_t doc_index = 0; doc_index < docids->size() && candidate_index < candidates.size();
         ++doc_index) {
        const uint32_t docid = (*docids)[doc_index];
        while (candidate_index < candidates.size() && candidates[candidate_index] < docid) {
            ++candidate_index;
        }
        if (candidate_index == candidates.size()) break;
        if (candidates[candidate_index] != docid) continue;

        selected_docids.push_back(docid);
        if (prx_doc_ordinals->empty()) {
            SNII_RETURN_IF_ERROR(append_prx_doc_ordinal(doc_index, &selected_ordinals));
        } else {
            selected_ordinals.push_back((*prx_doc_ordinals)[doc_index]);
        }
        ++candidate_index;
    }

    if (selected_docids.empty()) return Status::OK();
    if (selected_docids.size() == docids->size()) {
        chunk->docids = std::move(*docids);
        chunk->prx_doc_ordinals = std::move(*prx_doc_ordinals);
        return Status::OK();
    }
    chunk->docids = std::move(selected_docids);
    chunk->prx_doc_ordinals = std::move(selected_ordinals);
    return Status::OK();
}

Status BuildFlatPositionSource(const LogicalIndexReader& idx,
                               const snii::io::BatchRangeFetcher& round1, DocidSource* doc_source,
                               const TermPlan& p, const std::vector<uint32_t>& candidates,
                               std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>>* owners,
                               PosSource* src) {
    PosChunk chunk;
    std::vector<uint32_t> docids;
    std::vector<uint32_t> prx_doc_ordinals;
    if (!doc_source->chunks.empty()) {
        docids = std::move(doc_source->chunks.front().docids);
        prx_doc_ordinals = std::move(doc_source->chunks.front().prx_doc_ordinals);
    }
    if (p.pod_ref) {
        uint64_t poff = 0;
        uint64_t plen = 0;
        SNII_RETURN_IF_ERROR(idx.resolve_prx_window(p.entry, p.prx_base, &poff, &plen));
        auto fetcher = std::make_unique<snii::io::BatchRangeFetcher>(idx.reader());
        const size_t prx_handle = fetcher->add(poff, plen);
        SNII_RETURN_IF_ERROR(fetcher->fetch());
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
            SNII_RETURN_IF_ERROR(internal::inline_dd_region(p.entry, &dd));
        }
        SNII_RETURN_IF_ERROR(snii::format::decode_dd_region(dd, p.entry.dd_meta,
                                                            /*win_base=*/0, &docids));
    }
    SNII_RETURN_IF_ERROR(SelectCandidateDocsForPrx(&docids, &prx_doc_ordinals, candidates, &chunk));
    if (!chunk.docids.empty()) src->chunks.push_back(std::move(chunk));
    return Status::OK();
}

bool ChunkMayContainCandidate(const DocidChunk& chunk, const std::vector<uint32_t>& candidates) {
    if (chunk.docids.empty() || candidates.empty()) return false;
    const auto it = std::lower_bound(candidates.begin(), candidates.end(), chunk.docids.front());
    return it != candidates.end() && *it <= chunk.docids.back();
}

Status DecodeWindowedPositionSource(
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
        if (!ChunkMayContainCandidate(doc_chunk, candidates)) continue;
        if (!doc_chunk.windowed) {
            return Status::Corruption("phrase_query: expected windowed doc chunk");
        }
        PosChunk chunk;
        SNII_RETURN_IF_ERROR(SelectCandidateDocsForPrx(
                &doc_chunk.docids, &doc_chunk.prx_doc_ordinals, candidates, &chunk));
        if (chunk.docids.empty()) continue;

        snii::reader::WindowAbsRange range;
        SNII_RETURN_IF_ERROR(snii::reader::windowed_window_range(
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
    if (prx_fetcher->pending() > 0) SNII_RETURN_IF_ERROR(prx_fetcher->fetch());

    for (const WindowFetch& f : fetched) {
        src->chunks[f.chunk_index].prx = prx_fetcher->get(f.prx_handle);
    }
    if (!fetched.empty()) owners->push_back(std::move(prx_fetcher));
    return Status::OK();
}

Status BuildPositionSourcesForCandidates(
        const LogicalIndexReader& idx, const snii::io::BatchRangeFetcher& round1,
        const std::vector<TermPlan>& plans, std::vector<DocidSource>* doc_sources,
        const std::vector<uint32_t>& candidates,
        std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>>* owners,
        std::vector<PosSource>* srcs) {
    srcs->assign(plans.size(), PosSource {});
    for (size_t i = 0; i < plans.size(); ++i) {
        const TermPlan& p = plans[i];
        if (p.windowed) {
            SNII_RETURN_IF_ERROR(DecodeWindowedPositionSource(idx, p, &(*doc_sources)[i],
                                                              candidates, owners, &(*srcs)[i]));
            continue;
        }
        SNII_RETURN_IF_ERROR(BuildFlatPositionSource(idx, round1, &(*doc_sources)[i], p, candidates,
                                                     owners, &(*srcs)[i]));
    }
    return Status::OK();
}

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
    }

    // Positions the cursor at `target` (guaranteed present: candidates are the
    // intersection of exactly these chunks' docids). Monotonic forward advance.
    Status seek(uint32_t target) {
        while (ci_ < src_->chunks.size() &&
               (src_->chunks[ci_].docids.empty() || src_->chunks[ci_].docids.back() < target)) {
            ++ci_;
            li_ = 0;
        }
        if (ci_ >= src_->chunks.size()) {
            return Status::Corruption("phrase_query: cursor exhausted before target docid");
        }
        const std::vector<uint32_t>& d = src_->chunks[ci_].docids;
        while (li_ < d.size() && d[li_] < target) ++li_;
        if (li_ >= d.size() || d[li_] != target) {
            return Status::Corruption("phrase_query: candidate missing from posting chunk");
        }
        return Status::OK();
    }

    // [begin,end) of the current doc's positions, decoding the current chunk's
    // .prx exactly once (cached). Must follow a seek that landed on a real doc.
    Status positions(std::pair<const uint32_t*, const uint32_t*>* out) {
        if (ci_ >= src_->chunks.size() || li_ >= src_->chunks[ci_].docids.size()) {
            return Status::Corruption("phrase_query: cursor positions out of range");
        }
        if (decoded_pos_chunk_ != ci_) {
            ByteSource ps(src_->chunks[ci_].prx);
            if (src_->chunks[ci_].prx_doc_ordinals.empty()) {
                SNII_RETURN_IF_ERROR(snii::format::read_prx_window_csr(&ps, &pflat_, &poff_));
            } else {
                SNII_RETURN_IF_ERROR(snii::format::read_prx_window_csr_selective(
                        &ps, src_->chunks[ci_].prx_doc_ordinals, &pflat_, &poff_));
            }
            if (poff_.size() != src_->chunks[ci_].docids.size() + 1) {
                return Status::Corruption("phrase_query: prx/dd doc-count mismatch");
            }
            decoded_pos_chunk_ = ci_;
        }
        const uint32_t begin = poff_[li_];
        const uint32_t end = poff_[li_ + 1];
        if (begin == end) {
            *out = {nullptr, nullptr};
            return Status::OK();
        }
        if (end > pflat_.size()) {
            return Status::Corruption("phrase_query: prx offset out of range");
        }
        *out = {pflat_.data() + begin, pflat_.data() + end};
        return Status::OK();
    }

private:
    static constexpr size_t kNoChunk = static_cast<size_t>(-1);
    const PosSource* src_ = nullptr;
    size_t ci_ = 0;                       // current chunk
    size_t li_ = 0;                       // current local doc index within the chunk
    size_t decoded_pos_chunk_ = kNoChunk; // which chunk pflat_/poff_ currently hold
    std::vector<uint32_t> pflat_;         // current chunk's flat positions (reused)
    std::vector<uint32_t> poff_;          // current chunk's per-doc offsets (reused)
};

size_t AnchorPhrasePosition(const std::vector<TermPlan>& plans,
                            const std::vector<size_t>& phrase_plan_index) {
    size_t anchor = 0;
    uint32_t best_df = std::numeric_limits<uint32_t>::max();
    for (size_t phrase_pos = 0; phrase_pos < phrase_plan_index.size(); ++phrase_pos) {
        const TermPlan& plan = plans[phrase_plan_index[phrase_pos]];
        if (plan.df < best_df) {
            best_df = plan.df;
            anchor = phrase_pos;
        }
    }
    return anchor;
}

// Single streaming pass over the candidates: for each (ascending) candidate,
// advance every term's cursor to it, gather each term's positions IN PHRASE
// ORDER, and test the consecutive-phrase predicate (term[0]@p, term[1]@p+1,
// ...) with term-level short-circuit. Cursors decode each chunk's
// docids/positions exactly once and address positions by local index -- no
// per-candidate docid binary search, no full-candidate position
// materialization. Candidates are ascending so the emitted docids are already
// sorted.
Status EmitPhraseStreaming(const std::vector<TermPlan>& plans,
                           const std::vector<size_t>& phrase_plan_index,
                           const std::vector<uint32_t>& position_offsets,
                           std::vector<PosSource>& srcs, const std::vector<uint32_t>& candidates,
                           std::vector<uint32_t>* docids) {
    std::vector<PostingCursor> cur(plans.size());
    for (size_t i = 0; i < plans.size(); ++i) cur[i].init(&srcs[i]);

    const size_t phrase_len = phrase_plan_index.size();
    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(phrase_len);
    const size_t anchor = AnchorPhrasePosition(plans, phrase_plan_index);
    const uint32_t anchor_offset = position_offsets[anchor];
    for (uint32_t d : candidates) {
        for (size_t i = 0; i < cur.size(); ++i) SNII_RETURN_IF_ERROR(cur[i].seek(d));
        for (size_t pp = 0; pp < phrase_len; ++pp) {
            SNII_RETURN_IF_ERROR(cur[phrase_plan_index[pp]].positions(&span[pp]));
        }
        bool match = false;
        for (const uint32_t* p = span[anchor].first; p != span[anchor].second; ++p) {
            if (*p < anchor_offset) continue;
            const uint32_t start = *p - anchor_offset;
            bool ok = true;
            for (size_t t = 0; t < phrase_len; ++t) {
                if (t == anchor) continue;
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
            if (ok) {
                match = true;
                break;
            }
        }
        if (match) docids->push_back(d);
    }
    return Status::OK();
}

Status BuildPhraseExecutionState(const LogicalIndexReader& idx, snii::io::BatchRangeFetcher* round1,
                                 std::vector<TermPlan>* plans, PhraseExecutionState* state) {
    if (round1->pending() > 0) SNII_RETURN_IF_ERROR(round1->fetch());
    SNII_RETURN_IF_ERROR(internal::open_preludes(*round1, plans,
                                                 /*need_positions=*/true));

    state->owners.clear();
    state->candidates.clear();
    std::vector<DocidSource> doc_sources;
    SNII_RETURN_IF_ERROR(internal::build_docid_only_conjunction(idx, *round1, *plans,
                                                                &state->candidates, &doc_sources));
    if (state->candidates.empty()) return Status::OK();
    SNII_RETURN_IF_ERROR(BuildPositionSourcesForCandidates(
            idx, *round1, *plans, &doc_sources, state->candidates, &state->owners, &state->srcs));
    return Status::OK();
}

Status ExecutePhrasePlans(const LogicalIndexReader& idx, snii::io::BatchRangeFetcher* round1,
                          std::vector<TermPlan>* plans,
                          const std::vector<size_t>& phrase_plan_index,
                          std::vector<uint32_t>* docids) {
    PhraseExecutionState state;
    SNII_RETURN_IF_ERROR(BuildPhraseExecutionState(idx, round1, plans, &state));
    if (state.candidates.empty()) return Status::OK();

    std::vector<uint32_t> position_offsets;
    if (!internal::build_position_offsets(phrase_plan_index.size(), &position_offsets)) {
        return Status::InvalidArgument("phrase_query: phrase length exceeds doc position range");
    }
    return EmitPhraseStreaming(*plans, phrase_plan_index, position_offsets, state.srcs,
                               state.candidates, docids);
}

Status CollectExpectedTailPositions(const std::vector<TermPlan>& plans,
                                    const std::vector<uint32_t>& position_offsets,
                                    std::vector<PosSource>& srcs,
                                    const std::vector<uint32_t>& candidates,
                                    std::vector<ExpectedTailPositions>* out) {
    const size_t n = plans.size();
    std::vector<PostingCursor> cur(n);
    for (size_t i = 0; i < n; ++i) cur[i].init(&srcs[i]);

    std::vector<PostingCursor*> ordered(n);
    for (size_t i = 0; i < n; ++i) ordered[plans[i].order] = &cur[i];

    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(n);
    for (uint32_t d : candidates) {
        for (size_t i = 0; i < n; ++i) SNII_RETURN_IF_ERROR(cur[i].seek(d));
        for (size_t pp = 0; pp < n; ++pp) {
            SNII_RETURN_IF_ERROR(ordered[pp]->positions(&span[pp]));
        }

        ExpectedTailPositions match;
        match.docid = d;
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
                match.positions.push_back(tail_pos);
            }
        }
        if (!match.positions.empty()) out->push_back(std::move(match));
    }
    return Status::OK();
}

Status CollectExpectedTailPositions(const LogicalIndexReader& idx,
                                    const std::vector<ResolvedQueryTerm>& exact_terms,
                                    std::vector<ExpectedTailPositions>* out) {
    out->clear();
    snii::io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    SNII_RETURN_IF_ERROR(internal::plan_resolved_terms(idx, exact_terms, &round1, &plans,
                                                       /*need_positions=*/false));

    PhraseExecutionState state;
    SNII_RETURN_IF_ERROR(BuildPhraseExecutionState(idx, &round1, &plans, &state));
    if (state.candidates.empty()) return Status::OK();
    std::vector<uint32_t> position_offsets;
    if (!internal::build_position_offsets(plans.size() + 1, &position_offsets)) {
        return Status::InvalidArgument(
                "phrase_prefix_query: phrase length exceeds doc position range");
    }
    return CollectExpectedTailPositions(plans, position_offsets, state.srcs, state.candidates, out);
}

bool contains_any_position(const std::vector<uint32_t>& wanted,
                           std::pair<const uint32_t*, const uint32_t*> actual) {
    for (uint32_t pos : wanted) {
        if (std::binary_search(actual.first, actual.second, pos)) return true;
    }
    return false;
}

Status CollectTailMatchesAtExpectedPositions(const LogicalIndexReader& idx,
                                             const ResolvedQueryTerm& tail,
                                             const std::vector<ExpectedTailPositions>& expected,
                                             std::vector<uint32_t>* out) {
    if (expected.empty()) return Status::OK();

    snii::io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    SNII_RETURN_IF_ERROR(internal::plan_resolved_terms(idx, {tail}, &round1, &plans,
                                                       /*need_positions=*/false));

    PhraseExecutionState state;
    SNII_RETURN_IF_ERROR(BuildPhraseExecutionState(idx, &round1, &plans, &state));
    if (state.candidates.empty()) return Status::OK();

    PostingCursor cursor;
    cursor.init(&state.srcs[0]);
    size_t ei = 0;
    size_t ti = 0;
    while (ei < expected.size() && ti < state.candidates.size()) {
        const uint32_t want_doc = expected[ei].docid;
        const uint32_t tail_doc = state.candidates[ti];
        if (want_doc < tail_doc) {
            ++ei;
            continue;
        }
        if (tail_doc < want_doc) {
            ++ti;
            continue;
        }

        SNII_RETURN_IF_ERROR(cursor.seek(want_doc));
        std::pair<const uint32_t*, const uint32_t*> actual;
        SNII_RETURN_IF_ERROR(cursor.positions(&actual));
        if (contains_any_position(expected[ei].positions, actual)) out->push_back(want_doc);
        ++ei;
        ++ti;
    }
    return Status::OK();
}

} // namespace

Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("phrase_query: null out");
    docids->clear();
    if (terms.empty()) return Status::OK();
    if (terms.size() == 1) return term_query(idx, terms.front(), docids);
    if (!idx.has_positions()) {
        return Status::Unsupported("phrase_query: index has no positions");
    }

    // Round 1: preludes (windowed) + docid postings (slim/inline) batched
    // together. Positions are fetched after the docid-only conjunction has
    // produced final candidates, so phrase verification does not read PRX for
    // windows later removed by the docid intersection.
    snii::io::BatchRangeFetcher round1(idx.reader());
    const PhraseTermMapping mapping = BuildPhraseTermMapping(terms);
    std::vector<TermPlan> plans;
    bool all_present = false;
    SNII_RETURN_IF_ERROR(internal::plan_terms(idx, mapping.unique_terms, &round1, &plans,
                                              &all_present,
                                              /*need_positions=*/false));
    if (!all_present) return Status::OK();
    return ExecutePhrasePlans(idx, &round1, &plans, mapping.phrase_plan_index, docids);
}

Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return phrase_query(idx, terms, docids);
}

Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("phrase_prefix_query: null out");
    docids->clear();
    if (terms.empty()) return Status::OK();
    if (terms.size() == 1) return prefix_query(idx, terms.front(), docids);
    if (!idx.has_positions()) {
        return Status::Unsupported("phrase_prefix_query: index has no positions");
    }

    std::vector<ResolvedQueryTerm> exact_terms;
    exact_terms.reserve(terms.size() - 1);
    for (size_t i = 0; i + 1 < terms.size(); ++i) {
        ResolvedQueryTerm resolved;
        bool found = false;
        SNII_RETURN_IF_ERROR(internal::resolve_query_term(idx, terms[i], &resolved, &found));
        if (!found) return Status::OK();
        exact_terms.push_back(std::move(resolved));
    }

    std::vector<LogicalIndexReader::PrefixHit> tail_hits;
    SNII_RETURN_IF_ERROR(idx.prefix_terms(terms.back(), &tail_hits));
    if (tail_hits.empty()) return Status::OK();

    std::vector<ExpectedTailPositions> expected;
    SNII_RETURN_IF_ERROR(CollectExpectedTailPositions(idx, exact_terms, &expected));
    if (expected.empty()) return Status::OK();

    std::vector<uint32_t> acc;
    for (LogicalIndexReader::PrefixHit& hit : tail_hits) {
        ResolvedQueryTerm tail {std::move(hit.entry), hit.frq_base, hit.prx_base};
        std::vector<uint32_t> tail_docs;
        SNII_RETURN_IF_ERROR(
                CollectTailMatchesAtExpectedPositions(idx, tail, expected, &tail_docs));
        internal::union_sorted_into(&acc, tail_docs);
    }
    *docids = std::move(acc);
    return Status::OK();
}

Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return phrase_prefix_query(idx, terms, docids);
}

} // namespace snii::query
