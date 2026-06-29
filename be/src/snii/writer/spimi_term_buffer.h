#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "snii/writer/compact_posting_pool.h"
#include "snii/writer/memory_reporter.h"

namespace snii::writer {

// One term's posting list: docids ascending, with parallel freqs and (when
// positions are enabled) a single FLAT positions buffer.
//
// positions_flat holds every position for the term in document order, partitioned
// by freqs: doc i owns the next freqs[i] entries. This is the SAME layout the
// accumulator stores natively, so no per-doc vector-of-vectors is ever built on
// the build/merge hot path (that vector-of-vectors was the dominant peak-RSS
// driver for high-df terms). doc_positions(i) returns a non-owning span view of
// doc i's positions for consumers that want per-doc access (e.g. the prx window
// builder, tests). positions_flat is empty when positions are disabled.
struct TermPostings {
    std::string term;
    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    std::vector<uint32_t> positions_flat; // empty when positions disabled

    // OPTIONAL streamed-positions source (peak-RSS optimization for very-high-df
    // terms). When set, positions_flat is left EMPTY and the writer pulls positions
    // SEQUENTIALLY in document order via pos_pump(dst, n) -- filling `dst[0..n)` with
    // the next n positions -- one window at a time, so the term's full flat positions
    // buffer (tens of MiB for the widest term) is never materialized. The yielded
    // bytes are byte-identical to building from positions_flat (same values, same
    // order). pos_total is the total number of positions the pump will yield (==
    // sum(freqs)); it lets the writer validate without a flat buffer. When pos_pump
    // is null, positions come from positions_flat as before. Only the writer's prx
    // builders consume this; all other consumers use positions_flat.
    //
    // OWNERSHIP CONTRACT (synchronous-consume-once): a streamed pos_pump captures
    // references into the producer's stack and its parked run readers/arena, valid ONLY
    // for the duration of the synchronous fn(TermPostings&&) call that delivered this
    // TermPostings. The consumer MUST pull all positions inside fn() and MUST NOT store
    // the TermPostings or invoke pos_pump after fn() returns. Callers that retain the
    // TermPostings pass allow_stream_positions=false, which materializes positions into
    // positions_flat instead (no pump). As a safety net, a deferred call to a streamed
    // pump throws std::logic_error rather than dereferencing freed state.
    std::function<void(uint32_t*, size_t)> pos_pump;
    uint64_t pos_total = 0;

    // Byte offset of doc i's first position within positions_flat (prefix sum of
    // freqs). O(i) -- callers iterating all docs should track a running offset.
    size_t pos_offset(size_t doc_index) const {
        size_t off = 0;
        for (size_t i = 0; i < doc_index; ++i) off += freqs[i];
        return off;
    }
    // Non-owning view of doc i's positions (length freqs[i]) into positions_flat.
    std::span<const uint32_t> doc_positions(size_t doc_index) const {
        const size_t off = pos_offset(doc_index);
        return std::span<const uint32_t>(positions_flat.data() + off, freqs[doc_index]);
    }

    // Rebuilds the per-doc position lists (for callers/tests wanting per-doc access)
    // from positions_flat partitioned by freqs. O(total positions); allocates.
    std::vector<std::vector<uint32_t>> positions_per_doc() const {
        std::vector<std::vector<uint32_t>> out(freqs.size());
        size_t off = 0;
        for (size_t i = 0; i < freqs.size(); ++i) {
            out[i].assign(positions_flat.begin() + off, positions_flat.begin() + off + freqs[i]);
            off += freqs[i];
        }
        return out;
    }

    // Sets the flat positions from per-doc lists (convenience for tests / callers
    // that produce per-doc positions). Does NOT touch freqs; the caller is expected
    // to keep freqs[i] == per_doc[i].size() consistent (the writer validates this).
    void set_positions_per_doc(const std::vector<std::vector<uint32_t>>& per_doc) {
        positions_flat.clear();
        for (const auto& d : per_doc)
            positions_flat.insert(positions_flat.end(), d.begin(), d.end());
    }
};

// In-memory SPIMI (Single-Pass In-Memory Indexing) accumulator for one logical
// index. Records term occurrences and produces lexicographically sorted terms
// with ascending-docid posting lists.
//
// TERM-ID ACCUMULATION (no per-token string work): tokens are accumulated by an
// INTEGER term-id, not by hashing/constructing a std::string per token. The
// caller supplies a VOCABULARY mapping term-id -> term string; the buffer keeps
// a DENSE std::vector<Term> indexed by term-id, so the hot add_token path is a
// vector index + a couple of pushes -- no hashing, no allocation per token. The
// vocabulary is resolved to strings only once per distinct term at finalize.
//
// Two construction modes:
//   * BORROWED vocab (the fast path): pass a non-null `vocab` that the caller
//     owns and keeps alive; add_token(term_id, ...) indexes straight into it.
//   * OWNED vocab (compatibility): pass a null `vocab`; the string-keyed
//     add_token(string_view, ...) interns each new term into an internal owned
//     vocabulary (assigning ids in first-seen order) and forwards to the id
//     path. Existing callers that feed strings keep working unchanged.
//
// SPILL / K-WAY MERGE (out-of-core, bounds input RAM): when a non-zero
// spill_threshold_bytes is set, the REAL resident accumulator size (the posting
// arena + the vocab-sized slot index, pool_.arena_bytes() + slot_of_.capacity()*4)
// is compared against the threshold as tokens arrive; once it crosses the
// threshold the buffer SORTS its current terms,
// writes a self-describing sorted RUN to a temp file, and CLEARS memory. Each
// run record is keyed by the TERM-ID (varint); the k-way merge orders runs by
// the id's VOCAB STRING so the merged stream stays lexicographic. Because
// tokens arrive in globally ascending docid order, a term that reappears in a
// later run only covers strictly-later docids, so concatenating its postings in
// run order during the final merge keeps docids ascending. for_each_term_sorted
// flushes the residual buffer as a final run, then k-way merges all runs
// materializing only ONE merged term at a time -> peak memory stays bounded by
// the threshold (plus the widest single term), NOT by total postings. With the
// default threshold 0 (unlimited) the path is exactly the in-memory behavior.
//
// Internal representation is a COMPACT TAGGED VARINT byte stream per term, held in
// a shared SEGMENTED ARENA (CompactPostingPool), NOT per-term uint32 vectors. Each
// term owns ONE arena chain holding a stream of per-TOKEN entries in arrival
// order: every token contributes varint((pos << 1) | new_doc_bit); when new_doc_bit
// is set, the token's doc differs from the previous one, so a zigzag-varint(docid -
// prev_docid) immediately follows. Frequencies are NOT stored -- a doc's freq is
// the count of consecutive same-doc tokens, recovered while decoding. This drops
// the entire freq stream and the second (positions) chain versus a freq/prox split,
// so the payload is ~3.4x smaller than raw uint32 docids/freqs/positions, and the
// shared arena removes per-vector doubling slack and per-term vector headers. Each
// append writes straight into the chain (no deferred per-doc flush): the only live
// per-term state is the current doc id (to detect a doc change) and the delta base.
// to_postings() decodes a term's chain back to the SAME flat TermPostings the
// writer consumes, so the produced .idx is BYTE-IDENTICAL. positions_flat stays
// empty (and pos is tagged as 0) when positions are disabled; freq still counts.
//
// Duplicate vocab strings: the vocab is assumed to map each id to a DISTINCT
// string (a dense vocabulary). If two ids share a string they sort adjacently
// but are emitted as two separate terms; callers must not rely on coalescing.
class SpimiTermBuffer {
public:
    // BORROWED-vocab constructor: `vocab` maps term-id -> term string and is
    // borrowed (NOT owned) -- the caller must keep it alive for the buffer's
    // lifetime. add_token(term_id, ...) accumulates by id with no string work.
    // spill_threshold_bytes is the gate-2 internal buffer cap (e.g. 512 MiB),
    // sourced from config; == 0 means unlimited (pure in-memory, default). A
    // positive value caps the REAL resident accumulator size (pool_.arena_bytes() +
    // slot_of_.capacity()*4), triggering a spill when that crosses the cap -- NOT the
    // old per-token estimate.
    // `reporter` is the OPTIONAL writer-level build-RAM reporter (null off-Doris /
    // unit tests). When non-null, the accumulator reports its REAL resident-byte
    // deltas -- pool_.arena_bytes() + slot_of_.capacity()*4 -- positive on grow,
    // negative on every reset/free, exactly once. NEVER reports live_bytes_ (a gated
    // estimate that feeds only the spill threshold).
    explicit SpimiTermBuffer(const std::vector<std::string>* vocab, bool has_positions,
                             size_t spill_threshold_bytes = 0, MemoryReporter* reporter = nullptr);

    // OWNED-vocab (compatibility) constructor: no external vocab. The string-keyed
    // add_token interns terms into an internal vocabulary on first occurrence.
    explicit SpimiTermBuffer(bool has_positions, size_t spill_threshold_bytes = 0,
                             MemoryReporter* reporter = nullptr);

    ~SpimiTermBuffer();

    SpimiTermBuffer(const SpimiTermBuffer&) = delete;
    SpimiTermBuffer& operator=(const SpimiTermBuffer&) = delete;

    // Records one token by TERM-ID: term `term_id` occurs in `docid` at `pos`.
    // `term_id` must be in [0, vocab_size). An out-of-range id latches an
    // InvalidArgument into status() and is ignored. For a given term, docids are
    // expected to arrive in non-decreasing order, and positions within a docid in
    // ascending order; out-of-order docids (INCLUDING a REVISITED docid -- the same
    // docid appearing again after a different one) are tolerated and reordered at
    // finalize: SortByDocid stably sorts by docid and COALESCES same-docid groups
    // (summing freqs, concatenating positions in document order), so the emitted
    // postings have exactly ONE strictly-ascending entry per docid -- matching the
    // k-way merge path and the writer's strictly-ascending precondition.
    void add_token(uint32_t term_id, uint32_t docid, uint32_t pos);

    // Compatibility overload: records one token by TERM STRING. Valid ONLY on an
    // OWNED-vocab buffer (constructed without an external vocab); interns `term`
    // into the internal vocabulary on first occurrence, then forwards by id. Called
    // on a BORROWED-vocab buffer it is REJECTED (latches InvalidArgument, token
    // ignored) -- interning would grow the owned vocab out of step with the borrowed
    // one and corrupt the build. It also allocates a std::string per call, so the
    // hot path is the id overload; prefer that and reserve this for tests / legacy
    // string-fed callers.
    void add_token(std::string_view term, uint32_t docid, uint32_t pos);

    // Number of DISTINCT terms accumulated so far (touched ids still resident).
    size_t unique_terms() const;
    uint64_t total_tokens() const { return total_tokens_; }
    bool has_positions() const { return has_positions_; }

    // OK unless an add_token validation error (out-of-range term-id, wrong vocab
    // mode) was latched. for_each_term_sorted now returns its own I/O doris::Status
    // directly; callers that use add_token's latch-and-report pattern MUST check
    // this after draining to surface input-side validation errors.
    [[nodiscard]] doris::Status status() const { return spill_status_; }

    // TEST-ONLY: number of spill run files written so far (== 0 in pure in-memory
    // mode). Lets tests assert that a gate-2 spill actually fired once the REAL
    // resident size crossed the configured cap. Not part of the production API.
    size_t run_count_for_test() const { return run_paths_.size(); }

    // Materializes all terms sorted lexicographically; each term's docids are
    // ascending. Convenience wrapper around for_each_term_sorted that keeps the
    // whole result alive at once. Prefer for_each_term_sorted for low peak memory.
    // MUST be called at most once: it drains internal state. A SECOND drain (a
    // repeat call, or a finalize_sorted after a for_each_term_sorted, or vice versa)
    // returns EMPTY and latches an error into status() rather than re-emitting.
    std::vector<TermPostings> finalize_sorted();

    // Streams terms to `fn` in lexicographic order, building ONE transient
    // TermPostings at a time and freeing that term's accumulated arrays before
    // moving to the next. This keeps at most a single term's postings duplicated,
    // avoiding the input+output coexistence peak. MUST be called at most once: it
    // drains internal state. A SECOND drain invokes `fn` zero times and returns
    // an Internal error (a re-merge of the still-present run files would otherwise
    // re-emit every term). Returns non-OK on spill/merge I/O or corruption errors,
    // or if a prior add_token latched a validation error into status().
    doris::Status for_each_term_sorted(const std::function<void(TermPostings&&)>& fn);

private:
    // Compact per-term accumulator: ONE tagged-varint arena chain plus a few cursors.
    // Every token is appended immediately (no deferred flush), so the only running
    // state is the current doc id and the delta base. A sentinel chain head of
    // kNoChain marks a term that has not started its chain yet (so an all-empty term
    // costs no arena bytes). ntok / ndocs bound the decode loop and size reserves.
    // Total ~36 B per live term.
    static constexpr uint32_t kNoChain = 0xFFFFFFFFu;
    struct Term {
        uint32_t head = kNoChain;          // chain read entry point
        CompactPostingPool::SliceWriter w; // append cursor for the chain (8 B)
        uint32_t ntok = 0;                 // total tokens (entries) in the chain
        uint32_t cur_docid = 0;            // most-recent doc id: detects doc change AND
                                           // is the zigzag delta base for the next doc
        uint8_t level = 0;                 // current slice level of w (packed here, not in w)
        bool started = false;              // false until the first token is appended
        bool sorted = true;                // false if a docid arrived out of ascending order
    };
    static_assert(sizeof(CompactPostingPool::SliceWriter) == 8,
                  "SliceWriter must stay 8 bytes to keep Term compact");

    // The active vocabulary (term-id -> string): either the borrowed pointer or,
    // in owned mode, &owned_vocab_. Always non-null after construction.
    const std::vector<std::string>& vocab() const { return *vocab_; }

    // Accumulates one already-validated token into the per-id Term.
    void accumulate(uint32_t term_id, uint32_t docid, uint32_t pos);

    // Decodes `t`'s compact chain into a TermPostings (the exact docids/freqs/
    // positions the writer consumes), sorting by docid first if `t.sorted` is false.
    // When `allow_stream_positions` is true (the in-memory drain path), a large
    // sorted term's positions are provided via TermPostings::pos_pump instead of a
    // materialized positions_flat (peak-RSS win). The spill path passes false so the
    // run codec always sees a fully-materialized positions_flat.
    TermPostings to_postings(std::string term, Term&& t, bool allow_stream_positions) const;

    // Returns the touched term-ids sorted by their vocab string (lexicographic).
    // Sorts by a PRECOMPUTED integer string-rank (term-id -> lexicographic rank),
    // not by full std::string compare: a single std::string sort over the whole
    // vocabulary is amortized across every spill, so each spill's sort is an
    // integer compare instead of paying a fresh O(touched * strcmp) on every spill.
    std::vector<uint32_t> sorted_ids() const;
    // Builds string_rank_ (term-id -> lexicographic rank) once, lazily. Idempotent.
    void ensure_string_rank() const;
    // Streams the in-memory terms in sorted order, draining the slot pool (the
    // in-memory single-pass path). When `allow_stream_positions` is true, large
    // sorted terms stream positions via pos_pump (valid only because the callback
    // consumes each term synchronously while the arena is still resident); callers
    // that RETAIN the TermPostings past the drain (finalize_sorted) must pass false.
    doris::Status drain_sorted(const std::function<void(TermPostings&&)>& fn, bool allow_stream_positions);
    // Spills the current buffer to a fresh sorted run file and clears memory.
    doris::Status spill_to_run();
    // Writes all current terms (sorted) to an already-open RunWriter, draining.
    doris::Status drain_to_writer(class RunWriter* w);
    // REAL resident accumulator bytes: pool_.arena_bytes() + slot_of_.capacity()*4.
    // The single source of truth for both the gate-2 spill trigger and the spill
    // space-precheck -- replaces the old gated live_bytes_ estimate.
    uint64_t resident_bytes() const;
    // Reports the signed change in REAL resident bytes (pool_.arena_bytes() +
    // slot_of_.capacity()*4) to mem_reporter_ since the previous call, then caches the
    // new total. Single-source diff: every grow/reset/free emits EXACTLY ONE delta
    // (self-balancing -> impossible to double-count or miss a negative). No-op when
    // mem_reporter_ is null.
    void report_arena_delta();
    // Final k-way merge over the spilled runs (+ the residual flushed as a run).
    // When `allow_stream_positions` is true (the streaming for_each path), a wide
    // merged term streams positions via pos_pump (valid only because fn consumes
    // synchronously while the run readers stay parked); callers that RETAIN the
    // TermPostings past the merge (finalize_sorted) MUST pass false.
    doris::Status merge_runs(const std::function<void(TermPostings&&)>& fn, bool allow_stream_positions);
    // Deletes every temp run file; called from the destructor (RAII cleanup).
    void cleanup_runs();
    // Frees a drained term's accumulator (id leaves the touched set).
    void release_term(uint32_t term_id);

    const std::vector<std::string>* vocab_; // active vocab (borrowed or &owned_)
    std::vector<std::string> owned_vocab_;  // owned mode: interned term strings
    // Owned mode only: term string -> term-id, for interning on first occurrence.
    std::unordered_map<std::string, uint32_t> intern_;

    bool has_positions_;
    size_t spill_threshold_bytes_; // 0 => unlimited (no spilling)
    uint64_t total_tokens_ = 0;

    // POOLED accumulators (replaces a dense vocab-sized std::vector<Term>, which
    // cost ~80 B per vocab id even for the ~empty majority -- the single largest
    // input-phase memory line). slot_of_ is the only vocab-sized array: a 4 B index
    // per id (0 == no live Term; otherwise slot index + 1). slots_ holds ONE Term
    // per CURRENTLY-LIVE id, so its size tracks the live touched count, not the
    // vocabulary. On first touch an id claims a slot (reusing a freed one from
    // free_slots_ when available, else appending). release_term frees the slot back
    // to the pool and clears slot_of_[id]. touched_ids_ lists every live id so
    // finalize/spill iterate touched ids without scanning the whole vocabulary.
    // present_[id] is now (slot_of_[id] != 0). The hot add path is still a vector
    // index + a couple of pushes: no hashing, no per-token allocation.
    std::vector<uint32_t> slot_of_;    // vocab-sized: id -> slot index + 1 (0=empty)
    std::vector<Term> slots_;          // live Term pool (size ~ live touched count)
    std::vector<uint32_t> free_slots_; // recycled slot indices (drained terms)
    std::vector<uint32_t> touched_ids_;
    size_t live_term_count_ = 0; // present (non-drained) terms; == unique_terms()

    // Shared arena backing every live term's DOC and POS varint byte chains. Holds
    // the bulk of the accumulator's memory in a few large blocks (no per-term vector
    // headers, no per-vector doubling slack) -- the compact-RSS win.
    CompactPostingPool pool_;

    // Optional writer-level build-RAM reporter (null off-Doris / unit tests) and the
    // last resident-byte total it was told about. report_arena_delta() diffs the live
    // total (arena_bytes() + slot_of_.capacity()*4) against reported_resident_.
    MemoryReporter* mem_reporter_ = nullptr;
    int64_t reported_resident_ = 0;

    // Returns the live Term for `term_id`, claiming a pool slot on first touch.
    Term& term_slot(uint32_t term_id, bool* new_term);

    // Appends one byte / one varint to a term's tagged chain, lazily starting the
    // chain on first use (so an untouched term costs no arena bytes).
    void put_byte(Term* t, uint8_t b);
    void put_varint(Term* t, uint64_t v);

    std::vector<std::string> run_paths_; // spilled run temp files (deleted in dtor)
    doris::Status spill_status_;                // first spill / range error, at finalize
    bool drained_ = false;               // set once finalize_sorted/for_each_term_sorted has run;
                                         // a second drain would (spilled path) re-merge the run
                                         // files and re-emit every term, or (in-memory path) emit
                                         // nothing -- both wrong. Guard against the double-drain.

    // Lazily-built vocab-sized map: term-id -> its lexicographic rank among all
    // vocab strings. Computed once (one full std::string sort of the vocabulary)
    // on the first sorted_ids() call, then reused by every spill's id sort. mutable
    // so the const sorted_ids() can fill it on demand.
    mutable std::vector<uint32_t> string_rank_;
};

} // namespace snii::writer
