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

#include <parallel_hashmap/phmap.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/writer/bigram_drop_filter.h"
#include "storage/index/snii/writer/compact_posting_pool.h"
#include "storage/index/snii/writer/memory_reporter.h"

namespace doris::snii::writer {

class GlobalMemoryLimiter; // G09 process-wide build-RAM registry (see below)

// Heterogeneous probe key for a hidden phrase-bigram term (G01 part C): the
// synthetic term make_phrase_bigram_term(left, right) addressed by its two
// fragments, WITHOUT composing the string. The intern-set functors hash and
// compare it piecewise (marker / varint(len(left)) / left / right), so a repeat
// word pair costs zero allocation and zero byte copies on the add hot path.
struct PhraseBigramTermView {
    std::string_view left;
    std::string_view right;
};

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
        for (size_t i = 0; i < doc_index; ++i) {
            off += freqs[i];
        }
        return off;
    }
    // Non-owning view of doc i's positions (length freqs[i]) into positions_flat.
    std::span<const uint32_t> doc_positions(size_t doc_index) const {
        const size_t off = pos_offset(doc_index);
        return {positions_flat.data() + off, freqs[doc_index]};
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
        for (const auto& d : per_doc) {
            positions_flat.insert(positions_flat.end(), d.begin(), d.end());
        }
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
// spill_threshold_bytes is set, the REAL resident accumulator size (see
// resident_bytes(): the posting arena PLUS every live vocab / pair-map / slot /
// rank structure, G08) is compared against the threshold as tokens arrive; once
// it crosses the threshold the buffer SORTS its current terms,
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
    // positive value caps the REAL resident accumulator size (resident_bytes():
    // arena + every live vocab/pair-map/slot/rank structure, G08), triggering a
    // spill when that crosses the cap -- NOT the old per-token estimate.
    // `reporter` is the OPTIONAL writer-level build-RAM reporter (null off-Doris /
    // unit tests). When non-null, the accumulator reports its REAL resident-byte
    // deltas -- resident_bytes() diffs -- positive on grow, negative on every
    // reset/free, exactly once. NEVER reports live_bytes_ (a gated estimate that
    // feeds only the spill threshold).
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
    // one and corrupt the build. Interning probes a heterogeneous (string_view-keyed)
    // set, so a repeat token for an already-seen term allocates NOTHING; a std::string
    // is materialized only on a term's FIRST occurrence (stored once in owned_vocab_).
    // The id overload remains the hot path (no hashing at all); prefer that and
    // reserve this for tests / legacy string-fed callers.
    void add_token(std::string_view term, uint32_t docid, uint32_t pos);

    // Sentinel "no term id" return of add_token_returning_id (a real id would
    // require a four-billion-string vocabulary). Returned only when the token was
    // REJECTED (wrong vocab mode; the error is latched into status()).
    static constexpr uint32_t kInvalidTermId = 0xFFFFFFFFU;

    // Same contract/behavior as add_token(std::string_view, ...) but returns the
    // OWNED-vocab term-id the token was interned/accumulated under (G05): the SNII
    // column writer captures each unigram's id here and feeds adjacent pairs to
    // the id-keyed add_bigram_token below, so the per-pair hot path never hashes
    // or compares term BYTES again. The returned id is stable for the buffer's
    // lifetime for every NON-bigram term (only marker-prefixed bigram terms are
    // ever evicted/recycled by the G04 diet). Returns kInvalidTermId (and latches
    // InvalidArgument) on a borrowed-vocab buffer.
    uint32_t add_token_returning_id(std::string_view term, uint32_t docid, uint32_t pos);

    // ZERO-ALLOC hidden phrase-bigram token (G01 part C): records one occurrence
    // of the synthetic term make_phrase_bigram_term(left, right) WITHOUT ever
    // composing that string on the hot path. The intern probe hashes and
    // compares the term piecewise (marker + varint(len(left)) + left + right)
    // via the transparent PhraseBigramTermView overloads below; the owned
    // std::string is constructed EXACTLY ONCE, on the pair's first-time intern.
    // Byte-identical accumulation to
    //   add_token(format::make_phrase_bigram_term(left, right), docid, pos)
    // (the two entry points intern to the SAME term-id -- one shared content
    // hash/equality), and the same OWNED-vocab-mode-only contract: called on a
    // borrowed-vocab buffer it latches InvalidArgument and ignores the token.
    void add_bigram_token(std::string_view left, std::string_view right, uint32_t docid,
                          uint32_t pos);

    // G05 PAIR-KEYED hidden phrase-bigram token: records one occurrence of the
    // synthetic term make_phrase_bigram_term(vocab[left_id], vocab[right_id])
    // keyed by the uint64 (left_id << 32 | right_id) PAIR KEY -- no byte hashing,
    // no fragment compares, no string storage on the accumulation path at all.
    // `left_id`/`right_id` are the UNIGRAM ids add_token_returning_id handed the
    // caller for the two constituent tokens (both already interned, so their
    // vocab strings are pinned for the buffer's lifetime -- only bigram terms are
    // ever evicted). The pair's Term accumulates under an owned-vocab id whose
    // vocab string stays EMPTY until the composed on-disk term string is
    // materialized from the two unigram strings at the LAST possible moment:
    //   * at a gate-2 spill, for pair terms actually written to the run (df>=2;
    //     df==1 pair terms are evicted instead, exactly like G04), because run
    //     records pin the id and the k-way merge orders by the vocab string;
    //   * at drain start (flush), for every still-live pair term, BEFORE any
    //     sort -- so the emitted term order is identical to the composed-string
    //     order the string-keyed path produces.
    // Byte-identical accumulation and drain output to
    //   add_bigram_token(vocab[left_id], vocab[right_id], docid, pos).
    // OWNED-vocab mode only (latches InvalidArgument otherwise); ids must be
    // in-range non-bigram terms (else latches InvalidArgument, token ignored).
    // MIXING CONTRACT: within one buffer a given pair must be fed EITHER through
    // this id-keyed path OR through the string paths above, never both -- the two
    // interning tables cannot see each other's entries, so mixing would emit the
    // same composed term twice (the documented duplicate-vocab-string caveat).
    // The production writer only ever uses this path; the string paths remain
    // for tests/legacy callers.
    void add_bigram_token(uint32_t left_id, uint32_t right_id, uint32_t docid, uint32_t pos);

    // G04 "bigram diet" phase 2. Call ONCE, before any add, on an OWNED-vocab
    // buffer, and ONLY when the flush-time G01 bigram df-prune WILL be active
    // (config snii_bigram_prune_min_df != 0) -- the caller (SNII column writer)
    // owns that gate. Enables, for every hidden phrase-bigram term (marker
    // prefix, sentinel included):
    //   (a) POSITION SUPPRESSION: bigram tokens accumulate DOCS+FREQ ONLY. Their
    //       positions were pure dead bytes since G01 (surviving bigrams are
    //       written docs+freq with no .prx; pruned ones vanish), yet were still
    //       varint-encoded per token into the arena, spilled into every run and
    //       re-materialized at drain. Suppressed terms emit an EMPTY
    //       positions_flat (and never a pos_pump); freqs are unaffected. The
    //       flush-time validate accepts this because write_prx is false for
    //       bigrams whenever pruning is active. Unigrams are untouched.
    //   (b) VOCAB-CAP EVICTION (when vocab_cap_bytes > 0): once the live bigram
    //       intern storage (string capacity + a fixed per-term overhead
    //       estimate) exceeds vocab_cap_bytes, bigram terms whose CURRENT df is
    //       exactly 1 (the Zipf long tail; never the sentinel) are evicted
    //       INCREMENTALLY -- a bounded sweep step per bigram add, plus a pass at
    //       each gate-2 spill -- releasing their intern string, intern-set entry,
    //       term slot and postings, and recycling their term-id for the next
    //       intern. Every evicted term is recorded in the ever-dropped bloom
    //       (bigram_dropped_filter()); the flush-time process_term drops any
    //       bigram the bloom may contain IN ADDITION to the df threshold, so an
    //       evicted-then-reappearing pair (whose re-interned postings would be
    //       INCOMPLETE) can never materialize. Bloom false positives only
    //       over-drop, which the G01 reader fallback contract makes safe.
    //       Ids already written to a spill run are NEVER evicted or recycled
    //       (the run records reference them; their vocab strings are immutable
    //       from first spill on).
    //   (c) G06 DRAIN-SIDE DF GATE (when bigram_drain_min_df > 0): sinks the
    //       flush-time phrase-bigram df prune (LogicalIndexWriter::process_term's
    //       `df < bigram_prune_min_df` gate) into the buffer drain for PAIR-KEYED
    //       bigram terms, so the Zipf low-df tail is dropped WITHOUT ever
    //       composing its term strings or decoding its postings (on wikipedia
    //       that tail's materialize+emit was ~1/3 of build CPU). See
    //       prepare_pair_terms_for_drain for the exact drop rule and its
    //       exactness guarantee. `bigram_drain_min_df` passed HERE governs
    //       MID-FEED spill drains, which run before the final doc count (and
    //       thus the exact flush threshold) exists -- pass the flush value when
    //       the config fixes it, or a LOWER BOUND of it (e.g. the auto formula's
    //       0-doc floor) otherwise; every mid-feed drop is bloom-recorded, so
    //       even an over-estimate only ever over-drops safely (the G01 reader
    //       fallback contract). The FINAL drain must gate with the EXACT
    //       process_term threshold: set_bigram_drain_min_df below re-plumbs it
    //       at flush (LogicalIndexWriter::build_blocks does this) before the
    //       terminal drain runs. 0 (default) disables the gate entirely: every
    //       live pair term materializes at drain, exactly the pre-G06 behavior.
    // Calling this on a BORROWED-vocab buffer latches InvalidArgument and is
    // ignored (the diet needs the owned intern table).
    void configure_bigram_diet(uint64_t vocab_cap_bytes, uint32_t bigram_drain_min_df = 0);
    bool bigram_diet_configured() const { return bigram_diet_; }

    // G06: (re)sets the drain-side phrase-bigram df gate to `min_df` -- the
    // EXACT effective flush threshold process_term will gate with (0 disables
    // the gate). LogicalIndexWriter::build_blocks calls this with its resolved
    // bigram_prune_min_df right before draining the term source, so the final
    // drain's drop set is byte-identical to what process_term would have pruned.
    // Plain state update, safe on any buffer in any mode: the gate only ever
    // examines pair-map entries, so a borrowed-vocab / no-pair-term buffer is
    // unaffected regardless of the value.
    void set_bigram_drain_min_df(uint32_t min_df) { bigram_drain_min_df_ = min_df; }

    // Ever-dropped bloom over evicted bigram terms; nullptr until the first
    // eviction (so the flush path probes nothing when the feature never fired).
    // Borrowed; valid for the buffer's lifetime.
    const BigramDropFilter* bigram_dropped_filter() const { return bigram_drop_filter_.get(); }

    // Live bigram intern storage estimate the vocab cap is enforced against:
    // sum over live non-sentinel bigram terms of (string capacity +
    // kBigramInternFixedOverheadBytes) for string-keyed terms, or exactly
    // kBigramInternFixedOverheadBytes for a G05 pair-keyed term (it owns NO
    // string until materialization -- the footprint is the fixed pair-map /
    // vocab-slot overhead). Exposed for the cap-bound tests / observability.
    uint64_t bigram_intern_bytes() const { return bigram_intern_bytes_; }

    // Fixed per-term overhead added to a bigram string's capacity when
    // accounting intern storage against the vocab cap: the std::string header in
    // owned_vocab_, the intern-set entry (or the G05 pair-map entry + reverse
    // pair-key slot), and the 4 B slot/rank entries. An estimate (allocator
    // slack varies); deterministic so tests can reason about the cap exactly.
    // A G05 pair-keyed bigram term's WHOLE footprint is this constant (fixed
    // bytes per pair-map entry; no string).
    static constexpr uint64_t kBigramInternFixedOverheadBytes = 64;

    // TEST-ONLY: number of live pair-keyed bigram terms (pair-map entries,
    // including entries whose id was materialized+pinned by a spill). Not part
    // of the production API.
    size_t bigram_pair_terms_for_test() const { return bigram_pair_map_.size(); }

    // G09: joins the PROCESS-WIDE build-RAM registry. Registers this buffer's
    // current resident bytes with `limiter` and forwards every subsequent
    // (debounced, see report_arena_delta) resident total to it; the destructor
    // un-registers. When the registered sum across ALL of the process's live
    // buffers exceeds the limiter's budget, the limiter may set this buffer's
    // ADVISORY spill-request flag from ANOTHER thread; the flag is observed --
    // and the forced spill run ON THIS BUFFER'S OWN THREAD -- by the next
    // add_token's maybe_spill_after_token (see there for the honor rule).
    // Call at most once, right after construction (extra calls are ignored);
    // `limiter` must outlive this buffer. Null / never attached = the G08
    // per-writer behavior, byte-identical.
    void attach_global_limiter(GlobalMemoryLimiter* limiter);

    // TEST-ONLY: G09 advisory-flag observability -- read the pending flag, and
    // plant a request directly (what the limiter does cross-thread) so the
    // owner-honors-at-next-token contract is testable without a registry.
    bool global_spill_requested_for_test() const {
        return global_spill_requested_.load(std::memory_order_relaxed);
    }
    void request_global_spill_for_test() {
        global_spill_requested_.store(true, std::memory_order_relaxed);
    }

    // G09 forced-spill floor (config snii_forced_spill_min_arena_bytes): a
    // pending process-wide forced-spill request is honored only once the
    // reclaimable posting arena holds at least this much (never below one
    // arena block, so a run is always writable). A request planted while the
    // arena is below the floor is a NO-OP that stays PENDING -- it is NOT
    // retried as a spill every token -- and is honored when the arena regrows
    // past the floor. Without the floor, an unreachable global budget (the
    // persistent vocab/pair structures of all writers alone exceeding it)
    // re-flagged every buffer on every report and each honored with a single
    // 32 KiB arena block: thousands of tiny runs per buffer, EMFILE at the
    // k-way merge reopen, failed loads (the conc=16 wikipedia field storm).
    static constexpr uint64_t kDefaultForcedSpillMinArenaBytes = 64ULL << 20; // 64 MiB
    void set_forced_spill_min_arena_bytes(uint64_t bytes) { forced_spill_min_arena_bytes_ = bytes; }
    uint64_t forced_spill_min_arena_bytes() const { return forced_spill_min_arena_bytes_; }

    // G09 run-file cap (config snii_spill_max_run_files_per_buffer): when a
    // new spill would grow the accumulated run-file count past this cap, the
    // existing runs are first MERGE-COMPACTED into one (a k-way merge of the
    // run files back into a single fresh run; term stream byte-identical, the
    // old files deleted) so the buffer never holds more than the cap + 1 run
    // files. Bounds both the final k-way merge's fan-in and -- decisively --
    // its OPEN FILE DESCRIPTORS: every run of a buffer is (re)opened
    // simultaneously and held open for the whole merge, so unbounded run
    // counts across ~100 concurrent writers exhausted the BE nofile rlimit
    // ('Too many open files' at run reopen). 0 disables the cap.
    static constexpr size_t kDefaultMaxRunFilesPerBuffer = 64;
    void set_max_run_files(size_t cap) { max_run_files_ = cap; }
    size_t max_run_files() const { return max_run_files_; }

    // Number of DISTINCT terms accumulated so far (touched ids still resident).
    size_t unique_terms() const;
    uint64_t total_tokens() const { return total_tokens_; }
    bool has_positions() const { return has_positions_; }

    // OK unless an add_token validation error (out-of-range term-id, wrong vocab
    // mode) was latched. for_each_term_sorted now returns its own I/O Status
    // directly; callers that use add_token's latch-and-report pattern MUST check
    // this after draining to surface input-side validation errors.
    [[nodiscard]] Status status() const { return spill_status_; }

    // TEST-ONLY: number of spill run files currently HELD (== 0 in pure
    // in-memory mode). Lets tests assert that a gate-2 spill actually fired
    // once the REAL resident size crossed the configured cap. NOTE: a G09
    // run-cap merge-compaction (see set_max_run_files) collapses the list to
    // ONE file, so the count is not monotonic. Not part of the production API.
    size_t run_count_for_test() const { return run_paths_.size(); }

    // TEST-ONLY: the REAL resident accumulator bytes the gate-2 trigger and the
    // MemoryReporter see (resident_bytes()). Lets the G08 accounting tests assert
    // coverage (>= the externally-measured pair-map/vector footprint) and
    // monotonicity without widening access to the private accounting. Not part of
    // the production API.
    uint64_t resident_bytes_for_test() const { return resident_bytes(); }

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
    Status for_each_term_sorted(const std::function<void(TermPostings&&)>& fn);

private:
    // Compact per-term accumulator: ONE tagged-varint arena chain plus a few cursors.
    // Every token is appended immediately (no deferred flush), so the only running
    // state is the current doc id and the delta base. A sentinel chain head of
    // kNoChain marks a term that has not started its chain yet (so an all-empty term
    // costs no arena bytes). ntok / ndocs bound the decode loop and size reserves.
    // Total ~36 B per live term.
    static constexpr uint32_t kNoChain = 0xFFFFFFFFU;
    struct Term {
        uint32_t head = kNoChain;          // chain read entry point
        CompactPostingPool::SliceWriter w; // append cursor for the chain (8 B)
        uint32_t ntok = 0;                 // total tokens (entries) in the chain
        uint32_t cur_docid = 0;            // most-recent doc id: detects doc change AND
                                           // is the zigzag delta base for the next doc
        // G04/G06: EXACT count of new-doc GROUPS in the chain (one per new_doc
        // tag, i.e. per maximal same-docid token run). Always an UPPER BOUND on
        // the coalesced df (distinct docids), and EQUAL to it while `sorted`
        // holds -- a non-decreasing docid feed cannot revisit a docid
        // non-adjacently, so every group is a distinct doc. ndocs == 1 answers
        // the G04 eviction's "df == 1 vs >= 2" question exactly even when
        // unsorted (an overcount can never make a df>=2 term look like 1), and
        // the G06 drain-side bigram df gate drops a pair term only when
        // `sorted` is set, i.e. when this IS the df process_term would compute.
        // Replaces the former u8 saturating-at-2 counter; the u32 lands in
        // Term's padding, so sizeof(Term) is unchanged.
        uint32_t ndocs = 0;
        uint8_t level = 0;    // current slice level of w (packed here, not in w)
        bool started = false; // false until the first token is appended
        bool sorted = true;   // false if a docid arrived out of ascending order
        // G04 part (b of G01 phase-2 note): true for hidden phrase-bigram terms
        // when the diet is configured -- the chain then stores the new_doc tag
        // ONLY (no position payload; identical bytes to a positions-disabled
        // encoding) and to_postings emits an empty positions_flat.
        bool pos_suppressed = false;
    };
    static_assert(sizeof(CompactPostingPool::SliceWriter) == 8,
                  "SliceWriter must stay 8 bytes to keep Term compact");

    // The active vocabulary (term-id -> string): either the borrowed pointer or,
    // in owned mode, &owned_vocab_. Always non-null after construction.
    const std::vector<std::string>& vocab() const { return *vocab_; }

    // Accumulates one already-validated token into the per-id Term.
    void accumulate(uint32_t term_id, uint32_t docid, uint32_t pos);

    // Per-token gate-2 tail of accumulate(): reports the token's resident growth,
    // then spills when the unified cap / local threshold fires with a worthwhile
    // reclaimable arena (the G08 anti-churn floor), when the G09 process-wide
    // limiter's advisory request flag is pending (honored here, on the owner's
    // own thread; bypasses the G08 floor but requires one allocated arena block
    // so a run is writable), or when the arena nears its hard 4 GiB offset
    // limit. Every add path funnels through accumulate(), so every add path
    // observes this gate.
    void maybe_spill_after_token();

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
    Status drain_sorted(const std::function<void(TermPostings&&)>& fn, bool allow_stream_positions);
    // Spills the current buffer to a fresh sorted run file and clears memory.
    // `evict_low_df_bigrams`: gate-2 mid-feed spills pass true so df==1 bigram
    // terms are EVICTED (bloom-recorded, id recycled) instead of being written
    // to the run -- a run record would otherwise pin the term's vocab string
    // forever (in-run ids are never evictable). The FINAL residual spill inside
    // merge_runs passes false: no token can arrive after it, so its df==1
    // bigrams are already doomed by the flush-time df threshold and blooming
    // them would only add false-positive pressure.
    Status spill_to_run(bool evict_low_df_bigrams);
    // G09 run-file cap enforcement (see set_max_run_files): merge-compacts the
    // current run files into ONE fresh run (same term stream, ids ordered by
    // the current string rank -- in-run ids' vocab strings are pinned, so the
    // rank order over them never changes between spills), deletes the old
    // files and replaces run_paths_ with the compacted one. Called by
    // spill_to_run before opening a new run once the cap is reached.
    Status compact_runs();
    // Writes all current terms (sorted) to an already-open RunWriter, draining.
    // Skips ids whose slot is gone (evicted mid-feed) and, when
    // `evict_low_df_bigrams`, evicts eligible df==1 bigrams instead of writing
    // them; every id actually written is marked in-run (never evictable after).
    Status drain_to_writer(class RunWriter* w, bool evict_low_df_bigrams);
    // REAL resident accumulator bytes -- the single source of truth for the gate-2
    // spill trigger and every MemoryReporter delta. G08: sums EVERY live input-side
    // structure -- the posting arena (unigram AND bigram chains, docs+prx payload)
    // plus the vocab-sized slot index, the Term slot pool + free/touched lists, the
    // owned vocabulary (headers by capacity + string heap payloads via
    // owned_vocab_heap_bytes_) and its intern set, the G05 pair map (capacity *
    // slot size) + reverse pair-key slots, and the G04/rank bookkeeping arrays
    // (free_ids_ / id_in_run_ / string_rank_). Replaces the pre-G05 arena +
    // slot-index-only figure, which left the whole pair-key/vocab machinery
    // invisible to the gate (the wikipedia ~20 GiB overshoot). Capacity, not size,
    // throughout: the reserved tail is resident RSS and survives spills.
    uint64_t resident_bytes() const;
    // Reports the signed change in REAL resident bytes (resident_bytes()) to
    // mem_reporter_ since the previous call, then caches the new total.
    // Single-source diff: every grow/reset/free emits EXACTLY ONE delta
    // (self-balancing -> impossible to double-count or miss a negative). No-op when
    // mem_reporter_ is null.
    void report_arena_delta();
    // Final k-way merge over the spilled runs (+ the residual flushed as a run).
    // When `allow_stream_positions` is true (the streaming for_each path), a wide
    // merged term streams positions via pos_pump (valid only because fn consumes
    // synchronously while the run readers stay parked); callers that RETAIN the
    // TermPostings past the merge (finalize_sorted) MUST pass false.
    Status merge_runs(const std::function<void(TermPostings&&)>& fn, bool allow_stream_positions);
    // Deletes every temp run file; called from the destructor (RAII cleanup).
    void cleanup_runs();
    // Frees a drained term's accumulator (id leaves the touched set).
    void release_term(uint32_t term_id);

    // ---- G04 bigram vocab-cap eviction (owned-vocab mode only) ---------------
    // Shared first-time-intern tail for both owned-mode add paths: recycles a
    // freed (evicted) term-id when one is available -- otherwise appends a fresh
    // id -- stores `term_str` as the id's vocab string, inserts the id into the
    // intern set, and does the bigram intern-storage accounting. Returns the id.
    uint32_t intern_owned_term(std::string&& term_str);
    // True when eviction can fire: diet configured with a non-zero cap on an
    // owned-vocab buffer.
    bool bigram_evict_enabled() const {
        return bigram_diet_ && bigram_vocab_cap_bytes_ != 0 && vocab_ == &owned_vocab_;
    }
    // Cap-accounted footprint of one bigram term's intern storage.
    static uint64_t bigram_term_footprint(const std::string& s) {
        return s.capacity() + kBigramInternFixedOverheadBytes;
    }
    // Is `id` evictable right now: live slot, current df == 1 (ndocs == 1),
    // never written to a spill run, and a non-sentinel phrase-bigram term.
    bool bigram_evictable(uint32_t id) const;
    // Evicts one eligible id: records the term in the ever-dropped bloom,
    // releases its slot/postings, erases it from the intern set, frees its vocab
    // string and recycles the id. Bumps the eviction seam (+ the vocab epoch for
    // string-keyed terms). Eligibility is the caller's job: bigram_evictable
    // (the df==1 cap sweep / spill rule) OR the G06 mid-feed df-gate drop
    // (prepare_pair_terms_for_drain: a never-spilled pair term with exact
    // df < the drain threshold -- possibly df >= 2 -- whose reappearance the
    // bloom must cover).
    void evict_bigram_term(uint32_t id);
    // Bounded incremental sweep step (amortized, never stop-the-world): when the
    // bigram intern storage is over the cap (and the sweep is not paused after a
    // fruitless full lap), scans up to kVocabSweepStride ids from a persistent
    // cursor, evicting every eligible one EXCEPT `just_touched_id` -- the term
    // the current add extended. Skipping it costs one compare and guarantees a
    // recurring pair whose occurrences arrive back-to-back reaches df==2 (and
    // thus permanent immunity) instead of being churned at df==1 by its own
    // add's sweep -- decisive when the vocabulary is no larger than the stride.
    // Called once per owned-mode add.
    void maybe_sweep_bigram_vocab(uint32_t just_touched_id);

    // ---- G05 pair-keyed bigram terms (owned-vocab mode only) -----------------
    // Composes the uint64 pair key add_bigram_token(left_id, right_id) interns
    // bigram terms under.
    static uint64_t make_pair_key(uint32_t left_id, uint32_t right_id) {
        return (static_cast<uint64_t>(left_id) << 32) | right_id;
    }
    // True when `id` is a live pair-keyed bigram term that has NOT yet had its
    // composed string materialized OR was materialized by a spill (the pair-map
    // entry outlives materialization so later occurrences keep finding the id);
    // false for every unigram / string-keyed / recycled id.
    bool is_pair_term(uint32_t id) const {
        return id < pair_of_.size() && pair_of_[id] != kNoPairKey;
    }
    // First-time intern of a pair key: recycles a freed id when available (else
    // appends a fresh id whose vocab string stays EMPTY), records the reverse
    // id -> pair-key mapping, inserts the pair-map entry, and accounts the FIXED
    // per-entry footprint against the vocab cap. Never bumps vocab_epoch_ (the
    // id's vocab string is empty before and after).
    uint32_t intern_pair_term(uint64_t pair_key);
    // FNV-1a 64 of the pair's synthetic term bytes, computed piecewise from the
    // two unigram vocab strings (== fnv of the composed string; the drop-filter
    // key for pair-term evictions).
    uint64_t pair_term_content_fnv(uint64_t pair_key) const;
    // Composes and stores the pair's on-disk term string into owned_vocab_[id]
    // (the single materialization point), switches the cap accounting from the
    // fixed pair footprint to the string footprint, and bumps vocab_epoch_ so
    // the cached lexicographic rank is rebuilt over the COMPOSED string before
    // any subsequent sort. The pair-map entry is kept (later adds of the pair
    // must keep resolving to this id).
    void materialize_pair_term(uint32_t id, uint64_t pair_key);
    // MUST run at the start of every drain that sorts (drain_sorted and
    // drain_to_writer), BEFORE sorted_ids(): pair-keyed terms carry EMPTY vocab
    // strings, which would rank first en bloc and emit the run / stream out of
    // lexicographic order. For every live not-yet-materialized pair term this
    // does exactly one of:
    //   * EVICT it (mid-feed spills only, df==1 -- same rule and bloom as the
    //     G04 string path);
    //   * G06 DF-GATE DROP it (bigram_drain_min_df_ > 0 only): a pair term
    //     whose EXACT df is provably below the flush prune threshold is dropped
    //     WITHOUT materialization and WITHOUT emission -- the same no-trace
    //     disposition process_term's df gate would give it (no dict entry, no
    //     postings, no bloom membership; the reader's G01 pruned-segment
    //     contract covers the miss). "Provably exact" means Term::sorted still
    //     holds, so Term::ndocs == the coalesced df process_term would compute;
    //     an out-of-order-fed term is NEVER dropped here (it materializes and
    //     process_term gates it on the exact coalesced df). On MID-FEED spills
    //     the drop additionally records the term in the ever-dropped bloom via
    //     the full G04 eviction (the pair may reappear with these docids
    //     missing) and therefore requires the eviction machinery (vocab cap
    //     on); on the FINAL drain (terminal in-memory drain / the residual
    //     spill inside merge_runs) nothing can reappear, so the drop skips the
    //     bloom entirely and needs no cap.
    //   * MATERIALIZE its composed string (everything else).
    // Afterwards every id the sort can see ranks by its final on-disk bytes, so
    // the emitted order is identical to the string-keyed build's. Flush-side df
    // pruning and bloom drops stay in LogicalIndexWriter::process_term as the
    // unchanged backstop for whatever still reaches it (string-keyed terms,
    // spill-materialized pair terms, out-of-order-fed pair terms).
    void prepare_pair_terms_for_drain(bool evict_low_df_bigrams);
    // Shared release tail for a live not-yet-materialized pair-keyed bigram
    // term: un-accounts the fixed pair footprint, erases the pair-map entry and
    // the reverse id -> pair-key mapping, releases the term's slot/postings and
    // recycles the id. Deliberately does NOT touch the drop bloom: the G04
    // eviction path blooms BEFORE calling this; the G06 final-drain df drop
    // calls it alone (no bloom -- nothing reappears after the final drain, and
    // blooming would only add false-positive pressure on surviving hot pairs).
    // Never bumps vocab_epoch_ (the id's vocab string was empty and stays so).
    void release_pair_term(uint32_t id, uint64_t pair_key);

    const std::vector<std::string>* vocab_; // active vocab (borrowed or &owned_)
    std::vector<std::string> owned_vocab_;  // owned mode: interned term strings

    // G08: running sum of the owned vocab strings' HEAP payloads (0 for SSO
    // strings -- their bytes live inside the headers owned_vocab_.capacity()
    // already charges; capacity+1 for heap strings). Maintained incrementally so
    // resident_bytes() stays O(1): intern_owned_term and materialize_pair_term
    // CREDIT a stored string, evict_bigram_term DEBITS it before freeing (the
    // credit/debit symmetry), and the terminal drains zero it when owned_vocab_
    // is released.
    uint64_t owned_vocab_heap_bytes_ = 0;

    // G08: fixed per-entry estimate for one intern-set entry. Sized for the
    // pre-G10 NODE-based set (16 B next-ptr+id node, its malloc chunk rounding,
    // and an amortized bucket-array share) and deliberately UNCHANGED by the G10
    // swap to the flat set: resident_bytes() feeds the gate-2 spill trigger, so
    // keeping the constant keeps the resident-byte sequence -- and therefore
    // every spill point and the drained output -- bit-identical to the prior
    // build. For the flat set it OVER-approximates the real cost (~5 B per slot
    // by capacity), which can only fire the gate earlier, never overshoot.
    // Deterministic so the accounting tests can reason about it, and ZERO for an
    // empty set so an untouched (borrowed-mode) buffer charges nothing for it.
    static constexpr uint64_t kInternEntryEstimateBytes = 48;

    // Heterogeneous (is_transparent) functors backing the owned-mode interning set.
    // The set stores ONLY 4-byte term-ids; each id's string lives EXACTLY ONCE in
    // owned_vocab_ (F03 single-store -- no second owned-string map key). Both functors
    // hold a back-pointer to owned_vocab_ and dereference a stored id to
    // owned_vocab_[id] for content hashing/equality.
    //
    // CONTENT HASH: a byte-INCREMENTAL FNV-1a 64 (fnv_update below) replaces
    // std::hash<string_view> so the SAME hash is computable either over a whole
    // string (id / string_view probes) or fragment-by-fragment over a
    // PhraseBigramTermView probe (marker, varint(len(left)), left, right) WITHOUT
    // composing the synthetic bigram string -- the G01 zero-alloc requirement.
    // std::hash cannot be computed piecewise (implementation-defined), which is
    // exactly why it was replaced; equal content ALWAYS hashes equal across all
    // three key forms because every form feeds the identical byte sequence.
    //
    // BOTH functors MUST be transparent (P0919/P1690): a transparent hash alone
    // does NOT enable heterogeneous find(); the equal functor must be transparent
    // too, and both must accept every probe key form.
    static constexpr uint64_t kFnvOffsetBasis = 1469598103934665603ULL;
    static constexpr uint64_t kFnvPrime = 1099511628211ULL;
    static uint64_t fnv_update(uint64_t h, std::string_view bytes) noexcept {
        for (const char c : bytes) {
            h ^= static_cast<unsigned char>(c);
            h *= kFnvPrime;
        }
        return h;
    }
    static size_t hash_term_bytes(std::string_view s) noexcept {
        return static_cast<size_t>(fnv_update(kFnvOffsetBasis, s));
    }
    // Full 64-bit piecewise FNV of the synthetic bigram term addressed by `v`.
    // Fragment order mirrors make_phrase_bigram_term's byte layout exactly:
    // marker ++ varint(len(left)) ++ left ++ right -- so the value EQUALS the
    // FNV-1a 64 of the composed string (the intern-set content hash AND the
    // BigramDropFilter key hash; the G05 pair-keyed eviction blooms with this,
    // never composing the string).
    static uint64_t bigram_view_fnv(const PhraseBigramTermView& v) noexcept {
        uint64_t h = fnv_update(kFnvOffsetBasis, format::kPhraseBigramTermMarker);
        char varint_buf[5];
        const size_t n = format::encode_phrase_bigram_varint32(static_cast<uint32_t>(v.left.size()),
                                                               varint_buf);
        h = fnv_update(h, std::string_view(varint_buf, n));
        h = fnv_update(h, v.left);
        h = fnv_update(h, v.right);
        return h;
    }
    static size_t hash_bigram_view(const PhraseBigramTermView& v) noexcept {
        return static_cast<size_t>(bigram_view_fnv(v));
    }
    struct OwnedVocabHash {
        using is_transparent = void;
        const std::vector<std::string>* vocab = nullptr;
        size_t operator()(std::string_view s) const noexcept { return hash_term_bytes(s); }
        size_t operator()(uint32_t id) const noexcept {
            return hash_term_bytes(std::string_view((*vocab)[id]));
        }
        size_t operator()(const PhraseBigramTermView& v) const noexcept {
            return hash_bigram_view(v);
        }
    };
    struct OwnedVocabEq {
        using is_transparent = void;
        const std::vector<std::string>* vocab = nullptr;
        bool operator()(uint32_t a, uint32_t b) const noexcept { return a == b; }
        bool operator()(uint32_t a, std::string_view s) const noexcept {
            return std::string_view((*vocab)[a]) == s;
        }
        bool operator()(std::string_view s, uint32_t a) const noexcept {
            return std::string_view((*vocab)[a]) == s;
        }
        // Piecewise fragment compare against the stored composed string -- no
        // temporary composition on either side.
        bool operator()(uint32_t a, const PhraseBigramTermView& v) const noexcept {
            return format::phrase_bigram_term_equals(std::string_view((*vocab)[a]), v.left,
                                                     v.right);
        }
        bool operator()(const PhraseBigramTermView& v, uint32_t a) const noexcept {
            return format::phrase_bigram_term_equals(std::string_view((*vocab)[a]), v.left,
                                                     v.right);
        }
    };
    // Owned mode only: interns each distinct term to a term-id on first occurrence.
    // Keyed by term-id (NOT std::string) so the vocab string is stored exactly once
    // (in owned_vocab_); probed heterogeneously with a string_view so a repeat token
    // costs no temporary std::string.
    //
    // G10: a FLAT (open-addressing, SwissTable) set, not std::unordered_set. The
    // node-based set chased one heap node per chained probe -- a cache miss per
    // node through _M_find_before_node_tr (~7% of wiki-import CPU) -- while the
    // flat set probes a contiguous control-byte group. phmap honors the functors'
    // heterogeneous probes: raw_hash_set templates find()/erase() on the key
    // argument whenever BOTH Hash and Eq expose is_transparent (its KeyArgImpl is
    // KeyArg<IsTransparent<Eq> && IsTransparent<Hash>>), and evaluates equality as
    // eq(stored_id, probe) -- overloads OwnedVocabEq provides for all three key
    // forms. phmap post-mixes the functor's hash (phmap_mix) identically on the
    // insert, find and rehash paths, so content-equal key forms still resolve to
    // the same slot group.
    //
    // SAFETY under the intern_owned_term invariant (ids hash owned_vocab_[id]):
    //   * a REHASH (insert-triggered growth / tombstone compaction) re-hashes the
    //     STORED ids -- dereferencing owned_vocab_[id] -- and moves only the
    //     4-byte id slots, NEVER the strings. Every stored id's string is live at
    //     every insert (evict_bigram_term erases the id BEFORE clearing its
    //     string), so no rehash can read a freed string.
    //   * erase(id) hashes owned_vocab_[id] to locate the slot (same ordering
    //     requirement, already satisfied), then tombstones that one slot; no
    //     other element moves or is re-hashed.
    //   * flat-hash iterator/reference invalidation (every insert may invalidate
    //     ALL of them, unlike unordered_set's stable nodes) is moot here: no code
    //     iterates intern_ or holds an iterator/reference across a mutation --
    //     both find() results are copied out (`term_id = *it`) immediately.
    phmap::flat_hash_set<uint32_t, OwnedVocabHash, OwnedVocabEq> intern_;

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

    // ---- G09 process-wide limiter hookup (null / false = feature off) --------
    // The registry this buffer joined via attach_global_limiter (borrowed; must
    // outlive the buffer), and the ADVISORY forced-spill request flag the
    // limiter sets from other threads (only ever under the registry mutex; the
    // owner reads it relaxed on its own thread each token). The flag pointer
    // doubles as the buffer's registry identity.
    GlobalMemoryLimiter* global_limiter_ = nullptr;
    std::atomic<bool> global_spill_requested_ {false};
    // G09 forced-spill floor / run-file cap (see the public setters above).
    uint64_t forced_spill_min_arena_bytes_ = kDefaultForcedSpillMinArenaBytes;
    size_t max_run_files_ = kDefaultMaxRunFilesPerBuffer;

    // Returns the live Term for `term_id`, claiming a pool slot on first touch.
    Term& term_slot(uint32_t term_id, bool* new_term);

    // Appends one varint to a term's tagged chain, lazily starting the chain on
    // first use (so an untouched term costs no arena bytes). G10: the lazy
    // chain-start check runs ONCE per varint, not once per appended byte (the
    // former per-byte put_byte helper) -- identical semantics because a varint
    // is never empty, so "start before the first byte of this varint" and
    // "start on the first appended byte" coincide.
    void put_varint(Term* t, uint64_t v);

    // ---- G05 pair-keyed bigram state (owned-vocab mode only) -----------------
    // pair key (left_id << 32 | right_id) -> owned-vocab term-id. The bigram
    // accumulation hot path is ONE integer-keyed flat-map probe; no term bytes
    // are hashed, compared or stored. Entries are erased on eviction and RETAINED
    // across materialization (an in-run materialized id keeps accumulating later
    // occurrences of its pair).
    phmap::flat_hash_map<uint64_t, uint32_t> bigram_pair_map_;
    // Reverse map id -> pair key (kNoPairKey = not a pair term), sized lazily to
    // the vocab on the first pair intern; the sweep/evict/materialize paths need
    // id-first access. kNoPairKey is unreachable as a real key: left_id ==
    // 0xFFFFFFFF would require a four-billion-entry vocabulary.
    static constexpr uint64_t kNoPairKey = 0xFFFFFFFFFFFFFFFFULL;
    std::vector<uint64_t> pair_of_;

    // ---- G04 bigram diet state (owned-vocab mode only) -----------------------
    bool bigram_diet_ = false;            // position suppression + (cap) eviction on
    uint64_t bigram_vocab_cap_bytes_ = 0; // 0 = no eviction (suppression only)
    // G06 drain-side phrase-bigram df gate (0 = off). Mid-feed spill drains see
    // the configure_bigram_diet value (the flush threshold or a lower bound of
    // it); the final drain sees the EXACT flush threshold, re-plumbed by
    // LogicalIndexWriter::build_blocks via set_bigram_drain_min_df.
    uint32_t bigram_drain_min_df_ = 0;
    uint64_t bigram_intern_bytes_ = 0; // live bigram intern storage (cap metric)
    std::vector<uint32_t> free_ids_;   // evicted ids awaiting recycling
    std::vector<uint8_t> id_in_run_;   // 1 = id written to some spill run (pinned);
                                       // sized lazily at the first evict-enabled spill
    std::unique_ptr<BigramDropFilter> bigram_drop_filter_; // created on first eviction
    uint32_t sweep_cursor_ = 0;              // next vocab id the incremental sweep examines
    uint64_t sweep_scanned_since_evict_ = 0; // fruitless-lap detector
    uint64_t sweep_rearm_bytes_ = 0;         // paused sweep re-arms once bytes reach this
    // Bumped whenever an EXISTING vocab id's string changes (eviction clears it,
    // recycling re-assigns it). string_rank_ caches this epoch: a recycled id
    // would otherwise keep its stale rank (the vocab SIZE alone cannot detect an
    // in-place string change), and a stale rank would emit spill runs out of
    // lexicographic order -- corrupting the k-way merge.
    uint64_t vocab_epoch_ = 0;

    std::vector<std::string> run_paths_; // spilled run temp files (deleted in dtor)
    Status spill_status_;                // first spill / range error, at finalize
    bool drained_ = false;               // set once finalize_sorted/for_each_term_sorted has run;
                                         // a second drain would (spilled path) re-merge the run
                                         // files and re-emit every term, or (in-memory path) emit
                                         // nothing -- both wrong. Guard against the double-drain.

    // Lazily-built vocab-sized map: term-id -> its lexicographic rank among all
    // vocab strings. Computed once (one full std::string sort of the vocabulary)
    // on the first sorted_ids() call, then reused by every spill's id sort UNTIL
    // the vocab grows OR an existing id's string mutates (G04 eviction/recycling;
    // detected via string_rank_epoch_ != vocab_epoch_). mutable so the const
    // sorted_ids() can fill it on demand.
    mutable std::vector<uint32_t> string_rank_;
    mutable uint64_t string_rank_epoch_ = 0; // vocab_epoch_ the rank was built at
};

// TEST-ONLY observability seam (mirrors the reader-side decode-counter pattern).
// Counts how many times a vocabulary string is MATERIALIZED into owned_vocab_ during
// owned-mode interning. With single-store interning this is bumped EXACTLY ONCE per
// DISTINCT term (the owned_vocab_.emplace_back) and NEVER per token -- so feeding the
// same term M times still materializes it once, and the per-token temporary probe
// string is gone entirely. Writer tests use it for deterministic allocation
// assertions (count == distinct terms). Process-global; reset between tests. Not part
// of the production API.
namespace testing {
uint64_t vocab_string_materialization_count();
void reset_vocab_string_materialization_count();

// G04 bigram vocab-cap observability seams (same always-on relaxed-atomic
// pattern). Deterministic on the single-threaded build path; reset between
// tests. Not part of the production API.
//   bigram_evictions : terms evicted from the intern table (cap sweep + spill
//                      pass combined); each was recorded in the drop bloom.
//   vocab_cap_sweeps : bounded incremental sweep STEPS executed (each scans at
//                      most kVocabSweepStride vocabulary ids).
uint64_t bigram_evictions();
uint64_t vocab_cap_sweeps();
void reset_bigram_vocab_cap_counters();

// G05 pair-keyed bigram observability seams (same always-on relaxed-atomic
// pattern; deterministic on the single-threaded build path; reset between
// tests). One of the two is bumped per add_bigram_token(left_id, right_id)
// call that reaches the pair map:
//   bigram_pair_map_hits   : the pair key was already interned (the
//                            overwhelming majority of the per-token stream);
//   bigram_pair_map_misses : first-time intern of the pair key (== distinct
//                            pairs interned, counting re-interns after an
//                            eviction).
uint64_t bigram_pair_map_hits();
uint64_t bigram_pair_map_misses();
void reset_bigram_pair_map_counters();

// G06 drain-side df-gate seam: pair-keyed bigram terms dropped by
// prepare_pair_terms_for_drain's df gate WITHOUT materialization -- final-drain
// drops (no bloom) and mid-feed df-gate drops (bloomed via the G04 eviction)
// both count; plain df==1 evictions (the pre-G06 G04 rule, taken before the
// gate is consulted) do NOT. Like the pair-map seams above, the increment is
// compiled ONLY under BE_TEST: the final drain executes it once per live pair
// term (hundreds of millions per wikipedia segment, across concurrent
// writers), where an always-on shared atomic would cache-line ping-pong.
// Deterministic on the single-threaded build path; reset between tests. Not
// part of the production API.
uint64_t bigram_drain_df_drops();
void reset_bigram_drain_df_drops();

// G09 process-wide limiter seam: spills that observed -- and cleared -- a
// PENDING global forced-spill request at the moment they fired (whether or not
// the per-writer gate would also have spilled that token; the request was
// consumed either way). Incremented under BE_TEST only, matching the pair-map
// seams' contention rationale (the check sits on the per-token path of every
// concurrent writer). Deterministic on the single-threaded build path; reset
// between tests. Not part of the production API.
uint64_t global_forced_spills();
void reset_global_forced_spills();

// G09 run-file cap seam: merge-compactions of a buffer's accumulated spill
// runs (each collapses the whole run list into one file). Always-on relaxed
// atomic (a compaction is rare -- at most once per cap-many spills -- so
// contention is a non-issue, unlike the per-token seams above). Deterministic
// on the single-threaded build path; reset between tests. Not part of the
// production API.
uint64_t run_compactions();
void reset_run_compactions();
} // namespace testing

} // namespace doris::snii::writer
