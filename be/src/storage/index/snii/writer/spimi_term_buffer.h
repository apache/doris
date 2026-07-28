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
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/compact_posting_pool.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/term_posting_source.h"

namespace doris::segment_v2::inverted_index {
class CommonWordSet;
}

namespace doris::snii::writer {

using StreamedTermConsumer = std::function<Status(StreamedTermPostings&&)>;

// G11: compiled-in marker for the per-token prefetch candidate (the locality
// bench keys its in-process A/B test off this).
#define SNII_G11_PREFETCH 1

class GlobalMemoryLimiter; // G09 process-wide build-RAM registry (see below)

struct PlainTermId {
    uint32_t value = 0;
};

struct ClassifiedPlainTerm {
    PlainTermId id;
    bool is_common = false;
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
    std::vector<uint32_t> docids; // absolute docids
    std::vector<uint32_t> freqs;
    std::vector<uint32_t> positions_flat; // empty when positions disabled
    // Per-term posting shape. A positioned logical index may mix ordinary terms
    // with docs-only accelerator terms; the latter carry one posting per doc and
    // deliberately omit frequencies/positions from the final index.
    bool retain_positions = true;

    size_t document_count() const { return docids.size(); }

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
// resident_bytes(): the posting arena PLUS every live vocab / slot / rank
// structure, G08) is compared against the threshold as tokens arrive. Once it
// crosses the threshold and enough reclaimable posting arena has accumulated,
// the buffer SORTS its current terms,
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
// order: positioned and ordinary docs-only tokens contribute
// varint((pos << 1) | new_doc_bit); when new_doc_bit is set, a
// zigzag-varint(docid - prev_docid) immediately follows. Statless CommonGrams are
// deduplicated per document and store only that document delta, omitting the
// constant new_doc tag. Frequencies are otherwise recovered as the count of
// consecutive same-doc tokens. This drops
// the entire freq stream and the second (positions) chain versus a freq/prox split,
// so the payload is ~3.4x smaller than raw uint32 docids/freqs/positions, and the
// shared arena removes per-vector doubling slack and per-term vector headers. Each
// positioned and ordinary docs-only tokens append straight into the chain.
// Stateless CommonGrams keep a singleton doc id inline and backfill it only when a
// second document arrives. The other live per-term state is the current doc id (to
// detect a doc change) and the delta base.
// The production writer drains each chain through a bounded TermPostingSource.
// to_postings() remains only for explicit materialization in run maintenance and
// test/finalize helpers. positions_flat stays empty (and pos is tagged as 0) when
// positions are disabled; freq still counts.
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
    // positive value is a soft spill threshold for the REAL resident accumulator
    // size (resident_bytes(): arena + every live vocab/slot/rank structure, G08),
    // triggering a spill once enough reclaimable arena has accumulated -- NOT a
    // hard cap on persistent vocabulary memory or the old per-token estimate.
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
    // finalize: sort_by_docid stably sorts by docid and COALESCES same-docid groups
    // (summing freqs, concatenating positions in document order), so the emitted
    // postings have exactly ONE strictly-ascending entry per docid -- matching the
    // k-way merge path and the writer's strictly-ascending precondition.
    void add_token(uint32_t term_id, uint32_t docid, uint32_t pos);
    void add_token(uint32_t term_id, uint32_t docid, uint32_t pos, bool retain_positions);

    // Compatibility overload: records one token by TERM STRING. Valid ONLY on an
    // OWNED-vocab buffer before enable_common_gram_pair_keys(); interns `term` into
    // the internal vocabulary on first occurrence, then forwards by id. Pair-key
    // mode must use the typed plain/gram APIs below so a physical gram and its
    // transient pair key cannot become two ids for the same logical term. Called on
    // a BORROWED-vocab buffer it is REJECTED (latches InvalidArgument, token ignored)
    // -- interning would grow the owned vocab out of step with the borrowed one and
    // corrupt the build. Interning probes a heterogeneous (string_view-keyed) set,
    // so a repeat token for an already-seen term allocates NOTHING; a std::string is
    // materialized only on a term's FIRST occurrence (stored once in owned_vocab_).
    // The id overload remains the hot path (no hashing at all); prefer that and
    // reserve this for tests / legacy string-fed callers.
    void add_token(std::string_view term, uint32_t docid, uint32_t pos);
    void add_token(std::string_view term, uint32_t docid, uint32_t pos, bool retain_positions);

    // SNII CommonGrams fast path. Plain terms are interned once and returned as
    // stable ids; each gram occurrence hashes a fixed 10-byte pair of those ids
    // instead of constructing and hashing the variable-length physical gram key.
    PlainTermId intern_plain_term(std::string_view physical_plain_term);
    // Production CommonGrams path. A physical-key hit proves the injectively mapped
    // logical term was validated previously; a miss validates exactly once before
    // materializing the vocabulary entry.
    PlainTermId intern_plain_term(std::string_view physical_plain_term,
                                  std::string_view logical_plain_term);
    ClassifiedPlainTerm intern_classified_plain_term(
            std::string_view physical_plain_term, std::string_view logical_plain_term,
            const segment_v2::inverted_index::CommonWordSet& common_words);
    void add_plain_token(PlainTermId term_id, uint32_t docid, uint32_t pos);
    void add_common_gram(PlainTermId left, PlainTermId right, uint32_t docid, uint32_t pos,
                         bool retain_positions);
    void add_common_gram_and_plain(PlainTermId left, PlainTermId right, uint32_t docid,
                                   uint32_t gram_pos, uint32_t plain_pos,
                                   bool retain_gram_positions);
    void enable_common_gram_pair_keys();

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
    // persistent vocabulary/slot structures of all writers alone exceeding it)
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
    // coverage and monotonicity without widening access to the private
    // accounting. Not part of
    // the production API.
    uint64_t resident_bytes_for_test() const { return resident_bytes(); }
    size_t string_rank_capacity_for_test() const { return string_rank_.capacity(); }
#ifdef BE_TEST
    static size_t hash_term_bytes_for_test(std::string_view term) { return hash_term_bytes(term); }
    static size_t owned_term_key_size_for_test();
    void set_owned_term_hash_mask_for_test(size_t mask);
#endif

    // Materializes all terms sorted lexicographically; each term's docids are
    // ascending. Convenience wrapper around for_each_term_sorted that keeps the
    // whole result alive at once. Prefer for_each_term_sorted for low peak memory.
    // The returned vectors are caller-owned compatibility output and are not
    // charged to this buffer's internal MemoryReporter after the callback returns.
    // MUST be called at most once: it drains internal state. A SECOND drain (a
    // repeat call, or a finalize_sorted after a for_each_term_sorted, or vice versa)
    // returns EMPTY and latches an error into status() rather than re-emitting.
    std::vector<TermPostings> finalize_sorted();

    // Streams terms to `fn` in lexicographic order. Each source is borrowed for
    // the synchronous callback and fills the writer-owned transfer buffer. The
    // callback must exhaust the source before returning success.
    // MUST be called at most once: it drains internal state. A SECOND drain invokes
    // `fn` zero times and returns an Internal error (a re-merge of the still-present
    // run files would otherwise re-emit every term). Returns non-OK on spill/merge
    // I/O or corruption errors, or if a prior add_token latched a validation error
    // into status().
    Status for_each_term_sorted(const StreamedTermConsumer& fn);

private:
    struct CommonGramPairCache;
    struct CommonGramPlainTermCache;

    enum class PostingChainShape : uint8_t {
        kTaggedPositioned,
        kTaggedDocsOnly,
        kStatlessDocsOnly,
    };

    // Compact per-term accumulator: ONE tagged-varint arena chain plus a few cursors.
    // A statless CommonGram keeps its first distinct doc inline in cur_docid and
    // starts a chain only when a second doc arrives. For other posting shapes, a
    // sentinel chain head marks an empty term. ntok / ndocs bound the decode loop
    // and size reserves.
    // Total 28 B per live term.
    static constexpr uint32_t kNoChain = 0xFFFFFFFFU;
    struct Term {
        uint32_t head = kNoChain;          // chain read entry point
        CompactPostingPool::SliceWriter w; // chain cursor (8 B)
        uint32_t ntok = 0;                 // total tokens (entries) in the chain
        uint32_t cur_docid = 0;            // most-recent doc id: detects doc change AND
                                           // is the zigzag delta base for the next doc
        // Exact count of new-doc groups in the chain (one per new_doc tag). It
        // bounds the decode reserves and equals the distinct-doc count while the
        // input remains sorted; a later out-of-order coalesce can only shrink it.
        uint32_t ndocs = 0;
        PostingChainShape shape = PostingChainShape::kTaggedPositioned;
        uint8_t level = 0;    // current slice level of w (packed here, not in w)
        bool started = false; // false until the first token is accumulated
        bool sorted = true;   // false if a docid arrived out of ascending order
    };
    static_assert(sizeof(CompactPostingPool::SliceWriter) == 8,
                  "SliceWriter must stay 8 bytes to keep Term compact");
    static_assert(sizeof(Term) == 28, "Term must stay compact for high-cardinality imports");

    struct TrackedTermPostings {
        explicit TrackedTermPostings(MemoryReporter* reporter)
                : docids_reservation(reporter == nullptr ? MemoryReporter::Reservation()
                                                         : reporter->make_reservation()),
                  freqs_reservation(reporter == nullptr ? MemoryReporter::Reservation()
                                                        : reporter->make_reservation()),
                  positions_reservation(reporter == nullptr ? MemoryReporter::Reservation()
                                                            : reporter->make_reservation()) {}

        TrackedTermPostings(const TrackedTermPostings&) = delete;
        TrackedTermPostings& operator=(const TrackedTermPostings&) = delete;

        // Reservations precede the posting vectors so physical allocations are
        // destroyed before their charges are released.
        MemoryReporter::Reservation docids_reservation;
        MemoryReporter::Reservation freqs_reservation;
        MemoryReporter::Reservation positions_reservation;
        TermPostings postings;
    };

    // The active vocabulary (term-id -> string): either the borrowed pointer or,
    // in owned mode, &owned_vocab_. Always non-null after construction.
    const std::vector<std::string>& vocab() const { return *vocab_; }

    // Accumulates one already-validated token into the per-id Term and checks the
    // spill gate once for that input token.
    void accumulate(uint32_t term_id, uint32_t docid, uint32_t pos, bool retain_positions);
    void accumulate_without_spill_gate(uint32_t term_id, uint32_t docid, uint32_t pos,
                                       PostingChainShape shape);
    void add_common_gram_without_spill_gate(PlainTermId left, PlainTermId right, uint32_t docid,
                                            uint32_t pos, bool retain_positions);

    // Per-token gate-2 tail of accumulate(): reports the token's resident growth,
    // then spills when the unified cap / local threshold fires with a worthwhile
    // reclaimable arena (the G08 anti-churn floor), when the G09 process-wide
    // limiter's advisory request flag is pending (honored here, on the owner's
    // own thread; bypasses the G08 floor but requires one allocated arena block
    // so a run is writable), or when the arena nears its hard 4 GiB offset
    // limit. Every public add path invokes this gate once; the fused CommonGrams
    // path invokes it after appending both the gram and its right plain token.
    void maybe_spill_after_token();

    Status to_postings(std::string term, Term&& t, TrackedTermPostings* tracked) const;
    class ArenaTermPostingSource;

    // Returns the touched term-ids sorted by their vocab string (lexicographic).
    // The first spill builds the full integer string-rank. Later spills reuse it
    // while the append-only vocabulary is unchanged. If the vocabulary grew, an
    // ordinary spill sorts only this run's touched ids by string; run compaction
    // and final merge rebuild the full rank when they actually require it.
    std::vector<uint32_t> sorted_ids() const;
    // Builds string_rank_ (term-id -> lexicographic rank) for the current complete
    // vocabulary. Idempotent until the append-only vocabulary grows.
    void ensure_string_rank() const;
    Status drain_sorted_streamed(const StreamedTermConsumer& fn);
    // Spills the current buffer to a fresh sorted run file and clears memory.
    Status spill_to_run();
    // G09 run-file cap enforcement (see set_max_run_files): merge-compacts the
    // current run files into ONE fresh run (same term stream, ids ordered by
    // the current string rank), deletes the old
    // files and replaces run_paths_ with the compacted one. Called by
    // spill_to_run before opening a new run once the cap is reached.
    Status compact_runs();
    // Writes all current terms (sorted) to an already-open RunWriter, draining.
    Status drain_to_writer(class RunWriter* w);
    // REAL resident accumulator bytes -- the single source of truth for the gate-2
    // spill trigger and every MemoryReporter delta. G08: sums EVERY live input-side
    // structure -- the posting arena (docs+prx payload)
    // plus the vocab-sized slot index, the Term slot pool + free/touched lists, the
    // owned vocabulary (headers by capacity + string heap payloads via
    // owned_vocab_heap_bytes_) and its intern set, plus the cached string ranks.
    // Capacity, not size, throughout: the reserved tail is resident RSS and
    // survives spills.
    uint64_t resident_bytes() const;
    // Reports the signed change in REAL resident bytes (resident_bytes()) to
    // mem_reporter_ since the previous call, then caches the new total.
    // Single-source diff: every grow/reset/free emits EXACTLY ONE delta
    // (self-balancing -> impossible to double-count or miss a negative). No-op when
    // mem_reporter_ is null.
    void report_arena_delta();
    Status merge_runs_streamed(const StreamedTermConsumer& fn);
    Status prepare_run_merge(std::function<std::string(std::string_view)>* materializer);
    void finish_run_merge();
    // Deletes every temp run file; called from the destructor (RAII cleanup).
    void cleanup_runs();
    // Frees a drained term's accumulator (id leaves the touched set).
    void release_term(uint32_t term_id);

    // Stores a first-seen owned-vocabulary term under a stable id.
    uint32_t append_owned_vocab_term(std::string&& term_str);
    uint32_t intern_owned_term(std::string&& term_str, size_t term_hash);
    uint32_t find_or_intern_owned_term(std::string_view term);
    uint32_t find_or_intern_common_gram_pair(PlainTermId left, PlainTermId right, uint64_t pair);
    uint32_t find_interned_plain_term(std::string_view term, size_t term_hash);
    void remember_plain_term(size_t term_hash, uint32_t term_id);
    bool transient_term_less(uint32_t left_id, uint32_t right_id) const;
    std::string materialize_transient_term(std::string_view term) const;

    const std::vector<std::string>* vocab_; // active vocab (borrowed or &owned_)
    std::vector<std::string> owned_vocab_;  // owned mode: interned term strings

    enum class CommonWordClassification : uint8_t {
        kUnknown,
        kNotCommon,
        kCommon,
    };
    // Stable semantic classification keyed by owned term id. Pair ids remain
    // kUnknown; physical plain ids are classified once from their logical bytes.
    std::vector<CommonWordClassification> common_word_classification_;

    // G08: running sum of the owned vocab strings' HEAP payloads (0 for SSO
    // strings -- their bytes live inside the headers owned_vocab_.capacity()
    // already charges; capacity+1 for heap strings). Maintained incrementally by
    // intern_owned_term so resident_bytes() stays O(1); terminal drains zero it
    // when owned_vocab_ is released.
    uint64_t owned_vocab_heap_bytes_ = 0;

    // G08: fixed per-entry estimate for one intern-set entry. Sized for the
    // pre-G10 NODE-based set (16 B next-ptr+id node, its malloc chunk rounding,
    // and an amortized bucket-array share) and deliberately UNCHANGED by the G10
    // swap to the flat set: resident_bytes() feeds the gate-2 spill trigger, so
    // keeping the constant keeps the resident-byte sequence -- and therefore
    // every spill point and the drained output -- bit-identical to the prior
    // build. It still OVER-approximates the 4-byte flat key plus control bytes
    // and load-factor slack, which can only fire the gate earlier, never overshoot.
    // Deterministic so the accounting tests can reason about it, and ZERO for an
    // empty set so an untouched (borrowed-mode) buffer charges nothing for it.
    static constexpr uint64_t kInternEntryEstimateBytes = 48;

    // The table slot stores only the stable vocabulary id. String probes hash
    // their bytes once; stored ids rehash through the sole owned vocabulary.
    // Equality always resolves term identity from the complete bytes, so hash
    // collisions are harmless and only add comparisons.
    static size_t hash_term_bytes(std::string_view s) noexcept {
        return std::hash<std::string_view> {}(s);
    }

    struct OwnedVocabHash {
        using is_transparent = void;
        const std::vector<std::string>* vocab = nullptr;
        size_t hash_mask = std::numeric_limits<size_t>::max();
        size_t operator()(std::string_view term) const noexcept {
            return hash_term_bytes(term) & hash_mask;
        }
        size_t operator()(uint32_t term_id) const noexcept {
            return (*this)(std::string_view((*vocab)[term_id]));
        }
    };
    struct OwnedVocabEq {
        using is_transparent = void;
        const std::vector<std::string>* vocab = nullptr;
        bool operator()(uint32_t left, uint32_t right) const noexcept { return left == right; }
        bool operator()(uint32_t stored, std::string_view probe) const noexcept;
        bool operator()(std::string_view probe, uint32_t stored) const noexcept;
    };
    // One flat table is the sole ordinary-term admission path. Heterogeneous
    // probes avoid temporary strings; prepared insertion materializes only a miss.
    // A failed table insertion rolls back the preceding vocabulary append, and no
    // iterator survives a mutation.
    phmap::flat_hash_set<uint32_t, OwnedVocabHash, OwnedVocabEq> intern_;

    // CommonGram pair ids are already a canonical, collision-free key. Keep them
    // out of the string-content intern table: an L0 cache miss probes this native
    // map and materializes the 10-byte transient vocabulary key only for a new
    // pair. The map survives ordinary spills because the persistent vocabulary
    // and term ids do; terminal drains release it before output reservations.
    phmap::flat_hash_map<uint64_t, uint32_t> common_gram_pair_intern_;

    bool has_positions_;
    bool common_gram_pair_keys_ = false;
    std::unique_ptr<CommonGramPairCache> common_gram_pair_cache_;
    uint64_t common_gram_pair_cache_bytes_ = 0;
    std::unique_ptr<CommonGramPlainTermCache> common_gram_plain_term_cache_;
    uint64_t common_gram_plain_term_cache_bytes_ = 0;
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

    // Appends one varint to a term's chain, lazily starting the chain on first use
    // (so an untouched term costs no arena bytes).
    void put_varint(Term* t, uint64_t v);

    std::vector<std::string> run_paths_; // spilled run temp files (deleted in dtor)
    Status spill_status_;                // first spill / range error, at finalize
    bool drained_ = false;               // set once finalize_sorted/for_each_term_sorted has run;
                                         // a second drain would (spilled path) re-merge the run
                                         // files and re-emit every term, or (in-memory path) emit
                                         // nothing -- both wrong. Guard against the double-drain.

    // Lazily-built vocab-sized map: term-id -> its lexicographic rank among all
    // vocab strings. `size() == vocab().size()` means the rank is current; a
    // smaller non-zero size is a stale rank retained after vocabulary growth.
    // Its capacity is still advanced on stale ordinary spills so resident-byte
    // accounting and spill-trigger timing keep the prior full-rank charge.
    mutable std::vector<uint32_t> string_rank_;
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
// G11 bench seam (honored under BE_TEST only): disables the add-path
// prefetch hints so the locality bench can A/B them within ONE process.
// Production builds prefetch unconditionally.
void set_bench_disable_g11_prefetch(bool disabled);

uint64_t vocab_string_materialization_count();
void reset_vocab_string_materialization_count();

// G09 process-wide limiter seam: spills that observed -- and cleared -- a
// PENDING global forced-spill request at the moment they fired (whether or not
// the per-writer gate would also have spilled that token; the request was
// consumed either way). Incremented under BE_TEST only because the check sits
// on the per-token path of every
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

// Number of complete-vocabulary lexicographic rank rebuilds. Ordinary spills
// with a stale rank must not increment it; run compaction and final merge may.
uint64_t string_rank_rebuilds();
void reset_string_rank_rebuilds();

// Complete touched vocabularies invert the dense rank in O(N); partial runs
// retain comparison sorting. Both counters compile out of production.
uint64_t dense_rank_inversions();
uint64_t rank_comparison_sorts();
void reset_rank_ordering_counts();

// CommonGrams pair-key terminal-ordering seam. Both counters compile out of
// production: tests use them to prove terminal sorting/materialization takes the
// trusted fixed-key path instead of re-running generic key validation.
uint64_t common_gram_pair_unchecked_decode_count();
uint64_t common_gram_trusted_plain_decode_count();
void reset_common_gram_pair_fast_path_counts();

// CommonGrams pair direct-cache seam. The counters compile out of production;
// tests use them to prove repeated pairs bypass key encoding and the intern table.
// Docs-only pairs additionally suppress same-document repeats, while positioned
// pairs reuse the cached term id and still accumulate every position.
uint64_t common_gram_pair_cache_probes();
uint64_t common_gram_pair_cache_pair_hits();
uint64_t common_gram_pair_cache_same_doc_hits();
void reset_common_gram_pair_cache_counts();

// Native CommonGram pair-interner seam. Normal production pair ingestion must
// never route a transient pair key through the generic string-content table.
uint64_t common_gram_native_pair_probes();
uint64_t common_gram_native_pair_hits();
uint64_t common_gram_native_pair_inserts();
void reset_common_gram_native_pair_intern_counts();

uint64_t common_gram_logical_validation_count();
void reset_common_gram_logical_validation_count();

// CommonGrams plain-term hot-cache seam: total cache probes, cache hits, and
// fallbacks that reached the global intern table.
uint64_t common_gram_plain_cache_probes();
uint64_t common_gram_plain_cache_hits();
uint64_t common_gram_plain_intern_table_probes();
void reset_common_gram_plain_cache_counts();

// Counts equality checks that must dereference owned vocabulary bytes after the
// inline length and prefix checks. Short terms must never increment this counter.
uint64_t owned_term_full_byte_comparison_count();
void reset_owned_term_full_byte_comparison_count();
void fail_next_owned_term_reserve();
void fail_next_owned_term_emplace();

uint64_t spill_gate_check_count();
void reset_spill_gate_check_count();

// Counts compact-chain varint decodes during arena source consumption. Tests use
// this to prevent positioned sources from replaying the token chain.
uint64_t compact_chain_varint_decode_count();
void reset_compact_chain_varint_decode_count();
} // namespace testing

} // namespace doris::snii::writer
