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

#include "storage/index/snii/writer/spimi_term_buffer.h"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <utility>

#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/writer/spill_run_codec.h"
#include "storage/index/snii/writer/temp_dir.h"

#if defined(__GLIBC__)
#include <malloc.h>
#endif

namespace doris::snii::writer {

namespace {

// Returns freed heap arenas to the OS (glibc only). The spill encode churns many
// small allocations whose freed chunks glibc retains in its arenas; trimming
// before the peak-RSS-defining merge phase recovers that retention. No-op (and
// harmless) on non-glibc libcs.
void TrimMalloc() {
#if defined(__GLIBC__)
    ::malloc_trim(0);
#endif
}

// Process-unique temp path for a spill run under `dir` (pid + monotonic counter so
// parallel builds / multiple buffers never collide).
std::string MakeRunPath(const std::string& dir) {
    static std::atomic<uint64_t> counter {0};
    const uint64_t n = counter.fetch_add(1);
    return dir + "/snii_spill_" + std::to_string(::getpid()) + "_" + std::to_string(n) + ".run";
}

// TEST-ONLY seam backing testing::vocab_string_materialization_count(). Bumped once
// per DISTINCT interned term (owned_vocab_.emplace_back), never per token. Relaxed:
// the build path is single-threaded, so only the COUNT matters, not ordering.
std::atomic<uint64_t> g_vocab_materializations {0};

// G04 bigram vocab-cap seams (same always-on relaxed pattern): evictions from
// the intern table, and bounded incremental sweep steps executed.
std::atomic<uint64_t> g_bigram_evictions {0};
std::atomic<uint64_t> g_vocab_cap_sweeps {0};

// Vocabulary ids examined per incremental sweep step. Small enough that a step
// is noise on the per-token add path, large enough that the sweep's amortized
// eviction rate (typically many eligible ids per step -- the over-cap
// vocabulary is dominated by the df==1 tail) outpaces the one-term-per-add
// intern growth that armed it.
constexpr uint32_t kVocabSweepStride = 64;

} // namespace

namespace testing {
uint64_t vocab_string_materialization_count() {
    return g_vocab_materializations.load(std::memory_order_relaxed);
}
void reset_vocab_string_materialization_count() {
    g_vocab_materializations.store(0, std::memory_order_relaxed);
}
uint64_t bigram_evictions() {
    return g_bigram_evictions.load(std::memory_order_relaxed);
}
uint64_t vocab_cap_sweeps() {
    return g_vocab_cap_sweeps.load(std::memory_order_relaxed);
}
void reset_bigram_vocab_cap_counters() {
    g_bigram_evictions.store(0, std::memory_order_relaxed);
    g_vocab_cap_sweeps.store(0, std::memory_order_relaxed);
}
} // namespace testing

SpimiTermBuffer::SpimiTermBuffer(const std::vector<std::string>* vocab, bool has_positions,
                                 size_t spill_threshold_bytes, MemoryReporter* reporter)
        : vocab_(vocab),
          // Bind the interning set's heterogeneous functors to &owned_vocab_ even in
          // borrowed mode: the back-pointer is harmless here because
          // add_token(string_view) rejects before touching intern_ (the functors are
          // never dereferenced), and binding unconditionally keeps both constructors
          // symmetric. Initialized in the member-init list (NOT the body): the functors
          // are NESTED types, whose default-constructibility is not yet established at
          // the point unordered_set's defaulted default ctor would be needed for a
          // body assignment, so default-constructing intern_ is ill-formed. The
          // (bucket_count, hash, equal) constructor sidesteps that entirely.
          // owned_vocab_ is constructed before intern_ (declaration order) and the
          // buffer is non-movable, so &owned_vocab_ is stable for the buffer's life.
          intern_(0, OwnedVocabHash {&owned_vocab_}, OwnedVocabEq {&owned_vocab_}),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter) {
    // Borrowed-vocab mode: only the 4 B/id slot-index array is sized to the
    // vocabulary; the Term pool (slots_) grows with the LIVE touched count, so an
    // all-but-empty vocabulary costs ~4 B/id instead of ~80 B/id.
    slot_of_.assign(vocab_->size(), 0);
    // The vocab-sized slot index is resident immediately and survives spills; report
    // its initial positive delta now.
    report_arena_delta();
}

SpimiTermBuffer::SpimiTermBuffer(bool has_positions, size_t spill_threshold_bytes,
                                 MemoryReporter* reporter)
        : vocab_(&owned_vocab_),
          // Owned-vocab mode: bind the interning set's heterogeneous functors to
          // &owned_vocab_ so a stored term-id dereferences to its string for content
          // hashing and equality. Initialized in the member-init list (NOT the body):
          // the functors are NESTED types whose default-constructibility is not yet
          // established where unordered_set's defaulted default ctor would be needed for
          // a body assignment, so the (bucket_count, hash, equal) constructor is used
          // instead. owned_vocab_ is constructed before intern_ (declaration order) and
          // the buffer is non-movable, so &owned_vocab_ is stable for the buffer's life.
          intern_(0, OwnedVocabHash {&owned_vocab_}, OwnedVocabEq {&owned_vocab_}),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter) {
    // Owned-vocab mode: the vocabulary grows as strings are interned in
    // add_token(string_view, ...).
}

SpimiTermBuffer::~SpimiTermBuffer() {
    // Balance the writer-level / Doris tracker on the error path: if the buffer is
    // destroyed while resident bytes were reported but not yet freed-and-reported
    // (e.g. a build aborts before draining), return them here so nothing leaks.
    if (mem_reporter_ != nullptr && reported_resident_ != 0) {
        mem_reporter_->report(-reported_resident_);
        reported_resident_ = 0;
    }
    cleanup_runs();
}

void SpimiTermBuffer::configure_bigram_diet(uint64_t vocab_cap_bytes) {
    // The diet suppresses positions and (with a cap) evicts through the OWNED
    // intern table; a borrowed vocab has neither, so reject loudly instead of
    // silently doing nothing (mirrors the add_bigram_token mode contract).
    if (vocab_ != &owned_vocab_) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi: configure_bigram_diet requires owned-vocab mode");
        }
        return;
    }
    bigram_diet_ = true;
    bigram_vocab_cap_bytes_ = vocab_cap_bytes;
}

void SpimiTermBuffer::report_arena_delta() {
    if (mem_reporter_ == nullptr) {
        return;
    }
    // Diff the REAL resident bytes (arena + slot index) against the last reported
    // total; emit the signed delta exactly once.
    const auto now = static_cast<int64_t>(resident_bytes());
    // Per-token zero-delta debounce: skip the locked fetch_add when resident is
    // unchanged (the common case -- arena_bytes() grows only ~every 32 KiB block and
    // the borrowed-vocab slot index is fixed-capacity, so most tokens see delta==0). A
    // delta==0 report() is a no-op (current_.fetch_add(0) plus a mirrored
    // consume_release(0)) and leaves reported_resident_ == now, so current_bytes(),
    // every over_cap() result, and the gate-2 spill timing stay bit-for-bit identical.
    // This debounces report() ONLY: accumulate() still evaluates over_cap()
    // UNCONDITIONALLY every token, because the writer-level UNIFIED total (shared with
    // the dict buffer) can cross the cap while this buffer's local delta is 0 -- gating
    // over_cap() on this delta would miss that spill.
    if (now == reported_resident_) {
        return;
    }
    mem_reporter_->report(now - reported_resident_);
    reported_resident_ = now;
}

size_t SpimiTermBuffer::unique_terms() const {
    return live_term_count_;
}

uint64_t SpimiTermBuffer::resident_bytes() const {
    // REAL resident accumulator bytes: the posting arena plus the vocab-sized slot
    // index (capacity, since the reserved-but-unused tail is still resident RSS and
    // survives spills -- spill_to_run does NOT free slot_of_). This is the gate-2
    // spill trigger metric and the spill space-precheck figure -- NOT the old gated
    // live_bytes_ estimate.
    return pool_.arena_bytes() + static_cast<uint64_t>(slot_of_.capacity()) * sizeof(uint32_t);
}

// Returns the live Term for `term_id`, claiming a pool slot on first touch (1 ==
// new). Reuses a freed slot from free_slots_ when available; otherwise appends a
// fresh Term to slots_. slot_of_[term_id] holds (slot index + 1); 0 means empty.
SpimiTermBuffer::Term& SpimiTermBuffer::term_slot(uint32_t term_id, bool* new_term) {
    uint32_t enc = slot_of_[term_id];
    if (enc != 0) {
        *new_term = false;
        return slots_[enc - 1];
    }
    *new_term = true;
    uint32_t slot;
    if (!free_slots_.empty()) {
        slot = free_slots_.back();
        free_slots_.pop_back();
    } else {
        slot = static_cast<uint32_t>(slots_.size());
        slots_.emplace_back();
    }
    slot_of_[term_id] = slot + 1;
    return slots_[slot];
}

// Appends one byte to a term's chain, starting the chain lazily on first use.
void SpimiTermBuffer::put_byte(Term* t, uint8_t b) {
    if (t->head == kNoChain) {
        t->head = pool_.start_chain(&t->w, &t->level);
    }
    pool_.append_byte(&t->w, &t->level, b);
}

void SpimiTermBuffer::put_varint(Term* t, uint64_t v) {
    uint8_t tmp[10];
    const size_t n = encode_varint64(v, tmp);
    for (size_t i = 0; i < n; ++i) {
        put_byte(t, tmp[i]);
    }
}

void SpimiTermBuffer::accumulate(uint32_t term_id, uint32_t docid, uint32_t pos) {
    bool new_term = false;
    Term& t = term_slot(term_id, &new_term);
    if (new_term) {
        touched_ids_.push_back(term_id);
        ++live_term_count_;
        // G04 position suppression: a hidden phrase-bigram term (marker prefix;
        // sentinel included -- its single token is position 0 either way) stores
        // no position payload once the diet is on. Decided ONCE per term at slot
        // claim (a 20-byte prefix check), never per token.
        if (bigram_diet_) {
            t.pos_suppressed = format::is_phrase_bigram_term(vocab()[term_id]);
        }
    }
    // A token starts a new doc unless it continues the most-recent doc for this term.
    const bool new_doc = !t.started || t.cur_docid != docid;
    // Tagged entry: varint((pos << 1) | new_doc). Positions are tagged 0 when
    // disabled buffer-wide OR suppressed for this term (G04 bigram diet: the
    // suppressed encoding is bit-identical to the positions-disabled one). The
    // new_doc bit lets the decoder recover per-doc freqs by counting.
    // Widen to 64-bit so a full 32-bit position survives the << 1 without truncation.
    const bool token_has_pos = has_positions_ && !t.pos_suppressed;
    const uint64_t tagged = token_has_pos
                                    ? ((static_cast<uint64_t>(pos) << 1) | (new_doc ? 1U : 0U))
                                    : (new_doc ? 1U : 0U);
    put_varint(&t, tagged);
    if (new_doc) {
        // Out-of-order docids are tolerated (zigzag delta is signed) and reordered at
        // finalize; flag them so to_postings sorts. The delta base is the previous
        // distinct doc (cur_docid), which is 0 for the very first doc (started==false).
        const int64_t base = t.started ? static_cast<int64_t>(t.cur_docid) : 0;
        if (t.started && docid < t.cur_docid) {
            t.sorted = false;
        }
        const int64_t delta = static_cast<int64_t>(docid) - base;
        put_varint(&t, zigzag_encode(delta));
        t.cur_docid = docid;
        t.started = true;
        if (t.ndocs2 < 2) {
            ++t.ndocs2; // saturating distinct-doc count: eviction needs df==1 vs >=2
        }
    }
    ++t.ntok;
    ++total_tokens_;

    // Gate-2 spill: trigger on REAL resident bytes (arena + slot index), NOT the old
    // gated live_bytes_ estimate. arena_bytes() is monotonic per fill and reset to 0
    // by spill_to_run()'s pool_.reset(), so the trigger self-rearms after each spill.
    // The OTHER trigger is the hard arena safety stop (active even in unlimited mode):
    // when the arena nears the 4 GiB uint32-offset limit -- without it, a single
    // >4 GiB in-memory segment wraps alloc_run and silently corrupts data. A forced
    // spill + final k-way merge stays byte-identical regardless of when it fires.
    constexpr uint64_t kArenaSpillCap = 0xE0000000ULL; // 3.5 GiB, < UINT32_MAX margin
    // Report this token's REAL resident growth FIRST so the writer's unified total
    // (reporter_->current_bytes()) reflects it before the gate-2 check. Single-source
    // diff: cheap (subtraction + relaxed atomic add; arena_bytes() is two field reads).
    report_arena_delta();
    // Gate-2 spill (UNIFIED): when a reporter is attached, trigger on the writer's TOTAL
    // build RAM (arena + slot index + dict) crossing the one configured cap -- the same
    // total and cap every buffer of this writer shares, not a per-buffer threshold. Off
    // Doris (no reporter) fall back to the local spill_threshold_bytes_. The hard arena
    // safety stop (4 GiB uint32-offset limit) is always active. spill_to_run() resets the
    // arena and reports its negative internally, so the unified total drops after a spill.
    const bool over_cap = mem_reporter_ != nullptr ? mem_reporter_->over_cap()
                                                   : (spill_threshold_bytes_ != 0 &&
                                                      resident_bytes() >= spill_threshold_bytes_);
    const bool arena_near_limit = pool_.arena_bytes() >= kArenaSpillCap;
    if ((over_cap || arena_near_limit) && spill_status_.ok()) {
        // Mid-feed spill: evict df==1 bigrams instead of writing them (a run
        // record would pin their vocab strings forever -- see drain_to_writer).
        spill_status_ = spill_to_run(/*evict_low_df_bigrams=*/true);
    }
}

void SpimiTermBuffer::add_token(uint32_t term_id, uint32_t docid, uint32_t pos) {
    // Hot path: a pooled slot lookup + a couple of pushes. No hashing, no string
    // construction per token. Reject (and latch) an out-of-range id.
    if (term_id >= slot_of_.size()) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi: term_id out of vocab range");
        }
        return;
    }
    accumulate(term_id, docid, pos);
}

void SpimiTermBuffer::add_token(std::string_view term, uint32_t docid, uint32_t pos) {
    // Compatibility path: intern the term into the owned vocabulary on first
    // occurrence, then accumulate by its id. ONLY valid in OWNED-vocab mode. In
    // BORROWED-vocab mode vocab_ points at the caller's vector, NOT &owned_vocab_:
    // interning here would grow owned_vocab_ / intern_ / slot_of_ out of step with
    // the active (borrowed) vocab, so the new id indexes the WRONG string and writes
    // a slot_of_ entry the borrowed-vocab build never reconciles -- silent
    // corruption. Reject (and latch) instead of forwarding by a bogus id.
    if (vocab_ != &owned_vocab_) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi: add_token(string_view) requires owned-vocab mode");
        }
        return;
    }
    // F03 single-store invariant, fixed at compile time: the interning set keys on the
    // 4-byte term-id, NEVER on a std::string, so each vocab string lives in exactly one
    // place (owned_vocab_). A regression to a string key would reintroduce the
    // double-store and fail this build.
    static_assert(std::is_same_v<decltype(intern_)::key_type, uint32_t>,
                  "intern_ must key on term-id (single-store); a string key reintroduces F03");

    // Heterogeneous probe with the string_view directly: NO per-token temporary
    // std::string (F21). The set element is the term-id; its content is resolved
    // through owned_vocab_ by the transparent functors.
    auto it = intern_.find(term);
    uint32_t term_id;
    if (it == intern_.end()) {
        // First occurrence: materialize the string exactly once (F03
        // single-store) and intern it, recycling an evicted id when available.
        term_id = intern_owned_term(std::string(term));
    } else {
        term_id = *it; // the set element IS the term-id
    }
    accumulate(term_id, docid, pos);
    // G04: amortized vocab-cap sweep step. Hooked on the string path too so
    // synthetic bigram terms fed by tests / legacy callers behave identically to
    // the piecewise add_bigram_token path. Two compares when the cap is off/idle.
    maybe_sweep_bigram_vocab(term_id);
}

void SpimiTermBuffer::add_bigram_token(std::string_view left, std::string_view right,
                                       uint32_t docid, uint32_t pos) {
    // Same OWNED-vocab-mode contract (and failure latch) as add_token(string_view):
    // interning into a borrowed vocab would corrupt the id space.
    if (vocab_ != &owned_vocab_) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi: add_bigram_token requires owned-vocab mode");
        }
        return;
    }
    // G01 part C hot path: probe the intern set with the PIECEWISE key -- the
    // transparent functors hash/compare marker + varint(len(left)) + left + right
    // fragment by fragment against the stored composed strings, so a REPEAT word
    // pair (the overwhelming majority of the ~per-token bigram stream) performs
    // zero allocation and zero byte copies. The composed std::string is built
    // exactly once, on first-time intern below.
    const PhraseBigramTermView probe {left, right};
    auto it = intern_.find(probe);
    uint32_t term_id;
    if (it == intern_.end()) {
        // The SOLE composition/materialization of this bigram's synthetic term
        // (F03 single-store): hash_bigram_view(probe) == hash_term_bytes(composed)
        // by construction (identical byte sequence through the same FNV update),
        // so the interned id lands in the same bucket the probe searched. An
        // EVICTED-then-reappearing pair misses the probe (eviction erased it) and
        // re-interns here as a fresh term -- by then it is already in the
        // ever-dropped bloom, so the flush drops it regardless of what its
        // re-accumulated df grows to (the G04 completeness invariant).
        term_id = intern_owned_term(format::make_phrase_bigram_term(left, right));
    } else {
        term_id = *it; // the set element IS the term-id
    }
    accumulate(term_id, docid, pos);
    // G04: amortized vocab-cap sweep step (bounded; a no-op two compares while
    // the bigram intern storage is under the cap or the sweep is paused).
    maybe_sweep_bigram_vocab(term_id);
}

// Shared owned-mode first-time-intern tail: stores `term_str` as the new id's
// vocab string (recycling an evicted id when one is free -- keeps the vocab
// vector, slot index and rank arrays from growing past the live vocabulary),
// inserts the id into the intern set, and accounts bigram intern storage
// against the G04 vocab cap. The string is stored BEFORE insert(term_id) so the
// set functors can hash owned_vocab_[term_id]; the set stores only the id, so a
// vocab reallocation never invalidates existing entries.
uint32_t SpimiTermBuffer::intern_owned_term(std::string&& term_str) {
    uint32_t term_id;
    if (!free_ids_.empty()) {
        // Recycle an evicted id. Its old string was cleared at eviction and the
        // id is in NO spill run (only never-spilled ids are evictable), so
        // re-keying it cannot mis-attribute any run record. The in-place string
        // change invalidates the cached lexicographic rank -> bump the epoch.
        term_id = free_ids_.back();
        free_ids_.pop_back();
        owned_vocab_[term_id] = std::move(term_str);
        ++vocab_epoch_;
    } else {
        term_id = static_cast<uint32_t>(owned_vocab_.size());
        owned_vocab_.emplace_back(std::move(term_str));
        slot_of_.push_back(0); // vocab grows: new id starts with no live slot
    }
    g_vocab_materializations.fetch_add(1, std::memory_order_relaxed);
    intern_.insert(term_id);
    if (bigram_diet_) {
        const std::string& s = owned_vocab_[term_id];
        // The sentinel is exempt from the cap (never evictable; one per index).
        if (format::is_phrase_bigram_term(s) && !format::is_phrase_bigram_sentinel_term(s)) {
            bigram_intern_bytes_ += bigram_term_footprint(s);
        }
    }
    return term_id;
}

namespace {

// Reorders a term's flat arrays into ascending-docid order, COALESCING any
// same-docid groups so the result has exactly one entry per docid -- matching the
// k-way-merge path's boundary-doc coalescing and the writer's strictly-ascending
// precondition. Only invoked for the rare term that received out-of-order docids
// (the common ascending path leaves t.sorted true and skips it).
//
// A docid may REVISIT (e.g. feed 5,1,5): the chain holds two separate doc-groups
// for doc 5. A STABLE sort keeps equal-docid groups in arrival order, then the
// coalesce pass sums their freqs and concatenates their positions in that same
// (document/arrival) order -- so the merged positions stay consistent with the
// merged freqs, exactly as the run-order merge would have produced.
void SortByDocid(std::vector<uint32_t>* docids, std::vector<uint32_t>* freqs,
                 std::vector<uint32_t>* positions_flat, bool has_positions) {
    const size_t n = docids->size();
    std::vector<size_t> order(n);
    std::iota(order.begin(), order.end(), 0);
    // STABLE so equal docids keep arrival order: their positions then concatenate in
    // document order, the same order the merge path's run concatenation yields.
    std::ranges::stable_sort(order,
                             [&](size_t a, size_t b) { return (*docids)[a] < (*docids)[b]; });

    std::vector<uint32_t> pos_off;
    if (has_positions) {
        pos_off.resize(n);
        uint32_t running = 0;
        for (size_t i = 0; i < n; ++i) {
            pos_off[i] = running;
            running += (*freqs)[i];
        }
    }
    std::vector<uint32_t> nd, nf, np;
    nd.reserve(n);
    nf.reserve(n);
    if (has_positions) {
        np.reserve(positions_flat->size());
    }
    for (size_t k : order) {
        // Coalesce a revisited docid into the previous entry (it sorts adjacent now):
        // sum freqs and append this group's positions right after the prior group's,
        // so flat doc order stays partitioned by the merged freqs.
        if (!nd.empty() && nd.back() == (*docids)[k]) {
            nf.back() += (*freqs)[k];
        } else {
            nd.push_back((*docids)[k]);
            nf.push_back((*freqs)[k]);
        }
        if (has_positions) {
            np.insert(np.end(), positions_flat->begin() + pos_off[k],
                      positions_flat->begin() + pos_off[k] + (*freqs)[k]);
        }
    }
    *docids = std::move(nd);
    *freqs = std::move(nf);
    if (has_positions) {
        *positions_flat = std::move(np);
    }
}

} // namespace

namespace {

// Decodes one varint from a pool chain cursor. The chain was written by
// encode_varint*, so the same LEB128 continuation-bit loop reconstructs it.
uint64_t DecodeChainVarint(CompactPostingPool::Cursor* c) {
    uint64_t result = 0;
    int shift = 0;
    for (;;) {
        const uint8_t b = c->next();
        result |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            break;
        }
        shift += 7;
    }
    return result;
}

void SkipChainVarint(CompactPostingPool::Cursor* c) {
    DecodeChainVarint(c);
}

} // namespace

// Decodes a term's compact tagged chain back into a flat TermPostings (the exact
// docids/freqs/positions_flat the writer consumes), so the produced index is
// byte-identical to the legacy raw-uint32 accumulator. The chain holds one entry
// per token: varint((pos << 1) | new_doc); each new_doc entry is followed by a
// zigzag(docid-delta). A doc's freq is the run length of consecutive same-doc
// tokens; positions stream out in document order (empty when positions disabled).
// Stream positions for a sorted term whose token count exceeds this: such a term's
// flat positions buffer (uint32 per token) would be the peak-RSS transient (tens of
// MiB for the widest term). Below it, the flat buffer is cheap and simpler.
static constexpr uint32_t kStreamPositionsTokenThreshold = 1U << 16; // 65536

TermPostings SpimiTermBuffer::to_postings(std::string term, Term&& t,
                                          bool allow_stream_positions) const {
    TermPostings tp;
    tp.term = std::move(term);
    if (t.ntok == 0 || t.head == kNoChain) {
        return tp;
    }

    // Reserve docids/freqs by ntok (an upper bound on the doc count: ntok >= ndocs).
    // The doc count is not stored separately to keep Term compact; since the corpus
    // is freq~1 per (term, doc), ntok ~= ndocs so the over-reserve is negligible.
    tp.docids.reserve(t.ntok);
    tp.freqs.reserve(t.ntok);

    // Per-TERM positions capability: a G04 position-suppressed bigram term
    // stored no position payload (its chain holds bare new_doc tags, identical
    // to a positions-disabled encoding), so it decodes -- and is emitted -- as a
    // docs+freq-only term: positions_flat stays empty and no pos_pump is ever
    // built. The flush path never writes .prx for it (write_prx false whenever
    // pruning is active), so nothing downstream misses the bytes.
    const bool term_has_pos = has_positions_ && !t.pos_suppressed;

    // For a large SORTED term, stream positions on demand instead of materializing a
    // multi-MiB flat buffer: the writer (prx builder) pulls them window by window via
    // pos_pump, decoding straight from the still-resident arena chain. Out-of-order
    // terms (rare, defensive) need a full sort, so they always use the flat path.
    const bool stream_pos = allow_stream_positions && term_has_pos && t.sorted &&
                            t.ntok >= kStreamPositionsTokenThreshold;
    if (term_has_pos && !stream_pos) {
        tp.positions_flat.reserve(t.ntok);
    }

    CompactPostingPool::Cursor c = pool_.cursor(t.head, t.w.cur);
    int64_t prev = 0;
    for (uint32_t i = 0; i < t.ntok; ++i) {
        const uint64_t tagged = DecodeChainVarint(&c);
        const bool new_doc = (tagged & 1U) != 0;
        if (new_doc) {
            prev += zigzag_decode(DecodeChainVarint(&c));
            tp.docids.push_back(static_cast<uint32_t>(prev));
            tp.freqs.push_back(0);
        }
        ++tp.freqs.back(); // count this token toward the current doc's freq
        if (term_has_pos && !stream_pos) {
            tp.positions_flat.push_back(static_cast<uint32_t>(tagged >> 1));
        }
    }

    // Decide the FINAL position handling now that df (= docids.size()) is known.
    // pos_pump is honored ONLY by the windowed writer path (build_windowed_entry),
    // taken when df >= kSlimDfThreshold. A SLIM term (df below it) goes through
    // build_slim_entry, which reads positions_flat directly -- so streaming would
    // leave it empty and crash. A high-ntok but low-df term (many repeats in few
    // docs) therefore falls back to materializing its df-bounded positions here.
    const bool windowed_path = tp.docids.size() >= format::kSlimDfThreshold;
    if (stream_pos && windowed_path) {
        // Hand the writer a sequential position source backed by a SECOND pass over the
        // same chain (the chain stays resident in pool_ for the whole drain). The pump
        // yields positions in document order -- identical to positions_flat -- so the
        // produced .prx is byte-for-byte the same. The cursor is shared/advanced across
        // calls (the writer pulls in order, exactly pos_total positions total).
        tp.pos_total = t.ntok;
        auto cur = std::make_shared<CompactPostingPool::Cursor>(pool_.cursor(t.head, t.w.cur));
        tp.pos_pump = [cur](uint32_t* dst, size_t count) {
            // Re-walk the tagged token stream, yielding one position per token. A new-doc
            // token is followed by a zigzag docid-delta varint that must be consumed and
            // discarded so the cursor stays aligned with the encoding.
            for (size_t k = 0; k < count; ++k) {
                const uint64_t tagged = DecodeChainVarint(cur.get());
                if ((tagged & 1U) != 0) {
                    SkipChainVarint(cur.get());
                }
                dst[k] = static_cast<uint32_t>(tagged >> 1);
            }
        };
    } else if (stream_pos) {
        // Slim fallback: the decode loop skipped positions (stream candidate) but the
        // term is slim, so materialize positions_flat in a second pass for build_slim.
        // (stream_pos implies term_has_pos, so a suppressed term never lands here.)
        tp.positions_flat.reserve(t.ntok);
        CompactPostingPool::Cursor pc = pool_.cursor(t.head, t.w.cur);
        for (uint32_t i = 0; i < t.ntok; ++i) {
            const uint64_t tagged = DecodeChainVarint(&pc);
            if ((tagged & 1U) != 0) {
                SkipChainVarint(&pc);
            }
            tp.positions_flat.push_back(static_cast<uint32_t>(tagged >> 1));
        }
    } else if (!t.sorted) {
        // Defensive reorder for the rare out-of-order-docid feed (merge of pre-sorted
        // runs). The common ascending path leaves t.sorted true and skips it.
        // term_has_pos (not the buffer flag): a suppressed term's positions_flat is
        // empty, and indexing it by freqs would read out of range.
        SortByDocid(&tp.docids, &tp.freqs, &tp.positions_flat, term_has_pos);
    }
    return tp;
}

void SpimiTermBuffer::ensure_string_rank() const {
    const std::vector<std::string>& v = vocab();
    if (string_rank_.size() == v.size() && string_rank_epoch_ == vocab_epoch_) {
        return; // already built (or empty vocab) and no id's string mutated since
    }
    // One full lexicographic sort of the vocabulary, amortized over every spill.
    // Rebuilt when the vocab GREW or when G04 eviction/recycling mutated an
    // existing id's string in place (epoch mismatch) -- a stale rank would order
    // spill runs / the k-way merge by the id's OLD string.
    std::vector<uint32_t> order(v.size());
    std::iota(order.begin(), order.end(), 0U);
    std::ranges::sort(order, [&](uint32_t a, uint32_t b) { return v[a] < v[b]; });
    string_rank_.assign(v.size(), 0U);
    for (uint32_t rank = 0; rank < order.size(); ++rank) {
        string_rank_[order[rank]] = rank;
    }
    string_rank_epoch_ = vocab_epoch_;
}

std::vector<uint32_t> SpimiTermBuffer::sorted_ids() const {
    ensure_string_rank();
    std::vector<uint32_t> ids = touched_ids_;
    const std::vector<uint32_t>& rank = string_rank_;
    // Integer rank compare instead of full std::string compare: equal-string ids
    // cannot occur for a dense vocab, so a strict rank order matches the original
    // lexicographic order exactly.
    std::ranges::sort(ids, [&](uint32_t a, uint32_t b) { return rank[a] < rank[b]; });
    return ids;
}

void SpimiTermBuffer::release_term(uint32_t term_id) {
    const uint32_t enc = slot_of_[term_id];
    if (enc == 0) {
        return; // not live (defensive)
    }
    const uint32_t slot = enc - 1;
    slots_[slot] = Term(); // free this term's arrays; the empty Term slot is reusable
    free_slots_.push_back(slot);
    slot_of_[term_id] = 0;
    --live_term_count_;
}

bool SpimiTermBuffer::bigram_evictable(uint32_t id) const {
    if (id >= slot_of_.size()) {
        return false; // defensive: slot index freed (post-drain) or stale cursor
    }
    const uint32_t enc = slot_of_[id];
    if (enc == 0) {
        return false; // no live postings: already drained (in-run) or evicted
    }
    if (slots_[enc - 1].ndocs2 != 1) {
        return false; // df >= 2: past the Zipf tail, never evicted (hot/warm)
    }
    if (id < id_in_run_.size() && id_in_run_[id] != 0) {
        return false; // a spill run references this id: string is pinned forever
    }
    const std::string& s = owned_vocab_[id];
    // The sentinel gates reader semantics ("bigram feature present") and must
    // never be dropped; everything non-bigram is out of scope.
    return format::is_phrase_bigram_term(s) && !format::is_phrase_bigram_sentinel_term(s);
}

// Evicts one eligible bigram id (caller checked bigram_evictable):
//   1. record the term bytes in the ever-dropped bloom -- the flush-time
//      process_term will drop this term even if the pair reappears and
//      re-accumulates past the df threshold (its postings would be missing the
//      docid dropped here);
//   2. drop the in-memory postings (release_term; the term's arena chain bytes
//      become dead until the next pool reset -- an amortized cost, reclaimed at
//      the next spill/drain);
//   3. erase the id from the intern set, so a reappearing pair re-interns as a
//      FRESH term instead of resurrecting the dropped one;
//   4. free the vocab string and recycle the id (bounding owned_vocab_ /
//      slot_of_ / string_rank_ to the live vocabulary), bumping the vocab epoch
//      so the cached lexicographic rank is rebuilt before the next spill sort.
void SpimiTermBuffer::evict_bigram_term(uint32_t id) {
    if (bigram_drop_filter_ == nullptr) {
        // First eviction: size partition 0 from the cap -- roughly the live-term
        // count the cap can hold at the fixed-overhead estimate, i.e. the upper
        // bound on evictions a single full sweep can produce.
        bigram_drop_filter_ = std::make_unique<BigramDropFilter>(bigram_vocab_cap_bytes_ /
                                                                 kBigramInternFixedOverheadBytes);
    }
    std::string& s = owned_vocab_[id];
    bigram_drop_filter_->insert(s);
    const uint64_t footprint = bigram_term_footprint(s);
    bigram_intern_bytes_ = bigram_intern_bytes_ > footprint ? bigram_intern_bytes_ - footprint : 0;
    intern_.erase(id);
    release_term(id);
    std::string().swap(s); // free the string payload (capacity 0)
    free_ids_.push_back(id);
    ++vocab_epoch_;
    g_bigram_evictions.fetch_add(1, std::memory_order_relaxed);
}

void SpimiTermBuffer::maybe_sweep_bigram_vocab(uint32_t just_touched_id) {
    if (!bigram_evict_enabled() || bigram_intern_bytes_ <= bigram_vocab_cap_bytes_) {
        return; // feature off or under the cap: two compares, no work
    }
    // Fruitless-lap pause: when a full lap over the vocabulary evicted nothing
    // (everything over the cap is df>=2 or run-pinned -- nothing legal to drop),
    // sweeping again is pure waste until the tail GROWS. sweep_rearm_bytes_ was
    // set to current-bytes + a delta at pause time; stay parked until then.
    if (bigram_intern_bytes_ < sweep_rearm_bytes_) {
        return;
    }
    g_vocab_cap_sweeps.fetch_add(1, std::memory_order_relaxed);
    const size_t vocab_size = owned_vocab_.size();
    uint64_t evicted = 0;
    for (uint32_t scanned = 0;
         scanned < kVocabSweepStride && bigram_intern_bytes_ > bigram_vocab_cap_bytes_; ++scanned) {
        if (sweep_cursor_ >= vocab_size) {
            sweep_cursor_ = 0; // circular scan; ids interned mid-lap are caught next lap
        }
        const uint32_t id = sweep_cursor_++;
        // Never evict the term the CURRENT add just extended: its df may be
        // about to grow (a recurring pair's occurrences often arrive
        // back-to-back), and skipping it guarantees such a pair reaches df==2
        // immunity instead of being churned at df==1 by its own add's sweep --
        // decisive when the live vocabulary is no larger than the stride.
        if (id == just_touched_id) {
            continue;
        }
        if (bigram_evictable(id)) {
            evict_bigram_term(id);
            ++evicted;
        }
    }
    if (evicted != 0) {
        sweep_scanned_since_evict_ = 0;
        sweep_rearm_bytes_ = 0;
        return;
    }
    sweep_scanned_since_evict_ += kVocabSweepStride;
    if (sweep_scanned_since_evict_ >= vocab_size) {
        // One full fruitless lap: pause until the bigram intern storage grows by
        // a meaningful delta (deterministic; scaled to the cap with a small floor
        // so tiny test caps still re-arm).
        const uint64_t delta = std::max<uint64_t>(4096, bigram_vocab_cap_bytes_ / 64);
        sweep_rearm_bytes_ = bigram_intern_bytes_ + delta;
        sweep_scanned_since_evict_ = 0;
    }
}

Status SpimiTermBuffer::drain_sorted(const std::function<void(TermPostings&&)>& fn,
                                     bool allow_stream_positions) {
    const std::vector<std::string>& v = vocab();
    for (uint32_t id : sorted_ids()) {
        const uint32_t enc = slot_of_[id];
        if (enc == 0) {
            // G04: touched_ids_ may hold ids whose slot is gone -- evicted bigram
            // terms (dropped + bloom-recorded) and the stale first entry of a
            // recycled id appearing twice (duplicates sort adjacently; the live
            // slot drains exactly once).
            continue;
        }
        Term term = slots_[enc - 1];
        release_term(id); // release this term's slot before building the next
        // Allow streaming positions only when the caller consumes synchronously (the
        // arena chain stays resident for the whole drain, so the pump can read from it).
        TermPostings tp = to_postings(v[id], std::move(term), allow_stream_positions);
        fn(std::move(tp));
    }
    touched_ids_.clear();
    // Drop the arena + the slot pool (their bytes are fully decoded) and return the
    // freed chunks to the OS so the process peak reflects only what survives the
    // drain, not retained input-phase arena memory.
    pool_.reset();
    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    TrimMalloc();
    // Arena reset + slot_of_ freed: now real resident ~0, so this emits the final
    // negative that returns every reported byte (no leak after the in-memory drain).
    report_arena_delta();
    return Status::OK();
}

Status SpimiTermBuffer::drain_to_writer(RunWriter* w, bool evict_low_df_bigrams) {
    Status st = Status::OK();
    const std::vector<std::string>& v = vocab();
    const bool evict = evict_low_df_bigrams && bigram_evict_enabled();
    if (evict && id_in_run_.size() < v.size()) {
        // Lazily size the run-pin map at the first evict-enabled spill (1 B/id,
        // bounded because the cap bounds the vocabulary).
        id_in_run_.resize(v.size(), 0);
    }
    // Spill writes by term-id (no string IO). Iterate touched ids in vocab-string
    // order so each run is sorted; the k-way merge re-orders runs by the same key.
    for (uint32_t id : sorted_ids()) {
        const uint32_t enc = slot_of_[id];
        if (enc == 0) {
            continue; // evicted mid-feed / stale duplicate of a recycled id (see drain_sorted)
        }
        if (evict && bigram_evictable(id)) {
            // G04 spill-side eviction: a df==1 bigram written to this run would
            // PIN its vocab string for the rest of the build (run records key on
            // the id, so an in-run id can never be evicted or recycled) -- across
            // tens of spills the pinned tail would defeat the cap entirely. Drop
            // it here instead: bloom-recorded, so if the pair reappears later its
            // re-interned term is dropped at flush; if it never reappears the
            // flush-time df threshold would have dropped it anyway (df 1 < any
            // active threshold's minimum of 1... except threshold==1, which the
            // bloom drop covers -- over-drop, safe under the fallback contract).
            evict_bigram_term(id);
            continue;
        }
        if (evict) {
            id_in_run_[id] = 1; // this run now references the id: pinned for good
        }
        Term term = slots_[enc - 1];
        release_term(id);
        // Spill path: the run codec serializes positions_flat directly, so positions
        // must be materialized (no streaming pump). A G04 position-suppressed
        // bigram term serializes an EMPTY position block (n_pos == 0), which the
        // run codec and merge handle per-term.
        TermPostings tp = to_postings(v[id], std::move(term), /*allow_stream=*/false);
        if (st.ok()) {
            st = w->write_term(id, tp);
        }
    }
    touched_ids_.clear();
    pool_.reset(); // all chains decoded into the run; free the arena for the refill
    // The spill returns the arena to 0; slot_of_ keeps its capacity (survives
    // the spill). Report the arena-drop negative now so the gate-2 spill is balanced
    // immediately, not deferred to the next token.
    report_arena_delta();
    return st;
}

Status SpimiTermBuffer::spill_to_run(bool evict_low_df_bigrams) {
    const std::string dir = resolve_temp_dir();
    // Best-effort space pre-check: fail with a clear, early error rather than a
    // mid-write IoError that leaves a half-written run. Best-effort only (TOCTOU; on
    // tmpfs this reports RAM). resident_bytes() (arena + slot index) is the REAL
    // resident figure about to drain -- a conservative over-estimate of the run size.
    const uint64_t resident = resident_bytes();
    const uint64_t avail = temp_dir_available_bytes(dir);
    if (avail < resident) {
        return Status::Error<ErrorCode::IO_ERROR, false>(
                "spimi: insufficient temp space in '" + dir + "' to spill ~" +
                std::to_string(resident) + " B (~" + std::to_string(avail) +
                " B free); set SNII_TEMP_DIR/TMPDIR to a larger disk");
    }
    const std::string path = MakeRunPath(dir);
    RunWriter w;
    RETURN_IF_ERROR(w.open(path));
    run_paths_.push_back(path); // tracked for cleanup even if a later step fails
    RETURN_IF_ERROR(drain_to_writer(&w, evict_low_df_bigrams));
    // drain emptied touched_ids_ and freed each term's arrays; terms_/present_ keep
    // their (vocab-sized) capacity so the next fill reuses the dense slots with no
    // re-allocation. present_ is already all-zero after release_term per id.
    return w.close();
}

Status SpimiTermBuffer::merge_runs(const std::function<void(TermPostings&&)>& fn,
                                   bool allow_stream_positions) {
    // Flush whatever is still resident as one final sorted run so the k-way merge
    // sees a uniform set of run files (and never holds two term sources at once).
    // NO eviction on this FINAL residual spill: no token can arrive after it, so
    // its df==1 bigrams are dropped by the flush-time df threshold regardless;
    // blooming them here would only inflate the filter (false-positive pressure
    // on hot survivors) and, at an explicit threshold of 1, wrongly drop terms
    // the control build would materialize.
    if (!touched_ids_.empty()) {
        Status s = spill_to_run(/*evict_low_df_bigrams=*/false);
        if (!s.ok() && spill_status_.ok()) {
            spill_status_ = s;
        }
    }
    if (!spill_status_.ok()) {
        return spill_status_; // a spill or add_token error; emit nothing
    }
    // All terms are now spilled; the merge reads runs and never touches the
    // accumulators. Free the pool + the vocab-sized slot index so the merge phase
    // holds none of the input-side arrays resident -- keeps spill-mode peak RSS
    // down. malloc_trim(0) returns the freed glibc arenas to the OS so the peak RSS
    // measurement reflects the merge transient, not retained input-phase chunks.
    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    TrimMalloc();
    // pool_ was already reset by the final spill_to_run -> drain_to_writer (reported
    // there); this swap frees slot_of_, so report the remaining negative now. After a
    // full spilled drain reported_resident_ returns to 0 (no leak).
    report_arena_delta();
    // The k-way merge keys its heap/gather on the term-id -> lexicographic rank array
    // instead of comparing vocab strings. Build it explicitly here (idempotent -- every
    // spill already builds it via sorted_ids(), and merge_runs is only reached after at
    // least one spill, but the explicit call keeps the rank fresh and sized to the vocab
    // even if a future caller path reaches the merge without a prior spill).
    ensure_string_rank();
    Status s = MergeRuns(run_paths_, vocab(), string_rank_, has_positions_, fn,
                         allow_stream_positions);
    // The merge churns one large coalesced TermPostings per term (the widest term's
    // arrays are tens of MiB) plus per-run reader windows; on completion glibc
    // retains those freed chunks in its arenas. Trim again so the post-merge resident
    // set (and thus the process peak high-water if a later phase allocates) reflects
    // only live state, not merge-transient retention.
    TrimMalloc();
    return s;
}

Status SpimiTermBuffer::for_each_term_sorted(const std::function<void(TermPostings&&)>& fn) {
    // Single-drain contract: a second call would re-merge the (still-present) run
    // files and re-emit every term, or emit nothing in the in-memory path. Return
    // an error and emit NOTHING rather than produce a wrong second stream.
    if (drained_) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "spimi: already drained (single-drain contract)");
    }
    drained_ = true;
    // The callback is invoked synchronously while the arena is resident, so large
    // sorted terms may stream positions via pos_pump (peak-RSS win for the writer).
    if (run_paths_.empty() && spill_status_.ok()) {
        return drain_sorted(fn, /*allow_stream_positions=*/true); // pure in-memory path
    }
    // Spilled path (or add_token latched a validation error): the merge may STREAM
    // a wide term's positions via pos_pump (fn consumes each term synchronously
    // while the run readers stay parked). merge_runs returns the I/O status
    // directly; add_token validation errors surface via spill_status_ inside it.
    return merge_runs(fn, /*allow_stream_positions=*/true);
}

std::vector<TermPostings> SpimiTermBuffer::finalize_sorted() {
    std::vector<TermPostings> out;
    // Single-drain contract (mirrors for_each_term_sorted): a second drain (including
    // a finalize_sorted after a for_each_term_sorted, or vice versa) would re-emit or
    // emit nothing. Latch an error and return EMPTY rather than a wrong result.
    if (drained_) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                    "spimi: already drained (single-drain contract)");
        }
        return out;
    }
    drained_ = true;
    out.reserve(touched_ids_.size());
    // RETAINS each TermPostings past the drain, so positions must be MATERIALIZED
    // (a streamed pos_pump would reference the arena, freed when the drain ends).
    if (run_paths_.empty() && spill_status_.ok()) {
        Status s = drain_sorted([&out](TermPostings&& tp) { out.push_back(std::move(tp)); },
                                /*allow_stream_positions=*/false);
        if (!s.ok() && spill_status_.ok()) {
            spill_status_ = s;
        }
    } else {
        // RETAINS each TermPostings past the merge, so positions MUST be materialized
        // (a streamed pos_pump would reference run readers freed when the merge ends).
        Status s = merge_runs([&out](TermPostings&& tp) { out.push_back(std::move(tp)); },
                              /*allow_stream_positions=*/false);
        if (!s.ok() && spill_status_.ok()) {
            spill_status_ = s;
        }
    }
    return out;
}

void SpimiTermBuffer::cleanup_runs() {
    for (const std::string& p : run_paths_) {
        std::remove(p.c_str());
    }
    run_paths_.clear();
}

} // namespace doris::snii::writer
