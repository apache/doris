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
#include <array>
#include <atomic>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "common/exception.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/writer/encoded_spill_run.h"
#include "storage/index/snii/writer/global_memory_limiter.h"
#include "storage/index/snii/writer/posting_external_sort.h"
#include "storage/index/snii/writer/spill_run_codec.h"
#include "storage/index/snii/writer/temp_dir.h"

namespace doris::snii::writer {

namespace {

#ifdef BE_TEST
std::atomic<uint64_t> g_owned_term_full_byte_comparisons {0};
std::atomic<bool> g_fail_next_owned_term_reserve {false};
std::atomic<bool> g_fail_next_owned_term_emplace {false};
std::atomic<uint64_t> g_spill_gate_checks {0};
std::atomic<uint64_t> g_compact_chain_varint_decodes {0};
#endif

} // namespace

bool SpimiTermBuffer::OwnedVocabEq::operator()(uint32_t stored,
                                               std::string_view probe) const noexcept {
#ifdef BE_TEST
    g_owned_term_full_byte_comparisons.fetch_add(1, std::memory_order_relaxed);
#endif
    DCHECK_LT(stored, vocab->size());
    return std::string_view((*vocab)[stored]) == probe;
}

bool SpimiTermBuffer::OwnedVocabEq::operator()(std::string_view probe,
                                               uint32_t stored) const noexcept {
    return (*this)(stored, probe);
}

#ifdef BE_TEST
size_t SpimiTermBuffer::owned_term_key_size_for_test() {
    return sizeof(decltype(intern_)::key_type);
}

void SpimiTermBuffer::set_owned_term_hash_mask_for_test(size_t mask) {
    DORIS_CHECK(intern_.empty());
    intern_ = decltype(intern_)(0, OwnedVocabHash {.vocab = &owned_vocab_, .hash_mask = mask},
                                OwnedVocabEq {&owned_vocab_});
}
#endif

namespace {

// Process-unique temp path for a spill run under `dir` (pid + monotonic counter so
// parallel builds / multiple buffers never collide).
std::string make_run_path(const std::string& dir) {
    static std::atomic<uint64_t> counter {0};
    const uint64_t n = counter.fetch_add(1);
    return dir + "/snii_spill_" + std::to_string(::getpid()) + "_" + std::to_string(n) + ".run";
}

// TEST-ONLY seam backing testing::vocab_string_materialization_count(). Bumped once
// per DISTINCT interned term (owned_vocab_.emplace_back), never per token. Relaxed:
// the build path is single-threaded, so only the COUNT matters, not ordering.
#ifdef BE_TEST
std::atomic<uint64_t> g_vocab_materializations {0};
#endif

// G09 seam: spills that consumed a pending process-wide forced-spill request
// (the limiter flagged this buffer as one of the largest reclaimable-arena
// consumers while SNII was over its memory share). Incremented under BE_TEST only
// (per-token path shared by concurrent writers).
std::atomic<uint64_t> g_global_forced_spills {0};

// Test seam for complete-vocabulary rank rebuilds. The increment is compiled
// out of production because ensure_string_rank() may run on the import path.
#ifdef BE_TEST
std::atomic<uint64_t> g_string_rank_rebuilds {0};
std::atomic<uint64_t> g_dense_rank_inversions {0};
std::atomic<uint64_t> g_rank_comparison_sorts {0};
#endif

// G11 bench seam: when set (BE_TEST paths only), the add-path prefetch hints
// are skipped so the locality bench can A/B them in one process. Production
// builds never read it (the hint compiles in unconditionally there).
std::atomic<bool> g_bench_disable_g11_prefetch {false};

// G11 add-path prefetch gate: always-on in production; toggleable under
// BE_TEST for the in-process A/B bench. The branch is perfectly predicted, so
// the bench's OFF arm measures the pre-G11 code path faithfully.
inline bool g11_prefetch_enabled() {
#ifdef BE_TEST
    return !g_bench_disable_g11_prefetch.load(std::memory_order_relaxed);
#else
    return true;
#endif
}

// G08: heap payload of one owned-vocab string -- 0 while it fits the SSO buffer
// (those bytes live inside the 32 B header owned_vocab_.capacity() charges), else
// the allocated buffer (capacity + NUL). The SSO capacity is probed from the
// running stdlib so the classification is exact, not hardcoded.
uint64_t string_heap_bytes(const std::string& s) {
    static const size_t kSsoCapacity = std::string().capacity();
    return s.capacity() > kSsoCapacity ? static_cast<uint64_t>(s.capacity()) + 1 : 0;
}

void order_ids_by_dense_rank(std::vector<uint32_t>* ids, const std::vector<uint32_t>& rank) {
    if (ids->size() == rank.size()) {
        // Touched ids are unique. Equal cardinality therefore means the run covers
        // the complete vocabulary, so invert the dense rank in linear time.
        for (uint32_t term_id = 0; term_id < rank.size(); ++term_id) {
            (*ids)[rank[term_id]] = term_id;
        }
#ifdef BE_TEST
        g_dense_rank_inversions.fetch_add(1, std::memory_order_relaxed);
#endif
        return;
    }

    std::ranges::sort(*ids, [&](uint32_t a, uint32_t b) { return rank[a] < rank[b]; });
#ifdef BE_TEST
    g_rank_comparison_sorts.fetch_add(1, std::memory_order_relaxed);
#endif
}

} // namespace

namespace testing {
void set_bench_disable_g11_prefetch(bool disabled) {
    g_bench_disable_g11_prefetch.store(disabled, std::memory_order_relaxed);
}
uint64_t vocab_string_materialization_count() {
#ifdef BE_TEST
    return g_vocab_materializations.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_vocab_string_materialization_count() {
#ifdef BE_TEST
    g_vocab_materializations.store(0, std::memory_order_relaxed);
#endif
}
uint64_t global_forced_spills() {
    return g_global_forced_spills.load(std::memory_order_relaxed);
}
void reset_global_forced_spills() {
    g_global_forced_spills.store(0, std::memory_order_relaxed);
}
uint64_t string_rank_rebuilds() {
#ifdef BE_TEST
    return g_string_rank_rebuilds.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_string_rank_rebuilds() {
#ifdef BE_TEST
    g_string_rank_rebuilds.store(0, std::memory_order_relaxed);
#endif
}
uint64_t dense_rank_inversions() {
#ifdef BE_TEST
    return g_dense_rank_inversions.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t rank_comparison_sorts() {
#ifdef BE_TEST
    return g_rank_comparison_sorts.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_rank_ordering_counts() {
#ifdef BE_TEST
    g_dense_rank_inversions.store(0, std::memory_order_relaxed);
    g_rank_comparison_sorts.store(0, std::memory_order_relaxed);
#endif
}
uint64_t owned_term_full_byte_comparison_count() {
#ifdef BE_TEST
    return g_owned_term_full_byte_comparisons.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_owned_term_full_byte_comparison_count() {
#ifdef BE_TEST
    g_owned_term_full_byte_comparisons.store(0, std::memory_order_relaxed);
#endif
}
void fail_next_owned_term_reserve() {
#ifdef BE_TEST
    g_fail_next_owned_term_reserve.store(true, std::memory_order_relaxed);
#endif
}
void fail_next_owned_term_emplace() {
#ifdef BE_TEST
    g_fail_next_owned_term_emplace.store(true, std::memory_order_relaxed);
#endif
}
uint64_t spill_gate_check_count() {
#ifdef BE_TEST
    return g_spill_gate_checks.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_spill_gate_check_count() {
#ifdef BE_TEST
    g_spill_gate_checks.store(0, std::memory_order_relaxed);
#endif
}
uint64_t compact_chain_varint_decode_count() {
#ifdef BE_TEST
    return g_compact_chain_varint_decodes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_compact_chain_varint_decode_count() {
#ifdef BE_TEST
    g_compact_chain_varint_decodes.store(0, std::memory_order_relaxed);
#endif
}
} // namespace testing

SpimiTermBuffer::SpimiTermBuffer(const std::vector<std::string>* vocab, bool has_positions,
                                 size_t spill_threshold_bytes, MemoryReporter* reporter)
        : vocab_(vocab),
          // Bind the equality functor to &owned_vocab_ even in borrowed mode:
          // add_token(string_view) rejects before the functor can dereference it,
          // and binding unconditionally keeps both constructors symmetric.
          // Initialized in the member-init list (NOT the body): the functors are
          // NESTED types, whose default-constructibility is not yet established at
          // the point the flat set's default ctor would be needed. The
          // (bucket_count, hash, equal) constructor sidesteps that entirely.
          // owned_vocab_ is constructed before intern_ (declaration order) and the
          // buffer is non-movable, so &owned_vocab_ is stable for the buffer's life.
          intern_(0, OwnedVocabHash {.vocab = &owned_vocab_}, OwnedVocabEq {&owned_vocab_}),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter),
          run_path_reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                                    : reporter->make_postings_reservation()),
          run_ends_(reporter) {
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
          // Owned-vocab mode: bind both functors to the sole vocabulary so stored
          // ids can rehash and string probes resolve full term equality.
          // Initialized in the member-init list (NOT the body):
          // the functors are NESTED types whose default-constructibility is not yet
          // established where the flat set's default ctor (whose noexcept spec inspects
          // the functors) would be needed for a body assignment, so the
          // (bucket_count, hash, equal) constructor is used instead. owned_vocab_ is
          // constructed before intern_ (declaration order) and the buffer is
          // non-movable, so &owned_vocab_ is stable for the buffer's life.
          intern_(0, OwnedVocabHash {.vocab = &owned_vocab_}, OwnedVocabEq {&owned_vocab_}),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter),
          run_path_reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                                    : reporter->make_postings_reservation()),
          run_ends_(reporter) {
    report_arena_delta();
}

SpimiTermBuffer::~SpimiTermBuffer() {
    // G09: leave the process-wide registry FIRST. unregister_buffer removes the
    // entry (and its bytes) under the registry mutex -- the same mutex every
    // flag store is made under -- so once it returns, no other thread can touch
    // global_spill_requested_ while this buffer dies.
    if (global_limiter_ != nullptr) {
        global_limiter_->unregister_buffer(&global_spill_requested_);
        global_limiter_ = nullptr;
    }
    // Balance the writer-level / Doris tracker on the error path: if the buffer is
    // destroyed while resident bytes were reported but not yet freed-and-reported
    // (e.g. a build aborts before draining), return them here so nothing leaks.
    if (mem_reporter_ != nullptr && reported_resident_ != 0) {
        mem_reporter_->report(-reported_resident_);
        reported_resident_ = 0;
    }
    cleanup_runs();
}

void SpimiTermBuffer::attach_global_limiter(GlobalMemoryLimiter* limiter) {
    // At-most-once: a re-attach would leave a stale registry entry behind (the
    // dtor un-registers only the current limiter).
    if (limiter == nullptr || global_limiter_ != nullptr) {
        return;
    }
    global_limiter_ = limiter;
    // Race-safe vs report: registration and every report run on the OWNER's
    // thread, strictly ordered; the registry serializes them against other
    // buffers' calls internally. Register with the current spillable arena
    // bytes (the victim-selection key) so the registry is exact from the first
    // moment. The buffer's total memory needs no reporting here: it already
    // reaches the limiter through the SniiIndexBuild observation tracker that
    // mem_reporter_ feeds.
    global_limiter_->register_buffer(&global_spill_requested_,
                                     static_cast<int64_t>(pool_.arena_bytes()));
}

void SpimiTermBuffer::report_arena_delta() {
    if (mem_reporter_ == nullptr && global_limiter_ == nullptr) {
        return;
    }
    // Diff the REAL resident bytes (resident_bytes()) against the last reported
    // total; emit the signed delta exactly once.
    const auto now = static_cast<int64_t>(resident_bytes());
    // Per-token zero-delta debounce: skip the locked fetch_add when resident is
    // unchanged (the common case -- arena_bytes() grows only ~every 32 KiB block and
    // the other charged structures grow by geometric capacity steps / per new term
    // only, so most tokens see delta==0). A
    // delta==0 report() is a no-op (current_.fetch_add(0) plus a mirrored
    // consume_release(0)) and leaves reported_resident_ == now, so current_bytes(),
    // every over_cap() result, and the gate-2 spill timing stay bit-for-bit identical.
    // The spill gate still evaluates the writer-level UNIFIED total whenever the
    // arena is large enough to reclaim, even if this buffer's local delta is 0:
    // the shared dict buffer may have crossed the cap independently.
    if (now == reported_resident_) {
        return;
    }
    if (mem_reporter_ != nullptr) {
        mem_reporter_->report(now - reported_resident_);
    }
    // G09: forward the current SPILLABLE arena bytes -- as an ABSOLUTE,
    // self-healing value -- to the process-wide registry (the victim-selection
    // key: only the arena is reclaimable by a forced spill; the persistent
    // vocab/pair structures are not). The delta reported above has already
    // moved the observation tracker the limiter judges the SUM by, so this
    // report carries no total of its own. This is the limiter's decision point:
    // report() flags the largest-arena eligible buffers (possibly this one)
    // while SNII is over its share. It only ever takes the registry mutex and
    // flips advisory atomics; no lock is held here while spilling (any spill
    // this buffer performs happens AFTER this returns, back in
    // maybe_spill_after_token, on this thread).
    if (global_limiter_ != nullptr) {
        global_limiter_->report(&global_spill_requested_,
                                static_cast<int64_t>(pool_.arena_bytes()));
    }
    reported_resident_ = now;
}

size_t SpimiTermBuffer::unique_terms() const {
    return live_term_count_;
}

uint64_t SpimiTermBuffer::resident_bytes() const {
    // Everything live is charged by CAPACITY (the reserved tail is resident RSS
    // and survives spills). All reads are O(1), since this runs once per token.
    uint64_t b = pool_.arena_bytes(); // posting chains: docs + prx payload
    b += static_cast<uint64_t>(slot_of_.capacity()) * sizeof(uint32_t); // vocab-sized slot index
    b += static_cast<uint64_t>(slots_.capacity()) * sizeof(Term);       // live Term pool
    b += static_cast<uint64_t>(free_slots_.capacity()) * sizeof(uint32_t);
    b += static_cast<uint64_t>(touched_ids_.capacity()) * sizeof(uint32_t);
    // Owned-vocab machinery (all zero in borrowed mode): string headers by vector
    // capacity, heap payloads via the incrementally-maintained counter, and the
    // intern set's entries at a fixed per-entry estimate (kept at the
    // pre-G10 node-set value so the gate-2 spill points are unchanged; see the
    // constant's comment).
    b += static_cast<uint64_t>(owned_vocab_.capacity()) * sizeof(std::string);
    b += owned_vocab_heap_bytes_;
    b += static_cast<uint64_t>(intern_.size()) * kInternEntryEstimateBytes;
    // Cached lexicographic ranks survive spills and are included by capacity.
    b += static_cast<uint64_t>(string_rank_.capacity()) * sizeof(uint32_t);
    return b;
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

void SpimiTermBuffer::put_varint(Term* t, uint64_t v) {
    if (t->head == kNoChain) {
        t->head = pool_.start_chain(&t->w, &t->level);
    }
    if (v < 0x80U) {
        pool_.append_byte(&t->w, &t->level, static_cast<uint8_t>(v));
        return;
    }
    pool_.append_varint(&t->w, &t->level, v);
}

void SpimiTermBuffer::accumulate_without_spill_gate(uint32_t term_id, uint32_t docid, uint32_t pos,
                                                    PostingChainShape shape) {
    const bool retain_positions = shape == PostingChainShape::kTaggedPositioned;
    DCHECK(!retain_positions || has_positions_);
    bool new_term = false;
    Term& t = term_slot(term_id, &new_term);
    if (new_term) {
        t.shape = shape;
        touched_ids_.push_back(term_id);
        ++live_term_count_;
    } else {
        DCHECK(t.shape == shape);
    }
    // Docs-only accelerator postings are sets. Tokens for one input document are
    // contiguous on the writer path, so discard repeated occurrences before they
    // allocate arena bytes or enter spill/sort/posting encoding.
    if (!retain_positions && t.started && t.cur_docid == docid) {
        ++total_tokens_;
        return;
    }
    // A token starts a new doc unless it continues the most-recent doc for this term.
    const bool first_token = !t.started;
    const bool new_doc = first_token || t.cur_docid != docid;
    // Widen to 64-bit so a full 32-bit position survives the shift.
    const uint64_t tagged = retain_positions
                                    ? ((static_cast<uint64_t>(pos) << 1) | (new_doc ? 1U : 0U))
                                    : (new_doc ? 1U : 0U);
    put_varint(&t, tagged);
    if (new_doc) {
        // Out-of-order docids are tolerated (zigzag delta is signed) and reordered at
        // finalize; flag them for the bounded external sort. The delta base is the previous
        // distinct doc (cur_docid), which is 0 for the very first doc (started==false).
        const int64_t base = t.started ? static_cast<int64_t>(t.cur_docid) : 0;
        if (t.started && docid < t.cur_docid) {
            t.sorted = false;
        }
        const int64_t delta = static_cast<int64_t>(docid) - base;
        put_varint(&t, zigzag_encode(delta));
        t.cur_docid = docid;
        t.started = true;
        // Exact new-doc group count; out-of-order coalescing can only shrink it.
        ++t.ndocs;
    }
    ++t.ntok;
    ++total_tokens_;
}

void SpimiTermBuffer::accumulate(uint32_t term_id, uint32_t docid, uint32_t pos,
                                 bool retain_positions) {
    accumulate_without_spill_gate(term_id, docid, pos,
                                  retain_positions ? PostingChainShape::kTaggedPositioned
                                                   : PostingChainShape::kTaggedDocsOnly);
    maybe_spill_after_token();
}

// Per-input-token gate-2 tail. Every add invokes it after one posting. It
// reports the token's REAL resident growth FIRST so the writer's unified total
// (reporter_->current_bytes()) reflects it before the gate check (single-source
// diff; cheap: a subtraction + relaxed atomic add), then evaluates the spill triggers:
//   * Gate-2 (UNIFIED): with a reporter attached, trigger on the writer's TOTAL
//     build RAM (arena + vocab structures + dict) crossing the one
//     configured cap -- the same total and cap every buffer of this writer
//     shares, not a per-buffer threshold. Off Doris (no reporter) fall back to
//     the local spill_threshold_bytes_ against resident_bytes().
//   * G08 anti-churn floor: a gate-2 spill reclaims ONLY the posting arena
//     (pool_.reset()); the vocab / slot structures resident_bytes()
//     now also charges SURVIVE it. Once those persistent bytes alone exceed the
//     cap, an unconditioned
//     trigger would spill EVERY subsequent token -- one-block runs, k-way-merge
//     and spill-fixed-cost blowup. Honor the cap only when at least a quarter of
//     it is reclaimable arena: peak stays bounded at persistent + cap/4 and no
//     run is smaller than cap/4, while the one-block minimum keeps small caps
//     (tests, tiny configs) spilling on the first block exactly as before.
//   * Hard arena safety stop, active even in unlimited mode and BYPASSING the
//     floor: when the arena nears the 4 GiB uint32-offset limit, spill now --
//     without it a single >4 GiB in-memory segment wraps alloc_run and silently
//     corrupts data. A forced spill + final k-way merge stays byte-identical
//     regardless of when it fires.
// spill_to_run() resets the arena and reports its negative internally, so the
// unified total drops (and the trigger self-rearms) after each spill.
void SpimiTermBuffer::maybe_spill_after_token() {
#ifdef BE_TEST
    g_spill_gate_checks.fetch_add(1, std::memory_order_relaxed);
#endif
    constexpr uint64_t kArenaSpillCap = 0xE0000000ULL; // 3.5 GiB, < UINT32_MAX margin
    const bool global_requested = global_spill_requested_.load(std::memory_order_relaxed);
    const bool arena_near_limit = pool_.arena_bytes() >= kArenaSpillCap;
    report_arena_delta();
    const uint64_t gate_cap =
            mem_reporter_ != nullptr ? mem_reporter_->cap_bytes() : spill_threshold_bytes_;
    const bool arena_worth_spilling =
            pool_.arena_bytes() >= std::max<uint64_t>(CompactPostingPool::kBlockSize, gate_cap / 4);
    // G09: the process-wide limiter flagged this buffer (one of the
    // largest-ARENA eligible consumers while SNII index-build memory was over
    // its share). Honored HERE, on the owner's own thread -- never on the
    // reporting thread that set the flag. The G08 anti-churn floor (cap/4) is
    // deliberately BYPASSED (each victim's arena is below cap/4 by
    // construction: it never reached its per-writer gate -- that is exactly
    // why the global sum grew), but the FORCED-SPILL FLOOR
    // (snii_forced_spill_min_arena_bytes, >= one arena block so a run is
    // writable) still applies: a forced spill reclaims ONLY the arena, so
    // honoring below the floor would cut a tiny run for near-zero relief.
    // Below the floor the request is a NO-OP that stays PENDING -- it is NOT
    // retried as a spill each token -- and is honored once the arena regrows
    // past the floor (the limiter's victim selection applies the same floor,
    // so a below-floor flag only arises from a floor/config race or a test
    // seam). A request that finds the owner already drained is never observed
    // again -- an advisory no-op (the dtor un-registers) -- and a stale
    // re-request after a spill costs at most one extra floor-sized run
    // (double-spill is harmless, byte-identical output).
    const bool global_spill_now =
            global_requested &&
            pool_.arena_bytes() >= std::max<uint64_t>(CompactPostingPool::kBlockSize,
                                                      forced_spill_min_arena_bytes_);
    const bool over_cap = !global_spill_now && !arena_near_limit && arena_worth_spilling &&
                          (mem_reporter_ != nullptr ? mem_reporter_->over_cap()
                                                    : (spill_threshold_bytes_ != 0 &&
                                                       resident_bytes() >= spill_threshold_bytes_));
    if ((over_cap || global_spill_now || arena_near_limit) && spill_status_.ok()) {
        if (global_requested) {
            // Consume the request BEFORE spilling: this spill releases exactly
            // the arena a forced spill would, so it satisfies the request no
            // matter which trigger won the OR above.
            global_spill_requested_.store(false, std::memory_order_relaxed);
#ifdef BE_TEST
            // Seam under BE_TEST only: per-token path shared by every
            // concurrent writer.
            g_global_forced_spills.fetch_add(1, std::memory_order_relaxed);
#endif
        }
        spill_status_ = spill_to_run();
    }
}

void SpimiTermBuffer::add_token(uint32_t term_id, uint32_t docid, uint32_t pos) {
    add_token(term_id, docid, pos, has_positions_);
}

void SpimiTermBuffer::add_token(uint32_t term_id, uint32_t docid, uint32_t pos,
                                bool retain_positions) {
    // Hot path: a pooled slot lookup + a couple of pushes. No hashing, no string
    // construction per token. Reject (and latch) an out-of-range id.
    if (term_id >= slot_of_.size()) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi: term_id out of vocab range");
        }
        return;
    }
    accumulate(term_id, docid, pos, retain_positions);
}

void SpimiTermBuffer::add_token(std::string_view term, uint32_t docid, uint32_t pos) {
    add_token(term, docid, pos, has_positions_);
}

void SpimiTermBuffer::add_token(std::string_view term, uint32_t docid, uint32_t pos,
                                bool retain_positions) {
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
    const uint32_t term_id = find_or_intern_owned_term(term);
    accumulate(term_id, docid, pos, retain_positions);
}

uint32_t SpimiTermBuffer::find_or_intern_owned_term(std::string_view term) {
    static_assert(std::is_same_v<decltype(intern_)::key_type, uint32_t>);
    DCHECK_LE(term.size(), std::numeric_limits<uint32_t>::max());
    const size_t term_hash = intern_.hash(term);
    const auto found = intern_.find(term, term_hash);
    if (found != intern_.end()) {
        const uint32_t term_id = *found;
        if (g11_prefetch_enabled()) {
            __builtin_prefetch(slot_of_.data() + term_id);
        }
        return term_id;
    }
    return intern_owned_term(std::string(term), term_hash);
}

bool SpimiTermBuffer::transient_term_less(uint32_t left_id, uint32_t right_id) const {
    const std::vector<std::string>& v = vocab();
    return std::string_view(v[left_id]) < std::string_view(v[right_id]);
}

// Prepared first-time insertion stores the string before emplace so every
// stored id remains resolvable during later growth rehashes.
uint32_t SpimiTermBuffer::intern_owned_term(std::string&& term_str, size_t term_hash) {
    const size_t next_vocab_size = owned_vocab_.size() + 1;
    DCHECK_LE(next_vocab_size, std::numeric_limits<uint32_t>::max());

    size_t target_capacity = owned_vocab_.capacity();
    if (target_capacity < next_vocab_size) {
        target_capacity = target_capacity <= std::numeric_limits<size_t>::max() / 2
                                  ? std::max(next_vocab_size, target_capacity * 2)
                                  : next_vocab_size;
    }

    // Prepare append-only vectors geometrically before publishing a vocabulary
    // id. A later reserve may throw after an earlier vector already changed
    // capacity, so the catch path must settle that resident delta before
    // propagating the failure.
    try {
        owned_vocab_.reserve(target_capacity);
#ifdef BE_TEST
        if (g_fail_next_owned_term_reserve.exchange(false, std::memory_order_relaxed)) {
            throw std::bad_alloc();
        }
#endif
        slot_of_.reserve(target_capacity);
    } catch (...) {
        report_arena_delta();
        throw;
    }
    report_arena_delta();

    const uint32_t term_id = append_owned_vocab_term(std::move(term_str));
    static_assert(std::is_nothrow_copy_constructible_v<uint32_t>);

    const auto rollback_append = [&]() {
#ifdef BE_TEST
        g_vocab_materializations.fetch_sub(1, std::memory_order_relaxed);
#endif
        owned_vocab_heap_bytes_ -= string_heap_bytes(owned_vocab_.back());
        slot_of_.pop_back();
        owned_vocab_.pop_back();
        report_arena_delta();
    };

    // phmap allocates a growth table before publishing the prepared slot, and
    // constructing this trivial key cannot throw. If allocation fails, the old
    // table is intact and only the preceding vocabulary append needs rollback.
    const auto [it, inserted] = [&]() {
        try {
#ifdef BE_TEST
            if (g_fail_next_owned_term_emplace.exchange(false, std::memory_order_relaxed)) {
                throw std::bad_alloc();
            }
#endif
            return intern_.emplace_with_hash(term_hash, term_id);
        } catch (...) {
            rollback_append();
            throw;
        }
    }();
    if (!inserted) {
        rollback_append();
    }
    DCHECK(inserted);
    DCHECK_EQ(*it, term_id);
    return term_id;
}

uint32_t SpimiTermBuffer::append_owned_vocab_term(std::string&& term_str) {
    const uint32_t term_id = static_cast<uint32_t>(owned_vocab_.size());
    owned_vocab_.emplace_back(std::move(term_str));
    slot_of_.push_back(0); // vocab grows: new id starts with no live slot
    // G08: credit the stored string's heap payload (0 for SSO); the header is
    // charged via owned_vocab_.capacity().
    owned_vocab_heap_bytes_ += string_heap_bytes(owned_vocab_[term_id]);
#ifdef BE_TEST
    g_vocab_materializations.fetch_add(1, std::memory_order_relaxed);
#endif
    return term_id;
}

namespace {

// Decodes one varint from a pool chain cursor. The chain was written by
// encode_varint*, so the same LEB128 continuation-bit loop reconstructs it.
uint64_t decode_chain_varint(CompactPostingPool::Cursor* c) {
#ifdef BE_TEST
    g_compact_chain_varint_decodes.fetch_add(1, std::memory_order_relaxed);
#endif
    return c->read_varint();
}

Status write_sorted_chain(EncodedRunWriter* writer, CompactPostingPool::Cursor cursor, uint32_t end,
                          const EncodedRunTerm& record) {
    RETURN_IF_ERROR(writer->begin_term(record));
    RETURN_IF_ERROR(writer->begin_fragment(record.document_groups, record.tokens));
    while (true) {
        const auto payload = cursor.next_payload_span(end);
        if (payload.empty()) {
            break;
        }
        RETURN_IF_ERROR(writer->append_payload(payload));
    }
    RETURN_IF_ERROR(writer->end_fragment());
    return writer->end_term();
}

Status append_sorted_tokens(EncodedRunWriter* writer, SortedPostingTokens* sorted,
                            bool positioned) {
    bool first = true;
    uint32_t previous = 0;
    while (true) {
        uint32_t doc = 0;
        uint32_t position = 0;
        bool end = false;
        RETURN_IF_ERROR(sorted->next(&doc, &position, &end));
        if (end) {
            return Status::OK();
        }
        const bool new_document = first || doc != previous;
        if (positioned || new_document) {
            RETURN_IF_ERROR(writer->append_token(doc, position, new_document));
        }
        first = false;
        previous = doc;
    }
}

Status write_unsorted_chain(EncodedRunWriter* writer, CompactPostingPool::Cursor cursor,
                            EncodedRunTerm record, MemoryReporter* reporter) {
    SortedPostingTokens sorted(reporter);
    int64_t document = 0;
    for (uint64_t token = 0; token < record.tokens; ++token) {
        const uint64_t tagged = decode_chain_varint(&cursor);
        if ((tagged & 1U) != 0) {
            document += zigzag_decode(decode_chain_varint(&cursor));
        }
        RETURN_IF_ERROR(
                sorted.append(static_cast<uint32_t>(document), static_cast<uint32_t>(tagged >> 1)));
    }
    RETURN_IF_ERROR(sorted.finish());
    record.document_groups = sorted.document_count();
    record.tokens = record.has_positions ? sorted.token_count() : record.document_groups;
    RETURN_IF_ERROR(writer->begin_term(record));
    RETURN_IF_ERROR(writer->begin_fragment(record.document_groups, record.tokens));
    RETURN_IF_ERROR(append_sorted_tokens(writer, &sorted, record.has_positions));
    RETURN_IF_ERROR(writer->end_fragment());
    return writer->end_term();
}

} // namespace

// Decodes the compact tagged chain directly into caller-owned posting windows.
class SpimiTermBuffer::ArenaTermPostingSource final : public TermPostingSource {
public:
    ArenaTermPostingSource(const CompactPostingPool* pool, const Term& term)
            : shape_(term.shape), remaining_docs_(term.ndocs), remaining_tokens_(term.ntok) {
        if (term.head != kNoChain) {
            doc_cursor_.emplace(pool->cursor(term.head, term.w.cur));
        }
    }

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        if (out == nullptr || exhausted == nullptr || target_docs == 0 || !out->empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi arena source: invalid fill arguments");
        }
        const uint32_t count = std::min(target_docs, remaining_docs_);
        if (count == 0) {
            *exhausted = true;
            return Status::OK();
        }

        RETURN_IF_ERROR(fill_tagged(count, out));

        remaining_docs_ -= count;
        *exhausted = remaining_docs_ == 0;
        if (*exhausted) {
            DCHECK_EQ(remaining_tokens_, 0U);
            DCHECK(!pending_new_doc_);
        }
        return Status::OK();
    }

    bool exhausted() const { return remaining_docs_ == 0; }

private:
    Status fill_tagged(uint32_t count, TermPostingBuffer* out) {
        const bool has_positions = shape_ == PostingChainShape::kTaggedPositioned;
        MutableTermPostingSpan documents;
        RETURN_IF_ERROR(out->grow_uninitialized(count, /*has_freqs=*/true, 0, &documents));
        for (uint32_t i = 0; i < count; ++i) {
            uint64_t tagged = 0;
            if (pending_new_doc_) {
                tagged = pending_tagged_;
                pending_new_doc_ = false;
            } else {
                DCHECK_GT(remaining_tokens_, 0U);
                tagged = decode_chain_varint(&*doc_cursor_);
            }
            DCHECK_NE(tagged & 1U, 0U);
            absolute_docid_ += zigzag_decode(decode_chain_varint(&*doc_cursor_));
            documents.docids[i] = static_cast<uint32_t>(absolute_docid_);
            uint32_t frequency = 0;
            while (true) {
                if (has_positions) {
                    RETURN_IF_ERROR(out->append_position(static_cast<uint32_t>(tagged >> 1)));
                }
                ++frequency;
                --remaining_tokens_;
                if (remaining_tokens_ == 0) {
                    break;
                }
                tagged = decode_chain_varint(&*doc_cursor_);
                if ((tagged & 1U) != 0) {
                    pending_tagged_ = tagged;
                    pending_new_doc_ = true;
                    break;
                }
            }
            documents.freqs[i] = frequency;
        }
        return Status::OK();
    }

    PostingChainShape shape_;
    std::optional<CompactPostingPool::Cursor> doc_cursor_;
    uint32_t remaining_docs_ = 0;
    uint32_t remaining_tokens_ = 0;
    int64_t absolute_docid_ = 0;
    uint64_t pending_tagged_ = 0;
    bool pending_new_doc_ = false;
};

void SpimiTermBuffer::ensure_string_rank() const {
    const std::vector<std::string>& v = vocab();
    if (string_rank_.size() == v.size()) {
        return; // already built for the current append-only vocabulary
    }
    // Build the complete rank required by the first spill and by k-way merge
    // paths. Ordinary spills with a stale rank deliberately do not call here.
    std::vector<uint32_t> order(v.size());
    std::iota(order.begin(), order.end(), 0U);
    std::ranges::sort(order, [&](uint32_t a, uint32_t b) { return transient_term_less(a, b); });
    string_rank_.assign(v.size(), 0U);
    for (uint32_t rank = 0; rank < order.size(); ++rank) {
        string_rank_[order[rank]] = rank;
    }
#ifdef BE_TEST
    g_string_rank_rebuilds.fetch_add(1, std::memory_order_relaxed);
#endif
}

std::vector<uint32_t> SpimiTermBuffer::sorted_ids() const {
    std::vector<uint32_t> ids = touched_ids_;
    const std::vector<std::string>& v = vocab();
    if (string_rank_.empty()) {
        // Preserve the fixed-vocabulary fast path: the first spill pays once for
        // a complete rank, then every later spill is integer-only until vocab grows.
        ensure_string_rank();
    }
    if (string_rank_.size() == v.size()) {
        order_ids_by_dense_rank(&ids, string_rank_);
    } else {
        // Vocabulary grew after the last complete rank. A run needs only its touched
        // terms in lexical order; defer the O(vocab log vocab) rebuild until a k-way
        // merge needs rank lookups for arbitrary ids. Reserve the same persistent
        // rank capacity the old rebuild allocated so resident accounting and later
        // spill-trigger timing remain unchanged.
        string_rank_.reserve(v.size());
        std::ranges::sort(ids, [&](uint32_t a, uint32_t b) { return transient_term_less(a, b); });
    }
    return ids;
}

void SpimiTermBuffer::release_term(uint32_t term_id) {
    const uint32_t enc = slot_of_[term_id];
    DCHECK_NE(enc, 0U);
    const uint32_t slot = enc - 1;
    slots_[slot] = Term(); // free this term's arrays; the empty Term slot is reusable
    free_slots_.push_back(slot);
    slot_of_[term_id] = 0;
    --live_term_count_;
}

Status SpimiTermBuffer::drain_sorted_streamed(const StreamedTermConsumer& fn) {
    const std::vector<std::string>& v = vocab();
    ensure_string_rank();
    report_arena_delta();
    order_ids_by_dense_rank(&touched_ids_, string_rank_);
    intern_ = decltype(intern_)(0, OwnedVocabHash {.vocab = &owned_vocab_},
                                OwnedVocabEq {&owned_vocab_});
    std::vector<uint32_t>().swap(string_rank_);
    report_arena_delta();

    constexpr size_t kSlotIndexPrefetchDistance = 32;
    constexpr size_t kTermPrefetchDistance = 16;
    Status callback_status = Status::OK();
    for (size_t ordinal = 0; ordinal < touched_ids_.size(); ++ordinal) {
        if (ordinal + kSlotIndexPrefetchDistance < touched_ids_.size()) {
            const uint32_t future_id = touched_ids_[ordinal + kSlotIndexPrefetchDistance];
            __builtin_prefetch(slot_of_.data() + future_id);
        }
        if (ordinal + kTermPrefetchDistance < touched_ids_.size()) {
            const uint32_t future_id = touched_ids_[ordinal + kTermPrefetchDistance];
            const uint32_t future_enc = slot_of_[future_id];
            DCHECK_NE(future_enc, 0U);
            __builtin_prefetch(slots_.data() + future_enc - 1);
            __builtin_prefetch(v.data() + future_id);
        }
        const uint32_t id = touched_ids_[ordinal];
        const uint32_t enc = slot_of_[id];
        DCHECK_NE(enc, 0U);
        Term term = slots_[enc - 1];
        slots_[enc - 1] = Term();
        slot_of_[id] = 0;
        --live_term_count_;

        std::string output_term(v[id]);
        DORIS_CHECK(term.sorted);
        ArenaTermPostingSource source(&pool_, term);
        StreamedTermPostings postings {
                .term = std::move(output_term),
                .retain_positions = term.shape == PostingChainShape::kTaggedPositioned,
                .source = &source};
        callback_status = fn(std::move(postings));
        if (callback_status.ok() && !source.exhausted()) {
            callback_status = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi arena source: consumer returned before term exhaustion");
        }
        if (!callback_status.ok()) {
            break;
        }
    }

    pool_.reset();
    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    std::vector<uint32_t>().swap(touched_ids_);
    live_term_count_ = 0;
    std::vector<std::string>().swap(owned_vocab_);
    owned_vocab_heap_bytes_ = 0;
    report_arena_delta();
    return callback_status;
}

Status SpimiTermBuffer::drain_to_writer(EncodedRunWriter* w) {
    Status st = Status::OK();
    // Spill writes by term-id (no string IO). Iterate touched ids in vocab-string
    // order so each run is sorted; the k-way merge re-orders runs by the same key.
    for (uint32_t id : sorted_ids()) {
        const uint32_t enc = slot_of_[id];
        DCHECK_NE(enc, 0U);
        Term term = slots_[enc - 1];
        release_term(id);
        if (st.ok()) {
            const EncodedRunTerm record {
                    .term_id = id,
                    .has_positions = term.shape == PostingChainShape::kTaggedPositioned,
                    .document_groups = term.ndocs,
                    .tokens = term.ntok,
            };
            auto cursor = pool_.cursor(term.head, term.w.cur);
            st = term.sorted ? write_sorted_chain(w, cursor, term.w.cur, record)
                             : write_unsorted_chain(w, cursor, record, mem_reporter_);
        }
    }
    touched_ids_.clear();
    pool_.reset(); // all chains copied into the run; free the arena for the refill
    // The spill returns the arena to 0; slot_of_ keeps its capacity (survives
    // the spill). Report the arena-drop negative now so the gate-2 spill is balanced
    // immediately, not deferred to the next token.
    report_arena_delta();
    return st;
}

Status SpimiTermBuffer::spill_to_run() {
    // Spools retain earlier sealed runs without rewriting an ever-growing
    // prefix. Only the final contiguous merge passes copy their payload again.
    auto path_scratch = mem_reporter_ == nullptr ? MemoryReporter::Reservation()
                                                 : mem_reporter_->make_postings_reservation();
    if (mem_reporter_ != nullptr) {
        RETURN_IF_ERROR(path_scratch.set_bytes(3 * PATH_MAX));
    }
    const std::string dir = run_spool_path_.empty()
                                    ? resolve_temp_dir()
                                    : run_spool_path_.substr(0, run_spool_path_.rfind('/'));
    if (dir.size() + 64 >= PATH_MAX) {
        return Status::Error<ErrorCode::IO_ERROR, false>("spimi: temporary path is too long");
    }
    // Best-effort space pre-check: fail with a clear, early error rather than a
    // mid-write IoError that leaves a half-written run. Best-effort only (TOCTOU; on
    // tmpfs this reports RAM). The ARENA -- not full resident_bytes(), which since
    // G08 also charges vocabulary structures a run never contains -- is what the
    // run re-encodes, and its block slack makes it a conservative over-estimate of
    // the run's on-disk size.
    const uint64_t arena = pool_.arena_bytes();
    const uint64_t avail = temp_dir_available_bytes(dir);
    if (avail < arena) {
        return Status::Error<ErrorCode::IO_ERROR, false>(
                "spimi: insufficient temp space in '" + dir + "' to spill ~" +
                std::to_string(arena) + " B (~" + std::to_string(avail) +
                " B free); set SNII_TEMP_DIR/TMPDIR to a larger disk");
    }
    if (run_spool_path_.empty()) {
        run_spool_path_ = make_run_path(dir);
        if (mem_reporter_ != nullptr) {
            RETURN_IF_ERROR(run_path_reservation_.set_bytes(run_spool_path_.capacity() + 1));
        }
    }
    uint64_t end = 0;
    {
        EncodedRunWriter writer(mem_reporter_);
        RETURN_IF_ERROR(writer.open(run_spool_path_, /*append=*/true));
        RETURN_IF_ERROR(drain_to_writer(&writer));
        RETURN_IF_ERROR(writer.close());
        end = writer.file_offset();
    }
    // Publish the range only after its complete header/payload/seal is closed.
    // Both the fixed directory buffer and its spill cache share the same budget.
    std::array<uint8_t, 12> record {};
    std::memcpy(record.data(), &end, sizeof(end));
    const uint32_t checksum = crc32c(Slice(record.data(), sizeof(end)));
    std::memcpy(record.data() + sizeof(end), &checksum, sizeof(checksum));
    RETURN_IF_ERROR(run_ends_.append(record));
    ++run_count_;
    return Status::OK();
}

Status SpimiTermBuffer::prepare_run_merge() {
    if (!touched_ids_.empty()) {
        Status status = spill_to_run();
        if (!status.ok() && spill_status_.ok()) {
            spill_status_ = status;
        }
    }
    if (!spill_status_.ok()) {
        return spill_status_;
    }

    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    std::vector<uint32_t>().swap(touched_ids_);
    report_arena_delta();

    ensure_string_rank();
    report_arena_delta();
    intern_ = decltype(intern_)(0, OwnedVocabHash {.vocab = &owned_vocab_},
                                OwnedVocabEq {&owned_vocab_});
    report_arena_delta();
    return Status::OK();
}

void SpimiTermBuffer::finish_run_merge() {
    std::vector<std::string>().swap(owned_vocab_);
    owned_vocab_heap_bytes_ = 0;
    std::vector<uint32_t>().swap(string_rank_);
    report_arena_delta();
}

Status SpimiTermBuffer::merge_runs_streamed(const StreamedTermConsumer& fn) {
    RETURN_IF_ERROR(prepare_run_merge());
    Status status = merge_spooled_run_sources(run_spool_path_, &run_ends_, run_count_, vocab(),
                                              string_rank_, has_positions_, fn, mem_reporter_,
                                              max_run_files_);
    finish_run_merge();
    if (status.ok()) {
        cleanup_runs();
    }
    return status;
}

Status SpimiTermBuffer::for_each_term_sorted(const StreamedTermConsumer& fn) {
    // Single-drain contract: a second call would re-merge the (still-present) run
    // files and re-emit every term, or emit nothing in the in-memory path. Return
    // an error and emit NOTHING rather than produce a wrong second stream.
    if (drained_) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "spimi: already drained (single-drain contract)");
    }
    drained_ = true;
    // The compatibility API permits revisiting document ids within one arena.
    // Its stable external sort shares the bounded run path with ordinary spill.
    if (run_count_ == 0 && spill_status_.ok()) {
        for (uint32_t id : touched_ids_) {
            if (!slots_[slot_of_[id] - 1].sorted) {
                spill_status_ = spill_to_run();
                break;
            }
        }
    }
    if (run_count_ == 0 && spill_status_.ok()) {
        return drain_sorted_streamed(fn);
    }
    return merge_runs_streamed(fn);
}

std::vector<TermPostings> SpimiTermBuffer::finalize_sorted() {
    std::vector<TermPostings> out;
    out.reserve(touched_ids_.size());
    Status status = for_each_term_sorted([&out](StreamedTermPostings&& streamed) {
        TermPostings materialized;
        materialized.term = std::move(streamed.term);
        materialized.retain_positions = streamed.retain_positions;
        TermPostingBuffer buffer(nullptr);
        bool exhausted = false;
        while (!exhausted) {
            buffer.clear_reuse();
            RETURN_IF_ERROR(
                    streamed.source->fill(format::kAdaptiveWindowDocs, &buffer, &exhausted));
            materialized.docids.insert(materialized.docids.end(), buffer.docids().begin(),
                                       buffer.docids().end());
            materialized.freqs.insert(materialized.freqs.end(), buffer.freqs().begin(),
                                      buffer.freqs().end());
            const size_t position_begin = materialized.positions_flat.size();
            materialized.positions_flat.resize(position_begin + buffer.position_count());
            RETURN_IF_ERROR(buffer.read_positions(
                    0, std::span(materialized.positions_flat).subspan(position_begin)));
        }
        out.push_back(std::move(materialized));
        return Status::OK();
    });
    if (!status.ok() && spill_status_.ok()) {
        spill_status_ = status;
        std::vector<TermPostings>().swap(out);
    }
    return out;
}

void SpimiTermBuffer::cleanup_runs() {
    if (!run_spool_path_.empty()) {
        std::remove(run_spool_path_.c_str());
    }
    run_ends_.release();
    std::string().swap(run_spool_path_);
    run_path_reservation_.reset();
    run_count_ = 0;
}

} // namespace doris::snii::writer
