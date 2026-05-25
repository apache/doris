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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

// Hard upper bound on a single term's byte length. Lucene's term comparator
// works on UTF-16 code units (capped to 32 KiB by the 2.x format's VInt term-
// length prefix), so a 64 KiB UTF-8 cap is comfortably above what any
// well-formed analyser produces and well below the uint32 truncation cliff
// in `Intern()`. Documents with longer single tokens are pathological;
// rejecting them here is preferable to silently corrupting `text_ref`.
inline constexpr size_t kMaxTermBytes = 64 * 1024;

// Hard upper bound on the term arena's resident bytes. The arena is indexed
// by `uint32_t text_ref`, so once it crosses 4 GiB the offsets wrap and
// readers decode attacker-controlled bytes as term lengths (CVE-style
// silent corruption). We cap well below 2^32 to leave headroom for the
// 4-byte length prefix on the final term insert.
inline constexpr size_t kMaxArenaBytes = (size_t {1} << 31); // 2 GiB

// Hard upper bound on the record vector's element count. 12 B/record at this
// cap is ~24 GiB resident — well past any real workload but below SIZE_MAX/12.
// Keeps `_records.size()` safely castable to `int32_t` for downstream Lucene
// 2.x format fields that the writer emits as int32.
inline constexpr size_t kMaxRecordCount = static_cast<size_t>(std::numeric_limits<int32_t>::max());

// One record per token occurrence in the SPIMI accumulator. Records are
// fixed-size (12 bytes) and stored in a flat append-only vector; the term text
// for `text_ref` is stored once in the buffer's arena.
struct SpimiRecord {
    uint32_t text_ref; // byte offset of the term entry in the arena
    uint32_t doc_id;
    uint32_t position;
};
static_assert(sizeof(SpimiRecord) == 12, "SpimiRecord must be 12 bytes");

// Append-only accumulator for the SPIMI ("Single-Pass In-Memory Indexing")
// full-text writer. Each Append() records one term occurrence as a SpimiRecord
// and interns the term in an internal byte arena so each distinct term is
// stored exactly once. There is no per-term hash of Posting objects and no
// freq/prox byte-pool slices: the only buffers held in memory are the record
// list, the term arena, and a lightweight open-addressed intern map.
//
// Memory model (per distinct term + per occurrence):
//   record (always)              : 12 bytes/occurrence
//   arena (term text once)       : sizeof(uint32_t) + term_len bytes/term
//   intern map (~0.75 load)      : sizeof(uint32_t) * (term_count / 0.75)
//                                = ~5.3 bytes/term amortised
//
// Thread-safety: not thread-safe. Each writer owns one instance.
class SpimiPostingBuffer {
public:
    // Per-instance limits. Default to the production constants above; tests
    // pass much smaller values so they can exercise saturation boundaries
    // without allocating 2 GiB. Sized so it can be passed by value cheaply.
    struct Limits {
        size_t max_term_bytes = kMaxTermBytes;
        size_t max_arena_bytes = kMaxArenaBytes;
        size_t max_record_count = kMaxRecordCount;
    };
    static_assert(sizeof(Limits) <= 32, "Limits must stay cheap-by-value");

    SpimiPostingBuffer();
    // Test seam: deterministic hash seed (default-constructed picks a random
    // per-instance seed at runtime to defend against hash-collision DoS from
    // attacker-controlled term bytes).
    explicit SpimiPostingBuffer(uint64_t hash_seed);
    // Test seam: deterministic seed + shrunken limits. Used by tests that
    // need to exercise the arena/record-cap saturation paths without a
    // 2 GiB allocation. NOT for production callers.
    SpimiPostingBuffer(uint64_t hash_seed, Limits limits);

    SpimiPostingBuffer(const SpimiPostingBuffer&) = delete;
    SpimiPostingBuffer& operator=(const SpimiPostingBuffer&) = delete;

    // Records one term occurrence at (doc_id, position). The call becomes a
    // no-op once the buffer is `Saturated()` — i.e. a previous Append would
    // have pushed `term.size() > kMaxTermBytes`, the arena past
    // `kMaxArenaBytes`, or the record vector past `kMaxRecordCount`. Reaching
    // saturation logs once and flips the buffer into a frozen state; callers
    // can poll `Saturated()` at `Finish()` time to detect partial segments.
    //
    // The void signature is deliberate: SPIMI is a shadow accumulator, and
    // halting the primary CLucene write path because one oversized adversarial
    // value would have OOM'd the SPIMI buffer is the wrong tradeoff. Freezing
    // the shadow is what callers want.
    //
    // `term` must be the canonical UTF-8 byte representation; identical byte
    // sequences are de-duplicated into a single arena entry.
    void Append(std::string_view term, uint32_t doc_id, uint32_t position);

    // True once a size limit has been crossed and subsequent Append calls
    // have been (or will be) dropped. Latches; cleared only by Reset().
    bool Saturated() const { return _saturated; }

    // Number of records appended so far.
    size_t RecordCount() const { return _records.size(); }

    // Number of distinct terms interned so far.
    size_t TermCount() const { return _term_count; }

    // Approximate in-RAM accumulator memory: records + arena + intern slots.
    // Counts the *capacity* of each container (the resident allocation).
    size_t MemoryUsage() const;

    // Records (read-only access for sort/iteration).
    const std::vector<SpimiRecord>& records() const { return _records; }

    // Resolves the term text referenced by `text_ref`. The returned view is
    // valid as long as the buffer is not modified.
    std::string_view TermAt(uint32_t text_ref) const;

    // Convenience overload.
    std::string_view TermFor(const SpimiRecord& rec) const { return TermAt(rec.text_ref); }

    // Clears all buffers but keeps their allocated capacity for reuse.
    void Reset();

    // Sorts records in place by (term bytes, doc_id, position).
    // Term order is byte-wise on the UTF-8 representation, which equals
    // code-point order for all of Unicode — the same order used by the
    // existing CLucene writer's term dictionary, so the SPIMI segment
    // emitter can stream-iterate sorted records directly into the term
    // dictionary. Sort is stable: equal keys keep insertion order.
    void Sort();

private:
    // Interns `term`, returning its arena offset (the value stored in
    // SpimiRecord::text_ref). On first sight the term is appended to the
    // arena as a 4-byte length followed by the term bytes; otherwise the
    // existing offset is returned.
    uint32_t Intern(std::string_view term);

    // Grows the intern map to the next power-of-two capacity, rehashing in
    // place from the arena.
    void GrowSlots(size_t new_capacity);

    // Keyed FNV-1a 64-bit byte hash; the per-instance seed is the initial
    // accumulator. Default-constructed buffers pick a random seed from a
    // per-process salt + per-instance counter at construction time, which
    // defeats attacker pre-computation of colliding terms (the original
    // un-keyed FNV-1a could be driven into O(n²) probe chains by
    // adversarial documents). The explicit-seed constructor is for tests.
    uint64_t HashTerm(std::string_view term) const;

    // Reads the term text at arena offset `text_ref` (length + bytes).
    std::string_view ArenaTermAt(uint32_t text_ref) const;

    // Compares the term at arena offset `text_ref` to `term`.
    bool ArenaTermEquals(uint32_t text_ref, std::string_view term) const;

    // Sentinel for an empty intern slot. Since text_ref is an offset into the
    // arena (which never reaches 4 GB in practice), 0xFFFFFFFF cannot collide
    // with a real offset.
    static constexpr uint32_t kEmpty = 0xFFFFFFFFU;

    // Initial intern map capacity (power of two); grown by doubling.
    static constexpr size_t kInitialSlots = 16;

    // Marks the buffer as full; further Appends are no-ops. See `Saturated()`.
    // `reason` names the size class that overflowed; `term_prefix` records up
    // to the first 32 bytes (hex-escaped) of the offending term so operators
    // can identify the upstream input that tripped the latch.
    void Saturate(const char* reason, std::string_view term_prefix);

    // Per-term compact posting state. Used when `_compact_mode` is
    // true to hold occurrences in delta-encoded varint streams keyed
    // by a dense term id. The flat `_records` vector is freed at
    // compaction time. Each occurrence costs ~2-3 bytes in the
    // stream versus 12 B in `_records`, which is what lets SPIMI
    // stay below CLucene's memory footprint on low-cardinality
    // (repetitive) workloads without losing positions or any other
    // index data — see SPIMI_DESIGN.md § 4.5 for the trade-off.
    struct TermPostingState {
        uint32_t last_doc = 0;
        uint32_t last_pos_in_doc = 0;
        bool first = true;
        std::vector<uint8_t> stream; // varint-encoded occurrences
    };

    // Records-mode → compact-mode trigger thresholds. Compaction
    // runs when records count crosses `kCompactCheckEvery` AND the
    // average occurrences per distinct term exceed `kCompactAvgOcc`.
    // Tuned so:
    //   - mostly_unique / all_unique workloads (avg ~1 occ/term)
    //     never trigger — keeps the existing fast path.
    //   - repetitive workloads (vocab << records) trigger after the
    //     first ~512 records — keeps the compact peak well below
    //     CLucene's per-term posting overhead.
    static constexpr size_t kCompactCheckEvery = 512;
    static constexpr size_t kCompactAvgOcc = 32;

    // Returns true after `MaybeCompact()` has migrated records into
    // `_term_streams` and freed `_records`. While true, Append
    // writes to `_term_streams` only; `records()` returns the empty
    // vector until `Sort()` materialises records from streams.
    bool _compact_mode = false;

    // Compact-mode invariant: all `EncodeOccurrenceToStream` calls
    // so far produced monotonic (doc, pos) — no "reset sentinel"
    // path taken. Holds for the common fulltext case where
    // upstream rows arrive in row-id order and tokens within a row
    // in position order. When true, `Sort()` can skip the global
    // `stable_sort` over `_records` and emit per-term streams
    // directly in text-sorted-term order. Saves ~14 ms on the
    // 12K-occurrence repetitive bench.
    bool _compact_streams_sorted = true;

    // Migrates `_records` into `_term_streams` (one stream per
    // distinct term, delta-encoded), then clears + shrinks
    // `_records`. Called from `Append` after every
    // `kCompactCheckEvery` appends when the cardinality heuristic
    // says we'd save memory. Idempotent and one-way: once
    // `_compact_mode` is true, this is never called again on the
    // same buffer.
    void MaybeCompact();

    // Looks up (or assigns) the dense term id for `text_ref`. Term
    // ids index into `_term_streams` / `_term_states` and are
    // assigned in first-Append order per term.
    uint32_t GetOrAssignTermId(uint32_t text_ref);

    // Appends one delta-encoded (doc, pos) pair into the term's
    // posting stream. Handles the first-occurrence, same-doc, and
    // new-doc encodings uniformly.
    void EncodeOccurrenceToStream(uint32_t term_id, uint32_t doc_id, uint32_t position);

    uint64_t _hash_seed = 0;
    Limits _limits {};
    std::vector<SpimiRecord> _records;
    std::vector<uint8_t> _arena;
    std::vector<uint32_t> _slots;
    size_t _term_count = 0;
    bool _saturated = false;

    // Compact-mode storage. Empty (and zero-capacity) while
    // `_compact_mode` is false. The map's key (text_ref) is an
    // arena offset whose value class is fully under our control:
    // arena byte layout is bounded by `kMaxArenaBytes`, term bytes
    // are bounded by `kMaxTermBytes`, and the only attacker
    // influence on arena offsets is via *term lengths*. With at
    // most O(terms) distinct keys (bounded by Saturate()), and the
    // map only used in the writer-side compact path (NOT touched
    // when parsing untrusted on-disk bytes), the keyed-hash
    // requirement that `_slots` enforces does not extend to this
    // map. Documented for the next reader.
    std::vector<TermPostingState> _term_states;
    // Compact-mode optimization (post-flamegraph): parallel array
    // to `_slots`, holding the term_id per slot for compact-mode
    // Append's O(1) lookup. Replaces the per-Append
    // `unordered_map::find` hot path (the flame graph hottest non-
    // analyzer frame was `_text_ref_to_term_id.find()` from
    // `GetOrAssignTermId`). With this, compact mode Append does:
    //   Intern → text_ref      (already touches the slot)
    //   _slot_term_ids[slot]   → term_id   (no second hash)
    // Same size + grow behaviour as `_slots`. `kInvalidTermId`
    // marks "no term_id assigned to this slot yet"; assigned on
    // either first Intern after compact-mode is active, or during
    // the MaybeCompact migration when a slot's existing text_ref
    // is being indexed for the first time.
    std::vector<uint32_t> _slot_term_ids;
    static constexpr uint32_t kInvalidTermId = 0xFFFFFFFFU;
    // Set by `Intern` to the slot index it ended up at. Compact-
    // mode Append reads this to grab term_id from
    // `_slot_term_ids[_last_intern_slot]`. Side-channel return
    // value to keep Intern's main return type unchanged for the
    // flat-mode callers.
    size_t _last_intern_slot = 0;
    std::unordered_map<uint32_t, uint32_t> _text_ref_to_term_id;
    // (P48 single-entry text_ref → term_id cache removed in P49 —
    // the slot-parallel `_slot_term_ids` array makes the lookup
    // O(1) without any cache. Keeping `_cached_text_ref` as a
    // legacy member would only burn a cache line.)
    uint32_t _cached_text_ref = static_cast<uint32_t>(-1);
    size_t _total_occurrences = 0; // running total for emit/test inspection
};

} // namespace doris::segment_v2::inverted_index::spimi
