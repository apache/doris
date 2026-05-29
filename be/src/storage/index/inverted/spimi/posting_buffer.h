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
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "util/faststring.h"
#include "util/frame_of_reference_coding.h"

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

// Shared byte-slice pool (a faithful port of Lucene's ByteBlockPool). Per-term
// posting streams are stored as chains of geometrically-growing slices inside
// shared fixed-size blocks. Blocks, once allocated, are NEVER reallocated, so
// appending never copies existing bytes — eliminating the std::vector realloc
// high-water that dominated the old per-term-vector representation's peak. Each
// slice's last byte is a forwarding marker `(16 | level)`; when a writer reaches
// it, AllocSlice() grows to the next level and writes a 4-byte forwarding
// address into the old slice's tail. A reader follows those addresses.
class BytePool {
public:
    static constexpr uint32_t kBlockShift = 15;
    static constexpr uint32_t kBlockSize = 1u << kBlockShift; // 32 KiB
    static constexpr uint32_t kBlockMask = kBlockSize - 1;
    static constexpr int kLevelCount = 10;
    // Lucene's slice growth schedule. levelSize[0]=5 is the first slice; the
    // last byte of every slice is the forwarding marker, so usable data per
    // slice is levelSize-1 for the first hop and levelSize-4 after (3 bytes are
    // overwritten by the forwarding address on the predecessor).
    static constexpr int kNextLevel[kLevelCount] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
    static constexpr int kLevelSize[kLevelCount] = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};
    static constexpr uint32_t kFirstSize = 5;

    // Allocates a fresh level-0 slice; returns the global offset of its first
    // writable byte.
    uint32_t NewSlice();

    // Called when a writer hits the marker at global offset `upto` (the last
    // byte of a full slice). Allocates the next-level slice, copies the 3 tail
    // data bytes forward, writes the 4-byte forwarding address into the old
    // slice, and returns the global offset of the first writable byte of the
    // new slice.
    uint32_t AllocSlice(uint32_t upto);

    // Hot path: append one byte to the stream whose next write position is
    // `upto`; returns the new next-write position. Crosses slices transparently.
    [[gnu::always_inline]] inline uint32_t WriteByte(uint32_t upto, uint8_t b) {
        uint8_t* buf = _blocks[upto >> kBlockShift];
        uint32_t local = upto & kBlockMask;
        // `!= 0` correctly detects the slice-end marker (Lucene's invariant):
        // `upto` only ever points at an UNWRITTEN position here (it advances
        // monotonically, one byte per call, and written bytes are never
        // re-examined). Unwritten positions are zero (blocks are zero-init)
        // EXCEPT the slice's last byte, which NewSlice/AllocSlice set to
        // `16|level` (>=16). So a non-zero byte at `upto` is always the marker,
        // never a data byte — a data byte of value 0 is simply written into a
        // zero slot and `upto` advances past it.
        if (buf[local] != 0) [[unlikely]] { // reached the forwarding marker
            upto = AllocSlice(upto);
            buf = _blocks[upto >> kBlockShift];
            local = upto & kBlockMask;
        }
        buf[local] = b;
        return upto + 1;
    }

    // Hot path: append a variable-length uint32 (LEB128) at `upto`.
    [[gnu::always_inline]] inline uint32_t WriteVInt(uint32_t upto, uint32_t v) {
        while (v & ~0x7Fu) {
            upto = WriteByte(upto, static_cast<uint8_t>((v & 0x7F) | 0x80));
            v >>= 7;
        }
        return WriteByte(upto, static_cast<uint8_t>(v));
    }

    uint8_t ByteAt(uint32_t off) const { return _blocks[off >> kBlockShift][off & kBlockMask]; }

    // Raw block pointer (for the slice reader). Block `i` is a fixed kBlockSize
    // array that is never moved after allocation.
    const uint8_t* BlockAt(uint32_t i) const { return _blocks[i]; }

    // Resident bytes (allocated blocks). Capacity == size: blocks are fixed.
    size_t MemoryUsage() const { return _blocks.size() * kBlockSize; }

    // Drops all blocks but keeps one for reuse (mirrors the buffer's Reset
    // "keep capacity" contract). Cheap: clears the block vector.
    void Reset();

    bool empty() const { return _byte_upto == 0 && _blocks.empty(); }

private:
    // Ensures the current block has room for `size` contiguous bytes (slices
    // never span blocks); allocates a new block otherwise.
    void EnsureRoom(uint32_t size);

    std::vector<uint8_t*> _blocks; // owned 32 KiB blocks; never reallocated
    uint32_t _byte_upto = 0;       // global next-free offset for NEW slices

public:
    ~BytePool();
    BytePool() = default;
    BytePool(const BytePool&) = delete;
    BytePool& operator=(const BytePool&) = delete;
};

// Sequential reader over one term's slice chain spanning [start, end) (port of
// Lucene's ByteSliceReader). Follows the 4-byte forwarding address at each
// slice boundary. Reads exactly the bytes the writer produced — used at flush.
class ByteSliceReader {
public:
    ByteSliceReader(const BytePool& pool, uint32_t start, uint32_t end) : _pool(&pool), _end(end) {
        _buffer = pool.BlockAt(start >> BytePool::kBlockShift);
        _offset = (start >> BytePool::kBlockShift) << BytePool::kBlockShift;
        _upto = start & BytePool::kBlockMask;
        const uint32_t first = BytePool::kFirstSize;
        _limit = (start + first >= end) ? (end - _offset) : (_upto + first - 4);
    }

    [[gnu::always_inline]] uint8_t ReadByte() {
        if (_upto == _limit) {
            NextSlice();
        }
        return _buffer[_upto++];
    }

    uint32_t ReadVInt() {
        uint8_t b = ReadByte();
        uint32_t v = b & 0x7F;
        int shift = 7;
        while (b & 0x80) {
            b = ReadByte();
            v |= static_cast<uint32_t>(b & 0x7F) << shift;
            shift += 7;
        }
        return v;
    }

private:
    void NextSlice() {
        const uint32_t next = (static_cast<uint32_t>(_buffer[_limit]) << 24) |
                              (static_cast<uint32_t>(_buffer[_limit + 1]) << 16) |
                              (static_cast<uint32_t>(_buffer[_limit + 2]) << 8) |
                              static_cast<uint32_t>(_buffer[_limit + 3]);
        _level = BytePool::kNextLevel[_level];
        const uint32_t new_size = static_cast<uint32_t>(BytePool::kLevelSize[_level]);
        _offset = (next >> BytePool::kBlockShift) << BytePool::kBlockShift;
        _buffer = _pool->BlockAt(next >> BytePool::kBlockShift);
        _upto = next & BytePool::kBlockMask;
        _limit = (next + new_size >= _end) ? (_end - _offset) : (_upto + new_size - 4);
    }

    const BytePool* _pool;
    const uint8_t* _buffer;
    uint32_t _end;
    uint32_t _offset = 0;
    uint32_t _upto = 0;
    uint32_t _limit = 0;
    int _level = 0;
};

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

    // Compact-mode direct-emit view. After Sort(allow_direct_emit=true) on a
    // compact buffer whose streams arrived monotonically, the segment writer
    // can iterate the per-term arrays in text-sorted term order WITHOUT the
    // buffer first materializing the 12 B/occ `_records` vector (the dominant
    // finish-time peak). `CompactDirectEmitReady()` reports whether this view
    // is valid; if false the caller must fall back to `records()`.
    struct CompactTermRef {
        uint32_t text_ref; // arena offset of the term bytes
        uint32_t term_id;  // index into the per-term posting arrays
    };
    bool CompactDirectEmitReady() const { return _compact_direct_emit; }
    const std::vector<CompactTermRef>& SortedCompactTerms() const { return _sorted_compact_terms; }
    // Decodes term `term_id`'s block-compressed posting streams into the
    // per-occurrence (doc_id, position) arrays, in append (= (doc, position))
    // order. `docs` and `positions` are resized to the term's occurrence
    // count. Valid only while CompactDirectEmitReady().
    void DecodeCompactTerm(uint32_t term_id, std::vector<uint32_t>& docs,
                           std::vector<uint32_t>& positions) const;

    // Streaming-emit support: a term's two slice-chain spans plus counts, so
    // the segment writer can drive `ByteSliceReader`s occurrence-by-occurrence
    // (buffering only one doc's positions) instead of materializing full
    // per-term docs/positions vectors — the dominant finish-phase peak on a
    // Zipfian corpus where one term holds a large share of all occurrences.
    struct CompactTermStreams {
        uint32_t doc_start;
        uint32_t doc_end;
        uint32_t pos_start;
        uint32_t pos_end;
        uint32_t occ_count;
        uint32_t doc_count;               // distinct docs == doc_freq for StartTerm
        uint32_t prefix_count;            // VInt-prefix occurrences in the slice chains
        const uint8_t* for_doc = nullptr; // FOR-packed doc-delta tail (null if not graduated)
        size_t for_doc_len = 0;
        const uint8_t* for_pos = nullptr; // FOR-packed position tail
        size_t for_pos_len = 0;
    };
    CompactTermStreams CompactStreamsFor(uint32_t term_id) const {
        const auto& s = _term_states[term_id];
        CompactTermStreams r {s.doc_start, s.doc_upto,  s.pos_start, s.pos_upto,
                              s.occ_count, s.doc_count, s.occ_count};
        if (s.for_index != kNoForIndex) {
            const ForTermState& f = *_for_states[s.for_index];
            r.prefix_count = kForThreshold;
            r.for_doc = f.doc_buf.data();
            r.for_doc_len = f.doc_buf.size();
            r.for_pos = f.pos_buf.data();
            r.for_pos_len = f.pos_buf.size();
        }
        return r;
    }
    const BytePool& Pool() const { return _pool; }

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
    //
    // `allow_direct_emit` (default true): when the buffer is in compact mode
    // and its streams arrived monotonically, prepare the compact direct-emit
    // view (SortedCompactTerms) and skip materializing `_records`. Pass false
    // to force `_records` materialization (e.g. when the caller still needs
    // `records()` for norms/doc-length computation).
    void Sort(bool allow_direct_emit = true);

private:
    // Inline hot-loop body run once per token in compact mode. Streams the
    // (doc_id, position) occurrence as two VInts (delta-coded doc id, plain
    // position) straight into the term's two slice chains in the shared `_pool`.
    // No per-term pending block and no SIMD batch: the per-token cost is a
    // couple of VInt byte writes into the pool (each a cursor store plus a
    // predictable slice-boundary branch), and there is no 512 B/term scratch
    // and no std::vector realloc high-water. `_compact_streams_sorted` tracks
    // whether the stream is globally (doc, position)-monotonic so emit can skip
    // the per-term re-sort.
    [[gnu::always_inline]] inline void EncodeOccurrenceToStreamInline(uint32_t term_id,
                                                                      uint32_t doc_id,
                                                                      uint32_t position) {
        auto& state = _term_states[term_id];
        uint32_t doc_delta;
        if (state.any) [[likely]] {
            if (doc_id < state.last_doc) [[unlikely]] {
                _compact_streams_sorted = false;
            } else if (doc_id == state.last_doc && position < state.last_pos) [[unlikely]] {
                _compact_streams_sorted = false;
            }
            if (doc_id != state.last_doc) {
                ++state.doc_count; // new doc (exact for the monotonic direct-emit path)
            }
            doc_delta = doc_id - state.last_doc; // modular delta; round-trips any order
        } else {
            state.doc_count = 1;
            // First occurrence: open the term's two slice chains lazily.
            state.doc_start = state.doc_upto = _pool.NewSlice();
            state.pos_start = state.pos_upto = _pool.NewSlice();
            doc_delta = doc_id; // delta from implicit 0
        }
        if (state.for_index != kNoForIndex) {
            // Graduated: stage the (doc-delta, position) tail and feed the SIMD
            // FOR encoder one frame (kForBatch) at a time. Deltas are
            // per-occurrence (continuous across the VInt-prefix→FOR-suffix
            // boundary), so decode just keeps threading `prev`.
            ForTermState& f = *_for_states[state.for_index];
            f.doc_pend[f.pend_n] = doc_delta;
            f.pos_pend[f.pend_n] = position;
            if (++f.pend_n == kForBatch) [[unlikely]] {
                f.doc_enc.put_batch(f.doc_pend, kForBatch);
                f.pos_enc.put_batch(f.pos_pend, kForBatch);
                f.pend_n = 0;
            }
        } else {
            // VInt prefix: delta-coded doc id + plain position, 1-3 bytes each
            // straight into the shared slice pool (no per-term scratch).
            state.doc_upto = _pool.WriteVInt(state.doc_upto, doc_delta);
            state.pos_upto = _pool.WriteVInt(state.pos_upto, position);
        }
        state.last_doc = doc_id;
        state.last_pos = position;
        state.any = true;
        ++state.occ_count;
        if (state.occ_count == kForThreshold && state.for_index == kNoForIndex) [[unlikely]] {
            // Graduate: this term is high-DF; pack its remaining occurrences.
            state.for_index = static_cast<uint32_t>(_for_states.size());
            _for_states.push_back(std::make_unique<ForTermState>());
        }
    }

    // Inline hot path of Intern(): checks the open-addressing
    // slot table for `term` and returns its existing arena
    // offset on a hit. Compiler inlines into `Append()` at the
    // call site, eliminating the function-call boundary that
    // showed up as 16-22 % of V4 wall-clock on the flame graphs.
    //
    // Returns kEmpty if the term is NOT yet interned — caller
    // falls through to `InternCold()` (out-of-line) to allocate
    // a fresh arena slot, claim the table slot, and possibly
    // grow the slot table. Splits hot match from cold insert so
    // the hot path stays small enough to inline cleanly.
    [[gnu::always_inline]] inline uint32_t InternHot(std::string_view term) {
        if (_slots.empty()) [[unlikely]] {
            return kEmpty;
        }
        const size_t mask = _slots.size() - 1;
        size_t slot = HashTerm(term) & mask;
        // Bounded probe: most slots are within 1-2 hops at
        // realistic load factors. After this many hops bail to
        // cold path which retries from scratch with no probe
        // bound. The bound itself doesn't change correctness.
        for (int hops = 0; hops < 8; ++hops) {
            const uint32_t entry = _slots[slot];
            if (entry == kEmpty) {
                return kEmpty;
            }
            if (ArenaTermEqualsInline(entry, term)) {
                _last_intern_slot = slot;
                return entry;
            }
            slot = (slot + 1) & mask;
        }
        return kEmpty; // fall through to cold path
    }

    // Inline hash and arena helpers — moved from .cpp so they
    // can be inlined into `InternHot` at the caller's TU.
    //
    // HashTerm is public: it is a pure, const, side-effect-free hash that was
    // public in the pre-slice-pool layout and is used as a unit-test seam
    // (verifying the per-instance hash-seed behaviour). Keep it accessible.
public:
    [[gnu::always_inline]] inline uint64_t HashTerm(std::string_view term) const {
        constexpr uint64_t kPrime = 1099511628211ULL;
        uint64_t h = _hash_seed;
        for (unsigned char c : term) {
            h ^= static_cast<uint64_t>(c);
            h *= kPrime;
        }
        return h;
    }

private:
    [[gnu::always_inline]] inline bool ArenaTermEqualsInline(uint32_t text_ref,
                                                             std::string_view term) const {
        const uint8_t* base = _arena.data() + text_ref;
        const uint32_t len =
                static_cast<uint32_t>(base[0]) | (static_cast<uint32_t>(base[1]) << 8) |
                (static_cast<uint32_t>(base[2]) << 16) | (static_cast<uint32_t>(base[3]) << 24);
        if (len != term.size()) {
            return false;
        }
        return std::memcmp(base + 4, term.data(), len) == 0;
    }

    // Interns `term`, returning its arena offset (the value stored in
    // SpimiRecord::text_ref). Out-of-line full path; hot callers
    // should use `InternHot()` and only call this on cache miss.
    uint32_t Intern(std::string_view term);
    // Cold insert path: assumes InternHot returned kEmpty and the
    // term needs a fresh arena slot. Doesn't re-probe; just
    // re-hashes and inserts. Slow path only, OK to be out-of-line.
    uint32_t InternCold(std::string_view term);

    // Grows the intern map to the next power-of-two capacity, rehashing in
    // place from the arena.
    void GrowSlots(size_t new_capacity);

    // `HashTerm` and `ArenaTermEqualsInline` are defined inline
    // in the public section above (after `InternHot`).

    // Reads the term text at arena offset `text_ref` (length + bytes).
    std::string_view ArenaTermAt(uint32_t text_ref) const;

    // Compares the term at arena offset `text_ref` to `term`.
    // Out-of-line variant used by code paths that don't need
    // inlining (e.g. test helpers). Hot path uses
    // `ArenaTermEqualsInline` in the public section above.
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

    // Per-term compact posting state (active when `_compact_mode` is true).
    //
    // Occurrences are streamed as two VInt byte chains in the shared `_pool`
    // (slice chains): `doc` holds delta-coded doc ids (tiny deltas; 0 within the
    // same doc) and `pos` holds plain-coded positions (small within-doc
    // offsets). `*_start` is the global offset of the chain's first byte (for
    // the reader); `*_upto` is the next write position (for the writer). No
    // per-term std::vector and no fixed pending block: the only per-term cost is
    // this ~36 B struct plus the actual VInt bytes in the pool.
    //
    // Emit decodes the chains back into the exact (doc, position) sequence that
    // was appended and drives the SAME FreqProxEncoder calls as the records
    // path, so the on-disk .frq/.prx bytes are unchanged. VInt delta coding
    // round-trips any uint32 sequence (modular deltas), so a non-monotonic
    // stream is decoded exactly and `_compact_streams_sorted` tells emit to
    // re-sort it.
    // Hybrid graduation threshold. A term's first kForThreshold occurrences are
    // byte-aligned VInt in the slice pool (cheap, no per-term scratch — right
    // for the Zipfian low-DF tail). Once a term crosses it, the remaining
    // occurrences are SIMD frame-of-reference (FOR) bit-packed sub-byte via
    // ForEncoder — high-DF terms hold the bulk of occurrences, so packing their
    // tail beats the ~2 B/occ byte floor, while the bounded VInt prefix and the
    // per-FOR-term frame scratch stay small (few terms graduate).
    static constexpr uint32_t kForThreshold = 512;
    static constexpr uint32_t kNoForIndex = 0xFFFFFFFFU;

    struct TermPostingState {
        uint32_t doc_start = 0; // global pool offset: first byte of doc chain
        uint32_t doc_upto = 0;  // global pool offset: next doc-chain write
        uint32_t pos_start = 0; // global pool offset: first byte of pos chain
        uint32_t pos_upto = 0;  // global pool offset: next pos-chain write
        uint32_t occ_count = 0; // total occurrences (sizes the decode)
        uint32_t doc_count = 0; // doc-id transitions; == distinct docs (doc_freq) for the
                                // monotonic direct-emit path (its only consumer). On
                                // non-monotonic streams it may over-count, but those take the
                                // records fallback which recomputes doc_freq, so it's unused there.
        uint32_t text_ref = 0;  // arena offset of the term bytes
        uint32_t last_doc = 0;  // last appended doc id (delta seed + monotonic check)
        uint32_t last_pos = 0;  // last appended position (monotonic check)
        uint32_t for_index = kNoForIndex; // index into _for_states once graduated
        bool any = false;                 // at least one occurrence appended
    };

    // Per-graduated-term FOR-packed tail (heap-stable: the ForEncoders hold
    // faststring* into the same node, which must not dangle when _for_states
    // grows — hence unique_ptr nodes, never moved).
    static constexpr uint32_t kForBatch = 128; // == ForEncoder frame size
    struct ForTermState {
        faststring doc_buf;
        faststring pos_buf;
        ForEncoder<uint32_t> doc_enc;
        ForEncoder<uint32_t> pos_enc;
        // Small staging buffers so occurrences are fed to ForEncoder in
        // frame-sized put_batch() calls instead of one out-of-line put() per
        // occurrence (the dominant FOR-path CPU cost on the flame graph).
        uint32_t doc_pend[kForBatch];
        uint32_t pos_pend[kForBatch];
        uint32_t pend_n = 0;
        ForTermState() : doc_enc(&doc_buf), pos_enc(&pos_buf) {}
    };

    // Seals the compact streams (terminal). VInt chains are self-delimiting and
    // need no trailing padding, so this only latches `_finalized`. Idempotent.
    void FinalizeBlocks();
    // Decodes a term's streams into `_records` (appending), tagging each
    // occurrence with `text_ref`. Used to materialize records when the
    // direct-emit fast path is not taken.
    void DecodeTermToRecords(uint32_t term_id);

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
    // Shared slice pool backing every term's doc/pos VInt prefix chains. Never
    // reallocates handed-out blocks, so it carries no per-term realloc
    // high-water — the dominant component of the old representation's peak.
    BytePool _pool;
    // FOR-packed tails for graduated (high-DF) terms; indexed by
    // TermPostingState::for_index. unique_ptr nodes keep the ForEncoders'
    // faststring pointers stable across growth.
    std::vector<std::unique_ptr<ForTermState>> _for_states;
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

    // Compact direct-emit view, populated by Sort(allow_direct_emit=true) on a
    // monotonic compact buffer. When `_compact_direct_emit` is true, the
    // per-term arrays (`_term_states`) are kept alive and `_sorted_compact_terms`
    // lists every term once in text-sorted order, so the segment writer can
    // stream postings straight from the arrays without the `_records`
    // materialization. See CompactDirectEmitReady().
    bool _compact_direct_emit = false;
    std::vector<CompactTermRef> _sorted_compact_terms;

    // Set by FinalizeBlocks() (from Sort()): the compact streams are flushed
    // and padded, so the buffer is frozen. Append() asserts this is false.
    // Cleared by Reset() to allow buffer reuse across segments.
    bool _finalized = false;
};

} // namespace doris::segment_v2::inverted_index::spimi
