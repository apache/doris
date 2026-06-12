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
#include <deque>
#include <limits>
#include <memory>
#include <string_view>
#include <vector>

#include "gtest/gtest_prod.h"
#include "storage/index/inverted/spimi/byte_output.h"

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

    // 64-bit LEB128. Used for the per-doc freq-chain docCode (`doc_delta << 1 |
    // freq_flag`): a full uint32 doc-delta shifted left by one needs 33 bits, so
    // it must NOT be truncated to uint32 (that loses the top bit and corrupts
    // backward — i.e. non-monotonic — doc transitions, which the records-fallback
    // path must round-trip). For the common small monotonic delta the encoded
    // bytes are byte-identical to WriteVInt; only large/backward deltas spend
    // the extra byte.
    [[gnu::always_inline]] inline uint32_t WriteVInt64(uint32_t upto, uint64_t v) {
        while (v & ~0x7FULL) {
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

    // 64-bit LEB128 read — pairs with BytePool::WriteVInt64 for the freq-chain
    // docCode (see WriteVInt64). The caller recovers the doc-delta as
    // `code >> 1` and the freq flag as `code & 1`.
    uint64_t ReadVInt64() {
        uint8_t b = ReadByte();
        uint64_t v = b & 0x7FULL;
        int shift = 7;
        while (b & 0x80) {
            b = ReadByte();
            v |= static_cast<uint64_t>(b & 0x7F) << shift;
            shift += 7;
        }
        return v;
    }

    // Bulk-appends every remaining chain byte to `out` — one memcpy per slice
    // run instead of a bounds check per byte. Used by the slim direct-copy
    // emit, where the chain bytes ARE the on-disk block bytes and decoding
    // them VInt-by-VInt only to re-encode the same values would be wasted
    // work. Traversal is identical to ReadByte's: copy up to the slice limit,
    // then follow the 4-byte forwarding address.
    void AppendRemainingTo(std::vector<uint8_t>& out) {
        while (true) {
            if (_limit > _upto) {
                out.insert(out.end(), _buffer + _upto, _buffer + _limit);
                _upto = _limit;
            }
            if (_offset + _upto >= _end) {
                return;
            }
            NextSlice();
        }
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
    // Grant the hash unit tests access to the private `HashTerm` so they can
    // assert per-instance keying / explicit-seed determinism without widening
    // the public surface.
    FRIEND_TEST(SpimiPostingBufferTest, KeyedHashDiffersAcrossDefaultInstances);
    FRIEND_TEST(SpimiPostingBufferTest, ExplicitSeedHashIsDeterministic);

public:
    // Per-instance limits. Default to the production constants above; tests
    // pass much smaller values so they can exercise saturation boundaries
    // without allocating 2 GiB. Sized so it can be passed by value cheaply.
    // Soft memory budget for spill-to-disk. When the buffer's resident
    // bytes cross this threshold, `ShouldFlush()` returns true and the
    // caller (SpillManager / InvertedIndexColumnWriter) should flush the
    // buffer to a temporary segment on disk and call Reset() to reclaim
    // memory. Unlike `Saturated()` (a hard limit that freezes the buffer),
    // `ShouldFlush()` is advisory: Append() continues to work after the
    // budget is exceeded, but each additional record increases peak RAM.
    // Default 128 MiB: with spill-to-disk (spilled bytes are freed, no resident
    // accumulation) a lower budget sharply cuts the large-doc write peak
    // (wiki 546→259 MB, below V2's 288) for only +0.75% .idx (more spills → a
    // slightly less optimal k-way re-encode). Short docs never cross it and are
    // unaffected.
    static constexpr size_t kDefaultMemoryBudget = size_t {128} << 20;

    struct Limits {
        size_t max_term_bytes = kMaxTermBytes;
        size_t max_arena_bytes = kMaxArenaBytes;
        size_t max_record_count = kMaxRecordCount;
        size_t memory_budget_bytes = kDefaultMemoryBudget;
    };
    static_assert(sizeof(Limits) <= 32, "Limits must stay cheap-by-value");

    // `omit_tfap` (default false): when true the field omits term-frequencies
    // and positions (DOCS_ONLY / support_phrase=false). In that mode the
    // per-occurrence absolute-position VInt is NOT written into the prox slice
    // chain and the prox chain is never allocated — positions were always
    // discarded at emit anyway (FreqProxEncoder::AddPosition no-ops, no .prx is
    // written), so this is the pure-waste position work removed end to end. The
    // per-doc freq/doc-delta chain is UNCHANGED (it drives StartDoc and the
    // on-disk .frq), so the emitted .idx stays byte-identical.
    explicit SpimiPostingBuffer(bool omit_tfap = false);
    // Test seam: deterministic hash seed (default-constructed picks a random
    // per-instance seed at runtime to defend against hash-collision DoS from
    // attacker-controlled term bytes).
    explicit SpimiPostingBuffer(uint64_t hash_seed, bool omit_tfap = false);
    // Test seam: deterministic seed + shrunken limits. Used by tests that
    // need to exercise the arena/record-cap saturation paths without a
    // 2 GiB allocation. NOT for production callers.
    SpimiPostingBuffer(uint64_t hash_seed, Limits limits, bool omit_tfap = false);

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

    // True when the buffer's resident memory has crossed the soft budget
    // (`Limits::memory_budget_bytes`). The caller should flush the buffer
    // to a spill segment and call Reset() to reclaim memory. Unlike
    // `Saturated()`, this is non-fatal: Append() keeps working, but peak
    // RAM grows with each additional record. Latches until
    // `ResetFlushNeeded()` is called (typically after the flush completes).
    bool ShouldFlush() const { return _flush_needed; }

    // Clears the `ShouldFlush()` latch. Called by the integration layer
    // after a successful flush-to-disk so the buffer can signal again on
    // the next budget crossing.
    void ResetFlushNeeded() { _flush_needed = false; }

    // Number of records appended so far. In flat mode this is
    // _records.size(); in compact mode the record vector has been
    // freed and occurrences live in per-term streams, so we return
    // _total_occurrences instead.
    size_t RecordCount() const { return _compact_mode ? _total_occurrences : _records.size(); }

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
        uint32_t doc_count; // distinct docs == doc_freq for StartTerm
    };
    CompactTermStreams CompactStreamsFor(uint32_t term_id) const {
        const auto& s = _term_states[term_id];
        return {s.doc_start, s.doc_upto, s.pos_start, s.pos_upto, s.occ_count, s.doc_count};
    }
    const BytePool& Pool() const { return _pool; }

    // True when this buffer omits term-freq+positions (DOCS_ONLY). In that mode
    // the prox slice chain is never written (pos_start==pos_end==0 for every
    // term) and emit must NOT construct a prox ByteSliceReader. The emit side
    // also tracks the same flag (via FreqProxEncoder); both derive from the
    // field's support_phrase property. A DCHECK at the top of
    // SpimiFulltextWriter::EmitSegment guards the only unsafe drift: an emit that
    // reads the prox chain (omit=false) while the buffer omitted it.
    bool OmitTfap() const { return _omit_tfap; }

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
        // CLucene/Lucene in-memory layout: the `doc_*` chain holds ONE entry PER
        // DOC (docCode = doc_delta<<1, with the low bit flagging freq==1, plus a
        // VInt(freq) when freq>1) and the `pos_*` chain holds the absolute
        // position PER OCCURRENCE. The previous per-OCCURRENCE doc-delta scheme
        // wrote a (mostly-zero) doc-delta for every occurrence, which exploded on
        // large high-freq-per-doc text — Wikipedia measured 4.2x V2's RAM. Grouping
        // freq per doc collapses a word's hundreds of same-doc hits into one entry.
        // The current doc's freq is buffered in `cur_doc_freq` and flushed to the
        // chain when the doc closes (here on a new doc, or at FinalizeBlocks).
        if (state.occ_count == 0) [[unlikely]] {
            state.doc_start = state.doc_upto = _pool.NewSlice(); // freq chain
            // The prox chain is dead in DOCS_ONLY (positions are discarded at
            // emit and no .prx is ever written), so skip the NewSlice() and the
            // per-token position VInt below. pos_start/pos_upto stay 0; emit
            // detects omit_tfap and never constructs a prox reader.
            if (!_omit_tfap) {
                state.pos_start = state.pos_upto = _pool.NewSlice(); // prox chain
                state.last_pos = 0; // within-doc position-delta base for this doc
            }
            state.last_doc = doc_id;
            state.cur_doc_delta = doc_id; // delta from implicit 0
            state.cur_doc_freq = 1;
            state.doc_count = 1;
        } else if (doc_id != state.last_doc) {
            if (doc_id < state.last_doc) [[unlikely]] {
                _compact_streams_sorted = false;
            }
            // Close the previous doc: write its (docCode, freq) to the freq chain.
            // docCode = (doc_delta << 1) | (freq==1) is computed in uint64 and
            // VInt64-coded — a full uint32 backward delta needs 33 bits, so a
            // uint32 << 1 would drop the top bit and corrupt non-monotonic runs.
            if (state.cur_doc_freq == 1) {
                state.doc_upto = _pool.WriteVInt64(
                        state.doc_upto, (static_cast<uint64_t>(state.cur_doc_delta) << 1U) | 1U);
            } else {
                state.doc_upto = _pool.WriteVInt64(
                        state.doc_upto, static_cast<uint64_t>(state.cur_doc_delta) << 1U);
                state.doc_upto = _pool.WriteVInt(state.doc_upto, state.cur_doc_freq);
            }
            state.cur_doc_delta = doc_id - state.last_doc; // modular delta (full uint32)
            state.last_doc = doc_id;
            state.cur_doc_freq = 1;
            ++state.doc_count;
            state.last_pos = 0; // reset within-doc position-delta base (no-op in omit)
        } else {
            // The position-monotonic check feeds `_compact_streams_sorted`, but
            // that only governs the prox re-sort — which is dead in DOCS_ONLY
            // (no positions are stored or decoded). Gate it under !_omit_tfap so
            // we don't read last_pos, which is no longer maintained in omit mode.
            // The DOC-order check above (444) STAYS: doc-delta order still
            // governs the .frq and the direct-emit-vs-sort decision.
            if (!_omit_tfap && position < state.last_pos) [[unlikely]] {
                _compact_streams_sorted = false;
            }
            ++state.cur_doc_freq;
        }
        // Within-doc position delta per occurrence (Lucene-style). last_pos is
        // reset to 0 at each doc boundary, so the first position in a doc is
        // stored as itself and the rest as intra-doc gaps — typically 1 byte vs
        // the 2-3 a long-doc absolute position needs, shrinking the in-memory
        // prox chain. The emit side rebuilds the absolute position (per-doc
        // prefix sum) before re-encoding, so the on-disk .prx is byte-identical.
        // Modular subtraction round-trips any order (matching the doc-delta
        // scheme). Skipped entirely in DOCS_ONLY (no prox chain, last_pos unread).
        if (!_omit_tfap) {
            state.pos_upto = _pool.WriteVInt(state.pos_upto, position - state.last_pos);
            state.last_pos = position;
        }
        ++state.occ_count;
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
        // 8-byte-at-a-time keyed hash (rapidhash-style mum mix). Replaces a
        // per-byte FNV-1a: ~3-6x fewer dependent multiplies for typical
        // 6-15 byte terms (Intern is 16-22% of V4 wall-clock). The per-instance
        // _hash_seed seeds the initial state and is folded by every mum step
        // (C5 DoS resistance); the result stays fully deterministic (same
        // seed+input => same hash). This hash only indexes the in-memory slot
        // table — term_id assignment order never reaches the on-disk bytes
        // (terms are emitted in lexicographic arena order), so output is
        // byte-identical regardless of which hash drives interning.
        auto mum = [](uint64_t a, uint64_t b) -> uint64_t {
            const __uint128_t r = static_cast<__uint128_t>(a) * b;
            return static_cast<uint64_t>(r) ^ static_cast<uint64_t>(r >> 64);
        };
        constexpr uint64_t k0 = 0xFF51AFD7ED558CCDULL;
        constexpr uint64_t k1 = 0xC4CEB9FE1A85EC53ULL;
        const char* p = term.data();
        size_t len = term.size();
        uint64_t h = _hash_seed ^ (static_cast<uint64_t>(len) * 0x9E3779B97F4A7C15ULL);
        while (len >= 8) {
            uint64_t k = 0;
            std::memcpy(&k, p, 8);
            h = mum(h ^ k, k0);
            p += 8;
            len -= 8;
        }
        if (len != 0) {
            uint64_t k = 0;
            std::memcpy(&k, p, len);
            h = mum(h ^ k, k1);
        }
        return mum(h, k0);
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
    // Per-term compact posting state. Occurrences are stored as two VInt slice
    // chains in the shared pool (delta-coded doc ids; plain positions). There is
    // NO in-memory FOR/bit-packing: a probe showed it cost more memory than it
    // saved and was slightly slower; the on-disk PFOR is applied at emit. This
    // mirrors CLucene/Lucene (VInt in RAM, bit-pack on serialize).
    struct TermPostingState {
        uint32_t doc_start = 0;     // pool offset: first byte of the per-doc freq chain
        uint32_t doc_upto = 0;      // pool offset: next freq-chain write
        uint32_t pos_start = 0;     // pool offset: first byte of the per-occ prox chain
        uint32_t pos_upto = 0;      // pool offset: next prox-chain write
        uint32_t occ_count = 0;     // total occurrences (sizes the decode)
        uint32_t doc_count = 0;     // distinct docs (== doc_freq) for the direct-emit path.
        uint32_t text_ref = 0;      // arena offset of the term bytes
        uint32_t last_doc = 0;      // last appended doc id (delta seed + monotonic check)
        uint32_t last_pos = 0;      // last appended position (monotonic check)
        uint32_t cur_doc_delta = 0; // raw modular doc-delta of the CURRENT open doc (full
                                    // uint32; the <<1 freq-flag pack is applied at write time
                                    // in uint64 so a backward delta's top bit is never lost)
        uint32_t cur_doc_freq = 0;  // freq within the CURRENT open doc, pending flush to chain
        // "first occurrence" is `occ_count == 0` — no separate bool needed (it
        // would add 4 padding bytes per term; on a 560 K-term Wikipedia segment
        // that is ~2 MB of pure padding). occ_count is incremented at the end of
        // every Append, so it is 0 iff nothing has been appended yet.
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
    //   - mostly_unique / all_unique workloads (avg ~1-3 occ/term)
    //     never trigger — flat records are lighter there (one 12 B record
    //     per occurrence beats a 44 B per-term state + 2 slice chains when
    //     terms barely repeat).
    //   - any workload with real term repetition (avg >= 8 occ/term)
    //     migrates to the compact slice-pool representation, whose peak is
    //     below CLucene's. This caught the large-vocabulary fulltext case
    //     (e.g. Wikipedia: avg ~20 occ/term, ~560 K terms) that the old
    //     threshold of 32 left stranded in flat mode — there flat
    //     materialized an ~11 M-record vector (12 B/occ) plus a same-size
    //     stable_sort scratch buffer, peaking ~4x CLucene's RAM. The
    //     crossover where compact wins is ~3-4 occ/term (the deque-backed
    //     per-term state has no realloc overshoot), so 8 sits safely above
    //     it while still leaving genuinely-diverse data on the flat path.
    static constexpr size_t kCompactCheckEvery = 512;
    static constexpr size_t kCompactAvgOcc = 8;

    // DOCS_ONLY (support_phrase=false). When true, EncodeOccurrenceToStreamInline
    // skips the prox slice-chain allocation + the per-token position VInt write,
    // and the records-fallback decode (DecodeCompactTerm) skips the prox reader
    // and emits position=0. The per-doc freq/doc-delta chain is unchanged, so the
    // on-disk .frq (hence .idx) is byte-identical to the non-gated path. Fixed at
    // construction from the field's support_phrase property. pos_start/pos_upto/
    // last_pos in TermPostingState are unused (stay 0/stale) while this is true.
    bool _omit_tfap = false;

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

    // Inline hot path of term-id resolution: when Intern() just touched
    // `text_ref`'s slot (the dominant case for any repeated term), the parallel
    // `_slot_term_ids` array gives the dense term id with a single load — no
    // hashmap, no re-probe. Returns kInvalidTermId on first sight OR when the
    // slot cache is stale (post-GrowSlots / migration loop); the caller then
    // falls through to the out-of-line `GetOrAssignTermIdCold`. Header-inlined
    // so it fuses into Append's hot loop (the old out-of-line GetOrAssignTermId
    // showed up as a separate ~1.4 % per-token call frame on the flame graph).
    [[gnu::always_inline]] inline uint32_t GetOrAssignTermIdHot(uint32_t text_ref) const {
        if (_last_intern_slot != static_cast<size_t>(-1) &&
            _last_intern_slot < _slot_term_ids.size() && _slots[_last_intern_slot] == text_ref)
                [[likely]] {
            return _slot_term_ids[_last_intern_slot]; // kInvalidTermId on first sight
        }
        return kInvalidTermId;
    }

    // Out-of-line term-id resolution: assigns a fresh dense id on first sight,
    // or recovers the existing id by re-probing `_slots`→`_slot_term_ids` when
    // the slot cache is stale. `_slot_term_ids` (kept in lockstep with `_slots`
    // by GrowSlots) is the sole source of truth — there is no auxiliary map.
    uint32_t GetOrAssignTermIdCold(uint32_t text_ref);

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
    bool _flush_needed = false;

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
    // Per-term posting state, indexed densely by term_id. A std::deque (NOT a
    // vector) on purpose: it never reallocates existing elements as it grows, so
    // it avoids BOTH of std::vector's growth costs that dominated the old peak —
    // (a) the up-to-2x capacity overshoot left after the final doubling, and
    // (b) the transient old+new copy during that doubling (a momentary 2x of the
    // whole array). On a 560 K-term Wikipedia segment these together added ~48 MB
    // of pure peak. The deque grows in small fixed chunks (waste < one chunk) and
    // never copies, so the array's resident size tracks the term count exactly.
    // term_id is a contiguous 0..N index (see GetOrAssignTermId*), so deque's
    // O(1) operator[] is the only access pattern; the extra map indirection per
    // occurrence is negligible next to the slice-pool VInt writes.
    std::deque<TermPostingState> _term_states;
    // Shared slice pool backing every term's doc/pos VInt prefix chains. Never
    // reallocates handed-out blocks, so it carries no per-term realloc
    // high-water — the dominant component of the old representation's peak.
    BytePool _pool;
    // Compact-mode term-id index: parallel array to `_slots`, holding the dense
    // term_id per slot. This is the SOLE source of truth for text_ref→term_id
    // (an earlier `_text_ref_to_term_id` unordered_map was removed — it cost a
    // per-term node allocation and ~3 MB of resident map on high-cardinality
    // unicode corpora, and added allocator-lock contention under concurrent
    // load, all for a cold-path lookup the slot table already answers via a
    // re-probe). Compact-mode Append does:
    //   Intern → text_ref          (already touches the slot)
    //   _slot_term_ids[slot]       → term_id   (no second hash, no map)
    // Same size + grow behaviour as `_slots` (GrowSlots migrates both in
    // lockstep). `kInvalidTermId` marks "no term_id assigned to this slot yet";
    // assigned on first Intern after compact mode activates, or during the
    // MaybeCompact migration when a slot's existing text_ref is first indexed.
    std::vector<uint32_t> _slot_term_ids;
    static constexpr uint32_t kInvalidTermId = 0xFFFFFFFFU;
    // Set by `Intern` to the slot index it ended up at. Compact-
    // mode Append reads this to grab term_id from
    // `_slot_term_ids[_last_intern_slot]`. Side-channel return
    // value to keep Intern's main return type unchanged for the
    // flat-mode callers.
    size_t _last_intern_slot = 0;
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
