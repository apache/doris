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

#include "storage/index/inverted/spimi/posting_buffer.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <random>
#include <unordered_map>

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

BytePool::~BytePool() {
    for (uint8_t* b : _blocks) {
        delete[] b;
    }
}

void BytePool::EnsureRoom(uint32_t size) {
    // Slices never span blocks; if the current block can't hold `size`
    // contiguous bytes, start a fresh zero-filled block. The tail of the old
    // block is left unused (bounded waste < kLevelSize max = 200 B/block).
    //
    // The `>= _blocks.size()` guard is essential: when a slice exactly fills a
    // block, `_byte_upto` lands on the next block's base (a multiple of
    // kBlockSize) whose masked local offset is 0 — without this guard the
    // size check would see "0 + size ≤ kBlockSize" and conclude the
    // (non-existent) next block has room, handing out an offset into an
    // unallocated block.
    if (_blocks.empty() || (_byte_upto >> kBlockShift) >= _blocks.size() ||
        (_byte_upto & kBlockMask) + size > kBlockSize) {
        auto* blk = new uint8_t[kBlockSize](); // value-init → all zero (no markers)
        _blocks.push_back(blk);
        _byte_upto = static_cast<uint32_t>(_blocks.size() - 1) << kBlockShift;
    }
}

uint32_t BytePool::NewSlice() {
    EnsureRoom(kFirstSize);
    const uint32_t g = _byte_upto;
    _blocks[g >> kBlockShift][(g & kBlockMask) + kFirstSize - 1] = 16; // level-0 marker
    _byte_upto += kFirstSize;
    return g;
}

uint32_t BytePool::AllocSlice(uint32_t upto) {
    // `upto` is the global index of the marker byte (last byte of the full
    // slice). Grow to the next level, copy the 3 tail data bytes forward, and
    // overwrite the old slice's last 4 bytes with the forwarding address.
    uint8_t* old_buf = _blocks[upto >> kBlockShift];
    const uint32_t ol = upto & kBlockMask;
    const int level = old_buf[ol] & 15;
    const int new_level = kNextLevel[level];
    const auto new_size = static_cast<uint32_t>(kLevelSize[new_level]);
    EnsureRoom(new_size);
    const uint32_t new_global = _byte_upto;
    uint8_t* nbuf = _blocks[new_global >> kBlockShift];
    const uint32_t nl = new_global & kBlockMask;
    _byte_upto += new_size;
    // Copy forward the 3 data bytes we are about to overwrite with the address.
    nbuf[nl] = old_buf[ol - 3];
    nbuf[nl + 1] = old_buf[ol - 2];
    nbuf[nl + 2] = old_buf[ol - 1];
    // Write the forwarding address (big-endian global offset) into the old tail.
    old_buf[ol - 3] = static_cast<uint8_t>(new_global >> 24);
    old_buf[ol - 2] = static_cast<uint8_t>(new_global >> 16);
    old_buf[ol - 1] = static_cast<uint8_t>(new_global >> 8);
    old_buf[ol] = static_cast<uint8_t>(new_global);
    nbuf[nl + new_size - 1] = static_cast<uint8_t>(16 | new_level); // new marker
    return new_global + 3;
}

void BytePool::Reset() {
    for (uint8_t* b : _blocks) {
        delete[] b;
    }
    _blocks.clear();
    _byte_upto = 0;
}

namespace {

inline uint32_t LoadLittleEndianU32(const uint8_t* p) {
    uint32_t v = 0;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline void StoreLittleEndianU32(uint8_t* p, uint32_t v) {
    std::memcpy(p, &v, sizeof(v));
}

// Big-endian first-8-byte prefix of a term, left-aligned so the resulting
// u64 orders identically to a full lexicographic (unsigned-byte) compare:
// shorter terms zero-pad their low bytes, and any prefix tie falls back to a
// full ArenaTermAt compare. Lets the per-term sort below skip the double
// ArenaTermAt + memcmp on the common case where two terms already differ
// within their first 8 bytes. The sort order (hence emitted bytes) is
// unchanged — only the comparison cost drops.
inline uint64_t PrefixKey8(std::string_view t) {
    uint64_t k = 0;
    const size_t n = t.size() < 8 ? t.size() : 8;
    for (size_t i = 0; i < n; ++i) {
        k = (k << 8) | static_cast<uint8_t>(t[i]);
    }
    k <<= (8U - n) * 8U;
    return k;
}

// Per-process random seed for the keyed FNV-1a hash. Combined with a
// per-instance counter at construction, this defeats attacker
// pre-computation of hash collisions across writers (un-keyed FNV-1a
// could be driven into O(n²) probe chains by adversarial document text).
//
// We mix a one-shot `std::random_device` salt with a monotonic
// per-instance counter scaled by the golden ratio constant. Two sources
// are sufficient: `std::random_device` is non-deterministic and the
// counter guarantees that two buffers constructed in the same instant
// still get distinct seeds. An earlier version also mixed
// `steady_clock::now()` — that added no real entropy under load (the
// counter alone disambiguates same-nanosecond constructions) and gave a
// misleading "defence against weak random_device" comment, so it was
// removed.
uint64_t MakeHashSeed() {
    static std::atomic<uint64_t> counter {0};
    static const uint64_t process_salt = []() {
        std::random_device rd;
        return (static_cast<uint64_t>(rd()) << 32) | static_cast<uint64_t>(rd());
    }();
    const uint64_t bump = counter.fetch_add(1, std::memory_order_relaxed);
    return process_salt ^ (bump * 0x9E3779B97F4A7C15ULL);
}

} // namespace

// Member init order follows DECLARATION order (`_omit_tfap` is declared before
// `_hash_seed`/`_limits` in the header) to avoid -Wreorder.
SpimiPostingBuffer::SpimiPostingBuffer(bool omit_tfap)
        : _omit_tfap(omit_tfap), _hash_seed(MakeHashSeed()) {}

SpimiPostingBuffer::SpimiPostingBuffer(uint64_t hash_seed, bool omit_tfap)
        : _omit_tfap(omit_tfap), _hash_seed(hash_seed) {}

SpimiPostingBuffer::SpimiPostingBuffer(uint64_t hash_seed, Limits limits, bool omit_tfap)
        : _omit_tfap(omit_tfap), _hash_seed(hash_seed), _limits(limits) {}

// HashTerm is now defined inline in the header for use by
// InternHot's inlined fast path. See posting_buffer.h.

std::string_view SpimiPostingBuffer::ArenaTermAt(uint32_t text_ref) const {
    const uint8_t* base = _arena.data() + text_ref;
    const uint32_t len = LoadLittleEndianU32(base);
    return {reinterpret_cast<const char*>(base + sizeof(uint32_t)), len};
}

std::string_view SpimiPostingBuffer::TermAt(uint32_t text_ref) const {
    return ArenaTermAt(text_ref);
}

bool SpimiPostingBuffer::ArenaTermEquals(uint32_t text_ref, std::string_view term) const {
    const uint8_t* base = _arena.data() + text_ref;
    const uint32_t len = LoadLittleEndianU32(base);
    if (len != term.size()) {
        return false;
    }
    if (len == 0) {
        return true;
    }
    return std::memcmp(base + sizeof(uint32_t), term.data(), len) == 0;
}

void SpimiPostingBuffer::Saturate(const char* reason, std::string_view term_prefix) {
    if (!_saturated) {
        // Capture up to 32 bytes of the offending term; render bytes
        // > 0x7E as `\xHH` so the log line stays single-line and
        // operator-friendly even when the input contains control bytes
        // or non-UTF-8. (Term arenas in this codebase are UTF-8 but the
        // input that tripped the latch may well be malformed — that is
        // a likely reason for saturation in the first place.)
        constexpr size_t kPrefixCap = 32;
        const size_t take = std::min(term_prefix.size(), kPrefixCap);
        std::string escaped;
        escaped.reserve(take * 2);
        for (size_t i = 0; i < take; ++i) {
            const auto c = static_cast<unsigned char>(term_prefix[i]);
            // Escape backslash AND the double-quote — the prefix is rendered
            // inside `"..."` in the log line, so a literal `"` from
            // attacker-controlled input would close the quoted span and let
            // garbage spill into the log. Glog isn't structured so this is
            // cosmetic, not exploitable, but cleaner this way.
            if (c >= 0x20 && c <= 0x7E && c != '\\' && c != '"') {
                escaped.push_back(static_cast<char>(c));
            } else {
                // Buffer is sized exactly for `\xHH\0` = 5 bytes; any change
                // to the format string MUST update this constant in lockstep.
                static_assert(sizeof("\\xFF") == 5, "escape buffer must fit \\xHH plus NUL");
                char buf[5];
                std::snprintf(buf, sizeof(buf), "\\x%02X", c);
                escaped.append(buf);
            }
        }
        LOG(WARNING) << "SPIMI buffer saturated: " << reason << " (records=" << _records.size()
                     << " arena=" << _arena.size() << " terms=" << _term_count << " term_prefix=\""
                     << escaped << (term_prefix.size() > kPrefixCap ? "..." : "")
                     << "\"); subsequent appends will be dropped";
        _saturated = true;
    }
}

void SpimiPostingBuffer::GrowSlots(size_t new_capacity) {
    std::vector<uint32_t> new_slots(new_capacity, kEmpty);
    // Only sized when compact mode is active; otherwise stays empty
    // and `_slot_term_ids.swap(new_slot_term_ids)` swaps two empties.
    std::vector<uint32_t> new_slot_term_ids(_slot_term_ids.empty() ? 0 : new_capacity,
                                            kInvalidTermId);
    const size_t mask = new_capacity - 1;
    for (size_t old_slot = 0; old_slot < _slots.size(); ++old_slot) {
        const uint32_t text_ref = _slots[old_slot];
        if (text_ref == kEmpty) {
            continue;
        }
        const std::string_view term = ArenaTermAt(text_ref);
        size_t slot = HashTerm(term) & mask;
        while (new_slots[slot] != kEmpty) {
            slot = (slot + 1) & mask;
        }
        new_slots[slot] = text_ref;
        // Migrate the parallel term_id when compact mode is active
        // (`_slot_term_ids` non-empty). In flat mode the array stays
        // empty — see Intern() for the deferred-init rationale.
        if (!_slot_term_ids.empty()) {
            new_slot_term_ids[slot] = _slot_term_ids[old_slot];
        }
    }
    _slots.swap(new_slots);
    _slot_term_ids.swap(new_slot_term_ids);
}

uint32_t SpimiPostingBuffer::InternCold(std::string_view term) {
    // Cold path: term is not in the slot table (or InternHot
    // ran out of probe budget). Run the full hash + probe and
    // insert if still missing. Handles slot-table grow.
    return Intern(term);
}

uint32_t SpimiPostingBuffer::Intern(std::string_view term) {
    // Caching attempts on the Intern hot path:
    //   * 1-entry "last term" cache — hit rate <1 % because the
    //     analyzer emits distinct sequential tokens; pure
    //     overhead.
    //   * 64-bucket direct-mapped (hash, text_ref) cache —
    //     regressed plain_log / json_log / wikipedia_zipf /
    //     repetitive measurably. Bucket compute + load + cmp
    //     adds ~5 ns/call; hit savings ~20 ns × few-% hit rate;
    //     net negative.
    // Both reverted. If we revisit, the right place is probably
    // to make the analyzer emit (term_id, term_bytes) pairs so
    // SPIMI never re-hashes — bigger refactor touching the
    // Tokenizer interface.
    if (_slots.empty()) {
        _slots.assign(kInitialSlots, kEmpty);
        // `_slot_term_ids` is deferred until compact mode activates
        // — flat mode doesn't call `GetOrAssignTermId` at all, so
        // pre-allocating a parallel uint32 per slot would just
        // double the slot-table memory for the entire flat-mode
        // lifetime. `MaybeCompact` initializes it before the first
        // GetOrAssignTermId call.
    }

    const size_t mask = _slots.size() - 1;
    size_t slot = HashTerm(term) & mask;
    while (true) {
        const uint32_t entry = _slots[slot];
        if (entry == kEmpty) {
            // Insert: append [length][bytes] to the arena, claim the slot.
            const auto text_ref = static_cast<uint32_t>(_arena.size());
            const auto len = static_cast<uint32_t>(term.size());
            const size_t old_size = _arena.size();
            const size_t needed = old_size + sizeof(uint32_t) + len;
            // Pre-reserve with 1.25× growth (vs std::vector's default 2×).
            // Caps capacity overhead at ~12.5 % of live arena size and is
            // material for memory-budget targets — the arena is the second-
            // largest bucket after _records on token-heavy fulltext
            // workloads. The fallback branch ensures we always have room
            // for the current insertion.
            if (needed > _arena.capacity()) {
                const size_t target =
                        std::max(needed, _arena.size() + _arena.size() / 4 + sizeof(uint32_t));
                _arena.reserve(target);
            }
            _arena.resize(needed);
            StoreLittleEndianU32(_arena.data() + old_size, len);
            if (len != 0) {
                std::memcpy(_arena.data() + old_size + sizeof(uint32_t), term.data(), len);
            }
            _slots[slot] = text_ref;
            ++_term_count;
            _last_intern_slot = slot;

            // Grow when load factor exceeds 3/4. The slot array stays at
            // power-of-two sizes for the bitmask probing, so the growth
            // factor here is 2× — but slots are only 4 bytes each so the
            // capacity overhead is small in absolute terms compared to
            // _records and _arena.
            if (_term_count * 4 > _slots.size() * 3) {
                GrowSlots(_slots.size() * 2);
                // `_last_intern_slot` now points into the OLD table layout —
                // invalidate it so the compact-mode term-id resolution takes
                // the cold path (re-probe `_slots`→`_slot_term_ids`) for this
                // single insertion.
                _last_intern_slot = static_cast<size_t>(-1);
            }
            return text_ref;
        }
        if (ArenaTermEquals(entry, term)) {
            _last_intern_slot = slot;
            return entry;
        }
        slot = (slot + 1) & mask;
    }
}

uint32_t SpimiPostingBuffer::GetOrAssignTermIdCold(uint32_t text_ref) {
    // Reached when `GetOrAssignTermIdHot` returned kInvalidTermId — either the
    // term's first occurrence (slot cache valid, but no id assigned yet) or a
    // stale slot cache (MaybeCompact migration loop / post-GrowSlots). Resolve
    // the slot, then read-or-assign its dense term_id via `_slot_term_ids` —
    // the sole source of truth (kept in lockstep with `_slots` by GrowSlots).
    size_t slot;
    if (_last_intern_slot != static_cast<size_t>(-1) && _last_intern_slot < _slot_term_ids.size() &&
        _slots[_last_intern_slot] == text_ref) {
        // Slot cache is valid (common: first occurrence of a term Intern just
        // inserted). The `_slots[_last_intern_slot] == text_ref` check is
        // LOAD-BEARING: without it a stale slot would collapse distinct terms.
        slot = _last_intern_slot;
    } else {
        // Stale cache: re-probe the slot table for text_ref. The term IS
        // interned (Append always Interns before resolving a term_id), so the
        // matching slot is guaranteed present — assert it rather than guarding.
        DCHECK(!_slots.empty());
        const size_t mask = _slots.size() - 1;
        const std::string_view term = ArenaTermAt(text_ref);
        slot = HashTerm(term) & mask;
        while (_slots[slot] != text_ref) {
            DCHECK(_slots[slot] != kEmpty); // text_ref must be interned
            slot = (slot + 1) & mask;
        }
    }
    // `_slot_term_ids` is sized in lockstep with `_slots` (MaybeCompact assigns
    // it; GrowSlots migrates it), so the resolved slot indexes it in bounds.
    DCHECK_LT(slot, _slot_term_ids.size());
    uint32_t id = _slot_term_ids[slot];
    if (id == kInvalidTermId) {
        id = static_cast<uint32_t>(_term_states.size());
        auto& st = _term_states.emplace_back();
        st.text_ref = text_ref;
        _slot_term_ids[slot] = id;
    }
    return id;
}

void SpimiPostingBuffer::EncodeOccurrenceToStream(uint32_t term_id, uint32_t doc_id,
                                                  uint32_t position) {
    // Out-of-line entry (used by the MaybeCompact migration loop); delegates
    // to the inlined block-pending hot path so both share one implementation.
    EncodeOccurrenceToStreamInline(term_id, doc_id, position);
}

void SpimiPostingBuffer::FinalizeBlocks() {
    if (_finalized) {
        return;
    }
    // Close every term's last open doc: its (docCode, freq) is still buffered in
    // cur_doc_freq/cur_doc_delta and must be flushed to the freq chain so the
    // chain holds one entry per doc for the whole term. (Per-doc freq is written
    // on doc transitions during Append; the final doc has no transition.)
    // docCode is VInt64-coded (delta<<1|flag in uint64) for the same
    // backward-delta-safety reason as the Append hot path.
    for (auto& s : _term_states) {
        if (s.occ_count == 0) {
            continue;
        }
        if (s.cur_doc_freq == 1) {
            s.doc_upto = _pool.WriteVInt64(s.doc_upto,
                                           (static_cast<uint64_t>(s.cur_doc_delta) << 1U) | 1U);
        } else {
            s.doc_upto =
                    _pool.WriteVInt64(s.doc_upto, static_cast<uint64_t>(s.cur_doc_delta) << 1U);
            s.doc_upto = _pool.WriteVInt(s.doc_upto, s.cur_doc_freq);
        }
    }
    // NOTE: the intern slot tables (_slots/_slot_term_ids) are NOT freed here.
    // FinalizeBlocks() runs before Sort()'s path fork, and the compact-mode
    // records-fallback path (non-monotonic streams) decodes into _records, sets
    // _compact_mode=false, then falls through to the FLAT-mode counting sort
    // which reads _slots. Freeing them here would empty that sort's input. They
    // are instead freed in the direct-emit branch of Sort(), where _slots is
    // provably dead (emit iterates _sorted_compact_terms, never _slots).
    _finalized = true;
}

void SpimiPostingBuffer::DecodeCompactTerm(uint32_t term_id, std::vector<uint32_t>& docs,
                                           std::vector<uint32_t>& positions) const {
    const auto& state = _term_states[term_id];
    const uint32_t n = state.occ_count;
    docs.resize(n);
    positions.resize(n);
    if (n == 0) {
        return;
    }
    // Per-doc freq chain (docCode = doc_delta<<1, low bit = freq==1, else a
    // trailing VInt(freq)) + per-occurrence absolute positions in the prox chain.
    // Expand back to the per-occurrence (doc, position) sequence as appended.
    //
    // In DOCS_ONLY there is NO prox chain (EncodeOccurrenceToStreamInline skipped
    // it), so positions[] is filled with 0 and no prox reader is constructed —
    // constructing one over the unwritten pos_start/pos_upto==0 span would read
    // garbage. The 0 positions are discarded downstream (emit drops them via the
    // encoder no-op / the omit-aware EmitFromCompactDirect), so this stays
    // byte-neutral.
    ByteSliceReader freq_reader(_pool, state.doc_start, state.doc_upto);
    const bool read_pos = !_omit_tfap;
    uint32_t prev = 0;
    uint32_t i = 0;
    if (read_pos) {
        ByteSliceReader pos_reader(_pool, state.pos_start, state.pos_upto);
        while (i < n) {
            const uint64_t code = freq_reader.ReadVInt64();
            prev += static_cast<uint32_t>(code >> 1U); // modular doc delta; round-trips any order
            const uint32_t freq = (code & 1U) ? 1U : freq_reader.ReadVInt();
            // Positions are stored as within-doc deltas (reset to 0 per doc);
            // prefix-sum them back to absolute. Modular add round-trips any order.
            uint32_t pos = 0;
            for (uint32_t k = 0; k < freq; ++k) {
                docs[i] = prev;
                pos += pos_reader.ReadVInt();
                positions[i] = pos;
                ++i;
            }
        }
    } else {
        while (i < n) {
            const uint64_t code = freq_reader.ReadVInt64();
            prev += static_cast<uint32_t>(code >> 1U); // modular doc delta; round-trips any order
            const uint32_t freq = (code & 1U) ? 1U : freq_reader.ReadVInt();
            for (uint32_t k = 0; k < freq; ++k) {
                docs[i] = prev;
                positions[i] = 0; // DOCS_ONLY: no stored position; discarded at emit
                ++i;
            }
        }
    }
}

void SpimiPostingBuffer::DecodeTermToRecords(uint32_t term_id) {
    const auto& state = _term_states[term_id];
    const uint32_t n = state.occ_count;
    const uint32_t text_ref = state.text_ref;
    std::vector<uint32_t> docs;
    std::vector<uint32_t> positions;
    DecodeCompactTerm(term_id, docs, positions);
    for (uint32_t i = 0; i < n; ++i) {
        _records.push_back(SpimiRecord {text_ref, docs[i], positions[i]});
    }
}

void SpimiPostingBuffer::MaybeCompact() {
    if (_compact_mode) {
        return;
    }
    if (_records.size() < kCompactCheckEvery) {
        return;
    }
    if (_term_count == 0 || _records.size() / _term_count < kCompactAvgOcc) {
        // Vocabulary diverse — flat records remain the better
        // representation. Bail out and don't keep re-checking until
        // we cross the next `kCompactCheckEvery` boundary.
        return;
    }
    // Migrate all records into per-term arrays. Records are walked
    // in insertion order so the arrays see the same (doc, pos)
    // sequence Append() would have produced directly. `_term_states` is a
    // std::deque, so no reserve() — it grows in fixed chunks without ever
    // reallocating, sidestepping the vector doubling overshoot + copy spike.
    // The per-term StreamVByte streams need no pre-reservation: occurrences
    // accumulate in the fixed-size pending block and are flushed in bulk, so
    // there is no per-occurrence array growth to amortize.
    // Initialize the parallel `_slot_term_ids` array now that we're
    // entering compact mode — see `Intern()` for the deferred-init
    // rationale. The slot table itself (`_slots`) is already
    // populated from the flat-mode Appends; we just need a
    // term_id-per-slot companion that the post-migration Append hot
    // path can read in O(1).
    _slot_term_ids.assign(_slots.size(), kInvalidTermId);
    // Force the cold path during migration: `_last_intern_slot` is
    // stale (points at the last flat-mode Append's term) and would
    // mis-route records onto term_id 0 (P51 corruption bug). The cold
    // path (`GetOrAssignTermIdCold`) re-probes `_slots` for each record's
    // text_ref and assigns/reads `_slot_term_ids[slot]`, so it is correct
    // for the stale-cache case AND leaves `_slot_term_ids` populated so
    // post-migration Appends hit the fast path.
    _last_intern_slot = static_cast<size_t>(-1);
    for (const auto& rec : _records) {
        const uint32_t term_id = GetOrAssignTermIdCold(rec.text_ref);
        EncodeOccurrenceToStream(term_id, rec.doc_id, rec.position);
    }
    _records.clear();
    _records.shrink_to_fit();
    _compact_mode = true;
}

// The hot append path deliberately keeps term-lookup, slice-pool growth, the
// VInt→FOR graduation branch and the trailing-batch flush inline (no
// per-occurrence call overhead); splitting it would add indirection on the
// busiest loop in the writer for no readability win.
// NOLINTNEXTLINE(readability-function-size)
void SpimiPostingBuffer::Append(std::string_view term, uint32_t doc_id, uint32_t position) {
    // Sort() finalizes the compact streams (flushes trailing blocks, adds
    // decode padding) and is terminal — appending afterwards would corrupt the
    // fixed-width-block decode. Reset() clears this to reuse the buffer.
    DCHECK(!_finalized) << "Append() after Sort()/FinalizeBlocks() is not allowed";
    if (_saturated) {
        return;
    }
    // C1 / C6 — hard upper bounds on per-term size, arena size, and record
    // count. Crossing any of these would either (a) truncate `text_ref` from
    // size_t to uint32_t at the cast in Intern() and silently corrupt the
    // emitted index file, or (b) consume unbounded resident memory on an
    // adversarial single value. Saturation freezes the buffer; subsequent
    // Append calls become no-ops. Callers detect via `Saturated()` at
    // Finish() time.
    if (term.size() > _limits.max_term_bytes) {
        Saturate("term exceeds max_term_bytes", term);
        return;
    }
    if (!_compact_mode && _records.size() >= _limits.max_record_count) {
        Saturate("records exceed max_record_count", term);
        return;
    }
    if (_compact_mode && _total_occurrences >= _limits.max_record_count) {
        Saturate("occurrences exceed max_record_count", term);
        return;
    }
    // The slice pool is addressed by a uint32 global offset; saturate well
    // below the 4 GiB wrap (same 2 GiB bound as the arena) so a pathological
    // input can never overflow `_byte_upto`. One Append adds at most a 200 B
    // slice + one 32 KiB block, so 2 GiB headroom is ample.
    if (_compact_mode && _pool.MemoryUsage() >= _limits.max_arena_bytes) [[unlikely]] {
        Saturate("posting pool would exceed max_arena_bytes", term);
        return;
    }
    // The Intern() path can grow the arena by up to (4 + term.size()) bytes
    // on a first-sight term. Check before the cast in Intern() so we
    // never reach a state where `_arena.size() >= 2^32`.
    if (_arena.size() + sizeof(uint32_t) + term.size() > _limits.max_arena_bytes) {
        Saturate("arena would exceed max_arena_bytes", term);
        return;
    }
    // Inline hot-path Intern at the Append call site: a slot-table
    // hit is the dominant case for any non-degenerate vocabulary,
    // and `InternHot` is `[[gnu::always_inline]]` in the header so
    // the hash + probe + arena byte compare all fuse into Append's
    // hot loop. Flame graphs put `Intern` cumulative at 16-22 % of
    // V4 wall-clock; the function-call boundary alone is ~5-10 ns
    // per token, and the inlined fast path additionally lets the
    // compiler CSE the term length + hash across the branch.
    uint32_t text_ref = InternHot(term);
    if (text_ref == kEmpty) [[unlikely]] {
        text_ref = InternCold(term);
    }
    ++_total_occurrences;

    if (_compact_mode) {
        // Compact mode: stream the (doc, pos) occurrence straight into the
        // term's two VInt slice chains. The repeated-term term-id lookup is
        // header-inlined (`GetOrAssignTermIdHot`, a single `_slot_term_ids`
        // load) so it fuses into Append's hot loop; only first-sight / stale-
        // cache tokens take the out-of-line cold path. `EncodeOccurrence...`
        // is likewise header-inlined — no call boundary in the steady state.
        uint32_t term_id = GetOrAssignTermIdHot(text_ref);
        if (term_id == kInvalidTermId) [[unlikely]] {
            term_id = GetOrAssignTermIdCold(text_ref);
        }
        EncodeOccurrenceToStreamInline(term_id, doc_id, position);
        // Soft memory budget check (same as the flat-mode path below). The
        // SpimiIndexWriter facade polls FlushNeeded() to spill to disk once
        // the per-term varint streams cross the configured budget.
        if (!_flush_needed && MemoryUsage() >= _limits.memory_budget_bytes) {
            _flush_needed = true;
        }
        return;
    }

    // Override std::vector's 2× growth with 1.25× so capacity overhead stays
    // ≤ 12.5 % of live size instead of ≤ 50 %. The 12 B record dominates
    // memory usage for high-occurrence fulltext workloads, so the smaller
    // growth factor materially cuts peak resident bytes (~30 % saving on
    // the records vector alone) without measurable CPU impact (the
    // additional reallocs are O(log_1.25 N) ≈ 1.5 × O(log_2 N), still
    // ~50 reallocs for N = 10⁹). The arena and intern map use the same
    // manual-growth pattern in Intern() / GrowSlots().
    if (_records.size() == _records.capacity()) {
        const size_t new_capacity =
                std::max(_records.size() + _records.size() / 4 + 1, static_cast<size_t>(16));
        _records.reserve(new_capacity);
    }
    _records.push_back(SpimiRecord {text_ref, doc_id, position});

    // Cardinality check at `kCompactCheckEvery` boundaries. If
    // vocabulary is small enough, switch to compact mode (one-way).
    if (_records.size() % kCompactCheckEvery == 0) {
        MaybeCompact();
    }

    // Soft memory budget check. When the buffer's resident bytes cross
    // the configured budget, latch `_flush_needed` so the caller knows
    // to spill to disk. Unlike saturation this is non-fatal: subsequent
    // Appends continue to work, but the caller should flush ASAP to
    // keep peak RAM bounded. The check is cheap (one MemoryUsage()
    // call per Append) and only latches once per flush cycle.
    if (!_flush_needed && MemoryUsage() >= _limits.memory_budget_bytes) {
        _flush_needed = true;
    }
}

size_t SpimiPostingBuffer::MemoryUsage() const {
    // `_slot_term_ids` is allocated lazily — only populated when
    // compact mode activates (see `MaybeCompact`). In flat mode
    // it stays empty so the slot-table memory is just `_slots`.
    size_t bytes = _arena.capacity() + _slots.capacity() * sizeof(uint32_t) +
                   _slot_term_ids.capacity() * sizeof(uint32_t);
    bytes += _records.capacity() * sizeof(SpimiRecord);
    if (_compact_mode) {
        // Per-term state array + the shared slice-pool blocks holding the
        // doc/pos VInt chains. (No FOR tails — in-memory FOR removed.)
        // `_term_states` is a std::deque: size()*sizeof is exact to within one
        // partially-filled chunk (no capacity() overshoot to account for).
        bytes += _term_states.size() * sizeof(TermPostingState);
        bytes += _pool.MemoryUsage();
    }
    return bytes;
}

void SpimiPostingBuffer::Reset() {
    _records.clear();
    _arena.clear();
    // Re-zero the slot tables in place to preserve their capacity for buffer
    // reuse (avoids re-allocating the slot table on the next segment). Two
    // cases: (a) a flat-mode or non-direct-emit segment leaves the tables
    // populated, so this fill resets them cheaply; (b) the compact direct-emit
    // path freed them at finalize, so these vectors are empty here and the fill
    // is a no-op — the next Append()'s Intern() re-allocates `_slots` (its
    // `if (_slots.empty())` branch) and MaybeCompact() re-sizes `_slot_term_ids`.
    // Both cases are correct.
    std::fill(_slots.begin(), _slots.end(), kEmpty);
    std::fill(_slot_term_ids.begin(), _slot_term_ids.end(), kInvalidTermId);
    _term_count = 0;
    _saturated = false;
    _flush_needed = false;
    _last_intern_slot = static_cast<size_t>(-1);
    // Compact-mode state: drop per-term streams but don't keep them
    // around — they're heavy and a reused buffer should start over
    // with the flat-records fast path.
    _term_states.clear();
    _term_states.shrink_to_fit();
    _pool.Reset();
    _compact_mode = false;
    _compact_streams_sorted = true;
    _total_occurrences = 0;
    _compact_direct_emit = false;
    _sorted_compact_terms.clear();
    _sorted_compact_terms.shrink_to_fit();
    _finalized = false;
}

// The terminal flush orchestrates term sort, block finalization and the
// compact / direct-emit / materialized-records fork as one cohesive sequence;
// the branches share local state and extracting them would spread that state
// across helpers for a marginal complexity-score gain. Verified correct by the
// SPIMI roundtrip and segment-writer unit tests.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
void SpimiPostingBuffer::Sort(bool allow_direct_emit) {
    _compact_direct_emit = false;
    _sorted_compact_terms.clear();
    if (_compact_mode) {
        // Flush every term's trailing pending block so the StreamVByte streams
        // are complete and decodable (by direct-emit or DecodeTermToRecords).
        FinalizeBlocks();
        // Fast path: streams were filled monotonically (no reset
        // sentinel was ever emitted). Each stream's decoded records
        // are then already in (doc, pos) order. We can emit per-term
        // contiguously in TEXT-SORTED term order — the global
        // `stable_sort` below becomes a no-op and we save the
        // O(N log N) work entirely. On the 12K-occurrence repetitive
        // bench this drops `finish` from ~16 ms to ~2 ms (matches
        // CLucene's finish time).
        const bool can_skip_global_sort = _compact_streams_sorted;
        // Enumerate every (text_ref, term_id) pair directly from `_term_states`
        // (dense, indexed by term_id; `.text_ref` is the arena offset). This
        // replaced an iteration over the removed `_text_ref_to_term_id` map.
        // (prefix_key, text_ref, term_id). The u64 prefix key resolves the vast
        // majority of sort comparisons with one integer compare instead of two
        // ArenaTermAt() lookups + a memcmp; only a first-8-byte tie falls back
        // to the full arena compare. Precomputing the key is O(T) ArenaTermAt
        // calls, repaid by the O(T log T) sort.
        struct TermSortEntry {
            uint64_t key;
            uint32_t text_ref;
            uint32_t term_id;
        };
        std::vector<TermSortEntry> term_text_to_id;
        term_text_to_id.reserve(_term_states.size());
        for (uint32_t term_id = 0; term_id < _term_states.size(); ++term_id) {
            const uint32_t text_ref = _term_states[term_id].text_ref;
            term_text_to_id.push_back({PrefixKey8(ArenaTermAt(text_ref)), text_ref, term_id});
        }
        if (can_skip_global_sort) {
            std::sort(term_text_to_id.begin(), term_text_to_id.end(),
                      [this](const TermSortEntry& a, const TermSortEntry& b) {
                          if (a.key != b.key) {
                              return a.key < b.key;
                          }
                          return ArenaTermAt(a.text_ref) < ArenaTermAt(b.text_ref);
                      });
            if (allow_direct_emit) {
                // Direct-emit: keep the per-term arrays alive and hand the
                // segment writer the text-sorted term list. Skips the
                // `_records` materialization entirely (the dominant
                // finish-time peak: 12 B/occ on top of the 8 B/occ arrays).
                _sorted_compact_terms.reserve(term_text_to_id.size());
                for (const auto& e : term_text_to_id) {
                    _sorted_compact_terms.push_back(CompactTermRef {e.text_ref, e.term_id});
                }
                // The intern slot tables are now dead: this terminal direct-emit
                // path hands the segment writer _sorted_compact_terms + the
                // per-term arrays, and EmitFromCompactDirect never reads _slots.
                // Free them (swap-to-empty deallocates without a realloc copy) to
                // drop the emit-phase peak. Reset() re-inits them for buffer
                // reuse. Byte-identical: no emitted byte depends on _slots.
                std::vector<uint32_t>().swap(_slots);
                std::vector<uint32_t>().swap(_slot_term_ids);
                _compact_direct_emit = true;
                return;
            }
        }
        // Materialization path: caller needs `records()` (norms/doc-length),
        // or streams were non-monotonic and still need the global sort below.
        // Decode each term's StreamVByte streams back into flat records.
        _records.clear();
        _records.reserve(_total_occurrences);
        for (const auto& e : term_text_to_id) {
            DecodeTermToRecords(e.term_id);
        }
        _term_states.clear();
        _term_states.shrink_to_fit();
        _compact_mode = false;
        if (can_skip_global_sort) {
            // Records are now in (term-text, doc, pos) order without
            // a global sort. Return early; the stable_sort below is
            // only needed when streams contained reset sentinels.
            return;
        }
    }
    if (_records.empty()) {
        return;
    }
    // Flat-mode sort. The previous implementation called
    // `std::stable_sort(_records, byte_compare_arena_terms)`. That
    // ran the byte-wise term comparison O(N log N) times — for
    // 100 K records on plain-log/json-log fixtures, ~40 % of total
    // V4 wall-clock (per gperftools flame graph; SegmentWriter::Emit
    // + EncodeOccurrenceToStream were the remaining hot spots).
    //
    // The replacement is a two-stage sort that exploits two
    // invariants the flat-mode buffer maintains:
    //
    //   (1) Intern() dedups terms, so all records sharing a term
    //       share the same `text_ref`. Total distinct terms T is
    //       bounded by `_term_count` (≤ kMaxTerms ≈ 1 M and in
    //       practice << N for any non-degenerate workload).
    //   (2) Append() is monotonic per term: doc_id never decreases
    //       (`_rid` is a row counter), and within a doc positions
    //       come from the analyzer in ascending order. So records
    //       sharing a term arrive in (doc_id, position) ascending.
    //
    // Stage 1: collect the T distinct text_refs from the slot table
    //          (no extra dedup needed) and lexicographically sort
    //          them once. Cost: O(T log T) of arena byte compares —
    //          much smaller than the old O(N log N).
    // Stage 2: counting sort `_records` by the term's lex rank.
    //          Counting sort is stable, so per-term records stay in
    //          their original Append order.
    // Stage 3: within each rank bucket, sort by (doc_id, position).
    //          For production (sequential rows + monotonic analyzer
    //          positions) buckets are *already* sorted and the
    //          per-bucket std::sort costs O(bucket_size) of compares
    //          + zero swaps. For test/randomised inputs that
    //          Append records out of order, the per-bucket sort
    //          restores correctness. The comparator now reads only
    //          two uint32 fields — no arena byte compare anywhere
    //          inside the records-dimension sort.
    //
    // Output: `_records` reordered to (lex-rank, doc_id, position)
    // ascending, identical to what the old stable_sort produced.
    // The records themselves are unchanged (text_ref preserved),
    // so SegmentWriter::Emit (which reads `text_ref` to look up
    // term bytes via `buffer.TermAt`) is unaffected.
    //
    // The counting-sort path wins when T << N. When T ≈ N
    // (all-unique vocabularies — UUID columns, ingest IDs, the
    // mostly_unique/all_unique fixtures) it's a net loss: the
    // term-list sort already pays the O(N log N) byte compares,
    // and on top of that we burn cycles on hashmap build/lookup +
    // a non-contiguous placement pass. Heuristic: use counting
    // sort only when avg occurrences/term ≥ 4. For T ≈ N, fall
    // through to a direct std::sort over `_records` (the previous
    // baseline minus the unnecessary stable_sort overhead — all
    // records have unique (text_ref, doc_id, position) triples
    // because positions are monotonic within a doc and doc_ids
    // differ across rows, so stability buys nothing).
    constexpr size_t kCountingSortMinAvgOcc = 4;
    const bool use_counting_sort =
            _term_count > 0 && _records.size() / _term_count >= kCountingSortMinAvgOcc;
    if (!use_counting_sort) {
        std::sort(_records.begin(), _records.end(),
                  [this](const SpimiRecord& a, const SpimiRecord& b) {
                      if (a.text_ref != b.text_ref) {
                          const std::string_view ta = ArenaTermAt(a.text_ref);
                          const std::string_view tb = ArenaTermAt(b.text_ref);
                          const int c = ta.compare(tb);
                          DCHECK_NE(c, 0);
                          return c < 0;
                      }
                      if (a.doc_id != b.doc_id) {
                          return a.doc_id < b.doc_id;
                      }
                      return a.position < b.position;
                  });
        return;
    }
    // Same 8-byte-prefix-key trick as the compact-mode term sort above: most
    // comparisons resolve on one integer compare; only a first-8-byte tie pays
    // the double ArenaTermAt + memcmp. Sort order (hence emitted bytes) is
    // unchanged.
    struct RefKey {
        uint64_t key;
        uint32_t text_ref;
    };
    std::vector<RefKey> keyed_refs;
    keyed_refs.reserve(_term_count);
    for (uint32_t entry : _slots) {
        if (entry != kEmpty) {
            keyed_refs.push_back({PrefixKey8(ArenaTermAt(entry)), entry});
        }
    }
    DCHECK_EQ(keyed_refs.size(), _term_count);
    std::sort(keyed_refs.begin(), keyed_refs.end(), [this](const RefKey& a, const RefKey& b) {
        if (a.key != b.key) {
            return a.key < b.key;
        }
        return ArenaTermAt(a.text_ref) < ArenaTermAt(b.text_ref);
    });
    std::vector<uint32_t> sorted_text_refs;
    sorted_text_refs.reserve(keyed_refs.size());
    for (const RefKey& rk : keyed_refs) {
        sorted_text_refs.push_back(rk.text_ref);
    }

    // text_ref → lex rank. text_refs are sparse arena offsets, but
    // each Intern() call produces a *unique* text_ref bounded by
    // `_arena.size()` (≤ kMaxArenaBytes = 16 MiB). For typical
    // workloads `_term_count` is ~10 K and the hash map is the
    // right shape; for pathological all-unique workloads it grows
    // to N but is still amortised O(1) per insert.
    std::unordered_map<uint32_t, uint32_t> rank_by_text_ref;
    rank_by_text_ref.reserve(sorted_text_refs.size() * 2);
    for (uint32_t i = 0; i < sorted_text_refs.size(); ++i) {
        rank_by_text_ref.emplace(sorted_text_refs[i], i);
    }

    // Counting sort.
    std::vector<uint32_t> bucket_count(sorted_text_refs.size(), 0);
    std::vector<uint32_t> rank_for(_records.size());
    for (size_t i = 0; i < _records.size(); ++i) {
        const auto it = rank_by_text_ref.find(_records[i].text_ref);
        DCHECK(it != rank_by_text_ref.end());
        const uint32_t r = it->second;
        rank_for[i] = r;
        ++bucket_count[r];
    }
    std::vector<uint32_t> cursor(sorted_text_refs.size() + 1, 0);
    for (size_t i = 0; i < sorted_text_refs.size(); ++i) {
        cursor[i + 1] = cursor[i] + bucket_count[i];
    }
    // `cursor[i]` now == start offset for bucket i; we'll advance it
    // in-place during placement.
    std::vector<SpimiRecord> placed(_records.size());
    // Snapshot the starting cursor for each bucket BEFORE the in-
    // place placement advances it; we need both `start = original
    // cursor[i]` and `end = cursor[i] after placement` to re-sort
    // each bucket below.
    std::vector<uint32_t> bucket_start = cursor;
    for (size_t i = 0; i < _records.size(); ++i) {
        const uint32_t r = rank_for[i];
        placed[cursor[r]++] = _records[i];
    }
    _records = std::move(placed);

    // Per-bucket (doc_id, position) sort. In production this is
    // O(N) total across buckets because Append is monotonic per
    // term and std::sort on sorted input does N-1 compares + 0
    // swaps. For randomised input (unit tests) it's
    // O(sum b_i log b_i) which is still bounded above by the old
    // O(N log N) and runs on cheap uint32 comparisons rather than
    // arena byte compares.
    for (size_t i = 0; i < bucket_start.size() - 1; ++i) {
        const auto begin = _records.begin() + bucket_start[i];
        const auto end = _records.begin() + bucket_start[i + 1];
        if (end - begin <= 1) {
            continue;
        }
        std::sort(begin, end, [](const SpimiRecord& a, const SpimiRecord& b) {
            if (a.doc_id != b.doc_id) {
                return a.doc_id < b.doc_id;
            }
            return a.position < b.position;
        });
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
