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

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

inline uint32_t LoadLittleEndianU32(const uint8_t* p) {
    uint32_t v = 0;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline void StoreLittleEndianU32(uint8_t* p, uint32_t v) {
    std::memcpy(p, &v, sizeof(v));
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

SpimiPostingBuffer::SpimiPostingBuffer() : _hash_seed(MakeHashSeed()) {}

SpimiPostingBuffer::SpimiPostingBuffer(uint64_t hash_seed) : _hash_seed(hash_seed) {}

SpimiPostingBuffer::SpimiPostingBuffer(uint64_t hash_seed, Limits limits)
        : _hash_seed(hash_seed), _limits(limits) {}

uint64_t SpimiPostingBuffer::HashTerm(std::string_view term) const {
    // Keyed FNV-1a 64-bit: the per-instance seed is used as the initial
    // accumulator (replacing FNV's fixed offset basis), so an attacker who
    // does not know the seed cannot pre-compute colliding terms across
    // writers. The construction is not cryptographically keyed — it is
    // DoS-resistant, which is all we need: the attacker would have to find
    // collisions under an unknown seed, which is computationally equivalent
    // to a brute-force search of the 64-bit seed space.
    //
    // We intentionally avoid a stronger keyed hash (xxhash, SipHash) here
    // because the symbol resolution path for xxhash in the BE test binary
    // pulls two archives that both define `XXH64_*`, breaking the static
    // link. Keyed FNV is small, embedded, and good enough for the threat
    // model. If a stronger keyed hash is needed later, route through
    // util/hash_util's existing wrappers rather than `#include <xxhash.h>`.
    constexpr uint64_t kPrime = 1099511628211ULL;
    uint64_t h = _hash_seed;
    for (unsigned char c : term) {
        h ^= static_cast<uint64_t>(c);
        h *= kPrime;
    }
    return h;
}

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
    std::vector<uint32_t> new_slot_term_ids(
            _slot_term_ids.empty() ? 0 : new_capacity, kInvalidTermId);
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

uint32_t SpimiPostingBuffer::Intern(std::string_view term) {
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
            const uint32_t text_ref = static_cast<uint32_t>(_arena.size());
            const uint32_t len = static_cast<uint32_t>(term.size());
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
                // `_last_intern_slot` is now stale — caller should
                // re-resolve via `_text_ref_to_term_id` for this
                // single insertion (cold path).
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

namespace {

// Override std::vector's 2× growth with 1.25× on a byte buffer,
// matching the `_records.reserve(...)` pattern. Capacity is only
// bumped when truly full, NOT pre-emptively per write.
//
// Hot-loop note: callers should prefer `ReserveBytes` once + the
// unchecked `PushByteUnchecked` for sequences of pushes
// (e.g. WriteVInt's 1-5 bytes). The slow-growth variant remains for
// one-off calls and as the safety net when a caller forgets to
// reserve.
inline void EnsureCapacity(std::vector<uint8_t>* buf, size_t additional) {
    const size_t needed = buf->size() + additional;
    if (needed > buf->capacity()) [[unlikely]] {
        const size_t grown =
                std::max(needed, buf->capacity() + buf->capacity() / 4 + 16);
        buf->reserve(grown);
    }
}

inline void PushByteUnchecked(std::vector<uint8_t>* buf, uint8_t b) {
    // Caller must have called `EnsureCapacity` with enough headroom.
    // `push_back` is still used (not raw pointer write) because the
    // vector tracks size internally; in release the `if (capacity)`
    // branch inside push_back is dead given our reserve, so the
    // compiler reduces this to a `*end = b; ++end` pair.
    buf->push_back(b);
}

// VInt writer matching `ByteOutput::WriteVInt` byte layout. Used by
// the compact-mode posting streams so the encoding is consistent
// with how the rest of SPIMI handles variable-length ints.
// A uint32 VInt is at most 5 bytes; reserve all 5 upfront so the
// inner loop is branch-free on capacity. (Previously each
// `PushByteWithSlowGrowth` re-read size/capacity and branched —
// hot enough to surface in the flamegraph as `PushByteWithSlowGrowth`
// at ~277 M samples on the repetitive workload.)
inline void WriteVIntToBuf(std::vector<uint8_t>* buf, uint32_t v) {
    EnsureCapacity(buf, 5U);
    while ((v & ~0x7FU) != 0) {
        PushByteUnchecked(buf, static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    PushByteUnchecked(buf, static_cast<uint8_t>(v));
}

inline uint32_t ReadVIntFromBuf(const uint8_t* data, size_t* pos) {
    uint32_t v = 0;
    uint32_t shift = 0;
    while (true) {
        const uint8_t b = data[(*pos)++];
        v |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
    }
    return v;
}

} // namespace

uint32_t SpimiPostingBuffer::GetOrAssignTermId(uint32_t text_ref) {
    // Hot path: Intern() just touched a slot and left `_last_intern_slot`
    // pointing at it AND the slot's text_ref equals the queried `text_ref`.
    // The parallel `_slot_term_ids` array then gives the term_id in O(1)
    // — no hashmap probe.
    //
    // The `_slots[_last_intern_slot] == text_ref` check is LOAD-BEARING.
    // Without it the function silently returns whatever term_id sits at
    // the stale slot, collapsing distinct terms onto term_id 0. Two paths
    // exercise this: (a) `MaybeCompact`'s migration loop walks `_records`
    // calling GetOrAssignTermId without first calling Intern per record,
    // so `_last_intern_slot` points at the last-Append'd term, not the
    // record's term; (b) any future caller that forgets the implicit
    // "you must Intern with this exact text_ref first" contract.
    if (_last_intern_slot != static_cast<size_t>(-1) &&
        _last_intern_slot < _slot_term_ids.size() &&
        _slots[_last_intern_slot] == text_ref) [[likely]] {
        uint32_t id = _slot_term_ids[_last_intern_slot];
        if (id == kInvalidTermId) {
            id = static_cast<uint32_t>(_term_states.size());
            _term_states.emplace_back();
            _slot_term_ids[_last_intern_slot] = id;
            _text_ref_to_term_id.emplace(text_ref, id);
        }
        return id;
    }
    // Cold fallback: text_ref doesn't match the last-Intern'd slot, OR
    // _last_intern_slot is invalidated (post-GrowSlots, MaybeCompact
    // migration loop). The hashmap is the source of truth.
    auto it = _text_ref_to_term_id.find(text_ref);
    if (it != _text_ref_to_term_id.end()) {
        return it->second;
    }
    const auto id = static_cast<uint32_t>(_term_states.size());
    _term_states.emplace_back();
    _text_ref_to_term_id.emplace(text_ref, id);
    // Also populate `_slot_term_ids` for THIS text_ref's slot, so the
    // next Append on this same term hits the fast path. Slot is found
    // by re-probing the intern table — same hash + linear probe as
    // Intern. Cheap on miss (executed once per first-sight term in
    // cold mode, ≪ Append frequency).
    if (!_slots.empty()) {
        const size_t mask = _slots.size() - 1;
        const std::string_view term = ArenaTermAt(text_ref);
        size_t slot = HashTerm(term) & mask;
        while (true) {
            const uint32_t entry = _slots[slot];
            if (entry == text_ref) {
                _slot_term_ids[slot] = id;
                break;
            }
            if (entry == kEmpty) {
                break;  // text_ref not actually interned — shouldn't
                        // happen but defensively bail rather than loop
                        // forever.
            }
            slot = (slot + 1) & mask;
        }
    }
    return id;
}

void SpimiPostingBuffer::EncodeOccurrenceToStream(uint32_t term_id, uint32_t doc_id,
                                                  uint32_t position) {
    // Tagged-VInt delta encoding with absolute-reset fallback.
    // Per occurrence we write one tagged VInt and optionally one
    // trailing VInt. The encoding handles arbitrary input order
    // (out-of-order docs, non-monotonic positions) by emitting a
    // reset sentinel — so the random-input
    // `SpimiSegmentReaderTest.RandomisedSegmentRoundTrip` test
    // stays green while monotonic fulltext workloads (the common
    // case) get the delta-encoded compactness win.
    //
    // Tagged VInt encoding (lowest bit is the tag):
    //   tagged == 0          → absolute reset: read VInt(doc) +
    //                          VInt(pos). Used on first occurrence
    //                          and whenever doc < last_doc.
    //   tagged & 1 == 1      → same-doc continuation: pos_delta =
    //                          tagged >> 1 (no trailing VInt).
    //   tagged & 1 == 0,
    //   tagged != 0          → new-doc: doc_delta = tagged >> 1,
    //                          then VInt(pos).
    auto& state = _term_states[term_id];
    auto& s = state.stream;

    if (state.first || doc_id < state.last_doc) {
        if (doc_id < state.last_doc) {
            _compact_streams_sorted = false;
        }
        WriteVIntToBuf(&s, 0U); // reset sentinel
        WriteVIntToBuf(&s, doc_id);
        WriteVIntToBuf(&s, position);
    } else if (doc_id == state.last_doc) {
        // Same-doc continuation. Position may be < last_pos under
        // adversarial inputs (the caller does not enforce monotonic
        // within-doc positions), so guard with the same reset
        // fallback as the doc-decreasing case rather than emitting a
        // wrapped delta.
        if (position < state.last_pos_in_doc) {
            _compact_streams_sorted = false;
            WriteVIntToBuf(&s, 0U);
            WriteVIntToBuf(&s, doc_id);
            WriteVIntToBuf(&s, position);
        } else {
            const uint32_t pos_delta = position - state.last_pos_in_doc;
            WriteVIntToBuf(&s, (pos_delta << 1) | 1U);
        }
    } else {
        // doc_id > last_doc → new-doc with delta.
        const uint32_t doc_delta = doc_id - state.last_doc;
        WriteVIntToBuf(&s, doc_delta << 1);
        WriteVIntToBuf(&s, position);
    }
    state.last_doc = doc_id;
    state.last_pos_in_doc = position;
    state.first = false;
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
    // Migrate all records into per-term streams. Records are walked
    // in insertion order so the stream's delta-encoding sees the
    // same (doc, pos) sequence Append() would have produced
    // directly.
    _term_states.reserve(_term_count);
    _text_ref_to_term_id.reserve(_term_count);
    // Initialize the parallel `_slot_term_ids` array now that we're
    // entering compact mode — see `Intern()` for the deferred-init
    // rationale. The slot table itself (`_slots`) is already
    // populated from the flat-mode Appends; we just need a
    // term_id-per-slot companion that the post-migration Append hot
    // path can read in O(1).
    _slot_term_ids.assign(_slots.size(), kInvalidTermId);
    // Force the cold path during migration: `_last_intern_slot` is
    // stale (points at the last flat-mode Append's term) and would
    // mis-route records onto term_id 0 (P51 corruption bug). The
    // cold path uses the hashmap (correct) and ALSO populates
    // `_slot_term_ids[slot]` via re-probing, so post-migration
    // Appends still hit the fast path.
    _last_intern_slot = static_cast<size_t>(-1);
    for (const auto& rec : _records) {
        const uint32_t term_id = GetOrAssignTermId(rec.text_ref);
        EncodeOccurrenceToStream(term_id, rec.doc_id, rec.position);
    }
    _records.clear();
    _records.shrink_to_fit();
    _compact_mode = true;
}

void SpimiPostingBuffer::Append(std::string_view term, uint32_t doc_id, uint32_t position) {
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
    // The Intern() path can grow the arena by up to (4 + term.size()) bytes
    // on a first-sight term. Check before the cast in Intern() so we
    // never reach a state where `_arena.size() >= 2^32`.
    if (_arena.size() + sizeof(uint32_t) + term.size() > _limits.max_arena_bytes) {
        Saturate("arena would exceed max_arena_bytes", term);
        return;
    }
    const uint32_t text_ref = Intern(term);
    ++_total_occurrences;

    if (_compact_mode) {
        // Compact mode: write directly to the term's posting stream.
        // No record vector push, no growth bookkeeping. The per-term
        // varint stream is what keeps peak memory low on repetitive
        // workloads.
        const uint32_t term_id = GetOrAssignTermId(text_ref);
        EncodeOccurrenceToStream(term_id, doc_id, position);
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
}

size_t SpimiPostingBuffer::MemoryUsage() const {
    // `_slot_term_ids` is allocated lazily — only populated when
    // compact mode activates (see `MaybeCompact`). In flat mode
    // it stays empty so the slot-table memory is just `_slots`.
    size_t bytes = _arena.capacity() + _slots.capacity() * sizeof(uint32_t) +
                   _slot_term_ids.capacity() * sizeof(uint32_t);
    bytes += _records.capacity() * sizeof(SpimiRecord);
    if (_compact_mode) {
        // Per-term stream capacities + vector<TermPostingState>
        // backing array overhead + map slots. Map slot bytes are
        // approximated as 2× live entries × (sizeof(key)+sizeof(value)+
        // pointer for the bucket), matching libstdc++'s typical load
        // factor and bucket layout closely enough for accounting.
        bytes += _term_states.capacity() * sizeof(TermPostingState);
        for (const auto& s : _term_states) {
            bytes += s.stream.capacity();
        }
        bytes += _text_ref_to_term_id.size() * (sizeof(uint32_t) * 2 + sizeof(void*) * 2);
    }
    return bytes;
}

void SpimiPostingBuffer::Reset() {
    _records.clear();
    _arena.clear();
    // Keep the slot table allocated and re-zero it in place rather than
    // shrinking to empty. The previous Reset() cleared `_slots`, which
    // forced the next Append() to re-allocate the slot table from scratch
    // every time the buffer was reused across segments — defeating the
    // "keeps allocated capacity for reuse" contract advertised on Reset.
    std::fill(_slots.begin(), _slots.end(), kEmpty);
    std::fill(_slot_term_ids.begin(), _slot_term_ids.end(), kInvalidTermId);
    _term_count = 0;
    _saturated = false;
    _last_intern_slot = static_cast<size_t>(-1);
    // Compact-mode state: drop streams + maps but don't keep them
    // around — they're heavy and a reused buffer should start over
    // with the flat-records fast path.
    _term_states.clear();
    _term_states.shrink_to_fit();
    _text_ref_to_term_id.clear();
    _cached_text_ref = static_cast<uint32_t>(-1);
    _compact_mode = false;
    _compact_streams_sorted = true;
    _total_occurrences = 0;
}

void SpimiPostingBuffer::Sort() {
    if (_compact_mode) {
        _records.clear();
        _records.reserve(_total_occurrences);

        // Fast path: streams were filled monotonically (no reset
        // sentinel was ever emitted). Each stream's decoded records
        // are then already in (doc, pos) order. We can emit per-term
        // contiguously in TEXT-SORTED term order — the global
        // `stable_sort` below becomes a no-op and we save the
        // O(N log N) work entirely. On the 12K-occurrence repetitive
        // bench this drops `finish` from ~16 ms to ~2 ms (matches
        // CLucene's finish time).
        const bool can_skip_global_sort = _compact_streams_sorted;
        std::vector<std::pair<uint32_t, uint32_t>> term_text_to_id; // (text_ref, term_id)
        term_text_to_id.reserve(_text_ref_to_term_id.size());
        for (const auto& [text_ref, term_id] : _text_ref_to_term_id) {
            term_text_to_id.emplace_back(text_ref, term_id);
        }
        if (can_skip_global_sort) {
            std::sort(term_text_to_id.begin(), term_text_to_id.end(),
                      [this](const auto& a, const auto& b) {
                          return ArenaTermAt(a.first) < ArenaTermAt(b.first);
                      });
        }
        for (const auto& [text_ref, term_id] : term_text_to_id) {
            const auto& s = _term_states[term_id].stream;
            const uint8_t* data = s.data();
            size_t pos = 0;
            uint32_t cur_doc = 0;
            uint32_t cur_pos = 0;
            while (pos < s.size()) {
                const uint32_t tagged = ReadVIntFromBuf(data, &pos);
                if (tagged == 0U) {
                    cur_doc = ReadVIntFromBuf(data, &pos);
                    cur_pos = ReadVIntFromBuf(data, &pos);
                } else if ((tagged & 1U) != 0U) {
                    cur_pos += (tagged >> 1U);
                } else {
                    cur_doc += (tagged >> 1U);
                    cur_pos = ReadVIntFromBuf(data, &pos);
                }
                _records.push_back(SpimiRecord {text_ref, cur_doc, cur_pos});
            }
        }
        _term_states.clear();
        _term_states.shrink_to_fit();
        _text_ref_to_term_id.clear();
        _cached_text_ref = static_cast<uint32_t>(-1);
        _compact_mode = false;
        if (can_skip_global_sort) {
            // Records are now in (term-text, doc, pos) order without
            // a global sort. Return early; the stable_sort below is
            // only needed when streams contained reset sentinels.
            return;
        }
    }
    // The intern map dedups terms, so identical terms always share a single
    // text_ref. That means term *identity* can be tested by `text_ref ==
    // text_ref` (fast path), and term *order* falls back to a byte-wise
    // compare of the arena entries when refs differ.
    //
    // We use std::stable_sort so records that share the full key
    // (term, doc_id, position) keep their original insertion order — useful
    // for reproducibility and for the segment-emit phase, which assumes ties
    // are deterministic.
    std::stable_sort(_records.begin(), _records.end(),
                     [this](const SpimiRecord& a, const SpimiRecord& b) {
                         if (a.text_ref != b.text_ref) {
                             const std::string_view ta = ArenaTermAt(a.text_ref);
                             const std::string_view tb = ArenaTermAt(b.text_ref);
                             const int c = ta.compare(tb);
                             // Distinct text_refs cannot encode equal bytes
                             // because Intern() dedups; assert that invariant
                             // and dispatch strictly on the byte comparison.
                             DCHECK_NE(c, 0);
                             return c < 0;
                         }
                         if (a.doc_id != b.doc_id) {
                             return a.doc_id < b.doc_id;
                         }
                         return a.position < b.position;
                     });
}

} // namespace doris::segment_v2::inverted_index::spimi
