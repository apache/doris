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

#include "storage/index/inverted/spimi/term_dict_reader.h"

#include <algorithm>

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Stateful cursor over a `.tis` / `.tii` byte buffer. Inverses the
// `ByteOutput::Write*` primitives byte-for-byte.
class ByteCursor {
public:
    ByteCursor(const uint8_t* data, size_t len, size_t pos = 0)
            : _data(data), _len(len), _pos(pos) {}

    uint8_t ReadByte() {
        // Hard-fail on untrusted-byte underflow. The previous
        // DCHECK was a no-op in release builds, letting a
        // truncated/malformed `.tis` or `.tii` walk arbitrary heap
        // beyond `_len`. CLuceneError lands at the search-path
        // catch in `FullTextIndexReader::query` /
        // `SpimiSearcherBuilder::build` which converts it to
        // `INVERTED_INDEX_FILE_CORRUPTED`.
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis/.tii read past end of buffer");
        }
        return _data[_pos++];
    }
    int32_t ReadInt32BE() {
        const uint32_t b0 = ReadByte();
        const uint32_t b1 = ReadByte();
        const uint32_t b2 = ReadByte();
        const uint32_t b3 = ReadByte();
        return static_cast<int32_t>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
    }
    int64_t ReadInt64BE() {
        const uint64_t hi = static_cast<uint32_t>(ReadInt32BE());
        const uint64_t lo = static_cast<uint32_t>(ReadInt32BE());
        return static_cast<int64_t>((hi << 32) | lo);
    }
    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = ReadByte();
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
            // Bound shift to defeat crafted-bytes UB. See
            // term_docs_reader.cpp ReadVInt comment.
            if (shift >= 32U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .tis/.tii VInt: shift overflow on crafted input");
            }
        }
        return static_cast<int32_t>(v);
    }
    int64_t ReadVLong() {
        uint64_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = ReadByte();
            v |= static_cast<uint64_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
            // `<< shift` is UB on uint64 when shift >= 64. A crafted
            // .tis with ≥10 continuation bytes would otherwise drive
            // shift past 64 (e.g. 70, 77) — bound here.
            if (shift >= 64U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .tis/.tii VLong: shift overflow on crafted input");
            }
        }
        return static_cast<int64_t>(v);
    }
    // Inverse of `ByteOutput::WriteSCharsFromWide`. Decodes
    // `length` schars from the stream into a wide string. The 4-byte
    // branch is the unusual modified form CLucene uses (lead byte
    // 0x80..0x84 with a high bit set rather than the 0xF0.. prefix
    // standard UTF-8 expects); see `byte_output.cpp:64` for the
    // writer side. The reader detects it by elimination: a byte with
    // the high bit set that is not 110xxxxx / 1110xxxx / 11110xxx
    // must be a modified-4-byte lead.
    std::wstring ReadSChars(int32_t length) {
        std::wstring out;
        out.reserve(static_cast<size_t>(length));
        for (int32_t i = 0; i < length; ++i) {
            const uint8_t b0 = ReadByte();
            uint32_t code = 0;
            if ((b0 & 0x80U) == 0) {
                code = b0;
            } else if ((b0 & 0xE0U) == 0xC0U) {
                const uint8_t b1 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x1FU) << 6) | (b1 & 0x3FU);
            } else if ((b0 & 0xF0U) == 0xE0U) {
                const uint8_t b1 = ReadByte();
                const uint8_t b2 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x0FU) << 12) |
                       (static_cast<uint32_t>(b1 & 0x3FU) << 6) | (b2 & 0x3FU);
            } else if ((b0 & 0xF8U) == 0xF0U) {
                // Standard 4-byte UTF-8; the SPIMI writer never
                // emits this branch (it always takes the modified
                // 4-byte form below), but accept it for safety.
                const uint8_t b1 = ReadByte();
                const uint8_t b2 = ReadByte();
                const uint8_t b3 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x07U) << 18) |
                       (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                       (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
            } else {
                // Modified-4-byte form: lead = 0x80 | (code >> 18),
                // continuations 0x80|x for the other three groups.
                DCHECK_EQ(b0 & 0xC0U, 0x80U)
                        << "schar lead byte 0x" << std::hex << static_cast<int>(b0)
                        << " not a recognised SChar form";
                const uint8_t b1 = ReadByte();
                const uint8_t b2 = ReadByte();
                const uint8_t b3 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x7FU) << 18) |
                       (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                       (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
            }
            out.push_back(static_cast<wchar_t>(code));
        }
        return out;
    }
    size_t pos() const { return _pos; }
    // Advances the cursor by `n` bytes. Hard-fails on overrun (the caller
    // bounds-checks `n` first, but this is defence-in-depth).
    void Skip(size_t n) {
        if (n > _len - std::min(_pos, _len)) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis/.tii skip past end of buffer");
        }
        _pos += n;
    }

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos;
};

// Decodes one term-dictionary entry into the running state. Used for
// both .tis and .tii — they share the per-entry layout up to the
// optional trailing tis_pointer_delta (.tii only).
struct EntryState {
    int32_t field_number = -1;
    std::wstring term_wide;
    TermInfo info {}; // accumulates absolute freq_pointer / prox_pointer
};

// Decodes one entry. When `inline_format` is true the doc_freq slot carries
// the inlined bit in its LSB; an inlined entry omits the pointer deltas /
// skip_offset and instead carries VInt(frq_len) frq_bytes VInt(prx_len)
// prx_bytes. `buf_base` is the start of the underlying buffer so inline spans
// can be recorded as borrowed pointers into it (valid for the buffer's
// lifetime). For .tii entries (which never inline) `inline_format` may still
// be true (the -5 widened-doc_freq encoding) but the inlined bit is always 0.
void DecodeEntry(ByteCursor& cur, int32_t skip_interval, bool inline_format,
                 const uint8_t* buf_base, size_t buf_len, EntryState& state) {
    const int32_t prefix = cur.ReadVInt();
    const int32_t suffix = cur.ReadVInt();
    // Validate VInt-decoded lengths against attacker-controllable
    // bounds before using them to resize / read. Negative values
    // (high-bit-set varints) and prefixes exceeding the running
    // term width would silently produce huge size_t casts in
    // release builds.
    if (prefix < 0 || suffix < 0 || static_cast<size_t>(prefix) > state.term_wide.size())
            [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis: malformed term prefix/suffix length");
    }
    state.term_wide.resize(static_cast<size_t>(prefix));
    if (suffix > 0) {
        state.term_wide.append(cur.ReadSChars(suffix));
    }
    state.field_number = cur.ReadVInt();

    // Reset inline span state every entry (the running EntryState is reused
    // across the .tii walk and the .tis forward scan).
    state.info.inlined = false;
    state.info.inline_frq = nullptr;
    state.info.inline_frq_len = 0;
    state.info.inline_prx = nullptr;
    state.info.inline_prx_len = 0;

    bool inlined = false;
    if (inline_format) {
        const int32_t raw = cur.ReadVInt();
        // raw was written as (doc_freq << 1) | inlined_bit; doc_freq is
        // non-negative so raw is too. Guard against a crafted negative.
        if (raw < 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis: negative inline doc_freq word");
        }
        state.info.doc_freq = raw >> 1;
        inlined = (raw & 1) != 0;
    } else {
        state.info.doc_freq = cur.ReadVInt();
    }
    if (state.info.doc_freq < 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis: negative doc_freq");
    }

    if (inlined) {
        // Inline entry: the freq/prox pointer deltas are OMITTED, so the
        // running freq_pointer/prox_pointer accumulators MUST stay unchanged
        // (the writer kept them anchored at the previous external term).
        const int32_t frq_len = cur.ReadVInt();
        if (frq_len < 0 || static_cast<size_t>(frq_len) > buf_len - std::min(cur.pos(), buf_len))
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis: inline frq_len out of bounds");
        }
        const size_t frq_off = cur.pos();
        cur.Skip(static_cast<size_t>(frq_len));
        const int32_t prx_len = cur.ReadVInt();
        if (prx_len < 0 || static_cast<size_t>(prx_len) > buf_len - std::min(cur.pos(), buf_len))
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis: inline prx_len out of bounds");
        }
        const size_t prx_off = cur.pos();
        cur.Skip(static_cast<size_t>(prx_len));

        state.info.inlined = true;
        state.info.inline_frq = (frq_len > 0) ? buf_base + frq_off : nullptr;
        state.info.inline_frq_len = static_cast<uint32_t>(frq_len);
        state.info.inline_prx = (prx_len > 0) ? buf_base + prx_off : nullptr;
        state.info.inline_prx_len = static_cast<uint32_t>(prx_len);
        state.info.skip_offset = 0;
        return;
    }

    state.info.freq_pointer += cur.ReadVLong();
    state.info.prox_pointer += cur.ReadVLong();
    if (state.info.doc_freq >= skip_interval) {
        state.info.skip_offset = cur.ReadVInt();
    } else {
        state.info.skip_offset = 0;
    }
}

int CompareEntry(int32_t a_field, const std::wstring& a_term, int32_t b_field,
                 const std::wstring& b_term) {
    if (a_field != b_field) {
        return a_field < b_field ? -1 : 1;
    }
    if (a_term < b_term) {
        return -1;
    }
    if (a_term > b_term) {
        return 1;
    }
    return 0;
}

} // namespace

size_t TermDictReader::DecodeHeader(const std::vector<uint8_t>& bytes, int32_t* index_interval,
                                    int32_t* skip_interval, int32_t* format) {
    // Header must fit; untrusted-byte hard-fail.
    if (bytes.size() < 24U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis/.tii header too short");
    }
    ByteCursor cur(bytes.data(), bytes.size());
    const int32_t fmt = cur.ReadInt32BE();
    if (fmt != TermDictWriter::kFormat && fmt != TermDictWriter::kFormatInline) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis/.tii FORMAT mismatch");
    }
    *format = fmt;
    [[maybe_unused]] const int64_t legacy_size = cur.ReadInt64BE(); // always -1
    *index_interval = cur.ReadInt32BE();
    *skip_interval = cur.ReadInt32BE();
    [[maybe_unused]] const int32_t max_skip_levels = cur.ReadInt32BE();
    return cur.pos();
}

TermDictReader::TermDictReader(const std::vector<uint8_t>& tis_bytes,
                               const std::vector<uint8_t>& tii_bytes)
        : _tis_bytes(tis_bytes) {
    int32_t tii_index_interval = 0;
    int32_t tii_skip_interval = 0;
    int32_t tii_format = 0;
    int32_t tis_format = 0;
    const size_t tii_data_start =
            DecodeHeader(tii_bytes, &tii_index_interval, &tii_skip_interval, &tii_format);
    _tis_data_start = DecodeHeader(_tis_bytes, &_index_interval, &_skip_interval, &tis_format);
    if (tii_index_interval != _index_interval || tii_skip_interval != _skip_interval) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tii / .tis index/skip interval mismatch");
    }
    if (tii_format != tis_format) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tii / .tis FORMAT mismatch");
    }
    _inline_format = (tis_format == TermDictWriter::kFormatInline);

    // `LookupTerm` computes `_tis_bytes.size() - 8U` to find the .tis
    // footer offset. Without bounding `_tis_bytes.size()` against the
    // 24-byte header (already required) PLUS the 8-byte footer, a
    // crafted .tis of length in [24, 31] passes `DecodeHeader` but the
    // `size() - 8U` subtraction underflows `size_t` and the resulting
    // ByteCursor reads arbitrary heap until a fortuitous `_CLTHROWA`.
    if (_tis_bytes.size() < _tis_data_start + 8U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis missing footer (size < header + 8)");
    }

    // Read .tii footer (.tii has two trailing int64: tii_size, tis_size).
    if (tii_bytes.size() < 16U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tii footer underflow");
    }
    {
        ByteCursor footer(tii_bytes.data(), tii_bytes.size(), tii_bytes.size() - 16U);
        const int64_t tii_size = footer.ReadInt64BE();
        _tis_size = footer.ReadInt64BE();
        if (tii_size < 1 || static_cast<uint64_t>(tii_size) > tii_bytes.size()) [[unlikely]] {
            // tii_size == 0 (or negative) would leave `_tii_entries`
            // empty; `LookupTerm` then OOBs on the sentinel-must-
            // exist invariant. Huge tii_size DOS'd via reserve().
            SPIMI_THROW_CORRUPT("SPIMI .tii size invalid (must be >= 1 and bounded)");
        }
        _tii_entries.reserve(static_cast<size_t>(tii_size));
    }

    // Walk .tii entries. Each entry inherits prefix/freq/prox state
    // from the previous one (matches the writer's WriteEntry deltas).
    // `tis_pointer` in the entry is reconstructed from the running
    // vlong delta sum, which always lands at the byte offset where
    // the next .tis entry will start.
    EntryState state;
    int64_t running_tis_pointer = 0;
    const size_t tii_end = tii_bytes.size() - 16U;
    ByteCursor cur(tii_bytes.data(), tii_bytes.size(), tii_data_start);
    while (cur.pos() < tii_end) {
        // .tii entries are never inlined (the sparse index always anchors with
        // pointer-form info), so even on a -5 segment the inlined bit is 0; we
        // still pass _inline_format so the widened doc_freq slot is decoded.
        DecodeEntry(cur, _skip_interval, _inline_format, tii_bytes.data(), tii_bytes.size(), state);
        running_tis_pointer += cur.ReadVLong();
        TiiEntry e;
        e.field_number = state.field_number;
        e.term_wide = state.term_wide;
        e.info = state.info;
        e.tis_pointer = running_tis_pointer;
        _tii_entries.push_back(std::move(e));
    }
    // Verify the decoded entry count matches the footer-declared
    // `tii_size`. Round-2 added the footer-side bound, but a crafted
    // `.tii` could still claim `tii_size = 5` and contain zero
    // entries, leaving `_tii_entries` empty — then `LookupTerm` →
    // `LowerBoundTiiEntry` returns `lo - 1 = SIZE_MAX` and
    // `_tii_entries[SIZE_MAX]` is heap OOB read in release.
    if (_tii_entries.empty()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tii decoded no entries — sentinel missing");
    }
    // Sentinel invariant: the first entry MUST be the writer's
    // sentinel (field=-1, term=""). Without this, the lo-1
    // underflow on a smaller-than-anything lookup is unguarded.
    if (_tii_entries[0].field_number != -1 || !_tii_entries[0].term_wide.empty()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tii sentinel entry malformed");
    }
}

size_t TermDictReader::LowerBoundTiiEntry(int32_t target_field,
                                          const std::wstring& target_term_wide) const {
    // std::upper_bound for (field, term) then step back by one to get
    // the largest entry that is <= target. The first entry is the
    // sentinel (field=-1, term=""), which is always <= any real
    // term, so the "step back" is well-defined.
    size_t lo = 0;
    size_t hi = _tii_entries.size();
    while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1U);
        const auto& e = _tii_entries[mid];
        if (CompareEntry(e.field_number, e.term_wide, target_field, target_term_wide) <= 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    DCHECK_GT(lo, 0U) << "binary search ended at index 0 — sentinel invariant violated";
    return lo - 1;
}

std::optional<TermInfo> TermDictReader::LookupTerm(int32_t field_number,
                                                   std::string_view term_utf8) const {
    const std::wstring target_term = Utf8ToWide(term_utf8);

    const size_t start_idx = LowerBoundTiiEntry(field_number, target_term);
    const auto& anchor = _tii_entries[start_idx];

    // The anchor itself stores a real indexed term's `(field, term, info)`
    // (except for the sentinel at index 0 with field=-1). The writer
    // records the .tii entry BEFORE writing the next .tis entry, so
    // anchor.tis_pointer is the start of the .tis entry that
    // immediately follows the anchor. The anchor's own term/info are
    // not reachable via the linear scan — they must be matched here
    // first.
    if (anchor.field_number == field_number && anchor.term_wide == target_term) {
        return anchor.info;
    }

    // If the anchor's tis_pointer is past .tis data, no terms follow.
    if (static_cast<size_t>(anchor.tis_pointer) >= _tis_bytes.size() - 8U /*footer*/) {
        return std::nullopt;
    }

    // Replay the anchor's state and scan forward at most
    // _index_interval .tis entries — the next .tii entry covers
    // anything beyond, but a target that lies between two .tii
    // entries must be found in this window.
    EntryState state;
    state.field_number = anchor.field_number;
    state.term_wide = anchor.term_wide;
    state.info = anchor.info;

    const size_t tis_data_len = _tis_bytes.size() - 8U;
    ByteCursor cur(_tis_bytes.data(), tis_data_len, static_cast<size_t>(anchor.tis_pointer));
    for (int32_t i = 0; i < _index_interval; ++i) {
        if (cur.pos() >= tis_data_len) {
            return std::nullopt;
        }
        DecodeEntry(cur, _skip_interval, _inline_format, _tis_bytes.data(), tis_data_len, state);
        const int cmp =
                CompareEntry(state.field_number, state.term_wide, field_number, target_term);
        if (cmp == 0) {
            return state.info;
        }
        if (cmp > 0) {
            // Scanned past target without finding it.
            return std::nullopt;
        }
    }
    return std::nullopt;
}

} // namespace doris::segment_v2::inverted_index::spimi
