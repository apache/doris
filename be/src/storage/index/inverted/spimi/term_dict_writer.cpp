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

#include "storage/index/inverted/spimi/term_dict_writer.h"

#include "common/exception.h"
#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

int32_t SharedPrefixLength(const std::wstring& a, const std::wstring& b) {
    const auto limit = static_cast<int32_t>(std::min(a.size(), b.size()));
    int32_t i = 0;
    while (i < limit && a[i] == b[i]) {
        ++i;
    }
    return i;
}

int32_t CompareTermsByField(int32_t prev_field, const std::wstring& prev_term, int32_t field,
                            const std::wstring& term) {
    if (prev_field != field) {
        // We only support a single field per writer for now (Doris column
        // indexes are field-scoped), but preserve the convention: a newly
        // seen field must compare strictly greater than -1.
        return prev_field < field ? -1 : 1;
    }
    if (prev_term < term) {
        return -1;
    }
    if (prev_term > term) {
        return 1;
    }
    return 0;
}

} // namespace

TermDictWriter::TermDictWriter(ByteOutput* tis_out, ByteOutput* tii_out, int32_t index_interval,
                               int32_t skip_interval, bool inline_enabled)
        : _tis_out(tis_out),
          _tii_out(tii_out),
          _index_interval(index_interval),
          _skip_interval(skip_interval),
          _inline_enabled(inline_enabled) {
    DCHECK(_tis_out != nullptr);
    DCHECK(_tii_out != nullptr);
    DCHECK_EQ(_tis_out->FilePointer(), 0);
    DCHECK_EQ(_tii_out->FilePointer(), 0);
    DCHECK_GT(_index_interval, 0);
    DCHECK_GT(_skip_interval, 0);

    WriteHeader(_tis_out);
    WriteHeader(_tii_out);
}

void TermDictWriter::WriteHeader(ByteOutput* out) const {
    out->WriteInt(_inline_enabled ? kFormatInline : kFormat);
    out->WriteLong(-1);
    out->WriteInt(_index_interval);
    out->WriteInt(_skip_interval);
    out->WriteInt(kMaxSkipLevels);
}

void TermDictWriter::Add(int32_t field_number, std::string_view term_utf8, const TermInfo& info) {
    DCHECK(!_closed) << "TermDictWriter::Add called after Close()";
    DCHECK_GE(info.freq_pointer, _last_tis_info.freq_pointer) << "freqPointer out of order";
    DCHECK_GE(info.prox_pointer, _last_tis_info.prox_pointer) << "proxPointer out of order";

    const std::wstring term_wide = Utf8ToWide(term_utf8);

    // Strict ordering check against the previous .tis entry (skip on the
    // first call when _last_tis_field is sentinel -1).
    if (_last_tis_field != -1) {
        const int32_t cmp =
                CompareTermsByField(_last_tis_field, _last_tis_term, field_number, term_wide);
        DCHECK_LT(cmp, 0) << "Terms must be added in strictly ascending order";
    }

    // If we are at an indexInterval boundary, copy the *previous* .tis entry
    // into the .tii sparse index — exactly mirroring CLucene's behaviour.
    // At the very first Add() the "previous" entry is the empty sentinel
    // (field=-1, term="", info={0,0,0,0}); CLucene records it so the .tii's
    // first entry's pointer marks the start of .tis content. We keep the
    // same convention. The .tii sparse-index entry is NEVER inlined (it only
    // anchors binary search), so the previous entry's stored info — even an
    // inline one — is written pointer-form into .tii.
    if (_tis_size % _index_interval == 0) {
        WriteEntry(Stream::Tii, _last_tis_field, _last_tis_term, _last_tis_info);
    }

    WriteEntry(Stream::Tis, field_number, term_wide, info);
}

void TermDictWriter::AddInline(int32_t field_number, std::string_view term_utf8,
                               const TermInfo& info, const uint8_t* frq_bytes, uint32_t frq_len,
                               const uint8_t* prx_bytes, uint32_t prx_len) {
    DCHECK(!_closed) << "TermDictWriter::AddInline called after Close()";
    DCHECK(_inline_enabled) << "AddInline on a non-inline writer";
    // Inline terms contribute no pointer delta: the running last-pointer state
    // must stay unchanged so the next external term's delta is correct. We do
    // NOT compare info.freq_pointer/prox_pointer against _last_tis_info here.
    DCHECK_LE(frq_len, kInlineHardCapBytes) << "inline .frq exceeds hard cap";
    DCHECK_LE(prx_len, kInlineHardCapBytes) << "inline .prx exceeds hard cap";

    const std::wstring term_wide = Utf8ToWide(term_utf8);

    if (_last_tis_field != -1) {
        const int32_t cmp =
                CompareTermsByField(_last_tis_field, _last_tis_term, field_number, term_wide);
        DCHECK_LT(cmp, 0) << "Terms must be added in strictly ascending order";
    }

    if (_tis_size % _index_interval == 0) {
        WriteEntry(Stream::Tii, _last_tis_field, _last_tis_term, _last_tis_info);
    }

    InlinePayload payload;
    payload.frq = frq_bytes;
    payload.frq_len = frq_len;
    payload.prx = prx_bytes;
    payload.prx_len = prx_len;
    WriteEntry(Stream::Tis, field_number, term_wide, info, &payload);
}

void TermDictWriter::WriteEntry(Stream stream, int32_t field_number, const std::wstring& term_wide,
                                const TermInfo& info, const InlinePayload* inline_payload) {
    ByteOutput* out = (stream == Stream::Tis) ? _tis_out : _tii_out;
    const std::wstring& last_term = (stream == Stream::Tis) ? _last_tis_term : _last_tii_term;
    const TermInfo& last_info = (stream == Stream::Tis) ? _last_tis_info : _last_tii_info;
    // The .tii sparse index never inlines (it only anchors binary search), so
    // any inline_payload is for a .tis entry only.
    DCHECK(inline_payload == nullptr || stream == Stream::Tis);
    const bool inlined = (inline_payload != nullptr);

    WriteTerm(out, term_wide, last_term, field_number);

    // In inline format the doc_freq slot carries the inlined bit in its LSB.
    // In legacy (-4) format the doc_freq is written verbatim and `inlined`
    // is always false (no AddInline path), so this is back-compatible.
    if (_inline_enabled) {
        out->WriteVInt((info.doc_freq << 1) | (inlined ? 1 : 0));
    } else {
        out->WriteVInt(info.doc_freq);
    }

    if (inlined) {
        // Omit pointer deltas and skip_offset; emit the posting bytes verbatim.
        out->WriteVInt(static_cast<int32_t>(inline_payload->frq_len));
        if (inline_payload->frq_len > 0) {
            out->WriteBytes(inline_payload->frq, inline_payload->frq_len);
        }
        out->WriteVInt(static_cast<int32_t>(inline_payload->prx_len));
        if (inline_payload->prx_len > 0) {
            out->WriteBytes(inline_payload->prx, inline_payload->prx_len);
        }
    } else {
        out->WriteVLong(info.freq_pointer - last_info.freq_pointer);
        out->WriteVLong(info.prox_pointer - last_info.prox_pointer);
        if (info.doc_freq >= _skip_interval) {
            out->WriteVInt(info.skip_offset);
        }
    }

    if (stream == Stream::Tii) {
        // The .tii entry also stores the delta to the .tis byte offset of
        // the indexed term's start.
        const int64_t tis_pointer = _tis_out->FilePointer();
        _tii_out->WriteVLong(tis_pointer - _last_index_pointer);
        _last_index_pointer = tis_pointer;

        _last_tii_term = term_wide;
        _last_tii_field = field_number;
        _last_tii_info = info;
        ++_tii_size;
    } else {
        _last_tis_term = term_wide;
        _last_tis_field = field_number;
        if (inlined) {
            // Inline terms wrote NO freq/prox pointer delta, so the running
            // last-pointer state must stay anchored at the previous external
            // term's pointers — the next external term's delta is computed
            // against THAT. (Preserve last_info's pointers; the rest of
            // _last_tis_info is unused for delta computation.)
            // _last_tis_info is intentionally left unchanged here.
        } else {
            _last_tis_info = info;
        }
        ++_tis_size;
    }
}

void TermDictWriter::WriteTerm(ByteOutput* out, const std::wstring& term_wide,
                               const std::wstring& last_term_wide, int32_t field_number) {
    const int32_t start = SharedPrefixLength(term_wide, last_term_wide);
    const int32_t length = static_cast<int32_t>(term_wide.size()) - start;

    out->WriteVInt(start);
    out->WriteVInt(length);
    if (length > 0) {
        out->WriteSCharsFromWide(term_wide.data() + start, length);
    }
    out->WriteVInt(field_number);
}

void TermDictWriter::Close() {
    if (_closed) {
        return;
    }
    _closed = true;

    // Zero-term segments (e.g. a column whose values are all NULL or
    // empty under V4 — production-grade case discovered by the cloud
    // regression test) reach Close() with `_tii_size == 0`. The
    // reader's hardening (term_dict_reader.cpp:245) rejects a
    // tii_size == 0 footer to defeat crafted-segment OOB attacks,
    // so an honest empty segment would crash the read side.
    // Emit the sentinel TII entry (field=-1, term="", info={0,0,0,0},
    // tis_pointer=tii_data_start) so the .tii always carries at
    // least the sentinel that the reader's "first entry is the
    // sentinel" invariant requires. The .tis stays length-0.
    if (_tii_size == 0) {
        WriteEntry(Stream::Tii, _last_tis_field, _last_tis_term, _last_tis_info);
    }

    // .tis: writeLong(size). .tii: writeLong(size) + writeLong(tisSize).
    _tis_out->WriteLong(_tis_size);
    _tii_out->WriteLong(_tii_size);
    _tii_out->WriteLong(_tis_size);
}

} // namespace doris::segment_v2::inverted_index::spimi
