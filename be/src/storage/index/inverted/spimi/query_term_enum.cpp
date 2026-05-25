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

#include "storage/index/inverted/spimi/query_term_enum.h"

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Inline byte cursor mirroring the one in `term_dict_reader.cpp`.
// Kept local rather than extracted to a shared header because each
// reader has slightly different needs (this one needs to be
// re-entrant via the enclosing class's `_pos` field, while
// TermDictReader uses a local stack instance).
class Cursor {
public:
    Cursor(const uint8_t* data, size_t len, size_t& pos) : _data(data), _len(len), _pos(pos) {}

    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            _CLTHROWA(CL_ERR_IO, "SPIMI .tis read past end of buffer");
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
        }
        return static_cast<int64_t>(v);
    }
    // Decodes `length` schars from the stream into `out` appended
    // form. See `term_dict_reader.cpp::ReadSChars` for the four
    // recognised forms and the "modified 4-byte" rationale.
    void ReadSCharsAppend(int32_t length, std::wstring* out) {
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
                const uint8_t b1 = ReadByte();
                const uint8_t b2 = ReadByte();
                const uint8_t b3 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x07U) << 18) |
                       (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                       (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
            } else {
                DCHECK_EQ(b0 & 0xC0U, 0x80U)
                        << "tis schar lead byte 0x" << std::hex << static_cast<int>(b0);
                const uint8_t b1 = ReadByte();
                const uint8_t b2 = ReadByte();
                const uint8_t b3 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x7FU) << 18) |
                       (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                       (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
            }
            out->push_back(static_cast<wchar_t>(code));
        }
    }

private:
    const uint8_t* _data;
    size_t _len;
    size_t& _pos;
};

} // namespace

SpimiQueryTermEnum::SpimiQueryTermEnum(const uint8_t* tis_data, size_t tis_length,
                                       int32_t skip_interval,
                                       std::vector<std::wstring> field_names_by_number)
        : _tis_data(tis_data),
          _skip_interval(skip_interval),
          _field_names_by_number(std::move(field_names_by_number)),
          _pos(0) {
    // Validate BEFORE the subtraction — `tis_length - 8U` would
    // wrap to a huge size_t on truncated input in release builds
    // (DCHECK is a no-op there). Throw a CLucene error so the
    // searcher-cache build path lands at INVERTED_INDEX_FILE_CORRUPTED.
    if (tis_length < 24U + 8U) {
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis too short (header + footer)");
    }
    _tis_length = tis_length - 8U;
    Init();
}

SpimiQueryTermEnum::~SpimiQueryTermEnum() {
    if (_current_term != nullptr) {
        _CLDECDELETE(_current_term);
        _current_term = nullptr;
    }
}

void SpimiQueryTermEnum::Init() {
    Cursor cur(_tis_data, _tis_length, _pos);
    const int32_t format = cur.ReadInt32BE();
    if (format != TermDictWriter::kFormat) [[unlikely]] {
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis FORMAT mismatch");
    }
    [[maybe_unused]] const int64_t legacy_size = cur.ReadInt64BE(); // -1
    [[maybe_unused]] const int32_t index_interval = cur.ReadInt32BE();
    const int32_t file_skip_interval = cur.ReadInt32BE();
    [[maybe_unused]] const int32_t max_skip_levels = cur.ReadInt32BE();
    if (file_skip_interval != _skip_interval) [[unlikely]] {
        // Cross-file consistency check: .tis carries its own
        // skipInterval which must match what we were told (typically
        // pulled from the .tii header by the upper layer).
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis skipInterval disagrees with caller");
    }
    _data_start = _pos;
}

bool SpimiQueryTermEnum::next() {
    if (_exhausted) {
        return false;
    }
    if (_pos >= _tis_length) {
        _exhausted = true;
        return false;
    }
    DecodeOne();
    return true;
}

void SpimiQueryTermEnum::DecodeOne() {
    Cursor cur(_tis_data, _tis_length, _pos);
    const int32_t prefix = cur.ReadVInt();
    const int32_t suffix = cur.ReadVInt();
    // Validate VInt lengths against the running term width AND
    // negative-overflow before using them to resize / read.
    if (prefix < 0 || suffix < 0 || static_cast<size_t>(prefix) > _current_term_text.size())
            [[unlikely]] {
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis: malformed term prefix/suffix length");
    }
    _current_term_text.resize(static_cast<size_t>(prefix));
    if (suffix > 0) {
        cur.ReadSCharsAppend(suffix, &_current_term_text);
    }
    _current_field = cur.ReadVInt();
    _current_info.doc_freq = cur.ReadVInt();
    _current_info.freq_pointer += cur.ReadVLong();
    _current_info.prox_pointer += cur.ReadVLong();
    if (_current_info.doc_freq < 0) [[unlikely]] {
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis: negative doc_freq");
    }
    if (_current_info.doc_freq >= _skip_interval) {
        _current_info.skip_offset = cur.ReadVInt();
    } else {
        _current_info.skip_offset = 0;
    }

    // Rebuild `_current_term` with the new (field_name, term_text).
    // We release the previous Term and construct a fresh one — the
    // straightforward approach. CLucene's `SegmentTermEnum` does a
    // prev/_term swap micro-optimisation; we skip that for clarity
    // because Term construction is cheap (no allocation beyond a
    // refcount control block plus the interned field-name handle).
    if (_current_field < 0 || static_cast<size_t>(_current_field) >= _field_names_by_number.size())
            [[unlikely]] {
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis: field_number out of range");
    }
    const wchar_t* field_name = _field_names_by_number[static_cast<size_t>(_current_field)].c_str();
    if (_current_term != nullptr) {
        _CLDECDELETE(_current_term);
    }
    _current_term = _CLNEW lucene::index::Term(field_name, _current_term_text.c_str());
}

lucene::index::Term* SpimiQueryTermEnum::term(bool pointer) {
    if (_current_term == nullptr) {
        return nullptr;
    }
    if (pointer) {
        return _CL_POINTER(_current_term);
    }
    return _current_term;
}

int32_t SpimiQueryTermEnum::docFreq() const {
    return _current_info.doc_freq;
}

void SpimiQueryTermEnum::close() {
    if (_current_term != nullptr) {
        _CLDECDELETE(_current_term);
        _current_term = nullptr;
    }
    _exhausted = true;
}

} // namespace doris::segment_v2::inverted_index::spimi
