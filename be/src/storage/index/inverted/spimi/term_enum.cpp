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

#include "storage/index/inverted/spimi/term_enum.h"

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Inverse of `Utf8ToWide` — converts a wide-char string back to
// standard UTF-8.  Handles codepoints up to U+10FFFF.  Characters
// above U+FFFF are emitted as standard 4-byte UTF-8 (NOT the
// modified CLucene form used on disk — this is for in-memory
// comparison / output only).
std::string WideToUtf8(const std::wstring& wide) {
    std::string out;
    out.reserve(wide.size());
    for (const wchar_t wc : wide) {
        const auto code = static_cast<uint32_t>(wc);
        if (code <= 0x7FU) {
            out.push_back(static_cast<char>(code));
        } else if (code <= 0x7FFU) {
            out.push_back(static_cast<char>(0xC0U | (code >> 6)));
            out.push_back(static_cast<char>(0x80U | (code & 0x3FU)));
        } else if (code <= 0xFFFFU) {
            out.push_back(static_cast<char>(0xE0U | (code >> 12)));
            out.push_back(static_cast<char>(0x80U | ((code >> 6) & 0x3FU)));
            out.push_back(static_cast<char>(0x80U | (code & 0x3FU)));
        } else if (code <= 0x10FFFFU) {
            out.push_back(static_cast<char>(0xF0U | (code >> 18)));
            out.push_back(static_cast<char>(0x80U | ((code >> 12) & 0x3FU)));
            out.push_back(static_cast<char>(0x80U | ((code >> 6) & 0x3FU)));
            out.push_back(static_cast<char>(0x80U | (code & 0x3FU)));
        } else {
            // Replacement character U+FFFD.
            out.push_back(static_cast<char>(0xEFU));
            out.push_back(static_cast<char>(0xBFU));
            out.push_back(static_cast<char>(0xBDU));
        }
    }
    return out;
}

// Byte-stream cursor matching the one in term_dict_reader.cpp.
// Duplicated here to avoid coupling the merge-path reader to the
// query-path reader's translation unit.
class Cursor {
public:
    Cursor(const uint8_t* data, size_t len, size_t pos = 0)
            : _data(data), _len(len), _pos(pos) {}

    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: read past end of buffer");
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
            if (shift >= 32U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum VInt: shift overflow");
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
            if (shift >= 64U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum VLong: shift overflow");
            }
        }
        return static_cast<int64_t>(v);
    }
    // Inverse of `ByteOutput::WriteSCharsFromWide` — identical logic
    // to the ReadSChars in term_dict_reader.cpp.
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
                const uint8_t b1 = ReadByte();
                const uint8_t b2 = ReadByte();
                const uint8_t b3 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x07U) << 18) |
                       (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                       (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
            } else {
                // Modified-4-byte form (CLucene-specific).
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

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos;
};

} // namespace

TermEnum::TermEnum(const std::vector<uint8_t>& tis_bytes) : _tis_bytes(tis_bytes) {
    // Minimum size: 24-byte header + 8-byte footer.
    if (tis_bytes.size() < 32U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: buffer too short");
    }

    Cursor cur(tis_bytes.data(), tis_bytes.size());

    // Header.
    const int32_t format = cur.ReadInt32BE();
    if (format != TermDictWriter::kFormat) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: FORMAT mismatch");
    }
    [[maybe_unused]] const int64_t legacy = cur.ReadInt64BE(); // always -1
    _index_interval = cur.ReadInt32BE();
    _skip_interval = cur.ReadInt32BE();
    [[maybe_unused]] const int32_t max_skip_levels = cur.ReadInt32BE();

    _pos = cur.pos();
    _data_end = tis_bytes.size() - 8U; // footer is the last 8 bytes

    // Footer: int64 size (number of .tis entries).
    Cursor footer(tis_bytes.data(), tis_bytes.size(), _data_end);
    _total_entries = footer.ReadInt64BE();
    if (_total_entries < 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: negative entry count");
    }

    // Zero-term segment: nothing to iterate.
    if (_total_entries == 0) {
        _done = true;
    }
}

bool TermEnum::Next() {
    if (_done || _consumed >= _total_entries) {
        _done = true;
        return false;
    }
    if (_pos >= _data_end) {
        _done = true;
        return false;
    }

    Cursor cur(_tis_bytes.data(), _data_end, _pos);

    // Front-coded term: prefix_len, suffix_len, suffix bytes, field_number.
    const int32_t prefix_len = cur.ReadVInt();
    const int32_t suffix_len = cur.ReadVInt();
    if (prefix_len < 0 || suffix_len < 0 ||
        static_cast<size_t>(prefix_len) > _last_term.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: malformed prefix/suffix length");
    }

    _last_term.resize(static_cast<size_t>(prefix_len));
    if (suffix_len > 0) {
        _last_term.append(cur.ReadSChars(suffix_len));
    }

    const int32_t field_number = cur.ReadVInt();
    const int32_t doc_freq = cur.ReadVInt();
    if (doc_freq < 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: negative doc_freq");
    }

    const int64_t freq_pointer_delta = cur.ReadVLong();
    const int64_t prox_pointer_delta = cur.ReadVLong();

    _last_freq_pointer += freq_pointer_delta;
    _last_prox_pointer += prox_pointer_delta;

    int32_t skip_offset = 0;
    if (doc_freq >= _skip_interval) {
        skip_offset = cur.ReadVInt();
    }

    _pos = cur.pos();

    // Populate output.
    _current.field_number = field_number;
    _current.term_utf8 = WideToUtf8(_last_term);
    _current.info.doc_freq = doc_freq;
    _current.info.freq_pointer = _last_freq_pointer;
    _current.info.prox_pointer = _last_prox_pointer;
    _current.info.skip_offset = skip_offset;

    ++_consumed;
    if (_consumed >= _total_entries || _pos >= _data_end) {
        _done = true;
    }
    return true;
}

} // namespace doris::segment_v2::inverted_index::spimi
