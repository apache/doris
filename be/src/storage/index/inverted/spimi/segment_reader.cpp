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

#include "storage/index/inverted/spimi/segment_reader.h"

// `_CLTHROWA` for byte-parser hard-fail on untrusted .fnm bytes.

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Inverse of `ByteOutput::Write*` over a byte buffer, scoped to
// `.fnm` parsing. The shared pattern matches `term_dict_reader.cpp`
// — we keep a separate copy here so neither file pulls in the
// other's internals.
class FnmCursor {
public:
    FnmCursor(const uint8_t* data, size_t len) : _data(data), _len(len) {}

    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .fnm read past end of buffer");
        }
        return _data[_pos++];
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
    std::wstring ReadSChars(int32_t length) {
        // Same logic as `term_dict_reader.cpp::ByteCursor::ReadSChars`.
        // See byte_output.cpp:64 for the writer's "modified 4-byte"
        // form (lead = `0x80 | (code >> 18)`).
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
                DCHECK_EQ(b0 & 0xC0U, 0x80U)
                        << ".fnm schar lead byte 0x" << std::hex << static_cast<int>(b0);
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
    bool AtEnd() const { return _pos >= _len; }

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos = 0;
};

// Inverse of `Utf8ToWide`. CLucene's modified UTF-8 stream encodes
// each wide char as the matching 1..4 byte sequence (with the
// modified 4-byte form noted above). Re-encoding to standard UTF-8
// for `name` field keeps the rest of Doris's strings interoperable.
std::string WideToUtf8(const std::wstring& wide) {
    std::string out;
    out.reserve(wide.size());
    for (wchar_t wc : wide) {
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
        } else {
            out.push_back(static_cast<char>(0xF0U | (code >> 18)));
            out.push_back(static_cast<char>(0x80U | ((code >> 12) & 0x3FU)));
            out.push_back(static_cast<char>(0x80U | ((code >> 6) & 0x3FU)));
            out.push_back(static_cast<char>(0x80U | (code & 0x3FU)));
        }
    }
    return out;
}

} // namespace

std::vector<FieldInfoEntry> FieldInfosReader::Read(const std::vector<uint8_t>& fnm_bytes) {
    if (fnm_bytes.empty()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .fnm is empty");
    }
    FnmCursor cur(fnm_bytes.data(), fnm_bytes.size());

    const int32_t field_count = cur.ReadVInt();
    if (field_count < 0 || static_cast<size_t>(field_count) > fnm_bytes.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .fnm: field_count out of range");
    }
    std::vector<FieldInfoEntry> out;
    out.reserve(static_cast<size_t>(field_count));

    for (int32_t i = 0; i < field_count; ++i) {
        FieldInfoEntry fi;
        const int32_t name_wlen = cur.ReadVInt();
        if (name_wlen < 0 || static_cast<size_t>(name_wlen) > fnm_bytes.size()) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .fnm: name_wlen out of range");
        }
        const std::wstring wide_name = cur.ReadSChars(name_wlen);
        fi.name = WideToUtf8(wide_name);

        const uint8_t bits = cur.ReadByte();
        fi.is_indexed = (bits & FieldInfosWriter::kIsIndexed) != 0;
        fi.store_term_vector = (bits & FieldInfosWriter::kStoreTermVector) != 0;
        fi.store_position_with_term_vector =
                (bits & FieldInfosWriter::kStorePositionsWithTermVector) != 0;
        fi.store_offset_with_term_vector =
                (bits & FieldInfosWriter::kStoreOffsetWithTermVector) != 0;
        fi.omit_norms = (bits & FieldInfosWriter::kOmitNorms) != 0;
        fi.store_payloads = (bits & FieldInfosWriter::kStorePayloads) != 0;
        fi.has_prox = (bits & FieldInfosWriter::kTermFreqAndPositions) != 0;
        const bool has_version_tag = (bits & FieldInfosWriter::kHasVersionTag) != 0;

        fi.index_version = FieldInfosWriter::kIndexVersionV0;
        fi.flags = 0;
        if (has_version_tag) {
            fi.index_version = cur.ReadVInt();
            if (fi.index_version >= FieldInfosWriter::kIndexVersionV3) {
                fi.flags = cur.ReadVInt();
            }
        }
        out.push_back(std::move(fi));
    }
    if (!cur.AtEnd()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .fnm: trailing bytes after declared field count");
    }
    return out;
}

SpimiSegmentReader::SpimiSegmentReader(std::vector<uint8_t> tis_bytes,
                                       std::vector<uint8_t> tii_bytes,
                                       std::vector<uint8_t> frq_bytes,
                                       std::vector<uint8_t> prx_bytes,
                                       std::vector<uint8_t> fnm_bytes)
        : _tis_bytes(std::move(tis_bytes)),
          _tii_bytes(std::move(tii_bytes)),
          _frq_bytes(std::move(frq_bytes)),
          _prx_bytes(std::move(prx_bytes)),
          _fnm_bytes(std::move(fnm_bytes)) {
    _field_infos = FieldInfosReader::Read(_fnm_bytes);
    _term_dict = std::make_unique<TermDictReader>(_tis_bytes, _tii_bytes);
}

int32_t SpimiSegmentReader::FindFieldNumber(std::string_view field_name) const {
    for (size_t i = 0; i < _field_infos.size(); ++i) {
        if (_field_infos[i].name == field_name) {
            return static_cast<int32_t>(i);
        }
    }
    return -1;
}

std::vector<SpimiSegmentReader::DocFreq> SpimiSegmentReader::Search(
        std::string_view field_name, std::string_view term_utf8) const {
    const int32_t field_number = FindFieldNumber(field_name);
    if (field_number < 0) {
        return {};
    }
    const auto& fi = _field_infos[static_cast<size_t>(field_number)];

    const auto term_info = _term_dict->LookupTerm(field_number, term_utf8);
    if (!term_info.has_value()) {
        return {};
    }
    // Mirror the byte-safety checks `SpimiQueryTermDocs` does on
    // the production query path. SpimiSegmentReader is test-only
    // today but the same untrusted-byte invariants apply on a
    // corrupt `.tis` — without these the pointer arithmetic below
    // is UB and `_frq_bytes.size() - fp` underflows.
    if (term_info->doc_freq <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis: non-positive doc_freq");
    }
    if (term_info->freq_pointer < 0 ||
        static_cast<uint64_t>(term_info->freq_pointer) > _frq_bytes.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis freq_pointer out of .frq bounds");
    }
    const auto fp = static_cast<size_t>(term_info->freq_pointer);
    return SpimiTermDocsReader::ReadTerm(_frq_bytes.data() + fp, _frq_bytes.size() - fp,
                                         term_info->doc_freq, fi.has_prox);
}

} // namespace doris::segment_v2::inverted_index::spimi
