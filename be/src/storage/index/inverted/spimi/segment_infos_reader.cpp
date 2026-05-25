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

#include "storage/index/inverted/spimi/segment_infos_reader.h"

// `_CLTHROWA` for byte-parser hard-fail on untrusted segments_N bytes.

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

class Cursor {
public:
    Cursor(const uint8_t* d, size_t n) : _d(d), _n(n) {}

    uint8_t ReadByte() {
        if (_p >= _n) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI segments_N read past end of buffer");
        }
        return _d[_p++];
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
            if (shift >= 32U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI segments_N VInt: shift overflow on crafted input");
            }
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int32_t>(v);
    }
    // Reads a wide-char string written via `WriteWideString` —
    // VInt(length) + schars. We convert the resulting wchar_t
    // sequence back to UTF-8 here because the rest of Doris's
    // SegmentInfoEntry stores `name` as `std::string`.
    std::string ReadWideStringAsUtf8() {
        const int32_t wlen = ReadVInt();
        std::string out;
        for (int32_t i = 0; i < wlen; ++i) {
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
            } else {
                // 4-byte modified form (lead 0x80 | (code>>18)).
                DCHECK_EQ(b0 & 0xC0U, 0x80U) << "segments_N schar lead byte unexpected";
                const uint8_t b1 = ReadByte();
                const uint8_t b2 = ReadByte();
                const uint8_t b3 = ReadByte();
                code = (static_cast<uint32_t>(b0 & 0x7FU) << 18) |
                       (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                       (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
            }
            // Re-encode codepoint as UTF-8.
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

private:
    const uint8_t* _d;
    size_t _n;
    size_t _p = 0;
};

} // namespace

SegmentInfosReader::Manifest SegmentInfosReader::Read(const std::vector<uint8_t>& bytes) {
    if (bytes.empty()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI segments_N is empty");
    }
    Cursor cur(bytes.data(), bytes.size());
    const int32_t format = cur.ReadInt32BE();
    if (format != SegmentInfosWriter::kFormatSharedDocStore) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI segments_N FORMAT mismatch");
    }
    Manifest m;
    m.version = cur.ReadInt64BE();
    m.counter = cur.ReadInt32BE();
    const int32_t segment_count = cur.ReadInt32BE();
    // Cap segment_count against bytes.size() — each segment entry
    // takes at least ~10 bytes on wire, so segment_count > bytes
    // size is impossible without buffer overrun. Without the bound
    // a crafted segments_N could request a multi-gigabyte reserve.
    if (segment_count < 0 || static_cast<uint64_t>(segment_count) > bytes.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI segments_N segment_count out of range");
    }
    m.segments.reserve(static_cast<size_t>(segment_count));
    for (int32_t i = 0; i < segment_count; ++i) {
        SegmentInfoEntry e;
        e.name = cur.ReadWideStringAsUtf8();
        e.doc_count = cur.ReadInt32BE();
        // Bound `doc_count` against a realistic per-segment ceiling
        // so a crafted segments_N can't drive a downstream
        // `norms(field)` allocation of ~2 GB (one byte per doc),
        // amplified per field. 256M docs/segment is well above any
        // real workload but below `vector::max_size()` for any
        // sane element type.
        constexpr int32_t kMaxDocsPerSegment = 1 << 28;
        if (e.doc_count < 0 || e.doc_count > kMaxDocsPerSegment) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI segments_N doc_count out of range");
        }
        e.del_gen = cur.ReadInt64BE();
        e.doc_store_offset = cur.ReadInt32BE();
        if (e.doc_store_offset != -1) {
            e.doc_store_segment = cur.ReadWideStringAsUtf8();
            e.doc_store_is_compound_file = cur.ReadByte() != 0;
        }
        e.has_single_norm_file = cur.ReadByte() != 0;
        const int32_t norm_gen_len = cur.ReadInt32BE();
        if (norm_gen_len != SegmentInfosWriter::kNoNormGen) [[unlikely]] {
            // SPIMI segments do not carry per-field norm gens; a
            // value other than the NO sentinel would desync the
            // subsequent `is_compound_file` byte.
            SPIMI_THROW_CORRUPT("SPIMI segments_N norm_gen_len not NO sentinel");
        }
        e.is_compound_file = static_cast<int8_t>(cur.ReadByte());
        m.segments.push_back(std::move(e));
    }
    return m;
}

} // namespace doris::segment_v2::inverted_index::spimi
