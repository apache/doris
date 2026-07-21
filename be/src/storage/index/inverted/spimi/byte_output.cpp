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

#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

void ByteOutput::WriteInt(int32_t v) {
    WriteByte(static_cast<uint8_t>(v >> 24));
    WriteByte(static_cast<uint8_t>(v >> 16));
    WriteByte(static_cast<uint8_t>(v >> 8));
    WriteByte(static_cast<uint8_t>(v));
}

void ByteOutput::WriteLong(int64_t v) {
    WriteInt(static_cast<int32_t>(v >> 32));
    WriteInt(static_cast<int32_t>(v));
}

void ByteOutput::WriteVInt(int32_t v) {
    auto i = static_cast<uint32_t>(v);
    while ((i & ~0x7FU) != 0) {
        WriteByte(static_cast<uint8_t>((i & 0x7FU) | 0x80U));
        i >>= 7;
    }
    WriteByte(static_cast<uint8_t>(i));
}

void ByteOutput::WriteVLong(int64_t v) {
    auto i = static_cast<uint64_t>(v);
    while ((i & ~static_cast<uint64_t>(0x7F)) != 0) {
        WriteByte(static_cast<uint8_t>((i & 0x7FU) | 0x80U));
        i >>= 7;
    }
    WriteByte(static_cast<uint8_t>(i));
}

namespace {

// Shared encoding core for WriteSCharsFromWide / AppendSCharsFromWide: encodes
// one wide char into `buf`, returning the byte count. Matches CLucene's
// writeSChars<TCHAR> exactly — UTF-8 up to 0xFFFF, the "modified 4-byte"
// shape (0x80.. lead, no 0xF0 marker) for 0x10000..0x10FFFF, U+FFFD beyond.
inline uint32_t EncodeSChar(uint32_t code, uint8_t buf[4]) {
    if (code <= 0x7FU) {
        buf[0] = static_cast<uint8_t>(code);
        return 1;
    }
    if (code <= 0x7FFU) {
        buf[0] = static_cast<uint8_t>(0xC0U | (code >> 6));
        buf[1] = static_cast<uint8_t>(0x80U | (code & 0x3FU));
        return 2;
    }
    if (code <= 0xFFFFU) {
        buf[0] = static_cast<uint8_t>(0xE0U | (code >> 12));
        buf[1] = static_cast<uint8_t>(0x80U | ((code >> 6) & 0x3FU));
        buf[2] = static_cast<uint8_t>(0x80U | (code & 0x3FU));
        return 3;
    }
    if (code <= 0x10FFFFU) {
        buf[0] = static_cast<uint8_t>(0x80U | (code >> 18));
        buf[1] = static_cast<uint8_t>(0x80U | ((code >> 12) & 0x3FU));
        buf[2] = static_cast<uint8_t>(0x80U | ((code >> 6) & 0x3FU));
        buf[3] = static_cast<uint8_t>(0x80U | (code & 0x3FU));
        return 4;
    }
    // Replacement character (U+FFFD) in proper UTF-8.
    buf[0] = 0xEFU;
    buf[1] = 0xBFU;
    buf[2] = 0xBDU;
    return 3;
}

} // namespace

void ByteOutput::WriteSCharsFromWide(const wchar_t* s, int32_t length) {
    uint8_t buf[4];
    for (int32_t i = 0; i < length; ++i) {
        const uint32_t n = EncodeSChar(static_cast<uint32_t>(s[i]), buf);
        for (uint32_t k = 0; k < n; ++k) {
            WriteByte(buf[k]);
        }
    }
}

void AppendSCharsFromWide(const wchar_t* s, int32_t length, std::vector<uint8_t>* out) {
    uint8_t buf[4];
    for (int32_t i = 0; i < length; ++i) {
        const uint32_t n = EncodeSChar(static_cast<uint32_t>(s[i]), buf);
        out->insert(out->end(), buf, buf + n);
    }
}

namespace {

inline int32_t Utf8ByteCount(uint8_t b) {
    if ((b & 0x80U) == 0) {
        return 1;
    }
    if ((b & 0xE0U) == 0xC0U) {
        return 2;
    }
    if ((b & 0xF0U) == 0xE0U) {
        return 3;
    }
    if ((b & 0xF8U) == 0xF0U) {
        return 4;
    }
    return -1;
}

inline bool IsContinuationByte(uint8_t b) {
    return (b & 0xC0U) == 0x80U;
}

} // namespace

std::wstring Utf8ToWide(std::string_view utf8) {
    std::wstring out;
    Utf8ToWideInto(utf8, &out);
    return out;
}

void Utf8ToWideInto(std::string_view utf8, std::wstring* out_ptr) {
    // U+FFFD REPLACEMENT CHARACTER, emitted whenever the input bytes are not
    // a valid scalar value. Centralising the replacement here means
    // `WriteSCharsFromWide` always sees a well-formed Unicode scalar — it
    // does not need to special-case surrogates, overlong forms, or out-of-
    // range codepoints from a malformed input.
    constexpr wchar_t kReplacement = 0xFFFD;
    std::wstring& out = *out_ptr;
    out.clear();
    out.reserve(utf8.size());
    size_t i = 0;
    while (i < utf8.size()) {
        const auto b0 = static_cast<uint8_t>(utf8[i]);
        const int32_t n = Utf8ByteCount(b0);
        bool valid = (n >= 1 && n <= 4 && i + static_cast<size_t>(n) <= utf8.size());
        if (valid) {
            for (int32_t k = 1; k < n; ++k) {
                if (!IsContinuationByte(static_cast<uint8_t>(utf8[i + k]))) {
                    valid = false;
                    break;
                }
            }
        }
        wchar_t wc = kReplacement;
        if (valid) {
            uint32_t code = 0;
            if (n == 1) {
                code = b0;
            } else if (n == 2) {
                code = (static_cast<uint32_t>(b0 & 0x1FU) << 6) |
                       (static_cast<uint32_t>(utf8[i + 1]) & 0x3FU);
            } else if (n == 3) {
                code = (static_cast<uint32_t>(b0 & 0x0FU) << 12) |
                       ((static_cast<uint32_t>(utf8[i + 1]) & 0x3FU) << 6) |
                       (static_cast<uint32_t>(utf8[i + 2]) & 0x3FU);
            } else {
                code = (static_cast<uint32_t>(b0 & 0x07U) << 18) |
                       ((static_cast<uint32_t>(utf8[i + 1]) & 0x3FU) << 12) |
                       ((static_cast<uint32_t>(utf8[i + 2]) & 0x3FU) << 6) |
                       (static_cast<uint32_t>(utf8[i + 3]) & 0x3FU);
            }
            // C7 — reject the four classes of ill-formed sequences that map
            // to a "valid" wchar but would (a) sort differently from CLucene
            // in the SPIMI term dictionary, breaking the DCHECK_LT(strict-
            // ascending) invariant in `TermDictWriter`, or (b) confuse the
            // CLucene reader (surrogates inside a 32-bit `wchar_t` get the
            // 3-byte branch in `WriteSCharsFromWide`, producing bytes that
            // are neither WTF-8 nor CESU-8).
            const bool overlong = (n == 2 && code < 0x80U) || (n == 3 && code < 0x800U) ||
                                  (n == 4 && code < 0x10000U);
            const bool surrogate = (code >= 0xD800U && code <= 0xDFFFU);
            const bool out_of_range = (code > 0x10FFFFU);
            if (overlong || surrogate || out_of_range) {
                wc = kReplacement;
                i += static_cast<size_t>(n);
            } else {
                wc = static_cast<wchar_t>(code);
                i += static_cast<size_t>(n);
            }
        } else {
            // Invalid leading byte or truncated continuation — advance by
            // one byte and emit a replacement, per Unicode TR-36 best
            // practice (sub-part stand-alone-bytes substitution).
            i += 1;
        }
        out.push_back(wc);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
