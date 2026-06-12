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

// Inverse of `ByteOutput::WriteSCharsFromWide` — identical logic to the
// ReadSChars in term_dict_reader.cpp, lifted onto a ForwardByteSource so the
// same parse serves both the in-memory and the streaming-cursor backing.
std::wstring ReadSChars(ForwardByteSource& src, int32_t length) {
    std::wstring out;
    out.reserve(static_cast<size_t>(length));
    for (int32_t i = 0; i < length; ++i) {
        const uint8_t b0 = src.ReadByte();
        uint32_t code = 0;
        if ((b0 & 0x80U) == 0) {
            code = b0;
        } else if ((b0 & 0xE0U) == 0xC0U) {
            const uint8_t b1 = src.ReadByte();
            code = (static_cast<uint32_t>(b0 & 0x1FU) << 6) | (b1 & 0x3FU);
        } else if ((b0 & 0xF0U) == 0xE0U) {
            const uint8_t b1 = src.ReadByte();
            const uint8_t b2 = src.ReadByte();
            code = (static_cast<uint32_t>(b0 & 0x0FU) << 12) |
                   (static_cast<uint32_t>(b1 & 0x3FU) << 6) | (b2 & 0x3FU);
        } else if ((b0 & 0xF8U) == 0xF0U) {
            const uint8_t b1 = src.ReadByte();
            const uint8_t b2 = src.ReadByte();
            const uint8_t b3 = src.ReadByte();
            code = (static_cast<uint32_t>(b0 & 0x07U) << 18) |
                   (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                   (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
        } else {
            // Modified-4-byte form (CLucene-specific).
            const uint8_t b1 = src.ReadByte();
            const uint8_t b2 = src.ReadByte();
            const uint8_t b3 = src.ReadByte();
            code = (static_cast<uint32_t>(b0 & 0x7FU) << 18) |
                   (static_cast<uint32_t>(b1 & 0x3FU) << 12) |
                   (static_cast<uint32_t>(b2 & 0x3FU) << 6) | (b3 & 0x3FU);
        }
        out.push_back(static_cast<wchar_t>(code));
    }
    return out;
}

} // namespace

TermEnum::TermEnum(const std::vector<uint8_t>& tis_bytes)
        : _owned_src(std::make_unique<MemoryByteSource>(tis_bytes.data(), tis_bytes.size())),
          _src(_owned_src.get()) {
    Init();
}

TermEnum::TermEnum(ForwardByteSource* src) : _src(src) {
    Init();
}

void TermEnum::Init() {
    // Minimum size: 24-byte header + 8-byte footer.
    if (_src->Length() < 32) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: buffer too short");
    }

    // Header.
    const int32_t format = _src->ReadInt32BE();
    if (format != TermDictWriter::kFormat && format != TermDictWriter::kFormatInline) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: FORMAT mismatch");
    }
    _inline_format = (format == TermDictWriter::kFormatInline);
    [[maybe_unused]] const int64_t legacy = _src->ReadInt64BE(); // always -1
    _index_interval = _src->ReadInt32BE();
    _skip_interval = _src->ReadInt32BE();
    [[maybe_unused]] const int32_t max_skip_levels = _src->ReadInt32BE();

    _data_end = _src->Length() - 8; // footer is the last 8 bytes

    // Footer: int64 size (number of .tis entries) — 一次性随机读，不动主游标。
    uint8_t footer[8];
    _src->ReadAt(_data_end, footer, sizeof(footer));
    uint64_t total = 0;
    for (const uint8_t b : footer) {
        total = (total << 8) | b;
    }
    _total_entries = static_cast<int64_t>(total);
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
    if (_src->Position() >= _data_end) {
        _done = true;
        return false;
    }

    // Front-coded term: prefix_len, suffix_len, suffix bytes, field_number.
    const int32_t prefix_len = _src->ReadVInt();
    const int32_t suffix_len = _src->ReadVInt();
    if (prefix_len < 0 || suffix_len < 0 || static_cast<size_t>(prefix_len) > _last_term.size())
            [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: malformed prefix/suffix length");
    }

    _last_term.resize(static_cast<size_t>(prefix_len));
    if (suffix_len > 0) {
        _last_term.append(ReadSChars(*_src, suffix_len));
    }

    const int32_t field_number = _src->ReadVInt();

    int32_t doc_freq = 0;
    bool inlined = false;
    if (_inline_format) {
        const int32_t raw = _src->ReadVInt();
        if (raw < 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: negative inline doc_freq word");
        }
        doc_freq = raw >> 1;
        inlined = (raw & 1) != 0;
    } else {
        doc_freq = _src->ReadVInt();
    }
    if (doc_freq < 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: negative doc_freq");
    }

    // Reset inline span state each entry.
    _current.info.inlined = false;
    _current.info.inline_frq = nullptr;
    _current.info.inline_frq_len = 0;
    _current.info.inline_prx = nullptr;
    _current.info.inline_prx_len = 0;
    int32_t skip_offset = 0;

    if (inlined) {
        // Inline entry: freq/prox pointer deltas OMITTED; the running
        // accumulators stay unchanged (anchored at the previous external term).
        // span 借用/拷贝二选一：内存源 BorrowStable 返回稳定指针（旧契约，
        // 生命周期 = 底层缓冲）；流式源返回 nullptr，span 字节拷入
        // _inline_scratch（frq 在前、prx 紧随），仅本 entry 有效。
        _inline_scratch.clear();
        const int32_t frq_len = _src->ReadVInt();
        if (frq_len < 0 || static_cast<int64_t>(frq_len) > _data_end - _src->Position())
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: negative inline frq_len");
        }
        const uint8_t* borrow_frq = nullptr;
        size_t scratch_frq_len = 0;
        if (frq_len > 0) {
            borrow_frq = _src->BorrowStable(static_cast<size_t>(frq_len));
            if (borrow_frq == nullptr) {
                _src->ReadInto(&_inline_scratch, static_cast<size_t>(frq_len));
                scratch_frq_len = static_cast<size_t>(frq_len);
            }
        }
        const int32_t prx_len = _src->ReadVInt();
        if (prx_len < 0 || static_cast<int64_t>(prx_len) > _data_end - _src->Position())
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: negative inline prx_len");
        }
        const uint8_t* borrow_prx = nullptr;
        bool scratch_prx = false;
        if (prx_len > 0) {
            borrow_prx = _src->BorrowStable(static_cast<size_t>(prx_len));
            if (borrow_prx == nullptr) {
                _src->ReadInto(&_inline_scratch, static_cast<size_t>(prx_len));
                scratch_prx = true;
            }
        }

        _current.info.inlined = true;
        // scratch 指针在两段 ReadInto 全部完成后再取（中途可能扩容搬家）。
        _current.info.inline_frq =
                borrow_frq != nullptr ? borrow_frq
                                      : (scratch_frq_len > 0 ? _inline_scratch.data() : nullptr);
        _current.info.inline_frq_len = static_cast<uint32_t>(frq_len);
        _current.info.inline_prx =
                borrow_prx != nullptr
                        ? borrow_prx
                        : (scratch_prx ? _inline_scratch.data() + scratch_frq_len : nullptr);
        _current.info.inline_prx_len = static_cast<uint32_t>(prx_len);
    } else {
        _last_freq_pointer += _src->ReadVLong();
        _last_prox_pointer += _src->ReadVLong();
        if (doc_freq >= _skip_interval) {
            skip_offset = _src->ReadVInt();
        }
    }

    // 防御：entry 不得越过 footer（旧实现以 _data_end 截断 Cursor 逐字节
    // 兜底；源以流尾为界，这里补 entry 级越界检查，坏文件同样必抛）。
    if (_src->Position() > _data_end) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: entry runs into the footer");
    }

    // Populate output.
    _current.field_number = field_number;
    _current.term_utf8 = WideToUtf8(_last_term);
    _current.info.doc_freq = doc_freq;
    _current.info.freq_pointer = _last_freq_pointer;
    _current.info.prox_pointer = _last_prox_pointer;
    _current.info.skip_offset = skip_offset;
    // Slim kDefault hint (df < skip_interval): mirrors the writer's dispatch so
    // the merge re-encode decode site reads the no-codec-byte / no-doc_count
    // layout. PFOR/windowed terms (df >= skip_interval) keep the codec byte.
    _current.info.is_slim = doc_freq < _skip_interval;

    ++_consumed;
    if (_consumed >= _total_entries || _src->Position() >= _data_end) {
        _done = true;
    }
    return true;
}

} // namespace doris::segment_v2::inverted_index::spimi
