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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

// Lucene 2.x byte-encoding primitives. Reimplemented in Doris so the SPIMI
// writer does not link against CLucene. The byte layout matches CLucene's
// `IndexOutput` exactly so the produced .tis/.tii/.frq/.prx files can be read
// by the existing CLucene reader.
//
// Numeric primitives:
//   WriteInt / WriteLong : big-endian, fixed width
//   WriteVInt / WriteVLong : 7-bit-per-byte variable-length (MSB = continuation)
//
// String primitives:
//   WriteSCharsFromWide : matches `IndexOutput::writeSChars<TCHAR>` — UTF-8 for
//     codepoints <= 0xFFFF, and CLucene's *modified* 4-byte encoding (leading
//     byte 0x80..0x84) for codepoints in 0x10000..0x10FFFF. This is what the
//     existing CLucene reader expects, so we replicate it bit-for-bit.
//
// Subclasses implement WriteByte / WriteBytes / FilePointer; everything else
// is composed on top.
class ByteOutput {
public:
    virtual ~ByteOutput() = default;

    ByteOutput(const ByteOutput&) = delete;
    ByteOutput& operator=(const ByteOutput&) = delete;

    virtual void WriteByte(uint8_t b) = 0;
    virtual void WriteBytes(const uint8_t* b, size_t len) = 0;
    virtual int64_t FilePointer() const = 0;

    void WriteInt(int32_t v);
    void WriteLong(int64_t v);
    void WriteVInt(int32_t v);
    void WriteVLong(int64_t v);

    // Writes `length` wide characters using CLucene's encoding. The Doris
    // wchar_t is 32-bit on Linux, matching CLucene's TCHAR.
    void WriteSCharsFromWide(const wchar_t* s, int32_t length);

protected:
    ByteOutput() = default;
};

// In-memory backing — owns a byte vector. Convenient for tests and for
// computing index entries before writing them to a file.
class MemoryByteOutput final : public ByteOutput {
public:
    MemoryByteOutput() = default;

    void WriteByte(uint8_t b) override { _bytes.push_back(b); }
    void WriteBytes(const uint8_t* b, size_t len) override {
        _bytes.insert(_bytes.end(), b, b + len);
    }
    int64_t FilePointer() const override { return static_cast<int64_t>(_bytes.size()); }

    const std::vector<uint8_t>& bytes() const { return _bytes; }
    std::vector<uint8_t>& mutable_bytes() { return _bytes; }

    void Clear() { _bytes.clear(); }

private:
    std::vector<uint8_t> _bytes;
};

// Decodes a UTF-8 byte string into a wide-char string using the same rule as
// CLucene's `StringUtil::string_to_wstring`: each valid 1..4-byte UTF-8
// sequence becomes one wide character; invalid leading bytes are passed
// through as-is (1 byte → 1 wide char). Exposed here so the term-dictionary
// writer and unit tests share the conversion.
std::wstring Utf8ToWide(std::string_view utf8);

// Scratch-reuse variant: clears `out` and fills it with the same conversion.
// The term-dictionary writer calls this once per term, so reusing one member
// wstring avoids a heap allocation per term.
void Utf8ToWideInto(std::string_view utf8, std::wstring* out);

// Encodes wide characters with EXACTLY the byte layout WriteSCharsFromWide
// produces, appending to `out` instead of streaming through the virtual
// WriteByte. Lets hot writers stage a term suffix once and emit it with a
// single WriteBytes call. Both functions share one encoding core, so they
// cannot drift.
void AppendSCharsFromWide(const wchar_t* s, int32_t length, std::vector<uint8_t>* out);

} // namespace doris::segment_v2::inverted_index::spimi
