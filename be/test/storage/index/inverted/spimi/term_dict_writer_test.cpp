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

#include <gtest/gtest.h>

#include <cstdio>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// A minimal Lucene-format reader sufficient to validate the byte stream the
// TermDictWriter produces. We keep it inside the test so we don't depend on
// the CLucene library at unit-test time, and the test fails immediately if
// the writer drifts away from the on-disk format.
class ByteReader {
public:
    explicit ByteReader(const std::vector<uint8_t>& bytes) : _bytes(bytes) {}

    int32_t ReadInt() {
        int32_t v = (Byte() << 24);
        v |= (Byte() << 16);
        v |= (Byte() << 8);
        v |= Byte();
        return v;
    }

    int64_t ReadLong() {
        const auto hi = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        const auto lo = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        return (hi << 32) | lo;
    }

    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = Byte();
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
        uint64_t shift = 0;
        while (true) {
            const uint8_t b = Byte();
            v |= static_cast<uint64_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int64_t>(v);
    }

    uint8_t Byte() {
        EXPECT_LT(_pos, _bytes.size()) << "Read past end of buffer";
        return _bytes[_pos++];
    }

    size_t remaining() const { return _bytes.size() - _pos; }
    size_t position() const { return _pos; }
    void seek(size_t pos) { _pos = pos; }

private:
    const std::vector<uint8_t>& _bytes;
    size_t _pos = 0;
};

struct DecodedHeader {
    int32_t format;
    int64_t legacy_size;
    int32_t index_interval;
    int32_t skip_interval;
    int32_t max_skip_levels;
};

DecodedHeader ReadHeader(ByteReader& r) {
    DecodedHeader h {};
    h.format = r.ReadInt();
    h.legacy_size = r.ReadLong();
    h.index_interval = r.ReadInt();
    h.skip_interval = r.ReadInt();
    h.max_skip_levels = r.ReadInt();
    return h;
}

// Decoded representation of one .tis entry. We accumulate full wide-char
// terms by reconstructing the front-coded suffix.
struct TermEntry {
    int32_t field_number;
    std::wstring term_wide;
    int32_t doc_freq;
    int64_t freq_pointer;
    int64_t prox_pointer;
    int32_t skip_offset; // -1 if not written
};

// Decodes one modified-UTF-8 wide-char as written by LuceneOutput.
wchar_t DecodeWideChar(ByteReader& r) {
    const uint8_t b0 = r.Byte();
    if ((b0 & 0x80) == 0) {
        return static_cast<wchar_t>(b0);
    }
    // Disambiguate based on the top nibble. Note that CLucene's writer emits
    // 4-byte sequences with leading byte 0x80..0x84 (high bit set, second
    // bit clear-ish) — they collide with continuation bytes, so we use the
    // "no recognised UTF-8 lead" branch to detect them.
    if ((b0 & 0xE0) == 0xC0) {
        const uint8_t b1 = r.Byte();
        return static_cast<wchar_t>(((b0 & 0x1F) << 6) | (b1 & 0x3F));
    }
    if ((b0 & 0xF0) == 0xE0) {
        const uint8_t b1 = r.Byte();
        const uint8_t b2 = r.Byte();
        return static_cast<wchar_t>(((b0 & 0x0F) << 12) | ((b1 & 0x3F) << 6) | (b2 & 0x3F));
    }
    // 4-byte modified path: leading byte 0x80..0x87 with all top bits 10_b.
    const uint8_t b1 = r.Byte();
    const uint8_t b2 = r.Byte();
    const uint8_t b3 = r.Byte();
    return static_cast<wchar_t>(((b0 & 0x07) << 18) | ((b1 & 0x3F) << 12) | ((b2 & 0x3F) << 6) |
                                (b3 & 0x3F));
}

TermEntry DecodeEntry(ByteReader& r, const std::wstring& prev_term, int32_t skip_interval,
                      bool include_index_pointer, int64_t* tis_pointer_delta_out) {
    const int32_t prefix = r.ReadVInt();
    const int32_t suffix = r.ReadVInt();
    std::wstring term;
    term.reserve(prefix + suffix);
    if (prefix > 0) {
        term.append(prev_term, 0, static_cast<size_t>(prefix));
    }
    for (int32_t i = 0; i < suffix; ++i) {
        term.push_back(DecodeWideChar(r));
    }
    const int32_t field_number = r.ReadVInt();
    const int32_t doc_freq = r.ReadVInt();
    const int64_t freq_pointer_delta = r.ReadVLong();
    const int64_t prox_pointer_delta = r.ReadVLong();
    int32_t skip_offset = -1;
    if (doc_freq >= skip_interval) {
        skip_offset = r.ReadVInt();
    }
    if (include_index_pointer && tis_pointer_delta_out != nullptr) {
        *tis_pointer_delta_out = r.ReadVLong();
    }
    TermEntry entry;
    entry.field_number = field_number;
    entry.term_wide = term;
    entry.doc_freq = doc_freq;
    entry.freq_pointer = freq_pointer_delta;
    entry.prox_pointer = prox_pointer_delta;
    entry.skip_offset = skip_offset;
    return entry;
}

TermInfo Info(int32_t doc_freq, int64_t fp, int64_t pp, int32_t skip = 0) {
    TermInfo info;
    info.doc_freq = doc_freq;
    info.freq_pointer = fp;
    info.prox_pointer = pp;
    info.skip_offset = skip;
    return info;
}

} // namespace

TEST(TermDictWriterTest, HeaderMatchesLuceneFormat) {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    TermDictWriter w(&tis, &tii, /*index_interval=*/128, /*skip_interval=*/16);
    w.Close();

    {
        ByteReader r(tis.bytes());
        const auto h = ReadHeader(r);
        EXPECT_EQ(h.format, TermDictWriter::kFormat);
        EXPECT_EQ(h.legacy_size, -1);
        EXPECT_EQ(h.index_interval, 128);
        EXPECT_EQ(h.skip_interval, 16);
        EXPECT_EQ(h.max_skip_levels, TermDictWriter::kMaxSkipLevels);
        // After the header, only the footer (one long = .tis size = 0) remains.
        EXPECT_EQ(r.ReadLong(), 0);
        EXPECT_EQ(r.remaining(), 0U);
    }
    {
        // The writer's Close() emits a sentinel TII entry when
        // no Add() ever ran, so the reader's "first entry is the
        // sentinel" invariant holds even for honest empty
        // segments. After the header the bytes are: sentinel
        // entry + footer (tii_size = 1, tis_size = 0).
        ByteReader r(tii.bytes());
        const auto h = ReadHeader(r);
        EXPECT_EQ(h.format, TermDictWriter::kFormat);
        EXPECT_EQ(h.index_interval, 128);
        // Skip the sentinel entry (variable-length, terminates
        // before the final 16-byte footer).
        const size_t footer_offset = tii.bytes().size() - 16U;
        EXPECT_GT(footer_offset, r.position());
        r.seek(footer_offset);
        // .tii footer: tii_size (= 1, the sentinel), then tis_size.
        EXPECT_EQ(r.ReadLong(), 1);
        EXPECT_EQ(r.ReadLong(), 0);
        EXPECT_EQ(r.remaining(), 0U);
    }
}

TEST(TermDictWriterTest, SingleTermRoundTrip) {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    TermDictWriter w(&tis, &tii, 128, 16);
    w.Add(/*field=*/0, "apple", Info(/*df=*/3, /*fp=*/10, /*pp=*/20));
    w.Close();

    ByteReader r(tis.bytes());
    ReadHeader(r);
    int64_t tis_ptr_delta = 0;
    const auto e = DecodeEntry(r, L"", 16, /*include_index_pointer=*/false, &tis_ptr_delta);
    EXPECT_EQ(e.field_number, 0);
    EXPECT_EQ(e.term_wide, std::wstring(L"apple"));
    EXPECT_EQ(e.doc_freq, 3);
    EXPECT_EQ(e.freq_pointer, 10);
    EXPECT_EQ(e.prox_pointer, 20);
    EXPECT_EQ(e.skip_offset, -1) << "doc_freq < skip_interval ⇒ no skip_offset";
    EXPECT_EQ(r.ReadLong(), 1) << "tis footer: size = 1";
    EXPECT_EQ(r.remaining(), 0U);

    EXPECT_EQ(w.TisSize(), 1);
    // The first Add() always records the empty sentinel into .tii.
    EXPECT_EQ(w.TiiSize(), 1);
}

TEST(TermDictWriterTest, SkipOffsetWrittenWhenDocFreqMeetsInterval) {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    TermDictWriter w(&tis, &tii, 128, 16);
    w.Add(0, "banana", Info(/*df=*/16, 100, 200, /*skip=*/42));
    w.Close();

    ByteReader r(tis.bytes());
    ReadHeader(r);
    const auto e = DecodeEntry(r, L"", 16, false, nullptr);
    EXPECT_EQ(e.term_wide, std::wstring(L"banana"));
    EXPECT_EQ(e.skip_offset, 42);
}

TEST(TermDictWriterTest, FrontCodingSharesPrefix) {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    TermDictWriter w(&tis, &tii, 128, 16);
    w.Add(0, "apple", Info(1, 0, 0));
    w.Add(0, "apply", Info(1, 10, 10));
    w.Add(0, "banana", Info(1, 20, 20));
    w.Close();

    ByteReader r(tis.bytes());
    ReadHeader(r);

    // apple → prefix 0, suffix 5, raw "apple"
    {
        const int32_t prefix = r.ReadVInt();
        const int32_t suffix = r.ReadVInt();
        EXPECT_EQ(prefix, 0);
        EXPECT_EQ(suffix, 5);
        for (char c : std::string("apple")) {
            EXPECT_EQ(r.Byte(), static_cast<uint8_t>(c));
        }
        EXPECT_EQ(r.ReadVInt(), 0);  // field_number
        EXPECT_EQ(r.ReadVInt(), 1);  // doc_freq
        EXPECT_EQ(r.ReadVLong(), 0); // freq pointer delta
        EXPECT_EQ(r.ReadVLong(), 0); // prox pointer delta
    }
    // apply → prefix 4 ("appl"), suffix 1 ("y")
    {
        EXPECT_EQ(r.ReadVInt(), 4);
        EXPECT_EQ(r.ReadVInt(), 1);
        EXPECT_EQ(r.Byte(), static_cast<uint8_t>('y'));
        EXPECT_EQ(r.ReadVInt(), 0);
        EXPECT_EQ(r.ReadVInt(), 1);
        EXPECT_EQ(r.ReadVLong(), 10);
        EXPECT_EQ(r.ReadVLong(), 10);
    }
    // banana → prefix 0, suffix 6
    {
        EXPECT_EQ(r.ReadVInt(), 0);
        EXPECT_EQ(r.ReadVInt(), 6);
        for (char c : std::string("banana")) {
            EXPECT_EQ(r.Byte(), static_cast<uint8_t>(c));
        }
        EXPECT_EQ(r.ReadVInt(), 0);
        EXPECT_EQ(r.ReadVInt(), 1);
        EXPECT_EQ(r.ReadVLong(), 10);
        EXPECT_EQ(r.ReadVLong(), 10);
    }

    EXPECT_EQ(r.ReadLong(), 3);
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(TermDictWriterTest, FrontCodingPrefixIsInWideCharsForCjk) {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    TermDictWriter w(&tis, &tii, 128, 16);
    // "中华" (0x534E) and "中国" (0x56FD) share one wide character "中".
    // 华 (0x534E) < 国 (0x56FD), so 中华 comes first in wide-char order.
    // The writer must report prefix=1 (wide char), not 3 (UTF-8 bytes).
    w.Add(0, "\xE4\xB8\xAD\xE5\x8D\x8E", Info(1, 0, 0)); // 中华
    w.Add(0, "\xE4\xB8\xAD\xE5\x9B\xBD", Info(1, 5, 5)); // 中国
    w.Close();

    ByteReader r(tis.bytes());
    ReadHeader(r);
    // First entry: prefix=0, suffix=2 wide chars
    EXPECT_EQ(r.ReadVInt(), 0);
    EXPECT_EQ(r.ReadVInt(), 2);
    // Skip 6 bytes of wide-encoded suffix (two 3-byte CJK chars).
    for (int i = 0; i < 6; ++i) {
        (void)r.Byte();
    }
    EXPECT_EQ(r.ReadVInt(), 0);
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVLong(), 0);
    EXPECT_EQ(r.ReadVLong(), 0);

    // Second entry: prefix=1 (wide char), suffix=1
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 1);
    // Skip the 3 bytes of the suffix character.
    for (int i = 0; i < 3; ++i) {
        (void)r.Byte();
    }
    EXPECT_EQ(r.ReadVInt(), 0);
    EXPECT_EQ(r.ReadVInt(), 1);
}

TEST(TermDictWriterTest, IndexEntryEveryInterval) {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    // Tiny indexInterval so the test runs fast.
    TermDictWriter w(&tis, &tii, /*index_interval=*/2, /*skip_interval=*/16);
    for (int i = 0; i < 6; ++i) {
        char buf[8];
        std::snprintf(buf, sizeof(buf), "t%05d", i);
        w.Add(0, buf, Info(1, i * 4, i * 4));
    }
    w.Close();

    // With interval=2 the .tii records the empty sentinel before term#0,
    // then the lastTerm at sizes 2 and 4. That is: ("", field=-1),
    // ("t00001", field=0), ("t00003", field=0).
    EXPECT_EQ(w.TisSize(), 6);
    EXPECT_EQ(w.TiiSize(), 3);

    ByteReader r(tii.bytes());
    ReadHeader(r);

    // Sentinel entry: empty term, field=-1, all zeros.
    {
        int64_t delta = 0;
        const auto e = DecodeEntry(r, L"", 16, /*include_index_pointer=*/true, &delta);
        EXPECT_TRUE(e.term_wide.empty());
        EXPECT_EQ(e.field_number, -1);
        EXPECT_EQ(e.doc_freq, 0);
        EXPECT_GT(delta, 0) << "First index pointer must skip the header bytes";
    }
    // Subsequent entries: actual terms.
    {
        std::wstring prev;
        for (int i = 0; i < 2; ++i) {
            int64_t delta = 0;
            const auto e = DecodeEntry(r, prev, 16, /*include_index_pointer=*/true, &delta);
            EXPECT_EQ(e.field_number, 0);
            EXPECT_EQ(e.doc_freq, 1);
            EXPECT_GT(delta, 0);
            prev = e.term_wide;
        }
    }

    // Tii footer: tii_size then tis_size.
    EXPECT_EQ(r.ReadLong(), 3);
    EXPECT_EQ(r.ReadLong(), 6);
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(TermDictWriterTest, CloseIsIdempotent) {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    TermDictWriter w(&tis, &tii);
    w.Close();
    const auto bytes_first = tis.bytes();
    w.Close(); // Second call must be a no-op.
    EXPECT_EQ(tis.bytes(), bytes_first);
}

} // namespace doris::segment_v2::inverted_index::spimi
