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

#include "storage/index/inverted/spimi/segment_infos_writer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

class ByteReader {
public:
    explicit ByteReader(const std::vector<uint8_t>& bytes) : _bytes(bytes) {}

    uint8_t Byte() {
        EXPECT_LT(_pos, _bytes.size());
        return _bytes[_pos++];
    }

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

    size_t remaining() const { return _bytes.size() - _pos; }

private:
    const std::vector<uint8_t>& _bytes;
    size_t _pos = 0;
};

} // namespace

TEST(SegmentInfosWriterTest, SegmentsGenIsThreeFixedFields) {
    MemoryLuceneOutput out;
    SegmentInfosWriter writer;
    writer.WriteSegmentsGen(&out, /*generation=*/7);

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatLockless);
    EXPECT_EQ(r.ReadLong(), 7);
    EXPECT_EQ(r.ReadLong(), 7) << "generation written twice for redundancy";
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SegmentInfosWriterTest, EmptySegmentsNListHasHeaderOnly) {
    MemoryLuceneOutput out;
    SegmentInfosWriter writer;
    writer.WriteSegmentsN(&out, /*version=*/1, /*counter=*/0, /*segments=*/ {});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatSharedDocStore);
    EXPECT_EQ(r.ReadLong(), 1);
    EXPECT_EQ(r.ReadInt(), 0); // counter
    EXPECT_EQ(r.ReadInt(), 0); // segment count
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SegmentInfosWriterTest, SingleSegmentDefaultShape) {
    MemoryLuceneOutput out;
    SegmentInfosWriter writer;
    SegmentInfoEntry seg;
    seg.name = "_0";
    seg.doc_count = 1234;
    seg.del_gen = -1;
    seg.doc_store_offset = -1;
    seg.has_single_norm_file = true;
    seg.is_compound_file = 1;
    writer.WriteSegmentsN(&out, /*version=*/42, /*counter=*/1, {seg});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatSharedDocStore);
    EXPECT_EQ(r.ReadLong(), 42);
    EXPECT_EQ(r.ReadInt(), 1);
    EXPECT_EQ(r.ReadInt(), 1);
    // Segment name "_0" (wide-string)
    EXPECT_EQ(r.ReadVInt(), 2);
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('_'));
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('0'));
    EXPECT_EQ(r.ReadInt(), 1234);
    EXPECT_EQ(r.ReadLong(), -1);
    EXPECT_EQ(r.ReadInt(), -1);                             // doc_store_offset
    EXPECT_EQ(r.Byte(), 1U);                                // has_single_norm_file
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kNoNormGen); // normGen NO sentinel
    EXPECT_EQ(r.Byte(), 1U);                                // is_compound_file
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SegmentInfosWriterTest, SegmentWithDocStoreAppendsExtraFields) {
    MemoryLuceneOutput out;
    SegmentInfosWriter writer;
    SegmentInfoEntry seg;
    seg.name = "_5";
    seg.doc_count = 100;
    seg.del_gen = 3;
    seg.doc_store_offset = 0;
    seg.doc_store_segment = "_0";
    seg.doc_store_is_compound_file = true;
    seg.has_single_norm_file = false;
    seg.is_compound_file = -1;
    writer.WriteSegmentsN(&out, /*version=*/9, /*counter=*/2, {seg});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatSharedDocStore);
    EXPECT_EQ(r.ReadLong(), 9);
    EXPECT_EQ(r.ReadInt(), 2);
    EXPECT_EQ(r.ReadInt(), 1);
    // Name "_5"
    EXPECT_EQ(r.ReadVInt(), 2);
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('_'));
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('5'));
    EXPECT_EQ(r.ReadInt(), 100);
    EXPECT_EQ(r.ReadLong(), 3);
    EXPECT_EQ(r.ReadInt(), 0); // doc_store_offset triggers extra fields
    EXPECT_EQ(r.ReadVInt(), 2);
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('_'));
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('0'));
    EXPECT_EQ(r.Byte(), 1U); // doc_store_is_compound_file
    EXPECT_EQ(r.Byte(), 0U); // has_single_norm_file
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kNoNormGen);
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>(0xFFU)); // is_compound_file=-1 (int8)
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SegmentInfosWriterTest, MultipleSegmentsRoundTrip) {
    MemoryLuceneOutput out;
    SegmentInfosWriter writer;
    SegmentInfoEntry a;
    a.name = "_0";
    a.doc_count = 10;
    SegmentInfoEntry b;
    b.name = "_1";
    b.doc_count = 20;
    writer.WriteSegmentsN(&out, /*version=*/1, /*counter=*/2, {a, b});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatSharedDocStore);
    EXPECT_EQ(r.ReadLong(), 1);
    EXPECT_EQ(r.ReadInt(), 2);
    EXPECT_EQ(r.ReadInt(), 2);
    // Segment a name
    EXPECT_EQ(r.ReadVInt(), 2);
    (void)r.Byte();
    (void)r.Byte();
    EXPECT_EQ(r.ReadInt(), 10);
    EXPECT_EQ(r.ReadLong(), -1);
    EXPECT_EQ(r.ReadInt(), -1);
    EXPECT_EQ(r.Byte(), 1U);
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kNoNormGen);
    EXPECT_EQ(r.Byte(), 1U);
    // Segment b name
    EXPECT_EQ(r.ReadVInt(), 2);
    (void)r.Byte();
    (void)r.Byte();
    EXPECT_EQ(r.ReadInt(), 20);
    EXPECT_EQ(r.ReadLong(), -1);
    EXPECT_EQ(r.ReadInt(), -1);
    EXPECT_EQ(r.Byte(), 1U);
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kNoNormGen);
    EXPECT_EQ(r.Byte(), 1U);
    EXPECT_EQ(r.remaining(), 0U);
}

} // namespace doris::segment_v2::inverted_index::spimi
