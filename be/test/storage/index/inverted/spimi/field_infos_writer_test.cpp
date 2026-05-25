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

#include "storage/index/inverted/spimi/field_infos_writer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

class ByteReader {
public:
    explicit ByteReader(const std::vector<uint8_t>& bytes) : _bytes(bytes) {}

    uint8_t Byte() {
        EXPECT_LT(_pos, _bytes.size());
        return _bytes[_pos++];
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

TEST(FieldInfosWriterTest, EmptyFieldListIsJustZero) {
    MemoryByteOutput out;
    FieldInfosWriter writer(&out);
    writer.Write({});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadVInt(), 0);
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(FieldInfosWriterTest, SingleAsciiFieldV0DefaultBits) {
    MemoryByteOutput out;
    FieldInfosWriter writer(&out);

    FieldInfoEntry fi;
    fi.name = "body";
    // Defaults: is_indexed=true, has_prox=true, omit_norms=true,
    // everything else off, index_version=0.
    writer.Write({fi});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 4) << "name length (wide chars)";
    EXPECT_EQ(r.Byte(), 'b');
    EXPECT_EQ(r.Byte(), 'o');
    EXPECT_EQ(r.Byte(), 'd');
    EXPECT_EQ(r.Byte(), 'y');
    // bits: IS_INDEXED | OMIT_NORMS | TERM_FREQ_AND_POSITIONS
    EXPECT_EQ(r.Byte(), FieldInfosWriter::kIsIndexed | FieldInfosWriter::kOmitNorms |
                                FieldInfosWriter::kTermFreqAndPositions);
    EXPECT_EQ(r.remaining(), 0U) << "V0 omits the index_version VInt";
}

TEST(FieldInfosWriterTest, V4FieldAppendsVersionAndFlags) {
    MemoryByteOutput out;
    FieldInfosWriter writer(&out);

    FieldInfoEntry fi;
    fi.name = "title";
    fi.index_version = 4; // kV4
    fi.flags = 0x07;
    writer.Write({fi});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 5);
    for (char c : std::string("title")) {
        EXPECT_EQ(r.Byte(), static_cast<uint8_t>(c));
    }
    const uint8_t bits = r.Byte();
    EXPECT_NE(bits & FieldInfosWriter::kHasVersionTag, 0)
            << "index_version > V0 sets the 0x80 marker";
    EXPECT_EQ(r.ReadVInt(), 4);    // index_version
    EXPECT_EQ(r.ReadVInt(), 0x07); // flags (because V >= V3)
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(FieldInfosWriterTest, V1FieldEmitsVersionButNotFlags) {
    MemoryByteOutput out;
    FieldInfosWriter writer(&out);

    FieldInfoEntry fi;
    fi.name = "f";
    fi.index_version = 1; // kV1
    fi.flags = 0x42;      // should NOT be written at V1
    writer.Write({fi});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('f'));
    (void)r.Byte(); // bits
    EXPECT_EQ(r.ReadVInt(), 1) << "V1 ⇒ writes the version VInt only";
    EXPECT_EQ(r.remaining(), 0U) << "V1 < V3 ⇒ flags VInt is omitted";
}

TEST(FieldInfosWriterTest, MultipleFieldsRoundTrip) {
    MemoryByteOutput out;
    FieldInfosWriter writer(&out);

    FieldInfoEntry a;
    a.name = "field_a";
    a.has_prox = true;
    a.omit_norms = true;
    FieldInfoEntry b;
    b.name = "field_b";
    b.has_prox = false; // BKD-like field: no positions
    b.omit_norms = true;
    b.is_indexed = true;
    writer.Write({a, b});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadVInt(), 2);

    // Field a
    EXPECT_EQ(r.ReadVInt(), 7);
    for (char c : std::string("field_a")) {
        EXPECT_EQ(r.Byte(), static_cast<uint8_t>(c));
    }
    EXPECT_EQ(r.Byte(), FieldInfosWriter::kIsIndexed | FieldInfosWriter::kOmitNorms |
                                FieldInfosWriter::kTermFreqAndPositions);

    // Field b
    EXPECT_EQ(r.ReadVInt(), 7);
    for (char c : std::string("field_b")) {
        EXPECT_EQ(r.Byte(), static_cast<uint8_t>(c));
    }
    EXPECT_EQ(r.Byte(), FieldInfosWriter::kIsIndexed | FieldInfosWriter::kOmitNorms);
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(FieldInfosWriterTest, CjkFieldNameUsesWideCharLength) {
    MemoryByteOutput out;
    FieldInfosWriter writer(&out);

    FieldInfoEntry fi;
    fi.name = "中文"; // 2 wide chars, 6 UTF-8 bytes
    writer.Write({fi});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 2) << "name length is wide-char count, not bytes";
    // 6 bytes of modified UTF-8 for "中" + "文"
    EXPECT_EQ(r.Byte(), 0xE4);
    EXPECT_EQ(r.Byte(), 0xB8);
    EXPECT_EQ(r.Byte(), 0xAD);
    EXPECT_EQ(r.Byte(), 0xE6);
    EXPECT_EQ(r.Byte(), 0x96);
    EXPECT_EQ(r.Byte(), 0x87);
    (void)r.Byte(); // bits
}

TEST(FieldInfosWriterTest, BitsForBkdLikeNonProxField) {
    MemoryByteOutput out;
    FieldInfosWriter writer(&out);

    FieldInfoEntry fi;
    fi.name = "id";
    fi.is_indexed = true;
    fi.has_prox = false;
    fi.omit_norms = true;
    writer.Write({fi});

    ByteReader r(out.bytes());
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 2);
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('i'));
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('d'));
    EXPECT_EQ(r.Byte(), FieldInfosWriter::kIsIndexed | FieldInfosWriter::kOmitNorms);
}

} // namespace doris::segment_v2::inverted_index::spimi
