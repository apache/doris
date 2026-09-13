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

#include "storage/index/snii/format/tail_pointer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {
namespace {

constexpr size_t kExpectedTailPointerSize = 31;
constexpr size_t kTailChecksumOffset = 27;

TailPointer typical_tail() {
    return TailPointer {.directory_offset = 0x0011223344556677ULL,
                        .directory_length = 0x1020304050607080ULL,
                        .directory_crc32c = 0xABCD1234U};
}

std::vector<uint8_t> encode(const TailPointer& tail) {
    ByteSink sink;
    EXPECT_TRUE(encode_tail_pointer(tail, &sink).ok());
    return sink.buffer();
}

void store_fixed32(uint32_t value, uint8_t* destination) {
    ByteSink sink;
    sink.put_fixed32(value);
    std::copy(sink.buffer().begin(), sink.buffer().end(), destination);
}

void refresh_tail_crc(std::vector<uint8_t>* bytes) {
    ASSERT_EQ(kExpectedTailPointerSize, bytes->size());
    store_fixed32(crc32c(Slice(bytes->data(), kTailChecksumOffset)),
                  bytes->data() + kTailChecksumOffset);
}

TEST(SniiTailPointer, UsesExactV1ThirtyOneByteFieldLayout) {
    const auto bytes = encode(typical_tail());
    ASSERT_EQ(kExpectedTailPointerSize, tail_pointer_size());
    ASSERT_EQ(kExpectedTailPointerSize, bytes.size());

    ByteSource source {Slice(bytes)};
    uint32_t magic = 0;
    uint16_t version = 0;
    uint64_t directory_offset = 0;
    uint64_t directory_length = 0;
    uint32_t directory_crc = 0;
    uint8_t encoded_size = 0;
    uint32_t tail_crc = 0;
    ASSERT_TRUE(source.get_fixed32(&magic).ok());
    ASSERT_TRUE(source.get_fixed16(&version).ok());
    ASSERT_TRUE(source.get_fixed64(&directory_offset).ok());
    ASSERT_TRUE(source.get_fixed64(&directory_length).ok());
    ASSERT_TRUE(source.get_fixed32(&directory_crc).ok());
    ASSERT_TRUE(source.get_u8(&encoded_size).ok());
    ASSERT_TRUE(source.get_fixed32(&tail_crc).ok());
    EXPECT_TRUE(source.eof());

    EXPECT_EQ(kTailMagic, magic);
    EXPECT_EQ(1U, version);
    EXPECT_EQ(0x0011223344556677ULL, directory_offset);
    EXPECT_EQ(0x1020304050607080ULL, directory_length);
    EXPECT_EQ(0xABCD1234U, directory_crc);
    EXPECT_EQ(kExpectedTailPointerSize, encoded_size);
    EXPECT_EQ(crc32c(Slice(bytes.data(), kTailChecksumOffset)), tail_crc);
}

TEST(SniiTailPointer, RoundTripsThroughTheExactEncodedBytes) {
    const auto expected = encode(typical_tail());
    TailPointer decoded;
    ASSERT_TRUE(decode_tail_pointer(Slice(expected), &decoded).ok());
    EXPECT_EQ(expected, encode(decoded));
}

TEST(SniiTailPointer, NullEncodeAndDecodeOutputsAreInvalidArgument) {
    EXPECT_TRUE(encode_tail_pointer(typical_tail(), nullptr).is<ErrorCode::INVALID_ARGUMENT>());
    const auto bytes = encode(typical_tail());
    EXPECT_TRUE(decode_tail_pointer(Slice(bytes), nullptr).is<ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiTailPointer, RejectsWrongVersionAsUnsupportedWithValidCrc) {
    auto bytes = encode(typical_tail());
    ASSERT_EQ(kExpectedTailPointerSize, bytes.size());
    bytes[4] = 2;
    bytes[5] = 0;
    refresh_tail_crc(&bytes);

    TailPointer decoded;
    const Status status = decode_tail_pointer(Slice(bytes), &decoded);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

TEST(SniiTailPointer, TailCrcCoversEveryPrecedingFieldByte) {
    const auto expected = encode(typical_tail());
    ASSERT_EQ(kExpectedTailPointerSize, expected.size());
    for (size_t offset = 0; offset < kTailChecksumOffset; ++offset) {
        auto corrupted = expected;
        corrupted[offset] ^= 1;
        TailPointer decoded;
        const Status status = decode_tail_pointer(Slice(corrupted), &decoded);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
                << "unprotected byte offset=" << offset << " status=" << status;
    }
}

TEST(SniiTailPointer, RejectsMagicSizeAndTailCrcCorruption) {
    const auto expected = encode(typical_tail());
    ASSERT_EQ(kExpectedTailPointerSize, expected.size());

    auto bad_magic = expected;
    bad_magic[0] ^= 1;
    refresh_tail_crc(&bad_magic);
    TailPointer decoded;
    EXPECT_TRUE(decode_tail_pointer(Slice(bad_magic), &decoded)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());

    auto bad_size = expected;
    bad_size[kTailChecksumOffset - 1] = 30;
    refresh_tail_crc(&bad_size);
    EXPECT_TRUE(decode_tail_pointer(Slice(bad_size), &decoded)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());

    auto bad_crc = expected;
    bad_crc.back() ^= 1;
    EXPECT_TRUE(decode_tail_pointer(Slice(bad_crc), &decoded)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiTailPointer, RejectsEveryTruncationAndTrailingBytes) {
    const auto expected = encode(typical_tail());
    ASSERT_EQ(kExpectedTailPointerSize, expected.size());
    TailPointer decoded;
    for (size_t size = 0; size < expected.size(); ++size) {
        const Status status = decode_tail_pointer(Slice(expected.data(), size), &decoded);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
                << "accepted truncated size=" << size << " status=" << status;
    }

    auto longer = expected;
    longer.push_back(0);
    EXPECT_TRUE(decode_tail_pointer(Slice(longer), &decoded)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

} // namespace
} // namespace doris::snii::format
