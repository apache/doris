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

#include "snii/format/bootstrap_header.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/format/format_constants.h"

using namespace snii;
using namespace snii::format;
using doris::Status;

namespace {

// Fixed on-disk size of the bootstrap header (all fields + trailing crc32c).
// u32 magic + u16 format_version + u16 min_reader_version + u32 flags
//   + u32 header_length + u8 tail_pointer_size + u32 header_checksum
constexpr size_t kExpectedHeaderBytes = 4 + 2 + 2 + 4 + 4 + 1 + 4;

} // namespace

TEST(SniiBootstrapHeader, RoundTripDefaults) {
    BootstrapHeader in {}; // defaults: magic/version/min_reader_version
    in.flags = 0;
    in.tail_pointer_size = 40;

    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());
    EXPECT_EQ(sink.size(), kExpectedHeaderBytes);

    BootstrapHeader out {};
    ASSERT_TRUE(decode_bootstrap_header(sink.view(), &out).ok());
    EXPECT_EQ(out.magic, in.magic);
    EXPECT_EQ(out.format_version, in.format_version);
    EXPECT_EQ(out.min_reader_version, in.min_reader_version);
    EXPECT_EQ(out.flags, in.flags);
    EXPECT_EQ(out.header_length, kExpectedHeaderBytes);
    EXPECT_EQ(out.tail_pointer_size, in.tail_pointer_size);
}

TEST(SniiBootstrapHeader, RoundTripNonDefaultFields) {
    BootstrapHeader in {};
    in.flags = 0xDEADBEEFU;
    in.tail_pointer_size = 255;

    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    BootstrapHeader out {};
    ASSERT_TRUE(decode_bootstrap_header(sink.view(), &out).ok());
    EXPECT_EQ(out.flags, 0xDEADBEEFU);
    EXPECT_EQ(out.tail_pointer_size, 255);
    EXPECT_EQ(out.magic, kContainerMagic);
    EXPECT_EQ(out.format_version, kFormatVersion);
}

TEST(SniiBootstrapHeader, EncodeFillsHeaderLength) {
    BootstrapHeader in {};
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());
    // header_length is written into the buffer; first 4 bytes are magic LE.
    const auto& bytes = sink.buffer();
    ASSERT_EQ(bytes.size(), kExpectedHeaderBytes);
    uint32_t magic_le = static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
                        (static_cast<uint32_t>(bytes[2]) << 16) |
                        (static_cast<uint32_t>(bytes[3]) << 24);
    EXPECT_EQ(magic_le, kContainerMagic);
}

TEST(SniiBootstrapHeader, WrongMagicRejected) {
    BootstrapHeader in {};
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    auto bytes = sink.buffer();
    bytes[0] ^= 0xFF; // corrupt the magic; checksum would also fail, but magic
                      // check fires first
    BootstrapHeader out {};
    Status s = decode_bootstrap_header(Slice(bytes), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiBootstrapHeader, ChecksumMismatchRejected) {
    BootstrapHeader in {};
    in.flags = 7;
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    auto bytes = sink.buffer();
    // Flip a flags byte (offset 8 = after magic(4)+fmt(2)+min(2)); magic stays
    // valid, so only the checksum can detect this.
    bytes[8] ^= 0xFF;
    BootstrapHeader out {};
    Status s = decode_bootstrap_header(Slice(bytes), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiBootstrapHeader, MinReaderVersionTooNewUnsupported) {
    BootstrapHeader in {};
    // A future writer that demands a reader >= kFormatVersion+1: current reader
    // must refuse.
    in.min_reader_version = static_cast<uint16_t>(kFormatVersion + 1);
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    BootstrapHeader out {};
    Status s = decode_bootstrap_header(sink.view(), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>());
}

TEST(SniiBootstrapHeader, OlderFormatVersionUnsupported) {
    BootstrapHeader in {};
    in.format_version = static_cast<uint16_t>(kFormatVersion - 1);
    in.min_reader_version = static_cast<uint16_t>(kFormatVersion - 1);
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    BootstrapHeader out {};
    Status s = decode_bootstrap_header(sink.view(), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>());
}

TEST(SniiBootstrapHeader, FutureFormatVersionUnsupported) {
    BootstrapHeader in {};
    in.format_version = static_cast<uint16_t>(kFormatVersion + 1);
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    BootstrapHeader out {};
    Status s = decode_bootstrap_header(sink.view(), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>());
}

TEST(SniiBootstrapHeader, TruncatedRejected) {
    BootstrapHeader in {};
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    auto bytes = sink.buffer();
    bytes.pop_back(); // drop one checksum byte
    BootstrapHeader out {};
    Status s = decode_bootstrap_header(Slice(bytes), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiBootstrapHeader, EmptyInputRejected) {
    BootstrapHeader out {};
    std::vector<uint8_t> empty;
    Status s = decode_bootstrap_header(Slice(empty), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiBootstrapHeader, TrailingBytesAfterChecksumRejected) {
    BootstrapHeader in {};
    ByteSink sink;
    ASSERT_TRUE(encode_bootstrap_header(in, &sink).ok());

    auto bytes = sink.buffer();
    bytes.push_back(0x00); // extra byte beyond the fixed header
    BootstrapHeader out {};
    Status s = decode_bootstrap_header(Slice(bytes), &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}
