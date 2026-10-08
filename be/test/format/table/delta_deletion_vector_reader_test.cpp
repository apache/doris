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

#include "format/table/delta_deletion_vector_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "exec/common/endian.h"
#include "roaring/roaring.hh"
#include "util/hash_util.hpp"

namespace doris {
namespace {

constexpr uint32_t NATIVE_BITMAP_ARRAY_MAGIC = 1681511376;
constexpr uint32_t PORTABLE_BITMAP_ARRAY_MAGIC = 1681511377;
constexpr char Z85_ALPHABET[] =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

void append_little_endian32(std::vector<char>* output, uint32_t value) {
    const size_t start = output->size();
    output->resize(start + sizeof(value));
    LittleEndian::Store32(output->data() + start, value);
}

void append_little_endian64(std::vector<char>* output, uint64_t value) {
    const size_t start = output->size();
    output->resize(start + sizeof(value));
    LittleEndian::Store64(output->data() + start, value);
}

std::vector<char> serialize_roaring(const roaring::Roaring& bitmap) {
    std::vector<char> bytes(bitmap.getSizeInBytes());
    EXPECT_EQ(bitmap.write(bytes.data()), bytes.size());
    return bytes;
}

std::vector<char> native_payload(const std::vector<roaring::Roaring>& bitmaps) {
    std::vector<char> payload;
    append_little_endian32(&payload, NATIVE_BITMAP_ARRAY_MAGIC);
    append_little_endian32(&payload, bitmaps.size());
    for (const auto& bitmap : bitmaps) {
        const auto serialized = serialize_roaring(bitmap);
        append_little_endian32(&payload, serialized.size());
        payload.insert(payload.end(), serialized.begin(), serialized.end());
    }
    return payload;
}

std::vector<char> portable_payload(
        const std::vector<std::pair<uint32_t, roaring::Roaring>>& bitmaps) {
    std::vector<char> payload;
    append_little_endian32(&payload, PORTABLE_BITMAP_ARRAY_MAGIC);
    append_little_endian64(&payload, bitmaps.size());
    for (const auto& [key, bitmap] : bitmaps) {
        append_little_endian32(&payload, key);
        const auto serialized = serialize_roaring(bitmap);
        payload.insert(payload.end(), serialized.begin(), serialized.end());
    }
    return payload;
}

std::vector<char> wrap_sidecar(const std::vector<char>& payload) {
    std::vector<char> result(payload.size() + 8);
    BigEndian::Store32(result.data(), payload.size());
    std::copy(payload.begin(), payload.end(), result.begin() + 4);
    BigEndian::Store32(result.data() + 4 + payload.size(),
                       HashUtil::zlib_crc_hash(payload.data(), payload.size(), 0));
    return result;
}

std::string encode_z85(const std::vector<char>& input) {
    std::vector<char> padded = input;
    padded.resize((padded.size() + 3) / 4 * 4);
    std::string encoded;
    encoded.reserve(padded.size() / 4 * 5);
    for (size_t position = 0; position < padded.size(); position += 4) {
        uint64_t value = BigEndian::Load32(padded.data() + position);
        char block[5];
        for (int i = 4; i >= 0; --i) {
            block[i] = Z85_ALPHABET[value % 85];
            value /= 85;
        }
        encoded.append(block, sizeof(block));
    }
    return encoded;
}

TEST(DeltaDeletionVectorReaderTest, DecodesNativeSidecar) {
    roaring::Roaring low;
    low.add(1);
    low.add(3);
    roaring::Roaring high;
    high.add(7);
    const auto sidecar = wrap_sidecar(native_payload({low, high}));

    DeletionVector deletion_vector;
    ASSERT_TRUE(
            decode_delta_deletion_vector_buffer(sidecar.data(), sidecar.size(), 3, &deletion_vector)
                    .ok());
    EXPECT_EQ(deletion_vector.cardinality(), 3);
    EXPECT_TRUE(deletion_vector.contains(uint64_t {1}));
    EXPECT_TRUE(deletion_vector.contains(uint64_t {3}));
    EXPECT_TRUE(deletion_vector.contains((uint64_t {1} << 32) | 7));
}

TEST(DeltaDeletionVectorReaderTest, DecodesPortableSidecar) {
    roaring::Roaring low;
    low.add(11);
    roaring::Roaring high;
    high.add(13);
    const auto sidecar = wrap_sidecar(portable_payload({{0, low}, {2, high}}));

    DeletionVector deletion_vector;
    ASSERT_TRUE(
            decode_delta_deletion_vector_buffer(sidecar.data(), sidecar.size(), 2, &deletion_vector)
                    .ok());
    EXPECT_TRUE(deletion_vector.contains(uint64_t {11}));
    EXPECT_TRUE(deletion_vector.contains((uint64_t {2} << 32) | 13));
}

TEST(DeltaDeletionVectorReaderTest, DecodesInlineZ85) {
    roaring::Roaring bitmap;
    bitmap.add(21);
    bitmap.add(34);
    const auto payload = native_payload({bitmap});

    DeletionVector deletion_vector;
    ASSERT_TRUE(decode_delta_inline_deletion_vector(encode_z85(payload), payload.size(), 2,
                                                    &deletion_vector)
                        .ok());
    EXPECT_TRUE(deletion_vector.contains(uint64_t {21}));
    EXPECT_TRUE(deletion_vector.contains(uint64_t {34}));
}

TEST(DeltaDeletionVectorReaderTest, RejectsCorruptSidecarAndCardinality) {
    roaring::Roaring bitmap;
    bitmap.add(1);
    auto sidecar = wrap_sidecar(native_payload({bitmap}));
    sidecar[4] ^= 1;

    DeletionVector corrupt_result;
    EXPECT_FALSE(
            decode_delta_deletion_vector_buffer(sidecar.data(), sidecar.size(), 1, &corrupt_result)
                    .ok());

    sidecar = wrap_sidecar(native_payload({bitmap}));
    DeletionVector cardinality_result;
    EXPECT_FALSE(decode_delta_deletion_vector_buffer(sidecar.data(), sidecar.size(), 2,
                                                     &cardinality_result)
                         .ok());
}

TEST(DeltaDeletionVectorReaderTest, ResolvesPathAndUuidStorage) {
    std::string path;
    ASSERT_TRUE(
            resolve_delta_deletion_vector_path("p", "s3://bucket/table/dv.bin", "", &path).ok());
    EXPECT_EQ(path, "s3://bucket/table/dv.bin");

    std::vector<char> uuid_bytes(16);
    for (size_t i = 0; i < uuid_bytes.size(); ++i) {
        uuid_bytes[i] = static_cast<char>(i);
    }
    ASSERT_TRUE(resolve_delta_deletion_vector_path("u", "prefix/" + encode_z85(uuid_bytes),
                                                   "s3://bucket/table", &path)
                        .ok());
    EXPECT_EQ(path,
              "s3://bucket/table/prefix/deletion_vector_00010203-0405-0607-0809-0a0b0c0d0e0f.bin");
}

TEST(DeltaDeletionVectorReaderTest, ValidatesSidecarReadRange) {
    size_t bytes_read = 0;
    ASSERT_TRUE(validate_delta_deletion_vector_read_range(4, 16, bytes_read).ok());
    EXPECT_EQ(bytes_read, 24);
    EXPECT_FALSE(validate_delta_deletion_vector_read_range(-1, 16, bytes_read).ok());
    EXPECT_FALSE(validate_delta_deletion_vector_read_range(0, (1L << 30) + 1, bytes_read).ok());
}

} // namespace
} // namespace doris
