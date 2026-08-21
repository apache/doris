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

#include <gtest/gtest.h>
#include <snappy.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "util/decompressor.h"

namespace doris {

namespace {

void append_be32(std::vector<uint8_t>* buf, uint32_t value) {
    buf->push_back(static_cast<uint8_t>((value >> 24) & 0xff));
    buf->push_back(static_cast<uint8_t>((value >> 16) & 0xff));
    buf->push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    buf->push_back(static_cast<uint8_t>(value & 0xff));
}

// Build a SNAPPYBLOCK stream that holds a single small block:
//   [large block decompressed length][compressed small block length][snappy data]
// large_block_len lets the caller lie about the large-block length independently of
// the actual per-block snappy header, which is what the overflow path exploits.
std::vector<uint8_t> make_snappy_block(const std::string& raw, uint32_t large_block_len) {
    std::string compressed;
    snappy::Compress(raw.data(), raw.size(), &compressed);

    std::vector<uint8_t> stream;
    append_be32(&stream, large_block_len);
    append_be32(&stream, static_cast<uint32_t>(compressed.size()));
    stream.insert(stream.end(), compressed.begin(), compressed.end());
    return stream;
}

} // namespace

class SnappyBlockDecompressorTest : public ::testing::Test {};

// A well-formed stream round-trips fine.
TEST_F(SnappyBlockDecompressorTest, ValidStream) {
    std::string raw(4096, 'a');
    std::vector<uint8_t> stream = make_snappy_block(raw, static_cast<uint32_t>(raw.size()));

    std::unique_ptr<Decompressor> decompressor;
    ASSERT_TRUE(Decompressor::create_decompressor(CompressType::SNAPPYBLOCK, &decompressor).ok());

    std::vector<uint8_t> output(raw.size());
    size_t input_bytes_read = 0;
    size_t decompressed_len = 0;
    bool stream_end = false;
    size_t more_input_bytes = 0;
    size_t more_output_bytes = 0;

    Status st = decompressor->decompress(stream.data(), static_cast<uint32_t>(stream.size()),
                                         &input_bytes_read, output.data(),
                                         static_cast<uint32_t>(output.size()), &decompressed_len,
                                         &stream_end, &more_input_bytes, &more_output_bytes);

    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(decompressed_len, raw.size());
    EXPECT_EQ(0, memcmp(output.data(), raw.data(), raw.size()));
}

// A crafted block whose snappy header declares a decompressed length larger than the
// remaining output buffer must be rejected, not written out of bounds. The large-block
// length is set to 1 so the outer bounds check passes and execution reaches the inner
// per-block check.
TEST_F(SnappyBlockDecompressorTest, SmallBlockExceedsOutputBuffer) {
    std::string raw(4096, 'a');
    std::vector<uint8_t> stream = make_snappy_block(raw, /*large_block_len=*/1);

    std::unique_ptr<Decompressor> decompressor;
    ASSERT_TRUE(Decompressor::create_decompressor(CompressType::SNAPPYBLOCK, &decompressor).ok());

    // Output buffer far smaller than the block's real decompressed length (4096).
    std::vector<uint8_t> output(64);
    size_t input_bytes_read = 0;
    size_t decompressed_len = 0;
    bool stream_end = false;
    size_t more_input_bytes = 0;
    size_t more_output_bytes = 0;

    Status st = decompressor->decompress(stream.data(), static_cast<uint32_t>(stream.size()),
                                         &input_bytes_read, output.data(),
                                         static_cast<uint32_t>(output.size()), &decompressed_len,
                                         &stream_end, &more_input_bytes, &more_output_bytes);

    EXPECT_FALSE(st.ok());
}

// Even when the output buffer is large enough for the snappy payload, a small block whose
// decompressed length exceeds the declared large-block length must be rejected. Otherwise
// remaining_decompressed_large_block_len (uint32_t) underflows and the loop reports a bogus
// need-more-input state instead of failing on the inconsistent stream.
TEST_F(SnappyBlockDecompressorTest, SmallBlockExceedsLargeBlockLength) {
    std::string raw(4096, 'a');
    std::vector<uint8_t> stream = make_snappy_block(raw, /*large_block_len=*/1);

    std::unique_ptr<Decompressor> decompressor;
    ASSERT_TRUE(Decompressor::create_decompressor(CompressType::SNAPPYBLOCK, &decompressor).ok());

    // Output buffer large enough for the real payload, so only the large-block length check
    // can catch the inconsistency.
    std::vector<uint8_t> output(raw.size());
    size_t input_bytes_read = 0;
    size_t decompressed_len = 0;
    bool stream_end = false;
    size_t more_input_bytes = 0;
    size_t more_output_bytes = 0;

    Status st = decompressor->decompress(stream.data(), static_cast<uint32_t>(stream.size()),
                                         &input_bytes_read, output.data(),
                                         static_cast<uint32_t>(output.size()), &decompressed_len,
                                         &stream_end, &more_input_bytes, &more_output_bytes);

    EXPECT_FALSE(st.ok());
}

} // namespace doris
