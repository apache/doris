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
#include <lz4/lz4.h>

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

std::string lz4_compress(const std::string& raw) {
    int bound = LZ4_compressBound(static_cast<int>(raw.size()));
    std::string compressed(bound, '\0');
    int len = LZ4_compress_default(raw.data(), compressed.data(), static_cast<int>(raw.size()),
                                   bound);
    compressed.resize(len);
    return compressed;
}

// Build a Hadoop LZ4BLOCK stream:
//   [large block decompressed length][B1][compress(small block1)][B2][compress(small block2)]...
// large_block_len is written verbatim so the caller can declare a length that passes the
// outer capacity check independently of the small blocks' real decompressed size, which is
// what the stale-capacity path exploits.
std::vector<uint8_t> make_lz4_block(const std::vector<std::string>& small_blocks,
                                    uint32_t large_block_len) {
    std::vector<uint8_t> stream;
    append_be32(&stream, large_block_len);
    for (const auto& raw : small_blocks) {
        std::string compressed = lz4_compress(raw);
        append_be32(&stream, static_cast<uint32_t>(compressed.size()));
        stream.insert(stream.end(), compressed.begin(), compressed.end());
    }
    return stream;
}

Status run_decompress(const std::vector<uint8_t>& stream, std::vector<uint8_t>* output,
                      size_t* decompressed_len) {
    std::unique_ptr<Decompressor> decompressor;
    EXPECT_TRUE(Decompressor::create_decompressor(CompressType::LZ4BLOCK, &decompressor).ok());

    size_t input_bytes_read = 0;
    bool stream_end = false;
    size_t more_input_bytes = 0;
    size_t more_output_bytes = 0;

    return decompressor->decompress(const_cast<uint8_t*>(stream.data()),
                                    static_cast<uint32_t>(stream.size()), &input_bytes_read,
                                    output->data(), static_cast<uint32_t>(output->size()),
                                    decompressed_len, &stream_end, &more_input_bytes,
                                    &more_output_bytes);
}

} // namespace

class Lz4BlockDecompressorTest : public ::testing::Test {};

// A well-formed large block with two small blocks round-trips fine.
TEST_F(Lz4BlockDecompressorTest, ValidMultipleSmallBlocks) {
    std::string a(40, 'a');
    std::string b(40, 'b');
    std::vector<uint8_t> stream = make_lz4_block({a, b}, static_cast<uint32_t>(a.size() + b.size()));

    std::vector<uint8_t> output(a.size() + b.size());
    size_t decompressed_len = 0;
    Status st = run_decompress(stream, &output, &decompressed_len);

    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(decompressed_len, a.size() + b.size());
    EXPECT_EQ(0, memcmp(output.data(), a.data(), a.size()));
    EXPECT_EQ(0, memcmp(output.data() + a.size(), b.data(), b.size()));
}

// Two small blocks inside one large block, with an output buffer that only fits the first.
// The first block advances output_ptr; the second must be decompressed against the remaining
// capacity, not the fixed per-large-block capacity, otherwise it writes past the buffer.
// The declared large-block length equals the buffer size so the outer capacity check passes
// and execution reaches the inner loop.
TEST_F(Lz4BlockDecompressorTest, SecondSmallBlockExceedsRemainingOutput) {
    std::string a(40, 'a');
    std::string b(40, 'b');
    std::vector<uint8_t> stream = make_lz4_block({a, b}, /*large_block_len=*/64);

    // Buffer holds the first 40-byte block but not both (80 bytes).
    std::vector<uint8_t> output(64);
    size_t decompressed_len = 0;
    Status st = run_decompress(stream, &output, &decompressed_len);

    EXPECT_FALSE(st.ok());
}

} // namespace doris
