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

#include "util/block_compression.h"

#include <gtest/gtest.h>

#include <iostream>

namespace doris {
class BlockCompressionTest : public testing::Test {
public:
    BlockCompressionTest() {}
    virtual ~BlockCompressionTest() {}
};

static std::string generate_str(size_t len) {
    static char charset[] =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string result;
    result.resize(len);
    for (int i = 0; i < len; ++i) {
        result[i] = charset[rand() % sizeof(charset)];
    }
    return result;
}

void test_single_slice(segment_v2::CompressionTypePB type) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    EXPECT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    for (auto size : test_sizes) {
        auto orig = generate_str(size);
        size_t max_len = codec->max_compressed_len(size);
        std::string compressed;
        compressed.resize(max_len);
        {
            Slice compressed_slice(compressed);
            st = codec->compress(orig, &compressed_slice);
            EXPECT_TRUE(st.ok());

            std::string uncompressed;
            uncompressed.resize(size);
            {
                Slice uncompressed_slice(uncompressed);
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                EXPECT_TRUE(st.ok());

                EXPECT_STREQ(orig.c_str(), uncompressed.c_str());
            }
            // buffer not enough for decompress
            // snappy has no return value if given buffer is not enough
            // NOTE: For ZLIB, we even get OK with a insufficient output
            // when uncompressed size is 1
            if ((type == segment_v2::CompressionTypePB::ZLIB && uncompressed.size() > 1) &&
                type != segment_v2::CompressionTypePB::SNAPPY && uncompressed.size() > 0) {
                Slice uncompressed_slice(uncompressed);
                uncompressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                EXPECT_FALSE(st.ok());
            }
            // corrupt compressed data
            if (type != segment_v2::CompressionTypePB::SNAPPY) {
                Slice uncompressed_slice(uncompressed);
                compressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                EXPECT_FALSE(st.ok());
                compressed_slice.size += 1;
            }
        }
        // buffer not enough for compress
        if (type != segment_v2::CompressionTypePB::SNAPPY && size > 0) {
            Slice compressed_slice(compressed);
            compressed_slice.size = 1;
            st = codec->compress(orig, &compressed_slice);
            EXPECT_FALSE(st.ok());
        }
    }
}

TEST_F(BlockCompressionTest, single) {
    test_single_slice(segment_v2::CompressionTypePB::SNAPPY);
    test_single_slice(segment_v2::CompressionTypePB::ZLIB);
    test_single_slice(segment_v2::CompressionTypePB::LZ4);
    test_single_slice(segment_v2::CompressionTypePB::LZ4F);
}

void test_multi_slices(segment_v2::CompressionTypePB type) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    EXPECT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    std::vector<std::string> orig_strs;
    for (auto size : test_sizes) {
        orig_strs.emplace_back(generate_str(size));
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    size_t max_len = codec->max_compressed_len(total_size);

    std::string compressed;
    compressed.resize(max_len);
    {
        Slice compressed_slice(compressed);
        st = codec->compress(orig_slices, &compressed_slice);
        EXPECT_TRUE(st.ok());

        std::string uncompressed;
        uncompressed.resize(total_size);
        // normal case
        {
            Slice uncompressed_slice(uncompressed);
            st = codec->decompress(compressed_slice, &uncompressed_slice);
            EXPECT_TRUE(st.ok());

            EXPECT_STREQ(orig.c_str(), uncompressed.c_str());
        }
    }

    // buffer not enough failed
    if (type != segment_v2::CompressionTypePB::SNAPPY) {
        Slice compressed_slice(compressed);
        compressed_slice.size = 10;
        st = codec->compress(orig, &compressed_slice);
        EXPECT_FALSE(st.ok());
    }
}

TEST_F(BlockCompressionTest, multi) {
    test_multi_slices(segment_v2::CompressionTypePB::SNAPPY);
    test_multi_slices(segment_v2::CompressionTypePB::ZLIB);
    test_multi_slices(segment_v2::CompressionTypePB::LZ4);
    test_multi_slices(segment_v2::CompressionTypePB::LZ4F);
}

} // namespace doris
