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

#include "olap/rowset/segment_v2/page_compression.h"

#include <gtest/gtest.h>
#include <iostream>

#include "common/logging.h"
#include "util/block_compression.h"

namespace doris {
namespace segment_v2 {

class PageCompressionTest : public testing::Test {
public:
    PageCompressionTest() { }
    virtual ~PageCompressionTest() {
    }
};

static std::string generate_rand_str(size_t len) {
    static char charset[] = "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string result;
    result.resize(len);
    for (int i = 0; i < len; ++i) {
        result[i] = charset[rand() % sizeof(charset)];
    }
    return result;
}

static std::string generate_str(size_t len) {
    static char charset[] = "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string result;
    result.resize(len);
    for (int i = 0; i < len; ++i) {
        result[i] = charset[i % sizeof(charset)];
    }
    return result;
}

TEST_F(PageCompressionTest, normal) {
    const BlockCompressionCodec* codec = nullptr;
    get_block_compression_codec(segment_v2::CompressionTypePB::LZ4F, &codec);

    for (int i = 0; i < 2; ++i) {
        // compress
        PageCompressor compressor(codec);

        std::vector<Slice> raw_slices;
        std::string raw_data;
        if (i == 0) {
            raw_data = generate_rand_str(102400);
        } else {
            raw_data = generate_str(102400);
        }

        raw_slices.emplace_back(raw_data.data(), 10240);
        raw_slices.emplace_back(raw_data.data() + 10240, 10240);
        raw_slices.emplace_back(raw_data.data() + 20480, 81920);

        std::vector<Slice> compressed_slices;
        auto st = compressor.compress(raw_slices, &compressed_slices);
        ASSERT_TRUE(st.ok());

        std::string compressed_data = Slice::to_string(compressed_slices);

        // decompress
        PageDecompressor decompressor(compressed_data, codec);

        {
            Slice check_slice;
            st = decompressor.decompress_to(&check_slice);
            ASSERT_TRUE(st.ok());
            ASSERT_STREQ(raw_data.c_str(), check_slice.data);
            if (check_slice.data != compressed_data.data()) {
                delete[] check_slice.data;
            }
        }
    }
}

TEST_F(PageCompressionTest, bad_case) {
    const BlockCompressionCodec* codec = nullptr;
    get_block_compression_codec(segment_v2::CompressionTypePB::LZ4F, &codec);

    for (int i = 0; i < 2; ++i) {
        // compress
        PageCompressor compressor(codec);

        std::vector<Slice> raw_slices;
        std::string raw_data;
        if (i == 0) {
            raw_data = generate_rand_str(102400);
        } else {
            raw_data = generate_str(102400);
        }
        raw_slices.emplace_back(raw_data.data(), 102400);

        std::vector<Slice> compressed_slices;
        auto st = compressor.compress(raw_slices, &compressed_slices);
        ASSERT_TRUE(st.ok());

        std::string compressed_data = Slice::to_string(compressed_slices);

        Slice bad_compressed_slice(compressed_data.data(), compressed_data.size() - 1);
        // decompress
        PageDecompressor decompressor(bad_compressed_slice, codec);

        {
            Slice check_slice;
            st = decompressor.decompress_to(&check_slice);
            ASSERT_FALSE(st.ok());
        }
    }
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

