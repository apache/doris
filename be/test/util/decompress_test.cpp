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

#include "util/decompress.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "gen_cpp/Descriptors_types.h"
#include "util/compress.h"

using namespace std;

namespace doris {

// Fixture for testing class Decompressor
class DecompressorTest : public ::testing::Test {
protected:
    DecompressorTest() {
        uint8_t* ip = _input;

        for (int i = 0; i < 1024; i++) {
            for (uint8_t ch = 'a'; ch <= 'z'; ++ch) {
                *ip++ = ch;
            }

            for (uint8_t ch = 'Z'; ch >= 'A'; --ch) {
                *ip++ = ch;
            }
        }
    }

    void RunTest(THdfsCompression::type format) {
        std::unique_ptr<Codec> compressor;
        std::unique_ptr<Codec> decompressor;
        MemPool* mem_pool = new MemPool;

        EXPECT_TRUE(Codec::create_compressor(nullptr, mem_pool, true, format, &compressor).ok());
        EXPECT_TRUE(Codec::create_compressor(nullptr, mem_pool, true, format, &decompressor).ok());

        uint8_t* compressed = nullptr;
        int compressed_length = 0;
        EXPECT_TRUE(
                compressor->process_block(sizeof(_input), _input, &compressed_length, &compressed)
                        .ok());
        uint8_t* output = nullptr;
        int out_len = 0;
        EXPECT_TRUE(
                decompressor->process_block(compressed_length, compressed, &out_len, &output).ok());

        EXPECT_TRUE(memcmp(&_input, output, sizeof(_input)) == 0);

        // Try again specifying the output buffer and length.
        out_len = sizeof(_input);
        output = mem_pool->allocate(out_len);
        EXPECT_TRUE(
                decompressor->process_block(compressed_length, compressed, &out_len, &output).ok());

        EXPECT_TRUE(memcmp(&_input, output, sizeof(_input)) == 0);
    }

private:
    uint8_t _input[2 * 26 * 1024];
};

TEST_F(DecompressorTest, Default) {
    RunTest(THdfsCompression::DEFAULT);
}

TEST_F(DecompressorTest, Gzip) {
    RunTest(THdfsCompression::GZIP);
}

TEST_F(DecompressorTest, Deflate) {
    RunTest(THdfsCompression::DEFLATE);
}

TEST_F(DecompressorTest, Bzip) {
    RunTest(THdfsCompression::BZIP2);
}

TEST_F(DecompressorTest, Snappy) {
    RunTest(THdfsCompression::SNAPPY);
}

TEST_F(DecompressorTest, SnappyBlocked) {
    RunTest(THdfsCompression::SNAPPY_BLOCKED);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
