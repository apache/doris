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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <vector>

#include "util/rle_encoding.h"

using namespace doris;

static faststring build_rle(const std::vector<uint32_t>& values, int bit_width) {
    faststring buf;
    RleEncoder<uint32_t> enc(&buf, bit_width);
    for (auto v : values) enc.Put(v);
    enc.Flush();
    return buf;
}

TEST(RleBatchDecoderTest, TestGetBatchWithDictBasic) {
    // dictionary = {10,20,30,40}
    uint32_t dict[] = {10, 20, 30, 40};
    // indices: 0,1,2,3,2,1,0
    std::vector<uint32_t> idx = {0, 1, 2, 3, 2, 1, 0};
    auto buf = build_rle(idx, 2);
    RleBatchDecoder<uint32_t> dec(reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())),
                                  buf.size(), 2);

    uint32_t out[10] {};
    auto n = dec.GetBatchWithDict(dict, 4, out, 7);
    ASSERT_EQ(n, 7u);
    ASSERT_EQ(out[0], 10u);
    ASSERT_EQ(out[1], 20u);
    ASSERT_EQ(out[2], 30u);
    ASSERT_EQ(out[3], 40u);
    ASSERT_EQ(out[4], 30u);
    ASSERT_EQ(out[5], 20u);
    ASSERT_EQ(out[6], 10u);
}

TEST(RleBatchDecoderTest, TestGetBatchWithDictOutOfRange) {
    uint32_t dict[] = {1, 2, 3};
    std::vector<uint32_t> idx = {0, 1, 2, 3}; // 3 is out of range
    auto buf = build_rle(idx, 2);
    RleBatchDecoder<uint32_t> dec(reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())),
                                  buf.size(), 2);

    uint32_t out[4] {};
    auto n = dec.GetBatchWithDict(dict, 3, out, 4);
    // out-of-range should return (uint32_t)-1
    ASSERT_EQ(n, static_cast<uint32_t>(-1));
}

TEST(RleBatchDecoderTest, TestGetBatchWithDictTruncated) {
    uint32_t dict[] = {1, 2, 3};
    // create a buffer and then truncate it to simulate truncated input
    std::vector<uint32_t> idx = {0, 1, 2, 0, 1, 2, 0, 1};
    auto buf = build_rle(idx, 2);
    // truncate
    auto truncated_len = buf.size() / 2;
    RleBatchDecoder<uint32_t> dec(reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())),
                                  truncated_len, 2);

    uint32_t out[8] {};
    auto n = dec.GetBatchWithDict(dict, 3, out, 8);
    // truncated input should return 0
    ASSERT_EQ(n, 0u);
}
