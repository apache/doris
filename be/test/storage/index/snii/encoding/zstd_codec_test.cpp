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

#include "snii/encoding/zstd_codec.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/status.h"

using namespace snii;

TEST(SniiZstd, RoundTrip) {
    std::vector<uint8_t> in;
    for (int i = 0; i < 10000; ++i) {
        in.push_back(static_cast<uint8_t>(i % 7));
    }
    std::vector<uint8_t> comp, decomp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    EXPECT_LT(comp.size(), in.size());
    ASSERT_TRUE(zstd_decompress(Slice(comp), in.size(), &decomp).ok());
    EXPECT_EQ(decomp, in);
}

TEST(SniiZstd, WrongLenFails) {
    std::vector<uint8_t> in(100, 7), comp, decomp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    EXPECT_FALSE(zstd_decompress(Slice(comp), 99, &decomp).ok());
}

TEST(SniiZstd, EmptyInput) {
    std::vector<uint8_t> in, comp, decomp;
    ASSERT_TRUE(zstd_compress(Slice(in), 3, &comp).ok());
    ASSERT_TRUE(zstd_decompress(Slice(comp), 0, &decomp).ok());
    EXPECT_TRUE(decomp.empty());
}
