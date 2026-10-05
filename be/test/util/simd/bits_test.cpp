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

#include "util/simd/bits.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <algorithm>

#include "gtest/gtest_pred_impl.h"

namespace doris::simd {
TEST(BitsTest, BytesMaskToBitsMask) {
    // Length determined by architecture (16 on NEON/aarch64, else 32)
    constexpr auto len = bits_mask_length();
    std::vector<uint8_t> data(len, 0);
    // Mark some indices as 1 (non‑zero)
    std::vector<size_t> marked = {0, len / 2, len - 1};
    for (auto i : marked) {
        data[i] = 1;
    }

    // Build mask
    auto mask = bytes_mask_to_bits_mask(data.data());

    // Collect indices via iterate_through_bits_mask
    std::vector<size_t> collected;
    iterate_through_bits_mask([&](size_t idx) { collected.push_back(idx); }, mask);

    // Sort to compare (iterate_through_bits_mask already gives ascending, but be safe)
    std::sort(collected.begin(), collected.end());

    // Expect collected matches 'marked'
    EXPECT_EQ(collected.size(), marked.size());
    for (size_t i = 0; i < marked.size(); ++i) {
        EXPECT_EQ(collected[i], marked[i]);
    }

    // All zero -> mask == 0
    std::vector<uint8_t> zeros(len, 0);
    auto zero_mask = bytes_mask_to_bits_mask(zeros.data());
    EXPECT_EQ(zero_mask, decltype(zero_mask)(0));

    // All ones -> mask == bits_mask_all()
    std::vector<uint8_t> ones(len, 1);
    auto full_mask = bytes_mask_to_bits_mask(ones.data());
    EXPECT_EQ(full_mask, bits_mask_all());
}

TEST(BitsTest, CountZeroNum) {
    // Case 1: empty
    const int8_t* empty = nullptr;
    EXPECT_EQ(count_zero_num<size_t>(empty, size_t(0)), 0U);
    EXPECT_EQ(count_zero_num<size_t>(empty, static_cast<const uint8_t*>(nullptr), size_t(0)), 0U);

    // Case 2: all zero
    {
        std::vector<int8_t> v(10, 0);
        std::vector<uint8_t> null_map(10, 0);
        EXPECT_EQ(count_zero_num<size_t>(v.data(), v.size()), 10U);
        EXPECT_EQ(count_zero_num<size_t>(v.data(), null_map.data(), v.size()), 10U);
    }

    // Case 3: no zero, some nulls
    {
        std::vector<int8_t> v = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        std::vector<uint8_t> null_map = {0, 1, 0, 0, 1, 0, 0, 1, 0, 0};
        EXPECT_EQ(count_zero_num<size_t>(v.data(), v.size()), 0U);
        EXPECT_EQ(count_zero_num<size_t>(v.data(), null_map.data(), v.size()), 3U);
    }

    // Case 4: mixed zeros and nulls union
    {
        // zeros at 0,2,5 ; nulls at 1,4,6
        std::vector<int8_t> v = {0, 1, 0, 1, 1, 0, 1, 1};
        std::vector<uint8_t> null_map = {0, 1, 0, 0, 1, 0, 1, 0};
        EXPECT_EQ(count_zero_num<size_t>(v.data(), v.size()), 3U);
        EXPECT_EQ(count_zero_num<size_t>(v.data(), null_map.data(), v.size()), 6U);
    }

    // Case 5: large (>64) to exercise SIMD path
    {
        std::vector<int8_t> v(128);
        std::vector<uint8_t> null_map(128);
        size_t expect_zero = 0;
        size_t expect_union = 0;
        for (size_t i = 0; i < v.size(); ++i) {
            v[i] = (i % 5 == 0) ? 0 : 1;
            null_map[i] = (i % 7 == 0) ? 1 : 0;
            if (v[i] == 0) {
                ++expect_zero;
            }
            expect_union += static_cast<uint8_t>(!v[i]) | null_map[i];
        }
        EXPECT_EQ(count_zero_num<size_t>(v.data(), v.size()), expect_zero);
        EXPECT_EQ(count_zero_num<size_t>(v.data(), null_map.data(), v.size()), expect_union);
    }

    // Case 6: tail check (size not multiple of 16/64)
    {
        size_t n = 128 + 13;
        std::vector<int8_t> v(n);
        std::vector<uint8_t> null_map(n);
        size_t expect_zero = 0;
        size_t expect_union = 0;
        for (size_t i = 0; i < n; ++i) {
            v[i] = (i % 5 == 0) ? 0 : 1;
            null_map[i] = (i % 7 == 0) ? 1 : 0;
            if (v[i] == 0) {
                ++expect_zero;
            }
            expect_union += static_cast<uint8_t>(!v[i]) | null_map[i];
        }
        EXPECT_EQ(count_zero_num<size_t>(v.data(), n), expect_zero);
        EXPECT_EQ(count_zero_num<size_t>(v.data(), null_map.data(), n), expect_union);
    }
}

TEST(BitsTest, FindByte) {
    std::vector<uint8_t> v = {5, 0, 1, 7, 1, 9, 0, 3};
    EXPECT_EQ(find_byte<uint8_t>(v, 0, uint8_t(5)), 0U);
    EXPECT_EQ(find_byte<uint8_t>(v, 0, uint8_t(0)), 1U);
    EXPECT_EQ(find_byte<uint8_t>(v, 2, uint8_t(1)), 2U);
    EXPECT_EQ(find_byte<uint8_t>(v, 3, uint8_t(1)), 4U);
    EXPECT_EQ(find_byte<uint8_t>(v, 0, uint8_t(42)), v.size());
    EXPECT_EQ(find_byte<uint8_t>(v, v.size(), uint8_t(5)), v.size());
    const uint8_t* data = v.data();
    EXPECT_EQ(find_byte<uint8_t>(data, 0, 5, uint8_t(0)), 1U);
    EXPECT_EQ(find_byte<uint8_t>(data, 2, 6, uint8_t(1)), 2U);
    EXPECT_EQ(find_byte<uint8_t>(data, 3, 6, uint8_t(0)), 6U);
    EXPECT_EQ(find_byte<uint8_t>(data, 6, 6, uint8_t(3)), 6U);
}

TEST(BitsTest, ContainByte) {
    // Case 1: all zeros
    {
        std::vector<uint8_t> v = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_FALSE(contain_one(data, v.size()));
    }

    // Case 2: all ones
    {
        std::vector<uint8_t> v = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        const uint8_t* data = v.data();
        EXPECT_FALSE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 3: single zero at start
    {
        std::vector<uint8_t> v = {0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 4: single one at start
    {
        std::vector<uint8_t> v = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 5: single zero at end
    {
        std::vector<uint8_t> v = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 6: single one at end
    {
        std::vector<uint8_t> v = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 7: alternating zeros and ones
    {
        std::vector<uint8_t> v = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 8: small size zeros (less than SIMD block)
    {
        std::vector<uint8_t> v = {0, 0, 0};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_FALSE(contain_one(data, v.size()));
    }

    // Case 9: small size ones
    {
        std::vector<uint8_t> v = {1, 1, 1};
        const uint8_t* data = v.data();
        EXPECT_FALSE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 10: size = 1, zero
    {
        std::vector<uint8_t> v = {0};
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_FALSE(contain_one(data, v.size()));
    }

    // Case 11: size = 1, one
    {
        std::vector<uint8_t> v = {1};
        const uint8_t* data = v.data();
        EXPECT_FALSE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 12: large size with zero in middle
    {
        std::vector<uint8_t> v(100, 1);
        v[50] = 0;
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 13: large size with one in middle (all zeros except one)
    {
        std::vector<uint8_t> v(100, 0);
        v[50] = 1;
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 14: SIMD block boundary (32 bytes for AVX2)
    {
        std::vector<uint8_t> v(32, 1);
        const uint8_t* data = v.data();
        EXPECT_FALSE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 15: SIMD block + 1 byte
    {
        std::vector<uint8_t> v(33, 0);
        v[32] = 1;
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }

    // Case 16: multiple SIMD blocks all zeros
    {
        std::vector<uint8_t> v(128, 0);
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_FALSE(contain_one(data, v.size()));
    }

    // Case 17: multiple SIMD blocks with one at different position
    {
        std::vector<uint8_t> v(128, 0);
        v[64] = 1;
        const uint8_t* data = v.data();
        EXPECT_TRUE(contain_zero(data, v.size()));
        EXPECT_TRUE(contain_one(data, v.size()));
    }
}

TEST(BitsTest, FindOne) {
    std::vector<uint8_t> v = {5, 0, 1, 7, 1, 9, 0, 3};
    const uint8_t* data = v.data();
    EXPECT_EQ(find_one(v, 0), 2U);
    EXPECT_EQ(find_one(v, 3), 4U);
    EXPECT_EQ(find_one(v, 5), v.size());
    EXPECT_EQ(find_one(data, 0, v.size()), 2U);
    EXPECT_EQ(find_one(data, 4, v.size()), 4U);
    EXPECT_EQ(find_one(data, 5, v.size()), v.size());
}

TEST(BitsTest, FindZero) {
    std::vector<uint8_t> v = {5, 0, 1, 7, 1, 9, 0, 3};
    EXPECT_EQ(find_zero(v, 0), 1U);
    EXPECT_EQ(find_zero(v, 2), 6U);
    EXPECT_EQ(find_zero(v, 7), v.size());
}
} //namespace doris::simd
