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
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <random>

#include "gtest/gtest_pred_impl.h"

namespace doris::simd {
namespace {
void fill_filter_inputs(std::vector<uint8_t>& source, std::vector<uint8_t>& filter,
                        std::vector<uint8_t>& null_map, size_t size, size_t offset, int pattern,
                        std::mt19937_64& random) {
    for (size_t i = offset; i < offset + size; ++i) {
        switch (pattern) {
        case 0:
            source[i] = 0;
            filter[i] = 1;
            break;
        case 1:
            source[i] = 1;
            filter[i] = 0;
            break;
        case 2:
            source[i] = filter[i] = 1;
            break;
        case 3:
            source[i] = filter[i] = 0xff;
            break;
        case 4:
            source[i] = 0x80;
            filter[i] = 1;
            break;
        case 5:
            source[i] = i % 2;
            filter[i] = (i + 1) % 2;
            break;
        case 6:
            source[i] = random() % 2;
            filter[i] = random() % 2;
            break;
        default:
            source[i] = static_cast<uint8_t>(random());
            filter[i] = static_cast<uint8_t>(random());
            break;
        }
        null_map[i] = pattern < 3 ? 0 : static_cast<uint8_t>(random() % 3);
    }
}

template <bool nullable>
void check_filter_counts(const std::vector<uint8_t>& source, const std::vector<uint8_t>& filter,
                         const std::vector<uint8_t>& null_map, size_t size, size_t offset) {
    auto expected = source;
    FilterCounts expected_counts;
    for (size_t i = offset; i < offset + size; ++i) {
        expected_counts.input_rows += expected[i] != 0;
        expected[i] &= filter[i];
        if constexpr (nullable) {
            expected[i] &= !null_map[i];
        }
        expected_counts.output_rows += expected[i] != 0;
    }
    auto actual = source;
    const auto counts = filter_and_count<nullable>(actual.data() + offset, filter.data() + offset,
                                                   size, null_map.data() + offset);
    EXPECT_EQ(counts.input_rows, expected_counts.input_rows);
    EXPECT_EQ(counts.output_rows, expected_counts.output_rows);
    EXPECT_EQ(actual, expected);
}
} // namespace

TEST(BitsTest, FilterAndCount) {
    std::mt19937_64 random(20261027);
    for (const size_t size : {0,  1,   7,   15,  16,  17,  31,  32,   33,   63,   64,
                              65, 127, 128, 129, 255, 256, 257, 1023, 1024, 4093, 4096}) {
        for (size_t offset = 0; offset < 16; ++offset) {
            for (int pattern = 0; pattern < 8; ++pattern) {
                std::vector<uint8_t> source(size + 32, 0xa5);
                std::vector<uint8_t> filter(size + 32);
                std::vector<uint8_t> null_map(size + 32);
                fill_filter_inputs(source, filter, null_map, size, offset, pattern, random);
                check_filter_counts<false>(source, filter, null_map, size, offset);
                check_filter_counts<true>(source, filter, null_map, size, offset);
            }
        }
    }
}

TEST(BitsTest, FilterAndCountEmpty) {
    const auto counts = filter_and_count(nullptr, nullptr, 0);
    EXPECT_EQ(counts.input_rows, 0U);
    EXPECT_EQ(counts.output_rows, 0U);
    const auto nullable_counts = filter_and_count<true>(nullptr, nullptr, 0, nullptr);
    EXPECT_EQ(nullable_counts.input_rows, 0U);
    EXPECT_EQ(nullable_counts.output_rows, 0U);
}

TEST(BitsTest, ContainsBytesGuardPages) {
    const size_t page_size = sysconf(_SC_PAGESIZE);
    void* mapping = mmap(nullptr, page_size * 3, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(mapping, MAP_FAILED);
    auto* page = static_cast<uint8_t*>(mapping) + page_size;
    if (mprotect(page, page_size, PROT_READ | PROT_WRITE) != 0) {
        munmap(mapping, page_size * 3);
        FAIL() << "mprotect failed";
    }
    for (size_t size : {size_t(0), size_t(1), size_t(15), size_t(16), size_t(17), size_t(31),
                        size_t(32), size_t(33), size_t(65), page_size}) {
        for (auto* data : {page, page + page_size - size}) {
            std::fill(data, data + size, 0);
            EXPECT_FALSE(contain_one(data, size));
            EXPECT_EQ(contain_zero(data, size), size != 0);
            std::fill(data, data + size, 255);
            EXPECT_EQ(contain_one(data, size), size != 0);
            EXPECT_FALSE(contain_zero(data, size));
            if (size != 0) {
                for (size_t position : {size_t(0), size / 2, size - 1}) {
                    data[position] = 0;
                    EXPECT_TRUE(contain_zero(data, size));
                    data[position] = 255;
                }
                std::fill(data, data + size, 0);
                for (size_t position : {size_t(0), size / 2, size - 1}) {
                    data[position] = 128;
                    EXPECT_TRUE(contain_one(data, size));
                    data[position] = 0;
                }
            }
        }
    }
    EXPECT_EQ(munmap(mapping, page_size * 3), 0);
}

TEST(BitsTest, ContainsBytesAtEveryBoundary) {
    EXPECT_FALSE(contain_one(nullptr, 0));
    EXPECT_FALSE(contain_zero(nullptr, 0));
    for (size_t size : {1U, 15U, 16U, 17U, 31U, 32U, 33U, 63U, 64U, 65U, 8160U}) {
        for (size_t offset : {0U, 1U, 7U, 15U}) {
            std::vector<uint8_t> storage(size + offset, 0);
            auto* data = storage.data() + offset;
            EXPECT_FALSE(contain_one(data, size));
            EXPECT_TRUE(contain_zero(data, size));
            for (uint8_t value : {1, 2, 128, 255}) {
                std::fill(data, data + size, value);
                EXPECT_TRUE(contain_one(data, size));
                EXPECT_FALSE(contain_zero(data, size));
                for (size_t position : {size_t(0), size / 2, size - 1}) {
                    data[position] = 0;
                    EXPECT_TRUE(contain_zero(data, size));
                    data[position] = value;
                }
                std::fill(data, data + size, 0);
                for (size_t position : {size_t(0), size / 2, size - 1}) {
                    data[position] = value;
                    EXPECT_TRUE(contain_one(data, size));
                    data[position] = 0;
                }
            }
        }
    }
}

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
