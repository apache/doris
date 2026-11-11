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

#include "util/simd/selection.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <random>
#include <vector>

namespace doris::simd {
namespace {
constexpr size_t MASK_ROWS = bits_mask_length();
constexpr uint16_t CANARY = 0xa55a;

void check_selection(const std::array<uint8_t, MASK_ROWS>& flags, uint16_t base) {
    std::array<uint16_t, MASK_ROWS + 2> output;
    output.fill(CANARY);
    const auto count =
            append_selected_indices(bytes_mask_to_bits_mask(flags.data()), base, output.data() + 1);
    std::vector<uint16_t> expected;
    for (size_t i = 0; i < flags.size(); ++i) {
        if (flags[i]) {
            expected.push_back(base + i);
        }
    }
    ASSERT_EQ(count, expected.size());
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), output.begin() + 1));
    EXPECT_EQ(output.front(), CANARY);
    EXPECT_EQ(output.back(), CANARY);
}
} // namespace

TEST(SelectionTest, EmptyFullAndSingleBit) {
    std::array<uint8_t, MASK_ROWS> flags {};
    check_selection(flags, 0);
    flags.fill(1);
    check_selection(flags, 0);
    check_selection(flags, 65536 - MASK_ROWS);
    for (size_t bit = 0; bit < flags.size(); ++bit) {
        flags.fill(0);
        flags[bit] = 1;
        check_selection(flags, 17);
        check_selection(flags, 65536 - MASK_ROWS);
    }
}

TEST(SelectionTest, ExhaustiveByteMasks) {
    for (size_t group = 0; group < MASK_ROWS; group += 8) {
        for (unsigned mask = 0; mask < 256; ++mask) {
            std::array<uint8_t, MASK_ROWS> flags {};
            for (unsigned bit = 0; bit < 8; ++bit) {
                flags[group + bit] = (mask >> bit) & 1;
            }
            check_selection(flags, 0);
            check_selection(flags, 65536 - MASK_ROWS);
        }
    }
}

TEST(SelectionTest, ExhaustiveMixedMasks) {
    for (unsigned mask = 0; mask < 65536; ++mask) {
        std::array<uint8_t, MASK_ROWS> flags {};
        for (size_t bit = 0; bit < MASK_ROWS; ++bit) {
            const unsigned pattern = bit < 16 ? mask : ~mask;
            flags[bit] = (pattern >> (bit % 16)) & 1;
        }
        check_selection(flags, 19);
    }
}

TEST(SelectionTest, ConsecutiveMasks) {
    constexpr size_t rows = 65536 - MASK_ROWS;
    std::mt19937 random(20261029);
    for (const unsigned rate : {0, 1, 5, 10, 50, 90, 99, 100}) {
        std::vector<uint8_t> flags(rows);
        std::vector<uint16_t> expected;
        for (size_t i = 0; i < rows; ++i) {
            flags[i] = random() % 100 < rate;
            if (flags[i]) {
                expected.push_back(i);
            }
        }
        std::vector<uint16_t> output(rows + 2, CANARY);
        size_t count = 0;
        for (size_t base = 0; base < rows; base += MASK_ROWS) {
            count += append_selected_indices(bytes_mask_to_bits_mask(flags.data() + base), base,
                                             output.data() + 1 + count);
        }
        ASSERT_EQ(count, expected.size());
        EXPECT_TRUE(std::equal(expected.begin(), expected.end(), output.begin() + 1));
        EXPECT_EQ(output.front(), CANARY);
        EXPECT_EQ(output.back(), CANARY);
    }
}

} // namespace doris::simd
