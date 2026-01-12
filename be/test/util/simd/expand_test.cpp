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

#include "util/simd/expand.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <vector>

namespace doris::simd {

class ExpandTest : public ::testing::Test {
protected:
    static constexpr size_t kChunkSize = 4095;

    template <class CppType>
    void test_expand_load() {
        std::vector<CppType> srcs(kChunkSize);
        std::vector<CppType> dsts(kChunkSize);

        // Initialize source data
        for (size_t i = 0; i < kChunkSize; ++i) {
            srcs[i] = static_cast<CppType>(i);
        }

        // Test 1: no nulls
        {
            std::vector<uint8_t> nulls(kChunkSize, 0);

            Expand::expand_load(dsts.data(), srcs.data(), nulls.data(), kChunkSize);
            for (size_t i = 0; i < kChunkSize; ++i) {
                ASSERT_EQ(srcs[i], dsts[i]) << "Mismatch at index " << i << " (no nulls)";
            }

            // Also test branchless directly
            std::fill(dsts.begin(), dsts.end(), CppType {});
            Expand::expand_load_branchless(dsts.data(), srcs.data(), nulls.data(), kChunkSize);
            for (size_t i = 0; i < kChunkSize; ++i) {
                ASSERT_EQ(srcs[i], dsts[i])
                        << "Mismatch at index " << i << " (branchless, no nulls)";
            }
        }

        // Test 2: all nulls
        {
            std::vector<uint8_t> nulls(kChunkSize, 1);
            // When all are nulls, we don't need to check the result values
            // Just make sure it doesn't crash
            Expand::expand_load(dsts.data(), srcs.data(), nulls.data(), kChunkSize);
            Expand::expand_load_branchless(dsts.data(), srcs.data(), nulls.data(), kChunkSize);
        }

        // Test 3: interleaved nulls (every other element is null)
        {
            std::vector<uint8_t> nulls(kChunkSize);
            for (size_t i = 0; i < kChunkSize; ++i) {
                nulls[i] = i % 2;
            }

            std::vector<CppType> dsts1(kChunkSize);
            std::vector<CppType> dsts2(kChunkSize);

            Expand::expand_load_branchless(dsts1.data(), srcs.data(), nulls.data(), kChunkSize);
            Expand::expand_load(dsts2.data(), srcs.data(), nulls.data(), kChunkSize);

            // For non-null positions, values should match
            for (size_t i = 0; i < kChunkSize; ++i) {
                if (nulls[i] == 0) {
                    ASSERT_EQ(dsts1[i], dsts2[i])
                            << "Mismatch at index " << i << " (interleaved nulls)";
                }
            }
        }

        // Test 4: enumeration pattern (systematically vary null patterns)
        {
            std::vector<uint8_t> nulls(kChunkSize, 0);
            std::vector<uint8_t> pattern(8);
            size_t idx = 0;

            for (int k = 1; k <= 8 && idx + 8 < kChunkSize; ++k) {
                std::fill(pattern.begin(), pattern.end(), 0);
                std::fill(pattern.begin(), pattern.begin() + k, 1);

                do {
                    for (int j = 0; j < 8 && idx < kChunkSize; ++j) {
                        nulls[idx++] = pattern[j];
                    }
                } while (std::next_permutation(pattern.begin(), pattern.end()) &&
                         idx + 8 < kChunkSize);
            }

            std::vector<CppType> dsts1(kChunkSize);
            std::vector<CppType> dsts2(kChunkSize);

            Expand::expand_load_branchless(dsts1.data(), srcs.data(), nulls.data(), kChunkSize);
            Expand::expand_load(dsts2.data(), srcs.data(), nulls.data(), kChunkSize);

            for (size_t i = 0; i < kChunkSize; ++i) {
                if (nulls[i] == 0) {
                    ASSERT_EQ(dsts1[i], dsts2[i])
                            << "Mismatch at index " << i << " (enumeration pattern)";
                }
            }
        }

        // Test 5: random null pattern
        {
            std::vector<uint8_t> nulls(kChunkSize);
            std::mt19937 gen(42); // Fixed seed for reproducibility
            std::uniform_int_distribution<> dis(0, 1);

            for (size_t i = 0; i < kChunkSize; ++i) {
                nulls[i] = dis(gen);
            }

            std::vector<CppType> dsts1(kChunkSize);
            std::vector<CppType> dsts2(kChunkSize);

            Expand::expand_load_branchless(dsts1.data(), srcs.data(), nulls.data(), kChunkSize);
            Expand::expand_load(dsts2.data(), srcs.data(), nulls.data(), kChunkSize);

            for (size_t i = 0; i < kChunkSize; ++i) {
                if (nulls[i] == 0) {
                    ASSERT_EQ(dsts1[i], dsts2[i])
                            << "Mismatch at index " << i << " (random null pattern)";
                }
            }
        }
    }
};

TEST_F(ExpandTest, Int32) {
    test_expand_load<int32_t>();
}

TEST_F(ExpandTest, Int64) {
    test_expand_load<int64_t>();
}

TEST_F(ExpandTest, Float) {
    test_expand_load<float>();
}

TEST_F(ExpandTest, Double) {
    test_expand_load<double>();
}

TEST_F(ExpandTest, SmallChunks) {
    // Test edge cases with small data sizes
    std::vector<int32_t> src = {1, 2, 3, 4, 5};
    std::vector<int32_t> dst(5);
    std::vector<uint8_t> nulls = {0, 1, 0, 1, 0};

    Expand::expand_load(dst.data(), src.data(), nulls.data(), 5);

    // Expected: dst[0] = src[0] = 1, dst[2] = src[1] = 2, dst[4] = src[2] = 3
    EXPECT_EQ(dst[0], 1);
    EXPECT_EQ(dst[2], 2);
    EXPECT_EQ(dst[4], 3);
}

TEST_F(ExpandTest, EmptyInput) {
    std::vector<int32_t> src;
    std::vector<int32_t> dst;
    std::vector<uint8_t> nulls;

    // Should handle empty input without crashing
    Expand::expand_load(dst.data(), src.data(), nulls.data(), 0);
}

TEST_F(ExpandTest, SingleElement) {
    int32_t src = 42;
    int32_t dst = 0;
    uint8_t null_flag = 0;

    Expand::expand_load(&dst, &src, &null_flag, 1);
    EXPECT_EQ(dst, 42);

    dst = 0;
    null_flag = 1;
    Expand::expand_load(&dst, &src, &null_flag, 1);
    // When null, value is undefined but should not crash
}

TEST_F(ExpandTest, LargeChunk) {
    // Test with a large chunk to exercise SIMD paths thoroughly
    const size_t large_size = 100000;
    std::vector<int32_t> srcs(large_size);
    std::vector<int32_t> dsts(large_size);
    std::vector<uint8_t> nulls(large_size);

    std::mt19937 gen(123);
    std::uniform_int_distribution<> dis(0, 1);

    for (size_t i = 0; i < large_size; ++i) {
        srcs[i] = static_cast<int32_t>(i);
        nulls[i] = dis(gen);
    }

    std::vector<int32_t> dsts_ref(large_size);
    Expand::expand_load_branchless(dsts_ref.data(), srcs.data(), nulls.data(), large_size);
    Expand::expand_load(dsts.data(), srcs.data(), nulls.data(), large_size);

    for (size_t i = 0; i < large_size; ++i) {
        if (nulls[i] == 0) {
            ASSERT_EQ(dsts[i], dsts_ref[i]) << "Mismatch at index " << i << " (large chunk)";
        }
    }
}

} // namespace doris::simd
