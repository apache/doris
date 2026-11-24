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

#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>

namespace doris::segment_v2::inverted_index {
namespace {

// The classes byte by byte, written without any of the tricks of the classifier.
ByteClasses reference_classes(const char* src, size_t size, char* lowered) {
    ByteClasses classes;
    for (size_t i = 0; i < size; ++i) {
        const auto c = static_cast<unsigned char>(src[i]);
        const bool upper = c >= 'A' && c <= 'Z';
        const bool alnum = upper || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
        if (alnum) {
            classes.alnum |= uint64_t {1} << i;
        }
        if (c >= 0xC0) {
            classes.lead |= uint64_t {1} << i;
        }
        lowered[i] = static_cast<char>(upper ? c - 'A' + 'a' : c);
    }
    return classes;
}

void expect_block_matches(const char* src) {
    char lowered[BYTE_CLASS_BLOCK];
    char expected_lowered[BYTE_CLASS_BLOCK];
    const ByteClasses classes = classify_block<true>(src, lowered);
    const ByteClasses expected = reference_classes(src, BYTE_CLASS_BLOCK, expected_lowered);
    EXPECT_EQ(classes.alnum, expected.alnum);
    EXPECT_EQ(classes.lead, expected.lead);
    EXPECT_EQ(std::string(lowered, BYTE_CLASS_BLOCK),
              std::string(expected_lowered, BYTE_CLASS_BLOCK));
    // Without lowering, the classes are the same and the output buffer is not touched.
    char untouched[BYTE_CLASS_BLOCK];
    std::memset(untouched, 0x5A, sizeof(untouched));
    const ByteClasses unlowered = classify_block<false>(src, untouched);
    EXPECT_EQ(unlowered.alnum, expected.alnum);
    EXPECT_EQ(unlowered.lead, expected.lead);
    for (char c : untouched) {
        ASSERT_EQ(c, 0x5A);
    }
}

TEST(ByteClassifierTest, EveryByteValueInEveryPosition) {
    std::array<char, BYTE_CLASS_BLOCK> block {};
    for (int value = 0; value < 256; ++value) {
        // The value alone in each position, among spaces, and filling the whole block.
        for (size_t position = 0; position < BYTE_CLASS_BLOCK; ++position) {
            block.fill(' ');
            block[position] = static_cast<char>(value);
            expect_block_matches(block.data());
        }
        block.fill(static_cast<char>(value));
        expect_block_matches(block.data());
    }
}

TEST(ByteClassifierTest, RandomBlocks) {
    std::mt19937_64 random(20261103);
    std::array<char, BYTE_CLASS_BLOCK> block {};
    for (int round = 0; round < 20000; ++round) {
        for (char& c : block) {
            c = static_cast<char>(random());
        }
        expect_block_matches(block.data());
    }
}

TEST(ByteClassifierTest, EveryTailLength) {
    std::mt19937_64 random(7);
    std::array<char, BYTE_CLASS_BLOCK> source {};
    for (size_t size = 1; size < BYTE_CLASS_BLOCK; ++size) {
        for (int round = 0; round < 200; ++round) {
            for (char& c : source) {
                c = static_cast<char>(random());
            }
            char lowered[BYTE_CLASS_BLOCK];
            std::memset(lowered, 0x5A, sizeof(lowered));
            char expected_lowered[BYTE_CLASS_BLOCK];
            const ByteClasses classes = classify_tail<true>(source.data(), size, lowered);
            const ByteClasses expected = reference_classes(source.data(), size, expected_lowered);
            ASSERT_EQ(classes.alnum, expected.alnum) << "size " << size;
            ASSERT_EQ(classes.lead, expected.lead) << "size " << size;
            ASSERT_EQ(std::string(lowered, size), std::string(expected_lowered, size));
            // Nothing past the tail is written.
            for (size_t i = size; i < BYTE_CLASS_BLOCK; ++i) {
                ASSERT_EQ(lowered[i], 0x5A);
            }
        }
    }
}

TEST(ByteClassifierTest, TailReadsNothingPastTheValue) {
    const size_t page_size = sysconf(_SC_PAGESIZE);
    void* mapping = mmap(nullptr, page_size * 3, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(mapping, MAP_FAILED);
    auto* page = static_cast<char*>(mapping) + page_size;
    ASSERT_EQ(mprotect(page, page_size, PROT_READ | PROT_WRITE), 0);
    // A tail that ends at the guard page after the readable one: reading past it would fault.
    for (size_t size = 1; size < BYTE_CLASS_BLOCK; ++size) {
        char* const value = page + page_size - size;
        std::memset(value, 'A', size);
        char lowered[BYTE_CLASS_BLOCK];
        const ByteClasses classes = classify_tail<true>(value, size, lowered);
        EXPECT_EQ(classes.alnum, (uint64_t {1} << size) - 1);
        EXPECT_EQ(std::string(lowered, size), std::string(size, 'a'));
    }
    EXPECT_EQ(munmap(mapping, page_size * 3), 0);
}

TEST(ByteClassifierTest, AsciiLowerFoldsOnlyUpperCaseAscii) {
    std::string all;
    for (int value = 0; value < 256; ++value) {
        all.push_back(static_cast<char>(value));
    }
    std::string lowered = all;
    ascii_lower(lowered);
    for (int value = 0; value < 256; ++value) {
        const char expected =
                value >= 'A' && value <= 'Z' ? static_cast<char>(value - 'A' + 'a') : all[value];
        EXPECT_EQ(lowered[value], expected) << value;
    }
}

// The build that runs the unit tests without AVX2 (USE_AVX2=0, its own CMAKE_BUILD_DIR) sets
// EXPECT_NO_AVX2 when it runs them, so a CMake cache that kept AVX2 on fails here instead of
// passing as a second AVX2 run.
TEST(ByteClassifierTest, BuiltWithTheExpectedVectorWidth) {
    const char* expect_no_avx2 = std::getenv("EXPECT_NO_AVX2");
#ifdef __AVX2__
    constexpr bool kAvx2 = true;
#else
    constexpr bool kAvx2 = false;
#endif
    if (expect_no_avx2 != nullptr && std::string(expect_no_avx2) == "1") {
        EXPECT_FALSE(kAvx2);
    } else {
        GTEST_SKIP() << "AVX2 is " << (kAvx2 ? "on" : "off") << "; set EXPECT_NO_AVX2=1 to check "
                     << "a build without it";
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index
