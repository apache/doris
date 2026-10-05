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

// Must come before gtest.h.
#include "util/rle_encoding.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <time.h>

#include <algorithm>
#include <boost/utility/binary.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"
#include "util/bit_util.h"
#include "util/debug_util.h"
#include "util/faststring.h"

using std::string;
using std::vector;

namespace doris {

const int kMaxWidth = 64;

class TestRle : public testing::Test {};
// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != nullptr, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
template <typename T>
void ValidateRle(const std::vector<T>& values, int bit_width, uint8_t* expected_encoding,
                 int expected_len) {
    faststring buffer;
    RleEncoder<T> encoder(&buffer, bit_width);

    for (const auto& value : values) {
        encoder.Put(value);
    }
    int encoded_len = encoder.Flush();

    if (expected_len != -1) {
        EXPECT_EQ(encoded_len, expected_len);
    }
    if (expected_encoding != nullptr) {
        EXPECT_EQ(memcmp(buffer.data(), expected_encoding, expected_len), 0)
                << "\n"
                << "Expected: " << hexdump((const char*)expected_encoding, expected_len) << "\n"
                << "Got:      " << hexdump((const char*)buffer.data(), buffer.size());
    }

    // Verify read
    RleDecoder<T> decoder(buffer.data(), encoded_len, bit_width);
    for (const auto& value : values) {
        T val = 0;
        bool result = decoder.Get(&val);
        EXPECT_TRUE(result);
        EXPECT_EQ(value, val);
    }
}

TEST(Rle, SpecificSequences) {
    const int kTestLen = 1024;
    uint8_t expected_buffer[kTestLen];
    std::vector<uint64_t> values;

    // Test 50 0' followed by 50 1's
    values.resize(100);
    for (int i = 0; i < 50; ++i) {
        values[i] = 0;
    }
    for (int i = 50; i < 100; ++i) {
        values[i] = 1;
    }

    // expected_buffer valid for bit width <= 1 byte
    expected_buffer[0] = (50 << 1);
    expected_buffer[1] = 0;
    expected_buffer[2] = (50 << 1);
    expected_buffer[3] = 1;
    for (int width = 1; width <= 8; ++width) {
        ValidateRle(values, width, expected_buffer, 4);
    }

    for (int width = 9; width <= kMaxWidth; ++width) {
        ValidateRle(values, width, nullptr, 2 * (1 + BitUtil::Ceil(width, 8)));
    }

    // Test 100 0's and 1's alternating
    for (int i = 0; i < 100; ++i) {
        values[i] = i % 2;
    }
    int num_groups = BitUtil::Ceil(100, 8);
    expected_buffer[0] = (num_groups << 1) | 1;
    for (int i = 0; i < 100 / 8; ++i) {
        expected_buffer[i + 1] = BOOST_BINARY(1 0 1 0 1 0 1 0); // 0xaa
    }
    // Values for the last 4 0 and 1's
    expected_buffer[1 + 100 / 8] = BOOST_BINARY(0 0 0 0 1 0 1 0); // 0x0a

    // num_groups and expected_buffer only valid for bit width = 1
    ValidateRle(values, 1, expected_buffer, 1 + num_groups);
    for (int width = 2; width <= kMaxWidth; ++width) {
        ValidateRle(values, width, nullptr, 1 + BitUtil::Ceil(width * 100, 8));
    }
}

// ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
// is used, otherwise alternating values are used.
void TestRleValues(int bit_width, int num_vals, int value = -1) {
    const uint64_t mod = bit_width == 64 ? 1ULL : 1ULL << bit_width;
    std::vector<uint64_t> values;
    for (uint64_t v = 0; v < num_vals; ++v) {
        values.push_back((value != -1) ? value : (bit_width == 64 ? v : (v % mod)));
    }
    ValidateRle(values, bit_width, nullptr, -1);
}

TEST(Rle, TestValues) {
    for (int width = 1; width <= kMaxWidth; ++width) {
        TestRleValues(width, 1);
        TestRleValues(width, 1024);
        TestRleValues(width, 1024, 0);
        TestRleValues(width, 1024, 1);
    }
}

class BitRle : public testing::Test {
public:
    BitRle() {}

    virtual ~BitRle() {}
};

// Tests all true/false values
TEST_F(BitRle, AllSame) {
    const int kTestLen = 1024;
    std::vector<bool> values;

    for (int v = 0; v < 2; ++v) {
        values.clear();
        for (int i = 0; i < kTestLen; ++i) {
            values.push_back(v ? true : false);
        }

        ValidateRle(values, 1, nullptr, 3);
    }
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST_F(BitRle, Flush) {
    std::vector<bool> values;
    for (int i = 0; i < 16; ++i) values.push_back(1);
    values.push_back(false);
    ValidateRle(values, 1, nullptr, -1);
    values.push_back(true);
    ValidateRle(values, 1, nullptr, -1);
    values.push_back(true);
    ValidateRle(values, 1, nullptr, -1);
    values.push_back(true);
    ValidateRle(values, 1, nullptr, -1);
}

// Test some random bool sequences.
TEST_F(BitRle, RandomBools) {
    int iters = 0;
    const int n_iters = LOOP_LESS_OR_MORE(5, 20);
    while (iters < n_iters) {
        srand(iters++);
        if (iters % 10000 == 0) LOG(ERROR) << "Seed: " << iters;
        std::vector<uint64_t> values;
        bool parity = 0;
        for (int i = 0; i < 1000; ++i) {
            int group_size = rand() % 20 + 1; // NOLINT(*)
            if (group_size > 16) {
                group_size = 1;
            }
            for (int i = 0; i < group_size; ++i) {
                values.push_back(parity);
            }
            parity = !parity;
        }
        ValidateRle(values, (iters % kMaxWidth) + 1, nullptr, -1);
    }
}

// Test some random 64-bit sequences.
TEST_F(BitRle, Random64Bit) {
    int iters = 0;
    const int n_iters = LOOP_LESS_OR_MORE(5, 20);
    while (iters < n_iters) {
        srand(iters++);
        if (iters % 10000 == 0) LOG(ERROR) << "Seed: " << iters;
        std::vector<uint64_t> values;
        for (int i = 0; i < LOOP_LESS_OR_MORE(10, 1000); ++i) {
            int group_size = rand() % 20 + 1; // NOLINT(*)
            uint64_t cur_value =
                    (static_cast<uint64_t>(rand()) << 32) + static_cast<uint64_t>(rand());
            if (group_size > 16) {
                group_size = 1;
            }
            for (int i = 0; i < group_size; ++i) {
                values.push_back(cur_value);
            }
        }
        ValidateRle(values, 64, nullptr, -1);
    }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST_F(BitRle, RepeatedPattern) {
    std::vector<bool> values;
    const int min_run = 1;
    const int max_run = 32;

    for (int i = min_run; i <= max_run; ++i) {
        int v = i % 2;
        for (int j = 0; j < i; ++j) {
            values.push_back(v);
        }
    }

    // And go back down again
    for (int i = max_run; i >= min_run; --i) {
        int v = i % 2;
        for (int j = 0; j < i; ++j) {
            values.push_back(v);
        }
    }

    ValidateRle(values, 1, nullptr, -1);
}

TEST_F(TestRle, TestBulkPut) {
    size_t run_length;
    bool val = false;

    faststring buffer(1);
    RleEncoder<bool> encoder(&buffer, 1);
    encoder.Put(true, 10);
    encoder.Put(false, 7);
    encoder.Put(true, 5);
    encoder.Put(true, 15);
    encoder.Flush();

    RleDecoder<bool> decoder(buffer.data(), encoder.len(), 1);
    run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
    EXPECT_TRUE(val);
    EXPECT_EQ(10, run_length);

    run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
    EXPECT_FALSE(val);
    EXPECT_EQ(7, run_length);

    run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
    EXPECT_TRUE(val);
    EXPECT_EQ(20, run_length);

    EXPECT_EQ(0, decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max()));
}

TEST_F(TestRle, TestGetNextRun) {
    // Repeat the test with different number of items
    for (int num_items = 7; num_items < 200; num_items += 13) {
        // Test different block patterns
        //    1: 01010101 01010101
        //    2: 00110011 00110011
        //    3: 00011100 01110001
        //    ...
        for (int block = 1; block <= 20; ++block) {
            faststring buffer(1);
            RleEncoder<bool> encoder(&buffer, 1);
            for (int j = 0; j < num_items; ++j) {
                encoder.Put(!!(j & 1), block);
            }
            encoder.Flush();

            RleDecoder<bool> decoder(buffer.data(), encoder.len(), 1);
            size_t count = num_items * block;
            for (int j = 0; j < num_items; ++j) {
                size_t run_length;
                bool val = false;
                DCHECK_GT(count, 0);
                run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
                run_length = std::min(run_length, count);

                EXPECT_EQ(!!(j & 1), val);
                EXPECT_EQ(block, run_length);
                count -= run_length;
            }
            DCHECK_EQ(count, 0);
        }
    }
}

// Generate a random bit string which consists of 'num_runs' runs,
// each with a random length between 1 and 100. Returns the number
// of values encoded (i.e the sum run length).
static size_t GenerateRandomBitString(int num_runs, faststring* enc_buf, string* string_rep) {
    RleEncoder<bool> enc(enc_buf, 1);
    int num_bits = 0;
    for (int i = 0; i < num_runs; i++) {
        int run_length = random() % 100;
        bool value = static_cast<bool>(i & 1);
        enc.Put(value, run_length);
        string_rep->append(run_length, value ? '1' : '0');
        num_bits += run_length;
    }
    enc.Flush();
    return num_bits;
}

TEST_F(TestRle, TestRoundTripRandomSequencesWithRuns) {
    srand(time(nullptr));

    // Test the limiting function of GetNextRun.
    const int kMaxToReadAtOnce = (random() % 20) + 1;

    // Generate a bunch of random bit sequences, and "round-trip" them
    // through the encode/decode sequence.
    for (int rep = 0; rep < 100; rep++) {
        faststring buf;
        std::string string_rep;
        int num_bits = GenerateRandomBitString(10, &buf, &string_rep);
        RleDecoder<bool> decoder(buf.data(), buf.size(), 1);
        std::string roundtrip_str;
        int rem_to_read = num_bits;
        size_t run_len;
        bool val;
        while (rem_to_read > 0 &&
               (run_len = decoder.GetNextRun(&val, std::min(kMaxToReadAtOnce, rem_to_read))) != 0) {
            EXPECT_LE(run_len, kMaxToReadAtOnce);
            roundtrip_str.append(run_len, val ? '1' : '0');
            rem_to_read -= run_len;
        }

        EXPECT_EQ(string_rep, roundtrip_str);
    }
}
TEST_F(TestRle, TestSkip) {
    faststring buffer(1);
    RleEncoder<bool> encoder(&buffer, 1);

    // 0101010[1] 01010101 01
    //        "A"
    for (int j = 0; j < 18; ++j) {
        encoder.Put(!!(j & 1));
    }

    // 0011[00] 11001100 11001100 11001100 11001100
    //      "B"
    for (int j = 0; j < 19; ++j) {
        encoder.Put(!!(j & 1), 2);
    }

    // 000000000000 11[1111111111] 000000000000 111111111111
    //                   "C"
    // 000000000000 111111111111 0[00000000000] 111111111111
    //                                  "D"
    // 000000000000 111111111111 000000000000 111111111111
    for (int j = 0; j < 12; ++j) {
        encoder.Put(!!(j & 1), 12);
    }
    encoder.Flush();

    bool val = false;
    size_t run_length;
    RleDecoder<bool> decoder(buffer.data(), encoder.len(), 1);

    // position before "A"
    EXPECT_EQ(3, decoder.Skip(7));
    run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
    EXPECT_TRUE(val);
    EXPECT_EQ(1, run_length);

    // position before "B"
    EXPECT_EQ(7, decoder.Skip(14));
    run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
    EXPECT_FALSE(val);
    EXPECT_EQ(2, run_length);

    // position before "C"
    EXPECT_EQ(18, decoder.Skip(46));
    run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
    EXPECT_TRUE(val);
    EXPECT_EQ(10, run_length);

    // position before "D"
    EXPECT_EQ(24, decoder.Skip(49));
    run_length = decoder.GetNextRun(&val, std::numeric_limits<std::size_t>::max());
    EXPECT_FALSE(val);
    EXPECT_EQ(11, run_length);

    encoder.Flush();
}

// Helper to compare Put with run_length vs multiple Put(value) calls
template <typename T>
void ValidatePutRunLength(const std::vector<std::pair<T, size_t>>& runs, int bit_width) {
    // Encode using Put(value, run_length)
    faststring buffer1;
    RleEncoder<T> encoder1(&buffer1, bit_width);
    for (const auto& [value, length] : runs) {
        encoder1.Put(value, length);
    }
    encoder1.Flush();

    // Encode using multiple Put(value) calls
    faststring buffer2;
    RleEncoder<T> encoder2(&buffer2, bit_width);
    for (const auto& [value, length] : runs) {
        for (size_t i = 0; i < length; ++i) {
            encoder2.Put(value);
        }
    }
    encoder2.Flush();

    // Both should produce identical output
    EXPECT_EQ(buffer1.size(), buffer2.size());
    EXPECT_EQ(memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0)
            << "Put with run_length produced different output than individual Puts";

    // Verify decoding produces correct values
    RleDecoder<T> decoder(buffer1.data(), buffer1.size(), bit_width);
    for (const auto& [value, length] : runs) {
        for (size_t i = 0; i < length; ++i) {
            T decoded_val;
            EXPECT_TRUE(decoder.Get(&decoded_val));
            EXPECT_EQ(value, decoded_val);
        }
    }
}

// Test Put(value, run_length) with various patterns
TEST_F(TestRle, TestPutWithRunLength) {
    // Test 1: Single long run (should use fast path after 8 values)
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{42, 100}};
        ValidatePutRunLength(runs, 8);
    }

    // Test 2: Run length exactly 8 (boundary case)
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{1, 8}, {2, 8}, {3, 8}};
        ValidatePutRunLength(runs, 8);
    }

    // Test 3: Run length less than 8
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{1, 3}, {2, 5}, {3, 7}};
        ValidatePutRunLength(runs, 8);
    }

    // Test 4: Run length crossing buffer boundary (e.g., 5 + 10 with same value)
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{1, 5}, {1, 10}};
        ValidatePutRunLength(runs, 8);
    }

    // Test 5: Alternating values with various run lengths
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{0, 7}, {1, 9}, {0, 15}, {1, 3}, {0, 20}};
        ValidatePutRunLength(runs, 8);
    }

    // Test 6: Very long runs (test fast path)
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{0, 1000}, {1, 1000}, {0, 1000}};
        ValidatePutRunLength(runs, 8);
    }

    // Test 7: Run length of 1 (no batching benefit)
    {
        std::vector<std::pair<uint64_t, size_t>> runs;
        for (int i = 0; i < 100; ++i) {
            runs.push_back({static_cast<uint64_t>(i % 10), 1});
        }
        ValidatePutRunLength(runs, 8);
    }

    // Test 8: Boolean values
    {
        std::vector<std::pair<bool, size_t>> runs = {
                {true, 10}, {false, 7}, {true, 5}, {true, 15}, {false, 20}};
        ValidatePutRunLength(runs, 1);
    }

    // Test 9: Large bit width (64-bit)
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {
                {0xFFFFFFFFFFFFFFFFULL, 50}, {0x123456789ABCDEF0ULL, 30}, {0, 100}};
        ValidatePutRunLength(runs, 64);
    }

    // Test 10: Mixed short and long runs
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{1, 2},  {2, 100}, {3, 1},
                                                         {4, 50}, {5, 3},   {6, 200}};
        ValidatePutRunLength(runs, 8);
    }
}

// Test Put(value, run_length) specifically for the buffering logic
TEST_F(TestRle, TestPutRunLengthBuffering) {
    // Test case: value changes while buffer is partially filled
    // This tests the "value changed" branch in the while loop
    {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, 8);

        // Put 3 values of 'A', then 5 values of 'B' (total 8, triggers flush)
        encoder.Put(10, 3);
        encoder.Put(20, 5);
        encoder.Flush();

        // Decode and verify
        RleDecoder<uint64_t> decoder(buffer.data(), buffer.size(), 8);
        uint64_t val;
        for (int i = 0; i < 3; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(10, val);
        }
        for (int i = 0; i < 5; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(20, val);
        }
    }

    // Test case: run_length spans multiple buffer fills
    {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, 8);

        // Put 25 values (spans 3 buffer fills: 8 + 8 + 8 + 1)
        encoder.Put(42, 25);
        encoder.Flush();

        RleDecoder<uint64_t> decoder(buffer.data(), buffer.size(), 8);
        uint64_t val;
        for (int i = 0; i < 25; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(42, val);
        }
    }

    // Test case: existing repeated run (repeat_count >= 8) then value changes
    {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, 8);

        // Build up a repeated run
        encoder.Put(1, 20);
        // Now change value - should flush the repeated run
        encoder.Put(2, 5);
        encoder.Flush();

        RleDecoder<uint64_t> decoder(buffer.data(), buffer.size(), 8);
        uint64_t val;
        for (int i = 0; i < 20; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(1, val);
        }
        for (int i = 0; i < 5; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(2, val);
        }
    }
}

// Test Put(value, run_length=0) edge case
TEST_F(TestRle, TestPutRunLengthZero) {
    faststring buffer;
    RleEncoder<uint64_t> encoder(&buffer, 8);

    encoder.Put(1, 10);
    encoder.Put(2, 0); // Should be a no-op
    encoder.Put(1, 5); // Should continue the run of 1s
    encoder.Flush();

    RleDecoder<uint64_t> decoder(buffer.data(), buffer.size(), 8);
    uint64_t val;
    for (int i = 0; i < 15; ++i) {
        EXPECT_TRUE(decoder.Get(&val));
        EXPECT_EQ(1, val);
    }
    EXPECT_FALSE(decoder.Get(&val)); // No more values
}

// Test to cover the branch at L460-463 in Put():
// After FlushBufferedValues, if repeat_count_ >= 8 and run_length > 0,
// add remaining run_length to repeat_count_ directly.
//
// To trigger this branch:
// 1. Start fresh (repeat_count_=0, num_buffered_values_=0)
// 2. Put same value with run_length > 8 (e.g., 20)
// 3. First 8 values fill buffer, trigger FlushBufferedValues
// 4. FlushBufferedValues sees all 8 same values -> repeat_count_ stays at 8
// 5. After flush: repeat_count_=8, run_length=12 -> branch triggered!
TEST_F(TestRle, TestPutFlushThenFastPath) {
    // Test case 1: Single Put with run_length > 8, triggers the L460 branch
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{42, 20}};
        ValidatePutRunLength(runs, 8);
    }

    // Test case 2: run_length exactly causes flush then has remaining
    // Put 16 values: fills buffer twice
    // First 8: flush, repeat_count_=8, run_length=8 -> L460 branch!
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{99, 16}};
        ValidatePutRunLength(runs, 8);
    }

    // Test case 3: run_length = 9 (just over buffer size)
    // First 8: flush, repeat_count_=8, run_length=1 -> L460 branch!
    {
        std::vector<std::pair<uint64_t, size_t>> runs = {{77, 9}};
        ValidatePutRunLength(runs, 8);
    }

    // Test case 4: Verify the fast path after entering repeated run
    // Put(1, 8) -> flush -> repeat_count_=8
    // Put(1, 100) -> should hit fast path at L440
    {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, 8);

        encoder.Put(1, 8);   // Fill buffer and flush, repeat_count_ = 8
        encoder.Put(1, 100); // Should hit fast path (L440)
        encoder.Flush();

        RleDecoder<uint64_t> decoder(buffer.data(), buffer.size(), 8);
        uint64_t val;
        for (int i = 0; i < 108; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(1, val);
        }
        EXPECT_FALSE(decoder.Get(&val));
    }

    // Test case 5: Ensure L460 branch works with literal prefix
    // Put different values first to create literal, then same values
    // This ensures FlushBufferedValues might create literal, not repeat
    {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, 8);

        // Create a literal run first (different values)
        encoder.Put(1, 1);
        encoder.Put(2, 1);
        encoder.Put(3, 1);
        encoder.Put(4, 1);
        encoder.Put(5, 1);
        encoder.Put(6, 1);
        encoder.Put(7, 1);
        encoder.Put(8, 1); // Buffer full, flush as literal

        // Now put same value many times
        encoder.Put(99, 20); // Should eventually hit L460 branch
        encoder.Flush();

        RleDecoder<uint64_t> decoder(buffer.data(), buffer.size(), 8);
        uint64_t val;
        for (int i = 1; i <= 8; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(i, val);
        }
        for (int i = 0; i < 20; ++i) {
            EXPECT_TRUE(decoder.Get(&val));
            EXPECT_EQ(99, val);
        }
        EXPECT_FALSE(decoder.Get(&val));
    }
}

// Benchmark for Put function performance with consecutive equal values
// This test compares:
// 1. Put(value) called N times in a loop
// 2. Put(value, N) called once with run_length=N
TEST_F(TestRle, BenchmarkPutConsecutiveValues) {
    const int kNumIterations = 10000000; // Number of benchmark iterations
    const size_t kNumValues = 1000;      // 1M values per iteration
    const int kBitWidth = 8;

    // Warm up
    {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, kBitWidth);
        for (size_t i = 0; i < 10000; ++i) {
            encoder.Put(42);
        }
        encoder.Flush();
    }

    // Benchmark 2: Put(value, run_length) - tests optimized batch path
    int64_t total_time_batch_ns = 0;
    for (int iter = 0; iter < kNumIterations; ++iter) {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, kBitWidth);

        auto start = std::chrono::high_resolution_clock::now();
        encoder.Put(42, kNumValues);
        encoder.Flush();
        auto end = std::chrono::high_resolution_clock::now();

        total_time_batch_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    double avg_batch_ms = total_time_batch_ns / 1e6;

    std::cout << "=== RLE Put Benchmark (consecutive equal values) ===" << std::endl;
    std::cout << "Values per iteration: " << kNumValues << std::endl;
    std::cout << "Number of iterations: " << kNumIterations << std::endl;
    std::cout << "Put(value, N):        " << avg_batch_ms << " ms avg" << std::endl;
}

// Benchmark for mixed patterns: alternating runs of different lengths
TEST_F(TestRle, BenchmarkPutMixedPattern) {
    const int kNumIterations = 1000000;
    const int kBitWidth = 8;

    // Pattern: alternating values with varying run lengths
    // Simulates real-world data like null bitmaps or repeated values in columns
    std::vector<std::pair<uint64_t, size_t>> pattern = {{0, 100},  {1, 50},  {0, 200},
                                                        {1, 10},   {0, 500}, {1, 1000},
                                                        {0, 5000}, {1, 100}, {0, 10000}};

    // Benchmark batch puts
    int64_t total_time_batch_ns = 0;
    for (int iter = 0; iter < kNumIterations; ++iter) {
        faststring buffer;
        RleEncoder<uint64_t> encoder(&buffer, kBitWidth);

        auto start = std::chrono::high_resolution_clock::now();
        for (const auto& [value, length] : pattern) {
            encoder.Put(value, length);
        }
        encoder.Flush();
        auto end = std::chrono::high_resolution_clock::now();

        total_time_batch_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    double avg_batch_ms = total_time_batch_ns / 1e6;

    std::cout << "=== RLE Put Benchmark (mixed pattern) ===" << std::endl;
    std::cout << "Number of iterations: " << kNumIterations << std::endl;
    std::cout << "Put(value, N):        " << avg_batch_ms << " ms avg" << std::endl;
}

} // namespace doris
