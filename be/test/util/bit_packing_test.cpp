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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>

#include "util/bit_stream_utils.inline.h"
#include "util/faststring.h"

namespace doris {
namespace {

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
static_assert(PdepUnpack::should_use<uint16_t, 15>());
static_assert(PdepUnpack::should_use<uint32_t, 15>());
static_assert(!PdepUnpack::should_use<uint16_t, 16>());
static_assert(!PdepUnpack::should_use<uint32_t, 16>());
static_assert(!PdepUnpack::should_use<uint32_t, 32>());
#endif

template <typename T>
std::vector<T> make_values(int bit_width, int num_values) {
    const uint64_t mask = bit_width == std::numeric_limits<T>::digits
                                  ? std::numeric_limits<T>::max()
                                  : (1ULL << bit_width) - 1;
    std::vector<T> values(num_values);
    for (int i = 0; i < num_values; ++i) {
        values[i] = static_cast<T>((0x9E3779B9ULL * i + 0x7F4A7C15ULL) & mask);
    }
    return values;
}

template <typename T>
void pack_values(const std::vector<T>& values, int bit_width, faststring* packed) {
    BitWriter writer(packed);
    for (T value : values) {
        writer.PutValue(value, bit_width);
    }
    writer.Flush();
}

template <typename T>
void test_all_bit_widths() {
    constexpr int num_values = 65;
    for (int bit_width = 1; bit_width <= std::numeric_limits<T>::digits; ++bit_width) {
        std::vector<T> expected = make_values<T>(bit_width, num_values);
        faststring packed;
        pack_values(expected, bit_width, &packed);
        std::vector<T> actual(num_values);

        auto [end, values_read] =
                BitPacking::UnpackValues(bit_width, reinterpret_cast<const uint8_t*>(packed.data()),
                                         packed.size(), num_values, actual.data());

        EXPECT_EQ(values_read, num_values) << "bit_width=" << bit_width;
        EXPECT_EQ(end, reinterpret_cast<const uint8_t*>(packed.data()) + packed.size())
                << "bit_width=" << bit_width;
        EXPECT_EQ(actual, expected) << "bit_width=" << bit_width;
    }
}

template <typename T>
void test_truncated_input() {
    constexpr int num_values = 65;
    for (int bit_width = 1; bit_width <= std::numeric_limits<T>::digits; ++bit_width) {
        std::vector<T> expected = make_values<T>(bit_width, num_values);
        faststring packed;
        pack_values(expected, bit_width, &packed);
        const int64_t input_bytes = packed.size() - 1;
        const int64_t expected_values_read = input_bytes * 8 / bit_width;
        const int64_t expected_bytes_read = (expected_values_read * bit_width + 7) / 8;
        std::vector<T> actual(num_values, std::numeric_limits<T>::max());

        auto [end, values_read] =
                BitPacking::UnpackValues(bit_width, reinterpret_cast<const uint8_t*>(packed.data()),
                                         input_bytes, num_values, actual.data());

        EXPECT_EQ(values_read, expected_values_read) << "bit_width=" << bit_width;
        EXPECT_EQ(end, reinterpret_cast<const uint8_t*>(packed.data()) + expected_bytes_read)
                << "bit_width=" << bit_width;
        EXPECT_TRUE(std::equal(expected.begin(), expected.begin() + expected_values_read,
                               actual.begin()))
                << "bit_width=" << bit_width;
    }
}

TEST(BitPackingTest, PdepUnpackAllBitWidths) {
    test_all_bit_widths<uint8_t>();
    test_all_bit_widths<uint16_t>();
    test_all_bit_widths<uint32_t>();
}

TEST(BitPackingTest, PdepUnpackTruncatedInput) {
    test_truncated_input<uint8_t>();
    test_truncated_input<uint16_t>();
    test_truncated_input<uint32_t>();
}

} // namespace
} // namespace doris
