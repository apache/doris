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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>

#include "util/simd/parquet_kernels.h"

namespace doris::simd {
namespace {

TEST(ParquetSimdKernelsTest, ByteStreamSplitRestoresFourAndEightByteValues) {
    for (const size_t width : {4, 8}) {
        constexpr size_t rows = 67;
        std::vector<uint8_t> plain(rows * width);
        for (size_t byte = 0; byte < plain.size(); ++byte) {
            plain[byte] = static_cast<uint8_t>((byte * 29 + 17) & 0xff);
        }
        std::vector<uint8_t> encoded(plain.size());
        for (size_t row = 0; row < rows; ++row) {
            for (size_t byte = 0; byte < width; ++byte) {
                encoded[byte * rows + row] = plain[row * width + byte];
            }
        }

        for (const auto [offset, count] :
             {std::pair<size_t, size_t> {0, rows}, {3, 31}, {17, 33}}) {
            std::vector<uint8_t> decoded(count * width);
            byte_stream_split_decode(encoded.data(), width, offset, count, rows, decoded.data());
            EXPECT_EQ(0, memcmp(decoded.data(), plain.data() + offset * width, decoded.size()));
        }
    }
}

template <typename T>
void test_delta_prefix_sum() {
    std::vector<T> deltas(71);
    for (size_t row = 0; row < deltas.size(); ++row) {
        deltas[row] = static_cast<T>((row * 7) % 19);
    }
    auto expected = deltas;
    T expected_last = std::numeric_limits<T>::max() - 37;
    constexpr T min_delta = static_cast<T>(-11);
    using Unsigned = std::make_unsigned_t<T>;
    for (auto& value : expected) {
        value = static_cast<T>(static_cast<Unsigned>(value) + static_cast<Unsigned>(min_delta) +
                               static_cast<Unsigned>(expected_last));
        expected_last = value;
    }

    T last = std::numeric_limits<T>::max() - 37;
    delta_decode(deltas.data(), deltas.size(), min_delta, &last);
    EXPECT_EQ(deltas, expected);
    EXPECT_EQ(last, expected_last);
}

TEST(ParquetSimdKernelsTest, DeltaPrefixSumPreservesParquetUnsignedOverflow) {
    test_delta_prefix_sum<int32_t>();
    test_delta_prefix_sum<int64_t>();
}

TEST(ParquetSimdKernelsTest, DictionaryGatherHandlesTailsAndRepeatedIds) {
    for (const size_t width : {4, 8}) {
        constexpr size_t entries = 257;
        std::vector<uint8_t> dictionary(entries * width);
        for (size_t byte = 0; byte < dictionary.size(); ++byte) {
            dictionary[byte] = static_cast<uint8_t>((byte * 13 + 5) & 0xff);
        }
        std::vector<uint32_t> ids(69);
        for (size_t row = 0; row < ids.size(); ++row) {
            ids[row] = row % 9 == 0 ? 3 : static_cast<uint32_t>((row * 31) % entries);
        }
        std::vector<uint8_t> actual(ids.size() * width);
        dictionary_gather(dictionary.data(), ids.data(), ids.size(), width, actual.data());
        for (size_t row = 0; row < ids.size(); ++row) {
            EXPECT_EQ(0, memcmp(actual.data() + row * width, dictionary.data() + ids[row] * width,
                                width));
        }
    }
}

TEST(ParquetSimdKernelsTest, NullableExpansionIsSafeForOverlappingStorage) {
    for (const size_t width : {4, 8}) {
        const std::vector<std::vector<uint8_t>> null_masks {
                {1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 0},
                std::vector<uint8_t>(17, 0),
                std::vector<uint8_t>(17, 1),
        };
        for (const auto& nulls : null_masks) {
            const size_t compact_count =
                    static_cast<size_t>(std::count(nulls.begin(), nulls.end(), uint8_t {0}));
            std::vector<uint8_t> values(nulls.size() * width, 0xcd);
            for (size_t row = 0; row < compact_count; ++row) {
                for (size_t byte = 0; byte < width; ++byte) {
                    values[row * width + byte] =
                            static_cast<uint8_t>((row * width + byte + 1) & 0xff);
                }
            }
            const auto compact = values;

            expand_nullable_values(values.data(), compact_count, nulls.data(), nulls.size(), width);
            size_t source = 0;
            for (size_t row = 0; row < nulls.size(); ++row) {
                if (nulls[row] != 0) {
                    for (size_t byte = 0; byte < width; ++byte) {
                        EXPECT_EQ(values[row * width + byte], 0);
                    }
                } else {
                    for (size_t byte = 0; byte < width; ++byte) {
                        EXPECT_EQ(values[row * width + byte], compact[source * width + byte])
                                << "row=" << row << ", byte=" << byte << ", width=" << width;
                    }
                    ++source;
                }
            }
        }
    }
}

template <typename T>
void expect_raw_comparison(const std::vector<T>& values, T literal, RawComparisonOp op,
                           const std::vector<uint8_t>& expected) {
    std::vector<uint8_t> matches(values.size(), 1);
    if (matches.size() > 2) {
        matches[1] = 0;
    }
    auto masked_expected = expected;
    if (masked_expected.size() > 2) {
        masked_expected[1] = 0;
    }
    raw_compare(reinterpret_cast<const uint8_t*>(values.data()), values.size(), literal, op,
                matches.data());
    EXPECT_EQ(matches, masked_expected);
}

TEST(ParquetSimdKernelsTest, RawPredicatesPreserveExistingMaskAndDorisNanOrdering) {
    const float nan = std::numeric_limits<float>::quiet_NaN();
    const std::vector<float> floats {-3.0F, -2.0F, -1.0F, 0.0F, 1.0F, 2.0F,
                                     3.0F,  nan,   nan,   4.0F, 5.0F};
    expect_raw_comparison(floats, 0.0F, RawComparisonOp::GT, {0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1});
    expect_raw_comparison(floats, nan, RawComparisonOp::EQ, {0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0});
    expect_raw_comparison(floats, nan, RawComparisonOp::LE, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1});

    const std::vector<int32_t> ints {-3, -1, 0, 1, 3, 7, 9, 12, 15, 21, 27};
    expect_raw_comparison(ints, 3, RawComparisonOp::GE, {0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1});
    expect_raw_comparison(ints, 3, RawComparisonOp::NE, {1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1});

    const double double_nan = std::numeric_limits<double>::quiet_NaN();
    const std::vector<double> doubles {
            -std::numeric_limits<double>::infinity(), -2.0,       -1.0,      0.0, 1.0,
            std::numeric_limits<double>::infinity(),  double_nan, double_nan};
    expect_raw_comparison(doubles, 0.0, RawComparisonOp::EQ, {0, 0, 0, 1, 0, 0, 0, 0});
    expect_raw_comparison(doubles, 0.0, RawComparisonOp::NE, {1, 1, 1, 0, 1, 1, 1, 1});
    expect_raw_comparison(doubles, 0.0, RawComparisonOp::LT, {1, 1, 1, 0, 0, 0, 0, 0});
    expect_raw_comparison(doubles, 0.0, RawComparisonOp::LE, {1, 1, 1, 1, 0, 0, 0, 0});
    expect_raw_comparison(doubles, 0.0, RawComparisonOp::GT, {0, 0, 0, 0, 1, 1, 1, 1});
    expect_raw_comparison(doubles, 0.0, RawComparisonOp::GE, {0, 0, 0, 1, 1, 1, 1, 1});
    expect_raw_comparison(doubles, double_nan, RawComparisonOp::EQ, {0, 0, 0, 0, 0, 0, 1, 1});
    expect_raw_comparison(doubles, double_nan, RawComparisonOp::LT, {1, 1, 1, 1, 1, 1, 0, 0});
    expect_raw_comparison(doubles, double_nan, RawComparisonOp::LE, {1, 1, 1, 1, 1, 1, 1, 1});

    const std::vector<int64_t> bigints {-9, -3, -1, 0, 1, 3, 7, 11, 19};
    expect_raw_comparison(bigints, int64_t {3}, RawComparisonOp::LT, {1, 1, 1, 1, 1, 0, 0, 0, 0});
    expect_raw_comparison(bigints, int64_t {3}, RawComparisonOp::GE, {0, 0, 0, 0, 0, 1, 1, 1, 1});
}

} // namespace
} // namespace doris::simd
