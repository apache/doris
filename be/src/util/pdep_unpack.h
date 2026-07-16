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

#pragma once

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))

#include <immintrin.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>

namespace doris {

class PdepUnpack {
public:
    static bool is_supported() {
        return __builtin_cpu_supports("bmi2") && __builtin_cpu_supports("avx2");
    }

    template <typename T, int BIT_WIDTH>
    static constexpr bool is_supported_type() {
        return BIT_WIDTH > 0 && BIT_WIDTH <= std::numeric_limits<T>::digits &&
               (std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                std::is_same_v<T, uint32_t>);
    }

    template <typename T, int BIT_WIDTH>
    static constexpr bool should_use() {
        // Keep the generic implementation available for benchmarking all supported widths, but
        // only select PDEP in the production path for widths below 16. These widths can use the
        // byte/word deposit layouts and the AVX2 widening specializations below. At 16 bits and
        // above, unpack32() falls back to multiple generic 64-bit PDEP groups per batch and
        // competes with efficient scalar specializations, including copy-like 16- and 32-bit
        // cases. Benchmarks with L1-, L2-, and larger working sets show non-monotonic results and
        // repeatable regressions for multiple high widths. Because the profitable high widths are
        // CPU- and working-set-dependent, an irregular per-width allowlist would not be portable;
        // use the scalar implementation conservatively instead.
        return is_supported_type<T, BIT_WIDTH>() && BIT_WIDTH < 16;
    }

    template <typename T, int BIT_WIDTH>
    __attribute__((target("bmi2,avx2"))) static void unpack32(const uint8_t* input, T* output) {
        static_assert(is_supported_type<T, BIT_WIDTH>());
        if constexpr (std::is_same_v<T, uint8_t>) {
            unpack32_with_pdep<T, BIT_WIDTH>(input, output);
        } else if constexpr (std::is_same_v<T, uint16_t> && BIT_WIDTH <= 8) {
            for (int group = 0; group < 2; ++group) {
                const int first_value = group * 16;
                const uint8_t* group_input = input + group * 2 * BIT_WIDTH;
                uint64_t expanded0 = _pdep_u64(load_packed_group<0, 8 * BIT_WIDTH>(group_input),
                                               pdep_mask<uint8_t, BIT_WIDTH>());
                uint64_t expanded1 =
                        _pdep_u64(load_packed_group<8 * BIT_WIDTH, 8 * BIT_WIDTH>(group_input),
                                  pdep_mask<uint8_t, BIT_WIDTH>());
                __m128i packed8 = _mm_set_epi64x(static_cast<int64_t>(expanded1),
                                                 static_cast<int64_t>(expanded0));
                __m256i unpacked16 = _mm256_cvtepu8_epi16(packed8);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + first_value), unpacked16);
            }
        } else if constexpr (std::is_same_v<T, uint32_t> && BIT_WIDTH <= 8) {
            for (int group = 0; group < 4; ++group) {
                uint64_t expanded =
                        _pdep_u64(load_packed_group<0, 8 * BIT_WIDTH>(input + group * BIT_WIDTH),
                                  pdep_mask<uint8_t, BIT_WIDTH>());
                __m256i unpacked32 = _mm256_cvtepu8_epi32(_mm_cvtsi64_si128(expanded));
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + group * 8), unpacked32);
            }
        } else if constexpr (std::is_same_v<T, uint32_t> && BIT_WIDTH <= 15) {
            for (int group = 0; group < 4; ++group) {
                const int first_value = group * 8;
                const uint8_t* group_input = input + group * BIT_WIDTH;
                uint64_t expanded0 = _pdep_u64(load_packed_group<0, 4 * BIT_WIDTH>(group_input),
                                               pdep_mask<uint16_t, BIT_WIDTH>());
                uint64_t expanded1 =
                        _pdep_u64(load_packed_group<4 * BIT_WIDTH, 4 * BIT_WIDTH>(group_input),
                                  pdep_mask<uint16_t, BIT_WIDTH>());
                __m128i packed16 = _mm_set_epi64x(static_cast<int64_t>(expanded1),
                                                  static_cast<int64_t>(expanded0));
                __m256i unpacked32 = _mm256_cvtepu16_epi32(packed16);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + first_value), unpacked32);
            }
        } else {
            unpack32_with_pdep<T, BIT_WIDTH>(input, output);
        }
    }

private:
    template <typename T, int BIT_WIDTH>
    static constexpr uint64_t pdep_mask() {
        constexpr int lane_bits = sizeof(T) * 8;
        constexpr int lanes = 64 / lane_bits;
        constexpr uint64_t value_mask = (1ULL << BIT_WIDTH) - 1;
        uint64_t mask = 0;
        for (int lane = 0; lane < lanes; ++lane) {
            mask |= value_mask << (lane * lane_bits);
        }
        return mask;
    }

    template <int BIT_OFFSET, int PACKED_BITS>
    static uint64_t load_packed_group(const uint8_t* input) {
        constexpr int byte_offset = BIT_OFFSET / 8;
        constexpr int shift = BIT_OFFSET % 8;
        constexpr int bytes_needed = (shift + PACKED_BITS + 7) / 8;
        static_assert(PACKED_BITS <= 64);
        static_assert(bytes_needed <= 9);

        uint64_t low = 0;
        std::memcpy(&low, input + byte_offset, bytes_needed < 8 ? bytes_needed : 8);
        if constexpr (bytes_needed <= 8) {
            return low >> shift;
        } else {
            uint8_t high = input[byte_offset + 8];
            return static_cast<uint64_t>((static_cast<unsigned __int128>(high) << 64 | low) >>
                                         shift);
        }
    }

    template <typename T, int BIT_WIDTH, std::size_t GROUP>
    __attribute__((target("bmi2"))) static void unpack_group(const uint8_t* input, T* output) {
        constexpr int lanes = 64 / (sizeof(T) * 8);
        constexpr int first_value = GROUP * lanes;
        constexpr int bit_offset = first_value * BIT_WIDTH;
        constexpr int packed_bits = lanes * BIT_WIDTH;
        uint64_t packed = load_packed_group<bit_offset, packed_bits>(input);
        uint64_t expanded = _pdep_u64(packed, pdep_mask<T, BIT_WIDTH>());
        std::memcpy(output + first_value, &expanded, sizeof(expanded));
    }

    template <typename T, int BIT_WIDTH, std::size_t... GROUPS>
    __attribute__((target("bmi2"))) static void unpack32_with_pdep_impl(
            const uint8_t* input, T* output, std::index_sequence<GROUPS...>) {
        (unpack_group<T, BIT_WIDTH, GROUPS>(input, output), ...);
    }

    template <typename T, int BIT_WIDTH>
    __attribute__((target("bmi2"))) static void unpack32_with_pdep(const uint8_t* input,
                                                                   T* output) {
        constexpr int lanes = 64 / (sizeof(T) * 8);
        unpack32_with_pdep_impl<T, BIT_WIDTH>(input, output,
                                              std::make_index_sequence<32 / lanes> {});
    }
};

} // namespace doris

#endif
