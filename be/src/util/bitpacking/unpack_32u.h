// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once
#include "extend_32u.h"
#include "unpack_def.h"

// ------------------------------------ 17u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_17u_0[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                                      4u, 5u, 5u, 6u, 6u, 7u, 7u, 8u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_17u_1[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                                      4u, 5u, 5u, 6u, 6u, 7u, 7u, 8u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_17u_0[8]) = {0u, 2u, 4u, 6u, 8u, 10u, 12u, 14u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_17u_1[8]) = {15u, 13u, 11u, 9u, 7u, 5u, 3u, 1u};

// ------------------------------------ 18u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_18u_0[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                                      4u, 5u, 5u, 6u, 6u, 7u, 7u, 8u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_18u_1[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                                      5u, 6u, 6u, 7u, 7u, 8u, 8u, 9u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_18u_0[8]) = {0u, 4u, 8u, 12u, 16u, 20u, 24u, 28u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_18u_1[8]) = {14u, 10u, 6u, 2u, 30u, 26u, 22u, 18u};

// ------------------------------------ 19u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_19u_0[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                                      4u, 5u, 5u, 6u, 7u, 8u, 8u, 9u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_19u_1[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 4u, 5u,
                                                                      5u, 6u, 6u, 7u, 7u, 8u, 8u, 9u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_19u_0[8]) = {0u, 6u, 12u, 18u, 24u, 30u, 4u, 10u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_19u_1[8]) = {13u, 7u, 1u, 27u, 21u, 15u, 9u, 3u};

// ------------------------------------ 20u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_20u_0[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                                      5u, 6u, 6u, 7u, 7u, 8u, 8u, 9u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_20u_1[16]) = {0u, 1u, 1u, 2u, 3u, 4u, 4u, 5u,
                                                                      5u, 6u, 6u, 7u, 8u, 9u, 9u, 10u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_20u_0[8]) = {0u, 8u, 16u, 24u, 0u, 8u, 16u, 24u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_20u_1[8]) = {12u, 4u, 28u, 20u, 12u, 4u, 28u, 20u};

// ------------------------------------ 21u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_21u_0[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                                      5u, 6u, 6u, 7u, 7u, 8u, 9u, 10u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_21u_1[16]) = {0u, 1u, 1u, 2u, 3u, 4u, 4u, 5u,
                                                                      5u, 6u, 7u, 8u, 8u, 9u, 9u, 10u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_21u_0[8]) = {0u, 10u, 20u, 30u, 8u, 18u, 28u, 6u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_21u_1[8]) = {11u, 1u, 23u, 13u, 3u, 25u, 15u, 5u};

// ------------------------------------ 22u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_22u_0[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 4u, 5u,
                                                                      5u, 6u, 6u, 7u, 8u, 9u, 9u, 10u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_22u_1[16]) = {0u, 1u, 2u, 3u, 3u, 4u, 4u,  5u,
                                                                      6u, 7u, 7u, 8u, 8u, 9u, 10u, 11u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_22u_0[8]) = {0u, 12u, 24u, 4u, 16u, 28u, 8u, 20u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_22u_1[8]) = {10u, 30u, 18u, 6u, 26u, 14u, 2u, 22u};

// ------------------------------------ 23u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_23u_0[16]) = {0u, 1u, 1u, 2u, 2u, 3u, 4u,  5u,
                                                                      5u, 6u, 7u, 8u, 8u, 9u, 10u, 11u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_23u_1[16]) = {0u, 1u, 2u, 3u, 3u, 4u,  5u,  6u,
                                                                      6u, 7u, 7u, 8u, 9u, 10u, 10u, 11u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_23u_0[8]) = {0u, 14u, 28u, 10u, 24u, 6u, 20u, 2u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_23u_1[8]) = {9u, 27u, 13u, 31u, 17u, 3u, 21u, 7u};

// ------------------------------------ 24u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_24u_0[16]) = {0u, 1u, 1u, 2u, 3u, 4u,  4u,  5u,
                                                                      6u, 7u, 7u, 8u, 9u, 10u, 10u, 11u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_24u_1[16]) = {0u, 1u, 2u, 3u, 3u, 4u,  5u,  6u,
                                                                      6u, 7u, 8u, 9u, 9u, 10u, 11u, 12u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_24u_0[8]) = {0u, 16u, 0u, 16u, 0u, 16u, 0u, 16u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_24u_1[8]) = {8u, 24u, 8u, 24u, 8u, 24u, 8u, 24u};

// ------------------------------------ 25u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_25u_0[16]) = {0u, 1u, 1u, 2u, 3u, 4u,  4u,  5u,
                                                                      6u, 7u, 7u, 8u, 9u, 10u, 10u, 11u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_25u_1[16]) = {0u, 1u, 2u, 3u, 3u,  4u,  5u,  6u,
                                                                      7u, 8u, 8u, 9u, 10u, 11u, 11u, 12u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_25u_0[8]) = {0u, 18u, 4u, 22u, 8u, 26u, 12u, 30u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_25u_1[8]) = {7u, 21u, 3u, 17u, 31u, 13u, 27u, 9u};

// ------------------------------------ 26u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_26u_0[16]) = {0u, 1u, 1u, 2u, 3u, 4u,  4u,  5u,
                                                                      6u, 7u, 8u, 9u, 9u, 10u, 11u, 12u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_26u_1[16]) = {0u, 1u, 2u, 3u, 4u,  5u,  5u,  6u,
                                                                      7u, 8u, 8u, 9u, 10u, 11u, 12u, 13u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_26u_0[8]) = {0u, 20u, 8u, 28u, 16u, 4u, 24u, 12u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_26u_1[8]) = {6u, 18u, 30u, 10u, 22u, 2u, 14u, 26u};

// ------------------------------------ 27u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_27u_0[16]) = {0u, 1u, 1u, 2u, 3u,  4u,  5u,  6u,
                                                                      6u, 7u, 8u, 9u, 10u, 11u, 11u, 12u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_27u_1[16]) = {0u, 1u, 2u, 3u,  4u,  5u,  5u,  6u,
                                                                      7u, 8u, 9u, 10u, 10u, 11u, 12u, 13u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_27u_0[8]) = {0u, 22u, 12u, 2u, 24u, 14u, 4u, 26u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_27u_1[8]) = {5u, 15u, 25u, 3u, 13u, 23u, 1u, 11u};

// ------------------------------------ 28u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_28u_0[16]) = {0u, 1u, 1u, 2u, 3u,  4u,  5u,  6u,
                                                                      7u, 8u, 8u, 9u, 10u, 11u, 12u, 13u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_28u_1[16]) = {0u, 1u, 2u, 3u,  4u,  5u,  6u,  7u,
                                                                      7u, 8u, 9u, 10u, 11u, 12u, 13u, 14u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_28u_0[8]) = {0u, 24u, 16u, 8u, 0u, 24u, 16u, 8u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_28u_1[8]) = {4u, 12u, 20u, 28u, 4u, 12u, 20u, 28u};

// ------------------------------------ 29u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_29u_0[16]) = {0u, 1u, 1u, 2u,  3u,  4u,  5u,  6u,
                                                                      7u, 8u, 9u, 10u, 10u, 11u, 12u, 13u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_29u_1[16]) = {0u, 1u, 2u, 3u,  4u,  5u,  6u,  7u,
                                                                      8u, 9u, 9u, 10u, 11u, 12u, 13u, 14u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_29u_0[8]) = {0u, 26u, 20u, 14u, 8u, 2u, 28u, 22u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_29u_1[8]) = {3u, 9u, 15u, 21u, 27u, 1u, 7u, 13u};

// ------------------------------------ 30u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_30u_0[16]) = {0u, 1u, 1u, 2u,  3u,  4u,  5u,  6u,
                                                                      7u, 8u, 9u, 10u, 11u, 12u, 13u, 14u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_30u_1[16]) = {0u, 1u, 2u,  3u,  4u,  5u,  6u,  7u,
                                                                      8u, 9u, 10u, 11u, 12u, 13u, 14u, 15u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_30u_0[8]) = {0u, 28u, 24u, 20u, 16u, 12u, 8u, 4u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_30u_1[8]) = {2u, 6u, 10u, 14u, 18u, 22u, 26u, 30u};

// ------------------------------------ 31u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_31u_0[16]) = {0u, 1u, 1u, 2u,  3u,  4u,  5u,  6u,
                                                                      7u, 8u, 9u, 10u, 11u, 12u, 13u, 14u};
OWN_ALIGNED_64_ARRAY(static uint32_t permutex_idx_table_31u_1[16]) = {0u, 1u, 2u,  3u,  4u,  5u,  6u,  7u,
                                                                      8u, 9u, 10u, 11u, 12u, 13u, 14u, 15u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_31u_0[8]) = {0u, 30u, 28u, 26u, 24u, 22u, 20u, 18u};
OWN_ALIGNED_64_ARRAY(static uint64_t shift_table_31u_1[8]) = {1u, 3u, 5u, 7u, 9u, 11u, 13u, 15u};

template <typename OutType>
const uint8_t* unpack_Nu32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t bit_width, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_Nu32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t bit_width,
                                   uint32_t* dst_ptr) {
    uint64_t mask = OWN_BIT_MASK(bit_width);
    uint64_t next_dword;
    uint32_t* src32u_ptr = (uint32_t*)src_ptr;
    uint8_t* src8u_ptr = (uint8_t*)src_ptr;
    uint32_t* dst32u_ptr = (uint32_t*)dst_ptr;
    uint32_t bits_in_buf = 0u;
    uint64_t src = 0u;

    if (2u < values_to_read) {
        bits_in_buf = OWN_DWORD_WIDTH;
        src = (uint64_t)(*src32u_ptr);

        src32u_ptr++;

        while (2u < values_to_read) {
            if (bit_width > bits_in_buf) {
                next_dword = (uint64_t)(*src32u_ptr);
                src32u_ptr++;
                next_dword = next_dword << bits_in_buf;
                src = src | next_dword;
                bits_in_buf += OWN_DWORD_WIDTH;
            }
            *dst32u_ptr = (uint32_t)(src & mask);
            src = src >> bit_width;
            bits_in_buf -= bit_width;
            dst32u_ptr++;
            values_to_read--;
        }

        src8u_ptr = (uint8_t*)src32u_ptr;
    }

    while (0u < values_to_read) {
        while (bit_width > bits_in_buf) {
            next_dword = (uint64_t)(*src8u_ptr);
            src8u_ptr++;
            next_dword = next_dword << bits_in_buf;
            src = src | next_dword;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst32u_ptr = (uint32_t)(src & mask);
        src = src >> bit_width;
        bits_in_buf -= bit_width;
        dst32u_ptr++;
        values_to_read--;
    }
    return src8u_ptr;
}

template <>
inline const uint8_t* unpack_Nu32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t bit_width,
                                   uint64_t* dst_ptr) {
    uint64_t mask = OWN_BIT_MASK(bit_width);
    uint64_t next_dword;
    uint32_t* src32u_ptr = (uint32_t*)src_ptr;
    uint8_t* src8u_ptr = (uint8_t*)src_ptr;
    uint64_t* dst64u_ptr = (uint64_t*)dst_ptr;
    uint32_t bits_in_buf = 0u;
    uint64_t src = 0u;

    if (2u < values_to_read) {
        bits_in_buf = OWN_DWORD_WIDTH;
        src = (uint64_t)(*src32u_ptr);

        src32u_ptr++;

        while (2u < values_to_read) {
            if (bit_width > bits_in_buf) {
                next_dword = (uint64_t)(*src32u_ptr);
                src32u_ptr++;
                next_dword = next_dword << bits_in_buf;
                src = src | next_dword;
                bits_in_buf += OWN_DWORD_WIDTH;
            }
            *dst64u_ptr = (uint64_t)(src & mask);
            src = src >> bit_width;
            bits_in_buf -= bit_width;
            dst64u_ptr++;
            values_to_read--;
        }

        src8u_ptr = (uint8_t*)src32u_ptr;
    }

    while (0u < values_to_read) {
        while (bit_width > bits_in_buf) {
            next_dword = (uint64_t)(*src8u_ptr);
            src8u_ptr++;
            next_dword = next_dword << bits_in_buf;
            src = src | next_dword;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst64u_ptr = (uint64_t)(src & mask);
        src = src >> bit_width;
        bits_in_buf -= bit_width;
        dst64u_ptr++;
        values_to_read--;
    }
    return src8u_ptr;
}

// ********************** 17u ****************************** //
template <typename OutType>
const uint8_t* unpack_17u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_17u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_17u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(17u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(17u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_17u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_17u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_17u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_17u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 17u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 17u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_17u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(17u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(17u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_17u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_17u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_17u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_17u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 17u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 17u, dst_ptr);
    }
    return src_ptr;
}
// ********************** 18u ****************************** //

template <typename OutType>
const uint8_t* unpack_18u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_18u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_18u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(18u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(18u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_18u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_18u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_18u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_18u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 18u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 18u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_18u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(18u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(18u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_18u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_18u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_18u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_18u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 18u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 18u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 19u ****************************** //
template <typename OutType>
const uint8_t* unpack_19u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_19u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_19u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(19u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(19u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_19u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_19u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_19u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_19u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 19u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 19u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_19u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(19u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(19u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_19u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_19u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_19u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_19u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 19u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 19u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 20u ****************************** //
template <typename OutType>
const uint8_t* unpack_20u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_20u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_20u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(20u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(20u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_20u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_20u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_20u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_20u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 20u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 20u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_20u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(20u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(20u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_20u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_20u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_20u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_20u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 20u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 20u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 21u ****************************** //
template <typename OutType>
const uint8_t* unpack_21u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_21u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_21u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(21u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(21u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_21u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_21u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_21u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_21u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 21u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 21u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_21u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(21u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(21u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_21u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_21u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_21u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_21u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 21u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 21u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 22u ****************************** //
template <typename OutType>
const uint8_t* unpack_22u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_22u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_22u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(22u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(22u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_22u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_22u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_22u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_22u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 22u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 22u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_22u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(22u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(22u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_22u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_22u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_22u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_22u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 22u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 22u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 23u ****************************** //
template <typename OutType>
const uint8_t* unpack_23u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_23u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_23u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(23u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(23u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_23u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_23u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_23u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_23u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 23u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 23u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_23u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(23u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(23u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_23u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_23u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_23u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_23u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 23u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 23u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 24u ****************************** //
template <typename OutType>
const uint8_t* unpack_24u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_24u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_24u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(24u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(24u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_24u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_24u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_24u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_24u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 24u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 24u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_24u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(24u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(24u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_24u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_24u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_24u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_24u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 24u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 24u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 25u ****************************** //
template <typename OutType>
const uint8_t* unpack_25u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_25u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_25u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(25u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(25u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_25u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_25u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_25u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_25u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 25u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 25u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_25u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(25u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(25u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_25u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_25u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_25u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_25u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 25u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 25u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 26u ****************************** //
template <typename OutType>
const uint8_t* unpack_26u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_26u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_26u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(26u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(26u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_26u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_26u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_26u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_26u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 26u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 26u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_26u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(26u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(26u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_26u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_26u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_26u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_26u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 26u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 26u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 27u ****************************** //
template <typename OutType>
const uint8_t* unpack_27u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_27u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_27u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(27u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(27u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_27u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_27u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_27u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_27u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 27u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 27u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_27u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(27u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(27u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_27u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_27u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_27u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_27u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 27u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 27u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 28u ****************************** //
template <typename OutType>
const uint8_t* unpack_28u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_28u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_28u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(28u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(28u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_28u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_28u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_28u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_28u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 28u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 28u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_28u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(28u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(28u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_28u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_28u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_28u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_28u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 28u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 28u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 29u ****************************** //
template <typename OutType>
const uint8_t* unpack_29u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_29u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_29u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(29u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(29u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_29u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_29u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_29u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_29u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 29u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 29u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_29u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(29u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(29u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_29u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_29u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_29u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_29u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 29u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 29u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 30u ****************************** //
template <typename OutType>
const uint8_t* unpack_30u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_30u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_30u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(30u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(30u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_30u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_30u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_30u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_30u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 30u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 30u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_30u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask16 read_mask = OWN_BIT_MASK(OWN_BITS_2_DWORD(30u * OWN_WORD_WIDTH));
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(30u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_30u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_30u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_30u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_30u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi32(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 30u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 30u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 31u ****************************** //
template <typename OutType>
const uint8_t* unpack_31u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_31u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_31u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(31u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(31u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_31u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_31u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_31u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_31u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 2u * 31u;
            dst_ptr += 16u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 31u, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_31u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 16u) {
        __mmask32 read_mask = OWN_BIT_MASK(31u);
        __m512i parse_mask0 = _mm512_set1_epi32(OWN_BIT_MASK(31u));

        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(permutex_idx_table_31u_0);
        permutex_idx_ptr[1] = _mm512_load_si512(permutex_idx_table_31u_1);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_31u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_31u_1);

        while (values_to_read >= 16u) {
            __m512i srcmm, zmm[2];

            srcmm = _mm512_maskz_loadu_epi16(read_mask, src_ptr);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutexvar_epi32(permutex_idx_ptr[0], srcmm);
            zmm[1] = _mm512_permutexvar_epi32(permutex_idx_ptr[1], srcmm);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi64(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi64(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask0);

            extend_32u64u(zmm[0], dst_ptr);

            src_ptr += 2u * 31u;
            values_to_read -= 16u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = unpack_Nu32u(src_ptr, values_to_read, 31u, dst_ptr);
    }
    return src_ptr;
}

// ********************** 32u ****************************** //
template <typename OutType>
const uint8_t* unpack_32u32u(const uint8_t* src_ptr, uint32_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_32u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint8_t* dst_ptr) {
    return src_ptr;
}

template <>
inline const uint8_t* unpack_32u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint32_t* dst_ptr) {
    memcpy(dst_ptr, src_ptr, values_to_read * 4);
    return src_ptr;
}

template <>
inline const uint8_t* unpack_32u32u(const uint8_t* src_ptr, uint32_t values_to_read, uint64_t* dst_ptr) {
    uint32_t* src32u_ptr = (uint32_t*)src_ptr;
    if (values_to_read >= 16u) {
        while (values_to_read >= 16u) {
            __m512i src = _mm512_loadu_si512(src32u_ptr);
            extend_32u64u(src, dst_ptr);
            values_to_read -= 16u;
            src32u_ptr += 16u;
        }
    }
    while (values_to_read > 0) {
        *dst_ptr = (uint64_t)(*src32u_ptr);
        dst_ptr++;
        src32u_ptr++;
        values_to_read--;
    }
    return src_ptr + values_to_read * 4;
}
