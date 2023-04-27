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
#include "extend_8u.h"
#include "unpack_def.h"

OWN_ALIGNED_64_ARRAY(const uint32_t p_permutex_masks_3u[32]) = {
        0x0,      0x10020,  0x210021, 0x220002, 0x30003,  0x40023,  0x240024, 0x250005, 0x60006,  0x70026,  0x270027,
        0x280008, 0x90009,  0xA0029,  0x2A002A, 0x2B000B, 0x200000, 0x10020,  0x20021,  0x220002, 0x230003, 0x40023,
        0x50024,  0x250005, 0x260006, 0x70026,  0x80027,  0x280008, 0x290009, 0xA0029,  0xB002A,  0x2B000B};

OWN_ALIGNED_64_ARRAY(const uint32_t p_permutex_masks_5u[32]) = {
        0x200000, 0x210001, 0x30022,  0x40023,  0x250005, 0x260006, 0x80027,  0x90028,  0x2A000A, 0x2B000B, 0xD002C,
        0xE002D,  0x2F000F, 0x300010, 0x120031, 0x130032, 0x200000, 0x20021,  0x30022,  0x240004, 0x250005, 0x70026,
        0x80027,  0x290009, 0x2A000A, 0xC002B,  0xD002C,  0x2E000E, 0x2F000F, 0x110030, 0x120031, 0x330013};

OWN_ALIGNED_64_ARRAY(const uint32_t p_permutex_masks_6u[32]) = {
        0x200000, 0x20021,  0x230003, 0x50024,  0x260006, 0x80027,  0x290009, 0xB002A,  0x2C000C, 0xE002D, 0x2F000F,
        0x110030, 0x320012, 0x140033, 0x350015, 0x170036, 0x10000,  0x220021, 0x40003,  0x250024, 0x70006, 0x280027,
        0xA0009,  0x2B002A, 0xD000C,  0x2E002D, 0x10000F, 0x310030, 0x130012, 0x340033, 0x160015, 0x370036};
OWN_ALIGNED_64_ARRAY(const uint32_t p_permutex_masks_7u[32]) = {
        0x200000, 0x220021, 0x40023,  0x60005,  0x270007, 0x290028, 0xB002A,  0xD000C,  0x2E000E, 0x30002F, 0x120031,
        0x140013, 0x350015, 0x370036, 0x190038, 0x1B001A, 0x10000,  0x30002,  0x240023, 0x260025, 0x80007,  0xA0009,
        0x2B002A, 0x2D002C, 0xF000E,  0x110010, 0x320031, 0x340033, 0x160015, 0x180017, 0x390038, 0x3B003A};

// ------------------------------------ 3u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_3u_0[32]) = {0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u,
                                                              2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u,
                                                              4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u};
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_3u_1[32]) = {5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u,
                                                              3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u,
                                                              1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u};

// ------------------------------------ 5u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_5u_0[32]) = {0u, 2u, 4u, 6u, 0u, 2u, 4u, 6u, 0u, 2u, 4u,
                                                              6u, 0u, 2u, 4u, 6u, 0u, 2u, 4u, 6u, 0u, 2u,
                                                              4u, 6u, 0u, 2u, 4u, 6u, 0u, 2u, 4u, 6u};
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_5u_1[32]) = {3u, 1u, 7u, 5u, 3u, 1u, 7u, 5u, 3u, 1u, 7u,
                                                              5u, 3u, 1u, 7u, 5u, 3u, 1u, 7u, 5u, 3u, 1u,
                                                              7u, 5u, 3u, 1u, 7u, 5u, 3u, 1u, 7u, 5u};

// ------------------------------------ 6u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_6u_0[32]) = {0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u,
                                                              4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u,
                                                              0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u};
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_6u_1[32]) = {2u, 6u, 2u, 6u, 2u, 6u, 2u, 6u, 2u, 6u, 2u,
                                                              6u, 2u, 6u, 2u, 6u, 2u, 6u, 2u, 6u, 2u, 6u,
                                                              2u, 6u, 2u, 6u, 2u, 6u, 2u, 6u, 2u, 6u};

// ------------------------------------ 7u -----------------------------------------
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_7u_0[32]) = {0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u,
                                                              2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u,
                                                              4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u};
OWN_ALIGNED_64_ARRAY(static uint16_t shift_table_7u_1[32]) = {1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u,
                                                              7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u,
                                                              5u, 7u, 1u, 3u, 5u, 7u, 1u, 3u, 5u, 7u};

// ********************** 0u ****************************** //
template <typename OutType>
const uint8_t* unpack_0u8u(const uint8_t* src_ptr, int64_t values_to_read, OutType* dst_ptr);
template <>
inline const uint8_t* unpack_0u8u(const uint8_t* src_ptr, int64_t values_to_read, uint8_t* dst_ptr) {
    memset(dst_ptr, 0, values_to_read);
    return src_ptr;
}

template <>
inline const uint8_t* unpack_0u8u(const uint8_t* src_ptr, int64_t values_to_read, uint16_t* dst_ptr) {
    while (values_to_read >= 32u) {
        __m512i tmp = _mm512_set1_epi8(0x00);
        _mm512_storeu_si512(dst_ptr, tmp);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_0u8u(const uint8_t* src_ptr, int64_t values_to_read, uint32_t* dst_ptr) {
    while (values_to_read >= 16u) {
        __m512i tmp = _mm512_set1_epi8(0x00);
        _mm512_storeu_si512(dst_ptr, tmp);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_0u8u(const uint8_t* src_ptr, int64_t values_to_read, uint64_t* dst_ptr) {
    while (values_to_read >= 8u) {
        __m512i tmp = _mm512_set1_epi8(0x00);
        _mm512_storeu_si512(dst_ptr, tmp);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    return src_ptr;
}

// ********************** 1u ****************************** //
template <typename OutType>
const uint8_t* unpack_1u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);
template <>
inline const uint8_t* unpack_1u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    uint64_t bit_mask = 0x0101010101010101LLU;
    uint32_t i;
    uint8_t mask;

    while (values_to_read >= 64u) {
        uint64_t src_64 = *(uint64_t*)src_ptr;
        // convert mask to 512-bit register. 0 --> 0x00, 1 --> 0xFF
        __m512i unpacked_src = _mm512_movm_epi8(src_64);
        // make 0x00 --> 0x00, 0xFF --> 0x01
        unpacked_src = _mm512_abs_epi8(unpacked_src);
        _mm512_storeu_si512(dst_ptr, unpacked_src);

        src_ptr += 8u * 1u;
        dst_ptr += 64u;
        values_to_read -= 64u;
    }
    if (values_to_read > 32u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = bit_byte_pool.byte_buf[1];
            dst_ptr[i + 16u] = bit_byte_pool.byte_buf[2];
            dst_ptr[i + 24u] = bit_byte_pool.byte_buf[3];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = bit_byte_pool.byte_buf[1];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }

    if (values_to_read) {
        mask = OWN_1_BIT_MASK;
        uint8_t src = *src_ptr;
        for (i = 0u; i < values_to_read; i++) {
            if (8u == i) {
                src = *(++src_ptr);
            }
            dst_ptr[i] = (src & mask);
            src = src >> 1u;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_1u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    uint64_t bit_mask = 0x0101010101010101LLU;
    uint32_t i;
    uint8_t mask;

    while (values_to_read >= 64u) {
        uint64_t src_64 = *(uint64_t*)src_ptr;
        // convert mask to 512-bit register. 0 --> 0x00, 1 --> 0xFF
        __m512i unpacked_src = _mm512_movm_epi8(src_64);
        // make 0x00 --> 0x00, 0xFF --> 0x01
        unpacked_src = _mm512_abs_epi8(unpacked_src);

        extend_8u16u(unpacked_src, dst_ptr);

        src_ptr += 8u * 1u;
        values_to_read -= 64u;
    }
    if (values_to_read > 32u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint16_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = (uint16_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 16u] = (uint16_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 24u] = (uint16_t)bit_byte_pool.byte_buf[3];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint16_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = (uint16_t)bit_byte_pool.byte_buf[1];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }

    if (values_to_read) {
        mask = OWN_1_BIT_MASK;
        uint8_t src = *src_ptr;
        for (i = 0u; i < values_to_read; i++) {
            if (8u == i) {
                src = *(++src_ptr);
            }
            dst_ptr[i] = (uint16_t)(src & mask);
            src = src >> 1u;
        }
    }
    return src_ptr;
}
template <>
inline const uint8_t* unpack_1u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    uint64_t bit_mask = 0x0101010101010101LLU;
    uint32_t i;
    uint8_t mask;

    while (values_to_read >= 64u) {
        uint64_t src_64 = *(uint64_t*)src_ptr;
        // convert mask to 512-bit register. 0 --> 0x00, 1 --> 0xFF
        __m512i unpacked_src = _mm512_movm_epi8(src_64);
        // make 0x00 --> 0x00, 0xFF --> 0x01
        unpacked_src = _mm512_abs_epi8(unpacked_src);

        extend_8u32u(unpacked_src, dst_ptr);

        src_ptr += 8u * 1u;
        values_to_read -= 64u;
    }
    if (values_to_read > 32u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint32_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = (uint32_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 16u] = (uint32_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 24u] = (uint32_t)bit_byte_pool.byte_buf[3];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint32_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = (uint32_t)bit_byte_pool.byte_buf[1];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }

    if (values_to_read) {
        mask = OWN_1_BIT_MASK;
        uint8_t src = *src_ptr;
        for (i = 0u; i < values_to_read; i++) {
            if (8u == i) {
                src = *(++src_ptr);
            }
            dst_ptr[i] = (uint32_t)(src & mask);
            src = src >> 1u;
        }
    }
    return src_ptr;
}
template <>
inline const uint8_t* unpack_1u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    uint64_t bit_mask = 0x0101010101010101LLU;
    uint32_t i;
    uint8_t mask;

    while (values_to_read >= 64u) {
        uint64_t src_64 = *(uint64_t*)src_ptr;
        // convert mask to 512-bit register. 0 --> 0x00, 1 --> 0xFF
        __m512i unpacked_src = _mm512_movm_epi8(src_64);
        // make 0x00 --> 0x00, 0xFF --> 0x01
        unpacked_src = _mm512_abs_epi8(unpacked_src);

        extend_8u64u(unpacked_src, dst_ptr);

        src_ptr += 8u * 1u;
        values_to_read -= 64u;
    }
    if (values_to_read > 32u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint64_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = (uint64_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 16u] = (uint64_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 24u] = (uint64_t)bit_byte_pool.byte_buf[3];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 8u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint64_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 8u] = (uint64_t)bit_byte_pool.byte_buf[1];
            src = src >> 1u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }

    if (values_to_read) {
        mask = OWN_1_BIT_MASK;
        uint8_t src = *src_ptr;
        for (i = 0u; i < values_to_read; i++) {
            if (8u == i) {
                src = *(++src_ptr);
            }
            dst_ptr[i] = (uint64_t)(src & mask);
            src = src >> 1u;
        }
    }
    return src_ptr;
}

// ********************** 2u ****************************** //
template <typename OutType>
const uint8_t* unpack_2u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);
template <>
inline const uint8_t* unpack_2u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    uint64_t bit_mask = 0x0303030303030303LLU;
    uint32_t i;
    uint8_t mask = 3u;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_16U;              // first 16 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0303); // 2 times 1 then (8 - 2) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            __m512i srcmm1, srcmm2, srcmm3, tmpmm;

            srcmm1 = _mm512_srli_epi16(srcmm0, 2u);
            srcmm2 = _mm512_srli_epi16(srcmm0, 4u);
            srcmm3 = _mm512_srli_epi16(srcmm0, 6u);

            // turn 2 bit_width into 8 by zeroing 3 of each 4 elements.
            // move them into their places
            // srcmm0: a e i m 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm1: b f j n 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);       // ab ef 00 00 00 00 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);      // ij mn 00 00 00 00 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x00); // ab ef ab ef ij mn ij mn

            // srcmm2: c g k o 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm3: d h l p 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm2, srcmm3);       // cd gh 00 00 00 00 00 00
            srcmm1 = _mm512_unpackhi_epi8(srcmm2, srcmm3);      // kl op 00 00 00 00 00 00
            srcmm1 = _mm512_shuffle_i64x2(tmpmm, srcmm1, 0x00); // cd gh cd gh kl op kl op

            tmpmm = _mm512_unpacklo_epi16(srcmm0, srcmm1);       // abcd abcd ijkl ijkl
            srcmm0 = _mm512_unpackhi_epi16(srcmm0, srcmm1);      // efgh efgh mnop mnop
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x88);  // abcd ijkl efgh mnop
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // abcd efgh ijkl mnop

            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);

            _mm512_storeu_si512(dst_ptr, srcmm0);

            src_ptr += 8u * 2u;
            dst_ptr += 64u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = bit_byte_pool.byte_buf[3];
            dst_ptr[i + 16u] = bit_byte_pool.byte_buf[4];
            dst_ptr[i + 20u] = bit_byte_pool.byte_buf[5];
            dst_ptr[i + 24u] = bit_byte_pool.byte_buf[6];
            dst_ptr[i + 28u] = bit_byte_pool.byte_buf[7];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint64_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = bit_byte_pool.byte_buf[3];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = bit_byte_pool.byte_buf[1];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    if (values_to_read) {
        // 4 elements are stored in a single byte
        const uint32_t elements_in_byte = 4u;

        // Process rest of elements (there are 8 or less elements left)
        mask = OWN_2_BIT_MASK;
        uint8_t src_buffer = *src_ptr;

        for (i = 0u; i < values_to_read; i++) {
            // Check if 1st byte is processed, shift to next one if true
            if (elements_in_byte == i) {
                src_buffer = *(++src_ptr);
            }

            dst_ptr[i] = (src_buffer & mask);
            src_buffer = src_buffer >> 2u;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_2u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    uint64_t bit_mask = 0x0303030303030303LLU;
    uint32_t i;
    uint8_t mask = 3u;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_16U;              // first 16 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0303); // 2 times 1 then (8 - 2) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            __m512i srcmm1, srcmm2, srcmm3, tmpmm;

            srcmm1 = _mm512_srli_epi16(srcmm0, 2u);
            srcmm2 = _mm512_srli_epi16(srcmm0, 4u);
            srcmm3 = _mm512_srli_epi16(srcmm0, 6u);

            // turn 2 bit_width into 8 by zeroing 3 of each 4 elements.
            // move them into their places
            // srcmm0: a e i m 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm1: b f j n 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);       // ab ef 00 00 00 00 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);      // ij mn 00 00 00 00 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x00); // ab ef ab ef ij mn ij mn

            // srcmm2: c g k o 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm3: d h l p 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm2, srcmm3);       // cd gh 00 00 00 00 00 00
            srcmm1 = _mm512_unpackhi_epi8(srcmm2, srcmm3);      // kl op 00 00 00 00 00 00
            srcmm1 = _mm512_shuffle_i64x2(tmpmm, srcmm1, 0x00); // cd gh cd gh kl op kl op

            tmpmm = _mm512_unpacklo_epi16(srcmm0, srcmm1);       // abcd abcd ijkl ijkl
            srcmm0 = _mm512_unpackhi_epi16(srcmm0, srcmm1);      // efgh efgh mnop mnop
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x88);  // abcd ijkl efgh mnop
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // abcd efgh ijkl mnop

            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);

            extend_8u16u(srcmm0, dst_ptr);

            src_ptr += 8u * 2u;
            dst_ptr += 64;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint16_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint16_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = (uint16_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = (uint16_t)bit_byte_pool.byte_buf[3];
            dst_ptr[i + 16u] = (uint16_t)bit_byte_pool.byte_buf[4];
            dst_ptr[i + 20u] = (uint16_t)bit_byte_pool.byte_buf[5];
            dst_ptr[i + 24u] = (uint16_t)bit_byte_pool.byte_buf[6];
            dst_ptr[i + 28u] = (uint16_t)bit_byte_pool.byte_buf[7];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint64_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint16_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint16_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = (uint16_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = (uint16_t)bit_byte_pool.byte_buf[3];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint16_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint16_t)bit_byte_pool.byte_buf[1];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    if (values_to_read) {
        // 4 elements are stored in a single byte
        const uint32_t elements_in_byte = 4u;

        // Process rest of elements (there are 8 or less elements left)
        mask = OWN_2_BIT_MASK;
        uint8_t src_buffer = *src_ptr;

        for (i = 0u; i < values_to_read; i++) {
            // Check if 1st byte is processed, shift to next one if true
            if (elements_in_byte == i) {
                src_buffer = *(++src_ptr);
            }

            dst_ptr[i] = (uint16_t)(src_buffer & mask);
            src_buffer = src_buffer >> 2u;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_2u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    uint64_t bit_mask = 0x0303030303030303LLU;
    uint32_t i;
    uint8_t mask = 3u;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_16U;              // first 16 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0303); // 2 times 1 then (8 - 2) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            __m512i srcmm1, srcmm2, srcmm3, tmpmm;

            srcmm1 = _mm512_srli_epi16(srcmm0, 2u);
            srcmm2 = _mm512_srli_epi16(srcmm0, 4u);
            srcmm3 = _mm512_srli_epi16(srcmm0, 6u);

            // turn 2 bit_width into 8 by zeroing 3 of each 4 elements.
            // move them into their places
            // srcmm0: a e i m 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm1: b f j n 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);       // ab ef 00 00 00 00 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);      // ij mn 00 00 00 00 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x00); // ab ef ab ef ij mn ij mn

            // srcmm2: c g k o 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm3: d h l p 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm2, srcmm3);       // cd gh 00 00 00 00 00 00
            srcmm1 = _mm512_unpackhi_epi8(srcmm2, srcmm3);      // kl op 00 00 00 00 00 00
            srcmm1 = _mm512_shuffle_i64x2(tmpmm, srcmm1, 0x00); // cd gh cd gh kl op kl op

            tmpmm = _mm512_unpacklo_epi16(srcmm0, srcmm1);       // abcd abcd ijkl ijkl
            srcmm0 = _mm512_unpackhi_epi16(srcmm0, srcmm1);      // efgh efgh mnop mnop
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x88);  // abcd ijkl efgh mnop
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // abcd efgh ijkl mnop

            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);

            extend_8u32u(srcmm0, dst_ptr);

            src_ptr += 8u * 2u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint32_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint32_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = (uint32_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = (uint32_t)bit_byte_pool.byte_buf[3];
            dst_ptr[i + 16u] = (uint32_t)bit_byte_pool.byte_buf[4];
            dst_ptr[i + 20u] = (uint32_t)bit_byte_pool.byte_buf[5];
            dst_ptr[i + 24u] = (uint32_t)bit_byte_pool.byte_buf[6];
            dst_ptr[i + 28u] = (uint32_t)bit_byte_pool.byte_buf[7];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint64_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint32_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint32_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = (uint32_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = (uint32_t)bit_byte_pool.byte_buf[3];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint32_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint32_t)bit_byte_pool.byte_buf[1];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    if (values_to_read) {
        // 4 elements are stored in a single byte
        const uint32_t elements_in_byte = 4u;

        // Process rest of elements (there are 8 or less elements left)
        mask = OWN_2_BIT_MASK;
        uint8_t src_buffer = *src_ptr;

        for (i = 0u; i < values_to_read; i++) {
            // Check if 1st byte is processed, shift to next one if true
            if (elements_in_byte == i) {
                src_buffer = *(++src_ptr);
            }

            dst_ptr[i] = (uint32_t)(src_buffer & mask);
            src_buffer = src_buffer >> 2u;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_2u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    uint64_t bit_mask = 0x0303030303030303LLU;
    uint32_t i;
    uint8_t mask = 3u;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_16U;              // first 16 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0303); // 2 times 1 then (8 - 2) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            __m512i srcmm1, srcmm2, srcmm3, tmpmm;

            srcmm1 = _mm512_srli_epi16(srcmm0, 2u);
            srcmm2 = _mm512_srli_epi16(srcmm0, 4u);
            srcmm3 = _mm512_srli_epi16(srcmm0, 6u);

            // turn 2 bit_width into 8 by zeroing 3 of each 4 elements.
            // move them into their places
            // srcmm0: a e i m 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm1: b f j n 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);       // ab ef 00 00 00 00 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);      // ij mn 00 00 00 00 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x00); // ab ef ab ef ij mn ij mn

            // srcmm2: c g k o 0 0 0 0 0 0 0 0 0 0 0 0
            // srcmm3: d h l p 0 0 0 0 0 0 0 0 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm2, srcmm3);       // cd gh 00 00 00 00 00 00
            srcmm1 = _mm512_unpackhi_epi8(srcmm2, srcmm3);      // kl op 00 00 00 00 00 00
            srcmm1 = _mm512_shuffle_i64x2(tmpmm, srcmm1, 0x00); // cd gh cd gh kl op kl op

            tmpmm = _mm512_unpacklo_epi16(srcmm0, srcmm1);       // abcd abcd ijkl ijkl
            srcmm0 = _mm512_unpackhi_epi16(srcmm0, srcmm1);      // efgh efgh mnop mnop
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x88);  // abcd ijkl efgh mnop
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // abcd efgh ijkl mnop

            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);

            extend_8u64u(srcmm0, dst_ptr);

            src_ptr += 8u * 2u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint64_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint64_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = (uint64_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = (uint64_t)bit_byte_pool.byte_buf[3];
            dst_ptr[i + 16u] = (uint64_t)bit_byte_pool.byte_buf[4];
            dst_ptr[i + 20u] = (uint64_t)bit_byte_pool.byte_buf[5];
            dst_ptr[i + 24u] = (uint64_t)bit_byte_pool.byte_buf[6];
            dst_ptr[i + 28u] = (uint64_t)bit_byte_pool.byte_buf[7];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint64_t);
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint64_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint64_t)bit_byte_pool.byte_buf[1];
            dst_ptr[i + 8u] = (uint64_t)bit_byte_pool.byte_buf[2];
            dst_ptr[i + 12u] = (uint64_t)bit_byte_pool.byte_buf[3];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint32_t);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool16_t bit_byte_pool;
        uint16_t* tmp_src = (uint16_t*)src_ptr;
        uint16_t src = *tmp_src;

        for (i = 0u; i < 4u; i++) {
            bit_byte_pool.bit_buf = src & bit_mask;
            dst_ptr[i + 0u] = (uint64_t)bit_byte_pool.byte_buf[0];
            dst_ptr[i + 4u] = (uint64_t)bit_byte_pool.byte_buf[1];
            src = src >> 2u;
        }
        src_ptr += sizeof(uint16_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    if (values_to_read) {
        // 4 elements are stored in a single byte
        const uint32_t elements_in_byte = 4u;

        // Process rest of elements (there are 8 or less elements left)
        mask = OWN_2_BIT_MASK;
        uint8_t src_buffer = *src_ptr;

        for (i = 0u; i < values_to_read; i++) {
            // Check if 1st byte is processed, shift to next one if true
            if (elements_in_byte == i) {
                src_buffer = *(++src_ptr);
            }

            dst_ptr[i] = (uint64_t)(src_buffer & mask);
            src_buffer = src_buffer >> 2u;
        }
    }
    return src_ptr;
}

// ********************** 3u ****************************** //
template <typename OutType>
const uint8_t* unpack_3u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);
template <>
inline const uint8_t* unpack_3u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    uint64_t bit_mask0 = 0x0007000007000007LLU;
    uint64_t bit_mask1 = 0x0700000700000700LLU;
    uint64_t bit_mask2 = 0x0000070000070000LLU;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x07);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_3u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_3u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_3u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_3u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 8u * 3u;
            dst_ptr += 64u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + 8u;
        bit_byte_pool32_t bit_byte_pool32;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr = src_ptr + 4u;

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = bit_byte_pool64.byte_buf[0];
        dst_ptr[8] = bit_byte_pool64.byte_buf[3];
        dst_ptr[16] = bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = bit_byte_pool64.byte_buf[1];
        dst_ptr[11] = bit_byte_pool64.byte_buf[4];
        dst_ptr[19] = bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = bit_byte_pool64.byte_buf[2];
        dst_ptr[14] = bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = bit_byte_pool64.byte_buf[3];
        dst_ptr[17] = bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = bit_byte_pool64.byte_buf[1];
        dst_ptr[12] = bit_byte_pool64.byte_buf[4];
        dst_ptr[20] = bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = bit_byte_pool64.byte_buf[2];
        dst_ptr[15] = bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = bit_byte_pool64.byte_buf[0];
        dst_ptr[10] = bit_byte_pool64.byte_buf[3];
        dst_ptr[18] = bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = bit_byte_pool64.byte_buf[1];
        dst_ptr[13] = bit_byte_pool64.byte_buf[4];
        dst_ptr[21] = bit_byte_pool64.byte_buf[7] | (((uint8_t)src32 & 3u) << 1u);
        src32 = src32 >> 2u;
        bit_byte_pool32.bit_buf = src32 & bit_mask0;
        dst_ptr[22] = bit_byte_pool32.byte_buf[0];
        dst_ptr[30] = bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & bit_mask1;
        dst_ptr[25] = bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & bit_mask2;
        dst_ptr[28] = bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & bit_mask0;
        dst_ptr[23] = bit_byte_pool32.byte_buf[0];
        dst_ptr[31] = bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & bit_mask1;
        dst_ptr[26] = bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & bit_mask2;
        dst_ptr[29] = bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & bit_mask0;
        dst_ptr[24] = bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & bit_mask1;
        dst_ptr[27] = bit_byte_pool32.byte_buf[1];
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool48_t bit_byte_pool48;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        bit_byte_pool48.dw_buf[0] = (*tmp_src32);
        src_ptr = src_ptr + sizeof(uint32_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool48.word_buf[2] = (*tmp_src16);
        src_ptr = src_ptr + sizeof(uint16_t);
        uint64_t src64 = bit_byte_pool48.bit_buf;
        bit_byte_pool48.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = bit_byte_pool48.byte_buf[0];
        dst_ptr[8] = bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = bit_byte_pool48.byte_buf[1];
        dst_ptr[11] = bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = bit_byte_pool48.byte_buf[2];
        dst_ptr[14] = bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = bit_byte_pool48.byte_buf[0];
        dst_ptr[9] = bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = bit_byte_pool48.byte_buf[1];
        dst_ptr[12] = bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = bit_byte_pool48.byte_buf[2];
        dst_ptr[15] = bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = bit_byte_pool48.byte_buf[0];
        dst_ptr[10] = bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = bit_byte_pool48.byte_buf[1];
        dst_ptr[13] = bit_byte_pool48.byte_buf[4];
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool32;
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool32.word_buf[0] = (*tmp_src16);
        src_ptr = src_ptr + 2u;
        bit_byte_pool32.byte_buf[2] = (*src_ptr);
        src_ptr = src_ptr + 1u;
        uint32_t src32 = bit_byte_pool32.bit_buf;
        bit_byte_pool32.bit_buf = src32 & (uint32_t)bit_mask0;
        dst_ptr[0] = bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & (uint32_t)bit_mask0;
        dst_ptr[1] = bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & (uint32_t)bit_mask0;
        dst_ptr[2] = bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & (uint32_t)bit_mask1;
        dst_ptr[3] = bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & (uint32_t)bit_mask1;
        dst_ptr[4] = bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & (uint32_t)bit_mask1;
        dst_ptr[5] = bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & (uint32_t)bit_mask2;
        dst_ptr[6] = bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & (uint32_t)bit_mask2;
        dst_ptr[7] = bit_byte_pool32.byte_buf[2];
        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t mask = OWN_3_BIT_MASK;
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (3u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint8_t)(src & mask);
            src = src >> 3u;
            bits_in_buf -= 3u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_3u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    uint64_t bit_mask0 = 0x0007000007000007LLU;
    uint64_t bit_mask1 = 0x0700000700000700LLU;
    uint64_t bit_mask2 = 0x0000070000070000LLU;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x07);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_3u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_3u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_3u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_3u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u16u(zmm[0], dst_ptr);

            src_ptr += 8u * 3u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + 8u;
        bit_byte_pool32_t bit_byte_pool32;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr = src_ptr + 4u;

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint16_t)(bit_byte_pool64.byte_buf[0]);
        dst_ptr[8] = (uint16_t)(bit_byte_pool64.byte_buf[3]);
        dst_ptr[16] = (uint16_t)(bit_byte_pool64.byte_buf[6]);
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = (uint16_t)(bit_byte_pool64.byte_buf[1]);
        dst_ptr[11] = (uint16_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[19] = (uint16_t)bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = (uint16_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[14] = (uint16_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = (uint16_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = (uint16_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[17] = (uint16_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = (uint16_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[12] = (uint16_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[20] = (uint16_t)bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = (uint16_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[15] = (uint16_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = (uint16_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[10] = (uint16_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[18] = (uint16_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = (uint16_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[13] = (uint16_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[21] = (uint16_t)bit_byte_pool64.byte_buf[7] | (((uint8_t)src32 & 3u) << 1u);
        src32 = src32 >> 2u;
        bit_byte_pool32.bit_buf = src32 & bit_mask0;
        dst_ptr[22] = (uint16_t)bit_byte_pool32.byte_buf[0];
        dst_ptr[30] = (uint16_t)bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & bit_mask1;
        dst_ptr[25] = (uint16_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & bit_mask2;
        dst_ptr[28] = (uint16_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & bit_mask0;
        dst_ptr[23] = (uint16_t)bit_byte_pool32.byte_buf[0];
        dst_ptr[31] = (uint16_t)bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & bit_mask1;
        dst_ptr[26] = (uint16_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & bit_mask2;
        dst_ptr[29] = (uint16_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & bit_mask0;
        dst_ptr[24] = (uint16_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & bit_mask1;
        dst_ptr[27] = (uint16_t)bit_byte_pool32.byte_buf[1];
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool48_t bit_byte_pool48;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        bit_byte_pool48.dw_buf[0] = (*tmp_src32);
        src_ptr = src_ptr + sizeof(uint32_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool48.word_buf[2] = (*tmp_src16);
        src_ptr = src_ptr + sizeof(uint16_t);
        uint64_t src64 = bit_byte_pool48.bit_buf;
        bit_byte_pool48.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint16_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[8] = (uint16_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = (uint16_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[11] = (uint16_t)bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = (uint16_t)bit_byte_pool48.byte_buf[2];
        dst_ptr[14] = (uint16_t)bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = (uint16_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[9] = (uint16_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = (uint16_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[12] = (uint16_t)bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = (uint16_t)bit_byte_pool48.byte_buf[2];
        dst_ptr[15] = (uint16_t)bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = (uint16_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[10] = (uint16_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = (uint16_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[13] = (uint16_t)bit_byte_pool48.byte_buf[4];
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool32;
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool32.word_buf[0] = (*tmp_src16);
        src_ptr = src_ptr + 2u;
        bit_byte_pool32.byte_buf[2] = (*src_ptr);
        src_ptr = src_ptr + 1u;
        uint32_t src32 = bit_byte_pool32.bit_buf;
        bit_byte_pool32.bit_buf = src32 & (uint32_t)bit_mask0;
        dst_ptr[0] = (uint16_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & (uint32_t)bit_mask0;
        dst_ptr[1] = (uint16_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & (uint32_t)bit_mask0;
        dst_ptr[2] = (uint16_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & (uint32_t)bit_mask1;
        dst_ptr[3] = (uint16_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & (uint32_t)bit_mask1;
        dst_ptr[4] = (uint16_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & (uint32_t)bit_mask1;
        dst_ptr[5] = (uint16_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & (uint32_t)bit_mask2;
        dst_ptr[6] = (uint16_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & (uint32_t)bit_mask2;
        dst_ptr[7] = (uint16_t)bit_byte_pool32.byte_buf[2];
        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t mask = OWN_3_BIT_MASK;
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (3u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint16_t)(src & mask);
            src = src >> 3u;
            bits_in_buf -= 3u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}
template <>
inline const uint8_t* unpack_3u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    uint64_t bit_mask0 = 0x0007000007000007LLU;
    uint64_t bit_mask1 = 0x0700000700000700LLU;
    uint64_t bit_mask2 = 0x0000070000070000LLU;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x07);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_3u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_3u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_3u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_3u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u32u(zmm[0], dst_ptr);

            src_ptr += 8u * 3u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + 8u;
        bit_byte_pool32_t bit_byte_pool32;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr = src_ptr + 4u;

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint32_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[8] = (uint32_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[16] = (uint32_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = (uint32_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[11] = (uint32_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[19] = (uint32_t)bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = (uint32_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[14] = (uint32_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = (uint32_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = (uint32_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[17] = (uint32_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = (uint32_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[12] = (uint32_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[20] = (uint32_t)bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = (uint32_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[15] = (uint32_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = (uint32_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[10] = (uint32_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[18] = (uint32_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = (uint32_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[13] = (uint32_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[21] = (uint32_t)(bit_byte_pool64.byte_buf[7] | (((uint8_t)src32 & 3u) << 1u));
        src32 = src32 >> 2u;
        bit_byte_pool32.bit_buf = src32 & bit_mask0;
        dst_ptr[22] = (uint32_t)bit_byte_pool32.byte_buf[0];
        dst_ptr[30] = (uint32_t)bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & bit_mask1;
        dst_ptr[25] = (uint32_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & bit_mask2;
        dst_ptr[28] = (uint32_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & bit_mask0;
        dst_ptr[23] = (uint32_t)bit_byte_pool32.byte_buf[0];
        dst_ptr[31] = (uint32_t)bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & bit_mask1;
        dst_ptr[26] = (uint32_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & bit_mask2;
        dst_ptr[29] = (uint32_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & bit_mask0;
        dst_ptr[24] = (uint32_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & bit_mask1;
        dst_ptr[27] = (uint32_t)bit_byte_pool32.byte_buf[1];
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool48_t bit_byte_pool48;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        bit_byte_pool48.dw_buf[0] = (*tmp_src32);
        src_ptr = src_ptr + sizeof(uint32_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool48.word_buf[2] = (*tmp_src16);
        src_ptr = src_ptr + sizeof(uint16_t);
        uint64_t src64 = bit_byte_pool48.bit_buf;
        bit_byte_pool48.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint32_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[8] = (uint32_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = (uint32_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[11] = (uint32_t)bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = (uint32_t)bit_byte_pool48.byte_buf[2];
        dst_ptr[14] = (uint32_t)bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = (uint32_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[9] = (uint32_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = (uint32_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[12] = (uint32_t)bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = (uint32_t)bit_byte_pool48.byte_buf[2];
        dst_ptr[15] = (uint32_t)bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = (uint32_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[10] = (uint32_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = (uint32_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[13] = (uint32_t)bit_byte_pool48.byte_buf[4];
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool32;
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool32.word_buf[0] = (*tmp_src16);
        src_ptr = src_ptr + 2u;
        bit_byte_pool32.byte_buf[2] = (*src_ptr);
        src_ptr = src_ptr + 1u;
        uint32_t src32 = bit_byte_pool32.bit_buf;
        bit_byte_pool32.bit_buf = src32 & (uint32_t)bit_mask0;
        dst_ptr[0] = (uint32_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & (uint32_t)bit_mask0;
        dst_ptr[1] = (uint32_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & (uint32_t)bit_mask0;
        dst_ptr[2] = (uint32_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & (uint32_t)bit_mask1;
        dst_ptr[3] = (uint32_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & (uint32_t)bit_mask1;
        dst_ptr[4] = (uint32_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & (uint32_t)bit_mask1;
        dst_ptr[5] = (uint32_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & (uint32_t)bit_mask2;
        dst_ptr[6] = (uint32_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & (uint32_t)bit_mask2;
        dst_ptr[7] = (uint32_t)bit_byte_pool32.byte_buf[2];
        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t mask = OWN_3_BIT_MASK;
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (3u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint32_t)(src & mask);
            src = src >> 3u;
            bits_in_buf -= 3u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}
template <>
inline const uint8_t* unpack_3u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    uint64_t bit_mask0 = 0x0007000007000007LLU;
    uint64_t bit_mask1 = 0x0700000700000700LLU;
    uint64_t bit_mask2 = 0x0000070000070000LLU;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x07);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_3u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_3u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_3u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_3u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u64u(zmm[0], dst_ptr);

            src_ptr += 8u * 3u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + 8u;
        bit_byte_pool32_t bit_byte_pool32;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr = src_ptr + 4u;

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint64_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[8] = (uint64_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[16] = (uint64_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = (uint64_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[11] = (uint64_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[19] = (uint64_t)bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = (uint64_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[14] = (uint64_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = (uint64_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = (uint64_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[17] = (uint64_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = (uint64_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[12] = (uint64_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[20] = (uint64_t)bit_byte_pool64.byte_buf[7];
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = (uint64_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[15] = (uint64_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = (uint64_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[10] = (uint64_t)bit_byte_pool64.byte_buf[3];
        dst_ptr[18] = (uint64_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = (uint64_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[13] = (uint64_t)bit_byte_pool64.byte_buf[4];
        dst_ptr[21] = (uint64_t)(bit_byte_pool64.byte_buf[7] | (((uint8_t)src32 & 3u) << 1u));
        src32 = src32 >> 2u;
        bit_byte_pool32.bit_buf = src32 & bit_mask0;
        dst_ptr[22] = (uint64_t)bit_byte_pool32.byte_buf[0];
        dst_ptr[30] = (uint64_t)bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & bit_mask1;
        dst_ptr[25] = (uint64_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & bit_mask2;
        dst_ptr[28] = (uint64_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & bit_mask0;
        dst_ptr[23] = (uint64_t)bit_byte_pool32.byte_buf[0];
        dst_ptr[31] = (uint64_t)bit_byte_pool32.byte_buf[3];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & bit_mask1;
        dst_ptr[26] = (uint64_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & bit_mask2;
        dst_ptr[29] = (uint64_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & bit_mask0;
        dst_ptr[24] = (uint64_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & bit_mask1;
        dst_ptr[27] = (uint64_t)bit_byte_pool32.byte_buf[1];
        dst_ptr += 32u;
        values_to_read -= 32u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool48_t bit_byte_pool48;
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        bit_byte_pool48.dw_buf[0] = (*tmp_src32);
        src_ptr = src_ptr + sizeof(uint32_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool48.word_buf[2] = (*tmp_src16);
        src_ptr = src_ptr + sizeof(uint16_t);
        uint64_t src64 = bit_byte_pool48.bit_buf;
        bit_byte_pool48.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint64_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[8] = (uint64_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[3] = (uint64_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[11] = (uint64_t)bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[6] = (uint64_t)bit_byte_pool48.byte_buf[2];
        dst_ptr[14] = (uint64_t)bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 3u) & bit_mask0;
        dst_ptr[1] = (uint64_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[9] = (uint64_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 4u) & bit_mask1;
        dst_ptr[4] = (uint64_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[12] = (uint64_t)bit_byte_pool48.byte_buf[4];
        bit_byte_pool48.bit_buf = (src64 >> 5u) & bit_mask2;
        dst_ptr[7] = (uint64_t)bit_byte_pool48.byte_buf[2];
        dst_ptr[15] = (uint64_t)bit_byte_pool48.byte_buf[5];
        bit_byte_pool48.bit_buf = (src64 >> 6u) & bit_mask0;
        dst_ptr[2] = (uint64_t)bit_byte_pool48.byte_buf[0];
        dst_ptr[10] = (uint64_t)bit_byte_pool48.byte_buf[3];
        bit_byte_pool48.bit_buf = (src64 >> 7u) & bit_mask1;
        dst_ptr[5] = (uint64_t)bit_byte_pool48.byte_buf[1];
        dst_ptr[13] = (uint64_t)bit_byte_pool48.byte_buf[4];
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool32;
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        bit_byte_pool32.word_buf[0] = (*tmp_src16);
        src_ptr = src_ptr + 2u;
        bit_byte_pool32.byte_buf[2] = (*src_ptr);
        src_ptr = src_ptr + 1u;
        uint32_t src32 = bit_byte_pool32.bit_buf;
        bit_byte_pool32.bit_buf = src32 & (uint32_t)bit_mask0;
        dst_ptr[0] = (uint64_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 3u) & (uint32_t)bit_mask0;
        dst_ptr[1] = (uint64_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 6u) & (uint32_t)bit_mask0;
        dst_ptr[2] = (uint64_t)bit_byte_pool32.byte_buf[0];
        bit_byte_pool32.bit_buf = (src32 >> 1u) & (uint32_t)bit_mask1;
        dst_ptr[3] = (uint64_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 4u) & (uint32_t)bit_mask1;
        dst_ptr[4] = (uint64_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 7u) & (uint32_t)bit_mask1;
        dst_ptr[5] = (uint64_t)bit_byte_pool32.byte_buf[1];
        bit_byte_pool32.bit_buf = (src32 >> 2u) & (uint32_t)bit_mask2;
        dst_ptr[6] = (uint64_t)bit_byte_pool32.byte_buf[2];
        bit_byte_pool32.bit_buf = (src32 >> 5u) & (uint32_t)bit_mask2;
        dst_ptr[7] = (uint64_t)bit_byte_pool32.byte_buf[2];
        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t mask = OWN_3_BIT_MASK;
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (3u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint64_t)(src & mask);
            src = src >> 3u;
            bits_in_buf -= 3u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}

// ********************** 4u ****************************** //
template <typename OutType>
const uint8_t* unpack_4u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_4u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    uint64_t bit_mask = 0x0f0f0f0f0f0f0f0fLLU;
    uint32_t i;
    uint8_t mask = OWN_4_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_32U;              // first 32 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0F0F); // 4 times 1 then (8 - 4) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, tmpmm;

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_srli_epi16(srcmm0, 4u);

            // move elements into their places
            // srcmm0: a c e g 0 0 0 0
            // srcmm1: b d f h 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);        // ab ef 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);       // cd gh 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x44);  // ab ef cd gh
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // ab cd ef gh

            // turn 4 bit_width into 8 by zeroing 4 of each 8 bits.
            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);
            _mm512_storeu_si512(dst_ptr, srcmm0);

            src_ptr += 8u * 4u;
            dst_ptr += 64u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = bit_byte_pool.byte_buf[0];
        dst_ptr[2] = bit_byte_pool.byte_buf[1];
        dst_ptr[4] = bit_byte_pool.byte_buf[2];
        dst_ptr[6] = bit_byte_pool.byte_buf[3];
        dst_ptr[8] = bit_byte_pool.byte_buf[4];
        dst_ptr[10] = bit_byte_pool.byte_buf[5];
        dst_ptr[12] = bit_byte_pool.byte_buf[6];
        dst_ptr[14] = bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = bit_byte_pool.byte_buf[0];
        dst_ptr[3] = bit_byte_pool.byte_buf[1];
        dst_ptr[5] = bit_byte_pool.byte_buf[2];
        dst_ptr[7] = bit_byte_pool.byte_buf[3];
        dst_ptr[9] = bit_byte_pool.byte_buf[4];
        dst_ptr[11] = bit_byte_pool.byte_buf[5];
        dst_ptr[13] = bit_byte_pool.byte_buf[6];
        dst_ptr[15] = bit_byte_pool.byte_buf[7];
        tmp_src++;
        dst_ptr += 16u;
        src = *tmp_src;
        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = bit_byte_pool.byte_buf[0];
        dst_ptr[2] = bit_byte_pool.byte_buf[1];
        dst_ptr[4] = bit_byte_pool.byte_buf[2];
        dst_ptr[6] = bit_byte_pool.byte_buf[3];
        dst_ptr[8] = bit_byte_pool.byte_buf[4];
        dst_ptr[10] = bit_byte_pool.byte_buf[5];
        dst_ptr[12] = bit_byte_pool.byte_buf[6];
        dst_ptr[14] = bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = bit_byte_pool.byte_buf[0];
        dst_ptr[3] = bit_byte_pool.byte_buf[1];
        dst_ptr[5] = bit_byte_pool.byte_buf[2];
        dst_ptr[7] = bit_byte_pool.byte_buf[3];
        dst_ptr[9] = bit_byte_pool.byte_buf[4];
        dst_ptr[11] = bit_byte_pool.byte_buf[5];
        dst_ptr[13] = bit_byte_pool.byte_buf[6];
        dst_ptr[15] = bit_byte_pool.byte_buf[7];
        values_to_read -= 32u;
        dst_ptr += 16u;
        src_ptr += 16u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = bit_byte_pool.byte_buf[0];
        dst_ptr[2] = bit_byte_pool.byte_buf[1];
        dst_ptr[4] = bit_byte_pool.byte_buf[2];
        dst_ptr[6] = bit_byte_pool.byte_buf[3];
        dst_ptr[8] = bit_byte_pool.byte_buf[4];
        dst_ptr[10] = bit_byte_pool.byte_buf[5];
        dst_ptr[12] = bit_byte_pool.byte_buf[6];
        dst_ptr[14] = bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = bit_byte_pool.byte_buf[0];
        dst_ptr[3] = bit_byte_pool.byte_buf[1];
        dst_ptr[5] = bit_byte_pool.byte_buf[2];
        dst_ptr[7] = bit_byte_pool.byte_buf[3];
        dst_ptr[9] = bit_byte_pool.byte_buf[4];
        dst_ptr[11] = bit_byte_pool.byte_buf[5];
        dst_ptr[13] = bit_byte_pool.byte_buf[6];
        dst_ptr[15] = bit_byte_pool.byte_buf[7];
        dst_ptr += 16u;
        values_to_read -= 16u;
        src_ptr += 8u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & (uint32_t)bit_mask;
        dst_ptr[0] = bit_byte_pool.byte_buf[0];
        dst_ptr[2] = bit_byte_pool.byte_buf[1];
        dst_ptr[4] = bit_byte_pool.byte_buf[2];
        dst_ptr[6] = bit_byte_pool.byte_buf[3];
        bit_byte_pool.bit_buf = (src >> 4u) & (uint32_t)bit_mask;
        dst_ptr[1] = bit_byte_pool.byte_buf[0];
        dst_ptr[3] = bit_byte_pool.byte_buf[1];
        dst_ptr[5] = bit_byte_pool.byte_buf[2];
        dst_ptr[7] = bit_byte_pool.byte_buf[3];

        src_ptr += sizeof(uint32_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    uint8_t src;

    if (values_to_read > 0) {
        src = *src_ptr;
        i = 0u;
        while (values_to_read >= 2u) {
            src = *src_ptr;
            dst_ptr[i] = (src & mask);
            src = src >> 4u;
            dst_ptr[i + 1] = (src & mask);
            ++src_ptr;

            values_to_read -= 2u;
            i += 2u;
        }
    }

    if (values_to_read > 0) {
        src = *src_ptr;
        dst_ptr[i] = (src & mask);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_4u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    uint64_t bit_mask = 0x0f0f0f0f0f0f0f0fLLU;
    uint32_t i;
    uint8_t mask = OWN_4_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_32U;              // first 32 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0F0F); // 4 times 1 then (8 - 4) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, tmpmm;

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_srli_epi16(srcmm0, 4u);

            // move elements into their places
            // srcmm0: a c e g 0 0 0 0
            // srcmm1: b d f h 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);        // ab ef 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);       // cd gh 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x44);  // ab ef cd gh
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // ab cd ef gh

            // turn 4 bit_width into 8 by zeroing 4 of each 8 bits.
            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);
            extend_8u16u(srcmm0, dst_ptr);

            src_ptr += 8u * 4u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint16_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint16_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint16_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint16_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint16_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint16_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint16_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint16_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint16_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint16_t)bit_byte_pool.byte_buf[7];
        tmp_src++;
        dst_ptr += 16u;
        src = *tmp_src;
        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint16_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint16_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint16_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint16_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint16_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint16_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint16_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint16_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint16_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint16_t)bit_byte_pool.byte_buf[7];
        values_to_read -= 32u;
        dst_ptr += 16u;
        src_ptr += 16u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint16_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint16_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint16_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint16_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint16_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint16_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint16_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint16_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint16_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint16_t)bit_byte_pool.byte_buf[7];
        dst_ptr += 16u;
        values_to_read -= 16u;
        src_ptr += 8u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & (uint32_t)bit_mask;
        dst_ptr[0] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint16_t)bit_byte_pool.byte_buf[3];
        bit_byte_pool.bit_buf = (src >> 4u) & (uint32_t)bit_mask;
        dst_ptr[1] = (uint16_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint16_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint16_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint16_t)bit_byte_pool.byte_buf[3];

        src_ptr += sizeof(uint32_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    uint8_t src;

    if (values_to_read > 0) {
        src = *src_ptr;
        i = 0u;
        while (values_to_read >= 2u) {
            src = *src_ptr;
            dst_ptr[i] = (uint16_t)(src & mask);
            src = src >> 4u;
            dst_ptr[i + 1] = (uint16_t)(src & mask);
            ++src_ptr;

            values_to_read -= 2u;
            i += 2u;
        }
    }

    if (values_to_read > 0) {
        src = *src_ptr;
        dst_ptr[i] = (uint16_t)(src & mask);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_4u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    uint64_t bit_mask = 0x0f0f0f0f0f0f0f0fLLU;
    uint32_t i;
    uint8_t mask = OWN_4_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_32U;              // first 32 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0F0F); // 4 times 1 then (8 - 4) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, tmpmm;

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_srli_epi16(srcmm0, 4u);

            // move elements into their places
            // srcmm0: a c e g 0 0 0 0
            // srcmm1: b d f h 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);        // ab ef 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);       // cd gh 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x44);  // ab ef cd gh
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // ab cd ef gh

            // turn 4 bit_width into 8 by zeroing 4 of each 8 bits.
            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);
            extend_8u32u(srcmm0, dst_ptr);

            src_ptr += 8u * 4u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint32_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint32_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint32_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint32_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint32_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint32_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint32_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint32_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint32_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint32_t)bit_byte_pool.byte_buf[7];
        tmp_src++;
        dst_ptr += 16u;
        src = *tmp_src;
        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint32_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint32_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint32_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint32_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint32_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint32_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint32_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint32_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint32_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint32_t)bit_byte_pool.byte_buf[7];
        values_to_read -= 32u;
        dst_ptr += 16u;
        src_ptr += 16u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint32_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint32_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint32_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint32_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint32_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint32_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint32_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint32_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint32_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint32_t)bit_byte_pool.byte_buf[7];
        dst_ptr += 16u;
        values_to_read -= 16u;
        src_ptr += 8u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & (uint32_t)bit_mask;
        dst_ptr[0] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint32_t)bit_byte_pool.byte_buf[3];
        bit_byte_pool.bit_buf = (src >> 4u) & (uint32_t)bit_mask;
        dst_ptr[1] = (uint32_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint32_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint32_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint32_t)bit_byte_pool.byte_buf[3];

        src_ptr += sizeof(uint32_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    uint8_t src;

    if (values_to_read > 0) {
        src = *src_ptr;
        i = 0u;
        while (values_to_read >= 2u) {
            src = *src_ptr;
            dst_ptr[i] = (uint32_t)(src & mask);
            src = src >> 4u;
            dst_ptr[i + 1] = (uint32_t)(src & mask);
            ++src_ptr;

            values_to_read -= 2u;
            i += 2u;
        }
    }

    if (values_to_read > 0) {
        src = *src_ptr;
        dst_ptr[i] = (uint32_t)(src & mask);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_4u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    uint64_t bit_mask = 0x0f0f0f0f0f0f0f0fLLU;
    uint32_t i;
    uint8_t mask = OWN_4_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = OWN_MAX_32U;              // first 32 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0F0F); // 4 times 1 then (8 - 4) times 0
        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, tmpmm;

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_srli_epi16(srcmm0, 4u);

            // move elements into their places
            // srcmm0: a c e g 0 0 0 0
            // srcmm1: b d f h 0 0 0 0
            tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);        // ab ef 00 00
            srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);       // cd gh 00 00
            srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x44);  // ab ef cd gh
            srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8); // ab cd ef gh

            // turn 4 bit_width into 8 by zeroing 4 of each 8 bits.
            srcmm0 = _mm512_and_si512(srcmm0, parse_mask);
            extend_8u64u(srcmm0, dst_ptr);

            src_ptr += 8u * 4u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 32u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint64_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint64_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint64_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint64_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint64_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint64_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint64_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint64_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint64_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint64_t)bit_byte_pool.byte_buf[7];
        tmp_src++;
        dst_ptr += 16u;
        src = *tmp_src;
        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint64_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint64_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint64_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint64_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint64_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint64_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint64_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint64_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint64_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint64_t)bit_byte_pool.byte_buf[7];
        values_to_read -= 32u;
        dst_ptr += 16u;
        src_ptr += 16u;
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool;
        uint64_t* tmp_src = (uint64_t*)src_ptr;
        uint64_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & bit_mask;
        dst_ptr[0] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint64_t)bit_byte_pool.byte_buf[3];
        dst_ptr[8] = (uint64_t)bit_byte_pool.byte_buf[4];
        dst_ptr[10] = (uint64_t)bit_byte_pool.byte_buf[5];
        dst_ptr[12] = (uint64_t)bit_byte_pool.byte_buf[6];
        dst_ptr[14] = (uint64_t)bit_byte_pool.byte_buf[7];
        bit_byte_pool.bit_buf = (src >> 4u) & bit_mask;
        dst_ptr[1] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint64_t)bit_byte_pool.byte_buf[3];
        dst_ptr[9] = (uint64_t)bit_byte_pool.byte_buf[4];
        dst_ptr[11] = (uint64_t)bit_byte_pool.byte_buf[5];
        dst_ptr[13] = (uint64_t)bit_byte_pool.byte_buf[6];
        dst_ptr[15] = (uint64_t)bit_byte_pool.byte_buf[7];
        dst_ptr += 16u;
        values_to_read -= 16u;
        src_ptr += 8u;
    }
    if (values_to_read > 8u) {
        bit_byte_pool32_t bit_byte_pool;
        uint32_t* tmp_src = (uint32_t*)src_ptr;
        uint32_t src = *tmp_src;

        bit_byte_pool.bit_buf = src & (uint32_t)bit_mask;
        dst_ptr[0] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[2] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[4] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[6] = (uint64_t)bit_byte_pool.byte_buf[3];
        bit_byte_pool.bit_buf = (src >> 4u) & (uint32_t)bit_mask;
        dst_ptr[1] = (uint64_t)bit_byte_pool.byte_buf[0];
        dst_ptr[3] = (uint64_t)bit_byte_pool.byte_buf[1];
        dst_ptr[5] = (uint64_t)bit_byte_pool.byte_buf[2];
        dst_ptr[7] = (uint64_t)bit_byte_pool.byte_buf[3];

        src_ptr += sizeof(uint32_t);
        dst_ptr += 8u;
        values_to_read -= 8u;
    }

    uint8_t src;

    if (values_to_read > 0) {
        src = *src_ptr;
        i = 0u;
        while (values_to_read >= 2u) {
            src = *src_ptr;
            dst_ptr[i] = (uint64_t)(src & mask);
            src = src >> 4u;
            dst_ptr[i + 1] = (uint64_t)(src & mask);
            ++src_ptr;

            values_to_read -= 2u;
            i += 2u;
        }
    }

    if (values_to_read > 0) {
        src = *src_ptr;
        dst_ptr[i] = (uint64_t)(src & mask);
    }
    return src_ptr;
}

// ********************** 5u ****************************** //
template <typename OutType>
const uint8_t* unpack_5u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_5u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    uint64_t bit_mask0 = 0x00001f000000001fLLU;
    uint64_t bit_mask1 = 0x000000001f000000LLU;
    uint64_t bit_mask2 = 0x001f000000001f00LLU;
    uint64_t bit_mask3 = 0x0000001f00000000LLU;
    uint64_t bit_mask4 = 0x1f000000001f0000LLU;
    uint16_t mask = OWN_5_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x1F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_5u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_5u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_5u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_5u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 8u * 5u;
            dst_ptr += 64u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + sizeof(uint64_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        uint16_t src16 = *tmp_src16;
        src_ptr = src_ptr + sizeof(uint16_t);

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = bit_byte_pool64.byte_buf[0];
        dst_ptr[8] = bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[5] = bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[2] = bit_byte_pool64.byte_buf[1];
        dst_ptr[10] = bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask3;
        dst_ptr[7] = bit_byte_pool64.byte_buf[4];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask4;
        dst_ptr[4] = bit_byte_pool64.byte_buf[2];
        dst_ptr[12] = bit_byte_pool64.byte_buf[7] | (((uint8_t)(src16 & 1u)) << 4u);
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask0;
        dst_ptr[1] = bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask1;
        dst_ptr[6] = bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask2;
        dst_ptr[3] = bit_byte_pool64.byte_buf[1];
        dst_ptr[11] = bit_byte_pool64.byte_buf[6];
        dst_ptr[13] = (uint8_t)((src16 >> 1u) & mask);
        dst_ptr[14] = (uint8_t)((src16 >> 6u) & mask);
        dst_ptr[15] = (uint8_t)((src16 >> 11u) & mask);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr += sizeof(uint32_t);
        uint8_t src8 = *src_ptr;
        src_ptr++;

        dst_ptr[0] = (uint8_t)(src32 & (uint32_t)mask);
        dst_ptr[1] = (uint8_t)((src32 >> 5u) & (uint32_t)mask);
        dst_ptr[2] = (uint8_t)((src32 >> 10u) & (uint32_t)mask);
        dst_ptr[3] = (uint8_t)((src32 >> 15u) & (uint32_t)mask);
        dst_ptr[4] = (uint8_t)((src32 >> 20u) & (uint32_t)mask);
        dst_ptr[5] = (uint8_t)((src32 >> 25u) & (uint32_t)mask);
        dst_ptr[6] = (uint8_t)((src32 >> 30u) & (uint32_t)mask) | ((src8 << 2u) & (uint8_t)mask);
        dst_ptr[7] = ((src8 >> 3u) & (uint8_t)mask);

        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (5u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint8_t)(src & mask);
            src = src >> 5u;
            bits_in_buf -= 5u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_5u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    uint64_t bit_mask0 = 0x00001f000000001fLLU;
    uint64_t bit_mask1 = 0x000000001f000000LLU;
    uint64_t bit_mask2 = 0x001f000000001f00LLU;
    uint64_t bit_mask3 = 0x0000001f00000000LLU;
    uint64_t bit_mask4 = 0x1f000000001f0000LLU;
    uint16_t mask = OWN_5_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x1F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_5u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_5u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_5u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_5u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u16u(zmm[0], dst_ptr);

            src_ptr += 8u * 5u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + sizeof(uint64_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        uint16_t src16 = *tmp_src16;
        src_ptr = src_ptr + sizeof(uint16_t);

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint16_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[8] = (uint16_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[5] = (uint16_t)bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[2] = (uint16_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[10] = (uint16_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask3;
        dst_ptr[7] = (uint16_t)bit_byte_pool64.byte_buf[4];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask4;
        dst_ptr[4] = (uint16_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[12] = (uint16_t)(bit_byte_pool64.byte_buf[7] | (((uint8_t)(src16 & 1u)) << 4u));
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask0;
        dst_ptr[1] = (uint16_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = (uint16_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask1;
        dst_ptr[6] = (uint16_t)bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask2;
        dst_ptr[3] = (uint16_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[11] = (uint16_t)bit_byte_pool64.byte_buf[6];
        dst_ptr[13] = (uint16_t)((src16 >> 1u) & mask);
        dst_ptr[14] = (uint16_t)((src16 >> 6u) & mask);
        dst_ptr[15] = (uint16_t)((src16 >> 11u) & mask);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr += sizeof(uint32_t);
        uint8_t src8 = *src_ptr;
        src_ptr++;

        dst_ptr[0] = (uint16_t)(src32 & (uint32_t)mask);
        dst_ptr[1] = (uint16_t)((src32 >> 5u) & (uint32_t)mask);
        dst_ptr[2] = (uint16_t)((src32 >> 10u) & (uint32_t)mask);
        dst_ptr[3] = (uint16_t)((src32 >> 15u) & (uint32_t)mask);
        dst_ptr[4] = (uint16_t)((src32 >> 20u) & (uint32_t)mask);
        dst_ptr[5] = (uint16_t)((src32 >> 25u) & (uint32_t)mask);
        dst_ptr[6] = (uint16_t)((src32 >> 30u) & (uint32_t)mask) | ((src8 << 2u) & (uint8_t)mask);
        dst_ptr[7] = (uint16_t)((src8 >> 3u) & (uint8_t)mask);

        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (5u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint16_t)(src & mask);
            src = src >> 5u;
            bits_in_buf -= 5u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_5u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    uint64_t bit_mask0 = 0x00001f000000001fLLU;
    uint64_t bit_mask1 = 0x000000001f000000LLU;
    uint64_t bit_mask2 = 0x001f000000001f00LLU;
    uint64_t bit_mask3 = 0x0000001f00000000LLU;
    uint64_t bit_mask4 = 0x1f000000001f0000LLU;
    uint16_t mask = OWN_5_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x1F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_5u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_5u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_5u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_5u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u32u(zmm[0], dst_ptr);

            src_ptr += 8u * 5u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + sizeof(uint64_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        uint16_t src16 = *tmp_src16;
        src_ptr = src_ptr + sizeof(uint16_t);

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint32_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[8] = (uint32_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[5] = (uint32_t)bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[2] = (uint32_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[10] = (uint32_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask3;
        dst_ptr[7] = (uint32_t)bit_byte_pool64.byte_buf[4];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask4;
        dst_ptr[4] = (uint32_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[12] = (uint32_t)(bit_byte_pool64.byte_buf[7] | (((uint8_t)(src16 & 1u)) << 4u));
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask0;
        dst_ptr[1] = (uint32_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = (uint32_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask1;
        dst_ptr[6] = (uint32_t)bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask2;
        dst_ptr[3] = (uint32_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[11] = (uint32_t)bit_byte_pool64.byte_buf[6];
        dst_ptr[13] = (uint32_t)((src16 >> 1u) & mask);
        dst_ptr[14] = (uint32_t)((src16 >> 6u) & mask);
        dst_ptr[15] = (uint32_t)((src16 >> 11u) & mask);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr += sizeof(uint32_t);
        uint8_t src8 = *src_ptr;
        src_ptr++;

        dst_ptr[0] = (uint32_t)(src32 & (uint32_t)mask);
        dst_ptr[1] = (uint32_t)((src32 >> 5u) & (uint32_t)mask);
        dst_ptr[2] = (uint32_t)((src32 >> 10u) & (uint32_t)mask);
        dst_ptr[3] = (uint32_t)((src32 >> 15u) & (uint32_t)mask);
        dst_ptr[4] = (uint32_t)((src32 >> 20u) & (uint32_t)mask);
        dst_ptr[5] = (uint32_t)((src32 >> 25u) & (uint32_t)mask);
        dst_ptr[6] = (uint32_t)((src32 >> 30u) & (uint32_t)mask) | ((src8 << 2u) & (uint8_t)mask);
        dst_ptr[7] = (uint32_t)((src8 >> 3u) & (uint8_t)mask);

        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (5u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint32_t)(src & mask);
            src = src >> 5u;
            bits_in_buf -= 5u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_5u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    uint64_t bit_mask0 = 0x00001f000000001fLLU;
    uint64_t bit_mask1 = 0x000000001f000000LLU;
    uint64_t bit_mask2 = 0x001f000000001f00LLU;
    uint64_t bit_mask3 = 0x0000001f00000000LLU;
    uint64_t bit_mask4 = 0x1f000000001f0000LLU;
    uint16_t mask = OWN_5_BIT_MASK;

    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x1F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_5u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_5u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_5u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_5u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u64u(zmm[0], dst_ptr);

            src_ptr += 8u * 5u;
            values_to_read -= 64u;
        }
    }
    if (values_to_read > 16u) {
        bit_byte_pool64_t bit_byte_pool64;
        uint64_t* tmp_src64 = (uint64_t*)src_ptr;
        uint64_t src64 = *tmp_src64;
        src_ptr = src_ptr + sizeof(uint64_t);
        uint16_t* tmp_src16 = (uint16_t*)src_ptr;
        uint16_t src16 = *tmp_src16;
        src_ptr = src_ptr + sizeof(uint16_t);

        bit_byte_pool64.bit_buf = src64 & bit_mask0;
        dst_ptr[0] = (uint64_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[8] = (uint64_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 1u) & bit_mask1;
        dst_ptr[5] = (uint64_t)bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 2u) & bit_mask2;
        dst_ptr[2] = (uint64_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[10] = (uint64_t)bit_byte_pool64.byte_buf[6];
        bit_byte_pool64.bit_buf = (src64 >> 3u) & bit_mask3;
        dst_ptr[7] = (uint64_t)bit_byte_pool64.byte_buf[4];
        bit_byte_pool64.bit_buf = (src64 >> 4u) & bit_mask4;
        dst_ptr[4] = (uint64_t)bit_byte_pool64.byte_buf[2];
        dst_ptr[12] = (uint64_t)(bit_byte_pool64.byte_buf[7] | (((uint8_t)(src16 & 1u)) << 4u));
        bit_byte_pool64.bit_buf = (src64 >> 5u) & bit_mask0;
        dst_ptr[1] = (uint64_t)bit_byte_pool64.byte_buf[0];
        dst_ptr[9] = (uint64_t)bit_byte_pool64.byte_buf[5];
        bit_byte_pool64.bit_buf = (src64 >> 6u) & bit_mask1;
        dst_ptr[6] = (uint64_t)bit_byte_pool64.byte_buf[3];
        bit_byte_pool64.bit_buf = (src64 >> 7u) & bit_mask2;
        dst_ptr[3] = (uint64_t)bit_byte_pool64.byte_buf[1];
        dst_ptr[11] = (uint64_t)bit_byte_pool64.byte_buf[6];
        dst_ptr[13] = (uint64_t)((src16 >> 1u) & mask);
        dst_ptr[14] = (uint64_t)((src16 >> 6u) & mask);
        dst_ptr[15] = (uint64_t)((src16 >> 11u) & mask);
        dst_ptr += 16u;
        values_to_read -= 16u;
    }
    if (values_to_read > 8u) {
        uint32_t* tmp_src32 = (uint32_t*)src_ptr;
        uint32_t src32 = (*tmp_src32);
        src_ptr += sizeof(uint32_t);
        uint8_t src8 = *src_ptr;
        src_ptr++;

        dst_ptr[0] = (uint64_t)(src32 & (uint32_t)mask);
        dst_ptr[1] = (uint64_t)((src32 >> 5u) & (uint32_t)mask);
        dst_ptr[2] = (uint64_t)((src32 >> 10u) & (uint32_t)mask);
        dst_ptr[3] = (uint64_t)((src32 >> 15u) & (uint32_t)mask);
        dst_ptr[4] = (uint64_t)((src32 >> 20u) & (uint32_t)mask);
        dst_ptr[5] = (uint64_t)((src32 >> 25u) & (uint32_t)mask);
        dst_ptr[6] = (uint64_t)((src32 >> 30u) & (uint32_t)mask) | ((src8 << 2u) & (uint8_t)mask);
        dst_ptr[7] = (uint64_t)((src8 >> 3u) & (uint8_t)mask);

        dst_ptr += 8u;
        values_to_read -= 8u;
    }
    if (0u < values_to_read) {
        uint16_t next_byte;
        uint32_t bits_in_buf = OWN_BYTE_WIDTH;
        uint16_t src = (uint16_t)(*src_ptr);
        src_ptr++;
        while (0u != values_to_read) {
            if (5u > bits_in_buf) {
                next_byte = (uint16_t)(*src_ptr);
                src_ptr++;
                next_byte = next_byte << bits_in_buf;
                src = src | next_byte;
                bits_in_buf += OWN_BYTE_WIDTH;
            }
            *dst_ptr = (uint64_t)(src & mask);
            src = src >> 5u;
            bits_in_buf -= 5u;
            dst_ptr++;
            values_to_read--;
        }
    }
    return src_ptr;
}

// ********************** 6u ****************************** //
template <typename OutType>
const uint8_t* px_unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* px_unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(6u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (6u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint8_t)(src & mask);
        src = src >> 6u;
        bits_in_buf -= 6u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* px_unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(6u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (6u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint16_t)(src & mask);
        src = src >> 6u;
        bits_in_buf -= 6u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* px_unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(6u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (6u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint32_t)(src & mask);
        src = src >> 6u;
        bits_in_buf -= 6u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* px_unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(6u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (6u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint64_t)(src & mask);
        src = src >> 6u;
        bits_in_buf -= 6u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <typename OutType>
const uint8_t* unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x3F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_6u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_6u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_6u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_6u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 8u * 6u;
            dst_ptr += 64u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_6u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x3F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_6u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_6u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_6u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_6u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u16u(zmm[0], dst_ptr);

            src_ptr += 8u * 6u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_6u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x3F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_6u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_6u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_6u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_6u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u32u(zmm[0], dst_ptr);

            src_ptr += 8u * 6u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_6u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_6u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x3F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_6u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_6u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_6u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_6u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u64u(zmm[0], dst_ptr);

            src_ptr += 8u * 6u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_6u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}

// ********************** 7u ****************************** //
template <typename OutType>
const uint8_t* px_unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* px_unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(7u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (7u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint8_t)(src & mask);
        src = src >> 7u;
        bits_in_buf -= 7u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* px_unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(7u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (7u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint16_t)(src & mask);
        src = src >> 7u;
        bits_in_buf -= 7u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* px_unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(7u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (7u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint32_t)(src & mask);
        src = src >> 7u;
        bits_in_buf -= 7u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* px_unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    uint16_t mask = OWN_BIT_MASK(7u);
    uint16_t next_byte;
    uint32_t bits_in_buf = OWN_BYTE_WIDTH;
    uint16_t src = (uint16_t)(*src_ptr);
    src_ptr++;

    while (0u < values_to_read) {
        if (7u > bits_in_buf) {
            next_byte = (uint16_t)(*src_ptr);
            src_ptr++;
            next_byte = next_byte << bits_in_buf;
            src = src | next_byte;
            bits_in_buf += OWN_BYTE_WIDTH;
        }
        *dst_ptr = (uint64_t)(src & mask);
        src = src >> 7u;
        bits_in_buf -= 7u;
        dst_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <typename OutType>
const uint8_t* unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x7F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_7u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_7u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_7u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_7u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            _mm512_storeu_si512(dst_ptr, zmm[0]);

            src_ptr += 8u * 7u;
            dst_ptr += 64u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_7u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x7F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_7u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_7u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_7u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_7u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u16u(zmm[0], dst_ptr);

            src_ptr += 8u * 7u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_7u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}
template <>
inline const uint8_t* unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x7F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_7u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_7u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_7u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_7u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u32u(zmm[0], dst_ptr);

            src_ptr += 8u * 7u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_7u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_7u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 64u) {
        __mmask64 read_mask = 0xFFFFFFFFFFFFFF;
        __m512i parse_mask = _mm512_set1_epi8(0x7F);
        __m512i permutex_idx_ptr[2];
        permutex_idx_ptr[0] = _mm512_load_si512(p_permutex_masks_7u);
        permutex_idx_ptr[1] = _mm512_load_si512(p_permutex_masks_7u + 16u);

        __m512i shift_mask_ptr[2];
        shift_mask_ptr[0] = _mm512_load_si512(shift_table_7u_0);
        shift_mask_ptr[1] = _mm512_load_si512(shift_table_7u_1);

        while (values_to_read >= 64u) {
            __m512i srcmm0, srcmm1, zmm[2];

            srcmm0 = _mm512_maskz_loadu_epi8(read_mask, src_ptr);
            srcmm1 = _mm512_maskz_loadu_epi8(read_mask >> 1u, src_ptr + 1u);

            // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
            zmm[0] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[0], srcmm1);
            zmm[1] = _mm512_permutex2var_epi16(srcmm0, permutex_idx_ptr[1], srcmm1);

            // shifting elements so they start from the start of the word
            zmm[0] = _mm512_srlv_epi16(zmm[0], shift_mask_ptr[0]);
            zmm[1] = _mm512_sllv_epi16(zmm[1], shift_mask_ptr[1]);

            // gathering even and odd elements together
            zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
            zmm[0] = _mm512_and_si512(zmm[0], parse_mask);

            extend_8u64u(zmm[0], dst_ptr);

            src_ptr += 8u * 7u;
            values_to_read -= 64u;
        }
    }

    if (values_to_read > 0) {
        src_ptr = px_unpack_7u8u(src_ptr, values_to_read, dst_ptr);
    }
    return src_ptr;
}

template <typename OutType>
const uint8_t* unpack_8u8u(const uint8_t* src_ptr, uint64_t values_to_read, OutType* dst_ptr);

template <>
inline const uint8_t* unpack_8u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint8_t* dst_ptr) {
    memcpy(dst_ptr, src_ptr, values_to_read);
    return src_ptr;
}

template <>
inline const uint8_t* unpack_8u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint16_t* dst_ptr) {
    if (values_to_read >= 64u) {
        while (values_to_read >= 64u) {
            __m512i src = _mm512_loadu_si512(src_ptr);
            extend_8u16u(src, dst_ptr);
            values_to_read -= 64u;
            src_ptr += 64;
        }
    }
    while (values_to_read > 0) {
        *dst_ptr = (uint16_t)(*src_ptr);
        dst_ptr++;
        src_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_8u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint32_t* dst_ptr) {
    if (values_to_read >= 64u) {
        while (values_to_read >= 64u) {
            __m512i src = _mm512_loadu_si512(src_ptr);
            extend_8u32u(src, dst_ptr);
            values_to_read -= 64u;
            src_ptr += 64;
        }
    }
    while (values_to_read > 0) {
        //printf("value_to_read is %d\n",values_to_read);
        *dst_ptr = (uint32_t)(*src_ptr);
        dst_ptr++;
        src_ptr++;
        values_to_read--;
    }
    return src_ptr;
}

template <>
inline const uint8_t* unpack_8u8u(const uint8_t* src_ptr, uint64_t values_to_read, uint64_t* dst_ptr) {
    if (values_to_read >= 64u) {
        while (values_to_read >= 64u) {
            __m512i src = _mm512_loadu_si512(src_ptr);
            extend_8u64u(src, dst_ptr);
            values_to_read -= 64u;
            src_ptr += 64;
        }
    }
    while (values_to_read > 0) {
        *dst_ptr = (uint64_t)(*src_ptr);
        dst_ptr++;
        src_ptr++;
        values_to_read--;
    }
    return src_ptr;
}
