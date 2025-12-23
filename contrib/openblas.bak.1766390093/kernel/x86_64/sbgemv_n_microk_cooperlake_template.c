/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/
#include <immintrin.h>
#include "common.h"

// Include common macros for BF16 based operations with IA intrinsics
#include "bf16_common_macros.h"

#undef STORE16_COMPLETE_RESULT
#undef STORE16_MASK_COMPLETE_RESULT
#undef STORE8_COMPLETE_RESULT
#undef STORE8_MASK_COMPLETE_RESULT
#undef STORE4_COMPLETE_RESULT
#undef STORE4_MASK_COMPLETE_RESULT

#ifndef ZERO_BETA  // Beta is non-zero

#ifndef ONE_BETA       // BETA is not ONE

#define STORE16_COMPLETE_RESULT       STORE16_COMPLETE_RESULT_ALPHA_BETA
#define STORE16_MASK_COMPLETE_RESULT  STORE16_MASK_COMPLETE_RESULT_ALPHA_BETA
#define STORE8_COMPLETE_RESULT        STORE8_COMPLETE_RESULT_ALPHA_BETA
#define STORE8_MASK_COMPLETE_RESULT   STORE8_MASK_COMPLETE_RESULT_ALPHA_BETA
#define STORE4_COMPLETE_RESULT        STORE4_COMPLETE_RESULT_ALPHA_BETA
#define STORE4_MASK_COMPLETE_RESULT   STORE4_MASK_COMPLETE_RESULT_ALPHA_BETA

#else                  // BETA is ONE

#define STORE16_COMPLETE_RESULT       STORE16_COMPLETE_RESULT_ALPHA_ONE
#define STORE16_MASK_COMPLETE_RESULT  STORE16_MASK_COMPLETE_RESULT_ALPHA_ONE
#define STORE8_COMPLETE_RESULT        STORE8_COMPLETE_RESULT_ALPHA_ONE
#define STORE8_MASK_COMPLETE_RESULT   STORE8_MASK_COMPLETE_RESULT_ALPHA_ONE
#define STORE4_COMPLETE_RESULT        STORE4_COMPLETE_RESULT_ALPHA_ONE
#define STORE4_MASK_COMPLETE_RESULT   STORE4_MASK_COMPLETE_RESULT_ALPHA_ONE

#endif

#else  // BETA is zero

#ifndef ONE_ALPHA      // ALPHA is not ONE

#define STORE16_COMPLETE_RESULT       STORE16_COMPLETE_RESULT_ALPHA
#define STORE16_MASK_COMPLETE_RESULT  STORE16_MASK_COMPLETE_RESULT_ALPHA
#define STORE8_COMPLETE_RESULT        STORE8_COMPLETE_RESULT_ALPHA
#define STORE8_MASK_COMPLETE_RESULT   STORE8_MASK_COMPLETE_RESULT_ALPHA
#define STORE4_COMPLETE_RESULT        STORE4_COMPLETE_RESULT_ALPHA
#define STORE4_MASK_COMPLETE_RESULT   STORE4_MASK_COMPLETE_RESULT_ALPHA

#else                  // ALPHA is ONE

#define STORE16_COMPLETE_RESULT       STORE16_COMPLETE_RESULT_DIRECT
#define STORE16_MASK_COMPLETE_RESULT  STORE16_MASK_COMPLETE_RESULT_DIRECT
#define STORE8_COMPLETE_RESULT        STORE8_COMPLETE_RESULT_DIRECT
#define STORE8_MASK_COMPLETE_RESULT   STORE8_MASK_COMPLETE_RESULT_DIRECT
#define STORE4_COMPLETE_RESULT        STORE4_COMPLETE_RESULT_DIRECT
#define STORE4_MASK_COMPLETE_RESULT   STORE4_MASK_COMPLETE_RESULT_DIRECT

#endif

#endif



// 8 rows parallel processing BF16 GEMV kernel for big N && lda effective scenario (process before interleave)
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_32xN_lda_direct_alpha_beta(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_32xN_lda_direct_alpha_one(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_32xN_lda_direct_alpha(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_32xN_lda_direct(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_32x = m & (~31);
    BLASLONG tag_m_128x = m & (~127);

    __m512 accum512_0, accum512_1, accum512_2, accum512_3, accum512_4, accum512_5, accum512_6, accum512_7, \
           accum512_8, accum512_9, accum512_10, accum512_11, accum512_12, accum512_13, accum512_14, accum512_15;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    __m512i matrixArray_seed_0, matrixArray_seed_1, matrixArray_seed_2, matrixArray_seed_3;
    __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4, matrixArray_5, matrixArray_6, matrixArray_7;
    __m512i xArray_0;

    __m512i ZERO512 = _mm512_setzero_si512();

    unsigned int blend_hi_mask_value = ((unsigned int)0xaaaaaaaa);
    __mmask32 blend_hi_mask = *((__mmask32*) &blend_hi_mask_value);
    unsigned int blend_lo_mask_value = ((unsigned int)0x55555555);
    __mmask32 blend_lo_mask = *((__mmask32*) &blend_lo_mask_value);

    __m512i M512_EPI32_8 = _mm512_set1_epi32(8);
    __m512i idx_base_0   = _mm512_set_epi32(23,  7, 22,  6, 21,  5,  20,  4, 19,  3, 18,  2, 17,  1,  16,  0);
    __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_8);

    for (BLASLONG idx_m = 0; idx_m < tag_m_128x; idx_m+=128) {
        accum512_0 = _mm512_setzero_ps();
        accum512_1 = _mm512_setzero_ps();
        accum512_2 = _mm512_setzero_ps();
        accum512_3 = _mm512_setzero_ps();
        accum512_4 = _mm512_setzero_ps();
        accum512_5 = _mm512_setzero_ps();
        accum512_6 = _mm512_setzero_ps();
        accum512_7 = _mm512_setzero_ps();
 
        for (BLASLONG idx_n = 0; idx_n < n; idx_n++) {
            xArray_0 = _mm512_set1_epi16(x[idx_n]);
 
            BF16_MATRIX_LOAD_1x32(matrixArray_seed_0, a, lda, idx_n, idx_m +  0)
            BF16_MATRIX_LOAD_1x32(matrixArray_seed_1, a, lda, idx_n, idx_m + 32)
            BF16_MATRIX_LOAD_1x32(matrixArray_seed_2, a, lda, idx_n, idx_m + 64)
            BF16_MATRIX_LOAD_1x32(matrixArray_seed_3, a, lda, idx_n, idx_m + 96)

            matrixArray_0 = _mm512_mask_blend_epi16(blend_lo_mask, ZERO512, matrixArray_seed_0);
            matrixArray_1 = _mm512_mask_blend_epi16(blend_hi_mask, ZERO512, matrixArray_seed_0);
            matrixArray_2 = _mm512_mask_blend_epi16(blend_lo_mask, ZERO512, matrixArray_seed_1);
            matrixArray_3 = _mm512_mask_blend_epi16(blend_hi_mask, ZERO512, matrixArray_seed_1);
            matrixArray_4 = _mm512_mask_blend_epi16(blend_lo_mask, ZERO512, matrixArray_seed_2);
            matrixArray_5 = _mm512_mask_blend_epi16(blend_hi_mask, ZERO512, matrixArray_seed_2);
            matrixArray_6 = _mm512_mask_blend_epi16(blend_lo_mask, ZERO512, matrixArray_seed_3);
            matrixArray_7 = _mm512_mask_blend_epi16(blend_hi_mask, ZERO512, matrixArray_seed_3);

            BF16_DOT_1x32(accum512_0, matrixArray_0, xArray_0)
            BF16_DOT_1x32(accum512_1, matrixArray_1, xArray_0)
            BF16_DOT_1x32(accum512_2, matrixArray_2, xArray_0)
            BF16_DOT_1x32(accum512_3, matrixArray_3, xArray_0)
            BF16_DOT_1x32(accum512_4, matrixArray_4, xArray_0)
            BF16_DOT_1x32(accum512_5, matrixArray_5, xArray_0)
            BF16_DOT_1x32(accum512_6, matrixArray_6, xArray_0)
            BF16_DOT_1x32(accum512_7, matrixArray_7, xArray_0)
        }
        accum512_8  = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
        accum512_9  = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);
        accum512_10 = _mm512_permutex2var_ps(accum512_2, idx_base_0, accum512_3);
        accum512_11 = _mm512_permutex2var_ps(accum512_2, idx_base_1, accum512_3);
        accum512_12 = _mm512_permutex2var_ps(accum512_4, idx_base_0, accum512_5);
        accum512_13 = _mm512_permutex2var_ps(accum512_4, idx_base_1, accum512_5);
        accum512_14 = _mm512_permutex2var_ps(accum512_6, idx_base_0, accum512_7);
        accum512_15 = _mm512_permutex2var_ps(accum512_6, idx_base_1, accum512_7);

        STORE16_COMPLETE_RESULT(accum512_8,  y+idx_m+0)
        STORE16_COMPLETE_RESULT(accum512_9,  y+idx_m+16)
        STORE16_COMPLETE_RESULT(accum512_10, y+idx_m+32)
        STORE16_COMPLETE_RESULT(accum512_11, y+idx_m+48)
        STORE16_COMPLETE_RESULT(accum512_12, y+idx_m+64)
        STORE16_COMPLETE_RESULT(accum512_13, y+idx_m+80)
        STORE16_COMPLETE_RESULT(accum512_14, y+idx_m+96)
        STORE16_COMPLETE_RESULT(accum512_15, y+idx_m+112)
    }

    for (BLASLONG idx_m = tag_m_128x; idx_m < tag_m_32x; idx_m+=32) {
        accum512_0 = _mm512_setzero_ps();
        accum512_1 = _mm512_setzero_ps();
 
        for (BLASLONG idx_n = 0; idx_n < n; idx_n++) {
            xArray_0 = _mm512_set1_epi16(x[idx_n]);

            BF16_MATRIX_LOAD_1x32(matrixArray_seed_0, a, lda, idx_n, idx_m)

            matrixArray_0 = _mm512_mask_blend_epi16(blend_lo_mask, ZERO512, matrixArray_seed_0);
            matrixArray_1 = _mm512_mask_blend_epi16(blend_hi_mask, ZERO512, matrixArray_seed_0);

            BF16_DOT_1x32(accum512_0, matrixArray_0, xArray_0)
            BF16_DOT_1x32(accum512_1, matrixArray_1, xArray_0)
        }
        accum512_8  = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
        accum512_9  = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);

        STORE16_COMPLETE_RESULT(accum512_8, y+idx_m+0)
        STORE16_COMPLETE_RESULT(accum512_9, y+idx_m+16)
    }

    if (tag_m_32x != m) {
        unsigned int tail_mask_value = (((unsigned int)0xffffffff) >> (32-(m&31)));
        __mmask32 tail_mask = *((__mmask32*) &tail_mask_value);

        unsigned int store_tail_mask_value = (((unsigned int)0xffff) >> (16-(m&15)));
        __mmask32 store_tail_mask = *((__mmask32*) &store_tail_mask_value);

        accum512_0 = _mm512_setzero_ps();
        accum512_1 = _mm512_setzero_ps();
 
        for (BLASLONG idx_n = 0; idx_n < n; idx_n++) {
            xArray_0 = _mm512_set1_epi16(x[idx_n]);

            BF16_MATRIX_MASKZ_LOAD_1x32(matrixArray_seed_0, a, lda, idx_n, tag_m_32x, tail_mask)

            matrixArray_0 = _mm512_mask_blend_epi16(blend_lo_mask, ZERO512, matrixArray_seed_0);
            matrixArray_1 = _mm512_mask_blend_epi16(blend_hi_mask, ZERO512, matrixArray_seed_0);

            BF16_DOT_1x32(accum512_0, matrixArray_0, xArray_0)
            BF16_DOT_1x32(accum512_1, matrixArray_1, xArray_0)
        }
        accum512_8  = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
        accum512_9  = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);

        if ((m-tag_m_32x) >= 16) {
            STORE16_COMPLETE_RESULT(accum512_8, y+tag_m_32x+0)
            STORE16_MASK_COMPLETE_RESULT(accum512_9, y+tag_m_32x+16, store_tail_mask)
        } else {
            STORE16_MASK_COMPLETE_RESULT(accum512_8, y+tag_m_32x+0, store_tail_mask)
        }
    }

    return 0;
}
