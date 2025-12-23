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


// 32 rows parallel processing BF16 GEMV kernel for n=1 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_32x1_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_32x1_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_32x1_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_32x1(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_32x  = m & (~31);

    __m512i matrixArray_0, matrixArray_1, matrixArray_2;
    __m512i xArray;
    __m512  result_0, result_1;
#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    __m512i load_idx_lo   = _mm512_set_epi16(0, 15,  0, 14,  0, 13,  0, 12,  0, 11,  0, 10,  0,  9,  0,  8,\
                                             0,  7,  0,  6,  0,  5,  0,  4,  0,  3,  0,  2,  0,  1,  0,  0);
    __m512i M512_EPI16_16 = _mm512_set1_epi16(16);
    __m512i load_idx_hi   = _mm512_add_epi16(load_idx_lo, M512_EPI16_16);

    unsigned int interleve_mask_value = ((unsigned int) 0x55555555);
    __mmask32 interleave_mask = *((__mmask32*) &interleve_mask_value);

    xArray = _mm512_set1_epi16((short) x[0]);
    xArray = _mm512_mask_blend_epi16(interleave_mask, _mm512_setzero_si512(), xArray);

    if (tag_m_32x > 0) {
        for (BLASLONG idx_m = 0; idx_m < tag_m_32x; idx_m+=32) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(idx_m)]);  // Load 32 rows with n=1
            matrixArray_1 = _mm512_permutexvar_epi16(load_idx_lo, matrixArray_0);  // Expand the low 16 elements
            matrixArray_2 = _mm512_permutexvar_epi16(load_idx_hi, matrixArray_0);  // Expand the high 16 elements

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_1, (__m512bh) xArray);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_2, (__m512bh) xArray);

            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
            STORE16_COMPLETE_RESULT(result_1, y+idx_m+16)
        }
    }

    BLASLONG tail_num = m - tag_m_32x;
    if (tail_num > 16) {
        result_0 = _mm512_setzero_ps();
        result_1 = _mm512_setzero_ps();

        unsigned int tail_mask_value = (((unsigned int)0xffffffff) >> (32-tail_num));
        __mmask32 tail_mask = *((__mmask32*) &tail_mask_value);
        matrixArray_0 = _mm512_maskz_loadu_epi16(tail_mask, &a[(tag_m_32x)]);  // Load 32 rows with n=1
        matrixArray_1 = _mm512_permutexvar_epi16(load_idx_lo, matrixArray_0);  // Expand the low 16 elements
        matrixArray_2 = _mm512_permutexvar_epi16(load_idx_hi, matrixArray_0);  // Expand the high 16 elements

        result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_1, (__m512bh) xArray);
        result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_2, (__m512bh) xArray);

        unsigned short store_mask_value = (((unsigned short)0xffff) >> (32-tail_num));
        __mmask16 store_mask = *((__mmask16*) &store_mask_value);
        STORE16_COMPLETE_RESULT(result_0, y+tag_m_32x)
        STORE16_MASK_COMPLETE_RESULT(result_1, y+tag_m_32x+16, store_mask)
    } else if (tail_num > 8) {
        __m256 result256_0 = _mm256_setzero_ps();
        __m256 result256_1 = _mm256_setzero_ps();

        __m256i load_idx_lo256 = _mm512_castsi512_si256(load_idx_lo);
        __m256i load_idx_hi256 = _mm512_extracti32x8_epi32(load_idx_lo, 0x1);
        __m256i xArray256 = _mm512_castsi512_si256(xArray);

        unsigned short tail_mask_value = (((unsigned short)0xffff) >> (16-tail_num));
        __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
        __m256i matrixArray256_0 = _mm256_maskz_loadu_epi16(tail_mask, &a[(tag_m_32x)]);  // Load 16 rows with n=1
        __m256i matrixArray256_1 = _mm256_permutexvar_epi16(load_idx_lo256, matrixArray256_0);  // Expand the low 8 elements
        __m256i matrixArray256_2 = _mm256_permutexvar_epi16(load_idx_hi256, matrixArray256_0);  // Expand the high 8 elements

        result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_1, (__m256bh) xArray256);
        result256_1 = _mm256_dpbf16_ps(result256_1, (__m256bh) matrixArray256_2, (__m256bh) xArray256);

        unsigned char store_mask_value = (((unsigned char)0xff) >> (16-tail_num));
        __mmask8 store_mask = *((__mmask8*) &store_mask_value);
        STORE8_COMPLETE_RESULT(result256_0, y+tag_m_32x)
        STORE8_MASK_COMPLETE_RESULT(result256_1, y+tag_m_32x+8, store_mask)
    } else {
        __m128 result128_0 = _mm_setzero_ps();
        __m128 result128_1 = _mm_setzero_ps();

        __m128i load_idx_lo128 = _mm_set_epi16(0, 3, 0, 2, 0, 1, 0, 0);
        __m128i M128_EPI16_4   = _mm_set1_epi16(4);
        __m128i load_idx_hi128 = _mm_add_epi16(load_idx_lo128, M128_EPI16_4);

        __m128i xArray128 = _mm512_castsi512_si128(xArray);

        unsigned char tail_mask_value = (((unsigned char)0xff) >> (8-tail_num));
        __mmask8 tail_mask = *((__mmask8*) &tail_mask_value);
        __m128i matrixArray128_0 = _mm_maskz_loadu_epi16(tail_mask, &a[(tag_m_32x)]);  // Load 8 rows with n=1
        __m128i matrixArray128_1 = _mm_permutexvar_epi16(load_idx_lo128, matrixArray128_0);  // Expand the low 4 elements
        __m128i matrixArray128_2 = _mm_permutexvar_epi16(load_idx_hi128, matrixArray128_0);  // Expand the high 4 elements

        result128_0 = _mm_dpbf16_ps(result128_0, (__m128bh) matrixArray128_1, (__m128bh) xArray128);
        result128_1 = _mm_dpbf16_ps(result128_1, (__m128bh) matrixArray128_2, (__m128bh) xArray128);

        if (tail_num > 4) {
            unsigned char store_mask_value = (((unsigned char)0xf) >> (8-tail_num));
            __mmask8 store_mask = *((__mmask8*) &store_mask_value);
            STORE4_COMPLETE_RESULT(result128_0, y+tag_m_32x)
            STORE4_MASK_COMPLETE_RESULT(result128_1, y+tag_m_32x+4, store_mask)
        } else {
            unsigned char store_mask_value = (((unsigned char)0xf) >> (4-tail_num));
            __mmask8 store_mask = *((__mmask8*) &store_mask_value);
            STORE4_MASK_COMPLETE_RESULT(result128_0, y+tag_m_32x, store_mask)
        }
    }

    return 0;
}

// 32 rows parallel processing BF16 GEMV kernel for n=2 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_32x2_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_32x2_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_32x2_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_32x2(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_32x  = m & (~31);

    __m512i matrixArray_0, matrixArray_1;
    __m512i xArray;
    __m512  result_0, result_1;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    unsigned char load_mask_value = (((unsigned char)0xff) >> 6);
    __mmask8 load_mask = *((__mmask8*) &load_mask_value);
    xArray = _mm512_broadcastd_epi32(_mm_maskz_loadu_epi16(load_mask, x));

    if (tag_m_32x > 0) {
        for (BLASLONG idx_m = 0; idx_m < tag_m_32x; idx_m+=32) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(idx_m)*2]);     // Load 16 rows as n=2
            matrixArray_1 = _mm512_loadu_si512(&a[(idx_m+16)*2]);  // Load 16 rows as n=2

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_0, (__m512bh) xArray);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_1, (__m512bh) xArray);

            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
            STORE16_COMPLETE_RESULT(result_1, y+idx_m+16)
        }
    }

    if (m - tag_m_32x >= 16) {
        result_0 = _mm512_setzero_ps();

        matrixArray_0 = _mm512_loadu_si512(&a[(tag_m_32x)*2]);     // Load 16 rows with n=2

        result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_0, (__m512bh) xArray);

        STORE16_COMPLETE_RESULT(result_0, y+tag_m_32x)

        tag_m_32x += 16;
    }

    BLASLONG tail_num = m - tag_m_32x;
    if (tail_num > 8) {
        result_0 = _mm512_setzero_ps();

        unsigned short tail_mask_value = (((unsigned short)0xffff) >> (16-(m&15)));
        __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
        matrixArray_0 = _mm512_maskz_loadu_epi32(tail_mask, &a[(tag_m_32x)*2]);  // Load 16 rows with n=2

        result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_0, (__m512bh) xArray);

        STORE16_MASK_COMPLETE_RESULT(result_0, y+tag_m_32x, tail_mask)
    } else if (tail_num == 8) {
        __m256 result256 = _mm256_setzero_ps();

        __m256i matrixArray256 = _mm256_loadu_si256((__m256i *)&a[(tag_m_32x)*2]);     // Load 8 rows with n=2
        __m256i xArray256 = _mm512_castsi512_si256(xArray);
        result256 = _mm256_dpbf16_ps(result256, (__m256bh) matrixArray256, (__m256bh) xArray256);

        STORE8_COMPLETE_RESULT(result256, y+tag_m_32x)
    } else {
        __m256 result256 = _mm256_setzero_ps();

        unsigned char tail_mask_value = (((unsigned char)0xff) >> (8-(m&7)));
        __mmask8 tail_mask = *((__mmask8*) &tail_mask_value);
        __m256i matrixArray256 = _mm256_maskz_loadu_epi32(tail_mask, &a[(tag_m_32x)*2]);  // Load 8 rows with n=2
        __m256i xArray256 = _mm512_castsi512_si256(xArray);
        result256 = _mm256_dpbf16_ps(result256, (__m256bh) matrixArray256, (__m256bh) xArray256);

        STORE8_MASK_COMPLETE_RESULT(result256, y+tag_m_32x, tail_mask)
    }

    return 0;
}

// 32 rows parallel processing BF16 GEMV kernel for n=3 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_32x3_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_32x3_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_32x3_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_32x3(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_32x  = m & (~31);

    __m512  result_0, result_1;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    unsigned char x_load_mask_value = (((unsigned char)0xff) >> 5);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i xTmp = _mm_maskz_loadu_epi16(x_load_mask, x); // x0|x1|x2|0|0|0|0|0|
    __m512i xArray_0 = _mm512_broadcastd_epi32(xTmp);                          // x0|x1|x0|x1|...|x0|x1|
    __m512i xArray_1 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(xTmp, 0x1));  // x2| 0|x2| 0|...|x2| 0|

    __m512i load_idx_base;
    __m512i M512_EPI16_2, M512_EPI16_8, M512_EPI16_16;
    M512_EPI16_2  = _mm512_set1_epi16(2);
    M512_EPI16_8  = _mm512_add_epi16(M512_EPI16_2, M512_EPI16_2);
    M512_EPI16_8  = _mm512_add_epi16(M512_EPI16_8, M512_EPI16_8);
    M512_EPI16_16 = _mm512_add_epi16(M512_EPI16_8, M512_EPI16_8);
    load_idx_base = _mm512_set_epi16(46, 45, 43, 42, 40, 39, 37, 36, 34, 33, 31, 30, 28, 27, 25, 24,
                                     22, 21, 19, 18, 16, 15, 13, 12, 10,  9,  7,  6,  4,  3,  1,  0);

    if (tag_m_32x > 0) {
        __m512i load_idx01_1st, load_idx01_2nd, load_idx2_1st, load_idx2_2nd;
        __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4, matrixArray_5, matrixArray_6;

        unsigned int idx_blend_mask_value = ((unsigned int)0x80000000);
        __mmask32 idx_blend_mask = *((__mmask32*) &idx_blend_mask_value);

        load_idx01_1st = load_idx_base;
        load_idx01_2nd = _mm512_add_epi16(load_idx01_1st, M512_EPI16_16);
        load_idx2_1st  = _mm512_add_epi16(load_idx01_1st, M512_EPI16_2);
        load_idx2_2nd  = _mm512_add_epi16(load_idx01_2nd, M512_EPI16_2);
        load_idx2_2nd  = _mm512_mask_blend_epi16(idx_blend_mask, load_idx2_2nd, _mm512_setzero_si512());

        for (BLASLONG idx_m = 0; idx_m < tag_m_32x; idx_m+=32) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(idx_m)*3]);           // Load 10 rows with n=3 plus 2 element
            matrixArray_1 = _mm512_loadu_si512(&a[((idx_m+10)*3 + 2)]);  // Load 10 rows with n=3 plus 2 element
            matrixArray_2 = _mm512_loadu_si512(&a[((idx_m+21)*3 + 1)]);  // Load 10 rows with n=3 plus 2 element

            matrixArray_3 = _mm512_permutex2var_epi16(matrixArray_0, load_idx01_1st, matrixArray_1);  // Select the first 2 elements for each row
            matrixArray_4 = _mm512_permutex2var_epi16(matrixArray_1, load_idx01_2nd, matrixArray_2);  // Select the first 2 elements for each row
            matrixArray_5 = _mm512_permutex2var_epi16(matrixArray_0, load_idx2_1st,  matrixArray_1);  // Select the third element for each row
            matrixArray_6 = _mm512_permutex2var_epi16(matrixArray_1, load_idx2_2nd,  matrixArray_2);  // Select the third element for each row

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_3, (__m512bh) xArray_0);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_5, (__m512bh) xArray_1);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_4, (__m512bh) xArray_0);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_6, (__m512bh) xArray_1);

            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
            STORE16_COMPLETE_RESULT(result_1, y+idx_m+16)
        }
    }

    if (tag_m_32x != m) {
        __m256i load256_idx01_1st, load256_idx01_2nd, load256_idx2_1st, load256_idx2_2nd;
        __m256i matrixArray256_0, matrixArray256_1, matrixArray256_2, matrixArray256_3, matrixArray256_4, matrixArray256_5, matrixArray256_6;
        __m256 result256_0, result256_1;

        unsigned short idx256_blend_mask_value = ((unsigned short)0x8000);
        __mmask16 idx256_blend_mask = *((__mmask16*) &idx256_blend_mask_value);

        load256_idx01_1st = _mm512_castsi512_si256(load_idx_base);
        load256_idx01_2nd = _mm256_add_epi16(load256_idx01_1st, _mm512_castsi512_si256(M512_EPI16_8));
        load256_idx2_1st  = _mm256_add_epi16(load256_idx01_1st, _mm512_castsi512_si256(M512_EPI16_2)); 
        load256_idx2_2nd  = _mm256_add_epi16(load256_idx01_2nd, _mm512_castsi512_si256(M512_EPI16_2));
        load256_idx2_2nd  = _mm256_mask_blend_epi16(idx256_blend_mask, load256_idx2_2nd, _mm256_setzero_si256());

        if (m - tag_m_32x > 15) {
            result256_0 = _mm256_setzero_ps();
            result256_1 = _mm256_setzero_ps();

            matrixArray256_0 = _mm256_loadu_si256((__m256i *)&a[(tag_m_32x)*3]);       // Load 5 rows with n=3 plus 1 element
            matrixArray256_1 = _mm256_loadu_si256((__m256i *)&a[((tag_m_32x+5)*3 + 1)]);   // Load 5 rows with n=3 plus 1 element
            matrixArray256_2 = _mm256_loadu_si256((__m256i *)&a[((tag_m_32x+10)*3 + 2)]);  // Load 5 rows with n=3 plus 1 element

            matrixArray256_3 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx01_1st, matrixArray256_1);  // Select the first 2 elements for each row
            matrixArray256_4 = _mm256_permutex2var_epi16(matrixArray256_1, load256_idx01_2nd, matrixArray256_2);  // Select the first 2 elements for each row
            matrixArray256_5 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx2_1st,  matrixArray256_1);  // Select the third element for each row
            matrixArray256_6 = _mm256_permutex2var_epi16(matrixArray256_1, load256_idx2_2nd,  matrixArray256_2);  // Select the third element for each row

            result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_3, (__m256bh) _mm512_castsi512_si256(xArray_0));
            result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_5, (__m256bh) _mm512_castsi512_si256(xArray_1));
            result256_1 = _mm256_dpbf16_ps(result256_1, (__m256bh) matrixArray256_4, (__m256bh) _mm512_castsi512_si256(xArray_0));
            result256_1 = _mm256_dpbf16_ps(result256_1, (__m256bh) matrixArray256_6, (__m256bh) _mm512_castsi512_si256(xArray_1));

            STORE8_COMPLETE_RESULT(result256_0, y+tag_m_32x)
            STORE8_COMPLETE_RESULT(result256_1, y+tag_m_32x+8)

            tag_m_32x += 16;
        }

        if (tag_m_32x != m) {
            result256_0 = _mm256_setzero_ps();
            result256_1 = _mm256_setzero_ps();
            BLASLONG tail_num = m-tag_m_32x;

            if (tail_num > 10) {
                unsigned short tail_mask_value = (((unsigned short)0xffff) >> (16-((tail_num-10-1)*3+1)));
                __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
                matrixArray256_0 = _mm256_loadu_si256((__m256i *)&a[(tag_m_32x)*3]);       // Load 5 rows with n=3 plus 1 element
                matrixArray256_1 = _mm256_loadu_si256((__m256i *)&a[((tag_m_32x+5)*3 + 1)]);   // Load 5 rows with n=3 plus 1 element
                matrixArray256_2 = _mm256_maskz_loadu_epi16(tail_mask, &a[((tag_m_32x+10)*3 + 2)]);  // Load m-tag_m_32x-10 rows

                matrixArray256_3 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx01_1st, matrixArray256_1);  // Select the first 2 elements for each row
                matrixArray256_4 = _mm256_permutex2var_epi16(matrixArray256_1, load256_idx01_2nd, matrixArray256_2);  // Select the first 2 elements for each row
                matrixArray256_5 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx2_1st,  matrixArray256_1);  // Select the third element for each row
                matrixArray256_6 = _mm256_permutex2var_epi16(matrixArray256_1, load256_idx2_2nd,  matrixArray256_2);  // Select the third element for each row

                result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_3, (__m256bh) _mm512_castsi512_si256(xArray_0));
                result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_5, (__m256bh) _mm512_castsi512_si256(xArray_1));
                result256_1 = _mm256_dpbf16_ps(result256_1, (__m256bh) matrixArray256_4, (__m256bh) _mm512_castsi512_si256(xArray_0));
                result256_1 = _mm256_dpbf16_ps(result256_1, (__m256bh) matrixArray256_6, (__m256bh) _mm512_castsi512_si256(xArray_1));
            } else if (tail_num > 5) {
                unsigned short tail_mask_value = (((unsigned short)0xffff) >> (16-((tail_num-5-1)*3+2)));
                __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
                matrixArray256_0 = _mm256_loadu_si256((__m256i *)&a[(tag_m_32x)*3]);       // Load 5 rows with n=3 plus 1 element
                matrixArray256_1 = _mm256_maskz_loadu_epi16(tail_mask, &a[((tag_m_32x+5)*3+1)]);   // Load m-tag_m_32x-5 rows
                matrixArray256_2 = _mm256_setzero_si256();

                matrixArray256_3 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx01_1st, matrixArray256_1);  // Select the first 2 elements for each row
                matrixArray256_4 = _mm256_permutex2var_epi16(matrixArray256_1, load256_idx01_2nd, matrixArray256_2);  // Select the first 2 elements for each row
                matrixArray256_5 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx2_1st,  matrixArray256_1);  // Select the third element for each row
                matrixArray256_6 = _mm256_permutex2var_epi16(matrixArray256_1, load256_idx2_2nd,  matrixArray256_2);  // Select the third element for each row

                result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_3, (__m256bh) _mm512_castsi512_si256(xArray_0));
                result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_5, (__m256bh) _mm512_castsi512_si256(xArray_1));
                result256_1 = _mm256_dpbf16_ps(result256_1, (__m256bh) matrixArray256_4, (__m256bh) _mm512_castsi512_si256(xArray_0));
                result256_1 = _mm256_dpbf16_ps(result256_1, (__m256bh) matrixArray256_6, (__m256bh) _mm512_castsi512_si256(xArray_1));
            } else {
                unsigned short tail_mask_value = (((unsigned short)0xffff) >> (16-(tail_num*3)));
                __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
                matrixArray256_0 = _mm256_maskz_loadu_epi16(tail_mask, &a[(tag_m_32x)*3]);       // Load m-tag_m_32x rows
                matrixArray256_1 = _mm256_setzero_si256();

                matrixArray256_3 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx01_1st, matrixArray256_1);  // Select the first 2 elements for each row
                matrixArray256_5 = _mm256_permutex2var_epi16(matrixArray256_0, load256_idx2_1st,  matrixArray256_1);  // Select the third element for each row

                result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_3, (__m256bh) _mm512_castsi512_si256(xArray_0));
                result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray256_5, (__m256bh) _mm512_castsi512_si256(xArray_1));
            }

            unsigned short store_tail_mask_value = (((unsigned short)0xffff) >> (16-(tail_num)));
            __mmask16 store_tail_mask = *((__mmask16*) &store_tail_mask_value);
            __m512 result512 = _mm512_insertf32x8(_mm512_castps256_ps512(result256_0), result256_1, 0x1);
            STORE16_MASK_COMPLETE_RESULT(result512, y+tag_m_32x, store_tail_mask)
        }
    }

    return 0;
}

// 16 rows parallel processing BF16 GEMV kernel for n=4 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x4_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x4_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x4_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x4(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);
    __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3;
    __m512i xArray_01, xArray_23, xArray_remix;
    __m512  result;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    __m512i M512_EPI32_1 = _mm512_set1_epi32(1);
    __m512i idx_base_0 = _mm512_set_epi32(30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
    __m512i idx_base_1 = _mm512_add_epi32(idx_base_0, M512_EPI32_1);
    __m512i idx_base_remix = _mm512_inserti32x8(idx_base_0, _mm512_castsi512_si256(idx_base_1), 0x1);

    unsigned char x_load_mask_value = (((unsigned char)0xf) >> 2);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i xTmp = _mm_maskz_loadu_epi32(x_load_mask, x);               // |x0|x1|x2|x3|0|0|0|0|
    xArray_01 = _mm512_broadcastd_epi32(xTmp);                          // |x0|x1|x0|x1|...|x0|x1|
    xArray_23 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(xTmp, 0x1));  // |x2|x3|x2|x3|...|x2|x3|
    unsigned short blend_mask_value = ((unsigned short)0xff00);
    __mmask16 blend_mask = *((__mmask16*) &blend_mask_value);
    xArray_remix = _mm512_mask_blend_epi32(blend_mask, xArray_01, xArray_23); // |x0|x1|x0|x1|x0|x1|x0|x1|...|x2|x3|x2|x3|x2|x3|x2|x3| 

    if (tag_m_16x > 0) {
        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            result = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(idx_m)*4]);      // Load 8 rows with n=4
            matrixArray_1 = _mm512_loadu_si512(&a[(idx_m+8)*4]);    // Load 8 rows with n=4

            matrixArray_2 = _mm512_permutex2var_epi32(matrixArray_0, idx_base_0, matrixArray_1);  // |a0|a1|...|h0|h1|i0|i1|...|p0|p1|
            matrixArray_3 = _mm512_permutex2var_epi32(matrixArray_0, idx_base_1, matrixArray_1);  // |a2|a3|...|h2|h3|i2|i3|...|p2|p3|

            result = _mm512_dpbf16_ps(result, (__m512bh) matrixArray_2, (__m512bh) xArray_01);
            result = _mm512_dpbf16_ps(result, (__m512bh) matrixArray_3, (__m512bh) xArray_23);

            STORE16_COMPLETE_RESULT(result, y+idx_m)
        }
    }

    if (m - tag_m_16x > 7) {
        result = _mm512_setzero_ps();

        matrixArray_0 = _mm512_loadu_si512(&a[(tag_m_16x)*4]);                // Load 8 rows with n=4
        matrixArray_2 = _mm512_permutexvar_epi32(idx_base_remix, matrixArray_0);  // a0|a1|...|h0|h1|a2|a3|...|h2|h3|

        result = _mm512_dpbf16_ps(result, (__m512bh) matrixArray_2, (__m512bh) xArray_remix);
        __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(result), _mm512_extractf32x8_ps(result, 1));

        STORE8_COMPLETE_RESULT(result256, y+tag_m_16x)
        tag_m_16x += 8;
    }

    BLASLONG tail_num = m-tag_m_16x;
    if (tail_num != 0) {
        result = _mm512_setzero_ps();

        unsigned short tail_mask_value = (((unsigned short)0xffff) >> (16-tail_num*2));
        __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
        matrixArray_0 = _mm512_maskz_loadu_epi32(tail_mask, &a[(tag_m_16x)*4]);  // Load 8 rows with n=4
        matrixArray_2 = _mm512_permutexvar_epi32(idx_base_remix, matrixArray_0);  // a0|a1|...|h0|h1|a2|a3|...|h2|h3|

        result = _mm512_dpbf16_ps(result, (__m512bh) matrixArray_2, (__m512bh) xArray_remix);
        __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(result), _mm512_extractf32x8_ps(result, 1));

        unsigned char store_tail_mask_value = (((unsigned char)0xff) >> (8-tail_num));
        __mmask8 store_tail_mask = *((__mmask8*) &store_tail_mask_value);
        STORE8_MASK_COMPLETE_RESULT(result256, y+tag_m_16x, store_tail_mask)
    }

    return 0;
}

// 30 rows parallel processing BF16 GEMV kernel for n=5 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_30x5_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_30x5_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_30x5_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_30x5(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_30x = m - (m%30);

    unsigned char x_load_mask_value = (((unsigned char)0xff) >> 3);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i x128 = _mm_maskz_loadu_epi16(x_load_mask, x);                       // x0|x1|x2|x3|x4|0|0|0|

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    __m512  result_0, result_1;
    __m512i xArray_01 = _mm512_broadcastd_epi32(x128);                          // x0|x1|x0|x1|...|x0|x1|
    __m512i xArray_23 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x1));  // x2|x3|x2|x3|...|x2|x3|
    __m512i xArray_4  = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x2));  // x4| 0|x4| 0|...|x4| 0|

    __m512i M512_EPI16_2 = _mm512_set1_epi16(2);
    __m512i load_idx01_stage1_1st = _mm512_set_epi16( 0,  0,  0,  0,  0,  0,  0,  0, 58, 57, 53, 52, 48, 47, 43, 42, 
                                                        38, 37, 33, 32, 26, 25, 21, 20, 16, 15, 11, 10,  6,  5,  1,  0);
    __m512i load_idx01_stage1_2nd = _mm512_shuffle_i32x4(load_idx01_stage1_1st, load_idx01_stage1_1st, 0x39);
    __m512i load_idx01_stage1_3rd = _mm512_shuffle_i32x4(load_idx01_stage1_1st, load_idx01_stage1_1st, 0x4f);

    __m512i load_idx23_stage1_1st = _mm512_add_epi16(load_idx01_stage1_1st, M512_EPI16_2);
    __m512i load_idx23_stage1_2nd = _mm512_add_epi16(load_idx01_stage1_2nd, M512_EPI16_2);
    __m512i load_idx23_stage1_3rd = _mm512_add_epi16(load_idx01_stage1_3rd, M512_EPI16_2);

    __m512i load_idx4_stage1_1st  = _mm512_add_epi16(load_idx23_stage1_1st, M512_EPI16_2);
    __m512i load_idx4_stage1_2nd  = _mm512_add_epi16(load_idx23_stage1_2nd, M512_EPI16_2);
    __m512i load_idx4_stage1_3rd  = _mm512_add_epi16(load_idx23_stage1_3rd, M512_EPI16_2);

    __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4;
    __m512i matrixArray_stage1_0, matrixArray_stage1_1, matrixArray_stage1_2;
    __m512i matrixArray_stage2_0, matrixArray_stage2_1;

    unsigned int load_mask_value = (((unsigned int)0xffffffff) >> 2);
    __mmask32 load_mask = *((__mmask32*) &load_mask_value);
    unsigned short store_mask_value = (((unsigned short)0xffff) >> 2);
    __mmask16 store_mask = *((__mmask16*) &store_mask_value);

    if (tag_m_30x > 0) {
        unsigned short blend_mask_value_0 = ((unsigned short)0xf000);
        __mmask16 blend_mask_0 = *((__mmask16*) &blend_mask_value_0);
        unsigned short blend_mask_value_1 = ((unsigned short)0x3f00);
        __mmask16 blend_mask_1 = *((__mmask16*) &blend_mask_value_1);
        for (BLASLONG idx_m = 0; idx_m < tag_m_30x; idx_m+=30) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m)*5]);       // Load 6 rows with n=5
            matrixArray_1 = _mm512_maskz_loadu_epi16(load_mask, &a[((idx_m+6)*5)]);   // Load 6 rows with n=5
            matrixArray_2 = _mm512_maskz_loadu_epi16(load_mask, &a[((idx_m+12)*5)]);  // Load 6 rows with n=5
            matrixArray_3 = _mm512_maskz_loadu_epi16(load_mask, &a[((idx_m+18)*5)]);  // Load 6 rows with n=5
            matrixArray_4 = _mm512_maskz_loadu_epi16(load_mask, &a[((idx_m+24)*5)]);  // Load 6 rows with n=5

            // Process the 0|1 elements
            // Stage 1: Select the 0|1 elements for each row
            matrixArray_stage1_0 = _mm512_permutex2var_epi16(matrixArray_0, load_idx01_stage1_1st, matrixArray_1);
            matrixArray_stage1_1 = _mm512_permutex2var_epi16(matrixArray_2, load_idx01_stage1_2nd, matrixArray_3);
            matrixArray_stage1_2 = _mm512_permutexvar_epi16(load_idx01_stage1_3rd, matrixArray_4);
            // Stage 2: Reorder and compress all the 0|1 elements
            matrixArray_stage2_0 = _mm512_mask_blend_epi32(blend_mask_0, matrixArray_stage1_0, matrixArray_stage1_1);
            matrixArray_stage2_1 = _mm512_mask_blend_epi32(blend_mask_1, matrixArray_stage1_1, matrixArray_stage1_2);
            // Calculate the result of the 0|1 elements
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage2_0, (__m512bh) xArray_01);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_stage2_1, (__m512bh) xArray_01);

            // Process the 2|3 elements
            // Stage 1: Select the 2|3 elements for each row
            matrixArray_stage1_0 = _mm512_permutex2var_epi16(matrixArray_0, load_idx23_stage1_1st, matrixArray_1);
            matrixArray_stage1_1 = _mm512_permutex2var_epi16(matrixArray_2, load_idx23_stage1_2nd, matrixArray_3);
            matrixArray_stage1_2 = _mm512_permutexvar_epi16(load_idx23_stage1_3rd, matrixArray_4);
            // Stage 2: Reorder and compress all the 2|3 elements
            matrixArray_stage2_0 = _mm512_mask_blend_epi32(blend_mask_0, matrixArray_stage1_0, matrixArray_stage1_1);
            matrixArray_stage2_1 = _mm512_mask_blend_epi32(blend_mask_1, matrixArray_stage1_1, matrixArray_stage1_2);
            // Calculate the result of the 2|3 elements and accumulate the result of 0|1 elements
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage2_0, (__m512bh) xArray_23);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_stage2_1, (__m512bh) xArray_23);

            // Process the for 4 elements
            // Stage 1: Select the 4 elements for each row
            matrixArray_stage1_0 = _mm512_permutex2var_epi16(matrixArray_0, load_idx4_stage1_1st, matrixArray_1);
            matrixArray_stage1_1 = _mm512_permutex2var_epi16(matrixArray_2, load_idx4_stage1_2nd, matrixArray_3);
            matrixArray_stage1_2 = _mm512_permutexvar_epi16(load_idx4_stage1_3rd, matrixArray_4);
            // Stage 2: Reorder and compress all the 4 elements
            matrixArray_stage2_0 = _mm512_mask_blend_epi32(blend_mask_0, matrixArray_stage1_0, matrixArray_stage1_1);
            matrixArray_stage2_1 = _mm512_mask_blend_epi32(blend_mask_1, matrixArray_stage1_1, matrixArray_stage1_2);
            // Calculate the result of the 4 element and accumulate the result of 0|1 and 2|3 elements
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage2_0,  (__m512bh) xArray_4);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_stage2_1,  (__m512bh) xArray_4);

            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
            STORE16_MASK_COMPLETE_RESULT(result_1, y+idx_m+16, store_mask)
        }
    }

    if (m - tag_m_30x > 11) {
        BLASLONG tag_m_12x = m - ((m-tag_m_30x)%12);
        for (BLASLONG idx_m = tag_m_30x; idx_m < tag_m_12x; idx_m+=12) {
            unsigned short store_less_mask_value = (((unsigned short)0xffff) >> 4);
            __mmask16 store_less_mask = *((__mmask16*) &store_less_mask_value);
            result_0 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m)*5]);       // Load 6 rows with n=5
            matrixArray_1 = _mm512_maskz_loadu_epi16(load_mask, &a[((idx_m+6)*5)]);   // Load 6 rows with n=5

            // Interleave the elements
            matrixArray_stage1_0 = _mm512_permutex2var_epi16(matrixArray_0, load_idx01_stage1_1st, matrixArray_1);
            matrixArray_stage1_1 = _mm512_permutex2var_epi16(matrixArray_0, load_idx23_stage1_1st, matrixArray_1);
            matrixArray_stage1_2 = _mm512_permutex2var_epi16(matrixArray_0, load_idx4_stage1_1st, matrixArray_1);
            // Calculate and accumulate the result
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage1_0, (__m512bh) xArray_01);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage1_1, (__m512bh) xArray_23);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage1_2, (__m512bh) xArray_4);

            STORE16_MASK_COMPLETE_RESULT(result_0, y+idx_m, store_less_mask)
            tag_m_30x += 12;
        }
    }

    BLASLONG tail_num = m - tag_m_30x;
    if (tail_num > 6) {
        unsigned short store_less_mask_value = (((unsigned short)0xffff) >> (4+(12-tail_num)));
        __mmask16 store_less_mask = *((__mmask16*) &store_less_mask_value);
        unsigned int load_less_mask_value = (((unsigned int)0xffffffff) >> (2+(12-tail_num)*5));
        __mmask32 load_less_mask = *((__mmask32*) &load_less_mask_value);
        result_0 = _mm512_setzero_ps();

        matrixArray_0 = _mm512_maskz_loadu_epi16(load_mask, &a[(tag_m_30x)*5]);           // Load 6 rows with n=5
        matrixArray_1 = _mm512_maskz_loadu_epi16(load_less_mask, &a[((tag_m_30x+6)*5)]);  // Load x rows with n=5

        // Interleave the elements
        matrixArray_stage1_0 = _mm512_permutex2var_epi16(matrixArray_0, load_idx01_stage1_1st, matrixArray_1);
        matrixArray_stage1_1 = _mm512_permutex2var_epi16(matrixArray_0, load_idx23_stage1_1st, matrixArray_1);
        matrixArray_stage1_2 = _mm512_permutex2var_epi16(matrixArray_0, load_idx4_stage1_1st, matrixArray_1);
        // Calculate and accumulate the result
        result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage1_0, (__m512bh) xArray_01);
        result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage1_1, (__m512bh) xArray_23);
        result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage1_2, (__m512bh) xArray_4);

        STORE16_MASK_COMPLETE_RESULT(result_0, y+tag_m_30x, store_less_mask)
    } else {
        __m128i matrixArray128;
        __m128  result128, tmp128;
        for (BLASLONG i = tag_m_30x; i < m; i++) {
            result128 = _mm_setzero_ps();
            matrixArray128 = _mm_maskz_loadu_epi16(x_load_mask, &a[(i)*5]);       // Load 1 rows with n=5
            result128 = _mm_dpbf16_ps(result128, (__m128bh) matrixArray128, (__m128bh) x128);
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif

        }
    }

    return 0;
}

// 16 rows parallel processing BF16 GEMV kernel for n=6 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x6_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x6_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x6_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x6(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);

    unsigned char x_load_mask_value = (((unsigned char)0xff) >> 2);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i x128 = _mm_maskz_loadu_epi16(x_load_mask, x);                       // x0|x1|x2|x3|x4|x5|0|0|

    if (tag_m_16x > 0) {
        __m512  result_0;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i M512_EPI32_1 = _mm512_set1_epi32(1);
        __m512i load_idx01_1st = _mm512_set_epi32( 0,  0,  0,  0,  0, 30, 27, 24, 21, 18, 15, 12,  9,  6,  3,  0);
        __m512i load_idx01_2nd = _mm512_set_epi32(13, 10,  7,  4,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0);

        __m512i load_idx23_1st = _mm512_add_epi32(load_idx01_1st, M512_EPI32_1);
        __m512i load_idx23_2nd = _mm512_add_epi32(load_idx01_2nd, M512_EPI32_1);

        __m512i load_idx45_1st = _mm512_add_epi32(load_idx23_1st, M512_EPI32_1);
        __m512i load_idx45_2nd = _mm512_add_epi32(load_idx23_2nd, M512_EPI32_1);

        unsigned short blend_mask_value = ((unsigned short)0x0400);
        __mmask16 blend_mask = *((__mmask16*) &blend_mask_value);
        // Set the 11th element to be 0 as invalid index for a 512 bit epi32 register
        load_idx45_1st = _mm512_mask_blend_epi32(blend_mask, load_idx45_1st, load_idx01_2nd);
        // Set the 11th element to be 0 as 0 is the correct index
        load_idx45_2nd = _mm512_mask_blend_epi32(blend_mask, load_idx45_2nd, load_idx01_2nd);

        __m512i xArray_01 = _mm512_broadcastd_epi32(x128);                          // x0|x1|x0|x1|...|x0|x1|
        __m512i xArray_23 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x1));  // x2|x3|x2|x3|...|x2|x3|
        __m512i xArray_45 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x2));  // x4|x5|x4|x5|...|x4|x5|

        unsigned short permute_mask01_uint = (((unsigned short)0xf800));
        __mmask16 permute_mask01 = *((__mmask16*) &permute_mask01_uint);
        unsigned short permute_mask45_uint = (((unsigned short)0xfc00));
        __mmask16 permute_mask45 = *((__mmask16*) &permute_mask45_uint);

        __m512i matrixArray_0, matrixArray_1, matrixArray_2;
        __m512i matrixArray_stage_0, matrixArray_stage_1, matrixArray_stage_2;
        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            result_0 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(idx_m)*6]);           // Load 5 rows with n=6 plus 2 element
            matrixArray_1 = _mm512_loadu_si512(&a[((idx_m+5)*6 + 2)]);   // Load 5 rows with n=6 plus 2 element
            matrixArray_2 = _mm512_loadu_si512(&a[((idx_m+10)*6 + 4)]);  // Load 5 rows with n=6 plus 2 element

            // Stage 1: interleave for the a..k elements
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx01_1st, matrixArray_1);
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx23_1st, matrixArray_1);
            matrixArray_stage_2 = _mm512_permutex2var_epi32(matrixArray_0, load_idx45_1st, matrixArray_1);

            // Stage 2: interleave for the l..p elements and remix together
            matrixArray_stage_0 = _mm512_mask_permutexvar_epi32(matrixArray_stage_0, permute_mask01, load_idx01_2nd, matrixArray_2);
            matrixArray_stage_1 = _mm512_mask_permutexvar_epi32(matrixArray_stage_1, permute_mask01, load_idx23_2nd, matrixArray_2);
            matrixArray_stage_2 = _mm512_mask_permutexvar_epi32(matrixArray_stage_2, permute_mask45, load_idx45_2nd, matrixArray_2);

            // Calculate the result of the 0|1 elements
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_0, (__m512bh) xArray_01);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_1, (__m512bh) xArray_23);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_2, (__m512bh) xArray_45);

            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
        }

        if (m - tag_m_16x > 7) {
            __m256i M256_EPI32_1 = _mm512_castsi512_si256(M512_EPI32_1);
            __m256i load_idx01_1st = _mm256_set_epi32( 0,  0, 15, 12,  9,  6,  3,  0);
            __m256i load_idx01_2nd = _mm256_set_epi32( 5,  2,  0,  0,  0,  0,  0,  0);

            __m256i load_idx23_1st = _mm256_add_epi32(load_idx01_1st, M256_EPI32_1);
            __m256i load_idx23_2nd = _mm256_add_epi32(load_idx01_2nd, M256_EPI32_1);
            unsigned char blend_mask_value = ((unsigned char)0x20);
            __mmask8 blend_mask = *((__mmask8*) &blend_mask_value);
            // Set the 6th element to be 0 as invalid index for a 512 bit epi32 register
            load_idx23_1st = _mm256_mask_blend_epi32(blend_mask, load_idx23_1st, load_idx01_2nd);
            // Set the 6th element to be 0 as 0 is the correct index
            load_idx23_2nd = _mm256_mask_blend_epi32(blend_mask, load_idx23_2nd, load_idx01_2nd);

            __m256i load_idx45_1st = _mm256_add_epi32(load_idx23_1st, M256_EPI32_1);
            __m256i load_idx45_2nd = _mm256_add_epi32(load_idx23_2nd, M256_EPI32_1);

            unsigned char permute_mask01_uint = (((unsigned char)0xc0));
            __mmask8 permute_mask01 = *((__mmask8*) &permute_mask01_uint);
            unsigned char permute_mask45_uint = (((unsigned char)0xe0));
            __mmask8 permute_mask45 = *((__mmask8*) &permute_mask45_uint);

            __m256i matrixArray_0, matrixArray_1, matrixArray_2;
            __m256i matrixArray_stage_0;
            __m256  result256_0;

            result256_0 = _mm256_setzero_ps();

            matrixArray_0 = _mm256_loadu_si256((__m256i *)&a[(tag_m_16x)*6]);          // Load 2 rows with n=6 plus 4 element
            matrixArray_1 = _mm256_loadu_si256((__m256i *)&a[((tag_m_16x+2)*6 + 4)]);  // Load 2 rows with n=6 plus 4 element
            matrixArray_2 = _mm256_loadu_si256((__m256i *)&a[((tag_m_16x+5)*6 + 2)]);  // Load 2 rows with n=6 plus 4 element

            // Process the 0|1 elements
            // Select the 0|1 elements for each row
            matrixArray_stage_0 = _mm256_permutex2var_epi32(matrixArray_0, load_idx01_1st, matrixArray_1);
            matrixArray_stage_0 = _mm256_mask_permutexvar_epi32(matrixArray_stage_0, permute_mask01, load_idx01_2nd, matrixArray_2);
            // Calculate the result of the 0|1 elements
            result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray_stage_0, (__m256bh) _mm512_castsi512_si256(xArray_01));

            // Process the 2|3 elements
            // Select the 2|3 elements for each row
            matrixArray_stage_0 = _mm256_permutex2var_epi32(matrixArray_0, load_idx23_1st, matrixArray_1);
            matrixArray_stage_0 = _mm256_mask_permutexvar_epi32(matrixArray_stage_0, permute_mask45, load_idx23_2nd, matrixArray_2);
            // Calculate the result of the 0|1 elements
            result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray_stage_0, (__m256bh) _mm512_castsi512_si256(xArray_23));

            // Process the for 4 elements
            // Select the 4|5 elements for each row
            matrixArray_stage_0 = _mm256_permutex2var_epi32(matrixArray_0, load_idx45_1st, matrixArray_1);
            matrixArray_stage_0 = _mm256_mask_permutexvar_epi32(matrixArray_stage_0, permute_mask45, load_idx45_2nd, matrixArray_2);
            // Calculate the result of the 0|1 elements
            result256_0 = _mm256_dpbf16_ps(result256_0, (__m256bh) matrixArray_stage_0, (__m256bh) _mm512_castsi512_si256(xArray_45));

            STORE8_COMPLETE_RESULT(result256_0, y+tag_m_16x)
            tag_m_16x += 8;
        }
    }

    if (tag_m_16x != m) {
        __m128i matrixArray128;
        __m128  result128, tmp128;
        for (BLASLONG i = tag_m_16x; i < m; i++) {
            result128 = _mm_setzero_ps();
            matrixArray128 = _mm_maskz_loadu_epi16(x_load_mask, &a[(i)*6]);       // Load 1 rows with n=6
            result128 = _mm_dpbf16_ps(result128, (__m128bh) matrixArray128, (__m128bh) x128);
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif
        }
    }

    return 0;
}

// 16 rows parallel processing BF16 GEMV kernel for n=7 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x7_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x7_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x7_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x7(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);

    unsigned char x_load_mask_value = (((unsigned char)0xff) >> 1);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i x128 = _mm_maskz_loadu_epi16(x_load_mask, x);               // |x0|x1|x2|x3|x4|x5|x6|0|

    if (tag_m_16x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3;
        __m512i matrixArray_stage_0, matrixArray_stage_1, matrixArray_stage_2, matrixArray_stage_3;
        __m512i xArray_0123, xArray_4567;
        __m512  result_0, result_1, result_2, result_3;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i M512_EPI32_2 = _mm512_set1_epi32(2);
        __m512i load_idx_stage1_0 = _mm512_set_epi16(31, 27, 26, 25, 24, 23, 22, 21, 31, 20, 19, 18, 17, 16, 15, 14,
                                                     31, 13, 12, 11, 10,  9,  8,  7, 31,  6,  5,  4,  3,  2,  1,  0);
        __m512i load_idx_stage2_0 = _mm512_set_epi32(29, 25, 21, 17, 13,  9,  5,  1, 28, 24, 20, 16, 12,  8,  4,  0);
        __m512i load_idx_stage2_1 = _mm512_add_epi32(load_idx_stage2_0, M512_EPI32_2);

        unsigned short x_blend_mask_value = ((unsigned short)0xff00);
        __mmask16 x_blend_mask = *((__mmask16*) &x_blend_mask_value);
        xArray_0123 = _mm512_mask_blend_epi32(x_blend_mask, _mm512_broadcastd_epi32(x128), \
                                                            _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x1)));
        xArray_4567 = _mm512_mask_blend_epi32(x_blend_mask, _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x2)), \
                                                            _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x3)));

        unsigned int load_mask_value = (((unsigned int)0xffffffff) >> 4);
        __mmask32 load_mask = *((__mmask32*) &load_mask_value);
        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m)*7]);      // Load 4 rows with n=7
            matrixArray_1 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+4)*7]);    // Load 4 rows with n=7
            matrixArray_2 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+8)*7]);    // Load 4 rows with n=7
            matrixArray_3 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+12)*7]);   // Load 4 rows with n=7

            // Stage 1: padding
            matrixArray_0 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_0);                        // |a0|a1|a2|a3|...|b6|b7|c0|c1|c2|c3|...|d6|d7|
            matrixArray_1 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_1);                        // |e0|e1|e2|e3|...|f6|f7|g0|g1|g2|g3|...|h6|h7|
            matrixArray_2 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_2);                        // |i0|i1|i2|i3|...|j6|j7|k0|k1|k2|k3|...|l6|l7|
            matrixArray_3 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_3);                        // |m0|m1|m2|m3|...|n6|n7|o0|o1|o2|o3|...|p6|p7|

            // Stage 2: interleave per 32 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_0, matrixArray_1);  // |a0|a1|...|h0|h1|a2|a3|...|h2|h3|
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_1, matrixArray_1);  // |a4|a5|...|h4|h5|a6|a7|...|h6|h7|
            matrixArray_stage_2 = _mm512_permutex2var_epi32(matrixArray_2, load_idx_stage2_0, matrixArray_3);  // |i0|i1|...|p0|p1|i2|i3|...|p2|p3|
            matrixArray_stage_3 = _mm512_permutex2var_epi32(matrixArray_2, load_idx_stage2_1, matrixArray_3);  // |i4|i5|...|p4|p5|i6|i7|...|p6|p7|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_0, (__m512bh) xArray_0123);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_stage_2, (__m512bh) xArray_0123);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_1, (__m512bh) xArray_4567);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_stage_3, (__m512bh) xArray_4567);

            // Stage 3: interleave per 256 bits
            result_2 = _mm512_shuffle_f32x4(result_0, result_1, 0x44);
            result_3 = _mm512_shuffle_f32x4(result_0, result_1, 0xee);

            result_2 = _mm512_add_ps(result_2, result_3);

            STORE16_COMPLETE_RESULT(result_2, y+idx_m)
        }

        if (m - tag_m_16x > 7) {
            result_0 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_maskz_loadu_epi16(load_mask, &a[(tag_m_16x)*7]);      // Load 4 rows with n=7
            matrixArray_1 = _mm512_maskz_loadu_epi16(load_mask, &a[(tag_m_16x+4)*7]);    // Load 4 rows with n=7

            // Stage 1: padding
            matrixArray_0 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_0);                        // |a0|a1|a2|a3|...|b6|b7|c0|c1|c2|c3|...|d6|d7|
            matrixArray_1 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_1);                        // |e0|e1|e2|e3|...|f6|f7|g0|g1|g2|g3|...|h6|h7|

            // Stage 2: interleave per 32 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_0, matrixArray_1);  // |a0|a1|b0|b1|...|h0|h1|a2|a3|b2|b3|...|h2|h3|
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_1, matrixArray_1);  // |a4|a5|b4|b5|...|h4|h5|a6|a7|b6|b7|...|h6|h7|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_0, (__m512bh) xArray_0123);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_1, (__m512bh) xArray_4567);

            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(result_0), _mm512_extractf32x8_ps(result_0, 0x1));

            STORE8_COMPLETE_RESULT(result256, y+tag_m_16x)

            tag_m_16x += 8;
        }

        BLASLONG tail_num = m - tag_m_16x;
        if (tail_num > 3) {
            result_0 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_maskz_loadu_epi16(load_mask, &a[(tag_m_16x)*7]);      // Load 4 rows with n=7
            unsigned int tail_load_mask_value = (((unsigned int)0xffffffff) >> (4+(8-tail_num)*7));
            __mmask32 tail_load_mask = *((__mmask32*) &tail_load_mask_value);
            matrixArray_1 = _mm512_maskz_loadu_epi16(tail_load_mask, &a[(tag_m_16x+4)*7]);    // Load 4 rows with n=7

            // Stage 1: padding
            matrixArray_0 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_0);                        // |a0|a1|a2|a3|...|b6|b7|c0|c1|c2|c3|...|d6|d7|
            matrixArray_1 = _mm512_permutexvar_epi16(load_idx_stage1_0, matrixArray_1);                        // |e0|e1|e2|e3|...|f6|f7|g0|g1|g2|g3|...|h6|h7|

            // Stage 2: interleave per 32 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_0, matrixArray_1);  // |a0|a1|b0|b1|...|h0|h1|a2|a3|b2|b3|...|h2|h3|
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_1, matrixArray_1);  // |a4|a5|b4|b5|...|h4|h5|a6|a7|b6|b7|...|h6|h7|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_0, (__m512bh) xArray_0123);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_1, (__m512bh) xArray_4567);

            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(result_0), _mm512_extractf32x8_ps(result_0, 0x1));

            unsigned char tail_mask_value = (((unsigned char)0xff) >> (8-tail_num));
            __mmask8 tail_mask = *((__mmask8*) &tail_mask_value);
            STORE8_MASK_COMPLETE_RESULT(result256, y+tag_m_16x, tail_mask)
            tag_m_16x = m;
        }
    }

    if (tag_m_16x != m) {
        __m128i matrixArray128;
        __m128  result128, tmp128;
        for (BLASLONG i = tag_m_16x; i < m; i++) {
            result128 = _mm_setzero_ps();
            matrixArray128 = _mm_maskz_loadu_epi16(x_load_mask, &a[(i)*7]);       // Load 1 rows with n=7
            result128 = _mm_dpbf16_ps(result128, (__m128bh) matrixArray128, (__m128bh) x128);
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif
        }
    }

    return 0;
}

// 16 rows parallel processing BF16 GEMV kernel for n=8 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x8_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x8_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x8_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x8(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);

    __m128i x128 = _mm_loadu_si128((__m128i *)x);               // |x0|x1|x2|x3|x4|x5|x6|x7|

    if (tag_m_16x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3;
        __m512i matrixArray_stage_0, matrixArray_stage_1, matrixArray_stage_2, matrixArray_stage_3;
        __m512i xArray_0123, xArray_4567;
        __m512  result_0, result_1, result_2, result_3;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i M512_EPI32_2 = _mm512_set1_epi32(2);
        __m512i load_idx_stage2_0 = _mm512_set_epi32(29, 25, 21, 17, 13,  9,  5,  1, 28, 24, 20, 16, 12,  8,  4,  0);
        __m512i load_idx_stage2_1 = _mm512_add_epi32(load_idx_stage2_0, M512_EPI32_2);

        unsigned short x_blend_mask_value = ((unsigned short)0xff00);
        __mmask16 x_blend_mask = *((__mmask16*) &x_blend_mask_value);
        xArray_0123 = _mm512_mask_blend_epi32(x_blend_mask, _mm512_broadcastd_epi32(x128), \
                                                            _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x1)));
        xArray_4567 = _mm512_mask_blend_epi32(x_blend_mask, _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x2)), \
                                                            _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128, 0x3)));

        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(idx_m)*8]);     // Load 4 rows with n=8
            matrixArray_1 = _mm512_loadu_si512(&a[(idx_m+4)*8]);   // Load 4 rows with n=8
            matrixArray_2 = _mm512_loadu_si512(&a[(idx_m+8)*8]);   // Load 4 rows with n=8
            matrixArray_3 = _mm512_loadu_si512(&a[(idx_m+12)*8]);  // Load 4 rows with n=8

            // Stage 1: interleave per 32 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_0, matrixArray_1);  // |a0|a1|...|h0|h1|a2|a3|...|h2|h3|
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_1, matrixArray_1);  // |a4|a5|...|h4|h5|a6|a7|...|h6|h7|
            matrixArray_stage_2 = _mm512_permutex2var_epi32(matrixArray_2, load_idx_stage2_0, matrixArray_3);  // |i0|i1|...|p0|p1|i2|i3|...|p2|p3|
            matrixArray_stage_3 = _mm512_permutex2var_epi32(matrixArray_2, load_idx_stage2_1, matrixArray_3);  // |i4|i5|...|p4|p5|i6|i7|...|p6|p7|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_0, (__m512bh) xArray_0123);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_stage_2, (__m512bh) xArray_0123);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_1, (__m512bh) xArray_4567);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_stage_3, (__m512bh) xArray_4567);

            // Stage 2: interleave per 256 bits
            result_2 = _mm512_shuffle_f32x4(result_0, result_1, 0x44);
            result_3 = _mm512_shuffle_f32x4(result_0, result_1, 0xee);

            result_2 = _mm512_add_ps(result_2, result_3);

            STORE16_COMPLETE_RESULT(result_2, y+idx_m)
        }

        if (m - tag_m_16x > 7) {
            result_0 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(tag_m_16x)*8]);      // Load 4 rows with n=8
            matrixArray_1 = _mm512_loadu_si512(&a[(tag_m_16x+4)*8]);    // Load 4 rows with n=8

            // Stage 1: interleave per 32 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_0, matrixArray_1);  // |a0|a1|b0|b1|...|h0|h1|a2|a3|b2|b3|...|h2|h3|
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_1, matrixArray_1);  // |a4|a5|b4|b5|...|h4|h5|a6|a7|b6|b7|...|h6|h7|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_0, (__m512bh) xArray_0123);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_1, (__m512bh) xArray_4567);

            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(result_0), _mm512_extractf32x8_ps(result_0, 0x1));

            STORE8_COMPLETE_RESULT(result256, y+tag_m_16x)
            tag_m_16x += 8;
        }

        BLASLONG tail_num = m - tag_m_16x;
        if (tail_num > 3) {
            result_0 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(tag_m_16x)*8]);      // Load 4 rows with n=8
            unsigned short tail_load_mask_value = (((unsigned int)0xffff) >> ((8-tail_num)*4));
            __mmask16 tail_load_mask = *((__mmask16*) &tail_load_mask_value);
            matrixArray_1 = _mm512_maskz_loadu_epi32(tail_load_mask, &a[(tag_m_16x+4)*8]);    // Load 4 rows with n=8

            // Stage 1: interleave per 32 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_0, matrixArray_1);  // |a0|a1|b0|b1|...|h0|h1|a2|a3|b2|b3|...|h2|h3|
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage2_1, matrixArray_1);  // |a4|a5|b4|b5|...|h4|h5|a6|a7|b6|b7|...|h6|h7|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_0, (__m512bh) xArray_0123);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_stage_1, (__m512bh) xArray_4567);

            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(result_0), _mm512_extractf32x8_ps(result_0, 0x1));

            unsigned char tail_mask_value = (((unsigned char)0xff) >> (8-tail_num));
            __mmask8 tail_mask = *((__mmask8*) &tail_mask_value);
            STORE8_MASK_COMPLETE_RESULT(result256, y+tag_m_16x, tail_mask)
            tag_m_16x = m;
        }
    }

    if (tag_m_16x != m) {
        __m128i matrixArray128;
        __m128  result128, tmp128;
        for (BLASLONG i = tag_m_16x; i < m; i++) {
            result128 = _mm_setzero_ps();
            matrixArray128 = _mm_loadu_si128((__m128i *)&a[(i)*8]);       // Load 1 rows with n=8
            result128 = _mm_dpbf16_ps(result128, (__m128bh) matrixArray128, (__m128bh) x128);
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif
        }
    }

    return 0;
}

// 14 rows parallel processing BF16 GEMV kernel for n=9 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_14x9_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_14x9_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_14x9_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_14x9(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_14x = m - (m%14);

    unsigned char x_load_mask_value = (((unsigned char)0xff) >> 7);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i x128_0 = _mm_loadu_si128((__m128i *)x);                         // |x0|x1|x2|x3|x4|x5|x6|x7|
    __m128i x128_1 = _mm_maskz_loadu_epi16(x_load_mask, (x+8));  // |x8|0 |0 | 0| 0| 0| 0| 0|

    if (tag_m_14x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4, matrixArray_5;
        __m512i matrixArray_stage_0, matrixArray_stage_1, matrixArray_stage_2, matrixArray_stage_3;
        __m512i xArray_01, xArray_23, xArray_45, xArray_67, xArray_89;
        __m512  result_0, result_1;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m256i M256_EPI16_2 = _mm256_set1_epi16(2);
        __m256i idx_base_0 = _mm256_set_epi16( 0,  0, 55, 54, 46, 45, 37, 36, 28, 27, 19, 18, 10,  9,  1,  0);
        __m256i idx_base_1 = _mm256_add_epi16(idx_base_0, M256_EPI16_2);
        __m256i idx_base_2 = _mm256_add_epi16(idx_base_1, M256_EPI16_2);
        __m256i idx_base_3 = _mm256_add_epi16(idx_base_2, M256_EPI16_2);
        __m256i idx_base_4 = _mm256_add_epi16(idx_base_3, M256_EPI16_2);
        __m512i idx_idx    = _mm512_set_epi32( 0,  0, 22, 21, 20, 19, 18, 17, 16,  6,  5,  4,  3,  2,  1,  0);

        __m512i load_idx_stage1_0 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_0), idx_idx, _mm512_castsi256_si512(idx_base_1));
        __m512i load_idx_stage1_1 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_2), idx_idx, _mm512_castsi256_si512(idx_base_3));
        __m512i load_idx_stage1_2 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_1), idx_idx, _mm512_castsi256_si512(idx_base_0));
        __m512i load_idx_stage1_3 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_3), idx_idx, _mm512_castsi256_si512(idx_base_2));
        __m512i load_idx_stage1_4 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_4), idx_idx, _mm512_castsi256_si512(idx_base_4));
        __m512i load_idx_stage2_0 = _mm512_set_epi32( 0,  0, 22, 21, 20, 19, 18, 17, 16, 13, 12, 11, 10,  9,  8,  7);

        xArray_01 = _mm512_broadcastd_epi32(x128_0);                          // |x0|x1|x0|x1| ... |x0|x1|
        xArray_23 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x1));  // |x2|x3|x2|x3| ... |x2|x3|
        xArray_45 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x2));  // |x4|x5|x4|x5| ... |x4|x5|
        xArray_67 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x3));  // |x6|x7|x6|x7| ... |x6|x7|
        xArray_89 = _mm512_broadcastd_epi32(x128_1);                          // |x8|0 |x8| 0| ... |x8| 0|

        unsigned int load_mask_value = (((unsigned int)0xffffffff) >> 1);
        __mmask32 load_mask = *((__mmask32*) &load_mask_value);
        unsigned short blend_mask_value = ((unsigned short)0x3f80);
        __mmask16 blend_mask = *((__mmask16*) &blend_mask_value);
        unsigned short store_mask_value = (((unsigned short)0xffff) >> 2);
        __mmask16 store_mask = *((__mmask16*) &store_mask_value);
        for (BLASLONG idx_m = 0; idx_m < tag_m_14x; idx_m+=14) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(idx_m)*9]);                          // Load 3 rows with n=9 plus 5 elements
            matrixArray_1 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+3)*9 + 5]);   // Load 3 rows with n=9 plus 4 elements
            matrixArray_2 = _mm512_loadu_si512(&a[(idx_m+7)*9]);                        // Load 3 rows with n=9 plus 5 elements
            matrixArray_3 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+10)*9 + 5]);  // Load 3 rows with n=9 plus 4 elements

            // Stage 1: interleave per 16 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi16(matrixArray_0, load_idx_stage1_0, matrixArray_1);  // |a0|a1|...|g0|g1|a2|a3|...|g2|g3|x|x|x|x|
            matrixArray_stage_1 = _mm512_permutex2var_epi16(matrixArray_0, load_idx_stage1_1, matrixArray_1);  // |a4|a5|...|g4|g5|a6|a7|...|g6|g7|x|x|x|x|
            matrixArray_stage_2 = _mm512_permutex2var_epi16(matrixArray_2, load_idx_stage1_2, matrixArray_3);  // |h2|h3|...|n2|n3|h0|h1|...|n0|n1|x|x|x|x|
            matrixArray_stage_3 = _mm512_permutex2var_epi16(matrixArray_2, load_idx_stage1_3, matrixArray_3);  // |h6|h7|...|n6|n7|h4|h5|...|n4|n5|x|x|x|x|
            matrixArray_4       = _mm512_permutex2var_epi16(matrixArray_0, load_idx_stage1_4, matrixArray_1);  // |a8| x|...|g8| x| x| x|...| x| x|x|x|x|x|
            matrixArray_5       = _mm512_permutex2var_epi16(matrixArray_2, load_idx_stage1_4, matrixArray_3);  // | x| x|...| x| x|h8| x|...|n8| x|x|x|x|x|

            // Stage 2: interleave per 32 bits
            matrixArray_0 = _mm512_mask_blend_epi32(blend_mask, matrixArray_stage_0, matrixArray_stage_2);           // |a0|a1|b0|b1|...|h0|h1|i0|i1|j0|j1|...|n0|n1|x|x|x|x|
            matrixArray_1 = _mm512_permutex2var_epi32(matrixArray_stage_0, load_idx_stage2_0, matrixArray_stage_2);  // |a2|a3|b2|b3|...|h2|h3|i2|i3|j2|j3|...|n2|n3|x|x|x|x|
            matrixArray_2 = _mm512_mask_blend_epi32(blend_mask, matrixArray_stage_1, matrixArray_stage_3);           // |a4|a5|b4|b5|...|h4|h5|i4|i5|j4|j5|...|n4|n5|x|x|x|x|
            matrixArray_3 = _mm512_permutex2var_epi32(matrixArray_stage_1, load_idx_stage2_0, matrixArray_stage_3);  // |a6|a7|b6|b7|...|h6|h7|i6|i7|j6|j7|...|n6|n7|x|x|x|x|
            matrixArray_4 = _mm512_mask_blend_epi32(blend_mask, matrixArray_4, matrixArray_5);                       // |a8| x|b8| x|...|h8| x|i8| x|j8| x|...|n8| x|x|x|x|x|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_0, (__m512bh) xArray_01);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_1, (__m512bh) xArray_23);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_2, (__m512bh) xArray_45);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_3, (__m512bh) xArray_67);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_4, (__m512bh) xArray_89);
            result_0 = _mm512_add_ps(result_0, result_1);

            STORE16_MASK_COMPLETE_RESULT(result_0, y+idx_m, store_mask)
        }
    }

    if (tag_m_14x != m) {
        __m256i matrixArray256;
        __m256i x256 = _mm256_insertf128_si256(_mm256_castsi128_si256(x128_0), x128_1, 0x1);
        __m256  result256;
        __m128  result128, tmp128;
        unsigned short load256_mask_value = (((unsigned short)0xffff) >> 7);
        __mmask16 load256_mask = *((__mmask16*) &load256_mask_value);
        for (BLASLONG i = tag_m_14x; i < m; i++) {
            result256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_maskz_loadu_epi16(load256_mask, &a[(i)*9]);
            result256 = _mm256_dpbf16_ps(result256, (__m256bh) matrixArray256, (__m256bh) x256);
            result128 = _mm_add_ps(_mm256_castps256_ps128(result256), _mm256_extractf128_ps(result256, 0x1));
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif
        }
    }

    return 0;
}

// 12 rows parallel processing BF16 GEMV kernel for n=10 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_12x10_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_12x10_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_12x10_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_12x10(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_12x  = m - (m%12);

    unsigned char x_load_mask_value = (((unsigned char)0xf) >> 3);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i x128_0 = _mm_loadu_si128((__m128i *)x);                                  // |x0|x1|x2|x3|x4|x5|x6|x7|
    __m128i x128_1 = _mm_maskz_loadu_epi32(x_load_mask, (x+8));           // |x8|x9|0 | 0| 0| 0| 0| 0|

    if (tag_m_12x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4;
        __m512i matrixArray_stage_0, matrixArray_stage_1, matrixArray_stage_2, matrixArray_stage_3, matrixArray_stage_4, matrixArray_stage_5;
        __m512i xArray_01, xArray_23, xArray_45, xArray_67, xArray_89;
        __m512  result_0, result_1;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m256i M256_EPI32_1 = _mm256_set1_epi32(1);
        __m256i idx_base_0 = _mm256_set_epi32( 0,  0, 26, 21, 16, 10,  5,  0);
        __m256i idx_base_1 = _mm256_add_epi32(idx_base_0, M256_EPI32_1);
        __m256i idx_base_2 = _mm256_add_epi32(idx_base_1, M256_EPI32_1);
        __m256i idx_base_3 = _mm256_add_epi32(idx_base_2, M256_EPI32_1);
        __m256i idx_base_4 = _mm256_add_epi32(idx_base_3, M256_EPI32_1);
        __m512i idx_idx    = _mm512_set_epi32( 0,  0,  0,  0, 21, 20, 19, 18, 17, 16,  5,  4,  3,  2,  1,  0);

        __m512i load_idx_stage1_0 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_0), idx_idx, _mm512_castsi256_si512(idx_base_1));
        __m512i load_idx_stage1_1 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_2), idx_idx, _mm512_castsi256_si512(idx_base_3));
        __m512i load_idx_stage1_2 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_1), idx_idx, _mm512_castsi256_si512(idx_base_0));
        __m512i load_idx_stage1_3 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_3), idx_idx, _mm512_castsi256_si512(idx_base_2));
        __m512i load_idx_stage1_4 = _mm512_permutex2var_epi32(_mm512_castsi256_si512(idx_base_4), idx_idx, _mm512_castsi256_si512(idx_base_4));
        __m512i load_idx_stage2_0 = _mm512_set_epi32( 0,  0,  0,  0, 21, 20, 19, 18, 17, 16, 11, 10,  9,  8,  7,  6);

        xArray_01 = _mm512_broadcastd_epi32(x128_0);                          // |x0|x1|x0|x1| ... |x0|x1|
        xArray_23 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x1));  // |x2|x3|x2|x3| ... |x2|x3|
        xArray_45 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x2));  // |x4|x5|x4|x5| ... |x4|x5|
        xArray_67 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x3));  // |x6|x7|x6|x7| ... |x6|x7|
        xArray_89 = _mm512_broadcastd_epi32(x128_1);                          // |x8|x9|x8|x9| ... |x8|x9|

        unsigned short blend_mask_value = ((unsigned short)0x0fc0);
        __mmask16 blend_mask = *((__mmask16*) &blend_mask_value);
        unsigned short load_mask_value = (((unsigned short)0xffff) >> 1);
        __mmask16 load_mask = *((__mmask16*) &load_mask_value);
        unsigned short store_mask_value = (((unsigned short)0xffff) >> 4);
        __mmask16 store_mask = *((__mmask16*) &store_mask_value);
        for (BLASLONG idx_m = 0; idx_m < tag_m_12x; idx_m+=12) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_maskz_loadu_epi32(load_mask, &a[(idx_m)*10]);     // Load 3 rows with n=10
            matrixArray_1 = _mm512_maskz_loadu_epi32(load_mask, &a[(idx_m+3)*10]);   // Load 3 rows with n=10
            matrixArray_2 = _mm512_maskz_loadu_epi32(load_mask, &a[(idx_m+6)*10]);   // Load 3 rows with n=10
            matrixArray_3 = _mm512_maskz_loadu_epi32(load_mask, &a[(idx_m+9)*10]);   // Load 3 rows with n=10

            // Stage 1: interleave per 32 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage1_0, matrixArray_1);  // |a0|a1|...|f0|f1|a2|a3|...|f2|f3|x|x|x|x|x|x|x|x|
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage1_1, matrixArray_1);  // |a4|a5|...|f4|f5|a6|a7|...|f6|f7|x|x|x|x|x|x|x|x|
            matrixArray_stage_2 = _mm512_permutex2var_epi32(matrixArray_2, load_idx_stage1_2, matrixArray_3);  // |g2|g3|...|l2|l3|g0|g1|...|l0|l1|x|x|x|x|x|x|x|x|
            matrixArray_stage_3 = _mm512_permutex2var_epi32(matrixArray_2, load_idx_stage1_3, matrixArray_3);  // |g6|g7|...|l6|l7|g4|g5|...|l4|l5|x|x|x|x|x|x|x|x|
            matrixArray_stage_4 = _mm512_permutex2var_epi32(matrixArray_0, load_idx_stage1_4, matrixArray_1);  // |a8|a9|...|f8|f9| x| x|...| x| x|x|x|x|x|x|x|x|x|
            matrixArray_stage_5 = _mm512_permutex2var_epi32(matrixArray_2, load_idx_stage1_4, matrixArray_3);  // | x| x|...| x| x|g8|g9|...|l8|l9|x|x|x|x|x|x|x|x|

            // Stage 3: interleave per 256 bits
            matrixArray_0 = _mm512_mask_blend_epi32(blend_mask, matrixArray_stage_0, matrixArray_stage_2);           // |a0|a1|...|l0|l1|x|x|x|x|x|x|x|x|
            matrixArray_1 = _mm512_permutex2var_epi32(matrixArray_stage_0, load_idx_stage2_0, matrixArray_stage_2);  // |a2|a3|...|l2|l3|x|x|x|x|x|x|x|x|
            matrixArray_2 = _mm512_mask_blend_epi32(blend_mask, matrixArray_stage_1, matrixArray_stage_3);           // |a4|a5|...|l4|l5|x|x|x|x|x|x|x|x|
            matrixArray_3 = _mm512_permutex2var_epi32(matrixArray_stage_1, load_idx_stage2_0, matrixArray_stage_3);  // |a6|a7|...|l6|l7|x|x|x|x|x|x|x|x|
            matrixArray_4 = _mm512_mask_blend_epi32(blend_mask, matrixArray_stage_4, matrixArray_stage_5);           // |a8|a9|...|l8|l9|x|x|x|x|x|x|x|x|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_0, (__m512bh) xArray_01);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_1, (__m512bh) xArray_23);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_2, (__m512bh) xArray_45);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_3, (__m512bh) xArray_67);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_4, (__m512bh) xArray_89);
            result_0 = _mm512_add_ps(result_0, result_1);

            STORE16_MASK_COMPLETE_RESULT(result_0, y+idx_m, store_mask)
        }
    }

    if (tag_m_12x != m) {
        __m256i matrixArray256;
        __m256i x256 = _mm256_insertf128_si256(_mm256_castsi128_si256(x128_0), x128_1, 0x1);
        __m256  result256;
        __m128  result128, tmp128;
        unsigned char load256_mask_value = (((unsigned char)0xff) >> 3);
        __mmask8 load256_mask = *((__mmask8*) &load256_mask_value);
        for (BLASLONG i = tag_m_12x; i < m; i++) {
            result256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_maskz_loadu_epi32(load256_mask, &a[(i)*10]);
            result256 = _mm256_dpbf16_ps(result256, (__m256bh) matrixArray256, (__m256bh) x256);
            result128 = _mm_add_ps(_mm256_castps256_ps128(result256), _mm256_extractf128_ps(result256, 0x1));
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif
        }
    }

    return 0;
}

// 15 rows parallel processing BF16 GEMV kernel for n=11 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_15x11_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_15x11_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_15x11_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_15x11(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_15x = m - (m%15);

    unsigned char x_load_mask_value = (((unsigned char)0xff) >> 5);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i x128_0 = _mm_loadu_si128((__m128i *)x);                         // |x0|x1| x2|x3|x4|x5|x6|x7|
    __m128i x128_1 = _mm_maskz_loadu_epi16(x_load_mask, (x+8));  // |x8|x9|x10| 0| 0| 0| 0| 0|

    if (tag_m_15x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4, matrixArray_5;
        __m512i matrixArray_stage_0, matrixArray_stage_1, matrixArray_stage_2, matrixArray_stage_3, matrixArray_stage_4, matrixArray_stage_5;
        __m512i xArray_01, xArray_23, xArray_45, xArray_67, xArray_89, xArray_10;
        __m512  result_0, result_1;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i idx_stage1_base_0, idx_stage1_base_1, idx_stage1_base_2, idx_stage1_base_3, idx_stage1_base_4, idx_stage1_base_5;
        __m512i idx_stage2_base_0, idx_stage2_base_1, idx_stage2_base_2, idx_stage2_base_3;

        __m512i M512_EPI16_2, M512_EPI16_4, M512_EPI16_6, M512_EPI32_5;
        M512_EPI16_2 = _mm512_set1_epi16(2);
        M512_EPI16_4 = _mm512_add_epi16(M512_EPI16_2, M512_EPI16_2);
        M512_EPI16_6 = _mm512_add_epi16(M512_EPI16_4, M512_EPI16_2);
        M512_EPI32_5 = _mm512_set1_epi32(5);

        unsigned int BASE_MASK_10_value = ((unsigned int)0x000003ff);
        __mmask32 BASE_MASK_10 = *((__mmask32*) &BASE_MASK_10_value);
        unsigned int BASE_MASK_20_value = ((unsigned int)0x000ffc00);
        __mmask32 BASE_MASK_20 = *((__mmask32*) &BASE_MASK_20_value);
        unsigned int BASE_MASK_30_value = ((unsigned int)0x3ff00000);
        __mmask32 BASE_MASK_30 = *((__mmask32*) &BASE_MASK_30_value);

        idx_stage1_base_0 = _mm512_set_epi16( 0,  0, 49, 48, 38, 37, 27, 26, 16, 15,  5,  4, 47, 46, 36, 35,
                                             25, 24, 14, 13,  3,  2, 45, 44, 34, 33, 23, 22, 12, 11,  1,  0);
        idx_stage1_base_1 = _mm512_add_epi16(idx_stage1_base_0, M512_EPI16_6);

        idx_stage1_base_2 = _mm512_mask_add_epi16(idx_stage1_base_0, BASE_MASK_10, idx_stage1_base_0, M512_EPI16_2);
        idx_stage1_base_2 = _mm512_mask_sub_epi16(idx_stage1_base_2, BASE_MASK_20, idx_stage1_base_0, M512_EPI16_2);
        idx_stage1_base_3 = _mm512_add_epi16(idx_stage1_base_2, M512_EPI16_6);

        idx_stage1_base_4 = _mm512_mask_add_epi16(idx_stage1_base_2, BASE_MASK_10, idx_stage1_base_2, M512_EPI16_2);
        idx_stage1_base_4 = _mm512_mask_add_epi16(idx_stage1_base_4, BASE_MASK_20, idx_stage1_base_2, M512_EPI16_2);
        idx_stage1_base_4 = _mm512_mask_sub_epi16(idx_stage1_base_4, BASE_MASK_30, idx_stage1_base_2, M512_EPI16_4);
        idx_stage1_base_5 = _mm512_add_epi16(idx_stage1_base_4, M512_EPI16_6);

        unsigned short idx_stage2_mask_1_value = ((unsigned short)0x03e0);
        __mmask16 idx_stage2_mask_1 = *((__mmask16*) &idx_stage2_mask_1_value);
        unsigned short idx_stage2_mask_2_value = ((unsigned short)0x7c00);
        __mmask16 idx_stage2_mask_2 = *((__mmask16*) &idx_stage2_mask_2_value);
        idx_stage2_base_0 = _mm512_set_epi32( 0,  0,  0,  0,  0,  0, 20, 19, 18, 17, 16,  9,  8,  7,  6,  5);
        idx_stage2_base_1 = _mm512_set_epi32( 0, 25, 24, 23, 22, 21,  9,  8,  7,  6,  5,  4,  3,  2,  1,  0);
        idx_stage2_base_2 = _mm512_add_epi32(idx_stage2_base_0, M512_EPI32_5);
        idx_stage2_base_2 = _mm512_mask_add_epi32(idx_stage2_base_2, idx_stage2_mask_1, idx_stage2_base_2, M512_EPI32_5);
        idx_stage2_base_3 = _mm512_mask_sub_epi32(idx_stage2_base_1, idx_stage2_mask_2, idx_stage2_base_1, M512_EPI32_5);

        xArray_01 = _mm512_broadcastd_epi32(x128_0);                          // |x0 |x1 |x0 |x1 | ... |x0 |x1 |
        xArray_23 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x1));  // |x2 |x3 |x2 |x3 | ... |x2 |x3 |
        xArray_45 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x2));  // |x4 |x5 |x4 |x5 | ... |x4 |x5 |
        xArray_67 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x3));  // |x6 |x7 |x6 |x7 | ... |x6 |x7 |
        xArray_89 = _mm512_broadcastd_epi32(x128_1);                          // |x8 |x9 |x8 |x9 | ... |x8 |x9 |
        xArray_10 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_1, 0x1));  // |x10|0  |x10|0  | ... |x10|0  |

        unsigned int load_mask_value = (((unsigned int)0xffffffff) >> 9);
        __mmask32 load_mask = *((__mmask32*) &load_mask_value);

        unsigned short store_mask_value = (((unsigned short)0xffff) >> 1);
        __mmask16 store_mask = *((__mmask16*) &store_mask_value);

        for (BLASLONG idx_m = 0; idx_m < tag_m_15x; idx_m+=15) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[idx_m*11]);                             // Load 2 rows with n=11 plus 10 elements
            matrixArray_1 = _mm512_maskz_loadu_epi16(load_mask, &a[idx_m*11 + 32]);       // Load 2 rows with n=11 plus 1 element
            matrixArray_2 = _mm512_loadu_si512(&a[(idx_m+5)*11]);                         // Load 2 rows with n=11 plus 10 elements
            matrixArray_3 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+5)*11 + 32]);   // Load 2 rows with n=11 plus 1 element
            matrixArray_4 = _mm512_loadu_si512(&a[(idx_m+10)*11]);                        // Load 2 rows with n=11 plus 10 elements
            matrixArray_5 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+10)*11 + 32]);  // Load 2 rows with n=11 plus 1 element

            // Stage 1: interleave per 16 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi16(matrixArray_0, idx_stage1_base_0, matrixArray_1);  // |a0|a1|...|e0|e1|a2|a3|...|e2|e3|a4 |a5|...|e4 |e5|
            matrixArray_stage_1 = _mm512_permutex2var_epi16(matrixArray_0, idx_stage1_base_1, matrixArray_1);  // |a6|a7|...|e6|e7|a8|a9|...|e8|e9|a10|x |...|e10|x |
            matrixArray_stage_2 = _mm512_permutex2var_epi16(matrixArray_2, idx_stage1_base_2, matrixArray_3);  // |f2|f3|...|j2|j3|f0|f1|...|j0|j1|f4 |f5|...|j4 |j5|
            matrixArray_stage_3 = _mm512_permutex2var_epi16(matrixArray_2, idx_stage1_base_3, matrixArray_3);  // |f8|f9|...|j8|j9|f6|f7|...|j6|j7|f10|x |...|j10|x |
            matrixArray_stage_4 = _mm512_permutex2var_epi16(matrixArray_4, idx_stage1_base_4, matrixArray_5);  // |k4|k5|...|o4|o5|k2|k3|...|o2|o3|k0 |k1|...|o0 |o1|
            matrixArray_stage_5 = _mm512_permutex2var_epi16(matrixArray_4, idx_stage1_base_5, matrixArray_5);  // |k10|x|...|o10|x|k8|k9|...|o8|o9|k6 |k7|...|o6 |o7|

            // Stage 2: interleave per 32 bits
            matrixArray_0 = _mm512_mask_blend_epi32(idx_stage2_mask_1, matrixArray_stage_0, matrixArray_stage_2);    // |a0|a1|...|j0|j1|x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_3 = _mm512_mask_blend_epi32(idx_stage2_mask_1, matrixArray_stage_1, matrixArray_stage_3);    // |a6|a7|...|j6|j7|x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_1 = _mm512_permutex2var_epi32(matrixArray_stage_0, idx_stage2_base_0, matrixArray_stage_2);  // |a2|a3|...|j2|j3|x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_2 = _mm512_permutex2var_epi32(matrixArray_stage_0, idx_stage2_base_2, matrixArray_stage_2);  // |a4|a5|...|j4|j5|x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_4 = _mm512_permutex2var_epi32(matrixArray_stage_1, idx_stage2_base_0, matrixArray_stage_3);  // |a8|a9|...|j8|j9|x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_5 = _mm512_permutex2var_epi32(matrixArray_stage_1, idx_stage2_base_2, matrixArray_stage_3);  // |a10|x|...|j10|x|x|x|x|x|x|x|x|x|x|x|x|x|

            matrixArray_0 = _mm512_mask_blend_epi32(idx_stage2_mask_2, matrixArray_0,       matrixArray_stage_4);    // |a0|a1|.......................|o0|o1|x|x|
            matrixArray_3 = _mm512_mask_blend_epi32(idx_stage2_mask_2, matrixArray_3,       matrixArray_stage_5);    // |a6|a7|.......................|o6|o7|x|x|
            matrixArray_1 = _mm512_permutex2var_epi32(matrixArray_1      , idx_stage2_base_1, matrixArray_stage_4);  // |a2|a3|.......................|o2|o3|x|x|
            matrixArray_2 = _mm512_permutex2var_epi32(matrixArray_2      , idx_stage2_base_3, matrixArray_stage_4);  // |a4|a5|.......................|o4|o5|x|x|
            matrixArray_4 = _mm512_permutex2var_epi32(matrixArray_4      , idx_stage2_base_1, matrixArray_stage_5);  // |a8|a9|.......................|o8|o9|x|x|
            matrixArray_5 = _mm512_permutex2var_epi32(matrixArray_5      , idx_stage2_base_3, matrixArray_stage_5);  // |a10|x|.......................|o10|x|x|x|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_0, (__m512bh) xArray_01);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_1, (__m512bh) xArray_23);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_2, (__m512bh) xArray_45);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_3, (__m512bh) xArray_67);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_4, (__m512bh) xArray_89);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_5, (__m512bh) xArray_10);
            result_0 = _mm512_add_ps(result_0, result_1);

            STORE16_MASK_COMPLETE_RESULT(result_0, y+idx_m, store_mask)
        }
    }

    if (tag_m_15x != m) {
        __m256i matrixArray256;
        __m256i x256 = _mm256_insertf128_si256(_mm256_castsi128_si256(x128_0), x128_1, 0x1);
        __m256  result256;
        __m128  result128, tmp128;
        unsigned short load256_mask_value = (((unsigned short)0xffff) >> 5);
        __mmask16 load256_mask = *((__mmask16*) &load256_mask_value);
        for (BLASLONG i = tag_m_15x; i < m; i++) {
            result256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_maskz_loadu_epi16(load256_mask, &a[(i)*11]);
            result256 = _mm256_dpbf16_ps(result256, (__m256bh) matrixArray256, (__m256bh) x256);
            result128 = _mm_add_ps(_mm256_castps256_ps128(result256), _mm256_extractf128_ps(result256, 0x1));
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif
        }
    }

    return 0;
}

// 15 rows parallel processing BF16 GEMV kernel for n=12 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_15x12_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_15x12_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_15x12_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_15x12(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_15x = m - (m%15);

    unsigned char x_load_mask_value = (((unsigned char)0xff) >> 4);
    __mmask8 x_load_mask = *((__mmask8*) &x_load_mask_value);
    __m128i x128_0 = _mm_loadu_si128((__m128i *)x);                         // |x0|x1| x2| x3|x4|x5|x6|x7|
    __m128i x128_1 = _mm_maskz_loadu_epi16(x_load_mask, (x+8));  // |x8|x9|x10|x11| 0| 0| 0| 0|

    if (tag_m_15x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4, matrixArray_5;
        __m512i matrixArray_stage_0, matrixArray_stage_1, matrixArray_stage_2, matrixArray_stage_3, matrixArray_stage_4, matrixArray_stage_5;
        __m512i xArray_01, xArray_23, xArray_45, xArray_67, xArray_89, xArray_10;
        __m512  result_0, result_1;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i idx_stage1_base_0, idx_stage1_base_1, idx_stage1_base_2, idx_stage1_base_3, idx_stage1_base_4, idx_stage1_base_5;
        __m512i idx_stage2_base_0, idx_stage2_base_1, idx_stage2_base_2, idx_stage2_base_3;

        __m512i M512_EPI32_1, M512_EPI32_2, M512_EPI32_3, M512_EPI32_5;
        M512_EPI32_1 = _mm512_set1_epi32(1);
        M512_EPI32_2 = _mm512_add_epi32(M512_EPI32_1, M512_EPI32_1);
        M512_EPI32_3 = _mm512_add_epi32(M512_EPI32_2, M512_EPI32_1);
        M512_EPI32_5 = _mm512_add_epi32(M512_EPI32_3, M512_EPI32_2);

        unsigned short BASE_MASK_10_value = ((unsigned short)0x001f);
        __mmask16 BASE_MASK_10 = *((__mmask16*) &BASE_MASK_10_value);
        unsigned short BASE_MASK_20_value = ((unsigned short)0x03e0);
        __mmask16 BASE_MASK_20 = *((__mmask16*) &BASE_MASK_20_value);
        unsigned short BASE_MASK_30_value = ((unsigned short)0xfc00);
        __mmask16 BASE_MASK_30 = *((__mmask16*) &BASE_MASK_30_value);

        idx_stage1_base_0 = _mm512_set_epi32( 0, 26, 20, 14,  8,  2, 25, 19, 13,  7,  1,  24, 18, 12,  6,  0);
        idx_stage1_base_1 = _mm512_add_epi32(idx_stage1_base_0, M512_EPI32_3);

        idx_stage1_base_2 = _mm512_mask_add_epi32(idx_stage1_base_0, BASE_MASK_10, idx_stage1_base_0, M512_EPI32_1);
        idx_stage1_base_2 = _mm512_mask_sub_epi32(idx_stage1_base_2, BASE_MASK_20, idx_stage1_base_0, M512_EPI32_1);
        idx_stage1_base_3 = _mm512_add_epi32(idx_stage1_base_2, M512_EPI32_3);

        idx_stage1_base_4 = _mm512_mask_add_epi32(idx_stage1_base_2, BASE_MASK_10, idx_stage1_base_2, M512_EPI32_1);
        idx_stage1_base_4 = _mm512_mask_add_epi32(idx_stage1_base_4, BASE_MASK_20, idx_stage1_base_2, M512_EPI32_1);
        idx_stage1_base_4 = _mm512_mask_sub_epi32(idx_stage1_base_4, BASE_MASK_30, idx_stage1_base_2, M512_EPI32_2);
        idx_stage1_base_5 = _mm512_add_epi32(idx_stage1_base_4, M512_EPI32_3);

        unsigned short idx_stage2_mask_1_value = ((unsigned short)0x03e0);
        __mmask16 idx_stage2_mask_1 = *((__mmask16*) &idx_stage2_mask_1_value);
        unsigned short idx_stage2_mask_2_value = ((unsigned short)0x7c00);
        __mmask16 idx_stage2_mask_2 = *((__mmask16*) &idx_stage2_mask_2_value);
        idx_stage2_base_0 = _mm512_set_epi32( 0,  0,  0,  0,  0,  0, 20, 19, 18, 17, 16,  9,  8,  7,  6,  5);
        idx_stage2_base_1 = _mm512_set_epi32( 0, 25, 24, 23, 22, 21,  9,  8,  7,  6,  5,  4,  3,  2,  1,  0);
        idx_stage2_base_2 = _mm512_add_epi32(idx_stage2_base_0, M512_EPI32_5);
        idx_stage2_base_2 = _mm512_mask_add_epi32(idx_stage2_base_2, idx_stage2_mask_1, idx_stage2_base_2, M512_EPI32_5);
        idx_stage2_base_3 = _mm512_mask_sub_epi32(idx_stage2_base_1, idx_stage2_mask_2, idx_stage2_base_1, M512_EPI32_5);

        xArray_01 = _mm512_broadcastd_epi32(x128_0);                          // |x0 |x1 |x0 |x1 | ... |x0 |x1 |
        xArray_23 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x1));  // |x2 |x3 |x2 |x3 | ... |x2 |x3 |
        xArray_45 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x2));  // |x4 |x5 |x4 |x5 | ... |x4 |x5 |
        xArray_67 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_0, 0x3));  // |x6 |x7 |x6 |x7 | ... |x6 |x7 |
        xArray_89 = _mm512_broadcastd_epi32(x128_1);                          // |x8 |x9 |x8 |x9 | ... |x8 |x9 |
        xArray_10 = _mm512_broadcastd_epi32(_mm_shuffle_epi32(x128_1, 0x1));  // |x10|x11|x10|x11| ... |x10|x11|

        unsigned int load_mask_value = (((unsigned int)0xffffffff) >> 4);
        __mmask32 load_mask = *((__mmask32*) &load_mask_value);

        unsigned short store_mask_value = (((unsigned short)0xffff) >> 1);
        __mmask16 store_mask = *((__mmask16*) &store_mask_value);

        for (BLASLONG idx_m = 0; idx_m < tag_m_15x; idx_m+=15) {
            result_0 = _mm512_setzero_ps();
            result_1 = _mm512_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[idx_m*12]);                             // Load 2 rows with n=12 plus 8 elements
            matrixArray_1 = _mm512_maskz_loadu_epi16(load_mask, &a[idx_m*12 + 32]);       // Load 2 rows with n=12 plus 4 element
            matrixArray_2 = _mm512_loadu_si512(&a[(idx_m+5)*12]);                         // Load 2 rows with n=12 plus 8 elements
            matrixArray_3 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+5)*12 + 32]);   // Load 2 rows with n=12 plus 4 element
            matrixArray_4 = _mm512_loadu_si512(&a[(idx_m+10)*12]);                        // Load 2 rows with n=12 plus 8 elements
            matrixArray_5 = _mm512_maskz_loadu_epi16(load_mask, &a[(idx_m+10)*12 + 32]);  // Load 2 rows with n=12 plus 4 element

            // Stage 1: interleave per 16 bits
            matrixArray_stage_0 = _mm512_permutex2var_epi32(matrixArray_0, idx_stage1_base_0, matrixArray_1);  // |a0 |a1 |...|e0 |e1 |a2|a3|...|e2|e3|a4 |a5 |...|e4 |e5 |
            matrixArray_stage_1 = _mm512_permutex2var_epi32(matrixArray_0, idx_stage1_base_1, matrixArray_1);  // |a6 |a7 |...|e6 |e7 |a8|a9|...|e8|e9|a10|a11|...|e10|e11|
            matrixArray_stage_2 = _mm512_permutex2var_epi32(matrixArray_2, idx_stage1_base_2, matrixArray_3);  // |f2 |f3 |...|j2 |j3 |f0|f1|...|j0|j1|f4 |f5 |...|j4 |j5 |
            matrixArray_stage_3 = _mm512_permutex2var_epi32(matrixArray_2, idx_stage1_base_3, matrixArray_3);  // |f8 |f9 |...|j8 |j9 |f6|f7|...|j6|j7|f10|f11|...|j10|j11|
            matrixArray_stage_4 = _mm512_permutex2var_epi32(matrixArray_4, idx_stage1_base_4, matrixArray_5);  // |k4 |k5 |...|o4 |o5 |k2|k3|...|o2|o3|k0 |k1 |...|o0 |o1 |
            matrixArray_stage_5 = _mm512_permutex2var_epi32(matrixArray_4, idx_stage1_base_5, matrixArray_5);  // |k10|k11|...|o10|o11|k8|k9|...|o8|o9|k6 |k7 |...|o6 |o7 |       

            // Stage 2: interleave per 32 bits
            matrixArray_0 = _mm512_mask_blend_epi32(idx_stage2_mask_1, matrixArray_stage_0, matrixArray_stage_2);    // |a0 |a1 |...|j0 |j1 |x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_3 = _mm512_mask_blend_epi32(idx_stage2_mask_1, matrixArray_stage_1, matrixArray_stage_3);    // |a6 |a7 |...|j6 |j7 |x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_1 = _mm512_permutex2var_epi32(matrixArray_stage_0, idx_stage2_base_0, matrixArray_stage_2);  // |a2 |a3 |...|j2 |j3 |x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_2 = _mm512_permutex2var_epi32(matrixArray_stage_0, idx_stage2_base_2, matrixArray_stage_2);  // |a4 |a5 |...|j4 |j5 |x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_4 = _mm512_permutex2var_epi32(matrixArray_stage_1, idx_stage2_base_0, matrixArray_stage_3);  // |a8 |a9 |...|j8 |j9 |x|x|x|x|x|x|x|x|x|x|x|x|
            matrixArray_5 = _mm512_permutex2var_epi32(matrixArray_stage_1, idx_stage2_base_2, matrixArray_stage_3);  // |a10|a11|...|j10|j11|x|x|x|x|x|x|x|x|x|x|x|x|

            matrixArray_0 = _mm512_mask_blend_epi32(idx_stage2_mask_2, matrixArray_0,       matrixArray_stage_4);    // |a0|a1|.......................|o0|o1|x|x|
            matrixArray_3 = _mm512_mask_blend_epi32(idx_stage2_mask_2, matrixArray_3,       matrixArray_stage_5);    // |a6|a7|.......................|o6|o7|x|x|
            matrixArray_1 = _mm512_permutex2var_epi32(matrixArray_1      , idx_stage2_base_1, matrixArray_stage_4);  // |a2|a3|.......................|o2|o3|x|x|
            matrixArray_2 = _mm512_permutex2var_epi32(matrixArray_2      , idx_stage2_base_3, matrixArray_stage_4);  // |a4|a5|.......................|o4|o5|x|x|
            matrixArray_4 = _mm512_permutex2var_epi32(matrixArray_4      , idx_stage2_base_1, matrixArray_stage_5);  // |a8|a9|.......................|o8|o9|x|x|
            matrixArray_5 = _mm512_permutex2var_epi32(matrixArray_5      , idx_stage2_base_3, matrixArray_stage_5);  // |a10|x|.......................|o10|x|x|x|

            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_0, (__m512bh) xArray_01);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_1, (__m512bh) xArray_23);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_2, (__m512bh) xArray_45);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_3, (__m512bh) xArray_67);
            result_0 = _mm512_dpbf16_ps(result_0, (__m512bh) matrixArray_4, (__m512bh) xArray_89);
            result_1 = _mm512_dpbf16_ps(result_1, (__m512bh) matrixArray_5, (__m512bh) xArray_10);
            result_0 = _mm512_add_ps(result_0, result_1);

            STORE16_MASK_COMPLETE_RESULT(result_0, y+idx_m, store_mask)
        }
    }

    if (tag_m_15x != m) {
        __m256i matrixArray256;
        __m256i x256 = _mm256_insertf128_si256(_mm256_castsi128_si256(x128_0), x128_1, 0x1);
        __m256  result256;
        __m128  result128, tmp128;
        unsigned short load256_mask_value = (((unsigned short)0xffff) >> 4);
        __mmask16 load256_mask = *((__mmask16*) &load256_mask_value);
        for (BLASLONG i = tag_m_15x; i < m; i++) {
            result256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_maskz_loadu_epi16(load256_mask, &a[(i)*12]);
            result256 = _mm256_dpbf16_ps(result256, (__m256bh) matrixArray256, (__m256bh) x256);
            result128 = _mm_add_ps(_mm256_castps256_ps128(result256), _mm256_extractf128_ps(result256, 0x1));
            tmp128 = _mm_shuffle_ps(result128, result128, 14);
            result128 = _mm_add_ps(result128, tmp128);
            tmp128 = _mm_shuffle_ps(result128, result128, 1);
            result128 = _mm_add_ps(result128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * result128[0] + beta * y[i];
#else
            y[i] = alpha * result128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = result128[0] * alpha;
#else
            y[i] = result128[0];
#endif
#endif
        }
    }

    return 0;
}


// 16 rows parallel processing BF16 GEMV kernel for n=13 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x13_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x13_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x13_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x13(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);

    unsigned short x_load_mask_value = (((unsigned short)0xffff) >> 3);
    __mmask16 x_load_mask = *((__mmask16*) &x_load_mask_value);
    __m256i x256 = _mm256_maskz_loadu_epi16(x_load_mask, x);    // |x0|x1|x2|x3|x4|x5|x6|x7|x8|x9|x10|x11|x12|0|0|0|

    if (tag_m_16x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3,  matrixArray_4,  matrixArray_5,  matrixArray_6,  matrixArray_7, \
                matrixArray_8, matrixArray_9, matrixArray_10, matrixArray_11, matrixArray_12, matrixArray_13, matrixArray_14, matrixArray_15;
        __m512i xArray_0, xArray_1, xArray_2, xArray_3;
        __m512  accum512_0, accum512_1;
        __m512  result_0, result_1;

        __m256i matrixArray256_0, matrixArray256_1, matrixArray256_2, matrixArray256_3, matrixArray256_4, matrixArray256_5, matrixArray256_6, matrixArray256_7;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i M512_EPI32_4 = _mm512_set1_epi32(4);
        __m512i idx_base_0   = _mm512_set_epi32(27, 26, 25, 24, 11, 10,  9,  8, 19, 18, 17, 16,  3,  2,  1,  0);
        __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_4);

        // Prepare X with 2-step interleave way
        xArray_0 = _mm512_inserti32x8(_mm512_castsi256_si512(x256), x256, 0x1);
        BF16_INTERLEAVE_1x32(xArray)

        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load matrix
            BF16_MATRIX_MASKZ_LOAD_8x16(matrixArray256, a, 13, idx_m, 0, x_load_mask)

            matrixArray_8  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_0), matrixArray256_1, 0x1);
            matrixArray_9  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_2), matrixArray256_3, 0x1);
            matrixArray_10 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_4), matrixArray256_5, 0x1);
            matrixArray_11 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_6), matrixArray256_7, 0x1);

            BF16_MATRIX_MASKZ_LOAD_8x16(matrixArray256, a, 13, idx_m+8, 0, x_load_mask)

            matrixArray_12 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_0), matrixArray256_1, 0x1);
            matrixArray_13 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_2), matrixArray256_3, 0x1);
            matrixArray_14 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_4), matrixArray256_5, 0x1);
            matrixArray_15 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_6), matrixArray256_7, 0x1);

            // interleave per 256 bits
            BF16_INTERLEAVE256_8x32(matrixArray)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_8x32(matrixArray)

            // Calculate the temp result for a..p[0:15]
            BF16_2STEP_INTERLEAVED_DOT_8x32(accum512, matrixArray, xArray)

            // Reorder and add up the final result
            result_0 = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
            result_1 = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);
            result_0 = _mm512_add_ps(result_0, result_1);
            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
        }

        if (m - tag_m_16x > 7) {
            __m512i permutevar_idx = _mm512_set_epi32(15, 14, 13, 12,  7,  6,  5,  4, 11, 10,  9,  8,  3,  2,  1,  0);
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load matrix
            BF16_MATRIX_MASKZ_LOAD_8x16(matrixArray256, a, 13, tag_m_16x, 0, x_load_mask)

            matrixArray_8  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_0), matrixArray256_1, 0x1);
            matrixArray_9  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_2), matrixArray256_3, 0x1);
            matrixArray_10 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_4), matrixArray256_5, 0x1);
            matrixArray_11 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_6), matrixArray256_7, 0x1);

            // interleave per 256 bits
            matrixArray_0 = _mm512_shuffle_i32x4(matrixArray_8,  matrixArray_10, 0x44);
            matrixArray_1 = _mm512_shuffle_i32x4(matrixArray_8,  matrixArray_10, 0xee);
            matrixArray_2 = _mm512_shuffle_i32x4(matrixArray_9,  matrixArray_11, 0x44);
            matrixArray_3 = _mm512_shuffle_i32x4(matrixArray_9,  matrixArray_11, 0xee);

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x32(matrixArray)

            // Calculate the temp result for a..h[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x32(accum512, matrixArray, xArray)

            accum512_0 = _mm512_add_ps(accum512_0, accum512_1);
            accum512_0 = _mm512_permutexvar_ps(permutevar_idx, accum512_0);
            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
            STORE8_COMPLETE_RESULT(result256, y+tag_m_16x)
            tag_m_16x += 8;
        }

        if (m - tag_m_16x > 3) {
            __m256i xArray256_0, xArray256_1, xArray256_2, xArray256_3;
            __m256  accum256_0, accum256_1;

            xArray256_0 = _mm512_castsi512_si256(xArray_0);
            xArray256_1 = _mm512_castsi512_si256(xArray_1);
            xArray256_2 = _mm512_castsi512_si256(xArray_2);
            xArray256_3 = _mm512_castsi512_si256(xArray_3);

            accum256_0 = _mm256_setzero_ps();
            accum256_1 = _mm256_setzero_ps();

            BF16_MATRIX_MASKZ_LOAD_4x16(matrixArray256, a, 13, tag_m_16x, 0, x_load_mask)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x16(matrixArray256)

            // Calculate the temp result for a..d[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x16(accum256, matrixArray256, xArray256)

            accum256_0 = _mm256_add_ps(accum256_0, accum256_1);
            __m128 result128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
            STORE4_COMPLETE_RESULT(result128, y+tag_m_16x)
            tag_m_16x += 4;
        }
    }

    if (tag_m_16x != m) {
        __m256i matrixArray256;
        __m256  accum256;
        __m128  accum128, tmp128;
        for (BLASLONG i = tag_m_16x; i < m; i++) {
            accum256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_maskz_loadu_epi16(x_load_mask, &a[(i)*13]);       // Load 1 rows with n=13
            accum256 = _mm256_dpbf16_ps(accum256, (__m256bh) matrixArray256, (__m256bh) x256);
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf32x4_ps(accum256, 1));
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
            accum128 = _mm_add_ps(accum128, tmp128);
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
            accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * accum128[0] + beta * y[i];
#else
            y[i] = alpha * accum128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = accum128[0] * alpha;
#else
            y[i] = accum128[0];
#endif
#endif
        }
    }

    return 0;
}

// 16 rows parallel processing BF16 GEMV kernel for n=14 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x14_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x14_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x14_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x14(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);

    unsigned short x_load_mask_value = (((unsigned short)0xffff) >> 2);
    __mmask16 x_load_mask = *((__mmask16*) &x_load_mask_value);
    __m256i x256 = _mm256_maskz_loadu_epi16(x_load_mask, x);    // |x0|x1|x2|x3|x4|x5|x6|x7|x8|x9|x10|x11|x12|x13|0|0|

    if (tag_m_16x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3,  matrixArray_4,  matrixArray_5,  matrixArray_6,  matrixArray_7, \
                matrixArray_8, matrixArray_9, matrixArray_10, matrixArray_11, matrixArray_12, matrixArray_13, matrixArray_14, matrixArray_15;
        __m512i xArray_0, xArray_1, xArray_2, xArray_3;
        __m512  accum512_0, accum512_1;
        __m512  result_0, result_1;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i M512_EPI32_4 = _mm512_set1_epi32(4);
        __m512i idx_base_0   = _mm512_set_epi32(27, 26, 25, 24, 11, 10,  9,  8, 19, 18, 17, 16,  3,  2,  1,  0);
        __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_4);
        __m512i shift_idx    = _mm512_set_epi32(0,  13, 12, 11, 10,  9,  8,  7,  0,  6,  5,  4,  3,  2,  1,  0);

        unsigned int load_mask_value = (((unsigned int)0xffffffff) >> 4);
        __mmask32 load_mask = *((__mmask32*) &load_mask_value);

        // Prepare X with 2-step interleave way
        xArray_0 = _mm512_inserti32x8(_mm512_castsi256_si512(x256), x256, 0x1);
        BF16_INTERLEAVE_1x32(xArray)

        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load matrix
            BF16_MATRIX_MASKZ_LOAD_8x32_2(matrixArray, a, 14, idx_m, 0, load_mask)

            // Pre-stage: shift the 2nd vector 1 position right for each register
            BF16_PERMUTE_8x32_2(shift_idx, matrixArray)

            // interleave per 256 bits
            BF16_INTERLEAVE256_8x32(matrixArray)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_8x32(matrixArray)

            // Calculate the temp result for a..p[0:15]
            BF16_2STEP_INTERLEAVED_DOT_8x32(accum512, matrixArray, xArray)

            // Reorder and add up the final result
            result_0 = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
            result_1 = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);
            result_0 = _mm512_add_ps(result_0, result_1);
            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
        }

        if (m - tag_m_16x > 7) {
            __m512i permutevar_idx = _mm512_set_epi32(15, 14, 13, 12,  7,  6,  5,  4, 11, 10,  9,  8,  3,  2,  1,  0);
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load matrix
            BF16_MATRIX_MASKZ_LOAD_4x32_2(matrixArray, a, 14, tag_m_16x, 0, load_mask)

            // Pre-stage: shift the 2nd vector 1 position right for each register
            BF16_PERMUTE_4x32_2(shift_idx, matrixArray)

            // interleave per 256 bits
            BF16_INTERLEAVE256_4x32(matrixArray)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x32(matrixArray)

            // Calculate the temp result for a..h[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x32(accum512, matrixArray, xArray)

            accum512_0 = _mm512_add_ps(accum512_0, accum512_1);
            accum512_0 = _mm512_permutexvar_ps(permutevar_idx, accum512_0);
            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
            STORE8_COMPLETE_RESULT(result256, y+tag_m_16x)
            tag_m_16x += 8;
        }

        if (m - tag_m_16x > 3) {
            __m256i matrixArray256_0, matrixArray256_1, matrixArray256_2, matrixArray256_3, matrixArray256_4, matrixArray256_5, matrixArray256_6, matrixArray256_7;
            __m256i xArray256_0, xArray256_1, xArray256_2, xArray256_3;
            __m256  accum256_0, accum256_1;

            xArray256_0 = _mm512_castsi512_si256(xArray_0);
            xArray256_1 = _mm512_castsi512_si256(xArray_1);
            xArray256_2 = _mm512_castsi512_si256(xArray_2);
            xArray256_3 = _mm512_castsi512_si256(xArray_3);

            accum256_0 = _mm256_setzero_ps();
            accum256_1 = _mm256_setzero_ps();

            BF16_MATRIX_MASKZ_LOAD_4x16(matrixArray256, a, 14, tag_m_16x, 0, x_load_mask)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x16(matrixArray256)

            // Calculate the temp result for a..d[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x16(accum256, matrixArray256, xArray256)

            accum256_0 = _mm256_add_ps(accum256_0, accum256_1);
            __m128 result128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
            STORE4_COMPLETE_RESULT(result128, y+tag_m_16x)
            tag_m_16x += 4;
        }
    }

    if (tag_m_16x != m) {
        __m256i matrixArray256;
        __m256  accum256;
        __m128  accum128, tmp128;
        for (BLASLONG i = tag_m_16x; i < m; i++) {
            accum256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_maskz_loadu_epi16(x_load_mask, &a[(i)*14]);       // Load 1 rows with n=14
            accum256 = _mm256_dpbf16_ps(accum256, (__m256bh) matrixArray256, (__m256bh) x256);
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf32x4_ps(accum256, 1));
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
            accum128 = _mm_add_ps(accum128, tmp128);
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
            accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * accum128[0] + beta * y[i];
#else
            y[i] = alpha * accum128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = accum128[0] * alpha;
#else
            y[i] = accum128[0];
#endif
#endif
        }
    }

    return 0;
}

// 16 rows parallel processing BF16 GEMV kernel for n=15 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x15_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x15_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x15_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x15(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);

    unsigned short x_load_mask_value = (((unsigned short)0xffff) >> 1);
    __mmask16 x_load_mask = *((__mmask16*) &x_load_mask_value);
    __m256i x256 = _mm256_maskz_loadu_epi16(x_load_mask, x);    // |x0|x1|x2|x3|x4|x5|x6|x7|x8|x9|x10|x11|x12|x13|x14|0|

    if (tag_m_16x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3,  matrixArray_4,  matrixArray_5,  matrixArray_6,  matrixArray_7, \
                matrixArray_8, matrixArray_9, matrixArray_10, matrixArray_11, matrixArray_12, matrixArray_13, matrixArray_14, matrixArray_15;
        __m512i xArray_0, xArray_1, xArray_2, xArray_3;
        __m512  accum512_0, accum512_1;
        __m512  result_0, result_1;

        __m256i matrixArray256_0, matrixArray256_1, matrixArray256_2, matrixArray256_3, matrixArray256_4, matrixArray256_5, matrixArray256_6, matrixArray256_7;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i M512_EPI32_4 = _mm512_set1_epi32(4);
        __m512i idx_base_0   = _mm512_set_epi32(27, 26, 25, 24, 11, 10,  9,  8, 19, 18, 17, 16,  3,  2,  1,  0);
        __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_4);

        // Prepare X with 2-step interleave way
        xArray_0 = _mm512_inserti32x8(_mm512_castsi256_si512(x256), x256, 0x1);
        BF16_INTERLEAVE_1x32(xArray)

        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load matrix
            BF16_MATRIX_MASKZ_LOAD_8x16(matrixArray256, a, 15, idx_m, 0, x_load_mask)

            matrixArray_8  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_0), matrixArray256_1, 0x1);
            matrixArray_9  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_2), matrixArray256_3, 0x1);
            matrixArray_10 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_4), matrixArray256_5, 0x1);
            matrixArray_11 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_6), matrixArray256_7, 0x1);

            BF16_MATRIX_MASKZ_LOAD_8x16(matrixArray256, a, 15, idx_m+8, 0, x_load_mask)

            matrixArray_12 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_0), matrixArray256_1, 0x1);
            matrixArray_13 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_2), matrixArray256_3, 0x1);
            matrixArray_14 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_4), matrixArray256_5, 0x1);
            matrixArray_15 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_6), matrixArray256_7, 0x1);

            // interleave per 256 bits
            BF16_INTERLEAVE256_8x32(matrixArray)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_8x32(matrixArray)

            // Calculate the temp result for a..p[0:15]
            BF16_2STEP_INTERLEAVED_DOT_8x32(accum512, matrixArray, xArray)

            // Reorder and add up the final result
            result_0 = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
            result_1 = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);
            result_0 = _mm512_add_ps(result_0, result_1);
            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
        }

        if (m - tag_m_16x > 7) {
            __m512i permutevar_idx = _mm512_set_epi32(15, 14, 13, 12,  7,  6,  5,  4, 11, 10,  9,  8,  3,  2,  1,  0);
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load matrix
            BF16_MATRIX_MASKZ_LOAD_8x16(matrixArray256, a, 15, tag_m_16x, 0, x_load_mask)

            matrixArray_8  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_0), matrixArray256_1, 0x1);
            matrixArray_9  = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_2), matrixArray256_3, 0x1);
            matrixArray_10 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_4), matrixArray256_5, 0x1);
            matrixArray_11 = _mm512_inserti32x8(_mm512_castsi256_si512(matrixArray256_6), matrixArray256_7, 0x1);

            // interleave per 256 bits
            matrixArray_0 = _mm512_shuffle_i32x4(matrixArray_8,  matrixArray_10, 0x44);
            matrixArray_1 = _mm512_shuffle_i32x4(matrixArray_8,  matrixArray_10, 0xee);
            matrixArray_2 = _mm512_shuffle_i32x4(matrixArray_9,  matrixArray_11, 0x44);
            matrixArray_3 = _mm512_shuffle_i32x4(matrixArray_9,  matrixArray_11, 0xee);

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x32(matrixArray)

            // Calculate the temp result for a..h[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x32(accum512, matrixArray, xArray)

            accum512_0 = _mm512_add_ps(accum512_0, accum512_1);
            accum512_0 = _mm512_permutexvar_ps(permutevar_idx, accum512_0);
            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
            STORE8_COMPLETE_RESULT(result256, y+tag_m_16x)
            tag_m_16x += 8;
        }

        if (m - tag_m_16x > 3) {
            __m256i xArray256_0, xArray256_1, xArray256_2, xArray256_3;
            __m256  accum256_0, accum256_1;

            xArray256_0 = _mm512_castsi512_si256(xArray_0);
            xArray256_1 = _mm512_castsi512_si256(xArray_1);
            xArray256_2 = _mm512_castsi512_si256(xArray_2);
            xArray256_3 = _mm512_castsi512_si256(xArray_3);

            accum256_0 = _mm256_setzero_ps();
            accum256_1 = _mm256_setzero_ps();

            BF16_MATRIX_MASKZ_LOAD_4x16(matrixArray256, a, 15, tag_m_16x, 0, x_load_mask)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x16(matrixArray256)

            // Calculate the temp result for a..d[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x16(accum256, matrixArray256, xArray256)

            accum256_0 = _mm256_add_ps(accum256_0, accum256_1);
            __m128 result128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
            STORE4_COMPLETE_RESULT(result128, y+tag_m_16x)
            tag_m_16x += 4;
        }
    }

    if (tag_m_16x != m) {
        __m256i matrixArray256;
        __m256  accum256;
        __m128  accum128, tmp128;
        for (BLASLONG i = tag_m_16x; i < m; i++) {
            accum256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_maskz_loadu_epi16(x_load_mask, &a[(i)*15]);       // Load 1 rows with n=15
            accum256 = _mm256_dpbf16_ps(accum256, (__m256bh) matrixArray256, (__m256bh) x256);
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf32x4_ps(accum256, 1));
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
            accum128 = _mm_add_ps(accum128, tmp128);
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
            accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * accum128[0] + beta * y[i];
#else
            y[i] = alpha * accum128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = accum128[0] * alpha;
#else
            y[i] = accum128[0];
#endif
#endif
        }
    }

    return 0;
}

// 16 rows parallel processing BF16 GEMV kernel for n=16 && lda ineffective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_16x16_alpha_beta(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_16x16_alpha_one(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_16x16_alpha(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_16x16(BLASLONG m, float alpha, bfloat16 *a, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_16x  = m & (~15);

    __m256i x256 = _mm256_loadu_si256((__m256i *)x);    // |x0|x1|x2|x3|x4|x5|x6|x7|x8|x9|x10|x11|x12|x13|x14|x15|

    if (tag_m_16x > 0) {
        __m512i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3,  matrixArray_4,  matrixArray_5,  matrixArray_6,  matrixArray_7, \
                matrixArray_8, matrixArray_9, matrixArray_10, matrixArray_11, matrixArray_12, matrixArray_13, matrixArray_14, matrixArray_15;
        __m512i xArray_0, xArray_1, xArray_2, xArray_3;
        __m512  accum512_0, accum512_1;
        __m512  result_0, result_1;

#ifndef ONE_ALPHA
        __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
        __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

        __m512i M512_EPI32_4 = _mm512_set1_epi32(4);
        __m512i idx_base_0   = _mm512_set_epi32(27, 26, 25, 24, 11, 10,  9,  8, 19, 18, 17, 16,  3,  2,  1,  0);
        __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_4);

        // Prepare X with 2-step interleave way
        xArray_0 = _mm512_inserti32x8(_mm512_castsi256_si512(x256), x256, 0x1);
        BF16_INTERLEAVE_1x32(xArray)

        for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            matrixArray_8  = _mm512_loadu_si512(&a[(idx_m   )*16]);  // Load 2 rows with n=16
            matrixArray_9  = _mm512_loadu_si512(&a[(idx_m+2 )*16]);  // Load 2 rows with n=16
            matrixArray_10 = _mm512_loadu_si512(&a[(idx_m+4 )*16]);  // Load 2 rows with n=16
            matrixArray_11 = _mm512_loadu_si512(&a[(idx_m+6 )*16]);  // Load 2 rows with n=16
            matrixArray_12 = _mm512_loadu_si512(&a[(idx_m+8 )*16]);  // Load 2 rows with n=16
            matrixArray_13 = _mm512_loadu_si512(&a[(idx_m+10)*16]);  // Load 2 rows with n=16
            matrixArray_14 = _mm512_loadu_si512(&a[(idx_m+12)*16]);  // Load 2 rows with n=16
            matrixArray_15 = _mm512_loadu_si512(&a[(idx_m+14)*16]);  // Load 2 rows with n=16

            // interleave per 256 bits
            BF16_INTERLEAVE256_8x32(matrixArray)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_8x32(matrixArray)

            // Calculate the temp result for a..p[0:15]
            BF16_2STEP_INTERLEAVED_DOT_8x32(accum512, matrixArray, xArray)

            // Reorder and add up the final result
            result_0 = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
            result_1 = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);
            result_0 = _mm512_add_ps(result_0, result_1);
            STORE16_COMPLETE_RESULT(result_0, y+idx_m)
        }

        if (m - tag_m_16x > 7) {
            __m512i permutevar_idx = _mm512_set_epi32(15, 14, 13, 12,  7,  6,  5,  4, 11, 10,  9,  8,  3,  2,  1,  0);
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            matrixArray_4 = _mm512_loadu_si512(&a[(tag_m_16x   )*16]);  // Load 2 rows with n=16
            matrixArray_5 = _mm512_loadu_si512(&a[(tag_m_16x+2 )*16]);  // Load 2 rows with n=16
            matrixArray_6 = _mm512_loadu_si512(&a[(tag_m_16x+4 )*16]);  // Load 2 rows with n=16
            matrixArray_7 = _mm512_loadu_si512(&a[(tag_m_16x+6 )*16]);  // Load 2 rows with n=16

            // interleave per 256 bits
            BF16_INTERLEAVE256_4x32(matrixArray)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x32(matrixArray)

            // Calculate the temp result for a..h[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x32(accum512, matrixArray, xArray)

            accum512_0 = _mm512_add_ps(accum512_0, accum512_1);
            accum512_0 = _mm512_permutexvar_ps(permutevar_idx, accum512_0);
            __m256 result256 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
            STORE8_COMPLETE_RESULT(result256, y+tag_m_16x)
            tag_m_16x += 8;
        }

        if (m - tag_m_16x > 3) {
            __m256i matrixArray256_0, matrixArray256_1, matrixArray256_2,  matrixArray256_3, \
                    matrixArray256_4, matrixArray256_5, matrixArray256_6,  matrixArray256_7;
            __m256i xArray256_0, xArray256_1, xArray256_2, xArray256_3;
            __m256  accum256_0, accum256_1;

            xArray256_0 = _mm512_castsi512_si256(xArray_0);
            xArray256_1 = _mm512_castsi512_si256(xArray_1);
            xArray256_2 = _mm512_castsi512_si256(xArray_2);
            xArray256_3 = _mm512_castsi512_si256(xArray_3);

            accum256_0 = _mm256_setzero_ps();
            accum256_1 = _mm256_setzero_ps();

            matrixArray_0 = _mm512_loadu_si512(&a[(tag_m_16x   )*16]);  // Load 2 rows with n=16
            matrixArray_1 = _mm512_loadu_si512(&a[(tag_m_16x+2 )*16]);  // Load 2 rows with n=16

            matrixArray256_0 = _mm512_castsi512_si256(matrixArray_0);
            matrixArray256_1 = _mm512_extracti32x8_epi32(matrixArray_0, 0x1);
            matrixArray256_2 = _mm512_castsi512_si256(matrixArray_1);
            matrixArray256_3 = _mm512_extracti32x8_epi32(matrixArray_1, 0x1);

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x16(matrixArray256)

            // Calculate the temp result for a..d[0:15]
            BF16_2STEP_INTERLEAVED_DOT_4x16(accum256, matrixArray256, xArray256)

            accum256_0 = _mm256_add_ps(accum256_0, accum256_1);
            __m128 result128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
            STORE4_COMPLETE_RESULT(result128, y+tag_m_16x)
            tag_m_16x += 4;
        }
    }

    if (tag_m_16x != m) {
        __m256i matrixArray256;
        __m256  accum256;
        __m128  accum128, tmp128;
        for (BLASLONG i = tag_m_16x; i < m; i++) {
            accum256 = _mm256_setzero_ps();
            matrixArray256 = _mm256_loadu_si256((__m256i *)&a[(i)*16]);       // Load 1 rows with n=16
            accum256 = _mm256_dpbf16_ps(accum256, (__m256bh) matrixArray256, (__m256bh) x256);
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf32x4_ps(accum256, 1));
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
            accum128 = _mm_add_ps(accum128, tmp128);
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
            accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * accum128[0] + beta * y[i];
#else
            y[i] = alpha * accum128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = accum128[0] * alpha;
#else
            y[i] = accum128[0];
#endif
#endif
        }
    }

    return 0;
}

// 8 rows parallel processing BF16 GEMV kernel for n>16 && lda effective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_8x16p_lda_alpha_beta(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_8x16p_lda_alpha_one(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_8x16p_lda_alpha(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_8x16p_lda(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_8x  = m & (~7);

    unsigned int load_mask_value = (((unsigned int)0xffffffff) >> (32-n));
    __mmask32 load_mask = *((__mmask32*) &load_mask_value);
    __m512i x512 = _mm512_maskz_loadu_epi16(load_mask, x);    // |x0|x1|x2|x3|x4|x5|x6|x7|x8|x9|x10|x11|x12|x13|x14|x15|...

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    __m512i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3,  matrixArray_4,  matrixArray_5,  matrixArray_6,  matrixArray_7, \
            matrixArray_8, matrixArray_9, matrixArray_10, matrixArray_11, matrixArray_12, matrixArray_13, matrixArray_14, matrixArray_15;
    __m512  accum512_0, accum512_1, accum512_2, accum512_3;
    __m256  accum256;
    __m128  accum128;

    if (tag_m_8x > 0) {
        __m512i xArray_0, xArray_1, xArray_2, xArray_3;

        __m512i M512_EPI32_4 = _mm512_set1_epi32(4);
        __m512i idx_base_0   = _mm512_set_epi32(27, 26, 25, 24, 11, 10,  9,  8, 19, 18, 17, 16,  3,  2,  1,  0);
        __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_4);

        // Prepare X with 2-step interleave way
        xArray_0 = x512;
        BF16_INTERLEAVE_1x32(xArray)

        for (BLASLONG idx_m = 0; idx_m < tag_m_8x; idx_m+=8) {
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load 8 rows from matrix
            BF16_MATRIX_MASKZ_LOAD_8x32(matrixArray, a, lda, idx_m, 0, load_mask)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_8x32(matrixArray)

            // Calculate the temp result for a..h[0:31]
            BF16_2STEP_INTERLEAVED_DOT_8x32(accum512, matrixArray, xArray)

            // Reorder and add up the final result
            accum512_2 = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_1);
            accum512_3 = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_1);
            accum512_2 = _mm512_add_ps(accum512_2, accum512_3);
            accum256   = _mm256_add_ps(_mm512_castps512_ps256(accum512_2), _mm512_extractf32x8_ps(accum512_2, 1));
            STORE8_COMPLETE_RESULT(accum256, y+idx_m)
        }

        if (m - tag_m_8x > 3) {
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();

            // Load 4 rows from matrix
            BF16_MATRIX_MASKZ_LOAD_4x32(matrixArray, a, lda, tag_m_8x, 0, load_mask)

            // 2-step interleave for matrix
            BF16_INTERLEAVE_4x32(matrixArray)

            // Calculate the temp result for a..d[0:31]
            BF16_2STEP_INTERLEAVED_DOT_4x32(accum512, matrixArray, xArray)

            accum512_0 = _mm512_add_ps(accum512_0, accum512_1);
            accum256 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf32x4_ps(accum256, 1));
            STORE4_COMPLETE_RESULT(accum128, y+tag_m_8x)
            tag_m_8x += 4;
        }
    }

    if (tag_m_8x != m) {
        __m128  tmp128;
        for (BLASLONG i = tag_m_8x; i < m; i++) {
            accum512_0 = _mm512_setzero_ps();
            matrixArray_0 = _mm512_maskz_loadu_epi16(load_mask, &a[(i)*lda]);       // Load 1 rows with n=16
            accum512_0 = _mm512_dpbf16_ps(accum512_0, (__m512bh) matrixArray_0, (__m512bh) x512);
            accum256 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf32x4_ps(accum256, 1));
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
            accum128 = _mm_add_ps(accum128, tmp128);
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
            accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * accum128[0] + beta * y[i];
#else
            y[i] = alpha * accum128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = accum128[0] * alpha;
#else
            y[i] = accum128[0];
#endif
#endif
        }
    }

    return 0;
}

// 8 rows parallel processing BF16 GEMV kernel for big N && lda effective scenario (process before interleave)
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_1x128_lda_direct_alpha_beta(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_1x128_lda_direct_alpha_one(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_1x128_lda_direct_alpha(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_1x128_lda_direct(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_8x   = m & (~7);
    BLASLONG tag_n_32x  = n & (~31);
    BLASLONG tag_n_128x = n & (~127);

    __m512 accum512_bridge[16];
    __m512 accum512_t_0, accum512_t_1, accum512_t_2, accum512_t_3;
    __m256 accum256_0;
    __m128 accum128;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    __m512i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3;
    __m512i xArray_0, xArray_1, xArray_2, xArray_3;

    unsigned int tail_mask_value = (((unsigned int)0xffffffff) >> (32-(n&31)));
    __mmask32 tail_mask = *((__mmask32*) &tail_mask_value);

    __m512i M512_EPI32_4 = _mm512_set1_epi32(4);
    __m512i idx_base_0   = _mm512_set_epi32(27, 26, 25, 24, 11, 10,  9,  8, 19, 18, 17, 16,  3,  2,  1,  0);
    __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_4);

    if (tag_m_8x > 0) {
        for (BLASLONG idx_m = 0; idx_m < tag_m_8x; idx_m+=8) {
            for (int j = idx_m; j < idx_m + 8; j++) {
                accum512_t_0 = _mm512_setzero_ps();
                accum512_t_1 = _mm512_setzero_ps();
                accum512_t_2 = _mm512_setzero_ps();
                accum512_t_3 = _mm512_setzero_ps();
                /* Processing the main chunk with 128-elements per round */
                for (long idx_n = 0; idx_n < tag_n_128x; idx_n += 128) {
                    BF16_MATRIX_LOAD_1x32(matrixArray_0, a, lda, j, idx_n +  0)
                    BF16_MATRIX_LOAD_1x32(matrixArray_1, a, lda, j, idx_n + 32)
                    BF16_MATRIX_LOAD_1x32(matrixArray_2, a, lda, j, idx_n + 64)
                    BF16_MATRIX_LOAD_1x32(matrixArray_3, a, lda, j, idx_n + 96)

                    BF16_VECTOR_LOAD_1x32(xArray_0, x, idx_n + 0)
                    BF16_VECTOR_LOAD_1x32(xArray_1, x, idx_n + 32)
                    BF16_VECTOR_LOAD_1x32(xArray_2, x, idx_n + 64)
                    BF16_VECTOR_LOAD_1x32(xArray_3, x, idx_n + 96)

                    BF16_DOT_1x32(accum512_t_0, matrixArray_0, xArray_0)
                    BF16_DOT_1x32(accum512_t_1, matrixArray_1, xArray_1)
                    BF16_DOT_1x32(accum512_t_2, matrixArray_2, xArray_2)
                    BF16_DOT_1x32(accum512_t_3, matrixArray_3, xArray_3)
                }

                /* Processing the remaining <128 chunk with 32-elements per round */
                for (long idx_n = tag_n_128x; idx_n < tag_n_32x; idx_n += 32) {
                    BF16_MATRIX_LOAD_1x32(matrixArray_0, a, lda, j, idx_n)
                    BF16_VECTOR_LOAD_1x32(xArray_0, x, idx_n)
                    BF16_DOT_1x32(accum512_t_0, matrixArray_0, xArray_0)
                }

                /* Processing the remaining <32 chunk with masked 32-elements processing */
                if ((n&31) != 0) {
                    BF16_MATRIX_MASKZ_LOAD_1x32(matrixArray_0, a, lda, j, tag_n_32x, tail_mask)
                    BF16_VECTOR_MASKZ_LOAD_1x32(xArray_0, x, tag_n_32x, tail_mask)
                    BF16_DOT_1x32(accum512_t_2, matrixArray_0, xArray_0)
                }

                /* Accumulate the 4 registers into 1 register */
                accum512_t_0 = _mm512_add_ps(accum512_t_0, accum512_t_1);
                accum512_t_2 = _mm512_add_ps(accum512_t_2, accum512_t_3);
                accum512_t_0 = _mm512_add_ps(accum512_t_0, accum512_t_2);

                // Temply save the result into a ZMM
                accum512_bridge[j-idx_m] = accum512_t_0;
            }

            FP32_INTERLEAVE_8x16_ARRAY(accum512_bridge)
            FP32_ACCUM2_8x16_ARRAY(accum512_bridge)
            accum512_bridge[1] = _mm512_permutex2var_ps(accum512_bridge[0], idx_base_0, accum512_bridge[4]);
            accum512_bridge[2] = _mm512_permutex2var_ps(accum512_bridge[0], idx_base_1, accum512_bridge[4]);
            accum512_bridge[1] = _mm512_add_ps(accum512_bridge[1], accum512_bridge[2]);
            accum256_0 = _mm256_add_ps(_mm512_castps512_ps256(accum512_bridge[1]), _mm512_extractf32x8_ps(accum512_bridge[1], 1));
            STORE8_COMPLETE_RESULT(accum256_0, y+idx_m)
        }
    }

    if (tag_m_8x != m) {
        __m128  tmp128;
        for (BLASLONG j = tag_m_8x; j < m; j++) {
            accum512_t_0 = _mm512_setzero_ps();
            accum512_t_1 = _mm512_setzero_ps();
            accum512_t_2 = _mm512_setzero_ps();
            accum512_t_3 = _mm512_setzero_ps();
            /* Processing the main chunk with 128-elements per round */
            for (long idx_n = 0; idx_n < tag_n_128x; idx_n += 128) {
                BF16_MATRIX_LOAD_1x32(matrixArray_0, a, lda, j, idx_n +  0)
                BF16_MATRIX_LOAD_1x32(matrixArray_1, a, lda, j, idx_n + 32)
                BF16_MATRIX_LOAD_1x32(matrixArray_2, a, lda, j, idx_n + 64)
                BF16_MATRIX_LOAD_1x32(matrixArray_3, a, lda, j, idx_n + 96)

                BF16_VECTOR_LOAD_1x32(xArray_0, x, idx_n + 0)
                BF16_VECTOR_LOAD_1x32(xArray_1, x, idx_n + 32)
                BF16_VECTOR_LOAD_1x32(xArray_2, x, idx_n + 64)
                BF16_VECTOR_LOAD_1x32(xArray_3, x, idx_n + 96)

                BF16_DOT_1x32(accum512_t_0, matrixArray_0, xArray_0)
                BF16_DOT_1x32(accum512_t_1, matrixArray_1, xArray_1)
                BF16_DOT_1x32(accum512_t_2, matrixArray_2, xArray_2)
                BF16_DOT_1x32(accum512_t_3, matrixArray_3, xArray_3)
            }

            /* Processing the remaining <128 chunk with 32-elements per round */
            for (long idx_n = tag_n_128x; idx_n < tag_n_32x; idx_n += 32) {
                BF16_MATRIX_LOAD_1x32(matrixArray_0, a, lda, j, idx_n)
                BF16_VECTOR_LOAD_1x32(xArray_0, x, idx_n)
                BF16_DOT_1x32(accum512_t_0, matrixArray_0, xArray_0)
            }

            /* Processing the remaining <32 chunk with masked 32-elements processing */
            if ((n&31) != 0) {
                BF16_MATRIX_MASKZ_LOAD_1x32(matrixArray_0, a, lda, j, tag_n_32x, tail_mask)
                BF16_VECTOR_MASKZ_LOAD_1x32(xArray_0, x, tag_n_32x, tail_mask)
                BF16_DOT_1x32(accum512_t_2, matrixArray_0, xArray_0)
            }

            /* Accumulate the 4 registers into 1 register */
            accum512_t_0 = _mm512_add_ps(accum512_t_0, accum512_t_1);
            accum512_t_2 = _mm512_add_ps(accum512_t_2, accum512_t_3);
            accum512_t_0 = _mm512_add_ps(accum512_t_0, accum512_t_2);

            accum256_0 = _mm256_add_ps(_mm512_castps512_ps256(accum512_t_0), _mm512_extractf32x8_ps(accum512_t_0, 1));
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
            accum128 = _mm_add_ps(accum128, tmp128);
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
            accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[j] = alpha * accum128[0] + beta * y[j];
#else
            y[j] = alpha * accum128[0] + y[j];
#endif
#else
#ifndef ONE_ALPHA
            y[j] = accum128[0] * alpha;
#else
            y[j] = accum128[0];
#endif
#endif
        }
    }

    return 0;
}

// 8 rows parallel processing BF16 GEMV kernel for n=32 && lda effective scenario (process before interleave)
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_8x32_lda_direct_alpha_beta(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_8x32_lda_direct_alpha_one(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_8x32_lda_direct_alpha(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_8x32_lda_direct(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_8x  = m & (~7);
    BLASLONG tag_n_32x = n & (~31);

    __m512 accum512_0, accum512_1, accum512_2, accum512_3, accum512_4, accum512_5, accum512_6, accum512_7, \
           accum512_8, accum512_9, accum512_10, accum512_11, accum512_12, accum512_13, accum512_14, accum512_15;
    __m256 accum256_0;
    __m128 accum128;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_set1_ps(beta);
#endif
#endif

    __m512i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3,  matrixArray_4,  matrixArray_5,  matrixArray_6,  matrixArray_7;
    __m512i xArray_0;

    unsigned int tail_mask_value = (((unsigned int)0xffffffff) >> (32-(n&31)));
    __mmask32 tail_mask = *((__mmask32*) &tail_mask_value);

    if (tag_m_8x > 0) {
        __m512i M512_EPI32_4 = _mm512_set1_epi32(4);
        __m512i idx_base_0   = _mm512_set_epi32(27, 26, 25, 24, 11, 10,  9,  8, 19, 18, 17, 16,  3,  2,  1,  0);
        __m512i idx_base_1   = _mm512_add_epi32(idx_base_0, M512_EPI32_4);

        for (BLASLONG idx_m = 0; idx_m < tag_m_8x; idx_m+=8) {
            accum512_0 = _mm512_setzero_ps();
            accum512_1 = _mm512_setzero_ps();
            accum512_2 = _mm512_setzero_ps();
            accum512_3 = _mm512_setzero_ps();
            accum512_4 = _mm512_setzero_ps();
            accum512_5 = _mm512_setzero_ps();
            accum512_6 = _mm512_setzero_ps();
            accum512_7 = _mm512_setzero_ps();

            for (BLASLONG idx_n = 0; idx_n < tag_n_32x; idx_n+=32) {
                // Load 8 rows from matrix
                BF16_MATRIX_LOAD_8x32(matrixArray, a, lda, idx_m, idx_n)

                // Load x
                BF16_VECTOR_LOAD_1x32(xArray_0, x, idx_n)

                // Calculate the temp result for a..h[0:31]
                BF16_DOT_8x32(accum512, matrixArray, xArray_0)
            }

            if (tag_n_32x != n) {         // Go with masked 512
                // Load 8 rows from matrix
                BF16_MATRIX_MASKZ_LOAD_8x32(matrixArray, a, lda, idx_m, tag_n_32x, tail_mask)

                // Load x
                BF16_VECTOR_MASKZ_LOAD_1x32(xArray_0, x, tag_n_32x, tail_mask)

                // Calculate the temp result for a..h[0:31]
                BF16_DOT_8x32(accum512, matrixArray, xArray_0)
            }

            // 2-step interleave for FP32 regsiter array
            FP32_INTERLEAVE_8x16(accum512)

            // Accumulate the 2 batch of registers into 2 register (0 and 4)
            FP32_ACCUM2_8x16(accum512)

            accum512_1 = _mm512_permutex2var_ps(accum512_0, idx_base_0, accum512_4);
            accum512_2 = _mm512_permutex2var_ps(accum512_0, idx_base_1, accum512_4);
            accum512_1 = _mm512_add_ps(accum512_1, accum512_2);
            accum256_0 = _mm256_add_ps(_mm512_castps512_ps256(accum512_1), _mm512_extractf32x8_ps(accum512_1, 1));
            STORE8_COMPLETE_RESULT(accum256_0, y+idx_m)
        }
    }

    if (tag_m_8x != m) {
        __m128  tmp128;
        for (BLASLONG i = tag_m_8x; i < m; i++) {
            accum512_0 = _mm512_setzero_ps();
            for (BLASLONG idx_n = 0; idx_n < tag_n_32x; idx_n+=32) {
                // Load 32 elements from matrix
                BF16_MATRIX_LOAD_1x32(matrixArray_0, a, lda, i, idx_n)

                // Load 32 elements from x
                BF16_VECTOR_LOAD_1x32(xArray_0, x, idx_n)

                // Calculate and accumulate the temp result
                BF16_DOT_1x32(accum512_0, matrixArray_0, xArray_0)
            }

            if (tag_n_32x != n) {
                // Load tail elements from matrix
                BF16_MATRIX_MASKZ_LOAD_1x32(matrixArray_0, a, lda, i, tag_n_32x, tail_mask)

                // Load 32 elements from x
                BF16_VECTOR_MASKZ_LOAD_1x32(xArray_0, x, tag_n_32x, tail_mask)

                // Calculate and accumulate the temp result
                BF16_DOT_1x32(accum512_0, matrixArray_0, xArray_0)
            }

            accum256_0 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
            accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
            accum128 = _mm_add_ps(accum128, tmp128);
            tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
            accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
            y[i] = alpha * accum128[0] + beta * y[i];
#else
            y[i] = alpha * accum128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
            y[i] = accum128[0] * alpha;
#else
            y[i] = accum128[0];
#endif
#endif
        }
    }

    return 0;
}

// 8 rows parallel processing BF16 GEMV kernel for n<16 && lda effective scenario
#ifndef ZERO_BETA
#ifndef ONE_BETA
static int sbgemv_kernel_8x16m_lda_alpha_beta(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#else
static int sbgemv_kernel_8x16m_lda_alpha_one(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
#endif
#else
#ifndef ONE_ALPHA
static int sbgemv_kernel_8x16m_lda_alpha(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#else
static int sbgemv_kernel_8x16m_lda(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float *y)
#endif
#endif
{
    BLASLONG tag_m_8x  = m & (~7);

    __m256i matrixArray_0, matrixArray_1, matrixArray_2,  matrixArray_3,  matrixArray_4,  matrixArray_5,  matrixArray_6,  matrixArray_7;
    __m256i xArray256;

    // Keep align with other kernels and macro definition, the high 256bit is never used
#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_castps256_ps512(_mm256_set1_ps(alpha));
#endif
#ifndef ZERO_BETA
#ifndef ONE_BETA
    __m512  BETAVECTOR  = _mm512_castps256_ps512(_mm256_set1_ps(beta));
#endif
#endif

    __m256  accum256_0, accum256_1, accum256_2, accum256_3, accum256_4, accum256_5, accum256_6, accum256_7, \
            accum256_8, accum256_9, accum256_10, accum256_11, accum256_12, accum256_13, accum256_14, accum256_15;

    __m256i M256_EPI32_4 = _mm256_set1_epi32(4);
    __m256i idx_base_0   = _mm256_set_epi32(11, 10,  9,  8,  3,  2,  1,  0);
    __m256i idx_base_1   = _mm256_add_epi32(idx_base_0, M256_EPI32_4);

    unsigned short load_mask_value = (((unsigned short)0xffff) >> (16-n));
    __mmask16 load_mask = *((__mmask16*) &load_mask_value);

    if (n == 16) {
        BF16_VECTOR_LOAD_1x16(xArray256, x, 0)
    } else {
        BF16_VECTOR_MASKZ_LOAD_1x16(xArray256, x, 0, load_mask)
    }

    if (n == 16) {
        for (BLASLONG idx_m = 0; idx_m < tag_m_8x; idx_m+=8) {
            accum256_0 = _mm256_setzero_ps();
            accum256_1 = _mm256_setzero_ps();
            accum256_2 = _mm256_setzero_ps();
            accum256_3 = _mm256_setzero_ps();
            accum256_4 = _mm256_setzero_ps();
            accum256_5 = _mm256_setzero_ps();
            accum256_6 = _mm256_setzero_ps();
            accum256_7 = _mm256_setzero_ps();

            BF16_MATRIX_LOAD_8x16(matrixArray, a, lda, idx_m, 0)

            BF16_DOT_8x16(accum256, matrixArray, xArray256)

            // 2-step interleave for FP32 regsiter array
            FP32_INTERLEAVE_8x8(accum256)

            // Accumulate the 2 batch of registers into 2 register (0 and 4)
            FP32_ACCUM2_8x8(accum256)

            accum256_1 = _mm256_permutex2var_ps(accum256_0, idx_base_0, accum256_4);
            accum256_2 = _mm256_permutex2var_ps(accum256_0, idx_base_1, accum256_4);
            accum256_1 = _mm256_add_ps(accum256_1, accum256_2);

            STORE8_COMPLETE_RESULT(accum256_1, y+idx_m)
        }

        if (tag_m_8x != m) {
            __m128  accum128, tmp128;
            for (BLASLONG i = tag_m_8x; i < m; i++) {
                accum256_0 = _mm256_setzero_ps();
                matrixArray_0 = _mm256_loadu_si256((__m256i *)&a[(i)*lda]);       // Load 1 rows with n=16
                accum256_0 = _mm256_dpbf16_ps(accum256_0, (__m256bh) matrixArray_0, (__m256bh) xArray256);
                accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
                tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
                accum128 = _mm_add_ps(accum128, tmp128);
                tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
                accum128 = _mm_add_ps(accum128, tmp128);
                y[i] += accum128[0] * alpha;
            }
        }
    } else {
        for (BLASLONG idx_m = 0; idx_m < tag_m_8x; idx_m+=8) {
            accum256_0 = _mm256_setzero_ps();
            accum256_1 = _mm256_setzero_ps();
            accum256_2 = _mm256_setzero_ps();
            accum256_3 = _mm256_setzero_ps();
            accum256_4 = _mm256_setzero_ps();
            accum256_5 = _mm256_setzero_ps();
            accum256_6 = _mm256_setzero_ps();
            accum256_7 = _mm256_setzero_ps();

            BF16_MATRIX_MASKZ_LOAD_8x16(matrixArray, a, lda, idx_m, 0, load_mask)

            BF16_DOT_8x16(accum256, matrixArray, xArray256)

            // 2-step interleave for FP32 regsiter array
            FP32_INTERLEAVE_8x8(accum256)

            // Accumulate the 2 batch of registers into 2 register (0 and 4)
            FP32_ACCUM2_8x8(accum256)

            accum256_1 = _mm256_permutex2var_ps(accum256_0, idx_base_0, accum256_4);
            accum256_2 = _mm256_permutex2var_ps(accum256_0, idx_base_1, accum256_4);
            accum256_1 = _mm256_add_ps(accum256_1, accum256_2);

            STORE8_COMPLETE_RESULT(accum256_1, y+idx_m)
        }

        if (tag_m_8x != m) {
            __m128  accum128, tmp128;
            for (BLASLONG i = tag_m_8x; i < m; i++) {
                accum256_0 = _mm256_setzero_ps();
                matrixArray_0 = _mm256_maskz_loadu_epi16(load_mask, &a[(i)*lda]);       // Load 1 rows with n=16
                accum256_0 = _mm256_dpbf16_ps(accum256_0, (__m256bh) matrixArray_0, (__m256bh) xArray256);
                accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256_0), _mm256_extractf32x4_ps(accum256_0, 1));
                tmp128 = _mm_shuffle_ps(accum128, accum128, 0x0e);
                accum128 = _mm_add_ps(accum128, tmp128);
                tmp128 = _mm_shuffle_ps(accum128, accum128, 0x01);
                accum128 = _mm_add_ps(accum128, tmp128);
#ifndef ZERO_BETA
#ifndef ONE_BETA
                y[i] = alpha * accum128[0] + beta * y[i];
#else
                y[i] = alpha * accum128[0] + y[i];
#endif
#else
#ifndef ONE_ALPHA
                y[i] = accum128[0] * alpha;
#else
                y[i] = accum128[0];
#endif
#endif
            }
        }
    }

    return 0;
}
