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

/* need a new enough GCC for avx512 support */
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   >= 10 && defined(__AVX512BF16__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_SBDOT_ACCL_KERNEL 1
#include "common.h"
#include <immintrin.h>

static float sbdot_accl_kernel(BLASLONG n, bfloat16 *x, bfloat16 *y)
{
    __m128 accum128   = _mm_setzero_ps();
    if (n> 127) { /* n range from 128 to inf. */
        long tail_index_32  = n&(~31);
        long tail_index_128 = n&(~127);
        unsigned int tail_mask_uint = (((unsigned int)0xffffffff) >> (32-(n&31)));
        __mmask32 tail_mask = *((__mmask32*) &tail_mask_uint);

        __m512 accum512_0 = _mm512_setzero_ps();
        __m512 accum512_1 = _mm512_setzero_ps();
        __m512 accum512_2 = _mm512_setzero_ps();
        __m512 accum512_3 = _mm512_setzero_ps();

        /* Processing the main chunk with 128-elements per round */
        for (long i = 0; i < tail_index_128; i += 128) {
            accum512_0 = _mm512_dpbf16_ps(accum512_0, (__m512bh) _mm512_loadu_si512(&x[i+ 0]), (__m512bh) _mm512_loadu_si512(&y[i+ 0]));
            accum512_1 = _mm512_dpbf16_ps(accum512_1, (__m512bh) _mm512_loadu_si512(&x[i+32]), (__m512bh) _mm512_loadu_si512(&y[i+32]));
            accum512_2 = _mm512_dpbf16_ps(accum512_2, (__m512bh) _mm512_loadu_si512(&x[i+64]), (__m512bh) _mm512_loadu_si512(&y[i+64]));
            accum512_3 = _mm512_dpbf16_ps(accum512_3, (__m512bh) _mm512_loadu_si512(&x[i+96]), (__m512bh) _mm512_loadu_si512(&y[i+96]));
        }

        /* Processing the remaining <128 chunk with 32-elements per round */
        for (long j = tail_index_128; j < tail_index_32; j += 32) {
            accum512_0 = _mm512_dpbf16_ps(accum512_0, (__m512bh) _mm512_loadu_si512(&x[j]), (__m512bh) _mm512_loadu_si512(&y[j]));
        }

        /* Processing the remaining <32 chunk with masked 32-elements processing */
        if ((n&31) != 0) {
            accum512_2 = _mm512_dpbf16_ps(accum512_2,
                                          (__m512bh) _mm512_maskz_loadu_epi16(tail_mask, &x[tail_index_32]),
                                          (__m512bh) _mm512_maskz_loadu_epi16(tail_mask, &y[tail_index_32]));
        }

        /* Accumulate the 4 registers into 1 register */
        accum512_0 = _mm512_add_ps(accum512_0, accum512_1);
        accum512_2 = _mm512_add_ps(accum512_2, accum512_3);
        accum512_0 = _mm512_add_ps(accum512_0, accum512_2);

        __m256 accum256 = _mm256_add_ps(_mm512_castps512_ps256(accum512_0), _mm512_extractf32x8_ps(accum512_0, 1));
        accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf128_ps(accum256, 1));
    } else if (n > 31) { /* n range from 32 to 127 */
        /* Processing <128 chunk with 32-elements per round */
        __m256 accum256   = _mm256_setzero_ps();
        __m256 accum256_1 = _mm256_setzero_ps();
        int tail_index_32  = n&(~31);
        for (int j = 0; j < tail_index_32; j += 32) {
            accum256   = _mm256_dpbf16_ps(accum256,   (__m256bh) _mm256_loadu_si256((__m256i *)&x[j+ 0]), (__m256bh) _mm256_loadu_si256((__m256i *)&y[j+ 0]));
            accum256_1 = _mm256_dpbf16_ps(accum256_1, (__m256bh) _mm256_loadu_si256((__m256i *)&x[j+16]), (__m256bh) _mm256_loadu_si256((__m256i *)&y[j+16]));
        }
        accum256 = _mm256_add_ps(accum256, accum256_1);

        /* Processing the remaining <32 chunk with 16-elements processing */
        if ((n&16) != 0) {
            accum256 = _mm256_dpbf16_ps(accum256, (__m256bh) _mm256_loadu_si256((__m256i *)&x[tail_index_32]), (__m256bh) _mm256_loadu_si256((__m256i *)&y[tail_index_32]));
        }
        accum128 = _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf128_ps(accum256, 1));

        /* Processing the remaining <16 chunk with 8-elements processing */
        if ((n&8) != 0) {
            int tail_index_16  = n&(~15);
            accum128 = _mm_dpbf16_ps(accum128, (__m128bh) _mm_loadu_si128((__m128i *)&x[tail_index_16]), (__m128bh) _mm_loadu_si128((__m128i *)&y[tail_index_16]));
        }

        /* Processing the remaining <8 chunk with masked 8-elements processing */
        if ((n&7) != 0) {
            unsigned char tail_mask_uint = (((unsigned char)0xff) >> (8-(n&7)));
            __mmask8 tail_mask = *((__mmask8*) &tail_mask_uint);
            int tail_index_8   = n&(~7);
            accum128 = _mm_dpbf16_ps(accum128,
                                     (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &x[tail_index_8]),
                                     (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &y[tail_index_8]));
        }
    } else if (n > 15) { /* n range from 16 to 31 */
        /* Processing <32 chunk with 16-elements processing */
        __m256 accum256   = _mm256_setzero_ps();
        accum256 = _mm256_dpbf16_ps(accum256, (__m256bh) _mm256_loadu_si256((__m256i *)&x[0]), (__m256bh) _mm256_loadu_si256((__m256i *)&y[0]));
        accum128 += _mm_add_ps(_mm256_castps256_ps128(accum256), _mm256_extractf128_ps(accum256, 1));

        /* Processing the remaining <16 chunk with 8-elements processing */
        if ((n&8) != 0) {
            int tail_index_16  = n&(~15);
            accum128 = _mm_dpbf16_ps(accum128, (__m128bh) _mm_loadu_si128((__m128i *)&x[tail_index_16]), (__m128bh) _mm_loadu_si128((__m128i *)&y[tail_index_16]));
        }

        /* Processing the remaining <8 chunk with masked 8-elements processing */
        if ((n&7) != 0) {
            unsigned char tail_mask_uint = (((unsigned char)0xff) >> (8-(n&7)));
            __mmask8 tail_mask = *((__mmask8*) &tail_mask_uint);
            int tail_index_8   = n&(~7);
            accum128 = _mm_dpbf16_ps(accum128,
                                     (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &x[tail_index_8]),
                                     (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &y[tail_index_8]));
        }
    } else if (n > 7) { /* n range from 8 to 15 */
        /* Processing <16 chunk with 8-elements processing */
        accum128 = _mm_dpbf16_ps(accum128, (__m128bh) _mm_loadu_si128((__m128i *)&x[0]), (__m128bh) _mm_loadu_si128((__m128i *)&y[0]));

        /* Processing the remaining <8 chunk with masked 8-elements processing */
        if ((n&7) != 0) {
            unsigned char tail_mask_uint = (((unsigned char)0xff) >> (8-(n&7)));
            __mmask8 tail_mask = *((__mmask8*) &tail_mask_uint);
            int tail_index_8   = n&(~7);
            accum128 = _mm_dpbf16_ps(accum128,
                                     (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &x[tail_index_8]),
                                     (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &y[tail_index_8]));
        }
    } else { /* n range from 1 to 7 */
        unsigned char tail_mask_uint = (((unsigned char)0xff) >> (8-(n&7)));
        __mmask8 tail_mask = *((__mmask8*) &tail_mask_uint);
        accum128 = _mm_dpbf16_ps(accum128,
                                 (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &x[0]),
                                 (__m128bh) _mm_maskz_loadu_epi16(tail_mask, &y[0]));
    }

    /* Add up the 4 elements into lowest entry */
    __m128 accum128_1 = _mm_shuffle_ps(accum128, accum128, 14);
    accum128 = _mm_add_ps(accum128, accum128_1);
    accum128_1 = _mm_shuffle_ps(accum128, accum128, 1);
    accum128 = _mm_add_ps(accum128, accum128_1);

    return accum128[0];
}

#endif
