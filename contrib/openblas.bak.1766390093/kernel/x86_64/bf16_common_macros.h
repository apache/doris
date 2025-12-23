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
#ifndef __BF16_COMMON_MACROS
#define __BF16_COMMON_MACROS

#include <immintrin.h>

#define _MM512_BROADCASTD_EPI32(addr, zmm)             \
                    __asm__ ("vpbroadcastd (%1), %0;"  \
                            : "=v" (zmm)               \
                            : "r"  (addr) )

#define PREFETCH_T0(addr)             \
                    __asm__ ("prefetcht0 (%0);"  \
                            :                    \
                            : "r"  (addr) )

#define EXTRACT_LOW_256_FROM_512_2X(reg256, reg512)   \
    reg256##_0 = _mm512_castps512_ps256(reg512##_0);  \
    reg256##_1 = _mm512_castps512_ps256(reg512##_1);


#define BF16_MATRIX_LOAD_8x32(regArray, a, lda, idx_m, idx_n)      \
    regArray##_0 = _mm512_loadu_si512(&a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm512_loadu_si512(&a[(idx_m+1)*lda + idx_n]);  \
    regArray##_2 = _mm512_loadu_si512(&a[(idx_m+2)*lda + idx_n]);  \
    regArray##_3 = _mm512_loadu_si512(&a[(idx_m+3)*lda + idx_n]);  \
    regArray##_4 = _mm512_loadu_si512(&a[(idx_m+4)*lda + idx_n]);  \
    regArray##_5 = _mm512_loadu_si512(&a[(idx_m+5)*lda + idx_n]);  \
    regArray##_6 = _mm512_loadu_si512(&a[(idx_m+6)*lda + idx_n]);  \
    regArray##_7 = _mm512_loadu_si512(&a[(idx_m+7)*lda + idx_n]);


#define BF16_MATRIX_LOAD_8x16(regArray, a, lda, idx_m, idx_n)      \
    regArray##_0 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+0)*lda + idx_n]));  \
    regArray##_1 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+1)*lda + idx_n]));  \
    regArray##_2 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+2)*lda + idx_n]));  \
    regArray##_3 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+3)*lda + idx_n]));  \
    regArray##_4 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+4)*lda + idx_n]));  \
    regArray##_5 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+5)*lda + idx_n]));  \
    regArray##_6 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+6)*lda + idx_n]));  \
    regArray##_7 = _mm256_loadu_si256((__m256i *)(&a[(idx_m+7)*lda + idx_n]));


#define BF16_MATRIX_LOAD_8x8(regArray, a, lda, idx_m, idx_n)    \
    regArray##_0 = _mm_loadu_si128((__m128i *)(&a[(idx_m+0)*lda + idx_n]));  \
    regArray##_1 = _mm_loadu_si128((__m128i *)(&a[(idx_m+1)*lda + idx_n]));  \
    regArray##_2 = _mm_loadu_si128((__m128i *)(&a[(idx_m+2)*lda + idx_n]));  \
    regArray##_3 = _mm_loadu_si128((__m128i *)(&a[(idx_m+3)*lda + idx_n]));  \
    regArray##_4 = _mm_loadu_si128((__m128i *)(&a[(idx_m+4)*lda + idx_n]));  \
    regArray##_5 = _mm_loadu_si128((__m128i *)(&a[(idx_m+5)*lda + idx_n]));  \
    regArray##_6 = _mm_loadu_si128((__m128i *)(&a[(idx_m+6)*lda + idx_n]));  \
    regArray##_7 = _mm_loadu_si128((__m128i *)(&a[(idx_m+7)*lda + idx_n]));


#define BF16_MATRIX_LOAD_1x32(regArray, a, lda, idx_m, idx_n)       \
    regArray = _mm512_loadu_si512(&a[idx_m*lda + idx_n]);


#define BF16_MATRIX_MASKZ_LOAD_8x32(regArray, a, lda, idx_m, idx_n, mask)      \
    regArray##_0 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+1)*lda + idx_n]);  \
    regArray##_2 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+2)*lda + idx_n]);  \
    regArray##_3 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+3)*lda + idx_n]);  \
    regArray##_4 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+4)*lda + idx_n]);  \
    regArray##_5 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+5)*lda + idx_n]);  \
    regArray##_6 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+6)*lda + idx_n]);  \
    regArray##_7 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+7)*lda + idx_n]);


#define BF16_MATRIX_MASKZ_LOAD_8x16(regArray, a, lda, idx_m, idx_n, mask)      \
    regArray##_0 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+1)*lda + idx_n]);  \
    regArray##_2 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+2)*lda + idx_n]);  \
    regArray##_3 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+3)*lda + idx_n]);  \
    regArray##_4 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+4)*lda + idx_n]);  \
    regArray##_5 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+5)*lda + idx_n]);  \
    regArray##_6 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+6)*lda + idx_n]);  \
    regArray##_7 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+7)*lda + idx_n]);


#define BF16_MATRIX_MASKZ_LOAD_8x8(regArray, a, lda, idx_m, idx_n, mask)    \
    regArray##_0 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+1)*lda + idx_n]);  \
    regArray##_2 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+2)*lda + idx_n]);  \
    regArray##_3 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+3)*lda + idx_n]);  \
    regArray##_4 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+4)*lda + idx_n]);  \
    regArray##_5 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+5)*lda + idx_n]);  \
    regArray##_6 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+6)*lda + idx_n]);  \
    regArray##_7 = _mm_maskz_loadu_epi16(mask, &a[(idx_m+7)*lda + idx_n]);


#define BF16_MATRIX_MASKZ_LOAD_4x32(regArray, a, lda, idx_m, idx_n, mask)      \
    regArray##_0 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+1)*lda + idx_n]);  \
    regArray##_2 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+2)*lda + idx_n]);  \
    regArray##_3 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+3)*lda + idx_n]);


#define BF16_MATRIX_MASKZ_LOAD_4x16(regArray, a, lda, idx_m, idx_n, mask)      \
    regArray##_0 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+1)*lda + idx_n]);  \
    regArray##_2 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+2)*lda + idx_n]);  \
    regArray##_3 = _mm256_maskz_loadu_epi16(mask, &a[(idx_m+3)*lda + idx_n]);


#define BF16_MATRIX_MASKZ_LOAD_8x32_2(regArray, a, lda, idx_m, idx_n, mask)    \
    regArray##_0 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+2)*lda + idx_n]);  \
    regArray##_2 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+4)*lda + idx_n]);  \
    regArray##_3 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+6)*lda + idx_n]);  \
    regArray##_4 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+8)*lda + idx_n]);  \
    regArray##_5 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+10)*lda + idx_n]);  \
    regArray##_6 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+12)*lda + idx_n]);  \
    regArray##_7 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+14)*lda + idx_n]);


#define BF16_MATRIX_MASKZ_LOAD_4x32_2(regArray, a, lda, idx_m, idx_n, mask)    \
    regArray##_0 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+0)*lda + idx_n]);  \
    regArray##_1 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+2)*lda + idx_n]);  \
    regArray##_2 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+4)*lda + idx_n]);  \
    regArray##_3 = _mm512_maskz_loadu_epi16(mask, &a[(idx_m+6)*lda + idx_n]);

#define BF16_MATRIX_MASKZ_LOAD_1x32(regArray, a, lda, idx_m, idx_n, mask)      \
    regArray = _mm512_maskz_loadu_epi16(mask, &a[idx_m*lda + idx_n]);

#define BF16_VECTOR_LOAD_1x32(reg, x, idx_n)     \
    reg = _mm512_loadu_si512(x + idx_n);


#define BF16_VECTOR_LOAD_1x16(reg, x, idx_n)     \
    reg = _mm256_loadu_si256((__m256i *)(x + idx_n));


#define BF16_VECTOR_LOAD_1x8(reg, x, idx_n)      \
    reg = _mm_loadu_si128((__m128i *)(x + idx_n));


#define BF16_VECTOR_MASKZ_LOAD_1x32(reg, x, idx_n, mask)     \
    reg = _mm512_maskz_loadu_epi16(mask, x + idx_n);


#define BF16_VECTOR_MASKZ_LOAD_1x16(reg, x, idx_n, mask)     \
    reg = _mm256_maskz_loadu_epi16(mask, x + idx_n);


#define BF16_VECTOR_MASKZ_LOAD_1x8(reg, x, idx_n, mask)      \
    reg = _mm_maskz_loadu_epi16(mask, x + idx_n);


/* 2-step interleave for matrix against 8 rows with 32 BF16 elements per row
    Input  - register array of 8 rows of raw-major matrix
    Output - the output of Step 2

    Step 1: 2-element interleave for matrix
    |a0|a1|b0|b1|a2|a3|b2|b3|a8 |a9 |b8 |b9 |a10|a11|b10|b11|a16|a17|b16|b17|a18|a19|b18|b19|a24|a25|b24|b25|a26|a27|b26|b27
    |c0|c1|d0|d1|c2|c3|d2|d3|c8 |c9 |d8 |d9 |c10|c11|d10|d11|c16|c17|d16|d17|c18|c19|d18|d19|c24|c25|d24|d25|c26|c27|d26|d27
    |e0|e1|f0|f1|e2|e3|f2|f3|e8 |e9 |f8 |f9 |e10|e11|f10|f11|e16|e17|f16|f17|e18|e19|f18|f19|e24|e25|f24|f25|e26|e27|f26|f27
    |g0|g1|h0|h1|g2|g3|h2|h3|g8 |g9 |h8 |h9 |g10|g11|h10|h11|g16|g17|h16|h17|g18|g19|h18|h19|g24|g25|h24|h25|g26|g27|h26|h27
    |a4|a5|b4|b5|a6|a7|b6|b7|a12|a13|b12|b13|a14|a15|b14|b15|a20|a21|b20|b21|a22|a23|b22|b23|a28|a29|b28|b29|a30|a31|b30|b31
    |c4|c5|d4|d5|c6|c7|d6|d7|c12|c13|d12|d13|c14|c15|d14|d15|c20|c21|d20|d21|c22|c23|d22|d23|c28|c29|d28|d29|c30|c31|d30|d31
    |e4|e5|f4|f5|e6|e7|f6|f7|e12|e13|f12|f13|e14|e15|f14|f15|e20|e21|f20|f21|e22|e23|f22|f23|e28|e29|f28|f29|e30|e31|f30|f31
    |g4|g5|h4|h5|g6|g7|h6|h7|g12|g13|h12|h13|g14|g15|h14|h15|g20|g21|h20|h21|g22|g23|h22|h23|g28|g29|h28|h29|g30|g31|h30|h31

    Step 2: 4-element interleave for matrix
    |a0|a1|b0|b1|c0|c1|d0|d1|a8 |a9 |b8 |b9 |c8 |c9 |d8 |d9 |a16|a17|b16|b17|c16|c17|d16|d17|a24|a25|b24|b25|c24|c25|d24|d25
    |a2|a3|b2|b3|c2|c3|d2|d3|a10|a11|b10|b11|c10|c11|d10|d11|a18|a19|b18|b19|c18|c19|d18|d19|a26|a27|b26|b27|c26|c27|d26|d27
    |e0|e1|f0|f1|g0|g1|h0|h1|e8 |e9 |f8 |f9 |g8 |g9 |h8 |h9 |e16|e17|f16|f17|g16|g17|h16|h17|e24|e25|f24|f25|g24|g25|h24|h25
    |e2|e3|f2|f3|g2|g3|h2|h3|e10|e11|f10|f11|g10|g11|h10|h11|e18|e19|f18|f19|g18|g19|h18|h19|e26|e27|f26|f27|g26|g27|h26|h27
    |a4|a5|b4|b5|c4|c5|d4|d5|a12|a13|b12|b13|c12|c13|d12|d13|a20|a21|b20|b21|c20|c21|d20|d21|a28|a29|b28|b29|c28|c29|d28|d29
    |a6|a7|b6|b7|c6|c7|d6|d7|a14|a15|b14|b15|c14|c15|d14|d15|a22|a23|b22|b23|c22|c23|d22|d23|a30|a31|b30|b31|c30|c31|d30|d31
    |e4|e5|f4|f5|g4|g5|h4|h5|e12|e13|f12|f13|g12|g13|h12|h13|e20|e21|f20|f21|g20|g21|h20|h21|e28|e29|f28|f29|g28|g29|h28|h29
    |e6|e7|f6|f7|g6|g7|h6|h7|e14|e15|f14|f15|g14|g15|h14|h15|e22|e23|f22|f23|g22|g23|h22|h23|e30|e31|f30|f31|g30|g31|h30|h31
*/
#define BF16_INTERLEAVE_8x32(regArray)                                  \
    regArray##_8  = _mm512_unpacklo_epi32(regArray##_0, regArray##_1);  \
    regArray##_9  = _mm512_unpacklo_epi32(regArray##_2, regArray##_3);  \
    regArray##_10 = _mm512_unpacklo_epi32(regArray##_4, regArray##_5);  \
    regArray##_11 = _mm512_unpacklo_epi32(regArray##_6, regArray##_7);  \
    regArray##_12 = _mm512_unpackhi_epi32(regArray##_0, regArray##_1);  \
    regArray##_13 = _mm512_unpackhi_epi32(regArray##_2, regArray##_3);  \
    regArray##_14 = _mm512_unpackhi_epi32(regArray##_4, regArray##_5);  \
    regArray##_15 = _mm512_unpackhi_epi32(regArray##_6, regArray##_7);  \
                                                                        \
    regArray##_0 = _mm512_unpacklo_epi64(regArray##_8,  regArray##_9);  \
    regArray##_1 = _mm512_unpackhi_epi64(regArray##_8,  regArray##_9);  \
    regArray##_2 = _mm512_unpacklo_epi64(regArray##_10, regArray##_11); \
    regArray##_3 = _mm512_unpackhi_epi64(regArray##_10, regArray##_11); \
    regArray##_4 = _mm512_unpacklo_epi64(regArray##_12, regArray##_13); \
    regArray##_5 = _mm512_unpackhi_epi64(regArray##_12, regArray##_13); \
    regArray##_6 = _mm512_unpacklo_epi64(regArray##_14, regArray##_15); \
    regArray##_7 = _mm512_unpackhi_epi64(regArray##_14, regArray##_15);


/* 2-step interleave for matrix against 8 rows with 16 BF16 elements per row
    Input  - register array of 8 rows of raw-major matrix
    Output - the output of Step 2

    Step 1: 2-element interleave for matrix
    |a0|a1|b0|b1|a2|a3|b2|b3|a8 |a9 |b8 |b9 |a10|a11|b10|b11
    |c0|c1|d0|d1|c2|c3|d2|d3|c8 |c9 |d8 |d9 |c10|c11|d10|d11
    |e0|e1|f0|f1|e2|e3|f2|f3|e8 |e9 |f8 |f9 |e10|e11|f10|f11
    |g0|g1|h0|h1|g2|g3|h2|h3|g8 |g9 |h8 |h9 |g10|g11|h10|h11
    |a4|a5|b4|b5|a6|a7|b6|b7|a12|a13|b12|b13|a14|a15|b14|b15
    |c4|c5|d4|d5|c6|c7|d6|d7|c12|c13|d12|d13|c14|c15|d14|d15
    |e4|e5|f4|f5|e6|e7|f6|f7|e12|e13|f12|f13|e14|e15|f14|f15
    |g4|g5|h4|h5|g6|g7|h6|h7|g12|g13|h12|h13|g14|g15|h14|h15

    Step 2: 4-element interleave for matrix
    |a0|a1|b0|b1|c0|c1|d0|d1|a8 |a9 |b8 |b9 |c8 |c9 |d8 |d9
    |a2|a3|b2|b3|c2|c3|d2|d3|a10|a11|b10|b11|c10|c11|d10|d11
    |e0|e1|f0|f1|g0|g1|h0|h1|e8 |e9 |f8 |f9 |g8 |g9 |h8 |h9
    |e2|e3|f2|f3|g2|g3|h2|h3|e10|e11|f10|f11|g10|g11|h10|h11
    |a4|a5|b4|b5|c4|c5|d4|d5|a12|a13|b12|b13|c12|c13|d12|d13
    |a6|a7|b6|b7|c6|c7|d6|d7|a14|a15|b14|b15|c14|c15|d14|d15
    |e4|e5|f4|f5|g4|g5|h4|h5|e12|e13|f12|f13|g12|g13|h12|h13
    |e6|e7|f6|f7|g6|g7|h6|h7|e14|e15|f14|f15|g14|g15|h14|h15
*/
#define BF16_INTERLEAVE_8x16(regArray)                                  \
    regArray##_8  = _mm256_unpacklo_epi32(regArray##_0, regArray##_1);  \
    regArray##_9  = _mm256_unpacklo_epi32(regArray##_2, regArray##_3);  \
    regArray##_10 = _mm256_unpacklo_epi32(regArray##_4, regArray##_5);  \
    regArray##_11 = _mm256_unpacklo_epi32(regArray##_6, regArray##_7);  \
    regArray##_12 = _mm256_unpackhi_epi32(regArray##_0, regArray##_1);  \
    regArray##_13 = _mm256_unpackhi_epi32(regArray##_2, regArray##_3);  \
    regArray##_14 = _mm256_unpackhi_epi32(regArray##_4, regArray##_5);  \
    regArray##_15 = _mm256_unpackhi_epi32(regArray##_6, regArray##_7);  \
                                                                        \
    regArray##_0  = _mm256_unpacklo_epi64(regArray##_8,  regArray##_9);    \
    regArray##_1  = _mm256_unpackhi_epi64(regArray##_8,  regArray##_9);    \
    regArray##_2  = _mm256_unpacklo_epi64(regArray##_10, regArray##_11);   \
    regArray##_3  = _mm256_unpackhi_epi64(regArray##_10, regArray##_11);   \
    regArray##_4  = _mm256_unpacklo_epi64(regArray##_12, regArray##_13);   \
    regArray##_5  = _mm256_unpackhi_epi64(regArray##_12, regArray##_13);   \
    regArray##_6  = _mm256_unpacklo_epi64(regArray##_14, regArray##_15);   \
    regArray##_7  = _mm256_unpackhi_epi64(regArray##_14, regArray##_15);

/* 2-step interleave for matrix against 8 rows with 32 BF16 elements per row
    Input  - register array of 8 rows of raw-major matrix
    Output - the output of Step 2

    Step 1: 2-element interleave for matrix
    |a0|a1|b0|b1|a2|a3|b2|b3|a8 |a9 |b8 |b9 |a10|a11|b10|b11|a16|a17|b16|b17|a18|a19|b18|b19|a24|a25|b24|b25|a26|a27|b26|b27
    |c0|c1|d0|d1|c2|c3|d2|d3|c8 |c9 |d8 |d9 |c10|c11|d10|d11|c16|c17|d16|d17|c18|c19|d18|d19|c24|c25|d24|d25|c26|c27|d26|d27
    |a4|a5|b4|b5|a6|a7|b6|b7|a12|a13|b12|b13|a14|a15|b14|b15|a20|a21|b20|b21|a22|a23|b22|b23|a28|a29|b28|b29|a30|a31|b30|b31
    |c4|c5|d4|d5|c6|c7|d6|d7|c12|c13|d12|d13|c14|c15|d14|d15|c20|c21|d20|d21|c22|c23|d22|d23|c28|c29|d28|d29|c30|c31|d30|d31

    Step 2: 4-element interleave for matrix
    |a0|a1|b0|b1|c0|c1|d0|d1|a8 |a9 |b8 |b9 |c8 |c9 |d8 |d9 |a16|a17|b16|b17|c16|c17|d16|d17|a24|a25|b24|b25|c24|c25|d24|d25
    |a2|a3|b2|b3|c2|c3|d2|d3|a10|a11|b10|b11|c10|c11|d10|d11|a18|a19|b18|b19|c18|c19|d18|d19|a26|a27|b26|b27|c26|c27|d26|d27
    |a4|a5|b4|b5|c4|c5|d4|d5|a12|a13|b12|b13|c12|c13|d12|d13|a20|a21|b20|b21|c20|c21|d20|d21|a28|a29|b28|b29|c28|c29|d28|d29
    |a6|a7|b6|b7|c6|c7|d6|d7|a14|a15|b14|b15|c14|c15|d14|d15|a22|a23|b22|b23|c22|c23|d22|d23|a30|a31|b30|b31|c30|c31|d30|d31
*/
#define BF16_INTERLEAVE_4x32(regArray)                                 \
    regArray##_4 = _mm512_unpacklo_epi32(regArray##_0, regArray##_1);  \
    regArray##_5 = _mm512_unpacklo_epi32(regArray##_2, regArray##_3);  \
    regArray##_6 = _mm512_unpackhi_epi32(regArray##_0, regArray##_1);  \
    regArray##_7 = _mm512_unpackhi_epi32(regArray##_2, regArray##_3);  \
                                                                       \
    regArray##_0 = _mm512_unpacklo_epi64(regArray##_4, regArray##_5);  \
    regArray##_1 = _mm512_unpackhi_epi64(regArray##_4, regArray##_5);  \
    regArray##_2 = _mm512_unpacklo_epi64(regArray##_6, regArray##_7);  \
    regArray##_3 = _mm512_unpackhi_epi64(regArray##_6, regArray##_7);


/* 2-step interleave for matrix against 8 rows with 16 BF16 elements per row
    Input  - register array of 8 rows of raw-major matrix
    Output - the output of Step 2

    Step 1: 2-element interleave for matrix
    |a0|a1|b0|b1|a2|a3|b2|b3|a8 |a9 |b8 |b9 |a10|a11|b10|b11
    |c0|c1|d0|d1|c2|c3|d2|d3|c8 |c9 |d8 |d9 |c10|c11|d10|d11
    |a4|a5|b4|b5|a6|a7|b6|b7|a12|a13|b12|b13|a14|a15|b14|b15
    |c4|c5|d4|d5|c6|c7|d6|d7|c12|c13|d12|d13|c14|c15|d14|d15

    Step 2: 4-element interleave for matrix
    |a0|a1|b0|b1|c0|c1|d0|d1|a8 |a9 |b8 |b9 |c8 |c9 |d8 |d9
    |a2|a3|b2|b3|c2|c3|d2|d3|a10|a11|b10|b11|c10|c11|d10|d11
    |a4|a5|b4|b5|c4|c5|d4|d5|a12|a13|b12|b13|c12|c13|d12|d13
    |a6|a7|b6|b7|c6|c7|d6|d7|a14|a15|b14|b15|c14|c15|d14|d15
*/
#define BF16_INTERLEAVE_4x16(regArray)                                 \
    regArray##_4 = _mm256_unpacklo_epi32(regArray##_0, regArray##_1);  \
    regArray##_5 = _mm256_unpacklo_epi32(regArray##_2, regArray##_3);  \
    regArray##_6 = _mm256_unpackhi_epi32(regArray##_0, regArray##_1);  \
    regArray##_7 = _mm256_unpackhi_epi32(regArray##_2, regArray##_3);  \
                                                                       \
    regArray##_0 = _mm256_unpacklo_epi64(regArray##_4, regArray##_5);  \
    regArray##_1 = _mm256_unpackhi_epi64(regArray##_4, regArray##_5);  \
    regArray##_2 = _mm256_unpacklo_epi64(regArray##_6, regArray##_7);  \
    regArray##_3 = _mm256_unpackhi_epi64(regArray##_6, regArray##_7);


/* 2-step interleave for x with 32 BF16 elements
    Input  - original vector
    Output - the output of Step 2

    Step 1: 2-element interleave for x:
    |x0|x1|x0|x1|x2|x3|x2|x3|x8 |x9 |x8 |x9 |x10|x11|x10|x11|x16|x17|x16|x17|x18|x19|x18|x19|x24|x25|x24|x25|x26|x27|x26|x27
    |x4|x5|x4|x5|x6|x7|x6|x7|x12|x13|x12|x13|x14|x15|x14|x15|x20|x21|x20|x21|x22|x23|x22|x23|x28|x29|x28|x29|x30|x31|x30|x31
 
    Step 2: 4-element interleave for x:
    |x0|x1|x0|x1|x0|x1|x0|x1|x8 |x9 |x8 |x9 |x8 |x9 |x8 |x9 |x16|x17|x16|x17|x16|x17|x16|x17|x24|x25|x24|x25|x24|x25|x24|x25
    |x2|x3|x2|x3|x2|x3|x2|x3|x10|x11|x10|x11|x10|x11|x10|x11|x18|x19|x18|x19|x18|x19|x18|x19|x26|x27|x26|x27|x26|x27|x26|x27
    |x4|x5|x4|x5|x4|x5|x4|x5|x12|x13|x12|x13|x12|x13|x12|x13|x20|x21|x20|x21|x20|x21|x20|x21|x28|x29|x28|x29|x28|x29|x28|x29
    |x6|x7|x6|x7|x6|x7|x6|x7|x14|x15|x14|x15|x14|x15|x14|x15|x22|x23|x22|x23|x22|x23|x22|x23|x30|x31|x30|x31|x30|x31|x30|x31
*/
#define BF16_INTERLEAVE_1x32(regArray)                                 \
    regArray##_1 = _mm512_unpacklo_epi32(regArray##_0, regArray##_0);  \
    regArray##_3 = _mm512_unpackhi_epi32(regArray##_0, regArray##_0);  \
                                                                       \
    regArray##_0 = _mm512_unpacklo_epi64(regArray##_1, regArray##_1);  \
    regArray##_1 = _mm512_unpackhi_epi64(regArray##_1, regArray##_1);  \
    regArray##_2 = _mm512_unpacklo_epi64(regArray##_3, regArray##_3);  \
    regArray##_3 = _mm512_unpackhi_epi64(regArray##_3, regArray##_3);


/* 2-step interleave for x with 16 BF16 elements
    Input  - original vector
    Output - the output of Step 2

    Step 1: 2-element interleave for x:
    |x0|x1|x0|x1|x2|x3|x2|x3|x8 |x9 |x8 |x9 |x10|x11|x10|x11
    |x4|x5|x4|x5|x6|x7|x6|x7|x12|x13|x12|x13|x14|x15|x14|x15

    Step 2: 4-element interleave for x:
    |x0|x1|x0|x1|x0|x1|x0|x1|x8 |x9 |x8 |x9 |x8 |x9 |x8 |x9
    |x2|x3|x2|x3|x2|x3|x2|x3|x10|x11|x10|x11|x10|x11|x10|x11
    |x4|x5|x4|x5|x4|x5|x4|x5|x12|x13|x12|x13|x12|x13|x12|x13
    |x6|x7|x6|x7|x6|x7|x6|x7|x14|x15|x14|x15|x14|x15|x14|x15
*/
#define BF16_INTERLEAVE_1x16(regArray)                                 \
    regArray##_1 = _mm256_unpacklo_epi32(regArray##_0, regArray##_0);  \
    regArray##_3 = _mm256_unpackhi_epi32(regArray##_0, regArray##_0);  \
                                                                       \
    regArray##_0 = _mm256_unpacklo_epi64(regArray##_1, regArray##_1);  \
    regArray##_1 = _mm256_unpackhi_epi64(regArray##_1, regArray##_1);  \
    regArray##_2 = _mm256_unpacklo_epi64(regArray##_3, regArray##_3);  \
    regArray##_3 = _mm256_unpackhi_epi64(regArray##_3, regArray##_3);

/* 1-step interleave to exchange the high-256s bit and low-256 bits of 4 pair of registers
   |a0|a1|...|a14|a15|i0|i1|...|i14|i15|
   |b0|b1|...|b14|b15|j0|j1|...|j14|j15|
   |c0|c1|...|c14|c15|k0|k1|...|k14|k15|
   |d0|d1|...|d14|d15|l0|l1|...|l14|l15|
   |e0|e1|...|e14|e15|m0|m1|...|m14|m15|
   |f0|f1|...|f14|f15|n0|n1|...|n14|n15|
   |g0|g1|...|g14|g15|o0|o1|...|o14|o15|
   |h0|h1|...|h14|h15|p0|p1|...|p14|p15|
*/
#define BF16_INTERLEAVE256_8x32(regArray)                                     \
    regArray##_0 = _mm512_shuffle_i32x4(regArray##_8,  regArray##_12, 0x44);  \
    regArray##_1 = _mm512_shuffle_i32x4(regArray##_8,  regArray##_12, 0xee);  \
    regArray##_2 = _mm512_shuffle_i32x4(regArray##_9,  regArray##_13, 0x44);  \
    regArray##_3 = _mm512_shuffle_i32x4(regArray##_9,  regArray##_13, 0xee);  \
    regArray##_4 = _mm512_shuffle_i32x4(regArray##_10, regArray##_14, 0x44);  \
    regArray##_5 = _mm512_shuffle_i32x4(regArray##_10, regArray##_14, 0xee);  \
    regArray##_6 = _mm512_shuffle_i32x4(regArray##_11, regArray##_15, 0x44);  \
    regArray##_7 = _mm512_shuffle_i32x4(regArray##_11, regArray##_15, 0xee);


/* 1-step interleave to exchange the high-256s bit and low-256 bits of 2 pair of registers
   |a0|a1|...|a14|a15|e0|e1|...|e14|e15|
   |b0|b1|...|b14|b15|f0|f1|...|f14|f15|
   |c0|c1|...|c14|c15|g0|g1|...|g14|g15|
   |d0|d1|...|d14|d15|h0|h1|...|h14|h15|
*/
#define BF16_INTERLEAVE256_4x32(regArray)                                    \
    regArray##_0 = _mm512_shuffle_i32x4(regArray##_4,  regArray##_6, 0x44);  \
    regArray##_1 = _mm512_shuffle_i32x4(regArray##_4,  regArray##_6, 0xee);  \
    regArray##_2 = _mm512_shuffle_i32x4(regArray##_5,  regArray##_7, 0x44);  \
    regArray##_3 = _mm512_shuffle_i32x4(regArray##_5,  regArray##_7, 0xee);


#define BF16_PERMUTE_8x32(idx, regArray) \
    regArray##_8  = _mm512_permutexvar_epi16(idx, regArray##_0);  \
    regArray##_9  = _mm512_permutexvar_epi16(idx, regArray##_1);  \
    regArray##_10 = _mm512_permutexvar_epi16(idx, regArray##_2);  \
    regArray##_11 = _mm512_permutexvar_epi16(idx, regArray##_3);  \
    regArray##_12 = _mm512_permutexvar_epi16(idx, regArray##_4);  \
    regArray##_13 = _mm512_permutexvar_epi16(idx, regArray##_5);  \
    regArray##_14 = _mm512_permutexvar_epi16(idx, regArray##_6);  \
    regArray##_15 = _mm512_permutexvar_epi16(idx, regArray##_7);


#define BF16_PERMUTE_8x32_2(idx, regArray) \
    regArray##_8  = _mm512_permutexvar_epi32(idx, regArray##_0);  \
    regArray##_9  = _mm512_permutexvar_epi32(idx, regArray##_1);  \
    regArray##_10 = _mm512_permutexvar_epi32(idx, regArray##_2);  \
    regArray##_11 = _mm512_permutexvar_epi32(idx, regArray##_3);  \
    regArray##_12 = _mm512_permutexvar_epi32(idx, regArray##_4);  \
    regArray##_13 = _mm512_permutexvar_epi32(idx, regArray##_5);  \
    regArray##_14 = _mm512_permutexvar_epi32(idx, regArray##_6);  \
    regArray##_15 = _mm512_permutexvar_epi32(idx, regArray##_7);


#define BF16_PERMUTE_4x32(idx, regArray) \
    regArray##_4 = _mm512_permutexvar_epi16(idx, regArray##_0);  \
    regArray##_5 = _mm512_permutexvar_epi16(idx, regArray##_1);  \
    regArray##_6 = _mm512_permutexvar_epi16(idx, regArray##_2);  \
    regArray##_7 = _mm512_permutexvar_epi16(idx, regArray##_3);


#define BF16_PERMUTE_4x32_2(idx, regArray) \
    regArray##_4 = _mm512_permutexvar_epi32(idx, regArray##_0);  \
    regArray##_5 = _mm512_permutexvar_epi32(idx, regArray##_1);  \
    regArray##_6 = _mm512_permutexvar_epi32(idx, regArray##_2);  \
    regArray##_7 = _mm512_permutexvar_epi32(idx, regArray##_3);


/* Calculate the dot result for 2-step interleaved matrix and vector
   (Assume throughput for _mm512_dpbf16_ps is 0.5, tunable per platform)
*/
#define BF16_2STEP_INTERLEAVED_DOT_8x32(accumArray, matArray, xArray)                                   \
    accumArray##_0 = _mm512_dpbf16_ps(accumArray##_0, (__m512bh) matArray##_0, (__m512bh) xArray##_0);  \
    accumArray##_1 = _mm512_dpbf16_ps(accumArray##_1, (__m512bh) matArray##_2, (__m512bh) xArray##_0);  \
    accumArray##_0 = _mm512_dpbf16_ps(accumArray##_0, (__m512bh) matArray##_1, (__m512bh) xArray##_1);  \
    accumArray##_1 = _mm512_dpbf16_ps(accumArray##_1, (__m512bh) matArray##_3, (__m512bh) xArray##_1);  \
    accumArray##_0 = _mm512_dpbf16_ps(accumArray##_0, (__m512bh) matArray##_4, (__m512bh) xArray##_2);  \
    accumArray##_1 = _mm512_dpbf16_ps(accumArray##_1, (__m512bh) matArray##_6, (__m512bh) xArray##_2);  \
    accumArray##_0 = _mm512_dpbf16_ps(accumArray##_0, (__m512bh) matArray##_5, (__m512bh) xArray##_3);  \
    accumArray##_1 = _mm512_dpbf16_ps(accumArray##_1, (__m512bh) matArray##_7, (__m512bh) xArray##_3);


/* Calculate the dot result for 2-step interleaved matrix and vector
   (Assume throughput for _mm256_dpbf16_ps is 0.5, tunable per platform)
*/
#define BF16_2STEP_INTERLEAVED_DOT_8x16(accumArray, matArray, xArray)                                   \
    accumArray##_0 = _mm256_dpbf16_ps(accumArray##_0, (__m256bh) matArray##_0, (__m256bh) xArray##_0);  \
    accumArray##_1 = _mm256_dpbf16_ps(accumArray##_1, (__m256bh) matArray##_2, (__m256bh) xArray##_0);  \
    accumArray##_0 = _mm256_dpbf16_ps(accumArray##_0, (__m256bh) matArray##_1, (__m256bh) xArray##_1);  \
    accumArray##_1 = _mm256_dpbf16_ps(accumArray##_1, (__m256bh) matArray##_3, (__m256bh) xArray##_1);  \
    accumArray##_0 = _mm256_dpbf16_ps(accumArray##_0, (__m256bh) matArray##_4, (__m256bh) xArray##_2);  \
    accumArray##_1 = _mm256_dpbf16_ps(accumArray##_1, (__m256bh) matArray##_6, (__m256bh) xArray##_2);  \
    accumArray##_0 = _mm256_dpbf16_ps(accumArray##_0, (__m256bh) matArray##_5, (__m256bh) xArray##_3);  \
    accumArray##_1 = _mm256_dpbf16_ps(accumArray##_1, (__m256bh) matArray##_7, (__m256bh) xArray##_3);

/* Calculate the dot result for 2-step interleaved matrix and vector
   (Assume throughput for _mm512_dpbf16_ps is 0.5, tunable per platform)
*/
#define BF16_2STEP_INTERLEAVED_DOT_4x32(accumArray, matArray, xArray)                                   \
    accumArray##_0 = _mm512_dpbf16_ps(accumArray##_0, (__m512bh) matArray##_0, (__m512bh) xArray##_0);  \
    accumArray##_1 = _mm512_dpbf16_ps(accumArray##_1, (__m512bh) matArray##_1, (__m512bh) xArray##_1);  \
    accumArray##_0 = _mm512_dpbf16_ps(accumArray##_0, (__m512bh) matArray##_2, (__m512bh) xArray##_2);  \
    accumArray##_1 = _mm512_dpbf16_ps(accumArray##_1, (__m512bh) matArray##_3, (__m512bh) xArray##_3);


/* Calculate the dot result for 2-step interleaved matrix and vector
   (Assume throughput for _mm256_dpbf16_ps is 0.5, tunable per platform)
*/
#define BF16_2STEP_INTERLEAVED_DOT_4x16(accumArray, matArray, xArray)                                   \
    accumArray##_0 = _mm256_dpbf16_ps(accumArray##_0, (__m256bh) matArray##_0, (__m256bh) xArray##_0);  \
    accumArray##_1 = _mm256_dpbf16_ps(accumArray##_1, (__m256bh) matArray##_1, (__m256bh) xArray##_1);  \
    accumArray##_0 = _mm256_dpbf16_ps(accumArray##_0, (__m256bh) matArray##_2, (__m256bh) xArray##_2);  \
    accumArray##_1 = _mm256_dpbf16_ps(accumArray##_1, (__m256bh) matArray##_3, (__m256bh) xArray##_3);


/* Calculate the dot result for matrix and vector at 32 elements per row
   (Assume throughput for _mm512_dpbf16_ps is 0.5, tunable per platform)
*/
#define BF16_DOT_8x32(accumArray, matArray, xArray)                                                 \
    accumArray##_0 = _mm512_dpbf16_ps(accumArray##_0, (__m512bh) matArray##_0, (__m512bh) xArray);  \
    accumArray##_1 = _mm512_dpbf16_ps(accumArray##_1, (__m512bh) matArray##_1, (__m512bh) xArray);  \
    accumArray##_2 = _mm512_dpbf16_ps(accumArray##_2, (__m512bh) matArray##_2, (__m512bh) xArray);  \
    accumArray##_3 = _mm512_dpbf16_ps(accumArray##_3, (__m512bh) matArray##_3, (__m512bh) xArray);  \
    accumArray##_4 = _mm512_dpbf16_ps(accumArray##_4, (__m512bh) matArray##_4, (__m512bh) xArray);  \
    accumArray##_5 = _mm512_dpbf16_ps(accumArray##_5, (__m512bh) matArray##_5, (__m512bh) xArray);  \
    accumArray##_6 = _mm512_dpbf16_ps(accumArray##_6, (__m512bh) matArray##_6, (__m512bh) xArray);  \
    accumArray##_7 = _mm512_dpbf16_ps(accumArray##_7, (__m512bh) matArray##_7, (__m512bh) xArray);

/* Calculate the dot result for matrix and vector at 32 elements per row
   (Assume throughput for _mm512_dpbf16_ps is 0.5, tunable per platform)
*/
#define BF16_DOT_1x32(accumArray, matArray, xArray)                                                 \
    accumArray = _mm512_dpbf16_ps(accumArray, (__m512bh) matArray, (__m512bh) xArray);

/* Calculate the dot result for matrix and vector at 16 elements per row
   (Assume throughput for _mm256_dpbf16_ps is 0.5, tunable per platform)
*/
#define BF16_DOT_8x16(accumArray, matArray, xArray)                                                 \
    accumArray##_0 = _mm256_dpbf16_ps(accumArray##_0, (__m256bh) matArray##_0, (__m256bh) xArray);  \
    accumArray##_1 = _mm256_dpbf16_ps(accumArray##_1, (__m256bh) matArray##_1, (__m256bh) xArray);  \
    accumArray##_2 = _mm256_dpbf16_ps(accumArray##_2, (__m256bh) matArray##_2, (__m256bh) xArray);  \
    accumArray##_3 = _mm256_dpbf16_ps(accumArray##_3, (__m256bh) matArray##_3, (__m256bh) xArray);  \
    accumArray##_4 = _mm256_dpbf16_ps(accumArray##_4, (__m256bh) matArray##_4, (__m256bh) xArray);  \
    accumArray##_5 = _mm256_dpbf16_ps(accumArray##_5, (__m256bh) matArray##_5, (__m256bh) xArray);  \
    accumArray##_6 = _mm256_dpbf16_ps(accumArray##_6, (__m256bh) matArray##_6, (__m256bh) xArray);  \
    accumArray##_7 = _mm256_dpbf16_ps(accumArray##_7, (__m256bh) matArray##_7, (__m256bh) xArray);


/* 2-step interleave for matrix against 8 rows with 16 fp32 elements per row
    Input  - register array of 8 rows of raw-major matrix
    Output - the output of Step 2

    Step 1: 2-element interleave for matrix
    |a0|b0|a1|b1|a4|b4|a5|b5|a8 |b8 |a9 |b9 |a12|b12|a13|b13|
    |c0|d0|c1|d1|c4|d4|c5|d5|c8 |d8 |c9 |d9 |c12|d12|c13|d13|
    |e0|f0|e1|f1|e4|f4|e5|f5|e8 |f8 |e9 |f9 |e12|f12|e13|f13|
    |g0|h0|g1|h1|g4|h4|g5|h5|g8 |h8 |g9 |h9 |g12|h12|g13|h13|
    |a2|b2|a3|b3|a6|b6|a7|b7|a10|b10|a11|b11|a14|b14|a15|b15|
    |c2|d2|c3|d3|c6|d6|c7|d7|c10|d10|c11|d11|c14|d14|c15|d15|
    |e2|f2|e3|f3|e6|f6|e7|f7|e10|f10|e11|f11|e14|f14|e15|f15|
    |g2|h2|g3|h3|g6|h6|g7|h7|g10|h10|g11|h11|g14|h14|g15|h15|

    Step 2: 4-element interleave for matrix
    |a0|b0|c0|d0|a4|b4|c4|d4|a8 |b8 |c8 |d8 |a12|b12|c12|d12|
    |a1|b1|c1|d1|a5|b5|c5|d5|a9 |b9 |c9 |d9 |a13|b13|c13|d13|
    |e0|f0|g0|h0|e4|f4|g4|h4|e8 |f8 |g8 |h8 |e12|f12|g12|h12|
    |e1|f1|g1|h1|e5|f5|g5|h5|e9 |f9 |g9 |h9 |e13|f13|g13|h13|
    |a2|b2|c2|d2|a6|b6|c6|d6|a10|b10|c10|d10|a14|b14|c14|d14|
    |a3|b3|c3|d3|a7|b7|c7|d7|a11|b11|c11|d11|a15|b15|c15|d15|
    |e2|f2|g2|h2|e6|f6|g6|h6|e10|f10|g10|h10|e14|f14|g14|h14|
    |e3|f3|g3|h3|e7|f7|g7|h7|e11|f11|g11|h11|e15|f15|g15|h15|
*/
#define FP32_INTERLEAVE_8x16(regArray)                               \
    regArray##_8  = _mm512_unpacklo_ps(regArray##_0, regArray##_1);  \
    regArray##_9  = _mm512_unpacklo_ps(regArray##_2, regArray##_3);  \
    regArray##_10 = _mm512_unpacklo_ps(regArray##_4, regArray##_5);  \
    regArray##_11 = _mm512_unpacklo_ps(regArray##_6, regArray##_7);  \
    regArray##_12 = _mm512_unpackhi_ps(regArray##_0, regArray##_1);  \
    regArray##_13 = _mm512_unpackhi_ps(regArray##_2, regArray##_3);  \
    regArray##_14 = _mm512_unpackhi_ps(regArray##_4, regArray##_5);  \
    regArray##_15 = _mm512_unpackhi_ps(regArray##_6, regArray##_7);  \
                                                                     \
    regArray##_0 = (__m512) _mm512_unpacklo_pd((__m512d) regArray##_8,  (__m512d) regArray##_9);  \
    regArray##_1 = (__m512) _mm512_unpackhi_pd((__m512d) regArray##_8,  (__m512d) regArray##_9);  \
    regArray##_4 = (__m512) _mm512_unpacklo_pd((__m512d) regArray##_10, (__m512d) regArray##_11); \
    regArray##_5 = (__m512) _mm512_unpackhi_pd((__m512d) regArray##_10, (__m512d) regArray##_11); \
    regArray##_2 = (__m512) _mm512_unpacklo_pd((__m512d) regArray##_12, (__m512d) regArray##_13); \
    regArray##_3 = (__m512) _mm512_unpackhi_pd((__m512d) regArray##_12, (__m512d) regArray##_13); \
    regArray##_6 = (__m512) _mm512_unpacklo_pd((__m512d) regArray##_14, (__m512d) regArray##_15); \
    regArray##_7 = (__m512) _mm512_unpackhi_pd((__m512d) regArray##_14, (__m512d) regArray##_15);

#define FP32_INTERLEAVE_8x16_ARRAY(regArray)                               \
    regArray[8]  = _mm512_unpacklo_ps(regArray[0], regArray[1]);  \
    regArray[9]  = _mm512_unpacklo_ps(regArray[2], regArray[3]);  \
    regArray[10] = _mm512_unpacklo_ps(regArray[4], regArray[5]);  \
    regArray[11] = _mm512_unpacklo_ps(regArray[6], regArray[7]);  \
    regArray[12] = _mm512_unpackhi_ps(regArray[0], regArray[1]);  \
    regArray[13] = _mm512_unpackhi_ps(regArray[2], regArray[3]);  \
    regArray[14] = _mm512_unpackhi_ps(regArray[4], regArray[5]);  \
    regArray[15] = _mm512_unpackhi_ps(regArray[6], regArray[7]);  \
                                                                     \
    regArray[0] = (__m512) _mm512_unpacklo_pd((__m512d) regArray[8],  (__m512d) regArray[9]);  \
    regArray[1] = (__m512) _mm512_unpackhi_pd((__m512d) regArray[8],  (__m512d) regArray[9]);  \
    regArray[4] = (__m512) _mm512_unpacklo_pd((__m512d) regArray[10], (__m512d) regArray[11]); \
    regArray[5] = (__m512) _mm512_unpackhi_pd((__m512d) regArray[10], (__m512d) regArray[11]); \
    regArray[2] = (__m512) _mm512_unpacklo_pd((__m512d) regArray[12], (__m512d) regArray[13]); \
    regArray[3] = (__m512) _mm512_unpackhi_pd((__m512d) regArray[12], (__m512d) regArray[13]); \
    regArray[6] = (__m512) _mm512_unpacklo_pd((__m512d) regArray[14], (__m512d) regArray[15]); \
    regArray[7] = (__m512) _mm512_unpackhi_pd((__m512d) regArray[14], (__m512d) regArray[15]);

/* 2-step interleave for matrix against 8 rows with 8 fp32 elements per row
    Input  - register array of 8 rows of raw-major matrix
    Output - the output of Step 2

    Step 1: 2-element interleave for matrix
    |a0|b0|a1|b1|a4|b4|a5|b5|
    |c0|d0|c1|d1|c4|d4|c5|d5|
    |e0|f0|e1|f1|e4|f4|e5|f5|
    |g0|h0|g1|h1|g4|h4|g5|h5|
    |a2|b2|a3|b3|a6|b6|a7|b7|
    |c2|d2|c3|d3|c6|d6|c7|d7|
    |e2|f2|e3|f3|e6|f6|e7|f7|
    |g2|h2|g3|h3|g6|h6|g7|h7|

    Step 2: 4-element interleave for matrix
    |a0|b0|c0|d0|a4|b4|c4|d4|
    |a1|b1|c1|d1|a5|b5|c5|d5|
    |e0|f0|g0|h0|e4|f4|g4|h4|
    |e1|f1|g1|h1|e5|f5|g5|h5|
    |a2|b2|c2|d2|a6|b6|c6|d6|
    |a3|b3|c3|d3|a7|b7|c7|d7|
    |e2|f2|g2|h2|e6|f6|g6|h6|
    |e3|f3|g3|h3|e7|f7|g7|h7|
*/
#define FP32_INTERLEAVE_8x8(regArray)                                \
    regArray##_8  = _mm256_unpacklo_ps(regArray##_0, regArray##_1);  \
    regArray##_9  = _mm256_unpacklo_ps(regArray##_2, regArray##_3);  \
    regArray##_10 = _mm256_unpacklo_ps(regArray##_4, regArray##_5);  \
    regArray##_11 = _mm256_unpacklo_ps(regArray##_6, regArray##_7);  \
    regArray##_12 = _mm256_unpackhi_ps(regArray##_0, regArray##_1);  \
    regArray##_13 = _mm256_unpackhi_ps(regArray##_2, regArray##_3);  \
    regArray##_14 = _mm256_unpackhi_ps(regArray##_4, regArray##_5);  \
    regArray##_15 = _mm256_unpackhi_ps(regArray##_6, regArray##_7);  \
                                                                     \
    regArray##_0 = (__m256) _mm256_unpacklo_pd((__m256d) regArray##_8,  (__m256d) regArray##_9);  \
    regArray##_1 = (__m256) _mm256_unpackhi_pd((__m256d) regArray##_8,  (__m256d) regArray##_9);  \
    regArray##_4 = (__m256) _mm256_unpacklo_pd((__m256d) regArray##_10, (__m256d) regArray##_11); \
    regArray##_5 = (__m256) _mm256_unpackhi_pd((__m256d) regArray##_10, (__m256d) regArray##_11); \
    regArray##_2 = (__m256) _mm256_unpacklo_pd((__m256d) regArray##_12, (__m256d) regArray##_13); \
    regArray##_3 = (__m256) _mm256_unpackhi_pd((__m256d) regArray##_12, (__m256d) regArray##_13); \
    regArray##_6 = (__m256) _mm256_unpacklo_pd((__m256d) regArray##_14, (__m256d) regArray##_15); \
    regArray##_7 = (__m256) _mm256_unpackhi_pd((__m256d) regArray##_14, (__m256d) regArray##_15);


/* Accumulate the result for 2 batch of 4-registers
*/
#define FP32_ACCUM2_8x16(regArray)                             \
    regArray##_0 = _mm512_add_ps(regArray##_0, regArray##_1);  \
    regArray##_2 = _mm512_add_ps(regArray##_2, regArray##_3);  \
    regArray##_4 = _mm512_add_ps(regArray##_4, regArray##_5);  \
    regArray##_6 = _mm512_add_ps(regArray##_6, regArray##_7);  \
    regArray##_0 = _mm512_add_ps(regArray##_0, regArray##_2);  \
    regArray##_4 = _mm512_add_ps(regArray##_4, regArray##_6);

#define FP32_ACCUM2_8x16_ARRAY(regArray)                             \
    regArray[0] = _mm512_add_ps(regArray[0], regArray[1]);  \
    regArray[2] = _mm512_add_ps(regArray[2], regArray[3]);  \
    regArray[4] = _mm512_add_ps(regArray[4], regArray[5]);  \
    regArray[6] = _mm512_add_ps(regArray[6], regArray[7]);  \
    regArray[0] = _mm512_add_ps(regArray[0], regArray[2]);  \
    regArray[4] = _mm512_add_ps(regArray[4], regArray[6]);

/* Accumulate the result for 2 batch of 4-registers
*/
#define FP32_ACCUM2_8x8(regArray)                              \
    regArray##_0 = _mm256_add_ps(regArray##_0, regArray##_1);  \
    regArray##_2 = _mm256_add_ps(regArray##_2, regArray##_3);  \
    regArray##_4 = _mm256_add_ps(regArray##_4, regArray##_5);  \
    regArray##_6 = _mm256_add_ps(regArray##_6, regArray##_7);  \
    regArray##_0 = _mm256_add_ps(regArray##_0, regArray##_2);  \
    regArray##_4 = _mm256_add_ps(regArray##_4, regArray##_6);


/* Store 16 (alpha * result + beta * y) to y
*/
#define STORE16_COMPLETE_RESULT_ALPHA_BETA(regResult, targetAddr)                                                 \
    regResult = _mm512_fmadd_ps(ALPHAVECTOR, regResult, _mm512_mul_ps(BETAVECTOR, _mm512_loadu_ps(targetAddr)));  \
    _mm512_storeu_ps(targetAddr, regResult);


/* Masked store 16 (alpha * result + beta * y) to y
*/
#define STORE16_MASK_COMPLETE_RESULT_ALPHA_BETA(regResult, targetAddr, mask)                                                  \
    regResult = _mm512_fmadd_ps(ALPHAVECTOR, regResult, _mm512_mul_ps(BETAVECTOR, _mm512_maskz_loadu_ps(mask, targetAddr)));  \
    _mm512_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 8 (alpha * result + beta * y) to y
*/
#define STORE8_COMPLETE_RESULT_ALPHA_BETA(regResult, targetAddr)                                                                                                  \
    regResult = _mm256_fmadd_ps(_mm512_castps512_ps256(ALPHAVECTOR), regResult, _mm256_mul_ps(_mm512_castps512_ps256(BETAVECTOR), _mm256_loadu_ps(targetAddr)));  \
    _mm256_storeu_ps(targetAddr, regResult);


/* Masked store 8 (alpha * result + beta * y) to y
*/
#define STORE8_MASK_COMPLETE_RESULT_ALPHA_BETA(regResult, targetAddr, mask)                                                                                                   \
    regResult = _mm256_fmadd_ps(_mm512_castps512_ps256(ALPHAVECTOR), regResult, _mm256_mul_ps(_mm512_castps512_ps256(BETAVECTOR), _mm256_maskz_loadu_ps(mask, targetAddr)));  \
    _mm256_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 4 (alpha * result + beta * y) to y
*/
#define STORE4_COMPLETE_RESULT_ALPHA_BETA(regResult, targetAddr)                                                                                         \
    regResult = _mm_fmadd_ps(_mm512_castps512_ps128(ALPHAVECTOR), regResult, _mm_mul_ps(_mm512_castps512_ps128(BETAVECTOR), _mm_loadu_ps(targetAddr)));  \
    _mm_storeu_ps(targetAddr, regResult);


/* Masked store 4 (alpha * result + beta * y) to y
*/
#define STORE4_MASK_COMPLETE_RESULT_ALPHA_BETA(regResult, targetAddr, mask)                                                                                          \
    regResult = _mm_fmadd_ps(_mm512_castps512_ps128(ALPHAVECTOR), regResult, _mm_mul_ps(_mm512_castps512_ps128(BETAVECTOR), _mm_maskz_loadu_ps(mask, targetAddr)));  \
    _mm_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 16 (alpha * result + y) to y
*/
#define STORE16_COMPLETE_RESULT_ALPHA_ONE(regResult, targetAddr)                       \
    regResult = _mm512_fmadd_ps(ALPHAVECTOR, regResult, _mm512_loadu_ps(targetAddr));  \
    _mm512_storeu_ps(targetAddr, regResult);


/* Masked store 16 (alpha * result + y) to y
*/
#define STORE16_MASK_COMPLETE_RESULT_ALPHA_ONE(regResult, targetAddr, mask)                        \
    regResult = _mm512_fmadd_ps(ALPHAVECTOR, regResult, _mm512_maskz_loadu_ps(mask, targetAddr));  \
    _mm512_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 8 (alpha * result + y) to y
*/
#define STORE8_COMPLETE_RESULT_ALPHA_ONE(regResult, targetAddr)                                                \
    regResult = _mm256_fmadd_ps(_mm512_castps512_ps256(ALPHAVECTOR), regResult, _mm256_loadu_ps(targetAddr));  \
    _mm256_storeu_ps(targetAddr, regResult);


/* Masked store 8 (alpha * result + y) to y
*/
#define STORE8_MASK_COMPLETE_RESULT_ALPHA_ONE(regResult, targetAddr, mask)                                                 \
    regResult = _mm256_fmadd_ps(_mm512_castps512_ps256(ALPHAVECTOR), regResult, _mm256_maskz_loadu_ps(mask, targetAddr));  \
    _mm256_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 4 (alpha * result + y) to y
*/
#define STORE4_COMPLETE_RESULT_ALPHA_ONE(regResult, targetAddr)                                          \
    regResult = _mm_fmadd_ps(_mm512_castps512_ps128(ALPHAVECTOR), regResult, _mm_loadu_ps(targetAddr));  \
    _mm_storeu_ps(targetAddr, regResult);


/* Masked store 4 (alpha * result + y) to y
*/
#define STORE4_MASK_COMPLETE_RESULT_ALPHA_ONE(regResult, targetAddr, mask)                                           \
    regResult = _mm_fmadd_ps(_mm512_castps512_ps128(ALPHAVECTOR), regResult, _mm_maskz_loadu_ps(mask, targetAddr));  \
    _mm_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 16 (result + y) to y
*/
#define STORE16_COMPLETE_RESULT_ONE_ONE(regResult, targetAddr)          \
    regResult = _mm512_add_ps(regResult, _mm512_loadu_ps(targetAddr));  \
    _mm512_storeu_ps(targetAddr, regResult);


/* Masked store 16 (result + y) to y
*/
#define STORE16_MASK_COMPLETE_RESULT_ONE_ONE(regResult, targetAddr, mask)           \
    regResult = _mm512_add_ps(regResult, _mm512_maskz_loadu_ps(mask, targetAddr));  \
    _mm512_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 8 (result + y) to y
*/
#define STORE8_COMPLETE_RESULT_ONE_ONE(regResult, targetAddr)           \
    regResult = _mm256_add_ps(regResult, _mm256_loadu_ps(targetAddr));  \
    _mm256_storeu_ps(targetAddr, regResult);


/* Masked store 8 (result + y) to y
*/
#define STORE8_MASK_COMPLETE_RESULT_ONE_ONE(regResult, targetAddr, mask)            \
    regResult = _mm256_add_ps(regResult, _mm256_maskz_loadu_ps(mask, targetAddr));  \
    _mm256_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 4 (result + y) to y
*/
#define STORE4_COMPLETE_RESULT_ONE_ONE(regResult, targetAddr)     \
    regResult = _mm_add_ps(regResult, _mm_loadu_ps(targetAddr));  \
    _mm_storeu_ps(targetAddr, regResult);


/* Masked store 4 (result + y) to y
*/
#define STORE4_MASK_COMPLETE_RESULT_ONE_ONE(regResult, targetAddr, mask)      \
    regResult = _mm_add_ps(regResult, _mm_maskz_loadu_ps(mask, targetAddr));  \
    _mm_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 16 (alpha * result) to y
*/
#define STORE16_COMPLETE_RESULT_ALPHA(regResult, targetAddr)  \
    _mm512_storeu_ps(targetAddr, _mm512_mul_ps(ALPHAVECTOR, regResult));


/* Masked store 16 (alpha * result) to y
*/
#define STORE16_MASK_COMPLETE_RESULT_ALPHA(regResult, targetAddr, mask)  \
    _mm512_mask_storeu_ps(targetAddr, mask, _mm512_mul_ps(ALPHAVECTOR, regResult));


/* Store 8 (alpha * result) to y
*/
#define STORE8_COMPLETE_RESULT_ALPHA(regResult, targetAddr)  \
    _mm256_storeu_ps(targetAddr, _mm256_mul_ps(_mm512_castps512_ps256(ALPHAVECTOR), regResult));


/* Masked store 8 (alpha * result) to y
*/
#define STORE8_MASK_COMPLETE_RESULT_ALPHA(regResult, targetAddr, mask)  \
    _mm256_mask_storeu_ps(targetAddr, mask, _mm256_mul_ps(_mm512_castps512_ps256(ALPHAVECTOR), regResult));


/* Store 4 (alpha * result) to y
*/
#define STORE4_COMPLETE_RESULT_ALPHA(regResult, targetAddr)  \
    _mm_storeu_ps(targetAddr, _mm_mul_ps(_mm512_castps512_ps128(ALPHAVECTOR), regResult));


/* Masked store 4 (alpha * result) to y
*/
#define STORE4_MASK_COMPLETE_RESULT_ALPHA(regResult, targetAddr, mask)  \
    _mm_mask_storeu_ps(targetAddr, mask, _mm_mul_ps(_mm512_castps512_ps128(ALPHAVECTOR), regResult));


/* Store 16 result to y
*/
#define STORE16_COMPLETE_RESULT_DIRECT(regResult, targetAddr)  \
    _mm512_storeu_ps(targetAddr, regResult);


/* Masked store 16 result to y
*/
#define STORE16_MASK_COMPLETE_RESULT_DIRECT(regResult, targetAddr, mask)  \
    _mm512_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 8 result to y
*/
#define STORE8_COMPLETE_RESULT_DIRECT(regResult, targetAddr)  \
    _mm256_storeu_ps(targetAddr, regResult);


/* Masked store 8 result to y
*/
#define STORE8_MASK_COMPLETE_RESULT_DIRECT(regResult, targetAddr, mask)  \
    _mm256_mask_storeu_ps(targetAddr, mask, regResult);


/* Store 4 result to y
*/
#define STORE4_COMPLETE_RESULT_DIRECT(regResult, targetAddr)  \
    _mm_storeu_ps(targetAddr, regResult);


/* Masked store 4 result to y
*/
#define STORE4_MASK_COMPLETE_RESULT_DIRECT(regResult, targetAddr, mask)  \
    _mm_mask_storeu_ps(targetAddr, mask, regResult);

#endif
