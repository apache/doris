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

#define HAVE_TOBF16_ACCL_KERNEL 1
#include "common.h"
#include <immintrin.h>

static void tobf16_accl_kernel(BLASLONG n, const double * in, bfloat16 * out)
{
    /* Get the 64-bytes unaligned header number targeting for avx512
     * processing (Assume input float array is natural aligned) */
    int align_header = ((64 - ((uintptr_t)in & (uintptr_t)0x3f)) >> 3) & 0x7;

    if (n < align_header) {align_header = n;}

    if (align_header != 0) {
        unsigned char align_mask8 = (((unsigned char)0xff) >> (8-align_header));
        __m512d a = _mm512_maskz_loadu_pd(*((__mmask8*) &align_mask8), &in[0]);
        _mm_mask_storeu_epi16(&out[0], *((__mmask8*) &align_mask8), (__m128i) _mm256_cvtneps_pbh(_mm512_cvtpd_ps(a)));
    }

    if (n == align_header) {
        return;
    } else {
        n -= align_header;
        in += align_header;
        out += align_header;
    }

    int tail_index_8   = n&(~7);
    int tail_index_32  = n&(~31);
    int tail_index_128 = n&(~127);
    unsigned char tail_mask8 = (((unsigned char) 0xff) >> (8 -(n&7)));

    /* Processing the main chunk with 128-elements per round */
    for (int i = 0; i < tail_index_128; i += 128) {
	// Fold 1
        __m512 data1_512_low  = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+ 0]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[i+ 8])), 1);
        __m512 data1_512_high = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+16]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[i+24])), 1);
        _mm512_storeu_si512(&out[i+ 0], (__m512i) _mm512_cvtne2ps_pbh(data1_512_high, data1_512_low));

	// Fold 2
        __m512 data2_512_low  = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+32]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[i+40])), 1);
        __m512 data2_512_high = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+48]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[i+56])), 1);
        _mm512_storeu_si512(&out[i+32], (__m512i) _mm512_cvtne2ps_pbh(data2_512_high, data2_512_low));

	// Fold 3
        __m512 data3_512_low  = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+64]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[i+72])), 1);
        __m512 data3_512_high = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+80]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[i+88])), 1);
        _mm512_storeu_si512(&out[i+64], (__m512i) _mm512_cvtne2ps_pbh(data3_512_high, data3_512_low));

	// Fold 4
        __m512 data4_512_low  = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+96]))),  _mm512_cvtpd_ps(_mm512_load_pd(&in[i+104])), 1);
        __m512 data4_512_high = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[i+112]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[i+120])), 1);
        _mm512_storeu_si512(&out[i+96], (__m512i) _mm512_cvtne2ps_pbh(data4_512_high, data4_512_low));
    }

    /* Processing the remaining <128 chunk with 32-elements per round */
    for (int j = tail_index_128; j < tail_index_32; j += 32) {
        __m512 data1_512_low  = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[j+ 0]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[j+ 8])), 1);
        __m512 data1_512_high = _mm512_insertf32x8(_mm512_castps256_ps512(_mm512_cvtpd_ps(_mm512_load_pd(&in[j+16]))), _mm512_cvtpd_ps(_mm512_load_pd(&in[j+24])), 1);
        _mm512_storeu_si512(&out[j], (__m512i) _mm512_cvtne2ps_pbh(data1_512_high, data1_512_low));
    }

    /* Processing the remaining <32 chunk with 8-elements per round */
    for (int j = tail_index_32; j < tail_index_8; j += 8) {
        _mm_storeu_si128((__m128i *)&out[j], (__m128i) _mm256_cvtneps_pbh(_mm512_cvtpd_ps(_mm512_load_pd(&in[j]))));
    }

    /* Processing the remaining <8 chunk with masked processing */
    if ((n&7) > 0) {
        __m512d data_512 = _mm512_maskz_load_pd(*((__mmask8*) &tail_mask8), &in[tail_index_8]);
        _mm_mask_storeu_epi16(&out[tail_index_8], *((__mmask8*) &tail_mask8), (__m128i) _mm256_cvtneps_pbh(_mm512_cvtpd_ps(data_512)));
    }
}

#endif
