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

static void tobf16_accl_kernel(BLASLONG n, const float * in, bfloat16 * out)
{
    /* Get the 64-bytes unaligned header number targeting for avx512
     * processing (Assume input float array is natural aligned) */
    int align_header = ((64 - ((uintptr_t)in & (uintptr_t)0x3f)) >> 2) & 0xf;

    if (n < align_header) {align_header = n;}

    if (align_header != 0) {
        uint16_t align_mask16 = (((uint16_t)0xffff) >> (16-align_header));
        __m512 a = _mm512_maskz_loadu_ps(*((__mmask16*) &align_mask16), &in[0]);
        _mm256_mask_storeu_epi16(&out[0], *((__mmask16*) &align_mask16), (__m256i) _mm512_cvtneps_pbh(a));
    }

    if (n == align_header) {
        return;
    } else {
        n -= align_header;
        in += align_header;
        out += align_header;
    }

    int tail_index_32  = n&(~31);
    int tail_index_128 = n&(~127);
    uint32_t tail_mask32 = (((uint32_t) 0xffffffff) >> (32-(n&31)));
    uint16_t tail_mask16 = (((uint16_t) 0xffff)     >> (16-(n&15)));

    /* Processing the main chunk with 128-elements per round */
    for (int i = 0; i < tail_index_128; i += 128) {
        _mm512_storeu_si512(&out[i+ 0], (__m512i) _mm512_cvtne2ps_pbh(_mm512_load_ps(&in[i+ 16]), _mm512_load_ps(&in[i+ 0])));
        _mm512_storeu_si512(&out[i+32], (__m512i) _mm512_cvtne2ps_pbh(_mm512_load_ps(&in[i+ 48]), _mm512_load_ps(&in[i+32])));
        _mm512_storeu_si512(&out[i+64], (__m512i) _mm512_cvtne2ps_pbh(_mm512_load_ps(&in[i+ 80]), _mm512_load_ps(&in[i+64])));
        _mm512_storeu_si512(&out[i+96], (__m512i) _mm512_cvtne2ps_pbh(_mm512_load_ps(&in[i+112]), _mm512_load_ps(&in[i+96])));
    }

    /* Processing the remaining <128 chunk with 32-elements per round */
    for (int j = tail_index_128; j < tail_index_32; j += 32) {
        _mm512_storeu_si512(&out[j], (__m512i) _mm512_cvtne2ps_pbh(_mm512_load_ps(&in[j+ 16]), _mm512_load_ps(&in[j])));
    }

    /* Processing the remaining <32 chunk with masked processing */
    if ((n&31) > 15) {
        __m512 b = _mm512_load_ps(&in[tail_index_32]);
        __m512 a = _mm512_maskz_load_ps(*((__mmask16*) &tail_mask16), &in[tail_index_32+16]);
        _mm512_mask_storeu_epi16(&out[tail_index_32], *((__mmask32*) &tail_mask32), (__m512i) _mm512_cvtne2ps_pbh(a, b));
    } else if ((n&31) > 0) {
        __m512 a = _mm512_maskz_load_ps(*((__mmask16*) &tail_mask16), &in[tail_index_32]);
        _mm256_mask_storeu_epi16(&out[tail_index_32], *((__mmask16*) &tail_mask16), (__m256i) _mm512_cvtneps_pbh(a));
    }
}

#endif
