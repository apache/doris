/***************************************************************************
Copyright (c) 2014-2015, The OpenBLAS Project
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
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX2__)) || (defined(__clang__) && __clang_major__ >= 6)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )


#include <immintrin.h>

#define HAVE_KERNEL_8 1

static void zscal_kernel_8( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

#ifdef __AVX512CD__
	/* _mm512_addsub_pd does not exist so we flip signs for odd elements of da_i */
	__m512d da_r = _mm512_set1_pd(alpha[0]);
	__m512d da_i = _mm512_set1_pd(alpha[1]) * _mm512_set4_pd(1, -1, 1, -1);
	for (; i < n2; i += 16) {
                __m512d x0 = _mm512_loadu_pd(&x[i +  0]);
                __m512d x1 = _mm512_loadu_pd(&x[i +  8]);
                __m512d y0 = _mm512_permute_pd(x0, 0x55);
                __m512d y1 = _mm512_permute_pd(x1, 0x55);
                _mm512_storeu_pd(&x[i +  0], _mm512_add_pd(da_r * x0, da_i * y0));
                _mm512_storeu_pd(&x[i +  8], _mm512_add_pd(da_r * x1, da_i * y1));
	}
#else
	__m256d da_r = _mm256_set1_pd(alpha[0]);
	__m256d da_i = _mm256_set1_pd(alpha[1]);
	for (; i < n2; i += 16) {
                __m256d x0 = _mm256_loadu_pd(&x[i +  0]);
                __m256d x1 = _mm256_loadu_pd(&x[i +  4]);
                __m256d x2 = _mm256_loadu_pd(&x[i +  8]);
                __m256d x3 = _mm256_loadu_pd(&x[i + 12]);
                __m256d y0 = _mm256_permute_pd(x0, 0x05);
                __m256d y1 = _mm256_permute_pd(x1, 0x05);
                __m256d y2 = _mm256_permute_pd(x2, 0x05);
                __m256d y3 = _mm256_permute_pd(x3, 0x05);
                _mm256_storeu_pd(&x[i +  0], _mm256_addsub_pd(da_r * x0, da_i * y0));
                _mm256_storeu_pd(&x[i +  4], _mm256_addsub_pd(da_r * x1, da_i * y1));
                _mm256_storeu_pd(&x[i +  8], _mm256_addsub_pd(da_r * x2, da_i * y2));
                _mm256_storeu_pd(&x[i + 12], _mm256_addsub_pd(da_r * x3, da_i * y3));
	}
#endif
}


static void zscal_kernel_8_zero_r( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

#ifdef __AVX512CD__
	__m512d da_i = _mm512_set1_pd(alpha[1]) * _mm512_set4_pd(1, -1, 1, -1);
	for (; i < n2; i += 16) {
                __m512d y0 = _mm512_permute_pd(_mm512_loadu_pd(&x[i +  0]), 0x55);
                __m512d y1 = _mm512_permute_pd(_mm512_loadu_pd(&x[i +  8]), 0x55);
                _mm512_storeu_pd(&x[i +  0], da_i * y0);
                _mm512_storeu_pd(&x[i +  8], da_i * y1);
	}
#else
	__m256d da_i = _mm256_set1_pd(alpha[1]) * _mm256_set_pd(1, -1, 1, -1);
	for (; i < n2; i += 16) {
                __m256d y0 = _mm256_permute_pd(_mm256_loadu_pd(&x[i +  0]), 0x05);
                __m256d y1 = _mm256_permute_pd(_mm256_loadu_pd(&x[i +  8]), 0x05);
                __m256d y2 = _mm256_permute_pd(_mm256_loadu_pd(&x[i + 16]), 0x05);
                __m256d y3 = _mm256_permute_pd(_mm256_loadu_pd(&x[i + 24]), 0x05);
                _mm256_storeu_pd(&x[i +  0], da_i * y0);
                _mm256_storeu_pd(&x[i +  4], da_i * y1);
                _mm256_storeu_pd(&x[i +  8], da_i * y2);
                _mm256_storeu_pd(&x[i + 12], da_i * y3);
	}
#endif
}


static void zscal_kernel_8_zero_i( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

#ifdef __AVX512CD__
	__m512d da_r = _mm512_set1_pd(alpha[0]);
	for (; i < n2; i += 16) {
                _mm512_storeu_pd(&x[i +  0], da_r * _mm512_loadu_pd(&x[i +  0]));
                _mm512_storeu_pd(&x[i +  8], da_r * _mm512_loadu_pd(&x[i +  8]));
	}
#else
	__m256d da_r = _mm256_set1_pd(alpha[0]);
	for (; i < n2; i += 16) {
                _mm256_storeu_pd(&x[i +  0], da_r * _mm256_loadu_pd(&x[i +  0]));
                _mm256_storeu_pd(&x[i +  4], da_r * _mm256_loadu_pd(&x[i +  4]));
                _mm256_storeu_pd(&x[i +  8], da_r * _mm256_loadu_pd(&x[i +  8]));
                _mm256_storeu_pd(&x[i + 12], da_r * _mm256_loadu_pd(&x[i + 12]));
	}
#endif
}


static void zscal_kernel_8_zero( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

	/* question to self: Why is this not just memset() */

#ifdef __AVX512CD__
	__m512d zero = _mm512_setzero_pd();
	for (; i < n2; i += 16) {
                _mm512_storeu_pd(&x[i], zero);
                _mm512_storeu_pd(&x[i +  8], zero);
	}
#else
	__m256d zero = _mm256_setzero_pd();
	for (; i < n2; i += 16) {
                _mm256_storeu_pd(&x[i +  0], zero);
                _mm256_storeu_pd(&x[i +  4], zero);
                _mm256_storeu_pd(&x[i +  8], zero);
                _mm256_storeu_pd(&x[i + 12], zero);
	}
#endif

}

#else
#include "zscal_microk_haswell-2.c"
#endif
