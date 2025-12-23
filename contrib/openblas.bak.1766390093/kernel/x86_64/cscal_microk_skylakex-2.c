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
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX2__)) || (defined(__clang__) && __clang_major__ >= 6)) || ( defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#include <immintrin.h>

#define HAVE_KERNEL_16 1

static void cscal_kernel_16( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

#ifdef __AVX512CD__
	/* _mm512_addsub_ps does not exist so we flip signs for odd elements of da_i */
	__m512 da_r = _mm512_set1_ps(alpha[0]);
	__m512 da_i = _mm512_set1_ps(alpha[1]) * _mm512_set4_ps(1, -1, 1, -1);
	for (; i < n2; i += 32) {
                __m512 x0 = _mm512_loadu_ps(&x[i +  0]);
                __m512 x1 = _mm512_loadu_ps(&x[i + 16]);
                __m512 y0 = _mm512_permute_ps(x0, 0xb1);
                __m512 y1 = _mm512_permute_ps(x1, 0xb1);
                _mm512_storeu_ps(&x[i +  0], _mm512_add_ps(da_r * x0, da_i * y0));
                _mm512_storeu_ps(&x[i + 16], _mm512_add_ps(da_r * x1, da_i * y1));
	}
#else
	__m256 da_r = _mm256_set1_ps(alpha[0]);
	__m256 da_i = _mm256_set1_ps(alpha[1]);
	for (; i < n2; i += 32) {
                __m256 x0 = _mm256_loadu_ps(&x[i +  0]);
                __m256 x1 = _mm256_loadu_ps(&x[i +  8]);
                __m256 x2 = _mm256_loadu_ps(&x[i + 16]);
                __m256 x3 = _mm256_loadu_ps(&x[i + 24]);
                __m256 y0 = _mm256_permute_ps(x0, 0xb1);
                __m256 y1 = _mm256_permute_ps(x1, 0xb1);
                __m256 y2 = _mm256_permute_ps(x2, 0xb1);
                __m256 y3 = _mm256_permute_ps(x3, 0xb1);
                _mm256_storeu_ps(&x[i +  0], _mm256_addsub_ps(da_r * x0, da_i * y0));
                _mm256_storeu_ps(&x[i +  8], _mm256_addsub_ps(da_r * x1, da_i * y1));
                _mm256_storeu_ps(&x[i + 16], _mm256_addsub_ps(da_r * x2, da_i * y2));
                _mm256_storeu_ps(&x[i + 24], _mm256_addsub_ps(da_r * x3, da_i * y3));
	}
#endif
}


static void cscal_kernel_16_zero_r( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

#ifdef __AVX512CD__
	__m512 da_i = _mm512_set1_ps(alpha[1]) * _mm512_set4_ps(1, -1, 1, -1);
	for (; i < n2; i += 32) {
                __m512 y0 = _mm512_permute_ps(_mm512_loadu_ps(&x[i +  0]), 0xb1);
                __m512 y1 = _mm512_permute_ps(_mm512_loadu_ps(&x[i + 16]), 0xb1);
                _mm512_storeu_ps(&x[i +  0], da_i * y0);
                _mm512_storeu_ps(&x[i + 16], da_i * y1);
	}
#else
	__m256 da_i = _mm256_set1_ps(alpha[1]) * _mm256_set_ps(1, -1, 1, -1, 1, -1, 1, -1);
	for (; i < n2; i += 32) {
                __m256 y0 = _mm256_permute_ps(_mm256_loadu_ps(&x[i +  0]), 0xb1);
                __m256 y1 = _mm256_permute_ps(_mm256_loadu_ps(&x[i +  8]), 0xb1);
                __m256 y2 = _mm256_permute_ps(_mm256_loadu_ps(&x[i + 16]), 0xb1);
                __m256 y3 = _mm256_permute_ps(_mm256_loadu_ps(&x[i + 24]), 0xb1);
                _mm256_storeu_ps(&x[i +  0], da_i * y0);
                _mm256_storeu_ps(&x[i +  8], da_i * y1);
                _mm256_storeu_ps(&x[i + 16], da_i * y2);
                _mm256_storeu_ps(&x[i + 24], da_i * y3);
	}
#endif
}


static void cscal_kernel_16_zero_i( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

#ifdef __AVX512CD__
	__m512 da_r = _mm512_set1_ps(alpha[0]);
	for (; i < n2; i += 32) {
                _mm512_storeu_ps(&x[i +  0], da_r * _mm512_loadu_ps(&x[i +  0]));
                _mm512_storeu_ps(&x[i + 16], da_r * _mm512_loadu_ps(&x[i + 16]));
	}
#else
	__m256 da_r = _mm256_set1_ps(alpha[0]);
	for (; i < n2; i += 32) {
                _mm256_storeu_ps(&x[i +  0], da_r * _mm256_loadu_ps(&x[i +  0]));
                _mm256_storeu_ps(&x[i +  8], da_r * _mm256_loadu_ps(&x[i +  8]));
                _mm256_storeu_ps(&x[i + 16], da_r * _mm256_loadu_ps(&x[i + 16]));
                _mm256_storeu_ps(&x[i + 24], da_r * _mm256_loadu_ps(&x[i + 24]));
	}
#endif
}


static void cscal_kernel_16_zero( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	BLASLONG i = 0;
	BLASLONG n2 = n + n;

	/* question to self: Why is this not just memset() */

#ifdef __AVX512CD__
	__m512 zero = _mm512_setzero_ps();
	for (; i < n2; i += 32) {
                _mm512_storeu_ps(&x[i], zero);
                _mm512_storeu_ps(&x[i + 16], zero);
	}
#else
	__m256 zero = _mm256_setzero_ps();
	for (; i < n2; i += 32) {
                _mm256_storeu_ps(&x[i +  0], zero);
                _mm256_storeu_ps(&x[i +  8], zero);
                _mm256_storeu_ps(&x[i + 16], zero);
                _mm256_storeu_ps(&x[i + 24], zero);
	}
#endif

}

#else
#include "cscal_microk_haswell-2.c"
#endif
