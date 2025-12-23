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

#define HAVE_KERNEL_16 1

static void sscal_kernel_16( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	int i = 0;

#ifdef __AVX512CD__
	__m512 __alpha5 = _mm512_broadcastss_ps(_mm_load_ss(alpha));
	BLASLONG nn = n & -32;
	for (; i < nn; i += 32) {
		__m512 a = _mm512_loadu_ps(&x[i +  0]);
		__m512 b = _mm512_loadu_ps(&x[i + 16]);
		a *= __alpha5;
		b *= __alpha5;
		_mm512_storeu_ps(&x[i +  0], a);
                _mm512_storeu_ps(&x[i + 16], b);
	}
	for (; i < n; i += 16) {
                _mm512_storeu_ps(&x[i +  0], __alpha5 * _mm512_loadu_ps(&x[i +  0]));
	}
#else
	__m256 __alpha = _mm256_broadcastss_ps(_mm_load_ss(alpha));
	for (; i < n; i += 16) {
                _mm256_storeu_ps(&x[i +  0], __alpha * _mm256_loadu_ps(&x[i +  0]));
                _mm256_storeu_ps(&x[i +  8], __alpha * _mm256_loadu_ps(&x[i +  8]));
	}
#endif
}


static void sscal_kernel_16_zero( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	int i = 0;

	/* question to self: Why is this not just memset() */

#ifdef __AVX512CD__
	__m512 zero = _mm512_setzero_ps();
	for (; i < n; i += 16) {
                _mm512_storeu_ps(&x[i], zero);
	}
#else
	__m256 zero = _mm256_setzero_ps();
	for (; i < n; i += 16) {
                _mm256_storeu_ps(&x[i +  0], zero);
                _mm256_storeu_ps(&x[i +  8], zero);
	}
#endif

}

#else
#include "sscal_microk_haswell-2.c"
#endif
