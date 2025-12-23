/***************************************************************************
Copyright (c) 2021, The OpenBLAS Project
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
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX512CD__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )


#include <immintrin.h>
#include "common.h"
#include <stdio.h>
#include <memory.h>

#define DECLARE_RESULT_512(M, N) __m512 result##M##N = _mm512_setzero_ps()
#define MATMUL_512(M, N) result##M##N = _mm512_fmadd_ps(Aval##M, Bval##N, result##M##N)

#define LOAD_KA_512(M, N) __m512 Aval##M = _mm512_loadu_ps(&A[(i + M)*lda + k]);
#define LOAD_KB_512(M, N) __m512 Bval##N = _mm512_loadu_ps(&B[(j + N)*ldb + k])
#define MASK_LOAD_KA_512(M, N) __m512 Aval##M = _mm512_maskz_loadu_ps(mask, &A[(i + M)*lda + k])
#define MASK_LOAD_KB_512(M, N) __m512 Bval##N = _mm512_maskz_loadu_ps(mask, &B[(j + N)*ldb + k])

#define REDUCE_4(rr0, rr1, rr2, rr3) \
	__m512 r0, r1, r2, r3, t0, t1, t2, t3;\
	r0 = _mm512_unpacklo_ps(rr0, rr1); r1 = _mm512_unpackhi_ps(rr0, rr1); \
	r2 = _mm512_unpacklo_ps(rr2, rr3); r3 = _mm512_unpackhi_ps(rr2, rr3); \
	t0 = _mm512_shuffle_ps(r0, r2, _MM_SHUFFLE(1, 0, 1, 0)); t1 = _mm512_shuffle_ps(r0, r2, _MM_SHUFFLE(3, 2, 3, 2)); \
	t2 = _mm512_shuffle_ps(r1, r3, _MM_SHUFFLE(1, 0, 1, 0)); t3 = _mm512_shuffle_ps(r1, r3, _MM_SHUFFLE(3, 2, 3, 2)); \
	r0 = _mm512_add_ps(t0, t1); r1 = _mm512_add_ps(t2, t3); t0 = _mm512_add_ps(r0, r1); \
	__m128 s0, s1, s2, s3; \
	s0 = _mm512_extractf32x4_ps(t0, 0); s1 = _mm512_extractf32x4_ps(t0, 1); s2 = _mm512_extractf32x4_ps(t0, 2); s3 = _mm512_extractf32x4_ps(t0, 3); \
	s0 = _mm_maskz_add_ps(mask8, s0, s1); s2 = _mm_maskz_add_ps(mask8, s2, s3); s0 = _mm_maskz_add_ps(mask8, s0, s2); \
	s0 = _mm_maskz_mul_ps(mask8, alpha_128, s0);

#define REDUCE_M4(N) REDUCE_4(result0##N, result1##N, result2##N, result3##N)
#define REDUCE_N4(M) REDUCE_4(result##M##0, result##M##1, result##M##2, result##M##3)

#if defined(B0)
#define STORE_REDUCE(M, N) C[(j+N)*ldc + i + M] = alpha * _mm512_reduce_add_ps(result##M##N)
#define STORE_M4(N, s0) _mm_mask_storeu_ps(&C[(j + N)*ldc + i], mask8, s0);
#define STORE_N4(M, s0) _mm_i32scatter_ps(&C[j*ldc + i + M], vindex_n, s0, 4);
#else
#define STORE_REDUCE(M, N) C[(j+N)*ldc + i + M] = alpha * _mm512_reduce_add_ps(result##M##N) + beta * C[(j+N)*ldc + i + M]
#define STORE_M4(N, s0) \
	asm("vfmadd231ps (%1), %2, %0": "+v"(s0):"r"(&C[(j + N)*ldc + i]), "v"(beta_128)); \
	_mm_mask_storeu_ps(&C[(j + N)*ldc + i], mask8, s0);

#define STORE_N4(M, s0) \
	s0 = _mm_fmadd_ps(_mm_i32gather_ps(&C[j*ldc + i + M], vindex_n, 4), beta_128, s0); \
	_mm_i32scatter_ps(&C[j*ldc + i + M], vindex_n, s0, 4);
#endif
#define STORE_REDUCE_M4(N) {\
	REDUCE_M4(N) \
	STORE_M4(N, s0) \
}
#define STORE_REDUCE_N4(M) {\
	REDUCE_N4(M) \
	STORE_N4(M, s0) \
}


#if defined(B0)
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT beta, FLOAT * C, BLASLONG ldc)
#endif
{
	// column major
	BLASLONG i, j, k;

	BLASLONG m4 = M & ~3;
	BLASLONG m2 = M & ~1;

	BLASLONG n4 = N & ~3;
	BLASLONG n2 = N & ~1;

	BLASLONG k16 = K & ~15;

	__mmask16 mask;
	__mmask8 mask8 = 0xff;  // just use to avoid SSE instruction

	__m128i vindex_n = _mm_set_epi32(ldc*3, ldc*2, ldc, 0);
	__m128 alpha_128 = _mm_broadcast_ss(&alpha);
#if !defined(B0)
	__m128 beta_128 = _mm_broadcast_ss(&beta);
#endif
	for (i = 0; i < m4; i += 4) {
		for (j = 0; j < n4; j += 4) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2); DECLARE_RESULT_512(2, 2); DECLARE_RESULT_512(3, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3); DECLARE_RESULT_512(2, 3); DECLARE_RESULT_512(3, 3);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x); LOAD_KA_512(1, x); LOAD_KA_512(2, x); LOAD_KA_512(3, x);
				LOAD_KB_512(x, 0); LOAD_KB_512(x, 1); LOAD_KB_512(x, 2); LOAD_KB_512(x, 3);

				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2); MATMUL_512(2, 2); MATMUL_512(3, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3); MATMUL_512(2, 3); MATMUL_512(3, 3);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x); MASK_LOAD_KA_512(1, x); MASK_LOAD_KA_512(2, x); MASK_LOAD_KA_512(3, x);
				MASK_LOAD_KB_512(x, 0); MASK_LOAD_KB_512(x, 1); MASK_LOAD_KB_512(x, 2); MASK_LOAD_KB_512(x, 3);

				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2); MATMUL_512(2, 2); MATMUL_512(3, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3); MATMUL_512(2, 3); MATMUL_512(3, 3);
			}
			STORE_REDUCE_M4(0); STORE_REDUCE_M4(1); STORE_REDUCE_M4(2); STORE_REDUCE_M4(3);
		}
		for (; j < n2; j += 2) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x); LOAD_KA_512(1, x); LOAD_KA_512(2, x); LOAD_KA_512(3, x);
				LOAD_KB_512(x, 0); LOAD_KB_512(x, 1);

				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x); MASK_LOAD_KA_512(1, x); MASK_LOAD_KA_512(2, x); MASK_LOAD_KA_512(3, x);
				MASK_LOAD_KB_512(x, 0); MASK_LOAD_KB_512(x, 1);

				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
			}
			STORE_REDUCE_M4(0); STORE_REDUCE_M4(1);
		}
		for (; j < N; j += 1) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x); LOAD_KA_512(1, x); LOAD_KA_512(2, x); LOAD_KA_512(3, x);
				LOAD_KB_512(x, 0);

				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x); MASK_LOAD_KA_512(1, x); MASK_LOAD_KA_512(2, x); MASK_LOAD_KA_512(3, x);
				MASK_LOAD_KB_512(x, 0);

				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
			}
			STORE_REDUCE_M4(0);
		}

	}
	for (; i < m2; i += 2) {
		for (j = 0; j < n4; j += 4) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x); LOAD_KA_512(1, x);
				LOAD_KB_512(x, 0); LOAD_KB_512(x, 1); LOAD_KB_512(x, 2); LOAD_KB_512(x, 3);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x); MASK_LOAD_KA_512(1, x);
				MASK_LOAD_KB_512(x, 0); MASK_LOAD_KB_512(x, 1); MASK_LOAD_KB_512(x, 2); MASK_LOAD_KB_512(x, 3);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3);
			}
			STORE_REDUCE_N4(0); STORE_REDUCE_N4(1);
		}
		for (; j < n2; j += 2) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x); LOAD_KA_512(1, x);
				LOAD_KB_512(x, 0); LOAD_KB_512(x, 1);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x); MASK_LOAD_KA_512(1, x);
				MASK_LOAD_KB_512(x, 0); MASK_LOAD_KB_512(x, 1);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
			}
			STORE_REDUCE(0, 0); STORE_REDUCE(1, 0);
			STORE_REDUCE(0, 1); STORE_REDUCE(1, 1);

		}
		for (; j < N; j += 1) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x); LOAD_KA_512(1, x);
				LOAD_KB_512(x, 0);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x); MASK_LOAD_KA_512(1, x);
				MASK_LOAD_KB_512(x, 0);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
			}
			STORE_REDUCE(0, 0); STORE_REDUCE(1, 0);
		}
	}
	for (; i < M; i += 1) {
		for (j = 0; j < n4; j += 4) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x);
				LOAD_KB_512(x, 0); LOAD_KB_512(x, 1); LOAD_KB_512(x, 2); LOAD_KB_512(x, 3);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x);
				MASK_LOAD_KB_512(x, 0); MASK_LOAD_KB_512(x, 1); MASK_LOAD_KB_512(x, 2); MASK_LOAD_KB_512(x, 3);


				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
			}
			STORE_REDUCE_N4(0);
		}
		for (; j < n2; j += 2) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x);
				LOAD_KB_512(x, 0); LOAD_KB_512(x, 1);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x);
				MASK_LOAD_KB_512(x, 0); MASK_LOAD_KB_512(x, 1);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
			}
			STORE_REDUCE(0, 0);
			STORE_REDUCE(0, 1);

		}
		for (; j < N; j += 1) {
			DECLARE_RESULT_512(0, 0);
			for (k = 0; k < k16; k += 16) {
				LOAD_KA_512(0, x);
				LOAD_KB_512(x, 0);

				MATMUL_512(0, 0);
			}
			int remains = K - k;
			if (remains) {
				mask = (1UL << remains) - 1;
				MASK_LOAD_KA_512(0, x);
				MASK_LOAD_KB_512(x, 0);

				MATMUL_512(0, 0);
			}
			STORE_REDUCE(0, 0);
		}
	}
	return 0;
}
#else
#include "../generic/gemm_small_matrix_kernel_tn.c"
#endif

