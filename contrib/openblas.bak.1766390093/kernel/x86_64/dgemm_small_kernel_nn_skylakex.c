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

#define DECLARE_RESULT_512(M, N) __m512d result##M##N = _mm512_setzero_pd()
#define LOAD_A_512(M, N) __m512d Aval##M = _mm512_loadu_pd(&A[lda * k + i + (M*8)])
#define MASK_LOAD_A_512(M, N) __m512d Aval##M = _mm512_maskz_loadu_pd(mask, &A[lda * k + i + (M*8)])
#define BROADCAST_LOAD_B_512(M, N) __m512d Bval##N = _mm512_broadcastsd_pd(_mm_load_pd1(&B[k + ldb * (j+N)]))
#define MATMUL_512(M, N) result##M##N = _mm512_fmadd_pd(Aval##M, Bval##N, result##M##N)
#if defined(B0)
#define STORE_512(M, N) result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
			_mm512_storeu_pd(&C[(j+N)*ldc + i + (M*8)], result##M##N)
#define MASK_STORE_512(M, N) result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
			_mm512_mask_storeu_pd(&C[(j+N)*ldc + i + (M*8)], mask, result##M##N)
#else
#define STORE_512(M, N) \
	result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
	asm("vfmadd231pd (%1), %2, %0": "+v"(result##M##N):"r"(&C[(j+N)*ldc + i + (M*8)]), "v"(beta_512)); \
	_mm512_storeu_pd(&C[(j+N)*ldc + i + (M*8)], result##M##N)
#define MASK_STORE_512(M, N) \
	result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
	asm("vfmadd231pd (%1), %2, %0 %{%3%}": "+v"(result##M##N):"r"(&C[(j+N)*ldc + i + (M*8)]), "v"(beta_512), "Yk"(mask)); \
	_mm512_mask_storeu_pd(&C[(j+N)*ldc + i + (M*8)], mask, result##M##N)
#endif

#define LOAD_KA_512(M, N) __m512d Aval##M = _mm512_loadu_pd(&mbuf[(mi + M)*K + k]);
#define LOAD_KB_512(M, N) __m512d Bval##N = _mm512_loadu_pd(&B[(j + N)*ldb + k])
#define MASK_LOAD_KA_512(M, N) __m512d Aval##M = _mm512_maskz_loadu_pd(mask, &mbuf[(mi + M)*K + k])
#define MASK_LOAD_KB_512(M, N) __m512d Bval##N = _mm512_maskz_loadu_pd(mask, &B[(j + N)*ldb + k])
#define REDUCE_4(rr0, rr1, rr2, rr3) \
	__m512d r0, r1, r2, r3, t0, t1, t2, t3;\
	r0 = _mm512_unpacklo_pd(rr0, rr1); r1 = _mm512_unpackhi_pd(rr0, rr1); \
	r2 = _mm512_unpacklo_pd(rr2, rr3); r3 = _mm512_unpackhi_pd(rr2, rr3); \
	t0 = _mm512_permutex2var_pd(r0, idx_lo, r2); t1 = _mm512_permutex2var_pd(r1, idx_lo, r3); \
	t2 = _mm512_permutex2var_pd(r0, idx_hi, r2); t3 = _mm512_permutex2var_pd(r1, idx_hi, r3); \
	r0 = _mm512_add_pd(t0, t1); r1 = _mm512_add_pd(t2, t3); t0 = _mm512_add_pd(r0, r1); \
	__m256d s0, s1; \
	s0 = _mm512_extractf64x4_pd(t0, 0); s1 = _mm512_extractf64x4_pd(t0, 1); \
	s0 = _mm256_add_pd(s0, s1); s0 = _mm256_mul_pd(alpha_256, s0);
#define REDUCE_M4(N) REDUCE_4(result0##N, result1##N, result2##N, result3##N)
#define REDUCE_N4(M) REDUCE_4(result##M##0, result##M##1, result##M##2, result##M##3)
#if defined(B0)
#define STORE_REDUCE(M, N) C[(j+N)*ldc + i + M] = alpha * _mm512_reduce_add_pd(result##M##N);
#define STORE_REDUCE_M4(N) {\
	REDUCE_M4(N) \
	_mm256_storeu_pd(&C[(j + N)*ldc + i], s0); \
}
#define STORE_REDUCE_N4(M) {\
	REDUCE_N4(M) \
	_mm256_i64scatter_pd(&C[j*ldc + i + M], vindex_n, s0, 8); \
}
#else
#define STORE_REDUCE(M, N) C[(j+N)*ldc + i + M] = alpha * _mm512_reduce_add_pd(result##M##N) + beta * C[(j+N)*ldc + i + M];
#define STORE_REDUCE_M4(N) {\
	REDUCE_M4(N) \
	asm("vfmadd231pd (%1), %2, %0": "+v"(s0):"r"(&C[(j + N)*ldc + i]), "v"(beta_256)); \
	_mm256_storeu_pd(&C[(j + N)*ldc + i], s0); \
}
#define STORE_REDUCE_N4(M) {\
	REDUCE_N4(M) \
	s1 = _mm256_i64gather_pd(&C[j*ldc + i + M], vindex_n, 8); \
	s0 = _mm256_fmadd_pd(s1, beta_256, s0); \
	_mm256_i64scatter_pd(&C[j*ldc + i + M], vindex_n, s0, 8); \
}
#endif

#if defined(B0)
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT beta, FLOAT * C, BLASLONG ldc)
#endif
{
	// column major
	BLASLONG i, j, k;

	BLASLONG m32 = M & ~31;
	BLASLONG m16 = M & ~15;
	BLASLONG m8 = M & ~7;
	BLASLONG m4 = M & ~3;
	BLASLONG m2 = M & ~1;

	BLASLONG n6 = N - (N % 6);
	BLASLONG n4 = N & ~3;
	BLASLONG n2 = N & ~1;


	__m512d alpha_512 = _mm512_broadcastsd_pd(_mm_load_pd1(&alpha));
#if !defined(B0)
	__m512d beta_512 = _mm512_broadcastsd_pd(_mm_load_pd1(&beta));
#endif

	for (i = 0; i < m32; i += 32) {
		for (j = 0; j < n4; j += 4) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2); DECLARE_RESULT_512(2, 2); DECLARE_RESULT_512(3, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3); DECLARE_RESULT_512(2, 3); DECLARE_RESULT_512(3, 3);

			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x); LOAD_A_512(2, x); LOAD_A_512(3, x);

				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);

				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2); MATMUL_512(2, 2); MATMUL_512(3, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3); MATMUL_512(2, 3); MATMUL_512(3, 3);
			}
			STORE_512(0, 0); STORE_512(1, 0); STORE_512(2, 0); STORE_512(3, 0);
			STORE_512(0, 1); STORE_512(1, 1); STORE_512(2, 1); STORE_512(3, 1);
			STORE_512(0, 2); STORE_512(1, 2); STORE_512(2, 2); STORE_512(3, 2);
			STORE_512(0, 3); STORE_512(1, 3); STORE_512(2, 3); STORE_512(3, 3);
		}
		for (; j < n2; j += 2) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x); LOAD_A_512(2, x); LOAD_A_512(3, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
			}
			STORE_512(0, 0); STORE_512(1, 0); STORE_512(2, 0); STORE_512(3, 0);
			STORE_512(0, 1); STORE_512(1, 1); STORE_512(2, 1); STORE_512(3, 1);
		}
		for (; j < N; j++) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x); LOAD_A_512(2, x); LOAD_A_512(3, x);
				BROADCAST_LOAD_B_512(x, 0);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
			}
			STORE_512(0, 0); STORE_512(1, 0); STORE_512(2, 0); STORE_512(3, 0);
		}
	}
	for (; i < m16; i += 16) {
		for (j = 0; j < n6; j += 6) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3);
			DECLARE_RESULT_512(0, 4); DECLARE_RESULT_512(1, 4);
			DECLARE_RESULT_512(0, 5); DECLARE_RESULT_512(1, 5);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);
				BROADCAST_LOAD_B_512(x, 4); BROADCAST_LOAD_B_512(x, 5);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3);
				MATMUL_512(0, 4); MATMUL_512(1, 4);
				MATMUL_512(0, 5); MATMUL_512(1, 5);
			}
			STORE_512(0, 0); STORE_512(1, 0);
			STORE_512(0, 1); STORE_512(1, 1);
			STORE_512(0, 2); STORE_512(1, 2);
			STORE_512(0, 3); STORE_512(1, 3);
			STORE_512(0, 4); STORE_512(1, 4);
			STORE_512(0, 5); STORE_512(1, 5);
		}
		for (; j < n2; j += 2) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
			}
			STORE_512(0, 0); STORE_512(1, 0);
			STORE_512(0, 1); STORE_512(1, 1);
		}
		for (; j < N; j++) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x);
				BROADCAST_LOAD_B_512(x, 0);
				MATMUL_512(0, 0); MATMUL_512(1, 0);
			}
			STORE_512(0, 0); STORE_512(1, 0);
		}
	}
	for (; i < m8; i += 8) {
		for (j = 0; j < n6; j += 6) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);
			DECLARE_RESULT_512(0, 4);
			DECLARE_RESULT_512(0, 5);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);
				BROADCAST_LOAD_B_512(x, 4); BROADCAST_LOAD_B_512(x, 5);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
				MATMUL_512(0, 4);
				MATMUL_512(0, 5);
			}
			STORE_512(0, 0);
			STORE_512(0, 1);
			STORE_512(0, 2);
			STORE_512(0, 3);
			STORE_512(0, 4);
			STORE_512(0, 5);
		}
		for (; j < n2; j += 2) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
			}
			STORE_512(0, 0);
			STORE_512(0, 1);
		}
		for (; j < N; j++) {
			DECLARE_RESULT_512(0, 0);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0);
				MATMUL_512(0, 0);
			}
			STORE_512(0, 0);
		}
	}
	int mm = M - i;
	if (!mm) return 0;
	if (mm > 4 || K < 16) {
		register __mmask8 mask = (1UL << mm) - 1;
		for (j = 0; j < n6; j += 6) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);
			DECLARE_RESULT_512(0, 4);
			DECLARE_RESULT_512(0, 5);
			for (k = 0; k < K; k++) {
				MASK_LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);
				BROADCAST_LOAD_B_512(x, 4); BROADCAST_LOAD_B_512(x, 5);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
				MATMUL_512(0, 4);
				MATMUL_512(0, 5);
			}
			MASK_STORE_512(0, 0);
			MASK_STORE_512(0, 1);
			MASK_STORE_512(0, 2);
			MASK_STORE_512(0, 3);
			MASK_STORE_512(0, 4);
			MASK_STORE_512(0, 5);
		}
		for (; j < n2; j += 2) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			for (k = 0; k < K; k++) {
				MASK_LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
			}
			MASK_STORE_512(0, 0);
			MASK_STORE_512(0, 1);
		}
		for (; j < N; j++) {
			DECLARE_RESULT_512(0, 0);
			for (k = 0; k < K; k++) {
				MASK_LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0);
				MATMUL_512(0, 0);
			}
			MASK_STORE_512(0, 0);
		}
	} else {
		/* M => [1, 4]
		 *
		 * This kernel use dot-like style to calc a value - C(x, y):
		 * C(x, y) = A(x, 0)*B(0, y) + A(x, 1)*B(1, y) +....+ A(x, K)*B(K, y)
		 *
		 * Alloc a buf to copy rest of A as row major,
		 * so memory access from 0 to K is continuous for both A & B.
		 *
		 * Loading to zmm and FMA 8 of k at one loop,
		 * finally reduce_add zmm to a single float result in C(x, y).
		 *
		 * Note: performance is bad when K is small.
		 */
		FLOAT *mbuf = (FLOAT *) malloc(sizeof(FLOAT)*mm*K);
		__mmask8 mask = (1UL << mm) - 1;
		BLASLONG k8 = K & ~7;
		BLASLONG k4 = K & ~3;
		for (k = 0; k < k4; k += 4) {
			__m256d  r0, r1, r2, r3;
			__m256d  t0, t1, t2, t3;
			r0 = _mm256_maskz_loadu_pd(mask, &A[i + lda*(0 + k)]);
			r1 = _mm256_maskz_loadu_pd(mask, &A[i + lda*(1 + k)]);
			r2 = _mm256_maskz_loadu_pd(mask, &A[i + lda*(2 + k)]);
			r3 = _mm256_maskz_loadu_pd(mask, &A[i + lda*(3 + k)]);

			t0 = _mm256_unpacklo_pd(r0, r1);
			t1 = _mm256_unpackhi_pd(r0, r1);
			t2 = _mm256_unpacklo_pd(r2, r3);
			t3 = _mm256_unpackhi_pd(r2, r3);

			r0 = _mm256_permute2f128_pd(t0, t2, 0x20);
			r1 = _mm256_permute2f128_pd(t1, t3, 0x20);
			r2 = _mm256_permute2f128_pd(t0, t2, 0x31);
			r3 = _mm256_permute2f128_pd(t1, t3, 0x31);

			switch (mm) {
				case 4: _mm256_storeu_pd(&mbuf[k + 3*K], r3);
				case 3: _mm256_storeu_pd(&mbuf[k + 2*K], r2);
				case 2: _mm256_storeu_pd(&mbuf[k + 1*K], r1);
				case 1: _mm256_storeu_pd(&mbuf[k + 0*K], r0);
			}
		}
		for (; k < K; k++) {
			for (int ii = 0; ii < mm; ii++) {
				mbuf[k + ii*K] = A[i + lda*k + ii];
			}
		}
		int mi = 0;
		__m256d alpha_256 = _mm256_broadcast_sd(&alpha);
#if !defined(B0)
		__m256d beta_256 = _mm256_broadcast_sd(&beta);
#endif
		__m256i vindex_n = _mm256_set_epi64x(ldc*3, ldc*2, ldc*1, 0);
		long long permute_table[] = {
			0, 1, 0|8, 1|8, 4, 5, 4|8, 5|8,
			2, 3, 2|8, 3|8, 6, 7, 6|8, 7|8,
		};
		__m512i idx_lo = _mm512_loadu_si512(permute_table);
		__m512i idx_hi = _mm512_loadu_si512(permute_table + 8);
		for (; i < m4; i += 4, mi += 4) {
			for (j = 0; j < n4; j += 4) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
				DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2); DECLARE_RESULT_512(2, 2); DECLARE_RESULT_512(3, 2);
				DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3); DECLARE_RESULT_512(2, 3); DECLARE_RESULT_512(3, 3);
				for (k = 0; k < k8; k += 8) {
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
				for (k = 0; k < k8; k += 8) {
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
				for (k = 0; k < k8; k += 8) {
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
		for (; i < m2; i += 2, mi += 2) {
			for (j = 0; j < n4; j += 4) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
				DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2);
				DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3);
				for (k = 0; k < k8; k += 8) {
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
				for (k = 0; k < k8; k += 8) {
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
				for (k = 0; k < k8; k += 8) {
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
		for (; i < M; i += 1, mi += 1) {
			for (j = 0; j < n4; j += 4) {
				DECLARE_RESULT_512(0, 0);
				DECLARE_RESULT_512(0, 1);
				DECLARE_RESULT_512(0, 2);
				DECLARE_RESULT_512(0, 3);
				for (k = 0; k < k8; k += 8) {
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
				for (k = 0; k < k8; k += 8) {
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
				for (k = 0; k < k8; k += 8) {
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
		free(mbuf);
	}
	return 0;
}
#else
#include "../generic/gemm_small_matrix_kernel_nn.c"
#endif

