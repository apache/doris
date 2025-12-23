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

#include <immintrin.h>
#include "common.h"
#include <stdio.h>
#include <memory.h>

#define DECLARE_RESULT_512(M, N) __m512 result##M##N = _mm512_setzero_ps()
#define LOAD_A_512(M, N) __m512 Aval##M = _mm512_loadu_ps(&A[lda * k + i + (M*16)])
#define MASK_LOAD_A_512(M, N) __m512 Aval##M = _mm512_maskz_loadu_ps(mask, &A[lda * k + i + (M*16)])
#define BROADCAST_LOAD_B_512(M, N) __m512 Bval##N = _mm512_broadcastss_ps(_mm_load_ss(&B[ldb * k + j + N]))
#define MATMUL_512(M, N) result##M##N = _mm512_fmadd_ps(Aval##M, Bval##N, result##M##N)

#define BROADCAST_LOAD_A_512(M, N) __m512 Aval##M = _mm512_broadcastss_ps(_mm_load_ss(&A[lda * k + i + M]))
#define LOAD_B_512(M, N) __m512 Bval##N = _mm512_loadu_ps(&B[ldb * k + j + (N*16)])
#define MASK_LOAD_B_512(M, N) __m512 Bval##N = _mm512_maskz_loadu_ps(mask, &B[ldb * k + j + (N*16)])
#if defined(B0)
#define STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
			_mm512_storeu_ps(&C[(j+N)*ldc + i + (M*16)], result##M##N)
#define MASK_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
			_mm512_mask_storeu_ps(&C[(j+N)*ldc + i + (M*16)], mask, result##M##N)
#define SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				_mm512_i32scatter_ps(&C[(j + N*16)*ldc + i + M], vindex_n, result##M##N, 4);
#define MASK_SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				_mm512_mask_i32scatter_ps(&C[(j + N*16)*ldc + i + M], mask, vindex_n, result##M##N, 4)
#else
#define STORE_512(M, N) \
	result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
	asm("vfmadd231ps (%1), %2, %0": "+v"(result##M##N):"r"(&C[(j+N)*ldc + i + (M*16)]), "v"(beta_512)); \
	_mm512_storeu_ps(&C[(j+N)*ldc + i + (M*16)], result##M##N)
#define MASK_STORE_512(M, N) \
	result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
	asm("vfmadd231ps (%1), %2, %0 %{%3%}": "+v"(result##M##N):"r"(&C[(j+N)*ldc + i + (M*16)]), "v"(beta_512), "Yk"(mask)); \
	_mm512_mask_storeu_ps(&C[(j+N)*ldc + i + (M*16)], mask, result##M##N)
#define SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				__m512 tmp##M##N = _mm512_i32gather_ps(vindex_n, &C[(j + N*16)*ldc + i + M], 4); \
				result##M##N = _mm512_fmadd_ps(tmp##M##N, beta_512, result##M##N); \
				_mm512_i32scatter_ps(&C[(j + N*16)*ldc + i + M], vindex_n, result##M##N, 4);
#define MASK_SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				__m512 tmp##M##N = _mm512_mask_i32gather_ps(_mm512_setzero_ps(), mask, vindex_n, &C[(j + N*16)*ldc + i + M], 4); \
				result##M##N = _mm512_fmadd_ps(tmp##M##N, beta_512, result##M##N); \
				_mm512_mask_i32scatter_ps(&C[(j + N*16)*ldc + i + M], mask, vindex_n, result##M##N, 4);
#endif

#if defined(B0)
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT beta, FLOAT * C, BLASLONG ldc)
#endif
{
	// column major
	BLASLONG i, j, k;

	BLASLONG m64 = M & ~63;
	BLASLONG m32 = M & ~31;
	BLASLONG m16 = M & ~15;
	BLASLONG m4 = M & ~3;
	BLASLONG m2 = M & ~1;

	BLASLONG n64 = N & ~63;
	BLASLONG n32 = N & ~31;
	BLASLONG n8 = N & ~7;
	BLASLONG n6 = N - (N % 6);
	BLASLONG n4 = N & ~3;
	BLASLONG n2 = N & ~1;


	__m512 alpha_512 = _mm512_broadcastss_ps(_mm_load_ss(&alpha));
#if !defined(B0)
	__m512 beta_512 = _mm512_broadcastss_ps(_mm_load_ss(&beta));
#endif

	for (i = 0; i < m64; i += 64) {
		for (j = 0; j < n6; j += 6) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2); DECLARE_RESULT_512(2, 2); DECLARE_RESULT_512(3, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3); DECLARE_RESULT_512(2, 3); DECLARE_RESULT_512(3, 3);
			DECLARE_RESULT_512(0, 4); DECLARE_RESULT_512(1, 4); DECLARE_RESULT_512(2, 4); DECLARE_RESULT_512(3, 4);
			DECLARE_RESULT_512(0, 5); DECLARE_RESULT_512(1, 5); DECLARE_RESULT_512(2, 5); DECLARE_RESULT_512(3, 5);

			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x); LOAD_A_512(2, x); LOAD_A_512(3, x);

				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);
				MATMUL_512(0, 2); MATMUL_512(1, 2); MATMUL_512(2, 2); MATMUL_512(3, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3); MATMUL_512(2, 3); MATMUL_512(3, 3);
				BROADCAST_LOAD_B_512(x, 4); BROADCAST_LOAD_B_512(x, 5);
				MATMUL_512(0, 4); MATMUL_512(1, 4); MATMUL_512(2, 4); MATMUL_512(3, 4);
				MATMUL_512(0, 5); MATMUL_512(1, 5); MATMUL_512(2, 5); MATMUL_512(3, 5);
			}
			STORE_512(0, 0); STORE_512(1, 0); STORE_512(2, 0); STORE_512(3, 0);
			STORE_512(0, 1); STORE_512(1, 1); STORE_512(2, 1); STORE_512(3, 1);
			STORE_512(0, 2); STORE_512(1, 2); STORE_512(2, 2); STORE_512(3, 2);
			STORE_512(0, 3); STORE_512(1, 3); STORE_512(2, 3); STORE_512(3, 3);
			STORE_512(0, 4); STORE_512(1, 4); STORE_512(2, 4); STORE_512(3, 4);
			STORE_512(0, 5); STORE_512(1, 5); STORE_512(2, 5); STORE_512(3, 5);
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
	for (; i < m32; i += 32) {
		for (j = 0; j < n8; j += 8) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3);
			DECLARE_RESULT_512(0, 4); DECLARE_RESULT_512(1, 4);
			DECLARE_RESULT_512(0, 5); DECLARE_RESULT_512(1, 5);
			DECLARE_RESULT_512(0, 6); DECLARE_RESULT_512(1, 6);
			DECLARE_RESULT_512(0, 7); DECLARE_RESULT_512(1, 7);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);
				BROADCAST_LOAD_B_512(x, 4); BROADCAST_LOAD_B_512(x, 5);
				BROADCAST_LOAD_B_512(x, 6); BROADCAST_LOAD_B_512(x, 7);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3);
				MATMUL_512(0, 4); MATMUL_512(1, 4);
				MATMUL_512(0, 5); MATMUL_512(1, 5);
				MATMUL_512(0, 6); MATMUL_512(1, 6);
				MATMUL_512(0, 7); MATMUL_512(1, 7);
			}
			STORE_512(0, 0); STORE_512(1, 0);
			STORE_512(0, 1); STORE_512(1, 1);
			STORE_512(0, 2); STORE_512(1, 2);
			STORE_512(0, 3); STORE_512(1, 3);
			STORE_512(0, 4); STORE_512(1, 4);
			STORE_512(0, 5); STORE_512(1, 5);
			STORE_512(0, 6); STORE_512(1, 6);
			STORE_512(0, 7); STORE_512(1, 7);
		}
		for (;j < n4; j += 4) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x); LOAD_A_512(1, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);

				MATMUL_512(0, 0); MATMUL_512(1, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3);
			}
			STORE_512(0, 0); STORE_512(1, 0);
			STORE_512(0, 1); STORE_512(1, 1);
			STORE_512(0, 2); STORE_512(1, 2);
			STORE_512(0, 3); STORE_512(1, 3);
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
	for (; i < m16; i += 16) {
		for (j = 0; j < n8; j += 8) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);
			DECLARE_RESULT_512(0, 4);
			DECLARE_RESULT_512(0, 5);
			DECLARE_RESULT_512(0, 6);
			DECLARE_RESULT_512(0, 7);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);
				BROADCAST_LOAD_B_512(x, 4); BROADCAST_LOAD_B_512(x, 5);
				BROADCAST_LOAD_B_512(x, 6); BROADCAST_LOAD_B_512(x, 7);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
				MATMUL_512(0, 4);
				MATMUL_512(0, 5);
				MATMUL_512(0, 6);
				MATMUL_512(0, 7);
			}
			STORE_512(0, 0);
			STORE_512(0, 1);
			STORE_512(0, 2);
			STORE_512(0, 3);
			STORE_512(0, 4);
			STORE_512(0, 5);
			STORE_512(0, 6);
			STORE_512(0, 7);
		}
		for (; j < n4; j += 4) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);
			for (k = 0; k < K; k++) {
				LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
			}
			STORE_512(0, 0);
			STORE_512(0, 1);
			STORE_512(0, 2);
			STORE_512(0, 3);
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
	if (mm >= 12) {
		register __mmask16 mask = (1UL << mm) - 1;
		for (j = 0; j < n8; j += 8) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);
			DECLARE_RESULT_512(0, 4);
			DECLARE_RESULT_512(0, 5);
			DECLARE_RESULT_512(0, 6);
			DECLARE_RESULT_512(0, 7);
			for (k = 0; k < K; k++) {
				MASK_LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);
				BROADCAST_LOAD_B_512(x, 4); BROADCAST_LOAD_B_512(x, 5);
				BROADCAST_LOAD_B_512(x, 6); BROADCAST_LOAD_B_512(x, 7);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
				MATMUL_512(0, 4);
				MATMUL_512(0, 5);
				MATMUL_512(0, 6);
				MATMUL_512(0, 7);
			}
			MASK_STORE_512(0, 0);
			MASK_STORE_512(0, 1);
			MASK_STORE_512(0, 2);
			MASK_STORE_512(0, 3);
			MASK_STORE_512(0, 4);
			MASK_STORE_512(0, 5);
			MASK_STORE_512(0, 6);
			MASK_STORE_512(0, 7);
		}
		for (; j < n4; j += 4) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);
			for (k = 0; k < K; k++) {
				MASK_LOAD_A_512(0, x);
				BROADCAST_LOAD_B_512(x, 0); BROADCAST_LOAD_B_512(x, 1);
				BROADCAST_LOAD_B_512(x, 2); BROADCAST_LOAD_B_512(x, 3);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
			}
			MASK_STORE_512(0, 0);
			MASK_STORE_512(0, 1);
			MASK_STORE_512(0, 2);
			MASK_STORE_512(0, 3);
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
	} else if (mm > 0) {
		int index_n[16];
		for (int ii = 0; ii < 16; ii++) {
			index_n[ii] = ii * ldc;
		}
		__m512i vindex_n = _mm512_loadu_si512(index_n);
		for (; i < m4; i += 4) {
			for (j = 0; j < n64; j += 64) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
				DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2); DECLARE_RESULT_512(2, 2); DECLARE_RESULT_512(3, 2);
				DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3); DECLARE_RESULT_512(2, 3); DECLARE_RESULT_512(3, 3);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
					LOAD_B_512(x, 0);
					LOAD_B_512(x, 1);
					LOAD_B_512(x, 2);
					LOAD_B_512(x, 3);
					MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
					MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
					MATMUL_512(0, 2); MATMUL_512(1, 2); MATMUL_512(2, 2); MATMUL_512(3, 2);
					MATMUL_512(0, 3); MATMUL_512(1, 3); MATMUL_512(2, 3); MATMUL_512(3, 3);
				}
				SCATTER_STORE_512(0, 0); SCATTER_STORE_512(1, 0); SCATTER_STORE_512(2, 0); SCATTER_STORE_512(3, 0);
				SCATTER_STORE_512(0, 1); SCATTER_STORE_512(1, 1); SCATTER_STORE_512(2, 1); SCATTER_STORE_512(3, 1);
				SCATTER_STORE_512(0, 2); SCATTER_STORE_512(1, 2); SCATTER_STORE_512(2, 2); SCATTER_STORE_512(3, 2);
				SCATTER_STORE_512(0, 3); SCATTER_STORE_512(1, 3); SCATTER_STORE_512(2, 3); SCATTER_STORE_512(3, 3);
			}
			for (; j < n32; j += 32) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
					LOAD_B_512(x, 0);
					LOAD_B_512(x, 1);
					MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
					MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
				}
				SCATTER_STORE_512(0, 0); SCATTER_STORE_512(1, 0); SCATTER_STORE_512(2, 0); SCATTER_STORE_512(3, 0);
				SCATTER_STORE_512(0, 1); SCATTER_STORE_512(1, 1); SCATTER_STORE_512(2, 1); SCATTER_STORE_512(3, 1);
			}
			__mmask16 mask = 0xffff;
			for (; j < N; j += 16) {
				int remains = N - j;
				if (remains < 16) mask = (1UL << remains) - 1;
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
					MASK_LOAD_B_512(x, 0);
					MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				}
				MASK_SCATTER_STORE_512(0, 0); MASK_SCATTER_STORE_512(1, 0); MASK_SCATTER_STORE_512(2, 0); MASK_SCATTER_STORE_512(3, 0);
			}
		}
		for (; i < m2; i += 2) {
			for (j = 0; j < n64; j += 64) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
				DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2);
				DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x);
					LOAD_B_512(x, 0);
					LOAD_B_512(x, 1);
					LOAD_B_512(x, 2);
					LOAD_B_512(x, 3);
					MATMUL_512(0, 0); MATMUL_512(1, 0);
					MATMUL_512(0, 1); MATMUL_512(1, 1);
					MATMUL_512(0, 2); MATMUL_512(1, 2);
					MATMUL_512(0, 3); MATMUL_512(1, 3);
				}
				SCATTER_STORE_512(0, 0); SCATTER_STORE_512(1, 0);
				SCATTER_STORE_512(0, 1); SCATTER_STORE_512(1, 1);
				SCATTER_STORE_512(0, 2); SCATTER_STORE_512(1, 2);
				SCATTER_STORE_512(0, 3); SCATTER_STORE_512(1, 3);
			}
			for (; j < n32; j += 32) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x);
					LOAD_B_512(x, 0);
					LOAD_B_512(x, 1);
					MATMUL_512(0, 0); MATMUL_512(1, 0);
					MATMUL_512(0, 1); MATMUL_512(1, 1);
				}
				SCATTER_STORE_512(0, 0); SCATTER_STORE_512(1, 0);
				SCATTER_STORE_512(0, 1); SCATTER_STORE_512(1, 1);
			}
			__mmask16 mask = 0xffff;
			for (; j < N; j += 16) {
				int remains = N - j;
				if (remains < 16) mask = (1UL << remains) - 1;
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x);
					MASK_LOAD_B_512(x, 0);
					MATMUL_512(0, 0); MATMUL_512(1, 0);
				}
				MASK_SCATTER_STORE_512(0, 0); MASK_SCATTER_STORE_512(1, 0);
			}
		}
		for (; i < M; i += 1) {
			for (j = 0; j < n64; j += 64) {
				DECLARE_RESULT_512(0, 0);
				DECLARE_RESULT_512(0, 1);
				DECLARE_RESULT_512(0, 2);
				DECLARE_RESULT_512(0, 3);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x);
					LOAD_B_512(x, 0);
					LOAD_B_512(x, 1);
					LOAD_B_512(x, 2);
					LOAD_B_512(x, 3);
					MATMUL_512(0, 0);
					MATMUL_512(0, 1);
					MATMUL_512(0, 2);
					MATMUL_512(0, 3);
				}
				SCATTER_STORE_512(0, 0);
				SCATTER_STORE_512(0, 1);
				SCATTER_STORE_512(0, 2);
				SCATTER_STORE_512(0, 3);
			}
			for (; j < n32; j += 32) {
				DECLARE_RESULT_512(0, 0);
				DECLARE_RESULT_512(0, 1);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x);
					LOAD_B_512(x, 0);
					LOAD_B_512(x, 1);
					MATMUL_512(0, 0);
					MATMUL_512(0, 1);
				}
				SCATTER_STORE_512(0, 0);
				SCATTER_STORE_512(0, 1);
			}
			__mmask16 mask = 0xffff;
			for (; j < N; j += 16) {
				int remains = N - j;
				if (remains < 16) mask = (1UL << remains) - 1;
				DECLARE_RESULT_512(0, 0);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x);
					MASK_LOAD_B_512(x, 0);
					MATMUL_512(0, 0);
				}
				MASK_SCATTER_STORE_512(0, 0);
			}
		}
	}
	return 0;
}
