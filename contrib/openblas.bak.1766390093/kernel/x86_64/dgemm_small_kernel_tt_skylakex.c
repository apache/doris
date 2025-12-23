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

#define DECLARE_RESULT_512(M, N) __m512d result##M##N = _mm512_setzero_pd()
#define BROADCAST_LOAD_A_512(M, N) __m512d Aval##M = _mm512_broadcastsd_pd(_mm_load_sd(&A[k  + lda * (i+M)]))
#define LOAD_B_512(M,N)  __m512d Bval##N = _mm512_loadu_pd(&B[ldb * k + j + (N*8)])
#define MASK_LOAD_B_512(M, N) __m512d Bval##N = _mm512_maskz_loadu_pd(mask, &B[ldb * k + j + (N*8)])
#define MATMUL_512(M, N) result##M##N = _mm512_fmadd_pd(Aval##M, Bval##N, result##M##N)

#if defined(B0)
#define STORE_8xy(v, N, x, y) _mm512_storeu_pd(&C[(j + N*8 + x + y*8)*ldc + i], v)
#define STORE_4xy(v, N, x, y) _mm256_storeu_pd(&C[(j + N*8 + x + y*4)*ldc + i], v)
#define SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
				_mm512_i64scatter_pd(&C[(j + N*8)*ldc + i + M], vindex_n, result##M##N, 8);
#define MASK_SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
				    _mm512_mask_i64scatter_pd(&C[(j + N*8)*ldc + i + M], mask, vindex_n, result##M##N, 8);
#else
#define STORE_8xy(v, N, x, y) \
	asm("vfmadd231pd (%1), %2, %0": "+v"(v): "r"(&C[(j + N*8 + x + y*8)*ldc + i]), "v"(beta_512)); \
	_mm512_storeu_pd(&C[(j + N*8 + x + y*8)*ldc + i], v)
#define STORE_4xy(v, N, x, y) \
	asm("vfmadd231pd (%1), %2, %0": "+v"(v): "r"(&C[(j + N*8 + x + y*4)*ldc + i]), "v"(beta_256)); \
	_mm256_storeu_pd(&C[(j + N*8 + x + y*4)*ldc + i], v)
#define SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
				__m512d tmp##M##N = _mm512_i64gather_pd(vindex_n, &C[(j + N*8)*ldc + i + M], 8); \
				result##M##N = _mm512_fmadd_pd(tmp##M##N, beta_512, result##M##N); \
				_mm512_i64scatter_pd(&C[(j + N*8)*ldc + i + M], vindex_n, result##M##N, 8);
#define MASK_SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_pd(result##M##N, alpha_512); \
				__m512d tmp##M##N = _mm512_mask_i64gather_pd(_mm512_setzero_pd(), mask, vindex_n, &C[(j + N*8)*ldc + i + M], 8); \
				result##M##N = _mm512_fmadd_pd(tmp##M##N, beta_512, result##M##N); \
				_mm512_mask_i64scatter_pd(&C[(j + N*8)*ldc + i + M], mask, vindex_n, result##M##N, 8);
#endif

#define REORDER_8x8(r0, r1, r2, r3, r4, r5, r6, r7) \
	__m512d t0, t1, t2, t3, t4, t5, t6, t7; \
	t0 = _mm512_unpacklo_pd(r0, r1); \
	t1 = _mm512_unpackhi_pd(r0, r1); \
	t2 = _mm512_unpacklo_pd(r2, r3); \
	t3 = _mm512_unpackhi_pd(r2, r3); \
	t4 = _mm512_unpacklo_pd(r4, r5); \
	t5 = _mm512_unpackhi_pd(r4, r5); \
	t6 = _mm512_unpacklo_pd(r6, r7); \
	t7 = _mm512_unpackhi_pd(r6, r7); \
	r0 = _mm512_shuffle_f64x2(t0, t2, 0x88); \
	r1 = _mm512_shuffle_f64x2(t1, t3, 0x88); \
	r2 = _mm512_shuffle_f64x2(t0, t2, 0xdd); \
	r3 = _mm512_shuffle_f64x2(t1, t3, 0xdd); \
	r4 = _mm512_shuffle_f64x2(t4, t6, 0x88); \
	r5 = _mm512_shuffle_f64x2(t5, t7, 0x88); \
	r6 = _mm512_shuffle_f64x2(t4, t6, 0xdd); \
	r7 = _mm512_shuffle_f64x2(t5, t7, 0xdd); \
	t0 = _mm512_permutex2var_pd(r0, idx_lo, r4); \
	t1 = _mm512_permutex2var_pd(r1, idx_lo, r5); \
	t2 = _mm512_permutex2var_pd(r2, idx_lo, r6); \
	t3 = _mm512_permutex2var_pd(r3, idx_lo, r7); \
	t4 = _mm512_permutex2var_pd(r0, idx_hi, r4); \
	t5 = _mm512_permutex2var_pd(r1, idx_hi, r5); \
	t6 = _mm512_permutex2var_pd(r2, idx_hi, r6); \
	t7 = _mm512_permutex2var_pd(r3, idx_hi, r7); \
	t0 = _mm512_mul_pd(t0, alpha_512); \
	t1 = _mm512_mul_pd(t1, alpha_512); \
	t2 = _mm512_mul_pd(t2, alpha_512); \
	t3 = _mm512_mul_pd(t3, alpha_512); \
	t4 = _mm512_mul_pd(t4, alpha_512); \
	t5 = _mm512_mul_pd(t5, alpha_512); \
	t6 = _mm512_mul_pd(t6, alpha_512); \
	t7 = _mm512_mul_pd(t7, alpha_512);

#define SAVE_8(N, x) {\
	STORE_8xy(t##x, N, x, 0); \
}

#define REORDER_STORE_8x8(N) {\
	REORDER_8x8(result0##N, result1##N, result2##N, result3##N, result4##N, result5##N, result6##N, result7##N); \
	SAVE_8(N, 0); SAVE_8(N, 1); SAVE_8(N, 2); SAVE_8(N, 3); SAVE_8(N, 4); SAVE_8(N, 5); SAVE_8(N, 6); SAVE_8(N, 7); \
}

#define MASK_SAVE_8() \
	switch (nn) { \
		case 8: SAVE_8(0, 7); \
		case 7: SAVE_8(0, 6); \
		case 6: SAVE_8(0, 5); \
		case 5: SAVE_8(0, 4); \
		case 4: SAVE_8(0, 3); \
		case 3: SAVE_8(0, 2); \
		case 2: SAVE_8(0, 1); \
		case 1: SAVE_8(0, 0); \
	}

#define MASK_REORDER_STORE_8x8(N) {\
	REORDER_8x8(result0##N, result1##N, result2##N, result3##N, result4##N, result5##N, result6##N, result7##N); \
	MASK_SAVE_8(); \
}

#define REORDER_4x8(r0, r1, r2, r3) \
	__m512d t0, t1, t2, t3; \
	t0 = _mm512_unpacklo_pd(r0, r1); \
	t1 = _mm512_unpackhi_pd(r0, r1); \
	t2 = _mm512_unpacklo_pd(r2, r3); \
	t3 = _mm512_unpackhi_pd(r2, r3); \
	r0 = _mm512_permutex2var_pd(t0, idx_lo, t2); \
	r1 = _mm512_permutex2var_pd(t1, idx_lo, t3); \
	r2 = _mm512_permutex2var_pd(t0, idx_hi, t2); \
	r3 = _mm512_permutex2var_pd(t1, idx_hi, t3); \
	t0 = _mm512_mul_pd(r0, alpha_512); \
	t1 = _mm512_mul_pd(r1, alpha_512); \
	t2 = _mm512_mul_pd(r2, alpha_512); \
	t3 = _mm512_mul_pd(r3, alpha_512);

#define SAVE_4(N, x, y) {\
	__m256d v4 = _mm512_extractf64x4_pd(t##x, y); \
	STORE_4xy(v4, N, x, y); \
}

#define REORDER_STORE_4x8(N) {\
	REORDER_4x8(result0##N, result1##N, result2##N, result3##N); \
	SAVE_4(N, 0, 0); SAVE_4(N, 1, 0); SAVE_4(N, 2, 0); SAVE_4(N, 3, 0); \
	SAVE_4(N, 0, 1); SAVE_4(N, 1, 1); SAVE_4(N, 2, 1); SAVE_4(N, 3, 1); \
}

#define MASK_SAVE_4() \
	switch (nn) { \
		case 8: SAVE_4(0, 3, 1); \
		case 7: SAVE_4(0, 2, 1); \
		case 6: SAVE_4(0, 1, 1); \
		case 5: SAVE_4(0, 0, 1); \
		case 4: SAVE_4(0, 3, 0); \
		case 3: SAVE_4(0, 2, 0); \
		case 2: SAVE_4(0, 1, 0); \
		case 1: SAVE_4(0, 0, 0); \
	}

#define MASK_REORDER_STORE_4x8(N) {\
	REORDER_4x8(result0##N, result1##N, result2##N, result3##N); \
	MASK_SAVE_4(); \
}


#if defined(B0)
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha, FLOAT * B, BLASLONG ldb, FLOAT beta, FLOAT * C, BLASLONG ldc)
#endif
{
	// column major
	BLASLONG i, j, k;

	BLASLONG m8 = M & ~7;
	BLASLONG m4 = M & ~3;
	BLASLONG m2 = M & ~1;

	BLASLONG n32 = N & ~31;
	BLASLONG n16 = N & ~15;

	__m512d alpha_512 = _mm512_broadcastsd_pd(_mm_load_sd(&alpha));
#if !defined(B0)
	__m512d beta_512 = _mm512_broadcastsd_pd(_mm_load_sd(&beta));
	__m256d beta_256 = _mm256_broadcastsd_pd(_mm_load_sd(&beta));
#endif
	long long permute_table[] = {
		0, 1, 4, 5, 0|8, 1|8, 4|8, 5|8,
		2, 3, 6, 7, 2|8, 3|8, 6|8, 7|8,
	};
	__m512i idx_lo = _mm512_loadu_si512(permute_table);
	__m512i idx_hi = _mm512_loadu_si512(permute_table + 8);

	for (i = 0; i < m8; i += 8) {
		for (j = 0; j < n16; j += 16) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(4, 0); DECLARE_RESULT_512(5, 0); DECLARE_RESULT_512(6, 0); DECLARE_RESULT_512(7, 0);

			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			DECLARE_RESULT_512(4, 1); DECLARE_RESULT_512(5, 1); DECLARE_RESULT_512(6, 1); DECLARE_RESULT_512(7, 1);
			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
				BROADCAST_LOAD_A_512(4, x); BROADCAST_LOAD_A_512(5, x); BROADCAST_LOAD_A_512(6, x); BROADCAST_LOAD_A_512(7, x);
				LOAD_B_512(x, 0); LOAD_B_512(x, 1);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(4, 0); MATMUL_512(5, 0); MATMUL_512(6, 0); MATMUL_512(7, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
				MATMUL_512(4, 1); MATMUL_512(5, 1); MATMUL_512(6, 1); MATMUL_512(7, 1);
			}
			REORDER_STORE_8x8(0);
			REORDER_STORE_8x8(1);
		}
		__mmask8 mask = 0xff;
		int nn = 8;
		for (; j < N; j += 8) {
			if (N - j < 8) {
				nn = N - j;
				mask = (1UL << nn) - 1;
			}
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(4, 0); DECLARE_RESULT_512(5, 0); DECLARE_RESULT_512(6, 0); DECLARE_RESULT_512(7, 0);
			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
				BROADCAST_LOAD_A_512(4, x); BROADCAST_LOAD_A_512(5, x); BROADCAST_LOAD_A_512(6, x); BROADCAST_LOAD_A_512(7, x);
				MASK_LOAD_B_512(x, 0);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(4, 0); MATMUL_512(5, 0); MATMUL_512(6, 0); MATMUL_512(7, 0);
			}
			MASK_REORDER_STORE_8x8(0);
		}
	}
	for (; i < m4; i += 4) {
		long long permute_table2[] = {
			0, 1, 0|8, 1|8, 4, 5, 4|8, 5|8,
			2, 3, 2|8, 3|8, 6, 7, 6|8, 7|8,
		};
		idx_lo = _mm512_loadu_si512(permute_table2);
		idx_hi = _mm512_loadu_si512(permute_table2 + 8);

		for (j = 0; j < n32; j += 32) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2); DECLARE_RESULT_512(2, 2); DECLARE_RESULT_512(3, 2);
			DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3); DECLARE_RESULT_512(2, 3); DECLARE_RESULT_512(3, 3);
			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
				LOAD_B_512(x, 0); LOAD_B_512(x, 1); LOAD_B_512(x, 2); LOAD_B_512(x, 3);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
				MATMUL_512(0, 2); MATMUL_512(1, 2); MATMUL_512(2, 2); MATMUL_512(3, 2);
				MATMUL_512(0, 3); MATMUL_512(1, 3); MATMUL_512(2, 3); MATMUL_512(3, 3);
			}
			REORDER_STORE_4x8(0);
			REORDER_STORE_4x8(1);
			REORDER_STORE_4x8(2);
			REORDER_STORE_4x8(3);
		}
		for (; j < n16; j += 16) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
				LOAD_B_512(x, 0); LOAD_B_512(x, 1);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
			}
			REORDER_STORE_4x8(0);
			REORDER_STORE_4x8(1);
		}
		__mmask8 mask = 0xff;
		int nn = 8;
		for (; j < N; j += 8) {
			if (N - j < 8) {
				nn = N - j;
				mask = (1UL << nn) - 1;
			}
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
				MASK_LOAD_B_512(x, 0);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
			}
			MASK_REORDER_STORE_4x8(0);
		}
	}
	if (i < M) {
		long long index_n[8];
		for (int ii = 0; ii < 8; ii++) {
			index_n[ii] = ii * ldc;
		}
		__m512i vindex_n = _mm512_loadu_si512(index_n);
#if !defined(B0)
		__m512d beta_512 = _mm512_broadcastsd_pd(_mm_load_sd(&beta));
#endif
		for (; i < m2; i += 2) {
			for (j = 0; j < n32; j += 32) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
				DECLARE_RESULT_512(0, 2); DECLARE_RESULT_512(1, 2);
				DECLARE_RESULT_512(0, 3); DECLARE_RESULT_512(1, 3);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x);
					LOAD_B_512(x, 0); LOAD_B_512(x, 1); LOAD_B_512(x, 2); LOAD_B_512(x, 3);
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
			for (; j < n16; j += 16) {
				DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0);
				DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x);
					LOAD_B_512(x, 0); LOAD_B_512(x, 1);
					MATMUL_512(0, 0); MATMUL_512(1, 0);
					MATMUL_512(0, 1); MATMUL_512(1, 1);
				}
				SCATTER_STORE_512(0, 0); SCATTER_STORE_512(1, 0);
				SCATTER_STORE_512(0, 1); SCATTER_STORE_512(1, 1);
			}
			__mmask8 mask = 0xff;
			int nn = 8;
			for (; j < N; j += 8) {
				if (N - j < 8) {
					nn = N - j;
					mask = (1UL << nn) - 1;
				}
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
			for (j = 0; j < n32; j += 32) {
				DECLARE_RESULT_512(0, 0);
				DECLARE_RESULT_512(0, 1);
				DECLARE_RESULT_512(0, 2);
				DECLARE_RESULT_512(0, 3);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x);
					LOAD_B_512(x, 0); LOAD_B_512(x, 1); LOAD_B_512(x, 2); LOAD_B_512(x, 3);
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
			for (; j < n16; j += 16) {
				DECLARE_RESULT_512(0, 0);
				DECLARE_RESULT_512(0, 1);
				for (k = 0; k < K; k++) {
					BROADCAST_LOAD_A_512(0, x);
					LOAD_B_512(x, 0); LOAD_B_512(x, 1);
					MATMUL_512(0, 0);
					MATMUL_512(0, 1);
				}
				SCATTER_STORE_512(0, 0);
				SCATTER_STORE_512(0, 1);
			}
			__mmask8 mask = 0xff;
			int nn = 8;
			for (; j < N; j += 8) {
				if (N - j < 8) {
					nn = N - j;
					mask = (1UL << nn) - 1;
				}
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
