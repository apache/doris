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

#define DECLARE_RESULT_512(M, N) __m512 result##M##N = _mm512_setzero_ps()
#define BROADCAST_LOAD_A_512(M, N) __m512 Aval##M = _mm512_broadcastss_ps(_mm_load_ss(&A[k  + lda * (i+M)]))
#define LOAD_B_512(M,N)  __m512 Bval##N = _mm512_loadu_ps(&B[ldb * k + j + (N*16)])
#define MASK_LOAD_B_512(M, N) __m512 Bval##N = _mm512_maskz_loadu_ps(mask, &B[ldb * k + j + (N*16)])
#define MATMUL_512(M, N) result##M##N = _mm512_fmadd_ps(Aval##M, Bval##N, result##M##N)

#if defined(B0)
#define STORE_8xy(v, N, x, y) _mm256_storeu_ps(&C[(j + N*16 + x + y*8)*ldc + i], v)
#define STORE_4xy(v, N, x, y) _mm_mask_storeu_ps(&C[(j + N*16 + x + y*4)*ldc + i], mask8, v)
#define SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				_mm512_i32scatter_ps(&C[(j + N*16)*ldc + i + M], vindex_n, result##M##N, 4);
#define MASK_SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				    _mm512_mask_i32scatter_ps(&C[(j + N*16)*ldc + i + M], mask, vindex_n, result##M##N, 4);
#else
#define STORE_8xy(v, N, x, y) \
	asm("vfmadd231ps (%1), %2, %0": "+v"(v): "r"(&C[(j + N*16 + x + y*8)*ldc + i]), "v"(beta_256)); \
	_mm256_storeu_ps(&C[(j + N*16 + x + y*8)*ldc + i], v)
#define STORE_4xy(v, N, x, y) \
	asm("vfmadd231ps (%1), %2, %0": "+v"(v): "r"(&C[(j + N*16 + x + y*4)*ldc + i]), "v"(beta_128)); \
	_mm_mask_storeu_ps(&C[(j + N*16 + x + y*4)*ldc + i], mask8, v)
#define SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				__m512 tmp##M##N = _mm512_i32gather_ps(vindex_n, &C[(j + N*16)*ldc + i + M], 4); \
				result##M##N = _mm512_fmadd_ps(tmp##M##N, beta_512, result##M##N); \
				_mm512_i32scatter_ps(&C[(j + N*16)*ldc + i + M], vindex_n, result##M##N, 4);
#define MASK_SCATTER_STORE_512(M, N) result##M##N = _mm512_mul_ps(result##M##N, alpha_512); \
				__m512 tmp##M##N = _mm512_mask_i32gather_ps(_mm512_setzero_ps(), mask, vindex_n, &C[(j + N*16)*ldc + i + M], 4); \
				result##M##N = _mm512_fmadd_ps(tmp##M##N, beta_512, result##M##N); \
				_mm512_mask_i32scatter_ps(&C[(j + N*16)*ldc + i + M], mask, vindex_n, result##M##N, 4);
#endif

#define REORDER_8x16(r0, r1, r2, r3, r4, r5, r6, r7) \
	__m512 t0, t1, t2, t3, t4, t5, t6, t7, v; \
	t0 = _mm512_unpacklo_ps(r0, r1); \
	t1 = _mm512_unpackhi_ps(r0, r1); \
	t2 = _mm512_unpacklo_ps(r2, r3); \
	t3 = _mm512_unpackhi_ps(r2, r3); \
	t4 = _mm512_unpacklo_ps(r4, r5); \
	t5 = _mm512_unpackhi_ps(r4, r5); \
	t6 = _mm512_unpacklo_ps(r6, r7); \
	t7 = _mm512_unpackhi_ps(r6, r7); \
	v = _mm512_shuffle_ps(t0, t2, 0x4E);  \
	r0 = _mm512_mask_blend_ps(kc, t0, v); \
	r1 = _mm512_mask_blend_ps(k3, t2, v); \
	v = _mm512_shuffle_ps(t1, t3, 0x4E);  \
	r2 = _mm512_mask_blend_ps(kc, t1, v); \
	r3 = _mm512_mask_blend_ps(k3, t3, v); \
	v = _mm512_shuffle_ps(t4, t6, 0x4E);  \
	r4 = _mm512_mask_blend_ps(kc, t4, v); \
	r5 = _mm512_mask_blend_ps(k3, t6, v); \
	v = _mm512_shuffle_ps(t5, t7, 0x4E);  \
	r6 = _mm512_mask_blend_ps(kc, t5, v); \
	r7 = _mm512_mask_blend_ps(k3, t7, v); \
	t0 = _mm512_permutex2var_ps(r0, idx_lo, r4); \
	t1 = _mm512_permutex2var_ps(r1, idx_lo, r5); \
	t2 = _mm512_permutex2var_ps(r2, idx_lo, r6); \
	t3 = _mm512_permutex2var_ps(r3, idx_lo, r7); \
	t4 = _mm512_permutex2var_ps(r0, idx_hi, r4); \
	t5 = _mm512_permutex2var_ps(r1, idx_hi, r5); \
	t6 = _mm512_permutex2var_ps(r2, idx_hi, r6); \
	t7 = _mm512_permutex2var_ps(r3, idx_hi, r7); \
	t0 = _mm512_mul_ps(t0, alpha_512); \
	t1 = _mm512_mul_ps(t1, alpha_512); \
	t2 = _mm512_mul_ps(t2, alpha_512); \
	t3 = _mm512_mul_ps(t3, alpha_512); \
	t4 = _mm512_mul_ps(t4, alpha_512); \
	t5 = _mm512_mul_ps(t5, alpha_512); \
	t6 = _mm512_mul_ps(t6, alpha_512); \
	t7 = _mm512_mul_ps(t7, alpha_512);

#define SAVE_8(N, x, y) {\
	__m256 v8 = _mm512_extractf32x8_ps(t##x, y); \
	STORE_8xy(v8, N, x, y); \
}

#define REORDER_STORE_8x16(N) {\
	REORDER_8x16(result0##N, result1##N, result2##N, result3##N, result4##N, result5##N, result6##N, result7##N); \
	SAVE_8(N, 0, 0); SAVE_8(N, 1, 0); SAVE_8(N, 2, 0); SAVE_8(N, 3, 0); SAVE_8(N, 4, 0); SAVE_8(N, 5, 0); SAVE_8(N, 6, 0); SAVE_8(N, 7, 0); \
	SAVE_8(N, 0, 1); SAVE_8(N, 1, 1); SAVE_8(N, 2, 1); SAVE_8(N, 3, 1); SAVE_8(N, 4, 1); SAVE_8(N, 5, 1); SAVE_8(N, 6, 1); SAVE_8(N, 7, 1); \
}

#define MASK_SAVE_8() \
	switch (nn) { \
		case 16: SAVE_8(0, 7, 1); \
		case 15: SAVE_8(0, 6, 1); \
		case 14: SAVE_8(0, 5, 1); \
		case 13: SAVE_8(0, 4, 1); \
		case 12: SAVE_8(0, 3, 1); \
		case 11: SAVE_8(0, 2, 1); \
		case 10: SAVE_8(0, 1, 1); \
		case 9: SAVE_8(0, 0, 1); \
		case 8: SAVE_8(0, 7, 0); \
		case 7: SAVE_8(0, 6, 0); \
		case 6: SAVE_8(0, 5, 0); \
		case 5: SAVE_8(0, 4, 0); \
		case 4: SAVE_8(0, 3, 0); \
		case 3: SAVE_8(0, 2, 0); \
		case 2: SAVE_8(0, 1, 0); \
		case 1: SAVE_8(0, 0, 0); \
	}

#define MASK_REORDER_STORE_8x16(N) {\
	REORDER_8x16(result0##N, result1##N, result2##N, result3##N, result4##N, result5##N, result6##N, result7##N); \
	MASK_SAVE_8(); \
}

#define REORDER_4x16(r0, r1, r2, r3) \
	__m512 t0, t1, t2, t3, v; \
	t0 = _mm512_unpacklo_ps(r0, r1); \
	t1 = _mm512_unpackhi_ps(r0, r1); \
	t2 = _mm512_unpacklo_ps(r2, r3); \
	t3 = _mm512_unpackhi_ps(r2, r3); \
	v = _mm512_shuffle_ps(t0, t2, 0x4E);  \
	r0 = _mm512_mask_blend_ps(kc, t0, v); \
	r1 = _mm512_mask_blend_ps(k3, t2, v); \
	v = _mm512_shuffle_ps(t1, t3, 0x4E);  \
	r2 = _mm512_mask_blend_ps(kc, t1, v); \
	r3 = _mm512_mask_blend_ps(k3, t3, v); \
	t0 = _mm512_mul_ps(r0, alpha_512); \
	t1 = _mm512_mul_ps(r1, alpha_512); \
	t2 = _mm512_mul_ps(r2, alpha_512); \
	t3 = _mm512_mul_ps(r3, alpha_512);

#define SAVE_4(N, x, y) {\
	__m128 v4 = _mm512_extractf32x4_ps(t##x, y); \
	STORE_4xy(v4, N, x, y); \
}

#define REORDER_STORE_4x16(N) {\
	REORDER_4x16(result0##N, result1##N, result2##N, result3##N); \
	SAVE_4(N, 0, 0); SAVE_4(N, 1, 0); SAVE_4(N, 2, 0); SAVE_4(N, 3, 0); \
	SAVE_4(N, 0, 1); SAVE_4(N, 1, 1); SAVE_4(N, 2, 1); SAVE_4(N, 3, 1); \
	SAVE_4(N, 0, 2); SAVE_4(N, 1, 2); SAVE_4(N, 2, 2); SAVE_4(N, 3, 2); \
	SAVE_4(N, 0, 3); SAVE_4(N, 1, 3); SAVE_4(N, 2, 3); SAVE_4(N, 3, 3); \
}

#define MASK_SAVE_4() \
	switch (nn) { \
		case 16: SAVE_4(0, 3, 3); \
		case 15: SAVE_4(0, 2, 3); \
		case 14: SAVE_4(0, 1, 3); \
		case 13: SAVE_4(0, 0, 3); \
		case 12: SAVE_4(0, 3, 2); \
		case 11: SAVE_4(0, 2, 2); \
		case 10: SAVE_4(0, 1, 2); \
		case 9: SAVE_4(0, 0, 2); \
		case 8: SAVE_4(0, 3, 1); \
		case 7: SAVE_4(0, 2, 1); \
		case 6: SAVE_4(0, 1, 1); \
		case 5: SAVE_4(0, 0, 1); \
		case 4: SAVE_4(0, 3, 0); \
		case 3: SAVE_4(0, 2, 0); \
		case 2: SAVE_4(0, 1, 0); \
		case 1: SAVE_4(0, 0, 0); \
	}

#define MASK_REORDER_STORE_4x16(N) {\
	REORDER_4x16(result0##N, result1##N, result2##N, result3##N); \
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

	BLASLONG n64 = N & ~63;
	BLASLONG n32 = N & ~31;

	__m512 alpha_512 = _mm512_broadcastss_ps(_mm_load_ss(&alpha));
#if !defined(B0)
	__m256 beta_256 = _mm256_broadcastss_ps(_mm_load_ss(&beta));
	__m128 beta_128 = _mm_broadcastss_ps(_mm_load_ss(&beta));
#endif
	int permute_table[] = {
		0x0, 0x1, 0x2, 0x3, 0x10, 0x11, 0x12, 0x13, 0x8, 0x9, 0xa, 0xb, 0x18, 0x19, 0x1a, 0x1b,
		0x4, 0x5, 0x6, 0x7, 0x14, 0x15, 0x16, 0x17, 0xc, 0xd, 0xe, 0xf, 0x1c, 0x1d, 0x1e, 0x1f,
	};
	__m512i idx_lo = _mm512_loadu_si512(permute_table);
	__m512i idx_hi = _mm512_loadu_si512(permute_table + 16);
	__mmask16 kc = 0xcccc;
	__mmask16 k3 = 0x3333;
	__mmask8 mask8 = 0xff;  // force use AVX128 instead of SSE

	for (i = 0; i < m8; i += 8) {
		for (j = 0; j < n32; j += 32) {
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
			REORDER_STORE_8x16(0);
			REORDER_STORE_8x16(1);
		}
		__mmask16 mask = 0xffff;
		int nn = 16;
		for (; j < N; j += 16) {
			if (N - j < 16) {
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
			MASK_REORDER_STORE_8x16(0);
		}
	}
	for (; i < m4; i += 4) {
		for (j = 0; j < n64; j += 64) {
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
			REORDER_STORE_4x16(0);
			REORDER_STORE_4x16(1);
			REORDER_STORE_4x16(2);
			REORDER_STORE_4x16(3);
		}
		for (; j < n32; j += 32) {
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1); DECLARE_RESULT_512(1, 1); DECLARE_RESULT_512(2, 1); DECLARE_RESULT_512(3, 1);
			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
				LOAD_B_512(x, 0); LOAD_B_512(x, 1);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
				MATMUL_512(0, 1); MATMUL_512(1, 1); MATMUL_512(2, 1); MATMUL_512(3, 1);
			}
			REORDER_STORE_4x16(0);
			REORDER_STORE_4x16(1);
		}
		__mmask16 mask = 0xffff;
		int nn = 16;
		for (; j < N; j += 16) {
			if (N - j < 16) {
				nn = N - j;
				mask = (1UL << nn) - 1;
			}
			DECLARE_RESULT_512(0, 0); DECLARE_RESULT_512(1, 0); DECLARE_RESULT_512(2, 0); DECLARE_RESULT_512(3, 0);
			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(0, x); BROADCAST_LOAD_A_512(1, x); BROADCAST_LOAD_A_512(2, x); BROADCAST_LOAD_A_512(3, x);
				MASK_LOAD_B_512(x, 0);
				MATMUL_512(0, 0); MATMUL_512(1, 0); MATMUL_512(2, 0); MATMUL_512(3, 0);
			}
			MASK_REORDER_STORE_4x16(0);
		}
	}
	if (i < M) {
		int index_n[16];
		for (int ii = 0; ii < 16; ii++) {
			index_n[ii] = ii * ldc;
		}
		__m512i vindex_n = _mm512_loadu_si512(index_n);
#if !defined(B0)
		__m512 beta_512 = _mm512_broadcastss_ps(_mm_load_ss(&beta));
#endif
		for (; i < m2; i += 2) {
			for (j = 0; j < n64; j += 64) {
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
			for (; j < n32; j += 32) {
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
			__mmask16 mask = 0xffff;
			int nn = 16;
			for (; j < N; j += 16) {
				if (N - j < 16) {
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
			for (j = 0; j < n64; j += 64) {
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
			for (; j < n32; j += 32) {
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
			__mmask16 mask = 0xffff;
			int nn = 16;
			for (; j < N; j += 16) {
				if (N - j < 16) {
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
