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

#define VMOVLDUP(addr, zmm) asm("vmovsldup (%1), %0": "=v"(zmm): "r"(addr))
#define VMOVHDUP(addr, zmm) asm("vmovshdup (%1), %0": "=v"(zmm): "r"(addr))
#define BROADCAST64(base, step, n, offset, zmm) \
	if (n == 0) asm("vbroadcastsd %c2(%1), %0": "=v"(zmm): "r"(base), "n"(offset*2)); \
	else asm("vbroadcastsd %c4(%1, %2, %c3), %0": "=v"(zmm): "r"(base), "r"(step), "n"(n*2), "n"(offset*2))

#define DECLARE_A_PAIR(A) \
	__m512i A_lo_##A; __m512i A_hi_##A;

#define LOAD_A_PAIR(A) \
	VMOVLDUP(ptr_a##A, A_lo_##A); \
	VMOVHDUP(ptr_a##A, A_hi_##A);

#define MASK_LOAD_A_PAIR(A) { \
	__m512 tmp = _mm512_maskz_loadu_ps(mmask, ptr_a##A); \
	A_lo_##A = (__m512i) _mm512_moveldup_ps(tmp); \
	A_hi_##A = (__m512i) _mm512_movehdup_ps(tmp); \
}

#define LOAD_A_PAIR_TAIL(A) { \
	__m256i ymm = _mm256_loadu_si256((void *)ptr_a##A); \
	__m512 zmm = (__m512) _mm512_cvtepu16_epi32(ymm); \
	A_lo_##A = (__m512i) _mm512_moveldup_ps(zmm); \
	A_hi_##A = (__m512i) _mm512_movehdup_ps(zmm); \
}

#define MASK_LOAD_A_PAIR_TAIL(A) { \
	__m256i ymm = _mm256_maskz_loadu_epi16(mmask, ptr_a##A); \
	__m512 zmm = (__m512) _mm512_cvtepu16_epi32(ymm); \
	A_lo_##A = (__m512i) _mm512_moveldup_ps(zmm); \
	A_hi_##A = (__m512i) _mm512_movehdup_ps(zmm); \
}

#define DECLARE_B_PAIR() \
	__m512i B_lo; __m512i B_hi;

#define PREFETCH_B_STEP 32
#define PREFETCH_B(Bx, By) \
	if (By == 0) asm("prefetcht0 %c1(%0)": : "r"(ptr_b##Bx), "n"(PREFETCH_B_STEP * 2)); \
	else asm("prefetcht0 %c3(%0, %1, %c2)": : "r"(ptr_b##Bx), "r"(n_blksize), "n"(By*2), "n"(PREFETCH_B_STEP * 2))

#define BROADCAST_B_PAIR(Bx, By) \
	BROADCAST64(ptr_b##Bx, n_blksize, By, 0, B_lo); \
	BROADCAST64(ptr_b##Bx, n_blksize, By, 4, B_hi);

#define MASK_BROADCAST_B_PAIR(Bx, x) {\
	__m128 xmm = _mm_maskz_loadu_ps(nmask, ptr_b##Bx); \
	B_lo = (__m512i) _mm512_broadcastsd_pd((__m128d) xmm); \
	B_hi = (__m512i) _mm512_broadcastsd_pd(_mm_permute_pd((__m128d) xmm, 0x1)); \
}

#define BROADCAST_B_PAIR_TAIL(Bx, By) {\
	__m128i xmm = (__m128i) _mm_load_sd((double *)(ptr_b##Bx + n_blksize * By)); \
	xmm = _mm_cvtepu16_epi32(xmm); \
	B_lo = _mm512_broadcast_i32x2(xmm); \
	B_hi = _mm512_broadcast_i32x2((__m128i) _mm_permute_pd((__m128d) xmm, 0x1)); \
}

#define MASK_BROADCAST_B_PAIR_TAIL(Bx, By) {\
	__m128i xmm = _mm_maskz_loadu_epi16(nmask, ptr_b##Bx + n_blksize * By); \
	xmm = _mm_cvtepu16_epi32(xmm); \
	B_lo = _mm512_broadcast_i32x2(xmm); \
	B_hi = _mm512_broadcast_i32x2((__m128i) _mm_permute_pd((__m128d) xmm, 0x1)); \
}

#define DECLARE_RESULT_4X(A, Bx, By) \
	__m512 result_00_##A##Bx##By = _mm512_setzero_ps(); \
	__m512 result_01_##A##Bx##By = _mm512_setzero_ps(); \
	__m512 result_10_##A##Bx##By = _mm512_setzero_ps(); \
	__m512 result_11_##A##Bx##By = _mm512_setzero_ps();

#define FMA(a, b, r) r = _mm512_dpbf16_ps(r, (__m512bh)a, (__m512bh)b)

#define MATMUL_4X(A, Bx, By) \
	FMA(A_lo_##A, B_lo, result_00_##A##Bx##By); \
	FMA(A_hi_##A, B_lo, result_01_##A##Bx##By); \
	FMA(A_lo_##A, B_hi, result_10_##A##Bx##By); \
	FMA(A_hi_##A, B_hi, result_11_##A##Bx##By);

#define _STORE_C_2nx16(addr, val0, val1) \
	asm("vfmadd213ps (%1), %2, %0": "+v"(val0) : "r"(addr), "v"(alpha_512)); \
	asm("vfmadd213ps (%1, %3, 4), %2, %0": "+v"(val1) : "r"(addr), "v"(alpha_512), "r"(ldc)); \
	asm("vmovups %0, (%1)": : "v"(val0), "r"(addr)); \
	asm("vmovups %0, (%1, %2, 4)": : "v"(val1), "r"(addr), "r"(ldc))

#define _MASK_STORE_C_2nx16(addr, val0, val1) \
	asm("vfmadd213ps (%1), %2, %0 %{%3%} ": "+v"(val0) : "r"(addr), "v"(alpha_512), "Yk"(mmask)); \
	asm("vfmadd213ps (%1, %3, 4), %2, %0 %{%4%}": "+v"(val1) : "r"(addr), "v"(alpha_512), "r"(ldc), "Yk"(mmask)); \
	asm("vmovups %0, (%1) %{%2%}": : "v"(val0), "r"(addr), "Yk"(mmask)); \
	asm("vmovups %0, (%1, %2, 4) %{%3%}": : "v"(val1), "r"(addr), "r"(ldc), "Yk"(mmask))

#define _REORDER_C_2X(result_0, result_1) { \
	__m512 tmp0, tmp1; \
	tmp0 = _mm512_unpacklo_ps(result_0, result_1); \
	tmp1 = _mm512_unpackhi_ps(result_0, result_1); \
	result_0 = (__m512) _mm512_unpacklo_pd((__m512d) tmp0, (__m512d) tmp1); \
	result_1 = (__m512) _mm512_unpackhi_pd((__m512d) tmp0, (__m512d) tmp1); \
}

#define _STORE_2X(ptr_c, result_0, result_1) {\
	_REORDER_C_2X(result_0, result_1) \
	_STORE_C_2nx16(ptr_c, result_0, result_1); \
	ptr_c += ldc * 2; \
}

#define _MASK_STORE_2X(ptr_c, result_0, result_1) {\
	_REORDER_C_2X(result_0, result_1) \
	_MASK_STORE_C_2nx16(ptr_c, result_0, result_1); \
	ptr_c += ldc * 2; \
}

#define STORE_4X(A, Bx, By) { \
	_STORE_2X(ptr_c##A, result_00_##A##Bx##By, result_01_##A##Bx##By); \
	_STORE_2X(ptr_c##A, result_10_##A##Bx##By, result_11_##A##Bx##By); \
}

#define MASK_STORE_4X(A, Bx, By) { \
	_MASK_STORE_2X(ptr_c##A, result_00_##A##Bx##By, result_01_##A##Bx##By); \
	_MASK_STORE_2X(ptr_c##A, result_10_##A##Bx##By, result_11_##A##Bx##By); \
}

#define _STORE_C_16(addr, val0) \
	asm("vfmadd213ps (%1), %2, %0": "+v"(val0) : "r"(addr), "v"(alpha_512)); \
	asm("vmovups %0, (%1)": : "v"(val0), "r"(addr));

#define _MASK_STORE_C_16(addr, val0) \
	asm("vfmadd213ps (%1), %2, %0 %{%3%} ": "+v"(val0) : "r"(addr), "v"(alpha_512), "Yk"(mmask)); \
	asm("vmovups %0, (%1) %{%2%}": : "v"(val0), "r"(addr), "Yk"(mmask));

#define N_STORE_4X(A, Bx, By) { \
	_REORDER_C_2X(result_00_##A##Bx##By, result_01_##A##Bx##By); \
	_REORDER_C_2X(result_10_##A##Bx##By, result_11_##A##Bx##By); \
	switch(n_count) { \
		case 3: _STORE_C_16(ptr_c + ldc * 2, result_10_##A##Bx##By); \
		case 2: _STORE_C_16(ptr_c + ldc * 1, result_01_##A##Bx##By); \
		case 1: _STORE_C_16(ptr_c + ldc * 0, result_00_##A##Bx##By); \
	} \
	ptr_c##A += ldc * n_count; \
}

#define N_MASK_STORE_4X(A, Bx, By) { \
	_REORDER_C_2X(result_00_##A##Bx##By, result_01_##A##Bx##By); \
	_REORDER_C_2X(result_10_##A##Bx##By, result_11_##A##Bx##By); \
	switch(n_count) { \
		case 3: _MASK_STORE_C_16(ptr_c + ldc * 2, result_10_##A##Bx##By); \
		case 2: _MASK_STORE_C_16(ptr_c + ldc * 1, result_01_##A##Bx##By); \
		case 1: _MASK_STORE_C_16(ptr_c + ldc * 0, result_00_##A##Bx##By); \
	} \
	ptr_c##A += ldc * n_count; \
}


int CNAME (BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, IFLOAT * A, IFLOAT * B, FLOAT * C, BLASLONG ldc)
{
	IFLOAT *ptr_a = A, *ptr_b = B;
	IFLOAT *ptr_b0, *ptr_b1;
	IFLOAT *ptr_a0, *ptr_a1;
	FLOAT *ptr_c = C;
	FLOAT *ptr_c0, *ptr_c1;
	BLASLONG n_count = n;
	BLASLONG m_count, k_count;
	BLASLONG n_blksize = 4 * k;
	BLASLONG cn_offset = 0;
	__m512 alpha_512 = _mm512_broadcastss_ps(_mm_load_ss(&alpha));

	for (; n_count > 23; n_count -= 24) {
		IFLOAT *ptr_b00 = ptr_b;
		IFLOAT *ptr_b10 = ptr_b + n_blksize * 3;
		ptr_a0 = ptr_a;
		ptr_c = C + cn_offset * ldc;
		m_count = m;
		for (; m_count > 15; m_count -= 16) {
			ptr_b0 = ptr_b00;
			ptr_b1 = ptr_b10;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0); DECLARE_RESULT_4X(0, 0, 1); DECLARE_RESULT_4X(0, 0, 2);
			DECLARE_RESULT_4X(0, 1, 0); DECLARE_RESULT_4X(0, 1, 1); DECLARE_RESULT_4X(0, 1, 2);
			k_count = k;
			for (; k_count > 3; k_count -=4) {
				LOAD_A_PAIR(0);
				_mm_prefetch(ptr_a0 + 128, _MM_HINT_T0);
				ptr_a0 += 16 * 2;
				BROADCAST_B_PAIR(0, 0); PREFETCH_B(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR(0, 1); PREFETCH_B(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR(0, 2); PREFETCH_B(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4 * 2;
				BROADCAST_B_PAIR(1, 0); PREFETCH_B(1, 0); MATMUL_4X(0, 1, 0);
				BROADCAST_B_PAIR(1, 1); PREFETCH_B(1, 1); MATMUL_4X(0, 1, 1);
				BROADCAST_B_PAIR(1, 2); PREFETCH_B(1, 2); MATMUL_4X(0, 1, 2);
				ptr_b1 += 4 * 2;

				LOAD_A_PAIR(0);
				_mm_prefetch(ptr_a0 + 128, _MM_HINT_T0);
				ptr_a0 += 16 * 2;
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4 * 2;
				BROADCAST_B_PAIR(1, 0); MATMUL_4X(0, 1, 0);
				BROADCAST_B_PAIR(1, 1); MATMUL_4X(0, 1, 1);
				BROADCAST_B_PAIR(1, 2); MATMUL_4X(0, 1, 2);
				ptr_b1 += 4 * 2;
			}
			for (; k_count > 1; k_count -=2) {
				LOAD_A_PAIR(0);
				ptr_a0 += 16 * 2;
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4 * 2;
				BROADCAST_B_PAIR(1, 0); MATMUL_4X(0, 1, 0);
				BROADCAST_B_PAIR(1, 1); MATMUL_4X(0, 1, 1);
				BROADCAST_B_PAIR(1, 2); MATMUL_4X(0, 1, 2);
				ptr_b1 += 4 * 2;
			}
			if (k_count > 0) {
				LOAD_A_PAIR_TAIL(0);
				ptr_a0 += 16;
				BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR_TAIL(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR_TAIL(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4;
				BROADCAST_B_PAIR_TAIL(1, 0); MATMUL_4X(0, 1, 0);
				BROADCAST_B_PAIR_TAIL(1, 1); MATMUL_4X(0, 1, 1);
				BROADCAST_B_PAIR_TAIL(1, 2); MATMUL_4X(0, 1, 2);
				ptr_b1 += 4;
			}
			ptr_c0 = ptr_c;
			STORE_4X(0, 0, 0); STORE_4X(0, 0, 1); STORE_4X(0, 0, 2);
			STORE_4X(0, 1, 0); STORE_4X(0, 1, 1); STORE_4X(0, 1, 2);
			ptr_c += 16;
		}
		if (m_count > 0) {
			__mmask16 mmask = (1UL << m_count) - 1;
			ptr_b0 = ptr_b00;
			ptr_b1 = ptr_b10;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0); DECLARE_RESULT_4X(0, 0, 1); DECLARE_RESULT_4X(0, 0, 2);
			DECLARE_RESULT_4X(0, 1, 0); DECLARE_RESULT_4X(0, 1, 1); DECLARE_RESULT_4X(0, 1, 2);
			for (k_count = k; k_count > 1; k_count -=2) {
				MASK_LOAD_A_PAIR(0);
				ptr_a0 += m_count * 2;
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4 * 2;
				BROADCAST_B_PAIR(1, 0); MATMUL_4X(0, 1, 0);
				BROADCAST_B_PAIR(1, 1); MATMUL_4X(0, 1, 1);
				BROADCAST_B_PAIR(1, 2); MATMUL_4X(0, 1, 2);
				ptr_b1 += 4 * 2;
			}
			if (k_count > 0) {
				MASK_LOAD_A_PAIR_TAIL(0);
				ptr_a0 += m_count;
				BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR_TAIL(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR_TAIL(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4;
				BROADCAST_B_PAIR_TAIL(1, 0); MATMUL_4X(0, 1, 0);
				BROADCAST_B_PAIR_TAIL(1, 1); MATMUL_4X(0, 1, 1);
				BROADCAST_B_PAIR_TAIL(1, 2); MATMUL_4X(0, 1, 2);
				ptr_b1 += 4;
			}
			ptr_c0 = ptr_c;
			MASK_STORE_4X(0, 0, 0); MASK_STORE_4X(0, 0, 1); MASK_STORE_4X(0, 0, 2);
			MASK_STORE_4X(0, 1, 0); MASK_STORE_4X(0, 1, 1); MASK_STORE_4X(0, 1, 2);
			ptr_c += m_count;
		}
		ptr_b += 24 * k;
		cn_offset += 24;
	}
	for (; n_count > 11; n_count -= 12) {
		IFLOAT *ptr_b00 = ptr_b;
		ptr_a0 = ptr_a;
		ptr_a1 = ptr_a + 16 * k;
		ptr_c = C + cn_offset * ldc;
		m_count = m;
		for (; m_count > 31; m_count -= 32) {
			ptr_b0 = ptr_b00;
			DECLARE_A_PAIR(0); DECLARE_A_PAIR(1);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0); DECLARE_RESULT_4X(0, 0, 1); DECLARE_RESULT_4X(0, 0, 2);
			DECLARE_RESULT_4X(1, 0, 0); DECLARE_RESULT_4X(1, 0, 1); DECLARE_RESULT_4X(1, 0, 2);
			for (k_count = k; k_count > 1; k_count -=2) {
				LOAD_A_PAIR(0); LOAD_A_PAIR(1);
				ptr_a0 += 16 * 2;
				ptr_a1 += 16 * 2;
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0); MATMUL_4X(1, 0, 0);
				BROADCAST_B_PAIR(0, 1); MATMUL_4X(0, 0, 1); MATMUL_4X(1, 0, 1);
				BROADCAST_B_PAIR(0, 2); MATMUL_4X(0, 0, 2); MATMUL_4X(1, 0, 2);
				ptr_b0 += 4 * 2;
			}
			if (k_count > 0) {
				LOAD_A_PAIR_TAIL(0); LOAD_A_PAIR_TAIL(1);
				ptr_a0 += 16;
				ptr_a1 += 16;
				BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0); MATMUL_4X(1, 0, 0);
				BROADCAST_B_PAIR_TAIL(0, 1); MATMUL_4X(0, 0, 1); MATMUL_4X(1, 0, 1);
				BROADCAST_B_PAIR_TAIL(0, 2); MATMUL_4X(0, 0, 2); MATMUL_4X(1, 0, 2);
				ptr_b0 += 4;
			}
			ptr_c0 = ptr_c;
			ptr_c1 = ptr_c + 16;
			STORE_4X(0, 0, 0); STORE_4X(1, 0, 0);
			STORE_4X(0, 0, 1); STORE_4X(1, 0, 1);
			STORE_4X(0, 0, 2); STORE_4X(1, 0, 2);
			ptr_c += 16 * 2;
			ptr_a0 = ptr_a1;
			ptr_a1 = ptr_a0 + 16 * k;
		}
		for (; m_count > 15; m_count -= 16) {
			ptr_b0 = ptr_b00;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0); DECLARE_RESULT_4X(0, 0, 1); DECLARE_RESULT_4X(0, 0, 2);
			for (k_count = k; k_count > 1; k_count -=2) {
				LOAD_A_PAIR(0);
				ptr_a0 += 16 * 2;
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4 * 2;
			}
			if (k_count > 0) {
				LOAD_A_PAIR_TAIL(0);
				ptr_a0 += 16;
				BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR_TAIL(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR_TAIL(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4;
			}
			ptr_c0 = ptr_c;
			STORE_4X(0, 0, 0); STORE_4X(0, 0, 1); STORE_4X(0, 0, 2);
			ptr_c += 16;
		}
		if (m_count > 0) {
			__mmask16 mmask = (1UL << m_count) - 1;
			ptr_b0 = ptr_b00;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0); DECLARE_RESULT_4X(0, 0, 1); DECLARE_RESULT_4X(0, 0, 2);
			for (k_count = k; k_count > 1; k_count -=2) {
				MASK_LOAD_A_PAIR(0);
				ptr_a0 += m_count * 2;
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4 * 2;
			}
			if (k_count > 0) {
				MASK_LOAD_A_PAIR_TAIL(0);
				ptr_a0 += m_count;
				BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				BROADCAST_B_PAIR_TAIL(0, 1); MATMUL_4X(0, 0, 1);
				BROADCAST_B_PAIR_TAIL(0, 2); MATMUL_4X(0, 0, 2);
				ptr_b0 += 4;
			}
			ptr_c0 = ptr_c;
			MASK_STORE_4X(0, 0, 0); MASK_STORE_4X(0, 0, 1); MASK_STORE_4X(0, 0, 2);
			ptr_c += m_count;
		}
		ptr_b += 12 * k;
		cn_offset += 12;
	}
	for (; n_count > 3; n_count -= 4) {
		IFLOAT *ptr_b00 = ptr_b;
		ptr_a0 = ptr_a;
		ptr_c = C + cn_offset * ldc;
		m_count = m;
		for (; m_count > 15; m_count -= 16) {
			ptr_b0 = ptr_b00;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0);
			for (k_count = k; k_count > 1; k_count -=2) {
				LOAD_A_PAIR(0);
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += 4 * 2;
				ptr_a0 += 16 * 2;
			}
			if (k_count > 0) {
				LOAD_A_PAIR_TAIL(0);
				BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += 4;
				ptr_a0 += 16;
			}
			ptr_c0 = ptr_c;
			STORE_4X(0, 0, 0);
			ptr_c += 16;
		}
		if (m_count > 0) {
			__mmask16 mmask = (1UL << m_count) - 1;
			ptr_b0 = ptr_b00;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0);
			for (k_count = k; k_count > 1; k_count -=2) {
				MASK_LOAD_A_PAIR(0);
				BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += 4 * 2;
				ptr_a0 += m_count * 2;
			}
			if (k_count > 0) {
				MASK_LOAD_A_PAIR_TAIL(0);
				BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += 4;
				ptr_a0 += m_count;
			}
			ptr_c0 = ptr_c;
			MASK_STORE_4X(0, 0, 0);
			ptr_c += m_count;
		}
		ptr_b += 4 * k;
		cn_offset += 4;
	}
	if (n_count > 0) {
		__mmask8 nmask = (1UL << n_count) - 1;
		IFLOAT *ptr_b00 = ptr_b;
		ptr_a0 = ptr_a;
		ptr_c = C + cn_offset * ldc;
		m_count = m;
		for (; m_count > 15; m_count -= 16) {
			ptr_b0 = ptr_b00;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0);
			for (k_count = k; k_count > 1; k_count -=2) {
				LOAD_A_PAIR(0);
				MASK_BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += n_count * 2;
				ptr_a0 += 16 * 2;
			}
			if (k_count > 0) {
				LOAD_A_PAIR_TAIL(0);
				MASK_BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += n_count;
				ptr_a0 += 16;
			}
			ptr_c0 = ptr_c;
			N_STORE_4X(0, 0, 0);
			ptr_c += 16;
		}
		if (m_count > 0) {
			__mmask16 mmask = (1UL << m_count) - 1;
			ptr_b0 = ptr_b00;
			DECLARE_A_PAIR(0);
			DECLARE_B_PAIR();
			DECLARE_RESULT_4X(0, 0, 0);
			for (k_count = k; k_count > 1; k_count -=2) {
				MASK_LOAD_A_PAIR(0);
				MASK_BROADCAST_B_PAIR(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += n_count * 2;
				ptr_a0 += m_count * 2;
			}
			if (k_count > 0) {
				MASK_LOAD_A_PAIR_TAIL(0);
				MASK_BROADCAST_B_PAIR_TAIL(0, 0); MATMUL_4X(0, 0, 0);
				ptr_b0 += n_count;
				ptr_a0 += m_count;
			}
			ptr_c0 = ptr_c;
			N_MASK_STORE_4X(0, 0, 0);
			ptr_c += m_count;
		}
	}
	return 0;
}
