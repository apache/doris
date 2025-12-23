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

#include <stdio.h>
#include <immintrin.h>
#include "common.h"

#define _MM512_SHUFFLE_i32(result, in1, in2, imm8) \
	asm("vshufps %3, %2, %1, %0": "=v"(result): "v"(in1), "v"(in2), "N"(imm8))

#define REORDER_8x32(t0, t1, t2, t3, t4, t5, t6, t7) { \
	__m512i v; \
	t0 = _mm512_unpacklo_epi32(r0, r1); \
	t1 = _mm512_unpackhi_epi32(r0, r1); \
	t2 = _mm512_unpacklo_epi32(r2, r3); \
	t3 = _mm512_unpackhi_epi32(r2, r3); \
	t4 = _mm512_unpacklo_epi32(r4, r5); \
	t5 = _mm512_unpackhi_epi32(r4, r5); \
	t6 = _mm512_unpacklo_epi32(r6, r7); \
	t7 = _mm512_unpackhi_epi32(r6, r7); \
	_MM512_SHUFFLE_i32(v, t0, t2, 0x4E); \
	r0 = _mm512_mask_blend_epi32(kc, t0, v); \
	r1 = _mm512_mask_blend_epi32(k3, t2, v); \
	_MM512_SHUFFLE_i32(v, t1, t3, 0x4E); \
	r2 = _mm512_mask_blend_epi32(kc, t1, v); \
	r3 = _mm512_mask_blend_epi32(k3, t3, v); \
	_MM512_SHUFFLE_i32(v, t4, t6, 0x4E); \
	r4 = _mm512_mask_blend_epi32(kc, t4, v); \
	r5 = _mm512_mask_blend_epi32(k3, t6, v); \
	_MM512_SHUFFLE_i32(v, t5, t7, 0x4E); \
	r6 = _mm512_mask_blend_epi32(kc, t5, v); \
	r7 = _mm512_mask_blend_epi32(k3, t7, v); \
	t0 = _mm512_permutex2var_epi32(r0, idx_lo, r4); \
	t1 = _mm512_permutex2var_epi32(r1, idx_lo, r5); \
	t2 = _mm512_permutex2var_epi32(r2, idx_lo, r6); \
	t3 = _mm512_permutex2var_epi32(r3, idx_lo, r7); \
	t4 = _mm512_permutex2var_epi32(r0, idx_hi, r4); \
	t5 = _mm512_permutex2var_epi32(r1, idx_hi, r5); \
	t6 = _mm512_permutex2var_epi32(r2, idx_hi, r6); \
	t7 = _mm512_permutex2var_epi32(r3, idx_hi, r7); \
}

#define STORE_512_LO(x) \
	v = _mm512_permutex2var_epi64(t0##x, idx_lo2, t1##x); \
	_mm512_storeu_si512(boffset0 + x*32, v);

#define STORE_512_HI(x) \
	v = _mm512_permutex2var_epi64(t0##x, idx_hi2, t1##x); \
	_mm512_storeu_si512(boffset0 + (x + 8)*32, v);

#define MASK_STORE_512_LO(x) \
	v = _mm512_permutex2var_epi64(t0##x, idx_lo2, t1##x); \
	_mm512_mask_storeu_epi32(boffset0 + 2*x*remain_n, nmask, v);

#define MASK_STORE_512_HI(x) \
	v = _mm512_permutex2var_epi64(t0##x, idx_hi2, t1##x); \
	_mm512_mask_storeu_epi32(boffset0 + 2*(x + 8)*remain_n, nmask, v);

#define STORE_512(x, y) {\
	__m512i v; \
	if (x == 0) { STORE_512_LO(y); } \
	else { STORE_512_HI(y); } \
}

#define MASK_STORE_512(x, y) {\
	__m512i v; \
	if (x == 0) { MASK_STORE_512_LO(y); } \
	else { MASK_STORE_512_HI(y); } \
}

#define SET_TAIL(y, x) {\
	if (y == 0) tail = _mm512_permutex2var_epi64(t0##x, idx_lo2, t1##x); \
	else tail = _mm512_permutex2var_epi64(t0##x, idx_hi2, t1##x); \
}

#define GET_TAIL() \
	switch (n_store + 1) { \
		case 16: SET_TAIL(1, 7); break; \
		case 15: SET_TAIL(1, 6); break; \
		case 14: SET_TAIL(1, 5); break; \
		case 13: SET_TAIL(1, 4); break; \
		case 12: SET_TAIL(1, 3); break; \
		case 11: SET_TAIL(1, 2); break; \
		case 10: SET_TAIL(1, 1); break; \
		case  9: SET_TAIL(1, 0); break; \
		case  8: SET_TAIL(0, 7); break; \
		case  7: SET_TAIL(0, 6); break; \
		case  6: SET_TAIL(0, 5); break; \
		case  5: SET_TAIL(0, 4); break; \
		case  4: SET_TAIL(0, 3); break; \
		case  3: SET_TAIL(0, 2); break; \
		case  2: SET_TAIL(0, 1); break; \
		case  1: SET_TAIL(0, 0); break; \
	}


int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){
	BLASLONG i, j;

	IFLOAT *boffset0;
	IFLOAT *aoffset;
	IFLOAT *aoffset00, *aoffset01, *aoffset02, *aoffset03, *aoffset04, *aoffset05, *aoffset06, *aoffset07;
	IFLOAT *aoffset10, *aoffset11, *aoffset12, *aoffset13, *aoffset14, *aoffset15, *aoffset16, *aoffset17;
	aoffset = a;
	boffset0   = b;

	BLASLONG n16 = n & ~15;
	BLASLONG m32 = m & ~31;

	int permute_table[] = {
		0x0, 0x1, 0x2, 0x3, 0x10, 0x11, 0x12, 0x13, 0x8, 0x9, 0xa, 0xb, 0x18, 0x19, 0x1a, 0x1b,
		0x4, 0x5, 0x6, 0x7, 0x14, 0x15, 0x16, 0x17, 0xc, 0xd, 0xe, 0xf, 0x1c, 0x1d, 0x1e, 0x1f,
	};
	uint64_t permute_table2[] = {
		0x00, 0x01, 0x02, 0x03, 8|0x0, 8|0x1, 8|0x2, 8|0x3,
		0x04, 0x05, 0x06, 0x07, 8|0x4, 8|0x5, 8|0x6, 8|0x7,
	};
	__m512i idx_lo = _mm512_loadu_si512(permute_table);
	__m512i idx_hi = _mm512_loadu_si512(permute_table + 16);
	__m512i idx_lo2 = _mm512_loadu_si512(permute_table2);
	__m512i idx_hi2 = _mm512_loadu_si512(permute_table2 + 8);
	__mmask16 kc = 0xcccc;
	__mmask16 k3 = 0x3333;
	__m512i r0, r1, r2, r3, r4, r5, r6, r7;
	__m512i t00, t01, t02, t03, t04, t05, t06, t07;
	__m512i t10, t11, t12, t13, t14, t15, t16, t17;

	for (j = 0; j < n16; j += 16) {
		aoffset00 = aoffset;
		aoffset01 = aoffset00 + lda;
		aoffset02 = aoffset01 + lda;
		aoffset03 = aoffset02 + lda;
		aoffset04 = aoffset03 + lda;
		aoffset05 = aoffset04 + lda;
		aoffset06 = aoffset05 + lda;
		aoffset07 = aoffset06 + lda;
		aoffset10 = aoffset07 + lda;
		aoffset11 = aoffset10 + lda;
		aoffset12 = aoffset11 + lda;
		aoffset13 = aoffset12 + lda;
		aoffset14 = aoffset13 + lda;
		aoffset15 = aoffset14 + lda;
		aoffset16 = aoffset15 + lda;
		aoffset17 = aoffset16 + lda;
		aoffset += 16 * lda;
		for (i = 0; i < m32; i += 32) {
			r0 = _mm512_loadu_si512(aoffset00 + i);
			r1 = _mm512_loadu_si512(aoffset01 + i);
			r2 = _mm512_loadu_si512(aoffset02 + i);
			r3 = _mm512_loadu_si512(aoffset03 + i);
			r4 = _mm512_loadu_si512(aoffset04 + i);
			r5 = _mm512_loadu_si512(aoffset05 + i);
			r6 = _mm512_loadu_si512(aoffset06 + i);
			r7 = _mm512_loadu_si512(aoffset07 + i);
			REORDER_8x32(t00, t01, t02, t03, t04, t05, t06, t07);
			r0 = _mm512_loadu_si512(aoffset10 + i);
			r1 = _mm512_loadu_si512(aoffset11 + i);
			r2 = _mm512_loadu_si512(aoffset12 + i);
			r3 = _mm512_loadu_si512(aoffset13 + i);
			r4 = _mm512_loadu_si512(aoffset14 + i);
			r5 = _mm512_loadu_si512(aoffset15 + i);
			r6 = _mm512_loadu_si512(aoffset16 + i);
			r7 = _mm512_loadu_si512(aoffset17 + i);
			REORDER_8x32(t10, t11, t12, t13, t14, t15, t16, t17);
			STORE_512(0, 0); STORE_512(0, 1); STORE_512(0, 2); STORE_512(0, 3);
			STORE_512(0, 4); STORE_512(0, 5); STORE_512(0, 6); STORE_512(0, 7);
			STORE_512(1, 0); STORE_512(1, 1); STORE_512(1, 2); STORE_512(1, 3);
			STORE_512(1, 4); STORE_512(1, 5); STORE_512(1, 6); STORE_512(1, 7);
			boffset0 += 16 * 32;
		}
		if (i < m) {
			int remain_m = m - i;
			__mmask32 mmask = (1UL << remain_m) - 1;
			r0 = _mm512_maskz_loadu_epi16(mmask, aoffset00 + i);
			r1 = _mm512_maskz_loadu_epi16(mmask, aoffset01 + i);
			r2 = _mm512_maskz_loadu_epi16(mmask, aoffset02 + i);
			r3 = _mm512_maskz_loadu_epi16(mmask, aoffset03 + i);
			r4 = _mm512_maskz_loadu_epi16(mmask, aoffset04 + i);
			r5 = _mm512_maskz_loadu_epi16(mmask, aoffset05 + i);
			r6 = _mm512_maskz_loadu_epi16(mmask, aoffset06 + i);
			r7 = _mm512_maskz_loadu_epi16(mmask, aoffset07 + i);
			REORDER_8x32(t00, t01, t02, t03, t04, t05, t06, t07);
			r0 = _mm512_maskz_loadu_epi16(mmask, aoffset10 + i);
			r1 = _mm512_maskz_loadu_epi16(mmask, aoffset11 + i);
			r2 = _mm512_maskz_loadu_epi16(mmask, aoffset12 + i);
			r3 = _mm512_maskz_loadu_epi16(mmask, aoffset13 + i);
			r4 = _mm512_maskz_loadu_epi16(mmask, aoffset14 + i);
			r5 = _mm512_maskz_loadu_epi16(mmask, aoffset15 + i);
			r6 = _mm512_maskz_loadu_epi16(mmask, aoffset16 + i);
			r7 = _mm512_maskz_loadu_epi16(mmask, aoffset17 + i);
			REORDER_8x32(t10, t11, t12, t13, t14, t15, t16, t17);
			int n_store = remain_m/2;
			switch (n_store) {
				case 15: STORE_512(1, 6);
				case 14: STORE_512(1, 5);
				case 13: STORE_512(1, 4);
				case 12: STORE_512(1, 3);
				case 11: STORE_512(1, 2);
				case 10: STORE_512(1, 1);
				case  9: STORE_512(1, 0);
				case  8: STORE_512(0, 7);
				case  7: STORE_512(0, 6);
				case  6: STORE_512(0, 5);
				case  5: STORE_512(0, 4);
				case  4: STORE_512(0, 3);
				case  3: STORE_512(0, 2);
				case  2: STORE_512(0, 1);
				case  1: STORE_512(0, 0);
			}
			boffset0 += n_store * 32;
			if (m & 0x1) {
				__m512i tail;
				GET_TAIL();
				_mm256_storeu_si256((void *)boffset0, _mm512_cvtepi32_epi16(tail));
				boffset0 += 16;
			}
		}

	}
	if (j < n) {
		int remain_n = n - j;
		__mmask16 nmask = (1UL << remain_n) - 1;
		int load0, load1;
		if (remain_n > 8) {
			load0 = 8;
			load1 = remain_n - 8;
		} else {
			load0 = remain_n;
			load1 = 0;
		}
		aoffset00 = aoffset;
		aoffset01 = aoffset00 + lda;
		aoffset02 = aoffset01 + lda;
		aoffset03 = aoffset02 + lda;
		aoffset04 = aoffset03 + lda;
		aoffset05 = aoffset04 + lda;
		aoffset06 = aoffset05 + lda;
		aoffset07 = aoffset06 + lda;
		aoffset10 = aoffset07 + lda;
		aoffset11 = aoffset10 + lda;
		aoffset12 = aoffset11 + lda;
		aoffset13 = aoffset12 + lda;
		aoffset14 = aoffset13 + lda;
		aoffset15 = aoffset14 + lda;
		aoffset16 = aoffset15 + lda;
		aoffset17 = aoffset16 + lda;
		aoffset += 16 * lda;
		for (i = 0; i < m32; i += 32) {
			switch (load0) {
				case 8: r7 = _mm512_loadu_si512(aoffset07 + i);
				case 7: r6 = _mm512_loadu_si512(aoffset06 + i);
				case 6: r5 = _mm512_loadu_si512(aoffset05 + i);
				case 5: r4 = _mm512_loadu_si512(aoffset04 + i);
				case 4: r3 = _mm512_loadu_si512(aoffset03 + i);
				case 3: r2 = _mm512_loadu_si512(aoffset02 + i);
				case 2: r1 = _mm512_loadu_si512(aoffset01 + i);
				case 1: r0 = _mm512_loadu_si512(aoffset00 + i);
			}
			REORDER_8x32(t00, t01, t02, t03, t04, t05, t06, t07);
			switch (load1) {
				case 8: r7 = _mm512_loadu_si512(aoffset17 + i);
				case 7: r6 = _mm512_loadu_si512(aoffset16 + i);
				case 6: r5 = _mm512_loadu_si512(aoffset15 + i);
				case 5: r4 = _mm512_loadu_si512(aoffset14 + i);
				case 4: r3 = _mm512_loadu_si512(aoffset13 + i);
				case 3: r2 = _mm512_loadu_si512(aoffset12 + i);
				case 2: r1 = _mm512_loadu_si512(aoffset11 + i);
				case 1: r0 = _mm512_loadu_si512(aoffset10 + i);
			}
			REORDER_8x32(t10, t11, t12, t13, t14, t15, t16, t17);
			MASK_STORE_512(0, 0); MASK_STORE_512(0, 1); MASK_STORE_512(0, 2); MASK_STORE_512(0, 3);
			MASK_STORE_512(0, 4); MASK_STORE_512(0, 5); MASK_STORE_512(0, 6); MASK_STORE_512(0, 7);
			MASK_STORE_512(1, 0); MASK_STORE_512(1, 1); MASK_STORE_512(1, 2); MASK_STORE_512(1, 3);
			MASK_STORE_512(1, 4); MASK_STORE_512(1, 5); MASK_STORE_512(1, 6); MASK_STORE_512(1, 7);
			boffset0 += remain_n * 32;
		}
		if (i < m) {
			int remain_m = m - i;
			__mmask32 mmask = (1UL << remain_m) - 1;
			switch (load0) {
				case 8: r7 = _mm512_maskz_loadu_epi16(mmask, aoffset07 + i);
				case 7: r6 = _mm512_maskz_loadu_epi16(mmask, aoffset06 + i);
				case 6: r5 = _mm512_maskz_loadu_epi16(mmask, aoffset05 + i);
				case 5: r4 = _mm512_maskz_loadu_epi16(mmask, aoffset04 + i);
				case 4: r3 = _mm512_maskz_loadu_epi16(mmask, aoffset03 + i);
				case 3: r2 = _mm512_maskz_loadu_epi16(mmask, aoffset02 + i);
				case 2: r1 = _mm512_maskz_loadu_epi16(mmask, aoffset01 + i);
				case 1: r0 = _mm512_maskz_loadu_epi16(mmask, aoffset00 + i);
			}
			REORDER_8x32(t00, t01, t02, t03, t04, t05, t06, t07);
			switch (load1) {
				case 8: r7 = _mm512_maskz_loadu_epi16(mmask, aoffset17 + i);
				case 7: r6 = _mm512_maskz_loadu_epi16(mmask, aoffset16 + i);
				case 6: r5 = _mm512_maskz_loadu_epi16(mmask, aoffset15 + i);
				case 5: r4 = _mm512_maskz_loadu_epi16(mmask, aoffset14 + i);
				case 4: r3 = _mm512_maskz_loadu_epi16(mmask, aoffset13 + i);
				case 3: r2 = _mm512_maskz_loadu_epi16(mmask, aoffset12 + i);
				case 2: r1 = _mm512_maskz_loadu_epi16(mmask, aoffset11 + i);
				case 1: r0 = _mm512_maskz_loadu_epi16(mmask, aoffset10 + i);
			}
			REORDER_8x32(t10, t11, t12, t13, t14, t15, t16, t17);
			int n_store = remain_m/2;
			switch (n_store) {
				case 15: MASK_STORE_512(1, 6);
				case 14: MASK_STORE_512(1, 5);
				case 13: MASK_STORE_512(1, 4);
				case 12: MASK_STORE_512(1, 3);
				case 11: MASK_STORE_512(1, 2);
				case 10: MASK_STORE_512(1, 1);
				case  9: MASK_STORE_512(1, 0);
				case  8: MASK_STORE_512(0, 7);
				case  7: MASK_STORE_512(0, 6);
				case  6: MASK_STORE_512(0, 5);
				case  5: MASK_STORE_512(0, 4);
				case  4: MASK_STORE_512(0, 3);
				case  3: MASK_STORE_512(0, 2);
				case  2: MASK_STORE_512(0, 1);
				case  1: MASK_STORE_512(0, 0);
			}
			boffset0 += n_store * remain_n * 2;
			if (m & 0x1) {
				__m512i tail;
				GET_TAIL();
				_mm256_mask_storeu_epi16((void *)boffset0, nmask, _mm512_cvtepi32_epi16(tail));
			}
		}
	}
	return 0;
}
