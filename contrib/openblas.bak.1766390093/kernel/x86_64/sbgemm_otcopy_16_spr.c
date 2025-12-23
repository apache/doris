/***************************************************************************
 * Copyright (c) 2021, The OpenBLAS Project
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in
 * the documentation and/or other materials provided with the
 * distribution.
 * 3. Neither the name of the OpenBLAS project nor the names of
 * its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * *****************************************************************************/

#include <immintrin.h>
#include "common.h"

#define LOAD_A_8VEC(aptr)  \
	r0 = _mm256_loadu_si256((__m256i *)(aptr + lda*0)); \
	r1 = _mm256_loadu_si256((__m256i *)(aptr + lda*1)); \
	r2 = _mm256_loadu_si256((__m256i *)(aptr + lda*2)); \
	r3 = _mm256_loadu_si256((__m256i *)(aptr + lda*3)); \
	r4 = _mm256_loadu_si256((__m256i *)(aptr + lda*4)); \
	r5 = _mm256_loadu_si256((__m256i *)(aptr + lda*5)); \
	r6 = _mm256_loadu_si256((__m256i *)(aptr + lda*6)); \
	r7 = _mm256_loadu_si256((__m256i *)(aptr + lda*7));

#define MASK_LOAD_A_8VEC(aptr)  \
	r0 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*0)); \
	r1 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*1)); \
	r2 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*2)); \
	r3 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*3)); \
	r4 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*4)); \
	r5 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*5)); \
	r6 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*6)); \
	r7 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*7));

#define SWITCH_LOAD_A_8VEC(aptr, cond) \
	switch((cond)) { \
		case 8: r7 = _mm256_loadu_si256((__m256i *)(aptr + lda*7)); \
		case 7: r6 = _mm256_loadu_si256((__m256i *)(aptr + lda*6)); \
		case 6: r5 = _mm256_loadu_si256((__m256i *)(aptr + lda*5)); \
		case 5: r4 = _mm256_loadu_si256((__m256i *)(aptr + lda*4)); \
		case 4: r3 = _mm256_loadu_si256((__m256i *)(aptr + lda*3)); \
		case 3: r2 = _mm256_loadu_si256((__m256i *)(aptr + lda*2)); \
		case 2: r1 = _mm256_loadu_si256((__m256i *)(aptr + lda*1)); \
		case 1: r0 = _mm256_loadu_si256((__m256i *)(aptr + lda*0)); \
	}

#define SWITCH_MASK_LOAD_A_8VEC(aptr, cond) \
	switch((cond)) { \
		case 8: r7 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*7)); \
		case 7: r6 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*6)); \
		case 6: r5 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*5)); \
		case 5: r4 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*4)); \
		case 4: r3 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*3)); \
		case 3: r2 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*2)); \
		case 2: r1 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*1)); \
		case 1: r0 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aptr + lda*0)); \
	}

#define REORDER_8x16(t0, t1, t2, t3, t4, t5, t6, t7) \
	t0 = _mm256_unpacklo_epi16(r0, r1); \
	t1 = _mm256_unpackhi_epi16(r0, r1); \
	t2 = _mm256_unpacklo_epi16(r2, r3); \
	t3 = _mm256_unpackhi_epi16(r2, r3); \
	t4 = _mm256_unpacklo_epi16(r4, r5); \
	t5 = _mm256_unpackhi_epi16(r4, r5); \
	t6 = _mm256_unpacklo_epi16(r6, r7); \
	t7 = _mm256_unpackhi_epi16(r6, r7); \
	r0 = _mm256_unpacklo_epi32(t0, t2); \
	r1 = _mm256_unpacklo_epi32(t1, t3); \
	r2 = _mm256_unpacklo_epi32(t4, t6); \
	r3 = _mm256_unpacklo_epi32(t5, t7); \
	r4 = _mm256_unpackhi_epi32(t0, t2); \
	r5 = _mm256_unpackhi_epi32(t1, t3); \
	r6 = _mm256_unpackhi_epi32(t4, t6); \
	r7 = _mm256_unpackhi_epi32(t5, t7); \
	t0 = _mm256_unpacklo_epi64(r0, r2); \
	t1 = _mm256_unpackhi_epi64(r0, r2); \
	t2 = _mm256_unpacklo_epi64(r4, r6); \
	t3 = _mm256_unpackhi_epi64(r4, r6); \
	t4 = _mm256_unpacklo_epi64(r1, r3); \
	t5 = _mm256_unpackhi_epi64(r1, r3); \
	t6 = _mm256_unpacklo_epi64(r5, r7); \
	t7 = _mm256_unpackhi_epi64(r5, r7);

#define STORE_256_LO(x) \
	v = _mm256_permute2x128_si256(t0##x, t1##x, 0x20); \
	_mm256_storeu_si256((__m256i *)(boffset + x*32), v);

#define STORE_256_HI(x) \
	v = _mm256_permute2x128_si256(t0##x, t1##x, 0x31); \
	_mm256_storeu_si256((__m256i *)(boffset + (x + 8)*32), v);

#define MASK_STORE_256_LO(x) \
	v = _mm256_permute2x128_si256(t0##x, t1##x, 0x20); \
	_mm256_mask_storeu_epi16(boffset + x*m_load, mmask, v);

#define MASK_STORE_256_HI(x) \
	v = _mm256_permute2x128_si256(t0##x, t1##x, 0x31); \
	_mm256_mask_storeu_epi16(boffset + (x + 8)*m_load, mmask, v);

#define STORE_256(x, y) {\
	__m256i v; \
	if (x == 0) { STORE_256_LO(y); } \
	else { STORE_256_HI(y); } \
}

#define MASK_STORE_256(x, y) {\
	__m256i v; \
	if (x == 0) { MASK_STORE_256_LO(y); } \
	else { MASK_STORE_256_HI(y); } \
}

#define SWITCH_STORE_16x(cond, func) \
	switch((cond)) {\
		case 15: func(1, 6); \
		case 14: func(1, 5); \
		case 13: func(1, 4); \
		case 12: func(1, 3); \
		case 11: func(1, 2); \
		case 10: func(1, 1); \
		case 9: func(1, 0); \
		case 8: func(0, 7); \
		case 7: func(0, 6); \
		case 6: func(0, 5); \
		case 5: func(0, 4); \
		case 4: func(0, 3); \
		case 3: func(0, 2); \
		case 2: func(0, 1); \
		case 1: func(0, 0); \
	}


int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b) {
	IFLOAT *aoffset, *boffset;
	IFLOAT *aoffset00, *aoffset01, *aoffset10, *aoffset11;
	IFLOAT *boffset0;

	__m256i r0, r1, r2, r3, r4, r5, r6, r7;
	__m256i t00, t01, t02, t03, t04, t05, t06, t07;
	__m256i t10, t11, t12, t13, t14, t15, t16, t17;

	aoffset = a;
	boffset = b;
	BLASLONG n_count = n;
	BLASLONG m_count = m;
	for (; n_count > 15; n_count -= 16) {
		aoffset00 = aoffset;
		aoffset01 = aoffset00 + 8 * lda;
		aoffset10 = aoffset01 + 8 * lda;
		aoffset11 = aoffset10 + 8 * lda;
		aoffset += 16;
		m_count = m;
		for (; m_count > 31; m_count -= 32) {
			// first 16 rows
			LOAD_A_8VEC(aoffset00);
			REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
			LOAD_A_8VEC(aoffset01);
			REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
			STORE_256(0, 0); STORE_256(0, 1); STORE_256(0, 2); STORE_256(0, 3);
			STORE_256(0, 4); STORE_256(0, 5); STORE_256(0, 6); STORE_256(0, 7);
			STORE_256(1, 0); STORE_256(1, 1); STORE_256(1, 2); STORE_256(1, 3);
			STORE_256(1, 4); STORE_256(1, 5); STORE_256(1, 6); STORE_256(1, 7);
			// last 16 rows
			boffset += 16;
			LOAD_A_8VEC(aoffset10);
			REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
			LOAD_A_8VEC(aoffset11);
			REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
			STORE_256(0, 0); STORE_256(0, 1); STORE_256(0, 2); STORE_256(0, 3);
			STORE_256(0, 4); STORE_256(0, 5); STORE_256(0, 6); STORE_256(0, 7);
			STORE_256(1, 0); STORE_256(1, 1); STORE_256(1, 2); STORE_256(1, 3);
			STORE_256(1, 4); STORE_256(1, 5); STORE_256(1, 6); STORE_256(1, 7);
			aoffset00 += 32 * lda;
			aoffset01 += 32 * lda;
			aoffset10 += 32 * lda;
			aoffset11 += 32 * lda;
			boffset += 31 * 16;
		}
		if (m_count > 1) {
			int m_load = m_count & ~1;
			m_count -= m_load;
			__mmask16 mmask;
			SWITCH_LOAD_A_8VEC(aoffset00, m_load > 8 ? 8: m_load);
			REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
			if (m_load > 8) {
				SWITCH_LOAD_A_8VEC(aoffset01, m_load > 16 ? 8: m_load - 8);
				REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
			}
			int this_load = m_load > 16 ? 16 : m_load;
			mmask = (1UL << this_load) - 1;
			MASK_STORE_256(0, 0); MASK_STORE_256(0, 1); MASK_STORE_256(0, 2); MASK_STORE_256(0, 3);
			MASK_STORE_256(0, 4); MASK_STORE_256(0, 5); MASK_STORE_256(0, 6); MASK_STORE_256(0, 7);
			MASK_STORE_256(1, 0); MASK_STORE_256(1, 1); MASK_STORE_256(1, 2); MASK_STORE_256(1, 3);
			MASK_STORE_256(1, 4); MASK_STORE_256(1, 5); MASK_STORE_256(1, 6); MASK_STORE_256(1, 7);
			boffset0 = boffset;
			if (m_load > 16) {
				boffset += this_load;
				SWITCH_LOAD_A_8VEC(aoffset10, m_load > 24 ? 8: m_load - 16);
				REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
				if (m_load > 24) {
					SWITCH_LOAD_A_8VEC(aoffset11, m_load - 24);
					REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
				}
				this_load = m_load - 16;
				mmask = (1UL << this_load) - 1;
				MASK_STORE_256(0, 0); MASK_STORE_256(0, 1); MASK_STORE_256(0, 2); MASK_STORE_256(0, 3);
				MASK_STORE_256(0, 4); MASK_STORE_256(0, 5); MASK_STORE_256(0, 6); MASK_STORE_256(0, 7);
				MASK_STORE_256(1, 0); MASK_STORE_256(1, 1); MASK_STORE_256(1, 2); MASK_STORE_256(1, 3);
				MASK_STORE_256(1, 4); MASK_STORE_256(1, 5); MASK_STORE_256(1, 6); MASK_STORE_256(1, 7);
			}
			boffset = boffset0 + 16 * m_load;
			aoffset00 += m_load * lda;
		}
		if (m_count > 0) {
			// just copy lask K to B directly
			r0 = _mm256_loadu_si256((__m256i *)(aoffset00));
			_mm256_storeu_si256((__m256i *)(boffset), r0);
			boffset += 16;
		}
	}
	if (n_count > 0) {
		__mmask16 nmask = (1UL << n_count) - 1;
		aoffset00 = aoffset;
		aoffset01 = aoffset00 + 8 * lda;
		aoffset10 = aoffset01 + 8 * lda;
		aoffset11 = aoffset10 + 8 * lda;
		m_count = m;
		for (; m_count > 31; m_count -= 32) {
			// first 16 rows
			MASK_LOAD_A_8VEC(aoffset00);
			REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
			MASK_LOAD_A_8VEC(aoffset01);
			REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
			SWITCH_STORE_16x(n_count, STORE_256);
			// last 16 rows
			boffset0 = boffset;
			boffset += 16;
			MASK_LOAD_A_8VEC(aoffset10);
			REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
			MASK_LOAD_A_8VEC(aoffset11);
			REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
			SWITCH_STORE_16x(n_count, STORE_256);
			aoffset00 += 32 * lda;
			aoffset01 += 32 * lda;
			aoffset10 += 32 * lda;
			aoffset11 += 32 * lda;
			boffset = 32 * n_count + boffset0;
		}
		if (m_count > 1) {
			int m_load = m_count & ~1;
			m_count -= m_load;
			__mmask16 mmask;
			SWITCH_MASK_LOAD_A_8VEC(aoffset00, m_load > 8 ? 8: m_load);
			REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
			if (m_load > 8) {
				SWITCH_MASK_LOAD_A_8VEC(aoffset01, m_load > 16 ? 8: m_load - 8);
				REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
			}
			int this_load = m_load > 16 ? 16 : m_load;
			mmask = (1UL << this_load) - 1;
			SWITCH_STORE_16x(n_count, MASK_STORE_256);
			boffset0 = boffset;
			if (m_load > 16) {
				boffset += this_load;
				SWITCH_MASK_LOAD_A_8VEC(aoffset10, m_load > 24 ? 8: m_load - 16);
				REORDER_8x16(t00, t01, t02, t03, t04, t05, t06, t07);
				if (m_load > 24) {
					SWITCH_MASK_LOAD_A_8VEC(aoffset11, m_load - 24);
					REORDER_8x16(t10, t11, t12, t13, t14, t15, t16, t17);
				}
				this_load = m_load - 16;
				mmask = (1UL << this_load) - 1;
				SWITCH_STORE_16x(n_count, MASK_STORE_256);
			}
			boffset = boffset0 + n_count * m_load;
			aoffset00 += m_load * lda;
		}
		if (m_count > 0) {
			// just copy lask K to B directly
			r0 = _mm256_maskz_loadu_epi16(nmask, (__m256i *)(aoffset00));
			_mm256_mask_storeu_epi16((__m256i *)(boffset), nmask, r0);
			boffset += 16;
		}
	}
	return 0;
}
