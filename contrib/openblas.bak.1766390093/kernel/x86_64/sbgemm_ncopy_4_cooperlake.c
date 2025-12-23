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

#define REORDER_4x32(r0, r1, r2, r3) {\
	__m512i t0, t1, t2, t3; \
	t0 = _mm512_unpacklo_epi32(r0, r1); \
	t1 = _mm512_unpackhi_epi32(r0, r1); \
	t2 = _mm512_unpacklo_epi32(r2, r3); \
	t3 = _mm512_unpackhi_epi32(r2, r3); \
	r0 = _mm512_unpacklo_epi64(t0, t2); \
	r1 = _mm512_unpackhi_epi64(t0, t2); \
	r2 = _mm512_unpacklo_epi64(t1, t3); \
	r3 = _mm512_unpackhi_epi64(t1, t3); \
	t0 = _mm512_permutex2var_epi32(r0, idx_lo_128, r1); \
	t1 = _mm512_permutex2var_epi32(r0, idx_hi_128, r1); \
	t2 = _mm512_permutex2var_epi32(r2, idx_lo_128, r3); \
	t3 = _mm512_permutex2var_epi32(r2, idx_hi_128, r3); \
	r0 = _mm512_permutex2var_epi32(t0, idx_lo_256, t2); \
	r1 = _mm512_permutex2var_epi32(t1, idx_lo_256, t3); \
	r2 = _mm512_permutex2var_epi32(t0, idx_hi_256, t2); \
	r3 = _mm512_permutex2var_epi32(t1, idx_hi_256, t3); \
}

#define REORDER_4x8(r0, r1, r2, r3) {\
	__m128i t0, t1, t2, t3; \
	t0 = _mm_unpacklo_epi32(r0, r1); \
	t1 = _mm_unpackhi_epi32(r0, r1); \
	t2 = _mm_unpacklo_epi32(r2, r3); \
	t3 = _mm_unpackhi_epi32(r2, r3); \
	r0 = _mm_unpacklo_epi64(t0, t2); \
	r1 = _mm_unpackhi_epi64(t0, t2); \
	r2 = _mm_unpacklo_epi64(t1, t3); \
	r3 = _mm_unpackhi_epi64(t1, t3); \
}

#define GET_TAIL(tail, remain_m) \
	switch((remain_m + 1)/2) { \
		case 1: tail = r0; break; \
		case 2: tail = r1; break; \
		case 3: tail = r2; break; \
		case 4: tail = r3; break; \
	}

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){
	BLASLONG i, j;
	IFLOAT *aoffset;
	IFLOAT *aoffset0, *aoffset1, *aoffset2, *aoffset3;

	IFLOAT *boffset;

	aoffset = a;
	boffset = b;

	BLASLONG m32 = m & ~31;
	BLASLONG m8 = m & ~7;
	BLASLONG n4 = n & ~3;

	int permute_table[] = {
		0x0, 0x1, 0x2, 0x3, 0x10, 0x11, 0x12, 0x13, 0x8, 0x9, 0xa, 0xb, 0x18, 0x19, 0x1a, 0x1b,
		0x4, 0x5, 0x6, 0x7, 0x14, 0x15, 0x16, 0x17, 0xc, 0xd, 0xe, 0xf, 0x1c, 0x1d, 0x1e, 0x1f,
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	};
	__m512i idx_lo_128 = _mm512_loadu_si512(permute_table);
	__m512i idx_hi_128 = _mm512_loadu_si512(permute_table + 16);
	__m512i idx_lo_256 = _mm512_loadu_si512(permute_table + 32);
	__m512i idx_hi_256 = _mm512_loadu_si512(permute_table + 48);

	for (j = 0; j < n4; j += 4) {
		aoffset0  = aoffset;
		aoffset1  = aoffset0 + lda;
		aoffset2  = aoffset1 + lda;
		aoffset3  = aoffset2 + lda;
		aoffset += 4 * lda;

		for (i = 0; i < m32; i += 32) {
			__m512i r0, r1, r2, r3;
			r0 = _mm512_loadu_si512(aoffset0 + i);
			r1 = _mm512_loadu_si512(aoffset1 + i);
			r2 = _mm512_loadu_si512(aoffset2 + i);
			r3 = _mm512_loadu_si512(aoffset3 + i);
			REORDER_4x32(r0, r1, r2, r3);
			_mm512_storeu_si512(boffset + 32*0, r0);
			_mm512_storeu_si512(boffset + 32*1, r1);
			_mm512_storeu_si512(boffset + 32*2, r2);
			_mm512_storeu_si512(boffset + 32*3, r3);
			boffset += 32 * 4;
		}
		for (; i < m8; i += 8) {
			__m128i r0 = _mm_loadu_si128((void *)(aoffset0 + i));
			__m128i r1 = _mm_loadu_si128((void *)(aoffset1 + i));
			__m128i r2 = _mm_loadu_si128((void *)(aoffset2 + i));
			__m128i r3 = _mm_loadu_si128((void *)(aoffset3 + i));
			REORDER_4x8(r0, r1, r2, r3);
			_mm_storeu_si128((void *)(boffset + 8*0), r0);
			_mm_storeu_si128((void *)(boffset + 8*1), r1);
			_mm_storeu_si128((void *)(boffset + 8*2), r2);
			_mm_storeu_si128((void *)(boffset + 8*3), r3);
			boffset += 8 * 4;
		}
		if (i < m) {
			int remain_m = m - i;
			__mmask8 r_mask = (1UL << remain_m) - 1;
			__m128i r0 = _mm_maskz_loadu_epi16(r_mask, aoffset0 + i);
			__m128i r1 = _mm_maskz_loadu_epi16(r_mask, aoffset1 + i);
			__m128i r2 = _mm_maskz_loadu_epi16(r_mask, aoffset2 + i);
			__m128i r3 = _mm_maskz_loadu_epi16(r_mask, aoffset3 + i);
			REORDER_4x8(r0, r1, r2, r3);

			// store should skip the tail odd line
			int num_store = remain_m/2;
			switch(num_store) {
				case 3: _mm_storeu_si128((void *)(boffset + 8*2), r2);
				case 2: _mm_storeu_si128((void *)(boffset + 8*1), r1);
				case 1: _mm_storeu_si128((void *)(boffset + 8*0), r0);
			}
			boffset += 8 * num_store;

			if (m & 0x1) { // handling the tail
				__m128i tail;
				GET_TAIL(tail, remain_m);
				/* tail vector is fill with zero like:
				 *     a, 0, b, 0, c, 0, d, 0
				 * need to extract lo words of data and store
				 */
				tail = _mm_cvtepi32_epi16(tail);
				_mm_store_sd((double *)boffset, (__m128d) tail); // only lower 4 bfloat valid
				boffset += 4;
			}
		}
	}
	if (j < n) {
		int remain_n = n - j;
		__mmask8 nmask = (1UL << remain_n) - 1;
		aoffset0  = aoffset;
		aoffset1  = aoffset0 + lda;
		aoffset2  = aoffset1 + lda;
		aoffset3  = aoffset2 + lda;
		__m128i r0, r1, r2, r3;
		for (i = 0; i < m8; i += 8) {
			switch (remain_n) {
				case 3: r2 = _mm_loadu_si128((void *)(aoffset2 + i));
				case 2: r1 = _mm_loadu_si128((void *)(aoffset1 + i));
				case 1: r0 = _mm_loadu_si128((void *)(aoffset0 + i));
			}
			REORDER_4x8(r0, r1, r2, r3);
			_mm_mask_storeu_epi32(boffset + remain_n * 0, nmask, r0);
			_mm_mask_storeu_epi32(boffset + remain_n * 2, nmask, r1);
			_mm_mask_storeu_epi32(boffset + remain_n * 4, nmask, r2);
			_mm_mask_storeu_epi32(boffset + remain_n * 6, nmask, r3);
			boffset += 8 * remain_n;
		}
		if (i < m) {
			int remain_m = m - i;
			__mmask8 mmask = (1UL << remain_m) - 1;
			switch (remain_n) {
				case 3: r2 = _mm_maskz_loadu_epi16(mmask, aoffset2 + i);
				case 2: r1 = _mm_maskz_loadu_epi16(mmask, aoffset1 + i);
				case 1: r0 = _mm_maskz_loadu_epi16(mmask, aoffset0 + i);
			}
			REORDER_4x8(r0, r1, r2, r3);

			int num_store = remain_m/2;
			switch (num_store) {
				case 3: _mm_mask_storeu_epi32(boffset + remain_n * 4, nmask, r2);
				case 2: _mm_mask_storeu_epi32(boffset + remain_n * 2, nmask, r1);
				case 1: _mm_mask_storeu_epi32(boffset + remain_n * 0, nmask, r0);
			}
			boffset += 2 * num_store * remain_n;

			if (m & 0x1) {
				__m128i tail;
				GET_TAIL(tail, remain_m);
				tail = _mm_cvtepi32_epi16(tail);
				_mm_mask_storeu_epi16(boffset, nmask, tail);
			}
		}
	}
	return 0;
}
