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

#define STORE_VEC(Bx, By, vec) \
	if (By == 0) asm("vmovdqu16 %0, (%1)": : "v"(vec), "r"(boffset##Bx)); \
	else asm("vmovdqu16 %0, (%1, %2, %c3)": : "v"(vec), "r"(boffset##Bx), "r"(blk_size), "n"(By * 2));

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){
	BLASLONG i, j;

	IFLOAT *boffset0, *boffset1;

	boffset0   = b;

	BLASLONG n24 = n - (n % 24);
	BLASLONG n8 = n & ~7;
	BLASLONG m8 = m & ~7;
	BLASLONG m4 = m & ~3;
	BLASLONG m2 = m & ~1;

	int permute_table[] = {
		0x0, 0x1, 0x2, 0x3, 0x10, 0x11, 0x12, 0x13, 0x8, 0x9, 0xa, 0xb, 0x18, 0x19, 0x1a, 0x1b,
		0x4, 0x5, 0x6, 0x7, 0x14, 0x15, 0x16, 0x17, 0xc, 0xd, 0xe, 0xf, 0x1c, 0x1d, 0x1e, 0x1f,
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	};

	j = 0;
	if (n > 23) {
		/* n = 24 is the max width in current blocking setting */
		__m512i idx_lo_128 = _mm512_loadu_si512(permute_table);
		__m512i idx_hi_128 = _mm512_loadu_si512(permute_table + 16);
		__m512i idx_lo_256 = _mm512_loadu_si512(permute_table + 32);
		__m512i idx_hi_256 = _mm512_loadu_si512(permute_table + 48);
		__mmask32 mask24 = (1UL << 24) - 1;
		BLASLONG blk_size = m * 4;
		BLASLONG stride = blk_size * 3;

		for (; j < n24; j += 24) {
			boffset1 = boffset0 + stride;
			for (i = 0; i < m8; i += 8) {
				__m512i r0, r1, r2, r3, r4, r5, r6, r7;
				__m512i t0, t1, t2, t3, t4, t5, t6, t7;
				r0 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 0)*lda + j]);
				r1 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 1)*lda + j]);
				r2 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 2)*lda + j]);
				r3 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 3)*lda + j]);
				r4 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 4)*lda + j]);
				r5 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 5)*lda + j]);
				r6 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 6)*lda + j]);
				r7 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 7)*lda + j]);

				t0 = _mm512_unpacklo_epi16(r0, r1);
				t1 = _mm512_unpackhi_epi16(r0, r1);
				t2 = _mm512_unpacklo_epi16(r2, r3);
				t3 = _mm512_unpackhi_epi16(r2, r3);
				t4 = _mm512_unpacklo_epi16(r4, r5);
				t5 = _mm512_unpackhi_epi16(r4, r5);
				t6 = _mm512_unpacklo_epi16(r6, r7);
				t7 = _mm512_unpackhi_epi16(r6, r7);

				r0 = _mm512_permutex2var_epi32(t0, idx_lo_128, t2);
				r1 = _mm512_permutex2var_epi32(t1, idx_lo_128, t3);
				r2 = _mm512_permutex2var_epi32(t4, idx_lo_128, t6);
				r3 = _mm512_permutex2var_epi32(t5, idx_lo_128, t7);
				r4 = _mm512_permutex2var_epi32(t0, idx_hi_128, t2);
				r5 = _mm512_permutex2var_epi32(t1, idx_hi_128, t3);
				r6 = _mm512_permutex2var_epi32(t4, idx_hi_128, t6);
				r7 = _mm512_permutex2var_epi32(t5, idx_hi_128, t7);

				t0 = _mm512_permutex2var_epi32(r0, idx_lo_256, r2);
				t1 = _mm512_permutex2var_epi32(r1, idx_lo_256, r3);
				t2 = _mm512_permutex2var_epi32(r4, idx_lo_256, r6);
				t3 = _mm512_permutex2var_epi32(r5, idx_lo_256, r7);
				t4 = _mm512_permutex2var_epi32(r0, idx_hi_256, r2);
				t5 = _mm512_permutex2var_epi32(r1, idx_hi_256, r3);

				STORE_VEC(0, 0, t0); STORE_VEC(0, 1, t1); STORE_VEC(0, 2, t2);
				STORE_VEC(1, 0, t3); STORE_VEC(1, 1, t4); STORE_VEC(1, 2, t5);
				boffset0 += 32;
				boffset1 += 32;
			}
			for (; i < m2; i += 2) {
				__m512i r0, r1, t0, t1;
				r0 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 0)*lda + j]);
				r1 = _mm512_maskz_loadu_epi16(mask24, &a[(i + 1)*lda + j]);
				t0 = _mm512_unpacklo_epi16(r0, r1);
				t1 = _mm512_unpackhi_epi16(r0, r1);
				STORE_VEC(0, 0, _mm512_extracti32x4_epi32(t0, 0));
				STORE_VEC(0, 1, _mm512_extracti32x4_epi32(t1, 0));
				STORE_VEC(0, 2, _mm512_extracti32x4_epi32(t0, 1));
				STORE_VEC(1, 0, _mm512_extracti32x4_epi32(t1, 1));
				STORE_VEC(1, 1, _mm512_extracti32x4_epi32(t0, 2));
				STORE_VEC(1, 2, _mm512_extracti32x4_epi32(t1, 2));
				boffset0 += 8;
				boffset1 += 8;
			}
			for (; i < m; i++) {
				*(uint64_t *)(boffset0 + blk_size * 0) = *(uint64_t *)&a[i * lda + j + 0];
				*(uint64_t *)(boffset0 + blk_size * 1) = *(uint64_t *)&a[i * lda + j + 4];
				*(uint64_t *)(boffset0 + blk_size * 2) = *(uint64_t *)&a[i * lda + j + 8];
				*(uint64_t *)(boffset1 + blk_size * 0) = *(uint64_t *)&a[i * lda + j + 12];
				*(uint64_t *)(boffset1 + blk_size * 1) = *(uint64_t *)&a[i * lda + j + 16];
				*(uint64_t *)(boffset1 + blk_size * 2) = *(uint64_t *)&a[i * lda + j + 20];
				boffset0 += 4;
				boffset1 += 4;
			}
			boffset0 += stride * 2;
		}
	}

	for (; j < n8; j += 8) {
		boffset1 = boffset0 + m * 4;
		for (i = 0; i < m4; i += 4) {
			__m128i a0 = _mm_loadu_si128((void *)&a[(i + 0)*lda + j]);
			__m128i a1 = _mm_loadu_si128((void *)&a[(i + 1)*lda + j]);
			__m128i a2 = _mm_loadu_si128((void *)&a[(i + 2)*lda + j]);
			__m128i a3 = _mm_loadu_si128((void *)&a[(i + 3)*lda + j]);
			__m128i a00 = _mm_unpacklo_epi16(a0, a1);
			__m128i a01 = _mm_unpackhi_epi16(a0, a1);
			__m128i a10 = _mm_unpacklo_epi16(a2, a3);
			__m128i a11 = _mm_unpackhi_epi16(a2, a3);
			_mm_storeu_si128((void *)(boffset0 + 0), a00);
			_mm_storeu_si128((void *)(boffset0 + 8), a10);
			_mm_storeu_si128((void *)(boffset1 + 0), a01);
			_mm_storeu_si128((void *)(boffset1 + 8), a11);
			boffset0 += 16;
			boffset1 += 16;
		}
		for (; i < m2; i+= 2) {
			__m128i a0 = _mm_loadu_si128((void *)&a[(i + 0)*lda + j]);
			__m128i a1 = _mm_loadu_si128((void *)&a[(i + 1)*lda + j]);
			__m128i a00 = _mm_unpacklo_epi16(a0, a1);
			__m128i a01 = _mm_unpackhi_epi16(a0, a1);
			_mm_storeu_si128((void *)(boffset0 + 0), a00);
			_mm_storeu_si128((void *)(boffset1 + 0), a01);
			boffset0 += 8;
			boffset1 += 8;
		}
		for (; i < m; i++) {
			__m128d a0 = _mm_loadu_pd((void *)&a[(i + 0)*lda + j]);
			_mm_store_sd((void *)boffset0, a0);
			_mm_store_sd((void *)boffset1, _mm_permute_pd(a0, 0x1));
			boffset0 += 4;
			boffset1 += 4;
		}
		boffset0 = boffset1;
	}
	if (j < n) {
		uint32_t remains = n - j;
		__mmask8 r_mask = (1UL << remains) - 1;
		if (remains > 4) {
			boffset1 = boffset0 + m * 4;
			uint32_t tail1 = remains - 4;
			__mmask8 w_mask1 = (1UL << tail1) - 1;
			for (i = 0; i < m2; i += 2) {
				__m128i a0 = _mm_maskz_loadu_epi16(r_mask, &a[(i + 0)*lda + j]);
				__m128i a1 = _mm_maskz_loadu_epi16(r_mask, &a[(i + 1)*lda + j]);
				__m128i a00 = _mm_unpacklo_epi16(a0, a1);
				__m128i a01 = _mm_unpackhi_epi16(a0, a1);
				_mm_storeu_si128((void *)boffset0, a00);
				_mm_mask_storeu_epi32((void *)boffset1, w_mask1, a01);
				boffset0 += 8;
				boffset1 += 2 * tail1;
			}
			for (; i < m; i++) {
				__m128i a0 = _mm_maskz_loadu_epi16(r_mask, &a[(i + 0)*lda + j]);
				_mm_store_sd((void *)boffset0, (__m128d) a0);
				_mm_mask_storeu_epi16((void *)boffset1, w_mask1, (__m128i) _mm_permute_pd((__m128d) a0, 0x1));
				boffset0 += 4;
				boffset1 += tail1;
			}
		} else {
			for (i = 0; i < m2; i += 2) {
				__m128i a0 = _mm_maskz_loadu_epi16(r_mask, &a[(i + 0)*lda + j]);
				__m128i a1 = _mm_maskz_loadu_epi16(r_mask, &a[(i + 1)*lda + j]);
				__m128i a00 = _mm_unpacklo_epi16(a0, a1);
				_mm_mask_storeu_epi32((void *)boffset0, r_mask, a00);
				boffset0 += 2 * remains;
			}
			for (; i < m; i++) {
				__m128i a0 = _mm_maskz_loadu_epi16(r_mask, &a[(i + 0)*lda + j]);
				_mm_mask_storeu_epi16((void *)boffset0, r_mask, a0);
			}
		}
	}
	return 0;
}
