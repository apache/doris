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
#include <string.h>
#include "common.h"

#ifndef SBGEMM_KERNEL_SPR
#define SBGEMM_KERNEL_SPR
typedef struct {
	char palette_id;
	char start_row;
	char dummy0[14];  // bytes 2-15 reserved, must be zero
	short tile_colsb[8];
	char dummy1[16];  // bytes 32-47 reserved, must be zero
	char tile_rows[8];
	char dummy2[16];  // bytes 56-63 reserved, must be zero
} tilecfg;

/* tile0/tile1 -- A (m x 2k)
 * tile2/tile3 -- B (2k x n)
 * tile4-7 -- C (m x n)
 */
#define TCONF(cfg, m, n, k2) \
	memset(&cfg, 0, sizeof(tilecfg)); \
	cfg.palette_id = 1; \
	cfg.tile_rows[0] = m; \
	cfg.tile_rows[1] = m; \
	cfg.tile_rows[2] = k2>>1; \
	cfg.tile_rows[3] = k2>>1; \
	cfg.tile_rows[4] = m; \
	cfg.tile_rows[5] = m; \
	cfg.tile_rows[6] = m; \
	cfg.tile_rows[7] = m; \
	cfg.tile_colsb[0] = k2<<1; \
	cfg.tile_colsb[1] = k2<<1; \
	cfg.tile_colsb[2] = n * 4; \
	cfg.tile_colsb[3] = n * 4; \
	cfg.tile_colsb[4] = n * 4; \
	cfg.tile_colsb[5] = n * 4; \
	cfg.tile_colsb[6] = n * 4; \
	cfg.tile_colsb[7] = n * 4; \
	_tile_loadconfig(&cfg);

/* CONFIG for handling k2 and odd tail at the same time
 * tile0 -- A (m x 2k)
 * tile1 -- A (m x 1)
 * tile2 -- B (2k x n)
 * tile3 -- B (1 x n)
 * tile4 -- C (m x n)
 */
#define TCONF_TAIL(cfg, m, n, k2) \
	memset(&cfg, 0, sizeof(tilecfg)); \
	cfg.palette_id = 1; \
	cfg.tile_rows[0] = m; \
	cfg.tile_rows[1] = m; \
	cfg.tile_rows[2] = k2>>1; \
	cfg.tile_rows[3] = 1; \
	cfg.tile_rows[4] = m; \
	cfg.tile_colsb[0] = k2<<1; \
	cfg.tile_colsb[1] = 4; \
	cfg.tile_colsb[2] = n * 4; \
	cfg.tile_colsb[3] = n * 4; \
	cfg.tile_colsb[4] = n * 4; \
	_tile_loadconfig(&cfg);

#define T_A0	0
#define T_A1	1
#define T_B0	2
#define T_B1	3
#define T_C00	4
#define T_C01	5
#define T_C10	6
#define T_C11	7


#define LOAD_A(M, N) _tile_loadd(T_A##M, ptr_a##M, lda * 2)
#define LOAD_A_TAIL(M, N) {\
	__m256i ymm = _mm256_loadu_epi16(ptr_a##M); \
	__m512i zmm = _mm512_cvtepu16_epi32(ymm); \
	_mm512_storeu_epi16(tail_a + 16 * M, zmm); \
	_tile_loadd(T_A##M, tail_a + 16 * M, 2 * 2); \
}
#define MASK_LOAD_A_TAIL(M, N) {\
	__m256i ymm = _mm256_maskz_loadu_epi16(amask, ptr_a##M); \
	__m512i zmm = _mm512_cvtepu16_epi32(ymm); \
	_mm512_storeu_epi16(tail_a + 16 * M, zmm); \
	_tile_loadd(T_A##M, tail_a + 16 * M, 2 * 2); \
}
#define LOAD_B(M, N) _tile_loadd(T_B##N, ptr_b##N, ldb * 2)
#define LOAD_B_TAIL(M, N) {\
	__m256i ymm = _mm256_loadu_epi16(ptr_b##N); \
	__m512i zmm = _mm512_cvtepu16_epi32(ymm); \
	_mm512_storeu_epi16(tail_b + 16 * N, zmm); \
	_tile_loadd(T_B##N, tail_b + 16 * N, 2 * 2); \
}
#define MASK_LOAD_B_TAIL(M, N) {\
	__m256i ymm = _mm256_maskz_loadu_epi16(bmask, ptr_b##N); \
	__m512i zmm = _mm512_cvtepu16_epi32(ymm); \
	_mm512_storeu_epi16(tail_b + 16 * N, zmm); \
	_tile_loadd(T_B##N, tail_b + 16 * N, 2 * 2); \
}

#define MATMUL(M, N) _tile_dpbf16ps(T_C##M##N, T_A##M, T_B##N)
#define MATMUL_TAIL(M, N) _tile_dpbf16ps(T_C00, T_A##M, T_B##N)
#define STORE_C(M, N) _tile_stored(T_C##M##N, ptr_c##M##N, ldc * 4)
#define LOAD_C_F(M, N) _tile_loadd(T_C##M##N, ptr_c##M##N, ldc * 4)

#endif  // end of SBGEMM_KERNEL_SPR

#ifdef ALPHA_ONE
#undef LOAD_C
#define LOAD_C(M, N) _tile_loadd(T_C##M##N, ptr_c##M##N, ldc * 4)
#else
#undef LOAD_C
#define LOAD_C(M, N) _tile_zero(T_C##M##N)
#define ALPHA_STORE(N) \
	__m512 zmm_d##N = _mm512_loadu_ps(dst##N + noffset); \
	__m512 zmm_s##N = _mm512_loadu_ps(src##N + noffset); \
	zmm_d##N = _mm512_fmadd_ps(alpha_512, zmm_s##N, zmm_d##N); \
	_mm512_storeu_ps(dst##N + noffset, zmm_d##N);
#define MASK_APLPHA_STORE(N) \
	__m512 zmm_d##N = _mm512_maskz_loadu_ps(mask, dst##N + noffset); \
	__m512 zmm_s##N = _mm512_maskz_loadu_ps(mask, src##N + noffset); \
	zmm_d##N = _mm512_fmadd_ps(alpha_512, zmm_s##N, zmm_d##N); \
	_mm512_mask_storeu_ps(dst##N + noffset, mask, zmm_d##N);
#endif // end of ALPHA_ONE


#ifdef ALPHA_ONE
int sbgemm_kernel_spr_alpha_one(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, IFLOAT * A, IFLOAT * B, FLOAT * C, BLASLONG ldc)
#else
int sbgemm_kernel_spr_alpha(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, IFLOAT * A, IFLOAT * B, FLOAT * C, BLASLONG ldc)
#endif
{
	/* Row Major matrix for AMX requirement */
	IFLOAT *ptr_a = A, *ptr_b = B;
	IFLOAT *ptr_b0, *ptr_b1;
	IFLOAT *ptr_a0, *ptr_a1;
	FLOAT *ptr_c = C;
	FLOAT *ptr_c00, *ptr_c01, *ptr_c10, *ptr_c11;

	BLASLONG lda, ldb;
	BLASLONG m_count = m;
	BLASLONG n_count, k_count;

#ifndef ALPHA_ONE
	// make sure each row is 64 bytes aligned
	BLASLONG cn = (n & 31) ? (n & ~31) + 32 : n;
	FLOAT *raw_tmp_c;
	if (k < 32) {
		// only need to zero buff in this situation
		raw_tmp_c = (FLOAT *)calloc(1, sizeof(FLOAT) * m * cn + 64);
	} else {
		raw_tmp_c = (FLOAT *)malloc(sizeof(FLOAT) * m * cn + 64);
	}
	// align buf to 64 byte boundary
	FLOAT *tmp_c = (FLOAT *)(((uintptr_t) raw_tmp_c + 63) & ~(uintptr_t)63);
	ptr_c = tmp_c;
	BLASLONG ldc_o = ldc;
	ldc = cn;
#endif
	IFLOAT tail_a[32 * 2] __attribute__ ((aligned (64)));
	IFLOAT tail_b[32 * 2] __attribute__ ((aligned (64)));
	tilecfg cfg;

	if (k > 31) {
		for (; m_count > 31; m_count -= 32) {
			ptr_b = B;

			ptr_c00 = ptr_c;
			ptr_c01 = ptr_c00 + 16;
			ptr_c10 = ptr_c + 16 * ldc;
			ptr_c11 = ptr_c10 + 16;
			ptr_c += 32 * ldc;
			n_count = n;
			TCONF(cfg, 16, 16, 32);
			for (; n_count > 31; n_count -= 32) {
				ptr_a0 = ptr_a;
				ptr_a1 = ptr_a + 16 * k;

				ptr_b0 = ptr_b;
				ptr_b1 = ptr_b + 16 * k;
				ptr_b += 32 * k;

				lda = 32;
				ldb = 32;
				LOAD_C(0, 0); LOAD_C(0, 1);
				LOAD_C(1, 0); LOAD_C(1, 1);
				k_count = k;
				for (; k_count > 31; k_count -= 32) {
					LOAD_A(0, x); LOAD_A(1, x);
					ptr_a0 += 16 * 32;
					ptr_a1 += 16 * 32;
					LOAD_B(x, 0); LOAD_B(x, 1);
					ptr_b0 += 16 * 32;
					ptr_b1 += 16 * 32;

					MATMUL(0, 0); MATMUL(0, 1);
					MATMUL(1, 0); MATMUL(1, 1);
				}
				STORE_C(0, 0); STORE_C(0, 1);
				STORE_C(1, 0); STORE_C(1, 1);
				ptr_c00 += 32;
				ptr_c01 += 32;
				ptr_c10 += 32;
				ptr_c11 += 32;
			}
			for (; n_count > 0; n_count -= 16) {
				int tail_n = (n_count > 16) ? 16: n_count;
				ptr_a0 = ptr_a;
				ptr_a1 = ptr_a + 16 * k;

				ptr_b0 = ptr_b;
				ptr_b += tail_n * k;

				lda = 32;
				ldb = 2 * tail_n;
				TCONF(cfg, 16, tail_n, 32);
				LOAD_C(0, 0);
				LOAD_C(1, 0);
				k_count = k;
				for (; k_count > 31; k_count -= 32) {
					LOAD_A(0, x); LOAD_A(1, x);
					ptr_a0 += 16 * 32;
					ptr_a1 += 16 * 32;
					LOAD_B(x, 0);
					ptr_b0 += tail_n * 32;

					MATMUL(0, 0);
					MATMUL(1, 0);
				}
				STORE_C(0, 0);
				STORE_C(1, 0);
				ptr_c00 += tail_n;
				ptr_c10 += tail_n;
			}
			ptr_a += 32 * k;
		}
		for (; m_count > 0; m_count -= 16) {
			// process at most 16 m at a time
			int tail_m = (m_count > 16) ? 16: m_count;

			ptr_b = B;

			ptr_c00 = ptr_c;
			ptr_c01 = ptr_c00 + 16;
			ptr_c += tail_m * ldc;
			n_count = n;
			TCONF(cfg, tail_m, 16, 32);
			for (; n_count > 31; n_count -= 32) {
				ptr_a0 = ptr_a;

				ptr_b0 = ptr_b;
				ptr_b1 = ptr_b + 16 * k;
				ptr_b += 32 * k;

				lda = 32;
				ldb = 32;
				LOAD_C(0, 0); LOAD_C(0, 1);
				k_count = k;
				for (; k_count > 31; k_count -= 32) {
					LOAD_A(0, x);
					ptr_a0 += tail_m * 32;
					LOAD_B(x, 0); LOAD_B(x, 1);
					ptr_b0 += 16 * 32;
					ptr_b1 += 16 * 32;

					MATMUL(0, 0); MATMUL(0, 1);
				}
				STORE_C(0, 0); STORE_C(0, 1);
				ptr_c00 += 32;
				ptr_c01 += 32;
			}
			for (; n_count > 0; n_count -= 16) {
				int tail_n = (n_count > 16) ? 16: n_count;
				ptr_a0 = ptr_a;

				ptr_b0 = ptr_b;
				ptr_b += tail_n * k;

				lda = 32;
				ldb = 2 * tail_n;
				TCONF(cfg, tail_m, tail_n, 32);
				LOAD_C(0, 0);
				k_count = k;
				for (; k_count > 31; k_count -= 32) {
					LOAD_A(0, x);
					ptr_a0 += tail_m * 32;
					LOAD_B(x, 0);
					ptr_b0 += tail_n * 32;

					MATMUL(0, 0);
				}
				STORE_C(0, 0);
				ptr_c00 += tail_n;
			}
			ptr_a += tail_m * k;
		}
	}

	// process for k < 32
	BLASLONG k32 = k & ~31;
	BLASLONG k2 = k & ~1;
	if (k32 != k) {
		int remain_k2 = k2 - k32;
		m_count = m;
		ptr_a = A;
#ifndef ALPHA_ONE
		ptr_c = tmp_c;
#else
		ptr_c = C;
#endif
		if (remain_k2 > 0 && k2 != k) { // k%32 = 2x + 1 (x != 0)
			for (; m_count > 0; m_count -= 16) {
				int tail_m = (m_count > 16) ? 16: m_count;
				__mmask16 amask = (1UL << tail_m) - 1;

				ptr_a0 = ptr_a + tail_m * k32;
				ptr_a1 = ptr_a + tail_m * k2;
				ptr_a += tail_m * k;
				ptr_b = B;
				ptr_c00 = ptr_c;
				ptr_c += tail_m * ldc;
				n_count = n;
				lda = remain_k2;
				ldb = 32;
				if (n_count > 15) {
					TCONF_TAIL(cfg, tail_m, 16, remain_k2);
					LOAD_A(0, x); MASK_LOAD_A_TAIL(1, x);
					for (; n_count > 15; n_count -= 16) {
						ptr_b0 = ptr_b + 16 * k32;
						ptr_b1 = ptr_b + 16 * k2;
						LOAD_C_F(0, 0);
						LOAD_B(x, 0); LOAD_B_TAIL(x, 1);
						MATMUL(0, 0); MATMUL_TAIL(1, 1);
						STORE_C(0, 0);
						ptr_b += 16 * k;
						ptr_c00 += 16;
					}
				}
				if (n_count > 0) {
					int tail_n = (n_count > 16) ? 16: n_count;
					__mmask16 bmask = (1UL << tail_n) - 1;
					ptr_b0 =  ptr_b + tail_n * k32;
					ptr_b1 =  ptr_b + tail_n * k2;
					ldb = 2 * tail_n;
					TCONF_TAIL(cfg, tail_m, tail_n, remain_k2);
					LOAD_C_F(0, 0);
					LOAD_A(0, x); MASK_LOAD_A_TAIL(1, x);
					LOAD_B(x, 0); MASK_LOAD_B_TAIL(x, 1);
					MATMUL(0, 0); MATMUL_TAIL(1, 1);
					STORE_C(0, 0);
				}
			}

		} else if (remain_k2 > 0) { // k%32 = 2x
			for (; m_count > 0; m_count -= 16) {
				int tail_m = (m_count > 16) ? 16: m_count;

				ptr_a0 = ptr_a + tail_m * k32;
				ptr_a += tail_m * k;
				ptr_b = B;
				ptr_c00 = ptr_c;
				ptr_c += tail_m * ldc;
				n_count = n;
				lda = remain_k2;
				ldb = 32;
				if (n_count > 15) {
					TCONF(cfg, tail_m, 16, remain_k2);
					LOAD_A(0, x);
					for (; n_count > 15; n_count -= 16) {
						ptr_b0 = ptr_b + 16 * k32;
						LOAD_C_F(0, 0);
						LOAD_B(x, 0);
						MATMUL(0, 0);
						STORE_C(0, 0);
						ptr_b += 16 * k;
						ptr_c00 += 16;
					}
				}
				if (n_count > 0) {
					int tail_n = (n_count > 16) ? 16: n_count;
					ptr_b0 =  ptr_b + tail_n * k32;
					ldb = 2 * tail_n;
					TCONF(cfg, tail_m, tail_n, remain_k2);
					LOAD_C_F(0, 0);
					LOAD_A(0, x);
					LOAD_B(x, 0);
					MATMUL(0, 0);
					STORE_C(0, 0);
				}
			}
		} else { // k%32 = 1
			for (; m_count > 0; m_count -= 16) {
				int tail_m = (m_count > 16) ? 16: m_count;
				__mmask16 amask = (1UL << tail_m) - 1;

				ptr_a0 = ptr_a + tail_m * k2;
				ptr_a += tail_m * k;
				ptr_b = B;
				ptr_c00 = ptr_c;
				ptr_c += tail_m * ldc;
				n_count = n;
				if (n_count > 15) {
					TCONF(cfg, tail_m, 16, 2);
					MASK_LOAD_A_TAIL(0, x);
					for (; n_count > 15; n_count -= 16) {
						ptr_b0 = ptr_b + 16 * k2;
						LOAD_C_F(0, 0);
						LOAD_B_TAIL(x, 0);
						MATMUL(0, 0);
						STORE_C(0, 0);
						ptr_b += 16 * k;
						ptr_c00 += 16;
					}
				}
				if (n_count > 0) {
					int tail_n = (n_count > 16) ? 16: n_count;
					__mmask16 bmask = (1UL << tail_n) - 1;
					ptr_b0 =  ptr_b + tail_n * k2;
					TCONF(cfg, tail_m, tail_n, 2);
					LOAD_C_F(0, 0);
					MASK_LOAD_A_TAIL(0, x);
					MASK_LOAD_B_TAIL(x, 0);
					MATMUL(0, 0);
					STORE_C(0, 0);
				}
			}

		}
	}
#ifndef ALPHA_ONE
	__m512 alpha_512 = _mm512_broadcastss_ps(_mm_load_ss(&alpha));
	BLASLONG n16 = n & ~15;
	BLASLONG noffset;
	FLOAT *src0, *src1, *src2, *src3;
	FLOAT *dst0, *dst1, *dst2, *dst3;
	FLOAT *src = tmp_c;
	FLOAT *dst = C;
	m_count = m;
	for (; m_count > 3; m_count -= 4) {
		src0 = src;
		src1 = src0 + ldc;
		src2 = src1 + ldc;
		src3 = src2 + ldc;
		src += 4 * ldc;

		dst0 = dst;
		dst1 = dst0 + ldc_o;
		dst2 = dst1 + ldc_o;
		dst3 = dst2 + ldc_o;
		dst += 4 * ldc_o;

		noffset = 0;
		for (; noffset < n16; noffset += 16) {
			ALPHA_STORE(0);
			ALPHA_STORE(1);
			ALPHA_STORE(2);
			ALPHA_STORE(3);
		}
		if (noffset < n) {
			__mmask16 mask = (1UL << (n - noffset)) - 1;
			MASK_APLPHA_STORE(0);
			MASK_APLPHA_STORE(1);
			MASK_APLPHA_STORE(2);
			MASK_APLPHA_STORE(3);
		}
	}
	for (; m_count > 1; m_count -= 2) {
		src0 = src;
		src1 = src0 + ldc;
		src += 2 * ldc;

		dst0 = dst;
		dst1 = dst0 + ldc_o;
		dst += 2 * ldc_o;

		noffset = 0;
		for (; noffset < n16; noffset += 16) {
			ALPHA_STORE(0);
			ALPHA_STORE(1);
		}
		if (noffset < n) {
			__mmask16 mask = (1UL << (n - noffset)) - 1;
			MASK_APLPHA_STORE(0);
			MASK_APLPHA_STORE(1);
		}
	}
	for (; m_count > 0; m_count -= 1) {
		src0 = src;
		dst0 = dst;
		noffset = 0;
		for (; noffset < n16; noffset += 16) {
			ALPHA_STORE(0);
		}
		if (noffset < n) {
			__mmask16 mask = (1UL << (n - noffset)) - 1;
			MASK_APLPHA_STORE(0);
		}
	}
	free(raw_tmp_c);
#endif
	return 0;
}
