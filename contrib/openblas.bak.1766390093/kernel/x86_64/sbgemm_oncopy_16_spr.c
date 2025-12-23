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

typedef struct {
	char palette_id;
	char start_row;
	char dummy0[14];  // bytes 2-15 reserved, must be zero
	short tile_colsb[8];
	char dummy1[16];  // bytes 32-47 reserved, must be zero
	char tile_rows[8];
	char dummy2[16];  // bytes 56-63 reserved, must be zero
} tilecfg;

#define T_16x32 0
#define T_16xm  1
#define T_nx32  2
#define T_nxm   3

#define TCONF(cfg, m, n) \
	memset(&cfg, 0, sizeof(tilecfg)); \
	cfg.palette_id = 1; \
	cfg.tile_rows[T_16x32] = 16; \
	cfg.tile_colsb[T_16x32] = 64; \
	if (m) { \
		cfg.tile_rows[T_16xm] = 16; \
		cfg.tile_colsb[T_16xm] = m * 2; \
	} \
	if (n) { \
		cfg.tile_rows[T_nx32] = n; \
		cfg.tile_colsb[T_nx32] = 64; \
	} \
	if (m && n) { \
		cfg.tile_rows[T_nxm] = n; \
		cfg.tile_colsb[T_nxm] = m * 2; \
	} \
	_tile_loadconfig(&cfg);


int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b) {
	BLASLONG i, j;
	IFLOAT *aoffset, *boffset;
	IFLOAT *aoffset0;

	aoffset = a;
	boffset = b;

	BLASLONG n16 = n & ~15;
	BLASLONG m32 = m & ~31;
	BLASLONG m2 = m & ~1;

	BLASLONG tail_m = m2 - m32;
	BLASLONG tail_n = n - n16;
	tilecfg cfg;
	TCONF(cfg, tail_m, tail_n);

	for (j = 0; j < n16; j += 16) {
		aoffset0  = aoffset;
		for (i = 0; i < m32; i += 32) {
			_tile_loadd(T_16x32, aoffset0, lda * 2);
			_tile_stored(T_16x32, boffset, 32 * 2);
			aoffset0 += 32;
			boffset += 32 * 16;
		}
		if (i < m2) {
			_tile_loadd(T_16xm, aoffset0, lda * 2);
			_tile_stored(T_16xm, boffset, tail_m * 2);
			aoffset0 += tail_m;
			boffset += tail_m * 16;
			i = m2;
		}
		if (i < m) {
			/* the tail odd k should put alone */
			for (int ii = 0; ii < 16; ii++) {
				*(boffset + ii) = *(aoffset0 + lda * ii);
			}
			boffset += 16;
		}
		aoffset += 16 * lda;
	}
	if (j < n) {
		aoffset0  = aoffset;
		for (i = 0; i < m32; i += 32) {
			_tile_loadd(T_nx32, aoffset0, lda * 2);
			_tile_stored(T_nx32, boffset, 32 * 2);
			aoffset0 += 32;
			boffset += 32 * tail_n;
		}
		if (i < m2) {
			_tile_loadd(T_nxm, aoffset0, lda * 2);
			_tile_stored(T_nxm, boffset, tail_m * 2);
			aoffset0 += tail_m;
			boffset += tail_m * tail_n;
		}
		if (i < m) {
			for (int ii = 0; ii < tail_n; ii++) {
				*(boffset + ii) = *(aoffset0 + lda * ii);
			}
		}
	}
	return 0;
}
