/***************************************************************************
 * Copyright (c) 2024-2025, The OpenBLAS Project
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
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * *****************************************************************************/
#include "common.h"
#include <arm_neon.h>
#include <arm_sve.h>

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b) {
  BLASLONG pad_m = ((m + 7) & ~7);
  BLASLONG rest = (m & 7); // rest along m dim

  IFLOAT *a_offset;
  IFLOAT *a_offset0, *a_offset1, *a_offset2, *a_offset3;
  IFLOAT *a_offset4, *a_offset5, *a_offset6, *a_offset7;

  IFLOAT *b_offset;
  IFLOAT *b_offset0, *b_offset1;

  a_offset = a;
  b_offset = b;

  svuint16_t c0, c1, c2, c3, c4, c5, c6, c7;
  svuint16_t t0, t1, t2, t3;
  svuint32_t m00, m01, m10, m11;
  svuint64_t st_offsets_0, st_offsets_1;

  svbool_t pg16_first_4 = svwhilelt_b16(0, 4);
  svbool_t pg16_first_8 = svwhilelt_b16(0, 8);

  svbool_t pg64_first_4 = svwhilelt_b64(0, 4);
  
  u_int32_t sizeof_u64 = 8;
  u_int64_t _st_offsets_0[4] = {
      0 * sizeof_u64,
      1 * sizeof_u64,
      4 * sizeof_u64,
      5 * sizeof_u64,
  };

  u_int64_t _st_offsets_1[4] = {
      2 * sizeof_u64,
      3 * sizeof_u64,
      6 * sizeof_u64,
      7 * sizeof_u64,
  };

  st_offsets_0 = svld1_u64(pg64_first_4, _st_offsets_0);
  st_offsets_1 = svld1_u64(pg64_first_4, _st_offsets_1);

  for (BLASLONG j = 0; j < n / 8; j++) {
    a_offset0 = a_offset;
    a_offset1 = a_offset0 + lda;
    a_offset2 = a_offset1 + lda;
    a_offset3 = a_offset2 + lda;
    a_offset4 = a_offset3 + lda;
    a_offset5 = a_offset4 + lda;
    a_offset6 = a_offset5 + lda;
    a_offset7 = a_offset6 + lda;
    a_offset += 8;

    b_offset0 = b_offset;
    b_offset1 = b_offset0 + 4 * pad_m;

    b_offset += 8 * pad_m;
    for (BLASLONG i = 0; i < m / 8; i++) {
      // transpose 8x8 matrix and pack into two 4x8 block consists of two 2x4
      // small blocks
      c0 = svld1_u16(pg16_first_8, a_offset0);
      c1 = svld1_u16(pg16_first_8, a_offset1);
      c2 = svld1_u16(pg16_first_8, a_offset2);
      c3 = svld1_u16(pg16_first_8, a_offset3);
      c4 = svld1_u16(pg16_first_8, a_offset4);
      c5 = svld1_u16(pg16_first_8, a_offset5);
      c6 = svld1_u16(pg16_first_8, a_offset6);
      c7 = svld1_u16(pg16_first_8, a_offset7);

      t0 = svzip1_u16(c0, c1);
      t1 = svzip1_u16(c2, c3);
      t2 = svzip1_u16(c4, c5);
      t3 = svzip1_u16(c6, c7);

      m00 = svzip1_u32(svreinterpret_u32_u16(t0), svreinterpret_u32_u16(t1));
      m10 = svzip2_u32(svreinterpret_u32_u16(t0), svreinterpret_u32_u16(t1));
      m01 = svzip1_u32(svreinterpret_u32_u16(t2), svreinterpret_u32_u16(t3));
      m11 = svzip2_u32(svreinterpret_u32_u16(t2), svreinterpret_u32_u16(t3));

      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_0, svreinterpret_u64_u32(m00));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_1, svreinterpret_u64_u32(m01));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset1,
                                  st_offsets_0, svreinterpret_u64_u32(m10));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset1,
                                  st_offsets_1, svreinterpret_u64_u32(m11));

      a_offset0 += 8 * lda;
      a_offset1 += 8 * lda;
      a_offset2 += 8 * lda;
      a_offset3 += 8 * lda;
      a_offset4 += 8 * lda;
      a_offset5 += 8 * lda;
      a_offset6 += 8 * lda;
      a_offset7 += 8 * lda;

      b_offset0 += 32;
      b_offset1 += 32;
    }

    if (rest) {
      c0 = svld1_u16(pg16_first_8, a_offset0);
      c1 = (rest >= 2 ? svld1_u16(pg16_first_8, a_offset1) : svdup_u16(0));
      c2 = (rest >= 3 ? svld1_u16(pg16_first_8, a_offset2) : svdup_u16(0));
      c3 = (rest >= 4 ? svld1_u16(pg16_first_8, a_offset3) : svdup_u16(0));
      c4 = (rest >= 5 ? svld1_u16(pg16_first_8, a_offset4) : svdup_u16(0));
      c5 = (rest >= 6 ? svld1_u16(pg16_first_8, a_offset5) : svdup_u16(0));
      c6 = (rest == 7 ? svld1_u16(pg16_first_8, a_offset6) : svdup_u16(0));
      c7 = (svdup_u16(0));

      t0 = svzip1_u16(c0, c1);
      t1 = svzip1_u16(c2, c3);
      t2 = svzip1_u16(c4, c5);
      t3 = svzip1_u16(c6, c7);

      m00 = svzip1_u32(svreinterpret_u32_u16(t0), svreinterpret_u32_u16(t1));
      m10 = svzip2_u32(svreinterpret_u32_u16(t0), svreinterpret_u32_u16(t1));
      m01 = svzip1_u32(svreinterpret_u32_u16(t2), svreinterpret_u32_u16(t3));
      m11 = svzip2_u32(svreinterpret_u32_u16(t2), svreinterpret_u32_u16(t3));

      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_0, svreinterpret_u64_u32(m00));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_1, svreinterpret_u64_u32(m01));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset1,
                                  st_offsets_0, svreinterpret_u64_u32(m10));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset1,
                                  st_offsets_1, svreinterpret_u64_u32(m11));
    }
  }

  if (n & 4) {
    a_offset0 = a_offset;
    a_offset1 = a_offset0 + lda;
    a_offset2 = a_offset1 + lda;
    a_offset3 = a_offset2 + lda;
    a_offset4 = a_offset3 + lda;
    a_offset5 = a_offset4 + lda;
    a_offset6 = a_offset5 + lda;
    a_offset7 = a_offset6 + lda;
    a_offset += 4;

    b_offset0 = b_offset;
    b_offset += 4 * pad_m;

    for (BLASLONG i = 0; i < m / 8; i++) {
      // transpose 8x8 matrix and pack into two 4x8 block consists of two 2x4
      // small blocks
      c0 = svld1_u16(pg16_first_4, a_offset0);
      c1 = svld1_u16(pg16_first_4, a_offset1);
      c2 = svld1_u16(pg16_first_4, a_offset2);
      c3 = svld1_u16(pg16_first_4, a_offset3);
      c4 = svld1_u16(pg16_first_4, a_offset4);
      c5 = svld1_u16(pg16_first_4, a_offset5);
      c6 = svld1_u16(pg16_first_4, a_offset6);
      c7 = svld1_u16(pg16_first_4, a_offset7);

      t0 = svzip1_u16(c0, c1);
      t1 = svzip1_u16(c2, c3);
      t2 = svzip1_u16(c4, c5);
      t3 = svzip1_u16(c6, c7);

      m00 = svzip1_u32(svreinterpret_u32_u16(t0), svreinterpret_u32_u16(t1));
      m01 = svzip1_u32(svreinterpret_u32_u16(t2), svreinterpret_u32_u16(t3));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_0, svreinterpret_u64_u32(m00));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_1, svreinterpret_u64_u32(m01));

      a_offset0 += 8 * lda;
      a_offset1 += 8 * lda;
      a_offset2 += 8 * lda;
      a_offset3 += 8 * lda;
      a_offset4 += 8 * lda;
      a_offset5 += 8 * lda;
      a_offset6 += 8 * lda;
      a_offset7 += 8 * lda;

      b_offset0 += 32;
    }

    if (rest) {
      c0 = svld1_u16(pg16_first_4, a_offset0); // rest >= 1
      c1 = (rest >= 2 ? svld1_u16(pg16_first_4, a_offset1) : svdup_u16(0));
      c2 = (rest >= 3 ? svld1_u16(pg16_first_4, a_offset2) : svdup_u16(0));
      c3 = (rest >= 4 ? svld1_u16(pg16_first_4, a_offset3) : svdup_u16(0));
      c4 = (rest >= 5 ? svld1_u16(pg16_first_4, a_offset4) : svdup_u16(0));
      c5 = (rest >= 6 ? svld1_u16(pg16_first_4, a_offset5) : svdup_u16(0));
      c6 = (rest == 7 ? svld1_u16(pg16_first_4, a_offset6) : svdup_u16(0));
      c7 = (svdup_u16(0));

      t0 = svzip1_u16(c0, c1);
      t1 = svzip1_u16(c2, c3);
      t2 = svzip1_u16(c4, c5);
      t3 = svzip1_u16(c6, c7);

      m00 = svzip1_u32(svreinterpret_u32_u16(t0), svreinterpret_u32_u16(t1));
      m01 = svzip1_u32(svreinterpret_u32_u16(t2), svreinterpret_u32_u16(t3));

      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_0, svreinterpret_u64_u32(m00));
      svst1_scatter_u64offset_u64(pg64_first_4, (u_int64_t *)b_offset0,
                                  st_offsets_1, svreinterpret_u64_u32(m01));
    }
  }

  if (n & 2) {
    a_offset0 = a_offset;
    a_offset1 = a_offset0 + lda;
    a_offset2 = a_offset1 + lda;
    a_offset3 = a_offset2 + lda;
    a_offset4 = a_offset3 + lda;
    a_offset5 = a_offset4 + lda;
    a_offset6 = a_offset5 + lda;
    a_offset7 = a_offset6 + lda;
    a_offset += 2;

    b_offset0 = b_offset;
    b_offset1 = b_offset0 + 8;

    b_offset += 2 * pad_m;

    for (BLASLONG i = 0; i < m / 8; i++) {
      for (BLASLONG line = 0; line < 2; line++) {
        b_offset0[line * 4] = a_offset0[line];
        b_offset0[line * 4 + 1] = a_offset1[line];
        b_offset0[line * 4 + 2] = a_offset2[line];
        b_offset0[line * 4 + 3] = a_offset3[line];

        b_offset1[line * 4] = a_offset4[line];
        b_offset1[line * 4 + 1] = a_offset5[line];
        b_offset1[line * 4 + 2] = a_offset6[line];
        b_offset1[line * 4 + 3] = a_offset7[line];
      }
      b_offset0 += 16;
      b_offset1 += 16;

      a_offset0 += 8 * lda;
      a_offset1 += 8 * lda;
      a_offset2 += 8 * lda;
      a_offset3 += 8 * lda;
      a_offset4 += 8 * lda;
      a_offset5 += 8 * lda;
      a_offset6 += 8 * lda;
      a_offset7 += 8 * lda;
    }

    if (rest) {
      for (BLASLONG line = 0; line < 2; line++) {
        b_offset0[line * 4] = a_offset0[line];
        b_offset0[line * 4 + 1] = rest == 1 ? 0 : a_offset1[line];
        b_offset0[line * 4 + 2] = rest <= 2 ? 0 : a_offset2[line];
        b_offset0[line * 4 + 3] = rest <= 3 ? 0 : a_offset3[line];

        b_offset1[line * 4] = rest <= 4 ? 0 : a_offset4[line];
        b_offset1[line * 4 + 1] = rest <= 5 ? 0 : a_offset5[line];
        b_offset1[line * 4 + 2] = rest <= 6 ? 0 : a_offset6[line];
        b_offset1[line * 4 + 3] = 0;
      }
    }
  }

  if (n & 1) {
    a_offset0 = a_offset;
    a_offset1 = a_offset0 + lda;
    a_offset2 = a_offset1 + lda;
    a_offset3 = a_offset2 + lda;
    a_offset4 = a_offset3 + lda;
    a_offset5 = a_offset4 + lda;
    a_offset6 = a_offset5 + lda;
    a_offset7 = a_offset6 + lda;

    for (BLASLONG i = 0; i < m / 8; i++) {
      b_offset[0] = a_offset0[0];
      b_offset[1] = a_offset1[0];
      b_offset[2] = a_offset2[0];
      b_offset[3] = a_offset3[0];

      b_offset[4] = 0;
      b_offset[5] = 0;
      b_offset[6] = 0;
      b_offset[7] = 0;

      b_offset[8] = a_offset4[0];
      b_offset[9] = a_offset5[0];
      b_offset[10] = a_offset6[0];
      b_offset[11] = a_offset7[0];

      b_offset[12] = 0;
      b_offset[13] = 0;
      b_offset[14] = 0;
      b_offset[15] = 0;

      b_offset += 16;
      a_offset0 += 8 * lda;
      a_offset1 += 8 * lda;
      a_offset2 += 8 * lda;
      a_offset3 += 8 * lda;
      a_offset4 += 8 * lda;
      a_offset5 += 8 * lda;
      a_offset6 += 8 * lda;
      a_offset7 += 8 * lda;
    }

    if (rest) {
      b_offset[0] = *a_offset0;
      b_offset[1] = rest == 1 ? 0 : *a_offset1;
      b_offset[2] = rest <= 2 ? 0 : *a_offset2;
      b_offset[3] = rest <= 3 ? 0 : *a_offset3;

      b_offset[4] = 0;
      b_offset[5] = 0;
      b_offset[6] = 0;
      b_offset[7] = 0;

      b_offset[8] = rest <= 4 ? 0 : *a_offset4;
      b_offset[9] = rest <= 5 ? 0 : *a_offset5;
      b_offset[10] = rest <= 6 ? 0 : *a_offset6;
      b_offset[11] = 0;

      b_offset[12] = 0;
      b_offset[13] = 0;
      b_offset[14] = 0;
      b_offset[15] = 0;
    }
  }

  return 0;
}
