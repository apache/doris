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

#include <arm_sve.h>

#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b) {
  IFLOAT *a_offset;
  IFLOAT *a_offsetx[4];
  IFLOAT *b_offset;
  a_offset = a;
  b_offset = b;

  bfloat16_t zero_value_bf16;
  *((uint16_t *)(&zero_value_bf16)) = 0;

  svbool_t pg16_all = svptrue_b16(); // 16 elements for sve-256 machine.
  svbool_t pg16_first_8 = svwhilelt_b16(0, 8);

  svbfloat16_t v0, v1, v2, v3;
  svuint64_t t0, t1;

  BLASLONG rest = m & 7;
  svbool_t pg16_rest = svwhilelt_b16_s32(0, rest);

  for (BLASLONG j = 0; j < n / 4; j++) {
    a_offsetx[0] = a_offset;
    a_offsetx[1] = a_offsetx[0] + lda;
    a_offsetx[2] = a_offsetx[1] + lda;
    a_offsetx[3] = a_offsetx[2] + lda;
    a_offset += 4 * lda;

    for (BLASLONG i = 0; i < m / 8; i++) {
      v0 = svld1_bf16(pg16_first_8, (bfloat16_t *)a_offsetx[0]);
      v1 = svld1_bf16(pg16_first_8, (bfloat16_t *)a_offsetx[1]);
      v2 = svld1_bf16(pg16_first_8, (bfloat16_t *)a_offsetx[2]);
      v3 = svld1_bf16(pg16_first_8, (bfloat16_t *)a_offsetx[3]);

      t0 = svzip1_u64(svreinterpret_u64_bf16(v0), svreinterpret_u64_bf16(v1));
      t1 = svzip1_u64(svreinterpret_u64_bf16(v2), svreinterpret_u64_bf16(v3));

      svst1_bf16(pg16_all, (bfloat16_t *)b_offset, svreinterpret_bf16_u64(t0));
      svst1_bf16(pg16_all, (bfloat16_t *)b_offset + 16,
                 svreinterpret_bf16_u64(t1));

      a_offsetx[0] += 8;
      a_offsetx[1] += 8;
      a_offsetx[2] += 8;
      a_offsetx[3] += 8;

      b_offset += 32;
    }

    if (rest) { // remainder along k dim
      v0 = svld1_bf16(pg16_rest, (bfloat16_t *)a_offsetx[0]);
      v1 = svld1_bf16(pg16_rest, (bfloat16_t *)a_offsetx[1]);
      v2 = svld1_bf16(pg16_rest, (bfloat16_t *)a_offsetx[2]);
      v3 = svld1_bf16(pg16_rest, (bfloat16_t *)a_offsetx[3]);

      t0 = svzip1_u64(svreinterpret_u64_bf16(v0), svreinterpret_u64_bf16(v1));
      t1 = svzip1_u64(svreinterpret_u64_bf16(v2), svreinterpret_u64_bf16(v3));

      svst1_bf16(pg16_all, (bfloat16_t *)b_offset, svreinterpret_bf16_u64(t0));
      svst1_bf16(pg16_all, (bfloat16_t *)b_offset + 16,
                 svreinterpret_bf16_u64(t1));

      b_offset += 32;
    }
  }

  if (n & 2) {
    a_offsetx[0] = a_offset;
    a_offsetx[1] = a_offsetx[0] + lda;
    a_offset += 2 * lda;

    for (BLASLONG i = 0; i < m / 8; i++) {
      v0 = svld1_bf16(pg16_first_8, (bfloat16_t *)a_offsetx[0]);
      v1 = svld1_bf16(pg16_first_8, (bfloat16_t *)a_offsetx[1]);

      t0 = svzip1_u64(svreinterpret_u64_bf16(v0), svreinterpret_u64_bf16(v1));
      svst1_bf16(pg16_all, (bfloat16_t *)b_offset, svreinterpret_bf16_u64(t0));

      b_offset += 16;
      a_offsetx[0] += 8;
      a_offsetx[1] += 8;
    }

    if (rest) { // remainder along k dim
      v0 = svld1_bf16(pg16_rest, (bfloat16_t *)a_offsetx[0]);
      v1 = svld1_bf16(pg16_rest, (bfloat16_t *)a_offsetx[1]);

      t0 = svzip1_u64(svreinterpret_u64_bf16(v0), svreinterpret_u64_bf16(v1));
      svst1_bf16(pg16_all, (bfloat16_t *)b_offset, svreinterpret_bf16_u64(t0));

      b_offset += 16;
    }
  }

  if (n & 1) {
    a_offsetx[0] = a_offset;

    for (BLASLONG i = 0; i < m / 8; i++) {
      v0 = svld1_bf16(pg16_first_8, (bfloat16_t *)a_offsetx[0]);
      v1 = svdup_bf16(zero_value_bf16);

      t0 = svzip1_u64(svreinterpret_u64_bf16(v0), svreinterpret_u64_bf16(v1));
      svst1_bf16(pg16_all, (bfloat16_t *)b_offset, svreinterpret_bf16_u64(t0));

      b_offset += 16;
      a_offsetx[0] += 8;
    }

    if (rest) { // remainder along k dim
      v0 = svld1_bf16(pg16_rest, (bfloat16_t *)a_offsetx[0]);
      v1 = svdup_bf16(zero_value_bf16);
      t0 = svzip1_u64(svreinterpret_u64_bf16(v0), svreinterpret_u64_bf16(v1));
      svst1_bf16(pg16_all, (bfloat16_t *)b_offset, svreinterpret_bf16_u64(t0));
    }
  }

  return 0;
}
