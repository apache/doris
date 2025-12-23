/***************************************************************************
 * Copyright (c) 2022, The OpenBLAS Project
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

  svbool_t pg16 = svdupq_b16(1, 1, 1, 1, 0, 0, 0, 0);
  svbfloat16_t v0, v1, v2, v3;

  for (BLASLONG j = 0; j < n / 4; j++) {
    a_offsetx[0] = a_offset;
    a_offsetx[1] = a_offsetx[0] + lda;
    a_offsetx[2] = a_offsetx[1] + lda;
    a_offsetx[3] = a_offsetx[2] + lda;
    a_offset += 4 * lda;

    for (BLASLONG i = 0; i < m / 4; i++) {
      v0 = svld1_bf16(pg16, (bfloat16_t *)a_offsetx[0]);
      v1 = svld1_bf16(pg16, (bfloat16_t *)a_offsetx[1]);
      v2 = svld1_bf16(pg16, (bfloat16_t *)a_offsetx[2]);
      v3 = svld1_bf16(pg16, (bfloat16_t *)a_offsetx[3]);

      svst1_bf16(pg16, (bfloat16_t *)b_offset, v0);
      svst1_bf16(pg16, (bfloat16_t *)b_offset + 4, v1);
      svst1_bf16(pg16, (bfloat16_t *)b_offset + 8, v2);
      svst1_bf16(pg16, (bfloat16_t *)b_offset + 12, v3);

      b_offset += 16;
      a_offsetx[0] += 4;
      a_offsetx[1] += 4;
      a_offsetx[2] += 4;
      a_offsetx[3] += 4;
    }

    if (m & 3) {
      BLASLONG rest = m & 3;
      for (BLASLONG col = 0; col < 4; col++) {
        b_offset[4 * col] = a_offsetx[col][0];
        b_offset[4 * col + 1] = rest == 1 ? 0 : a_offsetx[col][1];
        b_offset[4 * col + 2] = rest <= 2 ? 0 : a_offsetx[col][2];
        b_offset[4 * col + 3] = rest <= 3 ? 0 : a_offsetx[col][3];
      }
      b_offset += 16;
    }
  }

  if (n & 2) {
    a_offsetx[0] = a_offset;
    a_offsetx[1] = a_offsetx[0] + lda;
    a_offset += 2 * lda;

    for (BLASLONG i = 0; i < m / 4; i++) {
      v0 = svld1_bf16(pg16, (bfloat16_t *)a_offsetx[0]);
      v1 = svld1_bf16(pg16, (bfloat16_t *)a_offsetx[1]);
      svst1_bf16(pg16, (bfloat16_t *)b_offset, v0);
      svst1_bf16(pg16, (bfloat16_t *)b_offset + 4, v1);

      b_offset += 8;
      a_offsetx[0] += 4;
      a_offsetx[1] += 4;
    }

    if (m & 3) {
      BLASLONG rest = m & 3;
      for (BLASLONG col = 0; col < 2; col++) {
        b_offset[4 * col] = a_offsetx[col][0];
        b_offset[4 * col + 1] = rest == 1 ? 0 : a_offsetx[col][1];
        b_offset[4 * col + 2] = rest <= 2 ? 0 : a_offsetx[col][2];
        b_offset[4 * col + 3] = rest <= 3 ? 0 : a_offsetx[col][3];
      }
      b_offset += 8;
    }
  }

  if (n & 1) {
    a_offsetx[0] = a_offset;
    for (BLASLONG i = 0; i < m / 4; i++) {
      v0 = svld1_bf16(pg16, (bfloat16_t *)a_offsetx[0]);
      svst1_bf16(pg16, (bfloat16_t *)b_offset, v0);
      b_offset += 4;
      a_offsetx[0] += 4;
    }
    if (m & 3) {
      BLASLONG rest = m & 3;
      b_offset[0] = a_offsetx[0][0];
      b_offset[1] = rest == 1 ? 0 : a_offsetx[0][1];
      b_offset[2] = rest <= 2 ? 0 : a_offsetx[0][2];
      b_offset[3] = rest <= 3 ? 0 : a_offsetx[0][3];
    }
  }

  return 0;
}
