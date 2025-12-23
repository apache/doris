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

#define INIT_C(M, N) mc##M##N = svdup_f32(0);

#define MATMUL(M, N) mc##M##N = svbfmmla(mc##M##N, ma##M, mb##N);

#define INIT_C_8x4 \
  do {             \
    INIT_C(0, 0);  \
    INIT_C(0, 1);  \
    INIT_C(1, 0);  \
    INIT_C(1, 1);  \
    INIT_C(2, 0);  \
    INIT_C(2, 1);  \
    INIT_C(3, 0);  \
    INIT_C(3, 1);  \
  } while (0);

#ifdef ALPHA_ONE
#define UPDATE_C(PG, PTR, DST, SRC) \
  do {                              \
    DST = svld1_f32((PG), (PTR));   \
    DST = svadd_z((PG), SRC, DST);  \
    svst1_f32((PG), (PTR), DST);    \
  } while (0);
#else
#define UPDATE_C(PG, PTR, DST, SRC)         \
  do {                                      \
    DST = svld1_f32((PG), (PTR));           \
    DST = svmad_z((PG), svalpha, SRC, DST); \
    svst1_f32((PG), (PTR), DST);            \
  } while (0);
#endif

#ifdef ALPHA_ONE
int sbgemm_kernel_neoversen2_alpha_one(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, IFLOAT * A, IFLOAT * B, FLOAT * C, BLASLONG ldc)
#else
int sbgemm_kernel_neoversen2_alpha(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, IFLOAT * A, IFLOAT * B, FLOAT * C, BLASLONG ldc)
#endif
{
  BLASLONG pad_k = (k + 3) & ~3;

  svbfloat16_t ma0, ma1, ma2, ma3, mb0, mb1;
  svfloat32_t mc00, mc01, mc10, mc11, mc20, mc21, mc30, mc31, 
              vc0, vc1, vc2, vc3, vc4, vc5, vc6, vc7, 
              oc0, oc1, oc2, oc3, oc4, oc5, oc6, oc7;
  svfloat32_t svalpha = svdup_f32(alpha);

  svbool_t pg16 = svptrue_b16();
  svbool_t pg16_low = svdupq_b16(1, 1, 1, 1, 0, 0, 0, 0);
  svbool_t pg32 = svptrue_b32();
  svbool_t pg32_low = svdupq_b32(1, 1, 0, 0);
  svbool_t pg32_first = svdupq_b32(1, 0, 0, 0);

  bfloat16_t *ptr_a = (bfloat16_t *)A;
  bfloat16_t *ptr_b = (bfloat16_t *)B;
  FLOAT *ptr_c = C;

  bfloat16_t *ptr_a0, *ptr_a1, *ptr_a2, *ptr_a3;
  bfloat16_t *ptr_b0, *ptr_b1;
  FLOAT *ptr_c0, *ptr_c1, *ptr_c2, *ptr_c3;

  for (BLASLONG j = 0; j < n / 4; j++) {
    ptr_c0 = ptr_c;
    ptr_c1 = ptr_c0 + ldc;
    ptr_c2 = ptr_c1 + ldc;
    ptr_c3 = ptr_c2 + ldc;
    ptr_c += 4 * ldc;
    ptr_a = (bfloat16_t *)A;

    for (BLASLONG i = 0; i < m / 8; i++) {
      ptr_a0 = ptr_a;
      ptr_a += 8 * pad_k;

      ptr_b0 = ptr_b;

      INIT_C_8x4;

      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        ma1 = svld1_bf16(pg16, ptr_a0 + 8);
        ma2 = svld1_bf16(pg16, ptr_a0 + 16);
        ma3 = svld1_bf16(pg16, ptr_a0 + 24);

        mb0 = svld1_bf16(pg16, ptr_b0);
        mb1 = svld1_bf16(pg16, ptr_b0 + 8);

        MATMUL(0, 0); MATMUL(0, 1);
        MATMUL(1, 0); MATMUL(1, 1);
        MATMUL(2, 0); MATMUL(2, 1);
        MATMUL(3, 0); MATMUL(3, 1);

        ptr_a0 += 32;
        ptr_b0 += 16;
      }

      vc0 = svuzp1(mc00, mc10);
      vc1 = svuzp1(mc20, mc30);
      vc2 = svuzp2(mc00, mc10);
      vc3 = svuzp2(mc20, mc30);
      vc4 = svuzp1(mc01, mc11);
      vc5 = svuzp1(mc21, mc31);
      vc6 = svuzp2(mc01, mc11);
      vc7 = svuzp2(mc21, mc31);

      UPDATE_C(pg32, ptr_c0, oc0, vc0);
      UPDATE_C(pg32, ptr_c0+4, oc1, vc1);
      UPDATE_C(pg32, ptr_c1, oc2, vc2);
      UPDATE_C(pg32, ptr_c1+4, oc3, vc3);
      UPDATE_C(pg32, ptr_c2, oc4, vc4)
      UPDATE_C(pg32, ptr_c2+4, oc5, vc5);
      UPDATE_C(pg32, ptr_c3, oc6, vc6)
      UPDATE_C(pg32, ptr_c3+4, oc7, vc7);

      ptr_c0 += 8;
      ptr_c1 += 8;
      ptr_c2 += 8;
      ptr_c3 += 8;
    }

    if (m & 4) {
      ptr_a0 = ptr_a;
      ptr_a += 4 * pad_k;
      ptr_b0 = ptr_b;

      INIT_C(0, 0); INIT_C(0, 1);
      INIT_C(1, 0); INIT_C(1, 1);

      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        ma1 = svld1_bf16(pg16, ptr_a0 + 8);
        mb0 = svld1_bf16(pg16, ptr_b0);
        mb1 = svld1_bf16(pg16, ptr_b0 + 8);

        MATMUL(0, 0); MATMUL(0, 1);
        MATMUL(1, 0); MATMUL(1, 1);

        ptr_a0 += 16;
        ptr_b0 += 16;
      }

      vc0 = svuzp1(mc00, mc10);
      vc1 = svuzp2(mc00, mc10);
      vc2 = svuzp1(mc01, mc11);
      vc3 = svuzp2(mc01, mc11);

      UPDATE_C(pg32, ptr_c0, oc0, vc0);
      UPDATE_C(pg32, ptr_c1, oc1, vc1);
      UPDATE_C(pg32, ptr_c2, oc2, vc2);
      UPDATE_C(pg32, ptr_c3, oc3, vc3);

      ptr_c0 += 4;
      ptr_c1 += 4;
      ptr_c2 += 4;
      ptr_c3 += 4;
    }

    if (m & 2) {
      ptr_a0 = ptr_a;
      ptr_a += 2 * pad_k;
      ptr_b0 = ptr_b;

      INIT_C(0, 0); INIT_C(0, 1);
      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        mb0 = svld1_bf16(pg16, ptr_b0);
        mb1 = svld1_bf16(pg16, ptr_b0 + 8);

        MATMUL(0, 0); MATMUL(0, 1);

        ptr_a0 += 8;
        ptr_b0 += 16;
      }

      vc0 = svuzp1(mc00, mc00);
      vc1 = svuzp2(mc00, mc00);
      vc2 = svuzp1(mc01, mc01);
      vc3 = svuzp2(mc01, mc01);

      UPDATE_C(pg32_low, ptr_c0, oc0, vc0);
      UPDATE_C(pg32_low, ptr_c1, oc1, vc1);
      UPDATE_C(pg32_low, ptr_c2, oc2, vc2);
      UPDATE_C(pg32_low, ptr_c3, oc3, vc3);

      ptr_c0 += 2;
      ptr_c1 += 2;
      ptr_c2 += 2;
      ptr_c3 += 2;
    }

    if (m & 1) {
      ptr_a0 = ptr_a;
      ptr_b0 = ptr_b;

      INIT_C(0, 0); INIT_C(0, 1);
      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16_low, ptr_a0);
        mb0 = svld1_bf16(pg16, ptr_b0);
        mb1 = svld1_bf16(pg16, ptr_b0 + 8);

        MATMUL(0, 0); MATMUL(0, 1);

        ptr_a0 += 4;
        ptr_b0 += 16;
      }

      vc1 = svuzp2(mc00, mc00);
      vc3 = svuzp2(mc01, mc01);

      UPDATE_C(pg32_first, ptr_c0, oc0, mc00);
      UPDATE_C(pg32_first, ptr_c1, oc1, vc1);
      UPDATE_C(pg32_first, ptr_c2, oc2, mc01);
      UPDATE_C(pg32_first, ptr_c3, oc3, vc3);

    }

    ptr_b += 4 * pad_k;
  }

  if (n & 2) {
    ptr_c0 = ptr_c;
    ptr_c1 = ptr_c0 + ldc;
    ptr_c += 2 * ldc;
    ptr_a = (bfloat16_t *)A;

    for (BLASLONG i = 0; i < m / 8; i++) {
      ptr_a0 = ptr_a;
      ptr_a += 8 * pad_k;

      ptr_b0 = ptr_b;

      INIT_C(0, 0);
      INIT_C(1, 0);
      INIT_C(2, 0);
      INIT_C(3, 0);

      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        ma1 = svld1_bf16(pg16, ptr_a0 + 8);
        ma2 = svld1_bf16(pg16, ptr_a0 + 16);
        ma3 = svld1_bf16(pg16, ptr_a0 + 24);

        mb0 = svld1_bf16(pg16, ptr_b0);

        MATMUL(0, 0);
        MATMUL(1, 0);
        MATMUL(2, 0);
        MATMUL(3, 0);

        ptr_a0 += 32;
        ptr_b0 += 8;
      }

      vc0 = svuzp1(mc00, mc10);
      vc1 = svuzp1(mc20, mc30);
      vc2 = svuzp2(mc00, mc10);
      vc3 = svuzp2(mc20, mc30);

      UPDATE_C(pg32, ptr_c0, oc0, vc0);
      UPDATE_C(pg32, ptr_c0 + 4, oc1, vc1);
      UPDATE_C(pg32, ptr_c1, oc2, vc2);
      UPDATE_C(pg32, ptr_c1 + 4, oc3, vc3);

      ptr_c0 += 8;
      ptr_c1 += 8;
    }

    if (m & 4) {
      ptr_a0 = ptr_a;
      ptr_a += 4 * pad_k;
      ptr_b0 = ptr_b;

      INIT_C(0, 0);
      INIT_C(1, 0);

      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        ma1 = svld1_bf16(pg16, ptr_a0 + 8);
        mb0 = svld1_bf16(pg16, ptr_b0);
        MATMUL(0, 0);
        MATMUL(1, 0);
        ptr_a0 += 16;
        ptr_b0 += 8;
      }

      vc0 = svuzp1(mc00, mc10);
      vc1 = svuzp2(mc00, mc10);

      UPDATE_C(pg32, ptr_c0, oc0, vc0);
      UPDATE_C(pg32, ptr_c1, oc1, vc1);

      ptr_c0 += 4;
      ptr_c1 += 4;
    }

    if (m & 2) {
      ptr_a0 = ptr_a;
      ptr_a += 2 * pad_k;
      ptr_b0 = ptr_b;

      INIT_C(0, 0);

      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        mb0 = svld1_bf16(pg16, ptr_b0);

        MATMUL(0, 0);

        ptr_a0 += 8;
        ptr_b0 += 8;
      }

      vc0 = svuzp1(mc00, mc00);
      vc1 = svuzp2(mc00, mc00);
      UPDATE_C(pg32_low, ptr_c0, oc0, vc0);
      UPDATE_C(pg32_low, ptr_c1, oc1, vc1);

      ptr_c0 += 2;
      ptr_c1 += 2;

    }

    if (m & 1) {
      ptr_a0 = ptr_a;
      ptr_b0 = ptr_b;
      INIT_C(0, 0);
      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16_low, ptr_a0);
        mb0 = svld1_bf16(pg16, ptr_b0);
        MATMUL(0, 0);
        ptr_a0 += 4;
        ptr_b0 += 8;
      }
      vc1 = svuzp2(mc00, mc00);

      UPDATE_C(pg32_first, ptr_c0, oc0, mc00);
      UPDATE_C(pg32_first, ptr_c1, oc1, vc1);
    }

    ptr_b += 2 * pad_k;
  }

  if (n & 1) {
    ptr_c0 = ptr_c;
    ptr_a = (bfloat16_t *)A;

    for (BLASLONG i = 0; i < m / 8; i++) {
      ptr_a0 = ptr_a;
      ptr_a += 8 * pad_k;

      ptr_b0 = ptr_b;

      INIT_C(0, 0);
      INIT_C(1, 0);
      INIT_C(2, 0);
      INIT_C(3, 0);

      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        ma1 = svld1_bf16(pg16, ptr_a0 + 8);
        ma2 = svld1_bf16(pg16, ptr_a0 + 16);
        ma3 = svld1_bf16(pg16, ptr_a0 + 24);

        mb0 = svld1_bf16(pg16_low, ptr_b0);

        MATMUL(0, 0);
        MATMUL(1, 0);
        MATMUL(2, 0);
        MATMUL(3, 0);

        ptr_a0 += 32;
        ptr_b0 += 4;
      }

      vc0 = svuzp1(mc00, mc10);
      vc1 = svuzp1(mc20, mc30);

      UPDATE_C(pg32, ptr_c0, oc0, vc0);
      UPDATE_C(pg32, ptr_c0 + 4, oc1, vc1);

      ptr_c0 += 8;
    }

    if (m & 4) {
      ptr_a0 = ptr_a;
      ptr_a += 4 * pad_k;
      ptr_b0 = ptr_b;
      INIT_C(0, 0);
      INIT_C(1, 0);
      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        ma1 = svld1_bf16(pg16, ptr_a0 + 8);
        mb0 = svld1_bf16(pg16_low, ptr_b0);
        MATMUL(0, 0);
        MATMUL(1, 0);
        ptr_a0 += 16;
        ptr_b0 += 4;
      }
      vc0 = svuzp1(mc00, mc10);
      UPDATE_C(pg32, ptr_c0, oc0, vc0);
      ptr_c0 += 4;
    }

    if (m & 2) {
      ptr_a0 = ptr_a;
      ptr_a += 2 * pad_k;
      ptr_b0 = ptr_b;

      INIT_C(0, 0);

      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16, ptr_a0);
        mb0 = svld1_bf16(pg16_low, ptr_b0);

        MATMUL(0, 0);

        ptr_a0 += 8;
        ptr_b0 += 4;
      }
      vc0 = svuzp1(mc00, mc00);
      UPDATE_C(pg32_low, ptr_c0, oc0, vc0);
      ptr_c0 += 2;
    }

    if (m & 1) {
      ptr_a0 = ptr_a;
      ptr_b0 = ptr_b;
      INIT_C(0, 0);
      for (BLASLONG p = 0; p < pad_k; p += 4) {
        ma0 = svld1_bf16(pg16_low, ptr_a0);
        mb0 = svld1_bf16(pg16_low, ptr_b0);
        MATMUL(0, 0);
        ptr_a0 += 4;
        ptr_b0 += 4;
      }
      UPDATE_C(pg32_first, ptr_c0, oc0, mc00);
    }
  }

  return 0;
}
