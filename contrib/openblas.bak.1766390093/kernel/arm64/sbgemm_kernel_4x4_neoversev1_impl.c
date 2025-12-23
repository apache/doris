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

#define INIT_C(M, N) mc##M##N = svdup_f32(0);

#define MATMUL(M, N) mc##M##N = svbfmmla(mc##M##N, ma##M, mb##N);

#define INIT_C_4x4                                                             \
  do {                                                                         \
    INIT_C(0, 0);                                                              \
    INIT_C(0, 1);                                                              \
    INIT_C(1, 0);                                                              \
    INIT_C(1, 1);                                                              \
  } while (0);

#ifdef ALPHA_ONE
#define UPDATE_C(PG, PTR, DST, SRC)                                            \
  do {                                                                         \
    DST = svld1_f32((PG), (PTR));                                              \
    DST = svadd_z((PG), SRC, DST);                                             \
    svst1_f32((PG), (PTR), DST);                                               \
  } while (0);
#else
#define UPDATE_C(PG, PTR, DST, SRC)                                            \
  do {                                                                         \
    DST = svld1_f32((PG), (PTR));                                              \
    DST = svmad_z((PG), svalpha, SRC, DST);                                    \
    svst1_f32((PG), (PTR), DST);                                               \
  } while (0);
#endif

#define ZIP_EVEN_ELEMENTS(PG, mc0, mc1, tmp, vc)                               \
  do {                                                                         \
    (tmp) = svuzp1_f32((mc0), (mc1));                                          \
    (vc) = svcompact_f32((PG), (tmp));                                         \
  } while (0)

#define ZIP_ODD_ELEMENTS(PG, mc0, mc1, tmp, vc)                                \
  do {                                                                         \
    (tmp) = svuzp2_f32((mc0), (mc1));                                          \
    (vc) = svcompact_f32((PG), (tmp));                                         \
  } while (0)

#define ACCUMULATE_LAST4_TO_FIRST4(M, N, TMP)                                  \
  do {                                                                         \
    TMP = svext_f32(mc##M##N, mc##M##N, 4);                                    \
    mc##M##N = svadd_f32_z(svptrue_b32(), mc##M##N, (TMP));                    \
  } while (0)

#ifdef ALPHA_ONE
int sbgemm_kernel_neoversev1_alpha_one(BLASLONG m, BLASLONG n, BLASLONG k,
                                       FLOAT alpha, IFLOAT *A, IFLOAT *B,
                                       FLOAT *C, BLASLONG ldc)
#else
int sbgemm_kernel_neoversev1_alpha(BLASLONG m, BLASLONG n, BLASLONG k,
                                   FLOAT alpha, IFLOAT *A, IFLOAT *B, FLOAT *C,
                                   BLASLONG ldc)
#endif
{

  BLASLONG pad_k = (k + 7) & ~7;
  svbfloat16_t ma0, ma1, mb0, mb1;
  svfloat32_t mc00, mc01, mc10, mc11, vc0, vc1, vc2, vc3, oc0, oc1, oc2, oc3;
  svfloat32_t tmp;
  svfloat32_t svalpha = svdup_f32(alpha);

  svbool_t pg16_all = svptrue_b16();

  svbool_t pg32_first_1 = svwhilelt_b32(0, 1);
  svbool_t pg32_first_2 = svwhilelt_b32(0, 2);
  svbool_t pg32_first_4 = svwhilelt_b32(0, 4);

  svbool_t pg32_select_first_2_per_quadword = svdupq_b32(1, 1, 0, 0);

  bfloat16_t *ptr_a = (bfloat16_t *)A;
  bfloat16_t *ptr_b = (bfloat16_t *)B;
  FLOAT *ptr_c = C;

  bfloat16_t *ptr_a0;
  bfloat16_t *ptr_b0;
  FLOAT *ptr_c0, *ptr_c1, *ptr_c2, *ptr_c3;

  for (BLASLONG j = 0; j < n / 4; j++) {
    ptr_c0 = ptr_c;
    ptr_c1 = ptr_c0 + ldc;
    ptr_c2 = ptr_c1 + ldc;
    ptr_c3 = ptr_c2 + ldc;
    ptr_c += 4 * ldc;
    ptr_a = (bfloat16_t *)A;

    for (BLASLONG i = 0; i < m / 4; i++) {
      ptr_a0 = ptr_a;
      ptr_a += 4 * pad_k;

      ptr_b0 = ptr_b;

      INIT_C_4x4;

      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        ma1 = svld1_bf16(pg16_all, ptr_a0 + 16);

        mb0 = svld1_bf16(pg16_all, ptr_b0);
        mb1 = svld1_bf16(pg16_all, ptr_b0 + 16);

        MATMUL(0, 0);
        MATMUL(0, 1);
        MATMUL(1, 0);
        MATMUL(1, 1);

        ptr_a0 += 32;
        ptr_b0 += 32;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);
      ACCUMULATE_LAST4_TO_FIRST4(0, 1, tmp);
      ACCUMULATE_LAST4_TO_FIRST4(1, 0, tmp);
      ACCUMULATE_LAST4_TO_FIRST4(1, 1, tmp);

      ZIP_EVEN_ELEMENTS(pg32_select_first_2_per_quadword, mc00, mc10, tmp, vc0);
      ZIP_ODD_ELEMENTS(pg32_select_first_2_per_quadword, mc00, mc10, tmp, vc1);

      ZIP_EVEN_ELEMENTS(pg32_select_first_2_per_quadword, mc01, mc11, tmp, vc2);
      ZIP_ODD_ELEMENTS(pg32_select_first_2_per_quadword, mc01, mc11, tmp, vc3);

      UPDATE_C(pg32_first_4, ptr_c0, oc0, vc0);
      UPDATE_C(pg32_first_4, ptr_c1, oc1, vc1);
      UPDATE_C(pg32_first_4, ptr_c2, oc2, vc2)
      UPDATE_C(pg32_first_4, ptr_c3, oc3, vc3)

      ptr_c0 += 4;
      ptr_c1 += 4;
      ptr_c2 += 4;
      ptr_c3 += 4;
    }

    if (m & 2) {
      ptr_a0 = ptr_a;
      ptr_a += 2 * pad_k;

      ptr_b0 = ptr_b;
      INIT_C(0, 0);
      INIT_C(0, 1);
      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        mb0 = svld1_bf16(pg16_all, ptr_b0);
        mb1 = svld1_bf16(pg16_all, ptr_b0 + 16);

        MATMUL(0, 0);
        MATMUL(0, 1);

        ptr_a0 += 16;
        ptr_b0 += 32;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);
      ACCUMULATE_LAST4_TO_FIRST4(0, 1, tmp);

      vc0 = svuzp1(mc00, mc00);
      vc1 = svuzp2(mc00, mc00);
      vc2 = svuzp1(mc01, mc01);
      vc3 = svuzp2(mc01, mc01);

      UPDATE_C(pg32_first_2, ptr_c0, oc0, vc0);
      UPDATE_C(pg32_first_2, ptr_c1, oc1, vc1);
      UPDATE_C(pg32_first_2, ptr_c2, oc2, vc2);
      UPDATE_C(pg32_first_2, ptr_c3, oc3, vc3);

      ptr_c0 += 2;
      ptr_c1 += 2;
      ptr_c2 += 2;
      ptr_c3 += 2;
    }

    if (m & 1) {
      ptr_a0 = ptr_a;
      ptr_b0 = ptr_b;

      INIT_C(0, 0);
      INIT_C(0, 1);
      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        mb0 = svld1_bf16(pg16_all, ptr_b0);
        mb1 = svld1_bf16(pg16_all, ptr_b0 + 16);

        MATMUL(0, 0);
        MATMUL(0, 1);

        ptr_a0 += 16;
        ptr_b0 += 32;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);
      ACCUMULATE_LAST4_TO_FIRST4(0, 1, tmp);

      // use compact is more straightforward
      vc1 = svuzp2(mc00, mc00);
      vc3 = svuzp2(mc01, mc01);

      UPDATE_C(pg32_first_1, ptr_c0, oc0, mc00);
      UPDATE_C(pg32_first_1, ptr_c1, oc1, vc1);
      UPDATE_C(pg32_first_1, ptr_c2, oc2, mc01);
      UPDATE_C(pg32_first_1, ptr_c3, oc3, vc3);
    }

    ptr_b += 4 * pad_k;
  }

  if (n & 2) {
    ptr_c0 = ptr_c;
    ptr_c1 = ptr_c0 + ldc;
    ptr_c += 2 * ldc;
    ptr_a = (bfloat16_t *)A;

    for (BLASLONG i = 0; i < m / 4; i++) {
      ptr_a0 = ptr_a;
      ptr_a += 4 * pad_k;

      ptr_b0 = ptr_b;

      INIT_C(0, 0);
      INIT_C(1, 0);

      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        ma1 = svld1_bf16(pg16_all, ptr_a0 + 16);

        mb0 = svld1_bf16(pg16_all, ptr_b0);

        MATMUL(0, 0);
        MATMUL(1, 0);

        ptr_a0 += 32;
        ptr_b0 += 16;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);
      ACCUMULATE_LAST4_TO_FIRST4(1, 0, tmp);

      ZIP_EVEN_ELEMENTS(pg32_select_first_2_per_quadword, mc00, mc10, tmp, vc0);
      ZIP_ODD_ELEMENTS(pg32_select_first_2_per_quadword, mc00, mc10, tmp, vc2);

      UPDATE_C(pg32_first_4, ptr_c0, oc0, vc0);
      UPDATE_C(pg32_first_4, ptr_c1, oc2, vc2);

      ptr_c0 += 4;
      ptr_c1 += 4;
    }

    if (m & 2) {
      ptr_a0 = ptr_a;
      ptr_a += 2 * pad_k;
      ptr_b0 = ptr_b;

      INIT_C(0, 0);

      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        mb0 = svld1_bf16(pg16_all, ptr_b0);

        MATMUL(0, 0);

        ptr_a0 += 16;
        ptr_b0 += 16;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);
      vc0 = svuzp1(mc00, mc00);
      vc1 = svuzp2(mc00, mc00);

      UPDATE_C(pg32_first_2, ptr_c0, oc0, vc0);
      UPDATE_C(pg32_first_2, ptr_c1, oc1, vc1);

      ptr_c0 += 2;
      ptr_c1 += 2;
    }

    if (m & 1) {
      ptr_a0 = ptr_a;
      ptr_b0 = ptr_b;
      INIT_C(0, 0);
      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        mb0 = svld1_bf16(pg16_all, ptr_b0);
        MATMUL(0, 0);
        ptr_a0 += 16;
        ptr_b0 += 16;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);
      vc1 = svuzp2(mc00, mc00);

      UPDATE_C(pg32_first_1, ptr_c0, oc0, mc00);
      UPDATE_C(pg32_first_1, ptr_c1, oc1, vc1);
    }

    ptr_b += 2 * pad_k;
  }

  if (n & 1) { // TODO: this case seems a overhead. find out whether it's in our
               // case.
    ptr_c0 = ptr_c;
    ptr_a = (bfloat16_t *)A;

    for (BLASLONG i = 0; i < m / 4; i++) {
      ptr_a0 = ptr_a;
      ptr_a += 4 * pad_k;

      ptr_b0 = ptr_b;

      INIT_C(0, 0);
      INIT_C(1, 0);

      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        ma1 = svld1_bf16(pg16_all, ptr_a0 + 16);

        mb0 = svld1_bf16(pg16_all, ptr_b0);

        MATMUL(0, 0);
        MATMUL(1, 0);

        ptr_a0 += 32;
        ptr_b0 += 16;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);
      ACCUMULATE_LAST4_TO_FIRST4(1, 0, tmp);

      ZIP_EVEN_ELEMENTS(pg32_select_first_2_per_quadword, mc00, mc10, tmp, vc0);

      UPDATE_C(pg32_first_4, ptr_c0, oc0, vc0);

      ptr_c0 += 4;
    }

    if (m & 2) {
      ptr_a0 = ptr_a;
      ptr_a += 2 * pad_k;
      ptr_b0 = ptr_b;

      INIT_C(0, 0);

      for (BLASLONG p = 0; p < pad_k; p += 8) {
        ma0 = svld1_bf16(pg16_all, ptr_a0);
        mb0 = svld1_bf16(pg16_all, ptr_b0);

        MATMUL(0, 0);

        ptr_a0 += 16;
        ptr_b0 += 16;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);

      vc0 = svuzp1(mc00, mc00);

      UPDATE_C(pg32_first_2, ptr_c0, oc0, vc0);

      ptr_c0 += 2;
    }

    if (m & 1) {
      ptr_a0 = ptr_a;
      ptr_b0 = ptr_b;

      INIT_C(0, 0);
      for (BLASLONG p = 0; p < pad_k; p += 8) {

        ma0 = svld1_bf16(pg16_all, ptr_a0);
        mb0 = svld1_bf16(pg16_all, ptr_b0);

        MATMUL(0, 0);
        ptr_a0 += 16;
        ptr_b0 += 16;
      }

      ACCUMULATE_LAST4_TO_FIRST4(0, 0, tmp);

      UPDATE_C(pg32_first_1, ptr_c0, oc0, mc00);
    }
  }

  return 0;
}
